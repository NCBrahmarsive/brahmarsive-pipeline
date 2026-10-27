#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health routine.

Process version: Oct 2026 (lifecycle <-> lead-status rules, rebuilt recycler,
owner action lists, workflow firing checks).

This script consumes a pre-pulled snapshot of HubSpot data (`raw_input.json`,
built by the weekly MCP data pull) and writes, for a single run date:

  snapshots/YYYY-MM-DD.json            -- raw + every worklist id (machine)
  reports/YYYY-MM-DD.md                -- Headline + Q4 scorecard + Sections 1-6
  reports/YYYY-MM-DD_owner_<first>.md  -- one Section-5 block per owner (email-ready)

Run:

    python3 pipeline.py --input raw_input.json

It must always be run with --input; there is no live-fetch path (the run
environment pulls via the HubSpot MCP tools and hands the data here).

Scoping / join / field rules (see the routine spec for the authority):
  * B2B only. Contacts filtered on customer_type=B2B; companies on
    customer_type__cloned_=B2B. Contact counts come from CONTACT only; deal
    value comes from DEAL, rolled up at COMPANY level.
  * Deal USD value is amount_in_home_currency. Raw amount + deal_currency_code
    are secondary reference only. Empty/0 home-currency value is FLAGGED.
  * Lead-status internal values are mixed format (labels AND codes). The run
    reads the hs_lead_status definition into lead_status_map; every rule below
    matches on the mapped LABEL, never a hard-coded internal value.
"""

import argparse
import datetime as dt
import json
import os
import sys

# =========================================================================
# EDITABLE CONSTANTS
# =========================================================================

PORTAL_ID = "50467095"
LINK = {
    "contact": f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-1/{{id}}",
    "company": f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-2/{{id}}",
    "deal": f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-3/{{id}}",
}

DEALSTAGE_LABELS = {
    "1217017229": "Initial Engagement",
    "appointmentscheduled": "Qualified to Buy",
    "presentationscheduled": "Discovery & Trial",
    "1217017230": "Quote Sent",
    "contractsent": "Commitment to Buy",
    "decisionmakerboughtin": "Negotiation",
    "closedwon": "Closed Won",
    "closedlost": "Closed Lost",
}
CLOSED_STAGES = {"closedwon", "closedlost"}
# Open pipeline order (closed stages shown separately as reference).
STAGE_ORDER = [
    "1217017229",            # Initial Engagement
    "appointmentscheduled",  # Qualified to Buy
    "presentationscheduled", # Discovery & Trial
    "1217017230",            # Quote Sent
    "contractsent",          # Commitment to Buy
    "decisionmakerboughtin", # Negotiation
]
CLOSED_ORDER = ["closedwon", "closedlost"]
TRIAL_STAGE = "presentationscheduled"         # Discovery & Trial
DATE_ENTERED_TRIAL = "date_entered_trial"      # hs_v2_date_entered_presentationscheduled

SALES_OWNERS = ["Theo Zirogiannis", "Jack Makhlouf", "Anup Ghoshal", "Biju Rajesh"]
# Anyone else (incl. Nicola Cole, Ramjee, Unassigned) rolls into Nicola's block.
NICOLA = "Nicola Cole"

SALES_WORKING_STATUSES = ["Attempted to Contact", "Contacted"]   # labels

# Lifecycle internal value -> label (for display).
LIFECYCLE_LABELS = {
    "subscriber": "Email Contact",
    "lead": "Lead",
    "marketingqualifiedlead": "Marketing Qualified Lead",
    "salesqualifiedlead": "Sales Qualified Lead",
    "opportunity": "Opportunity",
    "customer": "Customer",
    "other": "Other",
}

# Valid lead-status LABELS per lifecycle (Opportunity handled specially below).
VALID_STATUS_BY_LIFECYCLE = {
    "subscriber": ["New", "Marketing Engaged"],
    "lead": ["New", "Marketing Engaged"],
    "marketingqualifiedlead": ["MQL", "New", "Unqualified"],
    "salesqualifiedlead": ["Attempted to Contact", "Contacted"],
    "opportunity": ["Qualified", "In Progress"],
    "customer": ["Customer"],
}
# Lifecycles where a BLANK lead status is itself a mismatch (SQL and beyond).
BLANK_IS_MISMATCH_FROM = {"salesqualifiedlead", "opportunity", "customer"}

# The spec's VALID_STATUS lists reference the label "New"; the portal's
# hs_lead_status option for internal value NEW is labelled "New Lead".
# Reconcile the known discrepancy here so the guard does not raise on it and
# 3D does not produce false positives. Surfaced in the report's data notes.
LEAD_STATUS_LABEL_ALIASES = {"New": "New Lead"}

RECYCLE_DAYS = 90
RECYCLE_WARN_DAYS = 75
DEAL_IDLE_DAYS = 30
WINDOW_DAYS = 7
IDLE_BUCKETS = [30, 60]
QTB_STALE_DAYS = 14                 # open-deal idle threshold for headline %
MQL_FOLLOWUP_DAYS = 7

CLAIM_WINDOW_RUN = "2026-10-12"     # Section 5c runs only when today == this
CLAIM_DEADLINE = "2026-10-16"

Q4_START = "2026-10-01"
Q4_END = "2026-12-31"
Q4_WEEKS = 13
Q4_TARGETS = {"new_deals": 20, "entered_trial": 7, "closed_won": 3}

SCHEMA_VERSION = 2   # bumped for the Oct 2026 rebuild (old snapshots not comparable)


# =========================================================================
# Helpers
# =========================================================================

def parse_ts(value):
    """Parse an ISO string or epoch-ms to an aware UTC datetime, or None."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value / 1000.0, tz=dt.timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        return dt.datetime.fromtimestamp(int(s) / 1000.0, tz=dt.timezone.utc)
    s = s.replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d


def to_date(value):
    d = parse_ts(value)
    return d.date() if d else None


def days_since(value, run_dt):
    d = parse_ts(value)
    if d is None:
        return None
    return (run_dt - d).days


def to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def within_window(value, run_date, days=WINDOW_DAYS):
    d = to_date(value)
    if d is None:
        return False
    return (run_date - d).days < days and d <= run_date


def in_range(value, start, end):
    d = to_date(value)
    if d is None:
        return False
    return start <= d <= end


def fmt_usd(v):
    if v is None:
        return "—"
    return f"${v:,.0f}"


def fmt_days(d):
    if d is None:
        return "no activity"
    return f"{d}d"


def median(nums):
    nums = sorted(n for n in nums if n is not None)
    if not nums:
        return None
    n = len(nums)
    mid = n // 2
    if n % 2:
        return nums[mid]
    return (nums[mid - 1] + nums[mid]) / 2.0


def clink(c):
    return f"[{c.get('name') or c['id']}]({LINK['contact'].format(id=c['id'])})"


def complink(co):
    return f"[{co.get('name') or co['id']}]({LINK['company'].format(id=co['id'])})"


def dlink(d):
    return f"[{d.get('dealname') or d['id']}]({LINK['deal'].format(id=d['id'])})"


def delta(cur, prev):
    if prev is None:
        return f"{cur}"
    diff = cur - prev
    if diff > 0:
        return f"{cur} (+{diff})"
    if diff < 0:
        return f"{cur} ({diff})"
    return f"{cur} (=)"


def pct(n, d):
    if not d:
        return "n/a"
    return f"{round(100.0 * n / d)}%"


# =========================================================================
# Owner resolution
# =========================================================================

class Owners:
    def __init__(self, owner_map):
        self.map = owner_map or {}

    def name(self, owner_id):
        if owner_id in (None, "", "null"):
            return "Unassigned"
        return self.map.get(str(owner_id), f"Unknown ({owner_id})")

    def bucket(self, owner_id):
        """Which Section-5 block an owner's items roll into."""
        nm = self.name(owner_id)
        return nm if nm in SALES_OWNERS else NICOLA


# =========================================================================
# Load + guards
# =========================================================================

def load_raw(path):
    with open(path) as f:
        return json.load(f)


def canonical_status(label):
    """Apply label aliases so spec labels line up with portal labels."""
    return label


def apply_alias(label):
    """Map an actual portal label to the spec's label vocabulary (reverse alias)."""
    # spec label -> portal label is LEAD_STATUS_LABEL_ALIASES; we compare in
    # portal-label space, so translate the VALID_STATUS spec labels forward.
    return LEAD_STATUS_LABEL_ALIASES.get(label, label)


def valid_labels_for(lifecycle):
    """VALID_STATUS labels for a lifecycle, translated to portal-label space."""
    return [apply_alias(x) for x in VALID_STATUS_BY_LIFECYCLE.get(lifecycle, [])]


def run_guards(raw, status_label):
    """Raise on the conditions the spec marks as must-RAISE."""
    problems = []

    # Company query sanity (0 or full unfiltered count).
    cc = raw.get("companies_b2b_counts", {})
    total_b2b = cc.get("total_b2b")
    total_all = cc.get("total_all")
    if total_b2b == 0:
        problems.append("B2B company count is 0 — B2B filter or pull broken.")
    if total_all is not None and total_b2b == total_all:
        problems.append(
            f"B2B company count ({total_b2b}) equals the unfiltered company "
            f"count — customer_type__cloned_ filter is not applying.")

    # lead_status_map must contain every label used in VALID_STATUS_BY_LIFECYCLE
    # (after alias reconciliation).
    known_labels = set(status_label.values())
    for lifecycle, labels in VALID_STATUS_BY_LIFECYCLE.items():
        for lab in labels:
            portal_lab = apply_alias(lab)
            if portal_lab not in known_labels:
                problems.append(
                    f"lead_status_map has no label '{portal_lab}' "
                    f"(referenced as '{lab}' for lifecycle '{lifecycle}').")

    if problems:
        raise SystemExit("GUARD FAILURE:\n  - " + "\n  - ".join(problems))


# =========================================================================
# Derivation
# =========================================================================

def label_of(status_label, value):
    if value in (None, "", "null"):
        return None
    return status_label.get(value, value)


def build(raw, run_date):
    run_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)
    owners = Owners(raw.get("owners", {}))
    status_label = raw.get("lead_status_map", {})

    q4_start = dt.date.fromisoformat(Q4_START)
    q4_end = dt.date.fromisoformat(Q4_END)

    # ---- Companies: id -> record ----
    companies = {}
    for co in raw.get("companies_b2b", []):
        companies[str(co["id"])] = co

    def company_created_in_window(cid):
        co = companies.get(str(cid))
        if not co:
            return None  # unknown (not pulled) -> treat as existing/older
        return within_window(co.get("createdate"), run_date)

    # ---- Contacts ----
    contacts = []
    for c in raw.get("contacts_b2b", []):
        rec = dict(c)
        rec["id"] = str(c["id"])
        rec["status_label"] = label_of(status_label, c.get("hs_lead_status"))
        rec["owner_name"] = owners.name(c.get("owner_id"))
        rec["bucket"] = owners.bucket(c.get("owner_id"))
        rec["idle_days"] = days_since(c.get("notes_last_updated"), run_dt)
        rec["is_new"] = within_window(c.get("createdate"), run_date)
        contacts.append(rec)

    by_life = {}
    for c in contacts:
        by_life.setdefault(c.get("lifecyclestage"), []).append(c)

    funnel_counts = dict(raw.get("funnel_counts_b2b", {}))

    # ---- Deals ----
    deals = []
    for d in raw.get("deals_b2b", []):
        rec = dict(d)
        rec["id"] = str(d["id"])
        rec["stage"] = d.get("dealstage")
        rec["stage_label"] = DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage"))
        rec["usd"] = to_float(d.get("amount_in_home_currency"))
        rec["owner_name"] = owners.name(d.get("owner_id"))
        rec["bucket"] = owners.bucket(d.get("owner_id"))
        rec["idle_days"] = days_since(d.get("notes_last_updated"), run_dt)
        rec["is_open"] = d.get("dealstage") not in CLOSED_STAGES
        deals.append(rec)

    return {
        "run_date": run_date,
        "run_dt": run_dt,
        "owners": owners,
        "status_label": status_label,
        "companies": companies,
        "company_created_in_window": company_created_in_window,
        "contacts": contacts,
        "by_life": by_life,
        "funnel_counts": funnel_counts,
        "deals": deals,
        "q4_start": q4_start,
        "q4_end": q4_end,
    }


# =========================================================================
# Section computations
# =========================================================================

def compute(ctx):
    run_date = ctx["run_date"]
    run_dt = ctx["run_dt"]
    deals = ctx["deals"]
    contacts = ctx["contacts"]
    by_life = ctx["by_life"]
    owners = ctx["owners"]
    q4s, q4e = ctx["q4_start"], ctx["q4_end"]

    out = {}

    open_deals = [d for d in deals if d["is_open"]]

    # ---- Q4 scorecard ----
    today = run_date
    weeks_elapsed = max(0.0, (today - q4s).days / 7.0)
    def expected(target):
        return round(target * min(weeks_elapsed, Q4_WEEKS) / Q4_WEEKS)

    new_deals = [d for d in deals if in_range(d.get("createdate"), q4s, q4e)]
    entered_trial = [d for d in deals if in_range(d.get(DATE_ENTERED_TRIAL), q4s, q4e)]
    closed_won_q4 = [d for d in deals
                     if d["stage"] == "closedwon" and in_range(d.get("closedate"), q4s, q4e)]

    def owner_contrib(items):
        agg = {}
        for d in items:
            agg[d["owner_name"]] = agg.get(d["owner_name"], 0) + 1
        return dict(sorted(agg.items(), key=lambda kv: -kv[1]))

    out["q4"] = {
        "weeks_elapsed": round(weeks_elapsed, 1),
        "rows": [
            {"metric": "New deals", "definition": "Deals created in Q4",
             "actual": len(new_deals), "target": Q4_TARGETS["new_deals"],
             "expected": expected(Q4_TARGETS["new_deals"]),
             "contrib": owner_contrib(new_deals)},
            {"metric": "Entered Discovery & Trial",
             "definition": "Deals that entered Discovery & Trial in Q4 (cumulative)",
             "actual": len(entered_trial), "target": Q4_TARGETS["entered_trial"],
             "expected": expected(Q4_TARGETS["entered_trial"]),
             "contrib": owner_contrib(entered_trial)},
            {"metric": "Closed Won",
             "definition": "Deals closed won in Q4",
             "actual": len(closed_won_q4), "target": Q4_TARGETS["closed_won"],
             "expected": expected(Q4_TARGETS["closed_won"]),
             "contrib": owner_contrib(closed_won_q4)},
        ],
    }

    # ---- Section 1.1 open pipeline by stage ----
    stage_rows = []
    open_qty = 0
    open_usd = 0.0
    for stage in STAGE_ORDER:
        ds = [d for d in open_deals if d["stage"] == stage]
        usd = sum(d["usd"] or 0 for d in ds)
        idle = [d["idle_days"] for d in ds]
        stage_rows.append({
            "stage": stage, "label": DEALSTAGE_LABELS[stage],
            "qty": len(ds), "usd": usd,
            "median_idle": median(idle),
            "idle_ge14": sum(1 for x in idle if x is not None and x >= QTB_STALE_DAYS),
        })
        open_qty += len(ds)
        open_usd += usd
    closed_rows = []
    for stage in CLOSED_ORDER:
        ds = [d for d in deals if d["stage"] == stage]
        closed_rows.append({"stage": stage, "label": DEALSTAGE_LABELS[stage],
                            "qty": len(ds), "usd": sum(d["usd"] or 0 for d in ds)})
    out["s1_1"] = {"rows": stage_rows, "open_qty": open_qty, "open_usd": open_usd,
                   "closed": closed_rows}

    # ---- Section 1.2 open pipeline by owner ----
    owner_agg = {}
    for d in open_deals:
        a = owner_agg.setdefault(d["owner_name"], {"qty": 0, "usd": 0.0})
        a["qty"] += 1
        a["usd"] += d["usd"] or 0
    owner_rows = sorted(
        [{"owner": k, **v} for k, v in owner_agg.items()],
        key=lambda r: -r["usd"])
    # concentration
    top_owner = owner_rows[0] if owner_rows else None
    top_deal = max(open_deals, key=lambda d: d["usd"] or 0, default=None)
    out["s1_2"] = {
        "rows": owner_rows, "open_usd": open_usd,
        "top_owner": top_owner,
        "top_owner_share": (pct(top_owner["usd"], open_usd) if top_owner else "n/a"),
        "top_deal": top_deal,
        "top_deal_share": (pct(top_deal["usd"] or 0, open_usd) if top_deal else "n/a"),
    }

    # ---- Section 1.3 updated / moved in last 7 days ----
    moved, updated = [], []
    for d in deals:
        stage_moved = within_window(d.get("hs_v2_date_entered_current_stage"), run_date)
        mod = within_window(d.get("hs_lastmodifieddate"), run_date)
        if stage_moved:
            moved.append(d)
        elif mod:
            updated.append(d)
    moved.sort(key=lambda d: -(d["usd"] or 0))
    updated.sort(key=lambda d: -(d["usd"] or 0))
    out["s1_3"] = {"moved": moved, "updated": updated}

    # ---- Section 2.1 new companies by owner ----
    new_companies = [co for co in ctx["companies"].values()
                     if within_window(co.get("createdate"), run_date)]
    nc_by_owner = {}
    nc_no_owner = []
    for co in new_companies:
        onm = owners.name(co.get("owner_id"))
        nc_by_owner.setdefault(onm, []).append(co)
        if co.get("owner_id") in (None, "", "null"):
            nc_no_owner.append(co)
    out["s2_1"] = {"new_companies": new_companies, "by_owner": nc_by_owner,
                   "no_owner": nc_no_owner}

    # ---- Section 2.2 new contacts on existing vs new accounts ----
    new_contacts = [c for c in contacts if c["is_new"]]
    on_existing, on_new, orphan_new = [], [], []
    for c in new_contacts:
        cid = c.get("associatedcompanyid")
        if cid in (None, "", "null"):
            orphan_new.append(c)
            continue
        created_in_win = ctx["company_created_in_window"](cid)
        if created_in_win:
            on_new.append(c)
        else:
            on_existing.append(c)   # company older than window (or unknown)
    ne_by_owner = {}
    for c in on_existing:
        ne_by_owner.setdefault(c["owner_name"], []).append(c)
    out["s2_2"] = {"on_existing": on_existing, "on_new": on_new,
                   "by_owner": ne_by_owner, "new_contacts_total": len(new_contacts)}

    # ---- Section 2.3 SQLs by sales owner (B2B contacts only) ----
    sqls = by_life.get("salesqualifiedlead", [])
    sql_owner = {}
    for c in sqls:
        a = sql_owner.setdefault(c["owner_name"], {"qty": 0, "idle30": 0, "idle60": 0})
        a["qty"] += 1
        idle = c["idle_days"]
        if idle is None or idle >= 30:
            a["idle30"] += 1
        if idle is None or idle >= 60:
            a["idle60"] += 1
    sql_rows = sorted([{"owner": k, **v} for k, v in sql_owner.items()],
                      key=lambda r: -r["qty"])
    out["s2_3"] = {"rows": sql_rows, "total": len(sqls),
                   "idle30": sum(r["idle30"] for r in sql_rows),
                   "idle60": sum(r["idle60"] for r in sql_rows)}

    # ---- Section 3A companies missing owner/name ----
    comp_incomplete = []
    for co in ctx["companies"].values():
        miss_owner = co.get("owner_id") in (None, "", "null")
        miss_name = not (co.get("name") or "").strip()
        if miss_owner or miss_name:
            comp_incomplete.append(co)
    out["s3A"] = {"rows": comp_incomplete}

    # ---- Section 3B deal hygiene ----
    b_missing_usd = [d for d in open_deals if d["usd"] in (None, 0, 0.0)]
    b_idle = [d for d in open_deals
              if d["idle_days"] is None or d["idle_days"] >= DEAL_IDLE_DAYS]
    b_pastclose = [d for d in open_deals
                   if to_date(d.get("closedate")) and to_date(d.get("closedate")) < run_date]
    b_noowner = [d for d in open_deals if d.get("owner_id") in (None, "", "null")]
    out["s3B"] = {"missing_usd": b_missing_usd, "idle": b_idle,
                  "past_close": b_pastclose, "no_owner": b_noowner}

    # ---- Section 3C miscategorised ----
    misc = []
    for c in contacts:
        if (c["status_label"] in SALES_WORKING_STATUSES
                and c.get("lifecyclestage") in ("subscriber", "lead", "marketingqualifiedlead")):
            misc.append(c)
    out["s3C"] = {"rows": misc, "by_owner": group_by_owner(misc)}

    # ---- Section 3D status mismatches ----
    mism = []
    for c in contacts:
        life = c.get("lifecyclestage")
        lab = c["status_label"]
        reason = None
        expected_lbl = None
        if life == "opportunity":
            ndeals = to_float(c.get("num_associated_deals")) or 0
            if lab is None:
                reason = "blank status at Opportunity"
                expected_lbl = "Qualified / In Progress"
            elif lab == "In Progress" and ndeals > 0:
                reason = "In Progress but has deals (should be Qualified)"
                expected_lbl = "Qualified"
            elif lab == "Qualified" and ndeals == 0:
                reason = "Qualified but 0 deals (should be In Progress)"
                expected_lbl = "In Progress"
            elif lab not in ("Qualified", "In Progress"):
                reason = f"status '{lab}' invalid at Opportunity"
                expected_lbl = "Qualified / In Progress"
        elif life in VALID_STATUS_BY_LIFECYCLE:
            valid = valid_labels_for(life)
            if lab is None:
                if life in BLANK_IS_MISMATCH_FROM:
                    reason = f"blank status at {LIFECYCLE_LABELS.get(life, life)}"
                    expected_lbl = " / ".join(valid)
            elif lab not in valid:
                reason = f"status '{lab}' invalid at {LIFECYCLE_LABELS.get(life, life)}"
                expected_lbl = " / ".join(valid)
        if reason:
            mism.append({**c, "mismatch_reason": reason, "expected": expected_lbl})
    out["s3D"] = {"rows": mism, "by_owner": group_by_owner(mism)}

    # ---- Section 3E orphan new contacts ----
    orphans = [c for c in contacts if c["is_new"]
               and c.get("associatedcompanyid") in (None, "", "null")]
    out["s3E"] = {"rows": orphans, "by_owner": group_by_owner(orphans)}

    # ---- Section 3F lost-deal leftovers (Opportunity, all deals closed lost) ----
    # Build company -> deals map from the pulled B2B deals.
    deals_by_company = {}
    for d in deals:
        cid = str(d.get("company_id")) if d.get("company_id") else None
        if cid:
            deals_by_company.setdefault(cid, []).append(d)
    lost_leftovers = []
    for c in by_life.get("opportunity", []):
        cid = c.get("associatedcompanyid")
        cds = deals_by_company.get(str(cid), []) if cid else []
        if cds and all(d["stage"] == "closedlost" for d in cds):
            lost_leftovers.append(c)
    out["s3F"] = {"rows": lost_leftovers, "by_owner": group_by_owner(lost_leftovers)}

    # ---- Section 3G MQL follow-up ----
    mql_followup = []
    mql_no_owner = []
    for c in by_life.get("marketingqualifiedlead", []):
        if c["status_label"] != "MQL":
            continue
        # no sales activity for MQL_FOLLOWUP_DAYS+ since entering MQL
        entered = c.get("date_entered_mql")
        act_days = c["idle_days"]
        no_activity_since = (act_days is None or act_days >= MQL_FOLLOWUP_DAYS)
        entered_days = days_since(entered, run_dt)
        old_enough = entered_days is None or entered_days >= MQL_FOLLOWUP_DAYS
        if no_activity_since and old_enough:
            mql_followup.append(c)
        if c.get("owner_id") in (None, "", "null"):
            mql_no_owner.append(c)
    out["s3G"] = {"rows": mql_followup, "no_owner": mql_no_owner,
                  "by_owner": group_by_owner(mql_followup)}

    # ---- Section 3H won but not customer ----
    won_not_customer = []
    seen = set()
    for d in deals:
        if d["stage"] != "closedwon":
            continue
        ctype = (d.get("company_type") or "").upper()
        cid = str(d.get("company_id")) if d.get("company_id") else None
        if ctype == "PROSPECT" and cid and cid not in seen:
            seen.add(cid)
            won_not_customer.append({
                "id": cid, "name": d.get("company_name"),
                "deal": d, "owner_id": d.get("owner_id"),
            })
    out["s3H"] = {"rows": won_not_customer}

    # ---- Section 4 recycler health ----
    # 4A trigger match now: B2B SQL AND (last activity >90d OR (never active AND created>90d))
    def recycler_trigger(c):
        idle = c["idle_days"]
        if idle is not None:
            return idle > RECYCLE_DAYS
        # no activity ever
        created_days = days_since(c.get("createdate"), run_dt)
        return created_days is not None and created_days > RECYCLE_DAYS
    trigger_now = [c for c in sqls if recycler_trigger(c)]
    # Canary: the rebuilt trigger includes "B2B contact at SQL", so a genuinely
    # non-B2B SQL can NEVER meet the full trigger. The only way this fires is a
    # mislabelled record (customer_type actually B2B inside the non-B2B pull),
    # which would mean the B2B filter is broken. Stale non-B2B SQLs that the
    # recycler correctly ignores are expected and are reported as info, not an
    # alert.
    nonb2b_stale = []
    nonb2b_trigger = []
    for c in raw_nonb2b_sql(ctx):
        if str(c.get("customer_type")) == "B2B":
            nonb2b_trigger.append(c)
            continue
        idle = days_since(c.get("notes_last_updated"), run_dt)
        created_days = days_since(c.get("createdate"), run_dt)
        if (idle is not None and idle > RECYCLE_DAYS) or \
           (idle is None and created_days is not None and created_days > RECYCLE_DAYS):
            nonb2b_stale.append(c)
    is_claim_run = (run_date.isoformat() == CLAIM_WINDOW_RUN)
    out["s4A"] = {"rows": trigger_now, "by_owner": group_by_owner(trigger_now),
                  "nonb2b": nonb2b_trigger, "nonb2b_stale": nonb2b_stale,
                  "is_claim_run": is_claim_run}

    # 4C recycled this week
    recycled_week = [c for c in contacts
                     if within_window(c.get("recycled_date"), run_date)]
    rec_still_sql = [c for c in recycled_week if c.get("lifecyclestage") == "salesqualifiedlead"]
    out["s4C"] = {"rows": recycled_week, "by_owner": group_by_owner(recycled_week),
                  "still_sql": rec_still_sql}

    # 4D stuck recycles + re-promotions
    stuck, repromo = [], []
    for c in contacts:
        rec = to_date(c.get("recycled_date"))
        if not rec:
            continue
        if c.get("lifecyclestage") != "salesqualifiedlead":
            continue
        sqld = to_date(c.get("date_entered_sql"))
        if sqld and sqld < rec:
            stuck.append(c)
        elif sqld and sqld > rec:
            repromo.append(c)
    out["s4D"] = {"stuck": stuck, "repromo": repromo,
                  "by_owner": group_by_owner(stuck)}

    # ---- Section 6 workflow firing checks ----
    # Sales Activity > Lead Status: 3C created/modified this week == 0
    s3c_recent = [c for c in misc if c["is_new"]
                  or within_window(c.get("hs_lastmodifieddate"), run_date)]
    # Meeting Booked > Contacted: no "Contacted" at Email Contact/Lead/MQL
    contacted_early = [c for c in contacts if c["status_label"] == "Contacted"
                       and c.get("lifecyclestage") in ("subscriber", "lead", "marketingqualifiedlead")]
    # Stale SQL Recycler: 4A overdue (>1d past threshold) == 0 after claim window, and 4D stuck == 0
    overdue = [c for c in trigger_now
               if (c["idle_days"] is not None and c["idle_days"] > RECYCLE_DAYS + 1)
               or (c["idle_days"] is None)]
    recycler_healthy = (len(stuck) == 0) and (
        is_claim_run_or_before(run_date) or len(overdue) == 0)
    # Lifecycle > Lead Status Sync: 3D rows at Opportunity/Customer == 0
    s3d_oppcust = [c for c in mism if c.get("lifecyclestage") in ("opportunity", "customer")]
    # Deal Associated > Qualified: Opportunity + In Progress with deals>0 == 0
    opp_inprogress_deals = [c for c in by_life.get("opportunity", [])
                            if c["status_label"] == "In Progress"
                            and (to_float(c.get("num_associated_deals")) or 0) > 0]
    # Deal required fields: open deals CREATED this week with empty/zero USD == 0
    new_deal_missing_usd = [d for d in open_deals
                            if within_window(d.get("createdate"), run_date)
                            and d["usd"] in (None, 0, 0.0)]
    out["s6"] = {
        "rows": [
            {"wf": "Sales Activity > Lead Status",
             "healthy_if": "3C contacts created/modified this week = 0",
             "ok": len(s3c_recent) == 0, "offenders": s3c_recent},
            {"wf": "Meeting Booked > Contacted",
             "healthy_if": "no \"Contacted\" at Email Contact / Lead / MQL",
             "ok": len(contacted_early) == 0, "offenders": contacted_early},
            {"wf": "Stale SQL Recycler",
             "healthy_if": "4A overdue = 0 (after claim window) and 4D stuck = 0",
             "ok": recycler_healthy,
             "offenders": (overdue if not is_claim_run_or_before(run_date) else []) + stuck},
            {"wf": "Lifecycle > Lead Status Sync",
             "healthy_if": "3D rows at Opportunity/Customer = 0",
             "ok": len(s3d_oppcust) == 0, "offenders": s3d_oppcust},
            {"wf": "Deal Associated > Qualified",
             "healthy_if": "Opportunity + In Progress with deals > 0 = 0",
             "ok": len(opp_inprogress_deals) == 0, "offenders": opp_inprogress_deals},
            {"wf": "Deal required fields",
             "healthy_if": "open deals created this week with empty/zero USD = 0",
             "ok": len(new_deal_missing_usd) == 0, "offenders_deals": new_deal_missing_usd},
        ]
    }

    # ---- Headline numbers ----
    out["headline"] = {
        "wf_ok": sum(1 for r in out["s6"]["rows"] if r["ok"]),
        "wf_total": len(out["s6"]["rows"]),
        "stage_moved": len(out["s1_3"]["moved"]),
        "open_deals": len(open_deals),
        "open_idle14": sum(1 for d in open_deals
                           if d["idle_days"] is None or d["idle_days"] >= QTB_STALE_DAYS),
        "sql_total": len(sqls),
        "sql_idle30": out["s2_3"]["idle30"],
        "misc": len(misc),
        "mismatch": len(mism),
        "stuck_recycle": len(stuck),
        "deals_past_close": len(b_pastclose),
        "deals_missing_usd": len(b_missing_usd),
        "new_accounts_no_owner": len(nc_no_owner),
        "recycled_week": len(recycled_week),
        "orphans": len(orphans),
    }

    return out


def is_claim_run_or_before(run_date):
    return run_date <= dt.date.fromisoformat(CLAIM_WINDOW_RUN)


def raw_nonb2b_sql(ctx):
    return ctx.get("_nonb2b_sql", [])


def group_by_owner(items):
    g = {}
    for c in items:
        key = c.get("owner_name") or "Unassigned"
        g.setdefault(key, []).append(c)
    return g


# =========================================================================
# Week-on-week
# =========================================================================

def load_prev_snapshot(snap_dir, run_date):
    if not os.path.isdir(snap_dir):
        return None
    cands = []
    for name in os.listdir(snap_dir):
        if not name.endswith(".json"):
            continue
        try:
            d = dt.date.fromisoformat(name[:-5])
        except ValueError:
            continue
        if d < run_date:
            cands.append((d, name))
    if not cands:
        return None
    cands.sort()
    with open(os.path.join(snap_dir, cands[-1][1])) as f:
        prev = json.load(f)
    if prev.get("schema_version") != SCHEMA_VERSION:
        return {"_incompatible": True, "run_date": prev.get("run_date")}
    return prev


def prev_head(prev, key):
    if not prev or prev.get("_incompatible"):
        return None
    return prev.get("headline", {}).get(key)


# =========================================================================
# Rendering
# =========================================================================

def render(ctx, r, prev, data_notes):
    run_date = ctx["run_date"].isoformat()
    A = []
    w = A.append

    incompatible = bool(prev and prev.get("_incompatible"))
    comparable = bool(prev and not incompatible)

    w(f"# Brahmarsive Weekly Pipeline & MQL Health — {run_date}")
    w("")
    w(f"*Scope: B2B only (contacts `customer_type=B2B`, companies "
      f"`customer_type__cloned_=B2B`). Contact counts from the CONTACT object; "
      f"deal value from DEALS (USD via `amount_in_home_currency`), rolled up at "
      f"company level. Process version: Oct 2026.*")
    w("")
    if comparable:
        w(f"*Week-on-week deltas vs previous snapshot `{prev['run_date']}`.*")
    elif incompatible:
        w(f"*Prior snapshot `{prev['run_date']}` predates the Oct 2026 rebuild "
          f"(incompatible schema) — week-on-week comparison starts next run.*")
    else:
        w("*No prior snapshot found — week-on-week comparison starts next run.*")
    w("")
    if data_notes:
        w("> **Data notes:** " + " ".join(data_notes))
        w("")

    # ---------- Q4 SCORECARD ----------
    q4 = r["q4"]
    w(f"## Q4 Scorecard — B2B deals, {Q4_START} → {run_date} "
      f"({q4['weeks_elapsed']} of {Q4_WEEKS} weeks elapsed)")
    w("")
    w("| Metric | Definition | Actual | Target | Expected by now | Status |")
    w("|--------|------------|-------:|-------:|----------------:|:------:|")
    for row in q4["rows"]:
        status = "✅" if row["actual"] >= row["expected"] else "⚠️"
        pk = {"New deals": "q4_new", "Entered Discovery & Trial": "q4_trial",
              "Closed Won": "q4_won"}[row["metric"]]
        pv = prev_head(prev, pk)
        act = delta(row["actual"], pv) if comparable else str(row["actual"])
        w(f"| {row['metric']} | {row['definition']} | {act} | {row['target']} "
          f"| {row['expected']} | {status} |")
    w("")
    for row in q4["rows"]:
        contrib = ", ".join(f"{k} {v}" for k, v in row["contrib"].items()) or "—"
        w(f"- **{row['metric']} by owner:** {contrib}")
    w("")

    # ---------- HEADLINE FLAGS ----------
    h = r["headline"]
    w("## 🚩 Headline flags")
    w("")
    wf_icon = "✅" if h["wf_ok"] == h["wf_total"] else "⚠️"
    def hd(label, key, val, suffix=""):
        pv = prev_head(prev, key)
        return f"- {label}: {delta(val, pv) if comparable else val}{suffix}"
    w(f"- Workflow checks: {wf_icon} **{h['wf_ok']}/{h['wf_total']} healthy** "
      f"(see Section 6)")
    w(hd("Deals that changed stage this week", "stage_moved", h["stage_moved"]))
    w(f"- Open deals idle {QTB_STALE_DAYS}+d: **{pct(h['open_idle14'], h['open_deals'])}** "
      f"({h['open_idle14']} of {h['open_deals']} open)")
    w(f"- SQLs untouched 30+d: **{pct(h['sql_idle30'], h['sql_total'])}** "
      f"({h['sql_idle30']} of {h['sql_total']} B2B SQL contacts)")
    w(hd("Miscategorised leads (3C)", "misc", h["misc"]))
    w(hd("Status mismatches (3D)", "mismatch", h["mismatch"]))
    w(hd("Stuck recycles (4D)", "stuck_recycle", h["stuck_recycle"]))
    w(hd("Deals past close date", "deals_past_close", h["deals_past_close"]))
    w(hd("Deals missing/zero USD value", "deals_missing_usd", h["deals_missing_usd"]))
    w(hd("New accounts with no owner", "new_accounts_no_owner", h["new_accounts_no_owner"]))
    w(hd("Recycled this week", "recycled_week", h["recycled_week"]))
    w(hd("Orphan new contacts (3E)", "orphans", h["orphans"]))
    if r["s4A"]["nonb2b"]:
        w(f"- ⚠️ **{len(r['s4A']['nonb2b'])} NON-B2B SQL(s) match the recycler "
          f"trigger** — B2B filter may be broken (see 4A).")
    w("")

    # ========== SECTION 1 ==========
    w("# Section 1 — Current Pipeline Status")
    w("")
    s = r["s1_1"]
    w("## 1.1 Open pipeline by stage")
    w("")
    w("| Stage | Deals | Total USD | Median days idle | Idle ≥14d |")
    w("|-------|------:|----------:|-----------------:|----------:|")
    for row in s["rows"]:
        mi = "—" if row["median_idle"] is None else f"{row['median_idle']:.0f}"
        w(f"| {row['label']} | {row['qty']} | {fmt_usd(row['usd'])} | {mi} | {row['idle_ge14']} |")
    w(f"| **Open subtotal** | **{s['open_qty']}** | **{fmt_usd(s['open_usd'])}** | | |")
    w("")
    w("*Reference (not in open subtotal):*")
    for row in s["closed"]:
        w(f"- {row['label']}: {row['qty']} deals, {fmt_usd(row['usd'])}")
    w("")

    s = r["s1_2"]
    w("## 1.2 Open pipeline by sales owner")
    w("")
    w("| Owner | Open deals | Open USD |")
    w("|-------|-----------:|---------:|")
    for row in s["rows"]:
        w(f"| {row['owner']} | {row['qty']} | {fmt_usd(row['usd'])} |")
    w(f"| **Total** | | **{fmt_usd(s['open_usd'])}** |")
    w("")
    if s["top_owner"]:
        w(f"**Concentration:** {s['top_owner']['owner']} holds "
          f"{s['top_owner_share']} of open value ({fmt_usd(s['top_owner']['usd'])}). "
          + (f"Largest single open deal {dlink(s['top_deal'])} "
             f"({fmt_usd(s['top_deal']['usd'])}) = {s['top_deal_share']} of open value."
             if s["top_deal"] else ""))
        w("")

    s = r["s1_3"]
    w("## 1.3 Updated / moved in the last 7 days")
    w("")
    w(f"**Stage moved ({len(s['moved'])})** — real progression (entered current "
      f"stage within 7 days):")
    w("")
    if s["moved"]:
        w("| Deal | Owner | Current stage | USD | Last activity |")
        w("|------|-------|---------------|----:|--------------|")
        for d in s["moved"]:
            la = (to_date(d.get("notes_last_updated")) or "—")
            w(f"| {dlink(d)} | {d['owner_name']} | {d['stage_label']} | "
              f"{fmt_usd(d['usd'])} | {la} |")
    else:
        w("- None.")
    w("")
    w(f"**Updated only ({len(s['updated'])})** — field change within 7 days, no "
      f"stage change:")
    w("")
    if s["updated"]:
        w("| Deal | Owner | Current stage | USD | Last activity |")
        w("|------|-------|---------------|----:|--------------|")
        for d in s["updated"]:
            la = (to_date(d.get("notes_last_updated")) or "—")
            w(f"| {dlink(d)} | {d['owner_name']} | {d['stage_label']} | "
              f"{fmt_usd(d['usd'])} | {la} |")
    else:
        w("- None.")
    w("")

    # ========== SECTION 2 ==========
    w("# Section 2 — New Pipeline Status")
    w("")
    s = r["s2_1"]
    w(f"## 2.1 New companies created in last 7 days ({len(s['new_companies'])})")
    w("")
    if s["new_companies"]:
        w("| Company | Owner | Created | Source |")
        w("|---------|-------|---------|--------|")
        for co in sorted(s["new_companies"], key=lambda x: owner_sort(x)):
            onm = ctx["owners"].name(co.get("owner_id"))
            flag = " ⚠️ no owner" if co.get("owner_id") in (None, "", "null") else ""
            w(f"| {complink(co)} | {onm}{flag} | {to_date(co.get('createdate')) or '—'} "
              f"| {co.get('original_source') or '—'} |")
    else:
        w("- None.")
    w("")

    s = r["s2_2"]
    w("## 2.2 New contacts on existing companies (last 7 days)")
    w("")
    w(f"{len(s['on_existing'])} new contact(s) on existing accounts; "
      f"{len(s['on_new'])} new contact(s) on accounts created this week.")
    w("")
    if s["by_owner"]:
        for owner, items in sorted(s["by_owner"].items()):
            w(f"**{owner}** ({len(items)})")
            w("")
            # group by lifecycle
            bylife = {}
            for c in items:
                bylife.setdefault(c.get("lifecyclestage"), []).append(c)
            for life, cs in bylife.items():
                names = ", ".join(clink(c) for c in cs)
                w(f"- {LIFECYCLE_LABELS.get(life, life)}: {names}")
            w("")
    else:
        w("- None on existing accounts.")
        w("")

    s = r["s2_3"]
    w("## 2.3 SQLs by sales owner (B2B contacts — the true SQL total)")
    w("")
    w("| Owner | SQL contacts | Untouched 30+d | Untouched 60+d |")
    w("|-------|-------------:|---------------:|---------------:|")
    for row in s["rows"]:
        w(f"| {row['owner']} | {row['qty']} | {row['idle30']} | {row['idle60']} |")
    w(f"| **Total** | **{s['total']}** | **{s['idle30']}** | **{s['idle60']}** |")
    w("")

    # ========== SECTION 3 ==========
    w("# Section 3 — Admin / Data-Hygiene Worklist")
    w("")

    s = r["s3A"]
    w(f"## 3A Companies to complete ({len(s['rows'])}) — *owner: Nicola*")
    w("")
    if s["rows"]:
        w("| Company | Created | Owner | Name | Source | Created by |")
        w("|---------|---------|-------|------|--------|-----------|")
        for co in s["rows"]:
            onm = ctx["owners"].name(co.get("owner_id"))
            oflag = onm if co.get("owner_id") not in (None, "", "null") else "**none**"
            nm = co.get("name") or "**blank**"
            cb = co.get("created_by") or "—"
            w(f"| {complink(co)} | {to_date(co.get('createdate')) or '—'} | {oflag} "
              f"| {nm} | {co.get('original_source') or '—'} | {cb} |")
    else:
        w("- None.")
    w("")

    s = r["s3B"]
    w("## 3B Deal hygiene (open B2B deals)")
    w("")
    _deal_table(w, "(i) Missing or zero USD value", s["missing_usd"])
    _deal_table(w, f"(ii) Idle {DEAL_IDLE_DAYS}+ days", s["idle"])
    _deal_table(w, "(iii) Close date in the past", s["past_close"])
    _deal_table(w, "(iv) No deal owner", s["no_owner"])

    _owner_contact_section(
        w, "3C", "Miscategorised leads",
        "Sales working status but lifecycle still Email Contact / Lead / MQL "
        "(should be SQL).", r["s3C"]["by_owner"],
        extra=lambda c: f"{LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage'))} · {c['status_label']}")

    _owner_contact_section(
        w, "3D", "Status mismatches",
        "Lead status not valid for the contact's lifecycle (incl. the "
        "Opportunity deal-count rule).", r["s3D"]["by_owner"],
        extra=lambda c: f"{LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage'))} · "
                        f"now '{c['status_label'] or 'blank'}' → expect {c.get('expected')}")

    _owner_contact_section(
        w, "3E", "Orphan new contacts",
        "New (7d) B2B contacts not associated to any company "
        "(`associatedcompanyid` is null).", r["s3E"]["by_owner"])

    _owner_contact_section(
        w, "3F", "Lost-deal leftovers",
        "Contacts at Opportunity whose associated deals are ALL closed lost — "
        "they stay at Opportunity forever unless moved (rule TBC: recycle to "
        "MQL + Unqualified \"Lost deal\").", r["s3F"]["by_owner"])

    s = r["s3G"]
    _owner_contact_section(
        w, "3G", "MQL follow-up",
        f"Contacts at MQL with no sales activity for {MQL_FOLLOWUP_DAYS}+ days "
        f"since entering MQL" + (f" (plus {len(s['no_owner'])} MQL with no owner)"
                                  if s["no_owner"] else ""),
        s["by_owner"])

    s = r["s3H"]
    w(f"## 3H Won but not Customer ({len(s['rows'])})")
    w("")
    w("B2B companies with a Closed Won deal whose company Type is still Prospect.")
    w("")
    if s["rows"]:
        w("| Company | Won deal | USD |")
        w("|---------|----------|----:|")
        for row in s["rows"]:
            co = {"id": row["id"], "name": row["name"]}
            w(f"| {complink(co)} | {dlink(row['deal'])} | {fmt_usd(row['deal']['usd'])} |")
    else:
        w("- None.")
    w("")

    # ========== SECTION 4 ==========
    w("# Section 4 — Recycler Health Check")
    w("")
    w("*Live recycler (rebuilt Oct 2026): B2B contact at SQL AND (last activity "
      f"> {RECYCLE_DAYS} days ago OR (no activity ever AND created > {RECYCLE_DAYS} "
      "days ago)) → clear lifecycle, set MQL, lead status Unqualified, reason "
      "\"Stalled – no activity 90d\", recycled_date, enrol in re-engagement "
      "nurture. Section 4 diagnoses the workflow; it does not change it.*")
    w("")
    s = r["s4A"]
    claim_phase = is_claim_run_or_before(ctx["run_date"])
    hdr = ("held backlog (info — before claim window closes)"
           if claim_phase else "trigger matches NOW")
    w(f"## 4A Trigger match now — {len(s['rows'])} ({hdr})")
    w("")
    if s["rows"]:
        if not claim_phase:
            overdue = [c for c in s["rows"]
                       if c["idle_days"] is None or c["idle_days"] > RECYCLE_DAYS + 1]
            if overdue:
                w(f"⚠️ **ALERT: {len(overdue)} match(es) are >1 day past threshold "
                  f"— recycler not firing.**")
                w("")
        for owner, items in sorted(s["by_owner"].items(), key=lambda kv: -len(kv[1])):
            lst = ", ".join(f"{clink(c)} ({fmt_days(c['idle_days'])} idle)" for c in items)
            w(f"- **{owner}** ({len(items)}): {lst}")
    else:
        w("- ✅ No B2B SQLs currently match the recycler trigger.")
    w("")
    if s["nonb2b"]:
        w(f"⚠️ **ALERT: {len(s['nonb2b'])} NON-B2B SQL(s) match the trigger rules — "
          f"the B2B filter on the recycler may be broken.** "
          + ", ".join(f"#{c.get('id')} ({c.get('customer_type')})" for c in s["nonb2b"]))
        w("")
    else:
        w(f"- ✅ Canary clear: no mislabelled B2B records in the non-B2B SQL pull "
          f"(B2B filter intact). *Info: {len(s.get('nonb2b_stale', []))} genuinely "
          f"non-B2B SQL(s) are stale 90+d — correctly left untouched by the recycler.*")
        w("")

    s = r["s4C"]
    w(f"## 4C Recycled this week ({len(s['rows'])})")
    w("")
    if s["rows"]:
        for owner, items in sorted(s["by_owner"].items()):
            lst = ", ".join(clink(c) for c in items)
            w(f"- **{owner}** ({len(items)}): {lst}")
        if s["still_sql"]:
            w("")
            w(f"⚠️ {len(s['still_sql'])} recycled contact(s) still at SQL → see 4D.")
    else:
        w("- None recycled in the last 7 days.")
    w("")

    s = r["s4D"]
    w("## 4D Stuck recycles & re-promotions")
    w("")
    if s["stuck"]:
        w(f"⚠️ **ALERT: {len(s['stuck'])} stuck recycle(s)** — recycled_date set but "
          f"still at SQL and date_entered_sql < recycled_date (the move to MQL "
          f"failed, lifecycle not cleared first).")
        w("")
        for owner, items in sorted(s["by_owner"].items()):
            lst = ", ".join(clink(c) for c in items)
            w(f"- **{owner}** ({len(items)}): {lst}")
    else:
        w("- ✅ No stuck recycles.")
    w("")
    w(f"Re-promotions (info only — sales re-engaged after recycle): "
      f"{len(s['repromo'])}"
      + ("" if not s["repromo"] else " — "
         + ", ".join(clink(c) for c in s["repromo"])))
    w("")

    # ========== SECTION 5 ==========
    w("# Section 5 — Owner Action Lists")
    w("")
    for owner in SALES_OWNERS + [NICOLA]:
        block = render_owner_block(ctx, r, owner, prev)
        A.extend(block)

    # ========== SECTION 6 ==========
    w("# Section 6 — Workflow Firing Checks (\"built ≠ working\")")
    w("")
    w("| Workflow | Healthy if | Status |")
    w("|----------|------------|:------:|")
    for row in r["s6"]["rows"]:
        w(f"| {row['wf']} | {row['healthy_if']} | {'✅' if row['ok'] else '⚠️'} |")
    w("")
    for row in r["s6"]["rows"]:
        if row["ok"]:
            continue
        offs = row.get("offenders") or []
        offs_d = row.get("offenders_deals") or []
        links = [clink(c) for c in offs] + [dlink(d) for d in offs_d]
        w(f"- ⚠️ **{row['wf']}** ({len(links)}): " + (", ".join(links[:40]) or "—")
          + (" …" if len(links) > 40 else ""))
    w("")

    w("---")
    w(f"*Snapshot: `snapshots/{run_date}.json` · Report: `reports/{run_date}.md` · "
      f"Owner files: `reports/{run_date}_owner_<first>.md`*")
    w("")
    return "\n".join(A)


def owner_sort(co):
    return (co.get("owner_id") in (None, "", "null"), str(co.get("owner_id")))


def _deal_table(w, title, deals):
    w(f"**{title}** ({len(deals)})")
    w("")
    if deals:
        w("| Deal | Stage | Owner | USD | Last activity | Close date |")
        w("|------|-------|-------|----:|---------------|-----------|")
        for d in sorted(deals, key=lambda x: -(x["usd"] or 0)):
            la = to_date(d.get("notes_last_updated")) or "—"
            cd = to_date(d.get("closedate")) or "—"
            w(f"| {dlink(d)} | {d['stage_label']} | {d['owner_name']} | "
              f"{fmt_usd(d['usd'])} | {la} | {cd} |")
    else:
        w("- None.")
    w("")


def _owner_contact_section(w, code, title, desc, by_owner, extra=None):
    total = sum(len(v) for v in by_owner.values())
    w(f"## {code} {title} ({total})")
    w("")
    w(desc)
    w("")
    if by_owner:
        for owner, items in sorted(by_owner.items(), key=lambda kv: -len(kv[1])):
            if extra:
                lst = "; ".join(f"{clink(c)} — {extra(c)}" for c in items)
            else:
                lst = ", ".join(clink(c) for c in items)
            w(f"- **{owner}** ({len(items)}): {lst}")
    else:
        w("- None.")
    w("")


# =========================================================================
# Section 5 owner blocks
# =========================================================================

def render_owner_block(ctx, r, owner, prev):
    """Render one owner's Section-5 block; returns list of lines (may be header-only)."""
    lines = []
    w = lines.append
    is_nicola = (owner == NICOLA)

    # Gather this owner's items.
    def mine(items):
        return [c for c in items if c.get("bucket") == owner]

    # a. records changed this week (lifecycle or lead-status diff vs last week)
    changed = owner_changed_this_week(ctx, owner, prev)
    # b. recycling in next 15 days (75..89d idle, or no activity created 75-89d)
    warn = []
    for c in ctx["by_life"].get("salesqualifiedlead", []):
        if c.get("bucket") != owner:
            continue
        idle = c["idle_days"]
        if idle is not None and RECYCLE_WARN_DAYS <= idle < RECYCLE_DAYS:
            warn.append(c)
        elif idle is None:
            cd = days_since(c.get("createdate"), ctx["run_dt"])
            if cd is not None and RECYCLE_WARN_DAYS <= cd < RECYCLE_DAYS:
                warn.append(c)
    # c. claim window (only when today == CLAIM_WINDOW_RUN)
    claim = mine(r["s4A"]["rows"]) if r["s4A"]["is_claim_run"] else []
    # d. recycled this week
    recycled = mine(r["s4C"]["rows"])
    # e. deals needing attention (3B i/ii/iii)
    deals_att = []
    seen = set()
    for key in ("missing_usd", "idle", "past_close"):
        for d in r["s3B"][key]:
            if d.get("bucket") == owner and d["id"] not in seen:
                seen.add(d["id"])
                deals_att.append(d)
    # f. data fixes (3C,3D,3E,4D)
    fixes = {
        "3C": mine(r["s3C"]["rows"]),
        "3D": mine(r["s3D"]["rows"]),
        "3E": mine(r["s3E"]["rows"]),
        "4D": mine(r["s4D"]["stuck"]),
    }

    # Nicola also owns 3A, 3H, and everything owned by non-sales / Unassigned.
    nic_3a = r["s3A"]["rows"] if is_nicola else []
    nic_3h = r["s3H"]["rows"] if is_nicola else []
    # One-off reminder: on any run after the claim deadline, while the 4A backlog
    # is non-zero, prompt Nicola to manually enrol the remaining contacts.
    claim_closed_backlog = (
        len(r["s4A"]["rows"])
        if (is_nicola and ctx["run_date"] > dt.date.fromisoformat(CLAIM_DEADLINE)
            and len(r["s4A"]["rows"]) > 0)
        else 0)

    has_any = any([changed, warn, claim, recycled, deals_att,
                   any(fixes.values()), nic_3a, nic_3h, claim_closed_backlog])

    w(f"## 5 — {owner}" + ("  *(+ unassigned & non-sales owners)*" if is_nicola else ""))
    w("")
    if not has_any:
        w("- Nothing to action this week. ✅")
        w("")
        return lines

    if changed:
        w("**a. Your records changed this week** — review; flag anything wrong.")
        w("")
        for c in changed:
            w(f"- {clink(c)}: {c['_diff']}")
        w("")
    if warn:
        w(f"**b. Recycling in the next 15 days** ({len(warn)}) — log activity now "
          f"or they return to marketing.")
        w("")
        for c in warn:
            w(f"- {clink(c)} ({fmt_days(c['idle_days'])} idle)")
        w("")
    if claim:
        w(f"**c. CLAIM WINDOW** ({len(claim)}) — to keep a contact, log an activity "
          f"(call, email or note) by Fri 16 Oct. Any still without activity after "
          f"that will be recycled to marketing.")
        w("")
        for c in claim:
            w(f"- {clink(c)} ({fmt_days(c['idle_days'])} idle)")
        w("")
    if recycled:
        w(f"**d. Recycled this week** ({len(recycled)}).")
        w("")
        for c in recycled:
            w(f"- {clink(c)}")
        w("")
    if deals_att:
        w(f"**e. Deals needing attention** ({len(deals_att)}).")
        w("")
        for d in deals_att:
            why = []
            if d["usd"] in (None, 0, 0.0):
                why.append("missing USD")
            if d["idle_days"] is None or d["idle_days"] >= DEAL_IDLE_DAYS:
                why.append(f"idle {fmt_days(d['idle_days'])}")
            cd = to_date(d.get("closedate"))
            if cd and cd < ctx["run_date"]:
                why.append("past close date")
            w(f"- {dlink(d)} ({d['stage_label']}, {fmt_usd(d['usd'])}) — {', '.join(why)}")
        w("")
    if any(fixes.values()):
        w("**f. Data fixes.**")
        w("")
        for code, items in fixes.items():
            if not items:
                continue
            lst = ", ".join(clink(c) for c in items)
            w(f"- {code}: {lst}")
        w("")
    if nic_3a:
        w(f"**3A — companies to complete** ({len(nic_3a)}).")
        w("")
        for co in nic_3a:
            miss = []
            if co.get("owner_id") in (None, "", "null"):
                miss.append("owner")
            if not (co.get("name") or "").strip():
                miss.append("name")
            w(f"- {complink(co)} — missing {', '.join(miss)}")
        w("")
    if nic_3h:
        w(f"**3H — won but still Prospect** ({len(nic_3h)}).")
        w("")
        for row in nic_3h:
            co = {"id": row["id"], "name": row["name"]}
            w(f"- {complink(co)} — {dlink(row['deal'])}")
        w("")
    if claim_closed_backlog:
        w(f"**⚠️ Claim window closed** — open the Stale SQL Recycler workflow and "
          f"manually enrol the remaining {claim_closed_backlog} contact(s) "
          f"(4A backlog the recycler has not actioned).")
        w("")

    return lines


def owner_changed_this_week(ctx, owner, prev):
    """Contacts (this owner's bucket) whose lifecycle or lead status changed vs
    the previous comparable snapshot."""
    if not prev or prev.get("_incompatible"):
        return []
    prev_state = prev.get("contact_state", {})
    changed = []
    for c in ctx["contacts"]:
        if c.get("bucket") != owner:
            continue
        ps = prev_state.get(c["id"])
        if not ps:
            continue
        diffs = []
        if ps.get("lifecyclestage") != c.get("lifecyclestage"):
            diffs.append(
                f"lifecycle {LIFECYCLE_LABELS.get(ps.get('lifecyclestage'), ps.get('lifecyclestage'))}"
                f" → {LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage'))}")
        if ps.get("hs_lead_status") != c.get("hs_lead_status"):
            diffs.append(f"status {ps.get('hs_lead_status') or 'blank'} → "
                         f"{c.get('hs_lead_status') or 'blank'}")
        if diffs:
            changed.append({**c, "_diff": "; ".join(diffs)})
    return changed


# =========================================================================
# Snapshot
# =========================================================================

def build_snapshot(ctx, r, raw, generated):
    # Per-contact state for next week's 5a diff.
    contact_state = {c["id"]: {"lifecyclestage": c.get("lifecyclestage"),
                               "hs_lead_status": c.get("hs_lead_status")}
                     for c in ctx["contacts"]}

    def ids(items):
        return [c.get("id") for c in items]

    worklist_ids = {
        "s3A": ids(r["s3A"]["rows"]),
        "s3B_missing_usd": ids(r["s3B"]["missing_usd"]),
        "s3B_idle": ids(r["s3B"]["idle"]),
        "s3B_past_close": ids(r["s3B"]["past_close"]),
        "s3B_no_owner": ids(r["s3B"]["no_owner"]),
        "s3C": ids(r["s3C"]["rows"]),
        "s3D": ids(r["s3D"]["rows"]),
        "s3E": ids(r["s3E"]["rows"]),
        "s3F": ids(r["s3F"]["rows"]),
        "s3G": ids(r["s3G"]["rows"]),
        "s3H": [x["id"] for x in r["s3H"]["rows"]],
        "s4A": ids(r["s4A"]["rows"]),
        "s4C": ids(r["s4C"]["rows"]),
        "s4D_stuck": ids(r["s4D"]["stuck"]),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "run_date": ctx["run_date"].isoformat(),
        "generated_at_utc": generated,
        "headline": {
            "q4_new": r["q4"]["rows"][0]["actual"],
            "q4_trial": r["q4"]["rows"][1]["actual"],
            "q4_won": r["q4"]["rows"][2]["actual"],
            **r["headline"],
        },
        "section_counts": {
            "s3C": len(r["s3C"]["rows"]),
            "s3D": len(r["s3D"]["rows"]),
            "s3E": len(r["s3E"]["rows"]),
            "s4C": len(r["s4C"]["rows"]),
            "s4D_stuck": len(r["s4D"]["stuck"]),
            "s6_ok": r["headline"]["wf_ok"],
        },
        "worklist_ids": worklist_ids,
        "contact_state": contact_state,
        "raw": raw,
    }


# =========================================================================
# Main
# =========================================================================

def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly pipeline routine")
    ap.add_argument("--input", required=True, help="Pre-pulled raw JSON")
    ap.add_argument("--date", help="Run date YYYY-MM-DD (default: raw.generated_at date)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
    args = ap.parse_args()

    raw = load_raw(args.input)

    if args.date:
        run_date = dt.date.fromisoformat(args.date)
    elif raw.get("run_date"):
        run_date = dt.date.fromisoformat(raw["run_date"])
    else:
        run_date = parse_ts(raw.get("generated_at")).date()

    status_label = raw.get("lead_status_map", {})
    run_guards(raw, status_label)

    data_notes = list(raw.get("data_notes", []))

    ctx = build(raw, run_date)
    ctx["_nonb2b_sql"] = raw.get("sql_non_b2b", [])
    r = compute(ctx)

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev_snapshot(snap_dir, run_date)

    generated = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    report = render(ctx, r, prev, data_notes)
    snapshot = build_snapshot(ctx, r, raw, generated)

    snap_path = os.path.join(snap_dir, f"{run_date.isoformat()}.json")
    with open(snap_path, "w") as f:
        json.dump(snapshot, f, indent=2, default=str)
    rep_path = os.path.join(rep_dir, f"{run_date.isoformat()}.md")
    with open(rep_path, "w") as f:
        f.write(report)

    # Owner files (Section 5 block each).
    owner_files = []
    for owner in SALES_OWNERS + [NICOLA]:
        block = render_owner_block(ctx, r, owner, prev)
        first = owner.split()[0].lower()
        path = os.path.join(rep_dir, f"{run_date.isoformat()}_owner_{first}.md")
        header = [f"# {owner} — pipeline actions, week of {run_date.isoformat()}", ""]
        with open(path, "w") as f:
            f.write("\n".join(header + block))
        owner_files.append(path)

    print(f"Wrote {snap_path}")
    print(f"Wrote {rep_path}")
    for p in owner_files:
        print(f"Wrote {p}")
    if prev and prev.get("_incompatible"):
        print(f"Prior snapshot {prev['run_date']} incompatible (schema) — "
              f"comparison starts next run.")
    elif prev:
        print(f"Compared against previous snapshot: {prev['run_date']}")
    else:
        print("No previous snapshot found (first comparable run).")


if __name__ == "__main__":
    main()
