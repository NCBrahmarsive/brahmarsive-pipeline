#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report.  Process version: Oct 2026.

Reads a pre-pulled HubSpot dataset (raw_input.json, assembled via the HubSpot
MCP tools — direct api.hubapi.com egress is blocked in the run environment) and
writes, per run:

  snapshots/YYYY-MM-DD.json            raw + all worklist ids + metrics (machine)
  reports/YYYY-MM-DD.md                Headline + Sections 1-6 (readable)
  reports/YYYY-MM-DD_owner_<first>.md  one owner's Section-5 block (pasteable)

Run:  python3 pipeline.py --input raw_input.json [--date YYYY-MM-DD]

SCOPING (non-negotiable):
  * B2B only everywhere.  Contact counts come ONLY from the CONTACT object;
    pipeline value comes ONLY from DEALS rolled up at company level.  The two
    bases are never mixed.
  * Deal USD value = amount_in_home_currency.  Empty/0 is FLAGGED, never backfilled
    from raw `amount`.  Raw amount + currency shown only as secondary reference.
  * Lead-status values are mixed-format; every rule matches on the mapped LABEL
    (lead_status_map from the live property), never a hard-coded internal value.
"""

import argparse
import datetime as dt
import json
import os
import statistics
import sys

# =========================================================================
# EDITABLE CONSTANTS
# =========================================================================

PORTAL_ID = "50467095"

DEALSTAGE_LABELS = {
    "1217017229": "Initial Engagement",
    "appointmentscheduled": "Qualified to Buy",
    "presentationscheduled": "Discovery & Trial",
    "1217017230": "Quote Sent",
    "contractsent": "Commitment to Buy",
    "decisionmakerboughtin": "Negotiation",
    "closedwon": "Closed Won",
    "closedlost": "Closed Lost",
    # Legacy pipeline stage ids that still appear on some deals:
    "1211945117": "Closed Won",
    "1211945118": "Closed Lost",
}

# Open-pipeline stage order (the live Sales Pipeline).  Closed Won / Closed Lost
# are shown below the open subtotal as reference only.
STAGE_ORDER = [
    "Initial Engagement", "Qualified to Buy", "Discovery & Trial",
    "Quote Sent", "Commitment to Buy", "Negotiation",
]
CLOSED_LABELS = ["Closed Won", "Closed Lost"]

SALES_OWNERS = ["Theo Zirogiannis", "Jack Makhlouf", "Anup Ghoshal", "Biju Rajesh"]
# Anyone else (incl. Nicola Cole, Ramjee, Unassigned) rolls into Nicola's block.
NICOLA = "Nicola Cole"

SALES_WORKING_STATUSES = ["Attempted to Contact", "Contacted"]      # labels

# Valid lead-status LABELS per lifecycle stage.
# NOTE: the spec wrote "New"; the portal's actual label for internal value NEW is
# "New Lead".  We match on the live label, so the constant uses "New Lead".
VALID_STATUS_BY_LIFECYCLE = {
    "subscriber":            ["New Lead", "Marketing Engaged"],
    "lead":                  ["New Lead", "Marketing Engaged"],
    "marketingqualifiedlead": ["MQL", "New Lead", "Unqualified"],
    "salesqualifiedlead":    ["Attempted to Contact", "Contacted"],
    "opportunity":           ["Qualified", "In Progress"],
    "customer":              ["Customer"],
}
# Lifecycle stages at/after which a BLANK lead status is itself a mismatch.
SQL_OR_BEYOND = {"salesqualifiedlead", "opportunity", "customer"}

LIFECYCLE_LABELS = {
    "subscriber": "Email Contact", "lead": "Lead",
    "marketingqualifiedlead": "Marketing Qualified Lead",
    "salesqualifiedlead": "Sales Qualified Lead",
    "opportunity": "Opportunity", "customer": "Customer", "other": "Other",
}

RECYCLE_DAYS = 90
RECYCLE_WARN_DAYS = 75
DEAL_IDLE_DAYS = 30
WINDOW_DAYS = 7
IDLE_BUCKETS = [30, 60]
QTB_STALE_DAYS = 14
SQL_UNTOUCHED_HEADLINE_DAYS = 30
OPEN_DEAL_IDLE_HEADLINE_DAYS = 14

CLAIM_WINDOW_RUN = "2026-10-12"
CLAIM_DEADLINE = "2026-10-16"

Q4_START = "2026-10-01"
Q4_END = "2026-12-31"
Q4_WEEKS = 13
Q4_TARGETS = {"new_deals": 20, "entered_trial": 7, "closed_won": 3}

MQL_FOLLOWUP_DAYS = 7

# =========================================================================
# HELPERS
# =========================================================================

def parse_ts(value):
    if not value:
        return None
    v = str(value).strip()
    if not v:
        return None
    v = v.replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(v)
    except ValueError:
        # Try plain date
        try:
            d = dt.datetime.fromisoformat(v[:10])
        except ValueError:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d


def days_since(value, now_dt):
    d = parse_ts(value)
    if d is None:
        return None
    return (now_dt - d).days


def to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def truthy(value):
    return str(value).strip().lower() in ("true", "1", "yes")


def within_window(value, now_dt, window=WINDOW_DAYS):
    d = parse_ts(value)
    if d is None:
        return False
    delta = (now_dt - d).days
    return 0 <= delta < window


def in_range(value, start_date, end_date):
    """True if the date portion of value is within [start_date, end_date] inclusive."""
    d = parse_ts(value)
    if d is None:
        return False
    return start_date <= d.date() <= end_date


def median_or_none(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return round(statistics.median(vals), 1)


def contact_link(cid):
    return f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-1/{cid}"


def company_link(cid):
    return f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-2/{cid}"


def deal_link(did):
    return f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-3/{did}"


def md_link(text, url):
    text = (text or "").replace("|", "\\|").strip() or "(no name)"
    return f"[{text}]({url})"


def delta_str(current, prev):
    if prev is None:
        return f"{current}"
    diff = current - prev
    if diff > 0:
        return f"{current} (+{diff})"
    if diff < 0:
        return f"{current} ({diff})"
    return f"{current} (=)"


def money(value):
    if value is None:
        return "—"
    return f"${value:,.0f}"


def pct(numer, denom):
    if not denom:
        return "n/a"
    return f"{round(100.0 * numer / denom)}%"


# =========================================================================
# OWNER RESOLUTION
# =========================================================================

class Owners:
    def __init__(self, owner_map):
        self.map = {str(k): v for k, v in (owner_map or {}).items()}

    def name(self, owner_id):
        if owner_id is None or str(owner_id).strip() == "":
            return "Unassigned"
        return self.map.get(str(owner_id), f"Owner {owner_id}")

    def block(self, owner_id):
        """Which Section-5 block this record belongs to."""
        n = self.name(owner_id)
        return n if n in SALES_OWNERS else NICOLA


# =========================================================================
# LEAD STATUS
# =========================================================================

def label_for_status(raw_status, lead_status_map):
    if raw_status is None or str(raw_status).strip() == "":
        return None
    return lead_status_map.get(str(raw_status), str(raw_status))


def guard_lead_status_map(lead_status_map):
    """RAISE if any label used by the rules is missing from the live map."""
    have = set(lead_status_map.values())
    needed = set(SALES_WORKING_STATUSES)
    for labels in VALID_STATUS_BY_LIFECYCLE.values():
        needed.update(labels)
    missing = sorted(l for l in needed if l not in have)
    if missing:
        raise SystemExit(
            "GUARD FAILED: lead_status_map is missing label(s) required by the "
            f"rules: {missing}. Live labels present: {sorted(have)}"
        )


# =========================================================================
# DEAL CLASSIFICATION
# =========================================================================

def stage_label(stage):
    return DEALSTAGE_LABELS.get(str(stage), str(stage) if stage else "(no stage)")


def deal_is_won(d):
    return stage_label(d.get("dealstage")) == "Closed Won"


def deal_is_lost(d):
    return stage_label(d.get("dealstage")) == "Closed Lost"


def deal_is_closed(d):
    return deal_is_won(d) or deal_is_lost(d) or truthy(d.get("hs_is_closed"))


def deal_is_open(d):
    return not deal_is_closed(d)


def deal_usd(d):
    """USD value via amount_in_home_currency; None if empty/0 (to be flagged)."""
    v = to_float(d.get("amount_in_home_currency"))
    if v is None or v == 0:
        return None
    return v


# =========================================================================
# GROUPING
# =========================================================================

def group_by_owner(records, owners, key="owner_id"):
    out = {}
    for r in records:
        out.setdefault(owners.name(r.get(key)), []).append(r)
    return dict(sorted(out.items(), key=lambda kv: (-len(kv[1]), kv[0])))


# =========================================================================
# COMPUTE
# =========================================================================

def compute(raw, run_date):
    now_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)
    owners = Owners(raw.get("owners", {}))
    lsm = raw.get("lead_status_map", {})
    guard_lead_status_map(lsm)

    contacts = raw.get("contacts_b2b", [])
    sql_all = raw.get("sql_all_types", [])
    companies = raw.get("companies_b2b", [])
    deals = raw.get("deals_b2b", [])

    # Guards (spec: raise if a base is empty / obviously broken).
    if not contacts:
        raise SystemExit("GUARD FAILED: contacts_b2b is empty.")
    if not companies:
        raise SystemExit("GUARD FAILED: companies_b2b is empty.")
    if not deals:
        raise SystemExit("GUARD FAILED: deals_b2b is empty.")

    company_by_id = {str(c.get("id")): c for c in companies}

    # Attach computed label + helpers to each contact.
    for c in contacts:
        c["_label"] = label_for_status(c.get("hs_lead_status"), lsm)
        c["_owner_name"] = owners.name(c.get("owner_id"))
        try:
            c["_ndeals"] = int(float(c.get("num_associated_deals") or 0))
        except (TypeError, ValueError):
            c["_ndeals"] = 0
        c["_idle"] = days_since(c.get("notes_last_updated"), now_dt)
        c["_age"] = days_since(c.get("createdate"), now_dt)

    # ---- Deals: open rollup, stage buckets -------------------------------
    for d in deals:
        d["_owner_name"] = owners.name(d.get("owner_id"))
        d["_usd"] = deal_usd(d)
        d["_idle"] = days_since(d.get("notes_last_updated"), now_dt)
        d["_label"] = stage_label(d.get("dealstage"))
        d["_open"] = deal_is_open(d)

    open_deals = [d for d in deals if d["_open"]]

    # ======================= Q4 SCORECARD ================================
    q4_start = dt.date.fromisoformat(Q4_START)
    q4_end = dt.date.fromisoformat(Q4_END)
    q4_cap = min(run_date, q4_end)
    weeks_elapsed = (run_date - q4_start).days / 7.0

    new_deals = [d for d in deals if in_range(d.get("createdate"), q4_start, q4_cap)]
    entered_trial = [d for d in deals
                     if in_range(d.get("date_entered_trial"), q4_start, q4_cap)]
    closed_won = [d for d in deals
                  if deal_is_won(d) and in_range(d.get("closedate"), q4_start, q4_cap)]

    def expected(target):
        return round(target * min(weeks_elapsed, Q4_WEEKS) / Q4_WEEKS)

    def owner_contrib(dlist):
        counts = {}
        for d in dlist:
            counts[d["_owner_name"]] = counts.get(d["_owner_name"], 0) + 1
        return dict(sorted(counts.items(), key=lambda kv: -kv[1]))

    q4 = {
        "weeks_elapsed": round(weeks_elapsed, 1),
        "new_deals": {"actual": len(new_deals), "target": Q4_TARGETS["new_deals"],
                      "expected": expected(Q4_TARGETS["new_deals"]),
                      "by_owner": owner_contrib(new_deals)},
        "entered_trial": {"actual": len(entered_trial), "target": Q4_TARGETS["entered_trial"],
                          "expected": expected(Q4_TARGETS["entered_trial"]),
                          "by_owner": owner_contrib(entered_trial)},
        "closed_won": {"actual": len(closed_won), "target": Q4_TARGETS["closed_won"],
                       "expected": expected(Q4_TARGETS["closed_won"]),
                       "by_owner": owner_contrib(closed_won),
                       "usd": sum(d["_usd"] or 0 for d in closed_won)},
    }

    # ======================= SECTION 1 ===================================
    # 1.1 open pipeline by stage
    stage_rows = []
    open_qty = 0
    open_usd = 0.0
    for label in STAGE_ORDER:
        ds = [d for d in open_deals if d["_label"] == label]
        usd_sum = sum(d["_usd"] or 0 for d in ds)
        idle_vals = [d["_idle"] for d in ds]
        stage_rows.append({
            "stage": label, "qty": len(ds), "usd": usd_sum,
            "median_idle": median_or_none(idle_vals),
            "idle_14plus": sum(1 for v in idle_vals if v is not None and v >= OPEN_DEAL_IDLE_HEADLINE_DAYS),
        })
        open_qty += len(ds)
        open_usd += usd_sum
    # open deals in stages not in STAGE_ORDER (e.g. unknown/legacy open):
    other_open = [d for d in open_deals if d["_label"] not in STAGE_ORDER]
    if other_open:
        usd_sum = sum(d["_usd"] or 0 for d in other_open)
        idle_vals = [d["_idle"] for d in other_open]
        stage_rows.append({
            "stage": "(other open stages)", "qty": len(other_open), "usd": usd_sum,
            "median_idle": median_or_none(idle_vals),
            "idle_14plus": sum(1 for v in idle_vals if v is not None and v >= OPEN_DEAL_IDLE_HEADLINE_DAYS),
        })
        open_qty += len(other_open)
        open_usd += usd_sum

    closed_rows = []
    for label in CLOSED_LABELS:
        ds = [d for d in deals if d["_label"] == label]
        closed_rows.append({"stage": label, "qty": len(ds),
                            "usd": sum(d["_usd"] or 0 for d in ds)})

    # 1.2 open pipeline by owner
    owner_open = {}
    for d in open_deals:
        e = owner_open.setdefault(d["_owner_name"], {"qty": 0, "usd": 0.0})
        e["qty"] += 1
        e["usd"] += d["_usd"] or 0
    owner_open_rows = sorted(
        [{"owner": k, **v} for k, v in owner_open.items()],
        key=lambda r: -r["usd"])

    # concentration: largest single open deal + largest owner share
    biggest_deal = max(open_deals, key=lambda d: d["_usd"] or 0, default=None)
    concentration = None
    if open_usd > 0:
        parts = []
        if biggest_deal and (biggest_deal["_usd"] or 0) > 0:
            parts.append(("deal", biggest_deal, (biggest_deal["_usd"] or 0) / open_usd))
        if owner_open_rows:
            top = owner_open_rows[0]
            parts.append(("owner", top, top["usd"] / open_usd))
        concentration = parts

    # 1.3 updated / moved in last 7 days (stage moved vs updated-only)
    moved, updated = [], []
    prev_deal_stage = raw.get("_prev_deal_stage", {})  # injected later if prev snap
    for d in deals:
        entered_cur = within_window(d.get("hs_v2_date_entered_current_stage"), now_dt)
        prev_stage = prev_deal_stage.get(str(d.get("id")))
        stage_changed = prev_stage is not None and prev_stage != d.get("dealstage")
        if entered_cur or stage_changed:
            moved.append({
                "d": d,
                "prev": stage_label(prev_stage) if prev_stage else None,
            })
        elif within_window(d.get("hs_lastmodifieddate"), now_dt):
            updated.append({"d": d})

    # ======================= SECTION 2 ===================================
    # 2.1 new companies (7d) by owner
    new_companies = [c for c in companies if within_window(c.get("createdate"), now_dt)]
    new_companies_no_owner = [c for c in new_companies
                              if owners.name(c.get("owner_id")) == "Unassigned"]

    # 2.2 new contacts (7d) on existing companies vs new accounts
    new_contacts = [c for c in contacts if within_window(c.get("createdate"), now_dt)]
    new_on_existing, new_on_new_account, new_unknown_company = [], [], []
    for c in new_contacts:
        acid = c.get("associatedcompanyid")
        if acid is None or str(acid).strip() == "":
            continue  # orphan — handled in 3E
        comp = company_by_id.get(str(acid))
        if comp is None:
            new_unknown_company.append(c)
        elif within_window(comp.get("createdate"), now_dt):
            new_on_new_account.append(c)
        else:
            new_on_existing.append(c)

    # 2.3 SQL by sales owner (B2B contacts at SQL) — true SQL total (contacts only)
    sql_contacts = [c for c in contacts if c.get("lifecyclestage") == "salesqualifiedlead"]
    sql_by_owner = {}
    for c in sql_contacts:
        e = sql_by_owner.setdefault(c["_owner_name"], {"qty": 0, "u30": 0, "u60": 0})
        e["qty"] += 1
        idle = c["_idle"]
        if idle is None or idle >= IDLE_BUCKETS[0]:
            e["u30"] += 1
        if idle is None or idle >= IDLE_BUCKETS[1]:
            e["u60"] += 1
    sql_owner_rows = sorted([{"owner": k, **v} for k, v in sql_by_owner.items()],
                            key=lambda r: -r["qty"])
    sql_total = len(sql_contacts)
    sql_u30_total = sum(r["u30"] for r in sql_owner_rows)

    # ======================= SECTION 3 ===================================
    # 3A companies missing owner and/or name
    threeA = []
    for c in companies:
        no_owner = owners.name(c.get("owner_id")) == "Unassigned"
        no_name = c.get("name") is None or str(c.get("name")).strip() == ""
        if no_owner or no_name:
            threeA.append(c)

    # 3B deal hygiene (open deals)
    b_missing_usd = [d for d in open_deals if d["_usd"] is None]
    b_idle = [d for d in open_deals if d["_idle"] is not None and d["_idle"] >= DEAL_IDLE_DAYS]
    b_past_close = [d for d in open_deals
                    if parse_ts(d.get("closedate")) is not None
                    and parse_ts(d.get("closedate")).date() < run_date]
    b_no_owner = [d for d in open_deals if d["_owner_name"] == "Unassigned"]

    # 3C miscategorised leads
    threeC = [c for c in contacts
              if c["_label"] in SALES_WORKING_STATUSES
              and c.get("lifecyclestage") in ("subscriber", "lead", "marketingqualifiedlead")]

    # 3D status mismatches
    threeD = []
    for c in contacts:
        lc = c.get("lifecyclestage")
        if lc not in VALID_STATUS_BY_LIFECYCLE:
            continue  # 'other' / unknown: no defined expectation
        valid = VALID_STATUS_BY_LIFECYCLE[lc]
        label = c["_label"]
        reason = None
        expected_status = "/".join(valid)
        if label is None:
            if lc in SQL_OR_BEYOND:
                reason = "blank status"
        elif label not in valid:
            reason = "status not valid for lifecycle"
        # Opportunity deal-count rules
        if lc == "opportunity":
            if label == "In Progress" and c["_ndeals"] > 0:
                reason = "In Progress but has deal(s)"
                expected_status = "Qualified"
            elif label == "Qualified" and c["_ndeals"] == 0:
                reason = "Qualified but 0 deals"
                expected_status = "In Progress"
        if reason:
            threeD.append({"c": c, "reason": reason, "expected": expected_status})

    # 3E orphan new contacts (7d, no associated company)
    threeE = [c for c in new_contacts
              if c.get("associatedcompanyid") is None
              or str(c.get("associatedcompanyid")).strip() == ""]

    # 3F lost-deal leftovers: Opportunity contacts whose company's deals are ALL closed-lost
    deals_by_company = {}
    for d in deals:
        deals_by_company.setdefault(str(d.get("company_id")), []).append(d)
    threeF = []
    for c in contacts:
        if c.get("lifecyclestage") != "opportunity":
            continue
        acid = str(c.get("associatedcompanyid") or "")
        cdeals = deals_by_company.get(acid, [])
        if cdeals and all(deal_is_lost(d) for d in cdeals):
            threeF.append({"c": c, "ndeals": len(cdeals)})

    # 3G MQL follow-up: lead status label == MQL, 7+ days since entering MQL w/ no
    # sales activity since; plus MQLs with no owner.
    threeG = []
    for c in contacts:
        if c["_label"] != "MQL":
            continue
        entered = parse_ts(c.get("date_entered_mql"))
        age_mql = (now_dt - entered).days if entered else None
        last_contact = parse_ts(c.get("notes_last_contacted"))
        no_activity_since = (last_contact is None or
                             (entered is not None and last_contact <= entered))
        stale = age_mql is not None and age_mql >= MQL_FOLLOWUP_DAYS and no_activity_since
        no_owner = c["_owner_name"] == "Unassigned"
        if stale or no_owner:
            threeG.append({"c": c, "age_mql": age_mql, "no_owner": no_owner,
                           "stale": stale})

    # 3H won-but-not-customer: companies w/ a Closed Won deal whose type still Prospect
    won_company_ids = {str(d.get("company_id")) for d in deals if deal_is_won(d)}
    threeH = []
    for d in deals:
        if not deal_is_won(d):
            continue
        ctype = (d.get("company_type") or "").strip().upper()
        if ctype == "PROSPECT":
            threeH.append(d)
    # dedupe by company
    seen = set()
    threeH_u = []
    for d in threeH:
        cid = str(d.get("company_id"))
        if cid in seen:
            continue
        seen.add(cid)
        threeH_u.append(d)
    threeH = threeH_u

    # ======================= SECTION 4 — RECYCLER =========================
    def meets_trigger(c, created_key="createdate", idle_key="notes_last_updated"):
        """At SQL AND (last activity >90d OR (no activity ever AND created >90d))."""
        last = parse_ts(c.get(idle_key))
        if last is not None:
            return (now_dt - last).days > RECYCLE_DAYS
        created = parse_ts(c.get(created_key))
        return created is not None and (now_dt - created).days > RECYCLE_DAYS

    # 4A trigger match now (B2B SQL contacts)
    fourA = [c for c in sql_contacts if meets_trigger(c)]
    fourA_overdue = []
    for c in fourA:
        last = parse_ts(c.get("notes_last_updated"))
        base = last if last is not None else parse_ts(c.get("createdate"))
        days = (now_dt - base).days if base else None
        if days is not None and days > RECYCLE_DAYS + 1:
            fourA_overdue.append(c)
    # non-B2B SQLs meeting trigger (should be impossible → B2B filter broken)
    nonb2b_sql = [c for c in sql_all if c.get("customer_type") != "B2B"]
    fourA_nonb2b = [c for c in nonb2b_sql if meets_trigger(c)]

    # 4C recycled this week
    fourC = [c for c in contacts if within_window(c.get("recycled_date"), now_dt)]
    fourC_at_mql = [c for c in fourC if c.get("lifecyclestage") == "marketingqualifiedlead"]
    fourC_not_mql = [c for c in fourC if c.get("lifecyclestage") != "marketingqualifiedlead"]

    # 4D stuck recycles + re-promotions
    fourD_stuck, four_repromo = [], []
    for c in contacts:
        rec = parse_ts(c.get("recycled_date"))
        if rec is None:
            continue
        if c.get("lifecyclestage") != "salesqualifiedlead":
            continue
        dsql = parse_ts(c.get("date_entered_sql"))
        if dsql is not None and dsql < rec:
            fourD_stuck.append(c)
        elif dsql is not None and dsql > rec:
            four_repromo.append(c)

    # ======================= SECTION 6 — WORKFLOW CHECKS =================
    threeC_recent = [c for c in threeC
                     if within_window(c.get("createdate"), now_dt)
                     or within_window(c.get("hs_lastmodifieddate"), now_dt)]
    wf_contacted_early = [c for c in contacts
                          if c["_label"] == "Contacted"
                          and c.get("lifecyclestage") in ("subscriber", "lead", "marketingqualifiedlead")]
    wf_3d_opp_cust = [t for t in threeD
                      if t["c"].get("lifecyclestage") in ("opportunity", "customer")]
    wf_inprog_deals = [t for t in threeD if t["reason"] == "In Progress but has deal(s)"]
    new_open_deals = [d for d in open_deals if within_window(d.get("createdate"), now_dt)]
    wf_deal_fields = [d for d in new_open_deals if d["_usd"] is None]

    today_is_claim_run = (run_date.isoformat() == CLAIM_WINDOW_RUN)
    past_claim_deadline = run_date > dt.date.fromisoformat(CLAIM_DEADLINE)

    workflows = [
        {"name": "Sales Activity > Lead Status",
         "healthy_if": "3C contacts created/modified this week = 0",
         "ok": len(threeC_recent) == 0,
         "offenders": [("contact", c) for c in threeC_recent]},
        {"name": "Meeting Booked > Contacted",
         "healthy_if": 'no "Contacted" at Email Contact / Lead / MQL',
         "ok": len(wf_contacted_early) == 0,
         "offenders": [("contact", c) for c in wf_contacted_early]},
        {"name": "Stale SQL Recycler",
         "healthy_if": "4A overdue = 0 (after claim window) and 4D stuck = 0",
         "ok": (len(fourA_overdue) == 0 and len(fourD_stuck) == 0),
         "offenders": [("contact", c) for c in fourA_overdue]
                      + [("contact", c) for c in fourD_stuck]},
        {"name": "Lifecycle > Lead Status Sync",
         "healthy_if": "3D rows at Opportunity/Customer = 0",
         "ok": len(wf_3d_opp_cust) == 0,
         "offenders": [("contact", t["c"]) for t in wf_3d_opp_cust]},
        {"name": "Deal Associated > Qualified",
         "healthy_if": "Opportunity + In Progress with deals > 0 = 0",
         "ok": len(wf_inprog_deals) == 0,
         "offenders": [("contact", t["c"]) for t in wf_inprog_deals]},
        {"name": "Deal required fields",
         "healthy_if": "open deals CREATED this week with empty/zero USD = 0",
         "ok": len(wf_deal_fields) == 0,
         "offenders": [("deal", d) for d in wf_deal_fields]},
    ]

    # ======================= HEADLINE METRICS ============================
    open_idle_14 = sum(1 for d in open_deals
                       if d["_idle"] is not None and d["_idle"] >= OPEN_DEAL_IDLE_HEADLINE_DAYS)
    sql_untouched_30 = sum(1 for c in sql_contacts
                           if c["_idle"] is None or c["_idle"] >= SQL_UNTOUCHED_HEADLINE_DAYS)
    wf_warn = sum(1 for w in workflows if not w["ok"])

    metrics = {
        "contacts_b2b_total": len(contacts),
        "sql_total": sql_total,
        "open_deal_qty": open_qty,
        "open_deal_usd": round(open_usd, 2),
        "stage_moved": len(moved),
        "pct_open_idle_14": round(100.0 * open_idle_14 / open_qty) if open_qty else 0,
        "open_idle_14": open_idle_14,
        "pct_sql_untouched_30": round(100.0 * sql_untouched_30 / sql_total) if sql_total else 0,
        "sql_untouched_30": sql_untouched_30,
        "miscategorised": len(threeC),
        "status_mismatch": len(threeD),
        "stuck_recycles": len(fourD_stuck),
        "deals_past_close": len(b_past_close),
        "deals_missing_usd": len(b_missing_usd),
        "new_accounts_no_owner": len(new_companies_no_owner),
        "recycled_this_week": len(fourC),
        "orphans": len(threeE),
        "workflows_warn": wf_warn,
        "q4_new_deals": q4["new_deals"]["actual"],
        "q4_entered_trial": q4["entered_trial"]["actual"],
        "q4_closed_won": q4["closed_won"]["actual"],
        "wf_counts": {w["name"]: (0 if w["ok"] else len(w["offenders"])) for w in workflows},
    }

    return {
        "run_date": run_date.isoformat(),
        "owners": owners, "lsm": lsm, "now_dt": now_dt,
        "q4": q4,
        "stage_rows": stage_rows, "closed_rows": closed_rows,
        "open_qty": open_qty, "open_usd": open_usd,
        "owner_open_rows": owner_open_rows, "concentration": concentration,
        "biggest_deal": biggest_deal,
        "moved": moved, "updated": updated,
        "new_companies": new_companies, "new_companies_no_owner": new_companies_no_owner,
        "new_on_existing": new_on_existing, "new_on_new_account": new_on_new_account,
        "new_unknown_company": new_unknown_company,
        "sql_contacts": sql_contacts, "sql_owner_rows": sql_owner_rows,
        "sql_total": sql_total, "sql_u30_total": sql_u30_total,
        "threeA": threeA, "b_missing_usd": b_missing_usd, "b_idle": b_idle,
        "b_past_close": b_past_close, "b_no_owner": b_no_owner,
        "threeC": threeC, "threeD": threeD, "threeE": threeE, "threeF": threeF,
        "threeG": threeG, "threeH": threeH,
        "fourA": fourA, "fourA_overdue": fourA_overdue, "fourA_nonb2b": fourA_nonb2b,
        "fourC": fourC, "fourC_at_mql": fourC_at_mql, "fourC_not_mql": fourC_not_mql,
        "fourD_stuck": fourD_stuck, "four_repromo": four_repromo,
        "workflows": workflows,
        "today_is_claim_run": today_is_claim_run,
        "past_claim_deadline": past_claim_deadline,
        "metrics": metrics,
        "deals": deals, "open_deals": open_deals, "contacts": contacts,
        "companies": companies,
    }


# =========================================================================
# PREVIOUS SNAPSHOT / DELTAS
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
        snap = json.load(f)
    snap["_file_date"] = cands[-1][0].isoformat()
    return snap


def prev_metric(prev, key):
    if not prev or prev.get("schema") != "oct2026":
        return None
    return prev.get("metrics", {}).get(key)


# =========================================================================
# RENDER — MAIN REPORT
# =========================================================================

def flag_icon(ok):
    return "✅" if ok else "⚠️"


def render_report(R, prev):
    o = R["owners"]
    out = []
    A = out.append
    m = R["metrics"]
    comparable = bool(prev and prev.get("schema") == "oct2026")

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {R['run_date']}")
    A("")
    A("*Scope: B2B only. Contact counts from the CONTACT object; pipeline value "
      "from DEALS rolled up at company level. USD = `amount_in_home_currency`.*")
    if comparable:
        A(f"*Week-on-week deltas vs previous snapshot (`{prev['_file_date']}`).*")
    else:
        A("*No comparable prior snapshot (new report format) — week-on-week "
          "comparison starts next run.*")
    A("")

    # ---------------- Q4 SCORECARD ----------------
    q4 = R["q4"]
    A("## Q4 Scorecard")
    A("")
    A(f"*Q4 {Q4_START} → {Q4_END} · {q4['weeks_elapsed']} of {Q4_WEEKS} weeks "
      f"elapsed. B2B deals.*")
    A("")
    A("| Metric | Definition | Actual | Target | Expected by now | Status |")
    A("|--------|------------|--------|--------|-----------------|--------|")
    def q4row(key, defn):
        s = q4[key]
        status = flag_icon(s["actual"] >= s["expected"])
        pv = prev_metric(prev, f"q4_{key}")
        act = delta_str(s["actual"], pv) if comparable else str(s["actual"])
        A(f"| {key.replace('_',' ').title()} | {defn} | {act} | {s['target']} "
          f"| {s['expected']} | {status} |")
    q4row("new_deals", "deals created in Q4")
    q4row("entered_trial", "deals that entered Discovery & Trial in Q4 (cumulative)")
    q4row("closed_won", "closedwon with close date in Q4")
    A("")
    def contrib_line(label, key):
        by = q4[key]["by_owner"]
        if not by:
            A(f"- **{label}:** none yet.")
        else:
            parts = ", ".join(f"{k} {v}" for k, v in by.items())
            A(f"- **{label} by owner:** {parts}")
    contrib_line("New deals", "new_deals")
    contrib_line("Entered trial", "entered_trial")
    contrib_line("Closed won", "closed_won")
    if q4["closed_won"]["usd"]:
        A(f"- **Closed-won USD (Q4):** {money(q4['closed_won']['usd'])}")
    A("")

    # ---------------- HEADLINE FLAGS ----------------
    A("## Headline Flags")
    A("")
    def hl(label, key, suffix=""):
        cur = m[key]
        pv = prev_metric(prev, key)
        val = delta_str(cur, pv) if comparable else str(cur)
        A(f"- **{label}:** {val}{suffix}")
    wf_summary = " ".join(f"{w['name']}={flag_icon(w['ok'])}" for w in R["workflows"])
    A(f"- **Workflow checks:** {m['workflows_warn']} ⚠️ of {len(R['workflows'])} — {wf_summary}")
    hl("Deals that changed stage this week", "stage_moved")
    A(f"- **Open deals idle {OPEN_DEAL_IDLE_HEADLINE_DAYS}+d:** "
      f"{m['pct_open_idle_14']}% ({m['open_idle_14']} of {R['open_qty']})")
    A(f"- **SQLs untouched {SQL_UNTOUCHED_HEADLINE_DAYS}+d:** "
      f"{m['pct_sql_untouched_30']}% ({m['sql_untouched_30']} of {R['sql_total']})")
    hl("Miscategorised leads (3C)", "miscategorised")
    hl("Status mismatches (3D)", "status_mismatch")
    hl("Stuck recycles (4D)", "stuck_recycles")
    hl("Deals past close date (3B iii)", "deals_past_close")
    hl("Open deals missing/zero USD (3B i)", "deals_missing_usd")
    hl("New accounts with no owner", "new_accounts_no_owner")
    hl("Recycled this week (4C)", "recycled_this_week")
    hl("Orphan new contacts (3E)", "orphans")
    if R["fourA_nonb2b"]:
        A(f"- **⚠️ Non-B2B SQLs meeting recycle trigger:** {len(R['fourA_nonb2b'])} "
          "(B2B filter may be broken — see 4A)")
    A("")

    # ---------------- SECTION 1 ----------------
    A("## Section 1 — Current Pipeline Status")
    A("")
    A("### 1.1 Open pipeline by stage")
    A("")
    A("| Stage | Deals | Open USD | Median days idle | Idle ≥14d |")
    A("|-------|-------|----------|------------------|-----------|")
    for r in R["stage_rows"]:
        mi = r["median_idle"] if r["median_idle"] is not None else "—"
        A(f"| {r['stage']} | {r['qty']} | {money(r['usd'])} | {mi} | {r['idle_14plus']} |")
    A(f"| **Open subtotal** | **{R['open_qty']}** | **{money(R['open_usd'])}** | | |")
    A("")
    A("*Reference only (not in open subtotal):*")
    A("")
    A("| Stage | Deals | USD |")
    A("|-------|-------|-----|")
    for r in R["closed_rows"]:
        A(f"| {r['stage']} | {r['qty']} | {money(r['usd'])} |")
    A("")

    A("### 1.2 Open pipeline by sales owner")
    A("")
    A("| Owner | Open deals | Open USD |")
    A("|-------|-----------|----------|")
    for r in R["owner_open_rows"]:
        A(f"| {r['owner']} | {r['qty']} | {money(r['usd'])} |")
    A("")
    if R["concentration"]:
        for kind, obj, share in R["concentration"]:
            if kind == "deal":
                A(f"- **Concentration:** largest single open deal "
                  f"{md_link(obj.get('dealname'), deal_link(obj.get('id')))} "
                  f"({money(obj['_usd'])}) = {round(share*100)}% of open value.")
            else:
                A(f"- **Concentration:** top owner {obj['owner']} holds "
                  f"{money(obj['usd'])} = {round(share*100)}% of open value.")
    A("")

    A("### 1.3 Updated / moved in the last 7 days")
    A("")
    if not R["moved"] and not R["updated"]:
        A("- Nothing moved or updated in the window.")
        A("")
    if R["moved"]:
        A(f"**Stage moved ({len(R['moved'])})** — real progression:")
        A("")
        A("| Deal | Owner | Previous → Current | USD | Last activity |")
        A("|------|-------|--------------------|-----|---------------|")
        for item in sorted(R["moved"], key=lambda x: -(x["d"]["_usd"] or 0)):
            d = item["d"]
            prev_lbl = item["prev"] or "(unknown)"
            la = (d.get("notes_last_updated") or "—")[:10]
            A(f"| {md_link(d.get('dealname'), deal_link(d.get('id')))} | {d['_owner_name']} "
              f"| {prev_lbl} → {d['_label']} | {money(d['_usd'])} | {la} |")
        A("")
    if R["updated"]:
        A(f"**Updated only ({len(R['updated'])})** — field changes, no stage move:")
        A("")
        A("| Deal | Owner | Stage | USD | Last activity |")
        A("|------|-------|-------|-----|---------------|")
        for item in sorted(R["updated"], key=lambda x: -(x["d"]["_usd"] or 0)):
            d = item["d"]
            la = (d.get("notes_last_updated") or "—")[:10]
            A(f"| {md_link(d.get('dealname'), deal_link(d.get('id')))} | {d['_owner_name']} "
              f"| {d['_label']} | {money(d['_usd'])} | {la} |")
        A("")

    # ---------------- SECTION 2 ----------------
    A("## Section 2 — New Pipeline Status")
    A("")
    A("### 2.1 New companies created in last 7 days")
    A("")
    if not R["new_companies"]:
        A("- None this week.")
    else:
        for owner, rows in group_by_owner(R["new_companies"], o).items():
            A(f"**{owner}** ({len(rows)})")
            A("")
            A("| Company | Created | Owner? |")
            A("|---------|---------|--------|")
            for c in rows:
                owflag = "⚠️ none" if o.name(c.get("owner_id")) == "Unassigned" else "ok"
                A(f"| {md_link(c.get('name'), company_link(c.get('id')))} "
                  f"| {(c.get('createdate') or '—')[:10]} | {owflag} |")
            A("")
    if R["new_companies_no_owner"]:
        A(f"- ⚠️ {len(R['new_companies_no_owner'])} new company(ies) created with no owner.")
    A("")
    A("### 2.2 New contacts on existing companies (last 7 days)")
    A("")
    A(f"- New contacts on **new accounts** (company also created this week): "
      f"{len(R['new_on_new_account'])}")
    A(f"- New contacts on **existing accounts**: {len(R['new_on_existing'])}")
    if R["new_unknown_company"]:
        A(f"- New contacts on a company not in the B2B set (age unknown): "
          f"{len(R['new_unknown_company'])}")
    A("")
    if R["new_on_existing"]:
        for owner, rows in group_by_owner(R["new_on_existing"], o).items():
            A(f"**{owner}** ({len(rows)})")
            A("")
            A("| Contact | Lifecycle | Created |")
            A("|---------|-----------|---------|")
            for c in sorted(rows, key=lambda c: c.get("lifecyclestage") or ""):
                A(f"| {md_link(c.get('name'), contact_link(c.get('id')))} "
                  f"| {LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage'))} "
                  f"| {(c.get('createdate') or '—')[:10]} |")
            A("")
    A("### 2.3 SQLs by sales owner (contacts only — true SQL total)")
    A("")
    A("| Owner | SQLs | Untouched 30+d | Untouched 60+d |")
    A("|-------|------|----------------|----------------|")
    for r in R["sql_owner_rows"]:
        A(f"| {r['owner']} | {r['qty']} | {r['u30']} | {r['u60']} |")
    tot_u60 = sum(r["u60"] for r in R["sql_owner_rows"])
    pv_sql = prev_metric(prev, "sql_total")
    A(f"| **Total** | **{delta_str(R['sql_total'], pv_sql) if comparable else R['sql_total']}** "
      f"| **{R['sql_u30_total']}** | **{tot_u60}** |")
    A("")

    # ---------------- SECTION 3 ----------------
    A("## Section 3 — Admin / Data-Hygiene Worklist")
    A("")
    # 3A
    A(f"### 3A Companies to complete ({len(R['threeA'])}) — owner: Nicola")
    A("")
    if not R["threeA"]:
        A("- ✅ None.")
    else:
        A("| Company | Created | Owner | Name | Source | Created by |")
        A("|---------|---------|-------|------|--------|-----------|")
        for c in R["threeA"]:
            nm = c.get("name")
            name_disp = md_link(nm, company_link(c.get("id"))) if nm else \
                f"[(blank)]({company_link(c.get('id'))})"
            owner_disp = o.name(c.get("owner_id"))
            A(f"| {name_disp} | {(c.get('createdate') or '—')[:10]} "
              f"| {owner_disp} | {'blank' if not nm else 'ok'} "
              f"| {c.get('original_source') or '—'} | {c.get('created_by') or '—'} |")
    A("")
    # 3B
    A("### 3B Deal hygiene (open B2B deals)")
    A("")
    def deal_table(title, rows):
        A(f"**{title} ({len(rows)})**")
        A("")
        if not rows:
            A("- ✅ None.")
            A("")
            return
        A("| Deal | Stage | Owner | USD | Last activity | Close date |")
        A("|------|-------|-------|-----|---------------|-----------|")
        for d in sorted(rows, key=lambda d: -(d["_usd"] or 0)):
            raw_ref = ""
            if d["_usd"] is None:
                rawamt = to_float(d.get("amount"))
                raw_ref = f" (raw {rawamt:,.0f} {d.get('deal_currency_code') or ''})".rstrip() if rawamt is not None else ""
            A(f"| {md_link(d.get('dealname'), deal_link(d.get('id')))} | {d['_label']} "
              f"| {d['_owner_name']} | {money(d['_usd'])}{raw_ref} "
              f"| {(d.get('notes_last_updated') or '—')[:10]} "
              f"| {(d.get('closedate') or '—')[:10]} |")
        A("")
    deal_table("(i) USD empty or zero", R["b_missing_usd"])
    deal_table(f"(ii) Idle {DEAL_IDLE_DAYS}+ days", R["b_idle"])
    deal_table("(iii) Close date in the past", R["b_past_close"])
    deal_table("(iv) No deal owner", R["b_no_owner"])
    # 3C
    A(f"### 3C Miscategorised leads ({len(R['threeC'])})")
    A("")
    A("*Sales working status (Attempted to Contact / Contacted) but lifecycle still "
      "Subscriber / Lead / MQL — should be SQL+.*")
    A("")
    render_contact_worklist(A, R["threeC"], o, extra_cols=["lifecycle", "status"])
    # 3D
    A(f"### 3D Status mismatches ({len(R['threeD'])})")
    A("")
    A("*Lead status not valid for lifecycle (incl. blank at SQL+), plus Opportunity "
      "deal-count rule breaks.*")
    A("")
    if not R["threeD"]:
        A("- ✅ None.")
        A("")
    else:
        # group by owner
        by_owner = {}
        for t in R["threeD"]:
            by_owner.setdefault(t["c"]["_owner_name"], []).append(t)
        for owner, rows in sorted(by_owner.items(), key=lambda kv: -len(kv[1])):
            A(f"**{owner}** ({len(rows)})")
            A("")
            A("| Contact | Lifecycle | Current status | Expected | Reason |")
            A("|---------|-----------|----------------|----------|--------|")
            for t in rows:
                c = t["c"]
                A(f"| {md_link(c.get('name'), contact_link(c.get('id')))} "
                  f"| {LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage'))} "
                  f"| {c['_label'] or '(blank)'} | {t['expected']} | {t['reason']} |")
            A("")
    # 3E
    A(f"### 3E Orphan contacts — new (7d), no company ({len(R['threeE'])})")
    A("")
    render_contact_worklist(A, R["threeE"], o, extra_cols=["lifecycle"])
    # 3F
    A(f"### 3F Lost-deal leftovers ({len(R['threeF'])})")
    A("")
    A("*Opportunity contacts whose associated company's deals are ALL closed-lost "
      "(company-level rollup). Review: recycle to MQL + Unqualified \"Lost deal\" (rule TBC).*")
    A("")
    if not R["threeF"]:
        A("- ✅ None.")
        A("")
    else:
        by_owner = {}
        for t in R["threeF"]:
            by_owner.setdefault(t["c"]["_owner_name"], []).append(t)
        for owner, rows in sorted(by_owner.items(), key=lambda kv: -len(kv[1])):
            A(f"**{owner}** ({len(rows)})")
            A("")
            A("| Contact | Company deals (all lost) |")
            A("|---------|--------------------------|")
            for t in rows:
                c = t["c"]
                A(f"| {md_link(c.get('name'), contact_link(c.get('id')))} | {t['ndeals']} |")
            A("")
    # 3G
    A(f"### 3G MQL follow-up ({len(R['threeG'])})")
    A("")
    A(f"*Contacts at lead status MQL with no sales activity for {MQL_FOLLOWUP_DAYS}+ days "
      "since entering MQL, plus MQLs with no owner.*")
    A("")
    if not R["threeG"]:
        A("- ✅ None.")
        A("")
    else:
        by_owner = {}
        for t in R["threeG"]:
            by_owner.setdefault(t["c"]["_owner_name"], []).append(t)
        for owner, rows in sorted(by_owner.items(), key=lambda kv: -len(kv[1])):
            A(f"**{owner}** ({len(rows)})")
            A("")
            A("| Contact | Days at MQL | No owner? |")
            A("|---------|-------------|-----------|")
            for t in rows:
                c = t["c"]
                A(f"| {md_link(c.get('name'), contact_link(c.get('id')))} "
                  f"| {t['age_mql'] if t['age_mql'] is not None else '—'} "
                  f"| {'yes' if t['no_owner'] else ''} |")
            A("")
    # 3H
    A(f"### 3H Won but not Customer ({len(R['threeH'])})")
    A("")
    A("*Companies with a Closed Won deal whose company type is still Prospect.*")
    A("")
    if not R["threeH"]:
        A("- ✅ None.")
    else:
        A("| Company | Won deal | USD |")
        A("|---------|----------|-----|")
        for d in R["threeH"]:
            comp = md_link(d.get("company_name"), company_link(d.get("company_id")))
            A(f"| {comp} | {md_link(d.get('dealname'), deal_link(d.get('id')))} "
              f"| {money(d['_usd'])} |")
    A("")

    # ---------------- SECTION 4 ----------------
    A("## Section 4 — Recycler Health Check")
    A("")
    A("*Live recycler: B2B contact at SQL AND (last activity > 90d OR (no activity ever "
      "AND created > 90d)) → set MQL + Unqualified + recycled_date + enrol nurture. "
      "This section diagnoses; it does not change the workflow.*")
    A("")
    # 4A
    A(f"### 4A Trigger match now ({len(R['fourA'])})")
    A("")
    if R["past_claim_deadline"]:
        A(f"*Claim window closed ({CLAIM_WINDOW_RUN}). ALERT on matches overdue "
          "(>1 day past threshold): the recycler is not firing.*")
        A("")
        A(f"- Matches meeting trigger: **{len(R['fourA'])}**")
        A(f"- **{flag_icon(len(R['fourA_overdue'])==0)} Overdue matches (recycler should "
          f"have fired): {len(R['fourA_overdue'])}**")
    else:
        A("*Within the claim window — this is the held backlog (info).*")
        A("")
        A(f"- Held backlog: **{len(R['fourA'])}**")
    A("")
    if R["fourA_nonb2b"]:
        A(f"- **⚠️ ALERT: {len(R['fourA_nonb2b'])} NON-B2B SQL(s) meet the trigger — "
          "the B2B filter may be broken.**")
        A("")
    if R["fourA_overdue"]:
        by_owner = group_by_owner(R["fourA_overdue"], o)
        for owner, rows in by_owner.items():
            A(f"**{owner}** ({len(rows)})")
            A("")
            A("| Contact | Last activity | Days idle |")
            A("|---------|---------------|-----------|")
            for c in rows:
                A(f"| {md_link(c.get('name'), contact_link(c.get('id')))} "
                  f"| {(c.get('notes_last_updated') or 'never')[:10]} "
                  f"| {c['_idle'] if c['_idle'] is not None else '(no activity)'} |")
            A("")
    # 4C
    A(f"### 4C Recycled this week ({len(R['fourC'])})")
    A("")
    if not R["fourC"]:
        A("- None recycled in the window.")
    else:
        A(f"- Now at MQL: {len(R['fourC_at_mql'])} ✅")
        if R["fourC_not_mql"]:
            A(f"- ⚠️ Still NOT at MQL (see 4D): {len(R['fourC_not_mql'])}")
        A("")
        for owner, rows in group_by_owner(R["fourC"], o).items():
            A(f"**{owner}** ({len(rows)})")
            A("")
            A("| Contact | Recycled | Lifecycle now |")
            A("|---------|----------|---------------|")
            for c in rows:
                A(f"| {md_link(c.get('name'), contact_link(c.get('id')))} "
                  f"| {(c.get('recycled_date') or '—')[:10]} "
                  f"| {LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage'))} |")
            A("")
    # 4D
    A(f"### 4D Stuck recycles ({len(R['fourD_stuck'])}) — ALERT")
    A("")
    A("*recycled_date set AND lifecycle still SQL AND date_entered_sql < recycled_date "
      "(the move to MQL failed).*")
    A("")
    if not R["fourD_stuck"]:
        A("- ✅ None.")
        A("")
    else:
        for owner, rows in group_by_owner(R["fourD_stuck"], o).items():
            A(f"**{owner}** ({len(rows)})")
            A("")
            A("| Contact | Recycled | Entered SQL |")
            A("|---------|----------|-------------|")
            for c in rows:
                A(f"| {md_link(c.get('name'), contact_link(c.get('id')))} "
                  f"| {(c.get('recycled_date') or '—')[:10]} "
                  f"| {(c.get('date_entered_sql') or '—')[:10]} |")
            A("")
    if R["four_repromo"]:
        A(f"**Re-promotions (info only, {len(R['four_repromo'])})** — recycled then sales "
          "re-engaged (entered SQL after recycled_date):")
        A("")
        A("| Contact | Owner |")
        A("|---------|-------|")
        for c in R["four_repromo"]:
            A(f"| {md_link(c.get('name'), contact_link(c.get('id')))} | {c['_owner_name']} |")
        A("")

    # ---------------- SECTION 5 ----------------
    A("## Section 5 — Owner Action Lists")
    A("")
    for blk in build_owner_blocks(R, prev, comparable):
        out.extend(blk["lines"])
        A("")

    # ---------------- SECTION 6 ----------------
    A("## Section 6 — Workflow Firing Checks (built ≠ working)")
    A("")
    A("| Workflow | Healthy if | Status |")
    A("|----------|------------|--------|")
    for w in R["workflows"]:
        A(f"| {w['name']} | {w['healthy_if']} | {flag_icon(w['ok'])} |")
    A("")
    for w in R["workflows"]:
        if w["ok"]:
            continue
        A(f"**⚠️ {w['name']} — {len(w['offenders'])} offending record(s):**")
        A("")
        shown = w["offenders"][:50]
        for kind, rec in shown:
            if kind == "contact":
                A(f"- {md_link(rec.get('name'), contact_link(rec.get('id')))} "
                  f"({LIFECYCLE_LABELS.get(rec.get('lifecyclestage'), rec.get('lifecyclestage'))}"
                  f" / {rec.get('_label') or '(blank)'})")
            else:
                A(f"- {md_link(rec.get('dealname'), deal_link(rec.get('id')))} ({rec.get('_label')})")
        if len(w["offenders"]) > len(shown):
            A(f"- …and {len(w['offenders']) - len(shown)} more.")
        A("")

    A("---")
    A(f"*Snapshot: `snapshots/{R['run_date']}.json` · Report: `reports/{R['run_date']}.md`*")
    A("")
    return "\n".join(out)


def render_contact_worklist(A, rows, owners, extra_cols=None):
    extra_cols = extra_cols or []
    if not rows:
        A("- ✅ None.")
        A("")
        return
    for owner, group in group_by_owner(rows, owners).items():
        A(f"**{owner}** ({len(group)})")
        A("")
        header = "| Contact |"
        sep = "|---------|"
        if "lifecycle" in extra_cols:
            header += " Lifecycle |"
            sep += "-----------|"
        if "status" in extra_cols:
            header += " Lead status |"
            sep += "-------------|"
        A(header)
        A(sep)
        for c in group:
            line = f"| {md_link(c.get('name'), contact_link(c.get('id')))} |"
            if "lifecycle" in extra_cols:
                line += f" {LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage'))} |"
            if "status" in extra_cols:
                line += f" {c.get('_label') or '(blank)'} |"
            A(line)
        A("")


# =========================================================================
# OWNER BLOCKS (Section 5)
# =========================================================================

def build_owner_blocks(R, prev, comparable):
    o = R["owners"]
    now_dt = R["now_dt"]

    # prev contact lifecycle/status for 5a diff
    prev_contacts = {}
    if comparable:
        prev_contacts = prev.get("contact_states", {})

    # Recycling warning (b): their SQLs with last activity 75..89d ago, or no
    # activity and created 75..89d ago.
    def in_warn_window(c):
        last = parse_ts(c.get("notes_last_updated"))
        if last is not None:
            d = (now_dt - last).days
            return RECYCLE_WARN_DAYS <= d < RECYCLE_DAYS
        created = parse_ts(c.get("createdate"))
        if created is None:
            return False
        d = (now_dt - created).days
        return RECYCLE_WARN_DAYS <= d < RECYCLE_DAYS

    warn_sql = [c for c in R["sql_contacts"] if in_warn_window(c)]

    # changed-this-week (a)
    changed = []
    for c in R["contacts"]:
        ps = prev_contacts.get(str(c.get("id")))
        if not ps:
            continue
        old_lc, old_ls = ps.get("lifecyclestage"), ps.get("hs_lead_status")
        if old_lc != c.get("lifecyclestage") or old_ls != c.get("hs_lead_status"):
            changed.append({"c": c, "old_lc": old_lc, "old_ls": old_ls})

    # Assemble per-block membership
    def owns(rec, key="owner_id"):
        return o.block(rec.get(key))

    blocks_order = SALES_OWNERS + [NICOLA]
    blocks = {name: {"a": [], "b": [], "d": [], "e": [], "f": []} for name in blocks_order}

    for item in changed:
        blocks[o.block(item["c"].get("owner_id"))]["a"].append(item)
    for c in warn_sql:
        blocks[o.block(c.get("owner_id"))]["b"].append(c)
    for c in R["fourC"]:
        blocks[o.block(c.get("owner_id"))]["d"].append(c)
    for d in (R["b_missing_usd"] + R["b_idle"] + R["b_past_close"]):
        blocks[o.block(d.get("owner_id"))]["e"].append(d)
    # dedupe e by deal id (a deal can be in multiple 3B lists)
    for name in blocks:
        seen = set()
        uniq = []
        for d in blocks[name]["e"]:
            if d.get("id") in seen:
                continue
            seen.add(d.get("id"))
            uniq.append(d)
        blocks[name]["e"] = uniq
    # f: data fixes 3C, 3D, 3E, 4D
    for c in R["threeC"]:
        blocks[o.block(c.get("owner_id"))]["f"].append(("3C miscategorised", c))
    for t in R["threeD"]:
        blocks[o.block(t["c"].get("owner_id"))]["f"].append(("3D status mismatch", t["c"]))
    for c in R["threeE"]:
        blocks[o.block(c.get("owner_id"))]["f"].append(("3E orphan", c))
    for c in R["fourD_stuck"]:
        blocks[o.block(c.get("owner_id"))]["f"].append(("4D stuck recycle", c))

    results = []
    for name in blocks_order:
        lines = render_one_owner_block(name, blocks[name], R, comparable)
        results.append({"owner": name, "lines": lines})
    return results


def render_one_owner_block(name, blk, R, comparable):
    o = R["owners"]
    out = []
    A = out.append
    is_nicola = (name == NICOLA)

    body = []
    B = body.append

    # a
    if blk["a"]:
        B(f"**a. Your records changed this week ({len(blk['a'])})** — review; flag anything wrong.")
        B("")
        B("| Contact | Lifecycle (old → new) | Status (old → new) |")
        B("|---------|-----------------------|--------------------|")
        for item in blk["a"]:
            c = item["c"]
            lc = f"{LIFECYCLE_LABELS.get(item['old_lc'], item['old_lc'])} → {LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage'))}"
            ls = f"{item['old_ls'] or '(blank)'} → {c.get('hs_lead_status') or '(blank)'}"
            B(f"| {md_link(c.get('name'), contact_link(c.get('id')))} | {lc} | {ls} |")
        B("")
    # b
    if blk["b"]:
        B(f"**b. Recycling in the next 15 days ({len(blk['b'])})** — log activity now or "
          "they return to marketing.")
        B("")
        B("| Contact | Last activity | Days idle |")
        B("|---------|---------------|-----------|")
        for c in blk["b"]:
            B(f"| {md_link(c.get('name'), contact_link(c.get('id')))} "
              f"| {(c.get('notes_last_updated') or 'never')[:10]} "
              f"| {c['_idle'] if c['_idle'] is not None else '(no activity, by create date)'} |")
        B("")
    # c (claim window) — only on CLAIM_WINDOW_RUN
    if R["today_is_claim_run"]:
        claim = [c for c in R["fourA"] if o.block(c.get("owner_id")) == name]
        if claim:
            B(f"**c. CLAIM WINDOW ({len(claim)})** — to keep a contact, log an activity "
              f"(call, email or note) by Fri 16 Oct. Any still without activity after that "
              "will be recycled to marketing.")
            B("")
            B("| Contact | Last activity |")
            B("|---------|---------------|")
            for c in claim:
                B(f"| {md_link(c.get('name'), contact_link(c.get('id')))} "
                  f"| {(c.get('notes_last_updated') or 'never')[:10]} |")
            B("")
    # d
    if blk["d"]:
        B(f"**d. Recycled this week ({len(blk['d'])})**")
        B("")
        B("| Contact | Recycled | Lifecycle now |")
        B("|---------|----------|---------------|")
        for c in blk["d"]:
            B(f"| {md_link(c.get('name'), contact_link(c.get('id')))} "
              f"| {(c.get('recycled_date') or '—')[:10]} "
              f"| {LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage'))} |")
        B("")
    # e
    if blk["e"]:
        B(f"**e. Deals needing attention ({len(blk['e'])})**")
        B("")
        B("| Deal | Stage | USD | Last activity | Close date |")
        B("|------|-------|-----|---------------|-----------|")
        for d in sorted(blk["e"], key=lambda d: -(d["_usd"] or 0)):
            B(f"| {md_link(d.get('dealname'), deal_link(d.get('id')))} | {d['_label']} "
              f"| {money(d['_usd'])} | {(d.get('notes_last_updated') or '—')[:10]} "
              f"| {(d.get('closedate') or '—')[:10]} |")
        B("")
    # f
    if blk["f"]:
        B(f"**f. Data fixes ({len(blk['f'])})**")
        B("")
        B("| Issue | Contact |")
        B("|-------|---------|")
        for tag, c in blk["f"]:
            B(f"| {tag} | {md_link(c.get('name'), contact_link(c.get('id')))} |")
        B("")

    # Nicola-only extras
    nic_extra = []
    if is_nicola:
        N = nic_extra.append
        if R["threeA"]:
            N(f"**Companies to complete — 3A ({len(R['threeA'])})**")
            N("")
            N("| Company | Owner | Name |")
            N("|---------|-------|------|")
            for c in R["threeA"]:
                nm = c.get("name")
                name_disp = md_link(nm, company_link(c.get("id"))) if nm else \
                    f"[(blank)]({company_link(c.get('id'))})"
                N(f"| {name_disp} | {o.name(c.get('owner_id'))} | {'blank' if not nm else 'ok'} |")
            N("")
        if R["threeH"]:
            N(f"**Won but not Customer — 3H ({len(R['threeH'])})**")
            N("")
            N("| Company | Won deal |")
            N("|---------|----------|")
            for d in R["threeH"]:
                N(f"| {md_link(d.get('company_name'), company_link(d.get('company_id')))} "
                  f"| {md_link(d.get('dealname'), deal_link(d.get('id')))} |")
            N("")
        # one-off reminder
        if R["past_claim_deadline"] and len(R["fourA"]) > 0:
            N(f"**Claim window closed — open the Stale SQL Recycler workflow and manually "
              f"enrol the remaining {len(R['fourA'])} contact(s).**")
            N("")

    has_content = bool(body) or (is_nicola and nic_extra)
    A(f"### {name}" + (" (incl. non-sales & Unassigned)" if is_nicola else ""))
    A("")
    if not has_content:
        A("- Nothing to action this week.")
        A("")
        return out
    out.extend(body)
    if is_nicola and nic_extra:
        out.extend(nic_extra)
    return out


def render_owner_file(name, R, prev, comparable):
    """Standalone per-owner Section-5 block for pasting into email."""
    blocks = build_owner_blocks(R, prev, comparable)
    blk = next((b for b in blocks if b["owner"] == name), None)
    out = [f"# {name} — Pipeline actions for week of {R['run_date']}", ""]
    if blk:
        out.extend(blk["lines"])
    out.append("")
    return "\n".join(out)


# =========================================================================
# SNAPSHOT
# =========================================================================

def build_snapshot(R, raw):
    contact_states = {str(c.get("id")): {
        "lifecyclestage": c.get("lifecyclestage"),
        "hs_lead_status": c.get("hs_lead_status"),
    } for c in R["contacts"]}
    deal_stage = {str(d.get("id")): d.get("dealstage") for d in R["deals"]}

    def ids(rows, key="id"):
        return [str(r.get(key)) for r in rows]

    return {
        "schema": "oct2026",
        "run_date": R["run_date"],
        "generated_at": raw.get("generated_at"),
        "metrics": R["metrics"],
        "contact_states": contact_states,
        "deal_stage": deal_stage,
        "worklists": {
            "3A": ids(R["threeA"]),
            "3B_i": ids(R["b_missing_usd"]), "3B_ii": ids(R["b_idle"]),
            "3B_iii": ids(R["b_past_close"]), "3B_iv": ids(R["b_no_owner"]),
            "3C": ids(R["threeC"]), "3D": [str(t["c"].get("id")) for t in R["threeD"]],
            "3E": ids(R["threeE"]), "3F": [str(t["c"].get("id")) for t in R["threeF"]],
            "3G": [str(t["c"].get("id")) for t in R["threeG"]], "3H": ids(R["threeH"]),
            "4A": ids(R["fourA"]), "4A_overdue": ids(R["fourA_overdue"]),
            "4A_nonb2b": ids(R["fourA_nonb2b"]),
            "4C": ids(R["fourC"]), "4D_stuck": ids(R["fourD_stuck"]),
        },
        "raw_input": raw,
    }


# =========================================================================
# MAIN
# =========================================================================

def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly pipeline report (Oct 2026)")
    ap.add_argument("--input", required=True, help="Pre-pulled raw JSON")
    ap.add_argument("--date", help="Run date YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
    args = ap.parse_args()

    run_date = (dt.date.fromisoformat(args.date) if args.date
                else dt.datetime.now(dt.timezone.utc).date())

    with open(args.input) as f:
        raw = json.load(f)

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev_snapshot(snap_dir, run_date)
    # inject prev deal stages for 1.3
    if prev and prev.get("schema") == "oct2026":
        raw["_prev_deal_stage"] = prev.get("deal_stage", {})

    R = compute(raw, run_date)
    comparable = bool(prev and prev.get("schema") == "oct2026")

    report = render_report(R, prev)
    rep_path = os.path.join(rep_dir, f"{run_date.isoformat()}.md")
    with open(rep_path, "w") as f:
        f.write(report)

    # per-owner files
    owner_files = []
    for name in SALES_OWNERS + [NICOLA]:
        first = name.split()[0].lower()
        text = render_owner_file(name, R, prev, comparable)
        p = os.path.join(rep_dir, f"{run_date.isoformat()}_owner_{first}.md")
        with open(p, "w") as f:
            f.write(text)
        owner_files.append(p)

    # snapshot (strip computed helper keys from raw copy via json round-trip is fine;
    # we keep raw as-is minus the injected _prev_deal_stage)
    raw.pop("_prev_deal_stage", None)
    snapshot = build_snapshot(R, raw)
    snap_path = os.path.join(snap_dir, f"{run_date.isoformat()}.json")
    with open(snap_path, "w") as f:
        json.dump(snapshot, f, indent=2, default=str)

    print(f"Wrote {rep_path}")
    print(f"Wrote {snap_path}")
    for p in owner_files:
        print(f"Wrote {p}")
    print(f"Comparable prior snapshot: {prev['_file_date'] if comparable else 'none'}")
    # brief console summary
    m = R["metrics"]
    print("\n--- summary ---")
    print(f"B2B contacts: {m['contacts_b2b_total']} | SQL total: {m['sql_total']} "
          f"| open deals: {m['open_deal_qty']} (${m['open_deal_usd']:,.0f})")
    print(f"Q4: new_deals {m['q4_new_deals']} | entered_trial {m['q4_entered_trial']} "
          f"| closed_won {m['q4_closed_won']}")
    print(f"3C {m['miscategorised']} | 3D {m['status_mismatch']} | 3E {m['orphans']} "
          f"| 4A {len(R['fourA'])} | 4D stuck {m['stuck_recycles']} "
          f"| workflows ⚠️ {m['workflows_warn']}")


if __name__ == "__main__":
    main()
