#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report  —  process version Oct 2026.

This script consumes a PRE-PULLED HubSpot snapshot (``raw_input.json``) and
writes, for each run:

  snapshots/YYYY-MM-DD.json          machine snapshot (raw + worklist ids + diff state)
  reports/YYYY-MM-DD.md              the full report (Headline + Q4 + Sections 1-6)
  reports/YYYY-MM-DD_owner_<name>.md one Section-5 block per owner (paste into email)

Data is always pulled via the HubSpot MCP tools into ``raw_input.json`` (direct
egress to api.hubapi.com is blocked from the run environment), so this script
NEVER fetches live — run it only with ``--input``:

    python3 pipeline.py --input raw_input.json

Non-negotiable scoping rules baked in here (see the routine spec):
  * B2B only everywhere. Contact counts come from the CONTACT object; deal value
    comes from the DEAL object. The two bases are never mixed.
  * Deal USD value is ``amount_in_home_currency``. Empty/0 is flagged, never
    back-filled from raw ``amount`` (currencies are mixed INR/USD).
  * Lead-status values are mixed-format (labels AND codes). All rules are written
    in terms of LABELS and matched against the value->label map pulled at runtime
    (``lead_status_map`` in raw_input.json), never a hard-coded code.
"""

import argparse
import datetime as dt
import json
import os
import statistics
import sys

PORTAL_ID = 50467095

# =========================================================================
# EDITABLE CONSTANTS
# =========================================================================

# Deal stage internal value -> label.
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
# Open-pipeline display order (closed stages shown separately, as reference only).
STAGE_ORDER = [
    "Initial Engagement", "Qualified to Buy", "Discovery & Trial",
    "Quote Sent", "Commitment to Buy", "Negotiation",
]
CLOSED_LABELS = ["Closed Won", "Closed Lost"]
OPEN_STAGE_VALUES = {v for v, lbl in DEALSTAGE_LABELS.items()
                     if lbl not in CLOSED_LABELS}

# Sales owners each get their own Section-5 block. Everyone else (incl. Nicola
# Cole, Ramjee, and Unassigned) rolls into Nicola's block.
SALES_OWNERS = ["Theo Zirogiannis", "Jack Makhlouf", "Anup Ghoshal", "Biju Rajesh"]
NICOLA_BLOCK = "Nicola Cole"

# Lead-status LABELS (not codes). SALES_WORKING = sales is actively working it.
SALES_WORKING_STATUSES = ["Attempted to Contact", "Contacted"]

# Valid lead-status LABELS per lifecycle stage.
# NOTE: the spec lists "New" for subscriber/lead/MQL, but the portal's actual
# label for lead-status value NEW is "New Lead" — we match the live label (the
# spec rule "match the mapped value, never a hard-coded guess" wins). If the
# label map ever lacks a label named here, the run RAISES (see guard below).
NEW_LEAD_LABEL = "New Lead"
VALID_STATUS_BY_LIFECYCLE = {
    "subscriber":             [NEW_LEAD_LABEL, "Marketing Engaged"],
    "lead":                   [NEW_LEAD_LABEL, "Marketing Engaged"],
    "marketingqualifiedlead": ["MQL", NEW_LEAD_LABEL, "Unqualified"],
    "salesqualifiedlead":     ["Attempted to Contact", "Contacted"],
    "opportunity":            ["Qualified", "In Progress"],
    "customer":               ["Customer"],
}
# Lifecycles at/after SQL where a BLANK lead status is itself a mismatch.
SQL_OR_BEYOND = {"salesqualifiedlead", "opportunity", "customer"}

RECYCLE_DAYS = 90
RECYCLE_WARN_DAYS = 75
DEAL_IDLE_DAYS = 30
WINDOW_DAYS = 7
IDLE_BUCKETS = [30, 60]
QTB_STALE_DAYS = 14          # open-deal idle threshold used in headline/1.1
CLAIM_WINDOW_RUN = "2026-10-12"   # Section 5c runs only when today == this date
CLAIM_DEADLINE = "2026-10-16"
Q4_START = "2026-10-01"
Q4_END = "2026-12-31"
Q4_WEEKS = 13
Q4_TARGETS = {"new_deals": 20, "entered_trial": 7, "closed_won": 3}
MQL_FOLLOWUP_DAYS = 7

CONCENTRATION_SHARE = 0.40   # flag an owner/deal holding >= this share of open USD

# =========================================================================
# Small helpers
# =========================================================================

def parse_ts(value):
    """Parse a HubSpot ISO timestamp (or epoch-ms string) to aware UTC datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value / 1000, tz=dt.timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        return dt.datetime.fromtimestamp(int(s) / 1000, tz=dt.timezone.utc)
    s = s.replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(s)
    except ValueError:
        try:
            d = dt.datetime.fromisoformat(s[:10])
        except ValueError:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d


def to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value):
    f = to_float(value)
    return int(f) if f is not None else 0


def days_between(now_dt, ts_dt):
    if ts_dt is None:
        return None
    return (now_dt - ts_dt).days


def contact_link(cid):
    return f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-1/{cid}"


def company_link(cid):
    return f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-2/{cid}"


def deal_link(did):
    return f"https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-3/{did}"


def md_contact(c):
    nm = c.get("name") or f"Contact {c['id']}"
    return f"[{nm}]({contact_link(c['id'])})"


def md_company(c):
    nm = c.get("name") or f"Company {c['id']}"
    return f"[{nm}]({company_link(c['id'])})"


def md_deal(d):
    nm = d.get("dealname") or f"Deal {d['id']}"
    return f"[{nm}]({deal_link(d['id'])})"


def usd(x):
    if x is None:
        return "—"
    return f"${x:,.0f}"


def idle_str(days):
    if days is None:
        return "no activity"
    return f"{days}d"


# =========================================================================
# Owner resolution
# =========================================================================

class Owners:
    def __init__(self, owners_map):
        self.map = {str(k): v for k, v in (owners_map or {}).items()}

    def name(self, owner_id):
        if owner_id in (None, "", "null"):
            return "Unassigned"
        return self.map.get(str(owner_id), f"Owner {owner_id}")

    def block(self, owner_id):
        """Which Section-5 block a record belongs to."""
        nm = self.name(owner_id)
        return nm if nm in SALES_OWNERS else NICOLA_BLOCK


# =========================================================================
# Lead-status label mapping + guards
# =========================================================================

def build_label_fn(lead_status_map):
    m = dict(lead_status_map or {})

    def label(value):
        if value in (None, ""):
            return None
        return m.get(value, value)   # unknown code -> show raw code
    return label


def guard(raw):
    """Fail loudly on the conditions the spec says must RAISE."""
    # Label map must contain every label referenced by VALID_STATUS_BY_LIFECYCLE
    # and SALES_WORKING_STATUSES (prevents silent mismatches).
    labels_present = set((raw.get("lead_status_map") or {}).values())
    referenced = set(SALES_WORKING_STATUSES)
    for lst in VALID_STATUS_BY_LIFECYCLE.values():
        referenced.update(lst)
    missing = sorted(l for l in referenced if l not in labels_present)
    if missing:
        raise SystemExit(
            "GUARD: lead_status_map is missing label(s) used by the rules: "
            + ", ".join(missing)
            + ". Fix VALID_STATUS_BY_LIFECYCLE / SALES_WORKING_STATUSES or the pull."
        )
    # Company/deal/contact bases must be non-empty and not obviously the full
    # unfiltered set.
    if not raw.get("companies_b2b"):
        raise SystemExit("GUARD: companies_b2b is empty — company query failed.")
    if not raw.get("contacts_b2b"):
        raise SystemExit("GUARD: contacts_b2b is empty — contact query failed.")
    if not raw.get("deals_b2b"):
        raise SystemExit("GUARD: deals_b2b is empty — deal query failed.")


# =========================================================================
# Core computation
# =========================================================================

def compute(raw, run_date):
    now = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    q4_start = parse_ts(Q4_START)
    q4_end = parse_ts(Q4_END) + dt.timedelta(days=1)   # inclusive end-of-day
    claim_deadline = dt.date.fromisoformat(CLAIM_DEADLINE)

    owners = Owners(raw.get("owners"))
    label = build_label_fn(raw.get("lead_status_map"))

    contacts = raw["contacts_b2b"]
    deals = raw["deals_b2b"]
    companies = raw["companies_b2b"]
    sql_all = raw.get("sql_all_types", [])

    company_by_id = {str(c["id"]): c for c in companies}

    # ---- enrich contacts with parsed dates / derived fields --------------
    for c in contacts:
        c["_created"] = parse_ts(c.get("createdate"))
        c["_last_act"] = parse_ts(c.get("notes_last_updated"))
        c["_last_contacted"] = parse_ts(c.get("notes_last_contacted"))
        c["_recycled"] = parse_ts(c.get("recycled_date"))
        c["_ent_sql"] = parse_ts(c.get("date_entered_sql"))
        c["_ent_mql"] = parse_ts(c.get("date_entered_mql"))
        c["_idle"] = days_between(now, c["_last_act"])
        c["_status_label"] = label(c.get("hs_lead_status"))
        c["_owner"] = owners.name(c.get("owner_id"))
        c["_block"] = owners.block(c.get("owner_id"))
        c["_new"] = c["_created"] is not None and c["_created"] >= window_start
        c["_ndeals"] = to_int(c.get("num_associated_deals"))

    for d in deals:
        d["_created"] = parse_ts(d.get("createdate"))
        d["_closed"] = parse_ts(d.get("closedate"))
        d["_last_act"] = parse_ts(d.get("notes_last_updated"))
        d["_ent_stage"] = parse_ts(d.get("hs_v2_date_entered_current_stage"))
        d["_ent_trial"] = parse_ts(d.get("date_entered_trial"))
        d["_mod"] = parse_ts(d.get("hs_lastmodifieddate"))
        d["_idle"] = days_between(now, d["_last_act"])
        d["_usd"] = to_float(d.get("amount_in_home_currency"))
        d["_stage_label"] = DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage"))
        d["_open"] = d.get("dealstage") in OPEN_STAGE_VALUES
        d["_owner"] = owners.name(d.get("owner_id"))
        d["_block"] = owners.block(d.get("owner_id"))

    for co in companies:
        co["_created"] = parse_ts(co.get("createdate"))
        co["_owner"] = owners.name(co.get("owner_id"))
        co["_new"] = co["_created"] is not None and co["_created"] >= window_start

    R = {
        "run_date": run_date.isoformat(),
        "now": now,
        "window_start": window_start,
        "owners": owners,
        "label": label,
    }

    # =====================================================================
    # Q4 SCORECARD
    # =====================================================================
    def in_q4(ts):
        return ts is not None and q4_start <= ts < q4_end

    new_deals = [d for d in deals if in_q4(d["_created"])]
    entered_trial = [d for d in deals if in_q4(d["_ent_trial"])]
    closed_won_q4 = [d for d in deals
                     if d.get("dealstage") == "closedwon" and in_q4(d["_closed"])]

    weeks_elapsed = max(0.0, (run_date - q4_start.date()).days / 7.0)

    def expected(target):
        return round(target * min(weeks_elapsed, Q4_WEEKS) / Q4_WEEKS)

    def contrib(rows):
        agg = {}
        for d in rows:
            agg[d["_owner"]] = agg.get(d["_owner"], 0) + 1
        return dict(sorted(agg.items(), key=lambda kv: -kv[1]))

    R["q4"] = {
        "weeks_elapsed": round(weeks_elapsed, 1),
        "rows": [
            {"metric": "New deals", "def": "createdate in Q4",
             "actual": len(new_deals), "target": Q4_TARGETS["new_deals"],
             "expected": expected(Q4_TARGETS["new_deals"]),
             "contrib": contrib(new_deals)},
            {"metric": "Entered trial",
             "def": "date entered Discovery & Trial in Q4 (cumulative)",
             "actual": len(entered_trial), "target": Q4_TARGETS["entered_trial"],
             "expected": expected(Q4_TARGETS["entered_trial"]),
             "contrib": contrib(entered_trial)},
            {"metric": "Closed won", "def": "closedwon with closedate in Q4",
             "actual": len(closed_won_q4), "target": Q4_TARGETS["closed_won"],
             "expected": expected(Q4_TARGETS["closed_won"]),
             "contrib": contrib(closed_won_q4)},
        ],
    }

    # =====================================================================
    # SECTION 1 — CURRENT PIPELINE STATUS
    # =====================================================================
    open_deals = [d for d in deals if d["_open"]]

    # 1.1 by stage
    by_stage = {lbl: [] for lbl in STAGE_ORDER}
    for d in open_deals:
        by_stage.setdefault(d["_stage_label"], []).append(d)
    stage_rows = []
    open_qty = 0
    open_usd = 0.0
    for lbl in STAGE_ORDER:
        group = by_stage.get(lbl, [])
        idles = [d["_idle"] for d in group if d["_idle"] is not None]
        tot = sum((d["_usd"] or 0) for d in group)
        open_qty += len(group)
        open_usd += tot
        stage_rows.append({
            "stage": lbl, "qty": len(group), "usd": tot,
            "median_idle": int(statistics.median(idles)) if idles else None,
            "idle14": sum(1 for d in group if d["_idle"] is not None and d["_idle"] >= QTB_STALE_DAYS),
        })
    closed_rows = []
    for lbl in CLOSED_LABELS:
        group = [d for d in deals if d["_stage_label"] == lbl]
        closed_rows.append({"stage": lbl, "qty": len(group),
                            "usd": sum((d["_usd"] or 0) for d in group)})

    # 1.2 by owner
    owner_agg = {}
    for d in open_deals:
        a = owner_agg.setdefault(d["_owner"], {"qty": 0, "usd": 0.0})
        a["qty"] += 1
        a["usd"] += (d["_usd"] or 0)
    owner_rows = sorted(
        [{"owner": o, **v} for o, v in owner_agg.items()],
        key=lambda r: -r["usd"])
    top_owner_share = (max((r["usd"] for r in owner_rows), default=0) / open_usd) if open_usd else 0
    top_deal = max(open_deals, key=lambda d: (d["_usd"] or 0), default=None)
    top_deal_share = ((top_deal["_usd"] or 0) / open_usd) if (open_usd and top_deal) else 0

    # 1.3 moved / updated this week
    prev_deal_stage = R.get("_prev_deal_stage", {})  # filled by caller via closure below
    R["_sec1"] = {
        "stage_rows": stage_rows, "closed_rows": closed_rows,
        "open_qty": open_qty, "open_usd": open_usd,
        "owner_rows": owner_rows,
        "top_owner_share": top_owner_share, "top_deal": top_deal,
        "top_deal_share": top_deal_share,
        "open_deals": open_deals,
    }

    # =====================================================================
    # SECTION 2 — NEW PIPELINE STATUS
    # =====================================================================
    # 2.1 new companies by owner
    new_companies = [co for co in companies if co["_new"]]
    nc_by_owner = {}
    for co in new_companies:
        nc_by_owner.setdefault(co["_owner"], []).append(co)
    new_companies_unassigned = [co for co in new_companies if co["_owner"] == "Unassigned"]

    # 2.2 new contacts on existing vs new accounts
    new_contacts = [c for c in contacts if c["_new"]]
    new_on_existing = []     # contact + company
    new_on_new = []
    new_orphan = []          # (feeds 3E)
    for c in new_contacts:
        acid = c.get("associatedcompanyid")
        if acid in (None, "", "null"):
            new_orphan.append(c)
            continue
        co = company_by_id.get(str(acid))
        if co is None:
            # Associated to a company outside the B2B set (age unknown).
            c["_company_age"] = "unknown"
            new_on_existing.append(c)
        elif co["_created"] is not None and co["_created"] >= window_start:
            new_on_new.append(c)
        else:
            new_on_existing.append(c)

    noe_by_owner = {}
    for c in new_on_existing:
        noe_by_owner.setdefault(c["_owner"], {}).setdefault(c.get("lifecyclestage") or "unknown", []).append(c)

    # 2.3 SQLs by sales owner (CONTACTS only — the true SQL total)
    sqls = [c for c in contacts if c.get("lifecyclestage") == "salesqualifiedlead"]
    sql_owner = {}
    for c in sqls:
        a = sql_owner.setdefault(c["_owner"], {"qty": 0, "idle30": 0, "idle60": 0})
        a["qty"] += 1
        idle = c["_idle"]
        if idle is None or idle >= IDLE_BUCKETS[0]:
            a["idle30"] += 1
        if idle is None or idle >= IDLE_BUCKETS[1]:
            a["idle60"] += 1
    sql_owner_rows = sorted([{"owner": o, **v} for o, v in sql_owner.items()],
                            key=lambda r: -r["qty"])

    R["_sec2"] = {
        "new_companies": new_companies, "nc_by_owner": nc_by_owner,
        "new_companies_unassigned": new_companies_unassigned,
        "new_on_existing": new_on_existing, "new_on_new": new_on_new,
        "noe_by_owner": noe_by_owner,
        "sqls": sqls, "sql_owner_rows": sql_owner_rows, "sql_total": len(sqls),
    }

    # =====================================================================
    # SECTION 3 — ADMIN / DATA-HYGIENE
    # =====================================================================
    # 3A companies missing owner and/or name
    sec3a = [co for co in companies
             if (co.get("owner_id") in (None, "", "null")) or not (co.get("name") or "").strip()]

    # 3B deal hygiene (open deals)
    b_missing_usd = [d for d in open_deals if d["_usd"] in (None, 0, 0.0)]
    b_idle = [d for d in open_deals if d["_idle"] is None or d["_idle"] >= DEAL_IDLE_DAYS]
    b_past_close = [d for d in open_deals if d["_closed"] is not None and d["_closed"] < now]
    b_no_owner = [d for d in open_deals if d.get("owner_id") in (None, "", "null")]

    # 3C miscategorised
    sec3c = [c for c in contacts
             if c["_status_label"] in SALES_WORKING_STATUSES
             and c.get("lifecyclestage") in ("subscriber", "lead", "marketingqualifiedlead")]

    # 3D status mismatches
    sec3d = []
    for c in contacts:
        life = c.get("lifecyclestage")
        sl = c["_status_label"]
        valid = VALID_STATUS_BY_LIFECYCLE.get(life)
        reason = None
        expected_s = None
        if valid is not None:
            if sl in (None, ""):
                if life in SQL_OR_BEYOND:
                    reason = "(blank)"
                    expected_s = "/".join(valid)
            elif sl not in valid:
                reason = sl
                expected_s = "/".join(valid)
        # opportunity deal-count rules
        if life == "opportunity":
            if sl == "In Progress" and c["_ndeals"] > 0:
                reason = f"In Progress w/ {c['_ndeals']} deal(s)"
                expected_s = "Qualified"
            elif sl == "Qualified" and c["_ndeals"] == 0:
                reason = "Qualified w/ 0 deals"
                expected_s = "In Progress"
        if reason is not None:
            sec3d.append({**c, "_cur": reason, "_expected": expected_s})

    # 3E orphan contacts (new, no company)  == new_orphan
    sec3e = new_orphan

    # 3F lost-deal leftovers (company-association proxy — see report caveat)
    #     opportunity B2B contacts whose associated company's B2B deals are ALL
    #     closed-lost (at least one deal, none open, none won).
    deals_by_company = {}
    for d in deals:
        if d.get("company_id"):
            deals_by_company.setdefault(str(d["company_id"]), []).append(d)
    sec3f = []
    for c in contacts:
        if c.get("lifecyclestage") != "opportunity":
            continue
        acid = c.get("associatedcompanyid")
        if acid in (None, "", "null"):
            continue
        cds = deals_by_company.get(str(acid))
        if not cds:
            continue
        stages = {d.get("dealstage") for d in cds}
        if stages and stages.issubset({"closedlost"}):
            sec3f.append(c)

    # 3G MQL follow-up
    sec3g = []
    for c in contacts:
        if c.get("lifecyclestage") != "marketingqualifiedlead":
            continue
        no_owner = c.get("owner_id") in (None, "", "null")
        ent = c["_ent_mql"]
        stale = False
        if ent is not None and days_between(now, ent) is not None and days_between(now, ent) >= MQL_FOLLOWUP_DAYS:
            lc = c["_last_contacted"]
            if lc is None or lc <= ent:
                stale = True
        if stale or no_owner:
            c["_mql_reason"] = "no owner" if no_owner and not stale else (
                "no follow-up" if stale and not no_owner else "no owner + no follow-up")
            sec3g.append(c)

    # 3H won-but-not-customer companies
    won_company_ids = {str(d["company_id"]) for d in deals
                       if d.get("dealstage") == "closedwon" and d.get("company_id")}
    sec3h = [co for co in companies
             if str(co["id"]) in won_company_ids
             and (co.get("company_type") == "PROSPECT")]

    R["_sec3"] = {
        "a": sec3a, "b_missing_usd": b_missing_usd, "b_idle": b_idle,
        "b_past_close": b_past_close, "b_no_owner": b_no_owner,
        "c": sec3c, "d": sec3d, "e": sec3e, "f": sec3f, "g": sec3g, "h": sec3h,
    }

    # =====================================================================
    # SECTION 4 — RECYCLER HEALTH
    # =====================================================================
    def meets_trigger(c, created_key="_created"):
        la = c.get("_last_act")
        if la is not None:
            dd = days_between(now, la)
            return dd is not None and dd > RECYCLE_DAYS, dd
        cr = c.get(created_key)
        if cr is not None:
            dd = days_between(now, cr)
            return dd is not None and dd > RECYCLE_DAYS, dd
        return False, None

    # 4A trigger match now (B2B SQLs)
    sec4a = []
    sec4a_overdue = []
    for c in sqls:
        hit, age = meets_trigger(c)
        if hit:
            c["_trigger_age"] = age
            sec4a.append(c)
            if age is not None and age > RECYCLE_DAYS + 1:
                sec4a_overdue.append(c)
    # non-B2B SQL meeting trigger (B2B filter broken check)
    nonb2b_hits = []
    for c in sql_all:
        if (c.get("customer_type") or "") == "B2B":
            continue
        la = parse_ts(c.get("notes_last_updated"))
        cr = parse_ts(c.get("createdate"))
        if la is not None:
            dd = days_between(now, la)
            hit = dd is not None and dd > RECYCLE_DAYS
        elif cr is not None:
            dd = days_between(now, cr)
            hit = dd is not None and dd > RECYCLE_DAYS
        else:
            hit = False
        if hit:
            nonb2b_hits.append(c)

    # 4C recycled this week
    sec4c = [c for c in contacts
             if c["_recycled"] is not None and c["_recycled"] >= window_start]
    sec4c_still_sql = [c for c in sec4c if c.get("lifecyclestage") == "salesqualifiedlead"]

    # 4D stuck recycles + re-promotions
    sec4d_stuck = []
    sec4d_repromoted = []
    for c in contacts:
        if c["_recycled"] is None:
            continue
        if c.get("lifecyclestage") != "salesqualifiedlead":
            continue
        if c["_ent_sql"] is not None and c["_ent_sql"] < c["_recycled"]:
            sec4d_stuck.append(c)
        elif c["_ent_sql"] is not None and c["_ent_sql"] > c["_recycled"]:
            sec4d_repromoted.append(c)

    R["_sec4"] = {
        "a": sec4a, "a_overdue": sec4a_overdue, "nonb2b": nonb2b_hits,
        "c": sec4c, "c_still_sql": sec4c_still_sql,
        "d_stuck": sec4d_stuck, "d_repromoted": sec4d_repromoted,
    }

    # =====================================================================
    # SECTION 6 — WORKFLOW FIRING CHECKS
    # =====================================================================
    # Contact "created/modified this week" proxy = createdate OR notes_last_updated in window.
    def touched_this_week(c):
        return (c["_created"] is not None and c["_created"] >= window_start) or \
               (c["_last_act"] is not None and c["_last_act"] >= window_start)

    wf1 = [c for c in sec3c if touched_this_week(c)]
    wf2 = [c for c in contacts
           if c["_status_label"] == "Contacted"
           and c.get("lifecyclestage") in ("subscriber", "lead", "marketingqualifiedlead")]
    _wf3_seen = {}
    for c in sec4a_overdue + sec4d_stuck:
        _wf3_seen[str(c["id"])] = c
    wf3_bad = list(_wf3_seen.values())
    wf4 = [c for c in sec3d if c.get("lifecyclestage") in ("opportunity", "customer")]
    wf5 = [c for c in contacts
           if c.get("lifecyclestage") == "opportunity"
           and c["_status_label"] == "In Progress" and c["_ndeals"] > 0]
    wf6 = [d for d in open_deals
           if d["_created"] is not None and d["_created"] >= window_start
           and d["_usd"] in (None, 0, 0.0)]

    R["_sec6"] = [
        {"wf": "Sales Activity > Lead Status",
         "healthy": "3C contacts created/modified this week = 0",
         "ok": len(wf1) == 0, "items": wf1, "kind": "contact"},
        {"wf": "Meeting Booked > Contacted",
         "healthy": 'no "Contacted" at Email Contact / Lead / MQL',
         "ok": len(wf2) == 0, "items": wf2, "kind": "contact"},
        {"wf": "Stale SQL Recycler",
         "healthy": "4A overdue = 0 (after claim window) and 4D stuck = 0",
         "ok": len(wf3_bad) == 0, "items": wf3_bad, "kind": "contact"},
        {"wf": "Lifecycle > Lead Status Sync",
         "healthy": "3D rows at Opportunity/Customer = 0",
         "ok": len(wf4) == 0, "items": wf4, "kind": "contact"},
        {"wf": "Deal Associated > Qualified",
         "healthy": "Opportunity + In Progress with deals > 0 = 0",
         "ok": len(wf5) == 0, "items": wf5, "kind": "contact"},
        {"wf": "Deal required fields",
         "healthy": "open deals created this week with empty/zero USD = 0",
         "ok": len(wf6) == 0, "items": wf6, "kind": "deal"},
    ]

    # =====================================================================
    # SECTION 5 — OWNER ACTION LISTS
    # =====================================================================
    # 5b recycling in next 15 days (warn band 75..89 days)
    def in_warn_band(c):
        la = c["_last_act"]
        if la is not None:
            dd = days_between(now, la)
            return dd is not None and RECYCLE_WARN_DAYS <= dd < RECYCLE_DAYS
        cr = c["_created"]
        if cr is not None:
            dd = days_between(now, cr)
            return dd is not None and RECYCLE_WARN_DAYS <= dd < RECYCLE_DAYS
        return False
    warn_band = [c for c in sqls if in_warn_band(c)]

    blocks = {name: {"b": [], "d": [], "e": [], "f": []}
              for name in SALES_OWNERS + [NICOLA_BLOCK]}

    def blk(owner_id):
        return owners.block(owner_id)

    for c in warn_band:
        blocks[c["_block"]]["b"].append(c)
    for c in sec4c:
        blocks[c["_block"]]["d"].append(c)
    for d in (b_missing_usd + b_idle + b_past_close):
        blocks[d["_block"]].setdefault("e", []).append(d)
    for c in (sec3c + sec3d + sec3e + sec4d_stuck):
        blocks[c["_block"]]["f"].append(c)

    # dedupe section-e per block by deal id (a deal may hit several 3B tables)
    for name in blocks:
        seen = {}
        for d in blocks[name]["e"]:
            seen[d["id"]] = d
        blocks[name]["e"] = list(seen.values())

    claim_window_run_today = (run_date.isoformat() == CLAIM_WINDOW_RUN)
    show_claim_reminder = (run_date > claim_deadline and len(sec4a) > 0)

    R["_sec5"] = {
        "blocks": blocks, "warn_band": warn_band,
        "claim_window_run_today": claim_window_run_today,
        "claim_reminder": show_claim_reminder, "backlog_n": len(sec4a),
    }

    # =====================================================================
    # HEADLINE FLAGS
    # =====================================================================
    pct_open_idle14 = (sum(1 for d in open_deals if d["_idle"] is not None and d["_idle"] >= QTB_STALE_DAYS)
                       / len(open_deals) * 100) if open_deals else 0
    pct_sql_idle30 = (sum(1 for c in sqls if c["_idle"] is None or c["_idle"] >= 30)
                      / len(sqls) * 100) if sqls else 0
    deals_moved = [d for d in deals
                   if (d["_ent_stage"] is not None and d["_ent_stage"] >= window_start)]

    R["headline"] = {
        "wf_ok": sum(1 for w in R["_sec6"] if w["ok"]),
        "wf_total": len(R["_sec6"]),
        "deals_moved": len(deals_moved),
        "pct_open_idle14": round(pct_open_idle14),
        "pct_sql_idle30": round(pct_sql_idle30),
        "miscategorised": len(sec3c),
        "status_mismatch": len(sec3d),
        "stuck_recycles": len(sec4d_stuck),
        "deals_past_close": len(b_past_close),
        "deals_missing_usd": len(b_missing_usd),
        "new_accounts_no_owner": len(new_companies_unassigned),
        "recycled_this_week": len(sec4c),
        "sql_total": len(sqls),
        "open_qty": open_qty, "open_usd": round(open_usd),
    }
    R["_deals_moved"] = deals_moved
    R["_contacts"] = contacts
    R["_deals"] = deals
    R["_open_deals"] = open_deals
    return R


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
        return json.load(f)


def delta(cur, prev):
    if prev is None:
        return f"{cur} (—)"
    diff = cur - prev
    if diff > 0:
        return f"{cur} (+{diff})"
    if diff < 0:
        return f"{cur} ({diff})"
    return f"{cur} (=)"


# =========================================================================
# Report rendering
# =========================================================================

def render_report(R, raw, prev):
    out = []
    A = out.append
    now = R["now"]
    H = R["headline"]
    ph = (prev or {}).get("headline") if prev else None

    def pd(key):
        return ph.get(key) if ph else None

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {R['run_date']}")
    A("")
    A(f"*Scope: **B2B only**. Contact counts from the CONTACT object; deal value "
      f"from DEALS (`amount_in_home_currency`, USD). Generated "
      f"{raw.get('generated_at','')} · process version Oct 2026.*")
    A("")
    if prev:
        A(f"*Week-on-week deltas vs previous snapshot `{prev.get('run_date')}`. "
          f"Metrics new in this process version show `(—)` until next run.*")
    else:
        A("*No prior snapshot — week-on-week comparison starts next run.*")
    A("")

    # ---- Q4 SCORECARD -----------------------------------------------------
    q = R["q4"]
    A(f"## Q4 Scorecard  ·  {Q4_START} → {R['run_date']}  ·  week "
      f"{q['weeks_elapsed']:.1f} of {Q4_WEEKS}")
    A("")
    A("| Metric | Definition | Actual | Target | Expected by now | Status |")
    A("|--------|-----------|-------:|-------:|----------------:|:------:|")
    pq = {r["metric"]: r for r in prev["q4"]["rows"]} if (prev and prev.get("q4")) else {}
    for r in q["rows"]:
        prev_actual = pq.get(r["metric"], {}).get("actual") if pq else None
        st = "✅" if r["actual"] >= r["expected"] else "⚠️"
        A(f"| {r['metric']} | {r['def']} | {delta(r['actual'], prev_actual)} "
          f"| {r['target']} | {r['expected']} | {st} |")
    A("")
    for r in q["rows"]:
        if r["contrib"]:
            parts = ", ".join(f"{o}: {n}" for o, n in r["contrib"].items())
        else:
            parts = "none yet"
        A(f"- **{r['metric']}** by owner — {parts}")
    A("")

    # ---- HEADLINE FLAGS ---------------------------------------------------
    A("## 🔔 Headline flags")
    A("")
    wf_mark = "✅" if H["wf_ok"] == H["wf_total"] else "⚠️"
    A(f"- {wf_mark} **Workflows firing:** {H['wf_ok']}/{H['wf_total']} healthy "
      f"(see Section 6)")
    A(f"- **Deals changed stage this week:** {delta(H['deals_moved'], pd('deals_moved'))}")
    A(f"- **Open deals idle {QTB_STALE_DAYS}+ days:** {H['pct_open_idle14']}% "
      f"of {H['open_qty']} open")
    A(f"- **SQLs untouched 30+ days:** {H['pct_sql_idle30']}% of {H['sql_total']} SQLs")
    A(f"- {'⚠️ ' if H['miscategorised'] else ''}**Miscategorised leads (3C):** "
      f"{delta(H['miscategorised'], pd('miscategorised'))}")
    A(f"- {'⚠️ ' if H['status_mismatch'] else ''}**Status mismatches (3D):** "
      f"{delta(H['status_mismatch'], pd('status_mismatch'))}")
    A(f"- {'⚠️ ' if H['stuck_recycles'] else ''}**Stuck recycles (4D):** "
      f"{delta(H['stuck_recycles'], pd('stuck_recycles'))}")
    A(f"- {'⚠️ ' if H['deals_past_close'] else ''}**Deals past close date (3B iii):** "
      f"{delta(H['deals_past_close'], pd('deals_past_close'))}")
    A(f"- {'⚠️ ' if H['deals_missing_usd'] else ''}**Open deals missing/zero USD (3B i):** "
      f"{delta(H['deals_missing_usd'], pd('deals_missing_usd'))}")
    A(f"- {'⚠️ ' if H['new_accounts_no_owner'] else ''}**New accounts with no owner (2.1):** "
      f"{delta(H['new_accounts_no_owner'], pd('new_accounts_no_owner'))}")
    A(f"- **Recycled this week (4C):** {delta(H['recycled_this_week'], pd('recycled_this_week'))}")
    nonb2b = R["_sec4"]["nonb2b"]
    if nonb2b:
        A(f"- ⚠️ **{len(nonb2b)} non-B2B SQL(s) meet the recycler trigger** — B2B "
          f"filter may be broken (see 4A).")
    A("")

    # ---- SECTION 1 --------------------------------------------------------
    s1 = R["_sec1"]
    A("## Section 1 — Current pipeline status")
    A("")
    A("### 1.1 Open pipeline by stage (B2B, open only)")
    A("")
    A("| Stage | Deals | Total USD | Median days idle | Idle ≥14d |")
    A("|-------|------:|----------:|-----------------:|----------:|")
    for r in s1["stage_rows"]:
        mi = "—" if r["median_idle"] is None else f"{r['median_idle']}d"
        A(f"| {r['stage']} | {r['qty']} | {usd(r['usd'])} | {mi} | {r['idle14']} |")
    A(f"| **Open subtotal** | **{s1['open_qty']}** | **{usd(s1['open_usd'])}** | | |")
    A("")
    A("_Closed (reference only, not in open subtotal):_")
    for r in s1["closed_rows"]:
        A(f"- {r['stage']}: {r['qty']} deals · {usd(r['usd'])}")
    A("")
    A("### 1.2 Open pipeline by sales owner")
    A("")
    A("| Owner | Open deals | Open USD |")
    A("|-------|-----------:|---------:|")
    for r in s1["owner_rows"]:
        A(f"| {r['owner']} | {r['qty']} | {usd(r['usd'])} |")
    A("")
    if s1["top_owner_share"] >= CONCENTRATION_SHARE and s1["owner_rows"]:
        A(f"> ⚠️ **Concentration:** {s1['owner_rows'][0]['owner']} holds "
          f"{s1['top_owner_share']*100:.0f}% of open value.")
    if s1["top_deal_share"] >= CONCENTRATION_SHARE and s1["top_deal"]:
        A(f"> ⚠️ **Concentration:** one deal — {md_deal(s1['top_deal'])} "
          f"({usd(s1['top_deal']['_usd'])}) — is {s1['top_deal_share']*100:.0f}% of open value.")
    A("")
    A("### 1.3 Moved / updated in the last 7 days")
    A("")
    moved = R["_deals_moved"]
    prev_stage = {}
    if prev and prev.get("raw_deal_stage"):
        prev_stage = prev["raw_deal_stage"]
    updated = [d for d in R["_deals"]
              if d not in moved and d["_mod"] is not None and d["_mod"] >= R["window_start"]]
    if moved:
        A(f"**Stage moved ({len(moved)})** — real progression:")
        A("")
        A("| Deal | Owner | Prev → Current | USD | Last activity |")
        A("|------|-------|----------------|----:|---------------|")
        for d in sorted(moved, key=lambda x: -(x["_usd"] or 0)):
            prevs = prev_stage.get(str(d["id"]))
            prevlbl = DEALSTAGE_LABELS.get(prevs, prevs) if prevs else "(new/unknown)"
            la = d["_last_act"].date().isoformat() if d["_last_act"] else "—"
            A(f"| {md_deal(d)} | {d['_owner']} | {prevlbl} → {d['_stage_label']} "
              f"| {usd(d['_usd'])} | {la} |")
        A("")
    else:
        A("- No deals changed stage this week.")
        A("")
    if updated:
        A(f"**Updated only ({len(updated)})** — field edits, no stage change:")
        A("")
        for d in sorted(updated, key=lambda x: -(x["_usd"] or 0)):
            A(f"- {md_deal(d)} · {d['_owner']} · {d['_stage_label']} · {usd(d['_usd'])}")
        A("")

    # ---- SECTION 2 --------------------------------------------------------
    s2 = R["_sec2"]
    A("## Section 2 — New pipeline status")
    A("")
    A(f"### 2.1 New companies created in last {WINDOW_DAYS} days — by owner")
    A("")
    if s2["new_companies"]:
        for owner, group in sorted(s2["nc_by_owner"].items(), key=lambda kv: -len(kv[1])):
            A(f"**{owner}** ({len(group)})")
            for co in group:
                flag = " ⚠️ no owner" if owner == "Unassigned" else ""
                A(f"- {md_company(co)}{flag}")
            A("")
    else:
        A("- None this week.")
        A("")

    A("### 2.2 New contacts on EXISTING companies — by owner, then lifecycle")
    A("")
    if s2["new_on_existing"]:
        for owner, byl in sorted(s2["noe_by_owner"].items(), key=lambda kv: kv[0]):
            tot = sum(len(v) for v in byl.values())
            A(f"**{owner}** ({tot})")
            for life, group in sorted(byl.items(), key=lambda kv: kv[0]):
                names = ", ".join(md_contact(c) for c in group)
                A(f"- _{life}_: {names}")
            A("")
    else:
        A("- None this week.")
        A("")
    A(f"New contacts on **NEW** accounts (company also created this week): "
      f"**{len(s2['new_on_new'])}**")
    A("")

    A("### 2.3 SQLs by sales owner (contacts only — the true SQL total)")
    A("")
    A("| Owner | SQLs | Not touched 30+d | Not touched 60+d |")
    A("|-------|-----:|-----------------:|-----------------:|")
    for r in s2["sql_owner_rows"]:
        A(f"| {r['owner']} | {r['qty']} | {r['idle30']} | {r['idle60']} |")
    tot30 = sum(r["idle30"] for r in s2["sql_owner_rows"])
    tot60 = sum(r["idle60"] for r in s2["sql_owner_rows"])
    A(f"| **Total** | **{s2['sql_total']}** | **{tot30}** | **{tot60}** |")
    A("")

    # ---- SECTION 3 --------------------------------------------------------
    s3 = R["_sec3"]
    A("## Section 3 — Admin / data-hygiene worklist")
    A("")
    A(f"### 3A Companies to complete — missing owner and/or name ({len(s3['a'])}) · owner: Nicola")
    A("")
    if s3["a"]:
        A("| Company | Created | Owner | Name | Source | Created by |")
        A("|---------|---------|-------|------|--------|-----------|")
        for co in s3["a"][:200]:
            cr = co["_created"].date().isoformat() if co["_created"] else "—"
            nm = (co.get("name") or "").strip() or "**blank**"
            own = co["_owner"] if co.get("owner_id") not in (None, "", "null") else "**none**"
            cby = R["owners"].name(co["created_by"]) if co.get("created_by") not in (None, "", "null") else "—"
            A(f"| {md_company(co)} | {cr} | {own} | {nm} | "
              f"{co.get('original_source') or '—'} | {cby} |")
        if len(s3["a"]) > 200:
            A(f"| …and {len(s3['a'])-200} more | | | | | |")
    else:
        A("- None.")
    A("")

    A("### 3B Deal hygiene (open B2B deals)")
    A("")
    def deal_table(rows, title):
        A(f"**{title}** ({len(rows)})")
        if not rows:
            A("")
            A("- None.")
            A("")
            return
        A("")
        A("| Deal | Stage | Owner | USD | Last activity | Close date |")
        A("|------|-------|-------|----:|---------------|-----------|")
        for d in rows:
            la = d["_last_act"].date().isoformat() if d["_last_act"] else "—"
            cl = d["_closed"].date().isoformat() if d["_closed"] else "—"
            A(f"| {md_deal(d)} | {d['_stage_label']} | {d['_owner']} | {usd(d['_usd'])} "
              f"| {la} | {cl} |")
        A("")
    deal_table(s3["b_missing_usd"], "(i) USD empty or zero")
    deal_table(s3["b_idle"], f"(ii) Idle {DEAL_IDLE_DAYS}+ days")
    deal_table(s3["b_past_close"], "(iii) Close date in the past")
    deal_table(s3["b_no_owner"], "(iv) No deal owner")

    def contact_worklist(rows, title, extra=None):
        A(f"### {title} ({len(rows)})")
        A("")
        if not rows:
            A("- None.")
            A("")
            return
        by_owner = {}
        for c in rows:
            by_owner.setdefault(c["_owner"], []).append(c)
        for owner, group in sorted(by_owner.items(), key=lambda kv: -len(kv[1])):
            A(f"**{owner}** ({len(group)})")
            for c in group:
                A(f"- {md_contact(c)}{extra(c) if extra else ''}")
            A("")

    contact_worklist(
        s3["c"], "3C Miscategorised leads",
        extra=lambda c: f" — {c['_status_label']} @ {c.get('lifecyclestage')}")
    contact_worklist(
        s3["d"], "3D Status mismatches",
        extra=lambda c: f" — {c.get('lifecyclestage')} · is *{c['_cur']}* · expected *{c['_expected']}*")
    A(f"### 3E Orphan contacts — new ({WINDOW_DAYS}d) B2B contacts with no company ({len(s3['e'])})")
    A("")
    if s3["e"]:
        by_owner = {}
        for c in s3["e"]:
            by_owner.setdefault(c["_owner"], []).append(c)
        for owner, group in sorted(by_owner.items(), key=lambda kv: -len(kv[1])):
            A(f"**{owner}** ({len(group)}): " + ", ".join(md_contact(c) for c in group))
        A("")
    else:
        A("- None.")
        A("")
    A(f"### 3F Lost-deal leftovers — Opportunity contacts, all associated deals closed-lost ({len(s3['f'])})")
    A("")
    A("_Proxy via the contact's associated company's deals (contact→deal associations "
      "are not in this pull). Review list only — rule TBC (recycle to MQL + Unqualified "
      "\"Lost deal\")._")
    A("")
    if s3["f"]:
        by_owner = {}
        for c in s3["f"]:
            by_owner.setdefault(c["_owner"], []).append(c)
        for owner, group in sorted(by_owner.items(), key=lambda kv: -len(kv[1])):
            A(f"**{owner}** ({len(group)}): " + ", ".join(md_contact(c) for c in group))
        A("")
    else:
        A("- None.")
        A("")
    contact_worklist(
        s3["g"], "3G MQL follow-up",
        extra=lambda c: f" — {c.get('_mql_reason','')}")
    A(f"### 3H Won but not Customer — Prospect companies with a Closed Won deal ({len(s3['h'])})")
    A("")
    if s3["h"]:
        for co in s3["h"]:
            A(f"- {md_company(co)} — still type *Prospect*")
        A("")
    else:
        A("- None.")
        A("")

    # ---- SECTION 4 --------------------------------------------------------
    s4 = R["_sec4"]
    A("## Section 4 — Recycler health check")
    A("")
    A(f"### 4A Trigger match now — B2B SQLs idle > {RECYCLE_DAYS}d ({len(s4['a'])})")
    A("")
    after_claim = R["run_date"] > CLAIM_WINDOW_RUN
    if after_claim and s4["a_overdue"]:
        A(f"> ⚠️ **ALERT:** {len(s4['a_overdue'])} match(es) are >1 day past threshold "
          f"after the claim window closed — the recycler is not firing.")
        A("")
    elif not after_claim:
        A("> Held backlog (claim window still open) — reported as info.")
        A("")
    if s4["a"]:
        by_owner = {}
        for c in s4["a"]:
            by_owner.setdefault(c["_owner"], []).append(c)
        A("| Owner | SQLs | Contacts |")
        A("|-------|-----:|----------|")
        for owner, group in sorted(by_owner.items(), key=lambda kv: -len(kv[1])):
            names = ", ".join(f"{md_contact(c)} ({c.get('_trigger_age','?')}d)" for c in group[:40])
            if len(group) > 40:
                names += f", +{len(group)-40} more"
            A(f"| {owner} | {len(group)} | {names} |")
        A("")
    else:
        A("- No B2B SQLs currently meet the trigger.")
        A("")
    if s4["nonb2b"]:
        A(f"> ⚠️ **ALERT:** {len(s4['nonb2b'])} NON-B2B SQL(s) meet the trigger — the "
          f"B2B scoping on the recycler may be broken: "
          + ", ".join(f"[{c.get('name') or c['id']}]({contact_link(c['id'])}) "
                      f"({c.get('customer_type')})" for c in s4["nonb2b"][:20]))
        A("")

    A(f"### 4C Recycled this week ({len(s4['c'])})")
    A("")
    if s4["c"]:
        by_owner = {}
        for c in s4["c"]:
            by_owner.setdefault(c["_owner"], []).append(c)
        for owner, group in sorted(by_owner.items(), key=lambda kv: -len(kv[1])):
            A(f"**{owner}** ({len(group)}): " + ", ".join(md_contact(c) for c in group))
        A("")
        if s4["c_still_sql"]:
            A(f"> ⚠️ {len(s4['c_still_sql'])} recycled contact(s) are still at SQL — see 4D.")
            A("")
        else:
            A("- ✅ All recycled contacts this week are now at MQL.")
            A("")
    else:
        A("- None.")
        A("")
    A(f"### 4D Stuck recycles — recycled but still at SQL ({len(s4['d_stuck'])})")
    A("")
    if s4["d_stuck"]:
        A("> ⚠️ **ALERT:** the move to MQL failed (lifecycle not cleared before status).")
        A("")
        by_owner = {}
        for c in s4["d_stuck"]:
            by_owner.setdefault(c["_owner"], []).append(c)
        for owner, group in sorted(by_owner.items(), key=lambda kv: -len(kv[1])):
            A(f"**{owner}** ({len(group)}): " + ", ".join(md_contact(c) for c in group))
        A("")
    else:
        A("- ✅ None.")
        A("")
    if s4["d_repromoted"]:
        A(f"_Re-promotions (info — sales re-engaged, legitimate): {len(s4['d_repromoted'])}_: "
          + ", ".join(md_contact(c) for c in s4["d_repromoted"][:40]))
        A("")

    # ---- SECTION 5 --------------------------------------------------------
    A("## Section 5 — Owner action lists")
    A("")
    A(render_owner_blocks(R, prev, include_header=False))

    # ---- SECTION 6 --------------------------------------------------------
    A("## Section 6 — Workflow firing checks (\"built ≠ working\")")
    A("")
    A("| Workflow | Healthy if | Status |")
    A("|----------|-----------|:------:|")
    pwf = {w["wf"]: w for w in prev.get("sec6", [])} if (prev and prev.get("sec6")) else {}
    for w in R["_sec6"]:
        mark = "✅" if w["ok"] else "⚠️"
        prev_ct = pwf.get(w["wf"], {}).get("count")
        dstr = f" ({delta(len(w['items']), prev_ct)})" if prev_ct is not None else ""
        A(f"| {w['wf']} | {w['healthy']} | {mark}{dstr} |")
    A("")
    for w in R["_sec6"]:
        if not w["ok"]:
            if w["kind"] == "deal":
                links = ", ".join(md_deal(x) for x in w["items"][:40])
            else:
                links = ", ".join(md_contact(x) for x in w["items"][:40])
            more = f", +{len(w['items'])-40} more" if len(w["items"]) > 40 else ""
            A(f"- ⚠️ **{w['wf']}** ({len(w['items'])}): {links}{more}")
    A("")

    A("---")
    A(f"*Snapshot: `snapshots/{R['run_date']}.json` · Report: `reports/{R['run_date']}.md` "
      f"· per-owner files: `reports/{R['run_date']}_owner_<name>.md`*")
    A("")
    return "\n".join(out)


def render_owner_blocks(R, prev, include_header=True, only_owner=None):
    """Render Section-5 blocks. If only_owner is set, render just that block."""
    out = []
    A = out.append
    s5 = R["_sec5"]
    blocks = s5["blocks"]
    now = R["now"]

    # 5a diff requires a prior per-contact state map.
    prev_state = (prev or {}).get("contact_state") if prev else None

    names = SALES_OWNERS + [NICOLA_BLOCK]
    if only_owner:
        names = [only_owner]

    for name in names:
        b = blocks[name]
        lines = []
        L = lines.append

        # a. records changed this week (diff)
        changed = []
        if prev_state:
            for c in R["_contacts"]:
                if c["_block"] != name:
                    continue
                ps = prev_state.get(str(c["id"]))
                if not ps:
                    continue
                old_life, old_stat = ps.get("life"), ps.get("status")
                if old_life != c.get("lifecyclestage") or old_stat != c.get("hs_lead_status"):
                    changed.append((c, old_life, old_stat))
        if changed:
            L("**a. Your records changed this week** — review; flag anything wrong.")
            for c, ol, os_ in changed:
                L(f"- {md_contact(c)}: lifecycle {ol}→{c.get('lifecyclestage')}, "
                  f"status {ol if False else (R['label'](os_) or '—')}→{c['_status_label'] or '—'}")
            L("")

        # b. recycling in next 15 days
        bb = [c for c in s5["warn_band"] if c["_block"] == name]
        if bb:
            L("**b. Recycling in the next 15 days** — log activity now or they return "
              "to marketing.")
            for c in bb:
                L(f"- {md_contact(c)} (idle {idle_str(c['_idle'])})")
            L("")

        # c. claim window (only on CLAIM_WINDOW_RUN)
        if s5["claim_window_run_today"]:
            cc = [c for c in R["_sec4"]["a"] if c["_block"] == name]
            if cc:
                L("**c. CLAIM WINDOW** — to keep a contact, log an activity (call, email "
                  f"or note) by Fri {CLAIM_DEADLINE[5:]} Oct. Any without activity after "
                  "that will be recycled to marketing.")
                for c in cc:
                    L(f"- {md_contact(c)}")
                L("")

        # d. recycled this week
        if b["d"]:
            L("**d. Recycled this week**")
            for c in b["d"]:
                L(f"- {md_contact(c)}")
            L("")

        # e. deals needing attention
        if b["e"]:
            L("**e. Deals needing attention**")
            for d in b["e"]:
                tags = []
                if d["_usd"] in (None, 0, 0.0):
                    tags.append("USD empty/0")
                if d["_idle"] is None or d["_idle"] >= DEAL_IDLE_DAYS:
                    tags.append(f"idle {idle_str(d['_idle'])}")
                if d["_closed"] is not None and d["_closed"] < now:
                    tags.append("past close date")
                L(f"- {md_deal(d)} ({d['_stage_label']}, {usd(d['_usd'])}) — {', '.join(tags)}")
            L("")

        # f. data fixes
        if b["f"]:
            L("**f. Data fixes**")
            for c in b["f"]:
                L(f"- {md_contact(c)}")
            L("")

        # Nicola-only extras
        if name == NICOLA_BLOCK:
            s3 = R["_sec3"]
            if s3["a"]:
                L(f"**3A Companies to complete** ({len(s3['a'])}): "
                  + ", ".join(md_company(co) for co in s3["a"][:40])
                  + (f", +{len(s3['a'])-40} more" if len(s3["a"]) > 40 else ""))
                L("")
            if s3["h"]:
                L(f"**3H Won but not Customer** ({len(s3['h'])}): "
                  + ", ".join(md_company(co) for co in s3["h"]))
                L("")
            if s5["claim_reminder"]:
                L(f"**⏰ Claim window closed** — open the Stale SQL Recycler workflow and "
                  f"manually enrol the remaining {s5['backlog_n']} contact(s) from 4A.")
                L("")

        header = f"### {name}" + ("  _(+ non-sales & Unassigned)_" if name == NICOLA_BLOCK else "")
        if lines:
            out_block = [header, ""] + lines
        else:
            out_block = [header, "", "- Nothing to action this week.", ""]
        A("\n".join(out_block))

    text = "\n".join(out)
    if include_header:
        text = "## Section 5 — Owner action lists\n\n" + text
    return text


# =========================================================================
# Snapshot assembly
# =========================================================================

def build_snapshot(R, raw):
    def ids(rows, key="id"):
        return [str(r[key]) for r in rows]

    s3 = R["_sec3"]
    s4 = R["_sec4"]
    contact_state = {str(c["id"]): {"life": c.get("lifecyclestage"),
                                    "status": c.get("hs_lead_status")}
                     for c in R["_contacts"]}
    raw_deal_stage = {str(d["id"]): d.get("dealstage") for d in R["_deals"]}
    return {
        "run_date": R["run_date"],
        "generated_at": raw.get("generated_at"),
        "process_version": "Oct 2026",
        "headline": R["headline"],
        "q4": {"weeks_elapsed": R["q4"]["weeks_elapsed"],
               "rows": [{"metric": r["metric"], "actual": r["actual"],
                         "target": r["target"], "expected": r["expected"]}
                        for r in R["q4"]["rows"]]},
        "sec6": [{"wf": w["wf"], "ok": w["ok"], "count": len(w["items"])}
                 for w in R["_sec6"]],
        "worklist_ids": {
            "3A": ids(s3["a"]), "3B_missing_usd": ids(s3["b_missing_usd"]),
            "3B_idle": ids(s3["b_idle"]), "3B_past_close": ids(s3["b_past_close"]),
            "3B_no_owner": ids(s3["b_no_owner"]),
            "3C": ids(s3["c"]), "3D": ids(s3["d"]), "3E": ids(s3["e"]),
            "3F": ids(s3["f"]), "3G": ids(s3["g"]), "3H": ids(s3["h"]),
            "4A": ids(s4["a"]), "4A_overdue": ids(s4["a_overdue"]),
            "4C": ids(s4["c"]), "4D_stuck": ids(s4["d_stuck"]),
        },
        "contact_state": contact_state,
        "raw_deal_stage": raw_deal_stage,
    }


# =========================================================================
# Main
# =========================================================================

def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly pipeline report (Oct 2026)")
    ap.add_argument("--input", required=True, help="Pre-pulled raw JSON (required)")
    ap.add_argument("--date", help="Run date YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
    args = ap.parse_args()

    run_date = (dt.date.fromisoformat(args.date) if args.date
                else dt.datetime.now(dt.timezone.utc).date())

    with open(args.input) as f:
        raw = json.load(f)

    guard(raw)

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev_snapshot(snap_dir, run_date)
    R = compute(raw, run_date)

    report = render_report(R, raw, prev)
    rep_path = os.path.join(rep_dir, f"{run_date.isoformat()}.md")
    with open(rep_path, "w") as f:
        f.write(report)

    # Per-owner Section-5 files
    owner_files = []
    for name in SALES_OWNERS + [NICOLA_BLOCK]:
        block = render_owner_blocks(R, prev, include_header=True, only_owner=name)
        slug = name.split()[0].lower()
        p = os.path.join(rep_dir, f"{run_date.isoformat()}_owner_{slug}.md")
        with open(p, "w") as f:
            f.write(f"# Section 5 — {name} — {run_date.isoformat()}\n\n" + block + "\n")
        owner_files.append(p)

    snapshot = build_snapshot(R, raw)
    snap_path = os.path.join(snap_dir, f"{run_date.isoformat()}.json")
    with open(snap_path, "w") as f:
        json.dump(snapshot, f, indent=2)

    print(f"Wrote {rep_path}")
    print(f"Wrote {snap_path}")
    for p in owner_files:
        print(f"Wrote {p}")
    print(f"Prev snapshot: {prev['run_date'] if prev else 'none (first comparable run)'}")


if __name__ == "__main__":
    main()
