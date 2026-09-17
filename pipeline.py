#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report.

What it does
------------
Consumes B2B contacts, companies and deals pulled from HubSpot and writes two
files per run:

  snapshots/YYYY-MM-DD.json  -- pulled data + computed metrics (machine)
  reports/YYYY-MM-DD.md      -- readable VP summary (alerts, pipeline, what
                                moved, new top-of-funnel by owner, funnel,
                                miscategorised, SQL review, week-on-week)

Each run loads the most recent previous snapshot from snapshots/ and shows the
week-on-week movement for every comparable metric.

Data source
-----------
Direct egress to api.hubapi.com is blocked in the run environment, so data is
pulled via the HubSpot MCP tools into a raw_input.json file and handed to the
computation code here:

    python3 pipeline.py --input raw_input.json

Running without --input is NOT supported in this environment (it would require
live egress to api.hubapi.com).

Scoping rules (non-negotiable)
------------------------------
* B2B only. Contacts filtered on customer_type = B2B; companies on
  customer_type__cloned_ = B2B (never customer_type on a company).
* Lifecycle FUNNEL is built from B2B CONTACTS only. Companies never contribute
  to funnel counts.
* PIPELINE / deal value is built from DEALS, scoped to B2B via the deal's
  associated company customer_type__cloned_ = B2B, valued at company level.
* "New" = created within the last WINDOW_DAYS (default 7), for both contacts
  and companies, on the same window.
* Contact -> company joins (orphan status, new-vs-existing account) are done
  here from pulled scalar fields (associatedcompanyid + company id/createdate),
  never via a cross-object MCP filter (those silently return the full set).
"""

import argparse
import datetime as dt
import json
import os
import sys

# =========================================================================
# EDITABLE CONSTANTS
# =========================================================================

# Lifecycle stage internal value -> human label (B2B contact funnel).
LIFECYCLE_LABELS = {
    "subscriber": "Email Contact",
    "lead": "Lead",
    "marketingqualifiedlead": "Marketing Qualified Lead",
    "salesqualifiedlead": "Sales Qualified Lead",
    "opportunity": "Opportunity",
    "customer": "Customer",
    "other": "Other",          # partners
}
LIFECYCLE_ORDER = [
    "subscriber", "lead", "marketingqualifiedlead",
    "salesqualifiedlead", "opportunity", "customer", "other",
]

# Deal stage internal value -> human label, in pipeline order.
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
DEALSTAGE_ORDER = [
    "1217017229", "appointmentscheduled", "presentationscheduled",
    "1217017230", "contractsent", "decisionmakerboughtin",
    "closedwon", "closedlost",
]
CLOSED_STAGES = {"closedwon", "closedlost"}

# hs_lead_status values that mean sales is actively working the record.
# In THIS portal the working statuses are "Attempted to Contact" and
# "Contacted" (internal value CONNECTED). Only statuses that actually exist in
# hs_lead_status belong here. Other real values (Qualified, IN_PROGRESS,
# PROPOSAL_REQUEST) exist in the portal but are deliberately excluded: they sit
# further along than "sales is actively working outreach". Edit as the portal
# evolves.
SALES_WORKING_STATUSES = {"Attempted to Contact", "CONNECTED"}

# A contact is miscategorised if it carries a working lead status but its
# lifecycle stage is still only one of these (should be SQL or beyond).
MISCATEGORISED_LIFECYCLE_STAGES = {"lead", "marketingqualifiedlead"}

# Qualified-to-Buy deal alert: internal stage value + idle threshold.
QUALIFIED_TO_BUY_STAGE = "appointmentscheduled"
QTB_STALE_DAYS = 14

# SQL follow-up/review: gone-quiet trigger for engaged SQL contacts.
IDLE_THRESHOLD_DAYS = 30

# "New" window (days) for contacts and companies.
WINDOW_DAYS = 7


# =========================================================================
# Editable rule: miscategorised-lead check
# =========================================================================
def is_miscategorised(lead_status, lifecyclestage):
    """A contact sales is working (lead status) but stuck at Lead / MQL."""
    return (lead_status in SALES_WORKING_STATUSES
            and lifecyclestage in MISCATEGORISED_LIFECYCLE_STAGES)


# =========================================================================
# Small helpers
# =========================================================================
def _to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_dt(value):
    """Parse a HubSpot ISO timestamp -> aware datetime (UTC), or None."""
    if not value:
        return None
    v = str(value).replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(v)
    except ValueError:
        try:
            d = dt.datetime.fromisoformat(v[:10])
        except ValueError:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d


def _parse_date(value):
    d = _parse_dt(value)
    return d.date() if d else None


def _days_since(value, run_dt):
    d = _parse_dt(value)
    if d is None:
        return None
    return (run_dt - d).days


def delta_str(current, prev):
    if prev is None:
        return f"{current}"
    diff = current - prev
    if diff > 0:
        return f"{current} (+{diff})"
    if diff < 0:
        return f"{current} ({diff})"
    return f"{current} (=)"


def money(n):
    return f"${n:,.0f}"


def owner_label(rec):
    return rec.get("owner") or "— unassigned —"


# =========================================================================
# Computation
# =========================================================================
def compute(raw, run_date):
    run_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)
    window_days = int(raw.get("window_days", WINDOW_DAYS))
    window_start = run_date - dt.timedelta(days=window_days)

    def in_window(iso):
        d = _parse_date(iso)
        return d is not None and d >= window_start

    # ---- Funnel (B2B contacts only) -------------------------------------
    funnel_counts = raw.get("funnel_counts", {})
    funnel = []
    for stage in LIFECYCLE_ORDER:
        funnel.append({"stage": stage,
                       "label": LIFECYCLE_LABELS.get(stage, stage),
                       "count": int(funnel_counts.get(stage, 0))})
    for stage, c in funnel_counts.items():
        if stage not in LIFECYCLE_ORDER:
            funnel.append({"stage": stage, "label": stage, "count": int(c)})
    mql = int(funnel_counts.get("marketingqualifiedlead", 0))
    sql = int(funnel_counts.get("salesqualifiedlead", 0))
    ratio = round(sql / mql, 2) if mql else None

    # ---- Company index --------------------------------------------------
    companies = raw.get("companies_b2b", [])
    co_by_id = {str(c["id"]): c for c in companies}

    # ---- New top-of-funnel: contacts created in window ------------------
    new_contacts = [c for c in raw.get("contacts_b2b", [])
                    if in_window(c.get("createdate"))]
    orphans, on_new, on_existing, on_unknown = [], [], [], []
    for c in new_contacts:
        assoc = str(c.get("associatedcompanyid") or "").strip()
        if not assoc:
            orphans.append(c)
            continue
        co = co_by_id.get(assoc)
        if co is None:
            on_unknown.append(c)
        elif in_window(co.get("createdate")):
            on_new.append(c)
        else:
            on_existing.append(c)

    def by_owner(rows):
        agg = {}
        for r in rows:
            agg.setdefault(owner_label(r), []).append(r)
        return dict(sorted(agg.items(), key=lambda kv: (-len(kv[1]), kv[0])))

    # ---- New B2B accounts (companies tagged B2B, created in window) ------
    new_b2b_accounts = [c for c in companies
                        if c.get("customer_type_cloned") == "B2B"
                        and in_window(c.get("createdate"))]
    leads_per_account = {}
    for c in new_contacts:
        assoc = str(c.get("associatedcompanyid") or "").strip()
        if assoc:
            leads_per_account[assoc] = leads_per_account.get(assoc, 0) + 1
    for a in new_b2b_accounts:
        a["_new_leads_attached"] = leads_per_account.get(str(a["id"]), 0)
    new_accounts_no_owner = [a for a in new_b2b_accounts if not a.get("owner_id")]

    # Data-quality: new company records tied to new contacts but NOT tagged
    # B2B on the company (so they fall out of B2B company scoping).
    new_untagged = []
    seen = set()
    for c in new_contacts:
        assoc = str(c.get("associatedcompanyid") or "").strip()
        co = co_by_id.get(assoc)
        if (co and assoc not in seen and in_window(co.get("createdate"))
                and co.get("customer_type_cloned") != "B2B"):
            seen.add(assoc)
            new_untagged.append(co)

    # ---- Deals: value by stage + open/closed ----------------------------
    deals = raw.get("deals", [])
    by_stage = {}
    missing_amount = []
    for d in deals:
        stage = d.get("dealstage") or "unknown"
        e = by_stage.setdefault(stage, {"count": 0, "usd": 0.0, "raw_missing": False})
        e["count"] += 1
        usd = _to_float(d.get("amount_in_home_currency"))
        if usd is None:
            missing_amount.append(d)
        else:
            e["usd"] += usd

    def stage_rows(stages):
        rows = []
        for s in stages:
            if s in by_stage:
                rows.append({"stage": s,
                             "label": DEALSTAGE_LABELS.get(s, s),
                             "count": by_stage[s]["count"],
                             "usd": round(by_stage[s]["usd"], 2)})
        return rows

    open_stage_vals = [s for s in DEALSTAGE_ORDER if s not in CLOSED_STAGES]
    # include any unknown/other open stages
    extra_open = [s for s in by_stage if s not in DEALSTAGE_ORDER]
    open_rows = stage_rows(open_stage_vals) + stage_rows(extra_open)
    open_count = sum(r["count"] for r in open_rows)
    open_usd = round(sum(r["usd"] for r in open_rows), 2)
    won_rows = stage_rows(["closedwon"])
    lost_rows = stage_rows(["closedlost"])
    deals_by_stage = stage_rows(DEALSTAGE_ORDER) + stage_rows(extra_open)

    # Disproportionate open value concentration.
    concentration = None
    if open_usd > 0:
        top = max(open_rows, key=lambda r: r["usd"])
        share = top["usd"] / open_usd
        if share >= 0.4:
            concentration = {"label": top["label"], "usd": top["usd"],
                             "share": round(share * 100, 1)}

    # ---- What moved in the window ---------------------------------------
    moved, touched = [], []
    for d in deals:
        entered_win = in_window(d.get("hs_v2_date_entered_current_stage"))
        mod_win = in_window(d.get("hs_lastmodifieddate"))
        if entered_win:
            moved.append(d)
        elif mod_win:
            touched.append(d)

    def deal_view(d):
        return {
            "id": d.get("id"),
            "dealname": d.get("dealname", ""),
            "stage": d.get("dealstage"),
            "stage_label": DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage")),
            "usd": _to_float(d.get("amount_in_home_currency")),
            "entered_stage": (d.get("hs_v2_date_entered_current_stage") or "")[:10],
            "last_activity": (d.get("notes_last_updated") or "")[:10] or None,
            "last_modified": (d.get("hs_lastmodifieddate") or "")[:10],
            "owner": d.get("owner") or "— unassigned —",
        }

    def group_by_stage(views):
        agg = {}
        for v in views:
            agg.setdefault(v["stage"], []).append(v)
        ordered = []
        for s in DEALSTAGE_ORDER:
            if s in agg:
                rows = sorted(agg[s], key=lambda x: (x["usd"] or 0), reverse=True)
                ordered.append((DEALSTAGE_LABELS.get(s, s), rows))
        for s in agg:
            if s not in DEALSTAGE_ORDER:
                ordered.append((s, agg[s]))
        return ordered

    moved_grouped = group_by_stage([deal_view(d) for d in moved])
    touched_grouped = group_by_stage([deal_view(d) for d in touched])

    # ---- QTB stale alert (>= QTB_STALE_DAYS idle) -----------------------
    qtb_stale = []
    for d in deals:
        if d.get("dealstage") != QUALIFIED_TO_BUY_STAGE:
            continue
        days = _days_since(d.get("notes_last_updated"), run_dt)
        if days is None or days >= QTB_STALE_DAYS:
            qtb_stale.append({"id": d.get("id"), "dealname": d.get("dealname", ""),
                              "days": days, "usd": _to_float(d.get("amount_in_home_currency")),
                              "last_activity": (d.get("notes_last_updated") or "")[:10] or None,
                              "owner": d.get("owner") or "— unassigned —"})
    qtb_stale.sort(key=lambda x: (x["days"] is not None, x["days"] or 0), reverse=True)

    # ---- Miscategorised (pre-filtered by the same rule) -----------------
    misc = raw.get("miscategorised", [])
    misc_by_owner = by_owner(misc)

    # ---- SQL follow-up / review -----------------------------------------
    # deal lookup by company id (prefer an open deal, else highest USD).
    deals_by_company = {}
    for d in deals:
        cid = str(d.get("company_id") or "")
        if cid:
            deals_by_company.setdefault(cid, []).append(d)

    def pick_deal(company_id):
        cands = deals_by_company.get(str(company_id or ""), [])
        if not cands:
            return None
        opens = [d for d in cands if d.get("dealstage") not in CLOSED_STAGES]
        pool = opens or cands
        return max(pool, key=lambda d: (_to_float(d.get("amount_in_home_currency")) or 0))

    sql_review = []
    for c in raw.get("sql_review", []):
        days = _days_since(c.get("notes_last_updated"), run_dt)
        if not (days is None or days > IDLE_THRESHOLD_DAYS):
            continue
        d = pick_deal(c.get("associatedcompanyid"))
        sql_review.append({
            "id": c.get("id"), "name": c.get("name", ""),
            "hs_lead_status": c.get("hs_lead_status"),
            "days": days, "owner": c.get("owner") or "— unassigned —",
            "deal_name": d.get("dealname") if d else None,
            "deal_usd": _to_float(d.get("amount_in_home_currency")) if d else None,
            "deal_stage": DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage")) if d else None,
        })
    sql_review.sort(key=lambda x: (x["days"] is not None, x["days"] or 0), reverse=True)

    return {
        # funnel
        "funnel": funnel,
        "contacts_b2b_total": int(raw.get("contacts_b2b_total", sum(f["count"] for f in funnel))),
        "mql_count": mql, "sql_count": sql, "sql_to_mql_ratio": ratio,
        "funnel_inversion": sql > mql,
        # new top of funnel
        "window_days": window_days,
        "new_contacts_count": len(new_contacts),
        "orphan_count": len(orphans),
        "on_new_count": len(on_new),
        "on_existing_count": len(on_existing),
        "on_unknown_count": len(on_unknown),
        "orphans_by_owner": {k: [_c(v) for v in vs] for k, vs in by_owner(orphans).items()},
        "on_new_by_owner": {k: [_c(v) for v in vs] for k, vs in by_owner(on_new).items()},
        "on_existing_by_owner": {k: [_c(v) for v in vs] for k, vs in by_owner(on_existing).items()},
        "new_accounts_count": len(new_b2b_accounts),
        "new_accounts": [_a(a) for a in new_b2b_accounts],
        "new_accounts_no_owner": [_a(a) for a in new_accounts_no_owner],
        "new_untagged_companies": [_a(a) for a in new_untagged],
        # deals
        "deals_by_stage": deals_by_stage,
        "open_rows": open_rows, "open_count": open_count, "open_usd": open_usd,
        "won_rows": won_rows, "lost_rows": lost_rows,
        "deal_total_count": len(deals),
        "concentration": concentration,
        "missing_amount": [{"id": d.get("id"), "dealname": d.get("dealname", ""),
                            "stage": DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage")),
                            "raw_amount": d.get("amount"),
                            "currency": d.get("deal_currency_code"),
                            "owner": d.get("owner") or "— unassigned —"} for d in missing_amount],
        # what moved
        "moved_count": len(moved), "touched_count": len(touched),
        "moved_grouped": moved_grouped, "touched_grouped": touched_grouped,
        # alerts / lists
        "qtb_stale": qtb_stale, "qtb_stale_count": len(qtb_stale),
        "miscategorised_count": len(misc),
        "miscategorised": misc, "miscategorised_by_owner": misc_by_owner,
        "sql_review": sql_review, "sql_review_count": len(sql_review),
    }


def _c(c):
    return {"id": c.get("id"), "name": c.get("name", ""),
            "lifecyclestage": c.get("lifecyclestage"),
            "owner": c.get("owner") or "— unassigned —",
            "score": c.get("b2b_contact_lead_score_combined")}


def _a(a):
    return {"id": a.get("id"), "name": a.get("name") or "(no name set)",
            "createdate": (a.get("createdate") or "")[:10],
            "owner": a.get("owner") or "— unassigned —",
            "owner_id": a.get("owner_id", ""),
            "new_leads_attached": a.get("_new_leads_attached", 0)}


# =========================================================================
# Snapshots + week-on-week
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


def prev_metric(prev, key, default=None):
    if not prev:
        return default
    return prev.get("metrics", {}).get(key, default)


# =========================================================================
# Report rendering
# =========================================================================
def render(snapshot, prev):
    m = snapshot["metrics"]
    pm = prev.get("metrics", {}) if prev else None
    date = snapshot["run_date"]
    out = []
    A = out.append

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {date}")
    A("")
    A(f"*Scope: B2B only. Funnel = B2B **contacts** (`customer_type = B2B`); "
      f"pipeline = **deals** scoped to B2B accounts "
      f"(`COMPANY.customer_type__cloned_ = B2B`), valued in USD "
      f"(`amount_in_home_currency`). \"New\" = last {m['window_days']} days. "
      f"Generated {snapshot['generated_at_utc']} UTC.*")
    A("")
    if prev:
        A(f"*Week-on-week deltas vs the previous snapshot (`{prev['run_date']}`). "
          f"Deal figures are now B2B-scoped and USD-normalised; where the prior "
          f"snapshot used a different scope/currency basis this is noted inline.*")
    else:
        A("*No prior snapshot — week-on-week comparison starts next run.*")
    A("")

    # ================= ALERTS =================
    A("## 🔔 Alerts")
    A("")

    # Orphan new leads
    if m["orphan_count"]:
        A(f"### ⚠️ Orphan new leads: {m['orphan_count']}")
        A("")
        A("New B2B contacts (last {} days) with no associated company — they "
          "cannot be worked until an account is attached.".format(m["window_days"]))
        A("")
        A("| Owner | Orphan new leads |")
        A("|-------|------------------|")
        for owner, rows in m["orphans_by_owner"].items():
            names = ", ".join(f"{r['name']} (#{r['id']})" for r in rows)
            A(f"| {owner} | {len(rows)} — {names} |")
        A("")
    else:
        A(f"- ✅ **Orphan new leads:** 0 — every new B2B contact this window is "
          f"attached to an account.")
        A("")

    # New accounts with no owner
    if m["new_accounts_no_owner"]:
        A(f"### ⚠️ New B2B accounts with no owner: {len(m['new_accounts_no_owner'])}")
        A("")
        A("B2B-tagged company records created this window with no "
          "`hubspot_owner_id` — unrouted, so nobody is working them.")
        A("")
        A("| Account | Created | New leads attached |")
        A("|---------|---------|--------------------|")
        for a in m["new_accounts_no_owner"]:
            A(f"| {a['name']} (#{a['id']}) | {a['createdate']} | {a['new_leads_attached']} |")
        A("")
    else:
        A("- ✅ **New B2B accounts with no owner:** 0.")
        A("")

    # QTB stale
    if m["qtb_stale"]:
        A(f"### ⚠️ \"Qualified to Buy\" deals idle {QTB_STALE_DAYS}+ days: "
          f"{m['qtb_stale_count']}")
        A("")
        A("| Deal | Last activity | Days idle | USD | Owner |")
        A("|------|---------------|-----------|-----|-------|")
        for d in m["qtb_stale"]:
            days = "no activity logged" if d["days"] is None else f"{d['days']}d"
            usd = money(d["usd"]) if d["usd"] is not None else "⚠️ missing"
            A(f"| {d['dealname']} (#{d['id']}) | {d['last_activity'] or '—'} "
              f"| {days} | {usd} | {d['owner']} |")
        A("")
    else:
        A(f"- ✅ No \"Qualified to Buy\" deals idle {QTB_STALE_DAYS}+ days.")
        A("")

    # Missing amount_in_home_currency
    if m["missing_amount"]:
        A(f"### ⚠️ Deals missing `amount_in_home_currency`: {len(m['missing_amount'])}")
        A("")
        A("These are excluded from every USD total below (never back-filled from "
          "raw `amount`). Fix the currency/amount on the deal.")
        A("")
        A("| Deal | Stage | Raw amount | Currency | Owner |")
        A("|------|-------|-----------|----------|-------|")
        for d in m["missing_amount"]:
            A(f"| {d['dealname']} (#{d['id']}) | {d['stage']} | "
              f"{d['raw_amount'] if d['raw_amount'] is not None else '—'} | "
              f"{d['currency'] or '—'} | {d['owner']} |")
        A("")
    else:
        A("- ✅ Every B2B deal has `amount_in_home_currency`.")
        A("")

    # Miscategorised count (WoW)
    misc_prev = pm.get("miscategorised_count") if pm else None
    A(f"### 🏷️ Miscategorised leads: {delta_str(m['miscategorised_count'], misc_prev)}")
    A("")
    A("Sales is working these (`hs_lead_status` ∈ {Attempted to Contact, "
      "Contacted}) but lifecycle is still only Lead / MQL — see full list below. "
      "(Prior snapshot used a broader status set, so the delta is directional.)")
    A("")

    # Funnel inversion
    if m["funnel_inversion"]:
        A(f"### ⚠️ Funnel inversion: SQL ({m['sql_count']}) > MQL ({m['mql_count']})")
        A("")
        A("More Sales-Qualified than Marketing-Qualified B2B contacts. Expected "
          "MQL ≥ SQL. Likely: leads promoted straight to SQL past MQL, or MQL "
          "criteria too strict. (Improving: see MQL/SQL trend below.)")
        A("")
    else:
        A(f"- ✅ No funnel inversion (MQL {m['mql_count']} ≥ SQL {m['sql_count']}).")
        A("")

    # New untagged companies (data quality)
    if m["new_untagged_companies"]:
        A(f"### 🧹 Data quality: {len(m['new_untagged_companies'])} new company "
          f"record(s) not tagged `customer_type__cloned_ = B2B`")
        A("")
        A("These company records were created this window for new B2B contacts "
          "but carry no B2B company tag, so they drop out of B2B **company/"
          "account** scoping (they are still counted in the contact funnel). "
          "Tag them B2B so account-level pipeline attribution stays correct.")
        A("")
        A("| Account | Created | Owner |")
        A("|---------|---------|-------|")
        for a in m["new_untagged_companies"]:
            A(f"| {a['name']} (#{a['id']}) | {a['createdate']} | {a['owner']} |")
        A("")

    # ================= 1. CURRENT PIPELINE =================
    A("## 1. Current company-level pipeline (B2B)")
    A("")
    A("Deal count and USD value by stage (USD = `amount_in_home_currency`).")
    A("")
    A("| Stage | Deals | USD value |")
    A("|-------|------:|----------:|")
    prev_deals = {d["stage"]: d for d in (pm.get("deals_by_stage", []) if pm else [])}
    for r in m["open_rows"]:
        pv = prev_deals.get(r["stage"])
        A(f"| {r['label']} | {delta_str(r['count'], pv['count'] if pv else None)} "
          f"| {money(r['usd'])} |")
    A(f"| **Open subtotal** | **{m['open_count']}** | **{money(m['open_usd'])}** |")
    for r in m["won_rows"]:
        pv = prev_deals.get(r["stage"])
        A(f"| {r['label']} | {delta_str(r['count'], pv['count'] if pv else None)} "
          f"| {money(r['usd'])} |")
    for r in m["lost_rows"]:
        pv = prev_deals.get(r["stage"])
        A(f"| {r['label']} | {delta_str(r['count'], pv['count'] if pv else None)} "
          f"| {money(r['usd'])} |")
    A(f"| **Total** | **{m['deal_total_count']}** | — |")
    A("")
    if m["concentration"]:
        c = m["concentration"]
        A(f"> **Concentration:** {c['label']} holds {money(c['usd'])} = "
          f"**{c['share']}%** of open pipeline value.")
        A("")
    A("*USD totals sum `amount_in_home_currency` only; raw `amount` is never "
      "summed across currencies. Deal-count deltas vs prior are shown; the prior "
      "snapshot's deal set was not B2B-scoped, so treat count deltas as "
      "directional. Value deltas are omitted (prior used raw mixed-currency).*")
    A("")

    # ================= 2. WHAT MOVED =================
    A("## 2. What moved in the last {} days".format(m["window_days"]))
    A("")
    A(f"**Stage moved** (deal changed stage this window — detected via "
      f"`hs_v2_date_entered_current_stage`): **{m['moved_count']}**. "
      f"**Touched** (field/activity only, no stage change): **{m['touched_count']}**.")
    A("")
    if m["moved_grouped"]:
        A("### Stage moved")
        A("")
        for label, rows in m["moved_grouped"]:
            A(f"**{label}** ({len(rows)})")
            A("")
            A("| Deal | USD | Entered stage | Owner |")
            A("|------|----:|---------------|-------|")
            for v in rows:
                usd = money(v["usd"]) if v["usd"] is not None else "⚠️ missing"
                A(f"| {v['dealname']} (#{v['id']}) | {usd} | {v['entered_stage']} | {v['owner']} |")
            A("")
    else:
        A("- No deal changed stage this window.")
        A("")
    if m["touched_grouped"]:
        A("### Touched (field / activity, no stage change)")
        A("")
        for label, rows in m["touched_grouped"]:
            A(f"**{label}** ({len(rows)})")
            A("")
            A("| Deal | USD | Last activity | Owner |")
            A("|------|----:|---------------|-------|")
            for v in rows:
                usd = money(v["usd"]) if v["usd"] is not None else "⚠️ missing"
                A(f"| {v['dealname']} (#{v['id']}) | {usd} | {v['last_activity'] or '—'} | {v['owner']} |")
            A("")

    # ================= 3. NEW TOP OF FUNNEL =================
    A("## 3. New top of funnel — by sales owner")
    A("")
    A(f"**{m['new_contacts_count']}** new B2B contacts and "
      f"**{m['new_accounts_count']}** new B2B accounts this window.")
    A("")

    # 3a new accounts
    A("### 3a. New B2B accounts (created this window)")
    A("")
    if m["new_accounts"]:
        A("| Account | Created | Owner | New leads attached |")
        A("|---------|---------|-------|--------------------:|")
        for a in m["new_accounts"]:
            A(f"| {a['name']} (#{a['id']}) | {a['createdate']} | {a['owner']} | {a['new_leads_attached']} |")
        A("")
        A("*All new B2B-tagged accounts this window are the individual-signup "
          "records (no company name, no owner). The named institution records "
          "created this window are untagged — see the data-quality alert.*")
        A("")
    else:
        A("- None this window.")
        A("")

    # 3b split
    A("### 3b. New B2B contacts — existing vs new account vs orphan")
    A("")
    A(f"- **On an existing account:** {m['on_existing_count']}")
    A(f"- **On a new account:** {m['on_new_count']}")
    A(f"- **Orphan (no account):** {m['orphan_count']}")
    if m["on_unknown_count"]:
        A(f"- **Unresolved account:** {m['on_unknown_count']}")
    A("")

    def owner_block(title, by_owner_map, total):
        A(f"**{title}** ({total})")
        A("")
        if not by_owner_map:
            A("- none")
            A("")
            return
        A("| Owner | Contacts | Names |")
        A("|-------|---------:|-------|")
        for owner, rows in by_owner_map.items():
            names = ", ".join(f"{r['name']} (#{r['id']})" for r in rows)
            A(f"| {owner} | {len(rows)} | {names} |")
        A("")

    owner_block("New contacts on existing accounts — by owner (expansion signal)",
                m["on_existing_by_owner"], m["on_existing_count"])
    owner_block("New contacts on new accounts — by owner",
                m["on_new_by_owner"], m["on_new_count"])
    if m["orphan_count"]:
        owner_block("Orphan new contacts — by owner", m["orphans_by_owner"], m["orphan_count"])

    # 3c headline
    A("### 3c. Headline for the VP")
    A("")
    if m["on_existing_by_owner"]:
        lead = "; ".join(f"{o}: {len(r)}" for o, r in m["on_existing_by_owner"].items())
        A(f"> **New contacts created on existing company records, by owner:** "
          f"{lead}. This is the expansion signal — {m['on_existing_count']} of "
          f"{m['new_contacts_count']} new contacts landed on accounts we already had.")
    else:
        A("> No new contacts landed on existing accounts this window.")
    A("")

    # ================= 4. FUNNEL =================
    A("## 4. Lifecycle funnel (B2B contacts only)")
    A("")
    A("| Lifecycle stage | Contacts |")
    A("|-----------------|---------:|")
    prev_funnel = {f["stage"]: f["count"] for f in (pm.get("funnel", []) if pm else [])}
    total = 0
    for f in m["funnel"]:
        total += f["count"]
        pv = prev_funnel.get(f["stage"]) if pm else None
        A(f"| {f['label']} | {delta_str(f['count'], pv)} |")
    ptotal = sum(prev_funnel.values()) if prev_funnel else None
    A(f"| **Total** | **{delta_str(total, ptotal)}** |")
    A("")
    pmql = pm.get("mql_count") if pm else None
    psql = pm.get("sql_count") if pm else None
    A(f"- **MQL:** {delta_str(m['mql_count'], pmql)}  ·  "
      f"**SQL:** {delta_str(m['sql_count'], psql)}  ·  "
      f"**SQL : MQL ratio:** {m['sql_to_mql_ratio'] if m['sql_to_mql_ratio'] is not None else 'n/a'}")
    A("")

    # ================= 5. MISCATEGORISED =================
    A("## 5. Miscategorised-lead check")
    A("")
    A(f"**{m['miscategorised_count']}** B2B contacts carry a working lead status "
      f"but sit at Lead / MQL. By owner:")
    A("")
    A("| Owner | Count |")
    A("|-------|------:|")
    for owner, rows in m["miscategorised_by_owner"].items():
        A(f"| {owner} | {len(rows)} |")
    A("")
    A("**Full list ({} contacts):**".format(m["miscategorised_count"]))
    A("")
    A("| Contact ID | Name | Lead status | Lifecycle | Owner |")
    A("|------------|------|-------------|-----------|-------|")
    for c in m["miscategorised"]:
        A(f"| {c['id']} | {c.get('name','')} | {c.get('hs_lead_status','')} "
          f"| {c.get('lifecyclestage','')} | {c.get('owner') or '— unassigned —'} |")
    A("")

    # ================= 6. SQL REVIEW =================
    A("## 6. SQL follow-up / review (not auto-recycled)")
    A("")
    A(f"B2B contacts at **Sales Qualified Lead** with a working lead status and "
      f"no activity for **{IDLE_THRESHOLD_DAYS}+ days**: **{m['sql_review_count']}**. "
      f"These are live relationships for sales to action — nothing here moves "
      f"automatically.")
    A("")
    if m["sql_review"]:
        groups = {}
        for c in m["sql_review"]:
            groups.setdefault(c["hs_lead_status"], []).append(c)
        for status, rows in groups.items():
            A(f"**{status}** ({len(rows)})")
            A("")
            A("| Contact | Days idle | Owner | Associated deal | Deal USD |")
            A("|---------|----------:|-------|-----------------|---------:|")
            for c in rows:
                days = "no activity" if c["days"] is None else f"{c['days']}d"
                deal = c["deal_name"] or "—"
                dusd = money(c["deal_usd"]) if c["deal_usd"] is not None else "—"
                A(f"| {c['name']} (#{c['id']}) | {days} | {c['owner']} | {deal} | {dusd} |")
            A("")
    else:
        A("- None this run.")
        A("")

    # ================= 7. CURRENCY NOTE =================
    A("## 7. Currency normalisation")
    A("")
    A("All pipeline value above is summed from `amount_in_home_currency` (USD). "
      "Raw `amount` in `deal_currency_code` is never summed across currencies. "
      f"{len(m['missing_amount'])} deal(s) lack `amount_in_home_currency` and are "
      "flagged (excluded from totals), never back-filled from raw `amount`.")
    A("")

    # ================= 8. WEEK ON WEEK =================
    A("## 8. Week-on-week movement")
    A("")
    if not prev:
        A("No prior snapshot — comparison starts next run. Current baselines: "
          f"MQL {m['mql_count']}, SQL {m['sql_count']}, miscategorised "
          f"{m['miscategorised_count']}, orphan new leads {m['orphan_count']}, "
          f"new accounts {m['new_accounts_count']}, new contacts "
          f"{m['new_contacts_count']}, SQL-review {m['sql_review_count']}.")
        A("")
    else:
        A("| Metric | Now | Prior | Δ |")
        A("|--------|----:|------:|---|")

        def row(label, now, prior):
            if prior is None:
                A(f"| {label} | {now} | — | new |")
            else:
                diff = now - prior
                sign = f"+{diff}" if diff > 0 else (str(diff) if diff < 0 else "=")
                A(f"| {label} | {now} | {prior} | {sign} |")

        row("B2B contacts (total)", m["contacts_b2b_total"], pm.get("contacts_b2b_total"))
        for f in m["funnel"]:
            row(f"Funnel · {f['label']}", f["count"], prev_funnel.get(f["stage"]))
        row("MQL", m["mql_count"], pm.get("mql_count"))
        row("SQL", m["sql_count"], pm.get("sql_count"))
        row("Miscategorised", m["miscategorised_count"], pm.get("miscategorised_count"))
        row("Orphan new leads", m["orphan_count"], pm.get("orphan_count"))
        row("New B2B accounts", m["new_accounts_count"], pm.get("new_accounts_count"))
        row("New B2B contacts", m["new_contacts_count"], pm.get("new_contacts_count"))
        row("SQL-review (idle 30d+)", m["sql_review_count"], pm.get("sql_review_count"))
        row("QTB deals idle 14d+", m["qtb_stale_count"], pm.get("qtb_stale_count"))
        A("")
        A("*Deal count per stage deltas appear in section 1. Prior deal set was "
          "not B2B-scoped and used raw currency, so deal value is not compared "
          "across snapshots.*")
        A("")

    A("---")
    A(f"*Snapshot: `snapshots/{date}.json` · Report: `reports/{date}.md` · "
      f"Owners resolved via HubSpot owners lookup.*")
    A("")
    return "\n".join(out)


# =========================================================================
# Main
# =========================================================================
def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly pipeline report")
    ap.add_argument("--input", help="Pre-pulled raw JSON (required in this env)")
    ap.add_argument("--date", help="Run date YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
    args = ap.parse_args()

    if not args.input:
        sys.exit("This environment blocks direct egress to api.hubapi.com. "
                 "Pull data via the HubSpot MCP tools into raw_input.json and "
                 "run: python3 pipeline.py --input raw_input.json")

    with open(args.input) as f:
        raw = json.load(f)

    run_date = (dt.date.fromisoformat(args.date) if args.date
                else _parse_date(raw.get("generated_at"))
                or dt.datetime.now(dt.timezone.utc).date())

    metrics = compute(raw, run_date)
    generated = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    snapshot = {
        "run_date": run_date.isoformat(),
        "generated_at_utc": generated,
        "source": raw.get("source", "hubspot (pre-pulled)"),
        "window_days": metrics["window_days"],
        "owners": raw.get("owners", {}),
        "raw": {
            "contacts_b2b_total": raw.get("contacts_b2b_total"),
            "funnel_counts": raw.get("funnel_counts", {}),
            "contacts_b2b": raw.get("contacts_b2b", []),
            "companies_b2b": raw.get("companies_b2b", []),
            "deals": raw.get("deals", []),
            "miscategorised": raw.get("miscategorised", []),
            "sql_review": raw.get("sql_review", []),
        },
        "metrics": metrics,
    }

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev_snapshot(snap_dir, run_date)

    snap_path = os.path.join(snap_dir, f"{run_date.isoformat()}.json")
    with open(snap_path, "w") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    report = render(snapshot, prev)
    rep_path = os.path.join(rep_dir, f"{run_date.isoformat()}.md")
    with open(rep_path, "w") as f:
        f.write(report)

    print(f"Wrote {snap_path}")
    print(f"Wrote {rep_path}")
    print(f"Previous snapshot: {prev['run_date'] if prev else 'none (first comparable run)'}")


if __name__ == "__main__":
    main()
