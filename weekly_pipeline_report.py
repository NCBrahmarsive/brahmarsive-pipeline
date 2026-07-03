#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report.

Pulls contacts, deals and companies from HubSpot, scopes everything to B2B,
computes the funnel / MQL-SQL health / deal value by stage / alerts, compares
against the most recent prior snapshot (week-over-week) and writes:

    snapshots/YYYY-MM-DD.json   raw pulled data + computed metrics
    reports/YYYY-MM-DD.md       readable leadership summary

Two data sources are supported:

    --api            fetch live from the HubSpot CRM v3 API using $HUBSPOT_TOKEN
                     (HubSpot Service Key; scopes crm.objects.contacts.read +
                     crm.objects.deals.read + crm.objects.companies.read).

    --input FILE     read an already-pulled raw dataset (same shape the --api
                     path produces internally). Used when direct egress to
                     api.hubapi.com is blocked (e.g. the managed Claude-Code web
                     session, whose egress policy denies the HubSpot host); in
                     that case the weekly routine pulls via the HubSpot MCP tools
                     and hands the raw JSON to this script.

The token value is never printed or written to disk.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

# =============================================================================
# EDITABLE CONFIG  --  tune the business rules here.
# =============================================================================

# HubSpot account default (home) currency. All value maths use the USD-converted
# `amount_in_home_currency`; raw `amount` is shown only as a secondary reference.
HOME_CURRENCY = "USD"

# --- Thresholds (days) -------------------------------------------------------
MOVED_WINDOW_DAYS = 7      # "what moved this week"
QTB_STALE_DAYS = 14        # alert: Qualified-to-Buy deal with no activity in N+ days
STALLED_SQL_DAYS = 30      # follow-up/review: engaged SQL gone quiet for N+ days

# --- Lifecycle stage: internal value -> display label ------------------------
LIFECYCLE_LABELS = {
    "subscriber": "Email Contact",
    "lead": "Lead",
    "marketingqualifiedlead": "Marketing Qualified Lead",
    "salesqualifiedlead": "Sales Qualified Lead",
    "opportunity": "Opportunity",
    "customer": "Customer",
    "other": "Other (partners)",
}
# Funnel display order.
LIFECYCLE_ORDER = [
    "subscriber", "lead", "marketingqualifiedlead",
    "salesqualifiedlead", "opportunity", "customer", "other",
]
# Pre-deal lifecycle stages are contact counts only (no deal value attached).
PRE_DEAL_STAGES = {"subscriber", "lead", "marketingqualifiedlead"}

# --- Deal stage: internal value -> display label (Sales Pipeline) ------------
DEALSTAGE_LABELS = {
    "1217017229": "Initial Engagement",
    "appointmentscheduled": "Qualified to Buy",
    "presentationscheduled": "Discovery & Trial",
    "1217017230": "Quote Sent",
    "decisionmakerboughtin": "Negotiation",
    "contractsent": "Commitment to Buy",
    "closedwon": "Closed Won",
    "closedlost": "Closed Lost",
    # Marketing / lifecycle pipeline stages (kept for completeness):
    "1212087329": "Subscribers",
    "1311777171": "Marketing Engaged",
    "1212087330": "Free Trial",
    "1211945117": "Closed Won (Lifecycle)",
    "1211945118": "Closed Lost (Lifecycle)",
}
DEALSTAGE_ORDER = [
    "1217017229", "appointmentscheduled", "presentationscheduled",
    "1217017230", "decisionmakerboughtin", "contractsent",
    "closedwon", "closedlost",
]
# The internal value HubSpot uses for the "Qualified to Buy" stage.
QUALIFIED_TO_BUY_STAGE = "appointmentscheduled"
CLOSED_STAGES = {"closedwon", "closedlost", "1211945117", "1211945118"}

# --- hs_lead_status internal values that mean "sales is actively working it" -
# Used by the miscategorised-lead check below. (Contact-object values.)
SALES_WORKING_STATUSES = {
    "Qualified", "CONNECTED", "Attempted to Contact",
    "IN_PROGRESS", "PROPOSAL_REQUEST",
}
# Lifecycle stages that are "too early" given a sales-working status.
NOT_YET_ADVANCED_STAGES = {"lead", "marketingqualifiedlead"}

# --- Stalled-SQL follow-up: engaged-but-quiet lead statuses ------------------
# Distinct from the auto-recycled "Attempted to Contact" cases on purpose.
STALLED_ENGAGED_STATUSES = {"CONNECTED", "Qualified", "IN_PROGRESS", "PROPOSAL_REQUEST"}

# Human-readable labels for lead statuses (contact + company enums combined).
LEAD_STATUS_LABELS = {
    "NEW": "New Lead",
    "Marketing Engaged": "Marketing Engaged",
    "MQL": "MQL",
    "Attempted to Contact": "Attempted to Contact",
    "ATTEMPTED_TO_CONTACT": "Attempted to Contact",
    "CONNECTED": "Connected",
    "Qualified": "Qualified",
    "IN_PROGRESS": "In Progress",
    "PROPOSAL_REQUEST": "Proposal Request",
    "UNQUALIFIED": "Unqualified",
    "FUTURE_LEAD": "Future Lead",
    "BAD_TIMING": "Bad Timing",
    "Onboarded Partner": "Onboarded Partner",
}

# Owner id -> name (from the HubSpot owner enum).
OWNER_MAP = {
    "83228721": "Nicola Cole",
    "83249199": "Ramjee Jadu",
    "83361077": "Vrinda Vinod",
    "84004434": "Anup Ghoshal",
    "84004493": "Theo Zirogiannis",
    "85402717": "Vangili Palaniappan",
    "87058564": "Al Mufeed Khazi",
    "87318764": "Jack Makhlouf",
    "87318803": "Biju Rajesh",
    "88914104": "SASHI KUMAR",
    "90325392": "Panthalarajakumaran Kamaraj",
    "90325421": "Aksharaa Parameswaran",
}


def is_miscategorised(contact: dict) -> bool:
    """MISCATEGORISED-LEAD CHECK (editable).

    A B2B contact is miscategorised when sales is clearly working it
    (hs_lead_status in SALES_WORKING_STATUSES) but its lifecyclestage is still
    only `lead` or `marketingqualifiedlead` -- i.e. it has NOT been advanced to
    salesqualifiedlead or beyond. Edit SALES_WORKING_STATUSES /
    NOT_YET_ADVANCED_STAGES above to tune.
    """
    return (
        contact.get("hs_lead_status") in SALES_WORKING_STATUSES
        and contact.get("lifecyclestage") in NOT_YET_ADVANCED_STAGES
    )


# =============================================================================
# Helpers
# =============================================================================

def owner_name(owner_id: str | None) -> str:
    if not owner_id:
        return "(unassigned)"
    return OWNER_MAP.get(str(owner_id), f"owner {owner_id}")


def lead_status_label(value: str | None) -> str:
    if not value:
        return "(none)"
    return LEAD_STATUS_LABELS.get(value, value)


def parse_ts(value) -> dt.datetime | None:
    """Parse a HubSpot ISO-8601 timestamp into an aware UTC datetime."""
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        d = dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc)


def to_float(value):
    """Home-currency amount as float, or None if empty/absent."""
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fmt_usd(value) -> str:
    if value is None:
        return "n/a"
    return f"${value:,.0f}"


def fmt_delta(cur, prev, money=False) -> str:
    """Render 'cur (+/-delta)' vs prior snapshot, or 'cur (new)' if no prior."""
    render = fmt_usd if money else (lambda v: f"{v:,}")
    if prev is None:
        return f"{render(cur)} (—, first snapshot)"
    diff = cur - prev
    if diff == 0:
        return f"{render(cur)} (±0)"
    sign = "+" if diff > 0 else "−"
    return f"{render(cur)} ({sign}{render(abs(diff))})"


# =============================================================================
# Data acquisition
# =============================================================================

CONTACT_PROPS = ["firstname", "lastname", "email", "lifecyclestage",
                 "hs_lead_status", "customer_type", "hs_lastmodifieddate",
                 "notes_last_updated", "hubspot_owner_id", "num_associated_deals"]
DEAL_PROPS = ["dealname", "dealstage", "amount", "amount_in_home_currency",
              "deal_currency_code", "hs_lastmodifieddate", "notes_last_updated",
              "hubspot_owner_id", "pipeline"]
COMPANY_PROPS = ["name", "lifecyclestage", "hs_lead_status",
                 "customer_type__cloned_", "notes_last_updated",
                 "hubspot_owner_id", "num_associated_deals"]


def _api_search(session, base, object_type, properties, filter_groups):
    """Paginate a CRM v3 search, returning all matching records."""
    url = f"{base}/crm/v3/objects/{object_type}/search"
    out, after = [], None
    while True:
        body = {"properties": properties, "limit": 200,
                "filterGroups": filter_groups}
        if after:
            body["after"] = after
        r = session.post(url, json=body, timeout=60)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("results", []))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            return out


def fetch_from_api() -> dict:
    """Live pull from HubSpot. Requires `requests` and $HUBSPOT_TOKEN."""
    import requests  # imported lazily so --input mode has no dependency

    token = os.environ.get("HUBSPOT_TOKEN")
    if not token:
        sys.exit("HUBSPOT_TOKEN is not set in the environment.")
    base = "https://api.hubapi.com"
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}",
                      "Content-Type": "application/json"})

    b2b_contact = [{"filters": [{"propertyName": "customer_type",
                                 "operator": "EQ", "value": "B2B"}]}]
    contacts = _api_search(s, base, "contacts", CONTACT_PROPS, b2b_contact)
    deals = _api_search(s, base, "deals", DEAL_PROPS, [])
    b2b_company = [{"filters": [{"propertyName": "customer_type__cloned_",
                                 "operator": "EQ", "value": "B2B"}]}]
    companies = _api_search(s, base, "companies", COMPANY_PROPS, b2b_company)

    # Build the same raw shape the --input path expects.
    funnel = defaultdict(int)
    miscategorised, stalled = [], []
    now = dt.datetime.now(dt.timezone.utc)
    stall_cut = now - dt.timedelta(days=STALLED_SQL_DAYS)
    for c in contacts:
        p = c.get("properties", {})
        stage = p.get("lifecyclestage") or "unknown"
        funnel[stage] += 1
        if is_miscategorised(p):
            miscategorised.append({
                "id": c.get("id"),
                "name": (f"{p.get('firstname','')} {p.get('lastname','')}".strip()
                         or p.get("email") or c.get("id")),
                "email": p.get("email"),
                "lifecyclestage": p.get("lifecyclestage"),
                "hs_lead_status": p.get("hs_lead_status"),
                "hubspot_owner_id": p.get("hubspot_owner_id"),
            })
        last = parse_ts(p.get("notes_last_updated"))
        if (p.get("lifecyclestage") == "salesqualifiedlead"
                and p.get("hs_lead_status") in STALLED_ENGAGED_STATUSES
                and last is not None and last < stall_cut):
            stalled.append({
                "object": "contact", "id": c.get("id"),
                "name": (f"{p.get('firstname','')} {p.get('lastname','')}".strip()
                         or p.get("email") or c.get("id")),
                "hs_lead_status": p.get("hs_lead_status"),
                "notes_last_updated": p.get("notes_last_updated"),
                "hubspot_owner_id": p.get("hubspot_owner_id"),
                "num_associated_deals": p.get("num_associated_deals"),
            })
    for co in companies:
        p = co.get("properties", {})
        last = parse_ts(p.get("notes_last_updated"))
        if (p.get("lifecyclestage") == "salesqualifiedlead"
                and p.get("hs_lead_status") in STALLED_ENGAGED_STATUSES
                and last is not None and last < stall_cut):
            stalled.append({
                "object": "company", "id": co.get("id"),
                "name": p.get("name") or co.get("id"),
                "hs_lead_status": p.get("hs_lead_status"),
                "notes_last_updated": p.get("notes_last_updated"),
                "hubspot_owner_id": p.get("hubspot_owner_id"),
                "num_associated_deals": p.get("num_associated_deals"),
            })

    return {
        "pull_date": now.date().isoformat(),
        "account": {"home_currency": HOME_CURRENCY},
        "counts": {"contacts_b2b_total": len(contacts),
                   "companies_b2b_total": len(companies),
                   "deals_total": len(deals)},
        "funnel": dict(funnel),
        "miscategorised": miscategorised,
        "stalled_sqls": stalled,
        "deals": [{"id": d.get("id"), **d.get("properties", {})} for d in deals],
    }


def load_raw(path: str) -> dict:
    return json.loads(Path(path).read_text())


# =============================================================================
# Analysis
# =============================================================================

def find_prior_snapshot(snap_dir: Path, current_date: str) -> dict | None:
    if not snap_dir.exists():
        return None
    candidates = sorted(p for p in snap_dir.glob("*.json")
                        if p.stem < current_date)
    if not candidates:
        return None
    data = json.loads(candidates[-1].read_text())
    # snapshots are stored as {"raw": ..., "metrics": ...}; compare on metrics.
    return data.get("metrics", data)


def analyze(raw: dict, run_date: dt.date, prior: dict | None) -> dict:
    now = dt.datetime.combine(run_date, dt.time(0, 0), dt.timezone.utc)
    moved_cut = now - dt.timedelta(days=MOVED_WINDOW_DAYS)
    qtb_cut = now - dt.timedelta(days=QTB_STALE_DAYS)

    funnel = {k: int(v) for k, v in raw.get("funnel", {}).items()}
    mql = funnel.get("marketingqualifiedlead", 0)
    sql = funnel.get("salesqualifiedlead", 0)
    ratio = round(sql / mql, 2) if mql else None

    # ---- Deals: per-stage count + USD value; flag missing home currency ----
    deals = raw.get("deals", [])
    by_stage = defaultdict(lambda: {"count": 0, "value_usd": 0.0,
                                    "orig_by_ccy": defaultdict(float)})
    missing_home_ccy = []
    prior_stage = (prior or {}).get("per_deal_stage", {})
    per_deal_stage = {}
    moved, qtb_stale = [], []

    for d in deals:
        did = str(d.get("id"))
        stage = d.get("dealstage") or "unknown"
        per_deal_stage[did] = stage
        home = to_float(d.get("amount_in_home_currency"))
        orig = to_float(d.get("amount"))
        ccy = d.get("deal_currency_code") or "?"
        bs = by_stage[stage]
        bs["count"] += 1
        if home is None:
            missing_home_ccy.append({
                "id": did, "dealname": d.get("dealname"),
                "stage": stage, "deal_currency_code": ccy,
                "amount": d.get("amount"),
            })
        else:
            bs["value_usd"] += home
            if orig is not None:
                bs["orig_by_ccy"][ccy] += orig

        lastmod = parse_ts(d.get("hs_lastmodifieddate"))
        activity = parse_ts(d.get("notes_last_updated"))

        # ---- moved this week ----
        moved_by_mod = lastmod is not None and lastmod >= moved_cut
        moved_by_act = activity is not None and activity >= moved_cut
        if moved_by_mod or moved_by_act:
            prev_st = prior_stage.get(did)
            if prev_st and prev_st != stage:
                change = (f"Stage change: {DEALSTAGE_LABELS.get(prev_st, prev_st)}"
                          f" → {DEALSTAGE_LABELS.get(stage, stage)}")
            elif moved_by_act and moved_by_mod:
                change = "Activity logged + field update"
            elif moved_by_act:
                change = "Activity logged"
            else:
                change = "Field update (no logged activity)"
            moved.append({
                "id": did, "dealname": d.get("dealname"), "stage": stage,
                "value_usd": home, "deal_currency_code": ccy, "amount": orig,
                "change": change,
                "last_activity": d.get("notes_last_updated"),
                "last_modified": d.get("hs_lastmodifieddate"),
                "owner": owner_name(d.get("hubspot_owner_id")),
            })

        # ---- alert: Qualified to Buy, no activity in 14+ days ----
        if stage == QUALIFIED_TO_BUY_STAGE:
            if activity is None or activity < qtb_cut:
                days_idle = (None if activity is None
                             else (now - activity).days)
                qtb_stale.append({
                    "id": did, "dealname": d.get("dealname"),
                    "value_usd": home, "deal_currency_code": ccy,
                    "amount": orig,
                    "last_activity": d.get("notes_last_updated"),
                    "days_idle": days_idle,
                    "owner": owner_name(d.get("hubspot_owner_id")),
                })

    # normalise stage dicts
    deals_by_stage = {}
    for stage, bs in by_stage.items():
        deals_by_stage[stage] = {
            "count": bs["count"],
            "value_usd": round(bs["value_usd"], 2),
            "orig_by_ccy": {k: round(v, 2) for k, v in bs["orig_by_ccy"].items()},
        }

    open_value = sum(v["value_usd"] for s, v in deals_by_stage.items()
                     if s not in CLOSED_STAGES)
    open_count = sum(v["count"] for s, v in deals_by_stage.items()
                     if s not in CLOSED_STAGES)
    won_value = deals_by_stage.get("closedwon", {}).get("value_usd", 0.0)
    won_count = deals_by_stage.get("closedwon", {}).get("count", 0)

    # ---- stalled SQLs: compute days idle, group by lead status ----
    stalled = []
    for r in raw.get("stalled_sqls", []):
        last = parse_ts(r.get("notes_last_updated"))
        days_idle = (now - last).days if last else None
        stalled.append({**r,
                        "days_idle": days_idle,
                        "owner": owner_name(r.get("hubspot_owner_id"))})
    stalled.sort(key=lambda x: (x["days_idle"] is not None, x["days_idle"] or 0),
                 reverse=True)

    miscategorised = raw.get("miscategorised", [])

    # ---- sort moved: group by stage (by total moved value desc), value desc --
    moved.sort(key=lambda m: (m["value_usd"] or 0), reverse=True)
    qtb_stale.sort(key=lambda x: (x["value_usd"] or 0), reverse=True)

    return {
        "date": run_date.isoformat(),
        "generated_at": now.isoformat(),
        "home_currency": HOME_CURRENCY,
        "counts": raw.get("counts", {}),
        "funnel": funnel,
        "mql_sql": {"mql": mql, "sql": sql, "ratio": ratio,
                    "inverted": sql > mql},
        "deals_by_stage": deals_by_stage,
        "deal_totals": {"open_count": open_count, "open_value": round(open_value, 2),
                        "won_count": won_count, "won_value": won_value},
        "miscategorised_count": len(miscategorised),
        "miscategorised": miscategorised,
        "stalled_sql_count": len(stalled),
        "stalled_sqls": stalled,
        "deals_missing_home_currency": missing_home_ccy,
        "qtb_stale": qtb_stale,
        "deals_moved_this_week": moved,
        "per_deal_stage": per_deal_stage,
    }


# =============================================================================
# Report rendering
# =============================================================================

def render_report(m: dict, prior: dict | None) -> str:
    d = m["date"]
    L = []
    p_funnel = (prior or {}).get("funnel", {})
    p_mqlsql = (prior or {}).get("mql_sql", {})
    p_stages = (prior or {}).get("deals_by_stage", {})
    p_misc = (prior or {}).get("miscategorised_count")
    p_stall = (prior or {}).get("stalled_sql_count")

    L.append(f"# Brahmarsive Weekly B2B Pipeline & MQL Health — {d}")
    L.append("")
    if prior:
        L.append(f"_Week-over-week vs prior snapshot **{prior.get('date')}**. "
                 f"All deal values in {m['home_currency']} "
                 f"(`amount_in_home_currency`)._")
    else:
        L.append(f"_First snapshot — week-over-week comparison starts next run. "
                 f"All deal values in {m['home_currency']} "
                 f"(`amount_in_home_currency`)._")
    L.append("")

    # ---------------- ALERTS ----------------
    L.append("## 🚨 Alerts")
    L.append("")
    ms = m["mql_sql"]
    if ms["inverted"]:
        L.append(f"- **⚠️ Funnel inversion: SQL ({ms['sql']:,}) > MQL "
                 f"({ms['mql']:,}).** More contacts sit in Sales Qualified than "
                 f"Marketing Qualified — MQL is either being skipped on the way "
                 f"in or under-populated. Ratio SQL/MQL = {ms['ratio']}.")
    misc_line = fmt_delta(m["miscategorised_count"], p_misc)
    L.append(f"- **Miscategorised leads: {misc_line}.** "
             f"Sales-working status but still stuck at Lead/MQL "
             f"(see §5).")
    stall_line = fmt_delta(m["stalled_sql_count"], p_stall)
    L.append(f"- **Stalled engaged SQLs (>{STALLED_SQL_DAYS}d quiet): "
             f"{stall_line}.** Live relationships for sales to action "
             f"(see §6) — follow-up/review, NOT auto-recycle.")
    qtb = m["qtb_stale"]
    if qtb:
        total = sum(x["value_usd"] or 0 for x in qtb)
        L.append(f"- **Qualified-to-Buy deals idle {QTB_STALE_DAYS}+ days: "
                 f"{len(qtb)}** (≈{fmt_usd(total)} exposed). See §7.")
    else:
        L.append(f"- Qualified-to-Buy deals idle {QTB_STALE_DAYS}+ days: **0**.")
    mh = m["deals_missing_home_currency"]
    if mh:
        L.append(f"- **Deals missing `amount_in_home_currency`: {len(mh)}** — "
                 f"excluded from USD totals rather than summed at face value "
                 f"(see §8).")
    L.append("")

    # ---------------- FUNNEL ----------------
    L.append("## 1. Lifecycle funnel (B2B contacts)")
    L.append("")
    L.append("| Lifecycle stage | Contacts | WoW |")
    L.append("|---|--:|:--|")
    total = 0
    for st in LIFECYCLE_ORDER:
        cur = m["funnel"].get(st, 0)
        total += cur
        prev = p_funnel.get(st) if prior else None
        wow = _wow_cell(cur, prev, prior)
        L.append(f"| {LIFECYCLE_LABELS.get(st, st)} | {cur:,} | {wow} |")
    L.append(f"| **Total B2B contacts** | **{total:,}** |  |")
    L.append("")

    # ---------------- MQL / SQL ----------------
    L.append("## 2. MQL vs SQL health")
    L.append("")
    L.append(f"- **MQL:** {fmt_delta(ms['mql'], p_mqlsql.get('mql') if prior else None)}")
    L.append(f"- **SQL:** {fmt_delta(ms['sql'], p_mqlsql.get('sql') if prior else None)}")
    L.append(f"- **SQL / MQL ratio:** {ms['ratio']}"
             + ("  — ⚠️ **inverted (SQL > MQL)**" if ms["inverted"] else ""))
    L.append("")

    # ---------------- DEAL VALUE BY STAGE ----------------
    L.append("## 3. Deal count & value by stage")
    L.append("")
    L.append(f"Values in {m['home_currency']} via `amount_in_home_currency`. "
             "Pre-deal lifecycle stages (Email Contact, Lead, MQL) are contact "
             "counts only and carry no deal value.")
    L.append("")
    L.append("| Deal stage | Deals | Value (USD) | WoW deals | WoW value | Orig. currency ref |")
    L.append("|---|--:|--:|:--|:--|---|")
    stages_seen = [s for s in DEALSTAGE_ORDER if s in m["deals_by_stage"]]
    stages_seen += [s for s in m["deals_by_stage"] if s not in DEALSTAGE_ORDER]
    tot_count = tot_value = 0
    for st in stages_seen:
        v = m["deals_by_stage"][st]
        tot_count += v["count"]
        tot_value += v["value_usd"]
        pv = p_stages.get(st) if prior else None
        wc = _wow_cell(v["count"], (pv or {}).get("count") if pv else None, prior)
        wv = _wow_cell(v["value_usd"], (pv or {}).get("value_usd") if pv else None,
                       prior, money=True)
        orig = ", ".join(f"{ccy} {amt:,.0f}"
                         for ccy, amt in sorted(v["orig_by_ccy"].items())
                         if ccy != HOME_CURRENCY) or "—"
        L.append(f"| {DEALSTAGE_LABELS.get(st, st)} | {v['count']} | "
                 f"{fmt_usd(v['value_usd'])} | {wc} | {wv} | {orig} |")
    L.append(f"| **All stages** | **{tot_count}** | **{fmt_usd(tot_value)}** |  |  |  |")
    L.append("")
    dt_ = m["deal_totals"]
    L.append(f"- **Open pipeline:** {dt_['open_count']} deals, "
             f"{fmt_usd(dt_['open_value'])}.")
    L.append(f"- **Closed Won:** {dt_['won_count']} deals, "
             f"{fmt_usd(dt_['won_value'])}.")
    L.append("")

    # ---------------- MOVED THIS WEEK ----------------
    L.append(f"## 4. What moved this week (last {MOVED_WINDOW_DAYS} days)")
    L.append("")
    moved = m["deals_moved_this_week"]
    if not moved:
        L.append("_No deals modified or with logged activity in the window._")
    else:
        L.append(f"{len(moved)} deal(s) active this week — grouped by current "
                 "stage, highest USD value first.")
        L.append("")
        groups = defaultdict(list)
        for x in moved:
            groups[x["stage"]].append(x)
        ordered = sorted(groups.items(),
                         key=lambda kv: sum(z["value_usd"] or 0 for z in kv[1]),
                         reverse=True)
        for st, items in ordered:
            gtot = sum(z["value_usd"] or 0 for z in items)
            L.append(f"### {DEALSTAGE_LABELS.get(st, st)} — {len(items)} deal(s), "
                     f"{fmt_usd(gtot)}")
            L.append("")
            L.append("| Deal | Value (USD) | What changed | Last activity | Owner |")
            L.append("|---|--:|---|---|---|")
            for z in sorted(items, key=lambda z: z["value_usd"] or 0, reverse=True):
                la = (z["last_activity"] or "—")[:10]
                val = fmt_usd(z["value_usd"])
                if z["deal_currency_code"] and z["deal_currency_code"] != HOME_CURRENCY \
                        and z["amount"] is not None:
                    val += f" _({z['deal_currency_code']} {z['amount']:,.0f})_"
                L.append(f"| {_clip(z['dealname'])} | {val} | {z['change']} | "
                         f"{la} | {z['owner']} |")
            L.append("")

    # ---------------- MISCATEGORISED ----------------
    L.append("## 5. Miscategorised leads")
    L.append("")
    L.append("Sales-working `hs_lead_status` (Qualified / Connected / Attempted "
             "to Contact / In Progress / Proposal Request) but lifecycle still "
             "only Lead or MQL — should be advanced to Sales Qualified.")
    L.append("")
    misc = m["miscategorised"]
    L.append(f"**Count: {fmt_delta(m['miscategorised_count'], p_misc)}**")
    L.append("")
    if misc:
        L.append("| Contact ID | Name | Lifecycle | Lead status | Owner |")
        L.append("|---|---|---|---|---|")
        for c in misc:
            L.append(f"| `{c['id']}` | {_clip(c.get('name') or '')} | "
                     f"{LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage'))} | "
                     f"{lead_status_label(c.get('hs_lead_status'))} | "
                     f"{owner_name(c.get('hubspot_owner_id'))} |")
        L.append("")
        L.append("Contact IDs: " + ", ".join(f"`{c['id']}`" for c in misc))
    else:
        L.append("_None this week._")
    L.append("")

    # ---------------- STALLED SQLs ----------------
    L.append(f"## 6. Stalled engaged SQLs — follow-up / review "
             f"(>{STALLED_SQL_DAYS} days quiet)")
    L.append("")
    L.append("**These are live relationships for sales to action — this is a "
             "review list, NOT auto-recycle. Nothing here is moved "
             "automatically.** Sales Qualified B2B contacts/companies that were "
             "engaged (Connected / Qualified / In Progress / Proposal Request) "
             f"but have had no logged activity for {STALLED_SQL_DAYS}+ days. "
             "Sorted most-stalled first.")
    L.append("")
    stalled = m["stalled_sqls"]
    L.append(f"**Count: {fmt_delta(m['stalled_sql_count'], p_stall)}**")
    L.append("")
    if stalled:
        by_status = defaultdict(list)
        for r in stalled:
            by_status[r.get("hs_lead_status")].append(r)
        for status in sorted(by_status,
                             key=lambda s: len(by_status[s]), reverse=True):
            items = by_status[status]
            L.append(f"### {lead_status_label(status)} — {len(items)}")
            L.append("")
            L.append("| Record | Type | Days idle | Owner | Deal | Deal value (USD) |")
            L.append("|---|---|--:|---|---|--:|")
            for r in items:
                deal = r.get("deal_name") or "—"
                dval = fmt_usd(r["deal_value_usd"]) if r.get("deal_value_usd") is not None else "—"
                L.append(f"| {_clip(r.get('name') or '')} | {r.get('object')} | "
                         f"{r.get('days_idle') if r.get('days_idle') is not None else '?'} | "
                         f"{r.get('owner')} | {deal} | {dval} |")
            L.append("")
    else:
        L.append("_None this week._")
    L.append("")

    # ---------------- QTB STALE ----------------
    L.append(f"## 7. Qualified-to-Buy deals idle {QTB_STALE_DAYS}+ days")
    L.append("")
    if qtb:
        L.append("| Deal | Value (USD) | Days idle | Last activity | Owner |")
        L.append("|---|--:|--:|---|---|")
        for x in qtb:
            la = (x["last_activity"] or "never")[:10]
            di = x["days_idle"] if x["days_idle"] is not None else "never"
            L.append(f"| {_clip(x['dealname'])} | {fmt_usd(x['value_usd'])} | "
                     f"{di} | {la} | {x['owner']} |")
    else:
        L.append("_None — every Qualified-to-Buy deal has recent activity._")
    L.append("")

    # ---------------- MISSING HOME CURRENCY ----------------
    L.append("## 8. Deals flagged: missing `amount_in_home_currency`")
    L.append("")
    if mh:
        L.append("Excluded from USD totals (never summed at raw face value "
                 "across currencies).")
        L.append("")
        L.append("| Deal | Stage | Raw amount | Currency |")
        L.append("|---|---|--:|---|")
        for x in mh:
            L.append(f"| {_clip(x['dealname'])} | "
                     f"{DEALSTAGE_LABELS.get(x['stage'], x['stage'])} | "
                     f"{x.get('amount') if x.get('amount') not in (None, '') else '—'} | "
                     f"{x.get('deal_currency_code')} |")
    else:
        L.append("_None — every deal has a USD home-currency value._")
    L.append("")

    L.append("---")
    L.append(f"_Scope: B2B only (`customer_type` = B2B for contacts, "
             f"`customer_type__cloned_` = B2B for companies). "
             f"B2B contacts: {m['counts'].get('contacts_b2b_total','?'):,} · "
             f"B2B companies: {m['counts'].get('companies_b2b_total','?'):,} · "
             f"deals: {m['counts'].get('deals_total','?')}._")
    return "\n".join(L)


def _wow_cell(cur, prev, prior, money=False):
    if not prior:
        return "—"
    if prev is None:
        return "new"
    diff = cur - prev
    render = fmt_usd if money else (lambda v: f"{v:,}")
    if diff == 0:
        return "±0"
    return ("+" if diff > 0 else "−") + render(abs(diff))


def _clip(s: str, n: int = 60) -> str:
    s = (s or "").replace("|", "/").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


# =============================================================================
# Main
# =============================================================================

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--api", action="store_true",
                     help="fetch live from HubSpot using $HUBSPOT_TOKEN")
    src.add_argument("--input", metavar="FILE",
                     help="read an already-pulled raw dataset JSON")
    ap.add_argument("--date", help="run date YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--repo-dir", default=".",
                    help="repo root containing snapshots/ and reports/")
    args = ap.parse_args()

    run_date = (dt.date.fromisoformat(args.date) if args.date
                else dt.datetime.now(dt.timezone.utc).date())
    repo = Path(args.repo_dir)
    snap_dir, rep_dir = repo / "snapshots", repo / "reports"
    snap_dir.mkdir(exist_ok=True)
    rep_dir.mkdir(exist_ok=True)

    raw = fetch_from_api() if args.api else load_raw(args.input)
    prior = find_prior_snapshot(snap_dir, run_date.isoformat())
    metrics = analyze(raw, run_date, prior)

    snap_path = snap_dir / f"{run_date.isoformat()}.json"
    rep_path = rep_dir / f"{run_date.isoformat()}.md"
    snapshot = {"raw": raw, "metrics": metrics}
    snap_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False))
    rep_path.write_text(render_report(metrics, prior))

    print(f"Wrote {snap_path}")
    print(f"Wrote {rep_path}")
    if prior:
        print(f"Compared against prior snapshot: {prior.get('date')}")
    else:
        print("No prior snapshot found — week-over-week starts next run.")


if __name__ == "__main__":
    main()
