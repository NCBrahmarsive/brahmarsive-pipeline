#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report.

What it does
------------
Pulls contacts and deals from HubSpot, filters the funnel to B2B contacts,
and writes two files per run:

  snapshots/YYYY-MM-DD.json  -- the pulled data + computed metrics (machine)
  reports/YYYY-MM-DD.md      -- readable summary with funnel, ratios, deal
                                values, alerts and the miscategorised list

A Word (.docx) copy of the report is also produced via pandoc, using
templates/pipeline_report_template.docx as the reference doc when present.

Each run loads the most recent previous snapshot from snapshots/ and shows the
week-on-week movement for every metric.

Currency
--------
The HubSpot account default (home) currency is USD, but deals may be created in
other currencies (notably INR). All deal *values* are reported and summed using
`amount_in_home_currency` (HubSpot's USD-converted value). The original `amount`
+ `deal_currency_code` are shown only as a secondary reference. Raw amounts are
never summed across mixed currencies. A deal with an empty
`amount_in_home_currency` is flagged, not silently coerced to its raw amount.

Two data sources
----------------
1. Live (default): fetch straight from HubSpot's REST API using the
   HUBSPOT_TOKEN environment variable (a HubSpot Service key with
   crm.objects.contacts.read + crm.objects.deals.read).

       python3 pipeline.py

2. Pre-pulled (--input FILE): consume data already pulled from HubSpot and
   stored as JSON. This is used when direct egress to api.hubapi.com is not
   permitted from the run environment (e.g. an org egress policy blocks it),
   so the data is fetched via the HubSpot MCP tools and handed to the same
   computation code here.

       python3 pipeline.py --input raw_input.json

The token value is never printed or committed.
"""

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import urllib.request
import urllib.parse
import urllib.error

# --------------------------------------------------------------------------
# Reference maps (HubSpot internal value -> human label).
# Query on the internal values; display the labels.
# --------------------------------------------------------------------------

LIFECYCLE_LABELS = {
    "subscriber": "Email Contact",
    "lead": "Lead",
    "marketingqualifiedlead": "Marketing Qualified Lead",
    "salesqualifiedlead": "Sales Qualified Lead",
    "opportunity": "Opportunity",
    "customer": "Customer",
    "other": "Other",
}
# Funnel order for display.
LIFECYCLE_ORDER = [
    "subscriber", "lead", "marketingqualifiedlead",
    "salesqualifiedlead", "opportunity", "customer", "other",
]

DEALSTAGE_LABELS = {
    "1217017229": "Initial Engagement",
    "appointmentscheduled": "Qualified to Buy",
    "presentationscheduled": "Discovery & Trial",
    "1217017230": "Quote Sent",
    "decisionmakerboughtin": "Negotiation",
    "contractsent": "Commitment to Buy",
    "closedwon": "Closed Won",
    "closedlost": "Closed Lost",
    "1212087329": "Subscribers",
    "1311777171": "Marketing Engaged",
    "1212087330": "Free Trial",
    "1211945117": "Closed Won",
    "1211945118": "Closed Lost",
}

# The B2B filter is the single exclusion switch: keeping only customer_type=B2B
# automatically drops B2C, Partner, Vendor, Internal and Partner-B2B contacts.
CUSTOMER_TYPE_FILTER = "B2B"

# HubSpot account (home) currency. All deal totals are reported in this.
HOME_CURRENCY = "USD"

# =========================================================================
# MISCATEGORISED-LEAD CHECK  (edit here to tune the rule)
# =========================================================================
# A contact is "miscategorised" when sales is clearly working it (its
# hs_lead_status is one of the working statuses below) but its lifecyclestage
# has NOT yet advanced past Lead / MQL. Such contacts should already be
# Sales Qualified Lead or beyond.
#
# Values below are HubSpot INTERNAL values (not the display labels):
#   Qualified            -> "Qualified"              (label "Qualified")
#   Connected            -> "CONNECTED"              (label "Contacted")
#   Attempted to contact -> "Attempted to Contact"   (label "Attempted to Contact")
#   In progress          -> "IN_PROGRESS"            (label "In Progress")
#   Proposal request     -> "PROPOSAL_REQUEST"       (label "Proposal Request")
MISCATEGORISED_LEAD_STATUSES = {
    "Qualified",
    "CONNECTED",
    "Attempted to Contact",
    "IN_PROGRESS",
    "PROPOSAL_REQUEST",
}
# Lifecycle stages that are "too early" given the working lead status.
MISCATEGORISED_LIFECYCLE_STAGES = {"lead", "marketingqualifiedlead"}


def is_miscategorised(props):
    """Return True if this contact looks miscategorised (see rule above)."""
    return (
        props.get("hs_lead_status") in MISCATEGORISED_LEAD_STATUSES
        and props.get("lifecyclestage") in MISCATEGORISED_LIFECYCLE_STAGES
    )

# =========================================================================
# STALE-ENGAGED-SQL CHECK (SQLs sales has engaged but gone quiet)
# =========================================================================
# Distinct from the auto-recycled "Attempted to Contact" cases (handled by
# the separate HubSpot stale-lead recycler workflow). These are SQLs where
# sales has already made real contact/progress, but activity has since
# stalled. This is a follow-up/review list for sales to action — nothing
# here is moved automatically. 30-day threshold is tunable below.
STALE_SQL_LEAD_STATUSES = {"CONNECTED", "Qualified", "IN_PROGRESS", "PROPOSAL_REQUEST"}
STALE_SQL_ACTIVITY_DAYS = 30

# =========================================================================

# Deal-alert configuration.
QUALIFIED_TO_BUY_STAGE = "appointmentscheduled"   # internal value for "Qualified to Buy"
STALE_ACTIVITY_DAYS = 14

# "What moved this week" window.
MOVED_THIS_WEEK_DAYS = 7

# Contact properties we pull.
CONTACT_PROPERTIES = [
    "lifecyclestage", "hs_lead_status", "customer_type",
    "b2b_contact_lead_score_combined", "hs_lastmodifieddate",
    "notes_last_updated", "hubspot_owner_id", "hs_full_name_or_email",
]
# Deal properties we pull. notes_last_updated is HubSpot's "Last Activity Date".
# amount_in_home_currency is HubSpot's USD-converted deal value.
DEAL_PROPERTIES = [
    "dealstage", "amount", "deal_currency_code", "amount_in_home_currency",
    "hs_lastmodifieddate", "notes_last_updated", "dealname",
]

HUBSPOT_BASE = "https://api.hubapi.com"


# --------------------------------------------------------------------------
# Live fetch (HubSpot REST API). Used when direct egress is allowed.
# --------------------------------------------------------------------------

def _search_all(object_type, properties, token, filter_groups=None):
    """Page through the CRM search API and return every matching record."""
    url = f"{HUBSPOT_BASE}/crm/v3/objects/{object_type}/search"
    results = []
    after = None
    while True:
        payload = {"properties": properties, "limit": 200}
        if filter_groups:
            payload["filterGroups"] = filter_groups
        if after:
            payload["after"] = after
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url, data=data, method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            raise SystemExit(
                f"HubSpot API error {e.code} for {object_type}. "
                f"(token not shown)"
            )
        results.extend(body.get("results", []))
        after = body.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    return results


def fetch_from_hubspot(token):
    """Return (b2b_contacts, deals) pulled live from HubSpot."""
    contacts = _search_all(
        "contacts", CONTACT_PROPERTIES, token,
        filter_groups=[{
            "filters": [{
                "propertyName": "customer_type",
                "operator": "EQ",
                "value": CUSTOMER_TYPE_FILTER,
            }]
        }],
    )
    deals = _search_all("deals", DEAL_PROPERTIES, token)
    return contacts, deals


def build_inputs_from_live(contacts, deals):
    """Aggregate live-pulled records into the same shape as an --input file."""
    funnel = {}
    miscategorised = []
    sql_contacts = []
    for c in contacts:
        p = c.get("properties", {})
        stage = p.get("lifecyclestage") or "other"
        funnel[stage] = funnel.get(stage, 0) + 1
        if is_miscategorised(p):
            miscategorised.append({
                "id": str(c.get("id")),
                "name": p.get("hs_full_name_or_email", ""),
                "hs_lead_status": p.get("hs_lead_status"),
                "lifecyclestage": p.get("lifecyclestage"),
            })
        if stage == "salesqualifiedlead" and p.get("hs_lead_status") in STALE_SQL_LEAD_STATUSES:
            sql_contacts.append({
                "id": str(c.get("id")),
                "name": p.get("hs_full_name_or_email", ""),
                "hs_lead_status": p.get("hs_lead_status"),
                "notes_last_updated": p.get("notes_last_updated"),
                "owner": p.get("hubspot_owner_id"),
                "record_type": "contact",
            })
    deal_records = [
        {"id": str(d.get("id")), **d.get("properties", {})} for d in deals
    ]
    return {
        "contacts_b2b_total": len(contacts),
        "funnel_counts": funnel,
        "deals": deal_records,
        "miscategorised": miscategorised,
        "sql_contacts": sql_contacts,
        "sql_companies": [],
    }


# --------------------------------------------------------------------------
# Computation
# --------------------------------------------------------------------------

def _to_float(value):
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _home_usd(deal):
    """Return (usd_value, is_missing). USD from amount_in_home_currency."""
    raw = deal.get("amount_in_home_currency")
    if raw is None or str(raw).strip() == "":
        return 0.0, True
    try:
        return float(raw), False
    except (TypeError, ValueError):
        return 0.0, True


def _orig_amount_str(deal):
    """Original amount + currency as a secondary reference string."""
    amt = deal.get("amount")
    cur = deal.get("deal_currency_code") or ""
    if amt is None or str(amt).strip() == "":
        return "—"
    try:
        return f"{float(amt):,.0f} {cur}".strip()
    except (TypeError, ValueError):
        return f"{amt} {cur}".strip()


def _parse_ts(value):
    """Parse a HubSpot ISO timestamp to a datetime, or None."""
    if not value:
        return None
    v = value.replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(v)
    except ValueError:
        return None


def _days_since(value, run_dt):
    last = _parse_ts(value)
    if last is None:
        return None
    if last.tzinfo is None:
        last = last.replace(tzinfo=dt.timezone.utc)
    return (run_dt - last).days


def compute_metrics(raw, run_date, prev=None):
    funnel_counts = raw["funnel_counts"]
    run_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)

    # Previous deal stages (id -> internal stage) for stage-change detection.
    prev_deal_stage = {}
    if prev:
        for d in prev.get("raw", {}).get("deals", []):
            prev_deal_stage[str(d.get("id"))] = d.get("dealstage")

    # Funnel (ordered, with labels).
    funnel = []
    for stage in LIFECYCLE_ORDER:
        funnel.append({
            "stage": stage,
            "label": LIFECYCLE_LABELS.get(stage, stage),
            "count": int(funnel_counts.get(stage, 0)),
        })
    # Any stage we did not know about.
    for stage, count in funnel_counts.items():
        if stage not in LIFECYCLE_ORDER:
            funnel.append({"stage": stage, "label": stage, "count": int(count)})

    mql = int(funnel_counts.get("marketingqualifiedlead", 0))
    sql = int(funnel_counts.get("salesqualifiedlead", 0))
    ratio = round(sql / mql, 2) if mql else None

    # Deals by stage: count + summed USD (home currency). Flag missing home FX.
    by_stage = {}
    currencies = set()
    missing_home = []
    for d in raw["deals"]:
        stage = d.get("dealstage") or "unknown"
        usd, missing = _home_usd(d)
        entry = by_stage.setdefault(
            stage, {"count": 0, "usd": 0.0, "missing": 0})
        entry["count"] += 1
        if missing:
            entry["missing"] += 1
            missing_home.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "stage": stage,
                "label": DEALSTAGE_LABELS.get(stage, stage),
                "amount_raw": _orig_amount_str(d),
            })
        else:
            entry["usd"] += usd
        if d.get("deal_currency_code"):
            currencies.add(d["deal_currency_code"])
    deals_by_stage = []
    for stage, entry in by_stage.items():
        deals_by_stage.append({
            "stage": stage,
            "label": DEALSTAGE_LABELS.get(stage, stage),
            "count": entry["count"],
            "total_usd": round(entry["usd"], 2),
            "missing_home_count": entry["missing"],
        })
    deals_by_stage.sort(key=lambda x: (-x["total_usd"], -x["count"], x["label"]))

    # Alert: Qualified-to-Buy deals with no activity in 14+ days.
    stale = []
    for d in raw["deals"]:
        if d.get("dealstage") != QUALIFIED_TO_BUY_STAGE:
            continue
        days = _days_since(d.get("notes_last_updated"), run_dt)
        if days is None or days >= STALE_ACTIVITY_DAYS:
            usd, missing = _home_usd(d)
            stale.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "notes_last_updated": d.get("notes_last_updated"),
                "days_since_activity": days,
                "usd": usd,
                "usd_missing": missing,
                "amount_raw": _orig_amount_str(d),
            })
    stale.sort(key=lambda x: (x["days_since_activity"] is not None,
                              x["days_since_activity"] or 0), reverse=True)

    # "What moved this week": deals touched (modified OR new activity) in the
    # last MOVED_THIS_WEEK_DAYS. Group by current stage, highest USD first.
    moved = []
    for d in raw["deals"]:
        mod_days = _days_since(d.get("hs_lastmodifieddate"), run_dt)
        act_days = _days_since(d.get("notes_last_updated"), run_dt)
        touched = ((mod_days is not None and mod_days <= MOVED_THIS_WEEK_DAYS)
                   or (act_days is not None and act_days <= MOVED_THIS_WEEK_DAYS))
        if not touched:
            continue
        stage = d.get("dealstage") or "unknown"
        usd, missing = _home_usd(d)
        did = str(d.get("id"))
        # Detect what changed.
        prev_stage = prev_deal_stage.get(did)
        changed = []
        if prev and did in prev_deal_stage and prev_stage != stage:
            changed.append(
                f"Stage change: {DEALSTAGE_LABELS.get(prev_stage, prev_stage or '—')} "
                f"→ {DEALSTAGE_LABELS.get(stage, stage)}")
        if act_days is not None and act_days <= MOVED_THIS_WEEK_DAYS:
            changed.append("New activity")
        if (mod_days is not None and mod_days <= MOVED_THIS_WEEK_DAYS
                and (act_days is None or act_days > MOVED_THIS_WEEK_DAYS)
                and not changed):
            changed.append("Field update")
        if prev and did not in prev_deal_stage:
            changed.append("New since last snapshot")
        moved.append({
            "id": did,
            "dealname": d.get("dealname", ""),
            "stage": stage,
            "label": DEALSTAGE_LABELS.get(stage, stage),
            "usd": usd,
            "usd_missing": missing,
            "amount_raw": _orig_amount_str(d),
            "last_activity": d.get("notes_last_updated"),
            "last_modified": d.get("hs_lastmodifieddate"),
            "activity_days": act_days,
            "changed": ", ".join(changed) if changed else "Updated",
        })

    # Alert: SQLs sales has engaged but gone quiet (30+ days), grouped by
    # lead status. Distinct from the auto-recycled Attempted-to-Contact case.
    # Contacts AND companies (companies use customer_type__cloned_=B2B).
    engaged = []
    for c in raw.get("sql_contacts", []):
        engaged.append({**c, "record_type": c.get("record_type", "contact")})
    for c in raw.get("sql_companies", []):
        engaged.append({**c, "record_type": c.get("record_type", "company")})

    stale_sqls = []
    for c in engaged:
        days = _days_since(c.get("notes_last_updated"), run_dt)
        if days is None or days >= STALE_SQL_ACTIVITY_DAYS:
            stale_sqls.append({**c, "days_since_activity": days})
    stale_sqls.sort(key=lambda x: (x["days_since_activity"] is not None,
                                    x["days_since_activity"] or 0), reverse=True)

    misc = raw.get("miscategorised", [])
    total_usd = round(sum(s["total_usd"] for s in deals_by_stage), 2)
    return {
        "funnel": funnel,
        "mql_count": mql,
        "sql_count": sql,
        "sql_to_mql_ratio": ratio,
        "funnel_inversion": sql > mql,
        "deals_by_stage": deals_by_stage,
        "deal_total_usd": total_usd,
        "deal_currencies": sorted(currencies),
        "missing_home_deals": missing_home,
        "deals_moved_this_week": moved,
        "qualified_to_buy_stale": stale,
        "stale_engaged_sqls": stale_sqls,
        "stale_engaged_sql_count": len(stale_sqls),
        "miscategorised_count": len(misc),
        "miscategorised": misc,
    }


# --------------------------------------------------------------------------
# Snapshots (raw + metrics) and week-on-week
# --------------------------------------------------------------------------

def load_prev_snapshot(snap_dir, run_date):
    """Most recent snapshot strictly before run_date, or None."""
    if not os.path.isdir(snap_dir):
        return None
    candidates = []
    for name in os.listdir(snap_dir):
        if not name.endswith(".json"):
            continue
        try:
            d = dt.date.fromisoformat(name[:-5])
        except ValueError:
            continue
        if d < run_date:
            candidates.append((d, name))
    if not candidates:
        return None
    candidates.sort()
    with open(os.path.join(snap_dir, candidates[-1][1])) as f:
        return json.load(f)


def delta_str(current, prev):
    """Format 'current (+N)' / 'current (-N)' / 'current (=)'."""
    if prev is None:
        return f"{current}"
    diff = current - prev
    if diff > 0:
        return f"{current} (+{diff})"
    if diff < 0:
        return f"{current} ({diff})"
    return f"{current} (=)"


def money_delta_str(current, prev):
    if prev is None:
        return f"{current:,.0f}"
    diff = current - prev
    if diff > 0:
        return f"{current:,.0f} (+{diff:,.0f})"
    if diff < 0:
        return f"{current:,.0f} ({diff:,.0f})"
    return f"{current:,.0f} (=)"


# --------------------------------------------------------------------------
# Report rendering
# --------------------------------------------------------------------------

def render_report(snapshot, prev):
    m = snapshot["metrics"]
    prev_m = prev["metrics"] if prev else None
    date = snapshot["run_date"]
    out = []
    A = out.append

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {date}")
    A("")
    A(f"*Scope: B2B contacts only (`customer_type = {CUSTOMER_TYPE_FILTER}`); "
      f"B2B companies via `customer_type__cloned_ = {CUSTOMER_TYPE_FILTER}`. "
      f"Deal values in {HOME_CURRENCY} (home currency). "
      f"Generated {snapshot['generated_at_utc']} UTC from HubSpot.*")
    A("")
    if prev:
        A(f"*Week-on-week deltas are vs the previous snapshot "
          f"(`{prev['run_date']}`).*")
    else:
        A("*No prior snapshot found — week-on-week comparison starts next run.*")
    A("")

    # ---- Alerts -----------------------------------------------------------
    A("## 🔔 Alerts")
    A("")

    # Alert headline counts up top.
    stale = m["qualified_to_buy_stale"]
    stale_sqls = m["stale_engaged_sqls"]
    if prev_m:
        prev_stale_sql = prev_m.get(
            "stale_engaged_sql_count",
            len(prev_m.get("stale_engaged_sqls", [])))
    else:
        prev_stale_sql = None
    misc_prev = prev_m["miscategorised_count"] if prev_m else None
    A(f"- **\"Qualified to Buy\" deals idle {STALE_ACTIVITY_DAYS}+ days:** "
      f"{len(stale)}")
    A(f"- **Engaged SQLs gone quiet ({STALE_SQL_ACTIVITY_DAYS}+ days, review):** "
      f"{delta_str(len(stale_sqls), prev_stale_sql)}")
    A(f"- **Miscategorised leads:** "
      f"{delta_str(m['miscategorised_count'], misc_prev)}")
    if m["funnel_inversion"]:
        A(f"- ⚠️ **Funnel inversion:** SQL ({m['sql_count']}) > MQL "
          f"({m['mql_count']})")
    if m["missing_home_deals"]:
        A(f"- ⚠️ **Deals missing {HOME_CURRENCY} value (excluded from totals):** "
          f"{len(m['missing_home_deals'])}")
    A("")

    if stale:
        A(f"### ⚠️ {len(stale)} \"Qualified to Buy\" deal(s) with no activity "
          f"in {STALE_ACTIVITY_DAYS}+ days")
        A("")
        A(f"| Deal | Last activity | Days idle | Value ({HOME_CURRENCY}) | Orig |")
        A("|------|---------------|-----------|-----------|------|")
        for d in stale:
            days = "no activity logged" if d["days_since_activity"] is None \
                else f"{d['days_since_activity']}d"
            last = (d["notes_last_updated"] or "—")[:10]
            usd = "⚠️ no FX" if d["usd_missing"] else f"{d['usd']:,.0f}"
            A(f"| {d['dealname']} (#{d['id']}) | {last} | {days} | {usd} "
              f"| {d['amount_raw']} |")
        A("")
    else:
        A(f"- ✅ No \"Qualified to Buy\" deals idle for {STALE_ACTIVITY_DAYS}+ days.")
        A("")

    misc_prev2 = prev_m["miscategorised_count"] if prev_m else None
    A(f"### 🏷️ Miscategorised leads: "
      f"{delta_str(m['miscategorised_count'], misc_prev2)}")
    A("")
    A("Sales is working these contacts (lead status) but their lifecycle stage "
      "is still only Lead / MQL — they should be Sales Qualified Lead or beyond.")
    A("")
    if m["miscategorised"]:
        A("| Contact ID | Name | Lead status | Lifecycle stage |")
        A("|------------|------|-------------|-----------------|")
        for c in m["miscategorised"]:
            A(f"| {c['id']} | {c.get('name','')} | {c.get('hs_lead_status','')} "
              f"| {c.get('lifecyclestage','')} |")
    else:
        A("- None this run.")
    A("")

    if m["funnel_inversion"]:
        A(f"### ⚠️ Funnel inversion: SQL ({m['sql_count']}) > MQL "
          f"({m['mql_count']})")
        A("")
        A("More Sales Qualified than Marketing Qualified Leads. Expected the "
          "funnel to narrow (MQL ≥ SQL). Likely causes: leads skipping the MQL "
          "stage, MQL criteria too strict, or bulk SQL promotion. Worth a look.")
        A("")

    # ---- Deals moved this week -------------------------------------------
    A(f"## 📈 What moved this week (last {MOVED_THIS_WEEK_DAYS} days)")
    A("")
    A("Deals with a field/stage update **or** new activity in the last "
      f"{MOVED_THIS_WEEK_DAYS} days — the quick \"how did the pipeline shift?\" "
      "view for the Tuesday leadership call. Grouped by current stage, highest "
      f"{HOME_CURRENCY} value first.")
    A("")
    moved = m["deals_moved_this_week"]
    if moved:
        # Group by current stage; order groups by total USD desc.
        groups = {}
        for d in moved:
            groups.setdefault(d["stage"], []).append(d)
        group_order = sorted(
            groups.keys(),
            key=lambda s: -sum(x["usd"] for x in groups[s]))
        A(f"*{len(moved)} deal(s) active this week.*")
        A("")
        for stage in group_order:
            group = sorted(groups[stage], key=lambda x: -x["usd"])
            label = DEALSTAGE_LABELS.get(stage, stage)
            gtotal = sum(x["usd"] for x in group)
            A(f"**{label}** — {len(group)} deal(s), {gtotal:,.0f} {HOME_CURRENCY}")
            A("")
            A(f"| Deal | Value ({HOME_CURRENCY}) | Orig | What changed | Last activity |")
            A("|------|-----------|------|--------------|---------------|")
            for d in group:
                usd = "⚠️ no FX" if d["usd_missing"] else f"{d['usd']:,.0f}"
                last = (d["last_activity"] or d["last_modified"] or "—")[:10]
                A(f"| {d['dealname']} (#{d['id']}) | {usd} | {d['amount_raw']} "
                  f"| {d['changed']} | {last} |")
            A("")
    else:
        A("- No deals moved in the last 7 days.")
        A("")

    # ---- Stalled engaged SQLs (follow-up / review) ------------------------
    A(f"### 🕓 SQLs engaged but gone quiet: "
      f"{delta_str(len(stale_sqls), prev_stale_sql)}")
    A("")
    A(f"**Follow-up / review — NOT auto-recycle.** Sales has already made real "
      f"contact on these (Connected / Qualified / In Progress / Proposal Request) "
      f"but activity has since stalled {STALE_SQL_ACTIVITY_DAYS}+ days. These are "
      "live relationships for sales to action — distinct from the \"Attempted to "
      "Contact\" cases the stale-lead recycler handles automatically. Nothing "
      "here moves without a human. Sorted most-stalled first.")
    A("")
    if stale_sqls:
        by_status = {}
        for c in stale_sqls:
            by_status.setdefault(c.get("hs_lead_status", "—"), []).append(c)
        for status, group in by_status.items():
            group.sort(key=lambda x: (x["days_since_activity"] is not None,
                                      x["days_since_activity"] or 0), reverse=True)
            A(f"**{status}** ({len(group)})")
            A("")
            A("| Record | Type | Days idle | Owner | Associated deal |")
            A("|--------|------|-----------|-------|-----------------|")
            for c in group:
                days = "no activity logged" if c["days_since_activity"] is None \
                    else f"{c['days_since_activity']}d"
                deal = c.get("deal_name")
                if deal:
                    dval = c.get("deal_usd")
                    deal_cell = f"{deal} ({dval:,.0f} {HOME_CURRENCY})" \
                        if dval is not None else deal
                else:
                    deal_cell = "—"
                A(f"| {c.get('name','')} (#{c.get('id','')}) "
                  f"| {c.get('record_type','contact')} | {days} "
                  f"| {c.get('owner') or '—'} | {deal_cell} |")
            A("")
        A("*No directly-associated deals were found for these records this run.*")
        A("")
    else:
        A(f"- ✅ No engaged SQLs idle for {STALE_SQL_ACTIVITY_DAYS}+ days.")
        A("")

    # ---- Funnel -----------------------------------------------------------
    A("## Funnel — B2B contacts per lifecycle stage")
    A("")
    A("| Lifecycle stage | Contacts |")
    A("|-----------------|----------|")
    prev_funnel = {f["stage"]: f["count"] for f in prev_m["funnel"]} if prev_m else {}
    total = 0
    for f in m["funnel"]:
        total += f["count"]
        pv = prev_funnel.get(f["stage"]) if prev_m else None
        A(f"| {f['label']} | {delta_str(f['count'], pv)} |")
    prev_total = sum(prev_funnel.values()) if prev_m else None
    A(f"| **Total** | **{delta_str(total, prev_total)}** |")
    A("")

    # ---- MQL / SQL --------------------------------------------------------
    A("## MQL vs SQL")
    A("")
    pmql = prev_m["mql_count"] if prev_m else None
    psql = prev_m["sql_count"] if prev_m else None
    A(f"- **MQL (Marketing Qualified Lead):** {delta_str(m['mql_count'], pmql)}")
    A(f"- **SQL (Sales Qualified Lead):** {delta_str(m['sql_count'], psql)}")
    ratio = m["sql_to_mql_ratio"]
    A(f"- **SQL : MQL ratio:** {ratio if ratio is not None else 'n/a (MQL = 0)'}")
    A(f"- **Funnel inversion (SQL > MQL):** "
      f"{'⚠️ YES' if m['funnel_inversion'] else 'no'}")
    A("")

    # ---- Deals ------------------------------------------------------------
    A(f"## Deals — count and value by stage ({HOME_CURRENCY})")
    A("")
    A(f"> **Currency:** deals are created in mixed currencies "
      f"({', '.join(m['deal_currencies']) or '—'}). All totals below use "
      f"`amount_in_home_currency` ({HOME_CURRENCY}); raw amounts are never "
      f"summed across currencies. Deals with no {HOME_CURRENCY} value are "
      f"flagged and excluded from the totals.")
    A("")
    A(f"| Deal stage | Deals | Total value ({HOME_CURRENCY}) |")
    A("|------------|-------|--------------------|")
    prev_deals = {d["stage"]: d for d in prev_m["deals_by_stage"]} if prev_m else {}
    tot_count = 0
    tot_usd = 0.0
    for d in m["deals_by_stage"]:
        tot_count += d["count"]
        tot_usd += d["total_usd"]
        pv = prev_deals.get(d["stage"]) if prev_m else None
        pv_count = pv["count"] if pv else None
        pv_usd = pv.get("total_usd") if pv else None
        flag = f" ⚠️{d['missing_home_count']} no-FX" if d.get("missing_home_count") else ""
        A(f"| {d['label']} | {delta_str(d['count'], pv_count)} "
          f"| {money_delta_str(d['total_usd'], pv_usd)}{flag} |")
    prev_tot_usd = prev_m.get("deal_total_usd") if prev_m else None
    A(f"| **Total** | **{tot_count}** | **{money_delta_str(tot_usd, prev_tot_usd)}** |")
    A("")
    A("*Value per stage note: pre-deal lifecycle stages (Email Contact, Lead, "
      "MQL) are contact counts only and carry no deal value — deal value exists "
      "only from the deal pipeline stages above.*")
    A("")

    if m["missing_home_deals"]:
        A(f"**⚠️ {len(m['missing_home_deals'])} deal(s) missing "
          f"`amount_in_home_currency`** (flagged, not counted in {HOME_CURRENCY} "
          "totals):")
        A("")
        A("| Deal | Stage | Orig amount |")
        A("|------|-------|-------------|")
        for d in m["missing_home_deals"]:
            A(f"| {d['dealname']} (#{d['id']}) | {d['label']} | {d['amount_raw']} |")
        A("")

    A("---")
    A(f"*Snapshot: `snapshots/{date}.json` · Report: `reports/{date}.md`*")
    A("")
    return "\n".join(out)


# --------------------------------------------------------------------------
# Word (.docx) conversion via pandoc
# --------------------------------------------------------------------------

def convert_to_docx(md_path, docx_path, template_path):
    """Convert the markdown report to .docx with pandoc. Returns True on success."""
    if not _which("pandoc"):
        print("pandoc not found — skipping .docx generation.", file=sys.stderr)
        return False
    cmd = ["pandoc", md_path, "-o", docx_path]
    if template_path and os.path.isfile(template_path):
        cmd += [f"--reference-doc={template_path}"]
    else:
        if template_path:
            print(f"Reference template {template_path} not found — "
                  "using pandoc defaults.", file=sys.stderr)
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"pandoc conversion failed: {e}", file=sys.stderr)
        return False


def _which(prog):
    for path in os.environ.get("PATH", "").split(os.pathsep):
        candidate = os.path.join(path, prog)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly pipeline report")
    ap.add_argument("--input", help="Pre-pulled raw JSON (skips live fetch)")
    ap.add_argument("--date", help="Run date YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
    ap.add_argument("--no-docx", action="store_true", help="Skip .docx generation")
    args = ap.parse_args()

    run_date = (dt.date.fromisoformat(args.date) if args.date
                else dt.datetime.now(dt.timezone.utc).date())

    if args.input:
        with open(args.input) as f:
            raw = json.load(f)
        source = raw.get("source", "hubspot (pre-pulled)")
    else:
        token = os.environ.get("HUBSPOT_TOKEN")
        if not token:
            sys.exit("HUBSPOT_TOKEN not set in environment.")
        contacts, deals = fetch_from_hubspot(token)
        raw = build_inputs_from_live(contacts, deals)
        source = "hubspot (live REST)"

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    tpl_dir = os.path.join(args.outdir, "templates")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev_snapshot(snap_dir, run_date)
    metrics = compute_metrics(raw, run_date, prev)

    generated = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    snapshot = {
        "run_date": run_date.isoformat(),
        "generated_at_utc": generated,
        "source": source,
        "customer_type_filter": CUSTOMER_TYPE_FILTER,
        "home_currency": HOME_CURRENCY,
        "raw": {
            "contacts_b2b_total": raw.get("contacts_b2b_total"),
            "funnel_counts_internal": raw["funnel_counts"],
            "deals": raw["deals"],
            "miscategorised_contacts": raw.get("miscategorised", []),
            "sql_contacts": raw.get("sql_contacts", []),
            "sql_companies": raw.get("sql_companies", []),
        },
        "metrics": metrics,
    }

    snap_path = os.path.join(snap_dir, f"{run_date.isoformat()}.json")
    with open(snap_path, "w") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    report = render_report(snapshot, prev)
    rep_path = os.path.join(rep_dir, f"{run_date.isoformat()}.md")
    with open(rep_path, "w") as f:
        f.write(report)

    print(f"Wrote {snap_path}")
    print(f"Wrote {rep_path}")

    if not args.no_docx:
        docx_path = os.path.join(rep_dir, f"{run_date.isoformat()}.docx")
        tpl = os.path.join(tpl_dir, "pipeline_report_template.docx")
        if convert_to_docx(rep_path, docx_path, tpl):
            print(f"Wrote {docx_path}")

    if prev:
        print(f"Compared against previous snapshot: {prev['run_date']}")
    else:
        print("No previous snapshot found (first run).")


if __name__ == "__main__":
    main()
