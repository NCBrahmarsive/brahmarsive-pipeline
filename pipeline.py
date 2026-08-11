#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report.

What it does
------------
Pulls contacts, companies and deals from HubSpot, filters the funnel to B2B,
and writes three files per run:

  snapshots/YYYY-MM-DD.json  -- the pulled data + computed metrics (machine)
  reports/YYYY-MM-DD.md      -- readable summary (funnel, ratios, deal values,
                                alerts, "what moved this week", follow-up list)
  reports/YYYY-MM-DD.docx    -- Word rendering of the .md (via pandoc, done by
                                the caller/runner after this script writes .md)

Each run loads the most recent previous snapshot from snapshots/ and shows the
week-on-week movement for every metric.

Two data sources
----------------
1. Live (default): fetch straight from HubSpot's REST API using the
   HUBSPOT_TOKEN environment variable. Requires direct egress to
   api.hubapi.com.

       python3 pipeline.py

2. Pre-pulled (--input FILE): consume data already pulled from HubSpot and
   stored as JSON. This is the path used in the managed/CI environment where
   egress to api.hubapi.com is blocked: the data is fetched via the HubSpot
   MCP tools and handed to the same computation code here.

       python3 pipeline.py --input raw_input.json

The token value is never printed or committed.

Currency
--------
The HubSpot account home currency is USD. Deals may be created in other
currencies (notably INR). All deal *values* are reported and summed using
`amount_in_home_currency` (HubSpot's USD-converted value). The original
`amount` + `deal_currency_code` are shown only as a secondary reference.
Raw `amount` is NEVER summed across mixed currencies. A deal whose
`amount_in_home_currency` is empty is flagged, not silently treated as zero.
"""

import argparse
import datetime as dt
import json
import os
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
# Pipeline order used to group the "deals moved this week" view.
DEALSTAGE_ORDER = [
    "1217017229", "appointmentscheduled", "presentationscheduled",
    "1217017230", "decisionmakerboughtin", "contractsent",
    "closedwon", "closedlost",
]

# The B2B filter is the single exclusion switch: keeping only customer_type=B2B
# (contacts) / customer_type__cloned_=B2B (companies) automatically drops B2C,
# Partner, Vendor, Internal and Partner-B2B records.
CUSTOMER_TYPE_FILTER = "B2B"

# Deal value is always reported/summed in the account home currency (USD).
HOME_CURRENCY = "USD"
HOME_CURRENCY_AMOUNT_PROP = "amount_in_home_currency"

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
# the separate HubSpot stale-lead recycler workflow). These are SQL-stage
# records (contacts AND companies) where sales has already made real
# contact/progress (CONNECTED / Qualified / In progress / Proposal request),
# but activity has since stalled past the threshold below. This is a
# follow-up/review list for sales to action — nothing here is moved
# automatically. 30-day threshold is tunable.
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
DEAL_PROPERTIES = [
    "dealstage", "amount", "deal_currency_code", HOME_CURRENCY_AMOUNT_PROP,
    "hs_lastmodifieddate", "notes_last_updated", "notes_last_contacted",
    "dealname",
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
    """Aggregate live-pulled records into the same shape as an --input file.

    Note: in live mode owner IDs are left as raw IDs (name resolution needs a
    separate owners lookup) and company-level stale SQLs are not gathered.
    The managed/MCP path (--input) resolves owner names and includes companies.
    """
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
    """(usd_value, is_missing) for a deal, from amount_in_home_currency.

    is_missing is True when the home-currency value is absent/blank — such a
    deal is FLAGGED and contributes 0 to totals (never falls back to raw
    amount, which could be in a foreign currency)."""
    raw = deal.get(HOME_CURRENCY_AMOUNT_PROP)
    if raw is None or raw == "":
        return 0.0, True
    try:
        return float(raw), False
    except (TypeError, ValueError):
        return 0.0, True


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
    """Whole days between an ISO timestamp and run_dt; None if unparseable."""
    last = _parse_ts(value)
    if last is None:
        return None
    if last.tzinfo is None:
        last = last.replace(tzinfo=dt.timezone.utc)
    return (run_dt - last).days


def _last_activity(deal):
    """Best available 'last activity' timestamp for a deal."""
    return deal.get("notes_last_updated") or deal.get("notes_last_contacted")


def compute_metrics(raw, run_date, prev=None):
    run_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)
    funnel_counts = raw["funnel_counts"]

    # Funnel (ordered, with labels).
    funnel = []
    for stage in LIFECYCLE_ORDER:
        funnel.append({
            "stage": stage,
            "label": LIFECYCLE_LABELS.get(stage, stage),
            "count": int(funnel_counts.get(stage, 0)),
        })
    for stage, count in funnel_counts.items():
        if stage not in LIFECYCLE_ORDER:
            funnel.append({"stage": stage, "label": stage, "count": int(count)})

    mql = int(funnel_counts.get("marketingqualifiedlead", 0))
    sql = int(funnel_counts.get("salesqualifiedlead", 0))
    ratio = round(sql / mql, 2) if mql else None

    # ---- Deals by stage: count + summed USD (home currency) --------------
    by_stage = {}
    currencies = set()
    missing_home_currency = []
    for d in raw["deals"]:
        stage = d.get("dealstage") or "unknown"
        usd, missing = _home_usd(d)
        entry = by_stage.setdefault(stage, {"count": 0, "usd": 0.0, "missing": 0})
        entry["count"] += 1
        entry["usd"] += usd
        if missing:
            entry["missing"] += 1
            missing_home_currency.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "dealstage": stage,
                "label": DEALSTAGE_LABELS.get(stage, stage),
                "amount": d.get("amount"),
                "currency": d.get("deal_currency_code"),
            })
        if d.get("deal_currency_code"):
            currencies.add(d["deal_currency_code"])
    deals_by_stage = []
    for stage, entry in by_stage.items():
        deals_by_stage.append({
            "stage": stage,
            "label": DEALSTAGE_LABELS.get(stage, stage),
            "count": entry["count"],
            "total_usd": round(entry["usd"], 2),
            "missing_home_currency": entry["missing"],
        })
    deals_by_stage.sort(key=lambda x: (-x["total_usd"], -x["count"], x["label"]))

    # ---- Alert: Qualified-to-Buy deals idle 14+ days ---------------------
    stale = []
    for d in raw["deals"]:
        if d.get("dealstage") != QUALIFIED_TO_BUY_STAGE:
            continue
        days = _days_since(_last_activity(d), run_dt)
        if days is None or days >= STALE_ACTIVITY_DAYS:
            usd, missing = _home_usd(d)
            stale.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "last_activity": _last_activity(d),
                "days_since_activity": days,
                "usd": usd,
                "usd_missing": missing,
                "amount": d.get("amount"),
                "currency": d.get("deal_currency_code"),
            })
    stale.sort(key=lambda x: (x["days_since_activity"] is not None,
                              x["days_since_activity"] or 0), reverse=True)

    # ---- "What moved this week": deals touched in the last 7 days ---------
    prev_deal_stage = {}
    if prev:
        for d in prev.get("raw", {}).get("deals", []):
            prev_deal_stage[str(d.get("id"))] = d.get("dealstage")
    moved = []
    for d in raw["deals"]:
        d_mod = _days_since(d.get("hs_lastmodifieddate"), run_dt)
        d_act = _days_since(_last_activity(d), run_dt)
        touched = ((d_mod is not None and d_mod < MOVED_THIS_WEEK_DAYS) or
                   (d_act is not None and d_act < MOVED_THIS_WEEK_DAYS))
        if not touched:
            continue
        stage = d.get("dealstage") or "unknown"
        usd, missing = _home_usd(d)
        # What changed, best-effort.
        prev_stage = prev_deal_stage.get(str(d.get("id")))
        if prev and prev_stage and prev_stage != stage:
            change = (f"Stage change: "
                      f"{DEALSTAGE_LABELS.get(prev_stage, prev_stage)} → "
                      f"{DEALSTAGE_LABELS.get(stage, stage)}")
        elif prev and str(d.get("id")) not in prev_deal_stage:
            change = "New deal since last snapshot"
        elif d_act is not None and d_act < MOVED_THIS_WEEK_DAYS:
            change = "Activity logged"
        else:
            change = "Field/property update"
        moved.append({
            "id": d.get("id"),
            "dealname": d.get("dealname", ""),
            "stage": stage,
            "label": DEALSTAGE_LABELS.get(stage, stage),
            "usd": usd,
            "usd_missing": missing,
            "amount": d.get("amount"),
            "currency": d.get("deal_currency_code"),
            "last_activity": _last_activity(d),
            "last_modified": d.get("hs_lastmodifieddate"),
            "change": change,
        })

    # ---- Follow-up: SQLs (contacts + companies) engaged but gone quiet ----
    stale_sqls = []
    combined = ([{**c, "record_type": "Contact"} for c in raw.get("sql_contacts", [])] +
                [{**c, "record_type": "Company"} for c in raw.get("sql_companies", [])])
    for c in combined:
        days = _days_since(c.get("notes_last_updated"), run_dt)
        if days is None or days >= STALE_SQL_ACTIVITY_DAYS:
            deal = c.get("associated_deal") or {}
            stale_sqls.append({
                "id": c.get("id"),
                "name": c.get("name", ""),
                "record_type": c.get("record_type", "Contact"),
                "hs_lead_status": c.get("hs_lead_status"),
                "owner": c.get("owner"),
                "notes_last_updated": c.get("notes_last_updated"),
                "days_since_activity": days,
                "deal_name": deal.get("name"),
                "deal_usd": deal.get("usd"),
            })
    stale_sqls.sort(key=lambda x: (x["days_since_activity"] is not None,
                                   x["days_since_activity"] or 0), reverse=True)

    misc = raw.get("miscategorised", [])
    return {
        "funnel": funnel,
        "mql_count": mql,
        "sql_count": sql,
        "sql_to_mql_ratio": ratio,
        "funnel_inversion": sql > mql,
        "deals_by_stage": deals_by_stage,
        "deal_value_basis": f"{HOME_CURRENCY_AMOUNT_PROP} ({HOME_CURRENCY})",
        "deal_currencies": sorted(currencies),
        "deals_missing_home_currency": missing_home_currency,
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

def _usd(v, missing=False):
    if missing:
        return "⚠️ no USD value"
    return f"${v:,.0f}"


def render_report(snapshot, prev):
    m = snapshot["metrics"]
    prev_m = prev["metrics"] if prev else None
    date = snapshot["run_date"]
    # Deal-value week-on-week is only comparable when the prior snapshot used
    # the same USD (home-currency) basis. Older snapshots summed raw mixed
    # currencies, so we suppress value deltas (but keep count deltas) then.
    value_wow_ok = bool(prev_m) and prev_m.get("deal_value_basis") == m.get("deal_value_basis")
    out = []
    A = out.append

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {date}")
    A("")
    A(f"*Scope: B2B only (`customer_type = {CUSTOMER_TYPE_FILTER}` for contacts, "
      f"`customer_type__cloned_ = {CUSTOMER_TYPE_FILTER}` for companies). "
      f"Generated {snapshot['generated_at_utc']} UTC from HubSpot. "
      f"Deal values in {HOME_CURRENCY} (home currency).*")
    A("")
    if prev:
        A(f"*Week-on-week deltas are vs the previous snapshot "
          f"(`{prev['run_date']}`).*")
    else:
        A("*No prior snapshot found — week-on-week comparison starts next run.*")
    A("")

    # ---- Alerts (headline counts) ----------------------------------------
    A("## 🔔 Alerts")
    A("")
    misc_prev = prev_m["miscategorised_count"] if prev_m else None
    stale_prev = prev_m.get("stale_engaged_sql_count") if prev_m else None
    A(f"- **Qualified-to-Buy deals idle {STALE_ACTIVITY_DAYS}+ days:** "
      f"{len(m['qualified_to_buy_stale'])}")
    A(f"- **Miscategorised leads:** {delta_str(m['miscategorised_count'], misc_prev)}")
    A(f"- **Engaged SQLs gone quiet ({STALE_SQL_ACTIVITY_DAYS}+ days, follow-up):** "
      f"{delta_str(m['stale_engaged_sql_count'], stale_prev)}")
    if m["funnel_inversion"]:
        A(f"- **⚠️ Funnel inversion:** SQL ({m['sql_count']}) > MQL ({m['mql_count']})")
    if m["deals_missing_home_currency"]:
        A(f"- **⚠️ Deals missing USD value:** "
          f"{len(m['deals_missing_home_currency'])} (excluded from value sums)")
    A("")

    # Qualified-to-Buy detail.
    stale = m["qualified_to_buy_stale"]
    if stale:
        A(f"### ⚠️ {len(stale)} \"Qualified to Buy\" deal(s) with no activity "
          f"in {STALE_ACTIVITY_DAYS}+ days")
        A("")
        A("| Deal | Last activity | Days idle | USD value | Original |")
        A("|------|---------------|-----------|-----------|----------|")
        for d in stale:
            days = "no activity logged" if d["days_since_activity"] is None \
                else f"{d['days_since_activity']}d"
            last = (d["last_activity"] or "—")[:10]
            orig = f"{_to_float(d['amount']):,.0f} {d.get('currency') or ''}".strip()
            A(f"| {d['dealname']} (#{d['id']}) | {last} | {days} "
              f"| {_usd(d['usd'], d['usd_missing'])} | {orig} |")
        A("")
    else:
        A(f"- ✅ No \"Qualified to Buy\" deals idle for {STALE_ACTIVITY_DAYS}+ days.")
        A("")

    # Miscategorised detail.
    A(f"### 🏷️ Miscategorised leads: "
      f"{delta_str(m['miscategorised_count'], misc_prev)}")
    A("")
    A("Sales is working these contacts (lead status) but their lifecycle stage "
      "is still only Lead / MQL — they should be Sales Qualified Lead or beyond.")
    A("")
    if m["miscategorised"]:
        A("| Contact ID | Name | Lead status | Lifecycle stage |")
        A("|------------|------|-------------|-----------------|")
        for c in m["miscategorised"]:
            stage_lbl = LIFECYCLE_LABELS.get(c.get("lifecyclestage", ""),
                                             c.get("lifecyclestage", ""))
            A(f"| {c['id']} | {c.get('name','')} | {c.get('hs_lead_status','')} "
              f"| {stage_lbl} |")
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

    # ---- What moved this week --------------------------------------------
    A(f"## 📈 What moved this week (last {MOVED_THIS_WEEK_DAYS} days)")
    A("")
    moved = m["deals_moved_this_week"]
    if moved:
        A(f"{len(moved)} deal(s) had a stage change, activity, or field update "
          f"in the last {MOVED_THIS_WEEK_DAYS} days. Grouped by current stage, "
          f"highest {HOME_CURRENCY} value first.")
        A("")
        # Group by current stage, ordered by pipeline; highest value first.
        groups = {}
        for d in moved:
            groups.setdefault(d["stage"], []).append(d)
        ordered_stages = ([s for s in DEALSTAGE_ORDER if s in groups] +
                          [s for s in groups if s not in DEALSTAGE_ORDER])
        for stage in ordered_stages:
            rows = sorted(groups[stage], key=lambda x: x["usd"], reverse=True)
            A(f"**{DEALSTAGE_LABELS.get(stage, stage)}** ({len(rows)})")
            A("")
            A("| Deal | USD value | What changed | Last activity |")
            A("|------|-----------|--------------|---------------|")
            for d in rows:
                last = (d["last_activity"] or "—")[:10]
                A(f"| {d['dealname']} (#{d['id']}) "
                  f"| {_usd(d['usd'], d['usd_missing'])} | {d['change']} | {last} |")
            A("")
    else:
        A(f"- No deals moved in the last {MOVED_THIS_WEEK_DAYS} days.")
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
    A(f"## Deals — count and value ({HOME_CURRENCY}) by stage")
    A("")
    if len(m["deal_currencies"]) > 1:
        A(f"> **Currency note:** deals exist in mixed currencies "
          f"({', '.join(m['deal_currencies'])}). All values below use "
          f"`{HOME_CURRENCY_AMOUNT_PROP}` (HubSpot's {HOME_CURRENCY} conversion) "
          f"so totals are meaningful; raw `amount` is never summed across "
          f"currencies.")
        A("")
    if not value_wow_ok and prev_m:
        A("> *Deal-value week-on-week deltas are suppressed this run: the prior "
          "snapshot summed raw mixed currencies, whereas values are now "
          f"{HOME_CURRENCY}-normalised. Value deltas resume next run. Deal-count "
          "deltas are shown.*")
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
        flag = ""
        if d["missing_home_currency"]:
            flag = f" ⚠️{d['missing_home_currency']} no-USD"
        if value_wow_ok:
            pv_usd = pv["total_usd"] if pv else None
            val = money_delta_str(d["total_usd"], pv_usd)
        else:
            val = f"{d['total_usd']:,.0f}"
        A(f"| {d['label']} | {delta_str(d['count'], pv_count)} "
          f"| {val}{flag} |")
    prev_tot_count = sum(x["count"] for x in prev_m["deals_by_stage"]) if prev_m else None
    A(f"| **Total** | **{delta_str(tot_count, prev_tot_count)}** "
      f"| **{tot_usd:,.0f}** |")
    A("")
    A("*Value per stage note: pre-deal lifecycle stages (Email Contact, Lead, "
      "MQL) are contact counts only and carry no deal value — deal value exists "
      "only from the deal pipeline stages above.*")
    A("")
    if m["deals_missing_home_currency"]:
        A(f"**⚠️ {len(m['deals_missing_home_currency'])} deal(s) have no "
          f"`{HOME_CURRENCY_AMOUNT_PROP}` and are excluded from value sums:**")
        A("")
        A("| Deal | Stage | Original amount |")
        A("|------|-------|-----------------|")
        for d in m["deals_missing_home_currency"]:
            orig = f"{d.get('amount') or '—'} {d.get('currency') or ''}".strip()
            A(f"| {d['dealname']} (#{d['id']}) | {d['label']} | {orig} |")
        A("")

    # ---- Follow-up: engaged SQLs gone quiet ------------------------------
    stale_sqls = m["stale_engaged_sqls"]
    A(f"## 🕓 Follow-up / review — SQLs engaged but gone quiet: "
      f"{delta_str(m['stale_engaged_sql_count'], stale_prev)}")
    A("")
    A(f"B2B **contacts and companies** at Sales Qualified Lead where sales has "
      f"already made real contact (status: Contacted / Qualified / In Progress / "
      f"Proposal Request) but there's been **no activity in "
      f"{STALE_SQL_ACTIVITY_DAYS}+ days**. This is a follow-up/review list for "
      f"sales to action — **NOT auto-recycle**. Distinct from the \"Attempted "
      f"to Contact\" cases the stale-lead recycler handles automatically. "
      f"Nothing here moves without a human. Sorted most-stalled first.")
    A("")
    if stale_sqls:
        by_status = {}
        for c in stale_sqls:
            by_status.setdefault(c["hs_lead_status"], []).append(c)
        # Show a status label using the same map contacts use.
        status_labels = {
            "CONNECTED": "Contacted", "Qualified": "Qualified",
            "IN_PROGRESS": "In Progress", "PROPOSAL_REQUEST": "Proposal Request",
        }
        for status, group in sorted(by_status.items(),
                                    key=lambda kv: -max(g["days_since_activity"] or 0
                                                        for g in kv[1])):
            lbl = status_labels.get(status, status)
            A(f"**{lbl}** ({len(group)})")
            A("")
            A("| Record | Type | Days idle | Owner | Associated deal (USD) |")
            A("|--------|------|-----------|-------|-----------------------|")
            for c in group:
                days = "no activity logged" if c["days_since_activity"] is None \
                    else f"{c['days_since_activity']}d"
                if c.get("deal_name"):
                    deal = f"{c['deal_name']} ({_usd(_to_float(c.get('deal_usd')))})"
                else:
                    deal = "—"
                A(f"| {c['name']} (#{c['id']}) | {c['record_type']} | {days} "
                  f"| {c.get('owner') or '—'} | {deal} |")
            A("")
    else:
        A(f"- ✅ No engaged SQLs idle for {STALE_SQL_ACTIVITY_DAYS}+ days.")
        A("")

    # ---- Week-on-week summary --------------------------------------------
    A("## 📊 Week-on-week movement")
    A("")
    if not prev_m:
        A("No prior snapshot — week-on-week comparison starts next run.")
        A("")
    else:
        A(f"Versus `{prev['run_date']}`:")
        A("")
        A("| Metric | This run | Change |")
        A("|--------|----------|--------|")

        def _row(label, cur, pv):
            if pv is None:
                A(f"| {label} | {cur} | n/a (new) |")
            else:
                diff = cur - pv
                sign = f"+{diff}" if diff > 0 else (str(diff) if diff < 0 else "=")
                A(f"| {label} | {cur} | {sign} |")

        _row("MQL", m["mql_count"], prev_m.get("mql_count"))
        _row("SQL", m["sql_count"], prev_m.get("sql_count"))
        _row("Miscategorised leads", m["miscategorised_count"],
             prev_m.get("miscategorised_count"))
        _row("Engaged SQLs gone quiet", m["stale_engaged_sql_count"],
             prev_m.get("stale_engaged_sql_count"))
        _row("Total B2B contacts", sum(f["count"] for f in m["funnel"]),
             sum(f["count"] for f in prev_m["funnel"]))
        _row("Total deals", sum(d["count"] for d in m["deals_by_stage"]),
             sum(d["count"] for d in prev_m["deals_by_stage"]))
        A("")
        if not value_wow_ok:
            A("*Deal-value deltas per stage resume next run "
              f"(prior snapshot pre-dates {HOME_CURRENCY} normalisation).*")
            A("")

    A("---")
    A(f"*Snapshot: `snapshots/{date}.json` · Report: `reports/{date}.md` · "
      f"Word: `reports/{date}.docx`*")
    A("")
    return "\n".join(out)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly pipeline report")
    ap.add_argument("--input", help="Pre-pulled raw JSON (skips live fetch)")
    ap.add_argument("--date", help="Run date YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
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
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev_snapshot(snap_dir, run_date)
    metrics = compute_metrics(raw, run_date, prev=prev)

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
        json.dump(snapshot, f, indent=2)

    report = render_report(snapshot, prev)
    rep_path = os.path.join(rep_dir, f"{run_date.isoformat()}.md")
    with open(rep_path, "w") as f:
        f.write(report)

    print(f"Wrote {snap_path}")
    print(f"Wrote {rep_path}")
    if prev:
        print(f"Compared against previous snapshot: {prev['run_date']}")
    else:
        print("No previous snapshot found (first run).")


if __name__ == "__main__":
    main()
