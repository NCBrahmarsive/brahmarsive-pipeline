#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report.

What it does
------------
Pulls contacts, companies and deals from HubSpot, filters the funnel to B2B
contacts, and writes two files per run:

  snapshots/YYYY-MM-DD.json  -- the pulled data + computed metrics (machine)
  reports/YYYY-MM-DD.md      -- readable summary with funnel, ratios, deal
                                values (USD), alerts, "deals moved this week",
                                the miscategorised list and the engaged-but-
                                quiet SQL follow-up list

Each run loads the most recent previous snapshot from snapshots/ and shows the
week-on-week movement for every metric.

Two data sources
----------------
1. Live (default): fetch straight from HubSpot's REST API using the
   HUBSPOT_TOKEN environment variable (a HubSpot Service key with
   crm.objects.contacts.read + crm.objects.deals.read + companies.read).

       python3 pipeline.py

2. Pre-pulled (--input FILE): consume data already pulled from HubSpot and
   stored as JSON. This is used when direct egress to api.hubapi.com is not
   permitted from the run environment (e.g. an org egress policy blocks it),
   so the data is fetched via the HubSpot MCP tools and handed to the same
   computation code here.

       python3 pipeline.py --input raw_input.json

The token value is never printed or committed.

Currency
--------
The HubSpot account default (home) currency is USD, but deals may be created
in other currencies (notably INR). Every deal carries both `amount` (original)
and `amount_in_home_currency` (HubSpot's USD-converted value) plus
`deal_currency_code`. All deal *sums* use `amount_in_home_currency` so totals
are meaningful; the original amount + currency is shown only as a secondary
reference. Raw `amount` is NEVER summed across mixed currencies. If a deal's
`amount_in_home_currency` is empty it is FLAGGED, not silently coerced.

NOTE on the property name: the routine spec referred to this as
`hs_deal_amount_in_home_currency`, but the actual HubSpot internal name in this
account is `amount_in_home_currency` (label "Amount in company currency").
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

# HubSpot internal name for the USD (home currency) deal amount. See module
# docstring: the spec called this hs_deal_amount_in_home_currency, but the
# real property in this account is amount_in_home_currency.
HOME_CURRENCY_PROP = "amount_in_home_currency"

# The B2B filter is the single exclusion switch: keeping only customer_type=B2B
# automatically drops B2C, Partner, Vendor, Internal and Partner-B2B contacts.
# (Companies use the cloned property customer_type__cloned_ = B2B.)
CUSTOMER_TYPE_FILTER = "B2B"

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
#
# Applies to B2B contacts AND B2B companies (companies expose only the
# CONNECTED / Qualified statuses; IN_PROGRESS / PROPOSAL_REQUEST are
# contact-only lead statuses).
STALE_SQL_LEAD_STATUSES = {"CONNECTED", "Qualified", "IN_PROGRESS", "PROPOSAL_REQUEST"}
STALE_SQL_ACTIVITY_DAYS = 30

# =========================================================================

# Deal-alert configuration.
QUALIFIED_TO_BUY_STAGE = "appointmentscheduled"   # internal value for "Qualified to Buy"
STALE_ACTIVITY_DAYS = 14

# "Deals moved this week" window.
MOVED_WINDOW_DAYS = 7

# Contact properties we pull.
CONTACT_PROPERTIES = [
    "lifecyclestage", "hs_lead_status", "customer_type",
    "b2b_contact_lead_score_combined", "hs_lastmodifieddate",
    "notes_last_updated", "hubspot_owner_id", "hs_full_name_or_email",
]
# Deal properties we pull. notes_last_updated is HubSpot's "Last Activity Date".
DEAL_PROPERTIES = [
    "dealstage", "amount", HOME_CURRENCY_PROP, "deal_currency_code",
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
    """Return (b2b_contacts, deals, b2b_sql_companies) pulled live from HubSpot."""
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
    companies = _search_all(
        "companies",
        ["name", "hs_lead_status", "lifecyclestage",
         "notes_last_updated", "hubspot_owner_id"],
        token,
        filter_groups=[{
            "filters": [
                {"propertyName": "customer_type__cloned_", "operator": "EQ",
                 "value": CUSTOMER_TYPE_FILTER},
                {"propertyName": "lifecyclestage", "operator": "EQ",
                 "value": "salesqualifiedlead"},
                {"propertyName": "hs_lead_status", "operator": "IN",
                 "values": ["CONNECTED", "Qualified"]},
            ]
        }],
    )
    return contacts, deals, companies


def build_inputs_from_live(contacts, deals, companies):
    """Aggregate live-pulled records into the same shape as an --input file.

    NOTE: live mode does not resolve owner IDs to names or attach associated
    deals to SQL records — the --input path (pulled via the HubSpot MCP tools)
    does that enrichment. Live mode is a fallback for when direct egress to
    api.hubapi.com is available.
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
                "record_type": "contact",
                "deal": None,
            })
    sql_companies = []
    for co in companies:
        p = co.get("properties", {})
        sql_companies.append({
            "id": str(co.get("id")),
            "name": p.get("name", ""),
            "hs_lead_status": p.get("hs_lead_status"),
            "notes_last_updated": p.get("notes_last_updated"),
            "owner": p.get("hubspot_owner_id"),
            "record_type": "company",
            "deal": None,
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
        "sql_companies": sql_companies,
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


def _has_value(value):
    return value is not None and value != ""


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
    """Whole days between a HubSpot timestamp and run_dt, or None if unparseable."""
    last = _parse_ts(value)
    if last is None:
        return None
    if last.tzinfo is None:
        last = last.replace(tzinfo=dt.timezone.utc)
    return (run_dt - last).days


def compute_metrics(raw, run_date, prev):
    funnel_counts = raw["funnel_counts"]
    run_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)

    # ---- Funnel (ordered, with labels) -----------------------------------
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

    # ---- Deals by stage: count + summed USD (amount_in_home_currency) -----
    by_stage = {}
    currencies = set()
    missing_home_currency = []
    for d in raw["deals"]:
        stage = d.get("dealstage") or "unknown"
        entry = by_stage.setdefault(
            stage, {"count": 0, "total_usd": 0.0, "missing_usd": 0})
        entry["count"] += 1
        if _has_value(d.get(HOME_CURRENCY_PROP)):
            entry["total_usd"] += _to_float(d.get(HOME_CURRENCY_PROP))
        else:
            entry["missing_usd"] += 1
            missing_home_currency.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "stage": stage,
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
            "total_usd": round(entry["total_usd"], 2),
            "missing_usd": entry["missing_usd"],
        })
    deals_by_stage.sort(key=lambda x: (-x["total_usd"], -x["count"], x["label"]))

    # ---- Deals moved this week -------------------------------------------
    prev_deal_stage = {}
    if prev:
        for d in prev.get("raw", {}).get("deals", []):
            prev_deal_stage[str(d.get("id"))] = d.get("dealstage")
    prev_deal_ids = set(prev_deal_stage) if prev else set()

    moved = []
    for d in raw["deals"]:
        did = str(d.get("id"))
        mod_days = _days_since(d.get("hs_lastmodifieddate"), run_dt)
        act_days = _days_since(d.get("notes_last_updated"), run_dt)
        candidates = [x for x in (mod_days, act_days) if x is not None]
        if not candidates or min(candidates) > MOVED_WINDOW_DAYS:
            continue
        stage = d.get("dealstage") or "unknown"
        # What changed?
        if prev and did not in prev_deal_ids:
            change = "New deal"
        elif prev and did in prev_deal_stage and prev_deal_stage[did] != stage:
            change = (f"Stage change: "
                      f"{DEALSTAGE_LABELS.get(prev_deal_stage[did], prev_deal_stage[did])}"
                      f" → {DEALSTAGE_LABELS.get(stage, stage)}")
        elif act_days is not None and act_days <= MOVED_WINDOW_DAYS:
            change = "Activity logged"
        else:
            change = "Field update"
        moved.append({
            "id": did,
            "dealname": d.get("dealname", ""),
            "stage": stage,
            "label": DEALSTAGE_LABELS.get(stage, stage),
            "usd": _to_float(d.get(HOME_CURRENCY_PROP)) if _has_value(d.get(HOME_CURRENCY_PROP)) else None,
            "amount": d.get("amount"),
            "currency": d.get("deal_currency_code"),
            "last_activity": d.get("notes_last_updated"),
            "last_modified": d.get("hs_lastmodifieddate"),
            "days_since_activity": act_days,
            "change": change,
        })

    # ---- Alert: Qualified-to-Buy deals with no activity in 14+ days -------
    stale = []
    for d in raw["deals"]:
        if d.get("dealstage") != QUALIFIED_TO_BUY_STAGE:
            continue
        days = _days_since(d.get("notes_last_updated"), run_dt)
        if days is None or days >= STALE_ACTIVITY_DAYS:
            stale.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "notes_last_updated": d.get("notes_last_updated"),
                "days_since_activity": days,
                "usd": _to_float(d.get(HOME_CURRENCY_PROP)) if _has_value(d.get(HOME_CURRENCY_PROP)) else None,
                "amount": d.get("amount"),
                "currency": d.get("deal_currency_code"),
            })
    stale.sort(key=lambda x: (x["days_since_activity"] is not None,
                              x["days_since_activity"] or 0), reverse=True)

    # ---- Follow-up: SQLs (contacts + companies) engaged but gone quiet ----
    engaged = list(raw.get("sql_contacts", [])) + list(raw.get("sql_companies", []))
    stale_sqls = []
    for c in engaged:
        days = _days_since(c.get("notes_last_updated"), run_dt)
        if days is None or days >= STALE_SQL_ACTIVITY_DAYS:
            stale_sqls.append({
                "id": c.get("id"),
                "name": c.get("name", ""),
                "hs_lead_status": c.get("hs_lead_status"),
                "notes_last_updated": c.get("notes_last_updated"),
                "owner": c.get("owner"),
                "record_type": c.get("record_type", "contact"),
                "deal": c.get("deal"),
                "days_since_activity": days,
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
        return f"${current:,.0f}"
    diff = current - prev
    if diff > 0:
        return f"${current:,.0f} (+${diff:,.0f})"
    if diff < 0:
        return f"${current:,.0f} (-${abs(diff):,.0f})"
    return f"${current:,.0f} (=)"


def _usd(value):
    return "—" if value is None else f"${value:,.0f}"


def _ref_amount(amount, currency):
    """Secondary reference: original amount + currency (never summed)."""
    if not _has_value(amount):
        return "—"
    try:
        return f"{float(amount):,.0f} {currency or ''}".strip()
    except (TypeError, ValueError):
        return f"{amount} {currency or ''}".strip()


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
    A(f"*Scope: B2B contacts only (`customer_type = {CUSTOMER_TYPE_FILTER}`; "
      f"companies use `customer_type__cloned_ = {CUSTOMER_TYPE_FILTER}`). "
      f"Generated {snapshot['generated_at_utc']} UTC from HubSpot. Deal values "
      f"are USD (`{HOME_CURRENCY_PROP}`).*")
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
    # Headline counts line.
    stale_sqls = m["stale_engaged_sqls"]
    prev_stale_sql = None
    if prev_m and "stale_engaged_sqls" in prev_m:
        prev_stale_sql = len(prev_m["stale_engaged_sqls"])
    misc_prev = prev_m["miscategorised_count"] if prev_m else None
    A(f"- **Qualified-to-Buy deals idle {STALE_ACTIVITY_DAYS}+ days:** "
      f"{len(m['qualified_to_buy_stale'])}")
    A(f"- **Engaged SQLs gone quiet ({STALE_SQL_ACTIVITY_DAYS}+ days):** "
      f"{delta_str(len(stale_sqls), prev_stale_sql)}")
    A(f"- **Miscategorised leads:** {delta_str(m['miscategorised_count'], misc_prev)}")
    if m["funnel_inversion"]:
        A(f"- **⚠️ Funnel inversion:** SQL ({m['sql_count']}) > MQL ({m['mql_count']})")
    if m["deals_missing_home_currency"]:
        A(f"- **⚠️ Deals missing USD value ({HOME_CURRENCY_PROP} empty):** "
          f"{len(m['deals_missing_home_currency'])} (excluded from sums — see below)")
    A("")

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
            last = (d["notes_last_updated"] or "—")[:10]
            A(f"| {d['dealname']} (#{d['id']}) | {last} | {days} "
              f"| {_usd(d['usd'])} | {_ref_amount(d['amount'], d['currency'])} |")
        A("")
    else:
        A(f"- ✅ No \"Qualified to Buy\" deals idle for {STALE_ACTIVITY_DAYS}+ days.")
        A("")

    if m["funnel_inversion"]:
        A(f"### ⚠️ Funnel inversion: SQL ({m['sql_count']}) > MQL "
          f"({m['mql_count']})")
        A("")
        A("More Sales Qualified than Marketing Qualified Leads. Expected the "
          "funnel to narrow (MQL ≥ SQL). Likely causes: leads skipping the MQL "
          "stage, MQL criteria too strict, or bulk SQL promotion. Worth a look.")
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

    # ---- Deals by stage ---------------------------------------------------
    A("## Deals — count and value by stage (USD)")
    A("")
    A(f"> Values use `{HOME_CURRENCY_PROP}` (HubSpot's USD conversion). Deals "
      f"exist in {', '.join(m['deal_currencies']) or 'USD'}; raw `amount` is "
      f"never summed across currencies. Original amount + currency shown only "
      f"as a per-deal reference elsewhere.")
    A("")
    # Value-delta is only comparable if the prior snapshot also stored USD
    # totals (total_usd). Older snapshots stored raw mixed-currency sums
    # (total_amount) which are NOT comparable, so we suppress those deltas.
    prev_deals = {}
    prev_has_usd = False
    if prev_m:
        prev_deals = {d["stage"]: d for d in prev_m["deals_by_stage"]}
        prev_has_usd = any("total_usd" in d for d in prev_m["deals_by_stage"])
    A("| Deal stage | Deals | Total value (USD) |")
    A("|------------|-------|-------------------|")
    tot_count = 0
    tot_usd = 0.0
    for d in m["deals_by_stage"]:
        tot_count += d["count"]
        tot_usd += d["total_usd"]
        pv = prev_deals.get(d["stage"])
        pv_count = pv["count"] if pv else None
        pv_usd = pv.get("total_usd") if (pv and prev_has_usd) else None
        flag = f" ⚠️{d['missing_usd']} no-USD" if d.get("missing_usd") else ""
        A(f"| {d['label']} | {delta_str(d['count'], pv_count)} "
          f"| {money_delta_str(d['total_usd'], pv_usd)}{flag} |")
    prev_tot_count = sum(d["count"] for d in prev_m["deals_by_stage"]) if prev_m else None
    A(f"| **Total** | **{delta_str(tot_count, prev_tot_count)}** | **${tot_usd:,.0f}** |")
    A("")
    if prev_m and not prev_has_usd:
        A("*USD-normalised deal values start this run (the prior snapshot summed "
          "raw mixed-currency amounts, which are not comparable), so per-stage "
          "value deltas begin next run. Deal-count deltas are shown.*")
        A("")
    A("*Value per stage note: pre-deal lifecycle stages (Email Contact, Lead, "
      "MQL) are contact counts only and carry no deal value — deal value exists "
      "only from the deal pipeline stages above.*")
    A("")
    if m["deals_missing_home_currency"]:
        A(f"**⚠️ {len(m['deals_missing_home_currency'])} deal(s) have an empty "
          f"`{HOME_CURRENCY_PROP}` and are excluded from the USD sums** (flagged, "
          f"not coerced from raw amount):")
        A("")
        A("| Deal | Stage | Original amount |")
        A("|------|-------|-----------------|")
        for d in m["deals_missing_home_currency"]:
            A(f"| {d['dealname']} (#{d['id']}) | {d['label']} "
              f"| {_ref_amount(d['amount'], d['currency'])} |")
        A("")

    # ---- Deals moved this week -------------------------------------------
    moved = m["deals_moved_this_week"]
    A(f"## 📈 Deals moved this week (last {MOVED_WINDOW_DAYS} days)")
    A("")
    A("What's active and how the pipeline shifted this week — grouped by current "
      "stage, highest USD value first.")
    A("")
    if not moved:
        A(f"- No deals modified or with activity in the last {MOVED_WINDOW_DAYS} days.")
        A("")
    else:
        # Group by current stage; order groups by total USD desc.
        groups = {}
        for d in moved:
            groups.setdefault(d["stage"], []).append(d)
        def group_usd(items):
            return sum((x["usd"] or 0) for x in items)
        ordered = sorted(groups.items(),
                         key=lambda kv: group_usd(kv[1]), reverse=True)
        A(f"*{len(moved)} deal(s) moved across {len(ordered)} stage(s).*")
        A("")
        for stage, items in ordered:
            items.sort(key=lambda x: (x["usd"] or 0), reverse=True)
            label = DEALSTAGE_LABELS.get(stage, stage)
            A(f"### {label} — {len(items)} deal(s), "
              f"${group_usd(items):,.0f}")
            A("")
            A("| Deal | USD value | What changed | Last activity | Original |")
            A("|------|-----------|--------------|---------------|----------|")
            for d in items:
                la = (d["last_activity"] or "—")[:10]
                A(f"| {d['dealname']} (#{d['id']}) | {_usd(d['usd'])} "
                  f"| {d['change']} | {la} "
                  f"| {_ref_amount(d['amount'], d['currency'])} |")
            A("")

    # ---- Follow-up / review: engaged SQLs gone quiet ----------------------
    A(f"## 🕓 Follow-up / review: engaged SQLs gone quiet "
      f"({delta_str(len(stale_sqls), prev_stale_sql)})")
    A("")
    A(f"Sales has already made real contact on these B2B SQLs (contacts **and** "
      f"companies) but activity has since stalled ({STALE_SQL_ACTIVITY_DAYS}+ "
      f"days idle). **This is a follow-up / review list, NOT an auto-recycle** — "
      f"these are live relationships for sales to action and nothing here moves "
      f"automatically. Distinct from the \"Attempted to Contact\" cases the "
      f"stale-lead recycler handles. Sorted most-stalled first.")
    A("")
    if stale_sqls:
        by_status = {}
        for c in stale_sqls:
            by_status.setdefault(c["hs_lead_status"], []).append(c)
        for status, group in by_status.items():
            group.sort(key=lambda x: (x["days_since_activity"] is not None,
                                      x["days_since_activity"] or 0), reverse=True)
            A(f"**{status}** ({len(group)})")
            A("")
            A("| Record | Type | Days idle | Owner | Associated deal | Deal USD |")
            A("|--------|------|-----------|-------|-----------------|----------|")
            for c in group:
                days = "no activity logged" if c["days_since_activity"] is None \
                    else f"{c['days_since_activity']}d"
                deal = c.get("deal") or {}
                dname = deal.get("name", "—") if deal else "—"
                dusd = _usd(deal.get("usd")) if deal else "—"
                A(f"| {c['name']} (#{c['id']}) | {c.get('record_type','contact')} "
                  f"| {days} | {c.get('owner') or '—'} | {dname} | {dusd} |")
            A("")
    else:
        A(f"- ✅ No engaged SQLs idle for {STALE_SQL_ACTIVITY_DAYS}+ days.")
        A("")

    # ---- Miscategorised list ---------------------------------------------
    A(f"## 🏷️ Miscategorised leads ({delta_str(m['miscategorised_count'], misc_prev)})")
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

    A("---")
    A(f"*Snapshot: `snapshots/{date}.json` · Report: `reports/{date}.md`*")
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
        contacts, deals, companies = fetch_from_hubspot(token)
        raw = build_inputs_from_live(contacts, deals, companies)
        source = "hubspot (live REST)"

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
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
        "home_currency_property": HOME_CURRENCY_PROP,
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
