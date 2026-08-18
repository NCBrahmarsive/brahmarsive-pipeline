#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report.

What it does
------------
Pulls contacts, deals and companies from HubSpot, filters the funnel to B2B
contacts, and writes per run:

  snapshots/YYYY-MM-DD.json  -- the pulled data + computed metrics (machine)
  reports/YYYY-MM-DD.md      -- readable summary with funnel, ratios, deal
                                values (USD), alerts, "deals moved this week",
                                the stale-engaged-SQL follow-up list and the
                                miscategorised list

Each run loads the most recent previous snapshot from snapshots/ and shows the
week-on-week movement for every metric.

Two data sources
----------------
1. Live (default): fetch straight from HubSpot's REST API using the
   HUBSPOT_TOKEN environment variable.

       python3 pipeline.py

2. Pre-pulled (--input FILE): consume data already pulled from HubSpot and
   stored as JSON. This is used when direct egress to api.hubapi.com is not
   permitted from the run environment (e.g. an org egress policy blocks it),
   so the data is fetched via the HubSpot MCP tools and handed to the same
   computation code here.

       python3 pipeline.py --input raw_input.json

Currency
--------
The HubSpot account home currency is USD, but deals can be created in other
currencies (notably INR). Every deal total in this report is summed using
HubSpot's home-currency (USD) converted value -- stored on the deal as
`amount_in_home_currency` (HubSpot's "Amount in company currency"). The raw
`amount` + `deal_currency_code` are shown only as a secondary reference and are
NEVER summed across mixed currencies. Deals whose home-currency value is empty
are flagged rather than falling back to the raw amount.

The token value is never printed or committed.
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

# The B2B filter is the single exclusion switch: keeping only customer_type=B2B
# automatically drops B2C, Partner, Vendor, Internal and Partner-B2B contacts.
CUSTOMER_TYPE_FILTER = "B2B"

# Owner-id -> name resolution (HubSpot owners lookup). Used for live mode; the
# --input path resolves owners upstream via the MCP owners lookup.
OWNER_NAMES = {
    "83228721": "Nicola Cole",
    "83249199": "Ramjee Jadu",
    "83361077": "Vrinda Vinod",
    "84004434": "Anup Ghoshal",
    "84004493": "Theo Zirogiannis",
    "85402717": "Vangili Palaniappan",
    "87058564": "Al Mufeed Khazi",
    "87318764": "Jack Makhlouf",
    "87318803": "Biju Rajesh",
    "90325392": "Panthalarajakumaran Kamaraj",
    "90325421": "Aksharaa Parameswaran",
    "96259786": "Mokesh Kumarasamy",
}

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
# stalled. This is a follow-up/review list for sales to action -- nothing
# here is moved automatically. 30-day threshold is tunable below.
#
# Contacts carry the full set of working statuses; companies in this portal
# only expose CONNECTED / Qualified (no In Progress / Proposal Request), so
# the intersection applies automatically per object type.
STALE_SQL_LEAD_STATUSES = {"CONNECTED", "Qualified", "IN_PROGRESS", "PROPOSAL_REQUEST"}
STALE_SQL_ACTIVITY_DAYS = 30

# Human labels for lead-status internal values (contacts & companies).
LEAD_STATUS_LABELS = {
    "CONNECTED": "Connected",
    "Qualified": "Qualified",
    "IN_PROGRESS": "In Progress",
    "PROPOSAL_REQUEST": "Proposal Request",
    "Attempted to Contact": "Attempted to Contact",
    "ATTEMPTED_TO_CONTACT": "Attempted to Contact",
}

# =========================================================================

# Deal-alert configuration.
QUALIFIED_TO_BUY_STAGE = "appointmentscheduled"   # internal value for "Qualified to Buy"
STALE_ACTIVITY_DAYS = 14

# "Deals moved this week" window.
MOVED_THIS_WEEK_DAYS = 7

# The deal property holding the home-currency (USD) converted amount. HubSpot's
# label is "Amount in company currency". (The routine spec calls this
# `hs_deal_amount_in_home_currency`, which does not exist in this portal;
# `amount_in_home_currency` is the equivalent home-currency field.)
HOME_CURRENCY_PROP = "amount_in_home_currency"
VALUE_BASIS = "home_currency_usd"

# Contact properties we pull.
CONTACT_PROPERTIES = [
    "lifecyclestage", "hs_lead_status", "customer_type",
    "b2b_contact_lead_score_combined", "hs_lastmodifieddate",
    "notes_last_updated", "hubspot_owner_id", "hs_full_name_or_email",
]
# Deal properties we pull. notes_last_updated is HubSpot's "Last Activity Date".
DEAL_PROPERTIES = [
    "dealstage", "amount", "deal_currency_code", HOME_CURRENCY_PROP,
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
        ["name", "hs_lead_status", "lifecyclestage", "notes_last_updated",
         "hubspot_owner_id"],
        token,
        filter_groups=[{
            "filters": [
                {"propertyName": "customer_type__cloned_", "operator": "EQ",
                 "value": CUSTOMER_TYPE_FILTER},
                {"propertyName": "lifecyclestage", "operator": "EQ",
                 "value": "salesqualifiedlead"},
                {"propertyName": "hs_lead_status", "operator": "IN",
                 "values": sorted(STALE_SQL_LEAD_STATUSES)},
            ]
        }],
    )
    return contacts, deals, companies


def build_inputs_from_live(contacts, deals, companies):
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
            oid = p.get("hubspot_owner_id")
            sql_contacts.append({
                "id": str(c.get("id")),
                "name": p.get("hs_full_name_or_email", ""),
                "hs_lead_status": p.get("hs_lead_status"),
                "notes_last_updated": p.get("notes_last_updated"),
                "owner": OWNER_NAMES.get(oid, oid),
                "hubspot_owner_id": oid,
            })
    sql_companies = []
    for co in companies:
        p = co.get("properties", {})
        oid = p.get("hubspot_owner_id")
        sql_companies.append({
            "id": str(co.get("id")),
            "name": p.get("name", ""),
            "hs_lead_status": p.get("hs_lead_status"),
            "notes_last_updated": p.get("notes_last_updated"),
            "owner": OWNER_NAMES.get(oid, oid),
            "hubspot_owner_id": oid,
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
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
    """Whole days between an ISO timestamp and the run datetime, or None."""
    last = _parse_ts(value)
    if last is None:
        return None
    if last.tzinfo is None:
        last = last.replace(tzinfo=dt.timezone.utc)
    return (run_dt - last).days


def _usd(deal):
    """Home-currency (USD) value of a deal, or None if not set/convertible."""
    return _to_float(deal.get(HOME_CURRENCY_PROP))


def compute_metrics(raw, run_date, prev=None):
    funnel_counts = raw["funnel_counts"]
    run_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)

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

    # ---- Deals by stage: count + summed USD (home currency) -------------
    by_stage = {}
    currencies = set()
    flagged_deals = []          # empty/missing home-currency value
    for d in raw["deals"]:
        stage = d.get("dealstage") or "unknown"
        entry = by_stage.setdefault(stage, {"count": 0, "usd": 0.0})
        entry["count"] += 1
        usd = _usd(d)
        if usd is None:
            flagged_deals.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "stage": stage,
                "label": DEALSTAGE_LABELS.get(stage, stage),
                "amount": d.get("amount"),
                "currency": d.get("deal_currency_code"),
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
        })
    deals_by_stage.sort(key=lambda x: (-x["total_usd"], -x["count"], x["label"]))

    # ---- Deals moved this week ------------------------------------------
    prev_deal_stage = {}
    if prev:
        for pd in prev.get("raw", {}).get("deals", []):
            prev_deal_stage[str(pd.get("id"))] = pd.get("dealstage")
    moved = []
    for d in raw["deals"]:
        mod_days = _days_since(d.get("hs_lastmodifieddate"), run_dt)
        act_days = _days_since(d.get("notes_last_updated"), run_dt)
        recent_mod = mod_days is not None and mod_days < MOVED_THIS_WEEK_DAYS
        recent_act = act_days is not None and act_days < MOVED_THIS_WEEK_DAYS
        if not (recent_mod or recent_act):
            continue
        stage = d.get("dealstage") or "unknown"
        did = str(d.get("id"))
        # Detect what changed.
        if prev and did not in prev_deal_stage:
            change = "New deal (not in prior snapshot)"
        elif prev and prev_deal_stage.get(did) != stage:
            change = (f"Stage change: "
                      f"{DEALSTAGE_LABELS.get(prev_deal_stage.get(did), prev_deal_stage.get(did))}"
                      f" → {DEALSTAGE_LABELS.get(stage, stage)}")
        elif recent_act:
            change = "Activity logged"
        else:
            change = "Field update"
        moved.append({
            "id": did,
            "dealname": d.get("dealname", ""),
            "stage": stage,
            "label": DEALSTAGE_LABELS.get(stage, stage),
            "usd": _usd(d),
            "currency": d.get("deal_currency_code"),
            "amount": d.get("amount"),
            "change": change,
            "last_activity": d.get("notes_last_updated"),
            "hs_lastmodifieddate": d.get("hs_lastmodifieddate"),
            "days_since_activity": act_days,
        })
    # Group by current stage, highest USD first within each stage.
    moved.sort(key=lambda x: (x["usd"] if x["usd"] is not None else -1), reverse=True)

    # ---- Alert: Qualified-to-Buy deals with no activity in 14+ days -----
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
                "usd": _usd(d),
                "amount": d.get("amount"),
                "currency": d.get("deal_currency_code"),
            })
    stale.sort(key=lambda x: (x["days_since_activity"] is not None,
                              x["days_since_activity"] or 0), reverse=True)

    # ---- Alert: SQLs engaged but gone quiet (contacts + companies) ------
    stale_sqls = []
    for c in raw.get("sql_contacts", []):
        days = _days_since(c.get("notes_last_updated"), run_dt)
        if days is None or days >= STALE_SQL_ACTIVITY_DAYS:
            stale_sqls.append({
                "type": "contact",
                "id": c.get("id"),
                "name": c.get("name", ""),
                "hs_lead_status": c.get("hs_lead_status"),
                "owner": c.get("owner"),
                "notes_last_updated": c.get("notes_last_updated"),
                "days_since_activity": days,
                "deal_name": c.get("deal_name"),
                "deal_usd": c.get("deal_usd"),
            })
    for co in raw.get("sql_companies", []):
        days = _days_since(co.get("notes_last_updated"), run_dt)
        if days is None or days >= STALE_SQL_ACTIVITY_DAYS:
            stale_sqls.append({
                "type": "company",
                "id": co.get("id"),
                "name": co.get("name", ""),
                "hs_lead_status": co.get("hs_lead_status"),
                "owner": co.get("owner"),
                "notes_last_updated": co.get("notes_last_updated"),
                "days_since_activity": days,
                "deal_name": co.get("deal_name"),
                "deal_usd": co.get("deal_usd"),
            })
    stale_sqls.sort(key=lambda x: (x["days_since_activity"] is not None,
                                    x["days_since_activity"] or 0), reverse=True)

    misc = raw.get("miscategorised", [])
    return {
        "value_basis": VALUE_BASIS,
        "funnel": funnel,
        "mql_count": mql,
        "sql_count": sql,
        "sql_to_mql_ratio": ratio,
        "funnel_inversion": sql > mql,
        "deals_by_stage": deals_by_stage,
        "deal_currencies": sorted(currencies),
        "flagged_deals": flagged_deals,
        "deals_moved_this_week": moved,
        "qualified_to_buy_stale": stale,
        "stale_engaged_sqls": stale_sqls,
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

def _fmt_usd(v):
    return "n/a" if v is None else f"${v:,.0f}"


def render_report(snapshot, prev):
    m = snapshot["metrics"]
    prev_m = prev["metrics"] if prev else None
    # Deal values are only comparable week-on-week if the prior snapshot was
    # also computed on the USD home-currency basis.
    prev_usd_basis = bool(prev_m and prev_m.get("value_basis") == VALUE_BASIS)
    date = snapshot["run_date"]
    out = []
    A = out.append

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {date}")
    A("")
    A(f"*Scope: B2B contacts only (`customer_type = {CUSTOMER_TYPE_FILTER}`). "
      f"Generated {snapshot['generated_at_utc']} UTC from HubSpot.*")
    A("")
    A("*Deal values are HubSpot's home-currency (USD) converted amounts "
      "(`amount_in_home_currency`). Raw amount + currency are shown only as a "
      "secondary reference and never summed across currencies.*")
    A("")
    if prev:
        A(f"*Week-on-week deltas are vs the previous snapshot "
          f"(`{prev['run_date']}`).*")
    else:
        A("*No prior snapshot found — week-on-week comparison starts next run.*")
    A("")

    # ---- Alerts summary line ---------------------------------------------
    stale = m["qualified_to_buy_stale"]
    stale_sqls = m["stale_engaged_sqls"]
    prev_stale_sql = len(prev_m.get("stale_engaged_sqls", [])) if prev_m else None
    misc_prev = prev_m.get("miscategorised_count") if prev_m else None
    A("## \U0001f514 Alerts")
    A("")
    A(f"- **\"Qualified to Buy\" deals idle {STALE_ACTIVITY_DAYS}+ days:** {len(stale)}")
    A(f"- **Engaged SQLs gone quiet ({STALE_SQL_ACTIVITY_DAYS}+ days):** "
      f"{delta_str(len(stale_sqls), prev_stale_sql)}")
    A(f"- **Miscategorised leads:** {delta_str(m['miscategorised_count'], misc_prev)}")
    if m["funnel_inversion"]:
        A(f"- **⚠️ Funnel inversion:** SQL ({m['sql_count']}) > MQL ({m['mql_count']})")
    if m["flagged_deals"]:
        A(f"- **⚠️ Deals with no USD (home-currency) value:** "
          f"{len(m['flagged_deals'])} (excluded from value totals — see below)")
    A("")

    # ---- Qualified to Buy stale detail -----------------------------------
    if stale:
        A(f"### ⚠️ {len(stale)} \"Qualified to Buy\" deal(s) with no activity "
          f"in {STALE_ACTIVITY_DAYS}+ days")
        A("")
        A("| Deal | Last activity | Days idle | USD value | Raw (orig) |")
        A("|------|---------------|-----------|-----------|------------|")
        for d in stale:
            days = "no activity logged" if d["days_since_activity"] is None \
                else f"{d['days_since_activity']}d"
            last = (d["notes_last_updated"] or "—")[:10]
            raw_amt = "—"
            if d.get("amount") not in (None, "", "0"):
                raw_amt = f"{float(d['amount']):,.0f} {d.get('currency') or ''}".strip()
            A(f"| {d['dealname']} (#{d['id']}) | {last} | {days} | "
              f"{_fmt_usd(d['usd'])} | {raw_amt} |")
        A("")
    else:
        A(f"- ✅ No \"Qualified to Buy\" deals idle for {STALE_ACTIVITY_DAYS}+ days.")
        A("")

    # ---- Stale engaged SQLs (follow-up / review) -------------------------
    A(f"### \U0001f553 SQLs engaged but gone quiet: {delta_str(len(stale_sqls), prev_stale_sql)}")
    A("")
    A(f"**Follow-up / review — NOT auto-recycle.** Sales has already made real "
      f"contact on these Sales Qualified Leads, but activity has stalled "
      f"({STALE_SQL_ACTIVITY_DAYS}+ days idle). These are live relationships for sales "
      "to action; nothing here moves automatically. Distinct from the "
      "\"Attempted to Contact\" cases the stale-lead recycler handles on its own. "
      "Includes B2B contacts and B2B companies. Sorted most-stalled first.")
    A("")
    if stale_sqls:
        by_status = {}
        for c in stale_sqls:
            by_status.setdefault(c["hs_lead_status"], []).append(c)
        for status in sorted(by_status, key=lambda s: -len(by_status[s])):
            group = by_status[status]
            label = LEAD_STATUS_LABELS.get(status, status)
            A(f"**{label}** ({len(group)})")
            A("")
            A("| Record | Type | Days idle | Owner | Associated deal (USD) |")
            A("|--------|------|-----------|-------|-----------------------|")
            for c in group:
                days = "no activity logged" if c["days_since_activity"] is None \
                    else f"{c['days_since_activity']}d"
                deal = "—"
                if c.get("deal_name"):
                    deal = f"{c['deal_name']} ({_fmt_usd(c.get('deal_usd'))})"
                A(f"| {c['name']} (#{c['id']}) | {c['type']} | {days} | "
                  f"{c.get('owner') or '—'} | {deal} |")
            A("")
    else:
        A(f"- ✅ No engaged SQLs idle for {STALE_SQL_ACTIVITY_DAYS}+ days.")
        A("")

    # ---- Miscategorised leads --------------------------------------------
    A(f"### \U0001f3f7️ Miscategorised leads: "
      f"{delta_str(m['miscategorised_count'], misc_prev)}")
    A("")
    A("Sales is working these contacts (lead status) but their lifecycle stage "
      "is still only Lead / MQL — they should be Sales Qualified Lead or beyond.")
    A("")
    if m["miscategorised"]:
        A("| Contact ID | Name | Lead status | Lifecycle stage |")
        A("|------------|------|-------------|-----------------|")
        for c in m["miscategorised"]:
            A(f"| {c['id']} | {c.get('name','')} | "
              f"{LEAD_STATUS_LABELS.get(c.get('hs_lead_status'), c.get('hs_lead_status',''))} "
              f"| {LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage',''))} |")
    else:
        A("- None this run.")
    A("")
    A(f"Contact IDs: {', '.join(c['id'] for c in m['miscategorised']) or 'none'}")
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
    moved = m["deals_moved_this_week"]
    A(f"## \U0001f4c8 Deals moved this week (last {MOVED_THIS_WEEK_DAYS} days)")
    A("")
    A("Deals with a stage change, logged activity, or field update in the last "
      f"{MOVED_THIS_WEEK_DAYS} days — the \"what moved this week\" view for the Tuesday "
      "leadership call. Grouped by current stage, highest USD value first.")
    A("")
    if moved:
        by_stage_moved = {}
        for d in moved:
            by_stage_moved.setdefault(d["label"], []).append(d)
        # Order stages by their highest-value deal.
        stage_order = sorted(
            by_stage_moved,
            key=lambda lbl: max((x["usd"] or 0) for x in by_stage_moved[lbl]),
            reverse=True,
        )
        for label in stage_order:
            group = by_stage_moved[label]
            A(f"**{label}** ({len(group)})")
            A("")
            A("| Deal | USD value | What changed | Last activity |")
            A("|------|-----------|--------------|---------------|")
            for d in group:
                last = (d["last_activity"] or d["hs_lastmodifieddate"] or "—")[:10]
                A(f"| {d['dealname']} (#{d['id']}) | {_fmt_usd(d['usd'])} | "
                  f"{d['change']} | {last} |")
            A("")
    else:
        A("- No deals moved in the last 7 days.")
        A("")

    # ---- Funnel ----------------------------------------------------------
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

    # ---- MQL / SQL -------------------------------------------------------
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

    # ---- Deals -----------------------------------------------------------
    A("## Deals — count and value by stage (USD)")
    A("")
    if len(m["deal_currencies"]) > 1:
        A(f"> Deals span multiple currencies ({', '.join(m['deal_currencies'])}). "
          f"All totals below use HubSpot's home-currency (USD) converted value "
          f"(`amount_in_home_currency`); raw amounts are never summed across "
          f"currencies.")
        A("")
    if not prev_usd_basis and prev_m:
        A("> **Week-on-week note:** the prior snapshot summed raw mixed-currency "
          "amounts, so USD value deltas are not comparable this run. Deal *counts* "
          "show deltas; USD *values* show current figures only. USD-basis "
          "value comparison starts next run.")
        A("")
    A("| Deal stage | Deals | Total value (USD) |")
    A("|------------|-------|-------------------|")
    prev_deals = {d["stage"]: d for d in prev_m["deals_by_stage"]} if prev_m else {}
    tot_count = 0
    tot_usd = 0.0
    for d in m["deals_by_stage"]:
        tot_count += d["count"]
        tot_usd += d["total_usd"]
        pv = prev_deals.get(d["stage"]) if prev_m else None
        pv_count = pv["count"] if pv else None
        pv_usd = pv.get("total_usd") if (pv and prev_usd_basis) else None
        usd_cell = (money_delta_str(d["total_usd"], pv_usd) if prev_usd_basis
                    else f"{d['total_usd']:,.0f}")
        A(f"| {d['label']} | {delta_str(d['count'], pv_count)} | ${usd_cell} |")
    A(f"| **Total** | **{tot_count}** | **${tot_usd:,.0f}** |")
    A("")
    A("*Value per stage note: pre-deal lifecycle stages (Email Contact, Lead, "
      "MQL) are contact counts only and carry no deal value — deal value exists "
      "only from the deal pipeline stages above.*")
    A("")

    # ---- Flagged deals (no USD value) ------------------------------------
    if m["flagged_deals"]:
        A(f"### ⚠️ {len(m['flagged_deals'])} deal(s) with no home-currency (USD) value")
        A("")
        A("These deals have an empty `amount_in_home_currency` and are excluded "
          "from the USD totals above (no fallback to raw amount). Fix the "
          "currency/amount in HubSpot so they convert.")
        A("")
        A("| Deal | Stage | Raw amount | Currency |")
        A("|------|-------|------------|----------|")
        for d in m["flagged_deals"]:
            raw_amt = d.get("amount")
            raw_amt = "—" if raw_amt in (None, "") else raw_amt
            A(f"| {d['dealname']} (#{d['id']}) | {d['label']} | {raw_amt} | "
              f"{d.get('currency') or '—'} |")
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
    metrics = compute_metrics(raw, run_date, prev=prev)

    generated = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    snapshot = {
        "run_date": run_date.isoformat(),
        "generated_at_utc": generated,
        "source": source,
        "customer_type_filter": CUSTOMER_TYPE_FILTER,
        "value_basis": VALUE_BASIS,
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
