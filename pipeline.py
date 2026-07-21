#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report.

What it does
------------
Pulls contacts, deals and companies from HubSpot, filters the funnel to B2B
records, and writes two files per run:

  snapshots/YYYY-MM-DD.json  -- the pulled data + computed metrics (machine)
  reports/YYYY-MM-DD.md      -- readable summary with funnel, ratios, deal
                                values, alerts and the miscategorised list

Each run loads the most recent previous snapshot from snapshots/ and shows the
week-on-week movement for every metric.

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

Currency
--------
The HubSpot account default (home) currency is USD, but deals can be created in
other currencies (notably INR). All deal totals are summed on HubSpot's
home-currency-converted value (`amount_in_home_currency`, USD). Raw `amount` +
`deal_currency_code` are kept only as a secondary per-deal reference and are
NEVER summed across mixed currencies. Deals with an empty home-currency value
are flagged, not silently fallen back to the raw amount.
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
# (Companies carry the equivalent flag on customer_type__cloned_.)
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
# stalled. This is a follow-up/review list for sales to action -- nothing
# here is moved automatically. 30-day threshold is tunable below.
STALE_SQL_LEAD_STATUSES = {"CONNECTED", "Qualified", "IN_PROGRESS", "PROPOSAL_REQUEST"}
STALE_SQL_ACTIVITY_DAYS = 30

# =========================================================================

# Deal-alert configuration.
QUALIFIED_TO_BUY_STAGE = "appointmentscheduled"   # internal value for "Qualified to Buy"
STALE_ACTIVITY_DAYS = 14
# "What moved this week" lookback window.
MOVED_THIS_WEEK_DAYS = 7

# Contact properties we pull.
CONTACT_PROPERTIES = [
    "lifecyclestage", "hs_lead_status", "customer_type",
    "b2b_contact_lead_score_combined", "hs_lastmodifieddate",
    "notes_last_updated", "hubspot_owner_id", "hs_full_name_or_email",
]
# Deal properties we pull. notes_last_updated is HubSpot's "Last Activity Date".
# amount_in_home_currency is HubSpot's home-currency (USD) converted value; it
# is the field the task refers to as hs_deal_amount_in_home_currency.
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
    """Aggregate live-pulled records into the same shape as an --input file.

    NOTE: In live mode hubspot_owner_id is left as the raw id (owner-name
    resolution needs a separate owners lookup, which the --input path does via
    the HubSpot MCP owners tool). Companies are not pulled in this simple live
    path; the --input path is the authoritative source for this environment.
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
                "deal_name": None,
                "deal_amount_usd": None,
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
# Computation helpers
# --------------------------------------------------------------------------

def _to_float(value):
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _has_value(value):
    return value is not None and str(value).strip() != ""


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
    """Whole days between an ISO timestamp and run_dt, or None if unparseable."""
    last = _parse_ts(value)
    if last is None:
        return None
    if last.tzinfo is None:
        last = last.replace(tzinfo=dt.timezone.utc)
    return (run_dt - last).days


def _deal_usd(d):
    """Home-currency (USD) value of a deal, or None if it is empty/missing.

    Never falls back to the raw `amount` when the home-currency value is
    empty -- an empty value is flagged instead (see missing_home_currency).
    """
    home = d.get("amount_in_home_currency")
    if not _has_value(home):
        return None
    try:
        return float(home)
    except (TypeError, ValueError):
        return None


def _last_touch(d):
    """Most recent of hs_lastmodifieddate / notes_last_updated for a deal."""
    stamps = [_parse_ts(d.get("hs_lastmodifieddate")),
              _parse_ts(d.get("notes_last_updated"))]
    stamps = [s for s in stamps if s is not None]
    if not stamps:
        return None
    return max((s.replace(tzinfo=dt.timezone.utc) if s.tzinfo is None else s)
               for s in stamps)


# --------------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------------

def compute_metrics(raw, run_date, prev_raw=None):
    funnel_counts = raw["funnel_counts"]
    run_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)

    # ---- Funnel (ordered, with labels) ------------------------------------
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

    # ---- Deals by stage: count + USD (home-currency) total ----------------
    by_stage = {}
    currencies = set()
    missing_home_currency = []
    for d in raw["deals"]:
        stage = d.get("dealstage") or "unknown"
        entry = by_stage.setdefault(
            stage, {"count": 0, "usd": 0.0, "currencies": set()})
        entry["count"] += 1
        if d.get("deal_currency_code"):
            currencies.add(d["deal_currency_code"])
            entry["currencies"].add(d["deal_currency_code"])
        usd = _deal_usd(d)
        if usd is None:
            missing_home_currency.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "stage": stage,
                "label": DEALSTAGE_LABELS.get(stage, stage),
                "amount": d.get("amount"),
                "currency": d.get("deal_currency_code"),
            })
        else:
            entry["usd"] += usd
    deals_by_stage = []
    for stage, entry in by_stage.items():
        deals_by_stage.append({
            "stage": stage,
            "label": DEALSTAGE_LABELS.get(stage, stage),
            "count": entry["count"],
            "total_usd": round(entry["usd"], 2),
            "currencies": sorted(entry["currencies"]),
        })
    deals_by_stage.sort(key=lambda x: (-x["total_usd"], -x["count"], x["label"]))

    # ---- Alert: Qualified-to-Buy deals with no activity in 14+ days --------
    stale = []
    for d in raw["deals"]:
        if d.get("dealstage") != QUALIFIED_TO_BUY_STAGE:
            continue
        days = _days_since(d.get("notes_last_updated"), run_dt)
        if days is None or days >= STALE_ACTIVITY_DAYS:
            usd = _deal_usd(d)
            stale.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "notes_last_updated": d.get("notes_last_updated"),
                "days_since_activity": days,
                "amount_usd": usd,
                "amount": d.get("amount"),
                "currency": d.get("deal_currency_code"),
            })
    stale.sort(key=lambda x: (x["days_since_activity"] is not None,
                              x["days_since_activity"] or 0), reverse=True)

    # ---- "What moved this week" -------------------------------------------
    prev_deal_stage = {}
    if prev_raw:
        for d in prev_raw.get("deals", []):
            prev_deal_stage[str(d.get("id"))] = d.get("dealstage")
    moved = []
    for d in raw["deals"]:
        lm = _days_since(d.get("hs_lastmodifieddate"), run_dt)
        na = _days_since(d.get("notes_last_updated"), run_dt)
        touched = [x for x in (lm, na) if x is not None]
        if not touched or min(touched) > MOVED_THIS_WEEK_DAYS:
            continue
        stage = d.get("dealstage") or "unknown"
        prev_stage = prev_deal_stage.get(str(d.get("id")))
        activity_recent = na is not None and na <= MOVED_THIS_WEEK_DAYS
        if prev_stage is not None and prev_stage != stage:
            change = (f"stage change: {DEALSTAGE_LABELS.get(prev_stage, prev_stage)} "
                      f"→ {DEALSTAGE_LABELS.get(stage, stage)}")
        elif prev_stage is None and prev_raw is not None:
            change = "new deal (not in prior snapshot)"
        elif activity_recent:
            change = "activity logged"
        else:
            change = "field update"
        last_act = _parse_ts(d.get("notes_last_updated"))
        moved.append({
            "id": d.get("id"),
            "dealname": d.get("dealname", ""),
            "stage": stage,
            "label": DEALSTAGE_LABELS.get(stage, stage),
            "amount_usd": _deal_usd(d),
            "currency": d.get("deal_currency_code"),
            "amount": d.get("amount"),
            "change": change,
            "last_activity": (d.get("notes_last_updated") or "")[:10]
                             if d.get("notes_last_updated") else None,
            "last_modified": (d.get("hs_lastmodifieddate") or "")[:10],
            "days_since_activity": na,
        })
    moved.sort(key=lambda x: (x["amount_usd"] is not None, x["amount_usd"] or 0),
               reverse=True)

    # ---- SQLs engaged but gone quiet (contacts + companies) ---------------
    stale_sqls = []
    for kind, records in (("contact", raw.get("sql_contacts", [])),
                          ("company", raw.get("sql_companies", []))):
        for c in records:
            days = _days_since(c.get("notes_last_updated"), run_dt)
            if days is None or days >= STALE_SQL_ACTIVITY_DAYS:
                stale_sqls.append({
                    "id": c.get("id"),
                    "name": c.get("name", ""),
                    "record_type": kind,
                    "hs_lead_status": c.get("hs_lead_status"),
                    "notes_last_updated": c.get("notes_last_updated"),
                    "days_since_activity": days,
                    "owner": c.get("owner"),
                    "deal_name": c.get("deal_name"),
                    "deal_amount_usd": c.get("deal_amount_usd"),
                })
    stale_sqls.sort(key=lambda x: (x["days_since_activity"] is not None,
                                    x["days_since_activity"] or 0), reverse=True)

    misc = raw.get("miscategorised", [])
    return {
        "value_basis": "usd_home_currency",
        "funnel": funnel,
        "mql_count": mql,
        "sql_count": sql,
        "sql_to_mql_ratio": ratio,
        "funnel_inversion": sql > mql,
        "deals_by_stage": deals_by_stage,
        "deal_currencies": sorted(currencies),
        "missing_home_currency": missing_home_currency,
        "qualified_to_buy_stale": stale,
        "moved_this_week": moved,
        "stale_engaged_sqls": stale_sqls,
        "miscategorised_count": len(misc),
        "miscategorised": misc,
    }


# --------------------------------------------------------------------------
# Snapshots (raw + metrics) and week-on-week
# --------------------------------------------------------------------------

def find_prev_snapshot(snap_dir, run_date):
    """Path of the most recent snapshot strictly before run_date, or None."""
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
    return os.path.join(snap_dir, candidates[-1][1])


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


# --------------------------------------------------------------------------
# Report rendering
# --------------------------------------------------------------------------

def render_report(snapshot, prev):
    m = snapshot["metrics"]
    prev_m = prev["metrics"] if prev else None
    # Deal *value* deltas are only meaningful when the prior snapshot summed on
    # the same basis (USD home currency). Older snapshots summed raw mixed
    # currencies, so value deltas are suppressed for one run in that case.
    value_comparable = bool(prev_m) and prev_m.get("value_basis") == "usd_home_currency"
    date = snapshot["run_date"]
    out = []
    A = out.append

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {date}")
    A("")
    A(f"*Scope: B2B only (`customer_type = {CUSTOMER_TYPE_FILTER}` for contacts, "
      f"`customer_type__cloned_ = {CUSTOMER_TYPE_FILTER}` for companies). "
      f"Generated {snapshot['generated_at_utc']} UTC from HubSpot.*")
    A("")
    A("*Deal values are HubSpot home-currency (USD) totals "
      "(`amount_in_home_currency`); raw amount + currency are shown only as a "
      "secondary reference and never summed across currencies.*")
    A("")
    if prev:
        A(f"*Week-on-week deltas are vs the previous snapshot "
          f"(`{prev['run_date']}`).*")
    else:
        A("*No prior snapshot found — week-on-week comparison starts next run.*")
    A("")

    # ---- Alerts -----------------------------------------------------------
    stale = m["qualified_to_buy_stale"]
    stale_sqls = m["stale_engaged_sqls"]
    prev_stale_sql = len(prev_m.get("stale_engaged_sqls", [])) if prev_m else None
    misc_prev = prev_m["miscategorised_count"] if prev_m else None

    A("## \U0001F514 Alerts")
    A("")
    A(f"- **⚠️ \"Qualified to Buy\" deals idle {STALE_ACTIVITY_DAYS}+ days:** "
      f"{len(stale)}")
    A(f"- **\U0001F553 Engaged SQLs gone quiet ({STALE_SQL_ACTIVITY_DAYS}+ days idle):** "
      f"{delta_str(len(stale_sqls), prev_stale_sql)}")
    A(f"- **\U0001F3F7️ Miscategorised leads:** "
      f"{delta_str(m['miscategorised_count'], misc_prev)}")
    if m["funnel_inversion"]:
        A(f"- **⚠️ Funnel inversion:** SQL ({m['sql_count']}) > "
          f"MQL ({m['mql_count']})")
    if m.get("missing_home_currency"):
        A(f"- **\U0001F4B1 Deals missing USD (home-currency) value:** "
          f"{len(m['missing_home_currency'])} (excluded from value totals — see below)")
    A("")

    # QTB stale detail
    if stale:
        A(f"### ⚠️ {len(stale)} \"Qualified to Buy\" deal(s) with no activity "
          f"in {STALE_ACTIVITY_DAYS}+ days")
        A("")
        A("| Deal | Last activity | Days idle | USD value | Original |")
        A("|------|---------------|-----------|-----------|----------|")
        for d in stale:
            days = "no activity logged" if d["days_since_activity"] is None \
                else f"{d['days_since_activity']}d"
            last = (d["notes_last_updated"] or "")[:10] or "—"
            usd = "—" if d["amount_usd"] is None else f"${d['amount_usd']:,.0f}"
            orig = f"{_to_float(d.get('amount')):,.0f} {d.get('currency') or ''}".strip()
            A(f"| {d['dealname']} (#{d['id']}) | {last} | {days} | {usd} | {orig} |")
        A("")
    else:
        A(f"- ✅ No \"Qualified to Buy\" deals idle for {STALE_ACTIVITY_DAYS}+ days.")
        A("")

    # ---- Deals moved this week -------------------------------------------
    moved = m["moved_this_week"]
    A(f"## \U0001F4C8 What moved this week (last {MOVED_THIS_WEEK_DAYS} days)")
    A("")
    A(f"Deals with a stage change, logged activity or field update in the last "
      f"{MOVED_THIS_WEEK_DAYS} days. Grouped by current stage, highest USD value first.")
    A("")
    if moved:
        # Group by current stage, order groups by total USD desc.
        groups = {}
        for d in moved:
            groups.setdefault(d["label"], []).append(d)
        def group_val(items):
            return sum((x["amount_usd"] or 0) for x in items)
        for label in sorted(groups, key=lambda l: group_val(groups[l]), reverse=True):
            items = sorted(groups[label],
                           key=lambda x: (x["amount_usd"] is not None, x["amount_usd"] or 0),
                           reverse=True)
            A(f"### {label} ({len(items)})")
            A("")
            A("| Deal | USD value | What changed | Last activity |")
            A("|------|-----------|--------------|---------------|")
            for d in items:
                usd = "—" if d["amount_usd"] is None else f"${d['amount_usd']:,.0f}"
                la = d["last_activity"] or f"(modified {d['last_modified']})"
                A(f"| {d['dealname']} (#{d['id']}) | {usd} | {d['change']} | {la} |")
            A("")
    else:
        A("- No deals moved in the last 7 days.")
        A("")

    # ---- Stale engaged SQLs ----------------------------------------------
    A(f"## \U0001F553 Follow-up / review — engaged SQLs gone quiet: "
      f"{delta_str(len(stale_sqls), prev_stale_sql)}")
    A("")
    A(f"Sales has already made real contact on these B2B records (SQL stage, "
      f"working lead status) but activity has stalled {STALE_SQL_ACTIVITY_DAYS}+ days. "
      "This is a **follow-up / review** list for sales to action — NOT an "
      "auto-recycle. Distinct from the \"Attempted to Contact\" cases the "
      "stale-lead recycler handles automatically. Nothing here moves without a "
      "human. Sorted most-stalled first.")
    A("")
    if stale_sqls:
        by_status = {}
        for c in stale_sqls:
            by_status.setdefault(c["hs_lead_status"] or "(none)", []).append(c)
        for status in sorted(by_status):
            group = by_status[status]
            A(f"**{status}** ({len(group)})")
            A("")
            A("| Record | Type | Days idle | Owner | Associated deal (USD) |")
            A("|--------|------|-----------|-------|-----------------------|")
            for c in group:
                days = "no activity logged" if c["days_since_activity"] is None \
                    else f"{c['days_since_activity']}d"
                if c.get("deal_name"):
                    usd = c.get("deal_amount_usd")
                    deal = f"{c['deal_name']}" + (
                        f" (${usd:,.0f})" if isinstance(usd, (int, float)) else "")
                else:
                    deal = "—"
                A(f"| {c['name']} (#{c['id']}) | {c['record_type']} | {days} "
                  f"| {c.get('owner') or '—'} | {deal} |")
            A("")
    else:
        A(f"- ✅ No engaged SQLs idle for {STALE_SQL_ACTIVITY_DAYS}+ days.")
        A("")

    # ---- Miscategorised ---------------------------------------------------
    A(f"## \U0001F3F7️ Miscategorised leads: "
      f"{delta_str(m['miscategorised_count'], misc_prev)}")
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
    A("## Deals — count and value by stage (USD)")
    A("")
    if len(m["deal_currencies"]) > 1:
        A(f"> Deals span multiple currencies ({', '.join(m['deal_currencies'])}). "
          f"Totals below are HubSpot's home-currency (USD) converted values, so "
          f"they are directly comparable and summable.")
        A("")
    if not value_comparable and prev_m:
        A("> *Value week-on-week is suppressed this run: the prior snapshot summed "
          "raw mixed-currency amounts, so USD value deltas are not comparable. "
          "Count deltas are shown; value deltas resume next run.*")
        A("")
    A("| Deal stage | Deals | Total USD value | Currencies |")
    A("|------------|-------|-----------------|------------|")
    prev_deals = {d["stage"]: d for d in prev_m["deals_by_stage"]} if prev_m else {}
    tot_count = 0
    tot_usd = 0.0
    for d in m["deals_by_stage"]:
        tot_count += d["count"]
        tot_usd += d["total_usd"]
        pv = prev_deals.get(d["stage"]) if prev_m else None
        pv_count = pv["count"] if pv else None
        pv_usd = pv.get("total_usd") if (pv and value_comparable) else None
        val = money_delta_str(d["total_usd"], pv_usd)
        curr = ", ".join(d["currencies"]) or "—"
        A(f"| {d['label']} | {delta_str(d['count'], pv_count)} | {val} | {curr} |")
    A(f"| **Total** | **{tot_count}** | **${tot_usd:,.0f}** | |")
    A("")
    A("*Value per stage note: pre-deal lifecycle stages (Email Contact, Lead, "
      "MQL) are contact counts only and carry no deal value — deal value exists "
      "only from the deal pipeline stages above.*")
    A("")

    # ---- Deals missing USD value -----------------------------------------
    if m.get("missing_home_currency"):
        A("### \U0001F4B1 Deals missing a USD (home-currency) value")
        A("")
        A("These deals have an empty `amount_in_home_currency` and are **excluded** "
          "from the USD totals above (not silently fallen back to raw amount).")
        A("")
        A("| Deal | Stage | Raw amount |")
        A("|------|-------|------------|")
        for d in m["missing_home_currency"]:
            raw_amt = d.get("amount")
            orig = "(blank)" if not _has_value(raw_amt) else \
                f"{_to_float(raw_amt):,.0f} {d.get('currency') or ''}".strip()
            A(f"| {d['dealname']} (#{d['id']}) | {d['label']} | {orig} |")
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
        contacts, deals = fetch_from_hubspot(token)
        raw = build_inputs_from_live(contacts, deals)
        source = "hubspot (live REST)"

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev_path = find_prev_snapshot(snap_dir, run_date)
    prev = None
    prev_raw = None
    if prev_path:
        with open(prev_path) as f:
            prev = json.load(f)
        prev_raw = prev.get("raw", {})
        # Prior snapshots store deals under raw.deals.
        prev_raw = {"deals": prev_raw.get("deals", [])}

    metrics = compute_metrics(raw, run_date, prev_raw=prev_raw)

    generated = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    snapshot = {
        "run_date": run_date.isoformat(),
        "generated_at_utc": generated,
        "source": source,
        "customer_type_filter": CUSTOMER_TYPE_FILTER,
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
