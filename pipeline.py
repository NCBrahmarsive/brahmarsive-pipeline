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
"""

import argparse
import datetime as dt
import json
import os
import shutil
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
# Contacts use `customer_type`; companies use `customer_type__cloned_`.
CUSTOMER_TYPE_FILTER = "B2B"

# --------------------------------------------------------------------------
# Currency normalisation.
# The HubSpot account default (home) currency is USD. Deals may be created in
# other currencies (notably INR). We report and sum every deal using HubSpot's
# converted home-currency value so totals are meaningful; the original amount +
# currency code is kept only as a secondary per-deal reference. We NEVER sum the
# raw `amount` across mixed currencies. If the home-currency value is empty on a
# deal we flag it rather than falling back to the raw amount.
#
# NOTE: the portal's home-currency deal property is `amount_in_home_currency`
# (label "Amount in company currency"). The property `hs_deal_amount_in_home_currency`
# referenced in some briefs does NOT exist in this portal.
HOME_CURRENCY = "USD"
HOME_CURRENCY_PROP = "amount_in_home_currency"

# "Deals moved this week": look-back window (days) on last-modified / last-activity.
DEALS_MOVED_WINDOW_DAYS = 7

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

# Human labels for the working lead statuses (internal value -> display label).
LEAD_STATUS_LABELS = {
    "NEW": "New Lead",
    "Marketing Engaged": "Marketing Engaged",
    "MQL": "MQL",
    "Attempted to Contact": "Attempted to Contact",
    "CONNECTED": "Contacted",
    "Qualified": "Qualified",
    "IN_PROGRESS": "In Progress",
    "PROPOSAL_REQUEST": "Proposal Request",
    "UNQUALIFIED": "Unqualified",
    "FUTURE_LEAD": "Future Lead",
    "Onboarded Partner": "Onboarded Partner",
}

# =========================================================================

# Deal-alert configuration.
QUALIFIED_TO_BUY_STAGE = "appointmentscheduled"   # internal value for "Qualified to Buy"
STALE_ACTIVITY_DAYS = 14

# Contact properties we pull.
CONTACT_PROPERTIES = [
    "lifecyclestage", "hs_lead_status", "customer_type",
    "b2b_contact_lead_score_combined", "hs_lastmodifieddate",
    "notes_last_updated", "hubspot_owner_id", "hs_full_name_or_email",
]
# Deal properties we pull. notes_last_updated is HubSpot's "Last Activity Date".
# amount_in_home_currency is HubSpot's USD-converted value (see currency note).
DEAL_PROPERTIES = [
    "dealstage", "amount", "amount_in_home_currency", "deal_currency_code",
    "hs_exchange_rate", "hs_lastmodifieddate", "notes_last_updated", "dealname",
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
                # In live mode the owner is a raw id; the --input path resolves
                # it to a name via the HubSpot owners lookup.
                "owner": p.get("hubspot_owner_id"),
                "owner_id": p.get("hubspot_owner_id"),
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
        # Companies (customer_type__cloned_ = B2B) are only pulled via the
        # --input path; the live REST fallback leaves this empty.
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


def _parse_ts(value):
    """Parse a HubSpot ISO timestamp to a date, or None."""
    if not value:
        return None
    v = value.replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(v)
    except ValueError:
        return None


def _home_usd(deal):
    """USD (home-currency) value of a deal, or None if HubSpot hasn't set it.

    We never fall back to the raw `amount` here — a missing home-currency value
    is flagged instead, because raw amounts are in mixed currencies.
    """
    v = deal.get(HOME_CURRENCY_PROP)
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _days_since(ts_value, run_dt):
    """Whole days between an ISO timestamp and run_dt, or None if unparseable."""
    last = _parse_ts(ts_value)
    if last is None:
        return None
    if last.tzinfo is None:
        last = last.replace(tzinfo=dt.timezone.utc)
    return (run_dt - last).days


def _stale_sort_key(days):
    """Most-stalled first; records with no logged activity sort to the very top."""
    return float("inf") if days is None else days


def compute_metrics(raw, run_date, prev_deal_stages=None):
    prev_deal_stages = prev_deal_stages or {}
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
    # Any stage we did not know about.
    for stage, count in funnel_counts.items():
        if stage not in LIFECYCLE_ORDER:
            funnel.append({"stage": stage, "label": stage, "count": int(count)})

    mql = int(funnel_counts.get("marketingqualifiedlead", 0))
    sql = int(funnel_counts.get("salesqualifiedlead", 0))
    ratio = round(sql / mql, 2) if mql else None

    # ---- Deals by stage: count + USD (home-currency) total ----------------
    # Totals are summed in USD via amount_in_home_currency so mixed-currency
    # deals aggregate meaningfully. Deals with no home-currency value are
    # counted but flagged (their USD value is treated as unknown, not zero).
    by_stage = {}
    currencies = set()
    missing_home = []
    for d in raw["deals"]:
        stage = d.get("dealstage") or "unknown"
        entry = by_stage.setdefault(stage, {"count": 0, "usd": 0.0, "missing": 0})
        entry["count"] += 1
        usd = _home_usd(d)
        if usd is None:
            entry["missing"] += 1
            missing_home.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "stage": stage,
                "label": DEALSTAGE_LABELS.get(stage, stage),
                "currency": d.get("deal_currency_code"),
                "amount_raw": d.get("amount"),
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
            "usd_total": round(entry["usd"], 2),
            "missing_home_currency": entry["missing"],
        })
    deals_by_stage.sort(key=lambda x: (-x["usd_total"], -x["count"], x["label"]))

    # ---- Deals moved this week -------------------------------------------
    # Any deal whose last-modified OR last-activity date falls within the
    # look-back window. "What changed" is derived from THIS week's signals
    # (activity vs. field update); a stage difference vs. the prior snapshot is
    # surfaced separately and clearly labelled (the prior snapshot may be weeks
    # old, so it is context, not necessarily a change made this week).
    moved = []
    for d in raw["deals"]:
        d_mod = _days_since(d.get("hs_lastmodifieddate"), run_dt)
        d_act = _days_since(d.get("notes_last_updated"), run_dt)
        in_window = (
            (d_mod is not None and d_mod < DEALS_MOVED_WINDOW_DAYS)
            or (d_act is not None and d_act < DEALS_MOVED_WINDOW_DAYS)
        )
        if not in_window:
            continue
        stage = d.get("dealstage") or "unknown"
        act_recent = d_act is not None and d_act < DEALS_MOVED_WINDOW_DAYS
        if act_recent:
            change = "activity logged"
        else:
            change = "field/property update (no new activity logged)"
        prev_stage = prev_deal_stages.get(str(d.get("id")))
        stage_since_prev = None
        if prev_stage and prev_stage != stage:
            stage_since_prev = (
                f"{DEALSTAGE_LABELS.get(prev_stage, prev_stage)} "
                f"→ {DEALSTAGE_LABELS.get(stage, stage)}"
            )
        usd = _home_usd(d)
        moved.append({
            "id": d.get("id"),
            "dealname": d.get("dealname", ""),
            "stage": stage,
            "label": DEALSTAGE_LABELS.get(stage, stage),
            "usd_value": usd,
            "amount_raw": d.get("amount"),
            "currency": d.get("deal_currency_code"),
            "change": change,
            "stage_change_since_prev": stage_since_prev,
            "last_activity": d.get("notes_last_updated"),
            "last_modified": d.get("hs_lastmodifieddate"),
            "days_since_activity": d_act,
        })
    # Group by current stage; within a group, highest USD value first.
    moved_stage_usd = {}
    for m in moved:
        moved_stage_usd.setdefault(m["stage"], 0.0)
        moved_stage_usd[m["stage"]] += (m["usd_value"] or 0.0)
    moved.sort(key=lambda m: (
        -moved_stage_usd[m["stage"]], m["label"], -(m["usd_value"] or 0.0)
    ))

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
                "usd_value": _home_usd(d),
                "amount_raw": d.get("amount"),
                "currency": d.get("deal_currency_code"),
            })
    stale.sort(key=lambda x: _stale_sort_key(x["days_since_activity"]), reverse=True)

    # ---- SQLs engaged but gone quiet (30+ days) --------------------------
    # Covers B2B contacts AND B2B companies. A follow-up/review list for sales
    # to action manually — distinct from the auto-recycled "Attempted to
    # Contact" cases. Grouped by lead status, most-stalled first.
    stale_sqls = []
    for rec in raw.get("sql_contacts", []):
        days = _days_since(rec.get("notes_last_updated"), run_dt)
        if days is None or days >= STALE_SQL_ACTIVITY_DAYS:
            stale_sqls.append({
                "id": rec.get("id"),
                "name": rec.get("name", ""),
                "record_type": "contact",
                "hs_lead_status": rec.get("hs_lead_status"),
                "notes_last_updated": rec.get("notes_last_updated"),
                "days_since_activity": days,
                "owner": rec.get("owner"),
                "associated_deal": rec.get("associated_deal"),
            })
    for rec in raw.get("sql_companies", []):
        # Threshold uses the logged last-activity date, same rule as contacts:
        # no logged activity (None) qualifies (nothing in 30+ days — surface it
        # flagged). Last-modified is kept only as display context, never to
        # disqualify a record that has no activity of its own.
        act = rec.get("notes_last_updated")
        days = _days_since(act, run_dt)
        if days is None or days >= STALE_SQL_ACTIVITY_DAYS:
            stale_sqls.append({
                "id": rec.get("id"),
                "name": rec.get("name", ""),
                "record_type": "company",
                "hs_lead_status": rec.get("hs_lead_status"),
                "notes_last_updated": act,
                "days_since_activity": days,
                "last_modified": rec.get("hs_lastmodifieddate"),
                "owner": rec.get("owner"),
                "associated_deal": rec.get("associated_deal"),
            })
    stale_sqls.sort(key=lambda x: _stale_sort_key(x["days_since_activity"]), reverse=True)

    misc = raw.get("miscategorised", [])
    return {
        "funnel": funnel,
        "mql_count": mql,
        "sql_count": sql,
        "sql_to_mql_ratio": ratio,
        "funnel_inversion": sql > mql,
        "deals_by_stage": deals_by_stage,
        "deal_currencies": sorted(currencies),
        "deals_missing_home_currency": missing_home,
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

def _usd(value):
    """Format a USD home-currency value, or a flag when it's unknown."""
    if value is None:
        return "⚠️ no USD value"
    return f"${value:,.0f}"


def _orig_amount(amount_raw, currency):
    """Secondary reference: original amount + currency code (e.g. '₹300,000 INR')."""
    f = _to_float(amount_raw)
    cur = currency or ""
    if amount_raw in (None, ""):
        return "—"
    return f"{f:,.0f} {cur}".strip()


def render_report(snapshot, prev):
    m = snapshot["metrics"]
    prev_m = prev["metrics"] if prev else None
    date = snapshot["run_date"]
    out = []
    A = out.append

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {date}")
    A("")
    A(f"*Scope: B2B only (contacts `customer_type = {CUSTOMER_TYPE_FILTER}`, "
      f"companies `customer_type__cloned_ = {CUSTOMER_TYPE_FILTER}`). "
      f"Generated {snapshot['generated_at_utc']} UTC from HubSpot. "
      f"Deal values are HubSpot's home-currency ({HOME_CURRENCY}) conversion.*")
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

    # Alert 1: Qualified-to-Buy deals idle 14+ days.
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
              f"| {_usd(d['usd_value'])} | {_orig_amount(d['amount_raw'], d['currency'])} |")
        A("")
    else:
        A(f"- ✅ No \"Qualified to Buy\" deals idle for {STALE_ACTIVITY_DAYS}+ days.")
        A("")

    # Alert 2: engaged-SQL stalled count (detail table lives in its own section).
    stale_sqls = m["stale_engaged_sqls"]
    prev_stale = len(prev_m["stale_engaged_sqls"]) if (prev_m and "stale_engaged_sqls" in prev_m) else None
    A(f"### 🕓 SQLs engaged but gone quiet: "
      f"{delta_str(len(stale_sqls), prev_stale)}")
    A("")
    A(f"B2B contacts + companies at Sales Qualified Lead that sales has actively "
      f"engaged but which have been idle {STALE_SQL_ACTIVITY_DAYS}+ days. "
      f"Follow-up/review list — see the dedicated section below. "
      + ("" if prev_stale is not None
         else "_Week-on-week for this metric starts next run (not tracked in the prior snapshot)._"))
    A("")

    # Alert 3: miscategorised-lead count.
    misc_prev = prev_m.get("miscategorised_count") if prev_m else None
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
            A(f"| {c['id']} | {c.get('name','')} | {c.get('hs_lead_status','')} "
              f"| {LIFECYCLE_LABELS.get(c.get('lifecyclestage',''), c.get('lifecyclestage',''))} |")
    else:
        A("- None this run.")
    A("")

    # Alert 4: flagged deals with no USD (home-currency) value.
    missing = m.get("deals_missing_home_currency", [])
    if missing:
        A(f"### ⚠️ {len(missing)} deal(s) missing a {HOME_CURRENCY} "
          f"(home-currency) value")
        A("")
        A("These are excluded from USD stage totals rather than counted with "
          "their raw amount. Set the deal amount so HubSpot can convert it.")
        A("")
        A("| Deal | Stage | Original amount |")
        A("|------|-------|-----------------|")
        for d in missing:
            A(f"| {d['dealname']} (#{d['id']}) | {d['label']} "
              f"| {_orig_amount(d['amount_raw'], d['currency'])} |")
        A("")

    # Funnel-inversion flag.
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

    # ---- Deals by stage (USD) --------------------------------------------
    A(f"## Deals — count and value by stage ({HOME_CURRENCY})")
    A("")
    if len(m["deal_currencies"]) > 1:
        A(f"> **Currency note:** deals exist in mixed currencies "
          f"({', '.join(m['deal_currencies'])}). Totals below are summed in "
          f"{HOME_CURRENCY} using HubSpot's `{HOME_CURRENCY_PROP}` conversion, so "
          f"the grand total is meaningful. Raw `amount` is never summed across "
          f"currencies.")
        A("")
    A(f"| Deal stage | Deals | Total value ({HOME_CURRENCY}) |")
    A("|------------|-------|--------------------|")
    prev_deals = {d["stage"]: d for d in prev_m["deals_by_stage"]} if prev_m else {}
    # Prior snapshot summed raw mixed-currency amounts, so per-stage value
    # deltas aren't comparable this week; count deltas remain valid.
    prev_value_comparable = bool(prev_m) and all(
        "usd_total" in d for d in prev_m.get("deals_by_stage", [])
    )
    tot_count = 0
    tot_usd = 0.0
    for d in m["deals_by_stage"]:
        tot_count += d["count"]
        tot_usd += d["usd_total"]
        pv = prev_deals.get(d["stage"]) if prev_m else None
        pv_count = pv["count"] if pv else None
        flag = f" ⚠️×{d['missing_home_currency']}" if d["missing_home_currency"] else ""
        if prev_value_comparable:
            pv_usd = pv["usd_total"] if pv else None
            val_cell = money_delta_str(d["usd_total"], pv_usd)
        else:
            val_cell = f"{d['usd_total']:,.0f}"
        A(f"| {d['label']} | {delta_str(d['count'], pv_count)} | ${val_cell}{flag} |")
    A(f"| **Total** | **{tot_count}** | **${tot_usd:,.0f}** |")
    A("")
    if not prev_value_comparable and prev_m:
        A("*Per-stage value week-on-week starts next run: the prior snapshot "
          "summed raw mixed-currency amounts, which aren't comparable to this "
          f"run's {HOME_CURRENCY}-normalised totals. Count deltas are shown.*")
        A("")
    A("*Value per stage note: pre-deal lifecycle stages (Email Contact, Lead, "
      "MQL) are contact counts only and carry no deal value — deal value exists "
      "only from the deal pipeline stages above. ⚠️×N = N deal(s) in that stage "
      f"have no {HOME_CURRENCY} value and are excluded from the total (see alerts).*")
    A("")

    # ---- What moved this week --------------------------------------------
    moved = m.get("deals_moved_this_week", [])
    A(f"## What moved this week — deals active in the last {DEALS_MOVED_WINDOW_DAYS} days")
    A("")
    A("Deals whose last-modified or last-activity date falls within the last "
      f"{DEALS_MOVED_WINDOW_DAYS} days, grouped by current stage (highest value "
      "first). The quick answer to \"which deals are active and how did the "
      "pipeline shift this week?\" for the Tuesday leadership call.")
    A("")
    if moved:
        # Preserve the group ordering computed in compute_metrics.
        seen = []
        for mv in moved:
            if mv["stage"] not in seen:
                seen.append(mv["stage"])
        for stage in seen:
            group = [mv for mv in moved if mv["stage"] == stage]
            label = group[0]["label"]
            A(f"### {label} ({len(group)})")
            A("")
            A("| Deal | USD value | Original | What changed | Last activity |")
            A("|------|-----------|----------|--------------|---------------|")
            for mv in group:
                changed = mv["change"]
                if mv["stage_change_since_prev"]:
                    changed += f" · stage vs last snapshot: {mv['stage_change_since_prev']}"
                last = (mv["last_activity"] or "—")[:10]
                A(f"| {mv['dealname']} (#{mv['id']}) | {_usd(mv['usd_value'])} "
                  f"| {_orig_amount(mv['amount_raw'], mv['currency'])} | {changed} | {last} |")
            A("")
    else:
        A(f"- No deals modified or active in the last {DEALS_MOVED_WINDOW_DAYS} days.")
        A("")

    # ---- Engaged SQLs gone quiet (follow-up / review) --------------------
    A(f"## Engaged SQLs gone quiet — follow-up / review ({STALE_SQL_ACTIVITY_DAYS}+ days idle)")
    A("")
    A(f"**{delta_str(len(stale_sqls), prev_stale)}** B2B records at Sales "
      "Qualified Lead where sales has already made real contact "
      "(Contacted / Qualified / In Progress / Proposal Request) but activity has "
      f"since stalled for {STALE_SQL_ACTIVITY_DAYS}+ days. This is a "
      "**follow-up / review** list — live relationships for sales to action. "
      "**Nothing here is moved automatically**, and it is distinct from the "
      "auto-recycled \"Attempted to Contact\" cases the stale-lead recycler "
      "handles. Sorted most-stalled first.")
    A("")
    if stale_sqls:
        by_status = {}
        for c in stale_sqls:
            by_status.setdefault(c["hs_lead_status"], []).append(c)
        for status in sorted(by_status, key=lambda s: -len(by_status[s])):
            group = by_status[status]
            label = LEAD_STATUS_LABELS.get(status, status)
            A(f"### {label} ({len(group)})")
            A("")
            A("| Record | Type | Days idle | Owner | Associated deal (USD) |")
            A("|--------|------|-----------|-------|-----------------------|")
            for c in group:
                if c["days_since_activity"] is None:
                    days = "no activity logged"
                    if c.get("last_modified"):
                        days += f" (modified {c['last_modified'][:10]})"
                else:
                    days = f"{c['days_since_activity']}d"
                deal = c.get("associated_deal")
                if deal:
                    deal_cell = f"{deal.get('name','')} ({_usd(deal.get('usd_value'))})"
                else:
                    deal_cell = "—"
                A(f"| {c['name']} (#{c['id']}) | {c['record_type']} | {days} "
                  f"| {c.get('owner') or '—'} | {deal_cell} |")
            A("")
    else:
        A(f"- ✅ No engaged SQLs idle for {STALE_SQL_ACTIVITY_DAYS}+ days.")
        A("")

    A("---")
    A(f"*Snapshot: `snapshots/{date}.json` · Report: `reports/{date}.md`*")
    A("")
    return "\n".join(out)


# --------------------------------------------------------------------------
# Word (.docx) rendering
# --------------------------------------------------------------------------

def render_docx(md_path, docx_path, reference_doc):
    """Convert the markdown report to .docx via pandoc.

    Uses the committed reference doc for styling when present. Returns True on
    success; prints a warning and returns False if pandoc is unavailable or the
    conversion fails (the .md/.json outputs are still valid on their own).
    """
    if shutil.which("pandoc") is None:
        print("WARNING: pandoc not found — skipping .docx render. "
              "Install pandoc to produce the Word version.", file=sys.stderr)
        return False
    cmd = ["pandoc", md_path, "-o", docx_path]
    if reference_doc and os.path.isfile(reference_doc):
        cmd.append(f"--reference-doc={reference_doc}")
    else:
        print(f"WARNING: reference doc {reference_doc!r} not found — "
              "rendering .docx with pandoc defaults.", file=sys.stderr)
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"WARNING: pandoc failed ({e.returncode}) — skipping .docx.\n"
              f"{e.stderr}", file=sys.stderr)
        return False
    return True


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly pipeline report")
    ap.add_argument("--input", help="Pre-pulled raw JSON (skips live fetch)")
    ap.add_argument("--date", help="Run date YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
    ap.add_argument("--docx", dest="docx", action="store_true", default=True,
                    help="Also render a Word .docx via pandoc (default: on).")
    ap.add_argument("--no-docx", dest="docx", action="store_false",
                    help="Skip the Word .docx render.")
    ap.add_argument("--reference-doc",
                    default=os.path.join("templates", "pipeline_report_template.docx"),
                    help="pandoc --reference-doc used to style the .docx.")
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

    # Load the most recent previous snapshot first, so we can (a) show
    # week-on-week movement and (b) detect deal stage changes vs. last run.
    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)
    prev = load_prev_snapshot(snap_dir, run_date)

    prev_deal_stages = {}
    if prev:
        for d in prev.get("raw", {}).get("deals", []):
            if d.get("id") is not None:
                prev_deal_stages[str(d["id"])] = d.get("dealstage")

    metrics = compute_metrics(raw, run_date, prev_deal_stages=prev_deal_stages)

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

    # Word (.docx) render via pandoc, styled by the committed reference doc.
    if args.docx:
        docx_path = os.path.join(rep_dir, f"{run_date.isoformat()}.docx")
        written = render_docx(rep_path, docx_path,
                              os.path.join(args.outdir, args.reference_doc))
        if written:
            print(f"Wrote {docx_path}")

    if prev:
        print(f"Compared against previous snapshot: {prev['run_date']}")
    else:
        print("No previous snapshot found (first run).")


if __name__ == "__main__":
    main()
