#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report.

What it does
------------
Pulls contacts, companies and deals from HubSpot, filters the funnel to B2B,
and writes per run:

  snapshots/YYYY-MM-DD.json  -- the pulled data + computed metrics (machine)
  reports/YYYY-MM-DD.md      -- readable summary with funnel, ratios, deal
                                values (USD), alerts, "deals moved this week",
                                the stalled-engaged-SQL review list and the
                                miscategorised list
  reports/YYYY-MM-DD.docx    -- Word version (generated separately via pandoc)

Each run loads the most recent previous snapshot from snapshots/ and shows the
week-on-week movement for every metric.

Two data sources
----------------
1. Live (default): fetch straight from HubSpot's REST API using the
   HUBSPOT_TOKEN environment variable (a HubSpot Service key with
   crm.objects.contacts.read + crm.objects.companies.read +
   crm.objects.deals.read).

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

# Lead-status internal value -> display label.
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


def lead_status_label(value):
    return LEAD_STATUS_LABELS.get(value, value or "—")


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
# the separate HubSpot stale-lead recycler workflow). These are records
# (B2B contacts AND B2B companies) at lifecyclestage=salesqualifiedlead where
# sales has already made real contact/progress (one of the engaged statuses
# below), but activity has since stalled 30+ days. This is a follow-up/review
# list for sales to action -- nothing here is moved automatically. Records
# with no activity date logged at all are NOT surfaced here (they were never
# worked, a different data-hygiene issue). Threshold is tunable below.
STALE_SQL_LEAD_STATUSES = {"CONNECTED", "Qualified", "IN_PROGRESS", "PROPOSAL_REQUEST"}
STALE_SQL_ACTIVITY_DAYS = 30

# =========================================================================

# Deal-alert configuration.
QUALIFIED_TO_BUY_STAGE = "appointmentscheduled"   # internal value for "Qualified to Buy"
STALE_ACTIVITY_DAYS = 14

# "Deals moved this week" lookback window.
MOVED_WINDOW_DAYS = 7

# Contact properties we pull.
CONTACT_PROPERTIES = [
    "lifecyclestage", "hs_lead_status", "customer_type",
    "b2b_contact_lead_score_combined", "hs_lastmodifieddate",
    "notes_last_updated", "hubspot_owner_id", "hs_full_name_or_email",
]
# Deal properties we pull. notes_last_updated is HubSpot's "Last Activity Date".
# amount_in_home_currency is HubSpot's USD-converted deal value (this account's
# home currency is USD). The brief calls this hs_deal_amount_in_home_currency;
# in this portal the property's internal name is amount_in_home_currency.
HOME_CURRENCY_PROPERTY = "amount_in_home_currency"
DEAL_PROPERTIES = [
    "dealstage", "amount", HOME_CURRENCY_PROPERTY, "deal_currency_code",
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

    NOTE: the live path does not resolve hubspot_owner_id -> owner name or pull
    B2B companies / associated deals. The --input (MCP-pulled) path is the
    supported production path in this environment and carries those enrichments.
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
                "notes_last_updated": p.get("notes_last_updated"),
                "owner": p.get("hubspot_owner_id"),
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
    """USD (home-currency) value of a deal, or None if empty/missing.

    We never fall back to the raw `amount` for a missing home-currency value --
    a missing conversion is flagged instead (see missing_home_currency).
    """
    v = deal.get(HOME_CURRENCY_PROPERTY)
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_ts(value):
    """Parse a HubSpot ISO timestamp to a date, or None."""
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

    # Previous snapshot's per-deal stage (for "what changed" detection).
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
    for stage, count in funnel_counts.items():
        if stage not in LIFECYCLE_ORDER:
            funnel.append({"stage": stage, "label": stage, "count": int(count)})

    mql = int(funnel_counts.get("marketingqualifiedlead", 0))
    sql = int(funnel_counts.get("salesqualifiedlead", 0))
    ratio = round(sql / mql, 2) if mql else None

    # Deals by stage: count + summed USD (home currency). Raw amounts are NOT
    # summed across mixed currencies -- USD is the only meaningful total.
    by_stage = {}
    currencies = set()
    missing_home_currency = []
    for d in raw["deals"]:
        stage = d.get("dealstage") or "unknown"
        entry = by_stage.setdefault(stage, {"count": 0, "usd": 0.0})
        entry["count"] += 1
        usd = _home_usd(d)
        if usd is not None:
            entry["usd"] += usd
        else:
            missing_home_currency.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "dealstage": stage,
                "stage_label": DEALSTAGE_LABELS.get(stage, stage),
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
        })
    deals_by_stage.sort(key=lambda x: (-x["total_usd"], -x["count"], x["label"]))

    # Alert: Qualified-to-Buy deals with no activity in 14+ days.
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
                "usd": _home_usd(d),
                "amount": d.get("amount"),
                "currency": d.get("deal_currency_code"),
            })
    stale.sort(key=lambda x: (x["days_since_activity"] is not None,
                              x["days_since_activity"] or 0), reverse=True)

    # Stalled engaged SQLs: B2B contacts AND B2B companies at SQL with an
    # engaged lead status, gone quiet 30+ days. Grouped later by lead status.
    stale_sqls = []
    engaged = list(raw.get("sql_contacts", [])) + list(raw.get("sql_companies", []))
    for c in engaged:
        if c.get("hs_lead_status") not in STALE_SQL_LEAD_STATUSES:
            continue
        days = _days_since(c.get("notes_last_updated"), run_dt)
        if days is not None and days >= STALE_SQL_ACTIVITY_DAYS:
            stale_sqls.append({
                "id": c.get("id"),
                "name": c.get("name", ""),
                "hs_lead_status": c.get("hs_lead_status"),
                "record_type": c.get("record_type", "contact"),
                "owner": c.get("owner"),
                "notes_last_updated": c.get("notes_last_updated"),
                "days_since_activity": days,
                "deal": c.get("deal"),
            })
    stale_sqls.sort(key=lambda x: x["days_since_activity"], reverse=True)

    # Deals moved this week: modified OR last-activity within the window.
    cutoff = run_dt - dt.timedelta(days=MOVED_WINDOW_DAYS)
    moved = []
    for d in raw["deals"]:
        did = str(d.get("id"))
        last_mod = _parse_ts(d.get("hs_lastmodifieddate"))
        last_act = _parse_ts(d.get("notes_last_updated"))
        in_window = False
        for ts in (last_mod, last_act):
            if ts is None:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=dt.timezone.utc)
            if ts >= cutoff:
                in_window = True
        if not in_window:
            continue
        stage = d.get("dealstage") or "unknown"
        # What changed?
        act_days = _days_since(d.get("notes_last_updated"), run_dt)
        if prev and did not in prev_deal_stage:
            changed = "New deal"
        elif prev and prev_deal_stage.get(did) not in (None, stage):
            frm = DEALSTAGE_LABELS.get(prev_deal_stage.get(did), prev_deal_stage.get(did))
            changed = f"Stage change: {frm} → {DEALSTAGE_LABELS.get(stage, stage)}"
        elif act_days is not None and act_days <= MOVED_WINDOW_DAYS:
            changed = "Activity logged"
        else:
            changed = "Field update"
        moved.append({
            "id": d.get("id"),
            "dealname": d.get("dealname", ""),
            "dealstage": stage,
            "stage_label": DEALSTAGE_LABELS.get(stage, stage),
            "usd": _home_usd(d),
            "amount": d.get("amount"),
            "currency": d.get("deal_currency_code"),
            "notes_last_updated": d.get("notes_last_updated"),
            "hs_lastmodifieddate": d.get("hs_lastmodifieddate"),
            "changed": changed,
        })

    misc = raw.get("miscategorised", [])
    return {
        "methodology_version": 2,  # v2 = USD home-currency deal values
        "funnel": funnel,
        "mql_count": mql,
        "sql_count": sql,
        "sql_to_mql_ratio": ratio,
        "funnel_inversion": sql > mql,
        "deals_by_stage": deals_by_stage,
        "deal_currencies": sorted(currencies),
        "missing_home_currency": missing_home_currency,
        "qualified_to_buy_stale": stale,
        "stale_engaged_sqls": stale_sqls,
        "deals_moved_this_week": moved,
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


def usd_str(value):
    return "—" if value is None else f"${value:,.0f}"


def raw_ref(amount, currency):
    """Secondary reference: raw amount + currency, shown only when non-USD."""
    if not currency or currency == "USD":
        return ""
    if amount in (None, ""):
        return ""
    try:
        return f"{float(amount):,.0f} {currency}"
    except (TypeError, ValueError):
        return f"{amount} {currency}"


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
    A(f"*Scope: B2B only (contacts `customer_type = {CUSTOMER_TYPE_FILTER}`, "
      f"companies `customer_type__cloned_ = {CUSTOMER_TYPE_FILTER}`). "
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

    # Headline counts row.
    stale_sqls = m["stale_engaged_sqls"]
    # Older snapshots may predate a metric; fall back to None so no misleading
    # week-on-week delta is shown against a missing baseline.
    prev_stale = (len(prev_m["stale_engaged_sqls"])
                  if prev_m and "stale_engaged_sqls" in prev_m else None)
    misc_prev = prev_m.get("miscategorised_count") if prev_m else None
    qtb = m["qualified_to_buy_stale"]
    A(f"- **\"Qualified to Buy\" deals idle {STALE_ACTIVITY_DAYS}+ days:** "
      f"{len(qtb)}")
    A(f"- **Miscategorised leads:** {delta_str(m['miscategorised_count'], misc_prev)}")
    A(f"- **SQLs engaged but gone quiet ({STALE_SQL_ACTIVITY_DAYS}+ days):** "
      f"{delta_str(len(stale_sqls), prev_stale)}")
    if m["missing_home_currency"]:
        A(f"- **⚠️ Deals with no USD conversion (flagged, not summed):** "
          f"{len(m['missing_home_currency'])}")
    if m["funnel_inversion"]:
        A(f"- **⚠️ Funnel inversion:** SQL ({m['sql_count']}) > MQL "
          f"({m['mql_count']})")
    A("")

    # QTB stale detail.
    if qtb:
        A(f"### ⚠️ {len(qtb)} \"Qualified to Buy\" deal(s) with no activity "
          f"in {STALE_ACTIVITY_DAYS}+ days")
        A("")
        A("| Deal | USD value | Last activity | Days idle | Raw (if non-USD) |")
        A("|------|-----------|---------------|-----------|------------------|")
        for d in qtb:
            days = "no activity logged" if d["days_since_activity"] is None \
                else f"{d['days_since_activity']}d"
            last = (d["notes_last_updated"] or "—")[:10]
            A(f"| {d['dealname']} (#{d['id']}) | {usd_str(d['usd'])} | {last} "
              f"| {days} | {raw_ref(d['amount'], d['currency']) or '—'} |")
        A("")
    else:
        A(f"- ✅ No \"Qualified to Buy\" deals idle for {STALE_ACTIVITY_DAYS}+ days.")
        A("")

    # ---- Deals moved this week -------------------------------------------
    moved = m["deals_moved_this_week"]
    A(f"## 📈 Deals moved this week (last {MOVED_WINDOW_DAYS} days)")
    A("")
    A("What's active and how the pipeline shifted this week — deals modified or "
      "with logged activity in the window. Grouped by current stage, highest "
      "USD value first.")
    A("")
    if not moved:
        A("- No deal movement in the window.")
        A("")
    else:
        groups = {}
        for d in moved:
            groups.setdefault(d["stage_label"], []).append(d)
        group_order = sorted(
            groups.items(),
            key=lambda kv: sum((x["usd"] or 0) for x in kv[1]),
            reverse=True,
        )
        for label, items in group_order:
            items.sort(key=lambda x: (x["usd"] or 0), reverse=True)
            gtot = sum((x["usd"] or 0) for x in items)
            A(f"**{label}** — {len(items)} deal(s), ${gtot:,.0f} USD")
            A("")
            A("| Deal | USD value | What changed | Last activity |")
            A("|------|-----------|--------------|---------------|")
            for d in items:
                last = (d["notes_last_updated"] or "—")[:10]
                ref = raw_ref(d["amount"], d["currency"])
                val = usd_str(d["usd"]) + (f" ({ref})" if ref else "")
                A(f"| {d['dealname']} (#{d['id']}) | {val} | {d['changed']} "
                  f"| {last} |")
            A("")

    # ---- Stalled engaged SQLs (follow-up / review) -----------------------
    A(f"## 🕓 SQLs engaged but gone quiet — follow-up / review")
    A("")
    A(f"Sales has already made real contact on these B2B records (contacts and "
      f"companies at Sales Qualified Lead with an engaged lead status) but "
      f"activity has stalled {STALE_SQL_ACTIVITY_DAYS}+ days. This is a "
      f"follow-up/review list, **NOT an auto-recycle** — distinct from the "
      f"\"Attempted to Contact\" cases the stale-lead recycler handles "
      f"automatically. Nothing here moves without a human. Sorted most-stalled "
      f"first.")
    A("")
    A(f"Count: **{delta_str(len(stale_sqls), prev_stale)}**")
    A("")
    if stale_sqls:
        by_status = {}
        for c in stale_sqls:
            by_status.setdefault(c["hs_lead_status"], []).append(c)
        for status in sorted(by_status, key=lambda s: -len(by_status[s])):
            group = by_status[status]
            A(f"**{lead_status_label(status)}** ({len(group)})")
            A("")
            A("| Record | Type | Days idle | Owner | Associated deal (USD) |")
            A("|--------|------|-----------|-------|-----------------------|")
            for c in group:
                deal = c.get("deal")
                if deal:
                    dval = f"{deal.get('dealname','')} ({usd_str(deal.get('usd'))})"
                else:
                    dval = "—"
                A(f"| {c['name']} (#{c['id']}) | {c['record_type']} "
                  f"| {c['days_since_activity']}d | {c.get('owner') or '—'} "
                  f"| {dval} |")
            A("")
    else:
        A(f"- ✅ No engaged SQLs idle for {STALE_SQL_ACTIVITY_DAYS}+ days.")
        A("")

    # ---- Miscategorised leads --------------------------------------------
    A(f"## 🏷️ Miscategorised leads: "
      f"{delta_str(m['miscategorised_count'], misc_prev)}")
    A("")
    A("Sales is working these contacts (lead status) but their lifecycle stage "
      "is still only Lead / MQL — they should be Sales Qualified Lead or beyond.")
    A("")
    if m["miscategorised"]:
        A("| Contact ID | Name | Lead status | Lifecycle stage | Owner |")
        A("|------------|------|-------------|-----------------|-------|")
        for c in m["miscategorised"]:
            A(f"| {c['id']} | {c.get('name','')} "
              f"| {lead_status_label(c.get('hs_lead_status'))} "
              f"| {LIFECYCLE_LABELS.get(c.get('lifecyclestage'), c.get('lifecyclestage',''))} "
              f"| {c.get('owner') or '—'} |")
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
    prev_funnel = {f["stage"]: f["count"] for f in prev_m.get("funnel", [])} if prev_m else {}
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
    pmql = prev_m.get("mql_count") if prev_m else None
    psql = prev_m.get("sql_count") if prev_m else None
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
    A(f"> Values use HubSpot's USD home-currency conversion "
      f"(`{HOME_CURRENCY_PROPERTY}`). Raw amounts are never summed across mixed "
      f"currencies ({', '.join(m['deal_currencies']) or 'n/a'}). Deals with no "
      f"USD conversion are flagged below, not summed.")
    A("")
    # Was the previous snapshot on the same (USD) methodology?
    prev_usd_method = bool(prev_m) and any(
        "total_usd" in d for d in prev_m.get("deals_by_stage", []))
    if prev_m and not prev_usd_method:
        A("> ℹ️ Deal-value methodology changed this run (raw mixed-currency → "
          "USD home currency). Deal **value** week-on-week deltas are therefore "
          "omitted this run and resume next run on a like-for-like basis; deal "
          "**count** deltas are shown.")
        A("")
    A("| Deal stage | Deals | Total value (USD) |")
    A("|------------|-------|-------------------|")
    prev_deals = {d["stage"]: d for d in prev_m.get("deals_by_stage", [])} if prev_m else {}
    tot_count = 0
    tot_usd = 0.0
    for d in m["deals_by_stage"]:
        tot_count += d["count"]
        tot_usd += d["total_usd"]
        pv = prev_deals.get(d["stage"]) if prev_m else None
        pv_count = pv["count"] if pv else None
        pv_usd = pv.get("total_usd") if pv else None
        A(f"| {d['label']} | {delta_str(d['count'], pv_count)} "
          f"| {money_delta_str(d['total_usd'], pv_usd)} |")
    A(f"| **Total** | **{tot_count}** | **${tot_usd:,.0f}** |")
    A("")
    A("*Value per stage note: pre-deal lifecycle stages (Email Contact, Lead, "
      "MQL) are contact counts only and carry no deal value — deal value exists "
      "only from the deal pipeline stages above.*")
    A("")

    if m["missing_home_currency"]:
        A(f"### ⚠️ Deals missing USD conversion ({len(m['missing_home_currency'])})")
        A("")
        A("These deals have an empty `" + HOME_CURRENCY_PROPERTY + "`. They are "
          "flagged (not summed, and not back-filled from the raw amount). Fix "
          "the currency/exchange rate in HubSpot so they count toward totals.")
        A("")
        A("| Deal | Stage | Raw amount |")
        A("|------|-------|------------|")
        for d in m["missing_home_currency"]:
            amt = d.get("amount")
            amt_s = "(blank)" if amt in (None, "") else \
                f"{amt} {d.get('currency') or ''}".strip()
            A(f"| {d['dealname']} (#{d['id']}) | {d['stage_label']} | {amt_s} |")
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

    metrics = compute_metrics(raw, run_date, prev)

    generated = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    snapshot = {
        "run_date": run_date.isoformat(),
        "generated_at_utc": generated,
        "source": source,
        "customer_type_filter": CUSTOMER_TYPE_FILTER,
        "raw": {
            "contacts_b2b_total": raw.get("contacts_b2b_total"),
            "funnel_counts_internal": raw["funnel_counts"],
            "owners": raw.get("owners", {}),
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
