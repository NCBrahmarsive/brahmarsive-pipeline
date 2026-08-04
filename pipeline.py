#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report.

What it does
------------
Pulls contacts, deals and companies from HubSpot, filters the funnel to B2B
contacts, and writes per run:

  snapshots/YYYY-MM-DD.json  -- the pulled data + computed metrics (machine)
  reports/YYYY-MM-DD.md      -- readable summary with funnel, ratios, deal
                                values (USD-normalised), alerts, the
                                "deals moved this week" view, the stalled-SQL
                                follow-up list, and the miscategorised list

Each run loads the most recent previous snapshot from snapshots/ and shows the
week-on-week movement for every metric.

Two data sources
----------------
1. Live (default): fetch straight from HubSpot's REST API using the
   HUBSPOT_TOKEN environment variable.

       python3 pipeline.py

2. Pre-pulled (--input FILE): consume data already pulled from HubSpot and
   stored as JSON. Used when direct egress to api.hubapi.com is not permitted
   from the run environment, so the data is fetched via the HubSpot MCP tools
   and handed to the same computation code here.

       python3 pipeline.py --input raw_input.json

Currency
--------
The HubSpot account default (home) currency is USD, but deals are created in
several currencies (notably INR). All deal totals are reported in USD using
HubSpot's converted amount. In this portal that property is exposed as
`amount_in_home_currency` (label "Amount in company currency"); the name
`hs_deal_amount_in_home_currency` from the brief does not exist here. Raw
`amount` + `deal_currency_code` are kept only as a secondary reference and are
never summed across currencies. Deals with an empty converted amount are
flagged, not back-filled from the raw amount.
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
# (Companies carry the same flag on customer_type__cloned_.)
CUSTOMER_TYPE_FILTER = "B2B"

# All deal money is reported on this basis. If a prior snapshot used a
# different basis, week-on-week deal-value deltas are suppressed (with a note)
# because they would not be comparable.
DEAL_VALUE_BASIS = "USD (amount_in_home_currency)"

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
# the separate HubSpot stale-lead recycler workflow). These are SQL contacts
# AND companies where sales has already made real contact/progress
# (hs_lead_status one of the engaged statuses below), but activity has since
# stalled. This is a follow-up/review list for sales to action — nothing here
# is moved automatically. 30-day threshold is tunable below.
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
# amount_in_home_currency is HubSpot's USD-converted amount.
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
        d = dt.datetime.fromisoformat(v)
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d


def _days_since(value, run_dt):
    d = _parse_ts(value)
    if d is None:
        return None
    return (run_dt - d).days


def dealstage_label(stage):
    return DEALSTAGE_LABELS.get(stage, stage or "unknown")


def _last_activity(deal):
    """Best available 'last activity' timestamp string for a deal."""
    return deal.get("notes_last_updated") or deal.get("hs_lastmodifieddate")


# --------------------------------------------------------------------------
# Computation
# --------------------------------------------------------------------------

def compute_metrics(raw, run_date, prev_snapshot=None):
    funnel_counts = raw["funnel_counts"]
    run_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)

    # Prior deal stages, keyed by deal id, for stage-change detection.
    prev_deal_stage = {}
    if prev_snapshot:
        for d in prev_snapshot.get("raw", {}).get("deals", []):
            prev_deal_stage[str(d.get("id"))] = d.get("dealstage")

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

    # ---- Deals by stage: count + USD total + raw reference + flags -------
    by_stage = {}
    missing_home_currency = []
    all_currencies = set()
    for d in raw["deals"]:
        stage = d.get("dealstage") or "unknown"
        usd = _to_float(d.get("amount_in_home_currency"))
        cur = d.get("deal_currency_code")
        if cur:
            all_currencies.add(cur)
        entry = by_stage.setdefault(stage, {
            "count": 0, "usd_total": 0.0, "usd_missing": 0, "raw_by_ccy": {},
        })
        entry["count"] += 1
        if usd is None:
            entry["usd_missing"] += 1
            missing_home_currency.append({
                "id": d.get("id"), "dealname": d.get("dealname", ""),
                "stage": stage, "label": dealstage_label(stage),
                "raw_amount": d.get("amount"), "currency": cur,
            })
        else:
            entry["usd_total"] += usd
        raw_amt = _to_float(d.get("amount"))
        if raw_amt:
            entry["raw_by_ccy"][cur] = entry["raw_by_ccy"].get(cur, 0.0) + raw_amt

    deals_by_stage = []
    for stage, entry in by_stage.items():
        deals_by_stage.append({
            "stage": stage,
            "label": dealstage_label(stage),
            "count": entry["count"],
            "usd_total": round(entry["usd_total"], 2),
            "usd_missing": entry["usd_missing"],
            "raw_by_ccy": {k: round(v, 2) for k, v in entry["raw_by_ccy"].items()},
        })
    deals_by_stage.sort(key=lambda x: (-x["usd_total"], -x["count"], x["label"]))

    # ---- Alert: Qualified-to-Buy deals with no activity in 14+ days ------
    stale = []
    for d in raw["deals"]:
        if d.get("dealstage") != QUALIFIED_TO_BUY_STAGE:
            continue
        days = _days_since(_last_activity(d), run_dt)
        if days is None or days >= STALE_ACTIVITY_DAYS:
            stale.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "last_activity": _last_activity(d),
                "days_since_activity": days,
                "usd": _to_float(d.get("amount_in_home_currency")),
                "raw_amount": d.get("amount"),
                "currency": d.get("deal_currency_code"),
            })
    stale.sort(key=lambda x: (x["days_since_activity"] is not None,
                              x["days_since_activity"] or 0), reverse=True)

    # ---- Deals moved this week (last MOVED_WINDOW_DAYS days) --------------
    moved = []
    for d in raw["deals"]:
        did = str(d.get("id"))
        mod_days = _days_since(d.get("hs_lastmodifieddate"), run_dt)
        act_days = _days_since(d.get("notes_last_updated"), run_dt)
        in_window = (mod_days is not None and mod_days < MOVED_WINDOW_DAYS) or \
                    (act_days is not None and act_days < MOVED_WINDOW_DAYS)
        if not in_window:
            continue
        stage = d.get("dealstage") or "unknown"
        # Detect what changed.
        prev_stage = prev_deal_stage.get(did)
        if did in prev_deal_stage and prev_stage != stage:
            change = f"Stage change: {dealstage_label(prev_stage)} → {dealstage_label(stage)}"
            change_type = "stage_change"
        elif act_days is not None and act_days < MOVED_WINDOW_DAYS:
            change = "Activity logged"
            change_type = "activity"
        else:
            change = "Field update (no new activity)"
            change_type = "field_update"
        moved.append({
            "id": did,
            "dealname": d.get("dealname", ""),
            "stage": stage,
            "label": dealstage_label(stage),
            "usd": _to_float(d.get("amount_in_home_currency")),
            "raw_amount": d.get("amount"),
            "currency": d.get("deal_currency_code"),
            "change": change,
            "change_type": change_type,
            "last_activity": _last_activity(d),
            "days_since_activity": act_days,
        })

    # ---- Alert/section: SQLs (contacts + companies) engaged but quiet ----
    def _stale_records(records, kind):
        out = []
        for c in records:
            days = _days_since(c.get("notes_last_updated"), run_dt)
            if days is None or days >= STALE_SQL_ACTIVITY_DAYS:
                out.append({
                    "kind": kind,
                    "id": str(c.get("id")),
                    "name": c.get("name", ""),
                    "hs_lead_status": c.get("hs_lead_status"),
                    "owner": c.get("owner"),
                    "notes_last_updated": c.get("notes_last_updated"),
                    "days_since_activity": days,
                    "deal_name": c.get("associated_deal", {}).get("name") if isinstance(c.get("associated_deal"), dict) else None,
                    "deal_usd": c.get("associated_deal", {}).get("usd") if isinstance(c.get("associated_deal"), dict) else None,
                })
        return out

    stale_sqls = _stale_records(raw.get("sql_contacts", []), "contact")
    stale_sqls += _stale_records(raw.get("sql_companies", []), "company")
    # Sort most-stalled first; "no activity logged" (None) sorts to the very top.
    stale_sqls.sort(key=lambda x: (x["days_since_activity"] is None,
                                   x["days_since_activity"] or 0), reverse=True)

    misc = raw.get("miscategorised", [])
    return {
        "funnel": funnel,
        "mql_count": mql,
        "sql_count": sql,
        "sql_to_mql_ratio": ratio,
        "funnel_inversion": sql > mql,
        "deal_value_basis": DEAL_VALUE_BASIS,
        "deals_by_stage": deals_by_stage,
        "deal_currencies": sorted(all_currencies),
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


# --------------------------------------------------------------------------
# Report rendering
# --------------------------------------------------------------------------

def _fmt_usd(usd, missing_label="n/a (home currency missing)"):
    return f"${usd:,.0f}" if usd is not None else missing_label


def _fmt_raw(raw_amount, currency):
    v = _to_float(raw_amount)
    if v is None:
        return "—"
    return f"{v:,.0f} {currency or ''}".strip()


def render_report(snapshot, prev):
    m = snapshot["metrics"]
    prev_m = prev["metrics"] if prev else None
    date = snapshot["run_date"]
    # Deal-value week-on-week only makes sense if the prior snapshot used the
    # same basis (USD). The first USD run has no comparable predecessor.
    value_comparable = bool(prev_m) and prev_m.get("deal_value_basis") == m["deal_value_basis"]
    out = []
    A = out.append

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {date}")
    A("")
    A(f"*Scope: B2B only (`customer_type = {CUSTOMER_TYPE_FILTER}` for contacts, "
      f"`customer_type__cloned_ = {CUSTOMER_TYPE_FILTER}` for companies). "
      f"Generated {snapshot['generated_at_utc']} UTC from HubSpot.*")
    A("")
    A(f"*Deal values are USD, normalised via HubSpot's converted amount "
      f"(`amount_in_home_currency`). Raw amounts in other currencies are shown "
      f"only as a secondary reference and never summed across currencies.*")
    A("")
    if prev:
        A(f"*Week-on-week deltas are vs the previous snapshot (`{prev['run_date']}`).*")
    else:
        A("*No prior snapshot found — week-on-week comparison starts next run.*")
    A("")

    # ======================================================================
    # ALERTS
    # ======================================================================
    A("## 🔔 Alerts")
    A("")

    # Alert summary line.
    stale = m["qualified_to_buy_stale"]
    misc_prev = prev_m["miscategorised_count"] if prev_m else None
    stale_sql_prev = prev_m.get("stale_engaged_sql_count") if prev_m else None
    A(f"- **\"Qualified to Buy\" deals idle {STALE_ACTIVITY_DAYS}+ days:** {len(stale)}")
    A(f"- **Miscategorised leads:** {delta_str(m['miscategorised_count'], misc_prev)}")
    A(f"- **Engaged SQLs gone quiet ({STALE_SQL_ACTIVITY_DAYS}+ days):** "
      f"{delta_str(m['stale_engaged_sql_count'], stale_sql_prev)}")
    if m["funnel_inversion"]:
        A(f"- **⚠️ Funnel inversion:** SQL ({m['sql_count']}) > MQL ({m['mql_count']})")
    if m["deals_missing_home_currency"]:
        A(f"- **⚠️ Deals missing USD converted amount:** "
          f"{len(m['deals_missing_home_currency'])} (excluded from USD totals — see below)")
    A("")

    # --- Qualified to Buy stale ---
    if stale:
        A(f"### ⚠️ {len(stale)} \"Qualified to Buy\" deal(s) with no activity "
          f"in {STALE_ACTIVITY_DAYS}+ days")
        A("")
        A("| Deal | Last activity | Days idle | USD value | Raw (orig ccy) |")
        A("|------|---------------|-----------|-----------|----------------|")
        for d in stale:
            days = "no activity logged" if d["days_since_activity"] is None \
                else f"{d['days_since_activity']}d"
            last = (d["last_activity"] or "—")[:10]
            A(f"| {d['dealname']} (#{d['id']}) | {last} | {days} "
              f"| {_fmt_usd(d['usd'])} | {_fmt_raw(d['raw_amount'], d['currency'])} |")
        A("")
    else:
        A(f"- ✅ No \"Qualified to Buy\" deals idle for {STALE_ACTIVITY_DAYS}+ days.")
        A("")

    # --- Miscategorised leads ---
    A(f"### 🏷️ Miscategorised leads: {delta_str(m['miscategorised_count'], misc_prev)}")
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

    # --- Funnel inversion ---
    if m["funnel_inversion"]:
        A(f"### ⚠️ Funnel inversion: SQL ({m['sql_count']}) > MQL ({m['mql_count']})")
        A("")
        A("More Sales Qualified than Marketing Qualified Leads. Expected the "
          "funnel to narrow (MQL ≥ SQL). Likely causes: leads skipping the MQL "
          "stage, MQL criteria too strict, or bulk SQL promotion. Worth a look.")
        A("")

    # ======================================================================
    # DEALS MOVED THIS WEEK
    # ======================================================================
    moved = m["deals_moved_this_week"]
    A(f"## 📈 Deals moved this week (last {MOVED_WINDOW_DAYS} days)")
    A("")
    A("What's active and how the pipeline shifted this week — grouped by current "
      "stage, highest USD value first. \"What changed\" is derived by comparing "
      "each deal's stage to last week's snapshot; where the stage is unchanged we "
      "distinguish new activity from a bare field update.")
    A("")
    if not moved:
        A("- No deals modified or with new activity in the window.")
        A("")
    else:
        stage_changes = [d for d in moved if d["change_type"] == "stage_change"]
        n_field = len([d for d in moved if d['change_type'] == 'field_update'])
        A(f"**{len(moved)} deal(s) touched** — {len(stage_changes)} stage change(s), "
          f"{len([d for d in moved if d['change_type']=='activity'])} with new activity, "
          f"{n_field} field-only updates.")
        A("")
        if n_field:
            A("> *Field-only updates carry no new sales activity — they include HubSpot "
              "system/bulk recalculations (e.g. a currency exchange-rate refresh) and "
              "should not be read as pipeline movement.*")
            A("")
        # Group by stage; order groups by total USD desc.
        groups = {}
        for d in moved:
            groups.setdefault(d["stage"], []).append(d)

        def _grp_usd(items):
            return sum((x["usd"] or 0) for x in items)
        for stage in sorted(groups, key=lambda s: -_grp_usd(groups[s])):
            items = sorted(groups[stage], key=lambda x: (x["usd"] or 0), reverse=True)
            A(f"### {dealstage_label(stage)} ({len(items)})")
            A("")
            A("| Deal | USD value | What changed | Last activity | Raw (orig ccy) |")
            A("|------|-----------|--------------|---------------|----------------|")
            for d in items:
                last = (d["last_activity"] or "—")[:10]
                A(f"| {d['dealname']} (#{d['id']}) | {_fmt_usd(d['usd'])} "
                  f"| {d['change']} | {last} | {_fmt_raw(d['raw_amount'], d['currency'])} |")
            A("")

    # ======================================================================
    # FUNNEL
    # ======================================================================
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

    # ======================================================================
    # MQL / SQL
    # ======================================================================
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

    # ======================================================================
    # DEALS BY STAGE (value per stage, USD)
    # ======================================================================
    A("## Deals — count and value by stage (USD)")
    A("")
    if len(m["deal_currencies"]) > 1:
        A(f"> Deals exist in mixed currencies ({', '.join(m['deal_currencies'])}). "
          f"All totals below are USD via `amount_in_home_currency`; the raw-currency "
          f"column is reference only.")
        A("")
    if not value_comparable and prev_m:
        A("> **Note:** the previous snapshot summed raw mixed-currency amounts, so "
          "USD week-on-week value deltas are not comparable this run. Deal **counts** "
          "are compared; USD value week-on-week starts next run.")
        A("")
    A("| Deal stage | Deals | Total USD value | Raw by currency (ref) |")
    A("|------------|-------|-----------------|-----------------------|")
    prev_deals = {d["stage"]: d for d in prev_m["deals_by_stage"]} if prev_m else {}
    tot_count = 0
    tot_usd = 0.0
    tot_missing = 0
    for d in m["deals_by_stage"]:
        tot_count += d["count"]
        tot_usd += d["usd_total"]
        tot_missing += d["usd_missing"]
        pv = prev_deals.get(d["stage"]) if prev_m else None
        pv_count = pv["count"] if pv else None
        if value_comparable and pv is not None:
            usd_cell = money_delta_str(d["usd_total"], pv.get("usd_total"))
        else:
            usd_cell = f"${d['usd_total']:,.0f}"
        if d["usd_missing"]:
            usd_cell += f" ⚠️{d['usd_missing']} missing"
        raw_ref = ", ".join(f"{v:,.0f} {c}" for c, v in sorted(d["raw_by_ccy"].items())) or "—"
        A(f"| {d['label']} | {delta_str(d['count'], pv_count)} | {usd_cell} | {raw_ref} |")
    A(f"| **Total** | **{tot_count}** | **${tot_usd:,.0f}**"
      f"{f' ⚠️{tot_missing} missing' if tot_missing else ''} | |")
    A("")
    A("*Pre-deal lifecycle stages (Email Contact, Lead, MQL) are contact counts "
      "only and carry no deal value — deal value exists only from the deal "
      "pipeline stages above.*")
    A("")
    if m["deals_missing_home_currency"]:
        A(f"**⚠️ {len(m['deals_missing_home_currency'])} deal(s) missing a USD "
          f"converted amount** (`amount_in_home_currency` empty) — excluded from USD "
          f"totals rather than back-filled from the raw amount:")
        A("")
        A("| Deal | Stage | Raw amount (orig ccy) |")
        A("|------|-------|-----------------------|")
        for d in m["deals_missing_home_currency"]:
            A(f"| {d['dealname']} (#{d['id']}) | {d['label']} "
              f"| {_fmt_raw(d['raw_amount'], d['currency'])} |")
        A("")

    # ======================================================================
    # STALLED / ENGAGED SQLs — follow-up (NOT auto-recycle)
    # ======================================================================
    stale_sqls = m["stale_engaged_sqls"]
    A(f"## 🕓 Engaged SQLs gone quiet — follow-up / review: "
      f"{delta_str(m['stale_engaged_sql_count'], stale_sql_prev)}")
    A("")
    A(f"SQL contacts **and** companies where sales has already made real contact "
      f"(lead status Connected / Qualified / In Progress / Proposal Request) but the "
      f"last activity is {STALE_SQL_ACTIVITY_DAYS}+ days ago. This is a live "
      f"**follow-up/review** list for sales to action — **NOT** an auto-recycle. "
      f"Distinct from the \"Attempted to Contact\" cases the stale-lead recycler "
      f"handles automatically. Nothing here moves without a human. Sorted most "
      f"stalled first.")
    A("")
    if stale_sqls:
        by_status = {}
        for c in stale_sqls:
            by_status.setdefault(c["hs_lead_status"], []).append(c)
        status_label = {"CONNECTED": "Contacted", "Qualified": "Qualified",
                        "IN_PROGRESS": "In Progress", "PROPOSAL_REQUEST": "Proposal Request"}
        for status in sorted(by_status, key=lambda s: -len(by_status[s])):
            group = by_status[status]
            A(f"**{status_label.get(status, status)}** ({len(group)})")
            A("")
            A("| Record | Type | Days idle | Owner | Associated deal (USD) |")
            A("|--------|------|-----------|-------|-----------------------|")
            for c in group:
                days = "no activity logged" if c["days_since_activity"] is None \
                    else f"{c['days_since_activity']}d"
                deal = "—"
                if c.get("deal_name"):
                    deal = f"{c['deal_name']} ({_fmt_usd(c.get('deal_usd'))})"
                A(f"| {c['name']} (#{c['id']}) | {c['kind']} | {days} "
                  f"| {c.get('owner') or '—'} | {deal} |")
            A("")
    else:
        A(f"- ✅ No engaged SQLs idle for {STALE_SQL_ACTIVITY_DAYS}+ days.")
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

    prev = load_prev_snapshot(snap_dir, run_date)
    metrics = compute_metrics(raw, run_date, prev_snapshot=prev)

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
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

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
