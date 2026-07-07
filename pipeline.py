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

# Contact properties we pull.
CONTACT_PROPERTIES = [
    "lifecyclestage", "hs_lead_status", "customer_type",
    "b2b_contact_lead_score_combined", "hs_lastmodifieddate",
    "notes_last_updated", "hubspot_owner_id", "hs_full_name_or_email",
]
# Deal properties we pull. notes_last_updated is HubSpot's "Last Activity Date".
DEAL_PROPERTIES = [
    "dealstage", "amount", "deal_currency_code",
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


def compute_metrics(raw, run_date):
    funnel_counts = raw["funnel_counts"]

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

    # Deals by stage: count + summed raw amount.
    by_stage = {}
    currencies = set()
    for d in raw["deals"]:
        stage = d.get("dealstage") or "unknown"
        entry = by_stage.setdefault(stage, {"count": 0, "amount": 0.0})
        entry["count"] += 1
        entry["amount"] += _to_float(d.get("amount"))
        if d.get("deal_currency_code"):
            currencies.add(d["deal_currency_code"])
    deals_by_stage = []
    for stage, entry in by_stage.items():
        deals_by_stage.append({
            "stage": stage,
            "label": DEALSTAGE_LABELS.get(stage, stage),
            "count": entry["count"],
            "total_amount": round(entry["amount"], 2),
        })
    deals_by_stage.sort(key=lambda x: (-x["count"], x["label"]))

    # Alert: Qualified-to-Buy deals with no activity in 14+ days.
    run_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)
    stale = []
    for d in raw["deals"]:
        if d.get("dealstage") != QUALIFIED_TO_BUY_STAGE:
            continue
        last = _parse_ts(d.get("notes_last_updated"))
        if last is None:
            days = None
        else:
            if last.tzinfo is None:
                last = last.replace(tzinfo=dt.timezone.utc)
            days = (run_dt - last).days
        if days is None or days >= STALE_ACTIVITY_DAYS:
            stale.append({
                "id": d.get("id"),
                "dealname": d.get("dealname", ""),
                "notes_last_updated": d.get("notes_last_updated"),
                "days_since_activity": days,
                "amount": _to_float(d.get("amount")),
                "currency": d.get("deal_currency_code"),
            })
    stale.sort(key=lambda x: (x["days_since_activity"] is not None,
                              x["days_since_activity"] or 0), reverse=True)

    # Alert: SQLs sales has engaged but gone quiet (30+ days), grouped by
    # lead status. Distinct from the auto-recycled Attempted-to-Contact case.
    stale_sqls = []
    for c in raw.get("sql_contacts", []):
        last = _parse_ts(c.get("notes_last_updated"))
        if last is None:
            days = None
        else:
            if last.tzinfo is None:
                last = last.replace(tzinfo=dt.timezone.utc)
            days = (run_dt - last).days
        if days is None or days >= STALE_SQL_ACTIVITY_DAYS:
            stale_sqls.append({**c, "days_since_activity": days})
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

def render_report(snapshot, prev):
    m = snapshot["metrics"]
    prev_m = prev["metrics"] if prev else None
    date = snapshot["run_date"]
    out = []
    A = out.append

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {date}")
    A("")
    A(f"*Scope: B2B contacts only (`customer_type = {CUSTOMER_TYPE_FILTER}`). "
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
    stale = m["qualified_to_buy_stale"]
    if stale:
        A(f"### ⚠️ {len(stale)} \"Qualified to Buy\" deal(s) with no activity "
          f"in {STALE_ACTIVITY_DAYS}+ days")
        A("")
        A("| Deal | Last activity | Days idle | Amount |")
        A("|------|---------------|-----------|--------|")
        for d in stale:
            days = "no activity logged" if d["days_since_activity"] is None \
                else f"{d['days_since_activity']}d"
            last = d["notes_last_updated"] or "—"
            amt = f"{d['amount']:,.0f} {d.get('currency') or ''}".strip()
            A(f"| {d['dealname']} (#{d['id']}) | {last[:10]} | {days} | {amt} |")
        A("")
    else:
        A(f"- ✅ No \"Qualified to Buy\" deals idle for {STALE_ACTIVITY_DAYS}+ days.")
        A("")

    stale_sqls = m["stale_engaged_sqls"]
    prev_stale = len(prev_m["stale_engaged_sqls"]) if prev_m else None
    A(f"### 🕓 SQLs engaged but gone quiet: {delta_str(len(stale_sqls), prev_stale)}")
    A("")
    A(f"Sales has already made real contact on these — this is a follow-up/review "
      f"list ({STALE_SQL_ACTIVITY_DAYS}+ days idle), NOT an auto-recycle. Distinct "
      "from the \"Attempted to Contact\" cases the stale-lead recycler handles "
      "automatically. Nothing here moves without a human.")
    A("")
    if stale_sqls:
        by_status = {}
        for c in stale_sqls:
            by_status.setdefault(c["hs_lead_status"], []).append(c)
        for status, group in by_status.items():
            A(f"**{status}** ({len(group)})")
            A("")
            A("| Contact | Days idle | Owner |")
            A("|---------|-----------|-------|")
            for c in group:
                days = "no activity logged" if c["days_since_activity"] is None \
                    else f"{c['days_since_activity']}d"
                A(f"| {c['name']} (#{c['id']}) | {days} | {c.get('owner') or '—'} |")
            A("")
    else:
        A(f"- ✅ No engaged SQLs idle for {STALE_SQL_ACTIVITY_DAYS}+ days.")
        A("")

    misc_prev = prev_m["miscategorised_count"] if prev_m else None
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
    A("## Deals — count and value by stage")
    A("")
    if len(m["deal_currencies"]) > 1:
        A(f"> ⚠️ **Currency caveat:** deal amounts are stored in mixed "
          f"currencies ({', '.join(m['deal_currencies'])}) and are summed as-is "
          f"(no FX conversion). Totals below are not currency-normalised — read "
          f"them per stage, not as a single grand total.")
        A("")
    A("| Deal stage | Deals | Total amount (raw) |")
    A("|------------|-------|--------------------|")
    prev_deals = {d["stage"]: d for d in prev_m["deals_by_stage"]} if prev_m else {}
    tot_count = 0
    tot_amt = 0.0
    for d in m["deals_by_stage"]:
        tot_count += d["count"]
        tot_amt += d["total_amount"]
        pv = prev_deals.get(d["stage"]) if prev_m else None
        pv_count = pv["count"] if pv else None
        pv_amt = pv["total_amount"] if pv else None
        A(f"| {d['label']} | {delta_str(d['count'], pv_count)} "
          f"| {money_delta_str(d['total_amount'], pv_amt)} |")
    A(f"| **Total** | **{tot_count}** | **{tot_amt:,.0f}** |")
    A("")
    A("*Value per stage note: pre-deal lifecycle stages (Email Contact, Lead, "
      "MQL) are contact counts only and carry no deal value — deal value exists "
      "only from the deal pipeline stages above.*")
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

    metrics = compute_metrics(raw, run_date)

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
