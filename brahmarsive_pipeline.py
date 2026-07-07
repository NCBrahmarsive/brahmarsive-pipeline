#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health routine.

Pulls contacts, deals and companies from HubSpot, filters to B2B, and writes:
  - snapshots/YYYY-MM-DD.json   raw pulled data + computed metrics
  - reports/YYYY-MM-DD.md       readable leadership summary

Auth:  reads HUBSPOT_TOKEN from the environment (HubSpot private-app / service key
       with crm.objects.contacts.read + crm.objects.deals.read + companies.read).
       The token value is never printed or committed.

Two data-source modes:
  (default)      live pull from the HubSpot REST API using HUBSPOT_TOKEN.
  --input FILE   compute from a pre-pulled dataset (JSON produced out-of-band,
                 e.g. via the HubSpot MCP when direct api.hubapi.com egress is
                 blocked). --deals FILE supplies the raw deal list for that mode.

Usage:
  python3 brahmarsive_pipeline.py                       # live pull, dated today
  python3 brahmarsive_pipeline.py --date 2026-07-07     # live pull, fixed date
  python3 brahmarsive_pipeline.py --input pull.json --deals deals.json
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from glob import glob

# ─────────────────────────────────────────────────────────────────────────────
#  EDITABLE CONFIG  —  tune the health checks here
# ─────────────────────────────────────────────────────────────────────────────

# Scope: B2B only. Contacts carry `customer_type`; companies carry the cloned
# variant `customer_type__cloned_`. Keeping only B2B drops B2C / Partner /
# Vendor / Internal automatically.
B2B_VALUE = "B2B"
CONTACT_B2B_PROP = "customer_type"
COMPANY_B2B_PROP = "customer_type__cloned_"

# ── MISCATEGORISED-LEAD CHECK (edit the sets below to retune) ────────────────
# A contact is *miscategorised* when sales is clearly working it
# (hs_lead_status in MISCATEGORISED_WORKING_STATUSES) but its lifecyclestage is
# still only Lead or MQL (in MISCATEGORISED_STUCK_STAGES) — i.e. it has NOT been
# advanced to Sales Qualified Lead or beyond.
def is_miscategorised(hs_lead_status, lifecyclestage):
    return (hs_lead_status in MISCATEGORISED_WORKING_STATUSES
            and lifecyclestage in MISCATEGORISED_STUCK_STAGES)

MISCATEGORISED_WORKING_STATUSES = {
    "Qualified",              # Qualified
    "CONNECTED",              # Connected / "Contacted"
    "Attempted to Contact",   # Attempted to contact
    "IN_PROGRESS",            # In progress
    "PROPOSAL_REQUEST",       # Proposal request
}
MISCATEGORISED_STUCK_STAGES = {"lead", "marketingqualifiedlead"}

# ── STALLED-SQL FOLLOW-UP CHECK (review list, NOT auto-recycle) ──────────────
# SQLs sales engaged but which have gone quiet. Distinct from the auto-recycled
# "Attempted to Contact" bucket, which is intentionally excluded here.
STALLED_SQL_STATUSES = {"CONNECTED", "Qualified", "IN_PROGRESS", "PROPOSAL_REQUEST"}
STALLED_SQL_THRESHOLD_DAYS = 30      # surface if last activity older than this

# ── DEAL ALERTS ──────────────────────────────────────────────────────────────
QTB_STAGE = "appointmentscheduled"   # internal id for "Qualified to Buy"
QTB_IDLE_ALERT_DAYS = 14             # flag QTB deals idle this many days+
MOVED_THIS_WEEK_DAYS = 7             # "what moved this week" window

# ── LABEL MAPS (internal value → display label) ──────────────────────────────
LIFECYCLE_LABELS = {
    "subscriber": "Email Contact",
    "lead": "Lead",
    "marketingqualifiedlead": "Marketing Qualified Lead",
    "salesqualifiedlead": "Sales Qualified Lead",
    "opportunity": "Opportunity",
    "customer": "Customer",
    "other": "Other",
}
# Display order for the funnel (top → bottom).
LIFECYCLE_ORDER = ["subscriber", "lead", "marketingqualifiedlead",
                   "salesqualifiedlead", "opportunity", "customer", "other"]

DEALSTAGE_LABELS = {
    "1217017229": "Initial Engagement",
    "appointmentscheduled": "Qualified to Buy",
    "presentationscheduled": "Discovery & Trial",
    "1217017230": "Quote Sent",
    "decisionmakerboughtin": "Negotiation",
    "contractsent": "Commitment to Buy",
    "closedwon": "Closed Won",
    "closedlost": "Closed Lost",
}
DEALSTAGE_ORDER = ["1217017229", "appointmentscheduled", "presentationscheduled",
                   "1217017230", "decisionmakerboughtin", "contractsent",
                   "closedwon", "closedlost"]
OPEN_STAGES = ["1217017229", "appointmentscheduled", "presentationscheduled",
               "1217017230", "decisionmakerboughtin", "contractsent"]

LEAD_STATUS_LABELS = {
    "NEW": "New Lead", "CONNECTED": "Contacted", "IN_PROGRESS": "In Progress",
    "PROPOSAL_REQUEST": "Proposal Request", "UNQUALIFIED": "Unqualified",
}

HOME_CURRENCY = "USD"
API_ROOT = "https://api.hubapi.com"

# Properties to pull.
CONTACT_PROPS = ["lifecyclestage", "hs_lead_status", CONTACT_B2B_PROP,
                 "b2b_contact_lead_score_combined", "hs_lastmodifieddate",
                 "notes_last_updated", "hs_object_id", "hubspot_owner_id",
                 "hs_full_name_or_email"]
DEAL_PROPS = ["dealname", "dealstage", "amount", "deal_currency_code",
              "amount_in_home_currency", "hs_lastmodifieddate",
              "notes_last_updated", "hs_object_id", "pipeline", "hubspot_owner_id"]
COMPANY_PROPS = ["name", "lifecyclestage", "hs_lead_status", COMPANY_B2B_PROP,
                 "notes_last_updated", "hs_object_id", "hubspot_owner_id"]

# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_ts(s):
    """ISO-8601 (with optional Z / millis) → aware datetime, or None."""
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def days_since(ts_str, now):
    ts = parse_ts(ts_str)
    if ts is None:
        return None
    return int((now - ts).total_seconds() // 86400)


def to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def usd(v):
    return f"${v:,.0f}" if v is not None else "—"


def delta(cur, prev):
    """Signed change string vs prior snapshot value; '' if no prior."""
    if prev is None:
        return ""
    d = cur - prev
    if abs(d - round(d)) < 1e-9:
        d = int(round(d))
        return f" (+{d})" if d > 0 else (f" ({d})" if d < 0 else " (±0)")
    return f" (+{d:,.0f})" if d > 0 else (f" ({d:,.0f})" if d < 0 else " (±0)")


# ─────────────────────────────────────────────────────────────────────────────
#  Live HubSpot REST fetch
# ─────────────────────────────────────────────────────────────────────────────

def _token():
    tok = os.environ.get("HUBSPOT_TOKEN")
    if not tok:
        sys.exit("ERROR: HUBSPOT_TOKEN is not set in the environment.")
    return tok


def _search(object_type, props, filter_groups, token):
    """Paginate the CRM search API, returning all matching records."""
    url = f"{API_ROOT}/crm/v3/objects/{object_type}/search"
    after, out = None, []
    while True:
        body = {"properties": props, "limit": 200,
                "filterGroups": filter_groups or []}
        if after:
            body["after"] = after
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method="POST", headers={
            "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    payload = json.loads(r.read())
                break
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < 4:
                    time.sleep(2 ** attempt)
                    continue
                raise
        out.extend(payload.get("results", []))
        after = payload.get("paging", {}).get("next", {}).get("after")
        if not after:
            return out


def fetch_live(now):
    """Pull B2B contacts, all deals, B2B companies from HubSpot; build dataset."""
    token = _token()
    b2b_contact = [{"filters": [
        {"propertyName": CONTACT_B2B_PROP, "operator": "EQ", "value": B2B_VALUE}]}]
    contacts = _search("contacts", CONTACT_PROPS, b2b_contact, token)
    deals = _search("deals", DEAL_PROPS, [], token)
    b2b_company = [{"filters": [
        {"propertyName": COMPANY_B2B_PROP, "operator": "EQ", "value": B2B_VALUE}]}]
    companies = _search("companies", COMPANY_PROPS, b2b_company, token)

    lifecycle_counts, lead_status_counts = {}, {}
    miscategorised, stalled_contacts = [], []
    for c in contacts:
        p = c.get("properties", {})
        lc = p.get("lifecyclestage") or "unknown"
        ls = p.get("hs_lead_status") or "Unassigned"
        lifecycle_counts[lc] = lifecycle_counts.get(lc, 0) + 1
        lead_status_counts[ls] = lead_status_counts.get(ls, 0) + 1
        if is_miscategorised(ls, lc):
            miscategorised.append({
                "id": p.get("hs_object_id"), "name": p.get("hs_full_name_or_email"),
                "hs_lead_status": ls, "lifecyclestage": lc,
                "hubspot_owner_id": p.get("hubspot_owner_id"),
                "notes_last_updated": p.get("notes_last_updated")})
        d = days_since(p.get("notes_last_updated"), now)
        if (lc == "salesqualifiedlead" and ls in STALLED_SQL_STATUSES
                and d is not None and d > STALLED_SQL_THRESHOLD_DAYS):
            stalled_contacts.append({
                "id": p.get("hs_object_id"), "name": p.get("hs_full_name_or_email"),
                "hs_lead_status": ls, "notes_last_updated": p.get("notes_last_updated"),
                "hubspot_owner_id": p.get("hubspot_owner_id"), "deal": None})

    stalled_companies = []
    for co in companies:
        p = co.get("properties", {})
        ls = p.get("hs_lead_status")
        d = days_since(p.get("notes_last_updated"), now)
        if (p.get("lifecyclestage") == "salesqualifiedlead"
                and ls in STALLED_SQL_STATUSES and d is not None
                and d > STALLED_SQL_THRESHOLD_DAYS):
            stalled_companies.append({
                "id": p.get("hs_object_id"), "name": p.get("name"),
                "hs_lead_status": ls, "notes_last_updated": p.get("notes_last_updated"),
                "hubspot_owner_id": p.get("hubspot_owner_id"), "deal": None})

    return {
        "lifecycle_counts": lifecycle_counts,
        "lead_status_counts": lead_status_counts,
        "deals": [d.get("properties", {}) | {"id": d.get("id")} for d in deals],
        "miscategorised_contacts": miscategorised,
        "stalled_sql_contacts": stalled_contacts,
        "stalled_sql_companies": stalled_companies,
        "owners": {},
    }


def load_input(input_path, deals_path):
    """Load a pre-pulled dataset (used when direct API egress is unavailable)."""
    with open(input_path) as f:
        data = json.load(f)
    if deals_path:
        with open(deals_path) as f:
            data["deals"] = json.load(f)
    data.setdefault("miscategorised_contacts", [])
    data.setdefault("stalled_sql_contacts", [])
    data.setdefault("stalled_sql_companies", [])
    data.setdefault("owners", {})
    return data


# ─────────────────────────────────────────────────────────────────────────────
#  Metric computation
# ─────────────────────────────────────────────────────────────────────────────

def compute_metrics(data, now):
    lc = data["lifecycle_counts"]
    mql = lc.get("marketingqualifiedlead", 0)
    sql = lc.get("salesqualifiedlead", 0)

    # Deal aggregates by stage (USD via amount_in_home_currency only).
    stage_stats = {s: {"count": 0, "usd_total": 0.0, "missing_home": 0}
                   for s in DEALSTAGE_LABELS}
    currency_flags = []          # deals with empty home-currency value
    for d in data["deals"]:
        s = d.get("dealstage") or "unknown"
        stage_stats.setdefault(s, {"count": 0, "usd_total": 0.0, "missing_home": 0})
        stage_stats[s]["count"] += 1
        home = to_float(d.get("amount_in_home_currency"))
        if home is None:
            stage_stats[s]["missing_home"] += 1
            currency_flags.append({
                "id": d.get("id"), "dealname": d.get("dealname"),
                "dealstage": s, "amount": d.get("amount"),
                "deal_currency_code": d.get("deal_currency_code")})
        else:
            stage_stats[s]["usd_total"] += home

    # Deals moved this week (last-modified OR last-activity within window).
    moved = []
    for d in data["deals"]:
        dm = days_since(d.get("hs_lastmodifieddate"), now)
        da = days_since(d.get("notes_last_updated"), now)
        recent_mod = dm is not None and dm < MOVED_THIS_WEEK_DAYS
        recent_act = da is not None and da < MOVED_THIS_WEEK_DAYS
        if recent_mod or recent_act:
            if recent_act and (da is not None and (dm is None or da <= dm)):
                change = "activity logged"
            elif recent_act:
                change = "activity + field update"
            else:
                change = "field/stage update"
            moved.append({
                "id": d.get("id"), "dealname": d.get("dealname"),
                "dealstage": d.get("dealstage"),
                "usd": to_float(d.get("amount_in_home_currency")),
                "amount": d.get("amount"),
                "deal_currency_code": d.get("deal_currency_code"),
                "change": change,
                "last_activity": d.get("notes_last_updated"),
                "last_modified": d.get("hs_lastmodifieddate"),
                "days_since_activity": da})

    # QTB idle alert.
    qtb_idle = []
    for d in data["deals"]:
        if d.get("dealstage") != QTB_STAGE:
            continue
        da = days_since(d.get("notes_last_updated"), now)
        if da is None or da >= QTB_IDLE_ALERT_DAYS:
            qtb_idle.append({
                "id": d.get("id"), "dealname": d.get("dealname"),
                "usd": to_float(d.get("amount_in_home_currency")),
                "days_idle": da, "last_activity": d.get("notes_last_updated")})

    # Days-idle enrichment for stalled SQLs.
    for rec in data["stalled_sql_contacts"] + data["stalled_sql_companies"]:
        rec["days_idle"] = days_since(rec.get("notes_last_updated"), now)

    open_usd = sum(stage_stats[s]["usd_total"] for s in OPEN_STAGES if s in stage_stats)
    open_cnt = sum(stage_stats[s]["count"] for s in OPEN_STAGES if s in stage_stats)

    return {
        "lifecycle_counts": lc,
        "mql": mql, "sql": sql,
        "mql_sql_ratio": (sql / mql) if mql else None,
        "funnel_inversion": sql > mql,
        "stage_stats": stage_stats,
        "currency_flags": currency_flags,
        "deals_moved": moved,
        "qtb_idle": qtb_idle,
        "miscategorised_count": len(data["miscategorised_contacts"]),
        "stalled_sql_count": len(data["stalled_sql_contacts"]) + len(data["stalled_sql_companies"]),
        "total_deals": len(data["deals"]),
        "open_pipeline_usd": open_usd,
        "open_deal_count": open_cnt,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Snapshot / prior-snapshot handling
# ─────────────────────────────────────────────────────────────────────────────

def load_prior_snapshot(snap_dir, current_date):
    prior = sorted(p for p in glob(os.path.join(snap_dir, "*.json"))
                   if os.path.basename(p) != f"{current_date}.json")
    if not prior:
        return None, None
    with open(prior[-1]) as f:
        return json.load(f), os.path.basename(prior[-1]).replace(".json", "")


def snapshot_metrics_block(m):
    """The comparable metrics we persist for week-on-week."""
    return {
        "lifecycle_counts": m["lifecycle_counts"],
        "mql": m["mql"], "sql": m["sql"],
        "miscategorised_count": m["miscategorised_count"],
        "stalled_sql_count": m["stalled_sql_count"],
        "stage_stats": {s: {"count": v["count"], "usd_total": v["usd_total"]}
                        for s, v in m["stage_stats"].items()},
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Report rendering
# ─────────────────────────────────────────────────────────────────────────────

def owner_name(data, oid):
    return data.get("owners", {}).get(str(oid), f"owner {oid}" if oid else "unassigned")


def render_report(data, m, run_date, prior, prior_date, now):
    pm = (prior or {}).get("metrics", {})
    p_lc = pm.get("lifecycle_counts", {})
    p_stage = pm.get("stage_stats", {})
    L = []
    a = L.append

    a(f"# Brahmarsive Weekly B2B Pipeline & MQL Health — {run_date}")
    a("")
    if prior_date:
        a(f"_Week-on-week deltas vs prior snapshot **{prior_date}**. Scope: "
          f"`{CONTACT_B2B_PROP} = {B2B_VALUE}` (contacts) / "
          f"`{COMPANY_B2B_PROP} = {B2B_VALUE}` (companies). All deal values in "
          f"{HOME_CURRENCY} via `amount_in_home_currency`._")
    else:
        a("_First snapshot — no prior to compare against; week-on-week comparison "
          "starts next run. Scope: B2B only. All deal values in "
          f"{HOME_CURRENCY} via `amount_in_home_currency`._")
    a("")

    # ── ALERTS ───────────────────────────────────────────────────────────────
    a("## 🚨 Alerts")
    a("")
    if m["funnel_inversion"]:
        a(f"- **⚠️ Funnel inversion: SQL ({m['sql']}) > MQL ({m['mql']}).** More "
          "contacts sit at Sales Qualified Lead than Marketing Qualified Lead — "
          "MQL is not feeding SQL, or lifecycle stages are being set directly.")
    else:
        a(f"- ✅ No funnel inversion (MQL {m['mql']} ≥ SQL {m['sql']}).")
    mc = m["miscategorised_count"]
    a(f"- **Miscategorised leads: {mc}{delta(mc, pm.get('miscategorised_count'))}** "
      "— sales is working them but lifecycle is still Lead/MQL (see below).")
    sc = m["stalled_sql_count"]
    a(f"- **Stalled SQLs (review / follow-up): {sc}"
      f"{delta(sc, pm.get('stalled_sql_count'))}** — engaged SQLs quiet "
      f"{STALLED_SQL_THRESHOLD_DAYS}+ days (see below). Not auto-recycled.")
    if m["qtb_idle"]:
        a(f"- **{len(m['qtb_idle'])} 'Qualified to Buy' deal(s) idle "
          f"{QTB_IDLE_ALERT_DAYS}+ days:**")
        for q in sorted(m["qtb_idle"], key=lambda x: -(x["usd"] or 0)):
            idle = f"{q['days_idle']}d" if q["days_idle"] is not None else "no activity date"
            a(f"    - {q['dealname']} — {usd(q['usd'])}, idle {idle}")
    else:
        a(f"- ✅ No 'Qualified to Buy' deals idle {QTB_IDLE_ALERT_DAYS}+ days.")
    if m["currency_flags"]:
        a(f"- **⚠️ {len(m['currency_flags'])} deal(s) missing "
          f"`amount_in_home_currency`** — excluded from USD totals (not "
          "back-filled from raw amount):")
        for c in m["currency_flags"]:
            raw = f"{c['amount']} {c['deal_currency_code']}" if c.get("amount") else "no amount"
            a(f"    - {c['dealname']} ({DEALSTAGE_LABELS.get(c['dealstage'], c['dealstage'])}, {raw})")
    a("")

    # ── FUNNEL ────────────────────────────────────────────────────────────────
    a("## Lifecycle funnel (B2B contacts)")
    a("")
    a("| Stage | Contacts | WoW |")
    a("|---|---:|---:|")
    total = 0
    for s in LIFECYCLE_ORDER:
        cur = m["lifecycle_counts"].get(s, 0)
        total += cur
        a(f"| {LIFECYCLE_LABELS[s]} | {cur} | {delta(cur, p_lc.get(s)) or '—'} |")
    a(f"| **Total B2B** | **{total}** | |")
    a("")
    ratio = m["mql_sql_ratio"]
    ratio_s = f"{ratio:.2f} SQL per MQL" if ratio is not None else "n/a (MQL = 0)"
    a(f"**MQL → SQL:** MQL {m['mql']}{delta(m['mql'], pm.get('mql'))} vs "
      f"SQL {m['sql']}{delta(m['sql'], pm.get('sql'))} — ratio {ratio_s}. "
      + ("⚠️ **Inverted (SQL > MQL).**" if m["funnel_inversion"] else "Normal (MQL ≥ SQL)."))
    a("")

    # ── DEAL VALUE BY STAGE ────────────────────────────────────────────────────
    a("## Deal value by stage")
    a("")
    a(f"Deal counts and total {HOME_CURRENCY} value per stage. Pre-deal lifecycle "
      "stages (Email Contact, Lead, MQL) are **contact counts only** and carry no "
      "deal value.")
    a("")
    a(f"| Deal stage | Deals | WoW | Value ({HOME_CURRENCY}) | WoW |")
    a("|---|---:|---:|---:|---:|")
    for s in DEALSTAGE_ORDER:
        st = m["stage_stats"].get(s, {"count": 0, "usd_total": 0.0, "missing_home": 0})
        ps = p_stage.get(s, {})
        note = f" ⚠️{st['missing_home']} no-USD" if st.get("missing_home") else ""
        a(f"| {DEALSTAGE_LABELS[s]} | {st['count']}{note} | "
          f"{delta(st['count'], ps.get('count')) or '—'} | {usd(st['usd_total'])} | "
          f"{delta(st['usd_total'], ps.get('usd_total')) or '—'} |")
    a(f"| **Open pipeline (excl. closed)** | **{m['open_deal_count']}** | | "
      f"**{usd(m['open_pipeline_usd'])}** | |")
    a(f"| **All deals** | **{m['total_deals']}** | | | |")
    a("")

    # ── DEALS MOVED THIS WEEK ──────────────────────────────────────────────────
    a(f"## What moved this week (last {MOVED_THIS_WEEK_DAYS} days)")
    a("")
    a("Active deals — last-modified or last-activity within the window. Grouped by "
      "current stage, highest USD value first. Quick answer to \"which deals are "
      "active and how did the pipeline shift this week?\"")
    a("")
    if not m["deals_moved"]:
        a("_No deals moved in the window._")
    else:
        by_stage = {}
        for d in m["deals_moved"]:
            by_stage.setdefault(d["dealstage"], []).append(d)
        for s in DEALSTAGE_ORDER:
            if s not in by_stage:
                continue
            rows = sorted(by_stage[s], key=lambda x: -(x["usd"] or 0))
            a(f"### {DEALSTAGE_LABELS.get(s, s)} ({len(rows)})")
            a("")
            a(f"| Deal | Value ({HOME_CURRENCY}) | What changed | Last activity | Orig. |")
            a("|---|---:|---|---|---|")
            for d in rows:
                la = (d["last_activity"] or d["last_modified"] or "")[:10]
                orig = (f"{to_float(d['amount']):,.0f} {d['deal_currency_code']}"
                        if to_float(d["amount"]) is not None and d["deal_currency_code"] != HOME_CURRENCY
                        else "—")
                a(f"| {d['dealname']} | {usd(d['usd'])} | {d['change']} | {la} | {orig} |")
            a("")

    # ── MISCATEGORISED LEADS ───────────────────────────────────────────────────
    a("## Miscategorised leads")
    a("")
    a("Sales is working these (lead status = Qualified / Connected / Attempted to "
      "Contact / In Progress / Proposal Request) but lifecycle is still only "
      "**Lead** or **MQL** — they should likely be advanced to SQL.")
    a("")
    mis = data["miscategorised_contacts"]
    a(f"**Count: {mc}{delta(mc, pm.get('miscategorised_count'))}**")
    a("")
    if mis:
        a("| Contact | ID | Lead status | Lifecycle | Owner |")
        a("|---|---|---|---|---|")
        for c in mis:
            a(f"| {c.get('name') or '—'} | `{c['id']}` | "
              f"{LEAD_STATUS_LABELS.get(c['hs_lead_status'], c['hs_lead_status'])} | "
              f"{LIFECYCLE_LABELS.get(c['lifecyclestage'], c['lifecyclestage'])} | "
              f"{owner_name(data, c.get('hubspot_owner_id'))} |")
        a("")
        a("Contact IDs: " + ", ".join(f"`{c['id']}`" for c in mis))
    else:
        a("_None this run._")
    a("")

    # ── STALLED SQLs ───────────────────────────────────────────────────────────
    a("## Stalled SQLs — follow-up / review (NOT auto-recycle)")
    a("")
    a("Live SQL relationships sales has engaged that have gone quiet "
      f"({STALLED_SQL_THRESHOLD_DAYS}+ days since last activity). Distinct from the "
      "auto-recycled *Attempted to Contact* bucket. **Nothing here is moved "
      "automatically — these are for sales to action.** Sorted most-stalled first.")
    a("")
    a(f"**Count: {sc}{delta(sc, pm.get('stalled_sql_count'))}** "
      f"({len(data['stalled_sql_contacts'])} contacts, "
      f"{len(data['stalled_sql_companies'])} companies)")
    a("")
    recs = data["stalled_sql_contacts"] + data["stalled_sql_companies"]
    if recs:
        by_status = {}
        for r in recs:
            by_status.setdefault(r["hs_lead_status"], []).append(r)
        for status in sorted(by_status):
            rows = sorted(by_status[status],
                          key=lambda x: -(x["days_idle"] or 0))
            label = LEAD_STATUS_LABELS.get(status, status)
            a(f"### {label} ({len(rows)})")
            a("")
            a("| Record | Days idle | Owner | Deal | Deal value |")
            a("|---|---:|---|---|---:|")
            for r in rows:
                deal = r.get("deal") or {}
                dn = deal.get("dealname", "—") if deal else "—"
                dv = usd(to_float(deal.get("amount_in_home_currency"))) if deal else "—"
                a(f"| {r.get('name') or '—'} | {r.get('days_idle', '—')} | "
                  f"{owner_name(data, r.get('hubspot_owner_id'))} | {dn} | {dv} |")
            a("")
    else:
        a("_None this run._")
    a("")

    a("---")
    a(f"_Generated {now.strftime('%Y-%m-%d %H:%M UTC')} · scope B2B only · "
      "values in USD via `amount_in_home_currency` · raw pull in "
      f"`snapshots/{run_date}.json`._")
    return "\n".join(L) + "\n"


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly B2B pipeline report")
    ap.add_argument("--date", help="run date YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--input", help="pre-pulled dataset JSON (skips live API)")
    ap.add_argument("--deals", help="raw deals JSON (with --input)")
    ap.add_argument("--outdir", default=".", help="repo root for snapshots/ reports/")
    ap.add_argument("--now", help="override 'now' as ISO8601 (for deterministic runs)")
    args = ap.parse_args()

    now = parse_ts(args.now) if args.now else datetime.now(timezone.utc)
    run_date = args.date or now.strftime("%Y-%m-%d")

    if args.input:
        data = load_input(args.input, args.deals)
    else:
        data = fetch_live(now)

    m = compute_metrics(data, now)

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prior, prior_date = load_prior_snapshot(snap_dir, run_date)

    snapshot = {
        "run_date": run_date,
        "generated_at": now.isoformat(),
        "scope": {"contacts": f"{CONTACT_B2B_PROP}={B2B_VALUE}",
                  "companies": f"{COMPANY_B2B_PROP}={B2B_VALUE}"},
        "home_currency": HOME_CURRENCY,
        "prior_snapshot": prior_date,
        "metrics": snapshot_metrics_block(m),
        "raw": {
            "lifecycle_counts": data["lifecycle_counts"],
            "lead_status_counts": data.get("lead_status_counts", {}),
            "deals": data["deals"],
            "miscategorised_contacts": data["miscategorised_contacts"],
            "stalled_sql_contacts": data["stalled_sql_contacts"],
            "stalled_sql_companies": data["stalled_sql_companies"],
            "owners": data.get("owners", {}),
        },
        "notes": data.get("note"),
    }
    snap_path = os.path.join(snap_dir, f"{run_date}.json")
    with open(snap_path, "w") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    report = render_report(data, m, run_date, prior, prior_date, now)
    rep_path = os.path.join(rep_dir, f"{run_date}.md")
    with open(rep_path, "w") as f:
        f.write(report)

    print(f"Wrote {snap_path}")
    print(f"Wrote {rep_path}")
    print(f"  MQL={m['mql']} SQL={m['sql']} inversion={m['funnel_inversion']} "
          f"miscategorised={m['miscategorised_count']} "
          f"stalled_sql={m['stalled_sql_count']} "
          f"qtb_idle={len(m['qtb_idle'])} deals={m['total_deals']}")


if __name__ == "__main__":
    main()
