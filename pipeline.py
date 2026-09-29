#!/usr/bin/env python3
"""Brahmarsive weekly B2B pipeline & MQL-health report.

This is the Sections 1-4 report generator. It consumes a pre-pulled
``raw_input.json`` (data fetched from HubSpot via the MCP SQL layer) and writes:

  snapshots/YYYY-MM-DD.json   raw records + every worklist id + week-on-week values
  reports/YYYY-MM-DD.md       the readable Sections 1-4 report

Scoping rules honoured (see the routine spec):
  * B2B only. Contacts filtered on ``customer_type = B2B``; companies on
    ``customer_type__cloned_ = B2B``.
  * Two bases, never mixed. Contact counts (SQL totals, miscategorised, new
    leads) come from the CONTACT object only. Pipeline value comes from DEALS,
    scoped B2B via the associated COMPANY. Companies never feed SQL counts.
  * Deal USD value is ``amount_in_home_currency``. Raw ``amount`` +
    ``deal_currency_code`` are shown only as a secondary reference. Open deals
    with an empty home-currency value are flagged (admin worklist 3B), never
    back-filled from raw amount.
  * Contact->company joins are done here in Python from the pulled scalars
    (``associatedcompanyid`` on contacts, ``id``/``createdate`` on companies).

Run:  python3 pipeline.py --input raw_input.json
"""

import argparse
import datetime as dt
import json
import os
import statistics
import sys

# ===========================================================================
# EDITABLE CONSTANTS
# ===========================================================================

DEALSTAGE_LABELS = {
    "1217017229": "Initial Engagement",
    "appointmentscheduled": "Qualified to Buy",
    "presentationscheduled": "Discovery & Trial",
    "1217017230": "Quote Sent",
    "contractsent": "Commitment to Buy",
    "decisionmakerboughtin": "Negotiation",
    "closedwon": "Closed Won",
    "closedlost": "Closed Lost",
}

# Open-pipeline display order (open stages only).
STAGE_ORDER = [
    "Initial Engagement",
    "Qualified to Buy",
    "Discovery & Trial",
    "Quote Sent",
    "Commitment to Buy",
    "Negotiation",
]
CLOSED_LABELS = ["Closed Won", "Closed Lost"]
CLOSED_STAGES = {"closedwon", "closedlost"}

# hs_lead_status values that mean "sales is actively working this contact".
# The portal stores "Contacted" internally as CONNECTED, so both forms are
# accepted here; the display normalises to the two canonical labels.
SALES_WORKING_STATUSES = ["Attempted to Contact", "Contacted"]
_WORKING_STATUS_RAW = {"Attempted to Contact", "Contacted", "CONNECTED"}
MISCATEGORISED_LIFECYCLE_STAGES = {"lead", "marketingqualifiedlead"}

# What the live stale-SQL recycler workflow keys on today.
RECYCLER_TRIGGER_STATUS = "Attempted to Contact"

WINDOW_DAYS = 7
IDLE_BUCKETS = [30, 60]
QTB_STALE_DAYS = 14        # "idle" threshold for open deals (Section 1.1)
RECYCLE_DAYS = 90          # recycler no-activity threshold

LEAD_STATUS_LABELS = {
    "NEW": "New Lead",
    "CONNECTED": "Contacted",
    "IN_PROGRESS": "In Progress",
    "PROPOSAL_REQUEST": "Proposal Request",
    "UNQUALIFIED": "Unqualified",
    "MQL": "MQL",
    "Marketing Engaged": "Marketing Engaged",
    "Attempted to Contact": "Attempted to Contact",
    "Qualified": "Qualified",
}

CUSTOMER_TYPE_LABELS = {
    "Internal (Exclude)": "Internal",
}
NON_B2B_TYPES = {"B2C", "Partner", "Partner-B2B", "Internal (Exclude)", "Internal"}


# ===========================================================================
# Helpers
# ===========================================================================

def parse_ts(value):
    if not value:
        return None
    v = str(value).replace("Z", "+00:00")
    try:
        t = dt.datetime.fromisoformat(v)
    except ValueError:
        # date-only
        try:
            t = dt.datetime.fromisoformat(v[:10])
        except ValueError:
            return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t


def days_since(value, run_dt):
    t = parse_ts(value)
    if t is None:
        return None
    return (run_dt - t).days


def to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def status_label(raw):
    if raw is None or raw == "":
        return "(blank)"
    return LEAD_STATUS_LABELS.get(raw, raw)


def ctype_label(raw):
    if raw is None or raw == "":
        return "Unassigned"
    return CUSTOMER_TYPE_LABELS.get(raw, raw)


def is_working_status(raw):
    return raw in _WORKING_STATUS_RAW


def owner_name(owner_id, owners):
    if not owner_id:
        return "Unassigned"
    return owners.get(str(owner_id), f"Owner {owner_id}")


def within_window(iso, run_dt, window_days):
    t = parse_ts(iso)
    if t is None:
        return False
    return (run_dt - t) <= dt.timedelta(days=window_days) and t <= run_dt


def pct(n, d):
    return round(100.0 * n / d, 1) if d else 0.0


# ===========================================================================
# Metric computation
# ===========================================================================

def compute(raw, run_dt):
    owners = raw.get("owners", {})
    window = raw.get("window_days", WINDOW_DAYS)
    contacts = raw["contacts_b2b"]
    deals = raw["deals_b2b"]
    companies = raw["companies_b2b"]
    sql_all = raw["sql_all_types"]
    recycled_recent = raw.get("recycled_recent", [])

    new_company_ids = {str(c["id"]) for c in companies
                       if within_window(c.get("createdate"), run_dt, window)}
    all_company_ids = {str(c["id"]): c for c in companies}

    # ---- SECTION 1: deals ---------------------------------------------------
    open_deals = [d for d in deals if d.get("dealstage") not in CLOSED_STAGES]
    closed_deals = [d for d in deals if d.get("dealstage") in CLOSED_STAGES]

    def deal_idle(d):
        return days_since(d.get("notes_last_updated"), run_dt)

    # 1.1 open by stage
    by_stage = {}
    for d in open_deals:
        label = DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage"))
        e = by_stage.setdefault(label, {"deals": [], "usd": 0.0})
        e["deals"].append(d)
        e["usd"] += to_float(d.get("amount_in_home_currency")) or 0.0

    open_by_stage = []
    for label in STAGE_ORDER:
        e = by_stage.get(label)
        if not e:
            open_by_stage.append({"stage": label, "qty": 0, "usd": 0.0,
                                  "median_idle": None, "idle_14plus": 0})
            continue
        idles = [deal_idle(d) for d in e["deals"]]
        known = [x for x in idles if x is not None]
        idle14 = sum(1 for x in idles if x is None or x >= QTB_STALE_DAYS)
        open_by_stage.append({
            "stage": label,
            "qty": len(e["deals"]),
            "usd": round(e["usd"], 2),
            "median_idle": int(statistics.median(known)) if known else None,
            "idle_14plus": idle14,
        })
    open_qty = sum(s["qty"] for s in open_by_stage)
    open_usd = round(sum(s["usd"] for s in open_by_stage), 2)
    open_idle14 = sum(s["idle_14plus"] for s in open_by_stage)

    closed_ref = []
    for label in CLOSED_LABELS:
        cds = [d for d in closed_deals
               if DEALSTAGE_LABELS.get(d.get("dealstage")) == label]
        closed_ref.append({
            "stage": label,
            "qty": len(cds),
            "usd": round(sum(to_float(d.get("amount_in_home_currency")) or 0.0
                             for d in cds), 2),
        })

    # 1.2 open by owner
    owner_agg = {}
    for d in open_deals:
        oid = d.get("owner_id") or ""
        e = owner_agg.setdefault(oid, {"qty": 0, "usd": 0.0})
        e["qty"] += 1
        e["usd"] += to_float(d.get("amount_in_home_currency")) or 0.0
    open_by_owner = [{
        "owner": owner_name(oid, owners), "owner_id": oid,
        "qty": e["qty"], "usd": round(e["usd"], 2),
    } for oid, e in owner_agg.items()]
    open_by_owner.sort(key=lambda x: -x["usd"])

    top_owner = open_by_owner[0] if open_by_owner else None
    top_owner_share = pct(top_owner["usd"], open_usd) if top_owner else 0.0
    largest_deal = None
    for d in open_deals:
        v = to_float(d.get("amount_in_home_currency")) or 0.0
        if largest_deal is None or v > largest_deal["usd"]:
            largest_deal = {"dealname": d.get("dealname"), "usd": v,
                            "owner": owner_name(d.get("owner_id"), owners),
                            "id": d.get("id")}
    largest_deal_share = pct(largest_deal["usd"], open_usd) if largest_deal else 0.0

    # 1.3 moved / updated in last 7 days
    moved, updated = [], []
    for d in open_deals + closed_deals:
        entered = within_window(d.get("hs_v2_date_entered_current_stage"), run_dt, window)
        modified = within_window(d.get("hs_lastmodifieddate"), run_dt, window)
        rec = {
            "id": d.get("id"), "dealname": d.get("dealname"),
            "owner": owner_name(d.get("owner_id"), owners),
            "stage": DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage")),
            "usd": to_float(d.get("amount_in_home_currency")) or 0.0,
            "last_activity": (d.get("notes_last_updated") or "")[:10] or "—",
            "entered_stage": (d.get("hs_v2_date_entered_current_stage") or "")[:10],
        }
        if entered:
            moved.append(rec)
        elif modified:
            updated.append(rec)
    moved.sort(key=lambda x: -x["usd"])
    updated.sort(key=lambda x: -x["usd"])

    # ---- SECTION 2: top of funnel ------------------------------------------
    new_contacts = [c for c in contacts
                    if within_window(c.get("createdate"), run_dt, window)]

    # 2.1 new companies by owner
    new_companies = [c for c in companies
                     if within_window(c.get("createdate"), run_dt, window)]
    newco_by_owner = {}
    for c in new_companies:
        oid = c.get("owner_id") or ""
        newco_by_owner.setdefault(oid, []).append(c)

    # 2.2 new contacts on existing vs new accounts
    new_on_existing = []
    new_on_new = []
    orphan_new = []
    for c in new_contacts:
        acid = (c.get("associatedcompanyid") or "").strip()
        if not acid:
            orphan_new.append(c)
        elif acid in new_company_ids:
            new_on_new.append(c)
        else:
            new_on_existing.append(c)

    def group_owner_lifecycle(rows):
        out = {}
        for c in rows:
            oid = c.get("owner_id") or ""
            out.setdefault(oid, {}).setdefault(c.get("lifecyclestage") or "unknown", []).append(c)
        return out

    # 2.4 SQLs by owner (contacts only)
    sql_contacts = [c for c in contacts if c.get("lifecyclestage") == "salesqualifiedlead"]
    sql_by_owner = {}
    for c in sql_contacts:
        oid = c.get("owner_id") or ""
        e = sql_by_owner.setdefault(oid, {"qty": 0, "idle30": 0, "idle60": 0})
        e["qty"] += 1
        ds = days_since(c.get("notes_last_updated"), run_dt)
        if ds is None or ds >= 30:
            e["idle30"] += 1
        if ds is None or ds >= 60:
            e["idle60"] += 1
    sql_rows = [{
        "owner": owner_name(oid, owners), "owner_id": oid,
        "qty": e["qty"], "idle30": e["idle30"], "idle60": e["idle60"],
    } for oid, e in sql_by_owner.items()]
    sql_rows.sort(key=lambda x: -x["qty"])
    sql_total = len(sql_contacts)
    sql_idle30 = sum(r["idle30"] for r in sql_rows)
    sql_idle60 = sum(r["idle60"] for r in sql_rows)

    # ---- SECTION 3: admin / hygiene ----------------------------------------
    # 3A new accounts to complete (missing owner and/or name)
    incomplete_accounts = []
    for c in new_companies:
        missing_owner = not (c.get("owner_id") or "").strip()
        missing_name = not (c.get("name") or "").strip()
        if missing_owner or missing_name:
            incomplete_accounts.append({
                "id": str(c["id"]),
                "created": (c.get("createdate") or "")[:10],
                "owner": owner_name(c.get("owner_id"), owners) if not missing_owner else "none",
                "owner_id": c.get("owner_id") or "",
                "name": c.get("name") if not missing_name else "blank",
                "missing_owner": missing_owner,
                "missing_name": missing_name,
            })

    # 3B open deals missing USD
    deals_missing_usd = [{
        "id": d.get("id"), "dealname": d.get("dealname"),
        "stage": DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage")),
        "owner": owner_name(d.get("owner_id"), owners), "owner_id": d.get("owner_id") or "",
    } for d in open_deals if to_float(d.get("amount_in_home_currency")) is None]

    # 3C miscategorised
    misc = [c for c in contacts
            if is_working_status(c.get("hs_lead_status"))
            and c.get("lifecyclestage") in MISCATEGORISED_LIFECYCLE_STAGES]
    misc_by_owner = {}
    for c in misc:
        oid = c.get("owner_id") or ""
        misc_by_owner.setdefault(oid, []).append(c)

    # ---- SECTION 4: recycler health ----------------------------------------
    # 4A trigger match now (SQL, trigger status, idle >= RECYCLE_DAYS) by type
    trigger_by_type = {}
    for c in sql_all:
        if c.get("hs_lead_status") != RECYCLER_TRIGGER_STATUS:
            continue
        ds = days_since(c.get("notes_last_updated"), run_dt)
        if ds is None or ds >= RECYCLE_DAYS:
            t = ctype_label(c.get("customer_type"))
            trigger_by_type[t] = trigger_by_type.get(t, 0) + 1
    b2b_trigger = trigger_by_type.get("B2B", 0)
    nonb2b_trigger = sum(v for k, v in trigger_by_type.items() if k != "B2B")

    # stale B2B SQL backlog untouched 90+d
    stale_b2b_sql = []
    for c in sql_contacts:
        ds = days_since(c.get("notes_last_updated"), run_dt)
        if ds is None or ds >= RECYCLE_DAYS:
            stale_b2b_sql.append(c)

    # 4B backlog the trigger MISSES (not trigger status), by status
    backlog_missed = [c for c in stale_b2b_sql
                      if c.get("hs_lead_status") != RECYCLER_TRIGGER_STATUS]
    backlog_by_status = {}
    for c in backlog_missed:
        backlog_by_status.setdefault(status_label(c.get("hs_lead_status")), []).append(c)

    # 4C recycled this week by customer_type
    recycled_by_type = {}
    for c in recycled_recent:
        t = ctype_label(c.get("customer_type"))
        recycled_by_type.setdefault(t, []).append(c)

    # 4D bounce-back: recycled_date set AND lifecycle back to SQL
    bounce = [c for c in sql_contacts if c.get("recycled_date")]
    bounce_by_owner = {}
    bounce_by_status = {}
    for c in bounce:
        bounce_by_owner.setdefault(c.get("owner_id") or "", []).append(c)
        bounce_by_status.setdefault(status_label(c.get("hs_lead_status")), []).append(c)
    # all customer types (context)
    bounce_all_types = sum(1 for c in sql_all if c.get("recycled_date"))

    alerts = []
    if b2b_trigger == 0 and len(stale_b2b_sql) > 0:
        alerts.append(
            f"Recycler B2B trigger match = 0, but {len(stale_b2b_sql)} B2B SQLs are "
            f"untouched {RECYCLE_DAYS}+ days — the workflow keys on "
            f"'{RECYCLER_TRIGGER_STATUS}', a status the stale B2B SQLs no longer carry, "
            f"so it recycles none of them.")
    if nonb2b_trigger > 0:
        detail = ", ".join(f"{k} {v}" for k, v in sorted(trigger_by_type.items())
                           if k != "B2B")
        alerts.append(
            f"Recycler would recycle {nonb2b_trigger} NON-B2B contact(s) ({detail}) — "
            f"the workflow should not touch Partner / Internal / B2C.")
    if len(bounce) > 0:
        alerts.append(
            f"{len(bounce)} recycled B2B contact(s) are back at SQL (bounce-back) — "
            f"something re-promotes them after recycling (suspected lifecycle sync from "
            f"an associated company/deal still at SQL/Opportunity).")

    return {
        "run_dt": run_dt.isoformat(),
        "window_days": window,
        "alerts": alerts,
        "headline": {
            "deals_moved_this_week": len(moved),
            "open_idle14_pct": pct(open_idle14, open_qty),
            "open_idle14_count": open_idle14,
            "open_deal_qty": open_qty,
            "sql_untouched_30_pct": pct(sql_idle30, sql_total),
            "sql_untouched_30_count": sql_idle30,
            "sql_total": sql_total,
            "miscategorised": len(misc),
            "new_accounts_no_owner": sum(1 for a in incomplete_accounts if a["missing_owner"]),
            "deals_missing_usd": len(deals_missing_usd),
            "recycler_bounce_back": len(bounce),
        },
        "section1": {
            "open_by_stage": open_by_stage,
            "open_qty": open_qty, "open_usd": open_usd, "open_idle14": open_idle14,
            "closed_ref": closed_ref,
            "open_by_owner": open_by_owner,
            "top_owner": top_owner, "top_owner_share": top_owner_share,
            "largest_deal": largest_deal, "largest_deal_share": largest_deal_share,
            "moved": moved, "updated": updated,
        },
        "section2": {
            "new_companies_by_owner": {oid: [
                {"id": str(c["id"]), "name": c.get("name") or "(blank)",
                 "created": (c.get("createdate") or "")[:10]} for c in rows]
                for oid, rows in newco_by_owner.items()},
            "new_companies_total": len(new_companies),
            "new_contacts_total": len(new_contacts),
            "new_on_existing": [_c(c) for c in new_on_existing],
            "new_on_new_accounts": [_c(c) for c in new_on_new],
            "new_on_existing_by_owner_lifecycle": _grp_ol(new_on_existing, owners),
            "orphans": [_c(c) for c in orphan_new],
            "sql_rows": sql_rows,
            "sql_total": sql_total, "sql_idle30": sql_idle30, "sql_idle60": sql_idle60,
        },
        "section3": {
            "incomplete_accounts": incomplete_accounts,
            "deals_missing_usd": deals_missing_usd,
            "miscategorised_by_owner": {oid: [
                {"id": str(c["id"]), "name": c.get("name"),
                 "lifecyclestage": c.get("lifecyclestage"),
                 "hs_lead_status": status_label(c.get("hs_lead_status"))}
                for c in rows] for oid, rows in misc_by_owner.items()},
            "miscategorised_total": len(misc),
        },
        "section4": {
            "trigger_by_type": trigger_by_type,
            "b2b_trigger": b2b_trigger, "nonb2b_trigger": nonb2b_trigger,
            "stale_b2b_sql_90": len(stale_b2b_sql),
            "backlog_by_status": {s: [{"id": str(c["id"]), "name": c.get("name"),
                                       "owner_id": c.get("owner_id") or ""}
                                      for c in rows]
                                  for s, rows in backlog_by_status.items()},
            "backlog_missed_total": len(backlog_missed),
            "recycled_by_type": {t: [{"id": str(c["id"]), "name": c.get("name"),
                                      "lifecyclestage": c.get("lifecyclestage"),
                                      "recycled_date": (c.get("recycled_date") or "")[:10]}
                                     for c in rows]
                                 for t, rows in recycled_by_type.items()},
            "recycled_total": len(recycled_recent),
            "bounce_by_owner": {oid: [{"id": str(c["id"]), "name": c.get("name"),
                                       "hs_lead_status": status_label(c.get("hs_lead_status"))}
                                      for c in rows]
                                for oid, rows in bounce_by_owner.items()},
            "bounce_by_status": {s: len(rows) for s, rows in bounce_by_status.items()},
            "bounce_total": len(bounce),
            "bounce_all_types": bounce_all_types,
        },
        "owners": owners,
    }


def _c(c):
    return {"id": str(c["id"]), "name": c.get("name"),
            "lifecyclestage": c.get("lifecyclestage"),
            "owner_id": c.get("owner_id") or "",
            "associatedcompanyid": c.get("associatedcompanyid") or ""}


def _grp_ol(rows, owners):
    out = {}
    for c in rows:
        oid = c.get("owner_id") or ""
        out.setdefault(oid, {}).setdefault(c.get("lifecyclestage") or "unknown", 0)
        out[oid][c.get("lifecyclestage") or "unknown"] += 1
    return out


# ===========================================================================
# Week-on-week
# ===========================================================================

def load_prev(snap_dir, run_date):
    if not os.path.isdir(snap_dir):
        return None
    cand = []
    for name in os.listdir(snap_dir):
        if not name.endswith(".json"):
            continue
        try:
            d = dt.date.fromisoformat(name[:-5])
        except ValueError:
            continue
        if d < run_date:
            cand.append((d, name))
    if not cand:
        return None
    cand.sort()
    with open(os.path.join(snap_dir, cand[-1][1])) as f:
        return json.load(f)


def d_int(cur, prev_wow, key):
    if not prev_wow or key not in prev_wow:
        return str(cur)
    diff = cur - prev_wow[key]
    sign = f"+{diff}" if diff > 0 else (str(diff) if diff < 0 else "=")
    return f"{cur} ({sign})"


def d_money(cur, prev_wow, key):
    if not prev_wow or key not in prev_wow:
        return f"${cur:,.0f}"
    diff = cur - prev_wow[key]
    sign = f"+${diff:,.0f}" if diff > 0 else (f"-${abs(diff):,.0f}" if diff < 0 else "=")
    return f"${cur:,.0f} ({sign})"


# ===========================================================================
# Report rendering
# ===========================================================================

def render(m, run_date, generated, prev):
    prev_wow = (prev or {}).get("wow") if prev else None
    o = []
    A = o.append
    s1, s2, s3, s4 = m["section1"], m["section2"], m["section3"], m["section4"]
    h = m["headline"]

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {run_date}")
    A("")
    A(f"*Scope: B2B only (contacts `customer_type = B2B`, companies "
      f"`customer_type__cloned_ = B2B`). Contact counts from the CONTACT object; "
      f"pipeline value from DEALS (USD via `amount_in_home_currency`). "
      f"Generated {generated} UTC from HubSpot (MCP SQL layer).*")
    A("")
    if prev:
        note = "" if prev_wow else (" — prior snapshot predates this report format, "
                                    "so most week-on-week deltas start next run")
        A(f"*Week-on-week deltas vs `{prev['run_date']}`{note}.*")
    else:
        A("*No prior snapshot — week-on-week comparison starts next run.*")
    A("")

    # ---- Headline flags ----------------------------------------------------
    A("## Headline flags")
    A("")
    A(f"- **Deals moved stage this week:** {d_int(h['deals_moved_this_week'], prev_wow, 'deals_moved_this_week')}")
    A(f"- **Open deals idle 14+d:** {h['open_idle14_pct']}% "
      f"({h['open_idle14_count']} of {h['open_deal_qty']} open)")
    A(f"- **SQLs untouched 30+d:** {h['sql_untouched_30_pct']}% "
      f"({h['sql_untouched_30_count']} of {h['sql_total']} SQL contacts)")
    A(f"- **Miscategorised leads:** {d_int(h['miscategorised'], prev_wow, 'miscategorised')}")
    A(f"- **New accounts with no owner:** {d_int(h['new_accounts_no_owner'], prev_wow, 'new_accounts_no_owner')}")
    A(f"- **Open deals missing USD value:** {d_int(h['deals_missing_usd'], prev_wow, 'deals_missing_usd')}")
    A(f"- **Recycler bounce-backs (recycled but back at SQL):** "
      f"{d_int(h['recycler_bounce_back'], prev_wow, 'recycler_bounce_back')}")
    A("")

    if m["alerts"]:
        A("### 🔔 Alerts")
        A("")
        for a in m["alerts"]:
            A(f"- ⚠️ {a}")
        A("")

    # ---- SECTION 1 ---------------------------------------------------------
    A("# Section 1 — Current pipeline status")
    A("")
    A("## 1.1 Open pipeline by stage")
    A("")
    A("*B2B, open deals only. USD = `amount_in_home_currency`. "
      "Idle = days since last activity (`notes_last_updated`).*")
    A("")
    A("| Stage | Deals | Total USD | Median days idle | Idle ≥14d |")
    A("|-------|------:|----------:|-----------------:|----------:|")
    for s in s1["open_by_stage"]:
        med = "—" if s["median_idle"] is None else f"{s['median_idle']}d"
        A(f"| {s['stage']} | {s['qty']} | ${s['usd']:,.0f} | {med} | {s['idle_14plus']} |")
    A(f"| **Open subtotal** | **{s1['open_qty']}** | **${s1['open_usd']:,.0f}** | | **{s1['open_idle14']}** |")
    A("")
    A("*Closed (reference only, not in the open subtotal):*")
    A("")
    A("| Stage | Deals | Total USD |")
    A("|-------|------:|----------:|")
    for c in s1["closed_ref"]:
        A(f"| {c['stage']} | {c['qty']} | ${c['usd']:,.0f} |")
    A("")

    A("## 1.2 Open pipeline by sales owner")
    A("")
    A("| Owner | Open deals | Open USD |")
    A("|-------|-----------:|---------:|")
    for r in s1["open_by_owner"]:
        A(f"| {r['owner']} | {r['qty']} | ${r['usd']:,.0f} |")
    A(f"| **Total** | **{s1['open_qty']}** | **${s1['open_usd']:,.0f}** |")
    A("")
    if s1["top_owner"]:
        A(f"**Concentration:** {s1['top_owner']['owner']} holds "
          f"${s1['top_owner']['usd']:,.0f} ({s1['top_owner_share']}%) of open value. "
          f"Largest single open deal: {s1['largest_deal']['dealname']} "
          f"(${s1['largest_deal']['usd']:,.0f}, {s1['largest_deal_share']}% of open value, "
          f"{s1['largest_deal']['owner']}).")
        A("")

    A("## 1.3 Updated / moved in the last 7 days")
    A("")
    A("*STAGE MOVED = entered current stage within the window "
      "(`hs_v2_date_entered_current_stage`). UPDATED = modified within the window with "
      "no stage change. Previous stage is not shown this run (no prior per-deal stage "
      "snapshot yet); it becomes available next run.*")
    A("")
    if s1["moved"]:
        A(f"**Stage moved ({len(s1['moved'])})**")
        A("")
        A("| Deal | Owner | Current stage | USD | Entered stage | Last activity |")
        A("|------|-------|---------------|----:|---------------|---------------|")
        for d in s1["moved"]:
            A(f"| {d['dealname']} | {d['owner']} | {d['stage']} | ${d['usd']:,.0f} "
              f"| {d['entered_stage']} | {d['last_activity']} |")
        A("")
    else:
        A("- No deals changed stage in the window.")
        A("")
    if s1["updated"]:
        A(f"**Updated, no stage change ({len(s1['updated'])})**")
        A("")
        A("| Deal | Owner | Stage | USD | Last activity |")
        A("|------|-------|-------|----:|---------------|")
        for d in s1["updated"]:
            A(f"| {d['dealname']} | {d['owner']} | {d['stage']} | ${d['usd']:,.0f} "
              f"| {d['last_activity']} |")
        A("")
    else:
        A("- No field-only updates in the window.")
        A("")

    # ---- SECTION 2 ---------------------------------------------------------
    A("# Section 2 — New pipeline status (top of funnel)")
    A("")
    A(f"## 2.1 New companies created in last 7 days — {d_int(s2['new_companies_total'], prev_wow, 'new_companies')}")
    A("")
    A("| Owner | New companies |")
    A("|-------|--------------:|")
    ncbo = s2["new_companies_by_owner"]
    for oid in sorted(ncbo, key=lambda k: -len(ncbo[k])):
        A(f"| {owner_name(oid, m['owners'])} | {len(ncbo[oid])} |")
    A(f"| **Total** | **{s2['new_companies_total']}** |")
    A("")
    unassigned_ct = len(ncbo.get("", []))
    if unassigned_ct:
        A(f"⚠️ **{unassigned_ct} of {s2['new_companies_total']} new companies have no owner** "
          f"(see worklist 3A).")
        A("")

    A(f"## 2.2 New contacts on EXISTING companies — {len(s2['new_on_existing'])}")
    A("")
    A(f"*Of {s2['new_contacts_total']} new B2B contacts this week: "
      f"{len(s2['new_on_existing'])} on existing accounts, "
      f"{len(s2['new_on_new_accounts'])} on brand-new accounts, "
      f"{len(s2['orphans'])} with no company (2.3).*")
    A("")
    grp = s2["new_on_existing_by_owner_lifecycle"]
    if grp:
        A("| Owner | Lifecycle stage | New contacts |")
        A("|-------|-----------------|-------------:|")
        for oid in grp:
            for stage, n in grp[oid].items():
                A(f"| {owner_name(oid, m['owners'])} | {stage} | {n} |")
        A("")
    else:
        A("- None this week.")
        A("")
    A(f"New contacts on NEW accounts (associated company created this week): "
      f"**{len(s2['new_on_new_accounts'])}**.")
    A("")

    A(f"## 2.3 FLAG — new contacts not associated to any company (orphans): "
      f"{d_int(len(s2['orphans']), prev_wow, 'orphans')}")
    A("")
    if s2["orphans"]:
        A("| Contact | Owner |")
        A("|---------|-------|")
        for c in s2["orphans"]:
            A(f"| {c['name']} (#{c['id']}) | {owner_name(c['owner_id'], m['owners'])} |")
        A("")
    else:
        A("- None this week.")
        A("")

    A("## 2.4 SQLs by sales owner")
    A("")
    A("*True SQL total — B2B contacts at the Sales Qualified Lead stage (contacts only).*")
    A("")
    A("| Owner | SQLs | Not touched 30+d | Not touched 60+d |")
    A("|-------|-----:|-----------------:|-----------------:|")
    for r in s2["sql_rows"]:
        A(f"| {r['owner']} | {r['qty']} | {r['idle30']} | {r['idle60']} |")
    A(f"| **Total** | **{d_int(s2['sql_total'], prev_wow, 'sql_total')}** "
      f"| **{d_int(s2['sql_idle30'], prev_wow, 'sql_idle30')}** "
      f"| **{d_int(s2['sql_idle60'], prev_wow, 'sql_idle60')}** |")
    A("")

    # ---- SECTION 3 ---------------------------------------------------------
    A("# Section 3 — Admin / data-hygiene worklist")
    A("")
    A("*Actionable fix-list. Record IDs are in the snapshot JSON so each becomes a "
      "clickable list to work from.*")
    A("")
    A(f"## 3A New accounts to complete — {len(s3['incomplete_accounts'])}")
    A("")
    A("*B2B companies created this week missing an OWNER and/or a NAME.*")
    A("")
    if s3["incomplete_accounts"]:
        A("| Company ID | Created | Owner | Name |")
        A("|------------|---------|-------|------|")
        for a in s3["incomplete_accounts"]:
            A(f"| {a['id']} | {a['created']} | {a['owner']} | {a['name']} |")
        A("")
    else:
        A("- None this week.")
        A("")

    A(f"## 3B Open deals missing a USD value — {len(s3['deals_missing_usd'])}")
    A("")
    A("*Open B2B deals with empty `amount_in_home_currency` — these silently distort "
      "pipeline value and are never back-filled from raw amount.*")
    A("")
    if s3["deals_missing_usd"]:
        A("| Deal | Stage | Owner |")
        A("|------|-------|-------|")
        for d in s3["deals_missing_usd"]:
            A(f"| {d['dealname']} (#{d['id']}) | {d['stage']} | {d['owner']} |")
        A("")
    else:
        A("- None this run.")
        A("")

    A(f"## 3C Miscategorised leads — {d_int(s3['miscategorised_total'], prev_wow, 'miscategorised')}")
    A("")
    A("*B2B contacts whose lead status is a sales-working status "
      f"({' / '.join(SALES_WORKING_STATUSES)}) but whose lifecycle stage is still only "
      "Lead / MQL — they should be SQL or beyond.*")
    A("")
    mbo = s3["miscategorised_by_owner"]
    if mbo:
        A("| Owner | Count | Statuses |")
        A("|-------|------:|----------|")
        for oid in sorted(mbo, key=lambda k: -len(mbo[k])):
            statuses = {}
            for c in mbo[oid]:
                statuses[c["hs_lead_status"]] = statuses.get(c["hs_lead_status"], 0) + 1
            sdesc = ", ".join(f"{k} ×{v}" for k, v in statuses.items())
            A(f"| {owner_name(oid, m['owners'])} | {len(mbo[oid])} | {sdesc} |")
        A(f"| **Total** | **{s3['miscategorised_total']}** | |")
        A("")
    else:
        A("- None this run.")
        A("")

    # ---- SECTION 4 ---------------------------------------------------------
    A("# Section 4 — Recycler health check")
    A("")
    A("*The stale-SQL recycler is meant to move SQL→MQL after 90 days of no activity "
      "(sets `recycled_date`, `unqualified_reason`, enrols in the re-engagement nurture). "
      "This section diagnoses whether it is working; it does not change the workflow.*")
    A("")
    A(f"## 4A Trigger match now — keyed on `{RECYCLER_TRIGGER_STATUS}` + {RECYCLE_DAYS}d idle")
    A("")
    A("| Customer type | Contacts the trigger would catch now |")
    A("|---------------|-------------------------------------:|")
    tbt = s4["trigger_by_type"]
    if tbt:
        for t in sorted(tbt):
            A(f"| {t} | {tbt[t]} |")
    else:
        A("| (none) | 0 |")
    A("")
    A(f"- **B2B trigger match: {s4['b2b_trigger']}** — while **{s4['stale_b2b_sql_90']} "
      f"B2B SQLs are untouched {RECYCLE_DAYS}+ days**.")
    if s4["b2b_trigger"] == 0 and s4["stale_b2b_sql_90"] > 0:
        A(f"  - ⚠️ **The recycler catches none of the stale B2B backlog** — it is scoped "
          f"to a status the stale B2B SQLs don't carry.")
    if s4["nonb2b_trigger"] > 0:
        A(f"  - ⚠️ **{s4['nonb2b_trigger']} non-B2B match(es)** — the recycler would recycle "
          f"Partner / Internal / B2C contacts it should leave alone.")
    A("")

    A(f"## 4B Stale B2B SQL backlog the trigger MISSES — {s4['backlog_missed_total']}")
    A("")
    A(f"*B2B SQLs untouched {RECYCLE_DAYS}+ days that are NOT `{RECYCLER_TRIGGER_STATUS}` — "
      "the population the recycler should catch but doesn't.*")
    A("")
    bbs = s4["backlog_by_status"]
    if bbs:
        A("| Lead status | Count |")
        A("|-------------|------:|")
        for s in sorted(bbs, key=lambda k: -len(bbs[k])):
            A(f"| {s} | {len(bbs[s])} |")
        A(f"| **Total** | **{s4['backlog_missed_total']}** |")
        A("")

    A(f"## 4C Recycled this week — {d_int(s4['recycled_total'], prev_wow, 'recycled_this_week')}")
    A("")
    A("*Contacts whose `recycled_date` falls in the last 7 days (proof the workflow fired).*")
    A("")
    rbt = s4["recycled_by_type"]
    if rbt:
        A("| Customer type | Count | Contacts |")
        A("|---------------|------:|----------|")
        for t in rbt:
            names = ", ".join(f"{c['name']} (#{c['id']}, {c['lifecyclestage']})"
                              for c in rbt[t])
            A(f"| {t} | {len(rbt[t])} | {names} |")
        A("")
    else:
        A("- Nothing recycled this week.")
        A("")

    A(f"## 4D Bounce-back — {d_int(s4['bounce_total'], prev_wow, 'bounce_back')}")
    A("")
    A("*Contacts with `recycled_date` set AND lifecycle back at Sales Qualified Lead — "
      "they were recycled, then returned to SQL. A non-zero count means something "
      "re-promotes them (suspect: lifecycle sync from an associated company/deal still at "
      "SQL/Opportunity).*")
    A("")
    if s4["bounce_total"]:
        A(f"**{s4['bounce_total']} B2B bounce-backs** ({s4['bounce_all_types']} across all "
          f"customer types).")
        A("")
        A("By lead status: " + ", ".join(f"{k} ×{v}" for k, v in
                                          sorted(s4["bounce_by_status"].items(),
                                                 key=lambda kv: -kv[1])) + ".")
        A("")
        A("| Owner | Bounce-backs |")
        A("|-------|-------------:|")
        bbo = s4["bounce_by_owner"]
        for oid in sorted(bbo, key=lambda k: -len(bbo[k])):
            A(f"| {owner_name(oid, m['owners'])} | {len(bbo[oid])} |")
        A("")
    else:
        A("- No bounce-backs.")
        A("")

    A("---")
    A(f"*Snapshot: `snapshots/{run_date}.json` · Report: `reports/{run_date}.md`. "
      "Section 4 diagnoses the recycler; re-scoping the enrolment and fixing the "
      "re-promotion loop are separate HubSpot workflow changes.*")
    A("")
    return "\n".join(o)


# ===========================================================================
# Snapshot / week-on-week values
# ===========================================================================

def build_wow(m):
    h = m["headline"]
    s2, s4 = m["section2"], m["section4"]
    return {
        "deals_moved_this_week": h["deals_moved_this_week"],
        "miscategorised": h["miscategorised"],
        "new_accounts_no_owner": h["new_accounts_no_owner"],
        "deals_missing_usd": h["deals_missing_usd"],
        "recycler_bounce_back": h["recycler_bounce_back"],
        "sql_total": s2["sql_total"],
        "sql_idle30": s2["sql_idle30"],
        "sql_idle60": s2["sql_idle60"],
        "new_companies": s2["new_companies_total"],
        "new_on_existing": len(s2["new_on_existing"]),
        "orphans": len(s2["orphans"]),
        "recycled_this_week": s4["recycled_total"],
        "bounce_back": s4["bounce_total"],
        "open_qty": m["section1"]["open_qty"],
        "open_usd": m["section1"]["open_usd"],
    }


# ===========================================================================
# Main
# ===========================================================================

def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly B2B pipeline report")
    ap.add_argument("--input", required=True, help="Pre-pulled raw JSON")
    ap.add_argument("--date", help="Run date YYYY-MM-DD (default: from generated_at)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
    args = ap.parse_args()

    with open(args.input) as f:
        raw = json.load(f)

    run_dt = parse_ts(raw.get("generated_at")) or dt.datetime.now(dt.timezone.utc)
    run_date = dt.date.fromisoformat(args.date) if args.date else run_dt.date()

    # ---- Guard: B2B company filter must not be silently failing -------------
    rc = raw.get("reference_counts", {})
    cb2b, call = rc.get("company_b2b"), rc.get("company_all")
    if cb2b is not None:
        if cb2b == 0:
            sys.exit("GUARD: B2B company count is 0 — silent-fail signature on the "
                     "customer_type__cloned_ filter. Aborting.")
        if call is not None and cb2b == call:
            sys.exit("GUARD: B2B company count equals the full unfiltered count "
                     f"({cb2b} == {call}) — filter silently returned everything. Aborting.")

    m = compute(raw, run_dt)

    generated = run_dt.strftime("%Y-%m-%d %H:%M:%S")
    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev(snap_dir, run_date)

    snapshot = {
        "run_date": run_date.isoformat(),
        "generated_at_utc": generated,
        "source": raw.get("source", "hubspot (pre-pulled)"),
        "window_days": raw.get("window_days", WINDOW_DAYS),
        "reference_counts": rc,
        "metrics": m,
        "wow": build_wow(m),
    }

    snap_path = os.path.join(snap_dir, f"{run_date.isoformat()}.json")
    with open(snap_path, "w") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    report = render(m, run_date.isoformat(), generated, prev)
    rep_path = os.path.join(rep_dir, f"{run_date.isoformat()}.md")
    with open(rep_path, "w") as f:
        f.write(report)

    print(f"Wrote {snap_path}")
    print(f"Wrote {rep_path}")
    print(f"Compared against: {prev['run_date']}" if prev else "No previous snapshot.")


if __name__ == "__main__":
    main()
