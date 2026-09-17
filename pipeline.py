#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health routine.

Consumes a pre-pulled ``raw_input.json`` (data fetched from HubSpot via the
HubSpot MCP tools -- direct egress to api.hubapi.com is blocked in the run
environment) and writes, per run:

  snapshots/YYYY-MM-DD.json  -- pulled data + computed metrics + worklist ids
  reports/YYYY-MM-DD.md      -- Sections 1-4 (see below)

Run:  python3 pipeline.py --input raw_input.json   (--input is REQUIRED)

================= SCOPING / JOIN / FIELD RULES (honoured here) =================
* B2B only. Contacts scoped by customer_type=B2B; companies by
  customer_type__cloned_=B2B (customer_type on a COMPANY silently fails).
* Two bases, never mixed. Contact/SQL counts come from the CONTACT object only
  (sql_all_types / contacts_b2b). Pipeline value comes from DEALS scoped to B2B
  companies. Companies never feed contact/SQL counts.
* Deal USD value = amount_in_home_currency (NOT amount, which mixes INR/USD).
  A deal with an empty amount_in_home_currency is flagged (3B), never back-filled
  from raw amount.
* Contact->company joins are done here from pulled scalars (associatedcompanyid
  on the contact + id/createdate on the company), because contact->company
  cross-object filters silently fail in the MCP SQL layer.
"""

import argparse
import datetime as dt
import json
import os
import statistics
import sys

# =========================================================================
# EDITABLE CONSTANTS
# =========================================================================
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
    "Initial Engagement", "Qualified to Buy", "Discovery & Trial",
    "Quote Sent", "Commitment to Buy", "Negotiation",
]
CLOSED_STAGES = {"closedwon", "closedlost"}

# hs_lead_status values that mean "sales is actively working this contact".
# NOTE: these are HubSpot INTERNAL values. "Contacted" is stored internally as
# "CONNECTED"; "Attempted to Contact" is stored as itself. (Verified against the
# portal's hs_lead_status enum.) Only values that actually exist are listed.
SALES_WORKING_STATUSES = ["Attempted to Contact", "CONNECTED"]
LEAD_STATUS_LABELS = {
    "NEW": "New Lead", "Marketing Engaged": "Marketing Engaged", "MQL": "MQL",
    "Attempted to Contact": "Attempted to Contact", "CONNECTED": "Contacted",
    "Qualified": "Qualified", "IN_PROGRESS": "In Progress",
    "PROPOSAL_REQUEST": "Proposal Request", "UNQUALIFIED": "Unqualified",
    "FUTURE_LEAD": "Future Lead", "Onboarded Partner": "Onboarded Partner",
}
# The status the live 90-day stale-SQL recycler workflow keys on today.
RECYCLER_TRIGGER_STATUS = "Attempted to Contact"

MISCATEGORISED_LIFECYCLE_STAGES = {"lead", "marketingqualifiedlead"}

WINDOW_DAYS = 7
IDLE_BUCKETS = [30, 60]
QTB_STALE_DAYS = 14        # open-deal "idle" threshold (Section 1.1 / headline)
RECYCLE_DAYS = 90          # stale-SQL threshold the recycler should act on

B2B = "B2B"


# =========================================================================
# Helpers
# =========================================================================
def parse_date(value):
    """HubSpot ISO timestamp -> date, or None."""
    if not value:
        return None
    v = str(value).replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(v).date()
    except ValueError:
        try:
            return dt.date.fromisoformat(str(value)[:10])
        except ValueError:
            return None


def to_float(value):
    if value in (None, "", "Unassigned"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def days_since(ref_date, ts):
    """Whole days from ts (ISO) to ref_date; None if ts missing."""
    d = parse_date(ts)
    if d is None:
        return None
    return (ref_date - d).days


def owner_name(owners, owner_id):
    if owner_id in (None, "", "Unassigned"):
        return "Unassigned"
    return owners.get(str(owner_id), f"Owner {owner_id}")


def money(x):
    return f"${x:,.0f}" if x is not None else "—"


def delta(cur, prev):
    if prev is None:
        return f"{cur}"
    d = cur - prev
    if d > 0:
        return f"{cur} (+{d})"
    if d < 0:
        return f"{cur} ({d})"
    return f"{cur} (=)"


# =========================================================================
# Computation
# =========================================================================
def compute(raw, run_date):
    owners = raw.get("owners", {})
    window_start = run_date - dt.timedelta(days=WINDOW_DAYS)
    guard_alerts = []

    # ---- Guard: B2B company filter must not silently fail ------------------
    companies = raw.get("companies_b2b", [])
    ref = raw.get("portal_totals_reference", {})
    b2b_total = ref.get("b2b_companies_total")
    if b2b_total in (0, None) or b2b_total == ref.get("companies_total_unfiltered"):
        guard_alerts.append(
            "Company B2B filter returned 0 or the full unfiltered count -- "
            "possible silent-fail on customer_type__cloned_.")

    # =====================================================================
    # SECTION 1 -- CURRENT PIPELINE STATUS (deals, B2B companies)
    # =====================================================================
    deals = raw.get("deals_b2b", [])
    for d in deals:
        d["_stage_label"] = DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage"))
        d["_open"] = d.get("dealstage") not in CLOSED_STAGES
        d["_usd"] = to_float(d.get("amount_in_home_currency"))
        d["_idle"] = days_since(run_date, d.get("notes_last_updated"))
        d["_owner"] = owner_name(owners, d.get("owner_id"))

    open_deals = [d for d in deals if d["_open"]]

    # 1.1 by stage
    by_stage = []
    for label in STAGE_ORDER + ["Closed Won", "Closed Lost"]:
        pool = deals if label in ("Closed Won", "Closed Lost") else open_deals
        ds = [d for d in pool if d["_stage_label"] == label]
        idles = [d["_idle"] for d in ds if d["_idle"] is not None]
        by_stage.append({
            "stage": label,
            "qty": len(ds),
            "usd": round(sum(d["_usd"] or 0 for d in ds), 2),
            "median_idle": int(statistics.median(idles)) if idles else None,
            "idle_14plus": sum(1 for d in ds if d["_idle"] is not None and d["_idle"] >= QTB_STALE_DAYS),
            "reference_only": label in ("Closed Won", "Closed Lost"),
        })
    open_qty = len(open_deals)
    open_usd = round(sum(d["_usd"] or 0 for d in open_deals), 2)
    open_idle_14 = sum(1 for d in open_deals if d["_idle"] is not None and d["_idle"] >= QTB_STALE_DAYS)
    pct_open_idle_14 = round(100 * open_idle_14 / open_qty, 1) if open_qty else 0.0

    # 1.2 by owner
    owner_agg = {}
    for d in open_deals:
        e = owner_agg.setdefault(d["_owner"], {"qty": 0, "usd": 0.0})
        e["qty"] += 1
        e["usd"] += d["_usd"] or 0
    by_owner = sorted(
        [{"owner": o, "qty": v["qty"], "usd": round(v["usd"], 2)} for o, v in owner_agg.items()],
        key=lambda x: -x["usd"])
    # concentration: single largest open deal + largest owner share
    top_deal = max(open_deals, key=lambda d: d["_usd"] or 0, default=None)
    concentration = None
    if top_deal and open_usd:
        concentration = {
            "top_deal": top_deal.get("dealname"), "top_deal_usd": top_deal["_usd"],
            "top_deal_pct": round(100 * (top_deal["_usd"] or 0) / open_usd, 1),
            "top_owner": by_owner[0]["owner"] if by_owner else None,
            "top_owner_pct": round(100 * by_owner[0]["usd"] / open_usd, 1) if by_owner else None,
        }

    # 1.3 moved / updated in window
    moved, updated = [], []
    for d in open_deals + [x for x in deals if not x["_open"]]:
        entered = parse_date(d.get("hs_v2_date_entered_current_stage"))
        modified = parse_date(d.get("hs_lastmodifieddate"))
        rec = {"name": d.get("dealname"), "owner": d["_owner"], "stage": d["_stage_label"],
               "usd": d["_usd"], "last_activity": (d.get("notes_last_updated") or "")[:10] or "—",
               "entered": (d.get("hs_v2_date_entered_current_stage") or "")[:10] or "—"}
        if entered and entered >= window_start:
            moved.append(rec)
        elif modified and modified >= window_start:
            updated.append(rec)
    moved.sort(key=lambda r: -(r["usd"] or 0))
    updated.sort(key=lambda r: -(r["usd"] or 0))

    # =====================================================================
    # SECTION 2 -- NEW PIPELINE STATUS (top of funnel)
    # =====================================================================
    comp_by_id = {c["id"]: c for c in companies}
    contacts = raw.get("contacts_b2b", [])

    # 2.1 new B2B companies (createdate in window) by owner
    new_companies = [c for c in companies
                     if c.get("customer_type__cloned_") == B2B
                     and (parse_date(c.get("createdate")) or dt.date.min) >= window_start]
    new_co_by_owner = {}
    for c in new_companies:
        o = owner_name(owners, c.get("owner_id"))
        new_co_by_owner.setdefault(o, []).append(c["id"])

    # new B2B contacts (created in window)
    new_contacts = [c for c in contacts
                    if (parse_date(c.get("createdate")) or dt.date.min) >= window_start]

    # 2.2 new contacts on EXISTING vs NEW accounts (join on associatedcompanyid)
    on_existing, on_new_account, orphans = [], [], []
    for c in new_contacts:
        acid = c.get("associatedcompanyid")
        if not acid:
            orphans.append(c)
            continue
        co = comp_by_id.get(str(acid))
        co_created = parse_date(co.get("createdate")) if co else None
        if co_created is not None and co_created < window_start:
            on_existing.append(c)
        else:
            on_new_account.append(c)   # company new (or its createdate unknown)

    def group_owner_lifecycle(rows):
        g = {}
        for c in rows:
            o = owner_name(owners, c.get("owner_id"))
            ls = c.get("lifecyclestage") or "unknown"
            g.setdefault(o, {}).setdefault(ls, []).append(
                {"id": c["id"], "name": c.get("name")})
        return g
    existing_grouped = group_owner_lifecycle(on_existing)

    # 2.3 orphans by owner
    orphan_by_owner = {}
    for c in orphans:
        orphan_by_owner.setdefault(owner_name(owners, c.get("owner_id")), []).append(
            {"id": c["id"], "name": c.get("name")})

    # 2.4 SQLs by owner (B2B contacts only -- true SQL total)
    sqls = raw.get("sql_all_types", [])
    b2b_sqls = [s for s in sqls if s.get("customer_type") == B2B]
    sql_owner = {}
    for s in b2b_sqls:
        o = owner_name(owners, s.get("owner_id"))
        e = sql_owner.setdefault(o, {"qty": 0, "u30": 0, "u60": 0, "ids": []})
        e["qty"] += 1
        e["ids"].append(s["id"])
        idle = days_since(run_date, s.get("notes_last_updated"))
        if idle is None or idle >= 30:
            e["u30"] += 1
        if idle is None or idle >= 60:
            e["u60"] += 1
    sql_by_owner = sorted(
        [{"owner": o, **v} for o, v in sql_owner.items()], key=lambda x: -x["qty"])
    sql_total = len(b2b_sqls)
    sql_u30 = sum(v["u30"] for v in sql_owner.values())
    sql_u60 = sum(v["u60"] for v in sql_owner.values())

    # =====================================================================
    # SECTION 3 -- ADMIN / DATA-HYGIENE WORKLIST
    # =====================================================================
    # 3A new B2B companies missing owner and/or name
    admin_new_accounts = []
    for c in new_companies:
        missing_owner = c.get("owner_id") in (None, "", "Unassigned")
        missing_name = not (c.get("name") or "").strip()
        if missing_owner or missing_name:
            admin_new_accounts.append({
                "id": c["id"], "created": (c.get("createdate") or "")[:10],
                "owner": owner_name(owners, c.get("owner_id")) if not missing_owner else "none",
                "name": c.get("name") if not missing_name else "blank"})

    # 3B open B2B deals missing USD (amount_in_home_currency empty)
    deals_missing_usd = [{"id": d["id"], "dealname": d.get("dealname"),
                          "stage": d["_stage_label"], "owner": d["_owner"]}
                         for d in open_deals if d["_usd"] is None]
    closed_missing_usd = [{"id": d["id"], "dealname": d.get("dealname"),
                           "stage": d["_stage_label"], "owner": d["_owner"]}
                          for d in deals if not d["_open"] and d["_usd"] is None]

    # 3C miscategorised leads (working status but still lead/MQL)
    misc = [c for c in contacts
            if c.get("hs_lead_status") in SALES_WORKING_STATUSES
            and c.get("lifecyclestage") in MISCATEGORISED_LIFECYCLE_STAGES]
    misc_by_owner = {}
    for c in misc:
        o = owner_name(owners, c.get("owner_id"))
        e = misc_by_owner.setdefault(o, {"count": 0, "by_status": {}, "records": []})
        e["count"] += 1
        st = LEAD_STATUS_LABELS.get(c.get("hs_lead_status"), c.get("hs_lead_status"))
        e["by_status"][st] = e["by_status"].get(st, 0) + 1
        e["records"].append({"id": c["id"], "name": c.get("name"),
                             "status": st, "lifecyclestage": c.get("lifecyclestage")})

    # =====================================================================
    # SECTION 4 -- RECYCLER HEALTH CHECK
    # =====================================================================
    # stale = at SQL and untouched >= RECYCLE_DAYS (null activity = untouched)
    def stale(s):
        idle = days_since(run_date, s.get("notes_last_updated"))
        return idle is None or idle >= RECYCLE_DAYS

    # 4A trigger match now, split by customer_type
    trig_now = {}
    for s in sqls:
        if s.get("hs_lead_status") == RECYCLER_TRIGGER_STATUS and stale(s):
            ct = s.get("customer_type") or "Unassigned"
            trig_now.setdefault(ct, []).append(s["id"])
    trig_b2b = len(trig_now.get(B2B, []))
    trig_nonb2b = {k: v for k, v in trig_now.items() if k != B2B}

    stale_b2b_sqls = [s for s in b2b_sqls if stale(s)]
    recycler_alerts = []
    if trig_b2b == 0 and len(stale_b2b_sqls) > 0:
        recycler_alerts.append(
            f"Recycler trigger matches 0 B2B SQLs, but {len(stale_b2b_sqls)} B2B SQLs are "
            f"stale (untouched {RECYCLE_DAYS}+d). The workflow keys on "
            f"hs_lead_status='{RECYCLER_TRIGGER_STATUS}', which the stale backlog does not "
            f"carry, so it recycles none of them.")
    if trig_nonb2b:
        recycler_alerts.append(
            "Recycler trigger matches non-B2B contacts (should never recycle those): "
            + ", ".join(f"{k}={len(v)}" for k, v in trig_nonb2b.items()))

    # 4B stale B2B SQL backlog the trigger misses, grouped by lead status
    miss_by_status = {}
    for s in stale_b2b_sqls:
        if s.get("hs_lead_status") == RECYCLER_TRIGGER_STATUS:
            continue
        st = LEAD_STATUS_LABELS.get(s.get("hs_lead_status"), s.get("hs_lead_status") or "(none)")
        miss_by_status.setdefault(st, []).append(s["id"])

    # 4C recycled this week, by customer_type (from contacts recycled_date in window)
    recycled_week = [c for c in contacts
                     if (parse_date(c.get("recycled_date")) or dt.date.min) >= window_start
                     and c.get("recycled_date")]
    recycled_by_ct = {}
    for c in recycled_week:
        recycled_by_ct.setdefault(c.get("customer_type") or "Unassigned", []).append(c["id"])

    # 4D bounce-back: recycled_date set AND back at SQL (from B2B SQL base)
    bounce = [s for s in b2b_sqls if s.get("recycled_date")]
    bounce_by_owner, bounce_by_status = {}, {}
    for s in bounce:
        bounce_by_owner.setdefault(owner_name(owners, s.get("owner_id")), []).append(s["id"])
        st = LEAD_STATUS_LABELS.get(s.get("hs_lead_status"), s.get("hs_lead_status") or "(none)")
        bounce_by_status.setdefault(st, 0)
        bounce_by_status[st] += 1

    # =====================================================================
    # Headline flags
    # =====================================================================
    headline = {
        "deals_moved_this_week": len(moved),
        "pct_open_idle_14": pct_open_idle_14,
        "pct_sql_untouched_30": round(100 * sql_u30 / sql_total, 1) if sql_total else 0.0,
        "miscategorised": len(misc),
        "new_accounts_no_owner": sum(1 for c in new_companies
                                     if c.get("owner_id") in (None, "", "Unassigned")),
        "deals_missing_usd": len(deals_missing_usd),
        "recycler_bounceback": len(bounce),
    }

    return {
        "run_date": run_date.isoformat(),
        "window_start": window_start.isoformat(),
        "headline": headline,
        "guard_alerts": guard_alerts,
        # section 1
        "deals_by_stage": by_stage,
        "open_qty": open_qty, "open_usd": open_usd,
        "open_idle_14": open_idle_14, "pct_open_idle_14": pct_open_idle_14,
        "open_by_owner": by_owner, "concentration": concentration,
        "moved": moved, "updated": updated,
        # section 2
        "new_companies": [{"id": c["id"], "name": c.get("name"),
                           "owner": owner_name(owners, c.get("owner_id"))}
                          for c in new_companies],
        "new_companies_by_owner": {o: ids for o, ids in new_co_by_owner.items()},
        "new_contacts_count": len(new_contacts),
        "on_existing_count": len(on_existing),
        "on_new_account_count": len(on_new_account),
        "on_existing_grouped": existing_grouped,
        "orphans_count": len(orphans), "orphans_by_owner": orphan_by_owner,
        "sql_by_owner": sql_by_owner, "sql_total": sql_total,
        "sql_u30": sql_u30, "sql_u60": sql_u60,
        # section 3
        "admin_new_accounts": admin_new_accounts,
        "deals_missing_usd": deals_missing_usd,
        "closed_missing_usd": closed_missing_usd,
        "misc_by_owner": {o: v for o, v in misc_by_owner.items()},
        "misc_total": len(misc),
        # section 4
        "trigger_now_b2b": trig_b2b,
        "trigger_now_nonb2b": {k: len(v) for k, v in trig_nonb2b.items()},
        "stale_b2b_sql_count": len(stale_b2b_sqls),
        "recycler_alerts": recycler_alerts,
        "miss_by_status": {k: len(v) for k, v in miss_by_status.items()},
        "miss_by_status_ids": miss_by_status,
        "recycled_week_by_ct": {k: len(v) for k, v in recycled_by_ct.items()},
        "recycled_week_count": len(recycled_week),
        "bounce_count": len(bounce),
        "bounce_by_owner": {o: len(v) for o, v in bounce_by_owner.items()},
        "bounce_by_owner_ids": bounce_by_owner,
        "bounce_by_status": bounce_by_status,
    }


# =========================================================================
# Week-on-week
# =========================================================================
def load_prev(snap_dir, run_date):
    if not os.path.isdir(snap_dir):
        return None
    cands = []
    for name in os.listdir(snap_dir):
        if not name.endswith(".json"):
            continue
        try:
            d = dt.date.fromisoformat(name[:-5])
        except ValueError:
            continue
        if d < run_date:
            cands.append((d, name))
    if not cands:
        return None
    cands.sort()
    with open(os.path.join(snap_dir, cands[-1][1])) as f:
        snap = json.load(f)
    # Only comparable if the prior snapshot used this (v2) metric schema.
    if "metrics" in snap and "headline" in snap.get("metrics", {}):
        return snap
    return None


# =========================================================================
# Report rendering
# =========================================================================
def render(m, prev):
    pm = prev["metrics"] if prev else None
    A = []
    p = A.append
    rd = m["run_date"]

    def hd(key, cur):
        return delta(cur, pm["headline"][key] if pm else None)

    p(f"# Brahmarsive Weekly Pipeline & MQL Health — {rd}")
    p("")
    p("*Scope: B2B only. Contacts via `customer_type=B2B`; companies via "
      "`customer_type__cloned_=B2B`; deals scoped to B2B companies. Contact/SQL "
      "counts come from the CONTACT object; pipeline value from DEALS "
      "(`amount_in_home_currency`, USD). Two bases are never mixed.*")
    p("")
    if prev:
        p(f"*Week-on-week deltas vs `{prev['run_date']}`.*")
    else:
        p("*No prior snapshot in the current report schema — week-on-week "
          "comparison starts next run.*")
    p("")

    # Headline flags
    h = m["headline"]
    p("## 🔔 Headline flags")
    p("")
    p(f"- **Deals that changed stage this week:** {hd('deals_moved_this_week', h['deals_moved_this_week'])}")
    p(f"- **Open deals idle 14+ days:** {h['pct_open_idle_14']}% "
      f"({m['open_idle_14']} of {m['open_qty']})")
    p(f"- **SQLs untouched 30+ days:** {h['pct_sql_untouched_30']}% "
      f"({m['sql_u30']} of {m['sql_total']})")
    p(f"- **Miscategorised leads:** {hd('miscategorised', h['miscategorised'])}")
    p(f"- **New accounts with no owner:** {hd('new_accounts_no_owner', h['new_accounts_no_owner'])}")
    p(f"- **Open deals missing USD value:** {hd('deals_missing_usd', h['deals_missing_usd'])}")
    p(f"- **Recycler bounce-backs (recycled → back at SQL):** "
      f"{hd('recycler_bounceback', h['recycler_bounceback'])}")
    p("")
    for a in m["guard_alerts"]:
        p(f"> ⚠️ GUARD: {a}")
    if m["guard_alerts"]:
        p("")

    # ---------------- SECTION 1 ----------------
    p("# SECTION 1 — Current pipeline status")
    p("")
    p("### 1.1 Open pipeline by stage (B2B, open only)")
    p("")
    p("| Stage | Deals | Total USD | Median days idle | Idle ≥14d |")
    p("|-------|------:|----------:|-----------------:|----------:|")
    for s in m["deals_by_stage"]:
        if s["reference_only"]:
            continue
        mi = "—" if s["median_idle"] is None else f"{s['median_idle']}d"
        p(f"| {s['stage']} | {s['qty']} | {money(s['usd'])} | {mi} | {s['idle_14plus']} |")
    p(f"| **Open subtotal** | **{m['open_qty']}** | **{money(m['open_usd'])}** | | "
      f"**{m['open_idle_14']}** |")
    p("")
    p("*Reference only (not in open subtotal):*")
    p("")
    p("| Stage | Deals | Total USD |")
    p("|-------|------:|----------:|")
    for s in m["deals_by_stage"]:
        if s["reference_only"]:
            p(f"| {s['stage']} | {s['qty']} | {money(s['usd'])} |")
    p("")

    p("### 1.2 Open pipeline by sales owner")
    p("")
    p("| Owner | Open deals | Open USD |")
    p("|-------|-----------:|---------:|")
    for o in m["open_by_owner"]:
        p(f"| {o['owner']} | {o['qty']} | {money(o['usd'])} |")
    p(f"| **Total** | **{m['open_qty']}** | **{money(m['open_usd'])}** |")
    p("")
    c = m["concentration"]
    if c:
        p(f"**Concentration:** the single largest open deal — {c['top_deal']} "
          f"({money(c['top_deal_usd'])}) — is **{c['top_deal_pct']}%** of all open value. "
          f"Top owner **{c['top_owner']}** holds **{c['top_owner_pct']}%** of open value.")
        p("")

    p("### 1.3 Updated / moved in the last 7 days")
    p("")
    p(f"**Stage moved** (entered current stage since {m['window_start']}): "
      f"{len(m['moved'])} deal(s).")
    p("")
    if m["moved"]:
        p("| Deal | Owner | Current stage | USD | Entered | Last activity |")
        p("|------|-------|---------------|----:|---------|---------------|")
        for r in m["moved"]:
            p(f"| {r['name']} | {r['owner']} | {r['stage']} | {money(r['usd'])} "
              f"| {r['entered']} | {r['last_activity']} |")
        p("")
    p(f"**Updated** (field change, no stage move): {len(m['updated'])} deal(s).")
    p("")
    if m["updated"]:
        p("| Deal | Owner | Stage | USD | Last activity |")
        p("|------|-------|-------|----:|---------------|")
        for r in m["updated"]:
            p(f"| {r['name']} | {r['owner']} | {r['stage']} | {money(r['usd'])} "
              f"| {r['last_activity']} |")
        p("")

    # ---------------- SECTION 2 ----------------
    p("# SECTION 2 — New pipeline status (top of funnel)")
    p("")
    p("### 2.1 New companies created in the last 7 days — by owner")
    p("")
    if m["new_companies_by_owner"]:
        p("| Owner | New companies | Company IDs |")
        p("|-------|--------------:|-------------|")
        for o, ids in sorted(m["new_companies_by_owner"].items(), key=lambda x: -len(x[1])):
            flag = " ⚠️" if o == "Unassigned" else ""
            p(f"| {o}{flag} | {len(ids)} | {', '.join(ids)} |")
        p("")
        if "Unassigned" in m["new_companies_by_owner"]:
            p(f"> ⚠️ {len(m['new_companies_by_owner']['Unassigned'])} new B2B "
              f"compan(ies) have **no owner** — see 3A.")
            p("")
    else:
        p("- No new B2B companies created this week.")
        p("")

    p("### 2.2 New contacts on EXISTING companies — by owner, then lifecycle")
    p("")
    p(f"{m['new_contacts_count']} new B2B contacts this week: "
      f"**{m['on_existing_count']} on existing accounts**, "
      f"**{m['on_new_account_count']} on newly-created accounts**.")
    p("")
    if m["on_existing_grouped"]:
        p("| Owner | Lifecycle | Count | Contacts |")
        p("|-------|-----------|------:|----------|")
        for o, byls in sorted(m["on_existing_grouped"].items()):
            for ls, recs in byls.items():
                names = ", ".join(r["name"] for r in recs)
                p(f"| {o} | {ls} | {len(recs)} | {names} |")
        p("")
    else:
        p("- No new contacts landed on existing accounts this week.")
        p("")

    p("### 2.3 FLAG — new contacts not associated to any company (orphans)")
    p("")
    if m["orphans_count"] == 0:
        p("- ✅ 0 orphaned new contacts this week (every new B2B contact has an "
          "associated company).")
    else:
        p(f"**{m['orphans_count']} orphaned** new contacts (no `associatedcompanyid`):")
        p("")
        p("| Owner | Count | Contacts |")
        p("|-------|------:|----------|")
        for o, recs in m["orphans_by_owner"].items():
            p(f"| {o} | {len(recs)} | {', '.join(r['name'] for r in recs)} |")
    p("")

    p("### 2.4 SQLs by sales owner (B2B contacts — the true SQL total)")
    p("")
    p("| Owner | SQLs | Untouched 30+d | Untouched 60+d |")
    p("|-------|-----:|---------------:|---------------:|")
    for o in m["sql_by_owner"]:
        p(f"| {o['owner']} | {o['qty']} | {o['u30']} | {o['u60']} |")
    p(f"| **Total** | **{m['sql_total']}** | **{m['sql_u30']}** | **{m['sql_u60']}** |")
    p("")

    # ---------------- SECTION 3 ----------------
    p("# SECTION 3 — Admin / data-hygiene worklist")
    p("")
    p("### 3A New accounts to complete (new B2B companies missing owner and/or name)")
    p("")
    if m["admin_new_accounts"]:
        p("| Company ID | Created | Owner | Name |")
        p("|------------|---------|-------|------|")
        for c in m["admin_new_accounts"]:
            p(f"| {c['id']} | {c['created']} | {c['owner']} | {c['name']} |")
    else:
        p("- ✅ None this week.")
    p("")

    p("### 3B Deals missing a USD value (open B2B deals, `amount_in_home_currency` empty)")
    p("")
    if m["deals_missing_usd"]:
        p("| Deal | Stage | Owner |")
        p("|------|-------|-------|")
        for d in m["deals_missing_usd"]:
            p(f"| {d['dealname']} (#{d['id']}) | {d['stage']} | {d['owner']} |")
    else:
        p("- ✅ No open B2B deals missing USD.")
    p("")
    if m["closed_missing_usd"]:
        p(f"*Also {len(m['closed_missing_usd'])} closed deal(s) missing USD "
          f"(reference): " +
          ", ".join(f"{d['dealname']} (#{d['id']})" for d in m["closed_missing_usd"]) + ".*")
        p("")

    p("### 3C Miscategorised leads (working lead status but still Lead/MQL)")
    p("")
    p(f"**{m['misc_total']}** B2B contacts whose `hs_lead_status` is a sales-working "
      f"status ({', '.join(LEAD_STATUS_LABELS[s] for s in SALES_WORKING_STATUSES)}) but "
      f"whose lifecycle stage is still only Lead/MQL — they should be SQL or beyond.")
    p("")
    p("| Owner | Count | By status |")
    p("|-------|------:|-----------|")
    for o, v in sorted(m["misc_by_owner"].items(), key=lambda x: -x[1]["count"]):
        stx = ", ".join(f"{k}: {n}" for k, n in v["by_status"].items())
        p(f"| {o} | {v['count']} | {stx} |")
    p("")
    p("*Contact IDs per owner are in the snapshot JSON (`misc_by_owner`).*")
    p("")

    # ---------------- SECTION 4 ----------------
    p("# SECTION 4 — Recycler health check (is the 90-day workflow working?)")
    p("")
    p("The stale-SQL recycler is meant to move SQL→MQL after 90 days of no "
      "activity (set `recycled_date`, `unqualified_reason`, enrol in the "
      "re-engagement nurture). This section diagnoses it; it does **not** change it.")
    p("")
    for a in m["recycler_alerts"]:
        p(f"> 🚨 **ALERT:** {a}")
    if m["recycler_alerts"]:
        p("")

    p(f"### 4A Trigger match now — SQLs at `{RECYCLER_TRIGGER_STATUS}` and untouched "
      f"{RECYCLE_DAYS}+d, by customer type")
    p("")
    p(f"- **B2B:** {m['trigger_now_b2b']}")
    if m["trigger_now_nonb2b"]:
        for k, v in m["trigger_now_nonb2b"].items():
            p(f"- **{k}:** {v} (should be 0 — recycler must not touch non-B2B)")
    else:
        p("- **Non-B2B (Partner / Internal / B2C):** 0 ✅")
    p("")
    p(f"Stale B2B SQL backlog (untouched {RECYCLE_DAYS}+d, any status): "
      f"**{m['stale_b2b_sql_count']}**.")
    p("")

    p("### 4B Stale B2B SQL backlog the trigger MISSES (untouched 90+d, not the "
      "trigger status), by lead status")
    p("")
    if m["miss_by_status"]:
        p("| Lead status | Count |")
        p("|-------------|------:|")
        for k, v in sorted(m["miss_by_status"].items(), key=lambda x: -x[1]):
            p(f"| {k} | {v} |")
        p(f"| **Total** | **{sum(m['miss_by_status'].values())}** |")
        p("")
        p("*This is the population the recycler should be catching but isn't (IDs "
          "in `miss_by_status_ids`).*")
    else:
        p("- None.")
    p("")

    p("### 4C Recycled this week (`recycled_date` in the last 7 days), by customer type")
    p("")
    if m["recycled_week_by_ct"]:
        for k, v in m["recycled_week_by_ct"].items():
            p(f"- **{k}:** {v}")
    else:
        p("- None recycled this week.")
    p("")

    p("### 4D BOUNCE-BACK — recycled contacts that returned to SQL")
    p("")
    p(f"**{m['bounce_count']}** B2B contacts have a `recycled_date` set **and** are "
      f"back at `salesqualifiedlead`. A non-zero count means something re-promotes "
      f"them after the recycler demotes them — suspect a lifecycle sync from an "
      f"associated company/deal still at SQL/Opportunity. **This says whether the "
      f"recycle sticks — it currently does not.**")
    p("")
    if m["bounce_count"]:
        p("| Owner | Bounced back |")
        p("|-------|-------------:|")
        for o, n in sorted(m["bounce_by_owner"].items(), key=lambda x: -x[1]):
            p(f"| {o} | {n} |")
        p("")
        p("By lead status: " +
          ", ".join(f"{k}: {v}" for k, v in m["bounce_by_status"].items()) + ".")
        p("")

    p("---")
    p(f"*Snapshot: `snapshots/{rd}.json` · Report: `reports/{rd}.md`. Section 4 "
      f"diagnoses the recycler workflow; re-scoping enrolment and fixing the "
      f"re-promotion loop are separate HubSpot workflow changes.*")
    p("")
    return "\n".join(A)


# =========================================================================
# Main
# =========================================================================
def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly B2B pipeline report")
    ap.add_argument("--input", required=True, help="Pre-pulled raw JSON (REQUIRED)")
    ap.add_argument("--date", help="Run date YYYY-MM-DD (default: raw.as_of_date or today)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
    args = ap.parse_args()

    with open(args.input) as f:
        raw = json.load(f)

    if args.date:
        run_date = dt.date.fromisoformat(args.date)
    elif raw.get("as_of_date"):
        run_date = dt.date.fromisoformat(raw["as_of_date"])
    else:
        run_date = dt.datetime.now(dt.timezone.utc).date()

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev(snap_dir, run_date)
    metrics = compute(raw, run_date)

    snapshot = {
        "run_date": run_date.isoformat(),
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "source": raw.get("source"),
        "window_days": WINDOW_DAYS,
        "raw_counts": {
            "contacts_b2b": len(raw.get("contacts_b2b", [])),
            "sql_all_types": len(raw.get("sql_all_types", [])),
            "companies_b2b": len(raw.get("companies_b2b", [])),
            "deals_b2b": len(raw.get("deals_b2b", [])),
        },
        "portal_totals_reference": raw.get("portal_totals_reference"),
        "metrics": metrics,
    }

    snap_path = os.path.join(snap_dir, f"{run_date.isoformat()}.json")
    with open(snap_path, "w") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    report = render(metrics, prev)
    rep_path = os.path.join(rep_dir, f"{run_date.isoformat()}.md")
    with open(rep_path, "w") as f:
        f.write(report)

    print(f"Wrote {snap_path}")
    print(f"Wrote {rep_path}")
    print("Prev snapshot:", prev["run_date"] if prev else "none (comparison starts next run)")


if __name__ == "__main__":
    main()
