#!/usr/bin/env python3
"""Brahmarsive weekly B2B pipeline & MQL-health report.

Consumes a pre-pulled HubSpot dataset (``raw_input.json``) and writes:

  snapshots/YYYY-MM-DD.json  -- raw records + computed metrics + worklist IDs
  reports/YYYY-MM-DD.md      -- Sections 1-4 (see README / the routine spec)

Direct egress to api.hubapi.com is blocked in the run environment, so data is
pulled via the HubSpot MCP tools into raw_input.json and handed to this script.
Always run with --input:

    python3 pipeline.py --input raw_input.json

Scoping rules (non-negotiable):
  * B2B only. Contacts: customer_type = B2B. Companies: customer_type__cloned_ = B2B.
  * Two bases, never mixed. Contact/SQL counts come from the CONTACT object;
    pipeline value comes from DEALS (scoped B2B via the company), rolled up at
    company level. Companies never feed contact/SQL counts.
  * Deal USD value is amount_in_home_currency. Never sum raw amount across
    mixed currencies. Empty amount_in_home_currency -> admin worklist (3B).
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
STAGE_ORDER = [
    "Initial Engagement", "Qualified to Buy", "Discovery & Trial",
    "Quote Sent", "Commitment to Buy", "Negotiation",
]
CLOSED_LABELS = ["Closed Won", "Closed Lost"]
CLOSED_STAGES = {"closedwon", "closedlost"}

# hs_lead_status values that mean "sales is actively working this contact".
# NOTE: these are the INTERNAL stored values. In this portal the status whose
# label is "Contacted" is stored as "CONNECTED"; "Attempted to Contact" is
# stored verbatim. These two are the only working statuses that exist on
# lead/MQL B2B contacts today.
SALES_WORKING_STATUSES = ["Attempted to Contact", "CONNECTED"]
# What the live stale-SQL recycler workflow keys on today.
RECYCLER_TRIGGER_STATUS = "Attempted to Contact"

WINDOW_DAYS = 7
IDLE_BUCKETS = [30, 60]
QTB_STALE_DAYS = 14          # "idle" threshold for open deals
RECYCLE_DAYS = 90            # stale-SQL recycle threshold
MISCAT_LIFECYCLE_STAGES = {"lead", "marketingqualifiedlead"}

# Human labels for the lead-status internal values seen in this portal.
LEAD_STATUS_LABELS = {
    "NEW": "New Lead", "CONNECTED": "Contacted", "IN_PROGRESS": "In Progress",
    "PROPOSAL_REQUEST": "Proposal Request", "UNQUALIFIED": "Unqualified",
    "MQL": "MQL", "FUTURE_LEAD": "Future Lead",
    "Attempted to Contact": "Attempted to Contact", "Qualified": "Qualified",
}

# =========================================================================
# Helpers
# =========================================================================

def parse_ts(value):
    """Parse an ISO-8601 timestamp (HubSpot style) to an aware datetime."""
    if not value:
        return None
    v = str(value).strip()
    if not v:
        return None
    v = v.replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(v)
    except ValueError:
        # date-only fallback
        try:
            d = dt.datetime.fromisoformat(v[:10])
        except ValueError:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d


def days_since(value, now):
    d = parse_ts(value)
    if d is None:
        return None
    return (now - d).days


def status_label(s):
    if not s:
        return "(no status)"
    return LEAD_STATUS_LABELS.get(s, s)


def usd(x):
    return f"${x:,.0f}"


# =========================================================================
# Metric computation
# =========================================================================

def compute(raw):
    now = parse_ts(raw["generated_at"])
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    recycle_cutoff = now - dt.timedelta(days=RECYCLE_DAYS)
    owners = raw.get("owners", {})

    def oname(oid):
        if oid is None or str(oid).strip() == "":
            return "Unassigned"
        return owners.get(str(oid), f"Owner {oid}")

    contacts = raw["contacts_b2b"]
    sql_all = raw["sql_all_types"]
    deals = raw["deals_b2b"]
    companies_new = raw["companies_b2b"]
    assoc = {c["id"]: c for c in raw.get("associated_companies", [])}
    recycled_week = raw.get("recycled_week", [])

    m = {}

    # ---------------- Section 1: current pipeline (deals) ----------------
    for d in deals:
        d["_label"] = DEALSTAGE_LABELS.get(d["dealstage"], d.get("dealstage_label_raw") or d["dealstage"])
        d["_open"] = d["dealstage"] not in CLOSED_STAGES
        d["_usd"] = d["amount_in_home_currency"] if d["amount_in_home_currency"] is not None else 0
        d["_idle"] = days_since(d.get("notes_last_updated"), now)

    open_deals = [d for d in deals if d["_open"]]

    # 1.1 open by stage
    by_stage = []
    present = {}
    for d in open_deals:
        present.setdefault(d["_label"], []).append(d)
    for label in STAGE_ORDER:
        ds = present.get(label, [])
        idles = [d["_idle"] for d in ds if d["_idle"] is not None]
        by_stage.append({
            "stage": label,
            "qty": len(ds),
            "usd": sum(d["_usd"] for d in ds),
            "median_idle": int(statistics.median(idles)) if idles else None,
            "idle_ge_14": sum(1 for d in ds if d["_idle"] is None or d["_idle"] >= QTB_STALE_DAYS),
        })
    open_qty = len(open_deals)
    open_usd = sum(d["_usd"] for d in open_deals)
    open_idle_ge_14 = sum(1 for d in open_deals if d["_idle"] is None or d["_idle"] >= QTB_STALE_DAYS)
    m["pct_open_idle_14"] = round(100 * open_idle_ge_14 / open_qty) if open_qty else 0

    closed_ref = []
    for label in CLOSED_LABELS:
        ds = [d for d in deals if d["_label"] == label]
        closed_ref.append({"stage": label, "qty": len(ds), "usd": sum(d["_usd"] for d in ds)})

    m["s1_1"] = {"by_stage": by_stage, "open_qty": open_qty, "open_usd": open_usd,
                 "open_idle_ge_14": open_idle_ge_14, "closed_ref": closed_ref}

    # 1.2 open by owner
    by_owner = {}
    for d in open_deals:
        e = by_owner.setdefault(d["owner_id"], {"qty": 0, "usd": 0})
        e["qty"] += 1
        e["usd"] += d["_usd"]
    owner_rows = sorted(
        ({"owner": oname(oid), "owner_id": oid, "qty": e["qty"], "usd": e["usd"]}
         for oid, e in by_owner.items()),
        key=lambda r: -r["usd"])
    top_share = round(100 * owner_rows[0]["usd"] / open_usd) if owner_rows and open_usd else 0
    # single biggest deal share of open value
    biggest = max(open_deals, key=lambda d: d["_usd"]) if open_deals else None
    m["s1_2"] = {
        "owners": owner_rows, "top_owner_share": top_share,
        "biggest_deal": ({"name": biggest["dealname"], "usd": biggest["_usd"],
                          "owner": oname(biggest["owner_id"]),
                          "share": round(100 * biggest["_usd"] / open_usd) if open_usd else 0}
                         if biggest else None),
    }

    # 1.3 moved / updated in last 7d
    moved, updated = [], []
    for d in open_deals + [x for x in deals if not x["_open"]]:
        entered = parse_ts(d.get("hs_v2_date_entered_current_stage"))
        lastmod = parse_ts(d.get("hs_lastmodifieddate"))
        is_moved = entered is not None and entered >= window_start
        is_updated = (not is_moved) and lastmod is not None and lastmod >= window_start
        rec = {"name": d["dealname"], "owner": oname(d["owner_id"]), "stage": d["_label"],
               "usd": d["_usd"], "last_activity": (d.get("notes_last_updated") or "—")[:10],
               "entered": (d.get("hs_v2_date_entered_current_stage") or "—")[:10],
               "lastmod": (d.get("hs_lastmodifieddate") or "—")[:10]}
        if is_moved:
            moved.append(rec)
        elif is_updated:
            updated.append(rec)
    moved.sort(key=lambda r: -r["usd"])
    updated.sort(key=lambda r: -r["usd"])
    m["s1_3"] = {"moved": moved, "updated": updated}

    # ---------------- Section 2: new pipeline (top of funnel) ------------
    def in_window(ts_val):
        d = parse_ts(ts_val)
        return d is not None and d >= window_start

    # 2.1 new B2B companies
    new_companies = [c for c in companies_new if in_window(c.get("createdate"))]
    comp_by_owner = {}
    for c in new_companies:
        comp_by_owner.setdefault(c.get("owner_id"), []).append(c)
    m["s2_1"] = {
        "rows": sorted(({"owner": oname(oid), "count": len(cs)}
                        for oid, cs in comp_by_owner.items()), key=lambda r: -r["count"]),
        "total": len(new_companies),
        "no_owner": sum(1 for c in new_companies if not c.get("owner_id")),
    }

    # new B2B contacts in exact window
    new_contacts = [c for c in contacts if in_window(c.get("createdate"))]

    # 2.2 new contacts on EXISTING vs NEW accounts + 2.3 orphans
    on_existing, on_new, orphans = [], [], []
    for c in new_contacts:
        cid = c.get("associatedcompanyid")
        if not cid:
            orphans.append(c)
            continue
        comp = assoc.get(str(cid))
        if comp and in_window(comp.get("createdate")):
            on_new.append(c)
        else:
            on_existing.append(c)

    def group_owner_stage(rows):
        g = {}
        for c in rows:
            g.setdefault(oname(c.get("owner_id")), {}).setdefault(
                c.get("lifecyclestage") or "(none)", []).append(c)
        return g

    m["s2_2"] = {
        "existing_by_owner_stage": {
            o: {s: len(v) for s, v in stages.items()}
            for o, stages in group_owner_stage(on_existing).items()},
        "existing_total": len(on_existing),
        "on_new_total": len(on_new),
    }

    orphan_by_owner = {}
    for c in orphans:
        orphan_by_owner.setdefault(oname(c.get("owner_id")), []).append(c)
    m["s2_3"] = {
        "total": len(orphans),
        "by_owner": {o: [{"id": c["id"], "name": c["name"]} for c in cs]
                     for o, cs in orphan_by_owner.items()},
    }
    m["new_contacts_total"] = len(new_contacts)

    # 2.4 SQLs by owner (B2B contacts only -> the true SQL total)
    b2b_sql = [s for s in sql_all if s["customer_type"] == "B2B"]
    sql_owner = {}
    for s in b2b_sql:
        e = sql_owner.setdefault(s.get("owner_id"), {"qty": 0, "u30": 0, "u60": 0})
        e["qty"] += 1
        d = days_since(s.get("notes_last_updated"), now)
        if d is None or d >= IDLE_BUCKETS[0]:
            e["u30"] += 1
        if d is None or d >= IDLE_BUCKETS[1]:
            e["u60"] += 1
    sql_rows = sorted(({"owner": oname(oid), "qty": e["qty"], "u30": e["u30"], "u60": e["u60"]}
                       for oid, e in sql_owner.items()), key=lambda r: -r["qty"])
    sql_total = len(b2b_sql)
    sql_u30 = sum(r["u30"] for r in sql_rows)
    sql_u60 = sum(r["u60"] for r in sql_rows)
    m["s2_4"] = {"rows": sql_rows, "total": sql_total, "u30": sql_u30, "u60": sql_u60}
    m["sql_total"] = sql_total
    m["pct_sql_untouched_30"] = round(100 * sql_u30 / sql_total) if sql_total else 0

    # ---------------- Section 3: admin / data-hygiene worklist -----------
    # 3A new accounts to complete (missing owner and/or name)
    s3a = []
    for c in new_companies:
        miss_owner = not c.get("owner_id")
        miss_name = not (c.get("name") or "").strip()
        if miss_owner or miss_name:
            s3a.append({"id": c["id"], "created": (c.get("createdate") or "")[:10],
                        "owner": oname(c.get("owner_id")) if not miss_owner else "none",
                        "name": c.get("name") or "blank"})
    m["s3a"] = s3a

    # 3B open deals missing USD
    s3b = [{"id": d["id"], "name": d["dealname"], "stage": d["_label"], "owner": oname(d["owner_id"])}
           for d in open_deals if d["amount_in_home_currency"] is None]
    m["s3b"] = s3b

    # 3C miscategorised B2B contacts
    misc = [c for c in contacts
            if c.get("hs_lead_status") in SALES_WORKING_STATUSES
            and c.get("lifecyclestage") in MISCAT_LIFECYCLE_STAGES]
    misc_by_owner = {}
    for c in misc:
        misc_by_owner.setdefault(oname(c.get("owner_id")), []).append(c)
    m["s3c"] = {
        "total": len(misc),
        "by_owner": {o: {"count": len(cs),
                         "statuses": sorted({status_label(c["hs_lead_status"]) for c in cs}),
                         "ids": [{"id": c["id"], "name": c["name"],
                                  "status": status_label(c["hs_lead_status"]),
                                  "stage": c["lifecyclestage"]} for c in cs]}
                     for o, cs in misc_by_owner.items()},
    }

    # ---------------- Section 4: recycler health -------------------------
    # 4A trigger match NOW: SQL + RECYCLER_TRIGGER_STATUS + untouched 90+d, by customer_type
    trig_by_ct = {}
    for s in sql_all:
        if s.get("hs_lead_status") != RECYCLER_TRIGGER_STATUS:
            continue
        d = days_since(s.get("notes_last_updated"), now)
        if d is None or d >= RECYCLE_DAYS:
            trig_by_ct[s.get("customer_type") or "(none)"] = trig_by_ct.get(s.get("customer_type") or "(none)", 0) + 1

    # stale B2B SQL backlog = B2B SQL untouched 90+d (any status)
    stale_b2b = []
    for s in b2b_sql:
        d = days_since(s.get("notes_last_updated"), now)
        if d is None or d >= RECYCLE_DAYS:
            stale_b2b.append(s)
    b2b_trig = trig_by_ct.get("B2B", 0)
    nonb2b_trig = {k: v for k, v in trig_by_ct.items() if k != "B2B"}
    m["s4a"] = {
        "by_ct": trig_by_ct,
        "b2b_match": b2b_trig,
        "stale_b2b_backlog": len(stale_b2b),
        "alert_scope": (b2b_trig == 0 and len(stale_b2b) > 0),
        "alert_nonb2b": {k: v for k, v in nonb2b_trig.items() if v > 0},
    }

    # 4B stale B2B SQL backlog the trigger MISSES (not trigger status), by status
    miss_by_status = {}
    for s in stale_b2b:
        if s.get("hs_lead_status") == RECYCLER_TRIGGER_STATUS:
            continue
        miss_by_status.setdefault(status_label(s.get("hs_lead_status")), 0)
        miss_by_status[status_label(s.get("hs_lead_status"))] += 1
    m["s4b"] = {"by_status": dict(sorted(miss_by_status.items(), key=lambda kv: -kv[1])),
                "total": sum(miss_by_status.values())}

    # 4C recycled this week, by customer_type
    rec_by_ct = {}
    for r in recycled_week:
        if in_window(r.get("recycled_date")):
            rec_by_ct[r.get("customer_type") or "(none)"] = rec_by_ct.get(r.get("customer_type") or "(none)", 0) + 1
    m["s4c"] = {"by_ct": rec_by_ct, "total": sum(rec_by_ct.values()),
                "records": [{"id": r["id"], "name": r["name"], "ct": r.get("customer_type"),
                             "stage": r.get("lifecyclestage"), "recycled": (r.get("recycled_date") or "")[:10]}
                            for r in recycled_week if in_window(r.get("recycled_date"))]}

    # 4D bounce-back: recycled_date set AND back at SQL
    bounce = [s for s in sql_all if s.get("recycled_date")]
    bb_owner, bb_status, bb_ct = {}, {}, {}
    for s in bounce:
        bb_owner[oname(s.get("owner_id"))] = bb_owner.get(oname(s.get("owner_id")), 0) + 1
        bb_status[status_label(s.get("hs_lead_status"))] = bb_status.get(status_label(s.get("hs_lead_status")), 0) + 1
        bb_ct[s.get("customer_type") or "(none)"] = bb_ct.get(s.get("customer_type") or "(none)", 0) + 1
    m["s4d"] = {
        "total": len(bounce),
        "by_owner": dict(sorted(bb_owner.items(), key=lambda kv: -kv[1])),
        "by_status": dict(sorted(bb_status.items(), key=lambda kv: -kv[1])),
        "by_ct": bb_ct,
        "ids": [{"id": s["id"], "name": s["name"], "owner": oname(s.get("owner_id")),
                 "status": status_label(s.get("hs_lead_status"))} for s in bounce],
    }

    # ---------------- Headline ------------------------------------------
    m["headline"] = {
        "deals_moved": len(moved),
        "pct_open_idle_14": m["pct_open_idle_14"],
        "pct_sql_untouched_30": m["pct_sql_untouched_30"],
        "miscategorised": m["s3c"]["total"],
        "new_accounts_no_owner": m["s2_1"]["no_owner"],
        "deals_missing_usd": len(s3b),
        "bounce_back": m["s4d"]["total"],
        "open_qty": open_qty, "open_usd": open_usd, "sql_total": sql_total,
        "new_companies": m["s2_1"]["total"],
        "new_contacts_existing": m["s2_2"]["existing_total"],
        "new_contacts_on_new": m["s2_2"]["on_new_total"],
        "orphans": m["s2_3"]["total"],
        "recycled_week": m["s4c"]["total"],
        "sql_u30": sql_u30, "sql_u60": sql_u60,
    }
    return m


# =========================================================================
# Week-on-week
# =========================================================================

def load_prev(snap_dir, run_date):
    if not os.path.isdir(snap_dir):
        return None
    cands = []
    for n in os.listdir(snap_dir):
        if not n.endswith(".json"):
            continue
        try:
            d = dt.date.fromisoformat(n[:-5])
        except ValueError:
            continue
        if d < run_date:
            cands.append((d, n))
    if not cands:
        return None
    cands.sort()
    with open(os.path.join(snap_dir, cands[-1][1])) as f:
        return json.load(f)


def delta(cur, prev):
    """'132 (−8)' style. prev None -> no delta shown."""
    if prev is None:
        return f"{cur}"
    diff = cur - prev
    if diff == 0:
        return f"{cur} (=)"
    sign = "+" if diff > 0 else "−"
    return f"{cur} ({sign}{abs(diff)})"


def money_delta(cur, prev):
    if prev is None:
        return usd(cur)
    diff = cur - prev
    if diff == 0:
        return f"{usd(cur)} (=)"
    sign = "+" if diff > 0 else "−"
    return f"{usd(cur)} ({sign}{usd(abs(diff))[1:]})"


# =========================================================================
# Report rendering
# =========================================================================

def render(snapshot, prev):
    m = snapshot["metrics"]
    h = m["headline"]
    date = snapshot["run_date"]
    # prior headline only if comparable schema
    ph = None
    comparable = prev is not None and prev.get("schema_version") == snapshot["schema_version"]
    if comparable:
        ph = prev["metrics"]["headline"]

    def hp(key):
        return ph.get(key) if ph else None

    out = []
    A = out.append

    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {date}")
    A("")
    A(f"*Scope: B2B only (contacts `customer_type = B2B`; companies "
      f"`customer_type__cloned_ = B2B`; deals scoped B2B via the associated "
      f"company). Generated {snapshot['generated_at']}.*")
    A("")
    if comparable:
        A(f"*Week-on-week deltas vs the previous snapshot (`{prev['run_date']}`).*")
    elif prev is not None:
        A(f"*A prior snapshot exists (`{prev['run_date']}`) but predates this "
          f"report's methodology (different schema / a stale, ~3-month-old "
          f"window), so week-on-week comparison for these metrics starts next "
          f"run.*")
    else:
        A("*No prior snapshot found — week-on-week comparison starts next run.*")
    A("")

    # ---- Headline flags ----
    A("## 🚩 Headline flags")
    A("")
    A(f"- **Deals that changed stage this week:** {delta(h['deals_moved'], hp('deals_moved'))}")
    A(f"- **Open deals idle {QTB_STALE_DAYS}+ days:** {delta(h['pct_open_idle_14'], hp('pct_open_idle_14'))}% "
      f"({m['s1_1']['open_idle_ge_14']} of {h['open_qty']} open)")
    A(f"- **SQLs untouched {IDLE_BUCKETS[0]}+ days:** {delta(h['pct_sql_untouched_30'], hp('pct_sql_untouched_30'))}% "
      f"({h['sql_u30']} of {h['sql_total']} B2B SQLs)")
    A(f"- **Miscategorised leads:** {delta(h['miscategorised'], hp('miscategorised'))}")
    A(f"- **New accounts with no owner:** {delta(h['new_accounts_no_owner'], hp('new_accounts_no_owner'))}")
    A(f"- **Open deals missing USD value:** {delta(h['deals_missing_usd'], hp('deals_missing_usd'))}")
    A(f"- **Recycler bounce-backs (recycled but back at SQL):** "
      f"{delta(h['bounce_back'], hp('bounce_back'))} ⚠️")
    A("")

    # =====================================================================
    A("# Section 1 — Current pipeline status")
    A("")
    A("## 1.1 Open pipeline by stage")
    A("")
    A("*B2B, open deals only. USD = `amount_in_home_currency`. Idle = days since "
      "last activity (`notes_last_updated`).*")
    A("")
    A("| Stage | Deals | Open USD | Median days idle | Idle ≥14d |")
    A("|-------|------:|---------:|-----------------:|----------:|")
    s11 = m["s1_1"]
    prev_stage = {}
    if comparable:
        prev_stage = {r["stage"]: r for r in prev["metrics"]["s1_1"]["by_stage"]}
    for r in s11["by_stage"]:
        pv = prev_stage.get(r["stage"]) if comparable else None
        qty = delta(r["qty"], pv["qty"] if pv else None)
        val = money_delta(r["usd"], pv["usd"] if pv else None)
        med = "—" if r["median_idle"] is None else f"{r['median_idle']}d"
        A(f"| {r['stage']} | {qty} | {val} | {med} | {r['idle_ge_14']} |")
    A(f"| **Open subtotal** | **{delta(s11['open_qty'], hp('open_qty'))}** | "
      f"**{money_delta(s11['open_usd'], hp('open_usd'))}** | | **{s11['open_idle_ge_14']}** |")
    A("")
    A("*Reference (not in open subtotal):*")
    A("")
    A("| Stage | Deals | USD |")
    A("|-------|------:|----:|")
    for r in s11["closed_ref"]:
        A(f"| {r['stage']} | {r['qty']} | {usd(r['usd'])} |")
    A("")

    A("## 1.2 Open pipeline by sales owner")
    A("")
    A("| Owner | Open deals | Open USD |")
    A("|-------|-----------:|---------:|")
    for r in m["s1_2"]["owners"]:
        A(f"| {r['owner']} | {r['qty']} | {usd(r['usd'])} |")
    A("")
    s12 = m["s1_2"]
    A(f"**Concentration:** the top owner holds **{s12['top_owner_share']}%** of open "
      f"pipeline value.")
    if s12["biggest_deal"]:
        bd = s12["biggest_deal"]
        A(f" A single deal — *{bd['name']}* ({usd(bd['usd'])}, {bd['owner']}) — is "
          f"**{bd['share']}%** of all open value.")
    A("")

    A("## 1.3 Updated / moved in the last 7 days")
    A("")
    moved, updated = m["s1_3"]["moved"], m["s1_3"]["updated"]
    A(f"**Stage moved this week** (entered current stage within {WINDOW_DAYS}d): "
      f"{len(moved)}.")
    A("")
    if moved:
        A("| Deal | Owner | Current stage | USD | Entered stage |")
        A("|------|-------|---------------|----:|---------------|")
        for r in moved:
            A(f"| {r['name']} | {r['owner']} | {r['stage']} | {usd(r['usd'])} | {r['entered']} |")
        A("")
    else:
        A("*No deal changed stage in the window.* (The last snapshot is ~3 months "
          "old, so a snapshot-to-snapshot stage diff would surface months of "
          "movement rather than this week's — it is deliberately not used here. "
          "Clean week-on-week stage-change tracking resumes from this snapshot.)")
        A("")
    A(f"**Updated this week** (field change, no stage change): {len(updated)}. "
      f"These are edits/syncs, not progression.")
    A("")
    if updated:
        A("| Deal | Owner | Current stage | USD | Last activity | Last modified |")
        A("|------|-------|---------------|----:|---------------|---------------|")
        for r in updated:
            A(f"| {r['name']} | {r['owner']} | {r['stage']} | {usd(r['usd'])} | "
              f"{r['last_activity']} | {r['lastmod']} |")
        A("")

    # =====================================================================
    A("# Section 2 — New pipeline status (top of funnel)")
    A("")
    A(f"## 2.1 New companies (last {WINDOW_DAYS} days)")
    A("")
    s21 = m["s2_1"]
    A(f"New B2B companies created in-window: **{delta(s21['total'], hp('new_companies'))}**.")
    A("")
    if s21["rows"]:
        A("| Owner | New companies |")
        A("|-------|--------------:|")
        for r in s21["rows"]:
            flag = " ⚠️ no owner" if r["owner"] == "Unassigned" else ""
            A(f"| {r['owner']}{flag} | {r['count']} |")
        A("")
    if s21["no_owner"]:
        A(f"⚠️ **{s21['no_owner']} of {s21['total']}** new companies have **no owner** "
          f"(see worklist 3A).")
        A("")

    A(f"## 2.2 New contacts on existing companies (last {WINDOW_DAYS} days)")
    A("")
    s22 = m["s2_2"]
    A(f"New B2B contacts whose associated company pre-dates the window: "
      f"**{delta(s22['existing_total'], hp('new_contacts_existing'))}**. "
      f"New contacts landing on **new** accounts (company also created in-window): "
      f"**{delta(s22['on_new_total'], hp('new_contacts_on_new'))}**.")
    A("")
    if s22["existing_by_owner_stage"]:
        A("| Owner | Lifecycle stage | New contacts |")
        A("|-------|-----------------|-------------:|")
        for o, stages in s22["existing_by_owner_stage"].items():
            for st, cnt in stages.items():
                A(f"| {o} | {st} | {cnt} |")
        A("")

    A("## 2.3 New contacts not associated to any company (orphans)")
    A("")
    s23 = m["s2_3"]
    A(f"Orphaned new contacts (`associatedcompanyid` is null): "
      f"**{delta(s23['total'], hp('orphans'))}**.")
    A("")
    if s23["by_owner"]:
        A("| Owner | Count | Contacts |")
        A("|-------|------:|----------|")
        for o, cs in s23["by_owner"].items():
            names = ", ".join(f"{c['name']} (#{c['id']})" for c in cs)
            A(f"| {o} | {len(cs)} | {names} |")
        A("")

    A("## 2.4 SQLs by sales owner (true SQL total — contacts only)")
    A("")
    s24 = m["s2_4"]
    A("*B2B contacts at Sales Qualified Lead. This is the real SQL population — "
      "the company object is never counted here.*")
    A("")
    A(f"| Owner | SQLs | Untouched {IDLE_BUCKETS[0]}+d | Untouched {IDLE_BUCKETS[1]}+d |")
    A("|-------|-----:|-----------:|-----------:|")
    for r in s24["rows"]:
        A(f"| {r['owner']} | {r['qty']} | {r['u30']} | {r['u60']} |")
    A(f"| **Total** | **{delta(s24['total'], hp('sql_total'))}** | "
      f"**{delta(s24['u30'], hp('sql_u30'))}** | **{delta(s24['u60'], hp('sql_u60'))}** |")
    A("")

    # =====================================================================
    A("# Section 3 — Admin / data-hygiene worklist")
    A("")
    A("## 3A New accounts to complete (missing owner and/or name)")
    A("")
    if m["s3a"]:
        A("| Company ID | Created | Owner | Name |")
        A("|------------|---------|-------|------|")
        for r in m["s3a"]:
            A(f"| {r['id']} | {r['created']} | {r['owner']} | {r['name']} |")
    else:
        A("- ✅ None this week.")
    A("")

    A("## 3B Open deals missing a USD value (`amount_in_home_currency` empty)")
    A("")
    if m["s3b"]:
        A("| Deal | Stage | Owner |")
        A("|------|-------|-------|")
        for r in m["s3b"]:
            A(f"| {r['name']} (#{r['id']}) | {r['stage']} | {r['owner']} |")
        A("")
        A("*These silently distort pipeline value and are excluded from USD totals "
          "until fixed.*")
    else:
        A("- ✅ None this week.")
    A("")

    A("## 3C Miscategorised leads")
    A("")
    s3c = m["s3c"]
    A(f"Sales is working these contacts (`hs_lead_status` ∈ "
      f"{{Attempted to Contact, Contacted}}) but their lifecycle stage is still "
      f"only Lead / MQL — they should be SQL or beyond. Total: "
      f"**{delta(s3c['total'], hp('miscategorised'))}**.")
    A("")
    if s3c["by_owner"]:
        A("| Owner | Count | Statuses |")
        A("|-------|------:|----------|")
        for o, info in sorted(s3c["by_owner"].items(), key=lambda kv: -kv[1]["count"]):
            A(f"| {o} | {info['count']} | {', '.join(info['statuses'])} |")
        A("")
        A("*Full contact IDs per owner are in the snapshot JSON (worklist 3C).*")
    A("")

    # =====================================================================
    A("# Section 4 — Recycler health check")
    A("")
    A("*The stale-SQL recycler is meant to move SQL→MQL after 90 days of no "
      "activity (sets `recycled_date`, `unqualified_reason`, enrols in the "
      "re-engagement nurture). This section diagnoses whether it is working. It "
      "does not change the workflow.*")
    A("")

    s4a = m["s4a"]
    A(f"## 4A Trigger match now (SQL + `{RECYCLER_TRIGGER_STATUS}` + untouched "
      f"{RECYCLE_DAYS}+d)")
    A("")
    if s4a["by_ct"]:
        A("| Customer type | Matches |")
        A("|---------------|--------:|")
        for ct, n in sorted(s4a["by_ct"].items(), key=lambda kv: -kv[1]):
            A(f"| {ct} | {n} |")
    else:
        A("- No contacts currently match the trigger.")
    A("")
    A(f"Stale **B2B** SQL backlog (SQL untouched {RECYCLE_DAYS}+d, any status): "
      f"**{s4a['stale_b2b_backlog']}**. B2B trigger matches: **{s4a['b2b_match']}**.")
    A("")
    if s4a["alert_scope"]:
        A(f"> 🚨 **ALERT:** the B2B trigger matches **0** contacts while the stale "
          f"B2B SQL backlog is **{s4a['stale_b2b_backlog']}**. The workflow keys on "
          f"`{RECYCLER_TRIGGER_STATUS}`, but the stale B2B SQLs don't carry that "
          f"status — so the recycler catches **none** of them. (Re-scoping the "
          f"enrolment trigger is a separate HubSpot workflow change.)")
        A("")
    if s4a["alert_nonb2b"]:
        A(f"> 🚨 **ALERT:** non-B2B contacts match the trigger "
          f"({', '.join(f'{k}: {v}' for k, v in s4a['alert_nonb2b'].items())}) — "
          f"the recycler should not touch Partner / Internal / B2C.")
        A("")

    s4b = m["s4b"]
    A(f"## 4B Stale B2B SQL backlog the trigger misses")
    A("")
    A(f"B2B SQLs untouched {RECYCLE_DAYS}+d whose status is **not** "
      f"`{RECYCLER_TRIGGER_STATUS}` — the population the recycler should catch but "
      f"doesn't. Total: **{s4b['total']}**.")
    A("")
    if s4b["by_status"]:
        A("| Lead status | Count |")
        A("|-------------|------:|")
        for st, n in s4b["by_status"].items():
            A(f"| {st} | {n} |")
        A("")

    s4c = m["s4c"]
    A("## 4C Recycled this week")
    A("")
    A(f"Contacts whose `recycled_date` falls in the last {WINDOW_DAYS} days "
      f"(proof the workflow fired): **{delta(s4c['total'], hp('recycled_week'))}**.")
    A("")
    if s4c["by_ct"]:
        A("| Customer type | Recycled |")
        A("|---------------|---------:|")
        for ct, n in s4c["by_ct"].items():
            A(f"| {ct} | {n} |")
        A("")
        for r in s4c["records"]:
            if r["stage"] == "salesqualifiedlead":
                A(f"> ⚠️ *{r['name']}* (#{r['id']}) was recycled {r['recycled']} but is "
                  f"**still at Sales Qualified Lead** — i.e. this week's one recycle "
                  f"did not stick (it is also a bounce-back, 4D).")
        A("")

    s4d = m["s4d"]
    A("## 4D Bounce-back — recycled but back at SQL")
    A("")
    A(f"> 🚨 **{delta(s4d['total'], hp('bounce_back'))}** contacts have a "
      f"`recycled_date` set **and** are back at `salesqualifiedlead`. The recycle "
      f"is **not sticking** — something is re-promoting them to SQL (suspect: "
      f"lifecycle sync from an associated company/deal still at SQL/Opportunity). "
      f"Flagged for the workflow owner.")
    A("")
    A("**By customer type:** " + ", ".join(f"{k} {v}" for k, v in s4d["by_ct"].items()))
    A("")
    A("| Owner | Bounce-backs |")
    A("|-------|-------------:|")
    for o, n in s4d["by_owner"].items():
        A(f"| {o} | {n} |")
    A("")
    A("| Lead status | Bounce-backs |")
    A("|-------------|-------------:|")
    for st, n in s4d["by_status"].items():
        A(f"| {st} | {n} |")
    A("")
    A("*Full contact IDs are in the snapshot JSON (worklist 4D).*")
    A("")

    A("---")
    A(f"*Snapshot: `snapshots/{date}.json` · Report: `reports/{date}.md`. "
      f"Source: {snapshot['source']}.*")
    A("")
    return "\n".join(out)


# =========================================================================
# Main
# =========================================================================

def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly B2B pipeline report")
    ap.add_argument("--input", required=True, help="Pre-pulled raw JSON (required)")
    ap.add_argument("--date", help="Run date YYYY-MM-DD (default: from generated_at)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
    args = ap.parse_args()

    with open(args.input) as f:
        raw = json.load(f)

    if "contacts_b2b" not in raw or "deals_b2b" not in raw:
        sys.exit("Input is not in the expected schema (needs contacts_b2b + deals_b2b).")

    gen = parse_ts(raw["generated_at"])
    run_date = (dt.date.fromisoformat(args.date) if args.date else gen.date())

    # ---- guard: B2B company filter silent-fail signature ----
    companies = raw.get("companies_b2b", [])
    assoc = raw.get("associated_companies", [])
    if not companies and not assoc:
        sys.exit("GUARD: no B2B company records at all — suspected silent-fail on "
                 "the customer_type__cloned_ filter. Aborting.")

    metrics = compute(raw)

    # guard: SQL total must be contacts, not companies (companies at SQL ~322,
    # B2B contacts at SQL ~132-160). A total in the 300s signals the company
    # object leaked into the SQL count.
    if metrics["sql_total"] > 280:
        sys.exit(f"GUARD: SQL total {metrics['sql_total']} looks like companies, "
                 f"not contacts. Aborting.")

    snapshot = {
        "schema_version": raw.get("schema_version", 2),
        "run_date": run_date.isoformat(),
        "generated_at": raw["generated_at"],
        "source": raw.get("source", "hubspot (pre-pulled)"),
        "window_days": WINDOW_DAYS,
        "idle_buckets": IDLE_BUCKETS,
        "recycle_days": RECYCLE_DAYS,
        "raw": {
            "owners": raw.get("owners", {}),
            "contacts_b2b": raw["contacts_b2b"],
            "sql_all_types": raw["sql_all_types"],
            "companies_b2b": raw["companies_b2b"],
            "associated_companies": raw.get("associated_companies", []),
            "deals_b2b": raw["deals_b2b"],
            "recycled_week": raw.get("recycled_week", []),
        },
        "metrics": metrics,
    }

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev(snap_dir, run_date)

    snap_path = os.path.join(snap_dir, f"{run_date.isoformat()}.json")
    with open(snap_path, "w") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    report = render(snapshot, prev)
    rep_path = os.path.join(rep_dir, f"{run_date.isoformat()}.md")
    with open(rep_path, "w") as f:
        f.write(report)

    print(f"Wrote {snap_path}")
    print(f"Wrote {rep_path}")
    if prev:
        print(f"Previous snapshot: {prev['run_date']} "
              f"(schema v{prev.get('schema_version', 1)}; "
              f"{'comparable' if prev.get('schema_version') == snapshot['schema_version'] else 'not comparable — WoW starts next run'})")
    else:
        print("No previous snapshot (first run).")


if __name__ == "__main__":
    main()
