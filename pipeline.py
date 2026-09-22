#!/usr/bin/env python3
"""Brahmarsive weekly B2B pipeline & MQL-health report.

Consumes a pre-pulled HubSpot dataset (raw_input.json, built from the HubSpot MCP
tools) and writes, per run:

  snapshots/YYYY-MM-DD.json  -- raw data + computed metrics + all worklist record ids
  reports/YYYY-MM-DD.md      -- Sections 1-4 (see README / the routine spec)

Run:  python3 pipeline.py --input raw_input.json

Scoping rules (non-negotiable, honoured here):
  * B2B only. Contacts filtered on customer_type='B2B'; companies on
    customer_type__cloned_='B2B'; deals scoped via COMPANY.customer_type__cloned_.
  * Two bases, never mixed. Contact counts (funnel, SQL totals, miscategorised,
    new leads) come from the CONTACT object only. Pipeline value comes from DEALS.
    The company object never feeds contact/SQL counts.
  * Deal USD value is amount_in_home_currency. Raw amount + deal_currency_code are
    shown only as secondary reference; a deal with an empty amount_in_home_currency
    is flagged (admin worklist 3B), never valued off raw amount.
  * Contact->company joins (orphan flag, new-on-existing-vs-new-account) are done
    here from each contact's associatedcompanyid — cross-object contact->company
    filters silent-fail in the MCP SQL layer.
"""

import argparse
import datetime as dt
import json
import os
import statistics

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
# Open-pipeline display order (labels). Closed stages shown below, as reference only.
STAGE_ORDER = [
    "Initial Engagement", "Qualified to Buy", "Discovery & Trial",
    "Quote Sent", "Commitment to Buy", "Negotiation",
]
CLOSED_LABELS = ["Closed Won", "Closed Lost"]
CLOSED_STAGES = {"closedwon", "closedlost"}

# hs_lead_status values that mean sales is actively working the contact. These are
# the values that actually exist in this portal ("Contacted" is stored internally
# as CONNECTED, so both spellings are matched).
SALES_WORKING_STATUSES = ["Attempted to Contact", "Contacted", "CONNECTED"]
RECYCLER_TRIGGER_STATUS = "Attempted to Contact"   # what the live workflow keys on today

WINDOW_DAYS = 7
IDLE_BUCKETS = [30, 60]
QTB_STALE_DAYS = 14          # open-deal idle threshold (headline + per-stage)
RECYCLE_DAYS = 90            # stale-SQL recycler threshold

MISCAT_EARLY_STAGES = {"lead", "marketingqualifiedlead"}


# =========================================================================
# Helpers
# =========================================================================
def parse_dt(value):
    """Parse an ISO timestamp or YYYY-MM-DD date to an aware UTC datetime, or None."""
    if not value or str(value).strip().lower() == "unassigned":
        return None
    v = str(value).strip().replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(v)
    except ValueError:
        try:
            d = dt.datetime.strptime(v[:10], "%Y-%m-%d")
        except ValueError:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d


def days_since(value, run_dt):
    d = parse_dt(value)
    if d is None:
        return None
    return (run_dt - d).days


def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def usd(v):
    return f"${v:,.0f}"


def pct(n, d):
    return f"{(100.0 * n / d):.0f}%" if d else "n/a"


# =========================================================================
# Delta helpers (week-on-week)
# =========================================================================
def dstr(cur, prev):
    if prev is None:
        return f"{cur}"
    diff = cur - prev
    if diff > 0:
        return f"{cur} (+{diff})"
    if diff < 0:
        return f"{cur} ({diff})"
    return f"{cur} (=)"


def mdstr(cur, prev):
    if prev is None:
        return usd(cur)
    diff = cur - prev
    sign = "+" if diff > 0 else ""
    if diff == 0:
        return f"{usd(cur)} (=)"
    return f"{usd(cur)} ({sign}{diff:,.0f})"


# =========================================================================
# Computation
# =========================================================================
def compute(raw):
    run_date = dt.date.fromisoformat(raw["run_date"])
    run_dt = dt.datetime.combine(run_date, dt.time(), tzinfo=dt.timezone.utc)
    window_days = raw.get("window_days", WINDOW_DAYS)
    cutoff = run_date - dt.timedelta(days=window_days)          # inclusive (date)
    owners = raw.get("owners", {})

    def oname(oid):
        if not oid:
            return "Unassigned"
        return owners.get(str(oid), f"Owner {oid}")

    def in_window(value):
        d = parse_dt(value)
        return d is not None and d.date() >= cutoff

    deals = raw["deals_b2b"]
    for d in deals:
        d["label"] = DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage") or "unknown")
        d["is_open"] = d.get("dealstage") not in CLOSED_STAGES
        d["usd"] = fnum(d.get("amount_in_home_currency"))
        d["idle_days"] = days_since(d.get("notes_last_updated"), run_dt)
        d["owner_name"] = oname(d.get("owner_id"))
    open_deals = [d for d in deals if d["is_open"]]

    # ---- 1.1 Open pipeline by stage ----
    def stage_block(label):
        ds = [d for d in open_deals if d["label"] == label]
        idles = [d["idle_days"] for d in ds if d["idle_days"] is not None]
        idle_ge = sum(1 for d in ds if d["idle_days"] is None or d["idle_days"] >= QTB_STALE_DAYS)
        return {
            "label": label,
            "qty": len(ds),
            "usd": round(sum(d["usd"] or 0 for d in ds), 2),
            "median_idle": int(statistics.median(idles)) if idles else None,
            "idle_ge_thresh": idle_ge,
        }

    known = set(STAGE_ORDER)
    extra_open = sorted({d["label"] for d in open_deals if d["label"] not in known})
    stage_rows = [stage_block(l) for l in STAGE_ORDER] + [stage_block(l) for l in extra_open]
    open_qty = sum(r["qty"] for r in stage_rows)
    open_usd = round(sum(r["usd"] for r in stage_rows), 2)
    closed_rows = []
    for label in CLOSED_LABELS:
        ds = [d for d in deals if d["label"] == label]
        closed_rows.append({"label": label, "qty": len(ds),
                            "usd": round(sum(d["usd"] or 0 for d in ds), 2)})

    # ---- 1.2 Open pipeline by owner ----
    by_owner = {}
    for d in open_deals:
        e = by_owner.setdefault(d["owner_name"], {"qty": 0, "usd": 0.0})
        e["qty"] += 1
        e["usd"] += d["usd"] or 0
    owner_rows = sorted(
        [{"owner": o, "qty": e["qty"], "usd": round(e["usd"], 2)} for o, e in by_owner.items()],
        key=lambda x: -x["usd"])
    top_owner = owner_rows[0] if owner_rows else None
    biggest_deal = max(open_deals, key=lambda d: d["usd"] or 0, default=None)

    # ---- 1.3 Moved / updated in last 7 days ----
    moved, updated = [], []
    for d in open_deals + [x for x in deals if not x["is_open"]]:
        rec = {"id": d["id"], "dealname": d["dealname"], "owner": d["owner_name"],
               "stage": d["label"], "usd": d["usd"] or 0,
               "last_activity": (d.get("notes_last_updated") or "—")[:10]}
        if in_window(d.get("hs_v2_date_entered_current_stage")):
            moved.append(rec)
        elif in_window(d.get("hs_lastmodifieddate")):
            updated.append(rec)
    moved.sort(key=lambda x: -x["usd"])
    updated.sort(key=lambda x: -x["usd"])

    # ---- 2. Top of funnel ----
    new_contacts = raw["contacts_b2b_new"]
    new_contacts = [c for c in new_contacts if in_window(c.get("createdate"))]
    assoc = raw.get("assoc_companies", {})

    def assoc_is_new(cid):
        info = assoc.get(str(cid))
        return bool(info and in_window(info.get("createdate")))

    orphans = [c for c in new_contacts if not c.get("associatedcompanyid")]
    on_new, on_existing = [], []
    for c in new_contacts:
        cid = c.get("associatedcompanyid")
        if not cid:
            continue
        (on_new if assoc_is_new(cid) else on_existing).append(c)

    # 2.1 new companies by owner
    new_companies = [c for c in raw["companies_b2b_new"] if in_window(c.get("createdate"))]
    newco_by_owner = {}
    for c in new_companies:
        newco_by_owner.setdefault(oname(c.get("owner_id")), []).append(c)

    # 2.2 new contacts on existing companies: owner -> stage -> count
    existing_by_owner = {}
    for c in on_existing:
        st = existing_by_owner.setdefault(oname(c.get("owner_id")), {})
        st[c.get("lifecyclestage") or "unknown"] = st.get(c.get("lifecyclestage") or "unknown", 0) + 1

    # 2.3 orphans by owner
    orphan_by_owner = {}
    for c in orphans:
        orphan_by_owner[oname(c.get("owner_id"))] = orphan_by_owner.get(oname(c.get("owner_id")), 0) + 1

    # 2.4 SQLs by owner (contacts only — the true SQL total)
    sql = raw["sql_b2b"]
    sql_owner = {}
    for c in sql:
        e = sql_owner.setdefault(oname(c.get("owner_id")), {"qty": 0, "u30": 0, "u60": 0})
        e["qty"] += 1
        idle = days_since(c.get("notes_last_updated"), run_dt)
        if idle is None or idle >= IDLE_BUCKETS[0]:
            e["u30"] += 1
        if idle is None or idle >= IDLE_BUCKETS[1]:
            e["u60"] += 1
    sql_owner_rows = sorted(
        [{"owner": o, **e} for o, e in sql_owner.items()], key=lambda x: -x["qty"])
    sql_total = len(sql)
    sql_u30 = sum(r["u30"] for r in sql_owner_rows)
    sql_u60 = sum(r["u60"] for r in sql_owner_rows)

    # ---- 3. Admin / data-hygiene worklist ----
    # 3A new accounts to complete (missing owner and/or name)
    accounts_to_complete = [
        {"id": c["id"], "createdate": (c.get("createdate") or "")[:10],
         "owner": oname(c.get("owner_id")) if c.get("owner_id") else "none",
         "name": c.get("name") or "blank"}
        for c in new_companies if (not c.get("owner_id")) or (not c.get("name"))
    ]
    # 3B open deals missing USD value (amount_in_home_currency empty/null)
    deals_missing_usd = [
        {"id": d["id"], "dealname": d["dealname"], "stage": d["label"], "owner": d["owner_name"],
         "raw_amount": d.get("amount"), "currency": d.get("deal_currency_code")}
        for d in open_deals if d["usd"] is None
    ]
    # 3C miscategorised leads by owner
    miscat = raw["miscat_b2b"]
    miscat_by_owner = {}
    for c in miscat:
        e = miscat_by_owner.setdefault(oname(c.get("owner_id")),
                                       {"count": 0, "statuses": {}, "ids": []})
        e["count"] += 1
        st = c.get("hs_lead_status")
        e["statuses"][st] = e["statuses"].get(st, 0) + 1
        e["ids"].append({"id": c["id"], "name": c.get("name"), "status": st,
                         "stage": c.get("lifecyclestage")})
    miscat_rows = sorted([{"owner": o, **e} for o, e in miscat_by_owner.items()],
                         key=lambda x: -x["count"])

    # ---- 4. Recycler health check ----
    # Stale B2B SQL backlog: SQL contacts untouched RECYCLE_DAYS+ (null = untouched)
    def untouched_90(c):
        idle = days_since(c.get("notes_last_updated"), run_dt)
        return idle is None or idle >= RECYCLE_DAYS
    stale_b2b_sql = [c for c in sql if untouched_90(c)]
    stale_b2b_sql_count = len(stale_b2b_sql)

    # 4A trigger match now (server-side aggregate across all customer types)
    trig = raw.get("recycler_4a_trigger_match", {})
    b2b_trigger_now = sum(1 for c in sql
                          if c.get("hs_lead_status") == RECYCLER_TRIGGER_STATUS and untouched_90(c))
    trig = dict(trig)
    trig["B2B"] = b2b_trigger_now  # cross-check from records (expect to match aggregate)
    alert_b2b_zero = (trig.get("B2B", 0) == 0 and stale_b2b_sql_count > 0)
    nonb2b_matches = {k: v for k, v in trig.items() if k != "B2B" and v}
    alert_nonb2b = bool(nonb2b_matches)

    # 4B stale B2B SQL backlog the trigger MISSES (untouched 90+, not trigger status), by status
    missed = [c for c in stale_b2b_sql if c.get("hs_lead_status") != RECYCLER_TRIGGER_STATUS]
    missed_by_status = {}
    for c in missed:
        st = c.get("hs_lead_status") or "(none)"
        e = missed_by_status.setdefault(st, {"count": 0, "ids": []})
        e["count"] += 1
        e["ids"].append({"id": c["id"], "name": c.get("name"), "owner": oname(c.get("owner_id")),
                         "idle_days": days_since(c.get("notes_last_updated"), run_dt)})
    missed_rows = sorted([{"status": s, **e} for s, e in missed_by_status.items()],
                         key=lambda x: -x["count"])

    # 4C recycled this week, by customer_type
    recycled_recent = raw.get("recycled_recent", [])
    recycled_by_type = {}
    for c in recycled_recent:
        recycled_by_type[c.get("customer_type") or "Unassigned"] = \
            recycled_by_type.get(c.get("customer_type") or "Unassigned", 0) + 1

    # 4D bounce-back: recycled (recycled_date set) AND back at SQL, by owner + status
    bounce = [c for c in sql if c.get("recycled_date")]
    bounce_by_owner, bounce_by_status = {}, {}
    bounce_ids = []
    for c in bounce:
        bounce_by_owner[oname(c.get("owner_id"))] = bounce_by_owner.get(oname(c.get("owner_id")), 0) + 1
        st = c.get("hs_lead_status") or "(none)"
        bounce_by_status[st] = bounce_by_status.get(st, 0) + 1
        bounce_ids.append({"id": c["id"], "name": c.get("name"),
                           "owner": oname(c.get("owner_id")), "status": st,
                           "recycled_date": (c.get("recycled_date") or "")[:10]})

    # ---- Headline flags ----
    open_idle_ge = sum(1 for d in open_deals if d["idle_days"] is None or d["idle_days"] >= QTB_STALE_DAYS)
    headline = {
        "deals_moved": len(moved),
        "open_deals": len(open_deals),
        "open_idle_pct_num": open_idle_ge,
        "sql_total": sql_total,
        "sql_u30": sql_u30,
        "sql_u60": sql_u60,
        "miscat": len(miscat),
        "new_accounts_no_owner": sum(1 for c in new_companies if not c.get("owner_id")),
        "deals_missing_usd": len(deals_missing_usd),
        "bounce_back": len(bounce),
        "new_companies": len(new_companies),
        "new_contacts": len(new_contacts),
        "new_contacts_on_existing": len(on_existing),
        "new_contacts_on_new": len(on_new),
        "orphans": len(orphans),
        "recycled_this_week": len(recycled_recent),
        "open_pipeline_usd": open_usd,
        "stale_b2b_sql_backlog": stale_b2b_sql_count,
    }

    return {
        "run_date": raw["run_date"], "generated_at": raw.get("generated_at"),
        "window_days": window_days, "cutoff": cutoff.isoformat(),
        "headline": headline,
        "s1": {"stage_rows": stage_rows, "open_qty": open_qty, "open_usd": open_usd,
               "closed_rows": closed_rows, "owner_rows": owner_rows,
               "top_owner": top_owner, "biggest_deal": (
                   {"dealname": biggest_deal["dealname"], "usd": biggest_deal["usd"] or 0,
                    "owner": biggest_deal["owner_name"], "stage": biggest_deal["label"]}
                   if biggest_deal else None),
               "moved": moved, "updated": updated},
        "s2": {"new_companies": new_companies, "newco_by_owner": newco_by_owner,
               "existing_by_owner": existing_by_owner, "on_new_count": len(on_new),
               "on_existing_count": len(on_existing), "on_new": on_new,
               "orphans": orphans, "orphan_by_owner": orphan_by_owner,
               "sql_owner_rows": sql_owner_rows, "sql_total": sql_total,
               "sql_u30": sql_u30, "sql_u60": sql_u60},
        "s3": {"accounts_to_complete": accounts_to_complete,
               "deals_missing_usd": deals_missing_usd, "miscat_rows": miscat_rows,
               "miscat_total": len(miscat)},
        "s4": {"trigger": trig, "stale_b2b_sql": stale_b2b_sql_count,
               "alert_b2b_zero": alert_b2b_zero, "alert_nonb2b": alert_nonb2b,
               "nonb2b_matches": nonb2b_matches, "missed_rows": missed_rows,
               "missed_total": len(missed), "recycled_by_type": recycled_by_type,
               "recycled_total": len(recycled_recent),
               "bounce_total": len(bounce), "bounce_by_owner": bounce_by_owner,
               "bounce_by_status": bounce_by_status, "bounce_ids": bounce_ids},
        "worklists": {
            "3A_accounts_to_complete": accounts_to_complete,
            "3B_deals_missing_usd": deals_missing_usd,
            "3C_miscategorised": {r["owner"]: r["ids"] for r in miscat_rows},
            "4B_stale_sql_missed": {r["status"]: r["ids"] for r in missed_rows},
            "4D_bounce_back": bounce_ids,
            "2.3_orphans": [{"id": c["id"], "name": c.get("name"),
                             "owner": oname(c.get("owner_id"))} for c in orphans],
        },
    }


# =========================================================================
# Report rendering
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


def render(m, prev):
    ph = (prev or {}).get("metrics", {}).get("headline") if prev else None
    if prev and not ph:  # old-format snapshot
        ph = None
    h = m["headline"]

    def hd(key, prev_key=None):
        return None if ph is None else ph.get(prev_key or key)

    out = []
    A = out.append
    date = m["run_date"]
    A(f"# Brahmarsive Weekly Pipeline & MQL Health — {date}")
    A("")
    A("*Scope: B2B only — contacts `customer_type=B2B`, companies "
      "`customer_type__cloned_=B2B`, deals scoped via `COMPANY.customer_type__cloned_=B2B`. "
      "Contact counts come from the CONTACT object; pipeline value from DEALS "
      "(`amount_in_home_currency`, USD). The two bases are never mixed.*")
    A(f"*Generated {m.get('generated_at')} · window = last {m['window_days']} days "
      f"(created on/after {m['cutoff']}).*")
    A("")
    if prev and ph:
        A(f"*Week-on-week deltas vs previous snapshot (`{prev['run_date']}`).*")
    elif prev:
        A(f"*Previous snapshot (`{prev['run_date']}`) predates this report format — "
          "week-on-week comparison of the new metrics starts next run.*")
    else:
        A("*No prior snapshot — week-on-week comparison starts next run.*")
    A("")

    # ---- Headline flags ----
    A("## 🚩 Headline flags")
    A("")
    A(f"- **Deals changed stage this week:** {dstr(h['deals_moved'], hd('deals_moved'))}")
    A(f"- **Open deals idle {QTB_STALE_DAYS}+ days:** {pct(h['open_idle_pct_num'], h['open_deals'])} "
      f"({h['open_idle_pct_num']} of {h['open_deals']} open)")
    A(f"- **SQLs untouched {IDLE_BUCKETS[0]}+ days:** {pct(h['sql_u30'], h['sql_total'])} "
      f"({h['sql_u30']} of {h['sql_total']} SQL contacts)")
    A(f"- **Miscategorised leads:** {dstr(h['miscat'], hd('miscat'))}")
    A(f"- **New accounts with no owner:** {dstr(h['new_accounts_no_owner'], hd('new_accounts_no_owner'))}")
    A(f"- **Deals missing USD value:** {dstr(h['deals_missing_usd'], hd('deals_missing_usd'))}")
    A(f"- **Recycler bounce-backs (recycled but back at SQL):** "
      f"{dstr(h['bounce_back'], hd('bounce_back'))}")
    A("")

    s4 = m["s4"]
    if s4["alert_b2b_zero"] or s4["alert_nonb2b"]:
        A("> ⚠️ **Recycler is mis-scoped** — see Section 4. The 90-day stale-SQL "
          "recycler currently matches **0** of the B2B stale-SQL backlog while firing "
          "on non-B2B records it should leave alone.")
        A("")

    # =====================================================================
    A("# SECTION 1 — CURRENT PIPELINE STATUS")
    A("")
    s1 = m["s1"]
    A("## 1.1 Open pipeline by stage")
    A(f"*B2B, open deals only (all stages except Closed Won / Closed Lost). "
      f"Idle = days since last activity; median across the stage.*")
    A("")
    A("| Stage | Deals | Open USD | Median days idle | Idle ≥14d |")
    A("|-------|------:|---------:|-----------------:|----------:|")
    for r in s1["stage_rows"]:
        mi = "—" if r["median_idle"] is None else f"{r['median_idle']}d"
        A(f"| {r['label']} | {r['qty']} | {usd(r['usd'])} | {mi} | {r['idle_ge_thresh']} |")
    A(f"| **Open subtotal** | **{s1['open_qty']}** | **{usd(s1['open_usd'])}** | | |")
    A("")
    A("*Reference only (not in the open subtotal):*")
    A("")
    A("| Stage | Deals | USD |")
    A("|-------|------:|----:|")
    for r in s1["closed_rows"]:
        A(f"| {r['label']} | {r['qty']} | {usd(r['usd'])} |")
    A("")

    A("## 1.2 Open pipeline by sales owner")
    A("")
    A("| Owner | Open deals | Open USD |")
    A("|-------|-----------:|---------:|")
    for r in s1["owner_rows"]:
        A(f"| {r['owner']} | {r['qty']} | {usd(r['usd'])} |")
    A("")
    if s1["top_owner"] and s1["open_usd"]:
        t = s1["top_owner"]
        A(f"**Concentration:** {t['owner']} holds {usd(t['usd'])} "
          f"({pct(t['usd'], s1['open_usd'])}) of the {usd(s1['open_usd'])} open pipeline.")
        if s1["biggest_deal"]:
            b = s1["biggest_deal"]
            A(f" Largest single open deal: {b['dealname']} — {usd(b['usd'])} "
              f"({pct(b['usd'], s1['open_usd'])} of open value, {b['owner']}, {b['stage']}).")
        A("")

    A("## 1.3 Updated / moved in the last 7 days")
    A("")
    A(f"**Stage moved** ({len(s1['moved'])}) — entered current stage within the window "
      "(previous stage will be diffable once a prior same-format snapshot exists):")
    A("")
    if s1["moved"]:
        A("| Deal | Owner | → Current stage | USD | Last activity |")
        A("|------|-------|-----------------|----:|---------------|")
        for d in s1["moved"]:
            A(f"| {d['dealname']} | {d['owner']} | {d['stage']} | {usd(d['usd'])} | {d['last_activity']} |")
    else:
        A("- None this week.")
    A("")
    A(f"**Updated** ({len(s1['updated'])}) — field change within the window, no stage change:")
    A("")
    if s1["updated"]:
        A("| Deal | Owner | Stage | USD | Last activity |")
        A("|------|-------|-------|----:|---------------|")
        for d in s1["updated"]:
            A(f"| {d['dealname']} | {d['owner']} | {d['stage']} | {usd(d['usd'])} | {d['last_activity']} |")
    else:
        A("- None this week.")
    A("")

    # =====================================================================
    A("# SECTION 2 — NEW PIPELINE STATUS (top of funnel)")
    A("")
    s2 = m["s2"]
    A(f"## 2.1 New companies created in last 7 days — by owner ({h['new_companies']})")
    A("")
    if s2["newco_by_owner"]:
        A("| Owner | New companies |")
        A("|-------|--------------:|")
        for o, lst in sorted(s2["newco_by_owner"].items(), key=lambda kv: -len(kv[1])):
            flag = " ⚠️ no owner" if o == "Unassigned" else ""
            A(f"| {o}{flag} | {len(lst)} |")
    else:
        A("- No new B2B companies this week.")
    A("")

    A(f"## 2.2 New contacts on EXISTING companies — by owner, then lifecycle stage "
      f"({s2['on_existing_count']})")
    A("")
    if s2["existing_by_owner"]:
        A("| Owner | Lifecycle stage | New contacts |")
        A("|-------|-----------------|-------------:|")
        for o, stages in sorted(s2["existing_by_owner"].items(),
                                key=lambda kv: -sum(kv[1].values())):
            for st, n in sorted(stages.items(), key=lambda kv: -kv[1]):
                A(f"| {o} | {st} | {n} |")
    else:
        A("- None this week.")
    A("")
    A(f"New contacts on **NEW** accounts (associated company created in-window): "
      f"**{s2['on_new_count']}**.")
    A("")

    A(f"## 2.3 🚩 New contacts NOT associated to any company (orphans) — "
      f"{dstr(h['orphans'], hd('orphans'))}")
    A("")
    if s2["orphan_by_owner"]:
        A("| Owner | Orphan contacts |")
        A("|-------|----------------:|")
        for o, n in sorted(s2["orphan_by_owner"].items(), key=lambda kv: -kv[1]):
            A(f"| {o} | {n} |")
    else:
        A("- ✅ None — every new contact this week is associated to a company.")
    A("")

    A("## 2.4 SQLs by sales owner (contacts only — the true SQL total)")
    A("")
    A(f"| Owner | SQL qty | Untouched {IDLE_BUCKETS[0]}+d | Untouched {IDLE_BUCKETS[1]}+d |")
    A("|-------|--------:|------------:|------------:|")
    for r in s2["sql_owner_rows"]:
        A(f"| {r['owner']} | {r['qty']} | {r['u30']} | {r['u60']} |")
    A(f"| **Total** | **{dstr(s2['sql_total'], hd('sql_total'))}** | "
      f"**{dstr(s2['sql_u30'], hd('sql_u30'))}** | **{dstr(s2['sql_u60'], hd('sql_u60'))}** |")
    A("")

    # =====================================================================
    A("# SECTION 3 — ADMIN / DATA-HYGIENE WORKLIST")
    A("")
    A("*Actionable fix-list; record IDs are in the snapshot JSON for click-through.*")
    A("")
    s3 = m["s3"]
    A(f"## 3A New accounts to complete ({len(s3['accounts_to_complete'])})")
    A("*B2B companies created in the last 7 days missing an owner and/or a name.*")
    A("")
    if s3["accounts_to_complete"]:
        A("| Company ID | Created | Owner | Name |")
        A("|------------|---------|-------|------|")
        for c in s3["accounts_to_complete"]:
            A(f"| {c['id']} | {c['createdate']} | {c['owner']} | {c['name']} |")
    else:
        A("- ✅ None.")
    A("")

    A(f"## 3B Deals missing a USD value ({len(s3['deals_missing_usd'])})")
    A("*Open B2B deals where `amount_in_home_currency` is empty — these silently distort "
      "pipeline value and are never valued off raw amount.*")
    A("")
    if s3["deals_missing_usd"]:
        A("| Deal | Stage | Owner | Raw amount | Currency |")
        A("|------|-------|-------|-----------|----------|")
        for d in s3["deals_missing_usd"]:
            ra = "—" if d["raw_amount"] is None else f"{d['raw_amount']:,.0f}"
            A(f"| {d['dealname']} | {d['stage']} | {d['owner']} | {ra} | {d.get('currency') or '—'} |")
    else:
        A("- ✅ None.")
    A("")

    A(f"## 3C Miscategorised leads ({dstr(s3['miscat_total'], hd('miscat'))})")
    A("*B2B contacts whose lead status shows sales is working them "
      f"({', '.join(SALES_WORKING_STATUSES)}) but whose lifecycle stage is still only "
      "Lead / MQL — they should be SQL or beyond.*")
    A("")
    if s3["miscat_rows"]:
        A("| Owner | Count | By status |")
        A("|-------|------:|-----------|")
        for r in s3["miscat_rows"]:
            st = ", ".join(f"{k}: {v}" for k, v in r["statuses"].items())
            A(f"| {r['owner']} | {r['count']} | {st} |")
    else:
        A("- ✅ None.")
    A("")

    # =====================================================================
    A("# SECTION 4 — RECYCLER HEALTH CHECK")
    A("")
    A("*Does the 90-day stale-SQL recycler (SQL→MQL after 90d no activity: sets "
      "`recycled_date`, `unqualified_reason='No response / did not connect'`, enrols in "
      "\"SQL Recycle — Re-engagement Nurture\") actually work? This section diagnoses; it "
      "does not change the workflow.*")
    A("")
    trig = s4["trigger"]
    A(f"## 4A Trigger match NOW (SQL + `{RECYCLER_TRIGGER_STATUS}` + no activity {RECYCLE_DAYS}+d)")
    A("")
    A("| Customer type | Matches |")
    A("|---------------|--------:|")
    for k in ["B2B", "B2C", "Partner", "Internal", "Partner-B2B"]:
        if k in trig:
            A(f"| {k} | {trig[k]} |")
    A("")
    if s4["alert_b2b_zero"]:
        A(f"> ⚠️ **ALERT — recycler catches no B2B SQLs.** B2B trigger matches = "
          f"**{trig.get('B2B', 0)}** while the stale B2B SQL backlog (untouched {RECYCLE_DAYS}+d) "
          f"is **{s4['stale_b2b_sql']}**. The workflow keys on `{RECYCLER_TRIGGER_STATUS}`, but the "
          "stale B2B SQLs don't carry that status — so it recycles none of them (see 4B).")
        A("")
    if s4["alert_nonb2b"]:
        nb = ", ".join(f"{k}: {v}" for k, v in s4["nonb2b_matches"].items())
        A(f"> ⚠️ **ALERT — recycler fires on non-B2B records it should leave alone:** {nb}. "
          "These (Partner / Internal / B2C) should not be recycled by this workflow.")
        A("")

    A(f"## 4B Stale B2B SQL backlog the trigger MISSES ({s4['missed_total']})")
    A(f"*B2B SQLs untouched {RECYCLE_DAYS}+d that are NOT `{RECYCLER_TRIGGER_STATUS}` — the "
      "population the recycler should catch but doesn't, grouped by lead status.*")
    A("")
    if s4["missed_rows"]:
        A("| Lead status | Count |")
        A("|-------------|------:|")
        for r in s4["missed_rows"]:
            A(f"| {r['status']} | {r['count']} |")
        A(f"| **Total** | **{s4['missed_total']}** |")
    else:
        A("- None.")
    A("")

    A(f"## 4C Recycled this week ({dstr(s4['recycled_total'], None)})")
    A("*Contacts whose `recycled_date` falls in the last 7 days — proof the workflow fired.*")
    A("")
    if s4["recycled_by_type"]:
        A("| Customer type | Recycled |")
        A("|---------------|--------:|")
        for k, v in sorted(s4["recycled_by_type"].items(), key=lambda kv: -kv[1]):
            A(f"| {k} | {v} |")
    else:
        A("- None this week.")
    A("")

    A(f"## 4D 🚩 Bounce-back — recycled but back at SQL ({dstr(s4['bounce_total'], hd('bounce_back'))})")
    A("*Contacts with `recycled_date` set AND lifecycle stage back to Sales Qualified Lead. "
      "A non-zero count means the recycle isn't sticking — something re-promotes them "
      "(suspect: lifecycle sync from an associated company/deal still at SQL/Opportunity).*")
    A("")
    if s4["bounce_total"]:
        A(f"> ⚠️ **{s4['bounce_total']} recycled B2B contacts have returned to SQL.** The recycle "
          "is being undone. Re-scoping the recycler enrolment and fixing the re-promotion loop "
          "are separate HubSpot workflow changes.")
        A("")
        A("| By owner | Count | | By lead status | Count |")
        A("|----------|------:|---|----------------|------:|")
        bo = sorted(s4["bounce_by_owner"].items(), key=lambda kv: -kv[1])
        bs = sorted(s4["bounce_by_status"].items(), key=lambda kv: -kv[1])
        for i in range(max(len(bo), len(bs))):
            lo = f"{bo[i][0]} | {bo[i][1]}" if i < len(bo) else " | "
            ls = f"{bs[i][0]} | {bs[i][1]}" if i < len(bs) else " | "
            A(f"| {lo} | | {ls} |")
    else:
        A("- ✅ No bounce-backs.")
    A("")

    A("---")
    A(f"*Snapshot: `snapshots/{date}.json` (raw data + all worklist record IDs) · "
      f"Report: `reports/{date}.md`*")
    A("")
    return "\n".join(out)


# =========================================================================
# Main
# =========================================================================
def main():
    ap = argparse.ArgumentParser(description="Brahmarsive weekly pipeline & MQL health report")
    ap.add_argument("--input", required=True, help="Pre-pulled raw JSON (raw_input.json)")
    ap.add_argument("--outdir", default=".", help="Repo root for snapshots/ & reports/")
    args = ap.parse_args()

    with open(args.input, encoding="utf-8") as f:
        raw = json.load(f)

    # Guard: the B2B company filter must not silent-fail.
    g = raw.get("guard", {})
    cb2b, call = g.get("companies_b2b"), g.get("companies_all")
    if cb2b in (None, 0) or (call is not None and cb2b == call):
        raise SystemExit(
            f"GUARD FAILED: B2B company count ({cb2b}) is 0 or equals the unfiltered "
            f"count ({call}) — silent-fail signature on customer_type__cloned_. Aborting.")

    metrics = compute(raw)
    run_date = dt.date.fromisoformat(raw["run_date"])
    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev(snap_dir, run_date)

    snapshot = {
        "run_date": raw["run_date"], "generated_at": raw.get("generated_at"),
        "window_days": raw.get("window_days", WINDOW_DAYS),
        "scope": "B2B", "source": raw.get("source"),
        "guard": raw.get("guard"), "context_counts": raw.get("context_counts"),
        "metrics": metrics,
        "raw": {k: raw[k] for k in (
            "owners", "sql_b2b", "miscat_b2b", "contacts_b2b_new", "assoc_companies",
            "companies_b2b_new", "recycled_recent", "deals_b2b",
            "recycler_4a_trigger_match", "recycled_ever_count") if k in raw},
    }
    snap_path = os.path.join(snap_dir, f"{run_date.isoformat()}.json")
    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    report = render(metrics, prev)
    rep_path = os.path.join(rep_dir, f"{run_date.isoformat()}.md")
    with open(rep_path, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"Wrote {snap_path}")
    print(f"Wrote {rep_path}")
    print(f"Prev snapshot: {prev['run_date'] if prev else 'none (first run of this format)'}")


if __name__ == "__main__":
    main()
