#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report  (Process version: Oct 2026)

Consumes a pre-pulled raw_input.json (built by build_raw_input.py from HubSpot
MCP caches, because direct egress to api.hubapi.com is blocked in the run
environment) and writes:

  snapshots/YYYY-MM-DD.json              raw + every worklist id (machine)
  reports/YYYY-MM-DD.md                  Headline + Sections 1-6 (human)
  reports/YYYY-MM-DD_owner_<first>.md    one Section-5 block per owner (email-ready)

Run:
  python3 pipeline.py --input raw_input.json      # never run without --input

Scoping rules honoured (non-negotiable):
  * B2B only everywhere. Contact counts come from the CONTACT object; pipeline
    value comes from DEALS scoped to B2B companies. The two bases are never mixed.
  * Deal USD value = amount_in_home_currency. Empty/0 is flagged, never replaced
    by raw amount. Raw amount + currency shown only as secondary reference.
  * Lead-status rules match on the hs_lead_status *label* (from the live property
    definition), never a hard-coded internal value.

NB on the funnel: the B2B contact funnel is dominated by ~3066 plain 'lead'
rows that carry no per-record logic, so the funnel table/totals use the
aggregate funnel_counts while all worklists iterate the pulled contact records
(every lead that can appear in a worklist is pulled in full). See build_raw_input.py.
"""

import argparse
import datetime as dt
import json
import os

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
STAGE_ORDER = ["1217017229", "appointmentscheduled", "presentationscheduled",
               "1217017230", "contractsent", "decisionmakerboughtin"]
CLOSED_STAGES = ["closedwon", "closedlost"]
OPEN_STAGES = set(STAGE_ORDER)

SALES_OWNERS = ["Theo Zirogiannis", "Jack Makhlouf", "Anup Ghoshal", "Biju Rajesh"]
SALES_WORKING_STATUSES = ["Attempted to Contact", "Contacted"]  # labels

# Labels (aligned to the live property: the spec's "New" is stored as label
# "New Lead" for internal value NEW; aligned here so the guard does not false-fire).
VALID_STATUS_BY_LIFECYCLE = {
    "subscriber": ["New Lead", "Marketing Engaged"],
    "lead": ["New Lead", "Marketing Engaged"],
    "marketingqualifiedlead": ["MQL", "New Lead", "Unqualified"],
    "salesqualifiedlead": ["Attempted to Contact", "Contacted"],
    "opportunity": ["Qualified", "In Progress"],
    "customer": ["Customer"],
}
SPEC_LABEL_ALIGNMENTS = {"New": "New Lead"}  # for the run note

RECYCLE_DAYS = 90
RECYCLE_WARN_DAYS = 75
DEAL_IDLE_DAYS = 30
WINDOW_DAYS = 7
IDLE_BUCKETS = [30, 60]
QTB_STALE_DAYS = 14
CLAIM_WINDOW_RUN = "2026-10-12"
CLAIM_DEADLINE = "2026-10-16"
Q4_START = "2026-10-01"
Q4_END = "2026-12-31"
Q4_WEEKS = 13
Q4_TARGETS = {"new_deals": 20, "entered_trial": 7, "closed_won": 3}
MQL_FOLLOWUP_DAYS = 7

PORTAL = "50467095"
LINK = {
    "contact": f"https://app.hubspot.com/contacts/{PORTAL}/record/0-1/{{}}",
    "company": f"https://app.hubspot.com/contacts/{PORTAL}/record/0-2/{{}}",
    "deal":    f"https://app.hubspot.com/contacts/{PORTAL}/record/0-3/{{}}",
}
LIFECYCLE_LABELS = {
    "subscriber": "Email Contact", "lead": "Lead",
    "marketingqualifiedlead": "Marketing Qualified Lead",
    "salesqualifiedlead": "Sales Qualified Lead",
    "opportunity": "Opportunity", "customer": "Customer", "other": "Other",
}
LIFECYCLE_ORDER = ["subscriber", "lead", "marketingqualifiedlead",
                   "salesqualifiedlead", "opportunity", "customer", "other"]

# how many links to inline per worklist bucket before summarising the remainder
CAP = 25


# =========================================================================
# helpers
# =========================================================================
def D(s):
    return dt.date.fromisoformat(s[:10]) if s else None


def days_since(s, run):
    d = D(s)
    return (run - d).days if d else None


def money(v):
    try:
        return f"${float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def clink(cid, name):
    return f"[{name}]({LINK['contact'].format(cid)})"


def dlink(did, name):
    return f"[{name}]({LINK['deal'].format(did)})"


def colink(cid, name):
    return f"[{name}]({LINK['company'].format(cid)})"


def delta(cur, prev):
    if prev is None:
        return str(cur)
    diff = cur - prev
    if diff > 0:
        return f"{cur} (+{diff})"
    if diff < 0:
        return f"{cur} ({diff})"
    return f"{cur} (=)"


class Ctx:
    """Everything the renderer needs, computed once."""
    pass


# =========================================================================
# computation
# =========================================================================
def compute(raw, run):
    c = Ctx()
    c.run = run
    c.raw = raw
    c.owners = raw["owners"]
    c.lsmap = raw["lead_status_map"]
    c.contacts = raw["contacts_b2b"]
    c.deals = raw["deals_b2b"]
    c.companies_3a = raw["companies_3a"]
    c.funnel_counts = raw["funnel_counts"]
    c.new_company_ids = set(raw["new_company_ids"])
    c.totals = raw["totals"]
    c.non_b2b_sql = raw["non_b2b_sql"]

    q4s, q4e = D(Q4_START), D(Q4_END)
    recycle_cut = run - dt.timedelta(days=RECYCLE_DAYS)       # activity before => >90d
    win_cut = run - dt.timedelta(days=WINDOW_DAYS)

    def oname(oid):
        if not oid:
            return "Unassigned"
        return c.owners.get(str(oid), f"Owner {oid}")

    def sales_block(oid):
        """Return the Section-5 block owner: a SALES_OWNER, else 'Nicola Cole'."""
        n = oname(oid)
        return n if n in SALES_OWNERS else "Nicola Cole"

    def label(c_):
        v = c_.get("hs_lead_status")
        if not v:
            return None
        return c.lsmap.get(v, v)

    c.oname = oname
    c.sales_block = sales_block
    c.label = label

    # ---- guard: every label used in the rules must exist in the live map ---
    live_labels = set(c.lsmap.values())
    missing = []
    for labels in VALID_STATUS_BY_LIFECYCLE.values():
        for lb in labels:
            if lb not in live_labels:
                missing.append(lb)
    for st in SALES_WORKING_STATUSES:
        if st not in live_labels:
            missing.append(st)
    if missing:
        raise SystemExit("GUARD FAILED: lead_status_map is missing label(s) used "
                         f"in the rules: {sorted(set(missing))}. Aborting.")

    # ---- guard: company scoping sane -------------------------------------
    t = c.totals
    if t["b2b_company_total"] in (0, t["company_total"]):
        raise SystemExit("GUARD FAILED: B2B company count is 0 or the full "
                         "unfiltered count. Aborting.")

    # ---- per-contact enrichment ------------------------------------------
    for x in c.contacts:
        x["_stage"] = x["lifecyclestage"]
        x["_label"] = label(x)
        x["_owner"] = oname(x["owner_id"])
        x["_block"] = sales_block(x["owner_id"])
        x["_idle"] = days_since(x["notes_last_updated"], run)
        x["_new"] = bool(x["createdate"] and D(x["createdate"]) >= win_cut)
        x["_ndeals"] = int(x["num_associated_deals"]) if x.get("num_associated_deals") else 0

    sql = [x for x in c.contacts if x["_stage"] == "salesqualifiedlead"]
    opp = [x for x in c.contacts if x["_stage"] == "opportunity"]
    mql = [x for x in c.contacts if x["_stage"] == "marketingqualifiedlead"]
    c.sql, c.opp, c.mql = sql, opp, mql
    c.new_contacts = [x for x in c.contacts if x["_new"]]

    # ---- deals -----------------------------------------------------------
    for d in c.deals:
        d["_stage"] = d["dealstage"]
        d["_label"] = DEALSTAGE_LABELS.get(d["dealstage"], d["dealstage"])
        d["_owner"] = oname(d["owner_id"])
        d["_open"] = d["dealstage"] in OPEN_STAGES
        d["_idle"] = days_since(d["notes_last_updated"], run)
        try:
            d["_usd"] = float(d["amount_in_home_currency"]) if d["amount_in_home_currency"] not in (None, "") else None
        except ValueError:
            d["_usd"] = None
        d["_usd_bad"] = (d["_usd"] is None) or (d["_usd"] == 0)
    c.open_deals = [d for d in c.deals if d["_open"]]

    # ---- Q4 scorecard ----------------------------------------------------
    def in_q4(s):
        d = D(s)
        return bool(d and q4s <= d <= q4e)
    q4_new = [d for d in c.deals if in_q4(d["createdate"])]
    q4_trial = [d for d in c.deals if in_q4(d["date_entered_disc_trial"])]
    q4_won = [d for d in c.deals if d["dealstage"] == "closedwon" and in_q4(d["closedate"])]
    weeks_elapsed = max(0.0, (run - q4s).days / 7.0)
    frac = min(1.0, weeks_elapsed / Q4_WEEKS)

    def owner_contrib(deals):
        agg = {}
        for d in deals:
            agg[d["_owner"]] = agg.get(d["_owner"], 0) + 1
        return ", ".join(f"{k}: {v}" for k, v in sorted(agg.items(), key=lambda kv: -kv[1])) or "—"

    c.q4 = {
        "weeks_elapsed": weeks_elapsed, "frac": frac,
        "rows": [
            ("New deals", "createdate in Q4", len(q4_new), Q4_TARGETS["new_deals"],
             round(Q4_TARGETS["new_deals"] * frac), owner_contrib(q4_new)),
            ("Entered trial", "date entered Discovery & Trial in Q4 (cumulative)",
             len(q4_trial), Q4_TARGETS["entered_trial"],
             round(Q4_TARGETS["entered_trial"] * frac), owner_contrib(q4_trial)),
            ("Closed won", "closedwon with closedate in Q4", len(q4_won),
             Q4_TARGETS["closed_won"], round(Q4_TARGETS["closed_won"] * frac),
             owner_contrib(q4_won)),
        ],
    }

    # ---- Section 1 : pipeline -------------------------------------------
    stage_rows = []
    for st in STAGE_ORDER:
        ds = [d for d in c.open_deals if d["_stage"] == st]
        idles = sorted([d["_idle"] for d in ds if d["_idle"] is not None])
        med = idles[len(idles) // 2] if idles else None
        stage_rows.append({
            "stage": st, "label": DEALSTAGE_LABELS[st], "qty": len(ds),
            "usd": sum(d["_usd"] or 0 for d in ds),
            "median_idle": med,
            "idle14": len([d for d in ds if d["_idle"] is not None and d["_idle"] >= QTB_STALE_DAYS]),
        })
    c.stage_rows = stage_rows
    c.open_qty = sum(r["qty"] for r in stage_rows)
    c.open_usd = sum(r["usd"] for r in stage_rows)
    c.closed_ref = {}
    for st in CLOSED_STAGES:
        ds = [d for d in c.deals if d["_stage"] == st]
        c.closed_ref[st] = (len(ds), sum(d["_usd"] or 0 for d in ds))

    byowner = {}
    for d in c.open_deals:
        e = byowner.setdefault(d["_owner"], {"qty": 0, "usd": 0.0})
        e["qty"] += 1
        e["usd"] += d["_usd"] or 0
    c.owner_pipeline = sorted(byowner.items(), key=lambda kv: -kv[1]["usd"])

    c.moved = [d for d in c.open_deals
               if d["date_entered_current_stage"]
               and days_since(d["date_entered_current_stage"], run) is not None
               and days_since(d["date_entered_current_stage"], run) <= WINDOW_DAYS]

    # ---- Section 2.3 : SQL by owner --------------------------------------
    sqlo = {}
    for x in sql:
        e = sqlo.setdefault(x["_owner"], {"qty": 0, "i30": 0, "i60": 0})
        e["qty"] += 1
        if x["_idle"] is None or x["_idle"] >= 30:
            e["i30"] += 1
        if x["_idle"] is None or x["_idle"] >= 60:
            e["i60"] += 1
    c.sql_by_owner = sorted(sqlo.items(), key=lambda kv: -kv[1]["qty"])

    # ---- 2.1 / 2.2 new companies & contacts ------------------------------
    c.new_contacts_existing = [x for x in c.new_contacts
                               if x["associatedcompanyid"] and x["associatedcompanyid"] not in c.new_company_ids]
    c.new_contacts_newacct = [x for x in c.new_contacts
                              if x["associatedcompanyid"] in c.new_company_ids]
    c.new_companies = [co for co in c.companies_3a if co["id"] in c.new_company_ids]
    # (new B2B companies in the window all happen to also be in the 3A set this run)

    # ---- Section 3 worklists --------------------------------------------
    # 3A
    c.a3 = c.companies_3a
    # 3B deal hygiene
    c.b3_usd = [d for d in c.open_deals if d["_usd_bad"]]
    c.b3_idle = [d for d in c.open_deals if d["_idle"] is not None and d["_idle"] >= DEAL_IDLE_DAYS]
    c.b3_pastclose = [d for d in c.open_deals if D(d["closedate"]) and D(d["closedate"]) < run]
    c.b3_noowner = [d for d in c.open_deals if not d["owner_id"]]
    # 3C miscategorised
    c.c3 = [x for x in c.contacts
            if x["_label"] in SALES_WORKING_STATUSES
            and x["_stage"] in ("subscriber", "lead", "marketingqualifiedlead")]
    # 3D status mismatches
    d3 = []
    for x in c.contacts:
        st = x["_stage"]
        valid = VALID_STATUS_BY_LIFECYCLE.get(st)
        if valid is None:
            continue
        lab = x["_label"]
        bad = False
        expected = "/".join(valid)
        if st in ("salesqualifiedlead", "opportunity", "customer"):
            if lab is None:                 # blank at SQL or beyond
                bad = True
            elif lab not in valid:
                bad = True
        else:                               # subscriber / lead / mql
            if lab is not None and lab not in valid:
                bad = True
        # opportunity deal-coupling rules
        if st == "opportunity":
            if lab == "In Progress" and x["_ndeals"] > 0:
                bad, expected = True, "Qualified (has deals)"
            elif lab == "Qualified" and x["_ndeals"] == 0:
                bad, expected = True, "In Progress (0 deals)"
        if bad:
            d3.append((x, expected))
    c.d3 = d3
    # 3E orphans (new, no company)
    c.e3 = [x for x in c.new_contacts if not x["associatedcompanyid"]]
    # 3F lost-deal leftovers: Opportunity contacts whose associated deals are ALL closed lost
    #    (contact->deal association not pulled; approximated via num_associated_deals>0 and
    #     no OPEN/WON company deal for the contact's associated company). We flag the clear
    #     case: opportunity + Qualified + has deals, but the associated company's deals are
    #     all closed-lost.
    deals_by_company = {}
    for d in c.deals:
        deals_by_company.setdefault(d["company_id"], []).append(d)
    f3 = []
    for x in opp:
        if x["_ndeals"] <= 0:          # contact must actually have deal(s)
            continue
        cid = x["associatedcompanyid"]
        cds = deals_by_company.get(cid, [])
        if cds and all(d["dealstage"] == "closedlost" for d in cds):
            f3.append(x)
    c.f3 = f3
    # 3G MQL follow-up
    g3 = []
    for x in mql:
        if x["_label"] == "MQL":
            # days since entering MQL vs last sales activity
            since_mql = days_since(x["date_entered_mql"], run)
            idle = x["_idle"]
            no_activity = (idle is None) or (idle >= MQL_FOLLOWUP_DAYS)
            if since_mql is not None and since_mql >= MQL_FOLLOWUP_DAYS and no_activity:
                g3.append(("no activity " + (f"{idle}d" if idle is not None else "ever"), x))
    g3 += [("no owner", x) for x in mql if not x["owner_id"] and x not in [y for _, y in g3]]
    c.g3 = g3
    # 3H won but not customer (company type still Prospect)
    won_companies = {}
    for d in c.deals:
        if d["dealstage"] == "closedwon" and d["company_id"]:
            won_companies.setdefault(d["company_id"], d)
    c.h3 = [d for cid, d in won_companies.items() if (d["company_type"] or "").upper() == "PROSPECT"]
    c.h3_blanktype = [d for cid, d in won_companies.items()
                      if (d["company_type"] or "") not in ("PROSPECT", "Customer")]

    # ---- Section 4 recycler --------------------------------------------
    def trigger(x):
        """Live recycler trigger: SQL AND (last activity >90d OR (never active AND created>90d))."""
        if x["_stage"] != "salesqualifiedlead":
            return False
        idle = x["_idle"]
        if idle is not None:
            return idle > RECYCLE_DAYS
        cd = D(x["createdate"])
        return bool(cd and (run - cd).days > RECYCLE_DAYS)
    c.a4 = [x for x in sql if trigger(x)]
    c.a4_recycled = [x for x in c.a4 if x["recycled_date"]]      # matched but already recycled (stuck)
    c.a4_never = [x for x in c.a4 if not x["recycled_date"]]     # matched and never recycled
    # 4C recycled this week
    c.c4 = [x for x in c.contacts
            if x["recycled_date"] and days_since(x["recycled_date"], run) is not None
            and 0 <= days_since(x["recycled_date"], run) <= WINDOW_DAYS]
    c.c4_still_sql = [x for x in c.c4 if x["_stage"] == "salesqualifiedlead"]
    # 4D stuck recycles & re-promotions
    stuck, repromo = [], []
    for x in c.contacts:
        if x["recycled_date"] and x["_stage"] == "salesqualifiedlead":
            des, rd = D(x["date_entered_sql"]), D(x["recycled_date"])
            if des and rd and des > rd:
                repromo.append(x)
            else:
                stuck.append(x)
    c.d4_stuck, c.d4_repromo = stuck, repromo

    # ---- Section 6 workflow firing checks -------------------------------
    c3_thisweek = [x for x in c.c3
                   if (x["createdate"] and D(x["createdate"]) >= win_cut)
                   or (x["notes_last_updated"] and D(x["notes_last_updated"]) >= win_cut)]
    contacted_wrong = [x for x in c.contacts
                       if x["_label"] == "Contacted"
                       and x["_stage"] in ("subscriber", "lead", "marketingqualifiedlead")]
    d3_opp_cust = [t for t in d3 if t[0]["_stage"] in ("opportunity", "customer")]
    opp_inprog_deals = [x for x in opp if x["_label"] == "In Progress" and x["_ndeals"] > 0]
    newdeals_badusd = [d for d in c.deals
                       if d["_open"] and d["createdate"] and D(d["createdate"]) >= win_cut
                       and d["_usd_bad"]]
    c.wf = [
        ("Sales Activity > Lead Status", "3C contacts created/modified this week = 0",
         len(c3_thisweek) == 0, [clink(x["id"], x["name"]) for x in c3_thisweek]),
        ("Meeting Booked > Contacted", 'no "Contacted" at Email Contact / Lead / MQL',
         len(contacted_wrong) == 0, [clink(x["id"], x["name"]) for x in contacted_wrong]),
        ("Stale SQL Recycler", "4A overdue = 0 (after claim window) and 4D stuck = 0",
         len(c.a4) == 0 and len(c.d4_stuck) == 0,
         [f"{len(c.a4)} trigger-matches, {len(c.d4_stuck)} stuck"]),
        ("Lifecycle > Lead Status Sync", "3D rows at Opportunity/Customer = 0",
         len(d3_opp_cust) == 0, [clink(t[0]["id"], t[0]["name"]) for t in d3_opp_cust]),
        ("Deal Associated > Qualified", "Opportunity + In Progress with deals > 0 = 0",
         len(opp_inprog_deals) == 0, [clink(x["id"], x["name"]) for x in opp_inprog_deals]),
        ("Deal required fields", "open deals created this week with empty/zero USD = 0",
         len(newdeals_badusd) == 0, [dlink(d["id"], d["dealname"]) for d in newdeals_badusd]),
    ]
    c.wf_ok = sum(1 for _, _, ok, _ in c.wf if ok)

    # ---- date flags ------------------------------------------------------
    c.claim_today = (run.isoformat() == CLAIM_WINDOW_RUN)
    c.after_deadline = (run > D(CLAIM_DEADLINE))

    return c


# =========================================================================
# rendering helpers
# =========================================================================
def group_by_block(items, keyfn):
    g = {}
    for it in items:
        g.setdefault(keyfn(it), []).append(it)
    return g


def owner_order(keys):
    order = SALES_OWNERS + ["Nicola Cole"]
    ranked = [k for k in order if k in keys]
    rest = sorted(k for k in keys if k not in order)
    return ranked + rest


# =========================================================================
# report
# =========================================================================
def render(c, prev):
    run = c.run
    out = []
    A = out.append
    P = lambda: out.append("")

    A(f"# Brahmarsive Weekly B2B Pipeline & MQL Health — {run.isoformat()}")
    P()
    A(f"*Process version: Oct 2026. Scope: B2B only (contacts `customer_type = B2B`; "
      f"companies/deals `customer_type__cloned_ = B2B`). Contact counts from the CONTACT "
      f"object; pipeline value from DEALS (USD via `amount_in_home_currency`). "
      f"Generated {c.raw['generated_at'][:19]}Z.*")
    P()
    note = ", ".join(f'"{k}"->"{v}"' for k, v in SPEC_LABEL_ALIGNMENTS.items())
    A(f"*Lead-status rules match on the live property labels; spec shorthand aligned to "
      f"portal labels ({note}).*")
    P()
    if prev is None or not prev.get("comparable"):
        A("*No comparable prior snapshot (new Oct-2026 schema) — week-on-week comparison "
          "starts next run.*")
    else:
        A(f"*Week-on-week deltas vs `{prev['run_date']}`.*")
    P()

    pv = prev if (prev and prev.get("comparable")) else None

    def pget(k):
        return pv["headline"].get(k) if pv else None

    # ---- Q4 SCORECARD ----------------------------------------------------
    A("## Q4 Scorecard")
    P()
    A(f"*B2B deals, {Q4_START} → {run.isoformat()} · week {c.q4['weeks_elapsed']:.1f} of {Q4_WEEKS}.*")
    P()
    A("| Metric | Definition | Actual | Target | Expected by now | Status |")
    A("|--------|------------|-------:|-------:|----------------:|:------:|")
    for name, defn, actual, target, expected, _ in c.q4["rows"]:
        status = "✅" if actual >= expected else "⚠️"
        A(f"| {name} | {defn} | {actual} | {target} | {expected} | {status} |")
    P()
    for name, _, actual, _, _, contrib in c.q4["rows"]:
        wow = ""
        if pv:
            p = pv["headline"].get("q4_" + name.lower().replace(" ", "_"))
            if p is not None:
                wow = f" · WoW {actual - p:+d}"
        A(f"- **{name}** — by owner: {contrib}{wow}")
    P()

    # ---- HEADLINE FLAGS --------------------------------------------------
    A("## 🔔 Headline Flags")
    P()
    pct_idle = (len(c.b3_idle) / len(c.open_deals) * 100) if c.open_deals else 0
    sql_untouched30 = len([x for x in c.sql if x["_idle"] is None or x["_idle"] >= 30])
    pct_sql30 = (sql_untouched30 / len(c.sql) * 100) if c.sql else 0
    A(f"- **Workflow health:** {c.wf_ok}/6 ✅ "
      f"({6 - c.wf_ok} ⚠️ — see Section 6).")
    A(f"- **Deals that changed stage this week:** {len(c.moved)}")
    A(f"- **Open deals idle {QTB_STALE_DAYS}+d:** {len(c.b3_idle)}/{len(c.open_deals)} ({pct_idle:.0f}%)")
    A(f"- **SQLs untouched 30+d:** {sql_untouched30}/{len(c.sql)} ({pct_sql30:.0f}%)")
    A(f"- **Miscategorised leads (3C):** {delta(len(c.c3), pget('c3'))}")
    A(f"- **Status mismatches (3D):** {delta(len(c.d3), pget('d3'))}")
    A(f"- **Stuck recycles (4D):** {delta(len(c.d4_stuck), pget('d4'))}  ⚠️")
    A(f"- **Open deals past close date:** {len(c.b3_pastclose)}")
    A(f"- **Open deals missing/zero USD:** {len(c.b3_usd)}")
    A(f"- **New accounts with no owner:** {len(c.new_companies)}")
    A(f"- **Recycled this week (4C):** {delta(len(c.c4), pget('c4'))} "
      f"({len(c.c4_still_sql)} still at SQL)")
    A(f"- **4A recycle-trigger backlog:** {len(c.a4)} B2B SQLs match the recycler trigger "
      f"({len(c.a4_never)} never recycled, {len(c.a4_recycled)} recycled-but-stuck)")
    P()

    # ---- SECTION 1 -------------------------------------------------------
    A("## Section 1 — Current Pipeline Status")
    P()
    A("### 1.1 Open pipeline by stage (B2B, open only)")
    P()
    A("| Stage | Deals | Total USD | Median days idle | Idle ≥14d |")
    A("|-------|------:|----------:|-----------------:|----------:|")
    for r in c.stage_rows:
        med = r["median_idle"] if r["median_idle"] is not None else "—"
        A(f"| {r['label']} | {r['qty']} | {money(r['usd'])} | {med} | {r['idle14']} |")
    A(f"| **Open subtotal** | **{c.open_qty}** | **{money(c.open_usd)}** | | |")
    P()
    cw, cwv = c.closed_ref["closedwon"]
    cl, clv = c.closed_ref["closedlost"]
    A(f"*Reference (not in open subtotal): Closed Won {cw} / {money(cwv)} · "
      f"Closed Lost {cl} / {money(clv)}.*")
    P()
    A("### 1.2 Open pipeline by sales owner")
    P()
    A("| Owner | Open deals | Open USD |")
    A("|-------|-----------:|---------:|")
    for owner, e in c.owner_pipeline:
        A(f"| {owner} | {e['qty']} | {money(e['usd'])} |")
    P()
    if c.open_usd:
        top_deal = max(c.open_deals, key=lambda d: d["_usd"] or 0)
        share = (top_deal["_usd"] or 0) / c.open_usd * 100
        A(f"- **Concentration:** top open deal — {dlink(top_deal['id'], top_deal['dealname'])} "
          f"({top_deal['_owner']}) — {money(top_deal['_usd'])} = {share:.0f}% of open value.")
        to = c.owner_pipeline[0]
        A(f"- Top owner {to[0]} holds {money(to[1]['usd'])} "
          f"({to[1]['usd'] / c.open_usd * 100:.0f}% of open value).")
    P()
    A("### 1.3 Stage moved / updated in the last 7 days")
    P()
    if c.moved:
        A("**Stage moved** (entered current stage within 7 days):")
        A("")
        A("| Deal | Owner | Current stage | USD | Last activity |")
        A("|------|-------|---------------|----:|---------------|")
        for d in sorted(c.moved, key=lambda d: -(d["_usd"] or 0)):
            A(f"| {dlink(d['id'], d['dealname'])} | {d['_owner']} | {d['_label']} | "
              f"{money(d['_usd'])} | {d['notes_last_updated'] or '—'} |")
    else:
        A("- No deals changed stage in the last 7 days.")
    P()
    A("- *UPDATED (field change without stage move): `hs_lastmodifieddate` was not carried "
      "into this run's deal extract, and a bulk field update on 2026-11-02 touched most "
      "deals; itemising it would be noise, so only real stage progression is shown above.*")
    P()

    # ---- SECTION 2 -------------------------------------------------------
    A("## Section 2 — New Pipeline Status")
    P()
    A("### 2.1 New companies created in the last 7 days")
    P()
    if c.new_companies:
        A("| Company | Created | Owner |")
        A("|---------|---------|-------|")
        for co in c.new_companies:
            nm = co["name"] or "*(blank name)*"
            A(f"| {colink(co['id'], nm)} | {(co['createdate'] or '')[:10]} | "
              f"{c.oname(co['owner_id'])} |")
        A("")
        A(f"- ⚠️ All {len(c.new_companies)} new B2B companies have **no owner and no name** "
          f"(see 3A).")
    else:
        A("- No new B2B companies this week.")
    P()
    A("### 2.2 New contacts on existing vs new accounts (last 7 days)")
    P()
    A(f"- New B2B contacts in window: **{len(c.new_contacts)}** "
      f"(on existing accounts: {len(c.new_contacts_existing)}, "
      f"on new accounts: {len(c.new_contacts_newacct)}, orphans: {len(c.e3)}).")
    P()
    byb = group_by_block(c.new_contacts_existing, lambda x: x["_owner"])
    if byb:
        A("New contacts on **existing** accounts, by owner → lifecycle:")
        A("")
        for ow in owner_order(set(byb)):
            rows = byb[ow]
            stages = {}
            for x in rows:
                stages[LIFECYCLE_LABELS.get(x["_stage"], x["_stage"])] = \
                    stages.get(LIFECYCLE_LABELS.get(x["_stage"], x["_stage"]), 0) + 1
            sdesc = ", ".join(f"{k}: {v}" for k, v in stages.items())
            A(f"- **{ow}** ({len(rows)}): {sdesc}")
            for x in rows[:CAP]:
                A(f"  - {clink(x['id'], x['name'])} — {LIFECYCLE_LABELS.get(x['_stage'], x['_stage'])}"
                  f" / {x['_label'] or '—'}")
    P()
    A("### 2.3 SQLs by sales owner (true SQL total — contacts only)")
    P()
    A("| Owner | SQLs | Not touched 30+d | Not touched 60+d |")
    A("|-------|-----:|-----------------:|-----------------:|")
    t30 = t60 = tot = 0
    for owner, e in c.sql_by_owner:
        A(f"| {owner} | {e['qty']} | {e['i30']} | {e['i60']} |")
        tot += e["qty"]; t30 += e["i30"]; t60 += e["i60"]
    A(f"| **Total** | **{tot}** | **{t30}** | **{t60}** |")
    P()

    # ---- SECTION 3 -------------------------------------------------------
    A("## Section 3 — Admin / Data-Hygiene Worklist")
    P()
    # 3A
    A(f"### 3A Companies to complete — {len(c.a3)} B2B companies missing owner and/or name")
    P()
    A("*Owner of this list: Nicola.*")
    A("")
    A("| Company | Created | Owner | Name | Source | Created by |")
    A("|---------|---------|-------|------|--------|-----------|")
    for co in c.a3[:CAP]:
        nm = co["name"] or "**blank**"
        own = c.oname(co["owner_id"]) if co["owner_id"] else "**none**"
        A(f"| {colink(co['id'], co['id'])} | {(co['createdate'] or '')[:10]} | {own} | {nm} | "
          f"{co['original_source'] or '—'} | {co['created_by'] or '—'} |")
    if len(c.a3) > CAP:
        A(f"| …and {len(c.a3) - CAP} more | | | | | |")
    P()
    # 3B
    A("### 3B Deal hygiene (open B2B deals)")
    P()
    def deal_table(title, deals):
        A(f"**{title}** ({len(deals)})")
        A("")
        if not deals:
            A("- none")
            A("")
            return
        A("| Deal | Stage | Owner | USD | Last activity | Close date |")
        A("|------|-------|-------|----:|---------------|-----------|")
        for d in sorted(deals, key=lambda d: -(d["_usd"] or 0))[:CAP]:
            A(f"| {dlink(d['id'], d['dealname'])} | {d['_label']} | {d['_owner']} | "
              f"{money(d['_usd']) if d['_usd'] is not None else '**empty**'} | "
              f"{d['notes_last_updated'] or '—'} | {d['closedate'] or '—'} |")
        if len(deals) > CAP:
            A(f"| …and {len(deals) - CAP} more | | | | | |")
        A("")
    deal_table("(i) amount_in_home_currency empty or 0", c.b3_usd)
    deal_table(f"(ii) idle {DEAL_IDLE_DAYS}+ days", c.b3_idle)
    deal_table("(iii) close date in the past", c.b3_pastclose)
    deal_table("(iv) no deal owner", c.b3_noowner)
    # 3C
    A(f"### 3C Miscategorised leads — {len(c.c3)} contacts")
    P()
    A("*Sales working status (Attempted to Contact / Contacted) but lifecycle still "
      "subscriber/lead/MQL — should be SQL+.*")
    A("")
    _by_owner_list(A, c.c3, lambda x: x["_block"], lambda x:
                   f"{clink(x['id'], x['name'])} — {LIFECYCLE_LABELS.get(x['_stage'], x['_stage'])} / {x['_label']}")
    # 3D
    A(f"### 3D Status mismatches — {len(c.d3)} contacts")
    P()
    A("*Lead-status label not valid for the lifecycle (incl. blank at SQL+), plus the "
      "Opportunity deal-coupling rules.*")
    A("")
    # summary by (lifecycle, status -> expected)
    summ = {}
    for x, exp in c.d3:
        k = (LIFECYCLE_LABELS.get(x["_stage"], x["_stage"]), x["_label"] or "(blank)", exp)
        summ[k] = summ.get(k, 0) + 1
    A("| Lifecycle | Current status | Expected | Count |")
    A("|-----------|----------------|----------|------:|")
    for (lc, st, exp), n in sorted(summ.items(), key=lambda kv: -kv[1]):
        A(f"| {lc} | {st} | {exp} | {n} |")
    A("")
    A("By owner (links):")
    A("")
    _by_owner_list(A, [t[0] for t in c.d3], lambda x: x["_block"], lambda x:
                   f"{clink(x['id'], x['name'])} — {LIFECYCLE_LABELS.get(x['_stage'], x['_stage'])} / {x['_label'] or '(blank)'}",
                   per_owner_cap=15)
    # 3E
    A(f"### 3E Orphan contacts (new, no company) — {len(c.e3)}")
    P()
    if c.e3:
        _by_owner_list(A, c.e3, lambda x: x["_block"], lambda x: clink(x["id"], x["name"]))
    else:
        A("- None this week (every new B2B contact is associated to a company).")
        P()
    # 3F
    A(f"### 3F Lost-deal leftovers — {len(c.f3)} Opportunity contacts")
    P()
    A("*At Opportunity, have associated deal(s), and their account's deals are all Closed "
      "Lost (no open, no won). Approximated at company level — the contact→deal association "
      "is not queryable in the MCP SQL layer, so this joins each contact's associated "
      "company to its deals. Rule TBC: recycle to MQL + Unqualified \"Lost deal\".*")
    A("")
    if c.f3:
        _by_owner_list(A, c.f3, lambda x: x["_block"], lambda x:
                       f"{clink(x['id'], x['name'])} — {x['_label'] or '—'}")
    else:
        A("- None detected.")
        P()
    # 3G
    A(f"### 3G MQL follow-up — {len(c.g3)} contacts")
    P()
    if c.g3:
        _by_owner_list(A, c.g3, lambda t: t[1]["_block"], lambda t:
                       f"{clink(t[1]['id'], t[1]['name'])} — {t[0]}")
    else:
        A("- None this week.")
        P()
    # 3H
    A(f"### 3H Won but not Customer — {len(c.h3)} companies")
    P()
    A("*B2B companies with a Closed Won deal whose company type is still Prospect.*")
    A("")
    if c.h3:
        A("| Company | Won deal | Owner |")
        A("|---------|----------|-------|")
        for d in c.h3:
            A(f"| {colink(d['company_id'], d['company_id'])} | {dlink(d['id'], d['dealname'])} | {d['_owner']} |")
        A("")
    else:
        A("- None.")
        P()
    if c.h3_blanktype:
        A(f"- *Secondary: {len(c.h3_blanktype)} Closed-Won companies have a blank/other "
          f"company type (not Prospect, not Customer) — e.g. "
          f"{', '.join(dlink(d['id'], d['dealname'][:28]) for d in c.h3_blanktype[:4])}.*")
        P()

    # ---- SECTION 4 -------------------------------------------------------
    A("## Section 4 — Recycler Health Check")
    P()
    A("*Live recycler: B2B SQL AND (last activity >90d OR (no activity ever AND created "
      ">90d)) → clear lifecycle, set MQL, status Unqualified, recycled_date, enrol in "
      "\"SQL Recycle — Re-engagement Nurture\". Section 4 diagnoses the workflow; it does "
      "not change it.*")
    P()
    A(f"### 4A Trigger match now — {len(c.a4)} B2B SQLs")
    P()
    status = "⚠️ ALERT" if (c.after_deadline and c.a4) else "info (held backlog)"
    A(f"- Claim window closed {CLAIM_DEADLINE}; today is past it → **{status}**: "
      f"{len(c.a4)} B2B SQLs match the recycler trigger, so the recycler is not clearing "
      f"them. Of these, **{len(c.a4_never)} have never been recycled** and "
      f"**{len(c.a4_recycled)} were recycled but are stuck at SQL**.")
    A(f"- Non-B2B SQLs that meet the idle condition: {c.non_b2b_sql['idle90_trigger']}; "
      f"non-B2B SQLs actually recycled: **{c.non_b2b_sql['recycled']}** → B2B filter intact "
      f"(no non-B2B contact has been recycled).")
    P()
    A(f"### 4C Recycled this week — {len(c.c4)}")
    P()
    if c.c4:
        A(f"- {len(c.c4)} contacts have recycled_date in the last 7 days; "
          f"**{len(c.c4_still_sql)} are still at SQL** (the move to MQL did not complete → 4D).")
        A("")
        _by_owner_list(A, c.c4, lambda x: x["_block"], lambda x:
                       f"{clink(x['id'], x['name'])} — now {LIFECYCLE_LABELS.get(x['_stage'], x['_stage'])}"
                       f", recycled {x['recycled_date'][:10]}")
    else:
        A("- None recycled this week.")
        P()
    A(f"### 4D Stuck recycles — {len(c.d4_stuck)}  ⚠️ ALERT")
    P()
    A("*recycled_date set AND lifecycle still Sales Qualified Lead (the move to MQL failed "
      "— lifecycle not cleared first).*")
    A("")
    _by_owner_list(A, c.d4_stuck, lambda x: x["_block"], lambda x:
                   f"{clink(x['id'], x['name'])} — recycled {x['recycled_date'][:10]}, "
                   f"status {x['_label'] or '(blank)'}", per_owner_cap=15)
    A(f"- Re-promotions (recycled then legitimately re-entered SQL later): {len(c.d4_repromo)}.")
    P()

    # ---- SECTION 5 : OWNER ACTION LISTS ---------------------------------
    A("## Section 5 — Owner Action Lists")
    P()
    _render_owner_blocks(A, c, pv, for_main=True)

    # ---- SECTION 6 ------------------------------------------------------
    A("## Section 6 — Workflow Firing Checks (\"built ≠ working\")")
    P()
    A("| Workflow | Healthy if | Status |")
    A("|----------|------------|:------:|")
    for name, healthy, ok, _ in c.wf:
        A(f"| {name} | {healthy} | {'✅' if ok else '⚠️'} |")
    P()
    for name, _, ok, offenders in c.wf:
        if not ok and offenders:
            shown = offenders[:CAP]
            more = f" …and {len(offenders) - CAP} more" if len(offenders) > CAP else ""
            A(f"- **⚠️ {name}:** {', '.join(shown)}{more}")
    P()
    A("### Observed workflow defects")
    A("")
    A("- **Stale SQL Recycler** is firing but **not clearing lifecycle**: "
      f"{len(c.d4_stuck)} contacts carry recycled_date yet remain at SQL, and "
      f"{len(c.a4)} SQLs still match the trigger. The recycled records also show "
      "`unqualified_reason = \"No response/did not connect\"` (spec expects "
      "\"Stalled – no activity 90d\") and `Recycled Date` is documented as recycling "
      "\"back to Lead\" while the rebuilt spec sets MQL — both confirm the workflow does "
      "not match the Oct-2026 spec.")
    A("- **Lifecycle > Lead Status Sync / Deal Associated > Qualified**: see Section 6 "
      "table; mismatches at Opportunity/Customer and the SQL Unqualified backlog indicate "
      "the sync workflows are not reconciling status to lifecycle.")
    P()

    if c.after_deadline and len(c.a4) > 0:
        A("> **One-off reminder (Nicola):** the claim window closed "
          f"{CLAIM_DEADLINE}; {len(c.a4)} contacts remain in the 4A backlog. Open the "
          "Stale SQL Recycler workflow and manually enrol the remaining contacts "
          "(repeat each run until the backlog is 0).")
        P()

    A("---")
    A(f"*Snapshot: `snapshots/{run.isoformat()}.json` · Report: `reports/{run.isoformat()}.md` "
      f"· per-owner files: `reports/{run.isoformat()}_owner_<first>.md`*")
    P()
    return "\n".join(out)


def _by_owner_list(A, items, blockfn, descfn, per_owner_cap=CAP):
    if not items:
        A("- None.")
        A("")
        return
    g = group_by_block(items, blockfn)
    for ow in owner_order(set(g)):
        rows = g[ow]
        A(f"- **{ow}** ({len(rows)}):")
        for it in rows[:per_owner_cap]:
            A(f"  - {descfn(it)}")
        if len(rows) > per_owner_cap:
            A(f"  - …and {len(rows) - per_owner_cap} more")
    A("")


def _owner_block_body(c, owner, pv):
    """Return list of markdown lines for one owner's Section-5 block (items only)."""
    lines = []
    L = lines.append
    is_nicola = (owner == "Nicola Cole")

    def own(items, blockfn=lambda x: x["_block"]):
        return [x for x in items if blockfn(x) == owner]

    # a. changed this week (needs prior snapshot) -> deferred if none
    if pv:
        prevmap = pv.get("contact_states", {})
        changed = []
        for x in c.contacts:
            if x["_block"] != owner:
                continue
            p = prevmap.get(x["id"])
            if p and (p.get("lc") != x["_stage"] or p.get("ls") != (x["_label"] or "")):
                changed.append((x, p))
        if changed:
            L("**a. Your records changed this week** (review; flag anything wrong):")
            for x, p in changed[:CAP]:
                L(f"- {clink(x['id'], x['name'])}: "
                  f"{p.get('lc')}/{p.get('ls') or '—'} → {x['_stage']}/{x['_label'] or '—'}")
            L("")

    # b. recycling in next 15 days
    warn = []
    for x in own(c.sql):
        idle = x["_idle"]
        if idle is not None and RECYCLE_WARN_DAYS <= idle < RECYCLE_DAYS:
            warn.append((x, idle))
        elif idle is None and x["createdate"]:
            cdays = (c.run - D(x["createdate"])).days
            if RECYCLE_WARN_DAYS <= cdays < RECYCLE_DAYS:
                warn.append((x, cdays))
    if warn:
        L("**b. Recycling in the next 15 days** (log an activity now or they return to marketing):")
        for x, d in warn[:CAP]:
            L(f"- {clink(x['id'], x['name'])} — {d}d since last activity")
        L("")

    # c. claim window (only on CLAIM_WINDOW_RUN)
    if c.claim_today:
        cw = own(c.a4)
        if cw:
            L("**c. CLAIM WINDOW** — to keep a contact, log an activity (call, email or note) "
              f"by Fri 16 Oct. Any still without activity after that will be recycled:")
            for x in cw[:CAP]:
                L(f"- {clink(x['id'], x['name'])}")
            L("")

    # d. recycled this week
    rec = own(c.c4)
    if rec:
        L("**d. Recycled this week:**")
        for x in rec[:CAP]:
            L(f"- {clink(x['id'], x['name'])} — now {LIFECYCLE_LABELS.get(x['_stage'], x['_stage'])}")
        L("")

    # e. deals needing attention (3B i/ii/iii) for this owner
    mydeals = [d for d in (c.b3_usd + c.b3_idle + c.b3_pastclose) if d["_owner"] == owner]
    mydeals = {d["id"]: d for d in mydeals}.values()
    if mydeals:
        L("**e. Deals needing attention:**")
        for d in sorted(mydeals, key=lambda d: -(d["_usd"] or 0))[:CAP]:
            tags = []
            if d in c.b3_usd:
                tags.append("USD empty/0")
            if d in c.b3_idle:
                tags.append(f"idle {d['_idle']}d")
            if d in c.b3_pastclose:
                tags.append("past close")
            L(f"- {dlink(d['id'], d['dealname'])} — {d['_label']}, {money(d['_usd']) if d['_usd'] is not None else 'USD empty'} ({', '.join(tags)})")
        L("")

    # f. data fixes (3C,3D,3E,4D)
    fixes = []
    for x in own(c.c3):
        fixes.append((x, "miscategorised (3C)"))
    for x, exp in c.d3:
        if x["_block"] == owner:
            fixes.append((x, f"status mismatch (3D) → {exp}"))
    for x in own(c.e3):
        fixes.append((x, "orphan (3E)"))
    for x in own(c.d4_stuck):
        fixes.append((x, "stuck recycle (4D)"))
    if fixes:
        # dedupe by (id, tag)
        seen = set()
        L("**f. Data fixes:**")
        n = 0
        for x, tag in fixes:
            key = (x["id"], tag)
            if key in seen:
                continue
            seen.add(key)
            if n < 40:
                L(f"- {clink(x['id'], x['name'])} — {tag}")
            n += 1
        if n > 40:
            L(f"- …and {n - 40} more")
        L("")

    # Nicola-only extras
    if is_nicola:
        if c.a3:
            L(f"**3A — Companies to complete ({len(c.a3)}):** see Section 3A.")
            L("")
        if c.h3:
            L(f"**3H — Won but still Prospect ({len(c.h3)}):** "
              + ", ".join(colink(d['company_id'], d['dealname'][:30]) for d in c.h3))
            L("")
        if c.after_deadline and len(c.a4) > 0:
            L(f"**Claim window closed — manually enrol the remaining {len(c.a4)} contacts** "
              "in the Stale SQL Recycler workflow (repeat until backlog is 0).")
            L("")

    return lines


def _render_owner_blocks(A, c, pv, for_main=False):
    owners = SALES_OWNERS + ["Nicola Cole"]
    for owner in owners:
        body = _owner_block_body(c, owner, pv)
        A(f"### {owner}")
        A("")
        if body:
            for ln in body:
                A(ln)
        else:
            A("- Nothing outstanding this week.")
            A("")


def owner_file(c, owner, pv):
    out = [f"# Section 5 — {owner} — {c.run.isoformat()}", "",
           "*Brahmarsive weekly B2B pipeline. Your action items for this week.*", ""]
    body = _owner_block_body(c, owner, pv)
    if body:
        out += body
    else:
        out.append("Nothing outstanding this week.")
    return "\n".join(out)


# =========================================================================
# snapshot / week-on-week
# =========================================================================
def load_prev(snap_dir, run):
    if not os.path.isdir(snap_dir):
        return None
    cands = []
    for n in os.listdir(snap_dir):
        if n.endswith(".json"):
            try:
                d = dt.date.fromisoformat(n[:-5])
            except ValueError:
                continue
            if d < run:
                cands.append((d, n))
    if not cands:
        return None
    cands.sort()
    with open(os.path.join(snap_dir, cands[-1][1])) as f:
        prev = json.load(f)
    prev["comparable"] = bool(prev.get("schema") == "oct2026")
    return prev


def build_snapshot(c):
    return {
        "schema": "oct2026",
        "run_date": c.run.isoformat(),
        "generated_at": c.raw["generated_at"],
        "headline": {
            "c3": len(c.c3), "d3": len(c.d3), "e3": len(c.e3),
            "c4": len(c.c4), "d4": len(c.d4_stuck),
            "a4": len(c.a4), "open_deals": len(c.open_deals),
            "open_usd": c.open_usd,
            "q4_new_deals": c.q4["rows"][0][2],
            "q4_entered_trial": c.q4["rows"][1][2],
            "q4_closed_won": c.q4["rows"][2][2],
            "wf_ok": c.wf_ok,
        },
        "worklist_ids": {
            "3A": [co["id"] for co in c.a3],
            "3B_usd": [d["id"] for d in c.b3_usd],
            "3B_idle": [d["id"] for d in c.b3_idle],
            "3B_pastclose": [d["id"] for d in c.b3_pastclose],
            "3C": [x["id"] for x in c.c3],
            "3D": [x["id"] for x, _ in c.d3],
            "3E": [x["id"] for x in c.e3],
            "3F": [x["id"] for x in c.f3],
            "3G": [t[1]["id"] for t in c.g3],
            "3H": [d["company_id"] for d in c.h3],
            "4A": [x["id"] for x in c.a4],
            "4C": [x["id"] for x in c.c4],
            "4D_stuck": [x["id"] for x in c.d4_stuck],
        },
        # for next week's Section 5a diff:
        "contact_states": {x["id"]: {"lc": x["_stage"], "ls": (x["_label"] or "")}
                           for x in c.contacts},
    }


# =========================================================================
# main
# =========================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="raw_input.json (required)")
    ap.add_argument("--outdir", default=".")
    args = ap.parse_args()

    with open(args.input) as f:
        raw = json.load(f)
    run = dt.date.fromisoformat(raw["run_date"])

    c = compute(raw, run)

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    prev = load_prev(snap_dir, run)
    report = render(c, prev)

    with open(os.path.join(rep_dir, f"{run.isoformat()}.md"), "w") as f:
        f.write(report)

    snap = build_snapshot(c)
    with open(os.path.join(snap_dir, f"{run.isoformat()}.json"), "w") as f:
        json.dump(snap, f, indent=1)

    pv = prev if (prev and prev.get("comparable")) else None
    for owner in SALES_OWNERS + ["Nicola Cole"]:
        first = owner.split()[0].lower()
        with open(os.path.join(rep_dir, f"{run.isoformat()}_owner_{first}.md"), "w") as f:
            f.write(owner_file(c, owner, pv))

    # best-effort .docx via pandoc (uses the team reference template if present)
    md_path = os.path.join(rep_dir, f"{run.isoformat()}.md")
    docx_path = os.path.join(rep_dir, f"{run.isoformat()}.docx")
    tmpl = os.path.join(args.outdir, "templates", "pipeline_report_template.docx")
    import shutil
    import subprocess
    if shutil.which("pandoc"):
        cmd = ["pandoc", md_path, "-o", docx_path]
        if os.path.exists(tmpl):
            cmd += ["--reference-doc", tmpl]
        try:
            subprocess.run(cmd, check=True)
            print(f"Wrote reports/{run.isoformat()}.docx")
        except subprocess.CalledProcessError as e:
            print(f"pandoc failed ({e}); .docx not generated")
    else:
        print("pandoc not found; .docx not generated")

    print(f"Wrote reports/{run.isoformat()}.md")
    print(f"Wrote snapshots/{run.isoformat()}.json")
    print(f"Wrote {len(SALES_OWNERS) + 1} owner files")
    print(f"Workflow health: {c.wf_ok}/6 | 3D={len(c.d3)} 4D stuck={len(c.d4_stuck)} "
          f"4A={len(c.a4)} open deals={len(c.open_deals)} open USD={money(c.open_usd)}")


if __name__ == "__main__":
    main()
