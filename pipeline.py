#!/usr/bin/env python3
"""
Brahmarsive weekly B2B pipeline & MQL-health report  (process version: Oct 2026).

Consumes a pre-pulled raw_input.json (data fetched via the HubSpot MCP tools,
because direct egress to api.hubapi.com is blocked in the run environment) and
writes, per run:

  snapshots/YYYY-MM-DD.json           raw + every worklist id (+ per-contact
                                      lifecycle/lead-status for next week's diff)
  reports/YYYY-MM-DD.md               Headline + Sections 1-6
  reports/YYYY-MM-DD_owner_<name>.md  one Section-5 block per sales owner

Run:  python3 pipeline.py --input raw_input.json      (never run without --input)

SCOPING RULES (non-negotiable, honoured here):
  * B2B only everywhere. Contact counts come from the CONTACT object; deal value
    comes from DEALS scoped via COMPANY.customer_type__cloned_ = 'B2B'. The two
    bases are never mixed (companies never feed contact/SQL counts).
  * Deal USD value field is amount_in_home_currency. Raw amount + currency are
    secondary reference only. Empty/0 USD on a deal is FLAGGED, never back-filled.
  * Lead status is mixed-format; all rules match on the mapped LABEL, built from
    the live hs_lead_status property definition (lead_status_map in raw_input).
"""

import argparse
import datetime as dt
import json
import os
import subprocess
import sys

# =========================================================================
# EDITABLE CONSTANTS
# =========================================================================
PORTAL = "50467095"

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
    "1217017229", "appointmentscheduled", "presentationscheduled",
    "1217017230", "contractsent", "decisionmakerboughtin",
]
CLOSED_STAGES = {"closedwon", "closedlost"}

SALES_OWNERS = ["Theo Zirogiannis", "Jack Makhlouf", "Anup Ghoshal", "Biju Rajesh"]
# anyone else (incl. Nicola Cole, Ramjee, Unassigned) rolls into Nicola's block.

SALES_WORKING_STATUSES = ["Attempted to Contact", "Contacted"]        # labels

# VALID_STATUS_BY_LIFECYCLE uses LABELS. NB: the spec wrote the Lead/MQL "New"
# label, but the live hs_lead_status option label is "New Lead" (internal NEW).
# Reconciled here so the guard passes; see RECONCILE note printed at run time.
VALID_STATUS_BY_LIFECYCLE = {
    "subscriber":             ["New Lead", "Marketing Engaged"],
    "lead":                   ["New Lead", "Marketing Engaged"],
    "marketingqualifiedlead": ["MQL", "New Lead", "Unqualified"],
    "salesqualifiedlead":     ["Attempted to Contact", "Contacted"],
    "opportunity":            ["Qualified", "In Progress"],
    "customer":               ["Customer"],
}
# For the guard message: spec label -> reconciled portal label.
SPEC_LABEL_RECONCILE = {"New": "New Lead"}

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

LIFECYCLE_LABELS = {
    "subscriber": "Email Contact", "lead": "Lead",
    "marketingqualifiedlead": "Marketing Qualified Lead",
    "salesqualifiedlead": "Sales Qualified Lead",
    "opportunity": "Opportunity", "customer": "Customer", "other": "Other",
}

# =========================================================================
# helpers
# =========================================================================

def parse_dt(v):
    if not v:
        return None
    v = str(v).replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(v)
    except ValueError:
        try:
            d = dt.datetime.fromisoformat(v[:19] + "+00:00")
        except ValueError:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d


def to_date(v):
    d = parse_dt(v)
    return d.date() if d else None


def days_since(v, today):
    d = to_date(v)
    return None if d is None else (today - d).days


def fnum(v):
    if v in (None, "", "Unassigned"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def clink(cid):
    return f"https://app.hubspot.com/contacts/{PORTAL}/record/0-1/{cid}"


def colink(cid):
    return f"https://app.hubspot.com/contacts/{PORTAL}/record/0-2/{cid}"


def dlink(did):
    return f"https://app.hubspot.com/contacts/{PORTAL}/record/0-3/{did}"


def md_link(text, url):
    return f"[{text}]({url})"


# =========================================================================
# Delta vs prior snapshot
# =========================================================================

def load_prev(snap_dir, run_date):
    if not os.path.isdir(snap_dir):
        return None
    cands = []
    for n in os.listdir(snap_dir):
        if n.endswith(".json"):
            try:
                d = dt.date.fromisoformat(n[:-5])
            except ValueError:
                continue
            if d < run_date:
                cands.append((d, n))
    if not cands:
        return None
    cands.sort()
    try:
        return json.load(open(os.path.join(snap_dir, cands[-1][1])))
    except Exception:
        return None


def delta(cur, prev):
    if prev is None:
        return f"{cur}"
    d = cur - prev
    return f"{cur} (+{d})" if d > 0 else (f"{cur} ({d})" if d < 0 else f"{cur} (=)")


# =========================================================================
# compute
# =========================================================================

def compute(raw):
    today = dt.date.fromisoformat(raw["run_date"])
    win = today - dt.timedelta(days=WINDOW_DAYS)
    q4s = dt.date.fromisoformat(Q4_START)

    owners = raw["owners"]
    lsmap = raw["lead_status_map"]

    def oname(oid):
        return owners.get(str(oid), "Unassigned") if oid else "Unassigned"

    def label(status):
        if status in (None, ""):
            return None
        return lsmap.get(status, status)

    def bucket_owner(name):
        return name if name in SALES_OWNERS else "Nicola Cole"

    contacts = raw["contacts_b2b"]
    deals = raw["deals_b2b"]
    companies = raw["companies_b2b"]
    sql_all = raw["sql_all_types"]
    new_company_ids = set(raw.get("new_company_ids", []))

    # ---- GUARDS ----
    guard_msgs = []
    for lc, labels in VALID_STATUS_BY_LIFECYCLE.items():
        for lab in labels:
            if lab not in lsmap.values():
                guard_msgs.append(f"lead_status_map missing label '{lab}' (lifecycle {lc})")
    if not companies or len(companies) == raw.get("total_b2b_companies"):
        pass  # companies_b2b is intentionally the hygiene+new subset here
    if guard_msgs:
        raise SystemExit("GUARD FAILED:\n  " + "\n  ".join(guard_msgs))

    # enrich contacts
    for c in contacts:
        c["owner_name"] = oname(c.get("owner_id"))
        c["status_label"] = label(c.get("hs_lead_status"))
        c["idle"] = days_since(c.get("notes_last_updated"), today)
        c["created_idle"] = days_since(c.get("createdate"), today)
        c["is_new"] = (to_date(c.get("createdate")) is not None
                       and to_date(c.get("createdate")) >= win)

    for d in deals:
        d["owner_name"] = oname(d.get("owner_id"))
        d["usd"] = fnum(d.get("amount_in_home_currency"))
        last = d.get("notes_last_updated") or d.get("createdate")
        d["idle"] = days_since(last, today)
        d["is_closed"] = str(d.get("hs_is_closed")).lower() == "true"
        d["is_open"] = d.get("dealstage") not in CLOSED_STAGES
        d["stage_label"] = DEALSTAGE_LABELS.get(d.get("dealstage"), d.get("dealstage"))

    deals_by_company = {}
    for d in deals:
        deals_by_company.setdefault(d.get("company_id"), []).append(d)

    open_deals = [d for d in deals if d["is_open"]]

    # ====== Q4 SCORECARD ======
    def in_q4(v):
        dd = to_date(v)
        return dd is not None and q4s <= dd <= today
    q4_new = [d for d in deals if in_q4(d.get("createdate"))]
    q4_trial = [d for d in deals if in_q4(d.get("hs_v2_date_entered_presentationscheduled"))]
    q4_won = [d for d in deals if d.get("dealstage") == "closedwon" and in_q4(d.get("closedate"))]
    weeks_elapsed = min((today - q4s).days / 7.0, Q4_WEEKS)

    def expected(t):
        return round(t * weeks_elapsed / Q4_WEEKS)

    def owner_contrib(dlist):
        agg = {}
        for d in dlist:
            agg[d["owner_name"]] = agg.get(d["owner_name"], 0) + 1
        return ", ".join(f"{k}: {v}" for k, v in sorted(agg.items(), key=lambda x: -x[1])) or "—"

    q4 = {
        "weeks_elapsed": round(weeks_elapsed, 1),
        "new_deals": {"actual": len(q4_new), "target": Q4_TARGETS["new_deals"],
                      "expected": expected(Q4_TARGETS["new_deals"]),
                      "contrib": owner_contrib(q4_new),
                      "ids": [d["id"] for d in q4_new]},
        "entered_trial": {"actual": len(q4_trial), "target": Q4_TARGETS["entered_trial"],
                          "expected": expected(Q4_TARGETS["entered_trial"]),
                          "contrib": owner_contrib(q4_trial),
                          "ids": [d["id"] for d in q4_trial]},
        "closed_won": {"actual": len(q4_won), "target": Q4_TARGETS["closed_won"],
                       "expected": expected(Q4_TARGETS["closed_won"]),
                       "contrib": owner_contrib(q4_won),
                       "ids": [d["id"] for d in q4_won]},
    }

    # ====== SECTION 1 ======
    s1_stage = []
    for st in STAGE_ORDER:
        ds = [d for d in open_deals if d.get("dealstage") == st]
        usd = sum(d["usd"] or 0 for d in ds)
        s1_stage.append({
            "stage": st, "label": DEALSTAGE_LABELS[st], "qty": len(ds),
            "usd": usd, "median_idle": median([d["idle"] for d in ds]),
            "idle14": sum(1 for d in ds if (d["idle"] or 0) >= QTB_STALE_DAYS),
        })
    open_qty = len(open_deals)
    open_usd = sum(d["usd"] or 0 for d in open_deals)
    closed_ref = {}
    for st in ("closedwon", "closedlost"):
        ds = [d for d in deals if d.get("dealstage") == st]
        closed_ref[st] = {"qty": len(ds), "usd": sum(d["usd"] or 0 for d in ds)}

    # 1.2 by owner
    s12 = {}
    for d in open_deals:
        e = s12.setdefault(d["owner_name"], {"qty": 0, "usd": 0.0})
        e["qty"] += 1
        e["usd"] += d["usd"] or 0
    s12 = sorted(({"owner": k, **v} for k, v in s12.items()), key=lambda x: -x["usd"])
    top_deal = max(open_deals, key=lambda d: d["usd"] or 0) if open_deals else None

    # 1.3 moved / updated this week
    moved, updated = [], []
    for d in deals:
        entered = to_date(d.get("hs_v2_date_entered_current_stage"))
        modified = to_date(d.get("hs_lastmodifieddate"))
        if entered is not None and entered >= win:
            moved.append(d)
        elif modified is not None and modified >= win:
            updated.append(d)

    # ====== SECTION 2 ======
    new_companies = [c for c in companies if c["id"] in new_company_ids]
    s21 = {}
    for c in new_companies:
        nm = oname(c.get("owner_id"))
        s21.setdefault(nm, []).append(c)

    new_contacts = [c for c in contacts if c["is_new"]]
    new_on_existing, new_on_new, orphans_new = [], [], []
    for c in new_contacts:
        acid = c.get("associatedcompanyid")
        if not acid:
            orphans_new.append(c)
        elif acid in new_company_ids:
            new_on_new.append(c)
        else:
            new_on_existing.append(c)

    # 2.3 SQL by owner  (contact object only)
    sql_contacts = [c for c in contacts if c["lifecyclestage"] == "salesqualifiedlead"]
    s23 = {}
    for c in sql_contacts:
        e = s23.setdefault(c["owner_name"], {"qty": 0, "u30": 0, "u60": 0})
        e["qty"] += 1
        idle = c["idle"]
        if idle is None or idle >= 30:
            e["u30"] += 1
        if idle is None or idle >= 60:
            e["u60"] += 1
    s23 = sorted(({"owner": k, **v} for k, v in s23.items()), key=lambda x: -x["qty"])

    # ====== SECTION 3 ======
    # 3A companies missing owner/name
    s3a = [c for c in companies if (not c.get("owner_id")) or (not c.get("name"))]

    # 3B deal hygiene (open deals)
    s3b_usd = [d for d in open_deals if d["usd"] in (None, 0) or (d["usd"] or 0) == 0]
    s3b_idle = [d for d in open_deals if (d["idle"] or 0) >= DEAL_IDLE_DAYS]
    s3b_pastclose = [d for d in open_deals
                     if to_date(d.get("closedate")) and to_date(d.get("closedate")) < today]
    s3b_noowner = [d for d in open_deals if not d.get("owner_id")]

    # 3C miscategorised
    s3c = [c for c in contacts
           if c["status_label"] in SALES_WORKING_STATUSES
           and c["lifecyclestage"] in ("subscriber", "lead", "marketingqualifiedlead")]
    s3c_ids = {c["id"] for c in s3c}

    # 3D status mismatches
    s3d = []
    for c in contacts:
        lc = c["lifecyclestage"]
        if lc not in VALID_STATUS_BY_LIFECYCLE:
            continue
        valid = VALID_STATUS_BY_LIFECYCLE[lc]
        lab = c["status_label"]
        reason = None
        expected_s = "/".join(valid)
        if lab is None:
            if lc in ("salesqualifiedlead", "opportunity", "customer"):
                reason = "blank status at SQL+"
        elif lab not in valid:
            reason = f"{lab} invalid for {LIFECYCLE_LABELS.get(lc, lc)}"
        # opportunity deal-rule
        if lc == "opportunity":
            nd = c.get("num_associated_deals", 0)
            if lab == "In Progress" and nd > 0:
                reason = "In Progress but has deals (should be Qualified)"
            elif lab == "Qualified" and nd == 0:
                reason = "Qualified but 0 deals (should be In Progress)"
        if reason and c["id"] not in s3c_ids:
            s3d.append({**c, "mismatch": reason, "expected": expected_s})

    # 3E orphans (new 7d, no company)
    s3e = [c for c in new_contacts if not c.get("associatedcompanyid")]

    # 3F lost-deal leftovers (Opportunity contacts whose company deals are all closed-lost)
    s3f = []
    for c in contacts:
        if c["lifecyclestage"] != "opportunity":
            continue
        cd = deals_by_company.get(c.get("associatedcompanyid"), [])
        if cd and all(d.get("dealstage") == "closedlost" for d in cd):
            s3f.append({**c, "deal_count": len(cd)})

    # 3G MQL follow-up
    s3g = []
    for c in contacts:
        if c["lifecyclestage"] != "marketingqualifiedlead":
            continue
        if c["status_label"] != "MQL":
            # only flag MQLs with no owner here if status==MQL; owner-less MQLs below
            pass
        no_owner = not c.get("owner_id")
        since_mql = days_since(c.get("date_entered_mql"), today)
        idle = c["idle"]
        stale = (idle is None or idle >= MQL_FOLLOWUP_DAYS)
        is_mql_status = c["status_label"] == "MQL"
        if (is_mql_status and stale) or no_owner:
            s3g.append({**c, "since_mql": since_mql, "no_owner": no_owner})

    # 3H won-but-not-customer (from deals with company type)
    s3h = {}
    for d in deals:
        if d.get("dealstage") == "closedwon" and (d.get("company_type") or "").upper() == "PROSPECT":
            s3h[d.get("company_id")] = {"company_id": d.get("company_id"),
                                        "company_name": d.get("company_name"),
                                        "deal": d["dealname"], "deal_id": d["id"],
                                        "type": d.get("company_type")}
    s3h = list(s3h.values())
    s3h_blanktype = sorted({d.get("company_name") for d in deals
                            if d.get("dealstage") == "closedwon"
                            and not (d.get("company_type") or "")})

    # ====== SECTION 4 ======
    def trigger(c):
        idle = c["idle"]
        if idle is not None:
            return idle > RECYCLE_DAYS
        return (c.get("notes_last_updated") is None
                and (c["created_idle"] or 0) > RECYCLE_DAYS)

    s4a = [c for c in sql_contacts if trigger(c)]
    # non-B2B SQL meeting trigger (B2B filter broken check)
    s4a_nonb2b = []
    for c in sql_all:
        if (c.get("customer_type") or "") == "B2B":
            continue
        idle = days_since(c.get("notes_last_updated"), today)
        cre = days_since(c.get("createdate"), today)
        trig = (idle is not None and idle > RECYCLE_DAYS) or \
               (c.get("notes_last_updated") is None and (cre or 0) > RECYCLE_DAYS)
        if trig:
            s4a_nonb2b.append(c)

    # 4C recycled this week
    s4c = [c for c in contacts
           if to_date(c.get("recycled_date")) and to_date(c.get("recycled_date")) >= win]
    s4c_not_mql = [c for c in s4c if c["lifecyclestage"] == "salesqualifiedlead"]

    # 4D stuck recycles / re-promotions
    s4d_stuck, s4d_repromo = [], []
    for c in contacts:
        rd = to_date(c.get("recycled_date"))
        if not rd or c["lifecyclestage"] != "salesqualifiedlead":
            continue
        dsql = to_date(c.get("date_entered_sql"))
        if dsql and dsql < rd:
            s4d_stuck.append(c)
        elif dsql and dsql > rd:
            s4d_repromo.append(c)

    # ====== SECTION 5 warn / recycle ======
    s5b = {}
    for c in sql_contacts:
        idle = c["idle"]
        warn = (idle is not None and RECYCLE_WARN_DAYS <= idle < RECYCLE_DAYS) or \
               (c.get("notes_last_updated") is None
                and RECYCLE_WARN_DAYS <= (c["created_idle"] or 0) < RECYCLE_DAYS)
        if warn:
            s5b.setdefault(c["owner_name"], []).append(c)

    # ====== SECTION 6 workflow checks ======
    s3c_thisweek = [c for c in s3c
                    if (to_date(c.get("createdate")) and to_date(c.get("createdate")) >= win)]
    wf_meeting = [c for c in contacts if c["status_label"] == "Contacted"
                  and c["lifecyclestage"] in ("subscriber", "lead", "marketingqualifiedlead")]
    wf_lifecycle = [c for c in s3d if c["lifecyclestage"] in ("opportunity", "customer")]
    wf_dealassoc = [c for c in contacts if c["lifecyclestage"] == "opportunity"
                    and c["status_label"] == "In Progress" and c.get("num_associated_deals", 0) > 0]
    wf_dealfields = [d for d in open_deals
                     if (to_date(d.get("createdate")) and to_date(d.get("createdate")) >= win)
                     and (d["usd"] in (None, 0))]
    workflows = [
        ("Sales Activity > Lead Status", len(s3c_thisweek) == 0,
         [c["id"] for c in s3c_thisweek]),
        ("Meeting Booked > Contacted", len(wf_meeting) == 0, [c["id"] for c in wf_meeting]),
        ("Stale SQL Recycler", len(s4d_stuck) == 0, [c["id"] for c in s4d_stuck]),
        ("Lifecycle > Lead Status Sync", len(wf_lifecycle) == 0, [c["id"] for c in wf_lifecycle]),
        ("Deal Associated > Qualified", len(wf_dealassoc) == 0, [c["id"] for c in wf_dealassoc]),
        ("Deal required fields", len(wf_dealfields) == 0, [d["id"] for d in wf_dealfields]),
    ]

    pct_idle_open = round(100 * sum(1 for d in open_deals if (d["idle"] or 0) >= QTB_STALE_DAYS)
                          / open_qty) if open_qty else 0
    sqls_untouched30 = sum(1 for c in sql_contacts if c["idle"] is None or c["idle"] >= 30)
    pct_sql_untouched = round(100 * sqls_untouched30 / len(sql_contacts)) if sql_contacts else 0

    return {
        "today": today, "win": win,
        "oname": oname, "bucket_owner": bucket_owner,
        "q4": q4,
        "funnel_counts": raw["funnel_counts"],
        "sql_total": len(sql_contacts),
        "s1_stage": s1_stage, "open_qty": open_qty, "open_usd": open_usd,
        "closed_ref": closed_ref, "s12": s12, "top_deal": top_deal,
        "moved": moved, "updated": updated,
        "new_companies": new_companies, "s21": s21,
        "new_contacts": new_contacts, "new_on_existing": new_on_existing,
        "new_on_new": new_on_new,
        "s23": s23,
        "s3a": s3a, "s3b_usd": s3b_usd, "s3b_idle": s3b_idle,
        "s3b_pastclose": s3b_pastclose, "s3b_noowner": s3b_noowner,
        "s3c": s3c, "s3d": s3d, "s3e": s3e, "s3f": s3f, "s3g": s3g,
        "s3h": s3h, "s3h_blanktype": s3h_blanktype,
        "s4a": s4a, "s4a_nonb2b": s4a_nonb2b, "s4c": s4c, "s4c_not_mql": s4c_not_mql,
        "s4d_stuck": s4d_stuck, "s4d_repromo": s4d_repromo,
        "s5b": s5b,
        "workflows": workflows,
        "pct_idle_open": pct_idle_open, "pct_sql_untouched": pct_sql_untouched,
        "contacts": contacts, "deals": deals,
    }


# =========================================================================
# render
# =========================================================================

def cname(c):
    return c.get("name") or c.get("id")


def usd(v):
    return f"${v:,.0f}" if v else "$0"


def render(m, prev):
    today = m["today"]
    A = []
    w = A.append
    pm = (prev or {}).get("metrics_lite") if prev else None

    def pdelta(key, cur):
        if not pm or key not in pm:
            return f"{cur}"
        return delta(cur, pm[key])

    w(f"# Brahmarsive Weekly Pipeline & MQL Health — {today}")
    w("")
    w("*Scope: B2B only. Contact counts from the CONTACT object; deal value from "
      "DEALS scoped via `COMPANY.customer_type__cloned_ = 'B2B'`. USD = "
      "`amount_in_home_currency`. Process version: Oct 2026.*")
    w("")
    if not pm:
        w("> ℹ️ No comparable prior snapshot in the new (Oct 2026) schema — "
          "week-on-week deltas begin next run. The only prior snapshot "
          "(`2026-07-02`) predates this format.")
        w("")
    w("> ⚠️ **Lead-status reconciliation:** the spec's Lead/MQL label `New` does not "
      "exist in HubSpot; the live option label is **`New Lead`** (internal `NEW`). "
      "`VALID_STATUS_BY_LIFECYCLE` uses `New Lead`.")
    w("")

    # ---------- Q4 SCORECARD ----------
    q = m["q4"]
    w("## Q4 Scorecard")
    w("")
    w(f"*B2B deals, {Q4_START} → {today} · week {q['weeks_elapsed']} of {Q4_WEEKS}.*")
    w("")
    w("| Metric | Definition | Actual | Target | Expected by now | Status |")
    w("|---|---|---|---|---|---|")
    rows = [
        ("New deals", "created in Q4", q["new_deals"]),
        ("Entered trial", "entered Discovery & Trial in Q4 (cumulative)", q["entered_trial"]),
        ("Closed won", "Closed Won w/ close date in Q4", q["closed_won"]),
    ]
    for nm, defn, d in rows:
        st = "✅" if d["actual"] >= d["expected"] else "⚠️"
        w(f"| {nm} | {defn} | **{pdelta('q4_'+nm.split()[0].lower(), d['actual'])}** "
          f"| {d['target']} | {d['expected']} | {st} |")
    w("")
    for nm, d in (("New deals", q["new_deals"]), ("Entered trial", q["entered_trial"]),
                  ("Closed won", q["closed_won"])):
        w(f"- **{nm}** by owner — {d['contrib']}")
    w("")

    # ---------- HEADLINE FLAGS ----------
    wf_ok = sum(1 for _, ok, _ in m["workflows"] if ok)
    wf_bad = [n for n, ok, _ in m["workflows"] if not ok]
    w("## 🔔 Headline Flags")
    w("")
    w(f"- **Workflow firing checks:** {wf_ok}/{len(m['workflows'])} ✅"
      + (f" · ⚠️ {', '.join(wf_bad)}" if wf_bad else ""))
    w(f"- **Deals that changed stage this week:** {len(m['moved'])}")
    w(f"- **Open deals idle {QTB_STALE_DAYS}+ days:** {m['pct_idle_open']}% "
      f"({sum(x['idle14'] for x in m['s1_stage'])} of {m['open_qty']})")
    w(f"- **SQLs untouched 30+ days:** {m['pct_sql_untouched']}% of {m['sql_total']}")
    w(f"- **Miscategorised leads (3C):** {pdelta('s3c', len(m['s3c']))}")
    w(f"- **Status mismatches (3D):** {pdelta('s3d', len(m['s3d']))}")
    w(f"- **Stuck recycles (4D):** {pdelta('s4d', len(m['s4d_stuck']))}"
      + ("  ⚠️" if m["s4d_stuck"] else ""))
    w(f"- **Deals past close date:** {len(m['s3b_pastclose'])}")
    w(f"- **Open deals missing/zero USD:** {len(m['s3b_usd'])}")
    w(f"- **New accounts with no owner:** {sum(1 for c in m['new_companies'] if not c.get('owner_id'))}")
    w(f"- **Recycled this week (4C):** {pdelta('s4c', len(m['s4c']))}"
      + (f"  ⚠️ {len(m['s4c_not_mql'])} still at SQL" if m["s4c_not_mql"] else ""))
    if m["s4a_nonb2b"]:
        w(f"- **⚠️ Non-B2B SQLs matching recycler trigger:** {len(m['s4a_nonb2b'])} "
          "(B2B filter may be broken)")
    w("")

    # ---------- SECTION 1 ----------
    w("## Section 1 — Current Pipeline Status")
    w("")
    w("### 1.1 Open pipeline by stage")
    w("")
    w("| Stage | Deals | Open USD | Median days idle | Idle ≥14d |")
    w("|---|--:|--:|--:|--:|")
    for s in m["s1_stage"]:
        mi = "—" if s["median_idle"] is None else f"{s['median_idle']:.0f}"
        w(f"| {s['label']} | {s['qty']} | {usd(s['usd'])} | {mi} | {s['idle14']} |")
    w(f"| **Open subtotal** | **{m['open_qty']}** | **{usd(m['open_usd'])}** | | |")
    w("")
    cr = m["closed_ref"]
    w(f"*Reference (not in open subtotal): Closed Won {cr['closedwon']['qty']} "
      f"({usd(cr['closedwon']['usd'])}) · Closed Lost {cr['closedlost']['qty']} "
      f"({usd(cr['closedlost']['usd'])}).*")
    w("")

    w("### 1.2 Open pipeline by sales owner")
    w("")
    w("| Owner | Open deals | Open USD |")
    w("|---|--:|--:|")
    for e in m["s12"]:
        w(f"| {e['owner']} | {e['qty']} | {usd(e['usd'])} |")
    w("")
    if m["top_deal"] and m["open_usd"]:
        td = m["top_deal"]
        share = round(100 * (td["usd"] or 0) / m["open_usd"])
        w(f"> **Concentration:** {md_link(td['dealname'], dlink(td['id']))} "
          f"({td['owner_name']}) holds {usd(td['usd'])} — {share}% of all open pipeline value.")
        w("")

    w("### 1.3 Updated / moved in the last 7 days")
    w("")
    w(f"**Stage moved ({len(m['moved'])})** — entered current stage within the window:")
    w("")
    if m["moved"]:
        w("| Deal | Owner | Current stage | USD | Last activity |")
        w("|---|---|---|--:|---|")
        for d in sorted(m["moved"], key=lambda x: -(x["usd"] or 0)):
            w(f"| {md_link(d['dealname'], dlink(d['id']))} | {d['owner_name']} | "
              f"{d['stage_label']} | {usd(d['usd'])} | {d.get('notes_last_updated','—') or '—'} |")
    else:
        w("- None.")
    w("")
    w(f"**Updated only ({len(m['updated'])})** — modified this week, no stage change "
      "(field tidy-ups, not progression). Listed in the snapshot JSON.")
    w("")

    # ---------- SECTION 2 ----------
    w("## Section 2 — New Pipeline Status")
    w("")
    w(f"### 2.1 New companies (last 7 days) — {len(m['new_companies'])}")
    w("")
    if m["new_companies"]:
        w("| Company | Owner | Created | Source |")
        w("|---|---|---|---|")
        for c in m["new_companies"]:
            nm = m["oname"](c.get("owner_id"))
            flag = " ⚠️ no owner" if not c.get("owner_id") else ""
            label_nm = c.get("name") or "*(blank name)*"
            w(f"| {md_link(label_nm, colink(c['id']))} | {nm}{flag} | "
              f"{(c.get('createdate') or '')[:10]} | {c.get('original_source','')} |")
    else:
        w("- None.")
    w("")
    w(f"### 2.2 New contacts on existing companies (last 7 days) — {len(m['new_on_existing'])}")
    w(f"*(plus {len(m['new_on_new'])} new contacts on brand-new accounts)*")
    w("")
    if m["new_on_existing"]:
        byo = {}
        for c in m["new_on_existing"]:
            byo.setdefault(c["owner_name"], []).append(c)
        for ow in sorted(byo):
            w(f"**{ow}** ({len(byo[ow])})")
            w("")
            w("| Contact | Lifecycle | Status |")
            w("|---|---|---|")
            for c in byo[ow]:
                w(f"| {md_link(cname(c), clink(c['id']))} | "
                  f"{LIFECYCLE_LABELS.get(c['lifecyclestage'], c['lifecyclestage'])} | "
                  f"{c['status_label'] or '*(blank)*'} |")
            w("")
    else:
        w("- None.")
        w("")

    w(f"### 2.3 SQLs by sales owner — true SQL total (contacts): {m['sql_total']}")
    w("")
    w("| Owner | SQLs | Untouched 30+d | Untouched 60+d |")
    w("|---|--:|--:|--:|")
    t = {"qty": 0, "u30": 0, "u60": 0}
    for e in m["s23"]:
        w(f"| {e['owner']} | {e['qty']} | {e['u30']} | {e['u60']} |")
        for k in t:
            t[k] += e[k]
    w(f"| **Total** | **{t['qty']}** | **{t['u30']}** | **{t['u60']}** |")
    w("")

    # ---------- SECTION 3 ----------
    w("## Section 3 — Admin / Data-Hygiene Worklist")
    w("")
    w(f"### 3A Companies to complete (missing owner and/or name) — {len(m['s3a'])}  ·  owner: Nicola")
    w("")
    w("| Company | Created | Owner | Name | Source |")
    w("|---|---|---|---|---|")
    for c in m["s3a"]:
        nm = m["oname"](c.get("owner_id"))
        w(f"| {md_link('#'+c['id'], colink(c['id']))} | {(c.get('createdate') or '')[:10]} | "
          f"{nm if c.get('owner_id') else '**none**'} | "
          f"{c.get('name') or '**blank**'} | {c.get('original_source','')} |")
    w("")

    def deal_table(title, ds):
        w(f"**{title} — {len(ds)}**")
        w("")
        if not ds:
            w("- None.")
            w("")
            return
        w("| Deal | Stage | Owner | USD | Last activity | Close date |")
        w("|---|---|---|--:|---|---|")
        for d in sorted(ds, key=lambda x: -(x["usd"] or 0)):
            w(f"| {md_link(d['dealname'], dlink(d['id']))} | {d['stage_label']} | "
              f"{d['owner_name']} | {usd(d['usd'])} | {d.get('notes_last_updated','—') or '—'} | "
              f"{d.get('closedate','—') or '—'} |")
        w("")

    w("### 3B Deal hygiene (open B2B deals)")
    w("")
    deal_table("(i) amount_in_home_currency empty or 0", m["s3b_usd"])
    deal_table(f"(ii) idle {DEAL_IDLE_DAYS}+ days", m["s3b_idle"])
    deal_table("(iii) close date in the past", m["s3b_pastclose"])
    deal_table("(iv) no deal owner", m["s3b_noowner"])

    def contact_by_owner(title, items, extra=None):
        w(f"### {title} — {len(items)}")
        w("")
        if not items:
            w("- None.")
            w("")
            return
        byo = {}
        for c in items:
            byo.setdefault(c["owner_name"], []).append(c)
        CAP = 30
        for ow in sorted(byo):
            w(f"**{ow}** ({len(byo[ow])})")
            w("")
            hdr = "| Contact | Lifecycle | Status |" + (f" {extra[0]} |" if extra else "")
            sep = "|---|---|---|" + ("---|" if extra else "")
            w(hdr)
            w(sep)
            for c in byo[ow][:CAP]:
                base = (f"| {md_link(cname(c), clink(c['id']))} | "
                        f"{LIFECYCLE_LABELS.get(c['lifecyclestage'], c['lifecyclestage'])} | "
                        f"{c['status_label'] or '*(blank)*'} |")
                if extra:
                    base += f" {extra[1](c)} |"
                w(base)
            if len(byo[ow]) > CAP:
                w(f"| …and {len(byo[ow])-CAP} more — full list in that owner's "
                  f"Section 5 file | | |" + ("" if not extra else " |"))
            w("")

    contact_by_owner("3C Miscategorised leads (sales-worked but pre-SQL)", m["s3c"])
    contact_by_owner("3D Status mismatches", m["s3d"],
                     ("Expected / issue", lambda c: f"{c.get('expected','')} — {c.get('mismatch','')}"))
    contact_by_owner("3E Orphan contacts (new, no company)", m["s3e"])
    contact_by_owner("3F Lost-deal leftovers (Opportunity, all deals closed-lost)", m["s3f"],
                     ("Deals", lambda c: str(c.get("deal_count", 0))))
    contact_by_owner("3G MQL follow-up (stale / no owner)", m["s3g"],
                     ("Days since MQL", lambda c: str(c.get("since_mql") if c.get("since_mql") is not None else "—")))

    w(f"### 3H Won but not Customer — {len(m['s3h'])}")
    w("")
    if m["s3h"]:
        w("| Company | Won deal | Company type |")
        w("|---|---|---|")
        for h in m["s3h"]:
            w(f"| {md_link(h['company_name'] or '#'+str(h['company_id']), colink(h['company_id']))} | "
              f"{md_link(h['deal'], dlink(h['deal_id']))} | {h['type']} |")
    else:
        w("- None.")
    if m["s3h_blanktype"]:
        w("")
        w(f"*Also {len(m['s3h_blanktype'])} Closed-Won companies with blank Type "
          f"(neither Prospect nor Customer): {', '.join(x for x in m['s3h_blanktype'] if x)}.*")
    w("")

    # ---------- SECTION 4 ----------
    w("## Section 4 — Recycler Health Check")
    w("")
    claim_open = str(today) <= CLAIM_DEADLINE
    w(f"### 4A Trigger match now — {len(m['s4a'])} B2B SQLs")
    w("")
    w(f"Trigger: B2B SQL AND (last activity > {RECYCLE_DAYS}d ago OR never active & "
      f"created > {RECYCLE_DAYS}d ago).")
    if str(today) <= CLAIM_WINDOW_RUN:
        w("*Within the claim window — reported as the held backlog (info).*")
    else:
        overdue = [c for c in m["s4a"] if (c["idle"] or (c["created_idle"] or 0)) > RECYCLE_DAYS + 1]
        if overdue:
            w(f"> ⚠️ **ALERT:** {len(overdue)} match(es) are >1 day past threshold — "
              "the recycler is not firing.")
        else:
            w("- ✅ No overdue matches beyond threshold.")
    w("")
    if m["s4a"]:
        w("| Contact | Owner | Status | Days idle | Entered SQL |")
        w("|---|---|---|--:|---|")
        for c in sorted(m["s4a"], key=lambda x: -((x["idle"] or x["created_idle"]) or 0))[:60]:
            w(f"| {md_link(cname(c), clink(c['id']))} | {c['owner_name']} | "
              f"{c['status_label'] or '*(blank)*'} | "
              f"{c['idle'] if c['idle'] is not None else 'never'} | "
              f"{(c.get('date_entered_sql') or '')[:10]} |")
        if len(m["s4a"]) > 60:
            w(f"| …and {len(m['s4a'])-60} more (see snapshot) | | | | |")
    w("")
    if m["s4a_nonb2b"]:
        w(f"> ⚠️ **ALERT:** {len(m['s4a_nonb2b'])} non-B2B SQL(s) also meet the trigger — "
          "the recycler's B2B filter may be broken. Investigate.")
        w("")

    w(f"### 4C Recycled this week — {len(m['s4c'])}")
    w("")
    if m["s4c"]:
        byo = {}
        for c in m["s4c"]:
            byo.setdefault(c["owner_name"], []).append(c)
        w("| Contact | Owner | Now at | Recycled | OK? |")
        w("|---|---|---|---|---|")
        for ow in sorted(byo):
            for c in byo[ow]:
                ok = "✅ MQL" if c["lifecyclestage"] == "marketingqualifiedlead" else "⚠️ still SQL → 4D"
                w(f"| {md_link(cname(c), clink(c['id']))} | {ow} | "
                  f"{LIFECYCLE_LABELS.get(c['lifecyclestage'], c['lifecyclestage'])} | "
                  f"{(c.get('recycled_date') or '')[:10]} | {ok} |")
    else:
        w("- None this week.")
    w("")

    w(f"### 4D Stuck recycles — {len(m['s4d_stuck'])}"
      + ("  ⚠️ ALERT" if m["s4d_stuck"] else ""))
    w("")
    w("*recycled_date set AND still lifecycle = SQL AND entered-SQL before recycled "
      "(the move to MQL failed — lifecycle not cleared first).*")
    w("")
    if m["s4d_stuck"]:
        byo = {}
        for c in m["s4d_stuck"]:
            byo.setdefault(c["owner_name"], []).append(c)
        w("| Contact | Owner | Status | Recycled | Entered SQL |")
        w("|---|---|---|---|---|")
        for ow in sorted(byo):
            for c in byo[ow]:
                w(f"| {md_link(cname(c), clink(c['id']))} | {ow} | "
                  f"{c['status_label'] or '*(blank)*'} | {(c.get('recycled_date') or '')[:10]} | "
                  f"{(c.get('date_entered_sql') or '')[:10]} |")
    else:
        w("- ✅ None.")
    w("")
    if m["s4d_repromo"]:
        w(f"*Re-promotions (info only — sales re-engaged after recycle): {len(m['s4d_repromo'])}.*")
        w("")

    # ---------- SECTION 5 ----------
    w("## Section 5 — Owner Action Lists")
    w("")
    w("*(Full per-owner blocks are also written to `reports/" + str(today) +
      "_owner_<name>.md` for emailing. Section 5a — records changed this week — "
      "begins next run, once a comparable snapshot exists.)*")
    w("")
    blocks = build_owner_blocks(m, today)
    for owner in SALES_OWNERS + ["Nicola Cole"]:
        w(blocks[owner])
        w("")

    # ---------- SECTION 6 ----------
    w("## Section 6 — Workflow Firing Checks (built ≠ working)")
    w("")
    w("| Workflow | Healthy if | Status |")
    w("|---|---|---|")
    healthy = {
        "Sales Activity > Lead Status": "3C created/modified this week = 0",
        "Meeting Booked > Contacted": "no 'Contacted' at Email Contact/Lead/MQL",
        "Stale SQL Recycler": "4D stuck = 0 (and 4A overdue = 0 after claim window)",
        "Lifecycle > Lead Status Sync": "3D rows at Opportunity/Customer = 0",
        "Deal Associated > Qualified": "Opp + In Progress with deals > 0 = 0",
        "Deal required fields": "open deals created this week w/ empty/0 USD = 0",
    }
    for name, ok, ids in m["workflows"]:
        w(f"| {name} | {healthy[name]} | {'✅' if ok else '⚠️'} |")
    w("")
    for name, ok, ids in m["workflows"]:
        if not ok:
            links = ", ".join(md_link("#"+i, clink(i) if name != "Deal required fields" else dlink(i))
                              for i in ids[:25])
            more = f" …(+{len(ids)-25} more)" if len(ids) > 25 else ""
            w(f"- **{name}** ⚠️ {len(ids)}: {links}{more}")
    w("")
    w("---")
    w(f"*Snapshot: `snapshots/{today}.json` · Report: `reports/{today}.md`*")
    w("")
    return "\n".join(A)


def build_owner_blocks(m, today):
    """Return {owner_name: markdown block} for Section 5 (a-f)."""
    bo = m["bucket_owner"]
    # index worklists by bucketed owner
    def idx(items, keyfn=lambda c: c["owner_name"]):
        d = {}
        for it in items:
            d.setdefault(bo(keyfn(it)), []).append(it)
        return d

    s5b = {}
    for ow, lst in m["s5b"].items():
        s5b.setdefault(bo(ow), []).extend(lst)
    i3b = {}
    for d in m["s3b_usd"] + m["s3b_idle"] + m["s3b_pastclose"]:
        i3b.setdefault(bo(d["owner_name"]), []).append(d)
    i3c = idx(m["s3c"]); i3d = idx(m["s3d"]); i3e = idx(m["s3e"])
    i4c = idx(m["s4c"]); i4d = idx(m["s4d_stuck"])
    claim_backlog = idx(m["s4a"]) if str(today) == CLAIM_WINDOW_RUN else {}

    blocks = {}
    allowners = SALES_OWNERS + ["Nicola Cole"]
    for owner in allowners:
        L = [f"### {owner}"]
        has = False

        def sub(title, items, fmt):
            nonlocal has
            if not items:
                return
            has = True
            L.append(f"**{title}** ({len(items)})")
            L.append("")
            for it in items:
                L.append("- " + fmt(it))
            L.append("")

        # 5b recycling warning
        sub(f"Recycling in the next 15 days — log activity now or they return to marketing",
            s5b.get(owner, []),
            lambda c: f"{md_link(cname(c), clink(c['id']))} — idle "
                      f"{c['idle'] if c['idle'] is not None else 'never active'}d")
        # 5c claim window (only on CLAIM_WINDOW_RUN)
        if claim_backlog.get(owner):
            sub("CLAIM WINDOW — log an activity (call/email/note) by Fri 16 Oct to keep",
                claim_backlog[owner],
                lambda c: f"{md_link(cname(c), clink(c['id']))}")
        # 5d recycled this week
        sub("Recycled this week", i4c.get(owner, []),
            lambda c: f"{md_link(cname(c), clink(c['id']))} — now "
                      f"{LIFECYCLE_LABELS.get(c['lifecyclestage'], c['lifecyclestage'])}")
        # 5e deals needing attention
        seen = set()
        deals5 = []
        for d in i3b.get(owner, []):
            if d["id"] not in seen:
                seen.add(d["id"]); deals5.append(d)
        sub("Deals needing attention", deals5,
            lambda d: f"{md_link(d['dealname'], dlink(d['id']))} — {d['stage_label']}, "
                      f"{usd(d['usd'])}, idle {d['idle']}d, close {d.get('closedate','—')}")
        # 5f data fixes (3C,3D,3E,4D) deduped
        seen = set(); fixes = []
        for c in i3c.get(owner, []) + i3d.get(owner, []) + i3e.get(owner, []) + i4d.get(owner, []):
            if c["id"] not in seen:
                seen.add(c["id"]); fixes.append(c)
        sub("Data fixes (3C/3D/3E/4D)", fixes,
            lambda c: f"{md_link(cname(c), clink(c['id']))} — "
                      f"{LIFECYCLE_LABELS.get(c['lifecyclestage'], c['lifecyclestage'])}, "
                      f"{c['status_label'] or 'blank'}"
                      + (f" — {c.get('mismatch')}" if c.get("mismatch") else ""))

        if owner == "Nicola Cole":
            # 3A + 3H + non-sales/unassigned already roll in via bucket_owner
            if m["s3a"]:
                has = True
                L.append(f"**Companies to complete (3A)** ({len(m['s3a'])})")
                L.append("")
                for c in m["s3a"]:
                    L.append(f"- {md_link('#'+c['id'], colink(c['id']))} — "
                             f"{'no owner' if not c.get('owner_id') else m['oname'](c.get('owner_id'))}, "
                             f"{'blank name' if not c.get('name') else c.get('name')}")
                L.append("")
            if m["s3h"]:
                has = True
                L.append(f"**Won but not Customer (3H)** ({len(m['s3h'])})")
                L.append("")
                for h in m["s3h"]:
                    L.append(f"- {md_link(h['company_name'] or '#'+str(h['company_id']), colink(h['company_id']))} "
                             f"— {h['deal']} (type still {h['type']})")
                L.append("")
            # one-off reminder after claim deadline if backlog>0
            if str(today) > CLAIM_DEADLINE and len(m["s4a"]) > 0:
                has = True
                L.append(f"**⚠️ Claim window closed** — open the Stale SQL Recycler workflow "
                         f"and manually enrol the remaining {len(m['s4a'])} contacts.")
                L.append("")

        if not has:
            L.append("*No items this week.*")
            L.append("")
        blocks[owner] = "\n".join(L)
    return blocks


# =========================================================================
# main
# =========================================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="pre-pulled raw_input.json")
    ap.add_argument("--outdir", default=".")
    args = ap.parse_args()

    raw = json.load(open(args.input))
    run_date = dt.date.fromisoformat(raw["run_date"])
    print(f"RECONCILE: spec lead-status label(s) {SPEC_LABEL_RECONCILE} -> portal label(s).")

    m = compute(raw)

    snap_dir = os.path.join(args.outdir, "snapshots")
    rep_dir = os.path.join(args.outdir, "reports")
    os.makedirs(snap_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)
    prev = load_prev(snap_dir, run_date)

    report = render(m, prev)
    rep_path = os.path.join(rep_dir, f"{run_date}.md")
    open(rep_path, "w").write(report)

    # per-owner files
    blocks = build_owner_blocks(m, run_date)
    for owner in SALES_OWNERS + ["Nicola Cole"]:
        fn = owner.split()[0].lower()
        p = os.path.join(rep_dir, f"{run_date}_owner_{fn}.md")
        open(p, "w").write(f"# {owner} — pipeline actions, week of {run_date}\n\n"
                           + blocks[owner] + "\n")

    # snapshot: raw + worklist ids + per-contact lifecycle/status for next diff
    metrics_lite = {
        "q4_new": m["q4"]["new_deals"]["actual"],
        "q4_entered": m["q4"]["entered_trial"]["actual"],
        "q4_closed": m["q4"]["closed_won"]["actual"],
        "s3c": len(m["s3c"]), "s3d": len(m["s3d"]), "s3e": len(m["s3e"]),
        "s4c": len(m["s4c"]), "s4d": len(m["s4d_stuck"]),
        "moved": len(m["moved"]), "open_qty": m["open_qty"], "open_usd": round(m["open_usd"]),
        "sql_total": m["sql_total"],
        "wf": {n: ok for n, ok, _ in m["workflows"]},
    }
    snapshot = {
        "run_date": str(run_date),
        "generated_at": raw.get("generated_at"),
        "funnel_counts": raw["funnel_counts"],
        "metrics_lite": metrics_lite,
        "worklist_ids": {
            "s3a": [c["id"] for c in m["s3a"]],
            "s3b_usd": [d["id"] for d in m["s3b_usd"]],
            "s3b_idle": [d["id"] for d in m["s3b_idle"]],
            "s3b_pastclose": [d["id"] for d in m["s3b_pastclose"]],
            "s3c": [c["id"] for c in m["s3c"]],
            "s3d": [c["id"] for c in m["s3d"]],
            "s3e": [c["id"] for c in m["s3e"]],
            "s3f": [c["id"] for c in m["s3f"]],
            "s3g": [c["id"] for c in m["s3g"]],
            "s4a": [c["id"] for c in m["s4a"]],
            "s4c": [c["id"] for c in m["s4c"]],
            "s4d_stuck": [c["id"] for c in m["s4d_stuck"]],
            "moved": [d["id"] for d in m["moved"]],
        },
        "contact_states": {c["id"]: {"lifecyclestage": c["lifecyclestage"],
                                     "hs_lead_status": c.get("hs_lead_status"),
                                     "owner": c["owner_name"]}
                           for c in m["contacts"]},
    }
    snap_path = os.path.join(snap_dir, f"{run_date}.json")
    json.dump(snapshot, open(snap_path, "w"), indent=1)

    # docx via pandoc
    docx_status = _pandoc(rep_dir, run_date)

    print(f"Wrote {rep_path}")
    print(f"Wrote {snap_path}")
    print(f"Owner files: {len(SALES_OWNERS)+1}")
    print(f"pandoc: {docx_status}")


def _pandoc(rep_dir, run_date):
    md = os.path.join(rep_dir, f"{run_date}.md")
    docx = os.path.join(rep_dir, f"{run_date}.docx")
    tmpl = os.path.join("templates", "pipeline_report_template.docx")
    if not shutil_which("pandoc"):
        return "pandoc not installed (skipped .docx)"
    cmd = ["pandoc", md, "-o", docx]
    if os.path.exists(tmpl):
        cmd += ["--reference-doc=" + tmpl]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        return f"wrote {docx}"
    except subprocess.CalledProcessError as e:
        return f"pandoc failed: {e.stderr.decode()[:200]}"


def shutil_which(x):
    from shutil import which
    return which(x)


if __name__ == "__main__":
    main()
