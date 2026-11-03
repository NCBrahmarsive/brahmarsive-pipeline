#!/usr/bin/env python3
"""
Assemble raw_input.json for the Brahmarsive weekly pipeline report from the
datasets pulled via the HubSpot MCP tools and stored under .rawpull/.

Direct egress to api.hubapi.com is blocked in the run environment, so all CRM
data is pulled through the HubSpot MCP layer (query_crm_data / search_owners /
get_properties) and cached as JSON in .rawpull/. This script parses those
caches, normalises them, de-duplicates contacts by record id, and writes a
single raw_input.json in the shape pipeline.py expects.

Files consumed (all in .rawpull/):
  contacts_sql.json      402 B2B contacts at Sales Qualified Lead
  contacts_opp.json      198 B2B contacts at Opportunity
  contacts_other.json    103 B2B contacts at subscriber/MQL/customer/other
  contacts_leadexc.json  228 B2B lead-stage contacts whose lead status is NOT
                             New Lead / Marketing Engaged (the only lead-stage
                             rows that can be miscategorised or mismatched)
  contacts_new.json       24 B2B contacts created in the last 7 days
  companies_3a.json       25 B2B companies missing name and/or owner
  deals.json              87 B2B deals (scoped via COMPANY.customer_type__cloned_)
  aux.json                lead-status map, owners, funnel counts, aggregates

The B2B contact funnel is dominated by 3066 plain 'lead' rows that need no
per-record logic (only an aggregate count for the funnel table), so they are
carried as aux.funnel_counts rather than pulled individually. Every lead that
CAN appear in a worklist (miscategorised / status-mismatch / new-in-window) is
pulled in full via contacts_leadexc.json + contacts_new.json.
"""
import datetime as dt
import glob
import json
import os

RAW = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".rawpull")

CONTACT_FILES = [
    "contacts_sql.json", "contacts_opp.json", "contacts_other.json",
    "contacts_leadexc.json", "contacts_new.json",
]


def _load_results(path):
    """Return the list of property-dicts from an MCP query_crm_data cache."""
    with open(path) as f:
        blob = json.load(f)
    out = []
    for item in blob.get("results", []):
        content = item.get("content")
        if isinstance(content, str):
            try:
                rec = json.loads(content)
            except json.JSONDecodeError:
                continue
            props = rec.get("properties", rec)
        elif isinstance(content, dict):
            props = content.get("properties", content)
        else:
            continue
        out.append(props)
    return out


def _epoch_to_iso(v):
    if v in (None, "", "Unassigned"):
        return None
    try:
        ms = int(v)
    except (TypeError, ValueError):
        return None
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat()


def _ts(props, name):
    """Prefer the *_iso variant; fall back to converting the epoch-ms value."""
    iso = props.get(name + "_iso")
    if iso:
        return iso
    return _epoch_to_iso(props.get(name))


def _clean(v):
    if v in ("", "Unassigned"):
        return None
    return v


def _name(props):
    n = props.get("hs_full_name_or_email")
    if n and n.strip():
        return n.strip()
    fn = (props.get("firstname") or "").strip()
    ln = (props.get("lastname") or "").strip()
    full = (fn + " " + ln).strip()
    if full:
        return full
    return props.get("email") or "(no name)"


def normalise_contact(props):
    return {
        "id": str(props.get("hs_object_id")),
        "name": _name(props),
        "email": props.get("email"),
        "lifecyclestage": props.get("lifecyclestage"),
        "hs_lead_status": _clean(props.get("hs_lead_status")),
        "createdate": _ts(props, "createdate"),
        "notes_last_updated": _ts(props, "notes_last_updated"),
        "notes_last_contacted": _ts(props, "notes_last_contacted"),
        "associatedcompanyid": _clean(props.get("associatedcompanyid")),
        "recycled_date": _ts(props, "recycled_date"),
        "unqualified_reason": _clean(props.get("unqualified_reason")),
        "num_associated_deals": _clean(props.get("num_associated_deals")),
        "date_entered_sql": _ts(props, "hs_v2_date_entered_salesqualifiedlead"),
        "date_entered_mql": _ts(props, "hs_v2_date_entered_marketingqualifiedlead"),
        "owner_id": _clean(props.get("hubspot_owner_id")),
        "score": _clean(props.get("b2b_contact_lead_score_combined")),
    }


def _merge(a, b):
    """Merge b into a, keeping existing non-empty values in a."""
    for k, v in b.items():
        if a.get(k) in (None, "") and v not in (None, ""):
            a[k] = v
    return a


def main():
    aux = json.load(open(os.path.join(RAW, "aux.json")))

    # ---- contacts (dedupe by id across the stage files) -------------------
    by_id = {}
    for fn in CONTACT_FILES:
        for props in _load_results(os.path.join(RAW, fn)):
            c = normalise_contact(props)
            if not c["id"] or c["id"] == "None":
                continue
            if c["id"] in by_id:
                _merge(by_id[c["id"]], c)
            else:
                by_id[c["id"]] = c
    contacts_b2b = list(by_id.values())

    # ---- companies (3A worklist) ------------------------------------------
    comp_blob = json.load(open(os.path.join(RAW, "companies_3a.json")))
    owner_null = set(comp_blob.get("owner_null_ids", []))
    companies_3a = []
    for r in comp_blob.get("results", []):
        cid = str(r.get("hs_object_id"))
        companies_3a.append({
            "id": cid,
            "name": _clean(r.get("name")),         # None => blank
            "createdate": r.get("createdate_iso") or _epoch_to_iso(r.get("createdate")),
            "owner_id": _clean(r.get("hubspot_owner_id")),
            "original_source": _clean(r.get("hs_analytics_source")),
            "created_by": _clean(r.get("hs_created_by_user_id")),
            "lifecyclestage": r.get("lifecyclestage"),
            "missing_owner": (cid in owner_null) or not _clean(r.get("hubspot_owner_id")),
            "missing_name": not _clean(r.get("name")),
        })

    # ---- deals ------------------------------------------------------------
    deal_blob = json.load(open(os.path.join(RAW, "deals.json")))
    deals_b2b = []
    for d in deal_blob.get("deals", []):
        deals_b2b.append({
            "id": str(d["id"]),
            "dealname": d.get("name"),
            "dealstage": d.get("stage"),
            "amount": _clean(d.get("amount")),
            "amount_in_home_currency": _clean(d.get("usd")),
            "deal_currency_code": d.get("currency"),
            "createdate": d.get("createdate"),
            "closedate": d.get("closedate"),
            "notes_last_updated": _clean(d.get("nlu")),
            "hs_is_closed": d.get("closed"),
            "date_entered_current_stage": _clean(d.get("dcs")),
            "date_entered_disc_trial": _clean(d.get("ddt")),
            "owner_id": _clean(d.get("owner")),
            "company_id": _clean(d.get("company")),
            "company_type": _clean(d.get("ctype")),
        })

    raw = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "run_date": aux["run_date"],
        "window_days": 7,
        "idle_days": [30, 60],
        "source": "hubspot (pulled via HubSpot MCP, cached under .rawpull/)",
        "lead_status_map": aux["lead_status_map"],
        "owners": aux["owners"],
        "sales_owner_names": aux["sales_owner_names"],
        "funnel_counts": aux["funnel_counts"],
        "sql_status_counts": aux["sql_status_counts"],
        "lead_status_counts": aux["lead_status_counts"],
        "non_b2b_sql": aux["non_b2b_sql"],
        "totals": aux["totals"],
        "new_company_ids": aux["new_company_ids"],
        "contacts_b2b": contacts_b2b,
        "companies_3a": companies_3a,
        "deals_b2b": deals_b2b,
    }

    out = os.path.join(os.path.dirname(RAW), "raw_input.json")
    with open(out, "w") as f:
        json.dump(raw, f, indent=1)

    # sanity report
    stages = {}
    for c in contacts_b2b:
        stages[c["lifecyclestage"]] = stages.get(c["lifecyclestage"], 0) + 1
    print(f"Wrote {out}")
    print(f"  contacts_b2b pulled: {len(contacts_b2b)}  by stage: {stages}")
    print(f"  companies_3a: {len(companies_3a)}  deals_b2b: {len(deals_b2b)}")


if __name__ == "__main__":
    main()
