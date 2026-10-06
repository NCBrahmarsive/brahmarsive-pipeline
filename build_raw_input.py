#!/usr/bin/env python3
"""Assemble raw_input.json (new schema) from the per-dataset raw pulls.

All data was pulled from HubSpot via the MCP tools (direct egress to
api.hubapi.com is blocked in this environment) and saved under raw/.
This builder only reshapes + validates; it does not touch the network.
"""
import csv, json, os, re, sys

RAW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw")
OUT = sys.argv[1] if len(sys.argv) > 1 else "raw_input.json"
GENERATED_AT = "2026-10-06T06:11:55Z"


def load(name):
    with open(os.path.join(RAW, name)) as f:
        return json.load(f)


def name_of(p):
    n = (p.get("hs_full_name_or_email") or "").strip()
    if n:
        return n
    fn = (p.get("firstname") or "").strip()
    ln = (p.get("lastname") or "").strip()
    full = (fn + " " + ln).strip()
    return full or (p.get("email") or "").strip()


# ---- contacts_b2b : new(18) + miscategorised-candidates(51), dedup by id ----
new_raw = load("new_contacts.json")["results"]
contacts = {}
for r in new_raw:
    p = r["properties"]
    cid = str(p["hs_object_id"])
    contacts[cid] = {
        "id": cid,
        "name": name_of(p),
        "lifecyclestage": p.get("lifecyclestage"),
        "hs_lead_status": p.get("hs_lead_status"),
        "createdate": p.get("createdate"),
        "notes_last_updated": p.get("notes_last_updated"),
        "notes_last_contacted": p.get("notes_last_contacted"),
        "associatedcompanyid": p.get("associatedcompanyid"),
        "recycled_date": p.get("recycled_date"),
        "unqualified_reason": p.get("unqualified_reason"),
        "owner_id": p.get("hubspot_owner_id"),
        "b2b_contact_lead_score_combined": p.get("b2b_contact_lead_score_combined"),
        "_source": "new_7d",
    }

miscat = load("miscat.json")
for m in miscat:
    cid = str(m["id"])
    if cid in contacts:
        contacts[cid]["_source"] = "new_7d+miscat_candidate"
        continue
    contacts[cid] = {
        "id": cid,
        "name": m["name"],
        "lifecyclestage": m["lifecyclestage"],
        "hs_lead_status": m["hs_lead_status"],
        "createdate": None,
        "notes_last_updated": None,
        "notes_last_contacted": None,
        "associatedcompanyid": None,
        "recycled_date": None,
        "unqualified_reason": None,
        "owner_id": m.get("owner_id"),
        "b2b_contact_lead_score_combined": None,
        "_source": "miscat_candidate",
    }
contacts_b2b = list(contacts.values())

# ---- sql_all_types (all customer types) ----
sql_file = load("sql_all_types.json")["results"]
sql_all = []
for r in sql_file:
    p = r["properties"]
    sql_all.append({
        "id": str(p["hs_object_id"]),
        "name": name_of(p),
        "customer_type": p.get("customer_type"),
        "hs_lead_status": p.get("hs_lead_status"),
        "notes_last_updated": p.get("notes_last_updated"),
        "recycled_date": p.get("recycled_date"),
        "owner_id": p.get("hubspot_owner_id"),
        "lifecyclestage": p.get("lifecyclestage"),
    })

# ---- companies ----
comp = load("companies.json")
companies_b2b = [
    {"id": c["id"], "name": c["name"], "createdate": c["createdate"],
     "owner_id": c["owner_id"]}
    for c in comp["new_b2b_companies"]
]
associated_companies = comp["associated_companies"]

# ---- deals_b2b (from TSV) ----
def num(v):
    v = (v or "").strip()
    if v == "" or v.lower() == "unassigned":
        return None
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except ValueError:
        return None

def ts(v):
    v = (v or "").strip()
    if v == "" or v.lower() == "unassigned":
        return None
    return v

STAGE_RE = re.compile(r"\(([^()]*)\)\s*$")
deals = []
with open(os.path.join(RAW, "deals.tsv")) as f:
    rd = csv.reader(f, delimiter="\t")
    header = next(rd)
    for row in rd:
        if not row or all(c.strip() == "" for c in row):
            continue
        d = dict(zip(header, row))
        stage_label = d["dealstage"]
        m = STAGE_RE.search(stage_label)
        stage_internal = m.group(1) if m else stage_label
        deals.append({
            "id": d["deal_id"].strip(),
            "dealname": d["dealname"].strip(),
            "dealstage": stage_internal,
            "dealstage_label_raw": stage_label.strip(),
            "amount": num(d["amount"]),
            "amount_in_home_currency": num(d["amount_in_home_currency"]),
            "deal_currency_code": d["deal_currency_code"].strip() or None,
            "createdate": ts(d["createdate"]),
            "hs_lastmodifieddate": ts(d["hs_lastmodifieddate"]),
            "notes_last_updated": ts(d["notes_last_updated"]),
            "hs_is_closed": d["hs_is_closed"].strip().lower() == "true",
            "hs_v2_date_entered_current_stage": ts(d["hs_v2_date_entered_current_stage"]),
            "company_id": d["company_id"].strip() or None,
            "owner_id": d["owner_id"].strip() or None,
        })

recycled_week = load("recycled_week.json")
owners = load("owners.json")

# --------------------------- validation ---------------------------
errs = []
if len(deals) != 85: errs.append(f"deals={len(deals)} expected 85")
if len(sql_all) != 197: errs.append(f"sql_all={len(sql_all)} expected 197")
b2b_sql = [s for s in sql_all if s["customer_type"] == "B2B"]
if len(b2b_sql) != 159: errs.append(f"b2b_sql={len(b2b_sql)} expected 159")
bounce = [s for s in b2b_sql if s["recycled_date"]]
if len(bounce) != 37: errs.append(f"bounceback_b2b={len(bounce)} expected 37")
if len(new_raw) != 18: errs.append(f"new_contacts={len(new_raw)} expected 18")
if len(miscat) != 51: errs.append(f"miscat={len(miscat)} expected 51")
if errs:
    print("VALIDATION FAILED:\n  " + "\n  ".join(errs), file=sys.stderr)
    sys.exit(1)

out = {
    "schema_version": 2,
    "source": "hubspot (pulled via HubSpot MCP, 2026-10-06)",
    "generated_at": GENERATED_AT,
    "window_days": 7,
    "idle_days": [30, 60],
    "note": (
        "Direct egress to api.hubapi.com is blocked by org policy, so records "
        "were pulled via the HubSpot MCP tools. contacts_b2b holds the B2B "
        "contacts relevant to the report (created in the last 7d, plus the "
        "lead/MQL miscategorised candidates); SQL contact metrics come from "
        "sql_all_types (all customer types); deals scoped B2B via "
        "COMPANY.customer_type__cloned_='B2B'."),
    "owners": owners,
    "contacts_b2b": contacts_b2b,
    "sql_all_types": sql_all,
    "companies_b2b": companies_b2b,
    "associated_companies": associated_companies,
    "deals_b2b": deals,
    "recycled_week": recycled_week,
}
with open(OUT, "w") as f:
    json.dump(out, f, indent=2, ensure_ascii=False)
print(f"Wrote {OUT}")
print(f"  contacts_b2b={len(contacts_b2b)} (new+miscat, dedup)")
print(f"  sql_all_types={len(sql_all)} (B2B={len(b2b_sql)}, bounceback_b2b={len(bounce)})")
print(f"  companies_b2b(new)={len(companies_b2b)} associated_companies={len(associated_companies)}")
print(f"  deals_b2b={len(deals)}")
print(f"  recycled_week={len(recycled_week)}")
print("  validation: OK")
