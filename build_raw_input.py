#!/usr/bin/env python3
"""Assemble raw_input.json for the Brahmarsive weekly pipeline routine from the
records pulled via the HubSpot MCP tools on 2026-09-17. Pure assembly: no
analysis (pipeline.py does all derivations/joins)."""
import csv, datetime as dt, json, os

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = "/home/user/brahmarsive-pipeline/raw_input.json"

def iso(d):  # already-ISO passthrough / None
    return d or None

# ---- owners (search_owners) -------------------------------------------------
owners = {
    "26470572": "Julie Howlett", "83228721": "Nicola Cole",
    "83249199": "Ramjee Jadu", "83361077": "Vrinda Vinod",
    "83434425": "(blank user)", "83434667": "Hari",
    "83464879": "Meark Marketer", "84004434": "Anup Ghoshal",
    "84004493": "Theo Zirogiannis", "85402717": "Vangili Palaniappan",
    "87058564": "Al Mufeed Khazi", "87318764": "Jack Makhlouf",
    "87318803": "Biju Rajesh", "88914104": "SASHI KUMAR",
    "90325392": "Panthalarajakumaran Kamaraj", "90325421": "Aksharaa Parameswaran",
    "96259786": "Mokesh Kumarasamy", "97398549": "Hari haran",
}

# ---- new B2B contacts, last 7 days (query_crm_data, customer_type=B2B) -------
# fields: id, name, lifecyclestage, hs_lead_status, owner_id, createdate,
#         associatedcompanyid, notes_last_updated
new_contacts = [
 ("247912680032","Catherine Richards","lead","Attempted to Contact","84004493","2026-09-11T15:13:46.566Z","58304479529","2026-09-11T15:17:44.126Z"),
 ("247912701320","Amber Whitley","lead","Attempted to Contact","84004493","2026-09-11T15:01:18.635Z","58323001179","2026-09-11T15:07:55.379Z"),
 ("247892134474","Thomas Sloas","opportunity","NEW","87318764","2026-09-11T14:11:04.691Z","55294521201","2026-09-11T14:20:06.479Z"),
 ("247892127866","Russell Heicke","lead","NEW","84004493","2026-09-11T13:53:54.621Z","58298859595",None),
 ("248729103391","Danielle Vauclin","lead","Attempted to Contact","84004493","2026-09-16T07:58:49.336Z","58443765070","2026-09-16T08:23:01Z"),
 ("248729116779","Abraham Tucker","lead","Marketing Engaged","84004493","2026-09-16T08:58:07.155Z","46696075926","2026-09-16T09:03:13.585Z"),
 ("248049045669","Matthew Micari","lead","Attempted to Contact","84004493","2026-09-15T15:12:59.402Z","58413663076","2026-09-15T15:16:41.228Z"),
 ("248048462449","Jennifer Hurrell","lead","Attempted to Contact","84004493","2026-09-14T13:20:06.825Z","58426019709","2026-09-14T13:28:06.454Z"),
 ("248003297335","Nick Finken","lead","Attempted to Contact","84004493","2026-09-14T11:31:10.659Z","58432818733","2026-09-14T12:19:13.229Z"),
 ("248048334643","Christopher Garran","lead","Attempted to Contact","84004493","2026-09-14T12:38:23.646Z","58443366759","2026-09-14T12:46:19.868Z"),
 ("248283281200","Escalona Sarmiento","marketingqualifiedlead",None,None,"2026-09-13T18:01:14.691Z","58426002334","2026-09-15T08:00:00Z"),
 ("247794605920","Francisco Lopez","marketingqualifiedlead",None,None,"2026-09-11T04:51:30.861Z","58320205473","2026-09-15T08:00:00Z"),
 ("248048958885","Megan Bayer","lead","Marketing Engaged","87318764","2026-09-15T10:45:50.471Z","56591630705","2026-09-15T11:01:51.885Z"),
 ("248825873061","Sarah J. Rhoads","lead","Attempted to Contact","84004493","2026-09-16T12:34:31.238Z","43314531643","2026-09-16T12:45:33.024Z"),
 ("248776775235","Benedict Nguyenlee","lead","Attempted to Contact","84004493","2026-09-16T13:16:41.491Z","58414606407","2026-09-16T13:24:34.934Z"),
 ("248729124646","Karen Landry","lead","Attempted to Contact","84004493","2026-09-16T10:08:49.793Z","46696075926","2026-09-16T10:24:54.857Z"),
 ("248729123216","Cordelia Nnedu","lead","Attempted to Contact","84004493","2026-09-16T09:49:53.277Z","46725228926","2026-09-16T10:05:46.730Z"),
 ("248865242371","Denise Harvey","lead","Attempted to Contact","84004493","2026-09-16T15:25:03.772Z","43477242243","2026-09-16T15:29:40.409Z"),
 ("248776735373","Christina Arnold","lead","Attempted to Contact","84004493","2026-09-16T10:16:21.061Z","46696075926","2026-09-16T10:23:13.851Z"),
 ("248825866043","Linda Worley","lead","Attempted to Contact","87318764","2026-09-16T12:18:43.543Z","43314531643","2026-09-16T12:26:08.352Z"),
 ("248845037400","Christina Fieber","lead","Attempted to Contact","84004493","2026-09-16T14:01:41.462Z","58418953367","2026-09-16T14:13:28.352Z"),
 ("248845032257","Tobin Herringshaw","lead","Attempted to Contact","84004493","2026-09-16T13:50:36.541Z","58436161601","2026-09-16T13:57:09.203Z"),
 ("248845029134","Benjamin Moritz","lead","Attempted to Contact","84004493","2026-09-16T13:39:03.095Z","58416022332","2026-09-16T13:44:12.853Z"),
 ("248855158319","Jo Beck","lead","Attempted to Contact","84004493","2026-09-16T14:22:05.077Z","58418953367","2026-09-16T14:30:20.165Z"),
 ("248543241352","Har Hory","marketingqualifiedlead",None,None,"2026-09-15T03:43:59.191Z","58440013893","2026-09-17T08:00:00Z"),
 ("248578677553","Anatoly Mikutin","marketingqualifiedlead",None,None,"2026-09-15T08:37:10.941Z","58422044225","2026-09-17T08:00:00Z"),
]

# ---- miscategorised B2B contacts (70): working lead status but lifecycle lead/mql
# fields: id, name, hs_lead_status, lifecyclestage, owner_id
misc = [
 ("247912680032","Catherine Richards","Attempted to Contact","lead","84004493"),
 ("170921907058","Melanie Croucher","Attempted to Contact","lead","84004493"),
 ("201471431303","Lewis Matthews","Attempted to Contact","lead","84004493"),
 ("156089636432","Nicola Brandon","Attempted to Contact","lead","84004493"),
 ("236698956937","Margo Bruce","Attempted to Contact","lead","84004493"),
 ("156056267784","Simon Badley","Attempted to Contact","lead","84004493"),
 ("201457344900","Liam Fisher","Attempted to Contact","lead","84004493"),
 ("235381209239","Rena Williams","Attempted to Contact","marketingqualifiedlead","87318764"),
 ("247912701320","Amber Whitley","Attempted to Contact","lead","84004493"),
 ("241248188337","Dr. Isabella Kurnia Liem","Attempted to Contact","lead","84004434"),
 ("241259192485","Dr. Nur Arfian","Attempted to Contact","lead","84004434"),
 ("241263920400","Sutisa Thanoi","Attempted to Contact","lead","84004434"),
 ("244726516289","Celia Rodriguez","Attempted to Contact","lead","84004493"),
 ("238084873895","Nyree Hulme","Attempted to Contact","lead","84004493"),
 ("234277974867","Dr. Jobby George","Attempted to Contact","lead","84004434"),
 ("200720753590","Martin Rigby","Attempted to Contact","lead","84004493"),
 ("236698774419","Erika Neck","Attempted to Contact","lead","84004493"),
 ("222915600009","danish anwer","Attempted to Contact","marketingqualifiedlead","84004434"),
 ("166547187004","Gillian O'Connell","Attempted to Contact","lead","84004493"),
 ("156056956002","Kevin Sheil","Attempted to Contact","lead","84004493"),
 ("182220520075","Lecia Robinson","Attempted to Contact","lead","84004493"),
 ("245016990841","Antonia Roseweir","Attempted to Contact","marketingqualifiedlead","84004493"),
 ("244701470245","Sten Erici","Attempted to Contact","marketingqualifiedlead","84004493"),
 ("244927041061","Anique Atherley","Attempted to Contact","marketingqualifiedlead","84004493"),
 ("182329724764","Christa Marsh","Attempted to Contact","lead","84004493"),
 ("244717176946","Kuan Hsun Lin","Attempted to Contact","marketingqualifiedlead","84004493"),
 ("245017003522","André Batista","Attempted to Contact","marketingqualifiedlead","84004493"),
 ("245017017430","Sandro Schreiber de Oliveira","Attempted to Contact","marketingqualifiedlead","84004493"),
 ("245017008917","Jill Hall","Attempted to Contact","marketingqualifiedlead","84004493"),
 ("245016998512","Tara Leslie","Attempted to Contact","marketingqualifiedlead","84004493"),
 ("245002909959","Sharon Sneddon","Attempted to Contact","marketingqualifiedlead","84004493"),
 ("180631150759","Mary Carter","Attempted to Contact","lead","84004493"),
 ("201761989493","Jeremy King","Attempted to Contact","lead","84004493"),
 ("156054788627","Rachel Riendeau","Attempted to Contact","lead","84004493"),
 ("181724592977","Kelsey Picha","Attempted to Contact","lead","84004493"),
 ("170907279233","Monica Mobley","Attempted to Contact","lead","84004493"),
 ("248729103391","Danielle Vauclin","Attempted to Contact","lead","84004493"),
 ("180128749993","Darrell Demartino","Attempted to Contact","lead","84004493"),
 ("180631506770","Susanne Daly","Attempted to Contact","lead","84004493"),
 ("182392465326","Dawn Roberts","Attempted to Contact","lead","84004493"),
 ("248049045669","Matthew Micari","Attempted to Contact","lead","84004493"),
 ("201504430759","Shawn Macauley","Attempted to Contact","lead","84004493"),
 ("170902291322","Jenna Linskens","Attempted to Contact","lead","84004493"),
 ("156089636437","Erin Kay","Attempted to Contact","lead","84004493"),
 ("167949179204","Louise Franco","Attempted to Contact","lead","84004493"),
 ("248048462449","Jennifer Hurrell","Attempted to Contact","lead","84004493"),
 ("248003297335","Nick Finken","Attempted to Contact","lead","84004493"),
 ("248048334643","Christopher Garran","Attempted to Contact","lead","84004493"),
 ("180631260015","Chad Farmer","Attempted to Contact","lead","84004493"),
 ("182002738008","Rosemary Bassey","Attempted to Contact","lead","84004493"),
 ("182002674315","Dr. José A. Cardé","Attempted to Contact","lead","84004493"),
 ("180128815693","David Katz","Attempted to Contact","lead","84004493"),
 ("180631343707","Beth Kersten","Attempted to Contact","lead","84004493"),
 ("182003517243","Ghanshyam Heda","Attempted to Contact","lead","84004493"),
 ("181598200877","Kathryn Norton Vaughan","Attempted to Contact","lead","87318764"),
 ("180893370409","Mary Myers","Attempted to Contact","lead","84004493"),
 ("182003899430","Brandis Hubbard","Attempted to Contact","lead","84004493"),
 ("182099221659","Prasanna Abeyrathna","Attempted to Contact","lead","84004493"),
 ("180631571202","Dan Bunker","Attempted to Contact","lead","84004493"),
 ("248825873061","Sarah J. Rhoads","Attempted to Contact","lead","84004493"),
 ("248776775235","Benedict Nguyenlee","Attempted to Contact","lead","84004493"),
 ("248729124646","Karen Landry","Attempted to Contact","lead","84004493"),
 ("248729123216","Cordelia Nnedu","Attempted to Contact","lead","84004493"),
 ("248865242371","Denise Harvey","Attempted to Contact","lead","84004493"),
 ("248776735373","Christina Arnold","Attempted to Contact","lead","84004493"),
 ("248825866043","Linda Worley","Attempted to Contact","lead","87318764"),
 ("248845037400","Christina Fieber","Attempted to Contact","lead","84004493"),
 ("248845032257","Tobin Herringshaw","Attempted to Contact","lead","84004493"),
 ("248845029134","Benjamin Moritz","Attempted to Contact","lead","84004493"),
 ("248855158319","Jo Beck","Attempted to Contact","lead","84004493"),
]

# ---- recycled this week (9), all customer types (all landed B2B) ------------
# fields: id, name, customer_type, recycled_date, lifecyclestage, owner_id, hs_lead_status
recycled_week = [
 ("228277044048","nasrin fathi","B2B","2026-09-15T00:00:00Z","salesqualifiedlead","84004434","UNQUALIFIED"),
 ("210442806874","Ana María Insausti Serrano","B2B","2026-09-15T00:00:00Z","salesqualifiedlead","84004493","UNQUALIFIED"),
 ("170920180796","Andreas Protopapas","B2B","2026-09-15T00:00:00Z","salesqualifiedlead","84004493","UNQUALIFIED"),
 ("219389185417","Dhiraj Kalra","B2B","2026-09-14T00:00:00Z","salesqualifiedlead","84004434","UNQUALIFIED"),
 ("220031147837","Dhruv Kumar","B2B","2026-09-14T00:00:00Z","salesqualifiedlead","84004434","UNQUALIFIED"),
 ("226755596602","Dr.Mayson Louzon","B2B","2026-09-14T00:00:00Z","salesqualifiedlead","84004434","UNQUALIFIED"),
 ("224554915662","Mohamed Elsaidy","B2B","2026-09-16T00:00:00Z","salesqualifiedlead","84004493","UNQUALIFIED"),
 ("226760459534","Fatima M. Abbas","B2B","2026-09-16T00:00:00Z","salesqualifiedlead","84004493","UNQUALIFIED"),
 ("225878970037","Angham Abdelkader","B2B","2026-09-16T00:00:00Z","salesqualifiedlead","84004493","UNQUALIFIED"),
]

# ---- companies referenced by new contacts + new B2B companies ---------------
# fields: id, name(None=blank), createdate, owner_id(None), customer_type__cloned_(None)
companies = [
 ("58304479529",None,"2026-09-11T15:13:47.756Z","84004493",None),
 ("58323001179","Hanover College","2026-09-11T15:01:19.716Z","84004493",None),
 ("55294521201","Bluegrass Community & Technical College","2026-05-26T17:39:48.482Z","87318764","B2B"),
 ("58298859595","Downey Unified School District","2026-09-11T13:53:55.384Z","84004493",None),
 ("58443765070","Fletcher Technical Community College","2026-09-16T07:58:50.072Z","84004493",None),
 ("46696075926","Southern Arkansas University","2025-12-05T08:39:59.959Z","84004493","B2B"),
 ("58413663076","Beaver Country Day School","2026-09-15T15:13:00.584Z","84004493",None),
 ("58426019709","Bryant University","2026-09-14T13:20:08.376Z","84004493",None),
 ("58432818733","Saint Albert Catholic Schools","2026-09-14T11:31:11.525Z","84004493",None),
 ("58443366759","Cape Henry Collegiate School","2026-09-14T12:38:24.491Z","84004493",None),
 ("58426002334",None,"2026-09-13T18:01:18.812Z",None,"B2B"),
 ("58320205473",None,"2026-09-11T04:51:33.751Z",None,"B2B"),
 ("56591630705","Kansas Health Science University - Kansas College of Osteopathic Medicine","2026-07-15T15:38:02.924Z","87318764","B2B"),
 ("43314531643","University of Arkansas for Medical Sciences","2025-10-31T14:50:51.130Z","87318764","B2B"),
 ("58414606407","Collin College","2026-09-16T13:16:42.500Z","84004493",None),
 ("46725228926","TUSKEGEE UNIVERSITY","2025-12-05T11:56:12.427Z","84004493","B2B"),
 ("43477242243","Arkansas State University-Newport","2025-11-01T08:19:46.705Z","84004493","B2B"),
 ("58418953367","Horry-Georgetown Technical College","2026-09-16T14:01:42.379Z","84004493",None),
 ("58436161601","The New Community School","2026-09-16T13:50:37.597Z","84004493",None),
 ("58416022332","Governor of Wyoming","2026-09-16T13:39:03.989Z","84004493",None),
 ("58440013893",None,"2026-09-15T03:44:07.899Z",None,"B2B"),
 ("58422044225",None,"2026-09-15T08:37:14.633Z",None,"B2B"),
]

# ---- assemble contacts_b2b (union, dedup by id) -----------------------------
cb = {}
for (i,n,ls,st,ow,cd,acid,nlu) in new_contacts:
    cb[i] = {"id":i,"name":n,"lifecyclestage":ls,"hs_lead_status":st,"customer_type":"B2B",
             "createdate":iso(cd),"notes_last_updated":iso(nlu),"notes_last_contacted":None,
             "associatedcompanyid":acid,"recycled_date":None,"unqualified_reason":None,
             "owner_id":ow,"b2b_contact_lead_score_combined":None,"_src":["new"]}
for (i,n,st,ls,ow) in misc:
    if i in cb:
        cb[i]["_src"].append("miscategorised")
        # ensure status/lifecycle consistent
        cb[i]["hs_lead_status"] = cb[i]["hs_lead_status"] or st
    else:
        cb[i] = {"id":i,"name":n,"lifecyclestage":ls,"hs_lead_status":st,"customer_type":"B2B",
                 "createdate":None,"notes_last_updated":None,"notes_last_contacted":None,
                 "associatedcompanyid":None,"recycled_date":None,"unqualified_reason":None,
                 "owner_id":ow,"b2b_contact_lead_score_combined":None,"_src":["miscategorised"]}
for (i,n,ct,rd,ls,ow,st) in recycled_week:
    if i in cb:
        cb[i]["_src"].append("recycled_week"); cb[i]["recycled_date"]=iso(rd)
    else:
        cb[i] = {"id":i,"name":n,"lifecyclestage":ls,"hs_lead_status":st,"customer_type":ct,
                 "createdate":None,"notes_last_updated":None,"notes_last_contacted":None,
                 "associatedcompanyid":None,"recycled_date":iso(rd),"unqualified_reason":"No response/did not connect",
                 "owner_id":ow,"b2b_contact_lead_score_combined":None,"_src":["recycled_week"]}
contacts_b2b = list(cb.values())

# ---- sql_all_types (170) from parsed file -----------------------------------
sql_raw = json.load(open(os.path.join(HERE,"sql_all_types.json")))
sql_all_types = []
for r in sql_raw:
    fn=(r.get("firstname") or "").strip(); ln=(r.get("lastname") or "").strip()
    name=(fn+" "+ln).strip() or r.get("email") or r.get("id")
    sql_all_types.append({
        "id":r["id"],"name":name,"customer_type":r.get("customer_type"),
        "hs_lead_status":r.get("hs_lead_status"),
        "notes_last_updated":iso(r.get("notes_last_updated")),
        "recycled_date":iso(r.get("recycled_date")),
        "owner_id":r.get("owner_id"),
    })

# ---- companies_b2b ----------------------------------------------------------
companies_b2b = [{"id":i,"name":n,"createdate":iso(cd),"owner_id":ow,
                  "customer_type__cloned_":ct} for (i,n,cd,ow,ct) in companies]

# ---- deals_b2b from deals.tsv ----------------------------------------------
def val(x):
    x=(x or "").strip()
    return None if x in ("","Unassigned") else x
deals_b2b=[]
with open(os.path.join(HERE,"deals.tsv")) as f:
    for row in csv.DictReader(f, delimiter="\t"):
        deals_b2b.append({
            "id":row["deal_id"],"dealname":row["dealname"],"dealstage":row["dealstage"],
            "pipeline":row["pipeline"],"amount":val(row["amount"]),
            "amount_in_home_currency":val(row["amount_in_home_currency"]),
            "deal_currency_code":val(row["deal_currency_code"]),
            "createdate":val(row["createdate"]),"hs_lastmodifieddate":val(row["hs_lastmodifieddate"]),
            "notes_last_updated":val(row["notes_last_updated"]),
            "hs_is_closed":row["hs_is_closed"].strip(),
            "hs_v2_date_entered_current_stage":val(row["hs_v2_date_entered_current_stage"]),
            "company_id":val(row["company_id"]),"owner_id":val(row["owner_id"]),
        })

raw = {
    "generated_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "source": "hubspot (pulled via HubSpot MCP, 2026-09-17)",
    "as_of_date": "2026-09-17",
    "window_days": 7, "idle_days": [30, 60],
    "notes": ("B2B contacts scoped via customer_type=B2B; B2B companies via "
              "customer_type__cloned_=B2B; deals scoped via COMPANY.customer_type__cloned_=B2B. "
              "contacts_b2b = union of new-7d + miscategorised + recycled-this-week (the contact "
              "populations the report needs at record level). sql_all_types = every contact at "
              "lifecyclestage=salesqualifiedlead across all customer_types (true SQL base). "
              "companies_b2b = new B2B companies (7d) plus every company referenced by a new "
              "contact (for the new-vs-existing-account join), each tagged with its own "
              "customer_type__cloned_ and createdate."),
    "portal_totals_reference": {
        "b2b_contacts_by_lifecycle": {"subscriber":26,"lead":3131,"marketingqualifiedlead":60,
            "salesqualifiedlead":132,"opportunity":181,"customer":21,"other":10},
        "b2b_companies_total": 1856,
        "contacts_at_sql_by_customer_type": {"B2B":132,"B2C":8,"Partner":17,"Internal":8,
            "Partner-B2B":3,"Unassigned":2},
    },
    "owners": owners,
    "contacts_b2b": contacts_b2b,
    "sql_all_types": sql_all_types,
    "companies_b2b": companies_b2b,
    "deals_b2b": deals_b2b,
}

json.dump(raw, open(OUT,"w"), indent=2, ensure_ascii=False)
print("contacts_b2b:",len(contacts_b2b),"(new/misc/recycled union)")
print("sql_all_types:",len(sql_all_types))
print("companies_b2b:",len(companies_b2b))
print("deals_b2b:",len(deals_b2b))
print("wrote",OUT)
