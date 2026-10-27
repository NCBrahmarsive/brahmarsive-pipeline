#!/usr/bin/env python3
"""
Assemble raw_input.json for the Oct 2026 Brahmarsive routine from the HubSpot
MCP pull.

Bulk B2B contacts were pulled via query_crm_data and saved by the harness to
tool-result files (too large to inline); their paths are in CONTACT_FILES.
The smaller sets (deals, companies, new-lead contacts, non-B2B SQL) came back
inline and are embedded below, transcribed to the minimal fields the pipeline
needs. Deal figures are asserted against the server-side stage aggregates so a
transcription slip cannot pass silently.

Run:  python3 build_input.py
"""
import datetime as dt
import glob
import json
import os

RUN_DATE = "2026-10-27"
WINDOW_DAYS = 7
TOOLDIR = ("/root/.claude/projects/-home-user-brahmarsive-pipeline/"
           "2c8ca5d8-5335-5a10-9d0f-7293007d2606/tool-results")
CONTACT_FILES = [
    f"{TOOLDIR}/mcp-HubSpot-query_crm_data-1793082561384.txt",  # non-lead ORDER BY id off 0 (500)
    f"{TOOLDIR}/mcp-HubSpot-query_crm_data-1793082559925.txt",  # non-lead ORDER BY id off 500 (197)
    f"{TOOLDIR}/mcp-HubSpot-query_crm_data-1793082561868.txt",  # lead off-status ORDER BY id (214)
]

# --- Owner id (hs_internal_user_id) -> name ------------------------------
OWNERS = {
    "101183194": "Betty Thomas",
    "97398549": "Hari haran",
    "96259786": "Mokesh Kumarasamy",
    "87058564": "Al Mufeed Khazi",
    "90325421": "Aksharaa Parameswaran",
    "90325392": "Panthalarajakumaran Kamaraj",
    "87318803": "Biju Rajesh",
    "87318764": "Jack Makhlouf",
    "85402717": "Vangili Palaniappan",
    "84004493": "Theo Zirogiannis",
    "84004434": "Anup Ghoshal",
    "83361077": "Vrinda Vinod",
    "83249199": "Ramjee Jadu",
    "83228721": "Nicola Cole",
}

# --- hs_lead_status value -> label (from get_properties) ------------------
LEAD_STATUS_MAP = {
    "NEW": "New Lead",
    "Marketing Engaged": "Marketing Engaged",
    "MQL": "MQL",
    "Attempted to Contact": "Attempted to Contact",
    "CONNECTED": "Contacted",
    "Qualified": "Qualified",
    "IN_PROGRESS": "In Progress",
    "PROPOSAL_REQUEST": "Proposal Request",
    "UNQUALIFIED": "Unqualified",
    "FUTURE_LEAD": "Future Lead",
    "Onboarded Partner": "Onboarded Partner",
    "Customer": "Customer",
}

# Server-side aggregates used to validate the deal transcription.
DEAL_STAGE_COUNTS = {"1217017229": 26, "1217017230": 15, "appointmentscheduled": 9,
                     "closedlost": 23, "closedwon": 6, "presentationscheduled": 8}
DEAL_STAGE_USD = {"1217017229": 148981.2, "1217017230": 144900.3,
                  "appointmentscheduled": 1259577.3, "closedlost": 617153.4,
                  "closedwon": 50000.0, "presentationscheduled": 34000.0}

# --- Deals (87 B2B, scoped via COMPANY.customer_type__cloned_ = B2B) ------
# tuple: (id, stage, amount, usd, curr, created, close, lastmod, lastact,
#         closed, entered_current, entered_trial, company_id, company_type,
#         owner, dealname)
D = None  # None marks an empty USD / date (server showed "Unassigned")
DEALS_RAW = [
 ("59729752194","1217017229",0,0,"USD","2026-04-28","2027-01-01","2026-10-27","2026-10-27",False,"2026-04-28",None,"41738243363","PROSPECT","87318764","(AC) Western University of Health Sciences"),
 ("60877637801","1217017230",900000,9342.9,"INR","2026-06-05","2026-12-31","2026-10-13","2026-10-13",False,"2026-07-21","2026-07-21","42421571936","PROSPECT","87318803","Pondicherry Institute of Medical Sciences & Research, Pondicherry - New Deal"),
 ("60271617796","1217017229",20000,20000,"USD","2026-04-24","2026-12-31","2026-08-18","2026-07-28",False,"2026-07-28",None,"42422964917","PROSPECT","84004434","SKIMS Medical College & Hospital"),
 ("57865541931","1217017230",30000,30000,"USD","2026-03-11","2026-12-30","2026-09-23","2026-05-25",False,"2026-03-11",None,"42426101057","PROSPECT","87318803","ACS Medical College - New Deal"),
 ("57909130079","1217017230",30000,30000,"USD","2026-03-11","2027-02-20","2026-10-06","2026-10-06",False,"2026-03-17",None,"42436777492","PROSPECT","87318803","Aarupadai Veedu Medical College, Pondicherry - New Deal"),
 ("61025036046","1217017229",0,0,"INR","2026-06-11","2027-03-31","2026-10-20","2026-06-19",False,"2026-06-11",None,"42484802081","PROSPECT","87318803","Madha Medical College and Research Institute - New Deal"),
 ("66544515444","1217017229",600000,6228.6,"INR","2026-10-22","2027-06-30","2026-10-22","2026-10-22",False,"2026-10-22",None,"42486038583","PROSPECT","87318803","Velammal Medical College Hospital - New Deal"),
 ("53720619425","closedlost",0,0,"USD","2026-01-13","2026-03-10","2026-07-17","2026-05-15",True,"2026-03-10","2026-03-10","42520985455",None,"84004493","Pilot Program / Georgian College / Label Generation"),
 ("62853412644","1217017229",0,0,"INR","2026-07-21","2026-12-31","2026-10-13","2026-10-13",False,"2026-07-21",None,"42521057172","PROSPECT","87318803","Sri Lakshmi Narayana Institute of Medical Sciences - New Deal"),
 ("56896693933","closedwon",18000,18000,"USD","2026-02-24","2026-07-14","2026-10-02","2026-07-07",True,"2026-07-14",None,"42791143079",None,"84004493","Dean College - New Deal / Anatomy 360 & Digital Cadaver / 36 month subscription"),
 ("48172090196","closedlost",0,0,"USD","2025-11-04","2026-01-29","2026-10-23","2026-05-15",True,"2026-03-10","2025-11-04","42826182469",None,"84004493","Lincoln Memorial University (LMU) - New Deal"),
 ("46792559246","closedlost",23081,23081,"USD","2025-10-24","2026-04-21","2026-07-16","2026-03-16",True,"2026-04-21",None,"42826694802","PROSPECT","84004434","Culko Chandigarh University"),
 ("53697222183","closedlost",0,0,"USD","2026-01-13","2026-04-07","2026-07-16","2026-04-06",True,"2026-05-12",None,"43011026024","PROSPECT","84004493","University of Buckingham - New Deal"),
 ("46807130900","closedlost",230295,230295,"USD","2024-02-12","2026-04-21","2026-07-18","2026-03-19",True,"2026-04-21",None,"43011026177","PROSPECT","84004434","AIIMS Jodhpur"),
 ("62115451054","presentationscheduled",0,0,"USD","2026-07-07","2026-10-31","2026-10-13","2026-10-13",False,"2026-07-07","2026-07-07","43037869697","PROSPECT","84004493","Royal National Orthopaedic Hospital / Dr.Boyne BELLEW- New Deal"),
 ("59680085122","presentationscheduled",9000,9000,"USD","2026-04-27","2027-06-02","2026-10-27","2026-10-27",False,"2026-04-27","2026-04-27","43087532968","PROSPECT","87318764","(License) UC San Diego Medical"),
 ("53696123285","1217017230",4000,4000,"USD","2026-01-13","2026-10-31","2026-09-29","2026-09-29",False,"2026-01-13",None,"43139871593",None,"84004493","Royal London Hospital @ Barts Health NHS Trust - MasterClass / New Deal"),
 ("60177857961","closedwon",0,0,"USD","2026-05-12","2026-08-10","2026-08-10","2026-05-15",True,"2026-08-10",None,"43296406964","PROSPECT","84004493","Consultancy - Jailenne I. Quinones-Rodriguez / Sam Houston State University College of Osteopathic Medicine / Content Review Anatomy 360"),
 ("60182314582","1217017229",0,0,"USD","2026-05-12","2026-08-31","2026-07-16","2026-05-15",False,"2026-05-12",None,"43313998462","PROSPECT","84004493","Consultancy / Sinead Impey_School of Nursing & Midwifery at Trinity College Dublin / Content Review Nursing Clinical Skills"),
 ("46876791481","closedlost",230295,230295,"USD","2025-10-24","2025-01-17","2026-10-20",None,True,"2025-10-24",None,"43460540033","PROSPECT","84004434","KMC Manipal Medical College"),
 ("46807285426","closedlost",17000,17000,"USD","2025-10-24","2025-11-26","2026-07-16",None,True,"2025-11-26",None,"43546695186","PROSPECT","84004434","JIPMER"),
 ("59610029666","appointmentscheduled",5100,5100,"USD","2026-04-27","2026-10-31","2026-10-21","2026-10-06",False,"2026-04-27","2026-04-27","43547108023","PROSPECT","84004493","(AC) Nnamdi Azikiwe University / New Deal"),
 ("46799760157","appointmentscheduled",1136363,1136363,"USD","2025-10-24","2026-09-30","2026-10-13","2026-10-13",False,"2026-06-23",None,"43547119393",None,"84004434","Rajiv Gandhi University of Health Sciences (RGUHS)"),
 ("46803878191","closedlost",0,0,"USD","2024-02-12","2025-01-17","2026-07-18",None,True,"2025-10-24",None,"43593492877","PROSPECT","84004434","Subharti Medical College"),
 ("57851499068","closedlost",0,0,"USD","2026-03-10","2026-09-23","2026-09-23","2026-08-12",True,"2026-09-23","2026-03-10","43593493081",None,"84004493","Consultancy / Yossi Buskila _ Western Sydney University / Content Review Digital Cadaver"),
 ("46826397807","closedlost",23081,23081,"USD","2025-10-24","2026-04-21","2026-07-17","2026-04-20",True,"2026-04-21",None,"43698062718","PROSPECT","84004434","Ramaiah Medical College"),
 ("61390460829","closedlost",0,0,"USD","2026-06-23","2026-09-01","2026-09-28","2026-09-08",True,"2026-09-01","2026-06-23","46327334939","PROSPECT","84004493","Uniformed Services University of the Health Sciences (USU) - New Deal"),
 ("55031048968","presentationscheduled",None,None,"USD","2026-01-29","2026-12-16","2026-10-27","2026-10-27",False,"2026-07-28","2026-07-28","49963761434","PROSPECT","87318764","Univ Miami Bascom Eye Institute"),
 ("59729814549","presentationscheduled",0,0,"USD","2026-04-28","2026-12-16","2026-10-27","2026-10-27",False,"2026-07-28","2026-07-28","49967478154",None,"87318764","(AC) University of Miami"),
 ("65204227685","1217017229",0,0,"INR","2026-09-23","2027-09-30","2026-10-02","2026-09-22",False,"2026-09-23",None,"50329114229","PROSPECT","87318803","Kanyakumari Medical Mission (KMMC) - New Deal"),
 ("57442751146","closedwon",16000,16000,"USD","2026-03-03","2026-02-20","2026-10-22","2026-03-03",True,"2026-03-03",None,"50331657141","PROSPECT","87318803","Nandha Medical College & Hospital"),
 ("66642461746","1217017229",900000,9342.9,"INR","2026-10-23","2027-03-31","2026-10-26","2026-10-23",False,"2026-10-23",None,"50480011438","PROSPECT","87318803","Sree Mookambika Institute of Medical Sciences (SMIMS) - New Deal"),
 ("60273525591","1217017230",900000,9342.9,"INR","2026-05-15","2026-10-15","2026-10-12","2026-10-12",False,"2026-05-15",None,"50534626828","PROSPECT","87318803","Sri Muthukumaran Medical College Hospital & Research Institute - New Deal"),
 ("60877633452","1217017230",600000,6228.6,"INR","2026-06-05","2026-12-30","2026-10-02","2026-09-22",False,"2026-09-23","2026-09-23","50557379177","PROSPECT","87318803","Sri Lalithambigai Medical College & Hospital (SLMCH) - New Deal"),
 ("61027186293","1217017229",0,0,"INR","2026-06-11","2026-12-31","2026-10-20","2026-10-12",False,"2026-06-11",None,"50562390343","PROSPECT","87318803","Tagore Medical College & Hospital - New Deal"),
 ("62625005073","presentationscheduled",0,0,"USD","2026-07-14","2026-11-30","2026-10-14","2026-10-14",False,"2026-07-14","2026-07-14","50760688985","PROSPECT","84004493","Winston-Salem State University - New Deal"),
 ("57397572682","closedwon",16000,16000,"USD","2026-03-03","2026-02-20","2026-07-09","2026-03-03",True,"2026-03-03",None,"52448552523","Customer","84004434","Dharan Educational Charitable Trust - 100 users, 10MQ3S"),
 ("57855774766","closedwon",None,None,"USD","2026-03-10","2026-04-27","2026-07-16","2026-04-27",True,"2026-04-27","2026-04-14","52729192548",None,"87318764","(Pilot) UC San Diego Health"),
 ("57856173118","closedlost",30000,30000,"USD","2026-03-11","2026-09-23","2026-09-23","2026-07-15",True,"2026-09-23",None,"52817616555","PROSPECT","87318803","Plus Max Medical College and Hospital - New Deal"),
 ("59644501846","1217017230",9000,9000,"USD","2026-04-27","2026-12-16","2026-10-27","2026-10-27",False,"2026-07-14","2026-04-27","52918739114",None,"87318764","(License) Klamath Community College"),
 ("60873508132","1217017229",0,0,"INR","2026-06-05","2027-06-30","2026-10-22","2026-10-22",False,"2026-06-05",None,"52951887696","PROSPECT","87318803","Annapoorna Medical College & Hospital - New Deal"),
 ("60877638803","1217017230",900000,9342.9,"INR","2026-06-05","2026-12-31","2026-10-13","2026-10-13",False,"2026-07-21","2026-06-09","52980566063","PROSPECT","87318803","Sri Venkateswaraa Medical College - New Deal"),
 ("60032326939","closedlost",10000,10000,"USD","2026-05-05","2026-09-23","2026-09-23","2026-05-12",True,"2026-09-23",None,"52982671901","PROSPECT","87318803","Takshashila Medical College - New Deal"),
 ("58025515136","1217017230",6500,6500,"USD","2026-03-18","2026-11-20","2026-09-23","2026-06-22",False,"2026-03-18",None,"53052590974","PROSPECT","87318803","SNS College of Pharmacy and Health Sciences - New Deal"),
 ("58254625823","closedlost",0,0,"USD","2026-03-24","2026-04-27","2026-07-18","2026-04-14",True,"2026-04-27",None,"53138708041","PROSPECT","87318764","MQL-ERR Technology"),
 ("58567500715","closedlost",0,0,"USD","2026-03-31","2026-04-27","2026-07-10","2026-04-14",True,"2026-04-27",None,"53432420121",None,"87318764","MQL-Lone Star College"),
 ("60877638533","closedlost",0,0,"INR","2026-06-05","2026-06-05","2026-06-05",None,True,"2026-06-05",None,"53471631174","PROSPECT","87318803","Dhanalakshmi Srinivasan Medical College And Hospital - New Deal"),
 ("59158335603","closedlost",3000,3000,"USD","2026-04-14","2026-06-30","2026-07-08","2026-06-30",True,"2026-06-30",None,"53948642466","PROSPECT","84004434","Kalka Dental College - New Deal"),
 ("60271624241","presentationscheduled",25000,25000,"USD","2026-04-21","2026-10-30","2026-08-18","2026-05-07",False,"2026-05-15","2026-05-15","53958861476",None,"84004434","Graphic Era University - New Deal"),
 ("59369552254","1217017229",5000,5000,"USD","2026-04-21","2026-10-30","2026-08-18",None,False,"2026-04-21",None,"54175747653","PROSPECT","84004434","Government Medical College Jalna - New Deal"),
 ("59729814536","1217017229",0,0,"USD","2026-04-28","2026-12-16","2026-10-27","2026-10-27",False,"2026-04-28","2026-04-28","54243254356","PROSPECT","87318764","(AC) Noorda College of Osteopathic Medicine"),
 ("60877637263","1217017229",900000,9342.9,"INR","2026-06-05","2026-12-31","2026-10-20","2026-10-13",False,"2026-06-05",None,"54512194304","PROSPECT","87318803","Sri Manakula Vinayagar Medical College and Hospital - New Deal"),
 ("60177672318","presentationscheduled",0,0,"USD","2026-05-12","2027-03-05","2026-09-21",None,False,"2026-05-12","2026-05-12","54596806029","PROSPECT","84004493","NHS Supply Chain - Project_ 1700 NHS - Medical Simulations Devices and Technologies - Contract Start 10 May 2027"),
 ("60873510917","1217017229",300000,3114.3,"INR","2026-06-05","2026-12-31","2026-10-20",None,False,"2026-06-05",None,"54711431553","PROSPECT","87318803","St. Joseph University - New Deal"),
 ("60019887668","appointmentscheduled",90000,90000,"USD","2026-05-05","2026-12-16","2026-10-27","2026-10-27",False,"2026-05-05",None,"54728038532",None,"87318764","Mopec - Custom VR Product SIM"),
 ("57856172953","closedlost",28000,28000,"USD","2026-03-11","2026-06-05","2026-07-16","2026-02-25",True,"2026-06-05",None,"54751372574","PROSPECT","87318803","Mahatma Gandhi Medical College & Research Institute, Pondicherry - New Deal"),
 ("60873511034","1217017229",300000,3114.3,"INR","2026-06-05","2027-03-31","2026-10-20","2026-05-12",False,"2026-06-05",None,"54896144789","PROSPECT","87318803","Mahendra College of Nursing"),
 ("60182264878","appointmentscheduled",0,0,"USD","2026-05-12","2026-12-16","2026-10-27","2026-10-27",False,"2026-06-02",None,"54900624715",None,"87318764","(Pilot HAPS) UTSA - The University of Texas at San Antonio"),
 ("56460910657","closedlost",500000,5253.5,"INR","2026-02-17","2026-09-01","2026-09-01","2026-08-19",True,"2026-09-01","2026-02-17","54954901567","PROSPECT","84004434","National Institute of Medical Sciences & Research (NIMS) $5000"),
 ("60177653638","1217017229",70000,70000,"USD","2026-05-11","2026-08-31","2026-07-07","2026-06-30",False,"2026-05-11",None,"54954902951","PROSPECT","84004434","Dr. Abdulvahid Attar"),
 ("49594517360","closedlost",5000,5000,"USD","2025-11-18","2026-09-01","2026-09-01","2026-07-28",True,"2026-09-01","2026-05-19","54965254721","PROSPECT","84004434","Balvir Singh Tomar Institute of Medical Sciences & Research (NIMS University)"),
 ("60877632370","1217017230",300000,3114.3,"INR","2026-06-05","2026-11-20","2026-10-02","2026-08-24",False,"2026-09-23","2026-09-23","55019337885","PROSPECT","87318803","Nehru College Of Nursing - India - New Deal"),
 ("60873509972","closedlost",300000,3147.9,"INR","2026-06-05","2026-06-11","2026-06-11","2026-06-11",True,"2026-06-11","2026-06-09","55070419970","PROSPECT","87318803","Jenneys College of Nursing - New Deal"),
 ("60644345390","appointmentscheduled",0,0,"USD","2026-05-26","2026-12-16","2026-10-27","2026-10-27",False,"2026-05-26",None,"55294521201","PROSPECT","87318764","(Pilot HAPS) Bluegrass Community & Technical College"),
 ("60786237714","appointmentscheduled",0,0,"USD","2026-06-02","2026-12-16","2026-10-27","2026-10-27",False,"2026-06-02",None,"55338031513","PROSPECT","87318764","(Pilot HAPS) -University of Mississippi - Ole Miss"),
 ("60785744196","appointmentscheduled",0,0,"USD","2026-06-02","2026-12-16","2026-10-27","2026-10-27",False,"2026-06-02",None,"55338032276","PROSPECT","87318764","(Pilot HAPS) - University of Detroit Mercy"),
 ("60650552119","presentationscheduled",0,0,"USD","2026-05-26","2026-12-16","2026-10-27","2026-10-27",False,"2026-07-28","2026-07-28","55338032305","PROSPECT","87318764","(Pilot HAPS) Vincennes University"),
 ("60855218252","1217017230",900000,9342.9,"INR","2026-06-04","2026-11-20","2026-10-12","2026-10-12",False,"2026-06-04",None,"55404680769","PROSPECT","87318803","BSD College of Allied Health Science - New Deal"),
 ("60873509478","1217017229",300000,3114.3,"INR","2026-06-05","2026-12-31","2026-10-20","2026-06-25",False,"2026-06-05",None,"55405175977","PROSPECT","87318803","KG College of Health Science - New Deal"),
 ("60873509492","1217017229",300000,3114.3,"INR","2026-06-05","2026-12-31","2026-10-20","2026-06-17",False,"2026-06-05",None,"55405237386","PROSPECT","87318803","PPG College of Nursing - New Deal"),
 ("60854776364","1217017230",900000,9342.9,"INR","2026-06-04","2026-12-30","2026-10-12","2026-10-12",False,"2026-06-04",None,"55405259820","PROSPECT","87318803","Karpagam Faculty of Medical Science & Research - New Deal"),
 ("60959919902","1217017230",300000,3114.3,"INR","2026-06-08","2026-12-30","2026-10-02","2026-06-09",False,"2026-06-09","2026-06-09","55509996944","PROSPECT","87318803","JJ College of Nursing - New Deal"),
 ("63835058523","1217017230",300000,3114.3,"INR","2026-08-14","2026-12-31","2026-10-02","2026-08-14",False,"2026-08-14","2026-08-14","55512334387","PROSPECT","87318803","GK College of Nursing - New Deal"),
 ("60975080564","1217017230",300000,3114.3,"INR","2026-06-09","2026-12-31","2026-10-20","2026-06-09",False,"2026-06-09","2026-06-09","55515948091","PROSPECT","87318803","KMC Nursing College - New Deal"),
 ("61129524047","1217017229",0,0,"INR","2026-06-17","2026-12-31","2026-10-20","2026-06-17",False,"2026-06-17",None,"55654661973","PROSPECT","87318803","PPG Institute of Allied Health Sciences - New Deal"),
 ("61129459634","1217017229",0,0,"INR","2026-06-17","2026-12-31","2026-10-20","2026-06-19",False,"2026-06-17",None,"55655965566","PROSPECT","87318803","United College of Nursing - New Deal"),
 ("61129645988","1217017229",0,0,"INR","2026-06-17","2027-03-31","2026-10-20","2026-06-17",False,"2026-06-17",None,"55656027935","PROSPECT","87318803","Sasurie College of Nursing & Research - New Deal"),
 ("61176293467","1217017229",0,0,"INR","2026-06-17","2026-12-31","2026-10-20","2026-06-17",False,"2026-06-17",None,"55656089868","PROSPECT","87318803","PPG College Of Occupational Therapy - New Deal"),
 ("61390483981","1217017229",300000,3114.3,"INR","2026-06-22","2026-11-30","2026-10-20","2026-06-22",False,"2026-06-22",None,"55866408554","PROSPECT","87318803","Sowmiya Healthcare & Hospital - New Deal"),
 ("61764791587","appointmentscheduled",300000,3114.3,"INR","2026-06-30","2026-12-31","2026-10-20","2026-07-02",False,"2026-07-21",None,"56137307659","PROSPECT","87318803","Nightingale Institute of Nursing Education - New Deal"),
 ("61763929428","1217017229",0,0,"INR","2026-06-30","2026-12-31","2026-10-20",None,False,"2026-06-30",None,"56142665878","PROSPECT","87318803","Dr. Muthus College of Allied Health Science - New Deal"),
 ("48791355527","closedlost",0,0,"USD","2025-11-11","2026-01-09","2026-07-16","2026-03-10",True,"2026-03-10",None,"56463175501","PROSPECT","84004493","University of San Francisco - New Deal"),
 ("60177668384","closedwon",0,0,"USD","2026-05-12","2026-06-30","2026-07-15","2026-06-03",True,"2026-06-30","2026-05-12","56585946652","PROSPECT","87318764","(Pilot) University of California Irvine School of Medicine"),
 ("61763930416","closedlost",9000,9000,"USD","2026-06-30","2026-07-14","2026-07-15",None,True,"2026-07-14","2026-06-30","56585946652","PROSPECT","87318764","(License) University of California Irvine School of Medicine"),
 ("63837038150","1217017229",300000,3114.3,"INR","2026-08-14","2026-12-31","2026-10-02","2026-08-13",False,"2026-08-14",None,"57446353027","PROSPECT","87318803","Indra Ganesan College of Nursing - New Deal"),
 ("63701350283","appointmentscheduled",25000,25000,"USD","2026-08-11","2026-12-16","2026-10-27","2026-10-27",False,"2026-08-11",None,"58511365155","PROSPECT","87318764","Wellstar Hospital Network"),
 ("65204227706","1217017229",1000000,10381,"INR","2026-09-23","2026-12-30","2026-10-20","2026-09-22",False,"2026-09-23",None,"59478718549","PROSPECT","87318803","JR Medical College & Hospital - New Deal"),
]

# --- B2B companies: new (7d) + 3A (missing owner and/or name) -------------
# tuple: (id, name, createdate, owner, source, created_by, type)
CO_RAW = [
 # --- new B2B companies (createdate within window) ---
 ("59635093832","Royal College of Physicians and Surgeons of Glasgow","2026-10-20","84004493",None,"83228721",None),
 ("59532633402","College of Coastal Georgia","2026-10-20","87318764","OFFLINE",None,None),
 ("59624974106","Keele University School of Medicine","2026-10-20","84004493",None,"83228721",None),
 ("59494439568","CCAC Community College of Allegheny County","2026-10-20","87318764","OFFLINE",None,None),
 ("59622341915","Brunel University Medical School","2026-10-20","84004493",None,"83228721",None),
 ("59508230527","Ventura County Community College District","2026-10-20","87318764","OFFLINE",None,None),
 ("59505142354","Amarillo College","2026-10-20","87318764","OFFLINE",None,None),
 ("59541604468","Bergen Community College","2026-10-20","87318764","OFFLINE",None,None),
 ("59622806532","Norwich Medical School, University of East Anglia","2026-10-20","84004493",None,"83228721",None),
 ("59511016225","College of the Canyons","2026-10-20","87318764","OFFLINE",None,None),
 ("59541161380","KIDWAI MEMORIAL INSTITUTE OF ONCOLOGY","2026-10-20","87318803","PAID_SOCIAL",None,None),
 ("59506238014","Royal College of Surgeons in Ireland (RCSI)","2026-10-20","84004493",None,"83228721",None),
 ("59619012198","Royal College of Surgeons of Edinburgh","2026-10-20","84004493",None,"83228721",None),
 ("59639236763","University of Leeds School of Medicine","2026-10-20","84004493",None,"83228721",None),
 ("59534046296","Three Counties Medical School, University of Worcester","2026-10-20","84004493",None,"83228721",None),
 ("59509165891","UNC Charlotte","2026-10-20","87318764","OFFLINE",None,None),
 ("59635248687","University of Sheffield School of Medicine","2026-10-20","84004493",None,"83228721",None),
 ("59502619519","Aston University Medical School","2026-10-20","84004493",None,"83228721",None),
 ("59636642326","Edge Hill University Medical School","2026-10-20","84004493",None,"83228721",None),
 ("59629674022","St Mary's University, Twickenham School of Medicine","2026-10-20","84004493",None,"83228721",None),
 ("59635403446","Royal College of Surgeons of England","2026-10-20","84004493",None,"83228721",None),
 ("59615296926","University of Leicester Medical School","2026-10-20","84004493",None,"83228721",None),
 ("59622651493","University of St Andrews School of Medicine","2026-10-20","84004493",None,"83228721",None),
 ("59639856167","University of Birmingham School of Medicine","2026-10-20","84004493",None,"83228721",None),
 ("59534500699","Chikkamagaluru Institute Of Medical Sciences","2026-10-20","87318803","PAID_SOCIAL",None,None),
 ("60004655223",None,"2026-10-26","87318803","PAID_SOCIAL",None,None),
 ("59637843223","University of Cambridge School of Clinical Medicine","2026-10-20","84004493",None,"83228721",None),
 ("59889374470",None,"2026-10-26",None,"PAID_SEARCH",None,None),
 ("59956779872",None,"2026-10-26",None,"DIRECT_TRAFFIC",None,None),
 ("59937053844","Banyan Air Service","2026-10-26","87318764","DIRECT_TRAFFIC","83228721","PROSPECT"),
 ("59537742748","Western Carolina University","2026-10-20","87318764","OFFLINE",None,None),
 ("59619476748","University of Southampton School of Medicine","2026-10-20","84004493",None,"83228721",None),
 # --- 3A: B2B companies missing OWNER (any age) ---
 ("57336271793","Universidad María Auxiliadora","2026-08-11",None,"OFFLINE",None,None),
 ("54895276622",None,"2026-05-12",None,None,None,None),
 ("59083058077","Inbox.lv","2026-10-09",None,"OFFLINE",None,None),
 ("59088171801",None,"2026-10-09",None,"OFFLINE",None,None),
 ("58572253607","Wake Forest University School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58564085658","Loyola University Chicago Stritch School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58587120410","The University of Texas at Tyler School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58563156486","Boonshoft School of Medicine Wright State University","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58570512780","University of Pittsburgh School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58585726390","University of Chicago Division of the Biological Sciences, The Pritzker School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58573028102","Washington State University Elson S. Floyd College of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58580732833","Hackensack Meridian School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58580423040","University of Nevada, Reno School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58581197350","University of Vermont Larner College of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58567298462","LSU Health Sciences Center School of Medicine in New Orleans","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58557621164","Oregon Health & Science University School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58579339394","University of Texas Southwestern Medical School","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58573647969","Rutgers, Robert Wood Johnson Medical School","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58556085415","Ponce Health Sciences University School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58581699332","Rutgers New Jersey Medical School","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58585572160","University of Iowa Roy J. and Lucille A. Carver College of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58576434526","University of South Dakota Sanford School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58576898953","Jacobs School of Medicine and Biomedical Sciences at the University at Buffalo","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58565092113","University of North Carolina School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58569156950","CUNY School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("54897643786","Government T.D. Medical College, Vandanam, Alappuzha","2026-05-12",None,"OFFLINE","83228721",None),
 ("58587429907","NYU Grossman School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58577518690","Drexel University College of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58572872803","Louisiana State University School of Medicine in Shreveport","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58569738520","NYU Grossman Long Island School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("58565401385","Thomas F. Frist, Jr. College of Medicine at Belmont University","2026-09-23",None,None,"83228721","PROSPECT"),
 ("42485419414","Universidade do Estado Amazonas","2025-10-24",None,"OFFLINE","83228721","PROSPECT"),
 ("58566988686","San Juan Bautista School of Medicine","2026-09-23",None,None,"83228721","PROSPECT"),
 ("57119717454","University of Northern Iowa","2026-07-29",None,"DIRECT_TRAFFIC",None,None),
 ("58100429705",None,"2026-09-05",None,"DIRECT_TRAFFIC",None,None),
 # --- 3A: B2B companies missing NAME (have owner unless noted) ---
 ("58263803783",None,"2026-09-08","87318764","DIRECT_TRAFFIC",None,None),
 ("59298344824",None,"2026-10-15","87318764","DIRECT_TRAFFIC",None,None),
 ("59092636572",None,"2026-10-09","87318764","PAID_SEARCH",None,None),
 ("58422044225",None,"2026-09-15","87318764","PAID_SEARCH",None,None),
 ("58440013893",None,"2026-09-15","87318764","PAID_SEARCH",None,None),
 ("58320205473",None,"2026-09-11","87318764","PAID_SEARCH",None,None),
 ("58426002334",None,"2026-09-13","87318764","PAID_SEARCH",None,None),
 ("59189661825",None,"2026-10-13","87318764","DIRECT_TRAFFIC",None,None),
 ("57985973505",None,"2026-09-03","84004434","DIRECT_TRAFFIC",None,None),
 ("59093281845",None,"2026-10-09","87318764","PAID_SEARCH",None,None),
 ("56920388456",None,"2026-07-22","84004493","OFFLINE",None,None),
 ("58304479529",None,"2026-09-11","84004493","OFFLINE",None,None),
 ("59248799335",None,"2026-10-14","84004493","OFFLINE",None,None),
 ("59248861044",None,"2026-10-14","84004493","OFFLINE",None,None),
 ("55530389845",None,"2026-06-11","84004493","OFFLINE",None,None),
 ("54795758232",None,"2026-05-07","84004434",None,None,None),
 ("54771158653",None,"2026-05-05","84004434","OFFLINE",None,None),
 ("54795483828",None,"2026-05-07","84004434","OFFLINE",None,None),
 ("59033759123",None,"2026-10-06","84004493","OFFLINE",None,None),
 ("57804891556",None,"2026-08-27","84004493","OFFLINE",None,None),
 ("54770092303",None,"2026-05-05","84004434","OFFLINE",None,None),
 ("54771072044",None,"2026-05-05","84004434","OFFLINE",None,None),
 ("54770309738",None,"2026-05-05","84004434","OFFLINE",None,None),
 ("54770259026",None,"2026-05-05","84004434","OFFLINE",None,None),
 ("54770203225",None,"2026-05-05","84004434","OFFLINE",None,None),
 ("58472089775",None,"2026-09-18","87318764","PAID_SEARCH",None,None),
 ("58681944079",None,"2026-09-29","87318764","PAID_SEARCH",None,None),
]

# --- New B2B lead-stage contacts (createdate within window) --------------
# tuple: (id, name, hs_lead_status, owner, assoc_company, created, lastact)
NEWLEADS = [
 ("257421400671","Rebecca Pratt","Marketing Engaged","84004493","53957354358","2026-10-25","2026-10-25"),
 ("257421461579","Rachel Koshi","Marketing Engaged","84004493","56585562943","2026-10-25","2026-10-25"),
 ("257816693289","Fateme Pkz",None,None,"59889374470","2026-10-26",None),
 ("257883817526","Mahdi Abduallah",None,None,"59956779872","2026-10-26",None),
 ("257878549300","Janet Cope","Marketing Engaged","87318764","42521055049","2026-10-26","2026-10-26"),
 ("257888958067","Brittany Kirby","Attempted to Contact","84004493","42521055049","2026-10-26","2026-10-26"),
 ("257878613779","Catherine Quay","Marketing Engaged","84004493","42521055049","2026-10-26","2026-10-26"),
 ("258178375699","Van Whaley","Marketing Engaged","87318764","43369047726","2026-10-27",None),
 ("257947737439","Sigismund Pikul","Marketing Engaged","87318764","60009005655","2026-10-27",None),
 ("258120754102","Frances Fowler","Marketing Engaged","84004493","53883948825","2026-10-27",None),
 ("258120694435","Beth Budden","Marketing Engaged","87318764","59537742748","2026-10-27",None),
 ("257947737655","Gerald Brasington","Marketing Engaged","84004493","53883948825","2026-10-27",None),
 ("258120645777","Carmen Carrion","Marketing Engaged","87318764","60083723339","2026-10-27",None),
 ("257947737204","Sandra Brown","Marketing Engaged","87318764","60003126368","2026-10-27",None),
 ("258120645972","Ivy Price","Marketing Engaged","87318764","60010554429","2026-10-27",None),
 ("257947858610","Anya Goldina","Marketing Engaged","87318764","60012494646","2026-10-27",None),
 ("258178375539","Kathy Burleson","Marketing Engaged","87318764","60010090300","2026-10-27",None),
 ("257947737418","Sarah Scott","Marketing Engaged","87318764","60020851978","2026-10-27",None),
 ("258120694297","David Brooks","Marketing Engaged","87318764","60092983812","2026-10-27",None),
]

# --- Non-B2B SQL contacts (for the recycler mislabel canary / info) -------
# tuple: (id, customer_type, owner, created, lastact)
NONB2B = [
 ("220892771134","B2C","84004493","2026-05-11","2026-05-12"),
 ("179349831079","Partner","83249199","2025-11-26","2026-09-21"),
 ("199706443298","B2C","83228721","2026-02-05","2026-03-17"),
 ("210549938807","Partner","84004493","2026-03-20","2026-03-20"),
 ("166080445844","Internal (Exclude)","83228721","2025-10-22","2026-03-24"),
 ("179405796019","Partner","84004434","2025-09-17","2026-02-05"),
 ("182341252918","Partner","84004493","2025-12-05","2026-04-14"),
 ("182339071503","Partner","84004493","2025-12-05","2026-03-31"),
 ("213235232795","Partner","83228721","2026-04-02","2026-04-29"),
 ("227063293494","Partner","83228721","2026-06-08","2026-06-24"),
 ("182352222123","Partner","84004493","2025-12-05",None),
 ("166285965427","Internal (Exclude)","83228721","2025-10-22","2025-10-29"),
 ("202595407730","Partner","83228721","2026-02-16","2026-03-17"),
 ("230043964807","B2C","83228721","2026-06-21","2026-06-24"),
 ("198847427871","B2C","83228721","2026-02-03",None),
 ("182351973207","Partner","84004493","2025-12-05","2026-03-31"),
 ("167462931237","Partner","83228721","2025-10-26","2026-03-06"),
 ("210181513736","Partner","87318764","2026-03-19","2026-04-03"),
 ("182083264129","Internal (Exclude)","83228721","2025-12-04","2026-02-12"),
 ("222858645896","B2C","83228721","2026-05-20","2026-06-15"),
 ("221722638682","B2C","83228721","2026-05-24","2026-06-15"),
 ("182358309784","Partner","84004493","2025-12-05","2025-12-05"),
 ("202927634838","Partner","84004434","2026-02-16","2026-09-15"),
 ("226260809816","B2C","83228721","2026-06-04","2026-06-15"),
 ("166574753558","Internal (Exclude)","83228721","2025-10-23","2025-10-24"),
 ("165809747874","Internal (Exclude)","83228721","2025-10-21","2025-10-23"),
 ("179349831082","Partner","83249199","2025-11-26","2026-03-04"),
 ("191353025655","Internal (Exclude)","83228721","2026-02-04","2026-03-17"),
 ("210108280721","Partner","84004493","2026-03-18","2026-10-19"),
 ("218915632926","B2C","84004493","2026-04-30","2026-10-19"),
 ("205159095201","Partner","83249199","2026-02-25","2026-10-05"),
 ("229708668029","Partner-B2B","84004493","2026-06-19","2026-09-29"),
 ("226613145656","Partner-B2B","84004493","2026-06-05",None),
 ("222917643412","Partner-B2B","84004493","2026-05-21","2026-07-27"),
 ("212467910805",None,"83228721","2026-06-12","2026-06-16"),
 ("182079459225","Internal (Exclude)","83228721","2025-12-04","2026-06-03"),
]

# Company counts (server-side aggregates for the guard + headline).
COMPANY_COUNTS = {"total_b2b": 2060, "total_all": 2186, "new_7d": 32,
                  "missing_owner": 37}
# Funnel counts for B2B contacts (server-side GROUP BY at pull time).
FUNNEL_COUNTS_B2B = {"subscriber": 7, "lead": 3061, "marketingqualifiedlead": 67,
                     "salesqualifiedlead": 394, "opportunity": 198, "customer": 21,
                     "other": 10}


def g(p, *names):
    for n in names:
        v = p.get(n)
        if v not in (None, ""):
            return v
    return None


def contact_from_props(p):
    return {
        "id": str(p["hs_object_id"]),
        "name": g(p, "hs_full_name_or_email", "email") or str(p["hs_object_id"]),
        "lifecyclestage": p.get("lifecyclestage"),
        "hs_lead_status": p.get("hs_lead_status"),
        "createdate": g(p, "createdate_iso", "createdate"),
        "notes_last_updated": g(p, "notes_last_updated_iso", "notes_last_updated"),
        "associatedcompanyid": p.get("associatedcompanyid"),
        "recycled_date": g(p, "recycled_date_iso", "recycled_date"),
        "unqualified_reason": p.get("unqualified_reason"),
        "num_associated_deals": p.get("num_associated_deals"),
        "date_entered_sql": g(p, "hs_v2_date_entered_salesqualifiedlead_iso",
                              "hs_v2_date_entered_salesqualifiedlead"),
        "date_entered_mql": g(p, "hs_v2_date_entered_marketingqualifiedlead_iso",
                              "hs_v2_date_entered_marketingqualifiedlead"),
        "owner_id": p.get("hubspot_owner_id"),
        "b2b_contact_lead_score_combined": p.get("b2b_contact_lead_score_combined"),
    }


def main():
    contacts = []
    seen = set()
    for f in CONTACT_FILES:
        data = json.load(open(f))
        for r in data["results"]:
            p = json.loads(r["content"])["properties"]
            rec = contact_from_props(p)
            if rec["id"] in seen:
                continue
            seen.add(rec["id"])
            contacts.append(rec)
    # new lead-stage contacts
    for (cid, name, status, owner, co, created, la) in NEWLEADS:
        if cid in seen:
            continue
        seen.add(cid)
        contacts.append({
            "id": cid, "name": name, "lifecyclestage": "lead",
            "hs_lead_status": status, "createdate": created,
            "notes_last_updated": la, "associatedcompanyid": co,
            "recycled_date": None, "unqualified_reason": None,
            "num_associated_deals": None, "date_entered_sql": None,
            "date_entered_mql": None, "owner_id": owner,
            "b2b_contact_lead_score_combined": None,
        })

    # deals
    deals = []
    for t in DEALS_RAW:
        (did, stage, amount, usd, curr, created, close, lastmod, lastact,
         closed, ec, trial, coid, ctype, owner, dealname) = t
        deals.append({
            "id": did, "dealname": dealname, "dealstage": stage, "pipeline": "default",
            "amount": amount, "amount_in_home_currency": usd, "deal_currency_code": curr,
            "createdate": created, "closedate": close, "hs_lastmodifieddate": lastmod,
            "notes_last_updated": lastact, "hs_is_closed": closed,
            "hs_v2_date_entered_current_stage": ec, "date_entered_trial": trial,
            "company_id": coid, "company_name": None, "company_type": ctype,
            "owner_id": owner,
        })
    # fill company_name from CO_RAW where available (else leave None)
    co_names = {c[0]: c[1] for c in CO_RAW}
    # deal company names come from the deal pull; keep per-deal name if we have it
    deal_co_names = {
        "41738243363": "Western University of Health Sciences",
    }
    for d in deals:
        d["company_name"] = deal_co_names.get(d["company_id"]) or co_names.get(d["company_id"])

    # --- validate deal transcription against server aggregates ---
    cnt, usdsum = {}, {}
    for d in deals:
        s = d["dealstage"]
        cnt[s] = cnt.get(s, 0) + 1
        if d["amount_in_home_currency"]:
            usdsum[s] = round(usdsum.get(s, 0.0) + d["amount_in_home_currency"], 1)
    assert len(deals) == 87, f"deal count {len(deals)}"
    assert cnt == DEAL_STAGE_COUNTS, f"stage counts mismatch: {cnt}"
    for s, exp in DEAL_STAGE_USD.items():
        got = round(usdsum.get(s, 0.0), 1)
        assert abs(got - exp) < 1.0, f"USD mismatch {s}: {got} != {exp}"

    # companies (dedup/merge)
    companies = {}
    for (cid, name, created, owner, source, created_by, ctype) in CO_RAW:
        e = companies.setdefault(cid, {"id": cid, "name": None, "createdate": created,
                                       "owner_id": None, "original_source": None,
                                       "created_by_id": None, "type": None})
        if name and not e["name"]:
            e["name"] = name
        if owner and not e["owner_id"]:
            e["owner_id"] = owner
        if source and not e["original_source"]:
            e["original_source"] = source
        if created_by and not e["created_by_id"]:
            e["created_by_id"] = created_by
        if ctype and not e["type"]:
            e["type"] = ctype
    for e in companies.values():
        e["created_by"] = OWNERS.get(str(e.get("created_by_id")), e.get("created_by_id"))

    missing_name = sum(1 for e in companies.values() if not e["name"])

    # non-B2B SQL
    sql_non_b2b = [{"id": cid, "customer_type": ct, "owner_id": owner,
                    "createdate": created, "notes_last_updated": la}
                   for (cid, ct, owner, created, la) in NONB2B]

    raw = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "run_date": RUN_DATE,
        "window_days": WINDOW_DAYS,
        "idle_days": [30, 60],
        "lead_status_map": LEAD_STATUS_MAP,
        "owners": OWNERS,
        "funnel_counts_b2b": FUNNEL_COUNTS_B2B,
        "data_notes": [
            "Record-level B2B contacts cover all non-lead lifecycle stages plus "
            "lead-stage contacts that are new (<=7d) or carry a non-default lead "
            "status; default-status lead-stage contacts (New Lead / Marketing "
            "Engaged / blank) contribute to funnel_counts_b2b only, so they are "
            "not re-scanned for 3C/3D (where they are valid by definition).",
            "Lead-status label 'New' in VALID_STATUS_BY_LIFECYCLE is reconciled to "
            "the portal's actual option label 'New Lead' (value NEW) via an alias; "
            "without it the guard would flag every New-Lead contact as a mismatch.",
            "Deal B2B scope = COMPANY.customer_type__cloned_ = 'B2B' (87 of 113 "
            "deals). Deal counts/USD validated against server-side stage aggregates.",
        ],
        "contacts_b2b": contacts,
        "sql_non_b2b": sql_non_b2b,
        "companies_b2b": list(companies.values()),
        "companies_b2b_counts": {**COMPANY_COUNTS, "missing_name": missing_name},
        "deals_b2b": deals,
    }

    with open("raw_input.json", "w") as f:
        json.dump(raw, f, indent=1)

    print(f"contacts_b2b: {len(contacts)}")
    print(f"deals_b2b: {len(deals)} (validated vs aggregates)")
    print(f"companies_b2b: {len(companies)} (missing_name={missing_name})")
    print(f"sql_non_b2b: {len(sql_non_b2b)}")
    print("wrote raw_input.json")


if __name__ == "__main__":
    main()
