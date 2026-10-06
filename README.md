# brahmarsive-pipeline

Weekly **B2B** pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled records + computed metrics + the
  worklist record IDs (so every admin list is reproducible/clickable).
- `reports/YYYY-MM-DD.md` — Sections 1–4 below, with headline flags on top.
- `reports/YYYY-MM-DD.docx` — the same report via
  `pandoc --reference-doc=templates/pipeline_report_template.docx`.

Each run loads the most recent prior snapshot and shows week-on-week movement
for every headline number (when the prior snapshot uses the same schema).

## Report structure

- **Headline flags** — deals moved this week; % open deals idle 14+d; % SQLs
  untouched 30+d; miscategorised count; new accounts with no owner; deals
  missing USD; recycler bounce-back count.
- **Section 1 — Current pipeline status:** open deals by stage (qty, USD, median
  idle, idle≥14d); open deals by owner (with concentration call-out); deals
  moved vs merely updated in the last 7 days.
- **Section 2 — New pipeline status:** new companies by owner; new contacts on
  existing vs new accounts; orphan contacts; SQLs by owner (the true,
  contact-only SQL total).
- **Section 3 — Admin / data-hygiene worklist:** new accounts to complete
  (missing owner/name); open deals missing a USD value; miscategorised leads.
- **Section 4 — Recycler health check:** trigger match now; the stale-SQL
  backlog the trigger misses; recycled this week; bounce-backs (recycled but
  back at SQL). Diagnostic only — it does not change the workflow.

## Scope & rules (non-negotiable)

- **B2B only, everywhere.** Contacts `customer_type = B2B`; companies
  `customer_type__cloned_ = B2B` (`customer_type` silently fails on companies —
  never use it there).
- **Two bases, never mixed.** Every contact/SQL count comes from the CONTACT
  object; every pipeline value comes from DEALS, scoped B2B via the associated
  company (`COMPANY.customer_type__cloned_ = 'B2B'`) and rolled up at company
  level. The company object never feeds contact/SQL counts. (A prior run
  reported ~325 "SQLs" by counting companies; B2B *contacts* at SQL are ~159.)
- **Deal USD** is `amount_in_home_currency` (never sum raw `amount` across
  INR/USD). An empty `amount_in_home_currency` is flagged to the admin worklist
  (3B), never back-filled from raw `amount`.
- **Owners** are resolved to names from the HubSpot owners directory; records
  with no owner show as "Unassigned" (kept visible — it's a signal).
- **"New"** = `createdate` within the last 7 days. **"Idle"** = days since
  `notes_last_updated`.

## Data pull

Direct egress to `api.hubapi.com` is blocked by the environment's network
policy, so records are pulled via the **HubSpot MCP tools** and assembled into
`raw_input.json` (schema v2: `owners`, `contacts_b2b`, `sql_all_types`,
`companies_b2b`, `associated_companies`, `deals_b2b`, `recycled_week`). The
pipeline then runs offline against that file:

```bash
python3 pipeline.py --input raw_input.json
pandoc reports/YYYY-MM-DD.md -o reports/YYYY-MM-DD.docx \
  --reference-doc=templates/pipeline_report_template.docx
```

`pipeline.py` refuses to run without `--input` and guards against the B2B
company silent-fail (0 / full-set) and against a company-inflated SQL count.

## Editable constants

Stage labels/order, `SALES_WORKING_STATUSES` (the working `hs_lead_status`
values that exist in this portal — note "Contacted" is stored internally as
`CONNECTED`), `RECYCLER_TRIGGER_STATUS`, and the `WINDOW_DAYS` / `IDLE_BUCKETS`
/ `QTB_STALE_DAYS` / `RECYCLE_DAYS` thresholds all live at the top of
`pipeline.py`.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` routine.
