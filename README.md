# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data plus every computed metric.
- `reports/YYYY-MM-DD.md` — a VP-readable summary, in reading order:
  alerts → current pipeline → what moved → new top of funnel by owner →
  lifecycle funnel → miscategorised → SQL review → currency note →
  week-on-week.
- `reports/YYYY-MM-DD.docx` — the Word version, rendered from the markdown with
  `pandoc ... --reference-doc=templates/pipeline_report_template.docx`.

Each run loads the most recent previous snapshot and shows week-on-week
movement for every comparable metric.

## Scope & rules (non-negotiable)

- **B2B only.** Contacts are filtered on `customer_type = B2B`; companies on
  `customer_type__cloned_ = B2B` (never `customer_type` on a company — it
  silently fails).
- **Two separate bases.** The lifecycle **funnel** is built from B2B
  **contacts** only (companies never contribute). **Pipeline / deal value** is
  built from **deals**, scoped to B2B via the associated company
  (`COMPANY.customer_type__cloned_ = B2B`) and valued at company level.
- **"New"** = created in the last `WINDOW_DAYS` (default 7), for both contacts
  and companies, on the same window.
- **USD only.** Deal value sums `amount_in_home_currency`; raw `amount` is never
  summed across currencies. Deals missing `amount_in_home_currency` are flagged,
  never back-filled.
- **Joins done here, not in the query.** Contact→company joins (orphan status,
  new-vs-existing account) are computed in `pipeline.py` from pulled scalar
  fields (`associatedcompanyid`, company `id` + `createdate`), because
  cross-object CONTACT→COMPANY filters silently return the full set in the MCP
  SQL layer.

## Editable constants (top of `pipeline.py`)

- `LIFECYCLE_LABELS` / `DEALSTAGE_LABELS` — internal value → label maps.
- `SALES_WORKING_STATUSES` — lead statuses meaning sales is actively working the
  record. In this portal: `Attempted to Contact`, `CONNECTED` ("Contacted").
- `MISCATEGORISED_LIFECYCLE_STAGES` — stages too early given a working status.
- `IDLE_THRESHOLD_DAYS = 30` (SQL gone-quiet), `QTB_STALE_DAYS = 14` (deal
  alert), `WINDOW_DAYS = 7`.

## Alerts (top of the report)

Orphan new leads (by owner) · new B2B accounts with no owner · Qualified-to-Buy
deals idle 14+ days · deals missing `amount_in_home_currency` · miscategorised
count (WoW) · funnel-inversion (SQL > MQL) · a data-quality flag for new company
records not tagged B2B.

## Running

Direct egress to `api.hubapi.com` is blocked in the run environment, so data is
pulled via the HubSpot MCP tools into `raw_input.json` and handed to the
computation code:

```bash
python3 pipeline.py --input raw_input.json
# then the Word version:
pandoc reports/$(date +%F).md -o reports/$(date +%F).docx \
  --reference-doc=templates/pipeline_report_template.docx
```

Running without `--input` is intentionally unsupported here (it would need live
egress). `raw_input.json` shape: `owners`, `contacts_b2b_total`,
`funnel_counts` (contacts only), `contacts_b2b` (new-window records),
`companies_b2b` (associated + new accounts, each with `customer_type_cloned`),
`deals` (B2B-scoped, with `amount_in_home_currency` + associated `company_id`),
`miscategorised`, `sql_review`.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` competitor-monitoring
routine. Scheduling is managed at the routine/platform level; this repo holds
the logic and the committed outputs.
