# brahmarsive-pipeline

Weekly **B2B** pipeline & MQL-health report, sourced from HubSpot.
**Process version: Oct 2026.**

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — machine snapshot: headline numbers, Q4 scorecard,
  Section-6 counts, every worklist's record ids, and each contact's lifecycle +
  lead-status (so next week's Section 5a can diff what changed).
- `reports/YYYY-MM-DD.md` — the full report (Q4 scorecard, headline flags,
  Sections 1–6).
- `reports/YYYY-MM-DD.docx` — the same report via pandoc + the reference template.
- `reports/YYYY-MM-DD_owner_<first>.md` — one Section-5 block per owner, ready to
  paste into an email.

Each run loads the most recent previous snapshot and shows week-on-week movement.

## Non-negotiable scoping rules (baked into `pipeline.py`)

- **B2B only, everywhere.** Contacts: `customer_type = B2B`. Companies:
  `customer_type__cloned_ = B2B` (`customer_type` silently fails on companies).
- **Two bases, never mixed.** Contact counts (funnel, SQL totals, miscategorised,
  new leads) come from the CONTACT object. Pipeline/deal value comes from DEALS
  (scoped `COMPANY.customer_type__cloned_ = 'B2B'`). Companies never feed contact
  counts — companies-at-SQL (~322) ≠ contacts-at-SQL (~132/348).
- **Deal USD = `amount_in_home_currency`** (USD). Empty/0 is flagged, never
  back-filled from raw `amount` (currencies are mixed INR/USD).
- **Lead-status values are mixed-format** (labels AND codes, e.g. `CONNECTED` =
  label "Contacted"; `NEW` = label "New Lead"). All rules are written in LABELS
  and matched against the value→label map pulled at runtime; the run RAISES if the
  map is missing a referenced label.
- Contact→company joins are done inside `pipeline.py` from each contact's scalar
  `associatedcompanyid` (MCP cross-object contact filters are unreliable).

## Data pull

Direct egress to `api.hubapi.com` is blocked from the run environment, so data is
pulled via the HubSpot MCP tools into `raw_input.json` (see that file's schema),
then:

```bash
python3 pipeline.py --input raw_input.json --date YYYY-MM-DD
pandoc reports/YYYY-MM-DD.md -o reports/YYYY-MM-DD.docx \
  --reference-doc=templates/pipeline_report_template.docx
```

`pipeline.py` requires `--input` (it never fetches live).

## Editable constants

All thresholds and label maps live at the top of `pipeline.py`
(`DEALSTAGE_LABELS`, `STAGE_ORDER`, `SALES_OWNERS`, `VALID_STATUS_BY_LIFECYCLE`,
`RECYCLE_DAYS`, `Q4_TARGETS`, claim-window dates, etc.).

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` competitor-monitoring
routine.
