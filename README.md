# brahmarsive-pipeline

Weekly **B2B pipeline & MQL/recycler health** routine, sourced from HubSpot.
Runs on the same cadence as `brahmarsive-ci`.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data, computed metrics, and every
  worklist's record IDs (so each admin list is clickable/actionable).
- `reports/YYYY-MM-DD.md` — the report, Sections 1–4 (below).
- `reports/YYYY-MM-DD.docx` — the same report via
  `pandoc … --reference-doc=templates/pipeline_report_template.docx`.

Each run loads the most recent previous snapshot and shows week-on-week movement
for every headline number (deltas start once two same-schema snapshots exist).

## Report layout

- **Headline flags** — stage moves this week; % open deals idle 14+d; % SQLs
  untouched 30+d; miscategorised count; new accounts with no owner; open deals
  missing USD; recycler bounce-backs.
- **Section 1 — Current pipeline** (DEALS, B2B companies): open pipeline by
  stage (qty, USD, median idle, idle ≥14d), by owner (with concentration), and
  moved-vs-updated in the last 7 days.
- **Section 2 — New pipeline** (top of funnel): new companies by owner; new
  contacts on existing vs newly-created accounts; orphan new contacts; SQLs by
  owner (the true SQL total — contacts only).
- **Section 3 — Admin / hygiene worklist**: new accounts missing owner/name;
  open deals missing a USD value; miscategorised leads (working lead status but
  still Lead/MQL).
- **Section 4 — Recycler health**: does the 90-day stale-SQL recycler actually
  fire? Trigger match now (by customer type), the stale backlog it misses,
  recycled-this-week, and bounce-backs (recycled → back at SQL).

## Non-negotiable scoping / field rules

- **B2B only, everywhere.** Contacts: `customer_type = B2B`. Companies:
  `customer_type__cloned_ = B2B` (`customer_type` on a *company* silently fails).
- **Two bases, never mixed.** Every contact/SQL count comes from the CONTACT
  object; every pipeline value comes from DEALS scoped to B2B companies. The
  company object never feeds contact/SQL counts. (B2B contacts at SQL = 132;
  companies at SQL ≈ 322 — do not report the latter as "SQLs".)
- **Deal USD = `amount_in_home_currency`** (never sum raw `amount` across
  INR/USD). A deal with an empty `amount_in_home_currency` is flagged (3B), never
  back-filled from raw amount.
- **Joins in Python.** Contact→company cross-object filters silently fail in the
  MCP SQL layer, so each contact's `associatedcompanyid` (a reliable scalar) and
  each company's `id`/`createdate` are pulled separately and joined in
  `pipeline.py`.
- **Owners** are resolved to names from the HubSpot owners/users list; records
  with no owner show as **Unassigned** (kept visible — it's a signal).

## Data path

Direct egress to `api.hubapi.com` is blocked in the run environment, so data is
pulled via the **HubSpot MCP tools** and assembled into `raw_input.json`
(`build_raw_input.py` documents this week's assembly). `pipeline.py` then
consumes it:

```bash
python3 pipeline.py --input raw_input.json      # --input is REQUIRED
pandoc reports/$(date +%F).md -o reports/$(date +%F).docx \
  --reference-doc=templates/pipeline_report_template.docx
```

## Editable constants (top of `pipeline.py`)

`DEALSTAGE_LABELS`, `STAGE_ORDER`, `SALES_WORKING_STATUSES`
(`["Attempted to Contact", "CONNECTED"]` — note "Contacted" is stored internally
as `CONNECTED`), `RECYCLER_TRIGGER_STATUS = "Attempted to Contact"`,
`WINDOW_DAYS = 7`, `IDLE_BUCKETS = [30, 60]`, `QTB_STALE_DAYS = 14`,
`RECYCLE_DAYS = 90`.
