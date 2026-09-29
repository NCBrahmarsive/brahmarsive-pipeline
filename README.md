# brahmarsive-pipeline

Weekly **B2B pipeline & MQL-health** report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — pulled records, every computed metric, all
  worklist record IDs, and the week-on-week (`wow`) values the next run diffs.
- `reports/YYYY-MM-DD.md` — the readable Sections 1–4 report.
- `reports/YYYY-MM-DD.docx` — the same report rendered via pandoc using
  `templates/pipeline_report_template.docx`.

Each run loads the most recent previous snapshot and shows week-on-week movement
for every headline number.

## Report structure

- **Headline flags** — deals moved this week, % open deals idle 14+d, % SQLs
  untouched 30+d, miscategorised count, new accounts with no owner, open deals
  missing USD, recycler bounce-backs. Followed by alerts.
- **Section 1 — Current pipeline status:** open pipeline by stage (qty, USD,
  median idle, idle ≥14d); open pipeline by owner (with concentration callout);
  deals moved vs merely updated in the last 7 days.
- **Section 2 — New pipeline status:** new companies by owner; new contacts on
  existing vs new accounts; orphan contacts (no company); SQLs by owner with
  30/60-day untouched counts (the true SQL total, contacts only).
- **Section 3 — Admin / data-hygiene worklist:** new accounts missing owner/name
  (3A); open deals missing a USD value (3B); miscategorised leads (3C). Every
  item's record IDs are in the snapshot JSON.
- **Section 4 — Recycler health check:** does the 90-day stale-SQL recycler
  actually fire? Trigger match now by customer type (4A), the stale B2B backlog
  it misses (4B), what was recycled this week (4C), and bounce-backs — recycled
  contacts that returned to SQL (4D). Section 4 only *diagnoses*; it does not
  change the HubSpot workflow.

## Scope & rules (non-negotiable)

- **B2B only.** Contacts filter on `customer_type = B2B`; companies on
  `customer_type__cloned_ = B2B`. `customer_type` on a **company** silently
  fails and is never used.
- **Two bases, never mixed.** Any contact count (funnel, SQL totals,
  miscategorised, new leads) comes from the CONTACT object only. Any pipeline
  value comes from DEALS, scoped B2B via the associated COMPANY. Companies never
  feed contact/SQL counts. (SQL total is B2B *contacts* at the SQL stage — ~157
  — not companies at an SQL-like stage.)
- **Deal USD value is `amount_in_home_currency`.** Raw `amount` +
  `deal_currency_code` are secondary reference only; open deals with an empty
  home-currency value are flagged (3B), never back-filled from raw amount.
- **Contact→company joins are done in Python** from pulled scalars
  (`associatedcompanyid` on contacts; `id`/`createdate` on companies), because
  contact→company cross-object filters silently return the full set in the MCP
  SQL layer.
- **Owner resolution:** `hubspot_owner_id` equals a user's `hs_internal_user_id`
  (search the `users` object). Records with no owner show as "Unassigned".
- **Guard:** if the B2B company count is 0 or equals the full unfiltered count,
  the run aborts (silent-fail signature on the company filter).

## Data pull

Data is pulled from HubSpot via the MCP SQL layer (`query_crm_data`) and shaped
into `raw_input.json` with this schema:

```jsonc
{
  "generated_at": "<ISO>", "window_days": 7, "idle_days": [30, 60],
  "owners": { "<owner_id>": "<name>" },
  "reference_counts": { /* guard + context aggregate counts */ },
  "contacts_b2b":   [ /* B2B contacts: SQL + new-in-window + miscategorised */ ],
  "sql_all_types":  [ /* contacts at SQL across ALL customer types (recycler) */ ],
  "recycled_recent":[ /* recycled_date within the last 7 days, all types */ ],
  "companies_b2b":  [ /* B2B companies created in the window */ ],
  "deals_b2b":      [ /* B2B deals (scoped via COMPANY.customer_type__cloned_) */ ]
}
```

Deal B2B scoping uses the working cross-object filter
`WHERE COMPANY.customer_type__cloned_ = 'B2B'`.

## Running

```bash
python3 pipeline.py --input raw_input.json
pandoc reports/YYYY-MM-DD.md -o reports/YYYY-MM-DD.docx \
  --reference-doc=templates/pipeline_report_template.docx
```

`pipeline.py` requires `--input`; it does no live fetch.

## Editable constants

Deal-stage labels, `STAGE_ORDER`, `SALES_WORKING_STATUSES`,
`RECYCLER_TRIGGER_STATUS`, `WINDOW_DAYS`, `IDLE_BUCKETS`, `QTB_STALE_DAYS` and
`RECYCLE_DAYS` all live at the top of `pipeline.py`.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` routine.
