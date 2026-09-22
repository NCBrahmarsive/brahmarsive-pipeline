# brahmarsive-pipeline

Weekly **B2B pipeline & MQL-health** report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — pulled data + computed metrics + **all worklist
  record IDs** (so every admin/recycler list is clickable to work from).
- `reports/YYYY-MM-DD.md` — the four-section report (below), plus a
  `.docx` rendered via pandoc against `templates/pipeline_report_template.docx`.

Each run loads the most recent previous snapshot and shows week-on-week movement
for the headline metrics.

## Report sections

- **Headline flags** — deals moved this week; % open deals idle 14+d; % SQLs
  untouched 30+d; miscategorised count; new accounts with no owner; deals missing
  USD; recycler bounce-backs.
- **Section 1 — current pipeline:** open pipeline by stage (qty, USD, median idle,
  idle≥14d) and by owner (with concentration callout); moved vs updated this week.
- **Section 2 — top of funnel:** new companies by owner; new contacts on existing
  vs new accounts; orphan contacts; SQLs by owner (the true SQL total, contacts only).
- **Section 3 — admin worklist:** new accounts to complete; open deals missing a USD
  value; miscategorised leads by owner.
- **Section 4 — recycler health:** trigger match now (by customer type), the stale
  B2B SQL backlog the trigger misses, recycled-this-week, and bounce-backs.

## Scope & rules (non-negotiable)

- **B2B only, everywhere.** Contacts `customer_type='B2B'`; companies
  `customer_type__cloned_='B2B'` (`customer_type` silently fails on companies);
  deals scoped via `COMPANY.customer_type__cloned_='B2B'`.
- **Two bases, never mixed.** Every contact/SQL count comes from the CONTACT
  object; every pipeline value comes from DEALS. The company object never feeds
  contact/SQL counts (B2B contacts at SQL are ~132; companies at SQL are ~322 —
  do not report companies as SQLs).
- **USD value** is `amount_in_home_currency` (never sum raw `amount` across mixed
  INR/USD). A deal with an empty `amount_in_home_currency` is flagged (worklist 3B),
  never valued off raw amount.
- **Contact→company joins** (orphan flag, new-on-existing vs new-account) are done
  in `pipeline.py` from each contact's `associatedcompanyid` — cross-object
  contact→company filters silent-fail in the MCP SQL layer.
- **Owner resolution:** `hubspot_owner_id` → name via the `users` object
  (`hs_internal_user_id` equals the owner id). No owner shows as "Unassigned".

## Editable constants (top of `pipeline.py`)

`DEALSTAGE_LABELS`, `STAGE_ORDER`, `SALES_WORKING_STATUSES`
(`Attempted to Contact`, `Contacted`/`CONNECTED`), `RECYCLER_TRIGGER_STATUS`
(`Attempted to Contact`), `WINDOW_DAYS=7`, `IDLE_BUCKETS=[30,60]`,
`QTB_STALE_DAYS=14`, `RECYCLE_DAYS=90`.

## Running

Data is pulled from HubSpot via the MCP tools into `raw_input.json` (direct egress
to `api.hubapi.com` is not used), then:

```bash
python3 pipeline.py --input raw_input.json
pandoc reports/YYYY-MM-DD.md -o reports/YYYY-MM-DD.docx \
  --reference-doc=templates/pipeline_report_template.docx
```

`pipeline.py` guards against the B2B-company silent-fail: it aborts if the B2B
company count is 0 or equals the unfiltered count.

## Schedule

Weekly, on the same cadence as the `brahmarsive-ci` routine. Scheduling is managed
at the routine/platform level; this repo holds the logic and the committed outputs.
