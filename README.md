# brahmarsive-pipeline

Weekly **B2B** pipeline & MQL-health report, sourced from HubSpot.
Process version: **Oct 2026** (lifecycle ↔ lead-status rules, rebuilt recycler
diagnostics, per-owner action lists, workflow firing checks).

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — funnel counts, every worklist's ids, each
  contact's lifecycle + lead status (so next week can diff) and week-on-week
  `metrics_lite`.
- `reports/YYYY-MM-DD.md` — Q4 scorecard, headline flags, and Sections 1–6.
- `reports/YYYY-MM-DD_owner_<first>.md` — one Section-5 block per sales owner,
  ready to paste into an email.
- `reports/YYYY-MM-DD.docx` — the report via `pandoc` (uses
  `templates/pipeline_report_template.docx` as the reference doc when present).

## Scope & field rules (non-negotiable)

- **B2B only, everywhere.** Contacts filter `customer_type = B2B`; companies
  filter `customer_type__cloned_ = B2B` (`customer_type` silently fails on
  companies). Deals are scoped B2B via the cross-object
  `COMPANY.customer_type__cloned_ = 'B2B'` filter.
- **Two bases, never mixed.** Every contact / SQL count comes from the CONTACT
  object; all pipeline value comes from DEALS. Companies never feed contact
  counts. (Companies at the SQL stage are *not* SQLs — the true B2B SQL total is
  a contact count, 403 this run.)
- **Deal USD** = `amount_in_home_currency`. Raw `amount` + `deal_currency_code`
  are secondary reference only; empty/0 USD is flagged, never back-filled.
- **Lead status is mixed-format.** All rules match on the mapped *label*, built
  from the live `hs_lead_status` property definition. Note: the spec's Lead/MQL
  `New` label is stored as **`New Lead`** (internal `NEW`) — reconciled in
  `VALID_STATUS_BY_LIFECYCLE`.
- **Contact→company joins** (orphan, new-on-existing) are done in `pipeline.py`
  from pulled scalars, because MCP-SQL contact→company cross-object filters
  silently return the full set.

## Running

HubSpot egress is blocked from the run environment, so data is pulled via the
HubSpot MCP tools into `raw_input.json`, then:

```bash
python3 pipeline.py --input raw_input.json      # never run without --input
```

Editable constants (stage labels, sales owners, valid-status-by-lifecycle,
recycle/idle thresholds, claim window, Q4 targets) live at the top of
`pipeline.py`.

## Report sections

- **Q4 Scorecard** — new deals / entered-trial / closed-won vs. pace.
- **Headline flags** — workflow health, stage moves, idle %, mismatches,
  stuck recycles, missing USD, unowned new accounts, recycles.
- **1** Current pipeline (by stage, by owner, moved/updated this week).
- **2** New pipeline (new companies, new contacts on existing vs new accounts,
  SQLs by owner).
- **3** Data-hygiene worklist (3A companies, 3B deals, 3C miscategorised,
  3D status mismatches, 3E orphans, 3F lost-deal leftovers, 3G MQL follow-up,
  3H won-but-not-customer).
- **4** Recycler health (4A trigger matches, 4C recycled this week, 4D stuck).
- **5** Per-owner action lists.
- **6** Workflow firing checks ("built ≠ working").

## Schedule

Weekly, same cadence as the `brahmarsive-ci` routine.
