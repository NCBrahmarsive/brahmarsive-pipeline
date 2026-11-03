# brahmarsive-pipeline

Weekly **B2B pipeline & MQL-health** report, sourced from HubSpot.
Process version: **Oct 2026** (lifecycle ↔ lead-status rules, rebuilt recycler
diagnostics, per-owner action lists, workflow-firing checks).

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — raw inputs, every headline number, and the ids
  behind each worklist (so next week can diff lifecycle/lead-status changes).
- `reports/YYYY-MM-DD.md` — the full report: Q4 scorecard, headline flags, and
  Sections 1–6.
- `reports/YYYY-MM-DD.docx` — the same report via `pandoc`, styled with
  `templates/pipeline_report_template.docx`.
- `reports/YYYY-MM-DD_owner_<first>.md` — one Section-5 block per owner, ready
  to paste into an email.

Each run loads the most recent comparable snapshot and shows week-on-week deltas.

## Scope & rules (non-negotiable)

- **B2B only, everywhere.** Contacts use `customer_type = B2B`; companies and
  deals use `customer_type__cloned_ = B2B` (`customer_type` silently fails on
  companies — never use it there).
- **Two bases, never mixed.** Every contact/SQL count comes from the CONTACT
  object; every pipeline/deal value comes from DEALS scoped to B2B companies.
  (B2B *contacts* at SQL ≈ 402; B2B *companies* at SQL is a different number and
  must never be reported as the SQL count.)
- **Deal USD = `amount_in_home_currency`.** Empty or 0 is flagged, never
  replaced by raw `amount`. Raw `amount` + `deal_currency_code` are secondary.
- **Lead status is mixed-format** (labels and codes). The run reads the live
  `hs_lead_status` definition and matches on the **label**, never a hard-coded
  value. `CONNECTED` → "Contacted"; internal `NEW` → label "New Lead".
- **Owners** are resolved to names from the owners map (owner id =
  `hs_internal_user_id`); anyone who is not a sales owner, and Unassigned, rolls
  into Nicola's Section-5 block.

Editable constants (dealstage labels, `SALES_OWNERS`,
`VALID_STATUS_BY_LIFECYCLE`, recycle/idle thresholds, Q4 targets, claim-window
dates) live at the top of `pipeline.py`.

## Data flow

Direct egress to `api.hubapi.com` is blocked in the run environment, so CRM data
is pulled through the **HubSpot MCP tools** and cached under `.rawpull/`
(git-ignored). `build_raw_input.py` parses those caches into `raw_input.json`,
and `pipeline.py` computes the report from it.

```bash
# 1. pull CRM data via the HubSpot MCP tools into .rawpull/  (see build_raw_input.py header)
# 2. assemble the single input file
python3 build_raw_input.py
# 3. produce snapshot + report + owner files + .docx
python3 pipeline.py --input raw_input.json      # never run without --input
```

`pipeline.py` regenerates the `.docx` automatically when `pandoc` is installed.

## Report structure

- **Q4 scorecard** — new deals / entered-trial / closed-won vs target & pace.
- **Headline flags** — workflow health, stage moves, idle %, mismatches, stuck
  recycles, missing USD, ownerless accounts, recycle backlog.
- **Section 1** — open pipeline by stage and by owner; stage moves this week.
- **Section 2** — new companies, new contacts (existing vs new accounts),
  SQLs by owner with idle buckets.
- **Section 3** — hygiene worklists 3A–3H.
- **Section 4** — recycler health (4A trigger backlog, 4C recycled-this-week,
  4D stuck recycles).
- **Section 5** — per-owner action lists.
- **Section 6** — "built ≠ working" workflow-firing checks.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` routine. Scheduling is
managed at the routine/platform level; this repo holds the logic and the
committed outputs.
