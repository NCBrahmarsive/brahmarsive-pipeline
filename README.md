# brahmarsive-pipeline

Weekly **B2B** pipeline & MQL-health report, sourced from HubSpot.
**Process version: Oct 2026** (lifecycle ↔ lead-status rules, rebuilt recycler,
owner action lists, workflow firing checks, Q4 scorecard).

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — raw input + all worklist ids + computed metrics
  (also stores each contact's lifecycle + lead status, so next week's run can diff
  what changed).
- `reports/YYYY-MM-DD.md` — Q4 scorecard, headline flags, Sections 1–6.
- `reports/YYYY-MM-DD_owner_<first>.md` — one owner's Section-5 block, ready to
  paste into an email.
- `reports/YYYY-MM-DD.docx` — the main report via pandoc + the reference template.

Each run loads the most recent comparable prior snapshot and shows week-on-week
movement for the headline numbers and key worklist counts.

## Scope & rules (non-negotiable)

- **B2B only, everywhere.** Contacts filter on `customer_type = B2B`; companies on
  `customer_type__cloned_ = B2B` (`customer_type` silently fails on companies).
- **Two bases, never mixed.** Every contact/SQL count comes from the CONTACT object;
  all pipeline value comes from DEALS rolled up at company level.
- **Deal USD = `amount_in_home_currency`.** Empty/0 is flagged (3B i), never
  backfilled from the raw `amount`. Raw amount + currency shown only as reference.
- **Lead status is mixed-format.** Internal values are read live from the property
  and mapped to labels; all rules match on the label. (Portal label for internal
  `NEW` is "New Lead" — the spec's shorthand "New".)

## Data pull

Direct egress to `api.hubapi.com` is blocked in the run environment, so data is
pulled via the **HubSpot MCP tools** into `raw_input.json` (never a live fetch).
The required object sets, properties, and the `raw_input.json` schema are described
in the routine spec. Owner ids are resolved to names via the `users` object
(`hs_internal_user_id` == `hubspot_owner_id`).

## Running

```bash
python3 pipeline.py --input raw_input.json            # date defaults to today UTC
python3 pipeline.py --input raw_input.json --date 2026-11-10

pandoc reports/2026-11-10.md -o reports/2026-11-10.docx \
  --reference-doc=templates/pipeline_report_template.docx
```

`pipeline.py` requires `--input`; it does not fetch live.

## Report structure

- **Q4 scorecard** — new deals / entered-trial / closed-won vs target and pace.
- **Headline flags** — workflow health, stage moves, idle %, miscategorised,
  status mismatches, stuck recycles, deals past close / missing USD, orphans, etc.
- **Section 1** — current pipeline by stage and by owner, plus moved/updated-this-week.
- **Section 2** — new companies, new contacts on existing vs new accounts, SQLs by owner.
- **Section 3** — data-hygiene worklist (3A companies; 3B deals; 3C miscategorised;
  3D status mismatches; 3E orphans; 3F lost-deal leftovers; 3G MQL follow-up;
  3H won-but-not-Customer).
- **Section 4** — recycler health (4A trigger match, 4C recycled this week, 4D stuck).
- **Section 5** — per-owner action lists (sales owners + Nicola's rollup block).
- **Section 6** — "built ≠ working" workflow firing checks.

## Editable constants

All tunables (deal-stage labels, stage order, sales owners, valid-status-by-lifecycle,
recycle thresholds, Q4 targets, claim-window dates) live at the top of `pipeline.py`.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` routine.
