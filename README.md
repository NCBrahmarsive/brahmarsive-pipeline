# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, pulled from HubSpot.

Each run scopes everything to **B2B** (`customer_type` = B2B on contacts,
`customer_type__cloned_` = B2B on companies) and writes two files per run:

- `snapshots/YYYY-MM-DD.json` — raw pulled data + computed metrics.
- `reports/YYYY-MM-DD.md` — readable leadership summary (funnel, MQL/SQL health,
  deal value by stage, "what moved this week", alerts, miscategorised leads,
  stalled-SQL follow-up list).

## What it reports

1. **Lifecycle funnel** — B2B contact counts per lifecycle stage.
2. **MQL vs SQL** — counts, ratio, and a **funnel-inversion flag** if SQL > MQL.
3. **Deal count & value by stage** — USD value via `amount_in_home_currency`.
4. **What moved this week** — deals modified or with logged activity in the last
   7 days, grouped by stage, highest value first.
5. **Miscategorised leads** — sales-working `hs_lead_status` (Qualified /
   Connected / Attempted to Contact / In Progress / Proposal Request) still stuck
   at Lead or MQL. See `is_miscategorised()` at the top of the script.
6. **Stalled engaged SQLs** — Sales-Qualified records engaged (Connected /
   Qualified / In Progress / Proposal Request) but quiet 30+ days. This is a
   **follow-up / review** list — *not* auto-recycle; nothing is moved automatically.
7. **Alerts** — Qualified-to-Buy deals idle 14+ days; current miscategorised and
   stalled-SQL counts.
8. **Week-over-week** — every metric is diffed against the most recent prior
   snapshot in `snapshots/`.

## Currency

The account home currency is **USD**. All value maths use HubSpot's converted
`amount_in_home_currency`; the raw `amount` + `deal_currency_code` are shown only
as a secondary reference. Raw amounts are **never** summed across mixed
currencies. Deals missing `amount_in_home_currency` are flagged, not
face-value-summed.

> Note: this account's converted-amount property is `amount_in_home_currency`
> (HubSpot's standard "Amount in company currency"), not the
> `hs_deal_amount_in_home_currency` name — the script uses the former.

## Running

```bash
# Live pull from HubSpot (requires network egress to api.hubapi.com):
export HUBSPOT_TOKEN=...   # HubSpot Service Key, contacts+deals+companies read
python3 weekly_pipeline_report.py --api

# Offline: build the report from an already-pulled raw dataset:
python3 weekly_pipeline_report.py --input raw_pull.json --date 2026-07-03
```

In the managed Claude-Code web environment the egress policy denies
`api.hubapi.com`, so the weekly routine pulls the data via the HubSpot MCP tools
and feeds it to the script with `--input`. The `--api` path is the portable path
for any environment where the HubSpot host is reachable. The token value is never
printed or committed.

## Business rules

All tunable rules live in the **EDITABLE CONFIG** block at the top of
`weekly_pipeline_report.py`: lifecycle/deal-stage label maps, the day thresholds
(7 / 14 / 30), the lead-status sets, and the `is_miscategorised()` function.

## Schedule

Runs **weekly**, on the same cadence as the `brahmarsive-ci` competitor-monitoring
routine, via the scheduled-routines mechanism.
