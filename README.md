# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, generated from HubSpot.

## What it does

`brahmarsive_pipeline.py` pulls **contacts, deals and companies** from HubSpot,
filters to **B2B only** (contacts: `customer_type = B2B`; companies:
`customer_type__cloned_ = B2B` — this single filter drops B2C / Partner / Vendor
/ Internal), and writes two dated files per run:

- `snapshots/YYYY-MM-DD.json` — raw pulled data + computed metrics (drives
  week-on-week comparison on the next run).
- `reports/YYYY-MM-DD.md` — leadership summary: alerts, lifecycle funnel,
  MQL/SQL ratio (funnel-inversion check), deal value by stage, "what moved this
  week", miscategorised leads, and stalled-SQL follow-up list.

All monetary totals are in **USD** via HubSpot's `amount_in_home_currency`
(HubSpot's home-currency conversion of each deal). Raw `amount` +
`deal_currency_code` are shown only as a secondary reference; mixed-currency raw
amounts are never summed. Deals with an empty `amount_in_home_currency` are
**flagged**, not back-filled from raw amount.

> Note: the portal has no `hs_deal_amount_in_home_currency` property; the actual
> home-currency (USD) field is `amount_in_home_currency`, which is what the
> script uses.

## Health checks (editable at the top of the script)

- **Miscategorised leads** — `is_miscategorised()` and its status/stage sets.
  Sales is working the contact (lead status Qualified / Connected / Attempted to
  Contact / In Progress / Proposal Request) but lifecycle is still only Lead or
  MQL.
- **Stalled SQLs (follow-up / review, NOT auto-recycle)** —
  `STALLED_SQL_STATUSES` + `STALLED_SQL_THRESHOLD_DAYS` (default 30). Excludes the
  auto-recycled "Attempted to Contact" bucket.
- **Deal alerts** — Qualified-to-Buy deals idle `QTB_IDLE_ALERT_DAYS`+ (14);
  funnel inversion (SQL > MQL); missing home-currency values.

## Usage

```bash
export HUBSPOT_TOKEN=...           # HubSpot private-app key; never printed/committed
python3 brahmarsive_pipeline.py                    # live pull, dated today (UTC)
python3 brahmarsive_pipeline.py --date 2026-07-07  # live pull, fixed date
```

Standard library only — no dependencies.

### Offline / restricted-egress mode

Some execution environments block direct egress to `api.hubapi.com`. In that
case the data can be pulled out-of-band (e.g. via the HubSpot MCP) into JSON and
fed to the same compute/report code:

```bash
python3 brahmarsive_pipeline.py --input pull.json --deals deals.json --date YYYY-MM-DD
```

## Schedule

Runs **weekly**, same cadence as the `brahmarsive-ci` competitor-monitoring
routine. The recurring schedule is configured in the Claude Code scheduled-routine
settings (not in this repo).
