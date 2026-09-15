# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data plus computed metrics.
- `reports/YYYY-MM-DD.md` — a readable summary: funnel, MQL/SQL ratio,
  deal count & value by stage, alerts, the miscategorised-lead list, the
  "what moved this week" view, and the stalled-engaged-SQL follow-up list.
- `reports/YYYY-MM-DD.docx` — a Word version of the report, produced from the
  markdown with `pandoc … --reference-doc=templates/pipeline_report_template.docx`.

Each run loads the most recent previous snapshot and shows week-on-week
movement for every metric.

## Currency

Deal values are reported in **USD** (the HubSpot account home currency) using
each deal's `amount_in_home_currency` (HubSpot's FX-converted value). Raw
`amount` + `deal_currency_code` are shown only as a secondary reference; raw
amounts are **never** summed across mixed currencies. A deal with an empty
`amount_in_home_currency` is **flagged** and excluded from USD totals rather
than falling back to the raw amount.

> Property-name note: the routine brief calls this
> `hs_deal_amount_in_home_currency`; this portal exposes the home-currency
> value as `amount_in_home_currency` (label "Amount in company currency"),
> which is what the pull and the pipeline use.

## Scope & rules

- **B2B only.** Contacts are filtered to `customer_type = B2B`; companies to
  `customer_type__cloned_ = B2B`. That single filter drops B2C, Partner,
  Vendor, Internal and Partner-B2B records.
- **Miscategorised leads** (editable at the top of `pipeline.py`): a contact
  is flagged when sales is clearly working it (`hs_lead_status` ∈ {Qualified,
  Contacted, Attempted to Contact, In Progress, Proposal Request}) but its
  `lifecyclestage` is still only `lead` or `marketingqualifiedlead`.
- **Stalled engaged SQLs** (editable at the top of `pipeline.py`): B2B SQL
  records (contacts *and* companies) with an engaged status (Connected,
  Qualified, In Progress, Proposal Request) and no activity for 30+ days.
  This is a human follow-up / review list — nothing is auto-recycled.
- **What moved this week:** deals touched (stage change, activity, or field
  update) in the last 7 days, grouped by current stage, highest USD first.
- **Alerts:** "Qualified to Buy" deals idle 14+ days; the miscategorised count;
  the stalled-engaged-SQL count; deals missing a USD value; and a
  funnel-inversion flag when SQL > MQL.

## Running

```bash
# Live: pulls straight from HubSpot using the token.
export HUBSPOT_TOKEN=...        # HubSpot Service key: contacts.read + deals.read
python3 pipeline.py

# Pre-pulled: consume data already fetched from HubSpot (used when direct
# egress to api.hubapi.com is blocked by the run environment's egress policy;
# data is then pulled via the HubSpot MCP tools into raw_input.json).
python3 pipeline.py --input raw_input.json --date YYYY-MM-DD
```

The `.docx` step needs `pandoc` on the PATH (`apt-get install -y pandoc`); if
it's missing, the run still writes the `.json` and `.md` and skips the `.docx`
with a warning. Pass `--no-docx` to skip it explicitly.

The token is read from the environment and is never printed or committed.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` competitor-monitoring
routine. Scheduling is managed at the routine/platform level (this repo holds
the logic and the committed outputs).

## Reference maps

Lifecycle and deal-stage internal→label maps live at the top of `pipeline.py`.
Notably `dealstage = appointmentscheduled` is the **"Qualified to Buy"** stage.
