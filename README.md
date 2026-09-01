# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data plus computed metrics.
- `reports/YYYY-MM-DD.md` — a readable summary: alerts, funnel, MQL/SQL ratio,
  deal count & value by stage (USD), a "what moved this week" view, the stalled
  engaged-SQL follow-up list, and the miscategorised-lead list.
- `reports/YYYY-MM-DD.docx` — a Word version of the same report, produced with
  `pandoc … --reference-doc=templates/pipeline_report_template.docx`.

Each run loads the most recent previous snapshot and shows week-on-week
movement for every metric.

## Currency

The HubSpot account home currency is **USD**. Deals may be created in other
currencies (INR, GBP, EUR, NGN, PHP). All deal **values** are reported and
summed in USD using HubSpot's `amount_in_home_currency`; the original
`amount` + `deal_currency_code` are shown only as a secondary reference. Raw
`amount` is never summed across mixed currencies. Deals with no USD
home-currency value are flagged as a data-quality alert (never silently
substituted with the raw amount).

## Scope & rules

- **B2B only.** The funnel is filtered to `customer_type = B2B`. That single
  filter drops B2C, Partner, Vendor, Internal and Partner-B2B contacts.
- **Miscategorised leads** (editable at the top of `pipeline.py`): a contact
  is flagged when sales is clearly working it (`hs_lead_status` ∈ {Qualified,
  Contacted, Attempted to Contact, In Progress, Proposal Request}) but its
  `lifecyclestage` is still only `lead` or `marketingqualifiedlead`.
- **Stalled engaged SQLs** (follow-up / review, NOT auto-recycle): SQL records
  — contacts (`customer_type = B2B`) **and** companies
  (`customer_type__cloned_ = B2B`) — where sales has already made real contact
  (`hs_lead_status` ∈ {Connected, Qualified, In Progress, Proposal Request})
  but activity has gone quiet for 30+ days. Grouped by lead status, sorted
  most-stalled first, with owner names resolved. Nothing here moves
  automatically; it is a live list for sales to action.
- **What moved this week:** deals with a stage change, logged activity, or
  field update in the last 7 days, grouped by current stage, highest USD value
  first — the quick "how did the pipeline shift this week?" view.
- **Alerts:** "Qualified to Buy" deals idle 14+ days; the stalled engaged-SQL
  count; the miscategorised count; deals missing a USD home-currency value; and
  a funnel-inversion flag when SQL > MQL.

## Running

```bash
# Live: pulls straight from HubSpot using the token.
export HUBSPOT_TOKEN=...        # HubSpot Service key: contacts.read + deals.read
python3 pipeline.py

# Pre-pulled: consume data already fetched from HubSpot (used when direct
# egress to api.hubapi.com is blocked by the run environment's egress policy;
# data is then pulled via the HubSpot MCP tools into raw_input.json).
python3 pipeline.py --input raw_input.json --date YYYY-MM-DD

# Word version (install pandoc if missing):
pandoc reports/YYYY-MM-DD.md -o reports/YYYY-MM-DD.docx \
  --reference-doc=templates/pipeline_report_template.docx
```

The token is read from the environment and is never printed or committed.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` competitor-monitoring
routine. Scheduling is managed at the routine/platform level (this repo holds
the logic and the committed outputs).

## Reference maps

Lifecycle and deal-stage internal→label maps live at the top of `pipeline.py`.
Notably `dealstage = appointmentscheduled` is the **"Qualified to Buy"** stage.
