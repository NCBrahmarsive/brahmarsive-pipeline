# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data plus computed metrics.
- `reports/YYYY-MM-DD.md` — a readable summary: alerts, "deals moved this
  week", funnel, MQL/SQL ratio, deal count & USD value by stage, the
  stalled-SQL follow-up list, and the miscategorised-lead list.
- `reports/YYYY-MM-DD.docx` — a Word version of the same report, produced with
  `pandoc … --reference-doc=templates/pipeline_report_template.docx`.

Each run loads the most recent previous snapshot and shows week-on-week
movement for every metric.

## Scope & rules

- **B2B only.** Contacts are filtered to `customer_type = B2B`; companies to
  `customer_type__cloned_ = B2B`. That single filter per object drops B2C,
  Partner, Vendor, Internal and Partner-B2B records.
- **Currency (USD-normalised).** Deal totals are summed in USD via HubSpot's
  converted amount. In this portal that property is `amount_in_home_currency`
  (label "Amount in company currency") — the name `hs_deal_amount_in_home_currency`
  from the original brief does not exist here. Raw `amount` + `deal_currency_code`
  are reference only and never summed across currencies; deals with an empty
  converted amount are flagged, not back-filled from the raw amount.
- **Miscategorised leads** (editable at the top of `pipeline.py`): a contact
  is flagged when sales is clearly working it (`hs_lead_status` ∈ {Qualified,
  Contacted, Attempted to Contact, In Progress, Proposal Request}) but its
  `lifecyclestage` is still only `lead` or `marketingqualifiedlead`.
- **Stalled engaged SQLs** (editable at the top of `pipeline.py`): SQL contacts
  and companies with an engaged lead status (Connected/Qualified/In Progress/
  Proposal Request) whose last activity is 30+ days ago. A human follow-up list,
  NOT an auto-recycle.
- **Deals moved this week:** every deal modified or with new activity in the last
  7 days, grouped by current stage (highest USD first); stage changes are detected
  by diffing against last week's snapshot.
- **Alerts:** "Qualified to Buy" deals idle 14+ days; the miscategorised count;
  the stalled-engaged-SQL count; a funnel-inversion flag when SQL > MQL; and any
  deals missing a USD converted amount.

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

The token is read from the environment and is never printed or committed.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` competitor-monitoring
routine. Scheduling is managed at the routine/platform level (this repo holds
the logic and the committed outputs).

## Reference maps

Lifecycle and deal-stage internal→label maps live at the top of `pipeline.py`.
Notably `dealstage = appointmentscheduled` is the **"Qualified to Buy"** stage.
