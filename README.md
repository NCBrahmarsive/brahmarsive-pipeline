# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data plus computed metrics.
- `reports/YYYY-MM-DD.md` — a readable summary: alerts, funnel, MQL/SQL ratio,
  "deals moved this week", deal count & value (USD) by stage, the
  stale-engaged-SQL follow-up list, and the miscategorised-lead list.
- `reports/YYYY-MM-DD.docx` — a Word version of the report, converted with
  pandoc using `templates/pipeline_report_template.docx` as the reference doc.

Each run loads the most recent previous snapshot and shows week-on-week
movement for every metric.

## Scope & rules

- **B2B only.** Contacts are filtered to `customer_type = B2B`; companies (used
  only for the stale-engaged-SQL list) to `customer_type__cloned_ = B2B`. That
  single filter drops B2C, Partner, Vendor and Internal records.
- **Currency (USD).** The account home currency is USD but deals may be created
  in other currencies (notably INR). Every deal total is summed using HubSpot's
  home-currency (USD) value (`amount_in_home_currency`); raw `amount` +
  `deal_currency_code` are shown only as a secondary reference and never summed
  across currencies. Deals with an empty home-currency value are flagged, not
  fallen back to raw amount.
- **Miscategorised leads** (editable at the top of `pipeline.py`): a contact
  is flagged when sales is clearly working it (`hs_lead_status` ∈ {Qualified,
  Contacted, Attempted to Contact, In Progress, Proposal Request}) but its
  `lifecyclestage` is still only `lead` or `marketingqualifiedlead`.
- **Stale-engaged SQLs** (editable at the top of `pipeline.py`): B2B contacts
  and companies at `salesqualifiedlead` with a working lead status but no
  activity in 30+ days. A follow-up/review list for sales — nothing is
  auto-recycled here; distinct from the "Attempted to Contact" recycler.
- **Alerts:** "Qualified to Buy" deals idle 14+ days; engaged-SQL-gone-quiet
  count; miscategorised count; a funnel-inversion flag when SQL > MQL; and any
  deals missing a USD home-currency value.

## Running

```bash
# Live: pulls straight from HubSpot using the token.
export HUBSPOT_TOKEN=...        # HubSpot Service key: contacts.read + deals.read
python3 pipeline.py

# Pre-pulled: consume data already fetched from HubSpot (used when direct
# egress to api.hubapi.com is blocked by the run environment's egress policy;
# data is then pulled via the HubSpot MCP tools into raw_input.json).
python3 pipeline.py --input raw_input.json --date YYYY-MM-DD

# Word version (run after the markdown report is written):
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
