# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data plus computed metrics.
- `reports/YYYY-MM-DD.md` — a readable summary: alerts, "what moved this
  week", the engaged-but-stalled SQL review list, funnel, MQL/SQL ratio,
  deal count & USD value by stage, and the miscategorised-lead list.
- `reports/YYYY-MM-DD.docx` — a Word version of the report, produced with
  `pandoc … --reference-doc=templates/pipeline_report_template.docx`.

Each run loads the most recent previous snapshot and shows week-on-week
movement for every metric.

## Scope & rules

- **B2B only.** The funnel is filtered to `customer_type = B2B` (contacts) and
  `customer_type__cloned_ = B2B` (companies). That single filter drops B2C,
  Partner, Vendor, Internal and Partner-B2B records.
- **Currency.** The account home currency is USD. Deals may be created in other
  currencies (notably INR), so all deal totals are summed on HubSpot's
  home-currency USD value (`amount_in_home_currency`). Raw `amount` +
  `deal_currency_code` are kept only as a per-deal reference and are never
  summed across mixed currencies. Deals with an empty home-currency value are
  flagged, not silently fallen back to the raw amount.
- **Miscategorised leads** (editable at the top of `pipeline.py`): a contact
  is flagged when sales is clearly working it (`hs_lead_status` ∈ {Qualified,
  Contacted, Attempted to Contact, In Progress, Proposal Request}) but its
  `lifecyclestage` is still only `lead` or `marketingqualifiedlead`.
- **Engaged-but-stalled SQLs** (30-day threshold, tunable): B2B contacts *and*
  companies at `salesqualifiedlead` with a working lead status (Connected,
  Qualified, In Progress, Proposal Request) whose last activity is 30+ days
  old. A human follow-up/review list — nothing here is auto-recycled. Owner
  ids are resolved to names.
- **Alerts:** "Qualified to Buy" deals idle 14+ days; the engaged-stalled-SQL
  count; the miscategorised count; a funnel-inversion flag when SQL > MQL; and
  a count of deals missing a USD home-currency value.

## Running

```bash
# Live: pulls straight from HubSpot using the token.
export HUBSPOT_TOKEN=...        # HubSpot Service key: contacts.read + deals.read
python3 pipeline.py

# Pre-pulled: consume data already fetched from HubSpot (used when direct
# egress to api.hubapi.com is blocked by the run environment's egress policy;
# data is then pulled via the HubSpot MCP tools into raw_input.json).
python3 pipeline.py --input raw_input.json --date YYYY-MM-DD

# Word export (after the .md is written):
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
