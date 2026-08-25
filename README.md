# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data plus computed metrics.
- `reports/YYYY-MM-DD.md` — a readable summary: funnel, MQL/SQL ratio,
  deal count & USD value by stage, alerts, "what moved this week", the
  engaged-SQL follow-up list, and the miscategorised-lead list.
- `reports/YYYY-MM-DD.docx` — a Word version of the same report, rendered
  with pandoc using `templates/pipeline_report_template.docx` for styling.

Each run loads the most recent previous snapshot and shows week-on-week
movement for every metric.

## Scope & rules

- **B2B only.** Contacts are filtered to `customer_type = B2B`; companies to
  `customer_type__cloned_ = B2B`. That single filter drops B2C, Partner,
  Vendor, Internal and Partner-B2B records.
- **Currency normalisation.** The account/home currency is USD, but deals may
  be in other currencies (notably INR). Every deal value is reported and summed
  in USD via HubSpot's `amount_in_home_currency` conversion; the raw `amount` +
  `deal_currency_code` is shown only as a secondary reference and is **never**
  summed across mixed currencies. Deals with no home-currency value are flagged,
  not counted at their raw amount. (The portal has no
  `hs_deal_amount_in_home_currency` property; `amount_in_home_currency` is the
  home-currency field.)
- **Miscategorised leads** (editable at the top of `pipeline.py`): a contact
  is flagged when sales is clearly working it (`hs_lead_status` ∈ {Qualified,
  Contacted, Attempted to Contact, In Progress, Proposal Request}) but its
  `lifecyclestage` is still only `lead` or `marketingqualifiedlead`.
- **Engaged SQLs gone quiet** (30-day threshold, tunable): B2B contacts **and**
  companies at `salesqualifiedlead` with an active lead status (Contacted,
  Qualified, In Progress, Proposal Request) whose last activity is 30+ days ago.
  A **follow-up / review** list — nothing here is moved automatically, and it is
  distinct from the auto-recycled "Attempted to Contact" cases. Owners are
  resolved from `hubspot_owner_id` to real names.
- **What moved this week:** deals whose last-modified or last-activity date
  falls within the last 7 days, grouped by current stage (highest USD first).
- **Alerts:** "Qualified to Buy" deals idle 14+ days; the engaged-SQL stalled
  count; the miscategorised count; deals missing a USD value; and a
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

The token is read from the environment and is never printed or committed.

The `.docx` render runs automatically when `pandoc` is available (install it
first, e.g. `apt-get install -y pandoc`). Pass `--no-docx` to skip it, or
`--reference-doc PATH` to use a different Word style template.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` competitor-monitoring
routine. Scheduling is managed at the routine/platform level (this repo holds
the logic and the committed outputs).

## Reference maps

Lifecycle and deal-stage internal→label maps live at the top of `pipeline.py`.
Notably `dealstage = appointmentscheduled` is the **"Qualified to Buy"** stage.
