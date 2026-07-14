# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data plus computed metrics.
- `reports/YYYY-MM-DD.md` — a readable summary: alerts, "deals moved this
  week", the stalled-engaged-SQL review list, the miscategorised-lead list,
  the funnel, MQL/SQL ratio, and deal count & USD value by stage.
- `reports/YYYY-MM-DD.docx` — a Word version of the report, generated from the
  markdown via pandoc using `templates/pipeline_report_template.docx` as the
  reference document.

Each run loads the most recent previous snapshot and shows week-on-week
movement for every metric.

## Report sections

- **Alerts** (top): "Qualified to Buy" deals idle 14+ days (with USD value),
  miscategorised-lead count, stalled-engaged-SQL count, deals missing a USD
  conversion, and a funnel-inversion flag.
- **Deals moved this week** — every deal modified or with logged activity in
  the last 7 days, grouped by current stage (highest USD first), with what
  changed (stage change / new deal / activity / field update).
- **SQLs engaged but gone quiet** — B2B contacts and B2B companies at Sales
  Qualified Lead with an engaged lead status, idle 30+ days. Follow-up/review
  only — **not** auto-recycle. Grouped by lead status, owners resolved to names,
  associated deal + USD shown where one exists.
- **Funnel, MQL vs SQL, Deals by stage (USD)** — with week-on-week deltas.

## Currency

The account's home currency is USD. Deal values are reported and summed using
HubSpot's USD conversion (`amount_in_home_currency`; the brief refers to this
as `hs_deal_amount_in_home_currency`). Raw `amount` is never summed across
mixed currencies — it appears only as a secondary reference on non-USD deals.
Deals with an empty USD conversion are flagged, never back-filled from raw.

## Scope & rules

- **B2B only.** The funnel is filtered to `customer_type = B2B`. That single
  filter drops B2C, Partner, Vendor, Internal and Partner-B2B contacts.
- **Miscategorised leads** (editable at the top of `pipeline.py`): a contact
  is flagged when sales is clearly working it (`hs_lead_status` ∈ {Qualified,
  Contacted, Attempted to Contact, In Progress, Proposal Request}) but its
  `lifecyclestage` is still only `lead` or `marketingqualifiedlead`.
- **Alerts:** "Qualified to Buy" deals idle 14+ days; the miscategorised count;
  and a funnel-inversion flag when SQL > MQL.

## Running

```bash
# Live: pulls straight from HubSpot using the token.
export HUBSPOT_TOKEN=...        # HubSpot Service key: contacts.read + deals.read
python3 pipeline.py

# Pre-pulled: consume data already fetched from HubSpot (used when direct
# egress to api.hubapi.com is blocked by the run environment's egress policy;
# data is then pulled via the HubSpot MCP tools into raw_input.json).
python3 pipeline.py --input raw_input.json --date YYYY-MM-DD

# Word export (run after the markdown is written; install pandoc if needed):
pandoc reports/YYYY-MM-DD.md -o reports/YYYY-MM-DD.docx \
  --reference-doc=templates/pipeline_report_template.docx
```

The token is read from the environment and is never printed or committed.

The B2B scope filter differs by object: contacts use `customer_type = B2B`,
companies use `customer_type__cloned_ = B2B`.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` competitor-monitoring
routine. Scheduling is managed at the routine/platform level (this repo holds
the logic and the committed outputs).

## Reference maps

Lifecycle and deal-stage internal→label maps live at the top of `pipeline.py`.
Notably `dealstage = appointmentscheduled` is the **"Qualified to Buy"** stage.
