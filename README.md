# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data plus computed metrics.
- `reports/YYYY-MM-DD.md` — a readable summary: alerts, funnel, MQL/SQL ratio,
  deal count & **USD** value by stage, the "deals moved this week" view, the
  stalled-SQL follow-up list, and the miscategorised-lead list.
- `reports/YYYY-MM-DD.docx` — a Word version of the same report, produced with
  `pandoc` using `templates/pipeline_report_template.docx` as the style
  reference.

Each run loads the most recent previous snapshot and shows week-on-week
movement for every metric.

## Report sections

- **Alerts** (top): "Qualified to Buy" deals idle 14+ days; engaged SQLs gone
  quiet (30+ days); miscategorised-lead count; funnel-inversion flag; and a
  count of deals missing a USD home value.
- **Funnel** — B2B contacts per lifecycle stage, with week-on-week deltas.
- **MQL vs SQL** — counts, ratio, and the funnel-inversion check.
- **Deals — count and value by stage** — all values in **USD**
  (`amount_in_home_currency`); raw `amount` is never summed across currencies.
- **Deals moved this week** — deals with a stage change, new creation, logged
  activity, or field update in the last 7 days, grouped by current stage
  (highest-value first). Stage changes are detected against the prior snapshot.
- **SQLs engaged but gone quiet** — B2B contacts *and* B2B companies at
  `salesqualifiedlead` with an engaged status but no activity in 30+ days.
  A human-review list, not an auto-recycle.

## Currency normalisation

The account default currency is USD. Deals are created in mixed currencies
(notably INR), so all deal values are reported and summed using HubSpot's
home-currency conversion. **Note:** in this portal that property is
`amount_in_home_currency` (label "Amount in company currency"); the field
`hs_deal_amount_in_home_currency` referenced in some specs does not exist here.
A deal whose home value is empty is **flagged and counted as $0** — the raw
`amount` is never used as a fallback because it may be in a non-USD currency.

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
```

The token is read from the environment and is never printed or committed.

### Word (.docx) export

After the Markdown report is written, convert it with pandoc:

```bash
pandoc reports/YYYY-MM-DD.md -o reports/YYYY-MM-DD.docx \
  --reference-doc=templates/pipeline_report_template.docx
```

Install pandoc first if it is not present (`apt-get install -y pandoc`). The
reference doc controls Word styling; `templates/pipeline_report_template.docx`
is currently pandoc's default reference and can be replaced with a branded one.

## Schedule

Runs weekly, on the same cadence as the `brahmarsive-ci` competitor-monitoring
routine. Scheduling is managed at the routine/platform level (this repo holds
the logic and the committed outputs).

## Reference maps

Lifecycle and deal-stage internal→label maps live at the top of `pipeline.py`.
Notably `dealstage = appointmentscheduled` is the **"Qualified to Buy"** stage.
