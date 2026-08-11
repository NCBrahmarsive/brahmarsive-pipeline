# brahmarsive-pipeline

Weekly B2B pipeline & MQL-health report, sourced from HubSpot.

## What it produces (each run)

- `snapshots/YYYY-MM-DD.json` — the pulled data plus computed metrics.
- `reports/YYYY-MM-DD.md` — a readable summary: alerts, "what moved this week",
  funnel, MQL/SQL ratio, deal count & value by stage, the miscategorised-lead
  list, and the follow-up list of engaged-but-quiet SQLs.
- `reports/YYYY-MM-DD.docx` — a Word rendering of the `.md`, produced with
  `pandoc` using `templates/pipeline_report_template.docx` as the reference doc.

Each run loads the most recent previous snapshot and shows week-on-week
movement for every metric.

## Scope & rules

- **B2B only.** Contacts are filtered to `customer_type = B2B`; companies use
  `customer_type__cloned_ = B2B`. That single filter drops B2C, Partner,
  Vendor, Internal and Partner-B2B records.
- **Currency.** Account home currency is USD. Deal values are always reported
  and summed using `amount_in_home_currency` (HubSpot's USD conversion). Raw
  `amount` is never summed across mixed currencies and is shown only as a
  secondary reference. A deal with an empty `amount_in_home_currency` is
  flagged and excluded from value sums (never falls back to raw amount).
- **Miscategorised leads** (editable at the top of `pipeline.py`): a contact
  is flagged when sales is clearly working it (`hs_lead_status` ∈ {Qualified,
  Contacted, Attempted to Contact, In Progress, Proposal Request}) but its
  `lifecyclestage` is still only `lead` or `marketingqualifiedlead`.
- **Engaged-but-quiet SQLs** (follow-up/review, NOT auto-recycle): B2B contacts
  and companies at `salesqualifiedlead` with a working status (Contacted /
  Qualified / In Progress / Proposal Request) and no activity in 30+ days.
  Distinct from the auto-recycled "Attempted to Contact" cases.
- **Alerts:** "Qualified to Buy" deals idle 14+ days; the miscategorised count;
  the engaged-but-quiet SQL count; deals missing a USD value; and a
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

# Word rendering (after the .md is written):
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
