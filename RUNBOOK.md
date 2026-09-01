# Runbook — weekly B2B pipeline & MQL health

This is the exact data-pull and build spec for the weekly run, kept in the repo
as a backstop so the code and the scheduled routine prompt don't drift. If the
routine prompt and this file ever disagree, **this file wins** — update the
prompt to match.

## Environment note

Direct egress to `api.hubapi.com` is blocked in the run environment. Always
pull via the connected **HubSpot MCP tools** and hand the data to
`pipeline.py --input raw_input.json`. Never run `pipeline.py` without
`--input` (that mode needs live egress and will fail here).

## Scope

- **Contacts:** `customer_type = B2B` (this single filter drops B2C, Partner,
  Vendor, Internal, Partner-B2B).
- **Companies:** `customer_type__cloned_ = B2B` (companies use the cloned
  property).

## What to pull each run

### 1. Funnel counts (B2B contacts per lifecycle stage)
One search per stage, `customer_type = B2B` + `lifecyclestage = <stage>`,
`limit 1`, read the `total`. Stages (internal → label):
`subscriber` Email Contact · `lead` Lead · `marketingqualifiedlead` MQL ·
`salesqualifiedlead` SQL · `opportunity` Opportunity · `customer` Customer ·
`other` Other. Also capture the overall B2B total (sanity: stage sum = total).

### 2. Deals (all)
Pull **all** deals with these properties:

```
dealname, dealstage, amount, deal_currency_code, amount_in_home_currency,
hs_lastmodifieddate, notes_last_updated, hubspot_owner_id
```

- **Currency:** the account home currency is USD. Sum/report deal *values*
  using `amount_in_home_currency` (USD). Keep raw `amount` + `deal_currency_code`
  only as a secondary reference; never sum raw `amount` across currencies. If
  `amount_in_home_currency` is empty on a deal, flag it (do not fall back to raw).
  > NOTE: the property is `amount_in_home_currency`. HubSpot's
  > `hs_deal_amount_in_home_currency` does **not** exist in this portal.
- **Owner:** resolve each deal's `hubspot_owner_id` to an owner name (owners
  lookup) and store it as `owner` on each deal in `raw_input.json`. This feeds
  the Owner column in the "What moved this week" table.

### 3. Miscategorised leads
B2B contacts where `hs_lead_status` ∈
{`Qualified`, `CONNECTED`, `Attempted to Contact`, `IN_PROGRESS`,
`PROPOSAL_REQUEST`} **and** `lifecyclestage` ∈ {`lead`, `marketingqualifiedlead`}.
Store `id, name, hs_lead_status, lifecyclestage`.

### 4. SQL contacts (engaged) — for the stalled-SQL follow-up list
B2B contacts where `lifecyclestage = salesqualifiedlead` **and**
`hs_lead_status` ∈ {`CONNECTED`, `Qualified`, `IN_PROGRESS`, `PROPOSAL_REQUEST`}.
Resolve `hubspot_owner_id` → owner name. Store
`id, name, hs_lead_status, notes_last_updated, owner`.

### 5. SQL companies (engaged) — same follow-up list
B2B companies (`customer_type__cloned_ = B2B`) where
`lifecyclestage = salesqualifiedlead` **and** `hs_lead_status` ∈
{`CONNECTED`, `Qualified`}. (Companies have no `IN_PROGRESS` / `PROPOSAL_REQUEST`
status; their "Attempted to Contact" is `ATTEMPTED_TO_CONTACT`.) Resolve owner.
Store `id, name, hs_lead_status, notes_last_updated, owner` in `sql_companies`.

## Owner ID → name map (as of 2026-09)

Resolve live via the owners lookup; this is a convenience reference only.

| ID | Name |
|----|------|
| 83228721 | Nicola Cole |
| 84004434 | Anup Ghoshal |
| 84004493 | Theo Zirogiannis |
| 87318764 | Jack Makhlouf |
| 87318803 | Biju Rajesh |

## `raw_input.json` shape

```json
{
  "source": "...", "pulled_at": "YYYY-MM-DD", "value_basis": "usd_home_currency",
  "home_currency": "USD",
  "contacts_b2b_total": <int>,
  "funnel_counts": { "<internal stage>": <int>, ... },
  "miscategorised": [ {"id","name","hs_lead_status","lifecyclestage"} ],
  "sql_contacts":   [ {"id","name","hs_lead_status","notes_last_updated","owner"} ],
  "sql_companies":  [ {"id","name","hs_lead_status","notes_last_updated","owner"} ],
  "deals": [ {"id","dealname","dealstage","amount","deal_currency_code",
              "amount_in_home_currency","notes_last_updated",
              "hs_lastmodifieddate","owner"} ]
}
```

## Build, convert, commit

```bash
python3 pipeline.py --input raw_input.json --date YYYY-MM-DD
pandoc reports/YYYY-MM-DD.md -o reports/YYYY-MM-DD.docx \
  --reference-doc=templates/pipeline_report_template.docx   # install pandoc if missing
git add -A && git commit && git push
```

Commit `raw_input.json`, `snapshots/YYYY-MM-DD.json`, `reports/YYYY-MM-DD.md`
and `.docx`, plus any script changes.

## Report section order (produced by `pipeline.py`)

1. **📈 What moved this week** (leads the report — the Tuesday leadership view;
   includes the Sales owner column)
2. 🔔 Alerts
3. Funnel
4. MQL vs SQL
5. Deals — count and value by stage (USD)
6. 🕓 Stalled engaged SQLs — follow-up / review

Section order and the Owner column are handled by `pipeline.py`; the routine
only has to supply deal `owner` in `raw_input.json` (step 2 above).
