---
name: un-careers-scraper
description: Evaluate, configure, run, or integrate the Apify Actor nomad-agent/un-careers-scraper for current United Nations Secretariat vacancies from careers.un.org. Use for selecting UN Careers filters, creating capped or scheduled delta runs, consuming datasets through Apify REST or SDKs, interpreting job and diagnostic rows, estimating pay-per-event cost, or explaining coverage, caching, partial results, and source limitations.
---

# UN Careers Scraper

Use Actor `nomad-agent/un-careers-scraper`. It returns public vacancies from the UN Secretariat portal at careers.un.org. It does not cover every UN-system agency, rank candidates, apply to jobs, or scrape authenticated data.

## Configure a run

Start with a small paid run:

```json
{
  "keyword": "human rights",
  "categories": ["PD"],
  "jobFamilies": ["HRI"],
  "locationFilter": "Geneva",
  "maxItems": 25,
  "includeDescription": true,
  "sortDirection": "newest",
  "cacheTtlSeconds": 1800,
  "onlyNewSinceLastRun": false
}
```

- Use `keyword` for server-side free-text search.
- Use category codes `PD`, `GS`, `NPO`, `FS`, `INT`, or `CON` in `categories`.
- Use job-family codes from the Actor schema in `jobFamilies`, such as `PGM`, `HRI`, `HRA`, `IST`, `ECO`, or `POL`.
- Use network codes `DEVNET`, `INFONET`, `ITECNET`, `LEGALNET`, `LOGNET`, `MAGNET`, `POLNET`, `SAFETYNET`, or `SCINET` in `networks`.
- Use `locationFilter` for a case-insensitive substring match on the primary duty station. This filter is client-side, not geospatial.
- Set `maxItems` to a non-negative integer. The default is `100`; `0` means uncapped within the Actor's pagination safety ceiling. A run's maximum-charge budget can stop delivery earlier.
- Keep `includeDescription: true` for full plain-text descriptions. Set it to `false` for smaller records.
- Set `sortDirection` to `newest` or `oldest`.
- Keep the default `cacheTtlSeconds: 1800` for repeat runs; set `0` to fetch live.

For alerts, set `onlyNewSinceLastRun: true`. The first flagged run emits every matching current vacancy; later flagged runs omit IDs successfully delivered by earlier flagged runs. Emitted delta rows have `isNew: true`. The named seen-state store is shared by runs of this Actor and is best-effort, so separate consumers do not get independent cursors.

## Run through Apify

Keep the token in `APIFY_TOKEN`. Never embed it in source, prompts, logs, committed files, or URL query parameters.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/un-careers-scraper").call(
    run_input={"keyword": "data", "maxItems": 25}
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/un-careers-scraper').call({
  keyword: 'data',
  maxItems: 25,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

REST, for bounded synchronous runs:

```bash
curl --fail-with-body -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"keyword":"data","maxItems":25}' \
  'https://api.apify.com/v2/acts/nomad-agent~un-careers-scraper/run-sync-get-dataset-items'
```

Prefer an asynchronous run plus dataset polling when the caller cannot tolerate the synchronous endpoint's wait or response-size limits. Read `defaultDatasetId` after the run reaches a successful terminal state.

## Consume output safely

Normal rows contain:

- `jobId`, `title`, `company`, `department`, `location`, and `url`
- `category`, `categoryCode`, `jobFamily`, `jobFamilyCode`, `network`, and `networkCode`
- `level`, `recruitmentType`, `postedAt`, and `deadline`
- `description` when `includeDescription` is true
- `isNew` only on emitted delta rows

Treat source-provided fields as nullable and dates as source strings. `company` is always `United Nations`; use `department` for the hiring office. The URL is built from `jobId` and points to the careers.un.org detail page.

Do not assume every successful run contains job rows. A first-page upstream or unexpected failure deliberately produces an unbilled diagnostic row containing `warning` and `docs` while the run remains successful. A normal empty or no-new result can use the same shape. Branch on `warning` before validating a row as a vacancy. Later-page failures, deadlines, filters, caching, and charge limits can produce partial or zero results; inspect status, logs, and dataset shape together.

## Assess fit and cost

- Choose this Actor for UN Secretariat departments, offices, peace operations, and regional commissions recruiting through careers.un.org.
- Do not claim direct coverage of agency-specific systems such as UNICEF, WHO, UNDP, UNHCR, IMF, or the World Bank. Use separate sources or a multi-source Actor for wider international-organisation coverage.
- Pricing is pay per event: `$0.005` per Actor start and `$0.006` per delivered job. Estimate cost as `0.005 + 0.006 * delivered_jobs`; verify current Store pricing before making a budget commitment.
- Delta mode is the economical scheduled-monitoring path because already-seen vacancies are neither emitted nor billed as results.
- Treat keyword and taxonomy filters as the source portal's behavior, not normalized semantic matching. Validate deadlines and links before acting on a vacancy.
- Respect careers.un.org terms, applicable law, privacy obligations, and downstream redistribution rules.
