---
name: impactpool-scraper
description: Evaluate, configure, run, or integrate the Apify Actor nomad-agent/impactpool-scraper for public Impactpool.org UN, NGO, development-bank, EU-institution, and international-development vacancies. Use for deciding whether the Actor fits a job-search or monitoring workflow, selecting cost-bounded inputs, consuming dataset records through the Apify API or Python/JavaScript clients, and interpreting detail enrichment, delta state, diagnostic rows, partial results, and source limitations.
---

# Impactpool Scraper

Use Actor `nomad-agent/impactpool-scraper`. It returns public Impactpool vacancies; it does not rank candidates, apply to jobs, scrape authenticated data, or combine other job boards.

## Configure a run

Start conservatively. Use `maxItems` between 1 and 100 for exploration; the Actor clamps it to 0–1000, while `0` means unlimited within a 30-page safety cap. The run's maximum-charge budget can lower the effective result cap.

```json
{
  "keyword": "programme manager",
  "locationFilter": "Geneva",
  "maxItems": 50,
  "includeDetails": true,
  "onlyNewSinceLastRun": false,
  "resetSeenJobs": false,
  "cacheTtlSeconds": 1800
}
```

- Use `keyword` for Impactpool search and `locationFilter` for a case-insensitive duty-station substring filter.
- Keep `includeDetails: true` when descriptions and deadlines matter. It adds one detail request per candidate; postings with descriptions under 300 characters are dropped and can be retried later. Set it to `false` for faster listing-only discovery.
- Use `onlyNewSinceLastRun: true` for scheduled alerts. Seen IDs persist in the Actor's named key-value store and are recorded only after successful output. Use `resetSeenJobs: true` only when intentionally restarting that history.
- Keep caching enabled for repeat runs. Set `cacheTtlSeconds: 0` only when a live fetch is necessary.

## Integrate

Never embed an Apify token in source, logs, prompts, or URLs. Read it from `APIFY_TOKEN` and send it as a bearer token.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/impactpool-scraper").call(run_input={"maxItems": 50})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/impactpool-scraper').call({ maxItems: 50 });
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

Raw HTTP, for bounded synchronous jobs:

```bash
curl --fail-with-body -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"maxItems":50}' \
  'https://api.apify.com/v2/acts/nomad-agent~impactpool-scraper/run-sync-get-dataset-items'
```

Prefer an asynchronous Actor run plus dataset polling when the client cannot tolerate the synchronous endpoint's wait or response-size limits.

## Consume output safely

Normal rows have `id`, `title`, `company`, `location`, `seniority`, `url`, `snippet`, `postedAt`, `deadline`, `description`, `organizationUrl`, `organizationId`, and `source` (`impactpool`). Treat fields as nullable. `postedAt` is always `null` because Impactpool does not publish it. Detail fields are normally `null` when enrichment is disabled; a failed detail fetch can also leave them absent while preserving listing data.

Do not assume every successful run contains jobs. On a page-one or unexpected failure, the Actor deliberately succeeds with an unbilled diagnostic row shaped like `{"warning": "...", "source": "impactpool"}`. Branch on `warning` before validating a row as a job. Later-page failures, time budgets, charge limits, deduplication, filters, or thin descriptions can yield partial or zero results; inspect run logs and item count before declaring the source empty.

## Fit and limitations

- Choose this Actor for Impactpool-focused vacancy discovery, exports, alerts, and downstream matching. Choose a multi-source Actor when broad market coverage is required.
- Expect public listing HTML to change. Missing cards can mean no matches, a narrow filter, or parser drift.
- `locationFilter` is textual, not geospatial. Seniority and organisation values are source labels, not normalized taxonomies.
- Delta state is Actor-scoped and best-effort. Resetting it redelivers current postings; separate consumers sharing the same Actor also share its seen history.
- Detail fetches fail open, but the default full-detail mode drops unusably thin bodies. Results are current vacancies, not a guaranteed complete historical archive.
- Respect Impactpool's terms, robots/access policies, personal-data obligations, and applicable law. Store only data needed for the stated workflow and validate links before acting on a vacancy.
