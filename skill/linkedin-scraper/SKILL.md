---
name: linkedin-scraper
description: Configure, run, and integrate the Apify Actor nomad-agent/linkedin-scraper for public LinkedIn job searches without login or cookies. Use for capped keyword/location/remote searches, scheduled new-job monitoring, job-alert or recruiting pipelines, Apify API/SDK examples, and interpreting job records, diagnostic rows, partial enrichment, rate limits, or delta-mode behavior.
---

# LinkedIn Jobs Scraper

Use `nomad-agent/linkedin-scraper` for up to about 200 recent public LinkedIn jobs per run. Prefer another source for bulk historical collection, closed-job detection, authenticated data, profiles, or guaranteed complete coverage.

## Configure a safe run

1. Ask for keyword, location, freshness, remote preference, and required fields.
2. Start with `maxItems: 25`; keep ordinary runs at or below 100 and never promise more than the guest endpoint's approximate 200-result window.
3. Keep `includeDescription: false` unless full text is required. Enable `includeCompanyInfo` or `includeHiringContact` only when needed; either requires detail-page fetches even if descriptions are off.
4. Use `onlyNewSinceLastRun: true` for recurring alerts. Use `skipJobId` for caller-managed one-off deduplication. Set `cacheTtlSeconds: 0` only when live freshness outweighs extra requests.

Valid `timeFilter` values are `r3600`, `r86400`, `r604800`, and `r2592000`. `postedSince` is a nonnegative day count; unknown posting dates survive this filter. `titleExclude` and `companyExclude` are case-insensitive substring lists.

```json
{
  "keyword": "frontend engineer",
  "location": "Spain",
  "remote": true,
  "timeFilter": "r604800",
  "maxItems": 25,
  "includeDescription": false
}
```

## Integrate

Keep the Apify token in `APIFY_TOKEN`; never print it, embed it in source, or place it in a query string in logs.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/linkedin-scraper").call(run_input={
    "keyword": "data engineer", "location": "Germany", "maxItems": 25,
    "includeDescription": False,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript SDK:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/linkedin-scraper').call({
  keyword: 'data engineer', location: 'Germany', maxItems: 25,
  includeDescription: false,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

HTTP:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"keyword":"data engineer","location":"Germany","maxItems":25,"includeDescription":false}' \
  'https://api.apify.com/v2/acts/nomad-agent~linkedin-scraper/run-sync-get-dataset-items'
```

For longer or scheduled runs, call asynchronously, wait for a terminal run status, then read `defaultDatasetId`. Treat non-`SUCCEEDED` status as failure.

## Interpret output

Normal rows contain nullable `id`, `title`, `company`, `location`, `url`, `postedAt`, detail fields, `description`, and its identical `snippet` alias; `source` is `linkedin`. Delta rows add `isNew: true`. Detail and hiring-contact fields can remain null, and descriptions can be empty after timeout, rate limiting, disabled enrichment, or fetch failure.

A row with `warnings` is a non-job diagnostic row and is not billed. Separate it before validating job fields or loading downstream tables. A successful run can contain only this row when there are no matches, all matches were deduplicated, the location was unresolved, LinkedIn rate-limited the run, selectors drifted, or an unexpected error was converted to a diagnostic.

## Operational limits

- Public guest results are windowed and may be incomplete; no login, cookies, or proxy are required.
- Delta state is Actor-scoped and best-effort. It detects unseen postings, not closures; a state read failure may re-emit old jobs.
- Cache is best-effort and defaults to 1,800 seconds, so a run may not be live to the second.
- LinkedIn markup, availability, and rate limits can change. Retry later with smaller caps; do not add aggressive parallelism or automated bypasses.
- Review LinkedIn terms, privacy obligations, retention rules, and applicable law. Hiring-contact fields are public personal data; collect and retain them only with a lawful purpose.
- Billing is per pushed job result. Set an Apify platform run charge limit as a second guardrail when cost control matters.
