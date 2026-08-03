---
name: wellfound-scraper
description: Configure, run, and integrate the Apify Actor nomad-agent/wellfound-scraper for public Wellfound startup jobs. Use for keyword searches, remote/job-type/salary/freshness filters, scheduled new-job monitoring, Apify API or SDK setup, and interpreting structured job records, missing fields, Residential-proxy access errors, DataDome challenges, or diagnostic rows.
---

# Wellfound Jobs Scraper

Use `nomad-agent/wellfound-scraper` to collect public Wellfound job postings. Treat results as best-effort: Wellfound's DataDome protection can challenge listing or detail pages even through the required browser and proxy path.

## Configure a run

1. Ask for keyword, result cap, freshness, remote preference, job type, salary bounds, and whether the run repeats.
2. Start with `maxItems: 30`. Keep the default `proxyConfiguration` using Apify's `RESIDENTIAL` group; it requires a paid Apify plan and separately billed proxy bandwidth.
3. Set `onlyNewSinceLastRun: true` for recurring alerts. Its named Key-Value Store is Actor-scoped and best-effort; the first run returns all matching unseen jobs.
4. Use `debug: true` only to investigate a zero-link run. It stores `DEBUG_LISTING_HTML` and `DEBUG_LISTING_PNG` in that run's default Key-Value Store.

```json
{
  "keyword": "frontend engineer",
  "maxItems": 30,
  "postedSince": 7,
  "remoteOnly": true,
  "jobType": "Full-time",
  "salaryMin": 100000,
  "onlyNewSinceLastRun": true,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

Use only these `jobType` inputs: `""`, `Full-time`, `Part-time`, `Contract`, `Internship`, or `Temporary`. `titleExclude` and `companyExclude` are case-insensitive substring arrays. `postedSince`, `salaryMin`, and `salaryMax` keep records when Wellfound did not expose the field; `remoteOnly` instead requires `isRemote: true`. Salary filters compare `salaryMin` to the posting's upper bound and `salaryMax` to its lower bound.

Leave `startUrl` as `https://wellfound.com/jobs` and `detailUrlContains` as `/jobs/` unless using a specific Wellfound listing URL or adapting to a URL-structure change. Do not combine expectations around `proxyConfiguration` with deprecated `useResidentialProxy`; the legacy boolean applies only when `proxyConfiguration` is absent.

## Integrate

Keep the Apify token in `APIFY_TOKEN`; never print it, commit it, or put it in a logged query string.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/wellfound-scraper").call(run_input={
    "keyword": "backend engineer",
    "maxItems": 30,
    "remoteOnly": True,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript SDK:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/wellfound-scraper').call({
  keyword: 'backend engineer', maxItems: 30, remoteOnly: true,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

HTTP:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"keyword":"backend engineer","maxItems":30,"remoteOnly":true}' \
  'https://api.apify.com/v2/acts/nomad-agent~wellfound-scraper/run-sync-get-dataset-items'
```

For schedules or longer integrations, start the run asynchronously, wait for a terminal status, then read `defaultDatasetId`. Apply an Apify maximum-total-charge limit when cost control matters. Published pricing is one Actor-start event plus one event per returned job, with Residential bandwidth billed separately.

## Interpret output

Normal rows contain nullable `id`, `title`, `company`, `location`, `url`, `postedAt`, `snippet`, `salary`, `salaryMin`, `salaryMax`, `salaryCurrency`, `isRemote`, `jobType`, `equity`, and `companyLogo`; `source` is `wellfound`. `snippet` is the full plain-text JSON-LD description when available. Meta-fallback rows can contain only title, URL, snippet, source, and ID while the other job fields are null.

A row with `warning` is a non-job diagnostic row and is not billed as a result. Separate diagnostic rows before validating job fields or loading a downstream table. A successful run can contain only a diagnostic row when Residential access is unavailable, the listing cannot load, filters or delta state remove every job, details are challenged, the run budget is exhausted before a result, or an unexpected error is converted to a diagnostic.

## Respect limits

- Do not promise complete coverage, a precise posting count, or stable extraction through DataDome.
- Do not describe delta mode as closure detection; it remembers delivered posting IDs and does not report removed jobs.
- Do not claim that direct or datacenter access works. A caller may supply another proxy choice, but the Actor documents Residential as the working default.
- Review Wellfound's terms and applicable privacy, employment, and scraping laws before production use.
