---
name: american-jobs-bundle
description: Assess, configure, run, and integrate the Apify Actor nomad-agent/american-jobs-bundle, which merges six US-focused job-source scrapers into one deduplicated dataset. Use for US job search, alerts, recruiting feeds, Actor input and cost-cap design, Apify API or SDK integration, normalized output interpretation, incremental runs, and source-failure diagnosis.
---

# American Jobs Bundle

Use Actor `nomad-agent/american-jobs-bundle` for one normalized feed across LinkedIn US, AI Jobs, Built In, four remote boards, Hacker News Who Is Hiring, and Y Combinator Work at a Startup.

## Assess fit

- Choose it for broad US/remote discovery without maintaining six integrations.
- Choose a source subset when speed, cost, or topical precision matters.
- Do not promise exhaustive coverage, guaranteed freshness, application automation, company ATS crawling, or reliable hiring contacts. Sites can change or rate-limit scraping.
- Explain that Built In ignores `keyword`; only LinkedIn and AI Jobs use `location`. Hiring-contact fields come only from LinkedIn when the public posting exposes them.

## Configure a bounded run

Start small and expand only when the sample is useful:

```json
{
  "sources": ["linkedin", "ai_jobs_net"],
  "keyword": "software engineer",
  "location": "United States",
  "maxItemsPerSource": 10,
  "maxItems": 20,
  "cacheTtlSeconds": 1800,
  "concurrency": 2,
  "runTimeoutSecs": 300,
  "incrementalMode": false
}
```

Valid source keys are `linkedin`, `ai_jobs_net`, `builtin`, `remote_boards`, `hackernews`, and `ycombinator_was`. Omitting `sources` uses all six. Defaults are 36 items per source and 216 total. `maxItems: 0` removes the total cap, so never use it without explicit user intent and an Apify maximum-cost-per-run limit. Keep `concurrency` from 1 to 6 and `runTimeoutSecs` at least 30.

Use `incrementalMode: true` for recurring alerts. Its private named key-value store remembers delivered dedupe keys for that Apify account; the first incremental run returns all current matches, state is best-effort, and child sources still fetch on every run. Use `cacheTtlSeconds: 0` only when fresh fetching is worth the added work.

Do not set legacy `actorOwner`; it is ignored. Do not place `apifyToken` in source code, chat output, logs, URLs, or committed input. On Apify it is injected automatically; outside Apify, prefer the `APIFY_TOKEN` environment variable.

## Run and integrate

Use the Apify Console for exploration. For production, keep the token in a secret manager and use the SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/american-jobs-bundle").call(run_input={
    "sources": ["linkedin", "ai_jobs_net"],
    "keyword": "software engineer",
    "maxItemsPerSource": 10,
    "maxItems": 20,
})
items = client.dataset(run["defaultDatasetId"]).list_items().items
```

Equivalent REST flow:

```bash
curl -X POST \
  "https://api.apify.com/v2/acts/nomad-agent~american-jobs-bundle/run-sync-get-dataset-items" \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sources":["linkedin"],"keyword":"engineer","maxItemsPerSource":10,"maxItems":10}'
```

For runs that may exceed the synchronous endpoint's wait window, start an asynchronous run, poll it to a terminal status, then fetch `defaultDatasetId`. Preserve the caps and set a platform maximum cost per run.

## Interpret results and failures

Normal records contain `source`, `id`, `title`, `company`, `location`, `url`, `postedAt`, `deadline`, `snippet`, `salary`, and nullable LinkedIn contact fields. Treat empty strings as unavailable source data, not verified negatives. Date formats vary by source. Deduplication prefers a normalized URL, then ID, then title; it is useful but not semantic duplicate detection.

Check every row for `warning`. An unexpected bundle-level error can deliberately finish as `SUCCEEDED` with a diagnostic warning row, while an individual source error is logged and other sources continue. Therefore, do not equate `SUCCEEDED` with complete six-source coverage: inspect logs, source distribution, warning rows, item count, and truncation/cost-cap messages. A zero-row result can be a valid narrow query, an incremental run with no new URLs, an empty source selection, or upstream failures.

Report the Actor run ID, dataset ID, selected inputs, result count by `source`, warning rows, and whether `maxItems` or the cost limit truncated delivery. Never echo credentials.
