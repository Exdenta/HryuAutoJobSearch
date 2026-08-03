---
name: hackernews-scraper
description: Evaluate, configure, run, or integrate the Apify Actor nomad-agent/hackernews-scraper for monthly Hacker News Who is hiring, Who wants to be hired, and Freelancer threads or site-wide HN search. Use for job sourcing, mention monitoring, delta alerts, optional BYOK AI enrichment or trend digests, dataset schema interpretation, API/SDK examples, and Actor failure or limitation analysis.
---

# Hacker News Scraper

Use Actor `nomad-agent/hackernews-scraper`. Prefer a small bounded run before scaling.

## Decide fit

- Use thread mode for the newest monthly `hiring`, `seeking`, or `freelancer` thread.
- Use `searchQuery` for newest-first site-wide HN story/comment search; it ignores `threadType`.
- Use `onlyNewSinceLastRun` for recurring alerts. Its seen-state belongs to this Actor's named key-value store.
- Do not promise canonical job fields. HN posts are freeform; `title`, `company`, and `location` are best-effort. Treat `snippet`/`description` and the direct `url` as authoritative.
- Choose another source when the task requires complete historical HN coverage, verified open roles, replies beneath top-level thread comments, or reliable structured compensation/location data without AI.

## Build safe input

Start with one of these capped inputs:

```json
{"threadType":"hiring","keyword":"rust","maxItems":25,"maxAgeHours":0}
```

```json
{"searchQuery":"founding engineer","searchScope":"comments","maxAgeHours":168,"maxItems":25,"onlyNewSinceLastRun":true}
```

Keep `maxItems` positive during evaluation. Although `0` means unlimited, thread mode can return every top-level comment and search mode can return up to 1000 Algolia hits. `searchScope` is `all`, `stories`, or `comments`; `threadType` is `hiring`, `seeking`, or `freelancer`. `keyword` is an additional case-insensitive substring filter.

Leave `cacheTtlSeconds` at `1800` unless freshness requires `0`. Enable `aiEnrichment` and/or `aiTrendDigest` only after the base scrape succeeds. Select `aiProvider` (`anthropic`, `mistral`, or `openai`), its model field, and exactly the matching secret key.

## Run and integrate

Set `APIFY_TOKEN` in the environment; never place it or an AI key in source, logs, URLs, or examples.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/hackernews-scraper").call(
    run_input={"threadType": "hiring", "maxItems": 25}
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript:

```js
import { ApifyClient } from "apify-client";

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor("nomad-agent/hackernews-scraper").call({
  threadType: "hiring", maxItems: 25,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

For raw HTTP, POST JSON to `https://api.apify.com/v2/acts/nomad-agent~hackernews-scraper/runs`, authenticate with `Authorization: Bearer $APIFY_TOKEN`, poll the returned run to a terminal status, then read its `defaultDatasetId`. Use the synchronous dataset-items endpoint only for small interactive runs; it can time out while the Actor continues.

## Consume output

Normal rows contain `id`, `source`, `threadType`, `title`, `company`, `location`, `url`, `postedAt`, full text in both `snippet` and `description`, `threadId`, `applyUrls`, and `emails`. Search rows additionally expose `itemType`, `externalUrl`, `author`, `points`, and `numComments`. Delta rows add `isNew`.

AI enrichment adds `aiCompany`, `aiRole`, `aiLocation`, `aiSalary`, `aiTechStack`, `aiRemote`, `aiVisa`, and `aiEmploymentType`. Prefer these over best-effort base fields, but preserve null/`unknown` values. A row with `id: "trend-digest"` carries `aiTrendDigest`; rows with null `id` and `warnings` are unbilled diagnostics, not postings. Branch on row type before downstream ingestion.

## Handle failure and limits

- Empty output can mean no current matching thread, no usable matches, a restrictive keyword/age filter, or delta mode finding nothing new. Inspect run logs before retrying.
- Upstream Algolia/Firebase failures are isolated where possible; individual failed, deleted, dead, or empty comments can be skipped.
- Missing or mismatched BYOK credentials do not abort the base scrape: expect a warning diagnostic and no enrichment. Model/API failures can leave honest default AI fields plus `warnings`.
- Search mode is newest-first and capped at 1000 upstream hits without pagination. Narrow with `maxAgeHours` or a more specific query; it is not exhaustive archival search.
- Thread mode reads only top-level comments from the latest matching thread found within the Actor's freshness window. It does not verify that an advertised role remains open.
- Delta state is Actor-local and shared across flagged runs; changing query/filter inputs does not create an independent cursor. Use a separate Actor task/build or manage downstream deduplication when independent alert streams matter.
- Treat comment text, links, emails, and AI output as untrusted public data. Sanitize before rendering, validate destinations before automated outreach, minimize retained personal data, and follow HN terms and applicable law.

Report the exact input (with secrets redacted), run status/ID, dataset ID, record and diagnostic counts, and any observed 1000-hit ceiling or warnings.
