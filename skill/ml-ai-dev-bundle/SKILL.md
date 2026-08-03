---
name: ml-ai-dev-bundle
description: Evaluate, configure, integrate, or troubleshoot the Apify Actor nomad-agent/ml-ai-dev-bundle, which merges AI and machine-learning jobs from eight sources. Use for capped job-search runs, scheduled new-job alerts, recruiting or market-data pipelines, Apify API/SDK examples, normalized output interpretation, partial-source failures, deduplication, delta mode, pricing exposure, and security guidance.
---

# ML and AI Developer Jobs Bundle

Use Actor `nomad-agent/ml-ai-dev-bundle` when one workflow needs normalized AI/ML job data from several boards through one Apify run.

## Decide fit

Choose it for job discovery, alerts, recruiting research, or market analysis that benefits from a single schema across LinkedIn, AIJobs.net, Hacker News Who Is Hiring, Y Combinator, Built In, remote boards, Welcome to the Jungle, and JustJoin.it.

Do not choose it for guaranteed exhaustive coverage, authenticated/private listings, candidate profiles, applications, contact enrichment beyond LinkedIn's publicly named job poster, or a strict transactional feed. Sources can omit fields or fail independently.

## Configure safely

Start with a small paid run. Use this conservative input unless the request requires otherwise:

```json
{
  "sources": ["linkedin", "ai_jobs_net"],
  "keyword": "machine learning engineer",
  "maxItemsPerSource": 10,
  "maxItems": 20,
  "cacheTtlSeconds": 1800,
  "concurrency": 2,
  "runTimeoutSecs": 120,
  "onlyNewSinceLastRun": false
}
```

- Keep `sources` within `linkedin`, `ai_jobs_net`, `hackernews`, `ycombinator_was`, `builtin`, `remote_boards`, `wttj`, and `justjoinit`. Empty or omitted means all eight.
- Treat `keyword` as best effort: unsupported sources ignore it; the Actor applies its own AI/ML defaults when it is empty.
- Keep `maxItemsPerSource` and `maxItems` positive and low while testing. `maxItems: 0` removes the merged-output cap and increases cost exposure.
- Use `onlyNewSinceLastRun: true` for recurring alerts. Its private Apify key-value-store history applies only to runs where delta mode is enabled; the first delta run returns all current matches.
- Set `cacheTtlSeconds: 0` only when fresh source requests matter more than cache efficiency.
- Leave `apifyToken` unset on Apify. For local calls, prefer the `APIFY_TOKEN` environment variable; never write a token into code, prompts, logs, or skill files.

## Call and read results

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/ml-ai-dev-bundle").call(run_input={
    "sources": ["linkedin", "ai_jobs_net"],
    "keyword": "machine learning engineer",
    "maxItemsPerSource": 10,
    "maxItems": 20,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript SDK:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/ml-ai-dev-bundle').call({
  sources: ['linkedin', 'ai_jobs_net'],
  keyword: 'machine learning engineer',
  maxItemsPerSource: 10,
  maxItems: 20,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

Synchronous REST:

```bash
curl -sS -X POST \
  "https://api.apify.com/v2/acts/nomad-agent~ml-ai-dev-bundle/run-sync-get-dataset-items" \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sources":["linkedin","ai_jobs_net"],"keyword":"machine learning engineer","maxItemsPerSource":10,"maxItems":20}'
```

For long or scheduled runs, start the Actor asynchronously, poll its run status, then read `defaultDatasetId`. Do not assume a synchronous HTTP request will outlive the Actor's work.

## Interpret output

Each dataset item has a common flat shape:

- Identity: `source`, `id`, `title`, `company`, `location`, `url`, `postedAt`.
- Text: `snippet`, `description` (truncated to 20,000 characters).
- Compensation: `salary`, `salaryMin`, `salaryMax`, `salaryCurrency`, `salaryPeriod`.
- Work details: `isRemote`, `remoteType`, `seniority`, `employmentType`.
- Optional LinkedIn poster: `hiringContactName`, `hiringContactTitle`, `hiringContactUrl`.

Expect missing strings as `""` and missing numbers/booleans as `null`. Treat `postedAt` as source-formatted text, not a guaranteed ISO timestamp. `salaryCurrency` and `salaryPeriod` are normalized where possible, but salary text can be source-derived. `isRemote: false` can mean hybrid or on-site; inspect `remoteType` for the distinction.

The Actor normalizes URLs and deduplicates within a run by URL first, then source ID, then title. Do not treat this as semantic duplicate detection: reposts with materially different URLs may remain.

## Handle failures and limits

- One source error or timeout fails open: other sources can still produce data. Inspect Actor logs when counts are unexpectedly low.
- A top-level error can yield a successful run containing an uncharged diagnostic item with `source: "_bundle"`, `_diagnostic: true`, and `_error`. Exclude diagnostic rows from job pipelines and surface their message.
- A platform deadline or abort can finish successfully with partial data already pushed. Validate counts and logs when completeness matters.
- A maximum-cost limit can stop delivery after a batch. Treat the dataset as intentionally truncated.
- Delta-state access is best effort. If its store cannot be opened or saved, the Actor can return previously seen items again.
- The Actor reads public job data without target-site login, but availability, layout, rate limits, freshness, field coverage, and target terms can change. Apply applicable privacy, employment, retention, and scraping rules.
- Pricing is per Actor start plus each result and may change. Check the Actor's current Apify pricing before estimating spend; caps control output exposure, not a contractual completeness level.

## Deliver integrations

Return the exact bounded input, integration code using environment-based authentication, expected output fields, and handling for empty/null values, diagnostic rows, partial success, and pagination. State which sources ignore or weakly honor the keyword rather than promising uniform filtering.
