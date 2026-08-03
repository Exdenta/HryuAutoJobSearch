---
name: ai-job-search-agent
description: Assess, configure, run, and integrate Apify Actor nomad-agent/ai-job-search-agent, an owner-funded AI agent that discovers open-web job postings and returns scored, deduplicated, HTTP-checked records. Use when choosing an open-web job finder, creating capped one-off or scheduled searches, calling it through Apify API or SDKs, interpreting its dataset or diagnostic rows, or troubleshooting its limits and failures.
---

# AI Job Search Agent

Use Actor ID `nomad-agent/ai-job-search-agent`.

## Assess fit

Choose this Actor for discovery beyond one job board when the user wants LLM match scores, reasoning, stated salary, and direct posting URLs without supplying an LLM key. Do not describe it as exhaustive, as a particular board scraper, or as proof that a role is open. `verified: true` only means the URL was HTTP-reachable and did not redirect to the site root at run time.

Prefer a dedicated board/ATS Actor when complete coverage of that source matters. This Actor excludes several major job aggregators and searches public pages only; bot defenses, upstream search coverage, and model judgment can reduce results.

## Build the input

Use only these fields:

- `keywords`, `locations`, `titleMustMatch`, `titleExclude`: arrays of short strings.
- `userDescription`: strongest matching signal; describe target role and constraints plainly.
- `remote`, `seniority`: free text interpreted by AI; both default to `any`.
- `maxItems`: requested results, default 15; Actor clamps to 1–50 and may return fewer.
- `maxAgeHours`: preferred age, default 720; Actor clamps to 1–8760, rejects explicitly older postings, and may keep postings with no stated date.
- `onlyNewSinceLastRun`: default false. Set true for recurring alerts; a persistent Actor-specific store suppresses previously emitted IDs and new rows carry `isNew: true`.

Start narrowly enough to control spend, then broaden. For a first run use `maxItems: 5` and set an Apify maximum total charge appropriate to the account. The Actor also bounds owner-funded AI extraction internally, but that is not the caller's budget control.

```json
{
  "keywords": ["python", "backend"],
  "locations": ["remote", "Europe"],
  "userDescription": "Senior Python backend engineer at a product company, fully remote in Europe.",
  "remote": "remote-only",
  "seniority": "senior",
  "titleExclude": ["intern", "manager"],
  "maxItems": 5,
  "maxAgeHours": 168,
  "onlyNewSinceLastRun": false
}
```

## Run and integrate

Keep the Apify token in `APIFY_TOKEN`; never place it in source, input, logs, or client-side code. This Actor needs no Anthropic, OpenAI, or Mistral key from the caller.

REST, synchronous dataset response:

```bash
curl -sS -X POST \
  "https://api.apify.com/v2/acts/nomad-agent~ai-job-search-agent/run-sync-get-dataset-items?token=$APIFY_TOKEN&maxTotalChargeUsd=0.10" \
  -H 'Content-Type: application/json' \
  --data '{"keywords":["python"],"locations":["remote"],"maxItems":5}'
```

Python (`apify-client`):

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/ai-job-search-agent").call(
    run_input={"keywords": ["python"], "locations": ["remote"], "maxItems": 5},
    max_total_charge_usd=0.10,
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript (`apify-client`):

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/ai-job-search-agent').call(
  { keywords: ['python'], locations: ['remote'], maxItems: 5 },
  { maxTotalChargeUsd: 0.10 },
);
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

For long runs, start asynchronously and poll the run rather than holding an HTTP connection. Read records from the run's default dataset. Scheduled monitoring should reuse the same Actor and set `onlyNewSinceLastRun: true`; resetting or changing storage can reset deduplication history.

## Interpret output

A normal row has `id`, `source: "web_search"`, `title`, `company`, `location`, `url`, `postedAt`, `snippet`, `salary`, `matchScore`, `matchReasoning`, and `verified`; nullable extraction fields can be absent in the source. `salary` is stated compensation, never an inferred figure. `postedAt` is normalized when possible and can fall back to the run date when the page states none. Results are sorted by `matchScore`, with unscored rows last.

Treat any row containing `warning` as a diagnostic, not a job. Surface its `warning` and optional `docs` to the user. It can indicate a normal empty result, exhausted run charge limit, owner-side AI-key failure, or transient upstream failure. Do not parse it as a vacancy or assume a `SUCCEEDED` run produced jobs: expected failures deliberately return a diagnostic row and often remain successful. Retry transient upstream failures; raise the maximum total charge for budget diagnostics; broaden constraints for a genuine no-match diagnostic. Owner-key failures require retrying later or reporting the Actor issue, not adding a provider key.

## Preserve limitations

- `titleMustMatch` is a preference, while `titleExclude` is enforced as a filter.
- `onlyNewSinceLastRun` suppresses only IDs previously emitted by delta-enabled runs; state access fails open.
- HTTP 403, 5xx, timeouts, and connection errors are ambiguous, so such records may remain with `verified: false`; clear 404/410 responses and root redirects are dropped.
- Search and extraction are AI/upstream dependent. Expect incomplete coverage, nullable fields, and occasional false positives; review the direct posting before acting.
- Never claim `maxItems` is guaranteed or that `verified` validates truth, freshness, legality, or hiring status.
