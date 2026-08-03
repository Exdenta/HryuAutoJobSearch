---
name: lever-jobs-scraper
description: Configure, run, integrate, and troubleshoot the Apify Actor nomad-agent/lever-jobs-scraper, which fetches live postings from company Lever boards. Use for deciding whether the Actor fits a job-monitoring or aggregation workflow; choosing safe capped inputs; calling it through Apify Console, API, Python SDK, or JavaScript SDK; interpreting posting and diagnostic rows; and explaining delta mode, BYOK AI add-ons, costs, failures, security, and source limitations.
---

# Lever Jobs Scraper

Use Actor `nomad-agent/lever-jobs-scraper`. It queries Lever's public postings API live; it is not a general web crawler or a cross-ATS search index.

## Decide fit

Use it when the user has company Lever slugs, `jobs.lever.co` board URLs, or careers pages that link to Lever and needs structured current postings. Prefer another Actor when the companies use another ATS, discovery must span unknown ATSs, or historical/closed postings are required.

## Configure a bounded run

Start with the smallest useful input. Always set positive caps for automation unless the user explicitly accepts an unbounded run.

```json
{
  "companies": ["palantir", "spotify"],
  "keyword": "engineer",
  "remoteOnly": true,
  "includeDescription": false,
  "outputProfile": "minimal",
  "maxItemsPerCompany": 25,
  "maxItems": 50,
  "concurrency": 4
}
```

- Accept slugs, full Lever board URLs, or careers-page URLs in `companies`. Empty input runs the Actor's built-in sample boards; do not rely on that in production.
- Filter with `keyword`, `titleExclude`, `locationFilter`, `postedSince`, and `remoteOnly`. All text filters are case-insensitive substring matches. `postedSince` excludes records whose source date is missing.
- Choose `outputProfile`: `full`, `compact` (no `snippet`/`raw`), or `minimal` (essential fields). Keep `includeRawJson: false` unless unmapped Lever fields are needed.
- Treat `maxItemsPerCompany: 0` and `maxItems: 0` as unlimited. The Actor clamps `concurrency` to 1-16.
- Use `onlyNewSinceLastRun: true` for a recurring monitor. The first flagged run emits every current match as `isNew: true`; later flagged runs omit previously emitted IDs. Runs with the flag off do not touch delta state.

## Run and consume

Use Apify token authentication. Never paste a real token into source, logs, prompts, or output; load it from `APIFY_TOKEN` or a secret manager.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/lever-jobs-scraper").call(run_input={
    "companies": ["palantir"],
    "maxItemsPerCompany": 25,
    "maxItems": 25,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript SDK:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/lever-jobs-scraper').call({
  companies: ['palantir'], maxItemsPerCompany: 25, maxItems: 25,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

Synchronous REST call:

```bash
curl -sS -X POST \
  "https://api.apify.com/v2/acts/nomad-agent~lever-jobs-scraper/run-sync-get-dataset-items" \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"companies":["palantir"],"maxItemsPerCompany":25,"maxItems":25}'
```

For larger runs, start asynchronously, poll the run to a terminal status, then read `defaultDatasetId`; synchronous endpoints can time out before a long run finishes.

## Interpret output

Normal rows include `ats`, `company`, `id`, `title`, `department`, `location`, `url`, `postedAt`, `employmentType`, `remote`, salary fields, `globalId`, and `warnings`. `snippet` depends on profile/input. `remote` is tri-state: `true` for Lever remote, `false` for a stated non-remote workplace type, and `null` when Lever provides none. Salary and posting date are often absent because Lever boards do not consistently publish them.

Do not assume every dataset row is a job. Diagnostic rows have null posting fields and a populated `warnings` array for not-found companies, fetch failures, truncation, a skipped AI add-on, or a run-level error. Separate rows with a non-null `id`/`url` from diagnostic rows and surface their warnings. A run may intentionally finish successfully with diagnostics or partial data.

## Add AI only when requested

The optional flags `aiEnrichment`, `translateToEnglish`, and `companyEnrichment` are BYOK. Select `aiProvider` from `anthropic`, `mistral`, or `openai` and provide the matching key through Apify's secret input/environment support. Provider charges are separate from Actor charges. Apply caps before enabling AI.

- Anthropic uses `anthropicApiKey`/`ANTHROPIC_API_KEY` and `aiModel`.
- Mistral uses `mistralApiKey`/`MISTRAL_API_KEY` and `mistralModel`.
- OpenAI uses `openaiApiKey`/`OPENAI_API_KEY` and `openaiModel`.

Never print, persist, return, or commit provider keys. Missing keys cause a diagnostic row while ordinary postings still return. Treat AI fields and translations as best-effort derived data; inspect per-row warnings and retain source fields for audit.

## Handle failure and limits

- A bad/non-Lever slug produces a diagnostic row rather than failing the whole batch. Careers-page auto-detection is best-effort; use the direct Lever slug when it misses a client-rendered or unlinked board.
- Network/provider failures can yield partial data and warnings. Check run status, dataset warnings, and logs before retrying; retry only the affected bounded input.
- `maxItemsPerCompany` and `maxItems` truncation creates warning rows. Raise caps deliberately; results consume pay-per-event charges.
- Delta state is Actor-level persistent state keyed by `globalId`, not an arbitrary user-selected baseline. Do not use one shared Actor identity for independent monitoring baselines unless that behavior is acceptable.
- The source is live public Lever data: no login/proxy is required, but freshness, completeness, legal use, retention, and downstream contact practices remain the caller's responsibility.
