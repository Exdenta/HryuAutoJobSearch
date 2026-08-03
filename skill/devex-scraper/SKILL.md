---
name: devex-scraper
description: Configure, run, integrate, or troubleshoot the Apify Actor nomad-agent/devex-scraper for international-development, NGO, humanitarian, donor, consultancy, and UN jobs. Use when Codex or Claude must assess Actor fit, construct safe capped inputs, call it through Apify API/SDK, interpret discovery, delta, alive-check, warning, or failure rows, or explain BYOK provider costs and Devex search-index limitations.
---

# Devex Scraper

Use Actor `nomad-agent/devex-scraper` (ID `ZutM3sroBQcWaeA0d`) for public Devex job discovery or liveness checks. Prefer another source when direct page contents, exhaustive coverage, or guaranteed freshness is required: Devex blocks direct fetches, so this Actor relies on live search-result metadata.

## Choose a mode

- Discover jobs with optional `keyword`, `location`, `contractType`, and `workplace` filters.
- Set `onlyNewSinceLastRun: true` for scheduled monitoring. The first delta run returns all matches; later delta runs omit previously delivered IDs.
- Set `jobUrls` to check known Devex URLs. This overrides discovery and returns `isActive` as `true`, `false`, or honest `null` when evidence is ambiguous.

Keep `maxItems` at the smallest useful value; default to 12 and never exceed the Actor's hard cap of 50. In alive-check mode it also caps URLs checked.

## Build input

Select one provider and matching secret:

- `anthropic`: `anthropicApiKey`; optional `model` (default `claude-haiku-4-5-20251001`). Claude supplies built-in web search.
- `mistral`: `mistralApiKey`; optional `mistralModel` (`mistral-small-latest`, `mistral-medium-latest`, or `mistral-large-latest`). Keenable supplies search.
- `openai`: `openaiApiKey`; optional `openaiModel` (default `gpt-4.1-mini`). Keenable supplies search.

Use only schema values: `contractType` is empty or one of `full-time`, `part-time`, `contract`, `permanent`, `temporary`, `internship`, `consultancy`, `volunteer`; `workplace` is empty or one of `on-site`, `hybrid`, `remote`. Do not set `proxyConfiguration` expecting different results; it is accepted but inert.

Example capped discovery input:

```json
{
  "provider": "anthropic",
  "anthropicApiKey": "<ANTHROPIC_API_KEY>",
  "keyword": "monitoring evaluation",
  "location": "remote",
  "contractType": "consultancy",
  "workplace": "remote",
  "onlyNewSinceLastRun": true,
  "maxItems": 12
}
```

Never paste, log, commit, or echo real provider or Apify tokens. Prefer Apify secret inputs or environment variables (`ANTHROPIC_API_KEY`, `MISTRAL_API_KEY`, `OPENAI_API_KEY`, `APIFY_TOKEN`). Provider usage is billed separately from Apify pay-per-event charges.

## Integrate

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/devex-scraper").call(run_input={
    "provider": "anthropic",
    "anthropicApiKey": os.environ["ANTHROPIC_API_KEY"],
    "keyword": "governance",
    "maxItems": 12,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

HTTP synchronous endpoint:

```text
POST https://api.apify.com/v2/acts/nomad-agent~devex-scraper/run-sync-get-dataset-items
Authorization: Bearer <APIFY_TOKEN>
Content-Type: application/json
```

Send the same JSON input body. For longer workflows, start an asynchronous run, poll its terminal status, then read `defaultDatasetId`; treat non-`SUCCEEDED` statuses as transport/platform failures.

## Consume output

Normal rows contain `source`, `id`, deprecated alias `externalId`, `title`, `company`, `location`, `url`, `postedAt`, `deadline`, `salary`, `contractType`, `workplace`, `employerWebsite`, and `snippet`. Dates are `YYYY-MM-DD` only when stated; `null` is normal. Delta rows add `isNew: true`. Alive-check rows add `isActive` and ISO-8601 `checkedAt`.

Branch on `warning` before treating a row as a job. A keyless run succeeds with one uncharged warning row. Provider/auth/rate-limit/search failures and empty matches can also produce a succeeded run with a diagnostic warning row and no jobs. Preserve `null`; do not invent missing dates, salary, employer website, or liveness.

## Diagnose and set expectations

- Verify that `provider` matches its key and that the provider account has quota.
- Relax filters or broaden keyword/location when there is no provider error.
- Retry transient provider, Keenable, or search-index failures with bounded backoff.
- Expect incomplete or delayed coverage because results depend on public search indexing, not direct Devex pages.
- Expect alive-check `null` when search signals cannot prove open or closed status.
- Do not claim the Actor bypasses authentication, proxies through Apify, or retrieves private Devex data.
