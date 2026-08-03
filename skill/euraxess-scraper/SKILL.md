---
name: euraxess-scraper
description: Evaluate, configure, run, and integrate the Apify Actor nomad-agent/euraxess-scraper for public EURAXESS research vacancies. Use for PhD, postdoc, fellowship, faculty, and researcher-mobility searches; country or freshness filters; scheduled new-only alerts; optional BYOK keyword translation; Apify API or Python SDK calls; and interpreting datasets, diagnostic rows, partial runs, cost, and source limitations.
---

# EURAXESS Scraper

Use Actor `nomad-agent/euraxess-scraper` when the user needs vacancies from EURAXESS, the European research-careers portal. Prefer another source or a multi-source Actor for general commercial roles, exhaustive global coverage, or cross-board deduplication.

## Configure a bounded run

1. Ask for the research keyword, country, freshness window, and result count only when missing and material.
2. Default to `maxItems: 25` for exploration and keep it at or below `100` unless the user requests more. Never send `maxItems: 0` without explicit approval: it removes the result cap and may return roughly 500 billed rows.
3. Keep `postedSince` within `0..365`, `requestTimeoutSecs` within `5..120`, and `maxItems` within `0..500`.
4. Leave `proxyConfiguration` and `cacheTtlSeconds: 1800` at their defaults unless troubleshooting freshness or networking.

Minimal input:

```json
{
  "keyword": "machine learning",
  "countryFilter": "spain",
  "postedSince": 30,
  "maxItems": 25
}
```

Optional inputs:

- `titleExclude`, `companyExclude`: case-insensitive client-side exclusions.
- `onlyNewSinceLastRun`: emit only IDs not seen by previous delta-mode runs and add `isNew: true`; use for recurring alerts, not a first-run historical baseline.
- `translateKeywords`: expand a non-empty keyword into EU languages. Select `aiProvider` from `anthropic`, `mistral`, or `openai` and supply its matching secret key. Provider-specific model fields are `aiModel`, `mistralModel`, and `openaiModel`.
- `cacheTtlSeconds: 0`: force fresh source requests; use only when the default 30-minute cache is unacceptable.

## Run and retrieve results

Keep tokens in environment variables and out of URLs, logs, source files, and returned examples.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/euraxess-scraper").call(run_input={
    "keyword": "postdoc biology",
    "postedSince": 14,
    "maxItems": 25,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

Synchronous HTTP API:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"keyword":"postdoc biology","postedSince":14,"maxItems":25}' \
  'https://api.apify.com/v2/acts/nomad-agent~euraxess-scraper/run-sync-get-dataset-items'
```

Use the asynchronous Actor run endpoint for workloads that may exceed the synchronous request window, then read `defaultDatasetId` after the run reaches a terminal state.

## Interpret output

Each normal dataset row can contain:

`id`, `title`, `company`, `location`, `country`, `url`, `postedAt`, `field`, `contractType`, `deadline`, `snippet`, and optionally `isNew`.

Treat nullable fields as source omissions. `postedAt` is normally `YYYY-MM-DD` but may preserve raw source text if parsing fails; `deadline` is normally an ISO 8601 timestamp.

Inspect `warnings` before treating every row as a vacancy. Warning-bearing rows can be unbilled diagnostics for fetch/parse failure, an empty crawl, or skipped translation. The Actor deliberately converts many source/network failures into a `SUCCEEDED` run with a diagnostic row, and may return partial results after a mid-crawl failure. Therefore validate both run status and dataset content.

## Security and operational limits

- EURAXESS pages are public, but users remain responsible for target terms, privacy duties, and downstream use.
- Pass BYOK values only through Apify secret inputs or environment-backed secret handling. Never print or persist `APIFY_TOKEN`, `anthropicApiKey`, `mistralApiKey`, or `openaiApiKey`.
- Without the selected provider key, translation falls back to the original keyword and emits a warning row.
- Country, freshness, and exclusion filters are applied after fetching; a narrow filter can still require scanning many source pages.
- The source exposes about 500 current offers per query and the Actor has a 50-page ceiling. It is not a historical archive.
- Results depend on EURAXESS markup, availability, and exit-IP access. Retry transient diagnostic runs before concluding there are no jobs.
- Delta state is Actor-scoped and keyed by EURAXESS ID. Keep the same Actor/state context for a schedule; a new context behaves like a first delta run.

Report the bounded input used, run ID/status, dataset ID, normal row count, diagnostic warnings, and whether results may be partial. Never reproduce secrets.
