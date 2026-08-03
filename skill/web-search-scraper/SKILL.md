---
name: web-search-scraper
description: Assess, configure, run, and integrate Apify Actor nomad-agent/web-search-scraper, a BYOK AI agent that discovers open-web job postings with Anthropic, Mistral, or OpenAI and returns ranked, HTTP-checked records. Use when choosing an open-web job finder, preparing provider-specific inputs or Apify API/SDK calls, setting up capped one-off or scheduled searches, interpreting its dataset or diagnostic rows, or troubleshooting keys, provider behavior, delta mode, and incomplete results.
---

# Web Search Scraper

Use Actor ID `nomad-agent/web-search-scraper`.

## Assess fit

Choose this Actor to search beyond one job board when the user wants direct posting URLs, stated salary, and AI match scores with reasoning. It searches public pages and requires the caller's own AI-provider key in addition to an Apify token.

Prefer a dedicated board or ATS Actor when complete coverage of one source matters. Do not describe this Actor as exhaustive or treat `verified` as proof that a vacancy is accurate, fresh, or accepting applications.

## Choose a provider

Set `provider` explicitly and supply its matching secret:

| Provider | Secret input | Model input and default | Discovery path |
|---|---|---|---|
| `anthropic` | `anthropicApiKey` | `model`: `claude-haiku-4-5-20251001` | Claude agent with built-in web search, capped at six searches |
| `mistral` | `mistralApiKey` | `mistralModel`: `mistral-medium-latest` | keenable search, then Mistral page judging/extraction |
| `openai` | `openaiApiKey` | `openaiModel`: `gpt-4.1-mini` | keenable search, then OpenAI page judging/extraction |

Provider usage is billed by that provider separately from Apify. Never expose an Apify or provider key in source, logs, chat output, or client-side code. Read keys from environment variables or a secret manager and pass them only to the Actor's secret input fields.

## Build the input

- `keywords`, `locations`, `titleMustMatch`, `titleExclude`: arrays of short strings.
- `userDescription`: primary free-text matching signal; state the desired role and constraints plainly.
- `remote`, `seniority`: AI-interpreted free text, both defaulting to `any`; these are matching signals, not deterministic filters.
- `maxItems`: default 15, clamped to 1–50. The Actor may return fewer.
- `maxAgeHours`: default 168. Anthropic treats it as a preference and clamps it to 1–8760; the keenable/Mistral/OpenAI extraction path instead applies its own roughly 30-day freshness instruction.
- `onlyNewSinceLastRun`: default false. Enable for recurring alerts; previously emitted IDs from delta-enabled runs are suppressed and new rows carry `isNew: true`.

`titleMustMatch` is a preference and search hint. `titleExclude` is enforced after discovery with case-insensitive title containment.

Start with a small `maxItems` and an Apify maximum-total-charge limit, then broaden only if needed.

```json
{
  "provider": "openai",
  "openaiApiKey": "<OPENAI_API_KEY>",
  "openaiModel": "gpt-4.1-mini",
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

Replace the placeholder at execution time; never save a real key in this example or another repository file.

## Run and integrate

Prefer the user's existing Apify integration. With Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/web-search-scraper").call(
    run_input={
        "provider": "openai",
        "openaiApiKey": os.environ["OPENAI_API_KEY"],
        "keywords": ["python", "backend"],
        "locations": ["remote", "Europe"],
        "maxItems": 5,
    },
    max_total_charge_usd=0.10,
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

For a synchronous REST response, POST the same JSON input to:

```text
https://api.apify.com/v2/acts/nomad-agent~web-search-scraper/run-sync-get-dataset-items?token=<APIFY_TOKEN>&maxTotalChargeUsd=<LIMIT>
```

For long runs, start asynchronously and poll the run. Read records from the run's default dataset. Reuse the same Actor with `onlyNewSinceLastRun: true` for scheduled monitoring; its seen-ID state is Actor-wide and state access fails open.

## Interpret output

A normal row contains:

- `id`, `source: "web_search"`, `title`, `company`, `location`, `url`
- `postedAt`, `snippet`, `salary`
- `matchScore`, `matchReasoning`, `verified`
- optional `isNew` on delta runs

Extraction fields can be null. `salary` is copied from the posting, never inferred. `postedAt` is normalized where possible, falls back to the run date when absent, and can rarely remain unparsed. Rows are sorted by `matchScore`, with unscored rows last.

Treat a row containing `warning` as a diagnostic, not a job. Surface its `warning` and optional `docs`. A `SUCCEEDED` run can contain a diagnostic row or no jobs, so never equate run status with useful results.

`verified: true` means the liveness check received an HTTP response and did not see a clear dead-link signal. The Actor drops HTTP 404/410 responses and redirects to a site root; network errors are kept as unverified. Always review the direct posting before acting.

## Troubleshoot conservatively

- Missing or mismatched provider keys can produce a diagnostic row; Anthropic keys must start with `sk-ant-`.
- An exhausted Apify maximum-total-charge limit can prevent discovery or stop output early; raise the limit deliberately.
- Empty or partial results can come from upstream search coverage, bot defenses, model judgment, title exclusions, liveness checks, delta suppression, or provider quota. Retry transient failures, inspect diagnostics and run logs, then broaden the profile if appropriate.
- Never promise a fixed count, complete market coverage, or strict enforcement of preference fields.
