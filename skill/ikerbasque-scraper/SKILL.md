---
name: ikerbasque-scraper
description: Run, configure, integrate, or troubleshoot the Apify Actor nomad-agent/ikerbasque-scraper, which extracts Ikerbasque research calls in Spain's Basque Country. Use when Codex or Claude needs to assess Actor fit, choose bounded inputs, enable new-call alerts or optional BYOK deadline extraction, call the Actor through Apify API/SDK, consume its dataset, or explain its output, failure behavior, cost controls, security, and limitations.
---

# Ikerbasque Scraper

Use Actor `nomad-agent/ikerbasque-scraper` for Ikerbasque calls such as Research Fellow, Research Associate, Research Professor, permanent-position, and partner programmes. Do not present it as a broad Spain, Basque Country, or multi-source academic-jobs scraper.

## Configure a run

Start with a capped, non-AI run:

```json
{"includeClosed": false, "postedSince": 0, "maxItems": 50, "onlyNewSinceLastRun": false, "aiEnrichment": false, "cacheTtlSeconds": 1800}
```

- Keep `maxItems` positive to bound returned, billed results; `0` means unlimited.
- Set `postedSince` to a non-negative day count; `0` disables the filter. Records with unknown `postedAt` remain included.
- Enable `includeClosed` only when historical/closed calls are useful.
- Use `onlyNewSinceLastRun: true` for scheduled alerts. Its persistent seen-state applies only to runs that enable this flag; first use treats current matching calls as new.
- Keep `cacheTtlSeconds: 1800` normally; use `0` only when a live source fetch is required.
- Treat negative numeric inputs as `0`; avoid relying on schema rejection.

For structured deadlines, set `aiEnrichment: true`, choose `aiProvider` (`anthropic`, `mistral`, or `openai`), and supply only the matching secret field: `anthropicApiKey`, `mistralApiKey`, or `openaiApiKey`. Optional models are `aiModel`, `mistralModel`, and `openaiModel`. Enrichment is separately billed by that provider. Prefer Apify secret input or environment configuration; never log, echo, commit, or return keys.

## Call and consume

Python:

```python
from apify_client import ApifyClient

client = ApifyClient("<APIFY_TOKEN>")
run = client.actor("nomad-agent/ikerbasque-scraper").call(
    run_input={"includeClosed": False, "maxItems": 50}
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

HTTP:

```bash
curl -X POST \
  'https://api.apify.com/v2/acts/nomad-agent~ikerbasque-scraper/run-sync-get-dataset-items?token=<APIFY_TOKEN>' \
  -H 'Content-Type: application/json' \
  -d '{"includeClosed":false,"maxItems":50}'
```

For production, pass tokens via a secret/environment variable rather than shell history or source. Prefer asynchronous Actor runs when orchestration needs explicit status, logs, retries, or large response handling.

## Interpret output

Each normal dataset item contains:

- `id`: stable call URL slug.
- `title`, `company` (`Ikerbasque`), `location` (`Bilbao, Spain`), `status`.
- `postedAt`: detail-page modified/updated/published date in `YYYY-MM-DD`, not a guaranteed first-posted date; may be null.
- `deadline`: AI-extracted `YYYY-MM-DD` or null. Null is expected without enrichment or without an explicit calendar deadline.
- `url`, full plain-text `snippet`.
- `isNew: true` only on newly seen delta-mode results.
- `warnings`: non-fatal record notes when present.

Detect diagnostic rows by a non-empty `warnings` array with normal identity fields such as `id` and `title` null. Do not treat them as jobs. Missing-key AI configuration and source/runtime failures can yield these unbilled rows while the Actor run remains `SUCCEEDED`; inspect both dataset warnings and run logs. A detail-page failure is softer: the real call remains, usually with `postedAt: null`.

## Respect limits

- Source coverage is one small, non-paginated public Ikerbasque calls page, not an archive or all jobs at host institutes.
- `location` is institutional metadata, not a per-role duty station.
- `postedAt` is a best-available page timestamp; `postedSince` therefore cannot guarantee vacancy publication age.
- AI enrichment reads at most the first 4,000 characters of the internal description text and never guarantees a deadline.
- Delta state is best-effort; storage failure degrades to treating all matching calls as new.
- The Actor retries transient fetches, caches responses, caps billing at the run charge limit, and may return partial data near a platform deadline.

Validate required fields and deduplicate downstream by `id` or `url`. Preserve nulls rather than inventing dates or locations. Review Ikerbasque terms and applicable law before reuse; public accessibility is not legal advice.
