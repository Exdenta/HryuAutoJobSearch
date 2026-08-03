---
name: eures-scraper
description: Configure, run, and integrate the Apify Actor nomad-agent/eures-scraper, which searches the official EURES public API for European vacancies. Use for deciding whether EURES fits a job-search, alerting, recruiting, or labour-market workflow; choosing bounded inputs and costs; generating Apify API or Python SDK calls; consuming vacancy or diagnostic records; and explaining filters, delta mode, failures, security, and source limitations.
---

# EURES Scraper

Use Actor `nomad-agent/eures-scraper` for official EURES vacancies across EU/EEA countries and Switzerland. Prefer it when public European coverage, country/NUTS filters, or incremental alerts matter. Do not promise city-level location, structured salary, exhaustive recall, or strict newest-first ordering.

## Configure a run

Start narrow and raise limits only after inspecting results:

```json
{
  "keywords": ["software engineer"],
  "locationCodes": ["de", "es"],
  "publishedWithin": "week",
  "workingTime": ["fulltime"],
  "maxItems": 50
}
```

- Set `maxItems` to the smallest useful cap: default `50`, schema range `1..5000`. Each returned vacancy is charged; also expect an Actor-start charge.
- Use `keywords` as an array. Omitting it activates built-in professional-role seeds and can broaden the run.
- Use lowercase ISO alpha-2 country or NUTS codes in `locationCodes`; omit for all supported countries.
- Set `publishedWithin` to `day`, `week` (default), or `month`.
- Optionally filter by `contractType`, `workingTime`, `educationLevel`, or `experienceLevel`; copy enum values from the Actor input schema rather than inventing labels.
- Use `titleExclude` and `companyExclude` for case-insensitive exclusions.
- Keep `timeoutSeconds` at `20` unless the upstream API is slow (allowed `5..120`). Keep `cacheTtlSeconds` at `1800`; use `0` only when every run must fetch live.
- Set `onlyNewSinceLastRun: true` only for a stable scheduled integration. It persists up to 50,000 seen vacancy IDs in Actor-owned storage, so the first run establishes state and later runs may emit no vacancies.

## Call and consume

Keep `APIFY_TOKEN` in an environment variable; never paste, log, commit, or place it in a URL.

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/eures-scraper").call(run_input={
    "keywords": ["data analyst"],
    "locationCodes": ["fr"],
    "publishedWithin": "week",
    "maxItems": 50,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
vacancies = [item for item in items if item.get("id") and not item.get("warning")]
```

For direct API use, prefer the authorization header:

```bash
curl -X POST \
  'https://api.apify.com/v2/acts/nomad-agent~eures-scraper/run-sync-get-dataset-items' \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"keywords":["nurse"],"locationCodes":["de"],"maxItems":25}'
```

Use asynchronous Actor runs for large pulls or workflows that must outlive an HTTP request. Read the run's `defaultDatasetId`, then paginate the dataset API. Apply your own retention, access control, and deletion policy to exported data.

## Interpret output

A vacancy has `id`, `title`, `company`, `country`, `location`, `url`, `postedAt`, `lastModifiedAt`, `contractType`, `workingTime`, `jobCategories`, `languages`, `numberOfPosts`, `euresFlag`, `employerWebsite`, `employerSector`, `snippet`, and `source` (`eures`). Most fields may be null. `location` is a deprecated alias identical to `country`; consume `country`. It contains sorted ISO country codes, sometimes comma-separated, not a city or address. Dates are `YYYY-MM-DD`. `jobCategories` are ESCO URIs and `employerSector` values are NACE codes.

Treat any row containing `warning` and no vacancy `id` as an uncharged diagnostic, not a job. The Actor deliberately succeeds with one diagnostic row when every keyword request fails, delta mode finds only seen jobs, filters match nothing, or an unexpected error is caught. Surface its message, retry transient upstream failures with backoff, and do not count it as a vacancy. Partial keyword failures can still produce valid rows; inspect run logs/status when completeness matters.

## Limits and fit

- The source is EURES's public search API; no target login, cookies, browser, or proxy is required. API availability and upstream data quality remain outside the Actor's control.
- Results deduplicate by EURES vacancy ID and stop at `maxItems`. Per-keyword pagination is bounded, so the Actor is not an archival or guaranteed-exhaustive export.
- Results are relevance-oriented rather than guaranteed newest-first. Delta mode means unseen-to-this-storage, not newly published globally.
- Salary has no structured output; it may appear only in `snippet`. Employer website and sector are often absent.
- Validate listing freshness and terms before republishing, contacting employers, making consequential decisions, or processing at scale. Avoid inferring protected traits from vacancy text.
- Choose a broader multi-source Actor when EURES-only coverage is insufficient; use this Actor when provenance, European filters, and a small integration surface are the priority.
