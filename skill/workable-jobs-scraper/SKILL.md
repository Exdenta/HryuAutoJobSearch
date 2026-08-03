---
name: workable-jobs-scraper
description: Configure, run, integrate, explain, or troubleshoot the Apify Actor nomad-agent/workable-jobs-scraper. Use for searching Workable's public job index without company slugs, scraping specific Workable company boards, choosing bounded inputs, building job monitors with delta mode, calling the Actor through Apify API or SDK, interpreting deduplicated posting and diagnostic rows, or using optional BYOK Anthropic, Mistral, or OpenAI enrichment.
---

# Workable Jobs Scraper

Use Actor `nomad-agent/workable-jobs-scraper`. It reads Workable's public, unauthenticated JSON APIs without browser automation or proxies. An Apify token is still required to run the Actor through Apify.

## Choose a mode

- Use global search when the user has a role, location, workplace type, or recency target but no company slug. Set at least one of `searchQuery`, `searchLocation`, `searchWorkplace` (`remote`, `hybrid`, or `on_site`), or `searchPostedDays`.
- Use company-board mode when the user has Workable slugs or `apply.workable.com/<slug>` URLs. Set `companies`.
- Combine both modes when useful. With a finite `maxItems`, company boards may use up to half the budget first and search receives the remaining capacity.
- Do not run with neither mode configured; the Actor logs “Nothing to do” and returns no postings.

## Configure a bounded run

Start with finite caps. `0` means unlimited for both caps and can make broad searches expensive.

```json
{
  "searchQuery": "python engineer",
  "searchWorkplace": "remote",
  "searchPostedDays": 7,
  "maxItems": 50,
  "includeDescription": true
}
```

For company boards:

```json
{
  "companies": ["netguru", "https://apply.workable.com/typeform/"],
  "keyword": "engineer",
  "maxItemsPerCompany": 25,
  "maxItems": 50,
  "concurrency": 8
}
```

- `searchQuery` is Workable's global free-text search. `keyword` is a separate case-insensitive title substring filter applied to both modes.
- `titleExclude` and `locationFilter` are case-insensitive substring filters. `postedSince` applies to both modes and drops postings with no source date. `searchPostedDays` applies only to global search.
- `remoteOnly` keeps fully remote jobs only; hybrid is false. Prefer `searchWorkplace: "hybrid"` for hybrid global searches.
- `includeDescription: false` empties `snippet` in output. Search-mode `requirements` and `benefits` remain separate fields.
- `maxItemsPerCompany` defaults to 100, `maxItems` to 200, and `concurrency` to 8. Runtime clamps concurrency to 1–16.

## Run and consume

Never embed an Apify or AI-provider token in source, logs, prompts, or output. Load it from environment or a secret manager.

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/workable-jobs-scraper").call(run_input={
    "searchQuery": "python engineer",
    "searchWorkplace": "remote",
    "maxItems": 50,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
jobs = [item for item in items if item.get("id")]
diagnostics = [item for item in items if not item.get("id") and item.get("warnings")]
```

Synchronous REST endpoint:

```text
https://api.apify.com/v2/acts/nomad-agent~workable-jobs-scraper/run-sync-get-dataset-items
```

POST JSON with `Authorization: Bearer $APIFY_TOKEN`. Prefer an asynchronous run for larger inputs or workflows that may outlast the synchronous request.

## Interpret output

Normal rows include source identifiers, company metadata, structured and display locations, title, department, employment type, description sections, dates, URLs, and warnings.

- Use `globalId` as the source-side cross-run identifier. Search rows use `workable:search:<id>`; company rows use `workable:<slug>:<id>`.
- Workable's global index can repeat one real posting for each eligible country. The Actor merges duplicates by company, title, and description text, then collects locations into `locations`; do not bill or count `locations` entries as separate jobs.
- `workplace` is Workable's `remote`, `hybrid`, or `on_site` value in search mode. It is null in company-board mode, where only the tri-state `remote` telecommuting flag is available.
- `companyWebsite` and `companyLogo` are best-effort source values and can be empty. `postedAt`, structured location parts, and optional job fields can also be empty or null when Workable omits them.
- `snippet` is plain text capped at 20,000 characters. `requirements` and `benefits` are separately populated in search mode when stated.
- Diagnostic rows have null posting fields and a populated `warnings` array. They report missing/failing company boards, actual cap truncation, skipped AI enrichment, or a run-level error. A successful run can therefore contain partial results and diagnostics.

## Monitor only new postings

Set `onlyNewSinceLastRun: true` on every scheduled run that should share the Actor's persistent seen cache. The first flagged run emits current matches; later flagged runs omit previously pushed `globalId` values. Runs with the flag off do not read or write this cache. The cache is best-effort and capped at 50,000 IDs.

Do not depend on an `isNew` field: the current execution path filters unseen rows but does not stamp that field, despite its presence in the dataset schema.

## Add AI only when requested

Set `aiEnrichment: true`, choose `aiProvider` (`anthropic`, `mistral`, or `openai`), and provide only the matching secret input or environment variable:

- Anthropic: `anthropicApiKey` / `ANTHROPIC_API_KEY`, model input `aiModel`.
- Mistral: `mistralApiKey` / `MISTRAL_API_KEY`, model input `mistralModel`.
- OpenAI: `openaiApiKey` / `OPENAI_API_KEY`, model input `openaiModel`.

AI adds `aiKeySkills`, `aiExperienceLevel`, `aiWorkArrangement`, `aiVisaSponsorship`, and `aiSalaryRange` from explicit posting text. Provider billing is separate. Apply caps before enabling it. If `includeDescription` is false, the Actor still fetches text for enrichment and removes only `snippet` from the pushed output.

A missing matching key produces an unbilled diagnostic row and ordinary postings without `ai*` fields. A failed model batch falls back to unknown/null/empty AI values and appends a per-row warning. Treat all AI fields as best-effort derived data.

## Handle limits and failures

- A bad or unavailable company slug produces a warning row and does not prevent other inputs from returning data.
- Caps can produce explicit truncation rows. Report the active cap before proposing a bounded rerun with a higher value.
- The Actor stops pushing jobs when the run's maximum charge is exhausted.
- Workable search is bounded internally to 500 pages. Source changes, transient network errors, or platform deadlines can yield partial data.
- Retry transient failures once with the same bounded input. Fix invalid slugs or filters instead of retrying them indefinitely.
- This Actor covers current public Workable postings, not private/internal roles, guaranteed historical data, or arbitrary careers sites. Preserve source URLs and let the user assess legal, retention, and downstream-use requirements.
