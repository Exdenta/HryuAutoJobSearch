---
name: wttj-scraper
description: Evaluate, configure, run, and integrate the Apify Actor nomad-agent/wttj-scraper for current public Welcome to the Jungle job postings. Use when an agent must decide whether the Actor fits a French or European job-search, recruiting, alerting, salary-analysis, or labor-market workflow; choose bounded inputs; call it through Apify; interpret its normalized dataset or diagnostic rows; or explain detail mode, delta mode, cost controls, and source limitations.
---

# Welcome to the Jungle Scraper

Use `nomad-agent/wttj-scraper` for current public jobs from Welcome to the Jungle. Prefer it when French or European tech-company coverage, structured salary, contract type, experience, remote status, and company metadata matter. Do not present it as a general web crawler, application service, contact-data source, or historical archive.

## Configure a bounded run

Start narrow and raise `maxItems` only when needed:

```json
{
  "query": "data engineer",
  "countryCodesList": ["FR", "ES"],
  "includeRemote": true,
  "contractType": ["full_time", "freelance"],
  "experienceMax": 3,
  "indexLang": "en",
  "maxItems": 50,
  "includeDetails": false,
  "skipReposts": true,
  "cacheTtlSeconds": 1800
}
```

- Leave `countryCodesList` empty to search the default set: ES, FR, DE, NL, BE, IE, PT, IT, SE, DK, FI, NO, AT, CH, PL, LU, and EE. The legacy comma-separated `countryCodes` input is merged with it.
- Keep `includeRemote: true` to also include jobs WTTJ flags remote or hybrid regardless of country; set it false to restrict matches to selected-country offices.
- Use only schema-supported `contractType` values: `full_time`, `part_time`, `temporary`, `internship`, `apprenticeship`, `freelance`, `vie`, or `other`.
- Treat `experienceMax` as a maximum required-experience filter. Jobs missing experience data are excluded when it is set.
- Choose `indexLang` from `en`, `fr`, `es`, `de`, `nl`, `pt`, or `it`. It affects taxonomy labels, not the original job text.
- Keep `maxItems` positive. Algolia exposes at most about 1,000 hits per query, so a larger value cannot make a run exhaustive.
- Enable `includeDetails` only when full posting text, missions, candidate profile, benefits, and company description are needed. These fields come from the same search index rather than detail-page requests.
- Enable `skipReposts` to deduplicate stable job IDs within one run.
- Enable `onlyNewSinceLastRun` for scheduled alerts. The first run emits current matches; later runs emit unseen IDs. Let the Actor derive filter-specific state, or set a stable `stateName` only when runs should intentionally share history.
- Set `cacheTtlSeconds: 0` only when live freshness outweighs upstream load; the default is 1,800 seconds.

Also set Apify's maximum cost per run when the client surface supports it. Verify current Store pricing before estimating cost.

## Run and retrieve results

Require `APIFY_TOKEN` from the environment or a secret manager. Never print it, commit it, or place it in a shared URL.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/wttj-scraper").call(
    run_input={"query": "data engineer", "maxItems": 50},
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

Synchronous HTTP:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"data engineer","maxItems":50}' \
  'https://api.apify.com/v2/acts/nomad-agent~wttj-scraper/run-sync-get-dataset-items'
```

Use an asynchronous run plus paginated dataset retrieval for schedules, larger runs, or clients with short HTTP timeouts. Record the run ID, terminal status, `defaultDatasetId`, inputs, warnings, and item count.

## Interpret output

Expect one flat object per job. Use `source` (`wttj`) plus `id` for downstream identity and `url` for the public posting. Core fields are `title`, `company`, `location`, `postedAt`, `snippet`, `contractType`, `experienceMin`, `educationLevel`, `hasRemote`, `remoteType`, and `profession`.

Salary fields are `salary` plus nullable `salaryMin`, `salaryMax`, `salaryCurrency`, and `salaryPeriod`. Company fields are `companySlug`, `companyUrl`, `companyLogoUrl`, `companyEmployeeCount`, `companyCreationYear`, and `companySectors`. With `includeDetails`, also consume `descriptionText`, `keyMissions`, `candidateProfile`, `benefits`, and `companyDescription`.

Treat missing fields as unknown. Remote and taxonomy values reflect WTTJ's classifications. Salary numbers retain the source currency and period; do not combine them without normalization.

Detect non-job rows by the presence of `warning`. A successful run can contain only a diagnostic row when no jobs match, delta mode has no new IDs, the upstream query fails, or an unexpected error is converted to a fail-soft result. Do not count diagnostic rows as jobs.

## Handle costs, failures, and limits

- Expect pay-per-event charging for Actor start and returned results, subject to the run's maximum-charge budget. Verify current Store pricing instead of hardcoding it.
- Treat a partial dataset as useful when a timeout or charge ceiling stops the run. Delta state is updated only for successfully delivered records.
- Retry transient platform or upstream failures with bounded backoff. Broaden filters when a diagnostic row says no results; do not retry deterministic bad input indefinitely.
- Expect cache lag, missing source fields, Algolia's pagination ceiling, and upstream schema or availability changes. Do not claim exhaustive or historical coverage.
- Respect Welcome to the Jungle terms, Apify policies, privacy law, and downstream retention requirements. Do not infer sensitive traits or make automated employment decisions from scraped data.
