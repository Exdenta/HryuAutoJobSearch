---
name: justjoinit-scraper
description: Evaluate, configure, run, and integrate the Apify Actor nomad-agent/justjoinit-scraper for Polish and CEE technology job data. Use when an agent must decide whether the Actor fits a job-search, recruiting, salary-analysis, alerting, or market-data use case; choose safe capped inputs; call it through the Apify API or SDK; consume its normalized dataset; or explain full-detail enrichment, delta mode, costs, partial results, and limitations.
---

# JustJoin.it Scraper

Use `nomad-agent/justjoinit-scraper` for public JustJoin.it technology vacancies. Prefer it when Polish/CEE coverage, contract-specific B2B/UoP salary ranges, skills, workplace type, or location data matter. Do not present it as a general job-board crawler, applicant/contact-data source, or guaranteed historical archive.

## Configure a bounded run

Start narrow and raise limits only when the user asks:

```json
{
  "categories": ["1", "5"],
  "keywords": ["python"],
  "maxItems": 100,
  "maxPages": 2,
  "extractFullDetails": false,
  "cacheTtlSeconds": 1800
}
```

- Treat empty `categories` as all 23 categories, requiring separate paginated requests per category. Accept numeric or string category IDs `1` through `23`.
- Use `experienceLevels`, `workplaceTypes`, and `contractTypes` only when required. Common values are `junior|mid|senior|c_level|manager`, `remote|hybrid|office`, and `b2b|permanent|mandate_contract|internship|any`; the Actor also accepts new source values. Legacy `c-level` maps to `c_level`.
- Use `keywords`, `city`, `withSalaryOnly`, `salaryMin` (PLN-normalized), `postedSince`, `titleExclude`, and `companyExclude` for client-side filtering.
- Keep `maxItems` positive and modest for interactive work. Although `0` means unlimited, use it only on explicit request with a deliberate `maxPages` cap. `maxPages` is per category, defaults to 5, and is runtime-capped at 20; pages contain up to 50 offers.
- Enable `extractFullDetails` only when descriptions, skill proficiency, languages, or company size are needed; it adds one detail-page request per retained posting and fails open per posting.
- Enable `onlyNewSinceLastRun` for scheduled alerts. Let the Actor derive the filter-specific state bucket, or set a stable non-secret `stateKey` only when runs intentionally share history. Do not place credentials or personal data in `stateKey` or other inputs.
- Set `cacheTtlSeconds: 0` only when live freshness outweighs upstream load; the default is 1800 seconds.

## Run and retrieve results

Require `APIFY_TOKEN` from environment or a secret manager. Never print it, embed it in source, or put it in a shared URL.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/justjoinit-scraper").call(
    run_input={"categories": ["5"], "maxItems": 100, "maxPages": 2}
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

Synchronous HTTP:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"categories":["5"],"maxItems":100,"maxPages":2}' \
  'https://api.apify.com/v2/acts/nomad-agent~justjoinit-scraper/run-sync-get-dataset-items'
```

For long or scheduled runs, start the Actor asynchronously, poll the run to a terminal state, then read `defaultDatasetId`. Paginate dataset reads instead of assuming all items fit in one response.

## Interpret output

Expect one flat object per posting. Identity and navigation fields are `id`, `source` (`justjoinit`), and `url`. Core fields include `title`, `company`, `location`, `city`, coordinates, `workplaceType`, `experienceLevel`, `requiredSkills`, `niceToHaveSkills`, `postedAt`, `snippet`, and logo URL.

Prefer `salaryByType` for analysis: each disclosed contract may contain `type`, raw `min`/`max`, `currency`, `period`, `gross`, and PLN/EUR/USD-normalized bounds. The scalar `salary*` fields describe only the first disclosed contract. Optional detail fields are `description`, `skillLevels`, `languages`, and `companySize`; tolerate their absence even when enrichment is enabled.

Detect diagnostic rows by a `warning` field. A successful run can contain only a diagnostic row when no jobs match, delta mode finds nothing new, the charge budget is exhausted, or an unexpected upstream failure is converted to a fail-soft result. Do not count such rows as jobs. Treat missing nullable fields as unknown, not empty facts, and deduplicate primarily by `source` plus `id`.

## Handle costs, failures, and limits

- Expect pay-per-event charging: an Actor-start event plus result events. Verify current Store pricing before estimating cost; do not hardcode a quote into integrations.
- Treat partial datasets as useful when the run approaches its timeout or charge ceiling. Record run status, dataset ID, item count, warnings, and filter input for observability.
- Retry transient platform or upstream failures with bounded backoff. Do not retry a deterministic bad input indefinitely.
- Expect source schema changes, stale/missing publish dates, cache lag, incomplete salary disclosure, and detail-page parsing gaps. The Actor exposes public postings only; it does not supply applications, recruiter contacts, private profiles, or guaranteed exhaustive history.
- Respect JustJoin.it terms, Apify policies, privacy law, and downstream retention requirements. Avoid using inferred sensitive attributes or making automated employment decisions from scraped data.
