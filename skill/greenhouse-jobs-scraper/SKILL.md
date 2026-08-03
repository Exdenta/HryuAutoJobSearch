---
name: greenhouse-jobs-scraper
description: Evaluate, explain, configure, or integrate the Apify Actor nomad-agent/greenhouse-jobs-scraper. Use for scraping or monitoring company Greenhouse job boards, choosing safe capped inputs, calling the Actor through Apify API or Python SDK, and interpreting posting, warning, truncation, closed-job, salary, application-question, or BYOK AI-enrichment records.
---

# Greenhouse Jobs Scraper

Use `nomad-agent/greenhouse-jobs-scraper` for companies hosted on Greenhouse. It reads Greenhouse's public board API without login, browser automation, or proxies. Confirm a target board uses `boards.greenhouse.io/<slug>`; otherwise choose the correct ATS Actor.

## Configure a run

1. Normalize board URLs to company slugs, then set `companies`. An empty array invokes the Actor's built-in demo list, not “no companies.”
2. Apply server-side filters before raising caps: `keyword`, `titleExclude`, `locationFilter`, `departmentFilter`, and `postedSince`.
3. Keep `maxItemsPerCompany` and `maxItems` finite. Start with 25 and 100 respectively; raise only when needed. `0` means unlimited.
4. Leave `includeDescription: true` unless lighter output matters. Enable `includeCompensation` or `includeQuestions` only when needed; either adds one detail request per kept posting.
5. Use `onlyNewSinceLastRun: true` for scheduled monitoring. The first flagged run emits all current jobs; later flagged runs emit only new jobs plus unbilled `isClosed` rows.
6. Keep `concurrency` at 8 unless troubleshooting. Valid range is 1–16.

Safe starting input:

```json
{
  "companies": ["stripe", "gitlab"],
  "keyword": "engineer",
  "maxItemsPerCompany": 25,
  "maxItems": 100,
  "includeDescription": true
}
```

## Call the Actor

Use an Apify token from the environment; never embed it in source or logs.

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/greenhouse-jobs-scraper").call(run_input={
    "companies": ["stripe"],
    "maxItemsPerCompany": 25,
    "maxItems": 25,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

For HTTP integrations, POST JSON to:

```text
https://api.apify.com/v2/acts/nomad-agent~greenhouse-jobs-scraper/run-sync-get-dataset-items
```

Send the token in the `Authorization: Bearer ...` header. Prefer asynchronous Actor runs for larger batches or workflows that can exceed a synchronous request timeout.

## Interpret output

Normal rows contain `ats`, `company`, `id`, `globalId`, `title`, `department`, `location`, `url`, `postedAt`, `snippet`, salary fields, and `warnings`. Treat `globalId` (`greenhouse:<slug>:<id>`) as the stable merge/dedup key.

- `remote` and `employmentType` are always null because Greenhouse does not expose trustworthy structured values. `locationFilter: "remote"` is only a text approximation.
- Salary fields remain null unless structured compensation is published and requested, or AI extracts an explicit range.
- `questions` appears when `includeQuestions` is enabled.
- Warning/truncation rows may have null job fields; inspect `warnings` before treating every dataset row as a job.
- Delta rows with `isNew: true` are new postings. Rows with `isClosed: true` are removal signals with limited content, not live jobs.
- A failed board does not necessarily fail the Actor; it can produce a warning row while other companies succeed.

## Optional AI enrichment

Set `aiEnrichment: true`, select `aiProvider` (`anthropic`, `mistral`, or `openai`), and provide only the matching secret key or platform environment variable. Provider-specific model inputs are `aiModel`, `mistralModel`, and `openaiModel`. AI adds skills, experience level, work arrangement, visa sponsorship, and may extract explicitly stated salary data. It is separately billed by the provider.

If the matching key is missing or enrichment fails, base postings still return without reliable `ai*` fields. Never copy API keys into examples, datasets, issue reports, or chat output; use Apify secret inputs or environment variables.

## Limitations and failure handling

- This Actor covers public Greenhouse boards, not private/internal roles or arbitrary careers sites.
- Company slugs can be renamed, absent, or temporarily unavailable; surface warning rows and continue partial results.
- Filters are substring-based, dates can be absent, and description snippets are truncated; do not present them as semantic guarantees or full job text.
- Delta state belongs to this Actor's dedicated key-value store. Do not expect continuity after switching Actor identity or state storage.
- Caps can produce explicit truncation rows. Report partial output and the active cap before suggesting a rerun with a higher value.
- Do not retry unboundedly. Retry transient platform/network failures once; fix invalid slugs or inputs instead of retrying them.
