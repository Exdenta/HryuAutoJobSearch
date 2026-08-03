---
name: company-careers-bundle
description: Configure, run, and integrate the Apify Actor nomad-agent/company-careers-bundle, which probes Greenhouse, Lever, Ashby, Workable, SmartRecruiters, and Workday and returns normalized company job postings. Use for company-careers scraping, ATS discovery, job-alert feeds, delta monitoring, recruiting or market-data pipelines, capped API/SDK calls, output interpretation, or troubleshooting this Actor.
---

# Company Careers Bundle

Use Actor `nomad-agent/company-careers-bundle` when the user has company names/slugs or career-board URLs and wants one normalized dataset across multiple ATS providers. Do not use it for general web crawling, candidate profiles, application automation, or complete coverage of custom career sites.

## Choose inputs

1. Pass `companies`, `presetLists`, or both. A blank input runs a built-in company list, so always set one when the intended scope is specific.
2. Pass ordinary ATS board slugs or full board URLs. For Workday, pass the full `https://<tenant>.<datacenter>.myworkdayjobs.com/.../<site>` careers URL; a bare company slug cannot identify its datacenter and site.
3. Restrict `atsProviders` when the provider is known. Valid values are `greenhouse`, `lever`, `ashby`, `workable`, `smartrecruiters`, and `workday`.
4. Apply `keyword`, `titleExclude`, `locationFilter`, `postedSince`, or `remoteOnly` only when requested. Filters are case-insensitive substring matches. `remoteOnly` requires an explicit source signal; Greenhouse records have `remote: null` and will not pass it. `postedSince` drops undated postings.
5. Keep runs bounded. Start with `maxItemsPerCompany: 25`, `maxItems: 100`, and default `concurrency: 8`; raise caps only when the use case requires it. Schema limits are 0–5000 for both item caps (`0` means unlimited) and 1–16 for concurrency.
6. Set `includeDescription: false` unless descriptions or AI enrichment are needed.

Minimal input:

```json
{
  "companies": ["stripe", "openai"],
  "atsProviders": ["greenhouse", "ashby"],
  "maxItemsPerCompany": 25,
  "maxItems": 100,
  "includeDescription": false
}
```

For recurring alerts, set `onlyNewSinceLastRun: true`. It outputs unseen postings as `isNew: true` and may emit non-billed `isClosed: true` diagnostic rows. State is Actor-scoped; changing the company/provider scope does not make unprobed boards appear closed.

## Run and integrate

Prefer asynchronous runs for production and poll the run status before reading its default dataset. Use synchronous dataset return only for small, bounded calls.

REST, small synchronous run:

```bash
curl -sS -X POST \
  'https://api.apify.com/v2/acts/nomad-agent~company-careers-bundle/run-sync-get-dataset-items?token=YOUR_APIFY_TOKEN' \
  -H 'Content-Type: application/json' \
  -d '{"companies":["stripe"],"maxItemsPerCompany":25,"maxItems":25}'
```

Python SDK:

```python
from apify_client import ApifyClient

client = ApifyClient("YOUR_APIFY_TOKEN")
run = client.actor("nomad-agent/company-careers-bundle").call(run_input={
    "companies": ["stripe", "openai"],
    "maxItemsPerCompany": 25,
    "maxItems": 100,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript SDK:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/company-careers-bundle').call({
  companies: ['stripe', 'openai'],
  maxItemsPerCompany: 25,
  maxItems: 100,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

Keep tokens in environment variables or a secret manager. Never log, commit, or place real Apify or AI-provider keys in URLs, examples, datasets, or source files. URL-token syntax above is illustrative; prefer an authorization header where the client supports it.

## Interpret output

Treat rows with `title` and `url` as postings. Stable core fields include `ats`, `company`, `id`, `globalId`, `title`, `department`, `location`, `locations`, `city`, `region`, `country`, `url`, `postedAt`, `employmentType`, `remote`, `snippet`, `salary`, `salaryMin`, `salaryMax`, `salaryCurrency`, `salaryPeriod`, `hasEquity`, and `warnings`.

Do not infer missing values:

- `remote` is tri-state. `null` means the source did not state it.
- Salary fields are normally populated only by Ashby and Lever.
- `city`, `region`, and `country` are normally populated only by SmartRecruiters.
- SmartRecruiters and Workday list results have no description snippet.
- `postedAt`, employment type, compensation, or company display name may be absent or source-dependent.
- AI fields—`aiKeySkills`, `aiExperienceLevel`, `aiWorkArrangement`, and `aiVisaSponsorship`—exist only when enrichment succeeds.

Inspect every row's `warnings`. Rows without a posting can report all-provider misses, fetch failures, cap truncation, skipped enrichment, or a recovered run error. Branch on `isClosed`, `warnings`, and the presence of `title`/`url`; do not assume every dataset item is a job.

## Optional AI enrichment

Set `aiEnrichment: true`, select `aiProvider` (`anthropic`, `mistral`, or `openai`), and provide only the matching secret input or Actor environment variable. Default models are `claude-haiku-4-5-20251001`, `mistral-small-latest`, and `gpt-4.1-mini`. AI usage is billed by that provider. If the matching key is absent, the Actor still returns postings and adds a warning row instead of enrichment.

Use enrichment for extraction, not as a guarantee of factual correctness. Retain source URLs and validate consequential decisions against the original posting.

## Handle failures and limits

- A provider miss is expected during ATS probing; the Actor continues across other providers.
- Individual provider/network failures fail open and surface in logs or warning rows. A succeeded run can therefore be partial.
- Cap warning rows mean results were deliberately truncated; increase `maxItemsPerCompany` or `maxItems` only if completeness is worth the added billed results.
- `onlyNewSinceLastRun` is for monitoring, not historical export. Run without it for a full current snapshot.
- Public ATS endpoints can change, rate-limit, omit fields, or return stale data. The Actor does not cover bespoke career sites and does not submit applications.
- Review each ATS provider's terms and applicable privacy, employment, and data-use rules. Store only data needed for the stated purpose.

When troubleshooting, report the run ID, status, selected inputs with secrets redacted, warning rows, and the affected company/provider. Never request or reproduce a user's secret key.
