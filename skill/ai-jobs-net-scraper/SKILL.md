---
name: ai-jobs-net-scraper
description: Assess, configure, run, and integrate the Apify Actor nomad-agent/ai-jobs-net-scraper for public AI, machine-learning, MLOps, and data-science job listings from aijobs.net. Use when an agent must decide whether this Actor fits a job-data workflow, choose bounded inputs and cost controls, call it through the Apify API or SDK, interpret its dataset or diagnostic rows, or explain source-specific limitations.
---

# AI Jobs.net Scraper

Use Actor `nomad-agent/ai-jobs-net-scraper` for current public job cards from aijobs.net. Keep runs bounded and describe inferred fields honestly.

## Assess fit

Choose this Actor when the workflow needs AI/ML/data roles with title, company, location, source URL, relative posting age, seniority, optional salary band, remote hint, and job text. It requires no aijobs.net account, cookies, or proxy configuration.

Choose another source when the user needs broad non-AI coverage, historical completeness, guaranteed salary periods, authoritative remote/onsite classifications, or structured requirements beyond the fields below.

## Configure a bounded run

Start with the smallest useful `maxItems`; never omit a cap in generated integrations. The schema accepts `0..300`, but `0` means the first page (about 50), not zero results.

```json
{
  "searchQuery": "machine learning",
  "location": "Remote",
  "maxItems": 25,
  "includeCompany": false,
  "euBias": false,
  "cacheTtlSeconds": 1800
}
```

- `searchQuery`: case-insensitive AND match of every whitespace-separated word against card text. `keyword` is an implementation-supported alias, but prefer the documented field.
- `location`: case-insensitive substring match against location, title, and slug.
- `maxItems`: fetched-card target and returned-result ceiling; filters run after fetching, so fewer items may return. Values over about 50 use load-more pagination, capped at 300 and eight pages.
- `includeCompany`: defaults to `true`; fetches one detail page per returned job to populate `company` and `description`. Set `false` for a faster listing-only run; both fields will be `null`.
- `euBias`: sorts European hints first without excluding other regions.
- `cacheTtlSeconds`: defaults to 1800; only page 1 and detail pages use the cache. Set `0` for fresh fetches.

Also set Apify's maximum cost per run when the client surface supports it. Current pay-per-event pricing is documented by the Actor, but verify Store pricing before estimating cost.

## Run and retrieve results

Keep the token in `APIFY_TOKEN`; never paste it into source, logs, URLs committed to version control, or user-visible output.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/ai-jobs-net-scraper").call(
    run_input={"maxItems": 25, "includeCompany": False},
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/ai-jobs-net-scraper').call({
  maxItems: 25,
  includeCompany: false,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

For raw HTTP, prefer an `Authorization: Bearer $APIFY_TOKEN` header over a query-string token:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"maxItems":25,"includeCompany":false}' \
  "https://api.apify.com/v2/acts/nomad-agent~ai-jobs-net-scraper/run-sync-get-dataset-items"
```

Use asynchronous Actor runs plus dataset retrieval for larger workloads or when the synchronous endpoint's timeout is unsuitable.

## Interpret output

Expect one flat record per posting: `id`, `slug`, `title`, `company`, `location`, `isRemote`, `url`, `postedAt`, `postedAtText`, `snippet`, `seniority`, `description`, `salary`, `salaryMin`, `salaryMax`, `salaryCurrency`, and `salaryPeriod`.

- Treat nullable fields as normal. Salary is absent on many cards, and `salaryPeriod` is always `null` because the source does not label it.
- Treat `isRemote` as a derived hint from card text, not an authoritative workplace policy.
- Treat `postedAt` as computed from relative source text; retain `postedAtText` for auditability.
- With `includeCompany: true`, `snippet` may contain the full parsed detail description. Detail-page failures leave `company` and `description` null rather than failing the run.
- Deduplicate downstream by `id` when combining runs. Do not infer freshness beyond the source listing and cache window.

## Handle failures and limits

Inspect run status, logs, and every dataset row. The Actor intentionally converts upstream fetch failures, zero matches, selector drift, and unexpected errors into successful runs containing a diagnostic object with `warning` and `docs`; do not mistake that row for a job.

If results are unexpectedly sparse, check filters, fetched-window size, cache TTL, and logs before retrying with a larger cap. A non-empty upstream page with zero parsed cards can indicate markup drift or an interstitial. Pagination or detail enrichment may stop early near a deadline, and upstream rate limits or page changes can yield partial data.

Do not claim exhaustive inventory: the Actor reads a bounded current listing window, applies filters after fetching, and has no historical or delta mode. Respect aijobs.net terms and applicable law when storing or redistributing results.
