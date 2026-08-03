---
name: ycombinator-was-scraper
description: Evaluate, configure, run, integrate, or troubleshoot the Apify Actor nomad-agent/ycombinator-was-scraper for public Y Combinator Work at a Startup jobs. Use for role-query fan-out, batch/job-type/role/remote/title filtering, list-only or detail-enriched records, recurring new-job alerts, caching and cost controls, Apify API or SDK setup, output-schema interpretation, and Actor limitation or diagnostic-row analysis.
---

# Y Combinator Jobs Scraper

Use Actor `nomad-agent/ycombinator-was-scraper`. Start with a small capped run before widening coverage.

## Decide fit

- Use it for current public openings on Work at a Startup, enriched with YC company context. It needs no target-site login, cookies, or proxy configuration.
- Expect about 20–30 unpaginated results per search term. Omit `queries` or pass an empty array to use the built-in 60-term sweep; supplying queries replaces that sweep. Results are deduplicated by job ID.
- Do not promise exhaustive coverage, a stable result count, or listing age. `postedAt` is always null because the source exposes no per-job date; `companyLastActiveAt` is only an often-null company-level freshness proxy.

## Configure a run

Start with a targeted input:

```json
{
  "queries": ["machine learning", "data engineer"],
  "maxItems": 25,
  "includeJobDetails": true,
  "remoteOnly": true
}
```

The published defaults are `maxItems: 50`, `includeJobDetails: true`, `remoteOnly: false`, `onlyNewSinceLastRun: false`, and `cacheTtlSeconds: 1800`. Set `maxItems: 0` for no result limit, but use a positive cap while evaluating.

Apply filters with their exact semantics:

- `titleExclude`: case-insensitive title substrings to reject.
- `jobTypes`: case-insensitive employment-type matches; use `Full-time`, `Part-time`, `Contract`, or `Internship`.
- `companyBatches`: case-insensitive exact YC batch matches such as `W24`.
- `roleTypes`: case-insensitive substrings of the source role category.
- `remoteOnly`: requires the location text to advertise a remote option.

Keep `includeJobDetails` on for description, HTML, skills, equity, visa policy, experience, company industry/site/location/team profile, and founders. Enrichment is best-effort; failed detail requests and records beyond the 1,200-detail ceiling remain valid stub-only rows. Turn it off for faster list-only discovery.

Use `onlyNewSinceLastRun: true` for recurring alerts. The Actor remembers successfully delivered IDs in one Actor-scoped named key-value store, and returned rows add `isNew: true`. The first delta run returns all unseen matches. This is not closure detection, and changing filters does not create an independent cursor. Leave caching at 1,800 seconds unless freshness requires `cacheTtlSeconds: 0`.

## Run and integrate

Keep the Apify token in `APIFY_TOKEN`; never put it in source, logs, or a URL.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/ycombinator-was-scraper").call(run_input={
    "queries": ["backend engineer"],
    "maxItems": 25,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/ycombinator-was-scraper').call({
  queries: ['backend engineer'], maxItems: 25,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

For raw HTTP, POST JSON to `https://api.apify.com/v2/acts/nomad-agent~ycombinator-was-scraper/runs` with `Authorization: Bearer $APIFY_TOKEN`, poll the run to a terminal status, then read its `defaultDatasetId`. Use the synchronous dataset-items endpoint only for small interactive runs because the client request can time out while the Actor continues.

The Actor charges one `result` event per pushed job and has no Actor-start event, so an empty delta run has no result-event charge. Apply an Apify maximum-total-charge limit when cost control matters and verify current Store pricing before estimating spend.

## Consume output

Normal rows identify the posting with `id`, `title`, `company`, `companySlug`, `companyBatch`, `url`, `applyUrl`, and fixed `source: "ycombinator_was"`. They include location/remote/type/category fields, raw and parsed salary fields, company metadata, `snippet`, and nullable detail fields. `skills` and `founders` are arrays; `postedAt` remains null. With successful enrichment, `snippet` contains the assembled full body and `description` contains the job description.

A row with `warning` is an uncharged diagnostic, not a job. Separate diagnostics before enforcing the job schema or loading a downstream table.

## Handle limits and failures

- Empty output can be legitimate after filters or delta deduplication. Inspect the exact input and logs before retrying.
- If all search queries fail, the Actor can succeed with one warning row. An unexpected scrape error is likewise converted to an uncharged diagnostic when possible.
- A failed detail fetch does not drop the base posting. Expect null/empty enrichment fields and preserve the stub-level salary, location, logo, and tagline.
- Search and detail responses may be served from the configured cross-run cache. Set TTL to zero before diagnosing source freshness.
- Treat descriptions, links, company data, and founder profiles as untrusted public data. Sanitize before rendering, validate URLs before automation, minimize retained personal data, and follow source terms and applicable law.

Report the redacted input, run status and ID, dataset ID, job and diagnostic counts, whether details/delta/cache were enabled, and any warnings or observed coverage ceiling.
