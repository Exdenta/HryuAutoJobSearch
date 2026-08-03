---
name: infojobs-scraper
description: Use the Apify Actor nomad-agent/infojobs-scraper to find, export, integrate, or monitor public job postings from InfoJobs Spain. Use for Spanish job search, recruiting, salary or labour-market data, province/teleworking/contract/workday filtering, recurring new-job alerts, and Apify API or SDK integration; also use to decide whether this Actor fits a requested workflow and explain its output, costs, failures, and limits.
---

# InfoJobs Scraper

Use `nomad-agent/infojobs-scraper` for public Spain-focused InfoJobs listings. Prefer another source for countries outside Spain, authenticated/private data, complete job descriptions, applicant profiles, or applications.

## Plan the run

1. Confirm the user needs InfoJobs Spain rather than broad multi-country coverage.
2. Start with a capped probe: `maxItems: 15`, `cacheTtlSeconds: 1800`, and `extractSkills: false` unless skill tags are needed. Raise the cap only after inspecting results and expected pay-per-event cost.
3. Set only supported inputs:
   - `keyword` (string) and `province` (InfoJobs province ID; use `""` nationwide).
   - `teleworking`, `contractType`, and `workday` as accent/case-insensitive substrings of labels shown on result cards.
   - `postedSince` as non-negative days; `0` disables freshness filtering.
   - `titleExclude` and `companyExclude` as string arrays.
   - `extractSkills` (default `true`) for fail-open AI tags.
   - `onlyNewSinceLastRun` (default `false`) for persistent delta mode.
   - `maxItems` (default `15`), `timeoutSecs` (default `25`), and `cacheTtlSeconds` (default `1800`).
4. Keep `maxItems` positive and modest. Although `0` means no user cap, the Actor still stops after 50 pages (roughly 500 listings); do not present it as a complete crawl.
5. For recurring alerts, keep the search input stable and enable `onlyNewSinceLastRun`; this state is Actor-managed and retains at most 50,000 offer IDs.

Example input:

```json
{
  "keyword": "data engineer",
  "province": "28",
  "teleworking": "Teletrabajo",
  "postedSince": 7,
  "extractSkills": false,
  "maxItems": 25
}
```

## Run and integrate

Never place an Apify token in source code, logs, chat output, or a URL that may be retained. Read it from `APIFY_TOKEN` and redact it from errors.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/infojobs-scraper").call(run_input=actor_input)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript SDK:

```js
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/infojobs-scraper').call(actorInput);
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

For direct HTTP, POST JSON to `https://api.apify.com/v2/acts/nomad-agent~infojobs-scraper/runs` with `Authorization: Bearer $APIFY_TOKEN`; poll the returned run, then read its default dataset. Use the sync dataset endpoint only for small, bounded runs because the HTTP request must remain open.

## Interpret output

Normal dataset rows may contain `id`, `title`, `company`, `location`, `url`, `postedAt`, `postedAtText`, `salary`, `salaryMin`, `salaryMax`, `salaryCurrency`, `salaryPeriod`, `contractType`, `workday`, `teleworking`, `skills`, `snippet`, `isNew`, and `source: "infojobs"`.

- Treat `postedAt` as computed from relative Spanish text; retain `postedAtText` for audit. Unknown formats produce `postedAt: null` and survive freshness filtering.
- Treat missing salary fields as undisclosed, not zero. `skills: null` means disabled or unavailable; `[]` means extraction found none.
- Deduplicate on `id`, not title. `isNew` appears only for newly emitted delta-mode rows.
- Detect diagnostic rows (`warning`, `docs`, `source`) before consuming rows as jobs. The Actor intentionally reports several upstream, parsing, filtering, and unexpected-error cases as a succeeded run containing one diagnostic row.

## Handle failures and limits

- On a diagnostic row, surface its `warning`, suggest a narrower/broader query as appropriate, and retry transient fetch failures later. Do not claim a successful job result.
- On partial results, preserve the returned rows and disclose that pagination can stop after later-page fetch failures, timeout pressure, or the 50-page ceiling.
- InfoJobs markup, labels, and availability can change. Card snippets are not guaranteed full descriptions, relative dates are approximate, salary is only what the card publishes, and unknown date strings remain unfiltered.
- Facet filters are substring matches over parsed card labels, not guaranteed normalized enums. Province expects an InfoJobs ID, not an arbitrary place name.
- Cache reuse can make results up to `cacheTtlSeconds` old. Set `0` only when the user explicitly needs a live fetch and accepts more source traffic.
- AI skill extraction uses the Actor owner's configured model credential and fails open; never request or expose that credential. Respect InfoJobs terms, applicable law, rate limits, and personal-data rules.
