---
name: devex-jobs-scraper
description: Configure, run, and integrate the Apify Actor nomad-agent/devex-jobs-scraper for international-development, NGO, humanitarian, donor, and UN vacancy discovery or Devex URL alive-checks. Use when evaluating whether this Actor fits a job-data workflow, choosing bounded inputs and schedules, calling it through the Apify API or SDK, or consuming its dataset safely.
---

# Devex Jobs Scraper

Use `nomad-agent/devex-jobs-scraper` for public Devex posting metadata when direct page scraping is unreliable. Prefer another source when full descriptions, consistently populated dates/salaries, or authenticated Devex content is required.

## Choose a mode

- Use discovery by supplying optional `keyword`, `location`, `contractType`, and `workplace`.
- Use monitoring by adding `onlyNewSinceLastRun: true`; keep using the same Actor so its named state store can suppress previously delivered IDs.
- Use alive-check by supplying `jobUrls`; this replaces discovery and returns one verdict per accepted URL.
- Keep `maxItems` explicit and small while testing. The Actor clamps it to `1..50` and charges per delivered result plus an Actor-start event.

Valid `contractType` values: `full-time`, `part-time`, `contract`, `permanent`, `temporary`, `internship`, `consultancy`, `volunteer`. Valid `workplace` values: `on-site`, `hybrid`, `remote`. Do not invent undocumented inputs. `proxyConfiguration` is accepted but currently inert because discovery happens through upstream search providers rather than an Apify proxy.

Start with:

```json
{
  "keyword": "monitoring evaluation",
  "location": "remote",
  "maxItems": 10,
  "onlyNewSinceLastRun": true
}
```

For alive-check, send only canonical public Devex job URLs and a cap:

```json
{"jobUrls":["https://www.devex.com/jobs/example-role-1234567"],"maxItems":10}
```

## Run and retrieve data

Require the caller to provide `APIFY_TOKEN` through environment or secret storage. Never paste, log, commit, or return it.

REST, synchronously returning dataset items:

```bash
curl -sS -X POST \
  "https://api.apify.com/v2/acts/nomad-agent~devex-jobs-scraper/run-sync-get-dataset-items?token=$APIFY_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"keyword":"humanitarian policy","maxItems":10}'
```

JavaScript (`apify-client`):

```js
import { ApifyClient } from 'apify-client';
const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/devex-jobs-scraper').call({ keyword: 'humanitarian policy', maxItems: 10 });
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

Python (`apify-client`):

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/devex-jobs-scraper").call(run_input={"keyword": "humanitarian policy", "maxItems": 10})
items = client.dataset(run["defaultDatasetId"]).list_items().items
```

Use asynchronous `start()` plus run polling for long workflows. Add the client library only if it is already appropriate for the project; REST is sufficient otherwise.

## Consume records defensively

Normal discovery rows contain `source`, `id`, deprecated `externalId`, `title`, `company`, `location`, `url`, `postedAt`, `deadline`, `salary`, `contractType`, `workplace`, `employerWebsite`, and `snippet`. Delta rows also carry `isNew: true`.

Alive-check rows carry `url`, nullable `isActive`, and `checkedAt`. Treat `isActive: null` as unknown, never as closed. A row containing `warning` and `docs` is an unbilled diagnostic, not a job; surface it operationally and retry transient upstream failures with bounded backoff. Deduplicate discovery records by `id` (fall back to `url`), and tolerate missing or nullable fields. `externalId` only mirrors `id` and is deprecated.

## Respect limitations and safety

- Devex blocks direct detail-page fetches. Search metadata can omit descriptions, locations, dates, deadlines, salary, employer sites, and work arrangements; never fill absent facts by guessing.
- Search-backed discovery is not guaranteed exhaustive or perfectly fresh. Validate critical listings at the canonical URL before acting.
- Contract/workplace filters are AI search constraints, not deterministic post-filters.
- Delta state is Actor-scoped, not a complete historical archive; preserve your own dataset when auditability matters.
- Restrict alive-check inputs to public `https://www.devex.com/jobs/...` URLs. Do not send private URLs, credentials, personal data, or arbitrary targets.
- Respect Devex terms, robots policies, applicable law, and downstream retention requirements. Rate-limit schedules and avoid redundant runs.
