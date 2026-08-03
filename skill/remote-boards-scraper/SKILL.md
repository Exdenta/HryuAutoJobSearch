---
name: remote-boards-scraper
description: Assess, configure, run, integrate, and troubleshoot the Apify Actor nomad-agent/remote-boards-scraper, which merges remote jobs from RemoteOK, Remotive, WeWorkRemotely, and Himalayas. Use for remote-job feeds, capped or scheduled delta runs, Apify API or SDK examples, webhook delivery, output interpretation, or diagnosis of filters, source outages, sentinel rows, caching, deduplication, and result limits.
---

# Remote Boards Scraper

Use Actor ID `nomad-agent/remote-boards-scraper`.

## Assess fit

Choose this Actor for a normalized feed of public remote jobs from four boards:
RemoteOK, Remotive, WeWorkRemotely, and Himalayas. It is useful for job alerts,
newsletters, hiring research, and downstream candidate matching.

Do not present it as an application bot, ATS, candidate database, exhaustive
archive, or guaranteed-complete market dataset. WeWorkRemotely coverage is its
programming, full-stack, back-end, front-end, and DevOps/SysAdmin RSS feeds.
Upstream feeds can be unavailable or blocked, and source fields vary.

## Configure a run

Start with a capped test:

```json
{
  "keyword": "machine learning, data scientist",
  "postedSince": 14,
  "maxItemsPerSource": 10,
  "maxItems": 25
}
```

- `keyword` is a case-insensitive comma-separated OR filter over title,
  company, and category. Hyphens and spaces are interchangeable. It does not
  search tags or descriptions.
- `titleExclude` is an array of case-insensitive substrings excluded from the
  same title/company/category text.
- `postedSince` accepts `0` to disable filtering or up to `365` days. Dates are
  filtered after fetching; missing or unparseable dates remain included.
- Use `enableRemoteOK`, `enableRemotive`, `enableWeWorkRemotely`, and
  `enableHimalayas` to select sources. All default to true.
- `maxItemsPerSource` defaults to `50` and clamps at `500`; `maxItems` defaults
  to `150` and clamps at `2000`. Zero disables the corresponding result cap,
  although Himalayas still has a 500-row fetch safety ceiling.
- `cacheTtlSeconds` defaults to `1800`; set it to `0` only when every run must
  fetch live. Negative values are treated as zero.
- Set `onlyNewSinceLastRun: true` for repeat alerts. The seen set is persistent
  for this Actor/account and is not separately keyed by search profile. A
  different filtered search can therefore share listing history.
- Set `webhookUrl` to an HTTPS endpoint for best-effort delivery after dataset
  write. Real listings are posted in batches of 50 as
  `{items, offset, count, total}`. `webhookSecret` becomes the
  `X-Webhook-Secret` header. Webhook failure does not fail the run.

Filters run before per-source caps. The total cap is applied after cross-source
deduplication in source order: RemoteOK, Remotive, WeWorkRemotely, Himalayas.
Use small limits first and verify current Store pricing before estimating cost.

## Run through Apify

Keep the token in `APIFY_TOKEN`; never embed or log it.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/remote-boards-scraper").call(
    run_input={"keyword": "python", "maxItems": 25}
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
jobs = [item for item in items if not item.get("_sentinel")]
```

JavaScript:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/remote-boards-scraper').call({
  keyword: 'python',
  maxItems: 25,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
const jobs = items.filter((item) => !item._sentinel);
```

REST:

```bash
curl -X POST \
  'https://api.apify.com/v2/acts/nomad-agent~remote-boards-scraper/run-sync-get-dataset-items' \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"keyword":"python","maxItems":25}'
```

Prefer asynchronous runs when caller timeouts are short. Read the dataset only
after the run reaches a terminal status.

## Interpret output

Normal rows contain `source`, `id`, `title`, `company`, `companyLogo`,
`location`, `url`, `postedAt`, `snippet`, `salary`, `salaryMin`, `salaryMax`,
`salaryCurrency`, `salaryPeriod`, `jobType`, `category`, `tags`, and
`scrapedAt`.

- `source` is `remoteok`, `remotive`, `weworkremotely`, or `himalayas`.
- Treat nullable source fields as unavailable, not inferred data.
- `postedAt` preserves or derives the source timestamp format; it is not
  normalized consistently across all boards.
- `snippet` is plain text and may contain the full available description.
- Structured numeric salary fields are available only for RemoteOK and
  Himalayas. Remotive salary is free text; WeWorkRemotely provides none.
- `jobType` comes only from Remotive and Himalayas. `category` comes only from
  those sources. WeWorkRemotely supplies neither tags nor a company logo.
- Cross-source deduplication primarily normalizes listing URLs, then falls back
  to source ID and title/company. It does not guarantee semantic deduplication
  when boards use unrelated URLs for the same role.

Always remove rows with `_sentinel: true` before treating the dataset as jobs.
An enabled source that fetched no raw rows emits an uncharged source sentinel
with `warning` and `docs`. If feeds worked but all listings were removed by
filters, delta state, or caps, the Actor emits one run-level sentinel instead.

## Diagnose runs

Inspect terminal status, logs, dataset rows, enabled sources, and input together.
A successful run does not prove every source succeeded: source fetch errors,
cache failures, webhook errors, and delta-state failures are handled
independently and usually fail open.

- Source sentinel: retry the source later and inspect its fetch warnings.
- Run-level sentinel: loosen `keyword`, `titleExclude`, or `postedSince`; check
  whether delta mode already saw every matching listing.
- Fewer rows than requested: filters, cross-source deduplication, per-source or
  total caps, upstream feed size, the Himalayas fetch ceiling, and platform
  time or charge limits can all reduce delivery.
- Repeated rows in delta mode: persistent-state access failed open, the listing
  identity changed, or earlier runs did not use delta mode.
- Missing salary, category, job type, logo, or date: often normal source
  behavior; do not synthesize values without the user's request.
- Webhook missing while dataset is populated: inspect webhook warnings and the
  endpoint response; dataset delivery remains authoritative.

Retry transient source failures conservatively. Review each board's terms and
applicable employment-data and privacy rules before storing or redistributing
results.
