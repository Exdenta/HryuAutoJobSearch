---
name: academicpositions-scraper
description: Assess, configure, run, and integrate the Apify Actor nomad-agent/academicpositions-scraper for live postdoc, PhD, faculty, and research jobs from academicpositions.com. Use when an agent needs AcademicPositions data, capped or scheduled delta runs, Apify API or SDK examples, output interpretation, or diagnosis of Cloudflare, proxy, filtering, billing-cap, and partial-result behavior.
---

# AcademicPositions Scraper

Use Actor ID `nomad-agent/academicpositions-scraper`.

## Assess fit

Choose this Actor for public job postings on academicpositions.com, especially EU, UK, and Swiss academic and research roles. It returns one flat record per posting and supports scheduled new-only monitoring.

Do not present it as a general academic-web crawler, applicant tracker, enrichment service, or guaranteed complete archive. It only follows detail links found from the configured AcademicPositions listing page. The target site can throttle or block browser sessions, so a run may return partial results.

## Configure a run

Start with the smallest useful paid run:

```json
{
  "keyword": "machine learning postdoc",
  "countryFilter": "Netherlands",
  "maxItems": 10
}
```

- Set `maxItems` from `1` to `500`; use `5` or `10` while testing.
- Use `keyword` as free text appended to the listing URL.
- Use `countryFilter` as a case-insensitive substring filter on the extracted location. Records with no matching location are excluded.
- Use `postedSince` as `YYYY-MM-DD`. Records with missing or unparseable dates remain included.
- Set `onlyNewSinceLastRun: true` for monitoring. The first flagged run emits all found items; later flagged runs omit previously seen `globalId` values and mark emitted rows `isNew: true`.
- Keep the default Apify Residential `proxyConfiguration` for reliability. `useResidentialProxy` is deprecated and matters only when `proxyConfiguration` is absent.
- Change `startUrl` only for a specific AcademicPositions listing or saved search.
- Leave `detailUrlContains` as `/ad/` unless the site's detail URL structure changes.

Respect the platform's maximum total charge. A charge cap can stop delivery before `maxItems`; do not interpret that as exhaustion of matching listings.

## Run through Apify

Keep the token in `APIFY_TOKEN`; never embed it in source, logs, prompts, URLs committed to version control, or user-visible examples.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/academicpositions-scraper").call(
    run_input={"keyword": "postdoc", "maxItems": 10}
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/academicpositions-scraper').call({
  keyword: 'postdoc',
  maxItems: 10,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

For REST, prefer an `Authorization: Bearer` header over a token query parameter:

```bash
curl -X POST \
  'https://api.apify.com/v2/acts/nomad-agent~academicpositions-scraper/run-sync-get-dataset-items' \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"maxItems":10,"keyword":"postdoc"}'
```

Use asynchronous Actor runs instead of the synchronous endpoint when caller timeouts are short. Read results from `defaultDatasetId` only after a successful terminal run.

## Interpret output

Expect `title`, `company`, `location`, `field`, `url`, `postedAt`, `snippet`, `salary`, `salaryMin`, `salaryMax`, `salaryCurrency`, `globalId`, `isNew`, and `source`.

- Treat `title` and `url` as the dependable identifiers for emitted job rows.
- Treat nullable fields as genuinely unavailable, not empty or inferred data.
- `field` is source-labelled, never inferred from the title.
- Salary fields exist only when structured salary data is disclosed; they are usually null.
- `snippet` can contain the full plain-text description despite its name.
- `globalId` is derived from the canonical detail path and is the delta-mode deduplication key.
- `isNew` is absent/null outside delta mode and always true on emitted delta rows; previously seen rows are not emitted.
- Normal extracted rows use source `academicpositions`; fallback extraction may use the page hostname.

Do not assume dataset rows are all jobs without checking their shape. If the listing cannot be reached, the Actor can emit an uncharged diagnostic row containing `warning` and `source` and set an explanatory status message.

## Diagnose runs

Inspect run status, status message, logs, dataset shape, and item count together.

- Empty successful delta run: often means no unseen postings; confirm this was not the first flagged run.
- Warning row or zero jobs after listing failure: Cloudflare or connectivity prevented a valid listing load; retry later and retain Residential proxy settings.
- Fewer rows than `maxItems`: filters, missing extraction, rate limiting, run time budget, exhausted listing pages, or maximum-charge limits can all explain it.
- Missing dates or salaries: expected when the source omits structured values.
- Direct/no-proxy instability: use the default Residential proxy before changing scraper-specific settings.
- Repeated scheduled results: confirm every relevant run uses `onlyNewSinceLastRun: true` and the same Actor/account state.

Retry transient target-site failures conservatively. Do not increase concurrency or hammer failed pages; the Actor deliberately paces navigation and rotates blocked proxy sessions. Review academicpositions.com's terms and applicable privacy and employment-data rules before storing or redistributing results.
