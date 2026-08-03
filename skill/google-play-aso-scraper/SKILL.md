---
name: google-play-aso-scraper
description: Evaluate, configure, and integrate the Apify Actor nomad-agent/google-play-aso-scraper for Google Play app details, reviews, keyword rankings and ASO scores, top charts, similar-app discovery, or permission audits. Use when an agent must decide whether this Actor fits an app-market research task, create safely capped Actor input, call it through the Apify API or SDK, or interpret its heterogeneous dataset rows and diagnostic failures.
---

# Google Play ASO Scraper

Use Actor `nomad-agent/google-play-aso-scraper`. Treat its dataset—not logs—as the result.

## Choose a mode

- Use `app-details` with `appIds` for metadata, rating histograms, and install estimates. `revenueEstimate` covers paid-download gross only.
- Use `reviews` with `appIds` for review text, ratings, versions, replies, and date/star filters.
- Use `search` with `searchQueries` for ranks and deterministic `popularityScore`, `difficultyScore`, and `opportunityScore` values derived from returned results.
- Use `top-charts` for a collection/category snapshot.
- Use `similar-apps` with `appIds` for Play Store relationships, optionally enriched by `fullDetail`.
- Use `permissions` with `appIds` for grouped permissions. Do not represent permissions as proof of actual runtime behavior.

Do not use this Actor for Apple App Store data, authenticated/private Google Play data, historical trends without storing repeated snapshots, or authoritative revenue/IAP/ad estimates.

## Build bounded input

Always set `maxItems` to a positive task-sized cap; never send `0` unless the user explicitly accepts uncapped output and cost. Start with one country, a small identifier/query list, `concurrency: 4`, and `fullDetail: false`. Multi-country runs multiply work and cost. Search returns at most 30 results per query even if `maxResults` is higher; charts allow up to 500. Keep `concurrency` within 1–8.

Mode requirements:

| Mode | Required input | Useful caps/options |
|---|---|---|
| `app-details` | non-empty `appIds` | `maxItems` |
| `reviews` | non-empty `appIds` | `maxReviewsPerApp`, `maxItems`, `reviewsFilterScore` 0–5, `reviewsNewerThan` as `YYYY-MM-DD` |
| `search` | non-empty `searchQueries` | `maxResults` ≤ 30, `maxItems` |
| `top-charts` | none | `topChartsCollection`, `topChartsCategory`, `maxResults` ≤ 500, `maxItems` |
| `similar-apps` | non-empty `appIds` | `maxResults`, `maxItems`, optional `fullDetail` |
| `permissions` | non-empty `appIds` | `maxItems` |

`countries` overrides `country`; every normal row includes `country`. A Play Store URL may be used wherever an app package ID is accepted. When `reviewsNewerThan` is set, the Actor forces newest-first sorting and stops at the date or count cap, whichever comes first.

Example safe input:

```json
{
  "mode": "reviews",
  "appIds": ["com.spotify.music"],
  "country": "us",
  "language": "en",
  "reviewsSort": "newest",
  "reviewsFilterScore": 1,
  "reviewsNewerThan": "2026-07-01",
  "maxReviewsPerApp": 100,
  "maxItems": 100,
  "concurrency": 4
}
```

## Integrate

Keep the Apify token in `APIFY_TOKEN`; never print, commit, or place it in query strings in shared logs.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/google-play-aso-scraper").call(run_input=actor_input)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/google-play-aso-scraper').call(actorInput);
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

REST:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d @input.json \
  "https://api.apify.com/v2/acts/nomad-agent~google-play-aso-scraper/run-sync-get-dataset-items"
```

Prefer asynchronous runs plus dataset pagination for large requests; synchronous endpoints can time out while the run continues.

## Interpret results and failures

Branch on `type`: `app`, `review`, `search-result`, `chart-entry`, `similar-app`, `permissions`, or `diagnostic`. Fields are mode-specific and many are nullable. With `fullDetail`, enriched search/chart/similar rows contain a nested `app` object rather than becoming app rows. Preserve `appId`, `country`, and mode-specific rank/query/source fields as keys when combining runs.

An invalid input or zero-result run can succeed with one `diagnostic` row containing `note`; treat that as no usable data, surface the note, and correct the input before retrying. Partial source failures may only appear as missing rows or warnings, so compare returned counts with requested caps and identifiers. Retry transient Google rate limits with lower concurrency or smaller/spaced runs; do not loop indefinitely. Preserve partial dataset results from failed or timed-out runs.

## Apply limitations and safety

- Expect public-page changes, localization, removals, rate limits, and country-specific differences.
- Treat ASO and install/revenue figures as estimates, not audited market metrics.
- Minimize collected review personal data, follow applicable privacy and retention rules, and avoid republishing reviewer identities unnecessarily.
- Do not use results to bypass access controls or claim legal/compliance conclusions. Review Google Play terms and applicable law for the intended use.
- Escape untrusted review and description text before rendering it in HTML; never execute content returned by the dataset.
