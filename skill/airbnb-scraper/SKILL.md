---
name: airbnb-scraper
description: Assess, configure, run, and integrate the Apify Actor nomad-agent/airbnb-scraper for Airbnb location or map searches, listing-detail enrichment, availability calendars, reviews, and new-listing monitoring. Use when an agent must decide whether this Actor fits an Airbnb data task, create cost-capped Actor input, call it through the Apify API or SDK, consume its dataset, or diagnose partial, blocked, empty, or degraded runs.
---

# Airbnb Scraper

Use Actor `nomad-agent/airbnb-scraper` to collect public Airbnb listing data without login or cookies.

## Assess fit

- Use `search` for discovery by named location or latitude/longitude map box. It supports dates, guests, nightly price bounds, room type, minimum bedrooms/beds/bathrooms, pagination, and recursive price-range splitting.
- Use `detail` for known Airbnb room URLs or numeric listing IDs. It adds host, capacity, Superhost status, description, image/amenity counts, and optional calendar/reviews.
- Use search with `onlyNewSinceLastRun: true` for recurring discovery. The Actor retains up to about 50,000 seen IDs in its named key-value store and returns only newly seen listings.
- Do not claim property-type filtering, booking, private/account data, historical prices, exact market completeness, or per-night pricing when Airbnb labels a dated result as a stay total.

## Configure a bounded run

Start small, inspect results, then raise caps. Prefer Apify Residential proxy because Airbnb frequently blocks datacenter/shared IPs.

Search example:

```json
{
  "mode": "search",
  "location": "Lisbon, Portugal",
  "checkIn": "2026-09-23",
  "checkOut": "2026-09-28",
  "priceMax": 150,
  "roomType": "entire_home",
  "maxItems": 20,
  "maxPriceSplits": 2,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

For a map search, set both `latitude` and `longitude`; omit `location` if desired. `maxLocationDeviation` is the map-box half-width in degrees and defaults to `0.05`.

Detail example:

```json
{
  "mode": "detail",
  "listingUrls": ["https://www.airbnb.com/rooms/796929"],
  "calendarMonths": 1,
  "includeReviews": true,
  "maxReviewsPerListing": 10,
  "maxItems": 1,
  "concurrency": 1,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

Use `maxItems` as the hard listing cap. Lower `maxPriceSplits` to limit extra search requests in dense markets. Keep detail `concurrency` at 1 or 2; values up to 8 are accepted but increase blocking risk. Bound enrichment with `calendarMonths` from 0 to 12 and `maxReviewsPerListing` from 1 to 200. `onlyNewSinceLastRun` has no effect in detail mode.

Supported `roomType` values are `any`, `entire_home`, `private_room`, `shared_room`, and `hotel_room`. Dates use `YYYY-MM-DD`. Currency is an ISO code such as `USD` or `EUR`.

## Run and retrieve results

Keep the Apify token in an environment variable or secret manager. Never paste it into source, logs, prompts, or committed configuration.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/airbnb-scraper").call(run_input={
    "mode": "search",
    "location": "Lisbon, Portugal",
    "maxItems": 20,
    "maxPriceSplits": 2,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

HTTP API:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"mode":"detail","listingUrls":["796929"],"maxItems":1}' \
  "https://api.apify.com/v2/acts/nomad-agent~airbnb-scraper/runs"
```

Prefer the asynchronous runs endpoint for automation: poll the returned run ID until terminal status, then fetch `/v2/datasets/{defaultDatasetId}/items`. Use `run-sync-get-dataset-items` only when the caller can tolerate waiting for the whole run.

## Interpret output

Each normal dataset row is one flat listing. Common fields are `id`, `url`, `title`, `propertyType`, `price`, `currency`, `pricePer`, `rating`, `reviewsCount`, `roomType`, `beds`, `bedrooms`, `baths`, `lat`, `lng`, `thumbnail`, `imagesCount`, and `mode`.

Treat nullable fields as unavailable, not zero. Search rows normally lack detail-only values such as `hostName`, `amenitiesCount`, `personCapacity`, `isSuperhost`, and `description`. `calendar` is populated only for successful detail enrichment with `calendarMonths > 0`; `reviews` only when review enrichment succeeds. Delta search rows set `isNew: true`.

Inspect `pricePer` before comparing prices: dated searches may expose a stay `total`, not a nightly price. Airbnb may return fewer rows than `maxItems`; price splitting mitigates its roughly 270-result query ceiling but cannot guarantee exhaustive coverage.

The Actor deliberately reports many input, proxy, block, and upstream failures as a successful run with partial data or an uncharged diagnostic row containing `warning` and `docs`. Therefore check dataset rows, run status message, and logs—not terminal status alone. Preserve already returned rows when a later request is blocked.

## Handle limitations and failures

- On 403/429/challenge or partial output, use Residential proxy, reduce detail concurrency to 1, narrow the search, and retry.
- If a selected proxy group is unavailable to the account, the Actor stops without silently switching exit geography and should bill nothing.
- If `calendar` or `reviews` is null, explain that both depend on best-effort, unauthenticated internal Airbnb GraphQL queries whose persisted hashes can change; the rest of the detail row may still be valid.
- Expect public page structure and availability to change, geographic price variation by proxy exit, nullable fields, removed listings, and results ordered by Airbnb rather than guaranteed completeness.
- Review Airbnb terms and applicable privacy, database, and scraping law. Collect only what the use case requires and avoid republishing personal data without a lawful basis.
