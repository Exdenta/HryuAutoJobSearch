---
name: buyrentkenya-scraper
description: Evaluate, configure, and integrate the Apify Actor nomad-agent/buyrentkenya-scraper for Kenya property listings. Use when an agent must decide whether this Actor fits a real-estate search, monitoring, research, valuation, or feed use case; produce safely capped Actor input; call it through the Apify API or Python/JavaScript SDK; interpret listing and diagnostic rows; or explain detail enrichment, delta mode, pricing, failures, security, robots limits, and data limitations.
---

# BuyRentKenya Scraper

Use Actor `nomad-agent/buyrentkenya-scraper` for public BuyRentKenya sale/rent listings. Prefer a small trial before scaling.

## Decide fit

Use it for Kenyan property discovery, inventory monitoring, market research, and structured downstream feeds. It supports category/location search or specific BuyRentKenya listing URLs.

Do not present it as a valuation service, ownership/title verification, legal due diligence, exhaustive historical dataset, or guaranteed real-time feed. The source can omit, change, or remove fields and listings.

## Build safe input

Choose exactly one mode:

- Search: set `searchType` (`buy` or `rent`), `propertyType` (`all`, `houses`, `flats-apartments`, `land`, `commercial`, or rent-only `bedsitters`), and optional path slug `location`, such as `nairobi/westlands`.
- Detail: set `listingUrls` to full URLs beginning `https://www.buyrentkenya.com/listings/`. Search filters and delta mode do not apply.

Start with `maxPages: 1`, `maxItems: 25`, and `concurrency: 2`. Never exceed the Actor contract: `maxPages` 10, `maxItems` 1000 (`0` means unlimited), and `concurrency` 8. Treat `0` as unlimited only when the user explicitly requests and accepts the cost/volume risk.

Optional search filters are non-negative `minPrice`, `maxPrice`, `minAreaSqm`, `maxAreaSqm`, `minBedrooms`, `minBathrooms`, and boolean `furnished`. Unknown values fail an enabled filter. Land surface values are acres despite the `areaSqm` name. `minBathrooms` or `furnished` automatically enables detail fetching.

Set `fetchDetails: true` only when bathrooms, furnished status, exact posting date, agent phone, images, amenities, or description are needed; it adds one request per listing. Use `onlyNewSinceLastRun: true` only for recurring search runs; the Actor keeps up to about 50,000 seen IDs and only marks successfully pushed rows as seen.

## Run and retrieve

Keep the token in `APIFY_TOKEN`; never place it in source, logs, prompts, or committed input files.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/buyrentkenya-scraper").call(run_input={
    "searchType": "rent",
    "propertyType": "flats-apartments",
    "location": "nairobi/westlands",
    "maxPages": 1,
    "maxItems": 25,
    "fetchDetails": False,
    "concurrency": 2,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/buyrentkenya-scraper').call({
  searchType: 'buy', propertyType: 'houses', location: 'nairobi',
  maxPages: 1, maxItems: 25, fetchDetails: false, concurrency: 2,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

For raw HTTP, POST JSON to `https://api.apify.com/v2/acts/nomad-agent~buyrentkenya-scraper/runs` with `Authorization: Bearer $APIFY_TOKEN`, poll the returned run, then read its `defaultDatasetId`. Prefer this asynchronous flow for runs that may outlast an HTTP request.

## Interpret output

Normal rows can contain `id`, `url`, `title`, `price`, `currency`, `pricePerSqm`, location parts, room counts, surface, property/listing/seller type, agency, market age, posting date, and `source: "buyrentkenya"`. Detail fetching adds or refines `bathrooms`, `furnished`, `agentPhone`, `imagesCount`, `images`, `amenities`, `description`, and exact `postedAt`. Fields may be null.

`isNew` appears only in delta search mode. `_enrichError` means the base search row survived a detail-page failure. A row containing `warning` and `docs` is an uncharged run diagnostic, not a property; branch it away from listing ingestion. Confirm the run status and inspect logs even when the dataset is empty.

## Handle failures and limits

- Empty output can legitimately mean no matches, all unknown values failed filters, delta mode found nothing new, or the charge budget was exhausted.
- Per-detail errors degrade to base records; unexpected run errors may finish successfully with a diagnostic row. Do not equate `SUCCEEDED` with complete data.
- Narrow location/category and reduce pages or details before retrying HTTP/source changes. Respect backoff; do not bypass robots controls or raise concurrency beyond 8.
- Pagination is capped at 10 because the source robots rules permit only pages 2–10. Price/room filters run client-side because filtered query parameters are disallowed.
- Listings are public claims, not verified facts. Phone numbers and descriptions may contain personal data; minimize collection, restrict access/retention, and follow applicable law and source terms.
- Pricing includes Actor-start and per-result events. Check the current Actor Pricing tab before a large or scheduled run; never promise a fixed cost from this skill.
