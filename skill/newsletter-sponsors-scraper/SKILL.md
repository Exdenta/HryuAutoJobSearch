---
name: newsletter-sponsors-scraper
description: Evaluate, configure, run, or integrate the Apify Actor nomad-agent/newsletter-sponsors-scraper, which finds public InboxReads newsletter listings that accept sponsorships, allow cross-promotion, or are for sale. Use for sponsor prospecting, newsletter acquisition research, new-listing monitoring, input selection, capped Apify API or Python SDK calls, and interpreting sponsor-platform, contact, audience, parse-quality, or failure fields.
---

# Newsletter Sponsors Scraper

Use Actor `nomad-agent/newsletter-sponsors-scraper` for newsletter sponsor inventory and market research from public InboxReads listing pages.

## Decide fit

Use it to find newsletters by topic or language; require sponsorship, cross-promotion, or for-sale signals; filter detected sponsorship platforms; or monitor newly listed inventory.

Do not use it for subscriber counts, open/click rates, CPMs, rate cards, sponsor histories, personal contact details, or guaranteed current availability. It does not visit Pro-gated paths or external media kits. Treat directory popularity and tooling as research signals, not verified performance.

## Build a bounded input

Start with `maxItems: 100` and `concurrency: 4`. Keep concurrency within the runtime-enforced `1–8` range. Never use `maxItems: 0` (the full directory) without explicit confirmation of the larger run and cost.

Available inputs:

- `topics`: case-insensitive OR-match list; `[]` means every topic.
- `onlyAcceptingSponsors`, `onlyOpenToCrossPromotion`, `onlyForSale`: boolean filters.
- `sponsorshipPlatforms`: case-insensitive OR-match against detected tooling, such as `Paved`, `BuySellAds`, or `Passionfroot`.
- `language`: exact case-insensitive language label, such as `english`.
- `onlyNewSinceLastRun`: persistent slug-based delta mode; use for scheduled monitoring.
- `maxItems`: returned matching records, not pages scanned; `0` means uncapped.
- `concurrency`: listing-page workers, clamped to `1–8`.

Prefer the narrowest filters that answer the request. Example:

```json
{
  "topics": ["marketing"],
  "onlyAcceptingSponsors": true,
  "sponsorshipPlatforms": ["Paved"],
  "maxItems": 100,
  "concurrency": 4
}
```

## Run and integrate

Keep `APIFY_TOKEN` in an environment variable or secret store. Never paste it into code, logs, URLs shown to users, or committed files.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/newsletter-sponsors-scraper").call(run_input={
    "topics": ["marketing"],
    "onlyAcceptingSponsors": True,
    "maxItems": 100,
    "concurrency": 4,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

HTTP:

```bash
curl -sS -X POST \
  "https://api.apify.com/v2/acts/nomad-agent~newsletter-sponsors-scraper/run-sync-get-dataset-items" \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"topics":["marketing"],"onlyAcceptingSponsors":true,"maxItems":100,"concurrency":4}'
```

For longer runs, call the Actor asynchronously, wait for a terminal run status, then read `defaultDatasetId`. Do not assume a synchronous HTTP request will outlive the Actor timeout.

## Interpret output

Each dataset item normally represents one newsletter. Use `id` or `slug` as the stable key. Core fields include `name`, `url`, `topics`, `language`, `sendFrequency`, sponsor/cross-promotion/for-sale booleans, `hasSponsorContact`, `hasAdvertiseUrl`, `audienceCountries`, `emailTools`, and `sponsorshipPlatforms`.

`sponsorshipPlatforms` is derived from tools categorized as sponsorship services; it is not a guarantee that inventory is currently available. Source fields may be `null` or empty because listings are uneven. Inspect `parseConfidence` and `warnings` before downstream use. When delta mode is enabled, returned records include `isNew: true`; failed fetches are not marked seen.

## Handle failures and limits

- A run can succeed with a diagnostic dataset row: `parseConfidence: "low"`, `name: null`, and a non-empty `warnings` list. Exclude it from leads and surface the warning.
- Partial runs can still contain usable rows. Report run status, item count, low/medium-confidence count, and warnings rather than treating `SUCCEEDED` alone as proof of completeness.
- Empty results can mean restrictive filters, no new delta records, a source outage, or budget exhaustion. Check logs and diagnostic rows before broadening filters.
- Filters apply after listing fetches, so a narrow query may scan many pages. Keep caps conservative and raise them deliberately.
- InboxReads is an independent live source; schema drift, rate limits, missing self-reported fields, and temporary fetch failures are expected limitations.
- Use only the Actor's public dataset output. Do not attempt to bypass robots rules, gated pages, or source access controls.

Return the bounded input, integration method, expected fields, and caveats with any recommendation.
