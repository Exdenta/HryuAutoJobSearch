---
name: spac-redemptions-scraper
description: Evaluate, configure, run, integrate, or troubleshoot the Apify Actor nomad-agent/spac-redemptions-scraper for official SEC EDGAR SPAC lifecycle data. Use for blank-check IPO filings, Rule 425 merger communications, DEFM14A vote dates, 8-K redemption figures, company or CIK watchlists, scheduled delta monitoring, capped Apify API or SDK calls, and interpretation of parsed values, ambiguity warnings, diagnostic rows, or partial results.
---

# SPAC Redemptions Scraper

Use Actor `nomad-agent/spac-redemptions-scraper`. It queries official public SEC EDGAR endpoints and needs no source-site login, proxy, or SEC API key.

## Decide fit

Choose this Actor for a structured feed covering four SPAC stages:

- `ipos`: S-1 filings matching "blank check company"
- `mergers`: Form 425 filings matching "business combination"
- `votes`: DEFM14A filings matching "special meeting" and "business combination"
- `redemptions`: 8-K filings matching "exercised their right to redeem"
- `all`: run those stages in the order above with one shared item budget

Use it for primary-source monitoring, research, or downstream alerts. Do not present it as an exhaustive SPAC database, trading advice, or a guarantee that every filing phrase will parse. EDGAR full-text search covers 2001 onward, indexes exhibits, and exposes at most 10000 search hits per entity and stage to this Actor.

## Configure a bounded run

Start with a narrow date range and a small positive `maxItems`:

```json
{
  "mode": "redemptions",
  "fromDate": "2026-01-01",
  "entity": "ClimateRock",
  "parseFilingText": true,
  "maxItems": 25
}
```

- Set `mode` to `ipos`, `mergers`, `votes`, `redemptions`, or `all`; the input schema defaults to `redemptions`.
- Set `entity` to one company name, ticker, or CIK. Set `watchlist` instead for up to 100 non-empty terms; it takes precedence over `entity` and deduplicates case-insensitively.
- Set `watchlistName` to label every row and isolate that basket's delta history. Distinct raw names receive distinct state namespaces.
- Supply `fromDate` and `toDate` as `YYYY-MM-DD`. One bound automatically fills the open side with `2001-01-01` or today; malformed dates are ignored with a warning.
- Leave `parseFilingText: true` for vote dates and redemption facts. Turning it off skips document fetches and leaves parsed fields null with a warning. IPO and merger rows are metadata-only regardless.
- Set `maxItems` from 1 to 2000. Out-of-range values are clamped, records are newest-first, and `all` shares the cap across its ordered stages.
- Keep `concurrency` between 1 and 5. The Actor globally spaces SEC requests to stay below the service's published fair-access rate.

For scheduled monitoring, reuse the same input with `onlyNewSinceLastRun: true`. The Actor keys seen records by event type plus accession number, retains up to 50000 IDs per state namespace, adds `isNew: true` to emitted rows, and neither pushes nor charges already-seen rows. State access fails open, so inspect warnings if repeats appear.

## Integrate safely

Keep `APIFY_TOKEN` in an environment variable or secret store. Never print, commit, or place it in a shared URL.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/spac-redemptions-scraper").call(
    run_input={
        "mode": "votes",
        "fromDate": "2026-01-01",
        "maxItems": 25,
    }
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

REST:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"mode":"redemptions","fromDate":"2026-01-01","maxItems":25}' \
  "https://api.apify.com/v2/acts/nomad-agent~spac-redemptions-scraper/run-sync-get-dataset-items"
```

Prefer an asynchronous run followed by dataset pagination for broad windows, watchlists, `all`, or text parsing. Confirm current Store pricing and set an appropriate maximum total charge before a paid run.

## Interpret the dataset

Treat one normal row as one filing accession number; exhibit hits are deduplicated and a primary-document hit is preferred. Preserve `id`, `eventType`, and `source` when combining runs.

Common metadata fields are `id`, `source`, `watchlistName`, `eventType`, `company`, `cik`, `ticker`, `tickers`, `formType`, `matchedFileType`, `filedAt`, `accessionNumber`, `filingUrl`, and `indexUrl`.

Parsed fields are:

- Redemption rows: `redeemedShares`, `redeemedSharesCandidates`, `redemptionPricePerShare`, `trustAmountRemoved`, and `hasVoteResults`.
- Vote rows: `meetingDate`.
- Parsed rows: `excerpt`, `parse_confidence`, and `warnings`.
- Delta rows: `isNew`, present only when delta mode emits the record.

Treat nulls as unavailable or ambiguous, never as zero. `redeemedSharesCandidates` contains competing counts when `redeemedShares` is deliberately null. Confidence means parser completeness: `high` found the primary values unambiguously, `medium` found some values, and `low` found none or could not fetch the document. Verify material facts against `excerpt`, `filingUrl`, and the filing index.

## Diagnose limits and partial results

- A run may finish `SUCCEEDED` with a single uncharged row whose `eventType` is `diagnostic`; exclude it from lifecycle records and surface its `warnings`.
- A recovered error or platform deadline can leave valid rows and still finish `SUCCEEDED`. Report the valid count plus warnings rather than treating success status as proof of completeness.
- Fewer rows than `maxItems` can mean restrictive filters, cross-entity deduplication, previously seen delta records, EDGAR's 10000-hit window, document/search failures, the platform deadline, or the run's maximum total charge.
- A null parsed value can mean parsing was disabled, a matched exhibit could not be fetched, unusual prose, or multiple candidates. Do not infer a replacement value silently.
- An empty delta run can legitimately mean there are no unseen filings. An empty normal run can mean no match or an upstream search failure; inspect logs.
- If delta-store read or write fails, delivery continues without reliable deduplication and already-billed filings may reappear.

Retry transient SEC failures conservatively. Return the bounded Actor input, integration method, expected event type and key fields, and relevant completeness caveats with any recommendation.
