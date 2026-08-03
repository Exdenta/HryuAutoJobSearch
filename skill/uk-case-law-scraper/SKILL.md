---
name: uk-case-law-scraper
description: Configure, run, integrate, or evaluate the Apify Actor nomad-agent/uk-case-law-scraper for official UK judgments and tribunal decisions from The National Archives Find Case Law service. Use for keyword, court, party, judge, date-range, or exact neutral-citation searches; full-text and legislation-reference retrieval; scheduled delta monitoring; capped Apify API or SDK calls; and interpretation of judgment or diagnostic dataset rows.
---

# UK Case Law Scraper

Use Actor `nomad-agent/uk-case-law-scraper`. It searches The National Archives' unauthenticated Find Case Law Atom API and optionally fetches official Akoma Ntoso XML; calling the Actor still requires an Apify token.

## Decide fit

Use it for published UK court judgments and tribunal decisions, neutral citations, parties, judges, court/date filters, official links, full judgment text, and structured legislation references.

Do not present it as BAILII coverage, a legislation scraper, legal advice, a guarantee of archive completeness, or a computational-analysis licence. Find Case Law coverage varies by collection. For bulk text/data mining, check the Open Justice Licence and any required National Archives licence.

## Build bounded input

Always set `maxItems` to a task-sized cap. Its default is `50`; `0` means unlimited. Start without `includeFullText` unless the task needs parties, judges, body text, or legislation references.

Choose one search path:

- Set `citation` for an exact neutral-citation lookup such as `[2024] UKSC 1`. It overrides `query`, `courts`, `party`, `judge`, `fromDate`, and `toDate`. Matching ignores case and repeated whitespace. If no exact citation is found, the Actor returns up to 25 raw search hits tagged `citationExactMatch: false`; do not treat those as the requested case.
- Otherwise set optional `query`, `courts`, `party`, `judge`, `fromDate`, and `toDate`. An empty query browses the latest judgments matching the other filters. Use `YYYY-MM-DD` dates.

Example metadata search:

```json
{
  "query": "data protection",
  "courts": ["uksc", "ewca/civ"],
  "fromDate": "2024-01-01",
  "maxItems": 100
}
```

Set `includeFullText: true` to add `parties`, `judges`, `fullText`, and `legislationRefs`. Detail requests run at `concurrency` `1` to `8` (default `4`). A detail failure keeps the metadata row and returns empty enrichment fields; `fullText` is truncated to 400000 characters.

For monitoring, set `onlyNewSinceLastRun: true` and schedule the same input. Returned judgments receive `isNew: true`; already-seen IDs are skipped before detail fetching and are not pushed or billed. Delta state is shared across all runs and queries of this Actor in one named store, retains up to 50000 IDs, and keys by `fclId`, then `slug`, then `citation`. Because `maxItems` caps judgments considered, a delta run can return fewer new rows than the cap.

## Integrate safely

Keep `APIFY_TOKEN` in an environment variable or secret store. Never print, commit, or put it in a query-string URL.

Python:

```python
import os
from apify_client import ApifyClient

actor_input = {
    "citation": "[2024] UKSC 1",
    "includeFullText": True,
    "maxItems": 1,
}
client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/uk-case-law-scraper").call(
    run_input=actor_input
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

REST:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"query":"negligence","courts":["uksc"],"maxItems":50}' \
  "https://api.apify.com/v2/acts/nomad-agent~uk-case-law-scraper/run-sync-get-dataset-items"
```

Prefer an asynchronous run followed by dataset pagination for broad or full-text requests; synchronous HTTP calls can time out while the run continues.

## Interpret output

Treat the dataset as the result. A judgment metadata row can contain:

- `source`, `title`, `citation`, `court`, `date`, `url`, `xmlUrl`, and `pdfUrl`
- `slug`, `fclId`, `documentUri`, `contentHash`, and `updatedAt`
- `parties`, `judges`, `fullText`, and `legislationRefs` when full text is requested
- `isNew` in delta mode and `citationExactMatch` in citation mode

`legislationRefs` contains distinct `{text, uri, canonical}` objects parsed from official enriched XML. `contentHash` and `updatedAt` help detect upstream revisions; delta mode tracks seen IDs, not content-hash changes.

A zero-match or recovered unexpected error can produce a non-billed row with `diagnostic: true` and `message`. Exclude it from judgment records and surface the message. Network/search failures can yield partial data or a diagnostic row while the run reports `SUCCEEDED`; report both valid-row count and diagnostics.

Return the bounded Actor input, chosen integration method, expected fields, and relevant citation, delta, full-text, and licensing caveats with any recommendation.
