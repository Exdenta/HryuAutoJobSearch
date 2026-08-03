---
name: bluesky-scraper
description: Assess, configure, run, and integrate the Apify Actor nomad-agent/bluesky-scraper for public Bluesky posts, author feeds, threads, custom feeds, profiles, followers, and following. Use when choosing whether this Actor fits a Bluesky data task, building capped Actor input, calling it through Apify API or SDK, interpreting its dataset and diagnostics, or troubleshooting its AT Protocol and optional BYOK AI behavior.
---

# Bluesky Scraper

Use Actor `nomad-agent/bluesky-scraper`. Derive production inputs from the Actor's current input schema when available; the contract below is the integration baseline.

## Assess fit

Use this Actor for public Bluesky data without a Bluesky login, cookies, browser, or proxy:

- `search`: keyword/hashtag post search with date, author, mention, language, domain, URL, tag, sort, and engagement filters.
- `authorFeed`: posts from handles or DIDs, optionally including replies.
- `thread`: a post plus replies and optional ancestors.
- `feed`: a custom/algorithmic feed.
- `profile`, `followers`, `following`: public account and graph data.

Choose another approach for private data, DMs, authenticated actions, firehose streaming, historical completeness beyond what public endpoints expose, or guaranteed snapshots. The Actor calls Bluesky's public AppView API, so rate limits, removals, moderation, indexing lag, and upstream availability apply.

## Configure a bounded run

1. Select exactly one `mode` and its required source:
   - `search` requires non-empty `query`.
   - `authorFeed`, `profile`, `followers`, and `following` require `handles`.
   - `thread` requires `threadUris` containing bsky.app post URLs or `at://` post URIs.
   - `feed` requires `feedUris` containing bsky.app feed URLs or generator URIs.
2. Set a positive `maxItems`; never use `0` (unlimited) unless explicitly requested. Start with 10-100 for discovery.
3. Set the Apify run's maximum total charge as a second cost guard. Pricing is pay-per-event and can change; check the Actor page before estimating cost.
4. Add only mode-relevant options. `minLikes` and `minReposts` apply to post modes and filter client-side. `onlyNewSinceLastRun` applies only to `search`, `authorFeed`, and `feed` and uses persisted per-source high-water marks; its first run returns all available records.

Minimal examples:

```json
{"mode":"search","query":"#ai","sort":"latest","maxItems":25}
```

```json
{"mode":"authorFeed","handles":["bsky.app"],"includeReplies":false,"maxItems":25}
```

Search-only inputs are `since`, `until`, `fromAuthor`, `mentionsAuthor`, `language`, `domain`, `url`, `tag`, and `sort` (`latest` or `top`). Thread controls are `threadDepth` (default 6) and `parentHeight` (default 0). A `top` search returns at most one page (up to 100); unauthenticated `latest` search is paginated by walking backward through time.

## Run and consume

Keep `APIFY_TOKEN` in an environment variable or secret store. Do not place it in source, logs, URLs committed to version control, or skill content.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/bluesky-scraper").call(
    run_input={"mode": "profile", "handles": ["bsky.app"], "maxItems": 1},
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

JavaScript SDK:

```javascript
import { ApifyClient } from 'apify-client';

const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
const run = await client.actor('nomad-agent/bluesky-scraper').call({
  mode: 'search', query: '#ai', maxItems: 25,
});
const { items } = await client.dataset(run.defaultDatasetId).listItems();
```

REST (prefer an authorization header so the token is not in the URL):

```bash
curl -X POST \
  'https://api.apify.com/v2/acts/nomad-agent~bluesky-scraper/run-sync-get-dataset-items' \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"search","query":"#ai","maxItems":25}'
```

For longer runs, start asynchronously, poll the run to a terminal status, then read `defaultDatasetId`. Avoid the synchronous endpoint when the client or gateway timeout may be shorter than the Actor run.

## Interpret output

The default dataset contains flat records; branch on `mode` rather than assuming every field exists.

- Post modes include identifiers, URL, author, text, timestamps, engagement, facets, media/embed fields, and reply references.
- `authorFeed` and `feed` add `isRepost` and `feedOf`.
- `thread` adds `threadUri`, `rootUri`, `parentUri`, `depth`, and `isRoot`; rebuild the tree using URIs, not dataset order.
- Profile modes include `did`, `handle`, profile URL, bio/avatar, and timestamps. `profile` adds counts; follower/following rows add `subjectHandle`.
- `note` and `diagnostic` rows are uncharged status records, not scraped entities. Inspect `note` before accepting a run as complete.

`maxItems` is a cap across the whole run, not per handle, thread, or feed. Engagement counts are point-in-time values. Missing mode-specific fields and optional API fields are normal.

## Optional AI analysis

Set `aiAnalysis: true` only for post modes. Supply one of `anthropicApiKey`, `mistralApiKey`, or `openaiApiKey`; optionally select `aiProvider` (`anthropic`, `mistral`, or `openai`) and `aiModel`. Store provider keys as Apify secrets and never echo them.

Successful analysis adds `aiSentiment`, `aiTopics`, and `aiSummary`. Without a key, the Actor continues without AI and emits a `note` row. Provider failures do not discard source records; AI fields become null. Provider usage is billed separately.

## Diagnose failures and partial results

- Input-contract errors (missing query/source or unknown mode) are fatal validation errors.
- Bad or unavailable individual handles, feeds, or threads are logged and skipped; one target need not fail the whole run.
- Unexpected scrape errors can produce a `diagnostic` dataset row while the run remains `SUCCEEDED`. Therefore check run status, logs, diagnostics, and expected record counts together.
- A `note` can indicate missing AI credentials, a wall-clock stop, or a charge-budget stop.
- Empty output can be legitimate (no matches, filters, delta mode) or partial failure. Re-run a tiny known-public request such as profile `bsky.app`, inspect logs, and disable delta/filters before escalating.
- Date/search behavior comes from the public search API. `latest` supports time-window pagination; `top` is limited and not chronologically pageable.

Report the final input with secrets redacted, run ID/status, dataset ID/count, any `note`/`diagnostic` rows, and whether configured item and charge caps were reached.
