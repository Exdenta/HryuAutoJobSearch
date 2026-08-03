---
name: serp-multi-engine-scraper
description: Configure, integrate, and troubleshoot the Apify Actor nomad-agent/serp-multi-engine-scraper, which returns organic and optional ad results from Bing, DuckDuckGo, Baidu, and Yahoo. Use when Codex must decide whether this Actor fits a SERP, SEO, rank-tracking, research, or URL-discovery task; prepare Actor inputs or API/SDK calls; explain its flat dataset; control result volume and cost; or diagnose proxy, engine, device, pagination, and empty-result behavior.
---

# SERP Multi-Engine Scraper

Use Actor `nomad-agent/serp-multi-engine-scraper`. It fetches public SERP HTML without search-engine API keys, login, cookies, or a browser. Require an Apify token only when calling the Actor programmatically.

## Choose inputs

Always provide `queries`, a non-empty array of strings. Preserve search operators such as `site:` and `filetype:`.

| Input | Default | Contract |
|---|---:|---|
| `queries` | required | Search strings. Whitespace is normalized and case-insensitive duplicates are removed. |
| `engines` | `["bing", "duckduckgo"]` | Any of `bing`, `duckduckgo`, `baidu`, `yahoo`. |
| `device` | `"desktop"` | `desktop` or `mobile`; only Bing changes behavior. |
| `languageCode` | `""` | BCP-47 hint. Mapped by Bing, DuckDuckGo, and Yahoo; ignored by Baidu. |
| `region` | `""` | Country hint. Used by Bing; ignored where an engine has no region parameter. |
| `maxPagesPerQuery` | `1` | Integer `1`–`10`; each engine uses native pagination and stops early on blocked or empty pages. |
| `resultsPerPage` | `0` | Integer `0`–`50`; `0` uses the engine default. DuckDuckGo does not expose this control. |
| `maxItems` | `100` | Integer `0`–`1000`; total pushed-result cap, shared round-robin across query/engine groups. `0` removes this cap. |
| `includeAds` | `false` | Include sponsored rows and tag them with `ad: true`. |
| `proxyConfiguration` | off | Optional Apify proxy object. Yahoo needs a working residential proxy from Apify datacenter runs. |
| `concurrency` | `4` | Integer `1`–`8`; higher values can increase blocking. |

Keep `maxItems` finite unless the user explicitly accepts volume and cost scaling with queries × engines × pages. Each pushed search-result row is a billed result event; the Actor also has a start event. Respect any run maximum-total-charge limit.

For Yahoo, configure Apify residential proxy. Bing, DuckDuckGo, and Baidu normally work without a proxy. If Yahoo is the only engine and no usable proxy exists, the Actor returns a diagnostic instead of searching.

## Run the Actor

Prefer the user's existing Apify integration. For Python:

```python
from apify_client import ApifyClient

client = ApifyClient("<APIFY_TOKEN>")
run = client.actor("nomad-agent/serp-multi-engine-scraper").call(run_input={
    "queries": ["best python web framework"],
    "engines": ["bing", "duckduckgo"],
    "maxItems": 100,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

For one synchronous HTTP response, POST JSON to:

```text
https://api.apify.com/v2/acts/nomad-agent~serp-multi-engine-scraper/run-sync-get-dataset-items?token=<APIFY_TOKEN>
```

Do not claim support for Google, Yandex, Ecosia, People Also Ask, or related searches. They are not selectable or returned by this Actor.

## Interpret output

Treat each normal dataset item as one flat result row:

- `query`, `engine`, `rank`, `page`, `title`, `url`, `snippet`
- `ad`, `serpUrl`, `device`, `resultsTotal`

`rank` is 1-based within one query and engine across fetched pages, after URL deduplication. `page` is 1-based. `resultsTotal` is best-effort for Bing and Baidu and normally `null` for DuckDuckGo and Yahoo. `device` echoes the normalized input even when the selected engine ignores it.

When no billable result can be returned, the Actor can still succeed with one uncharged diagnostic row: result-identifying fields are `null`, and `snippet` explains the issue. Detect these rows before treating dataset items as SERP results.

Blocked or empty engine pages are retried once, then logged and skipped without failing the run. If results are unexpectedly empty, check the diagnostic `snippet` or run status, confirm that `queries` is non-empty, add residential proxy for Yahoo, lower concurrency, or retry. Never promise a fixed result count because engines can block, return fewer rows, or omit total estimates.
