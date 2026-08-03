---
name: nofluffjobs-scraper
description: Evaluate, configure, run, and integrate the Apify Actor nomad-agent/nofluffjobs-scraper for public NoFluffJobs postings across Poland and CEE. Use when an agent must decide whether the Actor fits a technology or business job-search, recruiting, salary-analysis, alerting, or market-data workflow; select categories and filters; call it through the Apify API or SDK; consume its normalized dataset; or explain detail enrichment, delta/repost mode, notifications, costs, partial results, and limitations.
---

# NoFluffJobs Scraper

Use `nomad-agent/nofluffjobs-scraper` for public NoFluffJobs vacancies across its unified Poland/CEE/EU index. Prefer it when category, salary, seniority, skills, remote status, or recurring-alert data matters. Do not present it as a keyword-search Actor, general web crawler, applicant/contact-data source, or historical archive. The deprecated `region` input is ignored and accepts only `pl`.

## Configure a bounded run

Start with a small probe and raise limits only when needed:

```json
{
  "categories": ["backend", "data"],
  "salaryCurrency": "EUR",
  "postedSince": 7,
  "remoteOnly": true,
  "maxItems": 50,
  "maxPages": 2,
  "cacheTtlSeconds": 1800
}
```

- Select `categories` from the Actor schema. Omitting or emptying it uses all 37 default categories; `ai-data` is an accepted legacy alias but is not in the default set.
- Use `salaryCurrency` only from `EUR|PLN|USD|GBP|CHF|CZK|HUF|UAH`. Salary values are requested as monthly amounts.
- Use `postedSince`, `titleExclude`, `companyExclude`, `remoteOnly`, and `withSalaryOnly` for client-side filtering. Missing or unparseable dates survive `postedSince`.
- Keep `maxItems` positive and modest for interactive work. `0` removes the item cap, so use it only on explicit request with a deliberate `maxPages` limit. `maxPages` is per category, defaults to 5, and is schema-capped at 20; each page contains up to 50 postings.
- Expect automatic best-effort detail enrichment for retained postings. There is no input to disable it. A successful detail lookup replaces the compact `snippet` fallback with an assembled full job body; a failed or mismatched detail lookup keeps the fallback and does not drop the posting.
- Enable `onlyNewSinceLastRun` for new and renewed/reposted jobs only. Combine it with `skipReposts` for strictly new jobs. Use a stable, non-secret `stateName` only when runs should intentionally share or isolate history; merely setting it activates tracking. The legacy `stateKey` name is still accepted.
- Set `webhookUrl` to receive `{source, count, truncated, items}` with at most 200 items, or supply both `telegramBotToken` and `telegramChatId` for a short Telegram summary. Keep credentials in a secret manager; notification failures are fail-open.
- Set `cacheTtlSeconds: 0` only when live freshness outweighs upstream load; the default is 1800 seconds.

## Run and retrieve results

Require `APIFY_TOKEN` from the environment or a secret manager. Never print it, embed it in source, or place it in a retained URL.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/nofluffjobs-scraper").call(
    run_input={"categories": ["backend"], "maxItems": 50, "maxPages": 2}
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

Synchronous HTTP for a small bounded run:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"categories":["backend"],"maxItems":50,"maxPages":2}' \
  'https://api.apify.com/v2/acts/nomad-agent~nofluffjobs-scraper/run-sync-get-dataset-items'
```

For long or scheduled work, start the Actor asynchronously, poll the run to a terminal state, then page through `defaultDatasetId` instead of assuming every item fits in one response.

## Interpret output

Expect one flat row per retained posting. Identity and navigation fields are `id`, `externalId` (the same source slug), `source` (`nofluffjobs`), and `url`. Core fields are `title`, `company`, `location`, `remote`, `category`, `seniority`, `skills`, `postedAt`, `snippet`, `salary`, `salaryMin`, `salaryMax`, `salaryCurrency`, `salaryPeriod`, and `isRepost`.

- Treat missing salary bounds as undisclosed, not zero. `salaryPeriod` is `month` when numeric salary data exists.
- Treat `snippet` as best-effort job text: normally an assembled detail body, otherwise a compact seniority/skills/salary fallback. Do not assume a fixed length.
- Expect cross-category deduplication by normalized company plus title, which may collapse location variants. Use `source` plus `id` when deduplicating these results against other systems.
- Read `isRepost` only when delta/repost state is active; otherwise it remains `false`.
- Detect diagnostic rows by `warning` before treating rows as jobs. A successful run may contain one diagnostic row when no jobs match, delta mode finds nothing new, or an unexpected error is converted to a fail-soft result.

## Handle costs, failures, and limits

- Expect pay-per-event charging for Actor start and delivered results. Verify current Store pricing before estimating cost; do not hardcode a quote into an integration.
- Preserve partial datasets when a timeout safety margin or run charge ceiling stops collection. Record run status, dataset ID, input, item count, and warnings for observability.
- Retry transient platform or upstream failures with bounded backoff. Do not retry deterministic schema errors indefinitely.
- Expect cache lag, missing dates or salary bounds, source schema changes, upstream blocking, and incomplete detail enrichment. The Actor reads public postings only; it does not apply to jobs, return recruiter contacts, expose private profiles, or guarantee exhaustive history.
- Respect NoFluffJobs terms, Apify policies, privacy law, and downstream retention requirements. Do not use scraped data for unsupported sensitive inference or fully automated employment decisions.
