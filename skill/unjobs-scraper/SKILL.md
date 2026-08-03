---
name: unjobs-scraper
description: Evaluate, configure, run, or integrate the Apify Actor nomad-agent/unjobs-scraper for current public UN, NGO, and international-organisation vacancies from unjobs.org. Use for choosing latest, closing-soon, all-organisation, organisation, or duty-station feeds; setting proxy, keyword, detail, cache, cap, or delta inputs; consuming Apify datasets; and handling diagnostic rows, shared seen state, partial results, cost, and source limitations.
---

# UNJobs.org Scraper

Use Actor `nomad-agent/unjobs-scraper`. It returns public vacancies aggregated by unjobs.org; it does not rank candidates, apply to jobs, or scrape authenticated data.

## Configure a run

Start with a small explicit cap:

```json
{
  "feed": "latest",
  "keywordFilter": "health",
  "maxItems": 25,
  "includeDetails": true,
  "onlyNewSinceLastRun": false,
  "resetSeenJobs": false,
  "cacheTtlSeconds": 1800,
  "proxyConfiguration": {"useApifyProxy": true}
}
```

- Choose `latest` for the 25 newest postings and scheduled alerts, `closing` for the paginated closing-soon feed, or `allOrganizations` for one page from every organisation feed discovered by the Actor. `organization` overrides `dutyStation`, and either overrides `feed`; supply the slug/code from the corresponding unjobs.org URL.
- Use `keywordFilter` for a case-insensitive title-or-organisation substring filter. It is applied after fetching, and rejected rows are not delivered or billed.
- Set `maxItems` to `0` for no local item cap or to `1`–`5000` for a bound. The default is `40`; the Actor clamps out-of-range values. Feed limits, crawl safety limits, the run deadline, deduplication, and the maximum-charge budget can stop earlier.
- Enable `includeDetails` for `closingDate`, full `description`, `country`, `city`, and `office`. This fetches each vacancy page and charges a detail event only when enrichment succeeds. Structured city/country replace the listing-derived `location`.
- Enable `onlyNewSinceLastRun` for alerts. Successfully delivered IDs persist in a named Actor key-value store and are shared by runs of this Actor. Use `resetSeenJobs: true` only for an intentional restart; storage errors fail open and can redeliver jobs.
- Keep `cacheTtlSeconds: 1800` for repeat runs. Set it to `0` for live fetching.

A proxy is required because unjobs.org blocks direct Apify platform IPs. Keep the default Apify Proxy automatic/datacenter configuration, select Residential when the account supports it and datacenter exits are refused, or provide owned proxy URLs through `proxyConfiguration`. Do not disable the proxy. Residential or third-party bandwidth can add separate cost.

## Run and retrieve data

Keep the token in `APIFY_TOKEN`. Never embed it in source, prompts, logs, committed files, or URL query parameters.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/unjobs-scraper").call(
    run_input={"organization": "unicef", "maxItems": 25}
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

REST, for a bounded synchronous run:

```bash
curl --fail-with-body -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"feed":"latest","maxItems":25}' \
  'https://api.apify.com/v2/acts/nomad-agent~unjobs-scraper/run-sync-get-dataset-items'
```

Prefer an asynchronous run plus status polling when the caller cannot tolerate the synchronous endpoint's wait or response-size limits. Set the run's Apify maximum total charge when a hard spend ceiling is required.

## Consume output safely

Normal rows contain nullable `id`, `title`, `company`, `location`, `url`, `updatedAt`, and `snippet`, plus `source: "unjobs"`. Detail mode can add nullable `closingDate`, `description`, `country`, `city`, and `office`. Treat `location` as best-effort without detail mode, dates as source-derived ISO strings, and `id` as the preferred deduplication key.

Do not assume a successful run contains vacancies. A first-page, proxy, or unexpected failure deliberately succeeds with an unbilled row shaped like `{"warning": "...", "source": "unjobs"}`. Branch on `warning` before validating a row as a job. Later-page failures, blocked exits, deadlines, filters, delta state, charge limits, or empty feeds can produce partial or zero results; inspect status, logs, item count, and row shape together.

## Respect limitations

- Use this Actor for unjobs.org-focused discovery, exports, and alerts. It is not a complete historical archive or a guarantee of every UN-system vacancy; use agency portals or multi-source Actors for broader coverage.
- `keywordFilter` is textual, not semantic. Organisation and duty-station values are unjobs.org slugs/codes, not normalized taxonomies.
- Source markup, Cloudflare rules, and proxy reputation can change. Validate critical deadlines and application links at the vacancy URL.
- Pricing is pay per event: the checked-in Actor README states a `$0.005` start fee and `$0.005` per delivered vacancy, with a separate detail event and possible proxy bandwidth charges. Verify current Store pricing before making a budget commitment.
- Respect unjobs.org terms, access policies, privacy obligations, redistribution rules, and applicable law.
