---
name: reliefweb-scraper
description: Evaluate, configure, run, or integrate the Apify Actor nomad-agent/reliefweb-scraper for current public ReliefWeb humanitarian, NGO, aid-worker, and UN-sector vacancies. Use for choosing bounded search, country, freshness, monitoring, proxy, cache, or organisation-enrichment inputs; calling the Actor through Apify; and consuming job, notice, warning, or partial-result rows safely.
---

# ReliefWeb Scraper

Use Actor `nomad-agent/reliefweb-scraper`. It reads ReliefWeb's public RSS feed without a ReliefWeb API key or approved `appname`; it does not apply to jobs, rank candidates, or provide a complete historical archive.

## Configure a run

Start with a small explicit cap:

```json
{
  "search": "monitoring evaluation",
  "country": "Kenya",
  "postedSince": 14,
  "maxItems": 20,
  "enrichOrganisation": false,
  "monitoringMode": false,
  "cacheTtlSeconds": 1800
}
```

- Use `search` for server-side free-text filtering. Use `country` for a case-insensitive substring match against ReliefWeb's published Country tag and `postedSince` for client-side freshness filtering.
- Set `maxItems` to the maximum delivered jobs. `0` means no local limit, but the feed itself supplies only roughly the 20 newest postings and does not paginate.
- Keep `proxyConfiguration` at its default Apify Residential group. ReliefWeb usually serves a stub to datacenter IPs; Residential access and bandwidth depend on the caller's Apify plan. The legacy `useResidentialProxy` field matters only when `proxyConfiguration` is absent.
- Keep `cacheTtlSeconds: 1800` for repeat runs. Use `0` only when a live fetch is required.
- Enable `enrichOrganisation` only when organisation website and description are needed. It follows ReliefWeb organisation profiles, is best-effort, and is a billed add-on.
- Enable `monitoringMode` for scheduled new-posting alerts. Seen IDs persist in the Actor's named key-value store and are shared by consumers of that Actor. Use `resetMonitoringState: true` for one intentional reset run, then turn it off.

Each successful feed fetch is charged an Actor-start event; each delivered job is charged as a result. Enriched organisations can add their own event charge, and Residential proxy bandwidth is billed separately. Set an Apify maximum total charge when a hard spend ceiling is required; it can reduce the delivered count.

## Run and retrieve data

Require `APIFY_TOKEN` through environment or secret storage. Never paste it into source, logs, prompts, or URLs.

REST, returning dataset items synchronously:

```bash
curl --fail-with-body -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"search":"programme officer","maxItems":20}' \
  'https://api.apify.com/v2/acts/nomad-agent~reliefweb-scraper/run-sync-get-dataset-items'
```

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/reliefweb-scraper").call(
    run_input={"search": "programme officer", "maxItems": 20}
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

Prefer an asynchronous Actor run plus status polling when the client cannot tolerate the synchronous endpoint's wait limit.

## Consume output safely

Normal rows contain nullable `id`, `title`, `company`, `country`, `location`, `latitude`, `longitude`, `url`, `postedAt`, `deadline`, `salary`, `orgWebsite`, `orgDescription`, and `snippet`, plus `source: "reliefweb"`. Treat `postedAt` as the feed's RFC 822 date string and `deadline` as ISO `YYYY-MM-DD`. Deduplicate by `id`, falling back to `url`.

Branch before treating every row as a job:

- A row with `warning` and `docs` reports an unbilled fetch or unexpected-run diagnostic. Surface it operationally and inspect the run status/logs.
- A row with `notice` means the feed was fetched successfully but no postings matched the inputs.
- Charge limits, the run deadline, filters, monitoring state, or enrichment failures can yield partial, empty, or nullable results. Never infer missing values.

## Respect limitations

- Use this Actor for current ReliefWeb-focused discovery, exports, or alerts. Use a multi-source Actor for broader coverage.
- Search is feed-backed and not exhaustive: ReliefWeb exposes roughly 20 current items and no further RSS pagination.
- Country and coordinates come from ReliefWeb's Country tag and an approximate country centroid; they are not precise job-site geocoding. Salary normally remains `null` because the feed has no structured salary field.
- Organisation enrichment fails open and can leave both organisation fields `null`. Monitoring state is best-effort shared Actor state, not an audit log.
- Validate critical vacancies at their canonical `url`. Respect ReliefWeb's terms, access policies, personal-data obligations, and applicable law.
