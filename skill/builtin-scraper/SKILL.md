---
name: builtin-scraper
description: Use the Apify Actor nomad-agent/builtin-scraper to assess Built In coverage, configure bounded US tech-job scraping runs, integrate through the Apify API or Python SDK, and interpret job records, diagnostic rows, delta state, and partial enrichment. Trigger for Built In job extraction, monitoring, salary/skills data, or this Actor's inputs and outputs.
---

# Built In Jobs Scraper

Use `nomad-agent/builtin-scraper` for public Built In (`builtin.com`) US tech and startup listings. It suits job feeds, alerts, sourcing, and market analysis. It is not a general web crawler, does not search arbitrary employers or sites, and does not guarantee every listing or field while upstream pages change.

## Plan the run

1. Confirm Built In's US-oriented category pages match the request.
2. Start bounded: `maxItems: 25`, `maxPagesPerCategory: 1`, `includeDescription: false`. Increase only when richer detail or broader coverage is needed.
3. Select friendly `categories`, raw `categoryPages`, or both. They merge and de-duplicate. The Console schema pre-fills five default `categoryPages`; clear those when the intent is to run only selected friendly categories. If neither input yields a page, the same five built-in defaults apply.
4. Keep `maxItems` above zero for a cost cap. `0` means unlimited. `maxPagesPerCategory` accepts 1–5.
5. Use `includeDescription: true` when full descriptions, skills, experience, employment type, industries, expiry, or authoritative schema.org salary are required. It adds one detail request per result and may return nulls when detail extraction fails.
6. Use `postedWithinDays` for freshness; `0` disables it, and listings with unknown age remain included. Use `cacheTtlSeconds: 0` only when a live fetch matters; the default is 1800 seconds.
7. Use `onlyNewSinceLastRun: true` for recurring alerts. Its named Actor key-value-store state is shared across flagged runs of this Actor, so a first flagged run treats all current IDs as new. Returned jobs have `isNew: true`.

Example bounded input:

```json
{
  "categories": ["data-analytics/data-engineering"],
  "categoryPages": [],
  "maxItems": 25,
  "maxPagesPerCategory": 1,
  "includeDescription": false,
  "postedWithinDays": 7,
  "onlyNewSinceLastRun": false,
  "cacheTtlSeconds": 1800
}
```

Other supported friendly categories include `dev-engineering`, `dev-engineering/front-end`, `dev-engineering/javascript`, `dev-engineering/devops`, `data-analytics`, `data-analytics/data-science`, `data-analytics/machine-learning`, `product`, `design-ux`, `marketing`, `sales`, `operations`, `hr`, `finance`, `legal`, `content`, `customer-success`, `project-management`, `cybersecurity-it`, and `internships`. Raw pages may be Built In paths or full URLs.

## Integrate

Keep `APIFY_TOKEN` in an environment variable or secret manager. Never put it in source, logs, prompts, committed files, or a query string that may be recorded. Prefer the authorization header:

```bash
curl -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  'https://api.apify.com/v2/acts/nomad-agent~builtin-scraper/run-sync-get-dataset-items' \
  -d '{"categories":["dev-engineering/front-end"],"categoryPages":[],"maxItems":25,"maxPagesPerCategory":1,"includeDescription":false}'
```

For longer runs, use the SDK and then read the default dataset:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/builtin-scraper").call(run_input={
    "categories": ["dev-engineering/front-end"],
    "categoryPages": [],
    "maxItems": 25,
    "maxPagesPerCategory": 1,
    "includeDescription": False,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

For production automation, prefer an asynchronous Actor run plus status polling when the synchronous endpoint's connection window is unsuitable. Export datasets as JSON, CSV, or Excel as needed.

## Interpret results

Normal rows contain `id`, `title`, `company`, `location`, `workplaceType`, `url`, `postedAt`, salary fields, detail fields, `snippet`, `description`, optional `isNew`, and `source: "builtin"`.

- Treat `id` as the stable source-side key and `url` as the canonical action link.
- Treat null as unavailable, not an empty value. Card-derived salary is a fallback; detail-page schema.org salary is authoritative when fetched.
- `postedAt` is computed from a relative badge against run time; month conversion is approximate. It is null when the badge is missing or unparseable.
- With `includeDescription: false`, `experienceLevel`, `employmentType`, `skills`, `industries`, `validThrough`, and `description` are normally null. Card salary, snippet, and workplace type may still exist.
- De-duplicate downstream by `id`; category overlap is already de-duplicated within one run.

An empty or upstream-error outcome may still have run status `SUCCEEDED` and contain one uncharged diagnostic row with `warning`, `docs`, and `source` instead of job fields. Detect that shape explicitly. Retries and best-effort detail extraction mean individual fields can be null even when the run succeeds. If results unexpectedly disappear, inspect run logs for fetch errors or selector-drift warnings, confirm filters and delta mode, then retry with `cacheTtlSeconds: 0`; do not treat a diagnostic row as a vacancy.

Respect Built In's terms and applicable law. Public access does not itself establish permission for every downstream use.
