---
name: jobs-ac-uk-scraper
description: Evaluate, configure, run, and integrate the Apify Actor nomad-agent/jobs-ac-uk-scraper for UK academic, research, postdoc, fellowship, PhD studentship, and university professional-services vacancies. Use when choosing whether this Actor fits a job-data workflow, preparing safely capped inputs, calling it through Apify API/SDK, interpreting its dataset, or diagnosing partial/empty runs, descriptions, caching, and delta mode.
---

# jobs.ac.uk Scraper

Use Actor `nomad-agent/jobs-ac-uk-scraper` for public jobs.ac.uk vacancies. It needs an Apify token, but no jobs.ac.uk login, cookies, or proxy configuration.

## Decide fit

Use it for UK academic/research hiring, including lectureships, postdocs, fellowships, studentships, and university professional services. Choose another source for general-market jobs, radius search, applicant profiles, or application submission. `location` accepts jobs.ac.uk region/city facet slugs; it is not a free-text distance search.

## Configure a bounded run

Start small and widen only after inspecting output:

```json
{
  "keywords": ["bioinformatics"],
  "disciplines": ["biological-sciences"],
  "location": ["scotland"],
  "maxItems": 25,
  "pageSize": 25,
  "postedSince": 14,
  "fetchDescriptions": false,
  "onlyNewSinceLastRun": false,
  "cacheTtlSeconds": 1800
}
```

- Keep `maxItems` positive and modest for exploratory runs. `0` means unlimited.
- Keep `pageSize` at or below 25; the Actor clamps it to the portal maximum.
- Turn on `fetchDescriptions` only when full advert text is needed: it adds one detail fetch and a billed detail event per enriched job.
- Use `onlyNewSinceLastRun: true` for recurring alerts, not a first-run snapshot. It uses persistent seen-state and returns `isNew: true` only for delivered delta records.
- Set `cacheTtlSeconds: 0` only when a live refetch is required.
- Use documented facet values. Invalid discipline and closed-enum filters are ignored with warnings; an unrecognized location facet can yield no results.
- Treat `employer` and `postedSince` as local post-filters. Records with unreadable `postedAt` are retained.

Other filters are `studentshipType` (`phds`, `masters`), `contractType` (`permanent`, `fixed-term-contract`, `temporary`), `hours` (`full-time`, `part-time`), `workplace` (`remote`, `hybrid`, `on-site`), and `employerSector`. Leave `keywords` empty only when the broad built-in ten-query defaults are intended.

## Run and retrieve

Prefer environment variables; never place a real token in source, logs, URLs, or chat.

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/jobs-ac-uk-scraper").call(run_input={
    "keywords": ["machine learning"],
    "maxItems": 25,
    "fetchDescriptions": False,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

For REST, POST JSON to `/v2/acts/nomad-agent~jobs-ac-uk-scraper/runs`; poll the returned run to a terminal status, then GET `/v2/datasets/{defaultDatasetId}/items`. Use an `Authorization: Bearer` header instead of a token query parameter. Use the synchronous dataset-items endpoint only for deliberately small runs that fit the caller timeout.

## Consume output

Expect flat records with `advertId`, `jobId`, `id`, `title`, `company`, `department`, `location`, `isRemote`, `workplaceType`, salary text plus parsed bounds/currency/period, `postedAt`, `deadline`, optional `description`, optional `isNew`, and `url`.

- Deduplicate downstream by `advertId` when present, then `jobId`, then `url`.
- Treat nullable fields as unknown, not false. `workplaceType: null` does not mean on-site.
- Accept `deadline` as ISO date, raw source text, or null; parse defensively.
- Do not assume `description` exists or succeeded merely because it was requested.
- Separate diagnostic rows containing `warning` and `source` from job rows before mapping or analytics.

## Diagnose failures

- Empty dataset: reduce filters, verify the location facet, disable delta mode, and inspect run logs. A successful delta run may legitimately return zero new jobs.
- Partial results or missing descriptions: inspect retry/deadline warnings; search and detail fetch failures are fail-soft where possible.
- Repeated older data: lower `cacheTtlSeconds`; use `0` only when freshness outweighs source load.
- Budget-limited run: the Actor may cap delivery at the remaining maximum charge. Raise the Apify run limit deliberately rather than using unlimited input.
- Upstream markup or availability changes: preserve the run ID, sanitized input, and warning text for support; never include tokens or private storage values.

Respect jobs.ac.uk terms, robots expectations, and applicable law. Store only fields needed for the stated purpose and apply retention/access controls when redistributing vacancy data.
