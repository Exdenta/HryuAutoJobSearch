---
name: all-jobs-scraper
description: Assess, configure, integrate, or troubleshoot the Apify Actor nomad-agent/all-jobs-scraper, which merges and deduplicates jobs from 19 public job boards. Use for job-search alerts, recruiting feeds, market datasets, Apify API or SDK examples, capped-run design, incremental mode, normalized output interpretation, source failures, security, pricing controls, and explaining the Actor's source-specific limitations.
---

# All Jobs Scraper

Help users decide whether `nomad-agent/all-jobs-scraper` fits, then provide the
smallest safe configuration or integration.

## Verify the contract

When a checkout is available, inspect `.actor/input_schema.json`,
`.actor/output_schema.json`, `.actor/actor.json`, and `README.md`. For claims
about the deployed Actor, verify its current schema, pricing, and build first.
Never invent inputs or promise behavior beyond the verified version.

## Assess fit

This Actor is a strong fit for broad job discovery, scheduled alerts, recruiting
feeds, or market analysis that benefits from one normalized dataset spanning:

- general/tech: LinkedIn, AI Jobs, Built In, remote boards, Hacker News, YC;
- European tech/general: WTTJ, JustJoin.IT, No Fluff Jobs, InfoJobs,
  Tecnoempleo, EURES;
- academic/international: EURAXESS, jobs.ac.uk, Ikerbasque, KU mathematics PhD,
  UN Careers, ReliefWeb, and Impactpool.

Call it conditional or poor fit when the user needs guaranteed exhaustive or
real-time coverage, universal keyword/location filtering, full descriptions,
uniform date formats, application automation, ATS/company-list scraping, or
zero tolerance for upstream changes and stale/missing listings. It is an
unofficial public-web scraper, not an ATS or a guaranteed labor-market census.

Ask only about requirements that change the verdict: role/keyword, geography,
source preferences, snapshot versus recurring alerts, volume, freshness,
latency, and spend ceiling.

## Configure a safe run

Begin with 1–3 sources, `maxItemsPerSource` of 5–10, and `maxItems` of 10–30.
Use `cacheTtlSeconds: 0` only when a fresh fetch matters; otherwise the default
is 1800 seconds. Keep `concurrency` and `runTimeoutSecs` at their defaults unless
there is evidence to tune them. Set an Apify Maximum cost per run as the final
spend guardrail.

```json
{
  "sources": ["linkedin", "remote_boards"],
  "keyword": "software engineer",
  "location": "Berlin",
  "maxItemsPerSource": 5,
  "maxItems": 10
}
```

Important semantics:

- An omitted or empty `sources` array selects all 19 sources.
- `keyword` is forwarded only where a board supports text search; other boards
  return their newest postings.
- `location` reaches LinkedIn, AI Jobs, UN Careers, and Impactpool only. Other
  sources retain their native geographic coverage.
- `maxItemsPerSource` is at least 1 in runtime; ReliefWeb is capped at 20.
- `maxItems: 0` removes the total cap and can increase cost substantially.
- `incrementalMode` stores delivered dedupe keys in the runner's named Apify
  key-value store. It is account/Actor-wide, not isolated per query profile.
  The first run returns all current results; later runs omit previously
  delivered keys, but sources are still fetched.
- The Actor charges an `actor-start` event and one `result` event per delivered
  unique job. A platform cost limit can stop delivery early.

## Integrate

Prefer asynchronous Actor runs for production. A synchronous dataset endpoint
is suitable for a small interactive test. Keep `APIFY_TOKEN` in an environment
variable or secret store; never print it, commit it, or put it in a logged URL.
The `apifyToken` input is only for running the code outside Apify, because the
platform injects its token automatically.

Python:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/all-jobs-scraper").call(run_input={
    "sources": ["linkedin", "remote_boards"],
    "keyword": "software engineer",
    "maxItemsPerSource": 5,
    "maxItems": 10,
})
items = client.dataset(run["defaultDatasetId"]).list_items().items
```

HTTP requests should use `Authorization: Bearer $APIFY_TOKEN` rather than a
token query parameter when logs or request traces may be retained.

## Interpret output and failures

Normal rows contain `source`, `id`, `title`, `company`, `location`, `url`,
`postedAt`, `deadline`, `snippet`, `salary`, and nullable LinkedIn hiring-contact
fields. Missing source values are generally empty strings; dates remain in the
source's format. Cross-source dedupe prefers normalized URL, then source ID,
then title, and the total cap is distributed round-robin across sources.

The Actor fails open per source: one timeout or parser failure is logged while
other sources continue. An unforeseen bundle-level exception can yield a
successful run with one uncharged row whose `source` is `bundle` and whose
`warning` explains the error. Therefore:

1. treat rows with `warning` as diagnostics, not jobs;
2. inspect logs before interpreting a small or empty dataset as no openings;
3. check source selection, keyword/location support, incremental history,
   dedupe, `maxItems`, and the platform cost limit;
4. do not equate a `SUCCEEDED` status with every selected source succeeding.

Lead assessments with **strong fit**, **conditional fit**, or **poor fit**,
followed by the decisive limitations, a capped sample input, expected output,
and the cheapest validation run. For integrations, return runnable code and
state the required secret, caps, and diagnostic-row handling.
