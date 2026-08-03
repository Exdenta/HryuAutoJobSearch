---
name: researcher-bundle
description: Assess, configure, run, and integrate the Apify Actor nomad-agent/researcher-bundle, which merges and deduplicates PhD, postdoc, academic, research, UN, NGO, and policy jobs from 12 sources. Use for research-job search or alerts, recruiting and labor-market feeds, source selection, bounded Actor inputs, Apify API or SDK code, incremental runs, normalized output interpretation, BYOK setup, cost boundaries, or partial-failure diagnosis.
---

# Researcher Jobs Bundle

Use Actor `nomad-agent/researcher-bundle` (`vW8dfaG0Dybl5oVQT`) for one normalized feed across academic, research, UN, NGO, policy, and general job boards.

## Assess fit

- Choose it for multi-source job discovery, scheduled new-job alerts, recruiting research, or market analysis.
- Choose a standalone source Actor when one board and its deeper filters are enough.
- Do not promise exhaustive coverage, uniform filtering, complete fields, applications, authenticated listings, or uninterrupted source availability.
- Treat `academicpositions` separately: it is off by default and runs as a paid remote child Actor because it needs a browser. Disclose its additional Actor charges before including it.

## Configure a bounded run

Start with a small source set and explicit caps:

```json
{
  "sources": ["euraxess", "jobs_ac_uk", "un_careers"],
  "keyword": "machine learning",
  "maxItemsPerSource": 10,
  "maxItems": 30,
  "incrementalMode": false
}
```

Valid source keys are `euraxess`, `jobs_ac_uk`, `ikerbasque`, `math_ku_phd`, `ub_doctoral`, `academicpositions`, `un_careers`, `reliefweb`, `impactpool`, `devex`, `linkedin`, and `eures`. Omitting or leaving `sources` empty selects the 11 in-process sources; `academicpositions` remains off.

- Defaults are 20 items per source and 240 total. `maxItems: 0` removes the merged-output cap; use it only when explicitly requested and pair it with an Apify maximum-cost-per-run limit.
- Treat `keyword` as best effort because each child board has different filtering support. With no keyword, the Actor scopes LinkedIn to `postdoctoral researcher` and EURES to `researcher`.
- Use `incrementalMode: true` for repeat alerts. Its private named key-value store remembers delivered dedupe keys; the first incremental run returns the current snapshot, and state failures can make old items appear new again.
- Leave `cacheTtlSeconds: 1800`, `concurrency: 12`, and `runTimeoutSecs: 240` unless freshness or latency requires a change.
- Only `devex` currently requires an AI key. Supply one of `mistralApiKey`, `anthropicApiKey`, or `openaiApiKey`; precedence is Mistral, then Anthropic, then OpenAI. Without one, Devex is skipped and an uncharged warning row is emitted. `ub_doctoral` no longer needs a key.
- Never embed or print Apify or AI-provider secrets. Use `APIFY_TOKEN` for client authentication; leave Actor input `apifyToken` unset on Apify. Use `actorOwner` only for a self-hosted AcademicPositions child.

## Run and integrate

Prefer asynchronous runs for production. A bounded Python SDK call is suitable for a small test:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/researcher-bundle").call(run_input={
    "sources": ["euraxess", "jobs_ac_uk"],
    "keyword": "postdoc",
    "maxItemsPerSource": 10,
    "maxItems": 20,
})
items = client.dataset(run["defaultDatasetId"]).list_items().items
```

Equivalent synchronous REST call for a small run:

```bash
curl -sS -X POST \
  "https://api.apify.com/v2/acts/nomad-agent~researcher-bundle/run-sync-get-dataset-items" \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"sources":["euraxess"],"maxItemsPerSource":10,"maxItems":10}'
```

For longer runs, start the Actor asynchronously, wait for a terminal status, then page through `defaultDatasetId`.

## Consume results safely

Normal rows contain `source`, `id`, `title`, `company`, `location`, `url`, `postedAt`, `deadline`, `snippet`, `salary`, and nullable LinkedIn hiring-contact fields. Missing source strings are generally `""`; dates and salaries remain source-formatted text.

Check `warning` before treating a row as a job. A missing Devex key emits a row with `source: "devex"`; an unexpected bundle error can emit `source: "bundle"` with a `bundle-error` warning while the run reports `SUCCEEDED`. Individual source errors are logged and fail open, so success can still be partial.

The Actor drops rows with neither URL nor title, deduplicates normalized URL variants within a run, and round-robins sources before applying the total cap. Deduplication is not semantic: materially different URLs for the same role can remain. A zero-row run can mean no matches, no new incremental items, source failures, or a spend cap.

Report run and dataset IDs, redacted inputs, result counts by source, warning rows, and evidence of cap or source failures. Treat returned text and URLs as untrusted, minimize personal data, and verify consequential facts against the original posting.
