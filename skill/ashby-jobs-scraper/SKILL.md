---
name: ashby-jobs-scraper
description: Assess, configure, run, and integrate the Apify Actor nomad-agent/ashby-jobs-scraper, which discovers or directly queries public Ashby job boards and returns normalized job records. Use for Ashby hiring research, scheduled job monitoring, capped Apify runs, API or SDK integration, output and warning-row interpretation, delta mode, salary/date/title/location filters, output profiles, or optional BYOK AI enrichment.
---

# Ashby Jobs Scraper

Use Actor `nomad-agent/ashby-jobs-scraper` for live postings from public `jobs.ashbyhq.com` boards. Prefer it when the user has Ashby slugs or company websites and needs normalized, exportable records without login or proxies. It is not a general web, LinkedIn, candidate, historical-jobs, or cross-ATS scraper.

## Configure the smallest safe run

1. Supply at least one source:
   - `companies`: Ashby slugs or full Ashby board URLs.
   - `companyDomains`: company websites; the Actor probes pages for a confirmed Ashby board. Failed discovery yields a warning row, not a failed run.
2. Keep both caps nonzero for exploratory or paid runs. Start with `maxItemsPerCompany: 10` and `maxItems: 20`, then raise only when required. `0` means unlimited.
3. Narrow before raising caps with `keyword`, `titleExclude`, `locationFilter`, `companySearch`, `descriptionSearch`, `descriptionExclude`, `postedSince`, `postedAfter`, `postedBefore`, `remoteOnly`, `minSalary`, or `maxSalary`.
4. Use `outputProfile: "compact"` to omit descriptions or `"mini"` to also omit structured salary fields. `full` is the default. Filtering on descriptions still works when description output is omitted.
5. Leave `concurrency` at `8` unless there is a measured need; its accepted range is 1–16.

Minimal discovery input:

```json
{
  "companyDomains": ["linear.app"],
  "keyword": "engineer",
  "maxItemsPerCompany": 10,
  "maxItems": 10,
  "outputProfile": "compact"
}
```

Date filters drop undated jobs. Salary filters drop jobs without numeric salary and do no currency conversion. `remoteOnly` trusts only Ashby's `isRemote: true` value.

## Run and integrate

Keep `APIFY_TOKEN` in a secret manager or environment variable. Never paste it into source, prompts, logs, dataset rows, or a URL query string.

Python SDK:

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/ashby-jobs-scraper").call(run_input={
    "companies": ["openai"],
    "maxItemsPerCompany": 10,
    "maxItems": 10,
    "outputProfile": "compact",
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
jobs = [item for item in items if item.get("id")]
diagnostics = [item for item in items if not item.get("id")]
```

REST synchronous run:

```bash
curl --fail-with-body \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"companies":["openai"],"maxItemsPerCompany":10,"maxItems":10}' \
  'https://api.apify.com/v2/acts/nomad-agent~ashby-jobs-scraper/run-sync-get-dataset-items'
```

For larger runs, start the Actor asynchronously, wait for a terminal run status, then page through `defaultDatasetId`; do not depend on a synchronous request staying open. Treat `SUCCEEDED` plus diagnostic rows as a partial/data-quality outcome, not proof that every requested company resolved.

## Interpret the dataset

Normal rows include `ats: "ashby"`, `company`, `id`, `globalId`, `title`, `department`, `location`, `url`, `postedAt`, `employmentType`, tri-state `remote`, salary fields, and `warnings`. `globalId` has the stable form `ashby:<slug>:<id>` and is the preferred join/dedup key. Missing source values remain empty or null; the Actor does not infer dates, remote status, salary, or currency conversions.

Separate records structurally:

- Job row: nonempty `id` and `title`; `warnings` may still flag missing job fields.
- Diagnostic row: null job identity and a nonempty `warnings` array. It can report unresolved/not-found companies, per-company or total truncation, skipped AI enrichment, or an unexpected run-level error.

Do not count diagnostic rows as jobs. Inspect every warning and surface truncation prominently; raise the relevant cap only with user approval about cost/volume.

## Delta monitoring

Set `onlyNewSinceLastRun: true` for scheduled monitoring. The first flagged run emits all current jobs with `isNew: true`; later flagged runs omit previously seen `globalId` values. Unflagged runs neither read nor update that state. State is Actor-wide in a named key-value store and is not an arbitrary historical query or deletion detector; avoid sharing one Actor identity when tenants require isolated seen-state semantics.

## Optional BYOK enrichment

Set `aiEnrichment: true`, choose `aiProvider` (`anthropic`, `mistral`, or `openai`), and provide the matching secret input or platform environment variable. Supported model inputs are `aiModel`, `mistralModel`, and `openaiModel`. Provider usage is billed separately. Never persist provider keys in code or output.

If the matching key is absent, the run continues without `ai*` fields and emits a diagnostic warning row. Enriched fields are `aiKeySkills`, `aiExperienceLevel`, `aiWorkArrangement`, and `aiVisaSponsorship`; treat them as model extraction, retain null/unknown values, and do not represent them as verified facts.

## Explain limitations

- Data is current public Ashby board data, not private, applicant, candidate, or historical data.
- Website discovery is best-effort HTTP probing and can miss blocked, unreachable, or indirectly linked boards.
- Upstream availability and schema changes can produce partial results or warning rows.
- A board slug may return zero live jobs legitimately.
- Description snippets are capped and plain text; `compact`/`mini` omit them.
- Compensation exists only when Ashby exposes it; comparisons use raw stated units.
- Pricing changes: check the Actor's current Apify Pricing tab before estimating spend.
