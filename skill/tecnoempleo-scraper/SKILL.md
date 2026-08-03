---
name: tecnoempleo-scraper
description: Configure, run, and integrate the Apify Actor nomad-agent/tecnoempleo-scraper for public Spanish IT and technology vacancies. Use when an agent must decide whether the Actor fits a Spain-focused job-search, recruiting, alerting, or labour-market workflow; choose bounded filters; call it through the Apify API or SDK; consume job or diagnostic dataset rows; or explain detail enrichment, salary fields, delta mode, partial results, costs, and source limitations.
---

# Tecnoempleo Scraper

Use Actor `nomad-agent/tecnoempleo-scraper` for public IT and technology vacancies from Tecnoempleo.com. Prefer it for Spain-focused searches and Tecnoempleo-specific monitoring. Choose a broader source for multi-country coverage, applications, applicant profiles, recruiter contacts, private data, or guaranteed historical archives.

## Configure a bounded run

Start with a small probe and raise the cap only after inspecting results:

```json
{
  "keyword": "python",
  "province": "Madrid",
  "remote": "any",
  "postedSince": 7,
  "includeDescription": false,
  "maxItems": 25
}
```

- Set only documented inputs: `keyword`, `province`, `remote`, `category`, `includeDescription`, `titleExclude`, `companyExclude`, `postedSince`, `onlyNewSinceLastRun`, `maxItems`, and `cacheTtlSeconds`.
- Use `remote` values `any`, `remote`, `hybrid`, or `on-site`. Province and category names are matched accent- and case-insensitively against live Tecnoempleo filters; an unrecognized value is ignored rather than rejected.
- Treat `includeDescription` as `true` by default. Set it to `false` for a faster probe; when enabled, the Actor fetches detail pages for full descriptions, ISO posting dates, and disclosed structured salary. Detail enrichment is capped at 300 returned postings and fails open per posting.
- Use non-negative `postedSince`; `0` disables the date filter. Records with an unparseable date remain eligible.
- Keep `maxItems` positive and modest. Its default is `80`; `0` removes the user cap, but source availability, the 400-page safety ceiling, run timeout, and charge budget still bound results.
- Enable `onlyNewSinceLastRun` for recurring alerts. It uses Actor-owned persistent state, so later runs may legitimately return no jobs.
- Keep `cacheTtlSeconds` at its `1800` default unless the user explicitly needs live fetching and accepts more source traffic.

## Call and retrieve results

Read `APIFY_TOKEN` from an environment variable or secret manager. Never print it, commit it, embed it in source, or place it in a retained URL.

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/tecnoempleo-scraper").call(run_input={
    "keyword": "data engineer",
    "remote": "hybrid",
    "includeDescription": False,
    "maxItems": 25,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
jobs = [item for item in items if item.get("id") and not item.get("warning")]
```

For direct HTTP, send the token in an authorization header:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"province":"Barcelona","maxItems":25,"includeDescription":false}' \
  'https://api.apify.com/v2/acts/nomad-agent~tecnoempleo-scraper/run-sync-get-dataset-items'
```

Use asynchronous Actor runs for large pulls or scheduled workflows. Poll the run to a terminal state, read `defaultDatasetId`, and paginate dataset reads.

## Interpret output

A normal row contains `id`, `title`, `company`, `location`, `category`, `technologies`, `url`, `postedAt`, `snippet`, `description`, `salaryMin`, `salaryMax`, `salaryCurrency`, `salaryPeriod`, and `source` (`tecnoempleo`).

- Deduplicate by `source` plus `id`, not title. `location` may be a province or a work-mode label and defaults to `Spain` when absent.
- Expect `postedAt` as `DD/MM/YYYY` from listing cards or `YYYY-MM-DD` after successful detail enrichment.
- Treat null salary fields as undisclosed, never as zero. Salary values come from source JobPosting data and are not inferred.
- Expect `description` and salary fields to remain null when enrichment is disabled, unavailable, skipped near the deadline, or beyond the 300-posting enrichment cap.
- Detect rows containing `warning` and no job `id`. They are uncharged diagnostics, not vacancies; a succeeded run may contain only one such row when no jobs match or an unexpected error is converted to fail-soft output.

## Handle costs, failures, and limits

- Expect pay-per-event charging for an Actor start and each returned result. Verify current Store pricing before quoting a cost; a run-level charge ceiling may stop delivery early.
- Preserve and label partial data when later listing pages or detail requests fail, the deadline approaches, or the charge budget is exhausted. The Actor falls back to Tecnoempleo's smaller RSS feed only when the primary HTML listing is unavailable or appears broken.
- Cache reuse can make content up to `cacheTtlSeconds` old. Delta mode means unseen by this Actor's stored history, not newly published globally.
- Tecnoempleo markup, taxonomy, availability, and employer disclosures can change. The Actor does not guarantee exhaustive recall, complete descriptions, salary disclosure, or listing liveness.
- Surface diagnostic `warning` text, retry transient failures with bounded backoff, and never count a diagnostic row as a job.
- Respect Tecnoempleo terms, Apify policies, applicable law, privacy rules, and downstream retention requirements. Validate listing freshness before consequential use.
