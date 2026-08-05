---
name: euraxess-enrich-translate-normalize-scraper
description: Configure, run, integrate, and troubleshoot the Apify Actor nomad-agent/euraxess-enrich-translate-normalize-scraper. Use for EURAXESS PhD, postdoc, fellowship, faculty, and researcher-mobility searches that need the strict nomad-agent-job-v1 schema, deterministic requirements and location extraction, a versioned EURAXESS extension, English output or multilingual keyword translation, optional null-only BYOK LLM enrichment, rolling unseen-job delivery, or Python parsing of Actor output.
---

# EURAXESS Enrich, Translate, and Normalize Scraper

Use [nomad-agent/euraxess-enrich-translate-normalize-scraper](https://apify.com/nomad-agent/euraxess-enrich-translate-normalize-scraper) for normalized EURAXESS research vacancies with complete raw detail content and explicit source-specific provenance.

## Establish the contract

1. Verify the deployed Actor input schema, price, and build when current state matters.
2. When the source checkout is available, inspect `.actor/input_schema.json`, `.actor/actor.json`, the EURAXESS extension schema, and the Actor README before generating configuration.
3. Read [references/output-schema.md](references/output-schema.md) before mapping fields or designing storage.
4. Preserve the complete nested record. Treat `null` as unknown and `[]` as source-confirmed empty; never collapse the distinction.

## Configure a bounded run

1. Start with `maxItems: 10` to `25`. Keep ordinary runs at or below `100`; `0` removes the cap and the source can expose roughly 500 current offers.
2. Use `keyword`, `countryFilter`, and `postedSince` deliberately. `countryFilter` is a case-insensitive client-side substring filter; `postedSince` accepts `0..365`, and `0` disables age pruning.
3. Keep `deliveryMode: "unseen"` for scheduled alerts. Reuse a public opaque `dedupeKey` for the same alert/profile. Use `deliveryMode: "all"` only when repeat delivery is intentional.
4. Enable `translateToEnglish` only when English normalized display fields justify the additional per-result charge. The Actor leaves organisation names, domains, locations, and complete raw descriptions unchanged.
5. Enable `translateKeywords` to expand a search term across EU languages. Enable `aiEnrichment` to fill description-backed null fields. Both require `aiProvider` plus the matching secret input: `anthropicApiKey`, `mistralApiKey`, or `openaiApiKey`.
6. Leave proxy and cache settings at their defaults unless troubleshooting. Keep `analyticsEnabled: false` unless the caller explicitly opts in. Never expose tokens or provider keys.

Minimal exploration input:

```json
{
  "keyword": "machine learning",
  "countryFilter": "spain",
  "postedSince": 30,
  "maxItems": 25,
  "deliveryMode": "all"
}
```

## Run the Actor

Prefer asynchronous SDK calls for production and synchronous dataset-returning calls only for small interactive tests.

```python
import os

from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor(
    "nomad-agent/euraxess-enrich-translate-normalize-scraper"
).call(run_input={
    "keyword": "machine learning",
    "countryFilter": "spain",
    "postedSince": 30,
    "maxItems": 25,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

Require a terminal `SUCCEEDED` run status before consuming the dataset. Do not interpret a zero-row run as proof that no vacancies exist without checking filters, delivery state, logs, source availability, and result limits.

## Parse output

Use [scripts/parse_output.py](scripts/parse_output.py) for the importable `parse_euraxess_output()` and `parse_euraxess_outputs()` functions. Copy the module beside the integration or import it by file path; it has no third-party dependencies.

```python
from parse_output import parse_euraxess_outputs

jobs = parse_euraxess_outputs(items)
for job in jobs:
    print(job.title, job.company_name, job.academic_levels)
    complete_record = job.normalized
```

The parser validates the stable envelope, EURAXESS source identity, and extension identifier; returns common convenience fields plus EURAXESS academic-level data; and retains the complete normalized record. Use a full JSON Schema validator or the Actor's canonical contract module when every nested scalar must be validated.

## Interpret results

- Require `schemaVersion == "nomad-agent-job-v1"` and `identity.source == "euraxess"`.
- Expect exactly six top-level keys: `schemaVersion`, `identity`, `data`, `custom`, `llm`, and `raw`.
- Validate `custom.schemaId` before interpreting `custom.data`; the extension carries EURAXESS-only labels and unsupported/malformed geofield payloads.
- Treat `identity.url` as the canonical posting URL and `data.application.url` as a distinct application URL when present.
- Sanitize `raw.descriptionHtml` before rendering; it is untrusted source HTML.
- Inspect `llm.status`, `requestedFields`, and `filledFields` before assuming optional enrichment succeeded.
- Expect no diagnostic dataset rows; inspect run logs for source, translation, and enrichment failures.

Report the bounded input, run ID/status, dataset ID, parsed row count, optional translation/enrichment settings, and any delivery-state caveat.
