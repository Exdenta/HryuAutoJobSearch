---
name: linkedin-enrich-translate-normalize-scraper
description: Configure, run, integrate, and troubleshoot the Apify Actor nomad-agent/linkedin-enrich-translate-normalize-scraper. Use for public LinkedIn job searches that need the strict nomad-agent-job-v1 nested schema, complete raw descriptions, deterministic card/detail/JSON-LD extraction, selected-field English translation, optional null-only BYOK LLM enrichment, rolling unseen-job delivery, or Python parsing of Actor dataset output.
---

# LinkedIn Enrich, Translate, and Normalize Scraper

Use [nomad-agent/linkedin-enrich-translate-normalize-scraper](https://apify.com/nomad-agent/linkedin-enrich-translate-normalize-scraper) when a consumer needs a stable, lossless LinkedIn job contract instead of the older compact or full-info flat outputs.

## Establish the contract

1. Verify the deployed Actor input schema, price, and build when current state matters.
2. When the source checkout is available, inspect `.actor/input_schema.json`, `.actor/actor.json`, and the Actor README before generating configuration.
3. Read [references/output-schema.md](references/output-schema.md) before mapping fields or designing storage.
4. Preserve the complete nested record. Treat `null` as unknown and `[]` as source-confirmed empty; never collapse the distinction.

## Configure a bounded run

1. Start with `maxItems: 10` to `25`; LinkedIn's public guest endpoint normally stops yielding new results near 200 regardless of a higher cap.
2. Supply `keyword`, `location`, and a `timeFilter` only when required. Valid time filters are `r3600`, `r86400`, `r604800`, and `r2592000`.
3. Keep `deliveryMode: "unseen"` for scheduled alerts. Reuse a public opaque `dedupeKey` for the same alert/profile. Use `deliveryMode: "all"` only when repeat delivery is intentional.
4. Enable `translateToEnglish` only when English title, industry, and job-function values justify the additional per-result charge. The Actor never translates company, location, description, or `raw` fields.
5. Enable `aiEnrichment` only when description-backed missing facts are needed. Set `aiProvider` and exactly the matching secret input: `anthropicApiKey`, `mistralApiKey`, or `openaiApiKey`. Static and source-established empty values always win.
6. Keep `analyticsEnabled: false` unless the caller explicitly opts in. Keep tokens and provider keys out of source, logs, URLs, and returned examples.

Minimal exploration input:

```json
{
  "keyword": "frontend engineer",
  "location": "Spain",
  "timeFilter": "r604800",
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
    "nomad-agent/linkedin-enrich-translate-normalize-scraper"
).call(run_input={
    "keyword": "frontend engineer",
    "location": "Spain",
    "timeFilter": "r604800",
    "maxItems": 25,
})
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

Require a terminal `SUCCEEDED` run status before consuming the dataset. Do not interpret a zero-row run as proof that no jobs exist without checking the run log, input, delivery state, source availability, and result cap.

## Parse output

Use [scripts/parse_output.py](scripts/parse_output.py) for the importable `parse_linkedin_output()` and `parse_linkedin_outputs()` functions. Copy the module beside the integration or import it by file path; it has no third-party dependencies.

```python
from parse_output import parse_linkedin_outputs

jobs = parse_linkedin_outputs(items)
for job in jobs:
    print(job.title, job.company_name, job.posting_url)
    complete_record = job.normalized
```

The parser validates the stable envelope and LinkedIn invariants, returns common convenience fields, and retains the complete normalized record. Use a full JSON Schema validator or the Actor's canonical contract module when every nested scalar must be validated.

## Interpret results

- Require `schemaVersion == "nomad-agent-job-v1"` and `identity.source == "linkedin"`.
- Expect exactly six top-level keys: `schemaVersion`, `identity`, `data`, `custom`, `llm`, and `raw`.
- Expect `custom` to be `null` for LinkedIn.
- Treat `identity.url` as the canonical posting URL and `data.application.url` as a distinct application URL when present.
- Sanitize `raw.descriptionHtml` before rendering; it is untrusted source HTML.
- Inspect `llm.status`, `requestedFields`, and `filledFields` before assuming optional enrichment succeeded.
- Expect no diagnostic dataset rows; zero-result and error diagnostics belong in the run log.

Report the bounded input, run ID/status, dataset ID, parsed row count, optional translation/enrichment settings, and any delivery-state caveat.
