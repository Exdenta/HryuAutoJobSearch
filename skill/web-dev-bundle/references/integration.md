# Integration patterns

Prefer the official Apify client and asynchronous runs for production. Keep the
Apify token in `APIFY_TOKEN`; keep an optional ranking key in its own secret.

## Recommended first-run input

```json
{
  "sources": ["linkedin", "remote_boards", "justjoinit"],
  "keywords": ["react", "typescript", "frontend engineer"],
  "location": "Madrid",
  "postedWithinDays": 14,
  "includeDetails": true,
  "maxItemsPerSource": 8,
  "maxItems": 20,
  "cacheTtlSeconds": 0
}
```

Remove `location` when the goal is worldwide remote supply; use
`remoteOnly: true` only when hybrid roles must be excluded.

## Python: run, consume jobs, inspect summary

```python
import os
from apify_client import ApifyClient

client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/web-dev-bundle").call(run_input={
    "sources": ["linkedin", "remote_boards", "justjoinit"],
    "keywords": ["react", "typescript"],
    "postedWithinDays": 14,
    "includeDetails": True,
    "maxItemsPerSource": 10,
    "maxItems": 25,
})

jobs = list(client.dataset(run["defaultDatasetId"]).iterate_items())
record = client.key_value_store(run["defaultKeyValueStoreId"]).get_record(
    "RUN-SUMMARY"
)
summary = record["value"] if record else None

if not summary:
    raise RuntimeError("RUN-SUMMARY missing; verify the deployed Actor build")
if summary["status"] == "partial":
    failed = {
        source: data["error"]
        for source, data in summary["sources"].items()
        if data["status"] != "succeeded"
    }
    print("Partial source coverage:", failed)
elif summary["status"] != "succeeded":
    raise RuntimeError(summary.get("error") or "Actor run failed")

for job in jobs:
    print(job["title"], job["company"], job["url"])
```

`actor().call()` waits for completion but uses a normal asynchronous Actor run,
which is safer than an HTTP sync endpoint for larger jobs.

## Python: candidate-ranked alert input

```python
run_input = {
    "sources": ["linkedin", "remote_boards", "justjoinit", "wttj"],
    "keywords": ["frontend engineer", "react", "typescript"],
    "countryCodes": ["ES", "FR", "DE"],
    "remoteOnly": True,
    "postedWithinDays": 7,
    "onlyNewSinceLastRun": True,
    "candidateProfile": (
        "Senior frontend engineer; React, TypeScript, Next.js; based in Spain; "
        "EU work authorization; English and Spanish; fully remote required."
    ),
    "preferences": "Avoid internships and people-management-only roles.",
    "minimumMatchScore": 7,
    "aiProvider": "anthropic",
    "anthropicApiKey": os.environ["ANTHROPIC_API_KEY"],
    "maxItemsPerSource": 20,
    "maxItems": 40,
}
```

Do not log `run_input` when it contains a profile or API key. A scheduled alert
should persist and inspect both the dataset and summary each run.

## HTTP: small synchronous smoke test

```sh
curl --fail-with-body \
  -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  'https://api.apify.com/v2/acts/nomad-agent~web-dev-bundle/run-sync-get-dataset-items' \
  -d '{
    "sources": ["linkedin", "remote_boards"],
    "keywords": ["react"],
    "maxItemsPerSource": 5,
    "maxItems": 10,
    "cacheTtlSeconds": 0
  }'
```

This endpoint returns dataset items, not `RUN-SUMMARY`. For production, create
a normal run, wait for completion, then read both storages using the returned
dataset and key-value-store IDs.

## Generic asynchronous REST flow

1. `POST /v2/acts/nomad-agent~web-dev-bundle/runs` with JSON input and bearer
   authorization.
2. Read `data.id`, `data.defaultDatasetId`, and `data.defaultKeyValueStoreId`.
3. Poll `GET /v2/actor-runs/{runId}` until terminal status.
4. On success, read `GET /v2/datasets/{datasetId}/items?clean=true`.
5. Read `GET /v2/key-value-stores/{storeId}/records/RUN-SUMMARY`.
6. Accept `summary.status=partial` only according to the application's source
   coverage policy; alert on `failed` or missing summary.

## Mapping into another system

Use `url` as the primary external identity. Store `source` and `id` for audit,
but do not assume source IDs are globally unique. Preserve nullable fields as
unknown; do not turn missing salary, location, remote status, or match reasons
into guesses.

For notifications, store the URL before sending and make delivery idempotent.
Actor delta mode reduces repeats but should not replace the consumer's own
delivery ledger.

## Common mistakes

- Sending `keywords` and assuming category-only sources honored them.
- Treating `location` as a global filter across all ten sources.
- Enabling `remoteOnly` while expecting hybrid jobs.
- Setting `maxItems: 0` in the first or scheduled run.
- Ignoring `RUN-SUMMARY` because the dataset is nonempty.
- Using candidate matching without the selected provider's valid key.
- Logging secrets or the candidate profile.
- Assuming a local schema proves the production Actor has been deployed.
