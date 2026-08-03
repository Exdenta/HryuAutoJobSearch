---
name: uk-nhs-ods-providers-scraper
description: Evaluate, configure, run, or integrate the Apify Actor nomad-agent/uk-nhs-ods-providers-scraper for official UK NHS Organisation Data Service provider records. Use for bounded searches by organisation name, postcode, provider type, specialty, role, status, or change date; full provider lookup by ODS code; scheduled delta runs; optional postcode geocoding or relationship-name enrichment; and interpretation of normalized records, diagnostics, and partial results.
---

# UK NHS ODS Providers Scraper

Use Actor `nomad-agent/uk-nhs-ods-providers-scraper`. It queries the public, unauthenticated NHS ODS ORD API; an NHS API key, browser, and proxy are not required.

## Decide fit

Use it for registered organisations and sites such as GP practices, pharmacies, dental practices, NHS trusts, hospices, care homes, and optical sites. It returns canonical ODS codes, registry metadata, and, by default, full address, contact, role, relationship, and date details.

Do not use it for patient or clinician records, service availability, appointments, reviews, private NHS systems, or guaranteed current operational status beyond the live registry fields. Primarily expect England data plus cross-border organisations that hold ODS codes.

## Build bounded input

Always set `maxItems` to a positive task-sized cap. The default is `100`; `0` is uncapped and can traverse the roughly 280,000-record registry. Keep `concurrency` within the actor's effective range of 1–16; start with `8`.

Choose one mode:

- Use `search` with at least one of `name`, `postCode`, `providerType`, `specialty`, `roleId`, `lastChangeDate`, or `onlyNewSinceLastRun`. A status alone does not make a valid bounded search.
- Use `detail` with non-empty `odsCodes`. Codes are deduplicated and may be supplied as bare codes or full registry URLs. Unknown or failed codes are skipped.

Search filters behave as follows:

- `roleId` overrides `providerType`, which overrides `specialty`.
- `providerType` supports `any`, `gp_practice`, `pharmacy`, `dental_practice`, `nhs_trust`, `nhs_trust_site`, `hospice`, `care_home`, `optical_site`, `independent_provider`, and `branch_surgery`.
- `specialty` is matched best-effort against the live ODS role vocabulary. If it cannot be resolved, the actor ignores it; use `roleId` when exactness matters.
- `status` is `active`, `inactive`, or `all`; `all` omits the status filter.
- `postCode` accepts a full postcode, district, or letters-only area. A letters-only area is expanded into numbered district prefixes and merged with ODS-code deduplication.
- `lastChangeDate` is sent to ORD as supplied. The upstream registry rejects dates more than about 185 days old.

Example search:

```json
{
  "mode": "search",
  "postCode": "BS1",
  "providerType": "pharmacy",
  "includeDetails": true,
  "maxItems": 100,
  "concurrency": 8
}
```

Example detail lookup:

```json
{
  "mode": "detail",
  "odsCodes": ["L81008", "RA7"],
  "includeGeo": true,
  "includeRelationshipNames": true,
  "maxItems": 10
}
```

## Choose enrichments and delta behavior

- Leave `includeDetails: true` in search mode for full records. Set it to `false` for faster identity-only search rows. Detail mode always fetches full records.
- Set `includeGeo: true` to add best-effort `latitude` and `longitude` from postcodes.io. Unresolved postcodes receive `null` coordinates.
- Set `includeRelationshipNames: true` to resolve `relationships[].targetName` with one extra ODS request per unique target. Search mode also needs `includeDetails: true`; failures leave the name `null`.
- Use `onlyNewSinceLastRun: true` only in search mode. The actor stores one date checkpoint in its named key-value store; it is shared across filter choices and is not an ID-level deduplication history. A first delta run without a name, postcode, or role filter records today's checkpoint and returns a diagnostic row with no providers. With one of those filters, the first run returns a baseline; later runs override `lastChangeDate` with the stored checkpoint and tag returned records `isNew: true`.

## Integrate safely

Keep the Apify token in `APIFY_TOKEN`; never print, commit, or put it in a shared query string.

Python:

```python
import os
from apify_client import ApifyClient

actor_input = {
    "mode": "search",
    "postCode": "BS1",
    "providerType": "pharmacy",
    "maxItems": 100,
}
client = ApifyClient(os.environ["APIFY_TOKEN"])
run = client.actor("nomad-agent/uk-nhs-ods-providers-scraper").call(
    run_input=actor_input
)
items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
```

REST:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $APIFY_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{"mode":"detail","odsCodes":["L81008"],"maxItems":1}' \
  "https://api.apify.com/v2/acts/nomad-agent~uk-nhs-ods-providers-scraper/run-sync-get-dataset-items"
```

Prefer an asynchronous run followed by dataset pagination for broad searches or detail enrichment; a synchronous request can time out while the Actor continues.

## Interpret output and failures

Treat the dataset, not logs, as the result. Every provider has `odsCode`, `name`, `status`, role fields, `postCode`, `orgRecordClass`, `lastChangeDate`, `orgLink`, and `source: "nhs-ods"`. Full records can also contain address fields, `uprn`, phone, website, `roles`, `relationships`, and operational/legal dates. Optional modes add coordinates, resolved relationship names, or `isNew`.

Handle these cases explicitly:

- A dataset row with `warning` and `docs` is a diagnostic, not a provider. Surface it and exclude it from provider processing.
- A valid search with no matches can produce an empty dataset. Do not treat that as proof of an outage.
- Search-page failures preserve already gathered rows. Detail-enrichment failures in search mode leave the lighter search row; failed codes in detail mode are omitted. Compare returned counts with requested codes and caps.
- Optional role-name, geocoding, and relationship-name lookups are best-effort. Empty descriptions or `null` enrichments do not invalidate the base record.
- The actor charges an Actor-start event and one result event per delivered provider, subject to the run's maximum total charge. Check the Actor page for current prices and keep the cap explicit.

Return the bounded Actor input, chosen integration method, expected fields, provider count, any diagnostic rows or missing requested codes, and relevant caveats.
