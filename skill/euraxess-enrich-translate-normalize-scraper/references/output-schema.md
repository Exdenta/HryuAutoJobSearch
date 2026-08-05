# EURAXESS normalized output schema

Actor: `nomad-agent/euraxess-enrich-translate-normalize-scraper`

Common contract version: `nomad-agent-job-v1`

EURAXESS extension:
`https://raw.githubusercontent.com/Exdenta/OinkJobSearch/main/apify/job_custom_schemas/euraxess-v1.schema.json`

## Semantics

- Every declared common-contract key is present.
- `null` means unknown, unavailable, or not established by the source.
- `[]` means the source explicitly established an empty collection.
- Common typed objects and the EURAXESS extension are strict within v1.
- Deterministic source parsing wins. Optional LLM enrichment can fill only still-null allowed paths.
- `raw.descriptionHtml` is complete untrusted source HTML; sanitize it before rendering.

## Top-level envelope

| Path | Type | Meaning |
|---|---|---|
| `schemaVersion` | string | Always `nomad-agent-job-v1`. |
| `identity` | object | Stable EURAXESS identity. |
| `data` | object | Normalized facts shared with other job Actors. |
| `custom` | object | Versioned EURAXESS-only extension. |
| `llm` | object | Optional enrichment status and provenance. |
| `raw` | object | Complete plain-text and HTML detail-page content. |

## Identity and company

| Path | Type |
|---|---|
| `identity.source` | string; always `euraxess` |
| `identity.externalId` | string or null |
| `identity.url` | string or null |
| `data.title` | string or null |
| `data.company.name` | string or null |
| `data.company.sourceId` | string or null |
| `data.company.department` | string or null |
| `data.company.url` | string or null |
| `data.company.logoUrl` | string or null |
| `data.classification.industries` | array of strings or null |
| `data.classification.jobFunctions` | array of strings or null |
| `data.domains` | array of normalized research-domain strings or null |
| `data.domainsRaw` | array of source research-domain strings or null |

## Locations

`data.locations` is an array of objects or null. Each object has:

| Field | Type |
|---|---|
| `raw`, `countryName`, `city`, `region`, `postalCode`, `streetAddress`, `facilityName` | string or null |
| `countryCode` | uppercase ISO 3166-1 alpha-2 string or null |
| `positionsAvailable` | nonnegative integer or null |
| `latitude`, `longitude` | finite number or null |

Valid EURAXESS Geofield JSON can populate coordinates. Malformed or unsupported
Geofield payloads remain in `custom.data.unparsedGeofields`.

## Employment and application

| Path | Type |
|---|---|
| `data.employment.workArrangements` | array of `remote`, `hybrid`, or `onsite`; or null |
| `data.employment.applicantLocationRequirements` | array of strings or null |
| `data.employment.workSchedules` | array of strings or null |
| `data.employment.contractTypes` | array of strings or null |
| `data.employment.durationMonths`, `hoursPerWeek` | nonnegative number or null |
| `data.employment.hoursPerWeekRaw` | string or null |
| `data.employment.startDate` | ISO 8601 date/datetime or null |
| `data.employment.startDateRaw` | string or null |
| `data.application.postedAt`, `deadline` | ISO 8601 date/datetime or null |
| `data.application.referenceNumber`, `referenceNumberIssuer` | string or null |
| `data.application.url`, `email` | string or null |
| `data.application.directApply` | boolean or null |
| `data.application.eligibilityCriteria`, `selectionProcess` | string or null |

`data.application.applicantSnapshot` is null or an object with nullable nonnegative
`count`, non-empty `raw`, and timezone-aware ISO 8601 `capturedAt`.

`data.application.hiringContacts` is null or an array. Each contact contains
nullable `name`, `title`, `organization`, `url`, `email`, and `address`; at
least one must be non-null. An address contains nullable `raw`, `countryName`,
`countryCode`, `city`, `region`, `postalCode`, and `streetAddress`.

## Seniority, requirements, and compensation

| Path | Type |
|---|---|
| `data.seniority.raw` | array of source seniority labels or null |
| `data.seniority.levels` | array of generic levels or research stages `R1` through `R4`; or null |
| `data.requirements.education` | array of education paths or null |
| `data.requirements.experience` | array of experience ranges or null |
| `data.requirements.languages` | array of language requirements or null |
| `data.requirements.requiredSkills`, `preferredSkills` | array of skill requirements or null |
| `data.requirements.certifications` | array of strings or null |
| `data.requirements.skillsQualifications`, `specificRequirements` | string or null |
| `data.benefits` | string or null |
| `data.funding.programme` | string or null |
| `data.compensation.currency`, `period`, `raw` | string or null |
| `data.compensation.exact`, `minimum`, `maximum` | nonnegative number or null |
| `data.constraints.visaSponsorship` | boolean or null |
| `data.constraints.workAuthorization`, `securityClearance`, `locationPreference` | string or null |

An education path contains nullable `level`, `field`, nonnegative
`yearsRequired`, and boolean `preferred`; at least `level` or `field` is
non-null. An experience range contains nullable `field`, nonnegative
`minimumYears`, nonnegative `maximumYears`, and `raw`; at least one fact is
non-null. A language requirement contains non-empty `language`, nullable
`level`, and nullable boolean `required`. A skill requirement contains
non-empty `name` and nullable nonnegative `yearsRequired`.

`data.compensation.exact` is mutually exclusive with `minimum`/`maximum`.
Minimum values cannot exceed maximum values.

## EURAXESS extension

`custom.schemaId` must equal the extension URL above. `custom.data` has exactly:

| Field | Type | Meaning |
|---|---|---|
| `academicLevelRaw` | array of unique non-empty strings or null | Unmodified EURAXESS Positions / Academic Level search classifications; not applicant education requirements. |
| `researchInfrastructureStaffPosition` | string or null | Source response to the research-infrastructure staff-position field. |
| `unmappedJobInformation` | object of string arrays or null | Unrecognized Job Information labels retained without guessing. |
| `unparsedGeofields` | array or null | Entries with nonnegative `locationIndex` and non-empty raw Geofield payload. |

## LLM and raw content

| Path | Type |
|---|---|
| `llm.status` | `not_requested`, `completed`, or `failed` |
| `llm.requestedFields` | ordered array of `data.*` paths |
| `llm.filledFields` | ordered unique subset of `requestedFields` |
| `llm.provider`, `model`, `promptVersion` | string or null |
| `llm.completedAt` | timezone-aware ISO 8601 datetime or null |
| `raw.description` | string or null |
| `raw.descriptionHtml` | string or null |

Only `completed` enrichment may have filled paths. Deterministic values and
source-confirmed empty arrays are never replaced.
