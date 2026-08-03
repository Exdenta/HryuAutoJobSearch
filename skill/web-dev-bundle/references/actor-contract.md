# Actor contract and decision reference

Use this as a compact reference. Verify the deployed Actor when current state
matters; the repository may contain changes that have not been pushed.

## Identity and purpose

- Actor: `nomad-agent/web-dev-bundle`
- Purpose: aggregate web/software-development job postings from ten source
  adapters inside one Actor run, normalize them, deduplicate them, and return
  one dataset.
- Sources: `linkedin`, `remote_boards`, `builtin`, `justjoinit`,
  `nofluffjobs`, `hackernews`, `ycombinator_was`, `wttj`, `infojobs`, and
  `tecnoempleo`.
- It does not apply to jobs, message employers, manage candidates, or guarantee
  every live vacancy on every board.

## Use-case fit

| Need | Fit | Why |
|---|---|---|
| Multi-board web/software job feed | Strong | One normalized dataset across ten adapters |
| Scheduled new-job alerts | Strong | Persistent `onlyNewSinceLastRun` delta mode |
| Candidate-specific shortlist | Strong with BYOK | Prompt-driven score/reasons and threshold |
| Hiring-market analysis | Conditional | Broad supply, but upstream coverage and fields vary |
| One city across every source | Conditional | Native location filters cover only some sources |
| Fully remote roles | Strong | Native filters where possible plus strict normalized filter |
| Hybrid plus remote | Conditional | `remoteOnly` is intentionally fully remote only |
| Non-tech/general jobs | Poor | Bundle is intentionally developer-scoped |
| Automated applications/outreach | Poor | Actor retrieves data only |
| Compliance-grade exhaustive archive | Poor | Public upstreams can omit, change, block, or remove data |

## Source capability matrix

`Native` means the bundle forwards a source-specific input. `Post` means the
bundle enforces the normalized field after fetching.

| Source | Text query | Geography | Remote-only | Recency | Full-detail toggle | Title exclusion |
|---|---|---|---|---|---|---|
| LinkedIn | one request per term | `location` | Native + Post | Native | Yes | Yes |
| Remote boards | comma-separated OR | inherently remote-oriented | Post | Native | No | Yes |
| Built In | unsupported; categories | No | Post | Native | Yes | No |
| JustJoin.it | term array/OR | `location` as city | Native + Post | Native | Yes | Yes |
| NoFluffJobs | unsupported; categories | No | Native + Post | Native | Source enriches details | Yes |
| Hacker News | one request per term | No | Post | Native (hours) | No | No |
| Y Combinator | query array/fan-out | No | Native + Post | No | Yes | Yes |
| WTTJ | one request per term | `countryCodes` | Post; native includes remote candidates | No | Yes | No |
| InfoJobs | one request per term | `location` as province | Native + Post | Native | No bundle toggle | Yes |
| Tecnoempleo | one request per term | `location` as province | Native + Post | Native | Yes | Yes |

Consequences:

- `keywords` cannot make Built In or NoFluffJobs perform free-text search.
  Their run summary should say `querySupport: unsupported`.
- `location` is not an actor-wide geographic guarantee.
- `remoteOnly` drops hybrid and unknown workplace records.
- Unknown posting dates survive recency filtering on sources that cannot judge
  them; Y Combinator and WTTJ have no bundle recency input.
- Full descriptions, salary, seniority, skills, and employer fields remain
  source-dependent even though every row has a uniform schema.

## Important inputs

| Input | Meaning / caution |
|---|---|
| `sources` | Subset of the ten source keys; fewer is faster and cheaper |
| `keywords` | Preferred list; single-query sources fan out fairly |
| `keyword` | Legacy single term; use only for compatibility |
| `location` | LinkedIn location, JustJoin city, InfoJobs/Tecnoempleo province |
| `countryCodes` | WTTJ ISO alpha-2 country codes |
| `remoteOnly` | Fully remote only; excludes hybrid/unknown |
| `titleExclude` | Native only on supported sources |
| `experienceLevels` | JustJoin.it-specific |
| `maxExperienceYears` | WTTJ-specific |
| `includeDetails` | Default true; improve context at latency/payload cost |
| `postedWithinDays` | Zero disables; not supported by YC/WTTJ |
| `onlyNewSinceLastRun` | Persistent seen-URL state, shared across search profiles in the account |
| `candidateProfile` | Prose profile; enables AI matching and requires a provider key |
| `preferences` | Optional prose or JSON text with must-haves/preferences |
| `minimumMatchScore` | 0–10; 0 ranks without filtering |
| `maxItemsPerSource` | Per-source fetch ceiling |
| `maxItems` | Delivered total/cost ceiling; 0 means unlimited and should be avoided initially |
| `cacheTtlSeconds` | Default 1800; use 0 for an explicitly fresh test |
| `runTimeoutSecs` | Per-source ceiling, not total run budget |

AI matching providers are `anthropic`, `mistral`, and `openai`. Use only one of
`anthropicApiKey`, `mistralApiKey`, or `openaiApiKey`, matching `aiProvider`.
The Actor fails before the start charge when candidate matching lacks the
required key. The profile and key must not appear in the dataset or summary.

## Normalized output

Identity/content fields:

`source`, `id`, `title`, `company`, `location`, `url`, `postedAt`, `snippet`,
`description`.

Structured fields:

`salary`, `salaryMin`, `salaryMax`, `salaryCurrency`, `salaryPeriod`,
`isRemote`, `remoteType`, `seniority`, `employmentType`, `skills`,
`companyLogo`, `companyUrl`, `companySize`, `industry`, and LinkedIn hiring
contact fields.

Candidate matching fields are always present but null unless enabled:

`matchScore`, `matchReason`, `mismatchReason`, `rankingModel`.

Deduplication removes normalized URL variants and corroborated cross-source
copies. It keeps the record with richer description/snippet content. It is
stronger than URL-only dedupe but cannot prove every semantic duplicate.

## RUN-SUMMARY

Read key-value-store record `RUN-SUMMARY`. Useful fields include:

- overall `status`, timestamps, `filters`, and optional `rankingModel`;
- per-source `status`, `querySupport`, `requestCount`, `rawFetched`,
  `afterFilters`, `delivered`, and `error`;
- totals: `totalRaw`, `totalAfterFilters`, `totalUnique`, `delivered`,
  `deduplicated`, `skippedPreviouslyDelivered`, and `truncated`;
- on matching failure: `failureStage: candidateMatching` and `error`.

Partial source failure is usable and explicit. All selected sources failing,
invalid input, or failed AI matching should fail the run rather than produce a
fake diagnostic job.

## Cost and operational cautions

- Verify the live Actor page before quoting price; pricing can change.
- `maxItems` bounds delivered/billable results. Start small.
- More keywords can create multiple requests on single-query sources.
- Full details add latency and payload size.
- Cache improves repeated-run speed but can make a freshness test look stale.
- Delta state is persistent and not separately namespaced per candidate/search.
- Upstream boards can throttle, block, change schemas, or return partial data.
