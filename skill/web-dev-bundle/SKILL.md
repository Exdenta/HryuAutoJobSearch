---
name: web-dev-bundle
description: Evaluate, explain, integrate, configure, or troubleshoot the Apify Actor nomad-agent/web-dev-bundle, which aggregates web-developer jobs from ten sources. Use when an agent must decide whether the Actor fits a job-search, alerting, recruiting, market-data, or candidate-matching use case; generate Apify API/SDK integration code; choose inputs and cost caps; interpret normalized jobs or RUN-SUMMARY; or explain source coverage, limitations, partial failures, deduplication, delta mode, and optional BYOK AI ranking to a human.
---

# Web Developer Jobs Bundle

Help a human or agent decide whether `nomad-agent/web-dev-bundle` is useful,
then produce the smallest safe integration that meets the need.

## Establish the live contract

Do not assume the repository and deployed Actor are identical.

1. When an Actor source checkout is available, read:
   - `apify/web-dev-bundle/.actor/input_schema.json`
   - `apify/web-dev-bundle/.actor/output_schema.json`
   - `apify/web-dev-bundle/README.md`
   If those files are absent, use the bundled references below.
2. When the user means the deployed Actor, verify its current schema, pricing,
   and build before making current-state claims. Say when local code appears
   ahead of production.
3. Never invent an input that is absent from the verified schema.

Read [references/actor-contract.md](references/actor-contract.md) for the
source/filter matrix and field semantics. Read
[references/integration.md](references/integration.md) when writing API/SDK
code, scheduling runs, or consuming results.

## Choose the task

### Assess usefulness

Infer or ask only for missing facts that can change the verdict:

- roles/technologies and whether they are mainly software/web development;
- target countries, city constraints, and fully remote versus hybrid;
- snapshot, scheduled alert, market analysis, or candidate matching;
- desired freshness, result volume, latency, and spend ceiling;
- whether the caller can supply an Apify token and, for AI ranking, an LLM key.

Return one verdict:

- **Strong fit** — web/software roles, multi-board coverage, normalized output,
  monitoring, or candidate ranking materially reduces integration work.
- **Conditional fit** — useful, but a required filter applies only to some
  sources, descriptions are incomplete on some boards, or a narrow geography
  needs downstream/AI review.
- **Poor fit** — non-tech hiring, guaranteed exhaustive coverage, applying to
  jobs, contacting candidates, deterministic filtering across every board, or
  zero-tolerance requirements for stale/missing upstream data.

Support the verdict with concrete coverage and limitations. Distinguish facts
from inferences. Never describe the Actor as an ATS, application bot, or
guaranteed-complete labor-market dataset.

### Build an integration

1. Start with a capped test: 1–3 sources, `maxItemsPerSource` 5–10,
   `maxItems` 10–30, and `cacheTtlSeconds: 0` only when freshness matters.
2. Prefer `keywords` over legacy `keyword`. Explain that Built In and
   NoFluffJobs are category-based and report text-query support as unsupported.
3. Use native geography deliberately: `location` reaches LinkedIn,
   JustJoin.it, InfoJobs, and Tecnoempleo; `countryCodes` reaches WTTJ.
4. Use `remoteOnly: true` only for fully remote jobs. It excludes hybrid and
   unknown rows after normalization.
5. Keep `includeDetails: true` when ranking or summarizing job fit; disable it
   for a fast listing-only scan.
6. Use `onlyNewSinceLastRun: true` for repeat alerts. Note that its seen set is
   account/Actor-wide, not separately keyed per search profile.
7. Add `candidateProfile`, `preferences`, and `minimumMatchScore` only when the
   user wants ranking. Supply exactly the selected provider's secret key.
8. Prefer asynchronous runs for production. Use synchronous dataset-returning
   calls only for small interactive tests.
9. Consume both the dataset and `RUN-SUMMARY`. A nonempty dataset does not mean
   every source succeeded.
10. Keep tokens and LLM keys in environment variables or a secret store. Never
    print them, embed them in source, or place them in URLs when a bearer header
    is available.

### Explain or troubleshoot a run

Inspect `RUN-SUMMARY` first:

- `status=succeeded`: all selected source requests completed.
- `status=partial`: use delivered jobs, but name each failed/partial source.
- `status=failed`: identify `failureStage`/`error`; do not treat it as an empty
  market.
- Compare `rawFetched`, `afterFilters`, `totalUnique`, and `delivered` to locate
  loss from source filters, remote-only enforcement, dedupe, candidate score,
  delta state, result caps, or charge limits.
- Check `querySupport`; do not blame zero keyword matches on a category-only
  source that never supported text search.
- `truncated=true` means the result/cost ceiling stopped delivery.

When no summary exists, verify that the deployed build exposes it before
diagnosing. Older production builds may only provide a dataset.

## Output expectations

For an assessment, lead with the verdict, then provide:

1. what it covers well;
2. material gaps for this use case;
3. a recommended capped input;
4. expected output/operational behavior;
5. the next cheapest validation step.

For integration work, return runnable code in the user's language, explain
required environment variables, read `RUN-SUMMARY`, and include explicit
handling for partial and failed runs. Keep optional fields out until the use
case requires them.
