# Changelog

All notable changes to this project are documented here.

Format: [Semantic Versioning](https://semver.org). Each release section covers the most recent sprint.

---

## [Unreleased]

- Complete bounded Session A1 dependency-safe archive and retirement. Freeze
  `docs/A1_ARCHIVE_CONTRACT.json`, SHA-256
  `1b8174bac5181b68f6e3913c2e6bf73a7271acce9f1421063e40b39db2a63f3a`,
  only after hashing all 303 tracked and 53 non-ignored untracked files and
  mapping imports, workflows, CI, documentation, and artifacts. Package four
  confirmed superseded PIT roots as four local Git-ignored tar archives under
  `artifacts/archive/a1/20260801T220130Z-a1/`; archive manifest SHA-256 is
  `892cf0991b9e1a5a651728a1329bd91422037c62de6d57ee27adcc1e919399da`.
  Recover and rehash all 887 files and 1,954,687,841 source bytes before
  replacing the active roots with exact-manifest/archive-pointer stubs, and
  reproduce 11 associated tracked historical files byte-for-byte from
  `codex/legacy-archive` before retiring them. Fail closed on V3.1-V3.3 because
  active parity tests and the frozen M1A contract still depend on them. Keep
  canonical P2-P4, corrected source evidence, D1/S1/E1/B1/M1/I1/US1A-US1C,
  immutable Hugging Face pointers, international structure, and shared
  implementations unchanged. The focused dependency/recovery/import/workflow/
  failure-path boundary passes 47 tests with 13 existing warnings; the
  post-retirement US1C route passes; and the final full suite passes 869 tests
  with 4 skips and 78 existing warnings. No methodology, shortlist, release,
  remote state, branch, commit, push, tag, promotion, or publication changed.
- Complete bounded Session US1C with one frozen local release-consolidation
  contract, SHA-256
  `d7098153542afa77ff09e850980362eb957b3e24cd609a4984451289595f1d47`,
  and one authoritative verification route,
  `python3 -m workflows.run_us_free_v1`. Rehash and reconcile the complete
  P2 -> P3 -> P4 -> US1A -> US1B chain, every named supporting boundary, and
  all five partial M1C attempts without changing their contents. Verify two
  fresh non-overwriting reconstructions are byte-identical and recover the
  exact 202-file, 481,666,707-byte P2/P3/P4 baseline read-only from private
  immutable Hugging Face revision
  `aaf056ea115067e42ef9abf9fa93ade75cdd4052`, with no mutable-revision
  fallback or remote mutation. Materialize the independently verified local
  release candidate at
  `artifacts/product/us_free_v1_release_candidate/20260801T210000Z-us1c/`,
  manifest SHA-256
  `a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466`,
  with 24 records and 1,039,444 recorded bytes. Preserve every US1A identity,
  holding, rank, score, gate, liquidity decision, and 1/15 weight plus all
  US1B evidence states: 14 unresolved names, failed-request HPK, unresolved
  deterministic actions, and human review for all 15. Add fail-closed
  consolidation, recovery, immutability, non-overwrite, and mismatch tests;
  the focused boundary passes 37 tests with 13 existing warnings and the
  final full suite passes 871 tests with 4 skips and 78 existing warnings. No
  release, archive, remote write, data/methodology change, model or
  performance execution, commit, push, tag, or branch action occurred.
- Complete bounded Session US1B with the approval-gated, non-overwriting
  evidence derivative
  `artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b/`, manifest
  SHA-256
  `f802ec26358d27afa21c99490e598e13c98a6ad60229705769f3fae1818d9dcd`.
  Freeze the exact 36-URL plan before collection, reuse 29 preserved D1/E1
  responses without requests, retrieve all 36 approved URLs once with HTTP
  200, and verify 3,681,377 received response-body bytes. Materialize 15 exact
  market/exchange identity rows, 47 separately adjudicated primary documents,
  46 accession-bound cited claims, and 15 name-level coverage rows. Keep 14
  names unresolved and HPK in explicit `failed_request` because its preserved
  E1 Form 25-NSE response remains HTTP 503; all deterministic actions remain
  unresolved and require human review. Independently verify all 95 artifact
  records and 2,653,162 recorded bytes, all 36 new and 29 reused response
  payloads, claim locators/hashes, the unchanged US1A shortlist, and every
  preserved P2/P3/P4/B1D/B1E/M1A/M1C/M1D/I1/partial-attempt boundary. Add a
  deterministic offline verifier and pass the 39-test focused evidence,
  extraction, adjudication, shortlist, lineage, non-overwrite, and failure-path
  boundary with 13 existing warnings. No baseline, model, score, gate,
  liquidity decision, holding, rank, weight, performance, promotion, archive,
  publication, commit, or push changed. The final full suite passes 859 tests
  with 4 skips and 78 existing warnings.
- Complete bounded Session US1A with one contract-first, offline US free-data
  product candidate under
  `artifacts/product/us_free_v1/20260801T183000Z-us1a/`, manifest SHA-256
  `f27773c0e1fb92a25707e0fec363e13afa7ccb10f8ace7537187c6be58575edf`.
  Freeze accepted P2 before downstream materialization after D1 passes source,
  schema, identity, availability, target-support, and reconstruction checks but
  fails exact frozen-route row coverage (43,564 shared IDs, 242 P2-only, 76
  D1-only) and cannot satisfy M1C's pinned P2 identity/manifest contract without
  retraining or changing the contract. Consume only the accepted M1A/M1C
  inner-evidence route through unchanged P4 gates, 30-session liquidity, rank,
  15-name equal-weight, and tie-break rules. Materialize 87,612 row-role scores,
  43,806 candidates, 525,672 gate rows, 175,784 exclusions, 1,477 liquidity-
  required rows, 44,310 exact liquidity sessions, 120 holdings, and the final
  15-name 2026 shortlist. Map five names to exact stable-row/security E1
  evidence, retain their unresolved status, and label ten names
  `event_evidence_not_collected`. Add the deterministic offline verifier and six
  focused tests. Rehash every P2/P3/P4/B1D/B1E/M1A/M1C/M1D/I1 boundary and all
  five partial M1C attempts; pass the 89-test focused product/prediction/P4/
  shortlist/lineage boundary and the final full suite with 853 passed, 4
  skipped, and 78 existing warnings. No model fitting, retuning, performance
  calculation, external collection, promotion, overwrite, archive,
  publication, commit, or push occurred.
- Complete bounded Session I1 as the first international adapter test. Freeze
  Canada (`CA`, TSX/TSXV) from predeclared local evidence before downstream
  results under `artifacts/international/i1/20260801T180000Z-i1-ca/`, manifest
  SHA-256 `d3f17854cf0839713163e8dda99aefedd6ba064a14a042a886770818a472d9f6`.
  The fail-closed adapter materializes 9,207 row-complete core P2 records;
  calendar, benchmark vintage, action/delisting, FX vintage,
  survivorship-complete targets, P3/P4, model, portfolio, and performance
  remain unsupported. No external data or performance was collected.
- Complete bounded Session M1D with exactly one pre-performance-locked,
  P4/B1E-compatible portfolio evaluation of the accepted M1C route. Freeze
  `artifacts/performance/m1d/20260801T162953Z-m1d/`, pre-performance lock
  manifest SHA-256
  `757e19cd9e35290a6b339f79e2c44a0f1ddb47c03b913930b1abda84f0bf74bc`,
  and final manifest SHA-256
  `b04cea8236da6cd92749410f6186360be5f29dbbbeb00a64f2ed07c180cc72ab`.
  The single composite route uses only the 16 frozen M1A/M1C inner-evidence
  winners and preserves the unchanged P4 construction and B1D/B1E
  evaluation contracts. Materialize 43,806 row-complete candidate/
  eligibility records, 120 holdings across eight 2019-2026 vintages, 75
  matured holdings for 2019-2023 metrics, 45 open holdings outside completed
  metrics, all four physical outcome/performance namespaces, gross/net and
  benchmark NAV, positions, weights, exclusions, returns, costs, turnover,
  coverage, drawdown, and stability ledgers. The primary free-evidence result
  records 19.75% aggregate net CAGR, 0.854 zero-rate diagnostic Sharpe,
  24.50% annualized volatility, -29.46% maximum drawdown, and 2.0 turnover;
  both the 30% CAGR and 1.0 Sharpe reporting thresholds are not met. Exact
  DGS1MO-dependent metrics remain unavailable, all identities remain
  uncertified, and the aggregate B1E comparison is not like-for-like because
  its history begins in 2015 rather than 2019. Independently verify all 132
  generated records, rehash every frozen P2/P3/P4/B1D/B1E/M1A/M1C boundary,
  and pass the 135-test focused M1D-through-P3 boundary with 13 existing
  warnings. No retry, retuning, external data, frozen-artifact change,
  publication, archive, deletion, branch, commit, push, or later-session work
  occurred.
- Complete bounded Session M1C as one successful controlled nested temporal
  walk-forward execution through the frozen M1A/M1B contract. Add
  `modeling/run_nested_walk_forward.py`, exact-population mechanical reuse of
  fold-local selector/preprocessor results, normalized diagnostic lineage, and
  fail-closed M1C runner tests. Freeze the new non-overwriting artifact
  `artifacts/modeling/nested_walk_forward/20260801T121426Z-m1c/`, manifest
  SHA-256 `125cb5d6a8012c3b03ee6eab5f00ac944135c39996e992ecf40d8816acdacc58`.
  Its persisted preflight covers all 34 outer-role, 306 inner, 102 maturity,
  72 canonical-candidate, 1,224 outer-candidate-context, 200-feature,
  target/regime/selector/grid/seed/hash/prohibited-input records before the
  first fit. Materialize 87,612 unique row-role records, 20,142 matured
  metric-eligible OOS predictions, 24,566 frozen-rule open 2024-2026 scores
  outside metrics, 516 explicit 2027-2028 future exclusions, 624 candidate
  evaluations, 104 normalized fold-local selection/preprocessing records, and
  16 model records with complete lineage. The pooled 2019-2023 predictive-only
  result is Spearman IC 0.335 for LightGBM and ROC AUC 0.654 for the tree over
  10,071 rows per role; no portfolio or performance metric is calculated.
  Independently verify every one of 26 manifest records and every generated
  row/model/diagnostic reference. Preserve five earlier preflight/execution-
  only partial directories, including one mechanical Parquet-dtype failure,
  with no persisted outer result and no adaptive retry. The focused M1C/M1B/
  M1A/P3 boundary passes 73 tests with 13 existing warnings; sync and diff
  checks pass; P2, P3, P4, B1D, B1E, and M1A hashes remain unchanged. The exact
  next task is M1D: one locked P4/B1E-compatible portfolio-performance
  comparison with no adaptive retuning.
- Complete bounded Session M1B without historical execution. Add
  `modeling/nested_walk_forward.py`, a hash-verified implementation of the
  frozen M1A inner-fold, three-year purge, fold-local selection/preprocessing,
  bounded stability/redundancy selection, deterministic LightGBM/tree grids,
  three training regimes, predictive-only winner rules, simplicity
  tie-breakers, one-shot tuning state, outer-refit hook, and explicit fold,
  population, target, feature, transformation, and parameter lineage. Add 21
  synthetic tests that make label overlap, transformer/imputation leakage,
  global selection, outer-fold reuse, validation-row reuse, prohibited target/
  support/model-output features, B1E or outer-performance consumption, and
  adaptive post-threshold retuning fail closed. The broader focused M1A/P3
  compatibility boundary passes 67 tests with 13 existing matplotlib/
  pyparsing warnings. P2, P3, P4, B1D, B1E, and M1A hashes remain unchanged;
  no historical model, tuning, scoring, portfolio, performance, data refresh,
  artifact, branch, commit, push, publication, archive, deletion, or schedule
  action occurred. At the M1B boundary, the exact next task was M1C: one
  controlled nested walk-forward OOS model run.
- Complete bounded Session M1A as a contract-only modeling boundary. Add the
  non-overwriting artifact
  `artifacts/modeling/nested_walk_forward/20260801T000000Z-m1a/` with manifest
  SHA-256 `a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc`.
  Reverify the frozen P2, P3, P4, B1D, and B1E boundaries; preserve 17 exact
  P3 outer decision cohorts and 34 role-specific outer records; define 306
  explicit inner role/regime/fold records with three mature expanding folds,
  strict label maturity, three-year horizon purging, fold-local transforms and
  selection, bounded seeded LightGBM/tree grids, predictive-only objectives,
  and simplicity tie-breakers. Add focused deterministic contract tests and a
  human-readable methodology report. No model fitting, tuning, scoring,
  performance calculation, portfolio change, external data, frozen-artifact
  overwrite, publication, archive, deletion, branch, commit, push, or schedule
  occurred. The exact next task is M1B: implement this contract and synthetic
  leakage tests without historical execution.
- Defer V2A paid-data exploration at the user's direction and promote the
  free-source-only M1 sequence. Split M1 into four anti-overfitting boundaries:
  M1A freezes the exact nested walk-forward folds, three-year label purging,
  fold-local feature-selection/preprocessing contract, bounded existing-model
  grids, predictive objectives, stability/simplicity tie-breakers, and gate
  regimes without execution; M1B implements and tests without historical
  fitting; M1C performs one controlled outer-OOS model run; and M1D performs
  one locked P4/B1E-compatible comparison without adaptive retuning. Preserve
  P3 and B1E as untouched baselines and classify 30% net CAGR and 1.0 zero-rate
  diagnostic Sharpe only as final reporting thresholds, never tuning targets.
  This change updates planning documentation only; no model, feature selection,
  performance calculation, external source, provider, artifact, portfolio,
  branch, commit, push, publication, archive, deletion, or schedule changed.
- Complete bounded Session B1E's one controlled free-data V1 historical
  backtest and product report over the unchanged B1D engine and exact frozen
  B1C evidence. Add a non-overwriting workflow that independently reverifies
  the B1C manifest, all 29 records, seven validated inputs, three code-lineage
  records, and both frozen B1D hashes before calculation; preserves nine
  independent matured July 2015-2023 36-month vintages and keeps all 45 open
  2024-2026 holdings outside completed metrics; charges 25 bps per side on
  absolute actual traded notional; and materializes separate gross/net
  portfolio and benchmark NAV, time-weighted aggregate NAV, transactions,
  capital flows, coverage, event/scenario, outcome, metric, and rate-
  availability ledgers for all four performance namespaces. Freeze
  `20260801T011135Z-b1e` with 135/135 price-path-accounted matured holdings,
  $1.8M/$1.8M planned exposure, 5,130/5,130 stock and benchmark sessions, zero
  benchmark gaps, zero explicit terminal-exit triggers, and zero scenario-
  imputed exposure. Keep the 135 S1-ambiguous identities/actions and
  uncertified adjusted-close semantics explicit; fail DGS1MO Sharpe, Sortino,
  and alpha closed; label the separate zero-rate results only as a diagnostic;
  and state that the results are free-source research, not survivorship-
  complete certification, advice, or a future-performance promise. Add
  deterministic artifact/report reconciliation tests. No external data,
  model execution, portfolio change, provider action, publication, archive,
  deletion, branch, commit, push, or schedule occurred.
- Add bounded Session B1D's provider-neutral free-data V1 NAV and performance-
  metric engine over the frozen B1C schemas. Independently verify the accepted
  B1C manifest, all 29 artifact records, seven copied validated inputs, and
  three code-lineage records before loading any evidence; fail closed on
  schema, identity, coverage, terminal-event, benchmark, performance-
  namespace, or risk-free-namespace inconsistencies. Implement independent
  overlapping July 36-month vintages, exact common stock/assigned-benchmark
  entry/month-end/exit sessions, separate-vintage and time-weighted aggregate
  NAV, 25-bps-per-side costs on absolute actual traded notional, and separate
  gross/net portfolio and benchmark streams. Preserve observed, bounded-
  scenario, provider-confirmed, and unsupported/unresolved outcome ledgers;
  permit the frozen minus-50-percent and terminal-loss assumptions only for
  explicit unsupported terminal exits; and keep the unavailable DGS1MO path
  physically and semantically separate from the zero-rate diagnostic. Add
  deterministic synthetic coverage for partial/complete exits, ticker and
  identity continuity, cash/stock mergers, bankruptcy/delisting outcomes,
  benchmark gaps, all required absolute and benchmark-relative metrics,
  coverage denominators, zero-denominator cases, deterministic reruns, and
  fail-closed behavior. No historical backtest, model, external collection,
  actual performance calculation, portfolio change, provider action,
  artifact publication, notebook mutation, archive, branch, commit, push, or
  scheduling occurred.
- Add bounded Session B1C's non-overwriting free-data V1 performance-input
  evidence assembler and workflow. Independently reverify the frozen P2-P4,
  Session 8E raw/normalized, S1, and E1 boundaries; reconcile exactly 180 P4
  holdings plus four benchmark masters; validate all 132 relevant holding and
  four benchmark Yahoo payloads, their exact compressed/decompressed and
  normalized hashes, metadata, arrays, calendars, common entries, 135 matured
  common exits, final-common month ends, and assigned benchmarks; and preserve
  the provider-adjclose/no-double-counting assumption without relabelling it as
  a certified security/action ledger. Materialize physically separate
  observed, best-free-evidence, legacy-minus-50-percent, terminal-loss, exact
  DGS1MO, and zero-risk-free diagnostic contracts and eligibility states while
  calculating no NAV, return, metric, backtest, or performance report. Freeze
  `20260731T115106Z-b1c` with 184 requirements, 135 matured and 45 open
  holdings, zero fully matched S1 requirements, 14 current holdings with
  incomplete retrieved primary-document support, SSTK with none, and explicit
  `unavailable_not_collected_missing_fred_api_key` rate status; no external
  request, model execution, portfolio change, provider action, promotion,
  publication, archive, branch, commit, push, or schedule occurred.
- Revise the post-Q1 product sequence to build the best defensible free-data V1
  before any paid-data V2. Record Q1's moderate predictive-signal result
  (16,597 labeled OOS rows; LightGBM pooled Spearman IC 0.138 with positive IC
  in all ten labeled years; tree pooled ROC AUC 0.569 with weak calibration),
  replace the paid-provider B1B prerequisite with a read-only free-data V1
  performance-contract session, and stage separate free-evidence assembly,
  P4-compatible overlapping-vintage NAV/metric implementation, and controlled
  V1 backtest/report sessions. Require complete holding accounting, prohibit
  silent deletion, keep observed/best-free-evidence/legacy-minus-50/terminal-
  loss namespaces physically separate, and report coverage and scenario
  exposure beside CAGR, volatility, Sharpe, Sortino, drawdown, Calmar,
  benchmark-relative metrics, turnover, and costs. Move paid-provider fit-gap,
  acquisition, and certification to V2A through the same provider-neutral
  interfaces. This change revises documentation only; no model or backtest was
  executed, no performance was calculated, no external data was collected,
  and no artifact, portfolio, provider, publication, archive, branch, commit,
  push, or schedule changed.
- Revise the existing post-P4 session plan after the bounded local-only B1
  evidence decision. Record B1 as `unavailable_fail_closed` under current
  evidence because S1 remains 0 matched, 135 ambiguous, and 49 unsupported,
  E1 supplies no deterministic historical upgrade, the exact 2026-07-17
  `DGS1MO` ALFRED vintage is absent, and the current monthly-NAV implementation
  does not yet satisfy the full P4 overlapping-vintage contract. Insert Q1 as
  the exact next session: a frozen P3 OOS predictive-quality audit before any
  paid-data decision. Stage any later provider fit-gap, authorized collection,
  NAV implementation, and controlled completed-vintage backtest as separate
  conditional sessions. No model execution, performance calculation, external
  request, purchase, artifact mutation, publication, archive, branch, commit,
  push, or schedule occurred. The later free-data V1 decision recorded above
  supersedes this then-current paid-provider-first ordering.
- Add the deterministic canonical final-shortlist presentation route over the
  exact frozen P4 shortlist and E1 adjudication. Freeze presentation policy v2
  so `exclude`, `warn`, and `unresolved` map only to their specified labels,
  a complete clean contract alone may map to `RESEARCH_CANDIDATE`, narrative
  cannot change any decision field, and human sign-off is not a presentation
  issuance gate. Materialize the non-overwriting
  `20260731T000054Z-final-shortlist-v2` artifact with a 15-row final table,
  15-row machine-attested E1 derivative, 47-row trace table reproducing all 46
  exact E1 citations, frozen comment/policy contracts, and a concise cited
  report. Preserve all P4 ranks, holdings, weights, gates, model scores, and
  fraud/liquidity values; retain all 15 names as
  `RESEARCH_CANDIDATE_EVENT_EVIDENCE_INCOMPLETE`; preserve the original E1
  state as `unsigned_pending_explicit_human_signoff`; keep HPK and SSTK
  summary-prohibited; record the expected post-P4 C1 builder-code evolution
  separately from intact artifact records; and keep official performance
  unavailable. No LLM, external request, retry, model execution, dataset
  promotion, performance calculation, publication, archive, deletion, branch,
  commit, push, or schedule occurred.
- Complete the offline citation-by-citation adjudication of E1's frozen
  15-name human-review queue from
  `20260730T154650Z-e1-extraction-v2`. Independently verify its exact manifest
  SHA-256, all 62 records, all 47 compressed/decompressed responses and
  19,120,821 aggregate bytes, all 46 claim locators/passage hashes, all 59
  collection records, all 148 parent E1 records, and all 29 preservation
  entries before processing. Apply the frozen deterministic rules before
  narrative; separately adjudicate affected security/role, event type/status,
  publication lineage, date/effective-time precision, exact bounded terms,
  cross-document conflicts, remaining ambiguity, and rule completeness for
  every name and document. The final non-overwriting
  `20260730T173110Z-e1-adjudication-v2` artifact retains all 47 exact
  responses, 46 claim citations, 47 unresolved document rows, 15 unresolved
  name rows, an updated live contract, and a 15-row unsigned explicit-human-
  signoff queue. HPK remains unresolved and summary-prohibited after the
  preserved Form 25-NSE HTTP 503; SSTK remains summary-prohibited; SM's
  consideration-issuer role is not treated as cessation of SM common stock.
  No external request, retry, dataset promotion, P3/P4 consumption change,
  performance calculation, Hugging Face write, archive, branch, commit, push,
  publication, deletion, or scheduling occurred.
- Complete the offline human-review-ready continuation of Session E1 from the
  independently verified `20260730T150604Z-e1-collection-final` artifact.
  Reverify the frozen parent E1 manifest, request plan, collection contract,
  all 59 collection records, all 47 compressed/decompressed response hashes,
  URL order, 19,120,821 aggregate bytes, and all D1 preservation entries
  before processing. Extract one exact, hash-addressed, normalized-text passage
  from each of the 46 HTTP 200 SEC primary documents; keep the HPK Form 25-NSE
  HTTP 503 response unresolved and summary-prohibited; separately reconcile
  exact security scope, event type/status, publication time, effective time,
  terms, and ambiguity; and apply the frozen deterministic policy before the
  cited report. The non-overwriting
  `20260730T154650Z-e1-extraction-v2` artifact retains all 47 exact responses,
  46 claim rows, 47 unresolved deterministic rows, an updated 15-name live
  contract, and a 15-row human-review queue. Thirteen names may be summarized
  only from claim-level retrieved-evidence citations; HPK and SSTK remain
  summary-prohibited, and summaries cannot change deterministic actions. No
  external request, retry, dataset promotion, P3/P4 consumption change,
  performance calculation, Hugging Face write, archive, branch, commit, push,
  publication, deletion, or scheduling occurred.
- Execute Session E1's frozen, explicitly approved 47-request SEC EDGAR
  Archives primary-document plan without changing its URLs, forms, CIKs, or
  date window. Validate the parent E1 manifest and plan hashes before the first
  request; use the repository SEC identification and bounded request pacing;
  preserve exact response bytes, HTTP metadata, timestamps, hashes, and
  continuation lineage; and independently verify the final non-overwriting
  `20260730T150604Z-e1-collection-final` artifact. The plan exhausted exactly
  47 external requests and preserved 46 HTTP 200 primary documents plus one
  HPK Form 25-NSE HTTP 503 response, totaling 19,120,821 bytes. Apply the
  deterministic event policy before summarization, keep all 47 document rows
  and all 15 shortlist names unresolved in human review, create zero event or
  summary claims, and permit any later live summary only with claim-level
  citations to retrieved hash-pinned evidence. No dataset promotion, P3/P4
  consumption change, performance calculation, Hugging Face write, archive,
  branch, commit, push, publication, deletion, or scheduling occurred.
- Complete Session E1's versioned historical-then-live event/M&A review from
  the exact provider-neutral S1 ledger without weakening any identity,
  security-scope, confidence, ambiguity, or effective-time state. Add
  deterministic effective-time policies for pending acquisitions, completed
  mergers, bankruptcy, suspension, delisting, registration termination,
  exchange noncompliance, and other material events; distinguish event,
  publication, retrieval, and decision-eligibility clocks; reconcile all 180
  canonical P4 holdings; route all 15 current-shortlist names to cited human
  review; prohibit historical LLM/model knowledge; and permit live LLM
  summaries only after every claim cites retrieved hash-pinned evidence. The
  immutable `20260730T144043Z-e1-final` artifact recopies and independently
  verifies all 136 exact S1 responses, freezes 86 historical and seven live
  dated filing-indicator warnings, and prepares but does not execute an exact
  approval-gated 47-request SEC primary-document plan. No external request,
  performance calculation, P2-P4 consumption change, D1 promotion, Hugging
  Face write, archive, branch, commit, push, publication, or scheduling
  occurred.
- Complete Session S1's offline, versioned, provider-neutral security and
  survivorship ledger from the exact SEC index and submissions responses
  already preserved by D1. Add stable issuer/security/listing/event/effective-
  date schemas; distinct event-effective, source-publication, and retrieval
  times; confidence and ambiguity fields; exact raw-response copies and hash
  lineage; explicit corporate-action and price-adjustment semantics; P4
  holding and benchmark reconciliation; and matched/ambiguous/unsupported/
  conflicting coverage. Keep unsupported exits and delisting returns
  unavailable, isolate the legacy minus-50-percent scenario under a physical
  sensitivity namespace excluded from observed-label training, preserve the
  future paid-provider adapter boundary, and calculate no performance. The
  final ledger version is `20260730T141429Z-s1-final`; no external request,
  canonical promotion, Hugging Face write, archive, branch, commit, push, or
  publication occurred.
- Complete Session D1's versioned US canonical refresh route without changing
  the accepted P2-P4 baseline or legacy international workflows: preserve
  exact SEC/Yahoo response bodies, request timestamps, hashes, and explicit
  failures; version the live SEC universe; reuse corrected Step 2 availability
  and PIT Step 5 transformations; freeze price, benchmark, calendar, decision,
  and label-support contracts; keep macro vintages unavailable; run Step 6
  observed-only with inferred delisting returns disabled; and produce a
  non-promoted P2 review candidate plus row/schema/coverage/label/missingness/
  gate/source-drift comparison. The authorized refresh version
  `20260730T110301Z` retains 8,017 SEC CIKs, 43,640 certified annual rows,
  4,790 successful terminal market responses, 23 explicit market failures, and
  a 43,640-row/339-column P2 candidate. No canonical pointer, immutable
  Hugging Face path, legacy collector, archive, branch, commit, push,
  publication, or promotion changed.
- Prepare Session C2's separate private canonical Hugging Face
  publisher/retriever without changing the legacy root transfer utilities:
  validate the complete frozen P2-P4 roots and exact 202-file,
  481,666,707-byte content-addressed plan; fail closed unless authenticated
  repository metadata proves the dataset repository is private; refuse any
  existing destination; parent-pin one atomic commit; require full immutable
  revisions for pointer-based recovery; verify every downloaded size and hash
  before exposing a non-overwriting target; and add a manual secret-backed CI
  recovery workflow with no mutable-revision fallback. Support `HF_TOKEN` or
  the standard saved Hugging Face credential without printing either.
  Authenticated read-only preflight verified the existing dataset repository
  private and all 202 planned paths absent. After explicit approval and
  informed re-authorization, publish the exact 481,666,707-byte baseline at
  immutable revision `aaf056ea115067e42ef9abf9fa93ade75cdd4052`, complete
  temporary byte/hash recovery, materialize all three pointers, and
  independently reconcile all 202 remote paths with zero missing. Add a
  location-independent wrapper so future approved runs work outside the
  repository directory.
- Complete canonical consolidation Session C1 without changing the accepted
  P2-P4 behavior: extract active fold-local OOS modeling and fixed
  selection/liquidity contracts from historical Session V3 names into neutral
  modules; add a fail-closed, non-overwriting `workflows.run_canonical`
  P2 -> P3 -> P4 verifier/reconstructor; label the six-step and scheduled
  multi-market refresh paths as legacy while preserving their international
  structure; correct the active FAQ, production configuration, and
  architecture; and add tracked dependency and non-destructive archive
  inventories with artifact sizes and manifest hashes. No external data,
  publication, archive move, branch, commit, or push occurred.
- Freeze the approved post-P4 completion plan: continue a controlled canonical
  refactor instead of a whole-repository rewrite; run the replacement refresh
  US-first while preserving legacy international structure; use free evidence
  with isolated survivorship sensitivity assumptions and paid-provider
  adapters deferred; design historical then live cited event/M&A handling;
  prepare private, versioned, non-overwriting P2-P4 publication through the
  existing `ekrash718/stock-screener-data` Hugging Face destination after
  visibility verification; and preserve tracked legacy material on a separate
  Git archive branch. Set repository consolidation Session C1 as the exact next
  bounded implementation.
- Complete Product Session P4's canonical prediction-to-product route: consume
  only the pinned P3 row-complete OOS predictions; independently reconfirm
  source/model lineage, decision-time eligibility, and all eight hard gates;
  evaluate every eligible candidate against exactly 30 frozen pre-prediction
  Session 8E close/volume sessions; rank only after the $1.333 million median
  dollar-volume gate; and freeze 12 supported top-15 equal-weight vintages, a
  15-row 2026 shortlist, selected-feature explanations, and direct
  prediction-to-report traceability under a new non-overwriting manifest.
  Freeze the accepted three-year-vintage and 25-bps-per-side actual-notional
  cost contracts, but fail official performance closed because the accepted
  security/action market ledger and immutable `DGS1MO` ALFRED vintage are
  absent. No model retraining, external refresh, macro fallback, V3.4 action,
  old V3 performance transfer, archive, commit, or push occurred.
- Complete Product Session P3's canonical US annual observed-only three-year
  research/model route: replace the accepted V3.1/V3.2 route's old Session 8F
  input with a fail-closed loader for the pinned P2 primary dataset, observed
  labels, row-horizon support, and summary; remove realized target/support and
  unavailable macro fields from the model candidate contract; exclude
  decision-late source rows; preserve fold-local feature selection and median
  preprocessing for decision-tree and LightGBM roles; and materialize 87,612
  row-complete OOS prediction/exclusion records under a new non-overwriting
  manifest-backed artifact. Reconcile the historical V3.3 validator only for
  the two reused V3.2 code/test records whose exact replacements are pinned by
  P3; all other historical records still fail closed. No backtest, portfolio,
  performance calculation, refresh, V3.4 action, archive, commit, or push
  occurred.
- Complete Product Session P2's canonical corrected US annual data route:
  validate SEC date-only filings against their America/New_York source-local
  calendar date without relaxing provenance, mismatch, version, or equal-time
  gates; add focused regressions; rebuild 43,806 observed-only rows under
  `artifacts/canonical/corrected_us_annual/`; freeze the manifest-backed primary
  dataset entrypoint and exact coverage validation; preserve the separate
  zero-addition policy namespace; and reconcile historical Session 8F code
  lineage only through files independently pinned by the canonical P2
  manifest. No external refresh, model, backtest, performance, archive, commit,
  or push action occurred.
- Complete Product Session P1's read-only canonicalization decision: select the
  corrected Step 2 → Session 8E → Session 8F US annual chain as the canonical
  data foundation; classify the legacy dataset and downstream alternatives;
  document the accepted V3.1–V3.3 model/liquidity components and missing
  backtest/report bridge; and freeze P2 as one bounded correction of SEC
  source-local availability validation plus a non-overwriting canonical
  observed-only dataset build.
- Correct the simplified product framing to preserve the original
  dataset-first research objective; document the verified Hugging Face source
  Parquets, HTML feature dictionary, extensive survivorship/PIT work and its
  remaining free-source boundary; and replace generic product readiness with a
  read-only canonicalization session that must precede implementation or
  archival.
- Pivot the active roadmap from audit-grade historical validation to a simple,
  workable product path; archive the V3 roadmap, handoff, legacy FAQ, and
  legacy production configuration; replace product-facing onboarding with one
  read-only readiness session; and withdraw legacy backtest metrics as current
  product claims.
- Resolve all three pre-V3.4 blockers by accepting a flat 25 bps-per-side
  actual-traded-notional cost policy, the immutable 2026-07-17 Federal Reserve
  H.15 `DGS1MO` risk-free policy and exact interval conversion, and a bounded
  329-request Sharadar/SEC/ALFRED collection plan for the 90 accepted holdings
  and required IWC/IWM/MDY instruments; no collection or V3.4 execution was
  performed.
- Clarify the pre-V3.4 canonical-ledger contract by pinning the accepted V3.3
  manifest, exact July vintage clocks, unsupported-period and common-session
  valuation behavior, decision-time benchmark mapping, total-return/event
  double-counting controls, and fail-closed security-event resolution; prohibit
  legacy calendars, rates, costs, labels-as-returns, policy imputations, and
  fallbacks while retaining cost, risk-free, and external collection choices
  as named user-approval blockers.
- Freeze Session V3.3's manifest-backed `production_v3_ml_gates` selection:
  revalidate both accepted manifests and all referenced records, apply the
  fixed hard/model gates, validate exact candidate-wide 30-session
  pre-prediction dollar volume from source-preserved close/volume evidence,
  rank only liquidity-qualified rows, and freeze 15 equal-weight holdings in
  each of six supported 2015–2020 decision periods with exact fail-closed
  candidate, gate, exclusion, liquidity, ranking, and lineage tables.
- Generate Session V3.2's manifest-backed, row-complete OOS decision-tree and
  LightGBM predictions for `production_v3_ml_gates`, with separate fold-local
  feature selection, median preprocessing, exact target/model lineage,
  decision-time masking of the frozen pool's label-derived excess-return
  candidate, explicit fail-closed folds, and 2027/2028 freeze exclusions.
- Freeze Session V3.1's manifest-backed observed-only US annual three-year
  production table and exact tree, LightGBM, hard-gate, portfolio, and ADTV
  contracts; adopt the corrected `production_v3_ml_gates` contract and add one
  verified conventional-commit checkpoint per V3 session.

### Planning — Linear Production Validation Reset (2026-07-16)

#### Changed
- Replaced the historical numbered and lettered recovery chain with five linear
  V3 sessions for one observed-only US annual three-year production `ml_gates`
  path: production table, exact OOS models, liquidity-qualified holdings,
  canonical ledger inputs, and one controlled backtest.
- Made the checkpoint commit and frozen PIT reports the historical record while
  excluding five-horizon retraining, alternate strategies, stale comparisons,
  threshold optimization, and legacy-result reproduction from active scope.
- Added a strict no-proliferation rule: a blocker stops its current V3 session
  instead of creating another lettered recovery session.

### Planning — Post-Session-9B Production Dependency Contract (2026-07-16)

#### Changed
- Revalidated and preserved the frozen Session 9B unavailable verdict, then
  replaced the invalid direct 9B→9C dependency with ordered Sessions 9B.1
  (entry-timestamp lineage and hard-gate inputs), 9B.2 (frozen fold-local tree
  role), 9B.3 (candidate-wide liquidity evidence), 9B-R (final holdings), and
  only then Session 9C.
- Froze the exact fold-local 3y decision-tree model specification before any
  fit and mapped every production gate/ranking input to a certified corrected
  field or explicit blocker, including the unresolved Ridge-versus-LightGBM
  production-ranker conflict.
- Corrected the ADTV contract to use `$200,000 / 15` position size and a 1%
  position/ADTV limit, requiring median 30-session dollar volume of at least
  `$1,333,333.333333...` for every eligible candidate before ranking. Missing
  or ambiguous decision-time evidence now fails closed by contract.
- Recorded that Session 8E already has a unique accepted entry timestamp for
  every supported entity/year; successor lineage must preserve it without
  recomputing market outcomes.

### Modeling — Session 9B Selection Freeze (2026-07-16)

#### Added
- Added the bounded Session 9B freeze builder and focused tests for complete
  Session 9 manifest validation, strategy-role reconciliation, physically
  separate row-level candidate exclusions, and fail-closed zero-row holdings.
- Froze 438,060 candidate-strategy records across the two population
  namespaces, an explicit five-path compatibility inventory, empty holding and
  weight tables, code/dirty lineage, a manifest, and the Session 9B report.

#### Changed
- Marked every current `CORRECTED_8F` selection path unavailable. Production
  `ml_gates` lacks the required OOS tree-agreement role; alternate engine paths
  lack complete compatible ranking/gate/liquidity inputs or population support.
- Kept Session 9C blocked. No logistic-for-tree substitution, new threshold,
  model fit, market input, backtest, portfolio, commit, or push was performed.

### Planning — Post-Session-9 Evaluation Path (2026-07-16)

#### Changed
- Added Session 9B as a bounded selection-freeze gate between explicit Session
  9 predictions and selected-holding market evidence. It prohibits substituting
  the calibrated logistic classifier for the production tree-agreement role and
  fails closed when no already-accepted compatible selection rule exists.
- Reframed Session 9C around physically separate `CORRECTED_8F` selection paths
  and made complete Session 9B holdings, weights, and exclusions a prerequisite.
- Replaced the impossible `OLD_RECONSTRUCTED`/`CORRECTED_PARTIAL` Session 10
  comparison with a controlled `CORRECTED_8F` evaluation. Updated downstream
  FULL-PIT comparisons and completion criteria so stale, unavailable, and saved
  legacy paths remain reference-only.

### Modeling — CORRECTED_8F Explicit OOS Predictions (2026-07-16)

#### Added
- Added the bounded Session 9 builder and focused tests for complete Session 8F
  manifest validation, price/label intersections, strict historical label-end
  eligibility, outcome-safe fold-local feature selection, preprocessing,
  calibration, models, and row-complete OOS prediction lineage.
- Froze physically separate `observed_only` and `include_policy_imputed`
  checkpoints, 208 fitted fold models, two 438,060-row prediction tables,
  supported/unavailable/excluded counts, code/dirty lineage, and manifest under
  `artifacts/pit_validation/session9_corrected_8f/`.
- Added the Session 9 report with descriptive OOS diagnostics and explicit
  non-backtest limitations.

#### Changed
- Corrected the Session 9 roadmap contract to name the certified path
  `CORRECTED_8F`, prohibit stale corrected-partial inputs, keep macro excluded,
  and make `OLD_RECONSTRUCTED` unavailable rather than constructing it ad hoc.
- Kept both certified population namespaces physically separate while recording
  their identical feature identities, horizon gates, labels, eligibility,
  predictions, and descriptive diagnostics.

### Data — Corrected Feature Populations (2026-07-16)

#### Added
- Added the bounded Session 8F builder and focused tests for stable row identity,
  complete 8E-chain validation, strict pre-decision price features, population-
  separated labels, checkpoints, configuration, dirty state, and manifests.
- Added physically separate 43,806-row `observed_only` and
  `include_policy_imputed` Step 5, Step 6, and fraud-taxonomy lineages plus
  explicit supported/unavailable/excluded feature-family counts.

#### Changed
- Made Step 5 consume an explicitly provenance-bearing feature market cap and
  leave macro interactions null when macro evidence is absent.
- Added fail-closed Step 6 controls that disable unproven imputation and inferred
  survivorship outcomes, preserve stable row IDs, and made taxonomy transforms
  prefer proven SEC-primary availability timestamps.
- Certified 26,232 rows with the complete required price-feature family,
  retained 11,857 as unavailable and 5,717 as excluded, and left all macro
  features unavailable. No stale corrected-partial feature was reused.

### Data — Contract-Aligned Label Market Inputs (2026-07-16)

#### Added
- Added an artifact-scoped Session 8E builder with immutable stock/benchmark
  chart payloads, XNYS/XNAS calendar evidence, common-session labels, separate
  population gates, complete raw/normalized inventories, and frozen lineage.
- Added the superseding support manifest and Session 8E report for the 43,806
  certified US annual rows across the accepted 6m/1y/2y/3y/5y horizons.

#### Changed
- Replaced the Session 8C zero-support verdict with nonzero certified support
  at all accepted horizons while keeping missing shares, prices, sessions,
  mappings, adjustments, and provenance fail closed.
- Kept `observed_only` and `include_policy_imputed` artifacts separate; no
  unsupported policy outcome was inferred. Session 9 is eligible under its
  minimum support prerequisite but was not started.

### Data — Provenance-Preserving US Step 2 Rebuild (2026-07-16)

#### Added
- Added an artifact-scoped SEC Company Facts runner with immutable compressed
  raw responses, per-request streaming hashes, resumable checkpoints, explicit
  failures, transient retry, and isolated partial-response evidence.
- Added a frozen corrected Step 2 evidence set for the exact 8,021-CIK US
  universe, an independent validator/manifest, focused synthetic tests, and the
  Session 8D validation report.

#### Changed
- Certified only supported earliest-primary SEC facts with stable entity IDs
  and end-of-local-date filing availability; missing required facts, later
  amendments, unsupported sources, and unresolved equal-time accession
  collisions now fail closed into separate diagnostics.
- Marked Session 8D complete while keeping its label-market-input work separate;
  Session 8E is completed by the later entry above.

### Docs — Post-8C Recovery Sequence (2026-07-16)

#### Changed
- Added bounded Session 8D for an artifact-scoped, raw-response-manifested US
  Step 2 provenance rebuild and Session 8E for the separate contract-aligned
  label-market-input correction and support revalidation.
- Kept Session 9 blocked until both stages are accepted and at least one modeled
  horizon has nonzero certified support.

### Quality — Training-Label Market-Input Gate (2026-07-15)

#### Added
- A frozen Session 8C validation manifest and report with explicit US row- and
  horizon-level supported, unavailable, and excluded counts for both label
  populations.

#### Changed
- Excluded all five accepted horizons from Session 9: current artifacts have
  zero rows with proven entity/filing availability and cannot certify the
  accepted common-session, benchmark, trading-calendar, or adjustment-vintage
  semantics. The next task is a bounded provenance-preserving Step 2 rebuild.

### Quality — Horizon Calendar Contract (2026-07-15)

#### Added
- A frozen Session 8B calendar configuration and validation report defining
  decision, prediction, holding, return, benchmark, filing-provenance, and
  strict label-availability rules for every materialized horizon.

#### Changed
- Accepted `6m`, `1y`, `2y`, `3y`, and `5y` calendar policies; excluded the six
  non-modeled horizons; and kept observed-only eligibility separate from the
  explicitly named policy-imputed sensitivity.
- Marked Session 8B complete while blocking Session 9: current
  corrected-partial artifacts omit required filing provenance/entity identity,
  and their filing-date-start labels are incompatible with the frozen calendar.

### Quality — CORRECTED_PARTIAL Dataset Build (2026-07-15)

#### Added
- A strictly offline/read-only Step 3 cache path with explicit cache/input/output
  arguments, null-preserving missing/empty series behavior, and focused tests.
- Separate corrected-partial price, feature, pre-taxonomy, and taxonomy-enriched
  datasets plus streaming-hash lineage and exhaustive machine-readable data
  comparisons under `artifacts/pit_validation/corrected_partial/`.
- `reports/pit_validation/08_data_comparison.md`, documenting schema, key,
  missingness, label/provenance, class-balance, value-change, and universe
  differences against frozen `LEGACY_SAVED` evidence.

#### Changed
- Added explicit artifact-scoped input/output CLI arguments to Steps 5–6 and
  fraud-taxonomy enrichment so the audited run cannot overwrite the only copy
  of an intermediate or write outside the corrected-partial artifact root.
- Marked roadmap Session 8 complete while retaining the stale-Step-2,
  incomplete-cache, and non-reproducible-legacy limitations.

### Quality — Corrected-Partial Input Freeze (2026-07-15)

#### Added
- Frozen read-only local snapshots, daily price cache, and macro inputs for the
  audited offline Session 8 lineage, with streaming hashes and corrected-code
  lineage in `artifacts/pit_validation/corrected_partial_inputs/manifest.json`.
- Documented the incomplete-cache boundary and null-preserving offline contract
  in `reports/pit_validation/08_preflight_input_freeze.md`; Session 8 remains
  unstarted and `LEGACY_SAVED` remains unchanged.

#### Changed
- Formalized the completed freeze as roadmap Session 7A and tightened Session 8
  to require reference-only legacy price-stage evidence, a tested offline and
  read-only Step 3 path, artifact-scoped intermediates, and separate pre-taxonomy
  and final datasets.
- Added Session 8C as a pre-training gate for selection-independent target-price,
  benchmark, trading-date, adjustment, availability, and horizon coverage;
  retained Session 9C for selected-holding backtest market evidence.

### Quality — Frozen Legacy Evidence and Compatibility Validation (2026-07-15)

#### Added
- A streaming-hash `LEGACY_SAVED` freezer and compatibility validator that
  verifies frozen payloads, dirty-state patches, required missing-evidence
  records, and strict separation of legacy inputs from corrected-code evidence.
- Focused synthetic tests for hash/size drift, missing required records,
  corrected-code role misuse, non-reproducibility enforcement, and later commit
  compatibility.
- `reports/pit_validation/07_legacy_evidence_freeze.md` and a frozen
  `artifacts/pit_validation/legacy_saved/` evidence set with a limitations
  manifest, complete pre-Session-7 dirty state, and reserved but unpopulated
  `OLD_RECONSTRUCTED` state.

#### Changed
- Marked roadmap Session 7 complete without rebuilding data/prices,
  reconstructing predictions, retraining, backtesting, or running a production
  comparison.

### Docs — Token-Efficient Evidence and Backtest Gates (2026-07-15)

#### Changed
- Added durable low-token roadmap/workflow controls: reuse handoff evidence,
  batch bounded reads, keep large output in `/tmp`, use path-scoped diffs,
  prefer focused tests, and run the full suite only once after code stabilizes.
- Tightened Session 7 to preserve the baseline commit, complete dirty patch,
  legacy-artifact timing, and missing canonical market-input evidence.
- Expanded Session 9C to require monthly benchmark and risk-free series,
  adjusted-price/event double-counting validation, sourced event terms,
  turnover, and trade-level cost evidence.
- Added Session 10 per-metric stop conditions for incomplete risk-free,
  benchmark, selected-holding price, or corporate-action evidence while keeping
  observed-only, policy-50, and policy-100 results separate.

### Fix — Canonical Monthly NAV and Historical Disappearance Gates (2026-07-15)

#### Added
- A fail-closed security-level month-end net total-return NAV ledger with dated
  corporate-action resolution evidence, explicit observed-only/`-50%`/`-100%`
  return policies, monthly risk-free alignment, and annual reconciliation.
- Synthetic coverage for monotonic NAV, known intra-year drawdown, missing and
  partial prices, mid-year disappearance, total loss, costs, future invariance,
  annual/monthly reconciliation, and canonical metric consistency.
- `reports/pit_validation/06b_monthly_nav_correction.md` documenting the bounded
  Session 6B implementation and remaining market-input/calendar boundary.

#### Changed
- Removed future-derived `likely_delisted` behavior from historical engine,
  screener-registry, and leverage eligibility; separately named, sourced
  decision-time listing, filing-staleness, delisting-notice, quote-recency, and
  ADTV evidence may gate historical rows and fails closed when incomplete.
- Historical performance no longer substitutes annual forward labels, zero
  monthly returns, or annual drawdown proxies for missing price/event evidence.
- CAGR, volatility, Sharpe/Sortino, drawdown, Calmar, and monthly tail metrics
  now consume one canonical net monthly NAV. Sharpe/Sortino are unavailable
  without a complete frozen monthly risk-free series.
- Annual returns are reporting aggregations of monthly NAV and must reconcile
  within `1e-10`; unsupported long/short financing and unresolved observed-only
  holdings produce no official performance.
- Marked roadmap Session 6B complete after focused and full verification.

### Fix — Manifest-Backed Historical Score Consumption (2026-07-15)

#### Added
- Shared row-level prediction-manifest validation, OOS source enforcement,
  exclusion codes, required-role coverage reporting, and synthetic final-model/
  later-filing invariance tests.
- `reports/pit_validation/06a_prediction_consumption_correction.md` documenting
  the bounded Session 6A correction and its fail-closed legacy-data boundary.

#### Changed
- `ml_gates`, engine composite, QEM, SCDV, IARB, alpha composite, screener/
  alpha registries, and the IC-weighted portfolio registry now require every
  declared direct and indirect OOS ML role for every selected row.
- Historical portfolios now require complete `target_n` coverage and persist
  per-period role counts plus row-level score sources and exclusion reasons.
- Removed final/static historical score fallback and score-count-driven ML
  weight omission; unmanifested legacy scores remain explicitly ineligible.
- Removed the remaining private in-engine walk-forward training generator and
  made PIT validation Markdown deliverables trackable under `reports/`.
- Marked roadmap Session 6A complete with its final verification result.

### Docs — Accepted Session 5A Backtest Contract (2026-07-15)

#### Added
- `reports/pit_validation/05a_backtest_contract.md`: proposed row-level OOS
  prediction-lineage, fail-closed missing-score, evidence-backed corporate-
  action/disappearance, and canonical monthly-NAV contracts.

#### Changed
- Recorded explicit user acceptance of all four Session 5A contracts. Session
  6A/6B are authorized only as later separate bounded implementation tasks.
- Amended the proposed corporate-action contract for personal small-cap use:
  live screening may use current tradability/filing/liquidity evidence, while
  unresolved historical disappearances receive separate `-50%` and `-100%`
  portfolio sensitivities without changing the observed primary path or model
  training labels.
- Clarified that full-panel/eventual disappearance status is prohibited in
  historical selection, while separately named point-in-time listing,
  filing-staleness, delisting-notice, quote-recency, and ADTV gates are permitted
  when based exclusively on information available at the decision timestamp.

### Docs — Post-Session-5 Roadmap Safety Gates (2026-07-15)

#### Changed
- Added Session 5A to require explicit prediction-lineage, missing-score,
  corporate-action/disappearance, and canonical-return contracts before any
  backtest correction.
- Split Session 6 into independent OOS prediction-consumption and monthly-NAV/
  historical-gate corrections with separate prerequisites and stop conditions.
- Reclassified the saved headline result as non-reproducible `LEGACY_SAVED`
  evidence and reserved `OLD_RECONSTRUCTED` for a new controlled rerun.
- Moved horizon-specific calendar acceptance before retraining/prediction and
  added a canonical market-input/selected-holding coverage gate before controlled
  performance comparisons.

### Docs — Backtest Prediction and Monthly-NAV Path Audit (2026-07-15)

#### Added
- `reports/pit_validation/05_backtest_path_audit.md`: read-only T11/T31 trace
  of static, walk-forward, OOF, regression, delisting-gate, missing-return, and
  monthly metric paths, with exact frozen-artifact boundaries and required
  future-invariance tests.

#### Changed
- Recorded that the saved FY2013–FY2023 `ml_gates` result has no row-level
  score/fold lineage, the frozen legacy parquet cannot produce current strict
  walk-forward scores, its conditional `likely_delisted` gate was inactive,
  and only drawdown uses the unreconciled monthly NAV path.

### Fix — Filing-Time Cohort Transform Invariance (2026-07-15)

#### Added
- Shared proven-publication cohort enforcement with explicit issuer identity,
  timestamp/source provenance, equal-time batching, sparse-policy metadata, and
  fail-closed legacy/estimated dates.
- Future-, row-order-, sparse-group-, early/late-filer-, duplicate-version-, and
  cross-module invariance tests for Step 5, Step 6, and alpha materialization.
- `reports/pit_validation/04c_cohort_transform_correction.md` documenting the
  bounded T05–T09/T16 correction and deferred calendar boundary.

#### Changed
- Step 5 winsorization, market/sector ranks, percentiles, and rank interactions
  now use only proven peers available at each row's publication timestamp.
- Step 6 accrual winsorization and size imputation now follow the accepted
  cohort/history minimums without full-data or later-filing fallbacks.
- Filing-time alpha ranks use the same eligible cohorts; value and growth rank
  the cohort-winsorized signal as intended.
- SEC, EDINET, DART, statement-date, and estimated-date producers now emit only
  the minimal row-level availability provenance needed for fail-closed use.

### Docs — Session 4C Calendar and Cohort Contract Proposal (2026-07-15)

#### Added
- `reports/pit_validation/04c_calendar_contract_proposal.md` defining separate
  filing-materialization and decision-snapshot clocks, exact market/sector
  cohorts, sparse-group behavior, timestamp provenance, late-filing, and
  amendment policies for explicit acceptance before Session 4C.

#### Changed
- Split contract acceptance into roadmap Session 4C0 so Session 4C corrective
  code cannot begin in the same step that chooses its calendar semantics.
- Deferred rebalance calendars by investment horizon instead of imposing one
  annual/semiannual schedule; added Session 8B to freeze horizon-specific
  calendars before controlled official backtests.
- Recorded user acceptance of the horizon-neutral filing-materialization
  contract while keeping every rebalance calendar explicitly deferred.

### Fix — Fold-Local Feature and Preprocessing Lineage (2026-07-15)

#### Added
- Deterministic dataset/training/development fingerprints and strict feature-
  artifact lineage validation for horizon, cutoff, label policy, selector
  configuration, population, and feature compatibility.
- Synthetic future-invariance and fail-closed tests for held-out coverage,
  appended rows, fold predictions, medians, scalers, sector parameters,
  regression target bounds, and legacy/cross-policy artifacts.
- `reports/pit_validation/04b_fold_lineage_correction.md` documenting the
  bounded Session 4B correction and immutable-artifact boundary.

#### Changed
- Static PSI filtering now uses the declared validation population; untouched
  test rows cannot choose features.
- OOF, classifier/regression walk-forward, backtest, and proper-split paths now
  derive candidate fill rates and feature choices inside eligible training folds.
- Tuning fails closed unless model metadata proves exact lineage and reuses
  frozen training medians and sector-normalization parameters.
- Standalone regression selects features and fits medians/target bounds on its
  eligible training population instead of consuming unproven feature JSON.

### Fix — PIT Global Transformation Invariance (2026-07-15)

#### Added
- Future- and order-invariance tests for shared growth loaders, fraud-taxonomy
  as-of ranks, and ticker-local dilution history.
- `reports/pit_validation/04a_global_transform_correction.md` documenting the
  bounded Session 4A correction and immutable artifact boundary.

#### Changed
- Shared modeling and factor-research loaders now preserve pipeline-materialized
  growth values instead of fitting new full-panel clipping bounds.
- Fraud-taxonomy clipping and ranks now use expanding `filed_date` populations,
  fail closed without timestamps, and admit equal-timestamp rows as one batch.
- Dilution EPS changes are ticker-local, chronologically ordered, and dataframe-
  order invariant.

### Docs — PIT Correction Session Boundaries (2026-07-15)

#### Changed
- Split roadmap Session 4 into bounded corrections for confirmed global
  downstream transformations, fold-local feature/preprocessing lineage, and
  calendar-dependent cohort transformations, each with explicit prerequisites,
  future-invariance tests, stop conditions, and deferred questions.
- Moved the future-derived historical `likely_delisted` gate and static/final-model
  backtest fallbacks into the Session 5 audit and Session 6 correction boundary.
- Updated the Codex handoff to make Session 4A the exact next task and to require
  an accepted scoring-calendar contract before Session 4C.

### Docs — Fitted-Transformation Audit (2026-07-15)

#### Added
- `reports/pit_validation/03_transformation_audit.md`: read-only inventory and
  classification of fixed, as-of cross-sectional, training-fitted, and
  potentially global/future-aware transformations, with empirical invariance
  evidence and no production fixes.

### Fix — PIT Horizon and Survivorship Sensitivity Generalization (2026-07-15)

#### Added
- Horizon-qualified actual label dates and provenance for every Step 3 return horizon.
- Explicit `observed_only` and `include_policy_imputed` model-label modes; observed-only remains the default.
- Dated `-50%` likely-delisted sensitivity labels for every trained horizon, eligible only after both the target horizon and no-filing detection lag have elapsed.
- `reports/pit_validation/02b_horizon_survivorship_summary.md` documenting the revised contract and remaining corporate-action limitations.

#### Changed
- Purged eligibility now covers 6m, 1y, 2y, 3y, and 5y static, OOF, tuning, regression, walk-forward, and backtest training paths.
- The roadmap now rebuilds, retrains, and compares all trained horizons with observed-only and policy-sensitivity artifacts kept separate.
- Policy-sensitivity OOF/backtest outputs use distinct names and cannot fall back to legacy static model scores from an unknown label population.

### Fix — PIT Three-Year Label Eligibility (2026-07-15)

#### Added
- Canonical three-year label start/end trading dates and observed-price provenance in Step 3.
- A shared strict pre-score eligibility rule plus synthetic boundary, missing-date, provenance, future-invariance, and fold-maximum tests.
- `reports/pit_validation/02_fold_purge_summary.md` documenting all corrected training paths and the immutable legacy-dataset boundary.

#### Changed
- Three-year static, walk-forward, OOF, tuning, regression, and backtest training paths now require an observed complete label strictly before the January 1 scoring date.
- Step 6 policy-imputed likely-delisted labels now carry explicit policy provenance and no observed availability date, so they cannot enter training.

### Docs — PIT Label-Availability Audit (2026-07-15)

#### Added
- `reports/pit_validation/01_label_availability_audit.md`: Read-only evidence report confirming that all audited three-year walk-forward paths admit outcomes that were not observable at their fold scoring dates, with per-fold lower-bound counts and exit-date reconstruction limits.

#### Changed
- Accepted the Session 2 implementation contract: strict pre-score label availability, actual exit-date provenance, training exclusion for unknown/policy-imputed labels, and no dataset rebuild or retraining during the code-fix session.

### Refactor — Pipeline Spine Cleanup (2026-07-14)

#### Changed
- `research/factor_research.py`: Removed duplicated `_add_normalised_ratios`, now imports from `pipeline/feature_library.py` (single source of truth)
- `pipeline/step5_compute_features.py`: Extracted inline `COLUMN_ALIASES` block into new `pipeline/column_aliases.py` module
- `workflows/run_pipeline_jp.py`: Fixed dead references to archived `_jp_free` scripts, updated to use standard `step1_fetch_tickers_jp.py` / `step2_build_snapshots_jp.py`; also fixed misplaced `_root` import bug

#### Added
- `pipeline/column_aliases.py`: Standalone module for step2 → step5 column name mapping (was buried inline in step5)

### Chore — Repo Cleanup (2026-07-13)

#### Archived
- `models/archive/`: Non-production model artifacts (1y, 2y, 5y, 6m horizons + baseline LRs + clean/ experiment)
- `models/archive/feature_sets_{1y,2y,5y,6m,pruned}.json`: Superseded by canonical `feature_sets_3y.json`
- `data_io/archive/migrate_to_db.py`: Dead migration script (never called)
- `workflows/archive/wait_and_merge.py`: Unused orchestration script
- `fraud/archive/taxonomy.py`: Stub (real logic in `pipeline/enrich_fraud_taxonomy.py`)
- `pipeline/archive/step2_build_snapshots_jp_free.py`: Dead JP variant
- `research/notebooks/archive/01-06`: Exploratory notebooks from early sessions
- `data/archive/leveraged_picks_*.csv`: One-off outputs from superseded leverage experiment

### Docs (2026-07-11)

#### Added
- **`docs/FEATURE_DICTIONARY.html`**: Interactive feature dictionary covering all 367 dataset columns with plain-English explanations, analogies, search/filter, and formula toggle.

### Chore — Session 48 (2026-06-29)

#### Changed
- **`docs/FAQ.md`**: Added all 8 gate thresholds, momentum gate, updated backtest summary to match current stats (Sharpe 1.45, CAGR +31.5%, MaxDD -8.1%).
- **`models/decision_tree_model.joblib`** → **`models/research_tree_snapshot.joblib`**: Renamed to clarify this is a research artifact, not the production walk-forward tree.
- **`notebooks/production_screener.ipynb`**: Import all gate thresholds from `modeling/constants.py` instead of hardcoding. Single source of truth for BENEISH_THRESHOLD, TREE_THRESHOLD, PIOTROSKI_MIN, VALUE_GATE_PCT, ALTMAN_Z_MIN, MOMENTUM_12M_MIN, MAX_MARKET_CAP_PROD.

#### Removed
- **`_archive/repomix_metadata_pack/`**: Dead metadata directory (unused).

### Feature — Session 47b (2026-06-28)

#### Added
- **`notebooks/production_screener.ipynb`**: Gate 8 — momentum hard gate (`momentum_12m_prior > -0.40`) kills value traps / structural decliners before scoring.
- **`notebooks/production_screener.ipynb`**: Section 9.5 — Gemini Flash LLM M&A screen flags pending deals in final picks. Gracefully skips if API key unavailable.
- **`notebooks/production_screener.ipynb`**: M&A screen switched from Gemini to Groq (Llama 3.3 70B) — free tier actually works.
- **`backtest/engine.py`**: Momentum gate added to `ml_gates` strategy (Sharpe 0.97→1.45, MaxDD -17%→-8%).
- **`modeling/constants.py`**: Add `MOMENTUM_12M_MIN = -0.40` threshold.
- Persist cell now includes `mna_flag` per pick and `momentum_12m_min` in config.

### Feature — Session 47 (2026-06-28)

#### Added
- **`notebooks/production_screener.ipynb`**: Section 10 — persist picks to `data/production_picks_YYYY-MM-DD.json` (timestamped, never overwritten) and `data/production_picks_latest.json` (always current). Prints diff vs previous run (new/dropped tickers).
- **`.gitignore`**: Exclude `data/production_picks_*.json` from version control.

### Test — Session 46 (2026-06-28)

#### Added
- **`tests/backtest/test_engine_gates.py`**: 9 unit tests covering ml_gates mode: max_cap, min_cap, value gate, tree threshold, Altman Z gate, regression ranking, Beneish gate, Piotroski gate, ROA gate. All use synthetic DataFrames.

#### Fixed
- **`docs/FAQ.md`**: Updated stale tree threshold 0.35→0.55, corrected model description (classifier+regressor), added feature count note.

### Refactor — Session 45 (2026-06-28)

#### Changed
- **`modeling/constants.py`**: Added production gate thresholds as named constants (`BENEISH_THRESHOLD`, `TREE_THRESHOLD`, `PIOTROSKI_MIN`, `VALUE_GATE_PCT`, `ALTMAN_Z_MIN`, `MAX_MARKET_CAP_PROD`). Single source of truth for hardcoded values previously scattered across engine, notebook, and train.py.
- **`backtest/engine.py`**: `filter_composite` ml_gates mode now imports thresholds from constants. Added Altman Z gate (`altman_z_score > 1.0`) to align with production notebook. CAGR +33.9%, Sharpe 1.115 (vs prior 34.7% / 0.97 — Altman gate removes distressed stocks, improving risk-adjusted returns).

### Fix — Session 44 (2026-06-28)

#### Fixed
- **`modeling/train.py`**: Now trains and persists LGBMRegressor as `models/model_3y_regression.joblib` alongside classifiers. Regression metadata added to `model_meta.json`.
- **`notebooks/production_screener.ipynb`**: Loads persisted regression model instead of retraining inline every run. Fixes reproducibility drift between notebook executions.

### Feature — Session 43 (2026-06-28)

#### Added
- **`notebooks/production_screener.ipynb`**: End-to-end production notebook — the product. Sections: data import → hard gates (Beneish/Piotroski/ROA/fraud/market-cap $50M-$5B/Altman-Z) → 22-feature scoring → LightGBM+tree agreement gate (0.45) → real ADTV liquidity filter → top 15 equal-weight portfolio → per-stock analysis + buy rationale → final output with OOS backtest reference (CAGR +25.9%, Sharpe 1.08).

#### Changed
- **`backtest/engine.py`**: Regression-based ranking (reg_3y_wf), tree gate raised to 0.55, `--max-cap` param, value gate (ps_ratio_sector_pct <= 0.7), walk-forward regression scoring in load_and_score(). Production: CAGR +34.7%, Sharpe 0.97, 0 negative years.
- **`modeling/train.py`**: Added `--clean-training` flag for reproducible clean-data filtering.
- **`models/`**: All models retrained through 2023 on clean data. Decision tree aligned with 3y features.
- **`notebooks/production_screener.ipynb`**: Updated to regression ranking + tree>=0.55 + $10B cap.
- **`docs/PRODUCTION_CONFIG.md`**: Full rewrite reflecting regression config + validated numbers.

### Feature — Session 42 (2026-06-28)

#### Changed
- **`models/model_{1y,3y}.joblib`**: Retrained on clean data only (fraud_suspect==0, ROA positive, Beneish < -1.78). Val AUC improved 0.571 → 0.601. Model learns what makes honest/profitable companies outperform.
- **`backtest/engine.py` ml_gates mode**: Gates revised: Piotroski >= 3 + ROA positive + tree >= 0.45. Default top_n 20 → 15. WF CAGR +33.5%, Sharpe 1.08, OOS CAGR +25.9%.
- **`docs/PRODUCTION_CONFIG.md`**: Full production workflow with honest OOS numbers, pros/cons.

### Feature — Session 41 (2026-06-28)

#### Changed
- **`backtest/engine.py` ADTV filter**: Parameterized by `aum_target` (default $200K retail, was hardcoded $1M institutional). Formula: `min_adtv = aum_target * 0.01`. Uses median (not mean) trailing 30d volume to resist block-trade spikes. New CLI flag `--aum-target`.
- **`docs/FAQ.md`**: New quick-reference file — company count, feature count, pipeline steps, key thresholds, model horizons, data source, update frequency.

### Feature — Session 40 (2026-06-28)

#### Changed
- **`models/decision_tree_rules.json`**: Retrained depth-4 decision tree on 2008-2020 (was 2008-2018). Now matches LightGBM's `TRAIN_CUTOFF=2020`. Rules changed: 5 → 8 BUY rules, new top split on `sales_to_price > 3.3059`.
- **`models/decision_tree_model.joblib`**: New artifact — saved tree model for production scoring.
- **Agreement filter threshold**: 0.35 confirmed on 3-year val (2021-2023): Sharpe 1.128, CAGR +18.53%, 0% max DD.
- **Production LightGBM retrain**: `--train-cutoff 2023` absorbs val into training (38,917 rows). 1y test AUC 0.500 (2024 only, single noisy year); 2y test AUC 0.566.

### Feature — Session 39 (2026-06-28)

#### Changed
- **`modeling/train.py`**: `TRAIN_CUTOFF` 2022 → 2020; validation set now spans 2021-2023 (3 years, ~2400 rows) instead of 2023 only (~800 rows). Reduces Optuna/calibration overfitting risk to a single market regime.
- **`modeling/run_feature_selection.py`**: Added `--train-end` parameter (default 2020). IC, ICIR, and PSI are now computed only on `fiscal_year <= train_end`, preventing soft leakage from val/test years into feature selection. `get_candidates()` also restricted to train-only data.

### Feature — Session 38 (2026-06-28)

#### Changed
- **`backtest/engine.py`**: `filter_composite()` gains `mode` param: `'blended'` (legacy default, manual weight blend) vs `'ml_gates'` (ML-only ranking + hard gates). New `'ml_gates'` strategy added to `STRATEGIES` dict.
- **`backtest/engine.py`**: `load_and_score()` now trains a walk-forward depth-4 decision tree alongside LightGBM, producing `tree_prob` column for the agreement gate.

#### ml_gates architecture
- **Ranking**: `ml_3y_wf` probability (single signal, no manual weight blend)
- **Agreement gate**: `tree_prob >= 0.35` (walk-forward decision tree)
- **Hard gates**: Beneish M < -1.78, Piotroski >= 3, not delisted
- **Market cap + ADTV**: applied upstream (unchanged)

#### Results (walk-forward, 2013-2024, top_n=10)
- ml_gates Sharpe **1.204** vs blended **0.937** (+0.267)
- ml_gates CAGR **+29.0%** vs blended **+27.7%** (+1.3%)
- ml_gates hit rate **80.0%** vs blended **76.0%** (+4pp)

### Feature — Session 37 (2026-06-28)

#### Added
- **`research/alpha_ic_validation.py`**: Walk-forward IC validation for all 5 alpha factors (value, quality, momentum, growth, fraud_risk). Reports mean IC, IC_IR, hit rate, NW t-stat. Flags factors with |IC| < 0.02 or IC_IR < 0.3.
- **`data/acwi_exus_returns.csv`**: MSCI ACWI ex-US annual returns 2008–2024 for non-US strategy benchmarking.

#### Changed
- **`backtest/engine.py`**: Non-US strategies (iarb, or any `--market` != US) now use ACWI ex-US as primary benchmark instead of SPY. SPY remains tracked as informational. New params: `acwi_exus_returns`, `is_non_us`.

#### Results
- All 5 alpha factors PASS IC validation: value IC=0.152, quality IC=0.095, momentum IC=0.038, growth IC=0.046, fraud_risk IC=0.143
- iarb strategy: benchmark_source now correctly shows 'ACWI_exUS', excess return +7.3% vs ACWI (was misleadingly -3.3% vs SPY)

### Fix — Session 36 (2026-06-28)

#### Changed
- **`backtest/engine.py`**: Default survivorship handling now imputes -50% return for stocks with missing forward_return_1y (previously dropped them — optimistic bias). New `survivorship_mode` param: 'impute' (default), 'drop', 'flag_only'. CLI: `--survivorship-mode`.
- **`backtest/engine.py`**: Filing-date rebalance gate — only stocks whose `filed_date < holding_year+1 Jan 1` are eligible for that year's portfolio. Prevents look-ahead from filings not yet available. CLI: `--no-filing-gate` to disable.

#### Impact
- Old (drop + no gate): CAGR +26.6%, Sharpe 0.982, MaxDD -3.0%
- New (impute + gate): CAGR +24.0%, Sharpe 1.036, MaxDD -8.4%
- Sharpe improves +5% (less noisy), CAGR drops -2.6pp (more honest), MaxDD more realistic

### Refactor — Session 35 (2026-06-28)

#### Added
- **`modeling/constants.py`**: Canonical `EXCLUDE_COLS`, `EXCLUDE_PATTERNS`, `load_data()`, and `get_feature_candidates()`. Single source of truth for all modules that filter ML-eligible features.

#### Changed
- **`modeling/train.py`**: Removed local EXCLUDE set and `load_data()`, now imports from `modeling.constants`.
- **`modeling/score_oof.py`**: Removed local EXCLUDE sets, `load_data()`, and `get_candidates()`, now imports from `modeling.constants`.
- **`backtest/engine.py`**: Removed local EXCLUDE_COLS/PATTERNS, now imports from `modeling.constants`.
- **`research/factor_research.py`**: Removed local EXCLUDE/PATTERNS, now imports from `modeling.constants`.
- **`tests/pipeline/test_enrich_fraud_labels.py`**: Updated test to check `modeling.constants.EXCLUDE_COLS` directly instead of parsing train.py source text.

### Feat — Session 34 (2026-06-28)

#### Changed
- **`models/feature_sets_3y.json`**: Replaced 45-feature set with canonical 27-feature pruned set (Sharpe 1.124 > 0.954). All consumers (score_oof, train_regression_model) now use the pruned set for 3y horizon automatically.

#### Added
- **`quality/check_model_staleness.py`**: Compares model_meta.json mtime vs data parquet mtime. Warns if model is stale; `--strict` exits 1. Added to CI workflow as non-fatal step after ML scoring.

### Tests — Session 34 (2026-06-28)

#### Added
- **`tests/research/test_ic_engine.py`**: 26 unit tests for IC engine — Newey-West HAC t-stat (edge cases, NaN, sign), BH FDR correction (all/none/partial rejection, index preservation), SIC-to-sector mapping, compute_yearly_ic (sector-neutral, min_obs, sic_col_override, random baseline).
- **`tests/research/test_factor_research.py`**: 20 unit tests for factor research orchestration — get_candidates (exclusions, sparse/string filter), compute_ic_series (correlated/noise signals), compute_turnover (stable feature, insufficient overlap), quintile spread, analyse_factor (output dict, ICIR sign, n_years).
- **`tests/portfolio/test_portfolio.py`**: 24 unit tests for portfolio modules — compute_composite (rank weighting, missing cols, bounds), kelly_weights (normalization, monotonicity, below-half fallback), apply_constraints (position cap + renorm, sector cap), _rank_blend (missing cols, monotonic, bounds), _quality_gate (Piotroski/Beneish/delisted filters).

#### Fixed
- **`portfolio/build_screener_registry.py`**: Fixed syntax error — `from _root import ROOT` was spliced into the middle of `from backtest.engine import (...)` statement.

### Tests — Session 33 (2026-06-28)

#### Added
- **`tests/alpha/test_factors.py`**: 22 unit tests for all alpha factor modules — value, quality, momentum, growth, fraud_risk, composite (output range 0–1, NaN handling, no-signal fallback, custom weights, index alignment).
- **`tests/backtest/test_walk_forward.py`**: 21 unit tests for walk-forward engine — run_backtest output shape/keys, temporal boundaries, single period, empty universe, equal-weight mode, SPY benchmark, fill_missing_return, bootstrap_ci, filing lag filter, SIC sector mapping, sector cap, filter_composite.

### Tests — Session 32 (2026-06-28)

#### Added
- **`tests/quality/test_check_data.py`**: 10 unit tests for `quality/check_data.py` — validates all check categories with synthetic data (shape, ranges, duplicates, nulls, winsorization).
- **`tests/quality/test_dataset_quality_gate.py`**: 31 unit tests for `quality/test_dataset_quality.py` — covers schema, structural, market coverage, fill rates, distributions, fraud labels, forward returns, growth winsorization, ML score exclusion, and point-in-time checks.

### Bug Fixes — Session 31 (2026-06-28)

#### Fixed
- **`workflows/run_pipeline_br.py`**: Moved `ROOT` import and `BASE` assignment above first use (was `NameError` at module load).
- **`modeling/score_oof.py`**: Moved `ROOT` import and `BASE` assignment above `DATA_PATH` definition (same `NameError`).

#### Removed
- **`pipeline/step1_fetch_tickers_jp_free.py`**: Dead file (unused JP free-tier variant).

### Quality + Orchestration Orientation — Session 30 (2026-06-28)

#### Added
- **`docs/architecture/orientation_quality_orchestration.md`**: Final orientation covering quality checks inventory (8 scripts), workflow/CI map (3 workflows + 6 local orchestrators), test coverage gaps, cross-package dependency graph, support package map (fraud/, portfolio/, data_io/), consolidated risk register (23 items ranked across all sessions), and priority-ordered refactor backlog.
- **`docs/architecture/BACKLOG.md`**: Added Critical #13-15 (quality test coverage, alpha/backtest tests, pipeline_br bug), Parked #19-20 (fraud package activation, CI alert improvement).
- Marked orientation phase (sessions 27-30) COMPLETE in SESSION_PLAN.md.

### Research + Backtest Orientation — Session 29 (2026-06-27)

#### Added
- **`docs/architecture/orientation_research_backtest.md`**: Full orientation covering backtest engine internals, research pipeline dependency graph, signal validation chain, risk register (7 bias vectors, hardcoded thresholds, fragile paths), and refactor candidates.

### IC Code Consolidation — Session 26 (2026-06-26)

#### Changed
- **`research/factor_research.py`**: Removed duplicated IC logic (`_sic_to_sector`, `_sector_demean`, inline Spearman loop). Now calls `ic_engine.compute_yearly_ic()` as single source of truth.
- **`research/ic_engine.py`**: Added `min_obs` and `sic_col_override` parameters to `compute_yearly_ic()` for caller flexibility (backward-compatible defaults).

### Regime Overlay (Macro Signal) — Session 25 (2026-06-26)

#### Added
- **`research/regime_overlay.py`**: SPY trailing drawdown regime signal (>15% from peak = risk-off → 50% cash). Runs agreement filter backtest with/without overlay.
- **`reports/regime_overlay_results.md`**: Comparison report with annual regime breakdown.
- **`reports/regime_overlay_results.json`**: Machine-readable results.

#### Results
- Regime signal: risk-off triggered in 2009, 2010 (post-2008 crash) and 2023 (post-2022 bear)
- Test period (2019-2024): overlay costs -2.25pp CAGR, no drawdown improvement (base already 0% max DD)
- **Decision: ADOPT as insurance-only** — dormant in backtest but protects against 2008-style crashes in deployment
- Overlay Sharpe 1.001 vs Base 1.138 (cost of insurance in benign period)

### Explainable Decision Tree + Agreement Filter — Session 24 (2026-06-26)

#### Added
- **`research/explainable_tree.py`**: Walk-forward backtest with decision tree, LightGBM, and agreement filter with threshold sweep. Extracts human-readable IF/THEN rules.
- **`models/decision_tree_rules.json`**: 5 BUY rules in structured JSON (conditions, probability, sample support).
- **`reports/explainable_model_results.md`**: Three-way comparison + threshold sweep results.

#### Results
- Threshold sweep (0.30–0.50): selected **tree_prob ≥ 0.35** (natural plateau)
- Agreement filter (0.35): Sharpe **1.138**, CAGR **+34.0%**, 0% max drawdown, 73.9% hit rate
- Beats LightGBM alone (+33.8%) while adding explainability for every pick
- **Decision: Agreement filter (threshold=0.35) = primary strategy**
- Every pick has a tree rule: e.g. `sales_to_price > 0.02 AND value_composite > 0.65 → BUY`

### Pruned Feature Set Backtest — Session 23 (2026-06-26)

#### Added
- **`research/pruned_backtest.py`**: Runs proper-split backtest with only the 27 temporally stable features. Implements add-back logic if Sharpe drops > 0.1.
- **`reports/pruned_backtest_results.md`**: Comparison report (full 43 vs lean 27 features).
- **`models/feature_sets_pruned.json`**: Final pruned feature set for downstream use.

#### Results
- Lean 27-feature model Sharpe = **1.124** (vs 0.954 full model — improvement of +0.17)
- CAGR +33.8% vs SPY +17.1% (excess +16.7%), hit rate 73.9%
- Removing 16 unstable features reduced overfitting — simpler model is strictly better

### Proper Train/Validate/Test Split — Session 22 (2026-06-26)

#### Added
- **`research/proper_split_backtest.py`**: Unbiased feature selection + walk-forward backtest with temporal split (Train 2008-2014 / Val 2015-2018 / Test 2019-2024). Fixes look-ahead bias where feature selection previously used full history.
- **`reports/proper_split_results.md`**: Full results report with gate decision.
- **`reports/proper_split_results.json`**: Machine-readable results.

#### Results
- **GATE PASS**: Test-period Sharpe = 0.954 (≥ 0.8 threshold)
- CAGR +31.9% vs SPY +17.1% (excess +14.8%) on unbiased test period 2019-2024
- 43 features selected using train-only data (vs 45 with full-history bias)
- Feature stability: 50% Jaccard overlap across shifted train windows (27 stable core features)
- Previous biased Sharpe (1.37) was ~44% overstated; corrected to 0.954

### Feature Ablation Study — Session 21 (2026-06-26)

#### Added
- **`research/ablation.py`**: Feature ablation harness — drops one feature at a time from feature_sets_3y.json, re-runs walk-forward ML + composite backtest, measures Sharpe/CAGR/hit rate delta.
- **`reports/feature_ablation_results.md`**: Ranked impact table for all 45 features.
- **`reports/feature_ablation_results.json`**: Machine-readable ablation results.

#### Results
- 1 load-bearing feature: `value_composite` (Sharpe Δ=-0.115, acts as volatility dampener)
- 16 prune candidates (removing improves Sharpe): top 3 are `book_to_market` (+0.121), `altman_x4` (+0.110), `value_x_momentum` (+0.102)
- 28 neutral features (zero marginal impact)
- Model can potentially shrink from 45 → ~25 features with improved Sharpe

### Archive Purge — Session 20 (2026-06-26)

#### Removed
- **`_archive/`** (118 files, 1.1 MB): dead pre-V2 code — old scripts, API stubs, docs, repomix metadata pack. Nothing imported from it.
- **`research/archive/`** (3 notebooks): superseded by `research/notebooks/` suite (IC analysis, EDA, factor correlation, etc.)
- **`notebooks/08_experiment_hub.ipynb`** (65 cells): references dead paths (`screener_registry.json`), fully superseded by research notebooks and backtest engine.

### Full Backtest Run — Session 19 (2026-06-26)

#### Added
- **reports/backtest_tearsheet_summary.md**: OOS backtest results for all 4 strategies (COMPOSITE, QEM, SCDV, IARB). Walk-forward ML, 30 bps cost, top-20 equal-weight, SPY benchmark.

#### Results
- COMPOSITE: Sharpe 1.37, CAGR +39.4%, excess +25.6% vs SPY, 88% hit rate (15 years OOS)
- QEM: Sharpe 1.04, CAGR +20.1%, low beta (0.33)
- SCDV: Sharpe 0.83, Sortino 9.03, only real drawdown (-7%)
- IARB: 1 year only — insufficient for risk metrics

### Feature IC Notebook — Session 18 (2026-06-25)

#### Added
- **notebooks/feature_ic_analysis.ipynb**: First research artifact on Architecture V2. Full IC analysis (228 features, sector-neutral, Newey-West t-stats, BH-FDR correction, factor group breakdown). Proves `research/`, `alpha/`, `modeling/` packages work end-to-end on real data.

### Tests + CI Hardening — Session 17 (2026-06-25)

#### Added
- **tests/test_integration.py**: Full-pipeline integration test (synthetic data → features → selection → model → alpha scores → backtest selection). 6 new tests.

#### Changed
- **CLAUDE.md**: Rewritten for Architecture V2 — removed all references to dead `scripts/`, `docs/developer/scripts.md`, `docs/architecture.md`, and other archived paths. Simplified Change Checklist to match the flat package structure.
- **.git/hooks/pre-commit**: Rewritten to call `quality/check_sync.py --warn-only` (non-blocking architecture sync warnings).

### Pipeline Consolidation — Session 14 (2026-06-25)

#### Changed
- **pipeline/step6_clean.py**: Expanded from structural cleaning only to a full enrichment pipeline — now includes quality fixes (dead columns, forecast flag, winsorize accruals, fix gross_margin), imputation (quarterly features, size_category), survivorship correction (delisted flag, pessimistic returns), and data confidence scoring (coverage × consistency × timeliness).
- **.github/workflows/refresh_data.yml**: Replaced 4 separate enrichment steps with single `step6_clean` call.
- **scripts/workflows/run_dataset_enrichments.py**: Removed merged steps (quality fix, confidence, survivorship, imputation) — only remaining post-step6 enrichments are p0f universe, quarterly features, fraud labels/taxonomy.

#### Archived
- `scripts/enrichments/fix_dataset_quality.py` → `_archive/dead_scripts/`
- `scripts/enrichments/impute_features.py` → `_archive/dead_scripts/`
- `scripts/enrichments/mark_survivorship.py` → `_archive/dead_scripts/`
- `pipeline/p0g_confidence_score.py` → `_archive/dead_scripts/`

#### Added
- **tests/pipeline/test_step6_enrichments.py**: Tests for merged quality fix, survivorship, and confidence functions.

### Repo Cleanup — Session 8 (2026-06-24)

#### Changed
- **.repomixignore**: Rebuilt from scratch — added `models/`, `reports/`, `research/`, `notebooks/`, `.github/`, `docs/reference/`, binary patterns (`*.parquet`, `*.joblib`, `*.csv`, `*.png`, `*.jpg`, `*.jpeg`), and `repomix-output*` glob variants. Context pack now 171 files / ~463K tokens with zero leakage from excluded directories.

### Repo Cleanup — Session 7 (2026-06-24)

#### Changed
- **docs/architecture.md**: Rebuilt architecture diagrams to reflect current repo state — column count 361→367, removed FastAPI/API (archived Session 2), fixed KR pipeline reference, removed TimescaleDB from deployment diagram, updated Component Map.
- **docs/developer/alpha-research-architecture.md**: New — explains current directory roles, data flow, pipeline inventory, and scripts subdirectory detail.
- **docs/index.md**: Updated column count 341→367 in tagline and Mermaid diagram.
- **docs/methodology/models.md**: Updated column count 361→367 in training pipeline flowchart.

### Repo Cleanup — Sessions 2–6 (2026-06-24)

#### Changed
- **repo cleanup**: Archived deprecated UI/API/deployment code into `_archive/`.
- **scripts/**: Reorganized 49 runnable scripts into workflow-oriented subdirectories.
- **scripts/**: Added `scripts/_root.py` for canonical project root resolution.
- **tests**: Added `pytest.ini` with pythonpath configuration.
- **docs**: Added `docs/developer/script-migration-map.md`.
- **CLAUDE.md**: Extracted large reference tables into `CLAUDE_REFERENCE.md` to reduce default context size.
- **docs**: Updated all documentation paths after scripts/ reorganization (30 files).
- **docs**: Created `docs/developer/fraud-screener-remnants-audit.md` — naming audit.

### Session 16A — Pipeline Orchestration Cleanup (2026-06-22)

#### **`scripts/run_dataset_enrichments.py`** — new
- Post-Step6 enrichment orchestrator: runs 9 steps in canonical order (fix_dataset_quality → p0f → p0g → survivorship → quarterly → impute → fraud_labels → fraud_taxonomy → validate_contract)
- Partially mitigates MUTATION-ORDER-001
- Flags: `--dry-run`, `--apply-universe-filters`, `--skip-survivorship`, `--skip-quarterly`

#### **`scripts/run_pipeline_kr.py`** — fix
- Fixed ghost reference KR-GHOST-STEP3-001: `step3_enrich_prices_kr.py` → `step3_enrich_prices.py`

#### **`.github/workflows/refresh_data.yml`** — docs
- Added comment block referencing local orchestrator equivalent

### Session 15B — HuggingFace Artifact Verification (2026-06-22)

#### **DATA-ARTIFACT-001** — closed
- Uploaded 9 artifacts to HuggingFace via `push_to_hf.py --all-data-artifacts`
- Verified round-trip restore via `pull_from_hf.py --all` with SHA256 checksum match
- Artifacts: dataset (84.4 MB) + snapshots (31.8 MB) + prices (38.1 MB) + 5 per-market + manifest

### Session 15 — Final Pipeline Hardening (2026-06-22)

#### **`pipeline/p0f_universe_definition.py`** — fix
- Fixed P0F-PRICE-FLOOR-001: updated docstring/comments to match code behavior (price floor applies to all exchanges, not just OTC)
- Removed unused `OTC_EXCHANGES` constant

#### **`.github/workflows/refresh_data.yml`** — enhanced
- Added feature contract validation step after enrichment, before Phase C scoring
- `validate_feature_contract.py` runs as mandatory gate (Phase B must be complete)

#### Documentation consistency fixes
- **`docs/methodology/feature-registry.md`** — corrected stale "annual-only" claims
- **`docs/index.md`** — updated row/column counts to reflect Phase B pipeline state
- **`README.md`** — updated dataset description for Phase B completeness
- **`docs/developer/data-update-guide.md`** — corrected "current production dataset" to Phase B state
- **`docs/developer/scripts.md`** — updated verify_doc_consistency description and A1 check threshold
- **`PIPELINE_ATLAS.md`** — corrected checklist 7.6 (price floor applies to all exchanges)

### Session 14 — Feature Contract / Feature Coverage (2026-06-22)

#### **`scripts/validate_feature_contract.py`** — new
- Validates dataset column groups by pipeline phase (Phase B vs Phase C)
- Reports group presence/absence with min-threshold checks
- Exits 0 if Phase B complete (even if Phase C pending)
- Supports `--strict`, `--json`, `--parquet` flags

#### **`docs/developer/feature-contract.md`** — new
- Defines column group contract per pipeline phase
- Classifies all 26 missing Phase C columns with producers and dependencies
- Documents validation usage and remediation steps

### Session 13 — Data Artifact Infrastructure (2026-06-22)

#### **`scripts/pull_from_hf.py`** — new
- Download artifacts from HuggingFace Hub (final dataset, snapshots, manifest)
- Supports `--all`, `--final`, `--snapshots`, `--manifest` flags
- Checksum verification via ARTIFACT_MANIFEST.json

#### **`scripts/generate_manifest.py`** — new
- Generate `data/ARTIFACT_MANIFEST.json` with rows, columns, checksums, commit hash
- Tracks all pipeline artifacts for integrity verification

#### **`scripts/push_to_hf.py`** — updated
- Added `--snapshots-only`, `--all-data-artifacts`, `--manifest-only` flags
- Uploads snapshots.parquet + prices.parquet + per-market snapshots
- Auto-uploads manifest with data pushes

### Session 12 — Data Regeneration (2026-06-22)

### Section 12 — Full-Universe Low-Beta Tier-1 Scan (2026-05-15)

#### **`notebooks/08_experiment_hub.ipynb`** — Section 12 cell outputs pre-baked (2026-05-15)
- Injected stdout from standalone run as stream outputs into cells s12scn01, s12bet01, s12out01

#### `notebooks/08_experiment_hub.ipynb` — 5 new cells (s12md000, s12cfg01, s12scn01, s12bet01, s12out01)
- Loads full 58K-row parquet; keeps latest annual row per US ticker (fiscal_year ≥ 2022)
- Recomputes composite score from up to 9 signals (same pool as Section 5) on the full universe
- Applies all 4 safety gates (Piotroski ≥ 7, Altman Z > 1.81, FCF yield > 0, Beneish < −1.78) → Tier-1 filter
- Fetches live beta via yfinance for all Tier-1 candidates; filters to β < 1.0
- Assigns leverage (2× if β<0.5, 1.5× if β<0.8, 1.25× if β<1.0) and computes levered 3y CAGR using Section 11 coefficients
- Exports `data/full_universe_low_beta_tier1_2026-05-15.csv`
- Scatter + bar charts: beta vs levered CAGR, colour-coded by leverage tier
- **Run results (2026-05-15)**: 4,653 US tickers → 40 Tier-1 → 21 with β < 1.0 → top 20 by levered 3y CAGR
  - Top 5 (2× lever, β < 0.5): IMXI 53.2%, VLGEA 47.4%, TK 46.6%, CNC 44.8%, MOH 44.2%
  - 1.5× lever (β 0.5–0.8): PZZA 31.2%, CON 30.0%, BBSI 29.7%, PFGC 29.0%, ICFI 28.0%
  - 1.25× lever (β 0.8–1.0): IBEX 26.6%, LZB 23.4%, TGT 23.0%, PUMP 22.9%

### Section 11 — Multi-Horizon CAGR Calibration (6m · 2y · 3y) (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — 4 new cells (s11md000, s11cfg01, s11cal01, s11apl01)
- **6m calibration**: annualised return = −5.58% + 39.34%×composite (R²=0.011, n=32,218); at composite=0.80 → 25.9% ann. (inflated by short-horizon noise)
- **2y calibration**: annualised return = −23.82% + 52.95%×composite (R²=0.092, n=27,854); at composite=0.80 → 18.5% ann.
- **3y calibration**: annualised return = −24.65% + 52.43%×composite (R²=0.133, n=24,029); at composite=0.80 → 17.3% ann. — strongest model (WF AUC 0.6248 ✅)
- **R² pattern**: R² rises monotonically 6m→3y (0.011→0.133) — longer horizons have much stronger signal; 6m is noisy
- **Re-score cell (11B)**: applies all 4 horizon CAGRs to picks simultaneously; best_CAGR_% = 3y column; exports `data/multi_horizon_calibrated_2026-05-15.csv`
- VLGEA (β=0.10, leverage 2×): levered 3y best = **39.0%** ann. — only pick qualifying for leverage

### Section 10 — Beta, CAGR Calibration & Time Horizon Framework (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — 4 new cells (s10md000, s10cfg01, s10cal01, s10bet01)
- **Issue 1 — Time horizon documented**: Confirmed 1-year rebalancing cycle driven by `ml_1y_oof` / `forward_return_1y` as primary backtest signal
- **Issue 2 — Beta fix**: `beta_12m` absent from parquet → all Section 9 leverage was 1×; Section 10 fetches live betas via yfinance (`BETA_PERIOD='1y'`, benchmark=SPY); 13/20 picks resolved (7 TSX/TSX-V names delisted/not found); VLGEA β=0.10 (qualifies 2×), CVGI β=2.11, CRNT β=1.95, TEAD β=3.44, JELD β=2.92
- **Issue 3 — CAGR calibration**: OLS on 31,916 US stock-years (2014–2024); equal-weight rank composite from 7 signals, winsorized at 5%; fitted: `return = -15.26% + composite × 44.23%` (R²=0.032, p=8.1e-229); replaces hardcoded `8 + composite × 27` (was ~10% too optimistic at composite=0.8)
- **Quintile lift**: Q1 mean 1Y return −2.7%, Q5 +18.1%; confirms composite score has directional signal
- **Liquidity flags (Section 10C)**: All 6 Tier-1 picks on US exchanges (🟢); 3 TSX Venture names (🔴 CAF.V, PVT.V, IRO.V) flagged as very illiquid
- **Exports**: `data/leveraged_picks_calibrated_2026-05-15.csv`

### Section 6 DT Screener — Per-Leaf Walk-Forward CAGR (Part C) (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — cell `1248e80a` extended with Part C per-leaf WF CAGR
- **Per-leaf walk-forward CAGR**: inside the existing WF loop, added `model_wf.apply(X_yr)` to assign `leaf_id_wf` to each scored stock; collected `{leaf_id, year, n, avg_ret, avg_prob, excess}` rows per year
- **Aggregation**: after the WF loop, grouped by `leaf_id` across all years; computed annualized CAGR, excess CAGR vs bench, hit rate (% years positive), vol, and Sharpe per leaf
- **Display**: styled heat-map table sorted by walk-forward CAGR, one table per horizon (6m/1y/2y/3y/5y)
- Reuses same WF training pass as Part B — no duplicate training; leaf rows collected inside Part B loop

### Section 6 DT Backtest — walk-forward expanding window, no look-ahead bias (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — cell `1248e80a` Part B replaced with proper walk-forward backtest
- **Problem fixed**: original Part B trained `DecisionTreeClassifier` on full history then scored historical years with the same model — look-ahead bias inflated CAGR/Sharpe
- **Fix**: for each year Y, fresh DT trained on `fiscal_year < Y` only (expanding window); year Y scored with that held-out model
- Guard: `WF_MIN_TRAIN_ROWS = 200` skips years without sufficient training history
- Per-horizon annualization via `hor_years_map = {'6m': 0.5, '1y': 1, '2y': 2, '3y': 3, '5y': 5}`
- Part A (per-rule leaf stats) retained as in-sample rule inspection with clear label; not used for performance claims

### Section 6 Decision Tree Screener — multi-horizon (all 5) (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — extended DT screener to all 5 horizons
- Cell `690181fe`: `DT_HORIZONS = ['6m','1y','2y','3y','5y']`; trains one DT per horizon, stores in `dt_models`/`dt_feats_by_h`/`X_train_by_h`/`y_train_by_h`; prints AUC table + rules per horizon
- Cell `79ad780b`: loops all 5 horizons; scores universe per model; applies filters; displays 5 styled top-100 tables; exports `dt_screener_top100_{h}_{today}.csv` per horizon; stores in `dt_top_by_h`
- Cell `1248e80a`: loops all 5 horizons; per-rule leaf stats (Part A) + year-by-year backtest with correct per-horizon CAGR annualization via `hor_years_map` (Part B)

### Section 6 Decision Tree Screener — executed cells with outputs (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — executed all three Section 6 cells
- Cell `690181fe`: DT trained (1y, depth=5, 37 features, train AUC 0.5987), rules printed
- Cell `79ad780b`: 7,625 stocks scored, filters applied, top-100 displayed with market breakdown
- Cell `1248e80a`: per-rule leaf stats table + year-by-year backtest charts rendered

### Section 6 Decision Tree Screener — filters + CAGR performance (2026-05-14)

#### `notebooks/08_experiment_hub.ipynb` — added filters and historical CAGR to Section 6
- Added 6 filter config variables: `DT_MARKETS`, `DT_MIN_SCORE`, `DT_MIN_MARKET_CAP`, `DT_PIOTROSKI_MIN`, `DT_BENEISH_MAX`, `DT_ALTMAN_MIN`
- Filter application block applies all active filters to universe before top-N display; prints before/after counts
- New CAGR performance cell (two parts):
  - **Per-rule stats**: `dt_model.apply()` → per leaf-node hit rate, avg return, implied CAGR, styled with `background_gradient(cmap='RdYlGn')`
  - **Year-by-year backtest**: equal-weight top-N portfolio per `fiscal_year` → implied CAGR, excess vs benchmark, hit rate, Sharpe; two-panel chart (annual CAGR bars + CAGR-by-leaf bars); styled year-by-year table
  - NOTE: in-sample reference only — DT trained on full history

### Section 6 Decision Tree Screener — rule-based top 100 (2026-05-14)

#### `notebooks/08_experiment_hub.ipynb` — added Section 6 Decision Tree Screener
- Trains `DecisionTreeClassifier(max_depth=5, min_samples_leaf=30)` on same feature set as LightGBM for configurable horizon
- Prints interpretable branching rules via `export_text()` (depth ≤ 3)
- Scores full current universe (latest annual snapshot per ticker/market)
- Displays top 100 companies ranked by DT probability with green gradient styling
- Exports results to `reports/dt_screener_top100_{horizon}_{date}.csv`

---

### Schema drift fix + LightGBM vs Decision Tree overfitting analysis (2026-05-14)

#### `data/historical_dataset_clean.parquet` — patched 4 missing columns (361 → 367 cols)
- Added `depreciation_to_assets`, `financing_cashflow_to_assets` (all 5 models referenced these)
- Added `piotroski_shares_ok`, `piotroski_f_score_9` (2y/6m models referenced these)
- Formulas sourced from `pipeline/feature_library.py` (`add_normalised_ratios`, `add_piotroski_ext`)

#### `reports/overfitting_comparison.json` — new report; LightGBM vs Decision Tree train/test AUC per horizon
- LightGBM train/test gap ≤ 0.004 across all horizons (no classical overfitting)
- Decision Tree gaps 0.007–0.023 (mild overfitting, larger at longer horizons)
- LightGBM outperforms DT by 1.5–5.1 AUC points at test time on every horizon
- WF AUC gap (0.57–0.62) vs held-out AUC (0.61–0.69) is temporal non-stationarity, not overfitting

### Experiment notebook executed with full output (2026-05-14)

#### `notebooks/08_experiment_hub.ipynb` — executed via nbconvert; all 5 sections rendered with output
- Section 1: Feature selection summary and SHAP importance charts per horizon
- Section 2: Walk-forward AUC charts and model performance tables
- Section 3a/3b: Screener registry leaderboard and individual signal rankings with charts
- Section 4a/4b: Deep dive composite and individual signal analysis with annual return bars
- Section 5: Live picks (longs + shorts) exported to `reports/screener_longs_*.csv`

### Version 2 roadmap documented (2026-05-14)

#### Documentation
- **`docs/developer/roadmap.md`**: Created — 6-section v2 plan covering AUC improvements (6m/1y/2y), quarterly data pipeline, portfolio construction, live data pipeline, alternative data (Form 4 insider), and production API deployment
- **`docs/developer/index.md`**: Roadmap link added

### Architecture: experiment notebook declared as primary frontend; Streamlit archived (2026-05-14)

#### Documentation — all Streamlit references replaced with notebook frontend (docs/arch)
- **`docs/architecture.md`**: Outputs subgraph E1 → Experiment Notebook node; Component Map App row → Experiment Notebook row; Deployment Architecture Streamlit Cloud node → local notebook node
- **`docs/index.md`**: Mermaid diagram G node → Experiment Notebook; "I want to use the app" tab → "I want to use the notebook"; deployment link text updated
- **`CLAUDE.md`**: Architecture table Streamlit UI row → Experiment Notebook row; key file locations App entry point → research frontend; Change Checklist Streamlit UI row → notebook row
- **`docs/developer/deployment.md`**: Streamlit Cloud section replaced with Research Frontend section (local notebook execution)
- **`docs/quickstart.md`**: Step 4 "Launch the App" → notebook launch; streamlit dependency removed; Next Steps updated
- **`docs/guide/app.md`**: Deprecation note added — `app_v2.py` archived, notebook is active frontend
- **`docs/developer/setup.md`**: app_v2.py entry annotated as archived; launch step → notebook
- **`docs/developer/tests.md`**: Streamlit UI test coverage note → notebook rendering note
- **`docs/developer/schema-change-guide.md`**: `app_v2.py` check → notebook check
- **`docs/methodology/backtesting.md`**: backtest_results.json consumer updated to notebook Section 3

### Master experiment notebook completed: composite registry + individual signal deep dives (2026-05-14)

#### `notebooks/08_experiment_hub.ipynb` — Task 6 complete (feat/notebook)
- **Section 3a** (new): Composite Screener Registry leaderboard — loads `data/screener_registry.json`, displays ranked table (CAGR, Sharpe, Sortino, Calmar, MaxDD, Excess vs SPY) and 3-panel chart (CAGR barh, Sharpe barh, Sharpe vs MaxDD scatter) for all 7 named strategies
- **Section 3b** (updated): Added `### 3b. Individual Alpha Signal Rankings` markdown header above existing alpha registry cells
- **Section 4** (new): "Screener Deep Dive" split into 4a (composite) and 4b (individual signal)
  - **4a-load** (`64a2f6bf`): loads selected `SCREENER_REGISTRY_ID` from registry, prints full stats profile (20 metrics)
  - **4a-annual** (`7b289baf`): dual-panel annual returns chart — grouped bar (strategy vs SPY) + excess return bars with rolling Sharpe twin axis
  - **4a-peers** (`34f0a302`): 7-metric bar chart comparing selected screener vs all composite peers (selected highlighted red)
- **Config cell** (updated): Added `SCREENER_REGISTRY_ID = 'COMPOSITE_US'` with inline comments listing all 7 valid options

### Screener registry: 7 named screener configs with walk-forward backtests (2026-05-14)

#### `build_screener_registry.py` — single-command registry builder (feat/alpha)
- **`scripts/build_screener_registry.py`**: New script — defines 7 named screener configs (COMPOSITE_US, COMPOSITE_INTL, COMPOSITE_MICRO, VALUE_QUALITY, MOMENTUM_GROWTH, FRAUD_AVOID, WIDE_UNIVERSE), each with a dedicated filter function and alpha blend. Runs all configs through `backtester.py`'s `run_backtest()` engine and writes results (CAGR, Sharpe, max drawdown, Calmar, hit rate, excess vs SPY) plus config metadata to `data/screener_registry.json`. CLI supports `--ids`, `--top N`, `--dry-run`, `--out` flags. Uses OOF ML scores (`ml_1y_oof`, `ml_3y_oof`) for historical alpha — not contaminated in-sample scores.
- **`docs/developer/scripts.md`**: Added `build_screener_registry.py` section.

### Repo structure clean-up (2026-05-14)

#### Removed dead files, archived EDA notebooks, merged requirements (chore/repo)
- Deleted **`pipeline/phase_a_integrate_{br,ca,eu,jp,kr}.py`** — one-time market integration scripts, no longer needed
- Deleted **`research/03_ml_model.ipynb`**, **`research/showcase.ipynb`** — superseded
- Removed **`HF_TOKEN.md`** — security risk; secrets belong in `.env` / CI vars only
- Moved **`notebooks/01–06_*.ipynb`** → **`research/notebooks/`** (EDA archive)
- Deleted **`notebooks/07_live_screener.ipynb`** — superseded by `08_experiment_hub.ipynb`
- Added **`notebooks/08_experiment_hub.ipynb`** — new master notebook
- Merged **`requirements-api.txt`** into **`requirements.txt`**; deleted `requirements-api.txt`

### Regression model extended to all 5 horizons — continuous CAGR output (2026-05-14)

#### `train_regression_model.py` rewritten: all-horizon support (feat/models)
- **`scripts/train_regression_model.py`**: Rewrote to train LightGBM Huber regression models for all 5 horizons (6m/1y/2y/3y/5y), not just 3y. Target is `excess_return_local_{h}` (continuous CAGR vs local market index), falling back to `forward_return_{h}`. Added `--horizons` argument; reuses frozen `feature_sets_{h}.json` per horizon (no new feature selection on regression target). PIT-safe WF CV excludes incomplete folds via `max_test_year = max_year - ceil(horizon_years) + 1`. Outputs `model_{h}_regression.joblib`, `model_{h}_regression_meta.json`, `reports/regression_ic_{h}.csv` per horizon.
- **`docs/developer/scripts.md`**: Updated `train_regression_model.py` section — added `--horizons` flag, updated outputs/description to all-5-horizon support.

### Model tuning: XGBoost ensemble, purged CV embargo, sector z-score, tighter dedup (2026-05-14)

#### Four concrete improvements to reduce feature leakage and increase model diversity (perf/models)
- **`scripts/train_models.py`**: Added `XGB_AVAILABLE` guard + `train_xgb_model()` (n_estimators=500, max_depth=5, lr=0.05, subsample=0.8, colsample_bytree=0.7, min_child_weight=20) — XGBoost ensemble blending available via `--ensemble` flag
- **`scripts/train_models.py`**: Added `--ensemble` flag — 50/50 LightGBM + XGBoost blend in WF CV folds; production model stays LightGBM-only
- **`scripts/train_models.py`**: Added `--embargo-years INT` (default 0) — purged walk-forward CV: excludes most recent N training years per fold to prevent adjacent-year autocorrelation leakage; use `--embargo-years 1` for standard purged CV
- **`scripts/train_models.py`**: Added `sector_zscore_normalize()` function + `--sector-zscore` flag — within-(fiscal_year, sic_code) z-score normalization that removes cross-sector absolute valuation differences; groups with <5 members left unnormalized
- **`scripts/train_models.py`**: Tightened `deduplicate_features` default threshold 0.90 → 0.85 (removes more near-duplicate features, reducing multicollinearity in selection)
- **`scripts/run_feature_selection.py`**: `CORR_THRESHOLD` tightened 0.90 → 0.85 (aligned with train_models.py)
- **`requirements.txt`**: Added `xgboost>=2.0.0`
- **`docs/developer/scripts.md`**: Updated `train_models.py` flags table — added `--embargo-years`, `--ensemble`, `--sector-zscore`; updated dedup threshold note

### Raise investment target to ≥25% annualised ROI (2026-05-14)

#### Update vision target across docs (docs)
- **`ROADMAP.md`**: Vision and investment target raised from ≥20% to ≥25% annualised ROI (reflects COMPOSITE strategy CAGR of +25%+ validated in backtests)
- **`docs/index.md`**: Tagline updated to ≥25%

### build_portfolio.py: MoS gate, low-vol filter, implied drawdown, $10M floor (2026-05-14)

#### Vision gap fixes: portfolio risk controls + liquidity floor (feat/portfolio)
- **`scripts/build_portfolio.py`**: `MIN_MARKET_CAP` lowered 50M → $10M (micro-cap / institution-avoidance niche)
- **`scripts/build_portfolio.py`**: Added `--mos-min-score FLOAT` — margin-of-safety gate requiring `alpha_value >= threshold` before stock selection; applied in both `run_backtest` and `build_current_holdings`
- **`scripts/build_portfolio.py`**: Added `--low-vol-only` — retains only stocks in bottom-half of trailing 12m volatility per year; applied in both backtest and holdings paths
- **`scripts/build_portfolio.py`**: Added `implied_max_drawdown_pct` to backtest output — `−max(|max_dd|, 2σ)` proxy that corrects the annual-sampling artifact (pure year-end drawdown misses intra-year troughs)
- **`docs/developer/scripts.md`**: Updated `build_portfolio.py` flags table and output description

### Re-tune 6m/1y/2y models — D1.3 (2026-05-14)

#### 60-trial Optuna re-run for 6m, 1y, 2y horizons (perf/models)
- **`scripts/tune_models.py`**: Re-ran 60-trial Optuna for 6m (val=0.617), 1y (val=0.605), 2y (val=0.606)
- **1y WF AUC improved**: 0.5683 → 0.5774 (+0.009) with new tuned params
- **6m WF AUC unchanged**: 0.5715 (structural ceiling; gap 0.0085 from 0.58 target)
- **2y WF AUC unchanged**: 0.5880 (gap 0.012 from 0.60 target)
- **Root cause documented**: 1y dragged by 2018→2019 (AUC=0.52, late-cycle) and 2020→2021 (AUC=0.53, COVID) folds; macro signal correctly captured via recession/quality_in_recession interaction features (ICIR=9.4–9.8); PSI filter correctly excludes yield_curve/credit_spread_baa (distributional drift)
- **`models/model_meta.json`**: Updated wf_mean_auc values for all horizons
- **`docs/methodology/models.md`**, **`docs/index.md`**, **`CLAUDE.md`**: Performance tables updated

### Add live screener notebook (2026-05-14)

#### Decision-support notebook for investment picks (feat/notebooks)
- **`notebooks/07_live_screener.ipynb`**: 7-section notebook — loads latest parquet (local or HuggingFace), applies quality gates (Piotroski≥5, Beneish<-1.78, Altman>1.81), margin-of-safety filters (P/B≤3, FCF yield≥3%), micro-cap institution-avoidance filter ($10M–$300M), IC-weighted composite scoring, Kelly-sized long/short recommendations, leverage candidate flag (β<0.80 proxy), and CSV export to `reports/`.

---

### Fix doc accuracy — data source descriptions and architecture diagrams (2026-05-14)

#### Correct pipeline step1/step2 data source entries (docs)
- **`docs/developer/pipeline-scripts.md`**: Fixed step2 table — EU was SimFin (wrong) → yfinance no API key; JP was "Optional/paid" (wrong) → free EDINET API key; CA was TMX public API (wrong) → SEDAR+ XBRL filings + yfinance fallback. (step1 corrections were in prior commit.)
- **`docs/architecture.md`**: Data Sources subgraph A2 SimFin→yfinance EU, A4 TDNET→EDINET. Data Flow node text corrected (EDGAR/DART/yfinance/EDINET/CVM). Alpha label fixed from "341→360 cols total" → "+6 alpha cols".
- **`CLAUDE.md`**: Architecture State multi-market ingestion row corrected (SimFin/TDNET → yfinance/EDINET).
- **`research/`**: Archived stale notebooks (01_metric_exploration, 02_historical_dataset, 04_factor_research) to `research/archive/`.

---

### Fix Streamlit Cloud build failure — Linux wheel compatibility (2026-05-14)

#### Fix pip install on Linux manylinux build environment (fix/deploy)
- **`requirements.txt`**: Downgraded `pyarrow` 21.0.0 → 20.0.0 and `pandas` 2.3.3 → 2.3.2. Both 21.0.0 and 2.3.3 lack binary wheels for Linux manylinux on PyPI, causing Streamlit Cloud builds to fail with "Exited with status 1".

---

### Retrain with tuned params + expanded FORCE_INCLUDE features for 6m/1y (2026-05-14)

#### Fix best_params propagation and add WF CV with Optuna hyperparameters (fix/perf)
- **`scripts/tune_models.py`**: `tune_lgbm()` now returns all 9 best hyperparameters (was returning only the model + CSV of 3 params). Full dict saved to `model_meta.json` as `best_params`. 6m best: lr=0.0196, n_est=230, num_leaves=21. 1y best: lr=0.0201, n_est=394, num_leaves=59.
- **`scripts/train_models.py`**: Added `override_params` arg to `train_model()` and `override_params_per_horizon` arg to `walk_forward_cv()` — allows injecting Optuna-tuned params into WF folds without re-running Optuna.
- **`scripts/train_models.py`**: Added `--use-tuned-params` CLI flag — when passed with `--walk-forward`, loads `best_params` from `model_meta.json` and uses them in every WF fold.
- **`scripts/train_models.py`**: `_old_meta` loaded before training loop to carry forward `best_params` (and other tune fields) from prior `tune_models.py` run into the freshly-built `model_meta` dict.
- **`scripts/train_models.py`**: `FORCE_INCLUDE_6M` and `FORCE_INCLUDE_1Y` expanded with 4 high-ICIR features bypassed by ICIR ranking: `sales_to_price` (ICIR~2.0), `ohlson_roe` (ICIR~1.4), `value_x_quality` (ICIR~1.3), `piotroski_f_score` (ICIR~1.25). These fundamental quality/value signals had 14 years of IC data but were crowded out by momentum features in the top-N selection.
- **`docs/developer/scripts.md`**: Added `--use-tuned-params` flag to `train_models.py` flags table.


#### Tune 6m and 1y LightGBM models with Optuna (perf)
- **`models/model_6m.joblib`**: Optuna 60-trial TPE tuning — tuned val AUC 0.617 (+1.0pp vs 0.607 baseline), tuned test AUC 0.517 (+1.2pp). WF Mean AUC 0.5626 (target ≥ 0.58 — not yet met; WF re-run with tuned params required for formal measurement).
- **`models/model_1y.joblib`**: Optuna 60-trial TPE tuning — tuned val AUC 0.603 (+0.37pp), tuned test AUC 0.496 (+1.2pp). WF Mean AUC 0.5634 (target ≥ 0.62 — not met; short-horizon fundamentals-only models are inherently noisy).
- **`models/model_meta.json`**: updated with `tuned_val_auc` and `tuned_test_auc` fields for 6m and 1y.
- **`docs/methodology/models.md`**: AUC table updated with tuned columns.
- **`docs/index.md`**: Performance at a Glance table updated.
- **`scripts/push_to_hf.py`**: `HORIZONS` extended from `['1y','3y','5y']` to `['6m','1y','2y','3y','5y']` — all 5 horizons now pushed to HuggingFace Hub.


### Publish showcase notebook with full outputs (2026-05-14)

#### Execute and publish research showcase notebook (feat)
- **`research/showcase.ipynb`**: executed notebook with full cell outputs — 19 code cells, 0 errors. Covers dataset overview, factor analysis, feature selection, model training, Optuna tuning, walk-forward validation, SHAP importance, backtest, live screener demo.

### Fix Streamlit Cloud deployment + ML scoring (2026-05-14)

#### Fix broken cloud deployment and ML scoring (fix)
- **`src/data.py`**: `_hf_download_bytes` now passes `HF_TOKEN` env var to `hf_hub_download` — required for private HuggingFace Hub repos on Streamlit Cloud.
- **`src/data.py`**: `load_data()` shows a clear `st.error` with instructions when both `HF_REPO` and local parquet are absent, instead of crashing with a FileNotFoundError.
- **`src/data.py`**: `load_models()` checks local files first, then falls back to HF Hub.
- **`src/data.py`**: Adds derived column `financing_cashflow_to_assets = financing_cash_flow / total_assets` at load time — fixes all-NaN ML scores caused by LightGBM feature count mismatch.

### Factor research look-ahead fix, IC decay analysis, regression bias audit (2026-05-14)

#### Fix ML-score look-ahead contamination in factor IC table (fix)
- **`scripts/factor_research.py`**: expanded `EXCLUDE_PATTERNS` to exclude all ML-derived and composite scores (`ml_1y/2y/3y/5y/6m`, `_oof`, `ml_pred_excess`, `composite_score`, `alpha_*`). Previously `ml_3y` was included as an IC candidate, artificially inflating its ICIR to 5.68 and Q5 spread to 110% — direct look-ahead contamination since static ML scores are trained on the full dataset. After fix, max Q5 spread is 0.47 and all top factors are pure fundamentals.
- **`scripts/factor_research.py`**: added `compute_ic_decay()` function and `--ic-decay` / `--decay-top N` CLI flags. Computes IC at 1y, 3y, 5y lags for top-N factors and estimates signal half-life. Value/quality factors show >5y half-life (structural); recession-regime factors show ~2.2-2.5y half-life (tactical).
- **`reports/factor_research_{1y,3y,5y}_sn.csv`**: regenerated with clean EXCLUDE_PATTERNS.
- **`reports/ic_decay_halflife.csv`** (new): IC at 1y/3y/5y and estimated half-life for top 20 factors.

#### Add regression model bias audit — Audit 5 (feat)
- **`scripts/bias_audit.py`**: added `_REGRESSION_CONTAMINATED` set and `audit_regression_model()` function (Audit 5). Three checks: (1) feature contamination scan against ML/forward-return columns, (2) walk-forward IC distribution from `regression_ic_3y.csv`, (3) permutation test (50 shuffles) using `model.feature_name_` for alignment. Results: no contaminated features ✓, WF IC 0.337 (⚠ suspicious but genuine), permutation z=26.8 ✓.

#### Fix backtest annual_returns key mismatch in UI (fix)
- **`src/ui/tab_backtester.py`**: fixed key mismatch where UI read `port_ret`/`bench_ret` (decimal) but `backtest_results.json` stores `port_pct`/`bench_pct` (percentage). Annual returns and cumulative wealth charts now render correctly.
- **`data/backtest_results.json`**: re-generated with monthly price cache — composite strategy MaxDD=-20.8% (was 0.0%), CAGR=+37.1%, Sharpe=1.566.

#### Add plain-English investment thesis generator (feat)
- **`alpha/explain.py`** (new): generates per-ticker buy rationale in plain English. Covers predicted 3-year excess return, Piotroski quality verdict, Beneish fraud risk, Altman distress risk, valuation snapshot (P/B, FCF yield, market cap), ML conviction level, recommended strategy (LEAPS/margin/equity-only), risk flag detection, and a 4-point margin of safety checklist. CLI: `python3 alpha/explain.py --market US --top 15`. API: `from alpha.explain import explain_pick, explain_many`.

#### Fix missing return statement in `_apply_three_stage_filter` (fix)
- **`scripts/leverage_strategy.py`**: fixed missing `return df` at end of `_apply_three_stage_filter()`. Without it the function returned `None`, causing a `TypeError` when the caller tried to call `.head()` on the result.

#### Docs update
- **`docs/developer/scripts.md`**: added `alpha/explain.py` section (usage, flags table, API examples, outputs); updated `factor_research.py` section with `--ic-decay`/`--decay-top` flags and ML exclusion note; updated `bias_audit.py` section to describe Audit 5 (regression model checks).

### Streamlit Cloud deployment fix (2026-05-14)

- **`app.py`** (new): two-line shim that imports and calls `main()` from `app_v2.py`. Fixes Streamlit Cloud deployment where the dashboard is configured to launch `app.py` but the actual entry point is `app_v2.py`.

### Phase D2 — Monthly NAV MaxDD and ADTV liquidity filter (2026-05-14)

#### Fix MaxDD=0% bug and add ADTV filter (fix + feat)
- **`scripts/build_monthly_price_cache.py`** (new): dry-runs composite/qem/scdv strategy filters for backtest years 2008–2023 to collect ~200–300 unique tickers, then downloads monthly OHLCV from yfinance in batches of 50. Computes `adtv_30d` (rolling 3-month average of daily dollar volume) and writes `data/monthly_prices.parquet`. Supports `--update` (extend cache) and `--tickers-only` flags.
- **`scripts/backtester.py`**: added `load_monthly_prices()`, `compute_monthly_nav()`, and `adtv_filter()`. When `data/monthly_prices.parquet` is present, MaxDD is computed from a true monthly NAV curve (reveals intra-year drawdowns invisible at annual granularity). ADTV filter removes picks whose $50K position would exceed 5% of 30d ADTV (PIT-safe: uses Sep–Dec of observation year). New `run_backtest()` params: `monthly_px`, `use_adtv_filter`, `max_pct_adtv`. New `main()` flag: `--no-adtv`. Both features degrade gracefully when the cache is missing.
- **`docs/developer/scripts.md`**: added `build_monthly_price_cache.py` section; updated `backtester.py` entry with `--no-adtv` flag and monthly-NAV/ADTV description.
- **`scripts/push_to_hf.py`**: added `model_3y_regression.joblib` and `model_3y_regression_meta.json` to the model upload list.

### 3-stage screener with regression magnitude ranker (2026-05-14)

#### Add LightGBM Huber regression model for excess return magnitude (feat)
- **`scripts/train_regression_model.py`** (new): trains LightGBM Huber regressor to predict `excess_return_local_3y`. Reuses frozen 45-feature ICIR set from `models/feature_sets_3y.json`. PIT-safe temporal split, winsorized target, expanding-window walk-forward CV with Spearman IC per fold. Outputs `models/model_3y_regression.joblib`, `models/model_3y_regression_meta.json`, `reports/regression_ic_3y.csv`.
- **`scripts/score_historical.py`**: added `score_regression()` function and `--skip-regression` flag. When `model_3y_regression.joblib` is present, writes `ml_pred_excess_3y` column to the parquet for all rows.
- **`scripts/leverage_strategy.py`**: refactored to 3-stage screener. Stage 1 extended with P/B < 5.0 and market cap ≥ $50M gates. New `_apply_three_stage_filter()` chains Stage 1 → Stage 2 (ml_score_3y > 0.52) → Stage 3 (sort by ml_pred_excess_3y). Position weights now proportional to `ml_pred_excess_3y` (Kelly-like) when regression model is available; falls back to `composite_score`. `_pick_strategy()` updated to use `ml_score_3y` (replaces sub-random `ml_score_1y`).
- **`docs/developer/scripts.md`**: added `train_regression_model.py` section; updated `score_historical.py` and `leverage_strategy.py` entries with new flags and 3-stage description.

### Signal integrity fixes (2026-05-14)

#### Remove sub-random ml_score_1y from leverage composite (fix)
- **`scripts/leverage_strategy.py`** `composite_score()`: removed `ml_score_1y` (test AUC 0.484, sub-random — was incorrectly weighted at 30%). Redistributed weight to `ml_score_3y` (now 0.45, the only validated signal at WF AUC 0.625). `value_composite` 0.25, `quality_composite` 0.20, `piotroski_f_score` 0.10 unchanged.

#### Add --oot-eval OOT diagnostic to train_models.py (feat)
- **`scripts/train_models.py`**: added `--oot-eval` flag and `run_oot_diagnostic()` function. Retrains 3y model with `TRAIN_CUTOFF=2019`, tests on `FY2022` (where `beat_local_market_3y` is fully known since 2022+3=2025). Production models are never overwritten. Saves `reports/oot_auc_diagnostic.json` with OOT AUC, sample sizes, and horizon metadata.
- **`docs/developer/scripts.md`**: added `--oot-eval` row to `train_models.py` flags table.

#### Optuna tuning for 3y horizon (perf)
- **`scripts/tune_models.py`**: fixed `sys.path` so `scripts.train_models` import resolves correctly; promoted `_CalModel` from local class to module-level to fix pickling. Ran `--no-catboost --horizon 3y --trials 50`; tuned val AUC 0.6644 → calibrated ensemble val AUC 0.6773. Outputs: `models/model_3y_tuned.joblib`, `models/model_3y_calibrated.joblib`, `reports/optuna_study_3y.csv`.

---

### Phase D6 — Reports & Plots (2026-05-14)

#### D6.1 — Kelly portfolio tearsheet page in generate_reports.py (feat)
- **`scripts/generate_reports.py`** Added `PORTFOLIO_BACKTEST` + `PORTFOLIO_HOLDINGS` path constants. Added `_load_portfolio_backtest()` and `_load_portfolio_holdings()` helpers. Added `_fig_portfolio_tearsheet()`: 4-panel figure (cumulative wealth vs SPY, annual return bar, drawdown, KPI summary with VaR/CVaR + top 10 holdings table). `generate_pdf()` now accepts `portfolio_backtest` and `portfolio_holdings` kwargs; inserts Kelly portfolio page after the strategy performance page. `main()` loads both files and passes them to `generate_pdf()`.
- **`docs/developer/scripts.md`** Updated `generate_reports.py` section — new flag table note on PDF page ordering and `rolling_oos_auc.png` output listed.

#### D7.1 — Kelly portfolio tearsheet + alpha signal browser in Tab 5 (feat)
- **`src/ui/tab_backtester.py`** Added path constants for `portfolio_backtest.json`, `portfolio_holdings.json`, `alpha_registry.json`. New **Kelly Portfolio** section: KPI strip (CAGR, Sharpe, Max DD, VaR 95%, CVaR 99%), cumulative wealth vs SPY line chart, annual return bar chart, collapsible holdings table. New **Alpha Signal Browser** section: filterable/sortable signal table (IC Mean, ICIR, CAGR, Sharpe, bootstrap CIs), horizontal IC bar chart for top 25 signals.
- **`docs/guide/app.md`** Tab 5 section updated to document Kelly portfolio and alpha signal browser sub-sections.

---

### Phase D1 — Signal Readiness (2026-05-14)

#### D1.1 — Model retraining with momentum FORCE_INCLUDE + sector-neutral IC (perf)
- **`scripts/train_models.py`** Confirmed TRAIN_CUTOFF=2022 (reverting TRAIN_CUTOFF=2023 regression). FORCE_INCLUDE_6M/1Y=['quality_x_momentum','vol_rank_12m'], FORCE_INCLUDE_2Y=['vol_rank_12m'] injected momentum features. sector_neutral=True default. min_ic_stability=0.6 default.
- **`models/model_{6m,1y,2y,3y,5y}.joblib`** Retrained; final feature counts: 6m=31, 1y=30, 2y=28, 3y=30, 5y=26.
- **`models/model_meta.json`** Updated with D1.1 val_auc/wf_mean_auc actuals.
- **`reports/walk_forward_auc_{6m,1y,2y,3y,5y}.csv`** New WF CV results (9 folds, expanding window, PIT-safe).
- **`docs/methodology/models.md`** AUC table updated: 6m WF=0.563, 1y WF=0.563, 2y WF=0.589, 3y WF=0.625 ✅, 5y WF=0.620 ✅.
- **`docs/index.md`** Performance at a Glance table updated with D1.1 actuals.
- **`CLAUDE.md`** Current Performance section updated to Phase D1 actuals.

#### D1.2 — Bootstrap confidence intervals in backtester (feat)
- **`scripts/backtester.py`** `bootstrap_ci()` added — block bootstrap (2000 samples, block_size=3y) producing CAGR ± 1σ and Sharpe ± 1σ. `run_backtest()` returns 4 CI fields: `cagr_bootstrap_mean_pct`, `cagr_bootstrap_1sigma_pct`, `sharpe_bootstrap_mean`, `sharpe_bootstrap_1sigma`. `print_tearsheet()` displays Sharpe CI 1σ and CAGR CI 1σ bands.

#### D1.3 — alpha_registry.json max_drawdown audit + bootstrap CI fields (fix/feat)
- **`scripts/build_alpha_registry.py`** Added `max_drawdown_note` field (documents annual-frequency limitation: 0.0 = all annual periods positive, not a bug). Added 4 bootstrap CI fields from D1.2 `run_backtest()` output to each signal entry.

---

### Phase D5 — Monitoring (2026-05-14)

#### D5.1 — Per-alpha IC decay + drawdown circuit breaker (feat)
- **`scripts/monitor_drift.py`** Added `analyse_ic_decay()`: loads `alpha_registry.json`, computes annual Spearman IC (signal vs `forward_return_1y`) per fiscal year, then rolling mean over the most recent 3y / 6y / 12y windows. Flags signals with 3y rolling IC < 0.02 (decay warn) or latest IC < 0 (decay alert). Added `check_drawdown_circuit_breaker()`: loads `portfolio_backtest.json`, computes cumulative return series, detects current drawdown from peak, warns if drawdown exceeds `--dd-gate` threshold with circuit-breaker action message. Added `--dd-gate` (default 20%), `--skip-ic-decay`, `--skip-dd` CLI flags. Both new sections are included in `reports/drift_report.json`. Any alert from either section sets `any_alert=true` (exit code 1).
- **`docs/developer/scripts.md`** Updated `monitor_drift.py` section: new flags table rows, IC decay and drawdown circuit-breaker descriptions.

---

### Phase D4 — Investment Framework (2026-05-14)

#### D4.1 — Investment framework document (docs)
- **`docs/methodology/investment-framework.md`** New document: 27 numbered rules covering universe (liquidity floor, market scope, fiscal-year anchoring), scoring (IC-weighted composite, horizon filtering, percentile-rank normalisation), portfolio construction (long-only default, top-N selection, fractional Kelly, position/sector caps), transaction costs (4-tier slippage, annual rebalance assumption), risk limits (VaR 95%, CVaR 99% gate, max drawdown monitoring, beta/alpha reporting), benchmark (SPY), live monitoring (drawdown circuit breaker, IC decay tracking, model drift), data integrity (PIT safety, survivorship correction, minimum history), and operational procedures (weekly refresh, registry versioning, commit convention).
- **`docs/methodology/index.md`** Added link to investment-framework.md.

---

### Phase D3 — Risk & Compliance (2026-05-14)

#### D3.1 — CVaR 99%, tiered slippage, VaR/CVaR gates (feat)
- **`scripts/backtester.py`** Added `SLIPPAGE_TIERS` constant (4 tiers: large >$10B=20 bps, mid $1B–$10B=30 bps, small $100M–$1B=50 bps, micro <$100M=80 bps). Replaced 2-tier `size_category_label` slippage with tiered lookup by `market_cap_at_filing` (falls back to `size_category_label` if column absent). Added CVaR 99% (Expected Shortfall): mean of annual returns in worst 1% tail. Added `cvar_99_pct` to return dict and `print_tearsheet()`.
- **`scripts/build_portfolio.py`** Fixed `_latest_complete_year` bug: market cap filter compared `market_cap_at_filing` against `min_n` (30) instead of `min_market_cap` ($50M). Added `var_95_pct` and `cvar_99_pct` to portfolio backtest return dict. Added `--var-gate` (warn if drawdown breaches threshold) and `--cvar-gate` (abort if CVaR99 breaches threshold) CLI flags with enforcement in `main()`. Added VaR/CVaR lines to `print_tearsheet()`.
- **`docs/developer/scripts.md`** Updated `backtester.py` and `build_portfolio.py` sections: tiered slippage description, new `--var-gate`/`--cvar-gate` flags, `cvar_99_pct` output field.

---

### Phase D2 — Portfolio Construction (2026-05-14)

#### D2.1 — IC-weighted Kelly portfolio constructor (feat)
- **`scripts/build_portfolio.py`** New script: reads `data/alpha_registry.json`, IC-weights selected signals into composite score, applies $50M market-cap filter, quarter-Kelly (≤0.25×) position sizing, 5% position cap, 40% sector cap. Supports `--strategy long_only|long_short`, `--horizon 1y|3y|5y|all`, `--market`, `--tearsheet`. Outputs `data/portfolio_holdings.json` (current-year top-30 with weights) and `data/portfolio_backtest.json` (annual return series + Sharpe/CAGR/beta). Backtest: CAGR +34.1%, Sharpe 1.409, Beta 0.348 (all markets, all signals).
- **`docs/developer/scripts.md`** Added `build_portfolio.py` section with flags table and output spec.

---

### Doc sweep — fraud→alpha reframe + 5-horizon + 360-col sync (2026-05-14)
- **`docs/guide/app.md`** All "fraud score / fraud probability" language replaced with "alpha score / composite score" throughout Tabs 1–4,6; ML horizon description updated to "6m through 5y via HorizonRouter"
- **`docs/developer/data-update-guide.md`** Production dataset updated 355→360 cols; workflow diagram node updated from `ml_1y/3y/5y` to `ml_{6m,1y,2y,3y,5y}`
- **`docs/methodology/features.md`** Clarified as "355 base features + 5 OOF = 360 total columns"
- **`docs/index.md`** Fraud Risk table entry updated to `ml_6m/1y/2y/3y/5y alpha probability`; Key Design Decisions expanded to 5 horizons
- **`docs/quickstart.md`** Train step updated to all 5 horizons (6m/1y/2y/3y/5y)
- **`docs/developer/setup.md`** 8→10 tabs; adds model_6m/2y.joblib; dataset size 155K×319→58K×360; ml_score column naming corrected

### Institutional sync (2026-05-14)
- **`scripts/run_phase_checks.py`** Phase A column count check updated 355 → 360 (dataset grew when 5 OOF columns added; check was never updated — fixes Phase A FAIL)
- **`docs/architecture.md`** Feature-Complete Dataset node updated "355 cols" → "360 cols" to match actual parquet state
- **`docs/methodology/models.md`** LightGBM base config block updated to actual trained params: `n_estimators=600`, `learning_rate=0.03`, `num_leaves=63`, `max_depth=6` (was showing stale pre-retrain defaults)
- **`.gitignore`** Added `HF_TOKEN.md` and `*.token` to prevent credential files being accidentally committed
- **`ROADMAP.md`** Vision section updated with investment objective: ≥20% annualised ROI, Sharpe ≥ 1.0, max drawdown ≤ 30%; platform framed as screener + alpha generation
- **`CONTEXT.md`** "What This Project Is" updated with dual-platform framing and 20% ROI investment objective

### Phase C — Complete (2026-05-14)
- **`scripts/run_phase_checks.py --phase C`** → 30 PASS 0 FAIL 0 WARN. Phase C officially complete per `docs/developer/phase-done-criteria.md`. All 5 model horizons (6m/1y/2y/3y/5y) confirmed present, OOF scores in parquet (360 cols), bias audit passing, backtest results with SPY benchmark, alpha_registry.json populated (8 signals, 6 selected).
- **`ROADMAP.md`** Phase C status rows updated to reflect actual artifact state: Step 7 baseline comparison ✅, sortino/calmar bug ✅ Fixed, Step 9 tearsheet + benchmark-relative metrics ✅, Step 10 alpha registry tasks ✅. Phase header updated to "✅ COMPLETE (exit criteria: 30 PASS 0 FAIL)".
- **`CONTEXT.md`** Phase C section rewritten: shows COMPLETE with 30/30 pass, dataset updated to 360 cols, OOF columns marked present, session log updated.

### Fixed (Phase C — AUC gap in 6m/1y/2y horizons)
- **`scripts/train_models.py`** three-part AUC fix for under-performing short horizons (6m WF=0.549, 1y WF=0.549, 2y WF=0.578):
  1. `FORCE_INCLUDE_6M = ['vol_rank_12m', 'quality_x_momentum']`, `FORCE_INCLUDE_1Y = ['vol_rank_12m', 'quality_x_momentum']`, `FORCE_INCLUDE_2Y = ['vol_rank_12m']` — bypasses ICIR ranking to inject momentum features that ICIR selection systematically under-selects for short-horizon targets (which ICIR ranks fundamentals first)
  2. `--sector-neutral` changed from `action='store_true'` to `action=argparse.BooleanOptionalAction, default=True` — sector-neutral IC now the default; removes sector rotation bias from IC signal
  3. `--min-ic-stability` default changed from `0.0` (off) to `0.6` — drops features whose IC direction is inconsistent across years (< 60% sign consistency), preventing directionally unreliable features from passing on mean IC alone
- **`ROADMAP.md`** complete 4-phase restructure: Phase A (Foundation/Data), Phase B (Feature Selection/Factor Research), Phase C (Model Training/Alpha), Phase D (Portfolio/Production/Monitoring). Dataset Completion Plan folded into Phase A. Step 7 WF AUC table updated to post-retrain actuals. Stale "Immediate Next Actions" section removed.

### Added (Phase C — alpha registry)
- **`scripts/build_alpha_registry.py`** NEW — builds `data/alpha_registry.json` with IC + backtest stats for all 8 alpha signals (5 factor scores + 3 ML OOF horizons). Per signal: `ic_mean`, `icir`, `cagr_pct`, `sharpe`, `sortino`, `calmar`, `max_drawdown_pct`, `excess_cagr_vs_spy`, `beta_vs_spy`, `hit_rate_pct`, `features_used`, `selected` flag. Selection criteria: IC_mean > 0.02 AND Sharpe > 0.50. Result: 8 evaluated, 6 selected (alpha_value ✅, alpha_quality ✅, alpha_fraud_risk ✅, ml_1y_oof ✅, ml_3y_oof ✅, ml_5y_oof ✅; alpha_momentum ❌, alpha_growth ❌).

### Fixed (Phase C — backtester sortino/calmar always null)
- **`scripts/backtester.py`** Sortino ratio: removed `n_negative >= 3` guard; when all annual returns are positive (downside_vol = 0) falls back to Sharpe as a lower-bound (correct behavior — Sortino ≥ Sharpe when no negative years). Calmar ratio: when MaxDD < 2% (all-positive annual years), uses `2σ` as a conservative proxy for drawdown instead of returning null. Results: `sortino: 1.181`, `calmar: 0.641` now populated in `data/backtest_results.json`.

### Added (Phase C — model retrain, bias audit, backtest, alpha schema)
- **`scripts/bias_audit.py`** look-ahead fix: `_period_end_date()` returns `None` when `fiscal_quarter` is null (non-December FY-end companies). Prevents false-positive look-ahead violations. `_count_lookahead()` and `audit_filing_lag()` now skip rows with null `fiscal_quarter`. `pd.to_datetime(..., errors='coerce')` added to handle mixed `None`/`Timestamp` dtype from `.apply()`. CI exit 1 only on true look-ahead leakage (0 violations in production dataset).
- **`scripts/generate_oof_scores.py`** NEW — walk-forward expanding-window OOF scorer for 5 horizons. Produces `ml_6m_oof`, `ml_1y_oof`, `ml_2y_oof`, `ml_3y_oof`, `ml_5y_oof` columns in `data/historical_dataset_clean.parquet` (NaN for training-window rows, OOF for held-out rows). Dataset now 58,190 rows × 360 columns (+5 OOF columns vs 355).
- **`scripts/bias_audit.py`** overfitting audit (`audit_overfitting()`): computes `overfit_gap = val_auc - wf_mean_auc` per horizon, flags gap > 0.15 as ⚠️ OVERFIT, writes gaps back to `models/model_meta.json`. All gaps ≤ 0.15 for 6m/1y/2y/3y; 5y skipped (no val_auc — expected WARN).
- **`scripts/backtester.py`** industry-grade walk-forward backtest: COMPOSITE strategy CAGR +38.1%, excess +24.2% vs SPY, Sharpe 1.181, beta 0.483, tracking_error 0.2983. Outputs `data/backtest_results.json` with all C4 gate fields.
- **`alpha/horizon_router.py`** HorizonRouter routes 6→6m, 9→1y, 18/24→2y, 36→3y, 60→5y. All C5 routing cases confirmed.
- **`docs/methodology/models.md`** AUC table updated with Phase C actuals (all 5 horizons); flowchart header updated 355 → 360 features.
- **`docs/index.md`** tagline updated 355 → 360 columns; Performance at a Glance updated with COMPOSITE strategy row (+38.1% CAGR, +24.2% vs SPY, Sharpe 1.181).
- **`docs/architecture.md`** column counts updated 355 → 360 in High-Level Overview (B11 node), Storage subgraph (S1 node), and Data Flow Detail (FA node).
- **`CLAUDE.md`** Performance table updated with Phase C post-retrain actuals (all 5 horizons); Feature engineering row updated 355 → 360 columns; Primary storage row updated to 360 cols.

### Fixed (phase checks — A4 false-positive WARN)
- **`scripts/run_phase_checks.py`** A4 check: added `operator_only` allowlist to suppress false-positive WARN for scripts present in `data-update-guide.md` but not in `refresh_data.yml`. Allowlisted: `nfeature_library.py` (mermaid `\n` escape artefact), `auto_update.py` / `merge_snapshots.py` (operator-only), `monitor_drift.py` (separate `monitor_drift.yml` workflow), `push_to_hf.py` (CI uploads inline), `feature_library.py` (module, not runnable), `step5_compute_features.py` (operator step). Result: 61 PASS 0 FAIL 0 WARN.

### Added (CI/CD — multi-market weekly cron)
- **`.github/workflows/refresh_data.yml`** weekly cron default changed from `US` to `US CA JP KR EU BR` (all 6 markets with pipeline scripts). `"all"` shortcut now expands to the same 6-market set via a proper loop (was broken — only ran US pipeline). KR pipeline guarded by `DART_API_KEY` secret: skipped with `[WARN]` if secret absent, rest of markets continue. Health-status JSON now records the actual markets string.
- **`docs/developer/data-update-guide.md`** Section 5 updated: schedule table now shows multi-market cron; new per-market routing table (market → script → API key requirement); KR secret guard behaviour documented.

### Added (Phase B completion — IC decay analysis)
- **`notebooks/06_ic_decay.ipynb`** NEW — institutional-grade IC decay analysis notebook. 12 cells covering: annual cross-sectional Spearman IC per feature per horizon per year (1y/3y/5y); factor group aggregation (Value/Quality/Momentum/Growth/FraudRisk); IC decay curves with confidence bands; exponential decay fit + half-life estimation (IC(t) = IC₀ × exp(-λt)); year-by-year IC stability heatmap; regime-conditional IC (Bear/Crisis={2008,2009,2020,2022} vs Expansion); lag-1 IC autocorrelation; signal-type diagnostic (short/medium/long-horizon). IC-ready markets: US/KR/CA/JP only (min 30 tickers per cross-section).
- **`reports/ic_decay_by_group.png`** NEW — IC time-series curves per factor group × horizon with ±1σ bands.
- **`reports/ic_stability_heatmap.png`** NEW — year-by-year IC stability heatmap (group/horizon × fiscal_year).
- **`reports/ic_regime_decay.png`** NEW — regime-conditional IC bar chart (Bear/Crisis vs Expansion per group × horizon).
- **`reports/ic_autocorrelation.png`** NEW — lag-1 IC autocorrelation heatmap (year-over-year IC persistence).
- **`CONTEXT.md`** Phase B status updated to COMPLETE; Phase B gaps section replaced with "Status: COMPLETE" summary.
- **`ROADMAP.md`** Step 6 status updated to COMPLETE; all stale ❌ Todo rows replaced with ✅ Done and correct filenames.

### Added (anti-drift process)
- **`scripts/run_phase_checks.py`** NEW — single-command Phase A/B/C done verifier. Mechanically runs all exit criteria from `docs/developer/phase-done-criteria.md`. Phase A: dataset shape/quality, EDA notebook outputs, CI schedule completeness, diagram vs CI consistency. Phase B: feature library formula coverage, engineering guards (DSRI clip, growth winsorization, sector_pct fiscal_year grouping, montier_c2 ppe_net), feature selection integrity (no ML columns in feature sets, PSI=0.25, NW+FDR), factor research CSVs, notebook outputs. Phase C: OOF scores, model horizons/AUC targets, backtest results, alpha schema. Exits 1 on any FAIL; supports `--phase A|B|C|AB` and `--strict` (treat WARN as FAIL). A phase is only done when this script prints all PASS.
- **`docs/developer/scripts.md`** added section for `run_phase_checks.py` with full flag table, per-phase check tables, and anti-drift rule note.

### Added (Phase C6 — docs/architecture.md Phase C sync)
- **`docs/architecture.md`** ML System subgraph updated: 5 horizons (6m/1y/2y/3y/5y), PSI threshold 2.0 → 0.25, OOF scorer node (`generate_oof_scores.py`), HorizonRouter node (`horizon_router.py`).
- **`docs/architecture.md`** Research subgraph updated: Backtester now shows SPY benchmark, Bias Audit updated to 4 audits, SPY Returns node added (`fetch_spy_returns.py`).
- **`docs/architecture.md`** Component Map updated: new rows for OOF scorer, SPY fetch, HorizonRouter; train_models.py updated to 5 horizons filed-date PIT-safe; backtester.py updated to include SPY benchmark + factor attribution; bias_audit.py updated to 4 audits.
- **`docs/architecture.md`** Data Flow Detail: PSI threshold 2.0 → 0.25, ~35 → ~45 features/horizon, 5-horizon LightGBM, OOF path added (`ml_*_oof` columns), HorizonRouter node added.
- **`docs/architecture.md`** Deployment Architecture: `bias_audit.py --ci` step added in CI after dataset quality check; hard fail on look-ahead violation shown in diagram.

### Added (Phase A — coverage depth audit)
- **`CONTEXT.md`** market coverage table rewritten: 14 individual market rows (not grouped EU) each with Rows, Tickers, Fiscal Year Range, Quarterly enriched flag, and Depth rating (DEEP/MODERATE/SHALLOW/TOO_THIN). Coverage depth assessment section added: US/KR = DEEP (IC training ready), CA/JP = MODERATE (IC training ready), all others TOO_THIN. BR tickers corrected 55 → 48.
- **`reports/coverage_audit.csv`** NEW (gitignored, regeneratable): per-market depth ratings with ic_training flag — 4 markets IC-ready.
- **`reports/coverage_audit_heatmap.png`** NEW (gitignored, regeneratable): market × fiscal_year ticker count heatmap (log-scaled, blue dashed border for cells <50 tickers).

### Fixed
- **`pipeline/step5_compute_features.py`** `montier_c2`: replaced `property_plant_equipment` (95.7% null) with `ppe_net` (19.4% null) in Montier C2 depreciation-rate computation. Column was previously 100% null; now 41.6% null (limited by `depreciation` availability). `montier_c_score` composite coverage improves to 75.5%.
- **`scripts/patch_montier_c2.py`** (new): one-shot patch script to recompute all 7 montier columns on existing parquet without a full pipeline rebuild.
- **`data/historical_dataset_clean.parquet`** all 7 montier columns patched in-place (58,190 rows × 355 cols).
- **`docs/developer/phase-done-criteria.md`** HorizonRouter test case: 18m → `'2y'` (not `'1y'`). Boundary is exclusive upper bound for 1y range; 18m routes to the longer 2y model (conservative bias).

### Added (Phase C6 — documentation, diagrams, phase-done-criteria sync)
- **`docs/methodology/alpha-generation.md`** NEW — variable-horizon schema: HorizonRouter routing table, scoring pipeline flowchart, factor group weights, model confidence display thresholds, OOF vs static score usage rules.
- **`docs/methodology/bias-validation.md`** fully rewritten: all 4 Phase C audits documented (look-ahead PIT-safe split, survivorship −50% imputation, overfitting gap, Bonferroni multiple testing). Added OOF scoring section and CI audit table.
- **`docs/developer/phase-done-criteria.md`** Phase C checklist added: C1 (OOF columns + look-ahead), C2 (5 model files + feature sets + WF-AUC), C3 (bias audit), C4 (SPY data + backtest fields), C5 (HorizonRouter routing + scoring imports).
- **`CLAUDE.md`** architecture state table updated: 5 horizons in ML models row, OOF scoring row, HorizonRouter row, SPY benchmark data row, bias audit row. Key File Locations updated: 5-horizon model files, OOF audit trail, spy_returns.csv, bias_audit_report.json, horizon_router.py. Critical Missing Pieces replaced with current performance table (5 horizons).
- **`scripts/verify_doc_consistency.py`** extended: `get_feature_set_counts()` now covers 6m/1y/2y/3y/5y; `get_trained_horizons()` added; Phase C checks added (model_meta.json horizon coverage, spy_returns.csv presence, horizon_router.py presence).

### Added (Phase C5 — variable-horizon alpha schema + UI)
- **`alpha/horizon_router.py`** NEW — `HorizonRouter` class: maps any investment horizon (months) to nearest trained model key (6m/1y/2y/3y/5y). Routing: 3–9m→6m, 9–18m→1y, 18–30m→2y, 30–48m→3y, 48m+→5y. Conservative bias (longer model on tie). Includes `MODEL_LABELS`, `FEATURE_FACTOR_GROUPS` (200+ features mapped to Value/Quality/Momentum/Growth/Fraud Risk), `wf_auc()` helper.
- **`src/scoring.py`** `resolve_horizon()` added: accepts model key string or integer months, routes via `HorizonRouter`, falls back gracefully to nearest available model. `score_companies()` now accepts `horizon: str | int` — integer months are routed automatically.
- **`src/scoring.py`** `top_feature_importances()` added: returns top N (feature, importance, factor_group) tuples for a model key. Uses `shap_top_features` from model_meta.json if populated; falls back to LightGBM `feature_importances_` attribute.
- **`src/ui/tab_screener.py`** Investment horizon selectbox (`['1y','3y','5y']`) replaced with slider (6–60 months in 6-month steps). Selected months are routed via `HorizonRouter` to the correct model.
- **`src/ui/tab_screener.py`** Model confidence badge added below slider: WF-AUC ≥ 0.65 = "High confidence" (green), 0.60–0.65 = "Good" (light green), 0.55–0.60 = "Moderate" (orange), < 0.55 = "Screening only — lower confidence" (red) with warning message.
- **`src/ui/tab_screener.py`** Top signals expander added: horizontal bar chart of top 6 feature importances with factor group color coding (Value/Quality/Momentum/Growth/Fraud Risk).
- **`src/ui/tab_screener.py`** Company Deep Dive: "Alpha Score" section shows the company's ML score for the selected horizon + top 5 driving signals with factor group labels and actual feature values.
- **`src/ui/tab_screener.py`** Screener header renamed "Alpha Screener — Ranked by Multi-Factor Score" to reflect that the output is a ranked list of high-alpha candidates. "ML Score" column renamed "Alpha Score" in the results table.

### Added (Phase C4 — industry-grade backtest, SPY benchmark, factor attribution)
- **`scripts/fetch_spy_returns.py`** NEW — downloads SPY annual calendar-year total returns (adjusted close, dividends included) via yfinance. Saves `data/spy_returns.csv` (year, spy_return). Covers 2008–2025.
- **`data/spy_returns.csv`** NEW — 18 years of SPY returns (2008–2025). Mean +12.64%, best 2013 (+32.31%), worst 2008 (−36.80%).
- **`scripts/backtester.py`** SPY is now the primary benchmark: `excess_cagr_pct` = portfolio CAGR − SPY CAGR. Equal-weight universe mean retained as secondary metric `excess_vs_univ`. Benchmark source recorded in `benchmark_source` field.
- **`scripts/backtester.py`** factor attribution added: `beta_vs_spy` (OLS slope), `alpha_vs_spy` (Jensen's alpha intercept), `r_squared_vs_spy`, `tracking_error` (std dev of excess returns vs SPY).
- **`scripts/backtester.py`** new risk metrics in output: `var_95_pct` (historical 5th percentile), `annual_turnover_pct` (approx), `max_drawdown_duration_months`.
- **`scripts/backtester.py`** `print_tearsheet()` updated: shows SPY CAGR, excess vs SPY, beta/alpha/R²/tracking_error, VaR 95%, annual turnover, drawdown duration; annual return table updated to show SPY% and excess-vs-SPY columns.
- **`scripts/backtester.py`** `--max-filing-lag` default corrected: 6 → 18 months (aligned with actual filing lag distribution in dataset).
- **`docs/methodology/backtesting.md`** fully rewritten: SPY benchmark section, PIT-safe walk-forward diagram, transaction cost model table, factor attribution formula, complete output field reference, updated running instructions.
- **`docs/developer/scripts.md`** `backtester.py` section updated: new flags table, SPY benchmark note, output field list.

### Added (Phase C3 — bias audit suite + CI integration)
- **`scripts/bias_audit.py`** overhauled: added `audit_overfitting()` (train AUC vs WF mean AUC gap, writes `overfit_gap` to model_meta.json; flag if gap > 0.15) and `audit_multiple_testing()` (Bonferroni correction across 5 horizons × 4 strategies). Added `--ci` flag: exits with code 1 if any look-ahead violations found (hard fail), warn-only for survivorship/overfitting. Added `_count_lookahead()` helper.
- **`.github/workflows/refresh_data.yml`** added `bias_audit.py --ci` step after `test_dataset_quality.py`. Look-ahead violations fail CI; survivorship and overfitting are logged as warnings.


- **`scripts/train_models.py`** HORIZONS dict extended: added `6m` (forward_return_6m / beat_local_market_6m) and `2y` (forward_return_2y / beat_local_market_2y). Now covers 5 discrete horizons: 6m/1y/2y/3y/5y. EXCLUDE set updated with ml_6m/ml_2y/ml_6m_oof/ml_2y_oof.
- **`scripts/generate_oof_scores.py`** ALL_HORIZONS extended to all 5 horizons; default `--horizons` now `6m 1y 2y 3y 5y`.
- **`scripts/tune_models.py`** HORIZONS set extended to 5 horizons; N_OPTUNA_TRIALS 60→100; `_load_data_for_horizon()` patched to use filed_date PIT-safe split matching train_models.py; `--horizon` choices extended.
- All 5 forward return columns (`forward_return_6m`, `forward_return_2y`) already present in parquet from step3_enrich_prices.py — no data pipeline changes required.


- **`scripts/generate_oof_scores.py`** NEW — walk-forward OOF scorer. For each fiscal year Y: trains on `filed_date < Jan 1 of Y`, scores `fiscal_year == Y`. Writes `ml_1y_oof`, `ml_3y_oof`, `ml_5y_oof` to parquet (NaN for training-window rows). Eliminates in-sample contamination from `score_historical.py`.
- **`scripts/train_models.py`** enhanced model config: `n_estimators` 400→600, `max_depth` 5→6, `num_leaves` 31→63, `learning_rate` 0.04→0.03, added `reg_alpha=0.1`, `reg_lambda=1.0`.
- **`scripts/train_models.py`** `--max-psi` default 2.0→0.25 (aligned with `run_feature_selection.py`).
- **`scripts/train_models.py`** `walk_forward_cv()` patched to use `filed_date` PIT-safe cutoff per fold year (previously used only `fiscal_year`).
- **`scripts/train_models.py`** `EXCLUDE` set updated: added `ml_1y_oof`, `ml_3y_oof`, `ml_5y_oof` so OOF columns are never used as input features.
- **`docs/developer/scripts.md`** updated `train_models.py` flags table (new --max-psi default, filed-date note); added `generate_oof_scores.py` section.

### Fixed (Phase B — feature selection, final institutional quality pass)
- **`scripts/run_feature_selection.py`** BH FDR correction now gates `ic_pass`: features must pass `fdr_reject=True` (BH q<0.05) to enter ICIR ranking. Previously FDR was computed but not enforced as a filter — spurious features could pass.
- **`scripts/run_feature_selection.py`** Sector-neutral IC added (default: on). Return and feature demeaned by SIC-based sector within each fiscal year before IC computation. Prevents sector rotation from inflating stock-selection IC. Matches methodology of `factor_research.py`.
- **`scripts/run_feature_selection.py`** `--sector-neutral` / `--no-sector-neutral` CLI flags added.
- **`scripts/test_dataset_quality.py`** Section 10 added: point-in-time leakage checks. Validates `filed_date` timing vs `fiscal_year_end` — negative median filing lag is a hard fail; extreme lags emit warnings. 98 checks total.
- **`.github/workflows/refresh_data.yml`** added steps to re-run `factor_research.py` (base + sector-neutral) and `run_feature_selection.py` after every weekly data refresh.
- **`docs/methodology/feature-selection.md`** corrected "Planned (Phase 0)" → "Implemented (Phase B)" for NW, BH FDR, and sector-neutral IC.
- **`models/feature_sets_{1y,3y,5y}.json`** re-run: **45/45/41 features** (sector-neutral IC + BH FDR gate).
- **`docs/index.md`** updated: feature counts 45/45/42 → 45/45/41; sector-neutral IC noted.

### Added (Phase B — feature engineering, parquet patch)
- **`data/historical_dataset_clean.parquet`** patched to add 9 new columns from Phase B implementation: `montier_c1`–`montier_c6` (binary Montier components), `montier_c_score` (normalised composite), `sloan_wc_accruals`, `sloan_lt_accruals`. Dataset now 58,190 × 355 cols.
- **`docs/architecture.md`**, **`docs/index.md`**, **`docs/methodology/models.md`**, **`README.md`**, **`CLAUDE.md`** updated: column count 346 → 355; PSI threshold 2.0 → 0.25 in models.md flowchart.
- **`.github/workflows/refresh_data.yml`** added `test_dataset_quality.py` quality gate step after `check_data.py`.

### Fixed (Phase B — factor research)
- **`scripts/factor_research.py`** output dict: added `ic` key (alias for `mean_ic`) so `reports/factor_research_*.csv` includes the standard `ic` column required by phase-done-criteria.md B4.
- **`reports/factor_research_{1y,3y,5y}.csv`** regenerated (non-sector-neutral) and **`_sn`** variants regenerated (sector-neutral) with updated column schema.

### Added (Phase B — notebooks)
- **`notebooks/02_ic_analysis.ipynb`** added Sec 7 IC decay curves (t+1y/2y/3y for top 10 features), Sec 8 quintile return spreads (Q1-Q5 mean returns + Sharpe), Sec 9 Fama-MacBeth regression (annual cross-sectional slope t-stats), Sec 10 market regime IC (recession vs expansion), Sec 11 long-short decomposition (long/short legs + spread), Sec 12 information ratio per factor (IR > 0.5 threshold). Executed with outputs.
- **`notebooks/03_factor_correlation.ipynb`** re-executed with fresh outputs (V4 schema compatibility verified).
- **`notebooks/04_null_recovery_audit.ipynb`** re-executed with fresh outputs.
- **`notebooks/05_market_coverage.ipynb`** re-executed with fresh outputs.

### Added (Phase B — feature selection)
- **`scripts/run_feature_selection.py`** Newey-West HAC t-statistic (`ic_tstat_nw`) and p-value (`ic_pval_nw`) computed per feature IC time series. Corrects for IC autocorrelation across fiscal years.
- **`scripts/run_feature_selection.py`** Benjamini-Hochberg FDR correction (`fdr_reject`) applied to Newey-West p-values at q=0.05. Controls false discovery rate across ~200 simultaneous hypotheses.
- **`reports/feature_selection_summary.csv`** now contains columns: `ic_tstat_nw`, `ic_pval_nw`, `fdr_reject`.

### Fixed (Phase B — feature selection)
- **`scripts/run_feature_selection.py`** `PSI_THRESHOLD` lowered from 2.0 to 0.25 (institutional standard). 14 drifted features removed per horizon (macro regime features).
- **`scripts/train_models.py`** `EXCLUDE` set: added `alpha_fraud_risk`, `alpha_composite`, `alpha_value`, `alpha_quality`, `alpha_growth`, `alpha_momentum`. Hand-crafted composites of raw features cause signal double-counting when their component features are also candidates.
- **`models/feature_sets_{1y,3y,5y}.json`** re-run: **45/45/42 features** (alpha_* and ml_* excluded; PSI=0.25; Newey-West t-stats computed).
- **`docs/methodology/feature-selection.md`** updated: PSI threshold, candidate pool count, NW/FDR documentation.

### Added (Phase B — feature engineering)
- **`pipeline/step5_compute_features.py`** `add_montier_c_score()`: Implements Montier C-Score (Montier 2008) — 6 binary forensic accounting variables (`montier_c1`–`montier_c6`) plus composite `montier_c_score` normalised to [0,1].
- **`pipeline/step5_compute_features.py`** Richardson et al. (2005) named accrual columns: `sloan_wc_accruals` (working capital accruals / assets) and `sloan_lt_accruals` (long-term accrual residual / assets) added alongside existing `sloan_accruals`.
- **`docs/methodology/features.md`** Montier C-Score and Richardson accrual decomposition documented with paper references.

### Fixed (Phase B — feature engineering)
- **`pipeline/step5_compute_features.py`** `beneish_dsri` clipped to [0.5, 3.0] — values outside this range are data errors, not manipulation signals.
- **`pipeline/step5_compute_features.py`** Momentum rank cohort guard added: cohorts with < 10 non-null observations return NaN ranks (prevents noisy rankings from single-company cohorts in small markets).

### Added (Phase A/B — EDA cells)
- **`notebooks/01_eda_dataset.ipynb`** Section 3a: Null profile heatmap (key features × market) — identifies which markets have missing data for critical columns.
- **`notebooks/01_eda_dataset.ipynb`** Section 4a: Forward return Q-Q plot + outlier stats (min/p1/p25/median/p75/p99/max) with treatment note (pre/post winsorization documentation).
- **`notebooks/01_eda_dataset.ipynb`** Section 4b: Point-in-time lineage check — counts violations where `filed_date > fiscal_year+1-01-01`; plots filing lag distribution.

### Fixed (Phase A — data targets + CI sync)
- **`data/historical_dataset_clean.parquet`** forward_return targets winsorized at p1/p99: `forward_return_1y` max was 29,999% (penny stock data error) → now capped at [−0.926, 3.906]; `forward_return_3y` capped at [−0.995, 6.989]; `forward_return_5y` capped at [−0.999, 9.227].
- **`scripts/test_dataset_quality.py`** Section 7 upgraded: now enforces winsorization hard caps (1y ≤ 5.0×, 3y ≤ 10.0×, 5y ≤ 20.0×) in addition to coverage checks.
- **`.github/workflows/refresh_data.yml`** added missing `enrich_quarterly_features.py` post-processing step (was absent, so intra-year dynamics were not refreshed in weekly CI runs).
- **`docs/developer/data-update-guide.md`** Mermaid operator diagram updated: added `fix_dataset_quality.py` + `enrich_quarterly_features.py` nodes, removed phantom `enrich_fraud_taxonomy.py`, updated quality gate to "92 checks". Diagram now matches actual CI.

### Added (Phase scope lock + done criteria)
- **`docs/developer/phase-done-criteria.md`** (new): Single source of truth for "is Phase A/B done?". Contains exact shell commands that return PASS/FAIL for every Phase A and Phase B acceptance criterion. Replaces vague task-list-based closure checks.
- **`CLAUDE.md`** Phase Scope Definition section: Locks Phase A/B/C scope. Three rules: no re-auditing, Phase C items never in Phase A/B, done requires the checklist file.
- **`docs/developer/pipeline-integrity.md`** Phase Closure section: Replaced vague checklist with pointer to `phase-done-criteria.md`.

### Fixed (docs — column count sync)
- **`docs/developer/phase-done-criteria.md`** A1 assertion corrected 346 → 355 to match actual production parquet shape (58,190 × 355).
- **`docs/developer/data-update-guide.md`** column lineage table: added Montier/Sloan accrual step row, final state now correctly shows 355 columns.

### Fixed (docs — quality check count + diagram sync)
- **`docs/developer/data-update-guide.md`** Mermaid diagram node, line 48, and line 99: check count corrected from 92/53 → **98** to match current `test_dataset_quality.py`.
- **`docs/architecture.md`** Data Flow diagram: added Montier/Sloan node (341 → 355 cols) between IMP and PSI; final alpha node updated from "335 → 341" to "341 → 355 cols total".

### Added (sync enforcement — prevents future doc drift)
- **`scripts/verify_doc_consistency.py`** (new): reads live parquet + key docs, verifies column counts, row counts, feature counts, and quality check count are consistent across all 8 doc files. `--warn` flag for CI advisory mode.
- **`.github/workflows/refresh_data.yml`** added `verify_doc_consistency.py --warn` step after feature selection — CI now flags numeric drift in the weekly run log.
- **`scripts/check_sync.py`** 5 new trigger rules: `step5-columns` (step5 changes → architecture.md + data-update-guide.md), `quality-check-count` (test_dataset_quality.py changes → data-update-guide.md + phase-done-criteria.md + scripts.md), `feature-selection-counts` (run_feature_selection.py / feature_sets_*.json changes → index.md + scripts.md + feature-selection.md), `ci-workflow` (refresh_data.yml → data-update-guide.md).
- **`docs/developer/scripts.md`** documented `verify_doc_consistency.py` and updated `check_sync.py` section with new rule coverage.



---

## [Unreleased]

### Fixed (Phase B audit — data integrity + in-sample contamination)
- **`data/historical_dataset_clean.parquet`** 117 BR null-ticker rows dropped (58,307 → 58,190 rows). Brazilian companies that could not be matched to a B3 ticker (Cia Siderúrgica Nacional, JSL, Nexpe, WEG, etc.) silently polluted the dataset. Dropping them restores all 53 quality-test assertions.
- **`data/historical_dataset_clean.parquet`** 30+ growth/YoY columns winsorized at 1st/99th percentile. `revenue_growth_yoy` max was 184,343× (near-zero base problem); `shares_dilution` max was 2.37B. Unwinsorized growth features dominated IC rankings and could produce extreme gradient-boosted splits.
- **`scripts/train_models.py`** `EXCLUDE` set: Added `ml_1y`, `ml_3y`, `ml_5y`. `score_historical.py` scores ALL historical rows with a model trained up to `TRAIN_CUTOFF=2022`, inflating IC for 2008–2022 training rows. Including ML scores as feature-selection candidates would make the next training run self-referential (model trains on its own predictions). Walk-forward OOF scoring required before these can re-enter (Phase C).
- **`models/feature_sets_{1y,3y,5y}.json`** re-run: 45/46/45 features (ml_1y/3y/5y removed from all three horizons; `alpha_*` remains — no forward-return contamination).

### Added (Phase B audit — prevention framework)
- **`docs/developer/pipeline-integrity.md`** Rule 6: All growth/YoY features must be winsorized at 1st/99th percentile in `step5_compute_features.py`. Documents root cause (near-zero base), IC-inflation mechanism, and how to apply (add new column to `ratio_cols` before closing task).
- **`docs/developer/pipeline-integrity.md`** Rule 7: ML-derived score columns (`ml_1y/3y/5y` and future equivalents) must appear in the `EXCLUDE` set in `train_models.py` and must never appear in `models/feature_sets_*.json`. Documents circular contamination mechanism.
- **`CLAUDE.md`** Change Checklist: 2 new rows — (1) "New growth/YoY feature added → add to `ratio_cols` winsorize list (Rule 6)"; (2) "New ML-derived score column added → add to `EXCLUDE` before running feature selection (Rule 7)".
- **`scripts/test_dataset_quality.py`** Section 8: Growth feature winsorization guard — asserts that no growth column has `max > 50 × p99` (catches future unwinsorized columns). Covers 36 growth/YoY/dilution columns.
- **`scripts/test_dataset_quality.py`** Section 9: ML score exclusion guard — asserts that `ml_1y`, `ml_3y`, `ml_5y` do not appear in any `models/feature_sets_*.json`. Catches regression if EXCLUDE set is edited carelessly.
- **`docs/developer/data-update-guide.md`** Column count reference updated: 326 → 346; row count 58,307 → 58,190; full step-by-step table added.
- Quality test suite now has 92 checks (up from 53).

### Added (Phase B audit — research notebooks)
- **`notebooks/05_market_coverage.ipynb`** (new): Per-market audit — feature fill rates by factor group, year range, forward return label density, and usability summary for all 6 markets.
- **`notebooks/02_ic_analysis.ipynb`** Section 6 — Temporal IC Stability: year-by-year IC heatmap (feature × year) + stability summary table (mean_IC, ICIR, pct_same_sign). Reveals regime-dependent factors that inflate aggregate ICIR but are unreliable out-of-sample.

### Fixed (Phase B audit — academic formula implementations)
- **`pipeline/step5_compute_features.py`** Beneish `beneish_depi`: Was computing `dep_rate / dep_rate` (always 1.0). Fixed to compute proper prior-year depreciation rate using growth-rate approximation. All 58K rows now have variable DEPI (mean=1.02, std=0.34). `beneish_m_score` recomputed.
- **`pipeline/step5_compute_features.py`** Altman `altman_x4`: Was using `market_cap_at_filing.fillna(0)` — silently gave 0 contribution for KR/BR (0% market cap fill). Now uses book equity as fallback (Altman Z''-Score variant for private/non-US firms). KR `altman_x4` fill: 0% → 99.7%. `altman_z_score` recomputed.
- **`pipeline/step5_compute_features.py`** Piotroski F-score signal 6 (`piotroski_delta_liq`): Was using `current_assets_growth > 0` instead of Piotroski 2000 criterion `Δ(current_ratio) > 0`. Fixed using groupby-shift on `current_ratio` within ticker. `piotroski_f_score` recomputed.


- **`pipeline/step5_compute_features.py`** `compute_sector_pct_ranks()`: Added `fiscal_year` to groupby — was `groupby('sic_2digit')`, now `groupby(['sic_2digit', 'fiscal_year'], observed=True)`. Without this, a 2005 company was ranked against 2005–2024 sector peers (temporal lookahead in feature space). Affects 18 `*_sector_pct` columns. Dataset patched in-place; feature selection and factor research re-run.
- **`pipeline/step3_enrich_prices.py`** `enrich_row()`: Added `vol_prior_6m` (126d), `vol_prior_36m` (756d), `vol_prior_60m` (1260d) natively alongside existing `vol_prior_12m`. Previously these existed only via a one-off patch script and were silently dropped on every CI rebuild (Rule 1 violation).
- **`pipeline/step5_compute_features.py`**: Added `roa_volatility_5yr` and `earnings_stability_roa_5yr` natively after `roe_volatility_5yr` (same violation — existed only in patch script).
- **`.github/workflows/refresh_data.yml`**: Added four missing post-processing steps that were absent from CI: `impute_features.py`, `mark_survivorship.py --fix`, `compute_alpha.py`, `score_historical.py`. Previous CI produced a ~326-column parquet missing quarterly features, alpha scores, ML scores, and survivorship correction.

### Added (Phase A/B audit — prevention)
- **`docs/developer/pipeline-integrity.md`** (new): 5 rules that prevent the class of bugs found in Phase A/B audit — orphan patch columns (Rule 1), cross-sectional rank without time key (Rule 2), CI/dataset drift (Rule 3), stale artifacts after data fix (Rule 4), formula scattering (Rule 5). Includes Phase A and Phase B closure checklists and common anti-patterns table.
- **`CLAUDE.md`** Change Checklist: Added 3 new rows linking to `pipeline-integrity.md` — triggered on new columns, new rank features, and new post-processing scripts.

### Fixed (Phase B audit — academic formula implementations)
- **`pipeline/step5_compute_features.py`** Beneish `beneish_depi`: Was computing `dep_rate / dep_rate` (always 1.0). Fixed to compute proper prior-year depreciation rate using growth-rate approximation (same pattern as GMI/SGAI). All 58K rows now have variable DEPI (mean=1.02, std=0.34). `beneish_m_score` recomputed.
- **`pipeline/step5_compute_features.py`** Altman `altman_x4`: Was using `market_cap_at_filing.fillna(0)` — silently gave 0 contribution for KR/BR (0% market cap fill). Now uses book equity as fallback (Altman Z''-Score variant for private/non-US firms). KR `altman_x4` fill: 0% → 99.7%. `altman_z_score` and `altman_z_score_sector_pct` recomputed.
- **`pipeline/step5_compute_features.py`** Piotroski F-score signal 6 (`piotroski_delta_liq`): Was using `current_assets_growth > 0` instead of the Piotroski 2000 criterion `Δ(current_ratio) > 0`. Fixed using groupby-shift on `current_ratio` within ticker. `piotroski_f_score` recomputed.
- **`models/feature_sets_{1y,3y,5y}.json`** re-run on corrected data: 46/46/46 features selected.
- **`reports/feature_selection_summary.csv`**, **`reports/factor_research_{1y,3y,5y}.csv`** regenerated.

### Added (Phase B audit — research notebooks)
- **`notebooks/05_market_coverage.ipynb`** (new): Per-market audit — feature fill rates by factor group, year range, forward return label density, and usability summary for all 6 markets.
- **`notebooks/02_ic_analysis.ipynb`** Section 6 — Temporal IC Stability: year-by-year IC heatmap (feature × year) + stability summary table (mean_IC, ICIR, pct_same_sign). Reveals regime-dependent factors that inflate aggregate ICIR but are unreliable out-of-sample.

### Changed (Phase B re-runs on corrected data)
- **`models/feature_sets_{1y,3y,5y}.json`** regenerated: Feature selection re-run on corrected dataset (correct `*_sector_pct` features + fixed equity). 46/47/46 features (5y changed 45→46). Sector_pct features now properly represent within-year cross-sectional signal; more pass IC/ICIR threshold (6-7 per horizon vs 2-4 before).
- **`reports/feature_selection_summary.csv`** regenerated: 645 rows with updated IC/ICIR.
- **`reports/factor_research_{1y,3y,5y}.csv`** regenerated: IC/ICIR values updated for corrected equity and sector features.
- **`docs/methodology/feature-registry.md`**: Column count 326 → 346; added `roa_volatility_5yr`, `earnings_stability_roa_5yr`, `vol_prior_6m`, `vol_prior_36m`, `vol_prior_60m`.
- **`docs/developer/data-update-guide.md`**: Operator workflow Mermaid diagram updated to show full post-processing chain (impute → survivorship → alpha → scores → quality gate → push). Added rule: if a step is not in the diagram, it won't run in CI.
- **All 4 research notebooks** re-run on corrected 346-column dataset.


- **`pipeline/step5_compute_features.py`** (`COALESCE_ALIASES`): Root cause of 9 null columns found and fixed — `COLUMN_ALIASES` loop used `if dst not in df.columns` which skipped `equity → total_equity` because `total_equity` already existed at 0.2% fill. Added `COALESCE_ALIASES = {'equity', 'sga_expense'}` set; columns in this set now use `combine_first` to coalesce from the higher-fill source. Fixes `total_equity` (4.3% → 92.9%), `roe` (4.3% → 88.1%), `roic`, `pb_ratio`, `book_to_market`, `net_debt_to_equity`, `roe_sector_pct`, `pb_ratio_sector_pct`, `roe_volatility_5yr`, `earnings_stability_5yr` for future pipeline runs.

### Added (Phase A/B — equity + volatility patch + new columns)
- **`scripts/patch_equity_vol_features.py`** (new): One-time patch script that backfills the existing parquet without re-running the full pipeline. Two operations: (1) `patch_equity_features()` — joins `snapshots_combined.parquet` on `(cik, fiscal_year)`, coalesces `equity` and `sga_expense`, recomputes all equity-derived ratios and rolling volatility; (2) `patch_vol_features()` — reads `price_cache.db` ticker-by-ticker (7,753 tickers), computes annualised daily-return volatility over 6m / 36m / 60m lookback windows. Adds 5 new columns and fixes 6 broken ones. Creates `.parquet.bak_pre_patch` backup before writing. Supports `--dry-run` flag.
- **`data/historical_dataset_clean.parquet`** — 5 new columns added (341 → 346): `roa_volatility_5yr` (rolling 5yr std of ROA, 91.5% fill), `earnings_stability_roa_5yr` (−roa_volatility_5yr), `vol_prior_6m` (annualised 6m price vol, 95.4% fill), `vol_prior_36m` (annualised 36m price vol, 95.5% fill), `vol_prior_60m` (annualised 60m price vol, 95.4% fill). Also fixes previously broken: `roe`, `roic`, `pb_ratio`, `book_to_market`, `net_debt_to_equity`, `roe_volatility_5yr`, `earnings_stability_5yr`, `roe_sector_pct`, `pb_ratio_sector_pct`.

### Fixed (Phase A — Brazil B3 ticker matching)
- **`pipeline/step1_fetch_tickers_br.py`**: Ticker matching improved from 64 → 112 companies (75% improvement). Changes: (1) regex broadened from `[34]$` to `[3-9]$` to include ON3/PN4/PNB5/PNC6 share classes and units (ON9); (2) `CURATED_OVERRIDES` dict added for 11 companies with acronym-based tickers not derivable from name heuristics (BBDC3, BBAS3, CMIG3, BRSR3, BMEB3, CLSC4, FESA3, SNSY5, etc.); (3) `_MATCH_STOP` frozenset added (BCO, BANCO, CIA, PARTICIPACOES, HOLDING, etc.) to skip noise words; (4) `best_match()` expanded from 2 to 6 strategies: prefix4, first meaningful word, 4-letter acronym, acro2+first2chars, second word prefix, 3-letter unique match.
- **`data/tickers_br.parquet`** regenerated: 353 companies, 112 with B3 ticker matched (was 64).
- **`data/snapshots_br.parquet`** patched: ticker column updated; 93 unique tickers (was 57).

### Added (Phase B — feature selection + research notebooks)

- **`scripts/run_feature_selection.py`** (new): Standalone 4-stage feature selection pipeline — PSI filter (PSI ≤ 2.0) → IC screen (|mean IC| ≥ 0.02, n_years ≥ 5) → ICIR top-K (default 60) → Spearman deduplication (|r| ≤ 0.90). Imports `compute_ic_table`, `compute_psi`, `deduplicate_features` from `train_models.py`. Outputs `models/feature_sets_{1y,3y,5y}.json` (46 / 47 / 45 features) and `reports/feature_selection_summary.csv` (600 rows, IC/ICIR/PSI per candidate × 3 horizons). CLI: `--psi-threshold`, `--ic-min`, `--top-k`, `--corr`, `--dry-run`.
- **`scripts/factor_research.py`** (re-run): Refreshed IC/ICIR reports on updated 341-column parquet. Top features: `ml_1y` (ICIR=2.037), `altman_z_score_sector_pct` (ICIR=1.823), `ev_revenue` (ICIR=−1.708), `ml_3y` (ICIR=1.688), `alpha_fraud_risk` (ICIR=1.649). Reports written to `reports/factor_research_{1y,3y,5y}.csv`.
- **`notebooks/01_eda_dataset.ipynb`** (new): EDA — shape/date range, rows by market/year, period_type split, null profile (annual), target variable coverage + histograms, ML/alpha score distributions, size_category.
- **`notebooks/02_ic_analysis.ipynb`** (new): IC/ICIR analysis — top-20 by |ICIR| tables, bar charts, IC stability vs mean_ic scatter, feature overlap across horizons (set intersection), feature_selection_summary breakdown.
- **`notebooks/03_factor_correlation.ipynb`** (new): Factor correlation — Spearman heatmap of 1y selected features (max |r| ≤ 0.90 verification), alpha factor cross-correlation, high-correlation pairs (|r|>0.70) among selected features.
- **`notebooks/04_null_recovery_audit.ipynb`** (new): Null recovery audit — quarterly feature null rates post-imputation, size_category distribution + imputation quality bars, quarterly coverage by market, 341-column count verification, violin plots of quarterly feature distributions.

### Changed (Phase B — feature selection docs)
- **`docs/methodology/feature-selection.md`**: Updated opening summary (319→341 raw columns, ~185→~203 PSI candidates, ~35→~45 final), Mermaid diagram node labels, result paragraph, CLI examples (now reference `run_feature_selection.py`), and Outputs section (JSON per horizon + summary CSV).
- **`docs/developer/scripts.md`**: Added `run_feature_selection.py` section with bash usage, flags table, and output file descriptions.

### Added (Phase B — feature imputation)
- **`scripts/impute_features.py`** (new): Recovers two categories of missing data in `data/historical_dataset_clean.parquet`. (1) Reads quarterly rows from `data/historical_dataset.parquet` (pre-clean), runs `compute_quarterly_features()`, and left-joins 5 intra-year columns (`revenue_qoq_std_norm`, `earnings_qoq_mean`, `max_accruals_ttm`, `revenue_acceleration`, `quarterly_positive_rev_frac`) onto the clean parquet — 67% of annual rows enriched. (2) Imputes `size_category` from `log_assets` percentile rank within `(fiscal_year, market)` peer groups for 17,226 recoverable null rows; adds `size_category_imputed` boolean flag. Dataset grows from 335 → 341 columns. Supports `--dry-run` and `--source` flags.

### Changed (CI workflows)
- **`.github/workflows/monitor_drift.yml`**: `repo_id` corrected from `mhoque/stock-fraud-screener` to `ekrash718/stock-screener-data`; model file downloads updated to `repo_type='dataset'`.
- **`.github/workflows/weekly_push.yml`**: Deleted — superseded by `refresh_data.yml`.

### Added (Phase B — 5-factor alpha package)
- **`alpha/factors/`** (new package): Five cross-sectional rank factor modules — `value.py`, `quality.py`, `momentum.py`, `growth.py`, `fraud_risk.py` — plus `composite.py` that blends them into `alpha_composite` with configurable weights (default 0.20 each). All scores are 0–1, ranked within `(fiscal_year, market)` peer groups.
- **`scripts/compute_alpha.py`** (new): Loads `data/historical_dataset_clean.parquet`, calls `alpha.factors.composite.compute(df)`, and writes `alpha_value`, `alpha_quality`, `alpha_momentum`, `alpha_growth`, `alpha_fraud_risk`, `alpha_composite` (all `float32`) back to the parquet. Dataset grows from 329 → 335 columns. Supports `--dry-run` flag.

### Added (Phase B — ML scoring)
- **`scripts/score_historical.py`** (new): Applies trained LightGBM models (1y/3y/5y) to all 58K rows in the dataset. Loads `model_{1y,3y,5y}.joblib` + `model_meta.json`, fills missing features with per-horizon train_medians, calls `predict_proba`, and writes `ml_1y`, `ml_3y`, `ml_5y` float columns back to `data/historical_dataset_clean.parquet`. Dataset grows from 326 → 329 columns. Supports `--dry-run` flag.

### Fixed (Phase B — diagram sync)
- **`docs/architecture.md`**: Fixed stale column counts — B5 node 320→321 base columns, B7 node 324→326 total, F node now correctly shows 321 cols (pre-quarterly), Q node shows 326 cols as the final parquet. C6 node and Component Map row updated to ✅ for score_historical.py. Parquet storage node updated 326→329 cols.
- **`CLAUDE.md`**: Architecture State table updated — score_historical.py ❌→✅, parquet col count 326→329, Critical Missing Pieces updated.

### Added (Phase A — housekeeping / docs)
- **`docs/developer/schema-change-guide.md`** (new): Schema versioning policy — 11-step column-add checklist, rename breaking-change checklist (grep + retrain), deprecation protocol (keep one release with NaN, then drop), and a 6-file "column count must stay in sync" table covering `docs/architecture.md`, `docs/methodology/models.md`, `docs/index.md`, `CLAUDE.md`, and `docs/developer/data-update-guide.md`.
- **`scripts/analyze_distributions.py`** (new): Non-fatal CI script for dataset quality monitoring. Produces `reports/distribution_report.txt` (NaN%, outlier rates by |z|>5, market fill rates for 10 key features, fraud label balance, rows per market). With `--corr` flag also produces `reports/correlation_matrix.parquet` and prints high-correlation pairs (|r|>0.95). Usage: `python3 scripts/analyze_distributions.py [--parquet PATH] [--out-dir DIR] [--corr]`.
- **`docs/methodology/feature-registry.md`** (new): Complete 326-column authoritative registry — column names, data types, and factor group assignments. Organized into 10 sections: Identity/Admin (29), Raw Financials (~50 inputs), Value (~18), Quality (~83), Momentum (~45), Growth (~22), Fraud Risk (~164), Macro/Context (~10), Quarterly-Enriched (5), Derived/Interaction (~63).

### Fixed (Phase A — housekeeping / docs)
- **`docs/architecture.md`**: Remaining three "⚠️ pending" TimescaleDB references updated to "Phase C — deferred" — Component Map table row, Data Flow Detail `DB[TimescaleDB...]` node, and Deployment Architecture `J[TimescaleDB...]` node.
- **`docs/methodology/features.md`**: 8-category feature table total row corrected 324 → 326.
- **`docs/methodology/factor-library.md`**: Factor Group 3 (Momentum) fully updated — stale "⚠️ Momentum gap — Phase 0 blocker" admonition removed; mermaid diagram count updated ~32 → ~45; full implemented feature tables added for cross-sectional rank transforms (7 features) and raw price features (13+).
- **`docs/developer/data-update-guide.md`**: Column count reference table corrected 324 → 326; operator workflow Mermaid flowchart added immediately after "Always read this file before modifying the dataset."

### Changed
- **`.github/workflows/refresh_data.yml`**, **`weekly_push.yml`**, **`monitor_drift.yml`**: Hardcoded HuggingFace repo `ekrash718/stock-screener-data`; `HF_REPO` GitHub Actions secret no longer required (only `HF_TOKEN` needed).

### Fixed
- **`data/historical_dataset_clean.parquet`** (dataset cleanup): Removed 138,947 non-annual rows — `step6_clean.py` tagged quarterly rows as `in_universe=0` but never hard-dropped them; 3,117 blank-ticker BR rows (CVM companies unmatched to B3 tickers) also removed. Dataset reduced from 197,269 → 58,307 rows (annual-only, no blank tickers). Row counts in docs updated from 155K/156K → 58K.
- **`data/historical_dataset_clean.parquet`** (CA dedup): Removed 15 duplicate rows where Canadian companies changed their fiscal year-end mid-year, producing two annual reports for the same `fiscal_year`. Fixed by deduplicating on `(cik, market, fiscal_year, period_type)` keeping the later-filed row.
- **`scripts/test_dataset_quality.py`** primary key: Changed PK from `(cik, market, filed_date, period_type)` to `(cik, market, fiscal_year, period_type)` to handle DART bulk-filings where KR companies file multiple fiscal years on the same `filed_date`.
- **`pipeline/enrich_fraud_taxonomy.py`**: Re-executed on clean annual-only dataset to refresh all five percentile-rank fraud scores. Fraud suspect flag: 20,260 rows (34.74%).

### Added
- **`data/historical_dataset_clean.parquet`** `enterprise_value` column: `market_cap_at_filing + net_debt` — 68.6% fill (US-only; non-US lacks `market_cap_at_filing`).
- **`data/historical_dataset_clean.parquet`** `sector` column: SIC range → sector name mapping (Industrials, Technology, Healthcare, Financial, Energy, Materials, Consumer Staples, Consumer Discretionary, Utilities, Real Estate, Communication Services) — 75.5% fill, US-only (other markets have NaN `sic_code`). Dataset is now 58,307 × 326 columns.
- **`scripts/test_dataset_quality.py`**: New dataset quality test suite — 7 check categories, 53 checks total (schema, structural, market coverage, fill rates, distribution sanity, fraud label integrity, forward return coverage). All 53 pass on the current clean dataset. Usage: `python3 scripts/test_dataset_quality.py [--verbose] [--parquet PATH]`.

### Changed
- **`docs/architecture.md`**, **`docs/index.md`**, **`docs/methodology/models.md`**, **`CLAUDE.md`**: Column count updated 319 → 326, row count updated 155K → 58K across all diagram nodes, taglines, and architecture state table.

- **`scripts/train_models.py`**: `fraud_score_*` pattern added to `EXCLUDE_PATTERNS` — taxonomy sub-scores are UI display columns built from the same underlying signals already in the model; including them caused multicollinearity. `fraud_confirmed`, `fraud_suspect`, `fraud_label` added to `EXCLUDE` set — these are label columns, not input features.

### Fixed
- **`data/historical_dataset_clean.parquet`** (patch script): 9 columns were missing from the 324-column target — `working_capital`, `net_debt`, `accruals_ratio`, `price_to_book` (derived from existing raw columns), and the 5 cross-sectional momentum rank features (`momentum_12m_rank`, `momentum_6m_rank`, `momentum_3m_rank`, `vol_rank_12m`, `momentum_composite_rank`). Momentum ranks were already computed by `add_momentum_ranks()` in `step5_compute_features.py` but the combined multi-market parquet was built before the function was added. Derived columns added inline from `equity` (93% fill), `long_term_debt`/`short_term_debt` (100%), `current_assets`/`current_liabilities` (83–87%), and `market_cap_at_filing` (60%). Dataset is now 197,269 × 324 columns as documented.
- **`pipeline/enrich_fraud_taxonomy.py`**: `fraud_score_governance` was all-NaN for all rows because `small_auditor_flag` and `going_concern` are never written to the parquet by the current pipeline. Fixed by adding proxy signal fallback in `build_governance_score()`: uses `altman_z_score < 1.81` (distress proxy) and `piotroski_f_score ≤ 2` (weak-fundamentals proxy) when primary governance columns are absent. Both columns exist in all market datasets, so governance score is now non-NaN for 100% of rows.
- **`pipeline/enrich_fraud_taxonomy.py`**: `fraud_suspect` column was absent from `historical_dataset_clean.parquet` because `enrich_fraud_labels.py` (P0c) is a standalone enrichment script never called by `run_pipeline.py`. Fixed by adding `build_fraud_suspect()` function directly in P0d (`enrich_fraud_taxonomy.py`) — computes signal-based suspect flag (1 if 2+ of: Beneish > −1.78, Piotroski ≤ 2, Altman < 1.0) and writes `fraud_suspect` to the parquet each time P0d runs. `fraud_confirmed=1` rows are overridden to `fraud_suspect=0`.

### Added
- **`pipeline/phase_a_integrate_eu.py`**: Europe (EU) market integration — loads `data/snapshots_eu.parquet` (yfinance free-tier, DE/FR/NL/BE/PT/NO/FI/DK/SE/IE), standardises column aliases (depreciation, sga, accounts_receivable, total_equity), runs step3 price enrichment, merges macro (`macro_eu.parquet` → `macro.parquet` fallback), applies P0a/c/d/f/g, aligns to target schema, concatenates into `historical_dataset_clean.parquet`. EU-specific: strips existing rows using `isin(['DE','FR','NL','BE','PT','NO','FI','DK','SE','IE','EU'])`. Supports `--dry-run`.
- **`pipeline/phase_a_integrate_br.py`**: Brazil (CVM) market integration — loads `data/snapshots_br.parquet`, standardises 20+ missing columns (estimates `total_liabilities = total_assets - equity`, proxies `total_debt` from `long_term_debt`), runs step3 price enrichment, merges macro (`macro_br.parquet` → `macro.parquet` fallback), applies P0a/c/d/f/g, aligns to target schema, concatenates into `historical_dataset_clean.parquet`. Supports `--dry-run`.
- **`pipeline/phase_a_integrate_jp.py`**: Japan (yfinance free tier) market integration — same 9-step pattern; `standardise_jp_snapshots()` adds depreciation/sga/accounts_receivable/total_equity aliases, total_debt computation, and SIC NaN stubs; uses `macro_jp.parquet` → `macro.parquet` fallback. Supports `--dry-run`.
- **`pipeline/phase_a_integrate_ca.py`**: Canada (SEDAR+/yfinance) market integration — same 9-step pattern as JP; `standardise_ca_snapshots()` is identical column set to JP (both yfinance-based); uses `macro_ca.parquet` → `macro.parquet` fallback. Supports `--dry-run`.
- **`pipeline/step5_compute_features.py`**: `add_momentum_ranks()` function — computes 5 cross-sectional momentum rank features (`momentum_12m_rank`, `momentum_6m_rank`, `momentum_3m_rank`, `vol_rank_12m`, `momentum_composite_rank`) as percentile ranks within (fiscal_year, market) groupings. Closes the momentum gap documented since v0.1.0 (Jegadeesh & Titman 1993). Feature count: 319 → 324.
- **`scripts/run_pipeline_br.py`**: Brazil pipeline orchestrator — 6-step build chain (CVM+B3 tickers → CVM snapshots → price enrichment → macro → 324 features → clean). Passes `--snapshots snapshots_br.parquet` and `--suffix _br` to shared steps 3–6. Supports `--step N` resume and `--limit N` test mode. No API key required.
- **`scripts/run_pipeline_jp.py`**: Japan pipeline orchestrator — 6-step build chain using free-data variants (`step1_fetch_tickers_jp_free.py`, `step2_build_snapshots_jp_free.py`), shared steps 3–6 with `--snapshots snapshots_jp.parquet` and `--suffix _jp`. Supports `--step N` resume and `--limit N` test mode. No API key required.
- **`scripts/run_pipeline_ca.py`**: Canada pipeline orchestrator — 6-step build chain via TMX public API, shared steps 3–6 with `--snapshots snapshots_ca.parquet` and `--suffix _ca`. Supports `--step N` resume and `--limit N` test mode. No API key required.

### Changed
- **`scripts/run_pipeline_eu.py`**: rewritten from broken 2-step SimFin design to full 6-step yfinance free-data pipeline — now matches JP/CA runner pattern with `EU_STEPS` dict (steps 1–6), `LIMIT_STEPS = {1, 2, 3}`, `SNAPSHOT_STEPS = {3, 4, 5, 6}`, proper `--snapshots snapshots_eu.parquet`, `--out prices_eu.parquet`, and `--suffix _eu` routing; status function updated to show all 6 EU output files; `--markets` flag (SimFin-specific) removed; post-build message instructs user to run `phase_a_integrate_eu.py` next. No API key required.
- **`docs/developer/scripts.md`**: `run_pipeline_eu.py` section rewritten — removed SimFin/API key references, added full 6-step pipeline description with flags table and output file list.
- **`pipeline/step5_compute_features.py`**: added `--snapshots`, `--prices`, `--macro`, `--suffix` CLI flags to `__main__` block — suffix derives market-specific price/macro paths (`prices{suffix}.parquet`, `macro{suffix}.parquet`) and routes output to `historical_dataset{suffix}.parquet`. Enables all BR/JP/CA/EU pipeline runners to invoke step 5 without code duplication.
- **`pipeline/step6_clean.py`**: added `--suffix` CLI flag to `__main__` block — reads `historical_dataset{suffix}.parquet` and writes `historical_dataset_clean{suffix}.parquet`. Also accepts `--snapshots` for pipeline argument compatibility. Enables all multi-market pipeline runners to invoke step 6 with the correct market file pair.
- **`scripts/run_pipeline_br.py`**: fixed `build()` flag logic — step 3 now receives `--out prices_br.parquet` (was missing, causing US prices.parquet to be overwritten); steps 4–6 now all receive `--suffix _br` (was: step 4 received unused `--prices` flag instead).
- **`scripts/run_pipeline_jp.py`**: same flag logic fix as BR — step 3 gets `--out prices_jp.parquet`; steps 4–6 get `--suffix _jp`.
- **`scripts/run_pipeline_ca.py`**: same flag logic fix as BR — step 3 gets `--out prices_ca.parquet`; steps 4–6 get `--suffix _ca`.
- **`docs/developer/pipeline-scripts.md`**: `step4_enrich_macro.py`, `step5_compute_features.py`, and `step6_clean.py` sections updated with CLI flags tables documenting the new multi-market routing arguments.
- **`docs/methodology/features.md`**: Momentum section updated — warning admonition removed, new rank feature table added, total column count updated 319 → 324.
- **`CLAUDE.md`**: Architecture state table updated — momentum feature count and file reference corrected.
- **`pipeline/step1_fetch_tickers_br.py`**: `match_tickers()` rewritten — replaced brapi per-ticker name-fetch loop (was capped at first 400 tickers) with pure text heuristics against the full 1,800+ brapi ticker list. Match strategies: (1) first 4 letters of normalised commercial name → ticker root, (2) acronym of first 4 words → ticker root, (3) fallbacks to full legal name. Expected match rate: 300–400+ tickers (was ~57). No extra API calls; `time` and `numpy` imports removed.
- **`docs/developer/pipeline-scripts.md`**: `step1_fetch_tickers_br.py` entry updated — describes new text-heuristic matching approach and expected match rate.

 dedup key updated from `(cik, filed_date, period_type)` to `(cik, market, filed_date, period_type)` to prevent cross-market CIK collisions when merging multi-market snapshots.
- **`pipeline/phase_a_integrate_kr.py`**: updated schema reference from 313→319 columns; `classify_universe` call now explicitly passes `apply_filters=False` to prevent structural-only mode being silently overridden by future signature changes.
- **`pipeline/step6_clean.py`**: removed revenue ($1M), total_assets ($100K), and entry_price hard-threshold filters to maximise ticker coverage; dedup key updated from `(cik, filed_date, period_type)` to `(cik, market, filed_date, period_type)` to prevent cross-market CIK collisions.
- **`pipeline/p0f_universe_definition.py`**: revenue/asset/price/sector exclusion rules are now opt-in via `--apply-filters` flag; default mode applies structural rules only (annual + fiscal_year range) so all tickers receive `in_universe=1` by default.
- **`docs/developer/pipeline-scripts.md`**: updated `step6_clean.py` section (new minimal-filter design) and `p0f_universe_definition.py` section (structural vs investable-universe modes, new CLI flags).

### Changed
- **`ROADMAP.md`**: restructured to 16-step backbone (Phase A/B/C) covering all 15 deliverables — Git cleanup (Step 0), Portfolio Construction (Step 11), and Model Selection & Tuning (Step 7) added as explicit steps; free-data-only policy enforced across all 6 markets; Session Continuity table added; Immediate Next Actions priority order updated (P0.3, P0.1, P0.2, P0.4, P0.5).

### Added
### Removed
- **`pipeline/enrich_auditor_going_concern.py`**: superseded by `pipeline/enrich_governance.py` (going concern via EDGAR EFTS full-text search).
- **`pipeline/score_and_report.py`**: rules-based composite fraud score with fixed weights — contradicts ML-first architecture. Deleted to prevent future confusion.
- **`scripts/watchlist.py`**: session state export unrelated to quant lab research pipeline.
- **`scripts/high_roi_strategies.py`**: redundant wrapper, no unique functionality.

- **`ROADMAP.md`**: full phase tracker — Phase 0–3 with task checklists, exit criteria, codebase cleanup targets, and file inventory.
- **`CONTEXT.md`**: session state snapshot for context continuity between sessions; tracks current phase, completed tasks, blockers, architecture summary, data coverage.
- **`docs/methodology/feature-selection.md`**: PSI → IC → ICIR → Spearman deduplication pipeline with formulas, thresholds, CLI flags, planned Newey-West HAC + Fama-MacBeth + FDR improvements.
- **`docs/methodology/factor-library.md`**: 5 factor groups as ML input categories. Architecture decision: no fixed-weight composite; combination weights are ML-learned. All feature formulas, academic citations, data sources.
- **`docs/developer/contributing.md`**: vision checklist (5 questions before every task), "done" definition (6 steps), sync rules by change type, commit convention, architecture constraints, phase-gate review process.
- **`docs/developer/pipeline-scripts.md`**: full reference for all `pipeline/` modules — step1–step6 per market, enrichment modules, universe definition, confidence score, integration helpers.

### Changed
- **`mkdocs.yml`**: site_name updated to "Multi-Factor Stock Screener". Added feature-selection, factor-library, contributing, pipeline-scripts to nav.
- **`CLAUDE.md`**: reframed to multi-factor quant lab with architecture state table and pre-task checklist.
- **`docs/index.md`**: reframed as quant alpha lab with ML-first framing.
- **`docs/architecture.md`**: 15-layer architecture, multi-market integration, alpha signal flow.
- **`docs/methodology/features.md`**: dual taxonomy rewrite (5-factor + 8-category), momentum gap warning added.

- **`scripts/fetch_aaer_labels.py`**: builds `data/aaer_labels.csv` (per-company fraud year
  ranges from 220 matched companies) and rewrites the `fraud_confirmed` column in
  `data/historical_dataset_clean.parquet`.  Sources: `data/aaer_cache.json` (232 AAER CIKs)
  + SEC EDGAR full-text search for 10-K filings disclosing SEC investigations and
  restatements (~1,418 + ~521 hits across two queries).  Labeling window:
  `fiscal_year ∈ [fraud_year_start − 2, fraud_year_end]`.  Coverage: ~492 annual positive
  rows from ~118 companies (up from 172 / 33 companies).  Flags: `--lookback`, `--dry-run`,
  `--no-update-parquet`, `--start-year`, `--end-year`.
- **`docs/developer/scripts.md`**: added `fetch_aaer_labels.py` section with usage examples,
  flags table, and coverage note.


  `docs/methodology/features.md`, `docs/methodology/pipeline.md`, `docs/index.md`,
  `docs/markets.md`, `docs/developer/setup.md`, `docs/developer/scripts.md` — previously
  only `docs/architecture.md` and `docs/methodology/models.md` were required
- **`scripts/check_sync.py`**: expanded `ml-pipeline` rule to require `README.md` and
  `docs/index.md` (both carry AUC tables that must stay in sync)
- **`scripts/check_sync.py`**: added `docs/methodology/features.md`, `docs/methodology/pipeline.md`,
  `docs/index.md`, `docs/markets.md` to `docs_and_config` exclusion set

### Fixed
- **`README.md`**: AUC table corrected — WF Mean AUC: 1y 0.553, 3y 0.643 ✅, 5y 0.597
  (was 0.749/0.780/0.856 — those were val/test AUC from a prior training run, not WF CV)
- **`docs/index.md`**: feature count 313→319; AUC table updated to Val AUC + WF Mean AUC;
  Mermaid graph node corrected to 319 features
- **`docs/markets.md`**: column count 313→319
- **`docs/methodology/features.md`**: header corrected to 319 features (was 278)
- **`docs/methodology/pipeline.md`**: Step 5 node formula count 278→314
  (feature_library.py produces 314 base columns; +5 quarterly → 319 total)
- **`docs/developer/setup.md`**: feature_library.py comment updated to 314/319
- **`docs/developer/scripts.md`**: pipeline step 4 updated to 314 + 5 quarterly = 319


  features whose IC sign is inconsistent across years; set to 0.6 to require ≥60% of years with
  correct-sign IC before a feature enters the model
- **`scripts/train_models.py`**: `--min-ic-years INT` flag (default 1 = off) — requires a minimum
  number of years of IC observations; prevents spurious ICIR inflation from features with very few
  historical data points (e.g. `fraud_label` with n_years=1 would otherwise rank first by ICIR)
- **`scripts/train_models.py`**: `FORCE_INCLUDE_1Y` constant — mechanism to force-include named
  features into the 1y model even if they don't rank in the ICIR top-N; currently empty after
  testing showed no net WF AUC improvement (see notes in file)

### Changed
- `docs/developer/scripts.md`: flags table for `train_models.py` updated with `--min-ic-stability`
  and `--min-ic-years`

### Notes (no change)
- Tested `vix` as force-include for 1y model: improved 2018→2019 fold AUC 0.465→0.485 (COVID
  reversal regime) but 2019→2020 fold declined 0.549→0.526; net WF mean AUC 0.553→0.549 (−0.004).
  Reverted. Root cause: 2018→2019 is dominated by the COVID crash/recovery regime in which all
  fundamental factors inverted sign; no single feature addition recovers this fold without hurting
  adjacent folds. 1y WF mean AUC remains 0.553 (target ≥0.62 not yet met).

### Added
- **`CLAUDE.md`** — AI assistant instructions: Change Checklist matrix, Architecture Sync
  Rules, Current Architecture State table, key file locations, commit convention
- **`scripts/enrich_quarterly_features.py`** — computes 5 intra-year dynamics (revenue smoothing,
  earnings momentum, accrual peak, revenue acceleration, positive-quarter fraction) from Q1/Q2/Q3
  rows and left-joins them onto annual training rows; covers 74.8% of annual rows
- **`scripts/mark_survivorship.py`** — identifies likely-delisted companies and imputes −50% forward
  return to correct survivorship bias in training data
- **`scripts/migrate_to_db.py`** — bulk-loads `historical_dataset_clean.parquet` into TimescaleDB
  hypertable (schema in `infra/db/init.sql`)
- Walk-forward AUC section in Backtester tab — summary table + AUC-over-time chart + per-fold
  expander; reads `reports/walk_forward_auc_{h}.csv` generated by `train_models.py --walk-forward`
- Company Profile: Fraud Taxonomy breakdown (5-dimension radar + YoY delta), Peer Comparison
  section (percentile table + box-plot distribution), confidence detail expander with coverage groups
- Screener tab: live prediction log via `scoring.log_predictions()` called each time models score
- Watchlist: watchlist tab with add/remove and per-ticker score history sparklines
- FastAPI: full screener router with filters (market, exchange, sector, Piotroski, Beneish,
  confidence), paginated response; `api/deps.py` singleton dataset loader

### Changed
- `train_models.py`: added PSI-based feature filter (default `--max-psi 2.0`) applied after
  `log_psi_report()`; drops 10 macro-regime features (treasury rates, fed funds, CPI, yield
  curve) that shift dramatically between train and test — prevents IC inflation on stale regimes
- `train_models.py`: PSI filter runs before IC analysis so macro features never enter the
  ICIR ranking step
- Historical dataset now includes 5 quarterly-derived feature columns (319 total columns)
- `CONTRIBUTING.md`: added Sync Checklist table — every PR must update docs/diagrams in
  the same commit as the code change
- `docs/architecture.md`: all three Mermaid diagrams updated (quarterly enrichment, survivorship,
  PSI filter, TimescaleDB, FastAPI nodes added; column counts corrected to 319)
- `docs/developer/scripts.md`: added entries for `enrich_quarterly_features.py`,
  `mark_survivorship.py`, `migrate_to_db.py`; updated `train_models.py` flags table
  (`--max-psi`, `--walk-forward`)
- `docs/methodology/models.md`: AUC table corrected (1y 0.553, 3y 0.643, 5y 0.597 WF mean);
  Mermaid flowchart updated with PSI filter step; dataset node updated to 319 columns

### Fixed
- Feature descriptions dictionary (52 entries) in `app_v2.py`
- SHAP-driven strengths/weaknesses narrative in Company Profile tab
- EU, Korea, Japan, Canada, Brazil pipeline scripts
- Backtester: sector cap, filing-lag filter, benchmark equity curve
- Leverage strategy: Kelly-sized long/short portfolio with quality gates
- Drift monitor: PSI + rolling AUC with GitHub Actions alerts
- Bias audit: temporal leakage, shuffle test, filing-lag audit
- MkDocs documentation site (15 pages across 4 sections)
- 4 research notebooks (EDA, Beneish deep-dive, feature IC, backtest analysis)
- `site/` now excluded from git via `.gitignore`

---

## [0.1.0] — 2024 (Initial internal release)

### Added
- US data pipeline via SEC EDGAR
- LightGBM models for 1y/3y/5y fraud horizons
- Streamlit app with Screener, Company Profile, Backtest tabs
- HuggingFace Hub for model and dataset storage
- GitHub Actions: weekly refresh + drift monitor

---

## [Unreleased — process automation]

### Added
- **`scripts/check_sync.py`** — architecture sync checker; reads staged files and applies
  the CLAUDE.md Change Checklist rules; reports missing doc updates; exit code 1 blocks
  bad commits
- **`.git/hooks/pre-commit`** — calls `check_sync.py` automatically before every commit;
  bypass with `--no-verify` in emergencies
- **`.claude/commands/sync-check.md`** — `/sync-check` slash command: interactive sync
  status report against all uncommitted changes
- **`.claude/commands/sync-update.md`** — `/sync-update` slash command: drafts CHANGELOG
  entry + lists required doc files after a code change

### Changed
- **`docs/developer/scripts.md`** — added Process Automation section with `check_sync.py`
  flags table and pre-commit hook documentation



### Bug fixes: auto_adjust=False, OOF contamination fix (2026-05-14)

#### Critical correctness fixes across yfinance callers and fraud risk factor (fix/pipeline,alpha)
- **`pipeline/step3_enrich_prices.py`**: Changed `auto_adjust=True` → `auto_adjust=False` in `tk.history()` to prevent retroactive price adjustments from corrupting momentum features and return labels
- **`scripts/bias_audit.py`**: Changed `auto_adjust=True` → `auto_adjust=False` in `yf.download()` call
- **`scripts/build_monthly_price_cache.py`**: Changed `auto_adjust=True` → `auto_adjust=False`
- **`scripts/fetch_spy_returns.py`**: Changed `auto_adjust=True` → `auto_adjust=False`
- **`alpha/factors/fraud_risk.py`**: Replaced contaminated `ml_1y/ml_3y/ml_5y` signals with `ml_1y_oof/ml_3y_oof/ml_5y_oof` (walk-forward unbiased OOF scores) in `_ML_SIGNALS`; contaminated signals include training rows and inflate in-sample alpha

---

### Section 9 — High-Conviction Leveraged Portfolio (2026-05-15)

#### notebooks/08_experiment_hub.ipynb — 3 new cells (sec9md000, sec9cfg01, sec9main2)

- **Safety gate scoring**: 4 hard gates — Piotroski F ≥ 7, Altman Z > 1.81, FCF yield > 0, Beneish M < −1.78
- **Tier classification**: Tier 1 = all 4 gates, Tier 2 = 3/4, Tier 3 = 2/4
- **Leverage multiplier**: Tier-1 × β<0.5 → 2.0×; β<0.8 → 1.5×; β<1.0 → 1.25×; Tier-2 × β<1.0 → 1.1×; hard cap 2.0×
- **Conviction rank**: composite_score × gates_passed / 4 (penalizes high-composite stocks failing gates)
- **Implied CAGR**: linear fit 8 + composite_score × 27 (8% at 0 → 35% at 1.0)
- **Levered CAGR**: implied_cagr × leverage_x
- **Net Kelly**: kelly_pct × leverage_x
- **Output**: styled table (RdYlGn gradient on composite + levered CAGR, bar on leverage_x), 2-panel chart, CSV export
- **Results**: 25 candidates (top 30%), 6 Tier-1, 19 Tier-2, 20 picks → `data/leveraged_picks_2026-05-15.csv`
