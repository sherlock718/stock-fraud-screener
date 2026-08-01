# Codex Roadmap — Canonical Product Consolidation

## Purpose

Consolidate the repository's existing work into one understandable,
reproducible dataset-to-screening product. Do not rebuild what already exists
or resume historical-performance certification.

## Product boundary

- One manifest-backed bronze/source layer.
- One cleaned longitudinal point-in-time dataset contract.
- One leakage-safe temporal research and model-evaluation route.
- Interpretable and sophisticated model options under the same split contract.
- One out-of-sample backtest and portfolio route with small-cap, liquidity,
  cost, and survivorship limitations explicit.
- One explainable ranked shortlist and report.
- Fraud risk remains one factor or safety gate.

## Session P1 — Read-only canonicalization decision

Map the existing implementation before changing it:

1. Identify canonical bronze/source artifacts and their manifests.
2. Trace the six-stage pipeline to cleaned-dataset outputs.
3. Map later PIT corrections against the main pipeline and identify true
   supersession, duplication, or unresolved conflicts.
4. Compare existing cleaned-dataset candidates on schema, time semantics,
   coverage, targets, provenance, and downstream compatibility.
5. Identify the single best existing temporal split, fold-local feature
   selection, model, backtest, liquidity, portfolio, and reporting route.
6. Classify relevant paths as canonical candidates, required dependencies,
   historical evidence, duplicates, unknowns, or future archive candidates.
7. Recommend exactly one bounded implementation task for P2.

Do not download or refresh data, call external services, edit or archive files,
retrain models, calculate performance, or revive V3.4.

**Deliverable:** a concise dependency/canonical-path map, evidence-backed
recommendation, blockers, archive candidates that require later confirmation,
and exactly one P2 implementation task.

## Session P2 — Establish one canonical data route

Implement only the task selected by P1. Prefer connecting or promoting existing
corrected components over creating new versions. Establish one canonical
source-to-cleaned-dataset command or contract, explicit validation failures,
lineage, and focused tests.

No model training or performance calculation unless P1 proves that the only P2
blocker belongs at that boundary.

**Deliverable:** one reproducible, documented canonical data route and an exact
statement of its coverage and remaining survivorship limitations.

## Session P3 — Establish one leakage-safe research/model route

Use the canonical dataset contract. Confirm target availability, temporal
splits, fold-local preprocessing and feature selection, and comparable
interpretable/sophisticated model interfaces. Reuse existing implementations.

**Deliverable:** one reproducible research/model command with OOS-only outputs
and focused leakage tests.

## Session P4 — Establish one product route

Connect OOS predictions to the existing backtest, small-cap/liquidity gates,
portfolio construction, and explainable report. Make costs and unresolved
survivorship boundaries visible. Do not claim future performance.

Only after the route is confirmed may clearly superseded files be archived in
one reviewed, dependency-safe change.

**Deliverable:** one usable dataset-to-shortlist product path, concise usage
documentation, and an honest limitations section.

## Session I1 — first international adapter completed

Canada (`CA`, TSX/TSXV) was selected before downstream model, shortlist,
portfolio, or performance results using preserved local evidence only. The
fresh artifact is `artifacts/international/i1/20260801T180000Z-i1-ca/`, manifest
SHA-256 `d3f17854cf0839713163e8dda99aefedd6ba064a14a042a886770818a472d9f6`.
It contains 9,207 row-complete core P2 compatibility records with CAD/IFRS
semantics. Calendar, benchmark-vintage, action/delisting, FX-vintage,
survivorship-complete target, P3/P4, model, portfolio, and performance
requirements fail closed. The five partial M1C attempts and frozen P2/P3/P4/
B1D/B1E/M1A/M1C/M1D boundaries remain unchanged. Stop after I1.

## Session US1A — final offline US free-data product completed

US1A froze the baseline before observing any new downstream output. D1 passed
its internal source-lineage, schema, stable-identity, availability, target-
support, and deterministic-record gates, but failed exact frozen-route row
coverage: 43,564 stable IDs are shared, 242 exist only in accepted P2, and 76
exist only in D1. Its manifest and population cannot satisfy M1C's pinned P2
contract without retraining or a contract change, so accepted P2 remains the
US release baseline.

The non-overwriting artifact is
`artifacts/product/us_free_v1/20260801T183000Z-us1a/`, manifest SHA-256
`f27773c0e1fb92a25707e0fec363e13afa7ccb10f8ace7537187c6be58575edf`.
It consumes only accepted M1A/M1C inner-evidence winners through unchanged P4
eligibility, liquidity, ranking, 15-name equal-weight, and tie-break rules. It
contains 87,612 row-role scores, 43,806 candidates, 525,672 gate rows, 1,477
liquidity-required rows, 44,310 exact liquidity sessions, 120 holdings, and the
final 15-name 2026 shortlist. Five names have exact existing E1 lineage and
remain unresolved; ten are explicitly `event_evidence_not_collected`. No
performance or external collection occurred. The focused product/prediction/
P4/shortlist/lineage boundary passes 89 tests, and the final full suite passes
853 tests with 4 skips and 78 existing warnings.

## Session US1B — frozen-shortlist evidence completed

The exact request plan was frozen and approved before collection. The fresh
non-overwriting derivative is
`artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b/`, manifest
SHA-256 `f802ec26358d27afa21c99490e598e13c98a6ad60229705769f3fae1818d9dcd`.
It reuses 29 exact D1/E1 responses without requests and records one attempt for
each of the 36 approved URLs; all 36 returned HTTP 200 and total 3,681,377
response-body bytes. Its 15 market rows, 47 document adjudications, 46 cited
claims, and 15 name rows are independently hash/locator verified.

Name-level states are 14 unresolved and one failed request. HPK remains failed
because its preserved E1 Form 25-NSE response is HTTP 503 and was not retried.
All 15 deterministic actions remain unresolved with explicit human review.
US1A identities, ranks, holdings, scores, gates, liquidity decisions, and 1/15
weights are unchanged; no model or performance route ran. Stop after US1B.
At that boundary US1C and all release, recovery, archive, publication, commit,
and push work remained unauthorized. A later explicit request authorized only
the bounded US1C local consolidation below. The post-US1B full suite passed
859 tests with 4 skips and 78 existing warnings.

## Session US1C — local release consolidation completed

The frozen contract is `docs/US1C_RELEASE_CONSOLIDATION_CONTRACT.json`,
SHA-256
`d7098153542afa77ff09e850980362eb957b3e24cd609a4984451289595f1d47`.
`python3 -m workflows.run_us_free_v1` is the single complete local free-data
V1 verification route. It verifies P2 -> P3 -> P4 -> US1A -> US1B and every
named frozen supporting boundary without external access by default.

Two fresh non-overwriting assemblies were byte-identical. Read-only recovery
from the existing private immutable Hugging Face revision
`aaf056ea115067e42ef9abf9fa93ade75cdd4052` verified all three P2/P3/P4
artifacts, 202 files, and 481,666,707 bytes without a mutable fallback or
remote mutation. The local candidate is
`artifacts/product/us_free_v1_release_candidate/20260801T210000Z-us1c/`,
manifest SHA-256
`a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466`,
with 24 records and 1,039,444 recorded bytes.

US1A and US1B reconcile exactly: every identity, holding, rank, score, gate,
liquidity decision, and 1/15 weight remains unchanged; evidence remains 14
unresolved names plus failed-request HPK, with unresolved deterministic
actions and human review for all 15. D1 remains incompatible with frozen M1C
because of 242 P2-only and 76 D1-only stable IDs. The focused US1C boundary
passes 37 tests with 13 existing warnings, and the single final full-suite run
passes 871 tests with 4 skips and 78 existing warnings. Stop after US1C. No
release occurred; A1, REL1, archive, publication, commit, push, and tag remain
unauthorized.

## Deferred work

The archived V3.4/V3.5 canonical-market-ledger program is not an active
dependency. New data vendors, multi-market expansion, strategy proliferation,
threshold tuning, new feature families, new fraud models, and broad repository
reorganization remain deferred until the canonical product path works.

## Post-P4 continuation

P2-P4 are complete as the local reproducible product baseline. The approved
continuation is a controlled refactor, not a whole-repository rewrite. The
ordered consolidation, private publication, US refresh, free-source
survivorship/event ledger, backtest, nested tuning, international restoration,
and archive plan is frozen in
`docs/CANONICAL_REFACTOR_COMPLETION_PLAN.md`.

C1 through D1, S1, and the offline E1 review are complete. The separately
approved frozen E1 primary-document plan has also been exhausted at exactly 47
SEC Archives requests under a non-overwriting, independently verified
artifact. Its offline extraction continuation now preserves 46 exact cited
claims, 47 fail-closed deterministic rows, the updated 15-name live contract,
and the explicit human-review queue under
`20260730T154650Z-e1-extraction-v2`. It does not resolve the free-source
historical market-ledger gap or unlock performance. The later offline
citation-by-citation adjudication is frozen under
`20260730T173110Z-e1-adjudication-v2`: 0/47 documents and 0/15 names satisfy
all deterministic requirements, so all remain unresolved in an unsigned
15-row explicit-human-signoff queue. It does not authorize a retry, promotion,
publication, archive, branch, commit, or push.

The local-only B1 evidence decision is also complete. Current evidence cannot
support provider-certified or survivorship-complete performance: S1 has 0
matched, 135 ambiguous, and 49 unsupported requirements; E1 supplies no
deterministic historical upgrade; and the exact 2026-07-17 `DGS1MO` ALFRED
vintage is absent. B1D supplies the tested P4-compatible overlapping 36-month
implementation, and B1E has now run it once over the frozen free evidence
without converting that evidence into a certified claim.

Q1 is complete and classifies the frozen P3 evidence as moderate predictive
signal. LightGBM produced pooled Spearman IC 0.138 across 16,597 labeled OOS
rows and positive annual IC in all ten labeled years; the decision tree
produced pooled ROC AUC 0.569 but weak calibration. This is enough to continue
to a disciplined backtest, while label selection and survivorship uncertainty
remain material.

The user selected the best defensible free-data product for V1 and deferred
paid data to V2. B1B through B1E are complete in the approved order. The final
non-overwriting B1E artifact is
`artifacts/performance/free_data_v1/20260801T011135Z-b1e/`, manifest SHA-256
`23588207812e2e950d3e521c6f2048e7607f20b99b691407c449ceb7752bf37c`.
It accounts for all 135 matured holdings and $1.8M planned exposure, observes
all 5,130 required stock and benchmark sessions with zero gaps, preserves 45
open holdings outside completed metrics, and records zero scenario-imputed
capital because no explicit terminal-exit trigger exists. All four physical
performance namespaces therefore have equal values without being blended.
The aggregate net zero-rate diagnostic has 18.69% CAGR versus 9.42% for its
benchmark, 24.26% annualized volatility, -30.32% maximum drawdown, and 0.826
Sharpe. DGS1MO-dependent Sharpe, Sortino, and alpha remain unavailable.

These are free-source historical research results, not survivorship-complete
or provider-certified performance, personalized advice, or a future-
performance promise. On 2026-08-01 the user deferred V2A and all paid-data
exploration to make the existing free-data route as strong as possible. M1A is
complete as a contract-only boundary under
`artifacts/modeling/nested_walk_forward/20260801T000000Z-m1a/`, manifest
SHA-256 `a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc`.
It preserves the exact P3 outer folds, freezes the nested expanding inner
folds, label maturity and horizon purging, fold-local feature selection and
preprocessing, bounded existing-model grids, predictive objectives,
simplicity tie-breakers, and three gate-training regimes. It added no model or
performance result. M1B is complete: the frozen contract now has hash-verified,
fail-closed modeling interfaces and 21 synthetic leakage tests, with no
historical fitting, tuning, scoring, portfolio, performance, or generated
artifact. M1C is complete under
`artifacts/modeling/nested_walk_forward/20260801T121426Z-m1c/`, manifest
SHA-256 `125cb5d6a8012c3b03ee6eab5f00ac944135c39996e992ecf40d8816acdacc58`.
It contains 87,612 row-complete source-row/role records, 20,142 matured
metric-eligible predictions, 24,566 open 2024-2026 scores outside metrics, and
516 explicit future exclusions. Its pooled 2019-2023 predictive-only evidence
is LightGBM Spearman IC 0.335 and tree ROC AUC 0.654 over 10,071 rows per role.
M1D is now complete under
`artifacts/performance/m1d/20260801T162953Z-m1d/`, with pre-performance lock
manifest SHA-256
`757e19cd9e35290a6b339f79e2c44a0f1ddb47c03b913930b1abda84f0bf74bc`
and final manifest SHA-256
`b04cea8236da6cd92749410f6186360be5f29dbbbeb00a64f2ed07c180cc72ab`.
The accepted M1C route was frozen from inner evidence before performance and
evaluated exactly once through unchanged P4/B1D/B1E rules. The primary result
is 19.75% aggregate net CAGR, 0.854 zero-rate diagnostic Sharpe, -29.46%
maximum drawdown, 24.50% annualized volatility, and 2.0 turnover. The 30%
CAGR and 1.0 Sharpe reporting thresholds are both not met, which is the final
M1 outcome rather than authorization to retry or retune. Open 2024-2026 rows
remain outside completed metrics; exact DGS1MO-dependent metrics and
provider-certified or survivorship-complete claims remain unavailable. I1,
US1C, and A1 were later separately authorized and completed below. Do not
begin REL1 or deferred V2A without separate authorization.

## Session A1 — dependency-safe archive and retirement completed

A1 first passed the complete offline US1C route and reverified candidate
manifest SHA-256
`a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466`.
The frozen contract is `docs/A1_ARCHIVE_CONTRACT.json`, SHA-256
`1b8174bac5181b68f6e3913c2e6bf73a7271acce9f1421063e40b39db2a63f3a`.
Its pre-action inventory hashes all 303 tracked and 53 non-ignored untracked
files, preserving the complete dirty-worktree boundary.

Four confirmed superseded payload groups were packaged under
`artifacts/archive/a1/20260801T220130Z-a1/`: corrected partial output, corrected
partial inputs, Session 9 corrected OOS output, and the Session 9B zero-holdings
freeze. The archive manifest SHA-256 is
`892cf0991b9e1a5a651728a1329bd91422037c62de6d57ee27adcc1e919399da`.
It covers four packages, 887 source files, and 1,954,687,841 source bytes.
Every package was independently extracted and every path, size, and SHA-256
matched before the original payload root became pointer-only.

The focused dependency/recovery/import/workflow/failure-path boundary passes
47 tests with 13 existing warnings. The complete post-retirement US1C route
passes with the expected candidate hash, and the final full suite passes 869
tests with 4 skips and 78 existing Step 5/Step 6 warnings.

Eleven associated tracked builders, tests, and reports were retired from the
active tree only after `git archive` recovery from `codex/legacy-archive`
commit `ed53232cb45cf8b82cfd8941ead5f9a88016e507` reproduced all 11 hashes.
V3.1-V3.3 failed the retirement gate and remain intact because parity tests
still import them and M1A still pins V3.1. Canonical P2-P4, corrected source
evidence, D1/S1/E1/B1/M1/I1/US1A-US1C, all immutable Hugging Face pointers,
international structure, shared implementations, and `.jupyter_ystore.db`
remain unchanged.

A1 made no external request, remote mutation, model/performance execution,
methodology change, release, branch switch, commit, push, or tag. Its packages
are local and Git-ignored; no remote-durability claim is made for them. Stop
after A1. REL1 and deferred V2A remain unauthorized.
