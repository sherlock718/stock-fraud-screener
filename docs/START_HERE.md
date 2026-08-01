# Start Here

## The goal

Build one simple, workable research-to-screening product:

1. preserve freely available ticker, financial, market, and macro source data;
2. produce a cleaned longitudinal point-in-time feature dataset;
3. perform leakage-safe feature analysis and selection;
4. train interpretable and sophisticated models;
5. backtest out-of-sample predictions with survivorship limitations made
   explicit;
6. apply small-cap and liquidity gates, construct portfolios, and report an
   explainable ranked shortlist.

Value, quality, momentum, growth, and fraud risk may all contribute. Fraud risk
is a factor or safety gate, not the product itself.

## Where the project stands

Most required layers already exist: source Parquets on Hugging Face, a
six-stage pipeline, a 367-column cleaned dataset, an HTML feature dictionary,
temporal feature selection, multiple model families, backtesting, liquidity
gates, portfolio construction, and extensive tests.

Substantial PIT and survivorship corrections also exist. Product Session P2
has now consolidated the corrected source-to-cleaned-dataset boundary into one
canonical US annual route. Product Session P3 has connected that contract to
one leakage-safe observed-only three-year research/model route. Product Session
P4 has now connected those row-complete OOS predictions to one fail-closed
small-cap/liquidity, portfolio, and explainable-shortlist route.

Therefore:

- there is no provider-certified or survivorship-complete historical-
  performance claim; B1E and M1D are explicitly limited free-source research
  results;
- the canonical primary dataset is US annual and observed-only;
- the canonical product output is a reproducible research shortlist, not
  personalized investment advice or a future-performance claim;
- certified macro vintages remain unavailable and are not synthesized;
- the V3.4 external market-ledger collection remains paused;
- S1 now measures the free SEC security/survivorship boundary. Provider-
  certified and survivorship-complete performance remain unavailable because
  historical listing/action coverage is incomplete and the immutable
  `DGS1MO` ALFRED vintage is absent; the approved V1 plan instead calculates
  free-source performance under separate observed and explicit survivorship-
  scenario namespaces after their contracts and NAV implementation pass;
- E1 now replaces the legacy knowledge-only M&A warning with deterministic,
  dated, cited historical/live review contracts; its frozen 47-request
  primary-document plan has now been executed without resolving any event by
  unsupported inference;
- nothing should be archived until its dependencies and replacement are known;
- the dataset remains historically enriched, not comprehensively
  survivorship-free.

## Canonical data entrypoint

Run:

```bash
python3 -m pipeline.build_corrected_feature_population
```

The command refuses to reuse a non-empty target and writes
`artifacts/canonical/corrected_us_annual/manifest.json`. The canonical primary
dataset is
`outputs/observed_only/features_taxonomy.parquet` relative to that root.

The frozen manifest SHA-256 is
`40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3`.
The primary Parquet has 43,806 rows, 339 columns, 43,806 unique stable row IDs,
and SHA-256
`46ed33affd8cc66df1f11dc8a41d6f804d106f0a2f8841509cbc931de013e56f`.
All rows have event-time materialization and non-empty PIT transform methods.
The physical policy-sensitivity output remains separate and adds zero
policy-only rows.

## Canonical research/model entrypoint

Run:

```bash
python3 -m modeling.build_canonical_research_model
```

The command refuses to reuse a non-empty target and writes
`artifacts/canonical/corrected_us_annual_3y_research_model/manifest.json`.
It fail-closes unless the pinned P2 manifest and the consumed primary features,
observed labels, row-horizon support, and summary records all match.

The frozen P3 manifest SHA-256 is
`8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2`.
Its OOS Parquet has 87,612 unique row-role records: exactly two roles for each
of 43,806 stable source rows. It contains 77,788 available OOS predictions and
9,824 explicit exclusions, with zero in-sample predictions. The prediction
Parquet SHA-256 is
`85164532421aec9909670f16fd56b4dcc9c5bcaaf7273c1ab98d1c464a7242a2`.

The route uses the observed three-year stock return for LightGBM regression
and observed three-year benchmark outperformance for the interpretable
decision tree. Target/support columns and uncertified macro fields are not
candidate features. Feature selection and median preprocessing are fitted
inside each historical fold only, and every fitted training label ends
strictly before its fold decision.

## Canonical product entrypoint

Run:

```bash
python3 -m portfolio.build_canonical_product
```

The command refuses a non-empty target and writes
`artifacts/canonical/corrected_us_annual_3y_product/manifest.json`. It consumes
only the pinned P3 prediction table, independently revalidates its two-role
row lineage and decision eligibility, and uses only frozen Session 8E raw
payloads for candidate-wide pre-prediction liquidity.

The frozen P4 manifest SHA-256 is
`28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7`.
The route retains all 43,806 source candidates, evaluates liquidity for all
1,218 rows passing the fixed non-liquidity/model gates, and records 1,013
liquidity passes. Twelve decision periods from 2015 through 2026 form exactly
15 equal-weight holdings each. The latest frozen 2026 shortlist has 15 rows
and SHA-256
`93bd6104a73e3752da019f7767cc7dfa72bfca837149179dd72b9b87bf466a3b`.
Its concise explainable report is `report/product_report.md` inside the
artifact, SHA-256
`dc3c719cee44f243c4b088c70a50c13187284832accaa867bef469cea044faba`.

The P4 artifact does not calculate performance. It freezes the accepted
three-year overlapping-vintage clock and flat 25-bps-per-side
actual-traded-notional cost policy, then fails official performance closed
because two inputs are absent: an evidence-backed security/action market
ledger and the immutable 2026-07-17 `DGS1MO` ALFRED vintage. Frozen Yahoo
payloads remain liquidity evidence only; old V3 performance and uncertified
rate or event fallbacks are prohibited. The later B1E artifact calculates only
the separately labeled free-source V1 research result; it does not alter P4 or
promote that result to provider-certified performance.

## What to do next

Session A1 completed dependency-safe retirement after US1C without changing
the product route. The one authoritative command remains:

```bash
python3 -m workflows.run_us_free_v1
```

It verifies P2 -> P3 -> P4 -> US1A -> US1B, the supporting frozen boundaries,
the five partial M1C attempts, the exact unchanged shortlist, and the frozen
release-consolidation contract. It performs no external request and creates no
artifact by default. The older C1 baseline-only diagnostic remains:

```bash
python3 -m workflows.run_canonical
```

That command validates only the pinned manifests in P2 -> P3 -> P4 order. C2
published the exact 202-file, 481,666,707-byte baseline privately at immutable
revision `aaf056ea115067e42ef9abf9fa93ade75cdd4052`. The authorized C2
checkpoint includes the recovery code and pointers, so a clean checkout can
recover it with:

```bash
python3 -m data_io.retrieve_canonical_from_hf
```

The pointers require this full revision and verify every downloaded byte size
and SHA-256 before exposing the target. Post-publication reconciliation
confirmed private visibility and all 202 paths present with zero missing. For
future separately approved versions, `scripts/publish_canonical` works from
any current directory and automates preparation through recovery verification.
See `docs/CANONICAL_ARTIFACT_PUBLICATION.md`.

D1 completed the versioned US canonical raw-refresh replacement under
`artifacts/canonical_refresh/us/20260730T110301Z/` and produced a review-only,
non-promoted P2 candidate. The pinned P2-P4 route and private revision remain
unchanged.

S1 completed the offline free-source ledger under
`artifacts/security_ledger/us/20260730T141429Z-s1-final/`, reusing and
represerving exact D1 SEC responses without a new external request. It
reconciles all 180 P4 holdings and four benchmark requirements as 0 matched,
135 ambiguous, 49 unsupported, and 0 conflicting. Unsupported exits and
delisting returns remain unavailable; the legacy minus-50-percent scenario is
physically separate and prohibited from observed-label training. This does not
unlock official performance.

E1 completed the offline event/M&A review under
`artifacts/event_review/us/20260730T144043Z-e1-final/`, manifest SHA-256
`e44ff9aa9d2ac3be2ae66e5fb006bba435cdda3eef4c4521b4d1e3362a58caa6`.
It reconciles all 180 holdings as unresolved without inventing an event,
preserves 86 historical and seven live filing-indicator warnings with dated
hash-addressed evidence, routes the current 15-name shortlist to human review,
and prohibits historical LLM knowledge. It reuses all 15 exact S1 submission
responses for the live shortlist and freezes 47 exact SEC primary-document
requests under a non-overwriting destination.

The separately approved collection is complete under
`artifacts/event_review/us/20260730T150604Z-e1-collection-final/`, manifest
SHA-256
`ad14c45402c95e2b652ac7e0f8b98707a44eb4279f3988be602bca5ec77208ee`.
It exhausted exactly 47 frozen SEC Archives requests, independently verifies
all 47 stored responses and 19,120,821 aggregate bytes, and records 46 HTTP
200 primary documents plus one preserved HPK Form 25-NSE HTTP 503 response.
All 47 document rows remain deterministically unresolved, all 15 shortlist
names remain in human review, and zero event or summary claims were created.
Any later live summary is allowed only for retrieved evidence and requires a
source ID, response hash, evidence path, and document locator for every claim.
This does not unlock performance.

The offline retrieved-document continuation is complete under
`artifacts/event_review/us/20260730T154650Z-e1-extraction-v2/`, manifest
SHA-256
`e8f0e81f3a051b801720241235b706f31ed0e68ec34063ca4bccf3f958ba264a`.
It independently reverifies the collection and frozen parent lineage before
processing, retains all 47 exact responses, and extracts 46 exact quoted claims
with reproducible normalized-text locators and full source lineage. All 47
document rows remain deterministically unresolved, and all 15 shortlist names
remain in an explicit human-review queue. Thirteen complete retrieved-document
sets permit citation-only summaries; HPK remains summary-prohibited because of
the preserved HTTP 503 and SSTK because there was no primary-document
candidate. A summary cannot change a deterministic action. This still does not
unlock performance.

The offline citation-by-citation adjudication is complete under
`artifacts/event_review/us/20260730T173110Z-e1-adjudication-v2/`, manifest
SHA-256
`dcbf95776b50139797410674d7f0ca410cfdaa1401917dea4fb47f7b00e9c7c6`.
It independently reverifies the complete extraction, collection, parent E1,
and preservation lineage before processing. Its name- and document-level
tables separately record exact affected security/role, event type/status,
SEC-acceptance publication lineage, date/effective-time precision, bounded
terms, conflicts, ambiguity, and frozen-rule completeness. No document or name
meets the complete deterministic contract: all 47 document rows and all 15
names remain unresolved and require explicit human sign-off. HPK and SSTK
remain summary-prohibited; summaries for the other 13 names are claim-cited
and cannot change the action. This still does not unlock performance.

The local-only B1 evidence decision remains `unavailable_fail_closed` for a
provider-certified or survivorship-complete claim: S1 has 0 matched, 135
ambiguous, and 49 unsupported requirements; E1 provides no deterministic
historical coverage upgrade; no preserved artifact satisfies the exact
2026-07-17 `DGS1MO` ALFRED-vintage contract. B1D now implements the complete
P4 independent overlapping-vintage contract, but that implementation cannot
upgrade the evidence to certified status.

Q1 is complete. It classified the frozen P3 evidence as moderate predictive
signal: LightGBM pooled Spearman IC is 0.138 across 16,597 labeled OOS rows and
positive in all ten labeled decision years; the tree has pooled ROC AUC 0.569
but weak calibration. Mature-year label coverage is 62.4 percent and strongly
conditional on size/price support, so the result supports a controlled
backtest rather than a proven-performance claim.

The free-data V1 sequence B1B-B1E is complete. The controlled artifact is
`artifacts/performance/free_data_v1/20260801T011135Z-b1e/`, manifest SHA-256
`23588207812e2e950d3e521c6f2048e7607f20b99b691407c449ceb7752bf37c`.
It runs only nine matured 2015-2023 vintages, accounts for all 135 holdings and
$1.8M planned exposure, observes all 5,130 required stock and assigned-
benchmark sessions with zero gaps, and keeps 45 open 2024-2026 holdings
outside completed metrics. No explicit terminal-exit trigger exists, so all
four physical performance namespaces are equal with zero scenario-imputed
capital. The aggregate net zero-rate diagnostic records 18.69% CAGR versus
9.42% for its benchmark, 24.26% volatility, -30.32% maximum drawdown, and
0.826 Sharpe. Exact DGS1MO-dependent Sharpe, Sortino, and alpha remain
unavailable.

These are free-source historical research results, not survivorship-complete
or provider-certified performance, personalized advice, or a future-
performance promise. The user has deferred V2A and paid-data exploration. M1A
is complete as a contract-only artifact at
`artifacts/modeling/nested_walk_forward/20260801T000000Z-m1a/`, manifest
SHA-256 `a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc`.
M1B is also complete in `modeling/nested_walk_forward.py` and
`tests/modeling/test_nested_walk_forward.py`. It implements the frozen nested
folds, selectors, preprocessing, deterministic grids, regimes, predictive
objectives, simplicity rules, one-shot state, and full lineage, with 21
synthetic fail-closed tests and no historical execution or generated artifact.
M1C is complete under the non-overwriting artifact
`artifacts/modeling/nested_walk_forward/20260801T121426Z-m1c/`, manifest
SHA-256 `125cb5d6a8012c3b03ee6eab5f00ac944135c39996e992ecf40d8816acdacc58`.
Its row-complete table has 87,612 unique source-row/model-role records, 20,142
matured metric-eligible OOS predictions, 24,566 open 2024-2026 production
scores outside predictive metrics, and 516 explicit 2027-2028 future
exclusions. Pooled 2019-2023 predictive-only evidence is Spearman IC 0.335 for
LightGBM and ROC AUC 0.654 for the tree over 10,071 rows per role. This is not
portfolio performance.

M1D is complete under the non-overwriting artifact
`artifacts/performance/m1d/20260801T162953Z-m1d/`. Its pre-performance lock
manifest SHA-256 is
`757e19cd9e35290a6b339f79e2c44a0f1ddb47c03b913930b1abda84f0bf74bc`,
and its final manifest SHA-256 is
`b04cea8236da6cd92749410f6186360be5f29dbbbeb00a64f2ed07c180cc72ab`.
Exactly one composite route was frozen from the 16 predeclared inner-evidence
winners before any M1D performance was observed, then evaluated once through
the unchanged P4 portfolio and B1D/B1E contracts. The primary namespace has
19.75% aggregate net CAGR, 0.854 zero-rate diagnostic Sharpe, 24.50%
annualized volatility, -29.46% maximum drawdown, and 2.0 turnover. The 30%
net-CAGR and 1.0 diagnostic-Sharpe reporting thresholds are both not met; the
misses are final and did not trigger a retry. All 75 matured 2019-2023
holdings are included in completed metrics, while all 45 open 2024-2026
holdings remain physically recorded outside them. Exact DGS1MO-dependent
metrics and provider-certified or survivorship-complete claims remain
unavailable. No follow-on I1, A1, REL1, or deferred V2A task is authorized by
this result.

Session I1 completed the first international adapter test only. Canada (`CA`,
TSX/TSXV) was frozen before downstream results from local evidence under
`artifacts/international/i1/20260801T180000Z-i1-ca/`, manifest SHA-256
`d3f17854cf0839713163e8dda99aefedd6ba064a14a042a886770818a472d9f6`. The
adapter normalizes 9,207 preserved legacy snapshot rows into stable P2
identities with CAD/IFRS semantics. Certified filing timestamps, local
calendar/benchmark vintage, actions/delistings, dated FX, survivorship-
complete targets, and P3/P4 downstream compatibility fail closed. No external
data was collected and the US route is unchanged.

Session US1A completed the final offline US free-data product contract under
`artifacts/product/us_free_v1/20260801T183000Z-us1a/`, manifest SHA-256
`f27773c0e1fb92a25707e0fec363e13afa7ccb10f8ace7537187c6be58575edf`.
The data-only gate review retains accepted P2: D1 has exact schema and complete
internal lineage but differs by 242 P2-only and 76 D1-only stable rows and
cannot be consumed by frozen M1C without retraining or changing its pinned P2
contract. The artifact applies the accepted M1A/M1C inner-evidence route to
unchanged P4 gates and liquidity, retains all 43,806 candidates and 87,612
row-role scores, and produces one 15-name 2026 equal-weight research shortlist.
Five names map to exact existing E1 stable-row/security evidence and remain
unresolved; ten carry explicit `event_evidence_not_collected` status. No model,
performance, external collection, or promotion action occurred.

Session US1B completed the separately approved frozen-shortlist evidence pass
under
`artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b/`, manifest
SHA-256 `f802ec26358d27afa21c99490e598e13c98a6ad60229705769f3fae1818d9dcd`.
It reused 29 preserved D1/E1 responses without a request and retrieved exactly
the 36 approved SEC/Nasdaq Trader URLs once each; all returned HTTP 200. The
artifact separately records 15 market/exchange identity rows, 47 primary-
document adjudications, 46 cited claims with reproducible locators, and 15
name states. Fourteen names are unresolved and HPK is failed-request because
its preserved E1 Form 25-NSE response remains HTTP 503. Every name retains an
unresolved deterministic action and explicit human review. The US1A baseline,
scores, gates, liquidity decisions, holdings, ranks, and weights are unchanged,
and no performance was run.

Session US1C froze the local release-consolidation contract at
`docs/US1C_RELEASE_CONSOLIDATION_CONTRACT.json`, SHA-256
`d7098153542afa77ff09e850980362eb957b3e24cd609a4984451289595f1d47`.
Two fresh non-overwriting candidate assemblies were byte-identical. Exact,
read-only recovery of the accepted P2/P3/P4 baseline from private immutable
Hugging Face revision `aaf056ea115067e42ef9abf9fa93ade75cdd4052`
verified 202 files and 481,666,707 bytes with no mutable-revision fallback and
no remote mutation. The resulting local candidate is
`artifacts/product/us_free_v1_release_candidate/20260801T210000Z-us1c/`,
manifest SHA-256
`a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466`,
with 24 records and 1,039,444 recorded bytes. It is a local verification and
release-candidate artifact, not a release.

Session A1 froze `docs/A1_ARCHIVE_CONTRACT.json`, SHA-256
`1b8174bac5181b68f6e3913c2e6bf73a7271acce9f1421063e40b39db2a63f3a`,
after inventorying and hashing all 303 tracked and 53 non-ignored untracked
files. Four confirmed superseded payload groups were packaged under
`artifacts/archive/a1/20260801T220130Z-a1/`; archive manifest SHA-256 is
`892cf0991b9e1a5a651728a1329bd91422037c62de6d57ee27adcc1e919399da`.
The packages preserve 887 source files and 1,954,687,841 source bytes. All four
were recovered byte-for-byte before their original roots became exact-manifest
plus archive-pointer stubs. Eleven associated tracked historical files were
also recovered byte-for-byte from `codex/legacy-archive` before retirement.

V3.1-V3.3 remain in place because active parity tests and the frozen M1A
contract still reference them. Canonical P2-P4, corrected source evidence,
D1/S1/E1/B1/M1/I1/US1A-US1C, immutable Hugging Face pointers, international
structure, and shared implementations remain unchanged. The A1 packages are
local and Git-ignored; no remote upload or durability claim was made for them.

Verify the complete candidate offline with:

```bash
python3 -m workflows.run_us_free_v1 \
  --us1c-artifact-root artifacts/product/us_free_v1_release_candidate/20260801T210000Z-us1c \
  --expected-us1c-manifest-sha256 a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466
```

Verify US1B offline with:

```bash
python3 -m portfolio.us1b_frozen_evidence \
  --artifact-root artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b \
  --verify-only \
  --expected-manifest-sha256 f802ec26358d27afa21c99490e598e13c98a6ad60229705769f3fae1818d9dcd
```

Verify US1A alone offline with:

```bash
python3 -m portfolio.build_us_free_product \
  --artifact-root artifacts/product/us_free_v1/20260801T183000Z-us1a \
  --verify-only \
  --expected-manifest-sha256 f27773c0e1fb92a25707e0fec363e13afa7ccb10f8ace7537187c6be58575edf
```

Stop after A1. Do not commit, push, tag, promote, release, publish, upload the
A1 packages, collect new evidence, retry the exhausted E1 plan, buy or trial a
provider, retrain or retune the frozen M1 route, rerun M1D trading performance,
or begin another market, REL1, or deferred V2A without separate authorization.

## Basic test command

```bash
python3 -m pytest tests/ -x -q
```

The combined B1C/B1D/B1E focused boundary has 35 passing tests. The B1E final
verification run produced 800 passed, 4 skipped, and 78 existing warnings.
The later live-collection E1/S1/fail-closed monthly-NAV boundary has 29 passing
tests. The final extraction/adjudication E1 boundary has 21 passing tests; the
full suite was not rerun for the isolated offline continuations. The M1B
synthetic boundary has 21 passing tests; its broader focused M1A/P3/modeling
compatibility boundary has 67 passing tests and 13 existing warnings. The
final focused M1C/M1B/M1A/P3 boundary has 73 passing tests and the same 13
existing warnings. M1D's complete preflight and final focused M1D/M1C/M1B/
M1A/P3/P4/B1D/B1E/monthly-NAV boundary each pass 135 tests with 13 existing
warnings. The full suite was not rerun for bounded M1C or M1D.
The focused US1A contract/product boundary passes 6 tests with the same 13
existing matplotlib/pyparsing warnings. The complete focused US1A/product/
prediction/P4/shortlist/lineage boundary passes 89 tests. The final full suite
passes 853 tests with 4 skips and the same 78 existing pandas warnings.
The dedicated US1B evidence/lineage/failure boundary passes 6 tests, and its
broader evidence/extraction/adjudication/product/shortlist regression boundary
passes 39 tests with 13 existing warnings.
The final post-US1B full suite passes 859 tests with 4 skips and the same 78
existing Step 5/Step 6 pandas warnings.
The dedicated US1C consolidation boundary passes 12 tests, and the broader
consolidation/reconstruction/recovery/immutability/non-overwrite/failure-path
boundary passes 37 tests with 13 existing matplotlib/pyparsing warnings. The
single final post-US1C full-suite run passes 871 tests with 4 skips and 78
existing Step 5/Step 6 pandas warnings.
The A1 dependency/recovery/import/workflow/failure-path boundary passes 47
tests with 13 existing matplotlib/pyparsing warnings. The post-retirement US1C
route passes, including the expected candidate manifest hash. The single
completed final post-A1 full-suite run passes 869 tests with 4 skips and 78
existing Step 5/Step 6 pandas warnings.

Tests demonstrate code behavior; they do not establish data freshness,
survivorship-free coverage, or future performance.
