# Codex Handoff

## User goal — do not narrow or expand it

Build one simple, workable research-to-screening product in this order:

1. Collect freely available ticker-level source data and preserve the source or
   normalized bronze inputs.
2. Build a cleaned longitudinal point-in-time dataset with multiple rows per
   security, reflecting when information became available. Include useful
   fundamental, momentum, growth, quality, macro, liquidity, and fraud-risk
   features plus future-price targets.
3. Define leakage-safe train/validation/test or walk-forward splits before
   feature analysis. Feature selection and preprocessing must be fold-local.
4. Analyze and rank features, then train both interpretable/tree-based and more
   sophisticated models.
5. Backtest only out-of-sample predictions with point-in-time membership,
   delisting/event handling, transaction costs, and honest limitations.
6. Apply a small-cap opportunity gate and a minimum-liquidity gate, improve
   fraud-risk filters later, construct portfolios, and produce an explainable
   report.

Fraud risk is one of five factors (Value, Quality, Momentum, Growth, Fraud
Risk), not the product identity. The immediate need is consolidation of the
existing implementation, not another methodology program.

## Corrected repository assessment

The repository substantially implements the intended layers. The problem is
not absence of work; it is that the usable path is split between the main
pipeline, older production artifacts, and later PIT-correction/validation
artifacts.

Confirmed existing capabilities:

- A six-stage dataset pipeline, feature library, and enrichment modules.
- A manifest-backed 367-column cleaned historical dataset and its source
  Parquets.
- Temporal/walk-forward splitting, fold-local feature-selection machinery, and
  preprocessing controls.
- LightGBM, regression, and interpretable decision-tree model paths.
- Five-factor alpha scoring, liquidity gates, backtesting, portfolio
  construction, and reporting code.
- Hundreds of tests; the most recent documented full run was 674 passed and 4
  skipped.
- Extensive PIT, target-availability, transformation, survivorship, monthly
  NAV, prediction-consumption, and market-input validation work.

Do not describe the project as having achieved nothing. It produced substantial
code, data, tests, and evidence. The failure was repeated widening and
certification work without consolidating those results into one obvious
canonical dataset-to-product route.

## Important corrections from the 2026-07-17 documentation review

### Source-data preservation

Earlier language calling raw-data preservation simply "incomplete" was too
broad. In the practical pipeline sense, source/bronze data is preserved:

- `data/snapshots.parquet` — normalized raw financial observations
- `data/prices.parquet` — price history
- `data/historical_dataset_clean.parquet` — cleaned feature dataset
- `data/ARTIFACT_MANIFEST.json` — hashes, provenance, generation metadata
- per-market snapshot files handled by the Hugging Face transfer utilities

`data_io/push_to_hf.py` and `data_io/pull_from_hf.py` explicitly upload,
download, and verify these artifacts. The configured dataset repository is
`ekrash718/stock-screener-data`.

Local evidence checked on 2026-07-17:

- `snapshots.parquet`: 33,312,586 bytes
- `prices.parquet`: 39,911,793 bytes
- `historical_dataset_clean.parquet`: 88,494,877 bytes
- `ARTIFACT_MANIFEST.json`: present, with `artifacts`, `generated_at`,
  `generated_by`, and `source_commit` metadata
- corrected SEC Company Facts evidence: 6,981 compressed payload files,
  approximately 761 MB
- contract-aligned raw evidence: 4,837 files, approximately 306 MB
- the broader `artifacts/pit_validation/` tree was previously measured at
  approximately 4 GiB

Exact HTTP-response-byte preservation is a stricter forensic standard. It is
present for important corrected/validation subsets, but has not been proven for
every historical API request. Do not confuse that narrower caveat with loss of
the practical Parquet source layer stored on Hugging Face.

### Feature dictionary

`docs/FEATURE_DICTIONARY.html` is a substantial, intentional explanatory
dictionary for the 367-column dataset. It documents the pipeline, feature
families, raw financial fields, derived ratios, momentum/macro inputs, targets,
model outputs, and fraud features. It is curated and groups some feature
families rather than providing a separate machine card for every column. Do not
call it incomplete without making that distinction.

### Survivorship bias

The project did extensive survivorship work, including:

- `pipeline/step0_historical_universe.py` and SEC full-index research
- historical-universe coverage reporting
- stale/delisted-company handling and sensitivity policies
- observed-only versus policy-imputed outcome separation
- corrected SEC provenance rebuilding
- contract-aligned labels and corrected feature populations
- fail-closed production/validation populations

The honest remaining limitation, stated in `docs/PIT_AUDIT_FINAL_REPORT.md`, is
that the dataset is historically enriched but not comprehensively
survivorship-free. Free SEC and market sources do not provide CRSP-quality
historical exchange membership, security-type/ticker history, delisting terms,
and returns. This is a source-coverage boundary after substantial mitigation,
not evidence that survivorship was ignored.

## Why the project became confusing

- The main data/model path and later corrected PIT artifacts were not folded
  back into one canonical product route.
- Validation sessions accumulated parallel manifests, reports, and artifact
  namespaces.
- Older documents sometimes describe an issue as resolved while later audits
  qualify the result.
- Historical-performance certification displaced the simpler dataset-first
  product objective.
- The prior active product framing narrowed the goal too far to merely
  producing a current shortlist.

## Product Session P1 decision — completed 2026-07-29

P1 completed the bounded read-only canonicalization pass. No repository file,
dataset, model, report artifact, or external state was changed during P1.

### Canonical foundation

Use the corrected US annual evidence chain:

1. `artifacts/pit_validation/corrected_step2/`
   - manifest SHA-256 `899cffd7...`;
   - frozen 8,021-CIK universe and raw SEC Company Facts responses;
   - 163,216 certified rows, including 43,806 annual rows.
2. `artifacts/pit_validation/contract_aligned_label_inputs/`
   - manifest SHA-256 `0ab15685...`;
   - frozen market payloads, mappings, calendars, benchmarks, events, and
     horizon labels.
3. `artifacts/pit_validation/corrected_feature_population/`
   - manifest SHA-256 `9c1e4b82...`;
   - 43,806 stable observed-only US annual rows with decision timestamps,
     explicit label support, and availability/label provenance.

`data/historical_dataset_clean.parquet` remains valuable historical and
multi-market evidence, but it is not the canonical PIT dataset. Its 58,190 rows
do not carry the corrected stable-row, availability, decision, label-end, and
prediction-lineage contract.

The 8F `include_policy_imputed` table is not the primary dataset: it is
identical to `observed_only` in all non-population values because Session 8E
added zero policy-only labels.

### Main-pipeline reconciliation

- Step 1 remains the frozen universe input, not proof of historical exchange
  membership.
- Legacy Step 2 is superseded for the canonical US route by
  `pipeline/step2_artifact_rebuild.py`.
- Legacy Step 3 price/target materialization is superseded by
  `pipeline/build_contract_label_inputs.py` and the pre-decision price features
  in `pipeline/build_corrected_feature_population.py`.
- Legacy Step 4 macro values are excluded because no certified
  vintage/release-lag macro input exists.
- Current Step 5 is reusable after the P2 availability fix.
- Current Step 6 is reusable only with imputation and the inferred
  survivorship policy disabled.
- Fraud taxonomy remains reusable under certified availability provenance.
- `workflows/run_pipeline.py` remains a legacy orchestrator and must not be
  relabeled as the canonical corrected route.

### Confirmed blocker to direct 8F adoption

Direct inspection found:

- `event_time_materialization_timestamp` is non-null for 0 of 43,806 rows;
- `step5_winsorization_methods` contains empty method mappings;
- event-time Beneish, Altman, and sector-rank outputs were dropped as all-null;
- the five-factor alpha computation produces zero non-null Value, Quality,
  Momentum, Growth, Fraud Risk, and composite scores.

The cause is `pipeline/event_time_cohorts.py::proven_availability()`. Corrected
SEC filing availability is represented as America/New_York end-of-day converted
to UTC, often crossing into the next UTC calendar date. The validator compares
that UTC date directly with the date-only SEC `filed_date` and rejects otherwise
valid rows. V3.1 had to rematerialize affected gates as a downstream workaround.

This defect does not invalidate certified accounting values, row identity,
market evidence, or labels. It blocks promotion of the current 8F feature
output as the canonical dataset.

### Supported downstream implementations

- Fold-local selection/preprocessing and both model roles:
  `modeling/fold_lineage.py` plus `modeling/build_session_v3_2_oos.py`.
- Strategy table and contract: `modeling/freeze_session_v3_1.py`.
- Candidate-wide liquidity and equal-weight holdings:
  `portfolio/build_session_v3_3_holdings.py`.
- Fail-closed OOS consumption: `modeling/prediction_lineage.py`.
- Canonical performance math: `backtest/monthly_nav.py`.

There is no connected canonical backtest or report yet. V3.3 stops at holdings;
the market-ledger adapter is absent, and `research/generate_reports.py` still
consumes legacy saved results. The archived V3.4 collection remains paused.

### Classification

- Canonical candidates: corrected Step 2, 8E market/label evidence, a repaired
  observed-only 8F table, V3.1/V3.2 model route, V3.3 selector, and canonical
  monthly NAV.
- Historical evidence: the legacy clean dataset, saved models, saved backtests,
  portfolio registries, and historical reports.
- Alternative research path: Session 9 multi-horizon logistic/ridge outputs.
- Future archive candidates after dependency confirmation:
  `corrected_partial`, `corrected_partial_inputs`, and the Session 9B
  zero-holdings freeze.
- Nothing is authorized for deletion or archival in P2.

## Frozen decisions for Product Session P2

- Do **not** execute the archived V3.4 329-request market-ledger collection.
- Do not require Sharadar access or new external credentials.
- Do not collect or refresh data, retrain models, calculate performance, or
  begin another audit/recovery chain.
- Do not overwrite corrected evidence or `data/historical_dataset_clean.parquet`.
- Do not introduce another strategy, model family, target, factor, or data
  vendor.
- Keep P2 to the corrected US annual observed-only data route. International,
  quarterly, macro, model, backtest, portfolio, and reporting integration are
  later sessions.
- Preserve the separate policy-sensitivity namespace, but do not promote it as
  the primary dataset when it adds no rows.
- Prefer promoting and connecting existing corrected components over creating
  another validation program or arbitrary version namespace.
- Preserve historical reports as evidence, but do not present their performance
  numbers as current product claims.

## Product Session P2 implementation — completed 2026-07-29

P2 established the canonical corrected US annual observed-only data route. No
external service, source refresh, model training, backtest, performance
calculation, V3.4 action, archive, commit, or push occurred.

### Availability correction

`pipeline/event_time_cohorts.py::proven_availability()` now preserves the
existing same-UTC-calendar representation and, only for
`sec_primary_filing`, also accepts a filing when the availability timestamp's
America/New_York source-local date equals date-only `filed_date`.

The change does not broaden the provenance allowlist. Missing/estimated
provenance, a source-local/UTC mismatch under both accepted SEC
representations, later duplicate versions, and unresolved equal-time
collisions still fail closed. Non-SEC proven sources retain the prior
UTC-calendar comparison.

Direct evidence from the frozen corrected Step 2 annual population:

- 43,806 rows have `sec_primary_filing`;
- 0 matched `filed_date` by UTC calendar date;
- all 43,806 matched after conversion to America/New_York;
- all 43,806 now pass `proven_availability()`;
- duplicate entity-period keys remain 0.

### Canonical entrypoint and artifact

Command:

```bash
python3 -m pipeline.build_corrected_feature_population
```

Canonical root: `artifacts/canonical/corrected_us_annual/`

Primary dataset:
`outputs/observed_only/features_taxonomy.parquet`

The builder refuses to reuse a non-empty root. Its manifest pins:

- corrected Step 2 manifest:
  `899cffd7a9d1dc3395a08bee5c65ad4a5e8a109a83c63346ac54c891fe706e08`;
- Session 8E manifest:
  `0ab15685a445f09919b19393e074b8b5903afef7e12defc7c44aeac624f4581a`;
- corrected Step 2 certified snapshots:
  `fb7e5ac206f257b16606d0c4e4bf67c68243a987dcda6f3d3af9e0d35cb6f83d`.

Canonical outputs:

- manifest: 33 artifact records, 8 code-lineage records, 3 validated inputs,
  SHA-256
  `40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3`;
- observed-only primary Parquet: 59,465,346 bytes, 43,806 rows, 339
  columns, 43,806 unique stable IDs, SHA-256
  `46ed33affd8cc66df1f11dc8a41d6f804d106f0a2f8841509cbc931de013e56f`;
- separate policy-sensitivity Parquet: 59,465,391 bytes, 43,806 rows, the
  same stable-ID set, zero policy-only additions, SHA-256
  `b51e749e026e30265fe2677909627b340cc50c395e51036b1ac62809f6cf486d`;
- validation summary SHA-256
  `891e30e73b2a1c68d7ffe7ea181e301ebfc81c5caba89f29ebe31589bdf8d0a5`;
- configuration SHA-256
  `6b3ca85cef6059bb0cc7b443c30057952ee65bada494042e429ae30d54c703e1`;
- support summary SHA-256
  `9dd0e50bd4154eae50f99aac3e179a558d60520a27ee0875d6e629fa7a7cbe31`;
- total canonical artifact size: 436,240,397 bytes.

Independent post-build validation rehashed all 44 manifest/input/code
references with zero failures. The primary stable-ID set exactly matches the
frozen 8F observed-only population.

### Coverage result

- Event-time materialization: 43,806 of 43,806.
- Non-empty Step 5 PIT transform methods: 43,806 of 43,806.
- Beneish M-score and Altman Z-score: 43,806 non-null each.
- Sector-relative outputs: 18 fields, with per-field non-null counts from
  19,208 to 39,647; exact counts are frozen in the manifest.
- Every configured non-ML prerequisite for Value, Quality, Momentum, Growth,
  and Fraud Risk has positive non-null support; exact per-column counts are
  frozen in the manifest.
- Required pre-decision price family: 26,232 supported, 11,857 unavailable,
  5,717 excluded.
- Observed labels: 24,127 supported at 6m/1y; 21,492 at 2y; 19,025 at 3y;
  14,514 at 5y. Each horizon also retains the explicit unavailable/excluded
  partition in the support summary.
- Certified macro vintage support remains 0. No macro value or interaction was
  synthesized.

The scope remains US annual only. The primary population remains observed-only.
The separate policy namespace is retained but adds zero rows. The population
is historically enriched, not comprehensively survivorship-free: the free
sources still lack CRSP-quality historical exchange membership,
security-type/ticker histories, delisting terms, and delisting returns.

### Verification

- Focused P2 availability/builder contracts: 18 passed.
- Broader Step 5/6/taxonomy boundary: 152 passed, 78 existing pandas warnings.
- The first full run produced 679 passed, 4 skipped, 78 warnings, and one
  historical Session 8F lineage failure because P2 intentionally changed three
  files frozen by the old manifest.
- The historical Session 8F validator was narrowed to accept code drift only
  when the exact replacement file is size- and hash-pinned by canonical P2
  manifest
  `40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3`.
  It does not redirect or execute Session 9.
- Focused lineage reconciliation plus P2 regressions: 19 passed.
- Definitive full suite: 680 passed, 4 skipped, 78 existing pandas warnings in
  82.66 seconds.

Frozen evidence remained unchanged:

- old 8F manifest:
  `9c1e4b82c2e7bc2a85228adf6668b797acec827e2bd1ff7c58d20cfb6a9ac01a`;
- `data/historical_dataset_clean.parquet`:
  `520a9b52e2a63d013a3527abbcde32c484a226c2739450d2a6a48ab175144dae`.

## Product Session P3 implementation — completed 2026-07-29

P3 established one canonical leakage-safe research/model command from the P2
US annual observed-only contract. No external source, refresh, backtest,
portfolio, performance calculation, V3.4 action, archive, commit, or push
occurred.

### Canonical entrypoint and fail-closed source contract

Command:

```bash
python3 -m modeling.build_canonical_research_model
```

Canonical root:
`artifacts/canonical/corrected_us_annual_3y_research_model/`

The builder refuses a non-empty target. It accepts only canonical P2 manifest
SHA-256
`40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3`
and independently validates the size and SHA-256 of exactly four consumed
same-artifact records:

- `outputs/observed_only/features_taxonomy.parquet`;
- `inputs/observed_only_labels.parquet`;
- `inputs/observed_only_row_horizon.parquet`;
- `support/feature_population_summary.parquet`.

The cross-source contract reconfirmed 43,806 unique stable feature rows,
219,030 unique row-horizon support rows, and 19,025 observed three-year labels.
The same 19,025 identities are `supported`; the other 19,064 are
`unavailable`, and 5,717 are `excluded`. Policy-imputed three-year rows remain
zero. Embedded feature-table statuses and targets exactly match the separate
observed label/support records.

### Leakage corrections and temporal folds

Direct code and artifact evidence contradicted two details in the historical
accepted V3.1/V3.2 route:

- its 119-column frozen pool contained `observed_excess_return_3y`, the realized
  three-year outcome itself;
- 403 P2 rows have filing availability and event-time materialization after
  their assigned decision timestamp.

P3 does not carry the realized outcome forward as a feature. It freezes 200
semantically eligible numeric source columns without using full-population
missingness, explicitly prohibits all target/support fields and the unavailable
macro family, and then applies the existing support threshold, IC selection,
and correlation pruning inside each training fold only. The 403 decision-late
rows remain in the output but fail closed for scoring and training.

The exact P2 decision calendar contains 19 annual folds from 2010 through 2028.
The P2 manifest creation timestamp is the scoring freeze: decisions through
2026 are eligible; the 258 source rows in 2027/2028 remain as explicit future
exclusions for both roles. Every training label has `label_end_date` strictly
before its fold decision, and every training row's source features were
available by that row's own decision timestamp.

The accepted model roles and fixed parameters remain:

- interpretable `sklearn.tree.DecisionTreeClassifier`;
- sophisticated `lightgbm.LGBMRegressor`.

Both roles share the exact temporal folds and candidate contract. Each fitted
fold selects and preprocesses independently. Thirteen folds per role fit from
2014 through 2026; four early folds per role fail closed for no eligible label
history. Training rows range from 82 to 9,134. The tree selects 17–22 features
per fitted fold and LightGBM selects 17–21, below the frozen cap of 28.
Selection fingerprints now cover the complete fold candidate population, not
only the ultimately selected columns.

### OOS artifact and hashes

P3 manifest:
`8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2`
(63,862 bytes).

Primary OOS predictions:
`predictions/oos_predictions.parquet`, 7,512,841 bytes, SHA-256
`85164532421aec9909670f16fd56b4dcc9c5bcaaf7273c1ab98d1c464a7242a2`.

The Parquet contains exactly 87,612 unique row-role records:

- 38,894 available OOS tree predictions;
- 38,894 available OOS LightGBM predictions;
- 4,251 early no-history exclusions per role;
- 403 decision-late source exclusions per role;
- 258 future-decision exclusions per role;
- zero in-sample predictions.

Every source row appears once for each role. Every row carries the pinned
source manifest, dataset, labels, and row-support artifact IDs. Every available
prediction carries exact feature-selection, preprocessing, target,
model-configuration, model, training-population, and training-label lineage.

Other key hashes:

- research/model contract:
  `d146518c5adeec47ccec9936c6cd724b5862d43cde1d25a959b9963616ef8329`;
- preflight validation:
  `0ce26bc1a2fc2eeac241a0f28ff7407a6c924c4ecea8c84b44cca555602a168c`;
- OOS lineage validation:
  `5d89976fd305225f5da6cc9334ab3275df52946917ea23ca6a52c42025373c77`;
- fold coverage:
  `9eceed787445c9c2b6355293357bb5132f1ab69247413beca872c1e13a10f10a`;
- exclusions:
  `c435113ad136740ae9ae1d4afba21448c4bf96c0e17f44ef2fbae933a7f88452`;
- verdict:
  `7a79a868913b0369493ef928f161735ec3938f924428d079ff48b837ac55b33f`.

The artifact has 142 files totaling 13,758,028 bytes including the manifest.
Its manifest pins 5 validated inputs, 141 generated records, and 5 code-lineage
records. Independent post-build rehashing found zero missing, size-mismatched,
or hash-mismatched references.

### Verification and limitations

- Focused P3 loader, leakage, fold, preprocessing, and lineage boundary:
  22 passed, 13 existing matplotlib/pyparsing deprecation warnings.
- The first full-suite boundary stopped after 547 passed and 4 skipped on one
  historical V3.3 lineage failure: the old V3.2 manifest correctly detected
  the intentional selection-fingerprint code/test changes.
- The historical V3.3 validator now accepts only
  `modeling/build_session_v3_2_oos.py` and
  `tests/modeling/test_build_session_v3_2_oos.py` when their current size and
  SHA-256 exactly match code-lineage records in canonical P3 manifest
  `8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2`.
  No data, model, prediction, configuration, or other historical record gets
  this reconciliation.
- Focused lineage reconciliation plus P3 regressions: 13 passed, 13 existing
  matplotlib/pyparsing deprecation warnings.
- Definitive full suite: 686 passed, 4 skipped, 78 existing pandas warnings in
  33.05 seconds.

Certified macro vintage support remains zero; no macro value or interaction
was used. The physical P2 policy-sensitivity namespace still adds zero rows and
was not promoted. The population remains historically enriched, not
comprehensively survivorship-free: free sources still lack CRSP-quality
historical exchange membership, security-type/ticker histories, delisting
terms, and delisting returns. P3 calculated no performance and did not connect
predictions to a backtest, portfolio, or report.

## Product Session P4 implementation — completed 2026-07-29

P4 established one canonical prediction-to-shortlist product command from the
P3 row-complete OOS contract. No model was retrained; no external source was
queried or refreshed; no historical V3 prediction or performance output was
consumed; and no V3.4, archive, commit, or push action occurred.

### Canonical entrypoint and artifact

Command:

```bash
python3 -m portfolio.build_canonical_product
```

Canonical root:
`artifacts/canonical/corrected_us_annual_3y_product/`

The builder refuses a non-empty target. Its manifest SHA-256 is
`28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7`
(23,518 bytes). The artifact contains 26 files totaling 31,668,282 bytes,
including 25 generated records, 8 validated inputs, and 5 code-lineage
records.

Key outputs:

- row-complete copied prediction lineage: 87,612 rows × 42 columns, SHA-256
  `c1ae19edeee70f4d32ff22a494104644c9aceac46425449ca72540791f5967ee`;
- candidate table: 43,806 rows × 111 columns, SHA-256
  `3a4cf810a3f666848bcf423cf174277e416d769d12f644b35b0aab98273a170d`;
- long gate table: 525,672 rows × 7 columns, SHA-256
  `91ee7ebd2bad579b8dc2dc9c4f2d87711ce68e5353be29a85ed1d8ac71fa2154`;
- liquidity coverage: 1,218 rows × 27 columns, SHA-256
  `e8cc5533820a06cbdb5e4104685fced7d77dd3bb2d5579f6013105b1e2a6712a`;
- liquidity evidence: 36,540 exact candidate-session rows × 18 columns,
  SHA-256
  `79945f6a4f5c8f877456339f7b16599992c517c91cc5c1a33db48933474f6323`;
- holdings: 180 rows × 453 columns, SHA-256
  `8f12c7f9a3659093186c4674a2f17058c9fe63a3ec40b7d894bc6c81f5dbcdf5`;
- latest 2026 shortlist: 15 rows × 453 columns, SHA-256
  `93bd6104a73e3752da019f7767cc7dfa72bfca837149179dd72b9b87bf466a3b`;
- report traceability: exactly 30 current-shortlist row-role records,
  SHA-256
  `c5331a259b2175f4998ef66f723d938bf0c8ceee45cde2ecd771d177d45942fb`;
- three-year vintage plan: 180 rows × 14 columns, SHA-256
  `80bd3849b372cba8d7654d3d0332a2deec53b33edb3d792da28e28aacbca549a`;
- concise product report: SHA-256
  `dc3c719cee44f243c4b088c70a50c13187284832accaa867bef469cea044faba`;
- fail-closed backtest status: SHA-256
  `d76c3d102dee922c83159a7c132000a143739ceeb96932ce4c783ae8f25ce746`.

### Prediction, eligibility, gate, and liquidity validation

P4 revalidated the exact P3 manifest and prediction hashes before reading any
score. The consumed table contains 87,612 unique stable-row/model-role records,
77,788 finite available OOS predictions, 9,824 explicit exclusions, zero
in-sample predictions, complete source lineage on every row, and complete
feature/preprocessing/target/configuration/model lineage on every available
prediction. Every available row reconfirmed feature availability by its
decision and a maximum training-label end strictly before that decision.

The eight fixed non-model hard gates were independently recomputed from the
pinned P2 feature table. All 43,806 pass flags and exact exclusion-code lists
matched P3. Liquidity became required only after every hard gate, both exact
P3 OOS roles, and tree probability at least 0.55 passed.

Frozen Session 8E supplied existing close/volume evidence only; P4 made no
request. For all 1,218 liquidity-required rows across 633 symbols, it
revalidated the source manifest/indexes and both compressed and uncompressed
payload hashes, then selected exactly 30 frozen regular-session closes strictly
before prediction. There were 1,013 passes, 189 below-threshold exclusions,
and 16 nonpositive-volume exclusions. The median threshold remained exactly
`$1,333,333.3333333333`, derived from `$200,000 / 15 / 1%`.

### Portfolio and explainable report

Ranking occurs only after liquidity pass, descending by the canonical P3
LightGBM three-year model score with stable row ID as the deterministic
tie-breaker. Twelve periods from 2015 through 2026 contain exactly 15 holdings
at weight `1/15`; incomplete early/future periods contain zero holdings.

The latest frozen decision is 2026-07-02. Its 15-row shortlist carries both
model roles, every model artifact ID, raw liquidity response hash, all gate
values, fraud-factor fields, the fold-selected feature names and their exact
decision-row values, and a plain-language rank explanation. The 30-row report
trace table maps each reported holding back to both original P3 row-role
records.

### Backtest, costs, and limitations

P4 freezes but does not execute the accepted annual overlapping three-year
vintage route. The cost policy is exactly 25 bps per side on absolute actual
traded notional, or 50 bps round trip; gross turnover has no one-half
multiplier, later vintages do not rebalance earlier ones, and the legacy
market-cap tiers and 30/60-bps defaults are prohibited.

Official performance is `unavailable_fail_closed`; no return, NAV, Sharpe,
drawdown, or other performance metric was calculated. Exact blockers:

1. no accepted evidence-backed dated security/ticker/exchange,
   adjustment-semantics, and corporate-action market ledger exists under
   `artifacts/pit_validation/session_v3_4_market_ledger_inputs/`;
2. the immutable Federal Reserve H.15 `DGS1MO` ALFRED vintage 2026-07-17 is
   absent.

Frozen Yahoo evidence is sufficient for the pre-decision liquidity gate but is
not silently promoted to canonical security/action/performance evidence. No
old V3 performance result, current/constant/interpolated rate, invented event,
or policy-imputed disappearance outcome is substituted.

The unresolved survivorship boundary remains explicit: the population is
historically enriched, not comprehensively survivorship-free, because free
sources do not provide CRSP-quality historical membership, security/ticker
histories, delisting terms, or delisting returns. Certified macro vintages
remain unavailable and unused. The shortlist is a frozen research output, not
personalized advice or a future-performance claim.

### Verification

- New P4 focused tests: 7 passed.
- Focused prediction-consumption, fold-lineage, P3 loader, monthly-NAV,
  backtest-gate, V3.3 portfolio, and P4 product boundary: 71 passed.
- Independent post-build rehash: all 38 manifest record/input/code references
  matched; all 633 consumed raw-symbol files matched their recorded compressed
  sizes and hashes.
- Output reconciliation: 43,806 unique candidates; 1,218 candidate-wide
  liquidity rows; 180 holdings; 12 supported periods; exact `1/15` weights;
  15 latest ranks `1..15`; and 30 unique current report row-role traces.
- Definitive full suite: 693 passed, 4 skipped, and 78 existing pandas warnings
  in 41.44 seconds.

## Canonical consolidation Session C1 — completed 2026-07-30

C1 made the completed P2-P4 route the obvious active local spine without
changing accepted model, gate, liquidity, portfolio, or performance behavior.

### Active route and shared implementation

Run:

```bash
python3 -m workflows.run_canonical
```

The command verified the accepted manifests in P2 -> P3 -> P4 order. It is
read-only by default, refuses non-empty unknown targets, and may invoke only an
absent stage with `--build-missing`.

Active P3 now imports fold-local fitting, model-role, exclusion, masking, and
exclusion-report behavior from `modeling/oos_modeling.py`. Active P4 imports
the fixed tree threshold, 15-name equal-weight contract, exact
`$1,333,333.3333333333` ADTV threshold, and fail-closed exclusion codes from
`portfolio/selection_contract.py`. Neither canonical builder imports a
historically named `session_v3_*` module. Historical builders and their tests
remain intact.

### Documentation and inventories

- `docs/ARCHITECTURE.md` names the active spine and legacy boundary.
- `docs/CANONICAL_DEPENDENCY_INVENTORY.md` classifies canonical, source,
  shared, international legacy, historical, cache, and unresolved paths and
  records P2-P4 manifest/file counts.
- `docs/ARCHIVE_INVENTORY.md` records dependencies, replacements, allocated
  sizes, manifest hashes, and proposed destinations without moving anything.
- FAQ, production configuration, onboarding, changelog, the old pipeline
  orientation, and `workflows/run_pipeline.py` now distinguish canonical from
  legacy behavior.
- `.github/workflows/refresh_data.yml` no longer schedules the stale legacy
  multi-market refresh. It remains manual opt-in and does not become a
  canonical publisher.

### Verification and preservation

- Focused canonical extraction tests: 19 passed.
- Broader historical/canonical lineage, model, portfolio, and orchestrator
  boundary: 30 passed, 13 existing matplotlib/pyparsing warnings.
- Canonical orchestrator: all three accepted manifest hashes verified.
- Definitive full suite: 699 passed, 4 skipped, 78 existing pandas warnings in
  37.47 seconds.

No external data was collected, no artifact was rebuilt or overwritten, no
Hugging Face action occurred, and no file was archived or deleted. After
explicit approval, the reviewed P2-P4/C1 state was checkpointed and
`codex/legacy-archive` was created at that checkpoint; nothing was pushed. All
P2-P4 artifacts and the legacy international structure remain preserved. The
unrelated untracked `.jupyter_ystore.db` remains excluded and untouched.

## Exact next task

C1 and its authorization-bound checkpoint/archive-branch close are complete.
The next bounded phase is C2 private canonical artifact publication. Begin C2
in a fresh task and pause at the external upload boundary for explicit
approval. Do not collect data, publish, move archive files, commit, or push
without the corresponding explicit authorization.

## High-value references

Read these only as needed after the handoff; do not reread the whole docs tree:

- `docs/FEATURE_DICTIONARY.html`
- `docs/developer/architecture-v2-blueprint.md`
- `docs/architecture/orientation_pipeline_spine.md`
- `docs/architecture/orientation_modeling_alpha.md`
- `docs/architecture/orientation_research_backtest.md`
- `docs/architecture/orientation_quality_orchestration.md`
- `docs/UNIVERSE_COVERAGE_REPORT.md`
- `docs/PIT_AUDIT_FINAL_REPORT.md`
- `docs/PIT_ARTIFACT_ARCHIVE.md`
- `reports/pit_validation/08d_step2_provenance_rebuild.md`
- `reports/pit_validation/08e_contract_aligned_label_market_inputs.md`
- `reports/pit_validation/08f_corrected_feature_population.md`
- `reports/pit_validation/v3_1_production_table_contract.md`
- `reports/pit_validation/v3_2_oos_predictions.md`
- `reports/pit_validation/v3_3_liquidity_holdings.md`
- `docs/archive/VALIDATION_V3_HANDOFF.md`
- `docs/archive/VALIDATION_V3_ROADMAP.md`

## Current repository state

P2-P4 and C1 are preserved in one reviewed checkpoint on the working branch
and `codex/legacy-archive`. The three ignored canonical artifact roots retain
their accepted frozen manifests. No canonical, frozen PIT, legacy dataset,
historical model, prediction, backtest, portfolio, report, international
entrypoint, or archive candidate was overwritten or moved. The archived V3
program remains preserved and paused.
