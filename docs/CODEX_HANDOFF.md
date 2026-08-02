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

## Canonical publication Session C2 — completed 2026-07-30

C2 made the exact accepted P2-P4 baseline privately recoverable from a clean
checkout without changing any legacy repository-root artifact.

### Exact local publication

`python3 -m data_io.publish_canonical_to_hf --prepare` independently rehashed
the three complete ignored artifact roots and reconciled every regular file
against each manifest's generated `records` list:

- P2: 34 files, 436,240,397 bytes, manifest
  `40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3`;
- P3: 142 files, 13,758,028 bytes, manifest
  `8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2`;
- P4: 26 files, 31,668,282 bytes, manifest
  `28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7`;
- total: 202 files and 481,666,707 bytes.

Every planned remote path is under
`canonical/<artifact-name>/<manifest-sha256>/...`. No legacy repository-root
path is present.

### Publication and recovery behavior

- `data_io/canonical_hf.py` contains strict local manifest/root validation,
  authenticated private-visibility proof, current-head and collision
  preflight, parent-pinned atomic commit construction, immutable pointer
  validation, non-overwriting staging, and independent size/hash retrieval.
- `data_io/publish_canonical_to_hf.py` separates local preparation,
  authenticated read-only preflight, explicit double-confirmed publication,
  and post-commit recovery through `--finalize-revision`.
- `data_io/retrieve_canonical_from_hf.py` requires the three tracked pointers,
  one shared full commit SHA, reverified private visibility, and an absent
  target.
- `.github/workflows/verify_canonical_artifacts.yml` is a manual
  secret-backed clean recovery check with no `latest` or branch fallback.
- `scripts/publish_canonical` is a location-independent wrapper, preventing the
  earlier module-resolution failure when publication is launched outside the
  repository.
- Focused C2 verification: 18 passed.
- Full suite: 717 passed, 4 skipped, and 78 existing pandas warnings in
  61.49 seconds.

The fine-grained Hugging Face token was validated and saved outside the
repository through the standard Hugging Face user credential store. Both
credential files were tightened to owner-only mode `0600`. The canonical
client uses `HF_TOKEN` when set and otherwise this saved credential; neither is
printed.

Authenticated read-only preflight verified:

- repository `ekrash718/stock-screener-data`;
- repository type `dataset`;
- private visibility `true`;
- checked parent revision
  `0b8f3baac4c823e0ef89d8a73da11c3f0e88c9db`;
- all 202 destination paths absent;
- zero uploads.

The user explicitly authorized and then explicitly re-authorized the upload
after disclosure that it includes frozen lineage/source-diff records. Codex's
environment prohibited the export, so the user ran the exact guarded command
directly. The publisher repeated its preflight, created one parent-pinned
commit, downloaded every record into a temporary target, independently
verified all sizes and SHA-256 values, and only then wrote:

- `data_io/canonical_artifact_pointers/p2.json`;
- `data_io/canonical_artifact_pointers/p3.json`;
- `data_io/canonical_artifact_pointers/p4.json`.

All pointers validate and share immutable revision
`aaf056ea115067e42ef9abf9fa93ade75cdd4052`. They cover exactly 202 unique
files and 481,666,707 bytes. Independent authenticated metadata reconciliation
at that exact revision confirmed:

- private visibility `true`;
- 202 expected paths;
- 202 present paths;
- zero missing paths.

The pointer files can exist only after the publisher's temporary byte/hash
recovery has completed successfully. Legacy repository-root objects were not
part of the plan and remain unchanged. After separate explicit authorization,
the C2 code, workflow, wrapper, tests, and pointer files were included in one
conventional checkpoint and pushed on `codex/pit-checkpoint`, completing
clean-checkout discovery.

## Canonical refresh Session D1 — completed 2026-07-30

D1 added a separate, versioned US refresh route and executed one explicitly
authorized free-source collection without changing the pinned canonical
baseline, immutable Hugging Face paths, legacy six-step workflow, or
international per-market structure.

### Route and source evidence

Plan-only mode makes no request:

```bash
python3 -m workflows.refresh_us_canonical \
  --refresh-id 20260730T110301Z
```

Collection additionally requires both `--collect` and the exact refresh ID in
`--confirm-external-collection`. The completed version is:

`artifacts/canonical_refresh/us/20260730T110301Z/`

- `universe/`: 8,017 unique SEC CIKs from versioned ticker, exchange, and
  per-CIK submissions evidence; 8,019 terminal raw requests, all successful.
- `filings/`: 8,017 Company Facts requests; 6,988 successful and 1,029
  explicit failures; 162,793 corrected certified rows, 43,640 certified annual
  rows, 53,107 excluded rows, and 2,026 unavailable entities.
- `market/`: 4,813 terminal Yahoo symbol records; 4,790 successful and 23
  explicit failures; 4,782 normalized regular-session price/event files; five
  explicit price, benchmark, calendar, decision, and label-support contracts.
- Every terminal stored SEC/Yahoo response and failure body used by the build
  was reverified by compressed and decompressed size/hash. Terminal collection
  records preserve request timestamps, URLs, response metadata, and hashes.

Interactive development interruptions left 540 superseded Yahoo raw chart
files physically preserved under the version root but unconsumed by
normalization and absent from the terminal latest-response set. Each affected
symbol was recollected without overwriting the earlier file, and the consumed
terminal record has a complete timestamp and exact response hash. The fetcher
now drains completed worker results into the append-only manifest when
interrupted, rate-limits all workers globally to two requests per second, and
applies a shared HTTP-429 cooldown.

### Review-only P2

`p2_review_candidate/` contains a 43,640-row, 339-column US annual
observed-only candidate:

- corrected SEC availability and Step 2 materialization were reused;
- certified accounting and PIT Step 5 transformations retain stable row
  identity through every stage;
- macro support is zero until certified release-vintage evidence exists;
- Step 6 ran with `--skip-imputation --skip-survivorship-policy`;
- inferred delisting returns and policy-imputed rows are both zero;
- P3/P4 consumption is prohibited until separately authorized promotion.

Candidate manifest SHA-256:
`545c2eec17dae8cdffd81fd8e1b89ebc1ccc3b47290b7b556f485bbaa5f436d6`.

`review/comparison/` pins baseline P2 manifest
`40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3`
and compares:

- row identity: 43,806 baseline rows, 43,640 candidate rows, 43,564 shared
  stable IDs, 242 baseline-only, and 76 candidate-only;
- schema: 339 columns on both sides, no added/removed columns, no dtype
  changes;
- feature coverage and missingness for every column;
- all five observed-label horizons, value summaries, price/label gates, and
  explicit exclusion/unavailability reasons;
- universe, Company Facts, price, and benchmark source drift, including
  collection timestamp ranges and changed payload/status counts.

One-year observed label support changes from 24,127 to 24,004 rows; two-year
from 21,492 to 21,381; three-year from 19,025 to 18,928; five-year from 14,514
to 14,440; and six-month from 24,127 to 24,004. These are review differences,
not accepted promotion gates.

Comparison manifest SHA-256:
`20ecca80fd2c75fe999d21c712918e47153a71b378d8441501c460fcce25db2a`.
Review manifest SHA-256:
`ca175587494c1529d21d6e7c7567dbe3b16c55913c9b4b7a84b9b0d1d4569bb9`.

The review manifest rehashed the P2-P4 manifests, corrected source evidence,
calendar contract, all three P2-P4 pointers, legacy workflow, and every
international Step 1/Step 2/orchestrator entrypoint before and after the run.
All preservation snapshots match. The private revision remains
`aaf056ea115067e42ef9abf9fa93ade75cdd4052`.

### Implementation and verification

New modules:

- `pipeline/us_refresh_sources.py`
- `pipeline/build_refreshed_us_p2.py`
- `quality/compare_canonical_p2_versions.py`
- `workflows/refresh_us_canonical.py`

The D1 wrapper drives the existing corrected adapters against caller-supplied
version roots. Step 2 now accepts dynamic US-universe sizes and retains exact
non-success bodies; SEC/Yahoo concurrent requests use global limits without
changing canonical baseline defaults. The final preservation correction
restored `pipeline/build_corrected_feature_population.py` byte-for-byte to the
pinned P2 size and SHA-256. Focused fixtures cover versioning, non-overwrite
behavior, exact raw payload recovery, corrected Step 2 reuse, explicit
contracts, macro fail-closed behavior, observed-only Step 6, comparison
dimensions, preservation, and external-confirmation gates.

Verification:

- complete focused D1/PIT/Step 5/Step 6 boundary: 132 passed, with the existing
  78 pandas performance/future warnings;
- focused D1 plus historical Session 8F lineage after the preservation fix:
  14 passed;
- independent artifact review: 69 component records, 19,797 terminal success
  payloads, and 1,059 failure-attempt payloads rehashed; all raw payloads were
  also decompressed and matched their recorded byte sizes and hashes;
- canonical P2 -> P3 -> P4 verify-only route: all three pinned manifest hashes
  matched;
- definitive post-fix full suite: 735 passed, 4 skipped, and 78 existing
  warnings in 55.50 seconds.

No canonical artifact or pointer was changed. No Hugging Face path was written,
no P2 version was promoted, no file was archived, and no branch, commit, push,
or publication occurred. The unrelated `.jupyter_ystore.db` remains untouched.

## Security ledger Session S1 — completed 2026-07-30

S1 added an offline, immutable security/survivorship ledger builder:

- `pipeline/security_ledger.py` defines provider-neutral issuer, security,
  listing, event, raw-evidence, and coverage schemas plus the adapter boundary;
- `workflows/build_security_ledger.py` builds only from frozen local evidence,
  makes no external request, and refuses mutable IDs or a non-empty target;
- `tests/pipeline/test_security_ledger.py` covers exact byte preservation,
  unresolved Form 25 and Item 1.03 handling, effective-date ambiguity,
  future-interval failure, versioning, and non-overwrite behavior.

Final version:
`artifacts/security_ledger/us/20260730T141429Z-s1-final/`.
Its manifest SHA-256 is
`28317e4ae0126367c38bb40c9fc8169394fc1141c475072d0dc484c141479a1e`.
It contains 148 manifest records totaling 3,688,493 bytes and recopies 136
exact SEC responses with their request URLs, publication/retrieval timestamps,
compressed and decompressed sizes, and hashes.

All 180 P4 holding rows and the four required benchmark instruments are
represented. Coverage is 0 matched, 135 ambiguous, 49 unsupported, and 0
conflicting. Forty-nine Form 25 delisting indicators, 14
registration-termination indicators, and 10 explicit Item 1.03 bankruptcy
indicators are recorded; effective times and outcomes remain unresolved where
the submission index does not prove them. No unsupported ticker/exchange
change, merger, suspension, security-type change, delisting return, or action
term was invented.

Primary outcomes remain unavailable for every ambiguous or unsupported
requirement. The legacy minus-50-percent unsupported-exit scenario is a
physical `sensitivity/` JSON contract, is not an observed fact, and is
prohibited from primary outputs, observed labels, model training, and official
performance. Price and action contracts prohibit adjustment double-counting
and require explicit terms for non-price consideration. No performance was
calculated or claimed.

The builder revalidated the pinned P4, D1 review, and D1 candidate manifests
before materialization. Independent verification rehashed all 148 records and
decompressed all 136 raw responses with zero mismatch. The canonical P2-P4
verify-only route still matches all three accepted hashes. No external request
was needed or made.

Verification:

- focused ledger, observed-label eligibility, and fail-closed monthly NAV:
  26 passed;
- definitive full suite: 739 passed, 4 skipped, and 78 existing warnings in
  42.36 seconds;
- all D1 preservation hashes, including every tracked legacy/international
  path and all three immutable pointer revisions, matched.

## Event/M&A Session E1 — completed 2026-07-30

E1 replaced the old notebook's knowledge-only Groq/Llama M&A warning with one
offline, versioned historical-then-live evidence review:

- `portfolio/event_review.py` consumes the exact provider-neutral S1 schemas,
  applies deterministic event/effective-time rules, validates cited live
  summary claims, builds the historical and live contracts, and independently
  verifies generated records and raw bytes;
- `workflows/build_event_review.py` builds or verifies E1 and makes no external
  request;
- `workflows/collect_live_event_evidence.py` prints the frozen plan by default
  and cannot request a byte without both `--collect` and the exact approval
  token;
- `tests/portfolio/test_event_review.py` covers complete and ambiguous event
  policy, future-publication ineligibility, filing-candidate selection,
  historical LLM prohibition, cited live claims, collection approval,
  exact-byte preservation, versioning, non-overwrite behavior, and the full
  180/15 artifact contract.

### Read-only evidence result and deterministic boundary

The bounded evidence pass confirmed:

- 134 S1 issuer, security, and listing rows have current-snapshot identity but
  no historical effective interval;
- all 73 S1 event rows are filing indicators with no event effective time and
  unresolved exact-security scope;
- 61 indicators across 30 P4 holding requirements were published by their
  decision dates, but none can be converted into a completed event, exclusion,
  backtest action, or return;
- S1 preserves exact submission-index responses and primary-document names,
  but not the primary filing documents themselves;
- the old notebook asked an LLM to decide pending deals from ticker/name and
  model knowledge, with no retrieved citations;
- `backtest/monthly_nav.py` accepts a corporate action only when missing price
  coverage has one dated, provenance-backed resolution; the unresolved S1
  indicators do not meet that contract.

E1 therefore preserves every S1 confidence and ambiguity state. Its policy
defines deterministic warn, exclude, unresolved, and human-review behavior for
pending acquisitions, completed mergers, bankruptcy, suspension, delisting,
registration termination, exchange noncompliance, and other material events.
Event effective time, source publication time, source retrieval time, and P4
decision eligibility remain separate. Filing family, missing price, ticker
disappearance, name similarity, model knowledge, and uncited narrative are
explicitly prohibited event sources.

Historical mode allows no LLM summary or current model knowledge. Evidence
published after a decision cannot change the selection action; post-decision
holding-interval evidence is reviewed separately. Live mode routes ambiguity
to human review, freezes deterministic policy first, and permits a later LLM
only to summarize exact retrieved evidence with a source ID, dated locator,
path, and matching response hash for every claim.

### Final artifact and live approval boundary

Final version:
`artifacts/event_review/us/20260730T144043Z-e1-final/`.
Its manifest SHA-256 is
`e44ff9aa9d2ac3be2ae66e5fb006bba435cdda3eef4c4521b4d1e3362a58caa6`.

The artifact contains 148 manifest records and recopies all 136 exact S1
responses. Historical reconciliation contains all 180 canonical P4 holdings:
all 180 remain `unresolved`, with zero pass, warn, or exclude outcomes because
the exact security/effective-time contract is incomplete. Eighty-six
historical filing-indicator warnings and seven live warnings each retain
publication/retrieval times, request URL, source ID, response hash, evidence
path, hash-addressed evidence URI, accession, form, primary-document name, and
a narrowly supported claim.

The live contract contains the current 15 names and CIKs:
`BXC/0001301787`, `SM/0000893538`, `SSTK/0001549346`,
`CLMB/0000945983`, `ARDT/0001756655`, `AN/0000350698`,
`HPK/0001792849`, `NOG/0001104485`, `CRTO/0001576427`,
`UVE/0000891166`, `UPWK/0001627475`, `AMPY/0001533924`,
`MTDR/0001520006`, `EBS/0001367644`, and `INGM/0001897762`.
All route to human review; no primary document or LLM summary is represented
as collected.

The exact unexecuted collection plan reuses the 15 S1 SEC submissions
responses and proposes only SEC Archives primary filing-document endpoints
under
`https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}`.
It contains 47 exact URLs across forms `25-NSE`, `424B3`, `8-K`, `8-K/A`,
`S-4`, and `S-4/A`, selected for forms or 8-K items `1.01`, `1.03`, `2.01`,
`3.01`, or `5.01`, over the inclusive 2025-07-30 through 2026-07-30 window.
The maximum is 47 requests to the non-overwriting
`live/collection/20260730T144043Z-e1-final/raw/sec/filings/` destination.
Expected raw payloads are HTML/plain-text or XML with a 100-MiB aggregate and
10-MiB per-response fail-closed cap. Exact request and response metadata,
bytes, hashes, and lineage would be preserved. This is a live collection and
requires separate explicit approval before the first request.

### Verification and preservation

- Focused S1/E1/backtest boundary: 39 passed.
- Independent E1 verification: all 148 records, 136 compressed/decompressed
  raw responses, 93 dated warning citations, 180 historical rows, and 15 live
  rows matched.
- Definitive full suite: 752 passed, 4 skipped, and 78 existing warnings in
  64.03 seconds.
- P2, P3, P4, D1 review, D1 candidate, and S1 manifest hashes all matched.
- Every D1-recorded canonical, source-evidence, legacy, international, and
  immutable pointer hash/revision matched before and after the build.

No external request, LLM call, official performance calculation, P2/P3/P4
consumption change, D1 promotion, Hugging Face write, archive, deletion,
branch, commit, push, publication, or scheduled collection occurred.

## Event/M&A Session E1 live collection — completed 2026-07-30

The user separately approved the exact frozen E1 47-request SEC EDGAR Archives
primary-document plan. Before the first request, the parent E1 artifact
independently verified and its manifest SHA-256
`e44ff9aa9d2ac3be2ae66e5fb006bba435cdda3eef4c4521b4d1e3362a58caa6`,
request-plan SHA-256
`ff856cff60eb35279c4f487bff5aaac679c83f22c8518d07d3c66daebd7ea433`,
and collection-contract SHA-256
`4fae9e662079ca479894db93ef67746cae77a1200abe2ebd06244e5e78a38cdf`
all matched.

The collector now uses the repository SEC identification
`CanonicalUSRefresh research@alpharesearch.io`, requests identity encoding,
paces starts at least 0.12 seconds apart, makes no automatic retry, enforces
10-MiB per-response and 100-MiB aggregate caps, preserves exact gzip-wrapped
response bytes plus request/response metadata, and can continue only the
unattempted suffix from an independently verified predecessor.

The initial live version
`artifacts/event_review/us/20260730T150213Z-e1-collection-v1/` stopped after
preserving the nineteenth response because SEC returned HTTP 503 for HPK Form
25-NSE accession `0001354457-25-000836`. The continuation
`20260730T150421Z-e1-collection-v2` reused those 19 verified responses and
issued only the 28 unattempted frozen URLs. The final offline freeze, which
made no new external request, is:

`artifacts/event_review/us/20260730T150604Z-e1-collection-final/`

Its manifest SHA-256 is
`ad14c45402c95e2b652ac7e0f8b98707a44eb4279f3988be602bca5ec77208ee`.
Independent verification rehashed all 59 manifest records, decompressed and
rehashed all 47 stored responses, matched all requested URLs to the frozen
plan in order, and confirmed 19,120,821 aggregate response bytes. The largest
response is 4,797,907 bytes. Final status is 46 HTTP 200 primary documents and
one preserved HTTP 503 response; the request plan is fully exhausted, while
primary-document retrieval is explicitly incomplete for that one HPK filing.

Deterministic policy was applied to all 47 planned document rows before any
summary. No exact security scope, event type/status, complete terms, or
effective time was inferred from filing family. All 47 remain `unresolved`,
all 15 shortlist names remain in human review, unsupported inference flags are
zero, and summary-claim rows are zero. The updated live contract allows a
later summary for 13 names with complete retrieved document sets only when
every claim includes a retrieved source ID, exact response hash, evidence
path, and document locator. HPK remains summary-prohibited because one response
is HTTP 503; SSTK remains summary-prohibited because the frozen plan contained
no primary-document candidate.

No dataset version was promoted; P3/P4 consumption, the P2-P4 manifests, D1,
S1, private Hugging Face revision
`aaf056ea115067e42ef9abf9fa93ade75cdd4052`, legacy workflows, and
international/per-market structure remain unchanged. No performance, Hugging
Face write, publication, archive, deletion, branch, commit, push, or schedule
occurred. The sandbox-only zero-request directory
`20260730T150146Z-e1-collection-v1` is empty and was preserved under the
no-delete instruction.

Focused E1/S1/fail-closed monthly-NAV verification passed all 29 collected
tests. The full suite was not rerun because the change is isolated to the
approval-gated E1 collector and focused verification establishes the new
final boundary without reopening the previously completed 752-test E1 suite.

## Event/M&A Session E1 retrieved-document extraction — completed 2026-07-30

The authorized offline continuation used only
`artifacts/event_review/us/20260730T150604Z-e1-collection-final/`. It made no
external request and did not retry HPK. Before processing, both the established
collection verifier and a separate direct audit rehashed all 59 collection
records, decompressed and rehashed all 47 stored responses, matched all frozen
URLs in order, confirmed 19,120,821 aggregate response bytes and the 46
HTTP 200/one HTTP 503 status boundary, revalidated the frozen parent E1
manifest/request-plan/collection-contract hashes, and matched all 29 D1
preservation entries.

The final non-overwriting extraction version is:

`artifacts/event_review/us/20260730T154650Z-e1-extraction-v2/`

Its manifest SHA-256 is
`e8f0e81f3a051b801720241235b706f31ed0e68ec34063ca4bccf3f958ba264a`.
The artifact contains 62 manifest records and recopies all 47 exact stored
responses. It has:

- 47 document-inventory rows, including the explicit HPK HTTP 503 no-claim row;
- 46 exact quoted primary-document claims, one for each HTTP 200 document;
- for every claim, source ID, request URL, accession/form,
  publication/retrieval times, exact response SHA-256, evidence path,
  reproducible normalized-text character locator, passage SHA-256, and exact
  claim text;
- 47 deterministic reconciliation rows that separately record exact-security
  scope, event type/status, publication time, effective-time status,
  transaction/action terms, and remaining ambiguity;
- an updated 15-name live contract and a 15-row explicit human-review queue;
- a concise cited report and extraction/citation contract.

The primary documents support narrow issuer or subsidiary facts including
asset acquisitions/dispositions, financing agreements and debt offerings,
SM's proposed and completed merger role as surviving/consideration issuer,
Criteo's proposed redomiciliation, common-stock offerings/repurchases,
government-contract modifications, a principal-stockholder governance change,
and an earnings-guidance correction. Those facts were not converted into an
acquisition, merger, bankruptcy, suspension, delisting, or other
selection-changing event for a shortlisted security unless the exact
security/effective-time contract was complete. It was not complete for any
document row. All 47 deterministic actions therefore remain `unresolved`, all
15 names remain in human review, and unsupported-inference flags remain zero.

Thirteen names with complete retrieved-document sets may have summaries only
from claim-level retrieved-evidence citations. HPK remains
summary-prohibited because its Form 25-NSE response is HTTP 503; SSTK remains
summary-prohibited because the frozen plan had no primary-document candidate.
Summaries cannot change deterministic actions.

The focused extraction tests pass 3 tests; the combined E1 collection,
deterministic-policy, and extraction boundary passes 18 tests. Independent
final artifact verification reproduces all 46 locators and hashes. The earlier
`20260730T154528Z-e1-extraction-final` materialization is preserved rather than
overwritten; v2 supersedes it after replacing one truncated NOG passage with a
complete exact sentence. The full suite was not rerun because the new offline
module and workflow are isolated and the task explicitly requested focused
tests.

No dataset promotion, P3/P4 consumption change, performance calculation,
external request, collection retry, Hugging Face write, archive, deletion,
branch, commit, push, publication, or schedule occurred. P2-P4, D1, S1,
original E1 and collection artifacts, private revision
`aaf056ea115067e42ef9abf9fa93ade75cdd4052`, legacy workflows, and
international/per-market structure remain unchanged.

## Event/M&A Session E1 citation-by-citation adjudication — completed 2026-07-30

The authorized offline continuation used only
`artifacts/event_review/us/20260730T154650Z-e1-extraction-v2/`. Before
adjudication, its manifest independently matched SHA-256
`e8f0e81f3a051b801720241235b706f31ed0e68ec34063ca4bccf3f958ba264a`.
The direct audit rehashed all 62 extraction records, decompressed and rehashed
all 47 stored responses and 19,120,821 aggregate bytes, reproduced all 46
normalized-text locators and passage hashes, and reverified all 59 collection
records, all 148 parent E1 records, and all 29 D1 preservation entries. It
confirmed exactly 46 HTTP 200 responses and the one preserved HPK HTTP 503.
No external request or HPK retry occurred.

The final non-overwriting adjudication version is:

`artifacts/event_review/us/20260730T173110Z-e1-adjudication-v2/`

Its manifest SHA-256 is
`dcbf95776b50139797410674d7f0ca410cfdaa1401917dea4fb47f7b00e9c7c6`.
The manifest independently verifies 70 records and recopies all 47 exact
responses. The artifact contains:

- a 15-row name-level adjudication table with affected security/role, event
  type/status, SEC-acceptance publication lineage, document-stated dates,
  announcement/effective-time precision, terms completeness, conflicts,
  remaining ambiguity, citations, and deterministic status;
- a 47-row document-level adjudication table, including 46 exact cited claims
  and the explicit HPK HTTP 503 no-claim row;
- a resolved-versus-unresolved deterministic reconciliation with zero resolved,
  exclude, warn, or no-action rows at either document or name level;
- an updated 15-name live contract and a 15-row unsigned explicit-human-
  signoff queue;
- a concise claim-cited report, a frozen adjudication contract, and a
  non-mutating sign-off contract requiring any signed result to be a separate
  timestamped derivative.

The frozen deterministic rules were applied before narrative. No document
supplied all of exact shortlisted-security scope, a recognized rule type and
status for that security, complete terms, and an exact effective timestamp.
All 47 documents and all 15 names therefore remain `unresolved`; all 15 require
explicit human sign-off. Date-only statements remain date-only and were not
converted to timestamps. SEC acceptance time is retained only as publication
lineage.

The adjudication narrowed every row to its exact passage. Asset
acquisitions/dispositions, financing, debt offerings, contract modifications,
governance, common-stock offerings, and proposed redomiciliation were not
converted into selection-changing events. The SM completion passage states
that Civitas common stock was converted or cancelled and that SM common stock
was consideration; it does not state that SM common stock ceased to be
independently eligible. HPK remains unresolved and summary-prohibited because
the Form 25-NSE response is HTTP 503. SSTK remains summary-prohibited because
there is no retrieved primary-document candidate. The other 13 summaries cite
only exact claims and cannot change deterministic action.

Focused adjudication verification passed 3 tests. The combined deterministic
E1, extraction, and adjudication boundary passed 21 tests in 66.28 seconds.
Independent final artifact verification matched all 70 records, 47 raw
responses, 46 citations, 47 document rows, 15 name rows, and 15 sign-off queue
rows. The earlier `20260730T172953Z-e1-adjudication-v1` materialization is
preserved rather than overwritten; v2 supersedes it by adding complete
name-level aggregates for every required adjudication field. The full suite
was not rerun because the new offline module is isolated and the task
explicitly requested focused tests.

No dataset promotion, P3/P4 consumption change, performance calculation,
external request, collection retry, Hugging Face write, archive, deletion,
branch, commit, push, publication, or schedule occurred. P2-P4, D1, S1,
original E1, collection, and extraction artifacts, private revision
`aaf056ea115067e42ef9abf9fa93ade75cdd4052`, legacy workflows, and
international/per-market structure remain unchanged.

## Canonical final-shortlist presentation — completed 2026-07-31

The final presentation route consumes only the exact frozen P4 latest
shortlist and E1 adjudication:

- P4 manifest:
  `28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7`;
- P4 latest shortlist:
  `93bd6104a73e3752da019f7767cc7dfa72bfca837149179dd72b9b87bf466a3b`;
- E1 adjudication manifest:
  `dcbf95776b50139797410674d7f0ca410cfdaa1401917dea4fb47f7b00e9c7c6`.

`portfolio/final_shortlist.py` and
`workflows/build_final_shortlist.py` implement an offline-only,
non-overwriting presentation derivative. Before materialization, the builder
rehashes all 25 P4 records, eight P4 validated inputs, all 70 E1 records, all
29 preservation entries, the frozen notebook, and the exact P4/E1 identity,
rank, stable-row-ID, and timestamp set.

Presentation policy v2 is frozen before narrative:

- E1 `exclude` maps only to `EXCLUDE_EVENT_EVIDENCE`;
- E1 `warn` maps only to `RESEARCH_CANDIDATE_EVENT_WARNING`;
- E1 `unresolved` maps only to
  `RESEARCH_CANDIDATE_EVENT_EVIDENCE_INCOMPLETE`;
- only an E1 `pass` with the complete clean contract may map to
  `RESEARCH_CANDIDATE`;
- narrative, summaries, LLM output, and human sign-off are prohibited
  decision inputs.

The completed version is:

`artifacts/final_shortlist/us/20260731T000054Z-final-shortlist-v2/`

Its manifest SHA-256 is
`04965561433188242307574b7238866aa2a436828b630bce29d64a6544f9ee95`.
It contains 17 files and 749,759 bytes, including 16 rehashable manifest
records:

- 15-row final presentation table, SHA-256
  `be22c8e55bce218b8d6cd7716e21603e6e6fae7800136ebac2a42bf040e90453`;
- 15-row machine-attested E1 derivative, SHA-256
  `79d1ed112e966b97038e8877f743a17a1f307bd8565be2155aec6356092934d3`;
- 47-row traceability table, SHA-256
  `1df2b70b3af98444fb7dc251df62fe7e70c6944065a41a4922c108ab142c1705`,
  with all 46 exact E1 claim citations plus the explicit SSTK no-citation row;
- presentation-policy-v2 contract, SHA-256
  `fbf6216ba371608bcd785c014f62b4f4e742db3f367bfb6a27acc7d1c38f97f3`;
- deterministic-comment contract, SHA-256
  `cd0c9a87e2bb4d426ea886e614c58f02c162ca665cf49c12dd70c61f584f6e05`;
- concise human-readable report, SHA-256
  `d0628ef5c40a26224831272d502f948fb1ada9782ffbc6eadc859e4e2beec544`.

All 15 names remain included in their exact frozen ranks with `holding=true`
and weight `1/15`. Every presentation label is
`RESEARCH_CANDIDATE_EVENT_EVIDENCE_INCOMPLETE`. The original E1 sign-off state
is preserved separately as `unsigned_pending_explicit_human_signoff`;
presentation human sign-off is not required and presentation status is
`not_required`. No signature was invented. The 13 eligible summaries retain
only exact E1 claim citations. HPK remains summary-prohibited because the
Form 25-NSE response is the preserved HTTP 503; SSTK remains
summary-prohibited because no primary-document candidate exists.

Historical and current source-code lineage are both explicit. The frozen P4
record for `portfolio/build_canonical_product.py` remains intact while the
current post-P4 C1 implementation has its separate current hash. That expected
code evolution is recorded as
`expected_post_p4_c1_code_evolution` and is not treated as P4 artifact-record
corruption.

Focused final-shortlist verification passes 5 tests. Independent artifact
verification reproduces every comment from structured fields, every policy
label from E1 action state, all 15 frozen P4 rows, all 46 E1 citations, both
summary prohibitions, the no-signoff presentation contract, the complete
manifest record set, and fail-closed performance.

No generative LLM, Groq, Llama, web search, external request, HPK retry,
dataset promotion, model execution, performance calculation, notebook
mutation, publication, archive, deletion, branch, commit, push, or schedule
occurred.

## B1 existing-local-evidence decision — completed 2026-07-31

The bounded read-only B1 review reverified the four accepted roots:

- P4 manifest
  `28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7`;
- S1 manifest
  `28317e4ae0126367c38bb40c9fc8169394fc1141c475072d0dc484c141479a1e`;
- E1 adjudication manifest
  `dcbf95776b50139797410674d7f0ca410cfdaa1401917dea4fb47f7b00e9c7c6`;
- final-shortlist manifest
  `04965561433188242307574b7238866aa2a436828b630bce29d64a6544f9ee95`.

The result is `unavailable_fail_closed` for current local/free evidence. S1
contains 0 matched, 135 ambiguous, and 49 unsupported requirements across 180
holdings and four benchmark requirements. E1 deterministically upgrades none
of them. No preserved local artifact satisfies the required Federal Reserve
H.15 `DGS1MO` ALFRED vintage with
`realtime_start=realtime_end=2026-07-17`. Frozen Yahoo payloads are
liquidity-only evidence. The existing monthly-NAV module has fail-closed
arithmetic but not the complete independent July, overlapping 36-month,
gross-and-net P4 contract.

This is not a finding that the model has no value or that a future proper
backtest is impossible. It means the next economic decision should be based on
the frozen model's predictive evidence before any paid-data commitment.

## Q1 frozen P3 predictive-quality audit — completed 2026-07-31

The bounded read-only audit independently rehashed the accepted P2, P3, and P4
manifests and every artifact record and validated input. The frozen prediction
and observed-label lineage remains intact. The current working tree differs
from the frozen P3 and P4 code-lineage hashes only for the two builders already
documented as expected post-P4 C1 shared-module extraction; no current builder
or model was executed.

Q1 used 16,597 labeled OOS rows from 2014-2023. LightGBM produced pooled
Spearman IC 0.138, Pearson 0.053, MAE 0.970, and RMSE 3.166. Annual IC was
positive in all ten labeled years, but top-decile target lift was positive in
7/10 and top-15 lift in only 5/10. The tree produced pooled ROC AUC 0.569, PR
AUC 0.439, and Brier 0.249; median annual AUC was 0.556, while calibration was
weak and the historical-prevalence baseline had better pooled Brier 0.241.

The evidence classification is moderate predictive signal. Available P3
prediction coverage is 38,894 per role, but only 42.7 percent have observed
labels overall and 62.4 percent within matured 2014-2023 decisions. The 2024-
2026 decisions have no mature three-year labels. Mature missingness is strongly
conditional on decision-time shares, price, and size support; the direction of
missing-label bias is unknown. Diagnostic target lifts are not portfolio
returns, and no trading performance was calculated.

## Free-data V1 / paid-data V2 decision — 2026-07-31

The user selected the best defensible free-data product for V1 and deferred
paid data to V2. Paid data is no longer a prerequisite for V1 performance.
Instead, V1 will reuse the provider-neutral P4/S1/E1 contracts, preserve all
free price/security/action evidence, account for every holding, and calculate
CAGR, volatility, Sharpe, Sortino, maximum drawdown, Calmar, benchmark-relative
metrics, turnover, and costs under physically separate observed and
predeclared survivorship-scenario namespaces.

V1 may include an observed-available diagnostic, a best-free-evidence full-
accounting scenario, the legacy minus-50-percent sensitivity, and a separately
predeclared conservative terminal-loss stress. No unresolved holding may be
silently dropped, no assumed return may enter observed labels, and every
headline metric must state holding/capital coverage, scenario-imputed exposure,
benchmark coverage, and risk-free-rate treatment. V1 is free-source historical
research, not a survivorship-complete or provider-certified claim. V2A later
assesses paid providers and reruns the unchanged engine to quantify divergence.

## Free-data V1 evidence Session B1C — completed 2026-07-31

B1C implemented the non-overwriting, performance-free evidence assembler in
`backtest/free_data_v1_evidence.py`, its workflow in
`workflows/build_free_data_v1_evidence.py`, and focused coverage in
`tests/backtest/test_free_data_v1_evidence.py`. It does not import or run the
legacy December-calendar NAV path and does not calculate NAV, a return, a
metric, or a backtest.

The accepted artifact is
`artifacts/performance_inputs/free_data_v1/20260731T115106Z-b1c/`. Its manifest
SHA-256 is
`98635ab48c5f381a0145cc6ab99ff76e072bc24f3c2b04a669edb80371ee71df`.
It contains 30 files and 8,680,255 aggregate bytes; all 29 manifest records
rehash exactly. The core output hashes are:

- performance contract
  `3f6b06ba5e39dd180f66562643addb54a34f413f7e1f983af66a006576f1f325`;
- 184-row requirements
  `990b2212e5a173b19eecd17c3acc6c06639d19c66408204242e20b8073cbe251`;
- 184-row security identity
  `3c789adabac0a5c7f0a039e580a9d74c003a763b9a9a839fb421cf9e67cb244f`;
- 184-row security actions
  `c76e1bb24cc66f21ba4028e1e06f0717dc5404207eca9959893c36d1b24c0a7c`;
- 512,413-row validated price table
  `901e7bb98adfce06a591eff649e04587ff123ec7284709702263fd90176bd61b`;
- four-row benchmark requirements
  `a57ca5dae7dbd2616c5b61974b49455777706857200e0ba67fbae097fd3b0261`;
- 184-row coverage
  `46f23ecdcc26ef2806dff2afe2beeeb910b068fe1adb1794ed4c5ff910d8f0eb`;
- coverage summary
  `1a843c31466f418fa701fa394f52deee0cb9ca07f11b6c4528348ba6ce30aee3`;
- rate status
  `a32eb42c70f417f7f17814eea007f5dabb513d2a9270655cf1226b3e4fe6f304`;
- request manifest
  `ff433930728ef20fcf95b6a461bba9dfba3f840878c847e3fc32c2e10c708236`.

The independent reconciliation confirms exactly 180 holdings and four
benchmark masters, 135 matured 2015-2023 holdings, 45 open 2024-2026 holdings,
zero fully matched S1 requirements, 135 S1-ambiguous matured holdings, 45
S1-unsupported open holdings, and four unsupported/incomplete benchmark
masters. E1 still resolves no event. Fourteen current holdings have retrieved
but incomplete primary-document support; SSTK has no retrieved primary-
document claim.

All 132 relevant holding symbols and four benchmark symbols passed exact raw
stored-size/hash, decompressed-size/hash, normalized-size/hash, ticker/provider
mapping, payload symbol/currency/type/exchange/timezone/daily-granularity,
strict timestamp/array alignment, XNYS/XNAS calendar, entry, completed-vintage
exit, final-common month-end, and assigned-benchmark checks. All 180 entries,
all 135 matured exits, and every required month-end are common observations;
benchmark gaps are zero. This is best-free-provider evidence only. Yahoo does
not certify the exact adjusted-close corporate-action semantics, S1 provides
no fully dated security/action match, and E1 provides no deterministic action
upgrade. No ticker substitution, ticker chaining, unsupported forward fill,
observed-namespace recovery, or assumed label/training outcome is allowed.

The four outcome namespaces and two rate namespaces are physically separate.
The exact DGS1MO acquisition path is limited to the official FRED metadata and
observations endpoints at `realtime_start=realtime_end=2026-07-17`, observation
dates 2015-07-01 through 2026-07-02, JSON daily frequency, and linear units.
`FRED_API_KEY` was unconfigured, so no external request was made and exact
unavailable status
`unavailable_not_collected_missing_fred_api_key` is preserved. The zero-risk-
free Sharpe diagnostic remains separate and is not relabelled as DGS1MO.

Focused verification passes 12 tests with no warnings. The single final full-
suite run passes 777 tests with 4 skips and 78 pre-existing pandas warnings
from unrelated Step 5 fragmentation and Step 6 date-coercion tests; B1C adds no
warning to that run.

## Free-data V1 NAV/metric Session B1D — completed 2026-08-01

B1D added the provider-neutral engine in `backtest/free_data_v1_nav.py` and its
deterministic synthetic boundary in
`tests/backtest/test_free_data_v1_nav.py`. It does not import, modify, or run
the legacy December-calendar NAV path and provides no historical-study command.
The engine is 76,524 bytes with SHA-256
`880cf13607851dcc1d0947ef7b62ecc798e1add841a707b85a80535cd74d034f`;
the test boundary is 37,600 bytes with SHA-256
`c9cb5b123b7450498a44938c64ebd166164df8a6216764a110e1a95217dee86f`.

Before any B1C table was loaded, the engine reverified accepted manifest
SHA-256
`98635ab48c5f381a0145cc6ab99ff76e072bc24f3c2b04a669edb80371ee71df`,
all 29 artifact records, all seven copied validated inputs, and all three B1C
code-lineage records. The loaded boundary independently reconciles its
manifest identity and no-performance claim; all requirement, identity,
action, coverage, benchmark, namespace, and rate schemas; exactly 180 holdings
plus four benchmark masters; 135 matured and 45 open holdings; 512,413 price
rows; the 25-bps actual-traded-notional policy; and the explicitly unavailable
DGS1MO state.

The engine implements independent annual July 36-month vintages and a time-
weighted aggregate strategy that treats later vintage contributions and
earlier vintage withdrawals as external flows. It uses every holding's exact
common entry, calendar-designated month-end, and exit session with its assigned
benchmark sleeve. Entry and remaining-position exit costs equal 0.0025 times
absolute actual traded notional; complete cash events create no invented exit
trade; partial exits charge only the remaining notional actually sold. Gross
and net portfolio and benchmark values, returns, and NAV remain separate.

Observed, bounded-scenario, provider-confirmed, and unsupported/unresolved
outcomes are separate frames inside each isolated performance-namespace
result. Ticker changes and stock mergers require exact effective time,
successor identity, exchange ratio, complete terms, source lineage, and a
resolved deterministic action. Cash mergers, bankruptcies, delistings, and
partial cash exits have event-specific complete-term checks. Unsupported
terminal outcomes remain visible and fail observed/best-free calculation
closed; only the frozen minus-50-percent and minus-100-percent contracts may
create a bounded scenario, and only for an explicit unsupported terminal exit.
No missing benchmark session, missing price, unsupported ticker chain, forward
fill, or disappearance inference is repaired or imputed.

The metric layer calculates gross and net CAGR, annualized sample volatility,
maximum drawdown and duration, Sharpe, Sortino, Calmar, beta, annualized alpha,
tracking error, information ratio, turnover without a half multiplier, hit
rate, and their history/trade/rate denominators. Zero or insufficient
denominators produce an explicit unavailable reason and never infinity. The
zero-rate diagnostic is accepted only under its exact diagnostic namespace;
the B1C DGS1MO path raises fail-closed because exact observations are absent.

Focused B1D tests pass 19 tests with no warnings. The combined B1D, B1C, and
legacy NAV boundary passes 41 tests with no warnings. Synthetic coverage
includes overlapping vintages; exact entry/month-end/exit alignment; partial
and complete exits; actual-notional costs; identity and ticker continuity;
cash/stock mergers; bankruptcy,
delisting, and unsupported outcomes; benchmark gaps; gross/net NAV; all four
outcome namespaces; both rate namespaces; all required metric formulas; zero-
denominator and insufficient-history cases; deterministic reruns; and fail-
closed schema/identity/coverage/namespace/event behavior. No persistent B1D
artifact was generated.

The single final full-suite boundary passes 796 tests with 4 skips and the same
78 pre-existing pandas warnings from Step 5 fragmentation and Step 6 date-
coercion tests; B1D adds no warning. `quality/check_sync.py --warn-only` exits
zero and reports `no changed files detected — nothing to check`.

No external data was collected or refreshed, no model or controlled backtest
was executed, no actual historical performance was calculated, no portfolio,
weight, threshold, provider, artifact, or notebook changed, and no archive,
delete, branch, commit, push, publication, or schedule action occurred.

## Free-data V1 controlled backtest Session B1E — completed 2026-08-01

B1E added the controlled historical-study adapter in
`backtest/free_data_v1_performance.py`, its command in
`workflows/run_free_data_v1_performance.py`, and focused artifact/report tests
in `tests/backtest/test_free_data_v1_performance.py`. It uses the unchanged
B1D engine and exact frozen B1C evidence only. No model or legacy performance
route is imported or executed.

Before calculation, the workflow independently matched:

- B1C manifest SHA-256
  `98635ab48c5f381a0145cc6ab99ff76e072bc24f3c2b04a669edb80371ee71df`;
- all 29 B1C artifact records, seven copied validated inputs, and three B1C
  code-lineage records;
- 184 requirements, including 180 holdings and four benchmark masters;
- 135 matured holdings in nine 15-name 2015-2023 vintages and 45 open
  holdings in three 15-name 2024-2026 vintages;
- all 512,413 validated price records and all schemas, identities, coverage,
  namespace, terminal-event, benchmark, and rate contracts;
- unchanged B1D engine SHA-256
  `880cf13607851dcc1d0947ef7b62ecc798e1add841a707b85a80535cd74d034f`;
- unchanged B1D test SHA-256
  `c9cb5b123b7450498a44938c64ebd166164df8a6216764a110e1a95217dee86f`.

The final non-overwriting artifact is:

`artifacts/performance/free_data_v1/20260801T011135Z-b1e/`

Its manifest SHA-256 is
`23588207812e2e950d3e521c6f2048e7607f20b99b691407c449ceb7752bf37c`.
The manifest enumerates and hashes all 85 generated records totaling 2,065,058
bytes and records every input, code, configuration, performance namespace,
outcome namespace, rate namespace, and output hash. Core hashes are:

- configuration
  `9a6efb4157d6229b74f476a39242c4550f2458aea483b64c00dd7aee3d92caa8`;
- preflight
  `99e6d867f152a1d9a8efca1a580e26d710f82f8465b995e1ea075c09d5f36905`;
- 2,080-row machine metric ledger
  `eeb7be1b0f1deeb11f3418206e9c25e12b41aa1626f26fc70298d5a84dd890e3`;
- machine availability ledger
  `51bcad0f6132ea53ab2073984c3d4c551b8352279e1748cb31b286960371fbee`;
- 540-row all-namespace holding coverage ledger
  `0d5ba263800ff41ece8e97cf4e11b3833b1295f4e0cb2661c2fd40d45e02bb56`;
- 45-row open-vintage ledger
  `a66ba3e4f31d3a33a02fbc80303daf845f208458d5c3908a443cd2f15c5e4a5c`;
- coverage-first summary
  `e5d26e4aa5af7e8aae3e30e78c3505056528ab951692c0a146839c0c28c4d701`;
- concise product report
  `44fe384fb8a107915bd884252175d97c083c028d4afb54b50f19e4993ba34635`.

Each performance namespace has a separate directory containing 333
separate-vintage NAV rows, 133 aggregate NAV rows, 5,130 holding-valuation
rows, 540 portfolio/benchmark transaction rows, nine capital-flow rows, 135
coverage rows, separate observed/bounded-scenario/provider-confirmed/
unsupported-unresolved outcome files, separate event and scenario ledgers,
portfolio and benchmark metric JSON, and separate zero-rate and unavailable-
DGS1MO metric tables. Because B1C contains zero explicit unsupported terminal-
exit triggers, all four namespaces have the same NAV and metric bytes; this is
recorded as zero scenario exposure rather than namespace blending.

Coverage leads the report: all 135 matured holdings and all $1,800,000 planned
entry exposure resolve to complete observed provider price paths; required and
observed stock sessions are 5,130/5,130; required and observed assigned-
benchmark sessions are 5,130/5,130; benchmark gaps are zero; explicit terminal
event triggers are 0/135; provider-confirmed, unsupported terminal-outcome, and
scenario-imputed exposure are all zero. The 45 open holdings remain physically
present and outside completed metrics. This price-path accounting does not
remove the evidence limitation: all 135 matured holdings remain S1-ambiguous,
zero have certified dated performance identity/action coverage, all four
benchmark masters remain certification-incomplete, and Yahoo adjusted-close
semantics remain uncertified.

For every performance namespace, aggregate gross CAGR is 18.830% versus
9.567% for its benchmark; aggregate net CAGR is 18.687% versus 9.423%.
Aggregate net annualized volatility is 24.257%, maximum drawdown is -30.318%
with a 20-month maximum duration, tracking error is 11.588%, information ratio
is 0.794, and turnover is 2.000 without a half multiplier. Under the physically
separate `zero_risk_free_sharpe_diagnostic`, aggregate net Sharpe is 0.826,
Sortino is 1.407, and annualized alpha is 7.993%. These zero-rate values are
diagnostics only. The exact `dgs1mo_alfred_2026_07_17` namespace has zero
observations, so all DGS1MO-dependent Sharpe, Sortino, and alpha rows are
machine-readable unavailable values with reason
`exact_dgs1mo_observations_absent`.

Focused B1E tests pass 4 tests, including two complete byte-identical reruns
against the frozen evidence, non-overwrite enforcement, code/input drift
failure, all physical namespace outputs, and report reproduction from the
metric ledger. The combined B1C/B1D/B1E boundary passes 35 tests. The single
final full-suite run passes 800 tests with 4 skips and the same 78 pre-existing
pandas warnings from unrelated Step 5 fragmentation and Step 6 date-coercion
tests; B1E adds no warning. `quality/check_sync.py --warn-only` exits zero with
`no changed files detected — nothing to check`; the final artifact verifier
independently rehashes all 85 records and reproduces the report from the metric
ledger; and `git diff --check` passes.

The report explicitly states that these are free-source historical research
results, not survivorship-complete certification, provider-certified
performance, personalized investment advice, or a future-performance promise.
Old V3 performance claims remain historical and non-transferable. No external
data was collected or refreshed; no model was executed, retrained, tuned, or
replaced; no portfolio, weight, gate, threshold, scenario, provider, canonical
P2-P4 artifact, or Architecture V2 notebook changed; and no provider trial,
purchase, publication, archive, deletion, branch, commit, push, or schedule
action occurred.

## Free-data modeling priority decision — 2026-08-01

The user explicitly deferred V2A and all paid-data exploration. The active
priority is to make the existing frozen free-source modeling route as strong
as possible while preventing look-ahead and adaptive backtest overfitting.
The staged sequence is M1A contract freeze, M1B implementation plus synthetic
leakage testing, M1C one controlled nested walk-forward OOS model run, and M1D
one locked P4/B1E-compatible performance comparison. P3 and B1E remain
untouched baselines. Aggregate net CAGR of 30% and zero-rate diagnostic Sharpe
of 1.0 are final reporting thresholds only; neither may be used to select
features, models, gates, hyperparameters, scenarios, or retries.

## M1A/M1B/M1C/M1D status

M1A is complete in the non-overwriting artifact
`artifacts/modeling/nested_walk_forward/20260801T000000Z-m1a/`, manifest
SHA-256 `a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc`.
The artifact revalidates the frozen P2/P3/P4/B1D/B1E boundaries, preserves the
exact P3 outer folds, and freezes the nested temporal, label-maturity,
three-year-purge, fold-local transformation/selection, feature exclusion,
bounded-grid, predictive-objective, tie-breaker, and gate-regime contracts.
It includes focused deterministic leakage-boundary tests. No model or
performance execution occurred.

M1B is complete in `modeling/nested_walk_forward.py` and
`tests/modeling/test_nested_walk_forward.py`. M1C is complete under
`artifacts/modeling/nested_walk_forward/20260801T121426Z-m1c/`, manifest
SHA-256 `125cb5d6a8012c3b03ee6eab5f00ac944135c39996e992ecf40d8816acdacc58`.
It executed the frozen contract once successfully, retained all 43,806 source
rows for both roles, and calculated predictive metrics only. M1D is complete
under `artifacts/performance/m1d/20260801T162953Z-m1d/`, with lock manifest
SHA-256 `757e19cd9e35290a6b339f79e2c44a0f1ddb47c03b913930b1abda84f0bf74bc`
and final manifest SHA-256
`b04cea8236da6cd92749410f6186360be5f29dbbbeb00a64f2ed07c180cc72ab`.
It froze the composite route from inner evidence and evaluated it exactly once
through the unchanged P4/B1D/B1E boundary. Preserve M1A, M1C, M1D, and the
P3/P4/B1D/B1E baselines; do not retry or retune from the result.

## Free-data modeling Session M1A — completed 2026-08-01

M1A froze the nested walk-forward experiment contract without historical model
execution. The non-overwriting artifact is
`artifacts/modeling/nested_walk_forward/20260801T000000Z-m1a/`, manifest
SHA-256 `a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc`.
It contains `experiment_contract.json`, `outer_folds.json`,
`inner_folds.json`, `label_maturity_ledger.json`,
`evidence_revalidation.json`, a methodology report, and hashes for every
generated record. The artifact records 34 exact P3 role-specific outer folds,
306 inner role/regime/fold rows, and 102 outer role/regime maturity rows.

The evidence pass matched the frozen P2, P3, P4, and B1E manifests; the P3 OOS
prediction and lineage records; the P4 prediction-lineage records; the B1E
configuration/preflight records; the frozen B1D engine/test hashes; and the
current modeling code hashes. P3's exact 2010-2026 annual outer boundary is
preserved. Inner folds are the latest three mature pre-outer annual cohorts,
expanding in time, with 100 minimum training rows, 50 minimum validation rows,
strict label-end ordering, explicit three-year horizon purges, and at least
two valid folds required for tuning. Early or under-mature folds fail closed
with an exact reason.

The contract freezes the 200-column P3 candidate universe, target/support/
identity/timestamp/future/model-output/policy/macro/gate exclusions, the
existing selector plus one deterministic stability/redundancy variant, only
LightGBM and the interpretable tree, 8/4-point seeded grids, predictive inner
objectives, simplicity tie-breakers, and the three gate-training regimes.
B1E values and all outer-OOS/portfolio metrics are prohibited selection inputs;
the CAGR and zero-rate Sharpe thresholds remain final reporting thresholds
only. Focused M1A contract tests pass 7 tests, including byte-identical
artifact reruns.

No source or market data was collected, no model was fit/tuned/scored, no
portfolio or performance was calculated, and no frozen P2-P4/B1D/B1E artifact
was altered. M1B, documented next, implemented this contract without
historical execution.

## Free-data modeling Session M1B — completed 2026-08-01

M1B added the neutral implementation in `modeling/nested_walk_forward.py`
without modifying the existing P3 builder/helpers or any frozen artifact. The
module has no command-line entrypoint and performs no work at import time. It:

- independently verifies the supplied M1A manifest SHA-256 and every generated
  M1A record before exposing its exact 34 outer, 306 inner, and 102 maturity
  rows;
- verifies the 200-column P3 candidate order and exact P3 outer-decision/role,
  inner-fold/role/regime, and maturity boundaries;
- materializes expanding inner populations with strict training-label maturity,
  explicit three-year purging, mature validation labels, row-identity
  separation, and frozen minimum row counts;
- implements all three frozen training regimes, the existing P3 fold-local IC
  selector, and the five-prefix bounded stability/missingness/direction/
  redundancy selector;
- fits median imputation only on the selected inner-training population and
  explicitly records that winsorization/scaling are not applied and encoding is
  unnecessary under the frozen numeric P3 contract;
- expands only the frozen seeded 8-point LightGBM and 4-point interpretable-
  tree grids, evaluates only inner-validation predictive objectives, and uses
  the frozen dispersion, feature-count, complexity, and canonical-ID
  tie-breakers;
- records fold, population, purge, target, feature, diagnostic,
  transformation, parameter, metric-scope, and winner lineage; and
- exposes an explicit `frozen_m1c` outer-refit hook that requires an inner-
  selected lock, exact P3 outer fold, target-free score input, and no adaptive
  retry state. It was not invoked in M1B.

The synthetic test module is 21 tests. It deliberately injects label overlap,
validation data into selector and imputer fitting, whole-population selection,
outer-OOS rows, duplicate validation identities, target/support/future/model/
policy/fraud/gate-output features, B1E/P4/outer-performance inputs, and a second
tuning attempt after a failed reporting threshold. Every attempt raises a
contract error. It also exercises expanding purged folds, the bounded stability
selector, all three regimes, exact grids/seeds, a synthetic tree candidate, full
lineage, and simplicity tie-breaking.

Verification:

- M1B synthetic boundary: 21 passed, 13 existing matplotlib/pyparsing warnings;
- broader M1A, fold-lineage, P3 shared modeling, canonical P3 builder, and
  prediction-consumption boundary: 67 passed with the same 13 warnings;
- M1B implementation SHA-256
  `162d05787d4e8661d6e6477a60630cdd2d9634b6ea20a930a05570b24d497c83`;
- M1B test SHA-256
  `9fa1b7f17f911c474041405a8e50663b8bf4b8cf1a77a72931baec93c64db216`;
- P2, P3, P4, B1D engine/tests, B1E, and M1A hashes all reverified unchanged.

No historical model was fit, tuned, selected, or scored; no portfolio or
performance calculation ran; no data was collected/refreshed; no artifact was
created or overwritten; and no branch, commit, push, publication, archive,
deletion, or schedule action occurred. At the M1B boundary, the exact next task
was M1C: one controlled nested walk-forward OOS model run through the frozen
M1A/M1B interfaces. M1D was not part of that task.

## Free-data modeling Session M1C — completed 2026-08-01

M1C completed one successful controlled nested temporal walk-forward execution
through the exact M1A/M1B interfaces. The accepted non-overwriting artifact is:

`artifacts/modeling/nested_walk_forward/20260801T121426Z-m1c/`

Its manifest SHA-256 is
`125cb5d6a8012c3b03ee6eab5f00ac944135c39996e992ecf40d8816acdacc58`.
The artifact has 27 files and approximately 40 MiB, including 26 manifest-
verified generated records. Before the first estimator fit, `preflight.json`
persisted and verified:

- all 43,806 P2 source rows and 87,612 required row-role identities;
- all 17 outer decisions, 34 frozen outer-role records, 102 outer training
  populations, 306 frozen inner records, 138 available inner populations, 168
  explicit unavailable inner records, and 102 maturity records;
- all 72 canonical role/regime/selector/grid candidates and 1,224 outer-fold
  candidate contexts;
- the exact 200-column P3 candidate order, per-regime feature lists, both
  targets, three regimes, two selectors, 8/4-point grids, seed 42, frozen
  environment, preserved hashes, and every prohibited selection/score input;
- no prior persisted outer prediction, metric, winner, or completed manifest.

Execution-time reuse was mechanical only. Of 138 available inner-population
occurrences, exact training/validation SHA-256 fingerprints identify 52 unique
population computations. M1C caches only fold-local selector results, median
preprocessing, and transformed matrices across exact matches. It rebuilds
fold-specific lineage for every consumer and never caches an estimator,
prediction, objective, metric, or winner. This leaves all candidates, grids,
seeds, folds, targets, regimes, selectors, objectives, and tie-breakers
unchanged.

The artifact materializes:

- 87,612 unique row-complete source-row/model-role records;
- 20,142 matured metric-eligible outer-OOS predictions, exactly 10,071 per
  role, over the five fitted 2019-2023 decision cohorts;
- 24,566 frozen-rule 2024-2026 open production scores outside predictive
  metrics;
- 11,742 scored historical rows with unavailable labels outside metrics;
- 516 explicit 2027-2028 future-decision exclusions and 67,470 explicit
  prediction/metric-exclusion records overall;
- 624 candidate evaluations, 104 normalized selector/preprocessor records
  with full diagnostics and transformed-value hashes, 16 inner-locked winner
  decisions, and 16 model files with full population/target/feature/
  preprocessing/parameter/winner lineage and model SHA-256 values;
- fold and pooled predictive-only metrics, a methodology report, execution
  summary, and manifest.

Pooled 2019-2023 LightGBM Spearman IC is 0.3347, with positive IC in each of
the five fitted matured folds. Pooled tree ROC AUC is 0.6538, PR AUC 0.4684,
and Brier score 0.2334; its calibration gap is 0.1559. These are predictive
metrics only, not portfolio returns or performance.

The independent disk-only verifier matched the manifest lock, all 26 generated
records, every one of 87,612 row-role identities, the complete P2 identity set
for each role, all exclusions and open-row metric boundaries, 624 candidate
evaluations, 88 available diagnostic hashes, all 16 model hashes/lineages, and
all 34/306 frozen M1A outer/inner records. The final focused M1C/M1B/M1A/P3
boundary passes 73 tests with 13 existing matplotlib/pyparsing warnings.
`quality/check_sync.py --warn-only` exits zero, and `git diff --check` passes.
The full suite was not rerun for this bounded M1C execution.

Five earlier non-overwriting M1C directories remain preserved and are
classified in the accepted preflight/manifest. Two contain passed preflight
only, two contain passed preflight plus incomplete model suffixes, and
`20260801T120027Z-m1c` contains passed preflight plus all 16 models but failed
before any outer result was persisted because the mixed tree/regression target
column lacked a normalized Parquet dtype. The correction was mechanical and
directly tested; the accepted retry used no observed outer value and changed no
selection input or rule. None of the five partial directories contains an
outer prediction, metric, winner table, or manifest, and none was deleted or
overwritten.

P2, P3, P4, the B1D engine/tests, B1E, and M1A were rehashed after successful
verification and remain byte-identical. No external data was collected, no
P3/P4/B1D/B1E/M1A artifact changed, and no portfolio, NAV, performance, CAGR,
Sharpe, drawdown, turnover, scenario, risk-free, M1D, publication, archive,
deletion, branch, commit, push, or schedule action occurred. Current code
hashes are:

- `modeling/nested_walk_forward.py`:
  `481d646ae7efac8798c8924599d8cbc3cc8d1bfed5f74d93e9bec534b4a23e96`;
- `modeling/run_nested_walk_forward.py`:
  `739829bea2d4152cd57211b00dd94a5d81cc6781912dec05b99dd6bf0a035905`;
- `tests/modeling/test_nested_walk_forward.py`:
  `0644591749eb83b0bf3080413ccd447aac696fe62d14987ae7c8e187468593ec`;
- `tests/modeling/test_run_nested_walk_forward.py`:
  `7123f160c4116e30eeab65d62448f52cf0e60e736fad96e6fe95846614d2108b`.

The exact next task at that boundary was M1D: freeze this M1C route before
performance, then run one locked P4/B1E-compatible portfolio-performance
comparison with no adaptive retuning. A reporting-threshold miss was final,
not permission to retry.

## Free-data modeling Session M1D — completed 2026-08-01

M1D completed exactly one locked P4/B1E-compatible portfolio-performance
comparison. The new non-overwriting artifact is:

`artifacts/performance/m1d/20260801T162953Z-m1d/`

Its pre-performance lock manifest SHA-256 is
`757e19cd9e35290a6b339f79e2c44a0f1ddb47c03b913930b1abda84f0bf74bc`.
The final manifest SHA-256 is
`b04cea8236da6cd92749410f6186360be5f29dbbbeb00a64f2ed07c180cc72ab`.
Before any portfolio or performance value was calculated, the persisted lock
independently verified every accepted M1C record and model lineage, the exact
M1C manifest supplied by the user, and the frozen P2/P3/P4/B1D/B1E/M1A
boundaries. It also persisted the complete 135-test preflight result, the
unchanged contracts, namespaces, rates evidence, inputs, hashes, prohibited
inputs, and one composite route containing the 16 M1A/M1C winners selected
only from predeclared inner-fold evidence and tie-breakers. No M1C outer
predictive metric or B1E performance value was used to select or alter that
route.

Exactly one execution then consumed the frozen route through the unchanged P4
eligibility, weighting, 15-name equal-weight portfolio, rebalance, benchmark,
and 25-bps-per-side actual-traded-notional cost rules, and the unchanged B1D/
B1E return, turnover, NAV, drawdown, coverage, namespace, scenario, and
risk-free contracts. There was no failed M1D attempt, retry, retuning,
configuration switch, or post-result assumption change.

The 132 manifest-verified generated records total 48,046,602 bytes and
materialize complete route and portfolio lineage: all 87,612 M1C row-role
predictions, all 43,806 source candidates, 525,672 individual gate decisions,
175,784 exclusions, 1,477 liquidity-required rows, 1,216 liquidity passes,
eight supported 2019-2026 decision periods, and exactly 15 holdings per period.
The position, weight, eligibility, inclusion, exclusion, rebalance, return,
transaction-cost, turnover, capital-flow, benchmark, coverage, and gross/net
NAV ledgers are row-complete. All four B1D/B1E outcome namespaces and all four
physical performance namespaces are separate. They have equal results because
there are zero explicit unsupported terminal-exit triggers and zero scenario-
imputed capital; they were not blended.

The primary `best_free_evidence_full_accounting` namespace records:

- aggregate gross CAGR 19.9030% and aggregate net CAGR 19.7471%;
- zero-rate net diagnostic Sharpe 0.854230 and net annualized volatility
  24.5049%;
- maximum drawdown -29.4611%, with an 18-month duration;
- turnover 2.0 under the frozen absolute-traded-notional definition, with no
  half-turnover multiplier;
- ending aggregate gross/net NAV of 3.570585/3.538151, versus benchmark
  gross/net NAV of 2.025139/2.004678;
- net benchmark CAGR 10.4274%; and
- $2,795,383.82 of portfolio actual traded notional and $6,988.46 of costs,
  versus $2,307,516.71 and $5,768.79 for the benchmark.

The 30% aggregate net-CAGR threshold is **not met**. The 1.0 zero-rate
diagnostic-Sharpe threshold is **not met**. Both misses are final M1 results
and did not authorize another run. All five completed vintages have positive
net CAGR and positive zero-rate Sharpe; vintage net CAGR has 21.3661% median,
12.2397% minimum, 31.8213% maximum, and 8.6063% sample standard deviation.
Vintage Sharpe has 0.953644 median, 0.574266 minimum, and 0.988844 maximum.

Coverage is complete only within the preserved free-source paths: all 2,850
required matured-holding stock sessions and all 2,850 assigned-benchmark
sessions are present with zero gaps. Of 120 total holdings, exactly 75 from
2019-2023 are mature and included in completed historical metrics. The 45 open
2024-2026 holdings remain physically present but outside those metrics; none
was silently converted into a completed outcome. Twenty-five holdings remain
S1-ambiguous and 95 S1-unsupported, with zero certified identities. Adjusted-
close semantics remain uncertified.

The exact DGS1MO physical namespace remains unavailable because the preserved
B1C evidence status is `unavailable_not_collected_missing_fred_api_key`; the
exact-rate metric reason is `exact_dgs1mo_observations_absent`. The reported
Sharpe is therefore explicitly a separate zero-rate diagnostic, not an exact
DGS1MO Sharpe. These are free-source historical research results, not
survivorship-complete or provider-certified performance, personalized advice,
or a future-performance promise.

The frozen B1E aggregate net CAGR is 18.6874% and its zero-rate diagnostic
Sharpe is 0.826193. That aggregate comparison is not like-for-like because
B1E begins in 2015 while M1D begins in 2019; M1D separately materializes the
common 2019-2023 vintage comparison. Mean holding overlap across 2019-2026 is
0.266667, so the routes are materially different.

After execution, an independent disk-only pass verified the final manifest,
all 132 records, every inventory entry and key row count, and rehashed all
preserved boundaries. Their manifest/hash values remain P2
`40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3`, P3
`8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2`, P4
`28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7`, B1D
engine `880cf13607851dcc1d0947ef7b62ecc798e1add841a707b85a80535cd74d034f`,
B1D tests `c9cb5b123b7450498a44938c64ebd166164df8a6216764a110e1a95217dee86f`, B1E
`23588207812e2e950d3e521c6f2048e7607f20b99b691407c449ceb7752bf37c`, M1A
`a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc`, and
M1C `125cb5d6a8012c3b03ee6eab5f00ac944135c39996e992ecf40d8816acdacc58`.
The final focused M1D/M1C/M1B/M1A/P3/P4/B1D/B1E/monthly-NAV boundary passes
135 tests with 13 existing matplotlib/pyparsing warnings. The full suite was
not rerun for this bounded M1D execution.

All five earlier partial M1C attempt directories remain unchanged and none
contains a manifest or outer-result record. No failed M1D attempt exists. No
external data was collected, no preserved artifact was changed or deleted,
and no I1, A1, REL1, deferred V2A, publication, archive, branch, commit, push,
or schedule work occurred. M1 is complete; no next session is authorized by
the M1D result.

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
their accepted frozen manifests. A1 later packaged and retired only four
confirmed superseded partial/Session-9 payload groups plus their 11 associated
tracked historical files; exact manifests and archive pointers remain at the
original artifact roots, and tracked recovery is byte-identical from the
archive branch. No canonical, corrected-source, legacy dataset, historical
model, prediction, backtest, portfolio, international entrypoint, or retained
V3 boundary was overwritten. The V3 program remains preserved and paused.
D1, S1, E1, the E1 live collection,
retrieved-document extraction, citation adjudication, and the final-shortlist
presentation, B1C free-data evidence assembly, and B1D synthetic-only NAV and
metric implementation, plus B1E controlled historical performance/report add
uncommitted code, tests, documentation, and ignored non-promoted version
artifacts. M1C adds the neutral nested runner, exact-population cache reuse,
focused tests, the accepted ignored non-promoted M1C artifact, and five
preserved partial attempt directories. M1D adds its fail-closed adapter,
runner, focused tests, and the sole accepted ignored non-promoted M1D artifact;
there is no failed M1D attempt. Every P2/P3/P4/B1D/B1E/M1A/M1C frozen hash
remains unchanged. M1, I1, US1C, and A1 are complete; REL1 and deferred V2A
have not begun. US1C adds one frozen consolidation contract, one local command,
focused tests, exact read-only recovery evidence, and one ignored non-promoted
release-candidate artifact. The unrelated untracked `.jupyter_ystore.db`
remains untouched.

## International restoration Session I1 — completed 2026-08-01

I1 selected exactly one first international adapter: Canada (`CA`, TSX/TSXV).
The selection was frozen before observing any model, shortlist, portfolio, or
performance result using predeclared local evidence only. The fresh artifact
is `artifacts/international/i1/20260801T180000Z-i1-ca/`, manifest SHA-256
`d3f17854cf0839713163e8dda99aefedd6ba064a14a042a886770818a472d9f6`.

The persisted preflight records the selection matrix, frozen contract, local
source lineage, preserved boundaries, and five partial M1C attempt inventory.
`pipeline/international_market_adapter.py` normalizes the preserved CA
snapshot source into 9,207 row-complete core P2 records with stable IDs, CAD
native currency, IFRS accounting, filing-date availability, and explicit
local-benchmark/target/purge/liquidity/cost/portfolio semantics. Focused
adapter tests pass 4/4. The artifact manifest independently verifies five
records including preflight, evidence, contract, compatibility summary, and
P2 compatibility Parquet.

Compatibility is intentionally fail-closed: certified filing publication
times, TSX/TSXV historical calendars, benchmark total-return vintage,
corporate actions/delistings, dated FX, survivorship-complete targets, P3/P4
validation, model fitting, portfolio construction, and performance are not
supported by preserved local evidence. No external data was collected. The
US P2/P3/P4 route, B1D/B1E, M1A/M1C/M1D, and all five partial M1C attempts were
rehashed and remain unchanged. No legacy market structure was deleted or
overwritten.

At the I1 boundary the instruction was to stop after the first-market adapter.
That boundary is superseded only by the separately authorized US1A session
documented below; no second market was started.

## US free-data product Session US1A — completed 2026-08-01

US1A froze the US data baseline before generating any new score, shortlist, or
portfolio output. The read-only D1 review rehashed its universe, filings,
market, P2-candidate, comparison, and 21,384 raw-inventory records. D1 passes
exact 339-column schema/dtype parity, unique internal stable identities,
complete event-time/decision timestamps, all five target horizons, explicit
supported/unavailable/excluded partitions, the frozen 200-feature schema, and
deterministic record reconstruction. It does not pass the exact frozen-route
boundary: 43,564 stable IDs are shared with accepted P2, 242 are P2-only, and
76 are D1-only. Frozen M1C matches all 43,806 accepted-P2 IDs and pins P2
manifest SHA-256
`40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3`.
Consuming D1 would require retraining or a contract change, so D1 is recorded
as `unsupported_for_US1A_frozen_M1_route` and accepted P2 remains selected.

The final non-overwriting artifact is:

`artifacts/product/us_free_v1/20260801T183000Z-us1a/`

Its manifest SHA-256 is
`f27773c0e1fb92a25707e0fec363e13afa7ccb10f8ace7537187c6be58575edf`.
The contract and preservation preflight were persisted before downstream
materialization. The route consumes only the 16 accepted M1A/M1C winners
selected from predeclared inner-fold evidence and applies unchanged P4 hard
gates, the 0.55 tree threshold, exact 30-session candidate-wide liquidity,
descending LightGBM rank, stable-row-ID tie-breaker, 15-name size, and 1/15
weights. No M1D or B1E performance value was a selection input.

The artifact contains complete copies of the P2, M1A, M1C, P4, D1, and E1
manifests plus frozen/current source and code lineage. Its row-complete outputs
contain:

- 87,612 unique M1C source-row/model-role score records;
- 43,806 unique candidate rows;
- 525,672 gate rows and 175,784 explicit exclusion rows;
- 1,477 liquidity-required candidates, 1,216 passes, and 44,310 exact
  candidate-session evidence rows;
- 120 equal-weight holdings over eight supported 2019-2026 decision periods;
- one exact 15-name 2026 shortlist and 30-row score-to-report trace table.

The 2026 shortlist is `CYH`, `RBBN`, `DSX`, `ACCO`, `BDTX`, `HPK`, `AMPY`,
`CRCT`, `ARDT`, `BLMN`, `MLCO`, `SSTK`, `HLLY`, `LFVN`, and `CRTO`, in frozen
rank order. Exact stable-row, ticker, SEC CIK, and decision-time matching maps
existing E1 adjudication evidence to `HPK`, `AMPY`, `ARDT`, `SSTK`, and
`CRTO`; all five remain deterministically unresolved. The other ten names
carry the explicit status `event_evidence_not_collected`. No event was
inferred and no external request was made.

Independent verification rehashed every generated record, reproduced the
report from structured outputs, reconfirmed all row/count/weight/event
contracts, and rehashed P2/P3/P4, B1D/B1E, M1A/M1C/M1D, I1, the M1D lock, and
all five partial M1C attempts. Focused US1A verification passes 6 tests with 13
existing matplotlib/pyparsing warnings. The combined requested product,
prediction-consumption, P4 gate/liquidity, shortlist, and lineage boundary
passes 89 tests. The definitive full suite passes 853 tests with 4 skips and
the same 78 existing Step 5/Step 6 pandas warnings. No US1A test adds a warning.

Verify the artifact offline with:

```bash
python3 -m portfolio.build_us_free_product \
  --artifact-root artifacts/product/us_free_v1/20260801T183000Z-us1a \
  --verify-only \
  --expected-manifest-sha256 f27773c0e1fb92a25707e0fec363e13afa7ccb10f8ace7537187c6be58575edf
```

No model was fit, tuned, retried, or replaced. No CAGR, Sharpe, NAV, drawdown,
turnover, scenario, or other historical performance was calculated or rerun.
No canonical, D1, E1, M1, I1, B1, partial-attempt, or legacy artifact was
overwritten. No external data, promotion, publication, archive, deletion,
branch, commit, push, tag, or schedule action occurred.

At the US1A boundary, the only eligible next task was US1B evidence for the
exact frozen shortlist, contingent on separate request-plan approval. That
approval was later supplied and the bounded result is recorded below; none of
the US1C, second-market, A1, REL1, V2A, paid-data, performance, publication,
archive, commit, or push prohibitions were broadened.

## US frozen-shortlist evidence Session US1B — completed 2026-08-01

The exact evidence contract and 36-URL plan were frozen before the first
request and approved only by token
`APPROVE-US1B-f1bcfc5d2c740d1626c3d6c11148ee1711ec5d70911165b897daf84f90f9bfbc`.
The fresh non-overwriting artifact is:

`artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b/`

Its manifest SHA-256 is
`f802ec26358d27afa21c99490e598e13c98a6ad60229705769f3fae1818d9dcd`.
All 95 records and 2,653,162 recorded bytes verify independently. Collection
made exactly 36 attempts for 36 approved unique URLs, all HTTP 200, totaling
3,681,377 received response-body bytes. It also copied and verified 29 exact
preserved responses without requesting them: 15 D1 submissions, one D1 SEC
CIK/ticker/exchange index, and 13 E1 primary-document responses, including the
unretried HPK HTTP 503.

Structured outputs contain 15 exact market/exchange identity rows, 47
document-level adjudications, 46 accession-bound cited claims with response
and passage hashes plus reproducible locators, 15 name-level adjudications,
and a 15-row evidence coverage table. Document states are 33 unsupported, 13
unresolved, and one failed request. Name states are 14 unresolved and one
failed request (HPK). There are zero covered, conflicting, unsupported, or
`event_evidence_not_collected` name states. All 15 deterministic actions remain
unresolved and all require human review. Nasdaq Trader presence corroborates
identity/exchange only and is not treated as event proof.

The evidence derivative preserves every original US1A shortlist column
exactly, including stable identity, rank, holding, score, gate, liquidity
decision, and 1/15 weight. No model was fit, tuned, rescored, or reranked, and
no performance was calculated or rerun. D1 remains unsupported for frozen
M1C because of 242 P2-only and 76 D1-only stable IDs. The post-collection
verifier rehashed US1A plus all P2/P3/P4/B1D/B1E/M1A/M1C/M1D/I1 and five
partial-M1C boundaries through the US1A boundary audit.

Verify offline with:

```bash
python3 -m portfolio.us1b_frozen_evidence \
  --artifact-root artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b \
  --verify-only \
  --expected-manifest-sha256 f802ec26358d27afa21c99490e598e13c98a6ad60229705769f3fae1818d9dcd
```

The dedicated US1B tests pass 6/6. The combined evidence, extraction,
adjudication, product, shortlist, lineage, non-overwrite, and failure-path
boundary passes 39 tests with 13 existing matplotlib/pyparsing warnings.
The definitive post-US1B full suite passes 859 tests with 4 skips and 78
existing Step 5/Step 6 pandas warnings.

That stop boundary was superseded only by the separately authorized bounded
US1C consolidation below. It did not authorize release, archive, publication,
tagging, commit, push, another market, paid-data work, or performance work.

## Local release consolidation Session US1C — completed 2026-08-01

The contract was frozen before output at
`docs/US1C_RELEASE_CONSOLIDATION_CONTRACT.json`, SHA-256
`d7098153542afa77ff09e850980362eb957b3e24cd609a4984451289595f1d47`.
The single local entry point is:

```bash
python3 -m workflows.run_us_free_v1
```

It fail-closed verifies the exact P2 -> P3 -> P4 -> US1A -> US1B chain,
supporting B1D/B1E/M1A/M1C/M1D/I1 boundaries, the M1D lock, and all five
partial M1C attempts. The read-only evidence pass independently verified US1A
manifest `f27773c0e1fb92a25707e0fec363e13afa7ccb10f8ace7537187c6be58575edf`
with 32 records and 37,863,802 recorded bytes, and US1B manifest
`f802ec26358d27afa21c99490e598e13c98a6ad60229705769f3fae1818d9dcd`
with 95 records and 2,653,162 recorded bytes. The US1B request-plan SHA-256 is
`f1bcfc5d2c740d1626c3d6c11148ee1711ec5d70911165b897daf84f90f9bfbc`:
36 approved URLs, 36 attempts, 36 newly collected HTTP 200 responses,
3,681,377 response-body bytes, and 29 reused D1/E1 responses.

All exact US1A identities, holdings, ranks, scores, gates, liquidity decisions,
and 1/15 weights reconcile unchanged through US1B and US1C. The final evidence
states remain 14 unresolved names and one failed-request name, HPK, with
unresolved deterministic actions and human review for all 15. D1 remains
unsupported for frozen M1C: accepted P2 and D1 share 43,564 stable IDs, with
242 P2-only and 76 D1-only IDs.

Two fresh, non-overwriting candidate assemblies were byte-identical. Recovery
used only read operations against private Hugging Face dataset revision
`aaf056ea115067e42ef9abf9fa93ade75cdd4052`; it verified the exact three
P2/P3/P4 artifacts, 202 files, and 481,666,707 bytes, with private visibility,
no mutable-revision fallback, and no remote mutation. Recovery evidence is at
`/private/tmp/us1c-hf-recovery-evidence-aaf056ea115067e42ef9abf9fa93ade75cdd4052.json`;
the fresh recovered copy remains at
`/private/tmp/us1c-hf-recovery-aaf056ea115067e42ef9abf9fa93ade75cdd4052`.

The fresh local release candidate is
`artifacts/product/us_free_v1_release_candidate/20260801T210000Z-us1c/`.
Its manifest SHA-256 is
`a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466`;
all 24 records and 1,039,444 recorded bytes were independently rehashed and
reconciled to the parents. The complete artifact has 25 files and 1,045,902
bytes including its 6,458-byte manifest. Verify it offline with:

```bash
python3 -m workflows.run_us_free_v1 \
  --us1c-artifact-root artifacts/product/us_free_v1_release_candidate/20260801T210000Z-us1c \
  --expected-us1c-manifest-sha256 a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466
```

The dedicated US1C tests pass 12/12. The focused consolidation,
reconstruction, recovery, immutability, non-overwrite, and failure-path
boundary passes 37 tests with 13 existing matplotlib/pyparsing warnings. The
single final full-suite run passes 871 tests with 4 skips and 78 existing
Step 5/Step 6 pandas warnings.

No data, model, feature, selection, gate, liquidity, portfolio, event, or
performance methodology changed. No evidence was collected, and no model,
score, rank, shortlist, or performance route ran. No artifact or remote state
was overwritten, uploaded, deleted, or repointed. No release, archive,
promotion, publication, branch, commit, push, or tag occurred. Stop after
US1C. A1 was later separately authorized and completed below; REL1 remains
unauthorized.

## Dependency-safe archive Session A1 — completed 2026-08-01

A1 began with a read-only evidence pass. The complete
`python3 -m workflows.run_us_free_v1` route passed, the US1C contract remained
`d7098153542afa77ff09e850980362eb957b3e24cd609a4984451289595f1d47`,
and the local candidate manifest matched
`a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466`.
The pre-action inventory at `docs/A1_REPOSITORY_INVENTORY_BEFORE.json`, SHA-256
`bfc3b754b3076ea348eec79b67b90b393df893117112f36254bf4bfe2f99c6f7`,
hashes all 303 tracked and 53 non-ignored untracked files. It preserves all 8
pre-existing tracked modifications, 52 pre-existing project files, and the
unrelated `.jupyter_ystore.db` exactly as found.

The frozen contract is `docs/A1_ARCHIVE_CONTRACT.json`, SHA-256
`1b8174bac5181b68f6e3913c2e6bf73a7271acce9f1421063e40b39db2a63f3a`.
It records the complete import, workflow, CI, documentation, and artifact
dependency map; exact source paths/manifests/inventories; destinations;
recovery commands; tracked archive-branch hashes; and exclusions before any
payload was moved. The V3.1-V3.3 group failed the retirement gate because
active parity tests still import the historical builders and M1A still pins
`modeling/freeze_session_v3_1.py`; all V3 code and artifacts remain intact.

The local ignored archive root is
`artifacts/archive/a1/20260801T220130Z-a1/`. Its manifest SHA-256 is
`892cf0991b9e1a5a651728a1329bd91422037c62de6d57ee27adcc1e919399da`.
It enumerates ten non-manifest records and four packages:

- corrected partial: 19 files, 565,934,344 source bytes, package SHA-256
  `e07f41abf149b9f4b23c47c21f6fad4f535d73feaf158ac1d2beec7af9a70dd6`;
- corrected partial inputs: 4 files, 1,316,859,187 source bytes, package
  SHA-256
  `0992283d399780624c11a3488d6134f744f823f630a9afd1d0eda7c546530de3`;
- Session 9 corrected OOS: 847 files, 30,992,893 source bytes, package SHA-256
  `1a66ff970d7fb0f1f197ee4a0e7eed1cd6c98d5c5e140d7c6c3724d0132674ce`;
- Session 9B zero-holdings freeze: 17 files, 40,901,417 source bytes, package
  SHA-256
  `8f3ab2976f79f1a05767e524809b43153f30f6cb234ebf1fad1cffd67bc3ae2e`.

All four packages were extracted into absent targets under
`/private/tmp/a1-artifact-recovery/`. Every one of 887 paths and all
1,954,687,841 source bytes matched the frozen per-file inventories and SHA-256
values before retirement. The four original roots now contain only their
byte-identical historical `manifest.json` and an `ARCHIVED_POINTER.json` with
package, inventory, archive-manifest, and recovery lineage.

The following 11 tracked historical paths were retired only after the
contract's `git archive` recovery command reconstructed each byte-identically
under `/private/tmp/a1-tracked-recovery`: the three partial-PIT quality tools,
two partial-PIT reports, two Session 9 builders, two Session 9 tests, and two
Session 9 reports named in `docs/A1_ARCHIVE_CONTRACT.json`. The source is
`codex/legacy-archive` commit
`ed53232cb45cf8b82cfd8941ead5f9a88016e507`; the branch was inspected through
a detached temporary worktree and was not switched or modified.

Canonical P2-P4, corrected Step 2 and Session 8E evidence, D1, S1, E1,
B1C-B1E, M1A-M1D, I1, US1A-US1C, all five partial M1C attempts, immutable
Hugging Face pointers, international/per-market structure, shared
implementations, datasets, models, unresolved historical material, and
`.jupyter_ystore.db` remain unchanged. No evidence was collected; no data,
feature, target, split, model, selection, gate, liquidity, portfolio, event,
or performance methodology changed; no model, score, rank, shortlist, or
performance route ran; and no release, remote mutation, branch switch,
commit, push, tag, promotion, or publication occurred.

The A1 packages are local and Git-ignored. They were not uploaded because A1
explicitly prohibited remote publication; therefore their remote durability
is not claimed. The tracked result pointer is
`docs/A1_ARCHIVE_MANIFEST.json`, SHA-256
`c41b675092c66ddb3609fd16cfd82bd5b9ea1876c3528278cff5e5d789fe177f`.

Post-retirement verification passed: 47 focused dependency, recovery, import,
workflow, and failure-path tests with 13 existing matplotlib/pyparsing
warnings; the complete US1C route with expected candidate manifest SHA-256;
and one completed final full-suite run with 869 passed, 4 skipped, and 78
existing Step 5/Step 6 pandas warnings in 521.01 seconds.

REL1 remains unauthorized and cannot begin until the complete dirty-worktree
diff is accepted, a clean-checkout verification boundary is
defined after an authorized checkpoint, and explicit commit/push/tag/release
authority is supplied. The 15 US1B names also remain unresolved and require
human review; provider-certified/survivorship-complete performance and exact
`DGS1MO` metrics remain unavailable. Stop after A1.

## Session REL1 — clean-checkout verification — completed 2026-08-02

Checkpoint `0b6fb150ede821eacc7b03f9769fd543390b3c8e` was verified in detached
temporary worktree `/private/tmp/rel1-clean-checkout-8irQNW`; the active
worktree was not switched or reset and `.jupyter_ystore.db` remains untouched.
The authoritative route passed after restoring frozen ignored inputs:
`P2 -> P3 -> P4 -> US1A -> US1B`, all supporting boundaries, shortlist,
lineage, and five partial M1C attempts. Two fresh US1C assemblies matched and
the candidate manifest is
`a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466`.

Immutable HF revision `aaf056ea115067e42ef9abf9fa93ade75cdd4052` recovered 202
files and 481,666,707 bytes read-only. A1 contract/archive manifest hashes are
`1b8174bac5181b68f6e3913c2e6bf73a7271acce9f1421063e40b39db2a63f3a` /
`892cf0991b9e1a5a651728a1329bd91422037c62de6d57ee27adcc1e919399da`.
Four package recoveries (887 files, 1,954,687,841 source bytes) and 11/11
tracked archive-branch recoveries passed.

Release decision: blocked. Only P2-P4 have immutable remote recovery. US1A,
US1B, supporting ignored artifacts, US1C, and A1 packages remain local-only;
A1 explicitly claims no remote durability. US1B remains 14 unresolved names
plus failed-request HPK, with human review required for all 15. The S1 ledger
gap and missing immutable 2026-07-17 `DGS1MO` vintage continue to block
provider-certified/survivorship-complete performance. No commit, push, tag,
upload, publication, promotion, or release occurred. The next task requires
explicit authorization for the documentation commit and a durability decision.

The single final clean-worktree full-suite run completed with 828 passed, 5
skipped, 24 failed, 16 errors, and 78 warnings in 119.07 seconds. Failures
were fail-closed missing or drifted ignored Session 8E/B1C, corrected-Step-2,
and E1 lineage inputs; no code, data, model, or methodology change was made.

## Session DUR1 — immutable artifact durability — completed 2026-08-02

Checkpoint/branch/tag are
`f104f0554aa1d4ac916aeef4c4a3b1891eb2fa3d` / `codex/pit-checkpoint` /
`rel1-us-free-v1`. The active worktree was never switched, reset, cleaned, or
overwritten, and `.jupyter_ystore.db` remains untouched. The authoritative
corrected contract is 59,477,180 bytes with SHA-256
`2912feeb29c68bf93c5bffe06ba5f684732ba154118fc3ffc207dc1e9f3c08ae`;
it contains 21 groups, 56,092 files, and 7,082,517,721 bytes. Its deterministic
transport is 24 upload objects and 6,708,421,820 bytes including the contract.

Preflight passed: all 24 corrective destinations absent; repo private; token
write scope verified; exact parent
`33309aeb92a5cca1bb41d5cf76d7330ffc38db0e`; projected logical storage
14,083,077,603 bytes with 85,916,922,397 configured bytes headroom. The single
corrective commit is `a282a1023f321b9bad84ec6f12e5d846345ff833`.
Both the parent and old P2-P4 revision
`aaf056ea115067e42ef9abf9fa93ade75cdd4052` remain unchanged.

Corrective recovery passed read-only for 56,092 files and 7,082,517,721 bytes.
It downloaded 227 objects totaling 7,130,615,722 bytes excluding the contract,
used no mutable fallback, and mutated no remote state. Recovery command:

```bash
python3 -m data_io.us_free_v1_durability --recover --revision a282a1023f321b9bad84ec6f12e5d846345ff833 --target <absent-path> --evidence-output <absent-json>
```

Detached worktree `/private/tmp/dur1-corrective-clean-a282a1023f32` contained
exactly the 56,092 corrected-contract ignored paths and no current-worktree
artifact. The authoritative US1C route passed, followed by 126 focused tests
with 13 existing warnings in 218.79 seconds. The single final full suite was
run exactly once using the same immutable alias bytes before corrective
publication: 876 passed, 4 skipped, and 78 existing warnings in 518.65
seconds. Dependency checking, `quality/check_sync.py --warn-only`, and
`git diff --check` pass.

DUR1 decision: complete. No required artifact remains dependent on local or
mutable state. The initial fail-closed revision remains immutable but is not
authoritative; use only corrective revision `a282a102...`. Do not begin
RATE1/QPOL1, collect `DGS1MO`, revise event policy, start V1.1, or rerun
performance. US1B remains 14 unresolved names plus failed-request HPK, with
human review for all 15.
