# Codex Handoff

## Current Objective

The post-Session-9B production `ml_gates` dependency contract is corrected and
frozen. The original Session 9B unavailable verdict remains valid and its
artifacts remain immutable. The required order is now Session 9B.1 selection
lineage/hard-gate inputs, Session 9B.2 exact fold-local tree agreement, Session
9B.3 candidate-wide preselection ADTV evidence, Session 9B-R final selection,
and only then Session 9C post-selection market evidence. No model fit/rescore,
market-outcome recomputation, external data, holdings, backtest, Session 9C,
commit, or push occurred.

## Confirmed Facts

- Independent revalidation reconfirmed the Session 9B manifest SHA-256
  `2b17030e...`; all 1 validated input, 16 records, and 2 code-lineage records
  matched their stored sizes and hashes. Each physical namespace still has
  219,030 candidate/path rows, zero non-null entry timestamps, and zero
  holdings. The frozen unavailable verdict is unchanged.
- Session 8E already contains 103,285/103,285 non-null certified entry
  timestamps. Every one of the 24,127 supported entity/year identities has
  exactly one entry timestamp across horizons, and each entry follows its
  prediction timestamp. The downstream null is a lineage-preservation defect;
  no market outcome needs to be recomputed.
- The final Session 8F corrected outputs directly support US market,
  `decision_market_cap`, `piotroski_f_score`, `piotroski_roa_pos`, and
  `momentum_12m_prior` where their certified availability gates pass. They do
  not contain `beneish_m_score`, `altman_z_score`, or
  `ps_ratio_sector_pct`; those accepted hard gates remain explicit blockers
  until deterministically materialized from certified corrected inputs under a
  frozen formula and missingness contract.
- The exact successor tree role is frozen before any fit: fold-local 3y
  `DecisionTreeClassifier`, binary `outperformed_benchmark`, up to 35
  fold-local absolute-Spearman-ICIR features selected against certified
  `stock_return`, training-fold median imputation, no scaling/calibration,
  `max_depth=4`, `min_samples_leaf=30`, `random_state=42`, and fully specified
  sklearn defaults recorded in the roadmap. It requires five historical
  cohorts, five features, and strict label availability.
- Session 9's 3y regression model is Ridge while the accepted production
  configuration names LightGBM. Treat model-family compatibility as unresolved;
  do not label the Ridge score as the production ranker without explicit
  contract reconciliation.
- The corrected production ADTV contract freezes AUM `$200,000`,
  `target_n=15`, equal-weight position `$13,333.333333...`, a 1% maximum
  position/ADTV ratio, and minimum median ADTV
  `$1,333,333.333333...`. ADTV is the median of exactly 30 valid regular-session
  unadjusted-close-times-volume observations strictly before the prediction
  timestamp, evaluated candidate-wide before ranking. All missing or ambiguous
  evidence fails closed. The legacy `AUM * 0.01` expression is rejected.

- Session 9B first revalidated the exact Session 9 manifest SHA-256 plus all 7
  validated inputs, 846 artifact records, four code-lineage records, and three
  hash-covered dirty-state references with zero failures.
- Production `ml_gates` remains unavailable because Session 9 supplies a
  calibrated logistic classifier rather than the required OOS tree-agreement
  probability. The logistic score was not substituted and the accepted 0.55
  tree threshold was not applied to it.
- Engine composite, QEM, and SCDV retain compatible classifier score families
  but lack complete frozen non-model ranking/gate fields and canonical ADTV
  eligibility. IARB additionally conflicts with CORRECTED_8F's certified US-only
  population. No reduced-signal or no-liquidity variant was invented.
- Each physical namespace has a separate 219,030-row candidate/exclusion table
  covering all 43,806 stable rows across five strategy paths. Source fold,
  label, feature, preprocessing, model, calibration, prediction, and rank
  lineage is retained for each declared score role. The identity hashes match
  because Session 8E added no policy-only rows; the files were not combined.
- Both holding/weight tables are present and empty. All five paths are frozen
  unavailable, no `target_n=15` portfolio was shrunk or renormalized, and
  Session 9C is not unblocked.
- The accepted entry timestamp is absent from the Session 9 prediction tables.
  Session 9B retains an explicit null entry-timestamp field and unavailable
  status on every candidate row instead of inferring it from another artifact.
- The Session 9B manifest SHA-256 is `2b17030e...`; it records the accepted
  contract/configuration sources, exact Session 9 input manifest, code/dirty
  lineage, per-population checkpoints, compatibility inventory, exclusions,
  zero holdings, and report.
- Session 9B focused tests passed 2/2; the full repository suite passed 658 with
  4 skipped and 78 existing warnings. Compilation, independent manifest/table
  validation, `git diff --check`, and the warn-only sync check also passed.

- Session 9 first validated the exact Session 8F manifest hash plus all two
  validated inputs, 33 records, seven code-lineage records, and three
  hash-covered dirty-state records with zero failures.
- The only modeled path is `CORRECTED_8F`. No stale corrected-partial artifact
  was read. `OLD_RECONSTRUCTED` is explicitly unavailable because no separately
  certified old-feature population with stable identity, proven availability,
  and 8E-aligned labels exists; none was constructed ad hoc.
- Required price support intersected with horizon-label support at
  23,575/23,575/21,030/18,613/14,209 rows for 6m/1y/2y/3y/5y in each namespace.
  Each 43,806-row horizon partition retains explicit unavailable and excluded
  counts; excluded remains 5,717.
- Every fold enforces strict `label_end_date < decision_timestamp` for training.
  Feature selection uses only pre-calibration development rows; median
  imputation, standardization, logistic/ridge models, and sigmoid classifier
  calibration are fitted only inside the historical fold. Macro is excluded.
- Both population namespaces are physically separate but have identical stable
  feature identities, gate rows, and label economics. Each prediction table has
  438,060 stable-row/horizon/model-kind records and 174,552 eligible OOS
  predictions. Early supported cohorts without two historical cohorts retain
  explicit model-unavailable reasons.
- The build froze 208 fitted fold models and 84 unavailable fold records.
  Descriptive observed-only OOS classifier AUCs for 6m/1y/2y/3y/5y are
  0.5661/0.5506/0.5680/0.5912/0.5747; regression ICs are
  0.0684/0.0174/0.0979/0.0937/0.2873. These are not backtest or acceptance
  claims; the sensitivity namespace is identical because 8E added no policy rows.
- The Session 9 manifest SHA-256 is `bb75e2be...`; all seven validated inputs,
  846 artifact records, four code-lineage records, and three dirty-state
  references independently passed after freeze.
- The corrected roadmap no longer treats stale `CORRECTED_PARTIAL` or
  unavailable `OLD_RECONSTRUCTED` as backtest sides. Session 9B must reconcile
  the explicit Session 9 scores with the accepted Session 5A strategy roles and
  freeze holdings only for a complete, already-specified compatible path. The
  Session 9 logistic classifier cannot silently replace the production
  tree-agreement probability.

- Session 8F first validated the complete 8E→8D→8C→8B→8 manifest chain: 4
  manifests, 14,504 referenced hashes, all 4,835 8E payload outcomes, and all
  8,021 8D response outcomes passed. All 4,814 market and 6,981 SEC successful
  payloads also matched their decompressed hashes.
- The only feature source is the 43,806 annual rows from
  `corrected_step2/outputs/certified_snapshots.parquet`. Stable 64-character row
  IDs preserve entity, fiscal period, and filing availability; every stage and
  both final outputs retain all 43,806 unique IDs.
- Required strict-predecision price features partition into 26,232 supported,
  11,857 unavailable, and 5,717 excluded rows. Non-supported rows contain zero
  populated required price values.
- Certified accounting and fraud-taxonomy lineage supports all 43,806 rows.
  Macro support is zero: 38,089 rows are unavailable and 5,717 excluded because
  no certified macro vintage/release-lag input exists.
- `observed_only` and `include_policy_imputed` are physically separate
  43,806-row outputs. Their support is identical and policy additions remain
  zero; no survivorship outcome was inferred.
- Step 5, Step 6, and taxonomy completed twice. Step 6's legacy quarterly/size
  and survivorship-policy imputations were disabled. Taxonomy uses proven
  `availability_timestamp` and `sec_primary_filing` provenance.
- The Session 8F manifest SHA-256 is `9c1e4b82...`; all 33 artifact records,
  seven code-lineage records, row identities, population labels, and count
  partitions independently passed after freeze.
- Session 8F did not use corrected-partial features, macro data, models, prior
  preprocessing, predictions, backtests, thresholds, commits, pushes, or any
  Session 9 action.

- Session 8E first revalidated the final Session 8D manifest and complete
  8C→8B→8 chain: all referenced files and all 6,981 stored/decompressed SEC
  payloads matched with zero failures.
- The only label source population is the 43,806 certified Session 8D annual
  rows. No stale corrected-partial row was used.
- Final observed support for 6m/1y/2y/3y/5y is respectively 24,127, 24,127,
  21,492, 19,025, and 14,514 rows. Each horizon partitions exactly into
  supported/unavailable/excluded counts; excluded is 5,717 for all horizons.
- Both population namespaces are physically separate. The policy sensitivity
  adds zero policy-only rows because no disappearance or corporate-action
  outcome was inferred.
- Frozen market evidence contains 4,814 successful chart payloads, 21 stored
  HTTP 404 bodies, 4,796 normalized symbols, 13,791,422 regular-session price
  rows, 84,274 event records, and supported IWC/IWM/MDY/SPY histories.
- Pinned XNYS/XNAS calendars contain 4,287 identical regular-session schedules
  over 2009-07-01 through 2026-07-16. OTC/CBOE and provider-exchange mismatches
  fail closed.
- The Session 8E manifest SHA-256 is `0ab15685...`; independent post-freeze
  record, payload, population, key, window, and arithmetic validation passed.
- Session 8E did not train/tune models, generate predictions, run backtests,
  optimize thresholds, overwrite existing data/8D artifacts, commit, or push.

- Session 8D validated the Session 8C chain before use: the 8C manifest, 8B
  calendar contract, Session 8 corrected-partial manifest, and all 31 referenced
  payloads matched their frozen sizes and SHA-256 hashes.
- The frozen `data/tickers.parquet` input has 8,021 unique 10-digit US CIKs and
  SHA-256 `c090752c...`; its artifact copy matches exactly.
- Final issuer classification is mutually exclusive and exhaustive: 4,937
  proven, 1,086 excluded-only, and 1,998 unavailable entities.
- Certified Step 2 contains 163,216 rows: 43,806 annual and 119,410 quarterly.
  Excluded diagnostics contain 53,360 periods: 13,052 annual and 40,308
  quarterly.
- Latest raw-source coverage is 6,981 successful official SEC Company Facts
  payloads and 1,040 explicit 404 failures. All stored compressed and
  decompressed hashes passed; 17 retry-history records are retained.
- Certified rows have stable `US:{CIK}` entity IDs, source filing date,
  end-of-New-York-date UTC availability, and
  `availability_provenance = sec_primary_filing`. Later amendments, missing
  primary revenue/assets, and unresolved equal-time accessions fail closed.
- The final Session 8D manifest is stored with the evidence set. Session 8D did
  not run Step 3+, labels, market data, models, predictions, backtests, or
  optimization.

- Session 8C validated both starting JSON artifacts before use. The calendar
  contract SHA-256 is `13cd7494...`; the corrected-partial manifest SHA-256 is
  `10b648c5...`; all 31 referenced payloads matched size and streaming hash.
- The final corrected-partial dataset has 191,579 rows, including 59,378 annual
  rows and 43,906 official-scope US annual rows. Every US annual row is excluded
  for every accepted horizon before the market-availability gate because all
  five lineage parquets omit `entity_id`, `availability_timestamp`, and
  `availability_provenance`.
- Existing filing-date-start observed/policy/unavailable payload counts for US
  annual rows are respectively: 6m 38,742/46/5,118; 1y 38,094/59/5,753; 2y
  33,944/215/9,747; 3y 30,121/292/13,493; and 5y 23,092/377/20,437. None is
  accepted calendar support, and both training populations remain at zero.
- Of 43,906 US annual rows, 12,636 lack `market_cap_at_filing` and default to
  SPY in the old computation. Benchmark entry is not persisted; stock and
  benchmark windows were independently resolved; cache dates have no exchange-
  session semantics; and cache payloads lack adjustment policy/vintage lineage.
- Session 8C supports no frozen-cache-conditioned diagnostic model path. A
  corrected Step 2 rebuild is required before downstream calendar-aligned label
  inputs can be assessed.
- The roadmap now separates recovery into Session 8D (freeze the 8,021-CIK US
  universe, official SEC Company Facts raw responses, and corrected Step 2) and
  Session 8E (contract-aligned market inputs and labels). Session 9 requires
  both and remains blocked unless a modeled horizon gains certified support.

- The Session 8 manifest parsed successfully and all 31 referenced inputs,
  outputs, corrected-code-lineage files, and dirty-state records matched their
  recorded sizes and SHA-256 hashes before calendar evidence was used.
- Session 8B freezes an annual June 30 local information cutoff, July 2
  00:00 UTC decision, July 2 00:01 UTC prediction, and annual `fiscal_year =
  Y - 1` cohort with no late-filing carry-forward.
- Every training target requires strict `label_end_date < decision_timestamp`;
  equality, missing dates, and unknown provenance fail closed. Holding and
  benchmark windows use common trading closes and the exact horizon interval.
- US SEC primary filings are the official Session 9 scope. EDINET submissions
  and DART receipts are calendar-valid but outside that US model scope;
  statement/reference dates, estimates, missing fields, and unsupported sources
  are excluded.
- Observed-only remains primary. `include_policy_imputed` is a separately named
  sensitivity with separate eligibility, models, predictions, artifacts, and
  reporting; the populations cannot be silently combined.
- The frozen stale Step 2 snapshots and every corrected-partial downstream
  dataset omit `entity_id`, `availability_timestamp`, and
  `availability_provenance`. They therefore provide zero certifiable rows under
  the Session 8B filing-availability contract.
- Session 8's forward labels start at filing date and use fixed day counts with
  independently resolved stock/benchmark dates. They cannot be reused as the
  accepted decision-aligned horizon labels.
- Session 8 validated the Session 7A manifest (`31c4f4e2...`), every frozen
  source/payload hash, all LEGACY_SAVED payloads, and the expected reference-only
  `data/prices.parquet` hash (`ead68437...`) before writing.
- Offline Step 3 opens SQLite with `mode=ro&immutable=1`, never fetches or
  refreshes, blocks cache writes, and preserves missing/empty series as null.
- The full corrected lineage contains 191,579 rows and 7,499 tickers. Separate
  price-stage, Step 5, pre-taxonomy clean, and final taxonomy datasets are kept
  under `artifacts/pit_validation/corrected_partial/`.
- LEGACY_SAVED has 58,190 annual rows; CORRECTED_PARTIAL has 59,378 annual and
  132,201 quarterly rows. Exact-key comparison finds 54,301 common rows, 5,077
  unresolved annual additions, 3,889 unresolved annual removals, and no exact
  six-field duplicate keys in either dataset.
- Schemas share 242 columns; 125 are legacy-only and 85 corrected-only. All
  54,301 common rows change at least one value across 200 shared columns.
- Corrected three-year structural eligibility is 121,695 observed stock labels,
  121,694 observed relative labels, and 381 separately identified policy
  sensitivity labels. LEGACY_SAVED has no certifiable horizon-qualified
  date/provenance population.
- All 597 likely-disappearing tickers (9,703 rows) remain in the universe.
- Material unresolved missingness includes all-null accounting-fraud scores and
  accrual inputs plus 72,799 missing size categories. Session 8 did not repair
  these stale-snapshot limitations.

- The project is a multi-factor stock screener with a three-year production ranking path.
- `modeling/constants.py::load_data()` now preserves materialized `growth_yoy`
  columns; its former global clipping path was removed in Session 4A.
- `research/factor_research.py::load_data()` likewise preserves materialized
  `growth_yoy` values after Session 4A.
- Three-year targets are derived from filing-date entry prices plus a fixed horizon.
- Before Session 2, walk-forward selection used only fiscal-year and filing-date
  cutoffs; corrected three-year paths now also enforce label availability.
- The current unit suite passes, but real-data quality and CI checks have known inconsistencies.
- Step 2 snapshot data has not been rebuilt after the earliest-primary XBRL code change.
- Step 3 defines a three-year target from `filed_date` plus 1,095 calendar days,
  using the first adjusted close on/after entry (within 5 days) and exit (within
  10 days). Returns are cumulative, not CAGR.
- The immutable legacy model-loader population has 58,013 annual deduplicated
  rows and 36,185 non-null three-year labels, but no persisted label dates or
  provenance; rebuilt pipeline output will add them later.
- The local cache reproduces the full stock-plus-benchmark label for 33,102 rows;
  3,083 labeled rows cannot be fully reconstructed reliably.
- Every audited three-year fold contains definitely incomplete outcomes. In
  `train.py` default walk-forward folds, the lower-bound affected count is
  5,477–8,616 rows per fold. `embargo_years=1` still admits 5,092–8,029.
- Static default training is also affected: `train.py` has at least 8,616
  incomplete labels at its 2021-01-01 cutoff; standalone regression has at
  least 13,942 at its 2023-01-01 cutoff.
- Accepted Session 2 contract: strict `label_end_date < scoring_date`; equality
  excluded; actual stock exit for stock returns; later stock/benchmark exit for
  relative labels; missing-date, unknown-provenance, and policy-imputed labels
  are training-ineligible.
- Session 2 retains the current January 1 fold scoring convention. Reviewing the
  real filing/rebalance calendar is deferred so the correction stays bounded.
- The current clean parquet remains immutable in Session 2. It will not receive
  silent legacy date fallbacks or be rebuilt/retrained during that session.
- Step 3 now persists actual three-year stock entry, stock exit, benchmark exit,
  combined availability date, and explicit observed-price provenance.
- Step 6 now marks heuristic likely-delisted returns and losing-class labels as
  policy-imputed with no observed availability date.
- A shared helper enforces non-null observed provenance and strict
  `label_end_date < scoring_date`; legacy schemas missing these fields yield no
  eligible three-year rows.
- The purge covers static classification/regression, feature selection, OOT,
  walk-forward CV, OOF, tuning/calibration, and backtest classifier/tree/regressor
  fits. Non-three-year target availability remains outside Session 2.
- Session 2B generalizes actual dates, provenance, and strict eligibility to all
  generated horizons and every trained 6m/1y/2y/3y/5y model path.
- Disappearing companies remain in the historical universe. `observed_only` is
  the primary default; `include_policy_imputed` is a separate `-50%` sensitivity
  population and cannot be selected silently.
- A policy label becomes sensitivity-eligible only after both its target horizon
  and the three-year no-filing detection lag have elapsed. It remains explicitly
  heuristic, not a claimed corporate-action truth.
- Both shared loaders globally fit 1st/99th percentile bounds for every
  `growth_yoy` column. On rows through FY2020, 2008–2025 bounds change 4,071
  historical feature cells across 15 columns relative to through-2020 bounds.
- Fraud-taxonomy accounting, dilution, quality, and distress scores globally
  clip/rank all years. Recomputing on rows through 2020 changes nearly every
  comparable historical score; dilution also uses an ungrouped, row-order-
  dependent EPS percentage change.
- Step 5/Step 6/alpha fiscal-year cohort ranks, percentiles, winsorization, and
  size imputation are not as-of at the individual filing date. The repository's
  PIT checker reports that only 54% of sector peers have filed at the median
  company's filing date.
- Static and standalone feature selection use held-out test distributions for
  PSI filtering. OOF/regression paths can reuse feature sets selected outside
  their historical folds, and static historical scores use final models plus
  same-year expanding medians.
- `likely_delisted` is inferred from the full panel's future last-filing pattern
  and is used directly as a historical backtest/screener gate. Dated policy-label
  eligibility does not make that hard gate PIT-safe.
- Fold-specific medians, scalers, regression target bounds, model fits, and
  validation calibration are generally fitted on their supplied training or
  development populations; they still inherit upstream feature leakage.
- Accepted correction order is 4A global loader/taxonomy transforms, 4B fold-local
  feature/preprocessing lineage, then 4C only after an explicit scoring-calendar
  and eligible-cohort contract is accepted.
- Session 4A is limited to audit findings T12–T15. Session 4B covers T19 and
  T21–T30 without changing model families or generating artifacts. Session 4C is
  limited to the cohort-dependent portions of T05–T09 and T16.
- T11's future-derived historical `likely_delisted` gate and T31's static score/
  same-year median fallbacks are backtest-consumption defects: Session 5 must
  quantify their paths and Session 6 may correct only the accepted boundary.
- Session 4A removed the T12/T13 loader clipping paths; materialized `growth_yoy`
  values now pass through both loaders unchanged.
- T14 taxonomy clipping/ranking now uses only rows with `filed_date` less than or
  equal to each scored row's timestamp. Equal timestamps are processed together,
  and missing timestamps fail closed.
- T15 EPS change is computed within ticker after filing-date chronology (with
  fiscal year as an additional key when present) and realigned to input rows.
- Static PSI uses the declared validation population; test rows do not choose
  candidates or features.
- OOF, classifier/regression walk-forward, and backtest walk-forward paths now
  select candidates/features inside each eligible training fold.
- Persisted feature/preprocessing lineage records exact dataset, training,
  selection, development, horizon, target, label-policy, cutoff, selector, and
  feature compatibility. Missing or mismatched lineage raises `LineageError`.
- OOF has no `model_meta.json` or full-panel feature fallback. Tuning rejects
  legacy metadata, and walk-forward CV rejects static tuned parameters without
  fold-specific compatible lineage.
- Optional sector normalization now fits on eligible training rows and applies
  frozen parameters to validation/test rows. This is the only T26 application
  correction included in 4B.
- A single calendar cannot represent both row materialization and portfolio
  comparison: filing-time features need each row's proven availability timestamp,
  while cross-sectional portfolio ranks need one common decision timestamp.
- Current historical backtests use January 1 of `fiscal_year + 1`; only 33.45%
  of all annual rows and 17.7% of US annual rows are available by that cutoff.
  This is documented legacy behavior, not an accepted forward calendar.
- `filed_date` is not uniform across markets: US code uses SEC primary filing
  dates and JP uses EDINET submission dates; CA/EU use statement dates, BR uses
  a reference date, and KR mixes DART receipt dates with an unflagged estimate.
  The clean data lacks an availability-provenance field.
- The 56 duplicate `(ticker, fiscal_year)` keys represent distinct CIKs. The
  clean annual data has no duplicate `(cik, market, fiscal_year)` keys, so ticker
  must not be used as the issuer identity for cohort membership.
- Shared Session 4C cohort logic requires stable `entity_id`, proven publication
  timestamp/source, fiscal year, period type, and market. SEC primary filings,
  EDINET submissions, and DART receipts are permitted; estimates, statement/
  reference dates, missing provenance, and legacy schemas fail closed.
- Equal publication timestamps are ranked as one batch. Later entity-period
  versions are ineligible under the earliest-primary policy, and unresolved
  equal-time collisions fail closed without inferred accession precedence.
- Step 5 history winsorization uses strictly prior proven same-market annual
  rows (minimum 50); market ranks require 10 available peers and sector ranks
  require 5. Sparse groups never use current/later or global populations.
- Step 6 accruals use the available fiscal-year/market cohort at 20 observations,
  then prior proven market history at 50; size imputation ranks all available
  cohort assets at a 20-observation minimum.
- Filing-time alpha factors use the same 10-observation market cohort. Value and
  growth now rank their cohort-winsorized signals; definitions, directions, and
  weights are unchanged.
- The frozen clean parquet has 58,190 annual rows across FY2008–FY2027 and lacks
  every horizon-qualified label date/provenance column required by the current
  strict backtest scorer. All current walk-forward classifier, tree, and
  regression training populations therefore fail closed to zero rows.
- The same parquet contains final/static `ml_1y`, `ml_3y`, `ml_5y`, and
  `ml_pred_excess_3y` values on all 58,190 rows without compatible row-level
  lineage. Persisted OOF 1y/3y/5y values cover 51,475 rows, exactly FY2014–FY2025,
  also without a prediction manifest.
- Only the 3y final classifier is locally present; its metadata records a 2023
  training cutoff. Current scoring would expose all 44,623 rows through FY2023
  to final scores, and 43,384 of those rows exercise at least one same-year
  expanding-median feature fill.
- The saved `backtest_results.json` contains one `ml_gates` result for
  FY2013–FY2023 and 161 aggregate holdings, but does not persist their identities,
  scores, model/fold IDs, training cutoffs, features, medians, or fallback
  choices. None of the eleven years can be certified as walk-forward or static.
- `ml_gates` requires the walk-forward tree gate, ranks by walk-forward 3y
  regression when more than five scores survive, and otherwise falls back to a
  slice-selected 3y classifier. The shared `_ml` helper silently selects static
  scores when a slice has at most five walk-forward scores.
- Engine `composite`, `qem`, `scdv`, and `iarb` can retain rows missing individual
  ML scores through non-ML components; `nlargest` can include missing scores when
  `top_n` exceeds non-null coverage. The intended fail-closed rule is not tested
  or documented.
- `alpha_composite` does consume model predictions indirectly: its 20%-weighted
  fraud-risk factor averages available OOF 1y/3y/5y ranks with fraud/distress
  signals. Registry strategies can then add the same OOF families directly.
- The frozen clean parquet does not contain `likely_delisted`, so the June 29
  saved result skipped all conditional delisting gates. Applying the Step 6
  definition in memory would flag 4,535 rows through FY2023; 296 survive the
  current common lag/January-1/cap/Beneish predicates and would be removed.
- The latent Step 6 flag uses the full panel maximum year and ticker last year,
  then copies eventual status to every ticker row. The frozen panel reaches
  FY2027, so the counterfactual flags last-filing year 2024 or earlier.
- Monthly NAV fills missing ticker-month returns with zero, treats post-
  disappearance months as zero when peers have data, ignores annual costs and
  `-50%` return imputations on partially covered years, and performs no annual/
  monthly reconciliation.
- CAGR and Sharpe use annual net returns; only max drawdown/duration use monthly
  prices when supplied. Calmar mixes annual CAGR/volatility with monthly
  drawdown or a 2-sigma proxy, so the official metrics do not share one NAV.
- Roadmap Session 5A now freezes four contracts before implementation: row-level
  prediction lineage, fail-closed missing scores, corporate-action/disappearance
  treatment, and a canonical reconciled return stream.
- Session 6A is limited to OOS prediction consumption and score exclusions;
  Session 6B is limited to historical delisting-gate and monthly-NAV behavior.
- The frozen saved result must be labeled `LEGACY_SAVED` and cannot be one side
  of a controlled comparison. A newly generated old-data run is separately
  labeled `OLD_RECONSTRUCTED`.
- Horizon-specific decision/prediction/holding/benchmark calendars are accepted
  in Session 8B before Session 9 generates predictions.
- Session 9C must reject incomplete selected-holding market data before Session
  10; the current selection-dependent 448-ticker cache is not presumed canonical.
- The proposed Session 5A lineage contract permits only manifest-backed
  `walk_forward_oos`/`oof_oos` historical scores and preserves the saved aggregate
  result as non-reproducible `LEGACY_SAVED` evidence.
- The proposed missing-score contract requires every selected row to have every
  declared ML role and requires all `target_n` holdings for an official period;
  smaller portfolios are separately named sensitivities.
- The proposed corporate-action contract removes future-derived disappearance
  status from historical selection, requires dated event/security evidence, and
  leaves an unresolved selected holding unavailable for the primary NAV rather
  than dropping or imputing it. For personal small-cap operation, a separately
  named live check may use current tradability/filing/liquidity evidence, while
  historical portfolio reporting shows distinct `-50%` and `-100%` unresolved-
  disappearance sensitivities. Model training remains on the existing separate
  `-50%` sensitivity unless later changed by an accepted modeling contract.
- The proposed return contract uses a security-level ledger and one month-end
  net total-return NAV for CAGR, Sharpe, drawdown, and Calmar, with strict price
  coverage and annual/monthly reconciliation.
- The user explicitly accepted all four Session 5A contracts on 2026-07-15.
  The accepted historical-selection wording prohibits full-panel or eventual
  disappearance status while allowing separately named as-of listing,
  filing-staleness, delisting-notice, quote-recency, and ADTV gates based only on
  information available at the decision timestamp.
- Session 6A centralizes historical score validation in
  `modeling/prediction_lineage.py`; allowed sources are only
  `walk_forward_oos` and `oof_oos`, with complete row, fold, population,
  feature/preprocessing, model, decision, and code-state lineage.
- Engine `ml_gates`, composite, QEM, SCDV, and IARB; fraud-risk/alpha
  composite; screener/alpha registries; and the IC-weighted portfolio registry
  now fail closed on every missing or incompatible direct/indirect ML role.
- `target_n` is exact for official engine/registry periods. Insufficient
  coverage forms no portfolio and persists candidate/gate/role/selection counts
  plus row-level score source and exclusion reason.
- The public backtest load path no longer trains or loads final models. Static
  classifier columns are removed, and changing final/static columns cannot
  affect manifest-backed historical selection.
- The obsolete private in-engine walk-forward training generator and its unused
  dependencies were removed during Session 6A closeout; there is no callable
  backtest-engine score-generation bypass left.
- Roadmap Session 6A is marked complete, and PIT validation Markdown reports are
  explicitly unignored so the deliverables can be tracked in a future commit.
- Session 6B removes `likely_delisted` from historical engine, screener-registry,
  and leverage gates. Optional as-of listing, filing-staleness, delisting-notice,
  quote-recency, and ADTV gates require their own decision-time evidence and
  provenance; incomplete supplied evidence fails closed.
- `backtest/monthly_nav.py` now owns the continuous security-level month-end net
  total-return NAV. It requires complete entry/monthly prices or one sourced
  event resolution, preserves holding weights, charges configured trade costs,
  holds resolved cash at 0%, floors economic value at zero, and records ledger
  and exclusion evidence.
- The observed-only path assigns no outcome to an unresolved event. The
  separately named `include_policy_imputed_50` and
  `include_policy_imputed_100` paths apply only to an explicitly recorded
  unresolved event and never affect historical selection or model labels.
- Annual and horizon performance are derived from canonical monthly NAV.
  Synthetic annual endpoints reconcile to monthly products within `1e-10`.
  CAGR, volatility, Sharpe/Sortino, drawdown, Calmar, and monthly tail metrics
  share `net_nav`; Sharpe/Sortino are unavailable without complete aligned
  monthly risk-free returns.
- The standalone IC-weighted long-only portfolio backtest also consumes the
  canonical NAV. Its long/short mode fails closed because financing, borrowing,
  and short-ledger terms are outside the accepted contract.

## Files Changed

- Session 8B added the frozen
  `artifacts/pit_validation/calendar_contract/session8b_calendar_contract.json`
  configuration and `reports/pit_validation/08b_horizon_calendar_contract.md`;
  it updated the roadmap, changelog, and this handoff only. It changed no
  production code, tests, datasets, models, predictions, price caches,
  thresholds, backtests, or production comparisons.
- Project Codex defaults only: `AGENTS.md`, `.codex/config.toml`,
  `.codex/hooks.json`, `docs/CODEX_WORKFLOW.md`, `docs/CODEX_ROADMAP.md`,
  and this handoff.
- Session 1 added `reports/pit_validation/01_label_availability_audit.md` and an
  `[Unreleased]` changelog entry. No production files or artifacts changed.
- Session 2 changed `pipeline/step3_enrich_prices.py`, `pipeline/step6_clean.py`,
  `modeling/label_eligibility.py`, `modeling/constants.py`, `modeling/train.py`,
  `modeling/train_regression_model.py`, `modeling/score_oof.py`,
  `modeling/tune.py`, and `backtest/engine.py`.
- Session 2 added focused tests in `tests/modeling/test_label_eligibility.py` and
  extended Step 3/Step 6 pipeline tests, plus
  `reports/pit_validation/02_fold_purge_summary.md` and documentation updates.
- Session 2B revised the same pipeline/model paths to use horizon-qualified
  fields, added explicit label-policy CLI/configuration plumbing, extended the
  roadmap's rebuild/retraining comparisons, and added
  `reports/pit_validation/02b_horizon_survivorship_summary.md`.
- Session 3 added `reports/pit_validation/03_transformation_audit.md` and a
  changelog entry only. No corrective implementation was made.
- This documentation task revised `docs/CODEX_ROADMAP.md`, this handoff, and
  `CHANGELOG.md` only. It did not change production code, data, models,
  predictions, reports, or generated artifacts.
- Session 4A changed `modeling/constants.py`, `research/factor_research.py`, and
  `pipeline/enrich_fraud_taxonomy.py`; added
  `tests/modeling/test_loader_invariance.py`; extended the fraud-taxonomy tests;
  and added `reports/pit_validation/04a_global_transform_correction.md` plus
  required changelog/handoff updates.
- Session 4B added `modeling/fold_lineage.py` and
  `tests/modeling/test_fold_lineage.py`; changed `modeling/train.py`,
  `modeling/run_feature_selection.py`, `modeling/score_oof.py`,
  `modeling/train_regression_model.py`, `modeling/tune.py`,
  `backtest/engine.py`, and `research/proper_split_backtest.py`; and added
  `reports/pit_validation/04b_fold_lineage_correction.md` plus required
  changelog/handoff updates.
- The Session 4C prerequisite documentation task added
  `reports/pit_validation/04c_calendar_contract_proposal.md`, inserted roadmap
  Session 4C0, and updated the changelog/handoff only. It changed no production
  code, tests, data, or artifacts.
- Session 4C added `pipeline/event_time_cohorts.py` and
  `tests/pipeline/test_event_time_cohorts.py`; changed the bounded Step 2 source
  provenance emitters, Step 5/standalone PIT winsorization, Step 6 cohort
  transforms, alpha factor rank modules, and their synthetic/integration tests;
  and added `reports/pit_validation/04c_cohort_transform_correction.md` plus
  changelog/handoff updates.
- Session 5 added `reports/pit_validation/05_backtest_path_audit.md` and updated
  `CHANGELOG.md` and this handoff only. It changed no production code, tests,
  data, models, predictions, price caches, or backtest artifacts.
- The post-Session-5 planning update changed `docs/CODEX_ROADMAP.md`,
  `CHANGELOG.md`, and this handoff only. It added no code, tests, data, models,
  predictions, price caches, or backtest artifacts.
- Session 5A added and then froze the accepted
  `reports/pit_validation/05a_backtest_contract.md`; it updated `CHANGELOG.md`
  and this handoff only. It changed no production code, tests, data, models,
  predictions, prices, calendars, or backtest artifacts.
- Session 6A added `modeling/prediction_lineage.py`,
  `tests/modeling/test_prediction_consumption.py`, and
  `reports/pit_validation/06a_prediction_consumption_correction.md`; changed
  `backtest/engine.py`, alpha fraud-risk consumption, both registry builders,
  the IC-weighted portfolio builder, affected synthetic fixtures/tests,
  `CHANGELOG.md`, and this handoff.
- Session 6B added `backtest/monthly_nav.py`,
  `tests/backtest/test_monthly_nav.py`, and
  `reports/pit_validation/06b_monthly_nav_correction.md`; changed the historical
  engine, alpha/screener registry consumers, IC-weighted portfolio backtest,
  leverage gate, affected backtest/portfolio tests, roadmap, changelog, and this
  handoff.
- Session 8 changed the four audited pipeline CLIs for artifact-scoped offline
  execution, extended focused Step 3 tests, added corrected-partial comparison
  and streaming-manifest utilities, generated the corrected artifacts, and
  added `reports/pit_validation/08_data_comparison.md` plus roadmap/changelog/
  handoff updates.
- Session 8C added the validation-only market-input manifest and
  `reports/pit_validation/08c_training_label_market_inputs.md`; it updated the
  roadmap, changelog, and this handoff. It changed no code, source datasets,
  price caches, labels, models, predictions, or backtest artifacts.
- The post-8C roadmap update added planned Sessions 8D and 8E and tightened the
  Session 9 prerequisite; it changed roadmap/changelog/handoff documentation
  only.
- Session 8D added `pipeline/step2_artifact_rebuild.py`,
  `quality/freeze_corrected_step2.py`, focused Step 2 artifact tests, the frozen
  `artifacts/pit_validation/corrected_step2/` evidence set, and
  `reports/pit_validation/08d_step2_provenance_rebuild.md`; it updated the US
  Step 2 selector, roadmap, changelog, and this handoff.
- Session 8E added `pipeline/build_contract_label_inputs.py`, focused tests,
  the frozen `artifacts/pit_validation/contract_aligned_label_inputs/` evidence
  set, and `reports/pit_validation/08e_contract_aligned_label_market_inputs.md`;
  it updated the roadmap, changelog, and this handoff.
- Session 8F added `pipeline/build_corrected_feature_population.py` and focused
  tests; changed Step 5 macro/market-cap handling, Step 6 fail-closed execution
  controls and row identity, and taxonomy availability clocks; generated the
  frozen `artifacts/pit_validation/corrected_feature_population/` evidence and
  `reports/pit_validation/08f_corrected_feature_population.md`; and updated the
  roadmap, changelog, and this handoff.

## Verification

- Session 9 focused verification passed 14 tests before the final run. An
  initial artifact attempt failed closed on a selector-target naming defect;
  the incomplete directory was preserved under `/tmp`, outcomes were removed
  from selector candidates, the regression test passed, and the canonical
  artifact root was rebuilt from scratch.
- Independent post-freeze validation matched the Session 9 manifest plus all 7
  inputs, 846 records, 4 code records, and 3 dirty-state references. Both
  438,060-row prediction tables have unique stable-row/horizon/model-kind keys;
  eligible predictions have ranks and all artifact IDs, ineligible records have
  explicit reasons, and all 208 fit folds satisfy the strict label-end cutoff.
- All 208 selected-feature records exclude the frozen macro list. The two
  namespaces' feature, gate, and label fingerprints match while their source,
  model, prediction, and checkpoint paths remain physically separate.
- Final full verification passed 656 tests with 4 skips and 78 pre-existing
  pandas fragmentation/date warnings.

- Session 8F focused verification passed 105 tests. The pre-edit full-chain
  validation passed 4 manifests and 14,504 referenced hashes, including all
  stored/decompressed 8D/8E successful payloads.
- Session 8F full verification passed 652 tests with 4 skips and 78 pre-existing
  pandas fragmentation/date warnings; targeted compilation also passed.
- Both 43,806-row final parquets have 254 columns, unique stable row IDs, stable
  entity/availability provenance, zero policy-imputed labels, and no populated
  required price values outside the supported price population. Independent
  manifest validation passed all 33 artifact and seven code records.

- Session 8E focused verification passed 5 tests. Independent final validation
  rehashed all manifest/inventory records, 4,814 decompressed successful
  payloads and 21 stored failure bodies, and validated both 219,030-row gate
  tables plus 103,285 observed labels with zero errors.
- Session 8E combined label-focused verification passed 17 tests. Full
  verification passed 648 tests with 4 skips and 78 pre-existing pandas
  fragmentation/date warnings; compilation, `git diff --check`, and the
  warn-only sync check passed.

- Session 8D revalidated the complete Session 8C chain, froze the exact
  8,021-CIK input, and independently matched all latest response CIKs to the
  universe. All 6,981 stored successful response gzip and decompressed payload
  hashes passed; 1,040 official 404 failures remain explicit.
- Certified output validation passed required-schema, non-null provenance,
  stable entity ID, UTC timestamp parsing, US/source consistency, and unique
  entity-period-key checks. The issuer partition is exactly 4,937 proven +
  1,086 excluded-only + 1,998 unavailable = 8,021.
- Focused Session 8D synthetic verification passed 34 tests; targeted modules
  compiled successfully.

- Session 8C parsed and semantically checked the Session 8B contract, then
  streamed and verified all 31 corrected-partial manifest payload hashes and
  sizes with zero failures. Raw output is
  `/tmp/session8c_manifest_validation.log`.
- Read-only schema/count checks covered the frozen Step 2 snapshot, all four
  downstream lineage parquets, the 43,906-row US annual population, accepted-
  horizon target/provenance/date fields, benchmark assignments, and the frozen
  SQLite schema/benchmark entries. No executable code changed, so no pytest run
  was required.

- Session 8B manifest validation checked 31 referenced records with zero size
  or SHA-256 failures; raw output is under
  `/tmp/session8b_manifest_validation.log`.
- Read-only schema checks confirmed that the frozen stale snapshots and all
  corrected-partial downstream parquets lack the required filing availability
  and entity fields. Bounded source checks confirmed the eleven materialized
  horizons, five modeled horizons, strict label helper, market provenance
  emitters, and current filing-date-start return behavior.
- Session 8B configuration JSON parsed and passed contract assertions for all
  eleven mutually exclusive accepted/excluded horizons, exact month mappings,
  five accepted model paths, strict cutoff wording, and observed/policy
  separation. Documentation whitespace checks passed. No tests were run because
  this policy-only session changed no executable code.
- Read-only dataset/cache analysis completed against frozen local inputs.
- Dataset SHA-256: `520a9b52e2a63d013a3527abbcde32c484a226c2739450d2a6a48ab175144dae`.
- Price-cache SHA-256: `d0e7c3ee05d89751ad86c3a2a763bbc322672448634e345b3a1a982c647c3def`.
- Focused tests: `python3 -m pytest tests/pipeline/test_step3_enrich_prices.py
  tests/backtest/test_walk_forward.py -q` → 52 passed, 9 pre-existing pandas
  `FutureWarning` messages.
- `git diff --check` passed.
- Session 1 changed no production code, data, models, predictions, or backtest
  artifacts.
- Session 2 focused verification: 72 passed with 9 pre-existing pandas
  `FutureWarning` messages.
- Targeted modules compile successfully with bytecode redirected to `/tmp`.
- No dataset, model, prediction, price cache, or backtest artifact was changed.
- Session 2B focused verification: 86 passed with the same 9 pre-existing pandas
  `FutureWarning` messages.
- Session 3 ran bounded source/notebook searches, read-only parquet invariance
  calculations, and `python3 -m quality.pit_validate`. The latter completed and
  independently reported residual full-year sector-percentile bias. A direct
  script invocation failed on the repository's `_root` import; module invocation
  succeeded.
- Roadmap/handoff sections were reread, and `git diff --check -- CHANGELOG.md`
  passed for the tracked documentation change. No production or artifact
  verification was required for this docs-only task.
- Session 4A focused verification: 77 passed across fraud taxonomy, modeling
  loader, and factor-research tests; `git diff --check` passed. A 58,000-row
  synthetic as-of rank completed in about 0.56 seconds. No data, model,
  prediction, or backtest artifact changed.
- Session 4B focused verification: 93 passed across modeling, synthetic
  fold-lineage, backtest walk-forward, and research IC/factor tests, with 9
  pre-existing pandas `FutureWarning` messages; targeted compilation and
  `git diff --check` passed. No data, feature/model, prediction, or backtest
  artifact changed.
- Session 4C0 evidence used bounded source reads and read-only parquet summaries.
  No model/backtest execution or production/artifact write occurred. Documentation
  `git diff --check` passed for the roadmap, handoff, and changelog changes.
- Session 4C focused verification: 113 passed across source provenance, Step 5,
  Step 6, event-time cohorts, and alpha factors. Full verification: 592 passed,
  4 skipped, with pre-existing pandas fragmentation/FutureWarning messages.
  Targeted compilation and `git diff --check` passed. No dataset, model,
  prediction, or backtest artifact changed.
- Session 5 used bounded source reads, artifact hashes, read-only parquet/JSON
  counts, and arithmetic over saved annual returns. It did not call the scorer,
  fit models, run strategies/backtests, rebuild data/cache, or regenerate an
  artifact. Documentation `git diff --check` passed.
- Post-Session-5 roadmap references and ordering were checked with bounded
  searches; documentation `git diff --check` passed. No production verification
  was required for the planning-only change.
- Session 5A documentation was checked for contract completeness, acceptance
  wording, and whitespace. No tests or production commands were run because the
  accepted contract changes no behavior.
- Session 6A focused verification passed 104 prediction/strategy/alpha/registry/
  integration tests. Full verification passed 613 tests with 4 skips and 86
  pre-existing pandas fragmentation/downcasting/date warnings. Targeted
  compilation and `git diff --check` passed; the warn-only architecture sync
  check reported no changed architecture files. No dataset, model, prediction,
  price, or backtest artifact changed.
- Session 6B focused verification passed 95 backtest/portfolio/registry/
  integration/prediction-consumption tests. Full verification passed 627 tests
  with 4 skips and 78 pre-existing pandas fragmentation/date warnings. Targeted
  compilation, `git diff --check`, and the warn-only sync check passed. No data,
  model, prediction, price, registry, or backtest result artifact changed.
- Session 7 froze 39 `LEGACY_SAVED` files (108,828,661 bytes) with 45 manifest
  records: 35 present and 10 explicitly missing. The complete pre-session dirty
  state contains 44 tracked modifications and 30 untracked files relative to
  baseline `3f706e3e10d2b354c6e8b9407760fa2074749c0a`; potential credential-bearing
  `.codex/config.toml` content was excluded while its path/size/hash was recorded.
- Session 7 focused synthetic verification passed 5 tests. The frozen manifest
  passed compatibility and source-hash validation with 0 errors and 0 warnings.
  No full suite was repeated because production behavior did not change.
- Frozen clean-data SHA-256 remains `520a9b52...`; actual frozen monthly-cache
  SHA-256 is `9c7ad56e...`, matching Session 5 but not the later handoff summary's
  `d0e7c3ee...`. The manifest records the discrepancy. No data, prices,
  predictions, models, backtests, or production comparisons were generated.
- Session 8 preflight froze read-only stale Step 2 snapshots (`f0849247...`),
  daily price cache (`d0e7c3ee...`), and macro enrichment (`7eca9aa4...`) under
  `artifacts/pit_validation/corrected_partial_inputs/`; copy hashes match their
  sources and `LEGACY_SAVED` remains unchanged. The preflight manifest SHA-256
  is `31c4f4e289ac49ef7c01f333370b4e3a35a3e786c0b289200bdc95e9c5c2f4d5`.
- Offline coverage audit found 47 absent company symbols covering 1,726 Korean
  snapshot rows and 33 empty-cache symbols covering 9,816 rows. Neither group
  has saved observed forward returns. Korean benchmarks `^KS11` and `^KQ11`
  are absent, so Session 8 must preserve unavailable relative labels as null
  without network fetches, substitution, or policy imputation.
- Session 8 focused Step 3 verification passed 38 tests. The completed price
  stage has 191,579 rows/7,499 tickers and its checkpoint has all 7,499 tickers.
  Targeted compilation, final payload/schema checks, frozen-input rehashing,
  manifest validation, `git diff --check`, and the warn-only sync check passed.
  The full suite was not repeated because the new shared path is explicitly
  offline and covered by focused tests.

## Deferred Questions

- Session 8F leaves macro features explicitly unavailable, and Session 9
  excludes them. A separately authorized vintage/release-lag evidence build
  would be required before any macro feature can enter a future model build.

- Session 8E completed the selection-independent common-session label-input
  evidence. Its accepted entry timestamp must first be preserved through
  Session 9B.1. Candidate-wide liquidity is Session 9B.3 work; selected-holding
  canonical return/NAV evidence remains Session 9C work after Session 9B-R.

- A combined capital-allocation rule for overlapping annual multi-year vintages
  is not inferred by the calendar contract. Horizon evaluation is by vintage;
  any official combined NAV remains subject to the accepted canonical-NAV
  contract and later market-input evidence.
- Define a future evidence-backed corporate-action policy for acquisitions,
  bankruptcies, ticker changes, restructurings, and migrations; free data is not
  assumed comprehensive.
- Session 8C's zero-support verdict is superseded by Session 8E for all five
  accepted horizons.
- Macro vintages/release lags, adjusted-price vintages, quarterly availability,
  duplicate amendment precedence, full-sample research/strategy selection, and
  evidence-backed corporate-action treatment remain separate bounded questions.
- Sessions 9B.1–9B.3 must first restore lineage, resolve every gate/model-role
  blocker, and freeze candidate-wide liquidity. Session 9B-R must then freeze
  complete holdings, weights, and exclusions for at least one compatible
  production `CORRECTED_8F` path. Session 9C may only then prove selected-
  holding price, benchmark, risk-free, cost, and event coverage. The current
  local monthly cache is not certified as canonical or complete.
- The roadmap now requires low-token execution: reuse handoff evidence, batch
  bounded reads, keep raw logs in `/tmp`, use path-scoped diffs, run focused
  tests during implementation, and run the full suite at most once after code
  stabilizes.
- Session 7 preserved the baseline commit, complete dirty state, artifact timing,
  and corrected-code/legacy-input separation. The saved legacy claim remains
  non-reproducible because holdings, weights, folds, and score sources are absent.
- Session 8 built only `CORRECTED_PARTIAL` from frozen local inputs and does not
  imply that it reproduces the legacy run. Session 9C still covers monthly
  benchmark/risk-free series, adjusted-price/event double counting, event terms,
  turnover, and actual trade costs. Session 10 fails closed per metric when any
  required evidence is incomplete.

## Exact Next Task

Session 9C cannot begin. Execute Session 9B.1 only: revalidate the complete
8E→8F→9→9B chain, build a new immutable lineage bridge that carries Session
8E's existing entry timestamp through the 8F/9 selection identity without
recomputing outcomes, and materialize or explicitly block every accepted
non-model hard-gate field from certified corrected inputs. Do not fit the now-
frozen tree specification, calculate ADTV, select holdings, run a backtest,
begin Session 9C, commit, or push. Session 9B.2, 9B.3, and 9B-R follow only in
that order after their prerequisites pass.
