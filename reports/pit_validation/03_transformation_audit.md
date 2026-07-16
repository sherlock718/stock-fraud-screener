# Session 3 — Fitted and Future-Aware Transformation Audit

Date: 2026-07-15  
Mode: read-only methodology audit; no production code, data, model, prediction,
or backtest artifact was changed.

## Executive verdict

The repository does not yet satisfy the rule that every fitted transformation is
learned only from information available to the relevant training fold or scoring
date. The audit found four broad confirmed leakage surfaces:

1. both modeling/research loaders globally fit 1st/99th percentile bounds for all
   `growth_yoy` columns;
2. fraud-taxonomy scores globally clip and rank across all years, and the dilution
   score also takes an ungrouped, row-order-dependent `eps_diluted.pct_change()`;
3. feature selection uses held-out future/test distributions for PSI filtering,
   while OOF and walk-forward consumers can reuse feature sets selected outside
   their folds;
4. `likely_delisted` is inferred from the full panel's future last-filing pattern
   and is then used as a hard historical backtest gate.

Several annual cohort transforms are stable to adding later *years* but are not
as-of transforms at an individual filing/scoring date. These include Step 5
fiscal-year ranks, sector percentiles, Step 6 cohort winsorization/imputation, and
the alpha factor ranks. The existing PIT checker independently reports that only
54% of sector peers have filed at the median company's filing date (11% for the
10th-percentile early filer), confirming that full-fiscal-year ranks use peers
that were unavailable to early filers.

No correction is proposed or implemented in this session.

## Classification

1. **Fixed formula** — constants or row/trailing-history arithmetic; no parameter
   is estimated from a comparison population.
2. **As-of cross-sectional** — parameters/ranks come from a universe that is
   explicitly frozen at a common scoring timestamp.
3. **Training-fold fitted** — parameters, feature choices, or models are learned
   only from the eligible fold training population (or an explicitly designated
   validation population) and then frozen for scoring.
4. **Potentially global/future-aware** — full-panel, future/test, same-cohort-later,
   order-dependent, or lineage-unknown observations can affect a historical value
   or model decision.

“Potentially” is retained where the code could be valid only under an external
as-of calling contract that is not enforced by the transformation itself.

## Quantified evidence on the frozen local dataset

The clean parquet has 58,190 annual rows, fiscal years 2008–2027, 367 columns,
16 `growth_yoy` columns, all six fraud-taxonomy outputs, all six alpha outputs,
56 duplicate `(ticker, fiscal_year)` rows, and no missing `filed_date` values.
The shared loader's 2008–2025, annual, deduplicated population has 58,013 rows.

### Global loader clipping

For the 25,985 rows through FY2020, clipping with 2008–2025 bounds rather than
bounds estimated only through FY2020 changes 4,071 historical feature cells
across 15 of the 16 `growth_yoy` columns. Counts by column are:

| Column | Historical cells changed |
|---|---:|
| `revenue_growth_yoy` | 205 |
| `asset_growth_yoy` | 410 |
| `receivables_growth_yoy` | 266 |
| `inventory_growth_yoy` | 248 |
| `net_income_growth_yoy` | 371 |
| `gross_profit_growth_yoy` | 224 |
| `capex_growth_yoy` | 296 |
| `ocf_growth_yoy` | 306 |
| `eps_growth_yoy` | 306 |
| `debt_growth_yoy` | 239 |
| `rd_growth_yoy` | 168 |
| `sga_growth_yoy` | 190 |
| `ap_growth_yoy` | 284 |
| `cogs_growth_yoy` | 188 |
| `ppe_growth_yoy` | 370 |
| `lt_debt_growth_yoy` | 0 (the through-2020 comparison population is null) |

This is direct future-invariance failure, not only a code-level suspicion.

### Global fraud-taxonomy scores

Recomputing each score once on all 2008–2025 rows and once on rows through 2020
changes nearly every comparable historical score:

| Score | Comparable rows through 2020 | Rows changed | Median absolute delta | Maximum absolute delta |
|---|---:|---:|---:|---:|
| accounting | 26,013 | 25,964 | 0.0123 | 0.0480 |
| dilution | 26,013 | 25,983 | 0.0118 | 0.8795 |
| quality | 24,134 | 24,108 | 0.0253 | 0.0979 |
| distress | 26,013 | 26,012 | 0.0211 | 0.0486 |
| governance | 26,013 | 0 | 0.0000 | 0.0000 |

The governance score is fixed-threshold arithmetic; the other four contain
global percentile transformations.

## Complete active-path inventory

Equivalent operations are grouped as one transformation family, but every active
call surface located by the audit is named.

| ID | Transformation and location | Class | Evidence and consequence |
|---|---|---:|---|
| T01 | Row-wise ratios, logs, fixed buckets, fixed clipping, fixed fills in `pipeline/step5_compute_features.py`, `pipeline/feature_library.py`, and fraud/governance formulas | 1 | No cross-row fit. Examples include safe division, fixed Altman/Ohlson formulas, log lower bounds, fixed score thresholds, and `clip(-1, 5)` target policy. |
| T02 | Prior-price momentum, volatility, 52-week high, and filing-date entry values in `pipeline/step3_enrich_prices.py` | 1 | Windows end at or before `filed_date`; forward returns are labels, not input transforms. Current-vintage adjusted-price provenance remains a separate unresolved data-vintage question. |
| T03 | Per-ticker lag/rolling features in Step 2 market builders, `pipeline/feature_library.py::add_piotroski_ext`, and Step 5 5-period volatility | 1 | Sorted ascending by ticker/time and uses `shift(1)` or trailing `rolling`; no centered/negative shift found. Correctness still depends on duplicate/amendment resolution. |
| T04 | Macro resample/forward-fill/backward as-of merge in `pipeline/step4_enrich_macro.py` | 1 / 4 | Direction is backward and arithmetic is trailing, but latest FRED vintages are fetched and observation dates are used without explicit publication lag. Historical revisions/release availability are not frozen, so macro values are potentially future-aware. |
| T05 | Step 5 `winsorize_pit()` for the `ratio_cols` list | 3 / 4 | Normal path uses observations filed before the filing quarter. If fewer than 50 exist, it bootstraps with the entire current quarter; without `filed_date`, it uses `fiscal_year <= current year`. Both fallbacks admit later same-cohort observations. |
| T06 | Standalone `pipeline/winsorize_pit.py` helpers | 1 / 3 / 4 | `winsorize_training_only` is fold-fitted (3); `winsorize_by_filed_date` has the same current-quarter bootstrap issue (4); `winsorize_expanding` includes the current fiscal-year cohort (4 at an intra-year score date); `winsorize_global` is explicitly global (4). No active production call to the standalone global helper was found. |
| T07 | Step 5 quality/value composites, momentum ranks, sector percentiles, and rank-derived interactions | 4 | Grouping by `(fiscal_year, market)` or `(sic_2digit, fiscal_year)` prevents later-year contamination but uses the complete cohort, including filings not available to early filers and rows later removed by the backtest filing gate. The ungrouped fallback ranks the full panel. |
| T08 | Step 6 accrual winsorization in `winsorize_accruals()` | 4 | Fits within `(market, fiscal_year)` on the complete cohort. Groups with fewer than 20 observations fall back to quantiles from the entire dataframe; missing grouping columns also trigger global quantiles. |
| T09 | Step 6 size-category imputation | 4 | Ranks only null-size rows within `(fiscal_year, market)`, not the full cohort, and does so after all rows are present. The imputed bucket therefore depends on other missing rows and later filings in the same year. |
| T10 | Step 6 quarterly aggregations | 1 / 4 | Formulas are fixed within `(ticker, fiscal_year)`, but all quarterly rows for the fiscal year are aggregated and merged without an explicit per-target-row filing cutoff. Valid for a post-year annual filing only if source availability/duplicate lineage is proven. |
| T11 | Step 6 `likely_delisted` and policy-return transform | 4 | `max_year` and each ticker's future `last_filing_year` are fit on the full panel. Dated policy labels are now training-gated, but `backtest/engine.py` and screener filters use `likely_delisted` directly as a historical hard gate, revealing future disappearance status. |
| T12 | Shared modeling loader growth clipping in `modeling/constants.py::load_data()` | 4 | Fits 1st/99th bounds on the complete 2008–2025 loader population before every static, tuning, OOF, regression, and much research/backtest path. Quantified above. Fixed target clips are class 1. |
| T13 | Research loader growth clipping in `research/factor_research.py::load_data()` | 4 | Duplicates T12's global bounds independently and affects all factor IC/ICIR, quintile, turnover, and decay results from this script. |
| T14 | Fraud taxonomy `_pct_rank_clip()` | 4 | Fits clipping bounds and percentile ranks across every year in the parquet. Tests explicitly assert that ranking is global. Four score families fail future invariance as quantified above. |
| T15 | Fraud dilution EPS change | 4 | `eps_diluted.pct_change()` is applied to the dataframe's current row order with no ticker grouping or chronological sort. It can cross ticker/year boundaries and is order-dependent before its global rank. |
| T16 | Alpha value/quality/growth/momentum/fraud ranks in `alpha/factors/*` | 4 | Default grouping is full `(fiscal_year, market)`, so materialized historical alpha values use later same-year filings. In value and growth, the computed winsorized `sig` is unused; the actual rank is taken from the original column, so those apparent winsorizations currently have no effect. |
| T17 | Strategy-time ranks in `backtest/engine.py`, `portfolio/build_screener_registry.py`, and `portfolio/build_portfolio.py` | 2 when called on a frozen eligible slice; otherwise 4 | `backtest.run_backtest()` filters by filing date before calling its filter function, so its local strategy ranks are as-of relative to that annual slice. `build_portfolio.py` does not impose a filing-date gate itself. Persisted upstream alpha/taxonomy features remain contaminated even when the local rank is valid. |
| T18 | Current-universe ranks/medians in `portfolio/leverage_strategy.py` | 2 / 4 | Latest-year cross-sectional ranks are class 2 for a current snapshot. Classifier inputs use current-universe medians instead of persisted training medians, which is not future leakage for a live snapshot but is an unfrozen, distribution-dependent production transform. Regression correctly uses metadata medians. |
| T19 | Candidate inclusion by global fill rate | 4 | `get_feature_candidates()` is safe only when passed a fold dataframe. `research/proper_split_backtest.py` calls `get_candidates(df)` on the full panel before train/validation selection; backtest `_select_features(df)` also uses the full panel. Feature existence can therefore be decided by future coverage. |
| T20 | IC/ICIR/FDR selection and correlation dedup | 3 when called on eligible training rows | `modeling/train.py` OOT selection and `backtest/engine.py` per-year `_ic_rank()` fit on the fold training population. `deduplicate_features()` is also training-fitted when its input is the fold. |
| T21 | Static training feature selection PSI in `modeling/train.py` | 4 | PSI bins combine train and `df_test`, and features are dropped using the held-out test distribution before model fitting. This consumes the test set for model design. |
| T22 | Standalone feature-selection PSI in `modeling/run_feature_selection.py` | 4 | `df_test` is always `fiscal_year > VAL_END`, regardless of `--train-end`; the test distribution filters features. The output feature sets therefore contain future/test information. |
| T23 | “Proper split” selection in `research/proper_split_backtest.py` | 3 / 4 | Train-vs-validation PSI and train IC/dedup are legitimate class 3 development fits, but candidate fill-rate selection is made on full `df` (T19), and all inputs first pass through the globally clipping loader (T12). It is therefore not end-to-end class 3. |
| T24 | Median imputation in classification, regression, OOT, OOF, and backtest fold fits | 3 | `train_model`, `train_xgb_model`, regression training, `score_oof.train_fold`, and backtest fold paths fit medians on their passed training population and reuse them for scoring. Production notebook classifier/regressor paths use persisted train medians; its tree `fillna(0)` is fixed formula. |
| T25 | `StandardScaler` + logistic baseline | 3 | The scaler sits inside a sklearn pipeline fit on training rows and is persisted with the classifier. |
| T26 | Sector z-score normalization in `modeling/train.py` | 3 for the training fit, with an application defect outside this audit | Means/stds are estimated on the training split only. Validation/test frames are not transformed by the same stored parameters, so the option is inconsistent, but it does not directly fit on future data. |
| T27 | Regression target winsorization | 3 | `train_regression()` fits target quantiles on eligible training labels per static/fold call. Backtest regression's `clip(-1, 5)` is fixed class 1. |
| T28 | LightGBM, XGBoost, decision-tree, logistic, and CatBoost model fits | 3 when their supplied training mask is eligible | Located static, OOT, OOF, walk-forward, tree, and regression fits use training rows. This classification does not cure upstream class-4 features or externally selected feature sets. |
| T29 | Optuna tuning, CatBoost early stopping, and isotonic calibration in `modeling/tune.py` | 3 | Hyperparameters/early stopping/calibration use the explicit validation split, and test is evaluated afterward. The inputs inherit T12/T21-selected features and legacy artifact lineage. |
| T30 | OOF/walk-forward reuse of persisted feature sets | 4 | `score_oof.py` loads `feature_sets_{h}.json` or `model_meta.json` once and reuses it for every historical fold. Only `feature_sets_3y.json` exists locally; other horizons fall back to `model_meta`. Those artifacts were selected outside each early fold and their metadata do not record the new label policy. Regression walk-forward likewise loads one persisted set. |
| T31 | Static model historical scoring and expanding medians in `backtest/engine.py` and `pipeline/build_monthly_price_cache.py` | 4 | Final static models are applied to historical years. Missing values use medians from `fiscal_year <= yr`, which includes the full current-year cohort and is not filing-date gated. Static scores can also serve as an early-year fallback where walk-forward scores are absent. |
| T32 | Full-sample factor research, IC/quintile summaries, and alpha validation | 4 for model/strategy choice; descriptive otherwise | Annual IC and qcut operations are cross-sectional by year, but ranking factors by all-year IC/ICIR/decay and then adopting them uses future outcomes. `research/factor_research.py` also inherits T13; alpha validation inherits T16. |
| T33 | Alpha registry signal selection and weights | 4 | `portfolio/build_alpha_registry.py` computes all-history IC, backtest Sharpe, and selection status on the same history. `build_portfolio.py` then uses selected signals and all-history IC means as weights in its historical backtest. |
| T34 | Research feature/threshold adaptation | 4 | `research/pruned_backtest.py` adds features back until test Sharpe recovers; `research/explainable_tree.py` sweeps agreement thresholds on the reported test period and adopts 0.35. Those outputs are test-tuned, not untouched OOS choices. |
| T35 | Drift-monitor PSI | 4 as a diagnostic only | `quality/monitor_drift.py` intentionally combines reference/current arrays to define PSI bins. It does not refit production models, so this is acceptable monitoring behavior but must not be reused as an unbiased training or performance decision. |
| T36 | Portfolio/risk statistics, percentile reports, SHAP importance, bootstrap metrics | 1 / descriptive | These summarize already-produced samples and do not transform model inputs. They become class 4 only if the same evaluated sample is used to choose a strategy, threshold, or feature, as in T33/T34. |

## Deduplication and record-resolution inventory

Deduplication does not estimate numeric parameters, but it can select a later
amendment or depend on concatenation order. It is therefore audited separately.

| Location | Rule | Classification |
|---|---|---|
| `modeling/constants.py`, `research/factor_research.py`, `research/alpha_ic_validation.py`, `quality/monitor_drift.py` | sort `total_assets` descending, keep first `(ticker, fiscal_year)` | Class 4: selects the largest-assets record, not the last record known at a scoring date. |
| `research/generate_reports.py` | keep first `(ticker, fiscal_year)` without an explicit preceding sort | Class 4/order-dependent. |
| `pipeline/step6_clean.py::run_clean()` | keep first `(cik, market, filed_date, period_type)` | Class 1 record identity rule, but source precedence is implicit when conflicting duplicates exist. |
| `data_io/merge_snapshots.py` | keep last duplicate after concatenating market frames | Class 1/order-defined; provenance/precedence depends on input dictionary order. |
| `pipeline/build_monthly_price_cache.py` | concatenate existing then refreshed prices, keep last `(ticker, date)` | Class 1 and appropriate for current cache refresh; historical reproducibility requires freezing the cache. |
| `pipeline/step0_historical_universe.py` | append supplement and keep last CIK | Class 1 explicit override. |
| `pipeline/step1_fetch_tickers.py`, `step1_fetch_tickers_eu.py` | keep first CIK/ticker | Class 1 identity rule with implicit source ordering. |
| `pipeline/build_fraud_labels.py` and fraud-label merge | keep first label identity / unique ticker-year confirmation | Class 1; event-source precedence is implicit for conflicts. |
| `portfolio/leverage_strategy.py` | sort fiscal year descending, keep latest ticker | Class 2 for a current snapshot; not suitable for historical as-of reconstruction. |
| `pipeline/step6_clean.py::run_survivorship()` | keep last annual row per ticker after full-panel disappearance inference | Class 4; part of T11. |

The local parquet contains 56 duplicate `(ticker, fiscal_year)` rows before the
shared loader resolves them, so these semantics are active rather than theoretical.

## Inactive and exploratory surfaces

- Archived pipeline scripts contain trailing-window price/financial arithmetic,
  but no additional fitted scaler/imputer/encoder/quantile path was found.
- Archived research notebooks perform full-sample quantiles, qcuts, ranks, ICIR,
  and exploratory winsorization. They are class 4 for any model-selection claim
  but are not active production code.
- `notebooks/feature_ic_analysis.ipynb` performs full-history research summaries;
  it is descriptive unless its rankings are used to select production features.
- `notebooks/production_screener.ipynb` uses persisted train medians for the main
  classifier/regressor and fixed zero fill for the tree. Its upstream dataset,
  feature set, and model lineage inherit the active issues above.
- No active `OneHotEncoder`, `OrdinalEncoder`, `LabelEncoder`, `SimpleImputer`,
  `KNNImputer`, `IterativeImputer`, `MinMaxScaler`, `RobustScaler`,
  `QuantileTransformer`, PCA, RFE, RFECV, or sklearn univariate selector was found.

## Confirmed, inferred, and unresolved

### Confirmed

- T12–T15, T21–T22, T30–T34, and the full-panel `likely_delisted` gate are direct
  code-level future/test/full-sample dependencies.
- Loader and fraud-taxonomy transforms fail empirical future-invariance checks on
  the frozen local dataset.
- Annual/sector cohorts are incomplete at individual filing dates; the repository's
  own read-only PIT checker quantifies the peer-availability gap.
- Fold median/model fits themselves are generally training-fitted.

### Inferred

- Materialized Step 5/alpha full-year ranks can change portfolio ordering even if
  a later backtest step filters unavailable filings, because the stored ranks are
  not recomputed on the filtered slice.
- Persisted OOF/model feature sets embed future information in early folds unless
  their exact historical generation lineage proves otherwise. Current JSON metadata
  is insufficient to prove such lineage and predates the explicit label policy.

### Unresolved

- The exact scoring/rebalance calendar needed to define each valid as-of
  cross-section remains deferred from Session 2.
- FRED vintage/release-lag and adjusted-price-vintage behavior are not frozen in
  current artifacts.
- Duplicate annual filings do not have an explicit amendment/accession precedence
  contract.
- Quarterly feature source rows do not carry an enforced availability cutoff at
  the annual target row.
- Existing model artifacts cannot be certified against the new label-eligibility
  and transformation contracts from their metadata alone.

## Session 4 correction boundary (not implemented)

Session 4 should be limited to the accepted transformation findings. At minimum,
the acceptance decision should explicitly cover loader clipping, fraud taxonomy,
annual cohort ranks/percentiles, Step 6 cohort transforms, fold-local feature
selection, static-score fallbacks, and the future-derived `likely_delisted` hard
gate. Macro vintages, quarterly availability, duplicate amendment precedence,
and broader strategy/test selection may require separate bounded sessions if they
cannot be corrected without expanding the agreed scope.

## Read-only verification performed

- bounded `rg` inventory across active Python, workflow, and notebook code;
- targeted source reads for every located fitted/quantile/rank/dedup family;
- read-only parquet schema and future-invariance calculations;
- `python3 -m quality.pit_validate` (completed; reports the sector-rank residual
  bias and 54% median peer availability);
- no network, external data refresh, model fit, retraining, backtest, or production
  artifact write was performed; only this report and required handoff/changelog
  documentation were written.

One invocation as `python3 quality/pit_validate.py` failed because direct script
execution cannot resolve `_root`; the supported module invocation succeeded.
