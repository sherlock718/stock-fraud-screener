# Scripts Reference

All scripts live in `scripts/`. Run from the repo root.

## Pipeline Scripts

### `run_pipeline.py` — US Data Pipeline

Main pipeline for fetching, enriching, and building the US dataset.

```bash
python3 scripts/run_pipeline.py                      # Full build from step 1
python3 scripts/run_pipeline.py --step 3             # Resume from step 3
python3 scripts/run_pipeline.py --limit 100          # Test with 100 tickers
python3 scripts/run_pipeline.py status               # Show last run status
python3 scripts/run_pipeline.py features             # Re-run feature engineering only
python3 scripts/run_pipeline.py enrich-prices        # Re-run price enrichment only
```

| Flag | Default | Description |
|---|---|---|
| `--step N` | `1` | Resume from pipeline step N |
| `--limit N` | None | Cap tickers processed (dev/test) |
| `command` | `build` | `build`, `features`, `enrich-prices`, `status` |

Pipeline steps:
1. Fetch SEC EDGAR filings → raw snapshots
2. Merge snapshots into single parquet
3. Clean and normalize dataset
4. Engineer 314 features (`feature_library.py`) + 5 quarterly dynamics → 319 total
5. Enrich with price data and sector/dividend data
6. Write `data/historical_dataset_clean.parquet`

---

### `score_historical.py` — Apply trained models to full dataset

Loads `model_{1y,3y,5y}.joblib` and `model_meta.json`, scores all 58K rows, and writes
`ml_1y`, `ml_3y`, `ml_5y` float columns (probability of beating local benchmark) back to
`data/historical_dataset_clean.parquet`. Also loads `model_3y_regression.joblib` (if present)
and writes `ml_pred_excess_3y` — the predicted magnitude of 3y excess return used by the
Stage 3 magnitude ranker in `leverage_strategy.py`. Missing features are filled with
per-horizon `train_medians` stored in model_meta.json.

```bash
python3 scripts/score_historical.py                  # Score and write parquet
python3 scripts/score_historical.py --dry-run        # Score only, print stats, no write
python3 scripts/score_historical.py --parquet PATH   # Use alternate parquet path
python3 scripts/score_historical.py --skip-regression  # Skip ml_pred_excess_3y scoring
```

| Flag | Default | Description |
|---|---|---|
| `--parquet` | `data/historical_dataset_clean.parquet` | Dataset path |
| `--models-dir` | `models/` | Directory with model_*.joblib + model_meta.json |
| `--dry-run` | off | Print score distribution but do not write parquet |
| `--skip-regression` | off | Skip loading `model_3y_regression.joblib` (faster if not needed) |

**Outputs**: Updates `data/historical_dataset_clean.parquet` in-place.
After running, `ml_1y`, `ml_3y`, `ml_5y`, and `ml_pred_excess_3y` are available for the
backtester and alpha factor package.

---

### `compute_alpha.py` — Compute 5-factor alpha scores

Loads `data/historical_dataset_clean.parquet`, calls `alpha.factors.composite.compute()` to
produce cross-sectional rank scores for all five factor groups, and writes the six new columns
(`alpha_value`, `alpha_quality`, `alpha_momentum`, `alpha_growth`, `alpha_fraud_risk`,
`alpha_composite`) back to the same parquet. Dataset grows from 329 → 335 columns.

Scores are computed within `(fiscal_year, market)` peer groups so each company is ranked
against its contemporaries in the same market — not against the global universe.

```bash
python3 scripts/compute_alpha.py                     # Score and write parquet
python3 scripts/compute_alpha.py --dry-run           # Print score distributions, no write
python3 scripts/compute_alpha.py --parquet PATH      # Use alternate parquet path
```

| Flag | Default | Description |
|---|---|---|
| `--parquet` | `data/historical_dataset_clean.parquet` | Dataset path |
| `--dry-run` | off | Print score distributions but do not write parquet |

**Outputs**: Updates `data/historical_dataset_clean.parquet` in-place (329 → 335 columns).
New columns added: `alpha_value`, `alpha_quality`, `alpha_momentum`, `alpha_growth`,
`alpha_fraud_risk`, `alpha_composite` (all `float32`, range 0–1, higher = stronger signal).

**Score coverage**: ~24K rows have `alpha_composite = NaN` — these are non-US rows where price
data is absent, so momentum, value, and fraud ML signals are all null. Scores are valid for all
US rows and the majority of EU/KR rows.

**Re-run when**: any of the five factor modules in `alpha/factors/` are changed, or after
`score_historical.py` adds updated `ml_*` columns.

---

### `impute_features.py` — Recover quarterly features and size_category

Fills two categories of missing data in `data/historical_dataset_clean.parquet`:

1. **Quarterly features** — The 5 intra-year dynamics (`revenue_qoq_std_norm`,
   `earnings_qoq_mean`, `max_accruals_ttm`, `revenue_acceleration`,
   `quarterly_positive_rev_frac`) are absent from the clean parquet because the
   pipeline removes quarterly rows in step 6 before enrichment runs. This script reads
   quarterly rows from `data/historical_dataset.parquet` (pre-clean, 176MB), computes
   the features via `compute_quarterly_features()`, and left-joins onto the clean parquet
   by `(ticker, fiscal_year)`. Coverage: 67% of annual rows (companies with ≥2 quarterly
   filings).

2. **size_category imputation** — 17,226 rows have null `size_category` but non-null
   `log_assets`. These are assigned buckets 0–3 via percentile rank within each
   `(fiscal_year, market)` peer group. A `size_category_imputed` boolean flag is added.

```bash
python3 scripts/impute_features.py                           # Compute and write parquet
python3 scripts/impute_features.py --dry-run                 # Coverage stats only, no write
python3 scripts/impute_features.py --parquet PATH            # Alternate clean parquet path
python3 scripts/impute_features.py --source PATH             # Alternate pre-clean parquet path
```

| Flag | Default | Description |
|---|---|---|
| `--parquet` | `data/historical_dataset_clean.parquet` | Clean dataset to enrich |
| `--source` | `data/historical_dataset.parquet` | Pre-clean parquet with quarterly rows |
| `--dry-run` | off | Print coverage stats but do not write parquet |

**Outputs**: Updates `data/historical_dataset_clean.parquet` in-place (335 → 341 columns).
New columns: 5 quarterly dynamics + `size_category_imputed` flag.

**Re-run when**: clean parquet is rebuilt (full pipeline re-run) — the join is idempotent;
existing quarterly columns are dropped before re-merging.

---

### `patch_equity_vol_features.py` — Equity coalesce fix + multi-horizon volatility

One-time backfill script that fixes equity-derived features and adds multi-horizon price volatility columns to `data/historical_dataset_clean.parquet`.

**Two operations:**
1. **Equity patch** — joins `snapshots_combined.parquet` on `(cik, fiscal_year)`, coalesces `equity` (92.4% fill) into `total_equity`, recomputes `roe`, `roic`, `pb_ratio`, `book_to_market`, `net_debt_to_equity`, sector percentile ranks, and 5yr rolling volatility.
2. **Volatility patch** — reads `data/price_cache.db` (7,753 tickers, daily prices as JSON), computes annualised daily-return volatility over 6m / 36m / 60m windows.

```bash
python3 scripts/patch_equity_vol_features.py              # Apply both patches and save
python3 scripts/patch_equity_vol_features.py --dry-run    # Report fill rates, no write
```

| Flag | Default | Description |
|---|---|---|
| `--dry-run` | off | Print fill rate stats but do not write parquet |

**Outputs**: Updates `data/historical_dataset_clean.parquet` in-place (341 → 346 columns).
Creates backup at `data/historical_dataset_clean.parquet.bak_pre_patch` before writing.
New columns: `roa_volatility_5yr`, `earnings_stability_roa_5yr`, `vol_prior_6m`, `vol_prior_36m`, `vol_prior_60m`.
Fixed columns (all were near-0% fill due to equity coalesce bug): `roe`, `roic`, `pb_ratio`, `book_to_market`, `net_debt_to_equity`, `roe_sector_pct`, `pb_ratio_sector_pct`, `roe_volatility_5yr`, `earnings_stability_5yr`.

**Dependencies**: Requires `data/snapshots_combined.parquet` and `data/price_cache.db`.

---

### `patch_montier_c2.py` — Montier C2 null fix

One-shot patch that recomputes all 7 Montier C-score columns in `data/historical_dataset_clean.parquet` after fixing the root cause in `pipeline/step5_compute_features.py` (C2 used `property_plant_equipment` which is 95.7% null; changed to `ppe_net` which is 19.4% null).

```bash
python3 scripts/patch_montier_c2.py            # Recompute and save
python3 scripts/patch_montier_c2.py --dry-run  # Print null rates only, no write
```

| Flag | Default | Description |
|---|---|---|
| `--dry-run` | off | Print post-fix null rates but do not write parquet |

**Fixed columns**: `montier_c1` (24.5% null), `montier_c2` (41.6% null, was 100%), `montier_c3` (21.8%), `montier_c4` (48.3%), `montier_c5` (26.5%), `montier_c6` (16.9%), `montier_c_score` (24.5%).

**Root cause**: `add_montier_c_score()` in `step5_compute_features.py` computed the C2 depreciation rate signal using `property_plant_equipment` (95.7% null). The null mask `c2[(dep.isna()) | (ppe.isna())] = np.nan` propagated those nulls through the entire `montier_c2` column. Fix: use `ppe_net` (19.4% null).


---

### `run_pipeline_eu.py` — EU Pipeline (yfinance free-data)

Orchestrates the full 6-step EU pipeline using Wikipedia index scraping (step 1) and yfinance fundamentals (step 2). No API key required. Covers ~350+ major tickers across DE, FR, NL, BE, SE, NO, DK, FI, IT, ES, PT, AT, IE (~4–5 years of history).

```bash
python3 scripts/run_pipeline_eu.py build              # full build (steps 1–6)
python3 scripts/run_pipeline_eu.py build --step 2     # resume from step 2
python3 scripts/run_pipeline_eu.py build --limit 50   # test run
python3 scripts/run_pipeline_eu.py status             # check output file state
```

| Flag | Default | Description |
|---|---|---|
| `--step N` | `1` | Resume from step N (1–6) |
| `--limit N` | None | Cap tickers for test runs (steps 1–3 only) |

Pipeline steps:
1. `step1_fetch_tickers_eu.py` — Wikipedia index scrape (DAX, CAC 40, AEX, BEL 20, OMX, etc.)
2. `step2_build_snapshots_eu.py` — yfinance fundamentals → `data/snapshots_eu.parquet`
3. `step3_enrich_prices.py` — price enrichment → `data/prices_eu.parquet`
4. `step4_enrich_macro.py` — macro enrichment → `data/macro_eu.parquet`
5. `step5_compute_features.py` — 324 features → `data/historical_dataset_eu.parquet`
6. `step6_clean.py` — clean and validate → `data/historical_dataset_clean_eu.parquet`

After building, integrate EU into the combined dataset:
```bash
python3 pipeline/phase_a_integrate_eu.py
```

---

### `run_pipeline_kr.py` — Korea Pipeline (DART)

```bash
export DART_API_KEY=your_key
python3 scripts/run_pipeline_kr.py build
python3 scripts/run_pipeline_kr.py build --step 3
python3 scripts/run_pipeline_kr.py status
```

Requires a DART (FSS) API key. Outputs Korean company snapshots.

---

### `run_pipeline_br.py` — Brazil Pipeline (CVM + B3)

Orchestrates the full 6-step Brazil pipeline: CVM ticker list → CVM financial snapshots → price enrichment → macro enrichment → 324 features → clean/validate.

```bash
python3 scripts/run_pipeline_br.py build              # full build from step 1
python3 scripts/run_pipeline_br.py build --step 2     # resume from step 2
python3 scripts/run_pipeline_br.py build --limit 50   # test run (caps tickers in steps 1–3)
python3 scripts/run_pipeline_br.py status             # inspect all 6 BR output files
```

| Flag | Default | Description |
|---|---|---|
| `--step N` | `1` | Resume from step N (1–6) |
| `--limit N` | None | Cap tickers for test runs (steps 1–3 only) |

Output files (all in `data/`):
- `tickers_br.parquet` — BR company list (CVM + B3)
- `snapshots_br.parquet` — CVM financial snapshots
- `prices_br.parquet` — price enrichment
- `macro_br.parquet` — macro enrichment
- `historical_dataset_br.parquet` — full feature dataset
- `historical_dataset_clean_br.parquet` — clean final dataset

No API key required. Data sources: CVM public bulk CSV + brapi.dev free ticker list.

---

### `run_pipeline_jp.py` — Japan Pipeline (free tier)

Orchestrates the full 6-step Japan pipeline using the free-data variants of steps 1–2 (yfinance-based, ~122–130 TSE tickers). No API key required.

```bash
python3 scripts/run_pipeline_jp.py build              # full build from step 1
python3 scripts/run_pipeline_jp.py build --step 2     # resume from step 2
python3 scripts/run_pipeline_jp.py build --limit 50   # test run
python3 scripts/run_pipeline_jp.py status             # inspect all 6 JP output files
```

| Flag | Default | Description |
|---|---|---|
| `--step N` | `1` | Resume from step N (1–6) |
| `--limit N` | None | Cap tickers for test runs (steps 1–3 only) |

Steps 1–2 use `step1_fetch_tickers_jp_free.py` and `step2_build_snapshots_jp_free.py`. For full TSE coverage (3,800+ tickers), obtain a free EDINET API key and swap to `step2_build_snapshots_jp.py`.

Output files: `tickers_jp.parquet`, `snapshots_jp.parquet`, `prices_jp.parquet`, `macro_jp.parquet`, `historical_dataset_jp.parquet`, `historical_dataset_clean_jp.parquet`.

---

### `run_pipeline_ca.py` — Canada Pipeline (TMX)

Orchestrates the full 6-step Canada pipeline. No API key required. Data source: TMX public API.

```bash
python3 scripts/run_pipeline_ca.py build              # full build from step 1
python3 scripts/run_pipeline_ca.py build --step 2     # resume from step 2
python3 scripts/run_pipeline_ca.py build --limit 50   # test run
python3 scripts/run_pipeline_ca.py status             # inspect all 6 CA output files
```

| Flag | Default | Description |
|---|---|---|
| `--step N` | `1` | Resume from step N (1–6) |
| `--limit N` | None | Cap tickers for test runs (steps 1–3 only) |

Output files: `tickers_ca.parquet`, `snapshots_ca.parquet`, `prices_ca.parquet`, `macro_ca.parquet`, `historical_dataset_ca.parquet`, `historical_dataset_clean_ca.parquet`.

---

## Model Scripts

### `run_feature_selection.py` — Standalone Feature Selection Pipeline

Runs the full PSI → IC+FDR → ICIR → Spearman deduplication pipeline across all three horizons. IC stage uses Newey-West HAC t-statistics, Benjamini-Hochberg FDR correction (q<0.05, hard gate), and sector-neutral IC (SIC demeaning by default).

```bash
python3 scripts/run_feature_selection.py               # Full run, sector-neutral IC (default)
python3 scripts/run_feature_selection.py --no-sector-neutral  # Disable sector demeaning
python3 scripts/run_feature_selection.py --dry-run     # Print stats only, no files written
python3 scripts/run_feature_selection.py --psi-threshold 0.20 --ic-min 0.03
```

| Flag | Default | Description |
|---|---|---|
| `--psi-threshold FLOAT` | `0.25` | Drop candidates with PSI above this (institutional standard) |
| `--ic-min FLOAT` | `0.02` | Minimum \|mean IC\| to pass the IC screen |
| `--top-k INT` | `60` | Keep top-K features by \|ICIR\| before deduplication |
| `--corr FLOAT` | `0.90` | Spearman \|r\| threshold for near-duplicate removal |
| `--sector-neutral` | on | Demean return + feature by SIC sector each year before IC |
| `--no-sector-neutral` | — | Disable sector-neutral IC |
| `--dry-run` | off | Print coverage stats but do not write files |

**Outputs**:
- `models/feature_sets_{6m,1y,2y,3y,5y}.json` — selected feature list per horizon (all 5 horizons)
- `reports/feature_selection_summary.csv` — IC, ICIR, PSI, `ic_tstat_nw`, `ic_pval_nw`, `fdr_reject` for all candidates

---

### `train_models.py` — LightGBM Training

Trains LightGBM models for all 5 horizons (6m/1y/2y/3y/5y) using ICIR feature selection with
filed-date PIT-safe train/test splits. Model config: n_estimators=600, max_depth=6, num_leaves=63,
lr=0.03. FORCE_INCLUDE lists bypass ICIR ranking for momentum features on short horizons (6m/1y/2y)
where ICIR systematically under-selects momentum vs fundamentals. D1.3: macro-context features
(`macro_regime`, `yield_curve`, `credit_spread_baa`) added to FORCE_INCLUDE_6M/1Y/2Y to inject
cycle-regime signal that the PSI filter would otherwise exclude.

```bash
python3 scripts/train_models.py
python3 scripts/train_models.py --top-n 50
python3 scripts/train_models.py --train-cutoff 2017 --val-end 2019
python3 scripts/train_models.py --no-shap
python3 scripts/train_models.py --walk-forward   # PIT-safe walk-forward CV
```

| Flag | Default | Description |
|---|---|---|
| `--top-n N` | `40` | Max features per horizon after ICIR ranking |
| `--min-ic FLOAT` | `0.02` | Minimum absolute IC to include a feature |
| `--max-psi FLOAT` | `0.25` | Drop features with PSI above this threshold before IC ranking |
| `--min-ic-stability FLOAT` | `0.6` | Minimum fraction of years IC must have the correct sign. Set to 0.0 to disable; 0.6 drops directionally inconsistent features |
| `--min-ic-years INT` | `1` | Minimum years of IC data required to keep a feature (1 = off). Set to e.g. `5` to prevent ICIR inflation from features with very few historical observations |
| `--no-dedup` | False | Skip correlation deduplication (r > 0.85) |
| `--sector-neutral` / `--no-sector-neutral` | True | Demean IC scores within sectors before ranking (default on; use `--no-sector-neutral` to disable) |
| `--sector-zscore` | False | Apply within-(fiscal_year, sic_code) z-score normalization to selected features before training. Removes cross-sector valuation level differences so IC measures within-sector stock selection ability. Groups with <5 members are left unnormalized. |
| `--train-cutoff YEAR` | `2022` | Last training year (inclusive) |
| `--val-end YEAR` | `2023` | Last validation year (inclusive); test = after this |
| `--no-shap` | False | Skip SHAP computation (faster) |
| `--walk-forward` | False | Run PIT-safe expanding-window walk-forward CV; saves `reports/walk_forward_auc_{h}.csv`; returns per-fold AUC dict |
| `--use-tuned-params` | False | When used with `--walk-forward`, loads `best_params` from `model_meta.json` (written by `tune_models.py`) and injects them into each WF fold's LightGBM. Allows WF AUC measurement with Optuna-tuned hyperparameters without re-running Optuna on every fold. |
| `--embargo-years INT` | `0` | Purged walk-forward embargo: exclude most recent N training years from each WF fold to prevent adjacent-year autocorrelation leakage. Use `1` for standard purged CV. |
| `--ensemble` | False | Blend LightGBM + XGBoost (50/50) predictions in walk-forward CV folds. Requires `xgboost>=2.0.0`. Production model remains LightGBM-only. |
| `--oot-eval` | False | OOT diagnostic: retrain 3y model with cutoff=2019, test on FY2022 (beat_local_market_3y fully known as 2022+3=2025 prices exist). Does **not** overwrite production models. Saves `reports/oot_auc_diagnostic.json` |

Outputs: `models/model_{6m,1y,2y,3y,5y}.joblib`, `models/model_meta.json`

The PSI filter (`--max-psi`) runs **before** IC ranking and removes features with high Population Stability Index between training and scoring distributions. Default threshold of 0.25 is aligned with `run_feature_selection.py`.

Train split uses both `fiscal_year` and `filed_date` cutoffs — rows filed after January 1 of (TRAIN_CUTOFF+1) are excluded even if their fiscal year is in the training window. This eliminates look-ahead from late SEC filings.

Walk-forward CV (`--walk-forward`) excludes folds where forward returns have not yet fully elapsed (`max_test_year = max_fiscal_year - horizon_years + 1`), preventing inflated AUC from partially-realised returns.

---

### `generate_oof_scores.py` — Walk-Forward Out-of-Fold Scoring

Computes true out-of-sample ML scores using an expanding-window approach.
For each fiscal year Y: train on filed_date < Jan 1 of Y, score fiscal_year == Y.
Eliminates in-sample contamination from `score_historical.py`.

Writes `ml_1y_oof`, `ml_3y_oof`, `ml_5y_oof` to the parquet. Training-window rows get NaN.
Feature sets loaded from `models/feature_sets_{h}.json` (Phase B output).

```bash
python3 scripts/generate_oof_scores.py
python3 scripts/generate_oof_scores.py --horizons 1y 3y
python3 scripts/generate_oof_scores.py --min-train-years 6
python3 scripts/generate_oof_scores.py --n-estimators 600
python3 scripts/generate_oof_scores.py --dry-run
```

| Flag | Default | Description |
|---|---|---|
| `--parquet` | `data/historical_dataset_clean.parquet` | Dataset path |
| `--horizons` | `1y 3y 5y` | Horizons to score |
| `--min-train-years` | `6` | Min fiscal years in window before first OOF score |
| `--n-estimators` | `600` | LightGBM n_estimators per fold |
| `--dry-run` | off | Compute scores but do NOT write parquet |

Outputs: Updates parquet with `ml_{h}_oof` columns; `reports/oof_auc_{h}.csv` per-fold AUC audit trail.

---

### `tune_models.py` — Optuna + CatBoost Tuning

Runs Optuna hyperparameter search and trains a CatBoost ensemble.

```bash
python3 scripts/tune_models.py
python3 scripts/tune_models.py --horizon 1y --trials 100
python3 scripts/tune_models.py --no-catboost
```

| Flag | Default | Description |
|---|---|---|
| `--horizon` | All | One of `6m`, `1y`, `2y`, `3y`, `5y`; omit to tune all |
| `--trials N` | `100` | Number of Optuna trials per horizon |
| `--no-catboost` | False | Skip CatBoost training; LightGBM only |

Soft ensemble output: `0.5 × lgbm_proba + 0.5 × catboost_proba`

Uses same PIT-safe filed_date split as `train_models.py`.

---

### `train_regression_model.py` — Huber Regression for Continuous CAGR (All Horizons)

Trains LightGBM Huber regression models to predict **continuous excess return** (CAGR, decimal)
for all 5 horizons: 6m, 1y, 2y, 3y, 5y. Target is `excess_return_local_{h}` (outperformance vs
local market index), falling back to `forward_return_{h}` if the excess column is unavailable.

Reuses the frozen ICIR-selected feature set from `models/feature_sets_{h}.json` per horizon (no
new feature selection on the regression target — prevents overfitting). Primary metric: Spearman IC
(rank correlation of predicted vs actual excess return). IC > 0.05 useful; IC > 0.10 strong.

```bash
python3 scripts/train_regression_model.py                       # All 5 horizons + walk-forward CV
python3 scripts/train_regression_model.py --horizons 1y 3y 5y  # Subset of horizons
python3 scripts/train_regression_model.py --horizons 3y --no-walk-forward
python3 scripts/train_regression_model.py --train-cutoff 2020
```

| Flag | Default | Description |
|---|---|---|
| `--horizons H [H ...]` | all 5 | Horizons to train: `6m 1y 2y 3y 5y` |
| `--train-cutoff` | `2022` | Last fiscal_year included in training set |
| `--val-end` | `2023` | Last fiscal_year included in validation set |
| `--walk-forward` | on | Run expanding-window walk-forward CV (Spearman IC per fold) |
| `--no-walk-forward` | off | Skip walk-forward CV (faster) |

**Outputs per horizon** (e.g. `3y`):
- `models/model_{h}_regression.joblib` — trained Huber regressor (5 files total)
- `models/model_{h}_regression_meta.json` — feature list, IC stats, train medians, winsorization bounds
- `reports/regression_ic_{h}.csv` — walk-forward Spearman IC per fold

**Run after**: `train_models.py` (needs `models/feature_sets_{h}.json` for each horizon).
**Run before**: `score_historical.py` (to write `ml_pred_excess_{h}` columns to parquet).

---

### `backtester.py` — Walk-Forward Backtester

Walk-forward backtest engine with SPY benchmark, factor attribution, transaction costs, and
inverse-volatility weighting. Scores are generated in-process via PIT-safe walk-forward ML.
Transaction costs use a 4-tier market-cap slippage model: large-cap >$10B = 20 bps, mid-cap
$1B–$10B = 30 bps, small-cap $100M–$1B = 50 bps, micro-cap <$100M = 80 bps.

Requires `data/spy_returns.csv` (generated by `fetch_spy_returns.py`) for SPY benchmark;
falls back to equal-weight universe mean if the file is missing.

When `data/monthly_prices.parquet` is present (built by `build_monthly_price_cache.py`):
- **MaxDD** is computed from a true monthly NAV curve (reveals intra-year drawdowns invisible at annual granularity)
- **ADTV filter** removes picks whose $50K position would exceed 5% of 30d average daily dollar volume

```bash
python3 scripts/backtester.py                            # All 4 strategies vs SPY
python3 scripts/backtester.py --strategy composite       # One strategy
python3 scripts/backtester.py --top 20 --cost 30 --tearsheet
python3 scripts/backtester.py --market US --fill-missing -0.5
python3 scripts/backtester.py --no-adtv                  # Disable ADTV filter
```

| Flag | Default | Description |
|---|---|---|
| `--strategy` | `all` | `composite`, `qem`, `scdv`, `iarb`, `all` |
| `--market` | None | Filter to one market, e.g. `US` |
| `--top N` | `20` | Top N picks per year |
| `--cost BPS` | `30` | Round-trip cost bps fallback (overridden by tiered slippage when market_cap_at_filing is present) |
| `--smallcap-cost BPS` | `60` | Fallback small-cap cost bps when size_category_label is available but not market cap |
| `--min-cap N` | `50000000` | Minimum market cap filter (USD, 0 to disable) |
| `--equal-weight` | False | Equal-weight instead of inverse-vol weighting |
| `--fill-missing FLOAT` | None | Impute return for picks with missing forward_return_1y (e.g. `-0.5`) |
| `--max-filing-lag N` | `18` | Max months between fiscal year-end and filed_date (look-ahead filter) |
| `--tearsheet` | False | Print formatted tearsheet per strategy (includes SPY factor attribution) |
| `--no-adtv` | False | Disable ADTV liquidity filter (use when monthly_prices.parquet not built) |

**Output**: `data/backtest_results.json` with per-strategy: `cagr_pct`, `spy_cagr_pct`,
`excess_cagr_vs_spy`, `beta_vs_spy`, `alpha_vs_spy`, `r_squared_vs_spy`, `tracking_error`,
`annual_turnover_pct`, `var_95_pct`, `cvar_99_pct`, `max_drawdown_duration_months`, `sharpe`,
`sortino`, `calmar`, `cagr_bootstrap_mean_pct`, `cagr_bootstrap_1sigma_pct`,
`sharpe_bootstrap_mean`, `sharpe_bootstrap_1sigma`.

`run_backtest()` computes block bootstrap CIs (2000 samples, block_size=3y) via `bootstrap_ci()`.
`--tearsheet` displays Sharpe CI 1σ, CAGR CI 1σ, VaR 95%, and CVaR 99% (Expected Shortfall).

---

### `build_monthly_price_cache.py` — Monthly Price + ADTV Cache Builder

Downloads monthly OHLCV from yfinance for the ~200-300 tickers actually selected by the
backtester strategies. Writes `data/monthly_prices.parquet` used by `backtester.py` for true
monthly-NAV MaxDD and ADTV liquidity filtering.

**Step 1** — Dry-runs `composite`, `qem`, `scdv` filters for each year 2008–2023 to collect
the set of unique tickers ever picked (avoids downloading all 4,800+ tickers).
**Step 2** — Downloads monthly OHLCV from yfinance in batches of 50 (1.5 s sleep between batches).
**Step 3** — Computes `adtv_30d` as a rolling 3-month average of daily dollar volume
(`adj_close × volume / 21`), then writes the parquet.

```bash
python3 scripts/build_monthly_price_cache.py            # Full build
python3 scripts/build_monthly_price_cache.py --update   # Extend existing cache with new months
python3 scripts/build_monthly_price_cache.py --tickers-only  # Print tickers and exit
python3 scripts/build_monthly_price_cache.py --extra-tickers AAPL MSFT  # Add extra tickers
```

| Flag | Default | Description |
|---|---|---|
| `--market` | `US` | Market filter for strategy dry-run |
| `--top N` | `20` | top_n passed to strategy filters |
| `--tickers-only` | False | Print collected tickers and exit (no download) |
| `--update` | False | Extend existing cache with new months only |
| `--extra-tickers` | `[]` | Additional tickers to include beyond strategy picks |

**Output**: `data/monthly_prices.parquet` — columns: `ticker | date | adj_close | volume | adtv_30d`.
Covers 2007-01-01 through today (one extra year before backtest start for ADTV warm-up).

---

### `build_alpha_registry.py` — Alpha Registry Builder

Builds `data/alpha_registry.json` — IC + backtest statistics for all 8 alpha signals (5 factor
scores + 3 ML OOF horizons). For each signal, computes mean cross-sectional Spearman IC vs the
signal's target forward return, then runs a top-N long-only backtest with transaction costs via
`run_backtest()`. Signals are tagged `selected=true` when `IC_mean > 0.02` AND `Sharpe > 0.50`.

Factor scores (`alpha_value`, `alpha_quality`, `alpha_momentum`, `alpha_growth`, `alpha_fraud_risk`)
are computed on-the-fly using `alpha.factors.*`. ML OOF scores (`ml_1y_oof`, `ml_3y_oof`,
`ml_5y_oof`) are read directly from the parquet (pre-computed by `generate_oof_scores.py`).

```bash
python3 scripts/build_alpha_registry.py               # Default: top 20, 30 bps cost
python3 scripts/build_alpha_registry.py --top 20 --cost 30
python3 scripts/build_alpha_registry.py --market US   # US market only
```

| Flag | Default | Description |
|---|---|---|
| `--top N` | `20` | Top N picks per year in backtest |
| `--cost BPS` | `30` | Round-trip transaction cost in bps |
| `--market` | None | Restrict to one market (e.g. `US`); default: all markets |

**Output**: `data/alpha_registry.json` — top-level keys: `generated_at`, `selection_criteria`,
`signals` (list). Per signal: `signal_id`, `category` (`factor`/`ml`), `horizon`, `market`,
`features_used`, `ic_mean`, `icir`, `cagr_pct`, `sharpe`, `sortino`, `calmar`,
`max_drawdown_pct`, `max_drawdown_note` (explains 0.0 annual-freq artifact),
`cagr_bootstrap_mean_pct`, `cagr_bootstrap_1sigma_pct`, `sharpe_bootstrap_mean`, `sharpe_bootstrap_1sigma`,
`excess_cagr_vs_spy`, `beta_vs_spy`, `hit_rate_pct`, `n_years`, `top_n`, `cost_bps`, `selected`.

**Re-run when**: any `alpha/factors/*.py` compute function changes, OOF scores are regenerated,
or selection thresholds are adjusted.

---

### `build_portfolio.py` — IC-Weighted Kelly Portfolio Constructor

```bash
python3 scripts/build_portfolio.py                                      # Default: long_only, horizon=1y, all markets
python3 scripts/build_portfolio.py --strategy long_short --horizon 3y
python3 scripts/build_portfolio.py --market US --top-n 20 --tearsheet
python3 scripts/build_portfolio.py --horizon all --kelly-fraction 0.25 --tearsheet
python3 scripts/build_portfolio.py --mos-min-score 0.55 --low-vol-only  # MoS gate + low-vol filter
```

Reads `data/alpha_registry.json` to IC-weight selected signals into a composite score.
Ranks stocks, applies market-cap filter ($10M), quarter-Kelly position sizing, sector cap,
and position cap. Runs a historical annual backtest and outputs current-year holdings.

Horizon filter (`--horizon`): keeps all factor signals (horizon-agnostic) plus the ML OOF
signal matching the horizon (e.g. `--horizon 3y` keeps `ml_3y_oof`). Use `all` to include
all three OOF signals.

Latest holdings use the most recent fiscal year with ≥ `--top-n` complete signal rows
(skips 2026/2027 where OOF scores are zero).

| Flag | Default | Description |
|---|---|---|
| `--strategy` | `long_only` | `long_only` or `long_short` (top N long, bottom N short, 50/50 notional) |
| `--horizon` | `1y` | ML OOF signal horizon: `1y`, `3y`, `5y`, `all` |
| `--market` | `all` | Market filter: `US`, `KR`, `all`, etc. |
| `--top-n` | `30` | Stocks selected long (and short) per year |
| `--kelly-fraction` | `0.25` | Fractional Kelly multiplier (≤ 0.25× full Kelly) |
| `--sector-cap` | `0.40` | Maximum total weight in any single SIC sector |
| `--position-cap` | `0.05` | Maximum weight per stock (5%) |
| `--min-market-cap` | `10000000` | Minimum market cap in USD ($10M floor — micro-cap / institution-avoidance niche) |
| `--var-gate` | None | Warn if historical VaR 95% is worse than threshold (e.g. `-30` for −30%) |
| `--cvar-gate` | None | Abort if historical CVaR 99% is worse than threshold (e.g. `-40` for −40%) |
| `--mos-min-score` | None | Margin-of-safety gate: require `alpha_value >= threshold` before selection |
| `--low-vol-only` | False | Keep only stocks in bottom-half of trailing 12m volatility distribution |
| `--tearsheet` | False | Print formatted tearsheet to stdout |

**Output**:
- `data/portfolio_holdings.json` — latest-year holdings: `ticker`, `market`, `composite_score`, `weight_pct`, `market_cap_m`, `sic_code`
- `data/portfolio_backtest.json` — backtest: `cagr_pct`, `sharpe`, `sortino`, `max_drawdown_pct`, `implied_max_drawdown_pct`, `var_95_pct`, `cvar_99_pct`, `spy_cagr_pct`, `excess_cagr_pct`, `beta`, `alpha_annualised_pct`, `annual_returns`, `signals_used`, `ic_weights`

Note: `max_drawdown_pct` is computed from annual year-end snapshots and understates true intra-year drawdowns. `implied_max_drawdown_pct` = `−max(|max_dd|, 2σ)` is a conservative proxy that adds a 2-sigma floor.

**Re-run when**: alpha registry is rebuilt, or to change strategy/horizon/sizing parameters.

---

### `factor_research.py` — IC / ICIR Analysis

```bash
python3 scripts/factor_research.py
python3 scripts/factor_research.py --features gross_margin roe accruals_to_assets
python3 scripts/factor_research.py --all-horizons --ic-decay --decay-top 20
python3 scripts/factor_research.py --decay-plot
```

Computes IC, ICIR, and factor decay curves for all features. Used to select features for model training.

All ML-derived scores (`ml_1y`, `ml_3y`, `ml_5y`, `ml_6m`, `_oof`, `ml_pred_excess`, composite/alpha
scores) are excluded from IC candidates via `EXCLUDE_PATTERNS` to prevent look-ahead contamination.

| Flag | Default | Description |
|---|---|---|
| `--horizon` | `1y` | Forward-return horizon for IC computation (`1y`, `3y`, `5y`) |
| `--all-horizons` | False | Run IC analysis for 1y, 3y, and 5y horizons in sequence |
| `--top N` | `20` | Top N factors to display/save |
| `--features F [F...]` | (all) | Restrict analysis to named features |
| `--ic-decay` | False | Compute multi-lag IC decay and estimate signal half-life |
| `--decay-top N` | `20` | Number of top factors to include in IC decay analysis |
| `--decay-plot` | False | Plot IC decay curves (requires matplotlib) |

Output CSV columns: `ic` (mean IC), `mean_ic` (legacy alias), `icir`, `ic_tstat`, `pct_positive_ic`,
`n_years`, `turnover`, `q1_ret`, `q5_ret`, `q_spread`.

IC decay output (`reports/ic_decay_halflife.csv`) columns: `feature`, `ic_1y`, `ic_3y`, `ic_5y`,
`ic_decay_ratio_3y`, `halflife_yrs`.

---

### `leverage_strategy.py` — 3-Stage Long/Short Kelly Portfolio

Implements a 3-stage screener on top of the composite score:
- **Stage 1** — Hard fundamental gate: Piotroski ≥ 6, Beneish M < -1.78, Altman Z > 1.81, P/B < 5.0, market cap ≥ $50M
- **Stage 2** — Direction gate: `ml_score_3y > 0.52` (binary classifier P(beat_market))
- **Stage 3** — Magnitude ranker: sort survivors by `ml_pred_excess_3y` (Huber regression)
- **Stage 4** — Kelly position sizing proportional to `ml_pred_excess_3y`

Falls back to `composite_score` weighting when the regression model is unavailable.

```bash
python3 scripts/leverage_strategy.py
python3 scripts/leverage_strategy.py --market US --top-long 20
python3 scripts/leverage_strategy.py --market KR --top-long 15 --capital 10000
python3 scripts/leverage_strategy.py --long-only
python3 scripts/leverage_strategy.py --min-piotroski 7 --max-beneish -2.0
python3 scripts/leverage_strategy.py --output reports/leverage_picks.csv
```

| Flag | Default | Description |
|---|---|---|
| `--market` | `US` | Market code (US, KR, CA, ...) |
| `--top-long N` | `20` | Top N long candidates after 3-stage filter |
| `--top-short N` | `10` | Top N short candidates (Beneish-ranked) |
| `--capital FLOAT` | `10000` | Portfolio capital in EUR |
| `--long-only` | False | Suppress all short positions |
| `--min-piotroski N` | `6` | Piotroski F-Score threshold (Stage 1) |
| `--max-beneish FLOAT` | `-1.78` | Beneish M-Score upper bound (Stage 1) |
| `--output PATH` | `data/leverage_positions_<market>.csv` | Output CSV path |

**Outputs**: `data/leverage_positions_<market>.csv` (long book), `data/short_book_<market>.csv` (short book).

---

### `alpha/explain.py` — Plain-English Investment Thesis Generator

Converts quantitative screener output into a human-readable buy/sell rationale for each
stock. Reads the same parquet + models used by `leverage_strategy.py` and produces
structured plain-English sections: WHY BUY, Financial Quality (Piotroski), Fraud Risk
(Beneish), Distress Risk (Altman Z), Valuation, ML Signal, Recommended Trade, Risk Flags,
and a 4-point Margin of Safety checklist.

```bash
python3 alpha/explain.py --market US --top 15
python3 alpha/explain.py --market KR --top 10 --capital 20000
python3 alpha/explain.py --market US --output reports/thesis_us.txt
```

| Flag | Default | Description |
|---|---|---|
| `--market` | `US` | Market code (US, KR, CA, ...) |
| `--top N` | `15` | Top N long picks to explain |
| `--capital FLOAT` | `50000` | Portfolio capital used for position sizing context |
| `--output PATH` | None | Write text report to this file (stdout if omitted) |

**API usage**:
```python
from alpha.explain import explain_pick, explain_many
text   = explain_pick('APOG', df_row)      # single-ticker thesis
report = explain_many(positions_df, raw_df) # batch report
```

**Outputs**: Plain text to stdout (and optionally a `.txt` file). No parquet writes.

**Re-run when**: leverage picks change, models are retrained, or a new market is added.

---

## Reporting & Monitoring

### `generate_reports.py` — PDF Tearsheet + CSV Picks + Kelly Portfolio Page

```bash
python3 scripts/generate_reports.py
python3 scripts/generate_reports.py --top 25
python3 scripts/generate_reports.py --strategy composite --no-pdf
```

| Flag | Default | Description |
|---|---|---|
| `--strategy` | `composite` | Strategy to report on |
| `--top N` | `20` | Number of top picks to include |
| `--no-pdf` | False | Skip PDF generation (CSV only) |

**PDF pages** (in order):
1. Cover page
2. Strategy tearsheet (cumulative wealth, excess return, drawdown, rolling Sharpe, KPIs) — from `backtest_results.json`
3. Kelly portfolio tearsheet (cumulative wealth vs SPY, annual return bar, drawdown, KPI table with VaR/CVaR, top 10 holdings) — from `portfolio_backtest.json` + `portfolio_holdings.json`
4. OOS AUC bar chart per horizon — from `model_meta.json`
5. Top-N picks preview table

Outputs: `reports/tearsheet.pdf`, `reports/weekly_picks.csv`, `reports/rolling_oos_auc.png`

---

### `monitor_drift.py` — PSI + AUC Drift Monitoring + IC Decay + Drawdown Circuit Breaker

```bash
python3 scripts/monitor_drift.py
python3 scripts/monitor_drift.py --window 2024
python3 scripts/monitor_drift.py --psi-alert 0.20
python3 scripts/monitor_drift.py --auc-alert 0.05
python3 scripts/monitor_drift.py --dd-gate 20        # circuit breaker at 20% drawdown
python3 scripts/monitor_drift.py --skip-ic-decay     # skip IC decay step
python3 scripts/monitor_drift.py --skip-dd           # skip drawdown check
```

| Flag | Default | Description |
|---|---|---|
| `--window YEAR` | Latest year | Scoring window to compare against training distribution |
| `--psi-alert FLOAT` | `0.20` | PSI threshold that triggers alert (exit code 1) |
| `--auc-alert FLOAT` | `0.05` | AUC drop threshold that triggers alert |
| `--dd-gate FLOAT` | `20` | Drawdown circuit-breaker threshold in % — warns if current portfolio drawdown exceeds this |
| `--skip-ic-decay` | False | Skip per-alpha IC decay analysis |
| `--skip-dd` | False | Skip drawdown circuit-breaker check |

PSI interpretation: < 0.10 stable · 0.10–0.20 monitor · ≥ 0.20 alert

**IC decay analysis** (D5 addition): For each selected signal in `alpha_registry.json`, computes rolling Spearman IC (signal vs `forward_return_1y`) over the most recent 3y / 6y / 12y of fiscal-year data. Flags signals with 3y rolling IC < 0.02 (warn) or latest IC < 0 (alert). Decay messages are printed and included in `drift_report.json`.

**Drawdown circuit breaker** (D5 addition): Loads `data/portfolio_backtest.json` and computes current drawdown from the cumulative return peak. If drawdown exceeds `--dd-gate`, prints a circuit-breaker message (halve position sizing, no new positions until drawdown recovers below 10%) and sets `any_alert=true`.

Outputs: `reports/drift_report.json` (includes `ic_decay` and `drawdown` sections), `reports/drift_report.csv`
Exit code 1 if any alert fires (used by GitHub Actions to emit a warning).

---

### `analyze_distributions.py` — Dataset Distribution Analysis

```bash
python3 scripts/analyze_distributions.py
python3 scripts/analyze_distributions.py --parquet data/historical_dataset_clean.parquet --out-dir reports
python3 scripts/analyze_distributions.py --corr
```

| Flag | Default | Description |
|---|---|---|
| `--parquet PATH` | `data/historical_dataset_clean.parquet` | Input parquet path |
| `--out-dir DIR` | `reports` | Output directory |
| `--corr` | False | Also compute and save correlation matrix |

Non-fatal CI step for dataset quality monitoring. Produces:

- `reports/distribution_report.txt` — NaN% per column (sorted desc), top 20 columns by outlier rate (|z|>5), market fill rates for 10 key features, fraud label balance, rows per market and fiscal year range
- `reports/correlation_matrix.parquet` (with `--corr`) — pairwise Pearson correlation matrix for all numeric columns with >1000 non-null values; also prints high-correlation pairs (|r|>0.95) to stdout

---

### `bias_audit.py` — Bias Audit Suite (Look-Ahead / Survivorship / Overfitting / Regression)

Runs five bias checks against the dataset and models:

1. **Look-ahead bias** — verifies `filed_date >= period_end_date` for all rows. HARD FAIL in CI.
2. **Survivorship bias** — checks % of training rows from later-delisted companies (warn if < 5%).
3. **Overfitting audit** — compares `val_auc` vs walk-forward mean AUC per horizon. Writes `overfit_gap` to `model_meta.json`. Warn if gap > 0.15.
4. **Multiple testing** — documents Bonferroni correction across 5 horizons × 4 strategies.
5. **Regression model audit** — three checks for `model_3y_regression.joblib`:
   - Feature contamination scan: fails if any feature in `_REGRESSION_CONTAMINATED` is in the model's feature set (ML scores, forward returns, composite scores)
   - Walk-forward IC distribution: reads `reports/regression_ic_3y.csv` (`spearman_ic` column); warns if WF IC mean > 0.30 (suspiciously high)
   - Permutation test (50 shuffles): shuffles target labels, re-scores, checks IC degrades to ~0; genuine signal confirmed if permutation IC ≪ observed IC (z-score reported)

```bash
python3 scripts/bias_audit.py              # full report, exit 0
python3 scripts/bias_audit.py --ci         # exit 1 if look-ahead violations (CI mode)
python3 scripts/bias_audit.py --fix        # also compute FX-adjusted return columns
```

| Flag | Default | Description |
|---|---|---|
| `--ci` | off | Exit 1 if look-ahead violations found; survivorship/overfitting are warn-only |
| `--fix` | off | Compute and write `forward_return_{h}_usd` FX-adjusted columns to parquet |
| `--out PATH` | in-place | Output parquet path when `--fix` is set |

---

## Data Utilities

### `push_to_hf.py` — Upload to HuggingFace Hub

Uploads `data/historical_dataset_clean.parquet` and model artifacts to a HuggingFace Hub dataset repository.
Requires `HF_TOKEN` environment variable (or `~/.huggingface/token`).
Uploads models for all 5 horizons: `6m`, `1y`, `2y`, `3y`, `5y`.

```bash
# Upload both dataset and models
python3 scripts/push_to_hf.py --repo your-username/stock-screener-data

# Dataset only
python3 scripts/push_to_hf.py --repo your-username/stock-screener-data --data-only

# Models only
python3 scripts/push_to_hf.py --repo your-username/stock-screener-data --models-only

# Make repository public
python3 scripts/push_to_hf.py --repo your-username/stock-screener-data --public
```

| Flag | Default | Description |
|---|---|---|
| `--repo` | required | HuggingFace repo ID: `username/repo-name` |
| `--data-only` | false | Only upload `historical_dataset_clean.parquet` |
| `--models-only` | false | Only upload model `.joblib` files + `model_meta.json` |
| `--public` | false | Create repo as public (default: private) |
| `--message` | auto | HuggingFace commit message |

---

### `fetch_aaer_labels.py` — AAER Fraud Label Construction

Builds `data/aaer_labels.csv` and updates the `fraud_confirmed` column in
`data/historical_dataset_clean.parquet`.  No API key required — uses:

1. `data/aaer_cache.json` — pre-fetched AAER CIK/year pairs (232 unique CIKs)
2. SEC EDGAR full-text search — paginates through 10-K filings that disclose
   `"SEC investigation" AND "restatement"` or `"accounting fraud" AND "restatement"`
   as a proxy for confirmed accounting enforcement actions

Labeling window: `fraud_confirmed = 1` when the company has an AAER entry AND
`fiscal_year ∈ [fraud_year_start − lookback, fraud_year_end]` (default lookback = 2).

```bash
python3 scripts/fetch_aaer_labels.py                     # fetch + update parquet
python3 scripts/fetch_aaer_labels.py --dry-run           # preview without writing
python3 scripts/fetch_aaer_labels.py --no-update-parquet # CSV only
python3 scripts/fetch_aaer_labels.py --lookback 3        # wider lookback window
```

| Flag | Default | Description |
|---|---|---|
| `--lookback N` | `2` | Years before `fraud_year_start` to include as positive |
| `--start-year YEAR` | `2000` | Earliest year for EDGAR full-text search |
| `--end-year YEAR` | `2024` | Latest year for EDGAR full-text search |
| `--labels-output PATH` | `data/aaer_labels.csv` | Output CSV for per-company fraud ranges |
| `--parquet PATH` | `data/historical_dataset_clean.parquet` | Parquet to update |
| `--no-update-parquet` | False | Build CSV only; skip parquet write |
| `--dry-run` | False | Print coverage report without writing any files |

Output files:
- `data/aaer_labels.csv` — columns: `cik, ticker, name, fraud_year_start, fraud_year_end, n_filings, sources`
- `data/historical_dataset_clean.parquet` — `fraud_confirmed` column updated

Coverage (default settings): ~490 annual positive rows from ~120 companies (2009–2024).

---

### `check_data.py` — Dataset Health Check

```bash
python3 scripts/check_data.py
```

Prints row counts, null rates, feature coverage, and date ranges for the current parquet file.

---

### `refresh_data.py` — Incremental Data Refresh

Used by the GitHub Actions workflow. Downloads existing dataset, runs pipeline for new data, uploads back to HuggingFace.

```bash
python3 scripts/refresh_data.py --markets US
```

### `merge_snapshots.py` — Merge Raw Snapshots

Merges per-market snapshot parquet files (US, KR, EU, BR, JP, CA) into a single `snapshots_combined.parquet`. Deduplicates on `(cik, market, filed_date, period_type)` — the `market` key prevents cross-market CIK collisions. Usually called automatically by `wait_and_merge.py` after all market pipelines finish.

```bash
python3 scripts/merge_snapshots.py                        # merge only
python3 scripts/merge_snapshots.py --activate             # also overwrite snapshots.parquet
python3 scripts/merge_snapshots.py --activate --backup    # activate with backup
```

| Flag | Default | Description |
|---|---|---|
| `--activate` | off | Copy combined file to `data/snapshots.parquet` (pipeline default input) |
| `--backup` | off | Save existing `snapshots.parquet` as `snapshots.parquet.bak` before overwrite |

### `clean_dataset.py` — Normalize and Clean

Applies normalization, outlier clipping, and schema enforcement. Called automatically by `run_pipeline.py` step 3.

### `enrich_sectors_dividends.py` — Add Sector + Dividend Data

Fetches sector classifications and dividend history. Called automatically by `run_pipeline.py` step 5.

---

### `enrich_quarterly_features.py` — Intra-Year Feature Enrichment

Computes 5 quarterly-derived features from Q1/Q2/Q3 rows and left-joins them onto annual training rows. Corrects a data gap where intra-year dynamics are invisible in annual filings.

```bash
python3 scripts/enrich_quarterly_features.py           # dry-run: prints coverage stats
python3 scripts/enrich_quarterly_features.py --fix     # writes parquet in-place
python3 scripts/enrich_quarterly_features.py --fix --out data/historical_dataset_enriched.parquet
```

| Flag | Default | Description |
|---|---|---|
| `--fix` | False | Write enriched parquet (default is dry-run) |
| `--out PATH` | input path | Output path; defaults to overwriting the input |

Features added:

| Column | Description |
|---|---|
| `revenue_qoq_std_norm` | Std of Q1→Q2→Q3 revenue growth (earnings smoothing proxy) |
| `earnings_qoq_mean` | Mean QoQ net income growth (earnings momentum) |
| `max_accruals_ttm` | Max \|wc_accruals_to_assets\| across available quarters |
| `revenue_acceleration` | Q3/Q1 revenue ratio (intra-year sales ramp) |
| `quarterly_positive_rev_frac` | Fraction of quarters with positive QoQ revenue growth |

Coverage: 74.8% of annual rows enriched (requires at least 2 quarterly rows per ticker/year).

---

### `mark_survivorship.py` — Survivorship Bias Correction

Identifies likely-delisted companies (no filing in the last N years) and imputes a pessimistic −50% forward return to correct survivorship bias in the training data.

```bash
python3 scripts/mark_survivorship.py                   # report only (dry-run)
python3 scripts/mark_survivorship.py --fix             # write corrected parquet
python3 scripts/mark_survivorship.py --fix --lag 3     # custom lag threshold
python3 scripts/mark_survivorship.py --fix --out data/historical_survivorship.parquet
```

| Flag | Default | Description |
|---|---|---|
| `--fix` | False | Write corrected parquet (default is dry-run) |
| `--lag N` | `3` | Years of filing silence before marking as likely-delisted |
| `--out PATH` | input path | Output path; defaults to overwriting the input |

Adds a `likely_delisted` boolean column and imputes `forward_return_{1y,3y,5y} = −0.50` for final annual rows of delisted companies.

---

### `migrate_to_db.py` — Load Dataset into TimescaleDB

Bulk-loads `historical_dataset_clean.parquet` into a TimescaleDB hypertable for time-series queries. Schema is defined in `infra/db/init.sql`.

```bash
python3 scripts/migrate_to_db.py
python3 scripts/migrate_to_db.py --parquet data/historical_dataset_clean.parquet
python3 scripts/migrate_to_db.py --truncate   # wipe existing rows before loading
```

| Flag | Default | Description |
|---|---|---|
| `--parquet PATH` | `data/historical_dataset_clean.parquet` | Source parquet file |
| `--truncate` | False | Truncate target table before inserting |

Requires `DATABASE_URL` environment variable pointing to a live TimescaleDB instance.

---

### `pit_validate.py` — Point-in-Time Look-Ahead Audit

Validates that the historical dataset respects point-in-time data availability. Checks filing lag distributions, portfolio formation dates, sector percentile look-ahead exposure, and ML training look-ahead fractions. Run this after any full pipeline rebuild and before training.

```bash
python3 scripts/pit_validate.py                           # full report to stdout
python3 scripts/pit_validate.py --market US               # US only
python3 scripts/pit_validate.py --output data/pit_report.csv
```

| Flag | Default | Description |
|---|---|---|
| `--market` | All | Filter to one market (e.g. `US`) |
| `--output PATH` | stdout | Write CSV report to file |

Checks performed:
1. Filing lag distribution (months: fiscal_year_end → filed_date)
2. Portfolio formation date (quarter in which each 10-K was filed)
3. Sector percentile look-ahead (% of sector peers not yet filed at filing date)
4. ML training look-ahead (fraction of training rows with filed_date > Jan 1 of year)
5. Forward-return anchor (entry_price date == filed_date)

---

### `fix_dataset_quality.py` — Dataset Quality Fixes

Applies one-time quality fixes after a full pipeline rebuild, before training. Run once after `run_pipeline.py` completes.

```bash
python3 scripts/fix_dataset_quality.py                     # in-place fix
python3 scripts/fix_dataset_quality.py --dry-run           # report only, no write
python3 scripts/fix_dataset_quality.py --src custom.parquet
python3 scripts/fix_dataset_quality.py --out fixed.parquet
```

| Flag | Default | Description |
|---|---|---|
| `--dry-run` | False | Report issues without writing any files |
| `--src PATH` | `data/historical_dataset_clean.parquet` | Source parquet |
| `--out PATH` | input path | Output path (overwrites input if not specified) |

Fixes applied:
1. Drop columns with 100% null rate across the whole dataset
2. Add `is_forecast` flag — True for fiscal_year ≥ FORECAST_YEAR
3. Winsorize `accruals_to_assets` at 1st/99th percentile per (market, fiscal_year)
4. Fix `gross_margin` values > 1.5 — divides by 100 (corrects percentage-format entries)

---

### `test_dataset_quality.py` — Dataset Quality Test Suite

98-check automated test suite for `data/historical_dataset_clean.parquet`. 10 sections: schema, structural, market coverage, fill rates, distributions, fraud labels, forward returns (winsorization), growth winsorization, ML score exclusion, point-in-time leakage. Run after any dataset modification.

```bash
python3 scripts/test_dataset_quality.py              # show failures/warnings only
python3 scripts/test_dataset_quality.py --verbose    # print all 98 checks
python3 scripts/test_dataset_quality.py --parquet PATH  # custom file
```

| Flag | Default | Description |
|---|---|---|
| `--verbose` | False | Print all checks, not just failures |
| `--parquet PATH` | `data/historical_dataset_clean.parquet` | Path to parquet |

Sections:
1. **Schema** — required columns present, fiscal_year dtype
2. **Structural** — annual-only, no blank tickers, no duplicate PKs, no inf values
3. **Market coverage** — minimum ticker count and year span per market
4. **Fill rates** — core financial columns above minimum thresholds
5. **Distribution sanity** — fraud scores in [0,1], Piotroski in [0,9], in_universe binary
6. **Fraud label integrity** — leakage check, fraud_suspect consistency
7. **Forward return coverage + winsorization** — ≥15% fill per market; max absolute value must not exceed hard caps (1y: 5.0, 3y: 10.0, 5y: 20.0)
8. **Growth feature winsorization** (Rule 6) — any growth column with max > 50 × p99 fails
9. **ML score exclusion** (Rule 7) — `ml_1y/3y/5y` must not appear in `models/feature_sets_*.json`

---

### `build_fraud_labels.py` — Multi-Source Fraud Label System

Builds `data/fraud_labels.parquet` from three free public sources: SEC AAER releases, SEC EDGAR bankruptcy filings (Form 15/BK), and Stanford Securities Class Action Clearinghouse (SCAC). Use this to bootstrap or extend the fraud label set beyond what `fetch_aaer_labels.py` covers.

```bash
python3 scripts/build_fraud_labels.py                      # all sources
python3 scripts/build_fraud_labels.py --sources aaer scac  # specific sources only
python3 scripts/build_fraud_labels.py --output data/fraud_labels_2024.parquet
```

| Flag | Default | Description |
|---|---|---|
| `--sources` | `aaer scac bk` | Space-separated list of sources to include |
| `--output PATH` | `data/fraud_labels.parquet` | Output path |

Output columns: `ticker, market, fraud_year, label_type, source, description, fraud_confirmed, fraud_suspect, cik`

!!! note
    For routine AAER label updates, prefer `fetch_aaer_labels.py` which also updates the main parquet in-place. `build_fraud_labels.py` is for building the standalone label file from multiple sources.

---

### `wait_and_merge.py` — Pipeline Completion Monitor + Auto-Merge

Polls until all market pipelines (KR, EU, CA) have completed and their snapshot files have stabilised, then automatically runs the merge and feature engineering steps. Leave this running in a terminal when running multi-market pipelines in parallel.

```bash
python3 scripts/wait_and_merge.py
```

No flags. Behaviour:
1. Polls for `data/snapshots_kr.parquet`, `data/snapshots_eu.parquet`, `data/snapshots_ca.parquet`
2. Waits for file sizes to stabilise (no change over two poll cycles)
3. Runs `scripts/merge_snapshots.py --activate --backup`
4. Runs `python3 scripts/run_pipeline.py build --step 4` (features + clean on full dataset)

---

## Process Automation

### `check_sync.py` — Architecture Sync Checker

Reads git-staged (or specified) files and checks them against the `CLAUDE.md` Change Checklist rules. Reports missing doc/diagram updates. Called automatically by the pre-commit hook.

```bash
python3 scripts/check_sync.py                      # check staged files (same as pre-commit hook)
python3 scripts/check_sync.py --all-changed        # check all uncommitted files
python3 scripts/check_sync.py --warn-only          # report violations but don't block
python3 scripts/check_sync.py --files a.py b.py    # check a specific list of files
```

| Flag | Default | Description |
|---|---|---|
| `--all-changed` | False | Include unstaged + untracked files in check |
| `--warn-only` | False | Print warnings but always exit 0 (non-blocking) |
| `--files FILE…` | None | Check a specific list of files instead of git state |

**Exit codes**: `0` = all rules satisfied, `1` = sync violations found.

The pre-commit hook at `.git/hooks/pre-commit` calls this automatically before every `git commit`. To bypass in emergencies: `git commit --no-verify`.

**Rules now cover**: scripts/ changes → scripts.md; step5 columns → architecture.md + data-update-guide.md; test_dataset_quality.py changes → data-update-guide.md + phase-done-criteria.md; run_feature_selection.py / feature_sets_*.json changes → index.md + scripts.md + feature-selection.md; refresh_data.yml changes → data-update-guide.md.

---

### `verify_doc_consistency.py` — Cross-File Fact Verifier

Reads the live parquet and key docs, then checks that column counts, row counts, feature counts,
and quality check counts are consistent across all files. Also checks Phase C artifacts (model_meta.json
horizon coverage, spy_returns.csv, horizon_router.py presence). Run before Phase gate checks or after
any dataset change.

```bash
python3 scripts/verify_doc_consistency.py           # fail if any mismatch
python3 scripts/verify_doc_consistency.py --warn    # print mismatches, exit 0
```

| Flag | Default | Description |
|---|---|---|
| `--warn` | False | Print failures but always exit 0 (used in CI as advisory) |

**Checks**: column count (360) in index.md, README.md, architecture.md, models.md, CLAUDE.md, phase-done-criteria.md, data-update-guide.md; feature counts (all 5 horizons) in scripts.md, feature-selection.md; row count (58,190) in index.md; quality check count (98) in data-update-guide.md; Phase C: model_meta.json horizons, spy_returns.csv, horizon_router.py.

**In CI**: runs weekly after `run_feature_selection.py` as a non-blocking advisory step. Output appears in the GitHub Actions log.

---

### `run_phase_checks.py` — Phase Done Verifier (Anti-Drift)

Single command to mechanically verify all exit criteria from `docs/developer/phase-done-criteria.md`. A phase is only done when this script prints **all PASS**. Run this before declaring any phase complete — human judgment about "I think it's done" is not sufficient.

The A4 diagram-vs-CI check maintains an `operator_only` allowlist of scripts that appear in `data-update-guide.md` but are intentionally absent from `refresh_data.yml` (e.g. operator utilities, modules, or scripts that run in a separate workflow). Update this allowlist in `check_a4_diagram_vs_ci()` if a new operator-only script is added to the guide.

```bash
python3 scripts/run_phase_checks.py          # run Phase A + B checks (default)
python3 scripts/run_phase_checks.py --phase A   # Phase A only
python3 scripts/run_phase_checks.py --phase B   # Phase B only
python3 scripts/run_phase_checks.py --phase C   # Phase C only
python3 scripts/run_phase_checks.py --phase AB  # Phase A + B (same as default)
python3 scripts/run_phase_checks.py --strict    # exit 1 on WARN too (CI mode)
```

| Flag | Default | Description |
|---|---|---|
| `--phase A\|B\|C\|AB` | `AB` | Which phase(s) to check |
| `--strict` | False | Exit 1 on WARN in addition to FAIL |

**Exit codes**: `0` = all checks PASS (±WARN), `1` = any FAIL (or WARN in strict mode).

**Output**: Each check prints one line: `[PASS]`, `[FAIL]`, `[WARN]`, or `[SKIP]` with a detail message. Summary at end:

```
SUMMARY: 14 PASS  0 FAIL  1 WARN  2 SKIP
```

#### Phase A checks

| Check | What it verifies |
|---|---|
| A1 — Dataset shape | ≥58,000 rows × 360 cols; no inf; forward returns winsorized; 5 markets present |
| A2 — EDA notebook | `notebooks/01_*.ipynb` has forward_return histogram, outlier stats, PIT lineage, null profile |
| A3 — CI schedule | `refresh_data.yml` has all 6 required scripts |
| A4 — Diagram vs CI | Core scripts appear in both `data-update-guide.md` and `refresh_data.yml` |

#### Phase B checks

| Check | What it verifies |
|---|---|
| B1 — Feature library | 7 key formula families implemented + columns present in parquet |
| B2 — Feature engineering | DSRI clipped, growth cols winsorized, `sector_pct` ranks within `fiscal_year`, `montier_c2` uses `ppe_net` |
| B3 — Feature selection | No `alpha_*/ml_*` in feature sets; PSI threshold 0.25; NW+FDR columns in selection summary |
| B4 — Factor research | Factor research CSVs have IC/ICIR columns; notebook 02 has decay/quintile/IR plots |
| B5 — Notebook outputs | ≥50% cells have outputs in each notebook |

#### Phase C checks

| Check | What it verifies |
|---|---|
| C1 — OOF scores | `ml_1y_oof`, `ml_3y_oof`, `ml_5y_oof` present in parquet with <90% null |
| C2 — Models | `model_meta.json` covers 5 horizons; WF AUC ≥ targets |
| C4 — Backtest | `data/backtest_results.json` present with alpha/benchmark keys |
| C5 — Alpha schema | `alpha/signals/` directory exists with ≥1 signal JSON |

!!! important "Anti-drift rule"
    This script IS the phase gate. Do not use any other method to declare a phase done. If any check FAILs, fix only that item and re-run. Do not re-audit the whole phase from scratch.
