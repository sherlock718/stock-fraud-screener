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
`data/historical_dataset_clean.parquet`. Missing features are filled with per-horizon
`train_medians` stored in model_meta.json.

```bash
python3 scripts/score_historical.py                  # Score and write parquet
python3 scripts/score_historical.py --dry-run        # Score only, print stats, no write
python3 scripts/score_historical.py --parquet PATH   # Use alternate parquet path
```

| Flag | Default | Description |
|---|---|---|
| `--parquet` | `data/historical_dataset_clean.parquet` | Dataset path |
| `--models-dir` | `models/` | Directory with model_*.joblib + model_meta.json |
| `--dry-run` | off | Print score distribution but do not write parquet |

**Outputs**: Updates `data/historical_dataset_clean.parquet` in-place (326 → 329 columns).
After running, `ml_1y`, `ml_3y`, `ml_5y` are available for the backtester and alpha factor package.

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

Runs the full PSI → IC → ICIR → Spearman deduplication pipeline across all three horizons and writes `models/feature_sets_{1y,3y,5y}.json`.

```bash
python3 scripts/run_feature_selection.py               # Full run, writes JSON files
python3 scripts/run_feature_selection.py --dry-run     # Print stats only, no files written
python3 scripts/run_feature_selection.py --psi-threshold 0.20 --ic-min 0.03
```

| Flag | Default | Description |
|---|---|---|
| `--psi-threshold FLOAT` | `2.0` | Drop candidates with PSI above this (train vs test split) |
| `--ic-min FLOAT` | `0.02` | Minimum \|mean IC\| to pass the IC screen |
| `--top-k INT` | `60` | Keep top-K features by \|ICIR\| before deduplication |
| `--corr FLOAT` | `0.90` | Spearman \|r\| threshold for near-duplicate removal |
| `--dry-run` | off | Print coverage stats but do not write files |

**Outputs**:
- `models/feature_sets_{1y,3y,5y}.json` — selected feature list per horizon (~45 features each)
- `reports/feature_selection_summary.csv` — IC, ICIR, PSI, and selection status for all candidates

---

### `train_models.py` — LightGBM Training

Trains three LightGBM models (1y, 3y, 5y horizons) using ICIR feature selection.

```bash
python3 scripts/train_models.py
python3 scripts/train_models.py --top-n 50
python3 scripts/train_models.py --train-cutoff 2017 --val-end 2019
python3 scripts/train_models.py --no-shap
```

| Flag | Default | Description |
|---|---|---|
| `--top-n N` | `40` | Max features per horizon after ICIR ranking |
| `--min-ic FLOAT` | `0.02` | Minimum absolute IC to include a feature |
| `--max-psi FLOAT` | `2.0` | Drop features with PSI above this threshold before IC ranking |
| `--min-ic-stability FLOAT` | `0.0` | Minimum fraction of years IC must have the correct sign (0.0 = off). Set to e.g. `0.6` to drop directionally inconsistent features |
| `--min-ic-years INT` | `1` | Minimum years of IC data required to keep a feature (1 = off). Set to e.g. `5` to prevent ICIR inflation from features with very few historical observations |
| `--no-dedup` | False | Skip correlation deduplication (r > 0.90) |
| `--sector-neutral` | False | Demean IC scores within sectors before ranking |
| `--train-cutoff YEAR` | `2017` | Last training year (inclusive) |
| `--val-end YEAR` | `2019` | Last validation year (inclusive); test = after this |
| `--no-shap` | False | Skip SHAP computation (faster) |
| `--walk-forward` | False | Run expanding-window walk-forward CV; saves `reports/walk_forward_auc_{h}.csv` |

Outputs: `models/model_{1y,3y,5y}.joblib`, `models/model_meta.json`

The PSI filter (`--max-psi`) runs **before** IC ranking and removes features with high Population Stability Index between training and scoring distributions. This prevents macro-regime features (treasury rates, CPI, yield curve) from inflating ICIR scores on stale patterns. Default threshold of 2.0 removes ~10 macro features.

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
| `--horizon` | All | One of `1y`, `3y`, `5y`; omit to tune all |
| `--trials N` | `50` | Number of Optuna trials per horizon |
| `--no-catboost` | False | Skip CatBoost training; LightGBM only |

Soft ensemble output: `0.5 × lgbm_proba + 0.5 × catboost_proba`

---

## Backtesting & Research

### `backtester.py` — Walk-Forward Backtester

```bash
python3 scripts/backtester.py
python3 scripts/backtester.py --strategy composite
python3 scripts/backtester.py --top 25 --cost 50
python3 scripts/backtester.py --market US --tearsheet
```

| Flag | Default | Description |
|---|---|---|
| `--strategy` | `all` | `composite`, `1y`, `3y`, `5y`, `ensemble`, `all` |
| `--market` | None | Filter to one market, e.g. `US` |
| `--top N` | `20` | Top N picks per year |
| `--cost BPS` | `30` | Round-trip transaction cost in basis points |
| `--smallcap-cost BPS` | `60` | Extra cost for small-cap positions (< $300M) |
| `--min-cap N` | `50000000` | Minimum market cap filter (USD) |
| `--equal-weight` | False | Equal-weight instead of rank-weight |
| `--fill-missing FLOAT` | None | Fill missing returns with this value |
| `--max-filing-lag N` | `6` | Max months lag between fiscal year-end and filing |
| `--tearsheet` | False | Generate rolling OOS AUC plot |

Output: `data/backtest_results.json`, `reports/rolling_oos_auc.png`

---

### `factor_research.py` — IC / ICIR Analysis

```bash
python3 scripts/factor_research.py
python3 scripts/factor_research.py --features gross_margin roe accruals_to_assets
python3 scripts/factor_research.py --decay-plot
```

Computes IC, ICIR, and factor decay curves for all features. Used to select features for model training.

---

### `leverage_strategy.py` — Long/Short Kelly Portfolio

```bash
python3 scripts/leverage_strategy.py
python3 scripts/leverage_strategy.py --top-long 15 --top-short 8
python3 scripts/leverage_strategy.py --long-only
python3 scripts/leverage_strategy.py --min-piotroski 7 --max-beneish -2.0
python3 scripts/leverage_strategy.py --output reports/leverage_picks.csv
```

| Flag | Default | Description |
|---|---|---|
| `--top-long N` | `10` | Top N long candidates |
| `--top-short N` | `5` | Top N short candidates |
| `--long-only` | False | Suppress all short positions |
| `--min-piotroski N` | `6` | Piotroski F-Score threshold for longs |
| `--max-beneish FLOAT` | `-1.78` | Beneish M-Score upper bound for longs |
| `--output PATH` | `reports/leverage_picks.csv` | Output CSV path |

---

## Reporting & Monitoring

### `generate_reports.py` — PDF Tearsheet + CSV Picks

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

Outputs: `reports/tearsheet.pdf`, `reports/weekly_picks.csv`

---

### `monitor_drift.py` — PSI + AUC Drift Monitoring

```bash
python3 scripts/monitor_drift.py
python3 scripts/monitor_drift.py --window 2024
python3 scripts/monitor_drift.py --psi-alert 0.20
python3 scripts/monitor_drift.py --auc-alert 0.05
```

| Flag | Default | Description |
|---|---|---|
| `--window YEAR` | Latest year | Scoring window to compare against training distribution |
| `--psi-alert FLOAT` | `0.20` | PSI threshold that triggers alert (exit code 1) |
| `--auc-alert FLOAT` | `0.05` | AUC drop threshold that triggers alert |

PSI interpretation: < 0.10 stable · 0.10–0.20 monitor · ≥ 0.20 alert

Outputs: `reports/drift_report.json`, `reports/drift_report.csv`
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

### `bias_audit.py` — Look-Ahead / Survivorship Audit

```bash
python3 scripts/bias_audit.py
```

Runs four tests: temporal leakage, shuffle test, feature-return correlation, permutation importance stability.
Output: `reports/bias_audit_report.json`

---

## Data Utilities

### `push_to_hf.py` — Upload to HuggingFace Hub

Uploads `data/historical_dataset_clean.parquet` and model artifacts to a HuggingFace Hub dataset repository.
Requires `HF_TOKEN` environment variable (or `~/.huggingface/token`).

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

92-check automated test suite for `data/historical_dataset_clean.parquet`. Run after any dataset modification to verify integrity before push.

```bash
python3 scripts/test_dataset_quality.py              # show failures/warnings only
python3 scripts/test_dataset_quality.py --verbose    # print all 92 checks
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
7. **Forward return coverage** — ≥15% fill per market
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
