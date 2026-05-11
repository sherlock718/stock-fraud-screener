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
4. Engineer 278 features (`feature_library.py`)
5. Enrich with price data and sector/dividend data
6. Write `data/historical_dataset_clean.parquet`

---

### `run_pipeline_eu.py` — EU Pipeline (SimFin)

```bash
export SIMFIN_API_KEY=your_key
python3 scripts/run_pipeline_eu.py
```

Requires a SimFin API key. Outputs EU company snapshots in the same schema as the US pipeline.

---

### `run_pipeline_kr.py` — Korea Pipeline (DART)

```bash
export DART_API_KEY=your_key
python3 scripts/run_pipeline_kr.py
```

Requires a DART (FSS) API key. Outputs Korean company snapshots.

---

## Model Scripts

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

### `bias_audit.py` — Look-Ahead / Survivorship Audit

```bash
python3 scripts/bias_audit.py
```

Runs four tests: temporal leakage, shuffle test, feature-return correlation, permutation importance stability.
Output: `reports/bias_audit_report.json`

---

## Data Utilities

### `push_to_hf.py` — Upload to HuggingFace Hub

```bash
python3 scripts/push_to_hf.py
```

Uploads `data/historical_dataset_clean.parquet`, `data/refresh_status.json`, and model files to HuggingFace Hub.
Requires `HF_TOKEN` and `HF_REPO` environment variables.

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

Merges incremental snapshot files into a single consolidated parquet. Usually called automatically by `run_pipeline.py` step 2.

### `clean_dataset.py` — Normalize and Clean

Applies normalization, outlier clipping, and schema enforcement. Called automatically by `run_pipeline.py` step 3.

### `enrich_sectors_dividends.py` — Add Sector + Dividend Data

Fetches sector classifications and dividend history. Called automatically by `run_pipeline.py` step 5.

### `high_roi_strategies.py` — Strategy Comparison

Computes and compares all four strategy variants (COMPOSITE, 1Y, 3Y, 5Y) side-by-side. Useful for strategy selection research.

### `watchlist.py` — Watchlist Export

Exports the current watchlist from the app's session state to CSV for external use.

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
