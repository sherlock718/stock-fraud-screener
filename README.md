# Multi-Factor Stock Screener

A research-grade quantitative alpha generation platform covering 14 markets — US, Korea, Canada, Japan, Brazil, and 9 European markets. Built on SEC EDGAR, SEDAR+, B3, TDNET, SimFin, and DART.

## What It Does

- **Scores** companies across 5 factor groups: Value, Quality, Momentum, Growth, and Fraud Risk — combined into a composite alpha score
- **Trains** LightGBM models on 1y/3y/5y return horizons using 58,190 company-year observations and 355 columns
- **Backtests** 4 strategies (COMPOSITE, QEM, SCDV, IARB) with walk-forward ML retraining to avoid look-ahead bias
- **Flags fraud risk** as one factor — Beneish M-Score, Ohlson O-Score, Altman Z-Score, Piotroski F-Score, AAER-confirmed labels, and ML fraud probability
- **Ranks factors** by IC/ICIR across 1y/3y/5y horizons for systematic factor research

---

## Architecture

```
pipeline/               Raw data ingestion ONLY — fetch, snapshot, enrich, merge
├── step1_fetch_tickers*.py     Ticker discovery per market (US/EU/KR/JP/CA/BR)
├── step2_build_snapshots*.py   Build annual financial snapshots from raw filings
├── step3_enrich_kpis.py        Compute derived KPIs and features (200+)
├── feature_library.py          Shared feature engineering — single source of truth
├── build_historical_dataset.py Merge all market snapshots into one parquet
└── fraud_signals.py            Beneish/Ohlson/Altman/Piotroski scoring

scripts/                Analysis, ML, reporting — consumes output of pipeline/
├── train_models.py             Train LightGBM models (1y/3y/5y) with ICIR selection; alpha_* and ml_* excluded
├── tune_models.py              Optuna hyperparameter search + CatBoost ensemble
├── backtester.py               Walk-forward backtest engine
├── factor_research.py          IC/ICIR/turnover analysis
├── leverage_strategy.py        Kelly-sized long/short portfolio
├── bias_audit.py               Survivorship/leakage/FX bias checks
├── monitor_drift.py            PSI + rolling AUC drift monitoring
├── generate_reports.py         PDF tearsheet + CSV picks
├── push_to_hf.py               Upload dataset + models to HuggingFace Hub
└── refresh_data.py             Full refresh orchestrator (called by GitHub Actions)

models/
├── model_{1y,3y,5y}.joblib    Trained LightGBM classifiers
└── model_meta.json            Feature lists + OOS AUC

data/
├── historical_dataset_clean.parquet  148MB — main training/scoring data
├── snapshots_combined.parquet        Multi-market annual snapshots
├── prices_combined.parquet           Price/return data
└── backtest_results.json             Latest backtest output (consumed by app)

app_v2.py                       Streamlit dashboard (cloud-safe: loads from HF Hub when HF_REPO is set)

tests/
└── test_pipeline.py            Pytest suite: temporal split, feature leakage, IC table, bias audit, scoring

.github/workflows/
├── refresh_data.yml    Weekly data refresh (Sun 05:00 UTC) — full pipeline + HF upload
└── monitor_drift.yml   Weekly drift check (Mon 07:00 UTC) — PSI + AUC alerts
```

---

## ML Models

5 discrete horizon models (6m/1y/2y/3y/5y) — user selects investment horizon → HorizonRouter maps to nearest trained model.

| Horizon | Features | Val AUC | Tuned Val AUC | WF Mean AUC | Target |
|---------|----------|---------|---------------|-------------|--------|
| 6m      | 31       | 0.607   | **0.617**     | 0.5715      | ≥ 0.58 ❌ |
| 1y      | 31       | 0.599   | **0.605**     | **0.5774**  | ≥ 0.62 ❌ |
| 2y      | 28       | 0.585   | **0.606**     | 0.5880      | ≥ 0.60 ❌ |
| 3y      | 30       | 0.635   | **0.6644**    | 0.6248      | ≥ 0.62 ✅ |
| 5y      | 26       | —       | —             | 0.6200      | ≥ 0.62 ✅ |

**Regression model (3y magnitude):** LightGBM Huber regressor predicting `excess_return_local_3y`; WF Spearman IC = 0.34 (9 folds). Used as Stage 3 ranker in leverage strategy screener.

- Target: beat local market index over horizon
- Feature selection: BH FDR gate + top features by |ICIR|, deduplicated at |Spearman| > 0.85
- PIT-safe splits: `filed_date` + `fiscal_year` cutoff to eliminate look-ahead from late SEC filings
- OOF scores (`ml_{h}_oof`): `generate_oof_scores.py` produces true out-of-sample scores

---

## Backtesting

Walk-forward engine: for each year Y, retrain on data ≤ Y-1, then score and pick stocks for year Y.

**Current results (US, top 20, 30bps cost, $50M market cap floor, inverse-vol weighting):**

| Strategy  | CAGR   | Bench  | Excess | Sharpe | Calmar | MaxDD  | Hit Rate |
|-----------|--------|--------|--------|--------|--------|--------|----------|
| COMPOSITE | +25.0% | +11.9% | +13.1% | 1.327  | —¹     | 0.0%   | 75%      |
| QEM       | +14.9% | +11.1% | +3.9%  | 0.943  | 1.67   | -8.9%  | 65%      |
| SCDV      | +18.1% | +11.1% | +7.1%  | 1.071  | —¹     | -0.2%  | 65%      |
| IARB      | —      | —      | —      | —      | —      | —      | Insufficient data (non-US) |

¹ Sortino/Calmar suppressed when < 3 negative years or MaxDD < 2% — not enough data for reliable estimate.

Portfolio improvements applied:
- **$50M market cap floor** — removes truly illiquid stocks (no ADTV data available)
- **Inverse-volatility weighting** — `w_i = (1/σ_i) / Σ(1/σ_j)`, capped at 20% per position

```bash
# Run full backtest
python3 scripts/backtester.py --strategy all --market US

# Compare equal-weight vs vol-weighted
python3 scripts/backtester.py --strategy composite --market US --equal-weight

# Disable liquidity filter
python3 scripts/backtester.py --strategy composite --market US --min-cap 0
```

---

## Strategies

**COMPOSITE** — Blend of value, quality, ML(1y+3y), and Piotroski. Beneish filter applied. Best all-weather strategy.

**QEM (Quality + Earnings + Momentum)** — Piotroski ≥ 7, positive EPS growth, momentum > -10%. Best in bull markets.

**SCDV (Small Cap Deep Value)** — Micro/small caps, Piotroski ≥ 6, Altman Z > 1.81, Beneish safe. ML(3y) scoring.

**IARB (International Arbitrage)** — Non-US markets. Deep value, Piotroski ≥ 6. Currently limited by non-US data coverage (2022–present).

---

## Factor Research

```bash
python3 scripts/factor_research.py --all-horizons
python3 scripts/factor_research.py --horizon 3y --top 20
```

Outputs `reports/factor_research_{horizon}.csv` with IC, ICIR, t-statistic, and turnover per factor.

---

## Data Pipeline

```bash
# Full pipeline refresh (US)
python3 scripts/refresh_data.py

# Train models after data refresh
python3 scripts/train_models.py

# Advanced training options
python3 scripts/train_models.py \
    --train-cutoff 2019 \   # last year included in training set (default: 2019)
    --val-end 2021 \        # last year included in validation set (default: 2021)
    --sector-neutral        # compute sector-neutral IC in feature selection

# Run backtest
python3 scripts/backtester.py --strategy all --market US

# Launch app
streamlit run app_v2.py
```

---

## Bias Audit

Run after building the dataset to check for systematic biases before training:

```bash
# Report only (no writes)
python3 scripts/bias_audit.py

# Compute and append forward_return_{h}_usd columns (FX-adjusted)
python3 scripts/bias_audit.py --fix

# Write FX-adjusted output to a separate file
python3 scripts/bias_audit.py --fix --out data/historical_dataset_fx.parquet
```

Checks three biases:
1. **Survivorship bias** — fraction of training rows from delisted companies; warns if < 5%
2. **Filing date look-ahead leakage** — flags any row where `filed_date < period_end`
3. **FX contamination** — `forward_return_*` is in local currency; adds USD-adjusted columns via `(1 + local_ret) * (1 + fx_ret) - 1`

---

## Cloud Deployment (Streamlit Community Cloud)

1. Push data and models to HuggingFace Hub:

   ```bash
   export HF_TOKEN=your_hf_write_token
   python3 scripts/push_to_hf.py --repo your-username/stock-screener-data
   ```

   Options: `--data-only`, `--models-only`, `--public` (default: private repo).

2. Add `HF_REPO=your-username/stock-screener-data` as a secret in Streamlit Community Cloud (Settings → Secrets).

3. Deploy `app_v2.py` from GitHub. When `HF_REPO` is set, the app loads dataset and models from HuggingFace Hub automatically. Pipeline refresh buttons are hidden in cloud mode.

---

## Automated Weekly Refresh (GitHub Actions)

Two scheduled workflows keep the system current:

| Workflow | Schedule | What it does |
|----------|----------|-------------|
| `refresh_data.yml` | Sun 05:00 UTC | Full data pipeline → generate reports → push to HuggingFace |
| `monitor_drift.yml` | Mon 07:00 UTC | PSI + rolling AUC check → emit warning if drift detected |

**Required GitHub Actions secrets:**
- `HF_TOKEN` — HuggingFace write token
- `HF_REPO` — e.g. `your-username/stock-screener-data`

To trigger manually: Actions tab → Weekly Data Refresh → Run workflow.

See `docs/developer/monitoring.md` for drift alert thresholds and interpretation.

---

## Markets

| Market | Source     | Status           | Years        |
|--------|------------|------------------|--------------|
| US     | SEC EDGAR  | Full coverage    | 2008–2025    |
| CA     | SEDAR+     | Partial          | 2019–2025    |
| BR     | B3/CVM     | Partial          | 2015–2025    |
| JP     | TDNET      | Partial          | 2018–2025    |
| EU     | Exchange   | Thin coverage    | 2019–2025    |
| KR     | DART       | In progress      | 2015–2025    |

---

## Research Notebooks (V4)

All notebooks in `research/` are V4 schema compatible:

| Notebook | Status | Notes |
|----------|--------|-------|
| `01_metric_exploration.ipynb` | ✅ V4 | Feature distributions, correlation heatmap |
| `02_historical_dataset.ipynb` | ✅ V4 | Dataset assembly, coverage by market |
| `03_ml_model.ipynb`           | ✅ V4 | Walk-forward + calibration (3 bugs fixed, see below) |
| `04_factor_research.ipynb`    | ✅ V4 | IC/ICIR by horizon, SHAP summary |

**Walk-forward result dict keys** (`run_walk_forward()` returns a list of dicts with):
`test_year`, `n_train`, `n_test`, `auc`, `precision_at_10pct`, `mae`, `pred_proba`, `true_cls`, `pred_reg`, `true_reg`

**notebook-03 bug fixes applied:**
1. `train_final_model` returns 4 values `(clf, X, sub, feats)` — was incorrectly unpacked as 3
2. Walk-forward result key `lift1` renamed to `precision_at_10pct` — index key updated throughout
3. Calibration curve variables renamed: `y_score` → `pred_proba`, `y_true` → `true_cls`, `year` → `test_year`

---

## Known Limitations

- No ADTV (volume) data — using $50M market cap as liquidity proxy
- IARB strategy produces ~1 year of backtest (non-US data bulk-added from 2021)
- Korea DART pipeline running (~May 29 completion); merged dataset will be rebuilt after
- `app_data.parquet` is a stale 33-column file — app uses `historical_dataset_clean.parquet` directly

---

## Tests

```bash
pip install pytest
pytest tests/
```

The test suite uses synthetic in-memory data only — no files on disk, no network calls.

---

## Requirements

```bash
pip install streamlit lightgbm joblib scikit-learn pandas numpy scipy yfinance plotly
```
