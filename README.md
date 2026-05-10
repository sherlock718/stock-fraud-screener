# Stock Fraud & Quantitative Screener

A research-grade quantitative stock screening and backtesting system built on SEC EDGAR (US), SEDAR+ (Canada), B3 (Brazil), TDNET (Japan), and EU exchange data.

## What It Does

- **Screens** current-year companies using 5 composite signals: value, quality, ML probability, fraud risk, and momentum
- **Backtests** 4 strategies (COMPOSITE, QEM, SCDV, IARB) with walk-forward ML retraining to avoid look-ahead bias
- **Flags fraud risk** using Beneish M-Score, Ohlson O-Score, Altman Z-Score, Piotroski F-Score, and custom accruals signals
- **Ranks factors** by IC/ICIR across 1y/3y/5y horizons for factor research

---

## Architecture

```
pipeline/               Data ingestion (fetch tickers, financials, prices, KPIs)
├── step1_fetch_tickers*.py     Ticker discovery per market
├── step2_build_snapshots*.py   Build annual financial snapshots
├── step3_enrich_kpis.py        Compute derived KPIs (200+ features)
├── build_historical_dataset.py Merge into single parquet
└── fraud_signals.py            Beneish/Ohlson/Altman/Piotroski

scripts/
├── train_models.py             Train LightGBM models (1y/3y/5y)
├── backtester.py               Walk-forward backtest engine
├── factor_research.py          IC/ICIR/turnover analysis
├── bias_audit.py               Survivorship/leakage/FX bias checks
├── push_to_hf.py               Upload dataset + models to HuggingFace Hub
└── refresh_data.py             Full refresh orchestrator

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
└── weekly_push.yml             Weekly cron (Sun 02:00 UTC) to push data/models to HuggingFace Hub
```

---

## ML Models

| Horizon | Features | Train Rows | OOS AUC |
|---------|----------|------------|---------|
| 1y      | 27       | 40,907     | 0.749   |
| 3y      | 31       | 31,151     | 0.780   |
| 5y      | 31       | 21,965     | 0.856   |

- Target: beat local market index over horizon
- Feature selection: top features by |ICIR| (IC/StdIC), deduplicated at |Spearman| > 0.90
- Walk-forward retraining used in backtester (not static models) to avoid look-ahead bias

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

The workflow `.github/workflows/weekly_push.yml` runs every Sunday at 02:00 UTC.

**Required GitHub Actions secrets:**
- `HF_TOKEN` — HuggingFace write token
- `HF_REPO` — e.g. `your-username/stock-screener-data`

The workflow: checks out the repo → runs `scripts/run_pipeline.py build --step 4` (features + clean, no new API calls) → pushes updated dataset and models to HuggingFace Hub.

To trigger manually: Actions → Weekly Data Push to HuggingFace → Run workflow.

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
