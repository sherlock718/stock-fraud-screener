# Quick Start

Get the screener running in 5 minutes.

## Prerequisites

- Python 3.11+
- Git

## 1 — Clone and Install

```bash
git clone https://github.com/mhoque/stock-fraud-screener.git
cd stock-fraud-screener
pip install -r requirements.txt
```

Key dependencies installed:

| Package | Version | Purpose |
|---|---|---|
| streamlit | 1.50.0 | Web app framework |
| lightgbm | 4.6.0 | Primary ML model |
| catboost | 1.2.7 | Ensemble component |
| optuna | 4.3.0 | Hyperparameter tuning |
| shap | 0.47.2 | Feature attribution |
| plotly | 6.7.0 | Interactive charts |
| yfinance | 1.2.0 | Price data |

## 2 — Run the Data Pipeline

```bash
# US market only (recommended for first run)
python3 scripts/run_pipeline.py --market US

# All markets (takes longer)
python3 scripts/run_pipeline.py
```

This produces `data/historical_dataset_clean.parquet`.

!!! tip "Skip the pipeline"
    If you just want to explore the app, the dataset is hosted on HuggingFace. Set `HF_REPO` and `HF_TOKEN` in your environment and the pipeline will auto-download the base dataset.

## 3 — Train Models

```bash
python3 scripts/train_models.py
```

Trains 1y, 3y, 5y LightGBM models with ICIR feature selection. Outputs to `models/`.

For Optuna-tuned models with CatBoost ensemble:

```bash
python3 scripts/tune_models.py
```

## 4 — Launch the App

```bash
streamlit run app_v2.py
```

The app opens at `http://localhost:8501`.

## What You'll See

The app has 8 tabs:

| Tab | What it shows |
|---|---|
| **Overview** | Score distribution, top picks, market summary |
| **Screener** | Filter all companies by score, market, SIC code |
| **Company Profile** | Per-company deep dive with SHAP attribution |
| **Realtime Chart** | Live price chart + fraud score overlay |
| **Factor Research** | IC/ICIR charts, factor decay |
| **Backtest** | Walk-forward strategy performance |
| **Market Overview** | Cross-market comparison |
| **Model Diagnostics** | AUC, calibration, PSI drift |

## Next Steps

- [App Walkthrough →](guide/app.md) — tour of every tab
- [Score Interpretation →](guide/scores.md) — what the numbers mean
- [Architecture →](architecture.md) — how the system works
