# Quick Start

Get the platform running in 5 minutes.

## Prerequisites

- Python 3.11+
- Git

## 1 — Clone and Install

```bash
git clone https://github.com/sherlock718/stock-fraud-screener.git
cd stock-fraud-screener
pip install -r requirements.txt
```

Key dependencies installed:

| Package | Version | Purpose |
|---|---|---|
| lightgbm | 4.6.0 | Primary ML model |
| catboost | 1.2.7 | Ensemble component |
| optuna | 4.3.0 | Hyperparameter tuning |
| shap | 0.47.2 | Feature attribution |
| plotly | 6.7.0 | Interactive charts |
| yfinance | 1.2.0 | Price data |

## 2 — Run the Data Pipeline

```bash
# US market only (recommended for first run)
python3 scripts/workflows/run_pipeline.py --market US

# All markets (takes longer)
python3 scripts/workflows/run_pipeline.py
```

This produces `data/historical_dataset_clean.parquet`.

!!! tip "Skip the pipeline"
    If you just want to explore the app, the dataset is hosted on HuggingFace. Set `HF_REPO` and `HF_TOKEN` in your environment and the pipeline will auto-download the base dataset.

## 3 — Train Models

```bash
python3 scripts/modeling/train_models.py
```

Trains all 5 horizon models (6m/1y/2y/3y/5y) with ICIR feature selection. Outputs to `models/`.

For Optuna-tuned models with CatBoost ensemble:

```bash
python3 scripts/modeling/tune_models.py
```

## 4 — Launch the Research Notebook

```bash
# Rebuild screener and alpha registries first (if not already built)
python3 scripts/portfolio/build_screener_registry.py
python3 scripts/portfolio/build_alpha_registry.py

# Open the experiment hub notebook
jupyter notebook notebooks/08_experiment_hub.ipynb
```

The notebook has 5 sections:

| Section | What it shows |
|---|---|
| **1. Feature Selection** | PSI / ICIR feature stability and selection results |
| **2. Model Performance** | Walk-forward AUC, calibration, OOF scores |
| **3. Screener Rankings** | 3a: composite registry leaderboard · 3b: individual alpha signals |
| **4. Deep Dive** | 4a: composite strategy detail · 4b: individual signal deep dive |
| **5. Live Picks** | Current top-ranked companies per screener |

## Next Steps

- [Architecture →](architecture.md) — how the system works
- [Score Interpretation →](guide/scores.md) — what the numbers mean
- [Backtesting →](methodology/backtesting.md) — walk-forward methodology
