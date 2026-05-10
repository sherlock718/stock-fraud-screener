# Developer Setup

## Requirements

- Python 3.11+
- Git
- 8GB+ RAM recommended (training models on full US dataset)
- Optional: SIMFIN_API_KEY (for EU data), DART_API_KEY (for Korea), HF_TOKEN (HuggingFace)

## Clone and Install

```bash
git clone https://github.com/mhoque/stock-fraud-screener.git
cd stock-fraud-screener

# Create virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install all dependencies
pip install -r requirements.txt

# Install mkdocs for documentation (optional)
pip install mkdocs-material
```

## Directory Structure

```
stock-fraud-screener/
├── app_v2.py                  # Main Streamlit app (8 tabs)
├── requirements.txt
├── mkdocs.yml
│
├── scripts/                   # Pipeline, training, analysis scripts
│   ├── run_pipeline.py        # Main US pipeline
│   ├── run_pipeline_eu.py     # EU pipeline (SimFin)
│   ├── run_pipeline_kr.py     # Korea pipeline (DART)
│   ├── feature_library.py     # 278 feature definitions
│   ├── train_models.py        # LightGBM training
│   ├── tune_models.py         # Optuna + CatBoost + calibration
│   ├── backtester.py          # Walk-forward backtester
│   ├── factor_research.py     # IC/ICIR/decay library
│   ├── leverage_strategy.py   # Long/short Kelly portfolio
│   ├── monitor_drift.py       # PSI + AUC drift monitoring
│   ├── generate_reports.py    # PDF tearsheet + CSV picks
│   └── bias_audit.py          # Look-ahead / survivorship audit
│
├── research/                  # Jupyter notebooks
│   ├── 01_data_exploration.ipynb
│   ├── 02_factor_analysis.ipynb
│   ├── 03_ml_model.ipynb
│   ├── 04_bias_audit.ipynb
│   └── 05_leverage_analysis.ipynb
│
├── data/                      # Generated data (gitignored)
│   ├── historical_dataset_clean.parquet
│   ├── backtest_results.json
│   └── refresh_status.json
│
├── models/                    # Trained model artifacts (gitignored)
│   ├── model_1y.joblib
│   ├── model_3y.joblib
│   ├── model_5y.joblib
│   └── model_meta.json
│
├── reports/                   # Generated reports (gitignored)
│   ├── tearsheet.pdf
│   ├── weekly_picks.csv
│   └── rolling_oos_auc.png
│
├── .github/workflows/
│   ├── refresh_data.yml       # Weekly data refresh (Sunday 05:00 UTC)
│   └── monitor_drift.yml      # Weekly drift check (Monday 07:00 UTC)
│
└── docs/                      # This documentation
```

## Environment Variables

| Variable | Required | Purpose |
|---|---|---|
| `HF_TOKEN` | Optional | HuggingFace write token (for dataset upload) |
| `HF_REPO` | Optional | HuggingFace repo ID (e.g. `username/stock-fraud-screener`) |
| `SIMFIN_API_KEY` | For EU pipeline | SimFin API key |
| `DART_API_KEY` | For Korea pipeline | DART (FSS) API key |

Set in `.env` or export directly:

```bash
export HF_TOKEN=hf_xxx
export HF_REPO=mhoque/stock-fraud-screener
```

## From-Scratch Build

To build everything from scratch:

```bash
# 1. Run US pipeline (30–60 min)
python3 scripts/run_pipeline.py --market US

# 2. Train base models
python3 scripts/train_models.py

# 3. (Optional) Tune with Optuna + CatBoost
python3 scripts/tune_models.py

# 4. Run backtest
python3 scripts/backtester.py

# 5. Generate reports
python3 scripts/generate_reports.py --top 25

# 6. Launch app
streamlit run app_v2.py
```

## Development Branch

Active development happens on the `develop` branch. The `main` branch contains the latest stable release.

```bash
git checkout develop
git pull origin develop
```
