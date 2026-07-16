# Start Here — Onboarding Guide

## What This Is

A multi-factor stock screener that removes fraud/distress, then ranks survivors by predicted 3-year return. Production output: 15 equal-weight picks, refreshed annually.

**Backtest (2013-2023):** CAGR +31.5%, Sharpe 1.45, MaxDD -8.1%

---

## Setup

```bash
# 1. Clone + install
git clone <repo-url> && cd stock-fraud-screener-main
pip install -r requirements.txt

# 2. Environment (optional — only needed for M&A screen)
export GROQ_API_KEY=<from console.groq.com>

# 3. Data (already in repo via HuggingFace)
python data_io/pull_from_hf.py   # if data/ is empty
```

**Required files:**
- `data/historical_dataset_clean.parquet` — main dataset
- `data/monthly_prices.parquet` — ADTV liquidity data
- `models/model_3y.joblib`, `model_3y_regression.joblib`, `research_tree_snapshot.joblib` — trained models
- `models/model_meta.json` — feature lists + medians

---

## Run the Screener

Open `notebooks/production_screener.ipynb` and run all cells. Output:
- Top 15 picks ranked by predicted 3y return
- Per-stock analysis + buy rationale
- M&A screen (if Groq API key set)
- Picks saved to `data/production_picks_YYYY-MM-DD.json`

---

## How It Works (30-second version)

```
4,133 US stocks (FY2025)
    → 8 hard gates (Beneish, Piotroski, ROA, fraud, market cap, Altman Z, value, momentum)
    → 435 survivors
    → Score with LightGBM regression (predicted 3y return)
    → Gate: decision tree must agree (prob >= 0.55)
    → 39 pass agreement
    → ADTV liquidity filter
    → Top 15 by predicted return
    → Equal-weight, annual rebalance
```

**Key insight:** Fraud/distress removal IS the alpha. We don't pick winners — we remove losers, then let cheap+quality compound.

---

## Reading Order (Deep Understanding)

| # | Doc | What You'll Learn |
|---|-----|-------------------|
| 1 | `docs/CODEX_ROADMAP.md` | Active session-by-session validation and learning plan |
| 2 | `docs/FAQ.md` | Quick reference — thresholds, features, pipeline steps |
| 3 | `docs/PRODUCTION_CONFIG.md` | Current production config — gates, scoring, pros/cons |
| 4 | `docs/architecture/orientation_pipeline_spine.md` | How raw SEC filings → clean dataset |
| 5 | `docs/architecture/orientation_modeling_alpha.md` | How features → ML signal (training, walk-forward, alpha factors) |
| 6 | `docs/architecture/orientation_research_backtest.md` | How we validate it works (backtest engine, strategies, stats) |
| 7 | `docs/architecture/orientation_quality_orchestration.md` | How CI/tests keep it honest (523 tests, drift monitoring) |
| 8 | `notebooks/production_screener.ipynb` | The product — run it, see picks |

---

## Key Concepts

| Term | Meaning |
|------|---------|
| `reg_3y` | Model's predicted 3-year forward return (NOT realized CAGR) |
| `tree_prob` | Decision tree's probability the stock is a winner (gate: >= 0.55) |
| `ml_gates` | Production strategy — ML ranking + hard gates (no alpha composite) |
| Walk-forward | Train only on past data, score next year. No future leakage. |
| Survivorship imputation | Missing returns = -50% (pessimistic, prevents bias) |

---

## Single Source of Truth

All production thresholds live in one file: `modeling/constants.py`

```python
BENEISH_THRESHOLD = -1.78
TREE_THRESHOLD = 0.55
PIOTROSKI_MIN = 3
VALUE_GATE_PCT = 0.70
ALTMAN_Z_MIN = 1.0
MOMENTUM_12M_MIN = -0.40
MAX_MARKET_CAP_PROD = 10_000_000_000
```

Change here → changes everywhere (backtest engine, notebook, tests).

---

## Tests

```bash
python3 -m pytest tests/ -x -q   # 523 tests, ~10 seconds
```

---

## What This Is NOT

- Not a trading bot (no execution, no live orders)
- Not a short-term signal (annual rebalance, 3-year horizon)
- Not guaranteed to work forward (backtest ≠ prediction)
- Not a fraud detector (fraud removal is one of 8 gates, not the product)
