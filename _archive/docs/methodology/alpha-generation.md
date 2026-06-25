# Alpha Generation Schema

## Overview

The alpha generation system translates a user's investment horizon into a ranked list of
high-alpha stock candidates. The system:

1. Routes the requested horizon to the nearest trained discrete model
2. Scores all companies using that model
3. Combines ML alpha with 4 factor groups via composite ranking
4. Returns a sorted screener list with factor group attribution

---

## Variable-Horizon Model Routing

The system trains 5 discrete models — 6m, 1y, 2y, 3y, 5y — one per investment horizon.
Users input any number of months; `HorizonRouter` maps it to the nearest model.

```
6m  model: 3–9 months requested
1y  model: 9–18 months requested
2y  model: 18–30 months requested
3y  model: 30–48 months requested
5y  model: 48+ months requested
```

On a tie at a boundary, the longer model is used (conservative bias).

```python
from alpha.horizon_router import HorizonRouter

key = HorizonRouter.route(18)   # → '1y' (18 is the upper boundary of 1y, conservative)
key = HorizonRouter.route(24)   # → '2y'
key = HorizonRouter.route(60)   # → '5y'
```

**File:** `alpha/horizon_router.py`

---

## Scoring Pipeline

```
User horizon (months)
       │
       ▼
  HorizonRouter.route()
       │
       ▼
  Model key ('6m' / '1y' / '2y' / '3y' / '5y')
       │
       ▼
  score_companies(df, models, meta, horizon=key)
       │  - loads model_meta.json features for key
       │  - fills NaN with train_medians
       │  - returns ml_score ∈ [0, 1]
       │
       ▼
  composite_rank(df)
       │  - pct_rank each factor component
       │  - weighted average (value 25%, quality 20%,
       │    momentum 20%, fraud_safety 20%, ml_alpha 15%)
       │
       ▼
  Ranked alpha screener output
  (companies sorted by composite_score descending)
```

**Files:** `src/scoring.py` → `resolve_horizon()`, `score_companies()`, `composite_rank()`

---

## Factor Groups

The five factor groups feed the composite alpha score:

| Factor Group | Weight in Composite | Key Features |
|---|---|---|
| Value | 25% | P/E, P/B, EV/EBITDA, earnings_yield, FCF yield |
| Quality | 20% | Gross margin, ROE, Piotroski F, Altman Z, debt coverage |
| Momentum | 20% | 12m prior return, 6m return, vol-adjusted trend |
| Fraud Safety | 20% | Beneish M-score, Montier C, Sloan accruals |
| ML Alpha | 15% | Model probability score for selected horizon |

The composite score is a display-layer aggregation. ML training uses these groups as *input features*,
not pre-aggregated scores — the ML model learns its own weights per feature per horizon.

---

## Model Confidence Display

Each model has a walk-forward AUC (WF-AUC) that reflects out-of-sample predictive power:

| WF-AUC | Confidence Level | UI Badge Color |
|---|---|---|
| ≥ 0.65 | High confidence | Green |
| 0.60–0.65 | Good confidence | Light green |
| 0.55–0.60 | Moderate confidence | Orange |
| < 0.55 | Screening only | Red + warning |

When WF-AUC < 0.60, the UI shows: *"Lower confidence — use for screening only, not standalone signals."*

---

## Feature Attribution

`top_feature_importances(models, meta, key, top_n)` returns the top features driving the model's
alpha scores for a given horizon. Source priority:

1. `shap_top_features` from `model_meta.json` (populated after retraining with SHAP enabled)
2. LightGBM `feature_importances_` attribute (fallback, available immediately)

Features are labeled by factor group using `FEATURE_FACTOR_GROUPS` in `alpha/horizon_router.py`.
The screener UI shows top 5 features per company with their actual values in the Company Deep Dive section.

---

## Alpha Screener Output

The screener produces a **ranked list of high-alpha candidates** sorted by composite_score descending.
Each row includes:

| Column | Description |
|---|---|
| `composite_score` | Percentile rank 0–100 across all factor groups + ML |
| `Alpha Score` | ML model probability for selected horizon |
| `value_composite` | Percentile rank within Value factor |
| `quality_composite` | Percentile rank within Quality factor |
| `momentum_12m_prior` | Raw trailing 12-month price return |
| `beneish_m_score` | Fraud manipulation risk (lower = safer) |

A composite_score ≥ 60th percentile is the default screener threshold.

---

## OOF Scores vs Static Scores

| Score column | Source | Use case |
|---|---|---|
| `ml_1y` / `ml_3y` / `ml_5y` | score_historical.py — static, in-sample | Display in screener for current-year companies |
| `ml_1y_oof` / `ml_3y_oof` / `ml_5y_oof` | generate_oof_scores.py — walk-forward OOF | Backtesting (unbiased) |
| `ml_score` | Computed on-the-fly by score_companies() | Live screener scoring |

**Rule:** The backtester must use `ml_*_oof` scores. The live screener uses `ml_score` computed
on-the-fly from the loaded model — this is appropriate because it applies a previously-trained model
to new data (not training on the data being scored).
