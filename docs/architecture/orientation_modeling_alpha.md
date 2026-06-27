# Orientation: Modeling + Alpha Scoring (Session 28)

> How features become a buy/sell signal.

---

## 1. Model Architecture Map

### 1.1 Model Type & Hyperparameters

| Component | Value |
|-----------|-------|
| Primary model | LightGBM Classifier |
| n_estimators | 600 |
| max_depth | 6 |
| num_leaves | 63 |
| learning_rate | 0.03 |
| subsample | 0.80 |
| colsample_bytree | 0.70 |
| min_child_samples | 20 |
| reg_alpha / reg_lambda | 0.1 / 1.0 |
| scale_pos_weight | auto (neg/pos ratio) |
| Baseline comparator | Logistic Regression (StandardScaler + C=0.1) |
| Agreement gate | Depth-4 Decision Tree (session 24) — tree_prob ≥ 0.35 |
| Regression model | LightGBM Huber (predicts continuous excess return) |
| Tuning (optional) | Optuna + CatBoost comparison + isotonic calibration |

### 1.2 Horizons

Five discrete models, one per horizon:

| Key | Target column (binary) | Return column (continuous) |
|-----|----------------------|---------------------------|
| 6m | `beat_local_market_6m` | `forward_return_6m` |
| 1y | `beat_local_market_1y` | `forward_return_1y` |
| 2y | `beat_local_market_2y` | `forward_return_2y` |
| 3y | `beat_local_market_3y` | `forward_return_3y` |
| 5y | `beat_local_market_5y` | `forward_return_5y` |

**Target definition**: `beat_local_market_{h}` = 1 if stock's forward return > market benchmark return over horizon h (top-quartile in some variants), 0 otherwise.

### 1.3 Train/Test Split Mechanics

```
Train  : fiscal_year <= 2022 AND filed_date < 2023-01-01  (PIT-safe)
Val    : fiscal_year 2023
Test   : fiscal_year > 2023 (2024+)
```

- **Point-in-time (PIT) enforcement**: Rows with filed_date after the train cutoff boundary are excluded from training even if their fiscal_year ≤ cutoff. Prevents look-ahead from late-filed annual reports.
- **Dedup**: Within (ticker, fiscal_year), keeps row with largest `total_assets` (largest filer wins).
- **Universe filter**: `period_type == 'annual'`, `fiscal_year ∈ [2008, 2025]`.

### 1.4 Walk-Forward Logic

**Expanding-window walk-forward CV** (`walk_forward_cv()` in `modeling/train.py`):

```
For each fold year t in [first_year + min_train_years, TRAIN_CUTOFF]:
  Train: fiscal_year <= (t - embargo_years) AND filed_date < Jan 1 of (t+1)
  Test:  fiscal_year == t+1
```

- `min_train_years` default = 6 (so first fold has ≥6 years of data)
- `embargo_years` default = 0 (set to 1 for purged CV — prevents adjacent-year autocorrelation)
- Folds excluded where forward-return horizon hasn't fully elapsed by dataset end
- Optional ensemble mode: 50/50 blend of LightGBM + XGBoost per fold

**OOF scoring** (`modeling/score_oof.py`): Same expanding-window, writes `ml_{h}_oof` columns back to parquet. These are in-sample-contamination-free scores used by alpha factors.

### 1.5 Feature Selection Pipeline

**Source file**: `modeling/run_feature_selection.py`

```
Stage 1: PSI filter        — drop features with PSI > 0.25 (train vs test drift)
Stage 2: IC screen         — keep |mean_IC| ≥ 0.02, n_years ≥ 5, BH FDR q < 0.05
Stage 3: ICIR rank         — sort by |ICIR| descending, keep top 60
Stage 4: Spearman dedup    — drop pairs with |r| > 0.85 (keep higher ICIR)
```

Additional sophistication:
- **Newey-West HAC t-statistics** correct for autocorrelation in year-over-year IC
- **Benjamini-Hochberg FDR** controls false discovery rate at α=0.05 across ~300 candidates
- **IC stability filter**: feature's IC must have correct sign in ≥60% of years
- **Force-includes** for 6m/1y/2y: momentum/macro features that ICIR systematically under-selects

**Current feature counts** (from `models/feature_sets_*.json`):
- 3y: 45 features
- Pruned (27 temporally stable): used for backtest in sessions 23-24

---

## 2. Alpha Factor Composition

### 2.1 Factor Architecture

Each factor module lives in `alpha/factors/{name}.py` and exposes `compute(df, group_cols) → pd.Series`.

All factors produce a **cross-sectional percentile rank** (0–1) within `(fiscal_year, market)` groups. Higher = better signal direction for ALL factors (including fraud_risk, where high = safer).

### 2.2 Factor Details

| Factor | Module | Signals | Rank logic |
|--------|--------|---------|-----------|
| **Value** | `value.py` | ev_ebitda, ev_revenue (inverted); fcf_yield, earnings_yield, book_to_market (direct); ps_ratio, pe_ratio (inverted) | Mean of 7 percentile ranks; winsorized at 1%/99% before ranking |
| **Quality** | `quality.py` | roe, roa, roic, gross_margin, operating_margin, ocf_to_ni, piotroski_f_score (direct); accruals_to_assets, sloan_accruals (inverted); gross_profit_to_assets (direct) | Mean of 10 percentile ranks |
| **Momentum** | `momentum.py` | momentum_12m_prior, momentum_6m_prior, momentum_3m_prior, momentum_12m_rank, momentum_6m_rank, momentum_3m_rank | Mean of 6 percentile ranks (no winsorization) |
| **Growth** | `growth.py` | revenue_cagr_3y, revenue_growth_yoy, eps_growth_yoy, net_income_growth_yoy, ocf_growth_yoy, gross_profit_growth_yoy | Mean of 6 percentile ranks; winsorized |
| **Fraud Risk** | `fraud_risk.py` | beneish_m_score, ohlson_prob_bankruptcy, fraud_score_composite/accounting/distress (inverted = high = safer); altman_z_score (direct); ml_1y_oof, ml_3y_oof, ml_5y_oof (direct) | Mean of up to 9 percentile ranks |

### 2.3 Composite Blending

**File**: `alpha/factors/composite.py`

```python
DEFAULT_WEIGHTS = {
    "value":      0.20,
    "quality":    0.20,
    "momentum":   0.20,
    "growth":     0.20,
    "fraud_risk": 0.20,
}
```

Composite = weighted average of 5 factor scores. Weights are normalized to sum to 1.0.

**Interaction**: Factors are **additive only** — no multiplicative gates, no conditional overrides. Each factor contributes independently to the composite. The only non-linearity is that `fraud_risk` factor consumes ML OOF scores (`ml_{h}_oof`), creating an indirect dependency: ML model → OOF scores → fraud_risk factor → composite.

### 2.4 Supporting Modules

| File | Role |
|------|------|
| `alpha/horizon_router.py` | Maps user-requested months → nearest model key (3-9mo→6m, 9-18→1y, etc.) |
| `alpha/explain.py` | Generates plain-English investment thesis per stock (thresholds: Piotroski≥6, Beneish<-1.78, Altman>1.81, ML≥0.75 high conviction) |
| `alpha/__init__.py` | Registry: exports `value`, `quality`, `momentum`, `growth`, `fraud_risk`, `composite` |
| `modeling/alpha.py` | Orchestrator: calls `compute_composite(df)`, writes 6 alpha columns back to parquet |

---

## 3. Column Lineage: Features → Signal

```
┌─────────────────────────────────────────────────────────────────────┐
│ PIPELINE OUTPUT (step5/step6)                                        │
│   ~200 feature columns in historical_dataset_clean.parquet           │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│ FEATURE SELECTION (run_feature_selection.py)                          │
│   PSI → IC/FDR → ICIR rank → Dedup                                  │
│   Output: models/feature_sets_{h}.json (27–60 features per horizon)  │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│ ML TRAINING (modeling/train.py)                                       │
│   Input: selected features + beat_local_market_{h} target            │
│   Output: models/model_{h}.joblib                                    │
│           models/model_meta.json (features, medians, AUCs)           │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│ OOF SCORING (modeling/score_oof.py)                                   │
│   Walk-forward expanding window, writes:                             │
│     ml_6m_oof, ml_1y_oof, ml_2y_oof, ml_3y_oof, ml_5y_oof          │
│   (NaN for training-window rows — prevents contamination)            │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│ ALPHA FACTORS (alpha/factors/*.py)                                    │
│   Each factor: cross-sectional rank within (fiscal_year, market)     │
│   fraud_risk consumes ml_{h}_oof scores                              │
│   Output: alpha_value, alpha_quality, alpha_momentum,                │
│           alpha_growth, alpha_fraud_risk                              │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│ COMPOSITE (alpha/factors/composite.py)                                │
│   Weighted average (equal 20% each)                                  │
│   Output: alpha_composite (0–1 score)                                │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│ DECISION LAYER (research/explainable_tree.py + regime_overlay.py)     │
│   Agreement filter: LightGBM ranks + tree gates (prob ≥ 0.35)        │
│   Regime overlay: SPY trailing DD > 15% → 50% cash                   │
│   Output: final BUY/HOLD signal                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Column Groups

| Column set | Created by | Consumed by |
|-----------|-----------|------------|
| `forward_return_{h}` | pipeline/step3 (price data) | train.py IC analysis, regression target |
| `beat_local_market_{h}` | pipeline/step3 | train.py classifier target |
| `ml_{h}` | score_historical (all-row scoring — CONTAMINATED) | EXCLUDED from ML training |
| `ml_{h}_oof` | score_oof.py (walk-forward) | fraud_risk factor, EXCLUDED from ML training |
| `alpha_{factor}` | alpha/factors/*.py | composite.py, EXCLUDED from ML training |
| `alpha_composite` | alpha/factors/composite.py | screener/portfolio ranking |

---

## 4. Risk Register

### 4.1 Overfitting Vectors

| Risk | Severity | Location | Detail |
|------|----------|----------|--------|
| Feature selection on full data | HIGH | train.py PSI filter uses df_test | PSI computed between train and test. If test distribution informs which features pass, it's soft leakage. Feature selection should use train+val only. |
| Force-include overrides ICIR | MEDIUM | train.py:90-96 | 9 features hardcoded for 6m/1y, bypassing the selection pipeline. If chosen by hindsight, they inject lookahead. |
| Decision tree trained on 2008-2018 | MEDIUM | decision_tree_rules.json | Tree rules are frozen from one train window. No walk-forward — could overfit to that period's market regime. |
| Equal alpha weights | LOW | composite.py | 20% per factor regardless of realized IC. Slightly inefficient but avoids weight-mining overfitting. |
| ICIR from only ~14 years of data | MEDIUM | train.py IC analysis | With 14 year-level observations, ICIR estimates are noisy. A feature with IC=[+0.3, -0.1, ...] in 14 years could be random. |

### 4.2 Stale Model Artifacts

| Artifact | Risk | Detail |
|----------|------|--------|
| `models/model_meta.json` | NOT PRESENT | File missing or unparseable. Training may not have been run recently, or it was gitignored. All downstream consumers (score_oof, tune) depend on it. |
| `models/model_{h}.joblib` | UNKNOWN | Binary artifacts likely gitignored. Must be regenerated after any feature_sets change. |
| `models/decision_tree_rules.json` | LOW | Frozen rules from session 24. Valid as long as the 27 pruned features remain stable. |
| `models/feature_sets_pruned.json` | LOW | 27 features from temporal stability test. Canonical for backtest but may diverge from `feature_sets_3y.json` (45 features). |

### 4.3 Weight & Threshold Assumptions

| Threshold | Value | Location | Risk |
|-----------|-------|----------|------|
| Alpha weights | 20% equal | composite.py:17 | No empirical optimization — could underweight high-IC factors |
| Agreement filter | tree_prob ≥ 0.35 | Session 24 sweep | Chosen from {0.30, 0.35, 0.40, 0.50} — only 4 points tested |
| Regime overlay | SPY DD > 15% | research/regime_overlay.py | Arbitrary threshold, tested on limited sample (2009, 2010, 2023 triggers) |
| PSI threshold | 0.25 | run_feature_selection.py:44 | Industry standard but sensitive: 0.20 vs 0.30 could add/drop 5-10 features |
| IC minimum | 0.02 | run_feature_selection.py:45 | Very permissive — most quant shops use 0.03-0.05 |
| Correlation dedup | 0.85 | run_feature_selection.py:48 | Allows fairly correlated features through (0.80 more standard) |
| n_estimators | 600 | train.py:323 | Not tuned per horizon — same for all 5 models |
| ML conviction thresholds | 0.60, 0.75 | alpha/explain.py:25-26 | Used for position sizing/strategy — not validated against realized returns |
| Beneish manipulation | -1.78 | alpha/explain.py:28 | Academic threshold — applied as binary gate in explain.py |

### 4.4 Structural Risks

| Risk | Detail |
|------|--------|
| **Two divergent feature sets** | `feature_sets_3y.json` (45 features) vs `feature_sets_pruned.json` (27 features). The pruned set drives the backtest, but `score_oof.py` loads from `feature_sets_{h}.json` (45). Which is canonical for production? |
| **Fraud_risk factor circular dependency** | ML OOF scores are inputs to fraud_risk alpha factor, but alpha_fraud_risk is EXCLUDED from ML training. The exclusion prevents direct circularity but creates an implicit signal loop: ML → OOF → fraud_risk → composite → screener decisions. |
| **OOF file has undefined BASE** | `modeling/score_oof.py` line 44 uses `BASE` before it's assigned (line 52 assigns it). Would crash on import. Likely a copy-paste ordering bug. |
| **Regression model uncoupled from classifier** | `train_regression_model.py` shares feature sets but trains independently. No guarantee they agree — a stock could rank high on binary probability but low on predicted excess return. |
| **No automated retraining trigger** | Models must be manually retrained after new data arrives. No CI job or staleness check on model artifacts. |

---

## 5. Refactor Candidates (DO NOT EXECUTE)

| # | What | Why | Effort |
|---|------|-----|--------|
| 1 | **Unify feature_sets_pruned.json vs feature_sets_{h}.json** | Two parallel feature set standards cause confusion about which is canonical. Should be one source of truth per horizon. | Medium |
| 2 | **Fix BASE ordering in score_oof.py** | Line 44 uses BASE before line 52 defines it. Would crash. Trivial fix but currently likely shadowed by import order. | Trivial |
| 3 | **Extract model hyperparams to config** | n_estimators=600, max_depth=6, etc. are hardcoded in 3+ files (train.py, score_oof.py, tune.py). Should be one config. | Medium |
| 4 | **Add model staleness check** | CI or pre-commit hook that warns when model artifacts are older than the last data refresh. | Small |
| 5 | **Validate alpha factor signal presence** | Currently factors silently return NaN if all their signals are missing. Should warn loudly. | Small |
| 6 | **Weight optimization experiment** | Equal 20% weights are safe but possibly suboptimal. Run a restricted (train-only) optimization to see if IC-weighted outperforms. | Research task |
| 7 | **Consolidate train.py load_data() duplication** | `load_data()` is duplicated across train.py, score_oof.py, run_feature_selection.py with minor differences. Single shared loader would prevent drift. | Medium |
| 8 | **Add horizon_router to composite** | Currently composite weights are horizon-agnostic. A stock screened at 6m horizon still gets equal weight on 3y fraud ML scores. | Design decision |

---

## 6. Files Inspected

| File | Lines | Role |
|------|-------|------|
| `modeling/train.py` | 835 | Core training: IC, feature selection, LightGBM, walk-forward, OOT diagnostic |
| `modeling/score_oof.py` | 382 | Walk-forward OOF scoring — contamination-free ML probabilities |
| `modeling/run_feature_selection.py` | 353 | Standalone PSI/IC/FDR/dedup pipeline |
| `modeling/tune.py` | ~300 | Optuna + CatBoost + calibration + ensemble |
| `modeling/train_regression_model.py` | ~200 | Huber regression for continuous excess return prediction |
| `modeling/alpha.py` | 71 | Orchestrator: computes all alpha columns and writes parquet |
| `alpha/factors/composite.py` | 63 | Weighted blend of 5 factors |
| `alpha/factors/value.py` | 48 | 7-signal value rank |
| `alpha/factors/quality.py` | 48 | 10-signal quality rank |
| `alpha/factors/momentum.py` | 36 | 6-signal momentum rank |
| `alpha/factors/growth.py` | 42 | 6-signal growth rank |
| `alpha/factors/fraud_risk.py` | 60 | 6 fraud signals + 3 ML OOF scores |
| `alpha/factors/__init__.py` | 26 | Registry |
| `alpha/horizon_router.py` | 112 | Months → model key routing + factor group map |
| `alpha/explain.py` | 334 | Plain-English investment thesis generator |
| `models/feature_sets_pruned.json` | — | 27 temporally stable features |
| `models/feature_sets_3y.json` | — | 45 features (full ICIR pipeline output) |
| `models/decision_tree_rules.json` | — | 5 BUY rules, depth-4 tree |

---
