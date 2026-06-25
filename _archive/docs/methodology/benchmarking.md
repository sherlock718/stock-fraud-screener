# Model Benchmarking

AUC-ROC comparison of all fraud-detection signals against `fraud_confirmed` labels on the full annual dataset.

---

## Overview

The Benchmarking tab measures how well each signal discriminates confirmed fraud cases from non-fraud cases. All scores are evaluated against the `fraud_confirmed` binary label using standard ranking metrics.

**Dataset:** `historical_dataset_clean.parquet` — annual filings only
**Label:** `fraud_confirmed` (binary 0/1)
**Label source:** Curated from SEC AAER, regulatory enforcement actions, public fraud disclosures
**Sample size:** ~47,000 annual rows, 172 confirmed fraud cases (0.37% prevalence)

---

## Metrics Used

### AUC-ROC (Area Under the Receiver Operating Characteristic Curve)

AUC-ROC measures a classifier's ability to discriminate between positives and negatives across all possible score thresholds.

- **0.50** = no better than random
- **0.60** = weak but meaningful signal
- **0.70+** = strong discriminatory power
- **1.00** = perfect

With 0.37% prevalence, even strong AUC values translate to modest precision at any recall level — see Precision-Recall curves for operational use.

### Average Precision (AP)

AP is the area under the Precision-Recall curve, weighted by precision at each recall level. It is a stricter metric than AUC-ROC under class imbalance. A baseline random classifier achieves AP = prevalence rate ≈ 0.0037.

---

## Classical Baselines

### Beneish M-Score

**AUC-ROC: ~0.59 | Direction: Direct (higher = more suspect)**

The Beneish M-Score is an 8-variable model estimating the probability of earnings manipulation. The traditional threshold of −2.22 was calibrated for US GAAP companies. Our AUC measures rank-order discrimination across all markets.

Components contributing to the M-Score:

| Component | Abbreviation | Description |
|-----------|:------------:|-------------|
| Days Sales Outstanding Ratio | DSRI | Receivables growth relative to revenue |
| Gross Margin Index | GMI | Margin deterioration signal |
| Asset Quality Index | AQI | Non-current asset inflation |
| Sales Growth Index | SGI | Revenue growth above historical trend |
| Depreciation Index | DEPI | Reduced depreciation as % of PP&E |
| SGA Expense Index | SGAI | Overhead inflation |
| Leverage Index | LVGI | Debt increase ratio |
| Total Accruals to Total Assets | TATA | Net income minus OCF scaled by assets |

### Altman Z-Score

**AUC-ROC: ~0.58 | Direction: Inverted (lower Z = higher distress)**

The Altman Z-Score predicts bankruptcy risk, not fraud directly. It is included because financial distress and fraud are correlated — many frauds are concealment of underlying solvency problems.

```
Z = 1.2×X₁ + 1.4×X₂ + 3.3×X₃ + 0.6×X₄ + 1.0×X₅
```

- **Z < 1.81**: Distress zone
- **1.81 ≤ Z < 2.99**: Grey zone
- **Z ≥ 2.99**: Safe zone

For AUC computation, the score is **inverted** (scored as −Z) so that higher = more risk.

### Ohlson O-Score / Bankruptcy Probability

**AUC-ROC: ~0.65 | Direction: Inverted (healthy companies more likely to be confirmed fraud)**

!!! warning "Counterintuitive Direction"
    The Ohlson bankruptcy probability is **inversely** correlated with confirmed fraud in our dataset. Companies with *low* bankruptcy probability are *more* likely to be confirmed fraud cases. This is consistent with the nature of accounting fraud: fraudsters maintain an appearance of financial health precisely to avoid detection. The score is inverted for AUC computation.

### Piotroski F-Score

**AUC-ROC: ~0.44 | Direction: Inverted (weaker fundamentals = more fraud risk)**

The Piotroski F-Score is a 9-point fundamental strength score. Lower scores indicate weaker fundamentals, which correlates weakly with fraud. At AUC ~0.44, this is below random on our dataset — weak fundamentals alone do not predict confirmed fraud reliably.

### Sloan Accruals Ratio

**AUC-ROC: ~0.55 | Direction: Direct (higher accruals = more manipulation risk)**

The Sloan accruals ratio (net income minus operating cash flow, scaled by total assets) measures earnings quality. Higher positive accruals indicate income is not backed by cash — a classic earnings manipulation signal.

```
Sloan Accruals = (Net Income − Operating CFO − Investing CFO) / Average Total Assets
```

---

## Proprietary Composite Scores

### Fraud Score Composite

**AUC-ROC: ~0.54 | Direction: Direct**

Our ensemble fraud probability combining all sub-models:
- Accounting manipulation sub-score
- Dilution fraud sub-score
- Earnings quality sub-score
- Financial distress sub-score
- Governance fraud sub-score

Each sub-score is independently calibrated before aggregation. The composite is weighted by SHAP importance on the training set.

### Fraud Score Accounting

**AUC-ROC: ~0.57 | Direction: Direct**

The accounting manipulation component specifically, driven by Beneish-type accruals features and revenue recognition anomalies.

---

## ML Model Evaluation

### LightGBM Models (1y / 3y / 5y horizons)

Three gradient boosting models trained on different prediction horizons:

- **1-year**: Predicts fraud flags that materialize within 1 fiscal year
- **3-year**: Medium-term signals; higher recall for gradual frauds
- **5-year**: Long-horizon signals; better for structural red flags

**Feature count:** ~35 ICIR-selected features per model

**Caveat: In-Sample Evaluation**

The AUCs in the benchmarking tab are computed in-sample (all annual rows), not on the original holdout split. The held-out training evaluation AUCs were:

| Horizon | Original Holdout AUC |
|:-------:|:--------------------:|
| 1y      | 0.7764               |
| 3y      | 0.7949               |
| 5y      | 0.8603               |

The live benchmarking tab shows lower AUCs because (a) some features present at training time are absent from the current dataset version, filled with 0 or train medians, and (b) in-sample evaluation is a different (less rigorous) estimate than the original holdout.

**Missing features at inference time:**

| Feature | Filled With |
|---------|-------------|
| `piotroski_shares_ok` | Train median |
| `financing_cashflow_to_assets` | Train median |
| `piotroski_delta_gm` | Train median |
| `piotroski_f_score_9` | Train median |
| `piotroski_delta_at` | Train median |

---

## Score Direction Reference

| Model | Higher = | Invert for AUC? |
|-------|:--------:|:---------------:|
| Beneish M-Score | More suspect | No |
| Altman Z-Score | Less distress | Yes |
| Ohlson Bankruptcy Prob | Less distress | Yes |
| Piotroski F-Score | Stronger fundamentals | Yes |
| Sloan Accruals | More manipulation | No |
| Fraud Score Composite | Higher risk | No |
| Fraud Score Accounting | Higher accounting risk | No |
| LightGBM (all horizons) | Higher fraud probability | No |

---

## Precision-Recall Interpretation

With 0.37% fraud prevalence, precision at any recall threshold is very low for all models. This is expected — fraud is rare by design.

Operational use: use the Precision-Recall curve to select a threshold for your desired recall target. For example, if you want to catch 50% of frauds (recall = 0.5), read off the precision at that point — that tells you how many false positives you will investigate per true positive found.

**Random baseline AP = 0.0037** (the fraud prevalence rate). Any model with AP significantly above this is extracting useful signal.

---

## Using the Benchmarking Tab

1. View the **AUC bar chart** to rank all models by discrimination power
2. Check the **Full Results Table** for AP scores and direction notes
3. Use the **Precision-Recall curves** to select an operational threshold
4. Use the **ROC curves** to compare sensitivity/specificity trade-offs
5. Read the **Methodology & Caveats** expander for a full disclosure of limitations

The benchmarking tab auto-refreshes its cache hourly (`ttl=3600`). The parquet file is read directly, independent of the main app data pipeline.
