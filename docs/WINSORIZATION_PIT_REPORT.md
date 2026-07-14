# Winsorization PIT Report — Before/After Comparison

**Date:** 2026-07-14  
**Scope:** Issue 3 — Full-sample winsorization leakage

---

## 1. Transformations Identified

The only data-derived quantile transformation is the `winsorize()` function in
`pipeline/step5_compute_features.py` (lines 61-65), applied to 40+ feature columns.

Other `.clip()` calls use fixed domain-knowledge constants (e.g., `.clip(-0.9, 10)` for growth
rates, `.clip(-50, 50)` for Z-scores) and are NOT leaky — they don't derive bounds from data.

The `rank(pct=True)` calls in composite scores are already grouped by `(fiscal_year, market)`,
making them approximately PIT-correct.

---

## 2. Leakage Mechanism

The global `winsorize()` computes `series.quantile(0.01)` and `series.quantile(0.99)` over the
**entire dataset** (2008–2027), then clips all observations. This means:

- Bounds for 2010 observations incorporate 2024 data
- Adding new years changes the bounds retroactively
- Training values are not stable across model retraining runs

---

## 3. Before/After Comparison

### Feature Value Differences (Global vs PIT Expanding Window)

| Feature | N Valid | Global Clips | PIT Clips | % Values Differ | Max Δ |
|---------|---------|-------------|-----------|----------------|-------|
| pe_ratio | 37,581 | 752 | 20,706 | 2.02% | 199,000 |
| roa | 53,480 | 7 | 4,761 | 0.08% | 4.45 |
| roe | 51,271 | 0 | 7,706 | 1.54% | 8.93 |
| sloan_accruals | 48,338 | 968 | 10,810 | 2.12% | 5.91 |
| beneish_m_score | 58,190 | 1,164 | 1,262 | 2.21% | 25.49 |
| altman_z_score | 58,190 | 1,164 | 1,112 | 2.12% | 24.26 |
| revenue_growth | 42,671 | 854 | 16,161 | 2.22% | 14.74 |
| assets_growth | 45,453 | 910 | 13,560 | 2.49% | 6.44 |

**Average across all 28 columns: 1.84% of values differ.**

### Rank Correlation (Spearman ρ)

| Feature | ρ (global vs PIT) | Top-100 Overlap |
|---------|------------------|-----------------|
| pe_ratio | 0.999996 | 65/100 |
| roa | 0.999989 | 97/100 |
| sloan_accruals | 0.999996 | 21/100 |
| beneish_m_score | 0.999995 | 13/100 |
| revenue_growth | 0.999996 | 56/100 |
| altman_z_score | 0.999997 | 13/100 |

**Interpretation:** Rankings are nearly identical (ρ > 0.99999), but extreme-tail membership
(top/bottom 100) shifts significantly for fraud-critical features (sloan_accruals, beneish_m_score).

### Critical Test: Does Future Data Change Historical Values?

| Feature | Bounds@2015 | Bounds@Global | 2015 Obs Changed |
|---------|-------------|---------------|-----------------|
| roa | [-7.47, 0.28] | [-7.47, 0.28] | 8.1% |
| sloan_accruals | [-7.93, 0.94] | [-8.81, 0.61] | **50.9%** |
| beneish_m_score | [-28.64, 15.73] | [-39.33, 13.51] | 2.3% |
| revenue_growth | [-0.91, 14.67] | [-0.94, 15.65] | 19.3% |

**Conclusion:** For `sloan_accruals`, over half of 2015 training values change when 2016+ data
is added. This is genuine look-ahead contamination of the training set.

### Impact on OOS (2022-2024) Observations

For recent OOS data, the effect is small (0-2.8%) because the expanding window at 2022+
has nearly the full dataset available. The leakage primarily affects **early training years**.

---

## 4. Materiality Assessment

**The effect is methodologically significant but has bounded practical impact:**

- Rank ordering is essentially preserved (ρ > 0.99999)
- The model's LightGBM splits will be affected for early-year observations
- Fraud-critical tail membership changes by up to 50% for `sloan_accruals` in early years
- For a walk-forward backtest starting in 2015, this represents contaminated training data

**Verdict: FIX IS JUSTIFIED** on methodological grounds — training data must be stable
and independent of future observations, regardless of whether the practical impact
on final CAGR/Sharpe is modest.

---

## 5. Implementation

**Changed:** `pipeline/step5_compute_features.py`
- Added `winsorize_pit(df, col)` — expanding-window winsorization grouped by fiscal_year
- The winsorization loop now calls `winsorize_pit(df, col)` instead of `winsorize(df[col])`
- Original `winsorize()` function preserved for comparison and backward compatibility

**New file:** `pipeline/winsorize_pit.py`
- `winsorize_expanding()` — general expanding-window implementation
- `winsorize_global()` — preserved global version
- `winsorize_training_only()` — for model training (bounds from train fold only)
- `compare_winsorization_methods()` — comparison utility

**Tests:** `tests/pipeline/test_winsorize_pit.py`
- `test_future_data_cannot_change_historical_values` — core invariant
- `test_expanding_window_uses_only_past_data`
- `test_global_winsorize_is_not_pit_safe` — proves the bug
- `test_training_only_bounds_frozen`

---

## 6. Notes

- The PIT method clips MORE values for early years (tighter bounds from smaller samples).
  This is correct behavior — in 2010, extreme values hadn't been observed yet.
- Grouping by `fiscal_year` is an approximation. Some filings within a fiscal year may not
  have been public at the rebalance date. A more precise approach would use `filed_date`
  directly, but the fiscal_year approximation bounds the error to ~6 months.
- The existing `data/historical_dataset_clean.parquet` retains global-winsorized values.
  A full step5 re-run produces the PIT-correct version for comparison.
