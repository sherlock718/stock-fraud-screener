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

### Backtest Results (Global vs PIT Winsorization)

| Metric | Global | PIT | Δ |
|--------|--------|-----|---|
| CAGR % | 32.62 | 32.62 | 0.00 |
| Sharpe | 1.35 | 1.35 | 0.00 |
| Max Drawdown % | 0.0 | 0.0 | 0.00 |
| Hit Rate % | 88.4 | 88.4 | 0.00 |
| Annual Turnover % | 97.8 | 97.8 | 0.00 |

**Result: IDENTICAL.** All 15 years produce the same returns, portfolio membership is unchanged.

### Why No Backtest Difference

1. **Composite strategy** uses rank-based scoring (quality_composite, value_composite) which
   ranks within fiscal_year × market groups. Winsorization preserves ordering → ranks unchanged.
2. **ML model** (LightGBM, 28 features) only uses 2 winsorized features (`pb_ratio`, `ps_ratio`).
   The remaining 26 features are sector-percentiles, flags, or non-winsorized inputs.
3. **Winsorization preserves the middle 96-98%** of observations. Only tail values change,
   and these rarely cross LightGBM split thresholds.

### Fraud-Tail Membership Changes

| Signal | Threshold | Global Flagged | PIT Flagged | Membership Δ |
|--------|-----------|---------------|-------------|--------------|
| Beneish M-score > -1.78 | -1.78 | 6,614 | 6,608 | 6 (0.1%) |
| Sloan accruals top 10% | 90th pct | 4,834 | 4,834 | 0 (0.0%) |
| Altman Z-score < 1.81 | 1.81 | 27,198 | 27,198 | 0 (0.0%) |

### Expanding Bounds Convergence

PIT bounds converge to global bounds as more data accumulates:
- 2010Q2: sloan_accruals bounds [-0.28, 0.07] (from 178 observations)
- 2012Q2: bounds [-1.28, 0.28] (from 1,948 observations)
- Global: bounds [-8.81, 0.61] (from 48,338 observations)

For recent years (2020+), PIT ≈ global. The fix primarily affects early-year training data.

### Verdict

**The fix has ZERO practical impact on current production backtest results**, but is
**methodologically correct** and justified because:
- Training data must be stable and independent of future observations (proven by test)
- A future model retrained on PIT-winsorized features would see different early-year
  distributions, potentially affecting feature selection and split points
- The fix costs nothing in performance and eliminates a category of subtle bias

---

## 5. Implementation

**Changed:** `pipeline/step5_compute_features.py`
- Added `winsorize_pit(df, col)` — uses `filed_date` (not fiscal_year) as the availability gate
- Groups observations by filing quarter (calendar quarter of filed_date)
- For each quarter Q, bounds come from all observations with `filed_date < start_of_Q`
- The winsorization loop now calls `winsorize_pit(df, col)` instead of `winsorize(df[col])`
- Original `winsorize()` function preserved for comparison and backward compatibility
- Falls back to fiscal_year expanding window when filed_date is unavailable

**New file:** `pipeline/winsorize_pit.py`
- `winsorize_expanding()` — fiscal_year expanding window (documented approximation)
- `winsorize_by_filed_date()` — strict PIT using actual filing dates
- `winsorize_global()` — preserved global version
- `winsorize_training_only()` — for model training (bounds from train fold only)
- `compare_winsorization_methods()` — comparison utility

**Tests:** `tests/pipeline/test_winsorize_pit.py`
- `test_future_data_cannot_change_historical_values` — core invariant
- `test_expanding_window_uses_only_past_data`
- `test_global_winsorize_is_not_pit_safe` — proves the original bug
- `test_training_only_bounds_frozen`
- `test_only_prior_filings_inform_bounds` — proves filed_date correctness
- `test_filed_date_not_fiscal_year_determines_availability` — same fiscal_year, different bounds
- `test_adding_future_filings_does_not_change_past_clips` — stability under new filings

---

## 6. Notes

- The PIT method clips MORE values for early years (tighter bounds from smaller samples).
  This is correct behavior — in 2010, extreme values hadn't been observed yet.
- Grouping by `fiscal_year` is an approximation. Some filings within a fiscal year may not
  have been public at the rebalance date. A more precise approach would use `filed_date`
  directly, but the fiscal_year approximation bounds the error to ~6 months.
- The existing `data/historical_dataset_clean.parquet` retains global-winsorized values.
  A full step5 re-run produces the PIT-correct version for comparison.
