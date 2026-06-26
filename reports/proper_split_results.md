# Session 22: Proper Train/Validate/Test Split — Results

## Temporal Split Design

| Period | Years | Purpose | Rows |
|--------|-------|---------|------|
| Train | 2008-2014 | Feature selection (ICIR + BH FDR) | ~8,220 |
| Validate | 2015-2018 | PSI stability check | ~12,600 |
| Test | 2019-2024 | Final OOS metrics (NEVER used for decisions) | ~25,000 |

**Why this split:**
- 7 train years → enough IC observations for robust ICIR + BH FDR multiple-comparison correction
- Validation covers: flat (2015), value recovery (2016), melt-up (2017), vol shock (2018)
- Test covers: late-cycle (2019), COVID crash+recovery (2020), speculative frenzy (2021), rate-hike bear (2022), AI rally (2023), broadening (2024)
- 2025 excluded: only 6% forward_return_1y coverage (holding period incomplete)

---

## Test Period Results (2019-2024)

| Metric | Value |
|--------|-------|
| **CAGR** | +31.91% |
| SPY CAGR | +17.09% |
| **Excess CAGR** | +14.81% |
| **Sharpe** | 0.954 |
| Hit Rate | 70.5% |
| Max Drawdown | 0.00% (annual frequency) |
| Features used | 43 (selected on train only) |

### Annual Breakdown

| Year | Portfolio | SPY | Excess | Picks |
|------|-----------|-----|--------|-------|
| 2019 | +87.92% | +31.22% | +56.70% | 20 |
| 2020 | +48.99% | +18.33% | +30.66% | 20 |
| 2021 | +1.84% | +28.73% | -26.89% | 20 |
| 2022 | +28.41% | -18.18% | +46.58% | 20 |
| 2023 | +20.48% | +26.18% | -5.70% | 20 |
| 2024 | +19.41% | +24.89% | -5.48% | 19 |

**Pattern:** Strategy excels in value/quality regimes (2019, 2020, 2022) and underperforms in momentum/growth regimes (2021, 2023-2024). This is consistent with value+quality factor exposure.

---

## Feature Selection (Train-Only)

43 features survived the full pipeline (PSI → IC+FDR → ICIR → Dedup) using ONLY 2008-2014 data:

```
entry_price, sales_to_price, price_to_book, shares_growth, goodwill_to_assets,
value_x_momentum, earnings_yield, value_composite, altman_x4, book_to_market,
vol_prior_6m, debt_to_assets_sector_pct, vol_prior_36m, sic_2digit,
quarterly_positive_rev_frac, piotroski_shares_ok, financing_cashflow_to_assets,
piotroski_ocf_pos, fcf_yield, enterprise_value, price_to_52w_high, beneish_aqi,
ps_ratio_sector_pct, days_sales_outstanding, momentum_consistency,
piotroski_f_score_9, altman_x5, value_x_quality, filing_lag_days, montier_c1,
quality_x_momentum, capex_intensity, other_noncurrent_assets, momentum_12m_rank,
gross_profit_to_assets, ocf_to_assets, montier_c3, cash_conversion,
montier_c_score, ocf_to_debt, roe, piotroski_delta_at, small_x_quality
```

---

## Feature Stability Check

| Window | Features Selected |
|--------|-------------------|
| Primary (2008-2014) | 43 |
| Shifted (2010-2016) | 38 |
| **Overlap** | **27/54 = 50.0%** |

### Stable features (survived both windows):
```
altman_x4, altman_x5, beneish_aqi, book_to_market, capex_intensity,
debt_to_assets_sector_pct, entry_price, fcf_yield, financing_cashflow_to_assets,
goodwill_to_assets, gross_profit_to_assets, momentum_consistency, ocf_to_assets,
ocf_to_debt, other_noncurrent_assets, piotroski_delta_at, piotroski_f_score_9,
piotroski_ocf_pos, piotroski_shares_ok, price_to_52w_high, ps_ratio_sector_pct,
quarterly_positive_rev_frac, sales_to_price, shares_growth, value_composite,
value_x_momentum, vol_prior_6m
```

**Interpretation:** 50% Jaccard overlap is moderate — the core value/quality/accounting features are stable (value_composite, piotroski, fcf_yield, book_to_market), but peripheral features rotate depending on regime. This is normal for fundamental factor models.

---

## Comparison: Train-Only vs Full-History (Biased) Feature Set

| | Full-History (biased) | Train-Only (unbiased) |
|---|---|---|
| Features | 45 | 43 |
| Overlap | 27 shared features | |
| Dropped by restricting | 18 features | |
| New (not in biased set) | | 16 features |

The unbiased set drops some features that only look good in hindsight (e.g., roa_volatility_5yr, working_capital) and discovers features that were stable in early data but overlooked by full-history ranking (e.g., montier scores, filing_lag_days, shares_growth).

---

## Gate Decision

| Criterion | Result |
|-----------|--------|
| Test-period Sharpe | **0.954** |
| Threshold | ≥ 0.8 → PASS |
| Feature stability | 50% overlap (acceptable) |

### **GATE: PASS**

**Sharpe 0.954 ≥ 0.8 — the signal is real.** Proceed to sessions 23-25 (prune, explain, overlay).

Previous reported Sharpe (1.37 with full-history feature selection) was overstated by ~30%, but the underlying signal remains commercially viable even with unbiased feature selection.

---

## Key Takeaway

The look-ahead bias in feature selection inflated Sharpe from ~0.95 to ~1.37 (a ~44% overstatement). The honest, unbiased test-period Sharpe of 0.954 still clears the gate comfortably. The strategy works because:
1. Value+quality factors have genuine predictive power (7 years of train data confirms)
2. The walk-forward ML properly adapts year-by-year
3. Core features (value_composite, piotroski, book_to_market) are temporally stable
