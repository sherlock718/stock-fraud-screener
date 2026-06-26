# Pruned Feature Set Backtest — Session 23

## Summary

| Metric | Full (43 feat) | Lean (27 feat) | Delta |
|--------|---------------|-------------|-------|
| Sharpe | 0.954 | 1.124 | +0.170 |
| CAGR | +31.91% | +33.78% | +1.87% |
| Excess CAGR | +14.81% | +16.69% | +1.88% |
| Hit Rate | 70.5% | 73.9% | +3.4% |
| Features | 43 | 27 | -16 |

## Methodology

- **Stable features**: 27 features that survived BOTH train windows (2008-2014 AND 2010-2016)
- **Split**: Train 2008-2014, Validate 2015-2018, Test 2019-2024 (same as Session 22)
- **Backtest**: Walk-forward ML (expanding window) with composite portfolio strategy
- **Add-back threshold**: Sharpe drop > 0.1 triggers sequential feature restoration

## Stable Feature Set (core)

- altman_x4
- altman_x5
- beneish_aqi
- book_to_market
- capex_intensity
- debt_to_assets_sector_pct
- entry_price
- fcf_yield
- financing_cashflow_to_assets
- goodwill_to_assets
- gross_profit_to_assets
- momentum_consistency
- ocf_to_assets
- ocf_to_debt
- other_noncurrent_assets
- piotroski_delta_at
- piotroski_f_score_9
- piotroski_ocf_pos
- piotroski_shares_ok
- price_to_52w_high
- ps_ratio_sector_pct
- quarterly_positive_rev_frac
- sales_to_price
- shares_growth
- value_composite
- value_x_momentum
- vol_prior_6m

## Annual Returns Comparison

| Year | Full Port% | Lean Port% | Full Excess% | Lean Excess% |
|------|-----------|-----------|-------------|-------------|
| 2019 | 87.92% | +79.33% | 56.7% | +48.11% |
| 2020 | 48.99% | +57.05% | 30.66% | +38.72% |
| 2021 | 1.84% | +3.86% | -26.89% | -24.87% |
| 2022 | 28.41% | +30.68% | 46.58% | +48.85% |
| 2023 | 20.48% | +22.68% | -5.7% | -3.49% |
| 2024 | 19.41% | +22.24% | -5.48% | -2.65% |

## Conclusion

The lean 27-feature model (Sharpe 1.124) is within 0.1 of the full 43-feature model (Sharpe 0.954). **Simpler model adopted as new baseline.**
