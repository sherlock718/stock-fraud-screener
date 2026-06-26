# Feature Ablation Results

**Horizon**: 3y | **Strategy**: Composite | **Date**: 2026-06-26

**Baseline**: Sharpe=1.373, CAGR=39.42%, Hit Rate=88.3%

## Impact Table (sorted by Sharpe impact)

| # | Feature | Sharpe Δ | CAGR Δ (%) | Hit Δ (%) | Verdict |
|---|---------|----------|------------|-----------|---------|
| 1 | value_composite | -0.1150 | +4.71 | +2.7 | 🔴 LOAD-BEARING |
| 2 | financing_cashflow_to_assets | -0.0370 | +0.97 | +0.0 | ⚪ Neutral |
| 3 | asset_turnover | -0.0250 | -0.41 | -0.3 | ⚪ Neutral |
| 4 | quarterly_positive_rev_frac | -0.0240 | -0.38 | -0.3 | ⚪ Neutral |
| 5 | pb_ratio_sector_pct | -0.0160 | -0.32 | -0.3 | ⚪ Neutral |
| 6 | quality_x_momentum | -0.0090 | -0.21 | +0.4 | ⚪ Neutral |
| 7 | debt_to_assets_sector_pct | -0.0080 | -0.06 | +0.0 | ⚪ Neutral |
| 8 | piotroski_ocf_pos | -0.0020 | -0.04 | -0.3 | ⚪ Neutral |
| 9 | beneish_aqi | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 10 | roa_volatility_5yr | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 11 | gross_profit_to_assets | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 12 | accruals_avg_3y | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 13 | roe | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 14 | other_noncurrent_assets | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 15 | cash_conversion | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 16 | earnings_stability_5yr | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 17 | earnings_qoq_mean | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 18 | debt_to_assets | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 19 | ev_ebitda_sector_pct | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 20 | piotroski_roa_pos | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 21 | working_capital | +0.0000 | +0.00 | +0.0 | ⚪ Neutral |
| 22 | price_to_52w_high | +0.0030 | +0.08 | +0.0 | ⚪ Neutral |
| 23 | fcf_yield | +0.0060 | +0.12 | +0.0 | ⚪ Neutral |
| 24 | vol_rank_12m | +0.0070 | -0.14 | +0.0 | ⚪ Neutral |
| 25 | sga | +0.0090 | -0.22 | +0.0 | ⚪ Neutral |
| 26 | entry_price | +0.0090 | +0.19 | +0.0 | ⚪ Neutral |
| 27 | altman_x3 | +0.0130 | +0.33 | +0.0 | ⚪ Neutral |
| 28 | shares_dilution | +0.0170 | +0.39 | +0.0 | ⚪ Neutral |
| 29 | earnings_yield | +0.0180 | +0.31 | +0.0 | ⚪ Neutral |
| 30 | operating_margin_sector_pct | +0.0210 | +0.26 | +0.4 | 🟢 Prune candidate |
| 31 | piotroski_f_score_9 | +0.0220 | +0.33 | +0.4 | 🟢 Prune candidate |
| 32 | vol_prior_6m | +0.0230 | +0.20 | +0.0 | 🟢 Prune candidate |
| 33 | roa_sector_pct | +0.0250 | +0.10 | +0.4 | 🟢 Prune candidate |
| 34 | capex_intensity | +0.0250 | +0.37 | +0.4 | 🟢 Prune candidate |
| 35 | vol_prior_36m | +0.0260 | +0.66 | +0.4 | 🟢 Prune candidate |
| 36 | piotroski_shares_ok | +0.0260 | +0.47 | +0.4 | 🟢 Prune candidate |
| 37 | roa | +0.0330 | +0.46 | +0.0 | 🟢 Prune candidate |
| 38 | ocf_to_assets | +0.0430 | +0.55 | +0.7 | 🟢 Prune candidate |
| 39 | ps_ratio | +0.0490 | +0.49 | +0.7 | 🟢 Prune candidate |
| 40 | price_to_book | +0.0530 | +1.11 | +0.4 | 🟢 Prune candidate |
| 41 | small_x_quality | +0.0570 | +0.76 | +1.0 | 🟢 Prune candidate |
| 42 | ps_ratio_sector_pct | +0.0860 | -0.45 | +0.4 | 🟢 Prune candidate |
| 43 | value_x_momentum | +0.1020 | -0.09 | +0.7 | 🟢 Prune candidate |
| 44 | altman_x4 | +0.1100 | +0.07 | +0.4 | 🟢 Prune candidate |
| 45 | book_to_market | +0.1210 | +0.18 | +0.0 | 🟢 Prune candidate |

## Interpretation

- **LOAD-BEARING** (Sharpe drop > 0.10): Removing this feature significantly hurts performance.
- **Important** (Sharpe drop 0.05–0.10): Contributes meaningfully but not critical alone.
- **Prune candidate** (Sharpe improves > 0.02): Removing improves metrics — likely noise/overfitting.
- **Neutral** (within ±0.05): Feature has minimal marginal impact given other features present.
