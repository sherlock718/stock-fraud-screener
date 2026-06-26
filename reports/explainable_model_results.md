# Explainable Decision Tree Model — Session 24

## Summary — Three Models Compared

| Metric | LightGBM | Tree Only | Agreement (LightGBM+Tree) |
|--------|:---:|:---:|:---:|
| Sharpe | 1.124 | 2.238 | 1.138 |
| CAGR | +33.80% | +29.22% | +34.02% |
| Excess CAGR | +16.70% | +12.12% | +16.93% |
| Hit Rate | 73.9% | 74.7% | 73.9% |
| Max Drawdown | — | 0.00% | 0.00% |

## Decision

**Agreement filter adopted as primary strategy:**
- LightGBM provides ranking power (higher CAGR)
- Tree provides explainability gate (every pick has a human-readable reason)
- Only stocks where BOTH models agree are selected

Every screener output includes the tree rule that justified inclusion.

## Methodology

### Agreement Filter
1. Score all stocks with LightGBM (ranking)
2. Score all stocks with Decision Tree (probability)
3. Filter: keep only stocks where tree probability >= 0.35
4. Rank filtered stocks by LightGBM probability, take top 20

### Models
- **LightGBM**: `n_estimators=200, max_depth=4, learning_rate=0.05`
- **Decision Tree**: `max_depth=4, min_samples_leaf=50`
- **Features**: 27 (from 27 pruned stable set, IC-filtered walk-forward)
- **Train**: Expanding window, all data before each test year
- **Test**: Walk-forward scoring 2019-2024
- **Portfolio**: Composite strategy + sector caps + tiered slippage

## Top BUY Rules (from final tree trained on full pre-test data)

- Rule 1: IF sales_to_price > 0.0209 AND value_composite > 0.6534 AND entry_price <= 2.8527 AND book_to_market > 0.9899 → BUY (prob=74.5%, n=170)
- Rule 2: IF sales_to_price <= 0.0209 AND entry_price <= 248.1426 AND fcf_yield <= -0.0986 → BUY (prob=61.2%, n=56)
- Rule 3: IF sales_to_price > 0.0209 AND value_composite > 0.6534 AND entry_price <= 2.8527 AND book_to_market <= 0.9899 → BUY (prob=61.2%, n=156)
- Rule 4: IF sales_to_price > 0.0209 AND value_composite > 0.6534 AND entry_price > 2.8527 AND price_to_52w_high > 0.7152 → BUY (prob=56.5%, n=4425)
- Rule 5: IF sales_to_price > 0.0209 AND value_composite <= 0.6534 AND vol_prior_6m <= 0.5343 AND price_to_52w_high > 0.8928 → BUY (prob=54.0%, n=4091)

## Annual Returns — Agreement Filter

| Year | Agreement Port% | SPY% | Excess% | Picks |
|------|:---:|:---:|:---:|:---:|
| 2019 | +79.33% | +31.22% | +48.11% | 20 |
| 2020 | +57.05% | +18.33% | +38.72% | 20 |
| 2021 | +3.86% | +28.73% | -24.87% | 20 |
| 2022 | +30.68% | -18.18% | +48.85% | 20 |
| 2023 | +24.03% | +26.18% | -2.14% | 20 |
| 2024 | +22.24% | +24.89% | -2.65% | 19 |

## Full Tree Structure

```
|--- sales_to_price <= 0.02
|   |--- entry_price <= 248.14
|   |   |--- fcf_yield <= -0.10
|   |   |   |--- class: 1
|   |   |--- fcf_yield >  -0.10
|   |   |   |--- fcf_yield <= -0.00
|   |   |   |   |--- class: 0
|   |   |   |--- fcf_yield >  -0.00
|   |   |   |   |--- class: 0
|   |--- entry_price >  248.14
|   |   |--- gross_profit_to_assets <= 0.47
|   |   |   |--- value_composite <= 0.24
|   |   |   |   |--- class: 0
|   |   |   |--- value_composite >  0.24
|   |   |   |   |--- class: 0
|   |   |--- gross_profit_to_assets >  0.47
|   |   |   |--- class: 0
|--- sales_to_price >  0.02
|   |--- value_composite <= 0.65
|   |   |--- vol_prior_6m <= 0.53
|   |   |   |--- price_to_52w_high <= 0.89
|   |   |   |   |--- class: 0
|   |   |   |--- price_to_52w_high >  0.89
|   |   |   |   |--- class: 1
|   |   |--- vol_prior_6m >  0.53
|   |   |   |--- entry_price <= 18.98
|   |   |   |   |--- class: 0
|   |   |   |--- entry_price >  18.98
|   |   |   |   |--- class: 0
|   |--- value_composite >  0.65
|   |   |--- entry_price <= 2.85
|   |   |   |--- book_to_market <= 0.99
|   |   |   |   |--- class: 1
|   |   |   |--- book_to_market >  0.99
|   |   |   |   |--- class: 1
|   |   |--- entry_price >  2.85
|   |   |   |--- price_to_52w_high <= 0.72
|   |   |   |   |--- class: 0
|   |   |   |--- price_to_52w_high >  0.72
|   |   |   |   |--- class: 1
```

