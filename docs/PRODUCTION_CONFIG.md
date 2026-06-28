# Production Configuration (Session 43)

## Performance (Walk-Forward, 2013-2023, $10B cap)

| Metric | Full (11yr) | OOS (2021-2023) |
|--------|-------------|-----------------|
| CAGR | +34.7% | +43.5% |
| Sharpe | 0.97 | 1.68 |
| MaxDD | -17.2% | — |
| Worst year | +7.4% (2019) | +26.7% (2021) |
| Negative years | 0/11 | 0/3 |
| Hit rate | 80.3% | — |
| SPY CAGR | +13.6% | +10.2% |
| After 150bps costs | +32.5% | — |

## Model

- **Ranking:** LightGBM regressor (target: forward_return_3y magnitude)
- **Gate:** Decision tree classifier (tree_prob >= 0.55)
- **Features:** 28 canonical (3y model, selected via IC/ICIR on train-only)
- **Training data:** Clean stocks only (fraud_suspect==0, ROA positive, Beneish < -1.78)
- **Train period:** fiscal_year <= 2023 (expanding window in walk-forward)
- **Why regression over classification:** Regression ranks by HOW MUCH a stock beats market, not just probability. +3% CAGR improvement, same risk profile when combined with strict tree gate.

## Scoring + Portfolio Construction

```
Step 1: Apply hard gates:
  - Market = US
  - Market cap: $50M – $10B
  - Beneish M-score < -1.78
  - Piotroski F-score >= 3
  - piotroski_roa_pos == 1
  - Altman Z-score > 1.0
  - ps_ratio_sector_pct <= 0.70 (not overpriced vs sector)
Step 2: Score survivors with decision tree → tree_prob
Step 3: Gate: tree_prob >= 0.55 (strict agreement)
Step 4: Score with LightGBM regression → predicted 3y return
Step 5: ADTV liquidity filter (position < 1% of daily volume)
Step 6: Rank by reg_3y descending, take top 15
Step 7: Equal-weight, annual rebalance
```

## Why This Config

| Choice | Reason | Alternative tested |
|--------|--------|-------------------|
| Regression ranking | +3% CAGR vs classifier. Captures return magnitude, not just direction | Classifier: +30.9% CAGR (good but lower) |
| Tree >= 0.55 | Strict quality control on regression picks. 0 negative years. | 0.45: +30.9% CAGR but higher MaxDD |
| $10B cap | Avoids mega-cap institutional territory. Minimal cost (-0.2% CAGR vs no cap) | $5B too tight (-8% CAGR). No cap: +34.9% |
| Clean training | Regression on honest companies only. Prevents learning value-trap patterns | All-data: trains on bankruptcies |
| Value gate (P/S <=70th) | Removes overpriced growth that regression might chase | Without: more volatile, similar CAGR |
| Top 15 equal-weight | Balanced concentration. Regression less concentrated than classifier | Top 10 too lumpy |

## Pros

- 0 negative years in 11-year walk-forward backtest
- Outperformance NOT dependent on outliers (excl top 1%: still +26.6% CAGR)
- Survives heavy transaction costs (150bps: +32.5% CAGR)
- Picks are real small/mid-cap companies (median $400-700M market cap)
- 80.3% hit rate — 4 out of 5 years beat the market
- Low turnover impact: 71% annual replacement, costs absorbed

## Cons / Risks

- MaxDD -17.2% (higher than classifier's -10%)
- Regression may chase small-cap outliers in live trading (unverifiable until tried)
- Only 11 years of walk-forward data — not statistically definitive
- Deep value tilt: will underperform in growth/momentum markets
- $10B cap means missing some mid-cap winners ($10-50B range)
- Model trained on historical patterns; regime change could invalidate
