# Production Configuration (Session 42)

## Performance (Walk-Forward, 2013-2024)

| Metric | Full (12yr) | OOS (2021-2024) |
|--------|-------------|-----------------|
| CAGR | +33.5% | +25.9% |
| Sharpe | 1.08 | ~0.8 |
| Volatility | 30.9% | — |
| Worst year | +1.7% | +4.0% (2021) |
| Beat SPY | 50% of years | 25% |
| SPY CAGR | +14.5% | +15.4% |

## Model

- **Algorithm:** LightGBM classifier (binary: beat_local_market_3y)
- **Features:** 22 canonical (3y model), 30 (1y model)
- **Training data:** Clean stocks only (fraud_suspect==0, ROA positive, Beneish < -1.78)
- **Train period:** fiscal_year <= 2020
- **Val AUC:** 0.601 (vs 0.571 for original all-data model)

## Scoring + Portfolio Construction

```
Step 1: Score all US stocks (market_cap >= $50M) with LightGBM → ml_3y
Step 2: Score all with decision tree → tree_prob
Step 3: Apply hard gates:
  - Beneish M-score < -1.78
  - Piotroski F-score >= 3
  - piotroski_roa_pos == 1 (positive return on assets)
  - tree_prob >= 0.45
  - Market = US
  - Market cap >= $50M
Step 4: Rank by ml_3y descending
Step 5: Take top 15
Step 6: Equal-weight, annual rebalance
```

## Why This Config

| Choice | Reason | Alternative tested |
|--------|--------|-------------------|
| Clean training data | Removes value trap bias. Model learns what makes HONEST companies outperform | All-data model picks bankrupt companies |
| ROA positive gate | Effect size 2.0 for separating winners from losers in distressed zone | OCF gate too aggressive (kills 8% CAGR) |
| Tree >= 0.45 | Filters stocks tree disagrees with. 0.35 too loose, 0.50 same picks | Higher thresholds reduce stock count too much |
| Top 15 (not 10 or 20) | Balanced: top10=lumpy, top20=diluted. 15 = best CAGR/Sharpe balance | top10: +35.8% CAGR but 32.5% vol |
| Equal weight (not vol-wt) | Vol-weighting costs ~8% CAGR. Equal keeps simplicity | Vol-wt: Sharpe 1.41 but only 27% CAGR |
| No momentum gate | Costs CAGR without improving OOS. Model already captures momentum | mom>0: smoother but 25.5% CAGR |

## Pros

- Picks are investable (real companies: LOW, DELL, CNC, not bankrupt shells)
- Never negative in full 12-year backtest (worst: +1.7%)
- Clean model has higher predictive power (Val AUC 0.601 vs 0.571)
- Simple: equal-weight, annual rebalance, no complex optimization

## Cons / Risks

- OOS degradation: expect ~25% CAGR not 33% in live trading
- Regime-dependent: underperforms in growth/momentum markets (2021, 2024)
- Annual rebalance = stuck for 12 months even if thesis breaks
- Small OOS sample (4 years) — not statistically robust
- Deep value tilt means you'll look wrong during tech rallies
- 30.9% volatility means 20-30% swings year to year
