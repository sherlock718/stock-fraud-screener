# Production Configuration (Session 47b)

## Performance (Walk-Forward, 2013-2023, $10B cap, momentum gate)

| Metric | Current (w/ momentum gate) | Previous (w/o momentum) |
|--------|---------------------------|------------------------|
| CAGR | +31.5% | +34.7% |
| Sharpe | **1.45** | 0.97 |
| MaxDD | **-8.1%** | -17.2% |
| Excess vs SPY | +17.9% | +21.1% |
| Beta | -0.18 | — |
| SPY CAGR | +13.6% | +13.6% |

## Model

- **Ranking:** LightGBM regressor (target: forward_return_3y magnitude)
- **Gate:** Decision tree classifier (tree_prob >= 0.55)
- **Features:** 28 canonical (3y model, selected via IC/ICIR on train-only)
- **Training data:** Clean stocks only (fraud_suspect==0, ROA positive, Beneish < -1.78)
- **Train period:** fiscal_year <= 2023 (expanding window in walk-forward)
- **Why regression over classification:** Regression ranks by HOW MUCH a stock beats market, not just probability. +3% CAGR improvement, same risk profile when combined with strict tree gate.

## Scoring + Portfolio Construction

```
Step 1: Apply hard gates (8 total):
  - Market = US
  - Market cap: $50M – $10B
  - Beneish M-score < -1.78
  - Piotroski F-score >= 3
  - piotroski_roa_pos == 1
  - Altman Z-score > 1.0
  - ps_ratio_sector_pct <= 0.70 (not overpriced vs sector)
  - momentum_12m_prior > -0.40 (no structural decliners)
Step 2: Score survivors with decision tree → tree_prob
Step 3: Gate: tree_prob >= 0.55 (strict agreement)
Step 4: Score with LightGBM regression → predicted 3y return
Step 5: Candidate-wide ADTV liquidity filter before ranking
Step 6: Rank by reg_3y descending, take top 15
Step 7: M&A screen (LLM flag — manual review, not auto-exclude)
Step 8: Equal-weight, annual rebalance
```

### Frozen ADTV contract

- Portfolio AUM: `$200,000`.
- `target_n`: `15`; planned equal-weight position: `AUM / target_n =
  $13,333.333333...`.
- Limit: planned position must be no more than `1%` of candidate ADTV.
- Required ADTV: `(AUM / target_n) / 0.01 = $1,333,333.333333...`.
- ADTV estimator: median of exactly 30 valid regular-session observations of
  unadjusted close times volume, using only sessions whose market close is
  strictly before the candidate's prediction timestamp.
- Evidence timestamp: prediction timestamp; the certified Session 8E entry
  timestamp is the later execution timestamp.
- Scope: compute for every candidate that passes all non-liquidity hard gates
  and required model-role availability, before regression ranking and top-15
  selection.
- Missingness: fewer than 30 valid observations, missing/nonpositive close or
  volume, ambiguous security identity, missing timestamp, or missing payload
  fails the candidate closed. The gate cannot be disabled or computed only for
  a provisional top 15.

The legacy engine expression `AUM * 0.01` is not the accepted position-size
test. Session 9C covers post-selection market/NAV evidence; it does not replace
this preselection liquidity gate.

### Current corrected-path availability

This configuration is the production contract, not evidence that the current
`CORRECTED_8F` path can execute it. The frozen Session 9B verdict remains
unavailable: the final Session 8F outputs lack `beneish_m_score`,
`altman_z_score`, and `ps_ratio_sector_pct`; the required fold-local tree score
has not been built; candidate-wide ADTV evidence is absent; and Session 9's 3y
Ridge ranker is not the LightGBM family named above. Those items must be
explicitly reconciled under the post-Session-9B roadmap before production
holdings can be frozen.

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

- MaxDD -8.1% (improved by momentum gate, was -17.2% without)
- Regression may chase small-cap outliers in live trading (unverifiable until tried)
- Only 11 years of walk-forward data — not statistically definitive
- Deep value tilt: will underperform in growth/momentum markets
- $10B cap means missing some mid-cap winners ($10-50B range)
- Model trained on historical patterns; regime change could invalidate
- LLM M&A screen has knowledge cutoff — may miss very recent deals
