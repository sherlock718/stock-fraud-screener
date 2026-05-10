# Portfolio Strategies

The screener implements four portfolio construction strategies, each targeting a different risk/return profile.

## Strategy Overview

| Strategy | Label | Universe | Logic | Best For |
|---|---|---|---|---|
| COMPOSITE | Fraud-safe long | Low fraud score, all markets | Top quintile by composite score (inverted — lowest fraud = best) | Broad diversification |
| QEM | Quality + Earnings + Momentum | Low fraud, high quality, positive momentum | Piotroski ≥ 6 + score < 0.30 + 12M price momentum > 0 | Quality-growth investors |
| SCDV | Small Cap Deep Value | Low fraud, deep value small caps | Score < 0.30 + market cap 150M–1B + P/B < sector median | Value investors |
| IARB | International Arbitrage | Cross-market valuation gap | Same-sector companies in different markets with >30% valuation spread and score < 0.35 | Cross-market traders |

## COMPOSITE Strategy

**Universe:** All companies with composite score < 0.30 (fraud-safe) and confidence ≥ Medium

**Construction:**

1. Rank by composite score ascending (lowest fraud risk = rank 1)
2. Take top N (default: 25)
3. Equal-weight positions (1/N each)
4. Rebalance annually at fiscal year publication

**Backtest result:** +25.0% CAGR, +13.1% excess vs S&P 500, Sharpe 1.327

## QEM Strategy

**Universe:** Companies passing all three gates:
- Fraud score < 0.30
- Piotroski F-Score ≥ 6
- 12-month price momentum > 0%

**Construction:**

1. Score each passing company: `qem_score = (1 - composite_score) × 0.5 + (piotroski / 9) × 0.3 + momentum_rank × 0.2`
2. Take top 20
3. Equal-weight

**Backtest result:** +14.9% CAGR

**Why QEM works:** Combining fraud avoidance with quality gates and momentum filters removes the worst value traps. High Piotroski companies with clean accounting that are still in uptrends tend to sustain returns.

## SCDV Strategy

**Universe:** Companies passing all three gates:
- Fraud score < 0.30
- Market cap USD 150M – 1B (small cap)
- P/B ratio < sector median

**Construction:**

1. Sort by composite score ascending within each SIC sector
2. Take top 2 per sector (sector diversification)
3. Equal-weight, capped at 15% per position

**Backtest result:** +18.1% CAGR

**Why SCDV works:** Small caps are under-researched, creating more pricing inefficiencies. Value-screen + fraud-clean filter removes the "cheap for a reason" companies — often those cheap because of accounting manipulation.

## IARB Strategy

**Universe:** Pairs of companies in the same SIC sector across different markets where:
- Both have composite score < 0.35
- Price-to-Book or EV/EBITDA spread > 30%
- Same reporting standard where possible

**Construction:**

Long the cheaper market, short the more expensive market (or just long if no shorting available)

**Note:** IARB is the most experimental strategy. Cross-market accounting comparisons are subject to IFRS/GAAP differences. Use with caution until per-market models are calibrated.

## Position Sizing

All strategies use equal-weight by default. The leverage strategy uses Kelly criterion sizing — see [Leverage Strategy](../methodology/leverage.md).

## Rebalancing

- **Frequency:** Annual (after fiscal year publication)
- **Transaction costs:** 30 bps per trade assumed in backtest
- **Slippage budget:** Limit orders only; position size capped to avoid moving illiquid names
- **Tax:** Pre-tax returns; adjust for your jurisdiction

## Combining Strategies

The COMPOSITE strategy is the broadest and most reliable. QEM and SCDV are tilts — they sacrifice some diversification for higher expected alpha. A simple allocation:

| Allocation | Rationale |
|---|---|
| 60% COMPOSITE | Stability, broad fraud protection |
| 25% QEM | Quality tilt for growth |
| 15% SCDV | Value tilt for alpha |
