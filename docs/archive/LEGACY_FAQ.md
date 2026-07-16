# Archived FAQ — Quick Reference

> Historical reference archived on 2026-07-17. Thresholds, coverage, and
> performance statements below require verification before reuse.

## Dataset
- **Companies:** ~7,600 unique tickers (US-listed, SEC EDGAR filers)
- **Source:** SEC EDGAR annual filings (10-K), enriched with price data
- **Update frequency:** Weekly via CI (`refresh_data.yml`)

## Features
- **Canonical set:** 27 features (temporally stable across train windows)
- **Categories:** Fundamental ratios, Piotroski-F sub-scores, Beneish M-score components, growth/momentum

## Pipeline Steps
1. `step1` — Download raw filings from SEC EDGAR
2. `step2` — Parse XBRL into standardized fields
3. `step3` — Merge multi-year panels per ticker
4. `step4` — Compute forward returns (1y/3y/5y)
5. `step5` — Compute features (ratios, scores, growth YoY)
6. `step6` — Quality checks + final parquet export

## Models
- **Horizons:** 1-year, 3-year, 5-year forward return prediction
- **Primary:** LightGBM classifier (gate) + LightGBM regressor (ranking by predicted 3y return magnitude)
- **Agreement threshold:** tree_prob ≥ 0.55 (depth-4 decision tree must concur)
- **Features:** 28 for 3y horizon (walk-forward IC-ranked per year)

## Key Thresholds
| Metric | Threshold | Meaning |
|--------|-----------|---------|
| PSI | 0.25 | Feature drift alert (Population Stability Index) |
| IC | 0.02 | Minimum Information Coefficient for feature retention |
| Beneish M-score | −1.78 | Below = likely earnings manipulator |
| Piotroski F-score | ≥ 3 | Minimum financial strength |
| ROA | > 0 | Must be profitable (piotroski_roa_pos == 1) |
| Tree agreement | 0.55 | Minimum tree probability to pass gate |
| Altman Z-score | > 1.0 | Not in distress zone |
| P/S sector pct | ≤ 0.70 | Not overpriced vs sector peers |
| Momentum 12m | > −0.40 | No structural decliners / value traps |
| Market cap | $50M–$10B | Small/mid-cap focus |
| ADTV filter | AUM × 1% | Position can't exceed 1% of median 30d volume |

## Backtest Summary (Walk-Forward, 2013–2023)
- **Strategy:** ml_gates (LightGBM regression + decision tree agreement + 8 hard gates)
- **CAGR:** +31.5% (SPY +13.6%)
- **Sharpe:** 1.45
- **MaxDD:** −8.1%
- **Top N:** 15 equal-weight, annual rebalance
- **Train:** 2008–2020 | **Val:** 2021–2023 | **Test:** 2024
- **Benchmark:** SPY (US), ACWI ex-US (international)
- **M&A screen:** Groq/Llama 3.3 flags pending deals (manual review, not auto-exclude)
