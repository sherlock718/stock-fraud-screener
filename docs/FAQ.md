# FAQ — Quick Reference

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
- **Primary:** LightGBM (ranking) gated by depth-4 decision tree (explainability)
- **Agreement threshold:** tree_prob ≥ 0.35

## Key Thresholds
| Metric | Threshold | Meaning |
|--------|-----------|---------|
| PSI | 0.25 | Feature drift alert (Population Stability Index) |
| IC | 0.02 | Minimum Information Coefficient for feature retention |
| Beneish M-score | −1.78 | Below = likely earnings manipulator |
| Tree agreement | 0.35 | Minimum tree probability to pass gate |
| ADTV filter | AUM × 1% | Position can't exceed 1% of median 30d volume |

## Backtest Summary
- **Train:** 2008–2020 | **Val:** 2021–2023 | **Test:** 2024
- **Sharpe (test):** ~1.2 (ml_gates mode)
- **Benchmark:** SPY (US), ACWI ex-US (international)
