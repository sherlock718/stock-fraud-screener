# App Walkthrough

The Streamlit app (`app_v2.py`) has 8 tabs. This page walks through each one.

## Tab 1 — Overview

The landing tab. Shows the current universe at a glance.

**What you'll see:**

- Score distribution histogram — are most companies low-risk or are there many high-score outliers?
- Top 20 picks table — companies with the highest composite fraud probability this fiscal year
- Market breakdown — score distribution by market (US / EU / KR)
- Dataset health — last refresh date, number of companies scored, model versions

**How to use it:**

Start here to get a sense of the current risk landscape. A right-skewed distribution (most scores < 0.3) is normal. A sudden shift toward higher scores may indicate a sector under stress.

---

## Tab 2 — Screener

Filter and rank all companies in the universe.

**Filters available:**

| Filter | Description |
|---|---|
| Market | US, EU, KR (or all) |
| Score range | Min / max composite score |
| SIC code | Industry sector filter |
| Fiscal year | Which year's data to show |
| Confidence | High / Medium / Low data quality |

**Output:**

A sortable table with: ticker, company name, composite score, 1y/3y/5y scores, Beneish M-Score, Piotroski F-Score, market cap.

Click any row to navigate to that company's profile.

---

## Tab 3 — Company Profile

Deep dive into a single company.

**What you'll see:**

- **Score timeline** — composite score over all available fiscal years (trend matters as much as absolute level)
- **SHAP attribution chart** — horizontal bar chart showing which features pushed the score up (red) or down (blue)
- **Feature table** — the actual values for all ~35 model features, with benchmark comparisons
- **Accounting flags** — Beneish M-Score breakdown (8 components), Altman Z-Score zone, Piotroski F-Score
- **Strengths & Weaknesses narrative** — auto-generated plain-English summary of what the model found

!!! note "Narrative generation"
    The strengths/weaknesses narrative is generated from SHAP values — it describes which specific financial patterns drove the score, not generic risk warnings. A score of 0.82 with a narrative saying "Revenue accruals 2.1σ above sector median and auditor changed in FY2023" is more actionable than a plain "high risk" label.

---

## Tab 4 — Realtime Chart

Live price chart for any ticker.

**What you'll see:**

- OHLCV candlestick chart (daily, up to 2 years)
- Volume bars
- Fraud score overlay as a horizontal reference line — lets you visually judge whether the score change preceded price moves

**How to use it:**

Enter any ticker symbol. Use the date range selector to zoom. The fraud score line is static (based on last fiscal year data) — compare it to where the price was when the score was published.

---

## Tab 5 — Factor Research

IC/ICIR analysis for all features.

**What you'll see:**

- IC (Information Coefficient) time series per feature — does this feature predict future outperformance?
- ICIR (IC / StdIC) ranking bar chart — stability-adjusted predictive power
- Factor decay curve — how quickly does a feature's predictive power fade over 1–24 months?
- Correlation heatmap — which features are redundant?

**Useful for:**

Understanding which signals are actually driving model performance vs which are noise.

---

## Tab 6 — Backtest

Walk-forward strategy performance.

**What you'll see:**

- Cumulative wealth chart (portfolio vs benchmark)
- Annual excess returns bar chart
- Max drawdown chart
- Rolling 3-year Sharpe ratio
- KPI table: CAGR, Sharpe, Sortino, Calmar, Max Drawdown, Hit Rate

**Strategies available:**

Select from COMPOSITE, QEM, SCDV, or IARB in the dropdown.

See [Strategies](strategies.md) for construction details.

---

## Tab 7 — Market Overview

Cross-market comparison.

**What you'll see:**

- Average fraud score by market — which market has the highest aggregate risk?
- Score distribution by market (box plots)
- Top 10 risky companies per market
- Market-level trend over time

---

## Tab 8 — Model Diagnostics

Model health monitoring.

**What you'll see:**

- AUC by horizon (val / test / tuned / ensemble) — bar chart
- Calibration curve — are the model's stated probabilities accurate?
- PSI (Population Stability Index) — feature distribution drift vs training baseline
- Refresh status — last pipeline run timestamp, data lag

!!! warning "Drift alert"
    If PSI > 0.25 for 3+ features, the app shows a drift warning banner. This means the current company population differs significantly from what the model was trained on. Consider retraining.
