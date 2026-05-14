# App Walkthrough

The Streamlit app (`app_v2.py`) has 10 tabs. This page walks through each one.

---

## Tab 1 — Screener

Filter and rank all companies in the universe.

**Sidebar controls:**

| Filter | Description |
|---|---|
| Markets | US, EU, KR, JP, CA, BR (any combination) |
| Fiscal year range | Slider to restrict to specific years |
| Market cap preset | All sizes, Neglected ($50M–$500M), Small/Mid/Large cap |
| ML horizon | 6-month through 5-year alpha prediction window (HorizonRouter maps to nearest trained model) |
| Composite score | Minimum percentile threshold |
| Risk filters | Exclude likely-delisted; exclude Beneish M > −2.22; exclude Altman Z < 1.81 |
| Sector filter | GICS sector picker |
| Search | Ticker or company name substring |

**Output:**

A sortable table with: ticker, company name, composite score, ML alpha scores (6m/1y/2y/3y/5y), Beneish M-Score, Piotroski F-Score, market, fiscal year.

Expand any row to see the mini company card: score over time chart, top-3 concerns, and financial statement sparklines.

---

## Tab 2 — Company Profile

Deep dive into a single company.

**What you'll see:**

- **Score timeline** — composite alpha score across all available fiscal years
- **SHAP attribution chart** — horizontal bar chart showing which features drove the score up (red) or down (blue)
- **Feature table** — values for all ~35 model features with sector-relative percentile comparisons
- **Accounting flags** — Beneish M-Score breakdown (8 components with radar chart), Altman Z-Score zone, Piotroski F-Score
- **Strengths & Weaknesses narrative** — auto-generated from SHAP values: which financial patterns the model flagged

!!! note "Narrative generation"
    The narrative is generated from SHAP values — it describes the specific financial patterns that drove the score, not generic risk warnings. A score of 0.82 with "receivables accruals 2.1σ above sector median and auditor changed in FY2023" is more actionable than a plain "high risk" label.

---

## Tab 3 — Realtime Chart

Live price chart for any ticker.

**What you'll see:**

- OHLCV candlestick chart (daily, up to 2 years)
- Volume bars
- Alpha score overlay as a horizontal reference line

Enter any ticker symbol. The alpha score line is static (from the last filed fiscal year) — compare it to where the price was when the annual report was filed.

---

## Tab 4 — Market Overview

Cross-market risk comparison.

**What you'll see:**

- Average alpha score by market — which market has the highest aggregate risk?
- Score distribution box plots per market
- Top 10 highest-scoring companies per market
- Market-level score trend over fiscal years

---

## Tab 5 — Backtester

Walk-forward strategy performance simulation, Kelly portfolio tearsheet, and alpha signal browser.

**What you'll see:**

- Cumulative wealth chart (portfolio vs SPY benchmark)
- Annual excess return bar chart
- Max drawdown chart
- Rolling 3-year Sharpe ratio
- KPI table: CAGR, Sharpe, Sortino, Calmar, Max Drawdown, Hit Rate
- Walk-forward AUC chart by horizon (expanding-window CV)

**Kelly Portfolio section** (requires `data/portfolio_backtest.json`):
- KPI metrics strip: CAGR, Sharpe, Max DD, VaR 95%, CVaR 99%
- Cumulative wealth vs SPY line chart
- Annual return bar chart
- Collapsible current holdings table with weights and Kelly fractions

**Alpha Signal Browser** (requires `data/alpha_registry.json`):
- Filterable / sortable table of all selected signals (IC Mean, ICIR, CAGR, Sharpe, bootstrap CIs)
- Horizontal IC bar chart for top 25 signals (green ≥ 0.03, amber ≥ 0, red < 0)

**Strategies available:**

| Strategy | Description |
|---|---|
| COMPOSITE | Top-decile composite score, equal-weight |
| QEM | Quality × earnings momentum long/short |
| SCDV | Sector-constrained distress vs value |
| IARB | Inversion arbitrage: high Ohlson, low Beneish |

See [Strategies](strategies.md) for construction details and expected performance.

---

## Tab 6 — Watchlist

Personalised portfolio monitoring.

**What you'll see:**

- Your saved companies with current alpha scores
- Score change alerts — highlighted if any score moved more than ±0.10 since last checked
- Beneish M-Score and Altman Z-Score status per holding
- One-click removal from watchlist

Add companies from the Screener or Company Profile tabs. The watchlist is stored in `st.session_state` during the session.

---

## Tab 7 — Strategies

Strategy construction reference.

Detailed page explaining each of the four backtest strategies:

- Signal generation logic (which scores, which thresholds)
- Position sizing (equal weight vs Kelly-scaled)
- Rebalancing frequency
- Historical KPIs (CAGR, Sharpe, Max Drawdown) from the walk-forward backtest

---

## Tab 8 — User Guide

In-app documentation panel.

Renders the full user guide directly inside the Streamlit app — equivalent to this documentation site but accessible without leaving the app. Covers score interpretation, strategy descriptions, and FAQ.

---

## Tab 9 — Case Studies

Ten documented accounting fraud cases with quantitative pre-fraud signals.

**What you'll see:**

1. Select a case from the dropdown (Wirecard, Luckin, Enron, WorldCom, NMC Health, Steinhoff, Valeant, Satyam, Parmalat, Nikola)
2. Read the fraud summary and key financial warning signals that were visible before the revelation
3. View live Beneish M-Score, Altman Z-Score, and Fraud Score from the dataset if the ticker is available
4. See a score timeline chart with the fraud revelation year marked with a vertical line
5. Compare all cases in the overview summary table at the bottom

!!! tip "Using the cases for calibration"
    Before dismissing a high-scoring company as a false positive, check whether its signal pattern resembles any of the documented cases. DSRI > 1.3 + OCF/NI divergence is the Wirecard/Luckin/Valeant pattern. Z-Score < 1.81 + AQI elevation is the WorldCom/Parmalat pattern.

---

## Tab 10 — Benchmarking

AUC-ROC model comparison evaluated on the full annual dataset.

**What you'll see:**

1. **AUC bar chart** — all models ranked by discrimination power (classical baselines vs ML)
2. **Full results table** — AUC-ROC, Average Precision, and direction notes for every model
3. **Precision-Recall curves** — select an operational threshold for your desired recall target
4. **ROC curves** — sensitivity vs specificity comparison across models
5. **Methodology & Caveats expander** — full disclosure of evaluation procedure and known limitations

**Dataset:** ~47,000 annual filings with 172 confirmed fraud labels (0.37% prevalence).

See [Benchmarking Methodology](../methodology/benchmarking.md) for the full technical details.
