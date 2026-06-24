from __future__ import annotations

import streamlit as st


def tab_guide() -> None:
    st.title('📖 User Guide')
    st.markdown(
        'This guide explains every score, metric, and filter in the screener, '
        'and walks through how to use the tool for systematic stock research.'
    )

    with st.expander('🔍 What is this tool?', expanded=True):
        st.markdown("""
This is a **multi-market stock fraud and value screener** covering US, Canadian, European,
Japanese, and Brazilian listed companies. It combines:

- **Fundamental accounting data** from SEC EDGAR (annual filings)
- **Fraud-risk signals** using academic models (Beneish, Altman, Piotroski)
- **Machine learning scores** trained on historical forward returns
- **A composite ranking** that blends value, quality, momentum, safety, and ML signals

The goal is to surface **high-quality, undervalued, low-fraud-risk** companies across global markets
— useful for long-term fundamental investors, not day traders.
        """)

    with st.expander('📊 Composite Score (0–100)', expanded=True):
        st.markdown("""
The **Composite Score** is the main ranking number. It runs from **0 to 100**, where **100 is best**.

It is a weighted percentile rank across five components:

| Component | Weight | Higher is better when… |
|---|---|---|
| **Value** | 25% | P/E and P/B are low (cheap stock) |
| **Quality** | 20% | Piotroski F-Score is high (strong fundamentals) |
| **Momentum** | 20% | 12-month price return is high |
| **Fraud Safety** | 20% | Beneish M-Score is low (less earnings manipulation risk) |
| **ML Alpha** | 15% | ML model predicts strong 1-year forward return |

If a component is missing for a ticker, the remaining weights are rescaled to sum to 100%.

**How to use it:** Sort descending and look at scores above 70 as the starting pool.
Do not use the score alone — read the individual signals to understand *why* a company ranks high.
        """)

    with st.expander('🚨 Beneish M-Score (Earnings Manipulation Risk)'):
        st.markdown("""
The **Beneish M-Score** is an accounting model that estimates the probability of earnings
manipulation. It was developed by Professor Messod Beneish in 1999.

**Interpretation:**
- **M-Score > −1.78** → Possible manipulator — treat with caution
- **M-Score < −2.22** → Unlikely manipulator — lower risk
- Values in between are a grey zone

The score is built from eight financial ratios measuring changes in receivables, gross margins,
asset quality, sales growth, depreciation, leverage, and accruals.

**What it does NOT catch:** Fraud that doesn't show up in GAAP accounting (e.g. off-balance-sheet
vehicles, crypto asset manipulation). It is a signal, not a verdict.
        """)

    with st.expander('🏦 Altman Z-Score (Bankruptcy Risk)'):
        st.markdown("""
The **Altman Z-Score** predicts the probability of a company going bankrupt within two years.
It was developed by Edward Altman in 1968 and is still widely used.

**Interpretation (original manufacturing model):**
- **Z > 2.99** → Safe zone
- **1.81 < Z < 2.99** → Grey zone
- **Z < 1.81** → Distress zone — significant bankruptcy risk

Note: the thresholds differ for non-manufacturing and emerging-market companies. The screener
uses the original model for all companies — treat the thresholds as directional, not absolute.

**What to do with a low Z-Score:** Dig into leverage ratios, interest coverage, and cash runway.
A low Z-Score in a capital-intensive industry (utilities, real estate) is normal; in tech it is a warning.
        """)

    with st.expander('✅ Piotroski F-Score (Fundamental Quality)'):
        st.markdown("""
The **Piotroski F-Score** (0–9) is a checklist of nine binary signals across three categories:

| Category | Signals |
|---|---|
| **Profitability** (4 pts) | ROA > 0, Operating cash flow > 0, ROA improving, Cash flow > net income (accruals) |
| **Leverage & Liquidity** (3 pts) | Debt ratio falling, Current ratio rising, No dilution |
| **Operating Efficiency** (2 pts) | Gross margin improving, Asset turnover improving |

**Interpretation:**
- **8–9** → Strong fundamentals
- **5–7** → Average
- **0–2** → Weak — potential value trap or distress

**How to use it:** Pair with a low P/B ratio. Piotroski's original paper showed buying high-F-Score,
low P/B stocks outperformed the market by ~7.5% annually (1976–1996).
        """)

    with st.expander('🤖 ML Score (Machine Learning Alpha)'):
        st.markdown("""
The **ML Score** is the probability output of a LightGBM gradient-boosting model trained to
predict whether a company will be in the **top quartile of 1-year forward returns** given its
current fundamentals.

- Range: **0.0 to 1.0** (higher = model thinks this company will outperform)
- The model uses ~35 engineered features covering profitability, leverage, growth, valuation,
  and accounting quality — selected by IC/ICIR analysis to keep only stable, non-redundant predictors
- It is trained on historical annual filings with labels derived from actual price returns

**Caveats:**
- The model is trained on past data — regime changes may reduce accuracy
- It is better used as a ranking signal than as an absolute probability
- Always cross-check with Beneish and Piotroski before acting on a high ML score
        """)

    with st.expander('💰 Dividend Metrics'):
        st.markdown("""
Dividend data is fetched from Yahoo Finance and linked to each ticker.

| Field | Meaning |
|---|---|
| **Dividend Yield %** | Annual dividend / current price |
| **Annual Rate $** | Total dividends paid per share per year |
| **Payout Ratio %** | Dividends as % of earnings. >100% may be unsustainable |
| **Ex-Dividend Date** | You must own the stock before this date to receive the next dividend |

These are live data points and may differ slightly from the annual filing data used in scoring.
        """)

    with st.expander('🏭 GICS Sector Filter'):
        st.markdown("""
Companies are classified using the **Global Industry Classification Standard (GICS)**,
maintained by MSCI and S&P Global. The screener fetches this from Yahoo Finance.

The 11 GICS sectors are:
`Communication Services`, `Consumer Discretionary`, `Consumer Staples`, `Energy`,
`Financials`, `Health Care`, `Industrials`, `Information Technology`,
`Materials`, `Real Estate`, `Utilities`

Use the sector filter in the sidebar to focus on industries you understand well, or to
avoid sectors where the accounting models behave differently (e.g. Financials and Real Estate
have different leverage norms — Altman Z-Score thresholds don't apply directly).
        """)

    with st.expander('📈 Strategies Tab'):
        st.markdown("""
The **Strategies tab** shows pre-built stock screens derived from academic factor literature:

| Strategy | Logic |
|---|---|
| **QEM — Quality + Earnings Momentum** | High Piotroski F-Score + positive earnings revisions |
| **SCDV — Small-Cap Deep Value** | Low P/B + low P/E + small market cap |
| **IARB — International Arbitrage** | Undervalued in home market vs US-listed ADR peers |

Strategies are generated by running `python3 run_pipeline.py features` and saved as CSVs.
You can download each strategy list directly from the Strategies tab.
        """)

    with st.expander('⭐ Watchlist & Alerts'):
        st.markdown("""
The **Watchlist tab** lets you save tickers for ongoing monitoring.

- Add a ticker using the input box — it will be saved to `data/watchlist.json`
- Live prices are fetched from Yahoo Finance when you view the watchlist
- Alerts (price thresholds, score changes) are planned for a future release

**Tip:** Add tickers you've researched and want to track over multiple quarters.
Compare the Composite Score across annual reporting periods to see if fundamentals are improving.
        """)

    with st.expander('🔄 Data Refresh'):
        st.markdown("""
The sidebar **Data Refresh** panel lets you update the underlying dataset:

| Mode | What it does | Estimated time |
|---|---|---|
| **Quick** | Re-computes features from existing data (no API calls) | ~5 min |
| **Prices** | Re-pulls Yahoo Finance prices + recomputes features | ~30–60 min |
| **Full** | Full rebuild from SEC EDGAR (all filings) + prices | Several hours |

Run **Quick** after changing feature engineering code.
Run **Prices** monthly to update forward return calculations.
Run **Full** annually or after a major EDGAR data update.
        """)

    with st.expander('⚠️ Important Disclaimers'):
        st.markdown("""
**This tool is for research and educational purposes only. It is not financial advice.**

- Scores and rankings are based on historical accounting data, which may be restated or delayed
- The ML model is trained on past relationships that may not hold in the future
- Beneish and Altman scores are probabilistic signals, not auditor opinions
- No score replaces thorough fundamental research, reading actual filings, and understanding a business
- Past outperformance of any factor does not guarantee future results
- Always consult a qualified financial professional before making investment decisions

**Data sources:** SEC EDGAR (financial statements), Yahoo Finance (prices, sectors, dividends),
FRED (macroeconomic context)
        """)
