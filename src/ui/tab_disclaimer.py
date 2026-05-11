from __future__ import annotations

import streamlit as st


def tab_disclaimer() -> None:
    st.header('⚖️ Legal Disclaimer & Methodology Limitations')

    st.error(
        '**NOT FINANCIAL ADVICE.** This application provides quantitative fraud-risk signals '
        'derived from publicly available financial filings. Nothing here constitutes an '
        'investment recommendation, solicitation to buy or sell any security, or legal advice.',
        icon='⚠️',
    )

    st.markdown('---')

    with st.expander('📋 Full Legal Disclaimer', expanded=True):
        st.markdown("""
**Stock Fraud Screener — Disclaimer of Liability**

1. **Informational purpose only.** All scores, rankings, and outputs are provided for
   research and educational purposes. They are the output of statistical models and do not
   represent a determination that any company has committed fraud.

2. **No investment advice.** Nothing in this application constitutes financial, investment,
   legal, or tax advice. Always consult a licensed financial adviser before making any
   investment decision.

3. **Past signals ≠ future fraud.** Model performance metrics (AUC, CAGR) are in-sample
   or walk-forward estimates. They do not guarantee future detection accuracy.

4. **Model limitations.** The composite fraud score is trained on available public-filing
   data. It cannot detect frauds that leave no trace in financial statements (e.g., bribery,
   undisclosed related-party agreements, trade-secret theft). Private companies with limited
   disclosure are especially unreliable.

5. **Data quality.** Underlying data is sourced from SEC EDGAR, SimFin, and other providers.
   Errors, delays, and omissions in source data will propagate to model outputs. Filing-date
   lags mean scores may be based on information that is up to 12 months stale.

6. **Survivorship bias.** The training dataset may under-represent delisted or bankrupt
   companies depending on data-provider coverage. This is documented in
   `scripts/bias_audit.py`.

7. **No guarantee of completeness.** The universe covered is a subset of global equities.
   Many small-cap and emerging-market companies are not represented.

8. **User responsibility.** By using this application, you agree that the developers
   bear no liability for any losses incurred as a result of reliance on the outputs.
""")

    st.markdown('---')

    with st.expander('🔬 Methodology Limitations', expanded=False):
        st.markdown("""
### Known Limitations of the Model

| Limitation | Detail | Status |
|-----------|--------|--------|
| Look-ahead bias | Features currently use fiscal_year_end as knowledge cutoff, not actual filing_date | Planned fix in Phase 0a |
| Survivorship bias | Delisted and bankrupt stocks may be under-represented in training data | Bias audit script available |
| Label quality | Primary target is 1-year price return vs local market, not a fraud confirmed label | SEC AAER labels in progress |
| IFRS vs GAAP | Beneish components behave differently across accounting standards | Market-specific recalibration planned |
| Private companies | No model outputs for companies with no public filing history (e.g. Theranos) | Governance sub-scores only |
| Governance signals | Governance sub-score is heuristic-based, not trained on labelled governance failures | GNN layer planned in Phase E |
| Macro regime | Models trained on 2003–2024 data; performance in high-inflation regimes is uncertain | Regime analysis added to benchmarking tab |
| Small cap liquidity | Position-sizing recommendations do not account for individual stock liquidity | Kelly sizing uses portfolio-level estimates |

### Backtest Assumptions

- **Transaction costs:** 30 bps per trade (estimated, not measured)
- **Rebalancing:** Annual, at fiscal year data publication — assumes instant execution at next-day open
- **Slippage:** Not modelled for small-cap positions; may be significant for names below $200M market cap
- **Tax:** All returns are pre-tax; post-tax returns will vary significantly by jurisdiction
- **Dividends:** Included in price return calculation where available via yFinance

### Model Versioning

Current model version: **v4** — trained on US fundamentals only.
EU and Korean market models are in development (Phase A).
""")

    st.markdown('---')

    with st.expander('📊 Data Sources & Attribution', expanded=False):
        st.markdown("""
| Data | Source | License |
|------|--------|---------|
| US fundamentals (10-K/10-Q) | SEC EDGAR (public domain) | Free |
| Stock prices | Yahoo Finance via yfinance | Free (non-commercial) |
| EU fundamentals | SimFin (planned) | Free tier / commercial |
| Korean fundamentals | DART API (planned) | Free |
| Macro data | FRED / US Treasury (planned) | Free |
| Fraud labels | SEC AAER (in progress) | Free |

All SEC EDGAR data is in the public domain.
Yahoo Finance data is provided under their Terms of Service — this application
is for non-commercial research use only.
""")

    with st.expander('📬 Contact & Feedback', expanded=False):
        st.markdown("""
**Report false positives or data issues:**

- GitHub Issues: [github.com/sherlock718/stock-fraud-screener/issues](https://github.com/sherlock718/stock-fraud-screener/issues)
- The model benefits from false-positive feedback — if you believe a company is incorrectly
  flagged, please open an issue with the ticker, market, fiscal year, and your reasoning.

**Academic / research use:**

If you use this screener in academic research, please cite the methodology documentation
available at `docs/methodology/` in the repository.
""")

    st.caption(
        'Version 4.0 · Model trained 2024 · Data through latest available fiscal year · '
        'Source: SEC EDGAR / Yahoo Finance'
    )
