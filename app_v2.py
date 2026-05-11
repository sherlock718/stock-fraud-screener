from __future__ import annotations

import streamlit as st

from src.data import load_data, load_models
from src.sidebar import sidebar_refresh
from src.ui.tab_screener import tab_screener
from src.ui.tab_company_profile import tab_company_profile
from src.ui.tab_realtime_chart import tab_realtime_chart
from src.ui.tab_market_overview import tab_market_overview
from src.ui.tab_backtester import tab_backtester
from src.ui.tab_watchlist import tab_watchlist
from src.ui.tab_strategies import tab_strategies
from src.ui.tab_guide import tab_guide
from src.ui.tab_case_studies import tab_case_studies
from src.ui.tab_benchmarking import tab_benchmarking


def main() -> None:
    st.set_page_config(
        page_title='Stock Fraud & Value Screener',
        page_icon='🔍',
        layout='wide',
        initial_sidebar_state='expanded',
    )

    with st.spinner('Loading dataset…'):
        df_all = load_data()
    models, meta = load_models()

    with st.sidebar:
        sidebar_refresh()

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10 = st.tabs([
        '📊 Screener',
        '🏢 Company Profile',
        '📈 Realtime Chart',
        '🌍 Market Overview',
        '📉 Backtester',
        '⭐ Watchlist',
        '🎯 Strategies',
        '📖 User Guide',
        '📚 Case Studies',
        '📐 Benchmarking',
    ])

    with tab1:
        tab_screener(df_all, models, meta)
    with tab2:
        tab_company_profile(df_all, models, meta)
    with tab3:
        tab_realtime_chart(df_all)
    with tab4:
        tab_market_overview(df_all)
    with tab5:
        tab_backtester()
    with tab6:
        tab_watchlist(df_all)
    with tab7:
        tab_strategies()
    with tab8:
        tab_guide()
    with tab9:
        tab_case_studies(df_all)
    with tab10:
        tab_benchmarking(df_all)


if __name__ == '__main__':
    main()
