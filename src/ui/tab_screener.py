from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.config import MARKET_LABELS
from src.scoring import composite_rank, score_companies


def tab_screener(df_all: pd.DataFrame, models: dict, meta: dict) -> None:
    model_loaded = bool(models)

    with st.sidebar:
        st.title('🔍 Screener Controls')

        st.subheader('Markets')
        avail_markets   = sorted(df_all['market'].unique())
        market_options  = {MARKET_LABELS.get(m, m): m for m in avail_markets}
        selected_labels = st.multiselect(
            'Select markets',
            options=list(market_options.keys()),
            default=[MARKET_LABELS.get(m, m) for m in avail_markets if m != 'US'] or list(market_options.keys())[:3],
        )
        selected_markets = [market_options[l] for l in selected_labels] if selected_labels else avail_markets

        st.subheader('Fiscal Year')
        year_min   = int(df_all['fiscal_year'].min())
        year_max   = int(df_all['fiscal_year'].max())
        year_range = st.slider('Fiscal year range', year_min, year_max,
                               (max(year_min, year_max - 5), year_max))

        st.subheader('Company Size')
        cap_preset = st.radio('Market cap preset', [
            'All sizes', 'Neglected ($50M–$500M)', 'Small cap ($50M–$2B)',
            'Mid cap ($2B–$10B)', 'Large cap (>$10B)',
        ], index=0)
        cap_filter = {
            'All sizes':              (0,     1e15),
            'Neglected ($50M–$500M)': (50e6,  500e6),
            'Small cap ($50M–$2B)':   (50e6,  2e9),
            'Mid cap ($2B–$10B)':     (2e9,   10e9),
            'Large cap (>$10B)':      (10e9,  1e15),
        }[cap_preset]

        st.subheader('ML Scoring Horizon')
        if model_loaded:
            horizon = st.selectbox('Investment horizon', ['1y', '3y', '5y'], index=0)
        else:
            st.warning('Models not saved yet.\nRun: python3 scripts/train_models.py')
            horizon = '1y'

        st.subheader('Composite Score Filter')
        min_composite = st.slider('Min composite score (percentile)', 0, 100, 60) / 100

        st.subheader('Risk Filters')
        exclude_delisted = st.checkbox('Exclude likely delisted', value=True)
        exclude_beneish  = st.checkbox('Exclude Beneish M > -2.22 (manipulators)', value=True)
        exclude_altman   = st.checkbox('Exclude Altman Z < 1.81 (distressed)', value=False)

        st.subheader('Search')
        text_query = st.text_input('Ticker / company name', placeholder='e.g. AAPL, Samsung…')

        if 'sector' in df_all.columns:
            st.subheader('GICS Sector')
            avail_sectors = sorted(df_all['sector'].dropna().unique().tolist())
            selected_sectors = st.multiselect('Filter by sector', avail_sectors,
                                              placeholder='All sectors (leave blank)')
        else:
            selected_sectors = []

    df = df_all[
        df_all['market'].isin(selected_markets) &
        df_all['fiscal_year'].between(*year_range)
    ].copy()

    if 'market_cap_at_filing' in df.columns:
        df = df[df['market_cap_at_filing'].isna() | df['market_cap_at_filing'].between(*cap_filter)]
    if exclude_delisted and 'likely_delisted' in df.columns:
        df = df[~df['likely_delisted']]
    if exclude_beneish and 'beneish_m_score' in df.columns:
        df = df[df['beneish_m_score'].isna() | (df['beneish_m_score'] <= -2.22)]
    if exclude_altman and 'altman_z_score' in df.columns:
        df = df[df['altman_z_score'].isna() | (df['altman_z_score'] >= 1.81)]
    if text_query:
        q    = text_query.lower()
        mask = (df['ticker'].str.lower().str.contains(q, na=False) |
                df['name'].str.lower().str.contains(q, na=False))
        df   = df[mask]
    if selected_sectors and 'sector' in df.columns:
        df = df[df['sector'].isin(selected_sectors)]

    df     = score_companies(df, models, meta, horizon)
    df     = composite_rank(df)
    latest = (df.sort_values('fiscal_year', ascending=False)
                .drop_duplicates('cik', keep='first'))
    latest = latest[latest['composite_score'] >= min_composite].copy()
    latest = latest.sort_values('composite_score', ascending=False)

    st.title('🔍 Stock Fraud & Value Screener')
    model_status = f'ML model loaded ({horizon} horizon)' if model_loaded else '⚠️ ML model not saved yet'
    st.caption(f'{len(df_all):,} total rows | {model_status} | {len(latest):,} companies after filters')

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric('Companies shown', f'{len(latest):,}')
    c2.metric('Markets', len(selected_markets))
    c3.metric('Fiscal years', f'{year_range[0]}–{year_range[1]}')
    if 'beneish_m_score' in latest.columns:
        c4.metric('High Beneish risk', f'{(latest["beneish_m_score"] > -2.22).sum():,}')
    else:
        c4.metric('High Beneish risk', 'N/A')
    if model_loaded and 'ml_score' in latest.columns:
        c5.metric('ML score > 0.6', f'{(latest["ml_score"] > 0.6).sum():,}')
    else:
        c5.metric('ML score > 0.6', 'N/A')

    st.subheader('Top Companies by Composite Score')
    display_cols = (['ticker', 'name', 'market', 'fiscal_year', 'composite_score'] +
                    ([c for c in ['sector'] if c in latest.columns]) +
                    [c for c in ['value_composite', 'pe_ratio', 'pb_ratio'] if c in latest.columns][:2] +
                    [c for c in ['quality_composite', 'piotroski_f_score', 'roe'] if c in latest.columns][:2] +
                    [c for c in ['momentum_12m_prior'] if c in latest.columns] +
                    [c for c in ['beneish_m_score', 'altman_z_score'] if c in latest.columns] +
                    (['ml_score'] if model_loaded and 'ml_score' in latest.columns else []) +
                    (['market_cap_at_filing'] if 'market_cap_at_filing' in latest.columns else []))
    display_cols = [c for c in display_cols if c in latest.columns]
    show_df = latest[display_cols].head(200).copy()
    for col in show_df.select_dtypes('float').columns:
        if col == 'composite_score':
            show_df[col] = show_df[col].map(lambda x: int(round(x * 100)) if pd.notna(x) else '')
        elif col == 'market_cap_at_filing':
            show_df[col] = show_df[col].map(
                lambda x: (f'${x/1e9:.1f}B' if pd.notna(x) and x >= 1e9
                           else (f'${x/1e6:.0f}M' if pd.notna(x) else ''))
            )
        elif col in ['pe_ratio', 'pb_ratio', 'ev_ebitda']:
            show_df[col] = show_df[col].map(lambda x: f'{x:.1f}' if pd.notna(x) else '')
        else:
            show_df[col] = show_df[col].map(lambda x: f'{x:.3f}' if pd.notna(x) else '')
    st.dataframe(show_df, use_container_width=True, height=520)

    st.subheader('Company Deep Dive')
    tickers_list    = [''] + sorted(latest['ticker'].dropna().unique().tolist())
    selected_ticker = st.selectbox('Select ticker', tickers_list, index=0)
    if selected_ticker:
        co_all    = df_all[df_all['ticker'] == selected_ticker].sort_values('fiscal_year')
        co_ann    = co_all[co_all['period_type'] == 'annual']
        co_latest = co_ann.tail(1)
        if co_latest.empty:
            st.warning('No annual data found for this ticker.')
        else:
            row = co_latest.iloc[0]
            st.markdown(f"### {row.get('name', 'N/A')} ({selected_ticker})")

            d1, d2, d3, d4, d5, d6 = st.columns(6)
            d1.metric('Market', MARKET_LABELS.get(str(row.get('market', '')), str(row.get('market', ''))))
            d2.metric('Fiscal Year', int(row['fiscal_year']) if pd.notna(row.get('fiscal_year')) else 'N/A')
            d3.metric('Exchange', str(row.get('exchange', 'N/A')))
            if pd.notna(row.get('market_cap_at_filing')):
                cap = row['market_cap_at_filing']
                d4.metric('Market Cap', f'${cap/1e9:.2f}B' if cap >= 1e9 else f'${cap/1e6:.0f}M')
            if pd.notna(row.get('sector')):
                d5.metric('Sector', str(row['sector']))
            if pd.notna(row.get('industry')):
                d6.metric('Industry', str(row['industry']))

            div_yield = row.get('dividendYield') or row.get('trailingAnnualDividendYield')
            div_rate  = row.get('dividendRate') or row.get('trailingAnnualDividendRate')
            payout    = row.get('payoutRatio')
            if any(pd.notna(x) for x in [div_yield, div_rate, payout]):
                st.markdown('**Dividends**')
                e1, e2, e3 = st.columns(3)
                e1.metric('Dividend Yield',
                          f'{div_yield*100:.2f}%' if pd.notna(div_yield) else 'N/A')
                e2.metric('Annual Rate',
                          f'${div_rate:.2f}' if pd.notna(div_rate) else 'N/A')
                e3.metric('Payout Ratio',
                          f'{payout*100:.1f}%' if pd.notna(payout) else 'N/A')

            st.markdown('**Risk Signals**')
            rc1, rc2, rc3, rc4 = st.columns(4)
            risk_checks = [
                ('Beneish M-Score', 'beneish_m_score', -2.22, 'below',
                 'Below -2.22 = low manipulation risk'),
                ('Altman Z-Score',  'altman_z_score',   1.81, 'above',
                 'Above 1.81 = financially stable'),
                ('Piotroski F',     'piotroski_f_score', 6.0, 'above',
                 '7–9 = high quality'),
            ]
            for c_obj, (label, col, thresh, direction, _help) in zip([rc1, rc2, rc3], risk_checks):
                if col in row.index and pd.notna(row[col]):
                    val = row[col]
                    ok  = val <= thresh if direction == 'below' else val >= thresh
                    c_obj.metric(label, f'{val:.2f}',
                                 delta='✅ OK' if ok else '⚠️ Risk',
                                 delta_color='normal' if ok else 'inverse',
                                 help=_help)
            if 'composite_score' in row.index and pd.notna(row.get('composite_score')):
                rc4.metric('Composite Score',
                           f'{int(round(row["composite_score"] * 100))}/100',
                           help='Percentile rank across value, quality, momentum, fraud safety, ML')

            with st.expander('📈 Price Chart (last 2 years)', expanded=True):
                try:
                    import yfinance as yf
                    hist = yf.Ticker(selected_ticker).history(period='2y', auto_adjust=True)
                    if not hist.empty:
                        fig_price = go.Figure()
                        fig_price.add_trace(go.Candlestick(
                            x=hist.index,
                            open=hist['Open'], high=hist['High'],
                            low=hist['Low'],   close=hist['Close'],
                            name=selected_ticker,
                            increasing_line_color='#4CAF50',
                            decreasing_line_color='#F44336',
                        ))
                        fig_price.update_layout(
                            title=f'{selected_ticker} — 2-Year Price History',
                            xaxis_title='Date', yaxis_title='Price (USD)',
                            height=360, margin=dict(l=0, r=0, t=40, b=0),
                            xaxis_rangeslider_visible=False,
                            template='plotly_dark',
                        )
                        st.plotly_chart(fig_price, use_container_width=True)
                    else:
                        st.info('No price data available for this ticker.')
                except Exception as e:
                    st.warning(f'Could not load price chart: {e}')

            fin_cols = [c for c in ['revenue', 'gross_profit', 'operating_income',
                                    'net_income', 'operating_cash_flow'] if c in co_ann.columns]
            if fin_cols and len(co_ann) > 1:
                with st.expander('📊 Fundamental Trends', expanded=True):
                    tab_f1, tab_f2, tab_f3 = st.tabs(['Income', 'Balance Sheet', 'Margins'])

                    with tab_f1:
                        income_cols = [c for c in ['revenue', 'gross_profit',
                                                    'operating_income', 'net_income'] if c in co_ann.columns]
                        if income_cols:
                            fig_inc = go.Figure()
                            for col in income_cols:
                                vals = co_ann[col].fillna(0) / 1e6
                                fig_inc.add_trace(go.Bar(
                                    x=co_ann['fiscal_year'], y=vals,
                                    name=col.replace('_', ' ').title(),
                                ))
                            fig_inc.update_layout(
                                barmode='group', height=320,
                                yaxis_title='USD Million',
                                legend=dict(orientation='h', y=-0.25),
                                margin=dict(l=0, r=0, t=10, b=0),
                                template='plotly_dark',
                            )
                            st.plotly_chart(fig_inc, use_container_width=True)

                    with tab_f2:
                        bs_cols = [c for c in ['total_assets', 'equity',
                                                'total_debt'] if c in co_ann.columns]
                        if bs_cols:
                            fig_bs = go.Figure()
                            for col in bs_cols:
                                vals = co_ann[col].fillna(0) / 1e6
                                fig_bs.add_trace(go.Scatter(
                                    x=co_ann['fiscal_year'], y=vals,
                                    name=col.replace('_', ' ').title(),
                                    mode='lines+markers',
                                ))
                            fig_bs.update_layout(
                                height=320, yaxis_title='USD Million',
                                legend=dict(orientation='h', y=-0.25),
                                margin=dict(l=0, r=0, t=10, b=0),
                                template='plotly_dark',
                            )
                            st.plotly_chart(fig_bs, use_container_width=True)

                    with tab_f3:
                        margin_cols = [c for c in ['gross_margin', 'operating_margin',
                                                    'net_margin', 'roe', 'roa'] if c in co_ann.columns]
                        if margin_cols:
                            fig_mar = go.Figure()
                            for col in margin_cols:
                                fig_mar.add_trace(go.Scatter(
                                    x=co_ann['fiscal_year'],
                                    y=co_ann[col] * 100,
                                    name=col.replace('_', ' ').title(),
                                    mode='lines+markers',
                                ))
                            fig_mar.add_hline(y=0, line_dash='dash', line_color='gray')
                            fig_mar.update_layout(
                                height=320, yaxis_title='%',
                                legend=dict(orientation='h', y=-0.25),
                                margin=dict(l=0, r=0, t=10, b=0),
                                template='plotly_dark',
                            )
                            st.plotly_chart(fig_mar, use_container_width=True)

            if 'forward_return_1y' in co_ann.columns:
                fwd = co_ann[['fiscal_year', 'forward_return_1y']].dropna()
                if not fwd.empty:
                    with st.expander('📅 Forward Return History (1y)', expanded=False):
                        colors = ['#4CAF50' if v >= 0 else '#F44336'
                                  for v in fwd['forward_return_1y']]
                        fig_fwd = go.Figure(go.Bar(
                            x=fwd['fiscal_year'],
                            y=fwd['forward_return_1y'] * 100,
                            marker_color=colors,
                        ))
                        fig_fwd.add_hline(y=0, line_color='gray', line_width=1)
                        fig_fwd.update_layout(
                            yaxis_title='1-Year Return (%)', height=280,
                            margin=dict(l=0, r=0, t=10, b=0),
                            template='plotly_dark',
                        )
                        st.plotly_chart(fig_fwd, use_container_width=True)

    with st.expander('📊 Dataset Overview Charts', expanded=False):
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(1, 3, figsize=(16, 4))
        mkt_counts       = df_all.drop_duplicates('cik').groupby('market').size().sort_values(ascending=False)
        mkt_labels_short = [MARKET_LABELS.get(m, m).split(' ')[-1] for m in mkt_counts.index]
        axes[0].bar(mkt_labels_short, mkt_counts.values, color='#2196F3', edgecolor='white')
        axes[0].set_title('Companies per Market', fontweight='bold')
        axes[0].tick_params(axis='x', rotation=45)
        axes[0].set_ylabel('Companies')
        labeled_by_yr = df_all[df_all['forward_return_1y'].notna()].groupby('fiscal_year').size()
        axes[1].fill_between(labeled_by_yr.index, labeled_by_yr.values, alpha=0.7, color='#4CAF50')
        axes[1].set_title('Labeled Rows (1y) per Fiscal Year', fontweight='bold')
        axes[1].set_ylabel('Count')
        axes[1].grid(alpha=0.3)
        if 'composite_score' in latest.columns and latest['composite_score'].notna().any():
            axes[2].hist(latest['composite_score'].dropna(), bins=40, color='#FF9800', edgecolor='white', alpha=0.85)
            axes[2].axvline(min_composite, color='red', ls='--', lw=1.5, label=f'Threshold {min_composite:.2f}')
            axes[2].set_title('Composite Score Distribution', fontweight='bold')
            axes[2].set_xlabel('Composite Score')
            axes[2].legend()
        plt.tight_layout()
        st.pyplot(fig, use_container_width=True)
        plt.close()

    st.subheader('Export')
    csv = latest[display_cols].head(1000).to_csv(index=False)
    st.download_button('⬇️ Download top 1000 as CSV', csv, 'screener_results.csv', 'text/csv')
