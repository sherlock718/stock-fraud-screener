from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.config import MARKET_LABELS
from src.scoring import (composite_rank, log_predictions, score_companies,
                          top_feature_importances)
from alpha.horizon_router import HorizonRouter, MODEL_LABELS


def _horizon_slider_to_key(months: int, meta: dict) -> tuple[str, str]:
    """Return (model_key, human_label) for a slider month value."""
    key = HorizonRouter.route(months)
    available = HorizonRouter.available_keys(meta)
    if key not in available and available:
        key = available[0]
    label = f'{HorizonRouter.months_to_label(months)} horizon → {MODEL_LABELS.get(key, key)}'
    return key, label


def _model_confidence_badge(wf_auc: float | None) -> tuple[str, str]:
    """Return (badge_text, color_hex) for a WF-AUC value."""
    if wf_auc is None:
        return 'AUC unknown', '#888888'
    if wf_auc >= 0.65:
        return f'WF-AUC {wf_auc:.3f} — High confidence', '#4CAF50'
    if wf_auc >= 0.60:
        return f'WF-AUC {wf_auc:.3f} — Good confidence', '#8BC34A'
    if wf_auc >= 0.55:
        return f'WF-AUC {wf_auc:.3f} — Moderate confidence', '#FF9800'
    return f'WF-AUC {wf_auc:.3f} — Screening only', '#F44336'


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

        st.subheader('Investment Horizon')
        if model_loaded:
            horizon_months = st.slider(
                'Investment horizon (months)', min_value=6, max_value=60, value=12, step=6,
                help='Selects the nearest trained discrete model (6m / 1y / 2y / 3y / 5y)',
            )
            horizon_key, horizon_label = _horizon_slider_to_key(horizon_months, meta)
            wf_auc = HorizonRouter.wf_auc(meta, horizon_key)
            badge, badge_color = _model_confidence_badge(wf_auc)
            st.caption(f'**{horizon_label}**')
            st.markdown(
                f'<span style="background:{badge_color};color:white;padding:2px 8px;'
                f'border-radius:4px;font-size:0.8em">{badge}</span>',
                unsafe_allow_html=True,
            )
            if wf_auc is not None and wf_auc < 0.60:
                st.warning('Low WF-AUC — use this horizon for screening only, not standalone signals.')
        else:
            st.warning('Models not saved yet.\nRun: python3 scripts/train_models.py')
            horizon_key = '1y'

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

    df     = score_companies(df, models, meta, horizon_key)
    df     = composite_rank(df)
    latest = (df.sort_values('fiscal_year', ascending=False)
                .drop_duplicates('cik', keep='first'))
    latest = latest[latest['composite_score'] >= min_composite].copy()
    latest = latest.sort_values('composite_score', ascending=False)
    if model_loaded and 'ml_score' in latest.columns:
        log_predictions(latest, horizon_key)

    st.title('Alpha Screener — Ranked by Multi-Factor Score')
    if model_loaded:
        model_status = f'ML model: {MODEL_LABELS.get(horizon_key, horizon_key)}'
        if wf_auc:
            model_status += f' (WF-AUC {wf_auc:.3f})'
    else:
        model_status = 'ML model not saved yet'
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
        c5.metric('Alpha score > 0.6', f'{(latest["ml_score"] > 0.6).sum():,}')
    else:
        c5.metric('Alpha score > 0.6', 'N/A')

    # Top signal features for the selected model (displayed once, applies to all companies)
    if model_loaded:
        top_feats = top_feature_importances(models, meta, horizon_key, top_n=6)
        if top_feats:
            with st.expander(f'Top signals driving alpha scores ({horizon_key} model)', expanded=False):
                feat_df = pd.DataFrame(top_feats, columns=['Feature', 'Importance', 'Factor Group'])
                max_imp = feat_df['Importance'].max()
                feat_df['Bar'] = feat_df['Importance'] / max(max_imp, 1)
                group_colors = {
                    'Value': '#2196F3', 'Quality': '#4CAF50', 'Momentum': '#FF9800',
                    'Growth': '#9C27B0', 'Fraud Risk': '#F44336', 'Other': '#607D8B',
                }
                fig_feat = go.Figure()
                for _, row in feat_df.iterrows():
                    fig_feat.add_trace(go.Bar(
                        x=[row['Importance']], y=[row['Feature']],
                        orientation='h',
                        name=row['Factor Group'],
                        marker_color=group_colors.get(row['Factor Group'], '#607D8B'),
                        showlegend=False,
                    ))
                fig_feat.update_layout(
                    height=220, margin=dict(l=0, r=0, t=10, b=0),
                    xaxis_title='Feature Importance', template='plotly_dark',
                    bargap=0.3,
                )
                col_f1, col_f2 = st.columns([3, 1])
                with col_f1:
                    st.plotly_chart(fig_feat, use_container_width=True)
                with col_f2:
                    for _, row in feat_df.iterrows():
                        color = group_colors.get(row['Factor Group'], '#607D8B')
                        st.markdown(
                            f'<span style="background:{color};color:white;padding:1px 6px;'
                            f'border-radius:3px;font-size:0.75em">{row["Factor Group"]}</span> {row["Feature"]}',
                            unsafe_allow_html=True,
                        )

    alpha_score_col = 'Alpha Score' if model_loaded and 'ml_score' in latest.columns else None
    display_latest = latest.copy()
    if alpha_score_col:
        display_latest = display_latest.rename(columns={'ml_score': alpha_score_col})

    st.subheader('Top Companies by Composite Alpha Score')
    ml_col_name = alpha_score_col if alpha_score_col else 'ml_score'
    display_cols = (['ticker', 'name', 'market', 'fiscal_year', 'composite_score'] +
                    ([c for c in ['sector'] if c in display_latest.columns]) +
                    [c for c in ['value_composite', 'pe_ratio', 'pb_ratio'] if c in display_latest.columns][:2] +
                    [c for c in ['quality_composite', 'piotroski_f_score', 'roe'] if c in display_latest.columns][:2] +
                    [c for c in ['momentum_12m_prior'] if c in display_latest.columns] +
                    [c for c in ['beneish_m_score', 'altman_z_score'] if c in display_latest.columns] +
                    ([ml_col_name] if model_loaded and ml_col_name in display_latest.columns else []) +
                    (['market_cap_at_filing'] if 'market_cap_at_filing' in display_latest.columns else []))
    display_cols = [c for c in display_cols if c in display_latest.columns]
    show_df = display_latest[display_cols].head(200).copy()
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

    # --- Human Review Queue ---
    st.markdown('---')
    st.subheader('Human Review Queue — Top 20 Highest Fraud Risk')
    st.caption(
        'Companies with the highest **fraud_score_composite** across all markets, '
        'regardless of current screener filters. These warrant manual analyst review '
        'before any investment decision.'
    )

    _RQ_COLS = [
        'ticker', 'name', 'market', 'fiscal_year',
        'fraud_score_composite', 'fraud_score_accounting', 'fraud_score_dilution',
        'fraud_score_quality', 'fraud_score_distress', 'data_confidence', 'beneish_m_score',
    ]

    if 'fraud_score_composite' in df_all.columns:
        rq_src = (df_all[df_all['period_type'] == 'annual'].copy()
                  if 'period_type' in df_all.columns else df_all.copy())
        rq_latest = (rq_src.sort_values('fiscal_year', ascending=False)
                     .drop_duplicates('ticker', keep='first'))
        rq_latest = rq_latest[rq_latest['fraud_score_composite'].notna()]
        rq_top = rq_latest.nlargest(20, 'fraud_score_composite').copy()

        rq_cols = [c for c in _RQ_COLS if c in rq_top.columns]
        rq_df = rq_top[rq_cols].copy()

        def _rq_risk(v):
            if pd.isna(v):
                return '—'
            return 'High' if v > 0.65 else 'Medium' if v > 0.35 else 'Low'

        rq_df.insert(
            rq_df.columns.get_loc('fraud_score_composite') + 1,
            'Risk Level',
            rq_df['fraud_score_composite'].map(_rq_risk),
        )

        def _conf_label(v):
            if pd.isna(v):
                return '—'
            v = float(v)
            if v >= 0.85:
                return 'High'
            elif v >= 0.70:
                return 'Good'
            elif v >= 0.55:
                return 'Medium'
            return 'Low'

        if 'data_confidence' in rq_df.columns:
            rq_df['data_confidence'] = rq_top['data_confidence'].map(_conf_label)

        for col in rq_df.select_dtypes('float').columns:
            rq_df[col] = rq_df[col].map(lambda x: f'{x:.3f}' if pd.notna(x) else '—')

        st.dataframe(rq_df, use_container_width=True, hide_index=True, height=420)
        rq_csv = rq_top[rq_cols].to_csv(index=False)
        st.download_button(
            'Download Review Queue CSV', rq_csv,
            'fraud_review_queue.csv', 'text/csv',
            key='dl_review_queue',
        )
    else:
        st.info('`fraud_score_composite` not found in dataset — run the full feature pipeline first.')

    st.markdown('---')
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
                                 delta='OK' if ok else 'Risk',
                                 delta_color='normal' if ok else 'inverse',
                                 help=_help)
            if 'composite_score' in row.index and pd.notna(row.get('composite_score')):
                rc4.metric('Composite Score',
                           f'{int(round(row["composite_score"] * 100))}/100',
                           help='Percentile rank across value, quality, momentum, fraud safety, ML')

            # Alpha score + top factor contributors for this company
            if model_loaded and 'ml_score' in latest.columns:
                ticker_row = latest[latest['ticker'] == selected_ticker]
                if not ticker_row.empty:
                    alpha_val = ticker_row.iloc[0].get('ml_score')
                    if pd.notna(alpha_val):
                        st.markdown('**Alpha Score (factor contributions)**')
                        a1, a2 = st.columns([1, 3])
                        with a1:
                            badge_txt, badge_clr = _model_confidence_badge(wf_auc)
                            st.metric(f'Alpha Score ({horizon_key})', f'{alpha_val:.3f}')
                            st.markdown(
                                f'<span style="background:{badge_clr};color:white;padding:2px 6px;'
                                f'border-radius:3px;font-size:0.75em">{badge_txt}</span>',
                                unsafe_allow_html=True,
                            )
                        with a2:
                            top_feats_co = top_feature_importances(models, meta, horizon_key, top_n=5)
                            if top_feats_co:
                                group_colors = {
                                    'Value': '#2196F3', 'Quality': '#4CAF50',
                                    'Momentum': '#FF9800', 'Growth': '#9C27B0',
                                    'Fraud Risk': '#F44336', 'Other': '#607D8B',
                                }
                                for fname, imp, fgroup in top_feats_co:
                                    fval = ticker_row.iloc[0].get(fname)
                                    val_str = f'{fval:.3f}' if pd.notna(fval) else 'N/A'
                                    color = group_colors.get(fgroup, '#607D8B')
                                    st.markdown(
                                        f'<span style="background:{color};color:white;padding:1px 5px;'
                                        f'border-radius:3px;font-size:0.72em">{fgroup}</span> '
                                        f'**{fname}** = {val_str}',
                                        unsafe_allow_html=True,
                                    )

            with st.expander('Price Chart (last 2 years)', expanded=True):
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
                with st.expander('Fundamental Trends', expanded=True):
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
                    with st.expander('Forward Return History (1y)', expanded=False):
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

    with st.expander('Dataset Overview Charts', expanded=False):
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
    csv = display_latest[display_cols].head(1000).to_csv(index=False)
    st.download_button('Download top 1000 as CSV', csv, 'screener_results.csv', 'text/csv')

