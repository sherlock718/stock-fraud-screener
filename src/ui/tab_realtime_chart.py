from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def tab_realtime_chart(df_all: pd.DataFrame) -> None:
    st.title('📈 Realtime Price Chart')
    st.caption('Live price data via yfinance · fraud score overlay from screener dataset')

    col1, col2, col3 = st.columns([3, 1, 1])
    tickers = sorted(df_all['ticker'].unique().tolist())
    with col1:
        ticker = st.selectbox('Ticker', tickers, key='chart_ticker')
    with col2:
        period = st.selectbox('Period', ['6mo', '1y', '2y', '3y', '5y'], index=2, key='chart_period')
    with col3:
        chart_type = st.selectbox('Chart type', ['Candlestick', 'Line', 'OHLC'], key='chart_type')

    try:
        import yfinance as yf
        with st.spinner(f'Fetching {ticker}…'):
            hist = yf.Ticker(ticker).history(period=period)

        if hist.empty:
            st.warning(f'No price data for {ticker}. Try a US-listed ticker (e.g. AAPL, MSFT).')
        else:
            hist.index = pd.to_datetime(hist.index)

            fig_price = go.Figure()
            if chart_type == 'Candlestick':
                fig_price.add_trace(go.Candlestick(
                    x=hist.index, open=hist['Open'], high=hist['High'],
                    low=hist['Low'], close=hist['Close'],
                    name=ticker, increasing_line_color='#26A69A', decreasing_line_color='#EF5350',
                ))
            elif chart_type == 'OHLC':
                fig_price.add_trace(go.Ohlc(
                    x=hist.index, open=hist['Open'], high=hist['High'],
                    low=hist['Low'], close=hist['Close'], name=ticker,
                ))
            else:
                fig_price.add_trace(go.Scatter(
                    x=hist.index, y=hist['Close'], mode='lines',
                    name=ticker, line=dict(color='#42A5F5', width=2),
                ))

            co_df = df_all[df_all['ticker'] == ticker].sort_values('fiscal_year')
            if not co_df.empty and 'beneish_m_score' in co_df.columns:
                for _, yr_row in co_df.iterrows():
                    bm = yr_row.get('beneish_m_score')
                    fy = yr_row.get('fiscal_year')
                    if pd.isna(bm) or pd.isna(fy):
                        continue
                    x_pos = f'{int(fy)}-12-31'
                    fig_price.add_vline(x=x_pos, line_dash='dot', line_color='grey',
                                        annotation_text=f'FY{int(fy)} M={bm:.1f}',
                                        annotation_font_size=9)

            fig_price.update_layout(
                title=f'{ticker} — {period} Price Chart',
                xaxis_rangeslider_visible=False,
                height=460,
                margin=dict(t=50, b=20),
                yaxis_title='Price (USD)',
            )
            st.plotly_chart(fig_price, use_container_width=True)

            fig_vol = go.Figure(go.Bar(
                x=hist.index, y=hist['Volume'],
                marker_color='#78909C', name='Volume', opacity=0.6,
            ))
            fig_vol.update_layout(height=150, margin=dict(t=5, b=20),
                                  yaxis_title='Volume', showlegend=False)
            st.plotly_chart(fig_vol, use_container_width=True)

    except ImportError:
        st.error('yfinance is not installed — run `pip install yfinance`')
    except Exception as e:
        st.error(f'Could not load price data: {e}')

    co_df = df_all[df_all['ticker'] == ticker].sort_values('fiscal_year')
    if not co_df.empty:
        st.markdown('---')
        st.subheader(f'Fraud Score Timeline — {ticker}')

        score_options = [c for c in ['beneish_m_score', 'altman_z_score', 'piotroski_f_score',
                                      'composite_score', 'sloan_accruals', 'value_composite',
                                      'quality_composite'] if c in co_df.columns]
        selected_scores = st.multiselect(
            'Scores to overlay', score_options,
            default=[c for c in ['beneish_m_score', 'composite_score'] if c in co_df.columns],
            key='chart_scores_overlay',
        )
        if selected_scores:
            fig_scores = go.Figure()
            colors = ['#EF5350', '#42A5F5', '#66BB6A', '#FFA726', '#AB47BC', '#26C6DA', '#EC407A']
            for i, sc in enumerate(selected_scores):
                data_sc = co_df[['fiscal_year', sc]].dropna()
                fig_scores.add_trace(go.Bar(
                    x=data_sc['fiscal_year'], y=data_sc[sc],
                    name=sc.replace('_', ' ').title(),
                    marker_color=colors[i % len(colors)],
                    opacity=0.75,
                ))
            if 'beneish_m_score' in selected_scores:
                fig_scores.add_hline(y=-2.22, line_dash='dash', line_color='red',
                                     annotation_text='Beneish threshold')
            fig_scores.update_layout(height=300, barmode='group',
                                     margin=dict(t=10, b=20),
                                     xaxis_title='Fiscal Year')
            st.plotly_chart(fig_scores, use_container_width=True)

        with st.expander('Key financials by year', expanded=False):
            fin_cols = [c for c in ['fiscal_year', 'revenue', 'net_income', 'total_assets',
                                     'total_debt', 'cash', 'operating_cash_flow',
                                     'gross_margin', 'net_margin', 'roe', 'roa']
                        if c in co_df.columns]
            st.dataframe(co_df[fin_cols].set_index('fiscal_year').sort_index(ascending=False),
                         use_container_width=True)
