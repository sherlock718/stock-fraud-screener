from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def tab_market_overview(df_all: pd.DataFrame) -> None:
    st.title('🌍 Market Risk Overview')

    col_yr, col_metric = st.columns([1, 2])
    with col_yr:
        year_sel = st.selectbox(
            'Year', sorted(df_all['fiscal_year'].unique().tolist(), reverse=True),
            index=0, key='overview_year',
        )
    df_yr = df_all[df_all['fiscal_year'] == year_sel].copy()

    risk_metric_options = [c for c in ['beneish_m_score', 'altman_z_score', 'composite_score',
                                        'sloan_accruals', 'piotroski_f_score'] if c in df_yr.columns]
    with col_metric:
        risk_metric = st.selectbox('Risk metric', risk_metric_options, key='overview_metric')

    if not risk_metric:
        st.warning('No risk metrics found in dataset.')
        return

    st.markdown(f'**FY{int(year_sel)} · {len(df_yr):,} companies · metric: `{risk_metric}`**')

    kpi_cols = st.columns(4)
    metric_vals = df_yr[risk_metric].dropna()
    kpi_cols[0].metric('Companies', f'{len(df_yr):,}')
    kpi_cols[1].metric('Mean', f'{metric_vals.mean():.3f}' if len(metric_vals) else '—')
    kpi_cols[2].metric('Median', f'{metric_vals.median():.3f}' if len(metric_vals) else '—')
    if risk_metric == 'beneish_m_score':
        pct_risky = (metric_vals > -2.22).mean() * 100 if len(metric_vals) else 0
        kpi_cols[3].metric('% Risky (M > −2.22)', f'{pct_risky:.1f}%')
    elif risk_metric == 'altman_z_score':
        pct_distress = (metric_vals < 1.81).mean() * 100 if len(metric_vals) else 0
        kpi_cols[3].metric('% Distress (Z < 1.81)', f'{pct_distress:.1f}%')
    else:
        kpi_cols[3].metric('Std Dev', f'{metric_vals.std():.3f}' if len(metric_vals) else '—')

    st.markdown('---')

    col_dist, col_top = st.columns([1, 1])
    with col_dist:
        st.subheader('Score Distribution')
        fig_dist = px.histogram(df_yr, x=risk_metric, nbins=60,
                                color_discrete_sequence=['#42A5F5'],
                                labels={risk_metric: risk_metric.replace('_', ' ').title()})
        if risk_metric == 'beneish_m_score':
            fig_dist.add_vline(x=-2.22, line_dash='dash', line_color='red',
                               annotation_text='Manipulation threshold (−2.22)')
        elif risk_metric == 'altman_z_score':
            fig_dist.add_vline(x=1.81, line_dash='dash', line_color='red',
                               annotation_text='Distress threshold (1.81)')
            fig_dist.add_vline(x=2.99, line_dash='dash', line_color='orange',
                               annotation_text='Grey zone (2.99)')
        fig_dist.update_layout(height=350, margin=dict(t=20, b=20))
        st.plotly_chart(fig_dist, use_container_width=True)

    with col_top:
        st.subheader('Top 20 Highest Risk')
        if risk_metric == 'beneish_m_score':
            top_risky = df_yr.nlargest(20, risk_metric)
        else:
            top_risky = df_yr.nsmallest(20, risk_metric)

        show_cols = [c for c in ['ticker', 'name', 'market', 'exchange', risk_metric,
                                  'altman_z_score', 'piotroski_f_score', 'composite_score']
                     if c in top_risky.columns]
        st.dataframe(top_risky[show_cols].reset_index(drop=True),
                     use_container_width=True, hide_index=True)

    if 'market' in df_yr.columns:
        st.markdown('---')
        st.subheader(f'{risk_metric.replace("_", " ").title()} by Market')
        mkt_agg = (df_yr.groupby('market')[risk_metric]
                      .agg(['mean', 'median', 'count'])
                      .rename(columns={'mean': 'Mean', 'median': 'Median', 'count': 'Count'})
                      .reset_index()
                      .sort_values('Mean', ascending=(risk_metric not in ('beneish_m_score',))))

        fig_mkt = go.Figure()
        fig_mkt.add_trace(go.Bar(x=mkt_agg['market'], y=mkt_agg['Mean'],
                                  name='Mean', marker_color='#EF5350'))
        fig_mkt.add_trace(go.Bar(x=mkt_agg['market'], y=mkt_agg['Median'],
                                  name='Median', marker_color='#42A5F5'))
        fig_mkt.update_layout(barmode='group', height=350, margin=dict(t=20, b=20),
                               yaxis_title=risk_metric.replace('_', ' ').title())
        st.plotly_chart(fig_mkt, use_container_width=True)

    st.markdown('---')
    st.subheader(f'{risk_metric.replace("_", " ").title()} Over Time')
    time_agg = (df_all.groupby('fiscal_year')[risk_metric]
                    .agg(['mean', 'median'])
                    .rename(columns={'mean': 'Mean', 'median': 'Median'})
                    .reset_index())
    fig_time = go.Figure()
    fig_time.add_trace(go.Scatter(x=time_agg['fiscal_year'], y=time_agg['Mean'],
                                   mode='lines+markers', name='Mean',
                                   line=dict(color='#EF5350', width=2)))
    fig_time.add_trace(go.Scatter(x=time_agg['fiscal_year'], y=time_agg['Median'],
                                   mode='lines+markers', name='Median',
                                   line=dict(color='#42A5F5', width=2, dash='dot')))
    if risk_metric == 'beneish_m_score':
        fig_time.add_hline(y=-2.22, line_dash='dash', line_color='red',
                           annotation_text='Threshold')
    fig_time.update_layout(height=320, margin=dict(t=10, b=20),
                            xaxis_title='Fiscal Year',
                            yaxis_title=risk_metric.replace('_', ' ').title())
    st.plotly_chart(fig_time, use_container_width=True)

    scatter_y = 'quality_composite' if 'quality_composite' in df_yr.columns else 'piotroski_f_score'
    if scatter_y in df_yr.columns:
        st.markdown('---')
        st.subheader(f'Risk vs Quality Scatter — FY{int(year_sel)}')
        scatter_df = df_yr[[risk_metric, scatter_y, 'ticker', 'market']].dropna()
        if len(scatter_df) > 10:
            fig_scatter = px.scatter(
                scatter_df.head(2000),
                x=risk_metric, y=scatter_y,
                color='market',
                hover_data=['ticker'],
                opacity=0.55,
                labels={
                    risk_metric: risk_metric.replace('_', ' ').title(),
                    scatter_y:   scatter_y.replace('_', ' ').title(),
                },
            )
            if risk_metric == 'beneish_m_score':
                fig_scatter.add_vline(x=-2.22, line_dash='dash', line_color='red')
            fig_scatter.update_layout(height=400, margin=dict(t=20, b=20))
            st.plotly_chart(fig_scatter, use_container_width=True)
