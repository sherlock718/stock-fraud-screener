from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.charts import beneish_radar, fraud_taxonomy_radar
from src.features import _BENEISH_LABELS, _FEATURE_GROUPS
from src.narrative import generate_narrative

_TAXONOMY_COLS = [
    'fraud_score_accounting',
    'fraud_score_dilution',
    'fraud_score_quality',
    'fraud_score_distress',
    'fraud_score_governance',
]
_TAXONOMY_LABELS = {
    'fraud_score_accounting': 'Accounting Manipulation',
    'fraud_score_dilution':   'Dilution Fraud',
    'fraud_score_quality':    'Earnings Quality',
    'fraud_score_distress':   'Financial Distress',
    'fraud_score_governance': 'Governance Fraud',
}


def _confidence_badge(score: float) -> str:
    if score >= 0.85:
        return f'🟢 High ({score:.2f})'
    elif score >= 0.70:
        return f'🟡 Good ({score:.2f})'
    elif score >= 0.55:
        return f'🟠 Medium ({score:.2f})'
    else:
        return f'🔴 Low ({score:.2f})'


def _risk_label(score: float) -> str:
    if score > 0.70:
        return '🔴 High Risk'
    elif score > 0.40:
        return '🟠 Elevated Risk'
    else:
        return '🟢 Low Risk'


def tab_company_profile(df_all: pd.DataFrame, models: dict, meta: dict) -> None:
    st.title('🏢 Company Deep Dive')

    tickers = sorted(df_all['ticker'].unique().tolist())
    col_sel, col_year = st.columns([3, 1])
    with col_sel:
        selected_ticker = st.selectbox('Select ticker', tickers, key='profile_ticker')
    company_df = df_all[df_all['ticker'] == selected_ticker].sort_values('fiscal_year')
    if company_df.empty:
        st.warning('No data found for this ticker.')
        return

    avail_years = sorted(company_df['fiscal_year'].unique().tolist(), reverse=True)
    with col_year:
        selected_year = st.selectbox('Fiscal year', avail_years, index=0, key='profile_year')

    row = company_df[company_df['fiscal_year'] == selected_year].iloc[0]
    name_label = str(row.get('name', selected_ticker) or selected_ticker)
    mkt_label  = str(row.get('market', ''))

    # --- Header with confidence badge ---
    conf = row.get('data_confidence')
    header_cols = st.columns([4, 1])
    with header_cols[0]:
        st.subheader(f'{name_label}  ·  {selected_ticker}  ·  {mkt_label}  ·  FY{int(selected_year)}')
    with header_cols[1]:
        if pd.notna(conf):
            st.metric('Data Confidence', _confidence_badge(float(conf)))

    def _fmt(v, decimals=2, suffix=''):
        return f'{v:.{decimals}f}{suffix}' if pd.notna(v) else '—'

    # --- Key metrics row ---
    cols = st.columns(6)
    bm  = row.get('beneish_m_score')
    az  = row.get('altman_z_score')
    pf  = row.get('piotroski_f_score')
    cs  = row.get('composite_score')
    sl  = row.get('sloan_accruals')
    fsc = row.get('fraud_score_composite')

    cols[0].metric('Beneish M',
                   _fmt(bm),
                   delta='⚠️ manipulator' if pd.notna(bm) and bm > -2.22 else '✅ safe',
                   delta_color='inverse' if pd.notna(bm) and bm > -2.22 else 'off')
    cols[1].metric('Altman Z',
                   _fmt(az),
                   delta='distress' if pd.notna(az) and az < 1.81 else
                         'grey' if pd.notna(az) and az < 2.99 else 'safe')
    cols[2].metric('Piotroski F', f'{int(pf)}/9' if pd.notna(pf) else '—')
    cols[3].metric('Composite Score', _fmt(cs))
    cols[4].metric('Sloan Accruals', _fmt(sl, 3))
    if pd.notna(fsc):
        cols[5].metric('Fraud Score', _fmt(fsc, 3),
                       delta=_risk_label(float(fsc)),
                       delta_color='inverse' if fsc > 0.40 else 'off')
    else:
        cols[5].metric('Fraud Score', '—')

    # --- Fraud Taxonomy Section ---
    st.markdown('---')
    avail_taxonomy = [c for c in _TAXONOMY_COLS if c in row.index and pd.notna(row.get(c))]
    if avail_taxonomy:
        st.subheader('🚨 Fraud Taxonomy Breakdown')
        tc1, tc2 = st.columns([1, 1])
        with tc1:
            tax_fig = fraud_taxonomy_radar(row)
            if tax_fig:
                st.plotly_chart(tax_fig, use_container_width=True)
        with tc2:
            tax_rows = []
            for c in _TAXONOMY_COLS:
                val = row.get(c)
                if pd.notna(val):
                    v = float(val)
                    risk = '🔴 High' if v > 0.65 else '🟠 Medium' if v > 0.35 else '🟢 Low'
                    tax_rows.append({'Fraud Type': _TAXONOMY_LABELS[c],
                                     'Score': round(v, 3), 'Risk': risk})
                else:
                    tax_rows.append({'Fraud Type': _TAXONOMY_LABELS[c],
                                     'Score': None, 'Risk': 'N/A'})
            tax_df = pd.DataFrame(tax_rows)
            st.dataframe(tax_df, use_container_width=True, hide_index=True)

        # Year-over-year taxonomy change
        if len(avail_years) > 1:
            prev_year = avail_years[1] if len(avail_years) > 1 else None
            if prev_year:
                prev_row = company_df[company_df['fiscal_year'] == prev_year]
                if not prev_row.empty:
                    prev = prev_row.iloc[0]
                    changes = []
                    for c in avail_taxonomy:
                        curr_v = row.get(c)
                        prev_v = prev.get(c)
                        if pd.notna(curr_v) and pd.notna(prev_v):
                            delta = float(curr_v) - float(prev_v)
                            if abs(delta) >= 0.05:
                                direction = '⬆️ Worsened' if delta > 0 else '⬇️ Improved'
                                changes.append(f'**{_TAXONOMY_LABELS[c]}**: {delta:+.3f} ({direction})')
                    if changes:
                        st.caption(f'YoY changes vs FY{int(prev_year)}: ' + ' · '.join(changes))

    # --- Score History ---
    st.markdown('---')
    trend_cols = [c for c in ['fraud_score_composite', 'beneish_m_score', 'altman_z_score',
                               'composite_score', 'piotroski_f_score', 'sloan_accruals']
                  if c in company_df.columns]
    if trend_cols:
        st.subheader('Score History')
        selected_trends = st.multiselect(
            'Metrics to plot', trend_cols,
            default=[c for c in ['fraud_score_composite', 'beneish_m_score'] if c in trend_cols],
            key='profile_trends',
        )
        if selected_trends:
            fig_trend = go.Figure()
            colors = ['#EF5350', '#42A5F5', '#66BB6A', '#FFA726', '#AB47BC']
            for i, sc in enumerate(selected_trends):
                data_sc = company_df[['fiscal_year', sc]].dropna()
                fig_trend.add_trace(go.Scatter(
                    x=data_sc['fiscal_year'], y=data_sc[sc],
                    mode='lines+markers',
                    name=sc.replace('_', ' ').title(),
                    line=dict(color=colors[i % len(colors)], width=2),
                    marker=dict(size=7),
                ))
            if 'beneish_m_score' in selected_trends:
                fig_trend.add_hline(y=-2.22, line_dash='dash', line_color='red',
                                    annotation_text='Beneish threshold (-2.22)')
            fig_trend.update_layout(height=320, margin=dict(t=10, b=20),
                                    xaxis_title='Fiscal Year', yaxis_title='Score')
            st.plotly_chart(fig_trend, use_container_width=True)

    # --- Beneish Component Radar ---
    beneish_available = [c for c in _BENEISH_LABELS if c in row.index and pd.notna(row.get(c))]
    if beneish_available:
        st.markdown('---')
        c1, c2 = st.columns([1, 1])
        with c1:
            st.subheader('Beneish Component Radar')
            radar_fig = beneish_radar(row)
            if radar_fig:
                st.plotly_chart(radar_fig, use_container_width=True)
        with c2:
            st.subheader('Beneish Component Values')
            beneish_df = pd.DataFrame([
                {'Component': _BENEISH_LABELS[c], 'Value': round(float(row[c]), 4),
                 'Flag': '⚠️' if float(row[c]) > 1.0 else '✅'}
                for c in _BENEISH_LABELS if c in row.index and pd.notna(row.get(c))
            ])
            st.dataframe(beneish_df, use_container_width=True, hide_index=True)

    # --- Feature Deep Dive ---
    st.markdown('---')
    st.subheader('Feature Deep Dive')
    for group_name, feature_list in _FEATURE_GROUPS.items():
        avail = [f for f in feature_list if f in row.index and pd.notna(row.get(f))]
        if not avail:
            continue
        with st.expander(f'{group_name}  ({len(avail)} features)', expanded=(group_name == '🚨 Fraud Signals')):
            n_per_row = 4
            for chunk_start in range(0, len(avail), n_per_row):
                chunk = avail[chunk_start:chunk_start + n_per_row]
                metric_cols = st.columns(n_per_row)
                for j, feat in enumerate(chunk):
                    val = row[feat]
                    metric_cols[j].metric(feat.replace('_', ' ').title(), f'{val:.4f}')

    with st.expander('📋 All numeric features (latest year)', expanded=False):
        numeric_row = {
            k: round(float(v), 5)
            for k, v in row.items()
            if isinstance(v, (int, float, np.floating, np.integer)) and pd.notna(v)
        }
        full_df = pd.DataFrame(list(numeric_row.items()), columns=['Feature', 'Value'])
        st.dataframe(full_df, use_container_width=True, height=420, hide_index=True)

    # --- ML Score Breakdown ---
    if models and meta:
        st.markdown('---')
        st.subheader('ML Score Breakdown')
        horizon_sel = st.selectbox('Horizon', [h for h in ['1y', '3y', '5y'] if h in models],
                                   key='profile_ml_horizon')
        if horizon_sel and horizon_sel in meta:
            m = meta[horizon_sel]
            feats = [f for f in m['features'] if f in row.index]
            fill_vals = m.get('train_medians', {})
            X_single = pd.DataFrame([{f: (row[f] if pd.notna(row.get(f)) else fill_vals.get(f, 0.0))
                                       for f in feats}])
            try:
                prob = models[horizon_sel].predict_proba(X_single)[0, 1]
                st.metric(f'ML Score ({horizon_sel})', f'{prob:.4f}',
                          delta='High risk' if prob > 0.7 else 'Medium' if prob > 0.4 else 'Low risk')
                try:
                    import shap
                    explainer = shap.TreeExplainer(models[horizon_sel])
                    shap_vals = explainer.shap_values(X_single)
                    if isinstance(shap_vals, list):
                        sv = shap_vals[1][0]
                    else:
                        sv = shap_vals[0] if shap_vals.ndim == 2 else shap_vals
                    shap_df_full = pd.DataFrame({'Feature': feats, 'SHAP': sv}).sort_values('SHAP', key=abs, ascending=False)
                    shap_df = shap_df_full.head(15)
                    fig_shap = px.bar(shap_df, x='SHAP', y='Feature', orientation='h',
                                      color='SHAP', color_continuous_scale='RdBu_r',
                                      title=f'Top 15 SHAP values ({horizon_sel})')
                    fig_shap.update_layout(height=420, margin=dict(t=40, b=20))
                    st.plotly_chart(fig_shap, use_container_width=True)
                    st.markdown('---')
                    with st.expander('📖 Strengths & Weaknesses Story', expanded=True):
                        generate_narrative(
                            shap_df_full, row, selected_ticker, int(selected_year),
                            horizon_sel, df_all, prob,
                        )
                except ImportError:
                    st.info('Install shap (`pip install shap`) to see feature-level SHAP explanations.')
                except Exception:
                    pass
            except Exception as e:
                st.error(f'Could not score: {e}')
