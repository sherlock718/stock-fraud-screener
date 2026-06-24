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


_COVERAGE_GROUPS: dict[str, list[str]] = {
    'Financial Core': [
        'revenue', 'net_income', 'total_assets', 'total_equity',
        'operating_cash_flow', 'gross_profit', 'operating_income',
    ],
    'Fraud Signals': [
        'beneish_m_score', 'altman_z_score', 'piotroski_f_score', 'sloan_accruals',
    ],
    'Price / Returns': ['entry_price', 'forward_return_1y'],
    'Ratios': [
        'net_margin', 'roe', 'roa', 'ocf_margin', 'debt_to_equity', 'current_ratio',
    ],
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


def _confidence_detail(row: pd.Series, meta: dict) -> None:
    """Render expandable confidence breakdown: coverage groups + ML feature counts."""
    conf = row.get('data_confidence')
    label = _confidence_badge(float(conf)) if pd.notna(conf) else '—'

    with st.expander(f'Data Confidence: {label}', expanded=False):
        st.caption(
            'Confidence = average of Coverage (core column completeness), '
            'Consistency (accounting sanity checks), and Timeliness (filing lag + vintage).'
        )

        # Coverage breakdown by group
        st.markdown('**Core Feature Coverage**')
        cov_cols = st.columns(4)
        for i, (group, cols) in enumerate(_COVERAGE_GROUPS.items()):
            avail   = [c for c in cols if c in row.index]
            present = sum(1 for c in avail if pd.notna(row.get(c)))
            total   = len(avail)
            pct     = present / total if total else 0
            icon    = '✅' if pct == 1.0 else ('🟡' if pct >= 0.7 else '🔴')
            missing = [c for c in avail if pd.isna(row.get(c))]
            cov_cols[i].metric(group, f'{present}/{total}', delta=icon)
            if missing:
                cov_cols[i].caption('Missing: ' + ', '.join(missing))

        # ML feature coverage per horizon
        if meta:
            st.markdown('**ML Model Feature Coverage**')
            ml_cols = st.columns(3)
            for i, horizon in enumerate(['1y', '3y', '5y']):
                if horizon not in meta:
                    continue
                feats   = meta[horizon].get('features', [])
                present = sum(1 for f in feats if f in row.index and pd.notna(row.get(f)))
                total   = len(feats)
                pct     = present / total if total else 0
                icon    = '✅' if pct >= 0.90 else ('🟡' if pct >= 0.75 else '🔴')
                missing = [f for f in feats if not (f in row.index and pd.notna(row.get(f)))]
                ml_cols[i].metric(f'{horizon} horizon', f'{present}/{total} features', delta=icon)
                if missing:
                    ml_cols[i].caption('Missing: ' + ', '.join(missing[:5]) + ('…' if len(missing) > 5 else ''))


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

    # --- Header ---
    st.subheader(f'{name_label}  ·  {selected_ticker}  ·  {mkt_label}  ·  FY{int(selected_year)}')
    _confidence_detail(row, meta)

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

    # --- Peer Comparison ---
    st.markdown('---')
    st.subheader('🏆 Peer Comparison')

    # Determine peer group: SIC code preferred, market as fallback
    peer_col, peer_val, peer_label = None, None, ''
    if 'sic_code' in row.index and pd.notna(row.get('sic_code')):
        peer_col, peer_val = 'sic_code', row['sic_code']
        peer_label = f'SIC {peer_val}'
    elif 'market' in row.index and pd.notna(row.get('market')):
        peer_col, peer_val = 'market', row['market']
        peer_label = f'Market {peer_val}'

    if peer_col is None:
        st.info('No sector / market grouping available for peer comparison.')
    else:
        src_peer = (df_all[df_all['period_type'] == 'annual']
                    if 'period_type' in df_all.columns else df_all)
        peer_df  = src_peer[src_peer[peer_col] == peer_val]
        peer_latest = (peer_df.sort_values('fiscal_year', ascending=False)
                              .drop_duplicates('ticker', keep='first'))

        _PEER_METRICS = [
            'fraud_score_composite', 'beneish_m_score', 'altman_z_score',
            'piotroski_f_score', 'composite_score',
        ]
        peer_metrics = [c for c in _PEER_METRICS
                        if c in peer_latest.columns and peer_latest[c].notna().any()]

        if len(peer_latest) < 3 or not peer_metrics:
            st.info(f'Too few peers in {peer_label} (n={len(peer_latest)}) to compare.')
        else:
            st.caption(
                f'Comparing **{selected_ticker}** against {len(peer_latest):,} companies '
                f'in {peer_label} (latest annual filing each).'
            )

            pct_rows = []
            for metric in peer_metrics:
                my_val = row.get(metric)
                if pd.isna(my_val):
                    continue
                vals = peer_latest[metric].dropna()
                if len(vals) < 3:
                    continue
                my_val_f   = float(my_val)
                better_cnt = int((vals > my_val_f).sum())   # higher = better for most
                pct        = round((vals <= my_val_f).mean() * 100, 1)
                pct_rows.append({
                    'Metric':      metric.replace('_', ' ').title(),
                    '_col':        metric,
                    'My Value':    round(my_val_f, 3),
                    'Peer Median': round(float(vals.median()), 3),
                    'p25':         round(float(vals.quantile(0.25)), 3),
                    'p75':         round(float(vals.quantile(0.75)), 3),
                    'Percentile':  pct,
                    'Better Than': f'{better_cnt:,} / {len(vals):,}',
                })

            if pct_rows:
                disp_df = pd.DataFrame(pct_rows).drop(columns=['_col'])
                st.dataframe(disp_df, use_container_width=True, hide_index=True)

                # Box-plot distribution for a chosen metric
                box_choices = [r['Metric'] for r in pct_rows]
                box_metric_label = st.selectbox(
                    'Distribution chart', box_choices, key='profile_peer_metric'
                )
                orig_col = next(
                    (r['_col'] for r in pct_rows if r['Metric'] == box_metric_label), None
                )
                if orig_col:
                    vals_all = peer_latest[orig_col].dropna().tolist()
                    my_v     = float(row.get(orig_col, np.nan))
                    fig_box  = go.Figure()
                    fig_box.add_trace(go.Box(
                        y=vals_all, name=peer_label,
                        boxpoints='outliers',
                        marker_color='#42A5F5',
                        line_color='#1565C0',
                    ))
                    if pd.notna(my_v):
                        fig_box.add_hline(
                            y=my_v, line_color='red', line_dash='dash',
                            annotation_text=f'{selected_ticker}: {my_v:.3f}',
                            annotation_position='right',
                        )
                    fig_box.update_layout(
                        height=320, margin=dict(t=30, b=20),
                        yaxis_title=box_metric_label,
                        title=f'{box_metric_label} — {peer_label} distribution',
                    )
                    st.plotly_chart(fig_box, use_container_width=True)

                # Top / bottom peers table
                sort_col = peer_metrics[0]
                with st.expander(f'📋 Top / Bottom 10 peers by {sort_col.replace("_", " ").title()}',
                                 expanded=False):
                    show_cols = ['ticker', 'name'] + peer_metrics[:4]
                    show_cols = [c for c in show_cols if c in peer_latest.columns]
                    pc1, pc2 = st.columns(2)
                    pc1.caption('Lowest risk (best)')
                    pc1.dataframe(
                        peer_latest.nsmallest(10, sort_col)[show_cols],
                        use_container_width=True, hide_index=True,
                    )
                    pc2.caption('Highest risk (worst)')
                    pc2.dataframe(
                        peer_latest.nlargest(10, sort_col)[show_cols],
                        use_container_width=True, hide_index=True,
                    )
