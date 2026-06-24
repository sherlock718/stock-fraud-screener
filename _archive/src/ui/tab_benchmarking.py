from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)

_BASE = Path(__file__).resolve().parents[2]
_MODEL_META = _BASE / 'models' / 'model_meta.json'


def _compute_baselines(annual: pd.DataFrame, y: np.ndarray) -> list[dict]:
    rows = []
    specs = [
        ('Beneish M-Score',            'beneish_m_score',           False, 'Accounting manipulation — higher = more suspect'),
        ('Altman Z-Score',             'altman_z_score',            True,  'Financial distress — lower Z = higher distress'),
        ('Ohlson Bankruptcy Prob',     'ohlson_prob_bankruptcy',    True,  'Low-bankruptcy-risk companies more likely to be fraud (financial healthiness can mask fraud)'),
        ('Piotroski F-Score',          'piotroski_f_score',         True,  'Fundamental quality — weak fundamentals correlate with fraud'),
        ('Sloan Accruals',             'sloan_accruals',            False, 'Accruals ratio — high accruals signal earnings quality risk'),
        ('Fraud Score Composite',      'fraud_score_composite',     False, 'Our ensemble combination of all sub-scores'),
        ('Fraud Score Accounting',     'fraud_score_accounting',    False, 'Accounting manipulation sub-score'),
    ]
    for label, col, invert, note in specs:
        if col not in annual.columns:
            continue
        scores = annual[col].fillna(0).values
        s = -scores if invert else scores
        try:
            auc = roc_auc_score(y, s)
            ap  = average_precision_score(y, s)
        except Exception:
            continue
        rows.append({'Model': label, 'AUC-ROC': round(auc, 4), 'Avg Precision': round(ap, 4),
                     'Direction': 'Inverted' if invert else 'Direct', 'Note': note,
                     '_scores': s})
    return rows


def _compute_ml_models(annual: pd.DataFrame, y: np.ndarray) -> list[dict]:
    if not _MODEL_META.exists():
        return []
    meta = json.loads(_MODEL_META.read_text())
    rows = []
    for horizon in ['1y', '3y', '5y']:
        model_path = _BASE / 'models' / f'model_{horizon}.joblib'
        if not model_path.exists() or horizon not in meta:
            continue
        m = meta[horizon]
        feats = m['features']
        fill_vals = m.get('train_medians', {}) or {}
        X = pd.DataFrame(index=annual.index)
        for f in feats:
            if f in annual.columns:
                X[f] = annual[f].fillna(fill_vals.get(f) or 0.0)
            else:
                X[f] = float(fill_vals.get(f) or 0.0)
        try:
            model = joblib.load(str(model_path))
            proba = model.predict_proba(X)[:, 1]
            auc = roc_auc_score(y, proba)
            ap  = average_precision_score(y, proba)
            missing = len([f for f in feats if f not in annual.columns])
            rows.append({
                'Model': f'LightGBM ML ({horizon})',
                'AUC-ROC': round(auc, 4),
                'Avg Precision': round(ap, 4),
                'Direction': 'Direct',
                'Note': f'{len(feats)} features trained; {missing} filled with median/0 due to dataset version gap',
                '_scores': proba,
            })
        except Exception as e:
            rows.append({'Model': f'LightGBM ML ({horizon})', 'AUC-ROC': None,
                         'Avg Precision': None, 'Direction': '—', 'Note': f'Error: {e}',
                         '_scores': None})
    return rows


@st.cache_data(show_spinner='Computing benchmark scores…', ttl=3600)
def _benchmark_data(df_hash: int) -> tuple[list[dict], int, int]:
    df = pd.read_parquet(_BASE / 'data' / 'historical_dataset_clean.parquet')
    annual = df[df['period_type'] == 'annual'].copy() if 'period_type' in df.columns else df.copy()
    y = annual['fraud_confirmed'].fillna(0).astype(int).values
    baselines = _compute_baselines(annual, y)
    ml_rows   = _compute_ml_models(annual, y)
    return baselines + ml_rows, int(len(annual)), int(y.sum())


def tab_benchmarking(df_all: pd.DataFrame) -> None:
    st.title('📐 Model Benchmarking')
    st.caption(
        'AUC-ROC comparison of classical fraud-detection models vs our ML ensemble, '
        'evaluated on the full annual dataset using `fraud_confirmed` labels.'
    )

    df_hash = hash(len(df_all))
    all_rows, n_total, n_fraud = _benchmark_data(df_hash)

    valid = [r for r in all_rows if r['AUC-ROC'] is not None]
    valid_sorted = sorted(valid, key=lambda x: x['AUC-ROC'], reverse=True)

    # --- Dataset stats ---
    s1, s2, s3 = st.columns(3)
    s1.metric('Annual rows evaluated', f'{n_total:,}')
    s2.metric('Confirmed fraud cases', f'{n_fraud}')
    s3.metric('Fraud prevalence', f'{n_fraud / n_total * 100:.2f}%')

    st.markdown('---')

    # --- AUC bar chart ---
    st.subheader('AUC-ROC Comparison')
    models  = [r['Model'] for r in valid_sorted]
    aucs    = [r['AUC-ROC'] for r in valid_sorted]
    colors  = [
        '#EF5350' if 'LightGBM' in m or 'Composite' in m or 'Accounting' in m
        else '#42A5F5'
        for m in models
    ]

    fig_bar = go.Figure(go.Bar(
        x=aucs, y=models, orientation='h',
        marker_color=colors,
        text=[f'{a:.4f}' for a in aucs],
        textposition='outside',
    ))
    fig_bar.add_vline(x=0.5, line_dash='dash', line_color='grey',
                      annotation_text='Random (0.50)', annotation_position='top right')
    fig_bar.update_layout(
        height=max(300, 55 * len(models)),
        margin=dict(l=10, r=80, t=20, b=20),
        xaxis=dict(range=[0.3, 0.75], title='AUC-ROC'),
        yaxis=dict(autorange='reversed'),
        showlegend=False,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # --- Full results table ---
    st.subheader('Full Results Table')
    tbl_df = pd.DataFrame([
        {
            'Model': r['Model'],
            'AUC-ROC': f"{r['AUC-ROC']:.4f}" if r['AUC-ROC'] else '—',
            'Avg Precision': f"{r['Avg Precision']:.4f}" if r['Avg Precision'] else '—',
            'Direction': r['Direction'],
            'Note': r['Note'],
        }
        for r in valid_sorted
    ])
    st.dataframe(tbl_df, use_container_width=True, hide_index=True)

    # --- Precision-Recall curves ---
    st.markdown('---')
    st.subheader('Precision-Recall Curves')
    st.caption('Higher area under the curve (AP) = better at surfacing true fraud cases with minimal false positives.')

    df_pr = pd.read_parquet(_BASE / 'data' / 'historical_dataset_clean.parquet')
    ann_pr = df_pr[df_pr['period_type'] == 'annual'].copy() if 'period_type' in df_pr.columns else df_pr.copy()
    y_pr = ann_pr['fraud_confirmed'].fillna(0).astype(int).values

    fig_pr = go.Figure()
    color_palette = ['#EF5350', '#42A5F5', '#66BB6A', '#FFA726', '#AB47BC', '#26C6DA', '#FF7043', '#8D6E63', '#78909C']
    for i, row in enumerate(valid_sorted):
        if row.get('_scores') is None:
            continue
        precision, recall, _ = precision_recall_curve(y_pr, row['_scores'])
        fig_pr.add_trace(go.Scatter(
            x=recall, y=precision,
            mode='lines',
            name=f"{row['Model']} (AP={row['Avg Precision']:.4f})",
            line=dict(color=color_palette[i % len(color_palette)], width=2),
        ))
    prevalence = n_fraud / n_total
    fig_pr.add_hline(y=prevalence, line_dash='dot', line_color='grey',
                     annotation_text=f'Random ({prevalence:.4f})', annotation_position='top right')
    fig_pr.update_layout(
        height=400, margin=dict(t=20, b=20),
        xaxis_title='Recall', yaxis_title='Precision',
        legend=dict(orientation='h', y=-0.4),
    )
    st.plotly_chart(fig_pr, use_container_width=True)

    # --- ROC curves ---
    st.markdown('---')
    st.subheader('ROC Curves')
    fig_roc = go.Figure()
    for i, row in enumerate(valid_sorted):
        if row.get('_scores') is None:
            continue
        fpr, tpr, _ = roc_curve(y_pr, row['_scores'])
        fig_roc.add_trace(go.Scatter(
            x=fpr, y=tpr,
            mode='lines',
            name=f"{row['Model']} (AUC={row['AUC-ROC']:.4f})",
            line=dict(color=color_palette[i % len(color_palette)], width=2),
        ))
    fig_roc.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode='lines',
                                  name='Random', line=dict(dash='dash', color='grey')))
    fig_roc.update_layout(
        height=400, margin=dict(t=20, b=20),
        xaxis_title='False Positive Rate', yaxis_title='True Positive Rate',
        legend=dict(orientation='h', y=-0.4),
    )
    st.plotly_chart(fig_roc, use_container_width=True)

    # --- Methodology notes ---
    st.markdown('---')
    with st.expander('📋 Methodology & Caveats', expanded=False):
        st.markdown("""
**Evaluation dataset**
- Source: `historical_dataset_clean.parquet` — annual filings only
- Label: `fraud_confirmed` (binary 0/1) — curated from public fraud disclosures, regulatory actions, and SEC enforcement releases
- Class imbalance: ~0.37% positive rate (172 confirmed fraud out of 47,070 annual rows)

**Score directions**
- Beneish M-Score: higher = more manipulator (threshold −2.22)
- Altman Z-Score: lower = more financial distress (inverted for AUC)
- Ohlson Bankruptcy Probability: inverted — low-distress companies may still commit fraud; this signal is anti-correlated because healthy-looking companies are actually more likely to be confirmed fraud in our label set
- Piotroski F-Score: lower = weaker fundamentals (inverted for AUC)
- Sloan Accruals: higher = more accruals manipulation
- Fraud Score Composite / Accounting: our proprietary ensemble (higher = more risk)

**ML model evaluation caveat**
The LightGBM models were trained and evaluated on a specific train/test split. The AUCs here reflect **in-sample evaluation on all annual rows**, not the original holdout. Additionally, some features present at training time are missing in the current dataset version, filled with 0 or train medians — this depresses ML AUC vs the held-out training evaluation.

**AUC interpretation**
AUC = 0.50 is random. AUC = 1.0 is perfect. With 0.37% prevalence, even a strong AUC may translate to modest precision at any recall threshold — see the Precision-Recall curves for operational thresholds.
        """)
