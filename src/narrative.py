from __future__ import annotations

import pandas as pd
import streamlit as st

from src.features import _FEATURE_DESCRIPTIONS


def generate_narrative(
    shap_df_full: pd.DataFrame,
    row: pd.Series,
    ticker: str,
    year: int,
    horizon: str,
    df_all: pd.DataFrame,
    prob: float,
) -> None:
    """Render strengths/weaknesses SHAP story for the selected company."""
    latest_year = int(df_all['fiscal_year'].max())
    ref = df_all[df_all['fiscal_year'] == latest_year]
    medians = ref.median(numeric_only=True)

    concerns  = shap_df_full[shap_df_full['SHAP'] > 0].head(4)
    strengths = shap_df_full[shap_df_full['SHAP'] < 0].head(4)

    def _bullet(feat: str) -> str:
        info  = _FEATURE_DESCRIPTIONS.get(feat, {})
        label = info.get('label', feat.replace('_', ' ').title())
        fmt   = info.get('fmt', '.3f')
        desc  = info.get('desc', '')
        val   = row.get(feat)
        med   = medians.get(feat)

        val_str = f'{val:{fmt}}' if pd.notna(val) else 'N/A'
        if pd.notna(val) and feat in ref.columns and ref[feat].notna().any():
            pct = float((ref[feat].dropna() < val).mean()) * 100
            comparison = f'top {100 - pct:.0f}% of market' if pct >= 50 else f'bottom {pct:.0f}% of market'
            med_str = f'; median {med:{fmt}}' if pd.notna(med) else ''
            context = f'({comparison}{med_str})'
        elif pd.notna(med):
            context = f'(market median: {med:{fmt}})'
        else:
            context = ''

        return f'**{label}** = {val_str} {context}. {desc}'

    score_label = 'High Risk' if prob > 0.70 else 'Elevated Risk' if prob > 0.40 else 'Low Risk'

    st.markdown(f'#### 📖 Model Story — {ticker} · FY{year} · {horizon} horizon')
    st.caption(
        f'Composite fraud probability: **{prob:.3f}** ({score_label}). '
        f'The features below had the largest influence on this score.'
    )

    col_s, col_c = st.columns(2)

    with col_s:
        st.markdown('**✅ Strengths** — features *reducing* fraud probability')
        if strengths.empty:
            st.write('No dominant strength signals for this horizon.')
        else:
            for _, r in strengths.iterrows():
                st.markdown(f'- {_bullet(r["Feature"])}')

    with col_c:
        st.markdown('**⚠️ Concerns** — features *elevating* fraud probability')
        if concerns.empty:
            st.write('No dominant concern signals for this horizon.')
        else:
            for _, r in concerns.iterrows():
                st.markdown(f'- {_bullet(r["Feature"])}')
