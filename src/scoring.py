from __future__ import annotations

import numpy as np
import pandas as pd


def score_companies(
    df: pd.DataFrame,
    models: dict,
    meta: dict,
    horizon: str = '1y',
) -> pd.DataFrame:
    if horizon not in models or horizon not in meta:
        df['ml_score'] = np.nan
        return df
    clf   = models[horizon]
    feats = [f for f in meta[horizon]['features'] if f in df.columns]
    train_medians = meta[horizon].get('train_medians', {})
    fill_vals = {f: train_medians.get(f, 0.0) for f in feats}
    X = df[feats].fillna(pd.Series(fill_vals))
    try:
        df = df.copy()
        df['ml_score'] = clf.predict_proba(X)[:, 1]
    except Exception:
        df['ml_score'] = np.nan
    return df


def composite_rank(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def pct_rank(s: pd.Series, ascending: bool = True) -> pd.Series:
        return s.rank(pct=True, ascending=ascending, na_option='keep')

    components: dict[str, pd.Series] = {}
    if 'value_composite' in df.columns:
        components['value']   = pct_rank(df['value_composite'], ascending=False)
    elif 'pe_ratio' in df.columns:
        components['value']   = pct_rank(df['pe_ratio'], ascending=True)
    if 'quality_composite' in df.columns:
        components['quality'] = pct_rank(df['quality_composite'], ascending=False)
    elif 'piotroski_f_score' in df.columns:
        components['quality'] = pct_rank(df['piotroski_f_score'], ascending=False)
    if 'momentum_12m_prior' in df.columns:
        components['momentum']     = pct_rank(df['momentum_12m_prior'], ascending=False)
    if 'beneish_m_score' in df.columns:
        components['fraud_safety'] = pct_rank(df['beneish_m_score'], ascending=True)
    if 'ml_score' in df.columns and df['ml_score'].notna().any():
        components['ml_alpha']     = pct_rank(df['ml_score'], ascending=False)

    if not components:
        df['composite_score'] = np.nan
        return df

    weights = {'value': 0.25, 'quality': 0.20, 'momentum': 0.20,
               'fraud_safety': 0.20, 'ml_alpha': 0.15}
    total_w = sum(weights[k] for k in components)
    df['composite_score'] = sum(
        components[k] * (weights[k] / total_w) for k in components
    )
    return df
