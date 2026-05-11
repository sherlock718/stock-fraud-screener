from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

_LOG_PATH = Path(__file__).parent.parent / 'data' / 'prediction_log.csv'
_LOG_HEADER = ['logged_at', 'ticker', 'horizon', 'ml_score', 'composite_score', 'fiscal_year']


def _ensure_log() -> None:
    if not _LOG_PATH.exists():
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _LOG_PATH.open('w', newline='') as f:
            csv.writer(f).writerow(_LOG_HEADER)


def log_predictions(df: pd.DataFrame, horizon: str) -> None:
    """Append scored rows to the prediction log (one row per ticker per call)."""
    _ensure_log()
    now = datetime.now(timezone.utc).isoformat(timespec='seconds')
    score_col = 'ml_score' if 'ml_score' in df.columns else None
    comp_col  = 'composite_score' if 'composite_score' in df.columns else None
    rows = []
    for _, r in df.iterrows():
        rows.append([
            now,
            r.get('ticker', ''),
            horizon,
            round(float(r[score_col]), 4) if score_col and pd.notna(r.get(score_col)) else '',
            round(float(r[comp_col]),  4) if comp_col  and pd.notna(r.get(comp_col))  else '',
            int(r['fiscal_year']) if pd.notna(r.get('fiscal_year')) else '',
        ])
    with _LOG_PATH.open('a', newline='') as f:
        csv.writer(f).writerows(rows)


def score_companies(
    df: pd.DataFrame,
    models: dict,
    meta: dict,
    horizon: str = '1y',
    as_of_date: str | None = None,
) -> pd.DataFrame:
    """Score companies with the ML model for `horizon`.

    Args:
        as_of_date: ISO date string (e.g. '2024-06-30'). When set, only rows with
            filed_date <= as_of_date are scored — prevents look-ahead in backtesting.
    """
    df = df.copy()

    if as_of_date and 'filed_date' in df.columns:
        cutoff = pd.Timestamp(as_of_date)
        filed  = pd.to_datetime(df['filed_date'], errors='coerce')
        df = df[filed.isna() | (filed <= cutoff)].copy()

    if horizon not in models or horizon not in meta:
        df['ml_score'] = np.nan
        return df
    clf   = models[horizon]
    feats = [f for f in meta[horizon]['features'] if f in df.columns]
    train_medians = meta[horizon].get('train_medians', {})
    fill_vals = {f: train_medians.get(f, 0.0) for f in feats}
    X = df[feats].fillna(pd.Series(fill_vals))
    try:
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
