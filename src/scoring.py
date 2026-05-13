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


def resolve_horizon(horizon: str | int, meta: dict) -> str:
    """Resolve a horizon argument to a model key present in meta.

    Accepts:
        - A model key string: '6m', '1y', '2y', '3y', '5y'
        - An integer (months): routed via HorizonRouter
        - A legacy string like '1y' that's not in meta → nearest available key
    """
    from alpha.horizon_router import HorizonRouter
    if isinstance(horizon, int):
        key = HorizonRouter.route(horizon)
    else:
        key = str(horizon)
    if key in meta:
        return key
    # Fall back to nearest available key by month distance
    month_map = {'6m': 6, '1y': 12, '2y': 24, '3y': 36, '5y': 60}
    target = month_map.get(key, 12)
    available = [k for k in meta if k in month_map]
    if not available:
        return key
    return min(available, key=lambda k: abs(month_map[k] - target))


def score_companies(
    df: pd.DataFrame,
    models: dict,
    meta: dict,
    horizon: str | int = '1y',
    as_of_date: str | None = None,
) -> pd.DataFrame:
    """Score companies with the ML model for `horizon`.

    Args:
        horizon: Model key ('6m','1y','2y','3y','5y') or integer months.
            Integer months are routed to the nearest trained model via HorizonRouter.
        as_of_date: ISO date string (e.g. '2024-06-30'). When set, only rows with
            filed_date <= as_of_date are scored — prevents look-ahead in backtesting.
    """
    df = df.copy()

    if as_of_date and 'filed_date' in df.columns:
        cutoff = pd.Timestamp(as_of_date)
        filed  = pd.to_datetime(df['filed_date'], errors='coerce')
        df = df[filed.isna() | (filed <= cutoff)].copy()

    key = resolve_horizon(horizon, meta)

    if key not in models or key not in meta:
        df['ml_score'] = np.nan
        return df
    clf   = models[key]
    feats = [f for f in meta[key]['features'] if f in df.columns]
    train_medians = meta[key].get('train_medians', {})
    fill_vals = {f: train_medians.get(f, 0.0) for f in feats}
    X = df[feats].fillna(pd.Series(fill_vals))
    try:
        df['ml_score'] = clf.predict_proba(X)[:, 1]
    except Exception:
        df['ml_score'] = np.nan
    return df


def top_feature_importances(models: dict, meta: dict, key: str,
                              top_n: int = 10) -> list[tuple[str, float, str]]:
    """Return top N feature importances for a model key as (feature, importance, factor_group).

    Uses SHAP top features from model_meta.json if populated, otherwise falls back to
    LightGBM feature importances. Returns empty list if model is not loaded.
    """
    from alpha.horizon_router import HorizonRouter
    if key not in models or key not in meta:
        return []

    shap_top = meta[key].get('shap_top_features', [])
    if shap_top:
        results = []
        for item in shap_top[:top_n]:
            fname = item if isinstance(item, str) else item.get('feature', str(item))
            imp = item.get('importance', 1.0) if isinstance(item, dict) else 1.0
            results.append((fname, imp, HorizonRouter.factor_group(fname)))
        return results

    # Fallback: LightGBM feature_importances_
    clf = models[key]
    feats = meta[key].get('features', [])
    try:
        # Works for LightGBM and sklearn-wrapped models
        raw_model = clf
        if hasattr(clf, 'named_steps'):
            # Pipeline
            for step in reversed(list(clf.named_steps.values())):
                if hasattr(step, 'feature_importances_'):
                    raw_model = step
                    break
        importances = raw_model.feature_importances_
        pairs = sorted(zip(feats[:len(importances)], importances),
                       key=lambda x: x[1], reverse=True)
        return [(f, float(i), HorizonRouter.factor_group(f)) for f, i in pairs[:top_n]]
    except AttributeError:
        return [(f, 1.0, HorizonRouter.factor_group(f)) for f in feats[:top_n]]


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
