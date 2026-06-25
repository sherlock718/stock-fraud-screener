"""
Fraud Risk factor: cross-sectional rank composite of fraud and distress signals.

HIGH score = LOWER fraud risk = SAFER company. All signals are inverted so that
a high fraud_risk score means you can trust the company more.

Signals used:
  - beneish_m_score     (high = manipulator → invert)
  - ohlson_prob_bankruptcy  (high = distress → invert)
  - altman_z_score      (high = healthy → no invert)
  - fraud_score_composite   (high = risky → invert)
  - ml_1y_oof, ml_3y_oof, ml_5y_oof (unbiased OOF; high = likely to beat market → no invert)
                             Note: ML scores proxy fundamental quality, not fraud
"""

import numpy as np
import pandas as pd

_SIGNALS = [
    ("beneish_m_score",         True),
    ("ohlson_prob_bankruptcy",  True),
    ("altman_z_score",          False),
    ("fraud_score_composite",   True),
    ("fraud_score_accounting",  True),
    ("fraud_score_distress",    True),
]

_ML_SIGNALS = ["ml_1y_oof", "ml_3y_oof", "ml_5y_oof"]


def _winsorize(s: pd.Series, q: float = 0.01) -> pd.Series:
    lo, hi = s.quantile(q), s.quantile(1 - q)
    return s.clip(lo, hi)


def _cross_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True, na_option="keep")


def compute(df: pd.DataFrame, group_cols: tuple = ("fiscal_year", "market")) -> pd.Series:
    ranks = []
    for col, invert in _SIGNALS:
        if col not in df.columns:
            continue
        r = df.groupby(list(group_cols))[col].transform(_cross_rank)
        if invert:
            r = 1.0 - r
        ranks.append(r)

    for col in _ML_SIGNALS:
        if col not in df.columns:
            continue
        r = df.groupby(list(group_cols))[col].transform(_cross_rank)
        ranks.append(r)

    if not ranks:
        return pd.Series(np.nan, index=df.index)

    return pd.concat(ranks, axis=1).mean(axis=1)
