"""
Momentum factor: cross-sectional rank composite of price momentum signals.

High recent return = high momentum score. Score is 0–1, higher = stronger momentum.
"""

import numpy as np
import pandas as pd

_SIGNALS = [
    "momentum_12m_prior",
    "momentum_6m_prior",
    "momentum_3m_prior",
    "momentum_12m_rank",
    "momentum_6m_rank",
    "momentum_3m_rank",
]


def _cross_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True, na_option="keep")


def compute(df: pd.DataFrame, group_cols: tuple = ("fiscal_year", "market")) -> pd.Series:
    ranks = []
    for col in _SIGNALS:
        if col not in df.columns:
            continue
        r = df.groupby(list(group_cols))[col].transform(_cross_rank)
        ranks.append(r)

    if not ranks:
        return pd.Series(np.nan, index=df.index)

    return pd.concat(ranks, axis=1).mean(axis=1)
