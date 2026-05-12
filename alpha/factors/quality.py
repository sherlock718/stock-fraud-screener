"""
Quality factor: cross-sectional rank composite of earnings quality signals.

High quality = high score. Accruals inverted (low accruals = better quality).
Score is 0–1, higher = better quality.
"""

import numpy as np
import pandas as pd

_SIGNALS = [
    ("roe",                  False),
    ("roa",                  False),
    ("roic",                 False),
    ("gross_margin",         False),
    ("operating_margin",     False),
    ("ocf_to_ni",            False),
    ("piotroski_f_score",    False),
    ("accruals_to_assets",   True),   # high accruals = worse quality → invert
    ("sloan_accruals",       True),
    ("gross_profit_to_assets", False),
]


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

    if not ranks:
        return pd.Series(np.nan, index=df.index)

    return pd.concat(ranks, axis=1).mean(axis=1)
