"""
Growth factor: cross-sectional rank composite of revenue/earnings/cash growth signals.

High sustainable growth = high score. Score is 0–1, higher = better growth.
"""

import numpy as np
import pandas as pd

_SIGNALS = [
    "revenue_cagr_3y",
    "revenue_growth_yoy",
    "eps_growth_yoy",
    "net_income_growth_yoy",
    "ocf_growth_yoy",
    "gross_profit_growth_yoy",
]


def _winsorize(s: pd.Series, q: float = 0.01) -> pd.Series:
    lo, hi = s.quantile(q), s.quantile(1 - q)
    return s.clip(lo, hi)


def _cross_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True, na_option="keep")


def compute(df: pd.DataFrame, group_cols: tuple = ("fiscal_year", "market")) -> pd.Series:
    ranks = []
    for col in _SIGNALS:
        if col not in df.columns:
            continue
        sig = df.groupby(list(group_cols))[col].transform(_winsorize)
        r = df.groupby(list(group_cols))[col].transform(_cross_rank)
        ranks.append(r)

    if not ranks:
        return pd.Series(np.nan, index=df.index)

    return pd.concat(ranks, axis=1).mean(axis=1)
