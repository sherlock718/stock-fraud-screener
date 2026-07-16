"""
Momentum factor: cross-sectional rank composite of price momentum signals.

High recent return = high momentum score. Score is 0–1, higher = stronger momentum.
"""

import numpy as np
import pandas as pd

from pipeline.event_time_cohorts import attach_result_contract, event_time_rank

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
        r = event_time_rank(df, col, group_cols=group_cols, min_count=10)
        ranks.append(r)

    if not ranks:
        return attach_result_contract(
            pd.Series(np.nan, index=df.index), "alpha_factor_ranks"
        )

    return attach_result_contract(
        pd.concat(ranks, axis=1).mean(axis=1), "alpha_factor_ranks"
    )
