"""
Growth factor: cross-sectional rank composite of revenue/earnings/cash growth signals.

High sustainable growth = high score. Score is 0–1, higher = better growth.
"""

import numpy as np
import pandas as pd

from pipeline.event_time_cohorts import attach_result_contract, event_time_rank

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
        r = event_time_rank(
            df, col, group_cols=group_cols, min_count=10, winsorize=True
        )
        ranks.append(r)

    if not ranks:
        return attach_result_contract(
            pd.Series(np.nan, index=df.index), "alpha_factor_ranks"
        )

    return attach_result_contract(
        pd.concat(ranks, axis=1).mean(axis=1), "alpha_factor_ranks"
    )
