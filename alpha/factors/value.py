"""
Value factor: cross-sectional rank composite of cheap-stock signals.

All input signals are winsorized at 1st/99th percentile before ranking.
Low valuation = high value score (inverse rank for ratio signals).
Score is 0–1, higher = better value.
"""

import numpy as np
import pandas as pd

from pipeline.event_time_cohorts import attach_result_contract, event_time_rank

_SIGNALS = [
    ("ev_ebitda",       True),   # low EV/EBITDA = better value → invert
    ("ev_revenue",      True),
    ("fcf_yield",       False),  # high FCF yield = better value → no invert
    ("earnings_yield",  False),
    ("book_to_market",  False),
    ("ps_ratio",        True),
    ("pe_ratio",        True),
]


def _winsorize(s: pd.Series, q: float = 0.01) -> pd.Series:
    lo, hi = s.quantile(q), s.quantile(1 - q)
    return s.clip(lo, hi)


def _cross_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True, na_option="keep")


def compute(df: pd.DataFrame, group_cols: tuple = ("fiscal_year", "market")) -> pd.Series:
    """Return cross-sectional value rank score (0–1) aligned to df.index."""
    ranks = []
    for col, invert in _SIGNALS:
        if col not in df.columns:
            continue
        r = event_time_rank(
            df, col, group_cols=group_cols, min_count=10, winsorize=True
        )
        if invert:
            r = 1.0 - r
        ranks.append(r)

    if not ranks:
        return attach_result_contract(
            pd.Series(np.nan, index=df.index), "alpha_factor_ranks"
        )

    return attach_result_contract(
        pd.concat(ranks, axis=1).mean(axis=1), "alpha_factor_ranks"
    )
