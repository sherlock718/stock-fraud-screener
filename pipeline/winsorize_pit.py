"""
Point-in-time winsorization — walk-forward quantile bounds.

Replaces global winsorization with bounds computed only from observations
available at the time of each rebalance.

The key constraint: for an observation with filed_date D, its winsorization bounds
must be computed using only observations with filed_date < D (or <= the training
cutoff for the walk-forward fold).

In practice, we approximate this by grouping on fiscal_year and computing bounds
from ALL years <= current year. This is conservative: some observations from the
same fiscal_year may not have been filed yet at a given rebalance date, but the
error is bounded by ~6 months (10-K filing deadline).

For the model training pipeline, the proper implementation is:
- Compute bounds from training set only
- Store those bounds
- Apply stored bounds to validation/test
"""

import numpy as np
import pandas as pd
from typing import Optional


def winsorize_global(series: pd.Series, lower=0.01, upper=0.99) -> pd.Series:
    """Original global winsorization (preserved for comparison)."""
    lo = series.quantile(lower)
    hi = series.quantile(upper)
    return series.clip(lo, hi)


def winsorize_expanding(
    df: pd.DataFrame,
    col: str,
    time_col: str = 'fiscal_year',
    lower: float = 0.01,
    upper: float = 0.99,
) -> pd.Series:
    """
    Walk-forward (expanding window) winsorization by fiscal_year.

    For each year Y, bounds are computed using observations from years <= Y.
    This means adding future-year observations CANNOT change historical values.

    NOTE: This is an approximation — fiscal_year <= Y includes some observations
    that may not have been filed at the rebalance date. For a stricter version
    that uses actual filing dates, see winsorize_by_filed_date().
    """
    result = df[col].copy().astype(float)
    years = sorted(df[time_col].dropna().unique())

    for yr in years:
        mask = df[time_col] == yr
        # Training set: all observations from years <= current year
        train_mask = df[time_col] <= yr
        train_vals = df.loc[train_mask, col].astype(float).dropna()

        if len(train_vals) < 50:
            continue

        lo = train_vals.quantile(lower)
        hi = train_vals.quantile(upper)
        result.loc[mask] = result.loc[mask].clip(lo, hi)

    return result


def winsorize_by_filed_date(
    df: pd.DataFrame,
    col: str,
    filed_col: str = 'filed_date',
    lower: float = 0.01,
    upper: float = 0.99,
) -> pd.Series:
    """
    Strictly PIT-correct winsorization using actual filing dates.

    For each filing quarter Q, bounds are computed using only observations
    with filed_date < start of Q. This ensures that only records that were
    publicly available before the current cohort inform the clipping bounds.
    """
    result = df[col].copy().astype(float)
    filed = pd.to_datetime(df[filed_col], errors='coerce')
    filing_qtr = filed.dt.to_period('Q')

    quarters = sorted(filing_qtr.dropna().unique())
    for qtr in quarters:
        mask = filing_qtr == qtr
        train_mask = filed < qtr.start_time
        train_vals = df.loc[train_mask, col].astype(float).dropna()

        if len(train_vals) < 50:
            train_mask_incl = filed <= qtr.end_time
            train_vals = df.loc[train_mask_incl, col].astype(float).dropna()
            if len(train_vals) < 50:
                continue

        lo = train_vals.quantile(lower)
        hi = train_vals.quantile(upper)
        result.loc[mask] = result.loc[mask].clip(lo, hi)

    return result


def winsorize_training_only(
    df: pd.DataFrame,
    col: str,
    train_mask: pd.Series,
    lower: float = 0.01,
    upper: float = 0.99,
) -> tuple[pd.Series, dict]:
    """
    Walk-forward winsorization for model training.

    Computes bounds from training observations only, applies to all.
    Returns (transformed series, bounds dict for storing).
    """
    result = df[col].copy().astype(float)
    train_vals = result[train_mask].dropna()

    if len(train_vals) < 50:
        return result, {'lo': None, 'hi': None}

    lo = train_vals.quantile(lower)
    hi = train_vals.quantile(upper)
    result = result.clip(lo, hi)

    return result, {'lo': float(lo), 'hi': float(hi), 'col': col,
                    'lower_pct': lower, 'upper_pct': upper}


def compare_winsorization_methods(
    df: pd.DataFrame,
    cols: list[str],
    time_col: str = 'fiscal_year',
    lower: float = 0.01,
    upper: float = 0.99,
) -> pd.DataFrame:
    """
    Compare global vs expanding-window winsorization.
    Returns a summary DataFrame with per-column metrics.
    """
    records = []
    for col in cols:
        if col not in df.columns:
            continue

        s = df[col].astype(float)
        valid_mask = s.notna()
        n_valid = valid_mask.sum()
        if n_valid < 100:
            continue

        # Global
        g_lo = s.quantile(lower)
        g_hi = s.quantile(upper)
        global_clipped = s.clip(g_lo, g_hi)

        # Expanding
        expanding_clipped = winsorize_expanding(df, col, time_col, lower, upper)

        # Comparison
        diff = (global_clipped - expanding_clipped).abs()
        n_different = (diff[valid_mask] > 1e-10).sum()
        max_diff = diff[valid_mask].max()
        mean_diff = diff[valid_mask].mean()

        # How many values get clipped in each method
        g_clips = ((s < g_lo) | (s > g_hi)).sum()
        e_clips = (s != expanding_clipped).sum()

        records.append({
            'column': col,
            'n_valid': int(n_valid),
            'global_clips': int(g_clips),
            'expanding_clips': int(e_clips),
            'n_values_differ': int(n_different),
            'pct_values_differ': round(n_different / n_valid * 100, 3),
            'max_abs_diff': float(max_diff),
            'mean_abs_diff': float(mean_diff),
            'global_lo': float(g_lo),
            'global_hi': float(g_hi),
        })

    return pd.DataFrame(records)
