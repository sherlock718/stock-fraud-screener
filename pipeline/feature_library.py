"""Shared feature engineering helpers used by both backtester and train_models."""
from __future__ import annotations

import numpy as np
import pandas as pd


def add_normalised_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Divide raw dollar items by total_assets and compute effective_tax_rate."""
    ta  = df.get('total_assets')
    pti = df.get('pretax_income')
    if ta is not None:
        ta_safe = ta.replace(0, np.nan)
        for src, dst in [
            ('intangibles',         'intangibles_to_assets'),
            ('goodwill',            'goodwill_to_assets'),
            ('depreciation',        'depreciation_to_assets'),
            ('financing_cash_flow', 'financing_cashflow_to_assets'),
            ('fcf',                 'fcf_to_assets'),
        ]:
            if src in df.columns and dst not in df.columns:
                df[dst] = df[src] / ta_safe
    if 'tax_expense' in df.columns and pti is not None and 'effective_tax_rate' not in df.columns:
        pos = pti > 0
        df['effective_tax_rate'] = np.nan
        df.loc[pos, 'effective_tax_rate'] = df.loc[pos, 'tax_expense'] / pti[pos]
    return df


def add_piotroski_ext(df: pd.DataFrame) -> pd.DataFrame:
    """Add the three missing Piotroski signals (shares, delta_gm, delta_at) and piotroski_f_score_9."""
    df = df.sort_values(['ticker', 'fiscal_year'])
    for src, name in [
        ('shares_outstanding', 'piotroski_shares_ok'),
        ('gross_margin',       'piotroski_delta_gm'),
        ('asset_turnover',     'piotroski_delta_at'),
    ]:
        if src in df.columns:
            df[name] = df.groupby('ticker')[src].transform(
                lambda x: (x <= x.shift(1)).astype(float) if src == 'shares_outstanding'
                else (x > x.shift(1)).astype(float)
            )
    extra = [c for c in ['piotroski_shares_ok', 'piotroski_delta_gm', 'piotroski_delta_at']
             if c in df.columns]
    if extra and 'piotroski_f_score' in df.columns:
        df['piotroski_f_score_9'] = (
            df['piotroski_f_score'].astype('float64') + df[extra].sum(axis=1, min_count=1)
        )
    return df
