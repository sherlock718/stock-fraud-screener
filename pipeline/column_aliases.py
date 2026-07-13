"""Column aliasing from step2 → step5 naming conventions.

step2 uses descriptive suffix names (e.g., revenue_growth_yoy);
step5 uses shorter standard names (e.g., revenue_growth).
Both survive in the final dataset — originals are never removed.
"""
from __future__ import annotations

import pandas as pd

COLUMN_ALIASES = {
    'equity':                  'total_equity',
    'receivables':             'accounts_receivable',
    'revenue_growth_yoy':      'revenue_growth',
    'net_income_growth_yoy':   'net_income_growth',
    'asset_growth_yoy':        'assets_growth',
    'debt_growth_yoy':         'debt_growth',
    'receivables_growth_yoy':  'receivables_growth',
    'inventory_growth_yoy':    'inventory_growth',
    'ap_growth_yoy':           'ap_growth',
    'ocf_growth_yoy':          'ocf_growth',
    'capex_growth_yoy':        'capex_growth',
    'gross_profit_growth_yoy': 'gross_profit_growth',
    'sga_growth_yoy':          'sga_growth',
    'rd_growth_yoy':           'rd_growth',
    'eps_growth_yoy':          'eps_growth',
    'equity_change_yoy':       'equity_growth',
    'ppe_growth_yoy':          'ppe_growth',
    'cash_change_yoy':         'cash_growth',
    'cogs_growth_yoy':         'cogs_growth',
    'shares_dilution':         'shares_growth',
    'shares_outstanding':      'common_shares_outstanding',
}

COALESCE_ALIASES = {'equity', 'sga_expense'}


def apply_column_aliases(df: pd.DataFrame) -> pd.DataFrame:
    """Add alias columns without removing originals. Safe to call multiple times."""
    for src, dst in COLUMN_ALIASES.items():
        if src not in df.columns:
            continue
        if dst not in df.columns:
            df[dst] = df[src]
        elif src in COALESCE_ALIASES:
            df[dst] = df[src].combine_first(df[dst])
    if 'asset_growth_yoy' in df.columns and 'current_assets_growth' not in df.columns:
        df['current_assets_growth'] = df['asset_growth_yoy']
    return df
