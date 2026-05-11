"""
Step 6 — Clean and validate the final dataset.

Applies minimal structural filters to preserve maximum ticker coverage.
Revenue, asset, and price thresholds have been removed — all tickers
are kept regardless of size or liquidity. Use p0f_universe_definition.py
with --apply-filters to compute an investable-universe subset.

Filters applied:
  1. Required columns present (cik, ticker, filed_date, fiscal_year, period_type)
  2. Filed date is valid and after 2008-01-01 (XBRL coverage starts ~2009)
  3. Remove duplicate (cik, market, filed_date, period_type) rows (keep first)
  4. Infinite values → NaN

Output: data/historical_dataset_clean.parquet
"""

import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'
IN   = DATA / 'historical_dataset.parquet'
OUT  = DATA / 'historical_dataset_clean.parquet'

# Minimum required columns — rows missing ANY of these are dropped
REQUIRED_COLS = ['cik', 'ticker', 'filed_date', 'fiscal_year', 'period_type']


def run():
    DATA.mkdir(exist_ok=True)
    print('Step 6 — Cleaning and validating dataset')

    if not IN.exists():
        print(f'ERROR: {IN} not found — run step 5 first')
        sys.exit(1)

    df = pd.read_parquet(IN)
    n_raw = len(df)
    print(f'  Raw dataset: {n_raw:,} rows × {len(df.columns)} columns')

    # ── Filter 1: Required columns present ───────────────────────────────────
    before = len(df)
    df = df.dropna(subset=REQUIRED_COLS)
    n_dropped = before - len(df)
    if n_dropped:
        print(f'  Dropped {n_dropped:,} rows: missing required columns')

    # ── Filter 2: Valid filed_date (after 2008-01-01) ─────────────────────────
    before = len(df)
    df['filed_date'] = pd.to_datetime(df['filed_date'], errors='coerce')
    df = df[df['filed_date'].notna() & (df['filed_date'] >= '2008-01-01')]
    n_dropped = before - len(df)
    if n_dropped:
        print(f'  Dropped {n_dropped:,} rows: invalid or pre-2008 filing date')

    # ── Filter 3: Remove duplicates ───────────────────────────────────────────
    before = len(df)
    dedup_key = ['cik', 'market', 'filed_date', 'period_type'] if 'market' in df.columns else ['cik', 'filed_date', 'period_type']
    df = df.drop_duplicates(subset=dedup_key, keep='first')
    n_dropped = before - len(df)
    if n_dropped:
        print(f'  Dropped {n_dropped:,} duplicate rows')

    # ── Filter 4: Replace infinities with NaN ────────────────────────────────
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    inf_mask = df[numeric_cols].isin([np.inf, -np.inf])
    n_inf = inf_mask.sum().sum()
    if n_inf:
        df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)
        print(f'  Replaced {n_inf:,} infinite values with NaN')

    # ── Point-in-Time columns ─────────────────────────────────────────────────
    # as_of_date: explicit alias for when data is legally public (= filed_date)
    # filing_lag_days: days from FY Dec-31 end to filing (negative = non-Dec FY)
    df['as_of_date'] = df['filed_date']
    fy_end = pd.to_datetime(df['fiscal_year'].astype(str) + '-12-31', errors='coerce')
    df['filing_lag_days'] = (df['filed_date'] - fy_end).dt.days

    # ── Sort ──────────────────────────────────────────────────────────────────
    df = df.sort_values(['ticker', 'filed_date', 'period_type']).reset_index(drop=True)

    # ── Save ──────────────────────────────────────────────────────────────────
    df.to_parquet(OUT, index=False)

    # ── Report ────────────────────────────────────────────────────────────────
    n_clean   = len(df)
    pct_kept  = 100 * n_clean / n_raw if n_raw > 0 else 0
    n_tickers = df['ticker'].nunique()

    print(f'\nStep 6 complete.')
    print(f'  Raw rows:     {n_raw:,}')
    print(f'  Clean rows:   {n_clean:,} ({pct_kept:.1f}% kept)')
    print(f'  Dropped:      {n_raw - n_clean:,}')
    print(f'  Tickers:      {n_tickers:,}')
    print(f'  Features:     {len(df.columns)}')

    if 'period_type' in df.columns:
        annual = (df['period_type'] == 'annual').sum()
        qtrly  = (df['period_type'] == 'quarterly').sum()
        print(f'  Annual rows:    {annual:,}')
        print(f'  Quarterly rows: {qtrly:,}')

    # Forward return coverage
    horizons = ['6m', '1y', '2y', '3y', '5y', '10y', '15y']
    print('\n  Forward return label coverage:')
    for h in horizons:
        col = f'forward_return_{h}'
        if col in df.columns:
            n = df[col].notna().sum()
            pct = 100 * n / n_clean if n_clean > 0 else 0
            beat_col = f'beat_local_market_{h}'
            n_beat = df[beat_col].notna().sum() if beat_col in df.columns else 0
            print(f'    {h:>4s}: {n:>8,} labeled ({pct:.0f}%) | beat_local_market: {n_beat:,}')

    # Column coverage report
    print('\n  Column fill rates (top 20 most empty):')
    fill_rates = df.notna().mean().sort_values()
    for col, rate in fill_rates.head(20).items():
        print(f'    {col:<45s}: {rate*100:.0f}%')

    print(f'\n  Saved: {OUT}')


if __name__ == '__main__':
    run()
