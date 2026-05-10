"""
Step 6 — Clean and validate the final dataset.

Applies data quality filters to produce a reliable ML-ready dataset.
Strict enough to remove junk rows, lenient enough to preserve OTC/delisted companies.

Filters applied:
  1. Must have a valid entry price (can't invest without a price)
  2. Revenue > $1M (remove shell/blank-check companies)
  3. Total assets > $100K (minimum plausibility)
  4. Filed date is valid and after 2008-01-01 (XBRL coverage starts ~2009)
  5. Remove duplicate (cik, filed_date, period_type) rows (keep first)
  6. Infinite values → NaN
  7. Report column coverage statistics

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

    # ── Filter 3: Minimum revenue ($1M) ──────────────────────────────────────
    if 'revenue' in df.columns:
        before = len(df)
        rev = pd.to_numeric(df['revenue'], errors='coerce')
        df = df[rev.isna() | (rev >= 1e6)]
        n_dropped = before - len(df)
        if n_dropped:
            print(f'  Dropped {n_dropped:,} rows: revenue < $1M')

    # ── Filter 4: Minimum total assets ($100K) ────────────────────────────────
    if 'total_assets' in df.columns:
        before = len(df)
        ta = pd.to_numeric(df['total_assets'], errors='coerce')
        df = df[ta.isna() | (ta >= 1e5)]
        n_dropped = before - len(df)
        if n_dropped:
            print(f'  Dropped {n_dropped:,} rows: total assets < $100K')

    # ── Filter 5: Keep only rows with an entry price ──────────────────────────
    if 'entry_price' in df.columns:
        before = len(df)
        ep = pd.to_numeric(df['entry_price'], errors='coerce')
        df = df[ep.notna() & (ep > 0)]
        n_dropped = before - len(df)
        if n_dropped:
            print(f'  Dropped {n_dropped:,} rows: no valid entry price')

    # ── Filter 6: Remove duplicates ───────────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=['cik', 'filed_date', 'period_type'], keep='first')
    n_dropped = before - len(df)
    if n_dropped:
        print(f'  Dropped {n_dropped:,} duplicate (cik, filed_date, period_type) rows')

    # ── Filter 7: Replace infinities with NaN ─────────────────────────────────
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    inf_mask = df[numeric_cols].isin([np.inf, -np.inf])
    n_inf = inf_mask.sum().sum()
    if n_inf:
        df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)
        print(f'  Replaced {n_inf:,} infinite values with NaN')

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
