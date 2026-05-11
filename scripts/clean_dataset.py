"""
Apply data quality fixes to historical_dataset_clean.parquet in-place.

Fixes applied:
  1. Filter fiscal_year to 2008–2025
  2. Deduplicate annual (ticker, fiscal_year) — keep row with larger total_assets
  3. Winsorize revenue_growth_yoy at p1–p99
  4. Clip forward_return columns at -1 to +5
  5. Add likely_delisted flag (last filed ≥3 years before dataset max year)
  6. Add point-in-time columns: as_of_date, source_timestamp (Phase 0a)
"""

from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).parent.parent
SRC  = BASE / 'data' / 'historical_dataset_clean.parquet'


def run(src: Path = SRC, inplace: bool = True):
    df = pd.read_parquet(src)
    print(f'Loaded: {len(df):,} rows, {df["cik"].nunique():,} companies')

    # ── 1. Fiscal year filter ────────────────────────────────────────────────
    bad_fy = (df['fiscal_year'] < 2008) | (df['fiscal_year'] > 2025)
    print(f'  Drop fiscal_year outside 2008-2025: {bad_fy.sum()} rows')
    df = df[~bad_fy].copy()

    # ── 2. Deduplicate annual rows ───────────────────────────────────────────
    ann_mask = df['period_type'] == 'annual'
    ann = df[ann_mask].copy()
    dup_mask = ann.duplicated(['ticker', 'fiscal_year'], keep=False)
    if dup_mask.sum() > 0:
        print(f'  Dedup annual (ticker, fiscal_year): {dup_mask.sum()} rows → keep larger entity')
        ann = (
            ann.sort_values('total_assets', ascending=False, na_position='last')
               .drop_duplicates(['ticker', 'fiscal_year'], keep='first')
        )
        df = pd.concat([df[~ann_mask], ann], ignore_index=True)

    # ── 3. Winsorize revenue_growth_yoy ─────────────────────────────────────
    if 'revenue_growth_yoy' in df.columns:
        lo = df['revenue_growth_yoy'].quantile(0.01)
        hi = df['revenue_growth_yoy'].quantile(0.99)
        before_max = df['revenue_growth_yoy'].max()
        df['revenue_growth_yoy'] = df['revenue_growth_yoy'].clip(lo, hi)
        print(f'  Winsorize revenue_growth_yoy: max {before_max:.1f} → {hi:.2f}')

    # ── 4. Clip forward returns ──────────────────────────────────────────────
    fwd_cols = [c for c in df.columns if c.startswith('forward_return')]
    for col in fwd_cols:
        before_max = df[col].max()
        df[col] = df[col].clip(-1, 5)
        if before_max > 5:
            print(f'  Clip {col}: max {before_max:.1f} → 5.0')

    # ── 5. likely_delisted flag ──────────────────────────────────────────────
    max_year = int(df['fiscal_year'].max())
    last_filed = df.groupby('cik')['fiscal_year'].max().rename('last_fiscal_year')
    df = df.merge(last_filed, on='cik', how='left')
    df['likely_delisted'] = (max_year - df['last_fiscal_year']) >= 3
    df = df.drop(columns=['last_fiscal_year'])
    n_delisted = df['cik'].isin(df.loc[df['likely_delisted'], 'cik']).sum()
    print(f'  likely_delisted: {df["likely_delisted"].any() and df.groupby("cik")["likely_delisted"].first().sum()} companies flagged')

    # ── 6. Point-in-time columns (Phase 0a) ─────────────────────────────────
    # as_of_date: earliest date the data was publicly available (= filed_date).
    # This is the correct "knowledge cutoff" for backtesting — not fiscal_year_end.
    if 'filed_date' in df.columns:
        df['as_of_date'] = pd.to_datetime(df['filed_date'], errors='coerce')
    else:
        # Fallback: estimate as April 30 of fiscal_year+1 (SEC 10-K deadline)
        df['as_of_date'] = pd.to_datetime(
            (df['fiscal_year'] + 1).astype(str) + '-04-30', errors='coerce'
        )
    # source_timestamp: when this version of the data was processed
    df['source_timestamp'] = datetime.now(timezone.utc).isoformat()
    print(f'  as_of_date: added ({df["as_of_date"].notna().sum():,} non-null)')
    print(f'  source_timestamp: {df["source_timestamp"].iloc[0]}')

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f'\nAfter cleaning: {len(df):,} rows, {df["cik"].nunique():,} companies')
    ann_clean = df[df['period_type'] == 'annual']
    print(f'  Annual rows: {len(ann_clean):,}')
    print(f'  forward_return_1y labeled: {df["forward_return_1y"].notna().sum():,}')
    print(f'  likely_delisted companies: {df.groupby("cik")["likely_delisted"].first().sum():,}')

    out = src if inplace else src.with_name(src.stem + '_cleaned.parquet')
    df.to_parquet(out, index=False)
    print(f'\nSaved: {out}')


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--no-inplace', action='store_true', help='Write to *_cleaned.parquet instead')
    args = p.parse_args()
    run(inplace=not args.no_inplace)
