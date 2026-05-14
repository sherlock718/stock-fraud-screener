#!/usr/bin/env python3
"""
merge_snapshots.py — Combine US + KR + EU + BR + JP + CA snapshots into unified files.

Usage:
  python3 scripts/merge_snapshots.py               # dry run — show counts only
  python3 scripts/merge_snapshots.py --activate    # write combined files as the active dataset
  python3 scripts/merge_snapshots.py --activate --backup  # backup existing files first

What it does:
  1. Loads whichever per-market snapshot files exist (US, KR, EU, BR, JP, CA)
  2. Aligns schemas (union of all columns, NaN for missing)
  3. Writes data/snapshots_combined.parquet + data/prices_combined.parquet
  4. With --activate: copies combined → snapshots.parquet + prices.parquet
     (these are the default paths for steps 4-6)

After --activate, run:
  python3 scripts/run_pipeline.py build --step 4   # macro → features → clean on combined dataset
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pandas as pd

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'

SNAPSHOT_FILES = {
    'US': DATA / 'snapshots.parquet',
    'KR': DATA / 'snapshots_kr.parquet',
    'EU': DATA / 'snapshots_eu.parquet',
    'BR': DATA / 'snapshots_br.parquet',
    'JP': DATA / 'snapshots_jp.parquet',
    'CA': DATA / 'snapshots_ca.parquet',
}

PRICES_FILES = {
    'US': DATA / 'prices.parquet',
    'KR': DATA / 'prices_kr.parquet',
    'EU': DATA / 'prices_eu.parquet',
    'BR': DATA / 'prices_br.parquet',
    'JP': DATA / 'prices_jp.parquet',
    'CA': DATA / 'prices_ca.parquet',
}

OUT_SNAPSHOTS = DATA / 'snapshots_combined.parquet'
OUT_PRICES    = DATA / 'prices_combined.parquet'


def load_existing(files: dict[str, Path]) -> dict[str, pd.DataFrame]:
    loaded = {}
    for label, path in files.items():
        if path.exists():
            df = pd.read_parquet(path)
            loaded[label] = df
            print(f'  ✓ {label}: {len(df):,} rows, {df.columns.nunique()} cols — {path.name}')
        else:
            print(f'  ○ {label}: not found — {path.name}')
    return loaded


def align_and_concat(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Union all columns; fill missing with NaN. Deduplicate on (cik, filed_date, period_type)."""
    dfs = list(frames.values())
    all_cols = []
    seen = set()
    for df in dfs:
        for c in df.columns:
            if c not in seen:
                all_cols.append(c)
                seen.add(c)

    aligned = []
    for label, df in frames.items():
        missing = [c for c in all_cols if c not in df.columns]
        if missing:
            df = df.copy()
            for c in missing:
                df[c] = float('nan')
        aligned.append(df[all_cols])

    combined = pd.concat(aligned, ignore_index=True)

    # Normalise fiscal_year to int64 (some markets store as str)
    if 'fiscal_year' in combined.columns:
        combined['fiscal_year'] = pd.to_numeric(combined['fiscal_year'], errors='coerce').astype('Int64')

    # Deduplicate: include market to prevent cross-market CIK collisions
    key_cols = ['cik', 'market', 'filed_date', 'period_type']
    key_cols = [c for c in key_cols if c in combined.columns]
    if key_cols:
        before = len(combined)
        combined = combined.drop_duplicates(subset=key_cols, keep='last')
        dupes = before - len(combined)
        if dupes:
            print(f'  Removed {dupes:,} duplicate rows (same cik+market+filed_date+period_type)')

    return combined


def summary(df: pd.DataFrame, label: str):
    print(f'\n── {label} ────────────────────────────────────')
    print(f'  Rows:    {len(df):,}')
    print(f'  Columns: {len(df.columns)}')
    if 'market' in df.columns:
        print(f'  Markets: {df["market"].value_counts().to_dict()}')
    if 'cik' in df.columns:
        print(f'  Companies: {df["cik"].nunique():,}')
    if 'period_type' in df.columns:
        print(f'  Period types: {df["period_type"].value_counts().to_dict()}')
    if 'filed_date' in df.columns:
        print(f'  Date range: {df["filed_date"].min()} → {df["filed_date"].max()}')


def run(activate: bool, backup: bool):
    DATA.mkdir(exist_ok=True)
    print('merge_snapshots.py — Combining market datasets')
    print(f'  Activate: {activate}  |  Backup: {backup}')

    # ── Snapshots ──────────────────────────────────────────────────────────────
    print('\nLoading snapshots:')
    snap_frames = load_existing(SNAPSHOT_FILES)

    if not snap_frames:
        print('ERROR: no snapshot files found — run at least one market pipeline first')
        sys.exit(1)

    if len(snap_frames) == 1:
        label, df = next(iter(snap_frames.items()))
        print(f'\nOnly {label} snapshots found — nothing to merge yet.')
        print('Combined = single market data.')
        combined_snap = df.copy()
    else:
        combined_snap = align_and_concat(snap_frames)

    summary(combined_snap, 'Combined Snapshots')
    combined_snap.to_parquet(OUT_SNAPSHOTS, index=False)
    print(f'\n  Saved: {OUT_SNAPSHOTS}')

    # ── Prices ─────────────────────────────────────────────────────────────────
    print('\nLoading prices:')
    price_frames = load_existing(PRICES_FILES)

    if price_frames:
        if len(price_frames) == 1:
            combined_prices = next(iter(price_frames.values())).copy()
        else:
            combined_prices = align_and_concat(price_frames)
        summary(combined_prices, 'Combined Prices')
        combined_prices.to_parquet(OUT_PRICES, index=False)
        print(f'\n  Saved: {OUT_PRICES}')
    else:
        print('  No prices files found — skipping prices merge')
        combined_prices = None

    # ── Activate ───────────────────────────────────────────────────────────────
    if activate:
        print('\nActivating combined files as pipeline defaults ...')

        if backup:
            for src, dst_name in [
                (DATA / 'snapshots.parquet', 'snapshots_backup.parquet'),
                (DATA / 'prices.parquet',    'prices_backup.parquet'),
            ]:
                if src.exists():
                    dst = DATA / dst_name
                    shutil.copy2(src, dst)
                    print(f'  Backed up {src.name} → {dst.name}')

        shutil.copy2(OUT_SNAPSHOTS, DATA / 'snapshots.parquet')
        print(f'  snapshots_combined.parquet → snapshots.parquet')

        if combined_prices is not None:
            shutil.copy2(OUT_PRICES, DATA / 'prices.parquet')
            print(f'  prices_combined.parquet → prices.parquet')

        print('\nActive dataset updated.')
        print('Run the macro + features + clean steps:')
        print('  python3 scripts/run_pipeline.py build --step 4')
    else:
        print('\nDry run complete — combined files written but NOT activated.')
        print('To make these the active dataset, run:')
        print('  python3 scripts/merge_snapshots.py --activate')
        print('  python3 scripts/run_pipeline.py build --step 4')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Merge multi-market snapshots')
    parser.add_argument('--activate', action='store_true',
                        help='Copy combined files to snapshots.parquet / prices.parquet')
    parser.add_argument('--backup', action='store_true',
                        help='Backup existing snapshots.parquet / prices.parquet before overwriting')
    args = parser.parse_args()
    run(activate=args.activate, backup=args.backup)
