"""
Patch existing historical_dataset_clean.parquet with two fix categories:

1. Equity-derived features — roe, roic, pb_ratio, book_to_market,
   roe_volatility_5yr, earnings_stability_5yr, roa_volatility_5yr,
   pb_ratio_sector_pct, roe_sector_pct, net_debt_to_equity
   Root cause: step5 COLUMN_ALIASES skipped equity→total_equity coalesce
   because total_equity already existed (at 0.2% fill from a sparse XBRL tag).
   snapshots_combined has 'equity' at 92.4% fill — the correct source.

2. Multi-horizon volatility — vol_prior_6m, vol_prior_36m, vol_prior_60m
   Annualised daily return volatility over 6 / 36 / 60 month lookback windows.
   Computed from price_cache.db (SQLite, daily prices per ticker as JSON).

Usage:
    python3 scripts/patch_equity_vol_features.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from scripts._root import ROOT

BASE = ROOT

DATA = BASE / 'data'
MAIN_PATH = DATA / 'historical_dataset_clean.parquet'
SNAP_PATH = DATA / 'snapshots_combined.parquet'
PRICE_DB  = DATA / 'price_cache.db'

# ---------------------------------------------------------------------------
# Equity-derived features
# ---------------------------------------------------------------------------

def sdiv(a: pd.Series, b: pd.Series) -> pd.Series:
    """Safe division — returns NaN on zero/inf."""
    result = a / b.replace(0, np.nan)
    return result.replace([np.inf, -np.inf], np.nan)


def pct_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True, method='average')


def patch_equity_features(df: pd.DataFrame) -> pd.DataFrame:
    print('  Loading snapshots_combined for equity column ...')
    snap = pd.read_parquet(SNAP_PATH, columns=['cik', 'ticker', 'fiscal_year', 'equity', 'sga_expense'])

    # Build join key: prefer (cik, fiscal_year), fall back to (ticker, fiscal_year)
    join_cols = ['cik', 'fiscal_year']
    snap_dedup = (
        snap.groupby(join_cols, as_index=False)
            .agg({'equity': 'first', 'sga_expense': 'first'})
    )

    before_len = len(df)
    df = df.merge(
        snap_dedup.rename(columns={'equity': '_equity_src', 'sga_expense': '_sga_src'}),
        on=join_cols,
        how='left',
    )
    assert len(df) == before_len, 'merge changed row count'

    # Coalesce: prefer high-fill source columns over sparse existing ones
    df['total_equity'] = df['_equity_src'].combine_first(df.get('total_equity', pd.Series(np.nan, index=df.index)))
    if 'sga' in df.columns:
        df['sga'] = df['_sga_src'].combine_first(df['sga'])
    df.drop(columns=['_equity_src', '_sga_src'], inplace=True)

    ni  = df.get('net_income',          pd.Series(np.nan, index=df.index))
    ta  = df.get('total_assets',        pd.Series(np.nan, index=df.index))
    eq  = df['total_equity']
    mc  = df.get('market_cap_at_filing', pd.Series(np.nan, index=df.index))
    oi  = df.get('operating_income',    pd.Series(np.nan, index=df.index))
    lt  = df.get('long_term_debt',      pd.Series(0.0, index=df.index)).fillna(0)
    cash = df.get('cash',               pd.Series(0.0, index=df.index)).fillna(0)

    invested_capital = eq + lt - cash

    df['roe']              = sdiv(ni, eq)
    df['roic']             = sdiv(oi, invested_capital)
    df['pb_ratio']         = sdiv(mc, eq)
    df['book_to_market']   = sdiv(eq, mc)
    df['net_debt_to_equity'] = sdiv(lt - cash, eq)

    # Sector percentile ranks for roe and pb_ratio
    group_keys = ['fiscal_year', 'sic_code'] if 'sic_code' in df.columns else ['fiscal_year']
    df['roe_sector_pct']     = df.groupby(group_keys)['roe'].transform(pct_rank)
    df['pb_ratio_sector_pct'] = df.groupby(group_keys)['pb_ratio'].transform(pct_rank)

    # roe_volatility_5yr: rolling 5yr std of roe per ticker
    df = df.sort_values(['ticker', 'fiscal_year'])
    df['roe_volatility_5yr'] = (
        df.groupby('ticker')['roe']
          .transform(lambda x: x.rolling(5, min_periods=3).std())
    )
    df['earnings_stability_5yr'] = -df['roe_volatility_5yr']

    # roa_volatility_5yr: more robust (roa has 91.9% fill vs roe's 4.3% before fix)
    df['roa_volatility_5yr'] = (
        df.groupby('ticker')['roa']
          .transform(lambda x: x.rolling(5, min_periods=3).std())
    )
    df['earnings_stability_roa_5yr'] = -df['roa_volatility_5yr']

    # Winsorize newly computed ratios
    for col in ['roe', 'roic', 'pb_ratio', 'book_to_market', 'net_debt_to_equity']:
        if col in df.columns:
            lo = df[col].quantile(0.01)
            hi = df[col].quantile(0.99)
            df[col] = df[col].clip(lo, hi)

    print(f'    total_equity fill: {df["total_equity"].notna().mean()*100:.1f}%')
    print(f'    roe fill: {df["roe"].notna().mean()*100:.1f}%')
    print(f'    pb_ratio fill: {df["pb_ratio"].notna().mean()*100:.1f}%')
    print(f'    roe_volatility_5yr fill: {df["roe_volatility_5yr"].notna().mean()*100:.1f}%')
    print(f'    roa_volatility_5yr fill: {df["roa_volatility_5yr"].notna().mean()*100:.1f}%')

    return df


# ---------------------------------------------------------------------------
# Multi-horizon volatility from price_cache.db
# ---------------------------------------------------------------------------

# Lookback windows: (col_name, trading_days)
VOL_WINDOWS = [
    ('vol_prior_6m',  126),
    ('vol_prior_36m', 756),
    ('vol_prior_60m', 1260),
]
MIN_OBS = 20


def _parse_price_json(data_json: str) -> pd.Series | None:
    try:
        d = json.loads(data_json)
        s = pd.Series(d, dtype=float)
        s.index = pd.to_datetime(s.index)
        s = s.sort_index()
        return s
    except Exception:
        return None


def _vol_for_window(price_series: pd.Series, entry_date: pd.Timestamp, days_back: int) -> float | None:
    start = entry_date - timedelta(days=days_back)
    window = price_series[(price_series.index >= start) & (price_series.index < entry_date)]
    if len(window) < MIN_OBS:
        return None
    returns = window.pct_change().dropna()
    if len(returns) < MIN_OBS:
        return None
    return float(returns.std() * np.sqrt(252))


def patch_vol_features(df: pd.DataFrame) -> pd.DataFrame:
    print('  Computing multi-horizon volatility from price_cache.db ...')
    print(f'    Windows: {[c for c, _ in VOL_WINDOWS]}')

    # Pre-index: ticker → list of (row_idx, filed_date)
    df['_filed_date_ts'] = pd.to_datetime(df['filed_date'])
    ticker_to_rows: dict[str, list[tuple]] = {}
    for idx, row in df[['ticker', '_filed_date_ts']].iterrows():
        ticker_to_rows.setdefault(row['ticker'], []).append((idx, row['_filed_date_ts']))

    for col, _ in VOL_WINDOWS:
        df[col] = np.nan

    conn = sqlite3.connect(PRICE_DB)
    tickers_in_db = {r[0] for r in conn.execute('SELECT ticker FROM price_cache').fetchall()}

    processed = 0
    tickers_list = list(ticker_to_rows.keys())
    n = len(tickers_list)

    for i, ticker in enumerate(tickers_list):
        if ticker not in tickers_in_db:
            continue

        row_data = conn.execute(
            'SELECT data_json FROM price_cache WHERE ticker = ?', (ticker,)
        ).fetchone()
        if row_data is None:
            continue

        price_series = _parse_price_json(row_data[0])
        if price_series is None or price_series.empty:
            continue

        for row_idx, entry_date in ticker_to_rows[ticker]:
            for col, days_back in VOL_WINDOWS:
                v = _vol_for_window(price_series, entry_date, days_back)
                if v is not None:
                    df.at[row_idx, col] = v

        processed += 1
        if (i + 1) % 500 == 0:
            print(f'    {i+1}/{n} tickers processed ({processed} with price data) ...')

    conn.close()
    df.drop(columns=['_filed_date_ts'], inplace=True)

    for col, _ in VOL_WINDOWS:
        print(f'    {col} fill: {df[col].notna().mean()*100:.1f}%')

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(dry_run: bool = False) -> None:
    print('Loading historical_dataset_clean.parquet ...')
    df = pd.read_parquet(MAIN_PATH)
    print(f'  Shape before: {df.shape}')

    print()
    print('=== Patching equity-derived features ===')
    df = patch_equity_features(df)

    print()
    print('=== Patching multi-horizon volatility features ===')
    df = patch_vol_features(df)

    print()
    new_cols = ['roa_volatility_5yr', 'earnings_stability_roa_5yr', 'vol_prior_6m', 'vol_prior_36m', 'vol_prior_60m']
    print(f'  Shape after: {df.shape}')
    print(f'  New columns added: {[c for c in new_cols if c in df.columns]}')

    if dry_run:
        print('  [DRY RUN] — not saving')
        return

    backup = MAIN_PATH.with_suffix('.parquet.bak_pre_patch')
    print(f'  Backing up to {backup.name} ...')
    import shutil
    shutil.copy2(MAIN_PATH, backup)

    print(f'  Saving patched parquet ({len(df):,} rows × {len(df.columns)} cols) ...')
    df.to_parquet(MAIN_PATH, index=False)
    print('  Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true', help='Run without saving')
    args = parser.parse_args()
    main(dry_run=args.dry_run)
