"""
Enrich tickers with GICS sector, industry, and dividend data from yfinance.

Reads all unique tickers from data/app_data.parquet and fetches:
  sector, industry, dividendYield, dividendRate, payoutRatio,
  trailingAnnualDividendYield, exDividendDate

Output: data/sector_dividend_map.parquet

Usage:
    python3 scripts/enrich_sectors_dividends.py
    python3 scripts/enrich_sectors_dividends.py --tickers AAPL MSFT GOOG
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
import yfinance as yf
from scripts._root import ROOT

BASE = ROOT

APP_DATA  = BASE / 'data' / 'app_data.parquet'
OUT_PATH  = BASE / 'data' / 'sector_dividend_map.parquet'

FIELDS = [
    'sector', 'industry',
    'dividendYield', 'dividendRate', 'payoutRatio',
    'trailingAnnualDividendYield', 'trailingAnnualDividendRate',
    'exDividendDate',
]


def fetch_info(ticker: str) -> dict:
    try:
        info = yf.Ticker(ticker).info
        return {f: info.get(f) for f in FIELDS}
    except Exception:
        return {f: None for f in FIELDS}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--tickers', nargs='*', default=None,
                        help='Specific tickers to update (default: all from app_data)')
    parser.add_argument('--delay', type=float, default=0.15,
                        help='Seconds between requests (default: 0.15)')
    args = parser.parse_args()

    # Load existing map to allow incremental updates
    existing: dict[str, dict] = {}
    if OUT_PATH.exists():
        ex_df = pd.read_parquet(OUT_PATH)
        existing = ex_df.set_index('ticker').to_dict(orient='index')

    if args.tickers:
        tickers = [t.upper() for t in args.tickers]
    else:
        if not APP_DATA.exists():
            print(f'ERROR: {APP_DATA} not found — run the pipeline first.')
            return
        df = pd.read_parquet(APP_DATA, columns=['ticker'])
        tickers = sorted(df['ticker'].dropna().unique().tolist())

    # Only fetch tickers not already in the map (skip if re-running same day)
    missing = [t for t in tickers if t not in existing]
    print(f'Total tickers: {len(tickers)} | Already cached: {len(existing)} | To fetch: {len(missing)}')

    for i, ticker in enumerate(missing):
        if i % 100 == 0 and i > 0:
            print(f'  {i}/{len(missing)} ...')
        data = fetch_info(ticker)
        existing[ticker] = data
        time.sleep(args.delay)

    rows = [{'ticker': tk, **vals} for tk, vals in existing.items()]
    out_df = pd.DataFrame(rows)
    out_df.to_parquet(OUT_PATH, index=False)
    print(f'Saved {len(out_df)} rows → {OUT_PATH}')

    filled = out_df['sector'].notna().sum()
    print(f'  Sector filled: {filled}/{len(out_df)} ({filled/len(out_df)*100:.1f}%)')
    div_filled = out_df['dividendYield'].notna().sum()
    print(f'  Dividend yield filled: {div_filled}/{len(out_df)} ({div_filled/len(out_df)*100:.1f}%)')


if __name__ == '__main__':
    main()
