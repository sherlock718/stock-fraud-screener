"""
fetch_spy_returns.py — Download SPY annual calendar-year total returns via yfinance.

Saves to data/spy_returns.csv for use as benchmark in backtester.py.

Usage:
    python3 scripts/fetch_spy_returns.py
    python3 scripts/fetch_spy_returns.py --start 2005 --out data/spy_returns.csv

Output columns:
    year (int), spy_return (float)  — e.g. 2023, 0.2653

SPY total-return calculation uses adjusted close prices (dividends included).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

BASE     = Path(__file__).parent.parent
OUT_PATH = BASE / 'data' / 'spy_returns.csv'


def fetch_spy(start_year: int, end_year: int) -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError:
        sys.exit('ERROR: yfinance not installed. Run: pip install yfinance')

    print(f'Downloading SPY adjusted close {start_year}–{end_year}...')
    raw = yf.download('SPY', start=f'{start_year - 1}-12-01',
                      end=f'{end_year + 1}-01-31',
                      auto_adjust=True, progress=False)
    if raw is None or raw.empty:
        sys.exit('ERROR: yfinance returned no data for SPY.')

    close = raw['Close']
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    close.index = pd.to_datetime(close.index).tz_localize(None)
    close = close.sort_index().dropna()

    records = []
    for yr in range(start_year, end_year + 1):
        # Jan 1 of yr → Dec 31 of yr (use last trading day of each year)
        year_data = close[close.index.year == yr]
        prev_data  = close[close.index.year == yr - 1]

        if year_data.empty or prev_data.empty:
            continue

        entry = float(prev_data.iloc[-1])   # last trading day of year-1
        exit_ = float(year_data.iloc[-1])    # last trading day of yr

        if entry <= 0:
            continue

        spy_ret = exit_ / entry - 1
        records.append({'year': yr, 'spy_return': round(spy_ret, 6)})

    return pd.DataFrame(records)


def main() -> None:
    parser = argparse.ArgumentParser(description='Download SPY annual returns')
    parser.add_argument('--start', type=int, default=2008,
                        help='First calendar year (default: 2008)')
    parser.add_argument('--end', type=int, default=2025,
                        help='Last calendar year (default: 2025)')
    parser.add_argument('--out', default=str(OUT_PATH),
                        help='Output CSV path (default: data/spy_returns.csv)')
    args = parser.parse_args()

    df = fetch_spy(args.start, args.end)

    if df.empty:
        sys.exit('ERROR: No SPY returns computed. Check yfinance availability.')

    out = Path(args.out)
    out.parent.mkdir(exist_ok=True)
    df.to_csv(out, index=False)
    print(f'Saved {len(df)} annual SPY returns → {out}')
    print(f'  Range: {df["year"].min()}–{df["year"].max()}')
    print(f'  Mean annual return: {df["spy_return"].mean():+.2%}')
    print(f'  Best year: {df.loc[df["spy_return"].idxmax(), "year"]} '
          f'({df["spy_return"].max():+.2%})')
    print(f'  Worst year: {df.loc[df["spy_return"].idxmin(), "year"]} '
          f'({df["spy_return"].min():+.2%})')


if __name__ == '__main__':
    main()
