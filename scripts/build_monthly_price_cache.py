"""
Build a monthly price + ADTV cache for backtest tickers.

Step 1 — dry-run all strategy filters for each fiscal_year to collect the
         universe of (ticker, fiscal_year) picks actually selected by the
         backtest.  This keeps the yfinance download to ~200-300 unique
         tickers instead of all 4,800+ in the dataset.

Step 2 — for each unique ticker, download monthly OHLCV from yfinance
         covering 2008-01-01 through today.

Step 3 — write data/monthly_prices.parquet with columns:
             ticker | date | adj_close | volume | adtv_30d

Usage:
    python3 scripts/build_monthly_price_cache.py [--market US] [--top 20]
    python3 scripts/build_monthly_price_cache.py --tickers-only   # just print tickers
    python3 scripts/build_monthly_price_cache.py --update         # extend existing cache
"""
from __future__ import annotations
import argparse
import sys
import time
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
import yfinance as yf
from pathlib import Path

BASE = Path(__file__).parent.parent
sys.path.insert(0, str(BASE))

FULL_DATA    = BASE / 'data' / 'historical_dataset_clean.parquet'
CACHE_PATH   = BASE / 'data' / 'monthly_prices.parquet'
MODELS_DIR   = BASE / 'models'

MIN_MARKET_CAP      = 50_000_000
MAX_FILING_LAG_MONTHS = 18
BACKTEST_START_YEAR = 2008
BACKTEST_END_YEAR   = 2023


# ── Data loading (mirrors backtester.load_full_hist) ──────────────────────────

def _load_data() -> pd.DataFrame:
    from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
    df = pd.read_parquet(FULL_DATA)
    df = df[df['period_type'] == 'annual'].copy()
    df = add_piotroski_ext(df)
    df = add_normalised_ratios(df)
    return df.reset_index(drop=True)


def _load_ml_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Attach ml_1y / ml_3y static scores the same way the backtester does."""
    import json, joblib
    meta_path = MODELS_DIR / 'model_meta.json'
    if not meta_path.exists():
        return df
    meta = json.loads(meta_path.read_text())
    loaded: dict[str, tuple] = {}
    all_feats_set: set[str] = set()
    for h in ['1y', '3y', '5y']:
        p = MODELS_DIR / f'model_{h}.joblib'
        if not p.exists():
            continue
        clf = joblib.load(p)
        feats = [f for f in meta[h]['features'] if f in df.columns]
        loaded[h] = (clf, feats)
        all_feats_set.update(feats)

    all_feats_list = sorted(all_feats_set)
    all_years = sorted(df['fiscal_year'].unique())
    exp_med: dict[int, pd.Series] = {}
    for yr in all_years:
        exp_med[yr] = df.loc[df['fiscal_year'] <= yr, all_feats_list].median()

    for h, (clf, feats) in loaded.items():
        scores = np.full(len(df), np.nan)
        for yr in all_years:
            mask = (df['fiscal_year'] == yr).values
            if mask.sum() == 0:
                continue
            X = df.loc[mask, feats].fillna(exp_med[yr][feats])
            scores[mask] = clf.predict_proba(X)[:, 1]
        df[f'ml_{h}'] = scores

    return df


# ── Strategy filter import (reuse from backtester) ────────────────────────────

def _get_filters():
    from scripts.backtester import (
        filter_composite, filter_qem, filter_scdv,
        _apply_filing_lag_filter,
    )
    return {
        'composite': filter_composite,
        'qem':       filter_qem,
        'scdv':      filter_scdv,
    }, _apply_filing_lag_filter


# ── Step 1: collect tickers via dry-run ───────────────────────────────────────

def collect_tickers(df: pd.DataFrame, market: str, top_n: int) -> set[str]:
    filters, apply_lag = _get_filters()
    years = sorted(
        y for y in df['fiscal_year'].unique()
        if BACKTEST_START_YEAR <= y <= BACKTEST_END_YEAR
    )
    tickers: set[str] = set()
    for yr in years:
        yr_df = df[df['fiscal_year'] == yr].copy()
        yr_df = apply_lag(yr_df, yr, MAX_FILING_LAG_MONTHS)
        if MIN_MARKET_CAP > 0 and 'market_cap_at_filing' in yr_df.columns:
            yr_df = yr_df[yr_df['market_cap_at_filing'].fillna(0) >= MIN_MARKET_CAP]
        for label, fn in filters.items():
            idx = fn(yr_df, top_n, market)
            picked = yr_df.loc[idx, 'ticker'].dropna().tolist()
            tickers.update(picked)
    return tickers


# ── Step 2: download monthly OHLCV via yfinance ───────────────────────────────

DOWNLOAD_START = '2007-01-01'   # one extra year for rolling ADTV on first backtest year
BATCH_SIZE     = 50             # yfinance handles 50 tickers per call fine
SLEEP_BETWEEN  = 1.5            # seconds between batches (rate-limit courtesy)


def _download_batch(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    raw = yf.download(
        tickers,
        start=start,
        end=end,
        interval='1mo',
        auto_adjust=False,
        progress=False,
        threads=True,
    )
    if raw.empty:
        return pd.DataFrame()

    # yfinance returns MultiIndex columns when >1 ticker, flat when 1
    if isinstance(raw.columns, pd.MultiIndex):
        close = raw['Close'].copy()
        volume = raw['Volume'].copy()
    else:
        if len(tickers) == 1:
            close  = raw[['Close']].rename(columns={'Close': tickers[0]})
            volume = raw[['Volume']].rename(columns={'Volume': tickers[0]})
        else:
            return pd.DataFrame()

    frames = []
    for tkr in close.columns:
        c = close[tkr].dropna()
        v = volume[tkr].reindex(c.index).fillna(0)
        if c.empty:
            continue
        tmp = pd.DataFrame({'adj_close': c, 'volume': v})
        tmp.index = pd.to_datetime(tmp.index).tz_localize(None)
        tmp['ticker'] = tkr
        frames.append(tmp)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames).reset_index().rename(columns={'index': 'date', 'Date': 'date'})
    out['date'] = pd.to_datetime(out['date']).dt.normalize()
    # ADTV: rolling 3-month (approx 3 bars) average daily dollar volume per ticker
    # Monthly volume is already ~21 trading days; divide by 21 for daily estimate
    out = out.sort_values(['ticker', 'date'])
    out['daily_vol'] = (out['adj_close'] * out['volume'] / 21).fillna(0)
    out['adtv_30d'] = (
        out.groupby('ticker')['daily_vol']
        .transform(lambda x: x.rolling(3, min_periods=1).mean())
    )
    return out[['ticker', 'date', 'adj_close', 'volume', 'adtv_30d']]


def download_prices(tickers: list[str], verbose: bool = True) -> pd.DataFrame:
    today = pd.Timestamp.today().strftime('%Y-%m-%d')
    batches = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    frames = []
    for i, batch in enumerate(batches):
        if verbose:
            print(f'  batch {i+1}/{len(batches)}  ({len(batch)} tickers)…', flush=True)
        try:
            chunk = _download_batch(batch, DOWNLOAD_START, today)
            if not chunk.empty:
                frames.append(chunk)
        except Exception as exc:
            print(f'    WARNING: batch failed — {exc}')
        if i < len(batches) - 1:
            time.sleep(SLEEP_BETWEEN)

    if not frames:
        return pd.DataFrame(columns=['ticker', 'date', 'adj_close', 'volume', 'adtv_30d'])
    return pd.concat(frames, ignore_index=True)


# ── Step 3: write / update cache ──────────────────────────────────────────────

def update_cache(new_data: pd.DataFrame) -> pd.DataFrame:
    """Merge new_data into existing cache, deduplicating on (ticker, date)."""
    if CACHE_PATH.exists():
        existing = pd.read_parquet(CACHE_PATH)
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=['ticker', 'date'], keep='last')
    else:
        combined = new_data
    combined = combined.sort_values(['ticker', 'date']).reset_index(drop=True)
    combined['date'] = pd.to_datetime(combined['date'])
    combined['adj_close'] = combined['adj_close'].astype('float32')
    combined['volume']    = combined['volume'].astype('float64')
    combined['adtv_30d']  = combined['adtv_30d'].astype('float32')
    combined.to_parquet(CACHE_PATH, index=False)
    return combined


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Build monthly price + ADTV cache for backtest tickers')
    parser.add_argument('--market',       default='US',  help='Market filter for strategy dry-run (default: US)')
    parser.add_argument('--top',          type=int, default=20, help='top_n passed to strategy filters (default: 20)')
    parser.add_argument('--tickers-only', action='store_true', help='Print collected tickers and exit (no download)')
    parser.add_argument('--update',       action='store_true', help='Extend existing cache with new months only')
    parser.add_argument('--extra-tickers', nargs='*', default=[], help='Additional tickers to include')
    args = parser.parse_args()

    print('Loading dataset…')
    df = _load_data()
    print(f'  {len(df):,} rows loaded.')

    print('Attaching ML scores…')
    df = _load_ml_scores(df)

    print(f'Collecting picks via dry-run (market={args.market}, top={args.top})…')
    tickers = collect_tickers(df, args.market, args.top)
    if args.extra_tickers:
        tickers.update(args.extra_tickers)

    # Also grab tickers already in cache so we keep them fresh on --update
    if args.update and CACHE_PATH.exists():
        cached_tickers = set(pd.read_parquet(CACHE_PATH, columns=['ticker'])['ticker'].unique())
        tickers.update(cached_tickers)

    tickers_list = sorted(tickers)
    print(f'  {len(tickers_list)} unique tickers to download.')

    if args.tickers_only:
        for t in tickers_list:
            print(t)
        return

    print(f'Downloading monthly prices via yfinance…')
    data = download_prices(tickers_list)
    if data.empty:
        print('ERROR: no price data downloaded.')
        sys.exit(1)

    print(f'  {len(data):,} monthly rows for {data["ticker"].nunique()} tickers.')
    print(f'Saving to {CACHE_PATH}…')
    final = update_cache(data)
    print(f'Cache written: {len(final):,} rows  {CACHE_PATH.stat().st_size / 1e6:.1f} MB')


if __name__ == '__main__':
    main()
