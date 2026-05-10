"""
Step 3 — Enrich with prices, forward returns, and momentum features.

Architecture:
  - PriceCache: SQLite disk cache — one yfinance fetch per ticker, persists
    across restarts. Subsequent runs skip network entirely for cached tickers.
  - RateLimiter: token bucket at 1.5 req/s to avoid yfinance bans.
  - with_retry: exponential backoff for transient failures.

Forward return horizons (11 total):
  6m, 1y, 2y, 3y, 4y, 5y, 6y, 7y, 8y, 10y, 15y

Size-matched benchmarks (avoids penalising small caps vs S&P500):
  market_cap < $300M → IWC (micro-cap)
  market_cap < $2B   → IWM (small-cap)
  market_cap < $10B  → MDY (mid-cap)
  otherwise          → SPY (large-cap)

All 11 benchmark series cached at startup. O(1) lookup per filing date.

Output: data/prices.parquet
  cik, ticker, filed_date, fiscal_year, fiscal_quarter, period_type
  entry_price, market_cap_at_filing, shares_at_filing
  forward_return_{h}, benchmark_return_{h}, beat_local_market_{h},
    excess_return_local_{h}  for h in HORIZONS
  momentum_12m_prior, momentum_6m_prior, momentum_3m_prior
  price_to_52w_high, vol_prior_12m
"""

from __future__ import annotations

import sqlite3
import time
import threading
import json
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq

BASE   = Path(__file__).parent.parent
DATA   = BASE / 'data'
OUT    = DATA / 'prices.parquet'
CACHE  = DATA / 'price_cache.db'
SNAP   = DATA / 'snapshots.parquet'

HORIZONS = {
    '6m':  183,
    '1y':  365,
    '2y':  730,
    '3y':  1095,
    '4y':  1460,
    '5y':  1825,
    '6y':  2190,
    '7y':  2555,
    '8y':  2920,
    '10y': 3650,
    '15y': 5475,
}

BENCHMARK_TICKERS = ['SPY', 'MDY', 'IWM', 'IWC']

# Korean benchmarks (appended to cache at startup when KR rows present)
KR_BENCHMARK_TICKERS = ['^KS11', '^KQ11']   # KOSPI, KOSDAQ

# EU benchmarks keyed by market code
EU_BENCHMARK_MAP = {
    'DE': '^GDAXI',   # DAX
    'GB': '^FTSE',    # FTSE 100
    'SE': '^OMX',     # OMX Stockholm
    'NO': '^OSEAX',   # Oslo All-Share
    'DK': '^OMXC25',  # OMX Copenhagen 25
    'FI': '^OMXH25',  # OMX Helsinki 25
    'FR': '^FCHI',    # CAC 40
    'NL': '^AEX',     # AEX
}

# Single-benchmark markets
SINGLE_BENCHMARK_MAP = {
    'BR': '^BVSP',      # Ibovespa
    'CA': '^GSPTSE',    # TSX Composite
    'JP': '^N225',      # Nikkei 225
}

# SQLite schema
_CREATE_CACHE = """
CREATE TABLE IF NOT EXISTS price_cache (
    ticker TEXT PRIMARY KEY,
    fetched_at TEXT,
    data_json TEXT
);
"""


# ── PriceCache ────────────────────────────────────────────────────────────────

class PriceCache:
    """Thread-safe SQLite disk cache for yfinance adjusted close prices."""

    def __init__(self, db_path=CACHE):
        self.db_path = str(db_path)
        self._local = threading.local()
        self._init_db()

    def _conn(self):
        if not hasattr(self._local, 'conn'):
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.execute(_CREATE_CACHE)
            self._local.conn.commit()
        return self._local.conn

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute(_CREATE_CACHE)
        conn.commit()
        conn.close()

    def get(self, ticker: str) -> pd.Series | None:
        """Return Series(date → adj_close) or None if not cached."""
        row = self._conn().execute(
            'SELECT data_json FROM price_cache WHERE ticker=?', (ticker,)
        ).fetchone()
        if row is None:
            return None
        d = json.loads(row[0])
        if not d:
            return pd.Series(dtype=float)
        s = pd.Series(d)
        s.index = pd.to_datetime(s.index)
        return s.sort_index()

    def set(self, ticker: str, series: pd.Series):
        """Persist a price series to disk."""
        if series is None or series.empty:
            data_json = json.dumps({})
        else:
            data_json = series.to_json(date_format='iso')
        self._conn().execute(
            'INSERT OR REPLACE INTO price_cache (ticker, fetched_at, data_json) VALUES (?,?,?)',
            (ticker, datetime.utcnow().isoformat(), data_json)
        )
        self._conn().commit()

    def has(self, ticker: str) -> bool:
        row = self._conn().execute(
            'SELECT 1 FROM price_cache WHERE ticker=?', (ticker,)
        ).fetchone()
        return row is not None

    def count(self) -> int:
        return self._conn().execute('SELECT COUNT(*) FROM price_cache').fetchone()[0]


# ── RateLimiter ───────────────────────────────────────────────────────────────

class RateLimiter:
    """Token bucket: max `rate` requests per second."""

    def __init__(self, rate=1.5):
        self.rate      = rate
        self.tokens    = rate
        self.last_tick = time.monotonic()
        self._lock     = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_tick
            self.last_tick = now
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate
                time.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= 1


# ── Retry wrapper ─────────────────────────────────────────────────────────────

def with_retry(fn, max_attempts=4, base_delay=2.0, label=''):
    """Exponential backoff retry. Returns None on all failures."""
    for attempt in range(max_attempts):
        try:
            return fn()
        except Exception as e:
            if attempt == max_attempts - 1:
                print(f'    [retry] FAILED {label}: {e}')
                return None
            delay = base_delay * (2 ** attempt)
            print(f'    [retry] attempt {attempt+1}/{max_attempts} {label}: {e} — waiting {delay:.0f}s')
            time.sleep(delay)
    return None


# ── yfinance fetch ─────────────────────────────────────────────────────────────

_limiter = RateLimiter(rate=1.5)


def fetch_price_series(ticker: str) -> pd.Series | None:
    """Fetch full adjusted close history from yfinance. Returns Series(date→price)."""
    _limiter.wait()

    def _fetch():
        tk = yf.Ticker(ticker)
        hist = tk.history(period='max', auto_adjust=True, actions=False)
        if hist.empty:
            return None
        close = hist['Close'].copy()
        close.index = pd.to_datetime(close.index).tz_localize(None)
        close = close[close > 0].dropna()
        return close

    return with_retry(_fetch, label=ticker)


def get_price_series(ticker: str, cache: PriceCache) -> pd.Series | None:
    """Return price series from cache or fetch from yfinance."""
    if cache.has(ticker):
        return cache.get(ticker)
    series = fetch_price_series(ticker)
    cache.set(ticker, series if series is not None else pd.Series(dtype=float))
    return series


# ── Benchmark loading ──────────────────────────────────────────────────────────

def load_benchmarks(cache: PriceCache) -> dict[str, pd.Series]:
    """Load SPY, MDY, IWM, IWC price series. Cached to SQLite."""
    benches = {}
    for sym in BENCHMARK_TICKERS:
        s = get_price_series(sym, cache)
        if s is not None and not s.empty:
            benches[sym] = s
            print(f'  Benchmark {sym}: {len(s):,} days '
                  f'({s.index[0].date()} → {s.index[-1].date()})')
        else:
            print(f'  WARNING: could not load benchmark {sym}')
    return benches


def pick_benchmark(market_cap: float | None, market: str = 'US') -> str:
    """Return benchmark ticker based on market and market cap."""
    if market == 'KR':
        # Use KOSPI for large/mid, KOSDAQ for small (approximation)
        if market_cap is not None and not pd.isna(market_cap) and market_cap < 300e9:
            return '^KQ11'   # KOSDAQ — smaller companies
        return '^KS11'       # KOSPI
    # Single-benchmark markets (BR, CA, JP)
    if market in SINGLE_BENCHMARK_MAP:
        return SINGLE_BENCHMARK_MAP[market]
    # EU country benchmarks
    if market in EU_BENCHMARK_MAP:
        return EU_BENCHMARK_MAP[market]
    # US size-matched benchmarks
    if market_cap is None or pd.isna(market_cap):
        return 'SPY'
    if market_cap < 300e6:
        return 'IWC'
    if market_cap < 2e9:
        return 'IWM'
    if market_cap < 10e9:
        return 'MDY'
    return 'SPY'


# ── Price lookup helpers ───────────────────────────────────────────────────────

def price_on_or_after(series: pd.Series, target_date: pd.Timestamp,
                       max_lag: int = 5) -> float | None:
    """Return first available price on or after target_date within max_lag days."""
    if series is None or series.empty:
        return None
    subset = series[series.index >= target_date]
    deadline = target_date + timedelta(days=max_lag)
    subset = subset[subset.index <= deadline]
    return float(subset.iloc[0]) if len(subset) > 0 else None


def forward_return(series: pd.Series, entry_date: pd.Timestamp,
                   horizon_days: int) -> float | None:
    """Return (exit_price/entry_price - 1) for a given horizon."""
    entry_price = price_on_or_after(series, entry_date)
    if entry_price is None:
        return None
    target = entry_date + timedelta(days=horizon_days)
    exit_price = price_on_or_after(series, target, max_lag=10)
    if exit_price is None:
        return None
    return float(exit_price / entry_price - 1)


def prior_return(series: pd.Series, entry_date: pd.Timestamp,
                 days_back: int, skip_days: int = 21) -> float | None:
    """Momentum: return from (entry - days_back) to (entry - skip_days)."""
    if series is None or series.empty:
        return None
    start_date = entry_date - timedelta(days=days_back)
    end_date   = entry_date - timedelta(days=skip_days)
    start_p = price_on_or_after(series, start_date, max_lag=5)
    end_p   = price_on_or_after(series, end_date, max_lag=5)
    if start_p is None or end_p is None or start_p == 0:
        return None
    return float(end_p / start_p - 1)


def vol_prior(series: pd.Series, entry_date: pd.Timestamp,
              days_back: int = 252) -> float | None:
    """Annualised daily return volatility over prior window."""
    if series is None or series.empty:
        return None
    start = entry_date - timedelta(days=days_back)
    window = series[(series.index >= start) & (series.index < entry_date)]
    if len(window) < 20:
        return None
    returns = window.pct_change().dropna()
    return float(returns.std() * np.sqrt(252))


def price_to_52w_high(series: pd.Series, entry_date: pd.Timestamp) -> float | None:
    """Price at entry / 52-week high."""
    if series is None or series.empty:
        return None
    start = entry_date - timedelta(days=252)
    window = series[(series.index >= start) & (series.index <= entry_date)]
    if window.empty:
        return None
    high = window.max()
    entry_price = price_on_or_after(series, entry_date)
    if high == 0 or entry_price is None:
        return None
    return float(entry_price / high)


# ── Per-row enrichment ────────────────────────────────────────────────────────

def enrich_row(row: pd.Series, price_series: pd.Series,
               benchmarks: dict[str, pd.Series]) -> dict:
    """Compute all price-derived features for one (ticker, filing_date) row."""
    result = {
        'cik':           row['cik'],
        'ticker':        row['ticker'],
        'filed_date':    row['filed_date'],
        'fiscal_year':   row['fiscal_year'],
        'fiscal_quarter': row.get('fiscal_quarter'),
        'period_type':   row.get('period_type', 'annual'),
    }

    entry_date = pd.Timestamp(row['filed_date'])

    # Entry price + market cap
    entry_price = price_on_or_after(price_series, entry_date) if price_series is not None else None
    result['entry_price'] = entry_price

    shares = row.get('shares_outstanding') or row.get('common_shares_outstanding')
    market_cap = (entry_price * shares) if (entry_price and shares) else None
    result['market_cap_at_filing'] = market_cap
    result['shares_at_filing']     = shares

    # Pick size-matched benchmark
    market    = row.get('market', 'US') or 'US'
    bench_sym = pick_benchmark(market_cap, market)
    result['benchmark_used'] = bench_sym

    # Forward returns
    for h, days in HORIZONS.items():
        col_fwd   = f'forward_return_{h}'
        col_bench = f'benchmark_return_{h}'
        col_beat  = f'beat_local_market_{h}'
        col_exc   = f'excess_return_local_{h}'

        fwd   = forward_return(price_series, entry_date, days) if price_series is not None else None
        bench = forward_return(benchmarks.get(bench_sym), entry_date, days)

        result[col_fwd]   = fwd
        result[col_bench] = bench

        if fwd is not None and bench is not None:
            result[col_beat] = int(fwd > bench)
            result[col_exc]  = fwd - bench
        else:
            result[col_beat] = None
            result[col_exc]  = None

    # Momentum (skip 1 month to avoid short-term reversal noise)
    result['momentum_12m_prior'] = prior_return(price_series, entry_date, 365, skip_days=21)
    result['momentum_6m_prior']  = prior_return(price_series, entry_date, 183, skip_days=21)
    result['momentum_3m_prior']  = prior_return(price_series, entry_date, 91,  skip_days=21)

    # Volatility and 52-week high ratio
    result['vol_prior_12m']    = vol_prior(price_series, entry_date, 252)
    result['price_to_52w_high'] = price_to_52w_high(price_series, entry_date)

    return result


# ── Checkpoint helpers ─────────────────────────────────────────────────────────

CHECKPOINT = DATA / 'prices_checkpoint.json'


def load_checkpoint(path: Path = CHECKPOINT) -> set:
    if path.exists():
        with open(path) as f:
            return set(json.load(f))
    return set()


def save_checkpoint(done: set, path: Path = CHECKPOINT):
    with open(path, 'w') as f:
        json.dump(list(done), f)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(limit=None, snapshots_path=None, out_path=None):
    DATA.mkdir(exist_ok=True)
    print('Step 3 — Enriching with prices + forward returns')

    snap_file = Path(snapshots_path) if snapshots_path else SNAP
    out_file  = Path(out_path)       if out_path       else OUT
    ckpt_file = out_file.with_name(out_file.stem + '_checkpoint.json')

    # Load snapshots
    if not snap_file.exists():
        print(f'ERROR: {snap_file} not found — run step 2 first')
        import sys; sys.exit(1)

    snap = pd.read_parquet(snap_file)
    print(f'  Snapshots loaded: {len(snap):,} rows, {snap["ticker"].nunique():,} unique tickers')

    if limit:
        tickers_sample = snap['ticker'].drop_duplicates().head(limit)
        snap = snap[snap['ticker'].isin(tickers_sample)].copy()
        print(f'  TEST MODE: limited to {snap["ticker"].nunique():,} tickers '
              f'({len(snap):,} rows)')

    # Sort by ticker so checkpoint resumes cleanly
    snap = snap.sort_values(['ticker', 'filed_date']).reset_index(drop=True)

    cache = PriceCache(CACHE)
    print(f'  Price cache: {cache.count():,} tickers already cached')

    # Load benchmark series at startup (cached)
    print('  Loading benchmark series (SPY, MDY, IWM, IWC) ...')
    benchmarks = load_benchmarks(cache)

    # Load Korean benchmarks if KR rows present
    if 'market' in snap.columns and (snap['market'] == 'KR').any():
        print('  Loading Korean benchmark series (^KS11, ^KQ11) ...')
        for sym in KR_BENCHMARK_TICKERS:
            s = get_price_series(sym, cache)
            if s is not None and not s.empty:
                benchmarks[sym] = s
                print(f'  Benchmark {sym}: {len(s):,} days')

    # Load EU benchmarks for any EU market present
    if 'market' in snap.columns:
        eu_markets_present = set(snap['market'].unique()) & set(EU_BENCHMARK_MAP.keys())
        if eu_markets_present:
            eu_syms = sorted({EU_BENCHMARK_MAP[m] for m in eu_markets_present})
            print(f'  Loading EU benchmark series ({", ".join(eu_syms)}) ...')
            for sym in eu_syms:
                s = get_price_series(sym, cache)
                if s is not None and not s.empty:
                    benchmarks[sym] = s
                    print(f'  Benchmark {sym}: {len(s):,} days')
                else:
                    print(f'  WARNING: could not load EU benchmark {sym}')

    # Load single-benchmark markets (BR, CA, JP)
    if 'market' in snap.columns:
        single_markets_present = set(snap['market'].unique()) & set(SINGLE_BENCHMARK_MAP.keys())
        if single_markets_present:
            single_syms = sorted({SINGLE_BENCHMARK_MAP[m] for m in single_markets_present})
            print(f'  Loading single-market benchmark series ({", ".join(single_syms)}) ...')
            for sym in single_syms:
                s = get_price_series(sym, cache)
                if s is not None and not s.empty:
                    benchmarks[sym] = s
                    print(f'  Benchmark {sym}: {len(s):,} days')
                else:
                    print(f'  WARNING: could not load benchmark {sym}')

    done_tickers = load_checkpoint(ckpt_file)
    tickers_all  = snap['ticker'].unique().tolist()
    tickers_todo = [t for t in tickers_all if t not in done_tickers]
    print(f'  Tickers to process: {len(tickers_todo):,} '
          f'(already done: {len(done_tickers):,})')

    # If resuming and out_file exists, start collecting from existing file
    all_rows: list[dict] = []
    if out_file.exists() and done_tickers:
        print(f'  Loading existing {out_file.name} for incremental update ...')
        existing = pd.read_parquet(out_file)
        all_rows = existing.to_dict('records')
        print(f'    {len(all_rows):,} rows loaded from existing file')

    t_start    = time.time()
    n_done     = 0
    n_total    = len(tickers_todo)
    BATCH_SIZE = 50

    for i, ticker in enumerate(tickers_todo):
        rows_for_ticker = snap[snap['ticker'] == ticker]

        # Fetch price series (cached)
        price_series = get_price_series(ticker, cache)

        # Enrich each filing row
        for _, row in rows_for_ticker.iterrows():
            try:
                enriched = enrich_row(row, price_series, benchmarks)
                all_rows.append(enriched)
            except Exception as e:
                print(f'  ERROR enriching {ticker} row {row.get("filed_date")}: {e}')

        done_tickers.add(ticker)
        n_done += 1

        # Progress report every 200 tickers
        if n_done % 200 == 0 or n_done == n_total:
            elapsed  = time.time() - t_start
            rate     = n_done / elapsed if elapsed > 0 else 0
            remaining = (n_total - n_done) / rate / 60 if rate > 0 else 0
            pct      = 100 * n_done / n_total
            print(f'  [{pct:.0f}%] {n_done:,}/{n_total:,} tickers | '
                  f'{rate:.1f}/s | ~{remaining:.0f} min remaining | '
                  f'cache: {cache.count():,}')

        # Checkpoint + incremental save every BATCH_SIZE tickers
        if n_done % BATCH_SIZE == 0:
            save_checkpoint(done_tickers, ckpt_file)
            df_partial = pd.DataFrame(all_rows)
            df_partial.to_parquet(out_file, index=False)

    # Final save
    save_checkpoint(done_tickers, ckpt_file)
    df_out = pd.DataFrame(all_rows)
    if df_out.empty:
        print('  WARNING: no rows produced — check snapshots.parquet')
        import sys; sys.exit(1)

    df_out.to_parquet(out_file, index=False)

    # Summary
    labeled_1y = df_out['forward_return_1y'].notna().sum() if 'forward_return_1y' in df_out.columns else 0
    labeled_5y = df_out['forward_return_5y'].notna().sum() if 'forward_return_5y' in df_out.columns else 0
    print(f'\nStep 3 complete.')
    print(f'  Total rows:          {len(df_out):,}')
    print(f'  Unique tickers:      {df_out["ticker"].nunique():,}')
    print(f'  Labeled rows (1y):   {labeled_1y:,}')
    print(f'  Labeled rows (5y):   {labeled_5y:,}')
    print(f'  Saved: {out_file}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit',     type=int,  default=None)
    parser.add_argument('--snapshots', type=str,  default=None,
                        help='Path to snapshots parquet (default: data/snapshots.parquet)')
    parser.add_argument('--out',       type=str,  default=None,
                        help='Output parquet path (default: data/prices.parquet)')
    args = parser.parse_args()
    run(limit=args.limit, snapshots_path=args.snapshots, out_path=args.out)
