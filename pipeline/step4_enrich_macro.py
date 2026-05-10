"""
Step 4 — Add FRED macro context at each filing date.

Architecture:
  - All 9 FRED series loaded ONCE at startup into memory (9 API calls total).
  - O(1) lookup per filing row via pandas asof merge (forward-fill by date).
  - Zero FRED API calls during the per-row loop.

FRED series (all free, no registration required beyond API key):
  DGS10        → treasury_10y (%)
  DGS2         → treasury_2y (%)
  T10Y2Y       → yield_curve (10y - 2y spread, %)
  FEDFUNDS     → fed_funds_rate (%)
  BAA10Y       → credit_spread_baa (Baa corporate - 10y Treasury, %)
  BAMLH0A0HYM2 → hy_spread (ICE BofA High Yield OAS, %)
  CPIAUCSL     → cpi_yoy (YoY % change computed here)
  USREC        → recession (1=NBER recession, 0=expansion)
  VIXCLS       → vix (CBOE VIX closing level)

Derived features computed here (no API):
  yield_curve_slope  = treasury_10y - treasury_2y  (redundant with T10Y2Y but explicit)
  real_rate_10y      = treasury_10y - cpi_yoy
  credit_tightening  = credit_spread_baa change vs 6-month prior (tightening/easing signal)
  macro_regime       = 0 (low rate), 1 (rising rate), 2 (high rate), 3 (recession)

Output: data/macro.parquet
  cik, ticker, filed_date, fiscal_year, fiscal_quarter, period_type,
  treasury_10y, treasury_2y, yield_curve, fed_funds_rate,
  credit_spread_baa, hy_spread, cpi_yoy, recession, vix,
  real_rate_10y, credit_tightening, macro_regime
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

BASE  = Path(__file__).parent.parent
DATA  = BASE / 'data'
SNAP  = DATA / 'snapshots.parquet'
OUT   = DATA / 'macro.parquet'

# ── FRED config ───────────────────────────────────────────────────────────────

FRED_SERIES = {
    'DGS10':        'treasury_10y',
    'DGS2':         'treasury_2y',
    'T10Y2Y':       'yield_curve',
    'FEDFUNDS':     'fed_funds_rate',
    'BAA10Y':       'credit_spread_baa',
    'BAMLH0A0HYM2': 'hy_spread',
    'CPIAUCSL':     'cpi',          # raw index, we compute YoY below
    'USREC':        'recession',
    'VIXCLS':       'vix',
}

FRED_START = '2007-01-01'  # 2 years before 2009 to allow look-back features


def load_fred_series() -> dict[str, pd.Series]:
    """Fetch all FRED series. Returns dict of name → Series(date→value)."""
    api_key = os.getenv('FRED_API_KEY', '')
    if not api_key:
        # Try .env file
        env_path = BASE / '.env'
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith('FRED_API_KEY='):
                    api_key = line.split('=', 1)[1].strip().strip('"\'')
                    break

    if not api_key:
        print('WARNING: FRED_API_KEY not set — macro features will be NaN')
        return {}

    try:
        from fredapi import Fred
        fred = Fred(api_key=api_key)
    except ImportError:
        print('WARNING: fredapi not installed (pip install fredapi) — macro features will be NaN')
        return {}

    series_map = {}
    for fred_id, col_name in FRED_SERIES.items():
        for attempt in range(3):
            try:
                s = fred.get_series(fred_id, observation_start=FRED_START)
                s = s.dropna()
                s.index = pd.to_datetime(s.index).tz_localize(None)
                series_map[col_name] = s
                print(f'  FRED {fred_id} ({col_name}): {len(s):,} obs '
                      f'({s.index[0].date()} → {s.index[-1].date()})')
                time.sleep(0.2)
                break
            except Exception as e:
                if attempt == 2:
                    print(f'  WARNING: failed to load {fred_id}: {e}')
                else:
                    time.sleep(2 ** attempt)

    return series_map


def compute_cpi_yoy(cpi_series: pd.Series) -> pd.Series:
    """Compute rolling 12-month YoY CPI inflation from level series."""
    monthly = cpi_series.resample('MS').last().ffill()
    yoy = monthly.pct_change(periods=12) * 100
    return yoy.dropna()


def compute_credit_tightening(spread_series: pd.Series) -> pd.Series:
    """6-month change in credit spread (positive = tightening conditions)."""
    monthly = spread_series.resample('MS').last().ffill()
    return (monthly - monthly.shift(6)).dropna()


def build_macro_panel(series_map: dict) -> pd.DataFrame:
    """
    Build a daily macro panel by forward-filling all FRED series to a
    continuous daily date range (2007-01-01 → today).
    """
    date_range = pd.date_range(start=FRED_START, end=datetime.today(), freq='D')
    panel = pd.DataFrame(index=date_range)
    panel.index.name = 'date'

    for col, s in series_map.items():
        if col == 'cpi':
            continue  # handled separately
        panel[col] = s.reindex(date_range).ffill()

    # CPI YoY
    if 'cpi' in series_map:
        cpi_yoy = compute_cpi_yoy(series_map['cpi'])
        panel['cpi_yoy'] = cpi_yoy.reindex(date_range).ffill()
    else:
        panel['cpi_yoy'] = np.nan

    # Derived: real rate
    if 'treasury_10y' in panel.columns and 'cpi_yoy' in panel.columns:
        panel['real_rate_10y'] = panel['treasury_10y'] - panel['cpi_yoy']
    else:
        panel['real_rate_10y'] = np.nan

    # Derived: credit tightening (6m change in BAA spread)
    if 'credit_spread_baa' in series_map:
        ct = compute_credit_tightening(series_map['credit_spread_baa'])
        panel['credit_tightening'] = ct.reindex(date_range).ffill()
    else:
        panel['credit_tightening'] = np.nan

    # Derived: macro_regime
    # 0 = low rate + expansion, 1 = rising rate, 2 = high rate, 3 = recession
    if 'fed_funds_rate' in panel.columns and 'recession' in panel.columns:
        ffr = panel['fed_funds_rate'].fillna(0)
        rec = panel['recession'].fillna(0)
        ffr_change = ffr - ffr.shift(63)  # ~3m change
        regime = pd.Series(0, index=panel.index)
        regime[ffr >= 3.0] = 2           # high rate
        regime[(ffr < 3.0) & (ffr_change > 0.25)] = 1  # rising rate
        regime[rec > 0] = 3              # recession (override)
        panel['macro_regime'] = regime
    else:
        panel['macro_regime'] = np.nan

    return panel


def lookup_macro(panel: pd.DataFrame, date: pd.Timestamp) -> dict:
    """O(1) row lookup via searchsorted — returns macro context for a date."""
    if panel.empty:
        return {}
    # Use searchsorted for fast date lookup with forward fill
    idx = panel.index.searchsorted(date, side='right') - 1
    if idx < 0:
        idx = 0
    if idx >= len(panel):
        idx = len(panel) - 1
    row = panel.iloc[idx]
    return row.to_dict()


def run():
    DATA.mkdir(exist_ok=True)
    print('Step 4 — Adding FRED macro context')

    if not SNAP.exists():
        print(f'ERROR: {SNAP} not found — run step 2 first')
        sys.exit(1)

    snap = pd.read_parquet(SNAP)
    print(f'  Snapshots: {len(snap):,} rows')

    # Load all FRED series at startup (9 API calls, zero during loop)
    print('  Loading FRED series (9 API calls) ...')
    series_map = load_fred_series()

    if not series_map:
        print('  WARNING: No FRED data loaded — macro columns will be NaN')
        macro_panel = pd.DataFrame()
    else:
        macro_panel = build_macro_panel(series_map)
        print(f'  Macro panel built: {len(macro_panel):,} days')

    # Identify macro columns
    macro_cols = [
        'treasury_10y', 'treasury_2y', 'yield_curve', 'fed_funds_rate',
        'credit_spread_baa', 'hy_spread', 'cpi_yoy', 'recession', 'vix',
        'real_rate_10y', 'credit_tightening', 'macro_regime',
    ]

    # Key columns to preserve from snapshots
    key_cols = ['cik', 'ticker', 'filed_date', 'fiscal_year', 'fiscal_quarter', 'period_type']

    print(f'  Adding macro context to {len(snap):,} rows ...')

    if not macro_panel.empty:
        # Vectorised merge using asof (forward-fill by date)
        snap_dates = pd.DataFrame({'filed_date': pd.to_datetime(snap['filed_date'])})
        snap_dates = snap_dates.sort_values('filed_date').reset_index()

        macro_reset = macro_panel.reset_index().rename(columns={'date': 'filed_date'})
        macro_reset['filed_date'] = pd.to_datetime(macro_reset['filed_date'])
        macro_reset = macro_reset.sort_values('filed_date')

        # pd.merge_asof: for each row in snap_dates, find nearest prior macro row
        merged = pd.merge_asof(
            snap_dates,
            macro_reset,
            on='filed_date',
            direction='backward'
        )
        merged = merged.set_index('index').sort_index()

        # Attach to snap
        snap_out = snap[key_cols].copy()
        for col in macro_cols:
            if col in merged.columns:
                snap_out[col] = merged[col].values
            else:
                snap_out[col] = np.nan
    else:
        snap_out = snap[key_cols].copy()
        for col in macro_cols:
            snap_out[col] = np.nan

    snap_out.to_parquet(OUT, index=False)

    # Summary
    if 'recession' in snap_out.columns:
        rec_rows = snap_out['recession'].fillna(0).astype(int).sum()
        print(f'  Recession-period rows: {rec_rows:,}')
    if 'fed_funds_rate' in snap_out.columns:
        ffr_mean = snap_out['fed_funds_rate'].mean()
        print(f'  Avg fed funds rate across dataset: {ffr_mean:.2f}%')

    print(f'\nStep 4 complete.')
    print(f'  Total rows: {len(snap_out):,}')
    print(f'  Macro columns added: {len(macro_cols)}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    run()
