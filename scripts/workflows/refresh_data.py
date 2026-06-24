"""
Live data refresh controller.

Three modes:
  quick   — Steps 5+6 only (re-compute features, no API).  ~5 min.
  prices  — Steps 3+5+6 (re-pull yfinance prices + features).  ~30-60 min.
  full    — Steps 1-6 full rebuild from SEC EDGAR.  Several hours.

Writes data/refresh_status.json after each run so the frontend can show
last-refresh info and data staleness.

Usage:
    python3 scripts/workflows/refresh_data.py --mode quick
    python3 scripts/workflows/refresh_data.py --mode prices
    python3 scripts/workflows/refresh_data.py --mode full
    python3 scripts/workflows/refresh_data.py status
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from scripts._root import ROOT

BASE = ROOT

STATUS_PATH   = BASE / 'data' / 'refresh_status.json'
PIPELINE_PATH = BASE / 'run_pipeline.py'

MODES = {
    'quick':  {'steps': [5, 6],          'label': 'Quick (features only)',         'est_min': 5},
    'prices': {'steps': [3, 5, 6],       'label': 'Prices + features',             'est_min': 45},
    'full':   {'steps': [1, 2, 3, 4, 5, 6], 'label': 'Full rebuild (EDGAR + prices)', 'est_min': 240},
}

STEP_LABELS = {
    1: 'fetch_tickers    — SEC EDGAR ticker list',
    2: 'build_snapshots  — EDGAR XBRL financials',
    3: 'enrich_prices    — yfinance prices + forward returns',
    4: 'enrich_macro     — FRED macro context',
    5: 'compute_features — 170+ features (no API)',
    6: 'clean            — data quality filter',
}


# ── Status persistence ────────────────────────────────────────────────────────

def load_status() -> dict:
    if STATUS_PATH.exists():
        try:
            return json.loads(STATUS_PATH.read_text())
        except Exception:
            pass
    return {}


def save_status(status: dict) -> None:
    STATUS_PATH.write_text(json.dumps(status, indent=2))


# ── Dataset stats ─────────────────────────────────────────────────────────────

def _dataset_stats() -> dict:
    stats = {}
    clean = BASE / 'data' / 'historical_dataset_clean.parquet'
    app   = BASE / 'data' / 'app_data.parquet'
    if clean.exists():
        try:
            df = pd.read_parquet(clean)
            ann = df[df['period_type'] == 'annual'] if 'period_type' in df.columns else df
            stats['rows']          = int(len(df))
            stats['companies']     = int(df['ticker'].nunique()) if 'ticker' in df.columns else 0
            stats['fiscal_year_max'] = int(ann['fiscal_year'].max()) if 'fiscal_year' in ann.columns else 0
            stats['markets']       = dict(ann['market'].value_counts()) if 'market' in ann.columns else {}
            stats['file_size_mb']  = round(clean.stat().st_size / 1e6, 1)
            for col in ['forward_return_1y', 'forward_return_3y']:
                if col in df.columns:
                    stats[f'{col}_coverage_pct'] = round(ann[col].notna().mean() * 100, 1)
        except Exception as e:
            stats['error'] = str(e)
    stats['app_data_exists'] = app.exists()
    return stats


# ── Runner ────────────────────────────────────────────────────────────────────

def _run_pipeline_mode(mode: str) -> str:
    """Map mode → run_pipeline.py command and run it. Returns 'ok' or error string."""
    cmd_map = {
        'quick':  ['features'],
        'prices': ['enrich-prices'],
        'full':   ['build'],
    }
    cmd = [sys.executable, str(PIPELINE_PATH)] + cmd_map[mode]
    result = subprocess.run(cmd, cwd=str(BASE))
    if result.returncode != 0:
        return f'Pipeline exited with code {result.returncode}'
    return 'ok'


def cmd_refresh(mode: str) -> None:
    cfg = MODES[mode]
    print(f'\n{"="*60}')
    print(f'  Data Refresh — {cfg["label"]}')
    print(f'  Estimated time: ~{cfg["est_min"]} min')
    print(f'  Steps: {cfg["steps"]}')
    print(f'{"="*60}')
    for s in cfg['steps']:
        print(f'  {s}. {STEP_LABELS[s]}')
    print()

    started_at = datetime.now(timezone.utc)
    t0 = time.time()

    status = load_status()
    status['in_progress'] = True
    status['mode']        = mode
    status['started_at']  = started_at.isoformat()
    save_status(status)

    error = _run_pipeline_mode(mode)

    elapsed = round(time.time() - t0, 1)
    finished_at = datetime.now(timezone.utc)

    status = load_status()
    status['in_progress']      = False
    status['last_mode']        = mode
    status['last_mode_label']  = cfg['label']
    status['last_refresh']     = finished_at.isoformat()
    status['last_elapsed_sec'] = elapsed
    status['last_error']       = None if error == 'ok' else error
    status['dataset']          = _dataset_stats()
    save_status(status)

    if error == 'ok':
        ds = status['dataset']
        print(f'\n✓ Refresh complete in {elapsed:.0f}s')
        print(f'  Rows: {ds.get("rows", "?"):,} | Companies: {ds.get("companies", "?"):,}')
        print(f'  Fiscal year max: {ds.get("fiscal_year_max", "?")}')
    else:
        print(f'\n✗ Refresh failed: {error}')
        sys.exit(1)


def cmd_status() -> None:
    s = load_status()
    if not s:
        print('No refresh has been run yet.')
        return

    print(f'\n── Refresh Status ──────────────────────────────────')
    if s.get('in_progress'):
        print(f'  ⏳ Refresh in progress (mode: {s.get("mode")})')
        print(f'     Started: {s.get("started_at")}')
    else:
        lr = s.get('last_refresh', 'never')
        el = s.get('last_elapsed_sec', '?')
        err = s.get('last_error')
        print(f'  Last refresh:  {lr}')
        print(f'  Mode:          {s.get("last_mode_label", "?")}')
        print(f'  Duration:      {el}s')
        print(f'  Status:        {"✓ OK" if not err else f"✗ {err}"}')

    ds = s.get('dataset', {})
    if ds:
        print(f'\n── Dataset ─────────────────────────────────────────')
        print(f'  Rows:        {ds.get("rows", "?"):,}')
        print(f'  Companies:   {ds.get("companies", "?"):,}')
        print(f'  Max year:    {ds.get("fiscal_year_max", "?")}')
        print(f'  File size:   {ds.get("file_size_mb", "?")} MB')
        print(f'  1y coverage: {ds.get("forward_return_1y_coverage_pct", "?")}%')
        print(f'  3y coverage: {ds.get("forward_return_3y_coverage_pct", "?")}%')


def main() -> None:
    parser = argparse.ArgumentParser(description='Live data refresh controller')
    sub = parser.add_subparsers(dest='cmd')

    p_ref = sub.add_parser('refresh', help='Run a data refresh')
    p_ref.add_argument('--mode', choices=['quick', 'prices', 'full'],
                       required=True, help='Refresh mode')

    sub.add_parser('status', help='Show last refresh status')

    args = parser.parse_args()

    if args.cmd == 'refresh':
        cmd_refresh(args.mode)
    elif args.cmd == 'status':
        cmd_status()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
