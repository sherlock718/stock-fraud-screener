"""
run_pipeline.py — Master runner for the v4 alpha research dataset pipeline.

Usage:
  python scripts/run_pipeline.py build              # Full pipeline (all steps)
  python scripts/run_pipeline.py build --limit 200  # Test run (200 companies)
  python scripts/run_pipeline.py build --step 2     # Resume from step 2
  python scripts/run_pipeline.py features           # Recompute features only (step 5, no API)
  python scripts/run_pipeline.py enrich-prices      # Re-run price enrichment (step 3)
  python scripts/run_pipeline.py enrich-macro       # Re-run macro enrichment (step 4)
  python scripts/run_pipeline.py clean              # Re-run clean (step 6)
  python scripts/run_pipeline.py status             # Show dataset stats

Pipeline steps:
  1. fetch_tickers    — All 10-K filers from SEC (including OTC + delisted)
  2. build_snapshots  — EDGAR XBRL annual + quarterly snapshots
  3. enrich_prices    — yfinance prices, 11 forward return horizons, momentum
  4. enrich_macro     — FRED macro context at each filing date
  5. compute_features — 170+ features, sector percentiles, interactions (no API)
  6. clean            — Data quality filter, output final parquet

Each step is idempotent and checkpointed. Safe to interrupt and resume.
"""

import argparse
import subprocess
import sys
import os
import time
import json
from datetime import datetime
from pathlib import Path

BASE   = Path(__file__).parent.parent
DATA   = BASE / 'data'
PIPE   = BASE / 'pipeline'
LOG    = Path('/tmp/v4_build.log')
STATUS = DATA / 'pipeline_status.json'

STEPS = {
    1: ('fetch_tickers',    'Step 1/6 — Fetching ticker list'),
    2: ('build_snapshots',  'Step 2/6 — Building EDGAR snapshots (annual + quarterly)'),
    3: ('enrich_prices',    'Step 3/6 — Enriching with prices + forward returns'),
    4: ('enrich_macro',     'Step 4/6 — Adding FRED macro context'),
    5: ('compute_features', 'Step 5/6 — Computing 170+ features (no API)'),
    6: ('clean',            'Step 6/6 — Cleaning and validating dataset'),
}


def log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line)
    with open(LOG, 'a') as f:
        f.write(line + '\n')


def run_step(step_num, extra_args=None):
    module = STEPS[step_num][0]
    label  = STEPS[step_num][1]
    script = PIPE / f'step{step_num}_{module}.py'

    log(f'\n{"="*60}')
    log(label)
    log(f'{"="*60}')

    cmd = [sys.executable, str(script)] + (extra_args or [])
    result = subprocess.run(cmd, cwd=str(BASE))

    if result.returncode != 0:
        log(f'ERROR: Step {step_num} failed with exit code {result.returncode}')
        log('Check the log above for details. Fix the issue then run:')
        log(f'  python scripts/run_pipeline.py build --step {step_num}')
        sys.exit(1)

    update_status(step_num, 'completed')
    log(f'Step {step_num} complete.')


def update_status(step, state):
    status = load_status()
    status[f'step{step}'] = {'state': state, 'ts': datetime.now().isoformat()}
    DATA.mkdir(exist_ok=True)
    with open(STATUS, 'w') as f:
        json.dump(status, f, indent=2)


def load_status():
    if STATUS.exists():
        with open(STATUS) as f:
            return json.load(f)
    return {}


def cmd_status():
    import pandas as pd

    print('\n── Pipeline Status ──────────────────────────────────')
    status = load_status()
    for n, (module, label) in STEPS.items():
        state = status.get(f'step{n}', {}).get('state', 'pending')
        icon = '✓' if state == 'completed' else '→' if state == 'running' else '○'
        print(f'  {icon} {label}')

    print('\n── Dataset Files ─────────────────────────────────────')
    files = [
        ('data/tickers.parquet',                    'Company list'),
        ('data/snapshots.parquet',                  'Raw EDGAR snapshots'),
        ('data/prices.parquet',                     'Price enrichment'),
        ('data/macro.parquet',                      'Macro enrichment'),
        ('data/historical_dataset.parquet',         'Full feature dataset'),
        ('data/historical_dataset_clean.parquet',   'Clean final dataset'),
        ('data/price_cache.db',                     'yfinance disk cache'),
    ]
    for rel, label in files:
        path = BASE / rel
        if path.exists():
            mb = path.stat().st_size / 1e6
            try:
                df = pd.read_parquet(path) if path.suffix == '.parquet' else None
                rows = f'{len(df):,} rows × {len(df.columns)} cols' if df is not None else ''
            except Exception:
                rows = '(error reading)'
            print(f'  ✓ {label}: {mb:.1f} MB  {rows}')
        else:
            print(f'  ○ {label}: not yet built')

    clean = BASE / 'data/historical_dataset_clean.parquet'
    if clean.exists():
        try:
            df = pd.read_parquet(clean)
            print(f'\n── Final Dataset Summary ─────────────────────────────')
            print(f'  Rows:       {len(df):,}')
            print(f'  Companies:  {df["ticker"].nunique():,}')
            print(f'  Features:   {len(df.columns)}')
            if 'period_type' in df.columns:
                print(f'  Annual rows:    {(df["period_type"]=="annual").sum():,}')
                print(f'  Quarterly rows: {(df["period_type"]=="quarterly").sum():,}')
            if 'market' in df.columns:
                print(f'  Markets:    {df["market"].value_counts().to_dict()}')
            for label in ['forward_return_1y', 'forward_return_3y', 'forward_return_5y']:
                if label in df.columns:
                    n = df[label].notna().sum()
                    print(f'  {label}: {n:,} labeled rows')
        except Exception as e:
            print(f'  Error reading dataset: {e}')


def cmd_build(from_step=1, limit=None):
    log(f'Pipeline v4 starting — {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    if limit:
        log(f'TEST MODE: limit={limit} companies')
    log(f'Starting from step {from_step}')

    extra = ['--limit', str(limit)] if limit else []

    for step_num in range(from_step, 7):
        # Only pass --limit to steps that accept it
        step_extra = extra if step_num in (1, 2, 3) else []
        run_step(step_num, step_extra)

    log('\n' + '='*60)
    log('BUILD COMPLETE')
    log('='*60)
    cmd_status()


def main():
    parser = argparse.ArgumentParser(description='v4 Alpha Research Pipeline')
    parser.add_argument('command', choices=['build', 'features', 'enrich-prices',
                                             'enrich-macro', 'clean', 'status'],
                        nargs='?', default='status')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit to N companies (test mode)')
    parser.add_argument('--step', type=int, default=1,
                        help='Resume from step N (default: 1)')
    args = parser.parse_args()

    DATA.mkdir(exist_ok=True)

    if args.command == 'status':
        cmd_status()
    elif args.command == 'build':
        cmd_build(from_step=args.step, limit=args.limit)
    elif args.command == 'features':
        run_step(5)
        run_step(6)
    elif args.command == 'enrich-prices':
        run_step(3, ['--limit', str(args.limit)] if args.limit else [])
        run_step(5)
        run_step(6)
    elif args.command == 'enrich-macro':
        run_step(4)
        run_step(5)
        run_step(6)
    elif args.command == 'clean':
        run_step(6)


if __name__ == '__main__':
    main()
