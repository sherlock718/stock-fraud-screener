#!/usr/bin/env python3
"""
Brazil pipeline runner.

Steps:
  1. step1_fetch_tickers_br   → data/tickers_br.parquet
  2. step2_build_snapshots_br → data/snapshots_br.parquet
  3-6. Shared pipeline steps on BR snapshots → data/*_br.parquet

Usage:
  python3 scripts/run_pipeline_br.py build              # full build
  python3 scripts/run_pipeline_br.py build --step 2     # resume from step 2
  python3 scripts/run_pipeline_br.py build --limit 50   # test run
  python3 scripts/run_pipeline_br.py status             # check file state
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from _root import ROOT

BASE = ROOT
DATA = BASE / 'data'


def fmt(path: Path) -> str:
    if not path.exists():
        return '  ○ not yet built'
    size_mb = path.stat().st_size / 1e6
    try:
        import pandas as pd
        df = pd.read_parquet(path)
        return f'  ✓ {size_mb:.1f} MB  {len(df):,} rows × {len(df.columns)} cols'
    except Exception:
        return f'  ✓ {size_mb:.1f} MB'


def status():
    print('\n── Brazil Pipeline Status ────────────────────────────')
    files = [
        ('tickers_br.parquet',                  'BR company list (CVM + B3)'),
        ('snapshots_br.parquet',                'CVM financial snapshots'),
        ('prices_br.parquet',                   'Price enrichment (BR)'),
        ('macro_br.parquet',                    'Macro enrichment (BR)'),
        ('historical_dataset_br.parquet',       'Full feature dataset (BR)'),
        ('historical_dataset_clean_br.parquet', 'Clean final dataset (BR)'),
    ]
    for fname, label in files:
        p = DATA / fname
        mark = '✓' if p.exists() else '○'
        print(f'  {mark} {label}')
        if p.exists():
            print(fmt(p))
    print()


def run_step(script: str, extra: list[str], label: str):
    cmd = [sys.executable, str(BASE / script)] + extra
    print(f'\n{"="*60}')
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {label}')
    print(f'  {" ".join(str(c) for c in cmd)}')
    print('='*60)
    result = subprocess.run(cmd, cwd=BASE)
    if result.returncode != 0:
        print(f'\nERROR: {script} failed (exit {result.returncode})')
        sys.exit(result.returncode)


BR_STEPS = {
    1: ('pipeline/step1_fetch_tickers_br.py',   'Step 1 — Fetch BR ticker universe (CVM + B3)'),
    2: ('pipeline/step2_build_snapshots_br.py', 'Step 2 — Build CVM financial snapshots'),
    3: ('pipeline/step3_enrich_prices.py',       'Step 3 — Enrich with price data (yfinance)'),
    4: ('pipeline/step4_enrich_macro.py',        'Step 4 — Enrich with macro data'),
    5: ('pipeline/step5_compute_features.py',    'Step 5 — Compute 324 features'),
    6: ('pipeline/step6_clean.py',               'Step 6 — Clean and validate'),
}

# Steps that accept --limit for test runs
LIMIT_STEPS = {1, 2, 3}
# Steps that need to know which snapshots file to operate on
SNAPSHOT_STEPS = {3, 4, 5, 6}


def build(start_step: int, limit: int | None):
    for step_num in range(start_step, 7):
        script, label = BR_STEPS[step_num]

        extra: list[str] = []
        if step_num in LIMIT_STEPS and limit:
            extra += ['--limit', str(limit)]
        if step_num in SNAPSHOT_STEPS:
            extra += ['--snapshots', str(DATA / 'snapshots_br.parquet')]
        if step_num == 3:
            extra += ['--out', str(DATA / 'prices_br.parquet')]
        if step_num in {4, 5, 6}:
            extra += ['--suffix', '_br']

        run_step(script, extra, label)

    print('\n' + '='*60)
    print(f'[{datetime.now().strftime("%H:%M:%S")}] BRAZIL BUILD COMPLETE')
    print('='*60)
    status()


def main():
    parser = argparse.ArgumentParser(description='Brazil (CVM/B3) pipeline runner')
    sub = parser.add_subparsers(dest='cmd')

    p_build = sub.add_parser('build', help='Run pipeline steps')
    p_build.add_argument('--step',  type=int, default=1,    help='Resume from step N (1–6)')
    p_build.add_argument('--limit', type=int, default=None, help='Cap tickers for test runs')

    sub.add_parser('status', help='Show output file state')

    args = parser.parse_args()

    if args.cmd == 'status' or args.cmd is None:
        status()
    elif args.cmd == 'build':
        if args.step > 2:
            print('NOTE: Steps 3-6 use shared pipeline scripts with --snapshots / --suffix.')
            print('      Ensure snapshots_br.parquet is built (step 2) before resuming.')
            print()
        build(start_step=args.step, limit=args.limit)


if __name__ == '__main__':
    main()
