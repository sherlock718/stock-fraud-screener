#!/usr/bin/env python3
"""
Canada pipeline runner.

Steps:
  1. step1_fetch_tickers_ca   → data/tickers_ca.parquet
  2. step2_build_snapshots_ca → data/snapshots_ca.parquet
  3-6. Shared pipeline steps on CA snapshots → data/*_ca.parquet

Data source: TMX public API (no API key required).

Usage:
  python3 scripts/run_pipeline_ca.py build              # full build
  python3 scripts/run_pipeline_ca.py build --step 2     # resume from step 2
  python3 scripts/run_pipeline_ca.py build --limit 50   # test run
  python3 scripts/run_pipeline_ca.py status             # check file state
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

DATA = BASE / 'data'


def fmt(path: Path) -> str:
    if not path.exists():
        return '  ○ not yet built'
    size_mb = path.stat().st_size / 1e6
    try:
        import pandas as pd
from scripts._root import ROOT

BASE = ROOT
        df = pd.read_parquet(path)
        return f'  ✓ {size_mb:.1f} MB  {len(df):,} rows × {len(df.columns)} cols'
    except Exception:
        return f'  ✓ {size_mb:.1f} MB'


def status():
    print('\n── Canada Pipeline Status ────────────────────────────')
    files = [
        ('tickers_ca.parquet',                  'CA company list (TMX)'),
        ('snapshots_ca.parquet',                'CA financial snapshots (TMX)'),
        ('prices_ca.parquet',                   'Price enrichment (CA)'),
        ('macro_ca.parquet',                    'Macro enrichment (CA)'),
        ('historical_dataset_ca.parquet',       'Full feature dataset (CA)'),
        ('historical_dataset_clean_ca.parquet', 'Clean final dataset (CA)'),
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


CA_STEPS = {
    1: ('pipeline/step1_fetch_tickers_ca.py',   'Step 1 — Fetch CA ticker universe (TMX)'),
    2: ('pipeline/step2_build_snapshots_ca.py', 'Step 2 — Build CA financial snapshots (TMX)'),
    3: ('pipeline/step3_enrich_prices.py',       'Step 3 — Enrich with price data (yfinance)'),
    4: ('pipeline/step4_enrich_macro.py',        'Step 4 — Enrich with macro data'),
    5: ('pipeline/step5_compute_features.py',    'Step 5 — Compute 324 features'),
    6: ('pipeline/step6_clean.py',               'Step 6 — Clean and validate'),
}

LIMIT_STEPS    = {1, 2, 3}
SNAPSHOT_STEPS = {3, 4, 5, 6}


def build(start_step: int, limit: int | None):
    for step_num in range(start_step, 7):
        script, label = CA_STEPS[step_num]

        extra: list[str] = []
        if step_num in LIMIT_STEPS and limit:
            extra += ['--limit', str(limit)]
        if step_num in SNAPSHOT_STEPS:
            extra += ['--snapshots', str(DATA / 'snapshots_ca.parquet')]
        if step_num == 3:
            extra += ['--out', str(DATA / 'prices_ca.parquet')]
        if step_num in {4, 5, 6}:
            extra += ['--suffix', '_ca']

        run_step(script, extra, label)

    print('\n' + '='*60)
    print(f'[{datetime.now().strftime("%H:%M:%S")}] CANADA BUILD COMPLETE')
    print('='*60)
    status()


def main():
    parser = argparse.ArgumentParser(description='Canada (TMX) pipeline runner')
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
            print('      Ensure snapshots_ca.parquet is built (step 2) before resuming.')
            print()
        build(start_step=args.step, limit=args.limit)


if __name__ == '__main__':
    main()
