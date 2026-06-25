#!/usr/bin/env python3
"""
Korea pipeline runner.

Steps:
  1. step1_fetch_tickers_kr   → data/tickers_kr.parquet
  2. step2_build_snapshots_kr → data/snapshots_kr.parquet
  3-6. Shared pipeline steps on KR snapshots → data/*_kr.parquet

Usage:
  python3 scripts/workflows/run_pipeline_kr.py build              # full build
  python3 scripts/workflows/run_pipeline_kr.py build --step 2     # resume from step 2
  python3 scripts/workflows/run_pipeline_kr.py build --limit 50   # test run
  python3 scripts/workflows/run_pipeline_kr.py status             # check file state
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

DATA = BASE / 'data'


def fmt(path: Path) -> str:
    if not path.exists():
        return '  ○ not yet built'
    size_mb = path.stat().st_size / 1e6
    try:
        import pandas as pd
from _root import ROOT

BASE = ROOT
        df = pd.read_parquet(path)
        return f'  ✓ {size_mb:.1f} MB  {len(df):,} rows × {len(df.columns)} cols'
    except Exception:
        return f'  ✓ {size_mb:.1f} MB'


def status():
    print('\n── Korea Pipeline Status ─────────────────────────────')
    files = [
        ('tickers_kr.parquet',              'KR company list'),
        ('snapshots_kr.parquet',            'DART financial snapshots'),
        ('prices_kr.parquet',               'Price enrichment (KR)'),
        ('macro_kr.parquet',                'Macro enrichment (KR)'),
        ('historical_dataset_kr.parquet',   'Full feature dataset (KR)'),
        ('historical_dataset_clean_kr.parquet', 'Clean final dataset (KR)'),
    ]
    for fname, label in files:
        p = DATA / fname
        mark = '✓' if p.exists() else '○'
        print(f'  {mark} {label}')
        if p.exists():
            print(fmt(p))
    print()


def run_step(script: str, extra: list[str], log_prefix: str):
    cmd = [sys.executable, script] + extra
    print(f'\n{"="*60}')
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Running: {" ".join(cmd)}')
    print('='*60)
    result = subprocess.run(cmd, cwd=BASE)
    if result.returncode != 0:
        print(f'\nERROR: {script} failed (exit {result.returncode})')
        sys.exit(result.returncode)


KR_STEPS = {
    1: ('pipeline/step1_fetch_tickers_kr.py',   'KR company list'),
    2: ('pipeline/step2_build_snapshots_kr.py', 'DART financial snapshots'),
    3: ('pipeline/step3_enrich_prices.py',       'Price enrichment (KR)'),
    4: ('pipeline/step4_enrich_macro.py',        'Macro enrichment'),
    5: ('pipeline/step5_compute_features.py',    'Feature computation'),
    6: ('pipeline/step6_clean.py',               'Clean and validate'),
}

# Steps that accept --limit / --snapshots-file overrides
LIMIT_STEPS    = {1, 2, 3}
SNAPSHOT_STEPS = {3, 4, 5, 6}


def build(start_step: int, limit: int | None):
    extra_limit = ['--limit', str(limit)] if limit else []

    for step_num in range(start_step, 7):
        script, label = KR_STEPS[step_num]

        extra = []
        if step_num in LIMIT_STEPS and limit:
            extra += ['--limit', str(limit)]
        # Steps 3-6 need to know which snapshots file to use
        if step_num in SNAPSHOT_STEPS:
            extra += ['--snapshots', str(DATA / 'snapshots_kr.parquet')]
        if step_num in {5, 6}:
            extra += ['--suffix', '_kr']

        # Step 4 uses the prices output for KR
        if step_num == 4:
            extra += ['--prices', str(DATA / 'prices_kr.parquet')]

        run_step(script, extra, label)

    print('\n' + '='*60)
    print(f'[{datetime.now().strftime("%H:%M:%S")}] KOREA BUILD COMPLETE')
    print('='*60)
    status()


def main():
    parser = argparse.ArgumentParser(description='Korea (DART) pipeline runner')
    sub = parser.add_subparsers(dest='cmd')

    p_build = sub.add_parser('build')
    p_build.add_argument('--step',  type=int, default=1,  help='Resume from step N')
    p_build.add_argument('--limit', type=int, default=None, help='Limit for test runs')

    sub.add_parser('status')

    args = parser.parse_args()

    if args.cmd == 'status' or args.cmd is None:
        status()
    elif args.cmd == 'build':
        # Note: steps 3-6 need --snapshots / --suffix support in the shared scripts.
        # For now, steps 1-2 run standalone; steps 3-6 need a manual merge:
        #   python3 -m workflows.run_pipeline_kr build --step 1   (fetches KR tickers + snapshots)
        # Then merge snapshots_kr.parquet into snapshots.parquet and re-run
        # python3 -m workflows.run_pipeline build --step 3 on the combined file.
        #
        # Automated merge + unified run is planned for after first KR data fetch.
        if args.step > 2:
            print('NOTE: Steps 3-6 for KR data require the unified merge workflow.')
            print('      After step 2 completes, merge KR + US snapshots and run:')
            print('        python3 -m workflows.run_pipeline build --step 3')
            print()

        build(start_step=min(args.step, 2), limit=args.limit)


if __name__ == '__main__':
    main()
