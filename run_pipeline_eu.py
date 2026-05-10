#!/usr/bin/env python3
"""
EU pipeline runner (SimFin).

Steps:
  1. step2_build_snapshots_eu → data/snapshots_eu.parquet
     (No step1 needed — SimFin bulk API returns all EU companies)
  2. step3_enrich_prices_eu   → data/prices_eu.parquet
  3. step4_enrich_macro       → data/macro_eu.parquet
  4. step5_compute_features   → data/historical_dataset_eu.parquet
  5. step6_clean              → data/historical_dataset_clean_eu.parquet

Usage:
  python3 run_pipeline_eu.py build              # full build
  python3 run_pipeline_eu.py build --step 2     # resume from step 2
  python3 run_pipeline_eu.py build --markets de gb se  # specific markets
  python3 run_pipeline_eu.py build --limit 50   # test run
  python3 run_pipeline_eu.py status             # check file state
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent
DATA = BASE / 'data'
PIPE = BASE / 'pipeline'


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
    print('\n── EU Pipeline Status ────────────────────────────────')
    files = [
        ('snapshots_eu.parquet',                'SimFin EU financial snapshots'),
        ('prices_eu.parquet',                   'Price enrichment (EU)'),
        ('macro_eu.parquet',                    'Macro enrichment (EU)'),
        ('historical_dataset_eu.parquet',       'Full feature dataset (EU)'),
        ('historical_dataset_clean_eu.parquet', 'Clean final dataset (EU)'),
    ]
    for fname, label in files:
        p = DATA / fname
        mark = '✓' if p.exists() else '○'
        print(f'  {mark} {label}')
        if p.exists():
            print(fmt(p))
    print()


def run_step(script: str, extra: list[str]):
    cmd = [sys.executable, script] + extra
    print(f'\n{"="*60}')
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Running: {" ".join(cmd)}')
    print('='*60)
    result = subprocess.run(cmd, cwd=BASE)
    if result.returncode != 0:
        print(f'\nERROR: {script} failed (exit {result.returncode})')
        sys.exit(result.returncode)


EU_STEPS = {
    1: (str(PIPE / 'step2_build_snapshots_eu.py'), 'SimFin EU snapshots'),
    2: (str(PIPE / 'step3_enrich_prices.py'),       'Price enrichment (EU)'),
}

# Steps 3-5 (macro, features, clean) require merging EU snapshots into the
# combined dataset first, then re-running run_pipeline.py build --step 4.
# See merge_snapshots.py (planned) for the merge workflow.

STEP_LABELS = {
    1: 'Step 1/2 — SimFin EU snapshots',
    2: 'Step 2/2 — Price enrichment',
}


def build(start_step: int, limit: int | None, markets: list[str] | None):
    for step_num in range(max(start_step, 1), 3):
        script, label = EU_STEPS[step_num]
        extra = []

        if step_num == 1:
            if markets:
                extra += ['--markets'] + markets
            if limit:
                extra += ['--limit', str(limit)]

        elif step_num == 2:
            extra += ['--snapshots', str(DATA / 'snapshots_eu.parquet')]
            extra += ['--out',       str(DATA / 'prices_eu.parquet')]
            if limit:
                extra += ['--limit', str(limit)]

        run_step(script, extra)

    print('\n' + '='*60)
    print(f'[{datetime.now().strftime("%H:%M:%S")}] EU STEPS 1-2 COMPLETE')
    print('='*60)
    print()
    print('Next: merge EU into combined dataset, then run macro + features:')
    print('  python3 merge_snapshots.py             # combine US + KR + EU')
    print('  python3 run_pipeline.py build --step 4 # macro → features → clean')
    print()
    status()


def main():
    parser = argparse.ArgumentParser(description='EU (SimFin) pipeline runner')
    sub = parser.add_subparsers(dest='cmd')

    p_build = sub.add_parser('build')
    p_build.add_argument('--step',    type=int,  default=1,    help='Resume from step N (1-2)')
    p_build.add_argument('--limit',   type=int,  default=None, help='Limit companies per market (test runs)')
    p_build.add_argument('--markets', nargs='+', default=None,
                         help='Markets to load (default: all 8). E.g.: de gb se')

    sub.add_parser('status')

    args = parser.parse_args()

    if args.cmd == 'status' or args.cmd is None:
        status()
    elif args.cmd == 'build':
        build(start_step=args.step, limit=args.limit,
              markets=getattr(args, 'markets', None))


if __name__ == '__main__':
    main()


if __name__ == '__main__':
    main()
