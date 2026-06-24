#!/usr/bin/env python3
"""
wait_and_merge.py — Polls until all market pipelines complete, then auto-merges.

Run this once in a terminal and leave it. It will:
  1. Wait for snapshots_kr, snapshots_eu, snapshots_ca to appear/stabilise
  2. Run scripts/data_io/merge_snapshots.py --activate --backup
  3. Run python3 scripts/workflows/run_pipeline.py build --step 4  (features + clean on full dataset)

Usage:
  python3 scripts/workflows/wait_and_merge.py
"""

import subprocess
import sys
import time
from pathlib import Path
from scripts._root import ROOT

DATA = ROOT / 'data'
LOGS = ROOT / 'logs'

REQUIRED = {
    'KR': DATA / 'snapshots_kr.parquet',
    'JP': DATA / 'snapshots_jp.parquet',
}

# Already done — just needs KR and JP
ALREADY_DONE = {
    'BR': DATA / 'snapshots_br.parquet',
    'EU': DATA / 'snapshots_eu.parquet',
    'CA': DATA / 'snapshots_ca.parquet',
}

CHECK_INTERVAL = 120   # check every 2 minutes
MIN_SIZE_BYTES  = 10_000   # file must be > 10KB to count as real data


def file_ready(path: Path) -> bool:
    if not path.exists():
        return False
    size = path.stat().st_size
    return size > MIN_SIZE_BYTES


def all_done() -> dict[str, bool]:
    return {label: file_ready(path) for label, path in REQUIRED.items()}


def summarise(status: dict[str, bool]):
    for label, done in status.items():
        sym = '✓' if done else '○'
        path = REQUIRED[label]
        size = path.stat().st_size // 1024 if path.exists() else 0
        print(f'  {sym} {label}: {"done" if done else "waiting"}'
              + (f' ({size:,} KB)' if done else ''))


def run(cmd: list[str]):
    print(f'\n$ {" ".join(cmd)}')
    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        print(f'ERROR: command failed with exit code {result.returncode}')
        sys.exit(result.returncode)


print('wait_and_merge.py — Waiting for all market pipelines to complete')
print(f'Checking every {CHECK_INTERVAL}s. Press Ctrl+C to cancel.\n')

# Show already-done markets
for label, path in ALREADY_DONE.items():
    sym = '✓' if file_ready(path) else '○'
    print(f'  {sym} {label}: {"done" if file_ready(path) else "missing"}')
for label, path in REQUIRED.items():
    sym = '✓' if file_ready(path) else '○'
    print(f'  {sym} {label}: {"done" if file_ready(path) else "waiting"}')

while True:
    status = all_done()
    remaining = [k for k, v in status.items() if not v]

    if not remaining:
        print(f'\n[{time.strftime("%H:%M:%S")}] All pipelines complete!\n')
        summarise(status)
        break

    print(f'\n[{time.strftime("%H:%M:%S")}] Still waiting: {", ".join(remaining)}')
    summarise(status)
    time.sleep(CHECK_INTERVAL)

print('\n─── Step 1: Merging all markets ───')
run(['python3', 'scripts/data_io/merge_snapshots.py', '--activate', '--backup'])

print('\n─── Step 2: Re-running features + clean on merged dataset ───')
run(['python3', 'scripts/workflows/run_pipeline.py', 'build', '--step', '4'])

print('\n✓ All done. Your combined dataset is ready.')
print('  → data/historical_dataset_clean.parquet')
