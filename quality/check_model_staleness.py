#!/usr/bin/env python3
"""
Check whether trained model artifacts are older than source data.

Compares mtime of models/model_meta.json against data/historical_dataset_clean.parquet.
If model is older than data, the predictions may be stale.

Usage:
    python3 quality/check_model_staleness.py            # warn only (exit 0)
    python3 quality/check_model_staleness.py --strict   # exit 1 if stale
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from _root import ROOT

MODEL_META = ROOT / "models" / "model_meta.json"
DATA_FILE = ROOT / "data" / "historical_dataset_clean.parquet"


def check_staleness(strict: bool = False) -> int:
    if not MODEL_META.exists():
        print(f"WARNING: {MODEL_META} not found — cannot check staleness.")
        return 1 if strict else 0

    if not DATA_FILE.exists():
        print(f"WARNING: {DATA_FILE} not found — cannot check staleness.")
        return 0

    model_mtime = MODEL_META.stat().st_mtime
    data_mtime = DATA_FILE.stat().st_mtime

    model_dt = datetime.fromtimestamp(model_mtime)
    data_dt = datetime.fromtimestamp(data_mtime)

    if model_mtime >= data_mtime:
        print(f"OK: model ({model_dt:%Y-%m-%d %H:%M}) is newer than data ({data_dt:%Y-%m-%d %H:%M}).")
        return 0

    days_stale = (data_dt - model_dt).days
    msg = (
        f"STALE: model ({model_dt:%Y-%m-%d %H:%M}) is {days_stale} days older "
        f"than data ({data_dt:%Y-%m-%d %H:%M}). "
        f"Re-run: python3 modeling/train.py && python3 modeling/score_oof.py"
    )

    if strict:
        print(f"ERROR: {msg}")
        return 1
    else:
        print(f"WARNING: {msg}")
        return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Check model staleness vs data")
    parser.add_argument("--strict", action="store_true",
                        help="Exit 1 if model is stale (default: warn only)")
    args = parser.parse_args()
    sys.exit(check_staleness(strict=args.strict))


if __name__ == "__main__":
    main()
