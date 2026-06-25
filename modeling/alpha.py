"""
compute_alpha.py — Compute 5-factor alpha scores and write to parquet.

Loads historical_dataset_clean.parquet, computes all 5 factor scores plus the
composite, and writes alpha_value, alpha_quality, alpha_momentum, alpha_growth,
alpha_fraud_risk, alpha_composite back to the same parquet.

Usage:
  python3 scripts/compute_alpha.py [--parquet PATH] [--dry-run]

Flags:
  --parquet   Path to dataset parquet (default: data/historical_dataset_clean.parquet)
  --dry-run   Compute but do NOT write parquet; print summary stats only
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from alpha.factors.composite import compute as compute_composite
from _root import ROOT

ALPHA_COLS = [
    "alpha_value", "alpha_quality", "alpha_momentum",
    "alpha_growth", "alpha_fraud_risk", "alpha_composite",
]


def run(parquet_path: Path, dry_run: bool) -> None:
    print(f"Loading dataset: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    print(f"  Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    print("\nComputing 5-factor alpha scores...")
    scores = compute_composite(df)

    for col in ALPHA_COLS:
        s = scores[col]
        pct = np.percentile(s.dropna(), [10, 25, 50, 75, 90])
        print(f"  {col}: null={s.isna().sum():,} "
              f"p10={pct[0]:.4f} p25={pct[1]:.4f} p50={pct[2]:.4f} "
              f"p75={pct[3]:.4f} p90={pct[4]:.4f}")
        df[col] = scores[col].astype("float32")

    if dry_run:
        print("\n[dry-run] Parquet NOT written.")
        return

    df.to_parquet(parquet_path, index=False)
    print(f"\nWritten: {parquet_path}  ({df.shape[0]:,} rows × {df.shape[1]} columns)")
    print(f"New columns added: {', '.join(ALPHA_COLS)}")


def main():
    parser = argparse.ArgumentParser(description="Compute 5-factor alpha scores")
    parser.add_argument("--parquet", default="data/historical_dataset_clean.parquet")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    parquet_path = Path(args.parquet)
    if not parquet_path.exists():
        sys.exit(f"ERROR: {parquet_path} not found")

    run(parquet_path, args.dry_run)


if __name__ == "__main__":
    main()
