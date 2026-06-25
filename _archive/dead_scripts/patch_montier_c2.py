"""
Patch montier C-score columns in historical_dataset_clean.parquet.

Fixes: montier_c2 was 100% null because property_plant_equipment (96% null)
was used instead of ppe_net (19% null).  step5 now uses ppe_net; this script
applies the corrected computation to the existing parquet without a full rebuild.

Usage:
    python3 scripts/patch_montier_c2.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.step5_compute_features import add_montier_c_score
from scripts._root import ROOT

PARQUET = ROOT / "data" / "historical_dataset_clean.parquet"
MONTIER_COLS = [
    "montier_c1", "montier_c2", "montier_c3",
    "montier_c4", "montier_c5", "montier_c6",
    "montier_c_score",
]


def main(dry_run: bool = False) -> None:
    print(f"Loading {PARQUET} …")
    df = pd.read_parquet(PARQUET)
    original_shape = df.shape
    print(f"  Shape: {original_shape[0]:,} rows × {original_shape[1]} cols")

    # Drop old montier cols so the function writes fresh ones
    existing = [c for c in MONTIER_COLS if c in df.columns]
    print(f"  Dropping old montier cols: {existing}")
    df.drop(columns=existing, inplace=True)

    print("Recomputing montier C-score (ppe_net fix) …")
    df = add_montier_c_score(df)

    print("\nNull-rate audit after patch:")
    for col in MONTIER_COLS:
        null_pct = df[col].isna().mean() * 100
        print(f"  {col:30s}  {null_pct:5.1f}% null")

    if dry_run:
        print("\n[dry-run] Not writing parquet.")
        return

    print(f"\nWriting {PARQUET} …")
    df.to_parquet(PARQUET, index=False)
    print(f"Done. Final shape: {df.shape[0]:,} rows × {df.shape[1]} cols")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
