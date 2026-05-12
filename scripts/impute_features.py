"""
impute_features.py — Rule-based null recovery for the clean parquet.

Two operations:
  1. Quarterly features — loads quarterly rows from historical_dataset.parquet,
     runs compute_quarterly_features(), and joins the 5 intra-year columns onto
     historical_dataset_clean.parquet (which contains only annual rows).
  2. size_category imputation — for rows where size_category is null but
     log_assets exists, assigns a 0-3 bucket via log_assets percentile rank
     within (fiscal_year, market). Adds size_category_imputed boolean flag.

Why quarterly features live here and not in enrich_quarterly_features.py:
  The clean parquet contains only annual rows (quarterly rows removed in step 6).
  enrich_quarterly_features.py expects both annual and quarterly rows in the same
  parquet. This script reads quarterly rows from historical_dataset.parquet (the
  pre-clean, unfiltered snapshot) to compute the features, then joins them onto
  the clean parquet.

Usage:
  python3 scripts/impute_features.py [--parquet PATH] [--source PATH] [--dry-run]

Flags:
  --parquet   Clean dataset path (default: data/historical_dataset_clean.parquet)
  --source    Pre-clean dataset with quarterly rows (default: data/historical_dataset.parquet)
  --dry-run   Compute but do NOT write; print coverage stats only
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.enrich_quarterly_features import compute_quarterly_features, NEW_FEATURES as QUARTERLY_COLS

SIZE_FLAG_COL = "size_category_imputed"


def _impute_size_category(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    null_mask = df["size_category"].isna() & df["log_assets"].notna()

    if null_mask.sum() == 0:
        df[SIZE_FLAG_COL] = False
        return df

    def _rank_to_bucket(s: pd.Series) -> pd.Series:
        ranks = s.rank(pct=True, na_option="keep")
        return pd.cut(ranks, bins=[0, 0.25, 0.5, 0.75, 1.0],
                      labels=[0, 1, 2, 3], include_lowest=True).astype("float32")

    imputed = df[null_mask].groupby(
        ["fiscal_year", "market"], group_keys=False
    )["log_assets"].transform(_rank_to_bucket)

    df.loc[null_mask, "size_category"] = imputed
    df[SIZE_FLAG_COL] = null_mask
    return df


def run(clean_path: Path, source_path: Path, dry_run: bool) -> None:
    print(f"Loading clean parquet: {clean_path}")
    df = pd.read_parquet(clean_path)
    print(f"  Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    # ── 1. Quarterly features ─────────────────────────────────────────────────
    print(f"\nLoading quarterly source: {source_path}")
    df_src = pd.read_parquet(source_path)
    q_rows = (df_src["period_type"] == "quarterly").sum()
    print(f"  {len(df_src):,} rows — {q_rows:,} quarterly rows available")

    print("\nComputing quarterly features…")
    q_feats = compute_quarterly_features(df_src)
    print(f"  Feature rows computed: {len(q_feats):,}")

    for col in QUARTERLY_COLS:
        fill = q_feats[col].notna().mean()
        print(f"    {col:40s} fill={fill:.1%}")

    # Drop any pre-existing quarterly cols to allow idempotent re-runs
    existing = [c for c in QUARTERLY_COLS if c in df.columns]
    if existing:
        print(f"\n  Dropping existing quarterly cols: {existing}")
        df = df.drop(columns=existing)

    df = df.merge(q_feats, on=["ticker", "fiscal_year"], how="left")
    n_enriched = df[QUARTERLY_COLS[0]].notna().sum()
    print(f"\n  Annual rows enriched: {n_enriched:,} / {len(df):,} "
          f"({n_enriched/len(df):.1%})")

    # ── 2. size_category imputation ───────────────────────────────────────────
    print("\nImputing size_category from log_assets…")
    before_null = df["size_category"].isna().sum()
    df = _impute_size_category(df)
    after_null = df["size_category"].isna().sum()
    n_imputed = df[SIZE_FLAG_COL].sum()
    print(f"  size_category null: {before_null:,} → {after_null:,} "
          f"({n_imputed:,} rows imputed from log_assets)")

    print(f"\nFinal shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    new_cols = QUARTERLY_COLS + [SIZE_FLAG_COL]
    print(f"New columns added: {new_cols}")

    if dry_run:
        print("\n[dry-run] Parquet NOT written.")
        return

    df.to_parquet(clean_path, index=False)
    print(f"\nWritten: {clean_path}  ({df.shape[0]:,} rows × {df.shape[1]} columns)")


def main():
    parser = argparse.ArgumentParser(description="Impute missing features into clean parquet")
    parser.add_argument("--parquet", default="data/historical_dataset_clean.parquet")
    parser.add_argument("--source",  default="data/historical_dataset.parquet")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    clean_path  = Path(args.parquet)
    source_path = Path(args.source)

    if not clean_path.exists():
        sys.exit(f"ERROR: {clean_path} not found")
    if not source_path.exists():
        sys.exit(f"ERROR: {source_path} not found — needed for quarterly rows")

    run(clean_path, source_path, args.dry_run)


if __name__ == "__main__":
    main()
