"""
Mark likely-delisted companies and impute pessimistic forward returns.

Survivorship bias arises when companies that delisted/went bankrupt between the
training window are silently excluded because they have no forward-return data.
The model then only sees "survivors", inflating apparent performance.

This script:
  1. Identifies companies whose last annual filing year is more than LAG years
     before the dataset's maximum year — these are likely-delisted.
  2. For their final row, if forward_return_1y is NaN, imputes DELISTING_RETURN.
  3. Adds a `likely_delisted` boolean column.
  4. Writes the corrected dataset back to the same parquet file (or --out path).

Usage:
    python3 scripts/mark_survivorship.py               # report only (dry-run)
    python3 scripts/mark_survivorship.py --fix         # write corrected parquet
    python3 scripts/mark_survivorship.py --fix --lag 3 # custom lag threshold
    python3 scripts/mark_survivorship.py --fix --out data/historical_survivorship.parquet
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scripts._root import ROOT

BASE = ROOT

DATA_PATH  = BASE / "data" / "historical_dataset_clean.parquet"
FALLBACK   = BASE / "data" / "historical_dataset.parquet"

# Companies whose last filing year is this many years before dataset max
# are considered likely-delisted.
DEFAULT_LAG = 3

# Forward return imputed for likely-delisted companies with no return data.
# -0.50 = conservative loss assumption (50% drawdown before/at delisting).
DELISTING_RETURN = -0.50


def _resolve_data_path() -> Path:
    for p in [DATA_PATH, FALLBACK]:
        if p.exists():
            return p
    print("ERROR: No dataset parquet found.")
    sys.exit(1)


def mark_survivorship(df: pd.DataFrame, lag: int) -> pd.DataFrame:
    """Add likely_delisted flag and impute delisting returns."""
    df = df.copy()
    ann = df[df["period_type"] == "annual"] if "period_type" in df.columns else df

    max_year = int(ann["fiscal_year"].max())
    last_year_per_ticker = (ann.groupby("ticker")["fiscal_year"].max()
                              .rename("last_filing_year"))

    delisted_tickers = last_year_per_ticker[
        last_year_per_ticker <= (max_year - lag)
    ].index

    df["likely_delisted"] = df["ticker"].isin(delisted_tickers)

    n_delisted = df["likely_delisted"].sum()
    print(f"  Dataset max year        : {max_year}")
    print(f"  Delisting lag threshold : {lag} years  (last_year ≤ {max_year - lag})")
    print(f"  Likely-delisted tickers : {len(delisted_tickers):,}")
    print(f"  Rows flagged            : {n_delisted:,} / {len(df):,} "
          f"({100*n_delisted/max(len(df),1):.1f}%)")

    # For likely-delisted companies, impute forward_return = DELISTING_RETURN
    # only on their final filing row (where return is NaN — they never recovered).
    imputed = 0
    for h in ["1y", "3y", "5y"]:
        col = f"forward_return_{h}"
        if col not in df.columns:
            continue
        # Identify last annual row per delisted ticker
        last_rows = (df[df["likely_delisted"] & (df.get("period_type", "annual") == "annual")]
                       .sort_values("fiscal_year")
                       .drop_duplicates("ticker", keep="last")
                       .index)
        missing_return = df.loc[last_rows, col].isna()
        fill_idx = last_rows[missing_return]
        df.loc[fill_idx, col] = DELISTING_RETURN
        imputed += int(missing_return.sum())
    print(f"  Forward returns imputed : {imputed:,} ({DELISTING_RETURN:.0%} assigned)")

    # Also update beat_local_market labels if present
    for h in ["1y", "3y", "5y"]:
        ret_col  = f"forward_return_{h}"
        beat_col = f"beat_local_market_{h}"
        if ret_col not in df.columns or beat_col not in df.columns:
            continue
        # For rows where we imputed the return, recalculate the beat label.
        # Delisted companies with -50% returns won't beat the market.
        mask = df["likely_delisted"] & (df[beat_col].isna()) & df[ret_col].notna()
        df.loc[mask, beat_col] = 0

    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Mark survivorship bias in dataset")
    parser.add_argument("--fix", action="store_true",
                        help="Write corrected parquet (default: dry-run / report only)")
    parser.add_argument("--lag", type=int, default=DEFAULT_LAG,
                        help=f"Years of silence before marking delisted (default: {DEFAULT_LAG})")
    parser.add_argument("--out", type=Path, default=None,
                        help="Output path (default: overwrites the input parquet)")
    args = parser.parse_args()

    data_path = _resolve_data_path()
    print(f"Loading {data_path}...")
    df = pd.read_parquet(data_path)
    print(f"  {len(df):,} rows × {len(df.columns)} columns")

    print("\nAnalysing survivorship bias...")
    df_corrected = mark_survivorship(df, lag=args.lag)

    if not args.fix:
        print("\nDry-run complete. Pass --fix to write the corrected parquet.")
        return

    out_path = args.out or data_path
    df_corrected.to_parquet(out_path, index=False)
    print(f"\n✓ Corrected dataset written → {out_path}")
    print(f"  Columns added: likely_delisted")
    print(f"  Forward returns imputed for likely-delisted companies: {DELISTING_RETURN:.0%}")


if __name__ == "__main__":
    main()
