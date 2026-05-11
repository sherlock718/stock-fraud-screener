"""
Dataset quality fixes — run once after any full pipeline rebuild, before training.

Fixes applied:
  1. Drop columns with 100% null rate across the whole dataset
  2. Add `is_forecast` flag: True for fiscal_year >= FORECAST_YEAR
  3. Winsorize `accruals_to_assets` at 1st/99th percentile per (market, fiscal_year)
  4. Fix `gross_margin` values > 1.5 — divide by 100 (percentage format error)

Usage:
    python3 scripts/fix_dataset_quality.py                      # in-place fix
    python3 scripts/fix_dataset_quality.py --dry-run            # report only, no write
    python3 scripts/fix_dataset_quality.py --src custom.parquet # custom input
    python3 scripts/fix_dataset_quality.py --out fixed.parquet  # custom output

Returns exit code 0 on success, 1 on error.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).parent.parent
DEFAULT_SRC = BASE / "data" / "historical_dataset_clean.parquet"

# fiscal_year >= this value is considered projected/forecast data
FORECAST_YEAR = 2025

# Columns known to be 100% null in the current dataset — hard-drop list.
# Any column that turns out to be 100% null in a future build is also dropped
# dynamically, but we record this explicit list for auditability.
KNOWN_NULL_COLUMNS = [
    "roic",
    "ppe",
    "total_equity",
    "roe_sector_pct",
    "pb_ratio",
    "pb_ratio_sector_pct",
    "acc_mt",
    "corp_code",
    "earnings_stability_5yr",
    "book_to_market",
]


def fix_null_columns(df: pd.DataFrame, verbose: bool = True) -> tuple[pd.DataFrame, list[str]]:
    """Drop columns with 100% null rate. Returns modified df + list of dropped column names."""
    null_rates = df.isnull().mean()
    dead_cols = null_rates[null_rates == 1.0].index.tolist()

    # Also drop explicitly known-null columns still present
    for c in KNOWN_NULL_COLUMNS:
        if c in df.columns and c not in dead_cols:
            null_rate = df[c].isnull().mean()
            if null_rate > 0.95:  # >= 95% null — effectively dead
                dead_cols.append(c)

    dead_cols = sorted(set(dead_cols))
    if dead_cols:
        df = df.drop(columns=dead_cols)
        if verbose:
            print(f"  Dropped {len(dead_cols)} dead columns: {dead_cols}")
    else:
        if verbose:
            print("  No 100%-null columns found.")
    return df, dead_cols


def add_forecast_flag(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """Add boolean `is_forecast` column. True for fiscal_year >= FORECAST_YEAR."""
    if "is_forecast" not in df.columns:
        df = df.copy()
        df["is_forecast"] = df["fiscal_year"].ge(FORECAST_YEAR)
        n_forecast = df["is_forecast"].sum()
        if verbose:
            print(
                f"  Added `is_forecast` flag: {n_forecast:,} rows flagged "
                f"(fiscal_year >= {FORECAST_YEAR})"
            )
    else:
        # Recompute in case data was extended
        old_count = df["is_forecast"].sum()
        df = df.copy()
        df["is_forecast"] = df["fiscal_year"].ge(FORECAST_YEAR)
        new_count = df["is_forecast"].sum()
        if verbose and old_count != new_count:
            print(
                f"  Updated `is_forecast`: {old_count:,} → {new_count:,} rows flagged"
            )
        elif verbose:
            print(f"  `is_forecast` already present: {new_count:,} rows flagged")
    return df


def winsorize_accruals(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Winsorize `accruals_to_assets` at 1st/99th percentile per (market, fiscal_year).
    Global winsorize would be too aggressive across markets; per-group is fairer.
    Falls back to global if a group has < 20 rows.
    """
    if "accruals_to_assets" not in df.columns:
        if verbose:
            print("  `accruals_to_assets` not present — skipped.")
        return df

    df = df.copy()
    col = "accruals_to_assets"
    original_max = df[col].abs().max()
    n_changed = 0

    def _winsorize_group(grp: pd.DataFrame) -> pd.DataFrame:
        nonlocal n_changed
        series = grp[col].dropna()
        if len(series) < 20:
            # Too few rows for reliable quantile — use global percentiles
            lo = df[col].quantile(0.01)
            hi = df[col].quantile(0.99)
        else:
            lo = series.quantile(0.01)
            hi = series.quantile(0.99)
        mask = grp[col].notna()
        before = (grp.loc[mask, col] < lo).sum() + (grp.loc[mask, col] > hi).sum()
        grp = grp.copy()
        grp.loc[mask, col] = grp.loc[mask, col].clip(lo, hi)
        n_changed += before
        return grp

    if "market" in df.columns and "fiscal_year" in df.columns:
        df = df.groupby(["market", "fiscal_year"], group_keys=False).apply(_winsorize_group)
    else:
        lo = df[col].quantile(0.01)
        hi = df[col].quantile(0.99)
        n_changed = (df[col] < lo).sum() + (df[col] > hi).sum()
        df[col] = df[col].clip(lo, hi)

    new_max = df[col].abs().max()
    if verbose:
        print(
            f"  Winsorized `accruals_to_assets`: {original_max:.1f} → {new_max:.4f} max abs; "
            f"{n_changed:,} values clipped"
        )
    return df


def fix_gross_margin(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """
    Fix `gross_margin` values stored as percentage (0–100) instead of decimal (0–1).
    Heuristic: if value > 1.5, divide by 100. Threshold of 1.5 allows for genuinely
    high gross margins (e.g. software at 80% = 0.80) while catching % format errors.
    """
    if "gross_margin" not in df.columns:
        if verbose:
            print("  `gross_margin` not present — skipped.")
        return df

    df = df.copy()
    mask = df["gross_margin"] > 1.5
    n_fixed = mask.sum()
    if n_fixed > 0:
        df.loc[mask, "gross_margin"] = df.loc[mask, "gross_margin"] / 100.0
        # After fixing, some may still be > 1.0 (e.g. gross margin > 100% is unusual but valid
        # for some service businesses with negative COGS adjustments) — leave those.
        remaining = (df["gross_margin"] > 1.0).sum()
        if verbose:
            print(
                f"  Fixed `gross_margin` > 1.5: {n_fixed:,} values divided by 100. "
                f"Remaining > 1.0: {remaining:,} (may be legitimate)"
            )
    else:
        if verbose:
            print("  `gross_margin`: no values > 1.5 found.")
    return df


def run(
    src: Path,
    out: Path | None,
    dry_run: bool = False,
    verbose: bool = True,
) -> bool:
    if not src.exists():
        print(f"ERROR: {src} not found")
        return False

    print(f"\nLoading {src.name} ...")
    df = pd.read_parquet(src)
    original_shape = df.shape
    print(f"  Shape: {original_shape[0]:,} rows × {original_shape[1]} columns\n")

    print("Fix 1: Drop dead columns (100% null)")
    df, dropped = fix_null_columns(df, verbose=verbose)

    print("\nFix 2: Add is_forecast flag")
    df = add_forecast_flag(df, verbose=verbose)

    print("\nFix 3: Winsorize accruals_to_assets")
    df = winsorize_accruals(df, verbose=verbose)

    print("\nFix 4: Fix gross_margin percentage format")
    df = fix_gross_margin(df, verbose=verbose)

    print(f"\nResult: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"  Columns removed: {original_shape[1] - df.shape[1]}")

    if dry_run:
        print("\nDry run — no file written.")
        return True

    dest = out or src
    print(f"\nWriting to {dest} ...")
    df.to_parquet(dest, index=False)
    size_mb = dest.stat().st_size / 1_048_576
    print(f"  Done — {size_mb:.1f} MB")
    return True


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Fix dataset quality issues")
    p.add_argument("--src", default=str(DEFAULT_SRC), help="Input parquet path")
    p.add_argument("--out", default=None, help="Output path (default: overwrite src)")
    p.add_argument("--dry-run", action="store_true", help="Report only, no write")
    p.add_argument("--quiet", action="store_true", help="Suppress verbose output")
    args = p.parse_args()

    ok = run(
        src=Path(args.src),
        out=Path(args.out) if args.out else None,
        dry_run=args.dry_run,
        verbose=not args.quiet,
    )
    sys.exit(0 if ok else 1)
