"""
Compute quarterly-derived features and join them onto annual rows.

For each annual row (ticker, fiscal_year), we look at Q1/Q2/Q3 rows in the
same fiscal_year and compute intra-year dynamics that are invisible in the
annual filing:
  - revenue_qoq_std_norm   : std of Q1→Q2→Q3 revenue growth, normalised (earnings smoothing)
  - earnings_qoq_mean      : mean QoQ net_income growth (earnings momentum)
  - max_accruals_ttm       : max |wc_accruals_to_assets| across available quarters
  - revenue_acceleration   : Q3/Q1 revenue ratio (intra-year sales ramp)
  - quarterly_positive_rev_frac : fraction of quarters with positive QoQ revenue growth

Usage:
    python3 scripts/enrich_quarterly_features.py                 # dry-run, prints stats
    python3 scripts/enrich_quarterly_features.py --fix           # writes parquet in-place
    python3 scripts/enrich_quarterly_features.py --fix --out data/historical_dataset_enriched.parquet
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE      = Path(__file__).parent.parent
DATA_PATH = BASE / "data" / "historical_dataset_clean.parquet"
FALLBACK  = BASE / "data" / "historical_dataset.parquet"

NEW_FEATURES = [
    "revenue_qoq_std_norm",
    "earnings_qoq_mean",
    "max_accruals_ttm",
    "revenue_acceleration",
    "quarterly_positive_rev_frac",
]


def _resolve_data_path() -> Path:
    for p in [DATA_PATH, FALLBACK]:
        if p.exists():
            return p
    print("ERROR: No dataset parquet found.")
    sys.exit(1)


def _extract_qnum(fq) -> int | None:
    """Convert 'Q1'/'2025Q1' style strings to 1/2/3."""
    if not isinstance(fq, str):
        return None
    for i in range(1, 5):
        if fq.endswith(f"Q{i}"):
            return i
    return None


def _safe_pct_change(series: pd.Series) -> pd.Series:
    """QoQ percentage change; NaN where prior or current is 0/NaN."""
    prev = series.shift(1)
    with np.errstate(divide="ignore", invalid="ignore"):
        chg = np.where(
            (prev == 0) | prev.isna() | series.isna(),
            np.nan,
            (series - prev) / prev.abs(),
        )
    return pd.Series(chg, index=series.index)


def compute_quarterly_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a DataFrame indexed by (ticker, fiscal_year) with NEW_FEATURES columns.
    Only rows with at least 2 quarterly data points are included.
    """
    q = df[df["period_type"] == "quarterly"].copy()
    q["q_num"] = q["fiscal_quarter"].apply(_extract_qnum)
    q = q[q["q_num"].notna()].copy()

    # Sort within each ticker by time
    q = q.sort_values(["ticker", "fiscal_year", "q_num"])

    # For each (ticker, fiscal_year) group, compute intra-year features
    records = []

    for (ticker, fy), grp in q.groupby(["ticker", "fiscal_year"], sort=False):
        grp = grp.sort_values("q_num")

        rev   = grp["revenue"].values if "revenue" in grp.columns else None
        ni    = grp["net_income"].values if "net_income" in grp.columns else None
        accr  = grp["wc_accruals_to_assets"].values if "wc_accruals_to_assets" in grp.columns else None
        n     = len(grp)

        if n < 2:
            continue

        rec: dict[str, object] = {"ticker": ticker, "fiscal_year": fy}

        # --- revenue_qoq_std_norm ---
        # std of QoQ revenue growth, normalised by mean quarterly revenue
        if rev is not None and np.count_nonzero(~np.isnan(rev)) >= 2:
            prev = rev[:-1]
            curr = rev[1:]
            valid = (prev != 0) & ~np.isnan(prev) & ~np.isnan(curr)
            if valid.sum() >= 1:
                with np.errstate(divide="ignore", invalid="ignore"):
                    chg = np.where(valid, (curr - prev) / np.abs(np.where(prev == 0, np.nan, prev)), np.nan)
                rec["revenue_qoq_std_norm"] = float(np.nanstd(chg))
            else:
                rec["revenue_qoq_std_norm"] = np.nan
        else:
            rec["revenue_qoq_std_norm"] = np.nan

        # --- earnings_qoq_mean ---
        # mean QoQ net_income growth across available quarters
        if ni is not None and np.count_nonzero(~np.isnan(ni)) >= 2:
            prev = ni[:-1]
            curr = ni[1:]
            valid = (prev != 0) & ~np.isnan(prev) & ~np.isnan(curr)
            if valid.sum() >= 1:
                with np.errstate(divide="ignore", invalid="ignore"):
                    chg = np.where(valid, (curr - prev) / np.abs(np.where(prev == 0, np.nan, prev)), np.nan)
                rec["earnings_qoq_mean"] = float(np.nanmean(chg))
            else:
                rec["earnings_qoq_mean"] = np.nan
        else:
            rec["earnings_qoq_mean"] = np.nan

        # --- max_accruals_ttm ---
        if accr is not None and np.count_nonzero(~np.isnan(accr)) >= 1:
            rec["max_accruals_ttm"] = float(np.nanmax(np.abs(accr)))
        else:
            rec["max_accruals_ttm"] = np.nan

        # --- revenue_acceleration ---
        # Q3/Q1 ratio; captures intra-year sales ramp (smoothed firms vs lumpy)
        if rev is not None:
            q_nums = grp["q_num"].values
            q1_mask = q_nums == 1
            q3_mask = q_nums == 3
            q1_rev = rev[q1_mask]
            q3_rev = rev[q3_mask]
            if len(q1_rev) == 1 and len(q3_rev) == 1 and q1_rev[0] not in (0, np.nan) and not np.isnan(q1_rev[0]):
                rec["revenue_acceleration"] = float(q3_rev[0] / q1_rev[0])
            else:
                rec["revenue_acceleration"] = np.nan
        else:
            rec["revenue_acceleration"] = np.nan

        # --- quarterly_positive_rev_frac ---
        if rev is not None and np.count_nonzero(~np.isnan(rev)) >= 2:
            prev = rev[:-1]
            curr = rev[1:]
            valid = ~np.isnan(prev) & ~np.isnan(curr)
            if valid.sum() >= 1:
                pos = np.sum((curr[valid] > prev[valid]))
                rec["quarterly_positive_rev_frac"] = float(pos / valid.sum())
            else:
                rec["quarterly_positive_rev_frac"] = np.nan
        else:
            rec["quarterly_positive_rev_frac"] = np.nan

        records.append(rec)

    if not records:
        return pd.DataFrame(columns=["ticker", "fiscal_year"] + NEW_FEATURES)

    result = pd.DataFrame(records)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich annual rows with quarterly-derived features")
    parser.add_argument("--fix", action="store_true",
                        help="Write enriched parquet (default: dry-run)")
    parser.add_argument("--out", type=Path, default=None,
                        help="Output path (default: overwrites input parquet)")
    args = parser.parse_args()

    data_path = _resolve_data_path()
    print(f"Loading {data_path}…")
    df = pd.read_parquet(data_path)
    print(f"  {len(df):,} rows × {len(df.columns)} columns")

    print("\nComputing quarterly-derived features…")
    q_feats = compute_quarterly_features(df)
    print(f"  Quarterly feature rows computed: {len(q_feats):,}")
    for f in NEW_FEATURES:
        fill = q_feats[f].notna().mean()
        print(f"    {f:35s} fill={fill:.1%}")

    # Drop existing columns to avoid dup on re-run
    existing = [c for c in NEW_FEATURES if c in df.columns]
    if existing:
        print(f"\n  Dropping existing columns: {existing}")
        df = df.drop(columns=existing)

    # Merge onto annual rows only; quarterly rows get NaN (they already have raw data)
    ann_mask = df["period_type"] == "annual"
    df_ann   = df[ann_mask].copy()
    df_other = df[~ann_mask].copy()

    df_ann = df_ann.merge(q_feats, on=["ticker", "fiscal_year"], how="left")

    enriched_count = df_ann[NEW_FEATURES[0]].notna().sum()
    print(f"\n  Annual rows enriched (at least 1 feature): {enriched_count:,} / {len(df_ann):,} "
          f"({enriched_count/len(df_ann):.1%})")

    df_out = pd.concat([df_ann, df_other], ignore_index=True)
    print(f"  Final dataset: {len(df_out):,} rows × {len(df_out.columns)} columns")
    print(f"  New columns added: {NEW_FEATURES}")

    if not args.fix:
        print("\nDry-run complete. Pass --fix to write the enriched parquet.")
        return

    out_path = args.out or data_path
    df_out.to_parquet(out_path, index=False)
    print(f"\n✓ Enriched dataset written → {out_path}")


if __name__ == "__main__":
    main()
