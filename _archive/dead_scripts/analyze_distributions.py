"""
Distribution analysis report for historical_dataset_clean.parquet.

Outputs:
  reports/distribution_report.txt  — NaN%, outlier rate, skew, market availability
  reports/correlation_matrix.parquet  — pairwise Pearson correlation of numeric columns

Usage:
  python3 scripts/analyze_distributions.py [--parquet PATH] [--out-dir DIR] [--corr]
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd


def nan_summary(df: pd.DataFrame) -> pd.DataFrame:
    pct = df.isnull().mean() * 100
    return pd.DataFrame({
        "nan_pct": pct,
        "non_null": df.notnull().sum(),
        "dtype": df.dtypes.astype(str),
    }).sort_values("nan_pct", ascending=False)


def outlier_rate(col: pd.Series, z_thresh: float = 5.0) -> float:
    """Fraction of values more than z_thresh standard deviations from the mean."""
    if col.dropna().shape[0] < 2:
        return float("nan")
    mu, sigma = col.mean(), col.std()
    if sigma == 0:
        return 0.0
    return float(((col - mu).abs() > z_thresh * sigma).mean())


def market_fill(df: pd.DataFrame) -> pd.DataFrame:
    """For each numeric column, fraction non-null per market."""
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    rows = []
    for mkt, grp in df.groupby("market"):
        fill = grp[numeric].notnull().mean()
        fill.name = mkt
        rows.append(fill)
    return pd.DataFrame(rows).T


def label_balance(df: pd.DataFrame) -> dict:
    out = {}
    for col in ["fraud_confirmed", "fraud_suspect"]:
        if col in df.columns:
            counts = df[col].value_counts(dropna=False)
            out[col] = counts.to_dict()
    return out


def run(parquet_path: str, out_dir: str, include_corr: bool) -> None:
    os.makedirs(out_dir, exist_ok=True)
    df = pd.read_parquet(parquet_path)
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()

    lines = []
    lines.append(f"Distribution report — {parquet_path}")
    lines.append(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns\n")

    # NaN summary
    ns = nan_summary(df)
    high_nan = ns[ns["nan_pct"] > 50]
    lines.append(f"=== Columns with >50% NaN ({len(high_nan)}) ===")
    for col, row in high_nan.iterrows():
        lines.append(f"  {col}: {row['nan_pct']:.1f}% NaN")

    # Outlier rates
    lines.append(f"\n=== Top 20 columns by outlier rate (|z|>5) ===")
    rates = {c: outlier_rate(df[c]) for c in numeric}
    top_outliers = sorted(rates.items(), key=lambda x: x[1] if not np.isnan(x[1]) else 0, reverse=True)[:20]
    for col, rate in top_outliers:
        if not np.isnan(rate) and rate > 0:
            lines.append(f"  {col}: {rate:.4f} ({rate*100:.2f}%)")

    # Market availability
    lines.append(f"\n=== Market fill rates for key features ===")
    key_features = [
        "revenue", "net_income", "total_assets", "market_cap_at_filing",
        "enterprise_value", "beneish_m_score", "altman_z_score",
        "piotroski_f_score", "momentum_12m_rank", "fraud_confirmed",
    ]
    for mkt, grp in df.groupby("market"):
        fills = {f: f"{grp[f].notnull().mean()*100:.0f}%" for f in key_features if f in grp.columns}
        lines.append(f"  {mkt}: " + " | ".join(f"{k}={v}" for k, v in fills.items()))

    # Label balance
    lines.append(f"\n=== Fraud label balance ===")
    for col, counts in label_balance(df).items():
        lines.append(f"  {col}: {counts}")

    # Market/fiscal year coverage
    lines.append(f"\n=== Rows per market ===")
    for mkt, grp in df.groupby("market"):
        yr_min = grp["fiscal_year"].min()
        yr_max = grp["fiscal_year"].max()
        lines.append(f"  {mkt}: {len(grp):,} rows, fiscal years {yr_min}–{yr_max}")

    report_path = os.path.join(out_dir, "distribution_report.txt")
    with open(report_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"Report written → {report_path}")

    if include_corr:
        corr_cols = [c for c in numeric if df[c].notnull().sum() > 1000]
        corr = df[corr_cols].corr()
        corr_path = os.path.join(out_dir, "correlation_matrix.parquet")
        corr.to_parquet(corr_path)
        print(f"Correlation matrix written → {corr_path}")

        # Print high-correlation pairs (> 0.95, excluding self)
        high_corr = []
        for i in range(len(corr.columns)):
            for j in range(i + 1, len(corr.columns)):
                val = corr.iloc[i, j]
                if abs(val) > 0.95:
                    high_corr.append((corr.columns[i], corr.columns[j], val))
        if high_corr:
            print(f"\nHigh-correlation pairs (|r|>0.95): {len(high_corr)}")
            for a, b, v in sorted(high_corr, key=lambda x: abs(x[2]), reverse=True)[:20]:
                print(f"  {a} × {b}: {v:.3f}")


def main():
    parser = argparse.ArgumentParser(description="Dataset distribution analysis")
    parser.add_argument("--parquet", default="data/historical_dataset_clean.parquet")
    parser.add_argument("--out-dir", default="reports")
    parser.add_argument("--corr", action="store_true", help="Also compute correlation matrix")
    args = parser.parse_args()

    if not os.path.exists(args.parquet):
        print(f"ERROR: {args.parquet} not found", file=sys.stderr)
        sys.exit(1)

    run(args.parquet, args.out_dir, args.corr)


if __name__ == "__main__":
    main()
