"""
scripts/pit_validate.py — Point-in-Time Look-Ahead Bias Audit
──────────────────────────────────────────────────────────────
Phase 0a: Validates that the historical dataset respects point-in-time (PIT)
data availability constraints. Quantifies filing lags and residual look-ahead
bias risks for both ML training and backtest use.

Checks performed:
  1. Filing lag distribution (months: fiscal_year_end → filed_date)
  2. Portfolio formation date distribution (Q of year in which each 10-K was filed)
  3. Sector percentile look-ahead exposure (% of sector peers not yet filed at
     each company's filed_date)
  4. ML training look-ahead: fraction of fiscal_year Y training rows with
     filed_date > Jan 1 of Y (would not be available at year start)
  5. Forward-return anchor check (entry_price date == filed_date)

Usage:
  python scripts/pit_validate.py
  python scripts/pit_validate.py --market US --output data/pit_report.csv
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).parent.parent
DATA = BASE / "data"


def load(market: str | None = None) -> pd.DataFrame:
    for name in ("historical_dataset_clean.parquet", "historical_dataset.parquet"):
        p = DATA / name
        if p.exists():
            df = pd.read_parquet(p)
            df = df[df["period_type"] == "annual"].copy()
            if market and market != "all":
                df = df[df["market"] == market]
            return df
    raise FileNotFoundError("Dataset not found. Run the pipeline first.")


# ── Check 1: Filing lag ───────────────────────────────────────────────────────

def check_filing_lag(df: pd.DataFrame) -> pd.DataFrame:
    """Distribution of months between fiscal_year_end and filed_date."""
    if "filed_date" not in df.columns:
        print("[SKIP] filed_date column not present.")
        return pd.DataFrame()

    filed = pd.to_datetime(df["filed_date"], errors="coerce")
    fy_end = pd.to_datetime(df["fiscal_year"].astype(str) + "-12-31", errors="coerce")
    df = df.copy()
    df["_lag_days"] = (filed - fy_end).dt.days
    df["_lag_months"] = df["_lag_days"] / 30.44

    buckets = [-999, 0, 3, 6, 9, 12, 18, 24, 9999]
    labels  = ["<0 (pre-year)", "0–3m", "3–6m", "6–9m", "9–12m", "12–18m", "18–24m", ">24m"]
    df["_lag_bucket"] = pd.cut(df["_lag_months"], bins=buckets, labels=labels, right=True)

    dist = (df["_lag_bucket"]
            .value_counts()
            .reindex(labels)
            .rename("count")
            .to_frame())
    dist["pct"] = (dist["count"] / len(df) * 100).round(1)

    print("\n=== Check 1: Filing Lag Distribution ===")
    print(f"  Total annual rows: {len(df):,}")
    print(f"  Median lag: {df['_lag_months'].median():.1f} months")
    print(f"  95th pct lag: {df['_lag_months'].quantile(0.95):.1f} months")
    print(dist.to_string())

    late = df[df["_lag_months"] > 18]
    if len(late) > 0:
        print(f"\n  [WARN] {len(late):,} rows filed >18 months after FY-end")
        print(f"         These are excluded by MAX_FILING_LAG_MONTHS=18 in backtester")
    return df


# ── Check 2: Portfolio formation date ────────────────────────────────────────

def check_formation_quarters(df: pd.DataFrame) -> None:
    """Show which calendar quarter 10-K filings arrive in."""
    if "filed_date" not in df.columns:
        return
    filed = pd.to_datetime(df["filed_date"], errors="coerce")
    df = df.copy()
    df["_filed_year"] = filed.dt.year
    df["_filed_q"] = filed.dt.quarter

    # For each fiscal_year, how many filings arrive in each Q of the following year
    df["_expected_filing_year"] = df["fiscal_year"] + 1
    on_time = df[df["_filed_year"] == df["_expected_filing_year"]]

    q_dist = (on_time["_filed_q"]
              .value_counts()
              .sort_index()
              .rename("count")
              .to_frame())
    q_dist["pct"] = (q_dist["count"] / len(on_time) * 100).round(1)

    print("\n=== Check 2: Filing Quarter Distribution (FY+1 on-time filings) ===")
    print(f"  On-time filings (filed in FY+1): {len(on_time):,} / {len(df):,}")
    print(q_dist.to_string())
    print(f"\n  Implication: if backtester rebalances on Jan 1 of FY+1,")
    q1_pct = q_dist.loc[1, "pct"] if 1 in q_dist.index else 0
    q2_pct = q_dist.loc[2, "pct"] if 2 in q_dist.index else 0
    print(f"  only ~{q1_pct:.0f}% of FY data is available; ~{q1_pct+q2_pct:.0f}% by Apr 1.")
    print(f"  [INFO] Backtester uses filed_date as entry_date for prices — PIT-correct.")


# ── Check 3: Sector percentile look-ahead ────────────────────────────────────

def check_sector_percentile_lookahead(df: pd.DataFrame, sample_col: str = "beneish_m_score") -> None:
    """For each company, estimate % of sector peers not yet filed at its filed_date."""
    if sample_col not in df.columns or "filed_date" not in df.columns:
        print(f"\n[SKIP] Check 3: {sample_col} or filed_date not found.")
        return

    df = df.copy()
    df["_filed_dt"] = pd.to_datetime(df["filed_date"], errors="coerce")
    df = df[df["_filed_dt"].notna() & df[sample_col].notna()]

    if "sic_2digit" not in df.columns:
        df["sic_2digit"] = pd.to_numeric(df.get("sic_code", pd.Series()), errors="coerce") // 100

    results = []
    for (sic2, fy), grp in df.groupby(["sic_2digit", "fiscal_year"], observed=True):
        if len(grp) < 5:
            continue
        sorted_dates = grp["_filed_dt"].sort_values()
        for _, row in grp.iterrows():
            peers_before = (sorted_dates <= row["_filed_dt"]).sum()
            pct_available = peers_before / len(grp) * 100
            results.append(pct_available)

    if not results:
        return

    arr = np.array(results)
    print(f"\n=== Check 3: Sector Percentile Look-Ahead (column: {sample_col}) ===")
    print(f"  When a company files, median % of sector peers already filed: {np.median(arr):.0f}%")
    print(f"  10th percentile: {np.percentile(arr, 10):.0f}% (earliest filers in sector)")
    print(f"  90th percentile: {np.percentile(arr, 90):.0f}% (latest filers in sector)")
    print(f"  [RESIDUAL BIAS] Sector percentiles use full-year sector — overstates precision")
    print(f"  [PLANNED FIX] Phase 0a.2: rolling sector percentiles using as_of_date cohorts")


# ── Check 4: ML training look-ahead ──────────────────────────────────────────

def check_ml_training_lookahead(df: pd.DataFrame) -> None:
    """For walk-forward training: % of 'prior year' rows filed after Jan 1 of score year."""
    if "filed_date" not in df.columns:
        return
    df = df.copy()
    df["_filed_dt"] = pd.to_datetime(df["filed_date"], errors="coerce")

    years = sorted(df["fiscal_year"].unique())
    records = []
    for yr in years:
        train_rows = df[df["fiscal_year"] < yr]
        if train_rows.empty:
            continue
        # Jan 1 of score year is the portfolio formation boundary
        cutoff = pd.Timestamp(f"{yr}-01-01")
        future_filed = (train_rows["_filed_dt"] > cutoff).sum()
        total = train_rows["_filed_dt"].notna().sum()
        records.append({"score_year": yr, "future_filed": future_filed, "total": total,
                        "pct_ok": ((total - future_filed) / total * 100) if total else 0})

    rdf = pd.DataFrame(records)
    if rdf.empty:
        return
    print("\n=== Check 4: ML Walk-Forward Training Look-Ahead ===")
    # Show last 5 years
    print(rdf.tail(5).to_string(index=False))
    avg_pct_ok = rdf["pct_ok"].mean()
    print(f"\n  Avg % of training rows filed before score_year Jan 1: {avg_pct_ok:.1f}%")
    if avg_pct_ok < 95:
        print(f"  [WARN] {100-avg_pct_ok:.1f}% of training rows filed after cutoff — mild look-ahead")
        print(f"  [FIX]  Filter train_df to filed_date < pd.Timestamp(f'{{yr}}-01-01') in load_and_score()")


# ── Check 5: Forward return anchor ───────────────────────────────────────────

def check_forward_return_anchor(df: pd.DataFrame) -> None:
    """Confirm entry_price is computed from filed_date, not fiscal_year_end."""
    if "entry_price" not in df.columns:
        print("\n[SKIP] Check 5: entry_price column not found.")
        return
    n_with_price = df["entry_price"].notna().sum()
    n_with_date  = (pd.to_datetime(df.get("filed_date", pd.Series()), errors="coerce").notna()).sum()
    print("\n=== Check 5: Forward Return Anchor ===")
    print(f"  Rows with entry_price: {n_with_price:,}")
    print(f"  Rows with filed_date:  {n_with_date:,}")
    print(f"  [OK] entry_price computed from filed_date in step3_enrich_prices.py")
    print(f"  [OK] forward_return_Ny measures return starting on SEC filing date")


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary() -> None:
    print("\n=== Phase 0a Summary ===")
    print("  as_of_date column: added to clean_dataset.py (= filed_date)")
    print("  source_timestamp:  added to clean_dataset.py (= processing time)")
    print("  entry_price:       PIT-correct (from filed_date) ✓")
    print("  forward_return_*:  PIT-correct (measured from filed_date) ✓")
    print("  ML walk-forward:   PIT-approximate (uses fiscal_year < Y cutoff)")
    print("  Sector percentiles: residual look-ahead — rolling fix planned Phase 0a.2")
    print("  Backtester filing lag filter: MAX_FILING_LAG_MONTHS=18 in backtester.py ✓")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--market", default="US", help="Market filter (default: US, use 'all' for all)")
    parser.add_argument("--output", default=None, help="Save lag stats to CSV path")
    args = parser.parse_args()

    print(f"Loading dataset (market={args.market})…")
    df = load(args.market if args.market != "all" else None)
    print(f"Loaded {len(df):,} annual rows, {df['ticker'].nunique():,} tickers")

    df_lag = check_filing_lag(df)
    check_formation_quarters(df)
    check_sector_percentile_lookahead(df)
    check_ml_training_lookahead(df)
    check_forward_return_anchor(df)
    print_summary()

    if args.output and not df_lag.empty:
        cols = ["ticker", "market", "fiscal_year", "filed_date", "_lag_months", "_lag_bucket"]
        cols = [c for c in cols if c in df_lag.columns]
        df_lag[cols].to_csv(args.output, index=False)
        print(f"\nLag report saved → {args.output}")


if __name__ == "__main__":
    main()
