"""
Dataset quality test suite for historical_dataset_clean.parquet.

Checks:
  1. Schema — required columns present, correct dtypes
  2. Structural — no quarterly rows, no blank tickers, no duplicate primary keys
  3. Market coverage — minimum ticker count and year span per market
  4. Fill rates — core financial columns above minimum thresholds
  5. Distribution sanity — ratios within plausible ranges, no inf values
  6. Fraud label integrity — no label leakage, confirmed vs suspect consistency
  7. Forward return coverage — at least 30% fill per market

Usage:
    python3 scripts/test_dataset_quality.py
    python3 scripts/test_dataset_quality.py --verbose
    python3 scripts/test_dataset_quality.py --parquet PATH
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).parent.parent
DEFAULT_PATH = BASE / "data" / "historical_dataset_clean.parquet"

PASS = "  [PASS]"
FAIL = "  [FAIL]"
WARN = "  [WARN]"


class TestResult:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        self.messages: list[str] = []

    def ok(self, msg: str) -> None:
        self.passed += 1
        self.messages.append(f"{PASS} {msg}")

    def fail(self, msg: str) -> None:
        self.failed += 1
        self.messages.append(f"{FAIL} {msg}")

    def warn(self, msg: str) -> None:
        self.warnings += 1
        self.messages.append(f"{WARN} {msg}")

    def summary(self) -> str:
        total = self.passed + self.failed + self.warnings
        return (
            f"\n{'='*60}\n"
            f"Results: {self.passed} passed  {self.failed} failed  {self.warnings} warnings  ({total} checks)\n"
        )


# ── 1. Schema ────────────────────────────────────────────────────────────────

REQUIRED_COLS = [
    "ticker", "cik", "fiscal_year", "market", "period_type", "filed_date",
    "revenue", "net_income", "total_assets", "equity", "operating_cash_flow",
    "long_term_debt", "short_term_debt", "net_debt", "enterprise_value",
    "beneish_m_score", "piotroski_f_score", "altman_z_score",
    "fraud_score_accounting", "fraud_score_distress", "fraud_score_composite",
    "fraud_suspect",
    "momentum_12m_rank", "momentum_6m_rank", "momentum_composite_rank",
    "forward_return_1y",
    "as_of_date", "filing_lag_days",
    "in_universe",
    "sector",
]

FRAUD_SCORE_COLS = [
    "fraud_score_accounting", "fraud_score_dilution", "fraud_score_quality",
    "fraud_score_distress", "fraud_score_governance", "fraud_score_composite",
]


def test_schema(df: pd.DataFrame, r: TestResult) -> None:
    print("\n[1] Schema checks")
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        r.fail(f"Missing required columns: {missing}")
    else:
        r.ok(f"All {len(REQUIRED_COLS)} required columns present")

    if df["fiscal_year"].dtype not in [np.int64, np.int32, "int64", "int32", "Int64"]:
        r.warn(f"fiscal_year dtype is {df['fiscal_year'].dtype}, expected int")
    else:
        r.ok("fiscal_year is integer dtype")


# ── 2. Structural ─────────────────────────────────────────────────────────────

def test_structural(df: pd.DataFrame, r: TestResult) -> None:
    print("\n[2] Structural integrity checks")

    # Annual-only
    if "period_type" in df.columns:
        non_annual = (df["period_type"] != "annual").sum()
        if non_annual > 0:
            r.fail(f"Non-annual rows found: {non_annual:,} (period_type != 'annual')")
        else:
            r.ok("All rows are period_type='annual'")

    # Blank tickers
    if "ticker" in df.columns:
        blank = (df["ticker"] == "").sum() + df["ticker"].isna().sum()
        if blank > 0:
            r.fail(f"Blank/null ticker rows: {blank:,}")
        else:
            r.ok("No blank or null ticker values")

    # Duplicate primary keys — includes fiscal_year because DART bulk-filings share filed_date
    pk = ["cik", "market", "fiscal_year", "period_type"]
    available_pk = [c for c in pk if c in df.columns]
    if len(available_pk) >= 3:
        dupes = df.duplicated(subset=available_pk).sum()
        if dupes > 0:
            r.warn(f"Duplicate primary key rows: {dupes:,} ({available_pk})")
        else:
            r.ok(f"No duplicate primary keys ({', '.join(available_pk)})")

    # Date validity
    if "fiscal_year" in df.columns:
        bad_year = ((df["fiscal_year"] < 2008) | (df["fiscal_year"] > 2030)).sum()
        if bad_year > 0:
            r.warn(f"fiscal_year out of 2008–2030 range: {bad_year:,} rows")
        else:
            r.ok("All fiscal_year values in valid range (2008–2030)")

    # Inf values
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    inf_count = np.isinf(df[numeric_cols]).sum().sum()
    if inf_count > 0:
        r.fail(f"Inf values found: {inf_count:,} across numeric columns")
    else:
        r.ok("No inf values in numeric columns")


# ── 3. Market Coverage ────────────────────────────────────────────────────────

MIN_TICKERS = {"US": 500, "CA": 100, "KR": 50, "JP": 20, "BR": 20}
MIN_YEARS = {"US": 10, "CA": 3, "KR": 5, "JP": 3, "BR": 3}


def test_market_coverage(df: pd.DataFrame, r: TestResult) -> None:
    print("\n[3] Market coverage checks")
    if "market" not in df.columns:
        r.fail("'market' column missing — cannot check coverage")
        return

    for mkt, min_t in MIN_TICKERS.items():
        subset = df[df["market"] == mkt]
        if len(subset) == 0:
            r.warn(f"{mkt}: no rows found")
            continue
        n_tickers = subset["ticker"].nunique()
        yr_range = subset["fiscal_year"].max() - subset["fiscal_year"].min()
        if n_tickers < min_t:
            r.warn(f"{mkt}: only {n_tickers} tickers (minimum expected: {min_t})")
        else:
            r.ok(f"{mkt}: {n_tickers} tickers, {yr_range+1} years")

        min_yr = MIN_YEARS.get(mkt, 3)
        if yr_range < min_yr:
            r.warn(f"{mkt}: only {yr_range+1} year(s) of history (expected >= {min_yr+1})")


# ── 4. Fill Rates ─────────────────────────────────────────────────────────────

FILL_THRESHOLDS = {
    "revenue":             0.90,
    "net_income":          0.85,
    "total_assets":        0.90,
    "equity":              0.85,
    "operating_cash_flow": 0.70,
    "beneish_m_score":     0.50,
    "altman_z_score":      0.50,
    "piotroski_f_score":   0.50,
    "fraud_score_composite": 0.50,
    "forward_return_1y":   0.20,
}


def test_fill_rates(df: pd.DataFrame, r: TestResult) -> None:
    print("\n[4] Fill rate checks")
    for col, threshold in FILL_THRESHOLDS.items():
        if col not in df.columns:
            r.warn(f"Column '{col}' missing — skipped fill check")
            continue
        fill = df[col].notna().mean()
        if fill < threshold:
            r.fail(f"{col}: fill={fill:.1%} below threshold {threshold:.0%}")
        else:
            r.ok(f"{col}: fill={fill:.1%} (>= {threshold:.0%})")


# ── 5. Distribution Sanity ────────────────────────────────────────────────────

def test_distributions(df: pd.DataFrame, r: TestResult) -> None:
    print("\n[5] Distribution sanity checks")

    # Fraud scores must be 0–1
    for col in FRAUD_SCORE_COLS:
        if col not in df.columns:
            continue
        s = df[col].dropna()
        if (s < 0).any() or (s > 1).any():
            r.fail(f"{col}: values outside [0,1] range")
        else:
            r.ok(f"{col}: all values in [0,1]")

    # Piotroski F-score 0–9
    if "piotroski_f_score" in df.columns:
        pf = df["piotroski_f_score"].dropna()
        if (pf < 0).any() or (pf > 9).any():
            r.warn(f"piotroski_f_score: values outside [0,9] — check for outliers")
        else:
            r.ok("piotroski_f_score: values within [0,9]")

    # in_universe must be 0/1
    if "in_universe" in df.columns:
        bad = ~df["in_universe"].isin([0, 1, True, False])
        if bad.sum() > 0:
            r.fail(f"in_universe: {bad.sum():,} values not 0/1")
        else:
            r.ok("in_universe: all values are 0 or 1")

    # fiscal_year distribution — no extreme concentration
    if "fiscal_year" in df.columns:
        top_year = df["fiscal_year"].value_counts().iloc[0]
        top_pct = top_year / len(df)
        if top_pct > 0.25:
            r.warn(f"Top fiscal_year holds {top_pct:.0%} of rows — possible over-concentration")
        else:
            r.ok(f"fiscal_year well-distributed (top year = {top_pct:.0%})")


# ── 6. Fraud Label Integrity ──────────────────────────────────────────────────

def test_fraud_labels(df: pd.DataFrame, r: TestResult) -> None:
    print("\n[6] Fraud label integrity checks")

    if "fraud_confirmed" not in df.columns:
        r.warn("'fraud_confirmed' column absent — label leakage check skipped")
        return

    fc = df["fraud_confirmed"]
    r.ok(f"fraud_confirmed: {int(fc.sum()):,} positive rows ({fc.mean():.2%})")

    # fraud_confirmed rows should NOT have fraud_suspect=1 (per P0d logic)
    if "fraud_suspect" in df.columns:
        leak = ((fc == 1) & (df["fraud_suspect"] == 1)).sum()
        if leak > 0:
            r.fail(f"fraud_suspect=1 on {leak:,} confirmed fraud rows (should be 0)")
        else:
            r.ok("fraud_suspect correctly zeroed on fraud_confirmed=1 rows")

    # fraud_score_* should NOT equal fraud_confirmed (leakage if correlation too high)
    for col in FRAUD_SCORE_COLS:
        if col not in df.columns:
            continue
        corr = df[col].corr(fc)
        if abs(corr) > 0.80:
            r.fail(f"Possible leakage: {col} correlation with fraud_confirmed = {corr:.3f}")
        elif abs(corr) > 0.50:
            r.warn(f"High correlation: {col} vs fraud_confirmed = {corr:.3f}")
        else:
            r.ok(f"{col} vs fraud_confirmed corr = {corr:.3f} (no leakage)")


# ── 7. Forward Return Coverage ────────────────────────────────────────────────

def test_forward_returns(df: pd.DataFrame, r: TestResult) -> None:
    print("\n[7] Forward return coverage")
    if "forward_return_1y" not in df.columns:
        r.fail("'forward_return_1y' column missing")
        return

    for mkt in df["market"].unique():
        subset = df[df["market"] == mkt]
        fill = subset["forward_return_1y"].notna().mean()
        if fill < 0.15:
            r.fail(f"{mkt} forward_return_1y fill={fill:.1%} (< 15% — price enrichment may have failed)")
        elif fill < 0.30:
            r.warn(f"{mkt} forward_return_1y fill={fill:.1%} (< 30% — limited price data)")
        else:
            r.ok(f"{mkt} forward_return_1y fill={fill:.1%}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(parquet_path: Path, verbose: bool = False) -> int:
    if not parquet_path.exists():
        print(f"ERROR: {parquet_path} not found")
        return 1

    print(f"Dataset Quality Test Suite")
    print(f"Input: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    print(f"Shape: {len(df):,} rows × {len(df.columns)} columns\n")

    r = TestResult()
    test_schema(df, r)
    test_structural(df, r)
    test_market_coverage(df, r)
    test_fill_rates(df, r)
    test_distributions(df, r)
    test_fraud_labels(df, r)
    test_forward_returns(df, r)

    if verbose:
        print()
        for msg in r.messages:
            print(msg)
    else:
        print()
        for msg in r.messages:
            if "[FAIL]" in msg or "[WARN]" in msg:
                print(msg)

    print(r.summary())
    return 1 if r.failed > 0 else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Dataset quality test suite")
    parser.add_argument("--parquet", default=str(DEFAULT_PATH), help="Path to parquet file")
    parser.add_argument("--verbose", action="store_true", help="Print all checks (not just failures)")
    args = parser.parse_args()
    sys.exit(run(Path(args.parquet), verbose=args.verbose))


if __name__ == "__main__":
    main()
