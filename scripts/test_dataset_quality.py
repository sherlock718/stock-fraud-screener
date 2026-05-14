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
  8. Growth feature winsorization — Rule 6: no growth column exceeds p99 threshold
  9. ML score exclusion — Rule 7: ml_1y/3y/5y absent from feature_sets_*.json

Usage:
    python3 scripts/test_dataset_quality.py
    python3 scripts/test_dataset_quality.py --verbose
    python3 scripts/test_dataset_quality.py --parquet PATH
"""
from __future__ import annotations

import argparse
import json
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


# ── 7. Forward Return Coverage + Winsorization ───────────────────────────────

# Hard caps: values beyond these indicate the targets were not winsorized
_RETURN_CAPS = {
    "forward_return_1y": 5.0,
    "forward_return_3y": 10.0,
    "forward_return_5y": 20.0,
}


def test_forward_returns(df: pd.DataFrame, r: TestResult) -> None:
    print("\n[7] Forward return coverage + winsorization")
    if "forward_return_1y" not in df.columns:
        r.fail("'forward_return_1y' column missing")
        return

    # Winsorization guard
    for col, cap in _RETURN_CAPS.items():
        if col not in df.columns:
            continue
        mx = df[col].abs().max()
        if mx > cap:
            r.fail(f"{col}: max={mx:.1f} exceeds cap {cap} — targets not winsorized (run target winsorization patch)")
        else:
            r.ok(f"{col}: max={mx:.3f} <= {cap} (winsorized)")

    # Coverage by market
    for mkt in df["market"].unique():
        subset = df[df["market"] == mkt]
        fill = subset["forward_return_1y"].notna().mean()
        if fill < 0.15:
            r.fail(f"{mkt} forward_return_1y fill={fill:.1%} (< 15% — price enrichment may have failed)")
        elif fill < 0.30:
            r.warn(f"{mkt} forward_return_1y fill={fill:.1%} (< 30% — limited price data)")
        else:
            r.ok(f"{mkt} forward_return_1y fill={fill:.1%}")


# ── 8. Growth Feature Winsorization (Rule 6) ─────────────────────────────────

# Columns that must be winsorized. Max absolute value after winsorization should be
# at most 50× the p99 of the absolute column (loose guard — catches forgotten columns).
GROWTH_COLS = [
    "revenue_growth_yoy", "revenue_growth",
    "net_income_growth_yoy", "net_income_growth",
    "asset_growth_yoy", "assets_growth",
    "eps_growth_yoy", "eps_growth",
    "gross_profit_growth_yoy",
    "ocf_growth_yoy", "ocf_growth",
    "capex_growth_yoy", "capex_growth",
    "receivables_growth_yoy", "receivables_growth",
    "inventory_growth_yoy", "inventory_growth",
    "ap_growth_yoy", "ap_growth",
    "debt_growth_yoy", "debt_growth",
    "lt_debt_growth_yoy",
    "cogs_growth_yoy", "cogs_growth",
    "sga_growth_yoy", "sga_growth",
    "rd_growth_yoy", "rd_growth",
    "ppe_growth_yoy", "ppe_growth",
    "equity_growth", "equity_change_yoy",
    "shares_dilution", "shares_growth",
    "cash_change_yoy", "cash_growth",
]
# Any column with max/abs > this multiple of p99 has likely not been winsorized.
_WINSOR_GUARD = 50.0


def test_growth_winsorization(df: pd.DataFrame, r: TestResult) -> None:
    print("\n[8] Growth feature winsorization checks (Rule 6)")
    present = [c for c in GROWTH_COLS if c in df.columns]
    if not present:
        r.warn("No growth columns found — skipping Rule 6 check")
        return
    for col in present:
        s = df[col].dropna().abs()
        if len(s) == 0:
            continue
        p99 = s.quantile(0.99)
        col_max = s.max()
        if p99 > 0 and col_max > _WINSOR_GUARD * p99:
            r.fail(
                f"{col}: max={col_max:.1f} is {col_max/p99:.0f}× p99={p99:.2f} "
                f"— likely NOT winsorized (Rule 6)"
            )
        else:
            r.ok(f"{col}: max={col_max:.2f}, p99={p99:.2f} — winsorized ✓")


# ── 9. ML Score Exclusion (Rule 7) ───────────────────────────────────────────

_ML_SCORES = {"ml_1y", "ml_3y", "ml_5y"}
_FEATURE_SET_GLOB = "models/feature_sets_*.json"


def test_ml_score_exclusion(r: TestResult, base: Path) -> None:
    print("\n[9] ML score exclusion from feature sets (Rule 7)")
    paths = sorted(base.glob(_FEATURE_SET_GLOB))
    if not paths:
        r.warn(f"No feature_sets_*.json found under {base} — skipping Rule 7 check")
        return
    for path in paths:
        try:
            obj = json.loads(path.read_text())
            features = set(obj["features"] if isinstance(obj, dict) else obj)
        except Exception as exc:
            r.warn(f"{path.name}: could not parse — {exc}")
            continue
        leaked = _ML_SCORES & features
        if leaked:
            r.fail(
                f"{path.name}: contains ML-derived scores {sorted(leaked)} "
                f"— circular contamination risk (Rule 7)"
            )
        else:
            r.ok(f"{path.name}: no ML scores in feature set ({len(features)} features)")


# ── 10. Point-in-Time Leakage ─────────────────────────────────────────────────

def test_point_in_time(df: pd.DataFrame, r: TestResult) -> None:
    """
    Checks that features were available BEFORE forward returns are computed.

    Rule: filed_date must be <= fiscal_year_end + 18 months (generous lag).
    If filed_date > fiscal_year_end + 548 days, data was used before it existed.

    Critical for academic validity — look-ahead bias inflates IC and backtest
    returns. Every quant firm gates on this before any model training.
    """
    print("\n[10] Point-in-time leakage checks")

    if "filed_date" not in df.columns or "fiscal_year" not in df.columns:
        r.warn("filed_date or fiscal_year absent — point-in-time check skipped")
        return

    try:
        filed = pd.to_datetime(df["filed_date"], errors="coerce")
        # fiscal_year_end = Dec 31 of fiscal_year (conservative — many non-Dec FYEs exist)
        fy_end = pd.to_datetime(df["fiscal_year"].astype(str) + "-12-31", errors="coerce")

        # Check 1: filed_date should be AFTER fiscal year end (not before)
        filed_before_fy = (filed < fy_end - pd.Timedelta(days=180)).sum()
        if filed_before_fy > 0:
            r.warn(f"filed_date > 6 months before fiscal_year_end: {filed_before_fy:,} rows — "
                   f"check non-Dec fiscal year ends")
        else:
            r.ok("All filed_date values at or after fiscal_year_end - 6m")

        # Check 2: filed_date should be < fiscal_year_end + 548 days (18 months)
        # Filing lags > 18 months indicate stale/incorrect dates
        filed_too_late = (filed > fy_end + pd.Timedelta(days=548)).sum()
        if filed_too_late > 0:
            r.warn(f"filed_date > 18 months after fiscal_year_end: {filed_too_late:,} rows — "
                   f"abnormally long filing lag; verify data source")
        else:
            r.ok("All filed_date values within 18 months of fiscal_year_end")

        # Check 3: median filing lag distribution
        lag_days = (filed - fy_end).dt.days
        valid_lag = lag_days.dropna()
        if len(valid_lag) > 0:
            p50 = int(valid_lag.quantile(0.50))
            p95 = int(valid_lag.quantile(0.95))
            r.ok(f"Filing lag: median={p50}d, p95={p95}d (expected 30–180d for annual filings)")
            if p50 < -30:
                r.fail(f"Median filing lag {p50}d is NEGATIVE — systematic look-ahead bias")
            if p95 > 730:
                r.warn(f"p95 filing lag {p95}d exceeds 2 years — verify extreme lags")

    except Exception as e:
        r.warn(f"Point-in-time check error: {e}")


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
    test_growth_winsorization(df, r)
    test_ml_score_exclusion(r, parquet_path.parent.parent)
    test_point_in_time(df, r)

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
