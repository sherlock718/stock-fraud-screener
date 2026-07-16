"""
Step 6 — Clean, enrich, and validate the final dataset.

Pipeline:
  1. Structural cleaning (required cols, date filter, dedup, inf→NaN)
  2. Quality fixes (drop dead columns, forecast flag, winsorize accruals, fix gross_margin)
  3. Imputation (quarterly features join, size_category from log_assets)
  4. Survivorship correction (flag likely-delisted, impute pessimistic returns)
  5. Data confidence score (coverage × consistency × timeliness)

Output: data/historical_dataset_clean.parquet
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.event_time_cohorts import (
    attach_contract_provenance,
    event_time_rank,
    winsorize_accruals_event_time,
)

BASE = Path(__file__).parent.parent
DATA = BASE / "data"
IN = DATA / "historical_dataset.parquet"
OUT = DATA / "historical_dataset_clean.parquet"

REQUIRED_COLS = ["cik", "ticker", "filed_date", "fiscal_year", "period_type"]

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Structural Cleaning
# ═══════════════════════════════════════════════════════════════════════════════

def run_structural_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Core structural filters: required cols, date, dedup, inf→NaN."""
    n_raw = len(df)

    # Required columns present
    before = len(df)
    df = df.dropna(subset=REQUIRED_COLS)
    n_dropped = before - len(df)
    if n_dropped:
        print(f"  Dropped {n_dropped:,} rows: missing required columns")

    # Valid filed_date (after 2008-01-01)
    before = len(df)
    df["filed_date"] = pd.to_datetime(df["filed_date"], errors="coerce")
    df = df[df["filed_date"].notna() & (df["filed_date"] >= "2008-01-01")]
    n_dropped = before - len(df)
    if n_dropped:
        print(f"  Dropped {n_dropped:,} rows: invalid or pre-2008 filing date")

    # Remove duplicates
    before = len(df)
    dedup_key = (["stable_row_id"] if "stable_row_id" in df.columns else
                 (["entity_id", "fiscal_year", "period_type"]
                  if "entity_id" in df.columns else
                  (["cik", "market", "filed_date", "period_type"]
                   if "market" in df.columns else ["cik", "filed_date", "period_type"])))
    if df.duplicated(dedup_key, keep=False).any() and "stable_row_id" in df.columns:
        raise ValueError("duplicate stable_row_id encountered during Step 6")
    df = df.drop_duplicates(subset=dedup_key, keep="first")
    n_dropped = before - len(df)
    if n_dropped:
        print(f"  Dropped {n_dropped:,} duplicate rows")

    # Replace infinities with NaN
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    inf_mask = df[numeric_cols].isin([np.inf, -np.inf])
    n_inf = inf_mask.sum().sum()
    if n_inf:
        df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)
        print(f"  Replaced {n_inf:,} infinite values with NaN")

    # Point-in-Time columns
    df["as_of_date"] = df["filed_date"]
    fy_end = pd.to_datetime(df["fiscal_year"].astype(str) + "-12-31", errors="coerce")
    df["filing_lag_days"] = (df["filed_date"] - fy_end).dt.days

    # Sort
    entity_key = "entity_id" if "entity_id" in df.columns else "ticker"
    df = df.sort_values([entity_key, "filed_date", "period_type"]).reset_index(drop=True)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Quality Fixes
# ═══════════════════════════════════════════════════════════════════════════════

FORECAST_YEAR = 2025

KNOWN_NULL_COLUMNS = [
    "roic", "ppe", "total_equity", "roe_sector_pct", "pb_ratio",
    "pb_ratio_sector_pct", "acc_mt", "corp_code", "earnings_stability_5yr",
    "book_to_market",
]


def fix_null_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Drop columns with 100% null rate (or >95% null for known-dead cols)."""
    null_rates = df.isnull().mean()
    dead_cols = null_rates[null_rates == 1.0].index.tolist()

    for c in KNOWN_NULL_COLUMNS:
        if c in df.columns and c not in dead_cols:
            if df[c].isnull().mean() > 0.95:
                dead_cols.append(c)

    dead_cols = sorted(set(dead_cols))
    if dead_cols:
        df = df.drop(columns=dead_cols)
        print(f"  Dropped {len(dead_cols)} dead columns: {dead_cols}")
    return df, dead_cols


def add_forecast_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Add boolean is_forecast column (fiscal_year >= FORECAST_YEAR)."""
    df["is_forecast"] = df["fiscal_year"].ge(FORECAST_YEAR)
    n_forecast = df["is_forecast"].sum()
    print(f"  Added is_forecast flag: {n_forecast:,} rows (fiscal_year >= {FORECAST_YEAR})")
    return df


def winsorize_accruals(df: pd.DataFrame) -> pd.DataFrame:
    """Winsorize accruals under the accepted filing-time fallback contract."""
    col = "accruals_to_assets"
    if col not in df.columns:
        return df

    df = df.copy()
    before = pd.to_numeric(df[col], errors="coerce")
    df[col], df["accruals_winsorization_method"] = winsorize_accruals_event_time(df, col)
    n_changed = int((before.notna() & df[col].notna() & before.ne(df[col])).sum())

    print(f"  Winsorized accruals_to_assets: {n_changed:,} values clipped")
    return df


def fix_gross_margin(df: pd.DataFrame) -> pd.DataFrame:
    """Fix gross_margin values stored as percentage (>1.5) instead of decimal."""
    if "gross_margin" not in df.columns:
        return df
    df = df.copy()
    mask = df["gross_margin"] > 1.5
    n_fixed = mask.sum()
    if n_fixed > 0:
        df.loc[mask, "gross_margin"] = df.loc[mask, "gross_margin"] / 100.0
        print(f"  Fixed gross_margin > 1.5: {n_fixed:,} values divided by 100")
    return df


def run_quality_fix(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all quality fixes in sequence."""
    print("  Quality: dropping dead columns...")
    df, _ = fix_null_columns(df)
    print("  Quality: adding forecast flag...")
    df = add_forecast_flag(df)
    print("  Quality: winsorizing accruals...")
    df = winsorize_accruals(df)
    print("  Quality: fixing gross_margin...")
    df = fix_gross_margin(df)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Imputation
# ═══════════════════════════════════════════════════════════════════════════════

SIZE_FLAG_COL = "size_category_imputed"


def _impute_size_category(df: pd.DataFrame) -> pd.DataFrame:
    """Impute size_category from log_assets percentile rank within (fiscal_year, market)."""
    if "size_category" not in df.columns or "log_assets" not in df.columns:
        df[SIZE_FLAG_COL] = False
        return df

    df = df.copy()
    null_mask = df["size_category"].isna() & df["log_assets"].notna()

    if null_mask.sum() == 0:
        df[SIZE_FLAG_COL] = False
        return df

    ranks = event_time_rank(
        df,
        "log_assets",
        group_cols=["fiscal_year", "market"],
        min_count=20,
    )
    imputed = pd.cut(
        ranks.loc[null_mask],
        bins=[0, 0.25, 0.5, 0.75, 1.0],
        labels=[0, 1, 2, 3],
        include_lowest=True,
    ).astype("float32")

    df.loc[null_mask, "size_category"] = imputed
    df[SIZE_FLAG_COL] = null_mask & df["size_category"].notna()
    df["size_category_imputation_method"] = pd.Series(pd.NA, index=df.index, dtype="string")
    df.loc[null_mask & ranks.notna(), "size_category_imputation_method"] = (
        "eligible_fiscal_market_cohort"
    )
    df.loc[null_mask & ranks.isna(), "size_category_imputation_method"] = "sparse_or_unproven"
    return df


def run_impute(df: pd.DataFrame, source_path: Path | None = None) -> pd.DataFrame:
    """Impute quarterly features and size_category."""
    # Quarterly features — import here to avoid circular imports at module level
    try:
        from pipeline.enrich_quarterly_features import (
            compute_quarterly_features,
            NEW_FEATURES as QUARTERLY_COLS,
        )

        src_path = source_path or IN
        if src_path.exists():
            print("  Impute: computing quarterly features...")
            df_src = pd.read_parquet(src_path)
            q_feats = compute_quarterly_features(df_src)

            existing = [c for c in QUARTERLY_COLS if c in df.columns]
            if existing:
                df = df.drop(columns=existing)

            df = df.merge(q_feats, on=["ticker", "fiscal_year"], how="left")
            n_enriched = df[QUARTERLY_COLS[0]].notna().sum()
            print(f"  Impute: {n_enriched:,} rows enriched with quarterly features")
        else:
            print(f"  Impute: source {src_path} not found — skipping quarterly features")
    except ImportError:
        print("  Impute: enrich_quarterly_features not available — skipping")

    # Size category imputation
    print("  Impute: size_category from log_assets...")
    before_null = df["size_category"].isna().sum() if "size_category" in df.columns else 0
    df = _impute_size_category(df)
    after_null = df["size_category"].isna().sum() if "size_category" in df.columns else 0
    n_imputed = before_null - after_null
    print(f"  Impute: size_category null {before_null:,} → {after_null:,} ({n_imputed:,} imputed)")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Survivorship Correction
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_LAG = 3
DELISTING_RETURN = -0.50
SURVIVORSHIP_HORIZON_DAYS = {
    "6m": 183,
    "1y": 365,
    "2y": 730,
    "3y": 1095,
    "5y": 1825,
}


def run_survivorship(df: pd.DataFrame, lag: int = DEFAULT_LAG) -> pd.DataFrame:
    """Flag likely-delisted tickers and impute pessimistic forward returns."""
    ann = df[df["period_type"] == "annual"] if "period_type" in df.columns else df

    max_year = int(ann["fiscal_year"].max())
    last_year_per_ticker = ann.groupby("ticker")["fiscal_year"].max().rename("last_filing_year")

    delisted_tickers = last_year_per_ticker[
        last_year_per_ticker <= (max_year - lag)
    ].index

    df["likely_delisted"] = df["ticker"].isin(delisted_tickers)
    n_delisted = df["likely_delisted"].sum()
    print(f"  Survivorship: {len(delisted_tickers):,} likely-delisted tickers, "
          f"{n_delisted:,} rows flagged")

    last_rows = (
        df[df["likely_delisted"] & (df.get("period_type", "annual") == "annual")]
        .sort_values("fiscal_year")
        .drop_duplicates("ticker", keep="last")
        .index
    )

    def _policy_available(index: pd.Index, horizon: str) -> pd.Series:
        filed = pd.to_datetime(df.loc[index, "filed_date"], errors="coerce")
        detection_date = filed + pd.DateOffset(years=lag)
        horizon_date = filed + pd.to_timedelta(
            SURVIVORSHIP_HORIZON_DAYS[horizon], unit="D"
        )
        return pd.concat(
            [detection_date.rename("detection"), horizon_date.rename("horizon")],
            axis=1,
        ).max(axis=1)

    # Impute pessimistic forward returns on final filing row. These rows remain
    # in the universe, but are a separate sensitivity population for training.
    imputed = 0
    for h in SURVIVORSHIP_HORIZON_DAYS:
        col = f"forward_return_{h}"
        if col not in df.columns:
            continue
        missing_return = df.loc[last_rows, col].isna()
        fill_idx = last_rows[missing_return]
        df.loc[fill_idx, col] = DELISTING_RETURN
        if len(fill_idx):
            stock_policy_available = _policy_available(fill_idx, h)
            benchmark_end = pd.to_datetime(
                df.loc[fill_idx, f"benchmark_label_end_date_{h}"], errors="coerce"
            ) if f"benchmark_label_end_date_{h}" in df.columns else pd.Series(
                pd.NaT, index=fill_idx, dtype="datetime64[ns]"
            )
            relative_policy_available = pd.concat(
                [stock_policy_available.rename("policy"), benchmark_end.rename("benchmark")],
                axis=1,
            ).max(axis=1)
            df.loc[fill_idx, f"stock_label_end_date_{h}"] = pd.NaT
            df.loc[fill_idx, f"label_end_date_{h}"] = pd.NaT
            df.loc[fill_idx, f"policy_stock_label_available_date_{h}"] = stock_policy_available
            df.loc[fill_idx, f"policy_label_available_date_{h}"] = relative_policy_available
            df.loc[fill_idx, f"stock_label_provenance_{h}"] = "policy_imputed_likely_delisted"
            df.loc[fill_idx, f"label_provenance_{h}"] = "policy_imputed_likely_delisted"
            bench_col = f"benchmark_return_{h}"
            excess_col = f"excess_return_local_{h}"
            if bench_col in df.columns and excess_col in df.columns:
                relative_idx = fill_idx[df.loc[fill_idx, bench_col].notna()]
                df.loc[relative_idx, excess_col] = (
                    DELISTING_RETURN - df.loc[relative_idx, bench_col]
                )
        imputed += int(missing_return.sum())

    # Update beat_local_market labels
    for h in SURVIVORSHIP_HORIZON_DAYS:
        ret_col = f"forward_return_{h}"
        beat_col = f"beat_local_market_{h}"
        if ret_col not in df.columns or beat_col not in df.columns:
            continue
        mask = df["likely_delisted"] & df[beat_col].isna() & df[ret_col].notna()
        df.loc[mask, beat_col] = 0
        df.loc[mask, f"label_end_date_{h}"] = pd.NaT
        df.loc[mask, f"label_provenance_{h}"] = "policy_imputed_likely_delisted"
        mask_idx = df.index[mask]
        if len(mask_idx):
            relative_available = _policy_available(mask_idx, h)
            benchmark_end_col = f"benchmark_label_end_date_{h}"
            if benchmark_end_col in df.columns:
                benchmark_end = pd.to_datetime(
                    df.loc[mask_idx, benchmark_end_col], errors="coerce"
                )
                relative_available = pd.concat(
                    [relative_available.rename("policy"), benchmark_end.rename("benchmark")],
                    axis=1,
                ).max(axis=1)
            df.loc[mask_idx, f"policy_label_available_date_{h}"] = relative_available

    print(f"  Survivorship: {imputed:,} forward returns imputed at {DELISTING_RETURN:.0%}")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Data Confidence Score
# ═══════════════════════════════════════════════════════════════════════════════

COVERAGE_GROUPS: dict[str, list[str]] = {
    "financial_core": [
        "revenue", "net_income", "total_assets", "total_equity",
        "operating_cash_flow", "gross_profit", "operating_income",
    ],
    "fraud_signals": [
        "beneish_m_score", "altman_z_score", "piotroski_f_score", "sloan_accruals",
    ],
    "price_returns": [
        "entry_price", "forward_return_1y",
    ],
    "ratios": [
        "net_margin", "roe", "roa", "ocf_margin", "debt_to_equity", "current_ratio",
    ],
}


def coverage_score(df: pd.DataFrame) -> pd.Series:
    """Fraction of core analytical columns that are non-null."""
    all_core = []
    for cols in COVERAGE_GROUPS.values():
        all_core.extend([c for c in cols if c in df.columns])

    if not all_core:
        return pd.Series(0.5, index=df.index)

    present = df[all_core].notna().astype(float)
    return present.mean(axis=1)


def consistency_score(df: pd.DataFrame) -> pd.Series:
    """Internal accounting consistency checks."""
    checks = []

    rev = pd.to_numeric(df["revenue"], errors="coerce") if "revenue" in df.columns else None
    ta = pd.to_numeric(df["total_assets"], errors="coerce") if "total_assets" in df.columns else None
    te = pd.to_numeric(df["total_equity"], errors="coerce") if "total_equity" in df.columns else None
    ni = pd.to_numeric(df["net_income"], errors="coerce") if "net_income" in df.columns else None
    gp = pd.to_numeric(df["gross_profit"], errors="coerce") if "gross_profit" in df.columns else None
    ocf = pd.to_numeric(df["operating_cash_flow"], errors="coerce") if "operating_cash_flow" in df.columns else None

    if ta is not None:
        checks.append((ta > 0).fillna(False).astype(float))

    if ta is not None and te is not None:
        ok = (ta.notna() & te.notna() & (ta > 0) & (te.abs() <= ta * 1.5))
        ok = ok | ta.isna() | te.isna()
        checks.append(ok.astype(float))

    if rev is not None:
        checks.append((rev > 0).fillna(False).astype(float))

    if rev is not None and ni is not None:
        ok = (rev.notna() & ni.notna() & (rev > 0) & (ni.abs() < rev * 3))
        ok = ok | rev.isna() | ni.isna() | (rev <= 0)
        checks.append(ok.astype(float))

    if rev is not None and ocf is not None:
        has_rev = rev.notna() & (rev > 0)
        has_ocf = ocf.notna()
        ok = (~has_rev) | has_ocf
        checks.append(ok.astype(float))

    if gp is not None and ni is not None:
        ok = (gp.notna() & ni.notna() & (gp >= ni)) | gp.isna() | ni.isna()
        checks.append(ok.astype(float))

    if not checks:
        return pd.Series(0.5, index=df.index)

    stacked = pd.concat(checks, axis=1)
    return stacked.mean(axis=1)


def timeliness_score(df: pd.DataFrame) -> pd.Series:
    """Score based on filing lag and fiscal year vintage."""
    score = pd.Series(1.0, index=df.index)

    if "filing_lag_days" in df.columns:
        lag = pd.to_numeric(df["filing_lag_days"], errors="coerce").fillna(90)
        lag_score = pd.Series(1.0, index=df.index)
        mask_mid = (lag > 60) & (lag <= 180)
        mask_late = lag > 180
        lag_score[mask_mid] = 1.0 - 0.3 * (lag[mask_mid] - 60) / 120
        lag_score[mask_late] = 0.5
        lag_score[lag < 0] = 1.0
        score = score * lag_score

    if "fiscal_year" in df.columns:
        fy = pd.to_numeric(df["fiscal_year"], errors="coerce").fillna(2015)
        vintage = pd.Series(1.0, index=df.index)
        vintage[fy < 2012] = 0.7
        vintage[(fy >= 2012) & (fy < 2018)] = 0.85
        score = score * vintage

    return score.clip(0.0, 1.0)


def build_confidence(df: pd.DataFrame) -> pd.Series:
    """Composite confidence = mean(coverage, consistency, timeliness)."""
    cov = coverage_score(df)
    cons = consistency_score(df)
    time = timeliness_score(df)
    return ((cov + cons + time) / 3.0).clip(0.0, 1.0).round(4)


def run_confidence(df: pd.DataFrame) -> pd.DataFrame:
    """Compute and attach data_confidence score."""
    print("  Confidence: computing coverage, consistency, timeliness...")
    df["data_confidence"] = build_confidence(df)
    mean_conf = df["data_confidence"].mean()
    print(f"  Confidence: mean={mean_conf:.3f}")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# Main Pipeline
# ═══════════════════════════════════════════════════════════════════════════════

def run(
    source_path: Path | None = None,
    *,
    enable_imputation: bool = True,
    enable_survivorship_policy: bool = True,
) -> None:
    """Execute the full step 6 pipeline."""
    DATA.mkdir(exist_ok=True)
    print("Step 6 — Clean, enrich, and validate dataset")

    in_path = source_path or IN
    if not in_path.exists():
        print(f"ERROR: {in_path} not found — run step 5 first")
        sys.exit(1)

    df = pd.read_parquet(in_path)
    n_raw = len(df)
    print(f"  Raw dataset: {n_raw:,} rows × {len(df.columns)} columns")

    # 1. Structural cleaning
    print("\n── Structural Cleaning ──")
    df = run_structural_clean(df)

    # 2. Quality fixes
    print("\n── Quality Fixes ──")
    df = run_quality_fix(df)

    # 3. Imputation
    print("\n── Imputation ──")
    if enable_imputation:
        df = run_impute(df, source_path=in_path)
    else:
        df[SIZE_FLAG_COL] = False
        print("  Imputation disabled by fail-closed configuration")

    # 4. Survivorship correction
    print("\n── Survivorship Correction ──")
    if enable_survivorship_policy:
        df = run_survivorship(df)
    else:
        print("  Survivorship policy disabled; no inferred outcome or flag added")

    # 5. Confidence score
    print("\n── Data Confidence Score ──")
    df = run_confidence(df)

    df = attach_contract_provenance(df)

    # ── Save ──
    df.to_parquet(OUT, index=False)

    # ── Report ──
    n_clean = len(df)
    pct_kept = 100 * n_clean / n_raw if n_raw > 0 else 0
    n_tickers = df["ticker"].nunique()

    print(f"\nStep 6 complete.")
    print(f"  Raw rows:     {n_raw:,}")
    print(f"  Clean rows:   {n_clean:,} ({pct_kept:.1f}% kept)")
    print(f"  Tickers:      {n_tickers:,}")
    print(f"  Features:     {len(df.columns)}")
    print(f"  Saved: {OUT}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Step 6 — Clean, enrich, and validate")
    parser.add_argument("--snapshots", type=str, default=None, help="Unused; pipeline compat")
    parser.add_argument("--input", type=str, default=None, help="Step 5 input parquet")
    parser.add_argument("--out", type=str, default=None, help="Pre-taxonomy output parquet")
    parser.add_argument("--suffix", type=str, default="", help="Market suffix, e.g. _br")
    parser.add_argument("--skip-imputation", action="store_true",
                        help="Do not run quarterly or size-category imputations")
    parser.add_argument("--skip-survivorship-policy", action="store_true",
                        help="Do not infer likely-delisted flags or policy labels")
    args = parser.parse_args()

    sfx = args.suffix
    if sfx:
        IN = DATA / f"historical_dataset{sfx}.parquet"
        OUT = DATA / f"historical_dataset_clean{sfx}.parquet"
    if args.input:
        IN = Path(args.input)
    if args.out:
        OUT = Path(args.out)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    run(
        source_path=IN,
        enable_imputation=not args.skip_imputation,
        enable_survivorship_policy=not args.skip_survivorship_policy,
    )
