"""
Standalone feature selection pipeline.

Stages (per horizon):
  1. PSI filter  — removes features with PSI > PSI_THRESHOLD between
                   train (fiscal_year <= TRAIN_CUTOFF) and test (> VAL_END)
  2. IC screen   — keeps |mean_IC| >= IC_MIN_ABS and n_years >= N_YEARS_MIN
  3. ICIR rank   — sort by |ICIR|, keep top TOP_K_ICIR
  4. Spearman dedup — drop near-duplicates (|r| > CORR_THRESHOLD)

Outputs:
  models/feature_sets_{1y,3y,5y}.json   — selected feature list per horizon
  reports/feature_selection_summary.csv  — all candidates with IC/ICIR/PSI stats
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

BASE = Path(__file__).parent.parent
sys.path.insert(0, str(BASE))

# Reuse constants and helpers from train_models to keep logic in sync
from scripts.train_models import (
    EXCLUDE,
    EXCLUDE_PATTERNS,
    FORCE_INCLUDE_1Y,
    HORIZONS,
    TRAIN_CUTOFF,
    VAL_END,
    compute_ic_table,
    compute_psi,
    deduplicate_features,
    load_data,
)

# ── Tunable thresholds ─────────────────────────────────────────────────────────
PSI_THRESHOLD  = 0.25   # institutional standard: PSI > 0.25 indicates significant distribution shift
IC_MIN_ABS     = 0.02   # minimum |mean IC| to pass the IC screen
N_YEARS_MIN    = 5      # minimum number of years with enough data for IC computation
TOP_K_ICIR     = 60     # keep this many features after ICIR rank (before dedup)
CORR_THRESHOLD = 0.90   # Spearman |r| threshold for near-duplicate removal
MIN_FILL       = 0.10   # minimum non-null fraction to be a candidate at all
# ───────────────────────────────────────────────────────────────────────────────

MODELS_DIR = BASE / "models"
REPORTS    = BASE / "reports"
MODELS_DIR.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)


def newey_west_tstat(ic_series: pd.Series, max_lags: int = 4) -> float:
    """
    Newey-West HAC t-statistic for mean IC.

    Standard IC t-stat assumes IID annual ICs, which ignores autocorrelation
    (IC persistence across fiscal years). Newey-West corrects for this.
    max_lags: typically floor(4 * (T/100)^(2/9)) — default 4 handles 10-30 year series.

    Reference: Newey & West (1987) "A Simple, Positive Semi-Definite, Heteroskedasticity
    and Autocorrelation Consistent Covariance Matrix." Econometrica, 55(3), 703-708.
    """
    x = ic_series.dropna().values
    n = len(x)
    if n < 3:
        return np.nan
    mu = x.mean()
    gamma0 = np.mean((x - mu) ** 2)
    hac_var = gamma0
    for lag in range(1, min(max_lags + 1, n)):
        cov = np.mean((x[lag:] - mu) * (x[:-lag] - mu))
        hac_var += 2 * (1 - lag / (max_lags + 1)) * cov  # Bartlett kernel
    hac_var = max(hac_var, 1e-12)
    return mu / np.sqrt(hac_var / n)


def bh_fdr_correction(pvalues: pd.Series, alpha: float = 0.05) -> pd.Series:
    """
    Benjamini-Hochberg FDR correction — returns boolean Series (True = reject H0).

    Accounts for multiple comparisons: with 300 candidate features, ~15 are
    expected to pass p < 0.05 purely by chance. BH controls FDR at level alpha.

    Reference: Benjamini & Hochberg (1995) "Controlling the False Discovery Rate."
    JRSS-B, 57(1), 289-300.
    """
    n = len(pvalues)
    if n == 0:
        return pd.Series(dtype=bool)
    sorted_idx = pvalues.argsort()
    sorted_pvals = pvalues.iloc[sorted_idx].values
    bh_thresholds = np.arange(1, n + 1) / n * alpha
    reject_sorted = sorted_pvals <= bh_thresholds
    # Keep all rejections up to the largest k where p_(k) <= k/m * alpha
    if reject_sorted.any():
        last_reject = np.where(reject_sorted)[0].max()
        reject_sorted[:last_reject + 1] = True
    result = pd.Series(False, index=pvalues.index)
    result.iloc[sorted_idx] = reject_sorted
    return result


def get_candidates(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if c not in EXCLUDE
        and not any(p in c for p in EXCLUDE_PATTERNS)
        and df[c].dtype in [np.float64, np.float32, np.int64, np.int32, "Int64"]
        and df[c].notna().mean() > MIN_FILL
    ]


def psi_filter(
    df_train: pd.DataFrame,
    df_test: pd.DataFrame,
    candidates: list[str],
    psi_thr: float = PSI_THRESHOLD,
) -> tuple[list[str], pd.DataFrame]:
    records = []
    for feat in candidates:
        psi = compute_psi(df_train[feat], df_test[feat])
        records.append({"feature": feat, "psi": round(float(psi), 6)})
    psi_df = pd.DataFrame(records).sort_values("psi", ascending=False)
    passed = psi_df.loc[psi_df["psi"] <= psi_thr, "feature"].tolist()
    n_removed = len(candidates) - len(passed)
    print(f"    PSI filter: {len(candidates)} → {len(passed)} ({n_removed} removed, PSI > {psi_thr})")
    return passed, psi_df


def ic_icir_filter(
    df_train: pd.DataFrame,
    features: list[str],
    return_col: str,
    ic_min: float = IC_MIN_ABS,
    top_k: int = TOP_K_ICIR,
    sector_neutral: bool = True,
) -> tuple[list[str], pd.DataFrame]:
    """
    IC/ICIR filter with Newey-West t-stats and BH FDR gating.

    sector_neutral=True: demeans return and feature within each (fiscal_year, sector)
    group before computing IC. Removes sector-driven IC inflation.
    Reference: standard practice at AQR, Two Sigma — sector neutralization
    prevents macro sector bets from masquerading as stock-selection alpha.
    """
    def _compute_yearly_ic(df: pd.DataFrame, feat: str, ret_col: str) -> pd.Series:
        """Compute IC per year, optionally sector-neutral."""
        def _sic_to_sector(sic: pd.Series) -> pd.Series:
            s = pd.to_numeric(sic, errors="coerce").fillna(0).astype(int)
            sector = pd.Series("Other", index=s.index)
            sector[s.between(100,  999)]  = "Agriculture/Mining"
            sector[s.between(1000, 1499)] = "Mining/Resources"
            sector[s.between(1500, 1999)] = "Construction"
            sector[s.between(2000, 3999)] = "Manufacturing"
            sector[s.between(4000, 4999)] = "Utilities/Transport"
            sector[s.between(5000, 5999)] = "Trade"
            sector[s.between(6000, 6799)] = "Finance/Insurance/RE"
            sector[s.between(7000, 7999)] = "Services/Hospitality"
            sector[s.between(8000, 8999)] = "Services/Professional"
            return sector

        sic_col = "sic_sector" if "sic_sector" in df.columns else (
                  "sic_code"   if "sic_code"   in df.columns else None)

        ics = {}
        for yr, grp in df.groupby("fiscal_year"):
            sub = grp[[feat, ret_col]].dropna()
            if len(sub) < 6:
                continue
            if sector_neutral and sic_col is not None:
                sub = sub.copy()
                sectors = _sic_to_sector(grp.loc[sub.index, sic_col])
                for col in [feat, ret_col]:
                    demeaned = sub[col].copy().astype(float)
                    for sec in sectors.unique():
                        mask = (sectors == sec) & sub[col].notna()
                        if mask.sum() >= 5:
                            demeaned[mask] -= sub.loc[mask, col].median()
                    sub[col] = demeaned
            ics[yr] = sub[feat].corr(sub[ret_col], method="spearman")
        return pd.Series(ics)

    ic_tbl = compute_ic_table(df_train, features, return_col)

    # ── Newey-West HAC t-statistics for each feature's IC time series ──
    nw_tstats = {}
    nw_pvalues = {}
    for feat in ic_tbl.index:
        yearly_ic = _compute_yearly_ic(df_train, feat, return_col)
        t = newey_west_tstat(yearly_ic)
        nw_tstats[feat] = t
        # two-tailed p-value from t-distribution (df = n_years - 1)
        n_obs = yearly_ic.notna().sum()
        if not np.isnan(t) and n_obs > 2:
            from scipy.stats import t as t_dist
            nw_pvalues[feat] = float(2 * t_dist.sf(abs(t), df=n_obs - 1))
        else:
            nw_pvalues[feat] = np.nan

    ic_tbl["ic_tstat_nw"]  = pd.Series(nw_tstats)
    ic_tbl["ic_pval_nw"]   = pd.Series(nw_pvalues)

    # ── Benjamini-Hochberg FDR correction ──
    valid_pvals = ic_tbl["ic_pval_nw"].dropna()
    if len(valid_pvals) > 0:
        fdr_reject = bh_fdr_correction(valid_pvals, alpha=0.05)
        ic_tbl["fdr_reject"] = fdr_reject.reindex(ic_tbl.index).fillna(False)
    else:
        ic_tbl["fdr_reject"] = False

    n_fdr = ic_tbl["fdr_reject"].sum()
    print(f"    Newey-West t-stats computed; BH FDR rejects {n_fdr}/{len(ic_tbl)} features")

    ic_pass = ic_tbl[
        (ic_tbl["mean_ic"].abs() >= ic_min) &
        (ic_tbl["n_years"] >= N_YEARS_MIN) &
        (ic_tbl["fdr_reject"].fillna(False))  # BH FDR gate: only keep features where H0 rejected
    ]
    print(f"    IC+FDR screen (|mean_IC| >= {ic_min}, BH FDR q<0.05): {len(ic_tbl)} → {len(ic_pass)}")

    ranked = ic_pass.sort_values("icir", key=abs, ascending=False)
    topk   = ranked.head(top_k)
    print(f"    ICIR top-{top_k}: {len(ic_pass)} → {len(topk)}")

    return topk.index.tolist(), ic_tbl


def run_selection(
    df: pd.DataFrame,
    horizon: str,
    force_include: list[str],
    psi_thr: float = PSI_THRESHOLD,
    ic_min: float  = IC_MIN_ABS,
    top_k: int     = TOP_K_ICIR,
    corr_thr: float = CORR_THRESHOLD,
    sector_neutral: bool = True,
) -> dict:
    ret_col, beat_col = HORIZONS[horizon]

    df_train = df[df["fiscal_year"] <= TRAIN_CUTOFF].copy()
    df_test  = df[df["fiscal_year"]  > VAL_END].copy()

    candidates = get_candidates(df)
    print(f"\n  [{horizon}] {len(candidates)} candidates")

    # Stage 1 — PSI filter
    psi_pass, psi_df = psi_filter(df_train, df_test, candidates, psi_thr)

    # Stage 2+3 — IC screen + ICIR rank (on train split only)
    df_train_ret = df_train[df_train[ret_col].notna()]
    if len(df_train_ret) < 200:
        print(f"    WARNING: only {len(df_train_ret)} train rows with {ret_col} — skipping horizon")
        return {}

    ic_pass, ic_tbl = ic_icir_filter(df_train_ret, psi_pass, ret_col, ic_min, top_k,
                                      sector_neutral=sector_neutral)

    # Force-include (only features that survived PSI and exist in data)
    for fi in force_include:
        if fi in df.columns and fi not in ic_pass and fi in psi_pass:
            ic_pass.append(fi)
            print(f"    Force-include: {fi}")

    # Stage 4 — Spearman dedup (on full train split)
    final = deduplicate_features(df_train, ic_pass, corr_threshold=corr_thr)

    print(f"  [{horizon}] FINAL: {len(final)} features selected")

    # Merge IC stats with PSI for the summary report
    merged = ic_tbl.reset_index().merge(
        psi_df.rename(columns={"psi": "psi_train_vs_test"}),
        on="feature", how="left",
    )
    merged["horizon"]   = horizon
    merged["selected"]  = merged["feature"].isin(final)

    return {
        "features": final,
        "n":        len(final),
        "horizon":  horizon,
        "generated": datetime.now(timezone.utc).isoformat(),
        "_ic_summary": merged,  # kept in-memory for combined CSV only
    }


def main():
    parser = argparse.ArgumentParser(description="Run feature selection across all horizons")
    parser.add_argument("--psi-threshold", type=float, default=PSI_THRESHOLD)
    parser.add_argument("--ic-min",        type=float, default=IC_MIN_ABS)
    parser.add_argument("--top-k",         type=int,   default=TOP_K_ICIR)
    parser.add_argument("--corr",          type=float, default=CORR_THRESHOLD)
    parser.add_argument("--sector-neutral", dest="sector_neutral", action="store_true",  default=True,
                        help="Demean return and feature within sector before IC (default: on)")
    parser.add_argument("--no-sector-neutral", dest="sector_neutral", action="store_false",
                        help="Disable sector-neutral IC")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print stats but do not write JSON files")
    args = parser.parse_args()

    # Build effective thresholds (may override module-level defaults)
    psi_thr  = args.psi_threshold
    ic_min   = args.ic_min
    top_k    = args.top_k
    corr_thr = args.corr
    sn       = args.sector_neutral

    print("Loading data …")
    df = load_data()
    print(f"  {len(df):,} annual rows · {df.shape[1]} columns")
    print(f"  Sector-neutral IC: {sn}")

    all_summaries = []
    results = {}

    for horizon in HORIZONS:
        force = FORCE_INCLUDE_1Y if horizon == "1y" else []
        result = run_selection(df, horizon, force,
                               psi_thr=psi_thr, ic_min=ic_min,
                               top_k=top_k, corr_thr=corr_thr,
                               sector_neutral=sn)
        if not result:
            continue
        results[horizon] = result
        all_summaries.append(result.pop("_ic_summary"))

    # Write JSON feature sets
    if not args.dry_run:
        for horizon, res in results.items():
            out = MODELS_DIR / f"feature_sets_{horizon}.json"
            with open(out, "w") as f:
                json.dump(res, f, indent=2)
            print(f"\n  Wrote {out}  ({res['n']} features)")

        # Combined summary CSV
        summary_df = pd.concat(all_summaries, ignore_index=True)
        summary_path = REPORTS / "feature_selection_summary.csv"
        summary_df.to_csv(summary_path, index=False)
        print(f"\n  Wrote {summary_path}  ({len(summary_df)} rows)")
    else:
        print("\n[dry-run] no files written")

    print("\nDone.")


if __name__ == "__main__":
    main()
