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
PSI_THRESHOLD  = 2.0    # features with PSI above this are dropped (macro-regime drift)
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
) -> tuple[list[str], pd.DataFrame]:
    ic_tbl = compute_ic_table(df_train, features, return_col)

    ic_pass = ic_tbl[
        (ic_tbl["mean_ic"].abs() >= ic_min) &
        (ic_tbl["n_years"] >= N_YEARS_MIN)
    ]
    print(f"    IC screen (|mean_IC| >= {ic_min}): {len(ic_tbl)} → {len(ic_pass)}")

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

    ic_pass, ic_tbl = ic_icir_filter(df_train_ret, psi_pass, ret_col, ic_min, top_k)

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
    parser.add_argument("--dry-run", action="store_true",
                        help="Print stats but do not write JSON files")
    args = parser.parse_args()

    # Build effective thresholds (may override module-level defaults)
    psi_thr  = args.psi_threshold
    ic_min   = args.ic_min
    top_k    = args.top_k
    corr_thr = args.corr

    print("Loading data …")
    df = load_data()
    print(f"  {len(df):,} annual rows · {df.shape[1]} columns")

    all_summaries = []
    results = {}

    for horizon in HORIZONS:
        force = FORCE_INCLUDE_1Y if horizon == "1y" else []
        result = run_selection(df, horizon, force,
                               psi_thr=psi_thr, ic_min=ic_min,
                               top_k=top_k, corr_thr=corr_thr)
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
