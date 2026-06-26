"""
Session 22: Proper Train/Validate/Test split backtest.

Addresses look-ahead bias in feature selection:
  - Feature selection (ICIR ranking) confined to TRAIN period only
  - ML models trained walk-forward (expanding window) within test period
  - Final metrics reported on TEST period (never used for any decision)
  - Stability check: shifted train window, feature overlap %

Temporal split design:
  Train:    2008-2014  (7 years — feature selection via ICIR + BH FDR)
  Validate: 2015-2018  (4 years — PSI stability check, regime-diverse)
  Test:     2019-2024  (6 years — includes COVID crash, rate hikes, recovery)

Why this split:
  - 7 train years give enough IC observations for robust ICIR + BH FDR correction
  - Validation covers: flat (2015), value recovery (2016), melt-up (2017), vol shock (2018)
  - Test covers: late-cycle (2019), COVID crash+recovery (2020), speculative frenzy (2021),
    rate-hike bear (2022), AI rally (2023), broadening (2024)
  - Feature selection target (forward_return_3y): fiscal_year 2014 needs prices through 2017 ✓
  - Walk-forward ML in test: each year retrains on all prior data (expanding window)
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _root import ROOT
from modeling.run_feature_selection import (
    get_candidates,
    ic_icir_filter,
    psi_filter,
)
from modeling.train import (
    compute_psi,
    deduplicate_features,
    load_data,
)
from backtest.engine import (
    filter_composite,
    load_spy_returns,
    RISK_FREE,
    SLIPPAGE_TIERS,
    MAX_POSITION_WEIGHT,
    MAX_SECTOR_WEIGHT,
    _sic_to_sector,
    _apply_sector_cap,
    _apply_filing_lag_filter,
)

# ── Temporal split constants ─────────────────────────────────────────────────
TRAIN_START = 2008
TRAIN_END = 2014
VAL_START = 2015
VAL_END = 2018
TEST_START = 2019
TEST_END = 2024

# Stability check: shifted window (same length, shifted +2 years)
TRAIN_START_ALT = 2010
TRAIN_END_ALT = 2016


def select_features_on_period(
    df: pd.DataFrame,
    train_start: int,
    train_end: int,
    val_start: int,
    val_end: int,
    ret_col: str = "forward_return_3y",
) -> tuple[list[str], pd.DataFrame]:
    """Run feature selection confined to a specific train period."""
    df_train = df[df["fiscal_year"].between(train_start, train_end)].copy()
    df_val = df[df["fiscal_year"].between(val_start, val_end)].copy()

    candidates = get_candidates(df)
    print(f"  {len(candidates)} candidate features")

    # PSI filter: train vs validate (checks distribution stability)
    psi_pass, psi_df = psi_filter(df_train, df_val, candidates, psi_thr=0.25)

    # IC/ICIR filter on train period only
    df_train_ret = df_train[df_train[ret_col].notna()]
    print(f"  Train rows with {ret_col}: {len(df_train_ret)}")

    if len(df_train_ret) < 100:
        print(f"  WARNING: only {len(df_train_ret)} train rows — insufficient")
        return [], pd.DataFrame()

    ic_pass, ic_tbl = ic_icir_filter(
        df_train_ret, psi_pass, ret_col, ic_min=0.02, top_k=60, sector_neutral=True
    )

    # Dedup on train data
    final = deduplicate_features(df_train, ic_pass, corr_threshold=0.85)
    print(f"  FINAL: {len(final)} features selected on {train_start}-{train_end}")

    return final, ic_tbl


def walk_forward_backtest(
    df: pd.DataFrame,
    features: list[str],
    test_start: int,
    test_end: int,
    top_n: int = 20,
) -> dict:
    """Walk-forward backtest on test period using restricted feature set.

    For each year Y in [test_start, test_end]:
      - Train LightGBM on all data with fiscal_year < Y (expanding window)
      - Restrict to features selected on train period only
      - Score year Y, apply composite strategy, record return
    """
    spy_returns = load_spy_returns()
    years = list(range(test_start, test_end + 1))
    annual_rows = []

    # Walk-forward ML scoring with restricted features
    beat_col = "beat_local_market_1y"
    ret_col = "forward_return_1y"

    if beat_col not in df.columns or ret_col not in df.columns:
        return {"error": "Missing return columns"}

    # Pre-compute filed_date for PIT filtering
    filed = pd.to_datetime(df.get("filed_date", pd.NaT), errors="coerce")

    print(f"\n  Walk-forward ML on test period {test_start}-{test_end}...")
    print(f"  Using {len(features)} train-selected features")

    # Score all test-period years walk-forward
    ml_scores = np.full(len(df), np.nan)

    for yr in years:
        cutoff = pd.Timestamp(f"{yr}-01-01")
        pit_mask = filed.isna() | (filed < cutoff)
        train_df = df[(df["fiscal_year"] < yr) & df[beat_col].notna() & pit_mask].copy()

        if train_df["fiscal_year"].nunique() < 3:
            continue

        # Use _ic_rank equivalent but restricted to train-selected features
        avail_feats = [f for f in features if f in train_df.columns]
        # Further filter by IC on training data (walk-forward within the restricted pool)
        sub = train_df[train_df[ret_col].notna()]
        feat_ics = []
        for feat in avail_feats:
            sub2 = sub[sub[feat].notna()]
            ics = []
            for train_yr in sub2["fiscal_year"].unique():
                g = sub2[sub2["fiscal_year"] == train_yr]
                if len(g) < 30:
                    continue
                c, _ = stats.spearmanr(g[feat], g[ret_col])
                if not np.isnan(c):
                    ics.append(c)
            if len(ics) >= 3:
                mean_ic = np.mean(ics)
                std_ic = np.std(ics) + 1e-8
                feat_ics.append((feat, abs(mean_ic / std_ic)))

        feat_ics.sort(key=lambda x: x[1], reverse=True)
        feats = [f[0] for f in feat_ics[:35]]

        if len(feats) < 5:
            continue

        train_med = train_df[feats].median()
        X_train = train_df[feats].fillna(train_med)
        y_train = train_df[beat_col].astype(int)

        pos = int((y_train == 1).sum())
        neg = int((y_train == 0).sum())
        clf = lgb.LGBMClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            num_leaves=20,
            subsample=0.8,
            colsample_bytree=0.7,
            min_child_samples=30,
            scale_pos_weight=neg / max(pos, 1),
            random_state=42,
            n_jobs=-1,
            verbose=-1,
        )
        clf.fit(X_train, y_train)

        score_mask = (df["fiscal_year"] == yr).values
        if score_mask.sum() == 0:
            continue
        X_score = df.loc[score_mask, feats].fillna(train_med)
        ml_scores[score_mask] = clf.predict_proba(X_score)[:, 1]

    df = df.copy()
    df["ml_1y_wf"] = ml_scores

    # Now run composite strategy on test years
    print(f"  Running composite strategy on test period...")

    for yr in years:
        yr_df = df[df["fiscal_year"] == yr].copy()
        yr_df = _apply_filing_lag_filter(yr_df, yr, 18)

        # Market cap filter
        if "market_cap_at_filing" in yr_df.columns:
            yr_df = yr_df[yr_df["market_cap_at_filing"].fillna(0) >= 50_000_000]

        idx = filter_composite(yr_df, top_n, None)
        picks = yr_df.loc[idx]

        if "forward_return_1y" not in picks.columns:
            continue

        picks_valid = picks[picks["forward_return_1y"].notna()]
        rets = picks_valid["forward_return_1y"]
        if len(rets) < 3:
            continue

        # Tiered transaction costs
        if "market_cap_at_filing" in picks_valid.columns:
            caps = picks_valid["market_cap_at_filing"].fillna(0).values
            per_pick_cost = np.array(
                [
                    next(bps for threshold, bps in SLIPPAGE_TIERS if cap >= threshold)
                    / 10000
                    for cap in caps
                ]
            )
        else:
            per_pick_cost = np.full(len(rets), 30 / 10000)

        net_rets = rets.values - per_pick_cost

        # Inverse-volatility weighting
        if "vol_prior_12m" in picks_valid.columns:
            raw_vol = picks_valid["vol_prior_12m"].clip(0.05, 3.0)
            raw_vol = raw_vol.fillna(raw_vol.median() if raw_vol.notna().any() else 0.4)
            inv_vol = 1.0 / raw_vol.values
            weights = inv_vol / inv_vol.sum()
            weights = np.minimum(weights, MAX_POSITION_WEIGHT)
            weights = weights / weights.sum()
            weights = _apply_sector_cap(
                weights, picks_valid.reset_index(drop=True), MAX_SECTOR_WEIGHT
            )
        else:
            weights = np.ones(len(net_rets)) / len(net_rets)

        port_ret = float(np.dot(weights, net_rets))

        # SPY benchmark
        spy_ret = spy_returns.get(int(yr))
        bench_ret = spy_ret if spy_ret is not None else 0.0

        annual_rows.append(
            {
                "year": yr,
                "port_ret": port_ret,
                "bench_ret": bench_ret,
                "excess": port_ret - bench_ret,
                "n_picks": len(rets),
                "hit_rate": float((rets.values > 0).mean()),
            }
        )

    if not annual_rows:
        return {"error": "No test years produced results"}

    res = pd.DataFrame(annual_rows)
    n = len(res)
    wealth = np.cumprod(1 + res["port_ret"].values)
    cagr = float(wealth[-1] ** (1 / n) - 1)
    vol = float(res["port_ret"].std())
    sharpe = float((cagr - RISK_FREE) / vol) if vol > 0 else np.nan

    # Max drawdown
    peak = np.maximum.accumulate(wealth)
    drawdowns = (wealth - peak) / peak
    max_dd = float(drawdowns.min())

    # Benchmark stats
    bench_wealth = np.cumprod(1 + res["bench_ret"].values)
    bench_cagr = float(bench_wealth[-1] ** (1 / n) - 1)

    return {
        "n_years": n,
        "cagr_pct": round(cagr * 100, 2),
        "bench_cagr_pct": round(bench_cagr * 100, 2),
        "excess_cagr_pct": round((cagr - bench_cagr) * 100, 2),
        "sharpe": round(sharpe, 3) if pd.notna(sharpe) else None,
        "max_drawdown_pct": round(max_dd * 100, 2),
        "hit_rate_pct": round(res["hit_rate"].mean() * 100, 1),
        "annual_returns": [
            {
                "year": int(r["year"]),
                "port_pct": round(r["port_ret"] * 100, 2),
                "bench_pct": round(r["bench_ret"] * 100, 2),
                "excess_pct": round(r["excess"] * 100, 2),
                "n_picks": int(r["n_picks"]),
            }
            for _, r in res.iterrows()
        ],
    }


def main():
    print("=" * 70)
    print("SESSION 22: Proper Train/Validate/Test Split")
    print("=" * 70)
    print(f"\n  Train:    {TRAIN_START}-{TRAIN_END}")
    print(f"  Validate: {VAL_START}-{VAL_END}")
    print(f"  Test:     {TEST_START}-{TEST_END}")
    print(f"  Stability: {TRAIN_START_ALT}-{TRAIN_END_ALT}")

    print("\nLoading data...")
    df = load_data()
    print(f"  {len(df):,} rows, {df['fiscal_year'].min()}-{df['fiscal_year'].max()}")

    # ── Step 1: Feature selection on TRAIN period only ────────────────────────
    print("\n" + "─" * 70)
    print(f"STEP 1: Feature selection on TRAIN period ({TRAIN_START}-{TRAIN_END})")
    print("─" * 70)

    features_primary, ic_tbl_primary = select_features_on_period(
        df, TRAIN_START, TRAIN_END, VAL_START, VAL_END, "forward_return_3y"
    )

    if not features_primary:
        print("\n  FATAL: No features survived selection on train period.")
        print("  This means the signal is too weak in early data to select features.")
        sys.exit(1)

    print(f"\n  Primary features ({len(features_primary)}):")
    for f in features_primary:
        print(f"    - {f}")

    # ── Step 2: Stability check — shifted train window ────────────────────────
    print("\n" + "─" * 70)
    print(f"STEP 2: Stability check — shifted window ({TRAIN_START_ALT}-{TRAIN_END_ALT})")
    print("─" * 70)

    features_alt, ic_tbl_alt = select_features_on_period(
        df, TRAIN_START_ALT, TRAIN_END_ALT, VAL_START, VAL_END, "forward_return_3y"
    )

    # Compute overlap
    set_primary = set(features_primary)
    set_alt = set(features_alt)
    overlap = set_primary & set_alt
    union = set_primary | set_alt
    overlap_pct = len(overlap) / len(union) * 100 if union else 0

    print(f"\n  Primary window ({TRAIN_START}-{TRAIN_END}): {len(features_primary)} features")
    print(f"  Shifted window ({TRAIN_START_ALT}-{TRAIN_END_ALT}): {len(features_alt)} features")
    print(f"  Overlap: {len(overlap)}/{len(union)} = {overlap_pct:.1f}%")
    print(f"  Stable features (in both): {sorted(overlap)}")
    print(f"  Only in primary: {sorted(set_primary - set_alt)}")
    print(f"  Only in shifted: {sorted(set_alt - set_primary)}")

    # ── Step 3: Walk-forward backtest on TEST period ──────────────────────────
    print("\n" + "─" * 70)
    print("STEP 3: Walk-forward backtest on TEST period (2017-2023)")
    print("─" * 70)

    result = walk_forward_backtest(df, features_primary, TEST_START, TEST_END, top_n=20)

    if "error" in result:
        print(f"\n  ERROR: {result['error']}")
        sys.exit(1)

    print(f"\n  TEST PERIOD RESULTS (features selected on {TRAIN_START}-{TRAIN_END} only):")
    print(f"  ─────────────────────────────────────────────")
    print(f"  CAGR:         {result['cagr_pct']:+.2f}%")
    print(f"  Bench (SPY):  {result['bench_cagr_pct']:+.2f}%")
    print(f"  Excess:       {result['excess_cagr_pct']:+.2f}%")
    print(f"  Sharpe:       {result['sharpe']}")
    print(f"  Max Drawdown: {result['max_drawdown_pct']:.2f}%")
    print(f"  Hit Rate:     {result['hit_rate_pct']:.1f}%")
    print(f"\n  Annual breakdown:")
    print(f"  {'Year':<6} {'Port%':<8} {'SPY%':<8} {'Excess%':<9} {'Picks'}")
    for row in result["annual_returns"]:
        print(
            f"  {row['year']}   {row['port_pct']:+6.2f}  {row['bench_pct']:+6.2f}"
            f"  {row['excess_pct']:+7.2f}   {row['n_picks']:3d}"
        )

    # ── Step 4: Compare with current (biased) feature set ─────────────────────
    print("\n" + "─" * 70)
    print("STEP 4: Comparison with current (full-history) feature set")
    print("─" * 70)

    current_path = ROOT / "models" / "feature_sets_3y.json"
    if current_path.exists():
        with open(current_path) as f:
            current = json.load(f)
        current_feats = set(current["features"])
        new_feats = set(features_primary)
        print(f"  Current (biased):    {len(current_feats)} features")
        print(f"  Train-only:          {len(new_feats)} features")
        print(f"  Overlap:             {len(current_feats & new_feats)}")
        print(f"  Dropped by restrict: {sorted(current_feats - new_feats)}")
        print(f"  New (not in biased): {sorted(new_feats - current_feats)}")

    # ── Gate decision ─────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("GATE DECISION")
    print("=" * 70)

    sharpe = result["sharpe"]
    if sharpe >= 0.8:
        gate = "PASS"
        msg = f"Sharpe {sharpe:.3f} >= 0.8 — signal is real. Proceed to sessions 23-25."
    elif sharpe >= 0.5:
        gate = "PROCEED_WITH_CAUTION"
        msg = f"Sharpe {sharpe:.3f} in [0.5, 0.8) — signal exists but weaker. Proceed, lower expectations."
    else:
        gate = "PIVOT"
        msg = f"Sharpe {sharpe:.3f} < 0.5 — feature selection was overfitted. Pivot needed."

    print(f"\n  Test-period Sharpe: {sharpe:.3f}")
    print(f"  Gate: {gate}")
    print(f"  {msg}")
    print(f"  Feature stability: {overlap_pct:.1f}% overlap between train windows")

    # ── Save results ──────────────────────────────────────────────────────────
    output = {
        "split": {
            "train": f"{TRAIN_START}-{TRAIN_END}",
            "validate": f"{VAL_START}-{VAL_END}",
            "test": f"{TEST_START}-{TEST_END}",
        },
        "features_selected": features_primary,
        "n_features": len(features_primary),
        "stability": {
            "alt_window": f"{TRAIN_START_ALT}-{TRAIN_END_ALT}",
            "features_alt": features_alt,
            "overlap_pct": round(overlap_pct, 1),
            "stable_features": sorted(overlap),
        },
        "test_results": result,
        "gate": {"sharpe": sharpe, "decision": gate, "message": msg},
    }

    out_path = ROOT / "reports" / "proper_split_results.json"
    out_path.parent.mkdir(exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Results saved to {out_path}")

    return output


if __name__ == "__main__":
    main()
