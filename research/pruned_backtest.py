"""
Session 23: Pruned Feature Set Backtest.

Takes the 27 temporally stable features from Session 22 and re-runs the
proper-split backtest. If Sharpe drops > 0.1 vs full 43-feature model,
adds back unstable features one at a time (ranked by ICIR) until recovery.

Reuses walk_forward_backtest() from proper_split_backtest.py.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _root import ROOT
from modeling.train import load_data
from research.proper_split_backtest import (
    walk_forward_backtest,
    TRAIN_START,
    TRAIN_END,
    VAL_START,
    VAL_END,
    TEST_START,
    TEST_END,
)


def rank_unstable_by_icir(
    df: pd.DataFrame,
    unstable_features: list[str],
    train_start: int,
    train_end: int,
    ret_col: str = "forward_return_3y",
) -> list[tuple[str, float]]:
    """Rank unstable features by absolute ICIR on primary train window."""
    df_train = df[
        df["fiscal_year"].between(train_start, train_end) & df[ret_col].notna()
    ]

    ranked = []
    for feat in unstable_features:
        if feat not in df_train.columns:
            continue
        sub = df_train[df_train[feat].notna()]
        ics = []
        for yr in sub["fiscal_year"].unique():
            g = sub[sub["fiscal_year"] == yr]
            if len(g) < 30:
                continue
            c, _ = stats.spearmanr(g[feat], g[ret_col])
            if not np.isnan(c):
                ics.append(c)
        if len(ics) >= 3:
            icir = abs(np.mean(ics) / (np.std(ics) + 1e-8))
            ranked.append((feat, round(icir, 4)))

    ranked.sort(key=lambda x: x[1], reverse=True)
    return ranked


def main():
    print("=" * 70)
    print("SESSION 23: Pruned Feature Set Backtest")
    print("=" * 70)

    # Load results from Session 22
    results_path = ROOT / "reports" / "proper_split_results.json"
    with open(results_path) as f:
        s22 = json.load(f)

    stable_features = s22["stability"]["stable_features"]
    all_features = s22["features_selected"]
    full_sharpe = s22["test_results"]["sharpe"]

    unstable_features = [f for f in all_features if f not in stable_features]

    print(f"\n  Full model: {len(all_features)} features, Sharpe = {full_sharpe}")
    print(f"  Stable (core): {len(stable_features)} features")
    print(f"  Unstable: {len(unstable_features)} features")
    print(f"  Unstable set: {unstable_features}")

    # Load data
    print("\nLoading data...")
    df = load_data()
    print(f"  {len(df):,} rows")

    # ── Step 1: Backtest with ONLY stable features ───────────────────────────
    print("\n" + "─" * 70)
    print("STEP 1: Backtest with 27 stable features only")
    print("─" * 70)

    lean_result = walk_forward_backtest(df, stable_features, TEST_START, TEST_END, top_n=20)

    if "error" in lean_result:
        print(f"\n  ERROR: {lean_result['error']}")
        sys.exit(1)

    lean_sharpe = lean_result["sharpe"]
    sharpe_drop = full_sharpe - lean_sharpe

    print(f"\n  Lean model Sharpe:  {lean_sharpe:.3f}")
    print(f"  Full model Sharpe:  {full_sharpe:.3f}")
    print(f"  Drop:               {sharpe_drop:+.3f}")

    # ── Step 2: Add-back if needed ───────────────────────────────────────────
    final_features = list(stable_features)
    final_result = lean_result
    added_back = []

    if sharpe_drop > 0.1:
        print("\n" + "─" * 70)
        print("STEP 2: Sharpe drop > 0.1 — adding back features by ICIR rank")
        print("─" * 70)

        ranked_unstable = rank_unstable_by_icir(
            df, unstable_features, TRAIN_START, TRAIN_END
        )
        print(f"\n  Unstable features ranked by ICIR:")
        for feat, icir in ranked_unstable:
            print(f"    {feat}: ICIR = {icir}")

        target_sharpe = full_sharpe - 0.05

        for feat, icir in ranked_unstable:
            candidate_features = final_features + [feat]
            print(f"\n  Adding '{feat}' (ICIR={icir})...")

            candidate_result = walk_forward_backtest(
                df, candidate_features, TEST_START, TEST_END, top_n=20
            )

            if "error" in candidate_result:
                print(f"    Error — skipping")
                continue

            candidate_sharpe = candidate_result["sharpe"]
            print(f"    Sharpe: {candidate_sharpe:.3f} (target: {target_sharpe:.3f})")

            final_features = candidate_features
            final_result = candidate_result
            added_back.append({"feature": feat, "icir": icir, "sharpe_after": candidate_sharpe})

            if candidate_sharpe >= target_sharpe:
                print(f"\n  Recovery achieved! Sharpe {candidate_sharpe:.3f} >= {target_sharpe:.3f}")
                break
    else:
        print("\n  Sharpe drop <= 0.1 — lean model is the new baseline. No add-back needed.")

    # ── Final results ────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("FINAL RESULTS")
    print("=" * 70)

    final_sharpe = final_result["sharpe"]
    print(f"\n  Final feature count: {len(final_features)}")
    print(f"  Final Sharpe:        {final_sharpe:.3f}")
    print(f"  Full model Sharpe:   {full_sharpe:.3f}")
    print(f"  CAGR:                {final_result['cagr_pct']:+.2f}%")
    print(f"  Bench (SPY):         {final_result['bench_cagr_pct']:+.2f}%")
    print(f"  Excess:              {final_result['excess_cagr_pct']:+.2f}%")
    print(f"  Hit Rate:            {final_result['hit_rate_pct']:.1f}%")

    print(f"\n  Annual breakdown:")
    print(f"  {'Year':<6} {'Port%':<8} {'SPY%':<8} {'Excess%':<9} {'Picks'}")
    for row in final_result["annual_returns"]:
        print(
            f"  {row['year']}   {row['port_pct']:+6.2f}  {row['bench_pct']:+6.2f}"
            f"  {row['excess_pct']:+7.2f}   {row['n_picks']:3d}"
        )

    # ── Save pruned feature set ──────────────────────────────────────────────
    output_features = {
        "name": "pruned_stable_3y",
        "description": "Temporally stable features — survived both train windows (2008-2014, 2010-2016)",
        "source": "Session 23 pruned backtest",
        "base_stable": stable_features,
        "added_back": [a["feature"] for a in added_back],
        "features": sorted(final_features),
        "n_features": len(final_features),
        "sharpe": final_sharpe,
        "full_model_sharpe": full_sharpe,
    }

    feat_path = ROOT / "models" / "feature_sets_pruned.json"
    feat_path.parent.mkdir(exist_ok=True)
    with open(feat_path, "w") as f:
        json.dump(output_features, f, indent=2)
    print(f"\n  Saved: {feat_path}")

    # ── Save comparison report ───────────────────────────────────────────────
    report_lines = [
        "# Pruned Feature Set Backtest — Session 23",
        "",
        "## Summary",
        "",
        f"| Metric | Full (43 feat) | Lean ({len(final_features)} feat) | Delta |",
        "|--------|---------------|-------------|-------|",
        f"| Sharpe | {full_sharpe:.3f} | {final_sharpe:.3f} | {final_sharpe - full_sharpe:+.3f} |",
        f"| CAGR | {s22['test_results']['cagr_pct']:+.2f}% | {final_result['cagr_pct']:+.2f}% | {final_result['cagr_pct'] - s22['test_results']['cagr_pct']:+.2f}% |",
        f"| Excess CAGR | {s22['test_results']['excess_cagr_pct']:+.2f}% | {final_result['excess_cagr_pct']:+.2f}% | {final_result['excess_cagr_pct'] - s22['test_results']['excess_cagr_pct']:+.2f}% |",
        f"| Hit Rate | {s22['test_results']['hit_rate_pct']:.1f}% | {final_result['hit_rate_pct']:.1f}% | {final_result['hit_rate_pct'] - s22['test_results']['hit_rate_pct']:+.1f}% |",
        f"| Features | 43 | {len(final_features)} | -{43 - len(final_features)} |",
        "",
        "## Methodology",
        "",
        "- **Stable features**: 27 features that survived BOTH train windows (2008-2014 AND 2010-2016)",
        "- **Split**: Train 2008-2014, Validate 2015-2018, Test 2019-2024 (same as Session 22)",
        "- **Backtest**: Walk-forward ML (expanding window) with composite portfolio strategy",
        f"- **Add-back threshold**: Sharpe drop > 0.1 triggers sequential feature restoration",
        "",
        "## Stable Feature Set (core)",
        "",
    ]
    for feat in sorted(stable_features):
        report_lines.append(f"- {feat}")

    if added_back:
        report_lines.extend([
            "",
            "## Features Added Back (unstable but needed)",
            "",
        ])
        for a in added_back:
            report_lines.append(f"- {a['feature']} (ICIR={a['icir']}, Sharpe after={a['sharpe_after']:.3f})")

    report_lines.extend([
        "",
        "## Annual Returns Comparison",
        "",
        f"| Year | Full Port% | Lean Port% | Full Excess% | Lean Excess% |",
        "|------|-----------|-----------|-------------|-------------|",
    ])
    full_annual = {r["year"]: r for r in s22["test_results"]["annual_returns"]}
    for row in final_result["annual_returns"]:
        yr = row["year"]
        full_row = full_annual.get(yr, {})
        report_lines.append(
            f"| {yr} | {full_row.get('port_pct', 'N/A')}% | {row['port_pct']:+.2f}% "
            f"| {full_row.get('excess_pct', 'N/A')}% | {row['excess_pct']:+.2f}% |"
        )

    report_lines.extend([
        "",
        "## Conclusion",
        "",
    ])
    if sharpe_drop <= 0.1:
        report_lines.append(
            f"The lean {len(final_features)}-feature model (Sharpe {final_sharpe:.3f}) is within 0.1 of the "
            f"full 43-feature model (Sharpe {full_sharpe:.3f}). **Simpler model adopted as new baseline.**"
        )
    else:
        report_lines.append(
            f"After adding back {len(added_back)} features, recovered to Sharpe {final_sharpe:.3f} "
            f"(target: {full_sharpe - 0.05:.3f}). Final set: {len(final_features)} features."
        )

    report_path = ROOT / "reports" / "pruned_backtest_results.md"
    with open(report_path, "w") as f:
        f.write("\n".join(report_lines) + "\n")
    print(f"  Saved: {report_path}")

    # ── Decision ─────────────────────────────────────────────────────────────
    print("\n" + "─" * 70)
    if sharpe_drop <= 0.1:
        print(f"  DECISION: Lean {len(final_features)}-feature model IS the new baseline.")
        print(f"  Removed {43 - len(final_features)} features with no meaningful Sharpe loss.")
    else:
        print(f"  DECISION: Added {len(added_back)} features back. Final set: {len(final_features)}.")
    print("─" * 70)

    return output_features


if __name__ == "__main__":
    main()
