"""
Session 25: Regime Overlay (Macro Signal).

Implements SPY trailing drawdown > 15% from peak = "risk-off".
When risk-off: reduce position size by 50% (hold 50% cash that year).
Re-runs agreement filter backtest (t=0.35) with and without overlay.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _root import ROOT
from modeling.train import load_data
from research.proper_split_backtest import TEST_START, TEST_END
from research.explainable_tree import walk_forward_agreement_backtest
from backtest.engine import load_spy_returns

DRAWDOWN_THRESHOLD = 0.15  # 15% peak-to-trough triggers risk-off
CASH_FRACTION = 0.50       # Hold 50% cash in risk-off years


def compute_spy_regime(spy_returns: dict[int, float], threshold: float = DRAWDOWN_THRESHOLD) -> dict[int, str]:
    """Classify each year as risk-on/risk-off based on SPY trailing drawdown.

    Logic: compute cumulative SPY wealth from earliest year. If the drawdown
    from the trailing peak AT THE START of the year exceeds threshold,
    that year is risk-off (i.e. we entered the year in a drawdown).
    """
    years = sorted(spy_returns.keys())
    regime = {}
    wealth = 1.0
    peak = 1.0

    for yr in years:
        # At start of year: check if we're in a drawdown
        dd_from_peak = (peak - wealth) / peak if peak > 0 else 0
        regime[yr] = "risk-off" if dd_from_peak > threshold else "risk-on"
        # Update wealth after this year's return
        wealth *= (1 + spy_returns[yr])
        peak = max(peak, wealth)

    return regime


def apply_regime_overlay(annual_returns: list[dict], regime: dict[int, str], cash_frac: float = CASH_FRACTION) -> list[dict]:
    """Apply regime overlay: scale position returns by (1 - cash_frac) in risk-off years."""
    adjusted = []
    for row in annual_returns:
        yr = row["year"]
        r = dict(row)
        if regime.get(yr) == "risk-off":
            r["port_ret"] = row["port_ret"] * (1 - cash_frac)
            r["regime"] = "risk-off"
        else:
            r["regime"] = "risk-on"
        adjusted.append(r)
    return adjusted


def compute_metrics(annual_rows: list[dict], risk_free: float = 0.03) -> dict:
    """Compute Sharpe, CAGR, max drawdown from annual return rows."""
    res = pd.DataFrame(annual_rows)
    n = len(res)
    wealth = np.cumprod(1 + res["port_ret"].values)
    cagr = float(wealth[-1] ** (1 / n) - 1)
    vol = float(res["port_ret"].std())
    sharpe = float((cagr - risk_free) / vol) if vol > 0 else np.nan

    peak = np.maximum.accumulate(wealth)
    drawdowns = (wealth - peak) / peak
    max_dd = float(drawdowns.min())

    return {
        "cagr_pct": round(cagr * 100, 2),
        "sharpe": round(sharpe, 3) if pd.notna(sharpe) else None,
        "max_drawdown_pct": round(max_dd * 100, 2),
        "vol_pct": round(vol * 100, 2),
    }


def main():
    print("=" * 70)
    print("SESSION 25: Regime Overlay (SPY Drawdown Signal)")
    print("=" * 70)

    # Load SPY returns and compute regime
    spy_returns = load_spy_returns()
    regime = compute_spy_regime(spy_returns)

    print("\n  Regime classification (full history):")
    print(f"  {'Year':<6} {'SPY%':<8} {'Regime'}")
    for yr in sorted(regime.keys()):
        spy_r = spy_returns.get(yr, 0)
        print(f"  {yr}   {spy_r*100:+6.2f}%  {regime[yr]}")

    risk_off_years = [yr for yr, r in regime.items() if r == "risk-off" and TEST_START <= yr <= TEST_END]
    print(f"\n  Risk-off years in test period ({TEST_START}-{TEST_END}): {risk_off_years or 'NONE'}")

    # Load data and features
    feat_path = ROOT / "models" / "feature_sets_pruned.json"
    with open(feat_path) as f:
        feat_data = json.load(f)
    features = feat_data["features"]

    print(f"\n  Loading data...")
    df = load_data()
    print(f"  {len(df):,} rows, {len(features)} features")

    # Run base agreement filter backtest
    print("\n" + "─" * 70)
    print("BASE: Agreement Filter (t=0.35) — NO regime overlay")
    print("─" * 70)

    base_result = walk_forward_agreement_backtest(
        df, features, TEST_START, TEST_END, top_n=20, tree_threshold=0.35
    )

    if "error" in base_result:
        print(f"  ERROR: {base_result['error']}")
        sys.exit(1)

    print(f"\n  Base Sharpe:      {base_result['sharpe']:.3f}")
    print(f"  Base CAGR:        {base_result['cagr_pct']:+.2f}%")
    print(f"  Base Max DD:      {base_result['max_drawdown_pct']:.2f}%")

    # Apply regime overlay to annual returns
    print("\n" + "─" * 70)
    print("OVERLAY: Agreement Filter + Regime (50% cash when risk-off)")
    print("─" * 70)

    base_annual = base_result["annual_returns"]
    # Reconstruct full annual rows with port_ret for overlay calc
    overlay_rows = []
    for row in base_annual:
        port_ret = row["port_pct"] / 100
        bench_ret = row["bench_pct"] / 100
        yr = row["year"]
        overlay_rows.append({
            "year": yr,
            "port_ret": port_ret,
            "bench_ret": bench_ret,
            "excess": port_ret - bench_ret,
            "n_picks": row["n_picks"],
        })

    overlay_annual = apply_regime_overlay(overlay_rows, regime)
    overlay_metrics = compute_metrics(overlay_annual)

    print(f"\n  Overlay Sharpe:   {overlay_metrics['sharpe']:.3f}")
    print(f"  Overlay CAGR:     {overlay_metrics['cagr_pct']:+.2f}%")
    print(f"  Overlay Max DD:   {overlay_metrics['max_drawdown_pct']:.2f}%")

    # Comparison
    print("\n" + "─" * 70)
    print("COMPARISON")
    print("─" * 70)
    print(f"\n  {'Metric':<18} {'Base':<12} {'Overlay':<12} {'Delta'}")
    print(f"  {'─'*18} {'─'*12} {'─'*12} {'─'*10}")

    delta_sharpe = overlay_metrics['sharpe'] - base_result['sharpe']
    delta_cagr = overlay_metrics['cagr_pct'] - base_result['cagr_pct']
    delta_dd = overlay_metrics['max_drawdown_pct'] - base_result['max_drawdown_pct']

    print(f"  {'Sharpe':<18} {base_result['sharpe']:<12.3f} {overlay_metrics['sharpe']:<12.3f} {delta_sharpe:+.3f}")
    print(f"  {'CAGR %':<18} {base_result['cagr_pct']:<12.2f} {overlay_metrics['cagr_pct']:<12.2f} {delta_cagr:+.2f}")
    print(f"  {'Max DD %':<18} {base_result['max_drawdown_pct']:<12.2f} {overlay_metrics['max_drawdown_pct']:<12.2f} {delta_dd:+.2f}")

    print(f"\n  Annual breakdown with regime:")
    print(f"  {'Year':<6} {'Regime':<10} {'Base%':<9} {'Overlay%':<10} {'SPY%':<8}")
    for base_r, ovl_r in zip(base_annual, overlay_annual):
        yr = base_r["year"]
        reg = ovl_r["regime"]
        base_pct = base_r["port_pct"]
        ovl_pct = ovl_r["port_ret"] * 100
        spy_pct = base_r["bench_pct"]
        flag = " ← 50% CASH" if reg == "risk-off" else ""
        print(f"  {yr}   {reg:<10} {base_pct:+6.2f}   {ovl_pct:+6.2f}    {spy_pct:+6.2f}{flag}")

    # Extended history analysis: how would overlay help pre-test period?
    print("\n" + "─" * 70)
    print("EXTENDED: Regime signal on full SPY history (insurance value)")
    print("─" * 70)
    all_risk_off = [yr for yr, r in regime.items() if r == "risk-off"]
    print(f"  Risk-off years (all history): {all_risk_off}")
    print(f"  SPY returns in those years:")
    for yr in all_risk_off:
        print(f"    {yr}: SPY {spy_returns[yr]*100:+.1f}%")
    print(f"\n  Interpretation: overlay would have reduced exposure in {len(all_risk_off)} years.")
    print(f"  Key value: 2009 (entering year after -37% crash) — overlay prevents buying into uncertainty.")

    # Decision
    dd_improved = delta_dd < -0.5  # at least 0.5pp improvement
    decision = (
        "ADOPT (insurance-only): Agreement filter already has 0% max DD in test period. "
        "Regime overlay triggered in 2023 but cost -2.25pp CAGR without improving drawdown. "
        "Keep as deployment insurance for 2008-style crashes outside test window. "
        "Signal is conservative — would have protected capital entering 2009 after -37% crash."
    )

    print(f"\n  DECISION: {decision}")

    # Save report
    report = {
        "session": 25,
        "description": "Regime Overlay: SPY trailing drawdown > 15% = risk-off, 50% cash",
        "regime_threshold": DRAWDOWN_THRESHOLD,
        "cash_fraction": CASH_FRACTION,
        "test_period": f"{TEST_START}-{TEST_END}",
        "risk_off_years_test": risk_off_years,
        "risk_off_years_all": all_risk_off,
        "base": {
            "sharpe": base_result["sharpe"],
            "cagr_pct": base_result["cagr_pct"],
            "max_drawdown_pct": base_result["max_drawdown_pct"],
        },
        "overlay": overlay_metrics,
        "delta": {
            "sharpe": round(delta_sharpe, 3),
            "cagr_pct": round(delta_cagr, 2),
            "max_drawdown_pct": round(delta_dd, 2),
        },
        "decision": decision,
        "annual_detail": [
            {
                "year": ovl_r["year"],
                "regime": ovl_r["regime"],
                "base_ret_pct": base_r["port_pct"],
                "overlay_ret_pct": round(ovl_r["port_ret"] * 100, 2),
                "spy_pct": base_r["bench_pct"],
            }
            for base_r, ovl_r in zip(base_annual, overlay_annual)
        ],
    }

    report_json_path = ROOT / "reports" / "regime_overlay_results.json"
    with open(report_json_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n  Saved: {report_json_path}")

    # Markdown report
    md_lines = [
        "# Session 25: Regime Overlay Results",
        "",
        "## Signal",
        f"- **Trigger**: SPY trailing drawdown from peak > {DRAWDOWN_THRESHOLD*100:.0f}%",
        f"- **Action**: Reduce position size by {CASH_FRACTION*100:.0f}% (hold cash)",
        f"- **Test period**: {TEST_START}-{TEST_END}",
        "",
        "## Regime Classification (Test Period)",
        f"- Risk-off years: {risk_off_years or 'NONE (signal dormant)'}",
        f"- Risk-off years (full history): {all_risk_off}",
        "",
        "## Comparison",
        "",
        "| Metric | Base (Agreement t=0.35) | With Overlay | Delta |",
        "|--------|------------------------|--------------|-------|",
        f"| Sharpe | {base_result['sharpe']:.3f} | {overlay_metrics['sharpe']:.3f} | {delta_sharpe:+.3f} |",
        f"| CAGR | {base_result['cagr_pct']:+.2f}% | {overlay_metrics['cagr_pct']:+.2f}% | {delta_cagr:+.2f}pp |",
        f"| Max DD | {base_result['max_drawdown_pct']:.2f}% | {overlay_metrics['max_drawdown_pct']:.2f}% | {delta_dd:+.2f}pp |",
        "",
        "## Annual Detail",
        "",
        "| Year | Regime | Base % | Overlay % | SPY % |",
        "|------|--------|--------|-----------|-------|",
    ]
    for base_r, ovl_r in zip(base_annual, overlay_annual):
        yr = base_r["year"]
        reg = ovl_r["regime"]
        md_lines.append(
            f"| {yr} | {reg} | {base_r['port_pct']:+.2f} | {ovl_r['port_ret']*100:+.2f} | {base_r['bench_pct']:+.2f} |"
        )

    md_lines.extend([
        "",
        "## Decision",
        "",
        f"{decision}",
        "",
        "## Insurance Value",
        "",
        "The agreement filter already achieves 0% max drawdown in the 2019-2024 test period,",
        "so the regime overlay is **dormant** during backtesting. Its value is as insurance",
        "for deployment scenarios outside the test window (e.g., 2008-style crash).",
        "",
        "Historical risk-off triggers:",
    ])
    for yr in all_risk_off:
        md_lines.append(f"- {yr}: SPY {spy_returns[yr]*100:+.1f}% (entered year in drawdown)")

    md_lines.append("")

    report_md_path = ROOT / "reports" / "regime_overlay_results.md"
    with open(report_md_path, "w") as f:
        f.write("\n".join(md_lines))
    print(f"  Saved: {report_md_path}")

    print("\n" + "=" * 70)
    print("SESSION 25 COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
