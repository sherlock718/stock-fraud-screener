"""
Build screener registry — backtest all named screener configs, write data/screener_registry.json.

Each screener config defines a named investment filter with specific market, size, and alpha
signal blend. The registry is the single source of truth consumed by the experiment notebook
(notebooks/08_experiment_hub.ipynb) for leaderboard comparisons.

Usage:
    python3 scripts/build_screener_registry.py               # run all screeners
    python3 scripts/build_screener_registry.py --top 15      # override top_n for all
    python3 scripts/build_screener_registry.py --ids COMPOSITE_US VALUE_QUALITY
    python3 scripts/build_screener_registry.py --dry-run     # list configs and exit
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

BASE = Path(__file__).parent.parent
sys.path.insert(0, str(BASE))

from scripts.backtester import (
    DEFAULT_COST_BPS,
    MIN_MARKET_CAP,
    SMALLCAP_COST_BPS,
    load_monthly_prices,
    load_spy_returns,
    run_backtest,
)

OUT_PATH = BASE / "data" / "screener_registry.json"
FULL_DATA = BASE / "data" / "historical_dataset_clean.parquet"

# ── Screener config definitions ──────────────────────────────────────────────
#
# Each config maps to a callable filter_fn that accepts (yr_df, top_n, market).
# Configs reuse and extend backtester.py strategy filters with extra pre-screens.


def _ml(s: pd.DataFrame, horizon: str) -> str:
    """Return the unbiased OOF ML column name for a given horizon (e.g. '1y' → 'ml_1y_oof')."""
    col = f"ml_{horizon}_oof"
    return col if col in s.columns else f"ml_{horizon}"


def _rank_blend(s: pd.DataFrame, weights: list[tuple[str, float]]) -> pd.Series:
    """Percentile-rank-blend columns with given weights; returns a score Series."""
    score = pd.Series(0.0, index=s.index)
    total_w = 0.0
    for col, w in weights:
        if col in s.columns and s[col].notna().sum() > 5:
            score += s[col].rank(pct=True) * w
            total_w += w
    return score / max(total_w, 1e-9)


def _quality_gate(s: pd.DataFrame, min_fscore: int = 6) -> pd.DataFrame:
    """Remove likely fraudsters + low Piotroski quality floor."""
    if "beneish_m_score" in s.columns:
        s = s[s["beneish_m_score"].fillna(0) < -1.78]
    if "likely_delisted" in s.columns:
        s = s[s["likely_delisted"].fillna(1) == 0]
    s = s[s["piotroski_f_score"].fillna(0) >= min_fscore]
    return s


# ── Individual filter functions ───────────────────────────────────────────────


def filter_composite_us(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    """Composite alpha, US only, all sizes."""
    s = yr_df[yr_df["market"] == "US"].copy()
    s = _quality_gate(s, min_fscore=5)
    s["_score"] = _rank_blend(
        s,
        [
            ("alpha_composite", 0.30),
            (_ml(s, "1y"), 0.25),
            (_ml(s, "3y"), 0.20),
            ("value_composite", 0.15),
            ("quality_composite", 0.10),
        ],
    )
    return s.nlargest(top_n, "_score").index


def filter_composite_intl(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    """Composite alpha, international (non-US) markets."""
    s = yr_df[yr_df["market"] != "US"].copy()
    s = _quality_gate(s, min_fscore=5)
    s["_score"] = _rank_blend(
        s,
        [
            ("alpha_composite", 0.30),
            (_ml(s, "3y"), 0.25),
            ("value_composite", 0.25),
            ("quality_composite", 0.20),
        ],
    )
    return s.nlargest(top_n, "_score").index


def filter_composite_micro(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    """Micro/small-cap composite — high return potential, high illiquidity risk."""
    s = yr_df.copy()
    if market:
        s = s[s["market"] == market]
    if "size_category_label" in s.columns:
        s = s[s["size_category_label"].isin(["micro", "small"])]
    if "market_cap_at_filing" in s.columns:
        s = s[s["market_cap_at_filing"].fillna(0) >= 10_000_000]  # $10M floor
    s = _quality_gate(s, min_fscore=6)
    s["_score"] = _rank_blend(
        s,
        [
            ("value_composite", 0.30),
            ("quality_composite", 0.25),
            (_ml(s, "3y"), 0.25),
            ("alpha_growth", 0.20),
        ],
    )
    return s.nlargest(top_n, "_score").index


def filter_value_quality(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    """Deep value + high quality (Piotroski ≥ 7), no momentum requirement."""
    s = yr_df.copy()
    if market:
        s = s[s["market"] == market]
    s = _quality_gate(s, min_fscore=7)
    if "altman_z_score" in s.columns:
        s = s[s["altman_z_score"].fillna(0) > 1.81]  # above distress zone
    s["_score"] = _rank_blend(
        s,
        [
            ("value_composite", 0.40),
            ("quality_composite", 0.35),
            ("alpha_value", 0.15),
            ("alpha_quality", 0.10),
        ],
    )
    return s.nlargest(top_n, "_score").index


def filter_momentum_growth(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    """Momentum + earnings growth; positive price trend + rising EPS."""
    s = yr_df.copy()
    if market:
        s = s[s["market"] == market]
    if "momentum_12m_prior" in s.columns:
        s = s[s["momentum_12m_prior"].fillna(-99) > 0.05]  # 5% positive price momentum
    if "eps_growth_yoy" in s.columns and s["eps_growth_yoy"].notna().mean() > 0.3:
        s = s[s["eps_growth_yoy"].fillna(-99) > 0]
    s = s[s["piotroski_f_score"].fillna(0) >= 5]  # relaxed quality floor
    if "beneish_m_score" in s.columns:
        s = s[s["beneish_m_score"].fillna(0) < -1.78]
    s["_score"] = _rank_blend(
        s,
        [
            ("alpha_momentum", 0.35),
            ("alpha_growth", 0.30),
            (_ml(s, "1y"), 0.20),
            ("quality_composite", 0.15),
        ],
    )
    return s.nlargest(top_n, "_score").index


def filter_fraud_avoid(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    """Fraud-screened composite — strict fraud filter + quality, composite blend."""
    s = yr_df.copy()
    if market:
        s = s[s["market"] == market]
    # Strict multi-signal fraud screen
    if "beneish_m_score" in s.columns:
        s = s[s["beneish_m_score"].fillna(0) < -1.78]
    if "fraud_score_composite" in s.columns:
        s = s[s["fraud_score_composite"].fillna(1.0) < 0.4]
    if "ohlson_prob_bankruptcy" in s.columns and s["ohlson_prob_bankruptcy"].notna().mean() > 0.3:
        s = s[s["ohlson_prob_bankruptcy"].fillna(1.0) < 0.3]
    if "altman_z_score" in s.columns:
        s = s[s["altman_z_score"].fillna(0) > 1.81]
    s = s[s["piotroski_f_score"].fillna(0) >= 5]
    s["_score"] = _rank_blend(
        s,
        [
            ("alpha_fraud_risk", 0.25),
            ("alpha_composite", 0.25),
            (_ml(s, "1y"), 0.25),
            ("value_composite", 0.15),
            ("quality_composite", 0.10),
        ],
    )
    return s.nlargest(top_n, "_score").index


def filter_wide_universe(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    """Wide diversified universe — all markets, minimal filters, pure alpha signal."""
    s = yr_df.copy()
    if market:
        s = s[s["market"] == market]
    if "likely_delisted" in s.columns:
        s = s[s["likely_delisted"].fillna(1) == 0]
    if "beneish_m_score" in s.columns:
        s = s[s["beneish_m_score"].fillna(0) < -1.78]
    s = s[s["piotroski_f_score"].fillna(0) >= 4]  # minimal quality floor
    s["_score"] = _rank_blend(
        s,
        [
            ("alpha_composite", 0.40),
            (_ml(s, "1y"), 0.30),
            (_ml(s, "3y"), 0.30),
        ],
    )
    return s.nlargest(top_n, "_score").index


# ── Registry spec: each entry names a screener and its backtest params ────────

REGISTRY_SPEC: list[dict] = [
    {
        "id": "COMPOSITE_US",
        "name": "Composite — US All-Cap",
        "description": "Full alpha_composite + OOF ML signals, US market only, quality-gated",
        "filter_fn": filter_composite_us,
        "market": "US",
        "top_n": 20,
        "cost_bps": DEFAULT_COST_BPS,
        "smallcap_cost_bps": SMALLCAP_COST_BPS,
    },
    {
        "id": "COMPOSITE_INTL",
        "name": "Composite — International",
        "description": "Composite alpha, all non-US markets (KR/JP/EU/BR/CA), value-heavy weight",
        "filter_fn": filter_composite_intl,
        "market": None,
        "top_n": 20,
        "cost_bps": 40,  # higher for international friction
        "smallcap_cost_bps": 80,
    },
    {
        "id": "COMPOSITE_MICRO",
        "name": "Composite — Micro/Small Cap",
        "description": "Micro and small caps, value + quality + 3y ML, $10M+ market cap floor",
        "filter_fn": filter_composite_micro,
        "market": None,
        "top_n": 25,
        "cost_bps": SMALLCAP_COST_BPS,
        "smallcap_cost_bps": 80,
    },
    {
        "id": "VALUE_QUALITY",
        "name": "Value + Quality",
        "description": "Deep value (P/B, EV/EBITDA) + high Piotroski (≥7) + Altman z > 1.81",
        "filter_fn": filter_value_quality,
        "market": None,
        "top_n": 20,
        "cost_bps": DEFAULT_COST_BPS,
        "smallcap_cost_bps": SMALLCAP_COST_BPS,
    },
    {
        "id": "MOMENTUM_GROWTH",
        "name": "Momentum + Growth",
        "description": "Positive price momentum (>5%) + rising EPS + alpha_momentum/growth blend",
        "filter_fn": filter_momentum_growth,
        "market": None,
        "top_n": 20,
        "cost_bps": DEFAULT_COST_BPS,
        "smallcap_cost_bps": SMALLCAP_COST_BPS,
    },
    {
        "id": "FRAUD_AVOID",
        "name": "Fraud-Screened Composite",
        "description": "Strict Beneish + fraud_score_composite + Ohlson + Altman screens before alpha",
        "filter_fn": filter_fraud_avoid,
        "market": None,
        "top_n": 20,
        "cost_bps": DEFAULT_COST_BPS,
        "smallcap_cost_bps": SMALLCAP_COST_BPS,
    },
    {
        "id": "WIDE_UNIVERSE",
        "name": "Wide Universe",
        "description": "All 14 markets, minimal Piotroski ≥ 4 + Beneish gate, pure alpha_composite",
        "filter_fn": filter_wide_universe,
        "market": None,
        "top_n": 30,
        "cost_bps": 35,
        "smallcap_cost_bps": 65,
    },
]


# ── Runner ────────────────────────────────────────────────────────────────────


def run_registry(
    ids: list[str] | None = None,
    top_n_override: int | None = None,
    verbose: bool = True,
) -> dict:
    """Run all (or selected) screeners; return registry dict."""
    df = pd.read_parquet(FULL_DATA)
    spy_returns = load_spy_returns()
    monthly_px = load_monthly_prices()

    specs = REGISTRY_SPEC
    if ids:
        ids_set = set(ids)
        specs = [s for s in REGISTRY_SPEC if s["id"] in ids_set]
        missing = ids_set - {s["id"] for s in specs}
        if missing:
            print(f"  ⚠  Unknown screener IDs: {sorted(missing)}")

    results: dict[str, dict] = {}
    for spec in specs:
        sid = spec["id"]
        top_n = top_n_override if top_n_override else spec["top_n"]
        if verbose:
            print(f"  → Running {sid} (top_n={top_n}) ...", end=" ", flush=True)

        try:
            res = run_backtest(
                df=df,
                filter_fn=spec["filter_fn"],
                label=sid,
                top_n=top_n,
                market=spec["market"],
                cost_bps=spec["cost_bps"],
                smallcap_cost_bps=spec["smallcap_cost_bps"],
                min_market_cap=MIN_MARKET_CAP,
                vol_weighted=True,
                fill_missing_return=None,
                spy_returns=spy_returns,
                monthly_px=monthly_px,
            )
            # Attach config metadata
            res["config"] = {
                "id": sid,
                "name": spec["name"],
                "description": spec["description"],
                "market": spec["market"],
                "top_n": top_n,
                "cost_bps": spec["cost_bps"],
                "smallcap_cost_bps": spec["smallcap_cost_bps"],
            }
            results[sid] = res
            if verbose:
                cagr = res.get("cagr_pct", "n/a")
                sharpe = res.get("sharpe", "n/a")
                print(f"CAGR={cagr}%  Sharpe={sharpe}")
        except Exception as exc:
            if verbose:
                print(f"ERROR: {exc}")
            results[sid] = {"id": sid, "error": str(exc)}

    return results


def _leaderboard(registry: dict) -> None:
    """Print a sorted leaderboard to stdout."""
    rows = []
    for sid, v in registry.items():
        if "error" in v:
            continue
        rows.append(
            {
                "ID": sid,
                "Name": v.get("config", {}).get("name", sid),
                "CAGR %": v.get("cagr_pct"),
                "Excess vs SPY": v.get("excess_cagr_vs_spy"),
                "Sharpe": v.get("sharpe"),
                "MaxDD %": v.get("max_drawdown_pct"),
                "Calmar": v.get("calmar"),
                "HitRate %": v.get("hit_rate_pct"),
                "N Years": v.get("n_years"),
            }
        )
    if not rows:
        print("  No successful runs to display.")
        return
    df = pd.DataFrame(rows).sort_values("Sharpe", ascending=False, na_position="last")
    df = df.reset_index(drop=True)
    print("\n── Screener Leaderboard (sorted by Sharpe) ──")
    print(df.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build screener_registry.json — backtest all named screeners"
    )
    parser.add_argument(
        "--ids",
        nargs="+",
        metavar="ID",
        help="Run only these screener IDs (e.g. COMPOSITE_US VALUE_QUALITY)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=None,
        metavar="N",
        dest="top_n",
        help="Override top_n for all screeners",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List screener configs and exit without running",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=OUT_PATH,
        help=f"Output JSON path (default: {OUT_PATH})",
    )
    args = parser.parse_args()

    if args.dry_run:
        print(f"{'ID':<20} {'Name':<35} {'Market':<10} {'top_n'}")
        print("-" * 75)
        for spec in REGISTRY_SPEC:
            print(
                f"{spec['id']:<20} {spec['name']:<35} "
                f"{str(spec['market']):<10} {spec['top_n']}"
            )
        return

    print(f"Building screener registry — {len(REGISTRY_SPEC)} configs")
    registry = run_registry(ids=args.ids, top_n_override=args.top_n, verbose=True)

    # Strip filter_fn (not JSON-serializable) from config before saving
    out = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "n_screeners": len(registry),
        "screeners": registry,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)

    def _default(obj):
        if isinstance(obj, float) and np.isnan(obj):
            return None
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    with open(args.out, "w") as f:
        json.dump(out, f, indent=2, default=_default)

    print(f"\nWritten → {args.out}")
    _leaderboard(registry)


if __name__ == "__main__":
    main()
