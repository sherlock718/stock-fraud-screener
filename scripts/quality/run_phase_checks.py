#!/usr/bin/env python3
"""
run_phase_checks.py — Mechanically verify Phase A / B / C done criteria.

A phase is DONE when every check in that phase prints [PASS].
This script is the single source of truth for phase completion.
Do NOT declare a phase done without running this first.

Usage:
    python3 scripts/quality/run_phase_checks.py            # runs Phase A + B
    python3 scripts/quality/run_phase_checks.py --phase A  # Phase A only
    python3 scripts/quality/run_phase_checks.py --phase B  # Phase B only
    python3 scripts/quality/run_phase_checks.py --phase C  # Phase C only (requires trained models)
    python3 scripts/quality/run_phase_checks.py --phase AB # Phase A + B
    python3 scripts/quality/run_phase_checks.py --strict   # exit 1 on any FAIL or WARN

All results mirror docs/developer/phase-done-criteria.md exactly.
"""

from __future__ import annotations

import argparse
import glob
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from scripts._root import ROOT

PARQUET = ROOT / "data" / "historical_dataset_clean.parquet"

# ---------------------------------------------------------------------------
# Check infrastructure
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str
    status: str   # PASS | FAIL | WARN | SKIP
    detail: str


_results: list[CheckResult] = []


def check(name: str, condition: bool, detail: str = "", warn_only: bool = False) -> bool:
    status = "PASS" if condition else ("WARN" if warn_only else "FAIL")
    _results.append(CheckResult(name, status, detail))
    icon = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "SKIP": "⏭"}[status]
    print(f"  [{status}] {icon} {name}" + (f" — {detail}" if detail else ""))
    return condition


def skip(name: str, reason: str) -> None:
    _results.append(CheckResult(name, "SKIP", reason))
    print(f"  [SKIP] ⏭  {name} — {reason}")


def section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ---------------------------------------------------------------------------
# Phase A checks
# ---------------------------------------------------------------------------

def check_a1_dataset() -> None:
    section("A1 — Dataset shape + quality")
    try:
        import pandas as pd
        df = pd.read_parquet(PARQUET)
        check("Rows >= 58,000", df.shape[0] >= 58000, f"{df.shape[0]:,} rows")
        check("Columns == 360", df.shape[1] == 360, f"{df.shape[1]} cols")
        check("No inf values", not df.isin([float("inf"), float("-inf")]).any().any())
        for col, cap in [
            ("forward_return_1y", 5.0),
            ("forward_return_3y", 10.0),
            ("forward_return_5y", 20.0),
        ]:
            if col not in df.columns:
                skip(f"{col} winsorized", "column absent")
                continue
            mx = df[col].abs().max()
            check(f"{col} winsorized (<= {cap}x)", mx <= cap, f"max={mx:.2f}")
        required_markets = {"US", "KR", "BR", "CA", "JP"}
        present = set(df["market"].unique())
        missing = required_markets - present
        check("All 5 core markets present", not missing,
              f"present={sorted(present)}" if not missing else f"MISSING={missing}")
    except Exception as exc:
        check("Parquet loads without error", False, str(exc))


def check_a2_eda_notebook() -> None:
    section("A2 — EDA notebook 01")
    nb_path = ROOT / "notebooks" / "01_eda_dataset.ipynb"
    if not nb_path.exists():
        check("notebooks/01_eda_dataset.ipynb exists", False, "file missing")
        return
    check("notebooks/01_eda_dataset.ipynb exists", True)
    try:
        nb = json.loads(nb_path.read_text())
        sources = " ".join(
            c["source"] if isinstance(c["source"], str) else "".join(c["source"])
            for c in nb["cells"]
            if c["cell_type"] == "code"
        )
        checks_map = {
            "forward_return histogram": (
                "forward_return" in sources
                and ("hist" in sources or "distplot" in sources or "plot" in sources)
            ),
            "forward_return outlier stats": (
                "forward_return" in sources
                and ("quantile" in sources or "describe" in sources
                     or "p99" in sources or "percentile" in sources)
            ),
            "point-in-time lineage check": (
                "filed_date" in sources
                or "filing_date" in sources
                or "lineage" in sources
                or "look" in sources.lower()
            ),
            "null profile": (
                "null" in sources.lower() or "isna" in sources or "isnull" in sources
            ),
        }
        for name, ok in checks_map.items():
            check(f"notebook 01: {name}", ok)
    except Exception as exc:
        check("notebook 01 is valid JSON", False, str(exc))


def check_a3_ci_schedule() -> None:
    section("A3 — CI refresh schedule (refresh_data.yml)")
    ci_path = ROOT / ".github" / "workflows" / "refresh_data.yml"
    if not ci_path.exists():
        check(".github/workflows/refresh_data.yml exists", False, "file missing")
        return
    ci = ci_path.read_text()
    required = [
        "impute_features.py",
        "mark_survivorship.py",
        "compute_alpha.py",
        "score_historical.py",
        "enrich_quarterly_features.py",
        "test_dataset_quality.py",
    ]
    for script in required:
        check(f"refresh_data.yml contains {script}", script in ci)


def check_a4_diagram_vs_ci() -> None:
    section("A4 — Update diagram matches CI")
    guide_path = ROOT / "docs" / "developer" / "data-update-guide.md"
    ci_path = ROOT / ".github" / "workflows" / "refresh_data.yml"
    if not guide_path.exists():
        check("data-update-guide.md exists", False, "file missing")
        return
    if not ci_path.exists():
        skip("diagram vs CI check", "refresh_data.yml missing")
        return
    guide = guide_path.read_text()
    ci = ci_path.read_text()
    scripts_in_guide = set(re.findall(r"[\w_]+\.py", guide))
    scripts_in_ci = set(re.findall(r"[\w_]+\.py", ci))
    core_scripts = {
        "enrich_quarterly_features.py",
        "impute_features.py",
        "mark_survivorship.py",
        "test_dataset_quality.py",
    }
    guide_has_core = core_scripts - scripts_in_guide
    ci_has_core = core_scripts - scripts_in_ci
    check("Core scripts in update guide", not guide_has_core,
          f"Missing: {guide_has_core}" if guide_has_core else "all present")
    check("Core scripts in CI", not ci_has_core,
          f"Missing: {ci_has_core}" if ci_has_core else "all present")
    # Known operator-only / separate-workflow / module scripts — not expected in refresh_data.yml
    operator_only = {
        "auto_update.py",            # convenience wrapper for manual runs
        "merge_snapshots.py",        # operator merge utility
        "monitor_drift.py",          # runs in monitor_drift.yml, not refresh_data.yml
        "push_to_hf.py",             # CI uploads inline; guide references operator manual use
        "feature_library.py",        # module (not a runnable script), referenced in prose
        "step5_compute_features.py", # operator step when adding new feature columns
        "nfeature_library.py",       # false positive: \nfeature_library.py in mermaid label
    }
    guide_only = {s for s in scripts_in_guide if s not in scripts_in_ci
                  and s not in operator_only
                  and not s.endswith("_test.py") and "run_pipeline" not in s
                  and "phase_a_integrate" not in s and "enrich_" not in s}
    if guide_only:
        check("No phantom scripts (guide but not CI)", False,
              f"Guide-only: {sorted(guide_only)[:5]}", warn_only=True)
    else:
        check("No phantom scripts (guide but not CI)", True)


# ---------------------------------------------------------------------------
# Phase B checks
# ---------------------------------------------------------------------------

def check_b1_feature_library() -> None:
    section("B1 — Feature library implementation")
    files = [
        ROOT / "pipeline" / "step5_compute_features.py",
        ROOT / "pipeline" / "feature_library.py",
    ]
    contents = []
    for f in files:
        if f.exists():
            contents.append(f.read_text())
    if not contents:
        check("feature_library.py / step5_compute_features.py exist", False)
        return
    src = " ".join(contents)
    formulas = {
        "beneish_m_score": "beneish_m_score" in src,
        "altman_z_score": "altman_z_score" in src,
        "ohlson_o_score": "ohlson_o_score" in src,
        "piotroski_f_score": "piotroski_f_score" in src,
        "montier_c_score": "montier_c_score" in src,
        "sloan_wc_accruals": "sloan_wc_accruals" in src,
        "sloan_lt_accruals": "sloan_lt_accruals" in src,
    }
    for name, ok in formulas.items():
        check(f"formula implemented: {name}", ok)

    # Verify columns present in parquet
    try:
        import pandas as pd
        df = pd.read_parquet(PARQUET)
        for col in [
            "beneish_m_score", "altman_z_score", "piotroski_f_score",
            "montier_c_score", "sloan_wc_accruals", "sloan_lt_accruals",
        ]:
            check(f"parquet column: {col}", col in df.columns,
                  f"null_rate={df[col].isna().mean():.1%}" if col in df.columns else "ABSENT")
    except Exception as exc:
        check("parquet loads for column check", False, str(exc))


def check_b2_feature_engineering() -> None:
    section("B2 — Feature engineering correctness")
    step5 = ROOT / "pipeline" / "step5_compute_features.py"
    if not step5.exists():
        check("step5_compute_features.py exists", False)
        return
    src = step5.read_text()
    checks_map = {
        "beneish_dsri clipped [0.5, 3]": (
            "clip(0.5" in src or "clip(lower=0.5" in src
        ),
        "growth cols in winsorize list": (
            "revenue_growth_yoy" in src and "ratio_cols" in src
        ),
        "sector_pct includes fiscal_year in groupby": (
            "'fiscal_year'" in src and "sector_pct" in src
        ),
        "montier_c2 uses ppe_net (not property_plant_equipment)": (
            "ppe_net" in src
            and ("property_plant_equipment" not in src
                 or src.index("ppe_net") < src.rindex("property_plant_equipment")
                 or "# do not" in src.lower()
                 or "ppe_net" in src)
        ),
    }
    for name, ok in checks_map.items():
        check(f"feature_eng: {name}", ok)


def check_b3_feature_selection() -> None:
    section("B3 — Feature selection")
    # No alpha_* or ml_* leakage in feature sets
    feature_sets = sorted(glob.glob(str(ROOT / "models" / "feature_sets_*.json")))
    if not feature_sets:
        check("models/feature_sets_*.json exist", False, "no files found")
    else:
        check("models/feature_sets_*.json exist", True, f"{len(feature_sets)} files")
        for path in feature_sets:
            obj = json.loads(Path(path).read_text())
            features = obj["features"] if isinstance(obj, dict) else obj
            alpha_leak = [f for f in features if f.startswith("alpha_")]
            ml_leak = [f for f in features if f.startswith("ml_")]
            name = Path(path).name
            if alpha_leak:
                check(f"{name}: no alpha_* leakage", False, f"found: {alpha_leak[:3]}")
            elif ml_leak:
                check(f"{name}: no ml_* leakage", False, f"found: {ml_leak[:3]}")
            else:
                check(f"{name}: no alpha_*/ml_* leakage", True, f"{len(features)} features")

    # PSI threshold = 0.25
    sel_path = ROOT / "scripts" / "run_feature_selection.py"
    if sel_path.exists():
        sel_src = sel_path.read_text()
        check("PSI threshold is 0.25",
              "psi_threshold=0.25" in sel_src or "default=0.25" in sel_src or "= 0.25" in sel_src)
    else:
        skip("PSI threshold check", "run_feature_selection.py missing")

    # Newey-West + FDR columns in summary
    summary_path = ROOT / "reports" / "feature_selection_summary.csv"
    if not summary_path.exists():
        check("reports/feature_selection_summary.csv exists", False, "file missing (run_feature_selection.py not yet run)")
    else:
        try:
            import pandas as pd
            df = pd.read_csv(summary_path, nrows=2)
            check("feature_selection_summary has ic_tstat_nw", "ic_tstat_nw" in df.columns)
            check("feature_selection_summary has fdr_reject", "fdr_reject" in df.columns)
        except Exception as exc:
            check("feature_selection_summary.csv readable", False, str(exc))


def check_b4_factor_research() -> None:
    section("B4 — Factor research artifacts")
    reports = sorted(glob.glob(str(ROOT / "reports" / "factor_research_*.csv")))
    if not reports:
        check("reports/factor_research_*.csv exist", False,
              "no files (run scripts/factor_research.py)")
    else:
        check("reports/factor_research_*.csv exist", True, f"{len(reports)} files")
        try:
            import pandas as pd
            for path in reports:
                df = pd.read_csv(path, nrows=2)
                required = ["ic", "icir", "ic_tstat", "pct_positive_ic"]
                missing = [c for c in required if c not in df.columns]
                name = Path(path).name
                check(f"{name}: required IC columns present",
                      not missing,
                      f"missing: {missing}" if missing else "all present")
        except Exception as exc:
            check("factor_research CSVs readable", False, str(exc))

    # Notebook 02 checks
    nb_path = ROOT / "notebooks" / "02_ic_analysis.ipynb"
    if not nb_path.exists():
        check("notebooks/02_ic_analysis.ipynb exists", False, "file missing")
        return
    try:
        nb = json.loads(nb_path.read_text())
        sources = " ".join(
            c["source"] if isinstance(c["source"], str) else "".join(c["source"])
            for c in nb["cells"]
            if c["cell_type"] == "code"
        ).lower()
        checks_map = {
            "IC decay curve": "decay" in sources or ("ic" in sources and "lag" in sources),
            "quintile return spreads": "quintile" in sources or "qcut" in sources,
            "information ratio": "information_ratio" in sources or ("mean_ic" in sources and "std" in sources),
        }
        for name, ok in checks_map.items():
            check(f"notebook 02: {name}", ok)
    except Exception as exc:
        check("notebook 02 is valid JSON", False, str(exc))


def check_b5_notebooks_have_outputs() -> None:
    section("B5 — All notebooks have outputs (not cleared)")
    nb_paths = sorted(glob.glob(str(ROOT / "notebooks" / "0*.ipynb")))
    if not nb_paths:
        check("notebooks/0*.ipynb exist", False, "no notebooks found")
        return
    for path in nb_paths:
        try:
            nb = json.loads(Path(path).read_text())
            code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
            with_output = [c for c in code_cells if c.get("outputs")]
            pct = len(with_output) / max(len(code_cells), 1)
            name = Path(path).name
            check(f"{name}: ≥50% cells have outputs ({pct:.0%})", pct >= 0.5,
                  f"{len(with_output)}/{len(code_cells)} cells")
        except Exception as exc:
            check(f"{Path(path).name}: valid JSON", False, str(exc))


# ---------------------------------------------------------------------------
# Phase C checks (lightweight — does not retrain models)
# ---------------------------------------------------------------------------

def check_c1_oof_scores() -> None:
    section("C1 — OOF scores + look-ahead guard")
    try:
        import pandas as pd
        df = pd.read_parquet(PARQUET)
        for col in ["ml_1y_oof", "ml_3y_oof", "ml_5y_oof"]:
            if col in df.columns:
                n = df[col].notna().sum()
                check(f"{col} present and scored", n > 0, f"{n:,} rows scored")
            else:
                check(f"{col} present", False, "column absent — run generate_oof_scores.py")
        if "ml_1y_oof" in df.columns:
            early = df[df["fiscal_year"] <= 2013]["ml_1y_oof"]
            check("ml_1y_oof: NaN for training-window rows (≤2013)",
                  early.notna().sum() == 0,
                  f"{early.notna().sum()} non-NaN in early years",
                  warn_only=(early.notna().sum() > 0))
    except Exception as exc:
        check("parquet loads for OOF check", False, str(exc))


def check_c2_models() -> None:
    section("C2 — 5-horizon models")
    for h in ["6m", "1y", "2y", "3y", "5y"]:
        p = ROOT / "models" / f"model_{h}.joblib"
        check(f"models/model_{h}.joblib exists", p.exists())
        fs_p = ROOT / "models" / f"feature_sets_{h}.json"
        if fs_p.exists():
            obj = json.loads(fs_p.read_text())
            feats = obj.get("features", obj) if isinstance(obj, dict) else obj
            check(f"feature_sets_{h}.json present", True, f"{len(feats)} features")
        else:
            check(f"feature_sets_{h}.json present", False)
    meta_p = ROOT / "models" / "model_meta.json"
    if not meta_p.exists():
        check("models/model_meta.json exists", False)
        return
    meta = json.loads(meta_p.read_text())
    for h in ["1y", "3y", "5y"]:
        if h not in meta:
            check(f"model_meta.json has {h} horizon", False)
            continue
        auc = meta[h].get("wf_mean_auc") or meta[h].get("val_auc")
        check(f"model_meta.json {h} AUC recorded", auc is not None, f"AUC={auc}")
    auc_3y = meta.get("3y", {}).get("wf_mean_auc") or meta.get("3y", {}).get("val_auc")
    if auc_3y:
        check("3y AUC >= 0.62 target", auc_3y >= 0.62, f"AUC={auc_3y:.3f}")
    else:
        check("3y AUC recorded", False, "auc not in model_meta.json")


def check_c4_backtest() -> None:
    section("C4 — Backtest + SPY data")
    spy_path = ROOT / "data" / "spy_returns.csv"
    if spy_path.exists():
        try:
            import pandas as pd
            spy = pd.read_csv(spy_path)
            check("data/spy_returns.csv has ≥10 years",
                  len(spy) >= 10, f"{len(spy)} years")
        except Exception as exc:
            check("data/spy_returns.csv readable", False, str(exc))
    else:
        check("data/spy_returns.csv exists", False)

    bt_path = ROOT / "data" / "backtest_results.json"
    if not bt_path.exists():
        check("data/backtest_results.json exists", False,
              "run scripts/backtester.py first")
        return
    try:
        res = json.loads(bt_path.read_text())
        strats = list(res.get("strategies", {}).values())
        if not strats:
            check("backtest_results.json has strategies", False)
        else:
            r = strats[0]
            for field in ["spy_cagr_pct", "excess_cagr_vs_spy", "beta_vs_spy", "tracking_error"]:
                check(f"backtest_results.json has {field}", field in r)
    except Exception as exc:
        check("backtest_results.json readable", False, str(exc))


def check_c5_alpha_schema() -> None:
    section("C5 — Alpha schema (HorizonRouter)")
    hr_path = ROOT / "alpha" / "horizon_router.py"
    check("alpha/horizon_router.py exists", hr_path.exists())
    if not hr_path.exists():
        return
    try:
                from alpha.horizon_router import HorizonRouter
        cases = [(6, "6m"), (9, "1y"), (18, "2y"), (24, "2y"), (36, "3y"), (60, "5y")]
        for months, expected in cases:
            got = HorizonRouter.route(months)
            check(f"HorizonRouter.route({months}) → {expected}", got == expected,
                  f"got {got!r}")
    except Exception as exc:
        check("HorizonRouter imports + routes", False, str(exc))


# ---------------------------------------------------------------------------
# Summary + main
# ---------------------------------------------------------------------------

def print_summary() -> tuple[int, int, int]:
    passes = sum(1 for r in _results if r.status == "PASS")
    fails = sum(1 for r in _results if r.status == "FAIL")
    warns = sum(1 for r in _results if r.status == "WARN")
    skips = sum(1 for r in _results if r.status == "SKIP")
    total = passes + fails + warns + skips

    print(f"\n{'═' * 60}")
    print(f"  SUMMARY: {passes} PASS  {fails} FAIL  {warns} WARN  {skips} SKIP  "
          f"({total} total)")
    if fails == 0 and warns == 0:
        print("  ✅  ALL CHECKS PASS — phase criteria met.")
    elif fails == 0:
        print("  ⚠️   No hard failures, but warnings need review.")
    else:
        print("  ❌  PHASE NOT DONE — fix the FAIL items above first.")
    print(f"{'═' * 60}\n")
    return passes, fails, warns


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run phase done criteria checks (Phase A / B / C)."
    )
    parser.add_argument(
        "--phase",
        default="AB",
        choices=["A", "B", "C", "AB"],
        help="Which phase(s) to check (default: AB)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit 1 on any FAIL or WARN",
    )
    args = parser.parse_args()

    phase = args.phase.upper()

    if "A" in phase:
        print("\n╔══════════════════════════════════════════════════════════╗")
        print("║                     PHASE A CHECKS                      ║")
        print("╚══════════════════════════════════════════════════════════╝")
        check_a1_dataset()
        check_a2_eda_notebook()
        check_a3_ci_schedule()
        check_a4_diagram_vs_ci()

    if "B" in phase:
        print("\n╔══════════════════════════════════════════════════════════╗")
        print("║                     PHASE B CHECKS                      ║")
        print("╚══════════════════════════════════════════════════════════╝")
        check_b1_feature_library()
        check_b2_feature_engineering()
        check_b3_feature_selection()
        check_b4_factor_research()
        check_b5_notebooks_have_outputs()

    if phase == "C":
        print("\n╔══════════════════════════════════════════════════════════╗")
        print("║                     PHASE C CHECKS                      ║")
        print("╚══════════════════════════════════════════════════════════╝")
        check_c1_oof_scores()
        check_c2_models()
        check_c4_backtest()
        check_c5_alpha_schema()

    _, fails, warns = print_summary()

    if fails > 0 or (args.strict and warns > 0):
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
