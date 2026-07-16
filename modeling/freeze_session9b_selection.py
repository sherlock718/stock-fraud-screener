"""Freeze the bounded Session 9B selection-compatibility verdict.

The module validates every Session 9 manifest record before reading prediction
tables.  It never fits, rescales, thresholds, or backtests a model.  When no
accepted strategy is complete it emits row-level candidate/exclusion lineage
and an explicit unavailable holding inventory.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SESSION9 = ROOT / "artifacts/pit_validation/session9_corrected_8f"
DEFAULT_ARTIFACT_ROOT = ROOT / "artifacts/pit_validation/session9b_oos_selection_freeze"
REPORT_PATH = ROOT / "reports/pit_validation/09b_oos_selection_freeze.md"
SESSION9_MANIFEST_SHA256 = "bb75e2be97104736786a14c2b6c435fecef5aa335fb41181fe1f1947cb094deb"
BASELINE_COMMIT = "3f706e3e10d2b354c6e8b9407760fa2074749c0a"
POPULATIONS = ("observed_only", "include_policy_imputed")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def record(path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(ROOT).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def validate_session9_manifest() -> dict[str, Any]:
    """Validate the exact manifest and every referenced Session 9 record."""
    manifest_path = SESSION9 / "manifest.json"
    actual = sha256_file(manifest_path)
    if actual != SESSION9_MANIFEST_SHA256:
        raise RuntimeError(f"Session 9 manifest hash mismatch: {actual}")
    manifest = json.loads(manifest_path.read_text())
    checked = {"validated_inputs": 0, "records": 0, "code_lineage": 0}
    for section in checked:
        for item in manifest[section]:
            path = ROOT / item["path"]
            if not path.is_file():
                raise RuntimeError(f"Session 9 {section} file missing: {path}")
            if path.stat().st_size != int(item["size_bytes"]):
                raise RuntimeError(f"Session 9 {section} size mismatch: {path}")
            if sha256_file(path) != item["sha256"]:
                raise RuntimeError(f"Session 9 {section} hash mismatch: {path}")
            checked[section] += 1
    indexed = {item["path"] for item in manifest["records"]}
    dirty = manifest["dirty_state"]["records"]
    for path in dirty:
        if path not in indexed:
            raise RuntimeError(f"Session 9 dirty-state file lacks a record: {path}")
    return {
        "result": "pass", "manifest_sha256": actual, **checked,
        "dirty_state_references": len(dirty), "dirty_state_hashed": len(dirty),
    }


def strategy_inventory() -> list[dict[str, Any]]:
    """Return the reconciled, selection-freeze strategy inventory."""
    common = {
        "model_path": "CORRECTED_8F",
        "target_n": 15,
        "missing_score_behavior": "fail_closed; no smaller portfolio or fallback",
        "holdings_status": "unavailable",
        "selected_count": 0,
    }
    inventory = [
        {
            **common, "strategy_path": "production_ml_gates",
            "required_scores": ["tree_agreement_gate:3y", "regression_ranker:3y"],
            "available_session9_scores": ["calibrated_logistic_classifier:3y", "ridge_regression:3y"],
            "gates": ["US", "$50M<=market_cap<=$10B", "beneish<-1.78", "piotroski>=3",
                      "positive_roa", "sector_ps_pct<=0.70", "tree_prob>=0.55",
                      "altman_z>1.0", "momentum_12m_prior>-0.40"],
            "ranking": "3y regression descending", "weighting": "equal_weight",
            "liquidity_rule": "position below 1% of ADTV",
            "blockers": [
                "missing compatible OOS tree_agreement_gate:3y; Session 9 classification is calibrated logistic",
                "required composite gate fields are not frozen in the Session 9 prediction table",
                "canonical ADTV eligibility is not frozen",
            ],
        },
        {
            **common, "strategy_path": "engine_composite",
            "required_scores": ["classifier_ranker:1y", "classifier_ranker:3y"],
            "available_session9_scores": ["calibrated_logistic_classifier:1y", "calibrated_logistic_classifier:3y"],
            "gates": ["optional market", "beneish<-1.78"],
            "ranking": "rank blend: value .25, quality .20, 1y classifier .30, 3y classifier .15, piotroski .10",
            "weighting": "engine default inverse volatility with position/sector caps",
            "liquidity_rule": "engine default ADTV gate",
            "blockers": ["value_composite and quality_composite are absent from CORRECTED_8F",
                         "canonical ADTV eligibility is not frozen",
                         "no accepted Session 9B market/weighting variant resolves current configuration ambiguity"],
        },
        {
            **common, "strategy_path": "engine_qem",
            "required_scores": ["classifier_ranker:1y"],
            "available_session9_scores": ["calibrated_logistic_classifier:1y"],
            "gates": ["piotroski>=7", "positive EPS growth", "momentum>-0.10", "beneish<-1.78", "positive earnings yield"],
            "ranking": "rank blend: EPS growth .20, quality .25, 1y classifier .25, momentum .15, value .15",
            "weighting": "engine default inverse volatility with position/sector caps",
            "liquidity_rule": "engine default ADTV gate",
            "blockers": ["eps_growth_yoy, value_composite, and quality_composite are absent from CORRECTED_8F",
                         "canonical ADTV eligibility is not frozen"],
        },
        {
            **common, "strategy_path": "engine_scdv",
            "required_scores": ["classifier_ranker:3y"],
            "available_session9_scores": ["calibrated_logistic_classifier:3y"],
            "gates": ["micro/small", "pb<2", "piotroski>=6", "beneish<-1.78", "altman_z>1.81"],
            "ranking": "rank blend: value .35, quality .25, 3y classifier .25, piotroski .15; debt penalty",
            "weighting": "engine default inverse volatility with position/sector caps",
            "liquidity_rule": "engine default ADTV gate",
            "blockers": ["value_composite, quality_composite, PB, Beneish score, and Altman score are absent from CORRECTED_8F",
                         "canonical ADTV eligibility is not frozen"],
        },
        {
            **common, "strategy_path": "engine_iarb",
            "required_scores": ["classifier_ranker:3y"],
            "available_session9_scores": ["calibrated_logistic_classifier:3y"],
            "gates": ["non-US", "pb<1.5", "piotroski>=6", "beneish<-1.78"],
            "ranking": "rank blend: value .30, quality .25, 3y classifier .25, momentum .20; market boost",
            "weighting": "engine default inverse volatility with position/sector caps",
            "liquidity_rule": "engine default ADTV gate",
            "blockers": ["CORRECTED_8F contains only the certified US SEC population",
                         "value_composite, quality_composite, PB, and Beneish score are absent from CORRECTED_8F",
                         "canonical ADTV eligibility is not frozen"],
        },
    ]
    for spec in inventory:
        spec["blockers"].append(
            "accepted entry timestamp is absent from the Session 9 prediction lineage"
        )
    return inventory


def _role_lookup(predictions: pd.DataFrame, horizon: str, model_kind: str) -> pd.DataFrame:
    cols = [
        "stable_row_id", "entity_id", "fiscal_year", "population", "decision_timestamp",
        "prediction_timestamp", "label_end_date", "label_provenance", "fold_id", "eligible",
        "exclusion_reason", "feature_artifact_id", "preprocessing_artifact_id",
        "model_artifact_id", "calibration_artifact_id", "prediction", "rank",
    ]
    out = predictions.loc[
        predictions["horizon"].eq(horizon) & predictions["model_kind"].eq(model_kind), cols
    ].copy()
    if out["stable_row_id"].duplicated().any():
        raise RuntimeError(f"duplicate Session 9 role rows for {horizon}/{model_kind}")
    return out


def build_candidate_table(predictions: pd.DataFrame, inventory: list[dict[str, Any]]) -> pd.DataFrame:
    """Retain every stable row and its exact source-score lineage per path."""
    base = predictions[["stable_row_id", "entity_id", "fiscal_year", "population",
                        "decision_timestamp", "prediction_timestamp"]].drop_duplicates()
    if base["stable_row_id"].duplicated().any():
        raise RuntimeError("Session 9 stable row maps to multiple decision rows")
    outputs = []
    base["entry_timestamp"] = pd.NaT
    base["entry_timestamp_status"] = "unavailable:not_present_in_session9_predictions"
    role_cache: dict[tuple[str, str], pd.DataFrame] = {}
    for spec in inventory:
        frame = base.copy()
        source_keys = []
        for required in spec["required_scores"]:
            role, horizon = required.split(":")
            kind = "regression" if role == "regression_ranker" else "classification"
            key = (horizon, kind)
            if key not in role_cache:
                role_cache[key] = _role_lookup(predictions, horizon, kind)
            prefix = f"{role}_{horizon}__"
            role_frame = role_cache[key].drop(columns=["entity_id", "fiscal_year", "population",
                                                        "decision_timestamp", "prediction_timestamp"])
            role_frame = role_frame.rename(columns={c: prefix + c for c in role_frame.columns
                                                    if c != "stable_row_id"})
            frame = frame.merge(role_frame, on="stable_row_id", how="left", validate="one_to_one")
            source_keys.append(f"{horizon}:{kind}")
        frame["strategy_path"] = spec["strategy_path"]
        frame["selection_status"] = "excluded_path_unavailable"
        frame["exclusion_code"] = "path_unavailable:" + spec["strategy_path"]
        frame["path_blockers"] = json.dumps(spec["blockers"], separators=(",", ":"))
        frame["source_prediction_roles"] = json.dumps(source_keys, separators=(",", ":"))
        frame["selected"] = False
        frame["weight"] = np.nan
        outputs.append(frame)
    return pd.concat(outputs, ignore_index=True)


def freeze_dirty_state(artifact_root: Path) -> list[Path]:
    lineage = artifact_root / "lineage"
    lineage.mkdir(parents=True, exist_ok=True)
    paths = [lineage / "git_status_porcelain.txt", lineage / "tracked_dirty.patch",
             lineage / "untracked_inventory.json"]
    paths[0].write_bytes(subprocess.check_output(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=ROOT))
    paths[1].write_bytes(subprocess.check_output(
        ["git", "diff", "--binary", BASELINE_COMMIT, "--", "."], cwd=ROOT))
    inventory = []
    prefix = artifact_root.relative_to(ROOT).as_posix() + "/"
    for raw in subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=ROOT).split(b"\0"):
        if raw:
            relative = raw.decode()
            path = ROOT / relative
            if path.is_file() and not relative.startswith(prefix):
                inventory.append(record(path, "untracked_worktree_file"))
    _write_json(paths[2], inventory)
    return paths


def write_report(preflight: dict[str, Any], inventory: list[dict[str, Any]],
                 counts: list[dict[str, Any]]) -> None:
    blockers = "\n".join(
        f"- `{spec['strategy_path']}`: " + "; ".join(spec["blockers"])
        for spec in inventory
    )
    candidate_rows = sum(row["candidate_strategy_rows"] for row in counts)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(f"""# Session 9B — OOS Selection Freeze

Date: {datetime.now(timezone.utc).date().isoformat()}

Status: **Complete — frozen unavailable verdict**

## Outcome

No compatible, explicitly named `CORRECTED_8F` selection path is complete, so
Session 9B freezes zero holdings and Session 9C remains blocked. The Session 9
calibrated logistic classifier was not substituted for the production
tree-agreement role, and no threshold, fallback, smaller portfolio, or weight
renormalization was introduced.

## Session 9 preflight

The exact Session 9 manifest `{preflight['manifest_sha256']}` and all
{preflight['validated_inputs']} validated inputs, {preflight['records']}
artifact records, {preflight['code_lineage']} code-lineage records, and
{preflight['dirty_state_references']} hash-covered dirty-state references passed
before any Session 9B artifact was written.

## Reconciled blockers

{blockers}

The production path retains its accepted top-15, descending 3y regression,
equal-weight, hard-gate, and 1%-ADTV configuration as documentation only. It is
not executable because the required OOS tree-agreement probability is absent.
The Session 9 logistic threshold was neither inferred nor optimized.

## Frozen row-level evidence

Both physical population namespaces retain separate candidate/exclusion and
holding tables. The freeze contains {candidate_rows:,} candidate-strategy rows
({counts[0]['candidate_strategy_rows']:,} per namespace), preserves the source
Session 9 fold/label/feature/preprocessing/model/calibration identifiers for
every declared score role, and marks every row with a path-specific unavailable
exclusion. Holding tables are present but empty; no period has a selected row or
weight.

The two namespaces remain economically identical but physically separate.
`include_policy_imputed` added no selection-only rows, so the row/exclusion
identity hashes match without combining the files.

## Explicitly not performed

No model was fitted, calibrated, rescored, or thresholded. No market data was
sourced, no liquidity value was invented, no backtest or performance metric was
run, Session 9C was not started, and no commit or push occurred.
""")


def build(artifact_root: Path = DEFAULT_ARTIFACT_ROOT) -> dict[str, Any]:
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"refusing to reuse non-empty artifact root: {artifact_root}")
    preflight = validate_session9_manifest()
    for name in ("configuration", "inputs", "selections", "holdings", "checkpoints",
                 "support", "lineage"):
        (artifact_root / name).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(SESSION9 / "manifest.json", artifact_root / "inputs/session9_manifest.json")

    inventory = strategy_inventory()
    _write_json(artifact_root / "configuration/strategy_inventory.json", inventory)
    _write_json(artifact_root / "support/preflight_validation.json", preflight)
    counts = []
    population_hashes = {}
    for population in POPULATIONS:
        source = SESSION9 / f"predictions/{population}/oos_predictions.parquet"
        predictions = pd.read_parquet(source)
        if not predictions["population"].eq(population).all():
            raise RuntimeError(f"Session 9 population namespace drifted: {population}")
        candidates = build_candidate_table(predictions, inventory)
        path = artifact_root / f"selections/{population}/candidate_exclusions.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        candidates.to_parquet(path, index=False)
        empty_holdings = candidates.iloc[0:0][[
            "stable_row_id", "entity_id", "fiscal_year", "population", "decision_timestamp",
            "prediction_timestamp", "entry_timestamp", "entry_timestamp_status",
            "strategy_path", "selected", "weight",
        ]]
        holding_path = artifact_root / f"holdings/{population}/holdings.parquet"
        holding_path.parent.mkdir(parents=True, exist_ok=True)
        empty_holdings.to_parquet(holding_path, index=False)
        counts.append({"population": population, "source_prediction_rows": len(predictions),
                       "candidate_strategy_rows": len(candidates), "selected_holdings": 0,
                       "unavailable_paths": len(inventory)})
        identity_cols = ["stable_row_id", "fiscal_year", "strategy_path", "selection_status", "exclusion_code"]
        hashed = pd.util.hash_pandas_object(candidates[identity_cols], index=False, categorize=True)
        population_hashes[population] = hashlib.sha256(hashed.to_numpy(dtype="uint64").tobytes()).hexdigest()
        _write_json(artifact_root / f"checkpoints/{population}.json", {
            "completed": True, "population": population,
            "source_prediction_rows": len(predictions),
            "candidate_strategy_rows": len(candidates), "strategy_paths": len(inventory),
            "selected_holdings": 0, "candidate_identity_hash": population_hashes[population],
            "entry_timestamp_status": "unavailable:not_present_in_session9_predictions",
        })
    pd.DataFrame(counts).to_parquet(artifact_root / "support/coverage_summary.parquet", index=False)

    identity = {
        "physically_separate_outputs": True,
        "candidate_identity_hash_observed_only": population_hashes["observed_only"],
        "candidate_identity_hash_include_policy_imputed": population_hashes["include_policy_imputed"],
        "candidate_exclusions_identical": population_hashes["observed_only"] == population_hashes["include_policy_imputed"],
        "selected_holdings_each": 0,
    }
    _write_json(artifact_root / "support/population_identity.json", identity)
    verdict = {
        "status": "unavailable", "complete_paths": [], "session9c_unblocked": False,
        "reason": "no explicitly named CORRECTED_8F path has every accepted model role, gate, ranking input, liquidity rule, target_n, and weighting input",
        "logistic_substituted_for_tree": False, "thresholds_invented_or_optimized": False,
        "models_refit_or_rescored": False, "market_data_sourced": False,
        "backtest_run": False, "session9c_started": False,
    }
    _write_json(artifact_root / "support/frozen_unavailable_verdict.json", verdict)
    write_report(preflight, inventory, counts)
    created_at = datetime.now(timezone.utc).isoformat()
    configuration = {
        "schema_version": 1, "session": "9B", "created_at_utc": created_at,
        "model_path": "CORRECTED_8F", "populations": list(POPULATIONS),
        "target_n": 15, "official_portfolio_requires_full_target_n": True,
        "selection_result": "unavailable", "holdings_frozen": 0,
        "source_scores": "explicit Session 9 OOS predictions only",
    }
    _write_json(artifact_root / "configuration/config.json", configuration)
    lineage_paths = freeze_dirty_state(artifact_root)
    code_paths = [ROOT / "modeling/freeze_session9b_selection.py",
                  ROOT / "tests/modeling/test_freeze_session9b_selection.py"]
    contract_paths = [ROOT / "reports/pit_validation/05a_backtest_contract.md",
                      ROOT / "docs/PRODUCTION_CONFIG.md", ROOT / "backtest/engine.py"]
    artifact_files = sorted(path for path in artifact_root.rglob("*")
                            if path.is_file() and path.name != "manifest.json")
    manifest = {
        "schema_version": 1, "artifact_class": "SESSION9B_OOS_SELECTION_FREEZE",
        "created_at_utc": created_at, "baseline_commit": BASELINE_COMMIT,
        "current_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "claim": verdict, "preflight": preflight, "configuration": configuration,
        "strategy_inventory": inventory, "population_identity": identity,
        "validated_inputs": [record(SESSION9 / "manifest.json", "session9_exact_manifest")],
        "records": [record(path, "session9b_configuration_selection_holding_or_lineage") for path in artifact_files],
        "code_lineage": [record(path, "session9b_code_or_test") for path in code_paths],
        "contract_lineage": [record(path, "accepted_strategy_contract_or_configuration") for path in contract_paths],
        "deliverables": [record(REPORT_PATH, "session9b_report")],
        "dirty_state": {"baseline": BASELINE_COMMIT, "complete_status_recorded": True,
                        "records": [path.relative_to(ROOT).as_posix() for path in lineage_paths]},
        "environment": {"python": platform.python_version(), "numpy": np.__version__, "pandas": pd.__version__},
    }
    manifest_path = artifact_root / "manifest.json"
    _write_json(manifest_path, manifest)
    return {"manifest": manifest_path, "manifest_sha256": sha256_file(manifest_path),
            "complete_paths": 0, "selected_holdings": 0,
            "candidate_strategy_rows": sum(row["candidate_strategy_rows"] for row in counts)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    args = parser.parse_args()
    result = build(args.artifact_root.resolve())
    print(json.dumps({key: str(value) if isinstance(value, Path) else value
                      for key, value in result.items()}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
