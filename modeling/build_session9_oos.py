"""Build the bounded Session 9 CORRECTED_8F fold-local OOS artifacts.

This module consumes only the physically separate Session 8F feature/label
populations.  It validates the complete Session 8F manifest before reading any
modeling input, excludes macro features, and emits row-complete OOS prediction
tables without running a backtest or constructing a portfolio.
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

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.calibration import CalibratedClassifierCV
from sklearn.frozen import FrozenEstimator
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.preprocessing import StandardScaler

from modeling.fold_lineage import SelectorConfig, select_fold_features


ROOT = Path(__file__).resolve().parents[1]
SESSION8F = ROOT / "artifacts/pit_validation/corrected_feature_population"
DEFAULT_ARTIFACT_ROOT = ROOT / "artifacts/pit_validation/session9_corrected_8f"
SESSION8F_MANIFEST_SHA256 = "9c1e4b82c2e7bc2a85228adf6668b797acec827e2bd1ff7c58d20cfb6a9ac01a"
BASELINE_COMMIT = "3f706e3e10d2b354c6e8b9407760fa2074749c0a"
HORIZONS = ("6m", "1y", "2y", "3y", "5y")
POPULATIONS = ("observed_only", "include_policy_imputed")
MODEL_KINDS = ("classification", "regression")
RANDOM_SEED = 42
MIN_DEVELOPMENT_ROWS = 100
MIN_CALIBRATION_ROWS = 30
SELECTOR = SelectorConfig(top_n=30, min_abs_ic=0.01, min_ic_years=1,
                          min_group_size=30, corr_threshold=0.85)
MACRO_FEATURES = frozenset({
    "treasury_3m", "treasury_2y", "treasury_5y", "treasury_10y", "treasury_30y",
    "yield_curve", "fed_funds", "fed_funds_rate", "credit_spread", "credit_spread_baa",
    "hy_spread", "cpi", "inflation", "unemployment", "recession", "vix",
    "real_rate", "credit_tighten", "macro_regime", "value_in_high_rate",
    "value_in_recession", "momentum_in_expansion", "quality_in_recession",
    "levered_in_tight_credit",
})


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


def validate_session8f_manifest() -> dict[str, Any]:
    """Validate every Session 8F input, output, code, and dirty-state record."""
    manifest_path = SESSION8F / "manifest.json"
    actual = sha256_file(manifest_path)
    if actual != SESSION8F_MANIFEST_SHA256:
        raise RuntimeError(f"Session 8F manifest hash mismatch: {actual}")
    manifest = json.loads(manifest_path.read_text())
    checked = {"validated_inputs": 0, "records": 0, "code_lineage": 0}
    indexed_records = {item["path"]: item for item in manifest["records"]}
    for section in checked:
        for item in manifest[section]:
            path = ROOT / item["path"]
            if not path.is_file():
                raise RuntimeError(f"Session 8F {section} file missing: {path}")
            if "size_bytes" in item and path.stat().st_size != int(item["size_bytes"]):
                raise RuntimeError(f"Session 8F {section} size mismatch: {path}")
            if sha256_file(path) != item["sha256"]:
                raise RuntimeError(f"Session 8F {section} hash mismatch: {path}")
            checked[section] += 1
    dirty_paths = manifest["dirty_state"]["records"]
    for path in dirty_paths:
        if path not in indexed_records:
            raise RuntimeError(f"Session 8F dirty-state file lacks a hash record: {path}")
    return {
        "result": "pass",
        "manifest_sha256": actual,
        **checked,
        "dirty_state_references": len(dirty_paths),
        "dirty_state_hashed": len(dirty_paths),
    }


def _frame_hash(frame: pd.DataFrame, columns: list[str]) -> str:
    ordered = frame[columns].sort_values(columns, kind="stable").reset_index(drop=True)
    values = pd.util.hash_pandas_object(ordered, index=False, categorize=True)
    digest = hashlib.sha256(json.dumps(columns, separators=(",", ":")).encode())
    digest.update(values.to_numpy(dtype="uint64", copy=False).tobytes())
    return digest.hexdigest()


def _load_population(population: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    features = pd.read_parquet(SESSION8F / f"outputs/{population}/features_taxonomy.parquet")
    gates = pd.read_parquet(SESSION8F / f"inputs/{population}_row_horizon.parquet")
    labels = pd.read_parquet(SESSION8F / f"inputs/{population}_labels.parquet")
    if not features["population"].eq(population).all():
        raise RuntimeError(f"feature namespace drifted for {population}")
    if features["stable_row_id"].isna().any() or features["stable_row_id"].duplicated().any():
        raise RuntimeError(f"stable row identity invalid for {population}")
    expected_horizons = set(HORIZONS)
    if set(gates["horizon"]) != expected_horizons or set(labels["horizon"]) != expected_horizons:
        raise RuntimeError(f"horizon support drifted for {population}")
    return features, gates, labels


def validate_population_identity(
    observed: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame],
    policy: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame],
) -> dict[str, Any]:
    of, og, ol = observed
    pf, pg, pl = policy
    feature_cols = ["stable_row_id", "entity_id", "fiscal_year", "decision_timestamp",
                    "price_feature_status", "price_feature_reason"]
    gate_cols = ["entity_id", "fiscal_year", "horizon", "decision_timestamp",
                 "classification", "reason"]
    label_cols = ["entity_id", "fiscal_year", "horizon", "decision_timestamp",
                  "label_end_date", "relative_return", "outperformed_benchmark",
                  "label_provenance", "policy_imputed"]
    result = {
        "physically_separate_sources": True,
        "feature_rows_each": len(of),
        "gate_rows_each": len(og),
        "label_rows_each": len(ol),
        "feature_identity_hash_observed": _frame_hash(of, feature_cols),
        "feature_identity_hash_policy": _frame_hash(pf, feature_cols),
        "gate_identity_hash_observed": _frame_hash(og, gate_cols),
        "gate_identity_hash_policy": _frame_hash(pg, gate_cols),
        "label_identity_hash_observed": _frame_hash(ol, label_cols),
        "label_identity_hash_policy": _frame_hash(pl, label_cols),
    }
    result["feature_rows_identical"] = (
        result["feature_identity_hash_observed"] == result["feature_identity_hash_policy"]
    )
    result["gate_rows_identical"] = (
        result["gate_identity_hash_observed"] == result["gate_identity_hash_policy"]
    )
    result["label_rows_identical"] = (
        result["label_identity_hash_observed"] == result["label_identity_hash_policy"]
    )
    if not all(result[key] for key in
               ("feature_rows_identical", "gate_rows_identical", "label_rows_identical")):
        raise RuntimeError("Session 8F populations are no longer economically identical")
    return result


def _assemble_population(population: str, features: pd.DataFrame,
                         gates: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    keys = ["entity_id", "fiscal_year"]
    gate = gates.rename(columns={"classification": "gate_classification",
                                 "reason": "gate_reason"}).copy()
    overlaps = [column for column in features.columns if column in gate.columns and column not in keys]
    base = gate.merge(features.drop(columns=overlaps), on=keys, how="left", validate="many_to_one")
    label_fields = [
        "entity_id", "fiscal_year", "horizon", "decision_timestamp", "label_end_date",
        "relative_return", "outperformed_benchmark", "label_provenance", "policy_imputed",
    ]
    lab = labels[label_fields].rename(columns={"decision_timestamp": "label_decision_timestamp"})
    base = base.merge(lab, on=["entity_id", "fiscal_year", "horizon"], how="left",
                      validate="one_to_one")
    base["decision_timestamp"] = pd.to_datetime(base["decision_timestamp"], utc=True)
    base["label_decision_timestamp"] = pd.to_datetime(
        base["label_decision_timestamp"], utc=True, errors="coerce"
    )
    base["label_end_date"] = pd.to_datetime(base["label_end_date"], utc=True, errors="coerce")
    timestamp_mismatch = (
        base["label_decision_timestamp"].notna()
        & base["decision_timestamp"].ne(base["label_decision_timestamp"])
    )
    if timestamp_mismatch.any():
        raise RuntimeError(f"Session 8E label decision timestamps drifted for {population}")
    base["population"] = population
    base["model_path"] = "CORRECTED_8F"
    base["base_eligible"] = (
        base["gate_classification"].eq("supported")
        & base["price_feature_status"].eq("supported")
        & base["label_end_date"].notna()
        & base["label_provenance"].notna()
        & base["relative_return"].notna()
        & base["outperformed_benchmark"].notna()
    )
    base["base_exclusion_reason"] = ""
    unsupported_gate = ~base["gate_classification"].eq("supported")
    base.loc[unsupported_gate, "base_exclusion_reason"] = (
        "session8e_" + base.loc[unsupported_gate, "gate_classification"].astype(str)
        + ":" + base.loc[unsupported_gate, "gate_reason"].astype(str)
    )
    unsupported_price = ~base["price_feature_status"].eq("supported") & ~unsupported_gate
    base.loc[unsupported_price, "base_exclusion_reason"] = (
        "session8f_price_" + base.loc[unsupported_price, "price_feature_status"].astype(str)
        + ":" + base.loc[unsupported_price, "price_feature_reason"].astype(str)
    )
    missing_label = base["base_exclusion_reason"].eq("") & ~base["base_eligible"]
    base.loc[missing_label, "base_exclusion_reason"] = "missing_session8e_certified_label"
    if base.duplicated(["stable_row_id", "horizon"]).any():
        raise RuntimeError(f"row/horizon identity collision for {population}")
    return base


def _artifact_id(path: Path) -> str:
    return f"sha256:{sha256_file(path)}"


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def _fit_model(
    train: pd.DataFrame,
    score: pd.DataFrame,
    population: str,
    horizon: str,
    fold_id: str,
    model_kind: str,
    output_root: Path,
) -> tuple[np.ndarray | None, dict[str, Any], str | None]:
    target = "outperformed_benchmark" if model_kind == "classification" else "relative_return"
    train_times = sorted(train["decision_timestamp"].dropna().unique())
    if len(train_times) < 2:
        return None, {}, "insufficient_historical_cohorts"
    calibration_time = train_times[-1]
    development = train[train["decision_timestamp"].lt(calibration_time)].copy()
    calibration = train[train["decision_timestamp"].eq(calibration_time)].copy()
    if len(development) < MIN_DEVELOPMENT_ROWS:
        return None, {}, "insufficient_development_rows"
    if model_kind == "classification":
        if len(calibration) < MIN_CALIBRATION_ROWS:
            return None, {}, "insufficient_calibration_rows"
        if development[target].nunique() < 2 or calibration[target].nunique() < 2:
            return None, {}, "insufficient_class_variation"

    # The Session 8E label field names are not part of the legacy modeling
    # exclusion patterns. Remove both outcomes from the selector candidates and
    # expose only a deliberately excluded-pattern target alias.
    selector_target = (
        f"_session9_beat_local_market_{horizon}"
        if model_kind == "classification"
        else f"_session9_excess_return_local_{horizon}"
    )
    selection_frame = development.drop(
        columns=["relative_return", "outperformed_benchmark"], errors="ignore"
    ).copy()
    selection_frame[selector_target] = development[target].to_numpy()
    features = select_fold_features(selection_frame, selector_target, SELECTOR)
    features = [feature for feature in features if feature not in MACRO_FEATURES]
    if not features:
        return None, {}, "no_fold_selected_features"
    if set(features) & MACRO_FEATURES:
        raise RuntimeError("macro feature entered a Session 9 fold")

    artifact_dir = output_root / f"models/{population}/{horizon}/{fold_id}/{model_kind}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    feature_path = artifact_dir / "features.json"
    _write_json(feature_path, {
        "model_path": "CORRECTED_8F", "population": population, "horizon": horizon,
        "fold_id": fold_id, "model_kind": model_kind, "target": target,
        "selector": SELECTOR.__dict__, "selection_scope": "development rows inside historical fold",
        "macro_features_excluded": sorted(MACRO_FEATURES), "features": features,
    })

    fit_frame = development if model_kind == "classification" else train
    imputer = SimpleImputer(strategy="median", keep_empty_features=True)
    scaler = StandardScaler()
    fit_imputed = imputer.fit_transform(fit_frame[features])
    fit_transformed = scaler.fit_transform(fit_imputed)
    preprocessor = {"features": features, "imputer": imputer, "scaler": scaler}
    preprocessing_path = artifact_dir / "preprocessing.joblib"
    joblib.dump(preprocessor, preprocessing_path, compress=3)

    if model_kind == "classification":
        base_model = LogisticRegression(
            C=0.1, class_weight="balanced", max_iter=1000, solver="lbfgs",
            random_state=RANDOM_SEED,
        )
        base_model.fit(fit_transformed, development[target].astype(int))
        calibration_x = scaler.transform(imputer.transform(calibration[features]))
        model = CalibratedClassifierCV(FrozenEstimator(base_model), method="sigmoid")
        model.fit(calibration_x, calibration[target].astype(int))
        score_x = scaler.transform(imputer.transform(score[features]))
        predictions = model.predict_proba(score_x)[:, 1]
        calibration_payload = {
            "method": "sigmoid", "calibration_decision_timestamp": str(calibration_time),
            "calibration_rows": len(calibration), "fit_scope": "historical training fold only",
        }
    else:
        model = Ridge(alpha=10.0)
        model.fit(fit_transformed, fit_frame[target].astype(float))
        score_x = scaler.transform(imputer.transform(score[features]))
        predictions = model.predict(score_x)
        calibration_payload = {"method": "not_applicable_regression"}

    model_path = artifact_dir / "model.joblib"
    joblib.dump(model, model_path, compress=3)
    calibration_path = artifact_dir / "calibration.json"
    _write_json(calibration_path, calibration_payload)
    ids = {
        "feature_artifact_id": _artifact_id(feature_path),
        "preprocessing_artifact_id": _artifact_id(preprocessing_path),
        "model_artifact_id": _artifact_id(model_path),
        "calibration_artifact_id": _artifact_id(calibration_path),
        "n_training": len(train),
        "n_development": len(development),
        "n_calibration": len(calibration) if model_kind == "classification" else 0,
        "training_label_end_max": train["label_end_date"].max().isoformat(),
        "selected_feature_count": len(features),
    }
    return predictions, ids, None


def _prediction_template(base: pd.DataFrame, model_kind: str) -> pd.DataFrame:
    fields = [
        "stable_row_id", "entity_id", "fiscal_year", "horizon", "population",
        "decision_timestamp", "prediction_timestamp", "label_end_date", "label_provenance",
        "relative_return", "outperformed_benchmark", "gate_classification", "gate_reason",
        "price_feature_status", "price_feature_reason", "model_path",
    ]
    out = base[fields].copy()
    out["model_kind"] = model_kind
    out["fold_id"] = out["decision_timestamp"].dt.strftime("decision_%Y%m%dT%H%M%SZ")
    out["eligible"] = False
    out["exclusion_reason"] = base["base_exclusion_reason"].astype(str)
    out["feature_artifact_id"] = pd.NA
    out["preprocessing_artifact_id"] = pd.NA
    out["model_artifact_id"] = pd.NA
    out["calibration_artifact_id"] = pd.NA
    out["prediction"] = np.nan
    out["rank"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    return out


def build_population(population: str, base: pd.DataFrame, artifact_root: Path) -> tuple[pd.DataFrame, list[dict]]:
    outputs = []
    fold_records: list[dict] = []
    for model_kind in MODEL_KINDS:
        prediction_rows = _prediction_template(base, model_kind)
        for horizon in HORIZONS:
            horizon_base = base[base["horizon"].eq(horizon)]
            for decision_timestamp in sorted(horizon_base["decision_timestamp"].dropna().unique()):
                score_mask = (
                    base["horizon"].eq(horizon)
                    & base["decision_timestamp"].eq(decision_timestamp)
                    & base["base_eligible"]
                )
                if not score_mask.any():
                    continue
                score = base[score_mask].copy()
                train = base[
                    base["horizon"].eq(horizon)
                    & base["base_eligible"]
                    & base["label_end_date"].lt(decision_timestamp)
                ].copy()
                if not train.empty and not train["label_end_date"].lt(decision_timestamp).all():
                    raise RuntimeError("strict training label eligibility failed")
                fold_id = pd.Timestamp(decision_timestamp).strftime("decision_%Y%m%dT%H%M%SZ")
                predictions, ids, reason = _fit_model(
                    train, score, population, horizon, fold_id, model_kind, artifact_root
                )
                record_item = {
                    "population": population, "horizon": horizon, "fold_id": fold_id,
                    "model_kind": model_kind, "decision_timestamp": str(decision_timestamp),
                    "n_training": len(train), "n_scored_population": len(score),
                    "strict_label_end_before_decision": True, "status": "fit" if reason is None else "unavailable",
                    "reason": reason, **ids,
                }
                fold_records.append(record_item)
                target_index = prediction_rows.index.intersection(score.index)
                if reason is not None:
                    prediction_rows.loc[target_index, "exclusion_reason"] = f"model_unavailable:{reason}"
                    continue
                prediction_rows.loc[target_index, "eligible"] = True
                prediction_rows.loc[target_index, "exclusion_reason"] = ""
                for key, value in ids.items():
                    if key.endswith("_artifact_id"):
                        prediction_rows.loc[target_index, key] = value
                prediction_rows.loc[target_index, "prediction"] = predictions
                ranks = pd.Series(predictions, index=target_index).rank(method="first", ascending=False).astype("Int64")
                prediction_rows.loc[target_index, "rank"] = ranks
        outputs.append(prediction_rows)
    combined = pd.concat(outputs, ignore_index=True)
    output_path = artifact_root / f"predictions/{population}/oos_predictions.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_path, index=False)
    checkpoint_path = artifact_root / f"checkpoints/{population}.json"
    _write_json(checkpoint_path, {
        "completed": True, "population": population, "rows": len(combined),
        "eligible_predictions": int(combined["eligible"].sum()), "folds": fold_records,
    })
    return combined, fold_records


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
    artifact_prefix = artifact_root.relative_to(ROOT).as_posix() + "/"
    for raw in subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=ROOT).split(b"\0"):
        if not raw:
            continue
        relative = raw.decode()
        path = ROOT / relative
        if path.is_file() and not relative.startswith(artifact_prefix):
            inventory.append(record(path, "untracked_worktree_file"))
    _write_json(paths[2], inventory)
    return paths


def build(artifact_root: Path = DEFAULT_ARTIFACT_ROOT) -> dict[str, Any]:
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"refusing to reuse non-empty artifact root: {artifact_root}")
    preflight = validate_session8f_manifest()
    for name in ("configuration", "inputs", "models", "predictions", "checkpoints",
                 "support", "lineage"):
        (artifact_root / name).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(SESSION8F / "manifest.json", artifact_root / "inputs/session8f_manifest.json")

    loaded = {population: _load_population(population) for population in POPULATIONS}
    identity = validate_population_identity(loaded[POPULATIONS[0]], loaded[POPULATIONS[1]])
    _write_json(artifact_root / "support/population_identity.json", identity)
    _write_json(artifact_root / "support/old_reconstructed_unavailable.json", {
        "path": "OLD_RECONSTRUCTED", "status": "unavailable",
        "reason": "no separately certified old-feature population with stable row identity, proven filing availability, and Session 8E-aligned labels exists",
        "constructed_ad_hoc": False,
    })

    all_predictions = []
    all_folds = []
    assembled = {}
    for population in POPULATIONS:
        base = _assemble_population(population, *loaded[population])
        assembled[population] = base
        predictions, folds = build_population(population, base, artifact_root)
        all_predictions.append(predictions)
        all_folds.extend(folds)

    summary_rows = []
    for population, base in assembled.items():
        for horizon in HORIZONS:
            group = base[base["horizon"].eq(horizon)]
            counts = group["gate_classification"].value_counts().to_dict()
            intersection_supported = int(group["base_eligible"].sum())
            summary_rows.append({
                "population": population, "horizon": horizon,
                "session8e_supported": int(counts.get("supported", 0)),
                "session8e_unavailable": int(counts.get("unavailable", 0)),
                "session8e_excluded": int(counts.get("excluded", 0)),
                "price_and_label_intersection_supported": intersection_supported,
                "price_and_label_intersection_unavailable": int(len(group) - intersection_supported - counts.get("excluded", 0)),
                "price_and_label_intersection_excluded": int(counts.get("excluded", 0)),
            })
    summary = pd.DataFrame(summary_rows)
    summary.to_parquet(artifact_root / "support/supported_unavailable_excluded_counts.parquet", index=False)
    pd.DataFrame(all_folds).to_parquet(artifact_root / "support/fold_summary.parquet", index=False)

    created_at = datetime.now(timezone.utc).isoformat()
    configuration = {
        "schema_version": 1, "session": "9", "created_at_utc": created_at,
        "model_path": "CORRECTED_8F", "old_reconstructed": "unavailable",
        "populations": list(POPULATIONS), "horizons": list(HORIZONS),
        "model_kinds": list(MODEL_KINDS), "random_seed": RANDOM_SEED,
        "selector": SELECTOR.__dict__, "minimum_development_rows": MIN_DEVELOPMENT_ROWS,
        "minimum_calibration_rows": MIN_CALIBRATION_ROWS,
        "training_eligibility": "Session 8F price support AND Session 8E label support AND strict label_end_date < fold decision_timestamp",
        "scoring_population": "Session 8F price support AND Session 8E horizon-label support",
        "classification": "fold-local median imputation, standardization, balanced logistic regression, latest-historical-cohort sigmoid calibration",
        "regression": "fold-local median imputation, standardization, ridge regression",
        "macro_features": "excluded",
        "macro_feature_names": sorted(MACRO_FEATURES),
        "stale_corrected_partial": "prohibited and unread",
        "backtests_portfolios_threshold_optimization_session9c": "not executed",
    }
    _write_json(artifact_root / "configuration/config.json", configuration)
    _write_json(artifact_root / "support/preflight_validation.json", preflight)
    lineage_paths = freeze_dirty_state(artifact_root)

    code_paths = [
        ROOT / "modeling/build_session9_oos.py", ROOT / "modeling/fold_lineage.py",
        ROOT / "modeling/constants.py", ROOT / "tests/modeling/test_build_session9_oos.py",
    ]
    artifact_files = sorted(path for path in artifact_root.rglob("*")
                            if path.is_file() and path.name != "manifest.json")
    input_paths = [SESSION8F / "manifest.json"]
    for population in POPULATIONS:
        input_paths.extend([
            SESSION8F / f"outputs/{population}/features_taxonomy.parquet",
            SESSION8F / f"inputs/{population}_labels.parquet",
            SESSION8F / f"inputs/{population}_row_horizon.parquet",
        ])
    manifest = {
        "schema_version": 1, "artifact_class": "SESSION9_CORRECTED_8F_OOS",
        "created_at_utc": created_at, "baseline_commit": BASELINE_COMMIT,
        "current_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "claim": {
            "session9_corrected_build_complete": True, "model_path": "CORRECTED_8F",
            "old_reconstructed_available": False, "stale_corrected_partial_read": False,
            "macro_features_used": False, "backtest_or_portfolio_run": False,
            "production_thresholds_optimized": False, "session9c_started": False,
        },
        "preflight": preflight, "configuration": configuration,
        "validated_inputs": [record(path, "session8f_certified_model_input") for path in input_paths],
        "records": [record(path, "session9_configuration_checkpoint_model_prediction_or_lineage")
                    for path in artifact_files],
        "code_lineage": [record(path, "session9_code_or_test") for path in code_paths],
        "dirty_state": {"baseline": BASELINE_COMMIT, "complete_status_recorded": True,
                        "records": [path.relative_to(ROOT).as_posix() for path in lineage_paths]},
        "population_identity": identity,
        "environment": {"python": platform.python_version(), "numpy": np.__version__,
                        "pandas": pd.__version__, "scikit_learn": sklearn.__version__,
                        "joblib": joblib.__version__},
    }
    manifest_path = artifact_root / "manifest.json"
    _write_json(manifest_path, manifest)
    return {
        "manifest": manifest_path, "manifest_sha256": sha256_file(manifest_path),
        "prediction_rows": sum(len(frame) for frame in all_predictions),
        "eligible_predictions": sum(int(frame["eligible"].sum()) for frame in all_predictions),
    }


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
