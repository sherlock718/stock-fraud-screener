"""Generate the Session V3.2 fold-local production OOS predictions.

Only the hash-pinned Session V3.1 table and configuration are consumed.  The
builder fits one decision tree and one LightGBM regressor per supported
historical decision fold, persists complete model/preprocessing lineage, and
does not select holdings, source market data, or run a backtest.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import subprocess
import warnings
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
import sklearn
from sklearn.tree import DecisionTreeClassifier
from scipy.stats import ConstantInputWarning

from modeling.fold_lineage import SelectorConfig, dataframe_fingerprint, select_fold_features


ROOT = Path(__file__).resolve().parents[1]
SESSION_V3_1 = ROOT / "artifacts/pit_validation/session_v3_1_production_contract"
DEFAULT_ARTIFACT_ROOT = ROOT / "artifacts/pit_validation/session_v3_2_oos_predictions"
REPORT_PATH = ROOT / "reports/pit_validation/v3_2_oos_predictions.md"
V3_1_MANIFEST_SHA256 = "2b5249cdb05c7bad1759abbd281ec1c90a8a9ce2fbd72973cd4dc905c8a86e5a"
V3_1_TABLE = SESSION_V3_1 / "outputs/observed_only_us_annual_3y.parquet"
V3_1_CONFIG = SESSION_V3_1 / "configuration/production_contract.json"
CONSUMED_RECORDS = (V3_1_TABLE, V3_1_CONFIG)
MODEL_ROLES = ("decision_tree", "lightgbm_regression")

EXCLUSION_NO_HISTORY = "fold_no_eligible_training_history"
EXCLUSION_TREE_CLASSES = "fold_tree_requires_two_training_classes"
EXCLUSION_NO_FEATURES = "fold_no_selected_features"
EXCLUSION_INVALID_MEDIANS = "fold_invalid_feature_medians"
EXCLUSION_INVALID_OUTPUT = "fold_invalid_model_output"
EXCLUSION_FUTURE = "future_decision_after_v3_1_freeze"
LABEL_DERIVED_CANDIDATE_AVAILABILITY = {
    "observed_excess_return_3y": "label_end_date",
}


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


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def validate_v3_1_inputs() -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate the exact V3.1 manifest and only the consumed table/config records."""
    manifest_path = SESSION_V3_1 / "manifest.json"
    actual_manifest_hash = sha256_file(manifest_path)
    if actual_manifest_hash != V3_1_MANIFEST_SHA256:
        raise RuntimeError(
            "Session V3.1 manifest hash mismatch: "
            f"expected={V3_1_MANIFEST_SHA256} actual={actual_manifest_hash}"
        )
    manifest = json.loads(manifest_path.read_text())
    indexed = {item["path"]: item for item in manifest.get("records", [])}
    validated = []
    for path in CONSUMED_RECORDS:
        relative = path.relative_to(ROOT).as_posix()
        expected = indexed.get(relative)
        if expected is None:
            raise RuntimeError(f"consumed V3.1 record absent from manifest: {relative}")
        if not path.is_file():
            raise RuntimeError(f"consumed V3.1 record missing: {relative}")
        actual_size = path.stat().st_size
        actual_hash = sha256_file(path)
        if actual_size != int(expected["size_bytes"]):
            raise RuntimeError(
                f"consumed V3.1 record size mismatch: {relative} "
                f"expected={expected['size_bytes']} actual={actual_size}"
            )
        if actual_hash != expected["sha256"]:
            raise RuntimeError(
                f"consumed V3.1 record hash mismatch: {relative} "
                f"expected={expected['sha256']} actual={actual_hash}"
            )
        validated.append({"path": relative, "size_bytes": actual_size, "sha256": actual_hash})

    config = json.loads(V3_1_CONFIG.read_text())
    if config != manifest.get("configuration"):
        raise RuntimeError("V3.1 configuration record differs from embedded manifest configuration")
    if config.get("session") != "V3.1" or config.get("strategy_name") != "production_v3_ml_gates":
        raise RuntimeError("V3.1 production configuration identity drifted")
    if config["feature_contract"].get("candidate_count") != 119:
        raise RuntimeError("V3.1 frozen feature candidate count drifted")

    preflight = {
        "result": "pass",
        "v3_1_manifest_sha256": actual_manifest_hash,
        "records_validated": validated,
        "records_validated_count": len(validated),
        "unconsumed_v3_1_records_revalidated": 0,
        "configuration_equals_manifest_configuration": True,
    }
    return config, preflight


def validate_table(table: pd.DataFrame, config: dict[str, Any], manifest: dict[str, Any]) -> None:
    expected = manifest["table"]
    checks = {
        "rows": len(table),
        "columns": len(table.columns),
        "stable_row_ids": table["stable_row_id"].nunique(),
        "observed_3y_targets": int(table["target_3y"].notna().sum()),
        "entry_timestamps": int(table["entry_timestamp"].notna().sum()),
        "all_non_model_hard_gates_pass": int(table["all_non_model_hard_gates_pass"].sum()),
    }
    if checks != expected:
        raise RuntimeError(f"V3.1 table contract summary drifted: expected={expected} actual={checks}")
    candidates = config["feature_contract"]["candidate_columns"]
    if len(candidates) != 119 or len(set(candidates)) != 119:
        raise RuntimeError("V3.1 feature candidate list is not exactly 119 unique columns")
    missing = sorted(set(candidates) - set(table.columns))
    if missing:
        raise RuntimeError(f"V3.1 table is missing frozen feature candidates: {missing}")
    if table["stable_row_id"].isna().any() or table["stable_row_id"].duplicated().any():
        raise RuntimeError("V3.1 stable row identity is incomplete or duplicated")
    if not table["market"].eq("US").all() or not table["period_type"].eq("annual").all():
        raise RuntimeError("V3.1 modeling population is not US annual")


def selector_config(config: dict[str, Any]) -> SelectorConfig:
    frozen = config["feature_contract"]["selector"]
    result = SelectorConfig(**frozen)
    if asdict(result) != frozen or result.top_n != 28:
        raise RuntimeError("V3.1 selector configuration did not round-trip exactly")
    return result


def _role_contract(role: str, config: dict[str, Any]) -> tuple[str, str]:
    if role == "decision_tree":
        return config["targets"]["decision_tree"], "_v3_2_beat_local_market_target"
    if role == "lightgbm_regression":
        return config["targets"]["lightgbm_regression"], "_v3_2_forward_return_target"
    raise ValueError(f"unsupported V3.2 model role: {role}")


def eligible_training_rows(table: pd.DataFrame, decision_timestamp: pd.Timestamp, target: str) -> pd.DataFrame:
    label_end = pd.to_datetime(table["label_end_date"], utc=True, errors="coerce")
    clean = (
        pd.to_numeric(table["piotroski_roa_pos"], errors="coerce").eq(1)
        & pd.to_numeric(table["beneish_m_score"], errors="coerce").lt(-1.78)
    )
    eligible = label_end.notna() & label_end.lt(decision_timestamp) & clean & table[target].notna()
    train = table.loc[eligible].copy()
    if not train.empty and not pd.to_datetime(train["label_end_date"], utc=True).lt(decision_timestamp).all():
        raise RuntimeError("strict label_end_date < decision_timestamp invariant failed")
    return train


def _select_features(
    train: pd.DataFrame,
    role: str,
    config: dict[str, Any],
    selector: SelectorConfig,
) -> tuple[list[str], str, str]:
    target, selector_target = _role_contract(role, config)
    candidates = list(config["feature_contract"]["candidate_columns"])
    selection = train[["fiscal_year", *candidates]].copy()
    selection[selector_target] = pd.to_numeric(train[target], errors="coerce").to_numpy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConstantInputWarning)
        features = select_fold_features(selection, selector_target, selector)
    if not set(features).issubset(candidates):
        raise RuntimeError("fold selector escaped the frozen 119 feature candidates")
    if len(features) > selector.top_n:
        raise RuntimeError("fold selector exceeded the frozen 28-feature cap")
    return features, target, selector_target


def _medians(train: pd.DataFrame, features: list[str]) -> dict[str, float] | None:
    medians = train[features].apply(pd.to_numeric, errors="coerce").median(axis=0, skipna=True)
    values = medians.to_numpy(dtype=float)
    if len(values) != len(features) or not np.isfinite(values).all():
        return None
    return {feature: float(medians[feature]) for feature in features}


def _transform(frame: pd.DataFrame, features: list[str], medians: dict[str, float]) -> np.ndarray:
    numeric = frame[features].apply(pd.to_numeric, errors="coerce")
    transformed = numeric.fillna(pd.Series(medians)).to_numpy(dtype=float)
    return transformed


def mask_unavailable_score_features(
    score: pd.DataFrame,
    features: list[str],
    decision_timestamp: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Mask frozen label-derived candidates until their certified availability."""
    masked = score.copy()
    counts: dict[str, int] = {}
    for feature, availability_column in LABEL_DERIVED_CANDIDATE_AVAILABILITY.items():
        if feature not in features:
            continue
        available_at = pd.to_datetime(masked[availability_column], utc=True, errors="coerce")
        unavailable = available_at.isna() | available_at.ge(decision_timestamp)
        counts[feature] = int((unavailable & masked[feature].notna()).sum())
        masked.loc[unavailable, feature] = np.nan
    return masked, counts


def _artifact_id(path: Path) -> str:
    return f"sha256:{sha256_file(path)}"


def fit_fold_role(
    train: pd.DataFrame,
    score: pd.DataFrame,
    role: str,
    fold_id: str,
    config: dict[str, Any],
    output_root: Path,
) -> tuple[np.ndarray | None, dict[str, Any], str | None]:
    """Fit one frozen V3.2 model role, returning exact fail-closed codes."""
    if train.empty:
        return None, {}, EXCLUSION_NO_HISTORY
    features, target, selector_target = _select_features(
        train, role, config, selector_config(config)
    )
    if role == "decision_tree" and pd.to_numeric(train[target], errors="coerce").nunique() != 2:
        return None, {}, EXCLUSION_TREE_CLASSES
    if not features:
        return None, {}, EXCLUSION_NO_FEATURES
    medians = _medians(train, features)
    if medians is None:
        return None, {}, EXCLUSION_INVALID_MEDIANS
    train_x = _transform(train, features, medians)
    fold_decision_timestamp = pd.Timestamp(score["decision_timestamp"].iloc[0])
    score_available, masked_score_values = mask_unavailable_score_features(
        score, features, fold_decision_timestamp
    )
    score_x = _transform(score_available, features, medians)
    if not np.isfinite(train_x).all() or not np.isfinite(score_x).all():
        return None, {}, EXCLUSION_INVALID_MEDIANS

    artifact_dir = output_root / f"models/{fold_id}/{role}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    feature_path = artifact_dir / "features.json"
    preprocessing_path = artifact_dir / "preprocessing.json"
    target_path = artifact_dir / "target.json"
    model_configuration_path = artifact_dir / "model_configuration.json"
    model_path = artifact_dir / "model.joblib"
    selection_population = train[
        [
            "stable_row_id",
            "fiscal_year",
            target,
            *config["feature_contract"]["candidate_columns"],
        ]
    ].copy()
    feature_payload = {
        "role": role,
        "fold_id": fold_id,
        "candidate_count": len(config["feature_contract"]["candidate_columns"]),
        "candidate_columns": config["feature_contract"]["candidate_columns"],
        "selector": config["feature_contract"]["selector"],
        "selector_target_alias": selector_target,
        "selected_feature_count": len(features),
        "selected_features": features,
        "selection_population_fingerprint": dataframe_fingerprint(selection_population),
    }
    preprocessing_payload = {
        "role": role,
        "fold_id": fold_id,
        "method": "fold_local_median_imputation_only",
        "fit_rows": len(train),
        "features": features,
        "medians": medians,
        "score_availability_rule": (
            "label-derived candidate value is masked when its certified "
            "label_end_date is null or not strictly before the score decision timestamp"
        ),
        "label_derived_candidate_availability": LABEL_DERIVED_CANDIDATE_AVAILABILITY,
        "masked_non_null_score_values": masked_score_values,
        "fit_population_fingerprint": dataframe_fingerprint(train[["stable_row_id", *features]]),
    }
    target_payload = {
        "role": role,
        "fold_id": fold_id,
        "target": target,
        "regression_fit_clip": config["targets"]["regression_clip"] if role == "lightgbm_regression" else None,
        "training_label_rule": config["decision_calendar"]["training_label_rule"],
        "training_label_end_max": pd.Timestamp(train["label_end_date"].max()).isoformat(),
        "clean_training_filter": config["model_training_population"]["required_filter"],
        "training_rows": len(train),
        "target_fingerprint": dataframe_fingerprint(train[["stable_row_id", "label_end_date", target]]),
    }
    write_json(feature_path, feature_payload)
    write_json(preprocessing_path, preprocessing_payload)
    write_json(target_path, target_payload)

    if role == "decision_tree":
        y = train[target].astype(int)
        n_negative = int(y.eq(0).sum())
        n_positive = int(y.eq(1).sum())
        params = {
            "max_depth": config["decision_tree"]["max_depth"],
            "min_samples_leaf": config["decision_tree"]["min_samples_leaf"],
            "min_samples_split": config["decision_tree"]["min_samples_split"],
            "class_weight": {0: 1.0, 1: n_negative / max(n_positive, 1)},
            "random_state": config["decision_tree"]["random_state"],
        }
        write_json(model_configuration_path, {
            "family": config["decision_tree"]["family"],
            "role": role,
            "fold_id": fold_id,
            "parameters": params,
            "parameter_source": "V3.1 frozen configuration plus frozen fold-local class-weight equation",
        })
        model = DecisionTreeClassifier(**params)
        model.fit(train_x, y)
        class_positions = np.flatnonzero(model.classes_ == 1)
        if len(class_positions) != 1:
            return None, {}, EXCLUSION_INVALID_OUTPUT
        predictions = model.predict_proba(score_x)[:, int(class_positions[0])]
    else:
        params = dict(config["lightgbm_ranker"]["parameters"])
        write_json(model_configuration_path, {
            "family": config["lightgbm_ranker"]["family"],
            "role": role,
            "fold_id": fold_id,
            "parameters": params,
            "parameter_source": "V3.1 frozen configuration",
        })
        model = lgb.LGBMRegressor(**params)
        clip_low, clip_high = config["targets"]["regression_clip"]
        y = pd.to_numeric(train[target], errors="coerce").clip(clip_low, clip_high)
        model.fit(train_x, y)
        predictions = model.predict(score_x)

    predictions = np.asarray(predictions, dtype=float)
    if len(predictions) != len(score) or not np.isfinite(predictions).all():
        return None, {}, EXCLUSION_INVALID_OUTPUT
    if role == "decision_tree" and ((predictions < 0) | (predictions > 1)).any():
        return None, {}, EXCLUSION_INVALID_OUTPUT
    joblib.dump(model, model_path, compress=3)
    lineage = {
        "feature_artifact_id": _artifact_id(feature_path),
        "preprocessing_artifact_id": _artifact_id(preprocessing_path),
        "target_artifact_id": _artifact_id(target_path),
        "model_configuration_artifact_id": _artifact_id(model_configuration_path),
        "model_artifact_id": _artifact_id(model_path),
        "training_rows": len(train),
        "training_label_end_max": pd.Timestamp(train["label_end_date"].max()).isoformat(),
        "selected_feature_count": len(features),
        "selected_features_json": json.dumps(features, separators=(",", ":")),
        "training_population_fingerprint": dataframe_fingerprint(
            train[["stable_row_id", "label_end_date", target, *features]]
        ),
    }
    return predictions, lineage, None


def prediction_template(table: pd.DataFrame, role: str) -> pd.DataFrame:
    fields = [
        "stable_row_id", "entity_id", "cik", "ticker", "market", "fiscal_year",
        "period_type", "availability_timestamp", "availability_provenance",
        "decision_timestamp", "prediction_timestamp", "entry_timestamp", "label_end_date",
        "target_status_3y", "target_provenance_3y", "target_3y", "tree_target_3y",
        "fold_id", "all_non_model_hard_gates_pass", "hard_gate_exclusion_codes",
    ]
    out = table[fields].copy()
    out["strategy_name"] = "production_v3_ml_gates"
    out["model_role"] = role
    out["prediction_status"] = "excluded"
    out["exclusion_code"] = ""
    out["prediction"] = np.nan
    out["feature_artifact_id"] = pd.NA
    out["preprocessing_artifact_id"] = pd.NA
    out["target_artifact_id"] = pd.NA
    out["model_configuration_artifact_id"] = pd.NA
    out["model_artifact_id"] = pd.NA
    out["training_rows"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out["training_label_end_max"] = pd.Series(pd.NA, index=out.index, dtype="string")
    out["selected_feature_count"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out["selected_features_json"] = pd.NA
    out["training_population_fingerprint"] = pd.NA
    return out


def build_predictions(
    table: pd.DataFrame,
    config: dict[str, Any],
    freeze_timestamp: pd.Timestamp,
    artifact_root: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = table.copy()
    table["decision_timestamp"] = pd.to_datetime(table["decision_timestamp"], utc=True)
    table["label_end_date"] = pd.to_datetime(table["label_end_date"], utc=True, errors="coerce")
    outputs = []
    fold_records = []
    for role in MODEL_ROLES:
        rows = prediction_template(table, role)
        future = table["decision_timestamp"].gt(freeze_timestamp)
        rows.loc[future, "exclusion_code"] = EXCLUSION_FUTURE
        rows.loc[future, "prediction_status"] = "future_excluded"
        target, _ = _role_contract(role, config)
        for decision_timestamp in sorted(table.loc[~future, "decision_timestamp"].unique()):
            score_mask = table["decision_timestamp"].eq(decision_timestamp)
            score = table.loc[score_mask].copy()
            train = eligible_training_rows(table, pd.Timestamp(decision_timestamp), target)
            fold_id = pd.Timestamp(decision_timestamp).strftime("decision_%Y%m%dT%H%M%SZ")
            predictions, lineage, exclusion = fit_fold_role(
                train, score, role, fold_id, config, artifact_root
            )
            fold_record = {
                "strategy_name": "production_v3_ml_gates",
                "model_role": role,
                "fold_id": fold_id,
                "decision_timestamp": pd.Timestamp(decision_timestamp),
                "scored_rows": len(score),
                "training_rows": len(train),
                "training_label_end_max": train["label_end_date"].max() if not train.empty else pd.NaT,
                "strict_label_end_before_decision": bool(
                    train.empty or train["label_end_date"].lt(decision_timestamp).all()
                ),
                "status": "fit" if exclusion is None else "failed_closed",
                "exclusion_code": exclusion or "",
                **lineage,
            }
            fold_records.append(fold_record)
            target_index = rows.index[score_mask]
            if exclusion is not None:
                rows.loc[target_index, "exclusion_code"] = exclusion
                continue
            rows.loc[target_index, "prediction_status"] = "oos_prediction_available"
            rows.loc[target_index, "prediction"] = predictions
            for key, value in lineage.items():
                rows.loc[target_index, key] = value
        outputs.append(rows)
    predictions = pd.concat(outputs, ignore_index=True)
    folds = pd.DataFrame(fold_records)
    return predictions, folds


def exclusion_report(predictions: pd.DataFrame) -> pd.DataFrame:
    return (
        predictions.groupby(
            ["model_role", "prediction_status", "exclusion_code"], dropna=False
        ).size().rename("rows").reset_index()
    )


def write_report(predictions: pd.DataFrame, folds: pd.DataFrame, freeze_timestamp: pd.Timestamp) -> None:
    coverage = folds.groupby(["model_role", "status"], dropna=False).agg(
        folds=("fold_id", "size"), rows=("scored_rows", "sum")
    ).reset_index()
    coverage_lines = "\n".join(
        f"| {row.model_role} | {row.status} | {row.folds:,} | {row.rows:,} |"
        for row in coverage.itertuples(index=False)
    )
    exclusions = exclusion_report(predictions)
    exclusion_lines = "\n".join(
        f"| {row.model_role} | {row.prediction_status} | {row.exclusion_code or 'none'} | {row.rows:,} |"
        for row in exclusions.itertuples(index=False)
    )
    available = int(predictions["prediction_status"].eq("oos_prediction_available").sum())
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(f"""# Session V3.2 — Exact OOS Production Predictions

Status: **Accepted; V3.3 requires separate approval**

## Outcome

Session V3.2 generated {available:,} row-role OOS predictions for
`production_v3_ml_gates` from only the hash-pinned V3.1 table and configuration.
Each fold independently selected at most 28 features for each model role, fit
median imputation on eligible historical rows only, and trained with labels
whose `label_end_date` was strictly before the decision timestamp. Tree and
LightGBM targets and parameters were consumed unchanged from V3.1.

The frozen candidate pool contains the label-derived
`observed_excess_return_3y`. It remains a frozen candidate for selection and is
valid in historical training only after its certified `label_end_date`; its
score-row value is masked unless that date is strictly before the fold decision,
then imputed from the historical-fold median. This preserves the exact frozen
119-candidate contract without exposing a future observed outcome to scoring.

The V3.1 artifact freeze was `{freeze_timestamp.isoformat()}`. Decisions through
2026 were eligible for modeling; every 2027/2028 row-role was retained and
marked `{EXCLUSION_FUTURE}`. No fallback model was used.

## Fold coverage

| Model role | Status | Folds | Scored rows |
|---|---|---:|---:|
{coverage_lines}

## Exclusions

| Model role | Prediction status | Exact exclusion code | Rows |
|---|---|---|---:|
{exclusion_lines}

## Boundary

This session did not read Session 9 predictions, optimize parameters or
thresholds, select holdings, collect market data, calculate ADTV, or run a
backtest. Generated model and prediction payloads remain Git-ignored. V3.3 was
not started.
""")


def freeze_dirty_state(artifact_root: Path) -> list[Path]:
    lineage = artifact_root / "lineage"
    lineage.mkdir(parents=True, exist_ok=True)
    paths = [
        lineage / "git_status_porcelain.txt",
        lineage / "tracked_dirty.patch",
        lineage / "untracked_inventory.json",
    ]
    paths[0].write_bytes(subprocess.check_output(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=ROOT
    ))
    paths[1].write_bytes(subprocess.check_output(["git", "diff", "--binary", "--", "."], cwd=ROOT))
    prefix = artifact_root.relative_to(ROOT).as_posix() + "/"
    inventory = []
    for raw in subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=ROOT
    ).split(b"\0"):
        if not raw:
            continue
        relative = raw.decode()
        path = ROOT / relative
        if path.is_file() and not relative.startswith(prefix):
            inventory.append(record(path, "untracked_worktree_file"))
    write_json(paths[2], inventory)
    return paths


def build(artifact_root: Path = DEFAULT_ARTIFACT_ROOT) -> dict[str, Any]:
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"refusing to reuse non-empty artifact root: {artifact_root}")
    config, preflight = validate_v3_1_inputs()
    v3_1_manifest = json.loads((SESSION_V3_1 / "manifest.json").read_text())
    table = pd.read_parquet(V3_1_TABLE)
    validate_table(table, config, v3_1_manifest)
    freeze_timestamp = pd.Timestamp(v3_1_manifest["created_at_utc"])
    for name in ("inputs", "configuration", "models", "predictions", "support", "lineage"):
        (artifact_root / name).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(SESSION_V3_1 / "manifest.json", artifact_root / "inputs/v3_1_manifest.json")
    shutil.copyfile(V3_1_CONFIG, artifact_root / "configuration/production_contract.json")

    predictions, folds = build_predictions(table, config, freeze_timestamp, artifact_root)
    exclusions = exclusion_report(predictions)
    prediction_path = artifact_root / "predictions/oos_predictions.parquet"
    fold_path = artifact_root / "support/fold_coverage.parquet"
    exclusion_path = artifact_root / "support/exclusions.parquet"
    preflight_path = artifact_root / "support/preflight_validation.json"
    verdict_path = artifact_root / "support/verdict.json"
    predictions.to_parquet(prediction_path, index=False)
    folds.to_parquet(fold_path, index=False)
    exclusions.to_parquet(exclusion_path, index=False)
    write_json(preflight_path, preflight)
    verdict = {
        "status": "accepted",
        "session_v3_2_complete": True,
        "strategy_name": "production_v3_ml_gates",
        "oos_predictions_generated": True,
        "fallback_models_used": False,
        "parameters_or_thresholds_optimized": False,
        "session9_predictions_read": False,
        "holdings_selected": False,
        "external_data_sourced": False,
        "adtv_calculated": False,
        "backtest_run": False,
        "session_v3_3_started": False,
    }
    write_json(verdict_path, verdict)
    write_report(predictions, folds, freeze_timestamp)
    lineage_paths = freeze_dirty_state(artifact_root)

    code_paths = [
        ROOT / "modeling/build_session_v3_2_oos.py",
        ROOT / "modeling/fold_lineage.py",
        ROOT / "tests/modeling/test_build_session_v3_2_oos.py",
    ]
    artifact_files = sorted(
        path for path in artifact_root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    )
    created_at = datetime.now(timezone.utc).isoformat()
    manifest = {
        "schema_version": 1,
        "artifact_class": "SESSION_V3_2_PRODUCTION_OOS_MODELS_AND_PREDICTIONS",
        "created_at_utc": created_at,
        "v3_1_freeze_timestamp": freeze_timestamp.isoformat(),
        "current_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "claim": verdict,
        "preflight": preflight,
        "configuration": config,
        "prediction_summary": {
            "rows": len(predictions),
            "source_rows": len(table),
            "model_roles": list(MODEL_ROLES),
            "available_predictions": int(predictions["prediction_status"].eq("oos_prediction_available").sum()),
            "future_excluded_rows": int(predictions["exclusion_code"].eq(EXCLUSION_FUTURE).sum()),
            "folds": len(folds),
            "fit_folds": int(folds["status"].eq("fit").sum()),
            "failed_closed_folds": int(folds["status"].eq("failed_closed").sum()),
        },
        "validated_inputs": [
            record(SESSION_V3_1 / "manifest.json", "accepted_v3_1_manifest"),
            *[record(path, "consumed_v3_1_table_or_configuration") for path in CONSUMED_RECORDS],
        ],
        "records": [record(path, "v3_2_model_prediction_coverage_exclusion_or_lineage") for path in artifact_files],
        "code_lineage": [record(path, "v3_2_builder_dependency_or_focused_test") for path in code_paths],
        "deliverables": [record(REPORT_PATH, "v3_2_report")],
        "dirty_state": {
            "complete_status_recorded": True,
            "records": [path.relative_to(ROOT).as_posix() for path in lineage_paths],
        },
        "environment": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "scikit_learn": sklearn.__version__,
            "lightgbm": lgb.__version__,
            "joblib": joblib.__version__,
        },
    }
    manifest_path = artifact_root / "manifest.json"
    write_json(manifest_path, manifest)
    return {
        "manifest": manifest_path,
        "manifest_sha256": sha256_file(manifest_path),
        **manifest["prediction_summary"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    args = parser.parse_args()
    result = build(args.artifact_root.resolve())
    print(json.dumps({key: str(value) if isinstance(value, Path) else value for key, value in result.items()}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
