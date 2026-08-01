"""Execute the single frozen Session M1C nested walk-forward run.

This module is intentionally an execution wrapper around the M1B interfaces.
It performs a complete read-only preflight before the first fit, refuses to
reuse an artifact directory, and never imports portfolio or performance code.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
import sklearn

from modeling import build_canonical_research_model as p3
from modeling.nested_walk_forward import (
    MODEL_ROLES,
    SELECTOR_METHODS,
    TRAINING_REGIMES,
    CandidateEvaluation,
    CandidateUnavailable,
    FoldLocalPreparedData,
    FoldPopulation,
    NestedWalkForwardContractError,
    TuningRunContext,
    _candidate_order_sha256,
    _fold_local_cache_key,
    _payload_sha256,
    _population_fingerprint,
    _role_target,
    _sha256_file,
    _training_mask,
    _regression_metrics,
    _tree_metrics,
    assert_predictive_selection_inputs,
    candidate_columns_for_regime,
    evaluate_tuning_candidate,
    frozen_grid_points,
    frozen_inner_fold_specs,
    load_frozen_m1a_contract,
    materialize_inner_fold,
    refit_locked_outer_candidate,
    select_inner_winner,
    tuning_candidates,
    validate_candidate_columns,
)

ROOT = Path(__file__).resolve().parents[1]
M1C_VERSION = "20260801T020000Z-m1c"
DEFAULT_ROOT = ROOT / "artifacts/modeling/nested_walk_forward" / M1C_VERSION
P2_MANIFEST_SHA256 = "40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3"
P3_MANIFEST_SHA256 = "8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2"
P4_MANIFEST_SHA256 = "28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7"
B1E_MANIFEST_SHA256 = "23588207812e2e950d3e521c6f2048e7607f20b99b691407c449ceb7752bf37c"
B1D_ENGINE_SHA256 = "880cf13607851dcc1d0947ef7b62ecc798e1add841a707b85a80535cd74d034f"
B1D_TEST_SHA256 = "c9cb5b123b7450498a44938c64ebd166164df8a6216764a110e1a95217dee86f"
M1A_MANIFEST_SHA256 = "a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc"


def _record(path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(ROOT).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
    }


def _artifact_record(path: Path, root: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def _verify_preserved_boundaries() -> dict[str, Any]:
    paths = {
        "p2": ROOT / "artifacts/canonical/corrected_us_annual/manifest.json",
        "p3": ROOT / "artifacts/canonical/corrected_us_annual_3y_research_model/manifest.json",
        "p4": ROOT / "artifacts/canonical/corrected_us_annual_3y_product/manifest.json",
        "b1e": ROOT / "artifacts/performance/free_data_v1/20260801T011135Z-b1e/manifest.json",
        "b1d_engine": ROOT / "backtest/free_data_v1_nav.py",
        "b1d_tests": ROOT / "tests/backtest/test_free_data_v1_nav.py",
        "m1a": ROOT / "artifacts/modeling/nested_walk_forward/20260801T000000Z-m1a/manifest.json",
    }
    expected = {"p2": P2_MANIFEST_SHA256, "p3": P3_MANIFEST_SHA256,
                "p4": P4_MANIFEST_SHA256, "b1e": B1E_MANIFEST_SHA256,
                "b1d_engine": B1D_ENGINE_SHA256, "b1d_tests": B1D_TEST_SHA256,
                "m1a": M1A_MANIFEST_SHA256}
    result = {}
    for name, path in paths.items():
        if not path.is_file() or _sha256_file(path) != expected[name]:
            actual = _sha256_file(path) if path.is_file() else None
            raise NestedWalkForwardContractError(
                f"preserved {name} boundary mismatch: expected={expected[name]} actual={actual}"
            )
        result[name] = _record(path, "preserved_boundary")
    return result


def _prepare_table() -> tuple[pd.DataFrame, dict[str, Any]]:
    features, labels, support, summary, p2_manifest, p2_preflight = p3.load_canonical_inputs()
    candidates = p3.leakage_safe_feature_candidates(features)
    frozen = load_frozen_m1a_contract()
    if tuple(candidates) != frozen.candidate_columns:
        raise NestedWalkForwardContractError("P2/P3 candidate universe differs from M1A")
    table = p3.materialize_model_table(features, labels)
    table["decision_timestamp"] = pd.to_datetime(table["decision_timestamp"], utc=True)
    table["label_end_date"] = pd.to_datetime(table["label_end_date"], utc=True, errors="coerce")
    if len(table) != 43806 or table["stable_row_id"].duplicated().any():
        raise NestedWalkForwardContractError("P2 model table identity drifted")
    return table, {"p2_preflight": p2_preflight, "frozen": frozen,
                   "p2_manifest": p2_manifest, "support_rows": len(support),
                   "summary_rows": len(summary)}


def _outer_training(
    table: pd.DataFrame,
    decision: pd.Timestamp,
    role: str,
    regime: str,
    frozen: Any,
) -> pd.DataFrame:
    target = _role_target(frozen, role)
    work = table.loc[pd.to_datetime(table["decision_timestamp"], utc=True).lt(decision)].copy()
    eligible = _training_mask(work, target, regime)
    eligible &= pd.to_datetime(work["label_end_date"], utc=True, errors="coerce").lt(decision)
    result = work.loc[eligible].copy()
    if result.empty:
        return result
    if not pd.to_datetime(result["label_end_date"], utc=True).lt(decision).all():
        raise NestedWalkForwardContractError("outer training label maturity failed")
    return result


def _score_population(table: pd.DataFrame, decision: pd.Timestamp) -> pd.DataFrame:
    score = table.loc[
        pd.to_datetime(table["decision_timestamp"], utc=True).eq(decision)
        & table["source_feature_available_at_decision"].eq(True)
    ].copy()
    return score


def _build_inner_populations(table: pd.DataFrame, frozen: Any, outer_fold: str,
                             role: str, regime: str) -> tuple[FoldPopulation, ...]:
    populations = []
    for spec in frozen_inner_fold_specs(frozen, outer_fold, role, regime):
        populations.append(materialize_inner_fold(
            table, spec, frozen, verify_frozen_counts=True
        ))
    return tuple(populations)


def _serialize_candidate(evaluation: CandidateEvaluation) -> dict[str, Any]:
    return {
        "candidate_id": evaluation.candidate.candidate_id,
        "target_role": evaluation.candidate.target_role,
        "training_regime": evaluation.candidate.regime,
        "selector_method": evaluation.candidate.selector_method,
        "parameters": evaluation.candidate.parameters,
        "complexity_score": evaluation.candidate.complexity_score,
        "availability_status": evaluation.availability_status,
        "failure_reason": evaluation.failure_reason,
        "valid_inner_fold_count": evaluation.valid_inner_fold_count,
        "aggregate_metrics": evaluation.aggregate_metrics,
        "fold_evaluations": [
            {"outer_fold": item.outer_fold, "inner_fold": item.inner_fold,
             "candidate_id": item.candidate_id,
             "availability_status": item.availability_status,
             "failure_reason": item.failure_reason, "metrics": item.metrics,
             "selected_feature_count": item.selected_feature_count,
             "lineage": item.lineage}
            for item in evaluation.fold_evaluations
        ],
        "evidence_scope": evaluation.evidence_scope,
        "lineage": evaluation.lineage,
    }


def _serialize_tuning_candidate(candidate: Any) -> dict[str, Any]:
    return {
        "candidate_id": candidate.candidate_id,
        "target_role": candidate.target_role,
        "training_regime": candidate.regime,
        "selector_method": candidate.selector_method,
        "parameters": candidate.parameters,
        "complexity_score": candidate.complexity_score,
    }


def _partial_attempt_inventory(current_root: Path) -> list[dict[str, Any]]:
    parent = current_root.parent
    if not parent.is_dir():
        return []
    attempts: list[dict[str, Any]] = []
    for path in sorted(parent.glob("*-m1c")):
        if path == current_root or not path.is_dir():
            continue
        files = [item for item in sorted(path.rglob("*")) if item.is_file()]
        names = {item.relative_to(path).as_posix() for item in files}
        model_files = sorted(name for name in names if name.endswith("model.joblib"))
        outer_result_files = {
            "outer_oos_predictions.parquet",
            "outer_oos_metrics.json",
            "candidate_evaluations.json",
            "winner_decisions.json",
            "manifest.json",
        }
        observed_outer_result = bool(names & outer_result_files)
        if "manifest.json" in names:
            status = "completed_artifact_present"
        elif model_files:
            status = "failed_incomplete_execution_after_passed_preflight"
        else:
            status = "failed_execution_after_passed_preflight_before_model_output"
        attempts.append(
            {
                "artifact_directory": path.name,
                "status": status,
                "file_count": len(files),
                "model_file_count": len(model_files),
                "outer_result_observed": observed_outer_result,
                "records": [
                    _artifact_record(item, path, "preserved_partial_attempt")
                    for item in files
                ],
            }
        )
    return attempts


def _target_status(frame: pd.DataFrame, target: str) -> pd.Series:
    decision_year = pd.to_datetime(
        frame["decision_timestamp"], utc=True, errors="coerce"
    ).dt.year
    observed = (
        pd.to_numeric(frame[target], errors="coerce").notna()
        & pd.to_datetime(
            frame["label_end_date"], utc=True, errors="coerce"
        ).notna()
    )
    result = pd.Series(
        "historical_label_unavailable_excluded_from_metrics",
        index=frame.index,
        dtype="object",
    )
    result.loc[decision_year.isin([2024, 2025, 2026])] = (
        "open_2024_2026_unlabeled_excluded_from_metrics"
    )
    result.loc[observed] = "matured_observed"
    return result


def _prediction_shell(frame: pd.DataFrame, role: str, frozen: Any) -> pd.DataFrame:
    columns = [
        "stable_row_id",
        "entity_id",
        "ticker",
        "fiscal_year",
        "decision_timestamp",
        "label_end_date",
    ]
    output = frame[columns].copy()
    target = _role_target(frozen, role)
    output["model_role"] = pd.Series(
        role, index=frame.index, dtype="string"
    )
    output["target"] = pd.to_numeric(
        frame[target], errors="coerce"
    ).astype("float64")
    output["target_status"] = _target_status(frame, target).astype("string")
    output["metric_eligible"] = False
    for column in (
        "outer_fold",
        "training_regime",
        "prediction_status",
        "exclusion_code",
        "model_path",
        "model_sha256",
        "winner_candidate_id",
        "selected_features_json",
        "parameters_json",
        "training_population_fingerprint",
        "feature_selection_lineage_sha256",
        "preprocessing_lineage_sha256",
        "outer_refit_lineage_sha256",
    ):
        output[column] = pd.Series(pd.NA, index=frame.index, dtype="string")
    output["prediction"] = np.nan
    output["selected_feature_count"] = pd.Series(
        pd.NA, index=frame.index, dtype="Int64"
    )
    output["training_rows"] = pd.Series(
        pd.NA, index=frame.index, dtype="Int64"
    )
    return output


def _array_sha256(values: np.ndarray) -> str:
    array = np.ascontiguousarray(values, dtype=np.float64)
    digest = hashlib.sha256()
    digest.update(json.dumps(list(array.shape), separators=(",", ":")).encode())
    digest.update(array.tobytes())
    return digest.hexdigest()


def _serialize_fold_local_cache(
    cache: dict[tuple[str, ...], FoldLocalPreparedData | str],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for key in sorted(cache):
        value = cache[key]
        identity = {
            "target_role": key[0],
            "training_regime": key[1],
            "target_column": key[2],
            "selector_method": key[3],
            "training_population_fingerprint": key[4],
            "validation_population_fingerprint": key[5],
        }
        if isinstance(value, str):
            records.append(
                {
                    **identity,
                    "availability_status": "unavailable",
                    "failure_reason": value,
                }
            )
            continue
        records.append(
            {
                **identity,
                "availability_status": "available",
                "selected_features": list(value.selected_features),
                "candidate_count": len(value.candidate_columns),
                "candidate_order_sha256": _candidate_order_sha256(
                    value.candidate_columns
                ),
                "selector_parameters": value.selector_parameters,
                "diagnostics": value.diagnostics,
                "diagnostics_sha256": _payload_sha256(value.diagnostics),
                "preprocessing": {
                    "medians": value.medians,
                    "transformations": value.transformations,
                    "train_shape": list(value.train_x.shape),
                    "validation_shape": list(value.validation_x.shape),
                    "train_values_sha256": _array_sha256(value.train_x),
                    "validation_values_sha256": _array_sha256(
                        value.validation_x
                    ),
                },
            }
        )
    return records


def _build_preflight(
    table: pd.DataFrame,
    prepared: dict[str, Any],
    preserved: dict[str, Any],
    artifact_root: Path,
) -> tuple[dict[str, Any], list[pd.Timestamp]]:
    frozen = prepared["frozen"]
    outer_decisions = sorted(
        {pd.Timestamp(row["decision_timestamp"]) for row in frozen.outer_folds}
    )
    outer_population_lineage: list[dict[str, Any]] = []
    available_inner_lineage: list[dict[str, Any]] = []
    score_lineage: list[dict[str, Any]] = []
    available_count_by_unit: dict[tuple[str, str, str], int] = {}
    unique_population_keys: set[tuple[str, ...]] = set()
    for decision in outer_decisions:
        outer_fold = decision.strftime("decision_%Y%m%dT%H%M%SZ")
        score = _score_population(table, decision)
        records = [
            row for row in frozen.outer_folds if row["outer_fold"] == outer_fold
        ]
        if len(records) != len(MODEL_ROLES):
            raise NestedWalkForwardContractError(
                f"outer role records are incomplete for {outer_fold}"
            )
        expected_scores = {int(row["p3_score_eligible_rows"]) for row in records}
        if expected_scores != {len(score)}:
            raise NestedWalkForwardContractError(
                f"outer score population drifted for {outer_fold}"
            )
        score_lineage.append(
            {
                "outer_fold": outer_fold,
                "decision_timestamp": decision,
                "score_rows": len(score),
                "score_population_fingerprint": _population_fingerprint(score),
            }
        )
        for role in MODEL_ROLES:
            for regime in TRAINING_REGIMES:
                training = _outer_training(
                    table, decision, role, regime, frozen
                )
                inner = _build_inner_populations(
                    table, frozen, outer_fold, role, regime
                )
                available_count_by_unit[(outer_fold, role, regime)] = len(inner)
                for population in inner:
                    available_inner_lineage.append(population.population_lineage)
                    unique_population_keys.add(
                        (
                            role,
                            regime,
                            population.target_column,
                            population.training_population_fingerprint,
                            population.validation_population_fingerprint,
                        )
                    )
                outer_population_lineage.append(
                    {
                        "outer_fold": outer_fold,
                        "decision_timestamp": decision,
                        "target_role": role,
                        "target_column": _role_target(frozen, role),
                        "training_regime": regime,
                        "training_rows": len(training),
                        "training_population_fingerprint": (
                            _population_fingerprint(training)
                        ),
                        "score_rows": len(score),
                        "score_population_fingerprint": (
                            score_lineage[-1]["score_population_fingerprint"]
                        ),
                        "score_interface_allowed_columns": [
                            "stable_row_id",
                            "decision_timestamp",
                            "source_feature_available_at_decision",
                            "frozen_regime_candidate_columns_only",
                        ],
                        "score_target_columns_excluded": [
                            "target_3y",
                            "tree_target_3y",
                            "label_end_date",
                        ],
                    }
                )

    canonical_candidates = {
        role: [_serialize_tuning_candidate(item) for item in tuning_candidates(frozen, role)]
        for role in MODEL_ROLES
    }
    candidate_contexts: list[dict[str, Any]] = []
    for decision in outer_decisions:
        outer_fold = decision.strftime("decision_%Y%m%dT%H%M%SZ")
        for role in MODEL_ROLES:
            for candidate in tuning_candidates(frozen, role):
                if candidate.parameters.get("random_state") != 42:
                    raise NestedWalkForwardContractError(
                        "deterministic candidate seed drifted"
                    )
                candidate_contexts.append(
                    {
                        "outer_fold": outer_fold,
                        "target_role": role,
                        "candidate_id": candidate.candidate_id,
                        "training_regime": candidate.regime,
                        "selector_method": candidate.selector_method,
                        "available_inner_fold_count": available_count_by_unit[
                            (outer_fold, role, candidate.regime)
                        ],
                        "minimum_valid_inner_folds": int(
                            frozen.contract["inner_protocol"][
                                "minimum_valid_inner_folds_for_tuning"
                            ]
                        ),
                    }
                )

    features_by_regime = {}
    for regime in TRAINING_REGIMES:
        features = candidate_columns_for_regime(frozen, regime)
        validate_candidate_columns(features, frozen, require_exact=False)
        features_by_regime[regime] = {
            "count": len(features),
            "order_sha256": _candidate_order_sha256(features),
            "columns": list(features),
        }
    assert_predictive_selection_inputs(
        {
            "objective": "frozen inner predictive objective",
            "scope": "inner_validation_only",
            "trigger": "predeclared_inner_validation",
        }
    )
    partial_attempts = _partial_attempt_inventory(artifact_root)
    if any(item["outer_result_observed"] for item in partial_attempts):
        raise NestedWalkForwardContractError(
            "a prior partial M1C attempt persisted an outer result"
        )
    preflight = {
        "artifact_class": "M1C_FAIL_CLOSED_PREFLIGHT",
        "artifact_id": artifact_root.name,
        "result": "pass",
        "model_fit_started": False,
        "outer_result_observed_from_prior_attempts": False,
        "execution_attempt": 1,
        "adaptive_retry_permitted": False,
        "counts": {
            "source_rows": len(table),
            "source_row_roles": len(table) * len(MODEL_ROLES),
            "outer_decision_cohorts": len(outer_decisions),
            "frozen_outer_role_records": len(frozen.outer_folds),
            "outer_training_populations": len(outer_population_lineage),
            "frozen_inner_records": len(frozen.inner_folds),
            "available_inner_populations": len(available_inner_lineage),
            "unavailable_inner_records": (
                len(frozen.inner_folds) - len(available_inner_lineage)
            ),
            "maturity_records": len(frozen.label_maturity_ledger),
            "canonical_candidates": sum(map(len, canonical_candidates.values())),
            "outer_candidate_contexts": len(candidate_contexts),
            "unique_fold_local_population_computations": len(
                unique_population_keys
            ),
        },
        "hashes": {
            "preserved_boundaries": preserved,
            "m1a_generated_records": frozen.manifest["records"],
            "m1a_code_lineage": frozen.manifest["code_lineage"],
            "p3_contract": _record(
                ROOT
                / "artifacts/canonical/corrected_us_annual_3y_research_model/configuration/research_model_contract.json",
                "frozen_p3_contract",
            ),
            "execution_code": [
                _record(ROOT / "modeling/nested_walk_forward.py", "m1b_interface"),
                _record(ROOT / "modeling/run_nested_walk_forward.py", "m1c_runner"),
            ],
            "p2_input_preflight": prepared["p2_preflight"],
        },
        "outer_contract": {
            "decision_cohorts": [str(item) for item in outer_decisions],
            "frozen_outer_role_records": list(frozen.outer_folds),
            "score_populations": score_lineage,
            "training_populations": outer_population_lineage,
        },
        "inner_contract": {
            "frozen_inner_records": list(frozen.inner_folds),
            "maturity_ledger": list(frozen.label_maturity_ledger),
            "available_population_lineage": available_inner_lineage,
        },
        "candidate_contract": {
            "canonical_candidates": canonical_candidates,
            "outer_candidate_contexts": candidate_contexts,
        },
        "feature_contract": {
            "frozen_candidate_count": len(frozen.candidate_columns),
            "frozen_candidate_order_sha256": _candidate_order_sha256(
                frozen.candidate_columns
            ),
            "frozen_candidate_columns": list(frozen.candidate_columns),
            "by_training_regime": features_by_regime,
            "exclusions": frozen.contract["feature_contract"]["exclusions"],
            "gate_feature_regime_allowed_raw_inputs": frozen.contract[
                "feature_contract"
            ]["gate_feature_regime_allowed_raw_inputs"],
        },
        "target_contract": {
            role: {
                "target": _role_target(frozen, role),
                "model_contract": frozen.contract["model_roles"][role],
            }
            for role in MODEL_ROLES
        },
        "selector_contract": {
            "methods": list(SELECTOR_METHODS),
            "baseline": frozen.contract["feature_contract"]["baseline_selector"],
            "bounded_variant": frozen.contract["feature_contract"]["bounded_variant"],
        },
        "regime_contract": frozen.contract["training_regimes"],
        "grid_and_seed_contract": {
            role: {
                "grid_points": list(frozen_grid_points(frozen, role)),
                "grid_size": len(frozen_grid_points(frozen, role)),
                "seeds": sorted(
                    {
                        int(point["random_state"])
                        for point in frozen_grid_points(frozen, role)
                    }
                ),
            }
            for role in MODEL_ROLES
        },
        "selection_contract": frozen.contract["selection_rule"],
        "prohibited_inputs": {
            "selection": frozen.contract["selection_rule"][
                "prohibited_selection_inputs"
            ],
            "b1e": frozen.contract["prohibited_inputs"],
            "feature_patterns": frozen.contract["feature_contract"]["exclusions"],
            "score_interface": (
                "target, label-support, future, model-output, policy, portfolio, "
                "and performance columns prohibited"
            ),
        },
        "open_row_contract": frozen.contract["execution_sequence"][
            "unlabeled_open_rows"
        ],
        "cache_contract": {
            "scope": (
                "selector, median preprocessor, and transformed matrices only "
                "for exact SHA-256-identical train/validation populations"
            ),
            "estimators_cached": False,
            "winner_metrics_cached": False,
            "available_population_occurrences": len(available_inner_lineage),
            "unique_population_computations": len(unique_population_keys),
            "selector_methods_per_population": len(SELECTOR_METHODS),
        },
        "deterministic_environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "pandas": pd.__version__,
            "numpy": np.__version__,
            "scikit_learn": sklearn.__version__,
            "lightgbm": lgb.__version__,
            "joblib": joblib.__version__,
            "seed": 42,
            "lightgbm_n_jobs": 1,
        },
        "preserved_partial_attempts": partial_attempts,
    }
    return preflight, outer_decisions


def _excluded_predictions(
    frame: pd.DataFrame,
    role: str,
    frozen: Any,
    outer_fold: str,
    exclusion_code: str,
    *,
    regime: str | None = None,
    winner_candidate_id: str | None = None,
    training: pd.DataFrame | None = None,
) -> pd.DataFrame:
    output = _prediction_shell(frame, role, frozen)
    output["outer_fold"] = outer_fold
    if regime is not None:
        output["training_regime"] = regime
    output["prediction_status"] = "excluded"
    output["prediction"] = np.nan
    output["exclusion_code"] = exclusion_code
    if winner_candidate_id is not None:
        output["winner_candidate_id"] = winner_candidate_id
    if training is not None:
        output["training_rows"] = len(training)
        output["training_population_fingerprint"] = _population_fingerprint(
            training
        )
    return output


def run(artifact_root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    if artifact_root.exists():
        if any(artifact_root.iterdir()):
            raise RuntimeError(
                f"refusing to reuse non-empty M1C artifact root: {artifact_root}"
            )
        raise RuntimeError(
            f"refusing to reuse existing M1C artifact root: {artifact_root}"
        )
    artifact_root.mkdir(parents=True, exist_ok=False)
    try:
        preserved = _verify_preserved_boundaries()
        table, prepared = _prepare_table()
        frozen = prepared["frozen"]
        preflight, outer_decisions = _build_preflight(
            table, prepared, preserved, artifact_root
        )
        _write_json(artifact_root / "preflight.json", preflight)
    except Exception as error:
        _write_json(
            artifact_root / "preflight_failure.json",
            {
                "artifact_class": "M1C_FAIL_CLOSED_PREFLIGHT_FAILURE",
                "artifact_id": artifact_root.name,
                "result": "fail",
                "model_fit_started": False,
                "error_type": type(error).__name__,
                "error": str(error),
            },
        )
        raise

    all_predictions: list[pd.DataFrame] = []
    all_candidates: list[dict[str, Any]] = []
    winners: list[dict[str, Any]] = []
    model_records: list[dict[str, Any]] = []
    fold_local_cache: dict[
        tuple[str, ...], FoldLocalPreparedData | str
    ] = {}
    candidates_by_role = {
        role: tuning_candidates(frozen, role) for role in MODEL_ROLES
    }
    for decision in outer_decisions:
        outer_fold = decision.strftime("decision_%Y%m%dT%H%M%SZ")
        score = _score_population(table, decision)
        for role in MODEL_ROLES:
            role_evaluations: list[CandidateEvaluation] = []
            for regime in TRAINING_REGIMES:
                inner = _build_inner_populations(
                    table, frozen, outer_fold, role, regime
                )
                if not inner:
                    continue
                for candidate in candidates_by_role[role]:
                    if candidate.regime != regime:
                        continue
                    evaluation = evaluate_tuning_candidate(
                        candidate,
                        inner,
                        frozen,
                        execution_scope="frozen_m1c",
                        fold_local_cache=fold_local_cache,
                    )
                    role_evaluations.append(evaluation)
                    all_candidates.append(_serialize_candidate(evaluation))
            if not role_evaluations:
                all_predictions.append(
                    _excluded_predictions(
                        score,
                        role,
                        frozen,
                        outer_fold,
                        "no_mature_inner_validation",
                    )
                )
                continue
            selection_context = TuningRunContext(
                outer_fold,
                decision,
                execution_scope="frozen_m1c",
                selection_locked=False,
            )
            try:
                role_winner = select_inner_winner(
                    role_evaluations,
                    role,
                    frozen,
                    selection_context,
                    selection_inputs={
                        "objective": "frozen inner predictive objective",
                        "scope": "inner_validation_only",
                    },
                )
            except CandidateUnavailable:
                all_predictions.append(
                    _excluded_predictions(
                        score,
                        role,
                        frozen,
                        outer_fold,
                        "no_candidate_minimum_inner_coverage",
                    )
                )
                continue
            winners.append(
                {
                    "outer_fold": outer_fold,
                    "target_role": role,
                    "winner": _serialize_candidate(role_winner),
                }
            )
            training = _outer_training(
                table,
                decision,
                role,
                role_winner.candidate.regime,
                frozen,
            )
            if training.empty or score.empty:
                all_predictions.append(
                    _excluded_predictions(
                        score,
                        role,
                        frozen,
                        outer_fold,
                        "outer_refit_unavailable",
                        regime=role_winner.candidate.regime,
                        winner_candidate_id=role_winner.candidate.candidate_id,
                        training=training,
                    )
                )
                continue
            refit_context = TuningRunContext(
                outer_fold,
                decision,
                execution_scope="frozen_m1c",
                selection_locked=True,
            )
            try:
                score_features = score[
                    [
                        "stable_row_id",
                        "decision_timestamp",
                        "source_feature_available_at_decision",
                        *candidate_columns_for_regime(
                            frozen, role_winner.candidate.regime
                        ),
                    ]
                ].copy()
                refit = refit_locked_outer_candidate(
                    training,
                    score_features,
                    role_winner,
                    frozen,
                    refit_context,
                )
            except CandidateUnavailable:
                all_predictions.append(
                    _excluded_predictions(
                        score,
                        role,
                        frozen,
                        outer_fold,
                        "outer_refit_candidate_unavailable",
                        regime=role_winner.candidate.regime,
                        winner_candidate_id=role_winner.candidate.candidate_id,
                        training=training,
                    )
                )
                continue

            model_path = (
                artifact_root / "models" / outer_fold / role / "model.joblib"
            )
            model_path.parent.mkdir(parents=True, exist_ok=True)
            joblib.dump(refit.estimator, model_path)
            model_sha256 = _sha256_file(model_path)
            model_record = {
                "outer_fold": outer_fold,
                "model_role": role,
                "path": model_path.relative_to(artifact_root).as_posix(),
                "size_bytes": model_path.stat().st_size,
                "sha256": model_sha256,
                "candidate_id": role_winner.candidate.candidate_id,
                "lineage": refit.lineage,
            }
            model_record["model_id"] = _payload_sha256(
                {
                    "outer_fold": outer_fold,
                    "model_role": role,
                    "candidate_id": role_winner.candidate.candidate_id,
                    "model_sha256": model_sha256,
                }
            )
            model_records.append(model_record)

            output = _prediction_shell(score, role, frozen)
            output["outer_fold"] = outer_fold
            output["training_regime"] = role_winner.candidate.regime
            matured = output["target_status"].eq("matured_observed")
            open_rows = output["target_status"].eq(
                "open_2024_2026_unlabeled_excluded_from_metrics"
            )
            output["prediction_status"] = np.select(
                [matured, open_rows],
                [
                    "oos_prediction_available",
                    "production_score_open_unlabeled",
                ],
                default="oos_score_label_unavailable",
            )
            output["prediction"] = refit.predictions
            output["metric_eligible"] = matured
            output["exclusion_code"] = np.select(
                [matured, open_rows],
                [
                    "",
                    "open_unlabeled_2024_2026_outside_predictive_metrics",
                ],
                default="historical_label_unavailable_outside_predictive_metrics",
            )
            output["model_path"] = model_record["path"]
            output["model_sha256"] = model_sha256
            output["winner_candidate_id"] = role_winner.candidate.candidate_id
            output["selected_features_json"] = json.dumps(
                list(refit.selected_features), separators=(",", ":")
            )
            output["selected_feature_count"] = len(refit.selected_features)
            output["parameters_json"] = json.dumps(
                role_winner.candidate.parameters,
                sort_keys=True,
                separators=(",", ":"),
            )
            output["training_rows"] = len(training)
            output["training_population_fingerprint"] = _population_fingerprint(
                training
            )
            output["feature_selection_lineage_sha256"] = _payload_sha256(
                refit.lineage["features"]
            )
            output["preprocessing_lineage_sha256"] = _payload_sha256(
                refit.lineage["transformations"]
            )
            output["outer_refit_lineage_sha256"] = _payload_sha256(
                refit.lineage
            )
            all_predictions.append(output)

    for decision in outer_decisions:
        outer_fold = decision.strftime("decision_%Y%m%dT%H%M%SZ")
        cohort = table.loc[
            pd.to_datetime(table["decision_timestamp"], utc=True).eq(decision)
        ]
        score_ids = set(map(str, _score_population(table, decision)["stable_row_id"]))
        missing = cohort.loc[
            ~cohort["stable_row_id"].astype(str).isin(score_ids)
        ]
        for role in MODEL_ROLES:
            if not missing.empty:
                all_predictions.append(
                    _excluded_predictions(
                        missing,
                        role,
                        frozen,
                        outer_fold,
                        "source_features_unavailable_at_decision",
                    )
                )

    outer_decision_set = set(outer_decisions)
    future = table.loc[
        ~pd.to_datetime(table["decision_timestamp"], utc=True).isin(
            outer_decision_set
        )
    ].copy()
    outer_cutoff = pd.Timestamp(frozen.contract["outer_protocol"]["outer_cutoff"])
    if not future.empty and not pd.to_datetime(
        future["decision_timestamp"], utc=True
    ).gt(outer_cutoff).all():
        raise NestedWalkForwardContractError(
            "rows outside the frozen outer boundary are not strictly future"
        )
    for role in MODEL_ROLES:
        item = _excluded_predictions(
            future,
            role,
            frozen,
            "outside_frozen_2010_2026_outer_boundary",
            "future_decision_after_frozen_scoring_cutoff",
        )
        item["target_status"] = "future_decision_not_scored"
        all_predictions.append(item)

    predictions = pd.concat(all_predictions, ignore_index=True)
    predictions.sort_values(
        ["decision_timestamp", "stable_row_id", "model_role"],
        kind="mergesort",
        inplace=True,
        ignore_index=True,
    )
    if predictions.duplicated(["stable_row_id", "model_role"]).any():
        raise NestedWalkForwardContractError(
            "M1C outer output row-role identity is duplicated"
        )
    expected_row_roles = len(table) * len(MODEL_ROLES)
    if len(predictions) != expected_row_roles:
        raise NestedWalkForwardContractError(
            f"M1C outer output is not row-complete: {len(predictions)} != {expected_row_roles}"
        )
    prediction_path = artifact_root / "outer_oos_predictions.parquet"
    predictions.to_parquet(prediction_path, index=False)
    exclusions = predictions.loc[predictions["exclusion_code"].ne("")].copy()
    exclusions.to_parquet(
        artifact_root / "prediction_exclusions.parquet", index=False
    )
    _write_json(artifact_root / "candidate_evaluations.json", all_candidates)
    _write_json(artifact_root / "winner_decisions.json", winners)
    _write_json(artifact_root / "model_records.json", model_records)
    _write_json(
        artifact_root / "fold_local_selection_preprocessing.json",
        _serialize_fold_local_cache(fold_local_cache),
    )

    matured_predictions = predictions.loc[predictions["metric_eligible"]].copy()
    if matured_predictions["target_status"].ne("matured_observed").any():
        raise NestedWalkForwardContractError(
            "non-mature target entered M1C predictive metrics"
        )
    metric_years = pd.to_datetime(
        matured_predictions["decision_timestamp"], utc=True
    ).dt.year
    if metric_years.isin([2024, 2025, 2026]).any():
        raise NestedWalkForwardContractError(
            "open 2024-2026 row entered predictive metrics"
        )
    fold_metrics: list[dict[str, Any]] = []
    for (fold, role), group in matured_predictions.groupby(
        ["outer_fold", "model_role"], sort=True
    ):
        metrics = (
            _regression_metrics(group["target"], group["prediction"].to_numpy())
            if role == "lightgbm_regression"
            else _tree_metrics(group["target"], group["prediction"].to_numpy())
        )
        fold_metrics.append(
            {
                "outer_fold": fold,
                "model_role": role,
                "rows": len(group),
                "metrics": metrics,
                "metric_scope": "outer_oos_after_all_fold_winners_locked",
            }
        )
    pooled_metrics: list[dict[str, Any]] = []
    for role, group in matured_predictions.groupby("model_role", sort=True):
        metrics = (
            _regression_metrics(group["target"], group["prediction"].to_numpy())
            if role == "lightgbm_regression"
            else _tree_metrics(group["target"], group["prediction"].to_numpy())
        )
        pooled_metrics.append(
            {
                "model_role": role,
                "rows": len(group),
                "metrics": metrics,
                "metric_scope": "pooled_outer_oos_after_all_fold_winners_locked",
            }
        )
    outer_metrics = {
        "claim": {
            "predictive_metrics_only": True,
            "portfolio_performance_calculated": False,
            "outer_oos_used_for_selection": False,
            "open_2024_2026_excluded": True,
        },
        "fold_metrics": fold_metrics,
        "pooled_metrics": pooled_metrics,
    }
    _write_json(artifact_root / "outer_oos_metrics.json", outer_metrics)

    execution_summary = {
        "source_rows": len(table),
        "row_role_records": len(predictions),
        "metric_eligible_outer_oos_predictions": len(matured_predictions),
        "open_2024_2026_production_scores": int(
            predictions["prediction_status"].eq(
                "production_score_open_unlabeled"
            ).sum()
        ),
        "historical_unlabeled_scores": int(
            predictions["prediction_status"].eq(
                "oos_score_label_unavailable"
            ).sum()
        ),
        "explicit_exclusion_records": len(exclusions),
        "future_2027_2028_exclusion_records": int(
            predictions["exclusion_code"].eq(
                "future_decision_after_frozen_scoring_cutoff"
            ).sum()
        ),
        "winner_records": len(winners),
        "model_records": len(model_records),
        "candidate_evaluation_records": len(all_candidates),
        "fold_local_cached_computations": len(fold_local_cache),
        "predictive_metric_fold_records": len(fold_metrics),
        "predictive_metric_pooled_records": len(pooled_metrics),
    }
    _write_json(artifact_root / "execution_summary.json", execution_summary)
    methodology = f"""# M1C methodology

This artifact is the single frozen M1C nested temporal walk-forward run. It uses the exact 43,806-row P2 observed-only model table, the exact P3 2010-2026 outer decision cohorts, the two existing model roles, three frozen training regimes, two frozen selectors, deterministic seed 42, and only the frozen 8-point LightGBM and 4-point tree grids.

Every winner is selected exclusively from expanding, mature, three-year-purged inner validation evidence. Feature selection and median preprocessing are fitted only on each inner-training population. Deterministic caching mechanically reuses those values only when complete training and validation population SHA-256 fingerprints are identical; estimators, predictions, objectives, and winners are never cached. After the inner winner is locked, it is refitted once on the complete eligible outer-training population and scored on the held-out cohort through the target-free scoring interface.

The row-complete output contains {len(predictions):,} stable-row/model-role records, including explicit 2027-2028 future exclusions. It has {len(matured_predictions):,} metric-eligible matured outer-OOS predictions and {execution_summary['open_2024_2026_production_scores']:,} frozen-rule production scores for open 2024-2026 rows. Those open scores and every other unlabeled row are physically excluded from predictive metrics.

## Limitations

No P4 gate, holding, portfolio, NAV, turnover, scenario, CAGR, Sharpe, drawdown, risk-free, B1D, or B1E value entered fitting, tuning, selection, scoring, or reporting. No portfolio performance is calculated here. This remains free-source, observed-only research with incomplete survivorship and event evidence; predictive metrics are not personalized advice or a future-performance claim.
"""
    (artifact_root / "methodology_report.md").write_text(methodology)

    records = [
        _artifact_record(path, artifact_root, "m1c_generated_output")
        for path in sorted(artifact_root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    ]
    manifest = {
        "artifact_class": "M1C_NESTED_WALK_FORWARD_OUTER_OOS",
        "version": artifact_root.name,
        "schema_version": 1,
        "claim": {
            "single_controlled_execution": True,
            "model_execution": True,
            "performance_calculated": False,
            "portfolio_constructed": False,
            "outer_oos_used_for_selection": False,
            "b1e_performance_consumed": False,
            "adaptive_retry_after_outer_result": False,
            "open_2024_2026_outside_predictive_metrics": True,
        },
        "inputs": {
            "p2_manifest_sha256": P2_MANIFEST_SHA256,
            "p3_manifest_sha256": P3_MANIFEST_SHA256,
            "p4_manifest_sha256": P4_MANIFEST_SHA256,
            "b1d_engine_sha256": B1D_ENGINE_SHA256,
            "b1d_test_sha256": B1D_TEST_SHA256,
            "b1e_manifest_sha256": B1E_MANIFEST_SHA256,
            "m1a_manifest_sha256": M1A_MANIFEST_SHA256,
            "p3_contract": _record(
                ROOT
                / "artifacts/canonical/corrected_us_annual_3y_research_model/configuration/research_model_contract.json",
                "frozen_input",
            ),
        },
        "code_lineage": [
            _record(ROOT / "modeling/nested_walk_forward.py", "m1b_interface"),
            _record(ROOT / "modeling/run_nested_walk_forward.py", "m1c_runner"),
        ],
        "configuration": {
            "roles": list(MODEL_ROLES),
            "regimes": list(TRAINING_REGIMES),
            "selectors": list(SELECTOR_METHODS),
            "seeds": [42],
            "grids": {
                role: list(frozen_grid_points(frozen, role))
                for role in MODEL_ROLES
            },
            "cache_scope": (
                "exact-population selector/preprocessor/transformed-matrix reuse only"
            ),
        },
        "outputs": execution_summary,
        "records": records,
        "preflight_sha256": _sha256_file(artifact_root / "preflight.json"),
        "preserved_partial_attempts": [
            {
                key: value
                for key, value in attempt.items()
                if key != "records"
            }
            for attempt in preflight["preserved_partial_attempts"]
        ],
    }
    _write_json(artifact_root / "manifest.json", manifest)
    return {
        "artifact_root": str(artifact_root),
        "manifest_sha256": _sha256_file(artifact_root / "manifest.json"),
        "prediction_rows": len(predictions),
        "matured_rows": len(matured_predictions),
        "open_rows": execution_summary["open_2024_2026_production_scores"],
        "future_exclusion_rows": execution_summary[
            "future_2027_2028_exclusion_records"
        ],
    }


def verify_artifact(
    artifact_root: Path,
    *,
    expected_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    """Independently re-read and verify a completed M1C artifact."""
    manifest_path = artifact_root / "manifest.json"
    if not manifest_path.is_file():
        raise NestedWalkForwardContractError("M1C manifest is missing")
    actual_manifest_sha256 = _sha256_file(manifest_path)
    if (
        expected_manifest_sha256 is not None
        and actual_manifest_sha256 != expected_manifest_sha256
    ):
        raise NestedWalkForwardContractError(
            "M1C manifest SHA-256 does not match the supplied lock"
        )
    manifest = json.loads(manifest_path.read_text())
    claim = manifest.get("claim", {})
    if (
        manifest.get("artifact_class")
        != "M1C_NESTED_WALK_FORWARD_OUTER_OOS"
        or manifest.get("version") != artifact_root.name
        or claim.get("single_controlled_execution") is not True
        or claim.get("performance_calculated") is not False
        or claim.get("portfolio_constructed") is not False
        or claim.get("outer_oos_used_for_selection") is not False
        or claim.get("b1e_performance_consumed") is not False
        or claim.get("open_2024_2026_outside_predictive_metrics") is not True
    ):
        raise NestedWalkForwardContractError("M1C manifest identity/claim drifted")
    records = manifest.get("records")
    if not isinstance(records, list) or not records:
        raise NestedWalkForwardContractError("M1C manifest record inventory is empty")
    record_paths: set[str] = set()
    for record in records:
        relative = str(record.get("path", ""))
        if not relative or relative in record_paths:
            raise NestedWalkForwardContractError(
                "M1C manifest contains an empty or duplicate record path"
            )
        record_paths.add(relative)
        path = artifact_root / relative
        if (
            not path.is_file()
            or path.stat().st_size != int(record.get("size_bytes", -1))
            or _sha256_file(path) != record.get("sha256")
        ):
            raise NestedWalkForwardContractError(
                f"M1C generated record mismatch: {relative}"
            )
    actual_paths = {
        path.relative_to(artifact_root).as_posix()
        for path in artifact_root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    if actual_paths != record_paths:
        raise NestedWalkForwardContractError(
            "M1C artifact contains unmanifested or missing generated files"
        )
    if _sha256_file(artifact_root / "preflight.json") != manifest.get(
        "preflight_sha256"
    ):
        raise NestedWalkForwardContractError("M1C preflight hash drifted")
    preflight = json.loads((artifact_root / "preflight.json").read_text())
    if (
        preflight.get("result") != "pass"
        or preflight.get("model_fit_started") is not False
        or preflight.get("counts", {}).get("frozen_outer_role_records") != 34
        or preflight.get("counts", {}).get("frozen_inner_records") != 306
        or preflight.get("counts", {}).get("maturity_records") != 102
        or preflight.get("counts", {}).get("source_rows") != 43806
        or preflight.get("adaptive_retry_permitted") is not False
        or preflight.get("outer_result_observed_from_prior_attempts") is not False
    ):
        raise NestedWalkForwardContractError("M1C persisted preflight is incomplete")

    preserved = _verify_preserved_boundaries()
    for name, record in preserved.items():
        expected = preflight["hashes"]["preserved_boundaries"].get(name)
        if expected != record:
            raise NestedWalkForwardContractError(
                f"M1C preserved-boundary preflight record drifted: {name}"
            )
    frozen = load_frozen_m1a_contract()
    table, _ = _prepare_table()
    predictions = pd.read_parquet(artifact_root / "outer_oos_predictions.parquet")
    required_columns = {
        "stable_row_id",
        "model_role",
        "outer_fold",
        "prediction_status",
        "prediction",
        "target",
        "target_status",
        "metric_eligible",
        "exclusion_code",
        "winner_candidate_id",
        "model_path",
        "model_sha256",
        "feature_selection_lineage_sha256",
        "preprocessing_lineage_sha256",
        "outer_refit_lineage_sha256",
    }
    if required_columns - set(predictions):
        raise NestedWalkForwardContractError(
            "M1C row-complete prediction schema is incomplete"
        )
    if (
        len(predictions) != len(table) * len(MODEL_ROLES)
        or predictions.duplicated(["stable_row_id", "model_role"]).any()
    ):
        raise NestedWalkForwardContractError(
            "M1C prediction row-role identity is not complete and unique"
        )
    expected_ids = set(map(str, table["stable_row_id"]))
    for role in MODEL_ROLES:
        role_ids = set(
            map(
                str,
                predictions.loc[
                    predictions["model_role"].eq(role), "stable_row_id"
                ],
            )
        )
        if role_ids != expected_ids:
            raise NestedWalkForwardContractError(
                f"M1C prediction identities do not match P2 for {role}"
            )
    decision_year = pd.to_datetime(
        predictions["decision_timestamp"], utc=True, errors="coerce"
    ).dt.year
    open_rows = predictions["prediction_status"].eq(
        "production_score_open_unlabeled"
    )
    if (
        not decision_year.loc[open_rows].isin([2024, 2025, 2026]).all()
        or predictions.loc[open_rows, "metric_eligible"].any()
        or predictions.loc[open_rows, "prediction"].isna().any()
    ):
        raise NestedWalkForwardContractError(
            "M1C open production-score boundary is invalid"
        )
    future = predictions["exclusion_code"].eq(
        "future_decision_after_frozen_scoring_cutoff"
    )
    if (
        not decision_year.loc[future].isin([2027, 2028]).all()
        or predictions.loc[future, "prediction"].notna().any()
        or predictions.loc[future, "metric_eligible"].any()
    ):
        raise NestedWalkForwardContractError(
            "M1C future-decision exclusions are invalid"
        )
    metric_rows = predictions["metric_eligible"].astype(bool)
    if (
        decision_year.loc[metric_rows].isin([2024, 2025, 2026, 2027, 2028]).any()
        or predictions.loc[metric_rows, "target_status"].ne("matured_observed").any()
        or predictions.loc[metric_rows, ["prediction", "target"]].isna().any().any()
    ):
        raise NestedWalkForwardContractError(
            "M1C predictive metric eligibility includes an open or invalid row"
        )
    expected_exclusions = predictions.loc[
        predictions["exclusion_code"].ne("")
    ].reset_index(drop=True)
    exclusions = pd.read_parquet(artifact_root / "prediction_exclusions.parquet")
    pd.testing.assert_frame_equal(
        expected_exclusions,
        exclusions.reset_index(drop=True),
        check_dtype=True,
        check_exact=True,
    )

    model_records = json.loads((artifact_root / "model_records.json").read_text())
    model_by_path = {}
    for record in model_records:
        path = artifact_root / record["path"]
        if (
            not path.is_file()
            or path.stat().st_size != int(record["size_bytes"])
            or _sha256_file(path) != record["sha256"]
            or record["lineage"].get("outer_oos_target_consumed") is not False
            or record["lineage"].get("b1e_performance_consumed") is not False
        ):
            raise NestedWalkForwardContractError(
                f"M1C model or model lineage mismatch: {record['path']}"
            )
        features = record["lineage"]["features"]
        if features.get("diagnostics_sha256") != _payload_sha256(
            features.get("diagnostics")
        ):
            raise NestedWalkForwardContractError(
                "M1C outer-refit feature diagnostics hash drifted"
            )
        model_by_path[record["path"]] = record
    predicted = predictions.loc[predictions["prediction"].notna()]
    for path, group in predicted.groupby("model_path", dropna=False):
        if path not in model_by_path or group["model_sha256"].nunique() != 1:
            raise NestedWalkForwardContractError(
                "M1C prediction-to-model lineage is incomplete"
            )
        if group["model_sha256"].iloc[0] != model_by_path[path]["sha256"]:
            raise NestedWalkForwardContractError(
                "M1C prediction model hash does not match model record"
            )

    cache_records = json.loads(
        (artifact_root / "fold_local_selection_preprocessing.json").read_text()
    )
    diagnostic_hashes = {
        row["diagnostics_sha256"]
        for row in cache_records
        if row["availability_status"] == "available"
        and row["diagnostics_sha256"] == _payload_sha256(row["diagnostics"])
    }
    candidates = json.loads(
        (artifact_root / "candidate_evaluations.json").read_text()
    )
    for candidate in candidates:
        if (
            candidate.get("evidence_scope") != "inner_validation_only"
            or candidate.get("lineage", {}).get("outer_oos_consumed") is not False
            or candidate.get("lineage", {}).get("b1e_performance_consumed") is not False
        ):
            raise NestedWalkForwardContractError(
                "M1C candidate evaluation escaped inner-only evidence"
            )
        for fold in candidate["fold_evaluations"]:
            if fold["availability_status"] != "available":
                continue
            digest = fold["lineage"]["features"]["diagnostics_sha256"]
            if digest not in diagnostic_hashes:
                raise NestedWalkForwardContractError(
                    "M1C candidate diagnostic hash has no materialized record"
                )

    metrics = json.loads((artifact_root / "outer_oos_metrics.json").read_text())
    if (
        metrics.get("claim", {}).get("predictive_metrics_only") is not True
        or metrics.get("claim", {}).get("portfolio_performance_calculated")
        is not False
    ):
        raise NestedWalkForwardContractError("M1C metric claim drifted")
    metric_folds = {row["outer_fold"] for row in metrics["fold_metrics"]}
    if any(year in fold for fold in metric_folds for year in ("2024", "2025", "2026")):
        raise NestedWalkForwardContractError(
            "M1C metric ledger contains an open decision year"
        )
    outputs = manifest["outputs"]
    expected_output_counts = {
        "source_rows": len(table),
        "row_role_records": len(predictions),
        "metric_eligible_outer_oos_predictions": int(metric_rows.sum()),
        "open_2024_2026_production_scores": int(open_rows.sum()),
        "explicit_exclusion_records": len(exclusions),
        "future_2027_2028_exclusion_records": int(future.sum()),
        "model_records": len(model_records),
        "candidate_evaluation_records": len(candidates),
        "fold_local_cached_computations": len(cache_records),
    }
    for key, expected in expected_output_counts.items():
        if int(outputs.get(key, -1)) != expected:
            raise NestedWalkForwardContractError(
                f"M1C manifest output count drifted: {key}"
            )
    return {
        "artifact_root": str(artifact_root),
        "manifest_sha256": actual_manifest_sha256,
        "generated_records_verified": len(records),
        "prediction_rows_verified": len(predictions),
        "metric_eligible_rows_verified": int(metric_rows.sum()),
        "open_rows_verified": int(open_rows.sum()),
        "future_exclusion_rows_verified": int(future.sum()),
        "models_verified": len(model_records),
        "candidate_evaluations_verified": len(candidates),
        "fold_local_diagnostics_verified": len(diagnostic_hashes),
        "preserved_boundaries_verified": len(preserved),
        "m1a_outer_records_verified": len(frozen.outer_folds),
        "m1a_inner_records_verified": len(frozen.inner_folds),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--expected-manifest-sha256")
    args = parser.parse_args()
    if args.verify_only:
        result = verify_artifact(
            args.out,
            expected_manifest_sha256=args.expected_manifest_sha256,
        )
    else:
        if args.expected_manifest_sha256 is not None:
            parser.error("--expected-manifest-sha256 requires --verify-only")
        result = run(args.out)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
