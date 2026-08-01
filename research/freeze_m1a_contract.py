"""Freeze the Session M1A nested walk-forward experiment contract.

This module only reads the frozen P2-P4/B1E evidence and current modeling
source files.  It reconstructs fold tables and writes one new, manifest-backed
contract artifact.  It never fits, tunes, scores, selects, calculates
performance, or consumes an outer-OOS/portfolio result.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import platform
from pathlib import Path
from typing import Any

import pandas as pd

from modeling.build_canonical_research_model import (
    load_canonical_inputs,
    materialize_model_table,
)


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_VERSION = "20260801T000000Z-m1a"
DEFAULT_ARTIFACT_ROOT = ROOT / "artifacts/modeling/nested_walk_forward" / ARTIFACT_VERSION
SOURCE_FREEZE = pd.Timestamp("2026-07-29T20:35:47.117913Z")
OUTER_CUTOFF = pd.Timestamp("2026-07-02T00:00:00Z")
INNER_FOLD_COUNT = 3
MIN_INNER_TRAIN_ROWS = 100
MIN_INNER_VALIDATION_ROWS = 50
MIN_VALID_INNER_FOLDS = 2

FROZEN_MANIFESTS = {
    "p2": (
        "artifacts/canonical/corrected_us_annual/manifest.json",
        "40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3",
    ),
    "p3": (
        "artifacts/canonical/corrected_us_annual_3y_research_model/manifest.json",
        "8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2",
    ),
    "p4": (
        "artifacts/canonical/corrected_us_annual_3y_product/manifest.json",
        "28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7",
    ),
    "b1e": (
        "artifacts/performance/free_data_v1/20260801T011135Z-b1e/manifest.json",
        "23588207812e2e950d3e521c6f2048e7607f20b99b691407c449ceb7752bf37c",
    ),
}

FROZEN_CODE = {
    "b1d_engine": (
        "backtest/free_data_v1_nav.py",
        "880cf13607851dcc1d0947ef7b62ecc798e1add841a707b85a80535cd74d034f",
    ),
    "b1d_tests": (
        "tests/backtest/test_free_data_v1_nav.py",
        "c9cb5b123b7450498a44938c64ebd166164df8a6216764a110e1a95217dee86f",
    ),
}

CURRENT_MODELING_CODE = (
    "modeling/constants.py",
    "modeling/fold_lineage.py",
    "modeling/oos_modeling.py",
    "modeling/build_canonical_research_model.py",
    "modeling/prediction_lineage.py",
    "modeling/freeze_session_v3_1.py",
)

P3_SELECTED_RECORDS = (
    "configuration/research_model_contract.json",
    "predictions/oos_predictions.parquet",
    "support/fold_coverage.parquet",
    "support/oos_lineage_validation.json",
    "support/preflight_validation.json",
    "support/verdict.json",
)
P4_SELECTED_RECORDS = (
    "configuration/product_contract.json",
    "outputs/prediction_lineage.parquet",
    "support/prediction_lineage_validation.json",
    "support/preflight_validation.json",
)
B1E_SELECTED_RECORDS = (
    "configuration/run_configuration.json",
    "support/preflight.json",
    "support/summary.json",
)

TARGETS = {
    "decision_tree": "tree_target_3y",
    "lightgbm_regression": "target_3y",
}
ROLES = tuple(TARGETS)
REGIMES = {
    "broad_downstream_gates": {
        "population": "PIT-available feature rows with an observed target; all P4 gates remain downstream",
        "gate_inputs_as_features": False,
        "training_filter": "target_not_null AND label_end_date.notna() AND source_feature_available_at_own_decision",
    },
    "gate_eligible_training": {
        "population": "broad population plus the existing PIT-available P3 training eligibility inputs",
        "gate_inputs_as_features": False,
        "training_filter": "broad AND piotroski_roa_pos == 1 AND beneish_m_score < -1.78",
    },
    "broad_gate_features": {
        "population": "same broad population; raw gate inputs may be selected only as PIT-available P3 candidates",
        "gate_inputs_as_features": True,
        "training_filter": "same as broad_downstream_gates",
    },
}
GATE_FEATURE_COLUMNS = (
    "feature_market_cap",
    "piotroski_f_score",
    "piotroski_roa_pos",
    "beneish_m_score",
    "altman_z_score",
    "ps_ratio_sector_pct",
    "momentum_12m_prior",
)

MACRO_COLUMNS = (
    "treasury_10y", "treasury_2y", "yield_curve", "fed_funds_rate",
    "credit_spread_baa", "hy_spread", "cpi_yoy", "recession", "vix",
    "real_rate_10y", "credit_tightening", "macro_regime",
    "value_in_high_rate", "value_in_recession", "momentum_in_expansion",
    "quality_in_recession", "levered_in_tight_credit",
)
LABEL_TOKENS = (
    "forward_return", "benchmark_return", "excess_return", "beat_local_market",
    "label_end_date", "label_provenance", "label_status", "label_reason",
    "policy_imputed", "outperformed_benchmark",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).isoformat().replace("+00:00", "Z")
    if value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, dict):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if hasattr(value, "item"):
        return value.item()
    return value


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_value(payload), indent=2, sort_keys=True, allow_nan=False)
        + "\n"
    )


def _record(path: Path, role: str, root: Path = ROOT) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _artifact_record(manifest: dict[str, Any], relative_path: str) -> dict[str, Any]:
    for section in ("records", "validated_inputs", "code_lineage"):
        for item in manifest.get(section, []):
            recorded = item.get("path", "")
            if recorded == relative_path or recorded.endswith("/" + relative_path):
                return item
    raise RuntimeError(f"frozen manifest does not record {relative_path}")


def verify_frozen_boundaries() -> dict[str, Any]:
    """Verify frozen manifests and selected records without consuming metrics."""
    evidence: dict[str, Any] = {"manifests": {}, "selected_records": [], "code": []}
    manifests: dict[str, dict[str, Any]] = {}
    for name, (relative, expected) in FROZEN_MANIFESTS.items():
        path = ROOT / relative
        actual = sha256_file(path)
        if actual != expected:
            raise RuntimeError(f"{name} manifest hash mismatch: {actual}")
        manifests[name] = json.loads(path.read_text())
        evidence["manifests"][name] = _record(path, f"frozen_{name}_manifest") | {
            "expected_sha256": expected,
            "hash_match": True,
        }

    roots = {
        "p3": ROOT / "artifacts/canonical/corrected_us_annual_3y_research_model",
        "p4": ROOT / "artifacts/canonical/corrected_us_annual_3y_product",
        "b1e": ROOT / "artifacts/performance/free_data_v1/20260801T011135Z-b1e",
    }
    for name, paths in {
        "p3": P3_SELECTED_RECORDS,
        "p4": P4_SELECTED_RECORDS,
        "b1e": B1E_SELECTED_RECORDS,
    }.items():
        for relative in paths:
            path = roots[name] / relative
            expected = _artifact_record(manifests[name], relative)
            actual = _record(path, f"frozen_{name}_validated_record")
            if actual["sha256"] != expected["sha256"] or actual["size_bytes"] != expected["size_bytes"]:
                raise RuntimeError(f"{name} selected record mismatch: {relative}")
            evidence["selected_records"].append(
                actual | {"frozen_manifest": name, "expected_sha256": expected["sha256"]}
            )

    for name, (relative, expected) in FROZEN_CODE.items():
        path = ROOT / relative
        actual = sha256_file(path)
        if actual != expected:
            raise RuntimeError(f"{name} hash mismatch: {actual}")
        evidence["code"].append(_record(path, f"frozen_{name}") | {"expected_sha256": expected})
    return evidence


def _iso(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat().replace("+00:00", "Z")


def _population_mask(table: pd.DataFrame, role: str, regime: str) -> pd.Series:
    target = TARGETS[role]
    mask = (
        table[target].notna()
        & table["label_end_date"].notna()
        & table["source_feature_available_at_decision"].astype(bool)
    )
    if regime == "gate_eligible_training":
        mask &= (
            pd.to_numeric(table["piotroski_roa_pos"], errors="coerce").eq(1)
            & pd.to_numeric(table["beneish_m_score"], errors="coerce").lt(-1.78)
        )
    return mask


def _range_for(frame: pd.DataFrame, column: str) -> tuple[str | None, str | None]:
    if frame.empty:
        return None, None
    return _iso(frame[column].min()), _iso(frame[column].max())


def reconstruct_outer_folds(table: pd.DataFrame) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    p3_root = ROOT / "artifacts/canonical/corrected_us_annual_3y_research_model"
    p3_folds = pd.read_parquet(p3_root / "support/fold_coverage.parquet")
    p3_folds["decision_timestamp"] = pd.to_datetime(p3_folds["decision_timestamp"], utc=True)
    table = table.copy()
    table["decision_timestamp"] = pd.to_datetime(table["decision_timestamp"], utc=True)
    table["label_end_date"] = pd.to_datetime(table["label_end_date"], utc=True, errors="coerce")
    outer = []
    for row in p3_folds.itertuples(index=False):
        if row.decision_timestamp > OUTER_CUTOFF:
            raise RuntimeError("P3 fold table contains a decision after the frozen M1A cutoff")
        role_mask = _population_mask(table, row.model_role, "gate_eligible_training")
        train = table.loc[
            role_mask
            & table["decision_timestamp"].lt(row.decision_timestamp)
            & table["label_end_date"].lt(row.decision_timestamp)
        ]
        start, end = _range_for(train, "decision_timestamp")
        purge_pool = table.loc[
            _population_mask(table, row.model_role, "broad_downstream_gates")
            & table["decision_timestamp"].lt(row.decision_timestamp)
        ]
        purge = int(purge_pool["label_end_date"].ge(row.decision_timestamp).sum())
        outer.append(
            {
                "outer_fold": row.fold_id,
                "target_role": row.model_role,
                "decision_timestamp": _iso(row.decision_timestamp),
                "training_decision_start": start,
                "training_decision_end": end,
                "p3_training_rows": int(row.training_rows),
                "p3_score_eligible_rows": int(row.score_eligible_rows),
                "p3_source_rows": int(row.source_rows),
                "p3_source_feature_unavailable_rows": int(row.source_feature_unavailable_rows),
                "p3_max_fitted_label_end": _iso(row.training_label_end_max),
                "p3_strict_label_end_before_decision": bool(row.strict_label_end_before_decision),
                "proposed_gate_regime_training_rows": int(len(train)),
                "proposed_gate_regime_max_fitted_label_end": _iso(train["label_end_date"].max()),
                "horizon_purge_count_from_broad_population": purge,
                "availability_status": "available" if row.status == "fit" else "unavailable",
                "failure_reason": str(row.exclusion_code) if row.exclusion_code else None,
            }
        )
    if len(outer) != 34 or {item["target_role"] for item in outer} != set(ROLES):
        raise RuntimeError("P3 outer fold reconstruction is incomplete")
    return outer, p3_folds


def build_inner_folds(table: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    table = table.copy()
    table["decision_timestamp"] = pd.to_datetime(table["decision_timestamp"], utc=True)
    table["label_end_date"] = pd.to_datetime(table["label_end_date"], utc=True, errors="coerce")
    decisions = sorted(
        timestamp for timestamp in table["decision_timestamp"].dropna().unique()
        if timestamp <= OUTER_CUTOFF
    )
    inner: list[dict[str, Any]] = []
    maturity: list[dict[str, Any]] = []
    for outer_decision in decisions:
        for role in ROLES:
            target_mask = _population_mask(table, role, "broad_downstream_gates")
            candidate_cohorts = []
            for validation_decision in decisions:
                if validation_decision >= outer_decision:
                    continue
                validation = table.loc[
                    target_mask
                    & table["decision_timestamp"].eq(validation_decision)
                    & table["label_end_date"].lt(outer_decision)
                ]
                if len(validation) >= MIN_INNER_VALIDATION_ROWS:
                    candidate_cohorts.append(validation_decision)
            selected = candidate_cohorts[-INNER_FOLD_COUNT:]
            for regime, regime_contract in REGIMES.items():
                regime_mask = _population_mask(table, role, regime)
                for slot in range(1, INNER_FOLD_COUNT + 1):
                    validation_decision = selected[slot - 1] if slot <= len(selected) else None
                    base = {
                        "outer_fold": outer_decision.strftime("decision_%Y%m%dT%H%M%SZ"),
                        "inner_fold": f"inner_{slot}",
                        "target_role": role,
                        "regime": regime,
                        "training_decision_start": None,
                        "training_decision_end": None,
                        "validation_decision_start": _iso(validation_decision),
                        "validation_decision_end": _iso(validation_decision),
                        "max_fitted_label_end": None,
                        "validation_label_end_max": None,
                        "outer_decision_timestamp": _iso(outer_decision),
                        "purge_count": 0,
                        "pre_purge_candidate_rows": 0,
                        "train_row_count": 0,
                        "validation_row_count": 0,
                        "availability_status": "unavailable",
                        "failure_reason": None,
                        "training_filter": regime_contract["training_filter"],
                    }
                    if validation_decision is None:
                        base["failure_reason"] = "no_mature_inner_validation_cohort"
                        inner.append(base)
                        continue
                    pre_purge = table.loc[
                        regime_mask & table["decision_timestamp"].lt(validation_decision)
                    ]
                    train = pre_purge.loc[pre_purge["label_end_date"].lt(validation_decision)]
                    validation = table.loc[
                        regime_mask
                        & table["decision_timestamp"].eq(validation_decision)
                        & table["label_end_date"].lt(outer_decision)
                    ]
                    purge = int(pre_purge["label_end_date"].ge(validation_decision).sum())
                    train_start, train_end = _range_for(train, "decision_timestamp")
                    base.update(
                        {
                            "training_decision_start": train_start,
                            "training_decision_end": train_end,
                            "max_fitted_label_end": _iso(train["label_end_date"].max()),
                            "validation_label_end_max": _iso(validation["label_end_date"].max()),
                            "purge_count": purge,
                            "pre_purge_candidate_rows": int(len(pre_purge)),
                            "train_row_count": int(len(train)),
                            "validation_row_count": int(len(validation)),
                        }
                    )
                    if len(train) < MIN_INNER_TRAIN_ROWS:
                        base["failure_reason"] = "insufficient_inner_training_rows"
                    elif len(validation) < MIN_INNER_VALIDATION_ROWS:
                        base["failure_reason"] = "insufficient_inner_validation_rows"
                    elif not train["label_end_date"].lt(validation_decision).all():
                        base["failure_reason"] = "inner_training_label_not_strictly_before_validation"
                    elif not validation["label_end_date"].lt(outer_decision).all():
                        base["failure_reason"] = "validation_label_not_mature_before_outer_decision"
                    else:
                        base["availability_status"] = "available"
                    inner.append(base)

        for role in ROLES:
            for regime in REGIMES:
                rows = [
                    row for row in inner
                    if row["outer_decision_timestamp"] == _iso(outer_decision)
                    and row["target_role"] == role and row["regime"] == regime
                ]
                available = [row for row in rows if row["availability_status"] == "available"]
                broad = _population_mask(table, role, regime)
                outer_train = table.loc[
                    broad & table["decision_timestamp"].lt(outer_decision)
                    & table["label_end_date"].lt(outer_decision)
                ]
                outer_pool = table.loc[
                    broad & table["decision_timestamp"].lt(outer_decision)
                ]
                maturity.append(
                    {
                        "outer_fold": outer_decision.strftime("decision_%Y%m%dT%H%M%SZ"),
                        "outer_decision_timestamp": _iso(outer_decision),
                        "target_role": role,
                        "regime": regime,
                        "outer_training_rows_after_purge": int(len(outer_train)),
                        "outer_training_decision_start": _range_for(outer_train, "decision_timestamp")[0],
                        "outer_training_decision_end": _range_for(outer_train, "decision_timestamp")[1],
                        "max_fitted_label_end": _iso(outer_train["label_end_date"].max()),
                        "outer_horizon_purge_count": int(outer_pool["label_end_date"].ge(outer_decision).sum()),
                        "strict_label_end_before_outer_decision": bool(
                            outer_train.empty or outer_train["label_end_date"].lt(outer_decision).all()
                        ),
                        "valid_inner_fold_count": len(available),
                        "minimum_valid_inner_folds": MIN_VALID_INNER_FOLDS,
                        "availability_status": (
                            "available_for_tuning" if len(available) >= MIN_VALID_INNER_FOLDS else "unavailable"
                        ),
                        "failure_reason": None if len(available) >= MIN_VALID_INNER_FOLDS else "fewer_than_two_valid_inner_folds",
                        "open_or_unlabeled_rows_excluded": int(
                            table.loc[
                                table["decision_timestamp"].eq(outer_decision)
                                & table[TARGETS[role]].isna()
                            ].shape[0]
                        ),
                    }
                )
    if len(inner) != len(decisions) * len(ROLES) * len(REGIMES) * INNER_FOLD_COUNT:
        raise RuntimeError("inner fold table has unexpected cardinality")
    return inner, maturity


def build_contract(evidence: dict[str, Any], table: pd.DataFrame, p3_folds: pd.DataFrame) -> dict[str, Any]:
    outer, _ = reconstruct_outer_folds(table)
    inner, maturity = build_inner_folds(table)
    p3_config = json.loads(
        (ROOT / "artifacts/canonical/corrected_us_annual_3y_research_model/configuration/research_model_contract.json").read_text()
    )
    candidates = p3_config["feature_contract"]["candidate_columns"]
    if len(candidates) != 200 or any(column not in candidates for column in GATE_FEATURE_COLUMNS):
        raise RuntimeError("P3 candidate feature universe or gate-feature contract drifted")
    return {
        "schema_version": 1,
        "artifact_class": "M1A_NESTED_WALK_FORWARD_EXPERIMENT_CONTRACT",
        "session": "M1A",
        "version": ARTIFACT_VERSION,
        "created_at_utc": "2026-08-01T00:00:00Z",
        "claim": {
            "contract_only": True,
            "model_execution": False,
            "performance_calculated": False,
            "winning_model_selected": False,
            "outer_oos_consumed_for_selection": False,
            "b1e_performance_consumed_for_selection": False,
        },
        "frozen_boundaries": {
            "p2_manifest_sha256": FROZEN_MANIFESTS["p2"][1],
            "p3_manifest_sha256": FROZEN_MANIFESTS["p3"][1],
            "p3_oos_predictions_sha256": next(
                item["sha256"] for item in evidence["selected_records"]
                if item["path"].endswith("predictions/oos_predictions.parquet")
            ),
            "p4_manifest_sha256": FROZEN_MANIFESTS["p4"][1],
            "b1d_engine_sha256": FROZEN_CODE["b1d_engine"][1],
            "b1e_manifest_sha256": FROZEN_MANIFESTS["b1e"][1],
        },
        "evidence_summary": {
            "p3_outer_fold_records": len(outer),
            "p3_outer_decision_cohorts": sorted({row["decision_timestamp"] for row in outer}),
            "p3_outer_roles": list(ROLES),
            "p3_outer_fit_records": int((p3_folds["status"] == "fit").sum()),
            "p3_outer_failed_closed_records": int((p3_folds["status"] != "fit").sum()),
            "p3_candidate_feature_count": len(candidates),
            "p3_candidate_feature_order_sha256": hashlib.sha256(
                json.dumps(candidates, separators=(",", ":")).encode()
            ).hexdigest(),
            "canonical_rows": int(len(table)),
            "canonical_decision_cohorts_in_m1a": 17,
            "unlabeled_open_decision_years_excluded_from_tuning": [2024, 2025, 2026],
        },
        "outer_protocol": {
            "decision_boundary": "exact P3 annual decision_timestamp cohorts; no random split",
            "outer_cutoff": _iso(OUTER_CUTOFF),
            "outer_training_rule": "row feature availability at own decision AND observed target AND label_end_date strictly before outer decision",
            "outer_oos_rule": "outer decision cohort is held out from every inner operation until final locked evaluation",
            "horizon": "3y",
            "horizon_purge_rule": "purge every candidate row whose label_end_date is on or after the applicable training/validation/outer boundary",
            "outer_folds": "outer_folds.json",
        },
        "inner_protocol": {
            "fold_type": "expanding annual temporal folds within each outer-training population",
            "validation_cohorts_per_outer": INNER_FOLD_COUNT,
            "validation_selection": "latest three pre-outer decision cohorts whose broad-role validation labels are fully mature before the outer decision and have at least 50 rows",
            "minimum_training_rows": MIN_INNER_TRAIN_ROWS,
            "minimum_validation_rows": MIN_INNER_VALIDATION_ROWS,
            "minimum_valid_inner_folds_for_tuning": MIN_VALID_INNER_FOLDS,
            "inner_training_rule": "decision_timestamp strictly before validation decision AND label_end_date strictly before validation decision",
            "inner_validation_rule": "validation decision cohort is inside outer training; label_end_date strictly before outer decision; no refit on validation",
            "locality": "every transformation, selector, ranking, redundancy calculation, and hyperparameter choice is fit on inner-training rows only",
            "inner_folds": "inner_folds.json",
            "label_maturity_ledger": "label_maturity_ledger.json",
        },
        "feature_contract": {
            "candidate_source": "P3 configuration feature_contract.candidate_columns",
            "candidate_count": 200,
            "candidate_order_sha256": hashlib.sha256(
                json.dumps(candidates, separators=(",", ":")).encode()
            ).hexdigest(),
            "availability": "candidate value and source feature materialization must be available at the row decision timestamp",
            "baseline_selector": {
                "name": "p3_fold_local_ic_selector",
                "implementation_reference": "modeling/fold_lineage.py::select_fold_features",
                "top_n": 28,
                "min_abs_ic": 0.02,
                "min_ic_years": 1,
                "min_group_size": 30,
                "corr_threshold": 0.85,
                "fit_scope": "inner-training only; refit on full outer-training only after winner is frozen",
            },
            "bounded_variant": {
                "name": "deterministic_stability_selection_with_redundancy_pruning",
                "subwindows": "five deterministic expanding prefixes of each inner-training decision range; no random resampling",
                "selection_frequency_minimum": 0.60,
                "direction_stability_minimum": 0.60,
                "missingness_maximum": 0.50,
                "minimum_abs_median_ic": 0.02,
                "redundancy_spearman_abs_maximum": 0.85,
                "selected_feature_cap": 28,
                "minimum_selected_features": 5,
                "fit_scope": "all subwindows, IC, missingness, redundancy, and ranking use inner-training rows only",
            },
            "recorded_diagnostics": [
                "selection_frequency_per_fold",
                "sign_direction_stability_per_fold",
                "fold_level_spearman_ic",
                "missingness_rate_per_inner_training_fold",
                "redundancy_pairs_and_pruned_features",
                "selected_set_size",
            ],
            "exclusions": {
                "targets": ["target_3y", "tree_target_3y", "stock_return", "outperformed_benchmark", "benchmark_return", "relative_return"],
                "label_support_and_availability": ["label_end_date", "label_provenance", "label_status", "label_reason", "target_status_3y", "target_provenance_3y", "policy_imputed", "source_feature_available_at_decision"],
                "identity_and_time": ["stable_row_id", "entity_id", "cik", "ticker", "market", "fiscal_year", "period_type", "availability_timestamp", "event_time_materialization_timestamp", "decision_timestamp", "prediction_timestamp", "entry_timestamp", "filed_date"],
                "future_values_and_model_outputs": ["forward_*", "future_*", "prediction", "prediction_*", "score_*", "model_*", "ml_*", "alpha_*", "composite_score"],
                "policy_outcomes": ["policy_imputed", "policy_*", "scenario_*"],
                "uncertified_macro_columns": list(MACRO_COLUMNS),
                "fraud_labels_and_outputs": ["fraud_confirmed", "fraud_suspect", "fraud_label", "fraud_score_*"],
                "gate_outputs": ["gate_*_pass", "gate_*_status", "gate_*_provenance", "hard_gate_exclusion_codes"],
            },
            "gate_feature_regime_allowed_raw_inputs": list(GATE_FEATURE_COLUMNS),
        },
        "model_roles": {
            "lightgbm_regression": {
                "family": "lightgbm.LGBMRegressor",
                "target": "target_3y",
                "grid": {
                    "n_estimators": [300, 600],
                    "max_depth": [4, 6],
                    "num_leaves": [31, 63],
                    "learning_rate": [0.03],
                    "subsample": [0.8],
                    "colsample_bytree": [0.7],
                    "min_child_samples": [20],
                    "reg_alpha": [0.1],
                    "reg_lambda": [1.0],
                    "random_state": [42],
                    "n_jobs": [1],
                    "verbose": [-1],
                    "deterministic": [True],
                    "force_col_wise": [True],
                },
                "grid_size": 8,
                "primary_objective": "median inner-fold Spearman IC",
                "diagnostics": ["IC dispersion", "positive-fold frequency", "Pearson correlation", "MAE", "RMSE", "coverage"],
                "regression_target_clip": [-1.0, 5.0],
            },
            "decision_tree": {
                "family": "sklearn.tree.DecisionTreeClassifier",
                "target": "tree_target_3y",
                "grid": {
                    "max_depth": [3, 4],
                    "min_samples_leaf": [50, 100],
                    "min_samples_split": [100],
                    "random_state": [42],
                    "class_weight": ["fold_local_{0:1.0,1:n_negative/max(n_positive,1)}"],
                },
                "grid_size": 4,
                "primary_objective": "median inner-fold ROC AUC",
                "diagnostics": ["PR AUC", "Brier score", "calibration", "prevalence", "coverage"],
                "probability_gate_threshold": 0.55,
                "two_class_requirement": "fail closed when inner-training target has fewer than two classes",
            },
        },
        "selection_rule": {
            "roles_tuned_independently": True,
            "candidate_unit": "role x regime x selector method x frozen parameter-grid point",
            "minimum_coverage": "candidate must have at least MIN_VALID_INNER_FOLDS valid folds; missing folds are unavailable, never imputed",
            "lightgbm_lexicographic_order": ["higher median Spearman IC", "lower IC standard deviation", "higher positive-fold frequency", "fewer selected features", "lower complexity score", "lexicographically smaller canonical candidate id"],
            "tree_lexicographic_order": ["higher median ROC AUC", "lower ROC AUC standard deviation", "fewer selected features", "lower complexity score", "lexicographically smaller canonical candidate id"],
            "complexity_scores": {
                "lightgbm": "n_estimators * num_leaves * max(1,max_depth)",
                "decision_tree": "max_depth * min_samples_leaf",
            },
            "tie_tolerance": 1e-12,
            "prohibited_selection_inputs": ["outer-OOS predictions or labels", "P4 holdings or gates", "B1D/B1E NAV or metrics", "CAGR", "Sharpe", "Sortino", "alpha", "drawdown", "information ratio", "turnover", "hit rate", "scenario results", "portfolio NAV", "risk-free values"],
        },
        "training_regimes": REGIMES,
        "execution_sequence": {
            "m1b": "implement this contract and synthetic fail-closed leakage tests; no historical fitting, tuning, scoring, or performance",
            "m1c": "run exactly once on frozen P2 and these roles; choose only from inner evidence; materialize a new row-complete outer-OOS artifact; preserve P3",
            "m1d": "lock one M1C route before consuming unchanged P4 and B1D/B1E evaluation; evaluate once; never retune on the same outer history",
            "unlabeled_open_rows": "2024-2026 remain outside tuning and historical evaluation; production scoring only under the frozen rule",
            "performance_thresholds": "30% aggregate net CAGR and 1.0 zero-risk-free diagnostic Sharpe are final reporting thresholds only, never objectives",
            "rate_namespace": "keep zero_risk_free_sharpe_diagnostic separate from unavailable dgs1mo_alfred_2026_07_17",
        },
        "environment_contract": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "determinism": "Python hash-independent sorted JSON; seeds fixed at 42; LightGBM n_jobs=1, deterministic=True, force_col_wise=True; no random split",
            "data_freeze": _iso(SOURCE_FREEZE),
            "external_data": "prohibited; only hash-validated local evidence",
        },
        "prohibited_inputs": {
            "b1e_paths": [
                "artifacts/performance/free_data_v1/20260801T011135Z-b1e/outputs/metrics.parquet",
                "artifacts/performance/free_data_v1/20260801T011135Z-b1e/report/product_report.md",
                "artifacts/performance/free_data_v1/20260801T011135Z-b1e/outputs/namespaces/",
            ],
            "meaning": "B1E may be hash-verified as a frozen boundary but no B1E value, metric, scenario, or report result may enter feature, parameter, regime, model, or winner selection",
            "fail_closed_rule": "any attempted performance-directed selection raises a contract error; no retry or adaptive retuning is allowed",
        },
        "tables": {
            "outer_folds": "outer_folds.json",
            "inner_folds": "inner_folds.json",
            "label_maturity_ledger": "label_maturity_ledger.json",
            "evidence_revalidation": "evidence_revalidation.json",
        },
    }


def _write_artifact(root: Path, contract: dict[str, Any], evidence: dict[str, Any], outer: list[dict[str, Any]], inner: list[dict[str, Any]], maturity: list[dict[str, Any]], table: pd.DataFrame) -> dict[str, Any]:
    if root.exists() and any(root.iterdir()):
        raise RuntimeError(f"refusing to reuse non-empty M1A artifact root: {root}")
    root.mkdir(parents=True, exist_ok=False)
    write_json(root / "experiment_contract.json", contract)
    write_json(root / "evidence_revalidation.json", evidence)
    write_json(root / "outer_folds.json", outer)
    write_json(root / "inner_folds.json", inner)
    write_json(root / "label_maturity_ledger.json", maturity)
    report = """# Session M1A methodology report

## Frozen outcome

M1A freezes a contract only. It performs no model fitting, tuning, scoring,
winner selection, portfolio construction, or performance calculation. The
P2, P3, P4, B1D, and B1E boundaries remain untouched.

## Confirmed evidence

- The four frozen manifest hashes and selected P3/P4/B1E records match their
  manifests; the B1D engine and focused test hashes match the frozen values.
- P3 has 17 exact annual outer decision cohorts from 2010 through 2026 and
  two role records per cohort. Its 2010-2013 records fail closed for missing
  eligible history; its 2014-2026 records are fitted with strict label-end
  ordering. The exact records are preserved in `outer_folds.json`.
- The P3 candidate universe is exactly 200 numeric, point-in-time candidates;
  no certified macro vintage is available. Candidate, target, support,
  timestamp, model-output, policy-outcome, and gate-output exclusions are
  explicit in `experiment_contract.json`.

## Frozen protocol

Each outer training population uses the latest three mature annual validation
cohorts available before the outer decision. Inner training expands through
time, purges labels ending on or after the inner boundary, and requires at
least 100 training and 50 validation rows. At least two valid inner folds are
required for a tuning result; otherwise the fold/regime is unavailable. Every
selector, missingness rule, redundancy calculation, imputation rule, model
parameter, and winner choice is local to inner training and is never refit on
validation. The selected pipeline is refit once on the complete outer-training
population only after its choice is frozen.

## Risks and unknowns carried forward

The dataset remains historically enriched rather than comprehensively
survivorship-free. The current P3 route uses a gate-eligible training filter;
M1 compares that fixed rule with the two predeclared broad-universe regimes,
while retaining P4 gates and portfolio rules unchanged. The bounded variant's
stability evidence is deliberately limited to deterministic expanding
subwindows. No performance result can resolve these modeling risks.

## Exact next task

M1B: implement this frozen contract and synthetic leakage tests without
historical model execution.
"""
    (root / "methodology_report.md").write_text(report)

    generated = [
        root / "experiment_contract.json",
        root / "evidence_revalidation.json",
        root / "outer_folds.json",
        root / "inner_folds.json",
        root / "label_maturity_ledger.json",
        root / "methodology_report.md",
    ]
    records = [_record(path, "m1a_generated_output", root) for path in generated]
    manifest = {
        "schema_version": 1,
        "artifact_class": "M1A_NESTED_WALK_FORWARD_EXPERIMENT_CONTRACT",
        "session": "M1A",
        "version": ARTIFACT_VERSION,
        "created_at_utc": "2026-08-01T00:00:00Z",
        "claim": contract["claim"],
        "frozen_boundaries": contract["frozen_boundaries"],
        "source_rows": int(len(table)),
        "outer_fold_records": len(outer),
        "inner_fold_records": len(inner),
        "label_maturity_records": len(maturity),
        "validated_inputs": evidence["manifests"]["p2"],
        "evidence_revalidation": "evidence_revalidation.json",
        "records": records,
        "configuration": {"path": "experiment_contract.json", "sha256": next(item["sha256"] for item in records if item["path"] == "experiment_contract.json")},
        "code_lineage": [
            _record(ROOT / path, "current_modeling_code") for path in CURRENT_MODELING_CODE
        ],
        "contract_freezer": _record(ROOT / "research/freeze_m1a_contract.py", "m1a_contract_freezer"),
        "environment": contract["environment_contract"],
        "limitations": contract["execution_sequence"],
    }
    write_json(root / "manifest.json", manifest)
    return manifest


def build(artifact_root: Path = DEFAULT_ARTIFACT_ROOT) -> dict[str, Any]:
    evidence = verify_frozen_boundaries()
    features, labels, *_ = load_canonical_inputs()
    table = materialize_model_table(features, labels)
    table["decision_timestamp"] = pd.to_datetime(table["decision_timestamp"], utc=True)
    table["label_end_date"] = pd.to_datetime(table["label_end_date"], utc=True, errors="coerce")
    outer, p3_folds = reconstruct_outer_folds(table)
    inner, maturity = build_inner_folds(table)
    contract = build_contract(evidence, table, p3_folds)
    return _write_artifact(artifact_root, contract, evidence, outer, inner, maturity, table)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    args = parser.parse_args()
    manifest = build(args.artifact_root)
    print(json.dumps({"artifact_root": str(args.artifact_root), "records": len(manifest["records"])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
