"""Build the canonical leakage-safe US annual three-year research/model route.

The command consumes only hash-validated records from Product Session P2's
canonical observed-only artifact. It preserves every source row for both model
roles, fits feature selection and median preprocessing inside each historical
decision fold, and emits only out-of-sample predictions or exact fail-closed
exclusions. It does not run a backtest, construct a portfolio, calculate
performance, source data, or synthesize unavailable macro values.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import subprocess
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
import sklearn
from pandas.api.types import is_bool_dtype, is_numeric_dtype

from modeling.oos_modeling import (
    EXCLUSION_FUTURE,
    MODEL_ROLES,
    exclusion_report,
    fit_fold_role,
)
from modeling.constants import (
    ALTMAN_Z_MIN,
    BENEISH_THRESHOLD,
    EXCLUDE_COLS,
    EXCLUDE_PATTERNS,
    MAX_MARKET_CAP_PROD,
    MOMENTUM_12M_MIN,
    PIOTROSKI_MIN,
    TREE_THRESHOLD,
    VALUE_GATE_PCT,
)
from modeling.fold_lineage import SelectorConfig


ROOT = Path(__file__).resolve().parents[1]
CANONICAL_P2 = ROOT / "artifacts/canonical/corrected_us_annual"
DEFAULT_ARTIFACT_ROOT = (
    ROOT / "artifacts/canonical/corrected_us_annual_3y_research_model"
)
CANONICAL_P2_MANIFEST_SHA256 = (
    "40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3"
)

FEATURES_PATH = (
    CANONICAL_P2 / "outputs/observed_only/features_taxonomy.parquet"
)
LABELS_PATH = CANONICAL_P2 / "inputs/observed_only_labels.parquet"
ROW_SUPPORT_PATH = CANONICAL_P2 / "inputs/observed_only_row_horizon.parquet"
SUMMARY_PATH = CANONICAL_P2 / "support/feature_population_summary.parquet"
CONSUMED_INPUTS = (
    FEATURES_PATH,
    LABELS_PATH,
    ROW_SUPPORT_PATH,
    SUMMARY_PATH,
)

HORIZONS = ("6m", "1y", "2y", "3y", "5y")
IDENTITY_KEYS = ["entity_id", "cik", "ticker", "fiscal_year"]
SOURCE_ROWS = 43_806
MIN_MARKET_CAP = 50_000_000
FEATURE_SELECTOR = SelectorConfig(top_n=28)

EXCLUSION_SOURCE_UNAVAILABLE = "row_features_unavailable_at_decision"
EXCLUSION_NO_SCORE_ROWS = "fold_no_decision_available_score_rows"

MACRO_COLUMNS = {
    "treasury_10y",
    "treasury_2y",
    "yield_curve",
    "fed_funds_rate",
    "credit_spread_baa",
    "hy_spread",
    "cpi_yoy",
    "recession",
    "vix",
    "real_rate_10y",
    "credit_tightening",
    "macro_regime",
    "value_in_high_rate",
    "value_in_recession",
    "momentum_in_expansion",
    "quality_in_recession",
    "levered_in_tight_credit",
}

LABEL_DERIVED_TOKENS = (
    "forward_return",
    "benchmark_return",
    "excess_return",
    "beat_local_market",
    "label_end_date",
    "label_provenance",
    "label_status",
    "label_reason",
    "policy_imputed",
    "outperformed_benchmark",
)

SOURCE_LINEAGE_COLUMNS = [
    "source_manifest_sha256",
    "source_dataset_artifact_id",
    "source_labels_artifact_id",
    "source_row_support_artifact_id",
]
MODEL_LINEAGE_COLUMNS = [
    "feature_artifact_id",
    "preprocessing_artifact_id",
    "target_artifact_id",
    "model_configuration_artifact_id",
    "model_artifact_id",
    "training_rows",
    "training_label_end_max",
    "selected_feature_count",
    "selected_features_json",
    "training_population_fingerprint",
]


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
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    )


def _indexed_records(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    records = {}
    for section in ("records", "validated_inputs"):
        for item in manifest.get(section, []):
            if "size_bytes" in item:
                records[item["path"]] = item
    return records


def validate_canonical_manifest() -> tuple[dict[str, Any], dict[str, Any]]:
    """Fail closed unless the P2 manifest and every consumed record are exact."""
    manifest_path = CANONICAL_P2 / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError(f"canonical P2 manifest is missing: {manifest_path}")
    actual_manifest_hash = sha256_file(manifest_path)
    if actual_manifest_hash != CANONICAL_P2_MANIFEST_SHA256:
        raise RuntimeError(
            "canonical P2 manifest hash mismatch: "
            f"expected={CANONICAL_P2_MANIFEST_SHA256} "
            f"actual={actual_manifest_hash}"
        )
    manifest = json.loads(manifest_path.read_text())
    entrypoint = manifest.get("canonical_entrypoint", {})
    if (
        manifest.get("artifact_class")
        != "CANONICAL_CORRECTED_US_ANNUAL_FEATURE_POPULATION"
        or entrypoint.get("primary_dataset")
        != "outputs/observed_only/features_taxonomy.parquet"
        or entrypoint.get("population") != "observed_only"
        or manifest.get("primary_dataset")
        != "outputs/observed_only/features_taxonomy.parquet"
    ):
        raise RuntimeError("canonical P2 observed-only dataset identity drifted")
    claim = manifest.get("claim", {})
    validation = manifest.get("canonical_validation", {})
    scope = validation.get("scope", {})
    if (
        not claim.get("canonical_dataset_ready")
        or claim.get("primary_population") != "observed_only"
        or int(claim.get("policy_only_additions", -1)) != 0
        or validation.get("rows") != SOURCE_ROWS
        or validation.get("stable_row_ids") != SOURCE_ROWS
        or validation.get("primary_population") != "observed_only"
        or validation.get("policy_imputed_rows") != 0
        or scope != {"market": "US", "period_type": "annual"}
    ):
        raise RuntimeError("canonical P2 scope or population contract drifted")

    indexed = _indexed_records(manifest)
    validated = []
    for path in CONSUMED_INPUTS:
        relative = path.relative_to(ROOT).as_posix()
        expected = indexed.get(relative)
        if expected is None:
            raise RuntimeError(
                f"consumed canonical record is absent from P2 manifest: {relative}"
            )
        if not path.is_file():
            raise RuntimeError(f"consumed canonical record is missing: {relative}")
        actual_size = path.stat().st_size
        actual_hash = sha256_file(path)
        if actual_size != int(expected["size_bytes"]):
            raise RuntimeError(
                f"consumed canonical record size mismatch: {relative} "
                f"expected={expected['size_bytes']} actual={actual_size}"
            )
        if actual_hash != expected["sha256"]:
            raise RuntimeError(
                f"consumed canonical record hash mismatch: {relative} "
                f"expected={expected['sha256']} actual={actual_hash}"
            )
        validated.append(
            {
                "path": relative,
                "size_bytes": actual_size,
                "sha256": actual_hash,
            }
        )
    preflight = {
        "result": "pass",
        "canonical_p2_manifest_sha256": actual_manifest_hash,
        "records_validated": validated,
        "records_validated_count": len(validated),
        "unconsumed_p2_records_revalidated": 0,
        "primary_population": "observed_only",
        "policy_only_additions": 0,
    }
    return manifest, preflight


def _series_equal(left: pd.Series, right: pd.Series) -> bool:
    if pd.api.types.is_datetime64_any_dtype(left.dtype) or (
        pd.api.types.is_datetime64_any_dtype(right.dtype)
    ):
        left_values = pd.to_datetime(left, utc=True, errors="coerce")
        right_values = pd.to_datetime(right, utc=True, errors="coerce")
        return bool(left_values.equals(right_values))
    if is_numeric_dtype(left.dtype) and is_numeric_dtype(right.dtype):
        left_values = pd.to_numeric(left, errors="coerce").to_numpy(dtype=float)
        right_values = pd.to_numeric(right, errors="coerce").to_numpy(dtype=float)
        return bool(np.allclose(left_values, right_values, equal_nan=True))
    left_values = left.astype("string").fillna("<NA>")
    right_values = right.astype("string").fillna("<NA>")
    return bool(left_values.equals(right_values))


def validate_source_contract(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    row_support: pd.DataFrame,
    summary: pd.DataFrame,
) -> dict[str, Any]:
    """Cross-check canonical features, observed labels, and support evidence."""
    required_feature_columns = {
        *IDENTITY_KEYS,
        "stable_row_id",
        "market",
        "period_type",
        "population",
        "availability_timestamp",
        "availability_provenance",
        "event_time_materialization_timestamp",
        "decision_timestamp",
        "prediction_timestamp",
        "label_status_3y",
        "label_reason_3y",
        "label_end_date_3y",
        "forward_return_3y",
        "benchmark_return_3y",
        "excess_return_local_3y",
        "beat_local_market_3y",
        "label_provenance_3y",
        "policy_imputed_3y",
    }
    missing = sorted(required_feature_columns - set(features.columns))
    if missing:
        raise RuntimeError(f"canonical primary dataset is missing columns: {missing}")
    if (
        len(features) != SOURCE_ROWS
        or features["stable_row_id"].isna().any()
        or features["stable_row_id"].duplicated().any()
        or features["stable_row_id"].nunique() != SOURCE_ROWS
    ):
        raise RuntimeError("canonical primary stable-row identity drifted")
    if (
        not features["market"].eq("US").all()
        or not features["period_type"].eq("annual").all()
        or not features["population"].eq("observed_only").all()
        or not features["availability_provenance"].eq(
            "sec_primary_filing"
        ).all()
    ):
        raise RuntimeError("canonical primary scope or availability provenance drifted")
    if features.duplicated(IDENTITY_KEYS).any():
        raise RuntimeError("canonical primary entity-year identity is duplicated")

    support_keys = [*IDENTITY_KEYS, "horizon"]
    if len(row_support) != SOURCE_ROWS * len(HORIZONS):
        raise RuntimeError("canonical observed row-horizon support is not row complete")
    if row_support.duplicated(support_keys).any():
        raise RuntimeError("canonical observed row-horizon support is duplicated")
    if set(row_support["horizon"]) != set(HORIZONS):
        raise RuntimeError("canonical observed row-horizon support horizons drifted")
    if not row_support["population"].eq("observed_only").all():
        raise RuntimeError("non-observed row entered observed-only support")
    horizon_counts = row_support.groupby("horizon").size().to_dict()
    if any(int(horizon_counts.get(horizon, 0)) != SOURCE_ROWS for horizon in HORIZONS):
        raise RuntimeError("canonical observed support is not complete by horizon")

    support_3y = row_support.loc[
        row_support["horizon"].eq("3y")
    ].copy()
    if not set(support_3y["classification"]).issubset(
        {"supported", "unavailable", "excluded"}
    ):
        raise RuntimeError("canonical observed 3y support has unknown classifications")
    status_counts = support_3y["classification"].value_counts().to_dict()
    if sum(int(value) for value in status_counts.values()) != SOURCE_ROWS:
        raise RuntimeError("canonical observed 3y support does not partition source rows")

    label_keys = [*IDENTITY_KEYS, "horizon"]
    if labels.duplicated(label_keys).any():
        raise RuntimeError("canonical observed labels have duplicate row-horizon identity")
    if not labels["population"].eq("observed_only").all():
        raise RuntimeError("non-observed label entered canonical observed-only labels")
    if labels["policy_imputed"].fillna(False).astype(bool).any():
        raise RuntimeError("policy-imputed label entered canonical observed-only labels")
    labels_3y = labels.loc[labels["horizon"].eq("3y")].copy()
    if len(labels_3y) != int(status_counts.get("supported", 0)):
        raise RuntimeError("observed 3y label count differs from supported row count")
    supported_keys = set(
        map(tuple, support_3y.loc[
            support_3y["classification"].eq("supported"), IDENTITY_KEYS
        ].to_numpy())
    )
    label_identity = set(map(tuple, labels_3y[IDENTITY_KEYS].to_numpy()))
    if label_identity != supported_keys:
        raise RuntimeError("observed 3y labels do not exactly match supported identities")

    support_check = features[
        [*IDENTITY_KEYS, "label_status_3y", "label_reason_3y"]
    ].merge(
        support_3y[[*IDENTITY_KEYS, "classification", "reason"]],
        on=IDENTITY_KEYS,
        how="left",
        validate="one_to_one",
    )
    if not _series_equal(
        support_check["label_status_3y"], support_check["classification"]
    ) or not _series_equal(
        support_check["label_reason_3y"], support_check["reason"]
    ):
        raise RuntimeError("embedded 3y support differs from same-artifact support")

    label_mapping = {
        "decision_timestamp": "decision_timestamp",
        "prediction_timestamp": "prediction_timestamp",
        "label_end_date_3y": "label_end_date",
        "forward_return_3y": "stock_return",
        "benchmark_return_3y": "benchmark_return",
        "excess_return_local_3y": "relative_return",
        "beat_local_market_3y": "outperformed_benchmark",
        "label_provenance_3y": "label_provenance",
        "policy_imputed_3y": "policy_imputed",
    }
    embedded = features.loc[
        features["label_status_3y"].eq("supported"),
        [*IDENTITY_KEYS, *label_mapping.keys()],
    ].merge(
        labels_3y[[*IDENTITY_KEYS, *label_mapping.values()]],
        on=IDENTITY_KEYS,
        how="left",
        validate="one_to_one",
        suffixes=("_feature", "_label"),
    )
    for feature_column, label_column in label_mapping.items():
        left_name = (
            f"{feature_column}_feature"
            if feature_column == label_column
            else feature_column
        )
        right_name = (
            f"{label_column}_label"
            if feature_column == label_column
            else label_column
        )
        if not _series_equal(embedded[left_name], embedded[right_name]):
            raise RuntimeError(
                "embedded observed label differs from same-artifact labels: "
                f"{feature_column}/{label_column}"
            )
    unsupported = ~features["label_status_3y"].eq("supported")
    if features.loc[unsupported, "forward_return_3y"].notna().any():
        raise RuntimeError("unsupported 3y source row contains a populated target")

    summary_3y = summary.loc[
        summary["population"].eq("observed_only")
        & summary["feature_family"].eq("label_3y")
    ]
    macro_summary = summary.loc[
        summary["population"].eq("observed_only")
        & summary["feature_family"].eq("macro_vintages")
    ]
    if len(summary_3y) != 1 or len(macro_summary) != 1:
        raise RuntimeError("canonical support summary is missing observed 3y or macro rows")
    summary_counts = summary_3y.iloc[0][
        ["supported", "unavailable", "excluded"]
    ].astype(int).to_dict()
    expected_counts = {
        name: int(status_counts.get(name, 0))
        for name in ("supported", "unavailable", "excluded")
    }
    if summary_counts != expected_counts:
        raise RuntimeError("canonical observed 3y summary differs from row support")
    if int(macro_summary.iloc[0]["supported"]) != 0:
        raise RuntimeError("canonical macro support is no longer unavailable")

    availability = pd.to_datetime(
        features["availability_timestamp"], utc=True, errors="coerce"
    )
    materialization = pd.to_datetime(
        features["event_time_materialization_timestamp"],
        utc=True,
        errors="coerce",
    )
    decision = pd.to_datetime(
        features["decision_timestamp"], utc=True, errors="coerce"
    )
    prediction = pd.to_datetime(
        features["prediction_timestamp"], utc=True, errors="coerce"
    )
    if (
        availability.isna().any()
        or materialization.isna().any()
        or decision.isna().any()
        or prediction.isna().any()
        or not prediction.eq(decision + pd.Timedelta(minutes=1)).all()
    ):
        raise RuntimeError("canonical feature or decision timestamp contract drifted")
    source_available = availability.le(decision) & materialization.le(decision)

    return {
        "rows": len(features),
        "stable_row_ids": features["stable_row_id"].nunique(),
        "observed_3y_targets": len(labels_3y),
        "target_status_counts": expected_counts,
        "decision_folds": int(decision.nunique()),
        "source_features_available_at_decision": int(source_available.sum()),
        "source_features_unavailable_at_decision": int((~source_available).sum()),
        "certified_macro_supported_rows": 0,
        "policy_imputed_3y_rows": 0,
    }


def load_canonical_inputs() -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    dict[str, Any],
    dict[str, Any],
]:
    manifest, preflight = validate_canonical_manifest()
    features = pd.read_parquet(FEATURES_PATH)
    labels = pd.read_parquet(LABELS_PATH)
    row_support = pd.read_parquet(ROW_SUPPORT_PATH)
    summary = pd.read_parquet(SUMMARY_PATH)
    source_validation = validate_source_contract(
        features, labels, row_support, summary
    )
    preflight["source_contract"] = source_validation
    return features, labels, row_support, summary, manifest, preflight


def leakage_safe_feature_candidates(features: pd.DataFrame) -> list[str]:
    """Freeze semantic candidates without future-population support filtering."""
    candidates = []
    for column in features.columns:
        if column in EXCLUDE_COLS or column in MACRO_COLUMNS:
            continue
        if any(pattern in column for pattern in EXCLUDE_PATTERNS):
            continue
        if any(token in column for token in LABEL_DERIVED_TOKENS):
            continue
        if not is_numeric_dtype(features[column].dtype) or is_bool_dtype(
            features[column].dtype
        ):
            continue
        candidates.append(column)
    if not candidates:
        raise RuntimeError("canonical leakage-safe feature pool is empty")
    if len(candidates) != len(set(candidates)):
        raise RuntimeError("canonical leakage-safe feature pool is duplicated")
    forbidden = [
        column
        for column in candidates
        if column in MACRO_COLUMNS
        or any(token in column for token in LABEL_DERIVED_TOKENS)
    ]
    if forbidden:
        raise RuntimeError(
            f"target-, support-, or macro-derived candidates escaped: {forbidden}"
        )
    return candidates


def _gate(
    frame: pd.DataFrame,
    name: str,
    value: pd.Series,
    predicate: Any,
) -> None:
    supported = value.notna()
    frame[f"gate_{name}_pass"] = supported & predicate(value)
    frame[f"gate_{name}_status"] = np.where(
        supported, "supported", "unavailable"
    )


def materialize_model_table(
    features: pd.DataFrame,
    labels: pd.DataFrame,
) -> pd.DataFrame:
    """Attach authoritative observed 3y targets without dropping source rows."""
    labels_3y = labels.loc[
        labels["horizon"].eq("3y"),
        [
            *IDENTITY_KEYS,
            "entry_timestamp",
            "label_end_date",
            "stock_return",
            "outperformed_benchmark",
            "label_provenance",
        ],
    ].rename(
        columns={
            "stock_return": "target_3y",
            "outperformed_benchmark": "tree_target_3y",
            "label_provenance": "target_provenance_3y",
        }
    )
    table = features.merge(
        labels_3y,
        on=IDENTITY_KEYS,
        how="left",
        validate="one_to_one",
    )
    table["target_status_3y"] = table["label_status_3y"]
    table["fold_id"] = pd.to_datetime(
        table["decision_timestamp"], utc=True
    ).dt.strftime("decision_%Y%m%dT%H%M%SZ")
    availability = pd.to_datetime(
        table["availability_timestamp"], utc=True, errors="coerce"
    )
    materialization = pd.to_datetime(
        table["event_time_materialization_timestamp"],
        utc=True,
        errors="coerce",
    )
    decision = pd.to_datetime(
        table["decision_timestamp"], utc=True, errors="coerce"
    )
    table["source_feature_available_at_decision"] = (
        availability.le(decision) & materialization.le(decision)
    )

    _gate(
        table,
        "market_us",
        table["market"],
        lambda value: value.eq("US"),
    )
    _gate(
        table,
        "market_cap",
        pd.to_numeric(table["feature_market_cap"], errors="coerce"),
        lambda value: value.ge(MIN_MARKET_CAP)
        & value.le(MAX_MARKET_CAP_PROD),
    )
    _gate(
        table,
        "beneish",
        pd.to_numeric(table["beneish_m_score"], errors="coerce"),
        lambda value: value.lt(BENEISH_THRESHOLD),
    )
    _gate(
        table,
        "piotroski",
        pd.to_numeric(table["piotroski_f_score"], errors="coerce"),
        lambda value: value.ge(PIOTROSKI_MIN),
    )
    _gate(
        table,
        "roa_positive",
        pd.to_numeric(table["piotroski_roa_pos"], errors="coerce"),
        lambda value: value.eq(1),
    )
    _gate(
        table,
        "altman",
        pd.to_numeric(table["altman_z_score"], errors="coerce"),
        lambda value: value.gt(ALTMAN_Z_MIN),
    )
    _gate(
        table,
        "value",
        pd.to_numeric(table["ps_ratio_sector_pct"], errors="coerce"),
        lambda value: value.le(VALUE_GATE_PCT),
    )
    _gate(
        table,
        "momentum",
        pd.to_numeric(table["momentum_12m_prior"], errors="coerce"),
        lambda value: value.gt(MOMENTUM_12M_MIN),
    )
    gate_names = (
        "market_us",
        "market_cap",
        "beneish",
        "piotroski",
        "roa_positive",
        "altman",
        "value",
        "momentum",
    )
    gate_columns = [f"gate_{name}_pass" for name in gate_names]
    table["all_non_model_hard_gates_pass"] = table[gate_columns].all(axis=1)
    table["hard_gate_exclusion_codes"] = table.apply(
        lambda row: json.dumps(
            [
                (
                    f"missing_gate_evidence:{name}"
                    if row[f"gate_{name}_status"] != "supported"
                    else f"hard_gate_failed:{name}"
                )
                for name in gate_names
                if not bool(row[f"gate_{name}_pass"])
            ],
            separators=(",", ":"),
        ),
        axis=1,
    )
    if (
        len(table) != SOURCE_ROWS
        or table["stable_row_id"].duplicated().any()
        or table["target_3y"].notna().sum()
        != table["label_status_3y"].eq("supported").sum()
    ):
        raise RuntimeError("canonical model table identity or target support drifted")
    return table


def freeze_configuration(
    candidates: list[str],
    source_manifest: dict[str, Any],
) -> dict[str, Any]:
    """Return the single P3 split, feature, preprocessing, and model contract."""
    return {
        "schema_version": 1,
        "session": "P3",
        "route_name": "canonical_us_annual_observed_only_3y",
        "strategy_name": "production_v3_ml_gates",
        "source": {
            "manifest": "artifacts/canonical/corrected_us_annual/manifest.json",
            "manifest_sha256": CANONICAL_P2_MANIFEST_SHA256,
            "dataset": (
                "artifacts/canonical/corrected_us_annual/outputs/"
                "observed_only/features_taxonomy.parquet"
            ),
            "population": "observed_only",
            "market": "US",
            "period_type": "annual",
            "source_freeze_timestamp": source_manifest["created_at_utc"],
            "policy_only_additions": 0,
            "certified_macro_vintages": "unavailable",
        },
        "decision_calendar": {
            "fold": "one fold per exact canonical decision_timestamp",
            "fold_id": "decision timestamp formatted decision_%Y%m%dT%H%M%SZ",
            "score_cutoff": (
                "decision_timestamp <= canonical P2 manifest created_at_utc"
            ),
            "score_feature_rule": (
                "availability_timestamp and event_time_materialization_timestamp "
                "<= row decision_timestamp"
            ),
            "training_label_rule": (
                "label_end_date strictly before fold decision_timestamp"
            ),
            "training_feature_rule": (
                "source feature timestamps <= the training row decision_timestamp"
            ),
        },
        "targets": {
            "decision_tree": "tree_target_3y",
            "decision_tree_source": (
                "same-artifact observed_only_labels.outperformed_benchmark"
            ),
            "lightgbm_regression": "target_3y",
            "lightgbm_source": (
                "same-artifact observed_only_labels.stock_return"
            ),
            "regression_clip": [-1.0, 5.0],
            "missingness": "observed-only null targets are never imputed",
        },
        "feature_contract": {
            "candidate_columns": candidates,
            "candidate_count": len(candidates),
            "candidate_freeze": (
                "semantic numeric schema only; no full-population support filter"
            ),
            "forbidden_label_tokens": list(LABEL_DERIVED_TOKENS),
            "forbidden_macro_columns": sorted(MACRO_COLUMNS),
            "selector": asdict(FEATURE_SELECTOR),
            "selector_fit": "inside each historical fold only",
            "preprocessing": (
                "fold-local median imputation only; invalid selected-feature "
                "median fails the fold closed"
            ),
        },
        "decision_tree": {
            "family": "sklearn.tree.DecisionTreeClassifier",
            "max_depth": 4,
            "min_samples_leaf": 50,
            "min_samples_split": 100,
            "class_weight": (
                "fold-local {0:1.0,1:n_negative/max(n_positive,1)}"
            ),
            "random_state": 42,
            "probability_role": "OOS tree agreement gate",
            "pass_rule": f"tree_prob >= {TREE_THRESHOLD}",
        },
        "lightgbm_ranker": {
            "family": "lightgbm.LGBMRegressor",
            "parameters": {
                "n_estimators": 600,
                "max_depth": 6,
                "learning_rate": 0.03,
                "num_leaves": 63,
                "subsample": 0.8,
                "colsample_bytree": 0.7,
                "min_child_samples": 20,
                "reg_alpha": 0.1,
                "reg_lambda": 1.0,
                "random_state": 42,
                "n_jobs": -1,
                "verbose": -1,
            },
            "role": "OOS continuous-return ranker",
        },
        "model_training_population": {
            "required_filter": (
                "piotroski_roa_pos == 1 AND beneish_m_score < -1.78"
            ),
            "missingness": (
                "missing target, label end, ROA, Beneish, or row feature "
                "availability fails the training row closed"
            ),
        },
        "output_contract": {
            "model_roles": list(MODEL_ROLES),
            "one_row_per_source_row_role": True,
            "available_status": "oos_prediction_available",
            "no_in_sample_predictions": True,
            "excluded_rows": "retained with an exact non-empty exclusion_code",
        },
        "limitations": {
            "macro": "no certified vintage/release-lag values are used",
            "survivorship": (
                "historically enriched, not comprehensively survivorship-free"
            ),
            "performance": "not calculated by this route",
            "downstream": "no backtest or portfolio is connected",
        },
    }


def eligible_training_rows(
    table: pd.DataFrame,
    decision_timestamp: pd.Timestamp,
    target: str,
) -> pd.DataFrame:
    """Return only decision-available rows with strictly elapsed observed labels."""
    label_end = pd.to_datetime(
        table["label_end_date"], utc=True, errors="coerce"
    )
    clean = (
        pd.to_numeric(table["piotroski_roa_pos"], errors="coerce").eq(1)
        & pd.to_numeric(table["beneish_m_score"], errors="coerce").lt(
            BENEISH_THRESHOLD
        )
    )
    eligible = (
        table["source_feature_available_at_decision"].fillna(False).astype(bool)
        & label_end.notna()
        & label_end.lt(decision_timestamp)
        & table[target].notna()
        & clean
    )
    train = table.loc[eligible].copy()
    if not train.empty and (
        not pd.to_datetime(train["label_end_date"], utc=True).lt(
            decision_timestamp
        ).all()
        or not train["source_feature_available_at_decision"].all()
    ):
        raise RuntimeError("canonical fold training availability invariant failed")
    return train


def prediction_template(
    table: pd.DataFrame,
    role: str,
    source_artifact_ids: dict[str, str],
) -> pd.DataFrame:
    fields = [
        "stable_row_id",
        "entity_id",
        "cik",
        "ticker",
        "market",
        "fiscal_year",
        "period_type",
        "availability_timestamp",
        "availability_provenance",
        "event_time_materialization_timestamp",
        "source_feature_available_at_decision",
        "decision_timestamp",
        "prediction_timestamp",
        "entry_timestamp",
        "label_end_date",
        "target_status_3y",
        "target_provenance_3y",
        "target_3y",
        "tree_target_3y",
        "fold_id",
        "all_non_model_hard_gates_pass",
        "hard_gate_exclusion_codes",
    ]
    output = table[fields].copy()
    output["strategy_name"] = "production_v3_ml_gates"
    output["model_role"] = role
    output["prediction_status"] = "excluded"
    output["exclusion_code"] = ""
    output["prediction"] = np.nan
    for column, value in source_artifact_ids.items():
        output[column] = value
    for column in MODEL_LINEAGE_COLUMNS[:5]:
        output[column] = pd.NA
    output["training_rows"] = pd.Series(
        pd.NA, index=output.index, dtype="Int64"
    )
    output["training_label_end_max"] = pd.Series(
        pd.NA, index=output.index, dtype="string"
    )
    output["selected_feature_count"] = pd.Series(
        pd.NA, index=output.index, dtype="Int64"
    )
    output["selected_features_json"] = pd.NA
    output["training_population_fingerprint"] = pd.NA
    return output


def build_oos_predictions(
    table: pd.DataFrame,
    config: dict[str, Any],
    source_freeze_timestamp: pd.Timestamp,
    artifact_root: Path,
    source_artifact_ids: dict[str, str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fit both roles on identical temporal folds and preserve every row-role."""
    table = table.copy()
    table["decision_timestamp"] = pd.to_datetime(
        table["decision_timestamp"], utc=True
    )
    table["label_end_date"] = pd.to_datetime(
        table["label_end_date"], utc=True, errors="coerce"
    )
    outputs = []
    fold_records = []
    for role in MODEL_ROLES:
        rows = prediction_template(table, role, source_artifact_ids)
        future = table["decision_timestamp"].gt(source_freeze_timestamp)
        source_unavailable = (
            ~table["source_feature_available_at_decision"].astype(bool)
            & ~future
        )
        rows.loc[future, "prediction_status"] = "future_excluded"
        rows.loc[future, "exclusion_code"] = EXCLUSION_FUTURE
        rows.loc[source_unavailable, "exclusion_code"] = (
            EXCLUSION_SOURCE_UNAVAILABLE
        )
        target = config["targets"][role]
        eligible_decisions = sorted(
            table.loc[~future, "decision_timestamp"].unique()
        )
        for decision_timestamp in eligible_decisions:
            fold_all = table["decision_timestamp"].eq(decision_timestamp)
            score_mask = (
                fold_all & table["source_feature_available_at_decision"].astype(bool)
            )
            score = table.loc[score_mask].copy()
            train = eligible_training_rows(
                table, pd.Timestamp(decision_timestamp), target
            )
            fold_id = pd.Timestamp(decision_timestamp).strftime(
                "decision_%Y%m%dT%H%M%SZ"
            )
            if score.empty:
                predictions = None
                lineage: dict[str, Any] = {}
                exclusion = EXCLUSION_NO_SCORE_ROWS
            else:
                predictions, lineage, exclusion = fit_fold_role(
                    train,
                    score,
                    role,
                    fold_id,
                    config,
                    artifact_root,
                )
            fold_record = {
                "strategy_name": "production_v3_ml_gates",
                "model_role": role,
                "fold_id": fold_id,
                "decision_timestamp": pd.Timestamp(decision_timestamp),
                "source_rows": int(fold_all.sum()),
                "score_eligible_rows": len(score),
                "source_feature_unavailable_rows": int(
                    (fold_all & ~table[
                        "source_feature_available_at_decision"
                    ].astype(bool)).sum()
                ),
                "training_rows": len(train),
                "training_label_end_max": (
                    train["label_end_date"].max()
                    if not train.empty
                    else pd.NaT
                ),
                "strict_label_end_before_decision": bool(
                    train.empty
                    or train["label_end_date"].lt(decision_timestamp).all()
                ),
                "training_features_available_at_own_decision": bool(
                    train.empty
                    or train["source_feature_available_at_decision"].all()
                ),
                "status": (
                    "fit" if exclusion is None else "failed_closed"
                ),
                "exclusion_code": exclusion or "",
                **lineage,
            }
            fold_records.append(fold_record)
            target_index = rows.index[score_mask]
            if exclusion is not None:
                rows.loc[target_index, "exclusion_code"] = exclusion
                continue
            rows.loc[
                target_index, "prediction_status"
            ] = "oos_prediction_available"
            rows.loc[target_index, "prediction"] = predictions
            for key, value in lineage.items():
                rows.loc[target_index, key] = value
        outputs.append(rows)
    return pd.concat(outputs, ignore_index=True), pd.DataFrame(fold_records)


def _artifact_id(path: Path) -> str:
    return f"sha256:{sha256_file(path)}"


def validate_oos_lineage(
    predictions: pd.DataFrame,
    folds: pd.DataFrame,
    table: pd.DataFrame,
    config: dict[str, Any],
    source_freeze_timestamp: pd.Timestamp,
    artifact_root: Path,
) -> dict[str, Any]:
    """Prove row-role completeness and fitted-fold artifact lineage."""
    expected_rows = len(table) * len(MODEL_ROLES)
    identity = ["stable_row_id", "model_role"]
    if len(predictions) != expected_rows or predictions.duplicated(identity).any():
        raise RuntimeError("OOS output is not exactly one row per source row-role")
    if set(predictions["model_role"]) != set(MODEL_ROLES):
        raise RuntimeError("OOS output model roles drifted")
    source_ids = set(table["stable_row_id"])
    for role in MODEL_ROLES:
        if set(predictions.loc[
            predictions["model_role"].eq(role), "stable_row_id"
        ]) != source_ids:
            raise RuntimeError(f"OOS output source identity is incomplete for {role}")
    if predictions[SOURCE_LINEAGE_COLUMNS].isna().any().any():
        raise RuntimeError("OOS output source artifact lineage is incomplete")

    available = predictions["prediction_status"].eq(
        "oos_prediction_available"
    )
    excluded = ~available
    prediction_values = pd.to_numeric(
        predictions["prediction"], errors="coerce"
    )
    if (
        not np.isfinite(prediction_values.loc[available]).all()
        or prediction_values.loc[excluded].notna().any()
        or predictions.loc[available, "exclusion_code"].ne("").any()
        or predictions.loc[excluded, "exclusion_code"].eq("").any()
        or predictions.loc[available, MODEL_LINEAGE_COLUMNS].isna().any().any()
    ):
        raise RuntimeError("OOS prediction/exclusion or model lineage is incomplete")
    if not predictions.loc[
        available, "source_feature_available_at_decision"
    ].all():
        raise RuntimeError("OOS prediction used a source row unavailable at decision")
    training_end = pd.to_datetime(
        predictions.loc[available, "training_label_end_max"],
        utc=True,
        errors="coerce",
    )
    decision = pd.to_datetime(
        predictions.loc[available, "decision_timestamp"], utc=True
    )
    if training_end.isna().any() or not training_end.lt(decision).all():
        raise RuntimeError("OOS prediction training target was not strictly elapsed")
    if pd.to_numeric(
        predictions.loc[available, "selected_feature_count"], errors="coerce"
    ).gt(FEATURE_SELECTOR.top_n).any():
        raise RuntimeError("OOS prediction exceeded fold-local feature cap")

    candidates = set(config["feature_contract"]["candidate_columns"])
    for payload in predictions.loc[
        available, "selected_features_json"
    ].drop_duplicates():
        selected = json.loads(payload)
        if not set(selected).issubset(candidates):
            raise RuntimeError("OOS selected feature escaped canonical candidates")
        if any(
            token in feature
            for feature in selected
            for token in LABEL_DERIVED_TOKENS
        ) or any(feature in MACRO_COLUMNS for feature in selected):
            raise RuntimeError("OOS selected target-, support-, or macro-derived feature")

    nonfuture_decisions = set(
        pd.to_datetime(
            table.loc[
                pd.to_datetime(table["decision_timestamp"], utc=True).le(
                    source_freeze_timestamp
                ),
                "decision_timestamp",
            ],
            utc=True,
        )
    )
    if folds.duplicated(["model_role", "fold_id"]).any():
        raise RuntimeError("fold coverage contains duplicate role-fold lineage")
    for role in MODEL_ROLES:
        role_decisions = set(
            pd.to_datetime(
                folds.loc[folds["model_role"].eq(role), "decision_timestamp"],
                utc=True,
            )
        )
        if role_decisions != nonfuture_decisions:
            raise RuntimeError(f"temporal decision fold coverage is incomplete for {role}")
    if (
        not folds["strict_label_end_before_decision"].all()
        or not folds["training_features_available_at_own_decision"].all()
    ):
        raise RuntimeError("fold-level training availability lineage failed")

    fitted = folds["status"].eq("fit")
    for fold in folds.loc[fitted].itertuples(index=False):
        model_root = artifact_root / "models" / fold.fold_id / fold.model_role
        paths = {
            "feature_artifact_id": model_root / "features.json",
            "preprocessing_artifact_id": model_root / "preprocessing.json",
            "target_artifact_id": model_root / "target.json",
            "model_configuration_artifact_id": (
                model_root / "model_configuration.json"
            ),
            "model_artifact_id": model_root / "model.joblib",
        }
        for column, path in paths.items():
            if not path.is_file() or getattr(fold, column) != _artifact_id(path):
                raise RuntimeError(
                    f"fitted fold artifact lineage mismatch: {fold.fold_id}/{fold.model_role}/{column}"
                )
        feature_payload = json.loads(paths["feature_artifact_id"].read_text())
        preprocessing = json.loads(
            paths["preprocessing_artifact_id"].read_text()
        )
        target_payload = json.loads(paths["target_artifact_id"].read_text())
        if (
            feature_payload["candidate_columns"]
            != config["feature_contract"]["candidate_columns"]
            or feature_payload["selector"]
            != config["feature_contract"]["selector"]
            or feature_payload["selected_features"]
            != preprocessing["features"]
            or preprocessing["method"]
            != "fold_local_median_imputation_only"
            or pd.Timestamp(target_payload["training_label_end_max"])
            >= pd.Timestamp(fold.decision_timestamp)
        ):
            raise RuntimeError(
                f"fitted fold-local contract drifted: {fold.fold_id}/{fold.model_role}"
            )

    future = predictions["exclusion_code"].eq(EXCLUSION_FUTURE)
    late = predictions["exclusion_code"].eq(EXCLUSION_SOURCE_UNAVAILABLE)
    return {
        "result": "pass",
        "source_rows": len(table),
        "model_roles": list(MODEL_ROLES),
        "row_role_rows": len(predictions),
        "unique_row_roles": int(
            predictions[identity].drop_duplicates().shape[0]
        ),
        "available_oos_predictions": int(available.sum()),
        "excluded_row_roles": int(excluded.sum()),
        "future_excluded_row_roles": int(future.sum()),
        "source_unavailable_row_roles": int(late.sum()),
        "folds": len(folds),
        "fit_folds": int(fitted.sum()),
        "failed_closed_folds": int((~fitted).sum()),
        "in_sample_predictions": 0,
        "lineage_complete": True,
    }


def freeze_dirty_state(artifact_root: Path) -> list[Path]:
    lineage = artifact_root / "lineage"
    lineage.mkdir(parents=True, exist_ok=True)
    paths = [
        lineage / "git_status_porcelain.txt",
        lineage / "tracked_dirty.patch",
        lineage / "untracked_inventory.json",
    ]
    paths[0].write_bytes(
        subprocess.check_output(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=ROOT,
        )
    )
    paths[1].write_bytes(
        subprocess.check_output(
            ["git", "diff", "--binary", "--", "."], cwd=ROOT
        )
    )
    prefix = artifact_root.relative_to(ROOT).as_posix() + "/"
    inventory = []
    for raw in subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
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
        raise RuntimeError(
            f"refusing to reuse non-empty artifact root: {artifact_root}"
        )
    (
        features,
        labels,
        _row_support,
        _summary,
        source_manifest,
        preflight,
    ) = load_canonical_inputs()
    candidates = leakage_safe_feature_candidates(features)
    config = freeze_configuration(candidates, source_manifest)
    table = materialize_model_table(features, labels)
    source_freeze_timestamp = pd.Timestamp(source_manifest["created_at_utc"])

    for name in (
        "inputs",
        "configuration",
        "models",
        "predictions",
        "support",
        "lineage",
    ):
        (artifact_root / name).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(
        CANONICAL_P2 / "manifest.json",
        artifact_root / "inputs/canonical_p2_manifest.json",
    )
    config_path = artifact_root / "configuration/research_model_contract.json"
    write_json(config_path, config)

    consumed_hashes = {
        path: sha256_file(path) for path in CONSUMED_INPUTS
    }
    source_artifact_ids = {
        "source_manifest_sha256": CANONICAL_P2_MANIFEST_SHA256,
        "source_dataset_artifact_id": f"sha256:{consumed_hashes[FEATURES_PATH]}",
        "source_labels_artifact_id": f"sha256:{consumed_hashes[LABELS_PATH]}",
        "source_row_support_artifact_id": (
            f"sha256:{consumed_hashes[ROW_SUPPORT_PATH]}"
        ),
    }
    predictions, folds = build_oos_predictions(
        table,
        config,
        source_freeze_timestamp,
        artifact_root,
        source_artifact_ids,
    )
    lineage_validation = validate_oos_lineage(
        predictions,
        folds,
        table,
        config,
        source_freeze_timestamp,
        artifact_root,
    )
    exclusions = exclusion_report(predictions)

    prediction_path = artifact_root / "predictions/oos_predictions.parquet"
    fold_path = artifact_root / "support/fold_coverage.parquet"
    exclusion_path = artifact_root / "support/exclusions.parquet"
    preflight_path = artifact_root / "support/preflight_validation.json"
    lineage_validation_path = (
        artifact_root / "support/oos_lineage_validation.json"
    )
    verdict_path = artifact_root / "support/verdict.json"
    predictions.to_parquet(prediction_path, index=False)
    folds.to_parquet(fold_path, index=False)
    exclusions.to_parquet(exclusion_path, index=False)
    write_json(preflight_path, preflight)
    write_json(lineage_validation_path, lineage_validation)
    verdict = {
        "status": "accepted",
        "product_session_p3_complete": True,
        "route_name": config["route_name"],
        "canonical_p2_observed_only_consumed": True,
        "oos_predictions_generated": True,
        "row_role_lineage_complete": True,
        "target_or_support_columns_used_as_features": False,
        "uncertified_macro_values_used": False,
        "policy_only_additions": 0,
        "parameters_or_thresholds_optimized": False,
        "performance_calculated": False,
        "backtest_run": False,
        "portfolio_constructed": False,
        "external_data_sourced_or_refreshed": False,
        "v3_4_resumed": False,
    }
    write_json(verdict_path, verdict)
    lineage_paths = freeze_dirty_state(artifact_root)

    artifact_files = sorted(
        path
        for path in artifact_root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    )
    code_paths = [
        ROOT / "modeling/build_canonical_research_model.py",
        ROOT / "modeling/oos_modeling.py",
        ROOT / "modeling/fold_lineage.py",
        ROOT / "tests/modeling/test_build_canonical_research_model.py",
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": (
            "CANONICAL_US_ANNUAL_OBSERVED_ONLY_3Y_RESEARCH_MODEL_OOS"
        ),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "canonical_entrypoint": {
            "command": "python3 -m modeling.build_canonical_research_model",
            "artifact_root": artifact_root.relative_to(ROOT).as_posix(),
            "predictions": "predictions/oos_predictions.parquet",
            "population": "observed_only",
            "market": "US",
            "period_type": "annual",
            "horizon": "3y",
        },
        "current_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "claim": verdict,
        "configuration": config,
        "preflight": preflight,
        "oos_lineage_validation": lineage_validation,
        "validated_inputs": [
            record(
                CANONICAL_P2 / "manifest.json",
                "canonical_p2_manifest",
            ),
            *[
                record(path, "consumed_canonical_p2_observed_record")
                for path in CONSUMED_INPUTS
            ],
        ],
        "records": [
            record(
                path,
                "p3_configuration_model_prediction_support_or_lineage",
            )
            for path in artifact_files
        ],
        "code_lineage": [
            record(path, "p3_builder_dependency_or_focused_test")
            for path in code_paths
        ],
        "limitations": [
            (
                "No certified macro vintage/release-lag input exists; no macro "
                "value or interaction is used."
            ),
            (
                "The US annual population is historically enriched but not "
                "comprehensively survivorship-free; free sources do not provide "
                "CRSP-quality historical membership, security/ticker histories, "
                "delisting terms, or delisting returns."
            ),
            (
                "This artifact contains OOS model outputs and exclusions only; "
                "it does not calculate performance or connect a backtest, "
                "portfolio, or report."
            ),
        ],
        "dirty_state": {
            "complete_status_recorded": True,
            "records": [
                path.relative_to(ROOT).as_posix() for path in lineage_paths
            ],
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
        **lineage_validation,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT
    )
    args = parser.parse_args()
    result = build(args.artifact_root.resolve())
    print(
        json.dumps(
            {
                key: str(value) if isinstance(value, Path) else value
                for key, value in result.items()
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
