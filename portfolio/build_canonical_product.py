"""Build the canonical P4 prediction-to-shortlist product artifact.

The route consumes only the row-complete canonical P3 OOS prediction table.
It revalidates the P3/P2 lineage, recomputes the fixed decision-time hard
gates, evaluates candidate-wide pre-prediction liquidity from the already
frozen Session 8E raw payloads, and freezes equal-weight holdings plus an
explainable latest shortlist.

Official performance remains fail-closed.  The accepted security/action ledger
and immutable DGS1MO vintage required by the historical three-year-vintage
contract do not exist locally, so this command records the exact blockers and
does not substitute Yahoo adjusted closes, legacy V3 results, or uncertified
rates.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from backtest.monthly_nav import OBSERVED_ONLY
from modeling.constants import (
    ALTMAN_Z_MIN,
    BENEISH_THRESHOLD,
    MAX_MARKET_CAP_PROD,
    MOMENTUM_12M_MIN,
    PIOTROSKI_MIN,
    TREE_THRESHOLD,
    VALUE_GATE_PCT,
)
from portfolio.selection_contract import (
    EX_LIQUIDITY_CURRENCY,
    EX_LIQUIDITY_EXCHANGE,
    EX_LIQUIDITY_INCOMPLETE,
    EX_LIQUIDITY_PRICE,
    EX_LIQUIDITY_RESPONSE,
    EX_LIQUIDITY_SESSION,
    EX_LIQUIDITY_STALE,
    EX_LIQUIDITY_SYMBOL,
    EX_LIQUIDITY_THRESHOLD,
    EX_LIQUIDITY_VOLUME,
    EX_PERIOD_INCOMPLETE,
    EX_RANK_ROLE,
    EX_TREE_ROLE,
    EX_TREE_THRESHOLD,
    MIN_ADTV,
    TARGET_N,
    WEIGHT,
)


ROOT = Path(__file__).resolve().parents[1]
CANONICAL_P3 = ROOT / "artifacts/canonical/corrected_us_annual_3y_research_model"
CANONICAL_P2 = ROOT / "artifacts/canonical/corrected_us_annual"
SESSION8E = ROOT / "artifacts/pit_validation/contract_aligned_label_inputs"
DEFAULT_ARTIFACT_ROOT = (
    ROOT / "artifacts/canonical/corrected_us_annual_3y_product"
)

P3_MANIFEST = CANONICAL_P3 / "manifest.json"
P3_PREDICTIONS = CANONICAL_P3 / "predictions/oos_predictions.parquet"
P2_MANIFEST = CANONICAL_P2 / "manifest.json"
P2_FEATURES = (
    CANONICAL_P2 / "outputs/observed_only/features_taxonomy.parquet"
)
SESSION8E_MANIFEST = SESSION8E / "manifest.json"

P3_MANIFEST_SHA256 = (
    "8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2"
)
P2_MANIFEST_SHA256 = (
    "40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3"
)
SESSION8E_MANIFEST_SHA256 = (
    "0ab15685a445f09919b19393e074b8b5903afef7e12defc7c44aeac624f4581a"
)
P3_PREDICTIONS_SHA256 = (
    "85164532421aec9909670f16fd56b4dcc9c5bcaaf7273c1ab98d1c464a7242a2"
)
P2_FEATURES_SHA256 = (
    "46ed33affd8cc66df1f11dc8a41d6f804d106f0a2f8841509cbc931de013e56f"
)

MODEL_ROLES = ("decision_tree", "lightgbm_regression")
AVAILABLE_PREDICTION = "oos_prediction_available"
SOURCE_ROWS = 43_806
ROW_ROLE_ROWS = SOURCE_ROWS * len(MODEL_ROLES)
HARD_GATE_NAMES = (
    "market_us",
    "market_cap",
    "beneish",
    "piotroski",
    "roa_positive",
    "altman",
    "value",
    "momentum",
)
MODEL_LINEAGE_COLUMNS = (
    "feature_artifact_id",
    "preprocessing_artifact_id",
    "target_artifact_id",
    "model_configuration_artifact_id",
    "model_artifact_id",
)
SOURCE_LINEAGE_COLUMNS = (
    "source_manifest_sha256",
    "source_dataset_artifact_id",
    "source_labels_artifact_id",
    "source_row_support_artifact_id",
)
ROLE_OUTPUT_COLUMNS = (
    "prediction_status",
    "exclusion_code",
    "prediction",
    *MODEL_LINEAGE_COLUMNS,
    "training_rows",
    "training_label_end_max",
    "selected_feature_count",
    "selected_features_json",
    "training_population_fingerprint",
)

AUM_USD = 200_000.0
MAX_POSITION_TO_ADTV = 0.01
TRANSACTION_COST_PER_SIDE = 0.0025
HOLDING_MONTHS = 36
BACKTEST_BLOCKERS = (
    {
        "code": "missing_canonical_security_action_market_ledger",
        "required": (
            "Evidence-backed dated security/ticker/exchange continuity, "
            "adjustment semantics, and complete corporate-action terms for "
            "every selected holding and benchmark sleeve."
        ),
        "missing_path": (
            "artifacts/pit_validation/session_v3_4_market_ledger_inputs/"
        ),
        "prohibited_fallback": (
            "Do not treat frozen Yahoo adjusted closes or old V3 performance "
            "artifacts as a canonical security/action ledger."
        ),
    },
    {
        "code": "missing_immutable_dgs1mo_alfred_vintage_2026_07_17",
        "required": (
            "Federal Reserve H.15 DGS1MO observations at immutable ALFRED "
            "vintage 2026-07-17 with the accepted release-time contract."
        ),
        "missing_path": (
            "artifacts/pit_validation/session_v3_4_market_ledger_inputs/"
            "raw/fred/DGS1MO_alfred_vintage_2026-07-17.json.gz"
        ),
        "prohibited_fallback": (
            "Do not use a constant, current, interpolated, carried, or "
            "uncertified risk-free series."
        ),
    },
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    )


def record(path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(ROOT).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _manifest_index(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for section in (
        "records",
        "validated_inputs",
        "code_lineage",
        "deliverables",
        "outputs",
    ):
        for item in manifest.get(section, []):
            if isinstance(item, dict) and item.get("path"):
                indexed[str(item["path"])] = item
    return indexed


def _validate_record(
    path: Path,
    item: dict[str, Any],
    label: str,
) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(f"{label} is missing: {path}")
    actual_size = path.stat().st_size
    actual_hash = sha256_file(path)
    if item.get("size_bytes") is not None and actual_size != int(
        item["size_bytes"]
    ):
        raise RuntimeError(
            f"{label} size mismatch: expected={item['size_bytes']} "
            f"actual={actual_size}"
        )
    if actual_hash != item.get("sha256"):
        raise RuntimeError(
            f"{label} hash mismatch: expected={item.get('sha256')} "
            f"actual={actual_hash}"
        )
    return {
        "path": path.relative_to(ROOT).as_posix(),
        "size_bytes": actual_size,
        "sha256": actual_hash,
    }


def validate_inputs() -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    """Validate the exact P3 route and the frozen market-evidence indexes."""
    if sha256_file(P3_MANIFEST) != P3_MANIFEST_SHA256:
        raise RuntimeError("canonical P3 manifest hash mismatch")
    p3 = json.loads(P3_MANIFEST.read_text())
    if (
        p3.get("artifact_class")
        != "CANONICAL_US_ANNUAL_OBSERVED_ONLY_3Y_RESEARCH_MODEL_OOS"
        or p3.get("claim", {}).get("status") != "accepted"
        or not p3.get("claim", {}).get("row_role_lineage_complete")
        or p3.get("claim", {}).get("performance_calculated")
    ):
        raise RuntimeError("canonical P3 acceptance contract drifted")
    p3_index = _manifest_index(p3)
    prediction_relative = P3_PREDICTIONS.relative_to(ROOT).as_posix()
    if prediction_relative not in p3_index:
        raise RuntimeError("canonical P3 predictions are absent from its manifest")
    prediction_record = _validate_record(
        P3_PREDICTIONS,
        p3_index[prediction_relative],
        "canonical P3 predictions",
    )
    if prediction_record["sha256"] != P3_PREDICTIONS_SHA256:
        raise RuntimeError("canonical P3 prediction hash is not the pinned P4 input")

    p2_relative = P2_MANIFEST.relative_to(ROOT).as_posix()
    feature_relative = P2_FEATURES.relative_to(ROOT).as_posix()
    if p2_relative not in p3_index or feature_relative not in p3_index:
        raise RuntimeError("canonical P3 lacks its P2 manifest/feature lineage")
    p2_record = _validate_record(
        P2_MANIFEST,
        p3_index[p2_relative],
        "canonical P2 manifest through P3",
    )
    feature_record = _validate_record(
        P2_FEATURES,
        p3_index[feature_relative],
        "canonical P2 features through P3",
    )
    if (
        p2_record["sha256"] != P2_MANIFEST_SHA256
        or feature_record["sha256"] != P2_FEATURES_SHA256
    ):
        raise RuntimeError("canonical P2 lineage is not the pinned P4 input")

    p2 = json.loads(P2_MANIFEST.read_text())
    p2_index = _manifest_index(p2)
    session8e_copy = CANONICAL_P2 / "inputs/session8e_manifest.json"
    session8e_copy_relative = session8e_copy.relative_to(ROOT).as_posix()
    if session8e_copy_relative not in p2_index:
        raise RuntimeError("canonical P2 lacks its Session 8E manifest record")
    session8e_copy_record = _validate_record(
        session8e_copy,
        p2_index[session8e_copy_relative],
        "canonical P2 Session 8E manifest copy",
    )
    if session8e_copy_record["sha256"] != SESSION8E_MANIFEST_SHA256:
        raise RuntimeError("canonical P2 points to an unexpected Session 8E")
    if sha256_file(SESSION8E_MANIFEST) != SESSION8E_MANIFEST_SHA256:
        raise RuntimeError("frozen Session 8E manifest hash mismatch")
    session8e = json.loads(SESSION8E_MANIFEST.read_text())
    if (
        session8e.get("artifact_class")
        != "SESSION8E_CONTRACT_ALIGNED_LABEL_INPUTS"
        or not session8e.get("raw_payloads", {}).get(
            "all_success_hashes_verified"
        )
    ):
        raise RuntimeError("Session 8E frozen market evidence contract drifted")
    session8e_index = _manifest_index(session8e)
    required_session8e_paths = (
        SESSION8E / "configuration/config.json",
        SESSION8E / "calendar/xnys_regular_sessions.parquet",
        SESSION8E / "raw/response_manifest.jsonl",
        SESSION8E / "raw/raw_inventory.json",
        SESSION8E / "normalized/normalization_summary.parquet",
    )
    validated_market_indexes = []
    for path in required_session8e_paths:
        relative = path.relative_to(ROOT).as_posix()
        if relative not in session8e_index:
            raise RuntimeError(
                f"required Session 8E market index is unmanifested: {relative}"
            )
        validated_market_indexes.append(
            _validate_record(path, session8e_index[relative], relative)
        )

    preflight = {
        "result": "pass",
        "canonical_p3_manifest_sha256": P3_MANIFEST_SHA256,
        "canonical_p3_prediction_sha256": P3_PREDICTIONS_SHA256,
        "canonical_p2_manifest_sha256": P2_MANIFEST_SHA256,
        "canonical_p2_feature_sha256": P2_FEATURES_SHA256,
        "session8e_manifest_sha256": SESSION8E_MANIFEST_SHA256,
        "validated_market_indexes": validated_market_indexes,
        "external_data_sourced_or_refreshed": False,
        "historical_v3_predictions_consumed": False,
    }
    return p3, p2, session8e, preflight


def consume_canonical_predictions(
    predictions: pd.DataFrame,
    p3_manifest: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Consume the P3 long OOS table without a score or lineage fallback."""
    required = {
        "stable_row_id",
        "entity_id",
        "cik",
        "ticker",
        "market",
        "fiscal_year",
        "period_type",
        "availability_timestamp",
        "event_time_materialization_timestamp",
        "source_feature_available_at_decision",
        "decision_timestamp",
        "prediction_timestamp",
        "entry_timestamp",
        "fold_id",
        "all_non_model_hard_gates_pass",
        "hard_gate_exclusion_codes",
        "model_role",
        "prediction_status",
        "exclusion_code",
        "prediction",
        *MODEL_LINEAGE_COLUMNS,
        *SOURCE_LINEAGE_COLUMNS,
        "training_label_end_max",
        "selected_features_json",
        "training_population_fingerprint",
    }
    missing = required - set(predictions.columns)
    if missing:
        raise RuntimeError(f"canonical P3 prediction schema is incomplete: {missing}")
    identity = ["stable_row_id", "model_role"]
    if (
        len(predictions) != ROW_ROLE_ROWS
        or predictions.duplicated(identity).any()
        or predictions["stable_row_id"].nunique() != SOURCE_ROWS
        or set(predictions["model_role"]) != set(MODEL_ROLES)
    ):
        raise RuntimeError("canonical P3 row-role population is incomplete")
    role_counts = predictions.groupby("stable_row_id")["model_role"].nunique()
    if not role_counts.eq(len(MODEL_ROLES)).all():
        raise RuntimeError("canonical P3 source row is missing a required role")

    common_columns = [
        "entity_id",
        "cik",
        "ticker",
        "market",
        "fiscal_year",
        "period_type",
        "availability_timestamp",
        "event_time_materialization_timestamp",
        "source_feature_available_at_decision",
        "decision_timestamp",
        "prediction_timestamp",
        "entry_timestamp",
        "fold_id",
        "all_non_model_hard_gates_pass",
        "hard_gate_exclusion_codes",
        *SOURCE_LINEAGE_COLUMNS,
    ]
    if (
        predictions.groupby("stable_row_id")[common_columns]
        .nunique(dropna=False)
        .gt(1)
        .any()
        .any()
    ):
        raise RuntimeError("canonical P3 roles disagree on shared row lineage")
    if predictions[list(SOURCE_LINEAGE_COLUMNS)].isna().any().any():
        raise RuntimeError("canonical P3 source lineage is incomplete")
    if not predictions["source_manifest_sha256"].eq(
        P2_MANIFEST_SHA256
    ).all():
        raise RuntimeError("canonical P3 row points to an unexpected source manifest")
    if not predictions["source_dataset_artifact_id"].eq(
        f"sha256:{P2_FEATURES_SHA256}"
    ).all():
        raise RuntimeError("canonical P3 row points to an unexpected source dataset")

    available = predictions["prediction_status"].eq(AVAILABLE_PREDICTION)
    numeric_prediction = pd.to_numeric(predictions["prediction"], errors="coerce")
    excluded = ~available
    if (
        not np.isfinite(numeric_prediction.loc[available]).all()
        or numeric_prediction.loc[excluded].notna().any()
        or predictions.loc[available, "exclusion_code"].ne("").any()
        or predictions.loc[excluded, "exclusion_code"].eq("").any()
        or predictions.loc[available, list(MODEL_LINEAGE_COLUMNS)]
        .isna()
        .any()
        .any()
    ):
        raise RuntimeError("canonical P3 prediction/exclusion contract failed")
    accepted_ids = {
        f"sha256:{item['sha256']}"
        for item in p3_manifest.get("records", [])
        if item.get("sha256")
    }
    for column in MODEL_LINEAGE_COLUMNS:
        unknown = set(predictions.loc[available, column]) - accepted_ids
        if unknown:
            raise RuntimeError(
                f"canonical P3 has unrecognized {column}: {sorted(unknown)[:3]}"
            )

    decision = pd.to_datetime(
        predictions["decision_timestamp"], utc=True, errors="coerce"
    )
    prediction_time = pd.to_datetime(
        predictions["prediction_timestamp"], utc=True, errors="coerce"
    )
    availability = pd.to_datetime(
        predictions["availability_timestamp"], utc=True, errors="coerce"
    )
    materialization = pd.to_datetime(
        predictions["event_time_materialization_timestamp"],
        utc=True,
        errors="coerce",
    )
    training_end = pd.to_datetime(
        predictions["training_label_end_max"], utc=True, errors="coerce"
    )
    if (
        decision.isna().any()
        or prediction_time.isna().any()
        or not prediction_time.eq(decision + pd.Timedelta(minutes=1)).all()
        or not predictions.loc[
            available, "source_feature_available_at_decision"
        ].eq(True).all()
        or not availability.loc[available].le(decision.loc[available]).all()
        or not materialization.loc[available].le(decision.loc[available]).all()
        or training_end.loc[available].isna().any()
        or not training_end.loc[available].lt(decision.loc[available]).all()
    ):
        raise RuntimeError("canonical P3 decision-time eligibility failed")

    expected = p3_manifest["oos_lineage_validation"]
    status_counts = {
        f"{role}:{status}": int(count)
        for (role, status), count in predictions.groupby(
            ["model_role", "prediction_status"]
        ).size().items()
    }
    validation = {
        "result": "pass",
        "row_role_rows": len(predictions),
        "source_rows": int(predictions["stable_row_id"].nunique()),
        "available_oos_predictions": int(available.sum()),
        "excluded_row_roles": int(excluded.sum()),
        "in_sample_predictions": 0,
        "roles": list(MODEL_ROLES),
        "status_counts": status_counts,
        "source_lineage_complete": True,
        "model_lineage_complete_for_available_predictions": True,
        "decision_time_eligibility_reconfirmed": True,
        "training_labels_strictly_elapsed": True,
    }
    for key in (
        "row_role_rows",
        "source_rows",
        "available_oos_predictions",
        "excluded_row_roles",
        "in_sample_predictions",
    ):
        if int(validation[key]) != int(expected[key]):
            raise RuntimeError(f"canonical P3 manifest/table mismatch for {key}")
    return predictions.copy(), validation


def _gate(
    frame: pd.DataFrame,
    name: str,
    value: pd.Series,
    predicate: Any,
) -> None:
    supported = value.notna()
    frame[f"gate_{name}_value"] = value
    frame[f"gate_{name}_status"] = np.where(
        supported, "supported", "unavailable"
    )
    frame[f"gate_{name}_pass"] = supported & predicate(value)
    frame[f"gate_{name}_provenance"] = frame[
        "source_dataset_artifact_id"
    ]


def build_candidate_frame(
    predictions: pd.DataFrame,
    features: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Join the two exact OOS roles and reconfirm every non-model gate."""
    if (
        len(features) != SOURCE_ROWS
        or features["stable_row_id"].duplicated().any()
        or features["stable_row_id"].nunique() != SOURCE_ROWS
    ):
        raise RuntimeError("canonical P2 feature identity drifted")
    base_columns = [
        "stable_row_id",
        "entity_id",
        "cik",
        "ticker",
        "market",
        "fiscal_year",
        "period_type",
        "availability_timestamp",
        "event_time_materialization_timestamp",
        "source_feature_available_at_decision",
        "decision_timestamp",
        "prediction_timestamp",
        "entry_timestamp",
        "fold_id",
        "all_non_model_hard_gates_pass",
        "hard_gate_exclusion_codes",
        *SOURCE_LINEAGE_COLUMNS,
    ]
    tree = predictions.loc[
        predictions["model_role"].eq("decision_tree"),
        [*base_columns, *ROLE_OUTPUT_COLUMNS],
    ].copy()
    tree = tree.rename(
        columns={
            column: f"decision_tree_{column}"
            for column in ROLE_OUTPUT_COLUMNS
        }
    )
    ranker = predictions.loc[
        predictions["model_role"].eq("lightgbm_regression"),
        ["stable_row_id", *ROLE_OUTPUT_COLUMNS],
    ].copy()
    ranker = ranker.rename(
        columns={
            column: f"lightgbm_regression_{column}"
            for column in ROLE_OUTPUT_COLUMNS
        }
    )
    frame = tree.merge(ranker, on="stable_row_id", validate="one_to_one")

    feature_columns = [
        "stable_row_id",
        "provider_symbol",
        "provider_exchange",
        "exchange_calendar",
        "decision_market_cap",
        "feature_market_cap",
        "beneish_m_score",
        "piotroski_f_score",
        "piotroski_roa_pos",
        "altman_z_score",
        "ps_ratio_sector_pct",
        "momentum_12m_prior",
        "fraud_score_accounting",
        "fraud_score_dilution",
        "fraud_score_quality",
        "fraud_score_distress",
        "fraud_score_governance",
        "fraud_score_composite",
    ]
    missing_features = set(feature_columns) - set(features.columns)
    if missing_features:
        raise RuntimeError(
            f"canonical P2 feature table lacks P4 fields: {missing_features}"
        )
    frame = frame.merge(
        features[feature_columns],
        on="stable_row_id",
        how="left",
        validate="one_to_one",
    )
    if frame["provider_symbol"].isna().all():
        raise RuntimeError("canonical provider mapping is unavailable")

    _gate(frame, "market_us", frame["market"], lambda value: value.eq("US"))
    _gate(
        frame,
        "market_cap",
        pd.to_numeric(frame["feature_market_cap"], errors="coerce"),
        lambda value: value.ge(50_000_000) & value.le(MAX_MARKET_CAP_PROD),
    )
    _gate(
        frame,
        "beneish",
        pd.to_numeric(frame["beneish_m_score"], errors="coerce"),
        lambda value: value.lt(BENEISH_THRESHOLD),
    )
    _gate(
        frame,
        "piotroski",
        pd.to_numeric(frame["piotroski_f_score"], errors="coerce"),
        lambda value: value.ge(PIOTROSKI_MIN),
    )
    _gate(
        frame,
        "roa_positive",
        pd.to_numeric(frame["piotroski_roa_pos"], errors="coerce"),
        lambda value: value.eq(1),
    )
    _gate(
        frame,
        "altman",
        pd.to_numeric(frame["altman_z_score"], errors="coerce"),
        lambda value: value.gt(ALTMAN_Z_MIN),
    )
    _gate(
        frame,
        "value",
        pd.to_numeric(frame["ps_ratio_sector_pct"], errors="coerce"),
        lambda value: value.le(VALUE_GATE_PCT),
    )
    _gate(
        frame,
        "momentum",
        pd.to_numeric(frame["momentum_12m_prior"], errors="coerce"),
        lambda value: value.gt(MOMENTUM_12M_MIN),
    )
    recomputed_pass = frame[
        [f"gate_{name}_pass" for name in HARD_GATE_NAMES]
    ].all(axis=1)
    recomputed_codes = frame.apply(
        lambda row: json.dumps(
            [
                (
                    f"missing_gate_evidence:{name}"
                    if row[f"gate_{name}_status"] != "supported"
                    else f"hard_gate_failed:{name}"
                )
                for name in HARD_GATE_NAMES
                if not bool(row[f"gate_{name}_pass"])
            ],
            separators=(",", ":"),
        ),
        axis=1,
    )
    if (
        not recomputed_pass.eq(frame["all_non_model_hard_gates_pass"]).all()
        or not recomputed_codes.eq(frame["hard_gate_exclusion_codes"]).all()
    ):
        raise RuntimeError("P4 hard-gate recomputation differs from canonical P3")

    frame["tree_role_pass"] = (
        frame["decision_tree_prediction_status"].eq(AVAILABLE_PREDICTION)
        & np.isfinite(
            pd.to_numeric(frame["decision_tree_prediction"], errors="coerce")
        )
    )
    frame["ranker_role_pass"] = (
        frame["lightgbm_regression_prediction_status"].eq(
            AVAILABLE_PREDICTION
        )
        & np.isfinite(
            pd.to_numeric(
                frame["lightgbm_regression_prediction"], errors="coerce"
            )
        )
    )
    frame["tree_threshold_pass"] = (
        frame["tree_role_pass"]
        & frame["decision_tree_prediction"].ge(TREE_THRESHOLD)
    )
    frame["liquidity_required"] = (
        recomputed_pass
        & frame["tree_role_pass"]
        & frame["ranker_role_pass"]
        & frame["tree_threshold_pass"]
    )
    required = frame.loc[frame["liquidity_required"]]
    if required.duplicated(
        ["provider_symbol", "prediction_timestamp"]
    ).any():
        raise RuntimeError("candidate-wide liquidity identity is ambiguous")
    if required[
        ["provider_symbol", "provider_exchange", "exchange_calendar"]
    ].isna().any().any():
        raise RuntimeError("liquidity-required candidate lacks provider mapping")

    validation = {
        "result": "pass",
        "source_rows": len(frame),
        "hard_gate_recomputation_exact": True,
        "liquidity_required_rows": len(required),
        "liquidity_required_symbols": int(
            required["provider_symbol"].nunique()
        ),
        "both_oos_roles_required": True,
        "tree_threshold": TREE_THRESHOLD,
    }
    return frame, validation


def _load_market_indexes() -> tuple[
    pd.DataFrame,
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    calendar = pd.read_parquet(
        SESSION8E / "calendar/xnys_regular_sessions.parquet"
    )
    calendar["session_date"] = pd.to_datetime(
        calendar["session_date"]
    ).dt.strftime("%Y-%m-%d")
    calendar["market_open"] = pd.to_datetime(
        calendar["market_open"], utc=True
    )
    calendar["market_close"] = pd.to_datetime(
        calendar["market_close"], utc=True
    )
    calendar = calendar.sort_values("market_close").reset_index(drop=True)
    responses: dict[str, dict[str, Any]] = {}
    response_path = SESSION8E / "raw/response_manifest.jsonl"
    for line in response_path.read_text().splitlines():
        item = json.loads(line)
        responses[str(item["symbol"])] = item
    inventory = {
        str(item["path"]): item
        for item in json.loads(
            (SESSION8E / "raw/raw_inventory.json").read_text()
        )
    }
    return calendar, responses, inventory


def _normalise_symbol(value: Any) -> str:
    return str(value or "").strip().upper().replace(".", "-")


def _load_raw_symbol(
    symbol: str,
    responses: dict[str, dict[str, Any]],
    inventory: dict[str, dict[str, Any]],
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    item = responses.get(symbol)
    if not item or item.get("status") != "success" or not item.get("stored_path"):
        raise RuntimeError(EX_LIQUIDITY_RESPONSE)
    relative = str(item["stored_path"])
    raw_record = inventory.get(relative)
    if raw_record is None:
        raise RuntimeError(EX_LIQUIDITY_RESPONSE)
    path = SESSION8E / relative
    if (
        not path.is_file()
        or path.stat().st_size != int(raw_record["size_bytes"])
        or path.stat().st_size != int(item["stored_size_bytes"])
        or sha256_file(path) != raw_record["sha256"]
        or raw_record["sha256"] != item["stored_sha256"]
    ):
        raise RuntimeError(EX_LIQUIDITY_RESPONSE)
    with gzip.open(path, "rb") as handle:
        payload = handle.read()
    if (
        len(payload) != int(item["response_size_bytes"])
        or hashlib.sha256(payload).hexdigest() != item["response_sha256"]
    ):
        raise RuntimeError(EX_LIQUIDITY_RESPONSE)
    try:
        result = json.loads(payload)["chart"]["result"][0]
        meta = result["meta"]
        timestamps = result.get("timestamp") or []
        quote = result.get("indicators", {}).get("quote", [{}])[0]
        adjusted = (
            result.get("indicators", {}).get("adjclose", [{}])[0].get(
                "adjclose"
            )
            or []
        )
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []
        if not (
            len(timestamps)
            and len(timestamps) == len(closes) == len(volumes) == len(adjusted)
        ):
            raise RuntimeError(EX_LIQUIDITY_SESSION)
        timezone_name = meta.get(
            "exchangeTimezoneName", "America/New_York"
        )
        source_time = pd.to_datetime(timestamps, unit="s", utc=True)
        frame = pd.DataFrame(
            {
                "source_timestamp": source_time,
                "session_date": [
                    instant.tz_convert(ZoneInfo(timezone_name)).strftime(
                        "%Y-%m-%d"
                    )
                    for instant in source_time
                ],
                "unadjusted_close": pd.to_numeric(closes, errors="coerce"),
                "regular_session_volume": pd.to_numeric(
                    volumes, errors="coerce"
                ),
                "provider_total_return_close": pd.to_numeric(
                    adjusted, errors="coerce"
                ),
            }
        )
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(EX_LIQUIDITY_RESPONSE) from exc
    if frame["session_date"].duplicated().any():
        raise RuntimeError(EX_LIQUIDITY_SESSION)
    lineage = {
        "provider_symbol": symbol,
        "path": path.relative_to(ROOT).as_posix(),
        "stored_size_bytes": path.stat().st_size,
        "stored_sha256": item["stored_sha256"],
        "response_size_bytes": item["response_size_bytes"],
        "response_sha256": item["response_sha256"],
        "retrieved_at_utc": item["retrieved_at_utc"],
        "request_params_json": json.dumps(
            item["request_params"], sort_keys=True
        ),
    }
    return frame, meta, lineage


def _candidate_liquidity(
    row: Any,
    prices: pd.DataFrame,
    meta: dict[str, Any],
    calendar: pd.DataFrame,
    raw_lineage: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    prediction = pd.Timestamp(row.prediction_timestamp)
    expected = calendar.loc[
        calendar["market_close"].lt(prediction)
    ].tail(30)
    code: str | None = None
    if len(expected) != 30:
        code = EX_LIQUIDITY_INCOMPLETE
    expected_dates = expected["session_date"].tolist()
    selected = prices.loc[prices["session_date"].isin(expected_dates)].copy()
    if code is None and (
        len(selected) != 30
        or set(selected["session_date"]) != set(expected_dates)
    ):
        if (
            expected_dates
            and expected_dates[-1] not in set(selected["session_date"])
        ):
            code = EX_LIQUIDITY_STALE
        else:
            code = EX_LIQUIDITY_INCOMPLETE
    if code is None and _normalise_symbol(meta.get("symbol")) != str(
        row.provider_symbol
    ):
        code = EX_LIQUIDITY_SYMBOL
    if code is None and meta.get("exchangeName") != row.provider_exchange:
        code = EX_LIQUIDITY_EXCHANGE
    if code is None and meta.get("currency") != "USD":
        code = EX_LIQUIDITY_CURRENCY

    invalid_close = 0
    invalid_volume = 0
    median = np.nan
    if not selected.empty:
        selected = selected.merge(
            expected[
                ["session_date", "market_open", "market_close"]
            ],
            on="session_date",
            how="left",
            validate="one_to_one",
        ).sort_values("market_close")
        valid_close = (
            np.isfinite(selected["unadjusted_close"])
            & selected["unadjusted_close"].gt(0)
        )
        valid_volume = (
            np.isfinite(selected["regular_session_volume"])
            & selected["regular_session_volume"].gt(0)
        )
        invalid_close = int((~valid_close).sum())
        invalid_volume = int((~valid_volume).sum())
        if code is None and invalid_close:
            code = EX_LIQUIDITY_PRICE
        elif code is None and invalid_volume:
            code = EX_LIQUIDITY_VOLUME
        selected["daily_dollar_volume"] = (
            selected["unadjusted_close"]
            * selected["regular_session_volume"]
        )
        if code is None:
            median = float(selected["daily_dollar_volume"].median())
            if not np.isfinite(median) or median < MIN_ADTV:
                code = EX_LIQUIDITY_THRESHOLD
        selected["stable_row_id"] = row.stable_row_id
        selected["provider_symbol"] = row.provider_symbol
        selected["provider_exchange"] = row.provider_exchange
        selected["exchange_calendar"] = row.exchange_calendar
        selected["prediction_timestamp"] = prediction
        selected["currency"] = meta.get("currency")
        selected["adjustment"] = "none_unadjusted_for_liquidity"
        selected["source"] = "frozen_session8e_yahoo_chart_payload"
        selected["retrieved_at_utc"] = raw_lineage["retrieved_at_utc"]
        selected["raw_response_sha256"] = raw_lineage["response_sha256"]

    entry = calendar.loc[
        calendar["market_close"].gt(prediction)
    ].head(1)
    entry_timestamp = (
        entry.iloc[0]["market_close"] if len(entry) == 1 else pd.NaT
    )
    entry_date = (
        entry.iloc[0]["session_date"] if len(entry) == 1 else None
    )
    entry_prices = prices.loc[prices["session_date"].eq(entry_date)]
    entry_price_observed = bool(
        len(entry_prices) == 1
        and np.isfinite(entry_prices.iloc[0]["unadjusted_close"])
        and entry_prices.iloc[0]["unadjusted_close"] > 0
        and np.isfinite(
            entry_prices.iloc[0]["provider_total_return_close"]
        )
        and entry_prices.iloc[0]["provider_total_return_close"] > 0
    )
    coverage = {
        "stable_row_id": row.stable_row_id,
        "provider_symbol": row.provider_symbol,
        "provider_exchange": row.provider_exchange,
        "exchange_calendar": row.exchange_calendar,
        "prediction_timestamp": prediction,
        "expected_session_count": 30,
        "observed_expected_session_count": len(selected),
        "valid_session_count": (
            len(selected) - invalid_close - invalid_volume
        ),
        "invalid_close_session_count": invalid_close,
        "invalid_volume_session_count": invalid_volume,
        "expected_first_market_close": (
            expected.iloc[0]["market_close"] if len(expected) else pd.NaT
        ),
        "expected_last_market_close": (
            expected.iloc[-1]["market_close"] if len(expected) else pd.NaT
        ),
        "median_30_session_dollar_volume": median,
        "minimum_adtv_usd": MIN_ADTV,
        "liquidity_pass": code is None,
        "exclusion_code": code,
        "entry_timestamp": entry_timestamp,
        "entry_session_date": entry_date,
        "entry_price_observed": entry_price_observed,
        "source": "frozen_session8e_yahoo_chart_payload",
        "retrieved_at_utc": raw_lineage["retrieved_at_utc"],
        "source_symbol": meta.get("symbol"),
        "source_exchange": meta.get("exchangeName"),
        "currency": meta.get("currency"),
        "exchange_timezone": meta.get("exchangeTimezoneName"),
        "raw_response_sha256": raw_lineage["response_sha256"],
        "adjustment": "none_unadjusted_for_liquidity",
    }
    return selected, coverage


def evaluate_candidate_wide_liquidity(
    candidates: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Validate exactly 30 frozen pre-prediction sessions for every candidate."""
    required = candidates.loc[candidates["liquidity_required"]].copy()
    calendar, responses, inventory = _load_market_indexes()
    price_cache: dict[str, tuple[pd.DataFrame, dict, dict]] = {}
    raw_rows: list[dict[str, Any]] = []
    evidence_rows: list[pd.DataFrame] = []
    coverage_rows: list[dict[str, Any]] = []
    symbol_failures: dict[str, str] = {}

    for symbol in sorted(required["provider_symbol"].unique()):
        try:
            price_cache[symbol] = _load_raw_symbol(
                symbol, responses, inventory
            )
            raw_rows.append(price_cache[symbol][2])
        except RuntimeError as exc:
            symbol_failures[symbol] = str(exc)

    for row in required.sort_values("stable_row_id").itertuples(index=False):
        if row.provider_symbol in symbol_failures:
            coverage_rows.append(
                {
                    "stable_row_id": row.stable_row_id,
                    "provider_symbol": row.provider_symbol,
                    "provider_exchange": row.provider_exchange,
                    "exchange_calendar": row.exchange_calendar,
                    "prediction_timestamp": row.prediction_timestamp,
                    "expected_session_count": 30,
                    "observed_expected_session_count": 0,
                    "valid_session_count": 0,
                    "invalid_close_session_count": 0,
                    "invalid_volume_session_count": 0,
                    "expected_first_market_close": pd.NaT,
                    "expected_last_market_close": pd.NaT,
                    "median_30_session_dollar_volume": np.nan,
                    "minimum_adtv_usd": MIN_ADTV,
                    "liquidity_pass": False,
                    "exclusion_code": symbol_failures[row.provider_symbol],
                    "entry_timestamp": pd.NaT,
                    "entry_session_date": None,
                    "entry_price_observed": False,
                    "source": "frozen_session8e_yahoo_chart_payload",
                    "retrieved_at_utc": None,
                    "source_symbol": None,
                    "source_exchange": None,
                    "currency": None,
                    "exchange_timezone": None,
                    "raw_response_sha256": None,
                    "adjustment": "none_unadjusted_for_liquidity",
                }
            )
            continue
        prices, meta, raw_lineage = price_cache[row.provider_symbol]
        evidence, coverage = _candidate_liquidity(
            row, prices, meta, calendar, raw_lineage
        )
        if not evidence.empty:
            evidence_rows.append(evidence)
        coverage_rows.append(coverage)

    evidence = (
        pd.concat(evidence_rows, ignore_index=True)
        if evidence_rows
        else pd.DataFrame()
    )
    coverage = pd.DataFrame(coverage_rows)
    raw_lineage = pd.DataFrame(raw_rows)
    if (
        len(coverage) != len(required)
        or coverage["stable_row_id"].duplicated().any()
        or set(coverage["stable_row_id"]) != set(required["stable_row_id"])
    ):
        raise RuntimeError("candidate-wide liquidity coverage is incomplete")
    passing = coverage.loc[coverage["liquidity_pass"]]
    if (
        not passing["valid_session_count"].eq(30).all()
        or passing["exclusion_code"].notna().any()
        or not passing["median_30_session_dollar_volume"].ge(
            MIN_ADTV
        ).all()
    ):
        raise RuntimeError("passing candidate has invalid liquidity evidence")
    if not evidence.empty and not evidence["market_close"].lt(
        evidence["prediction_timestamp"]
    ).all():
        raise RuntimeError("liquidity evidence contains a post-prediction close")
    evidence_counts = evidence.groupby("stable_row_id").size()
    evidenced_ids = set(evidence_counts.index)
    if not evidence_counts.reindex(evidenced_ids).eq(30).all():
        raise RuntimeError("liquidity evidence is not exactly 30 sessions")
    recompute = evidence.loc[
        evidence["stable_row_id"].isin(passing["stable_row_id"])
    ].groupby("stable_row_id")["daily_dollar_volume"].median()
    frozen = passing.set_index("stable_row_id")[
        "median_30_session_dollar_volume"
    ]
    if not np.allclose(
        recompute.sort_index(),
        frozen.reindex(recompute.index).sort_index(),
        rtol=0,
        atol=1e-9,
    ):
        raise RuntimeError("liquidity dollar-volume arithmetic drifted")
    validation = {
        "result": "pass",
        "liquidity_required_rows": len(required),
        "liquidity_coverage_rows": len(coverage),
        "liquidity_evidence_rows": len(evidence),
        "liquidity_pass_rows": len(passing),
        "raw_symbols_consumed": len(raw_lineage),
        "raw_symbol_failures": len(symbol_failures),
        "candidate_wide_before_ranking": True,
        "exact_pre_prediction_30_session_windows": True,
        "daily_dollar_volume_arithmetic_recomputed": True,
        "minimum_adtv_usd": MIN_ADTV,
    }
    return evidence, coverage, raw_lineage, validation


def _json_value(value: Any) -> str | None:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (float, np.floating)) and not np.isfinite(value):
        return None
    if isinstance(value, np.generic):
        value = value.item()
    return json.dumps(value, sort_keys=True)


def materialize_portfolios(
    candidates: pd.DataFrame,
    liquidity: pd.DataFrame,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    dict[str, Any],
]:
    """Rank only after every fixed gate and preserve incomplete periods."""
    candidates = candidates.rename(
        columns={
            "entry_timestamp": "p3_observed_label_entry_timestamp",
        }
    )
    liq = liquidity.rename(
        columns={"exclusion_code": "liquidity_exclusion_code"}
    )
    keep_liquidity = [
        "stable_row_id",
        "liquidity_pass",
        "median_30_session_dollar_volume",
        "liquidity_exclusion_code",
        "entry_timestamp",
        "entry_session_date",
        "entry_price_observed",
        "raw_response_sha256",
    ]
    frame = candidates.merge(
        liq[keep_liquidity],
        on="stable_row_id",
        how="left",
        validate="one_to_one",
    )
    frame["liquidity_pass"] = frame["liquidity_pass"].eq(True)
    frame["liquidity_status"] = np.where(
        frame["liquidity_required"],
        np.where(frame["liquidity_pass"], "pass", "fail"),
        "not_required",
    )
    frame["eligible_before_period_completeness"] = (
        frame["liquidity_required"] & frame["liquidity_pass"]
    )
    frame["rank"] = pd.Series(pd.NA, index=frame.index, dtype="Int64")
    frame["holding"] = False
    frame["weight"] = np.nan
    period_rows: list[dict[str, Any]] = []
    for decision, group in frame.groupby("decision_timestamp", sort=True):
        eligible = group.loc[
            group["eligible_before_period_completeness"]
        ].sort_values(
            ["lightgbm_regression_prediction", "stable_row_id"],
            ascending=[False, True],
        )
        frame.loc[eligible.index, "rank"] = pd.Series(
            np.arange(1, len(eligible) + 1),
            index=eligible.index,
            dtype="Int64",
        )
        supported = len(eligible) >= TARGET_N
        if supported:
            selected = eligible.index[:TARGET_N]
            frame.loc[selected, "holding"] = True
            frame.loc[selected, "weight"] = WEIGHT
        period_rows.append(
            {
                "decision_timestamp": decision,
                "prediction_timestamp": group[
                    "prediction_timestamp"
                ].iloc[0],
                "source_candidates": len(group),
                "liquidity_required_candidates": int(
                    group["liquidity_required"].sum()
                ),
                "liquidity_pass_candidates": len(eligible),
                "period_supported": supported,
                "holding_count": TARGET_N if supported else 0,
                "exclusion_code": (
                    None if supported else EX_PERIOD_INCOMPLETE
                ),
            }
        )
    periods = pd.DataFrame(period_rows)

    gate_rows: list[dict[str, Any]] = []
    exclusion_rows: list[dict[str, Any]] = []
    for row in frame.itertuples(index=False):
        for gate in HARD_GATE_NAMES:
            gate_rows.append(
                {
                    "stable_row_id": row.stable_row_id,
                    "decision_timestamp": row.decision_timestamp,
                    "gate": gate,
                    "status": getattr(row, f"gate_{gate}_status"),
                    "pass": getattr(row, f"gate_{gate}_pass"),
                    "value_json": _json_value(
                        getattr(row, f"gate_{gate}_value")
                    ),
                    "provenance": getattr(
                        row, f"gate_{gate}_provenance"
                    ),
                }
            )
        gate_rows.extend(
            [
                {
                    "stable_row_id": row.stable_row_id,
                    "decision_timestamp": row.decision_timestamp,
                    "gate": "oos_tree_role",
                    "status": row.decision_tree_prediction_status,
                    "pass": row.tree_role_pass,
                    "value_json": _json_value(
                        row.decision_tree_prediction
                    ),
                    "provenance": row.decision_tree_model_artifact_id,
                },
                {
                    "stable_row_id": row.stable_row_id,
                    "decision_timestamp": row.decision_timestamp,
                    "gate": "oos_lightgbm_role",
                    "status": row.lightgbm_regression_prediction_status,
                    "pass": row.ranker_role_pass,
                    "value_json": _json_value(
                        row.lightgbm_regression_prediction
                    ),
                    "provenance": (
                        row.lightgbm_regression_model_artifact_id
                    ),
                },
                {
                    "stable_row_id": row.stable_row_id,
                    "decision_timestamp": row.decision_timestamp,
                    "gate": "tree_probability_0_55",
                    "status": (
                        "evaluated"
                        if row.tree_role_pass
                        else "unavailable"
                    ),
                    "pass": row.tree_threshold_pass,
                    "value_json": _json_value(
                        row.decision_tree_prediction
                    ),
                    "provenance": f"canonical_p3:{P3_MANIFEST_SHA256}",
                },
                {
                    "stable_row_id": row.stable_row_id,
                    "decision_timestamp": row.decision_timestamp,
                    "gate": "liquidity",
                    "status": row.liquidity_status,
                    "pass": row.liquidity_pass,
                    "value_json": _json_value(
                        row.median_30_session_dollar_volume
                    ),
                    "provenance": (
                        "frozen_session8e_candidate_wide_pre_prediction"
                    ),
                },
            ]
        )
        codes = json.loads(row.hard_gate_exclusion_codes)
        if not row.tree_role_pass:
            codes.append(EX_TREE_ROLE)
        if not row.ranker_role_pass:
            codes.append(EX_RANK_ROLE)
        if row.tree_role_pass and not row.tree_threshold_pass:
            codes.append(EX_TREE_THRESHOLD)
        if row.liquidity_required and not row.liquidity_pass:
            codes.append(
                row.liquidity_exclusion_code or EX_LIQUIDITY_RESPONSE
            )
        for code in dict.fromkeys(codes):
            exclusion_rows.append(
                {
                    "stable_row_id": row.stable_row_id,
                    "decision_timestamp": row.decision_timestamp,
                    "exclusion_code": code,
                }
            )
    gates = pd.DataFrame(gate_rows)
    exclusions = pd.DataFrame(exclusion_rows)
    holdings = frame.loc[frame["holding"]].copy()

    supported = periods.loc[
        periods["period_supported"], "decision_timestamp"
    ]
    holding_counts = (
        holdings.groupby("decision_timestamp")
        .size()
        .reindex(supported)
    )
    weight_sums = (
        holdings.groupby("decision_timestamp")["weight"]
        .sum()
        .reindex(supported)
    )
    if (
        not holding_counts.eq(TARGET_N).all()
        or not np.allclose(weight_sums, 1.0)
        or holdings.duplicated(
            ["decision_timestamp", "stable_row_id"]
        ).any()
        or gates.duplicated(["stable_row_id", "gate"]).any()
        or len(gates) != len(frame) * 12
    ):
        raise RuntimeError("canonical portfolio construction invariants failed")
    unsupported = set(
        periods.loc[
            ~periods["period_supported"], "decision_timestamp"
        ]
    )
    if set(holdings["decision_timestamp"]) & unsupported:
        raise RuntimeError("an incomplete period formed a portfolio")
    latest_scored = frame.loc[
        frame["tree_role_pass"] & frame["ranker_role_pass"],
        "decision_timestamp",
    ].max()
    latest_period = periods.loc[
        periods["decision_timestamp"].eq(latest_scored)
    ]
    if len(latest_period) != 1 or not bool(
        latest_period.iloc[0]["period_supported"]
    ):
        raise RuntimeError("latest canonical decision cannot form a shortlist")
    shortlist = holdings.loc[
        holdings["decision_timestamp"].eq(latest_scored)
    ].sort_values("rank")
    validation = {
        "result": "pass",
        "source_rows": len(frame),
        "gate_rows": len(gates),
        "exclusion_rows": len(exclusions),
        "supported_periods": int(periods["period_supported"].sum()),
        "holding_rows": len(holdings),
        "latest_decision_timestamp": latest_scored,
        "latest_shortlist_rows": len(shortlist),
        "ranking_after_all_gates": True,
        "candidate_wide_liquidity_before_ranking": True,
        "target_n": TARGET_N,
        "weight_each": WEIGHT,
        "incomplete_periods_have_zero_holdings": True,
    }
    return frame, gates, exclusions, holdings, periods, validation


def _selected_feature_values(
    row: pd.Series,
    selected_features_json: Any,
) -> str:
    try:
        selected = json.loads(str(selected_features_json))
    except (TypeError, json.JSONDecodeError):
        selected = []
    values: dict[str, Any] = {}
    for feature in selected:
        value = row.get(feature)
        if value is None or value is pd.NA or pd.isna(value):
            values[feature] = None
        elif isinstance(value, np.generic):
            values[feature] = value.item()
        else:
            values[feature] = value
    return json.dumps(values, sort_keys=True)


def add_explanations(
    holdings: pd.DataFrame,
    features: pd.DataFrame,
) -> pd.DataFrame:
    """Attach decision-time feature values and plain-language rank context."""
    frame = holdings.merge(
        features,
        on="stable_row_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_feature"),
    )
    tree_values = []
    ranker_values = []
    explanations = []
    for _, row in frame.iterrows():
        tree_values.append(
            _selected_feature_values(
                row, row["decision_tree_selected_features_json"]
            )
        )
        ranker_values.append(
            _selected_feature_values(
                row,
                row["lightgbm_regression_selected_features_json"],
            )
        )
        explanations.append(
            (
                "Passed all eight fixed decision-time hard gates; "
                f"tree OOS score {row['decision_tree_prediction']:.4f} "
                f"met the {TREE_THRESHOLD:.2f} agreement gate; "
                f"LightGBM OOS three-year model score "
                f"{row['lightgbm_regression_prediction']:.4f} set rank "
                f"{int(row['rank'])}; median pre-decision 30-session "
                f"dollar volume was "
                f"${row['median_30_session_dollar_volume']:,.0f}. "
                "Model scores are frozen research outputs, not forecasts "
                "or promises of future return."
            )
        )
    frame["decision_tree_selected_feature_values_json"] = tree_values
    frame["lightgbm_selected_feature_values_json"] = ranker_values
    frame["explanation"] = explanations
    frame["accepted_p3_manifest_sha256"] = P3_MANIFEST_SHA256
    return frame


def build_backtest_status(
    holdings: pd.DataFrame,
    periods: pd.DataFrame,
    calendar: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Freeze the vintage clocks and fail official performance closed."""
    calendar = calendar.sort_values("market_close")
    plans: list[dict[str, Any]] = []
    for row in holdings.itertuples(index=False):
        entry = pd.Timestamp(row.entry_timestamp)
        target_exit = entry + pd.DateOffset(months=HOLDING_MONTHS)
        exit_rows = calendar.loc[
            calendar["market_close"].ge(target_exit)
            & calendar["market_close"].le(
                target_exit + pd.Timedelta(days=10)
            )
        ].head(1)
        exit_timestamp = (
            exit_rows.iloc[0]["market_close"]
            if len(exit_rows)
            else pd.NaT
        )
        plans.append(
            {
                "stable_row_id": row.stable_row_id,
                "ticker": row.ticker,
                "decision_timestamp": row.decision_timestamp,
                "prediction_timestamp": row.prediction_timestamp,
                "entry_timestamp": entry,
                "target_exit_timestamp": target_exit,
                "calendar_exit_timestamp": exit_timestamp,
                "holding_months": HOLDING_MONTHS,
                "weight": row.weight,
                "planned_vintage_aum_usd": AUM_USD,
                "planned_entry_notional_usd": AUM_USD * row.weight,
                "transaction_cost_rate_per_side": (
                    TRANSACTION_COST_PER_SIDE
                ),
                "transaction_cost_basis": "absolute_actual_traded_notional",
                "vintage_clock_status": (
                    "calendar_exit_available"
                    if pd.notna(exit_timestamp)
                    else "open_or_calendar_exit_unavailable"
                ),
            }
        )
    plan = pd.DataFrame(plans)
    status = {
        "status": "unavailable_fail_closed",
        "official_performance_available": False,
        "performance_calculated": False,
        "backtest_run": False,
        "return_policy": OBSERVED_ONLY,
        "holding_contract": {
            "separate_annual_vintages": True,
            "holding_months": HOLDING_MONTHS,
            "overlapping_vintages": True,
            "later_vintage_does_not_rebalance_earlier_vintage": True,
            "entry": (
                "first frozen common regular-session close strictly after "
                "prediction"
            ),
            "exit": (
                "first common regular-session close on or after entry plus "
                "36 calendar months, within ten calendar days"
            ),
        },
        "transaction_cost_policy": {
            "rate_per_side": TRANSACTION_COST_PER_SIDE,
            "basis_points_per_side": 25,
            "round_trip_basis_points": 50,
            "cost_equation": "0.0025 * absolute actual traded notional",
            "turnover_equation": (
                "sum(abs(trade_notional)) / pre_cost_vintage_nav"
            ),
            "half_turnover_multiplier": False,
            "legacy_market_cap_tiers_used": False,
            "legacy_30_60_bps_defaults_used": False,
        },
        "monthly_nav_implementation": "backtest/monthly_nav.py",
        "monthly_nav_status": (
            "not_run; its fail-closed observed-only missing-price/event "
            "semantics are retained, while the legacy December calendar is "
            "not substituted for the accepted July overlapping-vintage clock"
        ),
        "supported_selection_periods": int(
            periods["period_supported"].sum()
        ),
        "planned_holding_rows": len(plan),
        "blockers": list(BACKTEST_BLOCKERS),
        "old_v3_performance_transferred": False,
        "future_performance_claimed": False,
    }
    return plan, status


def render_report(
    shortlist: pd.DataFrame,
    period_validation: dict[str, Any],
    liquidity_validation: dict[str, Any],
    backtest_status: dict[str, Any],
) -> str:
    decision = pd.Timestamp(
        period_validation["latest_decision_timestamp"]
    ).strftime("%Y-%m-%d")
    rows = []
    for row in shortlist.sort_values("rank").itertuples(index=False):
        rows.append(
            "| {rank} | {ticker} | {tree:.4f} | {ranker:.4f} | "
            "${cap:,.0f} | ${adtv:,.0f} | {fraud:.3f} | "
            "`{stable}` |".format(
                rank=int(row.rank),
                ticker=row.ticker,
                tree=row.decision_tree_prediction,
                ranker=row.lightgbm_regression_prediction,
                cap=row.feature_market_cap,
                adtv=row.median_30_session_dollar_volume,
                fraud=row.fraud_score_composite,
                stable=str(row.stable_row_id)[:12],
            )
        )
    blocker_lines = "\n".join(
        f"- `{item['code']}`: {item['required']}"
        for item in backtest_status["blockers"]
    )
    return f"""# Canonical US Annual Three-Year Product

## Use

Run:

```bash
python3 -m portfolio.build_canonical_product
```

The command refuses a non-empty target and consumes only canonical P3 manifest
`{P3_MANIFEST_SHA256}` and its row-complete OOS prediction table. It does not
train a model, refresh data, use historical V3 predictions, or overwrite P2/P3.

## Latest frozen shortlist

Decision date: **{decision}**. This is a reproducible research shortlist from
the frozen evidence, not personalized investment advice and not a claim about
future performance.

| Rank | Ticker | Tree OOS score | LightGBM 3y model score | Decision cap | Median 30-session dollar volume | Fraud-risk composite | Stable row |
|---:|---|---:|---:|---:|---:|---:|---|
{chr(10).join(rows)}

Every row passed all eight fixed decision-time hard gates, both exact P3 OOS
roles, the 0.55 tree agreement threshold, and the $1,333,333.33 minimum median
30-session dollar-volume gate before ranking. The shortlist artifact carries
the exact selected-feature values, model/preprocessing/target/configuration
artifact IDs, raw liquidity hash, stable row ID, and source manifest hashes.

## Route validation

- P3 row-role records consumed: 87,612 (43,806 source rows × two roles).
- Candidate-wide liquidity rows: {liquidity_validation['liquidity_coverage_rows']:,}.
- Liquidity-passing candidate rows: {liquidity_validation['liquidity_pass_rows']:,}.
- Supported selection periods: {period_validation['supported_periods']}.
- Latest shortlist rows: {period_validation['latest_shortlist_rows']}.
- Portfolio construction: top 15 after all gates, equal weight 1/15.

## Backtest and transaction-cost status

Official performance is **unavailable and fail-closed**. No performance metric
or old V3 result is presented. The accepted cost contract is 25 bps per side
on absolute actual traded notional (50 bps round trip), with no half-turnover
multiplier and no legacy market-cap tiers. It is frozen in the backtest plan
but not charged against synthetic or incomplete market evidence.

Exact blockers:

{blocker_lines}

## Limitations

- The US annual population is historically enriched, not comprehensively
  survivorship-free. Free sources do not establish CRSP-quality historical
  membership, security/ticker histories, delisting terms, or delisting
  returns.
- Frozen Yahoo payloads are used only for the candidate-wide decision-time
  liquidity gate. They are not silently promoted to the missing canonical
  security/action ledger.
- Certified macro vintages remain unavailable and no macro value, interaction,
  or risk-free fallback is synthesized.
- Model scores are OOS research outputs for the frozen decisions. They are not
  forecasts, guarantees, or evidence of future performance.
"""


def _capture_dirty_state(artifact_root: Path) -> list[Path]:
    lineage = artifact_root / "lineage"
    lineage.mkdir(parents=True, exist_ok=True)
    status = lineage / "git_status_porcelain.txt"
    patch = lineage / "tracked_dirty.patch"
    untracked = lineage / "untracked_inventory.json"
    status.write_bytes(
        subprocess.check_output(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=ROOT,
        )
    )
    patch.write_bytes(
        subprocess.check_output(
            ["git", "diff", "--binary", "HEAD", "--", "."],
            cwd=ROOT,
        )
    )
    inventory = []
    for raw in subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
    ).split(b"\0"):
        if not raw:
            continue
        path = ROOT / raw.decode()
        if (
            path.is_file()
            and artifact_root not in path.parents
            and path != untracked
        ):
            inventory.append(record(path, "untracked_dirty_state"))
    write_json(untracked, inventory)
    return [status, patch, untracked]


def build(
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
) -> dict[str, Any]:
    """Build a new, non-overwriting P4 artifact."""
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(
            f"artifact root already exists and is non-empty: {artifact_root}"
        )
    p3, _p2, session8e, preflight = validate_inputs()
    predictions, prediction_validation = consume_canonical_predictions(
        pd.read_parquet(P3_PREDICTIONS), p3
    )
    features = pd.read_parquet(P2_FEATURES)
    candidates, gate_validation = build_candidate_frame(
        predictions, features
    )

    for folder in (
        "inputs",
        "configuration",
        "outputs",
        "support",
        "report",
        "lineage",
    ):
        (artifact_root / folder).mkdir(parents=True, exist_ok=True)
    shutil.copy2(P3_MANIFEST, artifact_root / "inputs/canonical_p3_manifest.json")
    shutil.copy2(
        SESSION8E_MANIFEST,
        artifact_root / "inputs/session8e_manifest.json",
    )
    write_json(
        artifact_root / "support/preflight_validation.json", preflight
    )
    write_json(
        artifact_root / "support/prediction_lineage_validation.json",
        prediction_validation,
    )
    write_json(
        artifact_root / "support/decision_gate_validation.json",
        gate_validation,
    )

    evidence, liquidity, raw_lineage, liquidity_validation = (
        evaluate_candidate_wide_liquidity(candidates)
    )
    (
        candidate_table,
        gates,
        exclusions,
        holdings,
        periods,
        portfolio_validation,
    ) = materialize_portfolios(candidates, liquidity)
    explained_holdings = add_explanations(holdings, features)
    latest_decision = pd.Timestamp(
        portfolio_validation["latest_decision_timestamp"]
    )
    shortlist = explained_holdings.loc[
        explained_holdings["decision_timestamp"].eq(latest_decision)
    ].sort_values("rank")
    if len(shortlist) != TARGET_N:
        raise RuntimeError("canonical latest shortlist is not exactly top 15")

    calendar, _, _ = _load_market_indexes()
    backtest_plan, backtest_status = build_backtest_status(
        explained_holdings, periods, calendar
    )
    report_trace = predictions.loc[
        predictions["stable_row_id"].isin(shortlist["stable_row_id"])
    ].merge(
        shortlist[["stable_row_id", "rank", "explanation"]],
        on="stable_row_id",
        validate="many_to_one",
    )
    if (
        len(report_trace) != TARGET_N * len(MODEL_ROLES)
        or report_trace.duplicated(["stable_row_id", "model_role"]).any()
    ):
        raise RuntimeError("prediction-to-report traceability is incomplete")

    predictions_output = predictions.copy()
    predictions_output["accepted_p3_manifest_sha256"] = P3_MANIFEST_SHA256
    predictions_output.to_parquet(
        artifact_root / "outputs/prediction_lineage.parquet", index=False
    )
    raw_lineage.to_parquet(
        artifact_root / "lineage/consumed_market_evidence.parquet",
        index=False,
    )
    evidence.to_parquet(
        artifact_root / "outputs/liquidity_evidence.parquet", index=False
    )
    liquidity.to_parquet(
        artifact_root / "support/liquidity_coverage.parquet", index=False
    )
    candidate_table.to_parquet(
        artifact_root / "outputs/candidates.parquet", index=False
    )
    gates.to_parquet(
        artifact_root / "outputs/gates.parquet", index=False
    )
    exclusions.to_parquet(
        artifact_root / "outputs/exclusions.parquet", index=False
    )
    explained_holdings.to_parquet(
        artifact_root / "outputs/holdings.parquet", index=False
    )
    shortlist.to_parquet(
        artifact_root / "outputs/latest_shortlist.parquet", index=False
    )
    periods.to_parquet(
        artifact_root / "support/period_coverage.parquet", index=False
    )
    report_trace.to_parquet(
        artifact_root / "outputs/report_traceability.parquet", index=False
    )
    backtest_plan.to_parquet(
        artifact_root / "outputs/backtest_vintage_plan.parquet", index=False
    )
    write_json(
        artifact_root / "support/liquidity_validation.json",
        liquidity_validation,
    )
    write_json(
        artifact_root / "support/portfolio_validation.json",
        portfolio_validation,
    )
    write_json(
        artifact_root / "support/backtest_status.json", backtest_status
    )

    contract = {
        "schema_version": 1,
        "session": "P4",
        "route_name": "canonical_us_annual_observed_only_3y_product",
        "source": {
            "canonical_p3_manifest_sha256": P3_MANIFEST_SHA256,
            "predictions": P3_PREDICTIONS.relative_to(ROOT).as_posix(),
            "predictions_sha256": P3_PREDICTIONS_SHA256,
            "historical_v3_predictions_allowed": False,
        },
        "selection": {
            "model_roles": list(MODEL_ROLES),
            "tree_threshold": TREE_THRESHOLD,
            "target_n": TARGET_N,
            "weight_each": WEIGHT,
            "rank": (
                "descending canonical P3 LightGBM prediction after all "
                "decision-time, model, and candidate-wide liquidity gates"
            ),
        },
        "liquidity": {
            "aum_usd": AUM_USD,
            "max_position_to_adtv": MAX_POSITION_TO_ADTV,
            "minimum_adtv_usd": MIN_ADTV,
            "window": (
                "exactly 30 frozen regular sessions whose market close is "
                "strictly before prediction"
            ),
            "daily_dollar_volume": (
                "unadjusted regular-session close * regular-session volume"
            ),
            "missingness": "fail_closed_no_substitution",
        },
        "backtest": backtest_status,
        "limitations": {
            "survivorship": (
                "historically enriched, not comprehensively "
                "survivorship-free"
            ),
            "performance": "unavailable_fail_closed",
            "macro": "no certified macro vintage values used",
        },
    }
    write_json(
        artifact_root / "configuration/product_contract.json", contract
    )
    report_text = render_report(
        shortlist,
        portfolio_validation,
        liquidity_validation,
        backtest_status,
    )
    report_path = artifact_root / "report/product_report.md"
    report_path.write_text(report_text)
    dirty_paths = _capture_dirty_state(artifact_root)

    artifact_paths = [
        path
        for path in sorted(artifact_root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    ]
    code_paths = [
        ROOT / "portfolio/build_canonical_product.py",
        ROOT / "tests/portfolio/test_build_canonical_product.py",
        ROOT / "portfolio/selection_contract.py",
        ROOT / "backtest/monthly_nav.py",
        ROOT / "modeling/constants.py",
    ]
    validated_inputs = [
        record(P3_MANIFEST, "canonical_p3_manifest"),
        record(P3_PREDICTIONS, "canonical_p3_row_complete_oos_predictions"),
        record(P2_MANIFEST, "canonical_p2_manifest_through_p3"),
        record(P2_FEATURES, "canonical_p2_features_through_p3"),
        record(SESSION8E_MANIFEST, "frozen_session8e_manifest"),
        record(
            SESSION8E / "raw/response_manifest.jsonl",
            "frozen_session8e_market_index",
        ),
        record(
            SESSION8E / "raw/raw_inventory.json",
            "frozen_session8e_market_index",
        ),
        record(
            SESSION8E / "calendar/xnys_regular_sessions.parquet",
            "frozen_session8e_market_calendar",
        ),
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": "CANONICAL_US_ANNUAL_OBSERVED_ONLY_3Y_PRODUCT",
        "created_at_utc": utc_now(),
        "canonical_entrypoint": {
            "command": "python3 -m portfolio.build_canonical_product",
            "artifact_root": artifact_root.relative_to(ROOT).as_posix(),
            "latest_shortlist": "outputs/latest_shortlist.parquet",
            "report": "report/product_report.md",
            "backtest_status": "support/backtest_status.json",
        },
        "claim": {
            "status": "accepted_with_fail_closed_performance",
            "product_session_p4_complete": True,
            "canonical_p3_predictions_consumed_only": True,
            "row_complete_oos_lineage_reconfirmed": True,
            "decision_time_eligibility_reconfirmed": True,
            "candidate_wide_liquidity_before_ranking": True,
            "portfolio_constructed": True,
            "explainable_shortlist_materialized": True,
            "prediction_to_report_traceability_complete": True,
            "backtest_route_connected": True,
            "backtest_run": False,
            "performance_calculated": False,
            "official_performance_available": False,
            "old_v3_performance_transferred": False,
            "future_performance_claimed": False,
            "external_data_sourced_or_refreshed": False,
            "uncertified_macro_values_used": False,
            "v3_4_resumed": False,
        },
        "configuration": contract,
        "preflight": preflight,
        "prediction_lineage_validation": prediction_validation,
        "decision_gate_validation": gate_validation,
        "liquidity_validation": liquidity_validation,
        "portfolio_validation": portfolio_validation,
        "backtest_status": backtest_status,
        "validated_inputs": validated_inputs,
        "records": [
            record(path, "p4_product_output_configuration_report_or_lineage")
            for path in artifact_paths
        ],
        "code_lineage": [
            record(path, "p4_builder_test_or_reused_dependency")
            for path in code_paths
        ],
        "limitations": [
            (
                "Official performance is unavailable and fail-closed because "
                "the accepted security/action market ledger and immutable "
                "DGS1MO ALFRED vintage are absent."
            ),
            (
                "The US annual population is historically enriched but not "
                "comprehensively survivorship-free; free sources do not "
                "provide CRSP-quality historical membership, security/ticker "
                "histories, delisting terms, or delisting returns."
            ),
            (
                "Frozen Yahoo payloads support the decision-time liquidity "
                "gate only and are not promoted to canonical performance "
                "evidence."
            ),
            (
                "No certified macro vintage values or interactions are used, "
                "and no future-performance claim is made."
            ),
        ],
        "dirty_state": {
            "complete_status_recorded": True,
            "records": [
                path.relative_to(ROOT).as_posix() for path in dirty_paths
            ],
        },
        "environment": {
            "python": sys.version.split()[0],
            "pandas": pd.__version__,
            "numpy": np.__version__,
        },
        "upstream_session8e_created_at_utc": session8e["created_at_utc"],
    }
    manifest_path = artifact_root / "manifest.json"
    write_json(manifest_path, manifest)
    return {
        "manifest": manifest_path,
        "manifest_sha256": sha256_file(manifest_path),
        "latest_shortlist_rows": len(shortlist),
        "holding_rows": len(explained_holdings),
        "supported_periods": portfolio_validation["supported_periods"],
        "performance_status": backtest_status["status"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=DEFAULT_ARTIFACT_ROOT,
    )
    args = parser.parse_args()
    result = build(args.artifact_root)
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
