"""Build the frozen US free-data product candidate without performance work.

US1A is a contract-first derivative of the accepted P2 data baseline, the
inner-evidence-selected M1A/M1C model route, and the unchanged P4 selection,
liquidity, and 15-name equal-weight portfolio rules.  It never fits a model,
collects data, calculates performance, or mutates a preserved artifact.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import inspect
import json
from pathlib import Path
import re
import shutil
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from backtest.free_data_v1_performance import verify_performance_artifact
from backtest.m1d_portfolio_performance import (
    B1D_ENGINE_SHA256,
    B1D_TEST_SHA256,
    B1E_MANIFEST_SHA256,
    M1A_MANIFEST_SHA256,
    M1A_ROOT,
    M1C_MANIFEST_SHA256,
    M1C_ROOT,
    P2_MANIFEST_SHA256,
    P2_ROOT,
    P3_MANIFEST_SHA256,
    P3_ROOT,
    P4_MANIFEST_SHA256,
    P4_ROOT,
    _build_locked_portfolio,
    verify_m1d_artifact,
)
from data_io.canonical_hf import build_publication_plan
from modeling.nested_walk_forward import load_frozen_m1a_contract
from modeling.run_nested_walk_forward import (
    _partial_attempt_inventory,
    verify_artifact as verify_m1c_artifact,
)
from portfolio.build_canonical_product import (
    HARD_GATE_NAMES,
    MODEL_ROLES,
    P2_FEATURES,
    _selected_feature_values,
    build_candidate_frame,
    evaluate_candidate_wide_liquidity,
    materialize_portfolios,
)
from portfolio.event_review_adjudication import (
    verify_event_review_adjudication_artifact,
)
from portfolio.selection_contract import MIN_ADTV, TARGET_N, TREE_THRESHOLD, WEIGHT


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_VERSION = "20260801T183000Z-us1a"
DEFAULT_PARENT = ROOT / "artifacts/product/us_free_v1"
DEFAULT_ARTIFACT_ROOT = DEFAULT_PARENT / DEFAULT_VERSION

D1_ROOT = ROOT / "artifacts/canonical_refresh/us/20260730T110301Z"
D1_P2_ROOT = D1_ROOT / "p2_review_candidate"
D1_P2_MANIFEST_SHA256 = (
    "545c2eec17dae8cdffd81fd8e1b89ebc1ccc3b47290b7b556f485bbaa5f436d6"
)
D1_COMPONENT_HASHES = {
    "universe": "84853ac89472b87a4bca6088fb39cf3f83fd66fd4048eb3c177515b572f44396",
    "filings": "7bb33dc02ea919d52e3b50e80c6880f2df9b8ec96e9116c40d26aab6a135ecc3",
    "market": "904dcef213b72fecfb43f2ec7e8933da705e737aea15b26b4545b5ae593561a4",
    "p2": D1_P2_MANIFEST_SHA256,
    "comparison": "20ecca80fd2c75fe999d21c712918e47153a71b378d8441501c460fcce25db2a",
}
D1_COMPONENT_PATHS = {
    "universe": D1_ROOT / "universe",
    "filings": D1_ROOT / "filings",
    "market": D1_ROOT / "market",
    "p2": D1_P2_ROOT,
    "comparison": D1_ROOT / "review/comparison",
}

B1E_ROOT = ROOT / "artifacts/performance/free_data_v1/20260801T011135Z-b1e"
M1D_ROOT = ROOT / "artifacts/performance/m1d/20260801T162953Z-m1d"
M1D_MANIFEST_SHA256 = (
    "b04cea8236da6cd92749410f6186360be5f29dbbbeb00a64f2ed07c180cc72ab"
)
M1D_LOCK_MANIFEST_SHA256 = (
    "757e19cd9e35290a6b339f79e2c44a0f1ddb47c03b913930b1abda84f0bf74bc"
)
I1_ROOT = ROOT / "artifacts/international/i1/20260801T180000Z-i1-ca"
I1_MANIFEST_SHA256 = (
    "d3f17854cf0839713163e8dda99aefedd6ba064a14a042a886770818a472d9f6"
)
E1_ROOT = ROOT / "artifacts/event_review/us/20260730T173110Z-e1-adjudication-v2"
E1_MANIFEST_SHA256 = (
    "dcbf95776b50139797410674d7f0ca410cfdaa1401917dea4fb47f7b00e9c7c6"
)

ARTIFACT_CLASS = "US_FREE_DATA_PRODUCT_CANDIDATE_US1A"
VERSION_PATTERN = re.compile(r"^\d{8}T\d{6}Z-us1a(?:-[a-z0-9._-]+)?$")
PROHIBITED_BASELINE_INPUTS = (
    "model scores",
    "shortlist membership",
    "portfolio results",
    "historical performance",
)
PROHIBITED_PRODUCT_ACTIONS = (
    "model fitting",
    "model retuning",
    "winner replacement",
    "external evidence collection",
    "historical performance calculation",
    "preserved artifact overwrite",
)


class USFreeProductContractError(RuntimeError):
    """Raised when the frozen US1A product contract cannot be proven."""


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_value(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).isoformat()
    if value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_value(payload), indent=2, sort_keys=True) + "\n")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _artifact_record(root: Path, path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _repo_record(path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(ROOT).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _version_timestamp(version: str) -> str:
    if not VERSION_PATTERN.fullmatch(version):
        raise USFreeProductContractError(f"invalid US1A version: {version}")
    value = datetime.strptime(version[:16], "%Y%m%dT%H%M%SZ").replace(
        tzinfo=timezone.utc
    )
    return value.isoformat()


def _verify_record_rows(base: Path, rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    count = 0
    total = 0
    paths: set[str] = set()
    for row in rows:
        relative = str(row.get("path", ""))
        if not relative or relative in paths or Path(relative).is_absolute():
            raise USFreeProductContractError(f"invalid or duplicate record: {relative}")
        paths.add(relative)
        path = base / relative
        if (
            not path.is_file()
            or path.stat().st_size != int(row.get("size_bytes", -1))
            or sha256_file(path) != row.get("sha256")
        ):
            raise USFreeProductContractError(f"record mismatch: {path}")
        count += 1
        total += path.stat().st_size
    return {"record_count": count, "record_bytes": total}


def _verify_manifest_records(
    root: Path,
    *,
    expected_manifest_sha256: str,
) -> dict[str, Any]:
    manifest_path = root / "manifest.json"
    actual = sha256_file(manifest_path)
    if actual != expected_manifest_sha256:
        raise USFreeProductContractError(
            f"manifest mismatch: {manifest_path} expected={expected_manifest_sha256} actual={actual}"
        )
    manifest = json.loads(manifest_path.read_text())
    summary = _verify_record_rows(root, manifest.get("records", []))
    return {"manifest_sha256": actual, **summary}


def _verify_raw_inventory(stage: str) -> dict[str, Any]:
    root = D1_ROOT / stage
    inventory_path = root / "raw/raw_inventory.json"
    rows = json.loads(inventory_path.read_text())
    summary = _verify_record_rows(root, rows)
    return {
        **summary,
        "inventory_path": inventory_path.relative_to(ROOT).as_posix(),
        "inventory_sha256": sha256_file(inventory_path),
    }


def assess_data_baseline() -> dict[str, Any]:
    """Apply only predeclared data gates; do not read any downstream score."""
    component_verification = {
        name: _verify_manifest_records(
            path,
            expected_manifest_sha256=D1_COMPONENT_HASHES[name],
        )
        for name, path in D1_COMPONENT_PATHS.items()
    }
    raw_verification = {
        stage: _verify_raw_inventory(stage)
        for stage in ("universe", "filings", "market")
    }

    p2 = pd.read_parquet(P2_FEATURES)
    d1_path = D1_P2_ROOT / "outputs/observed_only/features_taxonomy.parquet"
    d1 = pd.read_parquet(d1_path)
    p3_contract = json.loads(
        (P3_ROOT / "configuration/research_model_contract.json").read_text()
    )
    m1_candidates = list(p3_contract["feature_contract"]["candidate_columns"])
    m1c_ids = set(
        pd.read_parquet(
            M1C_ROOT / "outer_oos_predictions.parquet",
            columns=["stable_row_id"],
        )["stable_row_id"].astype(str)
    )
    p2_ids = set(p2["stable_row_id"].astype(str))
    d1_ids = set(d1["stable_row_id"].astype(str))

    decision = pd.to_datetime(d1["decision_timestamp"], utc=True, errors="coerce")
    availability = pd.to_datetime(
        d1["event_time_materialization_timestamp"], utc=True, errors="coerce"
    )
    labels = pd.read_parquet(D1_P2_ROOT / "inputs/observed_only_labels.parquet")
    support = pd.read_parquet(
        D1_P2_ROOT / "inputs/observed_only_row_horizon.parquet"
    )
    d1_entity_period = set(
        zip(d1["entity_id"].astype(str), pd.to_numeric(d1["fiscal_year"]).astype(int))
    )
    label_entity_period = set(
        zip(
            labels["entity_id"].astype(str),
            pd.to_numeric(labels["fiscal_year"]).astype(int),
        )
    )

    schema_pass = (
        list(p2.columns) == list(d1.columns)
        and all(str(p2[column].dtype) == str(d1[column].dtype) for column in p2)
    )
    stable_identity_pass = bool(
        d1["stable_row_id"].notna().all()
        and d1["stable_row_id"].nunique() == len(d1)
        and d1[["entity_id", "cik", "ticker", "fiscal_year"]].notna().all().all()
    )
    availability_pass = bool(availability.notna().all() and decision.notna().all())
    row_coverage_pass = d1_ids == m1c_ids
    target_support_pass = bool(
        set(labels["horizon"].astype(str)) == {"6m", "1y", "2y", "3y", "5y"}
        and label_entity_period.issubset(d1_entity_period)
        and not support.duplicated(["entity_id", "fiscal_year", "horizon"]).any()
        and set(support["classification"].astype(str))
        == {"supported", "unavailable", "excluded"}
    )
    feature_schema_compatible = bool(
        len(m1_candidates) == 200
        and set(m1_candidates).issubset(d1.columns)
        and [column for column in d1.columns if column in set(m1_candidates)]
        == m1_candidates
        and all(
            str(p2[column].dtype) == str(d1[column].dtype)
            for column in m1_candidates
        )
    )
    feature_compatibility_pass = bool(
        feature_schema_compatible
        and d1_ids == m1c_ids
        and D1_P2_MANIFEST_SHA256 == P2_MANIFEST_SHA256
    )
    deterministic_reconstruction_pass = bool(
        all(item["record_count"] > 0 for item in component_verification.values())
        and all(item["record_count"] > 0 for item in raw_verification.values())
    )

    gates = {
        "source_lineage": {
            "status": "pass",
            "rule": "all D1 component manifests, declared records, and preserved raw inventory bytes rehash",
            "components": component_verification,
            "raw_inventories": raw_verification,
        },
        "schema": {
            "status": "pass" if schema_pass else "fail",
            "rule": "exact P2 column order and dtypes",
            "baseline_columns": len(p2.columns),
            "candidate_columns": len(d1.columns),
        },
        "stable_identities": {
            "status": "pass" if stable_identity_pass else "fail",
            "rule": "unique non-null stable row ID and complete core identity",
            "candidate_rows": len(d1),
            "candidate_unique_stable_row_ids": int(d1["stable_row_id"].nunique()),
        },
        "availability_timestamps": {
            "status": "pass" if availability_pass else "fail",
            "rule": "every row has explicit event-time materialization and decision timestamps; late rows remain fail-closed",
            "materialized_rows": int(availability.notna().sum()),
            "available_by_decision_rows": int(availability.le(decision).sum()),
            "decision_late_rows": int(availability.gt(decision).sum()),
        },
        "row_coverage": {
            "status": "pass" if row_coverage_pass else "fail",
            "rule": "exact stable-row population consumed by the frozen M1C route",
            "accepted_p2_rows": len(p2),
            "d1_rows": len(d1),
            "common_stable_row_ids": len(p2_ids & d1_ids),
            "accepted_p2_only_stable_row_ids": len(p2_ids - d1_ids),
            "d1_only_stable_row_ids": len(d1_ids - p2_ids),
            "m1c_matches_accepted_p2": m1c_ids == p2_ids,
            "m1c_matches_d1": m1c_ids == d1_ids,
        },
        "target_support": {
            "status": "pass" if target_support_pass else "fail",
            "rule": "all five horizons and a unique supported/unavailable/excluded partition",
            "observed_label_rows": len(labels),
            "row_horizon_support_rows": len(support),
            "support_counts": {
                str(key): int(value)
                for key, value in support["classification"].value_counts().items()
            },
        },
        "feature_compatibility": {
            "status": "pass" if feature_compatibility_pass else "fail",
            "rule": "exact 200-column M1 feature contract plus accepted P2 manifest and row identities; no retraining or contract change",
            "feature_schema_compatible": feature_schema_compatible,
            "accepted_m1c_p2_manifest_sha256": P2_MANIFEST_SHA256,
            "d1_p2_manifest_sha256": D1_P2_MANIFEST_SHA256,
            "retraining_required_for_d1": True,
            "contract_change_required_for_d1": True,
        },
        "deterministic_reconstruction": {
            "status": "pass" if deterministic_reconstruction_pass else "fail",
            "rule": "versioned non-overwriting artifact with complete rehashable records and frozen raw inputs",
            "candidate_manifest_sha256": D1_P2_MANIFEST_SHA256,
        },
    }
    all_d1_gates_pass = all(item["status"] == "pass" for item in gates.values())
    selected = "D1" if all_d1_gates_pass else "accepted_P2"
    if selected == "accepted_P2" and not (m1c_ids == p2_ids):
        raise USFreeProductContractError("accepted P2 does not match frozen M1C IDs")
    return {
        "assessment_scope": "data_only_before_any_US1A_score_shortlist_or_portfolio_output",
        "prohibited_decision_inputs": list(PROHIBITED_BASELINE_INPUTS),
        "gates": gates,
        "d1_all_gates_pass": all_d1_gates_pass,
        "selected_baseline": selected,
        "selected_manifest_sha256": P2_MANIFEST_SHA256,
        "d1_release_status": "unsupported_for_US1A_frozen_M1_route",
        "decision_reason": (
            "D1 is internally reviewable but lacks the exact stable-row population and accepted manifest required by the frozen M1C route; using it would require retraining or a contract change."
        ),
        "downstream_output_observed_for_decision": False,
    }


def _verify_current_partial_attempts() -> dict[str, Any]:
    current = _partial_attempt_inventory(M1C_ROOT)
    preflight = json.loads((M1C_ROOT / "preflight.json").read_text())
    frozen = preflight.get("preserved_partial_attempts", [])
    if current != frozen or len(current) != 5:
        raise USFreeProductContractError("five preserved partial M1C attempts drifted")
    if any(item["outer_result_observed"] for item in current):
        raise USFreeProductContractError("a partial M1C attempt contains an outer result")
    return {
        "status": "pass",
        "attempt_count": len(current),
        "attempts": [
            {
                "artifact_directory": item["artifact_directory"],
                "status": item["status"],
                "file_count": item["file_count"],
                "model_file_count": item["model_file_count"],
                "outer_result_observed": item["outer_result_observed"],
                "record_bytes": int(sum(row["size_bytes"] for row in item["records"])),
                "records_sha256": hashlib.sha256(
                    json.dumps(item["records"], sort_keys=True).encode()
                ).hexdigest(),
            }
            for item in current
        ],
    }


def audit_preserved_boundaries() -> dict[str, Any]:
    """Rehash every required frozen boundary without executing a model or NAV."""
    publication = build_publication_plan()
    canonical = {
        item.spec.stage.lower(): {
            "manifest_sha256": item.spec.manifest_sha256,
            "file_count": len(item.files),
            "total_size_bytes": item.total_size_bytes,
        }
        for item in publication.artifacts
    }
    b1e = verify_performance_artifact(
        B1E_ROOT,
        expected_manifest_sha256=B1E_MANIFEST_SHA256,
    )
    m1a = load_frozen_m1a_contract()
    m1c = verify_m1c_artifact(
        M1C_ROOT,
        expected_manifest_sha256=M1C_MANIFEST_SHA256,
    )
    m1d = verify_m1d_artifact(
        M1D_ROOT,
        expected_manifest_sha256=M1D_MANIFEST_SHA256,
        expected_lock_manifest_sha256=M1D_LOCK_MANIFEST_SHA256,
    )
    i1 = _verify_manifest_records(
        I1_ROOT,
        expected_manifest_sha256=I1_MANIFEST_SHA256,
    )
    b1d_engine = ROOT / "backtest/free_data_v1_nav.py"
    b1d_test = ROOT / "tests/backtest/test_free_data_v1_nav.py"
    if sha256_file(b1d_engine) != B1D_ENGINE_SHA256:
        raise USFreeProductContractError("B1D engine drifted")
    if sha256_file(b1d_test) != B1D_TEST_SHA256:
        raise USFreeProductContractError("B1D test boundary drifted")
    return {
        "status": "pass",
        "performance_rerun": False,
        "canonical": canonical,
        "b1d": {
            "engine_sha256": B1D_ENGINE_SHA256,
            "test_sha256": B1D_TEST_SHA256,
        },
        "b1e": b1e,
        "m1a": {
            "manifest_sha256": M1A_MANIFEST_SHA256,
            "generated_records_verified": len(m1a.manifest["records"]),
            "outer_records": len(m1a.outer_folds),
            "inner_records": len(m1a.inner_folds),
            "maturity_records": len(m1a.label_maturity_ledger),
        },
        "m1c": m1c,
        "m1d": m1d,
        "m1d_lock_manifest_sha256": M1D_LOCK_MANIFEST_SHA256,
        "i1": i1,
        "partial_m1c_attempts": _verify_current_partial_attempts(),
    }


def _current_code_lineage() -> list[dict[str, Any]]:
    paths = [
        (ROOT / "portfolio/build_us_free_product.py", "us1a_builder"),
        (ROOT / "backtest/m1d_portfolio_performance.py", "m1c_to_p4_adapter"),
        (ROOT / "portfolio/build_canonical_product.py", "unchanged_p4_implementation"),
        (ROOT / "portfolio/selection_contract.py", "unchanged_p4_contract"),
        (ROOT / "modeling/nested_walk_forward.py", "m1b_frozen_interface"),
        (ROOT / "modeling/run_nested_walk_forward.py", "accepted_m1c_runner"),
        (ROOT / "portfolio/event_review_adjudication.py", "existing_e1_verifier"),
        (ROOT / "tests/portfolio/test_build_us_free_product.py", "us1a_focused_tests"),
    ]
    return [_repo_record(path, role) for path, role in paths]


def _function_lineage() -> dict[str, str]:
    return {
        function.__name__: hashlib.sha256(inspect.getsource(function).encode()).hexdigest()
        for function in (
            build_candidate_frame,
            evaluate_candidate_wide_liquidity,
            materialize_portfolios,
        )
    }


def _frozen_lineage_payload() -> dict[str, Any]:
    manifests = {
        "p2": json.loads((P2_ROOT / "manifest.json").read_text()),
        "m1c": json.loads((M1C_ROOT / "manifest.json").read_text()),
        "p4": json.loads((P4_ROOT / "manifest.json").read_text()),
    }
    return {
        "complete_frozen_manifests_copied_under_inputs": {
            "p2": P2_MANIFEST_SHA256,
            "m1c": M1C_MANIFEST_SHA256,
            "p4": P4_MANIFEST_SHA256,
        },
        "frozen_code_lineage": {
            name: manifest.get("code_lineage", [])
            for name, manifest in manifests.items()
        },
        "frozen_validated_inputs": {
            name: manifest.get("validated_inputs", manifest.get("inputs", {}))
            for name, manifest in manifests.items()
        },
        "current_code_lineage": _current_code_lineage(),
        "unchanged_p4_function_source_sha256": _function_lineage(),
        "route_selection": {
            "source": "M1A/M1C predeclared inner-fold evidence only",
            "m1d_performance_used": False,
            "outer_oos_metric_used": False,
            "b1e_performance_used": False,
        },
    }


def _product_contract(
    *,
    version: str,
    baseline: Mapping[str, Any],
    boundary_audit: Mapping[str, Any],
) -> dict[str, Any]:
    if baseline["selected_baseline"] != "accepted_P2":
        raise USFreeProductContractError("US1A baseline must be accepted P2")
    return {
        "schema_version": 1,
        "artifact_class": ARTIFACT_CLASS,
        "version": version,
        "contract_frozen_at_utc": _version_timestamp(version),
        "freeze_sequence": "baseline and route frozen before any US1A score, shortlist, or portfolio materialization",
        "data_baseline": {
            "selection": "accepted_P2",
            "manifest_sha256": P2_MANIFEST_SHA256,
            "d1_status": baseline["d1_release_status"],
            "decision_used_only_predeclared_data_gates": True,
            "downstream_output_observed_for_decision": False,
        },
        "model_route": {
            "selection": "accepted_M1A_M1C_inner_evidence_route",
            "m1a_manifest_sha256": M1A_MANIFEST_SHA256,
            "m1c_manifest_sha256": M1C_MANIFEST_SHA256,
            "winner_count": 16,
            "model_roles": list(MODEL_ROLES),
            "retuned": False,
            "retrained": False,
            "m1d_performance_used_for_selection": False,
        },
        "product_rules": {
            "source": "unchanged P4",
            "p4_manifest_sha256": P4_MANIFEST_SHA256,
            "hard_gates": list(HARD_GATE_NAMES),
            "tree_threshold": TREE_THRESHOLD,
            "minimum_adtv_usd": MIN_ADTV,
            "liquidity_window_sessions": 30,
            "portfolio_size": TARGET_N,
            "weight_each": WEIGHT,
            "rank": "descending M1C LightGBM score after fixed gates; stable_row_id tie-breaker",
        },
        "event_evidence": {
            "source_manifest_sha256": E1_MANIFEST_SHA256,
            "mapping_rule": "exact stable_row_id, ticker, SEC CIK, and decision timestamp only",
            "uncovered_status": "event_evidence_not_collected",
            "external_collection": False,
        },
        "claim": {
            "offline_product_candidate": True,
            "model_executed": False,
            "performance_calculated": False,
            "performance_rerun": False,
            "external_data_collected": False,
            "preserved_artifact_overwritten": False,
            "promotion": False,
            "publication": False,
        },
        "prohibited_actions": list(PROHIBITED_PRODUCT_ACTIONS),
        "boundary_audit_status": boundary_audit["status"],
    }


def _add_explanations(holdings: pd.DataFrame) -> pd.DataFrame:
    frame = holdings.copy()
    tree_values: list[str] = []
    ranker_values: list[str] = []
    explanations: list[str] = []
    for _, row in frame.iterrows():
        tree_values.append(
            _selected_feature_values(row, row["decision_tree_selected_features_json"])
        )
        ranker_values.append(
            _selected_feature_values(
                row, row["lightgbm_regression_selected_features_json"]
            )
        )
        explanations.append(
            "Passed all eight fixed decision-time hard gates; "
            f"M1C tree score {row['decision_tree_prediction']:.4f} met the "
            f"{TREE_THRESHOLD:.2f} agreement gate; M1C LightGBM score "
            f"{row['lightgbm_regression_prediction']:.4f} set rank "
            f"{int(row['rank'])}; median pre-decision 30-session dollar "
            f"volume was ${row['median_30_session_dollar_volume']:,.0f}. "
            "Scores are frozen research outputs, not forecasts or future-return promises."
        )
    frame["decision_tree_selected_feature_values_json"] = tree_values
    frame["lightgbm_selected_feature_values_json"] = ranker_values
    frame["explanation"] = explanations
    frame["accepted_p2_manifest_sha256"] = P2_MANIFEST_SHA256
    frame["accepted_m1c_manifest_sha256"] = M1C_MANIFEST_SHA256
    frame["unchanged_p4_manifest_sha256"] = P4_MANIFEST_SHA256
    return frame


def _map_event_evidence(shortlist: pd.DataFrame) -> pd.DataFrame:
    verification = verify_event_review_adjudication_artifact(E1_ROOT)
    if verification["manifest_sha256"] != E1_MANIFEST_SHA256:
        raise USFreeProductContractError("E1 adjudication manifest drifted")
    names = pd.read_parquet(E1_ROOT / "outputs/live/name_level_adjudication.parquet")
    by_id = names.set_index("stable_row_id", drop=False)
    rows: list[dict[str, Any]] = []
    for row in shortlist.sort_values("rank").itertuples(index=False):
        stable_id = str(row.stable_row_id)
        if stable_id in by_id.index:
            evidence = by_id.loc[stable_id]
            if isinstance(evidence, pd.DataFrame):
                raise USFreeProductContractError("E1 stable row is duplicated")
            decision_match = pd.Timestamp(evidence["decision_timestamp"]) == pd.Timestamp(
                row.decision_timestamp
            )
            identity_match = (
                str(evidence["ticker"]) == str(row.ticker)
                and str(evidence["sec_cik"]).zfill(10) == str(row.cik).zfill(10)
                and decision_match
            )
            if not identity_match:
                raise USFreeProductContractError(f"E1 identity mismatch: {stable_id}")
            rows.append(
                {
                    "stable_row_id": stable_id,
                    "rank": int(row.rank),
                    "ticker": str(row.ticker),
                    "cik": str(row.cik).zfill(10),
                    "decision_timestamp": pd.Timestamp(row.decision_timestamp),
                    "event_evidence_status": "existing_e1_exact_lineage_unresolved",
                    "event_evidence_collected": True,
                    "event_evidence_exact_identity_match": True,
                    "e1_requirement_id": evidence["requirement_id"],
                    "e1_deterministic_action": evidence["deterministic_action"],
                    "e1_deterministic_reason": evidence["deterministic_reason"],
                    "e1_human_review_required": bool(evidence["human_review_required"]),
                    "e1_summary_status": evidence["summary_status"],
                    "e1_cited_claim_ids": evidence["cited_claim_ids"],
                    "e1_remaining_ambiguity": evidence["remaining_ambiguity"],
                    "e1_manifest_sha256": E1_MANIFEST_SHA256,
                }
            )
        else:
            rows.append(
                {
                    "stable_row_id": stable_id,
                    "rank": int(row.rank),
                    "ticker": str(row.ticker),
                    "cik": str(row.cik).zfill(10),
                    "decision_timestamp": pd.Timestamp(row.decision_timestamp),
                    "event_evidence_status": "event_evidence_not_collected",
                    "event_evidence_collected": False,
                    "event_evidence_exact_identity_match": False,
                    "e1_requirement_id": None,
                    "e1_deterministic_action": None,
                    "e1_deterministic_reason": (
                        "no exact stable-row/security lineage exists in the frozen E1 adjudication"
                    ),
                    "e1_human_review_required": True,
                    "e1_summary_status": "not_available_no_exact_e1_row",
                    "e1_cited_claim_ids": None,
                    "e1_remaining_ambiguity": "event evidence was not collected for this US1A name",
                    "e1_manifest_sha256": E1_MANIFEST_SHA256,
                }
            )
    result = pd.DataFrame(rows).sort_values("rank").reset_index(drop=True)
    if len(result) != TARGET_N or result["stable_row_id"].duplicated().any():
        raise USFreeProductContractError("event evidence mapping is not 15-row complete")
    return result


def _attach_stock_limitations(shortlist: pd.DataFrame) -> pd.DataFrame:
    frame = shortlist.copy()
    frame["data_limitation"] = (
        "accepted P2 is frozen at its recorded source clock; D1 is unsupported for this route"
    )
    frame["model_limitation"] = (
        "M1C score is an inner-selected research score and is not a calibrated future-return promise"
    )
    frame["liquidity_limitation"] = (
        "30-session frozen Yahoo close-volume evidence supports only the pre-decision P4 liquidity gate"
    )
    frame["identity_limitation"] = (
        "free-source identity/action coverage is not provider-certified"
    )
    frame["event_limitation"] = np.where(
        frame["event_evidence_collected"],
        "existing exact-lineage E1 evidence remains deterministically unresolved",
        "event_evidence_not_collected",
    )
    frame["survivorship_limitation"] = (
        "historically enriched population is not comprehensively survivorship-free"
    )
    frame["performance_limitation"] = (
        "US1A calculated no historical performance; no performance claim transfers from M1D or B1E"
    )
    return frame


def _diagnostics(
    portfolio: Mapping[str, Any],
    shortlist: pd.DataFrame,
    event_mapping: pd.DataFrame,
) -> dict[str, Any]:
    scores = portfolio["m1c_lineage"]
    candidates = portfolio["candidates"]
    gates = portfolio["gates"]
    liquidity = portfolio["liquidity_coverage"]
    holdings = portfolio["holdings"]
    return {
        "status": "pass",
        "score_row_roles": len(scores),
        "score_source_rows": int(scores["stable_row_id"].nunique()),
        "candidate_rows": len(candidates),
        "gate_rows": len(gates),
        "exclusion_rows": len(portfolio["exclusions"]),
        "liquidity_required_rows": len(liquidity),
        "liquidity_pass_rows": int(liquidity["liquidity_pass"].sum()),
        "liquidity_evidence_rows": len(portfolio["liquidity_evidence"]),
        "holding_rows_2019_2026": len(holdings),
        "supported_decision_periods": int(portfolio["periods"]["period_supported"].sum()),
        "latest_decision_timestamp": pd.Timestamp(shortlist["decision_timestamp"].iloc[0]),
        "latest_shortlist_rows": len(shortlist),
        "latest_weight_sum": float(shortlist["weight"].sum()),
        "event_evidence_exact_lineage_rows": int(event_mapping["event_evidence_collected"].sum()),
        "event_evidence_not_collected_rows": int((~event_mapping["event_evidence_collected"]).sum()),
        "performance_calculated": False,
        "external_data_collected": False,
    }


def _render_methodology(contract: Mapping[str, Any], baseline: Mapping[str, Any]) -> str:
    failed = [name for name, gate in baseline["gates"].items() if gate["status"] == "fail"]
    return f"""# US1A Methodology

The product contract was frozen before any US1A score, shortlist, or portfolio output. The accepted P2 baseline remains selected because D1 failed the exact frozen-route gates `{', '.join(failed)}`. No model score, shortlist membership, portfolio result, or performance value entered that decision.

The model route is the exact accepted M1A/M1C route selected only from predeclared inner-fold evidence. No feature, winner, grid, seed, selector, threshold, gate, liquidity rule, portfolio size, weight, cost, or tie-breaker changed. The unchanged P4 functions construct the row-complete gates, candidate-wide liquidity evidence, and 15-name equal-weight portfolio.

Existing E1 event evidence is attached only when stable row ID, ticker, SEC CIK, and decision timestamp all match. Other names receive the explicit status `event_evidence_not_collected`; no source was queried and no event was inferred.

This artifact does not calculate or reproduce historical performance. It is a free-source research shortlist, not personalized investment advice or a future-performance promise.
"""


def _fmt_number(value: Any, digits: int = 4) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "n/a" if pd.isna(numeric) else f"{float(numeric):.{digits}f}"


def render_product_report(summary: Mapping[str, Any], shortlist: pd.DataFrame) -> str:
    rows = []
    for row in shortlist.sort_values("rank").itertuples(index=False):
        rows.append(
            "| "
            f"{int(row.rank)} | {row.ticker} | "
            f"{_fmt_number(row.lightgbm_regression_prediction)} | "
            f"{_fmt_number(row.decision_tree_prediction)} | "
            f"${float(row.median_30_session_dollar_volume):,.0f} | "
            f"{row.event_evidence_status} |"
        )
    diagnostics = summary["diagnostics"]
    return f"""# US Free-Data Product Candidate — US1A

## Frozen contract

- Data baseline: **accepted P2** (`{P2_MANIFEST_SHA256}`). D1 is **unsupported for this release candidate** because its stable-row population and manifest cannot be consumed by frozen M1C without retraining or a contract change.
- Model route: accepted M1A/M1C inner-evidence winners only (`{M1C_MANIFEST_SHA256}`).
- Product rules: unchanged P4 hard gates, {TREE_THRESHOLD:.2f} tree threshold, 30-session candidate-wide liquidity, 15 names, equal weight, and stable-row-ID tie-breaker.
- External evidence collection: **none**. Historical performance calculation or rerun: **none**.

## 2026 shortlist

| Rank | Ticker | M1C LightGBM score | M1C tree score | Median 30-session dollar volume | Event evidence |
|---:|---|---:|---:|---:|---|
{chr(10).join(rows)}

All 15 rows passed the unchanged decision-time/model/liquidity gates and carry exact score, gate, selected-feature, identity, and liquidity lineage in `outputs/final_shortlist_2026.parquet`. The table is a frozen research ranking, not a forecast.

## Coverage and event evidence

- Row-complete scores: {diagnostics['score_row_roles']:,} row-role records across {diagnostics['score_source_rows']:,} accepted-P2 rows.
- Gates and exclusions: {diagnostics['gate_rows']:,} gate rows and {diagnostics['exclusion_rows']:,} explicit exclusion rows.
- Liquidity: {diagnostics['liquidity_required_rows']:,} required candidates, {diagnostics['liquidity_pass_rows']:,} passes, and {diagnostics['liquidity_evidence_rows']:,} exact candidate-session evidence rows.
- Existing E1 exact-lineage coverage: {diagnostics['event_evidence_exact_lineage_rows']}/15 names; each remains deterministically unresolved.
- Missing event evidence: {diagnostics['event_evidence_not_collected_rows']}/15 names with explicit `event_evidence_not_collected` status.

## Limitations

- Data: accepted P2 is frozen at its recorded source clock; D1 was not promoted.
- Model: M1C scores are research outputs selected from nested predictive evidence, not calibrated future-return promises.
- Liquidity: frozen Yahoo close-volume evidence supports only the pre-decision P4 liquidity gate and is not promoted to certified action/performance evidence.
- Identity and events: free-source security/action identity is not provider-certified; exact-lineage E1 rows remain unresolved, and uncovered names have no collected event evidence.
- Survivorship: the population is historically enriched, not comprehensively survivorship-free.
- Performance: US1A calculated no CAGR, Sharpe, NAV, drawdown, turnover, scenario, or other historical-performance result. No B1E or M1D value transfers into this product claim.

This is a free-source research shortlist, not personalized investment advice, provider-certified performance, or a promise of future returns.
"""


def _record_role(path: Path, root: Path) -> str:
    relative = path.relative_to(root).as_posix()
    if relative.startswith("outputs/"):
        return "row_complete_product_output"
    if relative.startswith("configuration/"):
        return "frozen_product_contract"
    if relative.startswith("support/"):
        return "preflight_diagnostic_or_verification"
    if relative.startswith("lineage/"):
        return "source_code_or_row_lineage"
    if relative.startswith("inputs/"):
        return "exact_frozen_input_manifest_copy"
    if relative.startswith("report/"):
        return "human_readable_product_report"
    return "methodology_or_rebuild_instruction"


def build(artifact_root: Path = DEFAULT_ARTIFACT_ROOT) -> Path:
    """Materialize one fresh, contract-first, non-overwriting US1A artifact."""
    artifact_root = artifact_root.resolve()
    version = artifact_root.name
    _version_timestamp(version)
    if artifact_root.exists():
        raise FileExistsError(f"US1A artifact target already exists: {artifact_root}")

    baseline = assess_data_baseline()
    boundaries = audit_preserved_boundaries()
    contract = _product_contract(
        version=version,
        baseline=baseline,
        boundary_audit=boundaries,
    )

    for folder in ("configuration", "inputs", "lineage", "outputs", "report", "support"):
        (artifact_root / folder).mkdir(parents=True, exist_ok=False)
    _write_json(artifact_root / "configuration/product_contract.json", contract)
    _write_json(artifact_root / "support/baseline_gate_assessment.json", baseline)
    _write_json(artifact_root / "support/preserved_boundary_preflight.json", boundaries)
    _write_json(
        artifact_root / "support/contract_freeze.json",
        {
            "status": "frozen_before_downstream_materialization",
            "contract_sha256": sha256_file(
                artifact_root / "configuration/product_contract.json"
            ),
            "downstream_output_observed": False,
            "performance_output_used": False,
        },
    )

    input_manifests = {
        "p2_manifest.json": P2_ROOT / "manifest.json",
        "m1a_manifest.json": M1A_ROOT / "manifest.json",
        "m1c_manifest.json": M1C_ROOT / "manifest.json",
        "p4_manifest.json": P4_ROOT / "manifest.json",
        "d1_review_candidate_manifest.json": D1_P2_ROOT / "manifest.json",
        "e1_adjudication_manifest.json": E1_ROOT / "manifest.json",
    }
    for name, source in input_manifests.items():
        shutil.copy2(source, artifact_root / "inputs" / name)
    _write_json(artifact_root / "lineage/source_code_lineage.json", _frozen_lineage_payload())

    portfolio = _build_locked_portfolio()
    explained_holdings = _add_explanations(portfolio["holdings"])
    latest_mask = pd.to_datetime(
        explained_holdings["decision_timestamp"], utc=True
    ).dt.year.eq(2026)
    shortlist = explained_holdings.loc[latest_mask].sort_values("rank").reset_index(drop=True)
    if (
        len(shortlist) != TARGET_N
        or list(pd.to_numeric(shortlist["rank"]).astype(int)) != list(range(1, TARGET_N + 1))
        or not np.isclose(shortlist["weight"].sum(), 1.0)
        or not np.allclose(shortlist["weight"], WEIGHT)
    ):
        raise USFreeProductContractError("2026 shortlist contract failed")
    event_mapping = _map_event_evidence(shortlist)
    shortlist = shortlist.merge(
        event_mapping,
        on=["stable_row_id", "rank", "ticker", "cik", "decision_timestamp"],
        validate="one_to_one",
    )
    shortlist = _attach_stock_limitations(shortlist)

    trace = portfolio["predictions"].loc[
        portfolio["predictions"]["stable_row_id"].isin(shortlist["stable_row_id"])
    ].merge(
        shortlist[["stable_row_id", "rank", "event_evidence_status", "explanation"]],
        on="stable_row_id",
        validate="many_to_one",
    )
    if len(trace) != TARGET_N * len(MODEL_ROLES):
        raise USFreeProductContractError("shortlist row-role traceability failed")

    output_frames = {
        "outputs/row_complete_scores.parquet": portfolio["m1c_lineage"],
        "outputs/p4_consumption_predictions.parquet": portfolio["predictions"],
        "outputs/candidates.parquet": portfolio["candidates"],
        "outputs/gates.parquet": portfolio["gates"],
        "outputs/exclusions.parquet": portfolio["exclusions"],
        "outputs/liquidity_evidence.parquet": portfolio["liquidity_evidence"],
        "outputs/liquidity_coverage.parquet": portfolio["liquidity_coverage"],
        "outputs/liquidity_source_lineage.parquet": portfolio["liquidity_lineage"],
        "outputs/holdings_2019_2026.parquet": explained_holdings,
        "outputs/final_shortlist_2026.parquet": shortlist,
        "outputs/event_evidence_mapping.parquet": event_mapping,
        "outputs/report_traceability.parquet": trace,
        "support/period_coverage.parquet": portfolio["periods"],
    }
    for relative, frame in output_frames.items():
        _write_parquet(artifact_root / relative, frame)

    validation_payloads = {
        "support/prediction_validation.json": portfolio["prediction_validation"],
        "support/gate_validation.json": portfolio["gate_validation"],
        "support/liquidity_validation.json": portfolio["liquidity_validation"],
        "support/portfolio_validation.json": portfolio["portfolio_validation"],
    }
    for relative, payload in validation_payloads.items():
        _write_json(artifact_root / relative, payload)

    diagnostics = _diagnostics(portfolio, shortlist, event_mapping)
    summary = {
        "artifact_class": ARTIFACT_CLASS,
        "version": version,
        "selected_data_baseline": "accepted_P2",
        "selected_model_route": "accepted_M1A_M1C_inner_evidence_route",
        "product_rules": "unchanged_P4",
        "diagnostics": diagnostics,
        "unsupported_requirements": [
            "D1 consumption without retraining or a contract change",
            "event evidence for names without an exact E1 stable-row/security match",
            "provider-certified security/action identity",
            "comprehensively survivorship-free coverage",
            "historical performance or future-performance claims",
        ],
    }
    _write_json(artifact_root / "support/summary.json", summary)
    (artifact_root / "methodology.md").write_text(_render_methodology(contract, baseline))
    report = render_product_report(summary, shortlist)
    (artifact_root / "report/product_report.md").write_text(report)
    _write_json(
        artifact_root / "rebuild_or_verify.json",
        {
            "deterministic_command": (
                "python3 -m portfolio.build_us_free_product "
                f"--artifact-root artifacts/product/us_free_v1/{version} --verify-only"
            ),
            "mode": "offline_verify_existing_artifact",
            "external_data_collected": False,
            "performance_calculated": False,
        },
    )

    artifact_files = [
        path
        for path in sorted(artifact_root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    ]
    records = [
        _artifact_record(artifact_root, path, _record_role(path, artifact_root))
        for path in artifact_files
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": ARTIFACT_CLASS,
        "version": version,
        "created_at_utc": _version_timestamp(version),
        "claim": contract["claim"],
        "configuration_sha256": sha256_file(
            artifact_root / "configuration/product_contract.json"
        ),
        "selected_data_baseline": "accepted_P2",
        "selected_data_manifest_sha256": P2_MANIFEST_SHA256,
        "selected_model_manifest_sha256": M1C_MANIFEST_SHA256,
        "unchanged_product_manifest_sha256": P4_MANIFEST_SHA256,
        "event_evidence_manifest_sha256": E1_MANIFEST_SHA256,
        "validated_inputs": [
            _repo_record(P2_ROOT / "manifest.json", "accepted_p2_manifest"),
            _repo_record(M1A_ROOT / "manifest.json", "frozen_m1a_manifest"),
            _repo_record(M1C_ROOT / "manifest.json", "accepted_m1c_manifest"),
            _repo_record(P4_ROOT / "manifest.json", "unchanged_p4_manifest"),
            _repo_record(D1_P2_ROOT / "manifest.json", "reviewed_unsupported_d1_manifest"),
            _repo_record(E1_ROOT / "manifest.json", "existing_e1_adjudication_manifest"),
        ],
        "code_lineage": _current_code_lineage(),
        "outputs": diagnostics,
        "records": records,
        "limitations": summary["unsupported_requirements"],
    }
    _write_json(artifact_root / "manifest.json", manifest)
    verify_product_artifact(artifact_root)
    return artifact_root / "manifest.json"


def verify_product_artifact(
    artifact_root: Path,
    *,
    expected_manifest_sha256: str | None = None,
    reverify_preserved_boundaries: bool = True,
) -> dict[str, Any]:
    """Independently verify all US1A records and row-level contracts."""
    artifact_root = artifact_root.resolve()
    manifest_path = artifact_root / "manifest.json"
    if not manifest_path.is_file():
        raise USFreeProductContractError("US1A manifest is missing")
    actual_manifest_sha256 = sha256_file(manifest_path)
    if expected_manifest_sha256 and actual_manifest_sha256 != expected_manifest_sha256:
        raise USFreeProductContractError("US1A manifest hash mismatch")
    manifest = json.loads(manifest_path.read_text())
    if (
        manifest.get("artifact_class") != ARTIFACT_CLASS
        or manifest.get("version") != artifact_root.name
        or manifest.get("claim", {}).get("performance_calculated") is not False
        or manifest.get("claim", {}).get("external_data_collected") is not False
    ):
        raise USFreeProductContractError("US1A manifest identity or claim drifted")
    records = manifest.get("records", [])
    summary = _verify_record_rows(artifact_root, records)
    discovered = {
        path.relative_to(artifact_root).as_posix()
        for path in artifact_root.rglob("*")
        if path.is_file() and path != manifest_path
    }
    if discovered != {str(item["path"]) for item in records}:
        raise USFreeProductContractError("US1A manifest does not enumerate every record")

    contract_path = artifact_root / "configuration/product_contract.json"
    contract = json.loads(contract_path.read_text())
    if (
        sha256_file(contract_path) != manifest["configuration_sha256"]
        or contract["data_baseline"]["selection"] != "accepted_P2"
        or contract["model_route"]["selection"]
        != "accepted_M1A_M1C_inner_evidence_route"
        or contract["model_route"]["m1d_performance_used_for_selection"] is not False
    ):
        raise USFreeProductContractError("US1A frozen contract drifted")

    scores = pd.read_parquet(artifact_root / "outputs/row_complete_scores.parquet")
    candidates = pd.read_parquet(artifact_root / "outputs/candidates.parquet")
    gates = pd.read_parquet(artifact_root / "outputs/gates.parquet")
    liquidity = pd.read_parquet(artifact_root / "outputs/liquidity_coverage.parquet")
    evidence = pd.read_parquet(artifact_root / "outputs/liquidity_evidence.parquet")
    holdings = pd.read_parquet(artifact_root / "outputs/holdings_2019_2026.parquet")
    shortlist = pd.read_parquet(artifact_root / "outputs/final_shortlist_2026.parquet")
    events = pd.read_parquet(artifact_root / "outputs/event_evidence_mapping.parquet")
    trace = pd.read_parquet(artifact_root / "outputs/report_traceability.parquet")
    if (
        len(scores) != 87_612
        or scores.duplicated(["stable_row_id", "model_role"]).any()
        or len(candidates) != 43_806
        or candidates["stable_row_id"].duplicated().any()
        or len(gates) != 525_672
        or len(liquidity) != 1_477
        or len(evidence) != 44_310
        or len(holdings) != 120
        or len(shortlist) != TARGET_N
        or len(events) != TARGET_N
        or len(trace) != TARGET_N * len(MODEL_ROLES)
    ):
        raise USFreeProductContractError("US1A row-complete output counts drifted")
    shortlist_year = pd.to_datetime(shortlist["decision_timestamp"], utc=True).dt.year
    if (
        not shortlist_year.eq(2026).all()
        or list(pd.to_numeric(shortlist["rank"]).astype(int)) != list(range(1, 16))
        or not np.allclose(shortlist["weight"], WEIGHT)
        or not np.isclose(shortlist["weight"].sum(), 1.0)
        or int(shortlist["event_evidence_collected"].sum()) != 5
        or int((~shortlist["event_evidence_collected"]).sum()) != 10
        or not shortlist.loc[
            ~shortlist["event_evidence_collected"], "event_evidence_status"
        ].eq("event_evidence_not_collected").all()
    ):
        raise USFreeProductContractError("US1A shortlist or event coverage drifted")
    report = (artifact_root / "report/product_report.md").read_text()
    structured = json.loads((artifact_root / "support/summary.json").read_text())
    if report != render_product_report(structured, shortlist):
        raise USFreeProductContractError("US1A report does not reproduce")
    if reverify_preserved_boundaries:
        audit = audit_preserved_boundaries()
        if audit["status"] != "pass":
            raise USFreeProductContractError("preserved boundary rehash failed")
    return {
        "manifest_sha256": actual_manifest_sha256,
        **summary,
        "score_row_roles": len(scores),
        "candidate_rows": len(candidates),
        "gate_rows": len(gates),
        "liquidity_required_rows": len(liquidity),
        "liquidity_evidence_rows": len(evidence),
        "holding_rows": len(holdings),
        "shortlist_rows": len(shortlist),
        "event_evidence_exact_lineage_rows": int(shortlist["event_evidence_collected"].sum()),
        "event_evidence_not_collected_rows": int((~shortlist["event_evidence_collected"]).sum()),
        "performance_calculated": False,
        "external_data_collected": False,
        "preserved_boundaries_reverified": reverify_preserved_boundaries,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--expected-manifest-sha256")
    args = parser.parse_args()
    if args.verify_only:
        result = verify_product_artifact(
            args.artifact_root,
            expected_manifest_sha256=args.expected_manifest_sha256,
        )
    else:
        if args.expected_manifest_sha256:
            parser.error("--expected-manifest-sha256 requires --verify-only")
        manifest = build(args.artifact_root)
        result = {
            "artifact_root": str(args.artifact_root),
            "manifest_sha256": sha256_file(manifest),
            "performance_calculated": False,
            "external_data_collected": False,
        }
    print(json.dumps(_json_value(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
