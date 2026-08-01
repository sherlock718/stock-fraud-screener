"""One-shot M1D portfolio comparison for the frozen M1C route.

M1D is deliberately split into two irreversible phases:

1. ``prepare_m1d_lock`` independently verifies every frozen boundary and the
   focused-test report, then persists the exact M1C inner-selected route plus
   the unchanged P4/B1D/B1E contract before any portfolio is constructed.
2. ``execute_locked_m1d`` verifies that immutable lock and performs one
   portfolio/evaluation materialization.  A started root cannot be resumed or
   retried by this module.

The module never fits a model, changes an M1C winner, calls an external
service, substitutes DGS1MO, or promotes an open 2024-2026 observation into a
completed outcome.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import inspect
import json
import platform
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any, Iterable, Mapping
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd

from backtest.free_data_v1_evidence import (
    BENCHMARK_SYMBOLS,
    DEFAULT_INPUTS,
    _calendar_frames,
    _latest_response_records,
    _namespace_contracts,
    _validate_normalized_symbol,
    performance_contract,
    verify_frozen_inputs,
)
from backtest.free_data_v1_nav import (
    DGS1MO_NAMESPACE,
    OUTCOME_NAMESPACES,
    PERFORMANCE_NAMESPACES,
    ZERO_RATE_NAMESPACE,
    EvidenceBundle,
    EvidenceValidationError,
    PerformanceResult,
    RiskFreeUnavailableError,
    load_frozen_b1c_evidence,
    run_performance_engine,
    sha256_file,
    validate_evidence_bundle,
)
from backtest.free_data_v1_performance import (
    _benchmark_metrics,
    _coverage_ledger,
    _event_and_scenario_ledgers,
    _metric_rows_for_scope,
    _outcome_treatment,
    _used_outcomes,
)
from portfolio.build_canonical_product import (
    AVAILABLE_PREDICTION,
    HARD_GATE_NAMES,
    MODEL_LINEAGE_COLUMNS,
    MODEL_ROLES,
    P2_FEATURES,
    P2_FEATURES_SHA256,
    P3_PREDICTIONS,
    SOURCE_LINEAGE_COLUMNS,
    _load_market_indexes,
    build_backtest_status,
    build_candidate_frame,
    evaluate_candidate_wide_liquidity,
    materialize_portfolios,
)
from portfolio.selection_contract import MIN_ADTV, TARGET_N, TREE_THRESHOLD, WEIGHT


ROOT = Path(__file__).resolve().parents[1]
M1A_ROOT = ROOT / "artifacts/modeling/nested_walk_forward/20260801T000000Z-m1a"
M1C_ROOT = ROOT / "artifacts/modeling/nested_walk_forward/20260801T121426Z-m1c"
P2_ROOT = ROOT / "artifacts/canonical/corrected_us_annual"
P3_ROOT = ROOT / "artifacts/canonical/corrected_us_annual_3y_research_model"
P4_ROOT = ROOT / "artifacts/canonical/corrected_us_annual_3y_product"
B1C_ROOT = (
    ROOT / "artifacts/performance_inputs/free_data_v1/20260731T115106Z-b1c"
)
B1E_ROOT = ROOT / "artifacts/performance/free_data_v1/20260801T011135Z-b1e"

P2_MANIFEST_SHA256 = (
    "40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3"
)
P3_MANIFEST_SHA256 = (
    "8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2"
)
P4_MANIFEST_SHA256 = (
    "28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7"
)
B1C_MANIFEST_SHA256 = (
    "98635ab48c5f381a0145cc6ab99ff76e072bc24f3c2b04a669edb80371ee71df"
)
B1D_ENGINE_SHA256 = (
    "880cf13607851dcc1d0947ef7b62ecc798e1add841a707b85a80535cd74d034f"
)
B1D_TEST_SHA256 = (
    "c9cb5b123b7450498a44938c64ebd166164df8a6216764a110e1a95217dee86f"
)
B1E_MANIFEST_SHA256 = (
    "23588207812e2e950d3e521c6f2048e7607f20b99b691407c449ceb7752bf37c"
)
M1A_MANIFEST_SHA256 = (
    "a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc"
)
M1C_MANIFEST_SHA256 = (
    "125cb5d6a8012c3b03ee6eab5f00ac944135c39996e992ecf40d8816acdacc58"
)

ARTIFACT_CLASS = "LOCKED_M1C_P4_B1E_PORTFOLIO_COMPARISON_M1D"
VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*-m1d")
M1D_SCORE_STATUSES = {
    "oos_prediction_available",
    "oos_score_label_unavailable",
    "production_score_open_unlabeled",
}
MATURED_YEARS = tuple(range(2019, 2024))
OPEN_YEARS = (2024, 2025, 2026)
THRESHOLD_NET_CAGR = 0.30
THRESHOLD_ZERO_RATE_SHARPE = 1.0
PRIMARY_NAMESPACE = "best_free_evidence_full_accounting"

FROZEN_BOUNDARIES = {
    "p2": (P2_ROOT / "manifest.json", P2_MANIFEST_SHA256),
    "p3": (P3_ROOT / "manifest.json", P3_MANIFEST_SHA256),
    "p4": (P4_ROOT / "manifest.json", P4_MANIFEST_SHA256),
    "b1c": (B1C_ROOT / "manifest.json", B1C_MANIFEST_SHA256),
    "b1d_engine": (ROOT / "backtest/free_data_v1_nav.py", B1D_ENGINE_SHA256),
    "b1d_test": (ROOT / "tests/backtest/test_free_data_v1_nav.py", B1D_TEST_SHA256),
    "b1e": (B1E_ROOT / "manifest.json", B1E_MANIFEST_SHA256),
    "m1a": (M1A_ROOT / "manifest.json", M1A_MANIFEST_SHA256),
    "m1c": (M1C_ROOT / "manifest.json", M1C_MANIFEST_SHA256),
}


class M1DContractError(EvidenceValidationError):
    """Raised when the locked M1D boundary cannot be proven exactly."""


@dataclass(frozen=True)
class LockedM1D:
    root: Path
    lock_manifest_sha256: str
    preflight: Mapping[str, Any]
    route: Mapping[str, Any]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
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
    path.write_text(
        json.dumps(
            _json_value(payload), indent=2, sort_keys=True, allow_nan=False
        )
        + "\n"
    )


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _record(root: Path, path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _hash_payload(payload: Any) -> str:
    encoded = json.dumps(
        _json_value(payload), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _git_head() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _safe_relative(root: Path, relative: Any) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise M1DContractError(f"unsafe manifest path: {relative!r}")
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise M1DContractError(f"manifest path escapes root: {relative}") from exc
    return path


def _verify_records(root: Path, records: Any, label: str) -> dict[str, Any]:
    if not isinstance(records, list) or not records:
        raise M1DContractError(f"{label} records are absent")
    seen: set[str] = set()
    total = 0
    for item in records:
        relative = item.get("path") if isinstance(item, Mapping) else None
        if not relative or relative in seen:
            raise M1DContractError(f"{label} record paths are invalid")
        seen.add(str(relative))
        path = _safe_relative(root, relative)
        if not path.is_file():
            raise M1DContractError(f"{label} record is missing: {relative}")
        if path.stat().st_size != int(item.get("size_bytes", -1)):
            raise M1DContractError(f"{label} record size mismatch: {relative}")
        if sha256_file(path) != item.get("sha256"):
            raise M1DContractError(f"{label} record hash mismatch: {relative}")
        total += path.stat().st_size
    return {"record_count": len(records), "record_bytes": total}


def _verify_boundary_hashes() -> dict[str, Any]:
    rows = []
    for name, (path, expected) in FROZEN_BOUNDARIES.items():
        if not path.is_file():
            raise M1DContractError(f"frozen boundary missing: {name}")
        actual = sha256_file(path)
        if actual != expected:
            raise M1DContractError(
                f"frozen boundary hash mismatch: {name} expected={expected} actual={actual}"
            )
        rows.append(
            {
                "name": name,
                "path": path.relative_to(ROOT).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": actual,
            }
        )
    return {"status": "pass", "boundaries": rows}


def _verify_m1c_complete() -> tuple[dict[str, Any], dict[str, Any]]:
    manifest_path = M1C_ROOT / "manifest.json"
    if sha256_file(manifest_path) != M1C_MANIFEST_SHA256:
        raise M1DContractError("accepted M1C manifest hash mismatch")
    manifest = json.loads(manifest_path.read_text())
    record_summary = _verify_records(M1C_ROOT, manifest.get("records"), "M1C")
    for item in manifest.get("code_lineage", []):
        path = ROOT / str(item.get("path"))
        if (
            not path.is_file()
            or path.stat().st_size != item.get("size_bytes")
            or sha256_file(path) != item.get("sha256")
        ):
            raise M1DContractError(f"M1C code lineage drifted: {item.get('path')}")
    p3_contract = manifest.get("inputs", {}).get("p3_contract", {})
    p3_contract_path = ROOT / str(p3_contract.get("path"))
    if (
        not p3_contract_path.is_file()
        or p3_contract_path.stat().st_size != p3_contract.get("size_bytes")
        or sha256_file(p3_contract_path) != p3_contract.get("sha256")
    ):
        raise M1DContractError("M1C referenced P3 contract drifted")
    expected_inputs = {
        "p2_manifest_sha256": P2_MANIFEST_SHA256,
        "p3_manifest_sha256": P3_MANIFEST_SHA256,
        "p4_manifest_sha256": P4_MANIFEST_SHA256,
        "b1d_engine_sha256": B1D_ENGINE_SHA256,
        "b1d_test_sha256": B1D_TEST_SHA256,
        "b1e_manifest_sha256": B1E_MANIFEST_SHA256,
        "m1a_manifest_sha256": M1A_MANIFEST_SHA256,
    }
    for key, expected in expected_inputs.items():
        if manifest.get("inputs", {}).get(key) != expected:
            raise M1DContractError(f"M1C frozen input drifted: {key}")
    claim = manifest.get("claim", {})
    if not (
        claim.get("single_controlled_execution") is True
        and claim.get("outer_oos_used_for_selection") is False
        and claim.get("b1e_performance_consumed") is False
        and claim.get("portfolio_constructed") is False
        and claim.get("performance_calculated") is False
        and claim.get("open_2024_2026_outside_predictive_metrics") is True
    ):
        raise M1DContractError("M1C accepted claim boundary drifted")
    return manifest, {
        **record_summary,
        "code_lineage_count": len(manifest.get("code_lineage", [])),
        "path_referenced_input_count": 1,
        "manifest_sha256": M1C_MANIFEST_SHA256,
        "claim_verified": True,
    }


def _verify_manifest_record_set(root: Path, expected_hash: str, label: str) -> dict[str, Any]:
    path = root / "manifest.json"
    if sha256_file(path) != expected_hash:
        raise M1DContractError(f"{label} manifest hash mismatch")
    manifest = json.loads(path.read_text())
    summary = _verify_records(root, manifest.get("records"), label)
    return {**summary, "manifest_sha256": expected_hash}


def _parse_test_report(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise M1DContractError("focused boundary JUnit report is missing")
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError) as exc:
        raise M1DContractError("focused boundary JUnit report is invalid") from exc
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    if not suites:
        raise M1DContractError("focused boundary JUnit report has no suites")
    totals = {
        name: sum(int(float(item.attrib.get(name, 0))) for item in suites)
        for name in ("tests", "failures", "errors", "skipped")
    }
    if totals["tests"] <= 0 or totals["failures"] or totals["errors"]:
        raise M1DContractError(f"focused boundary tests did not pass: {totals}")
    return {
        **totals,
        "passed": totals["tests"] - totals["skipped"],
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _source_sha(function: Any) -> str:
    return hashlib.sha256(inspect.getsource(function).encode()).hexdigest()


def _verify_contract_identity() -> dict[str, Any]:
    m1a_contract = json.loads((M1A_ROOT / "experiment_contract.json").read_text())
    if m1a_contract.get("execution_sequence", {}).get("m1d") != (
        "lock one M1C route before consuming unchanged P4 and B1D/B1E evaluation; "
        "evaluate once; never retune on the same outer history"
    ):
        raise M1DContractError("M1A M1D execution sequence drifted")
    prohibited = set(
        m1a_contract.get("selection_rule", {}).get("prohibited_selection_inputs", [])
    )
    required_prohibited = {
        "outer-OOS predictions or labels",
        "P4 holdings or gates",
        "B1D/B1E NAV or metrics",
        "CAGR",
        "Sharpe",
        "drawdown",
        "turnover",
        "scenario results",
    }
    if not required_prohibited.issubset(prohibited):
        raise M1DContractError("M1A prohibited selection inputs drifted")
    p4 = json.loads((P4_ROOT / "manifest.json").read_text())
    selection = p4.get("configuration", {}).get("selection", {})
    liquidity = p4.get("configuration", {}).get("liquidity", {})
    transaction = p4.get("backtest_status", {}).get("transaction_cost_policy", {})
    if not (
        selection.get("tree_threshold") == TREE_THRESHOLD
        and selection.get("target_n") == TARGET_N
        and np.isclose(selection.get("weight_each"), WEIGHT)
        and np.isclose(liquidity.get("minimum_adtv_usd"), MIN_ADTV)
        and transaction.get("rate_per_side") == 0.0025
        and transaction.get("half_turnover_multiplier") is False
    ):
        raise M1DContractError("P4 portfolio/cost contract drifted")
    frozen_contract = json.loads(
        (B1C_ROOT / "contracts/free_data_v1_performance_contract.json").read_text()
    )
    if frozen_contract != performance_contract():
        raise M1DContractError("B1D/B1E performance contract drifted")
    return {
        "m1a_execution_sequence_verified": True,
        "m1a_prohibited_selection_inputs_verified": sorted(required_prohibited),
        "p4_selection_contract": {
            "hard_gates": list(HARD_GATE_NAMES),
            "tree_threshold": TREE_THRESHOLD,
            "target_n": TARGET_N,
            "weight_each": WEIGHT,
            "minimum_adtv_usd": MIN_ADTV,
        },
        "b1d_b1e_performance_contract_sha256": sha256_file(
            B1C_ROOT / "contracts/free_data_v1_performance_contract.json"
        ),
        "p4_function_source_sha256": {
            "build_candidate_frame": _source_sha(build_candidate_frame),
            "evaluate_candidate_wide_liquidity": _source_sha(
                evaluate_candidate_wide_liquidity
            ),
            "materialize_portfolios": _source_sha(materialize_portfolios),
            "build_backtest_status": _source_sha(build_backtest_status),
        },
    }


def _locked_winners() -> list[dict[str, Any]]:
    winners = json.loads((M1C_ROOT / "winner_decisions.json").read_text())
    models = json.loads((M1C_ROOT / "model_records.json").read_text())
    if len(winners) != 16 or len(models) != 16:
        raise M1DContractError("M1C winner/model route is not exactly 16 records")
    model_index = {(item["outer_fold"], item["model_role"]): item for item in models}
    expected = {
        (f"decision_{year}0702T000000Z", role)
        for year in range(2019, 2027)
        for role in MODEL_ROLES
    }
    actual = {(item.get("outer_fold"), item.get("target_role")) for item in winners}
    if actual != expected or set(model_index) != expected:
        raise M1DContractError("M1C locked route fold/role boundary drifted")
    locked = []
    for item in sorted(winners, key=lambda row: (row["outer_fold"], row["target_role"])):
        winner = item.get("winner", {})
        lineage = winner.get("lineage", {})
        key = (item["outer_fold"], item["target_role"])
        model = model_index[key]
        model_lineage = model.get("lineage", {})
        if not (
            winner.get("availability_status") == "available_for_selection"
            and winner.get("evidence_scope") == "inner_validation_only"
            and lineage.get("outer_oos_consumed") is False
            and lineage.get("b1e_performance_consumed") is False
            and model_lineage.get("outer_oos_target_consumed") is False
            and model_lineage.get("b1e_performance_consumed") is False
            and winner.get("candidate_id") == model.get("candidate_id")
        ):
            raise M1DContractError(f"M1C inner-only winner lineage failed: {key}")
        model_path = _safe_relative(M1C_ROOT, model.get("path"))
        if (
            model_path.stat().st_size != model.get("size_bytes")
            or sha256_file(model_path) != model.get("sha256")
        ):
            raise M1DContractError(f"M1C locked model drifted: {key}")
        locked.append(
            {
                "outer_fold": item["outer_fold"],
                "target_role": item["target_role"],
                "candidate_id": winner["candidate_id"],
                "training_regime": winner["training_regime"],
                "selector_method": winner["selector_method"],
                "parameters": winner["parameters"],
                "complexity_score": winner["complexity_score"],
                "valid_inner_fold_count": winner["valid_inner_fold_count"],
                "inner_evidence_aggregate": winner["aggregate_metrics"],
                "inner_fold_lineage_sha256": lineage["inner_fold_lineage_sha256"],
                "winner_lineage_sha256": model_lineage["winner_lineage_sha256"],
                "selected_features": model_lineage["transformations"]["features"],
                "feature_selection_lineage_sha256": model_lineage[
                    "transformations"
                ]["feature_selection_lineage_sha256"],
                "preprocessing_lineage_sha256": _hash_payload(
                    model_lineage["transformations"]
                ),
                "training_population_fingerprint": model_lineage["population"][
                    "training_population_fingerprint"
                ],
                "training_rows": model_lineage["population"]["training_rows"],
                "training_label_end_max": model_lineage["target"][
                    "training_label_end_max"
                ],
                "model_path": model["path"],
                "model_size_bytes": model["size_bytes"],
                "model_sha256": model["sha256"],
                "selection_inputs": "inner_validation_only",
                "outer_oos_metric_consumed": False,
                "portfolio_or_b1e_metric_consumed": False,
            }
        )
    return locked


def _code_lineage() -> list[dict[str, Any]]:
    paths = [
        ROOT / "backtest/m1d_portfolio_performance.py",
        ROOT / "workflows/run_m1d_portfolio_performance.py",
        ROOT / "tests/backtest/test_m1d_portfolio_performance.py",
        ROOT / "portfolio/build_canonical_product.py",
        ROOT / "portfolio/selection_contract.py",
        ROOT / "backtest/free_data_v1_nav.py",
        ROOT / "backtest/free_data_v1_performance.py",
    ]
    rows = []
    for path in paths:
        if not path.is_file():
            raise M1DContractError(f"M1D code lineage file is missing: {path}")
        rows.append(
            {
                "path": path.relative_to(ROOT).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
                "lineage_class": (
                    "frozen_unchanged_b1d"
                    if path.name in {"free_data_v1_nav.py"}
                    else "unchanged_p4_b1e_dependency"
                    if path.name in {
                        "build_canonical_product.py",
                        "selection_contract.py",
                        "free_data_v1_performance.py",
                    }
                    else "m1d_implementation"
                ),
            }
        )
    return rows


def prepare_m1d_lock(
    artifact_root: Path,
    *,
    version: str,
    focused_test_report: Path,
    created_at_utc: str | None = None,
) -> LockedM1D:
    """Persist the complete M1D preflight and route before portfolio use."""
    if not VERSION_PATTERN.fullmatch(version):
        raise ValueError("version must be path-safe and end in -m1d")
    artifact_root = artifact_root.resolve()
    if artifact_root.name != version:
        raise ValueError("artifact root basename must equal version")
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise M1DContractError(f"M1D target is not empty: {artifact_root}")

    boundaries = _verify_boundary_hashes()
    m1c_manifest, m1c_verification = _verify_m1c_complete()
    m1a_verification = _verify_manifest_record_set(
        M1A_ROOT, M1A_MANIFEST_SHA256, "M1A"
    )
    b1c_verification = _verify_manifest_record_set(
        B1C_ROOT, B1C_MANIFEST_SHA256, "B1C"
    )
    b1e_verification = _verify_manifest_record_set(
        B1E_ROOT, B1E_MANIFEST_SHA256, "B1E"
    )
    contract_identity = _verify_contract_identity()
    tests = _parse_test_report(focused_test_report)
    winners = _locked_winners()

    artifact_root.mkdir(parents=True, exist_ok=True)
    records: list[tuple[Path, str]] = []
    manifest_sources = {
        "p2_manifest.json": P2_ROOT / "manifest.json",
        "p3_manifest.json": P3_ROOT / "manifest.json",
        "p4_manifest.json": P4_ROOT / "manifest.json",
        "b1c_manifest.json": B1C_ROOT / "manifest.json",
        "b1e_manifest.json": B1E_ROOT / "manifest.json",
        "m1a_manifest.json": M1A_ROOT / "manifest.json",
        "m1c_manifest.json": M1C_ROOT / "manifest.json",
    }
    for name, source in manifest_sources.items():
        target = artifact_root / "inputs" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        if sha256_file(target) != sha256_file(source):
            raise M1DContractError(f"pre-performance input copy drifted: {name}")
        records.append((target, "preperformance_frozen_manifest_copy"))
    contract_sources = [
        "contracts/free_data_v1_performance_contract.json",
        *(f"contracts/namespaces/{name}.json" for name in PERFORMANCE_NAMESPACES),
        f"contracts/rates/{DGS1MO_NAMESPACE}.json",
        f"contracts/rates/{ZERO_RATE_NAMESPACE}.json",
    ]
    for relative in contract_sources:
        source = B1C_ROOT / relative
        target = artifact_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        records.append((target, "exact_unchanged_b1d_b1e_contract_copy"))

    test_copy = artifact_root / "evidence/focused_boundary_tests.xml"
    test_copy.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(focused_test_report, test_copy)
    if sha256_file(test_copy) != tests["sha256"]:
        raise M1DContractError("focused boundary test evidence copy drifted")
    records.append((test_copy, "preperformance_focused_boundary_test_evidence"))

    created = created_at_utc or utc_now()
    preflight = {
        "schema_version": 1,
        "session": "M1D",
        "version": version,
        "created_at_utc": created,
        "status": "pass_locked_before_portfolio_or_performance",
        "frozen_boundary_verification": boundaries,
        "m1c_complete_independent_verification": m1c_verification,
        "m1a_record_verification": m1a_verification,
        "b1c_record_verification": b1c_verification,
        "b1e_record_verification": b1e_verification,
        "contract_identity": contract_identity,
        "focused_boundary_tests": {
            **tests,
            "copied_path": test_copy.relative_to(artifact_root).as_posix(),
        },
        "accepted_m1c_manifest_sha256": M1C_MANIFEST_SHA256,
        "m1c_route_record_count": len(winners),
        "m1c_outer_metrics_opened_for_route_selection": False,
        "b1e_metric_or_report_values_opened_for_route_selection": False,
        "portfolio_constructed": False,
        "performance_calculated": False,
        "external_request_made": False,
        "prior_m1d_result_observed": False,
        "partial_m1c_attempts_preserved": m1c_manifest.get(
            "preserved_partial_attempts", []
        ),
    }
    preflight_path = artifact_root / "preflight.json"
    _write_json(preflight_path, preflight)
    records.append((preflight_path, "complete_fail_closed_preperformance_preflight"))

    route = {
        "schema_version": 1,
        "session": "M1D",
        "version": version,
        "locked_at_utc": created,
        "route_id": "accepted_m1c_inner_selected_two_role_route",
        "route_selection_rule": (
            "use every accepted M1C per-outer-fold role winner already selected by "
            "the frozen M1A lexicographic inner-validation rule; make no global, "
            "outer-OOS, portfolio, performance, or scenario choice"
        ),
        "route_record_count": len(winners),
        "route_records": winners,
        "score_consumption": {
            "row_complete_source": "outer_oos_predictions.parquet",
            "source_sha256": next(
                item["sha256"]
                for item in m1c_manifest["records"]
                if item["path"] == "outer_oos_predictions.parquet"
            ),
            "portfolio_eligible_score_statuses": sorted(M1D_SCORE_STATUSES),
            "metric_eligible_m1c_status": "oos_prediction_available",
            "open_production_status": "production_score_open_unlabeled",
            "open_2024_2026_used_for_portfolio_only": True,
            "open_2024_2026_used_for_completed_metrics": False,
        },
        "p4_portfolio_contract": {
            "hard_gates": list(HARD_GATE_NAMES),
            "tree_probability_threshold": TREE_THRESHOLD,
            "rank_role": "lightgbm_regression",
            "rank_order": "descending score then ascending stable_row_id",
            "liquidity_window": "exactly 30 pre-prediction regular sessions",
            "minimum_median_dollar_volume": MIN_ADTV,
            "target_holdings": TARGET_N,
            "weight_each": WEIGHT,
            "benchmark_column": "frozen P2 benchmark_symbol",
            "decision_clock": "annual July 2",
            "holding_months": 36,
        },
        "evaluation_contract": performance_contract(),
        "evaluation_scope": {
            "completed_matured_years": list(MATURED_YEARS),
            "open_unlabeled_years": list(OPEN_YEARS),
            "physical_performance_namespaces": list(PERFORMANCE_NAMESPACES),
            "physical_outcome_namespaces": list(OUTCOME_NAMESPACES),
            "risk_free_namespaces": [DGS1MO_NAMESPACE, ZERO_RATE_NAMESPACE],
            "transaction_cost_rate_per_side": 0.0025,
            "transaction_cost_basis": "absolute_actual_traded_notional",
            "turnover_half_multiplier": False,
            "aggregate_method": "time_weighted_external_flow_adjusted",
            "frozen_b1e_comparison_manifest_sha256": B1E_MANIFEST_SHA256,
        },
        "final_reporting_thresholds": {
            "aggregate_net_cagr": THRESHOLD_NET_CAGR,
            "zero_rate_diagnostic_sharpe": THRESHOLD_ZERO_RATE_SHARPE,
            "selection_or_retry_inputs": False,
        },
        "prohibited_inputs": sorted(
            set(
                json.loads((M1A_ROOT / "experiment_contract.json").read_text())[
                    "selection_rule"
                ]["prohibited_selection_inputs"]
            )
        ),
        "adaptive_retry_allowed": False,
        "configuration_change_after_result_allowed": False,
        "outer_oos_metric_consumed_for_lock": False,
        "b1e_value_consumed_for_lock": False,
        "performance_result_observed": False,
    }
    route_path = artifact_root / "route_lock.json"
    _write_json(route_path, route)
    records.append((route_path, "immutable_preperformance_route_and_evaluation_lock"))

    state_path = artifact_root / "state/01_route_locked.json"
    _write_json(
        state_path,
        {
            "state": "route_locked_not_executed",
            "locked_at_utc": created,
            "preflight_sha256": sha256_file(preflight_path),
            "route_lock_sha256": sha256_file(route_path),
            "portfolio_constructed": False,
            "performance_calculated": False,
            "performance_result_observed": False,
            "retry_allowed": False,
        },
    )
    records.append((state_path, "immutable_route_lock_state"))

    code_lineage = _code_lineage()
    lock_manifest = {
        "schema_version": 1,
        "artifact_class": "M1D_PREPERFORMANCE_ROUTE_LOCK",
        "version": version,
        "created_at_utc": created,
        "records": [
            _record(artifact_root, path, role)
            for path, role in sorted(records, key=lambda item: item[0].as_posix())
        ],
        "code_lineage": code_lineage,
        "frozen_boundaries": boundaries["boundaries"],
        "claim": {
            "preflight_complete": True,
            "route_locked": True,
            "portfolio_constructed": False,
            "performance_calculated": False,
            "outer_oos_metric_used_for_lock": False,
            "b1e_value_used_for_lock": False,
            "adaptive_retry_allowed": False,
        },
    }
    lock_manifest_path = artifact_root / "lock_manifest.json"
    _write_json(lock_manifest_path, lock_manifest)
    return LockedM1D(
        root=artifact_root,
        lock_manifest_sha256=sha256_file(lock_manifest_path),
        preflight=preflight,
        route=route,
    )


def verify_m1d_lock(
    artifact_root: Path,
    *,
    expected_lock_manifest_sha256: str,
    require_unstarted: bool = True,
) -> LockedM1D:
    artifact_root = artifact_root.resolve()
    lock_path = artifact_root / "lock_manifest.json"
    if not lock_path.is_file() or sha256_file(lock_path) != expected_lock_manifest_sha256:
        raise M1DContractError("M1D lock manifest hash mismatch")
    lock = json.loads(lock_path.read_text())
    if lock.get("artifact_class") != "M1D_PREPERFORMANCE_ROUTE_LOCK":
        raise M1DContractError("M1D lock identity drifted")
    _verify_records(artifact_root, lock.get("records"), "M1D lock")
    for item in lock.get("code_lineage", []):
        path = ROOT / item["path"]
        if (
            not path.is_file()
            or path.stat().st_size != item["size_bytes"]
            or sha256_file(path) != item["sha256"]
        ):
            raise M1DContractError(f"M1D locked code drifted: {item['path']}")
    _verify_boundary_hashes()
    if require_unstarted and (artifact_root / "state/02_execution_started.json").exists():
        raise M1DContractError("M1D execution has already started; retry is prohibited")
    preflight = json.loads((artifact_root / "preflight.json").read_text())
    route = json.loads((artifact_root / "route_lock.json").read_text())
    if not (
        preflight.get("status") == "pass_locked_before_portfolio_or_performance"
        and route.get("adaptive_retry_allowed") is False
        and route.get("performance_result_observed") is False
    ):
        raise M1DContractError("M1D pre-performance lock claim drifted")
    return LockedM1D(
        root=artifact_root,
        lock_manifest_sha256=expected_lock_manifest_sha256,
        preflight=preflight,
        route=route,
    )


def _m1c_predictions_for_p4() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Map M1C scores into P4's unchanged two-role consumption interface."""
    m1c = pd.read_parquet(M1C_ROOT / "outer_oos_predictions.parquet")
    p3 = pd.read_parquet(P3_PREDICTIONS)
    if (
        len(m1c) != 87_612
        or len(p3) != 87_612
        or m1c.duplicated(["stable_row_id", "model_role"]).any()
        or p3.duplicated(["stable_row_id", "model_role"]).any()
    ):
        raise M1DContractError("M1C/P3 row-role boundary is incomplete")
    joined = p3.merge(
        m1c,
        on=["stable_row_id", "model_role"],
        suffixes=("_p3", "_m1c"),
        validate="one_to_one",
    )
    identity_pairs = (
        ("entity_id_p3", "entity_id_m1c"),
        ("ticker_p3", "ticker_m1c"),
        ("fiscal_year_p3", "fiscal_year_m1c"),
        ("decision_timestamp_p3", "decision_timestamp_m1c"),
    )
    for left, right in identity_pairs:
        if not joined[left].fillna("<NA>").astype(str).eq(
            joined[right].fillna("<NA>").astype(str)
        ).all():
            raise M1DContractError(f"M1C/P3 row identity mismatch: {left}/{right}")

    models = json.loads((M1C_ROOT / "model_records.json").read_text())
    model_index = {
        (item["outer_fold"], item["model_role"]): item for item in models
    }
    winner_file_hash = sha256_file(M1C_ROOT / "winner_decisions.json")
    p3_contract_hash = sha256_file(
        P3_ROOT / "configuration/research_model_contract.json"
    )
    output = p3.copy()
    output = output.set_index(["stable_row_id", "model_role"], drop=False)
    source = m1c.set_index(["stable_row_id", "model_role"], drop=False)
    source = source.reindex(output.index)
    if source.index.has_duplicates or source["stable_row_id"].isna().any():
        raise M1DContractError("M1C/P3 row-role reindexing failed")
    score_available = source["prediction_status"].isin(M1D_SCORE_STATUSES)
    numeric = pd.to_numeric(source["prediction"], errors="coerce")
    if (
        not np.isfinite(numeric.loc[score_available]).all()
        or numeric.loc[~score_available].notna().any()
    ):
        raise M1DContractError("M1C score/exclusion values are invalid")
    output["prediction_status"] = np.where(
        score_available, AVAILABLE_PREDICTION, "excluded"
    )
    output["prediction"] = numeric
    output["exclusion_code"] = np.where(
        score_available, "", source["exclusion_code"].fillna("m1c_score_unavailable")
    )
    output["training_rows"] = pd.to_numeric(
        source["training_rows"], errors="coerce"
    ).astype("Int64")
    output["selected_feature_count"] = pd.to_numeric(
        source["selected_feature_count"], errors="coerce"
    ).astype("Int64")
    output["selected_features_json"] = source["selected_features_json"]
    output["training_population_fingerprint"] = source[
        "training_population_fingerprint"
    ]
    output["feature_artifact_id"] = source[
        "feature_selection_lineage_sha256"
    ].map(lambda value: f"sha256:{value}" if pd.notna(value) else None)
    output["preprocessing_artifact_id"] = source[
        "preprocessing_lineage_sha256"
    ].map(lambda value: f"sha256:{value}" if pd.notna(value) else None)
    output["target_artifact_id"] = np.where(
        score_available, f"sha256:{p3_contract_hash}", None
    )
    output["model_configuration_artifact_id"] = np.where(
        score_available, f"sha256:{winner_file_hash}", None
    )
    output["model_artifact_id"] = source["model_sha256"].map(
        lambda value: f"sha256:{value}" if pd.notna(value) else None
    )
    training_ends: dict[tuple[str, str], Any] = {}
    for key, model in model_index.items():
        training_ends[key] = model["lineage"]["target"]["training_label_end_max"]
    output["training_label_end_max"] = [
        training_ends.get((str(fold), str(role)))
        for fold, role in zip(source["outer_fold"], source["model_role"])
    ]
    for column in (*MODEL_LINEAGE_COLUMNS, "training_label_end_max"):
        output.loc[~score_available, column] = None
    output = output.reset_index(drop=True)
    decision = pd.to_datetime(output["decision_timestamp"], utc=True)
    training_end = pd.to_datetime(
        output["training_label_end_max"], utc=True, errors="coerce"
    )
    if (
        output.loc[score_available.to_numpy(), list(MODEL_LINEAGE_COLUMNS)]
        .isna()
        .any()
        .any()
        or training_end.loc[score_available.to_numpy()].isna().any()
        or not training_end.loc[score_available.to_numpy()].lt(
            decision.loc[score_available.to_numpy()]
        ).all()
    ):
        raise M1DContractError("M1C P4 model/training lineage is incomplete")

    lineage = source.reset_index(drop=True).copy()
    lineage["accepted_m1c_manifest_sha256"] = M1C_MANIFEST_SHA256
    lineage["p4_consumption_status"] = np.where(
        lineage["prediction_status"].isin(M1D_SCORE_STATUSES),
        AVAILABLE_PREDICTION,
        "excluded",
    )
    status_counts = {
        f"{role}:{status}": int(count)
        for (role, status), count in lineage.groupby(
            ["model_role", "prediction_status"]
        ).size().items()
    }
    validation = {
        "status": "pass",
        "row_role_rows": len(output),
        "source_rows": int(output["stable_row_id"].nunique()),
        "score_rows_consumed_for_portfolio": int(score_available.sum()),
        "excluded_row_roles": int((~score_available).sum()),
        "original_status_counts": status_counts,
        "open_2024_2026_scores_portfolio_eligible": True,
        "open_2024_2026_scores_metric_eligible": False,
        "p3_source_and_hard_gate_lineage_reused": True,
        "m1c_model_and_winner_lineage_substituted": True,
        "outer_oos_or_portfolio_metric_used_for_route_selection": False,
    }
    return output, lineage, validation


def _build_locked_portfolio() -> dict[str, Any]:
    predictions, m1c_lineage, prediction_validation = _m1c_predictions_for_p4()
    features = pd.read_parquet(P2_FEATURES)
    if sha256_file(P2_FEATURES) != P2_FEATURES_SHA256:
        raise M1DContractError("P2 feature input drifted before portfolio construction")
    candidates, gate_validation = build_candidate_frame(predictions, features)
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
    gates.loc[
        gates["provenance"].eq(f"canonical_p3:{P3_MANIFEST_SHA256}"),
        "provenance",
    ] = f"accepted_m1c:{M1C_MANIFEST_SHA256}"
    supported_years = set(
        pd.to_datetime(
            periods.loc[periods["period_supported"], "decision_timestamp"], utc=True
        ).dt.year.astype(int)
    )
    if (
        supported_years != set((*MATURED_YEARS, *OPEN_YEARS))
        or len(holdings) != 8 * TARGET_N
        or holdings.groupby("decision_timestamp").size().ne(TARGET_N).any()
        or not np.allclose(
            holdings.groupby("decision_timestamp")["weight"].sum(), 1.0
        )
    ):
        raise M1DContractError("locked M1C route cannot form the exact 2019-2026 P4 portfolios")
    enriched = holdings.merge(
        features,
        on="stable_row_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_feature"),
    )
    calendar, _, _ = _load_market_indexes()
    vintage_plan, p4_status = build_backtest_status(enriched, periods, calendar)
    if (
        len(vintage_plan) != len(enriched)
        or not vintage_plan["transaction_cost_rate_per_side"].eq(0.0025).all()
        or not vintage_plan["holding_months"].eq(36).all()
    ):
        raise M1DContractError("unchanged P4 vintage/cost plan drifted")
    matured = vintage_plan["calendar_exit_timestamp"].notna()
    matured_years = set(
        pd.to_datetime(vintage_plan.loc[matured, "decision_timestamp"], utc=True)
        .dt.year.astype(int)
    )
    open_years = set(
        pd.to_datetime(vintage_plan.loc[~matured, "decision_timestamp"], utc=True)
        .dt.year.astype(int)
    )
    if matured_years != set(MATURED_YEARS) or open_years != set(OPEN_YEARS):
        raise M1DContractError("M1D completed/open vintage partition drifted")
    selection = candidate_table[
        [
            "stable_row_id",
            "decision_timestamp",
            "ticker",
            "tree_role_pass",
            "ranker_role_pass",
            "tree_threshold_pass",
            "liquidity_required",
            "liquidity_pass",
            "eligible_before_period_completeness",
            "rank",
            "holding",
            "weight",
        ]
    ].copy()
    selection["included_in_portfolio"] = selection["holding"]
    selection["completed_metric_eligible"] = (
        selection["holding"]
        & pd.to_datetime(selection["decision_timestamp"], utc=True)
        .dt.year.isin(MATURED_YEARS)
    )
    selection["open_unlabeled_portfolio_only"] = (
        selection["holding"]
        & pd.to_datetime(selection["decision_timestamp"], utc=True)
        .dt.year.isin(OPEN_YEARS)
    )
    return {
        "predictions": predictions,
        "m1c_lineage": m1c_lineage,
        "prediction_validation": prediction_validation,
        "candidates": candidate_table,
        "selection": selection,
        "gates": gates,
        "exclusions": exclusions,
        "holdings": enriched,
        "periods": periods,
        "vintage_plan": vintage_plan,
        "liquidity_evidence": evidence,
        "liquidity_coverage": liquidity,
        "liquidity_lineage": raw_lineage,
        "gate_validation": gate_validation,
        "liquidity_validation": liquidity_validation,
        "portfolio_validation": portfolio_validation,
        "p4_status_contract": p4_status,
    }


def _validated_prices(
    holdings: pd.DataFrame,
    liquidity_coverage: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    preflight = verify_frozen_inputs(DEFAULT_INPUTS)
    consumed = preflight["consumed_paths"]
    mapping = pd.read_parquet(consumed["session8e_mapping"]).set_index("entity_id")
    for row in holdings.itertuples(index=False):
        if row.entity_id not in mapping.index:
            raise M1DContractError(f"Session 8E holding mapping missing: {row.stable_row_id}")
        mapped = mapping.loc[row.entity_id]
        if not (
            str(mapped["ticker"]) == str(row.ticker)
            and str(mapped["provider_symbol"]) == str(row.provider_symbol)
            and str(mapped["mapping_policy"]) == "exact_uppercase"
        ):
            raise M1DContractError(f"Session 8E holding mapping drifted: {row.stable_row_id}")

    symbol_contracts: dict[str, tuple[str, str, str]] = {}
    for symbol, group in holdings.groupby("provider_symbol", sort=True):
        contracts = group[["provider_exchange", "exchange_calendar"]].drop_duplicates()
        if len(contracts) != 1:
            raise M1DContractError(f"holding symbol contract is ambiguous: {symbol}")
        symbol_contracts[str(symbol)] = (
            "holding",
            str(contracts.iloc[0]["provider_exchange"]),
            str(contracts.iloc[0]["exchange_calendar"]),
        )
    for symbol in BENCHMARK_SYMBOLS:
        symbol_contracts[symbol] = ("benchmark", "PCX", "XNYS")

    responses = _latest_response_records(consumed["session8e_responses"])
    raw_inventory = {
        str(item["path"]): item
        for item in json.loads(consumed["session8e_raw_inventory"].read_text())
    }
    normalized_inventory = {
        str(item["path"]): item
        for item in json.loads(consumed["session8e_normalized_inventory"].read_text())
    }
    calendars = _calendar_frames(DEFAULT_INPUTS.session8e_root)
    frames: dict[str, pd.DataFrame] = {}
    lineage = []
    for symbol in sorted(symbol_contracts):
        role, exchange, calendar = symbol_contracts[symbol]
        if symbol not in responses:
            raise M1DContractError(f"required frozen Session 8E symbol absent: {symbol}")
        frame, item = _validate_normalized_symbol(
            symbol=symbol,
            expected_role=role,
            expected_exchange=exchange,
            expected_calendar=calendar,
            session8e_root=DEFAULT_INPUTS.session8e_root,
            response=responses[symbol],
            raw_inventory=raw_inventory,
            normalized_inventory=normalized_inventory,
            calendars=calendars,
        )
        frames[symbol] = frame
        lineage.append(item)
    response_hashes = {
        item["provider_symbol"]: item["response_sha256"] for item in lineage
    }
    selected_liquidity = liquidity_coverage[
        liquidity_coverage["stable_row_id"].isin(holdings["stable_row_id"])
    ].set_index("stable_row_id")
    for row in holdings.itertuples(index=False):
        if (
            row.stable_row_id not in selected_liquidity.index
            or selected_liquidity.loc[row.stable_row_id, "raw_response_sha256"]
            != response_hashes[str(row.provider_symbol)]
        ):
            raise M1DContractError(f"holding liquidity/performance raw hash mismatch: {row.stable_row_id}")
    prices = pd.concat(frames.values(), ignore_index=True).sort_values(
        ["symbol", "market_close"]
    ).reset_index(drop=True)
    return prices, pd.DataFrame(lineage).sort_values("provider_symbol"), frames


def _unsupported_identity_record(requirement: Mapping[str, Any], columns: list[str]) -> dict[str, Any]:
    cik = str(requirement.get("cik") or requirement.get("sec_cik") or "")
    record = {column: pd.NA for column in columns}
    record.update(
        {
            "requirement_id": requirement["requirement_id"],
            "instrument_role": "holding",
            "stable_row_id": requirement["stable_row_id"],
            "issuer_id": f"issuer:us-sec:{cik}",
            "security_id": f"security:unsupported:{str(requirement['stable_row_id'])[:24]}",
            "ticker": requirement["ticker"],
            "required_start": requirement["required_start"],
            "required_end": requirement["required_end"],
            "s1_coverage_status": "unsupported",
            "s1_identity_status": "unsupported_not_in_frozen_s1_requirement",
            "s1_listing_status": "unsupported",
            "s1_security_type_status": "unsupported",
            "s1_event_status": "unresolved",
            "s1_price_adjustment_status": "unsupported",
            "reason_codes": json.dumps(
                [
                    "m1d_holding_absent_from_frozen_s1_requirement_set",
                    "historical_listing_effective_dates_unavailable",
                    "security_type_history_unavailable",
                    "complete_corporate_action_terms_unavailable",
                ],
                sort_keys=True,
            ),
            "source_ids": "[]",
            "current_ticker_substitution_used": False,
            "ticker_chaining_used": False,
            "dated_security_lineage_complete": False,
            "identity_rule_status": "unsupported_not_in_frozen_s1_requirement_set",
            "certified_performance_identity_available": False,
        }
    )
    return record


def _unsupported_action_record(
    requirement: Mapping[str, Any],
    identity: Mapping[str, Any],
    columns: list[str],
) -> dict[str, Any]:
    record = {column: pd.NA for column in columns}
    record.update(
        {
            "requirement_id": requirement["requirement_id"],
            "instrument_role": "holding",
            "stable_row_id": requirement["stable_row_id"],
            "security_id": identity["security_id"],
            "ticker": requirement["ticker"],
            "s1_event_status": "unresolved",
            "s1_price_adjustment_status": "unsupported",
            "source_event_indicator_count": 0,
            "holding_window_indicator_count": 0,
            "deterministic_action": "unresolved",
            "historical_evaluation_status": "unavailable_fail_closed",
            "primary_document_support_state": "not_in_frozen_e1_requirement_set",
            "provider_adjclose_action_semantics_certified": False,
            "forward_fill_across_unresolved_event_allowed": False,
            "event_inferred_from_disappearance_or_form_family": False,
            "unsupported_recovery_allowed_in_observed_namespace": False,
            "assumed_outcome_allowed_in_labels_or_training": False,
            "primary_return_available": False,
        }
    )
    return record


def _build_requirements_and_evidence(
    portfolio: Mapping[str, Any],
) -> tuple[EvidenceBundle, dict[str, Any], pd.DataFrame]:
    holdings = portfolio["holdings"]
    plan = portfolio["vintage_plan"]
    prices, price_lineage, price_frames = _validated_prices(
        holdings, portfolio["liquidity_coverage"]
    )
    baseline = load_frozen_b1c_evidence(
        B1C_ROOT, expected_manifest_sha256=B1C_MANIFEST_SHA256
    )
    selected = holdings[
        [
            "stable_row_id",
            "entity_id",
            "cik",
            "ticker",
            "provider_symbol",
            "exchange",
            "provider_exchange",
            "exchange_calendar",
            "decision_timestamp",
            "prediction_timestamp",
            "entry_timestamp",
            "entry_session_date",
            "benchmark_symbol",
            "weight",
        ]
    ].merge(
        plan[
            [
                "stable_row_id",
                "target_exit_timestamp",
                "calendar_exit_timestamp",
                "holding_months",
                "planned_vintage_aum_usd",
                "planned_entry_notional_usd",
                "transaction_cost_rate_per_side",
                "transaction_cost_basis",
                "vintage_clock_status",
            ]
        ],
        on="stable_row_id",
        validate="one_to_one",
    )
    selected["requirement_id"] = selected["stable_row_id"].map(
        lambda value: f"holding:{value}"
    )
    selected["instrument_role"] = "holding"
    selected["sec_cik"] = selected["cik"].fillna("").astype(str)
    selected["decision_year"] = pd.to_datetime(
        selected["decision_timestamp"], utc=True
    ).dt.year.astype(int)
    selected["requirement_state"] = np.where(
        selected["calendar_exit_timestamp"].notna(),
        "matured_2019_2023",
        "open_2024_2026",
    )
    selected["required_start"] = selected["entry_timestamp"]
    selected["required_end"] = selected["target_exit_timestamp"]

    baseline_benchmarks = baseline.requirements[
        baseline.requirements["instrument_role"].eq("benchmark")
    ].copy()
    columns = list(baseline.requirements.columns)
    for column in columns:
        if column not in selected:
            selected[column] = pd.NA
    requirements = pd.concat(
        [selected[columns], baseline_benchmarks[columns]], ignore_index=True
    ).sort_values(["instrument_role", "requirement_id"]).reset_index(drop=True)

    baseline_identity = baseline.security_identity.set_index("requirement_id")
    identity_rows = []
    identity_columns = list(baseline.security_identity.columns)
    for requirement in selected.to_dict(orient="records"):
        requirement_id = requirement["requirement_id"]
        if requirement_id in baseline_identity.index:
            identity_rows.append(baseline_identity.loc[requirement_id].to_dict())
            identity_rows[-1]["requirement_id"] = requirement_id
        else:
            identity_rows.append(
                _unsupported_identity_record(requirement, identity_columns)
            )
    identity_rows.extend(
        baseline.security_identity[
            baseline.security_identity["instrument_role"].eq("benchmark")
        ].to_dict(orient="records")
    )
    identity = pd.DataFrame(identity_rows, columns=identity_columns).sort_values(
        ["instrument_role", "requirement_id"]
    ).reset_index(drop=True)

    baseline_actions = baseline.security_actions.set_index("requirement_id")
    identity_index = identity.set_index("requirement_id")
    action_columns = list(baseline.security_actions.columns)
    action_rows = []
    for requirement in selected.to_dict(orient="records"):
        requirement_id = requirement["requirement_id"]
        if requirement_id in baseline_actions.index:
            action_rows.append(baseline_actions.loc[requirement_id].to_dict())
            action_rows[-1]["requirement_id"] = requirement_id
        else:
            action_rows.append(
                _unsupported_action_record(
                    requirement,
                    identity_index.loc[requirement_id].to_dict(),
                    action_columns,
                )
            )
    action_rows.extend(
        baseline.security_actions[
            baseline.security_actions["instrument_role"].eq("benchmark")
        ].to_dict(orient="records")
    )
    actions = pd.DataFrame(action_rows, columns=action_columns).sort_values(
        ["instrument_role", "requirement_id"]
    ).reset_index(drop=True)

    calendar = _calendar_frames(DEFAULT_INPUTS.session8e_root)["XNYS"].copy()
    calendar["month"] = calendar["market_close"].dt.tz_localize(None).dt.to_period("M")
    month_ends = calendar.sort_values("market_close").groupby("month").tail(1)
    identity_by_id = identity.set_index("requirement_id")
    coverage_rows = []
    for requirement in selected.itertuples(index=False):
        stock = price_frames[str(requirement.provider_symbol)]
        benchmark = price_frames[str(requirement.benchmark_symbol)]
        common = stock.merge(
            benchmark,
            on=["session_date", "market_close"],
            suffixes=("_stock", "_benchmark"),
            validate="one_to_one",
        )
        common_closes = set(common["market_close"])
        entry = pd.Timestamp(requirement.entry_timestamp)
        matured = pd.notna(requirement.calendar_exit_timestamp)
        end = (
            pd.Timestamp(requirement.calendar_exit_timestamp)
            if matured
            else min(stock["market_close"].max(), benchmark["market_close"].max())
        )
        required_months = month_ends.loc[
            month_ends["market_close"].ge(entry)
            & month_ends["market_close"].le(end),
            "market_close",
        ].tolist()
        missing = [instant for instant in required_months if instant not in common_closes]
        entry_common = entry in common_closes
        exit_common = (
            pd.Timestamp(requirement.calendar_exit_timestamp) in common_closes
            if matured
            else False
        )
        if not entry_common or (matured and not exit_common) or missing:
            raise M1DContractError(
                f"M1D common stock/benchmark evidence gap: {requirement.requirement_id}"
            )
        ident = identity_by_id.loc[requirement.requirement_id]
        coverage_rows.append(
            {
                "requirement_id": requirement.requirement_id,
                "stable_row_id": requirement.stable_row_id,
                "instrument_role": "holding",
                "ticker": requirement.ticker,
                "provider_symbol": requirement.provider_symbol,
                "requirement_state": requirement.requirement_state,
                "decision_year": requirement.decision_year,
                "s1_coverage_status": ident["s1_coverage_status"],
                "s1_identity_status": ident["s1_identity_status"],
                "s1_event_status": ident["s1_event_status"],
                "fully_matched_s1_requirement": False,
                "provider_adjclose_semantics_certified": False,
                "certified_security_action_ledger": False,
                "benchmark_gap_scenario_imputation_allowed": False,
                "assigned_benchmark_symbol": requirement.benchmark_symbol,
                "entry_observed_common": True,
                "exit_observed_common": exit_common,
                "required_month_end_count": len(required_months),
                "observed_common_month_end_count": len(required_months),
                "missing_common_month_end_count": 0,
                "benchmark_gap_count": 0,
                "evidence_end_timestamp": end,
                "price_coverage_status": (
                    "complete_common_entry_month_ends_and_exit"
                    if matured
                    else "open_common_observations_to_evidence_end"
                ),
                "relative_evidence_status": (
                    "observed_common_provider_evidence_available"
                    if matured
                    else "open_not_matured"
                ),
            }
        )
    benchmark_coverage = baseline.coverage[
        baseline.coverage["instrument_role"].eq("benchmark")
    ].copy()
    coverage = pd.concat(
        [pd.DataFrame(coverage_rows), benchmark_coverage], ignore_index=True
    ).sort_values(["instrument_role", "requirement_id"]).reset_index(drop=True)
    benchmark_requirements = baseline.benchmark_requirements.copy()
    counts = selected["benchmark_symbol"].value_counts()
    benchmark_requirements["assigned_holding_count"] = benchmark_requirements[
        "ticker"
    ].map(counts).fillna(0).astype(int)

    namespace_contracts = {
        name: {"namespace": name, **contract}
        for name, contract in _namespace_contracts().items()
    }
    eligibility: dict[str, pd.DataFrame] = {}
    for namespace, contract in _namespace_contracts().items():
        frame = coverage[
            [
                "requirement_id",
                "stable_row_id",
                "instrument_role",
                "ticker",
                "requirement_state",
                "price_coverage_status",
                "s1_coverage_status",
                "relative_evidence_status",
            ]
        ].copy()
        matured = frame["requirement_state"].eq("matured_2019_2023")
        benchmark = frame["instrument_role"].eq("benchmark")
        frame["namespace"] = namespace
        frame["eligibility_state"] = np.select(
            [matured, benchmark],
            [
                (
                    "eligible_observed_common_price_evidence"
                    if namespace == "observed_available_diagnostic"
                    else "eligible_no_terminal_scenario_required"
                ),
                "benchmark_master_incomplete",
            ],
            default="open_not_matured",
        )
        frame["eligible_for_future_nav_engine"] = matured
        frame["scenario_triggered"] = False
        frame["scenario_return_if_triggered"] = contract.get("terminal_assumption")
        frame["assumed_outcome_used"] = False
        frame["allowed_in_labels_or_training"] = False
        frame["benchmark_gap_imputed"] = False
        frame["performance_calculated"] = False
        eligibility[namespace] = frame.sort_values(
            ["instrument_role", "requirement_id"]
        ).reset_index(drop=True)

    bundle = EvidenceBundle(
        requirements=requirements,
        security_identity=identity,
        security_actions=actions,
        prices=prices,
        benchmark_requirements=benchmark_requirements,
        coverage=coverage,
        namespace_eligibility=eligibility,
        performance_contract=baseline.performance_contract,
        namespace_contracts=namespace_contracts,
        rate_status=baseline.rate_status,
        risk_free_observations=None,
        source_manifest_sha256=M1C_MANIFEST_SHA256,
        source_manifest={"artifact_class": ARTIFACT_CLASS},
    )
    validate_evidence_bundle(bundle)
    holding_identity = identity[identity["instrument_role"].eq("holding")]
    summary = {
        "status": "pass_complete_preperformance_evidence_boundary",
        "requirement_rows": len(requirements),
        "holding_rows": len(selected),
        "matured_holding_rows": int(
            selected["requirement_state"].eq("matured_2019_2023").sum()
        ),
        "open_holding_rows": int(
            selected["requirement_state"].eq("open_2024_2026").sum()
        ),
        "price_rows": len(prices),
        "holding_symbols": int(selected["provider_symbol"].nunique()),
        "benchmark_symbols": len(BENCHMARK_SYMBOLS),
        "s1_ambiguous_holdings": int(
            holding_identity["s1_coverage_status"].eq("ambiguous").sum()
        ),
        "s1_unsupported_holdings": int(
            holding_identity["s1_coverage_status"].eq("unsupported").sum()
        ),
        "certified_identity_holdings": 0,
        "provider_adjustment_semantics_certified": False,
        "security_action_ledger_certified": False,
        "benchmark_gaps": int(coverage["benchmark_gap_count"].fillna(0).sum()),
        "missing_common_month_ends": int(
            coverage["missing_common_month_end_count"].fillna(0).sum()
        ),
        "dgs1mo_available": False,
        "dgs1mo_status": baseline.rate_status.get("dgs1mo_status"),
        "performance_calculated": False,
    }
    return bundle, summary, price_lineage


def _scope_coverage_dynamic(
    result: PerformanceResult,
    holdings: pd.DataFrame,
    *,
    year: int | None,
    completed_vintage_count: int,
    open_vintage_count: int,
) -> dict[str, Any]:
    selected = holdings if year is None else holdings[holdings["decision_year"].eq(year)]
    ids = set(selected["requirement_id"])
    outcomes = _used_outcomes(result)
    if len(outcomes):
        outcomes = outcomes[outcomes["requirement_id"].isin(ids)]
        used = outcomes[outcomes["used_in_nav"].astype(bool)]
    else:
        used = outcomes
    notional = selected.set_index("requirement_id")["planned_entry_notional_usd"]

    def count(name: str) -> int:
        if not len(outcomes):
            return 0
        return int(
            outcomes.loc[
                outcomes["outcome_namespace"].eq(name), "requirement_id"
            ].nunique()
        )

    def capital(name: str) -> float:
        if not len(outcomes):
            return 0.0
        selected_ids = set(
            outcomes.loc[
                outcomes["outcome_namespace"].eq(name), "requirement_id"
            ]
        )
        return float(
            pd.to_numeric(
                notional.reindex(sorted(selected_ids)), errors="coerce"
            ).fillna(0).sum()
        )

    ledger = result.holding_ledger[result.holding_ledger["requirement_id"].isin(ids)]
    capital_denominator = float(
        pd.to_numeric(selected["planned_entry_notional_usd"], errors="coerce").sum()
    )
    resolved_ids = set(used["requirement_id"]) if len(used) else set()
    resolved_capital = float(
        pd.to_numeric(
            notional.reindex(sorted(resolved_ids)), errors="coerce"
        ).fillna(0).sum()
    )
    terminal = (
        int(
            outcomes.loc[
                ~outcomes["event_type"].eq("complete_price_path"), "requirement_id"
            ].nunique()
        )
        if len(outcomes)
        else 0
    )
    return {
        "holding_count_denominator": int(len(selected)),
        "resolved_holding_count": int(len(resolved_ids)),
        "capital_denominator": capital_denominator,
        "resolved_capital": resolved_capital,
        "observed_holding_count": count("observed"),
        "observed_capital": capital("observed"),
        "provider_confirmed_holding_count": count("provider_confirmed"),
        "provider_confirmed_capital": capital("provider_confirmed"),
        "unsupported_unresolved_holding_count": count("unsupported_unresolved"),
        "unsupported_unresolved_capital": capital("unsupported_unresolved"),
        "scenario_imputed_holding_count": count("bounded_scenario"),
        "scenario_imputed_capital": capital("bounded_scenario"),
        "required_stock_session_count": int(len(ledger)),
        "observed_stock_session_count": int(len(ledger)),
        "required_benchmark_session_count": int(len(ledger)),
        "observed_benchmark_session_count": int(len(ledger)),
        "benchmark_gap_count": 0,
        "terminal_event_holding_count": terminal,
        "completed_vintage_count": 1 if year is not None else completed_vintage_count,
        "open_vintage_count": open_vintage_count,
    }


def _metric_ledger_dynamic(
    bundle: EvidenceBundle,
    result: PerformanceResult,
    benchmark_vintage: Mapping[int, Mapping[str, Mapping[str, Any]]],
    benchmark_aggregate: Mapping[str, Mapping[str, Any]],
) -> pd.DataFrame:
    holdings = bundle.requirements[
        bundle.requirements["instrument_role"].eq("holding")
        & bundle.requirements["calendar_exit_timestamp"].notna()
    ].copy()
    open_count = int(
        bundle.requirements["instrument_role"].eq("holding").sum() - len(holdings)
    )
    years = sorted(pd.to_numeric(holdings["decision_year"]).astype(int).unique())
    rows: list[dict[str, Any]] = []
    for year in years:
        coverage = _scope_coverage_dynamic(
            result,
            holdings,
            year=year,
            completed_vintage_count=len(years),
            open_vintage_count=open_count // TARGET_N,
        )
        for basis in ("gross", "net"):
            rows.extend(
                _metric_rows_for_scope(
                    bundle=bundle,
                    result=result,
                    performance_namespace=result.performance_namespace,
                    scope="separate_vintage",
                    decision_year=year,
                    basis=basis,
                    portfolio=result.vintage_metrics[year][basis],
                    benchmark=benchmark_vintage[year][basis],
                    coverage=coverage,
                )
            )
    aggregate_coverage = _scope_coverage_dynamic(
        result,
        holdings,
        year=None,
        completed_vintage_count=len(years),
        open_vintage_count=open_count // TARGET_N,
    )
    for basis in ("gross", "net"):
        rows.extend(
            _metric_rows_for_scope(
                bundle=bundle,
                result=result,
                performance_namespace=result.performance_namespace,
                scope="aggregate_strategy",
                decision_year=None,
                basis=basis,
                portfolio=result.aggregate_metrics[basis],
                benchmark=benchmark_aggregate[basis],
                coverage=aggregate_coverage,
            )
        )
    frame = pd.DataFrame(rows)
    frame["decision_year"] = frame["decision_year"].astype("Int64")
    return frame.sort_values(
        [
            "performance_namespace",
            "metric_scope",
            "decision_year",
            "basis",
            "stream",
            "risk_free_namespace",
            "metric_name",
        ],
        na_position="last",
    ).reset_index(drop=True)


def _assert_m1d_result(result: PerformanceResult) -> None:
    if not result.available or result.unavailable_reasons:
        raise M1DContractError(
            f"M1D namespace unavailable: {result.performance_namespace} "
            f"{result.unavailable_reasons}"
        )
    years = set(pd.to_numeric(result.vintage_nav["decision_year"]).astype(int))
    if years != set(MATURED_YEARS) or len(result.capital_ledger) != len(MATURED_YEARS):
        raise M1DContractError("M1D result completed-vintage boundary drifted")
    if int(result.coverage.get("open_holding_count", -1)) != 45:
        raise M1DContractError("M1D open holdings were not preserved")
    if int(result.coverage.get("benchmark_gap_count", -1)) != 0:
        raise M1DContractError("M1D benchmark gaps entered performance")


def _metric_value(
    metrics: pd.DataFrame,
    *,
    namespace: str,
    scope: str,
    basis: str,
    stream: str,
    name: str,
    year: int | None = None,
    rate: str = ZERO_RATE_NAMESPACE,
) -> float | None:
    mask = (
        metrics["performance_namespace"].eq(namespace)
        & metrics["metric_scope"].eq(scope)
        & metrics["basis"].eq(basis)
        & metrics["stream"].eq(stream)
        & metrics["metric_name"].eq(name)
        & metrics["risk_free_namespace"].eq(rate)
    )
    mask &= metrics["decision_year"].isna() if year is None else metrics[
        "decision_year"
    ].eq(year)
    selected = metrics.loc[mask, "metric_value"]
    if len(selected) != 1:
        raise M1DContractError(
            f"metric key is not unique: {namespace}/{scope}/{year}/{basis}/{stream}/{name}/{rate}"
        )
    value = selected.iloc[0]
    return None if pd.isna(value) else float(value)


def _baseline_comparison(
    m1d_metrics: pd.DataFrame,
    holdings: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if sha256_file(B1E_ROOT / "manifest.json") != B1E_MANIFEST_SHA256:
        raise M1DContractError("B1E baseline drifted before comparison")
    baseline = pd.read_parquet(B1E_ROOT / "outputs/metrics.parquet")
    baseline_namespace = PRIMARY_NAMESPACE
    metric_names = (
        "cagr",
        "annualized_volatility",
        "maximum_drawdown",
        "maximum_drawdown_duration_months",
        "sharpe_ratio",
        "sortino_ratio",
        "alpha",
        "tracking_error",
        "information_ratio",
        "turnover",
        "hit_rate",
    )
    rows = []
    for scope, years in (
        ("aggregate_strategy", [None]),
        ("separate_vintage", list(MATURED_YEARS)),
    ):
        for year in years:
            for basis in ("gross", "net"):
                for stream in ("portfolio", "benchmark"):
                    for name in metric_names:
                        try:
                            new = _metric_value(
                                m1d_metrics,
                                namespace=PRIMARY_NAMESPACE,
                                scope=scope,
                                year=year,
                                basis=basis,
                                stream=stream,
                                name=name,
                            )
                            old = _metric_value(
                                baseline,
                                namespace=baseline_namespace,
                                scope=scope,
                                year=year,
                                basis=basis,
                                stream=stream,
                                name=name,
                            )
                        except M1DContractError:
                            continue
                        rows.append(
                            {
                                "comparison_scope": (
                                    "aggregate_non_like_for_like_history_2019_2023_route_vs_2015_2023_baseline"
                                    if scope == "aggregate_strategy"
                                    else "common_vintage_like_for_like"
                                ),
                                "metric_scope": scope,
                                "decision_year": year,
                                "basis": basis,
                                "stream": stream,
                                "metric_name": name,
                                "risk_free_namespace": ZERO_RATE_NAMESPACE,
                                "m1d_metric_value": new,
                                "b1e_metric_value": old,
                                "difference_m1d_minus_b1e": (
                                    new - old if new is not None and old is not None else None
                                ),
                                "m1d_completed_years": "2019-2023",
                                "b1e_completed_years": "2015-2023",
                                "aggregate_direct_comparability": scope != "aggregate_strategy",
                            }
                        )
    comparison = pd.DataFrame(rows)
    comparison["decision_year"] = comparison["decision_year"].astype("Int64")

    baseline_holdings = pd.read_parquet(P4_ROOT / "outputs/holdings.parquet")
    baseline_holdings["decision_year"] = pd.to_datetime(
        baseline_holdings["decision_timestamp"], utc=True
    ).dt.year.astype(int)
    m1d_holdings = holdings.copy()
    m1d_holdings["decision_year"] = pd.to_datetime(
        m1d_holdings["decision_timestamp"], utc=True
    ).dt.year.astype(int)
    overlap_rows = []
    for year in (*MATURED_YEARS, *OPEN_YEARS):
        new_ids = set(
            m1d_holdings.loc[m1d_holdings["decision_year"].eq(year), "stable_row_id"]
        )
        old_ids = set(
            baseline_holdings.loc[
                baseline_holdings["decision_year"].eq(year), "stable_row_id"
            ]
        )
        intersection = new_ids & old_ids
        union = new_ids | old_ids
        overlap_rows.append(
            {
                "decision_year": year,
                "m1d_holding_count": len(new_ids),
                "b1e_holding_count": len(old_ids),
                "stable_row_overlap_count": len(intersection),
                "stable_row_overlap_fraction_of_15": len(intersection) / TARGET_N,
                "stable_row_jaccard": len(intersection) / len(union) if union else None,
            }
        )
    overlap = pd.DataFrame(overlap_rows)
    summary = {
        "frozen_b1e_manifest_sha256": B1E_MANIFEST_SHA256,
        "baseline_values_read_only_after_route_lock": True,
        "aggregate_comparison_directly_like_for_like": False,
        "aggregate_comparison_limitation": (
            "M1D completed history begins in 2019 because the frozen nested route has "
            "no selected outer models for 2015-2018; B1E aggregate begins in 2015"
        ),
        "common_vintage_years": list(MATURED_YEARS),
        "mean_holding_overlap_fraction": float(
            overlap["stable_row_overlap_fraction_of_15"].mean()
        ),
        "comparison_rows": len(comparison),
    }
    return comparison, overlap, summary


def _stability_summary(metrics: pd.DataFrame, results: Mapping[str, PerformanceResult]) -> dict[str, Any]:
    cagr = np.array(
        [
            _metric_value(
                metrics,
                namespace=PRIMARY_NAMESPACE,
                scope="separate_vintage",
                year=year,
                basis="net",
                stream="portfolio",
                name="cagr",
            )
            for year in MATURED_YEARS
        ],
        dtype=float,
    )
    sharpe = np.array(
        [
            _metric_value(
                metrics,
                namespace=PRIMARY_NAMESPACE,
                scope="separate_vintage",
                year=year,
                basis="net",
                stream="portfolio",
                name="sharpe_ratio",
            )
            for year in MATURED_YEARS
        ],
        dtype=float,
    )
    reference = results[PRIMARY_NAMESPACE].aggregate_nav.drop(
        columns=["performance_namespace"], errors="ignore"
    )
    namespaces_equal = all(
        reference.equals(
            result.aggregate_nav.drop(
                columns=["performance_namespace"], errors="ignore"
            )
        )
        for result in results.values()
    )
    return {
        "completed_vintage_count": len(MATURED_YEARS),
        "net_cagr_positive_vintages": int((cagr > 0).sum()),
        "net_cagr_median": float(np.median(cagr)),
        "net_cagr_minimum": float(np.min(cagr)),
        "net_cagr_maximum": float(np.max(cagr)),
        "net_cagr_sample_standard_deviation": float(np.std(cagr, ddof=1)),
        "zero_rate_net_sharpe_positive_vintages": int((sharpe > 0).sum()),
        "zero_rate_net_sharpe_median": float(np.median(sharpe)),
        "zero_rate_net_sharpe_minimum": float(np.min(sharpe)),
        "zero_rate_net_sharpe_maximum": float(np.max(sharpe)),
        "all_physical_namespace_aggregate_nav_values_equal": namespaces_equal,
        "namespace_equality_reason": (
            "no explicit unsupported terminal-exit trigger; zero scenario-imputed capital"
            if namespaces_equal
            else "an explicit namespace-specific terminal outcome was applied"
        ),
    }


def _fmt_percent(value: Any) -> str:
    return "unavailable" if value is None or pd.isna(value) else f"{float(value):.2%}"


def _fmt_number(value: Any) -> str:
    return "unavailable" if value is None or pd.isna(value) else f"{float(value):.3f}"


def render_m1d_report(summary: Mapping[str, Any]) -> str:
    metrics = summary["primary_metrics"]
    thresholds = summary["thresholds"]
    coverage = summary["coverage"]
    comparison = summary["b1e_comparison"]
    stability = summary["stability"]
    return f"""# M1D Locked M1C Portfolio Comparison

## Result

The exact accepted M1C inner-selected route was locked before portfolio or
performance calculation and evaluated once through the unchanged P4 portfolio
rules and unchanged B1D/B1E contract. No route, feature, parameter, gate,
portfolio rule, cost, benchmark, namespace, or assumption was changed after a
result was observed.

- Aggregate net CAGR: **{_fmt_percent(metrics['aggregate_net_cagr'])}**; 30%
  threshold **{'met' if thresholds['aggregate_net_cagr_met'] else 'not met'}**.
- Aggregate net zero-rate diagnostic Sharpe: **{_fmt_number(metrics['aggregate_net_zero_rate_sharpe'])}**;
  1.0 threshold **{'met' if thresholds['aggregate_net_zero_rate_sharpe_met'] else 'not met'}**.
- Aggregate net maximum drawdown: **{_fmt_percent(metrics['aggregate_net_maximum_drawdown'])}**.
- Aggregate net annualized volatility: **{_fmt_percent(metrics['aggregate_net_annualized_volatility'])}**.
- Aggregate net turnover: **{_fmt_number(metrics['aggregate_net_turnover'])}**,
  with no half multiplier.
- Net benchmark CAGR: **{_fmt_percent(metrics['benchmark_net_cagr'])}**.

## Coverage and open outcomes

- Completed performance includes exactly {coverage['matured_holding_rows']} holdings
  in five matured 2019-2023 vintages and ${coverage['matured_planned_capital']:,.0f}
  planned exposure.
- All {coverage['required_stock_session_count']:,} required stock sessions and
  {coverage['required_benchmark_session_count']:,} assigned-benchmark sessions
  were observed; benchmark gaps are {coverage['benchmark_gap_count']}.
- The 45 open/unlabeled 2024-2026 holdings remain in the row-complete portfolio
  and evidence outputs but are excluded from completed historical metrics.
- Scenario-imputed capital is ${coverage['scenario_imputed_capital']:,.0f}.

## Frozen B1E comparison and stability

- Frozen B1E aggregate net CAGR: **{_fmt_percent(comparison['b1e_aggregate_net_cagr'])}**.
- Frozen B1E aggregate net zero-rate diagnostic Sharpe:
  **{_fmt_number(comparison['b1e_aggregate_net_zero_rate_sharpe'])}**.
- Aggregate comparison is not directly like-for-like: M1D starts in 2019,
  while frozen B1E starts in 2015. Common-vintage 2019-2023 rows are separately
  materialized.
- Positive net-CAGR vintages: {stability['net_cagr_positive_vintages']}/5;
  median {_fmt_percent(stability['net_cagr_median'])}, range
  {_fmt_percent(stability['net_cagr_minimum'])} to
  {_fmt_percent(stability['net_cagr_maximum'])}.
- All four physical performance namespaces have
  {'equal' if stability['all_physical_namespace_aggregate_nav_values_equal'] else 'different'}
  aggregate NAV values. They remain physically separate.

## Evidence limitations

The result is free-source historical research, not survivorship-complete or
provider-certified performance, personalized investment advice, or a future-
performance promise. Frozen Yahoo adjusted close is the sole total-return
input and its exact corporate-action semantics are uncertified. The frozen S1
ledger does not provide complete dated identity/action coverage for the M1D
holdings, and no event is inferred from disappearance, ticker history, filing
family, or current knowledge.

The exact `DGS1MO` ALFRED 2026-07-17 observations remain absent. DGS1MO-
dependent Sharpe, Sortino, and alpha are unavailable with reason
`exact_dgs1mo_observations_absent`. The reported Sharpe is only the physically
separate zero-risk-free diagnostic; no rate was carried, interpolated,
substituted, averaged, or relabelled.
"""


def execute_locked_m1d(
    artifact_root: Path,
    *,
    expected_lock_manifest_sha256: str,
    created_at_utc: str | None = None,
) -> Path:
    """Execute one locked M1D comparison; this function cannot resume."""
    locked = verify_m1d_lock(
        artifact_root,
        expected_lock_manifest_sha256=expected_lock_manifest_sha256,
        require_unstarted=True,
    )
    artifact_root = locked.root
    started = created_at_utc or utc_now()
    state_started = artifact_root / "state/02_execution_started.json"
    _write_json(
        state_started,
        {
            "state": "execution_started_no_retry_allowed",
            "started_at_utc": started,
            "lock_manifest_sha256": expected_lock_manifest_sha256,
            "portfolio_result_observed": False,
            "performance_result_observed": False,
            "retry_allowed": False,
        },
    )

    portfolio = _build_locked_portfolio()
    bundle, evidence_summary, price_lineage = _build_requirements_and_evidence(portfolio)
    execution_preflight = {
        "schema_version": 1,
        "status": "pass_locked_portfolio_and_complete_evidence_before_performance",
        "lock_manifest_sha256": expected_lock_manifest_sha256,
        "route_lock_sha256": sha256_file(artifact_root / "route_lock.json"),
        "prediction_validation": portfolio["prediction_validation"],
        "gate_validation": portfolio["gate_validation"],
        "liquidity_validation": portfolio["liquidity_validation"],
        "portfolio_validation": portfolio["portfolio_validation"],
        "evidence_validation": evidence_summary,
        "matured_years": list(MATURED_YEARS),
        "open_years": list(OPEN_YEARS),
        "open_rows_completed_metric_eligible": False,
        "dgs1mo_available": False,
        "performance_calculated": False,
        "configuration_changed_after_lock": False,
    }
    execution_preflight_path = artifact_root / "support/execution_preflight.json"
    _write_json(execution_preflight_path, execution_preflight)
    state_evidence = artifact_root / "state/03_portfolio_evidence_locked.json"
    _write_json(
        state_evidence,
        {
            "state": "portfolio_and_evidence_locked_before_performance",
            "locked_at_utc": utc_now(),
            "execution_preflight_sha256": sha256_file(execution_preflight_path),
            "portfolio_constructed": True,
            "performance_calculated": False,
            "configuration_changed": False,
            "retry_allowed": False,
        },
    )

    preperformance_frames = {
        "outputs/portfolio/prediction_lineage.parquet": portfolio["m1c_lineage"],
        "outputs/portfolio/p4_consumption_predictions.parquet": portfolio["predictions"],
        "outputs/portfolio/candidates.parquet": portfolio["candidates"],
        "outputs/portfolio/eligibility_inclusion.parquet": portfolio["selection"],
        "outputs/portfolio/gates.parquet": portfolio["gates"],
        "outputs/portfolio/exclusions.parquet": portfolio["exclusions"],
        "outputs/portfolio/holdings.parquet": portfolio["holdings"],
        "outputs/portfolio/periods.parquet": portfolio["periods"],
        "outputs/portfolio/vintage_plan.parquet": portfolio["vintage_plan"],
        "outputs/portfolio/liquidity_evidence.parquet": portfolio["liquidity_evidence"],
        "outputs/portfolio/liquidity_coverage.parquet": portfolio["liquidity_coverage"],
        "outputs/portfolio/liquidity_lineage.parquet": portfolio["liquidity_lineage"],
        "inputs/evaluation/requirements.parquet": bundle.requirements,
        "inputs/evaluation/security_identity.parquet": bundle.security_identity,
        "inputs/evaluation/security_actions.parquet": bundle.security_actions,
        "inputs/evaluation/prices.parquet": bundle.prices,
        "inputs/evaluation/benchmark_requirements.parquet": bundle.benchmark_requirements,
        "inputs/evaluation/coverage.parquet": bundle.coverage,
        "inputs/evaluation/price_lineage.parquet": price_lineage,
    }
    for relative, frame in preperformance_frames.items():
        _write_parquet(artifact_root / relative, frame)
    for namespace, frame in bundle.namespace_eligibility.items():
        _write_parquet(
            artifact_root / f"inputs/evaluation/namespaces/{namespace}/eligibility.parquet",
            frame,
        )
    _write_json(
        artifact_root / "inputs/evaluation/rate_status.json", bundle.rate_status
    )

    results: dict[str, PerformanceResult] = {}
    benchmark_metrics: dict[str, Any] = {}
    metric_frames = []
    coverage_frames = []
    for namespace in PERFORMANCE_NAMESPACES:
        try:
            run_performance_engine(
                bundle,
                performance_namespace=namespace,
                risk_free_namespace=DGS1MO_NAMESPACE,
            )
        except RiskFreeUnavailableError:
            pass
        else:
            raise M1DContractError(f"DGS1MO unexpectedly calculated: {namespace}")
        result = run_performance_engine(
            bundle,
            performance_namespace=namespace,
            risk_free_namespace=ZERO_RATE_NAMESPACE,
        )
        _assert_m1d_result(result)
        results[namespace] = result
        matured_holdings = bundle.requirements[
            bundle.requirements["instrument_role"].eq("holding")
            & bundle.requirements["calendar_exit_timestamp"].notna()
        ]
        benchmark_metrics[namespace] = _benchmark_metrics(result, matured_holdings)
        vintage_benchmark, aggregate_benchmark = benchmark_metrics[namespace]
        metric_frames.append(
            _metric_ledger_dynamic(
                bundle, result, vintage_benchmark, aggregate_benchmark
            )
        )
        coverage_frames.append(_coverage_ledger(bundle, result))
    metrics = pd.concat(metric_frames, ignore_index=True)
    coverage_all = pd.concat(coverage_frames, ignore_index=True)
    comparison, overlap, comparison_summary = _baseline_comparison(
        metrics, portfolio["holdings"]
    )
    stability = _stability_summary(metrics, results)

    primary_result = results[PRIMARY_NAMESPACE]
    aggregate_net_cagr = _metric_value(
        metrics,
        namespace=PRIMARY_NAMESPACE,
        scope="aggregate_strategy",
        basis="net",
        stream="portfolio",
        name="cagr",
    )
    aggregate_net_sharpe = _metric_value(
        metrics,
        namespace=PRIMARY_NAMESPACE,
        scope="aggregate_strategy",
        basis="net",
        stream="portfolio",
        name="sharpe_ratio",
    )
    baseline_metrics = pd.read_parquet(B1E_ROOT / "outputs/metrics.parquet")
    matured_requirements = bundle.requirements[
        bundle.requirements["instrument_role"].eq("holding")
        & bundle.requirements["calendar_exit_timestamp"].notna()
    ]
    primary_metrics = {
        "aggregate_gross_cagr": _metric_value(
            metrics,
            namespace=PRIMARY_NAMESPACE,
            scope="aggregate_strategy",
            basis="gross",
            stream="portfolio",
            name="cagr",
        ),
        "aggregate_net_cagr": aggregate_net_cagr,
        "aggregate_net_zero_rate_sharpe": aggregate_net_sharpe,
        "aggregate_net_annualized_volatility": _metric_value(
            metrics,
            namespace=PRIMARY_NAMESPACE,
            scope="aggregate_strategy",
            basis="net",
            stream="portfolio",
            name="annualized_volatility",
        ),
        "aggregate_net_maximum_drawdown": _metric_value(
            metrics,
            namespace=PRIMARY_NAMESPACE,
            scope="aggregate_strategy",
            basis="net",
            stream="portfolio",
            name="maximum_drawdown",
        ),
        "aggregate_net_maximum_drawdown_duration_months": _metric_value(
            metrics,
            namespace=PRIMARY_NAMESPACE,
            scope="aggregate_strategy",
            basis="net",
            stream="portfolio",
            name="maximum_drawdown_duration_months",
        ),
        "aggregate_net_turnover": _metric_value(
            metrics,
            namespace=PRIMARY_NAMESPACE,
            scope="aggregate_strategy",
            basis="net",
            stream="portfolio",
            name="turnover",
        ),
        "benchmark_net_cagr": _metric_value(
            metrics,
            namespace=PRIMARY_NAMESPACE,
            scope="aggregate_strategy",
            basis="net",
            stream="benchmark",
            name="cagr",
        ),
    }
    threshold_summary = {
        "aggregate_net_cagr_threshold": THRESHOLD_NET_CAGR,
        "aggregate_net_cagr_observed": aggregate_net_cagr,
        "aggregate_net_cagr_met": bool(
            aggregate_net_cagr is not None and aggregate_net_cagr >= THRESHOLD_NET_CAGR
        ),
        "aggregate_net_zero_rate_sharpe_threshold": THRESHOLD_ZERO_RATE_SHARPE,
        "aggregate_net_zero_rate_sharpe_observed": aggregate_net_sharpe,
        "aggregate_net_zero_rate_sharpe_met": bool(
            aggregate_net_sharpe is not None
            and aggregate_net_sharpe >= THRESHOLD_ZERO_RATE_SHARPE
        ),
        "thresholds_used_for_selection_tuning_or_retry": False,
    }
    primary_coverage = primary_result.coverage
    coverage_summary = {
        **evidence_summary,
        "matured_planned_capital": float(
            pd.to_numeric(
                matured_requirements["planned_entry_notional_usd"], errors="coerce"
            ).sum()
        ),
        "required_stock_session_count": int(
            primary_coverage["required_stock_session_count"]
        ),
        "observed_stock_session_count": int(
            primary_coverage["observed_stock_session_count"]
        ),
        "required_benchmark_session_count": int(
            primary_coverage["required_benchmark_session_count"]
        ),
        "observed_benchmark_session_count": int(
            primary_coverage["observed_benchmark_session_count"]
        ),
        "benchmark_gap_count": int(primary_coverage["benchmark_gap_count"]),
        "scenario_imputed_holding_count": int(
            primary_coverage["scenario_imputed_holding_count"]
        ),
        "scenario_imputed_capital": float(
            primary_coverage["scenario_imputed_capital"]
        ),
        "open_2024_2026_in_completed_metrics": False,
    }
    b1e_comparison_summary = {
        **comparison_summary,
        "b1e_aggregate_net_cagr": _metric_value(
            baseline_metrics,
            namespace=PRIMARY_NAMESPACE,
            scope="aggregate_strategy",
            basis="net",
            stream="portfolio",
            name="cagr",
        ),
        "b1e_aggregate_net_zero_rate_sharpe": _metric_value(
            baseline_metrics,
            namespace=PRIMARY_NAMESPACE,
            scope="aggregate_strategy",
            basis="net",
            stream="portfolio",
            name="sharpe_ratio",
        ),
    }
    summary = {
        "schema_version": 1,
        "session": "M1D",
        "version": artifact_root.name,
        "completed_at_utc": utc_now(),
        "primary_namespace": PRIMARY_NAMESPACE,
        "completed_vintage_years": list(MATURED_YEARS),
        "open_vintage_years": list(OPEN_YEARS),
        "primary_metrics": primary_metrics,
        "thresholds": threshold_summary,
        "coverage": coverage_summary,
        "stability": stability,
        "b1e_comparison": b1e_comparison_summary,
        "risk_free_evidence": {
            "dgs1mo_namespace": DGS1MO_NAMESPACE,
            "dgs1mo_available": False,
            "dgs1mo_observation_count": 0,
            "unavailable_reason": "exact_dgs1mo_observations_absent",
            "zero_rate_namespace": ZERO_RATE_NAMESPACE,
            "zero_rate_is_diagnostic_only": True,
            "substitution_or_relabelling_used": False,
        },
        "limitations": [
            "free-source historical research only",
            "not survivorship-complete or provider-certified performance",
            "Yahoo adjusted-close exact corporate-action semantics are uncertified",
            "frozen S1/E1 does not fully cover the M1D selected identities/actions",
            "M1D aggregate starts in 2019 while frozen B1E aggregate starts in 2015",
            "exact DGS1MO ALFRED 2026-07-17 observations are absent",
            "open 2024-2026 outcomes are outside completed historical metrics",
        ],
    }

    output_records: list[tuple[Path, str]] = []
    metrics_path = artifact_root / "outputs/performance/metrics.parquet"
    _write_parquet(metrics_path, metrics)
    output_records.append((metrics_path, "machine_readable_metric_ledger"))
    availability_path = artifact_root / "outputs/performance/availability_reasons.parquet"
    _write_parquet(
        availability_path,
        metrics[metrics["availability_reason"].notna()].reset_index(drop=True),
    )
    output_records.append((availability_path, "machine_readable_metric_availability"))
    coverage_path = artifact_root / "outputs/performance/coverage.parquet"
    _write_parquet(coverage_path, coverage_all)
    output_records.append((coverage_path, "all_namespace_holding_coverage"))
    open_holdings = bundle.requirements[
        bundle.requirements["instrument_role"].eq("holding")
        & bundle.requirements["calendar_exit_timestamp"].isna()
    ].merge(
        bundle.coverage[
            [
                "requirement_id",
                "entry_observed_common",
                "required_month_end_count",
                "observed_common_month_end_count",
                "missing_common_month_end_count",
                "benchmark_gap_count",
                "price_coverage_status",
                "relative_evidence_status",
            ]
        ],
        on="requirement_id",
        validate="one_to_one",
    )
    open_holdings["completed_vintage_metrics_included"] = False
    open_holdings["exclusion_reason"] = "open_2024_2026_vintage_not_matured"
    open_path = artifact_root / "outputs/performance/open_vintages.parquet"
    _write_parquet(open_path, open_holdings)
    output_records.append((open_path, "open_unlabeled_vintages_excluded_from_metrics"))

    comparison_path = artifact_root / "outputs/comparison/b1e_metrics.parquet"
    _write_parquet(comparison_path, comparison)
    output_records.append((comparison_path, "frozen_b1e_metric_comparison"))
    overlap_path = artifact_root / "outputs/comparison/holding_overlap.parquet"
    _write_parquet(overlap_path, overlap)
    output_records.append((overlap_path, "p4_b1e_holding_overlap_stability"))

    namespace_hashes: dict[str, Any] = {}
    for namespace in PERFORMANCE_NAMESPACES:
        result = results[namespace]
        vintage_benchmark, aggregate_benchmark = benchmark_metrics[namespace]
        base = artifact_root / f"outputs/performance/namespaces/{namespace}"
        events, scenarios = _event_and_scenario_ledgers(result)
        frames = {
            "nav/vintage.parquet": result.vintage_nav,
            "nav/aggregate.parquet": result.aggregate_nav,
            "ledgers/positions.parquet": result.holding_ledger,
            "ledgers/rebalances.parquet": result.transaction_ledger,
            "ledgers/costs_turnover.parquet": result.transaction_ledger,
            "ledgers/capital.parquet": result.capital_ledger,
            "ledgers/coverage.parquet": coverage_all[
                coverage_all["performance_namespace"].eq(namespace)
            ].reset_index(drop=True),
            "ledgers/events.parquet": events,
            "ledgers/scenarios.parquet": scenarios,
            "returns/vintage.parquet": result.vintage_nav.assign(
                performance_namespace=namespace
            )[
                [
                    "performance_namespace",
                    "decision_year",
                    "date",
                    "gross_return",
                    "net_return",
                    "benchmark_gross_return",
                    "benchmark_net_return",
                ]
            ],
            "returns/aggregate.parquet": result.aggregate_nav.assign(
                performance_namespace=namespace
            )[
                [
                    "performance_namespace",
                    "date",
                    "gross_return",
                    "net_return",
                    "benchmark_gross_return",
                    "benchmark_net_return",
                ]
            ],
        }
        for outcome in OUTCOME_NAMESPACES:
            frames[f"outcomes/{outcome}.parquet"] = result.outcome_ledgers[outcome]
        for relative, frame in frames.items():
            path = base / relative
            _write_parquet(path, frame)
            output_records.append((path, "physical_namespace_performance_record"))
        portfolio_metrics_path = base / "metrics/portfolio.json"
        _write_json(
            portfolio_metrics_path,
            {
                "performance_namespace": namespace,
                "risk_free_namespace": ZERO_RATE_NAMESPACE,
                "outcome_treatment": _outcome_treatment(bundle, namespace),
                "vintage_metrics": result.vintage_metrics,
                "aggregate_metrics": result.aggregate_metrics,
            },
        )
        output_records.append((portfolio_metrics_path, "portfolio_metric_payload"))
        benchmark_metrics_path = base / "metrics/benchmark.json"
        _write_json(
            benchmark_metrics_path,
            {
                "performance_namespace": namespace,
                "risk_free_namespace": ZERO_RATE_NAMESPACE,
                "outcome_treatment": _outcome_treatment(bundle, namespace),
                "vintage_metrics": vintage_benchmark,
                "aggregate_metrics": aggregate_benchmark,
            },
        )
        output_records.append((benchmark_metrics_path, "benchmark_metric_payload"))
        namespace_metrics = metrics[metrics["performance_namespace"].eq(namespace)]
        zero_path = base / f"rates/{ZERO_RATE_NAMESPACE}/metrics.parquet"
        _write_parquet(
            zero_path,
            namespace_metrics[
                namespace_metrics["risk_free_namespace"].eq(ZERO_RATE_NAMESPACE)
            ].reset_index(drop=True),
        )
        output_records.append((zero_path, "zero_rate_diagnostic_metrics"))
        dgs_path = base / f"rates/{DGS1MO_NAMESPACE}/unavailable_metrics.parquet"
        _write_parquet(
            dgs_path,
            namespace_metrics[
                namespace_metrics["risk_free_namespace"].eq(DGS1MO_NAMESPACE)
            ].reset_index(drop=True),
        )
        output_records.append((dgs_path, "dgs1mo_fail_closed_metrics"))
        namespace_hashes[namespace] = {
            "aggregate_nav_sha256": sha256_file(base / "nav/aggregate.parquet"),
            "vintage_nav_sha256": sha256_file(base / "nav/vintage.parquet"),
            "positions_sha256": sha256_file(base / "ledgers/positions.parquet"),
            "rebalances_sha256": sha256_file(base / "ledgers/rebalances.parquet"),
            "scenario_ledger_sha256": sha256_file(base / "ledgers/scenarios.parquet"),
            "outcome_hashes": {
                outcome: sha256_file(base / f"outcomes/{outcome}.parquet")
                for outcome in OUTCOME_NAMESPACES
            },
        }

    weights_path = artifact_root / "outputs/performance/weights.parquet"
    _write_parquet(
        weights_path,
        matured_requirements[
            [
                "requirement_id",
                "stable_row_id",
                "ticker",
                "decision_year",
                "weight",
                "planned_vintage_aum_usd",
                "planned_entry_notional_usd",
                "entry_timestamp",
                "calendar_exit_timestamp",
            ]
        ].reset_index(drop=True),
    )
    output_records.append((weights_path, "completed_vintage_position_weight_ledger"))
    summary_path = artifact_root / "support/summary.json"
    _write_json(summary_path, summary)
    output_records.append((summary_path, "coverage_first_m1d_summary"))
    report_path = artifact_root / "report/m1d_report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_m1d_report(summary))
    output_records.append((report_path, "human_readable_m1d_report"))

    state_completed = artifact_root / "state/04_evaluation_completed.json"
    _write_json(
        state_completed,
        {
            "state": "one_locked_m1d_evaluation_completed",
            "completed_at_utc": summary["completed_at_utc"],
            "lock_manifest_sha256": expected_lock_manifest_sha256,
            "configuration_changed_after_lock": False,
            "adaptive_retry_performed": False,
            "successful_evaluation_count": 1,
            "threshold_miss_authorizes_retry": False,
        },
    )
    output_records.append((state_completed, "one_shot_completion_state"))

    all_files = sorted(
        path
        for path in artifact_root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    )
    role_by_path = {
        path.resolve(): role for path, role in output_records
    }
    manifest = {
        "schema_version": 1,
        "artifact_class": ARTIFACT_CLASS,
        "version": artifact_root.name,
        "created_at_utc": started,
        "completed_at_utc": summary["completed_at_utc"],
        "build_mode": "offline_one_shot_locked_m1c_p4_b1e_comparison",
        "current_head": _git_head(),
        "lock_manifest_sha256": expected_lock_manifest_sha256,
        "preflight_sha256": sha256_file(artifact_root / "preflight.json"),
        "route_lock_sha256": sha256_file(artifact_root / "route_lock.json"),
        "execution_preflight_sha256": sha256_file(execution_preflight_path),
        "frozen_boundaries": _verify_boundary_hashes()["boundaries"],
        "configuration": locked.route,
        "configuration_sha256": _hash_payload(locked.route),
        "namespace_hashes": namespace_hashes,
        "records": [
            _record(
                artifact_root,
                path,
                role_by_path.get(path.resolve(), "m1d_input_lineage_or_output"),
            )
            for path in all_files
        ],
        "code_lineage": _code_lineage(),
        "summary": summary,
        "claim": {
            "m1d_complete": True,
            "successful_evaluation_count": 1,
            "route_locked_before_portfolio_or_performance": True,
            "outer_oos_or_b1e_metric_used_for_route_selection": False,
            "p4_rules_changed": False,
            "b1d_b1e_evaluation_contract_changed": False,
            "open_2024_2026_excluded_from_completed_metrics": True,
            "all_physical_namespaces_materialized": True,
            "dgs1mo_metrics_available": False,
            "zero_rate_diagnostic_available": True,
            "adaptive_retry_performed": False,
            "external_data_collected": False,
            "survivorship_complete": False,
            "provider_certified": False,
        },
        "limitations": summary["limitations"],
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "numpy": np.__version__,
            "pandas": pd.__version__,
        },
    }
    manifest_path = artifact_root / "manifest.json"
    _write_json(manifest_path, manifest)
    verify_m1d_artifact(
        artifact_root,
        expected_manifest_sha256=sha256_file(manifest_path),
        expected_lock_manifest_sha256=expected_lock_manifest_sha256,
    )
    return manifest_path


def verify_m1d_artifact(
    artifact_root: Path,
    *,
    expected_manifest_sha256: str | None = None,
    expected_lock_manifest_sha256: str | None = None,
) -> Mapping[str, Any]:
    """Independently rehash every M1D record and reproduce its report."""
    artifact_root = artifact_root.resolve()
    manifest_path = artifact_root / "manifest.json"
    if not manifest_path.is_file():
        raise M1DContractError("M1D manifest is missing")
    actual_hash = sha256_file(manifest_path)
    if expected_manifest_sha256 and actual_hash != expected_manifest_sha256:
        raise M1DContractError("M1D manifest hash mismatch")
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("artifact_class") != ARTIFACT_CLASS:
        raise M1DContractError("M1D manifest identity drifted")
    records = manifest.get("records", [])
    discovered = {
        path.relative_to(artifact_root).as_posix()
        for path in artifact_root.rglob("*")
        if path.is_file() and path != manifest_path
    }
    paths = {item.get("path") for item in records}
    if discovered != paths or len(paths) != len(records):
        raise M1DContractError("M1D manifest does not enumerate every record")
    record_summary = _verify_records(artifact_root, records, "M1D")
    lock_hash = sha256_file(artifact_root / "lock_manifest.json")
    expected_lock = expected_lock_manifest_sha256 or manifest.get(
        "lock_manifest_sha256"
    )
    if lock_hash != expected_lock or manifest.get("lock_manifest_sha256") != expected_lock:
        raise M1DContractError("M1D immutable lock lineage drifted")
    if manifest.get("configuration_sha256") != _hash_payload(
        manifest.get("configuration")
    ):
        raise M1DContractError("M1D configuration payload drifted")
    for item in manifest.get("code_lineage", []):
        path = ROOT / item["path"]
        if (
            path.stat().st_size != item["size_bytes"]
            or sha256_file(path) != item["sha256"]
        ):
            raise M1DContractError(f"M1D code lineage drifted: {item['path']}")
    summary = json.loads((artifact_root / "support/summary.json").read_text())
    report = (artifact_root / "report/m1d_report.md").read_text()
    if report != render_m1d_report(summary):
        raise M1DContractError("M1D report does not reproduce from summary")
    metrics = pd.read_parquet(artifact_root / "outputs/performance/metrics.parquet")
    if set(metrics["performance_namespace"]) != set(PERFORMANCE_NAMESPACES):
        raise M1DContractError("M1D performance namespace set drifted")
    if set(metrics["risk_free_namespace"]) != {
        DGS1MO_NAMESPACE,
        ZERO_RATE_NAMESPACE,
    }:
        raise M1DContractError("M1D rate namespace set drifted")
    dgs = metrics[metrics["risk_free_namespace"].eq(DGS1MO_NAMESPACE)]
    if dgs["metric_value"].notna().any() or not dgs["availability_reason"].eq(
        "exact_dgs1mo_observations_absent"
    ).all():
        raise M1DContractError("M1D DGS1MO rows did not fail closed")
    open_holdings = pd.read_parquet(
        artifact_root / "outputs/performance/open_vintages.parquet"
    )
    if (
        len(open_holdings) != 45
        or set(pd.to_numeric(open_holdings["decision_year"]).astype(int))
        != set(OPEN_YEARS)
        or open_holdings["completed_vintage_metrics_included"].astype(bool).any()
    ):
        raise M1DContractError("M1D open/unlabeled outcome boundary drifted")
    selection = pd.read_parquet(
        artifact_root / "outputs/portfolio/eligibility_inclusion.parquet"
    )
    if len(selection) != 43_806 or int(selection["holding"].sum()) != 120:
        raise M1DContractError("M1D row-complete portfolio selection drifted")
    _verify_boundary_hashes()
    return {
        "manifest_sha256": actual_hash,
        **record_summary,
        "metric_rows": len(metrics),
        "open_holding_rows": len(open_holdings),
        "portfolio_candidate_rows": len(selection),
        "verification_status": "all_records_lineage_namespaces_and_report_verified",
    }
