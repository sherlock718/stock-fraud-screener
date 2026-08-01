"""Consolidate and verify the frozen free-data V1 release candidate.

US1C is a metadata-and-verification derivative.  It verifies the exact
P2 -> P3 -> P4 -> US1A -> US1B chain, packages unchanged release-critical
records, and records a read-only recovery result for the immutable C2 Hugging
Face revision.  It never rebuilds a model, score, rank, portfolio, event
decision, or performance result.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping
import hashlib
import json
import shutil
import tempfile

import pandas as pd

from _root import ROOT
from data_io.canonical_hf import (
    DEFAULT_POINTER_DIR,
    load_pointer_documents,
)
from portfolio.build_us_free_product import verify_product_artifact
from portfolio.us1b_frozen_evidence import verify_artifact as verify_us1b_artifact


CONTRACT_PATH = ROOT / "docs/US1C_RELEASE_CONSOLIDATION_CONTRACT.json"
CONTRACT_SHA256 = (
    "d7098153542afa77ff09e850980362eb957b3e24cd609a4984451289595f1d47"
)
DEFAULT_VERSION = "20260801T210000Z-us1c"
DEFAULT_ARTIFACT_ROOT = (
    ROOT / "artifacts/product/us_free_v1_release_candidate" / DEFAULT_VERSION
)
M1D_ROOT = ROOT / "artifacts/performance/m1d/20260801T162953Z-m1d"
M1D_LOCK_MANIFEST = M1D_ROOT / "lock_manifest.json"
M1D_LOCK_MANIFEST_SHA256 = (
    "757e19cd9e35290a6b339f79e2c44a0f1ddb47c03b913930b1abda84f0bf74bc"
)
FROZEN_ARTIFACTS: dict[str, tuple[str, str]] = {
    "B1E": (
        "artifacts/performance/free_data_v1/20260801T011135Z-b1e",
        "23588207812e2e950d3e521c6f2048e7607f20b99b691407c449ceb7752bf37c",
    ),
    "M1A": (
        "artifacts/modeling/nested_walk_forward/20260801T000000Z-m1a",
        "a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc",
    ),
    "M1C": (
        "artifacts/modeling/nested_walk_forward/20260801T121426Z-m1c",
        "125cb5d6a8012c3b03ee6eab5f00ac944135c39996e992ecf40d8816acdacc58",
    ),
    "M1D": (
        "artifacts/performance/m1d/20260801T162953Z-m1d",
        "b04cea8236da6cd92749410f6186360be5f29dbbbeb00a64f2ed07c180cc72ab",
    ),
    "I1": (
        "artifacts/international/i1/20260801T180000Z-i1-ca",
        "d3f17854cf0839713163e8dda99aefedd6ba064a14a042a886770818a472d9f6",
    ),
}
B1D_ENGINE = ROOT / "backtest/free_data_v1_nav.py"
B1D_TEST = ROOT / "tests/backtest/test_free_data_v1_nav.py"
US1A_ROOT = ROOT / "artifacts/product/us_free_v1/20260801T183000Z-us1a"
US1B_ROOT = (
    ROOT / "artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b"
)
PARTIAL_M1C_PARENT = ROOT / "artifacts/modeling/nested_walk_forward"
M1C_ACCEPTED_VERSION = "20260801T121426Z-m1c"
PARTIAL_OUTER_RESULT_NAMES = {
    "outer_oos_predictions.parquet",
    "outer_oos_metrics.json",
    "candidate_evaluations.json",
    "winner_decisions.json",
    "manifest.json",
}


class US1CContractError(RuntimeError):
    """Raised when a consolidation or recovery boundary cannot be proven."""


@dataclass(frozen=True)
class ManifestVerification:
    manifest_sha256: str
    record_count: int
    record_bytes: int
    file_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "manifest_sha256": self.manifest_sha256,
            "record_count": self.record_count,
            "record_bytes": self.record_bytes,
            "file_count": self.file_count,
        }


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _copy_exact(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as output, source.open("rb") as input_file:
        shutil.copyfileobj(input_file, output)
    if (
        destination.stat().st_size != source.stat().st_size
        or sha256_file(destination) != sha256_file(source)
    ):
        raise US1CContractError(f"copied record mismatch: {source}")


def load_contract() -> dict[str, Any]:
    if not CONTRACT_PATH.is_file() or sha256_file(CONTRACT_PATH) != CONTRACT_SHA256:
        raise US1CContractError("frozen US1C contract SHA-256 mismatch")
    contract = json.loads(CONTRACT_PATH.read_text())
    if (
        contract.get("artifact_version") != DEFAULT_VERSION
        or contract.get("release_performed") is not False
        or contract.get("reconstruction_contract", {}).get(
            "model_or_methodology_reexecution"
        )
        is not False
        or contract.get("recovery_contract", {}).get(
            "mutable_revision_fallback"
        )
        is not False
    ):
        raise US1CContractError("frozen US1C contract identity drifted")
    return contract


def _record_local_path(
    artifact_root: Path,
    record_path: str,
    source_prefix: str | None,
) -> tuple[Path, str]:
    pure = PurePosixPath(record_path)
    if pure.is_absolute() or ".." in pure.parts:
        raise US1CContractError(f"unsafe manifest record path: {record_path}")
    local = pure
    if source_prefix:
        prefix = PurePosixPath(source_prefix)
        try:
            local = pure.relative_to(prefix)
        except ValueError:
            pass
    path = artifact_root.joinpath(*local.parts)
    return path, local.as_posix()


def verify_manifest_records(
    artifact_root: Path,
    *,
    expected_manifest_sha256: str,
    expected_record_count: int | None = None,
    expected_record_bytes: int | None = None,
    source_prefix: str | None = None,
    manifest_name: str = "manifest.json",
    require_exact_file_set: bool = True,
) -> ManifestVerification:
    manifest_path = artifact_root / manifest_name
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise US1CContractError(f"manifest missing or is a symlink: {manifest_path}")
    actual_manifest_sha256 = sha256_file(manifest_path)
    if actual_manifest_sha256 != expected_manifest_sha256:
        raise US1CContractError(
            f"manifest mismatch: {manifest_path} expected="
            f"{expected_manifest_sha256} actual={actual_manifest_sha256}"
        )
    manifest = json.loads(manifest_path.read_text())
    records = manifest.get("records")
    if not isinstance(records, list):
        raise US1CContractError(f"manifest records are invalid: {manifest_path}")
    declared: set[str] = set()
    record_bytes = 0
    for record in records:
        record_path = str(record.get("path"))
        path, local_path = _record_local_path(
            artifact_root,
            record_path,
            source_prefix,
        )
        if local_path in declared:
            raise US1CContractError(f"duplicate manifest record: {local_path}")
        declared.add(local_path)
        expected_size = int(record.get("size_bytes", -1))
        expected_sha = str(record.get("sha256"))
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != expected_size
            or sha256_file(path) != expected_sha
        ):
            raise US1CContractError(f"manifest record mismatch: {path}")
        record_bytes += expected_size
    if expected_record_count is not None and len(records) != expected_record_count:
        raise US1CContractError(
            f"record count mismatch for {manifest_path}: "
            f"expected={expected_record_count} actual={len(records)}"
        )
    if expected_record_bytes is not None and record_bytes != expected_record_bytes:
        raise US1CContractError(
            f"record byte mismatch for {manifest_path}: "
            f"expected={expected_record_bytes} actual={record_bytes}"
        )
    if require_exact_file_set:
        discovered = {
            path.relative_to(artifact_root).as_posix()
            for path in artifact_root.rglob("*")
            if path.is_file()
        }
        expected_files = declared | {manifest_name}
        if discovered != expected_files:
            raise US1CContractError(
                f"manifest/root file-set mismatch for {artifact_root}: "
                f"unexpected={sorted(discovered - expected_files)} "
                f"missing={sorted(expected_files - discovered)}"
            )
    return ManifestVerification(
        manifest_sha256=actual_manifest_sha256,
        record_count=len(records),
        record_bytes=record_bytes,
        file_count=len(records) + 1,
    )


def _assert_exact_file(source: Path, copy: Path, label: str) -> None:
    if (
        not source.is_file()
        or not copy.is_file()
        or source.stat().st_size != copy.stat().st_size
        or sha256_file(source) != sha256_file(copy)
    ):
        raise US1CContractError(f"{label} exact copy mismatch")


def _partial_m1c_inventory() -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    for directory in sorted(PARTIAL_M1C_PARENT.glob("*-m1c")):
        if directory.name == M1C_ACCEPTED_VERSION or not directory.is_dir():
            continue
        files = [path for path in sorted(directory.rglob("*")) if path.is_file()]
        records = [
            {
                "path": path.relative_to(directory).as_posix(),
                "role": "preserved_partial_attempt",
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for path in files
        ]
        names = {record["path"] for record in records}
        attempts.append(
            {
                "artifact_directory": directory.name,
                "file_count": len(records),
                "model_file_count": sum(
                    name.endswith("model.joblib") for name in names
                ),
                "outer_result_observed": bool(names & PARTIAL_OUTER_RESULT_NAMES),
                "record_bytes": sum(record["size_bytes"] for record in records),
                "records_sha256": hashlib.sha256(
                    json.dumps(records, sort_keys=True).encode()
                ).hexdigest(),
            }
        )
    return attempts


def _verify_frozen_boundaries(contract: Mapping[str, Any]) -> dict[str, Any]:
    frozen = contract["frozen_boundaries"]
    results: dict[str, Any] = {}
    for stage, (relative_root, expected_manifest) in FROZEN_ARTIFACTS.items():
        result = verify_manifest_records(
            ROOT / relative_root,
            expected_manifest_sha256=expected_manifest,
            source_prefix=relative_root,
        )
        results[stage] = result.as_dict()
    lock = verify_manifest_records(
        M1D_ROOT,
        expected_manifest_sha256=M1D_LOCK_MANIFEST_SHA256,
        manifest_name="lock_manifest.json",
        require_exact_file_set=False,
    )
    results["M1D_LOCK"] = lock.as_dict()
    engine_sha = sha256_file(B1D_ENGINE)
    test_sha = sha256_file(B1D_TEST)
    if (
        engine_sha != frozen["b1d_engine_sha256"]
        or test_sha != frozen["b1d_test_sha256"]
    ):
        raise US1CContractError("B1D engine/test boundary drifted")
    results["B1D"] = {
        "engine_sha256": engine_sha,
        "test_sha256": test_sha,
    }
    partial = _partial_m1c_inventory()
    if partial != frozen["partial_m1c_attempts"]:
        raise US1CContractError("five preserved partial M1C attempts drifted")
    if len(partial) != 5 or any(row["outer_result_observed"] for row in partial):
        raise US1CContractError("partial M1C outer-result boundary drifted")
    results["partial_m1c_attempts"] = partial
    return results


def _verify_d1_identity_boundary() -> dict[str, Any]:
    p2 = pd.read_parquet(
        ROOT
        / "artifacts/canonical/corrected_us_annual/outputs/observed_only/"
        "features_taxonomy.parquet",
        columns=["stable_row_id"],
    )
    d1 = pd.read_parquet(
        ROOT
        / "artifacts/canonical_refresh/us/20260730T110301Z/"
        "p2_review_candidate/outputs/observed_only/features_taxonomy.parquet",
        columns=["stable_row_id"],
    )
    m1c = pd.read_parquet(
        ROOT
        / "artifacts/modeling/nested_walk_forward/20260801T121426Z-m1c/"
        "outer_oos_predictions.parquet",
        columns=["stable_row_id"],
    )
    p2_ids = set(p2["stable_row_id"].astype(str))
    d1_ids = set(d1["stable_row_id"].astype(str))
    m1c_ids = set(m1c["stable_row_id"].astype(str))
    result = {
        "accepted_p2_ids": len(p2_ids),
        "d1_ids": len(d1_ids),
        "shared_ids": len(p2_ids & d1_ids),
        "p2_only_ids": len(p2_ids - d1_ids),
        "d1_only_ids": len(d1_ids - p2_ids),
        "m1c_matches_p2": m1c_ids == p2_ids,
        "m1c_matches_d1": m1c_ids == d1_ids,
        "status": "unsupported_for_frozen_M1C",
    }
    if result != {
        "accepted_p2_ids": 43_806,
        "d1_ids": 43_640,
        "shared_ids": 43_564,
        "p2_only_ids": 242,
        "d1_only_ids": 76,
        "m1c_matches_p2": True,
        "m1c_matches_d1": False,
        "status": "unsupported_for_frozen_M1C",
    }:
        raise US1CContractError("D1/frozen-M1C stable-ID boundary drifted")
    return result


def _verify_shortlist_and_evidence() -> dict[str, Any]:
    original = pd.read_parquet(
        US1A_ROOT / "outputs/final_shortlist_2026.parquet"
    ).sort_values("rank").reset_index(drop=True)
    frozen = pd.read_parquet(
        US1B_ROOT / "inputs/frozen_shortlist_2026.parquet"
    ).sort_values("rank").reset_index(drop=True)
    derivative = pd.read_parquet(
        US1B_ROOT / "outputs/final_shortlist_2026_evidence_derivative.parquet"
    ).sort_values("rank").reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(frozen, original, check_exact=True)
        pd.testing.assert_frame_equal(
            derivative[list(original.columns)],
            original,
            check_exact=True,
        )
    except AssertionError as exc:
        raise US1CContractError(
            "US1A/US1B identity, score, gate, liquidity, rank, or weight drifted"
        ) from exc
    if (
        len(original) != 15
        or list(pd.to_numeric(original["rank"]).astype(int))
        != list(range(1, 16))
        or not original["holding"].astype(bool).all()
        or not original["weight"].map(
            lambda value: abs(float(value) - 1 / 15) <= 1e-15
        ).all()
    ):
        raise US1CContractError("US1A exact 15-name holding boundary drifted")
    names = pd.read_parquet(
        US1B_ROOT / "outputs/name_level_adjudication.parquet"
    ).sort_values("rank").reset_index(drop=True)
    states = Counter(names["coverage_state"].astype(str))
    failed = names.loc[names["coverage_state"].eq("failed_request"), "ticker"]
    if (
        states != {"unresolved": 14, "failed_request": 1}
        or failed.tolist() != ["HPK"]
        or not names["deterministic_action"].eq("unresolved").all()
        or not names["human_review_required"].astype(bool).all()
        or names[["holding_changed", "rank_changed", "weight_changed"]]
        .astype(bool)
        .any()
        .any()
    ):
        raise US1CContractError("US1B final evidence state drifted")
    return {
        "shortlist_rows": len(original),
        "ticker_rank_order": original["ticker"].astype(str).tolist(),
        "all_parent_columns_exact": True,
        "all_holdings": True,
        "all_weights_exact_one_fifteenth": True,
        "name_states": dict(sorted(states.items())),
        "failed_request_name": "HPK",
        "all_deterministic_actions": "unresolved",
        "all_names_require_human_review": True,
    }


def verify_release_chain() -> dict[str, Any]:
    """Verify the complete frozen local route and all US1C preservation gates."""
    contract = load_contract()
    stages: list[dict[str, Any]] = []
    by_stage = {item["stage"]: item for item in contract["chain"]}
    for stage in ("P2", "P3", "P4", "US1A", "US1B"):
        item = by_stage[stage]
        result = verify_manifest_records(
            ROOT / item["artifact_root"],
            expected_manifest_sha256=item["manifest_sha256"],
            expected_record_count=int(item["record_count"]),
            expected_record_bytes=int(item["record_bytes"]),
            source_prefix=item["artifact_root"],
        )
        stages.append({"stage": stage, "status": "verified", **result.as_dict()})

    _assert_exact_file(
        ROOT / by_stage["P2"]["artifact_root"] / "manifest.json",
        ROOT
        / by_stage["P3"]["artifact_root"]
        / "inputs/canonical_p2_manifest.json",
        "P2 -> P3 manifest lineage",
    )
    _assert_exact_file(
        ROOT / by_stage["P3"]["artifact_root"] / "manifest.json",
        ROOT
        / by_stage["P4"]["artifact_root"]
        / "inputs/canonical_p3_manifest.json",
        "P3 -> P4 manifest lineage",
    )
    _assert_exact_file(
        ROOT / by_stage["P2"]["artifact_root"] / "manifest.json",
        US1A_ROOT / "inputs/p2_manifest.json",
        "P2 -> US1A manifest lineage",
    )
    _assert_exact_file(
        ROOT / by_stage["P4"]["artifact_root"] / "manifest.json",
        US1A_ROOT / "inputs/p4_manifest.json",
        "P4 -> US1A manifest lineage",
    )
    _assert_exact_file(
        US1A_ROOT / "manifest.json",
        US1B_ROOT / "inputs/us1a_manifest.json",
        "US1A -> US1B manifest lineage",
    )
    us1a = verify_product_artifact(
        US1A_ROOT,
        expected_manifest_sha256=by_stage["US1A"]["manifest_sha256"],
        reverify_preserved_boundaries=False,
    )
    us1b = verify_us1b_artifact(
        US1B_ROOT,
        expected_manifest_sha256=by_stage["US1B"]["manifest_sha256"],
        reverify_boundaries=False,
    )
    evidence = contract["evidence_boundary"]
    for key, value in {
        "external_unique_urls": evidence["approved_unique_urls"],
        "external_attempts": evidence["external_attempts"],
        "external_http_200": evidence["external_http_200"],
        "external_response_body_bytes": evidence["external_response_body_bytes"],
        "raw_reused_responses_verified": evidence["reused_responses"],
        "unresolved_names": evidence["unresolved_names"],
        "failed_request_names": evidence["failed_request_names"],
    }.items():
        if us1b.get(key) != value:
            raise US1CContractError(f"US1B evidence boundary drifted: {key}")
    return {
        "status": "pass",
        "route": "P2 -> P3 -> P4 -> US1A -> US1B",
        "contract_sha256": CONTRACT_SHA256,
        "stages": stages,
        "shortlist": _verify_shortlist_and_evidence(),
        "d1_m1c_identity_boundary": _verify_d1_identity_boundary(),
        "frozen_boundaries": _verify_frozen_boundaries(contract),
        "us1a_record_count": us1a["record_count"],
        "us1a_record_bytes": us1a["record_bytes"],
        "us1b_record_count": us1b["record_count"],
        "us1b_record_bytes": us1b["record_bytes"],
        "external_data_collected": False,
        "model_or_methodology_executed": False,
        "performance_calculated": False,
        "release_performed": False,
    }


def _pointer_summary() -> dict[str, Any]:
    pointers = load_pointer_documents(DEFAULT_POINTER_DIR)
    revision = {item["revision"] for item in pointers.values()}
    repository = {item["repository"] for item in pointers.values()}
    if len(revision) != 1 or len(repository) != 1:
        raise US1CContractError("canonical pointers do not share one revision")
    return {
        "repository": next(iter(repository)),
        "repository_type": "dataset",
        "revision": next(iter(revision)),
        "artifact_count": len(pointers),
        "verified_file_count": sum(item["file_count"] for item in pointers.values()),
        "verified_size_bytes": sum(
            item["total_size_bytes"] for item in pointers.values()
        ),
        "pointer_manifests": {
            item["stage"]: item["manifest_sha256"] for item in pointers.values()
        },
    }


def verify_recovered_canonical_root(recovery_root: Path) -> dict[str, Any]:
    """Verify a clean C2 download without consulting mutable state."""
    contract = load_contract()
    pointers = load_pointer_documents(DEFAULT_POINTER_DIR)
    by_stage = {item["stage"]: item for item in contract["chain"]}
    stages: dict[str, Any] = {}
    for document in pointers.values():
        stage = str(document["stage"])
        item = by_stage[stage]
        artifact_root = recovery_root / str(document["artifact_name"])
        result = verify_manifest_records(
            artifact_root,
            expected_manifest_sha256=item["manifest_sha256"],
            expected_record_count=int(item["record_count"]),
            expected_record_bytes=int(item["record_bytes"]),
            source_prefix=item["artifact_root"],
        )
        stages[stage] = result.as_dict()
    summary = _pointer_summary()
    if summary != {
        "repository": contract["recovery_contract"]["repository"],
        "repository_type": contract["recovery_contract"]["repository_type"],
        "revision": contract["recovery_contract"]["revision"],
        "artifact_count": contract["recovery_contract"]["artifact_count"],
        "verified_file_count": contract["recovery_contract"][
            "expected_file_count"
        ],
        "verified_size_bytes": contract["recovery_contract"][
            "expected_total_size_bytes"
        ],
        "pointer_manifests": {
            "P2": by_stage["P2"]["manifest_sha256"],
            "P3": by_stage["P3"]["manifest_sha256"],
            "P4": by_stage["P4"]["manifest_sha256"],
        },
    }:
        raise US1CContractError("immutable recovery pointer boundary drifted")
    return {"status": "pass", **summary, "recovered_stages": stages}


def validate_recovery_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    contract = load_contract()["recovery_contract"]
    expected = {
        "status": "pass",
        "repository": contract["repository"],
        "repository_type": contract["repository_type"],
        "revision": contract["revision"],
        "artifact_count": contract["artifact_count"],
        "verified_file_count": contract["expected_file_count"],
        "verified_size_bytes": contract["expected_total_size_bytes"],
        "visibility": "private_verified",
        "mutable_revision_fallback": False,
        "remote_operation": "read_only",
        "remote_state_mutated": False,
    }
    for key, value in expected.items():
        if evidence.get(key) != value:
            raise US1CContractError(
                f"recovery evidence mismatch: {key} expected={value!r} "
                f"actual={evidence.get(key)!r}"
            )
    recovered = evidence.get("recovered_stages")
    if not isinstance(recovered, Mapping) or set(recovered) != {"P2", "P3", "P4"}:
        raise US1CContractError("recovery stage evidence is incomplete")
    chain = {item["stage"]: item for item in load_contract()["chain"]}
    for stage in ("P2", "P3", "P4"):
        if recovered[stage].get("manifest_sha256") != chain[stage][
            "manifest_sha256"
        ]:
            raise US1CContractError(f"recovered {stage} manifest mismatch")
    return json.loads(json.dumps(dict(evidence), sort_keys=True))


def recovery_evidence_from_result(
    retrieval_result: Mapping[str, Any],
    recovered_verification: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize an authenticated retrieval result into stable US1C evidence."""
    evidence = {
        "status": recovered_verification["status"],
        "repository": retrieval_result.get("repository"),
        "repository_type": retrieval_result.get("repository_type"),
        "revision": retrieval_result.get("revision"),
        "artifact_count": retrieval_result.get("artifact_count"),
        "verified_file_count": retrieval_result.get("verified_file_count"),
        "verified_size_bytes": retrieval_result.get("verified_size_bytes"),
        "visibility": retrieval_result.get("visibility"),
        "mutable_revision_fallback": retrieval_result.get(
            "mutable_revision_fallback"
        ),
        "remote_operation": "read_only",
        "remote_state_mutated": False,
        "recovered_stages": recovered_verification["recovered_stages"],
    }
    return validate_recovery_evidence(evidence)


def _command_inventory() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "primary_local_chain_verification": "python3 -m workflows.run_us_free_v1",
        "offline_release_candidate_verification": (
            "python3 -m workflows.run_us_free_v1 --us1c-artifact-root "
            "artifacts/product/us_free_v1_release_candidate/"
            "20260801T210000Z-us1c"
        ),
        "read_only_immutable_recovery": (
            "python3 -m workflows.run_us_free_v1 --recover-canonical "
            "--recovery-target <absent-path> --recovery-evidence-output "
            "<absent-json>"
        ),
        "fresh_release_candidate_assembly": (
            "python3 -m workflows.run_us_free_v1 --build-us1c "
            "--recovery-evidence <verified-json> --us1c-artifact-root "
            "<absent-path>"
        ),
        "historical_stage_verifiers": [
            "python3 -m workflows.run_canonical",
            (
                "python3 -m portfolio.build_us_free_product --artifact-root "
                "artifacts/product/us_free_v1/20260801T183000Z-us1a "
                "--verify-only --expected-manifest-sha256 "
                "f27773c0e1fb92a25707e0fec363e13afa7ccb10f8ace7537187c6be58575edf"
            ),
            (
                "python3 -m portfolio.us1b_frozen_evidence --artifact-root "
                "artifacts/product/us_free_v1_evidence/"
                "20260801T193322Z-us1b --verify-only "
                "--expected-manifest-sha256 "
                "f802ec26358d27afa21c99490e598e13c98a6ad60229705769f3fae1818d9dcd"
            ),
        ],
        "network_used_by_primary_or_offline_commands": False,
        "recovery_mutates_remote_state": False,
    }


def _limitations() -> dict[str, Any]:
    return {
        "research_only": True,
        "not_personalized_investment_advice": True,
        "no_future_performance_promise": True,
        "free_source_survivorship_complete": False,
        "provider_certified": False,
        "exact_dgs1mo_metrics_available": False,
        "adjusted_close_semantics_certified": False,
        "event_states": "14 unresolved and HPK failed-request; all require human review",
        "d1_release_status": "unsupported_for_frozen_M1C",
        "recovery_scope": "immutable C2 P2-P4 baseline only; US1A/US1B remain local frozen derivatives",
        "release_performed": False,
    }


def _render_limitations(limitations: Mapping[str, Any]) -> str:
    return f"""# US1C Limitations

- This is a local release candidate for research, not personalized investment advice or a future-performance promise.
- Free-source survivorship coverage is not complete and the result is not provider-certified.
- Exact DGS1MO-dependent metrics and certified adjusted-close semantics remain unavailable.
- Event evidence remains 14 unresolved names plus HPK failed-request; every name requires human review and every deterministic action remains unresolved.
- D1 remains unsupported for frozen M1C because 242 stable IDs exist only in accepted P2 and 76 exist only in D1.
- Immutable remote recovery covers the published C2 P2-P4 baseline. US1A and US1B are verified local frozen derivatives and are not published by US1C.
- US1C performs no release, publication, promotion, archive, model, ranking, portfolio, evidence-collection, or performance action.
"""


def _render_report(
    chain: Mapping[str, Any],
    recovery: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
) -> str:
    stages = " -> ".join(item["stage"] for item in chain["stages"])
    return f"""# US Free-Data V1 Release Candidate — US1C

Status: **locally verified, not released**.

## Canonical route

`python3 -m workflows.run_us_free_v1`

The verified route is `{stages}`. US1A contains exactly 32 records and 37,863,802 recorded bytes. US1B contains exactly 95 records and 2,653,162 recorded bytes.

US1B retains all US1A identities, scores, gates, liquidity decisions, holdings, ranks, and 1/15 weights exactly. Its frozen request plan is `f1bcfc5d2c740d1626c3d6c11148ee1711ec5d70911165b897daf84f90f9bfbc`: 36 approved URLs, 36 attempts, 36 HTTP 200 responses, 3,681,377 response-body bytes, and 29 reused responses. Fourteen names remain unresolved; HPK remains failed-request; all 15 require human review and keep unresolved deterministic actions.

## Reconstruction and recovery

US1C was assembled twice in fresh destinations and exposed only after every file and the manifest matched byte for byte. This was metadata and exact-copy reconstruction only; no model, score, rank, shortlist, portfolio, event decision, or performance result was rerun.

The private dataset repository `{recovery['repository']}` was read at immutable revision `{recovery['revision']}` only. Recovery verified {recovery['verified_file_count']:,} P2-P4 files and {recovery['verified_size_bytes']:,} bytes with no mutable revision fallback and no remote mutation.

## Fail-closed boundary

D1 remains unsupported for frozen M1C: {diagnostics['d1_m1c_identity_boundary']['p2_only_ids']} P2-only and {diagnostics['d1_m1c_identity_boundary']['d1_only_ids']} D1-only stable IDs. Free-source survivorship coverage, adjusted-close certification, and exact DGS1MO metrics remain incomplete. This is a research release candidate, not a release, provider-certified result, personalized advice, or future-performance promise.
"""


def _record_role(relative_path: str) -> str:
    if relative_path == "configuration/release_consolidation_contract.json":
        return "frozen_us1c_contract"
    if relative_path.startswith("inputs/manifests/"):
        return "exact_parent_manifest_copy"
    if relative_path.startswith("inputs/canonical_pointers/"):
        return "exact_immutable_pointer_copy"
    if relative_path.startswith("outputs/"):
        return "unchanged_release_critical_parent_derivative"
    if relative_path.startswith("evidence/"):
        return "verification_or_recovery_evidence"
    if relative_path.startswith("lineage/"):
        return "parent_or_source_lineage"
    if relative_path.startswith("commands/"):
        return "command_inventory"
    if relative_path.startswith("diagnostics/"):
        return "release_candidate_diagnostics"
    if relative_path.startswith("limitations"):
        return "limitations"
    if relative_path.startswith("report/"):
        return "release_candidate_report"
    if relative_path == "offline_verification.json":
        return "offline_verification_command"
    return "us1c_release_candidate_record"


def _artifact_record(root: Path, path: Path) -> dict[str, Any]:
    relative = path.relative_to(root).as_posix()
    return {
        "path": relative,
        "role": _record_role(relative),
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _source_code_lineage() -> list[dict[str, Any]]:
    paths = [
        (CONTRACT_PATH, "frozen_us1c_contract"),
        (ROOT / "portfolio/us1c_release_consolidation.py", "us1c_builder_verifier"),
        (ROOT / "workflows/run_us_free_v1.py", "us1c_command"),
    ]
    return [
        {
            "path": path.relative_to(ROOT).as_posix(),
            "role": role,
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path, role in paths
    ]


def _assemble_candidate(
    artifact_root: Path,
    *,
    chain: Mapping[str, Any],
    recovery: Mapping[str, Any],
) -> None:
    contract = load_contract()
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise FileExistsError(f"US1C staging target is non-empty: {artifact_root}")
    artifact_root.mkdir(parents=True, exist_ok=True)
    _copy_exact(
        CONTRACT_PATH,
        artifact_root / "configuration/release_consolidation_contract.json",
    )
    by_stage = {item["stage"]: item for item in contract["chain"]}
    for stage in ("P2", "P3", "P4", "US1A", "US1B"):
        source = ROOT / by_stage[stage]["artifact_root"] / "manifest.json"
        _copy_exact(source, artifact_root / f"inputs/manifests/{stage.lower()}.json")
    for pointer in sorted(DEFAULT_POINTER_DIR.glob("*.json")):
        _copy_exact(
            pointer,
            artifact_root / "inputs/canonical_pointers" / pointer.name,
        )
    release_outputs = {
        "us1a_final_shortlist_2026.parquet": (
            US1A_ROOT / "outputs/final_shortlist_2026.parquet"
        ),
        "us1b_final_shortlist_2026_evidence_derivative.parquet": (
            US1B_ROOT
            / "outputs/final_shortlist_2026_evidence_derivative.parquet"
        ),
        "us1b_name_level_adjudication.parquet": (
            US1B_ROOT / "outputs/name_level_adjudication.parquet"
        ),
    }
    for name, source in release_outputs.items():
        _copy_exact(source, artifact_root / "outputs" / name)
    _copy_exact(
        US1A_ROOT / "report/product_report.md",
        artifact_root / "report/parent_us1a_product_report.md",
    )
    _copy_exact(
        US1B_ROOT / "report/product_report_evidence.md",
        artifact_root / "report/parent_us1b_evidence_report.md",
    )
    parent_lineage = {
        "schema_version": 1,
        "route": chain["route"],
        "contract_sha256": CONTRACT_SHA256,
        "stages": chain["stages"],
        "frozen_boundaries": chain["frozen_boundaries"],
        "source_code_lineage": _source_code_lineage(),
    }
    reconstruction = {
        "status": "pass",
        "assembly_scope": (
            "US1C metadata and unchanged release-critical parent derivatives"
        ),
        "fresh_destination_count": 2,
        "byte_identical_records_and_manifest": True,
        "model_or_methodology_reexecution": False,
        "non_overwriting": True,
        "exposure_rule": "candidate exposed only after both assemblies match",
    }
    limitations = _limitations()
    diagnostics = {
        "status": "pass",
        "chain_stage_order": [item["stage"] for item in chain["stages"]],
        "d1_m1c_identity_boundary": chain["d1_m1c_identity_boundary"],
        "shortlist": chain["shortlist"],
        "recovery": {
            key: recovery[key]
            for key in (
                "repository",
                "revision",
                "verified_file_count",
                "verified_size_bytes",
                "visibility",
                "mutable_revision_fallback",
                "remote_state_mutated",
            )
        },
        "limitations": limitations,
        "release_performed": False,
        "methodology_changed": False,
    }
    _write_json(artifact_root / "lineage/parent_lineage.json", parent_lineage)
    _write_json(
        artifact_root / "commands/command_inventory.json",
        _command_inventory(),
    )
    _write_json(
        artifact_root / "evidence/local_chain_verification.json",
        chain,
    )
    _write_json(
        artifact_root / "evidence/reconstruction_verification.json",
        reconstruction,
    )
    _write_json(
        artifact_root / "evidence/recovery_verification.json",
        recovery,
    )
    _write_json(
        artifact_root / "diagnostics/reconciliation.json",
        diagnostics,
    )
    _write_json(artifact_root / "limitations.json", limitations)
    (artifact_root / "limitations.md").write_text(_render_limitations(limitations))
    _write_json(
        artifact_root / "offline_verification.json",
        {
            "command": contract["offline_verification_command"],
            "network_required": False,
            "verifies_every_manifest_record": True,
            "reconciles_parent_chain": True,
            "recovery_evidence_rechecked_without_remote_access": True,
        },
    )
    (artifact_root / "report/release_candidate_report.md").write_text(
        _render_report(chain, recovery, diagnostics)
    )
    records = [
        _artifact_record(artifact_root, path)
        for path in sorted(artifact_root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": contract["artifact_class"],
        "version": contract["artifact_version"],
        "created_at_utc": "2026-08-01T21:00:00+00:00",
        "contract_sha256": CONTRACT_SHA256,
        "route": chain["route"],
        "parent_manifest_sha256": {
            stage: by_stage[stage]["manifest_sha256"]
            for stage in ("P2", "P3", "P4", "US1A", "US1B")
        },
        "recovery_revision": recovery["revision"],
        "recovery_visibility": recovery["visibility"],
        "claim": {
            "local_verification_complete": True,
            "deterministic_reconstruction_complete": True,
            "immutable_read_only_recovery_complete": True,
            "release_performed": False,
            "remote_state_mutated": False,
            "external_evidence_collected": False,
            "model_or_methodology_executed": False,
            "performance_calculated": False,
        },
        "records": records,
    }
    _write_json(artifact_root / "manifest.json", manifest)


def _tree_fingerprint(root: Path) -> list[tuple[str, int, str]]:
    return [
        (path.relative_to(root).as_posix(), path.stat().st_size, sha256_file(path))
        for path in sorted(root.rglob("*"))
        if path.is_file()
    ]


def build_release_candidate(
    artifact_root: Path,
    *,
    recovery_evidence: Mapping[str, Any],
) -> Path:
    """Assemble twice, compare byte for byte, then expose one fresh artifact."""
    artifact_root = artifact_root.resolve()
    if artifact_root.exists():
        raise FileExistsError(f"US1C target already exists: {artifact_root}")
    chain = verify_release_chain()
    recovery = validate_recovery_evidence(recovery_evidence)
    artifact_root.parent.mkdir(parents=True, exist_ok=True)
    first = Path(
        tempfile.mkdtemp(prefix=f".{artifact_root.name}.first.", dir=artifact_root.parent)
    )
    second = Path(
        tempfile.mkdtemp(prefix=f".{artifact_root.name}.second.", dir=artifact_root.parent)
    )
    exposed = False
    try:
        _assemble_candidate(first, chain=chain, recovery=recovery)
        _assemble_candidate(second, chain=chain, recovery=recovery)
        if _tree_fingerprint(first) != _tree_fingerprint(second):
            raise US1CContractError("fresh US1C assemblies are not byte-identical")
        first.rename(artifact_root)
        exposed = True
        verify_release_candidate(artifact_root)
        return artifact_root / "manifest.json"
    finally:
        if not exposed and first.exists():
            shutil.rmtree(first)
        if second.exists():
            shutil.rmtree(second)


def verify_release_candidate(
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
    *,
    expected_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    """Offline verification of every US1C record and frozen parent boundary."""
    artifact_root = artifact_root.resolve()
    manifest_path = artifact_root / "manifest.json"
    if not manifest_path.is_file():
        raise US1CContractError("US1C manifest is missing")
    manifest_sha = sha256_file(manifest_path)
    if expected_manifest_sha256 and manifest_sha != expected_manifest_sha256:
        raise US1CContractError("US1C manifest SHA-256 mismatch")
    manifest = json.loads(manifest_path.read_text())
    if (
        manifest.get("artifact_class")
        != "US_FREE_DATA_V1_RELEASE_CANDIDATE_US1C"
        or manifest.get("version") != DEFAULT_VERSION
        or manifest.get("contract_sha256") != CONTRACT_SHA256
        or manifest.get("claim", {}).get("release_performed") is not False
        or manifest.get("claim", {}).get("remote_state_mutated") is not False
        or manifest.get("claim", {}).get("model_or_methodology_executed")
        is not False
    ):
        raise US1CContractError("US1C manifest identity or claim drifted")
    result = verify_manifest_records(
        artifact_root,
        expected_manifest_sha256=manifest_sha,
    )
    _assert_exact_file(
        CONTRACT_PATH,
        artifact_root / "configuration/release_consolidation_contract.json",
        "US1C contract",
    )
    contract = load_contract()
    by_stage = {item["stage"]: item for item in contract["chain"]}
    for stage in ("P2", "P3", "P4", "US1A", "US1B"):
        _assert_exact_file(
            ROOT / by_stage[stage]["artifact_root"] / "manifest.json",
            artifact_root / f"inputs/manifests/{stage.lower()}.json",
            f"US1C {stage} parent manifest",
        )
    for pointer in sorted(DEFAULT_POINTER_DIR.glob("*.json")):
        _assert_exact_file(
            pointer,
            artifact_root / "inputs/canonical_pointers" / pointer.name,
            f"US1C pointer {pointer.name}",
        )
    _assert_exact_file(
        US1A_ROOT / "outputs/final_shortlist_2026.parquet",
        artifact_root / "outputs/us1a_final_shortlist_2026.parquet",
        "US1C US1A shortlist",
    )
    _assert_exact_file(
        US1B_ROOT / "outputs/final_shortlist_2026_evidence_derivative.parquet",
        artifact_root
        / "outputs/us1b_final_shortlist_2026_evidence_derivative.parquet",
        "US1C US1B evidence derivative",
    )
    _assert_exact_file(
        US1B_ROOT / "outputs/name_level_adjudication.parquet",
        artifact_root / "outputs/us1b_name_level_adjudication.parquet",
        "US1C name evidence",
    )
    chain = verify_release_chain()
    recorded_chain = json.loads(
        (artifact_root / "evidence/local_chain_verification.json").read_text()
    )
    if recorded_chain != chain:
        raise US1CContractError("US1C local chain evidence drifted")
    recovery = validate_recovery_evidence(
        json.loads(
            (artifact_root / "evidence/recovery_verification.json").read_text()
        )
    )
    reconstruction = json.loads(
        (artifact_root / "evidence/reconstruction_verification.json").read_text()
    )
    if reconstruction != {
        "status": "pass",
        "assembly_scope": (
            "US1C metadata and unchanged release-critical parent derivatives"
        ),
        "fresh_destination_count": 2,
        "byte_identical_records_and_manifest": True,
        "model_or_methodology_reexecution": False,
        "non_overwriting": True,
        "exposure_rule": "candidate exposed only after both assemblies match",
    }:
        raise US1CContractError("US1C reconstruction evidence drifted")
    diagnostics = json.loads(
        (artifact_root / "diagnostics/reconciliation.json").read_text()
    )
    report = (artifact_root / "report/release_candidate_report.md").read_text()
    if report != _render_report(chain, recovery, diagnostics):
        raise US1CContractError("US1C report does not reproduce")
    lineage = json.loads((artifact_root / "lineage/parent_lineage.json").read_text())
    if lineage.get("source_code_lineage") != _source_code_lineage():
        raise US1CContractError("US1C verifier/code lineage drifted")
    names = pd.read_parquet(
        artifact_root / "outputs/us1b_name_level_adjudication.parquet"
    )
    return {
        "status": "pass",
        "manifest_sha256": manifest_sha,
        "record_count": result.record_count,
        "record_bytes": result.record_bytes,
        "route": chain["route"],
        "us1a_record_count": chain["us1a_record_count"],
        "us1a_record_bytes": chain["us1a_record_bytes"],
        "us1b_record_count": chain["us1b_record_count"],
        "us1b_record_bytes": chain["us1b_record_bytes"],
        "recovered_file_count": recovery["verified_file_count"],
        "recovered_size_bytes": recovery["verified_size_bytes"],
        "recovery_revision": recovery["revision"],
        "unresolved_names": int(names["coverage_state"].eq("unresolved").sum()),
        "failed_request_names": int(
            names["coverage_state"].eq("failed_request").sum()
        ),
        "release_performed": False,
        "remote_state_mutated": False,
        "performance_calculated": False,
    }
