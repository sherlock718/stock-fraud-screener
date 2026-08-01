import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

import workflows.run_us_free_v1 as workflow
from portfolio.us1c_release_consolidation import (
    CONTRACT_PATH,
    CONTRACT_SHA256,
    DEFAULT_VERSION,
    US1CContractError,
    build_release_candidate,
    load_contract,
    recovery_evidence_from_result,
    sha256_file,
    validate_recovery_evidence,
    verify_release_candidate,
    verify_release_chain,
)


def _recovered_stages() -> dict[str, dict[str, object]]:
    chain = {item["stage"]: item for item in load_contract()["chain"]}
    return {
        stage: {
            "manifest_sha256": chain[stage]["manifest_sha256"],
            "record_count": chain[stage]["record_count"],
            "record_bytes": chain[stage]["record_bytes"],
            "file_count": chain[stage]["record_count"] + 1,
        }
        for stage in ("P2", "P3", "P4")
    }


def _recovery_evidence() -> dict[str, object]:
    contract = load_contract()["recovery_contract"]
    return {
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
        "recovered_stages": _recovered_stages(),
    }


@pytest.fixture(scope="module")
def built_candidate(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("us1c") / DEFAULT_VERSION
    build_release_candidate(root, recovery_evidence=_recovery_evidence())
    return root


def _refresh_manifest_record(root: Path, relative_path: str) -> None:
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    target = root / relative_path
    for record in manifest["records"]:
        if record["path"] == relative_path:
            record["size_bytes"] = target.stat().st_size
            record["sha256"] = sha256_file(target)
            break
    else:
        raise AssertionError(f"missing manifest record: {relative_path}")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def test_us1c_contract_is_frozen_before_candidate_output() -> None:
    contract = load_contract()

    assert sha256_file(CONTRACT_PATH) == CONTRACT_SHA256
    assert contract["artifact_version"] == DEFAULT_VERSION
    assert contract["release_performed"] is False
    assert contract["reconstruction_contract"]["required_fresh_builds"] == 2
    assert contract["recovery_contract"]["revision"] == (
        "aaf056ea115067e42ef9abf9fa93ade75cdd4052"
    )


def test_complete_chain_rehashes_in_exact_order_and_preserves_boundaries() -> None:
    result = verify_release_chain()

    assert [item["stage"] for item in result["stages"]] == [
        "P2",
        "P3",
        "P4",
        "US1A",
        "US1B",
    ]
    assert result["us1a_record_count"] == 32
    assert result["us1a_record_bytes"] == 37_863_802
    assert result["us1b_record_count"] == 95
    assert result["us1b_record_bytes"] == 2_653_162
    assert result["shortlist"]["all_parent_columns_exact"] is True
    assert result["shortlist"]["name_states"] == {
        "failed_request": 1,
        "unresolved": 14,
    }
    assert result["d1_m1c_identity_boundary"]["p2_only_ids"] == 242
    assert result["d1_m1c_identity_boundary"]["d1_only_ids"] == 76
    assert len(result["frozen_boundaries"]["partial_m1c_attempts"]) == 5


def test_release_candidate_is_complete_offline_and_not_a_release(
    built_candidate: Path,
) -> None:
    result = verify_release_candidate(built_candidate)
    manifest = json.loads((built_candidate / "manifest.json").read_text())
    discovered = {
        path.relative_to(built_candidate).as_posix()
        for path in built_candidate.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }

    assert result["status"] == "pass"
    assert result["record_count"] == len(discovered)
    assert result["record_bytes"] == sum(
        int(item["size_bytes"]) for item in manifest["records"]
    )
    assert result["recovered_file_count"] == 202
    assert result["recovered_size_bytes"] == 481_666_707
    assert result["unresolved_names"] == 14
    assert result["failed_request_names"] == 1
    assert result["release_performed"] is False
    assert result["remote_state_mutated"] is False


def test_two_fresh_release_candidate_builds_are_byte_identical(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first" / DEFAULT_VERSION
    second = tmp_path / "second" / DEFAULT_VERSION
    build_release_candidate(first, recovery_evidence=_recovery_evidence())
    build_release_candidate(second, recovery_evidence=_recovery_evidence())

    first_files = {
        path.relative_to(first).as_posix(): (path.stat().st_size, sha256_file(path))
        for path in first.rglob("*")
        if path.is_file()
    }
    second_files = {
        path.relative_to(second).as_posix(): (path.stat().st_size, sha256_file(path))
        for path in second.rglob("*")
        if path.is_file()
    }
    assert first_files == second_files


def test_release_candidate_refuses_any_existing_target(tmp_path: Path) -> None:
    target = tmp_path / DEFAULT_VERSION
    target.mkdir()
    marker = target / "preserve.txt"
    marker.write_text("preserve")

    with pytest.raises(FileExistsError, match="already exists"):
        build_release_candidate(target, recovery_evidence=_recovery_evidence())

    assert marker.read_text() == "preserve"


def test_shortlist_mismatch_fails_even_with_rehashed_manifest(
    built_candidate: Path,
    tmp_path: Path,
) -> None:
    corrupt = tmp_path / "identity-mismatch"
    shutil.copytree(built_candidate, corrupt)
    relative = "outputs/us1a_final_shortlist_2026.parquet"
    shortlist = pd.read_parquet(corrupt / relative)
    shortlist.loc[0, "rank"] = 99
    shortlist.to_parquet(corrupt / relative, index=False)
    _refresh_manifest_record(corrupt, relative)

    with pytest.raises(US1CContractError, match="US1A shortlist"):
        verify_release_candidate(corrupt)


def test_recovery_mismatch_fails_even_with_rehashed_manifest(
    built_candidate: Path,
    tmp_path: Path,
) -> None:
    corrupt = tmp_path / "recovery-mismatch"
    shutil.copytree(built_candidate, corrupt)
    relative = "evidence/recovery_verification.json"
    evidence = json.loads((corrupt / relative).read_text())
    evidence["revision"] = "0" * 40
    (corrupt / relative).write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n"
    )
    _refresh_manifest_record(corrupt, relative)

    with pytest.raises(US1CContractError, match="recovery evidence mismatch"):
        verify_release_candidate(corrupt)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mutable_revision_fallback", True),
        ("visibility", "unknown"),
        ("remote_state_mutated", True),
    ],
)
def test_recovery_evidence_fails_closed(field: str, value: object) -> None:
    evidence = _recovery_evidence()
    evidence[field] = value

    with pytest.raises(US1CContractError, match="recovery evidence mismatch"):
        validate_recovery_evidence(evidence)


def test_recovery_command_writes_only_fresh_local_targets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "recovered"
    evidence_path = tmp_path / "recovery.json"
    expected = _recovery_evidence()
    retrieval = {
        key: expected[key]
        for key in (
            "repository",
            "repository_type",
            "revision",
            "artifact_count",
            "verified_file_count",
            "verified_size_bytes",
            "visibility",
            "mutable_revision_fallback",
        )
    }
    monkeypatch.setattr(workflow, "require_hf_token", lambda: "secret")
    monkeypatch.setattr(
        workflow,
        "retrieve_from_pointers",
        lambda **kwargs: retrieval,
    )
    monkeypatch.setattr(
        workflow,
        "verify_recovered_canonical_root",
        lambda _: {"status": "pass", "recovered_stages": _recovered_stages()},
    )

    result = workflow._recover(target, evidence_path)

    assert result == expected
    assert json.loads(evidence_path.read_text()) == expected
    with pytest.raises(FileExistsError, match="evidence target already exists"):
        workflow._recover(target, evidence_path)


def test_recovery_result_normalization_rejects_mutable_or_wrong_revision() -> None:
    evidence = _recovery_evidence()
    retrieval = {
        key: evidence[key]
        for key in (
            "repository",
            "repository_type",
            "revision",
            "artifact_count",
            "verified_file_count",
            "verified_size_bytes",
            "visibility",
            "mutable_revision_fallback",
        )
    }
    recovered = {"status": "pass", "recovered_stages": _recovered_stages()}
    assert recovery_evidence_from_result(retrieval, recovered) == evidence

    retrieval["revision"] = "latest"
    with pytest.raises(US1CContractError, match="recovery evidence mismatch"):
        recovery_evidence_from_result(retrieval, recovered)
