import hashlib
import json
from pathlib import Path

from quality.artifact_compatibility import (
    REQUIRED_ARTIFACT_IDS,
    REQUIRED_LIMITATIONS,
    validate_manifest,
)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _manifest(tmp_path: Path) -> Path:
    payload = b"frozen"
    (tmp_path / "payload.bin").write_bytes(payload)
    dirty_records = {}
    for name in ("status", "tracked_patch", "untracked_patch"):
        dirty_payload = f"{name}\n".encode()
        dirty_path = tmp_path / f"{name}.txt"
        dirty_path.write_bytes(dirty_payload)
        dirty_records[name] = {
            "path": dirty_path.name,
            "size_bytes": len(dirty_payload),
            "sha256": _sha(dirty_payload),
        }
    artifacts = []
    for artifact_id in sorted(REQUIRED_ARTIFACT_IDS):
        if artifact_id == "clean_dataset":
            artifacts.append({
                "id": artifact_id,
                "status": "present",
                "role": "legacy_evidence",
                "legacy_input": True,
                "snapshot_path": "payload.bin",
                "source_path": None,
                "size_bytes": len(payload),
                "sha256": _sha(payload),
            })
        else:
            artifacts.append({
                "id": artifact_id,
                "status": "missing",
                "role": "required_legacy_evidence",
                "legacy_input": False,
                "snapshot_path": None,
                "source_path": f"missing/{artifact_id}",
                "size_bytes": None,
                "sha256": None,
            })
    manifest = {
        "schema_version": 1,
        "snapshot_type": "LEGACY_SAVED",
        "baseline_commit": "3f706e3e10d2b354c6e8b9407760fa2074749c0a",
        "snapshot_code_commit": "snapshot-commit",
        "claim": {"reproducible": False, "snapshot_is_evidence_only": True},
        "limitations": [{"id": item} for item in REQUIRED_LIMITATIONS],
        "dirty_state": {
            "complete_status_recorded": True,
            **dirty_records,
            "sensitive_content_exclusions": [{"path": ".codex/config.toml"}],
        },
        "artifacts": artifacts,
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return path


def test_valid_synthetic_manifest_and_later_commit_warning(tmp_path):
    path = _manifest(tmp_path)
    result = validate_manifest(path, current_commit="later-docs-commit")
    assert result.compatible is True
    assert result.errors == ()
    assert result.warnings == ("current_commit_differs_from_snapshot_commit",)


def test_streaming_hash_mismatch_fails(tmp_path):
    path = _manifest(tmp_path)
    (tmp_path / "payload.bin").write_bytes(b"changed")
    result = validate_manifest(path)
    assert "size_mismatch:clean_dataset" in result.errors
    assert "hash_mismatch:clean_dataset" in result.errors


def test_missing_required_evidence_record_fails(tmp_path):
    path = _manifest(tmp_path)
    manifest = json.loads(path.read_text())
    manifest["artifacts"] = [
        item for item in manifest["artifacts"]
        if item["id"] != "canonical_benchmark_nav"
    ]
    path.write_text(json.dumps(manifest))
    result = validate_manifest(path)
    assert "missing_artifact_record:canonical_benchmark_nav" in result.errors


def test_corrected_code_cannot_be_marked_as_legacy_input(tmp_path):
    path = _manifest(tmp_path)
    manifest = json.loads(path.read_text())
    item = next(item for item in manifest["artifacts"] if item["id"] == "clean_dataset")
    item["role"] = "corrected_code_evidence"
    item["legacy_input"] = True
    path.write_text(json.dumps(manifest))
    result = validate_manifest(path)
    assert "corrected_evidence_marked_as_legacy_input:clean_dataset" in result.errors


def test_reproducible_legacy_claim_is_rejected(tmp_path):
    path = _manifest(tmp_path)
    manifest = json.loads(path.read_text())
    manifest["claim"]["reproducible"] = True
    path.write_text(json.dumps(manifest))
    assert "legacy_claim_must_be_marked_non_reproducible" in validate_manifest(path).errors
