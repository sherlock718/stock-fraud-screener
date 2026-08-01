from __future__ import annotations

from io import BytesIO
import hashlib
import json
from pathlib import Path
import tarfile

import pytest

from quality import archive_retirement as archive


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _fixture_contract(tmp_path: Path) -> tuple[Path, str, list[str]]:
    names = ["candidate_a", "candidate_b", "candidate_c", "candidate_d"]
    candidates = []
    for index, name in enumerate(names):
        relative = f"artifacts/pit_validation/{name}"
        root = tmp_path / relative
        _write(root / "manifest.json", f'{{"candidate": {index}}}\n'.encode())
        _write(root / "nested/payload.bin", bytes([index]) * (index + 3))
        inventory = archive._inventory(root, relative)
        candidates.append(
            {
                "archive_package": (
                    f"artifacts/archive/a1/20260801T220130Z-a1/packages/{name}.tar"
                ),
                "file_count": inventory["file_count"],
                "inventory_sha256": hashlib.sha256(
                    archive._json_bytes(inventory)
                ).hexdigest(),
                "manifest_sha256": archive._sha256(root / "manifest.json"),
                "recovery_command": f"recover {name}",
                "source_root": relative,
                "total_size_bytes": inventory["total_size_bytes"],
            }
        )

    repository_inventory = tmp_path / "docs/repository_inventory.json"
    _write(repository_inventory, b'{"fixture": true}\n')
    contract = {
        "archive_candidates": candidates,
        "archive_root": "artifacts/archive/a1/20260801T220130Z-a1",
        "artifact_class": "DEPENDENCY_SAFE_ARCHIVE_AND_RETIREMENT_A1",
        "artifact_version": "20260801T220130Z-a1",
        "pre_action_repository_inventory": {
            "path": "docs/repository_inventory.json",
            "sha256": archive._sha256(repository_inventory),
        },
    }
    contract_path = tmp_path / "docs/A1_ARCHIVE_CONTRACT.json"
    _write(contract_path, archive._json_bytes(contract))
    return contract_path, archive._sha256(contract_path), names


def test_archive_build_recovery_and_pointer_only_retirement(tmp_path: Path):
    contract_path, contract_sha, names = _fixture_contract(tmp_path)

    built = archive.build_archive(tmp_path, contract_path, contract_sha)
    assert built["status"] == "pass"
    assert built["package_count"] == 4
    assert archive.verify_archive(tmp_path, contract_path, contract_sha) == built

    target = tmp_path / "recovered/candidate_a"
    recovered = archive.recover_archive(
        tmp_path, contract_path, "candidate_a", target, contract_sha
    )
    assert recovered["status"] == "pass"
    assert (target / "manifest.json").is_file()
    with pytest.raises(archive.ArchiveContractError, match="already exists"):
        archive.recover_archive(
            tmp_path, contract_path, "candidate_a", target, contract_sha
        )

    with pytest.raises(archive.ArchiveContractError, match="confirmation"):
        archive.retire_sources(tmp_path, contract_path, "wrong", contract_sha)
    retired = archive.retire_sources(
        tmp_path, contract_path, "20260801T220130Z-a1", contract_sha
    )
    assert retired["status"] == "pass"
    assert [item["candidate"] for item in retired["retired"]] == names
    for candidate in json.loads(contract_path.read_text())["archive_candidates"]:
        stub = tmp_path / candidate["source_root"]
        assert sorted(path.name for path in stub.iterdir()) == [
            "ARCHIVED_POINTER.json",
            "manifest.json",
        ]
        assert archive._sha256(stub / "manifest.json") == candidate["manifest_sha256"]


def test_archive_fails_closed_on_source_drift_and_existing_destination(tmp_path: Path):
    contract_path, contract_sha, _ = _fixture_contract(tmp_path)
    drifted = tmp_path / "artifacts/pit_validation/candidate_c/nested/payload.bin"
    drifted.write_bytes(b"drift")
    with pytest.raises(archive.ArchiveContractError, match="inventory drifted"):
        archive.build_archive(tmp_path, contract_path, contract_sha)

    contract_path, contract_sha, _ = _fixture_contract(tmp_path / "fresh")
    fresh_root = tmp_path / "fresh"
    archive.build_archive(fresh_root, contract_path, contract_sha)
    with pytest.raises(archive.ArchiveContractError, match="already exists"):
        archive.build_archive(fresh_root, contract_path, contract_sha)


def test_archive_verifier_rejects_package_corruption(tmp_path: Path):
    contract_path, contract_sha, _ = _fixture_contract(tmp_path)
    archive.build_archive(tmp_path, contract_path, contract_sha)
    package = (
        tmp_path
        / "artifacts/archive/a1/20260801T220130Z-a1/packages/candidate_b.tar"
    )
    with package.open("ab") as handle:
        handle.write(b"corruption")
    with pytest.raises(archive.ArchiveContractError, match="record drifted"):
        archive.verify_archive(tmp_path, contract_path, contract_sha)


def test_recovery_rejects_path_traversal(tmp_path: Path):
    package = tmp_path / "unsafe.tar"
    with tarfile.open(package, "w") as tar:
        info = tarfile.TarInfo("../escape.txt")
        payload = b"escape"
        info.size = len(payload)
        tar.addfile(info, BytesIO(payload))
    with pytest.raises(archive.ArchiveContractError, match="escapes"):
        archive._safe_extract(package, tmp_path / "target")
    assert not (tmp_path / "escape.txt").exists()
