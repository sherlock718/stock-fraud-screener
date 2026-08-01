"""Dependency-safe Session A1 archive packaging, recovery, and retirement.

The command is local-only.  It never calls a network service, changes Git
refs, or touches a path outside the contract's four candidate roots and its
non-overwriting archive root.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT = ROOT / "docs/A1_ARCHIVE_CONTRACT.json"
EXPECTED_CONTRACT_SHA256 = (
    "1b8174bac5181b68f6e3913c2e6bf73a7271acce9f1421063e40b39db2a63f3a"
)


class ArchiveContractError(RuntimeError):
    """Raised when an A1 archive or dependency boundary does not match."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _write_new(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(payload)


def _inventory(root: Path, source_label: str) -> dict[str, Any]:
    if not root.is_dir():
        raise ArchiveContractError(f"candidate source root is missing: {root}")
    records: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or (path.exists() and not path.is_file() and not path.is_dir()):
            raise ArchiveContractError(f"unsupported candidate path type: {path}")
        if path.is_file():
            records.append(
                {
                    "relative_path": path.relative_to(root).as_posix(),
                    "sha256": _sha256(path),
                    "size_bytes": path.stat().st_size,
                }
            )
    return {
        "file_count": len(records),
        "records": records,
        "schema_version": 1,
        "source_root": source_label,
        "total_size_bytes": sum(record["size_bytes"] for record in records),
    }


def _load_contract(
    contract_path: Path,
    expected_sha256: str = EXPECTED_CONTRACT_SHA256,
) -> tuple[dict[str, Any], str]:
    actual = _sha256(contract_path)
    if actual != expected_sha256:
        raise ArchiveContractError(
            f"A1 contract hash mismatch: expected={expected_sha256} actual={actual}"
        )
    contract = json.loads(contract_path.read_text())
    if (
        contract.get("artifact_class")
        != "DEPENDENCY_SAFE_ARCHIVE_AND_RETIREMENT_A1"
        or contract.get("artifact_version") != "20260801T220130Z-a1"
    ):
        raise ArchiveContractError("A1 contract identity drifted")
    candidates = contract.get("archive_candidates", [])
    if len(candidates) != 4 or len({item["source_root"] for item in candidates}) != 4:
        raise ArchiveContractError("A1 candidate set is not exactly four unique roots")
    return contract, actual


def _candidate_name(candidate: dict[str, Any]) -> str:
    return Path(candidate["source_root"]).name


def _verify_source_inventory(repo_root: Path, candidate: dict[str, Any]) -> dict[str, Any]:
    source = repo_root / candidate["source_root"]
    inventory = _inventory(source, candidate["source_root"])
    actual_inventory_sha = hashlib.sha256(_json_bytes(inventory)).hexdigest()
    expected = {
        "file_count": candidate["file_count"],
        "total_size_bytes": candidate["total_size_bytes"],
        "inventory_sha256": candidate["inventory_sha256"],
        "manifest_sha256": candidate["manifest_sha256"],
    }
    actual = {
        "file_count": inventory["file_count"],
        "total_size_bytes": inventory["total_size_bytes"],
        "inventory_sha256": actual_inventory_sha,
        "manifest_sha256": _sha256(source / "manifest.json"),
    }
    if actual != expected:
        raise ArchiveContractError(
            f"candidate source inventory drifted for {source}: "
            f"expected={expected} actual={actual}"
        )
    return inventory


def _add_deterministic_file(tar: tarfile.TarFile, path: Path, arcname: str) -> None:
    info = tar.gettarinfo(str(path), arcname=arcname)
    if not info.isreg():
        raise ArchiveContractError(f"archive input is not a regular file: {path}")
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    with path.open("rb") as handle:
        tar.addfile(info, handle)


def _build_tar(source: Path, inventory: dict[str, Any], package: Path) -> None:
    package.parent.mkdir(parents=True, exist_ok=True)
    with package.open("xb") as raw:
        with tarfile.open(fileobj=raw, mode="w") as tar:
            for record in inventory["records"]:
                _add_deterministic_file(
                    tar,
                    source / record["relative_path"],
                    record["relative_path"],
                )


def _safe_extract(package: Path, target: Path) -> None:
    if target.exists():
        raise ArchiveContractError(f"recovery target already exists: {target}")
    target.mkdir(parents=True)
    target_resolved = target.resolve()
    with tarfile.open(package, mode="r") as tar:
        members = tar.getmembers()
        for member in members:
            if not member.isreg():
                raise ArchiveContractError(
                    f"archive contains a non-regular member: {member.name}"
                )
            destination = (target / member.name).resolve()
            if destination != target_resolved and target_resolved not in destination.parents:
                raise ArchiveContractError(
                    f"archive member escapes recovery target: {member.name}"
                )
        tar.extractall(target, members=members)


def _verify_recovered(
    target: Path, expected_inventory: dict[str, Any]
) -> dict[str, Any]:
    actual = _inventory(target, expected_inventory["source_root"])
    if actual != expected_inventory:
        raise ArchiveContractError(f"recovered inventory differs at {target}")
    return {
        "file_count": actual["file_count"],
        "inventory_sha256": hashlib.sha256(_json_bytes(actual)).hexdigest(),
        "status": "pass",
        "total_size_bytes": actual["total_size_bytes"],
    }


def build_archive(
    repo_root: Path,
    contract_path: Path,
    expected_contract_sha256: str = EXPECTED_CONTRACT_SHA256,
) -> dict[str, Any]:
    contract, contract_sha = _load_contract(contract_path, expected_contract_sha256)
    inventory_contract = contract["pre_action_repository_inventory"]
    repository_inventory = repo_root / inventory_contract["path"]
    if _sha256(repository_inventory) != inventory_contract["sha256"]:
        raise ArchiveContractError("pre-action repository inventory hash drifted")

    archive_root = repo_root / contract["archive_root"]
    if archive_root.exists():
        raise ArchiveContractError(f"archive root already exists: {archive_root}")
    archive_root.parent.mkdir(parents=True, exist_ok=True)
    staging = archive_root.parent / f".{archive_root.name}.building"
    if staging.exists():
        raise ArchiveContractError(f"archive staging root already exists: {staging}")
    staging.mkdir()

    package_results: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    try:
        contract_copy = staging / "contract/A1_ARCHIVE_CONTRACT.json"
        _write_new(contract_copy, contract_path.read_bytes())
        repo_inventory_copy = staging / "inventories/repository_before.json"
        _write_new(repo_inventory_copy, repository_inventory.read_bytes())

        for candidate in contract["archive_candidates"]:
            name = _candidate_name(candidate)
            inventory = _verify_source_inventory(repo_root, candidate)
            inventory_path = staging / f"inventories/{name}.json"
            _write_new(inventory_path, _json_bytes(inventory))
            package = staging / f"packages/{name}.tar"
            _build_tar(repo_root / candidate["source_root"], inventory, package)

            with tempfile.TemporaryDirectory(
                prefix=f"a1-{name}-recovery-", dir=archive_root.parent
            ) as temporary:
                recovered = Path(temporary) / name
                _safe_extract(package, recovered)
                proof = _verify_recovered(recovered, inventory)

            package_results.append(
                {
                    "file_count": candidate["file_count"],
                    "inventory_path": f"inventories/{name}.json",
                    "inventory_sha256": candidate["inventory_sha256"],
                    "manifest_sha256": candidate["manifest_sha256"],
                    "package_path": f"packages/{name}.tar",
                    "package_sha256": _sha256(package),
                    "package_size_bytes": package.stat().st_size,
                    "recovery_proof": proof,
                    "source_root": candidate["source_root"],
                    "source_total_size_bytes": candidate["total_size_bytes"],
                }
            )

        for path in sorted(p for p in staging.rglob("*") if p.is_file()):
            if path.name == "manifest.json":
                continue
            records.append(
                {
                    "path": path.relative_to(staging).as_posix(),
                    "sha256": _sha256(path),
                    "size_bytes": path.stat().st_size,
                }
            )
        manifest = {
            "artifact_class": "A1_LOCAL_ARCHIVE_PACKAGES",
            "artifact_version": contract["artifact_version"],
            "contract_sha256": contract_sha,
            "non_overwriting": True,
            "packages": package_results,
            "records": records,
            "recovery_proved_before_retirement": True,
            "remote_mutation": False,
            "schema_version": 1,
        }
        _write_new(staging / "manifest.json", _json_bytes(manifest))
        os.replace(staging, archive_root)
    except Exception:
        # Preserve a failed staging tree for diagnosis; never overwrite or hide it.
        raise
    return verify_archive(repo_root, contract_path, expected_contract_sha256)


def verify_archive(
    repo_root: Path,
    contract_path: Path,
    expected_contract_sha256: str = EXPECTED_CONTRACT_SHA256,
) -> dict[str, Any]:
    contract, contract_sha = _load_contract(contract_path, expected_contract_sha256)
    archive_root = repo_root / contract["archive_root"]
    manifest_path = archive_root / "manifest.json"
    if not manifest_path.is_file():
        raise ArchiveContractError("A1 archive manifest is missing")
    manifest = json.loads(manifest_path.read_text())
    if (
        manifest.get("artifact_class") != "A1_LOCAL_ARCHIVE_PACKAGES"
        or manifest.get("artifact_version") != contract["artifact_version"]
        or manifest.get("contract_sha256") != contract_sha
        or manifest.get("recovery_proved_before_retirement") is not True
        or manifest.get("remote_mutation") is not False
    ):
        raise ArchiveContractError("A1 archive manifest identity/boundary drifted")

    regular_files = sorted(
        p.relative_to(archive_root).as_posix()
        for p in archive_root.rglob("*")
        if p.is_file() and p != manifest_path
    )
    recorded = sorted(item["path"] for item in manifest.get("records", []))
    if regular_files != recorded:
        raise ArchiveContractError("A1 archive manifest does not enumerate every file")
    for record in manifest["records"]:
        path = archive_root / record["path"]
        if path.stat().st_size != record["size_bytes"] or _sha256(path) != record["sha256"]:
            raise ArchiveContractError(f"A1 archive record drifted: {path}")

    package_by_source = {item["source_root"]: item for item in manifest["packages"]}
    if set(package_by_source) != {
        item["source_root"] for item in contract["archive_candidates"]
    }:
        raise ArchiveContractError("A1 package/source set drifted")
    for candidate in contract["archive_candidates"]:
        package_record = package_by_source[candidate["source_root"]]
        package = archive_root / package_record["package_path"]
        inventory = archive_root / package_record["inventory_path"]
        if (
            _sha256(package) != package_record["package_sha256"]
            or package.stat().st_size != package_record["package_size_bytes"]
            or _sha256(inventory) != candidate["inventory_sha256"]
            or package_record["recovery_proof"].get("status") != "pass"
        ):
            raise ArchiveContractError(f"A1 package verification failed: {package}")
    return {
        "archive_manifest_sha256": _sha256(manifest_path),
        "archive_root": contract["archive_root"],
        "package_count": len(manifest["packages"]),
        "record_count": len(manifest["records"]),
        "status": "pass",
    }


def recover_archive(
    repo_root: Path,
    contract_path: Path,
    candidate_name: str,
    target_root: Path,
    expected_contract_sha256: str = EXPECTED_CONTRACT_SHA256,
) -> dict[str, Any]:
    contract, _ = _load_contract(contract_path, expected_contract_sha256)
    verify_archive(repo_root, contract_path, expected_contract_sha256)
    archive_root = repo_root / contract["archive_root"]
    manifest = json.loads((archive_root / "manifest.json").read_text())
    matches = [
        item for item in manifest["packages"]
        if Path(item["source_root"]).name == candidate_name
    ]
    if len(matches) != 1:
        raise ArchiveContractError(f"unknown A1 archive candidate: {candidate_name}")
    package_record = matches[0]
    expected_inventory = json.loads(
        (archive_root / package_record["inventory_path"]).read_text()
    )
    _safe_extract(archive_root / package_record["package_path"], target_root)
    proof = _verify_recovered(target_root, expected_inventory)
    proof["candidate"] = candidate_name
    proof["target_root"] = str(target_root)
    return proof


def retire_sources(
    repo_root: Path,
    contract_path: Path,
    confirmation: str,
    expected_contract_sha256: str = EXPECTED_CONTRACT_SHA256,
) -> dict[str, Any]:
    contract, contract_sha = _load_contract(contract_path, expected_contract_sha256)
    if confirmation != contract["artifact_version"]:
        raise ArchiveContractError("exact A1 artifact-version confirmation is required")
    verification = verify_archive(repo_root, contract_path, expected_contract_sha256)
    archive_root = repo_root / contract["archive_root"]
    archive_manifest_sha = verification["archive_manifest_sha256"]
    archive_manifest = json.loads((archive_root / "manifest.json").read_text())
    package_by_source = {
        item["source_root"]: item for item in archive_manifest["packages"]
    }

    inventories: dict[str, dict[str, Any]] = {}
    manifest_bytes: dict[str, bytes] = {}
    for candidate in contract["archive_candidates"]:
        inventory = _verify_source_inventory(repo_root, candidate)
        inventories[candidate["source_root"]] = inventory
        manifest_bytes[candidate["source_root"]] = (
            repo_root / candidate["source_root"] / "manifest.json"
        ).read_bytes()

    retired: list[dict[str, Any]] = []
    for candidate in contract["archive_candidates"]:
        source = repo_root / candidate["source_root"]
        package = package_by_source[candidate["source_root"]]
        shutil.rmtree(source)
        source.mkdir(parents=True)
        _write_new(source / "manifest.json", manifest_bytes[candidate["source_root"]])
        pointer = {
            "archive_contract_sha256": contract_sha,
            "archive_manifest_sha256": archive_manifest_sha,
            "archive_package": str(Path(contract["archive_root"]) / package["package_path"]),
            "archive_package_sha256": package["package_sha256"],
            "original_file_count": candidate["file_count"],
            "original_inventory_sha256": candidate["inventory_sha256"],
            "original_source_root": candidate["source_root"],
            "original_total_size_bytes": candidate["total_size_bytes"],
            "recovery_command": candidate["recovery_command"],
            "status": "archived_pointer_only",
        }
        _write_new(source / "ARCHIVED_POINTER.json", _json_bytes(pointer))
        if _sha256(source / "manifest.json") != candidate["manifest_sha256"]:
            raise ArchiveContractError(f"retained manifest drifted: {source}")
        if sorted(path.name for path in source.iterdir()) != [
            "ARCHIVED_POINTER.json", "manifest.json"
        ]:
            raise ArchiveContractError(f"retired source stub is not pointer-only: {source}")
        retired.append(
            {
                "candidate": _candidate_name(candidate),
                "pointer": str(source / "ARCHIVED_POINTER.json"),
                "retained_manifest_sha256": candidate["manifest_sha256"],
                "status": "retired",
            }
        )
    return {"archive": verification, "retired": retired, "status": "pass"}


def check_dependencies(repo_root: Path, contract_path: Path) -> dict[str, Any]:
    contract, _ = _load_contract(contract_path)
    active_docs = (
        "README.md",
        "docs/ARCHITECTURE.md",
        "docs/CANONICAL_DEPENDENCY_INVENTORY.md",
        "docs/FAQ.md",
        "docs/PRODUCTION_CONFIG.md",
        "docs/START_HERE.md",
    )
    retired_modules = (
        "modeling.build_session9_oos",
        "modeling.freeze_session9b_selection",
    )
    candidate_roots = tuple(item["source_root"] for item in contract["archive_candidates"])
    failures: list[str] = []

    paths_raw = subprocess.check_output(
        ["git", "ls-files", "-co", "--exclude-standard", "-z"],
        cwd=repo_root,
    ).decode()
    paths = sorted({item for item in paths_raw.split("\0") if item})
    for relative in paths:
        if relative == "quality/archive_retirement.py":
            # This module owns the frozen prohibited-pattern table below.
            continue
        path = repo_root / relative
        if not path.is_file() or path.suffix not in {".py", ".yml", ".yaml"}:
            continue
        text = path.read_text(errors="replace")
        for module in retired_modules:
            if module in text:
                failures.append(f"retired module reference: {relative}: {module}")
        for root in candidate_roots:
            if root in text:
                failures.append(f"retired artifact reference: {relative}: {root}")
    for relative in active_docs:
        path = repo_root / relative
        if not path.is_file():
            continue
        text = path.read_text(errors="replace")
        for root in candidate_roots:
            if root in text:
                failures.append(f"active documentation reference: {relative}: {root}")
    if failures:
        raise ArchiveContractError("\n".join(failures))
    return {
        "active_docs_checked": len(active_docs),
        "candidate_roots": len(candidate_roots),
        "files_checked": len(paths),
        "retired_modules": len(retired_modules),
        "status": "pass",
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--build", action="store_true")
    actions.add_argument("--verify", action="store_true")
    actions.add_argument("--recover", metavar="CANDIDATE")
    actions.add_argument("--retire", action="store_true")
    actions.add_argument("--check-dependencies", action="store_true")
    parser.add_argument("--target-root", type=Path)
    parser.add_argument("--confirm-retirement")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    contract_path = args.contract.resolve()
    if args.build:
        result = build_archive(ROOT, contract_path)
    elif args.verify:
        result = verify_archive(ROOT, contract_path)
    elif args.recover:
        if args.target_root is None:
            raise ArchiveContractError("--recover requires --target-root")
        result = recover_archive(
            ROOT, contract_path, args.recover, args.target_root.resolve()
        )
    elif args.retire:
        result = retire_sources(
            ROOT,
            contract_path,
            args.confirm_retirement or "",
        )
    else:
        result = check_dependencies(ROOT, contract_path)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
