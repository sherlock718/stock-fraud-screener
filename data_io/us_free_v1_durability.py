"""Freeze, publish, and recover the immutable US free-data V1 bundle.

DUR1 is artifact durability only.  The module inventories already-frozen
files, creates deterministic transport tar files, publishes one parent-pinned
private Hugging Face commit, and recovers every byte into an absent target.
It never builds data, fits a model, scores a row, or calculates performance.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Sequence

from _root import ROOT
from data_io.canonical_hf import (
    DEFAULT_POINTER_DIR,
    DEFAULT_REPO_ID,
    REPO_TYPE,
    load_pointer_documents,
    require_hf_token,
    verify_private_repository,
)


DURABILITY_VERSION = "20260802T020320Z-dur1-corrective"
CREATED_AT_UTC = "2026-08-02T02:03:20Z"
CHECKPOINT_COMMIT = "f104f0554aa1d4ac916aeef4c4a3b1891eb2fa3d"
CHECKPOINT_TAG = "rel1-us-free-v1"
P2_P4_REVISION = "aaf056ea115067e42ef9abf9fa93ade75cdd4052"
CORRECTIVE_PARENT_REVISION = "33309aeb92a5cca1bb41d5cf76d7330ffc38db0e"
E1_CALENDAR_ALIAS_PATH = (
    "artifacts/pit_validation/calendar_contract/session8b_calendar_contract.json"
)
E1_CALENDAR_CANONICAL_SOURCE = (
    "artifacts/canonical/corrected_us_annual/inputs/session8b_calendar_contract.json"
)
DEFAULT_CONTRACT = ROOT / "docs/DUR1_ARTIFACT_DURABILITY_CONTRACT.json"
DEFAULT_BUNDLE_ROOT = (
    ROOT / "artifacts/durability/us_free_v1" / DURABILITY_VERSION
)
REMOTE_PREFIX = f"durability/us-free-v1/{DURABILITY_VERSION}"
CONTRACT_REMOTE_PATH = f"{REMOTE_PREFIX}/contract.json"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")


class DurabilityError(RuntimeError):
    """Raised when any DUR1 immutability or recovery gate fails."""


@dataclass(frozen=True)
class SourceGroup:
    name: str
    paths: tuple[str, ...]
    reason: str
    required_by: tuple[str, ...]
    storage: str = "tar"


def _a1_source_stub_paths() -> tuple[str, ...]:
    """Resolve retired source stubs from A1's frozen contract."""
    contract_path = ROOT / "docs/A1_ARCHIVE_CONTRACT.json"
    try:
        contract = json.loads(contract_path.read_text())
        paths = tuple(
            str(candidate["source_root"])
            for candidate in contract["archive_candidates"]
        )
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise DurabilityError("cannot resolve A1 source stubs from its contract") from exc
    if len(paths) != 4 or len(set(paths)) != 4:
        raise DurabilityError("A1 contract must resolve exactly four distinct source stubs")
    return paths


SOURCE_GROUPS: tuple[SourceGroup, ...] = (
    SourceGroup(
        "canonical_p2_p4",
        (
            "artifacts/canonical/corrected_us_annual",
            "artifacts/canonical/corrected_us_annual_3y_research_model",
            "artifacts/canonical/corrected_us_annual_3y_product",
        ),
        "Accepted P2-P4 data, OOS-model, liquidity, portfolio, shortlist, and lineage baseline.",
        ("authoritative_route", "US1A", "US1C", "liquidity", "lineage", "shortlist"),
        "existing_remote",
    ),
    SourceGroup(
        "e1_preservation_calendar_alias",
        (E1_CALENDAR_ALIAS_PATH,),
        "Exact calendar-contract destination dereferenced by E1 preservation and downstream US1B, extraction, adjudication, and shortlist verification.",
        ("E1", "US1B", "shortlist", "preservation", "full_test_suite"),
        "existing_remote_alias",
    ),
    SourceGroup(
        "corrected_step2",
        ("artifacts/pit_validation/corrected_step2",),
        "Frozen corrected SEC source population required by corrected Step 2 and PIT failure paths.",
        ("corrected_step2", "P2_reconstruction", "full_test_suite"),
    ),
    SourceGroup(
        "session8e",
        ("artifacts/pit_validation/contract_aligned_label_inputs",),
        "Frozen Session 8E price, label, calendar, benchmark, raw-response, and liquidity evidence.",
        ("Session_8E", "P2", "P4", "B1C", "liquidity", "full_test_suite"),
    ),
    SourceGroup(
        "session8f",
        ("artifacts/pit_validation/corrected_feature_population",),
        "Frozen corrected feature population retained for V3 parity and lineage validation.",
        ("corrected_feature_population", "V3_parity", "lineage", "full_test_suite"),
    ),
    SourceGroup(
        "retained_v3",
        (
            "artifacts/pit_validation/session_v3_1_production_contract",
            "artifacts/pit_validation/session_v3_2_oos_predictions",
            "artifacts/pit_validation/session_v3_3_liquidity_holdings",
        ),
        "Retained V3.1-V3.3 parity artifacts that failed the A1 retirement gate.",
        ("V3_parity", "M1A", "liquidity", "preservation", "full_test_suite"),
    ),
    SourceGroup(
        "d1",
        ("artifacts/canonical_refresh/us/20260730T110301Z",),
        "Complete frozen D1 refresh, raw evidence, review candidate, comparison, and preservation lineage.",
        ("D1", "S1", "E1", "US1A", "US1C", "preservation", "full_test_suite"),
    ),
    SourceGroup(
        "s1",
        ("artifacts/security_ledger/us/20260730T141429Z-s1-final",),
        "Final free-source security and survivorship ledger with exact copied evidence.",
        ("S1", "E1", "B1C", "preservation", "full_test_suite"),
    ),
    SourceGroup(
        "e1_chain",
        ("artifacts/event_review/us",),
        "Complete non-overwriting E1 review, collection, extraction, and adjudication chain.",
        ("E1", "US1A", "US1B", "shortlist", "lineage", "full_test_suite"),
    ),
    SourceGroup(
        "final_shortlist",
        ("artifacts/final_shortlist/us/20260731T000054Z-final-shortlist-v2",),
        "Frozen P4/E1 presentation derivative and citation traceability boundary.",
        ("shortlist", "E1", "lineage", "full_test_suite"),
    ),
    SourceGroup(
        "b1c",
        ("artifacts/performance_inputs/free_data_v1/20260731T115106Z-b1c",),
        "Frozen complete performance-input evidence used by B1D, B1E, and M1D verification.",
        ("B1C", "B1D", "B1E", "M1D", "full_test_suite"),
    ),
    SourceGroup(
        "b1e",
        ("artifacts/performance/free_data_v1/20260801T011135Z-b1e",),
        "Frozen free-data V1 controlled performance and coverage result.",
        ("B1E", "M1A", "M1C", "M1D", "US1A", "US1C"),
    ),
    SourceGroup(
        "m1a_m1c",
        ("artifacts/modeling/nested_walk_forward",),
        "Frozen M1A contract, all five preserved partial M1C attempts, and accepted M1C result/models.",
        ("M1A", "M1C", "M1D", "US1A", "US1C", "preservation", "full_test_suite"),
    ),
    SourceGroup(
        "m1d",
        ("artifacts/performance/m1d/20260801T162953Z-m1d",),
        "Frozen M1D pre-performance lock and sole accepted one-shot performance result.",
        ("M1D", "US1A", "US1C", "lineage", "full_test_suite"),
    ),
    SourceGroup(
        "i1",
        ("artifacts/international/i1/20260801T180000Z-i1-ca",),
        "Frozen first-market Canada compatibility artifact and fail-closed adapter boundary.",
        ("I1", "US1A", "US1C", "preservation"),
    ),
    SourceGroup(
        "us1a",
        ("artifacts/product/us_free_v1/20260801T183000Z-us1a",),
        "Frozen accepted US product candidate, complete gates/liquidity lineage, and shortlist.",
        ("US1A", "US1B", "US1C", "liquidity", "shortlist", "full_test_suite"),
    ),
    SourceGroup(
        "us1b",
        ("artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b",),
        "Frozen exact-shortlist evidence derivative and human-review boundary.",
        ("US1B", "US1C", "E1", "shortlist", "full_test_suite"),
    ),
    SourceGroup(
        "us1c",
        ("artifacts/product/us_free_v1_release_candidate/20260801T210000Z-us1c",),
        "Frozen local release-consolidation candidate and recovery evidence.",
        ("US1C", "authoritative_route", "lineage", "full_test_suite"),
    ),
    SourceGroup(
        "a1_archive",
        ("artifacts/archive/a1/20260801T220130Z-a1",),
        "All four A1 archive packages, inventories, frozen contract copy, and recovery manifest.",
        ("A1", "archive_recovery", "preservation", "full_test_suite"),
        "a1_mixed",
    ),
    SourceGroup(
        "a1_stubs",
        _a1_source_stub_paths(),
        "Pointer-only A1 source stubs preserving exact historical manifests and recovery lineage.",
        ("A1", "archive_recovery", "preservation"),
    ),
    SourceGroup(
        "clean_checkout_data",
        ("data/tickers.parquet", "data/historical_dataset_clean.parquet"),
        "Ignored legacy integration inputs needed to reproduce the current complete clean-checkout test boundary.",
        ("full_test_suite", "historical_universe_checks"),
    ),
)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _canonical_sha(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _safe_relative(value: str, label: str = "path") -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or ".." in path.parts
        or "." in path.parts
        or path.as_posix() != value
    ):
        raise DurabilityError(f"unsafe {label}: {value!r}")
    return path


def _group_files(repo_root: Path, group: SourceGroup) -> list[Path]:
    files: list[Path] = []
    for relative in group.paths:
        path = repo_root / _safe_relative(relative, "source group path")
        if not path.exists():
            raise DurabilityError(f"required source is missing: {relative}")
        candidates = [path] if path.is_file() else sorted(path.rglob("*"))
        for candidate in candidates:
            if candidate.is_symlink():
                raise DurabilityError(
                    "durability sources may not contain symlinks: "
                    f"{candidate.relative_to(repo_root)}"
                )
            if candidate.is_file():
                files.append(candidate)
    return sorted(files)


def _is_manifest_like(path: str) -> bool:
    name = PurePosixPath(path).name.lower()
    return (
        "manifest" in name or "inventory" in name or name == "lock_manifest.json"
    ) and (name.endswith(".json") or name.endswith(".jsonl"))


def inventory_groups(
    repo_root: Path = ROOT,
    groups: Sequence[SourceGroup] = SOURCE_GROUPS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Rehash every required source and return groups plus per-file records."""
    repo_root = repo_root.resolve()
    seen: set[str] = set()
    group_records: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    for group in groups:
        paths = _group_files(repo_root, group)
        relative_paths = [path.relative_to(repo_root).as_posix() for path in paths]
        overlap = seen.intersection(relative_paths)
        if overlap:
            raise DurabilityError(
                f"source groups overlap at {sorted(overlap)[:5]}"
            )
        seen.update(relative_paths)
        manifest_paths = [path for path in relative_paths if _is_manifest_like(path)]
        manifest_relationships = []
        for index, manifest_path in enumerate(manifest_paths):
            local = repo_root / manifest_path
            manifest_relationships.append(
                {
                    "id": f"{group.name}:manifest:{index}",
                    "path": manifest_path,
                    "size_bytes": local.stat().st_size,
                    "sha256": sha256_file(local),
                    "relationship": "exact_tree_parent_or_inventory",
                }
            )
        if not manifest_relationships:
            manifest_relationships.append(
                {
                    "id": f"{group.name}:durability-contract",
                    "path": None,
                    "size_bytes": None,
                    "sha256": None,
                    "relationship": "durability_contract_authoritative_inventory",
                }
            )
        relationship_ids = [item["id"] for item in manifest_relationships]
        group_bytes = 0
        for path, relative in zip(paths, relative_paths):
            size = path.stat().st_size
            group_bytes += size
            sources.append(
                {
                    "path": relative,
                    "size_bytes": size,
                    "sha256": sha256_file(path),
                    "artifact_group": group.name,
                    "manifest_relationship_ids": relationship_ids,
                    "manifest_relationship_mode": (
                        "artifact_manifest_or_inventory"
                        if relative in manifest_paths
                        else "exact_tree_member"
                    ),
                    "recovery_destination": relative,
                    "reason": group.reason,
                    "required_by": list(group.required_by),
                }
            )
        group_records.append(
            {
                "name": group.name,
                "roots": list(group.paths),
                "storage": group.storage,
                "reason": group.reason,
                "required_by": list(group.required_by),
                "file_count": len(paths),
                "total_size_bytes": group_bytes,
                "manifest_relationships": manifest_relationships,
            }
        )
    return group_records, sorted(sources, key=lambda item: item["path"])


def _tar_info(path: str, size: int) -> tarfile.TarInfo:
    info = tarfile.TarInfo(path)
    info.size = size
    info.mode = 0o644
    info.mtime = 0
    info.uid = info.gid = 0
    info.uname = info.gname = ""
    return info


def write_deterministic_tar(
    repo_root: Path,
    source_records: Sequence[Mapping[str, Any]],
    output: Path,
) -> None:
    """Create a regular-file-only deterministic tar at an absent path."""
    if output.exists():
        raise DurabilityError(f"bundle payload already exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as raw, tarfile.open(fileobj=raw, mode="w") as archive:
        for record in sorted(source_records, key=lambda item: str(item["path"])):
            relative = _safe_relative(str(record["path"]), "tar member")
            source = repo_root.joinpath(*relative.parts)
            if not source.is_file() or source.is_symlink():
                raise DurabilityError(f"tar source is missing or unsafe: {relative}")
            with source.open("rb") as handle:
                archive.addfile(
                    _tar_info(relative.as_posix(), int(record["size_bytes"])),
                    handle,
                )


def verify_tar(
    path: Path,
    source_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    expected = {str(item["path"]): item for item in source_records}
    observed: set[str] = set()
    verified_bytes = 0
    with tarfile.open(path, "r") as archive:
        for member in archive:
            relative = _safe_relative(member.name, "tar member")
            if not member.isfile() or member.issym() or member.islnk():
                raise DurabilityError(f"non-regular tar member: {member.name}")
            name = relative.as_posix()
            if name in observed or name not in expected:
                raise DurabilityError(f"unexpected or duplicate tar member: {name}")
            record = expected[name]
            if member.size != record["size_bytes"]:
                raise DurabilityError(f"tar member size drifted: {name}")
            extracted = archive.extractfile(member)
            if extracted is None:
                raise DurabilityError(f"tar member cannot be read: {name}")
            digest = hashlib.sha256()
            for chunk in iter(lambda: extracted.read(1024 * 1024), b""):
                digest.update(chunk)
            if digest.hexdigest() != record["sha256"]:
                raise DurabilityError(f"tar member hash drifted: {name}")
            observed.add(name)
            verified_bytes += member.size
    if observed != set(expected):
        raise DurabilityError(
            f"tar member set drifted: missing={sorted(set(expected) - observed)[:5]}"
        )
    return {
        "file_count": len(observed),
        "source_size_bytes": verified_bytes,
        "status": "pass",
    }


def _canonical_remote_map() -> dict[str, dict[str, Any]]:
    documents = load_pointer_documents(DEFAULT_POINTER_DIR)
    if {item["revision"] for item in documents.values()} != {P2_P4_REVISION}:
        raise DurabilityError("tracked P2-P4 pointers no longer pin the frozen revision")
    roots = {
        "corrected_us_annual": "artifacts/canonical/corrected_us_annual",
        "corrected_us_annual_3y_research_model": (
            "artifacts/canonical/corrected_us_annual_3y_research_model"
        ),
        "corrected_us_annual_3y_product": (
            "artifacts/canonical/corrected_us_annual_3y_product"
        ),
    }
    result: dict[str, dict[str, Any]] = {}
    for document in documents.values():
        root = roots[str(document["artifact_name"])]
        for record in document["files"]:
            source_path = f"{root}/{record['relative_path']}"
            result[source_path] = {
                "repository_path": record["repository_path"],
                "revision": P2_P4_REVISION,
                "size_bytes": record["size_bytes"],
                "sha256": record["sha256"],
            }
    return result


def _attach_existing_remote_records(
    sources: Sequence[dict[str, Any]],
    canonical_remote: Mapping[str, Mapping[str, Any]],
) -> None:
    """Attach immutable P2 remote objects, including the E1 destination alias."""
    for source in sources:
        group = str(source["artifact_group"])
        if group == "canonical_p2_p4":
            lookup = str(source["path"])
        elif group == "e1_preservation_calendar_alias":
            lookup = E1_CALENDAR_CANONICAL_SOURCE
            source["immutable_alias_source"] = lookup
        else:
            continue
        remote = canonical_remote.get(lookup)
        if remote is None or (
            remote["size_bytes"] != source["size_bytes"]
            or remote["sha256"] != source["sha256"]
        ):
            raise DurabilityError(f"immutable pointer/source drift: {source['path']}")
        source["existing_remote"] = dict(remote)


def _payload_record(
    bundle_root: Path,
    local_path: Path,
    remote_path: str,
    kind: str,
    source_paths: Sequence[str],
) -> dict[str, Any]:
    return {
        "id": local_path.stem,
        "kind": kind,
        "local_path": local_path.relative_to(ROOT).as_posix(),
        "repository_path": remote_path,
        "size_bytes": local_path.stat().st_size,
        "sha256": sha256_file(local_path),
        "source_file_count": len(source_paths),
        "source_paths_sha256": _canonical_sha(sorted(source_paths)),
    }


def _validate_checkpoint() -> dict[str, Any]:
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    branch = subprocess.check_output(
        ["git", "branch", "--show-current"], cwd=ROOT, text=True
    ).strip()
    tags = subprocess.check_output(
        ["git", "tag", "--points-at", "HEAD"], cwd=ROOT, text=True
    ).splitlines()
    if head != CHECKPOINT_COMMIT or branch != "codex/pit-checkpoint" or CHECKPOINT_TAG not in tags:
        raise DurabilityError(
            "DUR1 must freeze from the exact requested checkpoint, branch, and tag"
        )
    return {"commit": head, "branch": branch, "tag": CHECKPOINT_TAG}


def freeze_contract(
    contract_path: Path = DEFAULT_CONTRACT,
    bundle_root: Path = DEFAULT_BUNDLE_ROOT,
) -> dict[str, Any]:
    """Rehash sources, package them, rehash again, and freeze one contract."""
    if contract_path.exists():
        raise DurabilityError(f"contract already exists: {contract_path}")
    if bundle_root.exists():
        raise DurabilityError(f"bundle root already exists: {bundle_root}")
    checkpoint = _validate_checkpoint()
    group_records, sources = inventory_groups()
    source_by_group: dict[str, list[dict[str, Any]]] = {}
    for source in sources:
        source_by_group.setdefault(str(source["artifact_group"]), []).append(source)
    bundle_root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle_root.name}.", dir=bundle_root.parent))
    try:
        payloads: list[dict[str, Any]] = []
        for group in SOURCE_GROUPS:
            records = source_by_group[group.name]
            if group.storage in {"existing_remote", "existing_remote_alias"}:
                continue
            if group.storage == "a1_mixed":
                package_records = [
                    item for item in records
                    if "/packages/" in str(item["path"])
                ]
                support_records = [item for item in records if item not in package_records]
                for record in package_records:
                    source = ROOT / str(record["path"])
                    payloads.append(
                        _payload_record(
                            staging,
                            source,
                            f"{REMOTE_PREFIX}/a1-packages/{source.name}",
                            "direct_file",
                            [str(record["path"])],
                        )
                    )
                output = staging / "packages/a1_archive_support.tar"
                write_deterministic_tar(ROOT, support_records, output)
                verify_tar(output, support_records)
                payloads.append(
                    _payload_record(
                        staging,
                        output,
                        f"{REMOTE_PREFIX}/packages/{output.name}",
                        "tar",
                        [str(item["path"]) for item in support_records],
                    )
                )
                continue
            output = staging / f"packages/{group.name}.tar"
            write_deterministic_tar(ROOT, records, output)
            verify_tar(output, records)
            payloads.append(
                _payload_record(
                    staging,
                    output,
                    f"{REMOTE_PREFIX}/packages/{output.name}",
                    "tar",
                    [str(item["path"]) for item in records],
                )
            )

        second_groups, second_sources = inventory_groups()
        if group_records != second_groups or sources != second_sources:
            raise DurabilityError("source inventory drifted during package creation")
        _attach_existing_remote_records(sources, _canonical_remote_map())
        staging.rename(bundle_root)
        for payload in payloads:
            local = Path(str(payload["local_path"]))
            if local.is_relative_to(staging.relative_to(ROOT)):
                payload["local_path"] = (
                    bundle_root.relative_to(ROOT) / local.relative_to(staging.relative_to(ROOT))
                ).as_posix()
        source_bytes = sum(int(item["size_bytes"]) for item in sources)
        contract = {
            "schema_version": 1,
            "artifact_class": "US_FREE_DATA_V1_IMMUTABLE_DURABILITY_DUR1",
            "artifact_version": DURABILITY_VERSION,
            "created_at_utc": CREATED_AT_UTC,
            "checkpoint": checkpoint,
            "repository": DEFAULT_REPO_ID,
            "repository_type": REPO_TYPE,
            "remote_prefix": REMOTE_PREFIX,
            "contract_repository_path": CONTRACT_REMOTE_PATH,
            "existing_immutable_revision": P2_P4_REVISION,
            "existing_immutable_revision_must_remain_unchanged": True,
            "corrective_parent_revision": CORRECTIVE_PARENT_REVISION,
            "corrective_parent_revision_must_remain_unchanged": True,
            "mutable_revision_fallback": False,
            "target_policy": "absent_target_only",
            "source_group_count": len(group_records),
            "source_file_count": len(sources),
            "source_total_size_bytes": source_bytes,
            "source_inventory_sha256": _canonical_sha(sources),
            "groups": group_records,
            "sources": sources,
            "upload_payload_file_count": len(payloads) + 1,
            "upload_payload_size_bytes_excluding_contract": sum(
                int(item["size_bytes"]) for item in payloads
            ),
            "payloads": sorted(payloads, key=lambda item: item["repository_path"]),
            "recovery": {
                "command": (
                    "python3 -m data_io.us_free_v1_durability --recover "
                    "--revision <immutable-40-char-revision> --target <absent-path> "
                    "--evidence-output <absent-json>"
                ),
                "destination_root": "caller-supplied absent directory",
                "source_paths_are_recovered_relative_to_destination_root": True,
                "verifies_every_source_size_and_sha256": True,
                "verifies_private_visibility": True,
            },
            "preflight_requirements": {
                "rehash_every_source": True,
                "rehash_every_payload": True,
                "reject_symlinks": True,
                "reject_missing_or_extra_package_members": True,
                "reject_remote_path_collisions": True,
                "require_private_visibility": True,
                "require_write_credentials": True,
                "require_sufficient_local_and_private_remote_storage": True,
                "require_parent_pinned_single_commit": True,
            },
            "methodology_boundary": {
                "external_evidence_collection": False,
                "data_feature_target_model_score_rank_gate_liquidity_portfolio_event_change": False,
                "model_or_performance_execution": False,
                "release_or_promotion": False,
            },
        }
        contract_path.parent.mkdir(parents=True, exist_ok=True)
        with contract_path.open("xb") as handle:
            handle.write(_json_bytes(contract))
        verified = verify_contract(contract_path, verify_sources=True, verify_payloads=True)
        return {**verified, "bundle_root": str(bundle_root)}
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def load_contract(path: Path = DEFAULT_CONTRACT) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise DurabilityError(f"durability contract is missing or unsafe: {path}")
    try:
        contract = json.loads(path.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DurabilityError("durability contract is not readable JSON") from exc
    if (
        contract.get("artifact_version") != DURABILITY_VERSION
        or contract.get("checkpoint", {}).get("commit") != CHECKPOINT_COMMIT
        or contract.get("existing_immutable_revision") != P2_P4_REVISION
        or contract.get("corrective_parent_revision")
        != CORRECTIVE_PARENT_REVISION
        or contract.get("mutable_revision_fallback") is not False
    ):
        raise DurabilityError("durability contract identity drifted")
    return contract


def verify_contract(
    path: Path = DEFAULT_CONTRACT,
    *,
    verify_sources: bool,
    verify_payloads: bool,
) -> dict[str, Any]:
    contract = load_contract(path)
    sources = contract.get("sources")
    payloads = contract.get("payloads")
    if not isinstance(sources, list) or not isinstance(payloads, list):
        raise DurabilityError("contract source or payload inventory is invalid")
    source_paths: set[str] = set()
    source_bytes = 0
    for record in sources:
        relative = _safe_relative(str(record.get("path")), "contract source")
        destination = _safe_relative(
            str(record.get("recovery_destination")), "recovery destination"
        )
        if relative != destination or relative.as_posix() in source_paths:
            raise DurabilityError(f"duplicate or remapped source: {relative}")
        source_paths.add(relative.as_posix())
        size = record.get("size_bytes")
        digest = record.get("sha256")
        if not isinstance(size, int) or size < 0 or not _SHA256_RE.fullmatch(str(digest)):
            raise DurabilityError(f"invalid source size/hash: {relative}")
        if not record.get("reason") or not record.get("manifest_relationship_ids"):
            raise DurabilityError(f"undocumented source dependency: {relative}")
        existing = record.get("existing_remote")
        if existing is not None:
            if (
                existing.get("revision") != P2_P4_REVISION
                or existing.get("size_bytes") != size
                or existing.get("sha256") != digest
            ):
                raise DurabilityError(f"immutable source relationship drifted: {relative}")
            _safe_relative(str(existing.get("repository_path")), "existing remote path")
        if record.get("artifact_group") == "e1_preservation_calendar_alias":
            if (
                relative.as_posix() != E1_CALENDAR_ALIAS_PATH
                or record.get("immutable_alias_source")
                != E1_CALENDAR_CANONICAL_SOURCE
                or existing is None
            ):
                raise DurabilityError("E1 calendar alias relationship drifted")
        source_bytes += size
        if verify_sources:
            local = ROOT.joinpath(*relative.parts)
            if (
                not local.is_file()
                or local.is_symlink()
                or local.stat().st_size != size
                or sha256_file(local) != digest
            ):
                raise DurabilityError(f"source drifted: {relative}")
    if (
        contract.get("source_file_count") != len(sources)
        or contract.get("source_total_size_bytes") != source_bytes
        or contract.get("source_inventory_sha256") != _canonical_sha(sources)
    ):
        raise DurabilityError("contract source aggregate drifted")
    remote_paths = {CONTRACT_REMOTE_PATH}
    for payload in payloads:
        remote = _safe_relative(str(payload.get("repository_path")), "remote path").as_posix()
        if remote in remote_paths:
            raise DurabilityError(f"duplicate remote destination: {remote}")
        remote_paths.add(remote)
        if verify_payloads:
            local = ROOT / _safe_relative(str(payload.get("local_path")), "payload path")
            if (
                not local.is_file()
                or local.is_symlink()
                or local.stat().st_size != payload.get("size_bytes")
                or sha256_file(local) != payload.get("sha256")
            ):
                raise DurabilityError(f"payload drifted: {local}")
            if payload.get("kind") == "tar":
                member_names = _tar_member_names(local)
                group_sources = [
                    item for item in sources
                    if item["path"] in member_names
                ]
                proof = verify_tar(local, group_sources)
                if (
                    proof["file_count"] != payload.get("source_file_count")
                    or _canonical_sha(sorted(item["path"] for item in group_sources))
                    != payload.get("source_paths_sha256")
                ):
                    raise DurabilityError(f"payload/source relationship drifted: {local}")
    return {
        "status": "pass",
        "contract_sha256": sha256_file(path),
        "contract_size_bytes": path.stat().st_size,
        "source_group_count": contract["source_group_count"],
        "source_file_count": len(sources),
        "source_total_size_bytes": source_bytes,
        "upload_payload_file_count": len(payloads) + 1,
        "upload_payload_size_bytes_excluding_contract": sum(
            int(item["size_bytes"]) for item in payloads
        ),
    }


def _tar_member_names(path: Path) -> set[str]:
    with tarfile.open(path, "r") as archive:
        return {member.name for member in archive if member.isfile()}


def _token_has_write_scope(identity: Mapping[str, Any]) -> bool:
    token = identity.get("auth", {}).get("accessToken", {})
    role = str(token.get("role", "")).lower()
    if role in {"write", "admin"}:
        return True
    display = str(token).lower()
    return "write" in display or "admin" in display


def remote_preflight(
    contract_path: Path,
    *,
    api: Any,
    token: str,
    private_storage_quota_bytes: int,
) -> dict[str, Any]:
    """Run a read-only credential/privacy/collision/storage preflight."""
    local = verify_contract(contract_path, verify_sources=True, verify_payloads=True)
    contract = load_contract(contract_path)
    parent = verify_private_repository(api, repo_id=DEFAULT_REPO_ID, token=token)
    if parent != CORRECTIVE_PARENT_REVISION:
        raise DurabilityError(
            "corrective publication parent drifted: "
            f"expected={CORRECTIVE_PARENT_REVISION} observed={parent}"
        )
    try:
        identity = api.whoami(token=token)
        files = list(
            api.list_repo_tree(
                repo_id=DEFAULT_REPO_ID,
                repo_type=REPO_TYPE,
                revision=parent,
                recursive=True,
                expand=True,
                token=token,
            )
        )
    except Exception as exc:
        raise DurabilityError("authenticated repository/storage preflight failed") from exc
    if not _token_has_write_scope(identity):
        raise DurabilityError("Hugging Face token write scope could not be proven")
    existing_paths = {str(getattr(item, "path", "")) for item in files}
    destinations = {CONTRACT_REMOTE_PATH} | {
        str(item["repository_path"]) for item in contract["payloads"]
    }
    collisions = sorted(destinations & existing_paths)
    if collisions:
        raise DurabilityError(
            f"immutable durability destinations already exist: {collisions[:5]}"
        )
    remote_logical_bytes = sum(
        int(getattr(item, "size", 0) or 0)
        for item in files
        if not str(getattr(item, "path", "")).endswith("/")
    )
    required_remote = (
        remote_logical_bytes
        + int(local["upload_payload_size_bytes_excluding_contract"])
        + int(local["contract_size_bytes"])
    )
    if private_storage_quota_bytes <= 0 or required_remote > private_storage_quota_bytes:
        raise DurabilityError(
            "configured private remote storage is insufficient: "
            f"required={required_remote} quota={private_storage_quota_bytes}"
        )
    local_free = shutil.disk_usage(DEFAULT_BUNDLE_ROOT).free
    recovery_headroom = int(contract["source_total_size_bytes"]) * 2
    if local_free < recovery_headroom:
        raise DurabilityError(
            f"local recovery storage is insufficient: free={local_free} required={recovery_headroom}"
        )
    return {
        **local,
        "repository": DEFAULT_REPO_ID,
        "repository_type": REPO_TYPE,
        "private": True,
        "parent_revision": parent,
        "token_write_scope_verified": True,
        "destination_paths_absent": True,
        "destination_file_count": len(destinations),
        "remote_logical_bytes_before": remote_logical_bytes,
        "remote_required_logical_bytes_after": required_remote,
        "private_storage_quota_bytes": private_storage_quota_bytes,
        "private_storage_headroom_bytes_after": private_storage_quota_bytes - required_remote,
        "local_free_bytes": local_free,
        "local_recovery_headroom_required_bytes": recovery_headroom,
        "old_p2_p4_revision": P2_P4_REVISION,
        "old_p2_p4_revision_mutated": False,
        "remote_operation": "read_only_preflight",
    }


def publish(
    contract_path: Path,
    *,
    api: Any,
    token: str,
    private_storage_quota_bytes: int,
    expected_contract_sha256: str,
) -> dict[str, Any]:
    """Create exactly one non-overwriting, parent-pinned private commit."""
    if not _SHA256_RE.fullmatch(expected_contract_sha256):
        raise DurabilityError("exact lowercase contract SHA-256 confirmation is required")
    if sha256_file(contract_path) != expected_contract_sha256:
        raise DurabilityError("contract SHA-256 confirmation mismatch")
    preflight = remote_preflight(
        contract_path,
        api=api,
        token=token,
        private_storage_quota_bytes=private_storage_quota_bytes,
    )
    contract = load_contract(contract_path)
    try:
        from huggingface_hub import CommitOperationAdd
    except ImportError as exc:
        raise DurabilityError("huggingface_hub is required for publication") from exc
    operations = [
        CommitOperationAdd(path_in_repo=CONTRACT_REMOTE_PATH, path_or_fileobj=contract_path)
    ]
    operations.extend(
        CommitOperationAdd(
            path_in_repo=str(item["repository_path"]),
            path_or_fileobj=ROOT / str(item["local_path"]),
        )
        for item in contract["payloads"]
    )
    try:
        commit = api.create_commit(
            repo_id=DEFAULT_REPO_ID,
            repo_type=REPO_TYPE,
            revision="main",
            operations=operations,
            commit_message=f"Freeze US free-data V1 durability bundle {DURABILITY_VERSION}",
            commit_description=(
                "Artifact durability only. Non-overwriting versioned payloads; "
                "existing immutable P2-P4 revision remains unchanged."
            ),
            token=token,
            parent_commit=preflight["parent_revision"],
            create_pr=False,
        )
    except Exception as exc:
        raise DurabilityError("single Hugging Face durability commit failed") from exc
    revision = str(getattr(commit, "oid", ""))
    if not _REVISION_RE.fullmatch(revision):
        raise DurabilityError("Hugging Face did not return an immutable commit hash")
    verified_revision = verify_private_repository(
        api, repo_id=DEFAULT_REPO_ID, token=token, revision=revision
    )
    if verified_revision != revision:
        raise DurabilityError("published revision identity mismatch")
    return {
        "status": "pass",
        "repository": DEFAULT_REPO_ID,
        "repository_type": REPO_TYPE,
        "revision": revision,
        "visibility": "private_verified",
        "parent_revision": preflight["parent_revision"],
        "contract_sha256": expected_contract_sha256,
        "uploaded_file_count": len(operations),
        "uploaded_payload_size_bytes": (
            preflight["upload_payload_size_bytes_excluding_contract"]
            + preflight["contract_size_bytes"]
        ),
        "old_p2_p4_revision": P2_P4_REVISION,
        "old_p2_p4_revision_mutated": False,
        "non_overwriting": True,
        "single_commit": True,
        "preflight": preflight,
    }


def _copy_verified(source: Path, destination: Path, record: Mapping[str, Any]) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as output, source.open("rb") as input_file:
        shutil.copyfileobj(input_file, output)
    if (
        destination.stat().st_size != record["size_bytes"]
        or sha256_file(destination) != record["sha256"]
    ):
        raise DurabilityError(f"downloaded payload drifted: {record['repository_path']}")


def _extract_verified_tar(
    archive_path: Path,
    staging: Path,
    expected: Mapping[str, Mapping[str, Any]],
) -> None:
    observed: set[str] = set()
    with tarfile.open(archive_path, "r") as archive:
        for member in archive:
            relative = _safe_relative(member.name, "recovery tar member")
            name = relative.as_posix()
            if (
                not member.isfile()
                or member.issym()
                or member.islnk()
                or name not in expected
                or name in observed
            ):
                raise DurabilityError(f"unsafe or undocumented tar member: {name}")
            destination = staging.joinpath(*relative.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            extracted = archive.extractfile(member)
            if extracted is None:
                raise DurabilityError(f"cannot extract tar member: {name}")
            with destination.open("xb") as output:
                shutil.copyfileobj(extracted, output)
            record = expected[name]
            if (
                destination.stat().st_size != record["size_bytes"]
                or sha256_file(destination) != record["sha256"]
            ):
                raise DurabilityError(f"recovered tar member drifted: {name}")
            observed.add(name)


def recover(
    contract_path: Path,
    *,
    revision: str,
    target: Path,
    api: Any,
    token: str,
    downloader: Callable[..., str | Path],
) -> dict[str, Any]:
    """Recover the complete published bundle into one absent target."""
    if not _REVISION_RE.fullmatch(revision):
        raise DurabilityError("recovery requires a full immutable 40-character revision")
    if target.exists():
        raise DurabilityError(f"recovery target already exists: {target}")
    contract = load_contract(contract_path)
    contract_sha = sha256_file(contract_path)
    verify_private_repository(api, repo_id=DEFAULT_REPO_ID, token=token, revision=revision)
    remote_contract = Path(
        downloader(
            repo_id=DEFAULT_REPO_ID,
            filename=CONTRACT_REMOTE_PATH,
            repo_type=REPO_TYPE,
            revision=revision,
            token=token,
        )
    )
    if sha256_file(remote_contract) != contract_sha:
        raise DurabilityError("remote durability contract differs from tracked contract")
    sources = {str(item["path"]): item for item in contract["sources"]}
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    downloaded_files = downloaded_bytes = 0
    try:
        for payload in contract["payloads"]:
            cached = Path(
                downloader(
                    repo_id=DEFAULT_REPO_ID,
                    filename=payload["repository_path"],
                    repo_type=REPO_TYPE,
                    revision=revision,
                    token=token,
                )
            )
            if (
                cached.stat().st_size != payload["size_bytes"]
                or sha256_file(cached) != payload["sha256"]
            ):
                raise DurabilityError(f"downloaded payload drifted: {payload['repository_path']}")
            downloaded_files += 1
            downloaded_bytes += int(payload["size_bytes"])
            if payload["kind"] == "tar":
                member_names = _tar_member_names(cached)
                expected = {name: sources[name] for name in member_names if name in sources}
                if len(expected) != payload["source_file_count"]:
                    raise DurabilityError(f"tar source relationship drifted: {payload['id']}")
                _extract_verified_tar(cached, staging, expected)
            elif payload["kind"] == "direct_file":
                matching = [
                    item for item in sources.values()
                    if item["sha256"] == payload["sha256"]
                    and item["size_bytes"] == payload["size_bytes"]
                    and _canonical_sha([item["path"]]) == payload["source_paths_sha256"]
                ]
                if len(matching) != 1:
                    raise DurabilityError(f"direct payload relationship drifted: {payload['id']}")
                record = matching[0]
                destination = staging / str(record["recovery_destination"])
                _copy_verified(cached, destination, payload)
            else:
                raise DurabilityError(f"unsupported payload kind: {payload['kind']}")
        for record in sources.values():
            existing = record.get("existing_remote")
            if not existing:
                continue
            cached = Path(
                downloader(
                    repo_id=DEFAULT_REPO_ID,
                    filename=existing["repository_path"],
                    repo_type=REPO_TYPE,
                    revision=revision,
                    token=token,
                )
            )
            destination = staging / str(record["recovery_destination"])
            _copy_verified(cached, destination, existing)
            downloaded_files += 1
            downloaded_bytes += int(existing["size_bytes"])
        discovered = {
            path.relative_to(staging).as_posix()
            for path in staging.rglob("*")
            if path.is_file()
        }
        if discovered != set(sources):
            raise DurabilityError(
                f"recovered file set drifted: missing={sorted(set(sources)-discovered)[:5]} "
                f"extra={sorted(discovered-set(sources))[:5]}"
            )
        for relative, record in sources.items():
            path = staging / relative
            if (
                path.is_symlink()
                or path.stat().st_size != record["size_bytes"]
                or sha256_file(path) != record["sha256"]
            ):
                raise DurabilityError(f"final recovered source drifted: {relative}")
        staging.rename(target)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return {
        "status": "pass",
        "repository": DEFAULT_REPO_ID,
        "repository_type": REPO_TYPE,
        "revision": revision,
        "visibility": "private_verified",
        "contract_sha256": contract_sha,
        "source_file_count": len(sources),
        "source_total_size_bytes": contract["source_total_size_bytes"],
        "downloaded_file_count": downloaded_files + 1,
        "downloaded_payload_size_bytes_excluding_contract": downloaded_bytes,
        "target": str(target),
        "mutable_revision_fallback": False,
        "remote_operation": "read_only",
        "remote_state_mutated": False,
        "old_p2_p4_revision": P2_P4_REVISION,
        "old_p2_p4_revision_mutated": False,
    }


def _write_json_exclusive(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(_json_bytes(value))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--freeze-contract", action="store_true")
    actions.add_argument("--verify-contract", action="store_true")
    actions.add_argument("--preflight", action="store_true")
    actions.add_argument("--publish", action="store_true")
    actions.add_argument("--recover", action="store_true")
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--bundle-root", type=Path, default=DEFAULT_BUNDLE_ROOT)
    parser.add_argument("--private-storage-quota-bytes", type=int)
    parser.add_argument("--expected-contract-sha256")
    parser.add_argument("--revision")
    parser.add_argument("--target", type=Path)
    parser.add_argument("--evidence-output", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    contract = args.contract.resolve()
    if args.freeze_contract:
        result = freeze_contract(contract, args.bundle_root.resolve())
    elif args.verify_contract:
        result = verify_contract(contract, verify_sources=True, verify_payloads=True)
    else:
        if not args.private_storage_quota_bytes and not args.recover:
            raise DurabilityError("remote preflight/publish requires --private-storage-quota-bytes")
        from huggingface_hub import HfApi, hf_hub_download

        token = require_hf_token()
        api = HfApi()
        if args.preflight:
            result = remote_preflight(
                contract,
                api=api,
                token=token,
                private_storage_quota_bytes=args.private_storage_quota_bytes,
            )
        elif args.publish:
            if not args.expected_contract_sha256:
                raise DurabilityError("publish requires --expected-contract-sha256")
            result = publish(
                contract,
                api=api,
                token=token,
                private_storage_quota_bytes=args.private_storage_quota_bytes,
                expected_contract_sha256=args.expected_contract_sha256,
            )
        else:
            if not args.revision or not args.target or not args.evidence_output:
                raise DurabilityError(
                    "recover requires --revision, --target, and --evidence-output"
                )
            if args.evidence_output.exists():
                raise DurabilityError(
                    f"recovery evidence target already exists: {args.evidence_output}"
                )
            result = recover(
                contract,
                revision=args.revision,
                target=args.target.resolve(),
                api=api,
                token=token,
                downloader=hf_hub_download,
            )
            _write_json_exclusive(args.evidence_output.resolve(), result)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
