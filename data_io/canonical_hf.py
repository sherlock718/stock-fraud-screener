"""Private, immutable Hugging Face storage for canonical P2-P4 artifacts.

This module is intentionally separate from the legacy root-level Hugging Face
push/pull utilities.  Canonical publication is content-addressed by each
artifact manifest hash, refuses existing remote paths, and produces tracked
pointer manifests only after an immutable Hub commit can be downloaded and
verified byte for byte.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Sequence

from _root import ROOT


DEFAULT_REPO_ID = "ekrash718/stock-screener-data"
REPO_TYPE = "dataset"
POINTER_SCHEMA_VERSION = 1
DEFAULT_POINTER_DIR = ROOT / "data_io/canonical_artifact_pointers"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")


class CanonicalArtifactError(RuntimeError):
    """A fail-closed canonical publication or retrieval error."""


@dataclass(frozen=True)
class ArtifactSpec:
    stage: str
    artifact_name: str
    artifact_root: Path
    manifest_sha256: str
    pointer_filename: str


@dataclass(frozen=True)
class PublicationFile:
    stage: str
    artifact_name: str
    source_path: str
    local_path: Path
    relative_path: str
    repository_path: str
    size_bytes: int
    sha256: str

    def public_record(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "repository_path": self.repository_path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
        }

    def plan_record(self) -> dict[str, Any]:
        return {
            "source_path": self.source_path,
            **self.public_record(),
        }


@dataclass(frozen=True)
class ArtifactPublication:
    spec: ArtifactSpec
    repository_prefix: str
    files: tuple[PublicationFile, ...]

    @property
    def total_size_bytes(self) -> int:
        return sum(item.size_bytes for item in self.files)

    def plan_record(self) -> dict[str, Any]:
        return {
            "stage": self.spec.stage,
            "artifact_name": self.spec.artifact_name,
            "manifest_sha256": self.spec.manifest_sha256,
            "repository_prefix": self.repository_prefix,
            "file_count": len(self.files),
            "total_size_bytes": self.total_size_bytes,
            "files": [item.plan_record() for item in self.files],
        }


@dataclass(frozen=True)
class PublicationPlan:
    repository: str
    repository_type: str
    workspace_root: Path
    artifacts: tuple[ArtifactPublication, ...]

    @property
    def files(self) -> tuple[PublicationFile, ...]:
        return tuple(
            item
            for artifact in self.artifacts
            for item in artifact.files
        )

    @property
    def total_size_bytes(self) -> int:
        return sum(item.total_size_bytes for item in self.artifacts)

    def public_record(self) -> dict[str, Any]:
        return {
            "schema_version": POINTER_SCHEMA_VERSION,
            "repository": self.repository,
            "repository_type": self.repository_type,
            "path_policy": (
                "canonical/<artifact-name>/<manifest-sha256>/"
                "<artifact-relative-path>"
            ),
            "artifact_count": len(self.artifacts),
            "file_count": len(self.files),
            "total_size_bytes": self.total_size_bytes,
            "artifacts": [
                artifact.plan_record() for artifact in self.artifacts
            ],
        }


DEFAULT_ARTIFACT_SPECS = (
    ArtifactSpec(
        stage="P2",
        artifact_name="corrected_us_annual",
        artifact_root=(
            ROOT / "artifacts/canonical/corrected_us_annual"
        ),
        manifest_sha256=(
            "40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee"
            "7a3c2b3"
        ),
        pointer_filename="p2.json",
    ),
    ArtifactSpec(
        stage="P3",
        artifact_name="corrected_us_annual_3y_research_model",
        artifact_root=(
            ROOT
            / "artifacts/canonical/"
            "corrected_us_annual_3y_research_model"
        ),
        manifest_sha256=(
            "8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc5"
            "49b80f2"
        ),
        pointer_filename="p3.json",
    ),
    ArtifactSpec(
        stage="P4",
        artifact_name="corrected_us_annual_3y_product",
        artifact_root=(
            ROOT
            / "artifacts/canonical/corrected_us_annual_3y_product"
        ),
        manifest_sha256=(
            "28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1e"
            "dd1aabc7"
        ),
        pointer_filename="p4.json",
    ),
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_hf_token() -> str:
    """Return environment or standard Hugging Face auth without printing it."""
    token = os.environ.get("HF_TOKEN")
    if not token:
        try:
            from huggingface_hub import get_token
        except ImportError as exc:
            raise CanonicalArtifactError(
                "huggingface_hub is required to load saved authentication"
            ) from exc
        token = get_token()
    if not token:
        raise CanonicalArtifactError(
            "HF_TOKEN or a saved Hugging Face token is required for "
            "authenticated private-repository verification"
        )
    return token


def _validate_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise CanonicalArtifactError(f"{label} is not a lowercase SHA-256")
    return value


def _validate_revision(value: Any) -> str:
    if not isinstance(value, str) or not _REVISION_RE.fullmatch(value):
        raise CanonicalArtifactError(
            "Hugging Face revision must be a full immutable 40-character "
            "lowercase commit SHA"
        )
    return value


def _safe_relative_path(value: Any, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value:
        raise CanonicalArtifactError(f"{label} must be a non-empty path")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or ".." in path.parts
        or "." in path.parts
        or path.as_posix() != value
    ):
        raise CanonicalArtifactError(f"{label} is not a safe relative path")
    return path


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CanonicalArtifactError(f"{label} is not readable JSON") from exc
    if not isinstance(value, dict):
        raise CanonicalArtifactError(f"{label} must contain a JSON object")
    return value


def _artifact_publication(
    spec: ArtifactSpec,
    workspace_root: Path,
) -> ArtifactPublication:
    workspace_root = workspace_root.resolve()
    artifact_root = spec.artifact_root.resolve()
    try:
        artifact_root_relative = artifact_root.relative_to(workspace_root)
    except ValueError as exc:
        raise CanonicalArtifactError(
            f"{spec.stage} artifact root is outside the workspace"
        ) from exc

    _validate_sha256(
        spec.manifest_sha256,
        f"{spec.stage} accepted manifest hash",
    )
    manifest_path = artifact_root / "manifest.json"
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise CanonicalArtifactError(
            f"{spec.stage} manifest is missing or is a symlink"
        )
    actual_manifest_sha = sha256_file(manifest_path)
    if actual_manifest_sha != spec.manifest_sha256:
        raise CanonicalArtifactError(
            f"{spec.stage} manifest hash mismatch: "
            f"expected={spec.manifest_sha256} "
            f"actual={actual_manifest_sha}"
        )

    manifest = _read_json(manifest_path, f"{spec.stage} manifest")
    declared_records = manifest.get("records")
    if not isinstance(declared_records, list):
        raise CanonicalArtifactError(
            f"{spec.stage} manifest records must be a list"
        )

    root_posix = PurePosixPath(artifact_root_relative.as_posix())
    repository_prefix = (
        f"canonical/{spec.artifact_name}/{spec.manifest_sha256}"
    )
    files: list[PublicationFile] = [
        PublicationFile(
            stage=spec.stage,
            artifact_name=spec.artifact_name,
            source_path=(
                artifact_root_relative / "manifest.json"
            ).as_posix(),
            local_path=manifest_path,
            relative_path="manifest.json",
            repository_path=f"{repository_prefix}/manifest.json",
            size_bytes=manifest_path.stat().st_size,
            sha256=actual_manifest_sha,
        )
    ]
    declared_relative_paths = {"manifest.json"}

    for index, record in enumerate(declared_records):
        if not isinstance(record, dict):
            raise CanonicalArtifactError(
                f"{spec.stage} record {index} must be a JSON object"
            )
        declared_path = _safe_relative_path(
            record.get("path"),
            f"{spec.stage} record {index} path",
        )
        try:
            relative_path = declared_path.relative_to(root_posix)
        except ValueError as exc:
            raise CanonicalArtifactError(
                f"{spec.stage} record {index} is outside its artifact root"
            ) from exc
        relative_text = relative_path.as_posix()
        if relative_text in declared_relative_paths:
            raise CanonicalArtifactError(
                f"{spec.stage} duplicate record path: {relative_text}"
            )
        declared_relative_paths.add(relative_text)

        size_bytes = record.get("size_bytes")
        if (
            not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes < 0
        ):
            raise CanonicalArtifactError(
                f"{spec.stage} record {index} has invalid size_bytes"
            )
        expected_sha = _validate_sha256(
            record.get("sha256"),
            f"{spec.stage} record {index} hash",
        )
        local_path = artifact_root.joinpath(*relative_path.parts)
        try:
            local_path.resolve().relative_to(artifact_root)
        except ValueError as exc:
            raise CanonicalArtifactError(
                f"{spec.stage} record {index} resolves outside its root"
            ) from exc
        if not local_path.is_file() or local_path.is_symlink():
            raise CanonicalArtifactError(
                f"{spec.stage} record is missing or is a symlink: "
                f"{relative_text}"
            )
        actual_size = local_path.stat().st_size
        if actual_size != size_bytes:
            raise CanonicalArtifactError(
                f"{spec.stage} size mismatch for {relative_text}: "
                f"expected={size_bytes} actual={actual_size}"
            )
        actual_sha = sha256_file(local_path)
        if actual_sha != expected_sha:
            raise CanonicalArtifactError(
                f"{spec.stage} hash mismatch for {relative_text}: "
                f"expected={expected_sha} actual={actual_sha}"
            )
        files.append(
            PublicationFile(
                stage=spec.stage,
                artifact_name=spec.artifact_name,
                source_path=declared_path.as_posix(),
                local_path=local_path,
                relative_path=relative_text,
                repository_path=(
                    f"{repository_prefix}/{relative_text}"
                ),
                size_bytes=size_bytes,
                sha256=expected_sha,
            )
        )

    actual_relative_paths: set[str] = set()
    for path in artifact_root.rglob("*"):
        if path.is_symlink():
            raise CanonicalArtifactError(
                f"{spec.stage} artifact contains a symlink: "
                f"{path.relative_to(artifact_root).as_posix()}"
            )
        if path.is_file():
            actual_relative_paths.add(
                path.relative_to(artifact_root).as_posix()
            )
    if actual_relative_paths != declared_relative_paths:
        missing = sorted(declared_relative_paths - actual_relative_paths)
        extra = sorted(actual_relative_paths - declared_relative_paths)
        raise CanonicalArtifactError(
            f"{spec.stage} manifest/root file-set mismatch: "
            f"missing={missing} extra={extra}"
        )

    files.sort(key=lambda item: item.relative_path)
    return ArtifactPublication(
        spec=spec,
        repository_prefix=repository_prefix,
        files=tuple(files),
    )


def build_publication_plan(
    *,
    repo_id: str = DEFAULT_REPO_ID,
    specs: Sequence[ArtifactSpec] = DEFAULT_ARTIFACT_SPECS,
    workspace_root: Path = ROOT,
) -> PublicationPlan:
    """Validate and enumerate the exact local P2-P4 publication."""
    if not isinstance(repo_id, str) or "/" not in repo_id:
        raise CanonicalArtifactError(
            "repository must be a Hugging Face namespace/name identifier"
        )
    if not specs:
        raise CanonicalArtifactError("at least one artifact is required")
    stages = [spec.stage for spec in specs]
    names = [spec.artifact_name for spec in specs]
    pointers = [spec.pointer_filename for spec in specs]
    if (
        len(stages) != len(set(stages))
        or len(names) != len(set(names))
        or len(pointers) != len(set(pointers))
    ):
        raise CanonicalArtifactError(
            "artifact stages, names, and pointer filenames must be unique"
        )
    artifacts = tuple(
        _artifact_publication(spec, workspace_root) for spec in specs
    )
    repository_paths = [
        item.repository_path
        for artifact in artifacts
        for item in artifact.files
    ]
    if len(repository_paths) != len(set(repository_paths)):
        raise CanonicalArtifactError(
            "publication contains duplicate repository paths"
        )
    return PublicationPlan(
        repository=repo_id,
        repository_type=REPO_TYPE,
        workspace_root=workspace_root.resolve(),
        artifacts=artifacts,
    )


def write_publication_plan(plan: PublicationPlan, output_path: Path) -> None:
    """Write a deterministic local plan without overwriting an existing file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output_path.open("x", encoding="utf-8") as handle:
            json.dump(
                plan.public_record(),
                handle,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
    except FileExistsError as exc:
        raise CanonicalArtifactError(
            f"publication plan target already exists: {output_path}"
        ) from exc


def verify_private_repository(
    api: Any,
    *,
    repo_id: str,
    token: str,
    revision: str | None = None,
) -> str:
    """Fail closed unless authenticated metadata proves the repo is private."""
    if not token:
        raise CanonicalArtifactError(
            "authenticated repository visibility verification requires "
            "HF_TOKEN"
        )
    if revision is not None:
        _validate_revision(revision)
    try:
        info = api.repo_info(
            repo_id=repo_id,
            repo_type=REPO_TYPE,
            revision=revision,
            token=token,
        )
    except Exception as exc:
        raise CanonicalArtifactError(
            "Hugging Face repository visibility could not be verified; "
            "publication/retrieval is blocked"
        ) from exc
    if getattr(info, "private", None) is not True:
        raise CanonicalArtifactError(
            "Hugging Face repository is public or its private visibility "
            "could not be proven"
        )
    actual_revision = _validate_revision(getattr(info, "sha", None))
    if revision is not None and actual_revision != revision:
        raise CanonicalArtifactError(
            "Hugging Face returned a different revision than the immutable "
            "pointer"
        )
    return actual_revision


def publication_preflight(
    plan: PublicationPlan,
    *,
    api: Any,
    token: str,
) -> dict[str, Any]:
    """Verify privacy and absence of every destination at one pinned head."""
    parent_revision = verify_private_repository(
        api,
        repo_id=plan.repository,
        token=token,
    )
    try:
        existing = set(
            api.list_repo_files(
                repo_id=plan.repository,
                repo_type=plan.repository_type,
                revision=parent_revision,
                token=token,
            )
        )
    except Exception as exc:
        raise CanonicalArtifactError(
            "remote repository paths could not be enumerated at the "
            "verified parent revision"
        ) from exc
    collisions = sorted(
        {item.repository_path for item in plan.files} & existing
    )
    if collisions:
        preview = collisions[:5]
        raise CanonicalArtifactError(
            "immutable canonical destination already exists; refusing to "
            f"overwrite {len(collisions)} path(s): {preview}"
        )
    return {
        "repository": plan.repository,
        "repository_type": plan.repository_type,
        "private": True,
        "parent_revision": parent_revision,
        "destination_file_count": len(plan.files),
        "destination_paths_absent": True,
    }


def publish_plan(
    plan: PublicationPlan,
    *,
    api: Any,
    token: str,
    commit_message: str,
    operation_factory: Callable[..., Any] | None = None,
) -> tuple[str, dict[str, dict[str, Any]], dict[str, Any]]:
    """Create one parent-pinned Hub commit for the complete P2-P4 plan."""
    preflight = publication_preflight(plan, api=api, token=token)
    if operation_factory is None:
        try:
            from huggingface_hub import CommitOperationAdd
        except ImportError as exc:
            raise CanonicalArtifactError(
                "huggingface_hub is required for publication"
            ) from exc
        operation_factory = CommitOperationAdd
    operations = [
        operation_factory(
            path_in_repo=item.repository_path,
            path_or_fileobj=item.local_path,
        )
        for item in plan.files
    ]
    try:
        result = api.create_commit(
            repo_id=plan.repository,
            repo_type=plan.repository_type,
            revision="main",
            operations=operations,
            commit_message=commit_message,
            commit_description=(
                "Private immutable canonical P2-P4 baseline; paths are "
                "content-addressed by accepted artifact manifest SHA-256."
            ),
            token=token,
            parent_commit=preflight["parent_revision"],
            create_pr=False,
        )
    except Exception as exc:
        raise CanonicalArtifactError(
            "canonical Hugging Face commit failed; no pointer manifests "
            "were written"
        ) from exc
    revision = _validate_revision(getattr(result, "oid", None))
    pointers = create_pointer_documents(plan, revision=revision)
    return revision, pointers, preflight


def create_pointer_documents(
    plan: PublicationPlan,
    *,
    revision: str,
) -> dict[str, dict[str, Any]]:
    """Create small immutable-revision pointer documents in memory."""
    revision = _validate_revision(revision)
    documents: dict[str, dict[str, Any]] = {}
    for artifact in plan.artifacts:
        files = [item.public_record() for item in artifact.files]
        document = {
            "schema_version": POINTER_SCHEMA_VERSION,
            "stage": artifact.spec.stage,
            "artifact_name": artifact.spec.artifact_name,
            "manifest_sha256": artifact.spec.manifest_sha256,
            "repository": plan.repository,
            "repository_type": plan.repository_type,
            "revision": revision,
            "repository_prefix": artifact.repository_prefix,
            "file_count": len(files),
            "total_size_bytes": artifact.total_size_bytes,
            "files": files,
        }
        documents[artifact.spec.pointer_filename] = document
    return documents


def _validate_pointer_document(
    document: Mapping[str, Any],
    spec: ArtifactSpec,
    *,
    expected_repo_id: str,
) -> dict[str, Any]:
    if document.get("schema_version") != POINTER_SCHEMA_VERSION:
        raise CanonicalArtifactError(
            f"{spec.stage} pointer schema version is unsupported"
        )
    for key, expected in (
        ("stage", spec.stage),
        ("artifact_name", spec.artifact_name),
        ("manifest_sha256", spec.manifest_sha256),
        ("repository_type", REPO_TYPE),
    ):
        if document.get(key) != expected:
            raise CanonicalArtifactError(
                f"{spec.stage} pointer {key} does not match the accepted "
                "contract"
            )
    repository = document.get("repository")
    if repository != expected_repo_id:
        raise CanonicalArtifactError(
            f"{spec.stage} pointer repository does not match the accepted "
            f"destination {expected_repo_id}"
        )
    revision = _validate_revision(document.get("revision"))
    expected_prefix = (
        f"canonical/{spec.artifact_name}/{spec.manifest_sha256}"
    )
    if document.get("repository_prefix") != expected_prefix:
        raise CanonicalArtifactError(
            f"{spec.stage} pointer repository prefix is not "
            "content-addressed as required"
        )
    files = document.get("files")
    if not isinstance(files, list) or not files:
        raise CanonicalArtifactError(
            f"{spec.stage} pointer must contain file records"
        )
    validated_files: list[dict[str, Any]] = []
    relative_paths: set[str] = set()
    repository_paths: set[str] = set()
    for index, record in enumerate(files):
        if not isinstance(record, dict):
            raise CanonicalArtifactError(
                f"{spec.stage} pointer file {index} is not an object"
            )
        relative_path = _safe_relative_path(
            record.get("relative_path"),
            f"{spec.stage} pointer file {index} relative_path",
        ).as_posix()
        repository_path = _safe_relative_path(
            record.get("repository_path"),
            f"{spec.stage} pointer file {index} repository_path",
        ).as_posix()
        if repository_path != f"{expected_prefix}/{relative_path}":
            raise CanonicalArtifactError(
                f"{spec.stage} pointer file {index} repository path does "
                "not match its immutable prefix"
            )
        if (
            relative_path in relative_paths
            or repository_path in repository_paths
        ):
            raise CanonicalArtifactError(
                f"{spec.stage} pointer contains a duplicate file path"
            )
        relative_paths.add(relative_path)
        repository_paths.add(repository_path)
        size_bytes = record.get("size_bytes")
        if (
            not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes < 0
        ):
            raise CanonicalArtifactError(
                f"{spec.stage} pointer file {index} has invalid size"
            )
        record_sha = _validate_sha256(
            record.get("sha256"),
            f"{spec.stage} pointer file {index} hash",
        )
        validated_files.append(
            {
                "relative_path": relative_path,
                "repository_path": repository_path,
                "size_bytes": size_bytes,
                "sha256": record_sha,
            }
        )
    manifest_records = [
        item
        for item in validated_files
        if item["relative_path"] == "manifest.json"
    ]
    if (
        len(manifest_records) != 1
        or manifest_records[0]["sha256"] != spec.manifest_sha256
    ):
        raise CanonicalArtifactError(
            f"{spec.stage} pointer does not pin the accepted manifest"
        )
    if document.get("file_count") != len(validated_files):
        raise CanonicalArtifactError(
            f"{spec.stage} pointer file_count is inconsistent"
        )
    total_size = sum(item["size_bytes"] for item in validated_files)
    if document.get("total_size_bytes") != total_size:
        raise CanonicalArtifactError(
            f"{spec.stage} pointer total_size_bytes is inconsistent"
        )
    return {
        "schema_version": POINTER_SCHEMA_VERSION,
        "stage": spec.stage,
        "artifact_name": spec.artifact_name,
        "manifest_sha256": spec.manifest_sha256,
        "repository": repository,
        "repository_type": REPO_TYPE,
        "revision": revision,
        "repository_prefix": expected_prefix,
        "file_count": len(validated_files),
        "total_size_bytes": total_size,
        "files": sorted(
            validated_files,
            key=lambda item: item["relative_path"],
        ),
    }


def validate_pointer_documents(
    documents: Mapping[str, Mapping[str, Any]],
    *,
    specs: Sequence[ArtifactSpec] = DEFAULT_ARTIFACT_SPECS,
    expected_repo_id: str = DEFAULT_REPO_ID,
) -> dict[str, dict[str, Any]]:
    expected_filenames = {spec.pointer_filename for spec in specs}
    if set(documents) != expected_filenames:
        raise CanonicalArtifactError(
            "pointer manifest set does not exactly match the accepted "
            f"artifacts: expected={sorted(expected_filenames)} "
            f"actual={sorted(documents)}"
        )
    validated = {
        spec.pointer_filename: _validate_pointer_document(
            documents[spec.pointer_filename],
            spec,
            expected_repo_id=expected_repo_id,
        )
        for spec in specs
    }
    repositories = {item["repository"] for item in validated.values()}
    repo_types = {item["repository_type"] for item in validated.values()}
    revisions = {item["revision"] for item in validated.values()}
    if (
        len(repositories) != 1
        or repo_types != {REPO_TYPE}
        or len(revisions) != 1
    ):
        raise CanonicalArtifactError(
            "P2-P4 pointers must share one repository, repository type, "
            "and immutable revision"
        )
    all_repository_paths = [
        record["repository_path"]
        for document in validated.values()
        for record in document["files"]
    ]
    if len(all_repository_paths) != len(set(all_repository_paths)):
        raise CanonicalArtifactError(
            "P2-P4 pointers contain duplicate repository paths"
        )
    return validated


def load_pointer_documents(
    pointer_dir: Path = DEFAULT_POINTER_DIR,
    *,
    specs: Sequence[ArtifactSpec] = DEFAULT_ARTIFACT_SPECS,
    expected_repo_id: str = DEFAULT_REPO_ID,
) -> dict[str, dict[str, Any]]:
    if not pointer_dir.is_dir():
        raise CanonicalArtifactError(
            f"canonical pointer directory is missing: {pointer_dir}"
        )
    expected = {spec.pointer_filename for spec in specs}
    actual = {path.name for path in pointer_dir.iterdir() if path.is_file()}
    if actual != expected:
        raise CanonicalArtifactError(
            "canonical pointer directory must contain exactly "
            f"{sorted(expected)}; found {sorted(actual)}"
        )
    documents = {
        filename: _read_json(
            pointer_dir / filename,
            f"canonical pointer {filename}",
        )
        for filename in sorted(expected)
    }
    return validate_pointer_documents(
        documents,
        specs=specs,
        expected_repo_id=expected_repo_id,
    )


def write_pointer_documents(
    pointer_dir: Path,
    documents: Mapping[str, Mapping[str, Any]],
    *,
    specs: Sequence[ArtifactSpec] = DEFAULT_ARTIFACT_SPECS,
    expected_repo_id: str = DEFAULT_REPO_ID,
) -> None:
    """Atomically materialize pointers, refusing any existing target."""
    validated = validate_pointer_documents(
        documents,
        specs=specs,
        expected_repo_id=expected_repo_id,
    )
    if pointer_dir.exists():
        raise CanonicalArtifactError(
            f"pointer target already exists; refusing overwrite: "
            f"{pointer_dir}"
        )
    pointer_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(
            prefix=f".{pointer_dir.name}.",
            dir=pointer_dir.parent,
        )
    )
    try:
        for filename, document in sorted(validated.items()):
            with (temporary / filename).open(
                "x",
                encoding="utf-8",
            ) as handle:
                json.dump(document, handle, indent=2, sort_keys=True)
                handle.write("\n")
        temporary.rename(pointer_dir)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise


def retrieve_pointer_documents(
    documents: Mapping[str, Mapping[str, Any]],
    *,
    target: Path,
    api: Any,
    token: str,
    downloader: Callable[..., str | Path],
    specs: Sequence[ArtifactSpec] = DEFAULT_ARTIFACT_SPECS,
    expected_repo_id: str = DEFAULT_REPO_ID,
) -> dict[str, Any]:
    """Download and independently verify every pointer record."""
    validated = validate_pointer_documents(
        documents,
        specs=specs,
        expected_repo_id=expected_repo_id,
    )
    if target.exists():
        raise CanonicalArtifactError(
            f"retrieval target already exists; refusing overwrite: {target}"
        )
    first = next(iter(validated.values()))
    repository = first["repository"]
    revision = first["revision"]
    verify_private_repository(
        api,
        repo_id=repository,
        token=token,
        revision=revision,
    )

    target.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{target.name}.",
            dir=target.parent,
        )
    )
    verified_files = 0
    verified_bytes = 0
    try:
        for spec in specs:
            document = validated[spec.pointer_filename]
            for record in document["files"]:
                try:
                    cached_path = Path(
                        downloader(
                            repo_id=repository,
                            filename=record["repository_path"],
                            repo_type=REPO_TYPE,
                            revision=revision,
                            token=token,
                        )
                    )
                except Exception as exc:
                    raise CanonicalArtifactError(
                        "immutable canonical artifact download failed for "
                        f"{record['repository_path']}"
                    ) from exc
                destination = (
                    staging
                    / spec.artifact_name
                    / record["relative_path"]
                )
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(cached_path, destination)
                actual_size = destination.stat().st_size
                if actual_size != record["size_bytes"]:
                    raise CanonicalArtifactError(
                        "downloaded canonical artifact size mismatch for "
                        f"{record['repository_path']}: "
                        f"expected={record['size_bytes']} "
                        f"actual={actual_size}"
                    )
                actual_sha = sha256_file(destination)
                if actual_sha != record["sha256"]:
                    raise CanonicalArtifactError(
                        "downloaded canonical artifact hash mismatch for "
                        f"{record['repository_path']}: "
                        f"expected={record['sha256']} actual={actual_sha}"
                    )
                verified_files += 1
                verified_bytes += actual_size
        staging.rename(target)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return {
        "repository": repository,
        "repository_type": REPO_TYPE,
        "revision": revision,
        "target": str(target),
        "artifact_count": len(validated),
        "verified_file_count": verified_files,
        "verified_size_bytes": verified_bytes,
        "visibility": "private_verified",
        "mutable_revision_fallback": False,
    }


def retrieve_from_pointers(
    *,
    pointer_dir: Path,
    target: Path,
    api: Any,
    token: str,
    downloader: Callable[..., str | Path],
    specs: Sequence[ArtifactSpec] = DEFAULT_ARTIFACT_SPECS,
    expected_repo_id: str = DEFAULT_REPO_ID,
) -> dict[str, Any]:
    documents = load_pointer_documents(
        pointer_dir,
        specs=specs,
        expected_repo_id=expected_repo_id,
    )
    return retrieve_pointer_documents(
        documents,
        target=target,
        api=api,
        token=token,
        downloader=downloader,
        specs=specs,
        expected_repo_id=expected_repo_id,
    )


def plan_summary(plan: PublicationPlan) -> dict[str, Any]:
    return {
        "repository": plan.repository,
        "repository_type": plan.repository_type,
        "artifact_count": len(plan.artifacts),
        "file_count": len(plan.files),
        "total_size_bytes": plan.total_size_bytes,
        "artifacts": [
            {
                "stage": artifact.spec.stage,
                "artifact_name": artifact.spec.artifact_name,
                "manifest_sha256": artifact.spec.manifest_sha256,
                "repository_prefix": artifact.repository_prefix,
                "file_count": len(artifact.files),
                "total_size_bytes": artifact.total_size_bytes,
            }
            for artifact in plan.artifacts
        ],
        "legacy_root_paths_touched": False,
        "uploads_performed": False,
    }
