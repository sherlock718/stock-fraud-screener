import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from _root import ROOT
from data_io.canonical_hf import (
    ArtifactSpec,
    CanonicalArtifactError,
    build_publication_plan,
    create_pointer_documents,
    load_pointer_documents,
    publication_preflight,
    publish_plan,
    require_hf_token,
    retrieve_pointer_documents,
    validate_pointer_documents,
    verify_private_repository,
    write_pointer_documents,
)


REVISION = "a" * 40
NEXT_REVISION = "b" * 40
PUBLISHED_C2_REVISION = "aaf056ea115067e42ef9abf9fa93ade75cdd4052"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _fixture_plan(tmp_path: Path, stages=("P2", "P3", "P4")):
    specs = []
    for index, stage in enumerate(stages, start=2):
        name = f"artifact_{stage.lower()}"
        root = tmp_path / "artifacts/canonical" / name
        root.mkdir(parents=True)
        payload = f"payload-{stage}".encode()
        payload_path = root / "outputs/value.bin"
        payload_path.parent.mkdir()
        payload_path.write_bytes(payload)
        source_path = payload_path.relative_to(tmp_path).as_posix()
        manifest = {
            "records": [
                {
                    "path": source_path,
                    "size_bytes": len(payload),
                    "sha256": _sha256_bytes(payload),
                }
            ]
        }
        manifest_path = root / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, sort_keys=True))
        specs.append(
            ArtifactSpec(
                stage=stage,
                artifact_name=name,
                artifact_root=root,
                manifest_sha256=_sha256_bytes(
                    manifest_path.read_bytes()
                ),
                pointer_filename=f"p{index}.json",
            )
        )
    plan = build_publication_plan(
        repo_id="ekrash718/stock-screener-data",
        specs=tuple(specs),
        workspace_root=tmp_path,
    )
    return plan, tuple(specs)


def test_token_loader_prefers_environment(monkeypatch):
    monkeypatch.setenv("HF_TOKEN", "environment-token")
    monkeypatch.setattr(
        "huggingface_hub.get_token",
        lambda: pytest.fail("saved token should not be read"),
    )

    assert require_hf_token() == "environment-token"


def test_token_loader_uses_standard_saved_token(monkeypatch):
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setattr(
        "huggingface_hub.get_token",
        lambda: "saved-token",
    )

    assert require_hf_token() == "saved-token"


def test_token_loader_fails_closed_without_authentication(monkeypatch):
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setattr("huggingface_hub.get_token", lambda: None)

    with pytest.raises(CanonicalArtifactError, match="saved Hugging Face"):
        require_hf_token()


def test_tracked_c2_pointers_pin_the_exact_published_baseline():
    documents = load_pointer_documents(
        ROOT / "data_io/canonical_artifact_pointers"
    )

    assert {item["revision"] for item in documents.values()} == {
        PUBLISHED_C2_REVISION
    }
    assert sum(item["file_count"] for item in documents.values()) == 202
    assert (
        sum(item["total_size_bytes"] for item in documents.values())
        == 481_666_707
    )


def test_publication_wrapper_works_outside_repository(tmp_path):
    result = subprocess.run(
        [ROOT / "scripts/publish_canonical", "--help"],
        cwd=tmp_path,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0
    assert "--publish" in result.stdout


class FakeApi:
    def __init__(
        self,
        *,
        private=True,
        revision=REVISION,
        existing=(),
        repo_error=None,
    ):
        self.private = private
        self.revision = revision
        self.existing = list(existing)
        self.repo_error = repo_error
        self.repo_calls = []
        self.list_calls = []
        self.commit_calls = []

    def repo_info(self, **kwargs):
        self.repo_calls.append(kwargs)
        if self.repo_error is not None:
            raise self.repo_error
        requested = kwargs.get("revision")
        return SimpleNamespace(
            private=self.private,
            sha=requested or self.revision,
        )

    def list_repo_files(self, **kwargs):
        self.list_calls.append(kwargs)
        return self.existing

    def create_commit(self, **kwargs):
        self.commit_calls.append(kwargs)
        return SimpleNamespace(oid=NEXT_REVISION)


def test_plan_is_complete_content_addressed_and_deterministic(tmp_path):
    plan, _ = _fixture_plan(tmp_path)

    assert len(plan.artifacts) == 3
    assert len(plan.files) == 6
    assert all(
        item.repository_path.startswith(
            f"canonical/{item.artifact_name}/"
        )
        for item in plan.files
    )
    assert {
        item.relative_path for item in plan.files
    } == {"manifest.json", "outputs/value.bin"}


def test_plan_rejects_unmanifested_file_without_modifying_it(tmp_path):
    plan, specs = _fixture_plan(tmp_path, stages=("P2",))
    extra = specs[0].artifact_root / "preserve.txt"
    extra.write_text("preserve")

    with pytest.raises(
        CanonicalArtifactError,
        match="manifest/root file-set mismatch",
    ):
        build_publication_plan(
            repo_id=plan.repository,
            specs=specs,
            workspace_root=tmp_path,
        )
    assert extra.read_text() == "preserve"


def test_plan_rejects_record_hash_mismatch(tmp_path):
    _, specs = _fixture_plan(tmp_path, stages=("P2",))
    (specs[0].artifact_root / "outputs/value.bin").write_bytes(b"changed")

    with pytest.raises(CanonicalArtifactError, match="size mismatch"):
        build_publication_plan(
            repo_id="ekrash718/stock-screener-data",
            specs=specs,
            workspace_root=tmp_path,
        )


@pytest.mark.parametrize("private", [False, None])
def test_visibility_fails_closed_unless_private_is_exactly_true(private):
    api = FakeApi(private=private)

    with pytest.raises(
        CanonicalArtifactError,
        match="public or its private visibility could not be proven",
    ):
        verify_private_repository(
            api,
            repo_id="ekrash718/stock-screener-data",
            token="secret",
        )


def test_visibility_fails_closed_when_metadata_request_errors():
    api = FakeApi(repo_error=RuntimeError("unavailable"))

    with pytest.raises(
        CanonicalArtifactError,
        match="visibility could not be verified",
    ):
        verify_private_repository(
            api,
            repo_id="ekrash718/stock-screener-data",
            token="secret",
        )


def test_preflight_pins_listing_and_refuses_any_existing_destination(
    tmp_path,
):
    plan, _ = _fixture_plan(tmp_path)
    collision = plan.files[0].repository_path
    api = FakeApi(existing=(collision,))

    with pytest.raises(
        CanonicalArtifactError,
        match="refusing to overwrite",
    ):
        publication_preflight(plan, api=api, token="secret")
    assert api.list_calls[0]["revision"] == REVISION
    assert api.commit_calls == []


def test_publish_uses_one_parent_pinned_commit_and_immutable_pointers(
    tmp_path,
):
    plan, specs = _fixture_plan(tmp_path)
    api = FakeApi()
    operations = []

    def operation_factory(**kwargs):
        operations.append(kwargs)
        return kwargs

    revision, pointers, preflight = publish_plan(
        plan,
        api=api,
        token="secret",
        commit_message="test",
        operation_factory=operation_factory,
    )

    assert revision == NEXT_REVISION
    assert preflight["parent_revision"] == REVISION
    assert len(api.commit_calls) == 1
    assert api.commit_calls[0]["parent_commit"] == REVISION
    assert api.commit_calls[0]["revision"] == "main"
    assert len(operations) == len(plan.files)
    validated = validate_pointer_documents(pointers, specs=specs)
    assert {item["revision"] for item in validated.values()} == {
        NEXT_REVISION
    }


def test_pointer_validation_rejects_mutable_revision(tmp_path):
    plan, specs = _fixture_plan(tmp_path)
    pointers = create_pointer_documents(plan, revision=REVISION)
    pointers[specs[0].pointer_filename]["revision"] = "latest"

    with pytest.raises(
        CanonicalArtifactError,
        match="full immutable 40-character",
    ):
        validate_pointer_documents(pointers, specs=specs)


def test_pointer_validation_rejects_a_different_repository(tmp_path):
    plan, specs = _fixture_plan(tmp_path)
    pointers = create_pointer_documents(plan, revision=REVISION)
    pointers[specs[0].pointer_filename]["repository"] = "other/data"

    with pytest.raises(
        CanonicalArtifactError,
        match="accepted destination",
    ):
        validate_pointer_documents(pointers, specs=specs)


def test_retrieval_pins_every_download_and_verifies_byte_for_byte(
    tmp_path,
):
    plan, specs = _fixture_plan(tmp_path)
    pointers = create_pointer_documents(plan, revision=REVISION)
    source_by_repo_path = {
        item.repository_path: item.local_path for item in plan.files
    }
    download_calls = []

    def downloader(**kwargs):
        download_calls.append(kwargs)
        return source_by_repo_path[kwargs["filename"]]

    target = tmp_path / "retrieved"
    api = FakeApi()
    result = retrieve_pointer_documents(
        pointers,
        target=target,
        api=api,
        token="secret",
        downloader=downloader,
        specs=specs,
    )

    assert result["verified_file_count"] == len(plan.files)
    assert result["mutable_revision_fallback"] is False
    assert all(call["revision"] == REVISION for call in download_calls)
    for artifact in plan.artifacts:
        for record in artifact.files:
            restored = (
                target
                / artifact.spec.artifact_name
                / record.relative_path
            )
            assert restored.stat().st_size == record.size_bytes
            assert _sha256_bytes(restored.read_bytes()) == record.sha256


def test_retrieval_hash_failure_leaves_no_partial_target(tmp_path):
    plan, specs = _fixture_plan(tmp_path)
    pointers = create_pointer_documents(plan, revision=REVISION)
    corrupt = tmp_path / "corrupt"
    corrupt.write_bytes(b"corrupt")

    target = tmp_path / "retrieved"
    with pytest.raises(CanonicalArtifactError, match="size mismatch"):
        retrieve_pointer_documents(
            pointers,
            target=target,
            api=FakeApi(),
            token="secret",
            downloader=lambda **_: corrupt,
            specs=specs,
        )
    assert not target.exists()


def test_pointer_write_is_non_overwriting(tmp_path):
    plan, specs = _fixture_plan(tmp_path)
    pointers = create_pointer_documents(plan, revision=REVISION)
    pointer_dir = tmp_path / "pointers"
    write_pointer_documents(pointer_dir, pointers, specs=specs)

    with pytest.raises(CanonicalArtifactError, match="refusing overwrite"):
        write_pointer_documents(pointer_dir, pointers, specs=specs)
