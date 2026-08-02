from __future__ import annotations

from io import BytesIO
import hashlib
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from data_io.us_free_v1_durability import (
    DurabilityError,
    E1_CALENDAR_ALIAS_PATH,
    E1_CALENDAR_CANONICAL_SOURCE,
    SourceGroup,
    _attach_existing_remote_records,
    _extract_verified_tar,
    _token_has_write_scope,
    inventory_groups,
    sha256_file,
    verify_tar,
    write_deterministic_tar,
)


def _record(root: Path, relative: str) -> dict[str, object]:
    path = root / relative
    return {
        "path": relative,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def test_inventory_records_every_path_reason_destination_and_manifest(tmp_path: Path):
    root = tmp_path / "artifacts/example"
    root.mkdir(parents=True)
    (root / "manifest.json").write_text('{"records": []}\n')
    (root / "payload.bin").write_bytes(b"frozen")
    groups, sources = inventory_groups(
        tmp_path,
        (
            SourceGroup(
                "example",
                ("artifacts/example",),
                "required fixture",
                ("tests",),
            ),
        ),
    )

    assert groups[0]["file_count"] == 2
    assert {item["path"] for item in sources} == {
        "artifacts/example/manifest.json",
        "artifacts/example/payload.bin",
    }
    assert all(item["reason"] == "required fixture" for item in sources)
    assert all(item["recovery_destination"] == item["path"] for item in sources)
    assert all(item["manifest_relationship_ids"] for item in sources)


def test_inventory_rejects_symlink(tmp_path: Path):
    root = tmp_path / "artifacts/example"
    root.mkdir(parents=True)
    source = root / "payload.bin"
    source.write_bytes(b"frozen")
    (root / "link.bin").symlink_to(source)
    with pytest.raises(DurabilityError, match="symlinks"):
        inventory_groups(
            tmp_path,
            (SourceGroup("example", ("artifacts/example",), "reason", ("tests",)),),
        )


def test_deterministic_tar_rehashes_every_member_and_rejects_drift(tmp_path: Path):
    (tmp_path / "one.bin").write_bytes(b"one")
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested/two.bin").write_bytes(b"two")
    records = [_record(tmp_path, "one.bin"), _record(tmp_path, "nested/two.bin")]
    first = tmp_path / "first.tar"
    second = tmp_path / "second.tar"
    write_deterministic_tar(tmp_path, records, first)
    write_deterministic_tar(tmp_path, records, second)

    assert first.read_bytes() == second.read_bytes()
    assert verify_tar(first, records) == {
        "file_count": 2,
        "source_size_bytes": 6,
        "status": "pass",
    }
    records[0]["sha256"] = "0" * 64
    with pytest.raises(DurabilityError, match="hash drifted"):
        verify_tar(first, records)


def test_recovery_rejects_tar_path_traversal(tmp_path: Path):
    unsafe = tmp_path / "unsafe.tar"
    with tarfile.open(unsafe, "w") as archive:
        info = tarfile.TarInfo("../escape.bin")
        payload = b"escape"
        info.size = len(payload)
        archive.addfile(info, BytesIO(payload))
    with pytest.raises(DurabilityError, match="unsafe recovery tar member"):
        _extract_verified_tar(unsafe, tmp_path / "target", {})
    assert not (tmp_path / "escape.bin").exists()


def test_e1_calendar_destination_is_an_explicit_immutable_alias():
    digest = hashlib.sha256(b"calendar").hexdigest()
    sources = [
        {
            "artifact_group": "e1_preservation_calendar_alias",
            "path": E1_CALENDAR_ALIAS_PATH,
            "size_bytes": 8,
            "sha256": digest,
        }
    ]
    remote = {
        E1_CALENDAR_CANONICAL_SOURCE: {
            "repository_path": "canonical/calendar.json",
            "revision": "a" * 40,
            "size_bytes": 8,
            "sha256": digest,
        }
    }

    _attach_existing_remote_records(sources, remote)

    assert sources[0]["immutable_alias_source"] == E1_CALENDAR_CANONICAL_SOURCE
    assert sources[0]["existing_remote"] == remote[E1_CALENDAR_CANONICAL_SOURCE]


def test_e1_calendar_alias_fails_closed_on_remote_drift():
    sources = [
        {
            "artifact_group": "e1_preservation_calendar_alias",
            "path": E1_CALENDAR_ALIAS_PATH,
            "size_bytes": 8,
            "sha256": hashlib.sha256(b"calendar").hexdigest(),
        }
    ]
    remote = {
        E1_CALENDAR_CANONICAL_SOURCE: {
            "repository_path": "canonical/calendar.json",
            "revision": "a" * 40,
            "size_bytes": 8,
            "sha256": "0" * 64,
        }
    }

    with pytest.raises(DurabilityError, match="immutable pointer/source drift"):
        _attach_existing_remote_records(sources, remote)


@pytest.mark.parametrize(
    ("identity", "expected"),
    [
        ({"auth": {"accessToken": {"role": "write"}}}, True),
        ({"auth": {"accessToken": {"role": "read"}}}, False),
        ({"auth": {}}, False),
    ],
)
def test_token_write_scope_must_be_explicit(identity, expected):
    assert _token_has_write_scope(identity) is expected
