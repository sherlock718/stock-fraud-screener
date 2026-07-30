from pathlib import Path

import pytest

import workflows.run_canonical as canonical


def test_accepted_route_verifies_in_dependency_order():
    results = canonical.run()
    assert [item["stage"] for item in results] == ["P2", "P3", "P4"]
    assert all(item["status"] == "verified" for item in results)


def test_missing_stage_fails_closed_without_build_permission(
    tmp_path, monkeypatch
):
    stage = canonical.Stage("P2", tmp_path / "missing", "0" * 64, lambda _: {})
    monkeypatch.setattr(canonical, "canonical_stages", lambda: (stage,))
    with pytest.raises(RuntimeError, match="--build-missing"):
        canonical.run()


def test_build_missing_never_reuses_nonempty_target(
    tmp_path, monkeypatch
):
    root = tmp_path / "occupied"
    root.mkdir()
    (root / "unrelated.txt").write_text("preserve")
    stage = canonical.Stage("P2", root, "0" * 64, lambda _: {})
    monkeypatch.setattr(canonical, "canonical_stages", lambda: (stage,))
    with pytest.raises(RuntimeError, match="non-empty"):
        canonical.run(build_missing=True)
    assert (root / "unrelated.txt").read_text() == "preserve"
