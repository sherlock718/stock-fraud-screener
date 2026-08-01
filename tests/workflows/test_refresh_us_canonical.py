import json

import pytest

from workflows.refresh_us_canonical import (
    PINNED_HF_REVISION,
    main,
    plan,
    preservation_snapshot,
)


def test_plan_is_offline_non_overwriting_and_non_promoting(tmp_path):
    result = plan("20260730T143000Z", tmp_path / "refresh")
    assert result["external_request_would_be_made"] is False
    assert result["non_overwriting"] is True
    assert result["automatic_promotion"] is False
    assert result["publication"] is False
    assert result["international_collectors_modified"] is False
    assert result["pinned_hugging_face_revision"] == PINNED_HF_REVISION
    assert [stage["stage"] for stage in result["stages"]] == [
        "universe",
        "filings",
        "market",
        "P2 candidate",
        "baseline comparison",
    ]


def test_preservation_snapshot_pins_p2_p4_and_international_structure():
    snapshot = preservation_snapshot()
    assert len(snapshot["canonical_manifests"]) == 3
    assert set(snapshot["pointer_revisions"].values()) == {
        PINNED_HF_REVISION
    }
    paths = snapshot["tracked_legacy_and_international"]
    assert "workflows/run_pipeline.py" in paths
    assert "workflows/run_pipeline_ca.py" in paths
    assert "pipeline/step1_fetch_tickers_jp.py" in paths


def test_collection_confirmation_guard_is_documented_by_cli_plan(tmp_path):
    result = plan("20260730T143000Z", tmp_path / "refresh")
    rendered = json.dumps(result)
    assert "SEC company_tickers" in rendered
    assert "Yahoo chart" in rendered
    with pytest.raises(ValueError):
        plan("latest", tmp_path / "refresh")


def test_collect_requires_exact_confirmation_before_route_runs(
    monkeypatch,
):
    monkeypatch.setattr(
        "sys.argv",
        [
            "refresh_us_canonical",
            "--refresh-id",
            "20260730T143000Z",
            "--collect",
        ],
    )
    with pytest.raises(RuntimeError, match="exactly match"):
        main()
