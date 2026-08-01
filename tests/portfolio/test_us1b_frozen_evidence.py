from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio.us1b_frozen_evidence import (
    APPROVAL_TOKEN,
    DEFAULT_ARTIFACT_ROOT,
    REQUEST_PLAN_SHA256,
    US1A_ROOT,
    build_frozen_request_plan,
    collect_approved_evidence,
    finalize_artifact,
    freeze_contract,
    verify_artifact,
)


def test_frozen_plan_is_exact_and_approval_bound() -> None:
    plan, rows = build_frozen_request_plan()

    canonical = json.dumps(
        plan, sort_keys=True, separators=(",", ":")
    ).encode()
    assert hashlib.sha256(canonical).hexdigest() == REQUEST_PLAN_SHA256
    assert len(rows) == rows["url"].nunique() == 36
    assert rows["order"].tolist() == list(range(1, 37))
    assert plan["reused"] == {
        "d1_submission_count": 15,
        "d1_exchange_index_count": 1,
        "e1_primary_response_count": 13,
        "e1_http_status_counts": {"200": 12, "503": 1},
    }


def test_materialized_artifact_verifies_every_lineage_boundary() -> None:
    result = verify_artifact(DEFAULT_ARTIFACT_ROOT)

    assert result["external_unique_urls"] == 36
    assert result["external_attempts"] == 36
    assert result["external_http_200"] == 36
    assert result["raw_collected_responses_verified"] == 36
    assert result["raw_reused_responses_verified"] == 29
    assert result["document_rows"] == 47
    assert result["claim_rows"] == 46
    assert result["market_rows"] == 15
    assert result["unresolved_names"] == 14
    assert result["failed_request_names"] == 1
    assert result["shortlist_unchanged"] is True
    assert result["performance_calculated"] is False
    assert result["preserved_boundaries_reverified"] is True


def test_shortlist_identity_rank_holding_and_weight_are_immutable() -> None:
    parent = pd.read_parquet(
        US1A_ROOT / "outputs/final_shortlist_2026.parquet"
    ).sort_values("rank").reset_index(drop=True)
    derivative = pd.read_parquet(
        DEFAULT_ARTIFACT_ROOT
        / "outputs/final_shortlist_2026_evidence_derivative.parquet"
    ).sort_values("rank").reset_index(drop=True)

    pd.testing.assert_frame_equal(
        derivative[list(parent.columns)], parent, check_exact=True
    )
    assert derivative["coverage_state"].value_counts().to_dict() == {
        "unresolved": 14,
        "failed_request": 1,
    }
    assert derivative["deterministic_action"].eq("unresolved").all()


def test_claims_are_cited_and_document_adjudication_is_separate() -> None:
    claims = pd.read_parquet(
        DEFAULT_ARTIFACT_ROOT / "outputs/document_claims.parquet"
    )
    documents = pd.read_parquet(
        DEFAULT_ARTIFACT_ROOT / "outputs/document_level_adjudication.parquet"
    )
    names = pd.read_parquet(
        DEFAULT_ARTIFACT_ROOT / "outputs/name_level_adjudication.parquet"
    )

    assert claims["claim_id"].is_unique
    assert claims["source_response_sha256"].str.fullmatch(
        r"[0-9a-f]{64}"
    ).all()
    assert claims["supporting_passage_sha256"].str.fullmatch(
        r"[0-9a-f]{64}"
    ).all()
    assert documents["document_state"].value_counts().to_dict() == {
        "unsupported": 33,
        "unresolved": 13,
        "failed_request": 1,
    }
    assert names["human_review_required"].all()
    assert not documents["summary_may_change_deterministic_action"].any()
    assert not documents["unsupported_inferences_applied"].any()


def test_freeze_collection_and_finalization_are_non_overwriting(
    tmp_path: Path,
) -> None:
    target = tmp_path / "20260801T193322Z-us1b-failure-test"
    freeze_contract(target, reverify_boundaries=False)
    with pytest.raises(FileExistsError):
        freeze_contract(target, reverify_boundaries=False)

    payload = b"explicit forbidden response preserved"

    def forbidden_fetcher(
        url: str, headers: dict[str, str], limit: int
    ) -> tuple[int, dict[str, str], bytes, bool]:
        del url, headers, limit
        return 403, {"Content-Type": "text/plain"}, payload, False

    inventory_path = collect_approved_evidence(
        target,
        approval_token=APPROVAL_TOKEN,
        fetcher=forbidden_fetcher,
        pace=False,
    )
    inventory = json.loads(inventory_path.read_text())
    assert inventory["attempt_count"] == 1
    assert inventory["logical_requests"][0]["request_status"] == (
        "failed_request_stop_status"
    )
    assert all(
        row["request_status"] == "not_attempted_after_stop_condition"
        for row in inventory["logical_requests"][1:]
    )
    stored = target / inventory["attempts"][0]["stored_path"]
    with gzip.open(stored, "rb") as handle:
        assert handle.read() == payload
    with pytest.raises(FileExistsError):
        collect_approved_evidence(
            target,
            approval_token=APPROVAL_TOKEN,
            fetcher=forbidden_fetcher,
            pace=False,
        )
    with pytest.raises(Exception, match="reconciliation"):
        finalize_artifact(target)


def test_collection_requires_exact_approval_token(tmp_path: Path) -> None:
    target = tmp_path / "20260801T193322Z-us1b-token-test"
    freeze_contract(target, reverify_boundaries=False)
    with pytest.raises(PermissionError, match="exact US1B approval token"):
        collect_approved_evidence(
            target,
            approval_token="APPROVE-US1B-wrong",
            pace=False,
        )
    assert not (target / "requests/request_inventory.json").exists()
