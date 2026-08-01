import json

import pandas as pd
import pytest

from portfolio.event_review_adjudication import (
    DEFAULT_EXTRACTION_ROOT,
    EXPECTED_EXTRACTION_MANIFEST_SHA256,
    build_event_review_adjudication,
    independently_verify_extraction,
    verify_event_review_adjudication_artifact,
)


def test_independent_extraction_verification_covers_complete_boundary():
    result = independently_verify_extraction(DEFAULT_EXTRACTION_ROOT)
    assert result["manifest_sha256"] == EXPECTED_EXTRACTION_MANIFEST_SHA256
    assert result["manifest_records_verified"] == 62
    assert result["stored_responses_verified"] == 47
    assert result["aggregate_response_bytes"] == 19_120_821
    assert result["claim_locators_and_hashes_verified"] == 46
    assert result["collection_records_verified"] == 59
    assert result["parent_e1_records_verified"] == 148
    assert result["preservation_entries_verified"] == 29


def test_full_offline_adjudication_is_human_signoff_ready(tmp_path):
    adjudication_id = "20260730T230000Z-e1-adjudication-test"
    target = tmp_path / adjudication_id
    manifest_path = build_event_review_adjudication(
        target,
        adjudication_id=adjudication_id,
        extraction_root=DEFAULT_EXTRACTION_ROOT,
    )
    manifest = json.loads(manifest_path.read_text())
    names = pd.read_parquet(
        target / "outputs/live/name_level_adjudication.parquet"
    )
    documents = pd.read_parquet(
        target / "outputs/live/document_level_adjudication.parquet"
    )
    reconciliation = pd.read_parquet(
        target / "outputs/live/deterministic_reconciliation.parquet"
    )
    contract = pd.read_parquet(
        target / "outputs/live/review_contract.parquet"
    )
    queue = pd.read_parquet(
        target / "outputs/live/human_signoff_queue.parquet"
    )

    assert len(names) == len(contract) == len(queue) == 15
    assert len(documents) == 47
    assert documents["claim_id"].notna().sum() == 46
    assert documents["deterministic_action"].eq("unresolved").all()
    assert not documents["frozen_rule_requirements_fully_satisfied"].any()
    assert not documents["unsupported_inferences_applied"].any()
    assert names["deterministic_action"].eq("unresolved").all()
    assert names["human_review_required"].all()
    assert not names["transaction_or_action_terms_complete"].any()
    assert names["summary_allowed"].sum() == 13
    assert not names.set_index("ticker").loc["HPK", "summary_allowed"]
    assert not names.set_index("ticker").loc["SSTK", "summary_allowed"]
    assert pd.isna(
        names.set_index("ticker").loc["HPK", "adjudication_summary"]
    )
    assert pd.isna(
        names.set_index("ticker").loc["SSTK", "adjudication_summary"]
    )
    assert (
        documents[
            documents["accession_number"].eq("0001104659-26-008380")
        ]["affected_security_role"]
        .iloc[0]
        .endswith("consideration security")
    )
    assert reconciliation["resolved_count"].eq(0).all()
    assert reconciliation["unresolved_count"].tolist() == [47, 15]
    assert queue["queue_status"].eq(
        "awaiting_explicit_human_signoff"
    ).all()
    assert manifest["claim"]["explicit_human_signoff_complete"] is False
    assert manifest["claim"]["external_request_made"] is False

    verified = verify_event_review_adjudication_artifact(target)
    assert verified["claim_citations_verified"] == 46
    assert verified["unresolved_name_rows"] == 15
    assert verified["explicit_human_signoff_complete"] is False


def test_offline_adjudication_refuses_nonempty_target(tmp_path):
    target = tmp_path / "not-empty"
    target.mkdir()
    (target / "preserve").write_text("x")
    with pytest.raises(RuntimeError, match="not empty"):
        build_event_review_adjudication(
            target,
            adjudication_id="20260730T230001Z-e1-adjudication-test",
            extraction_root=DEFAULT_EXTRACTION_ROOT,
        )
