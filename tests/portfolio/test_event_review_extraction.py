import gzip
import hashlib
import json

import pandas as pd
import pytest

from portfolio.event_review_extraction import (
    DEFAULT_COLLECTION_ROOT,
    _extract_bounded_passage,
    _normalized_document_text,
    build_retrieved_event_review,
    verify_retrieved_event_review_artifact,
)


def test_normalized_locator_is_exact_and_hash_reproducible():
    payload = (
        b"<html><body><p>Before.</p><p>On January 1, 2026, "
        b"the issuer entered an agreement with exact terms. "
        b"The agreement remained subject to review.</p></body></html>"
    )
    text = _normalized_document_text(payload)
    passage, start, end = _extract_bounded_passage(
        text,
        "On January 1, 2026",
    )
    assert text[start:end] == passage
    assert passage.startswith("On January 1, 2026")
    assert len(hashlib.sha256(passage.encode()).hexdigest()) == 64


def test_full_offline_extraction_preserves_fail_closed_contract(tmp_path):
    review_id = "20260730T220000Z-e1-extraction-test"
    target = tmp_path / review_id
    manifest_path = build_retrieved_event_review(
        target,
        review_id=review_id,
        collection_root=DEFAULT_COLLECTION_ROOT,
    )
    manifest = json.loads(manifest_path.read_text())
    documents = pd.read_parquet(
        target / "outputs/live/document_inventory.parquet"
    )
    claims = pd.read_parquet(
        target / "outputs/live/document_extracted_claims.parquet"
    )
    reconciliation = pd.read_parquet(
        target / "outputs/live/document_reconciliation.parquet"
    )
    live = pd.read_parquet(
        target / "outputs/live/review_contract.parquet"
    )
    queue = pd.read_parquet(
        target / "outputs/live/human_review_queue.parquet"
    )
    assert len(documents) == 47
    assert documents["http_status"].eq(200).sum() == 46
    assert len(claims) == 46
    assert claims["claim_text"].eq(claims["supporting_passage"]).all()
    assert len(reconciliation) == 47
    assert reconciliation["deterministic_action"].eq("unresolved").all()
    assert not reconciliation["unsupported_inferences_applied"].any()
    assert len(live) == len(queue) == 15
    assert live["human_review_required"].all()
    assert live["summary_allowed"].sum() == 13
    assert not live.set_index("ticker").loc["HPK", "summary_allowed"]
    assert not live.set_index("ticker").loc["SSTK", "summary_allowed"]
    assert (
        manifest["claim"]["hpk_http_503_unresolved_summary_prohibited"]
        is True
    )
    verified = verify_retrieved_event_review_artifact(target)
    assert verified["claims_verified"] == 46
    assert verified["external_request_made"] is False


def test_offline_extraction_refuses_nonempty_target(tmp_path):
    target = tmp_path / "not-empty"
    target.mkdir()
    (target / "preserve").write_text("x")
    with pytest.raises(RuntimeError, match="not empty"):
        build_retrieved_event_review(
            target,
            review_id="20260730T220001Z-e1-extraction-test",
            collection_root=DEFAULT_COLLECTION_ROOT,
        )
