import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio.event_review import (
    DEFAULT_D1_ROOT,
    DEFAULT_P4_ROOT,
    DEFAULT_S1_ROOT,
    FROZEN_E1_MANIFEST_SHA256,
    FROZEN_E1_PLAN_SHA256,
    ROOT,
    _is_live_document_candidate,
    build_event_review,
    collect_live_event_evidence,
    deterministic_event_decision,
    validate_summary_claims,
    verify_event_review_artifact,
    verify_live_event_collection_artifact,
)


def _complete_event(**overrides):
    event = {
        "event_type": "merger",
        "event_status": "announced_pending",
        "effective_at": "2026-06-01T00:00:00Z",
        "effective_time_status": "document_exact",
        "source_published_at": "2026-05-01T00:00:00Z",
        "source_retrieved_at": "2026-05-02T00:00:00Z",
        "source_id": "document:1",
        "source_response_sha256": "a" * 64,
        "ambiguity": "",
    }
    event.update(overrides)
    return event


def test_deterministic_policy_excludes_only_complete_effective_evidence():
    pending = deterministic_event_decision(
        _complete_event(),
        as_of_timestamp="2026-07-01T00:00:00Z",
        evaluation_retrieved_at="2026-07-02T00:00:00Z",
    )
    assert pending["canonical_event_type"] == "pending_acquisition"
    assert pending["action"] == "exclude"
    completed = deterministic_event_decision(
        _complete_event(
            event_status="completed",
            effective_at="2026-06-15T00:00:00Z",
        ),
        as_of_timestamp="2026-07-01T00:00:00Z",
        evaluation_retrieved_at="2026-07-02T00:00:00Z",
    )
    assert completed["canonical_event_type"] == "completed_merger"
    assert completed["action"] == "exclude"


def test_future_or_ambiguous_evidence_never_becomes_historical_exclusion():
    future_publication = deterministic_event_decision(
        _complete_event(source_published_at="2026-08-01T00:00:00Z"),
        as_of_timestamp="2026-07-01T00:00:00Z",
        evaluation_retrieved_at="2026-08-02T00:00:00Z",
    )
    assert future_publication["action"] == "no_action"
    unresolved = deterministic_event_decision(
        _complete_event(
            event_status="filing_indicator_unresolved",
            effective_at=None,
            effective_time_status="not_stated_in_submission_index",
            ambiguity=(
                "effective_time_and_security_scope_require_primary_document"
            ),
        ),
        as_of_timestamp="2026-07-01T00:00:00Z",
        evaluation_retrieved_at="2026-08-02T00:00:00Z",
    )
    assert unresolved["action"] == "unresolved"
    assert unresolved["human_review_required"] is True


@pytest.mark.parametrize(
    ("form", "items", "expected"),
    [
        ("25-NSE", "", True),
        ("S-4", "", True),
        ("8-K", "2.01,9.01", True),
        ("8-K/A", "1.03,9.01", True),
        ("8-K", "2.02,9.01", False),
        ("10-K", "1.03", False),
    ],
)
def test_live_document_candidates_are_deterministic_indicators_only(
    form, items, expected
):
    assert _is_live_document_candidate(form, items) is expected


def test_summary_claims_are_live_cited_only():
    evidence = pd.DataFrame(
        {
            "source_id": ["document:1"],
            "response_sha256": ["a" * 64],
            "e1_evidence_path": ["raw/document-1.html.gz"],
        }
    )
    claims = pd.DataFrame(
        {
            "claim_id": ["claim:1"],
            "claim_text": ["The cited document states a dated term."],
            "source_id": ["document:1"],
            "source_response_sha256": ["a" * 64],
            "evidence_path": ["raw/document-1.html.gz"],
            "evidence_locator": ["Item 2.01"],
        }
    )
    validate_summary_claims(claims, evidence, review_mode="live")
    with pytest.raises(ValueError, match="historical"):
        validate_summary_claims(claims, evidence, review_mode="historical")
    bad = claims.copy()
    bad["source_id"] = "model_knowledge"
    with pytest.raises(ValueError, match="unknown"):
        validate_summary_claims(bad, evidence, review_mode="live")


def _collection_fixture(root: Path) -> tuple[Path, str]:
    collection_id = "20260730T200000Z-e1-test"
    target = root / "review"
    (target / "live").mkdir(parents=True)
    approval = f"APPROVE-{collection_id}-LIVE-SEC-PRIMARY-DOCUMENTS"
    contract = {
        "collection_id": collection_id,
        "maximum_external_request_count": 1,
        "approval_token_after_explicit_user_authorization": approval,
    }
    (target / "live/collection_contract.json").write_text(
        json.dumps(contract)
    )
    destination = (
        f"live/collection/{collection_id}/raw/sec/filings/"
        "0000000001/000000000124000001/test.htm.gz"
    )
    pd.DataFrame(
        {
            "request_id": ["request:1"],
            "request_url": [
                "https://www.sec.gov/Archives/edgar/data/1/"
                "000000000124000001/test.htm"
            ],
            "destination_path": [destination],
            "source_publication_time": ["2024-01-01T00:00:00Z"],
            "form": ["8-K"],
            "accession_number": ["0000000001-24-000001"],
        }
    ).to_parquet(target / "live/collection_request_plan.parquet", index=False)
    return target, approval


def test_live_collector_makes_no_request_without_exact_approval(tmp_path):
    root, _ = _collection_fixture(tmp_path)
    calls = []

    def fetcher(url, headers):
        calls.append((url, headers))
        return 200, {}, b"evidence"

    with pytest.raises(PermissionError, match="approval"):
        collect_live_event_evidence(
            root, confirmation=None, fetcher=fetcher
        )
    assert calls == []


def test_approved_fixture_collection_preserves_exact_bytes_and_metadata(
    tmp_path,
):
    root, approval = _collection_fixture(tmp_path)
    payload = b"<html><body>dated source evidence</body></html>"

    def fetcher(url, headers):
        assert url.startswith("https://www.sec.gov/Archives/")
        assert headers["Accept-Encoding"] == "identity"
        return 200, {"Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"}, payload

    manifest_path = collect_live_event_evidence(
        root, confirmation=approval, fetcher=fetcher
    )
    document = json.loads(manifest_path.read_text())
    record = document["records"][0]
    assert record["response_sha256"] == hashlib.sha256(payload).hexdigest()
    stored = root / record["stored_path"]
    with gzip.open(stored, "rb") as handle:
        assert handle.read() == payload
    with pytest.raises(RuntimeError, match="not empty"):
        collect_live_event_evidence(
            root, confirmation=approval, fetcher=fetcher
        )


def test_versioned_collection_validates_frozen_parent_and_reconciles(
    tmp_path, monkeypatch
):
    source = (
        ROOT
        / "artifacts/event_review/us/20260730T144043Z-e1-final"
    )
    contract = json.loads(
        (source / "live/collection_contract.json").read_text()
    )
    collection_id = "20260730T210000Z-e1-collection-test"
    target = tmp_path / collection_id
    calls = []

    def fetcher(url, headers):
        calls.append(url)
        assert (
            headers["User-Agent"]
            == "CanonicalUSRefresh research@alpharesearch.io"
        )
        content_type = (
            "application/xml" if url.lower().endswith(".xml") else "text/html"
        )
        return 200, {"Content-Type": content_type}, b"<html>evidence</html>"

    monkeypatch.setattr(
        "portfolio.event_review.SEC_MIN_REQUEST_INTERVAL_SECONDS",
        0.0,
    )
    manifest_path = collect_live_event_evidence(
        source,
        confirmation=contract[
            "approval_token_after_explicit_user_authorization"
        ],
        fetcher=fetcher,
        collection_root=target,
        collection_id=collection_id,
        expected_manifest_sha256=FROZEN_E1_MANIFEST_SHA256,
        expected_plan_sha256=FROZEN_E1_PLAN_SHA256,
    )
    assert manifest_path == target / "manifest.json"
    assert len(calls) == 47
    reconciliation = pd.read_parquet(
        target / "outputs/live/document_reconciliation.parquet"
    )
    assert len(reconciliation) == 47
    assert reconciliation["deterministic_action"].eq("unresolved").all()
    assert not reconciliation["unsupported_inferences_applied"].any()
    verified = verify_live_event_collection_artifact(target)
    assert verified["requests_verified"] == 47
    assert verified["summary_claim_rows"] == 0


def test_versioned_collection_continues_only_unattempted_suffix(
    tmp_path, monkeypatch
):
    source = (
        ROOT
        / "artifacts/event_review/us/20260730T144043Z-e1-final"
    )
    contract = json.loads(
        (source / "live/collection_contract.json").read_text()
    )
    approval = contract[
        "approval_token_after_explicit_user_authorization"
    ]
    calls = []

    def first_fetcher(url, headers):
        calls.append(url)
        status = 503 if len(calls) == 3 else 200
        return status, {"Content-Type": "text/html"}, b"<html>evidence</html>"

    monkeypatch.setattr(
        "portfolio.event_review.SEC_MIN_REQUEST_INTERVAL_SECONDS",
        0.0,
    )
    first = tmp_path / "20260730T211000Z-e1-collection-test"
    collect_live_event_evidence(
        source,
        confirmation=approval,
        fetcher=first_fetcher,
        collection_root=first,
        collection_id=first.name,
        expected_manifest_sha256=FROZEN_E1_MANIFEST_SHA256,
        expected_plan_sha256=FROZEN_E1_PLAN_SHA256,
    )
    assert len(calls) == 3

    def continuation_fetcher(url, headers):
        calls.append(url)
        return 200, {"Content-Type": "text/html"}, b"<html>evidence</html>"

    final = tmp_path / "20260730T212000Z-e1-collection-test"
    collect_live_event_evidence(
        source,
        confirmation=approval,
        fetcher=continuation_fetcher,
        collection_root=final,
        collection_id=final.name,
        expected_manifest_sha256=FROZEN_E1_MANIFEST_SHA256,
        expected_plan_sha256=FROZEN_E1_PLAN_SHA256,
        predecessor_root=first,
    )
    assert len(calls) == 47
    manifest = json.loads((final / "manifest.json").read_text())
    assert manifest["collection"]["request_plan_execution_complete"] is True
    assert manifest["collection"]["external_requests_in_this_version"] == 44
    assert manifest["collection"]["external_requests_in_lineage"] == 47
    verified = verify_live_event_collection_artifact(final)
    assert verified["requests_verified"] == 47


def test_full_e1_build_reuses_frozen_evidence_without_external_request(
    tmp_path,
):
    target = tmp_path / "20260730T200000Z-e1-test"
    manifest_path = build_event_review(
        target,
        review_id="20260730T200000Z-e1-test",
        s1_root=DEFAULT_S1_ROOT,
        d1_root=DEFAULT_D1_ROOT,
        p4_root=DEFAULT_P4_ROOT,
    )
    manifest = json.loads(manifest_path.read_text())
    historical = pd.read_parquet(
        target / "outputs/historical/reconciliation.parquet"
    )
    live = pd.read_parquet(
        target / "outputs/live/review_contract.parquet"
    )
    request_plan = pd.read_parquet(
        target / "live/collection_request_plan.parquet"
    )
    assert len(historical) == 180
    assert historical["stable_row_id"].nunique() == 180
    assert historical["llm_summary_allowed"].eq(False).all()
    assert historical["deterministic_action"].eq("unresolved").all()
    assert len(live) == 15
    assert live["human_review_required"].all()
    assert live["llm_summary_allowed"].eq(False).all()
    assert len(request_plan) == 47
    assert request_plan["approval_required"].all()
    assert manifest["live"]["external_requests_made"] == 0
    assert manifest["claim"]["performance_calculated"] is False
    verified = verify_event_review_artifact(target)
    assert verified["raw_responses_verified"] == 136
    assert verified["historical_rows"] == 180
    assert verified["live_rows"] == 15


def test_event_review_refuses_mutable_id_and_nonempty_target(tmp_path):
    with pytest.raises(ValueError, match="immutable"):
        build_event_review(tmp_path / "bad", review_id="latest")
    target = tmp_path / "not-empty"
    target.mkdir()
    (target / "keep").write_text("x")
    with pytest.raises(RuntimeError, match="not empty"):
        build_event_review(
            target,
            review_id="20260730T200000Z-e1-test",
        )
