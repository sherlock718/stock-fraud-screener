import hashlib
import inspect
import json

import pandas as pd
import pytest

from portfolio.final_shortlist import (
    DEFAULT_E1_ROOT,
    DEFAULT_P4_ROOT,
    EXPECTED_E1_MANIFEST_SHA256,
    EXPECTED_NOTEBOOK_SHA256,
    EXPECTED_P4_MANIFEST_SHA256,
    EXPECTED_P4_SHORTLIST_SHA256,
    LABEL_CANDIDATE,
    LABEL_EXCLUDE,
    LABEL_INCOMPLETE,
    LABEL_WARNING,
    ORIGINAL_E1_SIGNOFF_STATUS,
    PERFORMANCE_STATUS,
    PRESENTATION_SIGNOFF_STATUS,
    ROOT,
    build_final_shortlist_artifact,
    build_presentation_tables,
    presentation_label_for_action,
    selection_comment_from_structured,
    sha256_file,
    verify_final_shortlist_artifact,
)


def _sources():
    p4 = pd.read_parquet(
        DEFAULT_P4_ROOT / "outputs/latest_shortlist.parquet"
    )
    names = pd.read_parquet(
        DEFAULT_E1_ROOT / "outputs/live/name_level_adjudication.parquet"
    )
    documents = pd.read_parquet(
        DEFAULT_E1_ROOT / "outputs/live/document_level_adjudication.parquet"
    )
    return p4, names, documents


def test_presentation_policy_v2_maps_only_structured_e1_action():
    assert presentation_label_for_action("exclude") == LABEL_EXCLUDE
    assert presentation_label_for_action("warn") == LABEL_WARNING
    assert presentation_label_for_action("unresolved") == LABEL_INCOMPLETE
    assert (
        presentation_label_for_action(
            "pass",
            complete_clean_contract=True,
        )
        == LABEL_CANDIDATE
    )
    with pytest.raises(ValueError):
        presentation_label_for_action(
            "pass",
            complete_clean_contract=False,
        )
    with pytest.raises(ValueError):
        presentation_label_for_action("no_action")
    assert "narrative" not in inspect.signature(
        presentation_label_for_action
    ).parameters
    assert "summary" not in inspect.signature(
        presentation_label_for_action
    ).parameters


def test_comments_and_decisions_reproduce_without_llm_or_narrative_inputs():
    p4, names, documents = _sources()
    presentation, derivative, traceability = build_presentation_tables(
        p4,
        names,
        documents,
        original_e1_signoff_status=ORIGINAL_E1_SIGNOFF_STATUS,
    )
    for row in presentation.to_dict("records"):
        assert row["selection_comment"] == selection_comment_from_structured(
            row
        )
        assert (
            "no adverse event is inferred"
            in row["selection_comment"]
        )

    mutated = names.copy()
    mutated["adjudication_summary"] = "UNTRUSTED GENERATED NARRATIVE"
    mutated["remaining_ambiguity"] = "UNTRUSTED NARRATIVE OVERRIDE"
    mutated["llm_output"] = "EXCLUDE EVERYTHING"
    mutated["narrative_recommendation"] = LABEL_EXCLUDE
    altered, altered_derivative, altered_traceability = (
        build_presentation_tables(
            p4,
            mutated,
            documents,
            original_e1_signoff_status=ORIGINAL_E1_SIGNOFF_STATUS,
        )
    )
    immutable = [
        "rank",
        "ticker",
        "stable_row_id",
        "decision_timestamp",
        "holding",
        "weight",
        "presentation_included",
        "presentation_excluded",
        "deterministic_recommendation",
        "selection_comment",
    ]
    pd.testing.assert_frame_equal(
        presentation[immutable],
        altered[immutable],
    )
    pd.testing.assert_series_equal(
        derivative["e1_deterministic_action"],
        altered_derivative["e1_deterministic_action"],
    )
    pd.testing.assert_series_equal(
        traceability["deterministic_recommendation"],
        altered_traceability["deterministic_recommendation"],
    )
    assert "llm" not in inspect.signature(
        build_presentation_tables
    ).parameters


def test_frozen_p4_values_and_all_e1_citations_reproduce_exactly():
    p4, names, documents = _sources()
    presentation, derivative, traceability = build_presentation_tables(
        p4,
        names,
        documents,
        original_e1_signoff_status=ORIGINAL_E1_SIGNOFF_STATUS,
    )
    assert sha256_file(DEFAULT_P4_ROOT / "manifest.json") == (
        EXPECTED_P4_MANIFEST_SHA256
    )
    assert sha256_file(
        DEFAULT_P4_ROOT / "outputs/latest_shortlist.parquet"
    ) == EXPECTED_P4_SHORTLIST_SHA256
    assert sha256_file(DEFAULT_E1_ROOT / "manifest.json") == (
        EXPECTED_E1_MANIFEST_SHA256
    )
    exact_pairs = (
        ("rank", "rank"),
        ("ticker", "ticker"),
        ("company_name", "name"),
        ("stable_row_id", "stable_row_id"),
        ("decision_timestamp", "decision_timestamp"),
        ("decision_tree_oos_score", "decision_tree_prediction"),
        (
            "lightgbm_oos_three_year_score",
            "lightgbm_regression_prediction",
        ),
        ("market_cap_gate_value", "gate_market_cap_value"),
        ("market_cap_gate_status", "gate_market_cap_status"),
        ("market_cap_gate_pass", "gate_market_cap_pass"),
        ("market_cap_gate_provenance", "gate_market_cap_provenance"),
        (
            "median_30_session_dollar_volume",
            "median_30_session_dollar_volume",
        ),
        ("liquidity_pass", "liquidity_pass"),
        ("fraud_risk_composite", "fraud_score_composite"),
        ("all_fixed_gates_pass", "all_non_model_hard_gates_pass"),
        ("holding", "holding"),
        ("weight", "weight"),
    )
    p4 = p4.sort_values("rank").reset_index(drop=True)
    for output_column, source_column in exact_pairs:
        assert presentation[output_column].tolist() == p4[
            source_column
        ].tolist()

    source_claims = documents.loc[
        documents["claim_id"].notna(),
        [
            "claim_id",
            "source_id",
            "source_response_sha256",
            "evidence_path",
            "document_locator",
            "supporting_passage_sha256",
            "supporting_passage",
        ],
    ].sort_values("claim_id").reset_index(drop=True)
    reproduced = traceability.loc[
        traceability["e1_claim_id"].notna(),
        [
            "e1_claim_id",
            "e1_source_id",
            "e1_source_response_sha256",
            "e1_evidence_path",
            "e1_document_locator",
            "e1_supporting_passage_sha256",
            "e1_supporting_passage",
        ],
    ].sort_values("e1_claim_id").reset_index(drop=True)
    reproduced.columns = source_claims.columns
    pd.testing.assert_frame_equal(source_claims, reproduced)
    assert len(reproduced) == 46
    assert len(derivative) == 15


def test_summary_signoff_and_performance_remain_fail_closed():
    p4, names, documents = _sources()
    presentation, derivative, _ = build_presentation_tables(
        p4,
        names,
        documents,
        original_e1_signoff_status=ORIGINAL_E1_SIGNOFF_STATUS,
    )
    by_ticker = presentation.set_index("ticker")
    assert by_ticker["e1_summary_allowed"].sum() == 13
    for ticker in ("HPK", "SSTK"):
        assert by_ticker.loc[ticker, "summary_prohibited"]
        assert pd.isna(by_ticker.loc[ticker, "frozen_e1_summary"])
        assert by_ticker.loc[ticker, "summary_prohibited_reason"]
    assert "HTTP 503" in by_ticker.loc[
        "HPK", "summary_prohibited_reason"
    ]
    assert "No primary-document candidate" in by_ticker.loc[
        "SSTK", "summary_prohibited_reason"
    ]
    assert not presentation["presentation_human_signoff_required"].any()
    assert presentation["presentation_signoff_status"].eq(
        PRESENTATION_SIGNOFF_STATUS
    ).all()
    assert presentation["original_e1_signoff_status"].eq(
        ORIGINAL_E1_SIGNOFF_STATUS
    ).all()
    assert not derivative["human_signature_present"].any()
    assert presentation["performance_status"].eq(PERFORMANCE_STATUS).all()
    assert not presentation["performance_metrics_available"].any()


def test_non_overwriting_artifact_manifest_rehash_and_preservation(tmp_path):
    shortlist_id = "20260731T120000Z-final-shortlist-v2"
    target = tmp_path / shortlist_id
    protected = {
        "p4_manifest": sha256_file(DEFAULT_P4_ROOT / "manifest.json"),
        "p4_shortlist": sha256_file(
            DEFAULT_P4_ROOT / "outputs/latest_shortlist.parquet"
        ),
        "e1_manifest": sha256_file(DEFAULT_E1_ROOT / "manifest.json"),
        "notebook": sha256_file(
            ROOT / "notebooks/production_screener.ipynb"
        ),
    }
    manifest_path = build_final_shortlist_artifact(
        target,
        shortlist_id=shortlist_id,
    )
    manifest = json.loads(manifest_path.read_text())
    verified = verify_final_shortlist_artifact(target)
    assert verified["presentation_rows"] == 15
    assert verified["e1_derivative_rows"] == 15
    assert verified["traceability_rows"] == 47
    assert verified["exact_e1_citations_reproduced"] == 46
    assert verified["summary_prohibited_rows"] == 2
    assert verified["performance_calculated"] is False
    assert manifest["source_p4"]["records_rehashed"] == 25
    assert manifest["source_p4"]["validated_inputs_rehashed"] == 8
    assert manifest["source_e1"]["records_rehashed"] == 70
    assert manifest["presentation"][
        "presentation_human_signoff_required"
    ] is False
    assert manifest["claim"]["generative_llm_used"] is False
    assert (
        manifest["historical_source_code_lineage"]["current_comparison"][0]
    )
    recorded = {
        item["path"]: item for item in manifest["records"]
    }
    for relative, item in recorded.items():
        path = target / relative
        assert path.stat().st_size == item["size_bytes"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"]
    assert sha256_file(DEFAULT_P4_ROOT / "manifest.json") == protected[
        "p4_manifest"
    ]
    assert sha256_file(
        DEFAULT_P4_ROOT / "outputs/latest_shortlist.parquet"
    ) == protected["p4_shortlist"]
    assert sha256_file(DEFAULT_E1_ROOT / "manifest.json") == protected[
        "e1_manifest"
    ]
    assert sha256_file(
        ROOT / "notebooks/production_screener.ipynb"
    ) == protected["notebook"] == EXPECTED_NOTEBOOK_SHA256
    with pytest.raises(RuntimeError, match="not empty"):
        build_final_shortlist_artifact(
            target,
            shortlist_id=shortlist_id,
        )
