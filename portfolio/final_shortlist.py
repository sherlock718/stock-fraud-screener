"""Build the canonical presentation-only derivative of frozen P4 and E1.

This module performs no network I/O, model execution, performance calculation,
or generative summarization. It rehashes the complete frozen P4 and E1
boundaries, applies presentation policy v2 from structured fields only, and
writes a new non-overwriting final-shortlist artifact.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from portfolio.selection_contract import MIN_ADTV, TREE_THRESHOLD, WEIGHT


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_P4_ROOT = (
    ROOT / "artifacts/canonical/corrected_us_annual_3y_product"
)
DEFAULT_E1_ROOT = (
    ROOT
    / "artifacts/event_review/us/"
    "20260730T173110Z-e1-adjudication-v2"
)
DEFAULT_ARTIFACT_PARENT = ROOT / "artifacts/final_shortlist/us"

EXPECTED_P4_MANIFEST_SHA256 = (
    "28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7"
)
EXPECTED_P4_SHORTLIST_SHA256 = (
    "93bd6104a73e3752da019f7767cc7dfa72bfca837149179dd72b9b87bf466a3b"
)
EXPECTED_E1_MANIFEST_SHA256 = (
    "dcbf95776b50139797410674d7f0ca410cfdaa1401917dea4fb47f7b00e9c7c6"
)
EXPECTED_NOTEBOOK_SHA256 = (
    "73026696f5d45ec2fa6bafb7941000433cad97fce4a19a9e3a69786d195ad21d"
)

FINAL_SHORTLIST_ID_PATTERN = re.compile(
    r"^\d{8}T\d{6}Z-final-shortlist-v\d+$"
)

LABEL_EXCLUDE = "EXCLUDE_EVENT_EVIDENCE"
LABEL_WARNING = "RESEARCH_CANDIDATE_EVENT_WARNING"
LABEL_INCOMPLETE = "RESEARCH_CANDIDATE_EVENT_EVIDENCE_INCOMPLETE"
LABEL_CANDIDATE = "RESEARCH_CANDIDATE"

ORIGINAL_E1_SIGNOFF_STATUS = "unsigned_pending_explicit_human_signoff"
PRESENTATION_SIGNOFF_STATUS = "not_required"
PERFORMANCE_STATUS = "unavailable_fail_closed"

RESEARCH_ONLY_DISCLAIMER = (
    "Research-only output; not personalized investment advice, a recommendation "
    "to transact, or a claim or guarantee of future performance."
)
LIMITATIONS = (
    "Official performance is unavailable and fail-closed; no performance "
    "metric was calculated.",
    "The US annual population is historically enriched, not comprehensively "
    "survivorship-free.",
    "Free sources do not establish CRSP-quality historical membership, "
    "security/ticker histories, delisting terms, or delisting returns.",
    "Frozen Yahoo evidence supports only the pre-decision liquidity gate and "
    "is not promoted to canonical performance evidence.",
    "Certified macro vintages remain unavailable and unused.",
    "Incomplete event evidence is neither an adverse-event warning nor an "
    "exclusion.",
)

PRESENTATION_COLUMNS = (
    "rank",
    "ticker",
    "company_name",
    "stable_row_id",
    "decision_timestamp",
    "decision_tree_oos_score",
    "lightgbm_oos_three_year_score",
    "tree_agreement_threshold",
    "tree_agreement_pass",
    "market_cap_gate_value",
    "market_cap_gate_status",
    "market_cap_gate_pass",
    "market_cap_gate_provenance",
    "median_30_session_dollar_volume",
    "liquidity_threshold",
    "liquidity_pass",
    "fraud_risk_composite",
    "all_fixed_gates_pass",
    "holding",
    "weight",
    "presentation_included",
    "presentation_excluded",
    "deterministic_recommendation",
    "selection_comment",
    "e1_deterministic_action",
    "e1_deterministic_reason",
    "original_e1_signoff_status",
    "presentation_human_signoff_required",
    "presentation_signoff_status",
    "event_evidence_comment",
    "e1_summary_allowed",
    "e1_summary_status",
    "frozen_e1_summary",
    "e1_cited_claim_ids_json",
    "e1_citation_count",
    "summary_prohibited",
    "summary_prohibited_reason",
    "performance_status",
    "performance_metrics_available",
    "limitations_json",
    "research_only_disclaimer",
)

E1_DERIVATIVE_COLUMNS = (
    "rank",
    "ticker",
    "company_name",
    "stable_row_id",
    "decision_timestamp",
    "e1_adjudication_manifest_sha256",
    "e1_requirement_id",
    "e1_deterministic_action",
    "e1_deterministic_reason",
    "e1_frozen_rule_requirements_fully_satisfied",
    "e1_summary_allowed",
    "e1_summary_status",
    "frozen_e1_summary",
    "e1_cited_claim_ids_json",
    "summary_prohibited",
    "summary_prohibited_reason",
    "original_e1_signoff_status",
    "presentation_human_signoff_required",
    "presentation_signoff_status",
    "summary_may_change_deterministic_action",
    "machine_attested",
    "human_signature_present",
)

TRACEABILITY_COLUMNS = (
    "rank",
    "ticker",
    "stable_row_id",
    "decision_timestamp",
    "p4_manifest_sha256",
    "p4_shortlist_sha256",
    "p4_source_path",
    "e1_adjudication_manifest_sha256",
    "e1_requirement_id",
    "e1_deterministic_action",
    "e1_name_source_path",
    "e1_document_source_path",
    "e1_claim_id",
    "e1_request_id",
    "e1_source_id",
    "e1_source_publication_time",
    "e1_source_retrieved_at",
    "e1_source_response_sha256",
    "e1_evidence_path",
    "e1_document_locator",
    "e1_supporting_passage_sha256",
    "e1_supporting_passage",
    "e1_summary_allowed",
    "summary_prohibited",
    "deterministic_recommendation",
    "selection_comment_sha256",
)

CURRENT_CODE_PATHS = (
    "portfolio/final_shortlist.py",
    "portfolio/selection_contract.py",
    "portfolio/build_canonical_product.py",
    "portfolio/event_review_adjudication.py",
    "workflows/build_final_shortlist.py",
    "tests/portfolio/test_final_shortlist.py",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _json_value(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


def _compact_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_value,
    )


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _record(
    artifact_root: Path,
    path: Path,
    role: str,
) -> dict[str, Any]:
    return {
        "path": path.relative_to(artifact_root).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _source_record(path: Path, role: str) -> dict[str, Any]:
    return {
        "path": _display_path(path),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _verify_record_list(
    base: Path,
    records: list[dict[str, Any]],
    *,
    repo_relative_paths: bool,
) -> int:
    seen: set[str] = set()
    for item in records:
        relative = str(item["path"])
        if relative in seen:
            raise RuntimeError(f"duplicate manifest record: {relative}")
        seen.add(relative)
        path = ROOT / relative if repo_relative_paths else base / relative
        if (
            not path.is_file()
            or path.stat().st_size != int(item["size_bytes"])
            or sha256_file(path) != item["sha256"]
        ):
            raise RuntimeError(f"manifest record mismatch: {relative}")
    return len(records)


def _preservation_entry_count(preservation: Mapping[str, Any]) -> int:
    return sum(
        len(preservation[key])
        for key in (
            "canonical_manifests",
            "pointer_revisions",
            "source_evidence",
            "tracked_legacy_and_international",
        )
    )


def _verify_preservation(preservation: Mapping[str, Any]) -> None:
    for group in (
        "canonical_manifests",
        "source_evidence",
        "tracked_legacy_and_international",
    ):
        for relative, expected in preservation[group].items():
            path = ROOT / relative
            if sha256_file(path) != expected:
                raise RuntimeError(f"preservation hash mismatch: {relative}")
    for name, expected_revision in preservation["pointer_revisions"].items():
        pointer_path = ROOT / "data_io/canonical_artifact_pointers" / name
        pointer = json.loads(pointer_path.read_text())
        if pointer.get("revision") != expected_revision:
            raise RuntimeError(f"pointer revision mismatch: {name}")


def _verify_p4_source(p4_root: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    manifest_path = p4_root / "manifest.json"
    shortlist_path = p4_root / "outputs/latest_shortlist.parquet"
    if sha256_file(manifest_path) != EXPECTED_P4_MANIFEST_SHA256:
        raise RuntimeError("frozen P4 manifest hash mismatch")
    if sha256_file(shortlist_path) != EXPECTED_P4_SHORTLIST_SHA256:
        raise RuntimeError("frozen P4 latest-shortlist hash mismatch")
    manifest = json.loads(manifest_path.read_text())
    if _verify_record_list(
        ROOT,
        manifest["records"],
        repo_relative_paths=True,
    ) != 25:
        raise RuntimeError("frozen P4 artifact-record count mismatch")
    if _verify_record_list(
        ROOT,
        manifest["validated_inputs"],
        repo_relative_paths=True,
    ) != 8:
        raise RuntimeError("frozen P4 validated-input count mismatch")

    shortlist = pd.read_parquet(shortlist_path).sort_values("rank")
    if (
        len(shortlist) != 15
        or shortlist["rank"].tolist() != list(range(1, 16))
        or shortlist["stable_row_id"].duplicated().any()
        or not shortlist["holding"].all()
        or not shortlist["all_non_model_hard_gates_pass"].all()
        or not shortlist["tree_threshold_pass"].all()
        or not shortlist["liquidity_pass"].all()
        or not shortlist["weight"].map(
            lambda value: math.isclose(
                float(value),
                WEIGHT,
                rel_tol=0.0,
                abs_tol=1e-15,
            )
        ).all()
    ):
        raise RuntimeError("frozen P4 shortlist contract mismatch")
    return manifest, shortlist.reset_index(drop=True)


def _verify_e1_source(
    e1_root: Path,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    manifest_path = e1_root / "manifest.json"
    if sha256_file(manifest_path) != EXPECTED_E1_MANIFEST_SHA256:
        raise RuntimeError("frozen E1 adjudication manifest hash mismatch")
    manifest = json.loads(manifest_path.read_text())
    if _verify_record_list(
        e1_root,
        manifest["records"],
        repo_relative_paths=False,
    ) != 70:
        raise RuntimeError("frozen E1 artifact-record count mismatch")
    if _preservation_entry_count(manifest["preservation"]) != 29:
        raise RuntimeError("frozen E1 preservation count mismatch")
    _verify_preservation(manifest["preservation"])

    names = pd.read_parquet(
        e1_root / "outputs/live/name_level_adjudication.parquet"
    ).sort_values("rank")
    documents = pd.read_parquet(
        e1_root / "outputs/live/document_level_adjudication.parquet"
    )
    signoff = json.loads(
        (e1_root / "signoff/human_signoff_contract.json").read_text()
    )
    if (
        len(names) != 15
        or len(documents) != 47
        or documents["claim_id"].notna().sum() != 46
        or names["deterministic_action"].ne("unresolved").any()
        or names["summary_allowed"].sum() != 13
        or names.set_index("ticker").loc["HPK", "summary_allowed"]
        or names.set_index("ticker").loc["SSTK", "summary_allowed"]
        or signoff["signoff_status"] != ORIGINAL_E1_SIGNOFF_STATUS
    ):
        raise RuntimeError("frozen E1 adjudication contract mismatch")
    return (
        manifest,
        names.reset_index(drop=True),
        documents.reset_index(drop=True),
        signoff,
    )


def presentation_label_for_action(
    deterministic_action: str,
    *,
    complete_clean_contract: bool = False,
) -> str:
    """Map only structured E1 action state to presentation policy v2."""
    mapping = {
        "exclude": LABEL_EXCLUDE,
        "warn": LABEL_WARNING,
        "unresolved": LABEL_INCOMPLETE,
    }
    if deterministic_action in mapping:
        return mapping[deterministic_action]
    if deterministic_action == "pass" and complete_clean_contract:
        return LABEL_CANDIDATE
    raise ValueError(
        "presentation policy has no eligible label for action/contract state"
    )


def selection_comment_from_structured(row: Mapping[str, Any]) -> str:
    """Render the frozen selection comment solely from structured fields."""
    action = str(row["e1_deterministic_action"])
    if action == "unresolved":
        event_clause = (
            "Event evidence remains unresolved because the evidence contract "
            "is incomplete; no adverse event is inferred."
        )
    elif action == "exclude":
        event_clause = (
            "Complete deterministic E1 evidence requires exclusion under the "
            "frozen policy."
        )
    elif action == "warn":
        event_clause = (
            "Complete deterministic E1 evidence produces an event warning "
            "under the frozen policy."
        )
    elif action == "pass":
        event_clause = (
            "The complete deterministic E1 contract is clean under the "
            "frozen policy."
        )
    else:
        raise ValueError(f"unsupported E1 action for comment: {action}")
    return (
        f"Rank {int(row['rank'])} after passing all eight fixed hard gates, "
        f"the {float(row['tree_agreement_threshold']):.2f} tree agreement "
        "requirement, and the liquidity threshold. "
        f"Tree OOS score {float(row['decision_tree_oos_score']):.6f}; "
        "LightGBM OOS score "
        f"{float(row['lightgbm_oos_three_year_score']):.6f}; "
        "median pre-decision dollar volume "
        f"${float(row['median_30_session_dollar_volume']):,.2f}; "
        f"fraud-risk composite {float(row['fraud_risk_composite']):.6f}. "
        f"{event_clause}"
    )


def _event_evidence_comment(
    *,
    action: str,
    summary_allowed: bool,
    citation_count: int,
    prohibited_reason: str | None,
) -> str:
    if action == "unresolved" and summary_allowed:
        return (
            "Event evidence is incomplete and E1 action remains unresolved. "
            f"A claim-cited E1 summary is available from {citation_count} "
            "exact citation(s). Incomplete evidence is not an adverse-event "
            "warning or exclusion."
        )
    if action == "unresolved":
        return (
            "Event evidence is incomplete and E1 action remains unresolved. "
            f"Summary is prohibited: {prohibited_reason} Incomplete evidence "
            "is not an adverse-event warning or exclusion."
        )
    if action == "exclude":
        return "Complete deterministic E1 evidence requires exclusion."
    if action == "warn":
        return "Complete deterministic E1 evidence produces an event warning."
    if action == "pass":
        return "The complete deterministic E1 contract is clean."
    raise ValueError(f"unsupported E1 action for evidence comment: {action}")


def _citation_dict(document: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "claim_id",
        "request_id",
        "source_id",
        "source_publication_time",
        "source_retrieved_at",
        "source_response_sha256",
        "evidence_path",
        "document_locator",
        "supporting_passage_sha256",
        "supporting_passage",
    )
    return {field: _json_value(document[field]) for field in fields}


def build_presentation_tables(
    p4_shortlist: pd.DataFrame,
    e1_names: pd.DataFrame,
    e1_documents: pd.DataFrame,
    *,
    original_e1_signoff_status: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build presentation, E1 derivative, and trace tables deterministically."""
    if original_e1_signoff_status != ORIGINAL_E1_SIGNOFF_STATUS:
        raise RuntimeError("original E1 unsigned state changed")

    p4 = p4_shortlist.sort_values("rank").reset_index(drop=True)
    names = e1_names.sort_values("rank").reset_index(drop=True)
    identity_columns = (
        "rank",
        "ticker",
        "stable_row_id",
        "decision_timestamp",
    )
    for column in identity_columns:
        left = p4[column].astype(str).tolist()
        right = names[column].astype(str).tolist()
        if left != right:
            raise RuntimeError(f"P4/E1 frozen identity mismatch: {column}")
    if p4["name"].astype(str).tolist() != names["name"].astype(str).tolist():
        raise RuntimeError("P4/E1 frozen company-name mismatch")

    claim_documents = e1_documents.loc[
        e1_documents["claim_id"].notna()
    ].copy()
    claim_lookup = {
        str(row["claim_id"]): row
        for row in claim_documents.to_dict("records")
    }
    presentation_rows: list[dict[str, Any]] = []
    derivative_rows: list[dict[str, Any]] = []
    trace_rows: list[dict[str, Any]] = []

    for p4_row, e1_row in zip(
        p4.to_dict("records"),
        names.to_dict("records"),
    ):
        action = str(e1_row["deterministic_action"])
        complete_clean = bool(
            e1_row["frozen_rule_requirements_fully_satisfied"]
        )
        recommendation = presentation_label_for_action(
            action,
            complete_clean_contract=complete_clean,
        )
        cited_ids = json.loads(str(e1_row["cited_claim_ids"]))
        ticker_claim_ids = sorted(
            claim_documents.loc[
                claim_documents["ticker"].eq(e1_row["ticker"]),
                "claim_id",
            ]
            .astype(str)
            .tolist()
        )
        if sorted(cited_ids) != ticker_claim_ids:
            raise RuntimeError(
                f"E1 name/document citation mismatch: {e1_row['ticker']}"
            )
        citations = [_citation_dict(claim_lookup[claim_id]) for claim_id in cited_ids]
        summary_allowed = bool(e1_row["summary_allowed"])
        summary_prohibited = not summary_allowed
        prohibited_reason = (
            str(e1_row["remaining_ambiguity"])
            if summary_prohibited
            else None
        )
        frozen_summary = (
            _json_value(e1_row["adjudication_summary"])
            if summary_allowed
            else None
        )
        if summary_allowed and not frozen_summary:
            raise RuntimeError("summary-eligible E1 name lacks frozen summary")
        if summary_prohibited and frozen_summary is not None:
            raise RuntimeError("summary-prohibited E1 name has narrative")

        structured = {
            "rank": int(p4_row["rank"]),
            "tree_agreement_threshold": TREE_THRESHOLD,
            "decision_tree_oos_score": float(
                p4_row["decision_tree_prediction"]
            ),
            "lightgbm_oos_three_year_score": float(
                p4_row["lightgbm_regression_prediction"]
            ),
            "median_30_session_dollar_volume": float(
                p4_row["median_30_session_dollar_volume"]
            ),
            "fraud_risk_composite": float(
                p4_row["fraud_score_composite"]
            ),
            "e1_deterministic_action": action,
        }
        selection_comment = selection_comment_from_structured(structured)
        event_comment = _event_evidence_comment(
            action=action,
            summary_allowed=summary_allowed,
            citation_count=len(citations),
            prohibited_reason=prohibited_reason,
        )
        presentation_rows.append(
            {
                "rank": int(p4_row["rank"]),
                "ticker": str(p4_row["ticker"]),
                "company_name": str(p4_row["name"]),
                "stable_row_id": str(p4_row["stable_row_id"]),
                "decision_timestamp": p4_row["decision_timestamp"],
                "decision_tree_oos_score": structured[
                    "decision_tree_oos_score"
                ],
                "lightgbm_oos_three_year_score": structured[
                    "lightgbm_oos_three_year_score"
                ],
                "tree_agreement_threshold": TREE_THRESHOLD,
                "tree_agreement_pass": bool(
                    p4_row["tree_threshold_pass"]
                ),
                "market_cap_gate_value": float(
                    p4_row["gate_market_cap_value"]
                ),
                "market_cap_gate_status": str(
                    p4_row["gate_market_cap_status"]
                ),
                "market_cap_gate_pass": bool(
                    p4_row["gate_market_cap_pass"]
                ),
                "market_cap_gate_provenance": str(
                    p4_row["gate_market_cap_provenance"]
                ),
                "median_30_session_dollar_volume": structured[
                    "median_30_session_dollar_volume"
                ],
                "liquidity_threshold": MIN_ADTV,
                "liquidity_pass": bool(p4_row["liquidity_pass"]),
                "fraud_risk_composite": structured[
                    "fraud_risk_composite"
                ],
                "all_fixed_gates_pass": bool(
                    p4_row["all_non_model_hard_gates_pass"]
                ),
                "holding": bool(p4_row["holding"]),
                "weight": float(p4_row["weight"]),
                "presentation_included": True,
                "presentation_excluded": False,
                "deterministic_recommendation": recommendation,
                "selection_comment": selection_comment,
                "e1_deterministic_action": action,
                "e1_deterministic_reason": str(
                    e1_row["deterministic_reason"]
                ),
                "original_e1_signoff_status": original_e1_signoff_status,
                "presentation_human_signoff_required": False,
                "presentation_signoff_status": PRESENTATION_SIGNOFF_STATUS,
                "event_evidence_comment": event_comment,
                "e1_summary_allowed": summary_allowed,
                "e1_summary_status": str(e1_row["summary_status"]),
                "frozen_e1_summary": frozen_summary,
                "e1_cited_claim_ids_json": _compact_json(cited_ids),
                "e1_citation_count": len(citations),
                "summary_prohibited": summary_prohibited,
                "summary_prohibited_reason": prohibited_reason,
                "performance_status": PERFORMANCE_STATUS,
                "performance_metrics_available": False,
                "limitations_json": _compact_json(list(LIMITATIONS)),
                "research_only_disclaimer": RESEARCH_ONLY_DISCLAIMER,
            }
        )
        derivative_rows.append(
            {
                "rank": int(e1_row["rank"]),
                "ticker": str(e1_row["ticker"]),
                "company_name": str(e1_row["name"]),
                "stable_row_id": str(e1_row["stable_row_id"]),
                "decision_timestamp": e1_row["decision_timestamp"],
                "e1_adjudication_manifest_sha256": (
                    EXPECTED_E1_MANIFEST_SHA256
                ),
                "e1_requirement_id": str(e1_row["requirement_id"]),
                "e1_deterministic_action": action,
                "e1_deterministic_reason": str(
                    e1_row["deterministic_reason"]
                ),
                "e1_frozen_rule_requirements_fully_satisfied": complete_clean,
                "e1_summary_allowed": summary_allowed,
                "e1_summary_status": str(e1_row["summary_status"]),
                "frozen_e1_summary": frozen_summary,
                "e1_cited_claim_ids_json": _compact_json(cited_ids),
                "summary_prohibited": summary_prohibited,
                "summary_prohibited_reason": prohibited_reason,
                "original_e1_signoff_status": original_e1_signoff_status,
                "presentation_human_signoff_required": False,
                "presentation_signoff_status": PRESENTATION_SIGNOFF_STATUS,
                "summary_may_change_deterministic_action": False,
                "machine_attested": True,
                "human_signature_present": False,
            }
        )
        trace_citations: list[dict[str, Any] | None] = (
            citations if citations else [None]
        )
        for citation in trace_citations:
            trace_rows.append(
                {
                    "rank": int(p4_row["rank"]),
                    "ticker": str(p4_row["ticker"]),
                    "stable_row_id": str(p4_row["stable_row_id"]),
                    "decision_timestamp": p4_row["decision_timestamp"],
                    "p4_manifest_sha256": EXPECTED_P4_MANIFEST_SHA256,
                    "p4_shortlist_sha256": EXPECTED_P4_SHORTLIST_SHA256,
                    "p4_source_path": (
                        "artifacts/canonical/"
                        "corrected_us_annual_3y_product/outputs/"
                        "latest_shortlist.parquet"
                    ),
                    "e1_adjudication_manifest_sha256": (
                        EXPECTED_E1_MANIFEST_SHA256
                    ),
                    "e1_requirement_id": str(e1_row["requirement_id"]),
                    "e1_deterministic_action": action,
                    "e1_name_source_path": (
                        "outputs/live/name_level_adjudication.parquet"
                    ),
                    "e1_document_source_path": (
                        "outputs/live/document_level_adjudication.parquet"
                    ),
                    "e1_claim_id": (
                        citation["claim_id"] if citation else None
                    ),
                    "e1_request_id": (
                        citation["request_id"] if citation else None
                    ),
                    "e1_source_id": (
                        citation["source_id"] if citation else None
                    ),
                    "e1_source_publication_time": (
                        citation["source_publication_time"]
                        if citation
                        else None
                    ),
                    "e1_source_retrieved_at": (
                        citation["source_retrieved_at"] if citation else None
                    ),
                    "e1_source_response_sha256": (
                        citation["source_response_sha256"]
                        if citation
                        else None
                    ),
                    "e1_evidence_path": (
                        citation["evidence_path"] if citation else None
                    ),
                    "e1_document_locator": (
                        citation["document_locator"] if citation else None
                    ),
                    "e1_supporting_passage_sha256": (
                        citation["supporting_passage_sha256"]
                        if citation
                        else None
                    ),
                    "e1_supporting_passage": (
                        citation["supporting_passage"]
                        if citation
                        else None
                    ),
                    "e1_summary_allowed": summary_allowed,
                    "summary_prohibited": summary_prohibited,
                    "deterministic_recommendation": recommendation,
                    "selection_comment_sha256": hashlib.sha256(
                        selection_comment.encode()
                    ).hexdigest(),
                }
            )

    presentation = pd.DataFrame(
        presentation_rows,
        columns=PRESENTATION_COLUMNS,
    )
    derivative = pd.DataFrame(
        derivative_rows,
        columns=E1_DERIVATIVE_COLUMNS,
    )
    traceability = pd.DataFrame(
        trace_rows,
        columns=TRACEABILITY_COLUMNS,
    )
    if (
        len(presentation) != 15
        or len(derivative) != 15
        or len(traceability) != 47
        or presentation["deterministic_recommendation"].ne(
            LABEL_INCOMPLETE
        ).any()
        or presentation["e1_deterministic_action"].ne("unresolved").any()
        or not presentation["presentation_included"].all()
        or presentation["presentation_excluded"].any()
        or presentation["presentation_human_signoff_required"].any()
        or presentation["performance_metrics_available"].any()
        or derivative["human_signature_present"].any()
        or traceability["e1_claim_id"].notna().sum() != 46
    ):
        raise RuntimeError("final-shortlist presentation contract failed")
    return presentation, derivative, traceability


def _presentation_policy() -> dict[str, Any]:
    return {
        "schema_version": 2,
        "policy_id": "canonical_final_shortlist_presentation_policy_v2",
        "policy_order": [
            "read_frozen_e1_deterministic_action",
            "map_action_to_presentation_label",
            "freeze_rank_holdings_and_weights_from_p4",
            "attach_citation_bound_evidence_without_mutating_decisions",
            "render_structured_comments",
        ],
        "action_to_label": {
            "exclude": LABEL_EXCLUDE,
            "warn": LABEL_WARNING,
            "unresolved": LABEL_INCOMPLETE,
        },
        "complete_clean_contract": {
            "e1_action": "pass",
            "frozen_rule_requirements_fully_satisfied": True,
            "label": LABEL_CANDIDATE,
        },
        "narrative_may_change_label": False,
        "summary_may_change_label": False,
        "incomplete_evidence_is_adverse_event_warning": False,
        "incomplete_evidence_is_exclusion": False,
        "presentation_human_signoff_required": False,
        "presentation_signoff_status": PRESENTATION_SIGNOFF_STATUS,
        "original_e1_signoff_status": ORIGINAL_E1_SIGNOFF_STATUS,
        "human_signature_invented": False,
        "decision_fields_immutable_from_p4": [
            "rank",
            "ticker",
            "company_name",
            "stable_row_id",
            "decision_timestamp",
            "holding",
            "weight",
        ],
        "prohibited_decision_inputs": [
            "generative_llm",
            "llm_output",
            "narrative",
            "summary",
            "human_signoff",
        ],
    }


def _comment_contract() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "contract_id": "deterministic_final_shortlist_comment_contract_v1",
        "template": (
            "Rank {rank} after passing all eight fixed hard gates, the "
            "{tree_threshold:.2f} tree agreement requirement, and the "
            "liquidity threshold. Tree OOS score {tree_score:.6f}; "
            "LightGBM OOS score {ranker_score:.6f}; median pre-decision "
            "dollar volume ${adtv:,.2f}; fraud-risk composite "
            "{fraud_score:.6f}. {event_clause}"
        ),
        "structured_inputs": [
            "rank",
            "tree_agreement_threshold",
            "decision_tree_oos_score",
            "lightgbm_oos_three_year_score",
            "median_30_session_dollar_volume",
            "fraud_risk_composite",
            "e1_deterministic_action",
        ],
        "unresolved_event_clause": (
            "Event evidence remains unresolved because the evidence contract "
            "is incomplete; no adverse event is inferred."
        ),
        "generative_model_dependency": False,
        "narrative_inputs_allowed": False,
        "summary_inputs_allowed": False,
    }


def _historical_code_lineage(
    p4_manifest: Mapping[str, Any],
    e1_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    comparisons = []
    for source, records in (
        ("P4", p4_manifest["code_lineage"]),
        ("E1_ADJUDICATION", e1_manifest["code_lineage"]),
    ):
        for historical in records:
            path = ROOT / historical["path"]
            current = {
                "path": historical["path"],
                "exists": path.is_file(),
                "size_bytes": path.stat().st_size if path.is_file() else None,
                "sha256": sha256_file(path) if path.is_file() else None,
            }
            matches = (
                current["exists"]
                and current["size_bytes"] == historical["size_bytes"]
                and current["sha256"] == historical["sha256"]
            )
            if (
                source == "P4"
                and historical["path"]
                == "portfolio/build_canonical_product.py"
                and not matches
            ):
                status = "expected_post_p4_c1_code_evolution"
            elif matches:
                status = "matches_historical_source_record"
            else:
                raise RuntimeError(
                    "unexpected historical source-code lineage drift: "
                    f"{source}:{historical['path']}"
                )
            comparisons.append(
                {
                    "source": source,
                    "historical": historical,
                    "current": current,
                    "matches": bool(matches),
                    "status": status,
                    "artifact_record_corruption": False,
                }
            )
    return {
        "schema_version": 1,
        "p4_manifest_sha256": EXPECTED_P4_MANIFEST_SHA256,
        "e1_adjudication_manifest_sha256": EXPECTED_E1_MANIFEST_SHA256,
        "p4_historical_code_lineage": p4_manifest["code_lineage"],
        "e1_historical_code_lineage": e1_manifest["code_lineage"],
        "current_comparison": comparisons,
        "p4_builder_code_evolution_policy": (
            "The historical P4 source record remains valid for the frozen "
            "artifact. The current post-P4 C1 implementation is recorded "
            "separately and is not treated as artifact-record corruption."
        ),
    }


def _render_report(
    shortlist_id: str,
    presentation: pd.DataFrame,
) -> str:
    rows = []
    details = []
    for row in presentation.sort_values("rank").itertuples(index=False):
        rows.append(
            "| {rank} | {ticker} | {tree:.6f} | {ranker:.6f} | "
            "${cap:,.0f} | ${adtv:,.0f} | {fraud:.6f} | {label} |".format(
                rank=int(row.rank),
                ticker=row.ticker,
                tree=row.decision_tree_oos_score,
                ranker=row.lightgbm_oos_three_year_score,
                cap=row.market_cap_gate_value,
                adtv=row.median_30_session_dollar_volume,
                fraud=row.fraud_risk_composite,
                label=row.deterministic_recommendation,
            )
        )
        evidence = (
            row.frozen_e1_summary
            if row.e1_summary_allowed
            else f"Summary prohibited: {row.summary_prohibited_reason}"
        )
        details.append(
            f"### {int(row.rank)}. {row.ticker} — {row.company_name}\n\n"
            f"{row.selection_comment}\n\n"
            f"{row.event_evidence_comment}\n\n"
            f"E1 evidence: {evidence}\n\n"
            f"Stable row ID: `{row.stable_row_id}`; decision timestamp: "
            f"`{pd.Timestamp(row.decision_timestamp).isoformat()}`; "
            f"holding: `{str(bool(row.holding)).lower()}`; "
            f"weight: `{float(row.weight):.15f}`; original E1 sign-off: "
            f"`{row.original_e1_signoff_status}`; presentation sign-off: "
            f"`{row.presentation_signoff_status}`."
        )
    limitations = "\n".join(f"- {item}" for item in LIMITATIONS)
    return f"""# Canonical Final Shortlist

Artifact: `{shortlist_id}`

Frozen sources:

- P4 manifest: `{EXPECTED_P4_MANIFEST_SHA256}`
- P4 latest shortlist: `{EXPECTED_P4_SHORTLIST_SHA256}`
- E1 adjudication manifest: `{EXPECTED_E1_MANIFEST_SHA256}`

All 15 frozen P4 names remain included, in their exact ranks and at their
exact `1/15` holdings weights. Every E1 action is `unresolved`; presentation
policy v2 therefore assigns only
`{LABEL_INCOMPLETE}`. Incomplete evidence is not an adverse-event warning or
exclusion. Human sign-off is not a presentation issuance gate; presentation
status is `{PRESENTATION_SIGNOFF_STATUS}` while the original E1 artifact
remains `{ORIGINAL_E1_SIGNOFF_STATUS}`.

| Rank | Ticker | Tree OOS | LightGBM OOS 3y | Market-cap gate | Median 30-session dollar volume | Fraud risk | Deterministic presentation |
|---:|---|---:|---:|---:|---:|---:|---|
{chr(10).join(rows)}

## Stock-level comments and cited evidence

{chr(10).join(details)}

## Performance and limitations

Official performance is **unavailable and fail-closed**. No return, NAV,
Sharpe ratio, drawdown, or other performance metric was calculated.

{limitations}

## Research-only disclaimer

{RESEARCH_ONLY_DISCLAIMER}
"""


def build_final_shortlist_artifact(
    artifact_root: Path,
    *,
    shortlist_id: str,
    p4_root: Path = DEFAULT_P4_ROOT,
    e1_root: Path = DEFAULT_E1_ROOT,
) -> Path:
    """Build one timestamped, versioned, non-overwriting presentation artifact."""
    if not FINAL_SHORTLIST_ID_PATTERN.fullmatch(shortlist_id):
        raise ValueError(
            "shortlist_id must be a UTC final-shortlist version identifier"
        )
    artifact_root = artifact_root.resolve()
    p4_root = p4_root.resolve()
    e1_root = e1_root.resolve()
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(
            f"final-shortlist target is not empty: {artifact_root}"
        )

    notebook = ROOT / "notebooks/production_screener.ipynb"
    if sha256_file(notebook) != EXPECTED_NOTEBOOK_SHA256:
        raise RuntimeError("historical production notebook hash mismatch")
    p4_manifest, p4_shortlist = _verify_p4_source(p4_root)
    e1_manifest, e1_names, e1_documents, e1_signoff = _verify_e1_source(
        e1_root
    )
    historical_lineage = _historical_code_lineage(
        p4_manifest,
        e1_manifest,
    )
    presentation, e1_derivative, traceability = build_presentation_tables(
        p4_shortlist,
        e1_names,
        e1_documents,
        original_e1_signoff_status=e1_signoff["signoff_status"],
    )

    artifact_root.mkdir(parents=True, exist_ok=True)
    outputs: list[tuple[Path, str]] = []

    def write_parquet(relative: str, frame: pd.DataFrame, role: str) -> None:
        path = artifact_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        outputs.append((path, role))

    write_parquet(
        "outputs/final_shortlist.parquet",
        presentation,
        "final_15_name_presentation_table",
    )
    write_parquet(
        "outputs/e1_presentation_derivative.parquet",
        e1_derivative,
        "machine_attested_e1_presentation_derivative",
    )
    write_parquet(
        "outputs/traceability.parquet",
        traceability,
        "machine_readable_p4_e1_citation_traceability",
    )

    source_copies = (
        (
            p4_root / "manifest.json",
            "source/p4_manifest.json",
            "exact_frozen_p4_manifest",
        ),
        (
            p4_root / "outputs/latest_shortlist.parquet",
            "source/p4_latest_shortlist.parquet",
            "exact_frozen_p4_latest_shortlist",
        ),
        (
            p4_root / "support/backtest_status.json",
            "source/p4_backtest_status.json",
            "exact_frozen_p4_performance_status",
        ),
        (
            e1_root / "manifest.json",
            "source/e1_adjudication_manifest.json",
            "exact_frozen_e1_adjudication_manifest",
        ),
        (
            e1_root / "outputs/live/name_level_adjudication.parquet",
            "source/e1_name_level_adjudication.parquet",
            "exact_frozen_e1_name_adjudication",
        ),
        (
            e1_root / "outputs/live/document_level_adjudication.parquet",
            "source/e1_document_level_adjudication.parquet",
            "exact_frozen_e1_document_adjudication",
        ),
        (
            e1_root / "signoff/human_signoff_contract.json",
            "source/e1_human_signoff_contract.json",
            "exact_frozen_e1_unsigned_signoff_contract",
        ),
        (
            e1_root / "contracts/deterministic_event_policy.json",
            "source/e1_deterministic_event_policy.json",
            "exact_frozen_e1_deterministic_policy",
        ),
    )
    for source, relative, role in source_copies:
        target = artifact_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        if (
            target.stat().st_size != source.stat().st_size
            or sha256_file(target) != sha256_file(source)
        ):
            raise RuntimeError(f"source copy mismatch: {source}")
        outputs.append((target, role))

    policy_path = artifact_root / "contracts/presentation_policy_v2.json"
    _write_json(policy_path, _presentation_policy())
    outputs.append((policy_path, "frozen_presentation_policy_v2"))
    comment_path = (
        artifact_root / "contracts/deterministic_comment_contract.json"
    )
    _write_json(comment_path, _comment_contract())
    outputs.append((comment_path, "frozen_deterministic_comment_contract"))

    historical_path = (
        artifact_root / "lineage/historical_source_code_lineage.json"
    )
    _write_json(historical_path, historical_lineage)
    outputs.append(
        (historical_path, "p4_e1_historical_source_code_lineage")
    )

    attestation = {
        "schema_version": 1,
        "attestation_type": (
            "hash_bound_machine_attested_unsigned_e1_presentation_derivative"
        ),
        "shortlist_id": shortlist_id,
        "created_at_utc": utc_now(),
        "source_e1_adjudication": {
            "artifact_root": _display_path(e1_root),
            "manifest_sha256": EXPECTED_E1_MANIFEST_SHA256,
            "manifest_records_rehashed": 70,
            "name_rows": 15,
            "document_rows": 47,
            "exact_claim_citations": 46,
        },
        "deterministic_actions": {"unresolved": 15},
        "presentation_label": LABEL_INCOMPLETE,
        "original_e1_signoff_status": ORIGINAL_E1_SIGNOFF_STATUS,
        "presentation_human_signoff_required": False,
        "presentation_signoff_status": PRESENTATION_SIGNOFF_STATUS,
        "human_signature_present": False,
        "signature_invented": False,
        "summary_may_change_deterministic_action": False,
        "performance_status": PERFORMANCE_STATUS,
        "performance_calculated": False,
        "external_request_made": False,
        "generative_llm_used": False,
    }
    attestation_path = artifact_root / "attestations/e1_derivative.json"
    _write_json(attestation_path, attestation)
    outputs.append(
        (attestation_path, "machine_attested_e1_derivative_attestation")
    )

    report_path = artifact_root / "report/final_shortlist_report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(_render_report(shortlist_id, presentation))
    outputs.append((report_path, "concise_human_readable_final_report"))

    current_code_lineage = []
    for relative in CURRENT_CODE_PATHS:
        path = ROOT / relative
        if not path.is_file():
            raise RuntimeError(f"current code-lineage path missing: {relative}")
        current_code_lineage.append(
            _source_record(path, "current_final_shortlist_code_or_test")
        )

    policy_records = {
        item["role"]: item
        for item in (
            _record(artifact_root, policy_path, "presentation_policy_v2"),
            _record(
                artifact_root,
                comment_path,
                "deterministic_comment_contract",
            ),
        )
    }
    records = [
        _record(artifact_root, path, role) for path, role in outputs
    ]
    manifest = {
        "schema_version": 2,
        "artifact_class": (
            "VERSIONED_CANONICAL_FINAL_SHORTLIST_PRESENTATION_V2"
        ),
        "shortlist_id": shortlist_id,
        "created_at_utc": utc_now(),
        "canonical_entrypoint": {
            "command": (
                "python3 -m workflows.build_final_shortlist "
                f"--shortlist-id {shortlist_id}"
            ),
            "artifact_root": _display_path(artifact_root),
            "final_shortlist": "outputs/final_shortlist.parquet",
            "e1_derivative": "outputs/e1_presentation_derivative.parquet",
            "traceability": "outputs/traceability.parquet",
            "report": "report/final_shortlist_report.md",
        },
        "source_p4": {
            "artifact_root": _display_path(p4_root),
            "manifest_sha256": EXPECTED_P4_MANIFEST_SHA256,
            "latest_shortlist_sha256": EXPECTED_P4_SHORTLIST_SHA256,
            "records_rehashed": 25,
            "validated_inputs_rehashed": 8,
            "records": p4_manifest["records"],
            "validated_inputs": p4_manifest["validated_inputs"],
        },
        "source_e1": {
            "artifact_root": _display_path(e1_root),
            "manifest_sha256": EXPECTED_E1_MANIFEST_SHA256,
            "records_rehashed": 70,
            "name_rows": 15,
            "document_rows": 47,
            "exact_claim_citations": 46,
            "records": e1_manifest["records"],
        },
        "current_code_lineage": current_code_lineage,
        "historical_source_code_lineage": historical_lineage,
        "preservation": e1_manifest["preservation"],
        "protected_unchanged_paths": [
            _source_record(
                notebook,
                "historical_inactive_notebook_outside_canonical_route",
            )
        ],
        "policy_lineage": policy_records,
        "records": records,
        "artifact_record_lineage": {
            "record_count": len(records),
            "all_regular_files_except_manifest_recorded": True,
            "records_rehashable": True,
        },
        "presentation": {
            "rows": 15,
            "recommendation_counts": {LABEL_INCOMPLETE: 15},
            "e1_action_counts": {"unresolved": 15},
            "exact_p4_rank_holding_weight_rows": 15,
            "summary_eligible_rows": 13,
            "summary_prohibited_rows": 2,
            "summary_prohibited_tickers": ["HPK", "SSTK"],
            "traceability_rows": 47,
            "exact_e1_citation_rows": 46,
            "presentation_human_signoff_required": False,
            "presentation_signoff_status": PRESENTATION_SIGNOFF_STATUS,
            "original_e1_signoff_status": ORIGINAL_E1_SIGNOFF_STATUS,
        },
        "claim": {
            "deterministic_policy_precedes_narrative": True,
            "narrative_may_change_presentation_label": False,
            "summary_may_change_presentation_label": False,
            "generative_llm_used": False,
            "external_request_made": False,
            "model_executed": False,
            "dataset_promoted": False,
            "performance_calculated": False,
            "official_performance_available": False,
            "human_signature_present": False,
            "human_signoff_required_for_presentation": False,
            "incomplete_evidence_treated_as_warning": False,
            "incomplete_evidence_treated_as_exclusion": False,
            "p4_artifact_records_corrupted": False,
            "e1_artifact_records_corrupted": False,
            "historical_p4_builder_code_evolution_preserved": True,
        },
        "limitations": list(LIMITATIONS),
        "research_only_disclaimer": RESEARCH_ONLY_DISCLAIMER,
    }
    manifest_path = artifact_root / "manifest.json"
    _write_json(manifest_path, manifest)

    # Reconfirm the frozen sources and protected notebook after materialization.
    if (
        sha256_file(p4_root / "manifest.json")
        != EXPECTED_P4_MANIFEST_SHA256
        or sha256_file(p4_root / "outputs/latest_shortlist.parquet")
        != EXPECTED_P4_SHORTLIST_SHA256
        or sha256_file(e1_root / "manifest.json")
        != EXPECTED_E1_MANIFEST_SHA256
        or sha256_file(notebook) != EXPECTED_NOTEBOOK_SHA256
    ):
        raise RuntimeError("a frozen source changed during materialization")
    verify_final_shortlist_artifact(artifact_root)
    return manifest_path


def verify_final_shortlist_artifact(
    artifact_root: Path,
) -> dict[str, Any]:
    """Rehash and reproduce the complete final-shortlist presentation."""
    artifact_root = artifact_root.resolve()
    manifest_path = artifact_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest["artifact_class"] != (
        "VERSIONED_CANONICAL_FINAL_SHORTLIST_PRESENTATION_V2"
    ):
        raise RuntimeError("unexpected final-shortlist artifact class")
    records = manifest["records"]
    _verify_record_list(
        artifact_root,
        records,
        repo_relative_paths=False,
    )
    recorded = {item["path"] for item in records}
    actual = {
        path.relative_to(artifact_root).as_posix()
        for path in artifact_root.rglob("*")
        if path.is_file() and path != manifest_path
    }
    if recorded != actual:
        raise RuntimeError("final-shortlist manifest record set is incomplete")

    if (
        sha256_file(artifact_root / "source/p4_manifest.json")
        != EXPECTED_P4_MANIFEST_SHA256
        or sha256_file(
            artifact_root / "source/p4_latest_shortlist.parquet"
        )
        != EXPECTED_P4_SHORTLIST_SHA256
        or sha256_file(
            artifact_root / "source/e1_adjudication_manifest.json"
        )
        != EXPECTED_E1_MANIFEST_SHA256
    ):
        raise RuntimeError("copied frozen source mismatch")

    presentation = pd.read_parquet(
        artifact_root / "outputs/final_shortlist.parquet"
    )
    derivative = pd.read_parquet(
        artifact_root / "outputs/e1_presentation_derivative.parquet"
    )
    traceability = pd.read_parquet(
        artifact_root / "outputs/traceability.parquet"
    )
    p4 = pd.read_parquet(
        artifact_root / "source/p4_latest_shortlist.parquet"
    )
    names = pd.read_parquet(
        artifact_root / "source/e1_name_level_adjudication.parquet"
    )
    documents = pd.read_parquet(
        artifact_root / "source/e1_document_level_adjudication.parquet"
    )
    rebuilt, rebuilt_derivative, rebuilt_traceability = (
        build_presentation_tables(
            p4,
            names,
            documents,
            original_e1_signoff_status=ORIGINAL_E1_SIGNOFF_STATUS,
        )
    )
    pd.testing.assert_frame_equal(presentation, rebuilt)
    pd.testing.assert_frame_equal(derivative, rebuilt_derivative)
    pd.testing.assert_frame_equal(traceability, rebuilt_traceability)

    historical = json.loads(
        (
            artifact_root / "lineage/historical_source_code_lineage.json"
        ).read_text()
    )
    expected_mismatch = [
        item
        for item in historical["current_comparison"]
        if not item["matches"]
    ]
    if (
        len(expected_mismatch) != 1
        or expected_mismatch[0]["historical"]["path"]
        != "portfolio/build_canonical_product.py"
        or expected_mismatch[0]["status"]
        != "expected_post_p4_c1_code_evolution"
    ):
        raise RuntimeError("historical/current P4 code lineage is incomplete")

    if (
        tuple(presentation.columns) != PRESENTATION_COLUMNS
        or tuple(derivative.columns) != E1_DERIVATIVE_COLUMNS
        or tuple(traceability.columns) != TRACEABILITY_COLUMNS
        or presentation["deterministic_recommendation"].ne(
            LABEL_INCOMPLETE
        ).any()
        or presentation["presentation_human_signoff_required"].any()
        or presentation["presentation_signoff_status"].ne(
            PRESENTATION_SIGNOFF_STATUS
        ).any()
        or presentation["original_e1_signoff_status"].ne(
            ORIGINAL_E1_SIGNOFF_STATUS
        ).any()
        or presentation["performance_metrics_available"].any()
        or derivative["human_signature_present"].any()
        or traceability["e1_claim_id"].notna().sum() != 46
        or not bool(
            presentation.set_index("ticker").loc["HPK", "summary_prohibited"]
        )
        or not bool(
            presentation.set_index("ticker").loc["SSTK", "summary_prohibited"]
        )
    ):
        raise RuntimeError("final-shortlist verification contract failed")
    return {
        "manifest_sha256": sha256_file(manifest_path),
        "records_verified": len(records),
        "presentation_rows": len(presentation),
        "e1_derivative_rows": len(derivative),
        "traceability_rows": len(traceability),
        "exact_e1_citations_reproduced": int(
            traceability["e1_claim_id"].notna().sum()
        ),
        "summary_eligible_rows": int(
            presentation["e1_summary_allowed"].sum()
        ),
        "summary_prohibited_rows": int(
            presentation["summary_prohibited"].sum()
        ),
        "presentation_human_signoff_required": False,
        "performance_status": PERFORMANCE_STATUS,
        "performance_calculated": False,
    }
