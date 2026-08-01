"""Offline, claim-cited extraction of retrieved E1 primary documents.

This module never performs network I/O.  It revalidates the frozen collection
before reading evidence, copies the exact preserved response bytes into a new
non-overwriting artifact, and keeps document facts separate from deterministic
event decisions.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import re
import shutil
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from portfolio.event_review import (
    DETERMINISTIC_EVENT_POLICY,
    REVIEW_ID_PATTERN,
    ROOT,
    _display_path,
    _record,
    _timestamp,
    _validate_preservation,
    _write_json,
    deterministic_event_decision,
    sha256_file,
    utc_now,
    verify_event_review_artifact,
    verify_live_event_collection_artifact,
)


DEFAULT_COLLECTION_ROOT = (
    ROOT
    / "artifacts/event_review/us/"
    "20260730T150604Z-e1-collection-final"
)
EXPECTED_COLLECTION_MANIFEST_SHA256 = (
    "ad14c45402c95e2b652ac7e0f8b98707a44eb4279f3988be602bca5ec77208ee"
)
EXPECTED_PARENT_E1_MANIFEST_SHA256 = (
    "e44ff9aa9d2ac3be2ae66e5fb006bba435cdda3eef4c4521b4d1e3362a58caa6"
)
EXPECTED_PLAN_SHA256 = (
    "ff856cff60eb35279c4f487bff5aaac679c83f22c8518d07d3c66daebd7ea433"
)
EXPECTED_CONTRACT_SHA256 = (
    "4fae9e662079ca479894db93ef67746cae77a1200abe2ebd06244e5e78a38cdf"
)

DOCUMENT_COLUMNS = (
    "request_id",
    "source_id",
    "rank",
    "ticker",
    "name",
    "sec_cik",
    "request_url",
    "accession_number",
    "form",
    "items",
    "filing_date",
    "source_publication_time",
    "source_retrieved_at",
    "http_status",
    "response_sha256",
    "evidence_path",
    "extraction_status",
    "claim_count",
    "summary_eligible_document",
    "remaining_ambiguity",
)

CLAIM_COLUMNS = (
    "claim_id",
    "request_id",
    "source_id",
    "ticker",
    "request_url",
    "accession_number",
    "form",
    "source_publication_time",
    "source_retrieved_at",
    "source_response_sha256",
    "evidence_path",
    "document_locator",
    "locator_normalization",
    "supporting_passage",
    "supporting_passage_sha256",
    "claim_text",
    "claim_text_mode",
)

RECONCILIATION_COLUMNS = (
    "request_id",
    "source_id",
    "ticker",
    "accession_number",
    "form",
    "claim_id",
    "exact_security_scope_status",
    "exact_security_scope",
    "event_type",
    "event_status",
    "announcement_publication_time",
    "announcement_publication_time_status",
    "effective_time",
    "effective_time_status",
    "transaction_or_action_terms",
    "terms_status",
    "remaining_ambiguity",
    "deterministic_action",
    "decision_eligibility_status",
    "policy_reason_code",
    "human_review_required",
    "summary_may_change_deterministic_action",
    "unsupported_inferences_applied",
)

LIVE_CONTRACT_COLUMNS = (
    "requirement_id",
    "stable_row_id",
    "rank",
    "ticker",
    "name",
    "sec_cik",
    "decision_timestamp",
    "live_review_as_of",
    "retrieved_primary_document_count",
    "successful_document_claim_count",
    "document_set_status",
    "deterministic_action",
    "review_status",
    "human_review_required",
    "summary_allowed",
    "summary_status",
    "summary_may_change_deterministic_action",
    "cited_claim_ids",
    "remaining_ambiguity",
)

HUMAN_REVIEW_COLUMNS = (
    "review_queue_id",
    "rank",
    "ticker",
    "name",
    "sec_cik",
    "priority",
    "queue_status",
    "document_set_status",
    "deterministic_action",
    "cited_claim_ids",
    "cited_source_ids",
    "review_questions",
    "summary_allowed",
    "summary_may_change_deterministic_action",
)


@dataclass(frozen=True)
class ExtractionSpec:
    anchor: str
    event_type: str
    event_status: str
    scope_status: str
    scope_text: str
    ambiguity: str


ISSUER_SCOPE = "issuer_or_subsidiary_activity_not_exact_shortlist_security"
ISSUER_SCOPE_TEXT = (
    "The passage identifies issuer or subsidiary activity but does not state "
    "that the shortlisted common-stock security is acquired, cancelled, "
    "suspended, delisted, or otherwise terminated."
)
DIRECT_SCOPE = "exact_security_class_named_but_selection_effect_unresolved"
DIRECT_SCOPE_TEXT = (
    "The passage expressly names the issuer's common stock or depositary-share "
    "structure, but the selection-changing effect and complete effective-time "
    "contract remain unresolved."
)
SURVIVOR_SCOPE = "exact_security_role_is_surviving_or_consideration_security"
SURVIVOR_SCOPE_TEXT = (
    "The passage names the shortlisted issuer's common stock as surviving or "
    "consideration stock; it does not state that this shortlisted security "
    "ceased to be independently eligible."
)
COMMON_AMBIGUITY = (
    "Complete exact-security effect, full action terms, and an exact effective "
    "timestamp are not all established by the cited passage."
)


def _spec(
    anchor: str,
    event_type: str,
    event_status: str,
    *,
    scope_status: str = ISSUER_SCOPE,
    scope_text: str = ISSUER_SCOPE_TEXT,
    ambiguity: str = COMMON_AMBIGUITY,
) -> ExtractionSpec:
    return ExtractionSpec(
        anchor=anchor,
        event_type=event_type,
        event_status=event_status,
        scope_status=scope_status,
        scope_text=scope_text,
        ambiguity=ambiguity,
    )


# Human-curated anchors are exact strings from the retrieved primary documents.
# The mapping is accession-bound so filing family or ticker metadata cannot
# create a claim.
EXTRACTION_SPECS: dict[str, ExtractionSpec] = {
    "0001104659-25-105300": _spec(
        "Under the Purchase Agreement, OpCo acquired",
        "asset_acquisition",
        "completed",
    ),
    "0001104659-25-084862": _spec(
        "On August 27, 2025, BlueLinx Holdings Inc.",
        "financing_agreement",
        "entered",
    ),
    "0000893538-26-000055": _spec(
        "The South Texas Divestiture was completed",
        "asset_disposition",
        "completed",
    ),
    "0001104659-26-025261": _spec(
        "On March 9, 2026, the Company entered into the Indenture",
        "debt_financing",
        "entered",
    ),
    "0001104659-26-017062": _spec(
        "On February 17, 2026, SM Energy Company",
        "asset_disposition",
        "announced_pending",
    ),
    "0001104659-26-008380": _spec(
        "As discussed in the Introduction, on January 30, 2026, the Mergers "
        "were consummated",
        "issuer_surviving_completed_merger",
        "completed",
        scope_status=SURVIVOR_SCOPE,
        scope_text=SURVIVOR_SCOPE_TEXT,
    ),
    "0001104659-25-123526": _spec(
        "On November 2, 2025, SM Energy Company",
        "issuer_proposed_merger",
        "announced_pending",
        scope_status=SURVIVOR_SCOPE,
        scope_text=SURVIVOR_SCOPE_TEXT,
    ),
    "0001104659-25-122082": _spec(
        "On November 2, 2025, SM Energy Company",
        "issuer_proposed_merger",
        "announced_pending",
        scope_status=SURVIVOR_SCOPE,
        scope_text=SURVIVOR_SCOPE_TEXT,
    ),
    "0001104659-25-118705": _spec(
        "On November 2, 2025, SM Energy Company",
        "issuer_proposed_merger",
        "announced_pending",
        scope_status=SURVIVOR_SCOPE,
        scope_text=SURVIVOR_SCOPE_TEXT,
    ),
    "0000893538-25-000144": _spec(
        "On November 2, 2025, SM Energy Company",
        "issuer_proposed_merger",
        "announced_pending",
        scope_status=SURVIVOR_SCOPE,
        scope_text=SURVIVOR_SCOPE_TEXT,
    ),
    "0000893538-25-000126": _spec(
        "On October 13, 2025, SM Energy Company",
        "financing_agreement",
        "entered",
    ),
    "0001437749-26-005335": _spec(
        "Upon the terms and subject to the conditions set forth in the "
        "Purchase Agreement, the Company purchased",
        "asset_acquisition",
        "completed",
    ),
    "0001628280-25-042486": _spec(
        "On September 18, 2025 (the “Amendment Effective Date”)",
        "financing_agreement",
        "entered",
    ),
    "0001193125-25-283199": _spec(
        "On November 14, 2025, AutoNation, Inc.",
        "debt_financing",
        "completed",
    ),
    "0001193125-25-273371": _spec(
        "We are offering",
        "debt_offering",
        "terms_incomplete_in_retrieved_text",
    ),
    "0001437749-26-022144": _spec(
        "On June 30, 2026, HighPeak Energy, Inc.",
        "financing_agreement",
        "entered",
    ),
    "0001437749-26-015209": _spec(
        "On May 6, 2026, HighPeak Energy, Inc.",
        "common_stock_at_the_market_offering",
        "authorized",
        scope_status=DIRECT_SCOPE,
        scope_text=DIRECT_SCOPE_TEXT,
    ),
    "0001437749-25-029107": _spec(
        "In connection with Jack Hightower’s retirement",
        "principal_stockholder_governance_change",
        "effective_date_stated",
        scope_status=DIRECT_SCOPE,
        scope_text=DIRECT_SCOPE_TEXT,
    ),
    "0001437749-25-024452": _spec(
        "On August 1, 2025, HighPeak Energy, Inc.",
        "financing_agreement",
        "entered",
    ),
    "0001193125-26-251470": _spec(
        "On June 1, 2026 (the “Closing Date”)",
        "asset_acquisition_and_common_stock_issuance",
        "completed",
        scope_status=DIRECT_SCOPE,
        scope_text=DIRECT_SCOPE_TEXT,
    ),
    "0001193125-26-237800": _spec(
        "On May 22, 2026, Northern Oil and Gas, Inc.",
        "asset_acquisition_and_common_stock_issuance",
        "announced_pending",
        scope_status=DIRECT_SCOPE,
        scope_text=DIRECT_SCOPE_TEXT,
    ),
    "0001193125-26-064523": _spec(
        "On February 23, 2026, Northern issued a press release announcing "
        "the closing",
        "asset_acquisition",
        "completed",
    ),
    "0001193125-25-310561": _spec(
        "On December 5, 2025, Northern Oil and Gas, Inc.",
        "asset_acquisition",
        "announced_pending",
    ),
    "0001104485-25-000161": _spec(
        "On November 5, 2025, Northern Oil and Gas, Inc.",
        "financing_agreement",
        "entered",
    ),
    "0001193125-25-226774": _spec(
        "On October 1, 2025, Northern Oil and Gas, Inc.",
        "debt_financing",
        "completed",
    ),
    "0001193125-26-018663": _spec(
        "We previously announced our intention to pursue a corporate "
        "redomiciliation from France to Luxembourg",
        "corporate_redomiciliation",
        "shareholder_vote_pending",
        scope_status=DIRECT_SCOPE,
        scope_text=DIRECT_SCOPE_TEXT,
    ),
    "0001193125-25-262679": _spec(
        "We previously announced our intention to pursue a corporate "
        "redomiciliation from France to Luxembourg",
        "corporate_redomiciliation",
        "shareholder_vote_pending",
        scope_status=DIRECT_SCOPE,
        scope_text=DIRECT_SCOPE_TEXT,
    ),
    "0001193125-26-275945": _spec(
        "On June 16, 2026, Universal Insurance Holdings, Inc.",
        "debt_financing",
        "completed",
    ),
    "0001627475-26-000039": _spec(
        "On June 23, 2026 , Upwork Inc.",
        "financing_agreement",
        "entered",
    ),
    "0001627475-25-000048": _spec(
        "This Current Report on Form 8-K/A amends the Original Form 8-K solely",
        "earnings_guidance_correction",
        "filed",
    ),
    "0001104659-25-125672": _spec(
        "On December 31, 2025, Amplify Energy Operating LLC",
        "financing_agreement",
        "entered",
    ),
    "0001104659-25-124890": _spec(
        "The Asset Sale was completed on December 29, 2025",
        "asset_disposition",
        "completed",
    ),
    "0001104659-25-124255": _spec(
        "The EQV Asset Sale was completed on December 23, 2025",
        "asset_disposition",
        "completed",
    ),
    "0001104659-25-106982": _spec(
        "On November 4, 2025, Amplify Oklahoma Operating LLC",
        "asset_disposition",
        "announced_pending",
    ),
    "0001104659-25-103773": _spec(
        "On October 28, 2025, Amplify Energy Operating LLC",
        "asset_disposition",
        "announced_pending",
    ),
    "0001104659-26-086469": _spec(
        "On July 22, 2026, wholly-owned subsidiaries of Matador Resources "
        "Company",
        "asset_acquisition",
        "announced_pending",
    ),
    "0001520006-26-000029": _spec(
        "On June 10, 2026, MRC Energy Company",
        "financing_agreement",
        "entered",
    ),
    "0001104659-26-024110": _spec(
        "On March 5, 2026, the Company received net proceeds",
        "debt_financing",
        "completed",
    ),
    "0001104659-26-020877": _spec(
        "On February 26, 2026, Matador Resources Company",
        "debt_offering",
        "announced_pending",
    ),
    "0001104659-25-120120": _spec(
        "On December 9, 2025, MRC Energy Company",
        "financing_agreement",
        "entered",
    ),
    "0001367644-26-000068": _spec(
        "On June 26, 2026 , Emergent BioSolutions Inc.",
        "government_contract_modification",
        "received",
    ),
    "0001367644-26-000038": _spec(
        "On April 16, 2026, Emergent BioSolutions Inc.",
        "financing_agreement",
        "entered",
    ),
    "0001367644-25-000188": _spec(
        "On September 5, 2025, Emergent BioSolutions Inc.",
        "government_contract_modification",
        "received",
    ),
    "0001628280-26-032163": _spec(
        "On May 5, 2026, Ingram Micro Holding Company",
        "common_stock_secondary_offering_and_repurchase",
        "announced",
        scope_status=DIRECT_SCOPE,
        scope_text=DIRECT_SCOPE_TEXT,
    ),
    "0001628280-26-016059": _spec(
        "On March 5, 2026, Ingram Micro Holding Company",
        "common_stock_secondary_offering_and_repurchase",
        "completed",
        scope_status=DIRECT_SCOPE,
        scope_text=DIRECT_SCOPE_TEXT,
    ),
    "0001628280-26-015358": _spec(
        "The selling stockholder identified in this prospectus supplement",
        "common_stock_secondary_offering_and_repurchase",
        "preliminary",
        scope_status=DIRECT_SCOPE,
        scope_text=DIRECT_SCOPE_TEXT,
    ),
}


def _normalized_document_text(payload: bytes) -> str:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(payload, "lxml")
    return " ".join(soup.get_text(" ", strip=True).split())


def _extract_bounded_passage(text: str, anchor: str) -> tuple[str, int, int]:
    start = text.find(anchor)
    if start < 0:
        raise RuntimeError(f"curated evidence anchor not found: {anchor}")
    if text.find(anchor, start + 1) >= 0:
        # Repeated disclosure is acceptable only because the accession-bound
        # locator pins the first exact occurrence deterministically.
        occurrence = "first"
    else:
        occurrence = "only"
    minimum_end = min(len(text), start + max(len(anchor), 240))
    maximum_end = min(len(text), start + 1_400)
    boundary = re.search(
        r"[.!?](?=\s+(?:[A-Z“]))",
        text[minimum_end:maximum_end],
    )
    end = (
        minimum_end + boundary.end()
        if boundary is not None
        else maximum_end
    )
    passage = text[start:end].strip()
    if not passage or not passage.startswith(anchor):
        raise RuntimeError("exact passage extraction failed")
    # Keep occurrence visible in the locator contract without changing text.
    _ = occurrence
    return passage, start, end


def _hash_id(prefix: str, value: str) -> str:
    return f"{prefix}:{hashlib.sha256(value.encode()).hexdigest()[:24]}"


def _canonical_policy_fields(spec: ExtractionSpec) -> tuple[str, str]:
    if spec.event_type == "issuer_proposed_merger":
        return "merger", "announced_pending"
    if spec.event_type == "issuer_surviving_completed_merger":
        # The shortlisted security is the surviving/consideration security,
        # so do not pass it to the completed-merger exclusion rule.
        return "other_material_event", "confirmed"
    return "other_material_event", "confirmed"


def _verify_collection_directly(
    collection_root: Path,
) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any]]:
    manifest_path = collection_root / "manifest.json"
    if sha256_file(manifest_path) != EXPECTED_COLLECTION_MANIFEST_SHA256:
        raise RuntimeError("collection manifest hash mismatch")
    manifest = json.loads(manifest_path.read_text())
    for item in manifest["records"]:
        path = collection_root / item["path"]
        if (
            not path.is_file()
            or path.stat().st_size != int(item["size_bytes"])
            or sha256_file(path) != item["sha256"]
        ):
            raise RuntimeError(f"collection record mismatch: {item['path']}")

    source_manifest = collection_root / "source/frozen_e1_manifest.json"
    source_plan = collection_root / "source/collection_request_plan.parquet"
    source_contract = collection_root / "source/collection_contract.json"
    if sha256_file(source_manifest) != EXPECTED_PARENT_E1_MANIFEST_SHA256:
        raise RuntimeError("frozen parent E1 manifest hash mismatch")
    if sha256_file(source_plan) != EXPECTED_PLAN_SHA256:
        raise RuntimeError("frozen request-plan hash mismatch")
    if sha256_file(source_contract) != EXPECTED_CONTRACT_SHA256:
        raise RuntimeError("frozen collection-contract hash mismatch")

    plan = pd.read_parquet(source_plan)
    request_manifest = json.loads(
        (collection_root / "request_manifest.json").read_text()
    )
    records = request_manifest["records"]
    if len(plan) != 47 or len(records) != 47:
        raise RuntimeError("frozen 47-request boundary mismatch")
    if list(plan["request_url"].astype(str)) != [
        str(item["request_url"]) for item in records
    ]:
        raise RuntimeError("request URL/order drift")
    total = 0
    statuses: dict[int, int] = {}
    for item in records:
        path = collection_root / item["stored_path"]
        if (
            path.stat().st_size != int(item["stored_size_bytes"])
            or sha256_file(path) != item["stored_sha256"]
        ):
            raise RuntimeError(f"stored response mismatch: {item['request_id']}")
        with gzip.open(path, "rb") as handle:
            payload = handle.read()
        if (
            len(payload) != int(item["response_size_bytes"])
            or hashlib.sha256(payload).hexdigest()
            != item["response_sha256"]
        ):
            raise RuntimeError(f"response-byte mismatch: {item['request_id']}")
        total += len(payload)
        status = int(item["http_status"])
        statuses[status] = statuses.get(status, 0) + 1
    if total != 19_120_821 or statuses != {200: 46, 503: 1}:
        raise RuntimeError("collection byte/status boundary mismatch")

    preservation = _validate_preservation(
        ROOT
        / "artifacts/canonical_refresh/us/20260730T110301Z/"
        "review/review_manifest.json"
    )
    if preservation != manifest["preservation"]:
        raise RuntimeError("preservation lineage mismatch")
    return manifest, plan, request_manifest


def _copy_exact_response(
    collection_root: Path,
    artifact_root: Path,
    response: dict[str, Any],
) -> str:
    source = collection_root / response["stored_path"]
    relative = Path("raw/sec/filings") / Path(response["stored_path"]).relative_to(
        "raw/sec/filings"
    )
    target = artifact_root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    if (
        target.stat().st_size != int(response["stored_size_bytes"])
        or sha256_file(target) != response["stored_sha256"]
    ):
        raise RuntimeError(f"copied response mismatch: {response['request_id']}")
    return relative.as_posix()


def _render_report(
    documents: pd.DataFrame,
    claims: pd.DataFrame,
    reconciliation: pd.DataFrame,
    live: pd.DataFrame,
) -> str:
    lines = [
        "# E1 Retrieved Primary-Document Extraction and Reconciliation",
        "",
        "## Outcome",
        "",
        (
            "All 47 preserved responses were reverified before extraction: "
            "46 HTTP 200 SEC primary documents and one HPK HTTP 503 response. "
            f"The {len(claims)} extracted document claims are exact quotations "
            "with normalized-text character locators and claim-level source "
            "lineage. No external request or retry occurred."
        ),
        "",
        (
            "The frozen deterministic rules were applied before this report. "
            f"All {len(reconciliation)} document rows remain unresolved and "
            f"all {int(live['human_review_required'].sum())} shortlist names "
            "remain in human review. A cited summary may describe retrieved "
            "evidence only; it cannot change a deterministic action."
        ),
        "",
        "## Live 15-name review",
        "",
    ]
    for row in live.sort_values("rank").itertuples(index=False):
        ticker_claims = claims[claims["ticker"].eq(row.ticker)]
        if ticker_claims.empty:
            citation = "no eligible retrieved claim"
        else:
            first = ticker_claims.iloc[0]
            citation = (
                f"{first['claim_id']}; {first['source_id']}; "
                f"{first['document_locator']}; SHA-256 "
                f"{first['source_response_sha256']}; "
                f"`{first['evidence_path']}`"
            )
        lines.append(
            f"- {row.ticker}: {row.document_set_status}; action "
            f"`{row.deterministic_action}`; summary "
            f"{'allowed with claim citations' if row.summary_allowed else 'prohibited'}. "
            f"Citation: {citation}."
        )
    lines.extend(
        [
            "",
            "## Human-review boundary",
            "",
            (
                "Reviewers must resolve exact security scope, recognized event "
                "type/status, complete terms, and exact effective time from the "
                "cited passages before any deterministic action can change. "
                "Filing family, missing price, ticker disappearance, name "
                "similarity, and uncited narrative remain prohibited inputs."
            ),
            "",
            (
                "HPK is summary-prohibited because its Form 25-NSE response is "
                "HTTP 503 and unresolved. SSTK is summary-prohibited because "
                "the frozen plan contained no primary-document candidate."
            ),
            "",
            "No dataset was promoted, no P3/P4 consumer changed, and no "
            "performance was calculated.",
            "",
        ]
    )
    return "\n".join(lines)


def build_retrieved_event_review(
    artifact_root: Path,
    *,
    review_id: str,
    collection_root: Path = DEFAULT_COLLECTION_ROOT,
) -> Path:
    """Build one offline, non-overwriting, human-review-ready E1 artifact."""
    if not REVIEW_ID_PATTERN.fullmatch(review_id):
        raise ValueError("review_id must be an immutable UTC identifier")
    artifact_root = artifact_root.resolve()
    collection_root = collection_root.resolve()
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"E1 extraction target is not empty: {artifact_root}")
    artifact_root.mkdir(parents=True, exist_ok=True)

    # Two independent pre-processing checks: the established verifier and a
    # direct byte/hash/lineage audit implemented above.
    collection_verification = verify_live_event_collection_artifact(
        collection_root
    )
    collection_manifest, plan, request_manifest = _verify_collection_directly(
        collection_root
    )
    parent_root = Path(collection_manifest["source_e1"]["artifact_root"])
    if not parent_root.is_absolute():
        parent_root = ROOT / parent_root
    parent_verification = verify_event_review_artifact(parent_root)

    responses = {
        str(item["request_id"]): item
        for item in request_manifest["records"]
    }
    document_rows: list[dict[str, Any]] = []
    claim_rows: list[dict[str, Any]] = []
    reconciliation_rows: list[dict[str, Any]] = []
    raw_paths: list[Path] = []

    evaluation = max(
        _timestamp(item["retrieved_at_utc"])
        for item in request_manifest["records"]
        if _timestamp(item["retrieved_at_utc"]) is not None
    )
    for item in plan.itertuples(index=False):
        response = responses[str(item.request_id)]
        evidence_path = _copy_exact_response(
            collection_root,
            artifact_root,
            response,
        )
        raw_paths.append(artifact_root / evidence_path)
        source_id = f"sec_primary_document:{item.request_id}"
        success = (
            int(response["http_status"]) == 200
            and response["payload_validation_status"]
            == "expected_text_or_xml"
        )
        if not success:
            document_rows.append(
                {
                    "request_id": item.request_id,
                    "source_id": source_id,
                    "rank": int(item.rank),
                    "ticker": item.ticker,
                    "name": item.name,
                    "sec_cik": item.sec_cik,
                    "request_url": item.request_url,
                    "accession_number": item.accession_number,
                    "form": item.form,
                    "items": item.items,
                    "filing_date": item.filing_date,
                    "source_publication_time": item.source_publication_time,
                    "source_retrieved_at": response["retrieved_at_utc"],
                    "http_status": int(response["http_status"]),
                    "response_sha256": response["response_sha256"],
                    "evidence_path": evidence_path,
                    "extraction_status": (
                        "unresolved_http_503_no_claim_summary_prohibited"
                    ),
                    "claim_count": 0,
                    "summary_eligible_document": False,
                    "remaining_ambiguity": (
                        "HPK Form 25-NSE primary document was not retrieved; "
                        "the preserved response is HTTP 503 and must not be "
                        "retried or summarized."
                    ),
                }
            )
            reconciliation_rows.append(
                {
                    "request_id": item.request_id,
                    "source_id": source_id,
                    "ticker": item.ticker,
                    "accession_number": item.accession_number,
                    "form": item.form,
                    "claim_id": None,
                    "exact_security_scope_status": "unavailable_http_503",
                    "exact_security_scope": None,
                    "event_type": "unresolved",
                    "event_status": "unresolved",
                    "announcement_publication_time": (
                        item.source_publication_time
                    ),
                    "announcement_publication_time_status": (
                        "source_lineage_only_no_document_claim"
                    ),
                    "effective_time": None,
                    "effective_time_status": "unavailable_http_503",
                    "transaction_or_action_terms": None,
                    "terms_status": "unavailable_http_503",
                    "remaining_ambiguity": (
                        "No claim may be extracted from an HTTP 503 response."
                    ),
                    "deterministic_action": "unresolved",
                    "decision_eligibility_status": (
                        "missing_retrieved_primary_document"
                    ),
                    "policy_reason_code": (
                        "missing_retrieved_primary_document"
                    ),
                    "human_review_required": True,
                    "summary_may_change_deterministic_action": False,
                    "unsupported_inferences_applied": False,
                }
            )
            continue

        spec = EXTRACTION_SPECS.get(str(item.accession_number))
        if spec is None:
            raise RuntimeError(
                f"no human-curated extraction spec: {item.accession_number}"
            )
        with gzip.open(artifact_root / evidence_path, "rb") as handle:
            payload = handle.read()
        text = _normalized_document_text(payload)
        passage, start, end = _extract_bounded_passage(text, spec.anchor)
        passage_sha = hashlib.sha256(passage.encode()).hexdigest()
        locator = (
            f"normalized_text_chars:{start}-{end};"
            f"passage_sha256:{passage_sha}"
        )
        claim_id = _hash_id(
            "claim",
            f"{source_id}\x1f{locator}\x1f{passage}",
        )
        claim_rows.append(
            {
                "claim_id": claim_id,
                "request_id": item.request_id,
                "source_id": source_id,
                "ticker": item.ticker,
                "request_url": item.request_url,
                "accession_number": item.accession_number,
                "form": item.form,
                "source_publication_time": item.source_publication_time,
                "source_retrieved_at": response["retrieved_at_utc"],
                "source_response_sha256": response["response_sha256"],
                "evidence_path": evidence_path,
                "document_locator": locator,
                "locator_normalization": (
                    "BeautifulSoup(lxml).get_text(' ',strip=True), then "
                    "collapse all whitespace runs to one ASCII space"
                ),
                "supporting_passage": passage,
                "supporting_passage_sha256": passage_sha,
                "claim_text": passage,
                "claim_text_mode": "exact_primary_document_quote",
            }
        )

        policy_type, policy_status = _canonical_policy_fields(spec)
        event = {
            "event_type": policy_type,
            "event_status": policy_status,
            "effective_at": None,
            "effective_time_status": (
                "document_date_or_status_language_only_not_exact_timestamp"
            ),
            "source_published_at": item.source_publication_time,
            "source_retrieved_at": response["retrieved_at_utc"],
            "source_id": source_id,
            "source_response_sha256": response["response_sha256"],
            "ambiguity": spec.ambiguity,
        }
        decision = deterministic_event_decision(
            event,
            as_of_timestamp=evaluation,
            evaluation_retrieved_at=evaluation,
        )
        if decision["action"] != "unresolved":
            raise RuntimeError("incomplete extracted evidence changed action")
        document_rows.append(
            {
                "request_id": item.request_id,
                "source_id": source_id,
                "rank": int(item.rank),
                "ticker": item.ticker,
                "name": item.name,
                "sec_cik": item.sec_cik,
                "request_url": item.request_url,
                "accession_number": item.accession_number,
                "form": item.form,
                "items": item.items,
                "filing_date": item.filing_date,
                "source_publication_time": item.source_publication_time,
                "source_retrieved_at": response["retrieved_at_utc"],
                "http_status": int(response["http_status"]),
                "response_sha256": response["response_sha256"],
                "evidence_path": evidence_path,
                "extraction_status": "exact_claim_extracted_human_review",
                "claim_count": 1,
                "summary_eligible_document": True,
                "remaining_ambiguity": spec.ambiguity,
            }
        )
        reconciliation_rows.append(
            {
                "request_id": item.request_id,
                "source_id": source_id,
                "ticker": item.ticker,
                "accession_number": item.accession_number,
                "form": item.form,
                "claim_id": claim_id,
                "exact_security_scope_status": spec.scope_status,
                "exact_security_scope": spec.scope_text,
                "event_type": spec.event_type,
                "event_status": spec.event_status,
                "announcement_publication_time": (
                    item.source_publication_time
                ),
                "announcement_publication_time_status": (
                    "SEC_acceptance_time_from_frozen_source_lineage"
                ),
                "effective_time": None,
                "effective_time_status": (
                    "document_date_or_status_language_only_not_exact_timestamp"
                ),
                "transaction_or_action_terms": passage,
                "terms_status": (
                    "narrow_exact_passage_only_full_terms_not_asserted"
                ),
                "remaining_ambiguity": spec.ambiguity,
                "deterministic_action": decision["action"],
                "decision_eligibility_status": decision[
                    "eligibility_status"
                ],
                "policy_reason_code": decision["reason_code"],
                "human_review_required": True,
                "summary_may_change_deterministic_action": False,
                "unsupported_inferences_applied": False,
            }
        )

    documents = pd.DataFrame(document_rows, columns=DOCUMENT_COLUMNS)
    claims = pd.DataFrame(claim_rows, columns=CLAIM_COLUMNS)
    reconciliation = pd.DataFrame(
        reconciliation_rows,
        columns=RECONCILIATION_COLUMNS,
    )
    if (
        len(documents) != 47
        or len(claims) != 46
        or len(reconciliation) != 47
        or documents["request_id"].nunique() != 47
        or claims["source_response_sha256"].str.len().ne(64).any()
        or reconciliation["deterministic_action"].ne("unresolved").any()
        or not reconciliation["human_review_required"].all()
        or reconciliation["unsupported_inferences_applied"].any()
    ):
        raise RuntimeError("document extraction/reconciliation boundary failed")

    source_live = pd.read_parquet(
        collection_root / "outputs/live/review_contract.parquet"
    )
    plan_counts = plan.groupby("ticker").size().to_dict()
    success_counts = (
        documents[documents["http_status"].eq(200)]
        .groupby("ticker")
        .size()
        .to_dict()
    )
    claim_ids = claims.groupby("ticker")["claim_id"].apply(list).to_dict()
    live_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []
    for row in source_live.sort_values("rank").itertuples(index=False):
        expected = int(plan_counts.get(row.ticker, 0))
        retrieved = int(success_counts.get(row.ticker, 0))
        ids = sorted(claim_ids.get(row.ticker, []))
        if expected == 0:
            document_set_status = "no_primary_document_candidate"
        elif retrieved != expected:
            document_set_status = "incomplete_retrieval_http_503"
        else:
            document_set_status = "complete_retrieved_document_set"
        summary_allowed = expected > 0 and retrieved == expected
        ambiguity = (
            "No primary-document candidate in the frozen plan."
            if expected == 0
            else (
                "HPK Form 25-NSE remains unresolved after HTTP 503."
                if retrieved != expected
                else (
                    "Retrieved claims remain deterministic-action incomplete; "
                    "a summary may describe cited evidence only."
                )
            )
        )
        live_rows.append(
            {
                "requirement_id": row.requirement_id,
                "stable_row_id": row.stable_row_id,
                "rank": int(row.rank),
                "ticker": row.ticker,
                "name": row.name,
                "sec_cik": row.sec_cik,
                "decision_timestamp": row.decision_timestamp,
                "live_review_as_of": row.live_review_as_of,
                "retrieved_primary_document_count": retrieved,
                "successful_document_claim_count": len(ids),
                "document_set_status": document_set_status,
                "deterministic_action": "unresolved",
                "review_status": "unresolved_human_review_required",
                "human_review_required": True,
                "summary_allowed": summary_allowed,
                "summary_status": (
                    "allowed_only_from_retrieved_claim_level_citations"
                    if summary_allowed
                    else "prohibited_incomplete_or_absent_retrieved_evidence"
                ),
                "summary_may_change_deterministic_action": False,
                "cited_claim_ids": json.dumps(ids),
                "remaining_ambiguity": ambiguity,
            }
        )
        sources = sorted(
            claims[claims["ticker"].eq(row.ticker)]["source_id"].tolist()
        )
        review_rows.append(
            {
                "review_queue_id": _hash_id(
                    "human_review",
                    f"{review_id}\x1f{row.ticker}",
                ),
                "rank": int(row.rank),
                "ticker": row.ticker,
                "name": row.name,
                "sec_cik": row.sec_cik,
                "priority": (
                    "critical_incomplete_retrieval"
                    if row.ticker == "HPK"
                    else (
                        "high_no_primary_candidate"
                        if row.ticker == "SSTK"
                        else "standard_claim_reconciliation"
                    )
                ),
                "queue_status": "open_human_review_required",
                "document_set_status": document_set_status,
                "deterministic_action": "unresolved",
                "cited_claim_ids": json.dumps(ids),
                "cited_source_ids": json.dumps(sources),
                "review_questions": json.dumps(
                    [
                        "What exact security is affected?",
                        "What recognized event type and status are proven?",
                        "What is the exact effective timestamp?",
                        "Are the transaction/action terms complete?",
                        "Does any conflict or ambiguity remain?",
                    ]
                ),
                "summary_allowed": summary_allowed,
                "summary_may_change_deterministic_action": False,
            }
        )
    live = pd.DataFrame(live_rows, columns=LIVE_CONTRACT_COLUMNS)
    human_review = pd.DataFrame(
        review_rows,
        columns=HUMAN_REVIEW_COLUMNS,
    )
    if (
        len(live) != 15
        or len(human_review) != 15
        or not live["human_review_required"].all()
        or live["summary_allowed"].sum() != 13
        or live.set_index("ticker").loc["HPK", "summary_allowed"]
        or live.set_index("ticker").loc["SSTK", "summary_allowed"]
        or live["summary_may_change_deterministic_action"].any()
    ):
        raise RuntimeError("updated 15-name live contract boundary failed")

    outputs: list[tuple[Path, str]] = []

    def write_parquet(relative: str, frame: pd.DataFrame, role: str) -> None:
        path = artifact_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        outputs.append((path, role))

    write_parquet(
        "outputs/live/document_extracted_claims.parquet",
        claims,
        "claim_level_exact_primary_document_extractions",
    )
    write_parquet(
        "outputs/live/document_reconciliation.parquet",
        reconciliation,
        "deterministic_event_reconciliation",
    )
    write_parquet(
        "outputs/live/document_inventory.parquet",
        documents,
        "primary_document_extraction_inventory",
    )
    write_parquet(
        "outputs/live/review_contract.parquet",
        live,
        "updated_15_name_live_review_contract",
    )
    write_parquet(
        "outputs/live/human_review_queue.parquet",
        human_review,
        "explicit_human_review_queue",
    )

    source_copies = (
        (
            collection_root / "manifest.json",
            artifact_root / "source/collection_manifest.json",
            "verified_collection_manifest",
        ),
        (
            collection_root / "request_manifest.json",
            artifact_root / "source/request_manifest.json",
            "exact_request_response_manifest",
        ),
        (
            collection_root / "source/frozen_e1_manifest.json",
            artifact_root / "source/frozen_e1_manifest.json",
            "frozen_parent_e1_manifest",
        ),
        (
            collection_root / "source/collection_request_plan.parquet",
            artifact_root / "source/collection_request_plan.parquet",
            "frozen_exact_request_plan",
        ),
        (
            collection_root / "source/collection_contract.json",
            artifact_root / "source/collection_contract.json",
            "frozen_collection_contract",
        ),
        (
            collection_root / "contracts/deterministic_event_policy.json",
            artifact_root / "contracts/deterministic_event_policy.json",
            "frozen_deterministic_event_policy",
        ),
        (
            collection_root / "contracts/llm_summary_contract.json",
            artifact_root / "contracts/llm_summary_contract.json",
            "frozen_summary_contract",
        ),
        (
            collection_root / "contracts/time_semantics.json",
            artifact_root / "contracts/time_semantics.json",
            "frozen_time_semantics",
        ),
    )
    for source, target, role in source_copies:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        outputs.append((target, role))

    extraction_contract_path = artifact_root / "contracts/extraction_contract.json"
    _write_json(
        extraction_contract_path,
        {
            "schema_version": 1,
            "evidence_boundary": "retrieved_SEC_primary_document_bytes_only",
            "normalization": (
                "BeautifulSoup(lxml).get_text(' ',strip=True), then collapse "
                "all whitespace runs to one ASCII space"
            ),
            "locator": (
                "zero-based normalized text character half-open interval plus "
                "SHA-256 of the exact supporting passage"
            ),
            "claim_policy": (
                "one accession-bound exact quoted passage per successful "
                "retrieved document; no filing-family inference"
            ),
            "effective_time_policy": (
                "date-only or status language is not upgraded to an exact "
                "effective timestamp"
            ),
            "summary_policy": (
                "retrieved evidence only; every claim requires source ID, "
                "URL, accession/form, publication/retrieval times, response "
                "hash, evidence path, locator, and exact passage; summary "
                "cannot change deterministic action"
            ),
            "prohibited_inputs": DETERMINISTIC_EVENT_POLICY[
                "unsupported_inference_policy"
            ],
        },
    )
    outputs.append((extraction_contract_path, "extraction_and_citation_contract"))

    report_path = artifact_root / "report/retrieved_event_review.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        _render_report(documents, claims, reconciliation, live)
    )
    outputs.append((report_path, "concise_cited_event_review_report"))

    preservation = _validate_preservation(
        ROOT
        / "artifacts/canonical_refresh/us/20260730T110301Z/"
        "review/review_manifest.json"
    )
    code_lineage = []
    for path in (
        ROOT / "portfolio/event_review.py",
        ROOT / "portfolio/event_review_extraction.py",
        ROOT / "workflows/reconcile_live_event_evidence.py",
        ROOT / "tests/portfolio/test_event_review_extraction.py",
    ):
        code_lineage.append(
            {
                "path": path.relative_to(ROOT).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    manifest = {
        "schema_version": 1,
        "artifact_class": (
            "VERSIONED_E1_RETRIEVED_PRIMARY_DOCUMENT_EXTRACTION"
        ),
        "review_id": review_id,
        "created_at_utc": utc_now(),
        "source_collection": {
            "artifact_root": _display_path(collection_root),
            "manifest_sha256": EXPECTED_COLLECTION_MANIFEST_SHA256,
            "request_plan_sha256": EXPECTED_PLAN_SHA256,
            "collection_contract_sha256": EXPECTED_CONTRACT_SHA256,
            "request_count": 47,
            "successful_primary_documents": 46,
            "http_503_responses": 1,
            "aggregate_response_bytes": 19_120_821,
            "verification": collection_verification,
        },
        "source_parent_e1": {
            "artifact_root": _display_path(parent_root),
            "manifest_sha256": EXPECTED_PARENT_E1_MANIFEST_SHA256,
            "verification": parent_verification,
        },
        "code_lineage": code_lineage,
        "preservation": preservation,
        "records": [
            *[_record(artifact_root, path, role) for path, role in outputs],
            *[
                _record(
                    artifact_root,
                    path,
                    (
                        "preserved_http_503_response"
                        if "primary_doc.xml.gz" in path.as_posix()
                        else "exact_sec_primary_document_response"
                    ),
                )
                for path in sorted(raw_paths)
            ],
        ],
        "extraction": {
            "document_rows": len(documents),
            "successful_document_rows": int(
                documents["http_status"].eq(200).sum()
            ),
            "claim_rows": len(claims),
            "deterministic_unresolved_rows": int(
                reconciliation["deterministic_action"].eq("unresolved").sum()
            ),
            "human_review_rows": len(human_review),
            "live_contract_rows": len(live),
            "summary_allowed_rows": int(live["summary_allowed"].sum()),
            "summary_prohibited_rows": int(
                live["summary_allowed"].eq(False).sum()
            ),
        },
        "claim": {
            "all_collection_bytes_verified_before_processing": True,
            "frozen_parent_e1_verified_before_processing": True,
            "frozen_plan_and_contract_verified_before_processing": True,
            "preservation_hashes_verified_before_processing": True,
            "primary_document_evidence_only": True,
            "deterministic_rules_precede_narrative": True,
            "unsupported_event_inferences_applied": False,
            "ambiguous_evidence_routed_to_human_review": True,
            "hpk_http_503_unresolved_summary_prohibited": True,
            "summary_claims_require_retrieved_evidence_citations": True,
            "summary_may_change_deterministic_action": False,
            "external_request_made": False,
            "collection_retried": False,
            "performance_calculated": False,
            "dataset_promoted": False,
            "p3_p4_consumption_changed": False,
        },
    }
    manifest_path = artifact_root / "manifest.json"
    _write_json(manifest_path, manifest)
    return manifest_path


def verify_retrieved_event_review_artifact(
    artifact_root: Path,
) -> dict[str, Any]:
    """Rehash the complete extraction artifact and reproduce every locator."""
    artifact_root = artifact_root.resolve()
    manifest_path = artifact_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    failures: list[str] = []
    for item in manifest["records"]:
        path = artifact_root / item["path"]
        if (
            not path.is_file()
            or path.stat().st_size != int(item["size_bytes"])
            or sha256_file(path) != item["sha256"]
        ):
            failures.append(item["path"])

    documents = pd.read_parquet(
        artifact_root / "outputs/live/document_inventory.parquet"
    )
    claims = pd.read_parquet(
        artifact_root / "outputs/live/document_extracted_claims.parquet"
    )
    reconciliation = pd.read_parquet(
        artifact_root / "outputs/live/document_reconciliation.parquet"
    )
    live = pd.read_parquet(
        artifact_root / "outputs/live/review_contract.parquet"
    )
    queue = pd.read_parquet(
        artifact_root / "outputs/live/human_review_queue.parquet"
    )
    document_by_source = documents.set_index("source_id", drop=False)
    locator_failures: list[str] = []
    locator_pattern = re.compile(
        r"^normalized_text_chars:(\d+)-(\d+);passage_sha256:([0-9a-f]{64})$"
    )
    for claim in claims.itertuples(index=False):
        if claim.source_id not in document_by_source.index:
            locator_failures.append(str(claim.claim_id))
            continue
        document = document_by_source.loc[claim.source_id]
        path = artifact_root / str(claim.evidence_path)
        with gzip.open(path, "rb") as handle:
            payload = handle.read()
        if hashlib.sha256(payload).hexdigest() != str(
            claim.source_response_sha256
        ):
            locator_failures.append(str(claim.claim_id))
            continue
        match = locator_pattern.fullmatch(str(claim.document_locator))
        if match is None:
            locator_failures.append(str(claim.claim_id))
            continue
        start, end = int(match.group(1)), int(match.group(2))
        text = _normalized_document_text(payload)
        passage = text[start:end]
        if (
            passage != claim.supporting_passage
            or passage != claim.claim_text
            or hashlib.sha256(passage.encode()).hexdigest()
            != match.group(3)
            or match.group(3) != claim.supporting_passage_sha256
            or str(document["request_url"]) != str(claim.request_url)
            or str(document["accession_number"])
            != str(claim.accession_number)
            or str(document["form"]) != str(claim.form)
        ):
            locator_failures.append(str(claim.claim_id))

    preservation = _validate_preservation(
        ROOT
        / "artifacts/canonical_refresh/us/20260730T110301Z/"
        "review/review_manifest.json"
    )
    if preservation != manifest["preservation"]:
        failures.append("preservation")
    if (
        len(documents) != 47
        or documents["http_status"].eq(200).sum() != 46
        or len(claims) != 46
        or len(reconciliation) != 47
        or reconciliation["deterministic_action"].ne("unresolved").any()
        or reconciliation["unsupported_inferences_applied"].any()
        or len(live) != 15
        or len(queue) != 15
        or not live["human_review_required"].all()
        or live["summary_allowed"].sum() != 13
        or live.set_index("ticker").loc["HPK", "summary_allowed"]
        or live.set_index("ticker").loc["SSTK", "summary_allowed"]
        or live["summary_may_change_deterministic_action"].any()
        or manifest["claim"]["external_request_made"]
        or manifest["claim"]["performance_calculated"]
    ):
        failures.append("artifact_contract")
    if failures or locator_failures:
        raise RuntimeError(
            "retrieved E1 extraction verification failed: "
            f"records={failures}, locators={locator_failures}"
        )
    return {
        "manifest_sha256": sha256_file(manifest_path),
        "records_verified": len(manifest["records"]),
        "raw_responses_verified": len(documents),
        "successful_documents": 46,
        "claims_verified": len(claims),
        "deterministic_rows": len(reconciliation),
        "live_rows": len(live),
        "human_review_rows": len(queue),
        "summary_allowed_rows": int(live["summary_allowed"].sum()),
        "performance_calculated": False,
        "external_request_made": False,
    }
