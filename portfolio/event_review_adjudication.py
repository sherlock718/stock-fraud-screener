"""Offline human-signoff-ready adjudication of the frozen E1 review queue.

This module performs no network I/O. It independently reverifies the frozen
retrieved-document extraction before reading evidence, adjudicates only the
exact extracted passages, applies the frozen deterministic policy before any
narrative, and writes a new non-overwriting artifact.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

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
)
from portfolio.event_review_extraction import (
    _normalized_document_text,
    verify_retrieved_event_review_artifact,
)


DEFAULT_EXTRACTION_ROOT = (
    ROOT
    / "artifacts/event_review/us/"
    "20260730T154650Z-e1-extraction-v2"
)
EXPECTED_EXTRACTION_MANIFEST_SHA256 = (
    "e8f0e81f3a051b801720241235b706f31ed0e68ec34063ca4bccf3f958ba264a"
)
EXPECTED_COLLECTION_MANIFEST_SHA256 = (
    "ad14c45402c95e2b652ac7e0f8b98707a44eb4279f3988be602bca5ec77208ee"
)
EXPECTED_PARENT_E1_MANIFEST_SHA256 = (
    "e44ff9aa9d2ac3be2ae66e5fb006bba435cdda3eef4c4521b4d1e3362a58caa6"
)
EXPECTED_RESPONSE_BYTES = 19_120_821


DOCUMENT_ADJUDICATION_COLUMNS = (
    "request_id",
    "source_id",
    "rank",
    "ticker",
    "name",
    "sec_cik",
    "accession_number",
    "form",
    "http_status",
    "claim_id",
    "source_publication_time",
    "source_publication_time_status",
    "source_retrieved_at",
    "source_response_sha256",
    "evidence_path",
    "document_locator",
    "supporting_passage_sha256",
    "supporting_passage",
    "affected_security",
    "affected_security_role",
    "exact_security_scope_status",
    "event_type",
    "event_status",
    "document_stated_time",
    "document_stated_time_role",
    "document_stated_time_precision",
    "announcement_time",
    "announcement_time_precision",
    "announcement_time_status",
    "effective_time",
    "effective_time_precision",
    "effective_time_status",
    "transaction_or_action_terms",
    "terms_complete",
    "terms_status",
    "cross_document_conflict_status",
    "cross_document_conflict",
    "remaining_ambiguity",
    "rule_exact_citation_complete",
    "rule_source_publication_complete",
    "rule_exact_shortlist_security_scope_complete",
    "rule_exact_effective_timestamp_complete",
    "rule_complete_terms",
    "rule_recognized_type_status_for_shortlist_security",
    "frozen_rule_requirements_fully_satisfied",
    "frozen_rule_match_status",
    "deterministic_action",
    "deterministic_reason_code",
    "human_review_required",
    "summary_allowed_for_name",
    "summary_may_change_deterministic_action",
    "unsupported_inferences_applied",
    "adjudication_status",
)

NAME_ADJUDICATION_COLUMNS = (
    "requirement_id",
    "stable_row_id",
    "rank",
    "ticker",
    "name",
    "sec_cik",
    "decision_timestamp",
    "live_review_as_of",
    "planned_document_count",
    "retrieved_document_count",
    "claim_count",
    "affected_securities_and_roles",
    "event_types_and_statuses",
    "source_publication_times",
    "source_publication_time_status",
    "document_stated_times",
    "announcement_times_and_precision",
    "effective_times_and_precision",
    "transaction_or_action_terms_complete",
    "terms_status",
    "resolved_document_count",
    "unresolved_document_count",
    "document_set_status",
    "cross_document_conflict_status",
    "cross_document_conflict",
    "frozen_rule_requirements_fully_satisfied",
    "deterministic_action",
    "deterministic_reason",
    "human_review_required",
    "summary_allowed",
    "summary_status",
    "adjudication_summary",
    "cited_claim_ids",
    "remaining_ambiguity",
    "human_signoff_status",
    "summary_may_change_deterministic_action",
)

LIVE_REVIEW_CONTRACT_COLUMNS = (
    "requirement_id",
    "stable_row_id",
    "rank",
    "ticker",
    "name",
    "sec_cik",
    "decision_timestamp",
    "live_review_as_of",
    "adjudication_id",
    "planned_document_count",
    "retrieved_document_count",
    "claim_count",
    "document_set_status",
    "resolved_document_count",
    "unresolved_document_count",
    "frozen_rule_requirements_fully_satisfied",
    "deterministic_action",
    "review_status",
    "human_review_required",
    "summary_allowed",
    "summary_status",
    "summary_may_change_deterministic_action",
    "cited_claim_ids",
    "remaining_ambiguity",
    "human_signoff_status",
)

HUMAN_SIGNOFF_QUEUE_COLUMNS = (
    "review_queue_id",
    "adjudication_id",
    "rank",
    "ticker",
    "name",
    "sec_cik",
    "priority",
    "queue_status",
    "document_set_status",
    "deterministic_action",
    "frozen_rule_requirements_fully_satisfied",
    "cited_claim_ids",
    "cited_source_ids",
    "required_signoff_checks",
    "proposed_human_signoff",
    "human_signoff_status",
    "summary_allowed",
    "summary_may_change_deterministic_action",
)

RECONCILIATION_COLUMNS = (
    "level",
    "population",
    "total_count",
    "resolved_count",
    "unresolved_count",
    "exclude_count",
    "warn_count",
    "no_action_count",
    "human_review_count",
    "rule_requirements_fully_satisfied_count",
)


@dataclass(frozen=True)
class AdjudicationSpec:
    affected_security: str
    affected_security_role: str
    scope_status: str
    event_type: str
    event_status: str
    stated_time: str | None = None
    stated_time_role: str = "not_stated"
    announcement_time: str | None = None
    effective_time: str | None = None
    ambiguity: str | None = None


def _spec(
    affected_security: str,
    affected_security_role: str,
    scope_status: str,
    event_type: str,
    event_status: str,
    *,
    stated_time: str | None = None,
    stated_time_role: str = "not_stated",
    announcement_time: str | None = None,
    effective_time: str | None = None,
    ambiguity: str | None = None,
) -> AdjudicationSpec:
    return AdjudicationSpec(
        affected_security=affected_security,
        affected_security_role=affected_security_role,
        scope_status=scope_status,
        event_type=event_type,
        event_status=event_status,
        stated_time=stated_time,
        stated_time_role=stated_time_role,
        announcement_time=announcement_time,
        effective_time=effective_time,
        ambiguity=ambiguity,
    )


NO_SHORTLIST_SECURITY = "no_shortlist_security_named_in_exact_passage"
NON_SHORTLIST_INSTRUMENT = "exact_non_shortlist_security_or_instrument_named"
SHORTLIST_CONSIDERATION = (
    "shortlist_common_stock_named_as_consideration_not_termination"
)
SHORTLIST_OFFERING = (
    "shortlist_common_stock_named_as_offering_instrument_not_termination"
)


# Every field below is bounded by the exact extracted passage for the accession.
# It deliberately narrows broader filing/document labels when the passage does
# not itself state the broader fact.
ADJUDICATION_SPECS: dict[str, AdjudicationSpec] = {
    "0001104659-25-105300": _spec(
        "All issued and outstanding membership interests of Disdero",
        "Target equity interests acquired by OpCo from Tumac",
        NON_SHORTLIST_INSTRUMENT,
        "asset_acquisition",
        "completed_as_stated",
    ),
    "0001104659-25-084862": _spec(
        "Revolving Credit Agreement obligations",
        "BlueLinx and certain subsidiaries are borrowers or guarantors",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "entered",
        stated_time="2025-08-27",
        stated_time_role="agreement_entry_date",
    ),
    "0000893538-26-000055": _spec(
        "South Texas Divestiture assets, not further identified in the passage",
        "The Company is the divesting party and cash-proceeds recipient",
        NO_SHORTLIST_SECURITY,
        "asset_disposition",
        "completed",
        stated_time="2026-04-30",
        stated_time_role="completion_date",
        effective_time="2026-04-30",
    ),
    "0001104659-26-025261": _spec(
        "Notes governed by the Indenture, with series terms absent here",
        "The Company entered the Indenture with guarantors and trustee",
        NON_SHORTLIST_INSTRUMENT,
        "debt_financing",
        "indenture_entered",
        stated_time="2026-03-09",
        stated_time_role="indenture_entry_date",
    ),
    "0001104659-26-017062": _spec(
        "Rights, titles and interests in specified Maverick Basin assets",
        "SM Energy agreed to sell the assets to the purchaser",
        NO_SHORTLIST_SECURITY,
        "asset_disposition",
        "sale_agreement_entered",
        stated_time="2026-02-17",
        stated_time_role="agreement_entry_date",
    ),
    "0001104659-26-008380": _spec(
        "Civitas Common Stock and SM Energy Common Stock",
        (
            "Civitas common stock was converted or cancelled; SM Energy "
            "common stock is the 1.45-share consideration security"
        ),
        SHORTLIST_CONSIDERATION,
        "merger",
        "completed",
        stated_time="2026-01-30",
        stated_time_role="consummation_date",
        effective_time="2026-01-30",
        ambiguity=(
            "The passage states cessation for Civitas common stock, not SM "
            "Energy common stock. SM common is consideration stock, and only "
            "a date—not an exact timestamp or complete terms—is stated."
        ),
    ),
    "0001104659-25-123526": _spec(
        "No security class is stated; the passage names merger-agreement parties",
        "SM Energy is a merger-agreement party and Merger Sub is its subsidiary",
        NO_SHORTLIST_SECURITY,
        "merger_agreement",
        "entered",
        stated_time="2025-11-02",
        stated_time_role="agreement_entry_date",
    ),
    "0001104659-25-122082": _spec(
        "No security class is stated; the passage names merger-agreement parties",
        "SM Energy is a merger-agreement party and Merger Sub is its subsidiary",
        NO_SHORTLIST_SECURITY,
        "merger_agreement",
        "entered",
        stated_time="2025-11-02",
        stated_time_role="agreement_entry_date",
    ),
    "0001104659-25-118705": _spec(
        "No security class is stated; the passage names merger-agreement parties",
        "SM Energy is a merger-agreement party and Merger Sub is its subsidiary",
        NO_SHORTLIST_SECURITY,
        "merger_agreement",
        "entered",
        stated_time="2025-11-02",
        stated_time_role="agreement_entry_date",
    ),
    "0000893538-25-000144": _spec(
        "No security class is stated; the passage names merger-agreement parties",
        "SM Energy is a merger-agreement party and Merger Sub is its subsidiary",
        NO_SHORTLIST_SECURITY,
        "merger_agreement",
        "entered",
        stated_time="2025-11-02",
        stated_time_role="agreement_entry_date",
    ),
    "0000893538-25-000126": _spec(
        "Amended Credit Agreement obligations",
        "SM Energy entered the amendment with the agent and lenders",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "amendment_entered",
        stated_time="2025-10-13",
        stated_time_role="agreement_entry_date",
    ),
    "0001437749-26-005335": _spec(
        "One hundred percent of Interworks Single Member SA share capital",
        "Target share capital purchased by the Company from the seller",
        NON_SHORTLIST_INSTRUMENT,
        "asset_acquisition",
        "completed_as_stated",
    ),
    "0001628280-25-042486": _spec(
        "Term Loan Credit Agreement obligations",
        "The Company, borrower, guarantors, agent and lenders amended the loan",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "amendment_entered",
        stated_time="2025-09-18",
        stated_time_role="amendment_effective_date",
        effective_time="2025-09-18",
    ),
    "0001193125-25-283199": _spec(
        "4.450% Senior Notes due 2029",
        "AutoNation is the issuer and seller of the debt notes",
        NON_SHORTLIST_INSTRUMENT,
        "debt_financing",
        "sale_closed",
        stated_time="2025-11-14",
        stated_time_role="debt_sale_closing_date",
        effective_time="2025-11-14",
    ),
    "0001193125-25-273371": _spec(
        "Senior Notes with amount, rate and maturity omitted in the passage",
        "The issuer is offering an incompletely described debt instrument",
        NON_SHORTLIST_INSTRUMENT,
        "debt_offering",
        "terms_incomplete_in_retrieved_text",
    ),
    "0001437749-26-022144": _spec(
        "Credit Agreement obligations",
        "HighPeak is the borrower under an amendment effective only later",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "amendment_entered_effectiveness_not_stated",
        stated_time="2026-06-30",
        stated_time_role="agreement_entry_date",
    ),
    "0001437749-26-015209": _spec(
        "HighPeak common stock, $0.0001 par value, as Placement Shares",
        "Issuer stock may be offered through or to agents under the ATM program",
        SHORTLIST_OFFERING,
        "common_stock_at_the_market_offering",
        "authorized_from_time_to_time",
        stated_time="2026-05-06",
        stated_time_role="sales_agreement_entry_date",
    ),
    "0001437749-25-029107": _spec(
        (
            "No security is stated; the incomplete passage names a Principal "
            "Stockholder Group and retirement/resignation context"
        ),
        "Governance participants are named in an incomplete sentence",
        "no_shortlist_security_named_passage_incomplete",
        "governance_change_fragment",
        "passage_incomplete",
        stated_time="2025-09-15",
        stated_time_role="governance_context_date",
        ambiguity=(
            "The exact extracted passage is an incomplete sentence and cannot "
            "support complete governance terms or any security-level effect."
        ),
    ),
    "0001437749-25-024452": _spec(
        "Credit Agreement obligations",
        "HighPeak is the borrower under an amendment effective only later",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "amendment_entered_effectiveness_not_stated",
        stated_time="2025-08-01",
        stated_time_role="agreement_entry_date",
    ),
    "0001193125-26-251470": _spec(
        "Oil and gas properties, interests and related acquired assets",
        "A wholly owned NOG subsidiary is purchaser; NOG is a limited-purpose party",
        NO_SHORTLIST_SECURITY,
        "asset_acquisition",
        "completed",
        stated_time="2026-06-01",
        stated_time_role="closing_date",
        effective_time="2026-06-01",
    ),
    "0001193125-26-237800": _spec(
        "NOG common stock as Stock Consideration and the acquired assets",
        "NOG common stock is acquisition consideration, not a cancelled security",
        SHORTLIST_CONSIDERATION,
        "asset_acquisition_with_stock_consideration",
        "agreement_entered_pending_closing",
        stated_time="2026-05-22",
        stated_time_role="agreement_entry_date",
        ambiguity=(
            "The passage states that NOG common stock is consideration; it "
            "does not state that NOG common stock ceases to be eligible, and "
            "closing time and complete terms remain unresolved."
        ),
    ),
    "0001193125-26-064523": _spec(
        "Antero Acquisitions, with underlying interests absent from the passage",
        "Northern announced the closing in a press release",
        NO_SHORTLIST_SECURITY,
        "asset_acquisition",
        "closing_announced",
        stated_time="2026-02-23",
        stated_time_role="announcement_date",
        announcement_time="2026-02-23",
    ),
    "0001193125-25-310561": _spec(
        "Specified upstream oil and gas rights, interests and related assets",
        "Northern and INR Holdings are joint prospective purchasers",
        NO_SHORTLIST_SECURITY,
        "asset_acquisition",
        "agreement_entered_pending_closing",
        stated_time="2025-12-05",
        stated_time_role="agreement_entry_date",
    ),
    "0001104485-25-000161": _spec(
        "Revolving Credit Facility obligations",
        "Northern entered the facility with agent and lenders",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "entered",
        stated_time="2025-11-05",
        stated_time_role="agreement_entry_date",
    ),
    "0001193125-25-226774": _spec(
        "7.875% Senior Notes due 2033",
        "Northern issued the debt notes under an indenture",
        NON_SHORTLIST_INSTRUMENT,
        "debt_financing",
        "issued",
        stated_time="2025-10-01",
        stated_time_role="issuance_date",
        effective_time="2025-10-01",
    ),
    "0001193125-26-018663": _spec(
        "French Criteo and Lux Criteo corporate forms; no security class stated",
        "The corporate entity is proposed to convert across jurisdictions",
        "corporate_entity_named_no_shortlist_security_class",
        "corporate_redomiciliation_proposal",
        "shareholder_approval_pending",
    ),
    "0001193125-25-262679": _spec(
        "French Criteo and Lux Criteo corporate forms; no security class stated",
        "The corporate entity is proposed to convert across jurisdictions",
        "corporate_entity_named_no_shortlist_security_class",
        "corporate_redomiciliation_proposal",
        "shareholder_approval_pending",
    ),
    "0001193125-26-275945": _spec(
        "7.75% Senior Unsecured Notes due 2031",
        "Universal issued and sold the debt notes to purchasers",
        NON_SHORTLIST_INSTRUMENT,
        "debt_financing",
        "issued_and_sold",
        stated_time="2026-06-16",
        stated_time_role="issuance_and_sale_date",
        effective_time="2026-06-16",
    ),
    "0001627475-26-000039": _spec(
        "Credit Facility obligations",
        "Upwork and domestic subsidiaries entered the facility as obligors",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "entered",
        stated_time="2026-06-23",
        stated_time_role="agreement_entry_date",
    ),
    "0001627475-25-000048": _spec(
        "No security; the passage concerns non-GAAP EPS guidance",
        "The issuer corrects a clerical error in earnings guidance",
        NO_SHORTLIST_SECURITY,
        "earnings_guidance_correction",
        "filed",
    ),
    "0001104659-25-125672": _spec(
        "Amended Credit Agreement obligations",
        "An Amplify subsidiary is borrower and other entities are parties",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "amendment_entered",
        stated_time="2025-12-31",
        stated_time_role="agreement_entry_date",
    ),
    "0001104659-25-124890": _spec(
        "Asset Sale assets, not further identified in the passage",
        "The seller completed the disposition for stated cash proceeds",
        NO_SHORTLIST_SECURITY,
        "asset_disposition",
        "completed",
        stated_time="2025-12-29",
        stated_time_role="completion_date",
        effective_time="2025-12-29",
    ),
    "0001104659-25-124255": _spec(
        "EQV Asset Sale assets, not further identified in the passage",
        "The seller completed the disposition for stated cash proceeds",
        NO_SHORTLIST_SECURITY,
        "asset_disposition",
        "completed",
        stated_time="2025-12-23",
        stated_time_role="completion_date",
        effective_time="2025-12-23",
    ),
    "0001104659-25-106982": _spec(
        "Specified Oklahoma oil and gas properties and equipment",
        "Indirect Amplify subsidiaries sold the assets to Revolution",
        NO_SHORTLIST_SECURITY,
        "asset_disposition",
        "sale_stated",
        stated_time="2025-11-04",
        stated_time_role="agreement_and_sale_date",
        effective_time="2025-11-04",
    ),
    "0001104659-25-103773": _spec(
        "Specified East Texas and Louisiana oil and gas properties and equipment",
        "Indirect Amplify subsidiaries sold the assets to Alpha",
        NO_SHORTLIST_SECURITY,
        "asset_disposition",
        "sale_stated",
        stated_time="2025-10-28",
        stated_time_role="agreement_and_sale_date",
        effective_time="2025-10-28",
    ),
    "0001104659-26-086469": _spec(
        "Securities governed by the Paloma agreement, class not stated here",
        "Matador subsidiaries are purchaser and limited-purpose guarantor",
        NO_SHORTLIST_SECURITY,
        "securities_acquisition_agreement",
        "entered_pending_closing",
        stated_time="2026-07-22",
        stated_time_role="agreement_entry_date",
    ),
    "0001520006-26-000029": _spec(
        "Secured revolving Credit Agreement obligations",
        "A Matador subsidiary entered the amendment",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "amendment_entered",
        stated_time="2026-06-10",
        stated_time_role="agreement_entry_date",
    ),
    "0001104659-26-024110": _spec(
        "Notes whose series terms are not stated in this passage",
        "The Company issued and sold the debt notes and received proceeds",
        NON_SHORTLIST_INSTRUMENT,
        "debt_financing",
        "issued_and_sold",
        stated_time="2026-03-05",
        stated_time_role="issuance_and_sale_date",
        effective_time="2026-03-05",
    ),
    "0001104659-26-020877": _spec(
        "6.000% Senior Notes due 2034",
        "Matador agreed to issue and sell the debt notes",
        NON_SHORTLIST_INSTRUMENT,
        "debt_offering",
        "purchase_agreement_entered",
        stated_time="2026-02-26",
        stated_time_role="agreement_entry_date",
    ),
    "0001104659-25-120120": _spec(
        "Secured revolving Credit Agreement obligations",
        "A Matador subsidiary entered the amendment",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "amendment_entered",
        stated_time="2025-12-09",
        stated_time_role="agreement_entry_date",
    ),
    "0001367644-26-000068": _spec(
        "Government contract modification, with agency name truncated here",
        "An Emergent subsidiary received the modification",
        NO_SHORTLIST_SECURITY,
        "government_contract_modification",
        "received",
        stated_time="2026-06-26",
        stated_time_role="receipt_date",
    ),
    "0001367644-26-000038": _spec(
        "Term Loan Agreement obligations",
        "Emergent entered the agreement as borrower with lenders and agent",
        NO_SHORTLIST_SECURITY,
        "financing_agreement",
        "entered",
        stated_time="2026-04-16",
        stated_time_role="agreement_entry_date",
    ),
    "0001367644-25-000188": _spec(
        "Government contract modification, with agency name truncated here",
        "An Emergent subsidiary received the modification",
        NO_SHORTLIST_SECURITY,
        "government_contract_modification",
        "received",
        stated_time="2025-09-05",
        stated_time_role="receipt_date",
    ),
    "0001628280-26-032163": _spec(
        "No security terms are stated in the incomplete underwriting sentence",
        "Issuer and selling stockholder entered an underwriting agreement",
        "no_shortlist_security_named_passage_incomplete",
        "underwriting_agreement",
        "entered",
        stated_time="2026-05-05",
        stated_time_role="agreement_entry_date",
        ambiguity=(
            "The extracted sentence ends after naming underwriters and does "
            "not state offering or repurchase terms."
        ),
    ),
    "0001628280-26-016059": _spec(
        "No security terms are stated in the incomplete underwriting sentence",
        "Issuer and selling stockholder entered an underwriting agreement",
        "no_shortlist_security_named_passage_incomplete",
        "underwriting_agreement",
        "entered",
        stated_time="2026-03-05",
        stated_time_role="agreement_entry_date",
        ambiguity=(
            "The extracted sentence ends after naming underwriters and does "
            "not state offering or repurchase terms."
        ),
    ),
    "0001628280-26-015358": _spec(
        "Ingram Micro common stock, $0.01 par value",
        "An existing selling stockholder is offering the common stock",
        SHORTLIST_OFFERING,
        "secondary_common_stock_offering",
        "preliminary_terms_incomplete",
    ),
}


CONFLICTS = {
    "BXC": (
        "none_identified_distinct_actions",
        "The two passages concern a target-interest acquisition and financing.",
    ),
    "SM": (
        "none_identified_chronological_progression",
        (
            "Repeated merger-agreement passages and the later consummation "
            "passage are chronological, not conflicting; other passages are "
            "distinct financing or asset actions."
        ),
    ),
    "CLMB": (
        "none_identified_single_passage",
        "Only one exact claim is available.",
    ),
    "ARDT": (
        "none_identified_single_passage",
        "Only one exact claim is available.",
    ),
    "AN": (
        "none_identified_preliminary_then_completed_debt_evidence",
        "Placeholder debt text and later completed note-sale terms do not conflict.",
    ),
    "HPK": (
        "not_assessable_incomplete_document_set_http_503",
        "The Form 25-NSE document is unavailable because HTTP 503 was preserved.",
    ),
    "NOG": (
        "none_identified_chronological_or_distinct_actions",
        "Pending/completed acquisition passages are chronological or distinct.",
    ),
    "CRTO": (
        "none_identified_duplicate_passage",
        "The two exact passages are identical redomiciliation statements.",
    ),
    "UVE": (
        "none_identified_single_passage",
        "Only one exact claim is available.",
    ),
    "UPWK": (
        "none_identified_distinct_actions",
        "The passages concern financing and a guidance correction.",
    ),
    "AMPY": (
        "none_identified_chronological_or_distinct_actions",
        "The asset-sale passages are chronological or distinct; financing is separate.",
    ),
    "MTDR": (
        "none_identified_distinct_actions",
        "The passages concern separate acquisition and financing actions.",
    ),
    "EBS": (
        "none_identified_distinct_actions",
        "The passages concern contract modifications and financing.",
    ),
    "INGM": (
        "none_identified_but_exact_passages_incomplete",
        "No direct conflict appears, but two underwriting passages are incomplete.",
    ),
    "SSTK": (
        "not_assessable_no_retrieved_documents",
        "The frozen plan contains no primary-document candidate.",
    ),
}


def _default_ambiguity(spec: AdjudicationSpec) -> str:
    return (
        spec.ambiguity
        or (
            "The exact passage does not establish all frozen-rule requirements "
            "for a selection-changing event affecting the shortlisted "
            "security: exact scope, recognized type/status, complete terms, "
            "and an exact effective timestamp."
        )
    )


def _resolve_artifact_root(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def _verify_manifest_records(root: Path, manifest: dict[str, Any]) -> int:
    for item in manifest["records"]:
        path = root / item["path"]
        if (
            not path.is_file()
            or path.stat().st_size != int(item["size_bytes"])
            or sha256_file(path) != item["sha256"]
        ):
            raise RuntimeError(f"manifest record mismatch: {item['path']}")
    return len(manifest["records"])


def independently_verify_extraction(
    extraction_root: Path = DEFAULT_EXTRACTION_ROOT,
) -> dict[str, Any]:
    """Reverify all frozen bytes, locators, lineage, and preservation hashes."""
    extraction_root = extraction_root.resolve()
    manifest_path = extraction_root / "manifest.json"
    if sha256_file(manifest_path) != EXPECTED_EXTRACTION_MANIFEST_SHA256:
        raise RuntimeError("extraction manifest hash mismatch")
    manifest = json.loads(manifest_path.read_text())
    extraction_records = _verify_manifest_records(extraction_root, manifest)
    if extraction_records != 62:
        raise RuntimeError("extraction record count mismatch")

    request_manifest = json.loads(
        (extraction_root / "source/request_manifest.json").read_text()
    )
    requests: dict[str, tuple[dict[str, Any], bytes]] = {}
    response_bytes = 0
    statuses: dict[int, int] = {}
    for item in request_manifest["records"]:
        path = extraction_root / item["stored_path"]
        stored = path.read_bytes()
        if (
            len(stored) != int(item["stored_size_bytes"])
            or hashlib.sha256(stored).hexdigest() != item["stored_sha256"]
        ):
            raise RuntimeError(f"stored response mismatch: {item['request_id']}")
        payload = gzip.decompress(stored)
        if (
            len(payload) != int(item["response_size_bytes"])
            or hashlib.sha256(payload).hexdigest() != item["response_sha256"]
        ):
            raise RuntimeError(f"response-byte mismatch: {item['request_id']}")
        response_bytes += len(payload)
        status = int(item["http_status"])
        statuses[status] = statuses.get(status, 0) + 1
        requests[str(item["request_id"])] = (item, payload)
    if (
        len(requests) != 47
        or response_bytes != EXPECTED_RESPONSE_BYTES
        or statuses != {200: 46, 503: 1}
    ):
        raise RuntimeError("stored response count/byte/status mismatch")

    claims = pd.read_parquet(
        extraction_root / "outputs/live/document_extracted_claims.parquet"
    )
    locator_pattern = re.compile(
        r"^normalized_text_chars:(\d+)-(\d+);"
        r"passage_sha256:([0-9a-f]{64})$"
    )
    for claim in claims.itertuples(index=False):
        item, payload = requests[str(claim.request_id)]
        if item["response_sha256"] != claim.source_response_sha256:
            raise RuntimeError(f"claim response mismatch: {claim.claim_id}")
        match = locator_pattern.fullmatch(str(claim.document_locator))
        if match is None:
            raise RuntimeError(f"claim locator invalid: {claim.claim_id}")
        start, end = int(match.group(1)), int(match.group(2))
        passage = _normalized_document_text(payload)[start:end]
        passage_hash = hashlib.sha256(passage.encode()).hexdigest()
        if (
            passage != claim.supporting_passage
            or passage != claim.claim_text
            or passage_hash != match.group(3)
            or passage_hash != claim.supporting_passage_sha256
        ):
            raise RuntimeError(f"claim locator mismatch: {claim.claim_id}")
    if len(claims) != 46:
        raise RuntimeError("claim count mismatch")

    source_collection = manifest["source_collection"]
    collection_root = _resolve_artifact_root(source_collection["artifact_root"])
    collection_manifest_path = collection_root / "manifest.json"
    if (
        sha256_file(collection_manifest_path)
        != EXPECTED_COLLECTION_MANIFEST_SHA256
        or (extraction_root / "source/collection_manifest.json").read_bytes()
        != collection_manifest_path.read_bytes()
    ):
        raise RuntimeError("collection lineage mismatch")
    collection_manifest = json.loads(collection_manifest_path.read_text())
    collection_records = _verify_manifest_records(
        collection_root,
        collection_manifest,
    )
    if collection_records != 59:
        raise RuntimeError("collection record count mismatch")

    source_parent = manifest["source_parent_e1"]
    parent_root = _resolve_artifact_root(source_parent["artifact_root"])
    parent_manifest_path = parent_root / "manifest.json"
    if (
        sha256_file(parent_manifest_path)
        != EXPECTED_PARENT_E1_MANIFEST_SHA256
        or (extraction_root / "source/frozen_e1_manifest.json").read_bytes()
        != parent_manifest_path.read_bytes()
    ):
        raise RuntimeError("parent E1 lineage mismatch")
    parent_manifest = json.loads(parent_manifest_path.read_text())
    parent_records = _verify_manifest_records(parent_root, parent_manifest)
    if parent_records != 148:
        raise RuntimeError("parent E1 record count mismatch")

    preservation = _validate_preservation(
        ROOT
        / "artifacts/canonical_refresh/us/20260730T110301Z/"
        "review/review_manifest.json"
    )
    if preservation != manifest["preservation"]:
        raise RuntimeError("preservation lineage mismatch")
    preservation_entries = sum(
        len(preservation[key])
        for key in (
            "canonical_manifests",
            "pointer_revisions",
            "source_evidence",
            "tracked_legacy_and_international",
        )
    )
    if preservation_entries != 29:
        raise RuntimeError("preservation entry count mismatch")

    return {
        "manifest_sha256": sha256_file(manifest_path),
        "manifest_records_verified": extraction_records,
        "stored_responses_verified": len(requests),
        "aggregate_response_bytes": response_bytes,
        "http_200_responses": statuses[200],
        "http_503_responses": statuses[503],
        "claim_locators_and_hashes_verified": len(claims),
        "collection_records_verified": collection_records,
        "parent_e1_records_verified": parent_records,
        "preservation_entries_verified": preservation_entries,
    }


def _copy_file(
    source: Path,
    target: Path,
    outputs: list[tuple[Path, str]],
    role: str,
) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    if target.stat().st_size != source.stat().st_size:
        raise RuntimeError(f"copied file size mismatch: {source}")
    outputs.append((target, role))


def _claim_lookup(claims: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        str(row.accession_number): pd.Series(row._asdict())
        for row in claims.itertuples(index=False)
    }


def _cite(
    lookup: dict[str, pd.Series],
    accession: str,
) -> str:
    return f"[{lookup[accession]['claim_id']}]"


def _name_summary(
    ticker: str,
    lookup: dict[str, pd.Series],
) -> str | None:
    """Return only citation-bound narrative for summary-eligible names."""
    if ticker in {"HPK", "SSTK"}:
        return None
    if ticker == "BXC":
        return (
            "OpCo acquired all Disdero membership interests for approximately "
            "$96 million, subject to adjustments "
            f"{_cite(lookup, '0001104659-25-105300')}; BlueLinx and certain "
            "subsidiaries also entered a revolving credit agreement "
            f"{_cite(lookup, '0001104659-25-084862')}."
        )
    if ticker == "SM":
        return (
            "The passages state a completed South Texas divestiture "
            f"{_cite(lookup, '0000893538-26-000055')}, an indenture "
            f"{_cite(lookup, '0001104659-26-025261')}, an asset-sale agreement "
            f"{_cite(lookup, '0001104659-26-017062')}, and a credit amendment "
            f"{_cite(lookup, '0000893538-25-000126')}. Four passages state "
            "entry into the SM/Civitas merger agreement "
            f"{_cite(lookup, '0001104659-25-123526')} "
            f"{_cite(lookup, '0001104659-25-122082')} "
            f"{_cite(lookup, '0001104659-25-118705')} "
            f"{_cite(lookup, '0000893538-25-000144')}. The completion passage "
            "states that Civitas common stock was converted or cancelled and "
            "that SM common stock was the 1.45-share consideration security "
            f"{_cite(lookup, '0001104659-26-008380')}; it does not state that "
            "SM common stock ceased to be independently eligible."
        )
    if ticker == "CLMB":
        return (
            "The Company purchased all Interworks share capital for "
            "approximately €8.0 million, subject to adjustments "
            f"{_cite(lookup, '0001437749-26-005335')}."
        )
    if ticker == "ARDT":
        return (
            "The Company and loan parties entered a term-loan amendment with "
            "September 18, 2025 stated only as the Amendment Effective Date "
            f"{_cite(lookup, '0001628280-25-042486')}."
        )
    if ticker == "AN":
        return (
            "AutoNation closed a $600 million sale of 4.450% Senior Notes due "
            f"2029 {_cite(lookup, '0001193125-25-283199')}; the earlier "
            "prospectus passage contains omitted amount, rate, and maturity "
            f"fields {_cite(lookup, '0001193125-25-273371')}."
        )
    if ticker == "NOG":
        return (
            "The passages describe pending and completed Parallax asset "
            "acquisition steps "
            f"{_cite(lookup, '0001193125-26-237800')} "
            f"{_cite(lookup, '0001193125-26-251470')}, pending and announced-"
            "closed Antero acquisition steps "
            f"{_cite(lookup, '0001193125-25-310561')} "
            f"{_cite(lookup, '0001193125-26-064523')}, a credit facility "
            f"{_cite(lookup, '0001104485-25-000161')}, and a senior-notes "
            f"issuance {_cite(lookup, '0001193125-25-226774')}. NOG common "
            "stock is stated as Parallax acquisition consideration, not as a "
            "security that ceased to be independently eligible "
            f"{_cite(lookup, '0001193125-26-237800')}."
        )
    if ticker == "CRTO":
        return (
            "Two passages identically state a proposed France-to-Luxembourg "
            "corporate conversion subject to closing conditions including "
            f"shareholder approval {_cite(lookup, '0001193125-25-262679')} "
            f"{_cite(lookup, '0001193125-26-018663')}."
        )
    if ticker == "UVE":
        return (
            "Universal issued and sold $100 million of 7.75% Senior Unsecured "
            f"Notes due 2031 {_cite(lookup, '0001193125-26-275945')}."
        )
    if ticker == "UPWK":
        return (
            "Upwork and domestic subsidiaries entered a credit facility "
            f"{_cite(lookup, '0001627475-26-000039')}; a separate amendment "
            "corrected a clerical error in non-GAAP EPS guidance "
            f"{_cite(lookup, '0001627475-25-000048')}."
        )
    if ticker == "AMPY":
        return (
            "An Amplify subsidiary entered a credit amendment "
            f"{_cite(lookup, '0001104659-25-125672')}. Exact passages state "
            "two asset-sale agreements and their later completion/proceeds "
            f"{_cite(lookup, '0001104659-25-106982')} "
            f"{_cite(lookup, '0001104659-25-124890')} "
            f"{_cite(lookup, '0001104659-25-103773')} "
            f"{_cite(lookup, '0001104659-25-124255')}."
        )
    if ticker == "MTDR":
        return (
            "Matador passages state a Paloma securities-purchase agreement "
            f"{_cite(lookup, '0001104659-26-086469')}, two credit amendments "
            f"{_cite(lookup, '0001520006-26-000029')} "
            f"{_cite(lookup, '0001104659-25-120120')}, an agreement to sell "
            f"6.000% Senior Notes due 2034 "
            f"{_cite(lookup, '0001104659-26-020877')}, and later note-sale "
            f"proceeds {_cite(lookup, '0001104659-26-024110')}."
        )
    if ticker == "EBS":
        return (
            "Emergent passages state two government-contract modifications "
            f"{_cite(lookup, '0001367644-25-000188')} "
            f"{_cite(lookup, '0001367644-26-000068')} and a term-loan "
            f"agreement {_cite(lookup, '0001367644-26-000038')}."
        )
    if ticker == "INGM":
        return (
            "A preliminary passage states that an existing selling stockholder "
            "is offering Ingram common stock "
            f"{_cite(lookup, '0001628280-26-015358')}. Two later passages state "
            "only that the issuer and selling stockholder entered underwriting "
            "agreements and end while naming underwriters "
            f"{_cite(lookup, '0001628280-26-016059')} "
            f"{_cite(lookup, '0001628280-26-032163')}."
        )
    raise RuntimeError(f"no summary template for {ticker}")


def _render_report(
    names: pd.DataFrame,
    documents: pd.DataFrame,
    reconciliation: pd.DataFrame,
) -> str:
    lines = [
        "# E1 Offline Citation-by-Citation Adjudication",
        "",
        "## Sign-off outcome",
        "",
        (
            "The frozen deterministic rules were applied before narrative. "
            f"Resolved names: {int(names['deterministic_action'].ne('unresolved').sum())}; "
            f"unresolved names: {int(names['deterministic_action'].eq('unresolved').sum())}. "
            "All 15 names require explicit human sign-off. This artifact is "
            "unsigned and does not change P3/P4 consumption."
        ),
        "",
        (
            "At document level, all 47 rows remain unresolved: 46 exact "
            "claim-cited SEC primary documents and the preserved HPK Form "
            "25-NSE HTTP 503 row. No external request or retry occurred."
        ),
        "",
        "SEC acceptance timestamps below are source-publication lineage only. "
        "Date-only statements remain date-only and were not converted to "
        "timestamps.",
        "",
        "## Name-level adjudication",
        "",
    ]
    for row in names.sort_values("rank").itertuples(index=False):
        if row.ticker == "HPK":
            narrative = (
                "Summary prohibited: the Form 25-NSE response is HTTP 503. "
                "The name remains unresolved regardless of the four other "
                "retrieved claims."
            )
        elif row.ticker == "SSTK":
            narrative = (
                "Summary prohibited: the frozen plan contains no retrieved "
                "primary-document candidate."
            )
        else:
            narrative = str(row.adjudication_summary)
        lines.append(
            f"- **{row.ticker}** — `{row.deterministic_action}`; "
            f"{row.human_signoff_status}. {narrative}"
        )
    lines.extend(
        [
            "",
            "## Deterministic reconciliation",
            "",
            (
                f"Documents: {int(reconciliation.loc[reconciliation['level'].eq('document'), 'resolved_count'].iloc[0])} "
                "resolved / 47 total. Names: "
                f"{int(reconciliation.loc[reconciliation['level'].eq('name'), 'resolved_count'].iloc[0])} "
                "resolved / 15 total. Exclude, warn, and no-action counts are "
                "all zero. Every incomplete or ambiguous row remains routed "
                "to human review."
            ),
            "",
            "## Claim-level citation index",
            "",
        ]
    )
    for row in documents.sort_values(["rank", "source_publication_time"]).itertuples(
        index=False
    ):
        if row.claim_id is None or pd.isna(row.claim_id):
            lines.append(
                f"- {row.ticker} {row.accession_number} {row.form}: no claim; "
                f"HTTP {row.http_status}; response SHA-256 "
                f"`{row.source_response_sha256}`; `{row.evidence_path}`."
            )
            continue
        lines.append(
            f"- {row.claim_id}: {row.ticker} {row.accession_number} "
            f"{row.form}; SEC acceptance lineage "
            f"`{row.source_publication_time}`; locator "
            f"`{row.document_locator}`; passage SHA-256 "
            f"`{row.supporting_passage_sha256}`; response SHA-256 "
            f"`{row.source_response_sha256}`; `{row.evidence_path}`."
        )
    lines.extend(
        [
            "",
            "## Human sign-off boundary",
            "",
            (
                "A human reviewer may confirm the unresolved action for each "
                "queue row. Any different action requires identifying an "
                "existing exact claim citation that fully supplies exact "
                "shortlisted-security scope, recognized type/status, complete "
                "terms, and effective-time precision under the frozen rules. "
                "Narrative may never change the deterministic action."
            ),
            "",
            (
                "Filing family, missing prices, ticker disappearance, name "
                "similarity, filing metadata alone, model knowledge, and "
                "uncited narrative were not used."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def _deterministic_row(
    document: pd.Series,
    claim: pd.Series | None,
    spec: AdjudicationSpec | None,
    *,
    evaluation: pd.Timestamp,
    summary_allowed: bool,
) -> dict[str, Any]:
    conflict_status, conflict = CONFLICTS[str(document["ticker"])]
    if claim is None or spec is None:
        return {
            "request_id": document["request_id"],
            "source_id": document["source_id"],
            "rank": int(document["rank"]),
            "ticker": document["ticker"],
            "name": document["name"],
            "sec_cik": document["sec_cik"],
            "accession_number": document["accession_number"],
            "form": document["form"],
            "http_status": int(document["http_status"]),
            "claim_id": None,
            "source_publication_time": document["source_publication_time"],
            "source_publication_time_status": (
                "SEC_acceptance_lineage_only_no_retrieved_claim"
            ),
            "source_retrieved_at": document["source_retrieved_at"],
            "source_response_sha256": document["response_sha256"],
            "evidence_path": document["evidence_path"],
            "document_locator": None,
            "supporting_passage_sha256": None,
            "supporting_passage": None,
            "affected_security": "unavailable_http_503",
            "affected_security_role": "unavailable_http_503",
            "exact_security_scope_status": "unavailable_http_503",
            "event_type": "unresolved",
            "event_status": "unresolved",
            "document_stated_time": None,
            "document_stated_time_role": "unavailable_http_503",
            "document_stated_time_precision": "unavailable",
            "announcement_time": None,
            "announcement_time_precision": "unavailable",
            "announcement_time_status": "unavailable_http_503",
            "effective_time": None,
            "effective_time_precision": "unavailable",
            "effective_time_status": "unavailable_http_503",
            "transaction_or_action_terms": None,
            "terms_complete": False,
            "terms_status": "unavailable_http_503",
            "cross_document_conflict_status": conflict_status,
            "cross_document_conflict": conflict,
            "remaining_ambiguity": (
                "HPK Form 25-NSE content is unavailable after the preserved "
                "HTTP 503. No event, security role, time, or terms may be inferred."
            ),
            "rule_exact_citation_complete": False,
            "rule_source_publication_complete": True,
            "rule_exact_shortlist_security_scope_complete": False,
            "rule_exact_effective_timestamp_complete": False,
            "rule_complete_terms": False,
            "rule_recognized_type_status_for_shortlist_security": False,
            "frozen_rule_requirements_fully_satisfied": False,
            "frozen_rule_match_status": "not_assessable_http_503",
            "deterministic_action": "unresolved",
            "deterministic_reason_code": "missing_retrieved_primary_document",
            "human_review_required": True,
            "summary_allowed_for_name": False,
            "summary_may_change_deterministic_action": False,
            "unsupported_inferences_applied": False,
            "adjudication_status": "unresolved_human_review_required",
        }

    ambiguity = _default_ambiguity(spec)
    event = {
        "event_type": spec.event_type,
        "event_status": spec.event_status,
        # A date-only statement is never converted to a timestamp here.
        "effective_at": None,
        "effective_time_status": (
            "date_only_not_exact_timestamp"
            if spec.effective_time
            else "not_established_by_exact_passage"
        ),
        "source_published_at": document["source_publication_time"],
        "source_retrieved_at": document["source_retrieved_at"],
        "source_id": document["source_id"],
        "source_response_sha256": document["response_sha256"],
        "ambiguity": ambiguity,
    }
    decision = deterministic_event_decision(
        event,
        as_of_timestamp=evaluation,
        evaluation_retrieved_at=evaluation,
    )
    if decision["action"] != "unresolved":
        raise RuntimeError("incomplete adjudicated evidence changed action")
    return {
        "request_id": document["request_id"],
        "source_id": document["source_id"],
        "rank": int(document["rank"]),
        "ticker": document["ticker"],
        "name": document["name"],
        "sec_cik": document["sec_cik"],
        "accession_number": document["accession_number"],
        "form": document["form"],
        "http_status": int(document["http_status"]),
        "claim_id": claim["claim_id"],
        "source_publication_time": document["source_publication_time"],
        "source_publication_time_status": "SEC_acceptance_lineage_only",
        "source_retrieved_at": document["source_retrieved_at"],
        "source_response_sha256": claim["source_response_sha256"],
        "evidence_path": claim["evidence_path"],
        "document_locator": claim["document_locator"],
        "supporting_passage_sha256": claim["supporting_passage_sha256"],
        "supporting_passage": claim["supporting_passage"],
        "affected_security": spec.affected_security,
        "affected_security_role": spec.affected_security_role,
        "exact_security_scope_status": spec.scope_status,
        "event_type": spec.event_type,
        "event_status": spec.event_status,
        "document_stated_time": spec.stated_time,
        "document_stated_time_role": spec.stated_time_role,
        "document_stated_time_precision": (
            "date_only" if spec.stated_time else "unavailable"
        ),
        "announcement_time": spec.announcement_time,
        "announcement_time_precision": (
            "date_only" if spec.announcement_time else "unavailable"
        ),
        "announcement_time_status": (
            "document_states_announcement_date_only"
            if spec.announcement_time
            else "not_established_by_exact_passage"
        ),
        "effective_time": spec.effective_time,
        "effective_time_precision": (
            "date_only" if spec.effective_time else "unavailable"
        ),
        "effective_time_status": (
            "document_states_date_only_not_exact_timestamp"
            if spec.effective_time
            else "not_established_by_exact_passage"
        ),
        "transaction_or_action_terms": claim["supporting_passage"],
        "terms_complete": False,
        "terms_status": "incomplete_exact_passage_only",
        "cross_document_conflict_status": conflict_status,
        "cross_document_conflict": conflict,
        "remaining_ambiguity": ambiguity,
        "rule_exact_citation_complete": True,
        "rule_source_publication_complete": True,
        "rule_exact_shortlist_security_scope_complete": False,
        "rule_exact_effective_timestamp_complete": False,
        "rule_complete_terms": False,
        "rule_recognized_type_status_for_shortlist_security": False,
        "frozen_rule_requirements_fully_satisfied": False,
        "frozen_rule_match_status": (
            "no_complete_selection_changing_rule_match_for_shortlist_security"
        ),
        "deterministic_action": decision["action"],
        "deterministic_reason_code": decision["reason_code"],
        "human_review_required": True,
        "summary_allowed_for_name": summary_allowed,
        "summary_may_change_deterministic_action": False,
        "unsupported_inferences_applied": False,
        "adjudication_status": "unresolved_human_review_required",
    }


def build_event_review_adjudication(
    artifact_root: Path,
    *,
    adjudication_id: str,
    extraction_root: Path = DEFAULT_EXTRACTION_ROOT,
) -> Path:
    """Build one offline, non-overwriting E1 adjudication artifact."""
    if not REVIEW_ID_PATTERN.fullmatch(adjudication_id):
        raise ValueError("adjudication_id must be an immutable UTC identifier")
    artifact_root = artifact_root.resolve()
    extraction_root = extraction_root.resolve()
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"E1 adjudication target is not empty: {artifact_root}")
    artifact_root.mkdir(parents=True, exist_ok=True)

    established_verification = verify_retrieved_event_review_artifact(
        extraction_root
    )
    independent_verification = independently_verify_extraction(extraction_root)
    extraction_manifest = json.loads(
        (extraction_root / "manifest.json").read_text()
    )

    source_documents = pd.read_parquet(
        extraction_root / "outputs/live/document_inventory.parquet"
    )
    source_claims = pd.read_parquet(
        extraction_root / "outputs/live/document_extracted_claims.parquet"
    )
    source_live = pd.read_parquet(
        extraction_root / "outputs/live/review_contract.parquet"
    )
    claims_by_request = source_claims.set_index("request_id", drop=False)
    summary_allowed_by_ticker = source_live.set_index("ticker")[
        "summary_allowed"
    ].to_dict()
    evaluation = max(
        _timestamp(value)
        for value in source_documents["source_retrieved_at"]
        if _timestamp(value) is not None
    )

    # Deterministic document adjudication is complete and validated before any
    # name-level narrative is constructed.
    document_rows = []
    for document in source_documents.sort_values(
        ["rank", "source_publication_time"]
    ).to_dict("records"):
        claim = None
        spec = None
        if document["request_id"] in claims_by_request.index:
            claim = claims_by_request.loc[document["request_id"]]
            spec = ADJUDICATION_SPECS.get(str(document["accession_number"]))
            if spec is None:
                raise RuntimeError(
                    "missing citation-bounded adjudication spec: "
                    f"{document['accession_number']}"
                )
        document_rows.append(
            _deterministic_row(
                pd.Series(document),
                claim,
                spec,
                evaluation=evaluation,
                summary_allowed=bool(
                    summary_allowed_by_ticker[document["ticker"]]
                ),
            )
        )
    documents = pd.DataFrame(
        document_rows,
        columns=DOCUMENT_ADJUDICATION_COLUMNS,
    )
    if (
        len(documents) != 47
        or documents["claim_id"].notna().sum() != 46
        or documents["deterministic_action"].ne("unresolved").any()
        or documents["frozen_rule_requirements_fully_satisfied"].any()
        or not documents["human_review_required"].all()
        or documents["unsupported_inferences_applied"].any()
    ):
        raise RuntimeError("document adjudication boundary failed")

    claim_lookup = _claim_lookup(source_claims)
    name_rows = []
    contract_rows = []
    queue_rows = []
    for row in source_live.sort_values("rank").to_dict("records"):
        ticker = str(row["ticker"])
        ticker_documents = documents[documents["ticker"].eq(ticker)]
        cited_claim_ids = sorted(
            ticker_documents["claim_id"].dropna().astype(str).tolist()
        )
        cited_source_ids = sorted(
            ticker_documents.loc[
                ticker_documents["claim_id"].notna(),
                "source_id",
            ]
            .astype(str)
            .tolist()
        )
        affected_securities_and_roles = [
            {
                "claim_id": item.claim_id,
                "affected_security": item.affected_security,
                "affected_security_role": item.affected_security_role,
                "scope_status": item.exact_security_scope_status,
            }
            for item in ticker_documents.itertuples(index=False)
        ]
        event_types_and_statuses = [
            {
                "claim_id": item.claim_id,
                "event_type": item.event_type,
                "event_status": item.event_status,
            }
            for item in ticker_documents.itertuples(index=False)
        ]
        source_publication_times = [
            {
                "claim_id": item.claim_id,
                "source_publication_time": item.source_publication_time,
                "status": item.source_publication_time_status,
            }
            for item in ticker_documents.itertuples(index=False)
        ]
        document_stated_times = [
            {
                "claim_id": item.claim_id,
                "value": item.document_stated_time,
                "role": item.document_stated_time_role,
                "precision": item.document_stated_time_precision,
            }
            for item in ticker_documents.itertuples(index=False)
        ]
        announcement_times = [
            {
                "claim_id": item.claim_id,
                "value": item.announcement_time,
                "precision": item.announcement_time_precision,
                "status": item.announcement_time_status,
            }
            for item in ticker_documents.itertuples(index=False)
        ]
        effective_times = [
            {
                "claim_id": item.claim_id,
                "value": item.effective_time,
                "precision": item.effective_time_precision,
                "status": item.effective_time_status,
            }
            for item in ticker_documents.itertuples(index=False)
        ]
        resolved_count = int(
            ticker_documents["deterministic_action"].ne("unresolved").sum()
        )
        unresolved_count = int(
            ticker_documents["deterministic_action"].eq("unresolved").sum()
        )
        conflict_status, conflict = CONFLICTS[ticker]
        summary_allowed = bool(row["summary_allowed"])
        summary = _name_summary(ticker, claim_lookup)
        if summary_allowed != (summary is not None):
            raise RuntimeError(f"summary eligibility mismatch: {ticker}")
        remaining = (
            "HPK Form 25-NSE remains unavailable after HTTP 503; summary and "
            "deterministic resolution are prohibited."
            if ticker == "HPK"
            else (
                "No primary-document candidate exists in the frozen plan; "
                "summary and deterministic resolution are prohibited."
                if ticker == "SSTK"
                else (
                    "No exact claim set fully supplies exact shortlisted-"
                    "security scope, recognized type/status, complete terms, "
                    "and an exact effective timestamp."
                )
            )
        )
        name_rows.append(
            {
                "requirement_id": row["requirement_id"],
                "stable_row_id": row["stable_row_id"],
                "rank": int(row["rank"]),
                "ticker": ticker,
                "name": row["name"],
                "sec_cik": row["sec_cik"],
                "decision_timestamp": row["decision_timestamp"],
                "live_review_as_of": row["live_review_as_of"],
                "planned_document_count": int(
                    row["retrieved_primary_document_count"]
                    + (1 if ticker == "HPK" else 0)
                ),
                "retrieved_document_count": int(
                    row["retrieved_primary_document_count"]
                ),
                "claim_count": len(cited_claim_ids),
                "affected_securities_and_roles": json.dumps(
                    affected_securities_and_roles
                ),
                "event_types_and_statuses": json.dumps(
                    event_types_and_statuses
                ),
                "source_publication_times": json.dumps(
                    source_publication_times
                ),
                "source_publication_time_status": (
                    "SEC_acceptance_lineage_only"
                    if not ticker_documents.empty
                    else "unavailable_no_retrieved_documents"
                ),
                "document_stated_times": json.dumps(document_stated_times),
                "announcement_times_and_precision": json.dumps(
                    announcement_times
                ),
                "effective_times_and_precision": json.dumps(effective_times),
                "transaction_or_action_terms_complete": False,
                "terms_status": (
                    "all_exact_passages_incomplete_for_frozen_selection_rule"
                    if not ticker_documents.empty
                    else "unavailable_no_retrieved_documents"
                ),
                "resolved_document_count": resolved_count,
                "unresolved_document_count": unresolved_count,
                "document_set_status": row["document_set_status"],
                "cross_document_conflict_status": conflict_status,
                "cross_document_conflict": conflict,
                "frozen_rule_requirements_fully_satisfied": False,
                "deterministic_action": "unresolved",
                "deterministic_reason": (
                    "incomplete_or_ambiguous_frozen_rule_contract"
                ),
                "human_review_required": True,
                "summary_allowed": summary_allowed,
                "summary_status": row["summary_status"],
                "adjudication_summary": summary,
                "cited_claim_ids": json.dumps(cited_claim_ids),
                "remaining_ambiguity": remaining,
                "human_signoff_status": "pending_explicit_human_signoff",
                "summary_may_change_deterministic_action": False,
            }
        )
        contract_rows.append(
            {
                "requirement_id": row["requirement_id"],
                "stable_row_id": row["stable_row_id"],
                "rank": int(row["rank"]),
                "ticker": ticker,
                "name": row["name"],
                "sec_cik": row["sec_cik"],
                "decision_timestamp": row["decision_timestamp"],
                "live_review_as_of": row["live_review_as_of"],
                "adjudication_id": adjudication_id,
                "planned_document_count": int(
                    row["retrieved_primary_document_count"]
                    + (1 if ticker == "HPK" else 0)
                ),
                "retrieved_document_count": int(
                    row["retrieved_primary_document_count"]
                ),
                "claim_count": len(cited_claim_ids),
                "document_set_status": row["document_set_status"],
                "resolved_document_count": resolved_count,
                "unresolved_document_count": unresolved_count,
                "frozen_rule_requirements_fully_satisfied": False,
                "deterministic_action": "unresolved",
                "review_status": "adjudicated_pending_explicit_human_signoff",
                "human_review_required": True,
                "summary_allowed": summary_allowed,
                "summary_status": row["summary_status"],
                "summary_may_change_deterministic_action": False,
                "cited_claim_ids": json.dumps(cited_claim_ids),
                "remaining_ambiguity": remaining,
                "human_signoff_status": "pending_explicit_human_signoff",
            }
        )
        queue_rows.append(
            {
                "review_queue_id": (
                    f"human_signoff:{hashlib.sha256((adjudication_id + chr(31) + ticker).encode()).hexdigest()[:24]}"
                ),
                "adjudication_id": adjudication_id,
                "rank": int(row["rank"]),
                "ticker": ticker,
                "name": row["name"],
                "sec_cik": row["sec_cik"],
                "priority": (
                    "critical_incomplete_retrieval"
                    if ticker == "HPK"
                    else (
                        "high_no_primary_candidate"
                        if ticker == "SSTK"
                        else "standard_explicit_signoff"
                    )
                ),
                "queue_status": "awaiting_explicit_human_signoff",
                "document_set_status": row["document_set_status"],
                "deterministic_action": "unresolved",
                "frozen_rule_requirements_fully_satisfied": False,
                "cited_claim_ids": json.dumps(cited_claim_ids),
                "cited_source_ids": json.dumps(cited_source_ids),
                "required_signoff_checks": json.dumps(
                    [
                        "Confirm every cited passage was reviewed literally.",
                        "Confirm no filing-family or metadata inference was used.",
                        "Confirm date-only evidence was not converted to a timestamp.",
                        "Confirm incomplete or ambiguous evidence remains unresolved.",
                        "Confirm narrative did not change the deterministic action.",
                    ]
                ),
                "proposed_human_signoff": "confirm_unresolved",
                "human_signoff_status": "pending_explicit_human_signoff",
                "summary_allowed": summary_allowed,
                "summary_may_change_deterministic_action": False,
            }
        )
    names = pd.DataFrame(name_rows, columns=NAME_ADJUDICATION_COLUMNS)
    live_contract = pd.DataFrame(
        contract_rows,
        columns=LIVE_REVIEW_CONTRACT_COLUMNS,
    )
    signoff_queue = pd.DataFrame(
        queue_rows,
        columns=HUMAN_SIGNOFF_QUEUE_COLUMNS,
    )
    if (
        len(names) != 15
        or len(live_contract) != 15
        or len(signoff_queue) != 15
        or names["deterministic_action"].ne("unresolved").any()
        or not names["human_review_required"].all()
        or names["summary_allowed"].sum() != 13
        or names.set_index("ticker").loc["HPK", "summary_allowed"]
        or names.set_index("ticker").loc["SSTK", "summary_allowed"]
        or names["summary_may_change_deterministic_action"].any()
    ):
        raise RuntimeError("name-level adjudication boundary failed")

    reconciliation = pd.DataFrame(
        [
            {
                "level": "document",
                "population": "47 frozen primary-document requests",
                "total_count": len(documents),
                "resolved_count": int(
                    documents["deterministic_action"].ne("unresolved").sum()
                ),
                "unresolved_count": int(
                    documents["deterministic_action"].eq("unresolved").sum()
                ),
                "exclude_count": int(
                    documents["deterministic_action"].eq("exclude").sum()
                ),
                "warn_count": int(
                    documents["deterministic_action"].eq("warn").sum()
                ),
                "no_action_count": int(
                    documents["deterministic_action"].eq("no_action").sum()
                ),
                "human_review_count": int(
                    documents["human_review_required"].sum()
                ),
                "rule_requirements_fully_satisfied_count": int(
                    documents[
                        "frozen_rule_requirements_fully_satisfied"
                    ].sum()
                ),
            },
            {
                "level": "name",
                "population": "15-name live review contract",
                "total_count": len(names),
                "resolved_count": int(
                    names["deterministic_action"].ne("unresolved").sum()
                ),
                "unresolved_count": int(
                    names["deterministic_action"].eq("unresolved").sum()
                ),
                "exclude_count": int(
                    names["deterministic_action"].eq("exclude").sum()
                ),
                "warn_count": int(
                    names["deterministic_action"].eq("warn").sum()
                ),
                "no_action_count": int(
                    names["deterministic_action"].eq("no_action").sum()
                ),
                "human_review_count": int(names["human_review_required"].sum()),
                "rule_requirements_fully_satisfied_count": int(
                    names["frozen_rule_requirements_fully_satisfied"].sum()
                ),
            },
        ],
        columns=RECONCILIATION_COLUMNS,
    )

    outputs: list[tuple[Path, str]] = []

    def write_parquet(relative: str, frame: pd.DataFrame, role: str) -> None:
        path = artifact_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        outputs.append((path, role))

    write_parquet(
        "outputs/live/name_level_adjudication.parquet",
        names,
        "name_level_citation_bounded_adjudication",
    )
    write_parquet(
        "outputs/live/document_level_adjudication.parquet",
        documents,
        "document_level_claim_cited_adjudication",
    )
    write_parquet(
        "outputs/live/deterministic_reconciliation.parquet",
        reconciliation,
        "resolved_versus_unresolved_deterministic_reconciliation",
    )
    write_parquet(
        "outputs/live/review_contract.parquet",
        live_contract,
        "updated_15_name_live_review_contract",
    )
    write_parquet(
        "outputs/live/human_signoff_queue.parquet",
        signoff_queue,
        "remaining_human_review_and_signoff_queue",
    )

    source_copies = (
        (
            extraction_root / "manifest.json",
            artifact_root / "source/extraction_manifest.json",
            "frozen_extraction_manifest",
        ),
        (
            extraction_root / "outputs/live/document_inventory.parquet",
            artifact_root / "source/extraction_document_inventory.parquet",
            "frozen_extraction_document_inventory",
        ),
        (
            extraction_root / "outputs/live/document_extracted_claims.parquet",
            artifact_root / "source/extraction_claims.parquet",
            "frozen_exact_extracted_claims",
        ),
        (
            extraction_root / "outputs/live/document_reconciliation.parquet",
            artifact_root / "source/extraction_document_reconciliation.parquet",
            "frozen_extraction_reconciliation",
        ),
        (
            extraction_root / "outputs/live/review_contract.parquet",
            artifact_root / "source/extraction_review_contract.parquet",
            "frozen_extraction_live_contract",
        ),
        (
            extraction_root / "outputs/live/human_review_queue.parquet",
            artifact_root / "source/extraction_human_review_queue.parquet",
            "frozen_extraction_human_review_queue",
        ),
        (
            extraction_root / "source/request_manifest.json",
            artifact_root / "source/request_manifest.json",
            "exact_request_response_manifest",
        ),
        (
            extraction_root / "source/collection_manifest.json",
            artifact_root / "source/collection_manifest.json",
            "verified_collection_manifest",
        ),
        (
            extraction_root / "source/frozen_e1_manifest.json",
            artifact_root / "source/frozen_e1_manifest.json",
            "frozen_parent_e1_manifest",
        ),
        (
            extraction_root / "source/collection_request_plan.parquet",
            artifact_root / "source/collection_request_plan.parquet",
            "frozen_exact_request_plan",
        ),
        (
            extraction_root / "source/collection_contract.json",
            artifact_root / "source/collection_contract.json",
            "frozen_collection_contract",
        ),
    )
    for source, target, role in source_copies:
        _copy_file(source, target, outputs, role)

    for contract in (
        "deterministic_event_policy.json",
        "extraction_contract.json",
        "llm_summary_contract.json",
        "time_semantics.json",
    ):
        _copy_file(
            extraction_root / "contracts" / contract,
            artifact_root / "contracts" / contract,
            outputs,
            f"frozen_{contract.removesuffix('.json')}",
        )

    adjudication_contract_path = (
        artifact_root / "contracts/adjudication_contract.json"
    )
    _write_json(
        adjudication_contract_path,
        {
            "schema_version": 1,
            "evidence_boundary": (
                "retrieved SEC primary-document bytes and the 46 exact "
                "extracted passages only"
            ),
            "policy_order": DETERMINISTIC_EVENT_POLICY["policy_order"],
            "deterministic_policy_sha256": sha256_file(
                extraction_root / "contracts/deterministic_event_policy.json"
            ),
            "publication_time_policy": (
                "SEC acceptance time is source-publication lineage, not proof "
                "of event announcement or effective time"
            ),
            "effective_time_policy": (
                "date-only statements retain date-only precision and are "
                "never converted to timestamps"
            ),
            "terms_policy": (
                "the exact passage is preserved as the bounded terms evidence; "
                "complete terms are false unless the passage itself is complete"
            ),
            "summary_policy": (
                "only summary-eligible names may receive retrieved-evidence "
                "narrative; every clause is claim-cited; narrative cannot "
                "change deterministic action"
            ),
            "signoff_policy": (
                "artifact remains unsigned until a human explicitly signs "
                "each queue row in a separate non-overwriting derivative"
            ),
            "prohibited_inputs": DETERMINISTIC_EVENT_POLICY[
                "unsupported_inference_policy"
            ],
        },
    )
    outputs.append(
        (adjudication_contract_path, "offline_adjudication_contract")
    )

    signoff_contract_path = artifact_root / "signoff/human_signoff_contract.json"
    _write_json(
        signoff_contract_path,
        {
            "schema_version": 1,
            "adjudication_id": adjudication_id,
            "signoff_status": "unsigned_pending_explicit_human_signoff",
            "queue_rows": 15,
            "proposed_signoff": "confirm_unresolved",
            "required_human_fields": [
                "reviewer_identity",
                "reviewed_at_utc",
                "review_queue_id",
                "signoff_decision",
                "signed_adjudication_manifest_sha256",
            ],
            "permitted_signoff_decisions": [
                "confirm_unresolved",
                "return_for_correction_with_existing_claim_citations",
            ],
            "mutation_policy": (
                "do not edit this artifact; create a separate timestamped, "
                "non-overwriting signed derivative"
            ),
            "summary_may_change_deterministic_action": False,
        },
    )
    outputs.append((signoff_contract_path, "explicit_human_signoff_contract"))

    report_path = artifact_root / "report/adjudication_report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(_render_report(names, documents, reconciliation))
    outputs.append((report_path, "concise_cited_adjudication_report"))

    raw_paths = []
    request_manifest = json.loads(
        (extraction_root / "source/request_manifest.json").read_text()
    )
    for item in request_manifest["records"]:
        source = extraction_root / item["stored_path"]
        target = artifact_root / item["stored_path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        if (
            target.stat().st_size != int(item["stored_size_bytes"])
            or sha256_file(target) != item["stored_sha256"]
        ):
            raise RuntimeError(f"copied response mismatch: {item['request_id']}")
        raw_paths.append(target)

    preservation = _validate_preservation(
        ROOT
        / "artifacts/canonical_refresh/us/20260730T110301Z/"
        "review/review_manifest.json"
    )
    if preservation != extraction_manifest["preservation"]:
        raise RuntimeError("preservation changed during adjudication")
    code_lineage = []
    for path in (
        ROOT / "portfolio/event_review.py",
        ROOT / "portfolio/event_review_extraction.py",
        ROOT / "portfolio/event_review_adjudication.py",
        ROOT / "workflows/adjudicate_live_event_review.py",
        ROOT / "tests/portfolio/test_event_review_adjudication.py",
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
        "artifact_class": "VERSIONED_E1_OFFLINE_HUMAN_SIGNOFF_ADJUDICATION",
        "adjudication_id": adjudication_id,
        "created_at_utc": utc_now(),
        "source_extraction": {
            "artifact_root": _display_path(extraction_root),
            "manifest_sha256": EXPECTED_EXTRACTION_MANIFEST_SHA256,
            "established_verification": established_verification,
            "independent_verification": independent_verification,
        },
        "source_collection": extraction_manifest["source_collection"],
        "source_parent_e1": extraction_manifest["source_parent_e1"],
        "code_lineage": code_lineage,
        "preservation": preservation,
        "records": [
            *[_record(artifact_root, path, role) for path, role in outputs],
            *[
                _record(
                    artifact_root,
                    path,
                    (
                        "preserved_hpk_http_503_response"
                        if path.name == "primary_doc.xml.gz"
                        else "exact_sec_primary_document_response"
                    ),
                )
                for path in sorted(raw_paths)
            ],
        ],
        "adjudication": {
            "name_rows": len(names),
            "document_rows": len(documents),
            "claim_cited_document_rows": int(documents["claim_id"].notna().sum()),
            "http_503_document_rows": int(documents["http_status"].eq(503).sum()),
            "resolved_document_rows": int(
                documents["deterministic_action"].ne("unresolved").sum()
            ),
            "unresolved_document_rows": int(
                documents["deterministic_action"].eq("unresolved").sum()
            ),
            "resolved_name_rows": int(
                names["deterministic_action"].ne("unresolved").sum()
            ),
            "unresolved_name_rows": int(
                names["deterministic_action"].eq("unresolved").sum()
            ),
            "human_signoff_queue_rows": len(signoff_queue),
            "summary_allowed_rows": int(names["summary_allowed"].sum()),
            "summary_prohibited_rows": int(
                names["summary_allowed"].eq(False).sum()
            ),
        },
        "claim": {
            "extraction_manifest_sha256_independently_verified": True,
            "all_62_extraction_records_verified_before_processing": True,
            "all_47_stored_responses_verified_before_processing": True,
            "all_46_claim_locators_verified_before_processing": True,
            "collection_and_parent_e1_lineage_verified_before_processing": True,
            "all_29_preservation_entries_verified_before_processing": True,
            "primary_document_evidence_only": True,
            "deterministic_rules_precede_narrative": True,
            "unsupported_event_inferences_applied": False,
            "all_incomplete_conflicting_or_ambiguous_rows_routed_to_review": True,
            "hpk_http_503_unresolved_summary_prohibited": True,
            "sstk_no_document_summary_prohibited": True,
            "sm_consideration_role_not_treated_as_sm_security_cessation": True,
            "date_only_statements_not_converted_to_timestamps": True,
            "summary_may_change_deterministic_action": False,
            "explicit_human_signoff_complete": False,
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


def verify_event_review_adjudication_artifact(
    artifact_root: Path,
) -> dict[str, Any]:
    """Rehash the full adjudication artifact and its frozen evidence contract."""
    artifact_root = artifact_root.resolve()
    manifest_path = artifact_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    _verify_manifest_records(artifact_root, manifest)

    extraction_manifest = artifact_root / "source/extraction_manifest.json"
    if sha256_file(extraction_manifest) != EXPECTED_EXTRACTION_MANIFEST_SHA256:
        raise RuntimeError("copied extraction manifest mismatch")
    preservation = _validate_preservation(
        ROOT
        / "artifacts/canonical_refresh/us/20260730T110301Z/"
        "review/review_manifest.json"
    )
    if preservation != manifest["preservation"]:
        raise RuntimeError("preservation mismatch")

    request_manifest = json.loads(
        (artifact_root / "source/request_manifest.json").read_text()
    )
    response_bytes = 0
    for item in request_manifest["records"]:
        path = artifact_root / item["stored_path"]
        stored = path.read_bytes()
        payload = gzip.decompress(stored)
        if (
            len(stored) != int(item["stored_size_bytes"])
            or hashlib.sha256(stored).hexdigest() != item["stored_sha256"]
            or len(payload) != int(item["response_size_bytes"])
            or hashlib.sha256(payload).hexdigest() != item["response_sha256"]
        ):
            raise RuntimeError(f"raw response mismatch: {item['request_id']}")
        response_bytes += len(payload)
    if len(request_manifest["records"]) != 47 or response_bytes != 19_120_821:
        raise RuntimeError("raw response boundary mismatch")

    names = pd.read_parquet(
        artifact_root / "outputs/live/name_level_adjudication.parquet"
    )
    documents = pd.read_parquet(
        artifact_root / "outputs/live/document_level_adjudication.parquet"
    )
    reconciliation = pd.read_parquet(
        artifact_root / "outputs/live/deterministic_reconciliation.parquet"
    )
    contract = pd.read_parquet(
        artifact_root / "outputs/live/review_contract.parquet"
    )
    queue = pd.read_parquet(
        artifact_root / "outputs/live/human_signoff_queue.parquet"
    )
    if (
        tuple(names.columns) != NAME_ADJUDICATION_COLUMNS
        or tuple(documents.columns) != DOCUMENT_ADJUDICATION_COLUMNS
        or tuple(reconciliation.columns) != RECONCILIATION_COLUMNS
        or tuple(contract.columns) != LIVE_REVIEW_CONTRACT_COLUMNS
        or tuple(queue.columns) != HUMAN_SIGNOFF_QUEUE_COLUMNS
        or len(names) != len(contract) != len(queue)
        or len(names) != 15
        or len(documents) != 47
        or documents["claim_id"].notna().sum() != 46
        or documents["deterministic_action"].ne("unresolved").any()
        or names["deterministic_action"].ne("unresolved").any()
        or not names["human_review_required"].all()
        or names["summary_allowed"].sum() != 13
        or names.set_index("ticker").loc["HPK", "summary_allowed"]
        or names.set_index("ticker").loc["SSTK", "summary_allowed"]
        or documents["unsupported_inferences_applied"].any()
        or manifest["claim"]["explicit_human_signoff_complete"]
        or manifest["claim"]["external_request_made"]
        or manifest["claim"]["performance_calculated"]
    ):
        raise RuntimeError("adjudication artifact contract failed")

    source_claims = pd.read_parquet(
        artifact_root / "source/extraction_claims.parquet"
    ).set_index("claim_id")
    for row in documents[documents["claim_id"].notna()].itertuples(index=False):
        source = source_claims.loc[row.claim_id]
        if (
            row.document_locator != source["document_locator"]
            or row.supporting_passage != source["supporting_passage"]
            or row.supporting_passage_sha256
            != source["supporting_passage_sha256"]
            or row.source_response_sha256 != source["source_response_sha256"]
            or row.evidence_path != source["evidence_path"]
        ):
            raise RuntimeError(f"adjudication citation drift: {row.claim_id}")

    return {
        "manifest_sha256": sha256_file(manifest_path),
        "records_verified": len(manifest["records"]),
        "raw_responses_verified": len(request_manifest["records"]),
        "aggregate_response_bytes": response_bytes,
        "claim_citations_verified": int(documents["claim_id"].notna().sum()),
        "document_rows": len(documents),
        "unresolved_document_rows": int(
            documents["deterministic_action"].eq("unresolved").sum()
        ),
        "name_rows": len(names),
        "unresolved_name_rows": int(
            names["deterministic_action"].eq("unresolved").sum()
        ),
        "human_signoff_queue_rows": len(queue),
        "summary_allowed_rows": int(names["summary_allowed"].sum()),
        "explicit_human_signoff_complete": False,
        "performance_calculated": False,
        "external_request_made": False,
    }
