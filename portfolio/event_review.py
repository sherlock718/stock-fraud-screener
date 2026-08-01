"""Versioned historical-then-live security-event review.

E1 consumes the provider-neutral S1 ledger without upgrading any S1
confidence, ambiguity, effective-time, or security-scope state.  Deterministic
policy is applied before an optional live summary.  Historical evaluation never
uses model knowledge, and every emitted warning is tied to exact dated source
evidence.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import re
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

import pandas as pd

from pipeline.security_ledger import (
    COVERAGE_COLUMNS,
    EVENT_COLUMNS,
    ISSUER_COLUMNS,
    LISTING_COLUMNS,
    RAW_EVIDENCE_COLUMNS,
    SECURITY_COLUMNS,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_S1_ROOT = (
    ROOT / "artifacts/security_ledger/us/20260730T141429Z-s1-final"
)
DEFAULT_D1_ROOT = ROOT / "artifacts/canonical_refresh/us/20260730T110301Z"
DEFAULT_P4_ROOT = (
    ROOT / "artifacts/canonical/corrected_us_annual_3y_product"
)
REVIEW_ID_PATTERN = re.compile(
    r"^[0-9]{8}T[0-9]{6}Z(?:-[a-z0-9-]+)?$"
)

PINNED_MANIFESTS = {
    "p2_manifest": (
        ROOT / "artifacts/canonical/corrected_us_annual/manifest.json",
        "40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3",
    ),
    "p3_manifest": (
        ROOT
        / "artifacts/canonical/corrected_us_annual_3y_research_model"
        / "manifest.json",
        "8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2",
    ),
    "p4_manifest": (
        DEFAULT_P4_ROOT / "manifest.json",
        "28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7",
    ),
    "d1_review_manifest": (
        DEFAULT_D1_ROOT / "review/review_manifest.json",
        "ca175587494c1529d21d6e7c7567dbe3b16c55913c9b4b7a84b9b0d1d4569bb9",
    ),
    "d1_p2_candidate_manifest": (
        DEFAULT_D1_ROOT / "p2_review_candidate/manifest.json",
        "545c2eec17dae8cdffd81fd8e1b89ebc1ccc3b47290b7b556f485bbaa5f436d6",
    ),
    "s1_manifest": (
        DEFAULT_S1_ROOT / "manifest.json",
        "28317e4ae0126367c38bb40c9fc8169394fc1141c475072d0dc484c141479a1e",
    ),
}

EXPECTED_S1_SCHEMAS = {
    "issuers": ISSUER_COLUMNS,
    "securities": SECURITY_COLUMNS,
    "listings": LISTING_COLUMNS,
    "events": EVENT_COLUMNS,
    "coverage": COVERAGE_COLUMNS,
    "raw_evidence": RAW_EVIDENCE_COLUMNS,
}

DETERMINISTIC_EVENT_POLICY = {
    "schema_version": 1,
    "policy_order": [
        "validate_exact_citation",
        "validate_source_publication_eligibility",
        "validate_exact_security_scope",
        "validate_event_effective_time",
        "apply_event_type_and_status_rule",
        "route_remaining_ambiguity_to_human_review",
        "permit_cited_live_summary_only_after_deterministic_action",
    ],
    "rules": [
        {
            "event_type": "pending_acquisition",
            "complete_statuses": ["announced_pending"],
            "action": "exclude",
            "reason": (
                "A cited, exact-security acquisition that is proven pending "
                "at the review time is excluded from a new selection."
            ),
        },
        {
            "event_type": "completed_merger",
            "complete_statuses": ["completed"],
            "action": "exclude",
            "reason": (
                "A security whose merger is proven effective by the review "
                "time is not an independently eligible security."
            ),
        },
        {
            "event_type": "bankruptcy",
            "complete_statuses": ["active", "filed", "ordered"],
            "action": "exclude",
            "reason": (
                "A cited bankruptcy state effective by the review time is a "
                "deterministic exclusion."
            ),
        },
        {
            "event_type": "suspension",
            "complete_statuses": ["active"],
            "action": "exclude",
            "reason": (
                "An active, exact-security trading suspension is a "
                "deterministic exclusion."
            ),
        },
        {
            "event_type": "delisting",
            "complete_statuses": ["effective", "completed"],
            "action": "exclude",
            "reason": (
                "A delisting proven effective for the exact security is a "
                "deterministic exclusion."
            ),
        },
        {
            "event_type": "delisting",
            "complete_statuses": ["announced_pending"],
            "action": "warn",
            "reason": (
                "A cited future delisting notice is warned before its "
                "effective time and requires terms review."
            ),
        },
        {
            "event_type": "registration_termination",
            "complete_statuses": ["filed", "effective"],
            "action": "warn",
            "reason": (
                "Registration termination is warned but is never treated as "
                "a delisting without exact security and listing evidence."
            ),
        },
        {
            "event_type": "exchange_noncompliance",
            "complete_statuses": ["notice_active"],
            "action": "warn",
            "reason": (
                "Exchange noncompliance is warned; only a separately cited "
                "suspension or delisting can exclude."
            ),
        },
        {
            "event_type": "other_material_event",
            "complete_statuses": ["confirmed"],
            "action": "warn",
            "reason": (
                "Other material events are warned and remain subject to "
                "human interpretation of cited evidence."
            ),
        },
    ],
    "unresolved_policy": (
        "Missing exact security scope, effective time, source publication "
        "time, citation, complete terms, or recognized status produces "
        "unresolved_human_review; it never produces an event claim."
    ),
    "unsupported_inference_policy": [
        "filing family alone",
        "missing price",
        "ticker disappearance",
        "name similarity",
        "current model knowledge",
        "uncited narrative",
    ],
}

LIVE_PRIMARY_FORMS = (
    "25",
    "25-NSE",
    "15-12B",
    "15-12G",
    "15-15D",
    "SC 13E3",
    "SC 13E3/A",
    "SC TO-T",
    "SC TO-T/A",
    "SC TO-I",
    "SC TO-I/A",
    "SC 14D9",
    "SC 14D9/A",
    "DEFM14A",
    "PREM14A",
    "S-4",
    "S-4/A",
    "424B3",
)
LIVE_8K_ITEMS = ("1.01", "1.03", "2.01", "3.01", "5.01")
LIVE_WINDOW_START = "2025-07-30T00:00:00+00:00"
LIVE_WINDOW_END = "2026-07-30T23:59:59+00:00"
FROZEN_E1_REVIEW_ID = "20260730T144043Z-e1-final"
FROZEN_E1_MANIFEST_SHA256 = (
    "e44ff9aa9d2ac3be2ae66e5fb006bba435cdda3eef4c4521b4d1e3362a58caa6"
)
FROZEN_E1_PLAN_SHA256 = (
    "ff856cff60eb35279c4f487bff5aaac679c83f22c8518d07d3c66daebd7ea433"
)
FROZEN_E1_CONTRACT_SHA256 = (
    "4fae9e662079ca479894db93ef67746cae77a1200abe2ebd06244e5e78a38cdf"
)
LIVE_RESPONSE_LIMIT_BYTES = 10 * 1024 * 1024
LIVE_AGGREGATE_LIMIT_BYTES = 100 * 1024 * 1024
SEC_MIN_REQUEST_INTERVAL_SECONDS = 0.12

HISTORICAL_COLUMNS = (
    "requirement_id",
    "stable_row_id",
    "rank",
    "ticker",
    "name",
    "sec_cik",
    "decision_timestamp",
    "entry_timestamp",
    "target_exit_timestamp",
    "s1_coverage_status",
    "s1_identity_status",
    "s1_listing_status",
    "s1_security_type_status",
    "s1_event_status",
    "s1_price_adjustment_status",
    "source_event_indicator_count",
    "published_by_decision_indicator_count",
    "holding_window_indicator_count",
    "decision_effective_event_count",
    "deterministic_action",
    "decision_eligibility_status",
    "historical_evaluation_status",
    "human_review_required",
    "llm_summary_allowed",
    "warning_ids",
    "cited_source_ids",
    "reason_codes",
)

WARNING_COLUMNS = (
    "warning_id",
    "review_mode",
    "review_context",
    "requirement_id",
    "stable_row_id",
    "ticker",
    "event_id",
    "source_event_type",
    "canonical_event_type",
    "source_event_status",
    "deterministic_action",
    "decision_eligibility_status",
    "human_review_required",
    "as_of_timestamp",
    "event_effective_at",
    "source_published_at",
    "source_retrieved_at",
    "source_id",
    "request_url",
    "source_response_sha256",
    "evidence_path",
    "evidence_uri",
    "accession_number",
    "form",
    "primary_document",
    "claim_text",
    "policy_reason_code",
)

LIVE_REVIEW_COLUMNS = (
    "requirement_id",
    "stable_row_id",
    "rank",
    "ticker",
    "name",
    "sec_cik",
    "decision_timestamp",
    "live_review_as_of",
    "s1_coverage_status",
    "baseline_warning_count",
    "baseline_source_ids",
    "deterministic_action",
    "review_status",
    "human_review_required",
    "primary_documents_retrieved",
    "llm_summary_allowed",
    "llm_summary_status",
    "external_collection_status",
    "external_request_count",
)

REQUEST_PLAN_COLUMNS = (
    "request_id",
    "request_action",
    "review_mode",
    "rank",
    "ticker",
    "name",
    "sec_cik",
    "source",
    "endpoint",
    "request_url",
    "form",
    "items",
    "filing_date",
    "source_publication_time",
    "accession_number",
    "primary_document",
    "date_range_start",
    "date_range_end",
    "destination_path",
    "expected_payload_type",
    "approval_required",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_id(prefix: str, *values: object) -> str:
    text = "\x1f".join("" if value is None else str(value) for value in values)
    return f"{prefix}:{hashlib.sha256(text.encode()).hexdigest()[:24]}"


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _record(root: Path, path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return str(path.resolve())


def _json_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value is None or pd.isna(value):
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _timestamp(value: object) -> pd.Timestamp | None:
    result = pd.to_datetime(value, utc=True, errors="coerce")
    return None if pd.isna(result) else pd.Timestamp(result)


def _blank(value: object) -> bool:
    return value is None or pd.isna(value) or not str(value).strip()


def _canonical_event_type(event_type: str, event_status: str) -> str:
    if event_type == "merger":
        if event_status == "announced_pending":
            return "pending_acquisition"
        if event_status == "completed":
            return "completed_merger"
    if event_type in {
        "pending_acquisition",
        "completed_merger",
        "bankruptcy",
        "suspension",
        "delisting",
        "registration_termination",
        "exchange_noncompliance",
        "other_material_event",
    }:
        return event_type
    return "other_material_event"


def deterministic_event_decision(
    event: pd.Series | dict[str, Any],
    *,
    as_of_timestamp: object,
    evaluation_retrieved_at: object,
) -> dict[str, Any]:
    """Apply cited effective-time policy without inferring missing facts."""
    item = dict(event)
    as_of = _timestamp(as_of_timestamp)
    evaluation = _timestamp(evaluation_retrieved_at)
    published = _timestamp(item.get("source_published_at"))
    retrieved = _timestamp(item.get("source_retrieved_at"))
    effective = _timestamp(item.get("effective_at"))
    canonical_type = _canonical_event_type(
        str(item.get("event_type") or ""),
        str(item.get("event_status") or ""),
    )

    if (
        as_of is None
        or evaluation is None
        or _blank(item.get("source_id"))
        or _blank(item.get("source_response_sha256"))
    ):
        return {
            "canonical_event_type": canonical_type,
            "action": "unresolved",
            "eligibility_status": "missing_cited_time_or_source",
            "human_review_required": True,
            "reason_code": "missing_cited_time_or_source",
        }
    if published is None:
        return {
            "canonical_event_type": canonical_type,
            "action": "unresolved",
            "eligibility_status": "source_publication_time_unavailable",
            "human_review_required": True,
            "reason_code": "source_publication_time_unavailable",
        }
    if published > as_of:
        return {
            "canonical_event_type": canonical_type,
            "action": "no_action",
            "eligibility_status": "published_after_as_of",
            "human_review_required": False,
            "reason_code": "published_after_as_of",
        }
    if retrieved is None or retrieved > evaluation:
        return {
            "canonical_event_type": canonical_type,
            "action": "unresolved",
            "eligibility_status": "not_retrieved_by_evaluation",
            "human_review_required": True,
            "reason_code": "not_retrieved_by_evaluation",
        }
    if (
        str(item.get("event_status")) == "filing_indicator_unresolved"
        or not _blank(item.get("ambiguity"))
    ):
        return {
            "canonical_event_type": canonical_type,
            "action": "unresolved",
            "eligibility_status": "security_scope_or_terms_unresolved",
            "human_review_required": True,
            "reason_code": "security_scope_or_terms_unresolved",
        }
    if (
        effective is None
        or str(item.get("effective_time_status")) not in {
            "exact",
            "document_exact",
            "provider_exact",
        }
    ):
        return {
            "canonical_event_type": canonical_type,
            "action": "unresolved",
            "eligibility_status": "event_effective_time_unresolved",
            "human_review_required": True,
            "reason_code": "event_effective_time_unresolved",
        }

    status = str(item.get("event_status") or "")
    matching = [
        rule
        for rule in DETERMINISTIC_EVENT_POLICY["rules"]
        if rule["event_type"] == canonical_type
        and status in rule["complete_statuses"]
    ]
    if not matching:
        return {
            "canonical_event_type": canonical_type,
            "action": "unresolved",
            "eligibility_status": "unsupported_event_status",
            "human_review_required": True,
            "reason_code": "unsupported_event_status",
        }
    rule = matching[0]
    if effective > as_of and rule["action"] == "exclude":
        return {
            "canonical_event_type": canonical_type,
            "action": "warn",
            "eligibility_status": "published_before_as_of_effective_after_as_of",
            "human_review_required": True,
            "reason_code": "known_future_effective_event",
        }
    return {
        "canonical_event_type": canonical_type,
        "action": rule["action"],
        "eligibility_status": "decision_eligible_exact_evidence",
        "human_review_required": rule["action"] == "warn",
        "reason_code": f"deterministic_{canonical_type}_{rule['action']}",
    }


def _read_s1_tables(s1_root: Path) -> dict[str, pd.DataFrame]:
    output = {}
    for name, columns in EXPECTED_S1_SCHEMAS.items():
        path = s1_root / f"outputs/primary/{name}.parquet"
        frame = pd.read_parquet(path)
        if tuple(frame.columns) != tuple(columns):
            raise RuntimeError(f"S1 {name} schema drifted")
        output[name] = frame
    required = pd.read_parquet(
        s1_root / "outputs/primary/required_instruments.parquet"
    )
    expected_required = (
        "requirement_id",
        "instrument_role",
        "stable_row_id",
        "sec_cik",
        "ticker",
        "required_start",
        "required_end",
    )
    if tuple(required.columns) != expected_required:
        raise RuntimeError("S1 required-instrument schema drifted")
    output["required_instruments"] = required
    return output


def _copy_s1_raw_evidence(
    s1_root: Path,
    artifact_root: Path,
    raw: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    for item in raw.itertuples(index=False):
        source = s1_root / str(item.ledger_stored_path)
        target_relative = Path("raw/s1") / str(item.ledger_stored_path)
        target = artifact_root / target_relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise RuntimeError(f"E1 raw target already exists: {target}")
        if (
            not source.is_file()
            or source.stat().st_size != int(item.stored_size_bytes)
            or sha256_file(source) != item.stored_sha256
        ):
            raise RuntimeError(f"S1 stored raw evidence mismatch: {source}")
        with gzip.open(source, "rb") as handle:
            payload = handle.read()
        if (
            len(payload) != int(item.response_size_bytes)
            or hashlib.sha256(payload).hexdigest() != item.response_sha256
        ):
            raise RuntimeError(f"S1 response bytes mismatch: {source}")
        shutil.copyfile(source, target)
        if (
            target.stat().st_size != int(item.stored_size_bytes)
            or sha256_file(target) != item.stored_sha256
        ):
            raise RuntimeError(f"E1 copied raw evidence mismatch: {target}")
        row = item._asdict()
        row.update(
            {
                "s1_ledger_path": str(item.ledger_stored_path),
                "e1_evidence_path": target_relative.as_posix(),
                "e1_stored_size_bytes": target.stat().st_size,
                "e1_stored_sha256": sha256_file(target),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _warning_claim(event: pd.Series) -> str:
    published = _timestamp(event.get("source_published_at"))
    published_text = published.isoformat() if published is not None else "unknown"
    return (
        f"SEC submissions index lists form {event.get('form')} accession "
        f"{event.get('accession_number')} for {event.get('issuer_id')}, "
        f"published {published_text}; the index does not establish the exact "
        "security scope, event effective time, or complete terms."
    )


def _warning_row(
    *,
    review_mode: str,
    review_context: str,
    requirement: pd.Series,
    event: pd.Series,
    evidence: pd.Series,
    as_of_timestamp: pd.Timestamp,
    evaluation_retrieved_at: pd.Timestamp,
) -> dict[str, Any]:
    decision = deterministic_event_decision(
        event,
        as_of_timestamp=as_of_timestamp,
        evaluation_retrieved_at=evaluation_retrieved_at,
    )
    evidence_path = str(evidence["e1_evidence_path"])
    evidence_uri = (
        f"{evidence_path}#sha256={event['source_response_sha256']}"
    )
    warning_id = _hash_id(
        "warning",
        review_mode,
        review_context,
        requirement["requirement_id"],
        event["event_id"],
        as_of_timestamp.isoformat(),
    )
    return {
        "warning_id": warning_id,
        "review_mode": review_mode,
        "review_context": review_context,
        "requirement_id": requirement["requirement_id"],
        "stable_row_id": requirement.get("stable_row_id"),
        "ticker": requirement["ticker"],
        "event_id": event["event_id"],
        "source_event_type": event["event_type"],
        "canonical_event_type": decision["canonical_event_type"],
        "source_event_status": event["event_status"],
        "deterministic_action": decision["action"],
        "decision_eligibility_status": decision["eligibility_status"],
        "human_review_required": decision["human_review_required"],
        "as_of_timestamp": as_of_timestamp,
        "event_effective_at": event["effective_at"],
        "source_published_at": event["source_published_at"],
        "source_retrieved_at": event["source_retrieved_at"],
        "source_id": event["source_id"],
        "request_url": evidence["request_url"],
        "source_response_sha256": event["source_response_sha256"],
        "evidence_path": evidence_path,
        "evidence_uri": evidence_uri,
        "accession_number": event["accession_number"],
        "form": event["form"],
        "primary_document": event["primary_document"],
        "claim_text": _warning_claim(event),
        "policy_reason_code": decision["reason_code"],
    }


def _event_candidates(
    coverage_row: pd.Series,
    events: pd.DataFrame,
) -> pd.DataFrame:
    if pd.isna(coverage_row.get("issuer_id")):
        return events.iloc[0:0]
    selected = events[events["issuer_id"].eq(coverage_row["issuer_id"])]
    security_id = coverage_row.get("security_id")
    if pd.notna(security_id):
        selected = selected[selected["security_id"].eq(security_id)]
    return selected


def build_historical_reconciliation(
    holdings: pd.DataFrame,
    coverage: pd.DataFrame,
    events: pd.DataFrame,
    e1_raw: pd.DataFrame,
    *,
    evaluation_retrieved_at: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Reconcile every canonical P4 holding under historical-only clocks."""
    coverage_by_id = coverage.set_index("requirement_id", drop=False)
    evidence_by_id = e1_raw.set_index("source_id", drop=False)
    rows: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for holding in holdings.sort_values(
        ["decision_timestamp", "rank"]
    ).itertuples(index=False):
        requirement_id = f"holding:{holding.stable_row_id}"
        if requirement_id not in coverage_by_id.index:
            raise RuntimeError(f"S1 coverage missing P4 holding {requirement_id}")
        covered = coverage_by_id.loc[requirement_id]
        candidates = _event_candidates(covered, events)
        decision_time = pd.Timestamp(holding.decision_timestamp)
        entry_time = pd.Timestamp(holding.entry_timestamp)
        target_exit = pd.Timestamp(
            getattr(holding, "target_exit_timestamp", pd.NaT)
        )
        if pd.isna(target_exit):
            raise RuntimeError(f"P4 holding missing target exit {requirement_id}")

        published = pd.to_datetime(
            candidates["source_published_at"], utc=True, errors="coerce"
        )
        by_decision = candidates[published.le(decision_time)]
        in_holding = candidates[
            published.gt(decision_time) & published.le(target_exit)
        ]
        for context, as_of, group in (
            ("decision", decision_time, by_decision),
            ("holding_interval", target_exit, in_holding),
        ):
            for _, event in group.iterrows():
                source_id = str(event["source_id"])
                if source_id not in evidence_by_id.index:
                    raise RuntimeError(
                        f"event source missing exact evidence: {source_id}"
                    )
                warnings.append(
                    _warning_row(
                        review_mode="historical",
                        review_context=context,
                        requirement=covered,
                        event=event,
                        evidence=evidence_by_id.loc[source_id],
                        as_of_timestamp=as_of,
                        evaluation_retrieved_at=evaluation_retrieved_at,
                    )
                )

        holding_warnings = [
            warning
            for warning in warnings
            if warning["requirement_id"] == requirement_id
        ]
        decision_actions = [
            warning["deterministic_action"]
            for warning in holding_warnings
            if warning["review_context"] == "decision"
        ]
        if "exclude" in decision_actions:
            action = "exclude"
            eligibility = "decision_eligible_exact_exclusion"
        elif "unresolved" in decision_actions:
            action = "unresolved"
            eligibility = "decision_event_evidence_unresolved"
        elif "warn" in decision_actions:
            action = "warn"
            eligibility = "decision_eligible_exact_warning"
        elif covered["coverage_status"] != "matched":
            action = "unresolved"
            eligibility = "s1_coverage_not_historically_matched"
        else:
            action = "pass"
            eligibility = "no_decision_eligible_material_event"

        reasons = set(_json_list(covered["reason_codes"]))
        reasons.update(
            warning["policy_reason_code"] for warning in holding_warnings
        )
        cited_sources = set(_json_list(covered["source_ids"]))
        cited_sources.update(
            warning["source_id"] for warning in holding_warnings
        )
        effective_count = sum(
            warning["decision_eligibility_status"]
            == "decision_eligible_exact_evidence"
            for warning in holding_warnings
        )
        rows.append(
            {
                "requirement_id": requirement_id,
                "stable_row_id": holding.stable_row_id,
                "rank": int(holding.rank),
                "ticker": holding.ticker,
                "name": holding.name,
                "sec_cik": str(holding.cik).zfill(10),
                "decision_timestamp": decision_time,
                "entry_timestamp": entry_time,
                "target_exit_timestamp": target_exit,
                "s1_coverage_status": covered["coverage_status"],
                "s1_identity_status": covered["identity_status"],
                "s1_listing_status": covered["listing_status"],
                "s1_security_type_status": covered[
                    "security_type_status"
                ],
                "s1_event_status": covered["event_status"],
                "s1_price_adjustment_status": covered[
                    "price_adjustment_status"
                ],
                "source_event_indicator_count": len(candidates),
                "published_by_decision_indicator_count": len(by_decision),
                "holding_window_indicator_count": len(in_holding),
                "decision_effective_event_count": effective_count,
                "deterministic_action": action,
                "decision_eligibility_status": eligibility,
                "historical_evaluation_status": (
                    "unavailable_fail_closed"
                    if action == "unresolved"
                    or covered["coverage_status"] != "matched"
                    else "policy_action_available_no_performance"
                ),
                "human_review_required": action in {"unresolved", "warn"},
                "llm_summary_allowed": False,
                "warning_ids": json.dumps(
                    sorted(
                        warning["warning_id"]
                        for warning in holding_warnings
                    )
                ),
                "cited_source_ids": json.dumps(sorted(cited_sources)),
                "reason_codes": json.dumps(sorted(reasons)),
            }
        )
    historical = pd.DataFrame(rows, columns=HISTORICAL_COLUMNS)
    warning_frame = pd.DataFrame(warnings, columns=WARNING_COLUMNS)
    if len(historical) != 180 or historical["stable_row_id"].nunique() != 180:
        raise RuntimeError("historical reconciliation is not all 180 holdings")
    if historical["llm_summary_allowed"].any():
        raise RuntimeError("historical evaluation cannot allow LLM summaries")
    return historical, warning_frame


def _recent_value(
    recent: dict[str, list[Any]], key: str, index: int
) -> Any:
    values = recent.get(key, [])
    return values[index] if index < len(values) else None


def _is_live_document_candidate(form: str, items: str) -> bool:
    if form in LIVE_PRIMARY_FORMS:
        return True
    item_set = {
        item.strip() for item in str(items or "").split(",") if item.strip()
    }
    return form in {"8-K", "8-K/A"} and bool(
        item_set.intersection(LIVE_8K_ITEMS)
    )


def build_live_review_contract(
    shortlist: pd.DataFrame,
    coverage: pd.DataFrame,
    events: pd.DataFrame,
    e1_raw: pd.DataFrame,
    artifact_root: Path,
    *,
    review_id: str,
    live_as_of: pd.Timestamp,
    evaluation_retrieved_at: pd.Timestamp,
    date_range_start: pd.Timestamp,
    date_range_end: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Build current-shortlist review and exact approval-gated request plan."""
    coverage_by_id = coverage.set_index("requirement_id", drop=False)
    evidence_by_id = e1_raw.set_index("source_id", drop=False)
    live_rows: list[dict[str, Any]] = []
    plan_rows: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    for holding in shortlist.sort_values("rank").itertuples(index=False):
        requirement_id = f"holding:{holding.stable_row_id}"
        covered = coverage_by_id.loc[requirement_id]
        candidates = _event_candidates(covered, events)
        published = pd.to_datetime(
            candidates["source_published_at"], utc=True, errors="coerce"
        )
        candidates = candidates[published.le(live_as_of)]
        for _, event in candidates.iterrows():
            source_id = str(event["source_id"])
            warnings.append(
                _warning_row(
                    review_mode="live",
                    review_context="current_shortlist",
                    requirement=covered,
                    event=event,
                    evidence=evidence_by_id.loc[source_id],
                    as_of_timestamp=live_as_of,
                    evaluation_retrieved_at=evaluation_retrieved_at,
                )
            )

        source_ids = _json_list(covered["source_ids"])
        submission_sources = [
            source
            for source in source_ids
            if source.startswith("submission_")
            and source in evidence_by_id.index
        ]
        if len(submission_sources) != 1:
            raise RuntimeError(
                f"current shortlist requires one S1 submission: {holding.ticker}"
            )
        source = evidence_by_id.loc[submission_sources[0]]
        source_path = artifact_root / str(source["e1_evidence_path"])
        with gzip.open(source_path, "rb") as handle:
            document = json.loads(handle.read())
        recent = document.get("filings", {}).get("recent", {})
        for index, form in enumerate(recent.get("form", [])):
            filed = _timestamp(_recent_value(recent, "filingDate", index))
            items = str(_recent_value(recent, "items", index) or "")
            if (
                filed is None
                or filed < date_range_start
                or filed > date_range_end
                or not _is_live_document_candidate(str(form), items)
            ):
                continue
            accession = str(
                _recent_value(recent, "accessionNumber", index) or ""
            )
            primary_document = str(
                _recent_value(recent, "primaryDocument", index) or ""
            )
            if not accession or not primary_document:
                continue
            accession_flat = accession.replace("-", "")
            archive_cik = str(int(str(holding.cik)))
            url = (
                "https://www.sec.gov/Archives/edgar/data/"
                f"{archive_cik}/{accession_flat}/{primary_document}"
            )
            suffix = Path(primary_document).suffix.lower()
            payload_type = (
                "application/xml"
                if suffix == ".xml"
                else "text/html_or_plain_text"
            )
            destination = (
                Path("live/collection")
                / review_id
                / "raw/sec/filings"
                / str(holding.cik).zfill(10)
                / accession_flat
                / f"{Path(primary_document).name}.gz"
            )
            plan_rows.append(
                {
                    "request_id": _hash_id("request", url),
                    "request_action": (
                        "external_request_requires_explicit_approval"
                    ),
                    "review_mode": "live",
                    "rank": int(holding.rank),
                    "ticker": holding.ticker,
                    "name": holding.name,
                    "sec_cik": str(holding.cik).zfill(10),
                    "source": "sec_edgar",
                    "endpoint": "sec_archives_primary_filing_document",
                    "request_url": url,
                    "form": str(form),
                    "items": items,
                    "filing_date": filed,
                    "source_publication_time": _recent_value(
                        recent, "acceptanceDateTime", index
                    )
                    or _recent_value(recent, "filingDate", index),
                    "accession_number": accession,
                    "primary_document": primary_document,
                    "date_range_start": date_range_start,
                    "date_range_end": date_range_end,
                    "destination_path": destination.as_posix(),
                    "expected_payload_type": payload_type,
                    "approval_required": True,
                }
            )

        holding_warnings = [
            warning
            for warning in warnings
            if warning["requirement_id"] == requirement_id
        ]
        external_count = sum(
            row["ticker"] == holding.ticker for row in plan_rows
        )
        live_rows.append(
            {
                "requirement_id": requirement_id,
                "stable_row_id": holding.stable_row_id,
                "rank": int(holding.rank),
                "ticker": holding.ticker,
                "name": holding.name,
                "sec_cik": str(holding.cik).zfill(10),
                "decision_timestamp": holding.decision_timestamp,
                "live_review_as_of": live_as_of,
                "s1_coverage_status": covered["coverage_status"],
                "baseline_warning_count": len(holding_warnings),
                "baseline_source_ids": json.dumps(
                    sorted(
                        set(source_ids).union(
                            warning["source_id"]
                            for warning in holding_warnings
                        )
                    )
                ),
                "deterministic_action": "unresolved",
                "review_status": "unresolved_human_review_required",
                "human_review_required": True,
                "primary_documents_retrieved": 0,
                "llm_summary_allowed": False,
                "llm_summary_status": (
                    "prohibited_until_primary_documents_are_retrieved_cited"
                ),
                "external_collection_status": (
                    "not_requested_requires_explicit_approval"
                ),
                "external_request_count": external_count,
            }
        )

    live = pd.DataFrame(live_rows, columns=LIVE_REVIEW_COLUMNS)
    plan = pd.DataFrame(plan_rows, columns=REQUEST_PLAN_COLUMNS)
    warning_frame = pd.DataFrame(warnings, columns=WARNING_COLUMNS)
    if len(live) != 15 or live["stable_row_id"].nunique() != 15:
        raise RuntimeError("live review contract is not the current 15 names")
    if live["llm_summary_allowed"].any():
        raise RuntimeError("LLM summary allowed before primary evidence")
    if plan["request_url"].duplicated().any():
        raise RuntimeError("live request plan contains duplicate URLs")

    actual_forms = sorted(set(plan["form"]))
    instruments = [
        {
            "rank": int(row.rank),
            "ticker": row.ticker,
            "name": row.name,
            "sec_cik": str(row.cik).zfill(10),
        }
        for row in shortlist.sort_values("rank").itertuples(index=False)
    ]
    approval_token = f"APPROVE-{review_id}-LIVE-SEC-PRIMARY-DOCUMENTS"
    collection_contract = {
        "schema_version": 1,
        "collection_id": review_id,
        "collection_mode": "live",
        "collection_status": "not_requested_requires_explicit_approval",
        "historical_or_live": "live",
        "source": "SEC EDGAR",
        "provider_adapter": "sec_edgar",
        "existing_evidence_reused": {
            "source": "S1 exact SEC submissions responses",
            "submission_requests_reused": 15,
            "new_submission_requests_proposed": 0,
        },
        "proposed_endpoints": [
            {
                "name": "SEC submissions",
                "url_template": (
                    "https://data.sec.gov/submissions/CIK##########.json"
                ),
                "request_action": "reuse_exact_S1_bytes_no_request",
                "request_count": 0,
            },
            {
                "name": "SEC Archives primary filing document",
                "url_template": (
                    "https://www.sec.gov/Archives/edgar/data/"
                    "{cik_without_zero_padding}/{accession_without_dashes}/"
                    "{primary_document}"
                ),
                "request_action": (
                    "external_request_requires_explicit_approval"
                ),
                "request_count": len(plan),
                "exact_urls_in": "live/collection_request_plan.parquet",
            },
        ],
        "instruments": instruments,
        "selection_forms": actual_forms,
        "selection_8k_items": list(LIVE_8K_ITEMS),
        "date_range": {
            "start_inclusive": date_range_start.isoformat(),
            "end_inclusive": date_range_end.isoformat(),
        },
        "maximum_external_request_count": len(plan),
        "versioned_non_overwriting_destination": (
            _display_path(artifact_root)
            + f"/live/collection/{review_id}/raw/sec/filings/"
        ),
        "expected_raw_payloads": {
            "count": len(plan),
            "types": sorted(set(plan["expected_payload_type"])),
            "storage": "exact response bytes plus deterministic gzip",
            "request_metadata": (
                "URL, request/retrieval timestamps, HTTP status, response "
                "headers, byte sizes, SHA-256 values, and lineage"
            ),
            "approximate_scope": (
                f"{len(plan)} primary filing documents; fail closed above "
                "100 MiB total or 10 MiB for one response"
            ),
        },
        "approval_token_after_explicit_user_authorization": approval_token,
        "scheduling": False,
        "automatic_collection": False,
    }
    return live, warning_frame, plan, collection_contract


def validate_summary_claims(
    claims: pd.DataFrame,
    raw_evidence: pd.DataFrame,
    *,
    review_mode: str,
) -> None:
    """Validate an optional LLM output as cited summary only, never policy."""
    if review_mode != "live":
        raise ValueError("LLM summaries are prohibited in historical mode")
    required = {
        "claim_id",
        "claim_text",
        "source_id",
        "source_response_sha256",
        "evidence_path",
        "evidence_locator",
    }
    if not required.issubset(claims.columns):
        raise ValueError("summary claim schema is incomplete")
    if claims.empty:
        return
    evidence = raw_evidence.set_index("source_id", drop=False)
    for row in claims.itertuples(index=False):
        if (
            _blank(row.claim_id)
            or _blank(row.claim_text)
            or _blank(row.source_id)
            or _blank(row.evidence_locator)
        ):
            raise ValueError("every summary claim requires cited evidence")
        if row.source_id not in evidence.index:
            raise ValueError(f"unknown summary source_id: {row.source_id}")
        source = evidence.loc[row.source_id]
        expected_hash = (
            source["response_sha256"]
            if "response_sha256" in source
            else source["source_response_sha256"]
        )
        if str(row.source_response_sha256) != str(expected_hash):
            raise ValueError("summary citation hash mismatch")
        known_paths = {
            str(source.get("e1_evidence_path", "")),
            str(source.get("ledger_stored_path", "")),
            str(source.get("collection_path", "")),
        }
        if str(row.evidence_path) not in known_paths:
            raise ValueError("summary citation path mismatch")


def _render_report(
    historical: pd.DataFrame,
    historical_warnings: pd.DataFrame,
    live: pd.DataFrame,
    live_warnings: pd.DataFrame,
    collection: dict[str, Any],
) -> str:
    historical_counts = historical["deterministic_action"].value_counts()
    live_tickers = ", ".join(live.sort_values("rank")["ticker"])
    forms = ", ".join(collection["selection_forms"]) or "none"
    return f"""# E1 Historical Then Live Event/M&A Review

## Outcome

E1 applies deterministic, effective-time-aware event policy to the exact S1
ledger without upgrading any S1 ambiguity or confidence state. It reconciles
all {len(historical)} canonical P4 holdings and the current {len(live)}-name
shortlist. No performance is calculated or claimed.

Historical action counts: pass={int(historical_counts.get('pass', 0))},
warn={int(historical_counts.get('warn', 0))},
exclude={int(historical_counts.get('exclude', 0))}, and
unresolved={int(historical_counts.get('unresolved', 0))}. There are
{len(historical_warnings)} dated historical filing-indicator warnings. Every
warning row contains the source publication time, retrieval time, request URL,
exact response hash, evidence path, and a hash-addressed evidence URI.

Historical LLM summaries are prohibited. S1 filing indicators with unresolved
effective time or security scope remain unresolved and cannot become an event,
warning of a completed outcome, backtest action, or return.

## Live current-shortlist contract

Tickers: {live_tickers}.

All 15 names route to cited human review. LLM summary is prohibited until exact
primary documents have been retrieved, preserved, and cited; an LLM may then
summarize only those documents and cannot change the deterministic action.
Current baseline warnings: {len(live_warnings)}.

No external request was made. The approval-gated plan reuses all 15 exact S1
submission responses and proposes at most
{collection['maximum_external_request_count']} SEC Archives primary-document
requests. Proposed forms: {forms}. The inclusive date range is
{collection['date_range']['start_inclusive']} through
{collection['date_range']['end_inclusive']}. The exact URLs, instruments, CIKs,
forms, filing dates, destinations, and payload types are frozen in
`live/collection_request_plan.parquet`.

## Prohibitions

- Do not infer an event from a filing family, missing price, ticker
  disappearance, name similarity, model knowledge, or uncited narrative.
- Do not feed an unresolved E1 record to the canonical backtest as a resolved
  corporate action.
- Do not use current model knowledge in historical evaluation.
- Do not calculate or claim official performance from this artifact.
"""


def _validate_preservation(
    d1_review_path: Path,
) -> dict[str, Any]:
    document = json.loads(d1_review_path.read_text())
    preservation = document["preservation"]
    verified: dict[str, Any] = {
        "canonical_manifests": {},
        "source_evidence": {},
        "tracked_legacy_and_international": {},
        "pointer_revisions": {},
    }
    for group in (
        "canonical_manifests",
        "source_evidence",
        "tracked_legacy_and_international",
    ):
        for relative, expected in preservation[group].items():
            path = ROOT / relative
            actual = sha256_file(path)
            if actual != expected:
                raise RuntimeError(f"preservation hash mismatch: {relative}")
            verified[group][relative] = actual
    for name, expected in preservation["pointer_revisions"].items():
        path = ROOT / "data_io/canonical_artifact_pointers" / name
        pointer = json.loads(path.read_text())
        revision = pointer.get("revision")
        if revision != expected:
            raise RuntimeError(f"pointer revision mismatch: {name}")
        verified["pointer_revisions"][name] = revision
    return verified


def build_event_review(
    artifact_root: Path,
    *,
    review_id: str,
    s1_root: Path = DEFAULT_S1_ROOT,
    d1_root: Path = DEFAULT_D1_ROOT,
    p4_root: Path = DEFAULT_P4_ROOT,
    live_window_start: str = LIVE_WINDOW_START,
    live_window_end: str = LIVE_WINDOW_END,
) -> Path:
    """Build one non-overwriting E1 artifact without an external request."""
    if not REVIEW_ID_PATTERN.fullmatch(review_id):
        raise ValueError("review_id must be an immutable UTC identifier")
    artifact_root = artifact_root.resolve()
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"E1 target is not empty: {artifact_root}")
    artifact_root.mkdir(parents=True, exist_ok=True)

    pinned_paths = {
        **PINNED_MANIFESTS,
        "p4_manifest": (
            p4_root / "manifest.json",
            PINNED_MANIFESTS["p4_manifest"][1],
        ),
        "d1_review_manifest": (
            d1_root / "review/review_manifest.json",
            PINNED_MANIFESTS["d1_review_manifest"][1],
        ),
        "d1_p2_candidate_manifest": (
            d1_root / "p2_review_candidate/manifest.json",
            PINNED_MANIFESTS["d1_p2_candidate_manifest"][1],
        ),
        "s1_manifest": (
            s1_root / "manifest.json",
            PINNED_MANIFESTS["s1_manifest"][1],
        ),
    }
    validated_inputs = []
    for name, (path, expected) in pinned_paths.items():
        actual = sha256_file(path)
        if actual != expected:
            raise RuntimeError(f"{name} hash mismatch")
        validated_inputs.append(
            {
                "name": name,
                "path": _display_path(path),
                "size_bytes": path.stat().st_size,
                "sha256": actual,
            }
        )
    preservation_before = _validate_preservation(
        d1_root / "review/review_manifest.json"
    )

    tables = _read_s1_tables(s1_root)
    e1_raw = _copy_s1_raw_evidence(
        s1_root, artifact_root, tables["raw_evidence"]
    )
    retrievals = pd.to_datetime(
        e1_raw["source_retrieved_at"], utc=True, errors="coerce"
    )
    if retrievals.isna().any():
        raise RuntimeError("S1 raw evidence has missing retrieval times")
    evaluation_retrieved_at = retrievals.max()

    holdings = pd.read_parquet(p4_root / "outputs/holdings.parquet")
    plan = pd.read_parquet(p4_root / "outputs/backtest_vintage_plan.parquet")
    holdings = holdings.merge(
        plan[["stable_row_id", "target_exit_timestamp"]],
        on="stable_row_id",
        how="left",
        validate="one_to_one",
    )
    shortlist = pd.read_parquet(p4_root / "outputs/latest_shortlist.parquet")
    historical, historical_warnings = build_historical_reconciliation(
        holdings,
        tables["coverage"],
        tables["events"],
        e1_raw,
        evaluation_retrieved_at=evaluation_retrieved_at,
    )
    start = pd.Timestamp(live_window_start)
    end = pd.Timestamp(live_window_end)
    if start.tzinfo is None or end.tzinfo is None or start > end:
        raise ValueError("live evidence window must be ordered and timezone-aware")
    live_as_of = min(end, evaluation_retrieved_at)
    live, live_warnings, request_plan, collection_contract = (
        build_live_review_contract(
            shortlist,
            tables["coverage"],
            tables["events"],
            e1_raw,
            artifact_root,
            review_id=review_id,
            live_as_of=live_as_of,
            evaluation_retrieved_at=evaluation_retrieved_at,
            date_range_start=start,
            date_range_end=end,
        )
    )

    outputs: list[tuple[Path, str]] = []

    def write_parquet(
        relative: str, frame: pd.DataFrame, role: str
    ) -> Path:
        path = artifact_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        outputs.append((path, role))
        return path

    write_parquet(
        "outputs/historical/reconciliation.parquet",
        historical,
        "historical_p4_event_reconciliation",
    )
    write_parquet(
        "outputs/historical/warnings.parquet",
        historical_warnings,
        "historical_dated_cited_warnings",
    )
    write_parquet(
        "outputs/live/review_contract.parquet",
        live,
        "live_current_shortlist_review_contract",
    )
    write_parquet(
        "outputs/live/warnings.parquet",
        live_warnings,
        "live_dated_cited_warnings",
    )
    write_parquet(
        "outputs/source_evidence.parquet",
        e1_raw,
        "exact_s1_source_evidence_lineage",
    )
    write_parquet(
        "live/collection_request_plan.parquet",
        request_plan,
        "approval_gated_live_request_plan",
    )

    policy_path = artifact_root / "contracts/deterministic_event_policy.json"
    _write_json(policy_path, DETERMINISTIC_EVENT_POLICY)
    outputs.append((policy_path, "deterministic_event_policy"))
    time_path = artifact_root / "contracts/time_semantics.json"
    _write_json(
        time_path,
        {
            "schema_version": 1,
            "event_effective_time": (
                "when the exact security state changes; required for a "
                "resolved deterministic event action"
            ),
            "source_publication_time": (
                "when evidence became public; the historical knowledge clock"
            ),
            "source_retrieval_time": (
                "when exact bytes were obtained; lineage only and not a "
                "substitute for historical publication time"
            ),
            "decision_eligibility_time": (
                "the P4 decision timestamp; evidence published later cannot "
                "change the historical selection action"
            ),
            "holding_interval": (
                "post-decision evidence through target exit is reviewed "
                "separately and never back-propagated into selection"
            ),
        },
    )
    outputs.append((time_path, "four_axis_time_contract"))
    summary_path = artifact_root / "contracts/llm_summary_contract.json"
    _write_json(
        summary_path,
        {
            "schema_version": 1,
            "historical_summary_allowed": False,
            "live_summary_allowed_only_after": [
                "exact source document retrieved",
                "exact bytes and hashes preserved",
                "each claim supplies source_id, hash, path, and locator",
                "deterministic policy action already frozen",
            ],
            "llm_role": "summary_only",
            "llm_may_not": [
                "discover an event from model knowledge",
                "change warn/exclude/unresolved action",
                "make an uncited claim",
                "infer from a filing family, missing price, disappearance, or name",
            ],
            "claim_schema": [
                "claim_id",
                "claim_text",
                "source_id",
                "source_response_sha256",
                "evidence_path",
                "evidence_locator",
            ],
        },
    )
    outputs.append((summary_path, "cited_llm_summary_contract"))
    collection_path = artifact_root / "live/collection_contract.json"
    _write_json(collection_path, collection_contract)
    outputs.append((collection_path, "explicit_collection_approval_contract"))

    summary = {
        "schema_version": 1,
        "historical_holdings": len(historical),
        "historical_action_counts": {
            key: int(value)
            for key, value in historical[
                "deterministic_action"
            ].value_counts().items()
        },
        "historical_warnings": len(historical_warnings),
        "historical_llm_summaries_allowed": False,
        "live_shortlist_rows": len(live),
        "live_warnings": len(live_warnings),
        "live_human_review_required": int(
            live["human_review_required"].sum()
        ),
        "live_llm_summaries_allowed_now": False,
        "maximum_external_request_count": len(request_plan),
        "external_requests_made": 0,
        "performance_calculated": False,
        "official_performance_claimed": False,
        "backtest_consumption_changed": False,
        "p3_p4_consumption_changed": False,
    }
    summary_output = artifact_root / "support/review_summary.json"
    _write_json(summary_output, summary)
    outputs.append((summary_output, "event_review_summary"))
    report_path = artifact_root / "report/event_review_report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        _render_report(
            historical,
            historical_warnings,
            live,
            live_warnings,
            collection_contract,
        )
    )
    outputs.append((report_path, "cited_event_review_report"))

    preservation_after = _validate_preservation(
        d1_root / "review/review_manifest.json"
    )
    if preservation_before != preservation_after:
        raise RuntimeError("canonical/legacy preservation changed during E1")
    raw_paths = sorted(
        path for path in (artifact_root / "raw").rglob("*") if path.is_file()
    )
    code_lineage = []
    for path in (
        ROOT / "portfolio/event_review.py",
        ROOT / "workflows/build_event_review.py",
        ROOT / "workflows/collect_live_event_evidence.py",
    ):
        if path.is_file():
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
            "VERSIONED_HISTORICAL_THEN_LIVE_EVENT_REVIEW"
        ),
        "review_id": review_id,
        "created_at_utc": utc_now(),
        "build_mode": "offline_from_exact_s1_evidence",
        "provider_adapters": ["sec_edgar", "future_provider_neutral"],
        "validated_inputs": validated_inputs,
        "preservation": preservation_after,
        "code_lineage": code_lineage,
        "records": [
            *[_record(artifact_root, path, role) for path, role in outputs],
            *[
                _record(
                    artifact_root, path, "preserved_exact_s1_raw_response"
                )
                for path in raw_paths
            ],
        ],
        "historical": {
            "holding_rows": len(historical),
            "warning_rows": len(historical_warnings),
            "llm_summary_allowed": False,
            "current_model_knowledge_allowed": False,
        },
        "live": {
            "shortlist_rows": len(live),
            "warning_rows": len(live_warnings),
            "human_review_rows": int(live["human_review_required"].sum()),
            "primary_document_requests_proposed": len(request_plan),
            "external_requests_made": 0,
            "llm_summary_allowed_now": False,
        },
        "claim": {
            "e1_contract_implemented": True,
            "deterministic_rules_precede_summary": True,
            "every_warning_has_dated_citation": True,
            "s1_ambiguity_preserved": True,
            "historical_effective_event_coverage_complete": False,
            "live_primary_document_collection_complete": False,
            "performance_calculated": False,
            "official_performance_available": False,
            "pinned_p2_p4_mutated": False,
            "d1_promoted": False,
            "external_request_made": False,
        },
    }
    manifest_path = artifact_root / "manifest.json"
    _write_json(manifest_path, manifest)
    return manifest_path


def verify_event_review_artifact(artifact_root: Path) -> dict[str, Any]:
    """Independently rehash E1 records, citations, and raw payload bytes."""
    manifest_path = artifact_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    failures = []
    for item in manifest["records"]:
        path = artifact_root / item["path"]
        if (
            not path.is_file()
            or path.stat().st_size != int(item["size_bytes"])
            or sha256_file(path) != item["sha256"]
        ):
            failures.append(item["path"])
    raw = pd.read_parquet(artifact_root / "outputs/source_evidence.parquet")
    raw_failures = []
    for item in raw.itertuples(index=False):
        path = artifact_root / str(item.e1_evidence_path)
        if (
            not path.is_file()
            or path.stat().st_size != int(item.e1_stored_size_bytes)
            or sha256_file(path) != item.e1_stored_sha256
        ):
            raw_failures.append(str(item.source_id))
            continue
        with gzip.open(path, "rb") as handle:
            payload = handle.read()
        if (
            len(payload) != int(item.response_size_bytes)
            or hashlib.sha256(payload).hexdigest() != item.response_sha256
        ):
            raw_failures.append(str(item.source_id))

    evidence = raw.set_index("source_id", drop=False)
    citation_failures = []
    warning_paths = (
        "outputs/historical/warnings.parquet",
        "outputs/live/warnings.parquet",
    )
    warning_count = 0
    for relative in warning_paths:
        warnings = pd.read_parquet(artifact_root / relative)
        warning_count += len(warnings)
        for warning in warnings.itertuples(index=False):
            if warning.source_id not in evidence.index:
                citation_failures.append(str(warning.warning_id))
                continue
            source = evidence.loc[warning.source_id]
            if (
                str(warning.source_response_sha256)
                != str(source["response_sha256"])
                or str(warning.evidence_path)
                != str(source["e1_evidence_path"])
                or _blank(warning.claim_text)
                or _timestamp(warning.source_published_at) is None
            ):
                citation_failures.append(str(warning.warning_id))
    historical = pd.read_parquet(
        artifact_root / "outputs/historical/reconciliation.parquet"
    )
    live = pd.read_parquet(
        artifact_root / "outputs/live/review_contract.parquet"
    )
    if (
        failures
        or raw_failures
        or citation_failures
        or len(historical) != 180
        or len(live) != 15
        or historical["llm_summary_allowed"].any()
        or live["llm_summary_allowed"].any()
        or manifest["claim"]["performance_calculated"]
    ):
        raise RuntimeError(
            "E1 verification failed: "
            f"records={failures}, raw={raw_failures}, "
            f"citations={citation_failures}"
        )
    return {
        "manifest_sha256": sha256_file(manifest_path),
        "records_verified": len(manifest["records"]),
        "raw_responses_verified": len(raw),
        "warnings_verified": warning_count,
        "historical_rows": len(historical),
        "live_rows": len(live),
        "performance_calculated": False,
    }


class LiveFetcher(Protocol):
    def __call__(
        self, url: str, headers: dict[str, str]
    ) -> tuple[int, dict[str, str], bytes]:
        """Return HTTP status, response headers, and exact body bytes."""


def _http_fetch(
    url: str, headers: dict[str, str]
) -> tuple[int, dict[str, str], bytes]:
    request = urllib.request.Request(url, headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=60)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return (
            int(response.status),
            dict(response.headers.items()),
            response.read(LIVE_RESPONSE_LIMIT_BYTES + 1),
        )


def _validate_frozen_collection_source(
    artifact_root: Path,
    *,
    expected_manifest_sha256: str | None,
    expected_plan_sha256: str | None,
) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any]]:
    contract_path = artifact_root / "live/collection_contract.json"
    plan_path = artifact_root / "live/collection_request_plan.parquet"
    manifest_path = artifact_root / "manifest.json"
    if not contract_path.is_file() or not plan_path.is_file():
        raise RuntimeError("frozen E1 collection source is incomplete")
    contract = json.loads(contract_path.read_text())
    plan = pd.read_parquet(plan_path)
    source_hashes = {
        "manifest_sha256": (
            sha256_file(manifest_path) if manifest_path.is_file() else None
        ),
        "plan_sha256": sha256_file(plan_path),
        "contract_sha256": sha256_file(contract_path),
    }
    if (
        expected_manifest_sha256 is not None
        and source_hashes["manifest_sha256"] != expected_manifest_sha256
    ):
        raise RuntimeError("frozen E1 manifest hash drifted")
    if (
        expected_plan_sha256 is not None
        and source_hashes["plan_sha256"] != expected_plan_sha256
    ):
        raise RuntimeError("frozen E1 request plan hash drifted")
    if len(plan) != int(contract["maximum_external_request_count"]):
        raise RuntimeError("live collection plan count drifted")
    if len(plan) > 47 or plan["request_url"].duplicated().any():
        raise RuntimeError("live collection request bound drifted")
    if "approval_required" in plan and not plan["approval_required"].all():
        raise RuntimeError("live collection approval boundary drifted")
    allowed_forms = set(contract.get("selection_forms", plan["form"].unique()))
    if not set(plan["form"]).issubset(allowed_forms):
        raise RuntimeError("live collection form scope drifted")
    allowed_ciks = {
        str(item["sec_cik"]).zfill(10)
        for item in contract.get("instruments", [])
    }
    if allowed_ciks and not set(plan["sec_cik"]).issubset(allowed_ciks):
        raise RuntimeError("live collection instrument scope drifted")
    start = _timestamp(contract.get("date_range", {}).get("start_inclusive"))
    end = _timestamp(contract.get("date_range", {}).get("end_inclusive"))
    dates = (
        pd.to_datetime(plan["filing_date"], utc=True, errors="coerce")
        if "filing_date" in plan
        else pd.Series(dtype="datetime64[ns, UTC]")
    )
    if (
        start is not None
        and end is not None
        and (
            dates.empty
            or dates.isna().any()
            or (dates < start).any()
            or (dates > end).any()
        )
    ):
        raise RuntimeError("live collection date scope drifted")
    for url in plan["request_url"]:
        parsed = urllib.parse.urlsplit(str(url))
        if (
            parsed.scheme != "https"
            or parsed.netloc != "www.sec.gov"
            or not parsed.path.startswith("/Archives/edgar/data/")
            or parsed.query
            or parsed.fragment
        ):
            raise RuntimeError(f"unapproved live collection URL: {url}")
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text())
        records = {
            item["path"]: item for item in manifest.get("records", [])
        }
        plan_record = records.get("live/collection_request_plan.parquet")
        if (
            plan_record is None
            or plan_record["sha256"] != source_hashes["plan_sha256"]
            or int(plan_record["size_bytes"]) != plan_path.stat().st_size
        ):
            raise RuntimeError("E1 manifest does not pin the request plan")
    return contract, plan, source_hashes


def _collection_payload_status(
    expected_type: str,
    response_headers: dict[str, str],
    payload: bytes,
) -> str:
    content_type = str(
        response_headers.get("Content-Type")
        or response_headers.get("content-type")
        or ""
    ).lower()
    prefix = payload[:4096].lstrip().lower()
    if expected_type == "application/xml":
        valid = (
            "xml" in content_type
            or prefix.startswith(b"<?xml")
            or prefix.startswith(b"<")
        )
    else:
        valid = (
            content_type.startswith("text/")
            or "html" in content_type
            or "xml" in content_type
            or (b"\x00" not in payload[:4096] and bool(payload))
        )
    return "expected_text_or_xml" if valid else "unexpected_payload_type"


def _collection_target_path(
    collection_root: Path,
    item: Any,
    *,
    legacy_destination: bool,
    source_artifact_root: Path,
) -> tuple[Path, str]:
    if legacy_destination:
        relative = str(item.destination_path)
        return source_artifact_root / relative, relative
    accession = str(item.accession_number).replace("-", "")
    relative = (
        Path("raw/sec/filings")
        / str(item.sec_cik).zfill(10)
        / accession
        / f"{Path(str(item.primary_document)).name}.gz"
    ).as_posix()
    return collection_root / relative, relative


def _append_request_log(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(item, sort_keys=True) + "\n")
        handle.flush()


def _build_collection_reconciliation(
    source_artifact_root: Path,
    collection_root: Path,
    plan: pd.DataFrame,
    request_manifest: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    records = {
        str(item["request_id"]): item
        for item in request_manifest["records"]
    }
    evaluation = max(
        (
            _timestamp(item["retrieved_at_utc"])
            for item in records.values()
            if _timestamp(item["retrieved_at_utc"]) is not None
        ),
        default=pd.Timestamp.now(tz="UTC"),
    )
    rows = []
    for item in plan.itertuples(index=False):
        response = records.get(str(item.request_id))
        success = bool(
            response
            and int(response["http_status"]) == 200
            and response["payload_validation_status"]
            == "expected_text_or_xml"
        )
        event = {
            "event_type": "other_material_event",
            "event_status": "filing_indicator_unresolved",
            "effective_at": None,
            "effective_time_status": "not_deterministically_extracted",
            "source_published_at": item.source_publication_time,
            "source_retrieved_at": (
                response["retrieved_at_utc"] if response else None
            ),
            "source_id": (
                f"sec_primary_document:{item.request_id}" if success else None
            ),
            "source_response_sha256": (
                response["response_sha256"] if success else None
            ),
            "ambiguity": (
                "primary_document_requires_exact_security_terms_and_"
                "effective_time_human_review"
            ),
        }
        decision = deterministic_event_decision(
            event,
            as_of_timestamp=evaluation,
            evaluation_retrieved_at=evaluation,
        )
        rows.append(
            {
                "request_id": item.request_id,
                "ticker": item.ticker,
                "sec_cik": item.sec_cik,
                "form": item.form,
                "items": item.items,
                "filing_date": item.filing_date,
                "accession_number": item.accession_number,
                "primary_document": item.primary_document,
                "request_url": item.request_url,
                "source_publication_time": item.source_publication_time,
                "request_status": (
                    "retrieved_expected_payload"
                    if success
                    else (
                        "request_not_attempted"
                        if response is None
                        else "retrieved_not_eligible"
                    )
                ),
                "http_status": (
                    int(response["http_status"]) if response else pd.NA
                ),
                "source_id": event["source_id"],
                "source_response_sha256": event[
                    "source_response_sha256"
                ],
                "evidence_path": (
                    response["stored_path"] if response else None
                ),
                "event_status": event["event_status"],
                "effective_at": None,
                "effective_time_status": event["effective_time_status"],
                "exact_security_scope_status": "not_deterministically_extracted",
                "deterministic_action": decision["action"],
                "decision_eligibility_status": decision[
                    "eligibility_status"
                ],
                "human_review_required": decision[
                    "human_review_required"
                ],
                "policy_reason_code": decision["reason_code"],
                "unsupported_inferences_applied": False,
                "summary_created": False,
            }
        )
    reconciliation = pd.DataFrame(rows)

    live = pd.read_parquet(
        source_artifact_root / "outputs/live/review_contract.parquet"
    )
    expected_counts = plan.groupby("ticker").size().to_dict()
    successful_counts = (
        reconciliation[
            reconciliation["request_status"].eq(
                "retrieved_expected_payload"
            )
        ]
        .groupby("ticker")
        .size()
        .to_dict()
    )
    live["primary_documents_retrieved"] = live["ticker"].map(
        successful_counts
    ).fillna(0).astype(int)
    live["deterministic_action"] = "unresolved"
    live["review_status"] = "unresolved_human_review_required"
    live["human_review_required"] = True
    live["external_collection_status"] = [
        (
            "complete_no_primary_document_candidates"
            if int(expected_counts.get(ticker, 0)) == 0
            else (
                "complete_primary_documents_retrieved"
                if int(successful_counts.get(ticker, 0))
                == int(expected_counts.get(ticker, 0))
                else "incomplete_primary_document_collection"
            )
        )
        for ticker in live["ticker"]
    ]
    live["llm_summary_allowed"] = [
        int(successful_counts.get(ticker, 0)) > 0
        and int(successful_counts.get(ticker, 0))
        == int(expected_counts.get(ticker, 0))
        for ticker in live["ticker"]
    ]
    live["llm_summary_status"] = [
        (
            "allowed_only_with_claim_level_retrieved_evidence_citations"
            if allowed
            else "prohibited_without_complete_retrieved_evidence"
        )
        for allowed in live["llm_summary_allowed"]
    ]
    claims = pd.DataFrame(
        columns=[
            "claim_id",
            "claim_text",
            "source_id",
            "source_response_sha256",
            "evidence_path",
            "evidence_locator",
        ]
    )
    return reconciliation, live, claims


def _finalize_collection_artifact(
    source_artifact_root: Path,
    collection_root: Path,
    collection_id: str,
    contract: dict[str, Any],
    plan: pd.DataFrame,
    source_hashes: dict[str, Any],
    request_manifest_path: Path,
) -> Path:
    request_manifest = json.loads(request_manifest_path.read_text())
    reconciliation, live, claims = _build_collection_reconciliation(
        source_artifact_root,
        collection_root,
        plan,
        request_manifest,
    )
    outputs: list[tuple[Path, str]] = []

    def write_parquet(relative: str, frame: pd.DataFrame, role: str) -> Path:
        path = collection_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        outputs.append((path, role))
        return path

    write_parquet(
        "outputs/live/document_reconciliation.parquet",
        reconciliation,
        "deterministic_primary_document_reconciliation",
    )
    write_parquet(
        "outputs/live/review_contract.parquet",
        live,
        "updated_live_evidence_review_contract",
    )
    claims_path = write_parquet(
        "outputs/live/summary_claims.parquet",
        claims,
        "claim_level_cited_live_summaries",
    )
    evidence = reconciliation[
        reconciliation["request_status"].eq("retrieved_expected_payload")
    ].rename(columns={"evidence_path": "collection_path"})
    validate_summary_claims(claims, evidence, review_mode="live")

    source_dir = collection_root / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_copies = [
        (
            source_artifact_root / "manifest.json",
            source_dir / "frozen_e1_manifest.json",
            "frozen_e1_manifest",
        ),
        (
            source_artifact_root / "live/collection_request_plan.parquet",
            source_dir / "collection_request_plan.parquet",
            "frozen_exact_request_plan",
        ),
        (
            source_artifact_root / "live/collection_contract.json",
            source_dir / "collection_contract.json",
            "frozen_collection_contract",
        ),
        (
            source_artifact_root
            / "contracts/deterministic_event_policy.json",
            collection_root / "contracts/deterministic_event_policy.json",
            "deterministic_event_policy",
        ),
        (
            source_artifact_root / "contracts/llm_summary_contract.json",
            collection_root / "contracts/llm_summary_contract.json",
            "claim_level_summary_contract",
        ),
        (
            source_artifact_root / "contracts/time_semantics.json",
            collection_root / "contracts/time_semantics.json",
            "event_review_time_semantics",
        ),
    ]
    for source, target, role in source_copies:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        outputs.append((target, role))

    aggregate_response_bytes = sum(
        int(item["response_size_bytes"])
        for item in request_manifest["records"]
    )
    total_success_bytes = sum(
        int(item["response_size_bytes"])
        for item in request_manifest["records"]
        if int(item["http_status"]) == 200
        and item["payload_validation_status"] == "expected_text_or_xml"
    )
    successful = int(
        reconciliation["request_status"].eq(
            "retrieved_expected_payload"
        ).sum()
    )
    unresolved = int(
        reconciliation["deterministic_action"].eq("unresolved").sum()
    )
    report_path = collection_root / "report/collection_reconciliation.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        f"""# E1 Live SEC Collection and Reconciliation

Collection `{collection_id}` executed the unchanged frozen E1 request plan:
{len(request_manifest['records'])} requests attempted, {successful} expected
HTML/XML responses preserved, and {aggregate_response_bytes} aggregate exact
response bytes ({total_success_bytes} bytes from eligible HTTP 200 payloads).
The frozen plan SHA-256 is `{source_hashes['plan_sha256']}` and its parent E1
manifest SHA-256 is `{source_hashes['manifest_sha256']}`.

Deterministic policy was applied to all {len(reconciliation)} planned filing
documents before any summarization. It produced {unresolved} unresolved
records, zero inferred events, and zero summary claims. Exact security scope,
event type/status, complete terms, and effective time were not inferred from
filing family. Every ambiguous record remains routed to human review in
`outputs/live/review_contract.parquet`.

The exact request URL, HTTP status and headers, request/retrieval timestamps,
response and stored-byte hashes, publication lineage, and evidence path are in
`request_manifest.json`. Raw evidence is under `raw/sec/filings/`. Any later
live summary is permitted only through the claim schema in
`contracts/llm_summary_contract.json`, with a retrieved source ID, exact
response hash, evidence path, and document locator for every claim.

No dataset was promoted, no P3/P4 consumer changed, and no performance was
calculated.
"""
    )
    outputs.append((report_path, "collection_reconciliation_report"))
    outputs.extend(
        [
            (request_manifest_path, "exact_request_response_manifest"),
        ]
    )
    raw_paths = sorted(
        path
        for path in (collection_root / "raw/sec/filings").rglob("*")
        if path.is_file()
    )
    request_log = collection_root / "request_log.jsonl"
    if request_log.is_file():
        outputs.append((request_log, "append_only_request_log"))
    preservation = _validate_preservation(
        DEFAULT_D1_ROOT / "review/review_manifest.json"
    )
    code_lineage = []
    for path in (
        ROOT / "portfolio/event_review.py",
        ROOT / "workflows/collect_live_event_evidence.py",
        ROOT / "tests/portfolio/test_event_review.py",
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
        "artifact_class": "VERSIONED_E1_LIVE_SEC_PRIMARY_DOCUMENT_COLLECTION",
        "collection_id": collection_id,
        "created_at_utc": utc_now(),
        "source_e1": {
            "artifact_root": _display_path(source_artifact_root),
            **source_hashes,
        },
        "code_lineage": code_lineage,
        "collection": {
            "source": "SEC EDGAR Archives primary filing documents",
            "request_count_attempted": len(request_manifest["records"]),
            "external_requests_in_this_version": sum(
                item.get("collection_lineage")
                != "reused_verified_predecessor"
                for item in request_manifest["records"]
            ),
            "external_requests_in_lineage": len(
                request_manifest["records"]
            ),
            "maximum_external_request_count": int(
                contract["maximum_external_request_count"]
            ),
            "successful_expected_payloads": successful,
            "aggregate_response_bytes": aggregate_response_bytes,
            "eligible_http_200_response_bytes": total_success_bytes,
            "per_response_limit_bytes": LIVE_RESPONSE_LIMIT_BYTES,
            "aggregate_limit_bytes": LIVE_AGGREGATE_LIMIT_BYTES,
            "request_interval_seconds": SEC_MIN_REQUEST_INTERVAL_SECONDS,
            "request_plan_execution_complete": (
                len(request_manifest["records"]) == len(plan)
            ),
            "evidence_retrieval_complete": successful == len(plan),
            "complete": len(request_manifest["records"]) == len(plan),
        },
        "preservation": preservation,
        "records": [
            *[_record(collection_root, path, role) for path, role in outputs],
            *[
                _record(
                    collection_root,
                    path,
                    "exact_sec_primary_document_response",
                )
                for path in raw_paths
            ],
        ],
        "live": {
            "shortlist_rows": len(live),
            "human_review_rows": int(live["human_review_required"].sum()),
            "deterministic_unresolved_document_rows": unresolved,
            "summary_claim_rows": len(claims),
        },
        "claim": {
            "frozen_e1_manifest_validated_before_collection": True,
            "frozen_request_plan_validated_before_collection": True,
            "deterministic_rules_precede_summary": True,
            "unsupported_event_inferences_applied": False,
            "ambiguous_evidence_routed_to_human_review": True,
            "summary_claims_require_retrieved_evidence_citations": True,
            "performance_calculated": False,
            "official_performance_available": False,
            "pinned_p2_p4_mutated": False,
            "d1_promoted": False,
            "external_request_made": bool(request_manifest["records"]),
        },
    }
    manifest_path = collection_root / "manifest.json"
    _write_json(manifest_path, manifest)
    return manifest_path


def collect_live_event_evidence(
    artifact_root: Path,
    *,
    confirmation: str | None,
    fetcher: LiveFetcher = _http_fetch,
    collection_root: Path | None = None,
    collection_id: str | None = None,
    expected_manifest_sha256: str | None = None,
    expected_plan_sha256: str | None = None,
    predecessor_root: Path | None = None,
) -> Path:
    """Collect only an explicitly approved frozen E1 request plan.

    Calling this function without the exact approval token performs no request.
    The E1 build itself never calls this function.
    """
    artifact_root = artifact_root.resolve()
    preliminary_contract = json.loads(
        (artifact_root / "live/collection_contract.json").read_text()
    )
    expected = preliminary_contract[
        "approval_token_after_explicit_user_authorization"
    ]
    if confirmation != expected:
        raise PermissionError(
            "explicit live collection approval token is required"
        )
    contract, plan, source_hashes = _validate_frozen_collection_source(
        artifact_root,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_plan_sha256=expected_plan_sha256,
    )
    legacy_destination = collection_root is None
    if collection_root is None:
        collection_root = artifact_root / "live/collection" / contract[
            "collection_id"
        ]
        collection_id = str(contract["collection_id"])
    else:
        collection_root = collection_root.resolve()
        if collection_id is None or not REVIEW_ID_PATTERN.fullmatch(
            collection_id
        ):
            raise ValueError(
                "collection_id must be a timestamped immutable identifier"
            )
    if collection_root.exists():
        raise RuntimeError(
            f"live collection target is not empty or already exists: "
            f"{collection_root}"
        )
    collection_root.mkdir(parents=True, exist_ok=True)

    metadata: list[dict[str, Any]] = []
    total_bytes = 0
    from pipeline.us_refresh_sources import SEC_HEADERS

    headers = {
        "User-Agent": SEC_HEADERS["User-Agent"],
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "text/plain;q=0.8,*/*;q=0.1"
        ),
        "Accept-Encoding": "identity",
    }
    request_log = collection_root / "request_log.jsonl"
    if predecessor_root is not None:
        predecessor_root = predecessor_root.resolve()
        predecessor_verification = verify_live_event_collection_artifact(
            predecessor_root
        )
        predecessor_manifest = json.loads(
            (predecessor_root / "manifest.json").read_text()
        )
        if (
            predecessor_manifest["source_e1"]["manifest_sha256"]
            != source_hashes["manifest_sha256"]
            or predecessor_manifest["source_e1"]["plan_sha256"]
            != source_hashes["plan_sha256"]
        ):
            raise RuntimeError("collection predecessor source lineage drifted")
        prior_request_manifest = json.loads(
            (predecessor_root / "request_manifest.json").read_text()
        )
        for prior in prior_request_manifest["records"]:
            source = predecessor_root / prior["stored_path"]
            target = collection_root / prior["stored_path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            if (
                target.stat().st_size != int(prior["stored_size_bytes"])
                or sha256_file(target) != prior["stored_sha256"]
            ):
                raise RuntimeError(
                    f"copied predecessor response mismatch: {source}"
                )
            inherited = {
                **prior,
                "collection_lineage": "reused_verified_predecessor",
                "predecessor_artifact": _display_path(predecessor_root),
                "predecessor_manifest_sha256": predecessor_verification[
                    "manifest_sha256"
                ],
            }
            metadata.append(inherited)
            total_bytes += int(prior["response_size_bytes"])
            _append_request_log(request_log, inherited)
    last_request_started = 0.0
    for item in plan.iloc[len(metadata) :].itertuples(index=False):
        delay = max(
            0.0,
            last_request_started
            + SEC_MIN_REQUEST_INTERVAL_SECONDS
            - time.monotonic(),
        )
        if delay:
            time.sleep(delay)
        requested_at = utc_now()
        last_request_started = time.monotonic()
        status, response_headers, payload = fetcher(item.request_url, headers)
        retrieved_at = utc_now()
        if len(payload) > LIVE_RESPONSE_LIMIT_BYTES:
            raise RuntimeError("one live evidence response exceeds 10 MiB")
        total_bytes += len(payload)
        if total_bytes > LIVE_AGGREGATE_LIMIT_BYTES:
            raise RuntimeError("live evidence collection exceeds 100 MiB")
        target, stored_path = _collection_target_path(
            collection_root,
            item,
            legacy_destination=legacy_destination,
            source_artifact_root=artifact_root,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise RuntimeError(f"live evidence target exists: {target}")
        with target.open("xb") as output:
            with gzip.GzipFile(
                filename="", mode="wb", fileobj=output, mtime=0
            ) as handle:
                handle.write(payload)
        record = {
            "request_id": item.request_id,
            "request_url": item.request_url,
            "requested_at_utc": requested_at,
            "retrieved_at_utc": retrieved_at,
            "request_headers": headers,
            "http_status": int(status),
            "response_headers": response_headers,
            "response_size_bytes": len(payload),
            "response_sha256": hashlib.sha256(payload).hexdigest(),
            "stored_path": stored_path,
            "stored_size_bytes": target.stat().st_size,
            "stored_sha256": sha256_file(target),
            "payload_validation_status": _collection_payload_status(
                str(item.expected_payload_type)
                if hasattr(item, "expected_payload_type")
                else "text/html_or_plain_text",
                response_headers,
                payload,
            ),
            "source_publication_time": str(item.source_publication_time),
            "ticker": (
                str(item.ticker) if hasattr(item, "ticker") else None
            ),
            "sec_cik": (
                str(item.sec_cik) if hasattr(item, "sec_cik") else None
            ),
            "form": item.form,
            "accession_number": item.accession_number,
            "primary_document": (
                str(item.primary_document)
                if hasattr(item, "primary_document")
                else Path(stored_path).name.removesuffix(".gz")
            ),
            "collection_lineage": "requested_this_collection",
        }
        metadata.append(record)
        _append_request_log(request_log, record)
        if int(status) in {403, 429, 503}:
            break
    manifest_path = collection_root / "request_manifest.json"
    _write_json(
        manifest_path,
        {
            "schema_version": 1,
            "collection_id": collection_id,
            "request_count": len(metadata),
            "maximum_request_count": contract[
                "maximum_external_request_count"
            ],
            "aggregate_response_size_bytes": total_bytes,
            "records": metadata,
            "summary_or_event_claims_created": False,
        },
    )
    if not legacy_destination:
        manifest_path = _finalize_collection_artifact(
            artifact_root,
            collection_root,
            str(collection_id),
            contract,
            plan,
            source_hashes,
            manifest_path,
        )
    return manifest_path


def verify_live_event_collection_artifact(
    collection_root: Path,
) -> dict[str, Any]:
    """Independently verify collected bytes, frozen URLs, and reconciliation."""
    collection_root = collection_root.resolve()
    manifest_path = collection_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    failures = []
    for item in manifest["records"]:
        path = collection_root / item["path"]
        if (
            not path.is_file()
            or path.stat().st_size != int(item["size_bytes"])
            or sha256_file(path) != item["sha256"]
        ):
            failures.append(item["path"])
    source_manifest = collection_root / "source/frozen_e1_manifest.json"
    source_plan = collection_root / "source/collection_request_plan.parquet"
    if (
        sha256_file(source_manifest)
        != manifest["source_e1"]["manifest_sha256"]
        or sha256_file(source_plan) != manifest["source_e1"]["plan_sha256"]
    ):
        failures.append("source_e1_hashes")
    plan = pd.read_parquet(source_plan)
    request_manifest = json.loads(
        (collection_root / "request_manifest.json").read_text()
    )
    planned_urls = list(plan["request_url"])
    requested_urls = [
        str(item["request_url"]) for item in request_manifest["records"]
    ]
    if requested_urls != planned_urls[: len(requested_urls)]:
        failures.append("request_url_or_order_drift")
    raw_failures = []
    total_bytes = 0
    evidence_rows = []
    for item in request_manifest["records"]:
        path = collection_root / item["stored_path"]
        if (
            not path.is_file()
            or path.stat().st_size != int(item["stored_size_bytes"])
            or sha256_file(path) != item["stored_sha256"]
        ):
            raw_failures.append(str(item["request_id"]))
            continue
        with gzip.open(path, "rb") as handle:
            payload = handle.read()
        total_bytes += len(payload)
        if (
            len(payload) != int(item["response_size_bytes"])
            or len(payload) > LIVE_RESPONSE_LIMIT_BYTES
            or hashlib.sha256(payload).hexdigest()
            != item["response_sha256"]
        ):
            raw_failures.append(str(item["request_id"]))
        evidence_rows.append(
            {
                "source_id": f"sec_primary_document:{item['request_id']}",
                "response_sha256": item["response_sha256"],
                "collection_path": item["stored_path"],
            }
        )
    if total_bytes > LIVE_AGGREGATE_LIMIT_BYTES:
        failures.append("aggregate_byte_limit")
    reconciliation = pd.read_parquet(
        collection_root / "outputs/live/document_reconciliation.parquet"
    )
    live = pd.read_parquet(
        collection_root / "outputs/live/review_contract.parquet"
    )
    claims = pd.read_parquet(
        collection_root / "outputs/live/summary_claims.parquet"
    )
    validate_summary_claims(
        claims,
        pd.DataFrame(evidence_rows),
        review_mode="live",
    )
    if (
        len(reconciliation) != len(plan)
        or reconciliation["unsupported_inferences_applied"].any()
        or not reconciliation["human_review_required"].all()
        or len(live) != 15
        or not live["human_review_required"].all()
        or manifest["claim"]["performance_calculated"]
    ):
        failures.append("deterministic_reconciliation_contract")
    current_preservation = _validate_preservation(
        DEFAULT_D1_ROOT / "review/review_manifest.json"
    )
    if current_preservation != manifest["preservation"]:
        failures.append("pinned_preservation")
    if failures or raw_failures:
        raise RuntimeError(
            "live E1 collection verification failed: "
            f"records={failures}, raw={raw_failures}"
        )
    return {
        "manifest_sha256": sha256_file(manifest_path),
        "records_verified": len(manifest["records"]),
        "requests_verified": len(request_manifest["records"]),
        "raw_responses_verified": len(request_manifest["records"]),
        "aggregate_response_size_bytes": total_bytes,
        "planned_document_rows": len(reconciliation),
        "live_rows": len(live),
        "summary_claim_rows": len(claims),
        "performance_calculated": False,
    }
