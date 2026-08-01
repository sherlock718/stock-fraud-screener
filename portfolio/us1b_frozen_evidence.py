"""Frozen-shortlist SEC and exchange evidence for product Session US1B.

The module is deliberately contract-first.  ``freeze_contract`` writes and
revalidates the exact approval-gated request plan without network I/O.
``collect_approved_evidence`` is the only network-capable entry point and
refuses any approval token, URL, destination, or request policy drift.
Extraction, adjudication, reporting, and verification are offline operations.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import socket
import time
from typing import Any, Callable
import urllib.error
import urllib.request

import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings

from portfolio.build_us_free_product import verify_product_artifact
from portfolio.event_review_adjudication import (
    verify_event_review_adjudication_artifact,
)


ROOT = Path(__file__).resolve().parents[1]
VERSION = "20260801T193322Z-us1b"
DEFAULT_ARTIFACT_ROOT = (
    ROOT / "artifacts/product/us_free_v1_evidence" / VERSION
)
US1A_ROOT = ROOT / "artifacts/product/us_free_v1/20260801T183000Z-us1a"
US1A_MANIFEST_SHA256 = (
    "f27773c0e1fb92a25707e0fec363e13afa7ccb10f8ace7537187c6be58575edf"
)
D1_UNIVERSE_ROOT = (
    ROOT / "artifacts/canonical_refresh/us/20260730T110301Z/universe"
)
D1_UNIVERSE_MANIFEST_SHA256 = (
    "84853ac89472b87a4bca6088fb39cf3f83fd66fd4048eb3c177515b572f44396"
)
E1_COLLECTION_ROOT = (
    ROOT / "artifacts/event_review/us/20260730T150604Z-e1-collection-final"
)
E1_COLLECTION_MANIFEST_SHA256 = (
    "ad14c45402c95e2b652ac7e0f8b98707a44eb4279f3988be602bca5ec77208ee"
)
E1_ADJUDICATION_ROOT = (
    ROOT / "artifacts/event_review/us/20260730T173110Z-e1-adjudication-v2"
)
E1_ADJUDICATION_MANIFEST_SHA256 = (
    "dcbf95776b50139797410674d7f0ca410cfdaa1401917dea4fb47f7b00e9c7c6"
)

EVIDENCE_AS_OF = "2026-08-01T19:33:22Z"
DISCOVERY_START = "2025-07-30T00:00:00Z"
DISCOVERY_END = "2026-07-30T23:59:59Z"
REQUEST_PLAN_SHA256 = (
    "f1bcfc5d2c740d1626c3d6c11148ee1711ec5d70911165b897daf84f90f9bfbc"
)
APPROVAL_TOKEN = f"APPROVE-US1B-{REQUEST_PLAN_SHA256}"

PRIMARY_FORMS = (
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
EIGHT_K_ITEMS = ("1.01", "1.03", "2.01", "3.01", "5.01")

MARKET_REQUESTS = (
    {
        "order": 1,
        "endpoint": "nasdaq_trader_nasdaq_listed",
        "url": (
            "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
        ),
    },
    {
        "order": 2,
        "endpoint": "nasdaq_trader_other_listed",
        "url": (
            "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
        ),
    },
)

SEC_HEADERS = {
    "User-Agent": "CanonicalUSRefresh research@alpharesearch.io",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "text/plain;q=0.8,*/*;q=0.1"
    ),
    "Accept-Encoding": "identity",
}
MARKET_HEADERS = {
    "User-Agent": "CanonicalUSRefresh research@alpharesearch.io",
    "Accept": "text/plain,*/*;q=0.1",
    "Accept-Encoding": "identity",
}
RETRY_STATUSES = (408, 429, 500, 502, 503, 504)
STOP_STATUSES = (403,)
TIMEOUT_SECONDS = 60
MINIMUM_INTERVAL_SECONDS = 0.25
RETRY_BACKOFF_SECONDS = 2
RETRY_AFTER_CAP_SECONDS = 30
MAX_ATTEMPTS_PER_URL = 2
SEC_RESPONSE_LIMIT = 10 * 1024 * 1024
MARKET_RESPONSE_LIMIT = 5 * 1024 * 1024
AGGREGATE_RESPONSE_LIMIT = 100 * 1024 * 1024

EXPECTED_IDENTITIES = (
    (1, "CYH", "0001108109", "5ae87717631466d46157a37a9dedd61c8a7e0891a27506842638d721cd4b0d83"),
    (2, "RBBN", "0001708055", "01bec4eda7f773bab31fcc69fc806172745fc803ffe2f458b3a4fcb6ca018797"),
    (3, "DSX", "0001318885", "8ad19f332f97270b5a9385719c56e27ffc734d9cee895474c7c13a56ecf8bc8d"),
    (4, "ACCO", "0000712034", "fd6db8fbd905efcab8273d88dff9d076aba42f5f2eca7c26679ae1f7f9f5683f"),
    (5, "BDTX", "0001701541", "ec8a32b5359d7dd6d39082b56feaa24f2eef05d01b84022abb603bbe73605599"),
    (6, "HPK", "0001792849", "ad905a5c51ac5c85e99238124c4295b58d780693f235182adbfef8c8e62f56f8"),
    (7, "AMPY", "0001533924", "8da62f798d9dcdfd14b1c8d5e351ee8fadc79068fb747c738dcde85e3ce39635"),
    (8, "CRCT", "0001828962", "cfa0f3c1355d56c5ed90572a3e8b9006bd28b1a1377984f0e64f3fb440ca7f3c"),
    (9, "ARDT", "0001756655", "73a7fcf140be2427c6bbd0fe769ddaa739013ddae323f46b69bd36e3868f3b22"),
    (10, "BLMN", "0001546417", "f80b0795c126b665dfe68e596a147c79bb69933fc6305940dd59019c6ecffec8"),
    (11, "MLCO", "0001381640", "ffd7eb0ad6d0a3289a8207333942376df3ded40c578498286563d54839f3f606"),
    (12, "SSTK", "0001549346", "3ab5561392ce2152fadfaef62d40af1ecffafde5046fd9ef8e2de1af5f13af02"),
    (13, "HLLY", "0001822928", "ec2ffc008a74be7b54ce42781bc035ec6f58329d72a1b55055e7c3ff0b5876e8"),
    (14, "LFVN", "0000849146", "729d9d3ba96614c06c2b64caf63a605a0b8f3468d061aa1566dfb476820f3826"),
    (15, "CRTO", "0001576427", "4bbf192322a1af1ce0515e858f9acdf09342b9ec1db66ff177783309ee372685"),
)

NEW_CLAIM_SPECS: dict[str, dict[str, Any]] = {
    "0001193125-25-179099": {
        "anchor": "On August 12, 2025, CHS/Community Health Systems, Inc.",
        "event_type": "issuer_subsidiary_financing",
        "event_status": "completed",
        "affected_security": "CHS subsidiary senior secured notes",
        "scope": False,
        "claim_text": "A Community Health Systems subsidiary completed a senior-secured-note offering.",
    },
    "0001193125-25-258896": {
        "anchor": "On October 30, 2025, CHS/Community Health Systems, Inc.",
        "event_type": "issuer_subsidiary_asset_disposition",
        "event_status": "announced_pending",
        "affected_security": "subsidiary ownership interests and operating assets",
        "scope": False,
        "claim_text": "Community Health Systems subsidiaries entered an asset and ownership-interest sale agreement.",
    },
    "0001193125-25-305375": {
        "anchor": "On December 1, 2025, CHS/Community Health Systems, Inc.",
        "event_type": "issuer_subsidiary_asset_disposition",
        "event_status": "completed",
        "affected_security": "subsidiary ambulatory outreach assets",
        "scope": False,
        "claim_text": "Community Health Systems subsidiaries completed an asset disposition.",
    },
    "0001193125-26-016693": {
        "anchor": "On January 20, 2026, CHS/Community Health Systems, Inc.",
        "event_type": "issuer_subsidiary_asset_disposition",
        "event_status": "announced_pending",
        "affected_security": "hospital operating assets and liabilities",
        "scope": False,
        "claim_text": "A Community Health Systems subsidiary entered a hospital asset-sale agreement.",
    },
    "0001193125-26-033755": {
        "anchor": "On February 1, 2026, CHS/Community Health Systems, Inc.",
        "event_type": "issuer_subsidiary_asset_disposition",
        "event_status": "completed",
        "affected_security": "subsidiary ownership interests and operating assets",
        "scope": False,
        "claim_text": "Community Health Systems subsidiaries completed an ownership-interest disposition.",
    },
    "0001193125-26-094191": {
        "anchor": "On March 5, 2026, CHS/Community Health Systems, Inc.",
        "event_type": "issuer_subsidiary_asset_disposition",
        "event_status": "announced_pending",
        "affected_security": "hospital operating assets and liabilities",
        "scope": False,
        "claim_text": "A Community Health Systems subsidiary entered a hospital asset-sale agreement.",
    },
    "0001193125-26-138026": {
        "anchor": "On April 1, 2026, CHS/Community Health Systems, Inc.",
        "event_type": "issuer_subsidiary_asset_disposition",
        "event_status": "completed",
        "affected_security": "hospital operating assets and liabilities",
        "scope": False,
        "claim_text": "A Community Health Systems subsidiary completed a hospital asset disposition.",
    },
    "0001193125-26-251540": {
        "anchor": "On June 1, 2026, CHS/Community Health Systems, Inc.",
        "event_type": "issuer_subsidiary_asset_disposition",
        "event_status": "completed",
        "affected_security": "hospital operating assets and liabilities",
        "scope": False,
        "claim_text": "A Community Health Systems subsidiary completed a hospital asset disposition.",
    },
    "0000950170-25-100909": {
        "anchor": "Effective July 29, 2025, ACCO Brands Corporation",
        "event_type": "issuer_financing_agreement",
        "event_status": "entered",
        "affected_security": "credit agreement",
        "scope": False,
        "claim_text": "ACCO Brands entered an amendment to its credit agreement.",
    },
    "0001140361-25-034446": {
        "anchor": "14,000,000 Shares of Common Stock This prospectus supplement relates",
        "event_type": "common_stock_secondary_offering",
        "event_status": "preliminary",
        "affected_security": "HLLY Class A common stock",
        "scope": True,
        "claim_text": "A preliminary prospectus described a selling-stockholder offering of HLLY Class A common stock.",
    },
    "0000876661-26-000625": {
        "anchor": "Redeemable Warrants, each whole warrant exercisable for one share of Common Stock",
        "event_type": "delisting",
        "event_status": "filed",
        "affected_security": "HLLY redeemable warrants",
        "scope": False,
        "claim_text": "The Form 25-NSE identifies HLLY redeemable warrants, not HLLY common stock, as the class to be struck from listing and registration.",
    },
}

DSX_ANCHOR = (
    "Genco Shipping & Trading Limited (Name of Subject Company (Issuer))"
)


class US1BContractError(RuntimeError):
    """Raised when the frozen evidence boundary drifts."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _record(base: Path, path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(base).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _canonical_sha(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _verify_manifest_hash(path: Path, expected: str, label: str) -> None:
    if not path.is_file() or sha256_file(path) != expected:
        raise US1BContractError(f"{label} manifest drifted")


def _load_shortlist() -> pd.DataFrame:
    shortlist = pd.read_parquet(
        US1A_ROOT / "outputs/final_shortlist_2026.parquet"
    ).sort_values("rank").reset_index(drop=True)
    observed = tuple(
        (
            int(row.rank),
            str(row.ticker),
            str(row.cik).zfill(10),
            str(row.stable_row_id),
        )
        for row in shortlist.itertuples(index=False)
    )
    if observed != EXPECTED_IDENTITIES:
        raise US1BContractError("US1A frozen shortlist identity/rank drifted")
    timestamps = pd.to_datetime(shortlist["decision_timestamp"], utc=True)
    if not timestamps.eq(pd.Timestamp("2026-07-02T00:00:00Z")).all():
        raise US1BContractError("US1A decision timestamp drifted")
    if not shortlist["weight"].map(
        lambda value: abs(float(value) - 1 / 15) <= 1e-15
    ).all():
        raise US1BContractError("US1A weights drifted")
    mapping = pd.read_parquet(
        US1A_ROOT / "outputs/event_evidence_mapping.parquet"
    ).sort_values("rank")
    matched = tuple(mapping.loc[mapping["event_evidence_collected"], "ticker"])
    if matched != ("HPK", "AMPY", "ARDT", "SSTK", "CRTO"):
        raise US1BContractError("US1A exact E1 match set drifted")
    if not mapping.loc[
        mapping["event_evidence_collected"], "e1_deterministic_action"
    ].eq("unresolved").all():
        raise US1BContractError("US1A E1 matches no longer unresolved")
    if not mapping.loc[
        ~mapping["event_evidence_collected"], "event_evidence_status"
    ].eq("event_evidence_not_collected").all():
        raise US1BContractError("US1A uncollected status set drifted")
    return shortlist


def _load_response_manifest() -> dict[str, dict[str, Any]]:
    path = D1_UNIVERSE_ROOT / "raw/response_manifest.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines() if line]
    return {str(row["logical_key"]): row for row in rows}


def _recent_value(recent: dict[str, list[Any]], key: str, index: int) -> Any:
    values = recent.get(key, [])
    return values[index] if index < len(values) else None


def _selected_document_rows(shortlist: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    submissions_root = D1_UNIVERSE_ROOT / "raw/submissions"
    start = DISCOVERY_START[:10]
    end = DISCOVERY_END[:10]
    for holding in shortlist.itertuples(index=False):
        cik = str(holding.cik).zfill(10)
        with gzip.open(submissions_root / f"CIK{cik}.json.gz", "rb") as handle:
            submission = json.loads(handle.read())
        recent = submission.get("filings", {}).get("recent", {})
        for index, form_value in enumerate(recent.get("form", [])):
            form = str(form_value)
            items = str(_recent_value(recent, "items", index) or "")
            item_set = {item.strip() for item in items.split(",") if item.strip()}
            selected = form in PRIMARY_FORMS or (
                form in {"8-K", "8-K/A"}
                and bool(item_set.intersection(EIGHT_K_ITEMS))
            )
            filing_date = str(
                _recent_value(recent, "filingDate", index) or ""
            )
            if not selected or not start <= filing_date <= end:
                continue
            accession = str(
                _recent_value(recent, "accessionNumber", index) or ""
            )
            primary_document = str(
                _recent_value(recent, "primaryDocument", index) or ""
            )
            if not accession or not primary_document:
                continue
            url = (
                "https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/{accession.replace('-', '')}/{primary_document}"
            )
            rows.append(
                {
                    "rank": int(holding.rank),
                    "ticker": str(holding.ticker),
                    "cik": cik,
                    "stable_row_id": str(holding.stable_row_id),
                    "form": form,
                    "items": items,
                    "filing_date": filing_date,
                    "source_publication_time": str(
                        _recent_value(recent, "acceptanceDateTime", index)
                        or filing_date
                    ),
                    "accession_number": accession,
                    "primary_document": primary_document,
                    "url": url,
                }
            )
    return rows


def build_frozen_request_plan() -> tuple[dict[str, Any], pd.DataFrame]:
    """Recreate the exact approved plan entirely from preserved evidence."""
    shortlist = _load_shortlist()
    e1_manifest = json.loads(
        (E1_COLLECTION_ROOT / "request_manifest.json").read_text()
    )
    e1_by_url = {
        str(item["request_url"]): item for item in e1_manifest["records"]
    }
    documents = _selected_document_rows(shortlist)
    for item in documents:
        item["action"] = (
            "reuse_e1_no_request"
            if item["url"] in e1_by_url
            else "external_request"
        )
    reused = [item for item in documents if item["action"].startswith("reuse")]
    new_sec = sorted(
        [item for item in documents if item["action"] == "external_request"],
        key=lambda item: (
            item["rank"],
            item["source_publication_time"],
            item["url"],
        ),
    )
    identities = [
        {
            "rank": int(row.rank),
            "ticker": str(row.ticker),
            "cik": str(row.cik).zfill(10),
            "stable_row_id": str(row.stable_row_id),
            "decision_timestamp": pd.Timestamp(
                row.decision_timestamp
            ).isoformat(),
            "weight": float(row.weight),
        }
        for row in shortlist.itertuples(index=False)
    ]
    external = list(MARKET_REQUESTS) + [
        {"order": order, **item}
        for order, item in enumerate(new_sec, start=3)
    ]
    plan = {
        "schema_version": 1,
        "artifact_root": (
            "artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b"
        ),
        "parent_us1a_manifest_sha256": US1A_MANIFEST_SHA256,
        "evidence_as_of_utc": EVIDENCE_AS_OF,
        "sec_discovery_window": {
            "start_inclusive": DISCOVERY_START,
            "end_inclusive": DISCOVERY_END,
            "discovery_source": "D1 exact submissions, no refresh",
        },
        "identities": identities,
        "recognized_primary_forms": list(PRIMARY_FORMS),
        "recognized_8k_items": list(EIGHT_K_ITEMS),
        "reused": {
            "d1_submission_count": 15,
            "d1_exchange_index_count": 1,
            "e1_primary_response_count": len(reused),
            "e1_http_status_counts": dict(
                Counter(
                    str(e1_by_url[item["url"]]["http_status"])
                    for item in reused
                )
            ),
        },
        "external_requests": external,
        "request_policy": {
            "method": "GET",
            "query_parameters": None,
            "request_body": None,
            "cookies": None,
            "sec_headers": SEC_HEADERS,
            "market_headers": MARKET_HEADERS,
            "timeout_seconds": TIMEOUT_SECONDS,
            "minimum_start_interval_seconds": MINIMUM_INTERVAL_SECONDS,
            "max_unique_urls": 36,
            "max_attempts": 72,
            "max_attempts_per_url": MAX_ATTEMPTS_PER_URL,
            "retry_statuses": list(RETRY_STATUSES),
            "retry_exceptions": ["timeout", "connection_reset"],
            "retry_backoff_seconds": RETRY_BACKOFF_SECONDS,
            "retry_after_cap_seconds": RETRY_AFTER_CAP_SECONDS,
            "stop_statuses": list(STOP_STATUSES),
            "sec_response_limit_bytes": SEC_RESPONSE_LIMIT,
            "market_response_limit_bytes": MARKET_RESPONSE_LIMIT,
            "aggregate_response_limit_bytes": AGGREGATE_RESPONSE_LIMIT,
            "non_overwrite": True,
            "preserve_every_http_body_and_attempt_metadata": True,
        },
        "prohibitions": [
            "no model score/rank/portfolio/performance source selection",
            "no generic web summaries",
            (
                "no ticker disappearance/name similarity/form-family-only/"
                "current-model-knowledge/uncited-narrative evidence"
            ),
            (
                "no retry of 13 preserved E1 responses including HPK HTTP 503"
            ),
            "no source outside exact URL list",
        ],
    }
    if _canonical_sha(plan) != REQUEST_PLAN_SHA256:
        raise US1BContractError("approved request-plan SHA-256 drifted")
    plan_rows = pd.DataFrame(external)
    if len(plan_rows) != 36 or plan_rows["url"].duplicated().any():
        raise US1BContractError("approved 36-URL boundary drifted")
    if len(documents) != 47 or len(reused) != 13 or len(new_sec) != 34:
        raise US1BContractError("approved SEC document boundary drifted")
    return plan, plan_rows


def _evidence_contract() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "session": "US1B",
        "request_plan_sha256": REQUEST_PLAN_SHA256,
        "approval_token": APPROVAL_TOKEN,
        "decision_timestamp_utc": "2026-07-02T00:00:00Z",
        "evidence_as_of_utc": EVIDENCE_AS_OF,
        "publication_eligibility": {
            "decision_action": "source publication at or before decision timestamp",
            "post_decision_review": (
                "after decision and at or before evidence-as-of; cannot alter US1A"
            ),
            "ineligible": "after evidence-as-of or missing publication lineage",
            "dynamic_market_source": (
                "Last-Modified must be at or before evidence-as-of; retrieval is not publication"
            ),
        },
        "exact_security_scope": {
            "required": [
                "frozen stable_row_id",
                "zero-padded SEC CIK",
                "frozen ticker",
                "explicit affected security class or instrument",
            ],
            "market_directory_role": (
                "corroboration only; ticker or name alone cannot prove identity or event"
            ),
        },
        "recognized_events": {
            "pending_acquisition": {"announced_pending": "exclude"},
            "completed_merger": {"completed": "exclude"},
            "bankruptcy": {
                "active": "exclude",
                "filed": "exclude",
                "ordered": "exclude",
            },
            "suspension": {"active": "exclude"},
            "delisting": {
                "effective": "exclude",
                "completed": "exclude",
                "announced_pending": "warn",
            },
            "registration_termination": {
                "filed": "warn",
                "effective": "warn",
            },
            "exchange_noncompliance": {"notice_active": "warn"},
            "ticker_change": {"effective": "warn"},
            "exchange_change": {"effective": "warn"},
            "security_type_change": {"effective": "warn"},
            "other_material_event": {"confirmed": "warn"},
        },
        "effective_time_precision": {
            "accepted": ["exact timestamp", "explicit date with date precision"],
            "same_day_rule": (
                "date-only evidence is unresolved for an intraday decision on the same date"
            ),
            "prohibited": "inventing an intraday timestamp from a date",
        },
        "terms_completeness": {
            "acquisition_or_tender": [
                "target and exact class",
                "buyer or offeror",
                "consideration",
                "status",
                "expiry/effective date",
                "amendment, termination, or outcome lineage",
            ],
            "delisting_or_suspension": [
                "exact class",
                "exchange",
                "reason",
                "effective/start date",
                "reinstatement or outcome if applicable",
            ],
            "bankruptcy": ["debtor", "court/chapter", "filing date", "status"],
            "identity_change": ["old identifier", "new identifier", "effective date"],
            "registration_termination": ["class", "form basis", "effective status"],
        },
        "conflict_rule": (
            "incompatible eligible claims remain conflicting unless a later cited document explicitly supersedes the earlier status"
        ),
        "name_status_precedence": [
            "conflicting",
            "failed_request",
            "covered",
            "unresolved",
            "unsupported",
            "event_evidence_not_collected",
        ],
        "human_review_required_for": [
            "conflicting",
            "failed_request",
            "unresolved",
            "unsupported",
            "event_evidence_not_collected",
        ],
        "claim_lineage_required": [
            "source_id",
            "URL",
            "retrieval time",
            "publication time",
            "response SHA-256",
            "evidence path",
            "reproducible document locator",
        ],
        "summary_policy": (
            "retrieved cited evidence only; narrative can never change deterministic action"
        ),
        "unsupported_inferences": [
            "generic web summary",
            "current model knowledge",
            "ticker disappearance",
            "name similarity",
            "filing family alone",
            "uncited narrative",
        ],
        "immutability": (
            "US1A identities, ranks, holdings, scores, gates, liquidity decisions, and weights remain unchanged"
        ),
    }


def _copy_exact(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError(f"non-overwriting target exists: {target}")
    shutil.copyfile(source, target)
    if source.stat().st_size != target.stat().st_size or sha256_file(
        source
    ) != sha256_file(target):
        raise US1BContractError(f"copied evidence mismatch: {source}")


def _copy_or_verify_exact(source: Path, target: Path) -> None:
    """Copy once, or verify a byte-identical preserved predecessor copy."""
    if target.exists():
        if (
            target.stat().st_size != source.stat().st_size
            or sha256_file(target) != sha256_file(source)
        ):
            raise US1BContractError(f"existing copied evidence drifted: {target}")
        return
    _copy_exact(source, target)


def freeze_contract(
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
    *,
    reverify_boundaries: bool = True,
) -> Path:
    """Materialize the approved contract without performing network I/O."""
    artifact_root = artifact_root.resolve()
    if artifact_root.exists():
        raise FileExistsError(f"US1B target already exists: {artifact_root}")
    _verify_manifest_hash(
        US1A_ROOT / "manifest.json", US1A_MANIFEST_SHA256, "US1A"
    )
    _verify_manifest_hash(
        D1_UNIVERSE_ROOT / "manifest.json",
        D1_UNIVERSE_MANIFEST_SHA256,
        "D1 universe",
    )
    _verify_manifest_hash(
        E1_COLLECTION_ROOT / "manifest.json",
        E1_COLLECTION_MANIFEST_SHA256,
        "E1 collection",
    )
    _verify_manifest_hash(
        E1_ADJUDICATION_ROOT / "manifest.json",
        E1_ADJUDICATION_MANIFEST_SHA256,
        "E1 adjudication",
    )
    if reverify_boundaries:
        verification = verify_product_artifact(US1A_ROOT)
        if (
            verification["record_count"] != 32
            or verification["record_bytes"] != 37_863_802
        ):
            raise US1BContractError("US1A complete record boundary drifted")
        verify_event_review_adjudication_artifact(E1_ADJUDICATION_ROOT)
    plan, plan_rows = build_frozen_request_plan()
    contract = _evidence_contract()

    artifact_root.mkdir(parents=True)
    _write_json(artifact_root / "configuration/evidence_contract.json", contract)
    _write_json(artifact_root / "requests/frozen_request_plan.json", plan)
    plan_rows.to_parquet(
        artifact_root / "requests/frozen_request_plan.parquet", index=False
    )
    _copy_exact(
        US1A_ROOT / "manifest.json",
        artifact_root / "inputs/us1a_manifest.json",
    )
    _copy_exact(
        D1_UNIVERSE_ROOT / "manifest.json",
        artifact_root / "inputs/d1_universe_manifest.json",
    )
    _copy_exact(
        E1_COLLECTION_ROOT / "manifest.json",
        artifact_root / "inputs/e1_collection_manifest.json",
    )
    _copy_exact(
        E1_ADJUDICATION_ROOT / "manifest.json",
        artifact_root / "inputs/e1_adjudication_manifest.json",
    )
    _copy_exact(
        US1A_ROOT / "outputs/final_shortlist_2026.parquet",
        artifact_root / "inputs/frozen_shortlist_2026.parquet",
    )
    _write_json(
        artifact_root / "support/pre_request_freeze.json",
        {
            "status": "frozen_before_first_external_request",
            "frozen_at_utc": EVIDENCE_AS_OF,
            "request_plan_sha256": REQUEST_PLAN_SHA256,
            "approved_external_unique_urls": 36,
            "maximum_http_attempts": 72,
            "us1a_manifest_sha256": US1A_MANIFEST_SHA256,
            "us1a_record_count": 32,
            "us1a_record_bytes": 37_863_802,
            "d1_status": "unsupported_for_US1A_frozen_M1_route",
            "external_requests_made": 0,
            "downstream_evidence_observed": False,
        },
    )
    _write_json(
        artifact_root / "state/01_contract_frozen.json",
        {
            "status": "pass",
            "request_plan_sha256": REQUEST_PLAN_SHA256,
            "external_requests_made": 0,
        },
    )
    return artifact_root


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


FetchResult = tuple[int, dict[str, str], bytes, bool]
Fetcher = Callable[[str, dict[str, str], int], FetchResult]


def _http_fetch(url: str, headers: dict[str, str], limit: int) -> FetchResult:
    opener = urllib.request.build_opener(_NoRedirect)
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        response = opener.open(request, timeout=TIMEOUT_SECONDS)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = response.read(limit + 1)
        return (
            int(response.status),
            dict(response.headers.items()),
            payload,
            len(payload) > limit,
        )


def _safe_retry_exception(error: BaseException) -> str | None:
    reason = getattr(error, "reason", error)
    if isinstance(reason, (TimeoutError, socket.timeout)):
        return "timeout"
    if isinstance(reason, ConnectionResetError):
        return "connection_reset"
    text = str(reason).lower()
    if "timed out" in text:
        return "timeout"
    if "connection reset" in text:
        return "connection_reset"
    return None


def _attempt_target(
    artifact_root: Path,
    request_item: dict[str, Any],
    attempt: int,
) -> Path:
    endpoint_value = request_item.get("endpoint")
    endpoint = "" if pd.isna(endpoint_value) else str(endpoint_value or "")
    if endpoint.startswith("nasdaq_trader"):
        name = Path(str(request_item["url"])).name
        return (
            artifact_root
            / "raw/collected/market/nasdaqtrader"
            / name
            / f"attempt-{attempt:02d}.body.gz"
        )
    cik = str(request_item["cik"])
    accession = str(request_item["accession_number"]).replace("-", "")
    name = Path(str(request_item["primary_document"])).name
    return (
        artifact_root
        / "raw/collected/sec/filings"
        / cik
        / accession
        / name
        / f"attempt-{attempt:02d}.body.gz"
    )


def _store_body(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"response target exists: {path}")
    with path.open("xb") as output:
        with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as gz:
            gz.write(payload)


def _append_attempt_log(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(item, sort_keys=True) + "\n")
        handle.flush()


def _copy_reused_evidence(artifact_root: Path) -> list[dict[str, Any]]:
    response_rows = _load_response_manifest()
    shortlist = _load_shortlist()
    inventory: list[dict[str, Any]] = []
    for row in shortlist.itertuples(index=False):
        cik = str(row.cik).zfill(10)
        source = D1_UNIVERSE_ROOT / "raw/submissions" / f"CIK{cik}.json.gz"
        target = (
            artifact_root
            / "raw/reused/d1/sec/submissions"
            / f"CIK{cik}.json.gz"
        )
        _copy_or_verify_exact(source, target)
        metadata = response_rows[f"submission_{cik}"]
        inventory.append(
            {
                "source_id": f"d1_submission_{cik}",
                "provider": "sec_edgar",
                "request_url": metadata["request_url"],
                "request_action": "reuse_verified_no_request",
                "http_status": int(metadata["http_status"]),
                "source_publication_time": metadata["response_headers"].get(
                    "Last-Modified"
                ),
                "source_retrieved_at": metadata["retrieved_at_utc"],
                "response_size_bytes": int(metadata["response_size_bytes"]),
                "response_sha256": metadata["response_sha256"],
                "stored_path": target.relative_to(artifact_root).as_posix(),
                "stored_size_bytes": target.stat().st_size,
                "stored_sha256": sha256_file(target),
                "ticker": str(row.ticker),
                "cik": cik,
                "lineage_manifest_sha256": D1_UNIVERSE_MANIFEST_SHA256,
            }
        )
    metadata = response_rows["company_tickers_exchange"]
    source = D1_UNIVERSE_ROOT / metadata["stored_path"]
    target = artifact_root / "raw/reused/d1/sec/index/company_tickers_exchange.json.gz"
    _copy_or_verify_exact(source, target)
    inventory.append(
        {
            "source_id": "d1_company_tickers_exchange",
            "provider": "sec_edgar",
            "request_url": metadata["request_url"],
            "request_action": "reuse_verified_no_request",
            "http_status": int(metadata["http_status"]),
            "source_publication_time": metadata["response_headers"].get(
                "Last-Modified"
            ),
            "source_retrieved_at": metadata["retrieved_at_utc"],
            "response_size_bytes": int(metadata["response_size_bytes"]),
            "response_sha256": metadata["response_sha256"],
            "stored_path": target.relative_to(artifact_root).as_posix(),
            "stored_size_bytes": target.stat().st_size,
            "stored_sha256": sha256_file(target),
            "ticker": None,
            "cik": None,
            "lineage_manifest_sha256": D1_UNIVERSE_MANIFEST_SHA256,
        }
    )
    selected = _selected_document_rows(shortlist)
    selected_urls = {item["url"] for item in selected}
    e1_manifest = json.loads(
        (E1_COLLECTION_ROOT / "request_manifest.json").read_text()
    )
    reused = [
        item
        for item in e1_manifest["records"]
        if str(item["request_url"]) in selected_urls
    ]
    if len(reused) != 13:
        raise US1BContractError("E1 exact response reuse set drifted")
    for item in reused:
        source = E1_COLLECTION_ROOT / str(item["stored_path"])
        target = artifact_root / "raw/reused/e1" / str(item["stored_path"])
        _copy_or_verify_exact(source, target)
        inventory.append(
            {
                "source_id": f"e1_primary_document:{item['request_id']}",
                "provider": "sec_edgar",
                "request_url": item["request_url"],
                "request_action": "reuse_verified_no_request",
                "http_status": int(item["http_status"]),
                "source_publication_time": item["source_publication_time"],
                "source_retrieved_at": item["retrieved_at_utc"],
                "response_size_bytes": int(item["response_size_bytes"]),
                "response_sha256": item["response_sha256"],
                "stored_path": target.relative_to(artifact_root).as_posix(),
                "stored_size_bytes": target.stat().st_size,
                "stored_sha256": sha256_file(target),
                "ticker": item.get("ticker"),
                "cik": item.get("sec_cik"),
                "form": item.get("form"),
                "accession_number": item.get("accession_number"),
                "primary_document": item.get("primary_document"),
                "lineage_manifest_sha256": E1_COLLECTION_MANIFEST_SHA256,
            }
        )
    return inventory


def collect_approved_evidence(
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
    *,
    approval_token: str,
    fetcher: Fetcher = _http_fetch,
    pace: bool = True,
) -> Path:
    """Collect only the exact approved 36-URL request plan."""
    artifact_root = artifact_root.resolve()
    if approval_token != APPROVAL_TOKEN:
        raise PermissionError("exact US1B approval token required")
    if not (artifact_root / "state/01_contract_frozen.json").is_file():
        raise US1BContractError("US1B contract is not frozen")
    if (artifact_root / "requests/request_inventory.json").exists():
        raise FileExistsError("US1B collection already materialized")
    frozen = json.loads(
        (artifact_root / "requests/frozen_request_plan.json").read_text()
    )
    if _canonical_sha(frozen) != REQUEST_PLAN_SHA256:
        raise US1BContractError("materialized request plan drifted")
    plan, plan_rows = build_frozen_request_plan()
    if plan != frozen:
        raise US1BContractError("current plan differs from frozen plan")

    reused = _copy_reused_evidence(artifact_root)
    log_path = artifact_root / "requests/attempt_log.jsonl"
    attempts: list[dict[str, Any]] = (
        [
            json.loads(line)
            for line in log_path.read_text().splitlines()
            if line
        ]
        if log_path.is_file()
        else []
    )
    if len({str(item["attempt_id"]) for item in attempts}) != len(attempts):
        raise US1BContractError("partial attempt log contains duplicate IDs")
    attempts_by_order: dict[int, list[dict[str, Any]]] = {}
    for item in attempts:
        attempts_by_order.setdefault(int(item["order"]), []).append(item)
    for rows in attempts_by_order.values():
        rows.sort(key=lambda item: int(item["attempt_number"]))
    logical: list[dict[str, Any]] = []
    total_bytes = sum(int(item["response_size_bytes"]) for item in attempts)
    stop = False
    last_started = 0.0
    for request_item in plan_rows.to_dict(orient="records"):
        order = int(request_item["order"])
        url = str(request_item["url"])
        prior_attempts = attempts_by_order.get(order, [])
        if prior_attempts:
            if any(str(item["url"]) != url for item in prior_attempts):
                raise US1BContractError("partial attempt URL/order drifted")
            prior = prior_attempts[-1]
            prior_status = str(prior["request_status"])
            completed = (
                prior_status == "retrieved"
                or prior_status.startswith("failed_request_stop")
                or prior_status.startswith("failed_request_oversize")
                or prior_status.startswith("failed_request_aggregate")
                or len(prior_attempts) >= MAX_ATTEMPTS_PER_URL
                or (
                    prior.get("http_status") not in RETRY_STATUSES
                    and prior.get("retry_exception_classification") is None
                )
            )
            if completed:
                logical.append(
                    {
                        "order": order,
                        "url": url,
                        "request_status": prior_status,
                        "attempt_count": len(prior_attempts),
                        "final_http_status": prior.get("http_status"),
                        "final_attempt_id": prior.get("attempt_id"),
                    }
                )
                if prior_status.startswith(
                    ("failed_request_stop", "failed_request_oversize", "failed_request_aggregate")
                ):
                    stop = True
                continue
        if stop:
            logical.append(
                {
                    "order": order,
                    "url": url,
                    "request_status": "not_attempted_after_stop_condition",
                    "attempt_count": 0,
                    "final_http_status": None,
                    "final_attempt_id": None,
                }
            )
            continue
        endpoint_value = request_item.get("endpoint")
        endpoint = (
            "sec_primary_document"
            if pd.isna(endpoint_value) or not endpoint_value
            else str(endpoint_value)
        )
        market = endpoint.startswith("nasdaq_trader")
        headers = MARKET_HEADERS if market else SEC_HEADERS
        limit = MARKET_RESPONSE_LIMIT if market else SEC_RESPONSE_LIMIT
        final_status = "failed_request"
        final_http_status: int | None = None
        final_attempt_id: str | None = None
        request_attempts = 0
        first_attempt = len(prior_attempts) + 1
        for attempt_number in range(first_attempt, MAX_ATTEMPTS_PER_URL + 1):
            if pace:
                delay = max(
                    0.0,
                    last_started + MINIMUM_INTERVAL_SECONDS - time.monotonic(),
                )
                if delay:
                    time.sleep(delay)
            requested_at = utc_now()
            last_started = time.monotonic()
            status: int | None = None
            response_headers: dict[str, str] = {}
            payload = b""
            oversize = False
            exception_class = None
            exception_message = None
            retry_exception = None
            try:
                status, response_headers, payload, oversize = fetcher(
                    url, headers, limit
                )
            except Exception as error:  # preserved as an explicit failure row
                exception_class = type(error).__name__
                exception_message = str(error)
                retry_exception = _safe_retry_exception(error)
            retrieved_at = utc_now()
            request_attempts += 1
            attempt_id = f"request-{order:02d}-attempt-{attempt_number:02d}"
            stored_path = None
            stored_size = 0
            stored_sha = None
            if status is not None:
                target = _attempt_target(
                    artifact_root, request_item, attempt_number
                )
                _store_body(target, payload)
                stored_path = target.relative_to(artifact_root).as_posix()
                stored_size = target.stat().st_size
                stored_sha = sha256_file(target)
                total_bytes += len(payload)
            if total_bytes > AGGREGATE_RESPONSE_LIMIT:
                stop = True
                final_status = "failed_request_aggregate_limit"
            elif oversize:
                stop = True
                final_status = "failed_request_oversize_partial_body"
            elif status is not None and 200 <= status < 300:
                final_status = "retrieved"
            elif status in STOP_STATUSES:
                stop = True
                final_status = "failed_request_stop_status"
            elif status is not None:
                final_status = "failed_request_http"
            else:
                final_status = "failed_request_exception"
            item = {
                "attempt_id": attempt_id,
                "order": order,
                "endpoint": endpoint,
                "url": url,
                "attempt_number": attempt_number,
                "requested_at_utc": requested_at,
                "retrieved_at_utc": retrieved_at,
                "request_headers": headers,
                "http_status": status,
                "response_headers": response_headers,
                "response_size_bytes": len(payload),
                "response_sha256": hashlib.sha256(payload).hexdigest()
                if status is not None
                else None,
                "stored_path": stored_path,
                "stored_size_bytes": stored_size,
                "stored_sha256": stored_sha,
                "oversize_partial_body": oversize,
                "exception_class": exception_class,
                "exception_message": exception_message,
                "retry_exception_classification": retry_exception,
                "request_status": final_status,
                "ticker": request_item.get("ticker"),
                "cik": request_item.get("cik"),
                "form": request_item.get("form"),
                "accession_number": request_item.get("accession_number"),
                "primary_document": request_item.get("primary_document"),
                "source_publication_time": request_item.get(
                    "source_publication_time"
                ),
            }
            attempts.append(item)
            attempts_by_order.setdefault(order, []).append(item)
            _append_attempt_log(log_path, item)
            final_http_status = status
            final_attempt_id = attempt_id
            retryable = (
                not stop
                and attempt_number < MAX_ATTEMPTS_PER_URL
                and (status in RETRY_STATUSES or retry_exception is not None)
            )
            if not retryable:
                break
            retry_after = response_headers.get("Retry-After")
            wait_seconds = RETRY_BACKOFF_SECONDS
            if retry_after and str(retry_after).isdigit():
                wait_seconds = min(int(retry_after), RETRY_AFTER_CAP_SECONDS)
            if pace and wait_seconds:
                time.sleep(wait_seconds)
        logical.append(
            {
                "order": order,
                "url": url,
                "request_status": final_status,
                "attempt_count": request_attempts,
                "final_http_status": final_http_status,
                "final_attempt_id": final_attempt_id,
            }
        )
    if len(attempts) > 72:
        raise US1BContractError("maximum HTTP attempt count exceeded")
    inventory = {
        "schema_version": 1,
        "request_plan_sha256": REQUEST_PLAN_SHA256,
        "approval_token_verified": True,
        "unique_url_count": 36,
        "attempt_count": len(attempts),
        "maximum_attempt_count": 72,
        "aggregate_received_body_bytes": total_bytes,
        "reused_response_count": len(reused),
        "reused_responses": reused,
        "logical_requests": logical,
        "attempts": attempts,
    }
    _write_json(artifact_root / "requests/request_inventory.json", inventory)
    _write_json(
        artifact_root / "state/02_collection_complete.json",
        {
            "status": "complete_with_explicit_failures"
            if any(row["request_status"] != "retrieved" for row in logical)
            else "complete",
            "unique_url_count": 36,
            "attempt_count": len(attempts),
            "retrieved_count": sum(
                row["request_status"] == "retrieved" for row in logical
            ),
            "failed_count": sum(
                row["request_status"].startswith("failed") for row in logical
            ),
            "not_attempted_count": sum(
                row["request_status"].startswith("not_attempted")
                for row in logical
            ),
            "aggregate_received_body_bytes": total_bytes,
        },
    )
    return artifact_root / "requests/request_inventory.json"


def _normalized_document_text(payload: bytes) -> str:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(payload, "lxml")
    return " ".join(soup.get_text(" ", strip=True).split())


def _bounded_passage(text: str, anchor: str) -> tuple[str, int, int]:
    start = text.find(anchor)
    if start < 0 or text.find(anchor, start + 1) >= 0:
        raise US1BContractError(f"claim anchor missing or non-unique: {anchor}")
    minimum_end = min(len(text), start + max(len(anchor), 360))
    maximum_end = min(len(text), start + 1_100)
    boundary = re.search(r"[.!?](?=\s+(?:[A-Z“]))", text[minimum_end:maximum_end])
    end = minimum_end + boundary.end() if boundary else maximum_end
    return text[start:end].strip(), start, end


def _hash_id(prefix: str, value: str) -> str:
    return f"{prefix}:{hashlib.sha256(value.encode()).hexdigest()[:24]}"


def _read_gzip_payload(artifact_root: Path, stored_path: str) -> bytes:
    with gzip.open(artifact_root / stored_path, "rb") as handle:
        return handle.read()


def _build_market_evidence(
    artifact_root: Path,
    inventory: dict[str, Any],
    shortlist: pd.DataFrame,
) -> pd.DataFrame:
    attempts = {int(item["order"]): item for item in inventory["attempts"]}
    directory_rows: dict[str, tuple[dict[str, str], dict[str, Any], str]] = {}
    for order in (1, 2):
        attempt = attempts[order]
        if attempt["request_status"] != "retrieved":
            continue
        payload = _read_gzip_payload(artifact_root, str(attempt["stored_path"]))
        lines = payload.decode("utf-8-sig", errors="replace").splitlines()
        reader = csv.DictReader(lines, delimiter="|")
        for row in reader:
            symbol = str(row.get("Symbol") or row.get("ACT Symbol") or "")
            if symbol:
                directory_rows[symbol] = (row, attempt, str(attempt["endpoint"]))

    sec_index_path = (
        artifact_root
        / "raw/reused/d1/sec/index/company_tickers_exchange.json.gz"
    )
    with gzip.open(sec_index_path, "rb") as handle:
        sec_index = json.loads(handle.read())
    fields = list(sec_index["fields"])
    sec_rows = [dict(zip(fields, values)) for values in sec_index["data"]]
    sec_by_identity = {
        (str(row["cik"]).zfill(10), str(row["ticker"])): row
        for row in sec_rows
    }
    output: list[dict[str, Any]] = []
    evidence_as_of = pd.Timestamp(EVIDENCE_AS_OF)
    for holding in shortlist.itertuples(index=False):
        ticker = str(holding.ticker)
        cik = str(holding.cik).zfill(10)
        sec = sec_by_identity.get((cik, ticker))
        directory = directory_rows.get(ticker)
        if sec is None or directory is None:
            raise US1BContractError(f"market/SEC identity coverage missing: {ticker}")
        row, attempt, endpoint = directory
        published = pd.to_datetime(
            attempt["response_headers"].get("Last-Modified"),
            utc=True,
            errors="coerce",
        )
        eligible = pd.notna(published) and published <= evidence_as_of
        if not eligible:
            raise US1BContractError(
                f"market publication time is not eligible: {ticker}"
            )
        market_exchange = (
            "Nasdaq"
            if endpoint == "nasdaq_trader_nasdaq_listed"
            else {"N": "NYSE", "A": "NYSE American", "P": "NYSE Arca"}.get(
                str(row.get("Exchange")), str(row.get("Exchange"))
            )
        )
        output.append(
            {
                "stable_row_id": str(holding.stable_row_id),
                "rank": int(holding.rank),
                "ticker": ticker,
                "cik": cik,
                "sec_legal_name": str(sec["name"]),
                "sec_exchange": str(sec["exchange"]),
                "market_security_name": str(row.get("Security Name")),
                "market_exchange": market_exchange,
                "test_issue": str(row.get("Test Issue")),
                "financial_status": row.get("Financial Status"),
                "listing_evidence_status": "ticker_present_in_frozen_directory_snapshot",
                "identity_scope_status": "corroborated_with_exact_SEC_CIK_ticker_index",
                "source_id": f"market_directory:request-{int(attempt['order']):02d}",
                "request_url": attempt["url"],
                "source_publication_time": published.isoformat(),
                "source_retrieved_at": attempt["retrieved_at_utc"],
                "source_response_sha256": attempt["response_sha256"],
                "evidence_path": attempt["stored_path"],
                "document_locator": f"pipe_delimited_symbol={ticker}",
                "publication_eligible": True,
                "absence_or_presence_is_event_proof": False,
            }
        )
    frame = pd.DataFrame(output).sort_values("rank").reset_index(drop=True)
    if len(frame) != 15 or frame["ticker"].nunique() != 15:
        raise US1BContractError("market evidence is not the frozen 15 names")
    return frame


def _new_claim_spec(item: dict[str, Any]) -> dict[str, Any]:
    accession = str(item["accession_number"])
    if str(item["ticker"]) == "DSX":
        return {
            "anchor": DSX_ANCHOR,
            "event_type": "third_party_tender_offer",
            "event_status": (
                "final_amendment_filed"
                if accession == "0001104659-26-086886"
                else "filed_or_amended"
            ),
            "affected_security": "Genco Shipping & Trading common stock",
            "scope": False,
            "claim_text": (
                "The filing identifies Genco as the subject company and a Diana Shipping subsidiary as offeror for Genco common stock."
            ),
        }
    spec = NEW_CLAIM_SPECS.get(accession)
    if spec is None:
        raise US1BContractError(f"no accession-bound claim spec: {accession}")
    return spec


def _build_document_and_claim_tables(
    artifact_root: Path,
    inventory: dict[str, Any],
    shortlist: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    selected = _selected_document_rows(shortlist)
    attempt_by_url = {
        str(item["url"]): item
        for item in inventory["attempts"]
        if item.get("ticker") == item.get("ticker")
    }
    reused_by_url = {
        str(item["request_url"]): item
        for item in inventory["reused_responses"]
        if str(item.get("source_id", "")).startswith("e1_primary_document:")
    }
    e1_claims = pd.read_parquet(
        E1_ADJUDICATION_ROOT / "source/extraction_claims.parquet"
    )
    e1_documents = pd.read_parquet(
        E1_ADJUDICATION_ROOT
        / "outputs/live/document_level_adjudication.parquet"
    )
    e1_claim_by_url = e1_claims.set_index("request_url", drop=False)
    e1_document_by_accession = e1_documents.set_index(
        "accession_number", drop=False
    )
    names = shortlist.set_index("ticker")
    documents: list[dict[str, Any]] = []
    claims: list[dict[str, Any]] = []
    decision = pd.Timestamp("2026-07-02T00:00:00Z")
    evidence_as_of = pd.Timestamp(EVIDENCE_AS_OF)
    for item in selected:
        url = str(item["url"])
        holding = names.loc[str(item["ticker"])]
        reused = url in reused_by_url
        source = reused_by_url[url] if reused else attempt_by_url.get(url)
        if source is None:
            raise US1BContractError(f"document response missing: {url}")
        http_status = int(source["http_status"])
        response_sha = str(source["response_sha256"])
        stored_path = str(source["stored_path"])
        retrieved_at = str(
            source.get("source_retrieved_at")
            if reused
            else source.get("retrieved_at_utc")
        )
        publication = pd.Timestamp(item["source_publication_time"])
        publication_status = (
            "eligible_at_decision"
            if publication <= decision
            else "eligible_post_decision_only"
            if publication <= evidence_as_of
            else "ineligible_after_evidence_as_of"
        )
        source_id = str(source["source_id"] if reused else source["attempt_id"])
        claim_id = None
        document_state = "failed_request" if http_status != 200 else "unresolved"
        exact_scope = False
        event_type = "unavailable"
        event_status = "unavailable"
        affected_security = "unavailable"
        terms_complete = False
        locator = None
        ambiguity = "retrieved primary document requires exact-rule adjudication"
        if http_status == 200 and reused:
            claim = e1_claim_by_url.loc[url]
            if isinstance(claim, pd.DataFrame):
                raise US1BContractError(f"multiple E1 claims for URL: {url}")
            adjudicated = e1_document_by_accession.loc[item["accession_number"]]
            if isinstance(adjudicated, pd.DataFrame):
                raise US1BContractError("duplicate E1 accession adjudication")
            claim_id = str(claim["claim_id"])
            passage = str(claim["supporting_passage"])
            payload = _read_gzip_payload(artifact_root, stored_path)
            normalized = _normalized_document_text(payload)
            if passage not in normalized or hashlib.sha256(
                passage.encode()
            ).hexdigest() != str(claim["supporting_passage_sha256"]):
                raise US1BContractError(f"reused E1 locator drifted: {claim_id}")
            locator = str(claim["document_locator"])
            exact_scope = bool(
                adjudicated["rule_exact_shortlist_security_scope_complete"]
            )
            event_type = str(adjudicated["event_type"])
            event_status = str(adjudicated["event_status"])
            affected_security = str(adjudicated["affected_security"])
            terms_complete = bool(adjudicated["terms_complete"])
            ambiguity = str(adjudicated["remaining_ambiguity"])
            claims.append(
                {
                    "claim_id": claim_id,
                    "document_id": _hash_id("document", url),
                    "source_id": source_id,
                    "ticker": item["ticker"],
                    "cik": item["cik"],
                    "request_url": url,
                    "accession_number": item["accession_number"],
                    "form": item["form"],
                    "source_publication_time": item["source_publication_time"],
                    "source_retrieved_at": retrieved_at,
                    "source_response_sha256": response_sha,
                    "evidence_path": stored_path,
                    "document_locator": locator,
                    "supporting_passage": passage,
                    "supporting_passage_sha256": claim[
                        "supporting_passage_sha256"
                    ],
                    "claim_text": claim["claim_text"],
                    "event_type": event_type,
                    "event_status": event_status,
                    "affected_security": affected_security,
                    "exact_shortlist_security_scope": exact_scope,
                    "lineage_action": "reused_exact_E1_claim",
                }
            )
        elif http_status == 200:
            spec = _new_claim_spec(item)
            payload = _read_gzip_payload(artifact_root, stored_path)
            normalized = _normalized_document_text(payload)
            passage, start, end = _bounded_passage(normalized, str(spec["anchor"]))
            locator = f"normalized_text_chars:{start}-{end};occurrence:1"
            claim_id = _hash_id(
                "claim", f"{response_sha}|{item['accession_number']}|{locator}"
            )
            exact_scope = bool(spec["scope"])
            event_type = str(spec["event_type"])
            event_status = str(spec["event_status"])
            affected_security = str(spec["affected_security"])
            document_state = "unresolved" if exact_scope else "unsupported"
            ambiguity = (
                "Exact HLLY common-stock scope is present, but the preliminary secondary-offering status is not a recognized complete selection-changing event contract."
                if item["accession_number"] == "0001140361-25-034446"
                else "The cited affected instrument is not the frozen shortlisted common-stock security."
            )
            claims.append(
                {
                    "claim_id": claim_id,
                    "document_id": _hash_id("document", url),
                    "source_id": source_id,
                    "ticker": item["ticker"],
                    "cik": item["cik"],
                    "request_url": url,
                    "accession_number": item["accession_number"],
                    "form": item["form"],
                    "source_publication_time": item["source_publication_time"],
                    "source_retrieved_at": retrieved_at,
                    "source_response_sha256": response_sha,
                    "evidence_path": stored_path,
                    "document_locator": locator,
                    "supporting_passage": passage,
                    "supporting_passage_sha256": hashlib.sha256(
                        passage.encode()
                    ).hexdigest(),
                    "claim_text": spec["claim_text"],
                    "event_type": event_type,
                    "event_status": event_status,
                    "affected_security": affected_security,
                    "exact_shortlist_security_scope": exact_scope,
                    "lineage_action": "extracted_from_new_approved_response",
                }
            )
        documents.append(
            {
                "document_id": _hash_id("document", url),
                "source_id": source_id,
                "stable_row_id": str(holding["stable_row_id"]),
                "rank": int(item["rank"]),
                "ticker": item["ticker"],
                "cik": item["cik"],
                "request_url": url,
                "accession_number": item["accession_number"],
                "form": item["form"],
                "items": item["items"],
                "source_publication_time": item["source_publication_time"],
                "publication_eligibility": publication_status,
                "source_retrieved_at": retrieved_at,
                "http_status": http_status,
                "source_response_sha256": response_sha,
                "evidence_path": stored_path,
                "claim_id": claim_id,
                "document_locator": locator,
                "event_type": event_type,
                "event_status": event_status,
                "affected_security": affected_security,
                "exact_shortlist_security_scope": exact_scope,
                "effective_time_precision": "unavailable_or_incomplete",
                "terms_complete": terms_complete,
                "conflict_status": "no_unresolved_cross_document_conflict_detected",
                "document_state": document_state,
                "deterministic_action": "unresolved",
                "human_review_required": True,
                "remaining_ambiguity": ambiguity,
                "summary_may_change_deterministic_action": False,
                "unsupported_inferences_applied": False,
                "lineage_action": "reused_E1" if reused else "collected_US1B",
            }
        )
    document_frame = pd.DataFrame(documents).sort_values(
        ["rank", "source_publication_time", "accession_number"]
    ).reset_index(drop=True)
    claim_frame = pd.DataFrame(claims).sort_values(
        ["ticker", "source_publication_time", "accession_number"]
    ).reset_index(drop=True)
    if len(document_frame) != 47 or len(claim_frame) != 46:
        raise US1BContractError("document/claim reconciliation count drifted")
    expected_states = {"unsupported": 33, "unresolved": 13, "failed_request": 1}
    if document_frame["document_state"].value_counts().to_dict() != expected_states:
        raise US1BContractError("document deterministic-state counts drifted")
    return document_frame, claim_frame


def _name_reason(ticker: str, document_count: int) -> str:
    if ticker == "HPK":
        return (
            "The preserved HPK Form 25-NSE response remains HTTP 503; the exact affected class and terms cannot be completed."
        )
    if ticker == "CYH":
        return (
            "Eight cited filings concern subsidiary debt or asset transactions, not an acquisition, cancellation, suspension, or delisting of CYH common stock."
        )
    if ticker == "DSX":
        return (
            "Twenty-three cited tender-offer filings identify Genco common stock as the subject security and a Diana subsidiary as offeror; they do not establish an event affecting DSX common stock."
        )
    if ticker == "ACCO":
        return "The cited filing concerns a credit-agreement amendment, not ACCO common stock."
    if ticker == "HLLY":
        return (
            "The preliminary prospectus concerns a secondary common-stock offering, while the Form 25-NSE concerns redeemable warrants; neither completes a recognized event contract for HLLY common stock."
        )
    if document_count == 0:
        return (
            "The preserved SEC submission contained no primary-document candidate under the frozen form/item filter; absence is not proof that no event exists."
        )
    return "Retrieved claims do not complete exact security scope, recognized status, effective time, and terms."


def _build_name_adjudication(
    shortlist: pd.DataFrame,
    documents: pd.DataFrame,
    claims: pd.DataFrame,
    market: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for holding in shortlist.sort_values("rank").itertuples(index=False):
        ticker = str(holding.ticker)
        docs = documents.loc[documents["ticker"].eq(ticker)]
        cited = claims.loc[claims["ticker"].eq(ticker), "claim_id"].tolist()
        failed = docs["document_state"].eq("failed_request").any()
        coverage_state = "failed_request" if failed else "unresolved"
        previous = str(holding.event_evidence_status)
        rows.append(
            {
                "stable_row_id": str(holding.stable_row_id),
                "rank": int(holding.rank),
                "ticker": ticker,
                "cik": str(holding.cik).zfill(10),
                "decision_timestamp": pd.Timestamp(
                    holding.decision_timestamp
                ).isoformat(),
                "evidence_as_of_utc": EVIDENCE_AS_OF,
                "weight": float(holding.weight),
                "us1a_event_evidence_status": previous,
                "exact_E1_stable_security_lineage_reused": ticker
                in {"HPK", "AMPY", "ARDT", "SSTK", "CRTO"},
                "submission_response_reused": True,
                "market_symbol_evidence_status": market.set_index("ticker").loc[
                    ticker, "listing_evidence_status"
                ],
                "planned_primary_document_count": len(docs),
                "retrieved_primary_document_count": int(
                    docs["http_status"].eq(200).sum()
                ),
                "failed_primary_document_count": int(
                    docs["document_state"].eq("failed_request").sum()
                ),
                "unsupported_document_count": int(
                    docs["document_state"].eq("unsupported").sum()
                ),
                "unresolved_document_count": int(
                    docs["document_state"].eq("unresolved").sum()
                ),
                "claim_count": len(cited),
                "coverage_state": coverage_state,
                "conflict_status": "no_unresolved_cross_document_conflict_detected",
                "deterministic_action": "unresolved",
                "holding_changed": False,
                "rank_changed": False,
                "weight_changed": False,
                "human_review_required": True,
                "summary_allowed": not failed and bool(cited),
                "summary_may_change_deterministic_action": False,
                "cited_claim_ids": json.dumps(cited),
                "adjudication_reason": _name_reason(ticker, len(docs)),
            }
        )
    frame = pd.DataFrame(rows).sort_values("rank").reset_index(drop=True)
    if len(frame) != 15 or frame["coverage_state"].value_counts().to_dict() != {
        "unresolved": 14,
        "failed_request": 1,
    }:
        raise US1BContractError("name-level deterministic states drifted")
    return frame


def _render_report(names: pd.DataFrame, inventory: dict[str, Any]) -> str:
    lines = [
        "# US Free-Data Product — US1B Evidence Derivative",
        "",
        "## Immutable parent and outcome",
        "",
        (
            f"This is a non-overwriting evidence derivative of US1A manifest `{US1A_MANIFEST_SHA256}`. "
            "The exact 15 identities, ranks, holdings, scores, gates, liquidity decisions, and 1/15 weights are unchanged."
        ),
        "",
        (
            f"The approved plan `{REQUEST_PLAN_SHA256}` completed 36/36 unique requests with "
            f"{inventory['attempt_count']} attempts and {inventory['aggregate_received_body_bytes']:,} response-body bytes. "
            "All 36 returned HTTP 200. Fifteen D1 submissions, one D1 SEC exchange index, and 13 E1 document responses were reused without a request."
        ),
        "",
        (
            "Deterministic name states are 0 covered, 14 unresolved, 0 conflicting, 0 unsupported, "
            "1 failed-request, and 0 event_evidence_not_collected. HPK is the failed-request state because its preserved Form 25-NSE HTTP 503 was not retried."
        ),
        "",
        "## Frozen shortlist with evidence status",
        "",
        "| Rank | Ticker | Weight | Market directory | Primary docs | State | Action | Human review |",
        "|---:|---|---:|---|---:|---|---|---|",
    ]
    for row in names.itertuples(index=False):
        lines.append(
            f"| {row.rank} | {row.ticker} | {row.weight:.12f} | present | "
            f"{row.retrieved_primary_document_count}/{row.planned_primary_document_count} | "
            f"{row.coverage_state} | {row.deterministic_action} | yes |"
        )
    lines.extend(["", "## Evidence-backed name notes", ""])
    for row in names.itertuples(index=False):
        claim_ids = json.loads(row.cited_claim_ids)
        citations = ", ".join(f"`{claim}`" for claim in claim_ids) or "none"
        lines.append(
            f"- **{row.ticker}** — {row.adjudication_reason} Claim IDs: {citations}."
        )
    lines.extend(
        [
            "",
            "## Deterministic interpretation boundary",
            "",
            (
                "SEC acceptance timestamps determine publication eligibility. Evidence published after "
                "the 2026-07-02 decision is post-decision review only and cannot back-propagate into selection. "
                "The Nasdaq Trader files were publication-eligible by their Last-Modified headers and corroborate "
                "ticker/exchange presence only; presence or absence is never event proof."
            ),
            "",
            (
                "Every claim is accession-bound in `outputs/document_claims.parquet` and carries its source ID, "
                "URL, publication/retrieval times, response SHA-256, evidence path, normalized-text locator, "
                "supporting passage, and passage SHA-256. Narrative cannot change an action."
            ),
            "",
            "## Limitations",
            "",
            "- Data: accepted P2 remains the baseline; D1 remains unsupported for frozen M1C because of its 242 P2-only and 76 D1-only stable IDs.",
            "- Model: no fitting, tuning, rescoring, reranking, or winner replacement occurred.",
            "- Liquidity: the exact US1A/P4 liquidity evidence and decisions are unchanged and were not refreshed.",
            "- Identity: SEC CIK/ticker/exchange evidence is exact, while market directories lack CIK and are corroboration only.",
            "- Event: a one-year frozen SEC form/item filter is not proof of event absence; foreign-issuer or uncaptured filings may remain outside the candidate set.",
            "- Survivorship: free evidence is not a provider-certified historical security/action ledger and does not make the route comprehensively survivorship-free.",
            "- Performance: no NAV, return, CAGR, Sharpe, drawdown, turnover, scenario, or future-performance claim was calculated or rerun.",
            "",
        ]
    )
    return "\n".join(lines)


def _preserved_boundary_summary() -> dict[str, Any]:
    verification = verify_product_artifact(US1A_ROOT)
    if (
        verification["manifest_sha256"] != US1A_MANIFEST_SHA256
        or verification["record_count"] != 32
        or verification["record_bytes"] != 37_863_802
    ):
        raise US1BContractError("US1A post-collection boundary drifted")
    e1 = verify_event_review_adjudication_artifact(E1_ADJUDICATION_ROOT)
    return {
        "status": "pass",
        "us1a": verification,
        "e1_adjudication": e1,
        "d1_status": "unsupported_for_US1A_frozen_M1_route",
        "performance_rerun": False,
    }


def finalize_artifact(
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
) -> Path:
    """Extract, adjudicate, report, and seal the collected US1B artifact."""
    artifact_root = artifact_root.resolve()
    if (artifact_root / "manifest.json").exists():
        raise FileExistsError("US1B artifact is already finalized")
    inventory_path = artifact_root / "requests/request_inventory.json"
    if not inventory_path.is_file():
        raise US1BContractError("US1B collection is incomplete")
    inventory = json.loads(inventory_path.read_text())
    if (
        inventory["request_plan_sha256"] != REQUEST_PLAN_SHA256
        or len(inventory["logical_requests"]) != 36
        or len(inventory["attempts"]) != 36
        or any(
            row["request_status"] != "retrieved"
            for row in inventory["logical_requests"]
        )
    ):
        raise US1BContractError("collected request reconciliation drifted")
    shortlist = _load_shortlist()
    market = _build_market_evidence(artifact_root, inventory, shortlist)
    documents, claims = _build_document_and_claim_tables(
        artifact_root, inventory, shortlist
    )
    names = _build_name_adjudication(shortlist, documents, claims, market)

    derivative = shortlist.merge(
        names[
            [
                "stable_row_id",
                "coverage_state",
                "deterministic_action",
                "human_review_required",
                "cited_claim_ids",
                "adjudication_reason",
            ]
        ],
        on="stable_row_id",
        how="left",
        validate="one_to_one",
        suffixes=("", "_us1b"),
    )
    baseline_columns = list(shortlist.columns)
    pd.testing.assert_frame_equal(
        derivative[baseline_columns].reset_index(drop=True),
        shortlist[baseline_columns].reset_index(drop=True),
        check_exact=True,
    )
    boundary = _preserved_boundary_summary()

    final_targets = (
        "outputs/market_exchange_evidence.parquet",
        "outputs/document_level_adjudication.parquet",
        "outputs/document_claims.parquet",
        "outputs/name_level_adjudication.parquet",
        "outputs/event_evidence_coverage_15.parquet",
        "outputs/final_shortlist_2026_evidence_derivative.parquet",
        "requests/logical_request_statuses.parquet",
        "requests/attempt_inventory.parquet",
        "requests/reused_response_inventory.parquet",
        "support/collection_resume_lineage.json",
        "support/adjudication_summary.json",
        "lineage/preserved_boundaries.json",
        "lineage/source_code_lineage.json",
        "methodology.md",
        "report/product_report_evidence.md",
        "rebuild_or_verify.json",
        "state/03_adjudication_complete.json",
    )
    existing_targets = [
        item for item in final_targets if (artifact_root / item).exists()
    ]
    if existing_targets:
        raise FileExistsError(
            f"US1B finalization target already exists: {existing_targets}"
        )

    output_dir = artifact_root / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    market.to_parquet(output_dir / "market_exchange_evidence.parquet", index=False)
    documents.to_parquet(
        output_dir / "document_level_adjudication.parquet", index=False
    )
    claims.to_parquet(output_dir / "document_claims.parquet", index=False)
    names.to_parquet(output_dir / "name_level_adjudication.parquet", index=False)
    names.to_parquet(output_dir / "event_evidence_coverage_15.parquet", index=False)
    derivative.to_parquet(
        output_dir / "final_shortlist_2026_evidence_derivative.parquet",
        index=False,
    )

    request_dir = artifact_root / "requests"
    pd.DataFrame(inventory["logical_requests"]).to_parquet(
        request_dir / "logical_request_statuses.parquet", index=False
    )
    attempts_frame = pd.DataFrame(inventory["attempts"]).copy()
    for column in ("request_headers", "response_headers"):
        attempts_frame[column] = attempts_frame[column].map(
            lambda value: json.dumps(value, sort_keys=True)
        )
    attempts_frame.to_parquet(
        request_dir / "attempt_inventory.parquet", index=False
    )
    reused_frame = pd.DataFrame(inventory["reused_responses"]).copy()
    reused_frame.to_parquet(
        request_dir / "reused_response_inventory.parquet", index=False
    )
    _write_json(
        artifact_root / "support/collection_resume_lineage.json",
        {
            "status": "recovered_without_repeating_a_request",
            "first_execution_completed_orders": [1, 2],
            "local_failure_before_order": 3,
            "local_failure_class": "AttributeError",
            "local_failure_message": "Pandas NaN endpoint dispatch before the first SEC request",
            "resume_started_at_order": 3,
            "repeated_request_orders": [],
            "final_unique_url_count": 36,
            "final_attempt_count": 36,
        },
    )
    _write_json(artifact_root / "lineage/preserved_boundaries.json", boundary)
    code_paths = [
        ROOT / "portfolio/us1b_frozen_evidence.py",
        ROOT / "workflows/build_us1b_frozen_evidence.py",
        ROOT / "tests/portfolio/test_us1b_frozen_evidence.py",
    ]
    _write_json(
        artifact_root / "lineage/source_code_lineage.json",
        {
            "repository_root": str(ROOT),
            "worktree_state": "uncommitted_preserved_session_work",
            "source_files": [
                {
                    "path": path.relative_to(ROOT).as_posix(),
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
                for path in code_paths
                if path.is_file()
            ],
        },
    )
    _write_json(
        artifact_root / "support/adjudication_summary.json",
        {
            "document_rows": len(documents),
            "claim_rows": len(claims),
            "document_states": {
                str(key): int(value)
                for key, value in documents["document_state"].value_counts().items()
            },
            "name_rows": len(names),
            "name_states": {
                str(key): int(value)
                for key, value in names["coverage_state"].value_counts().items()
            },
            "market_rows": len(market),
            "external_unique_urls": 36,
            "external_attempts": 36,
            "external_http_200": 36,
            "response_body_bytes": inventory["aggregate_received_body_bytes"],
            "us1a_shortlist_unchanged": True,
            "performance_calculated": False,
        },
    )
    methodology = f"""# US1B Methodology

The immutable parent is US1A `{US1A_MANIFEST_SHA256}`. The approved request
plan is `{REQUEST_PLAN_SHA256}`. Fifteen exact D1 SEC submissions and the D1
SEC CIK/ticker/exchange index were reused. Thirteen exact E1 document responses
were reused without retry, including HPK's HTTP 503. Only the approved 34 SEC
primary documents and two Nasdaq Trader symbol directories were requested.

Publication eligibility, exact-security scope, effective-time precision,
terms completeness, conflict, status precedence, and human-review rules are
frozen in `configuration/evidence_contract.json`. Rules were applied before
narrative. Ticker presence, disappearance, name similarity, filing family,
model knowledge, and uncited narrative cannot establish an event.

The derivative adds evidence fields only. It does not alter a US1A identity,
rank, holding, score, gate, liquidity decision, or weight and calculates no
performance.
"""
    (artifact_root / "methodology.md").write_text(methodology)
    report_dir = artifact_root / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "product_report_evidence.md").write_text(
        _render_report(names, inventory)
    )
    _write_json(
        artifact_root / "rebuild_or_verify.json",
        {
            "offline_verification_command": (
                "python3 -m portfolio.us1b_frozen_evidence "
                f"--artifact-root artifacts/product/us_free_v1_evidence/{VERSION} "
                "--verify-only"
            ),
            "network_required": False,
            "rebuild_requires_new_approval": True,
        },
    )
    _write_json(
        artifact_root / "state/03_adjudication_complete.json",
        {
            "status": "pass",
            "document_rows": 47,
            "claim_rows": 46,
            "name_rows": 15,
            "shortlist_changed": False,
            "performance_calculated": False,
        },
    )

    record_paths = sorted(
        path
        for path in artifact_root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    )
    records = [
        _record(
            artifact_root,
            path,
            "raw_or_generated_US1B_record"
            if "raw" in path.parts
            else "contract_lineage_output_or_report",
        )
        for path in record_paths
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": "US_FREE_DATA_PRODUCT_EVIDENCE_DERIVATIVE_US1B",
        "version": VERSION,
        "created_at_utc": utc_now(),
        "parent_us1a": {
            "artifact_root": "artifacts/product/us_free_v1/20260801T183000Z-us1a",
            "manifest_sha256": US1A_MANIFEST_SHA256,
            "record_count": 32,
            "record_bytes": 37_863_802,
        },
        "request_plan_sha256": REQUEST_PLAN_SHA256,
        "claim": {
            "external_unique_urls": 36,
            "external_attempts": 36,
            "external_http_200": 36,
            "external_response_body_bytes": inventory[
                "aggregate_received_body_bytes"
            ],
            "reused_responses": len(inventory["reused_responses"]),
            "document_rows": 47,
            "claim_rows": 46,
            "name_rows": 15,
            "covered_names": 0,
            "unresolved_names": 14,
            "conflicting_names": 0,
            "unsupported_names": 0,
            "failed_request_names": 1,
            "event_evidence_not_collected_names": 0,
            "us1a_shortlist_unchanged": True,
            "model_executed": False,
            "performance_calculated": False,
            "preserved_artifact_overwritten": False,
        },
        "records": records,
    }
    _write_json(artifact_root / "manifest.json", manifest)
    return artifact_root / "manifest.json"


def verify_artifact(
    artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
    *,
    expected_manifest_sha256: str | None = None,
    reverify_boundaries: bool = True,
) -> dict[str, Any]:
    """Independently verify every generated/raw record and frozen boundary."""
    artifact_root = artifact_root.resolve()
    manifest_path = artifact_root / "manifest.json"
    manifest_sha = sha256_file(manifest_path)
    if expected_manifest_sha256 and manifest_sha != expected_manifest_sha256:
        raise US1BContractError("US1B manifest SHA-256 mismatch")
    manifest = json.loads(manifest_path.read_text())
    record_bytes = 0
    for item in manifest["records"]:
        path = artifact_root / str(item["path"])
        if (
            not path.is_file()
            or path.stat().st_size != int(item["size_bytes"])
            or sha256_file(path) != item["sha256"]
        ):
            raise US1BContractError(f"US1B record mismatch: {item['path']}")
        record_bytes += int(item["size_bytes"])
    plan = json.loads(
        (artifact_root / "requests/frozen_request_plan.json").read_text()
    )
    if _canonical_sha(plan) != REQUEST_PLAN_SHA256:
        raise US1BContractError("frozen plan mismatch")
    inventory = json.loads(
        (artifact_root / "requests/request_inventory.json").read_text()
    )
    if len(inventory["attempts"]) != 36 or len(
        inventory["logical_requests"]
    ) != 36:
        raise US1BContractError("request inventory count mismatch")
    raw_failures: list[str] = []
    for item in inventory["attempts"]:
        if item["stored_path"]:
            path = artifact_root / str(item["stored_path"])
            payload = _read_gzip_payload(artifact_root, str(item["stored_path"]))
            if (
                len(payload) != int(item["response_size_bytes"])
                or hashlib.sha256(payload).hexdigest()
                != item["response_sha256"]
                or path.stat().st_size != int(item["stored_size_bytes"])
                or sha256_file(path) != item["stored_sha256"]
            ):
                raw_failures.append(str(item["attempt_id"]))
    for item in inventory["reused_responses"]:
        path = artifact_root / str(item["stored_path"])
        payload = _read_gzip_payload(artifact_root, str(item["stored_path"]))
        if (
            not path.is_file()
            or len(payload) != int(item["response_size_bytes"])
            or hashlib.sha256(payload).hexdigest() != item["response_sha256"]
            or path.stat().st_size != int(item["stored_size_bytes"])
            or sha256_file(path) != item["stored_sha256"]
        ):
            raw_failures.append(str(item["source_id"]))
    if raw_failures:
        raise US1BContractError(f"raw response verification failed: {raw_failures}")
    documents = pd.read_parquet(
        artifact_root / "outputs/document_level_adjudication.parquet"
    )
    claims = pd.read_parquet(artifact_root / "outputs/document_claims.parquet")
    names = pd.read_parquet(
        artifact_root / "outputs/name_level_adjudication.parquet"
    )
    market = pd.read_parquet(
        artifact_root / "outputs/market_exchange_evidence.parquet"
    )
    document_response_by_id = documents.set_index("document_id")[
        "source_response_sha256"
    ].to_dict()
    for claim in claims.itertuples(index=False):
        payload = _read_gzip_payload(artifact_root, str(claim.evidence_path))
        normalized = _normalized_document_text(payload)
        passage = str(claim.supporting_passage)
        if (
            passage not in normalized
            or hashlib.sha256(passage.encode()).hexdigest()
            != str(claim.supporting_passage_sha256)
            or hashlib.sha256(payload).hexdigest()
            != str(claim.source_response_sha256)
            or document_response_by_id.get(str(claim.document_id))
            != str(claim.source_response_sha256)
        ):
            raise US1BContractError(f"claim lineage drifted: {claim.claim_id}")
        locator_match = re.fullmatch(
            r"normalized_text_chars:(\d+)-(\d+);(?:occurrence:1|passage_sha256:[0-9a-f]{64})",
            str(claim.document_locator),
        )
        if locator_match and normalized[
            int(locator_match.group(1)) : int(locator_match.group(2))
        ] != passage:
            raise US1BContractError(f"claim locator drifted: {claim.claim_id}")
    derivative = pd.read_parquet(
        artifact_root / "outputs/final_shortlist_2026_evidence_derivative.parquet"
    ).sort_values("rank").reset_index(drop=True)
    original = _load_shortlist().sort_values("rank").reset_index(drop=True)
    pd.testing.assert_frame_equal(
        derivative[list(original.columns)], original, check_exact=True
    )
    if (
        len(documents) != 47
        or len(claims) != 46
        or len(names) != 15
        or len(market) != 15
        or documents["document_state"].value_counts().to_dict()
        != {"unsupported": 33, "unresolved": 13, "failed_request": 1}
        or names["coverage_state"].value_counts().to_dict()
        != {"unresolved": 14, "failed_request": 1}
        or not names["human_review_required"].all()
        or names["holding_changed"].any()
        or names["rank_changed"].any()
        or names["weight_changed"].any()
    ):
        raise US1BContractError("US1B adjudication/immutability mismatch")
    if reverify_boundaries:
        _preserved_boundary_summary()
    return {
        "manifest_sha256": manifest_sha,
        "record_count": len(manifest["records"]),
        "record_bytes": record_bytes,
        "raw_collected_responses_verified": len(inventory["attempts"]),
        "raw_reused_responses_verified": len(inventory["reused_responses"]),
        "external_unique_urls": 36,
        "external_attempts": 36,
        "external_http_200": 36,
        "external_response_body_bytes": inventory[
            "aggregate_received_body_bytes"
        ],
        "document_rows": 47,
        "claim_rows": 46,
        "market_rows": 15,
        "unresolved_names": 14,
        "failed_request_names": 1,
        "shortlist_rows": 15,
        "shortlist_unchanged": True,
        "performance_calculated": False,
        "preserved_boundaries_reverified": reverify_boundaries,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--freeze-only", action="store_true")
    parser.add_argument("--collect-only", action="store_true")
    parser.add_argument("--finalize", action="store_true")
    parser.add_argument("--approval-token")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--expected-manifest-sha256")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    modes = sum(
        (args.freeze_only, args.collect_only, args.finalize, args.verify_only)
    )
    if modes != 1:
        raise SystemExit(
            "choose exactly one of --freeze-only, --collect-only, "
            "--finalize, --verify-only"
        )
    if args.freeze_only:
        root = freeze_contract(args.artifact_root)
        print(
            json.dumps(
                {
                    "artifact_root": str(root),
                    "request_plan_sha256": REQUEST_PLAN_SHA256,
                    "external_requests_made": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return
    if args.collect_only:
        path = collect_approved_evidence(
            args.artifact_root,
            approval_token=str(args.approval_token or ""),
        )
        print(path.read_text())
        return
    if args.finalize:
        path = finalize_artifact(args.artifact_root)
        print(path.read_text())
        return
    print(
        json.dumps(
            verify_artifact(
                args.artifact_root,
                expected_manifest_sha256=args.expected_manifest_sha256,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
