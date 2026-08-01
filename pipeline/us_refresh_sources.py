"""Versioned source and contract stages for the US canonical refresh.

This module is deliberately separate from the legacy six-step and
international collectors. Every stage writes below a caller-supplied,
version-specific artifact root and refuses to replace a completed manifest.
HTTP response bytes are stored before parsing and are hash-addressed through
append-only response manifests.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from pipeline.build_contract_label_inputs import (
    BENCHMARKS,
    ENDPOINT as YAHOO_ENDPOINT,
    EXCHANGE_CALENDARS,
    HORIZONS,
    PROVIDER_EXCHANGES,
    START_DATE,
    _request_params,
    provider_symbol,
)
from pipeline.event_time_cohorts import proven_availability
from pipeline.step2_artifact_rebuild import sha256_file


ROOT = Path(__file__).resolve().parents[1]
BASELINE_CALENDAR_CONTRACT = (
    ROOT
    / "artifacts/pit_validation/calendar_contract/"
    "session8b_calendar_contract.json"
)
SEC_HEADERS = {
    "User-Agent": "CanonicalUSRefresh research@alpharesearch.io",
    "Accept": "application/json",
}
SEC_UNIVERSE_ENDPOINTS = {
    "company_tickers": "https://www.sec.gov/files/company_tickers.json",
    "company_tickers_exchange": (
        "https://www.sec.gov/files/company_tickers_exchange.json"
    ),
}
SEC_SUBMISSIONS_ENDPOINT = (
    "https://data.sec.gov/submissions/CIK{cik}.json"
)
REFRESH_ID_PATTERN = re.compile(r"^[0-9]{8}T[0-9]{6}Z(?:-[a-z0-9-]+)?$")
_MANIFEST_LOCK = threading.Lock()
_SEC_THREAD_LOCAL = threading.local()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_refresh_id(refresh_id: str) -> str:
    if not REFRESH_ID_PATTERN.fullmatch(refresh_id):
        raise ValueError(
            "refresh_id must be an immutable UTC identifier such as "
            "20260730T143000Z"
        )
    return refresh_id


def artifact_record(base: Path, path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(base).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _MANIFEST_LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())


class _RateLimiter:
    def __init__(self, interval_seconds: float):
        self.interval_seconds = max(0.0, interval_seconds)
        self._lock = threading.Lock()
        self._next_start = 0.0

    def wait(self) -> None:
        if not self.interval_seconds:
            return
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next_start - now)
            self._next_start = max(now, self._next_start) + self.interval_seconds
        if delay:
            time.sleep(delay)


def _thread_sec_session() -> requests.Session:
    if not hasattr(_SEC_THREAD_LOCAL, "session"):
        _SEC_THREAD_LOCAL.session = requests.Session()
    return _SEC_THREAD_LOCAL.session


def _gzip_bytes_exclusive(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as output:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=output,
            mtime=0,
        ) as compressed:
            compressed.write(payload)


def _verify_raw_record(artifact_root: Path, item: dict[str, Any]) -> bytes:
    path = artifact_root / item["stored_path"]
    if (
        not path.is_file()
        or path.stat().st_size != item["stored_size_bytes"]
        or sha256_file(path) != item["stored_sha256"]
    ):
        raise RuntimeError(f"stored response mismatch: {path}")
    with gzip.open(path, "rb") as handle:
        payload = handle.read()
    if (
        len(payload) != item["response_size_bytes"]
        or hashlib.sha256(payload).hexdigest() != item["response_sha256"]
    ):
        raise RuntimeError(f"decompressed response mismatch: {path}")
    return payload


def _verify_failure_attempts(
    artifact_root: Path,
    records: dict[str, dict[str, Any]],
) -> None:
    for item in records.values():
        for attempt in item.get("attempts", []):
            relative = attempt.get("stored_failure_path")
            size_key = "stored_failure_size_bytes"
            hash_key = "stored_failure_sha256"
            if relative is None and str(attempt.get("stored_path", "")).startswith(
                "raw/failures/"
            ):
                relative = attempt["stored_path"]
                size_key = "stored_size_bytes"
                hash_key = "stored_sha256"
            if relative is None:
                continue
            path = artifact_root / relative
            if (
                not path.is_file()
                or path.stat().st_size != attempt[size_key]
                or sha256_file(path) != attempt[hash_key]
            ):
                raise RuntimeError(f"stored failure response mismatch: {path}")
            with gzip.open(path, "rb") as handle:
                payload = handle.read()
            response_size_bytes = attempt.get(
                "response_size_bytes",
                attempt.get("bytes"),
            )
            if response_size_bytes is None:
                raise RuntimeError(
                    f"failure response size is not recorded: {path}"
                )
            if (
                len(payload) != response_size_bytes
                or hashlib.sha256(payload).hexdigest()
                != attempt["response_sha256"]
            ):
                raise RuntimeError(
                    f"decompressed failure response mismatch: {path}"
                )


def _latest_records(
    manifest_path: Path,
    *,
    key: str,
) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    if not manifest_path.is_file():
        return latest
    for line_number, line in enumerate(
        manifest_path.read_text().splitlines(),
        1,
    ):
        item = json.loads(line)
        value = str(item[key])
        if (
            value in latest
            and latest[value].get("status") == "success"
        ):
            raise RuntimeError(
                f"response follows success for {value} at line {line_number}"
            )
        latest[value] = item
    return latest


def _request_and_store(
    *,
    session: requests.Session,
    artifact_root: Path,
    logical_key: str,
    url: str,
    stored_relative_path: str,
    manifest_path: Path,
    prior: dict[str, dict[str, Any]],
    retries: int,
    rate_limiter: _RateLimiter | None = None,
) -> tuple[dict[str, Any], bytes | None]:
    previous = prior.get(logical_key)
    if previous and previous.get("status") == "success":
        return previous, _verify_raw_record(artifact_root, previous)

    attempts = []
    for attempt in range(1, retries + 1):
        started_at = utc_now()
        try:
            if rate_limiter is not None:
                rate_limiter.wait()
            response = session.get(
                url,
                headers=SEC_HEADERS,
                timeout=45,
            )
            payload = response.content
            completed_at = utc_now()
            status = int(response.status_code)
            response_path = Path(stored_relative_path)
            if status != 200:
                response_path = (
                    Path("raw/failures")
                    / f"{logical_key}.attempt{attempt}.{time.time_ns()}.bin.gz"
                )
            target = artifact_root / response_path
            _gzip_bytes_exclusive(target, payload)
            attempt_record = {
                "attempt": attempt,
                "started_at_utc": started_at,
                "completed_at_utc": completed_at,
                "http_status": status,
                "response_size_bytes": len(payload),
                "response_sha256": hashlib.sha256(payload).hexdigest(),
                "stored_path": response_path.as_posix(),
                "stored_size_bytes": target.stat().st_size,
                "stored_sha256": sha256_file(target),
            }
            attempts.append(attempt_record)
            item = {
                "logical_key": logical_key,
                "request_url": str(response.url),
                "retrieved_at_utc": completed_at,
                "http_status": status,
                "status": "success" if status == 200 else "failure",
                "response_headers": {
                    name: response.headers.get(name)
                    for name in (
                        "Date",
                        "ETag",
                        "Last-Modified",
                        "Cache-Control",
                        "Content-Type",
                    )
                    if response.headers.get(name)
                },
                **{
                    name: attempt_record[name]
                    for name in (
                        "response_size_bytes",
                        "response_sha256",
                        "stored_path",
                        "stored_size_bytes",
                        "stored_sha256",
                    )
                },
                "attempts": attempts,
            }
            if status != 200:
                item["failure_reason"] = f"http_{status}"
            _append_jsonl(manifest_path, item)
            prior[logical_key] = item
            if status == 200:
                return item, payload
            if status not in {408, 429, 500, 502, 503, 504}:
                return item, None
        except Exception as exc:
            attempts.append(
                {
                    "attempt": attempt,
                    "started_at_utc": started_at,
                    "completed_at_utc": utc_now(),
                    "error": f"{type(exc).__name__}:{exc}",
                }
            )
        if attempt < retries:
            time.sleep(min(8, 2 ** (attempt - 1)))
    item = {
        "logical_key": logical_key,
        "request_url": url,
        "retrieved_at_utc": utc_now(),
        "status": "failure",
        "failure_reason": "transport_or_retry_exhausted",
        "attempts": attempts,
    }
    _append_jsonl(manifest_path, item)
    prior[logical_key] = item
    return item, None


def _parse_universe_indices(
    company_tickers_payload: bytes,
    exchange_payload: bytes,
) -> pd.DataFrame:
    company_tickers = json.loads(company_tickers_payload)
    exchange_document = json.loads(exchange_payload)
    exchange_map = {
        str(row[0]).zfill(10): row[3] if len(row) > 3 and row[3] else "OTC"
        for row in exchange_document.get("data", [])
    }
    rows = []
    for key in sorted(company_tickers, key=lambda value: int(value)):
        item = company_tickers[key]
        rows.append(
            {
                "cik": str(item["cik_str"]).zfill(10),
                "ticker": str(item.get("ticker", "")).strip().upper(),
                "name": str(item.get("title", "")),
                "exchange": exchange_map.get(
                    str(item["cik_str"]).zfill(10),
                    "OTC",
                ),
            }
        )
    frame = pd.DataFrame(rows).drop_duplicates("cik", keep="first")
    if frame.empty or not frame["cik"].str.fullmatch(r"\d{10}").all():
        raise RuntimeError("SEC universe payload did not produce valid CIKs")
    return frame.reset_index(drop=True)


def collect_us_universe(
    artifact_root: Path,
    *,
    refresh_id: str,
    session: requests.Session | None = None,
    retries: int = 3,
    rate_delay: float = 0.12,
    limit: int | None = None,
    workers: int = 1,
) -> Path:
    """Collect and version the US SEC universe without touching data/tickers."""
    validate_refresh_id(refresh_id)
    artifact_root = artifact_root.resolve()
    manifest = artifact_root / "manifest.json"
    if manifest.exists():
        raise RuntimeError(
            f"universe version is already complete: {artifact_root}"
        )
    artifact_root.mkdir(parents=True, exist_ok=True)
    response_manifest = artifact_root / "raw/response_manifest.jsonl"
    prior = _latest_records(response_manifest, key="logical_key")
    client = session or requests.Session()
    limiter = _RateLimiter(rate_delay)

    index_payloads: dict[str, bytes] = {}
    for name, url in SEC_UNIVERSE_ENDPOINTS.items():
        _, payload = _request_and_store(
            session=client,
            artifact_root=artifact_root,
            logical_key=name,
            url=url,
            stored_relative_path=f"raw/index/{name}.json.gz",
            manifest_path=response_manifest,
            prior=prior,
            retries=retries,
            rate_limiter=limiter,
        )
        if payload is None:
            raise RuntimeError(f"required SEC universe source unavailable: {name}")
        index_payloads[name] = payload

    universe = _parse_universe_indices(
        index_payloads["company_tickers"],
        index_payloads["company_tickers_exchange"],
    )
    if limit is not None:
        universe = universe.head(limit).copy()

    sic_codes: dict[str, Any] = {}
    sic_descriptions: dict[str, str] = {}
    submission_status: dict[str, str] = {}
    if workers < 1:
        raise ValueError("workers must be positive")

    def collect_submission(cik: str) -> tuple[str, Any, str, str]:
        logical_key = f"submission_{cik}"
        local_prior = (
            {logical_key: prior[logical_key]}
            if logical_key in prior
            else {}
        )
        _, payload = _request_and_store(
            session=session or _thread_sec_session(),
            artifact_root=artifact_root,
            logical_key=logical_key,
            url=SEC_SUBMISSIONS_ENDPOINT.format(cik=cik),
            stored_relative_path=f"raw/submissions/CIK{cik}.json.gz",
            manifest_path=response_manifest,
            prior=local_prior,
            retries=retries,
            rate_limiter=limiter,
        )
        if payload is None:
            return cik, None, "", "unavailable"
        document = json.loads(payload)
        return (
            cik,
            document.get("sic"),
            str(document.get("sicDescription", "")),
            "supported",
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(collect_submission, str(cik)): str(cik)
            for cik in universe["cik"]
        }
        for future in as_completed(futures):
            cik, sic, description, status = future.result()
            sic_codes[cik] = sic
            sic_descriptions[cik] = description
            submission_status[cik] = status

    universe["sic_code"] = universe["cik"].map(sic_codes)
    universe["sic_description"] = universe["cik"].map(sic_descriptions)
    universe["submission_status"] = universe["cik"].map(submission_status)
    universe["market"] = "US"
    universe["country"] = "United States"
    universe["accounting_std"] = "GAAP"
    columns = [
        "cik",
        "ticker",
        "name",
        "exchange",
        "sic_code",
        "sic_description",
        "submission_status",
        "market",
        "country",
        "accounting_std",
    ]
    universe = universe[columns]
    universe_path = artifact_root / "outputs/universe.parquet"
    universe_path.parent.mkdir(parents=True, exist_ok=True)
    universe.to_parquet(universe_path, index=False)

    latest = _latest_records(response_manifest, key="logical_key")
    _verify_failure_attempts(artifact_root, latest)
    raw_records = []
    for item in latest.values():
        if item.get("stored_path"):
            _verify_raw_record(artifact_root, item)
            raw_records.append(
                artifact_record(
                    artifact_root,
                    artifact_root / item["stored_path"],
                    "exact_sec_http_response",
                )
            )
    for path in sorted((artifact_root / "raw/failures").glob("*.gz")):
        raw_records.append(
            artifact_record(
                artifact_root,
                path,
                "exact_sec_failure_response",
            )
        )
    raw_inventory = artifact_root / "raw/raw_inventory.json"
    raw_inventory.write_text(
        json.dumps(sorted(raw_records, key=lambda item: item["path"]), indent=2)
        + "\n"
    )
    collection_times = [
        item["retrieved_at_utc"]
        for item in latest.values()
        if item.get("retrieved_at_utc")
    ]
    version = {
        "schema_version": 1,
        "refresh_id": refresh_id,
        "created_at_utc": utc_now(),
        "universe_rows": len(universe),
        "unique_ciks": int(universe["cik"].nunique()),
        "supported_submission_rows": int(
            universe["submission_status"].eq("supported").sum()
        ),
        "collection_timestamp_min_utc": min(collection_times),
        "collection_timestamp_max_utc": max(collection_times),
        "legacy_ticker_file_modified": False,
    }
    version_path = artifact_root / "universe_version.json"
    version_path.write_text(json.dumps(version, indent=2) + "\n")
    manifest_payload = {
        "schema_version": 1,
        "artifact_class": "VERSIONED_US_SEC_UNIVERSE",
        "created_at_utc": version["created_at_utc"],
        "refresh_id": refresh_id,
        "source_endpoints": {
            **SEC_UNIVERSE_ENDPOINTS,
            "submissions": SEC_SUBMISSIONS_ENDPOINT,
        },
        "records": [
            artifact_record(
                artifact_root,
                response_manifest,
                "append_only_response_manifest",
            ),
            artifact_record(
                artifact_root,
                raw_inventory,
                "raw_response_inventory",
            ),
            artifact_record(
                artifact_root,
                universe_path,
                "versioned_us_universe",
            ),
            artifact_record(
                artifact_root,
                version_path,
                "universe_version_contract",
            ),
        ],
        "raw_responses": {
            "latest_requests": len(latest),
            "successful": sum(
                item.get("status") == "success" for item in latest.values()
            ),
            "failed": sum(
                item.get("status") != "success" for item in latest.values()
            ),
            "exact_payload_hashes_verified": True,
        },
        "version": version,
        "promotion_status": "source_evidence_only",
    }
    manifest.write_text(json.dumps(manifest_payload, indent=2) + "\n")
    return manifest


def _load_complete_manifest(path: Path, artifact_class: str) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(f"required manifest is missing: {path}")
    payload = json.loads(path.read_text())
    if payload.get("artifact_class") != artifact_class:
        raise RuntimeError(f"unexpected artifact class: {path}")
    return payload


def freeze_step2_candidate(
    *,
    artifact_root: Path,
    universe_root: Path,
    refresh_id: str,
) -> Path:
    """Validate a completed corrected Step 2 refresh and freeze its manifest."""
    validate_refresh_id(refresh_id)
    artifact_root = artifact_root.resolve()
    manifest_path = artifact_root / "manifest.json"
    if manifest_path.exists():
        raise RuntimeError(f"Step 2 candidate is already frozen: {artifact_root}")
    universe_manifest_path = universe_root / "manifest.json"
    _load_complete_manifest(
        universe_manifest_path,
        "VERSIONED_US_SEC_UNIVERSE",
    )
    universe_path = universe_root / "outputs/universe.parquet"
    frozen_universe = artifact_root / "inputs/tickers.parquet"
    if sha256_file(universe_path) != sha256_file(frozen_universe):
        raise RuntimeError("Step 2 did not freeze the versioned universe exactly")
    universe = pd.read_parquet(frozen_universe)
    expected_ciks = set(universe["cik"].astype(str))
    checkpoint_path = artifact_root / "checkpoints/checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text())
    if set(checkpoint["completed_ciks"]) != expected_ciks:
        raise RuntimeError("Step 2 checkpoint does not cover the universe")

    response_path = artifact_root / "raw/response_manifest.jsonl"
    latest = _latest_records(response_path, key="cik")
    if set(latest) != expected_ciks:
        raise RuntimeError("Step 2 response coverage does not match the universe")
    _verify_failure_attempts(artifact_root, latest)
    raw_records = []
    raw_dir = artifact_root / "raw/companyfacts"
    for cik, item in sorted(latest.items()):
        if item.get("stored_name"):
            path = raw_dir / item["stored_name"]
            if (
                path.stat().st_size != item["stored_size_bytes"]
                or sha256_file(path) != item["stored_sha256"]
            ):
                raise RuntimeError(f"Step 2 stored response mismatch for {cik}")
            with gzip.open(path, "rb") as handle:
                payload = handle.read()
            if (
                len(payload) != item["response_size_bytes"]
                or hashlib.sha256(payload).hexdigest()
                != item["response_sha256"]
            ):
                raise RuntimeError(f"Step 2 raw response mismatch for {cik}")
            raw_records.append(
                artifact_record(
                    artifact_root,
                    path,
                    "exact_sec_companyfacts_response",
                )
            )
    for path in sorted((artifact_root / "raw/failures").glob("*.gz")):
        raw_records.append(
            artifact_record(
                artifact_root,
                path,
                "exact_sec_companyfacts_failure_response",
            )
        )
    raw_inventory = artifact_root / "raw/raw_inventory.json"
    raw_inventory.write_text(json.dumps(raw_records, indent=2) + "\n")

    certified_path = artifact_root / "outputs/certified_snapshots.parquet"
    excluded_path = artifact_root / "outputs/excluded_periods.parquet"
    unavailable_path = artifact_root / "outputs/unavailable_entities.parquet"
    certified = pd.read_parquet(certified_path)
    required = {
        "entity_id",
        "cik",
        "fiscal_year",
        "fiscal_quarter",
        "period_type",
        "filed_date",
        "source_filing_date",
        "availability_timestamp",
        "availability_provenance",
        "market",
    }
    if not required.issubset(certified.columns):
        raise RuntimeError(
            f"corrected Step 2 schema missing {sorted(required - set(certified))}"
        )
    _, eligible = proven_availability(certified)
    if not eligible.all():
        raise RuntimeError("corrected Step 2 contains unproven availability rows")
    keys = [
        "entity_id",
        "fiscal_year",
        "period_type",
        "fiscal_quarter",
    ]
    if certified.duplicated(keys).any():
        raise RuntimeError("corrected Step 2 contains duplicate entity-periods")
    if set(certified["market"].dropna().unique()) != {"US"}:
        raise RuntimeError("corrected Step 2 is not exclusively US")

    unavailable = pd.read_parquet(unavailable_path)
    proven_entities = set(certified["cik"].astype(str))
    excluded_entities = set(
        pd.read_parquet(excluded_path)["cik"].astype(str)
    )
    unavailable_entities = set(unavailable["cik"].astype(str))
    if (
        proven_entities & unavailable_entities
        or excluded_entities & unavailable_entities
    ):
        raise RuntimeError("Step 2 row-bearing and unavailable entities overlap")
    if (
        proven_entities | excluded_entities | unavailable_entities
    ) != expected_ciks:
        raise RuntimeError("Step 2 entity classifications do not cover the universe")
    annual = certified[certified["period_type"].eq("annual")]
    if annual.empty:
        raise RuntimeError("corrected Step 2 has no annual population")
    collection_times = [
        item.get("retrieved_at_utc")
        for item in latest.values()
        if item.get("retrieved_at_utc")
    ]
    summary = {
        "schema_version": 1,
        "universe_rows": len(universe),
        "completed_ciks": len(expected_ciks),
        "certified_rows": len(certified),
        "certified_annual_rows": len(annual),
        "excluded_rows": len(pd.read_parquet(excluded_path)),
        "unavailable_entities": len(unavailable),
        "response_success": sum(
            item.get("status") == "success" for item in latest.values()
        ),
        "response_failure": sum(
            item.get("status") != "success" for item in latest.values()
        ),
        "collection_timestamp_min_utc": min(collection_times),
        "collection_timestamp_max_utc": max(collection_times),
        "availability_policy": (
            "earliest SEC primary filing; date-only filing availability at "
            "America/New_York end-of-day converted to UTC; later amendments "
            "excluded; equal-time collisions fail closed"
        ),
    }
    summary_path = artifact_root / "validation_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    records = [
        artifact_record(
            artifact_root,
            frozen_universe,
            "versioned_universe_input",
        ),
        artifact_record(
            artifact_root,
            response_path,
            "append_only_companyfacts_response_manifest",
        ),
        artifact_record(
            artifact_root,
            raw_inventory,
            "raw_companyfacts_inventory",
        ),
        artifact_record(
            artifact_root,
            checkpoint_path,
            "completed_cik_checkpoint",
        ),
        artifact_record(
            artifact_root,
            certified_path,
            "corrected_certified_snapshots",
        ),
        artifact_record(
            artifact_root,
            excluded_path,
            "excluded_periods",
        ),
        artifact_record(
            artifact_root,
            unavailable_path,
            "unavailable_entities",
        ),
        artifact_record(
            artifact_root,
            summary_path,
            "step2_validation_summary",
        ),
    ]
    manifest_payload = {
        "schema_version": 1,
        "artifact_class": "CANONICAL_US_REFRESH_CORRECTED_STEP2",
        "created_at_utc": utc_now(),
        "refresh_id": refresh_id,
        "validated_inputs": [
            {
                "path": str(universe_manifest_path),
                "sha256": sha256_file(universe_manifest_path),
            }
        ],
        "configuration": {
            "source": (
                "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
            ),
            "official_sec_company_facts_only": True,
            "availability_policy": summary["availability_policy"],
            "corrected_materializer": (
                "pipeline.step2_build_snapshots.build_period_snapshots"
            ),
        },
        "records": records,
        "raw_responses": {
            "manifested_payloads": len(raw_records),
            "every_stored_and_decompressed_hash_verified": True,
        },
        "validation": summary,
        "promotion_status": "source_evidence_only",
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2) + "\n")
    return manifest_path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_market_contracts(
    artifact_root: Path,
    *,
    end_date_exclusive: str,
) -> list[Path]:
    """Write the five explicit D1 market/decision/label contracts."""
    contracts = {
        "price_contract.json": {
            "schema_version": 1,
            "contract": "price",
            "provider": "Yahoo chart",
            "endpoint": YAHOO_ENDPOINT,
            "request_params": _request_params(end_date_exclusive),
            "raw_response_policy": "exact bytes stored before parsing",
            "raw_close": "unadjusted provider close",
            "total_return_close": "provider adjusted close only",
            "events": "preserved as evidence and never added separately",
            "decision_relation": "strictly before decision timestamp",
            "missing_data": "unavailable_no_substitution",
        },
        "benchmark_contract.json": {
            "schema_version": 1,
            "contract": "benchmark",
            "symbols": list(BENCHMARKS),
            "assignment": {
                "IWC": "market_cap < 300000000",
                "IWM": "300000000 <= market_cap < 2000000000",
                "MDY": "2000000000 <= market_cap < 10000000000",
                "SPY": "market_cap >= 10000000000",
            },
            "assignment_time": "decision",
            "common_session_required": True,
            "proxy_substitution": False,
        },
        "calendar_contract.json": {
            "schema_version": 1,
            "contract": "calendar",
            "exchange_calendars": {
                **EXCHANGE_CALENDARS,
                "benchmarks": "XNYS",
            },
            "regular_sessions_only": True,
            "unsupported_exchange": "excluded",
            "source_policy_contract": str(BASELINE_CALENDAR_CONTRACT),
        },
        "decision_contract.json": {
            "schema_version": 1,
            "contract": "decision",
            "cohort": "annual fiscal_year = decision_year - 1",
            "information_cutoff": (
                "June 30 23:59:59.999999999 America/New_York"
            ),
            "decision_timestamp": "July 2 00:00:00 UTC",
            "prediction_timestamp": "July 2 00:01:00 UTC",
            "late_filing": "excluded_no_carry_forward",
            "availability_provenance": "sec_primary_filing",
        },
        "label_support_contract.json": {
            "schema_version": 1,
            "contract": "label_support",
            "horizons_calendar_months": HORIZONS,
            "entry": (
                "first common stock/benchmark regular-session adjusted close "
                "strictly after prediction within five calendar days"
            ),
            "exit": (
                "first common close on or after entry plus horizon within ten "
                "calendar days"
            ),
            "availability": "label_end_date equals common exit close",
            "primary_population": "observed_only",
            "inferred_delisting_returns": False,
            "unsupported_outcomes": "unavailable",
        },
    }
    paths = []
    for name, payload in contracts.items():
        path = artifact_root / "contracts" / name
        _write_json(path, payload)
        paths.append(path)
    return paths


def initialize_market_candidate(
    *,
    step2_root: Path,
    artifact_root: Path,
    refresh_id: str,
    end_date_exclusive: str,
    calendar_contract_path: Path = BASELINE_CALENDAR_CONTRACT,
) -> None:
    """Initialize dynamic Session-8E-compatible inputs plus D1 contracts."""
    validate_refresh_id(refresh_id)
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"market artifact root is non-empty: {artifact_root}")
    for path in (
        "inputs",
        "configuration",
        "contracts",
        "calendar",
        "raw/chart",
        "checkpoints",
        "normalized/prices",
        "normalized/events",
        "outputs/observed_only",
        "outputs/include_policy_imputed",
        "support",
    ):
        (artifact_root / path).mkdir(parents=True, exist_ok=True)
    step2_manifest = step2_root / "manifest.json"
    _load_complete_manifest(
        step2_manifest,
        "CANONICAL_US_REFRESH_CORRECTED_STEP2",
    )
    snapshots = pd.read_parquet(
        step2_root / "outputs/certified_snapshots.parquet"
    )
    annual = snapshots[snapshots["period_type"].eq("annual")].copy()
    _, eligible = proven_availability(annual)
    if annual.empty or not eligible.all():
        raise RuntimeError("market inputs require proven corrected annual rows")
    annual.to_parquet(
        artifact_root / "inputs/certified_annual_population.parquet",
        index=False,
    )
    shutil.copy2(
        calendar_contract_path,
        artifact_root / "inputs/calendar_policy_contract.json",
    )
    mapping = annual[["entity_id", "cik", "ticker"]].drop_duplicates()
    mapping["provider_symbol"] = mapping["ticker"].map(provider_symbol)
    mapping["mapping_policy"] = np.where(
        mapping["ticker"].astype(str).str.upper().eq(
            mapping["provider_symbol"]
        ),
        "exact_uppercase",
        "dot_to_dash",
    )
    mapping.to_parquet(
        artifact_root / "inputs/security_mapping.parquet",
        index=False,
    )
    symbols = sorted(
        set(mapping["provider_symbol"].dropna()) | set(BENCHMARKS)
    )
    _write_json(artifact_root / "checkpoints/symbols.json", symbols)
    config = {
        "schema_version": 2,
        "artifact_class": "CANONICAL_US_REFRESH_MARKET_INPUTS",
        "refresh_id": refresh_id,
        "source_step2_manifest": str(step2_manifest),
        "source_step2_manifest_sha256": sha256_file(step2_manifest),
        "source_population_rows": len(annual),
        "endpoint": YAHOO_ENDPOINT,
        "request_params": _request_params(end_date_exclusive),
        "start_date": START_DATE,
        "end_date_exclusive": end_date_exclusive,
        "benchmarks": list(BENCHMARKS),
        "horizons_calendar_months": HORIZONS,
        "calendars": {**EXCHANGE_CALENDARS, "benchmarks": "XNYS"},
        "policy_imputation": "disabled_observed_only_primary",
        "created_at_utc": utc_now(),
    }
    _write_json(artifact_root / "configuration/config.json", config)
    write_market_contracts(
        artifact_root,
        end_date_exclusive=end_date_exclusive,
    )


def freeze_market_candidate(
    *,
    artifact_root: Path,
    step2_root: Path,
    refresh_id: str,
) -> Path:
    """Validate and freeze refreshed price/benchmark/label evidence."""
    validate_refresh_id(refresh_id)
    manifest_path = artifact_root / "manifest.json"
    if manifest_path.exists():
        raise RuntimeError(f"market candidate is already frozen: {artifact_root}")
    response_path = artifact_root / "raw/response_manifest.jsonl"
    latest = _latest_records(response_path, key="symbol")
    _verify_failure_attempts(artifact_root, latest)
    raw_records = []
    for symbol, item in sorted(latest.items()):
        if item.get("stored_path"):
            _verify_raw_record(artifact_root, item)
            raw_records.append(
                artifact_record(
                    artifact_root,
                    artifact_root / item["stored_path"],
                    "exact_yahoo_chart_response",
                )
            )
    for path in sorted((artifact_root / "raw/failures").glob("*.gz")):
        raw_records.append(
            artifact_record(
                artifact_root,
                path,
                "exact_yahoo_failure_response",
            )
        )
    raw_inventory = artifact_root / "raw/raw_inventory.json"
    _write_json(raw_inventory, raw_records)
    normalized_records = []
    for directory, role in (
        ("normalized/prices", "normalized_regular_session_prices"),
        ("normalized/events", "normalized_provider_events"),
    ):
        for path in sorted((artifact_root / directory).glob("*.parquet")):
            normalized_records.append(artifact_record(artifact_root, path, role))
    normalized_inventory = (
        artifact_root / "normalized/normalized_inventory.json"
    )
    _write_json(normalized_inventory, normalized_records)

    annual = pd.read_parquet(
        artifact_root / "inputs/certified_annual_population.parquet"
    )
    gate = pd.read_parquet(
        artifact_root / "support/observed_only_row_horizon.parquet"
    )
    if gate.duplicated(
        ["entity_id", "cik", "ticker", "fiscal_year", "horizon"]
    ).any():
        raise RuntimeError("label-support gate keys are not unique")
    support_rows = []
    for horizon in HORIZONS:
        frame = gate[gate["horizon"].eq(horizon)]
        counts = frame["classification"].value_counts().to_dict()
        if len(frame) != len(annual) or sum(counts.values()) != len(annual):
            raise RuntimeError(f"{horizon} support does not partition rows")
        support_rows.append(
            {
                "horizon": horizon,
                "candidate_rows": len(frame),
                "supported": int(counts.get("supported", 0)),
                "unavailable": int(counts.get("unavailable", 0)),
                "excluded": int(counts.get("excluded", 0)),
            }
        )
    if any(
        item.get("policy_imputed", False)
        for item in pd.read_parquet(
            artifact_root / "outputs/observed_only/labels.parquet"
        ).to_dict("records")
    ):
        raise RuntimeError("observed-only labels contain policy imputations")

    required = [
        artifact_root / "inputs/certified_annual_population.parquet",
        artifact_root / "inputs/security_mapping.parquet",
        artifact_root / "inputs/calendar_policy_contract.json",
        artifact_root / "configuration/config.json",
        *sorted((artifact_root / "contracts").glob("*.json")),
        artifact_root / "calendar/xnys_regular_sessions.parquet",
        artifact_root / "calendar/xnas_regular_sessions.parquet",
        artifact_root / "calendar/calendar_evidence.json",
        response_path,
        raw_inventory,
        artifact_root / "checkpoints/symbols.json",
        artifact_root / "normalized/normalization_summary.parquet",
        normalized_inventory,
        artifact_root / "support/observed_only_row_horizon.parquet",
        artifact_root / "support/include_policy_imputed_row_horizon.parquet",
        artifact_root / "support/horizon_support.parquet",
        artifact_root / "outputs/observed_only/labels.parquet",
        artifact_root / "outputs/include_policy_imputed/labels.parquet",
    ]
    missing = [path for path in required if not path.is_file()]
    if missing:
        raise RuntimeError(f"market contract outputs are missing: {missing}")
    step2_manifest = step2_root / "manifest.json"
    collection_times = [
        item.get("retrieved_at_utc")
        for item in latest.values()
        if item.get("retrieved_at_utc")
    ]
    manifest_payload = {
        "schema_version": 1,
        "artifact_class": "CANONICAL_US_REFRESH_MARKET_LABEL_CONTRACTS",
        "created_at_utc": utc_now(),
        "refresh_id": refresh_id,
        "validated_inputs": [
            {
                "path": str(step2_manifest),
                "sha256": sha256_file(step2_manifest),
            }
        ],
        "configuration": json.loads(
            (artifact_root / "configuration/config.json").read_text()
        ),
        "explicit_contracts": {
            path.stem: {
                "path": path.relative_to(artifact_root).as_posix(),
                "sha256": sha256_file(path),
            }
            for path in sorted((artifact_root / "contracts").glob("*.json"))
        },
        "records": [
            artifact_record(
                artifact_root,
                path,
                "market_contract_input_output_or_inventory",
            )
            for path in required
        ],
        "raw_responses": {
            "latest_requests": len(latest),
            "successful": sum(
                item.get("status") == "success" for item in latest.values()
            ),
            "failed": sum(
                item.get("status") != "success" for item in latest.values()
            ),
            "all_stored_and_decompressed_hashes_verified": True,
            "collection_timestamp_min_utc": min(collection_times),
            "collection_timestamp_max_utc": max(collection_times),
        },
        "label_support": support_rows,
        "population": "observed_only",
        "inferred_delisting_returns": False,
        "promotion_status": "source_evidence_only",
        "limitations": [
            "Yahoo chart is frozen provider evidence, not an official exchange feed.",
            "The retrieval timestamp certifies this payload vintage, not a historical vendor revision vintage.",
            "No missing security, benchmark, session, or corporate-action outcome is inferred.",
        ],
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2) + "\n")
    return manifest_path


def git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
    ).strip()


def runtime_environment() -> dict[str, Any]:
    return {
        "python": sys.version,
        "pandas": pd.__version__,
        "requests": requests.__version__,
    }
