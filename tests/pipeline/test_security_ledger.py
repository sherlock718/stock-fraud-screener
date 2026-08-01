import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from pipeline.security_ledger import (
    SecSubmissionAdapter,
    build_security_ledger,
    reconcile_coverage,
)


def _gzip(path: Path, payload: bytes) -> tuple[int, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.GzipFile(
        filename="", mode="wb", fileobj=path.open("wb"), mtime=0
    ) as handle:
        handle.write(payload)
    return path.stat().st_size, hashlib.sha256(path.read_bytes()).hexdigest()


def _source_fixture(root: Path) -> None:
    documents = {
        "company_tickers": (
            "https://www.sec.gov/files/company_tickers.json",
            "raw/index/company_tickers.json.gz",
            {"0": {"cik_str": 1, "ticker": "ABC", "title": "ABC Inc."}},
            {"Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"},
        ),
        "company_tickers_exchange": (
            "https://www.sec.gov/files/company_tickers_exchange.json",
            "raw/index/company_tickers_exchange.json.gz",
            {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [[1, "ABC Inc.", "ABC", "NYSE"]],
            },
            {"Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"},
        ),
        "submission_0000000001": (
            "https://data.sec.gov/submissions/CIK0000000001.json",
            "raw/submissions/CIK0000000001.json.gz",
            {
                "cik": "0000000001",
                "name": "ABC Inc.",
                "entityType": "operating",
                "tickers": ["ABC"],
                "exchanges": ["NYSE"],
                "filings": {
                    "recent": {
                        "form": ["25-NSE", "8-K"],
                        "filingDate": ["2023-06-01", "2023-07-01"],
                        "acceptanceDateTime": [
                            "20230601120000", "20230701120000"
                        ],
                        "accessionNumber": [
                            "0000000001-23-000001",
                            "0000000001-23-000002",
                        ],
                        "primaryDocument": ["abc-25.htm", "abc-8k.htm"],
                        "items": ["", "1.03"],
                    }
                },
            },
            {},
        ),
    }
    lines = []
    for key, (url, relative, document, headers) in documents.items():
        payload = json.dumps(document, separators=(",", ":")).encode()
        stored = root / relative
        stored_size, stored_hash = _gzip(stored, payload)
        lines.append(
            {
                "logical_key": key,
                "request_url": url,
                "retrieved_at_utc": "2024-01-02T00:00:00+00:00",
                "status": "success",
                "http_status": 200,
                "response_headers": headers,
                "response_size_bytes": len(payload),
                "response_sha256": hashlib.sha256(payload).hexdigest(),
                "stored_path": relative,
                "stored_size_bytes": stored_size,
                "stored_sha256": stored_hash,
            }
        )
    manifest = root / "raw/response_manifest.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(
        "".join(json.dumps(item, sort_keys=True) + "\n" for item in lines)
    )


def test_sec_adapter_preserves_bytes_and_keeps_form_25_unresolved(tmp_path):
    source = tmp_path / "source"
    _source_fixture(source)
    required = pd.DataFrame(
        {
            "sec_cik": ["0000000001"],
            "ticker": ["ABC"],
        }
    )
    target = tmp_path / "ledger"
    normalized = SecSubmissionAdapter(source, target).normalize(required)
    assert normalized["issuers"].loc[0, "issuer_id"] == (
        "issuer:us-sec:0000000001"
    )
    event = normalized["events"].iloc[0]
    assert event["event_type"] == "delisting"
    assert event["event_status"] == "filing_indicator_unresolved"
    assert pd.isna(event["effective_at"])
    assert not event["primary_return_available"]
    bankruptcy = normalized["events"].iloc[1]
    assert bankruptcy["event_type"] == "bankruptcy"
    assert bankruptcy["event_status"] == "filing_indicator_unresolved"
    copied = target / normalized["raw_evidence"].iloc[0]["ledger_stored_path"]
    assert copied.is_file()


def test_coverage_never_upgrades_current_snapshot_to_historical_match(tmp_path):
    source = tmp_path / "source"
    _source_fixture(source)
    required_adapter = pd.DataFrame(
        {"sec_cik": ["0000000001"], "ticker": ["ABC"]}
    )
    normalized = SecSubmissionAdapter(
        source, tmp_path / "ledger"
    ).normalize(required_adapter)
    required = pd.DataFrame(
        {
            "requirement_id": ["holding:1", "holding:2"],
            "instrument_role": ["holding", "holding"],
            "stable_row_id": ["1", "2"],
            "sec_cik": ["0000000001", "0000000001"],
            "ticker": ["ABC", "ABC"],
            "required_start": pd.to_datetime(
                ["2020-01-01", "2024-02-01"], utc=True
            ),
            "required_end": pd.to_datetime(
                ["2023-01-01", "2025-01-01"], utc=True
            ),
        }
    )
    coverage = reconcile_coverage(required, normalized)
    assert coverage.loc[0, "coverage_status"] == "ambiguous"
    assert coverage.loc[1, "coverage_status"] == "unsupported"
    assert "historical_listing_effective_dates_unavailable" in (
        coverage.loc[0, "reason_codes"]
    )


def test_full_builder_requires_pinned_manifests_and_nonempty_target_fails(
    tmp_path,
):
    with pytest.raises((FileNotFoundError, RuntimeError)):
        build_security_ledger(
            tmp_path / "ledger",
            ledger_id="20260730T200000Z",
            d1_root=tmp_path / "missing-d1",
            p4_root=tmp_path / "missing-p4",
        )
    target = tmp_path / "not-empty"
    target.mkdir()
    (target / "keep").write_text("x")
    with pytest.raises(RuntimeError, match="not empty"):
        build_security_ledger(
            target,
            ledger_id="20260730T200000Z",
        )


def test_invalid_mutable_ledger_id_fails(tmp_path):
    with pytest.raises(ValueError, match="immutable UTC"):
        build_security_ledger(
            tmp_path / "ledger",
            ledger_id="latest",
        )
