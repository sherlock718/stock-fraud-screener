import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from pipeline.step2_artifact_rebuild import freeze_universe
from pipeline.step2_artifact_rebuild import fetch_response
from pipeline.us_refresh_sources import (
    _verify_failure_attempts,
    collect_us_universe,
    initialize_market_candidate,
    sha256_file,
    validate_refresh_id,
)


class _Response:
    def __init__(self, url: str, payload: dict, status_code: int = 200):
        self.url = url
        self.content = json.dumps(payload, separators=(",", ":")).encode()
        self.status_code = status_code
        self.headers = {"Content-Type": "application/json"}


class _Session:
    def __init__(self, documents: dict[str, dict]):
        self.documents = documents
        self.urls = []

    def get(self, url, **_kwargs):
        self.urls.append(url)
        return _Response(url, self.documents[url])


def test_refresh_id_is_immutable_utc_identifier():
    assert validate_refresh_id("20260730T143000Z") == "20260730T143000Z"
    with pytest.raises(ValueError, match="immutable UTC"):
        validate_refresh_id("latest")


def test_universe_collection_preserves_exact_bytes_and_versions_output(tmp_path):
    tickers_url = "https://www.sec.gov/files/company_tickers.json"
    exchange_url = (
        "https://www.sec.gov/files/company_tickers_exchange.json"
    )
    submission_url = (
        "https://data.sec.gov/submissions/CIK0000000001.json"
    )
    documents = {
        tickers_url: {
            "0": {"cik_str": 1, "ticker": "abc", "title": "ABC Inc."}
        },
        exchange_url: {
            "data": [[1, "ABC Inc.", "ABC", "NYSE"]]
        },
        submission_url: {
            "sic": "3571",
            "sicDescription": "Electronic Computers",
        },
    }
    root = tmp_path / "versioned/universe"
    manifest = collect_us_universe(
        root,
        refresh_id="20260730T143000Z",
        session=_Session(documents),
        rate_delay=0,
    )
    universe = pd.read_parquet(root / "outputs/universe.parquet")
    assert universe.loc[0, "cik"] == "0000000001"
    assert universe.loc[0, "sic_code"] == "3571"
    assert universe.loc[0, "submission_status"] == "supported"
    responses = [
        json.loads(line)
        for line in (root / "raw/response_manifest.jsonl")
        .read_text()
        .splitlines()
    ]
    assert len(responses) == 3
    assert all(item["retrieved_at_utc"] for item in responses)
    index_record = next(
        item for item in responses if item["logical_key"] == "company_tickers"
    )
    with gzip.open(root / index_record["stored_path"], "rb") as handle:
        assert handle.read() == _Response(
            tickers_url,
            documents[tickers_url],
        ).content
    assert json.loads(manifest.read_text())["refresh_id"] == (
        "20260730T143000Z"
    )
    assert not (tmp_path / "data/tickers.parquet").exists()
    with pytest.raises(RuntimeError, match="already complete"):
        collect_us_universe(
            root,
            refresh_id="20260730T143000Z",
            session=_Session(documents),
            rate_delay=0,
        )


def test_dynamic_step2_universe_is_still_exact_and_us_only(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("pipeline.step2_artifact_rebuild.EXPECTED_US_CIKS", 2)
    source = tmp_path / "universe.parquet"
    pd.DataFrame(
        {
            "cik": ["0000000001", "0000000002"],
            "market": ["US", "US"],
        }
    ).to_parquet(source)
    record = freeze_universe(
        source,
        tmp_path / "artifact/inputs/tickers.parquet",
        expected_ciks=2,
    )
    assert record["rows"] == 2


def test_step2_preserves_non_success_response_bytes(tmp_path, monkeypatch):
    class Response:
        status_code = 404
        content = b'{"error":"not found"}'

    monkeypatch.setattr(
        "pipeline.step2_artifact_rebuild.requests.get",
        lambda *_args, **_kwargs: Response(),
    )
    monkeypatch.setattr(
        "pipeline.step2_artifact_rebuild.edgar_rate_wait",
        lambda: None,
    )
    record, facts = fetch_response(
        "0000000001",
        tmp_path / "raw/companyfacts",
        retries=1,
    )
    assert facts is None
    assert record["failure_reason"] == "company_facts_not_found"
    attempt = record["attempts"][0]
    with gzip.open(tmp_path / attempt["stored_failure_path"], "rb") as handle:
        assert handle.read() == Response.content
    assert attempt["response_size_bytes"] == len(Response.content)


def test_market_initialization_writes_explicit_contracts(tmp_path):
    step2 = tmp_path / "filings"
    (step2 / "outputs").mkdir(parents=True)
    (step2 / "manifest.json").write_text(
        json.dumps(
            {
                "artifact_class": "CANONICAL_US_REFRESH_CORRECTED_STEP2",
                "refresh_id": "20260730T143000Z",
                "records": [],
            }
        )
    )
    pd.DataFrame(
        {
            "entity_id": ["US:0000000001"],
            "cik": ["0000000001"],
            "ticker": ["ABC"],
            "fiscal_year": [2020],
            "fiscal_quarter": [pd.NA],
            "period_type": ["annual"],
            "filed_date": ["2021-03-01"],
            "availability_timestamp": [
                "2021-03-02T04:59:59.999999+00:00"
            ],
            "availability_provenance": ["sec_primary_filing"],
            "shares_outstanding": [100.0],
            "market": ["US"],
        }
    ).to_parquet(step2 / "outputs/certified_snapshots.parquet")
    calendar = tmp_path / "calendar.json"
    calendar.write_text('{"contract":"fixture"}\n')
    market = tmp_path / "market"
    initialize_market_candidate(
        step2_root=step2,
        artifact_root=market,
        refresh_id="20260730T143000Z",
        end_date_exclusive="2026-07-31",
        calendar_contract_path=calendar,
    )
    contracts = {
        path.name: json.loads(path.read_text())
        for path in (market / "contracts").glob("*.json")
    }
    assert set(contracts) == {
        "price_contract.json",
        "benchmark_contract.json",
        "calendar_contract.json",
        "decision_contract.json",
        "label_support_contract.json",
    }
    assert (
        contracts["label_support_contract.json"][
            "inferred_delisting_returns"
        ]
        is False
    )
    assert (
        contracts["price_contract.json"]["raw_response_policy"]
        == "exact bytes stored before parsing"
    )


def test_failure_verifier_accepts_existing_yahoo_attempt_byte_key(tmp_path):
    artifact_root = tmp_path / "market"
    failure = artifact_root / "raw/failures/ABC.attempt1.bin.gz"
    failure.parent.mkdir(parents=True)
    payload = b'{"chart":{"error":{"code":"Not Found"}}}'
    with gzip.open(failure, "wb") as handle:
        handle.write(payload)
    records = {
        "ABC": {
            "attempts": [
                {
                    "bytes": len(payload),
                    "response_sha256": hashlib.sha256(payload).hexdigest(),
                    "stored_path": failure.relative_to(
                        artifact_root
                    ).as_posix(),
                    "stored_size_bytes": failure.stat().st_size,
                    "stored_sha256": sha256_file(failure),
                }
            ]
        }
    }

    _verify_failure_attempts(artifact_root, records)
