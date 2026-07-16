import gzip
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from pipeline.step2_artifact_rebuild import (
    availability_timestamp,
    freeze_universe,
    load_response_records,
    quarantine_unmanifested_raw,
)
from pipeline.step2_build_snapshots import build_period_snapshots


def _facts(revenue_entries, asset_entries):
    return {"facts": {"us-gaap": {
        "Revenues": {"units": {"USD": revenue_entries}},
        "Assets": {"units": {"USD": asset_entries}},
    }}}


def test_stable_entity_universe_and_artifact_scope(tmp_path, monkeypatch):
    monkeypatch.setattr("pipeline.step2_artifact_rebuild.EXPECTED_US_CIKS", 2)
    source = tmp_path / "source.parquet"
    destination = tmp_path / "artifact/inputs/tickers.parquet"
    pd.DataFrame({"cik": ["0000000001", "0000000002"], "market": ["US", "US"]}).to_parquet(source)
    record = freeze_universe(source, destination)
    assert record["unique_ciks"] == 2
    assert destination.is_file()
    assert not (tmp_path / "snapshots.parquet").exists()


def test_primary_beats_later_amendment_and_date_only_is_end_of_day():
    facts = _facts(
        [{"fy": 2020, "fp": "FY", "val": 100, "filed": "2021-03-01", "form": "10-K"},
         {"fy": 2020, "fp": "FY", "val": 110, "filed": "2021-05-01", "form": "10-K/A"}],
        [{"fy": 2020, "fp": "FY", "val": 500, "filed": "2021-03-01", "form": "10-K"}],
    )
    proven, excluded = build_period_snapshots(facts, return_excluded=True)
    assert not excluded and proven[0]["revenue"] == 100
    assert availability_timestamp("2021-03-01") == "2021-03-02T04:59:59.999999+00:00"


def test_equal_time_primary_collision_fails_closed():
    facts = _facts(
        [{"fy": 2020, "fp": "FY", "val": 100, "filed": "2021-03-01", "form": "10-K"},
         {"fy": 2020, "fp": "FY", "val": 101, "filed": "2021-03-01", "form": "10-K", "accn": "b"}],
        [{"fy": 2020, "fp": "FY", "val": 500, "filed": "2021-03-01", "form": "10-K", "accn": "a"}],
    )
    proven, excluded = build_period_snapshots(facts, return_excluded=True)
    assert not proven
    assert "ambiguous_earliest_primary" in excluded[0]["exclusion_reason"]


def test_equal_time_distinct_accessions_fail_closed_even_when_values_match():
    facts = _facts(
        [{"fy": 2020, "fp": "FY", "val": 100, "filed": "2021-03-01", "form": "10-K", "accn": "a"},
         {"fy": 2020, "fp": "FY", "val": 100, "filed": "2021-03-01", "form": "10-K", "accn": "b"}],
        [{"fy": 2020, "fp": "FY", "val": 500, "filed": "2021-03-01", "form": "10-K", "accn": "a"}],
    )
    proven, excluded = build_period_snapshots(facts, return_excluded=True)
    assert not proven
    assert "ambiguous_earliest_primary" in excluded[0]["exclusion_reason"]


def test_amendment_only_is_excluded():
    entries = [{"fy": 2020, "fp": "FY", "val": 100, "filed": "2021-05-01", "form": "10-K/A"}]
    proven, excluded = build_period_snapshots(_facts(entries, [{**entries[0], "val": 500}]), return_excluded=True)
    assert not proven
    assert "missing_primary_revenue_or_assets" in excluded[0]["exclusion_reason"]


def test_raw_response_hash_and_resume_validation(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    payload = b'{"facts": {}}'
    stored = raw_dir / "CIK0000000001.json.gz"
    with stored.open("wb") as output:
        with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as handle:
            handle.write(payload)
    manifest = tmp_path / "responses.jsonl"
    record = {
        "cik": "0000000001", "status": "success", "stored_name": stored.name,
        "stored_size_bytes": stored.stat().st_size,
        "stored_sha256": hashlib.sha256(stored.read_bytes()).hexdigest(),
    }
    manifest.write_text(json.dumps(record) + "\n")
    assert load_response_records(manifest, raw_dir)["0000000001"]["status"] == "success"
    stored.write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="wrong-sized|hash mismatch"):
        load_response_records(manifest, raw_dir)


def test_failure_record_is_resumable_without_raw_payload(tmp_path):
    manifest = tmp_path / "responses.jsonl"
    manifest.write_text(json.dumps({
        "cik": "0000000001", "status": "failure", "failure_reason": "company_facts_unavailable"
    }) + "\n")
    assert load_response_records(manifest, tmp_path / "missing")["0000000001"]["status"] == "failure"


def test_retry_record_may_follow_transport_failure(tmp_path):
    manifest = tmp_path / "responses.jsonl"
    manifest.write_text("\n".join([
        json.dumps({"cik": "0000000001", "status": "failure", "failure_reason": "transport_error"}),
        json.dumps({"cik": "0000000001", "status": "failure", "failure_reason": "company_facts_not_found"}),
    ]) + "\n")
    record = load_response_records(manifest, tmp_path / "raw")["0000000001"]
    assert record["failure_reason"] == "company_facts_not_found"


def test_interrupted_unmanifested_response_is_preserved_as_orphan(tmp_path):
    raw_dir = tmp_path / "raw/companyfacts"
    raw_dir.mkdir(parents=True)
    partial = raw_dir / "CIK0000000001.json.gz"
    partial.write_bytes(b"partial")
    orphan_manifest = tmp_path / "raw/orphan_manifest.jsonl"
    quarantine_unmanifested_raw(raw_dir, {}, orphan_manifest)
    assert not partial.exists()
    item = json.loads(orphan_manifest.read_text())
    assert item["status"] == "interrupted_partial_response"
    assert (tmp_path / "raw/orphans" / item["stored_name"]).read_bytes() == b"partial"
