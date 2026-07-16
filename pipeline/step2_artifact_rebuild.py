"""Artifact-scoped, provenance-preserving US SEC Company Facts rebuild."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import time
from datetime import datetime, time as datetime_time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from pipeline.step2_build_snapshots import (
    HEADERS,
    add_yoy_features,
    build_period_snapshots,
    edgar_rate_wait,
)

EXPECTED_US_CIKS = 8021
EXTRACTION_SCHEMA_VERSION = 2
CHECKPOINT_EVERY = 250
SEC_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
SOURCE_TZ = ZoneInfo("America/New_York")


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def availability_timestamp(filed_date: str) -> str:
    """Treat an SEC date-only filing event as end-of-day New York time."""
    day = datetime.strptime(filed_date, "%Y-%m-%d").date()
    local = datetime.combine(day, datetime_time.max, tzinfo=SOURCE_TZ)
    return local.astimezone(timezone.utc).isoformat()


def freeze_universe(source: Path, destination: Path) -> dict:
    frame = pd.read_parquet(source)
    required = {"cik", "market"}
    if not required.issubset(frame.columns):
        raise RuntimeError(f"ticker universe missing columns: {sorted(required - set(frame.columns))}")
    ciks = frame["cik"].astype(str)
    if len(frame) != EXPECTED_US_CIKS or ciks.nunique() != EXPECTED_US_CIKS:
        raise RuntimeError("ticker universe is not the expected 8,021 unique-CIK population")
    if frame["cik"].isna().any() or not ciks.str.fullmatch(r"\d{10}").all():
        raise RuntimeError("ticker universe contains missing or non-10-digit CIKs")
    if set(frame["market"].dropna().unique()) != {"US"}:
        raise RuntimeError("ticker universe is not exclusively US")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if sha256_file(destination) != sha256_file(source):
            raise RuntimeError("frozen ticker universe already exists with a different hash")
    else:
        shutil.copyfile(source, destination)
    return {
        "path": destination.as_posix(),
        "size_bytes": destination.stat().st_size,
        "sha256": sha256_file(destination),
        "rows": len(frame),
        "unique_ciks": ciks.nunique(),
    }


def _append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def load_response_records(path: Path, raw_dir: Path) -> dict[str, dict]:
    records: dict[str, dict] = {}
    if not path.exists():
        return records
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        record = json.loads(line)
        cik = record["cik"]
        if cik in records and records[cik]["status"] == "success":
            raise RuntimeError(f"record follows successful response for CIK {cik} at line {line_number}")
        if record.get("stored_name"):
            raw = raw_dir / record["stored_name"]
            if not raw.is_file() or raw.stat().st_size != record["stored_size_bytes"]:
                raise RuntimeError(f"missing or wrong-sized manifested response for {cik}")
            if sha256_file(raw) != record["stored_sha256"]:
                raise RuntimeError(f"stored response hash mismatch for {cik}")
        records[cik] = record
    return records


def quarantine_unmanifested_raw(raw_dir: Path, records: dict[str, dict], orphan_manifest: Path) -> None:
    if not raw_dir.exists():
        return
    manifested = {item.get("stored_name") for item in records.values() if item.get("stored_name")}
    orphan_dir = raw_dir.parent / "orphans"
    for path in sorted(raw_dir.glob("CIK*.json.gz")):
        if path.name in manifested:
            continue
        orphan_dir.mkdir(parents=True, exist_ok=True)
        digest = sha256_file(path)
        destination = orphan_dir / f"{path.stem}.partial.{digest[:16]}.gz"
        path.replace(destination)
        _append_jsonl(orphan_manifest, {
            "cik": path.name[3:13], "status": "interrupted_partial_response",
            "stored_name": destination.name, "stored_size_bytes": destination.stat().st_size,
            "stored_sha256": digest, "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        })


def fetch_response(cik: str, raw_dir: Path, retries: int = 4) -> tuple[dict, dict | None]:
    url = SEC_URL.format(cik=cik)
    attempts = []
    for attempt in range(1, retries + 1):
        edgar_rate_wait()
        retrieved = datetime.now(timezone.utc).isoformat()
        try:
            response = requests.get(url, headers=HEADERS, timeout=30, stream=True)
            attempts.append({"attempt": attempt, "retrieved_at_utc": retrieved, "http_status": response.status_code})
            if response.status_code == 200:
                raw_dir.mkdir(parents=True, exist_ok=True)
                stored_name = f"CIK{cik}.json.gz"
                stored = raw_dir / stored_name
                digest = hashlib.sha256()
                byte_size = 0
                with stored.open("xb") as output:
                    with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                digest.update(chunk)
                                byte_size += len(chunk)
                                compressed.write(chunk)
                record = {
                    "cik": cik, "request_url": url, "retrieved_at_utc": retrieved,
                    "http_status": 200, "status": "success", "response_size_bytes": byte_size,
                    "response_sha256": digest.hexdigest(), "stored_name": stored_name,
                    "stored_size_bytes": stored.stat().st_size, "stored_sha256": sha256_file(stored),
                    "attempts": attempts,
                }
                try:
                    with gzip.open(stored, "rt", encoding="utf-8") as handle:
                        return record, json.load(handle)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    record["status"] = "invalid_payload"
                    record["failure_reason"] = "invalid_company_facts_json"
                    return record, None
            if response.status_code == 404:
                break
            if response.status_code == 429:
                time.sleep(10 * attempt)
        except Exception as exc:
            attempts.append({"attempt": attempt, "retrieved_at_utc": retrieved, "error": type(exc).__name__})
            if attempt < retries:
                time.sleep(2 ** (attempt - 1))
    statuses = [item.get("http_status") for item in attempts]
    if statuses and statuses[-1] == 404:
        failure_reason = "company_facts_not_found"
    elif any(status is not None for status in statuses):
        failure_reason = "company_facts_http_error"
    else:
        failure_reason = "transport_error"
    return {
        "cik": cik, "request_url": url, "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "http_status": attempts[-1].get("http_status") if attempts else None,
        "status": "failure", "failure_reason": failure_reason, "attempts": attempts,
    }, None


def _atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def _atomic_json(payload: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def run(
    tickers: Path,
    artifact_root: Path,
    *,
    limit: int | None = None,
    retry_transient: bool = False,
) -> dict:
    frozen = artifact_root / "inputs/tickers.parquet"
    universe_record = freeze_universe(tickers, frozen)
    companies = pd.read_parquet(frozen).to_dict("records")
    if limit is not None:
        companies = companies[:limit]

    raw_dir = artifact_root / "raw/companyfacts"
    response_manifest = artifact_root / "raw/response_manifest.jsonl"
    checkpoint_path = artifact_root / "checkpoints/checkpoint.json"
    certified_path = artifact_root / "outputs/certified_snapshots.parquet"
    excluded_path = artifact_root / "outputs/excluded_periods.parquet"
    unavailable_path = artifact_root / "outputs/unavailable_entities.parquet"
    records = load_response_records(response_manifest, raw_dir)
    quarantine_unmanifested_raw(raw_dir, records, artifact_root / "raw/orphan_manifest.jsonl")

    completed = set()
    certified_rows, excluded_rows, unavailable_rows = [], [], []
    if checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text())
        if checkpoint.get("extraction_schema_version") == EXTRACTION_SCHEMA_VERSION:
            completed = set(checkpoint["completed_ciks"])
            for path, target in ((certified_path, certified_rows), (excluded_path, excluded_rows), (unavailable_path, unavailable_rows)):
                if path.exists():
                    target.extend(pd.read_parquet(path).to_dict("records"))
    if retry_transient:
        retry_ciks = {
            cik for cik, item in records.items()
            if item.get("failure_reason") in {"transport_error", "company_facts_http_error"}
        }
        completed -= retry_ciks
        unavailable_rows = [row for row in unavailable_rows if str(row["cik"]) not in retry_ciks]

    def save() -> None:
        _atomic_parquet(pd.DataFrame(certified_rows), certified_path)
        _atomic_parquet(pd.DataFrame(excluded_rows), excluded_path)
        _atomic_parquet(pd.DataFrame(unavailable_rows), unavailable_path)
        outputs = {}
        for name, path in (("certified", certified_path), ("excluded", excluded_path), ("unavailable", unavailable_path)):
            outputs[name] = {"path": path.as_posix(), "size_bytes": path.stat().st_size, "sha256": sha256_file(path)}
        _atomic_json({
            "schema_version": 1, "extraction_schema_version": EXTRACTION_SCHEMA_VERSION,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "universe": universe_record, "completed_ciks": sorted(completed), "outputs": outputs,
        }, checkpoint_path)

    for index, company in enumerate(companies):
        cik = str(company["cik"])
        if cik in completed:
            continue
        record = records.get(cik)
        facts = None
        if record and record["status"] == "success":
            with gzip.open(raw_dir / record["stored_name"], "rt", encoding="utf-8") as handle:
                facts = json.load(handle)
        elif record and (
            record.get("failure_reason") in {"transport_error", "company_facts_http_error"}
            or all("error" in attempt for attempt in record.get("attempts", []))
        ):
            record, facts = fetch_response(cik, raw_dir)
            _append_jsonl(response_manifest, record)
            records[cik] = record
            if facts is None:
                unavailable_rows.append({"cik": cik, "entity_id": f"US:{cik}", "reason": record["failure_reason"]})
        elif record:
            unavailable_rows.append({"cik": cik, "entity_id": f"US:{cik}", "reason": record["failure_reason"]})
        else:
            record, facts = fetch_response(cik, raw_dir)
            _append_jsonl(response_manifest, record)
            records[cik] = record
            if facts is None:
                unavailable_rows.append({"cik": cik, "entity_id": f"US:{cik}", "reason": record["failure_reason"]})

        if facts is not None:
            proven, excluded = build_period_snapshots(facts, return_excluded=True)
            if not proven and not excluded:
                unavailable_rows.append({"cik": cik, "entity_id": f"US:{cik}", "reason": "no_supported_period_candidates"})
            for rows, is_proven in ((proven, True), (excluded, False)):
                rows = add_yoy_features(rows)
                for row in rows:
                    row.update({
                        "cik": cik, "ticker": company.get("ticker", ""), "name": company.get("name", ""),
                        "exchange": company.get("exchange"), "sic_code": company.get("sic_code"),
                        "sic_description": company.get("sic_description", ""), "market": "US",
                        "entity_id": f"US:{cik}", "country": company.get("country", "United States"),
                        "accounting_std": company.get("accounting_std", "GAAP"),
                    })
                    if is_proven:
                        row["source_filing_date"] = row["filed_date"]
                        row["availability_timestamp"] = availability_timestamp(row["filed_date"])
                        row["availability_provenance"] = "sec_primary_filing"
                        certified_rows.append(row)
                    else:
                        excluded_rows.append(row)
        completed.add(cik)
        if (index + 1) % CHECKPOINT_EVERY == 0:
            save()
            print(f"{index + 1}/{len(companies)} ciks; proven={len(certified_rows)} excluded={len(excluded_rows)} unavailable={len(unavailable_rows)}", flush=True)
    save()
    return {"proven": len(certified_rows), "excluded": len(excluded_rows), "unavailable": len(unavailable_rows)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--retry-transient", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(
        args.tickers.resolve(), args.artifact_root.resolve(), limit=args.limit,
        retry_transient=args.retry_transient,
    ), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
