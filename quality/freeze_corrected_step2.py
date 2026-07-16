"""Validate and freeze the Session 8D corrected Step 2 evidence set."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.metadata
import json
import os
import stat
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

EXPECTED_CIKS = 8021
BASELINE_COMMIT = "3f706e3e10d2b354c6e8b9407760fa2074749c0a"
SESSION8C_SHA256 = "06af0d470f33ec0e54670065a42e172c84b287bb6eddab717429fd4fac4b2816"
SESSION8B_SHA256 = "13cd7494aa7f0ff6e3f8a11efa0ee7a9a087968bb20b1fd5a9cb57f380148296"
SESSION8_SHA256 = "10b648c581ed56d6b6acb7f16d786be00409f4dfc96f0e92073693276e51b6ee"


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_gzip_payload(path: Path, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with gzip.open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def record(root: Path, path: Path, role: str) -> dict:
    return {
        "path": path.relative_to(root).as_posix(), "role": role,
        "size_bytes": path.stat().st_size, "sha256": sha256_file(path),
        "mode": stat.filemode(path.stat().st_mode),
    }


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", *args], cwd=root)


def _latest_response_records(path: Path) -> tuple[dict[str, dict], int]:
    latest = {}
    history = 0
    for line in path.read_text().splitlines():
        item = json.loads(line)
        if item["cik"] in latest:
            if latest[item["cik"]]["status"] == "success":
                raise RuntimeError(f"response record follows success for {item['cik']}")
            history += 1
        latest[item["cik"]] = item
    return latest, history


def _dependencies() -> dict:
    result = {}
    for package in ("pandas", "pyarrow", "requests"):
        try:
            result[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            result[package] = None
    return result


def validate_and_freeze(root: Path, artifact_root: Path) -> Path:
    manifest_path = artifact_root / "manifest.json"
    summary_path = artifact_root / "validation_summary.json"
    diagnostic_path = artifact_root / "diagnostics/stale_snapshot_comparison.json"
    response_path = artifact_root / "raw/response_manifest.jsonl"
    checkpoint_path = artifact_root / "checkpoints/checkpoint.json"
    frozen_tickers = artifact_root / "inputs/tickers.parquet"
    certified_path = artifact_root / "outputs/certified_snapshots.parquet"
    excluded_path = artifact_root / "outputs/excluded_periods.parquet"
    unavailable_path = artifact_root / "outputs/unavailable_entities.parquet"
    required_paths = [response_path, checkpoint_path, frozen_tickers, certified_path, excluded_path, unavailable_path]
    missing = [path for path in required_paths if not path.is_file()]
    if missing:
        raise RuntimeError(f"missing required artifacts: {missing}")

    prior = {
        "session8c_validation_manifest": root / "artifacts/pit_validation/training_label_market_inputs/session8c_validation_manifest.json",
        "session8b_calendar_contract": root / "artifacts/pit_validation/calendar_contract/session8b_calendar_contract.json",
        "session8_corrected_partial_manifest": root / "artifacts/pit_validation/corrected_partial/manifest.json",
    }
    expected = [SESSION8C_SHA256, SESSION8B_SHA256, SESSION8_SHA256]
    for path, digest in zip(prior.values(), expected):
        if sha256_file(path) != digest:
            raise RuntimeError(f"prior manifest hash mismatch: {path}")

    source_tickers = root / "data/tickers.parquet"
    if sha256_file(source_tickers) != sha256_file(frozen_tickers):
        raise RuntimeError("frozen universe does not match data/tickers.parquet")
    universe = pd.read_parquet(frozen_tickers)
    if len(universe) != EXPECTED_CIKS or universe["cik"].nunique() != EXPECTED_CIKS:
        raise RuntimeError("frozen universe is not 8,021 unique CIKs")

    checkpoint = json.loads(checkpoint_path.read_text())
    completed = set(checkpoint["completed_ciks"])
    expected_ciks = set(universe["cik"].astype(str))
    if completed != expected_ciks:
        raise RuntimeError(f"checkpoint incomplete: {len(completed)}/{len(expected_ciks)}")

    responses, retry_records = _latest_response_records(response_path)
    if set(responses) != expected_ciks:
        raise RuntimeError(f"response coverage mismatch: {len(responses)}/{len(expected_ciks)}")
    raw_dir = artifact_root / "raw/companyfacts"
    raw_hash_failures = []
    for cik, item in responses.items():
        if item["status"] not in {"success", "invalid_payload"}:
            continue
        path = raw_dir / item["stored_name"]
        if not path.is_file() or path.stat().st_size != item["stored_size_bytes"]:
            raw_hash_failures.append(f"stored_size:{cik}")
            continue
        if sha256_file(path) != item["stored_sha256"]:
            raw_hash_failures.append(f"stored_sha256:{cik}")
            continue
        payload_hash, payload_size = sha256_gzip_payload(path)
        if payload_hash != item["response_sha256"] or payload_size != item["response_size_bytes"]:
            raw_hash_failures.append(f"response_payload:{cik}")
    if raw_hash_failures:
        raise RuntimeError(f"raw response validation failures: {raw_hash_failures[:10]}")

    certified = pd.read_parquet(certified_path)
    excluded = pd.read_parquet(excluded_path)
    unavailable = pd.read_parquet(unavailable_path)
    required_columns = {
        "entity_id", "fiscal_year", "period_type", "market", "source_filing_date",
        "availability_timestamp", "availability_provenance", "cik",
    }
    if not required_columns.issubset(certified.columns):
        raise RuntimeError(f"certified schema missing {sorted(required_columns - set(certified.columns))}")
    if certified[list(required_columns)].isna().any().any():
        raise RuntimeError("certified provenance columns contain nulls")
    if set(certified["market"].unique()) != {"US"}:
        raise RuntimeError("certified output contains non-US rows")
    if set(certified["availability_provenance"].unique()) != {"sec_primary_filing"}:
        raise RuntimeError("certified output contains unsupported provenance")
    if not certified["entity_id"].eq("US:" + certified["cik"].astype(str)).all():
        raise RuntimeError("unstable entity_id construction")
    key = ["entity_id", "fiscal_year", "period_type", "fiscal_quarter"]
    if certified.duplicated(key).any():
        raise RuntimeError("duplicate certified entity-period keys")
    parsed_availability = pd.to_datetime(certified["availability_timestamp"], utc=True, errors="coerce")
    if parsed_availability.isna().any():
        raise RuntimeError("unparseable certified availability timestamps")

    response_status = Counter(item["status"] for item in responses.values())
    period_type = Counter(certified["period_type"])
    excluded_period_type = Counter(excluded["period_type"])
    exclusion_reasons = Counter(excluded["exclusion_reason"].fillna("missing_reason"))
    unavailable_reasons = Counter(unavailable["reason"].fillna("missing_reason"))
    proven_entities = set(certified["entity_id"])
    excluded_entities = set(excluded["entity_id"])
    unavailable_entities = set(unavailable["entity_id"])
    if proven_entities & unavailable_entities or excluded_entities & unavailable_entities:
        raise RuntimeError("unavailable entities overlap row-bearing entities")
    issuer_classification = {
        "proven_entities": len(proven_entities),
        "excluded_only_entities": len(excluded_entities - proven_entities),
        "unavailable_entities": len(unavailable_entities),
    }
    if sum(issuer_classification.values()) != EXPECTED_CIKS:
        raise RuntimeError("issuer classification does not partition the frozen universe")
    summary = {
        "schema_version": 1,
        "classification": {
            "proven_period_rows": len(certified),
            "excluded_period_rows": len(excluded),
            "unavailable_entities": len(unavailable),
            "classification_note": "Proven/excluded count period rows; unavailable counts frozen-universe entities with no usable Company Facts payload or no supported period candidates.",
        },
        "proven_period_type_counts": dict(sorted(period_type.items())),
        "excluded_period_type_counts": dict(sorted(excluded_period_type.items())),
        "issuer_classification": issuer_classification,
        "excluded_reason_counts": dict(sorted(exclusion_reasons.items())),
        "unavailable_reason_counts": dict(sorted(unavailable_reasons.items())),
        "source_coverage": {
            "frozen_ciks": len(expected_ciks), "latest_response_records": len(responses),
            "response_status_counts": dict(sorted(response_status.items())),
            "retry_history_records": retry_records, "raw_hash_failures": 0,
        },
        "schema": {"rows": len(certified), "columns": len(certified.columns), "duplicate_entity_period_keys": 0},
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

    stale = pd.read_parquet(
        root / "data/snapshots.parquet",
        columns=["cik", "market", "fiscal_year", "period_type", "fiscal_quarter"],
    )
    stale = stale[stale["market"].eq("US")]
    stale_keys = set(zip(
        stale["cik"].astype(str), stale["fiscal_year"], stale["period_type"],
        stale["fiscal_quarter"].fillna(""),
    ))
    corrected_keys = set(zip(
        certified["cik"].astype(str), certified["fiscal_year"], certified["period_type"],
        certified["fiscal_quarter"].fillna(""),
    ))
    diagnostic = {
        "classification": "diagnostic_only_not_correctness_evidence",
        "stale_us_rows": len(stale), "stale_us_entities": stale["cik"].nunique(),
        "corrected_proven_rows": len(certified), "corrected_proven_entities": certified["cik"].nunique(),
        "common_entity_period_keys": len(stale_keys & corrected_keys),
        "corrected_only_entity_period_keys": len(corrected_keys - stale_keys),
        "stale_only_entity_period_keys": len(stale_keys - corrected_keys),
    }
    diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostic_path.write_text(json.dumps(diagnostic, indent=2, sort_keys=True) + "\n")

    orphan_path = artifact_root / "raw/orphan_manifest.jsonl"
    orphan_records = []
    if orphan_path.exists():
        for line in orphan_path.read_text().splitlines():
            item = json.loads(line)
            path = artifact_root / "raw/orphans" / item["stored_name"]
            if path.stat().st_size != item["stored_size_bytes"] or sha256_file(path) != item["stored_sha256"]:
                raise RuntimeError("orphan/partial payload hash mismatch")
            orphan_records.append(item)

    lineage_dir = artifact_root / "lineage"
    lineage_dir.mkdir(parents=True, exist_ok=True)
    status_path = lineage_dir / "git_status_porcelain.txt"
    patch_path = lineage_dir / "tracked_dirty.patch"
    inventory_path = lineage_dir / "untracked_inventory.json"
    status_path.write_bytes(git(root, "status", "--porcelain=v1", "--untracked-files=all"))
    patch_path.write_bytes(git(root, "diff", "--binary", BASELINE_COMMIT, "--", "."))
    inventory = []
    for raw in git(root, "ls-files", "--others", "--exclude-standard", "-z").split(b"\0"):
        if not raw:
            continue
        path = root / os.fsdecode(raw)
        if path.is_file() and path not in {manifest_path, inventory_path}:
            inventory.append(record(root, path, "untracked_dirty_state"))
    inventory_path.write_text(json.dumps(inventory, indent=2) + "\n")

    code_paths = [
        root / "pipeline/step2_build_snapshots.py",
        root / "pipeline/step2_artifact_rebuild.py",
        root / "quality/freeze_corrected_step2.py",
        root / "tests/pipeline/test_step2_build_snapshots.py",
        root / "tests/pipeline/test_step2_artifact_rebuild.py",
    ]
    manifest = {
        "schema_version": 1, "artifact_class": "CORRECTED_STEP2_US_PROVENANCE",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_commit": BASELINE_COMMIT, "current_head": git(root, "rev-parse", "HEAD").decode().strip(),
        "claim": {"step2_only": True, "official_sec_company_facts_only": True, "session9_allowed": False},
        "configuration": {
            "frozen_ciks": EXPECTED_CIKS, "source": "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
            "primary_forms": ["10-K", "10-Q", "20-F", "10-KSB", "10-QSB"],
            "date_only_policy": "America/New_York end-of-calendar-day converted to UTC",
            "later_amendments": "excluded", "equal_time_distinct_accessions": "fail_closed",
            "output_root": artifact_root.relative_to(root).as_posix(),
        },
        "validated_prior_chain": [record(root, path, role) for role, path in prior.items()],
        "inputs": [record(root, frozen_tickers, "frozen_8021_cik_us_universe")],
        "evidence": [
            record(root, response_path, "per_request_raw_response_manifest"),
            record(root, checkpoint_path, "completed_cik_checkpoint"),
            record(root, summary_path, "validation_summary"),
            record(root, diagnostic_path, "stale_snapshot_diagnostic_only"),
            *([record(root, orphan_path, "interrupted_partial_response_manifest")] if orphan_path.exists() else []),
        ],
        "outputs": [
            record(root, certified_path, "certified_provenance_preserving_step2"),
            record(root, excluded_path, "excluded_period_diagnostics"),
            record(root, unavailable_path, "unavailable_entity_diagnostics"),
        ],
        "raw_responses": {
            "directory": raw_dir.relative_to(root).as_posix(),
            "manifested_payloads": sum(item["status"] in {"success", "invalid_payload"} for item in responses.values()),
            "every_stored_and_uncompressed_hash_verified": True,
            "interrupted_partial_payloads_isolated_and_verified": len(orphan_records),
        },
        "validation": summary,
        "code_lineage": [record(root, path, "session8d_code_or_test") for path in code_paths],
        "environment": {"python": sys.version, "dependencies": _dependencies()},
        "dirty_state": {
            "complete_status_recorded": True, "baseline": BASELINE_COMMIT,
            "status": record(root, status_path, "dirty_status"),
            "tracked_patch": record(root, patch_path, "tracked_dirty_patch"),
            "untracked_inventory": record(root, inventory_path, "untracked_path_size_hash_inventory"),
            "manifest_self_excluded": manifest_path.relative_to(root).as_posix(),
        },
        "limitations": [
            "Step 2 provenance only; no label-market-input support is claimed.",
            "Unavailable and excluded records are not substituted or inferred.",
            "No Step 3+, labels, market prices, models, predictions, backtests, or optimization were run.",
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--artifact-root", type=Path, required=True)
    args = parser.parse_args()
    path = validate_and_freeze(args.root.resolve(), args.artifact_root.resolve())
    print(f"{sha256_file(path)}  {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
