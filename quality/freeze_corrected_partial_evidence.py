"""Freeze Session 8 CORRECTED_PARTIAL lineage with streaming SHA-256 hashes."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


BASELINE_COMMIT = "3f706e3e10d2b354c6e8b9407760fa2074749c0a"
PREFLIGHT_SHA256 = "31c4f4e289ac49ef7c01f333370b4e3a35a3e786c0b289200bdc95e9c5c2f4d5"
REFERENCE_SHA256 = "ead68437e62752d38b8fa73c145ee360e24f39c254b03b971b49f6743b62c595"


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def record(root: Path, path: Path, *, role: str) -> dict:
    return {
        "path": path.relative_to(root).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "mode": stat.filemode(path.stat().st_mode),
    }


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", *args], cwd=root)


def dependency_versions() -> dict[str, str | None]:
    result = {}
    for package in ("numpy", "pandas", "pyarrow", "yfinance", "scikit-learn", "lightgbm"):
        try:
            result[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            result[package] = None
    return result


def build_manifest(root: Path, artifact_dir: Path) -> Path:
    manifest_path = artifact_dir / "manifest.json"
    lineage_dir = artifact_dir / "lineage"
    lineage_dir.mkdir(parents=True, exist_ok=True)

    preflight = root / "artifacts/pit_validation/corrected_partial_inputs/manifest.json"
    if sha256_file(preflight) != PREFLIGHT_SHA256:
        raise RuntimeError("Session 7A preflight manifest hash mismatch")
    reference = artifact_dir / "reference_evidence/prices.parquet"
    if sha256_file(reference) != REFERENCE_SHA256:
        raise RuntimeError("reference-only data/prices.parquet hash mismatch")
    preflight_data = json.loads(preflight.read_text())
    for item in preflight_data["frozen_inputs"]:
        frozen = root / item["snapshot_path"]
        source = root / item["source_path"]
        if sha256_file(frozen) != item["sha256"] or sha256_file(source) != item["sha256"]:
            raise RuntimeError(f"frozen/source hash mismatch: {item['id']}")

    status_path = lineage_dir / "git_status_porcelain.txt"
    tracked_patch_path = lineage_dir / "tracked_dirty.patch"
    untracked_path = lineage_dir / "untracked_inventory.json"
    status_path.write_bytes(git(root, "status", "--porcelain=v1", "--untracked-files=all"))
    tracked_patch_path.write_bytes(git(root, "diff", "--binary", BASELINE_COMMIT, "--", "."))

    untracked_raw = git(root, "ls-files", "--others", "--exclude-standard", "-z")
    untracked_records = []
    for raw in untracked_raw.split(b"\0"):
        if not raw:
            continue
        relative = os.fsdecode(raw)
        path = root / relative
        if path.is_file() and path != manifest_path and path != untracked_path:
            untracked_records.append(record(root, path, role="untracked_dirty_state"))
    untracked_path.write_text(json.dumps(untracked_records, indent=2) + "\n")

    input_records = []
    for item in preflight_data["frozen_inputs"]:
        input_records.append(record(root, root / item["snapshot_path"], role=item["id"]))
    input_records.append(record(root, reference, role="reference_only_cache_coverage_evidence_not_pipeline_input"))
    input_records.append(record(root, root / "artifacts/pit_validation/legacy_saved/dataset/historical_dataset_clean.parquet", role="LEGACY_SAVED_comparison_evidence"))

    output_paths = sorted(
        path for path in artifact_dir.rglob("*")
        if path.is_file() and path not in {manifest_path, status_path, tracked_patch_path, untracked_path, reference}
    )
    output_records = [record(root, path, role="CORRECTED_PARTIAL_output") for path in output_paths]

    code_paths = [
        "pipeline/step3_enrich_prices.py",
        "pipeline/step5_compute_features.py",
        "pipeline/step6_clean.py",
        "pipeline/enrich_fraud_taxonomy.py",
        "pipeline/event_time_cohorts.py",
        "pipeline/winsorize_pit.py",
        "modeling/label_eligibility.py",
        "quality/compare_corrected_partial.py",
        "quality/freeze_corrected_partial_evidence.py",
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": "CORRECTED_PARTIAL",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_commit": BASELINE_COMMIT,
        "current_head": git(root, "rev-parse", "HEAD").decode().strip(),
        "claim": {
            "uses_stale_pre_fix_step2_snapshots": True,
            "conditioned_on_frozen_incomplete_daily_cache": True,
            "reproduces_LEGACY_SAVED": False,
            "reference_prices_is_pipeline_input": False,
        },
        "configuration": {
            "offline": True,
            "daily_cache_mode": "sqlite mode=ro&immutable=1",
            "missing_cache_series": "unavailable/null",
            "empty_cache_series": "unavailable/null",
            "network_fetch_or_refresh": False,
            "pipeline_stages": ["offline Step 3", "Step 5", "Step 6", "fraud-taxonomy enrichment"],
            "output_root": artifact_dir.relative_to(root).as_posix(),
        },
        "commands": [
            "python3 pipeline/step3_enrich_prices.py --offline --snapshots artifacts/pit_validation/corrected_partial_inputs/data/snapshots.parquet --cache artifacts/pit_validation/corrected_partial_inputs/data/price_cache.db --out artifacts/pit_validation/corrected_partial/01_prices_offline.parquet",
            "python3 pipeline/step5_compute_features.py ... (failed before I/O: ModuleNotFoundError: pipeline)",
            "python3 -m pipeline.step5_compute_features --snapshots artifacts/pit_validation/corrected_partial_inputs/data/snapshots.parquet --prices artifacts/pit_validation/corrected_partial/01_prices_offline.parquet --macro artifacts/pit_validation/corrected_partial_inputs/data/macro.parquet --out artifacts/pit_validation/corrected_partial/02_historical_dataset.parquet",
            "python3 -m pipeline.step6_clean --input artifacts/pit_validation/corrected_partial/02_historical_dataset.parquet --out artifacts/pit_validation/corrected_partial/03_historical_dataset_clean_pre_taxonomy.parquet",
            "python3 -m pipeline.enrich_fraud_taxonomy --input artifacts/pit_validation/corrected_partial/03_historical_dataset_clean_pre_taxonomy.parquet --out artifacts/pit_validation/corrected_partial/04_historical_dataset_clean_taxonomy.parquet",
            "python3 -m quality.compare_corrected_partial --legacy artifacts/pit_validation/legacy_saved/dataset/historical_dataset_clean.parquet --corrected artifacts/pit_validation/corrected_partial/04_historical_dataset_clean_taxonomy.parquet --out-dir artifacts/pit_validation/corrected_partial/comparison",
        ],
        "environment": {
            "python": sys.version,
            "dependencies": dependency_versions(),
        },
        "inputs": input_records,
        "outputs": output_records,
        "corrected_code_lineage": [record(root, root / path, role="corrected_code") for path in code_paths],
        "dirty_state": {
            "complete_status_recorded": True,
            "baseline": BASELINE_COMMIT,
            "status": record(root, status_path, role="dirty_status"),
            "tracked_patch": record(root, tracked_patch_path, role="tracked_dirty_patch"),
            "untracked_inventory": record(root, untracked_path, role="untracked_path_size_streaming_hash_inventory"),
            "manifest_self_excluded_to_avoid_recursive_hash": manifest_path.relative_to(root).as_posix(),
        },
        "limitations": [
            "Built from stale pre-fix Step 2 snapshots.",
            "Conditioned on a frozen incomplete daily cache; missing/empty series remain null.",
            "LEGACY_SAVED predates corrected code and is non-reproducible evidence only.",
            "No decision calendar was chosen; structural eligibility is not scoring-date eligibility.",
            "No predictions, model training, backtests, threshold optimization, or production comparison were run.",
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--artifact-dir", type=Path, required=True)
    args = parser.parse_args()
    path = build_manifest(args.root.resolve(), args.artifact_dir.resolve())
    print(f"{sha256_file(path)}  {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
