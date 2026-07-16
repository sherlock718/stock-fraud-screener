"""Validate frozen PIT evidence manifests without loading artifact payloads."""
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SCHEMA_VERSION = 1
REQUIRED_ARTIFACT_IDS = {
    "clean_dataset",
    "classifier_model_1y",
    "classifier_model_3y",
    "classifier_model_5y",
    "model_feature_metadata",
    "row_level_prediction_lineage",
    "monthly_price_cache",
    "canonical_benchmark_nav",
    "monthly_risk_free_returns",
    "adjusted_price_provenance",
    "corporate_action_evidence",
    "security_mapping_evidence",
    "saved_backtest_result",
    "saved_holdings_and_weights",
    "saved_strategy_configuration",
}
REQUIRED_LIMITATIONS = {
    "legacy_artifacts_predate_sessions_1_through_6b",
    "legacy_performance_not_reproducible",
    "missing_holdings_weights_folds_and_score_sources",
    "missing_canonical_benchmark_nav",
    "missing_time_aligned_monthly_risk_free_returns",
    "missing_adjusted_price_provenance",
    "missing_corporate_action_and_security_mapping_evidence",
    "corrected_nav_schema_is_not_a_legacy_input",
}


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Hash *path* in bounded chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class ValidationResult:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def compatible(self) -> bool:
        return not self.errors


def _duplicates(values: Iterable[str]) -> set[str]:
    seen: set[str] = set()
    repeated: set[str] = set()
    for value in values:
        if value in seen:
            repeated.add(value)
        seen.add(value)
    return repeated


def validate_manifest(
    manifest_path: str | Path,
    *,
    check_sources: bool = False,
    current_commit: str | None = None,
) -> ValidationResult:
    """Validate structure, frozen hashes, and legacy/corrected role separation.

    ``current_commit`` is diagnostic only. Later documentation or validator
    commits must not invalidate a correctly frozen evidence manifest.
    """
    path = Path(manifest_path)
    errors: list[str] = []
    warnings: list[str] = []
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return ValidationResult((f"cannot_read_manifest: {exc}",), ())

    if manifest.get("schema_version") != SCHEMA_VERSION:
        errors.append("unsupported_schema_version")
    if manifest.get("snapshot_type") != "LEGACY_SAVED":
        errors.append("snapshot_type_must_be_LEGACY_SAVED")
    if manifest.get("baseline_commit") != "3f706e3e10d2b354c6e8b9407760fa2074749c0a":
        errors.append("unexpected_baseline_commit")

    claim = manifest.get("claim", {})
    if claim.get("reproducible") is not False:
        errors.append("legacy_claim_must_be_marked_non_reproducible")
    if claim.get("snapshot_is_evidence_only") is not True:
        errors.append("snapshot_must_be_evidence_only")

    limitation_ids = {item.get("id") for item in manifest.get("limitations", [])}
    for missing in sorted(REQUIRED_LIMITATIONS - limitation_ids):
        errors.append(f"missing_limitation:{missing}")

    worktree = manifest.get("dirty_state", {})
    if not worktree.get("complete_status_recorded"):
        errors.append("complete_dirty_status_not_recorded")
    if not worktree.get("tracked_patch") or not worktree.get("untracked_patch"):
        errors.append("complete_dirty_patches_not_recorded")
    if not worktree.get("sensitive_content_exclusions"):
        errors.append("sensitive_content_exclusions_not_recorded")
    for key in ("status", "tracked_patch", "untracked_patch"):
        record = worktree.get(key, {})
        record_path = record.get("path")
        if not record_path or not record.get("sha256"):
            errors.append(f"invalid_dirty_state_record:{key}")
            continue
        frozen_record = path.parent / record_path
        if not frozen_record.is_file():
            errors.append(f"dirty_state_payload_missing:{key}")
            continue
        if frozen_record.stat().st_size != record.get("size_bytes"):
            errors.append(f"dirty_state_size_mismatch:{key}")
        if sha256_file(frozen_record) != record["sha256"]:
            errors.append(f"dirty_state_hash_mismatch:{key}")

    artifacts = manifest.get("artifacts", [])
    ids = [item.get("id") for item in artifacts]
    for duplicate in sorted(_duplicates(ids)):
        errors.append(f"duplicate_artifact_id:{duplicate}")
    for missing in sorted(REQUIRED_ARTIFACT_IDS - set(ids)):
        errors.append(f"missing_artifact_record:{missing}")

    root = path.parent
    for item in artifacts:
        artifact_id = item.get("id", "<unknown>")
        status = item.get("status")
        if status not in {"present", "missing"}:
            errors.append(f"invalid_status:{artifact_id}")
            continue
        if item.get("role") == "corrected_code_evidence" and item.get("legacy_input") is not False:
            errors.append(f"corrected_evidence_marked_as_legacy_input:{artifact_id}")
        if status == "missing":
            if item.get("sha256") or item.get("snapshot_path"):
                errors.append(f"missing_artifact_has_payload:{artifact_id}")
            continue

        snapshot_path = item.get("snapshot_path")
        expected_hash = item.get("sha256")
        if not snapshot_path or not expected_hash:
            errors.append(f"present_artifact_missing_snapshot_or_hash:{artifact_id}")
            continue
        frozen = root / snapshot_path
        if not frozen.is_file():
            errors.append(f"frozen_artifact_missing:{artifact_id}")
            continue
        if frozen.stat().st_size != item.get("size_bytes"):
            errors.append(f"size_mismatch:{artifact_id}")
        if sha256_file(frozen) != expected_hash:
            errors.append(f"hash_mismatch:{artifact_id}")

        if check_sources and item.get("source_path"):
            source = Path(item["source_path"])
            if not source.is_absolute():
                source = root.parents[2] / source
            if not source.is_file():
                warnings.append(f"source_missing:{artifact_id}")
            elif sha256_file(source) != expected_hash:
                warnings.append(f"source_changed:{artifact_id}")

    if current_commit and current_commit != manifest.get("snapshot_code_commit"):
        warnings.append("current_commit_differs_from_snapshot_commit")

    return ValidationResult(tuple(errors), tuple(warnings))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--check-sources", action="store_true")
    parser.add_argument("--current-commit")
    args = parser.parse_args()
    result = validate_manifest(
        args.manifest,
        check_sources=args.check_sources,
        current_commit=args.current_commit,
    )
    print(json.dumps({
        "compatible": result.compatible,
        "errors": list(result.errors),
        "warnings": list(result.warnings),
    }, indent=2))
    return 0 if result.compatible else 1


if __name__ == "__main__":
    raise SystemExit(main())
