"""Run the versioned, non-overwriting US canonical raw-refresh route.

Without ``--collect`` this command emits a local execution plan and performs no
network action. External collection additionally requires the exact refresh ID
through ``--confirm-external-collection``. The route creates a review-only P2
candidate and comparison; it never changes the pinned P2-P4 roots or pointers
and never promotes, publishes, archives, commits, or pushes.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline import build_contract_label_inputs as market_builder
from pipeline.build_refreshed_us_p2 import build as build_p2_candidate
from pipeline.step2_artifact_rebuild import run as build_corrected_step2
from pipeline.us_refresh_sources import (
    BASELINE_CALENDAR_CONTRACT,
    collect_us_universe,
    freeze_market_candidate,
    freeze_step2_candidate,
    initialize_market_candidate,
    sha256_file,
    validate_refresh_id,
)
from quality.compare_canonical_p2_versions import (
    PINNED_BASELINE_MANIFEST_SHA256,
    compare as compare_p2_versions,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REFRESH_PARENT = ROOT / "artifacts/canonical_refresh/us"
PINNED_HF_REVISION = "aaf056ea115067e42ef9abf9fa93ade75cdd4052"
PINNED_POINTERS = (
    ROOT / "data_io/canonical_artifact_pointers/p2.json",
    ROOT / "data_io/canonical_artifact_pointers/p3.json",
    ROOT / "data_io/canonical_artifact_pointers/p4.json",
)
PINNED_CANONICAL_MANIFESTS = (
    ROOT / "artifacts/canonical/corrected_us_annual/manifest.json",
    ROOT
    / "artifacts/canonical/corrected_us_annual_3y_research_model/"
    "manifest.json",
    ROOT
    / "artifacts/canonical/corrected_us_annual_3y_product/manifest.json",
)
PINNED_SOURCE_RECORDS = {
    ROOT / "artifacts/pit_validation/corrected_step2/manifest.json": (
        "899cffd7a9d1dc3395a08bee5c65ad4a5e8a109a83c63346ac54c891fe706e08"
    ),
    (
        ROOT
        / "artifacts/pit_validation/contract_aligned_label_inputs/"
        "manifest.json"
    ): "0ab15685a445f09919b19393e074b8b5903afef7e12defc7c44aeac624f4581a",
    BASELINE_CALENDAR_CONTRACT: (
        "13cd7494aa7f0ff6e3f8a11efa0ee7a9a087968bb20b1fd5a9cb57f380148296"
    ),
}


def stage_paths(refresh_root: Path) -> dict[str, Path]:
    return {
        "root": refresh_root,
        "universe": refresh_root / "universe",
        "step2": refresh_root / "filings",
        "market": refresh_root / "market",
        "p2": refresh_root / "p2_review_candidate",
        "comparison": refresh_root / "review/comparison",
        "review_manifest": refresh_root / "review/review_manifest.json",
    }


def _preserved_tracked_paths() -> list[Path]:
    international = []
    for pattern in (
        "pipeline/step1_fetch_tickers_*.py",
        "pipeline/step2_build_snapshots_*.py",
        "workflows/run_pipeline_*.py",
    ):
        international.extend(ROOT.glob(pattern))
    fixed = [
        ROOT / "workflows/run_pipeline.py",
        ROOT / ".github/workflows/refresh_data.yml",
        *PINNED_POINTERS,
    ]
    return sorted({path.resolve() for path in [*international, *fixed]})


def preservation_snapshot() -> dict[str, Any]:
    pointer_revisions = {
        path.name: json.loads(path.read_text())["revision"]
        for path in PINNED_POINTERS
    }
    if set(pointer_revisions.values()) != {PINNED_HF_REVISION}:
        raise RuntimeError("canonical pointers no longer share the pinned C2 revision")
    canonical_hashes = {
        path.relative_to(ROOT).as_posix(): sha256_file(path)
        for path in PINNED_CANONICAL_MANIFESTS
    }
    if (
        canonical_hashes[
            "artifacts/canonical/corrected_us_annual/manifest.json"
        ]
        != PINNED_BASELINE_MANIFEST_SHA256
    ):
        raise RuntimeError("pinned P2 baseline manifest changed")
    source_hashes = {
        path.relative_to(ROOT).as_posix(): sha256_file(path)
        for path in PINNED_SOURCE_RECORDS
    }
    for path, expected in PINNED_SOURCE_RECORDS.items():
        if source_hashes[path.relative_to(ROOT).as_posix()] != expected:
            raise RuntimeError(f"pinned source evidence changed: {path}")
    return {
        "canonical_manifests": canonical_hashes,
        "source_evidence": source_hashes,
        "pointer_revisions": pointer_revisions,
        "tracked_legacy_and_international": {
            path.relative_to(ROOT).as_posix(): sha256_file(path)
            for path in _preserved_tracked_paths()
        },
    }


def plan(refresh_id: str, refresh_root: Path) -> dict[str, Any]:
    validate_refresh_id(refresh_id)
    paths = stage_paths(refresh_root.resolve())
    return {
        "schema_version": 1,
        "route": "US canonical raw refresh -> review-only P2",
        "refresh_id": refresh_id,
        "refresh_root": str(paths["root"]),
        "stages": [
            {
                "stage": "universe",
                "path": str(paths["universe"]),
                "external_sources": [
                    "SEC company_tickers",
                    "SEC company_tickers_exchange",
                    "SEC submissions per CIK",
                ],
                "exact_raw_responses": True,
            },
            {
                "stage": "filings",
                "path": str(paths["step2"]),
                "external_sources": ["SEC Company Facts per CIK"],
                "corrected_step2_materialization": True,
            },
            {
                "stage": "market",
                "path": str(paths["market"]),
                "external_sources": [
                    "Yahoo chart per mapped security and IWC/IWM/MDY/SPY"
                ],
                "contracts": [
                    "price",
                    "benchmark",
                    "calendar",
                    "decision",
                    "label_support",
                ],
            },
            {
                "stage": "P2 candidate",
                "path": str(paths["p2"]),
                "external_sources": [],
                "step5": "corrected PIT transformations",
                "step6": (
                    "observed-only; imputation and inferred delisting returns disabled"
                ),
            },
            {
                "stage": "baseline comparison",
                "path": str(paths["comparison"]),
                "dimensions": [
                    "row identity",
                    "schema",
                    "feature coverage",
                    "label support",
                    "missingness",
                    "gates",
                    "source drift",
                ],
            },
        ],
        "non_overwriting": True,
        "baseline_p2_manifest_sha256": PINNED_BASELINE_MANIFEST_SHA256,
        "pinned_hugging_face_revision": PINNED_HF_REVISION,
        "automatic_promotion": False,
        "publication": False,
        "international_collectors_modified": False,
        "external_request_would_be_made": False,
    }


def _write_review_manifest(
    *,
    paths: dict[str, Path],
    refresh_id: str,
    preserved_before: dict[str, Any],
) -> Path:
    preserved_after = preservation_snapshot()
    if preserved_before != preserved_after:
        raise RuntimeError(
            "pinned baseline, pointer, legacy, or international evidence changed"
        )
    component_manifests = {
        stage: path / "manifest.json"
        for stage, path in paths.items()
        if stage in {"universe", "step2", "market", "p2", "comparison"}
    }
    missing = [
        path for path in component_manifests.values() if not path.is_file()
    ]
    if missing:
        raise RuntimeError(f"review component manifest missing: {missing}")
    payload = {
        "schema_version": 1,
        "artifact_class": "CANONICAL_US_REFRESH_P2_REVIEW",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "refresh_id": refresh_id,
        "components": {
            name: {
                "path": str(path),
                "sha256": sha256_file(path),
            }
            for name, path in component_manifests.items()
        },
        "pinned_baseline": {
            "p2_manifest_sha256": PINNED_BASELINE_MANIFEST_SHA256,
            "hugging_face_revision": PINNED_HF_REVISION,
            "mutated": False,
        },
        "preservation": preserved_after,
        "policy": {
            "macro_vintages": "unavailable_until_certified",
            "primary_population": "observed_only",
            "inferred_delisting_returns": False,
            "legacy_six_step_preserved": True,
            "international_per_market_structure_preserved": True,
        },
        "promotion": {
            "status": "not_promoted",
            "automatic_promotion": False,
            "explicit_authorization_required": True,
            "p3_p4_consumption_allowed": False,
        },
        "external_actions": {
            "hugging_face_publish": False,
            "archive": False,
            "branch": False,
            "commit": False,
            "push": False,
        },
    }
    target = paths["review_manifest"]
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise RuntimeError(f"review manifest already exists: {target}")
    target.write_text(json.dumps(payload, indent=2) + "\n")
    return target


def run(
    *,
    refresh_id: str,
    refresh_root: Path,
    end_date_exclusive: str,
    workers: int = 4,
    retries: int = 3,
) -> dict[str, Any]:
    validate_refresh_id(refresh_id)
    paths = stage_paths(refresh_root.resolve())
    if paths["review_manifest"].exists():
        raise RuntimeError(f"refresh version is already complete: {refresh_root}")
    preserved_before = preservation_snapshot()

    if not (paths["universe"] / "manifest.json").is_file():
        collect_us_universe(
            paths["universe"],
            refresh_id=refresh_id,
            retries=retries,
            workers=workers,
        )
    universe = pd.read_parquet(
        paths["universe"] / "outputs/universe.parquet"
    )

    if not (paths["step2"] / "manifest.json").is_file():
        build_corrected_step2(
            paths["universe"] / "outputs/universe.parquet",
            paths["step2"],
            retry_transient=True,
            expected_ciks=len(universe),
            workers=workers,
        )
        freeze_step2_candidate(
            artifact_root=paths["step2"],
            universe_root=paths["universe"],
            refresh_id=refresh_id,
        )

    if not (paths["market"] / "manifest.json").is_file():
        config = paths["market"] / "configuration/config.json"
        if not config.is_file():
            initialize_market_candidate(
                step2_root=paths["step2"],
                artifact_root=paths["market"],
                refresh_id=refresh_id,
                end_date_exclusive=end_date_exclusive,
                calendar_contract_path=BASELINE_CALENDAR_CONTRACT,
            )
        if not (
            paths["market"] / "calendar/calendar_evidence.json"
        ).is_file():
            market_builder.freeze_calendar(paths["market"])
        if not (
            paths["market"] / "normalized/normalization_summary.parquet"
        ).is_file():
            market_builder.fetch(paths["market"], workers, retries)
            market_builder.normalize(paths["market"])
        if not (
            paths["market"] / "support/horizon_support.parquet"
        ).is_file():
            market_builder.materialize_labels(paths["market"])
        freeze_market_candidate(
            artifact_root=paths["market"],
            step2_root=paths["step2"],
            refresh_id=refresh_id,
        )

    if not (paths["p2"] / "manifest.json").is_file():
        build_p2_candidate(
            step2_root=paths["step2"],
            market_root=paths["market"],
            artifact_root=paths["p2"],
            refresh_id=refresh_id,
        )
    if not (paths["comparison"] / "manifest.json").is_file():
        compare_p2_versions(
            candidate_root=paths["p2"],
            candidate_step2_root=paths["step2"],
            candidate_market_root=paths["market"],
            output_root=paths["comparison"],
        )
    review = _write_review_manifest(
        paths=paths,
        refresh_id=refresh_id,
        preserved_before=preserved_before,
    )
    return {
        "refresh_id": refresh_id,
        "review_manifest": str(review),
        "review_manifest_sha256": sha256_file(review),
        "p2_candidate_manifest_sha256": sha256_file(
            paths["p2"] / "manifest.json"
        ),
        "comparison_manifest_sha256": sha256_file(
            paths["comparison"] / "manifest.json"
        ),
        "promotion_status": "not_promoted",
        "published": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-id", required=True)
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument(
        "--end-date-exclusive",
        default=(
            pd.Timestamp.now(tz="UTC").normalize() + pd.Timedelta(days=1)
        ).strftime("%Y-%m-%d"),
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument(
        "--collect",
        action="store_true",
        help="authorize the route to make its external SEC/Yahoo requests",
    )
    parser.add_argument(
        "--confirm-external-collection",
        help="must exactly match --refresh-id when --collect is used",
    )
    args = parser.parse_args()
    refresh_id = validate_refresh_id(args.refresh_id)
    refresh_root = (
        args.artifact_root.resolve()
        if args.artifact_root
        else (DEFAULT_REFRESH_PARENT / refresh_id).resolve()
    )
    if not args.collect:
        print(json.dumps(plan(refresh_id, refresh_root), indent=2))
        return 0
    if args.confirm_external_collection != refresh_id:
        raise RuntimeError(
            "--collect requires --confirm-external-collection to exactly "
            "match --refresh-id"
        )
    print(
        json.dumps(
            run(
                refresh_id=refresh_id,
                refresh_root=refresh_root,
                end_date_exclusive=args.end_date_exclusive,
                workers=args.workers,
                retries=args.retries,
            ),
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
