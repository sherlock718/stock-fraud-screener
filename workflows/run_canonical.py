"""Verify or reconstruct the pinned canonical P2 -> P3 -> P4 local route.

By default the command verifies the accepted manifests in dependency order.
With ``--build-missing`` it invokes a builder only when that stage's artifact
root is absent. Existing roots are never overwritten, and an existing manifest
must match the accepted hash before downstream work can continue.

This workflow is deterministic reconstruction only. It does not collect,
refresh, publish, archive, train a new strategy, or calculate performance.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Stage:
    name: str
    artifact_root: Path
    manifest_sha256: str
    builder: Callable[[Path], dict[str, Any]]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_stages() -> tuple[Stage, ...]:
    """Load canonical builders lazily, preserving dependency order."""
    from modeling.build_canonical_research_model import (
        CANONICAL_P2_MANIFEST_SHA256,
        DEFAULT_ARTIFACT_ROOT as P3_ROOT,
        build as build_p3,
    )
    from pipeline.build_corrected_feature_population import (
        DEFAULT_ARTIFACT_ROOT as P2_ROOT,
        build as build_p2,
    )
    from portfolio.build_canonical_product import (
        DEFAULT_ARTIFACT_ROOT as P4_ROOT,
        P3_MANIFEST_SHA256,
        build as build_p4,
    )

    return (
        Stage(
            "P2",
            P2_ROOT,
            CANONICAL_P2_MANIFEST_SHA256,
            build_p2,
        ),
        Stage(
            "P3",
            P3_ROOT,
            P3_MANIFEST_SHA256,
            build_p3,
        ),
        Stage(
            "P4",
            P4_ROOT,
            (
                "28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d"
                "915f3d1edd1aabc7"
            ),
            build_p4,
        ),
    )


def validate_stage(stage: Stage) -> dict[str, Any]:
    manifest = stage.artifact_root / "manifest.json"
    if not manifest.is_file():
        raise RuntimeError(
            f"{stage.name} manifest is missing: {manifest}"
        )
    actual = sha256_file(manifest)
    if actual != stage.manifest_sha256:
        raise RuntimeError(
            f"{stage.name} manifest hash mismatch: "
            f"expected={stage.manifest_sha256} actual={actual}"
        )
    return {
        "stage": stage.name,
        "status": "verified",
        "artifact_root": stage.artifact_root.relative_to(ROOT).as_posix(),
        "manifest_sha256": actual,
    }


def run(build_missing: bool = False) -> list[dict[str, Any]]:
    """Validate each stage, optionally building an absent stage in order."""
    results: list[dict[str, Any]] = []
    for stage in canonical_stages():
        manifest = stage.artifact_root / "manifest.json"
        if not manifest.exists():
            if not build_missing:
                raise RuntimeError(
                    f"{stage.name} is absent; rerun with --build-missing "
                    "to invoke its non-overwriting builder"
                )
            if stage.artifact_root.exists() and any(
                stage.artifact_root.iterdir()
            ):
                raise RuntimeError(
                    f"{stage.name} target exists and is non-empty: "
                    f"{stage.artifact_root}"
                )
            stage.builder(stage.artifact_root)
        results.append(validate_stage(stage))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--build-missing",
        action="store_true",
        help=(
            "invoke a canonical builder only for an absent stage; "
            "existing stages remain read-only"
        ),
    )
    args = parser.parse_args()
    print(
        json.dumps(
            {
                "route": "P2 -> P3 -> P4",
                "mode": (
                    "build_missing" if args.build_missing else "verify_only"
                ),
                "stages": run(build_missing=args.build_missing),
                "external_data_collected": False,
                "published": False,
                "performance_calculated": False,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
