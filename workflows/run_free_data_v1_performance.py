"""Preflight, build, or verify the controlled B1E free-data V1 artifact."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from backtest.free_data_v1_performance import (
    DEFAULT_ARTIFACT_PARENT,
    DEFAULT_B1C_ROOT,
    build_free_data_v1_performance,
    preflight_controlled_run,
    verify_performance_artifact,
)


def _default_version() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-b1e")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "build", "verify"))
    parser.add_argument("--version")
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--b1c-root", type=Path, default=DEFAULT_B1C_ROOT)
    parser.add_argument("--expected-manifest-sha256")
    args = parser.parse_args()

    if args.command == "preflight":
        result = preflight_controlled_run(args.b1c_root)
        print(json.dumps(result.summary, indent=2, sort_keys=True))
        return

    if args.command == "verify":
        if args.artifact_root is None:
            parser.error("verify requires --artifact-root")
        print(
            json.dumps(
                verify_performance_artifact(
                    args.artifact_root,
                    expected_manifest_sha256=args.expected_manifest_sha256,
                ),
                indent=2,
                sort_keys=True,
            )
        )
        return

    version = args.version or _default_version()
    target = args.artifact_root or DEFAULT_ARTIFACT_PARENT / version
    manifest = build_free_data_v1_performance(
        target,
        version=version,
        b1c_root=args.b1c_root,
    )
    print(manifest)


if __name__ == "__main__":
    main()
