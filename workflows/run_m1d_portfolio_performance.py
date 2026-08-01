"""Command boundary for the two-phase, one-shot M1D comparison."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from backtest.m1d_portfolio_performance import (
    execute_locked_m1d,
    prepare_m1d_lock,
    sha256_file,
    verify_m1d_artifact,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--prepare-lock", action="store_true")
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--verify", action="store_true")
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--version")
    parser.add_argument("--focused-test-report", type=Path)
    parser.add_argument("--lock-manifest-sha256")
    parser.add_argument("--expected-manifest-sha256")
    args = parser.parse_args()

    if args.prepare_lock:
        if not args.version or args.focused_test_report is None:
            parser.error("--prepare-lock requires --version and --focused-test-report")
        locked = prepare_m1d_lock(
            args.artifact_root,
            version=args.version,
            focused_test_report=args.focused_test_report,
        )
        result = {
            "artifact_root": str(locked.root),
            "lock_manifest_sha256": locked.lock_manifest_sha256,
            "status": "locked_before_portfolio_or_performance",
        }
    elif args.execute:
        if not args.lock_manifest_sha256:
            parser.error("--execute requires --lock-manifest-sha256")
        manifest = execute_locked_m1d(
            args.artifact_root,
            expected_lock_manifest_sha256=args.lock_manifest_sha256,
        )
        result = {
            "manifest": str(manifest),
            "manifest_sha256": sha256_file(manifest),
            "status": "one_locked_evaluation_complete",
        }
    else:
        if not args.lock_manifest_sha256:
            parser.error("--verify requires --lock-manifest-sha256")
        result = dict(
            verify_m1d_artifact(
                args.artifact_root,
                expected_manifest_sha256=args.expected_manifest_sha256,
                expected_lock_manifest_sha256=args.lock_manifest_sha256,
            )
        )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
