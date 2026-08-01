"""Preflight or build the bounded free-data V1 B1C evidence artifact."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from backtest.free_data_v1_evidence import (
    DEFAULT_ARTIFACT_PARENT,
    build_free_data_v1_evidence,
    preflight_summary,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("preflight", "build"),
        help="preflight is read-only; build writes one new version root",
    )
    parser.add_argument("--version")
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument(
        "--collect-dgs1mo",
        action="store_true",
        help=(
            "make only the exact frozen FRED metadata/observation requests; "
            "requires FRED_API_KEY"
        ),
    )
    args = parser.parse_args()

    if args.command == "preflight":
        if args.collect_dgs1mo:
            parser.error("preflight never makes an external request")
        print(json.dumps(preflight_summary(), indent=2, sort_keys=True))
        return

    if not args.version:
        parser.error("build requires --version ending in -b1c")
    target = args.artifact_root or DEFAULT_ARTIFACT_PARENT / args.version
    manifest = build_free_data_v1_evidence(
        target,
        version=args.version,
        collect_dgs1mo=args.collect_dgs1mo,
        fred_api_key=os.environ.get("FRED_API_KEY"),
    )
    print(manifest)


if __name__ == "__main__":
    main()
