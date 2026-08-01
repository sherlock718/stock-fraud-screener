"""Build an immutable S1 security ledger from already-frozen D1 evidence."""
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.security_ledger import (
    DEFAULT_D1_ROOT,
    DEFAULT_P4_ROOT,
    ROOT,
    build_security_ledger,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Offline S1 ledger build. This command makes no external request."
        )
    )
    parser.add_argument("--ledger-id", required=True)
    parser.add_argument(
        "--artifact-root",
        type=Path,
        help="default: artifacts/security_ledger/us/<ledger-id>",
    )
    parser.add_argument("--d1-root", type=Path, default=DEFAULT_D1_ROOT)
    parser.add_argument("--p4-root", type=Path, default=DEFAULT_P4_ROOT)
    args = parser.parse_args()
    target = args.artifact_root or (
        ROOT / "artifacts/security_ledger/us" / args.ledger_id
    )
    manifest = build_security_ledger(
        target,
        ledger_id=args.ledger_id,
        d1_root=args.d1_root,
        p4_root=args.p4_root,
    )
    print(manifest)


if __name__ == "__main__":
    main()

