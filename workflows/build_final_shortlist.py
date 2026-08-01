"""CLI for the offline canonical final-shortlist presentation derivative."""
from __future__ import annotations

import argparse
from pathlib import Path

from portfolio.final_shortlist import (
    DEFAULT_ARTIFACT_PARENT,
    DEFAULT_E1_ROOT,
    DEFAULT_P4_ROOT,
    build_final_shortlist_artifact,
    verify_final_shortlist_artifact,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build or verify the deterministic presentation-only derivative "
            "of the frozen canonical P4 shortlist and E1 adjudication."
        )
    )
    parser.add_argument("--shortlist-id")
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--p4-root", type=Path, default=DEFAULT_P4_ROOT)
    parser.add_argument("--e1-root", type=Path, default=DEFAULT_E1_ROOT)
    parser.add_argument("--verify-root", type=Path)
    args = parser.parse_args()
    if args.verify_root:
        print(verify_final_shortlist_artifact(args.verify_root))
        return
    if args.shortlist_id is None:
        parser.error("--shortlist-id is required to build")
    artifact_root = (
        args.artifact_root
        if args.artifact_root is not None
        else DEFAULT_ARTIFACT_PARENT / args.shortlist_id
    )
    manifest = build_final_shortlist_artifact(
        artifact_root,
        shortlist_id=args.shortlist_id,
        p4_root=args.p4_root,
        e1_root=args.e1_root,
    )
    print(manifest)


if __name__ == "__main__":
    main()
