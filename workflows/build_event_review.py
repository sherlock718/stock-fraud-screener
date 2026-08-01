"""Build or verify the offline E1 historical-then-live event review."""
from __future__ import annotations

import argparse
from pathlib import Path

from portfolio.event_review import (
    DEFAULT_D1_ROOT,
    DEFAULT_P4_ROOT,
    DEFAULT_S1_ROOT,
    ROOT,
    build_event_review,
    verify_event_review_artifact,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build E1 only from exact local S1/P4 evidence. This command "
            "makes no external request."
        )
    )
    parser.add_argument("--review-id")
    parser.add_argument(
        "--artifact-root",
        type=Path,
        help="default: artifacts/event_review/us/<review-id>",
    )
    parser.add_argument("--s1-root", type=Path, default=DEFAULT_S1_ROOT)
    parser.add_argument("--d1-root", type=Path, default=DEFAULT_D1_ROOT)
    parser.add_argument("--p4-root", type=Path, default=DEFAULT_P4_ROOT)
    parser.add_argument(
        "--verify-root",
        type=Path,
        help="independently verify an existing E1 artifact and exit",
    )
    args = parser.parse_args()
    if args.verify_root:
        print(verify_event_review_artifact(args.verify_root))
        return
    if not args.review_id:
        parser.error("--review-id is required unless --verify-root is used")
    target = args.artifact_root or (
        ROOT / "artifacts/event_review/us" / args.review_id
    )
    manifest = build_event_review(
        target,
        review_id=args.review_id,
        s1_root=args.s1_root,
        d1_root=args.d1_root,
        p4_root=args.p4_root,
    )
    print(manifest)


if __name__ == "__main__":
    main()
