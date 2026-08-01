"""CLI for offline E1 primary-document extraction and reconciliation."""
from __future__ import annotations

import argparse
from pathlib import Path

from portfolio.event_review_extraction import (
    DEFAULT_COLLECTION_ROOT,
    build_retrieved_event_review,
    verify_retrieved_event_review_artifact,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Offline-only extraction of already retrieved E1 SEC primary "
            "documents. This workflow never performs an external request."
        )
    )
    parser.add_argument(
        "--collection-root",
        type=Path,
        default=DEFAULT_COLLECTION_ROOT,
    )
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--review-id")
    parser.add_argument("--verify-root", type=Path)
    args = parser.parse_args()
    if args.verify_root:
        print(verify_retrieved_event_review_artifact(args.verify_root))
        return
    if args.artifact_root is None or args.review_id is None:
        parser.error("--artifact-root and --review-id are required to build")
    manifest = build_retrieved_event_review(
        args.artifact_root,
        review_id=args.review_id,
        collection_root=args.collection_root,
    )
    print(manifest)


if __name__ == "__main__":
    main()
