"""CLI for offline E1 citation-by-citation human-review adjudication."""
from __future__ import annotations

import argparse
from pathlib import Path

from portfolio.event_review_adjudication import (
    DEFAULT_EXTRACTION_ROOT,
    build_event_review_adjudication,
    verify_event_review_adjudication_artifact,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Offline-only adjudication of the frozen E1 15-name review queue. "
            "This workflow never performs an external request."
        )
    )
    parser.add_argument(
        "--extraction-root",
        type=Path,
        default=DEFAULT_EXTRACTION_ROOT,
    )
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--adjudication-id")
    parser.add_argument("--verify-root", type=Path)
    args = parser.parse_args()
    if args.verify_root:
        print(verify_event_review_adjudication_artifact(args.verify_root))
        return
    if args.artifact_root is None or args.adjudication_id is None:
        parser.error(
            "--artifact-root and --adjudication-id are required to build"
        )
    manifest = build_event_review_adjudication(
        args.artifact_root,
        adjudication_id=args.adjudication_id,
        extraction_root=args.extraction_root,
    )
    print(manifest)


if __name__ == "__main__":
    main()
