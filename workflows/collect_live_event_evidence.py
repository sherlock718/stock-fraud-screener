"""Execute an already-frozen E1 live collection plan after approval."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from portfolio.event_review import (
    FROZEN_E1_MANIFEST_SHA256,
    FROZEN_E1_PLAN_SHA256,
    collect_live_event_evidence,
    verify_live_event_collection_artifact,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "External SEC collection. Without --collect and the exact "
            "artifact approval token, this command only prints the plan."
        )
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument(
        "--collection-root",
        type=Path,
        help=(
            "new top-level non-overwriting event-review artifact directory"
        ),
    )
    parser.add_argument("--collection-id")
    parser.add_argument(
        "--predecessor-root",
        type=Path,
        help=(
            "verified incomplete collection artifact whose exact responses "
            "are reused before requesting only the unattempted suffix"
        ),
    )
    parser.add_argument("--collect", action="store_true")
    parser.add_argument("--confirm-external-collection")
    parser.add_argument(
        "--verify-root",
        type=Path,
        help="independently verify a completed live collection artifact",
    )
    args = parser.parse_args()
    if args.verify_root:
        print(verify_live_event_collection_artifact(args.verify_root))
        return
    contract = json.loads(
        (args.artifact_root / "live/collection_contract.json").read_text()
    )
    if not args.collect:
        print(json.dumps(contract, indent=2, sort_keys=True))
        return
    manifest = collect_live_event_evidence(
        args.artifact_root,
        confirmation=args.confirm_external_collection,
        collection_root=args.collection_root,
        collection_id=args.collection_id,
        expected_manifest_sha256=FROZEN_E1_MANIFEST_SHA256,
        expected_plan_sha256=FROZEN_E1_PLAN_SHA256,
        predecessor_root=args.predecessor_root,
    )
    print(manifest)


if __name__ == "__main__":
    main()
