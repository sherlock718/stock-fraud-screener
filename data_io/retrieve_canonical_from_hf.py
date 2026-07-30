"""Retrieve canonical P2-P4 artifacts from tracked immutable pointers."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from _root import ROOT
from data_io.canonical_hf import (
    CanonicalArtifactError,
    DEFAULT_POINTER_DIR,
    require_hf_token,
    retrieve_from_pointers,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pointer-dir",
        type=Path,
        default=DEFAULT_POINTER_DIR,
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=ROOT / "artifacts/canonical",
        help=(
            "non-existing canonical artifact root to create; existing "
            "targets are never reused"
        ),
    )
    args = parser.parse_args()
    try:
        from huggingface_hub import HfApi, hf_hub_download

        result = retrieve_from_pointers(
            pointer_dir=args.pointer_dir,
            target=args.target,
            api=HfApi(),
            token=require_hf_token(),
            downloader=hf_hub_download,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except ImportError:
        print("ERROR: huggingface_hub is required for canonical retrieval")
        return 1
    except CanonicalArtifactError as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
