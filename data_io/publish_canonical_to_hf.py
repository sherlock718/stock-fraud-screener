"""Prepare, preflight, or publish the private immutable P2-P4 baseline.

Preparation is local-only.  ``--check-visibility`` performs authenticated
read-only repository checks.  ``--publish`` is the explicit external-write
boundary and additionally requires ``--confirm-repo``.
"""
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from data_io.canonical_hf import (
    CanonicalArtifactError,
    DEFAULT_POINTER_DIR,
    DEFAULT_REPO_ID,
    build_publication_plan,
    plan_summary,
    publication_preflight,
    publish_plan,
    require_hf_token,
    retrieve_pointer_documents,
    write_pointer_documents,
    write_publication_plan,
)


def _api():
    try:
        from huggingface_hub import HfApi
    except ImportError as exc:
        raise CanonicalArtifactError(
            "huggingface_hub is required for authenticated Hub operations"
        ) from exc
    return HfApi()


def _downloader():
    try:
        from huggingface_hub import hf_hub_download
    except ImportError as exc:
        raise CanonicalArtifactError(
            "huggingface_hub is required for authenticated Hub operations"
        ) from exc
    return hf_hub_download


def _print_json(value: dict) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument(
        "--prepare",
        action="store_true",
        help="validate and summarize the exact local plan; never use network",
    )
    action.add_argument(
        "--check-visibility",
        action="store_true",
        help=(
            "verify private repository visibility and destination absence "
            "without uploading"
        ),
    )
    action.add_argument(
        "--publish",
        action="store_true",
        help=(
            "create the immutable remote commit, verify a temporary "
            "download, and write pointer manifests"
        ),
    )
    action.add_argument(
        "--finalize-revision",
        help=(
            "verify an already-created immutable revision and write pointers "
            "without uploading; recovery path for post-commit interruption"
        ),
    )
    parser.add_argument("--repo", default=DEFAULT_REPO_ID)
    parser.add_argument(
        "--plan-output",
        type=Path,
        help="optional non-overwriting JSON output for the exact local plan",
    )
    parser.add_argument(
        "--pointer-dir",
        type=Path,
        default=DEFAULT_POINTER_DIR,
    )
    parser.add_argument(
        "--confirm-repo",
        help="required with --publish and must exactly equal --repo",
    )
    parser.add_argument(
        "--message",
        default="Publish immutable canonical P2-P4 baseline",
    )
    args = parser.parse_args()

    try:
        plan = build_publication_plan(repo_id=args.repo)
        if args.plan_output is not None:
            write_publication_plan(plan, args.plan_output)
        summary = plan_summary(plan)
        if args.prepare:
            _print_json(summary)
            return 0

        token = require_hf_token()
        api = _api()
        if args.check_visibility:
            preflight = publication_preflight(
                plan,
                api=api,
                token=token,
            )
            _print_json(
                {
                    **summary,
                    **preflight,
                    "uploads_performed": False,
                }
            )
            return 0

        if args.finalize_revision:
            from data_io.canonical_hf import create_pointer_documents

            pointers = create_pointer_documents(
                plan,
                revision=args.finalize_revision,
            )
            with tempfile.TemporaryDirectory(
                prefix="canonical-hf-verification-"
            ) as temporary:
                verification_target = Path(temporary) / "canonical"
                retrieval = retrieve_pointer_documents(
                    pointers,
                    target=verification_target,
                    api=api,
                    token=token,
                    downloader=_downloader(),
                    expected_repo_id=plan.repository,
                )
            write_pointer_documents(
                args.pointer_dir,
                pointers,
                expected_repo_id=plan.repository,
            )
            _print_json(
                {
                    **summary,
                    "revision": args.finalize_revision,
                    "temporary_retrieval": retrieval,
                    "pointer_directory": str(args.pointer_dir),
                    "uploads_performed": False,
                    "finalized_existing_revision": True,
                }
            )
            return 0

        if args.confirm_repo != args.repo:
            raise CanonicalArtifactError(
                "--publish requires --confirm-repo to exactly match --repo"
            )
        revision, pointers, preflight = publish_plan(
            plan,
            api=api,
            token=token,
            commit_message=args.message,
        )
        try:
            with tempfile.TemporaryDirectory(
                prefix="canonical-hf-verification-"
            ) as temporary:
                verification_target = Path(temporary) / "canonical"
                retrieval = retrieve_pointer_documents(
                    pointers,
                    target=verification_target,
                    api=api,
                    token=token,
                    downloader=_downloader(),
                    expected_repo_id=plan.repository,
                )
            write_pointer_documents(
                args.pointer_dir,
                pointers,
                expected_repo_id=plan.repository,
            )
        except CanonicalArtifactError as exc:
            raise CanonicalArtifactError(
                f"remote commit {revision} was created, but verification or "
                "pointer materialization failed; after resolving the cause, "
                "resume without uploading via "
                f"--finalize-revision {revision}"
            ) from exc
        _print_json(
            {
                **summary,
                **preflight,
                "revision": revision,
                "temporary_retrieval": retrieval,
                "pointer_directory": str(args.pointer_dir),
                "uploads_performed": True,
            }
        )
        return 0
    except CanonicalArtifactError as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
