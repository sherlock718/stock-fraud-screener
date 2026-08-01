"""Verify the canonical free-data V1 route or perform bounded US1C actions."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_io.canonical_hf import (
    DEFAULT_POINTER_DIR,
    require_hf_token,
    retrieve_from_pointers,
)
from portfolio.us1c_release_consolidation import (
    DEFAULT_ARTIFACT_ROOT,
    build_release_candidate,
    recovery_evidence_from_result,
    verify_recovered_canonical_root,
    verify_release_candidate,
    verify_release_chain,
)


def _write_json_exclusive(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")


def _recover(target: Path, evidence_output: Path) -> dict[str, object]:
    if target.exists():
        raise FileExistsError(f"recovery target already exists: {target}")
    if evidence_output.exists():
        raise FileExistsError(
            f"recovery evidence target already exists: {evidence_output}"
        )
    from huggingface_hub import HfApi, hf_hub_download

    result = retrieve_from_pointers(
        pointer_dir=DEFAULT_POINTER_DIR,
        target=target,
        api=HfApi(),
        token=require_hf_token(),
        downloader=hf_hub_download,
    )
    recovered = verify_recovered_canonical_root(target)
    evidence = recovery_evidence_from_result(result, recovered)
    _write_json_exclusive(evidence_output, evidence)
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--us1c-artifact-root",
        type=Path,
        default=None,
        help="verify this US1C artifact; defaults to chain-only verification",
    )
    parser.add_argument("--expected-us1c-manifest-sha256")
    parser.add_argument("--build-us1c", action="store_true")
    parser.add_argument("--recovery-evidence", type=Path)
    parser.add_argument("--recover-canonical", action="store_true")
    parser.add_argument("--recovery-target", type=Path)
    parser.add_argument("--recovery-evidence-output", type=Path)
    args = parser.parse_args()

    if args.recover_canonical:
        if args.build_us1c or args.us1c_artifact_root:
            parser.error("--recover-canonical is a separate read-only operation")
        if not args.recovery_target or not args.recovery_evidence_output:
            parser.error(
                "--recover-canonical requires --recovery-target and "
                "--recovery-evidence-output"
            )
        result = _recover(
            args.recovery_target.resolve(),
            args.recovery_evidence_output.resolve(),
        )
    elif args.build_us1c:
        if args.expected_us1c_manifest_sha256:
            parser.error("expected manifest applies only to verification")
        if not args.recovery_evidence:
            parser.error("--build-us1c requires --recovery-evidence")
        target = (args.us1c_artifact_root or DEFAULT_ARTIFACT_ROOT).resolve()
        recovery = json.loads(args.recovery_evidence.read_text())
        manifest = build_release_candidate(target, recovery_evidence=recovery)
        result = verify_release_candidate(target)
        result["artifact_root"] = str(target)
        result["manifest_path"] = str(manifest)
    else:
        chain = verify_release_chain()
        if args.us1c_artifact_root:
            candidate = verify_release_candidate(
                args.us1c_artifact_root,
                expected_manifest_sha256=args.expected_us1c_manifest_sha256,
            )
            result = {"chain": chain, "release_candidate": candidate}
        else:
            if args.expected_us1c_manifest_sha256:
                parser.error(
                    "--expected-us1c-manifest-sha256 requires "
                    "--us1c-artifact-root"
                )
            result = chain
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
