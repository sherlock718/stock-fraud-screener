#!/usr/bin/env python3
"""
run_dataset_enrichments.py — Post-Step6 enrichment orchestrator.

Runs the canonical Phase B enrichment sequence on historical_dataset_clean.parquet.
Stops immediately if any step fails.

This script owns the mutation order for base dataset enrichment (Phase B).
It does NOT include Phase C scripts (OOF scoring, ML scoring, alpha, patches).

Usage:
    python3 scripts/workflows/run_dataset_enrichments.py                # run all steps
    python3 scripts/workflows/run_dataset_enrichments.py --dry-run      # print commands only
    python3 scripts/workflows/run_dataset_enrichments.py --apply-universe-filters  # full investable universe
    python3 scripts/workflows/run_dataset_enrichments.py --skip-survivorship       # skip mark_survivorship
    python3 scripts/workflows/run_dataset_enrichments.py --skip-quarterly          # skip enrich_quarterly
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from scripts._root import ROOT

BASE = ROOT

STEPS = [
    {
        "name": "Fix dataset quality",
        "cmd": ["scripts/enrichments/fix_dataset_quality.py"],
        "skip_flag": None,
    },
    {
        "name": "Universe definition (p0f)",
        "cmd": ["pipeline/p0f_universe_definition.py"],
        "extra_if_universe_filters": ["--apply-filters"],
        "skip_flag": None,
    },
    {
        "name": "Confidence score (p0g)",
        "cmd": ["pipeline/p0g_confidence_score.py"],
        "skip_flag": None,
    },
    {
        "name": "Survivorship correction",
        "cmd": ["scripts/enrichments/mark_survivorship.py", "--fix"],
        "skip_flag": "skip_survivorship",
    },
    {
        "name": "Quarterly feature enrichment",
        "cmd": ["scripts/enrichments/enrich_quarterly_features.py", "--fix"],
        "skip_flag": "skip_quarterly",
    },
    {
        "name": "Feature imputation",
        "cmd": ["scripts/enrichments/impute_features.py"],
        "skip_flag": None,
    },
    {
        "name": "Fraud labels",
        "cmd": ["pipeline/enrich_fraud_labels.py"],
        "skip_flag": None,
    },
    {
        "name": "Fraud taxonomy",
        "cmd": ["pipeline/enrich_fraud_taxonomy.py"],
        "skip_flag": None,
    },
    {
        "name": "Validate feature contract (Phase B gate)",
        "cmd": ["scripts/quality/validate_feature_contract.py"],
        "skip_flag": None,
    },
]


def run(args: argparse.Namespace) -> int:
    print(f"\n{'='*60}")
    print(f"  Post-Step6 Enrichment Orchestrator")
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'EXECUTE'}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    t0 = time.time()
    executed = 0

    for i, step in enumerate(STEPS):
        skip_flag = step.get("skip_flag")
        if skip_flag and getattr(args, skip_flag, False):
            print(f"  [{i}] SKIP — {step['name']}")
            continue

        cmd = [sys.executable, str(BASE / step["cmd"][0])] + step["cmd"][1:]
        if step.get("extra_if_universe_filters") and args.apply_universe_filters:
            cmd += step["extra_if_universe_filters"]

        print(f"  [{i}] {step['name']}")
        print(f"      $ {' '.join(cmd)}")

        if args.dry_run:
            continue

        result = subprocess.run(cmd, cwd=str(BASE))
        if result.returncode != 0:
            print(f"\n  FAILED at step {i} ({step['name']}) — exit code {result.returncode}")
            return result.returncode

        executed += 1
        print()

    elapsed = time.time() - t0
    if args.dry_run:
        print(f"\n  Dry run complete — {len(STEPS)} steps listed, 0 executed.")
    else:
        print(f"\n  All {executed} steps complete in {elapsed:.1f}s.")

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Post-Step6 enrichment orchestrator (Phase B)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing")
    parser.add_argument("--apply-universe-filters", action="store_true",
                        help="Pass --apply-filters to p0f (full investable universe)")
    parser.add_argument("--skip-survivorship", action="store_true",
                        help="Skip mark_survivorship step")
    parser.add_argument("--skip-quarterly", action="store_true",
                        help="Skip enrich_quarterly_features step")
    args = parser.parse_args()

    sys.exit(run(args))


if __name__ == "__main__":
    main()
