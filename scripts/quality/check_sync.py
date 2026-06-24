#!/usr/bin/env python3
"""
check_sync.py — Architecture sync checker.

Reads git-staged (or specified) files and applies the CLAUDE.md Change Checklist
rules. Reports which required doc/diagram updates are missing from the staged set.

Usage:
    python3 scripts/check_sync.py                 # checks currently staged files
    python3 scripts/check_sync.py --all-changed   # checks all uncommitted changes
    python3 scripts/check_sync.py --files a.py b.py  # check specific files
    python3 scripts/check_sync.py --warn-only     # exit 0 even if violations found
"""

from __future__ import annotations
import argparse
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


# ---------------------------------------------------------------------------
# Rule definitions — mirrors CLAUDE.md Change Checklist exactly
# ---------------------------------------------------------------------------

@dataclass
class Rule:
    name: str
    trigger_desc: str           # human-readable description of the trigger
    required_files: list[str]   # files that MUST also be staged/changed
    message: str                # what to do


def _starts_with_any(files: set[str], prefixes: tuple[str, ...]) -> bool:
    return any(f.startswith(p) for f in files for p in prefixes)


def _any_in(files: set[str], targets: list[str]) -> bool:
    return any(t in files for t in targets)


RULES: list[Rule] = [
    Rule(
        name="scripts-doc",
        trigger_desc="New or modified file under scripts/",
        required_files=["docs/developer/scripts.md"],
        message="Add a section (or update the flags table) for every touched script.",
    ),
    Rule(
        name="feature-count",
        trigger_desc="pipeline/feature_library.py changed (column count may have changed)",
        required_files=[
            "docs/architecture.md",
            "docs/methodology/models.md",
            "docs/methodology/features.md",
            "docs/index.md",
            "docs/developer/data-update-guide.md",
            "docs/developer/phase-done-criteria.md",
            "README.md",
        ],
        message="Update column count in all Mermaid nodes and every doc that references feature/column counts.",
    ),
    Rule(
        name="step5-columns",
        trigger_desc="pipeline/step5_compute_features.py changed (new columns may have been added)",
        required_files=[
            "docs/architecture.md",
            "docs/developer/data-update-guide.md",
            "docs/developer/pipeline-scripts.md",
            "docs/methodology/features.md",
        ],
        message="Update column counts in architecture.md Data Flow diagram + data-update-guide.md lineage table.",
    ),
    Rule(
        name="quality-check-count",
        trigger_desc="scripts/test_dataset_quality.py changed (check count may have changed)",
        required_files=[
            "docs/developer/data-update-guide.md",
            "docs/developer/phase-done-criteria.md",
            "docs/developer/scripts.md",
        ],
        message="Update check count (e.g. '98 checks') in data-update-guide.md, phase-done-criteria.md, and scripts.md.",
    ),
    Rule(
        name="feature-selection-counts",
        trigger_desc="scripts/run_feature_selection.py or models/feature_sets_*.json changed",
        required_files=[
            "docs/index.md",
            "docs/developer/scripts.md",
            "docs/methodology/feature-selection.md",
        ],
        message="Update 45/45/41 feature counts in index.md, scripts.md, and feature-selection.md.",
    ),
    Rule(
        name="ci-workflow",
        trigger_desc=".github/workflows/refresh_data.yml changed",
        required_files=["docs/developer/data-update-guide.md"],
        message="Update the Mermaid operator diagram in data-update-guide.md to match new CI steps.",
    ),
    Rule(
        name="ml-pipeline",
        trigger_desc="ML training or tuning script changed",
        required_files=["docs/methodology/models.md", "README.md", "docs/index.md"],
        message="Update AUC table (Val/Test/WF AUC) and Mermaid flowchart if pipeline structure changed.",
    ),
    Rule(
        name="pipeline-steps",
        trigger_desc="Pipeline step scripts (run_pipeline.py, enrich_quarterly_features.py, mark_survivorship.py) changed",
        required_files=["docs/architecture.md", "docs/developer/data-update-guide.md"],
        message="Update Data Pipeline subgraph and Data Flow Detail diagram in docs/architecture.md.",
    ),
    Rule(
        name="changelog",
        trigger_desc="Any source change (all non-docs files)",
        required_files=["CHANGELOG.md"],
        message="Add entry under [Unreleased] in CHANGELOG.md with the script/file name bolded.",
    ),
]


def get_staged_files() -> set[str]:
    out = subprocess.check_output(
        ["git", "diff", "--name-only", "--cached"], text=True
    )
    return {f.strip() for f in out.splitlines() if f.strip()}


def get_all_changed_files() -> set[str]:
    staged = subprocess.check_output(
        ["git", "diff", "--name-only", "--cached"], text=True
    )
    unstaged = subprocess.check_output(
        ["git", "diff", "--name-only"], text=True
    )
    untracked = subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard"], text=True
    )
    return {
        f.strip()
        for src in [staged, unstaged, untracked]
        for f in src.splitlines()
        if f.strip()
    }


def triggered_rules(changed: set[str]) -> list[tuple[Rule, list[str]]]:
    """Return (rule, missing_files) pairs for every triggered rule with gaps."""
    docs_and_config = {
        "docs/architecture.md", "docs/methodology/models.md",
        "docs/methodology/features.md", "docs/methodology/pipeline.md",
        "docs/developer/scripts.md", "docs/developer/setup.md",
        "docs/guide/app.md", "docs/index.md", "docs/markets.md",
        "CHANGELOG.md", "CLAUDE.md", "CONTRIBUTING.md",
        "README.md",
    }
    # Files that are purely doc/config (never trigger rules themselves)
    source_changes = {f for f in changed if f not in docs_and_config}

    results: list[tuple[Rule, list[str]]] = []
    for rule in RULES:
        # Determine if this rule is triggered
        triggered = False
        if rule.name == "scripts-doc":
            triggered = _starts_with_any(changed, ("scripts/",))
        elif rule.name == "feature-count":
            triggered = "pipeline/feature_library.py" in changed
        elif rule.name == "step5-columns":
            triggered = "pipeline/step5_compute_features.py" in changed
        elif rule.name == "quality-check-count":
            triggered = "scripts/test_dataset_quality.py" in changed
        elif rule.name == "feature-selection-counts":
            triggered = _any_in(changed, ["scripts/run_feature_selection.py"]) or \
                        any(f.startswith("models/feature_sets_") for f in changed)
        elif rule.name == "ci-workflow":
            triggered = ".github/workflows/refresh_data.yml" in changed
        elif rule.name == "ml-pipeline":
            triggered = _any_in(changed, ["scripts/train_models.py", "scripts/tune_models.py"])
        elif rule.name == "pipeline-steps":
            triggered = _any_in(changed, [
                "scripts/run_pipeline.py",
                "scripts/enrich_quarterly_features.py",
                "scripts/mark_survivorship.py",
                "pipeline/step1_fetch_tickers.py",
                "pipeline/step2_build_snapshots.py",
                "pipeline/step3_enrich_prices.py",
                "pipeline/step4_enrich_macro.py",
                "pipeline/step5_compute_features.py",
                "pipeline/step6_clean_dataset.py",
            ])
        elif rule.name == "changelog":
            # Triggered by any non-doc source change
            triggered = bool(source_changes)

        if not triggered:
            continue

        missing = [f for f in rule.required_files if f not in changed]
        if missing:
            results.append((rule, missing))

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Check architecture sync before commit.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all-changed", action="store_true",
                       help="Check all uncommitted files (staged + unstaged + untracked)")
    group.add_argument("--files", nargs="+", metavar="FILE",
                       help="Check a specific list of files")
    parser.add_argument("--warn-only", action="store_true",
                        help="Print warnings but always exit 0 (useful for non-blocking mode)")
    args = parser.parse_args()

    if args.files:
        changed = set(args.files)
    elif args.all_changed:
        changed = get_all_changed_files()
    else:
        changed = get_staged_files()

    if not changed:
        print("check_sync: no changed files detected — nothing to check.")
        sys.exit(0)

    violations = triggered_rules(changed)

    if not violations:
        print("check_sync: ✅  All sync rules satisfied.")
        sys.exit(0)

    print("check_sync: ❌  Sync violations found — the following docs must be updated:\n")
    for rule, missing in violations:
        print(f"  [{rule.name}] Triggered by: {rule.trigger_desc}")
        for f in missing:
            print(f"    ⚠  {f} not staged")
        print(f"    → {rule.message}")
        print()

    print("Stage the required doc updates and re-run, or use --warn-only to skip blocking.")
    print("See CLAUDE.md → Change Checklist for the full rules.\n")

    if args.warn_only:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
