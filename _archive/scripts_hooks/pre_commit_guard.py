#!/usr/bin/env python3
"""
Pre-commit guardrail script.
Tracked at: scripts/hooks/pre_commit_guard.py
Called by: .git/hooks/pre-commit

Checks:
1. Warn if >5 files staged in one commit
2. Warn if pipeline/step* changes without corresponding tests/pipeline/ change
3. Block if data/*.parquet is staged
4. Warn if code dirs changed without atlas/docs update staged
"""
import subprocess
import sys


def get_staged_files():
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True, text=True
    )
    return [f for f in result.stdout.strip().split("\n") if f]


def check_file_count(files):
    if len(files) > 5:
        print(f"⚠️  WARNING: {len(files)} files staged (threshold: 5)")
        print("   Consider splitting into smaller commits.")
        return 1
    return 0


def check_pipeline_without_tests(files):
    pipeline_steps = [f for f in files if f.startswith("pipeline/step")]
    test_files = [f for f in files if f.startswith("tests/pipeline/")]
    if pipeline_steps and not test_files:
        print("⚠️  WARNING: pipeline/step* changed without tests/pipeline/ change")
        print(f"   Changed: {', '.join(pipeline_steps)}")
        return 1
    return 0


def check_parquet_staged(files):
    parquets = [f for f in files if f.startswith("data/") and f.endswith(".parquet")]
    if parquets:
        print("🚫 BLOCKED: data/*.parquet files should not be committed")
        print(f"   Staged: {', '.join(parquets)}")
        print("   These are generated outputs. Unstage with: git reset HEAD <file>")
        return 2
    return 0


def check_docs_consistency(files):
    code_dirs = ("pipeline/", "scripts/", "tests/", "notebooks/", ".github/workflows/")
    doc_files = ("PIPELINE_ATLAS.md", "PARQUET_ATLAS.md", "AI_EDIT_LOG.md", "KNOWN_ISSUES.md")

    has_code_change = any(f.startswith(d) for f in files for d in code_dirs)
    has_doc_update = any(f in doc_files for f in files)

    if has_code_change and not has_doc_update:
        print("⚠️  Atlas/docs may need update. Confirm whether PIPELINE_ATLAS.md,")
        print("   PARQUET_ATLAS.md, AI_EDIT_LOG.md, or KNOWN_ISSUES.md should be updated.")
        return 1
    return 0


def main():
    files = get_staged_files()
    if not files:
        return 0

    warnings = 0
    warnings += check_file_count(files)
    warnings += check_pipeline_without_tests(files)
    warnings += check_docs_consistency(files)

    block = check_parquet_staged(files)
    if block:
        sys.exit(1)

    if warnings:
        print("\n   Proceeding despite warnings. Use --no-verify to skip (not recommended).")

    return 0


if __name__ == "__main__":
    main()
