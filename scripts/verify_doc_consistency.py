#!/usr/bin/env python3
"""
verify_doc_consistency.py — cross-file fact consistency checker.

Reads the parquet and key docs, then verifies that column counts, check counts,
feature counts, and other key numbers are consistent everywhere they appear.

Usage:
    python3 scripts/verify_doc_consistency.py          # fail on any mismatch
    python3 scripts/verify_doc_consistency.py --warn   # print mismatches, exit 0

Run before Phase gate checks or in CI after dataset changes.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

BASE = Path(__file__).parent.parent


def _read(path: str) -> str:
    p = BASE / path
    return p.read_text() if p.exists() else ""


def check(failures: list[str], label: str, found: str | int, expected: str | int) -> None:
    if str(found) != str(expected):
        failures.append(f"  FAIL  {label}: found '{found}', expected '{expected}'")
    else:
        print(f"  PASS  {label}")


def get_parquet_shape() -> tuple[int, int]:
    import pandas as pd
    p = BASE / "data" / "historical_dataset_clean.parquet"
    if not p.exists():
        return (0, 0)
    df = pd.read_parquet(p)
    return df.shape


def get_quality_check_count() -> int:
    """Read the authoritative check count from scripts.md (updated manually when test script changes)."""
    src = _read("docs/developer/scripts.md")
    m = re.search(r"(\d+) checks", src)
    return int(m.group(1)) if m else 0


def get_feature_set_counts() -> dict[str, int]:
    import json
    counts = {}
    for h in ["1y", "3y", "5y"]:
        p = BASE / "models" / f"feature_sets_{h}.json"
        if p.exists():
            data = json.loads(p.read_text())
            counts[h] = data.get("n", len(data.get("features", [])))
        else:
            counts[h] = 0
    return counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warn", action="store_true", help="Exit 0 even if failures found")
    args = parser.parse_args()

    failures: list[str] = []

    # ── Ground truth from live artefacts ──────────────────────────────────────
    rows, cols = get_parquet_shape()
    n_checks    = get_quality_check_count()
    fs          = get_feature_set_counts()
    feat_str    = f"{fs['1y']}/{fs['3y']}/{fs['5y']}"

    print(f"\nGround truth: {rows:,}×{cols} parquet | {n_checks} quality checks | {feat_str} features\n")

    # ── Check every doc ────────────────────────────────────────────────────────

    # docs/index.md
    idx = _read("docs/index.md")
    check(failures, "docs/index.md: column count", re.search(r"(\d+) columns", idx).group(1) if re.search(r"(\d+) columns", idx) else "missing", str(cols))
    check(failures, "docs/index.md: feature counts", re.search(r"(\d+/\d+/\d+) features", idx).group(1) if re.search(r"(\d+/\d+/\d+) features", idx) else "missing", feat_str)
    check(failures, "docs/index.md: row count", re.search(r"([\d,]+) company-year", idx).group(1).replace(",","") if re.search(r"([\d,]+) company-year", idx) else "missing", str(rows))

    # README.md
    readme = _read("README.md")
    check(failures, "README.md: column count", re.search(r"(\d+) columns", readme).group(1) if re.search(r"(\d+) columns", readme) else "missing", str(cols))

    # docs/architecture.md
    arch = _read("docs/architecture.md")
    col_hits = re.findall(r"(\d+) columns", arch)
    for hit in col_hits:
        check(failures, f"docs/architecture.md: column count mention ({hit})", hit, str(cols))

    # docs/methodology/models.md
    models = _read("docs/methodology/models.md")
    check(failures, "docs/methodology/models.md: column count", re.search(r"(\d+) features", models).group(1) if re.search(r"(\d+) features", models) else "missing", str(cols))

    # CLAUDE.md
    claude = _read("CLAUDE.md")
    check(failures, "CLAUDE.md: column count", re.search(r"(\d+) cols", claude).group(1) if re.search(r"(\d+) cols", claude) else "missing", str(cols))

    # docs/developer/phase-done-criteria.md
    crit = _read("docs/developer/phase-done-criteria.md")
    check(failures, "phase-done-criteria.md: column count assertion", re.search(r"shape\[1\] == (\d+)", crit).group(1) if re.search(r"shape\[1\] == (\d+)", crit) else "missing", str(cols))
    feat_match = re.search(r"(\d+)/(\d+)/(\d+) features", crit)
    if feat_match:
        check(failures, "phase-done-criteria.md: feature counts", feat_match.group(0).split(" ")[0], feat_str)

    # docs/developer/data-update-guide.md
    guide = _read("docs/developer/data-update-guide.md")
    check(failures, "data-update-guide.md: final column count", re.search(r"(\d+) columns\b", guide.split("Current production")[-1]).group(1) if "Current production" in guide and re.search(r"(\d+) columns\b", guide.split("Current production")[-1]) else "missing", str(cols))
    check(failures, "data-update-guide.md: quality check count (diagram node)", re.search(r"(\d+) checks must pass", guide).group(1) if re.search(r"(\d+) checks must pass", guide) else "missing", str(n_checks))

    # docs/developer/scripts.md
    scripts_doc = _read("docs/developer/scripts.md")
    feat_match2 = re.search(r"(\d+)/(\d+)/(\d+) features", scripts_doc)
    if feat_match2:
        check(failures, "scripts.md: feature counts", feat_match2.group(0).split(" ")[0], feat_str)

    # docs/methodology/feature-selection.md
    fs_doc = _read("docs/methodology/feature-selection.md")
    feat_match3 = re.search(r"(\d+/\d+/\d+) features", fs_doc)
    if feat_match3:
        check(failures, "feature-selection.md: feature counts", feat_match3.group(1), feat_str)

    # ── Summary ────────────────────────────────────────────────────────────────
    print()
    if failures:
        print(f"CONSISTENCY FAILURES ({len(failures)}):\n")
        for f in failures:
            print(f)
        print()
        if args.warn:
            sys.exit(0)
        sys.exit(1)
    else:
        print(f"All consistency checks passed.")
        sys.exit(0)


if __name__ == "__main__":
    main()
