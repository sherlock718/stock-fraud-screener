# FAQ

## What is the product?

An explainable multi-factor stock screener that produces a ranked shortlist.
Fraud risk is one factor or safety gate alongside value, quality, momentum, and
growth.

## Which route is authoritative?

The supported local free-data V1 route is:

```bash
python3 -m workflows.run_us_free_v1
```

The command verifies P2 -> P3 -> P4 -> US1A -> US1B plus the named supporting
B1, M1, I1, and partial-attempt boundaries without external access. The US1C
candidate remains local and is not a release. `python3 -m
workflows.run_canonical` remains a narrower P2 -> P3 -> P4 baseline diagnostic.

This is a research-to-screening baseline, not a claim that its historical
performance is certified or that its shortlist predicts future returns.

## Are the old backtest numbers current claims?

No. They are historical research results and are preserved only as such.

## Do we need new API credentials now?

No. Deterministic verification is local and uses pinned inputs. D1 already
provides a separately gated US refresh, and the accepted P2-P4 baseline is
privately recoverable at one immutable Hugging Face revision. New collection,
publication, or promotion still requires separate authorization.

## Why not delete the old work?

Historical reports and artifacts contain useful evidence. Session A1 moved
only four dependency-safe partial/Session-9 payload groups into verified local
archive packages and retired their 11 tracked builders, tests, and reports to
the existing `codex/legacy-archive` recovery branch. V3.1-V3.3 and every
canonical, corrected-source, international, B1, M1, I1, and US1 boundary remain
in place because they are active, pinned, or still referenced. Exact hashes,
recovery commands, and exclusions are recorded in
`docs/CANONICAL_DEPENDENCY_INVENTORY.md` and
`docs/ARCHIVE_INVENTORY.md`.
