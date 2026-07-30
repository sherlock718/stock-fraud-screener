# FAQ

## What is the product?

An explainable multi-factor stock screener that produces a ranked shortlist.
Fraud risk is one factor or safety gate alongside value, quality, momentum, and
growth.

## Which strategy is production?

The supported local baseline is the canonical US annual observed-only
three-year route:

```bash
python3 -m workflows.run_canonical
```

The command verifies the pinned P2 dataset, P3 row-complete OOS predictions,
and P4 shortlist artifact in dependency order. Use `--build-missing` only when
a canonical stage is absent; no existing artifact root is overwritten.

This is a research-to-screening baseline, not a claim that its historical
performance is certified or that its shortlist predicts future returns.

## Are the old backtest numbers current claims?

No. They are historical research results and are preserved only as such.

## Do we need new API credentials now?

No. Deterministic canonical verification/reconstruction is local and uses
pinned inputs. External US refresh and private artifact publication are later,
separately authorized sessions.

## Why not delete the old work?

Historical reports and artifacts may contain useful evidence. They are kept
out of the active workflow until the product works, then can be archived more
aggressively with dependency-aware cleanup. The current classifications and
proposed destinations are recorded in
`docs/CANONICAL_DEPENDENCY_INVENTORY.md` and
`docs/ARCHIVE_INVENTORY.md`; C1 does not move or delete anything.
