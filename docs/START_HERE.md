# Start Here

## The goal

Build one simple, workable stock-screening product that produces an
explainable ranked shortlist from current-enough company data.

The product is a multi-factor screener. Value, quality, momentum, growth, and
fraud risk may all contribute. Fraud detection is a safety input, not the
product itself.

## Where the project stands

The repository has useful data pipelines, models, factor code, portfolio code,
tests, and an existing production-screener notebook. It also accumulated
multiple strategies, historical roadmaps, and performance claims. We have not
yet confirmed which existing path still runs end to end or whether its inputs
are current enough for a usable result.

For that reason:

- there is no active performance claim;
- the audit-grade V3.4 market-ledger collection is paused;
- no paid market-data access is required for the next task;
- historical validation documents remain preserved under `docs/archive/` and
  `reports/pit_validation/`;
- one canonical product route will be chosen only after inspecting what
  actually works.

## What to do next

Run Product Session P1 from `docs/CODEX_ROADMAP.md`: a read-only readiness
check of the existing product path.

P1 answers only five questions:

1. What command or notebook is the closest existing product entrypoint?
2. Do its required datasets and models exist and load?
3. How current are its inputs?
4. Can it produce a ranked shortlist without external refreshes?
5. What single implementation task would make it usable?

P1 must not download data, retrain models, edit code, or calculate a new
backtest. Its purpose is to replace confusion with one evidence-backed next
step.

## Basic test command

```bash
python3 -m pytest tests/ -x -q
```

Tests demonstrate code behavior; they do not prove that data is current or
that historical returns will repeat.
