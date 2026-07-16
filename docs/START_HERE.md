# Start Here

## The goal

Build one simple, workable research-to-screening product:

1. preserve freely available ticker, financial, market, and macro source data;
2. produce a cleaned longitudinal point-in-time feature dataset;
3. perform leakage-safe feature analysis and selection;
4. train interpretable and sophisticated models;
5. backtest out-of-sample predictions with survivorship limitations made
   explicit;
6. apply small-cap and liquidity gates, construct portfolios, and report an
   explainable ranked shortlist.

Value, quality, momentum, growth, and fraud risk may all contribute. Fraud risk
is a factor or safety gate, not the product itself.

## Where the project stands

Most required layers already exist: source Parquets on Hugging Face, a
six-stage pipeline, a 367-column cleaned dataset, an HTML feature dictionary,
temporal feature selection, multiple model families, backtesting, liquidity
gates, portfolio construction, and extensive tests.

Substantial PIT and survivorship corrections also exist. The unresolved problem
is consolidation: older main-path outputs and later corrected artifacts have
not been reduced to one clearly supported dataset-to-product route.

Therefore:

- there is no active historical-performance claim;
- the V3.4 external market-ledger collection is paused;
- no paid data access or external request is required for the next task;
- nothing should be archived until its dependencies and replacement are known;
- existing code and data should be consolidated, not rebuilt from scratch.

## What to do next

Run Product Session P1 from `docs/CODEX_ROADMAP.md`: a read-only
canonicalization decision.

P1 maps the bronze data, pipeline, PIT corrections, dataset candidates,
feature-selection and modeling path, backtest, liquidity/portfolio code, and
reporting into one proposed canonical route. It identifies conflicts and future
archive candidates, then selects exactly one bounded implementation task.

P1 must not download data, call external services, edit or archive files,
retrain models, calculate performance, or resume V3.4.

## Basic test command

```bash
python3 -m pytest tests/ -x -q
```

Tests demonstrate code behavior; they do not establish data freshness,
survivorship-free coverage, or future performance.
