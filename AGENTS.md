# AGENTS.md — AI Assistant Instructions

Instructions for Codex when working in this repository.

**Project framing**: This is a **multi-factor stock screener and alpha generation platform**,
not a fraud screener. Fraud risk is one of five factors (Value · Quality · Momentum · Growth · Fraud Risk).

---

## Project Structure (Architecture V2)

```
pipeline/       Data pipeline steps (step1–step6, feature_library, enrichments)
modeling/       ML training, tuning, scoring, feature selection
alpha/          5-factor alpha score package (value, quality, momentum, growth, fraud_risk)
research/       Factor research, IC engine, feature selection engine, reports
fraud/          Fraud-specific rules, features, taxonomy
quality/        Data quality checks, bias audit, PIT validation, sync checker
backtest/       Walk-forward backtester engine
portfolio/      Alpha registry, portfolio construction, screener
data_io/        HuggingFace push/pull, AAER labels, merge snapshots
workflows/      Orchestration scripts (refresh pipelines per market)
tests/          Unit + integration tests
notebooks/      Experiment hub, EDA notebooks
data/           Parquet datasets (gitignored except metadata)
models/         Trained model artifacts (.joblib, meta.json)
```

---

## Change Checklist — Required Before Every Commit

| What changed | Must also update |
|---|---|
| New growth/YoY feature added | `pipeline/step5_compute_features.py` — add to `ratio_cols` winsorize list |
| New ML-derived score column added | `modeling/train.py` — add to `EXCLUDE` set |
| New post-processing step | Add to `refresh_data.yml` CI workflow |
| New factor added to `alpha/` | Add module in `alpha/factors/`, update `alpha/factors/__init__.py` |
| Factor weight changes | `alpha/factors/composite.py` — update `DEFAULT_WEIGHTS` |
| Any of the above | `CHANGELOG.md` — add entry under `[Unreleased]` |
| All changes | Commit with conventional message: `feat(scope):`, `fix(scope):`, `docs:`, etc. |

---

## "Done" Definition

A task is done only when ALL of the following are complete:

1. Code written and tested (pytest passes)
2. `CHANGELOG.md` entry added under `[Unreleased]`
3. Review the diff and report generated artifacts
4. Commit only when the user explicitly requests a commit
5. Push Git or HuggingFace artifacts only when the user explicitly requests it

---

## Commit Convention

```
feat(scope):     new feature
fix(scope):      bug fix
perf(scope):     performance improvement
refactor(scope): restructure, no behavior change
docs(scope):     documentation only
chore(scope):    build / tooling / CI changes
test(scope):     tests only
```

Scope examples: `pipeline`, `modeling`, `alpha`, `fraud`, `quality`, `research`, `backtest`, `portfolio`, `ci`

---

## Codex Low-Burn Workflow

Follow `docs/CODEX_WORKFLOW.md` for every Codex task in this repository.

Default behavior:

1. Read `docs/START_HERE.md`, `docs/CODEX_HANDOFF.md`, the current session in `docs/CODEX_ROADMAP.md`, and only the files directly relevant to the requested module.
2. Use one bounded objective per task. Do not turn a focused request into a whole-repository audit.
3. Start with the cheapest reliable approach: bounded `rg`, narrow file reads, quiet commands, and scoped tests.
4. Do not use subagents, MCP servers, connectors, web search, or external data unless the task explicitly requires them.
5. Do not paste large logs into the conversation. Save raw output to a temporary file when needed and summarize the evidence.
6. Run focused tests during implementation. Run the full suite once at the final verification boundary when justified.
7. Do not update market data, rebuild large datasets, retrain models, or overwrite artifacts without stating the expected scope first.
8. For PIT logic, target availability, data transformations, model splits, backtests, or performance claims: perform a read-only evidence pass before editing and prefer deeper reasoning.
9. For documentation, manifests, mechanical edits, and already-specified fixes: prefer lightweight reasoning.
10. At a module boundary, update `docs/CODEX_HANDOFF.md` with confirmed facts, files changed, verification, artifacts, unresolved questions, and the exact next task.

If context becomes crowded, finish the current bounded step and update the handoff instead of continuing broad exploration.

---

## Key Paths

- Dataset: `data/historical_dataset_clean.parquet`
- Models: `models/model_{1y,3y,5y}.joblib` + `models/model_meta.json`
- CI: `.github/workflows/refresh_data.yml`
- Tests: `pytest tests/` (pythonpath = `.`)
- Pre-commit: `quality/check_sync.py --warn-only` (architecture sync warnings)
