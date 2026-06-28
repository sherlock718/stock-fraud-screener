# CLAUDE.md — AI Assistant Instructions

Instructions for Claude Code when working in this repository.

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
3. `git add` + `git commit` with conventional message
4. `git push` to remote
5. HuggingFace push (`data_io/push_to_hf.py`) **if** `data/` or `models/` changed

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

## Session Execution Protocol (Sessions 35–41 only — remove after session 41)

Before executing any session work:
1. Read the **planned session entry** in `SESSION_PLAN.md` (under "Execution Phase")
2. Read the **backlog items** referenced in that entry from `docs/architecture/BACKLOG.md`
3. If the user's prompt diverges from the plan, **flag it and ask** before executing
4. Never invent new backlog items — work maps to existing Critical/Parked numbers
5. Handoff prompts must quote the SESSION_PLAN.md scope, not paraphrase or extend it

---

## Key Paths

- Dataset: `data/historical_dataset_clean.parquet`
- Models: `models/model_{1y,3y,5y}.joblib` + `models/model_meta.json`
- CI: `.github/workflows/refresh_data.yml`
- Tests: `pytest tests/` (pythonpath = `.`)
- Pre-commit: `quality/check_sync.py --warn-only` (architecture sync warnings)
