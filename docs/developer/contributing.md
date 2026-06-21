# Contributing Guide

This document defines how to contribute to the Multi-Factor Stock Screener project — what "done" means, how to keep everything in sync, and what to check before touching any file.

---

## Pre-Task Vision Checklist

Run this before starting **any** task. If the answer to any question is "no", stop and revise the plan.

| # | Question | Wrong answer |
|---|---|---|
| 1 | Does this align with the Renaissance-style ML-first approach? | "I'm adding a fixed-weight composite score" |
| 2 | Does this introduce fixed factor weights that should instead be ML-learned? | "Yes, I'm hardcoding a 20%/30%/50% split across factor groups" |
| 3 | Does this move toward many alpha signals vs one composite score? | "This reduces alpha signals to a single scored output" |
| 4 | Are docs, code, and diagrams staying in sync? | "I'll update docs later" |
| 5 | Is this a blocker for or dependency of alpha signal generation? | Skipping a blocker in favour of a nice-to-have |

The core vision: **ML discovers which features matter per market, horizon, and segment. Hundreds of alpha signals. Each independently backtested. Portfolio built from validated signals only.** See [CLAUDE.md](../../CLAUDE.md) and the session memory file for full architecture detail.

---

## "Done" Definition

A task is done only when **all** of the following are complete:

1. ✅ Code written and manually tested
2. ✅ All rows in the `CLAUDE.md` Change Checklist satisfied
3. ✅ `CHANGELOG.md` entry added under `[Unreleased]`
4. ✅ `git add` → `git commit` with conventional message
5. ✅ `git push` to remote
6. ✅ HuggingFace push (`scripts/push_to_hf.py`) **if** `data/` or `models/` changed

If you stop before step 6, the task is **in progress**, not done. Update `ROADMAP.md` accordingly.

---

## Sync Rules

### When you add a Python script (`scripts/` or `pipeline/`)

1. Add a section to the appropriate doc: `docs/developer/scripts.md` (for `scripts/`) or `docs/developer/pipeline-scripts.md` (for `pipeline/`).
2. Section must include: one-line description, bash usage with all flags, output files produced.
3. If the script adds a pipeline step, add a node to `docs/architecture.md` Data Pipeline subgraph.
4. Add a `CHANGELOG.md` entry.

### When you add or rename dataset columns

1. Update the column count in every Mermaid diagram that shows it:
   - `docs/architecture.md` — High-Level Overview (parquet node) + Data Flow Detail
   - `docs/methodology/models.md` — Training Pipeline flowchart (top node)
   - `docs/index.md` — tagline
2. If the column is a feature, add it to `docs/methodology/features.md` under the correct factor group.
3. If the column is an ML output (`ml_1y`, `ml_3y`, etc.), update `docs/methodology/models.md`.

### When you change model performance (AUC, Sharpe, etc.)

1. Update `docs/methodology/models.md` AUC table — Val AUC, Test AUC, WF Mean AUC, target met.
2. Update `docs/index.md` Performance at a Glance table.
3. If training pipeline structure changed, update the Mermaid flowchart in models.md.

### When you add a factor group feature or change feature selection

1. Update `docs/methodology/factor-library.md` — formula, data source, coverage.
2. Update `docs/methodology/feature-selection.md` if the selection method changed.
3. Update `docs/methodology/features.md` if a feature moved factor groups.

### When you add an alpha signal schema or registry

1. Document the schema in `docs/methodology/models.md` or a new `docs/methodology/alpha-signals.md`.
2. Update `ROADMAP.md` Phase 1 checkboxes.

---

## Commit Convention

```
feat(scope):     new feature
fix(scope):      bug fix
perf(scope):     performance improvement
refactor(scope): restructure without behaviour change
docs(scope):     documentation only
chore(scope):    build / tooling / CI
test(scope):     tests only
```

**Scope examples**: `quarterly`, `api`, `ui`, `models`, `db`, `pipeline`, `docs`, `alpha`, `momentum`, `factors`

Always include the affected file name in the commit body:

```
feat(momentum): add cross-sectional 12m-1m momentum rank

Added momentum_12m1m to pipeline/feature_library.py.
Updates docs/methodology/features.md momentum section.
Fixes Phase 0 blocker P0.1.
```

---

## Context Continuity

Every session:

1. **Read** `CONTEXT.md` — current phase, what's done, what's blocked.
2. **Read** `ROADMAP.md` — phase checkboxes.
3. **Run the pre-task checklist** before starting any work.
4. **Update `CONTEXT.md`** at the end of the session (session log entry + remaining task list).

Never burn session tokens re-deriving project state from scratch. The files exist to prevent this.

---

## Architecture Constraints

These are fixed decisions. Do not reverse them without explicit user discussion.

| Decision | Rule |
|---|---|
| ML-first | Factor groups are input categories. Fixed-weight composites are forbidden. |
| Monolith research pipeline | Layers 1–13 share a parquet and run as batch processes. No microservices for research steps. |
| FastAPI + React as separate services | Only the API and frontend are separate from the pipeline. |
| HuggingFace for data | All parquet files and models pushed via `scripts/push_to_hf.py` after training. |
| Feature selection before modeling | PSI → IC → ICIR → Spearman dedup is always run before LightGBM. No shortcuts. |
| Per-alpha backtesting | Every alpha signal must have an independent backtest before entering the portfolio. No unvalidated signals in production. |

---

## File Organisation

```
scripts/          Run-once or orchestration scripts (train, backtest, push, etc.)
pipeline/         Step modules (step1–step6 per market, feature_library, enrichment)
alpha/            Alpha signal schema, registry, engine (Phase 1+)
notebooks/        Research exploration (IC analysis, regime analysis, portfolio construction)
docs/             All documentation (auto-built with MkDocs)
data/             Parquet files + alpha_registry.json
models/           Joblib models + model_meta.json
reports/          Per-alpha backtest JSONs + tearsheets
infra/            DB schema + Docker
.github/          CI/CD workflows
CLAUDE.md         Architecture state + pre-task checklist (read every session)
CONTEXT.md        Session state snapshot (update every session)
ROADMAP.md        Phase tracker with checkboxes (authoritative task list)
CHANGELOG.md      Commit history in Keep a Changelog format
mkdocs.yml        Docs site config
```

---

## Adding New Markets

When integrating a new market (EU, KR, JP, CA, BR):

1. Confirm `step1_fetch_tickers_{market}.py` and `step2_build_snapshots_{market}.py` exist.
2. Run step3 price enrichment → step4 macro → step5 features → step6 clean.
3. Apply universe definition (`pipeline/p0f_universe_definition.py`).
4. Merge into `data/historical_dataset_clean.parquet` via the appropriate `pipeline/phase_a_integrate_{market}.py` script.
5. Verify column schema alignment (319 columns, same dtype).
6. Update `CONTEXT.md` Data Coverage table.
7. Update `docs/architecture.md` Multi-Market section.

---

## Phase-Gate Reviews

After completing each phase (0 → 1 → 2 → 3):

1. Run the phase exit criteria checklist from `ROADMAP.md`.
2. Show the user sample outputs (model metrics, alpha registry stats, backtest tearsheet, portfolio stats).
3. Ask alignment questions: "Does this match your vision? Any features behaving unexpectedly?"
4. Get explicit approval before moving to the next phase.

This prevents silent drift — the same problem that caused the fraud-screener framing to persist for multiple sessions.
