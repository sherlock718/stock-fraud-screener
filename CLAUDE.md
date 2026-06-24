# CLAUDE.md — AI Assistant Instructions

Instructions for Claude Code when working in this repository. These rules enforce
architectural sync so that code, documentation, and diagrams always stay consistent.

**Project framing**: This is a **multi-factor stock screener and alpha generation platform**,
not a fraud screener. Fraud risk is one of five factors (Value · Quality · Momentum · Growth · Fraud Risk).

---

## Phase Scope Definition (LOCKED)

| Phase | Contains | Does NOT contain |
|---|---|---|
| **Phase A** | Dataset · EDA / data quality · Update schedule · Update diagram | Model training, backtest, bias audit, alpha generation |
| **Phase B** | Feature library · Feature engineering · Feature selection · Factor research · Research notebooks | Model training, walk-forward AUC, backtest, Phase C items |
| **Phase C** | Model training · Tuning · Industry-grade backtest · Look-ahead bias audit · Alpha generation schema | Phase A/B items |

**RULE — No re-auditing**: Do not re-audit a phase. Run `docs/developer/phase-done-criteria.md`.
If all checks pass → phase is done. If a check fails → fix that item only.

**RULE — Phase scope is locked**: Phase C items (model retraining, walk-forward AUC, backtest,
look-ahead bias, alpha generation) NEVER appear in Phase A/B task lists, gap reports, or audits.

**RULE — "Done" requires the checklist**: A phase is not done because tasks were completed in
a prior session. It is done when every check in `docs/developer/phase-done-criteria.md` passes.

---

## Change Checklist — Required Before Every Commit

Work through this matrix for every change before staging files.

| What changed | Must also update |
|---|---|
| New script in `scripts/` | `docs/developer/scripts.md` — add section with usage, flags table, outputs |
| New script in `pipeline/` | `docs/developer/pipeline-scripts.md` — add section for that module |
| New column added | `docs/developer/pipeline-integrity.md` Rule 1 check — is it in step3 or step5? |
| New growth/YoY feature added | `pipeline/step5_compute_features.py` — add to `ratio_cols` winsorize list (Rule 6) |
| New ML-derived score column added | `scripts/modeling/train_models.py` — add to `EXCLUDE` set before running feature selection (Rule 7) |
| New cross-sectional rank feature | `docs/developer/pipeline-integrity.md` Rule 2 check — does groupby include fiscal_year? |
| New post-processing script | Add to `refresh_data.yml` AND update `docs/developer/data-update-guide.md` diagram |
| Modified script CLI flags | `docs/developer/scripts.md` or `pipeline-scripts.md` — update flags table |
| New pipeline step or data column | `docs/architecture.md` — Component Map row + Data Flow diagram + column count in all flowchart labels |
| Column count changes in dataset | `docs/architecture.md` High-Level diagram + Data Flow diagram + `docs/methodology/models.md` flowchart label + `docs/index.md` tagline |
| New factor added to alpha/ package | `docs/methodology/factor-library.md` — factor spec table (formula, IC target, data source) |
| Factor IC or weight changes | `docs/methodology/factor-library.md` — IC table + composite weight row |
| Features regrouped or renamed | `docs/methodology/features.md` — update the factor group table for that category |
| Feature selection method changes | `docs/methodology/feature-selection.md` — update the relevant pipeline step |
| Model performance changes (AUC, etc.) | `docs/methodology/models.md` — AUC table (Val AUC, Test AUC, WF Mean AUC, target flag) + `docs/index.md` Performance at a Glance |
| ML pipeline structural change | `docs/methodology/models.md` Mermaid flowchart + `docs/architecture.md` ML System subgraph |
| New system component (DB, API, queue) | `docs/architecture.md` — add node to High-Level Overview + row to Component Map |
| New API endpoint | Docstring in route file; if user-facing add entry to `docs/developer/` |
| New UI tab or feature in Experiment Notebook | `notebooks/08_experiment_hub.ipynb` |
| Any of the above | `CHANGELOG.md` — add entry under `[Unreleased]` with the script/file name bolded |
| All changes | Commit with conventional message: `feat(scope):`, `fix(scope):`, `docs:`, `perf:`, etc. |

---

## Architecture Sync Rules

### Adding a script (scripts/ or pipeline/)
1. New section in the appropriate scripts doc: description, bash usage examples, flags table, output files.
2. If the script adds a pipeline step, add a node to `docs/architecture.md` Data Pipeline subgraph.
3. If it changes data flow, update the Data Flow Detail diagram.

### Adding or removing dataset columns
1. Update the column count wherever it appears as a label in Mermaid diagrams:
   - `docs/architecture.md` → High-Level Overview (`historical_dataset_clean.parquet` node)
   - `docs/architecture.md` → Data Flow Detail diagram (`Feature Matrix` node)
   - `docs/methodology/models.md` → Training Pipeline flowchart (top node)
   - `docs/index.md` → tagline ("319 features")
2. If new columns are ML features, update `docs/methodology/features.md` under the correct factor group.
3. If new columns are factor scores (e.g. `alpha_momentum`), update `docs/methodology/factor-library.md`.

### Adding or changing a factor in alpha/
1. Add/update the factor spec in `docs/methodology/factor-library.md` — formula, IC target, universe coverage.
2. If it changes composite score weights, update the weighting table in the same file.
3. If it adds new columns to the parquet, follow the "Adding dataset columns" rules above.

### Changing model performance
1. Update `docs/methodology/models.md` AUC table — all four columns: Val AUC, Test AUC, WF Mean AUC, target met.
2. Update `docs/index.md` Performance at a Glance table.
3. If the training pipeline structure changed (new step, removed step), update the Mermaid flowchart in models.md.

### Adding a system component (DB, API, service)
1. Add a `subgraph` or node to `docs/architecture.md` High-Level Overview.
2. Add a row to the Component Map table.
3. Update the Deployment Architecture diagram if it affects deployment topology.

---

## Reference Tables (extracted to save tokens)

> Full architecture state, model performance, and file locations are in
> `CLAUDE_REFERENCE.md` (same directory). Read it when needed for a task.
> Key shortcuts: dataset = `data/historical_dataset_clean.parquet`, models = `models/model_*.joblib`

---

## "Done" Definition

A task is done only when ALL of the following are complete:

1. ✅ Code written and manually tested
2. ✅ All Change Checklist rows satisfied (docs updated)
3. ✅ `CHANGELOG.md` entry added under `[Unreleased]`
4. ✅ `git add` + `git commit` with conventional message
5. ✅ `git push` to remote
6. ✅ HuggingFace push (`scripts/data_io/push_to_hf.py`) **if** `data/` or `models/` changed

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

Scope examples: `quarterly`, `api`, `ui`, `models`, `db`, `pipeline`, `docs`, `alpha`, `momentum`, `factors`

Always include the affected script or file name in the commit body when adding/modifying
scripts, so git log is a useful change history.
