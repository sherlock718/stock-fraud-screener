# CLAUDE.md — AI Assistant Instructions

Instructions for Claude Code when working in this repository. These rules enforce
architectural sync so that code, documentation, and diagrams always stay consistent.

**Project framing**: This is a **multi-factor stock screener and alpha generation platform**,
not a fraud screener. Fraud risk is one of five factors (Value · Quality · Momentum · Growth · Fraud Risk).

---

## Change Checklist — Required Before Every Commit

Work through this matrix for every change before staging files.

| What changed | Must also update |
|---|---|
| New script in `scripts/` | `docs/developer/scripts.md` — add section with usage, flags table, outputs |
| New script in `pipeline/` | `docs/developer/pipeline-scripts.md` — add section for that module |
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
| New UI tab or feature in Streamlit | `docs/guide/app.md` |
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

## Current Architecture State (keep in sync with docs/architecture.md)

| Layer | Technology | Location | Status |
|---|---|---|---|
| US data ingestion | SEC EDGAR 10-K/10-Q | `scripts/run_pipeline.py` | ✅ |
| Multi-market ingestion | SimFin (EU), DART (KR), TDNET (JP), SEDAR+ (CA), B3 (BR) | `pipeline/` | ✅ |
| Feature engineering | 326 columns (enterprise_value + sector added) | `pipeline/step5_compute_features.py` | ✅ |
| Quarterly enrichment | 5 intra-year dynamics | `scripts/enrich_quarterly_features.py` | ✅ |
| Survivorship correction | Imputes −50% return for delisted | `scripts/mark_survivorship.py` | ✅ |
| AAER fraud labels | 492 positive rows / 118 companies | `scripts/fetch_aaer_labels.py` | ✅ |
| Historical ML scoring | Load models → score all rows → write ml_1y/3y/5y to parquet | `scripts/score_historical.py` | ❌ not yet built |
| Alpha factor package | 5-factor scores (Value/Quality/Momentum/Growth/FraudRisk) | `alpha/factors/` | ❌ not yet built |
| Primary storage | Parquet file | `data/historical_dataset_clean.parquet` 58K rows × 326 cols | ✅ |
| TimescaleDB | Hypertable for time-series queries | `infra/db/init.sql` + `scripts/migrate_to_db.py` | ⚠️ DB not loaded |
| ML models | LightGBM 1y/3y/5y, PSI filter + ICIR | `scripts/train_models.py` | ✅ |
| Calibration | Platt scaling | `scripts/tune_models.py` | ✅ |
| Drift monitoring | PSI + rolling AUC | `scripts/monitor_drift.py` | ✅ |
| Streamlit UI | 10-tab app | `app_v2.py` | ✅ (needs 5-factor UI update in Phase 2) |
| FastAPI | Screener router, pagination | `api/` | ✅ built |
| CI/CD | Weekly refresh + drift monitor | `.github/workflows/` | ✅ |
| Model/dataset hosting | HuggingFace Hub | `scripts/push_to_hf.py` | ✅ |

### Critical Missing Pieces (Phase 0 blockers)
1. **`score_historical.py`** — without this, `ml_1y/ml_3y/ml_5y` columns are never written to parquet; backtester uses 0% ML weight
2. **Momentum features** — 0 momentum features in all 319; most documented alpha source (Jegadeesh & Titman 1993)
3. **SPY benchmark** — backtester uses equal-weight universe mean, not SPY; CAGR/excess return numbers are misleading

### Current Performance
| Horizon | WF Mean AUC | Target | Met? |
|---|---|---|---|
| 1y | 0.553 | ≥ 0.62 | ❌ |
| 3y | 0.643 | ≥ 0.62 | ✅ |
| 5y | 0.597 | ≥ 0.62 | ❌ |

---

## Key File Locations

| What | File |
|---|---|
| Dataset (primary) | `data/historical_dataset_clean.parquet` |
| Models | `models/model_{1y,3y,5y}.joblib` |
| Model metadata + feature lists | `models/model_meta.json` |
| Walk-forward AUC results | `reports/walk_forward_auc_{1y,3y,5y}.csv` |
| Backtest results | `data/backtest_results.json` |
| DB schema | `infra/db/init.sql` |
| Feature definitions (314 base formulas) | `pipeline/feature_library.py` |
| Factor package (planned) | `alpha/factors/` |
| App entry point | `app_v2.py` |
| API entry point | `api/main.py` |
| Architecture doc | `docs/architecture.md` |
| Scripts reference | `docs/developer/scripts.md` |
| Pipeline modules reference | `docs/developer/pipeline-scripts.md` |
| Factor library reference | `docs/methodology/factor-library.md` |
| Feature selection methodology | `docs/methodology/feature-selection.md` |
| Model methodology | `docs/methodology/models.md` |
| Contributing + sync rules | `docs/developer/contributing.md` |

---

## "Done" Definition

A task is done only when ALL of the following are complete:

1. ✅ Code written and manually tested
2. ✅ All Change Checklist rows satisfied (docs updated)
3. ✅ `CHANGELOG.md` entry added under `[Unreleased]`
4. ✅ `git add` + `git commit` with conventional message
5. ✅ `git push` to remote
6. ✅ HuggingFace push (`scripts/push_to_hf.py`) **if** `data/` or `models/` changed

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
