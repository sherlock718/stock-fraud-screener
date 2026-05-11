# CLAUDE.md — AI Assistant Instructions

Instructions for Claude Code when working in this repository. These rules enforce
architectural sync so that code, documentation, and diagrams always stay consistent.

---

## Change Checklist — Required Before Every Commit

Work through this matrix for every change before staging files.

| What changed | Must also update |
|---|---|
| New script in `scripts/` | `docs/developer/scripts.md` — add section with usage, flags table, outputs |
| Modified script CLI flags | `docs/developer/scripts.md` — update the flags table for that script |
| New pipeline step or data column | `docs/architecture.md` — Component Map row + Data Flow diagram + column count in both flowchart labels |
| Column count changes in dataset | `docs/architecture.md` High-Level diagram + Data Flow diagram + `docs/methodology/models.md` flowchart label |
| Model performance changes (AUC, etc.) | `docs/methodology/models.md` — AUC table (Val AUC, Test AUC, WF Mean AUC, target flag) |
| ML pipeline structural change | `docs/methodology/models.md` Mermaid flowchart + `docs/architecture.md` ML System subgraph |
| New system component (DB, API, queue) | `docs/architecture.md` — add node to High-Level Overview + row to Component Map |
| New API endpoint | Docstring in route file; if user-facing add entry to `docs/developer/` |
| New UI tab or feature | `docs/guide/app.md` |
| Any of the above | `CHANGELOG.md` — add entry under `[Unreleased]` with the script/file name bolded |
| All changes | Commit with conventional message: `feat(scope):`, `fix(scope):`, `docs:`, `perf:`, etc. |

---

## Architecture Sync Rules

### Adding a script
1. New section in `docs/developer/scripts.md`: description, bash usage examples, flags table, output files.
2. If the script adds a pipeline step, add a node to `docs/architecture.md` Data Pipeline subgraph.
3. If it changes data flow, update the Data Flow Detail diagram.

### Adding or removing dataset columns
1. Update the column count wherever it appears as a label in Mermaid diagrams:
   - `docs/architecture.md` → High-Level Overview (`historical_dataset_clean.parquet` node)
   - `docs/architecture.md` → Data Flow Detail diagram (`Feature Matrix` node)
   - `docs/methodology/models.md` → Training Pipeline flowchart (top node)
2. If new columns are ML features, check `docs/methodology/features.md`.

### Changing model performance
1. Update `docs/methodology/models.md` AUC table — all four columns: Val AUC, Test AUC, WF Mean AUC, and whether target (≥ 0.62) is met.
2. If the training pipeline structure changed (new step, removed step), update the Mermaid flowchart in the same file.

### Adding a system component (DB, API, service)
1. Add a `subgraph` or node to `docs/architecture.md` High-Level Overview.
2. Add a row to the Component Map table.
3. Update the Deployment Architecture diagram if it affects the deployment topology.

---

## Current Architecture State (keep in sync with docs/architecture.md)

| Layer | Technology | Location | Status |
|---|---|---|---|
| US data ingestion | SEC EDGAR 10-K/10-Q | `scripts/run_pipeline.py` | ✅ |
| Multi-market ingestion | SimFin (EU), DART (KR), TDNET (JP), SEDAR+ (CA), B3 (BR) | `pipeline/` | ✅ |
| Feature engineering | 319 columns | `pipeline/feature_library.py` | ✅ |
| Quarterly enrichment | 5 intra-year dynamics | `scripts/enrich_quarterly_features.py` | ✅ |
| Survivorship correction | Imputes −50% return for delisted | `scripts/mark_survivorship.py` | ✅ |
| Primary storage | Parquet file | `data/historical_dataset_clean.parquet` | ✅ |
| TimescaleDB | Hypertable for time-series queries | `infra/db/init.sql` + `scripts/migrate_to_db.py` | ⚠️ DB not loaded |
| ML models | LightGBM 1y/3y/5y, PSI filter + ICIR | `scripts/train_models.py` | ✅ |
| Calibration | Platt scaling | `scripts/tune_models.py` | ✅ |
| Drift monitoring | PSI + rolling AUC | `scripts/monitor_drift.py` | ✅ |
| Streamlit UI | 10-tab app | `app_v2.py` | ✅ |
| FastAPI | Screener router, pagination | `api/` | ✅ built |
| CI/CD | Weekly refresh + drift monitor | `.github/workflows/` | ✅ |
| Model/dataset hosting | HuggingFace Hub | `scripts/push_to_hf.py` | ✅ |

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
| Feature definitions (313 formulas) | `pipeline/feature_library.py` |
| App entry point | `app_v2.py` |
| API entry point | `api/main.py` |
| Architecture doc | `docs/architecture.md` |
| Scripts reference | `docs/developer/scripts.md` |
| Model methodology | `docs/methodology/models.md` |

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

Scope examples: `quarterly`, `api`, `ui`, `models`, `db`, `pipeline`, `docs`

Always include the affected script or file name in the commit body when adding/modifying
scripts, so git log is a useful change history.
