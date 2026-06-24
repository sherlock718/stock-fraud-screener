# Cleanup Log

Progress tracker for repo cleanup sessions.

---

## Session 7 — Architecture Documentation Truth Rebuild (2026-06-24)

**What changed:**
1. Updated `docs/architecture.md`:
   - Column count 361→367 in Pipeline subgraph and Storage node
   - Removed FastAPI from Outputs subgraph (archived Session 2)
   - Removed TimescaleDB and FastAPI from Deployment Architecture diagram
   - Fixed KR integration Component Map row (`phase_a_integrate_kr.py` → actual step1/step2 files)
   - Removed stale hardcoded column counts from Component Map rows
   - Changed Pipeline subgraph label to "production spine"
   - Updated `infra/db/init.sql` reference (no longer exists)
2. Created `docs/developer/alpha-research-architecture.md`:
   - Full directory role table (15 directories)
   - ASCII data flow summary
   - Pipeline module inventory (12 modules)
   - Scripts subdirectory detail (all 9 subdirs with file listings)
   - Key invariants section
3. Updated `docs/index.md`: column count 341→367 in tagline and Mermaid diagram
4. Updated `docs/methodology/models.md`: column count 361→367 in training pipeline flowchart
5. Updated `CHANGELOG.md` with Session 7 summary

**Tests:** 341 passed (no code changes)

**Risks/deferred:**
- Data Flow Detail diagram still shows intermediate counts (321→326→341→367) which are approximate — they trace the enrichment chain, not exact current checkpoints. Left as-is because they describe the logical flow accurately.
- `docs/quickstart.md` is already correct (no column count references, paths updated in Session 5)

**Next session:** Session 8 — Final Repomix and .repomixignore cleanup

---

## Session 2 — Archive Dead UI/API/Deployment Layer (2026-06-24)

**What changed:**
1. Deleted `TestScoreCompanies` class from `tests/test_pipeline.py` (2 tests that imported deprecated `app_v2.py`)
2. Removed `api-change`, `app-change`, `infra-change` trigger rules from `scripts/check_sync.py`
3. Removed stale `app_v2.py` comment from `scripts/backtester.py`
4. Archived to `_archive/`: `src/`, `app.py`, `app_v2.py`, `.streamlit/`, `api/`, `infra/`, `Dockerfile`, `docker-compose.yml`, `mkdocs.yml`, `repomix_metadata_pack/`
5. Moved to `docs/reference/`: `AI_EDIT_LOG.md`, `PIPELINE_ATLAS.md`, `PARQUET_ATLAS.md`
6. Deleted 3 old repomix XML files (~11.3MB)
7. Updated `.repomixignore` to exclude `_archive/`

**Tests:** 341 passed (was 343 — 2 deleted tests)

**Risks/deferred:**
- `docs/guide/app.md` still references the deprecated Streamlit app — deferred to Session 6 audit
- No core MD files (`CLAUDE.md`, `CONTEXT.md`, `README.md`, `ROADMAP.md`) referenced the moved files

**Next session:** Session 3 — Design scripts/ structure (doc only, no moves)

---

## Session 3 — Script Migration Map (2026-06-24)

**What changed:**
1. Created `docs/developer/script-migration-map.md` with:
   - Target directory structure (11 subdirectories)
   - Complete old → new path mapping (49 scripts)
   - `__init__.py` plan (all empty markers)
   - `scripts/_root.py` definition
   - Cross-import resolution table (8 internal imports + 1 external from alpha/)
   - Path assumption fix list (42 scripts using `Path(__file__).parent.parent`)
   - CI workflow reference update table (3 workflow files, 19 line changes)
   - Subprocess reference update table (2 references in wait_and_merge.py)
   - Docstring/comment reference list (5 scripts with usage examples to update)

**Tests:** No code changed — migration map is documentation only.

**Risks/deferred:**
- Actual moves happen in Session 4 as one atomic commit
- Docstring updates are cosmetic but included for correctness

**Next session:** Session 4 — Move scripts atomically

---

## Session 4 — Move Scripts Atomically (2026-06-24)

**What changed:**
1. Created 10 subdirectories under `scripts/`: `_shared/`, `workflows/`, `data_io/`, `enrichments/`, `modeling/`, `analysis/`, `portfolio/`, `quality/`, `ops/`, `hooks/`
2. Added `__init__.py` to `scripts/` and all 10 subdirectories (empty package markers)
3. Created `scripts/_root.py` (canonical `ROOT = Path(__file__).resolve().parent.parent`)
4. Moved 49 scripts to their new subdirectories via `git mv`
5. Replaced all `Path(__file__).parent.parent` patterns with `from scripts._root import ROOT` (42 scripts)
6. Removed all `sys.path.insert(0, ...)` hacks from scripts (12 occurrences)
7. Fixed 8 cross-imports (train_models, backtester, enrich_quarterly_features, leverage_strategy)
8. Updated `alpha/explain.py` import of leverage_strategy
9. Updated `.github/workflows/refresh_data.yml` (19 path references)
10. Updated `.github/workflows/monitor_drift.yml` (2 path references)
11. Fixed subprocess references in `wait_and_merge.py` and `run_dataset_enrichments.py`
12. Updated docstring/usage examples in 6 workflow scripts
13. Fixed test imports in `tests/test_pipeline.py` (8 test methods) and `tests/pipeline/test_enrich_fraud_labels.py`
14. Added `pytest.ini` with `pythonpath = .` and `testpaths = tests`

**Tests:** 341 passed (same count as Session 3)

**Risks/deferred:**
- Documentation files (CLAUDE.md, architecture.md, scripts.md, etc.) still reference old paths — deferred to Session 5
- CHANGELOG.md not updated — deferred to Session 5
- `pipeline/` remains untouched as the production spine (by design)

**Next session:** Session 5 — Update all documentation references and CHANGELOG
