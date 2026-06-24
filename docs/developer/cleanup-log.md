# Cleanup Log

Progress tracker for repo cleanup sessions.

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
