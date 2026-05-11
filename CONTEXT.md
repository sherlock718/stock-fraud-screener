# CONTEXT — Session State Snapshot

> Update this file at the start and end of every session.
> Purpose: give any new session full project context in under 60 seconds.

---

## What This Project Is

**Renaissance-style quantitative alpha lab.** NOT a fraud screener. NOT a fixed-weight factor composite.

ML discovers which features matter per market and regime. Generates hundreds of alpha signals. Each alpha is independently backtested. Portfolio construction selects and weights validated alphas.

See `ROADMAP.md` for phase plan. See `CLAUDE.md` for architecture state and pre-task checklist.

---

## Current Phase

**Phase 0 — Foundation** (🟡 In Progress)

### Completed This Session
- [x] D1 — CLAUDE.md reframed to multi-factor platform
- [x] D2 — docs/index.md reframed
- [x] D3 — docs/architecture.md reframed
- [x] D4 — docs/methodology/models.md feature selection clarification
- [x] D5 — docs/methodology/pipeline.md reframed
- [x] D6 — docs/methodology/feature-selection.md (new)
- [x] D7 — docs/methodology/features.md dual taxonomy rewrite
- [x] ROADMAP.md created
- [x] CONTEXT.md created (this file)
- [x] Vision memory saved to memory/project_vision_quant_lab.md

### Remaining Phase 0 Docs
- [ ] D8 — docs/methodology/factor-library.md (feature groups as ML inputs, NOT score composite)
- [ ] D9 — docs/developer/contributing.md (vision checklist + sync rules)
- [ ] D10 — mkdocs.yml nav update + site_name fix
- [ ] D11 — git commit + push all docs

### Codebase Cleanup (not yet done)
- [ ] Delete `pipeline/enrich_auditor_going_concern.py` (superseded by enrich_governance.py)
- [ ] Delete `pipeline/score_and_report.py` (rules-based composite — contradicts ML-first vision)
- [ ] Delete `scripts/watchlist.py` (session state export)
- [ ] Delete `scripts/high_roi_strategies.py` (redundant wrapper)

### Phase 0 Code Blockers (not yet started)
- [ ] P0.1 — Cross-sectional momentum (12m-1m) → `pipeline/feature_library.py`
- [ ] P0.2 — Fix SPY benchmark → `scripts/backtester.py`
- [ ] P0.3 — `scripts/score_historical.py` (writes ml_1y/ml_3y/ml_5y to parquet) ← CRITICAL
- [ ] P0.4 — Newey-West HAC + Fama-MacBeth + FDR → `scripts/train_models.py`
- [ ] P0.5 — Verify 20-year data coverage per market

---

## Current Model Performance

| Horizon | WF AUC | Target | Status |
|---|---|---|---|
| 1y | 0.553 | ≥ 0.62 | ❌ |
| 3y | 0.643 | ≥ 0.62 | ✅ |
| 5y | 0.597 | ≥ 0.62 | ❌ |

**Root cause of weak 1y/5y**: momentum gap (0 true cross-sectional momentum features), SPY benchmark broken (using equal-weight universe mean), no historical ML scores in parquet.

---

## Architecture in One Paragraph

Data flows: SEC/SimFin/DART/TDNET/SEDAR+/B3 → annual snapshots (step1–step2) → price enrichment (step3) → macro join (step4) → 319 feature computation (step5, feature_library.py) → clean/normalise (step6) → feature selection (PSI → IC → ICIR → Spearman dedup, ~35 features per horizon) → LightGBM training (1y/3y/5y) → alpha signal generation (market × horizon × segment × feature_subset × model_type) → per-alpha backtesting → alpha selection (Sharpe > 0.5, drawdown < 30%, IC > 0.02) → portfolio construction → FastAPI → frontend.

The 5 factor groups (Value / Quality / Momentum / Growth / FraudRisk) are **ML input categories**, not scored with fixed weights.

---

## Key Files

| File | Purpose |
|---|---|
| `CLAUDE.md` | Architecture state, pre-task checklist, sync rules — read every session |
| `ROADMAP.md` | Phase plan with checkboxes — update as tasks complete |
| `CONTEXT.md` | This file — session state snapshot |
| `pipeline/feature_library.py` | Single source of truth for all 319 feature formulas |
| `scripts/train_models.py` | ML training with PSI/IC/ICIR feature selection |
| `scripts/backtester.py` | Walk-forward backtesting (SPY benchmark broken — P0.2) |
| `data/historical_dataset_clean.parquet` | Main dataset (US, ~155K rows) |
| `models/model_meta.json` | Selected features per horizon + training stats |

---

## Data Coverage

| Market | Status | Source |
|---|---|---|
| US | ✅ Built (step1–step6 done) | SEC EDGAR |
| EU | ⚠️ Partial (snapshots exist, not integrated) | SimFin |
| KR | ⚠️ Partial (phase_a_integrate_kr.py exists) | DART |
| JP | ❌ Not integrated | TDNET |
| CA | ❌ Not integrated | SEDAR+ |
| BR | ❌ Not integrated | B3/CVM |

---

## Critical Decisions Made

1. **Monolith** for research pipeline (layers 1–13), thin **FastAPI** + separate **React/Next.js** frontend as the only service boundaries.
2. **Feature groups are ML inputs**, not a scoring rubric. This is irreversible and must be preserved in all future docs and code.
3. **HuggingFace** for data storage. **Git** for code. Docs via MkDocs.
4. **Phase-gate reviews**: after each phase, show output → ask alignment questions → user approves before next phase starts.
5. **Four files to delete** (see cleanup section above) — rules-based composite code contradicts the vision.

---

## Next Session — Where to Resume

1. Execute D8: create `docs/methodology/factor-library.md`
2. Execute D9: create `docs/developer/contributing.md`
3. Execute D10: update `mkdocs.yml`
4. Execute D11: git commit + push
5. Codebase cleanup: delete 4 files
6. Start Phase 0 code: P0.3 first (score_historical.py) — it's the critical blocker

---

## Session Log

| Date | Work Done |
|---|---|
| 2026-05-11 | Vision realignment; D1–D7 docs; ROADMAP.md; CONTEXT.md; vision memory saved |
