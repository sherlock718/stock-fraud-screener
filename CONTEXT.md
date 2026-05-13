# CONTEXT — Session State Snapshot

> Update this file at the start and end of every session.
> Purpose: give any new session full project context in under 60 seconds.

---

## What This Project Is

**Renaissance-style quantitative alpha lab.** NOT a fraud screener. NOT a fixed-weight factor composite.

ML discovers which features matter per market and regime. Generates hundreds of alpha signals. Each alpha is independently backtested. Portfolio construction selects and weights validated alphas.

See `ROADMAP.md` for phase plan. See `CLAUDE.md` for architecture state and pre-task checklist.

---

## Phase Framing (locked — do not re-audit above phase scope)

| Phase | Scope |
|---|---|
| **Phase A** | Dataset · Data quality/EDA · Update schedule · Update framework + diagram |
| **Phase B** | Feature Library + Documentation · Feature Engineering · Feature Selection · Factor Research · Research Notebooks |
| **Phase C** | Model Train/Tuning · Look-ahead bias + model improvement · Industry-grade backtest · Alpha Generation Schema |

**Phase A**: mostly done — 5-market dataset built, momentum ranks present, feature pipeline running.  
**Phase B**: mostly done — 355-feature library, HAC/FDR selection, factor research reports, 5 EDA notebooks.  
**Phase C**: in progress — 3 models trained (1y/3y/5y), alpha package built, backtest partial; 6m/2y models pending.

---

## Verified Data State (as of 2026-05-13)

### historical_dataset_clean.parquet — 58,190 rows × 355 cols (all annual)

Quarterly signals are enriched into annual rows via `scripts/enrich_quarterly_features.py`.

| Market | Rows | Tickers | Fiscal Year Range | Quarterly enriched? |
|---|---|---|---|---|
| US | 44,059 | 4,847 | 2008–2027 | ✅ (merged into annual rows; 61–67% coverage) |
| CA | 8,955 | ~1,400 | 2021–2026 | ❌ annual only |
| KR | 2,538 | 300 | 2015–2025 | ✅ (merged) |
| JP | 517 | ~120 | 2021–2026 | ❌ annual only |
| DE/FR/IT/ES/SE/FI/NL/PT/DK | ~1,600 | ~300 | 2021–2026 | ❌ annual only |
| BR | 688 | ~55 | 2010–2025 | ✅ (merged) |

**Known data gaps:**
- CA/EU/JP: only 4–5 years of history — too shallow for reliable ICIR training
- BR: only ~55 tickers; CVM bulk list would give ~400+
- OOF score columns (`ml_1y_oof`, `ml_3y_oof`, `ml_5y_oof`): ABSENT — `generate_oof_scores.py` not yet run
- 6m/2y model horizons: not yet trained (`models/model_meta.json` has 1y/3y/5y only)

### Key column audit (selected)

| Column / Group | Null Rate | Notes |
|---|---|---|
| ml_1y, ml_3y, ml_5y | 0% | ✅ Static ML scores present |
| ml_*_oof | absent | ❌ OOF scores not generated yet (Phase C) |
| alpha_composite | 41.6% | ✅ Present; limited by value/momentum data |
| alpha_fraud_risk | 0% | ✅ Present |
| momentum_12m_rank | 7.9% | ✅ Present |
| momentum_composite_rank | 5.0% | ✅ Present |
| montier_c2 | 41.6% | ✅ Fixed 2026-05-13 (was 100% null — ppe bug) |
| montier_c_score | 24.5% | ✅ Fixed |
| quarterly_positive_rev_frac | 33% null | ✅ Present |

---

## Phase A — Remaining Gaps

| Gap | File | Why it matters |
|---|---|---|
| BR tickers: only ~55 (need ~400+) | `scripts/step1_fetch_tickers_br.py` | Too thin for cross-sectional signal validation |
| CA/EU/JP: 4–5 yr only | All integrate scripts | Cannot compute reliable 5yr CAGR or ICIR |
| KR DART ingestion ongoing | `scripts/run_pipeline_kr.py` | ETA ~29 May 2026 |
| Multi-market GitHub Actions refresh | `.github/workflows/` | Currently US-only weekly cron |
| `fraud_score_governance` all-NaN | `pipeline/enrich_governance.py` | Governance signal missing |

**Exit criteria for Phase A**: All in `docs/developer/phase-done-criteria.md` → run `scripts/check_sync.py` before declaring done.

---

## Phase B — Remaining Gaps

| Gap | File | Why it matters |
|---|---|---|
| IC decay notebook (how long signals predict) | `notebooks/06_ic_decay.ipynb` | Required for factor research completeness |
| Regime-conditional factor analysis | `notebooks/07_regime_analysis.ipynb` | Bear vs bull factor stability |
| Cross-market factor comparison | `notebooks/08_cross_market_factors.ipynb` | Validate signal universality |
| Alpha registry schema | `alpha/signals/` | Phase B output: signal_id, IC, AUC, horizon |

**Exit criteria for Phase B**: All in `docs/developer/phase-done-criteria.md`.

---

## Phase C — Current State

| Item | Status |
|---|---|
| Models (1y/3y/5y) | ✅ Trained; val_auc 1y=0.577, 3y=0.740, 5y=NaN |
| 6m/2y models | ❌ Not yet trained |
| OOF scoring | ❌ `generate_oof_scores.py` exists, not yet run |
| Bias audit | ⚠️ `bias_audit.py` exists; pass/fail not confirmed |
| SPY benchmark data | ✅ `data/spy_returns.csv` present |
| Backtest | ⚠️ `backtester.py` partial — SPY benchmark wired, per-alpha backtest missing |
| Alpha package | ✅ `alpha/factors/` — 5 factor scores, HorizonRouter |
| Alpha registry | ❌ Not yet built |

---

## Scripts Present (key)

| Script | Purpose | Status |
|---|---|---|
| `pipeline/feature_library.py` | All 355 feature formulas | ✅ |
| `pipeline/step5_compute_features.py` | Feature computation (montier_c2 bug FIXED) | ✅ |
| `scripts/run_feature_selection.py` | PSI → IC (HAC) → ICIR (FDR) → Spearman dedup | ✅ |
| `scripts/train_models.py` | Walk-forward LightGBM training (1y/3y/5y) | ✅ |
| `scripts/score_historical.py` | Load models → score all rows → write ml_* to parquet | ✅ |
| `scripts/generate_oof_scores.py` | OOF walk-forward scoring (ml_*_oof cols) | ✅ (not yet run) |
| `scripts/factor_research.py` | IC/ICIR factor research reports | ✅ |
| `scripts/bias_audit.py` | 4-audit look-ahead/survivorship/overfit/FDR | ✅ |
| `scripts/backtester.py` | Walk-forward backtester + SPY benchmark | ⚠️ Partial |
| `scripts/push_to_hf.py` | Push data/models to HuggingFace Hub | ✅ |

---

## Key Files

| File | Purpose |
|---|---|
| `CLAUDE.md` | Architecture state, pre-task checklist, sync rules |
| `ROADMAP.md` | Phase plan with status rows |
| `CONTEXT.md` | This file |
| `data/historical_dataset_clean.parquet` | Main dataset (355 cols, 58,190 rows) |
| `models/model_meta.json` | Selected features per horizon (1y/3y/5y) + training stats |
| `reports/factor_research_*.csv` | IC/ICIR factor research per horizon |
| `docs/developer/phase-done-criteria.md` | Machine-checkable exit criteria per phase |
| `scripts/check_sync.py` | Pre-commit doc sync validator |

---

## Session Log

| Date | Work Done |
|---|---|
| 2026-05-11 | Vision realignment; D1–D7 docs; ROADMAP.md; CONTEXT.md; vision memory saved |
| 2026-05-11 | Roadmap restructured to 16-step backbone; Phase B/C parked; full data audit run |
| 2026-05-13 | Full Phase A/B audit: montier_c2 100% null bug FIXED (ppe→ppe_net); CONTEXT.md rewritten; ROADMAP.md status sync; doc sweep; coverage depth task queued |
