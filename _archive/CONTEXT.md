# CONTEXT — Session State Snapshot

> Update this file at the start and end of every session.
> Purpose: give any new session full project context in under 60 seconds.

---

## What This Project Is

**Renaissance-style quantitative alpha lab.** NOT a fraud screener. NOT a fixed-weight factor composite.

**Dual platform**: (1) **Stock screener** — cross-sectional ML ranking by factor strength across markets; (2) **Alpha generation platform** — walk-forward ML models discover which factors matter per market, horizon, and regime.

**Investment objective**: ≥20% annualised ROI on deployed capital, Sharpe ≥ 1.0, max drawdown ≤ 30%. Current backtest: +38.1% CAGR, Sharpe 1.181, +24.2% vs SPY. Walk-forward OOS validation required before live capital.

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
**Phase B**: **COMPLETE** — 355-feature library, HAC/FDR selection, factor research reports, 6 EDA notebooks (incl. 06_ic_decay). All checks pass: `run_phase_checks.py` → 59 PASS 0 FAIL.  
**Phase C**: **COMPLETE** — `run_phase_checks.py --phase C` → 30 PASS 0 FAIL 0 WARN (2026-05-14). All 5 model horizons trained, OOF scores generated, bias audit passed, backtest run (CAGR +38.1%, Sharpe 1.181), alpha_registry.json built (8 signals, 6 selected).  
Remaining Phase C enhancements (beyond exit criteria): retrain 6m/1y/2y to meet AUC targets (fixes coded), ablation study, slippage modelling, per-signal JSONs in `reports/alpha_backtests/`.

---

## Verified Data State (as of 2026-05-14)

### historical_dataset_clean.parquet — 58,190 rows × 360 cols (355 base + 5 OOF)

Quarterly signals are enriched into annual rows via `scripts/enrichments/enrich_quarterly_features.py`.

| Market | Rows | Tickers | Fiscal Year Range | Quarterly enriched? | Depth |
|---|---|---|---|---|---|
| US | 44,059 | 4,847 | 2008–2027 | ✅ (61–67% coverage) | DEEP — 18/20 yrs ≥50 tickers |
| CA | 8,955 | 2,005 | 2021–2026 | ❌ annual only | MODERATE — 5/6 yrs ≥50 tickers |
| KR | 2,538 | 300 | 2015–2025 | ✅ (merged) | DEEP — 11/11 yrs ≥50 tickers |
| JP | 517 | 122 | 2021–2026 | ❌ annual only | MODERATE — 4/6 yrs ≥50 tickers |
| DE | 356 | 77 | 2021–2025 | ❌ annual only | SHALLOW — 5/5 yrs ≥50 tickers, <200 |
| FR | 193 | 40 | 2021–2025 | ❌ annual only | TOO_THIN — max 40/yr |
| IT | 192 | 40 | 2021–2025 | ❌ annual only | TOO_THIN — max 40/yr |
| ES | 169 | 35 | 2021–2026 | ❌ annual only | TOO_THIN — max 35/yr |
| SE | 139 | 30 | 2021–2025 | ❌ annual only | TOO_THIN — max 30/yr |
| FI | 116 | 25 | 2021–2025 | ❌ annual only | TOO_THIN — max 25/yr |
| NL | 115 | 24 | 2021–2025 | ❌ annual only | TOO_THIN — max 24/yr |
| BR | 688 | 48 | 2010–2025 | ✅ (merged) | TOO_THIN — max 47/yr |
| DK | 71 | 15 | 2021–2025 | ❌ annual only | TOO_THIN — max 15/yr |
| PT | 82 | 17 | 2021–2025 | ❌ annual only | TOO_THIN — max 17/yr |

**Coverage depth assessment** (see `reports/coverage_audit.csv` + `reports/coverage_audit_heatmap.png`):
- **IC training ready**: US (DEEP), KR (DEEP), CA (MODERATE), JP (MODERATE) — these 4 markets have ≥50 tickers/yr for reliable cross-sectional factor IC
- **Too thin for standalone IC**: DE/FR/IT/ES/SE/FI/NL/PT/DK/BR — can supplement US signal but cannot be trained independently
- **BR ticker gap**: only 48 unique tickers (CVM bulk list gives ~400+); history is there (2010–2025) but cross-sectional depth insufficient

**Known data gaps:**
- EU/JP: only 4–5 years of history — too shallow for 5yr CAGR or ICIR time-series
- BR: only 48 tickers; CVM bulk list would give ~400+
- OOF score columns (`ml_1y_oof`, `ml_3y_oof`, `ml_5y_oof`): ABSENT — `generate_oof_scores.py` not yet run
- 6m/2y model horizons: not yet trained (`models/model_meta.json` has 1y/3y/5y only)

### Key column audit (selected)

| Column / Group | Null Rate | Notes |
|---|---|---|
| ml_1y, ml_3y, ml_5y | 0% | ✅ Static ML scores present |
| ml_*_oof (6m/1y/2y/3y/5y) | varies | ✅ OOF scores generated (NaN for training-window rows) |
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
| BR tickers: only ~55 (need ~400+) | `pipeline/step1_fetch_tickers_br.py` | Too thin for cross-sectional signal validation |
| CA/EU/JP: 4–5 yr only | All integrate scripts | Cannot compute reliable 5yr CAGR or ICIR |
| KR DART ingestion ongoing | `scripts/workflows/run_pipeline_kr.py` | ETA ~29 May 2026 |
| Multi-market GitHub Actions refresh | `.github/workflows/` | Currently US-only weekly cron |
| `fraud_score_governance` all-NaN | `pipeline/archive/enrich_governance.py` (ARCHIVED) | Governance signal missing; going_concern logic needs migration to step5 |

**Exit criteria for Phase A**: All in `docs/developer/phase-done-criteria.md` → run `scripts/quality/check_sync.py` before declaring done.

---

## Phase B — Status: COMPLETE

All Phase B exit criteria pass (`run_phase_checks.py` → 59 PASS 0 FAIL 1 WARN as of 2026-05-13).

Previously listed gaps resolved:
- `notebooks/06_ic_decay.ipynb` ✅ — IC decay, half-life estimation, regime conditioning, lag-1 autocorrelation
- Regime-conditional factor analysis ✅ — covered in `notebooks/02_factor_research.ipynb` (Section 10) + `06_ic_decay.ipynb`
- Cross-market factor comparison ✅ — covered in `notebooks/02_factor_research.ipynb` + coverage depth audit
- Alpha registry schema — deferred to Phase C (not a Phase B exit criterion per `phase-done-criteria.md`)

---

## Phase C — Status: COMPLETE (2026-05-14)

`run_phase_checks.py --phase C` → **30 PASS 0 FAIL 0 WARN**

| Item | Status |
|---|---|
| Models (all 5 horizons: 6m/1y/2y/3y/5y) | ✅ Trained + feature sets + model_meta.json |
| OOF scoring | ✅ `ml_6m_oof`, `ml_1y_oof`, `ml_2y_oof`, `ml_3y_oof`, `ml_5y_oof` in parquet |
| Bias audit | ✅ `bias_audit.py` passes all 4 checks |
| SPY benchmark data | ✅ `data/spy_returns.csv` (18 years) |
| Backtest | ✅ COMPOSITE +38.1% CAGR, +24.2% vs SPY, Sharpe 1.181 |
| Alpha package | ✅ `alpha/factors/` — 5 factor scores, HorizonRouter |
| Alpha registry | ✅ `data/alpha_registry.json` — 8 signals, 6 selected |
| Baseline comparison | ✅ `baseline_lr_{6m,1y,2y,3y,5y}.joblib` all present |

**Phase C enhancements queued (beyond exit criteria):**
- Retrain 6m/1y/2y with FORCE_INCLUDE + sector-neutral IC + stability filter → improve AUC (currently 6m=0.549, 1y=0.549, 2y=0.578)
- Ablation study (feature group contribution analysis)
- Slippage modelling in `scripts/_shared/backtester.py`
- Per-signal JSON files `reports/alpha_backtests/{signal_id}.json`

---

## Scripts Present (key)

| Script | Purpose | Status |
|---|---|---|
| `pipeline/feature_library.py` | All 355 feature formulas | ✅ |
| `pipeline/step5_compute_features.py` | Feature computation (montier_c2 bug FIXED) | ✅ |
| `scripts/modeling/run_feature_selection.py` | PSI → IC (HAC) → ICIR (FDR) → Spearman dedup | ✅ |
| `scripts/modeling/train_models.py` | Walk-forward LightGBM training (1y/3y/5y) | ✅ |
| `scripts/modeling/score_historical.py` | Load models → score all rows → write ml_* to parquet | ✅ |
| `scripts/modeling/generate_oof_scores.py` | OOF walk-forward scoring (ml_*_oof cols) | ✅ (not yet run) |
| `scripts/analysis/factor_research.py` | IC/ICIR factor research reports | ✅ |
| `scripts/quality/bias_audit.py` | 4-audit look-ahead/survivorship/overfit/FDR | ✅ |
| `scripts/_shared/backtester.py` | Walk-forward backtester + SPY benchmark | ⚠️ Partial |
| `scripts/data_io/push_to_hf.py` | Push data/models to HuggingFace Hub | ✅ |

---

## Key Files

| File | Purpose |
|---|---|
| `CLAUDE.md` | Architecture state, pre-task checklist, sync rules |
| `ROADMAP.md` | Phase plan with status rows |
| `CONTEXT.md` | This file |
| `data/historical_dataset_clean.parquet` | Main dataset (360 cols = 355 base + 5 OOF, 58,190 rows) |
| `models/model_meta.json` | Selected features per horizon (1y/3y/5y) + training stats |
| `reports/factor_research_*.csv` | IC/ICIR factor research per horizon |
| `docs/developer/phase-done-criteria.md` | Machine-checkable exit criteria per phase |
| `scripts/quality/check_sync.py` | Pre-commit doc sync validator |

---

## Session Log

| Date | Work Done |
|---|---|
| 2026-05-11 | Vision realignment; D1–D7 docs; ROADMAP.md; CONTEXT.md; vision memory saved |
| 2026-05-11 | Roadmap restructured to 16-step backbone; Phase B/C parked; full data audit run |
| 2026-05-13 | Full Phase A/B audit: montier_c2 100% null bug FIXED (ppe→ppe_net); CONTEXT.md rewritten; ROADMAP.md status sync; doc sweep; coverage depth task queued |
| 2026-05-13 | Phase A+B mechanically verified (59 PASS 0 FAIL); anti-drift process built (run_phase_checks.py); notebooks/06_ic_decay.ipynb executed with IC decay/half-life/regime/autocorr outputs |
| 2026-05-14 | Phase C mechanically verified: `run_phase_checks.py --phase C` → 30 PASS 0 FAIL 0 WARN. All 5 model horizons confirmed present, OOF scores in parquet, bias audit passed, backtest complete (CAGR +38.1%), alpha_registry.json has 8 signals (6 selected). ROADMAP.md + CONTEXT.md synced to reflect Phase C COMPLETE. |
| 2026-05-14 | Full A/B/C institutional audit: fixed Phase A FAIL (column count 355→360 in run_phase_checks.py); fixed architecture.md stale node (355→360 cols); fixed models.md LightGBM config (n_estimators 500→600, lr 0.05→0.03, num_leaves 31→63); added HF_TOKEN.md + *.token to .gitignore; embedded 20% ROI investment target into ROADMAP.md + CONTEXT.md; platform framing updated (screener + alpha generation platform). All phases A/B/C now pass. |
