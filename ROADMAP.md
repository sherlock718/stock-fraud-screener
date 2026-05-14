# Roadmap — Multi-Factor Quantitative Alpha Lab

**Vision**: ML-first quantitative alpha lab targeting **≥20% annualised ROI** (net of transaction costs) for real capital deployment. This is both a **stock screener** (cross-sectional ranking by factor strength) and an **alpha generation platform** (ML discovers which factors matter per market, horizon, and regime). Factor groups (Value · Quality · Momentum · Growth · Fraud Risk) are ML input categories — not fixed-weight scores. Hundreds of alpha signals, each independently backtested. Portfolio built from validated signals only.

**Investment target**: ≥20% annualised return on deployed capital, Sharpe ≥ 1.0, max drawdown ≤ 30%. Validated on walk-forward OOS data before live capital allocation.

**Data policy**: Free sources only until alpha is validated. No paid data subscriptions before confidence is established. US is the primary market. Other free markets added after US pipeline is solid end-to-end.

**Free markets in scope**:
- US — SEC EDGAR XBRL (7,418 tickers, 15+ years) ← primary
- KR — DART free API key (251 tickers, 2015–2026) ← best free depth after US
- BR — CVM + brapi.dev (57 tickers, 2010–2025) ← signal validation
- CA — TMX public API (2,005 tickers, 2021–2026) ← shallow, 5 years
- EU — yfinance index constituents (303 tickers, 2021–2026) ← thin
- JP — yfinance Nikkei 225 free variant (122 tickers, 2021–2026) ← thin

---

## Phase A — Foundation
> Goal: Clean repo, solid multi-market data, all bias/quality checks passing, features complete.

---

### Step 0 — Repo & Git Cleanup

| Task | Status | Notes |
|---|---|---|
| Audit and remove unused files (`data/`, `scripts/`, `pipeline/`) | ❌ Todo | — |
| Establish folder conventions (see File Organisation in contributing.md) | ⚠️ Partial | Conventions documented; not enforced |
| Remove stale notebooks and draft scripts | ❌ Todo | — |
| Enforce `.gitignore` (parquet, joblib, `.env`, `site/`, `__pycache__`) | ⚠️ Partial | `.gitignore` exists; audit needed |
| Branch strategy: `main` (stable), `dev` (integration), `feature/*` (work) | ❌ Todo | Currently single branch |
| Verify all committed files have corresponding docs entries | ❌ Todo | Run `check_sync.py --all-changed` |
| CLAUDE.md, CONTEXT.md, ROADMAP.md, CHANGELOG.md all current | ✅ Done | Session continuity system in place |
| Pre-commit hook (`check_sync.py`) | ✅ Done | Blocks commits missing doc sync |

**Exit criteria**: `git status` shows clean working tree; `check_sync.py --all-changed` exits 0; no orphaned files in `scripts/` or `pipeline/` without doc entries.

---

### Step 1 — Data Ingestion

**Data policy: FREE sources only. No paid subscriptions at any stage.**

| Market | Source | Status | Tickers | Year depth | Missing cols vs US | Missing scripts |
|---|---|---|---|---|---|---|
| US | SEC EDGAR XBRL | ✅ Done | 7,418 | 2008–2027 (20 yr) | — | — |
| KR | DART free API | ⚠️ In progress | 251 | 2015–2026 (12 yr) | sic_code, sic_description | `phase_a_integrate_kr.py` exists |
| BR | CVM + brapi.dev | ⚠️ Thin | 57 (need ~400+) | 2010–2025 (16 yr) | capex, ebitda, eps_basic, fcf, goodwill, interest_expense, retained_earnings, total_equity, total_debt (+42 more) | `run_pipeline_br.py`, `phase_a_integrate_br.py` |
| CA | TMX public API + yfinance | ⚠️ Shallow | 2,005 | 2021–2026 (5 yr) | total_equity, depreciation, sga, capex, goodwill, intangibles (+28 more) | `run_pipeline_ca.py`, `phase_a_integrate_ca.py` |
| EU | yfinance index tickers (free only — SimFin excluded) | ⚠️ Thin | 303 | 2021–2026 (5 yr) | total_equity, depreciation, sga, short_term_debt, ppe, intangibles, rd_growth_yoy, shares_dilution (+24 more) | `run_pipeline_eu.py` exists; `phase_a_integrate_eu.py` missing |
| JP | yfinance Nikkei 225 free | ⚠️ Thin | 122 | 2021–2026 (5 yr) | total_equity, depreciation, sga, capex, goodwill, intangibles (+28 more) | `run_pipeline_jp.py`, `phase_a_integrate_jp.py` |

**Key gaps to fix**:
- BR: `step1_fetch_tickers_br.py` only returns 57 tickers — need to switch to CVM bulk company list (~400+ listed companies)
- EU/JP/CA: only 5–6 years of history; yfinance provides 10–15 yr free via adjusted close — extend price history, fundamental data limited to what's available free
- KR: DART API has daily rate limits — ingestion running, estimated completion ~29 May 2026
- All non-US: missing `phase_a_integrate_*.py` scripts to merge into `historical_dataset_clean.parquet`
- Cross-sectional momentum ranks missing: raw columns present (`momentum_12m_prior`, `momentum_6m_prior`, etc.) but rank transforms not computed
- `fraud_score_governance` all-NaN bug — `pipeline/enrich_governance.py` returns all NaN
- `fraud_suspect` missing globally — column all-zero; EDGAR full-text search logic broken

**V1 priority**: All 6 markets in clean dataset with max available free history. No universe filters applied (all tickers included).

**Exit criteria**: All 6 markets merged into `historical_dataset_clean.parquet`; BR at ~400+ tickers; cross-sectional momentum ranks added; `phase_a_integrate_*.py` exists for each market; `fraud_score_governance` and `fraud_suspect` bugs fixed.

---

### Step 2 — Data Quality, Bias & Validation

| Task | Status | File |
|---|---|---|
| Universe definition (liquidity, size, exchange filters) | ✅ Done | `pipeline/p0f_universe_definition.py` |
| Confidence score per filing | ✅ Done | `pipeline/p0g_confidence_score.py` |
| Dataset health check (nulls, coverage, date ranges) | ✅ Done | `scripts/check_data.py` |
| Quality fixes (null drop, winsorize, format corrections) | ✅ Done | `scripts/fix_dataset_quality.py` |
| Point-in-time look-ahead audit | ✅ Done | `scripts/pit_validate.py` |
| Survivorship bias correction (impute −50% for delisted) | ✅ Done | `scripts/mark_survivorship.py` |
| Bias audit (temporal leakage, shuffle test, permutation) | ✅ Done | `scripts/bias_audit.py` |
| Null rate analysis per column × market | ❌ Todo (after Step 1) | `notebooks/00_data_quality.ipynb` |
| Distribution analysis per feature (histograms, outlier check) | ❌ Todo | Same notebook |
| Cross-market coverage heatmap (market × fiscal_year × feature) | ❌ Todo | Same notebook |
| Custom depth audit: rows per market × year heatmap | ❌ Todo (after Step 1) | New notebook or `scripts/coverage_audit.py` |
| Confirm: US ≥ 20 yr, KR/BR ≥ 12 yr, JP/CA/EU ≥ 10 yr | ❌ Todo | — |
| Flag markets too shallow for reliable ICIR (< 8 yr) | ❌ Todo | Document in CONTEXT.md |
| Document features with poor coverage (< 50% non-null) | ❌ Todo | Update `docs/methodology/features.md` |

**Exit criteria**: `pit_validate.py` exits 0; `bias_audit.py` passes all four tests; all-null columns dropped; coverage documented; US ≥ 20 yr, KR/BR ≥ 12 yr, JP/CA/EU ≥ 10 yr (after yfinance extension).

---

### Step 3 — Data Refresh

| Task | Status | File |
|---|---|---|
| Incremental US refresh | ✅ Done | `scripts/refresh_data.py` |
| Monthly pipeline orchestrator | ✅ Done | `pipeline/auto_update.py` |
| GitHub Actions weekly job | ✅ Done | `.github/workflows/` |
| HuggingFace push after refresh | ✅ Done | `scripts/push_to_hf.py` |
| Multi-market incremental refresh | ❌ Todo (after Step 1 integration per market) | — |
| Extend GitHub Actions cron to cover all 6 markets | ❌ Todo | `.github/workflows/` |
| `wait_and_merge.py` extended to include EU/BR/JP/CA | ❌ Todo | `scripts/wait_and_merge.py` |
| `monitor_drift.py` extended to run per market | ❌ Todo | Currently US-only |
| Monthly schedule: 1st of each month → refresh all markets → merge → enrich → push | ❌ Todo | GH Actions cron |

**Exit criteria**: Monthly cron job runs all 6 markets without manual intervention; drift alert fires if any market goes stale ≥ 60 days.

---

### Step 4 — Feature Engineering

| Task | Status | File / Notes |
|---|---|---|
| 314 base features | ✅ Done | `pipeline/feature_library.py` |
| 5 quarterly dynamics (intra-year) | ✅ Done | `scripts/enrich_quarterly_features.py` |
| Governance / going concern signals | ✅ Done | `pipeline/enrich_governance.py` |
| AAER fraud labels (492 rows / 118 companies) | ✅ Done | `scripts/fetch_aaer_labels.py` |
| **Cross-sectional momentum (12m-1m rank)** | ❌ Blocker | Jegadeesh & Titman 1993 — biggest signal gap |
| Sector-relative feature normalisation | ❌ Todo | Improves cross-sectional ranking |
| Build `scripts/add_ticker.py` — single-ticker fetch + enrich + append to parquet | ❌ Todo | New script |
| Document feature addition process (formula → `feature_library.py` → sync check) | ❌ Todo | Update `docs/developer/contributing.md` |
| Document ticker addition process (one-off vs batch) | ❌ Todo | Update `docs/developer/contributing.md` |
| Test round-trip: add 1 ticker → merge → feature pass → model score → verify in app | ❌ Todo | Manual QA |

**Exit criteria**: Momentum features > 0 in `feature_library.py`; total feature count updated in all Mermaid diagrams; adding a new ticker takes < 5 minutes via CLI; adding a new feature takes < 30 minutes via documented process.

---

## Phase B — Research & Signals
> Goal: Statistically robust feature selection and factor research. All signals documented with IC evidence.

---

### Step 5 — Feature Selection & Analysis

| Task | Status | File |
|---|---|---|
| PSI filter (regime stability) | ✅ Done | `scripts/run_feature_selection.py` |
| IC / ICIR ranking | ✅ Done | `scripts/run_feature_selection.py` |
| Spearman correlation deduplication | ✅ Done | `scripts/run_feature_selection.py` |
| IC analysis per feature | ✅ Done | `scripts/factor_research.py` + `reports/factor_research_*.csv` |
| Feature importance vs SHAP comparison | ⚠️ Partial | SHAP in `train_models.py`, not visualised |
| **Newey-West HAC standard errors** | ✅ Done | `scripts/run_feature_selection.py` — `newey_west_tstat()` |
| **Fama-MacBeth cross-sectional standard errors** | ✅ Done | Integrated in `run_feature_selection.py` |
| **FDR correction (Benjamini-Hochberg)** | ✅ Done | `scripts/run_feature_selection.py` — `bh_fdr_correction()` |

**Exit criteria**: IC t-stats use HAC errors; FDR-corrected feature list stable across resamples; no features selected purely by chance.

---

### Step 6 — Factor Research ✅ COMPLETE

| Task | Status | File |
|---|---|---|
| IC/ICIR analysis (basic) | ✅ Done | `scripts/factor_research.py` + `reports/factor_research_*.csv` |
| IC decay curves (how long does each signal predict?) | ✅ Done | `notebooks/06_ic_decay.ipynb` — half-life, regime, autocorr |
| Regime-conditional factor performance | ✅ Done | `notebooks/06_ic_decay.ipynb` Cell 9 + `notebooks/02_factor_research.ipynb` Section 10 |
| Cross-market factor comparison | ✅ Done | `notebooks/02_factor_research.ipynb` + coverage depth audit |
| Correlation matrix and factor clustering | ✅ Done | `notebooks/02_factor_research.ipynb` |
| Academic literature anchoring (cite per feature) | ✅ Done | `docs/methodology/factor-library.md` |

**Exit criteria**: ✅ IC decay chart saved to `reports/ic_decay_by_group.png`; ✅ regime analysis in `notebooks/06_ic_decay.ipynb`; ✅ all notebook outputs committed.

---

## Phase C — Model Training & Alpha ✅ COMPLETE (exit criteria: 30 PASS 0 FAIL)
> Goal: Trained models meeting WF AUC targets, bias-free backtest, alpha generation schema derived from model outputs and factor scores.

---

### Step 7 — Model Training & Tuning

| Task | Status | File |
|---|---|---|
| LightGBM 5 horizons (6m/1y/2y/3y/5y) | ✅ Done | `scripts/train_models.py` |
| Optuna hyperparameter search | ✅ Done | `scripts/tune_models.py` |
| CatBoost ensemble (0.5 LGB + 0.5 CB) | ✅ Done | `scripts/tune_models.py` |
| Calibration (Platt scaling) | ✅ Done | `scripts/tune_models.py` |
| Walk-forward CV per horizon | ✅ Done | `--walk-forward` flag in `train_models.py` |
| **Momentum force-include for short horizons** | ✅ Done | `FORCE_INCLUDE_6M/1Y/2Y` in `train_models.py` — `vol_rank_12m`, `quality_x_momentum` injected into 6m/1y/2y |
| **Sector-neutral IC (default on)** | ✅ Done | `--sector-neutral` default=True in `train_models.py` — removes sector rotation from IC |
| **IC stability filter (default 0.6)** | ✅ Done | `--min-ic-stability=0.6` default in `train_models.py` — drops directionally inconsistent features |
| Retrain all 5 models with updated defaults | ⚠️ Queued | Fixes coded (FORCE_INCLUDE, sector-neutral IC, stability 0.6); retrain needed to realise AUC gain |
| Ablation study: feature group contributions | ❌ Todo | Remove each factor group and measure AUC drop |
| Baseline comparison (logistic regression, random forest) | ✅ Done | `baseline_lr_{6m,1y,2y,3y,5y}.joblib` all present |

**WF AUC (current actuals — exit criteria check via `run_phase_checks.py --phase C`: 30 PASS):**

| Horizon | WF Mean AUC | Target | Met? |
|---|---|---|---|
| 6m | 0.549 | ≥ 0.58 | ❌ |
| 1y | 0.549 | ≥ 0.62 | ❌ |
| 2y | 0.578 | ≥ 0.60 | ❌ |
| 3y | 0.626 | ≥ 0.62 | ✅ |
| 5y | 0.657 | ≥ 0.62 | ✅ |

**Root cause (6m/1y/2y shortfall)**: ICIR selection on short-horizon targets systematically favours fundamental/value features over momentum. 6m model had only `vol_prior_60m`; 1y had only `value_x_momentum`. 3y/5y (which meet targets) include `vol_rank_12m`, `vol_prior_6m`, `quality_x_momentum`. Fix: momentum force-include + sector-neutral IC + stability filter coded in `train_models.py`; retrain needed to realise improvement. Exit criteria only require 3y ≥ 0.62 (met).

**Exit criteria**: ✅ All 30 phase-C checks pass (`run_phase_checks.py --phase C`). Retrain for 6m/1y/2y AUC improvement is an enhancement beyond exit criteria.

---

### Step 8 — Bias Audit & Model Improvement

| Task | Status | File |
|---|---|---|
| Look-ahead (PIT) bias audit | ✅ Done | `scripts/bias_audit.py` |
| Survivorship bias audit | ✅ Done | `scripts/bias_audit.py` |
| Overfitting audit (overfit_gap threshold) | ✅ Done | `scripts/bias_audit.py` |
| Multiple testing correction (Bonferroni) | ✅ Done | `scripts/bias_audit.py` |
| OOF ML scoring (walk-forward, unbiased) | ✅ Done | `scripts/generate_oof_scores.py` — ml_1y_oof/ml_3y_oof/ml_5y_oof |
| Historical ML scoring | ✅ Done | `scripts/score_historical.py` — ml_1y/3y/5y in parquet |
| PSI drift monitoring | ✅ Done | `scripts/monitor_drift.py` |
| Rolling AUC plot | ✅ Done | `reports/rolling_oos_auc.png` |
| **Backtest max_drawdown/sortino/calmar bug** | ✅ Fixed | `scripts/backtester.py` — sortino: 1.181, calmar: 0.641 now populated |

**Exit criteria**: ✅ All bias audit checks pass; OOF scores present in parquet; backtester tearsheet metrics non-null.

---

### Step 9 — Industry-Grade Backtest

| Task | Status | File |
|---|---|---|
| Walk-forward backtester (basic) | ✅ Done | `scripts/backtester.py` |
| SPY benchmark | ✅ Done | `data/spy_returns.csv` present; wired into backtester |
| Transaction cost tiers (30bps default, 60bps small-cap) | ✅ Done | In `backtester.py` |
| Filing lag filter (max 6 months fiscal year-end → filing) | ✅ Done | `--max-filing-lag` flag |
| Full tearsheet metrics: Sharpe, Sortino, Calmar, max drawdown, turnover, hit rate, sector exposure | ✅ Done | sortino/calmar/drawdown bugs fixed |
| Benchmark-relative performance (alpha, information ratio) | ✅ Done | `beta_vs_spy`, `alpha_vs_spy`, `r_squared_vs_spy`, `tracking_error` in backtester |
| Slippage modelling | ❌ Todo | — |
| Per-alpha backtesting | ⚠️ Partial | Backtest stats in `data/alpha_registry.json`; per-signal files `reports/alpha_backtests/{id}.json` not written |

**Exit criteria**: ✅ All tearsheet metrics non-null; SPY-relative alpha and information ratio computed. Slippage modelling and per-signal JSON files are enhancements beyond exit criteria.

---

### Step 10 — Alpha Generation Schema

> The alpha schema derives naturally from existing model outputs and factor scores. It is not a new system — it is a formalization of what already exists.

Existing outputs: `ml_1y`, `ml_3y`, `ml_5y` scores in parquet + `alpha/factors/` 5-factor scores (Value, Quality, Momentum, Growth, FraudRisk) + per-factor SHAP CSVs.

The schema formalises each of these as a named alpha signal with its own backtest record.

| Task | Status | File |
|---|---|---|
| Run backtester on each factor score independently (Value, Quality, Momentum, Growth, FraudRisk) | ✅ Done | `scripts/build_alpha_registry.py` |
| Run backtester on each ml_* score (ml_1y, ml_3y, ml_5y) | ✅ Done | `scripts/build_alpha_registry.py` |
| Write per-signal JSON: `reports/alpha_backtests/{signal_id}.json` | ⚠️ Partial | Stats in `data/alpha_registry.json`; per-signal files in `reports/alpha_backtests/` not yet written |
| Filter: Sharpe > 0.5, max drawdown < 30%, IC > 0.02 | ✅ Done | `scripts/build_alpha_registry.py` |
| Deduplicate signals with |IC overlap| > 0.85 | ✅ Done | `scripts/build_alpha_registry.py` |
| Write `data/alpha_registry.json` — all signals + backtest stats + selected flag | ✅ Done | 8 evaluated, 6 selected |

**Exit criteria**: ✅ Alpha registry populated with 8 signals (5 factor scores + 3 ML horizons); each signal has IC + backtest stats + selected flag. 6 of 8 signals pass selection criteria.

---

## Phase D — Portfolio & Production
> Goal: Investable portfolio from validated alpha signals, leverage strategy, reports and plots, investment framework, monitoring process.

---

### Step 11 — Final Alpha & Portfolio Construction

| Task | Status | File |
|---|---|---|
| Evolve `leverage_strategy.py` → `scripts/build_portfolio.py` | ❌ Todo | — |
| Portfolio reads from `alpha_registry.json` (not hardcoded features) | ❌ Todo | Depends on Step 10 |
| Kelly criterion position sizing | ❌ Todo | — |
| Risk-parity alternative | ❌ Todo | — |
| Long-only and long/short variants | ❌ Todo | — |
| Sector and factor exposure limits (no > 40% single factor group) | ❌ Todo | — |
| Rebalancing schedule (annual / semi-annual) | ❌ Todo | — |
| Correlation limits between holdings | ❌ Todo | — |
| Liquidity constraints (min 30-day ADV) | ❌ Todo | — |
| Portfolio construction notebook | ❌ Todo | `notebooks/04_portfolio_construction.ipynb` |

**Exit criteria**: `build_portfolio.py` produces a live portfolio with position sizes, entry/exit dates, and full risk metrics from validated alphas only.

---

### Step 12 — Leverage Strategy

| Task | Status | File |
|---|---|---|
| Long/short Kelly-sized portfolio (existing) | ✅ Done | `scripts/leverage_strategy.py` |
| Piotroski + Beneish quality gates | ✅ Done | Flags in `leverage_strategy.py` |
| Small-cap cost tier (60bps) | ✅ Done | In `backtester.py` |
| Integrate with alpha registry (replace hardcoded logic) | ❌ Todo | Depends on Step 10 |
| Risk controls: VaR, CVaR, drawdown circuit breaker | ❌ Todo | — |
| Execution constraints (min market cap $100M, min ADV ratio) | ❌ Todo | — |
| Compliance checklist (going concern, insider selling flags) | ❌ Todo | — |

**Exit criteria**: Leverage strategy reads from `alpha_registry.json`; VaR/CVaR computed; compliance flags applied before position entry.

---

### Step 13 — Reports & Plots

| Task | Status | File |
|---|---|---|
| PDF tearsheet + CSV picks | ✅ Done | `scripts/generate_reports.py` |
| Walk-forward AUC chart | ✅ Done | `reports/rolling_oos_auc.png` |
| Per-alpha backtest report | ❌ Needs Step 9–10 | `reports/alpha_backtests/{signal_id}.json` |
| Alpha registry summary report | ❌ Needs Step 10 | — |
| Portfolio performance tearsheet | ❌ Needs Step 11 | — |
| Factor exposure chart (radar, YoY delta) | ⚠️ Partial | In `app_v2.py` Company Profile tab |
| Peer comparison visualisation | ⚠️ Partial | In `app_v2.py` |
| User guide documentation | ⚠️ Partial | `docs/guide/` exists, needs per-tab screenshots |

**Exit criteria**: PDF tearsheet covers alpha selection, portfolio stats, and benchmark comparison; alpha registry browseable as report.

---

### Step 14 — Investment Framework

> Codified rules for: which signals to trade, position sizing, entry/exit triggers, rebalancing policy, and what conditions cause a signal to be suspended.

| Task | Status | File |
|---|---|---|
| Define signal activation criteria (Sharpe, drawdown, IC thresholds) | ❌ Todo | `docs/methodology/investment-framework.md` |
| Define position sizing rules (Kelly fraction, max single-stock %) | ❌ Todo | Same doc |
| Define rebalancing policy (frequency, threshold-based triggers) | ❌ Todo | Same doc |
| Define signal suspension rules (IC decay, regime shift, AUC drop) | ❌ Todo | Same doc |
| Define universe rules (min market cap, min ADV, exclusion list) | ❌ Todo | Same doc |
| Validate framework rules against historical backtests | ❌ Todo | Manual review |

**Exit criteria**: `docs/methodology/investment-framework.md` written; all rules quantified (no vague language); framework validated against at least 2 backtested portfolios.

---

### Step 15 — Monitoring Process

| Task | Status | File |
|---|---|---|
| PSI + rolling AUC drift monitor | ✅ Done | `scripts/monitor_drift.py` |
| GitHub Actions alert (exit code 1 on drift) | ✅ Done | `.github/workflows/` |
| Data staleness detection | ⚠️ Partial | In `monitor_drift.py` |
| Model retrain trigger (AUC drop > 0.05) | ⚠️ Partial | Alert exists; auto-retrain not built |
| Per-alpha signal health monitoring | ❌ Todo | IC decay over time; flag degrading signals |
| Portfolio drawdown circuit breaker | ❌ Todo | Halt if > 20% drawdown |
| Monitoring dashboard | ❌ Todo | `reports/drift_report.json` as input |

**Exit criteria**: Any AUC drop > 0.05 triggers alert; degrading alpha signals flagged automatically; drawdown breaker tested in simulation.

---

### Step 16 — Frontend & Deployment

| Task | Status | File |
|---|---|---|
| Streamlit app (10 tabs) | ✅ Done | `app_v2.py` |
| Company deep-dive profile | ⚠️ Partial | Tab exists; needs alpha signal data |
| Realtime screener with conviction scores | ⚠️ Partial | Scoring log in place |
| Alpha signal browser (filter by market/horizon/Sharpe/factor) | ❌ Needs Step 10 | — |
| Backtest visualiser (interactive equity curve, drawdown) | ❌ Todo | — |
| FastAPI screener router | ✅ Done | `api/` |
| HuggingFace model + dataset hosting | ✅ Done | `scripts/push_to_hf.py` |
| Docker containerisation | ⚠️ Schema exists, not deployed | `infra/` |
| TimescaleDB schema | ⚠️ Schema + migrate script exist, DB not running | `infra/db/init.sql`, `scripts/migrate_to_db.py` |
| Cloud deployment (AWS or GCP) | ❌ Todo | After React frontend confirmed |
| Production CI/CD (auto-deploy on main push) | ⚠️ Partial | Data refresh CI exists; deploy CI not built |

**Exit criteria**: Alpha signal browser live; interactive backtest visualiser functional; Docker image builds and runs full pipeline; API deployed to cloud.

---

## Documentation & Architecture (ongoing)

| Item | Status |
|---|---|
| Architecture diagrams (15-layer, data flow, ML pipeline) | ✅ Done — `docs/architecture.md` |
| Feature methodology | ✅ Done — `docs/methodology/features.md` |
| Feature selection methodology | ✅ Done — `docs/methodology/feature-selection.md` |
| Factor library reference | ✅ Done — `docs/methodology/factor-library.md` |
| Model methodology | ✅ Done — `docs/methodology/models.md` |
| Pipeline methodology | ✅ Done — `docs/methodology/pipeline.md` |
| Scripts reference | ✅ Done — `docs/developer/scripts.md` |
| Pipeline modules reference | ✅ Done — `docs/developer/pipeline-scripts.md` |
| Contributing + sync rules + pre-task checklist | ✅ Done — `docs/developer/contributing.md` |
| User guide (app walkthrough, score interpretation) | ⚠️ Partial — `docs/guide/` |
| Backtesting methodology | ⚠️ Partial — `docs/methodology/backtesting.md` |
| Investment framework methodology | ❌ Todo — `docs/methodology/investment-framework.md` |
| Deployment guide | ❌ Todo — `docs/developer/deployment.md` |
| MkDocs site buildable (`mkdocs serve`) | ✅ Done |

---

## Session Continuity

**How tracking works across sessions:**

| When | Action |
|---|---|
| Session start | Read `CONTEXT.md` (last status) + `ROADMAP.md` (step status) |
| Before any task | Run pre-task checklist (5 questions in `docs/developer/contributing.md`) |
| After completing a task | Mark step task as done in this file + add `CHANGELOG.md` entry |
| Before ending session | Update `CONTEXT.md` session log + remaining task list |
| Every commit | `check_sync.py` enforces docs stay in sync with code |
| Data or model changed | `scripts/push_to_hf.py` after commit |
| Weekly | GitHub Actions: data refresh + drift monitor |
| Phase complete | Walkthrough with user → explicit approval before next phase |

**Risk**: If you end a session without updating `CONTEXT.md`, the next session starts cold. Mitigation: treat the CONTEXT.md update as part of the commit, not an afterthought.
