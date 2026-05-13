# Roadmap — Multi-Factor Quantitative Alpha Lab

**Vision**: ML-first quantitative alpha lab. Factor groups (Value · Quality · Momentum · Growth · Fraud Risk) are ML input categories — not fixed-weight scores. ML discovers which factors matter per market, horizon, and segment. Hundreds of alpha signals, each independently backtested. Portfolio built from validated signals only.

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
> Goal: Clean repo, solid US data, all bias/quality checks passing, features complete.

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

**V1 priority**: All 6 markets in clean dataset with max available free history. No universe filters applied (all tickers included).

**Exit criteria**: All 6 markets merged into `historical_dataset_clean.parquet`; BR at ~400+ tickers; cross-sectional momentum ranks added; `phase_a_integrate_*.py` exists for each market.

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
| **Data coverage verification** (depth check per market) | ❌ Do AFTER Stage 1 | P0.5 — EU/JP/CA only 5–6 yr now; check passes only after history extended |

**Exit criteria**: `pit_validate.py` exits 0; `bias_audit.py` passes all four tests; coverage verified ≥ 20 yr US, ≥ 12 yr KR/BR, ≥ 10 yr JP/CA/EU (after yfinance extension).

---

### Step 3 — Data Refresh

| Task | Status | File |
|---|---|---|
| Incremental US refresh | ✅ Done | `scripts/refresh_data.py` |
| Monthly pipeline orchestrator | ✅ Done | `pipeline/auto_update.py` |
| GitHub Actions weekly job | ✅ Done | `.github/workflows/` |
| HuggingFace push after refresh | ✅ Done | `scripts/push_to_hf.py` |
| Multi-market incremental refresh | ❌ Todo | After Step 1 integration per market |

**Exit criteria**: GitHub Actions weekly job runs without manual intervention; `monitor_drift.py` alerts on data staleness.

---

### Step 4 — Feature Engineering

| Task | Status | File / Notes |
|---|---|---|
| 314 base features | ✅ Done | `pipeline/feature_library.py` |
| 5 quarterly dynamics (intra-year) | ✅ Done | `scripts/enrich_quarterly_features.py` |
| Governance / going concern signals | ✅ Done | `pipeline/enrich_governance.py` |
| AAER fraud labels (492 rows / 118 companies) | ✅ Done | `scripts/fetch_aaer_labels.py` |
| **Cross-sectional momentum (12m-1m rank)** | ❌ Blocker P0.1 | Jegadeesh & Titman 1993 — biggest signal gap |
| Sector-relative feature normalisation | ❌ Todo | Improves cross-sectional ranking |
| Earnings revision features | ❌ Phase B | Needs consensus estimate source |

**Exit criteria**: Momentum features > 0 in `feature_library.py`; total feature count updated in all Mermaid diagrams.

---

## Dataset Completion Plan — 5 Stages
> Execute these stages in order before Phase B. All stages use free data sources only.

---

### Stage 1 — Complete the Dataset (current priority)

Goal: All 6 markets merged, all features present, no universe filters.

| Task | Status | Notes |
|---|---|---|
| Add cross-sectional momentum rank transforms (`momentum_12m_rank`, `momentum_6m_rank`, `momentum_3m_rank`, `vol_rank_12m`) | ✅ Done | Present in dataset (7.9% null; 5.0% null for composite) |
| Fix BR ticker expansion: switch `step1_fetch_tickers_br.py` to CVM bulk list | ❌ Todo | Currently 55 tickers; CVM has ~400+ listed companies |
| Build `scripts/run_pipeline_br.py` | ✅ Done | Exists |
| Build `scripts/run_pipeline_jp.py` | ✅ Done | Exists |
| Build `scripts/run_pipeline_ca.py` | ✅ Done | Exists |
| Build `pipeline/phase_a_integrate_eu.py` | ✅ Done | Exists |
| Build `pipeline/phase_a_integrate_br.py` | ✅ Done | Exists |
| Build `pipeline/phase_a_integrate_jp.py` | ✅ Done | Exists |
| Build `pipeline/phase_a_integrate_ca.py` | ✅ Done | Exists |
| Extend EU yfinance fundamental history (free tier only) | ❌ Todo | No SimFin; use yfinance + any free EDGAR-equivalent for EU |
| Fix `fraud_score_governance` all-NaN bug | ❌ Todo | `pipeline/enrich_governance.py` returns all NaN |
| Fix `fraud_suspect` missing globally | ❌ Todo | Column all-zero; EDGAR full-text search logic broken |
| Add missing derived features: `working_capital`, `net_debt`, `price_to_book`, `accruals_ratio` | ❌ Todo | Easy derivations; add to `feature_library.py` |
| Run `fix_dataset_quality.py` + `mark_survivorship.py` after all markets merged | ❌ Todo | Final clean pass |
| KR DART ingestion complete | ⏳ Running | Daily API rate limit; ETA ~29 May 2026 |

**Exit criteria**: `historical_dataset_clean.parquet` contains all 6 markets; BR ~400+ tickers; momentum ranks present; all `phase_a_integrate_*.py` scripts exist and tested.

---

### Stage 2 — Coverage Depth Check
> **Do NOT run this stage until Stage 1 is complete.** EU/JP/CA currently only 5–6 yr — check will fail now.

| Task | Status | File |
|---|---|---|
| Run `pit_validate.py` per market | ❌ Todo (after Stage 1) | `scripts/pit_validate.py` |
| Custom depth audit: rows per market × year heatmap | ❌ Todo | New notebook or `scripts/coverage_audit.py` |
| Confirm: US ≥ 20 yr, KR/BR ≥ 12 yr, JP/CA/EU ≥ 10 yr | ❌ Todo | — |
| Flag markets too shallow for reliable ICIR (< 8 yr) | ❌ Todo | Document in CONTEXT.md which markets to exclude from model training |

**Exit criteria**: All markets meet minimum year depth; shallow-market exclusions documented; `pit_validate.py` exits 0.

---

### Stage 3 — EDA and Data Quality

| Task | Status | File |
|---|---|---|
| Null rate analysis per column × market | ❌ Todo | `notebooks/00_data_quality.ipynb` |
| Distribution analysis per feature (histograms, outlier check) | ❌ Todo | Same notebook |
| Cross-market coverage heatmap (market × fiscal_year × feature) | ❌ Todo | Same notebook |
| Run `bias_audit.py` on full merged dataset | ❌ Todo | `scripts/bias_audit.py` |
| Run `fix_dataset_quality.py` — drop all-null cols, winsorize, format fixes | ❌ Todo | `scripts/fix_dataset_quality.py` |
| IC/ICIR analysis per feature (basic factor research) | ❌ Todo | `scripts/factor_research.py` |
| Document features with poor coverage (< 50% non-null) | ❌ Todo | Update `docs/methodology/features.md` |

**Exit criteria**: Notebook committed; all-null columns dropped; coverage documented; no temporal leakage in bias audit.

---

### Stage 4 — Monthly Data Update Schedule

| Task | Status | File |
|---|---|---|
| Extend GitHub Actions cron to cover all 6 markets | ❌ Todo | `.github/workflows/` |
| Per-market refresh scripts (`run_pipeline_br/jp/ca.py`) | ❌ Needs Stage 1 | — |
| `wait_and_merge.py` extended to include EU/BR/JP/CA | ❌ Todo | `scripts/wait_and_merge.py` |
| HuggingFace push after every successful multi-market merge | ❌ Todo | `scripts/push_to_hf.py` |
| `monitor_drift.py` extended to run per market | ❌ Todo | Currently US-only |
| Monthly schedule: 1st of each month → refresh all markets → merge → enrich → push | ❌ Todo | GH Actions cron |

**Exit criteria**: Monthly cron job runs all 6 markets without manual intervention; drift alert fires if any market goes stale ≥ 60 days.

---

### Stage 5 — Process: Add New Ticker or Feature

| Task | Status | File |
|---|---|---|
| Build `scripts/add_ticker.py` — single-ticker fetch + enrich + append to parquet | ❌ Todo | New script |
| Document feature addition process (formula → `feature_library.py` → sync check) | ❌ Todo | Update `docs/developer/contributing.md` |
| Document ticker addition process (one-off vs batch) | ❌ Todo | Update `docs/developer/contributing.md` |
| Add column-addition checklist to CLAUDE.md Change Checklist | ❌ Todo | `CLAUDE.md` |
| Test round-trip: add 1 ticker → merge → feature pass → model score → verify in app | ❌ Todo | Manual QA |

**Exit criteria**: Adding a new ticker takes < 5 minutes via CLI; adding a new feature takes < 30 minutes via documented process; `check_sync.py` catches doc gaps.

---

## Phase B — Research & Signals
> Goal: Statistically robust feature selection, trained models, alpha signals generated, backtest trustworthy.

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

### Step 6 — Factor Research

| Task | Status | File |
|---|---|---|
| IC/ICIR analysis (basic) | ⚠️ Partial | `scripts/factor_research.py` |
| IC decay curves (how long does each signal predict?) | ❌ Todo | `notebooks/01_feature_ic_analysis.ipynb` |
| Regime-conditional factor performance | ❌ Todo | `notebooks/02_regime_analysis.ipynb` |
| Cross-market factor comparison | ❌ Todo | `notebooks/03_cross_market_factors.ipynb` |
| Correlation matrix and factor clustering | ❌ Todo | — |
| Academic literature anchoring (cite per feature) | ⚠️ Partial | `docs/methodology/factor-library.md` done |

**Exit criteria**: IC decay chart for each factor group; regime analysis identifies which factors invert in bear/inflation regimes; notebook outputs committed.

---

### Step 7 — Model Selection & Tuning

| Task | Status | File |
|---|---|---|
| LightGBM 1y/3y/5y (current) | ✅ Done | `scripts/train_models.py` |
| Optuna hyperparameter search | ✅ Done | `scripts/tune_models.py` |
| CatBoost ensemble (0.5 LGB + 0.5 CB) | ✅ Done | `scripts/tune_models.py` |
| Baseline comparison (logistic regression, random forest) | ❌ Todo | Validate ML adds value over simpler models |
| Walk-forward CV per model type | ⚠️ Partial | `--walk-forward` flag exists in `train_models.py` |
| Calibration (Platt scaling) | ✅ Done | `scripts/tune_models.py` |
| Ablation study: feature group contributions | ❌ Todo | Remove each factor group and measure AUC drop |
| Model performance table (Val/Test/WF AUC) | ✅ Tracked | `docs/methodology/models.md` |

**Current WF AUC**: 1y 0.553 ❌ · 3y 0.643 ✅ · 5y 0.597 ❌ (target ≥ 0.62)

**Exit criteria**: Best model per horizon selected with statistical justification; WF AUC ≥ 0.62 on at least 2 of 3 horizons; ablation confirms ML beats logistic regression baseline.

---

### Step 8 — Alpha Signal Generation

| Task | Status | File |
|---|---|---|
| Alpha signal schema | ❌ Todo | `alpha/signals/base.py` |
| Alpha registry | ❌ Todo | `alpha/signals/registry.py` |
| Alpha generation loop (market × horizon × segment × feature_subset × model_type) | ❌ Todo | `scripts/generate_alphas.py` |
| Initial run: US × 1y × all features | ❌ Todo | First alpha library |

**Exit criteria**: Alpha registry populated with ≥ 50 signals from US; each signal has signal_id, horizon, market, features_used, model_type, train/val AUC.

---

### Step 9 — Industry-Grade Backtest Framework

| Task | Status | File |
|---|---|---|
| Walk-forward backtester (basic) | ⚠️ Partial | `scripts/backtester.py` |
| **`score_historical.py`** (write ml_1y/3y/5y to parquet) | ✅ Done | ml_1y/3y/5y present in parquet at 0% null |
| **SPY benchmark fix** | ✅ Done | `data/spy_returns.csv` present; wired into backtester |
| Per-alpha backtesting | ❌ Todo | `scripts/backtest_alpha.py` |
| Transaction cost tiers (30bps default, 60bps small-cap) | ✅ Done | In `backtester.py` |
| Slippage modelling | ❌ Todo | — |
| Full tearsheet metrics: Sharpe, Sortino, Calmar, max drawdown, turnover, hit rate, sector exposure | ⚠️ Partial | Some in `backtester.py`; not all |
| Benchmark-relative performance (alpha, information ratio) | ❌ Todo | Needs P0.2 first |
| Rolling OOS AUC plot | ✅ Done | `reports/rolling_oos_auc.png` |
| Filing lag filter (max 6 months fiscal year-end → filing) | ✅ Done | `--max-filing-lag` flag |

**Exit criteria**: `score_historical.py` built and run; SPY benchmark in place; all tearsheet metrics computed; backtester output is trustworthy and auditable.

---

### Step 10 — Final Alpha Selection

| Task | Status | File |
|---|---|---|
| Alpha filter (Sharpe > 0.5, drawdown < 30%, IC > 0.02) | ❌ Todo | `scripts/select_alphas.py` |
| Alpha deduplication (remove signals with |r| > 0.85 IC overlap) | ❌ Todo | — |
| Ensemble: ML learns to combine surviving alphas | ❌ Todo | — |
| Alpha registry finalization: `data/alpha_registry.json` | ❌ Todo | All signals + backtest stats + selected flag |

**Exit criteria**: ≥ 10 alphas pass selection filter from US; alpha registry written; per-alpha backtest JSON exists in `reports/alpha_backtests/`.

---

## Phase C — Portfolio & Production
> Goal: Investable portfolio, production-ready infrastructure, interactive frontend.

---

### Step 11 — Portfolio Construction

| Task | Status | File |
|---|---|---|
| Evolve `leverage_strategy.py` → `scripts/build_portfolio.py` | ❌ Todo | — |
| Kelly criterion position sizing | ❌ Todo | — |
| Risk-parity alternative | ❌ Todo | — |
| Long-only and long/short variants | ❌ Todo | — |
| Sector and factor exposure limits | ❌ Todo | No > 40% single factor group |
| Rebalancing schedule (annual / semi-annual) | ❌ Todo | — |
| Correlation limits between holdings | ❌ Todo | — |
| Liquidity constraints (min 30-day ADV) | ❌ Todo | — |
| Portfolio construction notebook | ❌ Todo | `notebooks/04_portfolio_construction.ipynb` |

**Exit criteria**: `build_portfolio.py` produces a live portfolio with position sizes, entry/exit dates, and full risk metrics from validated alphas only.

---

### Step 12 — Leverage Trading / Small-Cap Strategy

| Task | Status | File |
|---|---|---|
| Long/short Kelly-sized portfolio (existing) | ✅ Done | `scripts/leverage_strategy.py` |
| Piotroski + Beneish quality gates | ✅ Done | Flags in `leverage_strategy.py` |
| Small-cap cost tier (60bps) | ✅ Done | In `backtester.py` |
| Integrate with alpha registry (replace hardcoded logic) | ❌ Todo | Depends on Step 10 |
| Risk controls: VaR, CVaR, drawdown circuit breaker | ❌ Todo | — |
| Execution constraints (min market cap $100M, min ADV ratio) | ❌ Todo | — |
| Compliance checklist (going concern, insider selling flags) | ❌ Todo | — |

**Exit criteria**: Leverage strategy reads from `alpha_registry.json`, not hardcoded features; VaR/CVaR computed; compliance flags applied before position entry.

---

### Step 13 — Reporting & Visualisation

| Task | Status | File |
|---|---|---|
| PDF tearsheet + CSV picks | ✅ Done | `scripts/generate_reports.py` |
| Walk-forward AUC chart | ✅ Done | `reports/rolling_oos_auc.png` |
| Per-alpha backtest report | ❌ Needs Step 9–10 | `reports/alpha_backtests/{signal_id}.json` |
| Alpha registry summary report | ❌ Needs Step 8–10 | — |
| Portfolio performance tearsheet | ❌ Needs Step 11 | — |
| Factor exposure chart (radar, YoY delta) | ⚠️ Partial | In `app_v2.py` Company Profile tab |
| Peer comparison visualisation | ⚠️ Partial | In `app_v2.py` |
| User guide documentation | ⚠️ Partial | `docs/guide/` exists, needs per-tab screenshots |

**Exit criteria**: PDF tearsheet covers alpha selection, portfolio stats, and benchmark comparison; alpha registry browseable as report.

---

### Step 14 — Monitoring

| Task | Status | File |
|---|---|---|
| PSI + rolling AUC drift monitor | ✅ Done | `scripts/monitor_drift.py` |
| GitHub Actions alert (exit code 1 on drift) | ✅ Done | `.github/workflows/` |
| Data staleness detection | ⚠️ Partial | In `monitor_drift.py` |
| Model retrain trigger (AUC drop > 0.05) | ⚠️ Partial | Alert exists; auto-retrain not built |
| Per-alpha signal health monitoring | ❌ Todo | IC decay over time; flag degrading signals |
| Portfolio drawdown circuit breaker | ❌ Todo | Halt if > 20% drawdown |
| Monitoring dashboard | ❌ Todo | Grafana or custom; `reports/drift_report.json` as input |

**Exit criteria**: Any AUC drop > 0.05 triggers alert; degrading alpha signals flagged automatically; drawdown breaker tested in simulation.

---

### Step 15 — Frontend (Interactive UI)

| Task | Status | File |
|---|---|---|
| Streamlit app (10 tabs) | ✅ Done | `app_v2.py` |
| Company deep-dive profile | ⚠️ Partial | Tab exists; needs alpha signal data |
| Factor radar (5 dimensions, YoY delta) | ⚠️ Partial | In `app_v2.py` |
| Realtime screener with conviction scores | ⚠️ Partial | Scoring log in place |
| Alpha signal browser (filter by market/horizon/Sharpe/factor) | ❌ Needs Step 8 | — |
| Backtest visualiser (interactive equity curve, drawdown) | ❌ Todo | — |
| React/Next.js frontend (replaces Streamlit) | ❌ Phase C final | Build confidence first |
| FastAPI screener router | ✅ Done | `api/` |

**Exit criteria**: Alpha signal browser live; interactive backtest visualiser functional; company deep-dive shows per-signal contributions.

---

### Step 16 — Deployment Infrastructure

| Task | Status | File |
|---|---|---|
| HuggingFace model + dataset hosting | ✅ Done | `scripts/push_to_hf.py` |
| Docker containerisation | ⚠️ Schema exists, not deployed | `infra/` |
| TimescaleDB schema | ⚠️ Schema + migrate script exist, DB not running | `infra/db/init.sql`, `scripts/migrate_to_db.py` |
| TimescaleDB loaded with current dataset | ❌ Todo | Needs running DB |
| Cloud deployment (AWS or GCP) | ❌ Todo | After React frontend confirmed |
| Production CI/CD (auto-deploy on main push) | ⚠️ Partial | Data refresh CI exists; deploy CI not built |
| Monitoring dashboard (Grafana or custom) | ❌ Todo | — |

**Exit criteria**: Docker image builds and runs full pipeline; TimescaleDB populated; API deployed to cloud; monitoring dashboard live.

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

---

## Immediate Next Actions

Priority order — complete Stage 1 entirely before moving to Phase B blockers:

**Stage 1 (dataset completion — do first):**
1. Add cross-sectional momentum rank features to `pipeline/feature_library.py`
2. Fix BR ticker expansion in `scripts/run_pipeline_br.py` (CVM bulk list, ~400 tickers)
3. Build `pipeline/phase_a_integrate_eu.py` + `br.py` + `jp.py` + `ca.py`
4. Build `scripts/run_pipeline_br.py`, `run_pipeline_jp.py`, `run_pipeline_ca.py`
5. Extend JP/CA yfinance history (free, 10–15 yr back)
6. Fix `fraud_score_governance` NaN bug + `fraud_suspect` missing
7. Run full merge → `fix_dataset_quality.py` → `mark_survivorship.py`

**Stage 2 (coverage depth check — after Stage 1):**
8. Run `pit_validate.py` + custom depth audit across all 6 markets

**Phase B blockers (after Stage 2):**
9. **P0.3** — Build `scripts/score_historical.py` (write `ml_1y/3y/5y` to parquet; backtester is blind without this)
10. **P0.2** — Fix SPY benchmark in `scripts/backtester.py`
11. **P0.4** — Newey-West HAC / Fama-MacBeth / FDR in `scripts/train_models.py`

**Stage 3–5 (after Phase B setup):**
12. EDA/QC notebook + bias audit on full merged dataset
13. Monthly update schedule (GitHub Actions all 6 markets)
14. `scripts/add_ticker.py` + documented add-feature process
