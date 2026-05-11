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

| Market | Source | Status | Coverage |
|---|---|---|---|
| US | SEC EDGAR XBRL | ✅ Done | 7,418 tickers, 2008–2027 |
| KR | DART (free API key) | ⚠️ In progress | 251 tickers, 2015–2026 — snapshot exists, not merged |
| BR | CVM + brapi.dev | ⚠️ Thin | 57 tickers, 2010–2025 — usable, not merged |
| CA | TMX public API | ⚠️ Shallow | 2,005 tickers, 2021–2026 — 5 years only |
| EU | yfinance (Wikipedia index tickers) | ⚠️ Thin | 303 tickers, 2021–2026 |
| JP | yfinance Nikkei 225 (`jp_free` variant) | ⚠️ Thin | 122 tickers, 2021–2026 |

**V1 priority**: Validate US end-to-end first. Integrate KR once US alpha is confirmed. BR, CA, EU, JP as signal-count additions.

**Exit criteria**: US parquet is clean and passing Step 2 checks. At least one non-US market merged into `historical_dataset_clean.parquet`.

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
| **Data coverage verification** (20-year depth check per market) | ❌ Blocker | P0.5 — verify depth before trusting ICIR |

**Exit criteria**: `pit_validate.py` exits 0; `bias_audit.py` passes all four tests; coverage verified ≥ 10 years for US, ≥ 8 years for KR.

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

## Phase B — Research & Signals
> Goal: Statistically robust feature selection, trained models, alpha signals generated, backtest trustworthy.

---

### Step 5 — Feature Selection & Analysis

| Task | Status | File |
|---|---|---|
| PSI filter (regime stability) | ✅ Done | `scripts/train_models.py` |
| IC / ICIR ranking | ✅ Done | `scripts/train_models.py` |
| Spearman correlation deduplication | ✅ Done | `scripts/train_models.py` |
| IC analysis per feature | ⚠️ Partial | `scripts/factor_research.py` |
| Feature importance vs SHAP comparison | ⚠️ Partial | SHAP in `train_models.py`, not visualised |
| **Newey-West HAC standard errors** | ❌ Blocker P0.4 | Correct for autocorrelation in IC t-stats |
| **Fama-MacBeth cross-sectional standard errors** | ❌ Blocker P0.4 | — |
| **FDR correction (Benjamini-Hochberg)** | ❌ Blocker P0.4 | Prevent spurious feature selection |

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
| **`score_historical.py`** (write ml_1y/3y/5y to parquet) | ❌ Critical blocker P0.3 | Backtester is blind without ML scores |
| **SPY benchmark fix** | ❌ Blocker P0.2 | Current: universe mean. Needed: SPY for US, local index for others |
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

Priority order — each unblocks the next:

1. **Step 0** — Git and repo cleanup (branches, orphaned files, `.gitignore` audit)
2. **P0.3** — Build `scripts/score_historical.py` (ML scores must be in parquet; backtester is blind without them)
3. **P0.1** — Cross-sectional momentum to `pipeline/feature_library.py` (largest feature gap)
4. **P0.2** — Fix SPY benchmark in `scripts/backtester.py` (makes backtest numbers trustworthy)
5. **P0.4** — Newey-West / Fama-MacBeth / FDR in `scripts/train_models.py` (statistical robustness)
6. **P0.5** — Data coverage verification (confirm 20-year depth before trusting ICIR rankings)
7. After all P0 blockers resolved: run full backtest → validate US alpha → proceed to Phase B
