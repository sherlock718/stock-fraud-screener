# Session 30: Quality, Orchestration & Support Packages Orientation

> Final orientation session. Covers: `quality/`, `workflows/`, `tests/`, `fraud/`, `portfolio/`, `data_io/`, and CI.

---

## 1. Quality Checks Inventory

### 1.1 Scripts & What They Validate

| Script | Purpose | When Run | Gate Type |
|--------|---------|----------|-----------|
| `quality/check_data.py` | Dataset shape, value ranges, nulls, time-series gaps, core ML feature nulls, cross-market sanity, post-step6 fixes | After every pipeline rebuild | HARD — exit 1 on failure |
| `quality/test_dataset_quality.py` | 10-section test suite: schema, structural, market coverage, fill rates, distributions, fraud labels, forward returns, growth winsorization, ML score exclusion, PIT leakage | After rebuild, CI mandatory | HARD |
| `quality/bias_audit.py` | Survivorship bias, look-ahead (filed_date < period_end), overfitting gap (val_auc vs WF), FX adjustment, multiple testing correction, regression model contamination audit | CI (--ci = hard fail on look-ahead only) | MIXED — look-ahead=HARD, rest=WARN |
| `quality/pit_validate.py` | Filing lag distribution, formation quarter, sector percentile look-ahead, ML training look-ahead, forward-return anchor | Manual / research | ADVISORY |
| `quality/monitor_drift.py` | PSI per feature, score distribution shift, AUC degradation, IC decay (rolling 3y/6y/12y), drawdown circuit breaker | Weekly CI (monitor_drift.yml) | ALERT — exit 1 if PSI>0.20 or AUC drops>0.05 |
| `quality/check_sync.py` | Architecture doc sync — enforces CLAUDE.md Change Checklist rules on staged files | Pre-commit (--warn-only) | SOFT — warning only |
| `quality/validate_feature_contract.py` | Group-level column presence check: Phase B (pipeline) vs Phase C (ML layer) | Post-enrichment orchestrator | HARD for Phase B, optional for Phase C |
| `quality/run_phase_checks.py` | Mechanistic Phase A/B/C done-criteria verifier (dataset shape, feature library, feature selection, factor research, models, backtest) | Manual / milestone gate | EXIT 1 if any FAIL |
| `quality/check_model_staleness.py` | Model freshness vs data freshness. Warns if model_meta.json older than dataset. `--strict` exits 1. | Manual / CI (optional) | MIXED — warn default, HARD with `--strict` |

### 1.2 What's Validated vs What's NOT

| Validated | Not Validated |
|-----------|---------------|
| Schema presence (required columns) | Column semantic correctness (are values plausible given the company?) |
| Forward return winsorization caps | Forward return *accuracy* (is -50% imputation correct?) |
| Look-ahead at filing-date level | Look-ahead at *feature-construction* level (e.g., sector percentiles using full-year cohort) |
| PSI drift detection | Concept drift (feature distributions stable but relationship to target changed) |
| ML score exclusion from feature sets | ML OOF scores in fraud_risk factor (accepted circular dependency) |
| Null rates above threshold | *Why* nulls are missing (MCAR vs MNAR) |
| Structural dedup (ticker,fiscal_year) | Cross-market dedup (same company dual-listed in US + KR) |

---

## 2. Workflow / CI Map

### 2.1 CI Workflows

| Workflow | File | Trigger | What It Does | Failure Mode |
|----------|------|---------|--------------|--------------|
| **Weekly Data Refresh** | `.github/workflows/refresh_data.yml` | Sunday 05:00 UTC (cron) + manual dispatch | Full 6-market pipeline rebuild → quality gates → alpha scores → ML scores → factor research → feature selection → HF push | Exit on quality gate failure; non-fatal errors on factor_research/feature_selection |
| **CI Tests** | `.github/workflows/ci.yml` | Push/PR to main/develop | `pytest tests/ -v --tb=short` | Exit 1 if any test fails |
| **Model Drift Monitor** | `.github/workflows/monitor_drift.yml` | Monday 07:00 UTC (cron) + manual | Downloads data+models from HF → PSI + AUC + IC decay + drawdown check | Warning annotation on drift; non-blocking |

### 2.2 Local Orchestration Scripts

| Script | Purpose | Steps Executed |
|--------|---------|----------------|
| `workflows/run_pipeline.py` | Master pipeline runner | Steps 1-6 sequentially; supports `build`, `features`, `enrich-prices`, `enrich-macro`, `clean`, `status` |
| `workflows/refresh_data.py` | Frontend-friendly refresh controller | Three modes: `quick` (5+6), `prices` (3+5+6), `full` (1-6). Writes `data/refresh_status.json` |
| `workflows/run_dataset_enrichments.py` | Post-step6 enrichment orchestrator | Universe (p0f) → quarterly → fraud labels → fraud taxonomy → validate_feature_contract |
| `workflows/wait_and_merge.py` | Multi-market merge poller | Polls for KR/JP snapshots → merge_snapshots → re-run steps 4-6 |
| `workflows/run_pipeline_{br,ca,eu,jp,kr}.py` | Per-market pipeline runners | Market-specific data sources (DART API for KR, CVM for BR, etc.) |

### 2.3 Execution Flow (CI)

```
refresh_data.yml:
  ┌─ Download existing dataset from HF (incremental base)
  ├─ For each market: run_pipeline_{market}
  ├─ step6_clean (unified)
  ├─ validate_feature_contract (Phase B gate)
  ├─ compute_alpha (5-factor scores)
  ├─ ML scoring (download models from HF, score_historical)
  ├─ check_data --fail-fast (MANDATORY GATE)
  ├─ test_dataset_quality (MANDATORY GATE)
  ├─ bias_audit --ci (look-ahead HARD FAIL)
  ├─ factor_research (non-fatal)
  ├─ run_feature_selection (non-fatal)
  ├─ generate_reports
  └─ Push to HuggingFace + upload artifacts
```

---

## 3. Test Coverage Map

### 3.1 What's Tested

| Test File | Tests | Module Covered |
|-----------|-------|----------------|
| `tests/pipeline/test_step1_fetch_tickers.py` | Ticker fetch logic | pipeline/step1 |
| `tests/pipeline/test_step2_build_snapshots.py` | EDGAR snapshot building | pipeline/step2 |
| `tests/pipeline/test_step3_enrich_prices.py` | Price enrichment, forward returns | pipeline/step3 |
| `tests/pipeline/test_step4_enrich_macro.py` | Macro enrichment | pipeline/step4 |
| `tests/pipeline/test_step5_compute_features.py` | Feature computation, winsorize, sector_pct | pipeline/step5 |
| `tests/pipeline/test_step6_clean.py` | Cleaning, dedup, validation | pipeline/step6 |
| `tests/pipeline/test_step6_enrichments.py` | Step6 enrichment sub-functions | pipeline/step6 |
| `tests/pipeline/test_feature_library.py` | Normalised ratios, Piotroski ext | pipeline/feature_library |
| `tests/pipeline/test_enrich_fraud_labels.py` | Fraud label enrichment | pipeline/enrich_fraud_labels |
| `tests/pipeline/test_enrich_fraud_taxonomy.py` | Fraud taxonomy scoring | pipeline/enrich_fraud_taxonomy |
| `tests/pipeline/test_p0f.py` | Universe definition | pipeline/p0f_universe_definition |
| `tests/pipeline/test_p0g_confidence_score.py` | Confidence scoring | pipeline/step6_clean |
| `tests/test_pipeline.py` | Core pipeline logic (temporal split, feature selection, model training) | modeling/train, pipeline |
| `tests/test_integration.py` | Full-pipeline end-to-end on synthetic data | All packages |
| `tests/test_ablation.py` | Feature ablation logic | research/ablation |

### 3.2 Test Coverage Status (Updated Sessions 32-46)

> **Total test count: 523** (was ~100 at Session 30). Major expansion in sessions 32-34, 46.

| Module | Status | Session Added | Tests |
|--------|--------|---------------|-------|
| `quality/check_data + dataset_quality` | **COVERED** | Session 32 | 41 tests |
| `alpha/factors/*` | **COVERED** | Session 33 | 23 tests |
| `backtest/engine.py` (walk-forward + gates) | **COVERED** | Sessions 33, 46 | 29 tests (incl. 9 gate tests) |
| `research/ic_engine + factor_research` | **COVERED** | Session 33 | 20 tests |
| `portfolio/*` | **COVERED** | Session 34 | 24 tests |
| `workflows/*` | Still **NO** unit tests | — | 0 tests |
| `fraud/*` | Still **NO** unit tests (stubs) | — | 0 tests |
| `data_io/*` | Still **NO** unit tests | — | 0 tests |
| `modeling/score_oof.py` | Still **NO** unit tests | — | 0 tests |

---

## 4. Cross-Package Dependency Graph

```
┌─────────────────────────────────────────────────────────────────┐
│                     EXTERNAL DEPENDENCIES                         │
│  _root.py (ROOT path)  ←──── ALL packages import this           │
│  pandas, numpy, scipy, joblib, sklearn, yfinance, requests      │
└─────────────────────────────────────────────────────────────────┘

┌──────────┐     ┌──────────┐     ┌───────────┐     ┌──────────┐
│ pipeline │────→│ quality  │     │ workflows │────→│ pipeline │
│ step1-6  │     │ (reads   │     │ (shells   │     │ step1-6  │
│ feature_ │     │  parquet)│     │  out to   │     │          │
│ library  │     │          │     │  scripts) │     │          │
└──────────┘     └──────────┘     └───────────┘     └──────────┘
      │                                  │
      ▼                                  ▼
┌──────────┐     ┌──────────┐     ┌───────────┐
│ modeling │────→│ alpha    │────→│ portfolio │
│ train.py │     │ factors/ │     │ build_*   │
│ score.py │     │composite │     │ leverage  │
│ score_oof│     │          │     │           │
└──────────┘     └──────────┘     └───────────┘
      │                │                 │
      ▼                ▼                 ▼
┌──────────┐     ┌──────────┐     ┌───────────┐
│ research │────→│ backtest │     │  data_io  │
│ ic_engine│     │ engine   │←────│ merge_snap│
│ factor_  │     │          │     │ push/pull │
│ research │     │          │     │ fetch_aaer│
└──────────┘     └──────────┘     └───────────┘
                       ▲
                       │
              ┌────────┴───────┐
              │   portfolio    │
              │ build_alpha_   │
              │ registry       │
              │ build_screener │
              └────────────────┘

Key cross-package imports:
  portfolio/build_alpha_registry.py → backtest.engine, alpha.factors.*
  portfolio/build_screener_registry.py → backtest.engine
  portfolio/leverage_strategy.py → pipeline.feature_library
  quality/monitor_drift.py → models/ (joblib), data/alpha_registry.json
  research/* → backtest.engine (filter_composite, run_backtest)
```

### Internal vs Shell Coupling

| Coupling Type | Examples |
|---------------|----------|
| **Python import** | portfolio → backtest.engine, alpha.factors; leverage_strategy → pipeline.feature_library |
| **Shell subprocess** | workflows/* → pipeline/step*.py; run_dataset_enrichments → quality/validate_feature_contract; CI → quality/*.py, modeling.*, research.* |
| **Data file coupling** | All packages read `data/historical_dataset_clean.parquet`; modeling reads/writes `models/`; quality reads `data/alpha_registry.json`, `data/portfolio_backtest.json` |

---

## 5. Support Package Map

### 5.1 fraud/

| File | Status | Purpose |
|------|--------|---------|
| `fraud/__init__.py` | Empty | Package marker |
| `fraud/features.py` | Stub | Future: fraud-specific feature computation |
| `fraud/rules.py` | Stub | Future: rule-based fraud detection |
| `fraud/taxonomy.py` | Stub (NotImplementedError) | Maps fraud indicators to 5 sub-categories. Currently non-functional |

**Connection to spine:** Fraud taxonomy is *already implemented* in `pipeline/enrich_fraud_taxonomy.py`. This package is a planned extraction target, not yet active.

### 5.2 portfolio/

| File | Purpose | Depends On |
|------|---------|------------|
| `build_alpha_registry.py` | Builds `data/alpha_registry.json` — IC-weighted signal registry | backtest.engine, alpha.factors.* |
| `build_portfolio.py` | Portfolio construction: IC-weighted composite → Kelly-sized positions → backtest | alpha_registry.json, data parquet |
| `build_screener_registry.py` | Screener registry for product output | backtest.engine |
| `leverage_strategy.py` | Leveraged strategy variant | pipeline.feature_library |

**Connection to spine:** Downstream consumer. Takes ML scores + alpha factors → ranked stock picks + position sizes.

### 5.3 data_io/

| File | Purpose | External Service |
|------|---------|-----------------|
| `fetch_aaer_labels.py` | Scrapes SEC AAER enforcement actions for fraud labels | SEC website |
| `fetch_spy_returns.py` | Downloads SPY annual returns for benchmarking | yfinance |
| `merge_snapshots.py` | Merges per-market snapshots into unified dataset | Local files |
| `migrate_to_db.py` | Parquet → SQLite migration (for app/API use) | Local |
| `pull_from_hf.py` | Downloads dataset from HuggingFace Hub | HuggingFace |
| `push_to_hf.py` | Uploads dataset + models to HuggingFace Hub | HuggingFace |

**Connection to spine:** Input (fetch_aaer_labels feeds fraud labels) and output (push/pull for CI persistence).

---

## 6. Consolidated Risk Register (Sessions 27-30)

### Tier 1: HIGH Severity

| # | Risk | Source | Detail |
|---|------|--------|--------|
| 1 | ~~**Survivorship bias (backtest default=DROP)**~~ | Session 29 | **RESOLVED Session 36**: Default changed to `impute` (-50%). Pessimistic assumption now standard. |
| 2 | ~~**Feature selection uses test data (soft leakage)**~~ | Session 28 | **RESOLVED Session 39**: All IC/PSI computation restricted to fiscal_year <= 2020 (train-only). |
| 3 | ~~**No unit tests for quality scripts**~~ | Session 30 | **RESOLVED Session 32**: 41 tests for quality/check_data + dataset_quality. |
| 4 | **Fraud taxonomy package is non-functional** | Session 30 | `fraud/taxonomy.py` still raises NotImplementedError. All fraud logic lives in pipeline/enrich_fraud_taxonomy.py instead. Still unresolved. |
| 5 | ~~**Portfolio/alpha modules untested**~~ | Session 30 | **RESOLVED Sessions 33-34**: 23 alpha tests, 24 portfolio tests, 29 backtest tests added. |

### Tier 2: MEDIUM Severity

| # | Risk | Source | Detail |
|---|------|--------|--------|
| 6 | Force-include overrides ICIR selection (9 features) | Session 28 | Momentum/macro features bypass statistical selection pipeline |
| 7 | Decision tree trained on single window (2008-2018) | Session 28 | No walk-forward; could overfit to one market regime |
| 8 | ~~Two divergent feature sets (27 vs 45)~~ | Session 28 | **RESOLVED**: 27 pruned is canonical for production. Walk-forward uses dynamic IC-ranking (28 for 3y). |
| 9 | Annual rebalance look-ahead (assumes Jan 1 availability) | Session 29 | Most 10-Ks filed Mar-Jun; backtest uses too-early entry |
| 10 | Benchmark mismatch for non-US (SPY for iarb) | Session 29 | Excess return claims meaningless without proper benchmark |
| 11 | ~~EXCLUDE_COLS defined in 3 separate files~~ | Session 29 | **RESOLVED Session 35**: Consolidated in `modeling/constants.py`, imported everywhere. |
| 12 | Walk-forward loop copy-pasted 4 times | Session 29 | Bug fix in one copy won't propagate to others |
| 13 | RISK_FREE constant (3%) distorts zero-rate era Sharpe | Session 29 | Reporting accuracy issue |
| 14 | ~~workflows/run_pipeline_br.py has undefined BASE~~ | Session 30 | **RESOLVED Session 31**: Fixed ordering bug. |
| 15 | monitor_drift depends on alpha_registry.json existing | Session 30 | IC decay check fails silently if registry absent |

### Tier 3: LOW Severity

| # | Risk | Source | Detail |
|---|------|--------|--------|
| 16 | YoY growth from near-zero base (99,900%) | Session 27 | Mitigated by step5 winsorization but could dominate pre-winsorize |
| 17 | Naming inconsistencies (_yoy vs no suffix, equity vs total_equity) | Session 27 | Confusing but not incorrect |
| 18 | Sector percentile uses full-year cohort | Session 27/29 | Mild look-ahead at feature level |
| 19 | EDGAR HTTP 200 assumption | Session 27 | Silent data loss on failures |
| 20 | yfinance rate limit fragility | Session 27 | IP ban → NaN forward returns |
| 21 | FRED API key not validated | Session 27 | Graceful degradation but no alert |
| 22 | Equal alpha weights (20% each) | Session 28 | Suboptimal but avoids weight-mining |
| 23 | CI non-fatal steps swallow errors | Session 30 | factor_research, feature_selection failures logged but don't block push |

---

## 7. Refactor Backlog (Priority-Ordered)

### ~~Theme A: Testing Infrastructure~~ — ALL COMPLETE (Sessions 32-34, 46)

| # | Item | Status |
|---|------|--------|
| A1 | Unit tests for quality/ scripts | **DONE** Session 32 (41 tests) |
| A2 | Unit tests for alpha/factors/* | **DONE** Session 33 (23 tests) |
| A3 | Unit tests for backtest/engine.py | **DONE** Sessions 33, 46 (29 tests) |
| A4 | Unit tests for portfolio/* | **DONE** Session 34 (24 tests) |

### Theme B: Code Consolidation (LOE: 2-3 sessions)

| # | Item | LOE | Why |
|---|------|-----|-----|
| B1 | Extract shared walk-forward training loop | 1 session | 4 copies across research scripts |
| B2 | ~~Unify EXCLUDE_COLS/PATTERNS (single source)~~ | **DONE** Session 35 | `modeling/constants.py` |
| B3 | Consolidate `_sic_to_sector()` duplication | 0.5 session | engine.py + ic_engine.py |
| B4 | ~~Unify feature sets (27 pruned as canonical)~~ | **DONE** Session 34 | 27 canonical confirmed |
| B5 | ~~Fix undefined BASE in run_pipeline_br.py~~ | **DONE** Session 31 | Fixed ordering |

### Theme C: Production Hardening (LOE: 3-4 sessions)

| # | Item | LOE | Why |
|---|------|-----|-----|
| C1 | ~~Model staleness CI check~~ | **DONE** Session 34 | `quality/check_model_staleness.py` |
| C2 | ~~Survivorship default = impute (not drop)~~ | **DONE** Session 36 | Default is now -50% imputation |
| C3 | ~~Filing-date aware rebalance timing~~ | **DONE** Session 36 | Rebalance gated by filing_date |
| C4 | Non-US benchmark (ACWI for iarb) | 0.5 session | SPY benchmark meaningless for non-US |
| C5 | Activate fraud/ package (extract from pipeline) | 1 session | Currently stub/dead |

### Theme D: Research Improvements (LOE: 2-3 sessions)

| # | Item | LOE | Why |
|---|------|-----|-----|
| D1 | IC validation per alpha factor | 1 session | Factors may not predict; misleading to show |
| D2 | Walk-forward feature selection for production | 1 session | Current selects once on full training data |
| D3 | Remove composite weight blend — ML only + gates | 1 session | Manual weights compete with learned weights |
| D4 | Time-varying risk-free rate | 0.5 session | Sharpe accuracy |

---

## 8. Final Architecture Synthesis (All 4 Orientation Sessions)

```
═══════════════════════════════════════════════════════════════════
                    FULL SYSTEM ARCHITECTURE
═══════════════════════════════════════════════════════════════════

DATA SOURCES                    PIPELINE                         OUTPUT
─────────────                   ────────                         ──────
SEC EDGAR ──→ step1 (tickers)
              step2 (snapshots) ──→ raw financials
yfinance  ──→ step3 (prices)   ──→ forward returns, momentum
FRED      ──→ step4 (macro)    ──→ macro context
              step5 (features)  ──→ 170+ ratios, scores, composites
              step6 (clean)     ──→ data/historical_dataset_clean.parquet
                                        │
              ┌─────────────────────────┼──────────────────────────┐
              │                         │                          │
              ▼                         ▼                          ▼
       ENRICHMENTS               MODELING                   QUALITY GATES
       ───────────               ────────                   ─────────────
       p0f (universe)            train.py (LightGBM)       check_data
       fraud labels              score_oof.py (WF)         test_dataset_quality
       fraud taxonomy            score_historical.py        bias_audit (CI)
       quarterly feat.           feature_selection          validate_feature_contract
              │                         │                   monitor_drift (weekly)
              │                         │                          │
              ▼                         ▼                          │
         ALPHA LAYER              RESEARCH                         │
         ───────────              ────────                         │
         value factor             ic_engine                        │
         quality factor           factor_research                  │
         momentum factor          proper_split_backtest            │
         growth factor            pruned_backtest                  │
         fraud_risk factor        explainable_tree                 │
         composite blend          regime_overlay                   │
              │                         │                          │
              └───────────┬─────────────┘                          │
                          ▼                                        │
                    BACKTEST ENGINE                                 │
                    ──────────────                                  │
                    Walk-forward ML → annual rebalance              │
                    Strategy filters (composite, qem, scdv, iarb)  │
                    SPY benchmark comparison                        │
                          │                                        │
                          ▼                                        │
                    PORTFOLIO                                       │
                    ─────────                                       │
                    alpha_registry.json (IC-weighted signals)       │
                    build_portfolio (Kelly sizing)                  │
                    screener_registry (product output)              │
                          │                                        │
                          ▼                                        ▼
                    DATA I/O                                  CI ORCHESTRATION
                    ────────                                  ────────────────
                    push_to_hf → HuggingFace Hub             refresh_data.yml (weekly)
                    pull_from_hf ← HuggingFace Hub           monitor_drift.yml (weekly)
                    fetch_aaer (SEC enforcement)              ci.yml (push/PR)
                    fetch_spy_returns                         run_pipeline.py (local)
                    merge_snapshots                           run_dataset_enrichments.py
```

### Key Data Files (System State)

| File | Written By | Read By |
|------|-----------|---------|
| `data/historical_dataset_clean.parquet` | step6_clean | ALL packages |
| `models/model_{h}.joblib` | modeling/train | modeling/score*, quality/monitor_drift |
| `models/model_meta.json` | modeling/train | quality/bias_audit, monitor_drift |
| `models/feature_sets_*.json` | modeling/feature_selection | modeling/train, score_oof |
| `data/alpha_registry.json` | portfolio/build_alpha_registry | portfolio/build_portfolio, quality/monitor_drift |
| `data/portfolio_backtest.json` | portfolio/build_portfolio | quality/monitor_drift |
| `data/refresh_status.json` | workflows/refresh_data | Frontend (staleness indicator) |
| `reports/drift_report.json` | quality/monitor_drift | CI artifacts |

---

## 9. Quiz

Test your understanding of the quality + orchestration layer:

1. **Which quality check is the ONLY hard-fail gate in the CI pipeline for look-ahead bias?**
   (Name the script and the specific flag)

2. **If `quality/monitor_drift.py` finds PSI > 0.20 for 3 features, what happens in CI?**
   (Exit code? Does it block the data refresh?)

3. **What's the execution order in `run_dataset_enrichments.py`?**
   (Name the 5 steps in order)

4. **Which two portfolio scripts import from `backtest.engine`?**

5. **The `fraud/` package currently has how many functional (non-stub) modules?**

6. **Name 3 modules that have ZERO unit test coverage but are critical to correctness.**

7. **What does `workflows/wait_and_merge.py` poll for, and what does it trigger when ready?**

8. **`check_sync.py` runs when? What does it enforce?**

---

## 10. Quiz Answers

1. `quality/bias_audit.py --ci` — exits 1 ONLY on look-ahead violations (filed_date < period_end). Survivorship/overfitting are warn-only.

2. Exit code 1 from monitor_drift. But it runs in its OWN workflow (`monitor_drift.yml`), NOT in `refresh_data.yml`. So it does NOT block the data refresh — it just produces a GitHub warning annotation.

3. Universe definition (p0f) → Quarterly feature enrichment → Fraud labels → Fraud taxonomy → Validate feature contract

4. `portfolio/build_alpha_registry.py` and `portfolio/build_screener_registry.py`

5. Zero. All three files (`features.py`, `rules.py`, `taxonomy.py`) are stubs or contain only constants. Actual fraud logic lives in `pipeline/enrich_fraud_taxonomy.py`.

6. Any 3 of: `quality/check_data.py`, `quality/bias_audit.py`, `alpha/factors/*.py`, `backtest/engine.py`, `portfolio/build_portfolio.py`, `modeling/score_oof.py`, `research/ic_engine.py`

7. Polls for `data/snapshots_kr.parquet` and `data/snapshots_jp.parquet` (>10KB). When both exist, triggers `data_io/merge_snapshots --activate --backup` then `run_pipeline build --step 4`.

8. Runs as pre-commit hook (with `--warn-only`). Enforces that when certain source files change (e.g., step5, feature_library, CI workflows), the corresponding documentation files are also staged. Rules mirror the CLAUDE.md Change Checklist.
