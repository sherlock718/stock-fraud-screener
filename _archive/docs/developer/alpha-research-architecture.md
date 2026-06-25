# Alpha Research Architecture

How the codebase is organized after the Session 2–6 cleanup.

---

## Directory Roles

| Directory | Role | Mutable at runtime? |
|---|---|---|
| `pipeline/` | Production data spine — steps 1–6 + enrichment modules | No (read-only reference) |
| `scripts/workflows/` | Orchestration — calls pipeline/ and enrichments in order | No |
| `scripts/data_io/` | Fetching, syncing, merging external data (HF, AAER, SPY) | Writes to `data/` |
| `scripts/enrichments/` | Post-step6 dataset mutators (quarterly, impute, survivorship, patches) | Writes to parquet |
| `scripts/modeling/` | Feature selection, model training, scoring, alpha computation | Writes to `models/` and parquet |
| `scripts/analysis/` | Reproducible factor/alpha/distribution analysis commands | Writes to `reports/` |
| `scripts/portfolio/` | Portfolio and strategy construction (registries, leverage) | Writes to `reports/` |
| `scripts/quality/` | Validation, contracts, drift monitoring, bias audit | Read-only checks |
| `scripts/ops/` | Manifests, monitoring reports | Writes to `reports/` |
| `scripts/_shared/` | Shared engines (backtester) imported by other scripts | Library only |
| `scripts/hooks/` | Git hooks (pre-commit guard) | No |
| `alpha/` | Alpha/factor package — factor scores, horizon router, SHAP explain | Library only |
| `research/` | Notebooks and exploratory workspace | Scratch |
| `tests/` | Test suite (pytest) | No |
| `_archive/` | Deprecated UI/API/deployment code (Session 2) | Dead code |

---

## Data Flow Summary

```
Sources (SEC/DART/yfinance/EDINET/CVM)
    │
    ▼
pipeline/step1–step6  ← production spine, multi-market
    │
    ▼
scripts/enrichments/  ← post-pipeline mutations (survivorship, quarterly, impute)
    │
    ▼
scripts/modeling/     ← feature selection → train → score → alpha
    │
    ▼
data/historical_dataset_clean.parquet  (367 columns, ~58K rows)
    │
    ├──► scripts/analysis/    → reports/
    ├──► scripts/portfolio/   → registries, strategies
    ├──► scripts/quality/     → validation pass/fail
    └──► notebooks/           → experiment hub (manual)
```

---

## Pipeline Module Inventory

The `pipeline/` directory is the production data spine. It is **never modified** by cleanup sessions.

| Module | Purpose |
|---|---|
| `step1_fetch_tickers*.py` | Ticker discovery per market (US, EU, JP, KR, CA, BR) |
| `step2_build_snapshots*.py` | Annual/quarterly fundamental snapshots |
| `step3_enrich_prices.py` | OHLCV joins, volatility features (6m/12m/36m/60m) |
| `step4_enrich_macro.py` | T-bill, inflation, macro indicator joins |
| `step5_compute_features.py` | 321 feature formulas (Beneish, Altman, Piotroski, growth, value) |
| `step6_clean.py` | Final cleaning, deduplication, export |
| `p0f_universe_definition.py` | Universe filtering (market cap, liquidity) |
| `p0g_confidence_score.py` | Data completeness confidence scoring |
| `enrich_fraud_labels.py` | AAER fraud label assignment |
| `enrich_fraud_taxonomy.py` | Fraud type classification |
| `feature_library.py` | Feature registry/definitions |
| `archive/` | Superseded pipeline scripts (kept for reference) |

---

## Scripts Subdirectory Detail

### `scripts/workflows/`
Orchestration scripts that call pipeline/ and enrichments/ in the correct order.

- `run_pipeline.py` — US market pipeline
- `run_pipeline_*.py` — Market-specific pipelines (BR, CA, EU, JP, KR)
- `refresh_data.py` — Full refresh orchestrator
- `run_dataset_enrichments.py` — Post-step6 enrichment chain
- `wait_and_merge.py` — Waits for multi-market pipelines then merges

### `scripts/data_io/`
External data fetch and sync utilities.

- `fetch_aaer_labels.py` — SEC enforcement labels
- `fetch_spy_returns.py` — SPY benchmark returns
- `merge_snapshots.py` — Merge multi-market snapshots
- `migrate_to_db.py` — Parquet → TimescaleDB (Phase C deferred)
- `pull_from_hf.py` / `push_to_hf.py` — HuggingFace artifact sync

### `scripts/enrichments/`
Post-step6 dataset enrichment/mutators. Each writes back to parquet.

- `enrich_quarterly_features.py` — 5 intra-year dynamics
- `enrich_sectors_dividends.py` — Sector/dividend enrichment
- `mark_survivorship.py` — Delisted flag + −50% imputation
- `impute_features.py` — Quarterly imputation + size_category
- `fix_dataset_quality.py` — Data quality repairs
- `clean_dataset.py` — Final cleaning pass
- `build_fraud_labels.py` — Fraud label construction
- `build_monthly_price_cache.py` — Price cache for backtesting
- `patch_equity_vol_features.py` — One-time equity/vol fix
- `patch_montier_c2.py` — One-time Montier C2 correction

### `scripts/modeling/`
Feature selection, training, and scoring.

- `run_feature_selection.py` — PSI + ICIR feature selection
- `train_models.py` — LightGBM 5 horizons
- `tune_models.py` — Optuna + CatBoost ensemble + Platt calibration
- `score_historical.py` — Write ml_1y/3y/5y to parquet
- `generate_oof_scores.py` — Walk-forward OOF scoring
- `compute_alpha.py` — 5-factor composite alpha scores
- `train_regression_model.py` — Regression variant

### `scripts/analysis/`
Reproducible research/analysis commands.

- `factor_research.py` — IC/ICIR/decay analysis
- `analyze_distributions.py` — Feature distribution analysis
- `generate_reports.py` — PDF tearsheet + CSV picks

### `scripts/portfolio/`
Portfolio construction and strategy.

- `build_portfolio.py` — Portfolio construction
- `build_screener_registry.py` — Screener ranking registry
- `build_alpha_registry.py` — Alpha signal registry
- `leverage_strategy.py` — Long/short Kelly sizing

### `scripts/quality/`
Validation and monitoring (read-only checks).

- `check_data.py` — Data integrity checks
- `check_sync.py` — Cross-file sync validation
- `bias_audit.py` — Look-ahead, survivorship, overfitting, multiple testing
- `monitor_drift.py` — PSI + AUC drift monitoring
- `pit_validate.py` — Point-in-time validation
- `run_phase_checks.py` — Phase completion criteria
- `test_dataset_quality.py` — 98 dataset quality checks
- `validate_feature_contract.py` — Feature contract enforcement
- `verify_doc_consistency.py` — Documentation consistency

### `scripts/ops/`
Operational utilities.

- `generate_manifest.py` — Artifact manifest generation

### `scripts/_shared/`
Shared library code imported by other scripts.

- `backtester.py` — Walk-forward backtest engine (SPY benchmark, factor attribution)

---

## Key Invariants

1. **pipeline/ is untouchable** — cleanup sessions never modify it
2. **scripts/ imports from `scripts._root`** — all scripts use `ROOT` for path resolution
3. **Column count is 367** — verified from `data/historical_dataset_clean.parquet`
4. **Tests must pass at 341** — no test changes in documentation sessions
5. **`_archive/` is dead** — never imported, never executed, excluded from `.repomixignore`
