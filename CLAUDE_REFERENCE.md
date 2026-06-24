# CLAUDE_REFERENCE.md — On-demand context (not auto-loaded)

These tables were extracted from CLAUDE.md to reduce per-message token cost.
Claude can `Read` this file when architecture state, performance numbers,
or file locations are needed for a task.

---

## Current Architecture State (keep in sync with docs/architecture.md)

| Layer | Technology | Location | Status |
|---|---|---|---|
| US data ingestion | SEC EDGAR 10-K/10-Q | `scripts/workflows/run_pipeline.py` | ✅ |
| Multi-market ingestion | yfinance (EU/JP free), DART (KR), EDINET (JP full), SEDAR+ (CA), CVM/brapi (BR) | `pipeline/` | ✅ |
| Feature engineering | 361 columns (355 base + 5 OOF + 1 regression: ml_pred_excess_3y) | `pipeline/step5_compute_features.py` + `scripts/modeling/generate_oof_scores.py` + `scripts/modeling/score_historical.py` | ✅ |
| Quarterly enrichment | 5 intra-year dynamics | `scripts/enrichments/enrich_quarterly_features.py` | ✅ |
| Feature imputation | Quarterly cols + size_category recovery | `scripts/enrichments/impute_features.py` | ✅ |
| Survivorship correction | Imputes −50% return for delisted | `scripts/enrichments/mark_survivorship.py` | ✅ |
| AAER fraud labels | 492 positive rows / 118 companies | `scripts/data_io/fetch_aaer_labels.py` | ✅ |
| OOF ML scoring | Walk-forward OOF → ml_1y_oof/ml_3y_oof/ml_5y_oof (unbiased) | `scripts/modeling/generate_oof_scores.py` | ✅ Phase C |
| Historical ML scoring | Load models → score all rows → write ml_1y/3y/5y to parquet | `scripts/modeling/score_historical.py` | ✅ |
| Alpha factor package | 5-factor scores (Value/Quality/Momentum/Growth/FraudRisk) | `alpha/factors/` | ✅ |
| Horizon routing | Maps investment horizon (months) to nearest model key | `alpha/horizon_router.py` | ✅ Phase C |
| Primary storage | Parquet file | `data/historical_dataset_clean.parquet` 58K rows × 361 cols | ✅ |
| SPY benchmark data | Annual calendar-year SPY total returns | `data/spy_returns.csv` | ✅ Phase C |
| TimescaleDB | Hypertable for time-series queries | `infra/db/init.sql` + `scripts/data_io/migrate_to_db.py` | ⚠️ DB not loaded |
| Feature selection | 4-stage pipeline: PSI→IC→ICIR→dedup | `scripts/modeling/run_feature_selection.py` | ✅ |
| ML models | LightGBM 5 horizons (6m/1y/2y/3y/5y), filed-date PIT-safe, n_estimators=600 | `scripts/modeling/train_models.py` | ✅ Phase C |
| Regression model | LightGBM Huber → excess_return_local_3y magnitude; WF IC 0.34 | `scripts/modeling/train_regression_model.py` | ✅ |
| Calibration + tuning | Platt scaling, Optuna 100 trials | `scripts/modeling/tune_models.py` | ✅ |
| Bias audit | Look-ahead + survivorship + overfitting + multiple testing | `scripts/quality/bias_audit.py` | ✅ Phase C |
| Drift monitoring | PSI + rolling AUC | `scripts/quality/monitor_drift.py` | ✅ |
| Experiment Notebook | Master research frontend · screener rankings · deep dive · live picks | `notebooks/08_experiment_hub.ipynb` | ✅ |
| FastAPI | Screener router, pagination | `api/` | ✅ built |
| CI/CD | Weekly refresh + bias audit + drift monitor | `.github/workflows/` | ✅ |
| Model/dataset hosting | HuggingFace Hub | `scripts/data_io/push_to_hf.py` | ✅ |

### Current Performance (Phase D1 — post-D1.3 re-tune 6m/1y/2y with 60-trial Optuna)
| Horizon | WF Mean AUC | Target | Met? | Notes |
|---|---|---|---|---|
| 6m | 0.5715 | ≥ 0.58 | ❌ | Gap 0.0085; near-miss, structural |
| 1y | 0.5774 | ≥ 0.62 | ❌ | Gap 0.043; dragged by 2018→2019 (0.52) and 2020→2021 (COVID) folds |
| 2y | 0.5880 | ≥ 0.60 | ❌ | Gap 0.012; Optuna-neutral |
| 3y | 0.6248 | ≥ 0.62 | ✅ | |
| 5y | 0.6200 | ≥ 0.62 | ✅ | |

---

## Key File Locations

| What | File |
|---|---|
| Dataset (primary) | `data/historical_dataset_clean.parquet` |
| Models (5 horizons) | `models/model_{6m,1y,2y,3y,5y}.joblib` |
| Model metadata + feature lists | `models/model_meta.json` |
| Selected feature sets (post-selection) | `models/feature_sets_{6m,1y,2y,3y,5y}.json` |
| OOF audit trail per horizon | `reports/oof_auc_{6m,1y,2y,3y,5y}.csv` |
| IC/ICIR factor research reports | `reports/factor_research_{1y,3y,5y}.csv` |
| Feature selection summary (all candidates) | `reports/feature_selection_summary.csv` |
| Walk-forward AUC results | `reports/walk_forward_auc_{1y,3y,5y}.csv` |
| Backtest results | `data/backtest_results.json` |
| SPY benchmark returns | `data/spy_returns.csv` |
| Bias audit report | `reports/bias_audit_report.json` |
| DB schema | `infra/db/init.sql` |
| Feature definitions (314 base formulas) | `pipeline/feature_library.py` |
| Horizon router | `alpha/horizon_router.py` |
| Factor package | `alpha/factors/` |
| Research frontend | `notebooks/08_experiment_hub.ipynb` |
| API entry point | `api/main.py` |
| Architecture doc | `docs/architecture.md` |
| Scripts reference | `docs/developer/scripts.md` |
| Pipeline modules reference | `docs/developer/pipeline-scripts.md` |
| Factor library reference | `docs/methodology/factor-library.md` |
| Feature selection methodology | `docs/methodology/feature-selection.md` |
| Model methodology | `docs/methodology/models.md` |
| Contributing + sync rules | `docs/developer/contributing.md` |
