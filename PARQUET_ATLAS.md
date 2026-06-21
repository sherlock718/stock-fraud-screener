# PARQUET_ATLAS.md — Parquet File Registry

Generated 2026-06-21. Every parquet file referenced in `pipeline/` and `scripts/`.

---

## Primary Pipeline Chain (US)

| File | Created By | Read By | Overwrites/Mutates | Key Columns | Unique Key | Type |
|------|-----------|---------|-------------------|-------------|-----------|------|
| `data/tickers.parquet` | `pipeline/step1_fetch_tickers.py` | `pipeline/step2_build_snapshots.py` | Created fresh each run | ticker, cik, company_name, exchange, market | (ticker) | intermediate |
| `data/snapshots.parquet` | `pipeline/step2_build_snapshots.py` | `step3_enrich_prices.py`, `step4_enrich_macro.py`, `step5_compute_features.py`, `scripts/merge_snapshots.py` | Created fresh each run | ticker, fiscal_year, period_type, filed_date, revenue, total_assets, net_income, ... | (ticker, fiscal_year, period_type) | intermediate |
| `data/prices.parquet` | `pipeline/step3_enrich_prices.py` | `pipeline/step5_compute_features.py`, `scripts/merge_snapshots.py` | Created fresh each run | ticker, fiscal_year, entry_price, entry_date, forward_return_{6m,1y,2y,3y,5y}, momentum_{3m,6m,12m}_prior, beat_local_market_{1y,3y,5y} | (ticker, fiscal_year) | intermediate |
| `data/macro.parquet` | `pipeline/step4_enrich_macro.py` | `pipeline/step5_compute_features.py` | Created fresh each run | cik, ticker, filed_date, fiscal_year, fiscal_quarter, period_type, treasury_10y, treasury_2y, yield_curve, fed_funds_rate, credit_spread_baa, hy_spread, cpi_yoy, recession, vix, real_rate_10y, credit_tightening, macro_regime | (ticker, fiscal_year) | intermediate |
| `data/historical_dataset.parquet` | `pipeline/step5_compute_features.py` | `pipeline/step6_clean.py`, `scripts/impute_features.py`, `scripts/mark_survivorship.py`, `scripts/enrich_quarterly_features.py` | Created fresh each run | All 350+ feature columns + identifiers + targets | (ticker, fiscal_year, period_type) | intermediate |
| `data/historical_dataset_clean.parquet` | `pipeline/step6_clean.py` | Nearly everything downstream (see below) | Created by step6, then MUTATED in-place by multiple enrichment scripts | All features + `in_universe`, `excl_reason`, `data_confidence`, `fraud_confirmed`, `fraud_*` taxonomy, `ml_*` scores | (ticker, fiscal_year, period_type) | **FINAL** |

### Who reads `historical_dataset_clean.parquet`

- `scripts/train_models.py` — ML training
- `scripts/generate_oof_scores.py` — OOF scoring (also writes back)
- `scripts/score_historical.py` — Historical scoring (also writes back)
- `scripts/backtester.py` — Backtest
- `scripts/bias_audit.py` — Audit (may write back with `--fix`)
- `scripts/monitor_drift.py` — Drift monitoring
- `scripts/factor_research.py` — IC/ICIR research
- `scripts/compute_alpha.py` — Alpha factor computation
- `scripts/run_feature_selection.py` — Feature selection
- `scripts/check_data.py` — Data quality checks
- `scripts/clean_dataset.py` — Additional cleaning
- `scripts/fix_dataset_quality.py` — Quality fixes
- `scripts/generate_reports.py` — Report generation
- `scripts/build_portfolio.py` — Portfolio construction
- `scripts/build_screener_registry.py` — Screener output
- `scripts/pit_validate.py` — PIT validation
- `scripts/mark_survivorship.py` — Survivorship correction
- `scripts/enrich_quarterly_features.py` — Quarterly enrichment
- `scripts/impute_features.py` — Feature imputation
- `scripts/fetch_aaer_labels.py` — AAER label merge
- `scripts/patch_equity_vol_features.py` — Patch features
- `scripts/patch_montier_c2.py` — Patch Montier C2
- `scripts/push_to_hf.py` — Upload to HuggingFace
- `scripts/migrate_to_db.py` — DB migration
- `scripts/verify_doc_consistency.py` — Doc verification
- `scripts/run_phase_checks.py` — Phase gate checks
- `pipeline/enrich_fraud_labels.py` — Fraud labels (mutates in-place)
- `pipeline/enrich_fraud_taxonomy.py` — Fraud taxonomy (mutates in-place)
- `pipeline/enrich_feature_dictionary.py` — Feature docs
- `pipeline/p0f_universe_definition.py` — Universe (mutates in-place)
- `pipeline/p0g_confidence_score.py` — Confidence (mutates in-place)

### Who MUTATES `historical_dataset_clean.parquet` in-place

| Script | Columns Added/Modified |
|--------|----------------------|
| `pipeline/p0f_universe_definition.py` | `in_universe`, `excl_reason` |
| `pipeline/p0g_confidence_score.py` | `data_confidence` |
| `pipeline/enrich_fraud_labels.py` | `fraud_confirmed`, `fraud_source` |
| `pipeline/enrich_fraud_taxonomy.py` | `fraud_revenue_manip`, `fraud_expense_manip`, `fraud_asset_manip`, `fraud_disclosure`, `fraud_other` |
| `scripts/generate_oof_scores.py` | `ml_1y_oof`, `ml_3y_oof`, `ml_5y_oof` |
| `scripts/score_historical.py` | `ml_1y`, `ml_3y`, `ml_5y`, `ml_pred_excess_3y` |
| `scripts/fetch_aaer_labels.py` | `fraud_confirmed` (update) |
| `scripts/bias_audit.py --fix` | Various (bias corrections) |
| `scripts/mark_survivorship.py --fix` | `delisted_flag`, forward return corrections |
| `scripts/enrich_quarterly_features.py --fix` | Quarterly-derived features |
| `scripts/impute_features.py` | Imputed quarterly cols + `size_category` |
| `scripts/patch_equity_vol_features.py` | Equity vol feature corrections |
| `scripts/patch_montier_c2.py` | Montier C-score columns |
| `scripts/clean_dataset.py` | Data quality fixes |
| `scripts/fix_dataset_quality.py` | Quality patches |
| `scripts/compute_alpha.py` | `alpha_value`, `alpha_quality`, `alpha_momentum`, `alpha_growth`, `alpha_fraud_risk`, `alpha_composite` |

---

## Multi-Market Pipeline Files

| File | Created By | Read By | Unique Key | Type |
|------|-----------|---------|-----------|------|
| `data/tickers_br.parquet` | `step1_fetch_tickers_br.py` | `step2_build_snapshots_br.py` | (ticker) | intermediate |
| `data/tickers_ca.parquet` | `step1_fetch_tickers_ca.py` | `step2_build_snapshots_ca.py` | (ticker) | intermediate |
| `data/tickers_eu.parquet` | `step1_fetch_tickers_eu.py` | `step2_build_snapshots_eu.py` | (ticker) | intermediate |
| `data/tickers_jp.parquet` | `step1_fetch_tickers_jp.py` / `step1_fetch_tickers_jp_free.py` | `step2_build_snapshots_jp.py` / `step2_build_snapshots_jp_free.py` | (ticker) | intermediate |
| `data/tickers_kr.parquet` | `step1_fetch_tickers_kr.py` | `step2_build_snapshots_kr.py` | (ticker) | intermediate |
| `data/snapshots_br.parquet` | `step2_build_snapshots_br.py` | `scripts/merge_snapshots.py`, `run_pipeline_br.py` (step3 --snapshots) | (ticker, fiscal_year, period_type) | intermediate |
| `data/snapshots_ca.parquet` | `step2_build_snapshots_ca.py` | `scripts/merge_snapshots.py`, `run_pipeline_ca.py` | (ticker, fiscal_year, period_type) | intermediate |
| `data/snapshots_eu.parquet` | `step2_build_snapshots_eu.py` | `scripts/merge_snapshots.py`, `run_pipeline_eu.py` | (ticker, fiscal_year, period_type) | intermediate |
| `data/snapshots_jp.parquet` | `step2_build_snapshots_jp.py` / `_jp_free.py` | `scripts/merge_snapshots.py`, `run_pipeline_jp.py` | (ticker, fiscal_year, period_type) | intermediate |
| `data/snapshots_kr.parquet` | `step2_build_snapshots_kr.py` | `scripts/merge_snapshots.py`, `run_pipeline_kr.py` | (ticker, fiscal_year, period_type) | intermediate |
| `data/prices_br.parquet` | `step3_enrich_prices.py --out prices_br.parquet` | `scripts/merge_snapshots.py` | (ticker, fiscal_year) | intermediate |
| `data/prices_ca.parquet` | `step3_enrich_prices.py --out` | `scripts/merge_snapshots.py` | (ticker, fiscal_year) | intermediate |
| `data/prices_eu.parquet` | `step3_enrich_prices.py --out` | `scripts/merge_snapshots.py` | (ticker, fiscal_year) | intermediate |
| `data/prices_jp.parquet` | `step3_enrich_prices.py --out` | `scripts/merge_snapshots.py` | (ticker, fiscal_year) | intermediate |
| `data/prices_kr.parquet` | `step3_enrich_prices.py --out` | `scripts/merge_snapshots.py` | (ticker, fiscal_year) | intermediate |
| `data/macro_br.parquet` | `step4_enrich_macro.py --suffix _br` | `step5 --suffix _br` | (ticker, fiscal_year) | intermediate |
| `data/macro_ca.parquet` | `step4_enrich_macro.py --suffix _ca` | `step5 --suffix _ca` | (ticker, fiscal_year) | intermediate |
| `data/macro_eu.parquet` | `step4_enrich_macro.py --suffix _eu` | `step5 --suffix _eu` | (ticker, fiscal_year) | intermediate |
| `data/macro_jp.parquet` | `step4_enrich_macro.py --suffix _jp` | `step5 --suffix _jp` | (ticker, fiscal_year) | intermediate |
| `data/macro_kr.parquet` | `step4_enrich_macro.py --suffix _kr` | `step5 --suffix _kr` | (ticker, fiscal_year) | intermediate |
| `data/historical_dataset_br.parquet` | `step5 --suffix _br` | `step6 --suffix _br` | (ticker, fiscal_year, period_type) | intermediate |
| `data/historical_dataset_ca.parquet` | `step5 --suffix _ca` | `step6 --suffix _ca` | (ticker, fiscal_year, period_type) | intermediate |
| `data/historical_dataset_eu.parquet` | `step5 --suffix _eu` | `step6 --suffix _eu` | (ticker, fiscal_year, period_type) | intermediate |
| `data/historical_dataset_jp.parquet` | `step5 --suffix _jp` | `step6 --suffix _jp` | (ticker, fiscal_year, period_type) | intermediate |
| `data/historical_dataset_kr.parquet` | `step5 --suffix _kr` | `step6 --suffix _kr` | (ticker, fiscal_year, period_type) | intermediate |
| `data/historical_dataset_clean_br.parquet` | `step6 --suffix _br` | `scripts/merge_snapshots.py` (via wait_and_merge) | (ticker, fiscal_year, period_type) | market-final |
| `data/historical_dataset_clean_ca.parquet` | `step6 --suffix _ca` | (same) | (ticker, fiscal_year, period_type) | market-final |
| `data/historical_dataset_clean_eu.parquet` | `step6 --suffix _eu` | (same) | (ticker, fiscal_year, period_type) | market-final |
| `data/historical_dataset_clean_jp.parquet` | `step6 --suffix _jp` | (same) | (ticker, fiscal_year, period_type) | market-final |
| `data/historical_dataset_clean_kr.parquet` | `step6 --suffix _kr` | (same) | (ticker, fiscal_year, period_type) | market-final |

---

## Combined/Merged Files

| File | Created By | Read By | Notes |
|------|-----------|---------|-------|
| `data/snapshots_combined.parquet` | `scripts/merge_snapshots.py` | `scripts/patch_equity_vol_features.py` | Union of all market snapshots. With `--activate` copies to `snapshots.parquet` |
| `data/prices_combined.parquet` | `scripts/merge_snapshots.py` | — | Union of all market prices. With `--activate` copies to `prices.parquet` |

---

## Auxiliary / Derived Files

| File | Created By | Read By | Key Columns | Type |
|------|-----------|---------|-------------|------|
| `data/fraud_labels.parquet` | `scripts/build_fraud_labels.py` | `scripts/train_models.py`, `scripts/generate_oof_scores.py` | ticker, cik, start_year, end_year, source, fraud_type | reference |
| `data/monthly_prices.parquet` | `scripts/build_monthly_price_cache.py` | `scripts/backtester.py` | ticker, date, adj_close, volume | cache |
| `data/app_data.parquet` | `scripts/refresh_data.py` (copy of clean) | `scripts/leverage_strategy.py`, `scripts/enrich_sectors_dividends.py` | Same as historical_dataset_clean | app-layer copy |
| `data/sector_dividend_map.parquet` | `scripts/enrich_sectors_dividends.py` | Streamlit app | ticker, sector, industry, dividend_yield | reference |
| `reports/correlation_matrix.parquet` | `scripts/analyze_distributions.py` | — | Feature×feature Pearson correlations | report |

---

## Data Flow Summary

```
step1 (tickers.parquet)
  ↓
step2 (snapshots.parquet)  ──→  merge_snapshots.py (snapshots_combined.parquet)
  ↓
step3 (prices.parquet)     ──→  merge_snapshots.py (prices_combined.parquet)
  ↓
step4 (macro.parquet)
  ↓
step5 (historical_dataset.parquet)
  ↓
step6 (historical_dataset_clean.parquet)  ← FINAL, then mutated by:
  ├─ p0f_universe_definition.py   (in_universe, excl_reason)
  ├─ p0g_confidence_score.py      (data_confidence)
  ├─ enrich_fraud_labels.py       (fraud_confirmed, fraud_source)
  ├─ enrich_fraud_taxonomy.py     (fraud sub-scores)
  ├─ fetch_aaer_labels.py         (fraud_confirmed update)
  ├─ generate_oof_scores.py       (ml_*_oof)
  ├─ score_historical.py          (ml_1y, ml_3y, ml_5y, ml_pred_excess_3y)
  ├─ mark_survivorship.py         (delisted corrections)
  ├─ enrich_quarterly_features.py (quarterly-derived)
  ├─ impute_features.py           (imputation)
  ├─ compute_alpha.py             (alpha_* scores)
  └─ patch scripts                (various fixes)
```

---

## Mutation Order (observed / inferred execution sequence post-step6)

> **Needs verification before use.** This order is inferred from code reading and
> dependency analysis, not confirmed by running the full sequence end-to-end.

1. `pipeline/p0f_universe_definition.py`
2. `pipeline/p0g_confidence_score.py`
3. `scripts/mark_survivorship.py --fix`
4. `scripts/enrich_quarterly_features.py --fix`
5. `scripts/impute_features.py`
6. `pipeline/enrich_fraud_labels.py`
7. `pipeline/enrich_fraud_taxonomy.py`
8. `scripts/fetch_aaer_labels.py`
9. `scripts/generate_oof_scores.py`
10. `scripts/score_historical.py`
11. `scripts/compute_alpha.py`
12. `scripts/patch_*` (as needed)

---

*End of PARQUET_ATLAS.md*
