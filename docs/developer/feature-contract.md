# Feature Contract

The feature contract defines which column groups the dataset must contain at each pipeline phase. It validates structural completeness without being brittle to individual column additions or removals.

---

## Pipeline Phases

### Phase B — Feature Dataset (Steps 1–6 + enrichment mutators)

The regenerated dataset after running the full pipeline (Steps 1–6) plus post-step6 enrichment mutators. Does NOT require trained models.

| Group | Min Columns | Source | Description |
|---|---|---|---|
| identifiers | 10 of 16 | step1/step2 | CIK, ticker, fiscal_year, market, etc. |
| raw_financials | 20 of 27 | step2 | Revenue, assets, equity, debt, cash flows |
| price_momentum | 8 of 12 | step3 | Entry price, forward returns, momentum, volatility |
| macro | 8 of 12 | step4 | Treasury, credit spread, VIX, recession, macro regime |
| computed_features | 25 of 32 | step5 | Ratios, scores (Altman/Beneish/Piotroski), composites |
| universe_confidence | 3 of 3 | p0f + p0g | in_universe, excl_reason, data_confidence |
| fraud_labels | 2 of 2 | enrich_fraud_labels | fraud_confirmed, fraud_suspect |
| fraud_taxonomy | 5 of 6 | enrich_fraud_taxonomy | fraud_score_* sub-scores |

### Phase C — ML Scoring Layer (requires trained models)

Added by scoring scripts that depend on trained model artifacts.

| Group | Min Columns | Source | Description |
|---|---|---|---|
| oof_scores | 3 of 5 | generate_oof_scores.py | Walk-forward OOF probabilities (ml_*_oof) |
| ml_scores | 3 of 6 | score_historical.py | Full-sample ML scores (ml_1y, ml_3y, ml_5y, ml_pred_excess_3y) |
| alpha_factors | 5 of 6 | compute_alpha.py | 5-factor alpha scores + composite |
| vol_patches | 3 of 5 | patch_equity_vol_features.py | Multi-horizon equity volatility + ROA vol |
| survivorship | 1 of 1 | mark_survivorship.py | delisted_flag |
| quarterly_enriched | 2 of 3 | enrich_quarterly_features.py | revenue_qoq_std, earnings_momentum, filing_lag_trend |

---

## 26 Phase C Overlay Columns (not part of base pipeline)

These columns belong to the Phase C scoring overlay. They are absent from the base Phase B dataset by design — their absence does NOT indicate a pipeline failure. They require trained model artifacts or Phase C scripts to produce.

| Category | Columns | Producer | Required for |
|---|---|---|---|
| OOF scores (5) | ml_6m_oof, ml_1y_oof, ml_2y_oof, ml_3y_oof, ml_5y_oof | generate_oof_scores.py | Unbiased ML features for alpha computation |
| ML scores (6) | ml_6m, ml_1y, ml_2y, ml_3y, ml_5y, ml_pred_excess_3y | score_historical.py | Backtesting, portfolio construction |
| Alpha factors (6) | alpha_value, alpha_quality, alpha_momentum, alpha_growth, alpha_fraud_risk, alpha_composite | compute_alpha.py | Screener ranking, portfolio weighting |
| Vol patches (5) | equity_vol_6m, equity_vol_12m, equity_vol_36m, equity_vol_60m, roa_vol_5y | patch_equity_vol_features.py | Multi-horizon vol features |
| Survivorship (1) | delisted_flag | mark_survivorship.py | Survivorship bias correction |
| Quarterly (3) | revenue_qoq_std, earnings_momentum, filing_lag_trend | enrich_quarterly_features.py | Intra-year dynamics |

**Status:** All 26 are Phase C overlay columns, pending by design. The Phase B base dataset is complete without them. They become available after running the Phase C scoring pipeline (train → OOF → score → alpha → patches).

---

## Validation

Run the contract validator:

```bash
python3 scripts/quality/validate_feature_contract.py
```

### Flags

| Flag | Effect |
|---|---|
| `--parquet PATH` | Check a different parquet file |
| `--strict` | Exit 1 if Phase C is incomplete |
| `--json` | Machine-readable JSON output |

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Current phase is internally consistent |
| 1 | Phase B incomplete (structural problem) or `--strict` and Phase C incomplete |

### Expected output (current state)

```
Phase B (pipeline + enrichment): COMPLETE (8/8 groups)
Phase C (ML scoring + patches):  PENDING (0/6 groups)
Result: Phase B COMPLETE, Phase C PENDING (expected before model retrain)
```

---

## What to Do When a Group Is Missing

### Phase B group fails

A Phase B group failure means the pipeline build is incomplete:

1. Check which group failed in the validator output
2. Re-run the corresponding pipeline step or enrichment script
3. Verify with `python3 scripts/quality/validate_feature_contract.py`

Execution order for Phase B:
```
step1 → step2 → step3 → step4 → step5 → step6
→ p0f_universe_definition.py
→ p0g_confidence_score.py
→ enrich_fraud_labels.py
→ enrich_fraud_taxonomy.py
→ fetch_aaer_labels.py
```

### Phase C group fails (expected until model retrain)

Phase C groups require trained model artifacts. Run in order:

```
1. scripts/modeling/train_models.py              → models/*.joblib
2. scripts/modeling/generate_oof_scores.py       → ml_*_oof columns
3. scripts/modeling/score_historical.py          → ml_* columns
4. scripts/modeling/compute_alpha.py             → alpha_* columns
5. scripts/enrichments/patch_equity_vol_features.py → equity_vol_* columns
6. scripts/enrichments/mark_survivorship.py --fix   → delisted_flag
7. scripts/enrichments/enrich_quarterly_features.py → quarterly columns
```

---

## Design Decisions

- **Group-level, not column-level**: The contract checks that enough representative columns from each group exist. Individual columns can be added/removed without breaking the contract.
- **Min thresholds allow slack**: Each group has a `min_present` lower than `total`. This prevents false failures when optional columns within a group are legitimately absent.
- **Two-phase split**: Phase B and Phase C are independently assessed. Phase C pending is normal state between data regeneration and model retrain.
- **No schema lock**: The contract does NOT enforce exact column counts or names. It validates structural completeness of pipeline stages.

---

## Architecture Boundary

**Phase B is the base pipeline dataset.** It is complete when the validator reports 8/8 groups passing. The absence of ML/OOF/alpha columns does NOT mean Phase B is incomplete or that the pipeline is broken.

**Phase C is a scoring overlay.** It adds model-derived columns on top of a complete Phase B dataset. Phase C columns are pending by design until model retrain is executed. This is not a bug, not a gap, and not a pipeline failure.

**Factor research and feature engineering are a separate future phase.** New signal ideas (liquidity, paid data, domain-judgment improvements) are not part of the current pipeline refactor. They belong after the pipeline is stable and Phase C is restored.

**Session 14 did not perform feature engineering.** It added validation tooling and documentation only. No new features were computed, no models were trained, no pipeline logic was modified.
