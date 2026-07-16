# Session V3.2 — Exact OOS Production Predictions

Status: **Accepted; V3.3 requires separate approval**

## Outcome

Session V3.2 generated 78,492 row-role OOS predictions for
`production_v3_ml_gates` from only the hash-pinned V3.1 table and configuration.
Each fold independently selected at most 28 features for each model role, fit
median imputation on eligible historical rows only, and trained with labels
whose `label_end_date` was strictly before the decision timestamp. Tree and
LightGBM targets and parameters were consumed unchanged from V3.1.

The frozen candidate pool contains the label-derived
`observed_excess_return_3y`. It remains a frozen candidate for selection and is
valid in historical training only after its certified `label_end_date`; its
score-row value is masked unless that date is strictly before the fold decision,
then imputed from the historical-fold median. This preserves the exact frozen
119-candidate contract without exposing a future observed outcome to scoring.

The V3.1 artifact freeze was `2026-07-16T19:11:45.225877+00:00`. Decisions through
2026 were eligible for modeling; every 2027/2028 row-role was retained and
marked `future_decision_after_v3_1_freeze`. No fallback model was used.

## Fold coverage

| Model role | Status | Folds | Scored rows |
|---|---|---:|---:|
| decision_tree | failed_closed | 4 | 4,302 |
| decision_tree | fit | 13 | 39,246 |
| lightgbm_regression | failed_closed | 4 | 4,302 |
| lightgbm_regression | fit | 13 | 39,246 |

## Exclusions

| Model role | Prediction status | Exact exclusion code | Rows |
|---|---|---|---:|
| decision_tree | excluded | fold_no_eligible_training_history | 4,302 |
| decision_tree | future_excluded | future_decision_after_v3_1_freeze | 258 |
| decision_tree | oos_prediction_available | none | 39,246 |
| lightgbm_regression | excluded | fold_no_eligible_training_history | 4,302 |
| lightgbm_regression | future_excluded | future_decision_after_v3_1_freeze | 258 |
| lightgbm_regression | oos_prediction_available | none | 39,246 |

## Boundary

This session did not read Session 9 predictions, optimize parameters or
thresholds, select holdings, collect market data, calculate ADTV, or run a
backtest. Generated model and prediction payloads remain Git-ignored. V3.3 was
not started.
