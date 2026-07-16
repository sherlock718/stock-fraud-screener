# Session 2 — Three-Year Fold Purge Summary

> Superseded operationally by Session 2B's horizon-qualified schema and explicit
> survivorship sensitivity contract. See
> `reports/pit_validation/02b_horizon_survivorship_summary.md`. The original
> three-year findings and strict timestamp rule remain valid.

Date: 2026-07-15  
Scope: code and synthetic tests only; no dataset rebuild, model training, or
performance run.

## Implemented Contract

A three-year target is training-eligible only when its target is non-null, its
availability date is non-null, its provenance is an observed market-price
source, and its availability date is strictly before the January 1 scoring date.
An end date equal to the scoring date is excluded.

Step 3 persists:

- `label_start_date`: actual stock entry trading date;
- `stock_label_end_date`: actual stock exit trading date;
- `benchmark_label_end_date`: actual benchmark exit trading date;
- `label_end_date`: later of the stock and benchmark exit dates;
- explicit stock, benchmark, and combined label provenance.

`forward_return_3y` uses the stock end date. `beat_local_market_3y` and
`excess_return_local_3y` use the combined end date. Step 6's heuristic
likely-delisted `-50%` returns and derived losing-class labels are marked
`policy_imputed_likely_delisted` and have no availability date. Acquisitions are
not inferred or given a special fallback because the current pipeline has no
reliable event taxonomy; any such policy-imputed or unknown label is ineligible.

## Purged Paths

The shared rule in `modeling/label_eligibility.py` is applied to:

- `modeling/train.py`: static three-year classifier, feature selection,
  regression, walk-forward CV, and OOT diagnostic;
- `modeling/train_regression_model.py`: static and walk-forward three-year
  continuous-target training;
- `modeling/score_oof.py`: three-year expanding-window OOF training;
- `modeling/tune.py`: three-year hyperparameter, CatBoost, calibration, and
  ensemble training population;
- `backtest/engine.py`: three-year LightGBM, decision-tree agreement, and
  regression walk-forward fits.

Non-three-year masks retain their prior behavior; correcting other horizon
availability is outside this bounded session.

## Fold Invariant and Legacy Boundary

For every corrected fold, the eligible training population satisfies:

```text
max(label_end_date) < scoring_date
```

using `stock_label_end_date` for stock-return regression and `label_end_date`
for benchmark-relative targets. Synthetic tests demonstrate before, exact,
after, missing-date, unknown-provenance, policy-imputed, future-row invariance,
and maximum-end-date cases.

The current `data/historical_dataset_clean.parquet` has no persisted label dates
or provenance. The shared helper therefore returns no eligible three-year rows
for that legacy schema rather than inferring dates from fiscal year. The file was
not changed. A usable corrected dataset will be produced only at the controlled
Session 8 rebuild boundary.
