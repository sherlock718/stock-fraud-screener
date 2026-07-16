# Session 2B — Horizon Eligibility and Survivorship Sensitivity Summary

Date: 2026-07-15  
Scope: code, roadmap, and synthetic tests only. No dataset, model, prediction, or
backtest artifact was rebuilt or overwritten.

## Why Session 2B Was Added

Session 2 fixed the production three-year label leak. The same pipeline also
trains 6m, 1y, 2y, and 5y targets, so leaving those paths unchanged would retain
future-label leakage and require another dataset rebuild later.

The original observed-only rule also needed an explicit survivorship caveat.
Companies with missing terminal prices remain in the historical universe, but
training only on observed outcomes may be optimistic when failures are more
likely to be missing. Conversely, treating every disappearance as a true `-50%`
delisting return mislabels acquisitions, restructurings, ticker changes, and
other events. Session 2B therefore makes these separate reproducible scenarios.

## Canonical Horizon Schema

Step 3 persists a common actual stock `label_start_date` and, for every generated
horizon `{h}`:

```text
stock_label_end_date_{h}
benchmark_label_end_date_{h}
label_end_date_{h}
stock_label_provenance_{h}
benchmark_label_provenance_{h}
label_provenance_{h}
```

Stock returns use the actual stock exit date. Benchmark-relative labels use the
later actual stock/benchmark exit date. Observed eligibility remains strict:
the appropriate end date must be earlier than the scoring date, not equal to it.

## Two Separate Training Populations

`observed_only` is the default everywhere. It accepts only non-null targets with
actual dates and observed-price provenance.

`include_policy_imputed` is an explicit sensitivity mode. Step 6 keeps the
company in the universe, writes the existing `-50%` assumption for missing final
returns, marks it `policy_imputed_likely_delisted`, and persists:

```text
policy_stock_label_available_date_{h} = max(
    filed_date + target_horizon,
    filed_date + no_filing_detection_lag,
)

policy_label_available_date_{h} = max(
    policy_stock_label_available_date_{h},
    actual benchmark exit date,
)
```

The policy label is sensitivity-eligible only when that date is strictly before
the scoring date. This prevents the final dataset's knowledge of future filing
absence from entering earlier folds. The policy mode is available through
`--label-policy include_policy_imputed` in classification, regression, OOF,
tuning, and backtest model-scoring commands. Metadata records the selected mode.
OOF sensitivity columns/reports and backtest JSON use a `_policy` suffix so they
do not overwrite the observed-only outputs. Session 9 will additionally place
all trained models under separate artifact directories.
Sensitivity backtests also discard legacy static score columns and do not load
static models with unknown/different label policy; early years without enough
eligible sensitivity history remain unscored instead of silently falling back.

The policy return is not asserted to be a real delisting return. Observed-only
and sensitivity models, predictions, and backtests must remain separate in
Sessions 8–10 and 14–15.

## Coverage

The shared rule now applies to every trained horizon in:

- `modeling/train.py`;
- `modeling/train_regression_model.py`;
- `modeling/score_oof.py`;
- `modeling/tune.py`;
- `backtest/engine.py`.

Step 3 stores dates for all eleven generated horizons. Step 6 creates the
existing conservative sensitivity treatment across all five trained horizons:
6m, 1y, 2y, 3y, and 5y.

## Remaining Limitations

- The current clean parquet has none of the new horizon-qualified fields and is
  intentionally training-ineligible until the controlled Session 8 rebuild.
- `-50%` is a stress assumption, not event truth. Paid event/security-master
  data is still required to distinguish bankruptcy, acquisition, delisting
  proceeds, ticker changes, restructurings, and migrations.
- Observed-only results may be optimistic; policy-sensitivity results may be too
  negative for acquisitions and not negative enough for total losses. The two
  runs provide an uncertainty range, not proof that either is unbiased.
- Portfolio-return missingness and static-score fallback behavior remain part of
  the scheduled Session 5 backtest-path audit; Session 2B changes model-label
  eligibility, not official performance measurement.
