# Session 4A — Global Transformation Correction

Date: 2026-07-15  
Scope: accepted findings T12–T15 only. No dataset rebuild, artifact generation,
model training, backtest change, or Session 4B/4C work was performed.

## Corrected contract

### T12–T13: downstream growth loaders

`modeling/constants.py::load_data()` and
`research/factor_research.py::load_data()` now preserve materialized
`growth_yoy` values exactly. They no longer estimate independent 1st/99th
percentile bounds from the rows being loaded. The canonical materialization path
remains the pipeline; fixed target-return clipping is unchanged.

### T14: fraud-taxonomy clipping and ranking

The active owner remains `pipeline/enrich_fraud_taxonomy.py`. Each rank-based
taxonomy component now uses an expanding as-of population keyed by `filed_date`:
only rows whose filing timestamp is less than or equal to the scored row's filing
timestamp contribute clipping bounds and percentile ranks. Rows with the same
timestamp enter as one batch, so their results do not depend on dataframe order.

The implementation uses an order-statistic count tree, preserving pandas-style
linear quantiles and average-tie percentile ranks without repeatedly scanning the
full historical population. A missing or invalid `filed_date` fails closed; no
fiscal-year or undated global fallback is allowed.

This correction does not define or change fiscal-year cohort semantics. Annual
cohort ranks, scoring-calendar decisions, and eligible-cohort rules remain in the
accepted Session 4C boundary.

### T15: dilution EPS history

EPS percentage change is now computed within ticker after chronological ordering
by filing timestamp, with fiscal year as an additional ordering key when present.
The result is restored to the input row keys before the as-of dilution rank is
computed. Another ticker's EPS values therefore cannot enter a company's EPS
history, and input shuffling cannot change the aligned dilution score.

## Invariance evidence

Focused tests verify that:

- both downstream loaders preserve already-materialized extreme growth values;
- appending a later-year outlier does not change earlier loaded growth values;
- neither downstream loader calls a quantile estimator;
- later/unavailable filings do not change earlier taxonomy values;
- all taxonomy sub-scores are unchanged by row shuffling after ticker alignment;
- dilution remains order-invariant for repeated ticker histories; and
- changing one ticker's EPS history does not change another ticker's EPS growth.

## Verification

`PYTHONDONTWRITEBYTECODE=1 python3 -m pytest
tests/pipeline/test_enrich_fraud_taxonomy.py
tests/modeling/test_loader_invariance.py
tests/research/test_factor_research.py -q` → **77 passed**.

`git diff --check` → passed.

A read-only 58,000-row synthetic timing check completed one expanding as-of rank
in approximately 0.56 seconds. It wrote no repository artifact.

No data parquet, model, prediction, report output other than this required
correction report, or backtest artifact was rebuilt or regenerated.
