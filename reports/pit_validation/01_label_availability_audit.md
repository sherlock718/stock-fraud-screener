# Session 1 — Three-Year Label-Availability Audit

**Date:** 2026-07-15  
**Scope:** Read-only audit of the three-year modeling and backtest paths  
**Verdict:** **Confirmed target leakage.** Every audited walk-forward path admits
three-year outcomes that were not fully observable at its stated fold scoring
date. `modeling/train.py` with the default `embargo_years=0` is affected in every
three-year fold. `embargo_years=1` reduces neither the issue nor its order of
magnitude enough to establish label availability.

No production code, dataset, price cache, model, prediction, or backtest artifact
was modified or regenerated.

## Reproducibility Boundary

- Code commit inspected: `3f706e3e10d2b354c6e8b9407760fa2074749c0a`
- Worktree was already dirty with documentation/configuration files before this
  audit; this report does not assume those files were part of the commit.
- Dataset:
  `data/historical_dataset_clean.parquet`, SHA-256
  `520a9b52e2a63d013a3527abbcde32c484a226c2739450d2a6a48ab175144dae`
- Price cache: `data/price_cache.db`, SHA-256
  `d0e7c3ee05d89751ad86c3a2a763bbc322672448634e345b3a1a982c647c3def`
- Cache contents: 7,465 symbols, fetched between 2026-06-22 02:21:40 and
  2026-06-22 14:55:30 (stored UTC-naive timestamps).
- Model-loader population: 58,013 annual, deduplicated ticker/fiscal-year rows
  spanning FY2008–FY2025; 36,185 have non-null three-year stock, benchmark,
  binary, and excess-return labels.
- The raw backtest loader uses all 58,190 annual rows without the modeling
  loader's ticker/fiscal-year deduplication.

## Confirmed Target Definition

### Price and date convention in current code

`pipeline/step3_enrich_prices.py` defines the three-year horizon as 1,095
calendar days.

| Element | Current implementation |
|---|---|
| Information/as-of date | `filed_date`; the clean dataset's `as_of_date` equals `filed_date` for all 58,013 model rows |
| Intended entry date | `filed_date` |
| Actual entry date | First adjusted-close observation on or after `filed_date`, within 5 calendar days |
| Intended exit date | `filed_date + 1,095 calendar days` |
| Actual exit date | First adjusted-close observation on or after the intended exit, within 10 calendar days |
| Stock return | `exit_adjusted_close / entry_adjusted_close - 1`; cumulative return, not CAGR |
| Benchmark return | Same formula and dates, using the row's size/market benchmark |
| Classification label | `beat_local_market_3y = int(stock_return > benchmark_return)` |
| Continuous excess label | `excess_return_local_3y = stock_return - benchmark_return` |
| Main `train.py` regressor | `forward_return_3y`, clipped to `[-1, 5]` during fitting |
| Standalone regression target | Prefers `excess_return_local_3y`; falls back to `forward_return_3y` |

The source fetches yfinance `Adj Close` with `auto_adjust=False` and
`actions=False`. This is split/dividend adjusted according to yfinance's stored
series. There is no explicit mapping for mergers, acquisitions, ticker changes,
reorganizations, migrations, or delisting proceeds.

### Missing terminal price and corporate actions

- Step 3 returns null when either entry or exit is unavailable within its lookup
  window; it does not use the last observed price.
- `pipeline/step6_clean.py` contains a later heuristic that calls a ticker
  "likely delisted" when its last filing is at least three fiscal years behind
  the dataset maximum, then fills a missing final-row 1y/3y/5y stock return with
  `-50%` and sets a missing beat label to zero.
- That heuristic is based on the full dataset's maximum year, not an as-of fold
  state, and it does not distinguish bankruptcy from acquisition, ticker change,
  restructuring, or data loss.
- The audited clean parquet does not contain `likely_delisted` or persisted
  label-date/provenance columns. It contains 76 exact `-0.50` three-year stock
  returns. Their origin cannot be identified reliably from the clean parquet;
  71 reproduce from the current cache as market returns and five do not.

## Exit-Date Reconstruction

The clean parquet persists return values but not the actual entry/exit trading
dates. I reconstructed dates from the frozen SQLite cache using the exact Step 3
lookup rule and validated the reconstructed values against the stored stock and
benchmark returns.

| Result | Rows |
|---|---:|
| Non-null three-year labels | 36,185 |
| Stock return reproduced | 33,102 |
| Benchmark return reproduced | 34,572 |
| Both reproduced (full binary/excess-label reconstruction) | 33,102 |
| Not fully reproducible from the current cache | 3,083 |

For the 33,102 fully reproduced labels, stock exit lag after the intended date
was 0 days for 19,364 rows, 1 day for 7,029, 2 days for 5,674, 3 days for 1,004,
and 4 days for 31. Stock and benchmark actual exit dates differed in only one
row. This confirms the implemented date convention, but it does not make the
3,083 remaining historical exit dates reliable.

Non-reconstructed labels by market: KR 1,613; US 1,233; CA 137; DE 46; BR 34;
JP 17; FI 2; FR 1. Because the dataset does not persist dates or label
provenance, those rows are a blocker to an exact all-row purge count. They are
not guessed below.

## Eligibility Rule Used for This Audit

The roadmap requires a complete target to be observable **before** the scoring
date. The appropriate strict rule is:

```text
label_end_date < scoring_date
```

For a stock-only `forward_return_3y` target, `label_end_date` is the actual stock
exit trading date. For `beat_local_market_3y` or `excess_return_local_3y`, it is
the later of the stock and benchmark actual exit trading dates. A missing end
date is ineligible.

The fold tables below use a deliberately conservative evidence classification:

- **Definitely incomplete:** intended exit date is on or after the scoring
  date. Since the actual exit cannot precede the intended exit, these rows are
  unquestionably unavailable.
- **Boundary ambiguous:** intended exit precedes the scoring date, but the
  permitted 10-day lookup window reaches or crosses it. These are not counted as
  definitely incomplete without the persisted actual date.
- **Definitely complete:** the entire permitted exit window ends before the
  scoring date.

Thus the reported affected counts are lower bounds, not estimates.

## Current Training Masks and Affected Rows

### Shared modeling/OOF mask

The default three-year masks in `train.py`,
`train_regression_model.py`, and `score_oof.py` are equivalent over their
overlapping folds:

```text
fiscal_year < scoring_year
AND filed_date < January 1 of scoring_year
AND three-year target is non-null
```

`train.py` expresses the fiscal-year term as `fiscal_year <= t` where the test
year is `t + 1`. It filters recent *test folds* using the dataset's maximum
fiscal year, but never filters training rows by their label end date.

The table lists every default scoring date used by at least one audited modeling
path. Applicability: OOF = 2014–2025; `train.py` walk-forward = 2015–2021;
standalone regression walk-forward = 2015–2023.

| Scoring date | Labeled training rows | Definitely incomplete | Boundary ambiguous | Definitely complete |
|---|---:|---:|---:|---:|
| 2014-01-01 | 4,684 | 4,331 | 30 | 323 |
| 2015-01-01 | 6,611 | 5,477 | 44 | 1,090 |
| 2016-01-01 | 8,666 | 5,810 | 47 | 2,809 |
| 2017-01-01 | 10,955 | 6,271 | 43 | 4,641 |
| 2018-01-01 | 13,342 | 6,731 | 42 | 6,569 |
| 2019-01-01 | 15,900 | 7,234 | 48 | 8,618 |
| 2020-01-01 | 18,819 | 7,864 | 47 | 10,908 |
| 2021-01-01 | 21,958 | 8,616 | 45 | 13,297 |
| 2022-01-01 | 26,903 | 11,002 | 49 | 15,852 |
| 2023-01-01 | 32,761 | 13,942 | 53 | 18,766 |
| 2024-01-01 | 36,185 | 14,227 | 61 | 21,897 |
| 2025-01-01 | 36,185 | 9,282 | 1,196 | 25,707 |

The unusually large 2025 boundary is still unresolved by persisted lineage; it
must not be silently treated as complete.

### `train.py` embargo comparison

`embargo_years=0` is the default. `embargo_years=1` changes only the fiscal-year
cap to `fiscal_year <= scoring_year - 2`; it does not enforce a three-year target
end date.

| Scoring date | Default labeled / incomplete | Embargo 1 labeled / incomplete |
|---|---:|---:|
| 2015-01-01 | 6,611 / 5,477 | 6,226 / 5,092 |
| 2016-01-01 | 8,666 / 5,810 | 8,204 / 5,348 |
| 2017-01-01 | 10,955 / 6,271 | 10,483 / 5,799 |
| 2018-01-01 | 13,342 / 6,731 | 12,863 / 6,252 |
| 2019-01-01 | 15,900 / 7,234 | 15,373 / 6,707 |
| 2020-01-01 | 18,819 / 7,864 | 18,257 / 7,302 |
| 2021-01-01 | 21,958 / 8,616 | 21,371 / 8,029 |

**Answer to the roadmap question:** yes, `embargo_years=0` admits incomplete
outcomes in every fold. An embargo of one fiscal year also admits incomplete
outcomes in every fold and is not a valid substitute for timestamp-based
eligibility.

### Static training paths

| Path and effective scoring cutoff | Labeled training rows | Definitely incomplete | Boundary ambiguous | Definitely complete |
|---|---:|---:|---:|---:|
| `train.py` default, 2021-01-01 | 21,958 | 8,616 | 45 | 13,297 |
| `train.py` OOT diagnostic, 2020-01-01 | 18,819 | 7,864 | 47 | 10,908 |
| `train_regression_model.py` default, 2023-01-01 | 32,761 | 13,942 | 53 | 18,766 |

The OOT diagnostic's test year may have an elapsed horizon at execution time,
but its training target mask is still contaminated at its claimed 2020 cutoff.

### Backtest walk-forward paths

`backtest/engine.py` loads all annual rows, then for each score year uses
`fiscal_year < score_year`, non-null target, and `filed_date < January 1`. The
LightGBM classifier and decision tree share this mask. The three-year regressor
adds the clean-stock filters after selecting non-null targets. None checks a
label end date.

| Scoring date | Classifier/tree labeled / incomplete | Clean regressor labeled / incomplete |
|---|---:|---:|
| 2013-01-01 | 2,862 / 2,845 | 1,698 / 1,687 |
| 2014-01-01 | 4,692 / 4,337 | 2,722 / 2,458 |
| 2015-01-01 | 6,620 / 5,482 | 3,807 / 3,026 |
| 2016-01-01 | 8,677 / 5,815 | 4,911 / 3,213 |
| 2017-01-01 | 10,968 / 6,276 | 6,213 / 3,491 |
| 2018-01-01 | 13,358 / 6,738 | 7,543 / 3,736 |
| 2019-01-01 | 15,920 / 7,243 | 8,893 / 3,982 |
| 2020-01-01 | 18,843 / 7,875 | 10,449 / 4,236 |
| 2021-01-01 | 21,986 / 8,628 | 12,088 / 4,545 |
| 2022-01-01 | 26,935 / 11,014 | 13,836 / 4,943 |
| 2023-01-01 | 32,798 / 13,955 | 16,250 / 5,801 |
| 2024-01-01 | 36,222 / 14,236 | 17,696 / 5,608 |

Boundary-ambiguous counts for classifier/tree range from 0 to 65 per fold; for
the clean regressor they range from 0 to 30. They are excluded from the
"incomplete" counts above.

## Test Coverage Found

`tests/pipeline/test_step3_enrich_prices.py` verifies that entry prices are on or
after filing, exits are after entry, missing exits return null at Step 3, adjusted
close is used, and forward returns ignore pre-filing prices. Those are useful
target-construction tests.

No inspected test asserts any of the required training invariants:

- `label_end_date < scoring_date`;
- exact-scoring-date exclusion;
- missing label-end exclusion;
- maximum training label end per fold;
- future-target invariance;
- consistent stock/benchmark availability for binary and excess targets.

## Confirmed Findings, Inferences, and Unknowns

### Confirmed

1. Actual training masks use fiscal year, filing date, and target non-nullness,
   not target availability.
2. Every audited three-year walk-forward fold contains definitely incomplete
   labeled outcomes.
3. Default `embargo_years=0` is unsafe; `embargo_years=1` remains unsafe.
4. The dataset persists no label start/end dates or label provenance.
5. The current cache reproduces 33,102 complete stock-plus-benchmark labels and
   validates the intended/actual date rule for those rows.
6. Missing/corporate-action treatment is heuristic and not event-specific.

### Methodological recommendation (not yet implemented)

Persist or deterministically derive:

```text
label_start_date = actual stock entry trading date
stock_label_end_date = actual stock exit trading date
benchmark_label_end_date = actual benchmark exit trading date
label_end_date = stock_label_end_date                         # stock return
label_end_date = max(stock_label_end_date, benchmark_label_end_date)  # beat/excess
eligible = label_end_date.notna() AND label_end_date < scoring_date
```

Use a strict `<` convention: a close observed on the scoring date is not assumed
available before that date's score. A policy-imputed label needs explicit
provenance and an availability date; it must not inherit the intended market
exit date automatically.

### Unresolved blockers

1. Exact actual exit dates and provenance for 3,083 labeled rows cannot be
   reconstructed reliably from the current clean parquet/cache pair.
2. The source artifact that produced each stored label is not linked by hash or
   fetch timestamp.
3. The clean dataset does not identify which `-50%` values were observed returns
   versus survivorship-policy imputations.
4. No event taxonomy distinguishes delisting, bankruptcy, acquisition, ticker
   change, restructuring, or migration.
5. Scoring is represented as January 1 of a fiscal-year label while the rows
   being scored are generally filed later. Session 2 should enforce the existing
   fold cutoff first and document this convention; changing the portfolio
   scoring convention is a separate, potentially broader decision.

## Stop Condition and Next Gate

The audit can conclusively answer the Session 1 objective—historical folds did
train on outcomes not yet fully observable—but cannot produce exact dates for
every affected row. Per the roadmap stop condition, this report stops here: no
dates were guessed, and no rebuild, purge, retraining, or code fix was performed.

Before Session 2, accept the strict timestamp convention above and decide how
policy-imputed/missing-provenance labels should be treated. The safest default is
to make them ineligible until provenance and availability are explicit.

## Accepted Session 2 Contract — 2026-07-15

The user accepted the audit findings and authorized the following implementation
contract for Session 2:

1. A label may enter a fold's training set only when its complete target was
   observable strictly before that fold's scoring date:
   `label_end_date < scoring_date`. Equality is excluded.
2. For an observed stock-return target, `label_end_date` is the actual stock exit
   trading date. For benchmark-relative binary or excess-return targets, it is
   the later of the actual stock and benchmark exit trading dates.
3. The canonical pipeline will persist actual trading dates and explicit label
   provenance. It will not infer availability from fiscal year alone.
4. A label with a missing date, unknown provenance, or policy-imputed return is
   ineligible for model training until both provenance and an availability date
   are explicit. There is no silent legacy fallback.
5. Session 2 retains the current January 1 fold scoring-date convention so the
   fix remains bounded. Whether portfolio scoring should instead follow actual
   filing/rebalance dates is recorded for later audit and is not part of Session
   2.
6. Session 2 changes code and synthetic/focused tests only. It does not mutate
   the current clean parquet, rebuild data, retrain models, or claim corrected
   performance. The corrected dataset is produced later at the roadmap's
   controlled rebuild boundary.

This closes the Session 1 decision gate. Session 2 may proceed.
