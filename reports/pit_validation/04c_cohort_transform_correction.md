# Session 4C — Filing-Time Cohort Transform Correction

Date: 2026-07-15  
Status: complete  
Scope: bounded T05–T09 and T16 correction under the accepted Session 4C0
filing-materialization contract.

## Outcome

Step 5, Step 6, and alpha factor materialization now use each row's actual
proven publication timestamp. The implementation has no rebalance calendar and
does not reinterpret filing-time ranks as common-date portfolio ranks.

Rows are eligible only when they carry a stable `entity_id`, fiscal year,
period type, market, parseable `availability_timestamp`, and an explicitly
permitted `availability_provenance`. The permitted source values are:

- `sec_primary_filing`;
- `edinet_submission`; and
- `dart_receipt`.

Estimated Korean dates, yfinance statement dates, Brazilian reference dates,
missing provenance, mismatched filing/publication dates, and legacy schemas
fail closed. Later entity-period versions are ineligible; unresolved equal-time
collisions are also ineligible. The current US producer continues to select the
earliest primary filing and does not switch to amendments.

## Corrected transformations

- Step 5 ratio/growth winsorization fits 1st/99th bounds on strictly prior
  proven same-market annual history. Fewer than 50 non-null observations leaves
  the proven row raw and records `raw_sparse`; unproven rows are null.
- Step 5 quality/value composites and momentum ranks use the proven
  `(fiscal_year, market)` cohort available by each timestamp, with equal-time
  rows treated as one batch and a 10-observation minimum per signal.
- Step 5 sector percentiles use proven
  `(fiscal_year, market, sic_2digit)` cohorts with a five-observation minimum
  and no market/global fallback. Rank-derived interactions consume only the
  materialized ranks.
- Step 6 accrual winsorization first uses the proven fiscal-year/market cohort
  available by the row timestamp when it has 20 observations, then strictly
  prior same-market annual history when it has 50, and otherwise retains the
  raw proven value with an explicit sparse method.
- Step 6 size-category imputation ranks `log_assets` over all proven available
  fiscal-year/market peers, not only missing-size rows, and requires 20 values.
- Filing-time value, quality, momentum, growth, and fraud-risk alpha ranks use
  the same proven market cohort and 10-observation minimum. Value and growth
  rank their cohort-winsorized signal, fixing the previously unused temporary
  winsorized series without changing signal definitions, directions, or weights.

Pipeline output carries the publication timestamp, source provenance, contract
version, provenance policy, and sparse/fitted winsorization or imputation method.

## Minimal source provenance plumbing

- US SEC rows certify availability only when every materialized field came from
  an earliest primary 10-K/10-Q/20-F-family filing. Amendment-only or other-form
  fallbacks remain raw extraction rows but are not certified for cohort use.
- Japan records the full EDINET `submitDateTime`.
- Korea certifies only a date derived from the DART receipt number; the existing
  April 1 estimate is explicitly marked estimated and has no availability
  timestamp.
- Canada/EU yfinance statement dates and Brazil CVM reference dates are marked
  `statement_date_unproven` and remain ineligible.

No broader extraction behavior, amendment switching, or artifact migration was
introduced.

## Verification

- Focused Session 4C suite: 113 passed. Coverage includes later-filing and
  later-year invariance, early/late filers, equal-time batching, row shuffles,
  sparse market/sector behavior, source provenance, duplicate versions,
  Step 6 accrual/size behavior, and Step 5-to-alpha cohort agreement.
- Full suite after integration-fixture provenance was added: 592 passed and
  4 skipped. Existing pandas fragmentation/FutureWarning messages remain.
- Targeted modules compile with bytecode redirected to `/tmp`.
- `git diff --check` passes.

No dataset was rebuilt. No data, model, prediction, report artifact (other than
this source-controlled report), or backtest output was generated or overwritten.
No model was trained, and no backtest was run.

## Deferred boundaries

Session 4C does not select or integrate any decision-snapshot/rebalance calendar.
Those calendars remain deferred by investment horizon to Session 9B. Quarterly
lineage, macro/price vintages, full amendment switching, factor definitions and
weights, targets, strategy thresholds, backtest consumers, and downstream
portfolio consumers are unchanged.
