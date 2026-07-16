# Session 4C Prerequisite — Calendar and Eligible-Cohort Contract Proposal

Date: 2026-07-15  
Status: **accepted 2026-07-15; rebalance calendar intentionally deferred by horizon**  
Mode: read-only evidence and documentation. No transformation implementation,
dataset rebuild, artifact generation, model training, or backtest execution.

## Executive decision proposed

Separate two clocks because one timestamp cannot correctly represent both a
filing-time materialized feature and a portfolio decision snapshot:

1. **Filing-materialization clock:** a row's feature value is frozen at its
   proven availability timestamp. It may be used later as “the percentile known
   when this filing arrived,” but it is not a common-date portfolio rank.
2. **Decision-snapshot clock:** must be chosen separately by investment horizon
   and operational rebalance policy. No January, semiannual, or three-times-yearly
   schedule is accepted in this contract.

Session 4C should correct only the horizon-neutral filing-materialization clock.
Historical and production consumers must not reinterpret those filing-time ranks
as common-date portfolio ranks. Horizon-specific decision calendars are deferred
until corrected data/predictions exist and must be frozen without optimizing on
reported test performance.

## Evidence from current code and frozen data

### Current clocks differ

- `backtest/engine.py::run_backtest()` uses January 1 of `fiscal_year + 1` as
  the holding/filing gate.
- `notebooks/production_screener.ipynb` takes every row in the latest US fiscal
  year without an explicit run-time filing cutoff.
- `portfolio/build_portfolio.py` performs annual fiscal-year slices without a
  filing-date gate.
- alpha registry/research paths compute factors on complete fiscal-year cohorts.
- Step 5/6 materialization has a row `filed_date`, but most cohort transforms
  currently use the complete fiscal-year cohort.

The January 1 convention is documented, not accepted as the future production
or comparison calendar. On the frozen annual data, 33.45% of all rows and 17.7% of US rows
have `filed_date < January 1` of the following year. Coverage rises to 86.5% for
US by April 1 and 97.6% by July 1, but changing to either date would change the
scoring calendar and return alignment. Those figures show why Session 4C must
not select a calendar by convenience.

### `filed_date` is not one cross-market concept

- US Step 2 selects the earliest primary SEC filing date in code. The frozen
  snapshot has not been rebuilt since that correction.
- JP Step 2 uses EDINET `submitDateTime`.
- KR Step 2 uses a DART receipt date but silently substitutes `year-04-01` when
  no receipt date is available.
- CA/EU Step 2 uses yfinance statement column dates.
- BR Step 2 uses the statement reference date.

The clean parquet has no availability-provenance field. Date-shape evidence is
consistent with those code paths: 62.8% of CA, 99.1% of BR, and 86.5–100% of
most EU annual rows are dated December 31; 74 KR rows covering multiple fiscal
years share 26 entity/receipt dates. Those values may be conservative retrieval
dates or statement dates, but they cannot all be certified as filing availability
timestamps from the current schema.

The 56 duplicate `(ticker, fiscal_year)` keys are 56 pairs of different CIKs,
not duplicate entity-year filings. The clean annual data has no duplicate
`(cik, market, fiscal_year)` keys. Ticker is therefore not the entity key.

## Proposed contract

### Required row schema

Every cohort-transform input must carry:

- `entity_id`: stable issuer identity; current source is `(market, cik)`, never
  ticker alone;
- `fiscal_year` and `period_type`;
- `availability_timestamp`: actual source publication/filing timestamp;
- `availability_provenance`: enumerated source such as `sec_primary_filing`,
  `edinet_submission`, or `dart_receipt`; and
- `market`, plus `sic_2digit` where sector comparison is requested.

Missing/invalid timestamps, missing provenance, estimated dates, statement/end
dates, and unknown provenance are ineligible. No fiscal-year, quarter-end,
retrieval-date, or global-data fallback may manufacture availability.

Existing `filed_date` may populate `availability_timestamp` only when its source
path records a permitted provenance. Legacy artifacts without provenance remain
uncertified and fail closed; Session 4C does not rebuild them.

### Filing-materialization clock

For a target row with availability timestamp `T`:

- historical fit populations use rows with proven timestamp strictly `< T`;
- equal-`T` rows are one batch for cross-sectional ranks, so row order cannot
  affect results;
- market ranks use `(fiscal_year, market)` peers available by `T`;
- sector ranks use `(fiscal_year, market, sic_2digit)` peers available by `T`;
- rows from later timestamps, even in the same fiscal year or quarter, cannot
  change the target row; and
- the materialized value is explicitly identified as a filing-time feature, not
  as a rank at a later portfolio decision date.

### Decision-snapshot clocks are deferred

The correct rebalance frequency may differ by investment horizon. Session 4C
therefore does not choose or implement a production or historical rebalance
schedule. Existing January 1 backtests remain legacy behavior to be audited, not
the accepted forward design.

Every future decision-snapshot consumer must supply its decision timestamp
explicitly, select only rows proven available before it, and recompute
cross-sectional ranks from that snapshot's raw signals. A stored filing-time
percentile can never masquerade as a common-date percentile. Until a horizon's
calendar is accepted, that consumer must fail closed or remain outside official
performance claims.

### Transformation-specific populations and sparse rules

1. **Step 5 ratio/growth winsorization:** fit 1st/99th bounds on prior proven
   annual rows in the same market with timestamps `< T`. Minimum 50 non-null
   observations. With fewer than 50, leave the raw value unchanged and record
   that no bound was fitted; never include later current-quarter rows or another
   market/global population.
2. **Step 5 market composites and momentum ranks:** rank the eligible
   `(fiscal_year, market)` as-of cohort. Minimum 10 non-null observations per
   signal; otherwise return null for that signal. Rank-derived interactions use
   only those resulting ranks.
3. **Step 5 sector percentiles:** rank the eligible
   `(fiscal_year, market, sic_2digit)` cohort. Minimum 5 non-null peers; otherwise
   return null. Do not fall back to market-wide or global ranks.
4. **Step 6 accrual winsorization:** first use the eligible
   `(fiscal_year, market)` cohort when it has at least 20 non-null observations.
   Otherwise use prior proven same-market annual history with at least 50
   observations. If neither exists, leave the raw value unchanged and record no
   fitted bound. Never use the full dataframe.
5. **Step 6 size-category imputation:** compute `log_assets` ranks over all
   eligible rows in the `(fiscal_year, market)` cohort, not only rows whose size
   is missing. Require 20 non-null observations; otherwise leave size missing.
6. **Alpha factors:** use the same eligible market cohort and minimum 10
   observations per signal. Value/growth winsorization is fitted within that
   eligible cohort and the winsorized signal is the value ranked. Sector and
   market definitions, signal directions, and factor weights do not change.

All fitted parameters/ranks must carry their decision/materialization timestamp,
population identity, grouping keys, minimum-count rule, and provenance policy.

### Amendments and duplicate records

The current US producer intentionally preserves the earliest primary filing and
does not switch to later amendments. This proposal retains that source-vintage
policy for Session 4C; implementing full amendment switching is deferred.

For any future input containing multiple rows for the same
`(entity_id, fiscal_year)`:

- different proven timestamps are versions, but only the earliest-primary
  version is permitted under the current policy;
- conflicting rows with the same timestamp and no accession/source precedence
  fail closed for that entity-year; and
- ticker collisions do not deduplicate distinct entity IDs.

### Fail-closed market policy

Session 4C code may operate only on rows whose timestamp provenance is explicitly
permitted. A market is not allowlisted by name alone. Source paths must emit the
provenance field; mixed exact/estimated sources such as current KR require row-
level provenance. Unsupported rows receive null cohort transforms or a clear
consumer error, depending on whether a row-level or snapshot API is used.

This permits generic code without claiming that current legacy non-US dates are
PIT-safe. Adding source provenance in producer code is allowed only as required
schema enforcement; rebuilding data remains Sessions 8–9 work.

## Acceptance choices

The rebalance decision is explicitly **deferred by horizon**, not guessed. The
user accepted these technical materialization rules on 2026-07-15:

1. use each row's actual proven publication timestamp for filing-time features;
2. require row-level timestamp provenance and fail closed for legacy/estimated
   dates instead of allowlisting markets by inference; and
3. retain earliest-primary/no-amendment-switching for Session 4C.

Session 4C may now implement the bounded T05–T09/T16 event-time transforms and
tests without changing data or models. This acceptance does not authorize a
rebalance schedule or common decision-snapshot integration.

## Deferred boundaries

- every production/historical rebalance calendar, to be chosen by horizon after
  corrected data and predictions exist and before controlled official backtests;
- full accession-level amendment vintage reconstruction;
- quarterly source-row availability (T10);
- macro and adjusted-price vintages;
- T11/T31 backtest gates and static-score fallbacks; and
- rebuilding or comparing corrected datasets/models/backtests.
