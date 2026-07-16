# Session 8C — Training-Label Market-Input Validation

## Decision

Session 8C is a no-go for all five accepted horizons. `6m`, `1y`, `2y`, `3y`,
and `5y` each have zero supported rows. All 43,906 rows in the official US
annual candidate population are excluded at the filing-provenance/entity gate,
before a row can qualify as market-input unavailable or supported. Session 9
must not begin.

This is a validation result, not a repaired dataset. No existing filing-date-
start label is reinterpreted as a Session 8B calendar-aligned label. The frozen
machine-readable result is
`artifacts/pit_validation/training_label_market_inputs/session8c_validation_manifest.json`.

## Manifest and payload validation

Both starting JSON artifacts parsed before their evidence was used:

| Artifact | SHA-256 | Result |
|---|---|---|
| Session 8B calendar contract (4,375 bytes) | `13cd7494aa7f0ff6e3f8a11efa0ee7a9a087968bb20b1fd5a9cb57f380148296` | Five accepted and six excluded horizons; required fields and separate populations present |
| Corrected-partial manifest (12,050 bytes) | `10b648c581ed56d6b6acb7f16d786be00409f4dfc96f0e92073693276e51b6ee` | All 31 referenced inputs, outputs, corrected-code files, and dirty-state records matched recorded sizes and streaming SHA-256 hashes |

Raw hash output is in `/tmp/session8c_manifest_validation.log`. The validated
final parquet has 191,579 rows: 59,378 annual and 132,201 quarterly. Official
Session 9 scope contains 43,906 US annual rows; the other 147,673 rows are
outside that row scope.

## Fail-closed classification

Counts below are mutually exclusive. `excluded` takes precedence for an
out-of-scope row or a failure of entity, filing provenance, calendar alignment,
or semantic proof. `unavailable` is reserved for an otherwise admissible row
that reaches the market-input gate but lacks a required observation. A
`supported` row must pass every gate.

| Horizon | US annual rows | Supported | Unavailable after admissibility gates | Excluded | Verdict |
|---|---:|---:|---:|---:|---|
| 6m | 43,906 | 0 | 0 | 43,906 | Unsupported |
| 1y | 43,906 | 0 | 0 | 43,906 | Unsupported |
| 2y | 43,906 | 0 | 0 | 43,906 | Unsupported |
| 3y | 43,906 | 0 | 0 | 43,906 | Unsupported |
| 5y | 43,906 | 0 | 0 | 43,906 | Unsupported |

The same zero-supported verdict applies separately to `observed_only` and
`include_policy_imputed`. Policy labels do not cure missing issuer/filing
provenance, calendar incompatibility, or market-input semantics, so neither
population reaches the availability gate. They remain separate and are not
unioned.

## Filing and identity evidence

The frozen Step 2 snapshot and all four corrected-partial lineage parquets have
191,579 rows and omit all three required fields: `entity_id`,
`availability_timestamp`, and `availability_provenance`. The US annual rows do
have non-null `cik`, ticker, and `filed_date`, but the accepted contract forbids
inferring the missing fields from them. Earliest-primary filing selection,
effective local-time availability, and ambiguity handling therefore cannot be
proven row by row.

Consequently, strict `label_end_date < decision_timestamp` cannot certify any
row. Existing end dates belong to labels started at each row's filing date and
fixed day counts. Even the 7,357 apparent 6m and 1,658 apparent 1y observed
labels ending before July 2 of `fiscal_year + 1` are not accepted evidence;
2y, 3y, and 5y have zero such apparent rows. Those counts cannot be promoted or
reinterpreted under the Session 8B calendar.

## Frozen payload coverage, not contract support

For transparency, the table below describes the incompatible filing-date-start
payload within the 43,906 US annual rows. `Unavailable` here means neither the
recorded observed provenance nor the separately recorded policy provenance is
present. These are diagnostics only; none is a supported calendar-aligned row.

| Horizon | Recorded observed stock and relative labels | Separate policy-imputed labels | Recorded payload unavailable | Contract-supported observed-only | Contract-supported include-policy |
|---|---:|---:|---:|---:|---:|
| 6m | 38,742 | 46 | 5,118 | 0 | 0 |
| 1y | 38,094 | 59 | 5,753 | 0 | 0 |
| 2y | 33,944 | 215 | 9,747 | 0 | 0 |
| 3y | 30,121 | 292 | 13,493 | 0 | 0 |
| 5y | 23,092 | 377 | 20,437 | 0 | 0 |

## Market-input evidence and stop conditions

- Benchmark cache presence is not benchmark proof. Frozen `SPY`, `IWC`, `IWM`,
  and `MDY` entries exist through June 18, 2026, but the SQLite schema stores
  only ticker, `fetched_at`, and a date/value JSON object.
- Benchmark assignment is not contract-aligned. It uses filing-date entry price
  and shares, not proven decision-time size. Of 43,906 US annual rows, 12,636
  have missing `market_cap_at_filing`; the code defaults them to SPY, while the
  contract requires missing size to fail closed. Recorded assignments are SPY
  17,870, IWC 10,585, IWM 8,277, and MDY 7,174.
- Common entry/exit cannot be proven. The stock entry is recorded only in one
  shared `label_start_date`; benchmark entry is not persisted. Stock and
  benchmark dates were resolved independently. Recorded exits differ for 3
  observed 6m rows, 1 observed 1y row, and 1 observed 3y row; matching recorded
  exits do not prove the missing common-entry or accepted decision-window rule.
- Trading-calendar semantics are unknown. Cache timestamps are timezone-naive
  dates with no exchange calendar, session type, holiday provenance, or
  regular-session flag.
- Adjustment semantics and vintage are unknown. Current code requests
  yfinance `Adj Close`, but cache payloads do not preserve vendor/version,
  requested field, adjustment policy, split/dividend components, corporate-
  action lineage, or revision vintage. A fetch timestamp alone does not prove
  total-return semantics or adjustment vintage.
- Observed stock/relative markers describe the old computation only. They do
  not establish contract-aligned prices, a common stock/benchmark window,
  benchmark assignment, trading sessions, or adjustment provenance.

Each condition independently stops certification. Missing filing provenance
and entity identity stop every row first; the remaining market-evidence gaps
would still prevent support even if that first gate were ignored.

## Required correction and exact next task

A corrected Step 2 rebuild is required. The smallest next task is a clean,
provenance-preserving US Step 2 rebuild from the already-supported SEC primary-
filing logic, persisting stable `entity_id`, `availability_timestamp`, and
`availability_provenance`. Stop after validating and freezing that rebuilt Step
2 output. Do not rebuild downstream stages, generate labels, train, predict,
backtest, compare production performance, optimize thresholds, or begin Session
9 in that task.

Session 8C is complete because every accepted horizon has an explicit excluded
verdict. It supports no horizon and authorizes no frozen-cache-conditioned
diagnostic model output.
