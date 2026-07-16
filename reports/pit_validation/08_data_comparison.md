# Session 8 — CORRECTED_PARTIAL Data Comparison

Date: 2026-07-15

## Outcome and boundary

Session 8 built `CORRECTED_PARTIAL` through only offline Step 3, Step 5,
Step 6, and fraud-taxonomy enrichment. It uses the frozen stale pre-fix Step 2
snapshots and is conditioned on the frozen incomplete daily cache. It does not
reproduce `LEGACY_SAVED`, which remains non-reproducible evidence only.

The Session 7A manifest and every frozen payload/source hash were validated
before writing. The manifest hash was `31c4f4e2...`; snapshots, daily cache, and
macro hashes were respectively `f0849247...`, `d0e7c3ee...`, and `7eca9aa4...`.
The reference-only `data/prices.parquet` was frozen at the expected
`ead68437...` hash and was not used by any pipeline stage. Frozen inputs and
`LEGACY_SAVED` remained byte-identical.

The price cache was opened with SQLite `mode=ro&immutable=1`. Missing symbols
return unavailable/null, frozen empty series remain empty/unavailable, and the
offline path cannot call the fetcher or write the cache. All working files,
checkpoints, intermediates, outputs, lineage, and comparisons are under
`artifacts/pit_validation/corrected_partial/`.

## Data-level result

| Measure | LEGACY_SAVED | CORRECTED_PARTIAL |
|---|---:|---:|
| Rows | 58,190 | 191,579 |
| Annual rows | 58,190 | 59,378 |
| Quarterly rows | 0 | 132,201 |
| Columns | 367 | 327 |
| Exact six-field key duplicates | 0 | 0 |
| Common exact keys | 54,301 | 54,301 |

The exact key is `(cik, ticker, filed_date, fiscal_year, fiscal_quarter,
period_type)`. `CORRECTED_PARTIAL` adds 137,278 keys and lacks 3,889 legacy
keys. Of the additions, 132,201 are explained quarterly rows retained from the
broader frozen Step 2 input. The remaining 5,077 annual additions and all 3,889
annual removals are unresolved stale-snapshot/legacy-universe differences and
must be treated as unexpected until separately explained. Every affected key is
recorded in `comparison/row_key_differences.csv`; year/market/period counts are
in `comparison/row_counts_by_year_market.csv`.

All retained rows have non-null filing dates. The 54,301 common keys have exact
filing-date equality because filing date is part of the comparison key; any
filing-date shift therefore appears explicitly as one removal plus one addition.
Per-market minimum, maximum, null, and unique-date counts are in
`comparison/filing_date_summary.csv`.

Market scope differs materially. Examples: BR grows from 688 legacy annual rows
to 3,833 annual plus 10,399 quarterly rows; KR changes from 2,538 annual rows to
453 annual plus 1,273 quarterly rows; US changes from 44,059 annual rows to
43,906 annual plus 120,529 quarterly rows. The exhaustive fiscal-year/market
table is machine-readable rather than truncated here.

The schemas share 242 columns; 125 are legacy-only and 85 corrected-only.
Legacy-only fields include saved ML/OOF scores, alpha outputs, fraud labels, and
many all-null features removed by Step 6. Corrected-only fields include actual
horizon label dates/provenance, policy-availability dates, and event-time
provenance. One shared dtype differs: `macro_regime` is `float64` in legacy and
`int64` in corrected. Complete dtypes, schema presence, and missingness are in
`comparison/schema_missingness_and_changes.csv`.

Among the 54,301 common exact keys, every row changes in at least one shared
non-key value. Two hundred shared columns change; 36 are unchanged. The largest
changed populations include `data_confidence` and four populated taxonomy
scores (54,301 each), `fraud_score_quality` (51,667), several corrected
distress/leverage features (about 49,000), and price/label fields. Every shared
column's exact changed/unchanged row count is in
`comparison/shared_column_value_changes.csv`; no value-change category is
silently omitted.

Material unexpected missingness remains visible rather than repaired:

- `fraud_score_accounting` is null on all 191,579 rows because its available
  inputs are absent or all-null after the stale Step 2/Step 6 lineage.
- `accruals_to_assets` is all-null versus 16.9% missing in legacy, and
  `max_accruals_ttm` is all-null versus 33.0% missing in legacy.
- `size_category` is missing on 72,799 corrected rows (38.0%) versus 1.8% in
  legacy; Step 6 could not impute any of those rows from the available evidence.
- Conversely, several leverage/distress features have substantially better
  corrected coverage; these are still lineage differences, not proof of
  correctness or performance.

## Label dates, provenance, eligibility, and balance

`LEGACY_SAVED` has raw targets but no horizon-qualified label dates or
provenance, so zero rows are structurally certifiable under the corrected
contract. `CORRECTED_PARTIAL` materializes actual dates/provenance. Selected
structural counts (no scoring cutoff) are:

| Horizon | Observed stock | Policy stock | Observed relative | Policy relative |
|---|---:|---:|---:|---:|
| 6m | 164,140 | 57 | 164,140 | 57 |
| 1y | 155,717 | 70 | 155,717 | 70 |
| 2y | 138,274 | 277 | 138,274 | 277 |
| 3y | 121,695 | 381 | 121,694 | 381 |
| 5y | 91,106 | 470 | 91,106 | 470 |
| 15y | 3,784 | 0 | 3,784 | 0 |

These are structural observed-only and separately named policy-sensitivity
populations, not scoring-date eligibility. Session 8B calendars remain
unresolved. All eleven horizons, target/date coverage, and provenance splits
are in `comparison/horizon_label_eligibility.csv`.

Raw three-year relative class balance changes from 13,502 positive / 22,720
negative / 21,968 null in legacy to 46,574 positive / 75,501 negative / 69,504
null in corrected, mostly because corrected retains quarterly rows. Corrected
fraud-label class balance is unavailable because the explicitly authorized
lineage included fraud-taxonomy scoring, not the separate fraud-label enrichment
stage; legacy has 538 confirmed and 20,194 suspect rows. Complete raw class
counts are in `comparison/class_balance.csv`.

## Disappearing companies and preservation

Step 6 flags 597 likely-disappearing tickers across 9,703 rows and policy-imputes
1,255 horizon returns at `-50%` with explicit policy provenance/availability.
All 597 tickers remain in the corrected universe; none is dropped. The complete
ticker list and year spans are in `comparison/disappearing_companies_remaining.csv`.

The pre-taxonomy clean dataset is preserved separately from the final enriched
dataset. No frozen input, legacy payload, model, prediction, threshold,
backtest, or production comparison was changed or generated.

## Artifacts and limitations

The artifact manifest records streaming hashes, baseline commit, complete dirty
state, exact commands (including one direct-script import failure before I/O),
configuration, Python/dependency versions, inputs, outputs, corrected-code
lineage, and offline-cache behavior. Large raw execution logs remain in `/tmp`.

This result is strictly `CORRECTED_PARTIAL`: it uses stale pre-fix Step 2
snapshots, is conditioned on the frozen incomplete daily cache (including absent
Korean benchmarks and missing/empty company series), and does not reproduce the
saved legacy run. It is not OOS evidence and supports no model, backtest,
threshold, or production-performance claim.
