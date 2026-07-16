# Session 8D — Provenance-Preserving US Step 2 Rebuild

## Verdict

Session 8D is complete. A clean US Step 2 evidence set was rebuilt from the
official SEC Company Facts API for the frozen 8,021-CIK universe. The certified
output contains stable entity identity and proven primary-filing availability.
This repairs the Step 2 provenance boundary only: it does not establish
training-label market-input support and does not unblock Session 9.

## Validated prerequisites and frozen scope

- The Session 8C validation manifest SHA-256 matched `06af0d47...`.
- Its Session 8B calendar contract reference matched `13cd7494...`.
- Its Session 8 corrected-partial manifest reference matched `10b648c5...`, and
  all 31 referenced payload sizes and streaming hashes matched before use.
- `data/tickers.parquet` contained exactly 8,021 unique, non-null, 10-digit CIKs,
  all in market `US`. Its frozen copy matches source SHA-256 `c090752c...`.
- The only external endpoint used was
  `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`.
- No existing dataset, checkpoint, model, price cache, label, prediction, or
  backtest artifact was overwritten.

All run inputs, raw payloads, response records, checkpoints, outputs,
diagnostics, validation summaries, lineage, and manifests are under
`artifacts/pit_validation/corrected_step2/`.

## Provenance and exclusion policy

Supported primary forms are `10-K`, `10-Q`, `20-F`, `10-KSB`, and `10-QSB`.
Later amendments and unsupported forms are never used in certified values.
Within one earliest proven accession, comparative Company Facts contexts are
resolved by current period end, then full-year duration for `FY` or shortest
quarter duration for `Q1`–`Q3`. Distinct accessions at the same earliest filing
date fail closed; accession precedence is never inferred.

SEC filing dates are date-only. In accordance with Session 8B, certified
`availability_timestamp` treats the date as end-of-day in America/New_York and
then converts it to UTC. Every certified row has non-null `entity_id`, fiscal
year, period type, market, `source_filing_date`, `availability_timestamp`, and
`availability_provenance = sec_primary_filing`. Unsupported optional facts are
omitted and named; missing required primary revenue/assets or ambiguous source
records are excluded from the certified population.

## Final counts

Issuer-level results form an exact, mutually exclusive partition of the frozen
universe:

| Issuer classification | Count |
|---|---:|
| Proven (at least one certified period) | 4,937 |
| Excluded-only (period candidates, none certifiable) | 1,086 |
| Unavailable (no usable payload or no supported period candidate) | 1,998 |
| **Frozen universe** | **8,021** |

Period-level results are a separate population:

| Period classification | Annual | Quarterly | Total |
|---|---:|---:|---:|
| Proven | 43,806 | 119,410 | **163,216** |
| Excluded | 13,052 | 40,308 | **53,360** |

Excluded-period reasons are 53,313 missing primary revenue/assets, 40 ambiguous
earliest-primary plus missing primary revenue/assets, and 7 ambiguous
earliest-primary only. Unavailable-entity reasons are 1,040 official SEC 404
responses and 958 successful payloads with no supported period candidates.
After one bounded retry, no transport or other HTTP failure remains in the final
latest-response population.

## Raw-response evidence

- Latest response coverage is exactly 8,021 CIKs: 6,981 successful payloads and
  1,040 explicit 404 failures.
- The append-only response log contains 17 earlier retry-history records.
- Every successful response records request URL, CIK, retrieval timestamp, HTTP
  status, response byte count and SHA-256, stored gzip byte count and SHA-256,
  and attempts.
- Independent validation rehashed every stored gzip and streamed every
  decompressed response; failures: **0**.
- One response interrupted during a controlled pause was never manifested as a
  successful source. It is isolated, preserved, and hash-verified in the
  orphan/partial-response evidence.

## Validation and diagnostic comparison

The certified parquet has 163,216 rows and 83 columns. Required provenance is
non-null, all rows are US/SEC-primary, entity IDs equal `US:{CIK}`, availability
timestamps parse as UTC-aware values, and duplicate certified entity-period
keys are zero.

The stale US Step 2 snapshot is diagnostic only: 160,972 entity-period keys are
common, 2,244 are corrected-only, and 3,463 are stale-only. The stale snapshot
has 164,435 rows/4,930 CIKs versus 163,216 proven rows/4,937 CIKs now. These row
counts are not correctness evidence.

Focused synthetic verification passed 34 tests covering artifact scope, stable
identity, primary/amendment ordering, equal-time ambiguity, date-only
availability, raw hashing, resume behavior, transient retry, partial-response
isolation, and failure recording. The final evidence manifest is stored beside
the frozen evidence set.

## Stop boundary

Session 8D ran Step 2 only. It did not run Step 3+, create or alter labels,
source market prices or benchmarks, train or score models, generate
predictions, run backtests, optimize thresholds, or begin Session 8E/9. Session
8E remains a separate task, and Session 9 remains blocked.
