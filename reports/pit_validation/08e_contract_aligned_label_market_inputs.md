# Session 8E — Contract-Aligned Label Market Inputs

## Verdict

Session 8E is complete. Every Session 8B-accepted horizon has nonzero certified
support in the 43,806-row Session 8D annual population. Session 9's minimum
support prerequisite is therefore met, but Session 9 was not started.

| Horizon | Supported | Unavailable | Excluded |
|---|---:|---:|---:|
| 6m | 24,127 | 13,962 | 5,717 |
| 1y | 24,127 | 13,962 | 5,717 |
| 2y | 21,492 | 16,597 | 5,717 |
| 3y | 19,025 | 19,064 | 5,717 |
| 5y | 14,514 | 23,575 | 5,717 |

Counts partition all 43,806 candidates for each horizon. They apply separately
and identically to `observed_only` and `include_policy_imputed`; no unsupported
disappearance or corporate-action outcome was inferred, so the sensitivity
population contains zero policy-only additions.

## Validated input chain

Before retrieval, the final Session 8D manifest (`899cffd7...`), its 17
referenced records, the 8C→8B→8 manifest chain, all 31 Session 8 records, all
8,021 latest SEC response records, and all 6,981 stored/decompressed SEC
payloads were rehashed with zero failures. Only
`corrected_step2/outputs/certified_snapshots.parquet` annual rows were used.

## Frozen market evidence

- Source endpoint: Yahoo Finance chart API, one daily request per mechanically
  mapped certified ticker plus `IWC`, `IWM`, `MDY`, and `SPY`, covering
  2009-07-01 through the exclusive 2026-07-17 boundary.
- Retrieval result: 4,814 successful raw payloads and 21 explicit HTTP 404s.
  All successful gzip and decompressed hashes pass. The 21 replayed 404 bodies
  are also stored and hashed. No proxy was substituted.
- Normalization: 4,796 supported symbols, 21 unavailable retrievals, and 18
  excluded payloads missing or misaligning raw/adjusted close. Frozen outputs
  contain 13,791,422 regular-session price rows and 84,274 provider event
  records.
- Calendars: pinned `exchange-calendars==4.5.6` XNYS and XNAS schedules, 4,287
  sessions from 2009-07-01 through 2026-07-16. Dates, opens, and closes are
  identical across the frozen interval. OTC and CBOE rows fail closed because
  no supported exchange-calendar mapping was frozen.
- Adjustment policy: provider `adjclose` is the sole total-return close.
  Unadjusted close, adjusted close, dividends, splits, capital gains, metadata,
  request parameters, retrieval timestamps, and raw payloads are preserved.
  Events are never added separately, preventing double counting.
- Vintage: evidence certifies the frozen retrieval timestamp and payload hash,
  not an unavailable historical vendor revision vintage. Yahoo is not an
  official exchange feed; this limitation is retained in the manifest.

The evidence set and manifest are under
`artifacts/pit_validation/contract_aligned_label_inputs/`; the final manifest
SHA-256 is `0ab15685a445f09919b19393e074b8b5903afef7e12defc7c44aeac624f4581a`.

## Decision alignment and fail-closed rules

Rows require certified SEC-primary availability by the June 30 New York cutoff,
a supported exchange/provider mapping, nonzero decision-available shares, and
the last regular-session raw close strictly before the July 2 decision. That
market cap freezes IWC/IWM/MDY/SPY assignment using the Session 8B thresholds.

Entry is the first common stock/benchmark adjusted close strictly after the
July 2 00:01 UTC prediction within five calendar days. Exit is the first common
close on or after entry plus 6/12/24/36/60 calendar months within ten days.
Stock and benchmark use identical session timestamps; `label_end_date` is the
common exit close availability. Later model consumers must still enforce
strict `label_end_date < decision_timestamp` for each training fold.

Per horizon, excluded rows comprise 5,470 unsupported exchange-calendar rows,
185 late filings, and 62 provider-exchange mismatches. Unavailable counts are
driven by 10,431 missing decision-available share observations, 751 missing
predecision closes, 58 missing stock mappings/payloads, 5 missing common entry
sessions, and horizon-varying missing common exits (2,717 at 6m/1y to 12,330 at
5y).

## Verification and stop boundary

The new builder's focused tests pass 5/5; the combined label-focused check
passes 17/17. Independent post-freeze validation rehashed every
manifest/inventory record and raw payload, confirmed both 219,030-row population
gate tables, zero duplicate entity-year-horizon keys, exact return arithmetic,
strict entry-before-exit ordering, and complete mutually exclusive counts.
The full suite passes 648 tests with 4 skips and 78 pre-existing pandas warnings.

No existing dataset or Session 8D artifact was overwritten. No model was
trained or tuned; no prediction, backtest, threshold optimization, commit, or
push occurred.
