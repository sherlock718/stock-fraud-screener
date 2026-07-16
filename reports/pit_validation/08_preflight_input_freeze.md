# Session 8 Preflight — Corrected-Partial Input Freeze

Date: 2026-07-15

## Outcome

The local inputs required by the audited corrected-partial lineage are frozen
under `artifacts/pit_validation/corrected_partial_inputs/`. The three payloads
are read-only copy-on-write snapshots, and each copied hash matches its source.
`LEGACY_SAVED` was not modified. This preflight is not Session 8 completion and
does not materialize `CORRECTED_PARTIAL`. The preflight manifest SHA-256 is
`31c4f4e289ac49ef7c01f333370b4e3a35a3e786c0b289200bdc95e9c5c2f4d5`.

## Audited lineage and inputs

The required data stages are Step 3 for actual horizon label dates and
provenance, Step 5 for corrected event-time features, Step 6 for corrected
survivorship and policy-availability provenance, and fraud-taxonomy enrichment
for corrected as-of sub-scores. Their frozen inputs are:

| Input | Bytes | SHA-256 |
|---|---:|---|
| Stale pre-fix Step 2 snapshots | 33,312,586 | `f084924799f910fb228799a7c73f00baec465ed8cb42331fd261c7d9cb579644` |
| Daily price cache | 1,281,269,760 | `d0e7c3ee05d89751ad86c3a2a763bbc322672448634e345b3a1a982c647c3def` |
| Macro enrichment | 2,272,216 | `7eca9aa4998833e83c627a4162c9e9a42e8d6202fb5d2148d8397832066dfa65` |

The Session 7 clean dataset and monthly-price payloads also still match their
manifest hashes (`520a9b52...` and `9c7ad56e...`). The Session 7 manifest hash
remains `00b23794...`, and all 39 frozen legacy files remain untouched.

## Offline cache boundary

The snapshots contain 191,579 rows and 7,499 tickers. The daily cache has 7,465
records. Forty-seven absent company symbols affect 1,726 Korean snapshot rows;
none exists in the saved price-stage parquet. Thirty-three empty company cache
entries affect 9,816 rows, none with an observed forward return. The required
Korean benchmarks `^KS11` and `^KQ11` are also absent.

Session 8 must therefore use an offline-only cache reader. Missing or empty
series remain unavailable and produce null observed labels; it must not fetch,
substitute, or impute market data. This preserves the observed evidence rather
than materializing an unresolved policy.

## Verification and limitations

- Source and snapshot streaming SHA-256 hashes match for all three inputs.
- Frozen payload permissions are read-only (`0444`).
- Available storage was 124 GiB before the freeze; logical payload size is
  approximately 1.23 GiB and copy-on-write avoids an immediate full duplicate.
- No pipeline, model, prediction, backtest, threshold, or production command ran.
- `CORRECTED_PARTIAL` will still use stale pre-fix Step 2 snapshots and will not
  reproduce the saved legacy run.

The exact next task remains Session 8: validate this input manifest, execute the
four-stage lineage into `artifacts/pit_validation/corrected_partial/`, and perform
the required data-level comparison. Session 8B remains out of scope.
