# Session 8F — Corrected Feature Population

## Verdict

Session 8F is complete. The corrected feature, cleaning, and fraud-taxonomy
lineage required before Session 9 is frozen under
`artifacts/pit_validation/corrected_feature_population/`. No model, prediction,
backtest, threshold optimization, commit, push, or Session 9 work occurred.

Both population namespaces contain 43,806 rows and 4,831 stable entities. They
are physically separate even though Session 8E certified zero policy-only
labels. Each final parquet has 254 columns and preserves every deterministic
`stable_row_id`, `entity_id`, filing availability timestamp, and
`sec_primary_filing` provenance from source through Step 5, Step 6, and taxonomy.

## Frozen input validation

Before editing or materializing 8F outputs, the complete Session 8E chain was
revalidated:

- 4 linked manifests and 14,504 referenced file hashes passed;
- all 4,835 latest Session 8E market outcomes partitioned into 4,814 successful
  payloads and 21 explicit failures;
- all successful stored market payloads matched both compressed and
  decompressed hashes;
- all 8,021 latest Session 8D SEC outcomes partitioned into 6,981 successful
  responses and 1,040 failures; and
- every successful SEC payload matched its compressed and decompressed hashes.

The build then used only
`corrected_step2/outputs/certified_snapshots.parquet` and Session 8E-certified
mappings, regular-session prices, calendars, gates, and labels. It used zero
rows or features from `corrected_partial`, zero legacy preprocessing artifacts,
and no macro file.

## Population support

Counts below apply independently to each physical namespace.

| Feature family | Supported | Unavailable | Excluded |
|---|---:|---:|---:|
| Certified accounting | 43,806 | 0 | 0 |
| Required pre-decision price features | 26,232 | 11,857 | 5,717 |
| Macro vintages | 0 | 38,089 | 5,717 |
| Fraud taxonomy | 43,806 | 0 | 0 |
| 6m label | 24,127 | 13,962 | 5,717 |
| 1y label | 24,127 | 13,962 | 5,717 |
| 2y label | 21,492 | 16,597 | 5,717 |
| 3y label | 19,025 | 19,064 | 5,717 |
| 5y label | 14,514 | 23,575 | 5,717 |

Every row above belongs to exactly one status per family. Exclusion takes
precedence over unavailability. Rows outside the supported price family contain
no populated required price values.

## Proven transformations and fail-closed boundaries

The price feature clock is the Session 8B decision timestamp. Market cap uses
the last Session 8E raw close strictly before decision multiplied by certified,
decision-available shares; the result is checked against Session 8E's frozen
decision market cap. Momentum uses frozen adjusted closes with 365/183/91-day
lookbacks and a 21-day skip. Volatility uses only adjusted-close returns before
decision over 126/252/756/1,260-calendar-day windows. The 52-week-high ratio
uses only prior closes. If any required member is absent, the full price family
is null and the row carries an explicit reason.

Step 5 uses the explicit provenance-bearing feature market cap. Its macro
interactions now remain null when their macro source is missing; the artifact's
macro scaffold contains keys only and cannot introduce a value. Step 6 ran with
quarterly/size imputation and the future-panel survivorship heuristic disabled.
No likely-delisted flag or pessimistic outcome was inferred. Taxonomy ranking
uses `availability_timestamp` when present and rejects non-SEC-primary
provenance.

## Frozen artifacts and lineage

The artifact contains exact input copies, configuration, price-feature and
stage intermediates, execution logs, a completed checkpoint, physical final
outputs, a machine-readable support table, validation summary, complete dirty
state, code lineage, and a manifest. The manifest SHA-256 is:

`9c1e4b82c2e7bc2a85228adf6668b797acec827e2bd1ff7c58d20cfb6a9ac01a`

Independent post-freeze validation matched all 33 artifact records and seven
code-lineage records. Both final parquets preserve all 43,806 stable IDs, have
zero policy-imputed labels, and contain zero required price values outside the
supported price population.

## Verification and stop boundary

Focused Step 5, Step 6, taxonomy, and Session 8F tests passed 105 tests. Final
verification passed 652 tests with 4 skips and 78 pre-existing pandas
fragmentation/date warnings. Macro vintages remain a separate unavailable evidence area;
they must stay excluded from Session 9 unless a separately authorized build
proves release dates, vintages, mappings, and as-of transformations.
