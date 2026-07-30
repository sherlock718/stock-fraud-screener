# Start Here

## The goal

Build one simple, workable research-to-screening product:

1. preserve freely available ticker, financial, market, and macro source data;
2. produce a cleaned longitudinal point-in-time feature dataset;
3. perform leakage-safe feature analysis and selection;
4. train interpretable and sophisticated models;
5. backtest out-of-sample predictions with survivorship limitations made
   explicit;
6. apply small-cap and liquidity gates, construct portfolios, and report an
   explainable ranked shortlist.

Value, quality, momentum, growth, and fraud risk may all contribute. Fraud risk
is a factor or safety gate, not the product itself.

## Where the project stands

Most required layers already exist: source Parquets on Hugging Face, a
six-stage pipeline, a 367-column cleaned dataset, an HTML feature dictionary,
temporal feature selection, multiple model families, backtesting, liquidity
gates, portfolio construction, and extensive tests.

Substantial PIT and survivorship corrections also exist. Product Session P2
has now consolidated the corrected source-to-cleaned-dataset boundary into one
canonical US annual route. Product Session P3 has connected that contract to
one leakage-safe observed-only three-year research/model route. Product Session
P4 has now connected those row-complete OOS predictions to one fail-closed
small-cap/liquidity, portfolio, and explainable-shortlist route.

Therefore:

- there is no active historical-performance claim;
- the canonical primary dataset is US annual and observed-only;
- the canonical product output is a reproducible research shortlist, not
  personalized investment advice or a future-performance claim;
- certified macro vintages remain unavailable and are not synthesized;
- the V3.4 external market-ledger collection is paused;
- official performance remains unavailable until the accepted security/action
  ledger and immutable `DGS1MO` ALFRED vintage exist;
- nothing should be archived until its dependencies and replacement are known;
- the dataset remains historically enriched, not comprehensively
  survivorship-free.

## Canonical data entrypoint

Run:

```bash
python3 -m pipeline.build_corrected_feature_population
```

The command refuses to reuse a non-empty target and writes
`artifacts/canonical/corrected_us_annual/manifest.json`. The canonical primary
dataset is
`outputs/observed_only/features_taxonomy.parquet` relative to that root.

The frozen manifest SHA-256 is
`40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3`.
The primary Parquet has 43,806 rows, 339 columns, 43,806 unique stable row IDs,
and SHA-256
`46ed33affd8cc66df1f11dc8a41d6f804d106f0a2f8841509cbc931de013e56f`.
All rows have event-time materialization and non-empty PIT transform methods.
The physical policy-sensitivity output remains separate and adds zero
policy-only rows.

## Canonical research/model entrypoint

Run:

```bash
python3 -m modeling.build_canonical_research_model
```

The command refuses to reuse a non-empty target and writes
`artifacts/canonical/corrected_us_annual_3y_research_model/manifest.json`.
It fail-closes unless the pinned P2 manifest and the consumed primary features,
observed labels, row-horizon support, and summary records all match.

The frozen P3 manifest SHA-256 is
`8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2`.
Its OOS Parquet has 87,612 unique row-role records: exactly two roles for each
of 43,806 stable source rows. It contains 77,788 available OOS predictions and
9,824 explicit exclusions, with zero in-sample predictions. The prediction
Parquet SHA-256 is
`85164532421aec9909670f16fd56b4dcc9c5bcaaf7273c1ab98d1c464a7242a2`.

The route uses the observed three-year stock return for LightGBM regression
and observed three-year benchmark outperformance for the interpretable
decision tree. Target/support columns and uncertified macro fields are not
candidate features. Feature selection and median preprocessing are fitted
inside each historical fold only, and every fitted training label ends
strictly before its fold decision.

## Canonical product entrypoint

Run:

```bash
python3 -m portfolio.build_canonical_product
```

The command refuses a non-empty target and writes
`artifacts/canonical/corrected_us_annual_3y_product/manifest.json`. It consumes
only the pinned P3 prediction table, independently revalidates its two-role
row lineage and decision eligibility, and uses only frozen Session 8E raw
payloads for candidate-wide pre-prediction liquidity.

The frozen P4 manifest SHA-256 is
`28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7`.
The route retains all 43,806 source candidates, evaluates liquidity for all
1,218 rows passing the fixed non-liquidity/model gates, and records 1,013
liquidity passes. Twelve decision periods from 2015 through 2026 form exactly
15 equal-weight holdings each. The latest frozen 2026 shortlist has 15 rows
and SHA-256
`93bd6104a73e3752da019f7767cc7dfa72bfca837149179dd72b9b87bf466a3b`.
Its concise explainable report is `report/product_report.md` inside the
artifact, SHA-256
`dc3c719cee44f243c4b088c70a50c13187284832accaa867bef469cea044faba`.

The P4 artifact does not calculate performance. It freezes the accepted
three-year overlapping-vintage clock and flat 25-bps-per-side
actual-traded-notional cost policy, then fails official performance closed
because two inputs are absent: an evidence-backed security/action market
ledger and the immutable 2026-07-17 `DGS1MO` ALFRED vintage. Frozen Yahoo
payloads remain liquidity evidence only; old V3 performance and uncertified
rate or event fallbacks are prohibited.

## What to do next

Session C1 consolidated the accepted P2-P4 baseline. The local route remains:

```bash
python3 -m workflows.run_canonical
```

The command validates the pinned manifests in P2 -> P3 -> P4 order. C2
published the exact 202-file, 481,666,707-byte baseline privately at immutable
revision `aaf056ea115067e42ef9abf9fa93ade75cdd4052`. The authorized C2
checkpoint includes the recovery code and pointers, so a clean checkout can
recover it with:

```bash
python3 -m data_io.retrieve_canonical_from_hf
```

The pointers require this full revision and verify every downloaded byte size
and SHA-256 before exposing the target. Post-publication reconciliation
confirmed private visibility and all 202 paths present with zero missing. For
future separately approved versions, `scripts/publish_canonical` works from
any current directory and automates preparation through recovery verification.
See `docs/CANONICAL_ARTIFACT_PUBLICATION.md`.

The next implementation phase is D1, the US canonical raw-refresh replacement.
Do not collect external data, move archive files, create a branch, commit,
push, or publish a new version without separate authorization. Official
performance remains unavailable until the accepted market/security/action
ledger and immutable risk-free vintage exist.

## Basic test command

```bash
python3 -m pytest tests/ -x -q
```

The C2 focused publisher/retriever boundary has 18 passing tests. The C2 final
verification run produced 717 passed, 4 skipped, and 78 existing pandas
warnings.

Tests demonstrate code behavior; they do not establish data freshness,
survivorship-free coverage, or future performance.
