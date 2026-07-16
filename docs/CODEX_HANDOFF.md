# Codex Handoff

## User goal — do not narrow or expand it

Build one simple, workable research-to-screening product in this order:

1. Collect freely available ticker-level source data and preserve the source or
   normalized bronze inputs.
2. Build a cleaned longitudinal point-in-time dataset with multiple rows per
   security, reflecting when information became available. Include useful
   fundamental, momentum, growth, quality, macro, liquidity, and fraud-risk
   features plus future-price targets.
3. Define leakage-safe train/validation/test or walk-forward splits before
   feature analysis. Feature selection and preprocessing must be fold-local.
4. Analyze and rank features, then train both interpretable/tree-based and more
   sophisticated models.
5. Backtest only out-of-sample predictions with point-in-time membership,
   delisting/event handling, transaction costs, and honest limitations.
6. Apply a small-cap opportunity gate and a minimum-liquidity gate, improve
   fraud-risk filters later, construct portfolios, and produce an explainable
   report.

Fraud risk is one of five factors (Value, Quality, Momentum, Growth, Fraud
Risk), not the product identity. The immediate need is consolidation of the
existing implementation, not another methodology program.

## Corrected repository assessment

The repository substantially implements the intended layers. The problem is
not absence of work; it is that the usable path is split between the main
pipeline, older production artifacts, and later PIT-correction/validation
artifacts.

Confirmed existing capabilities:

- A six-stage dataset pipeline, feature library, and enrichment modules.
- A manifest-backed 367-column cleaned historical dataset and its source
  Parquets.
- Temporal/walk-forward splitting, fold-local feature-selection machinery, and
  preprocessing controls.
- LightGBM, regression, and interpretable decision-tree model paths.
- Five-factor alpha scoring, liquidity gates, backtesting, portfolio
  construction, and reporting code.
- Hundreds of tests; the most recent documented full run was 674 passed and 4
  skipped.
- Extensive PIT, target-availability, transformation, survivorship, monthly
  NAV, prediction-consumption, and market-input validation work.

Do not describe the project as having achieved nothing. It produced substantial
code, data, tests, and evidence. The failure was repeated widening and
certification work without consolidating those results into one obvious
canonical dataset-to-product route.

## Important corrections from the 2026-07-17 documentation review

### Source-data preservation

Earlier language calling raw-data preservation simply "incomplete" was too
broad. In the practical pipeline sense, source/bronze data is preserved:

- `data/snapshots.parquet` — normalized raw financial observations
- `data/prices.parquet` — price history
- `data/historical_dataset_clean.parquet` — cleaned feature dataset
- `data/ARTIFACT_MANIFEST.json` — hashes, provenance, generation metadata
- per-market snapshot files handled by the Hugging Face transfer utilities

`data_io/push_to_hf.py` and `data_io/pull_from_hf.py` explicitly upload,
download, and verify these artifacts. The configured dataset repository is
`ekrash718/stock-screener-data`.

Local evidence checked on 2026-07-17:

- `snapshots.parquet`: 33,312,586 bytes
- `prices.parquet`: 39,911,793 bytes
- `historical_dataset_clean.parquet`: 88,494,877 bytes
- `ARTIFACT_MANIFEST.json`: present, with `artifacts`, `generated_at`,
  `generated_by`, and `source_commit` metadata
- corrected SEC Company Facts evidence: 6,981 compressed payload files,
  approximately 761 MB
- contract-aligned raw evidence: 4,837 files, approximately 306 MB
- the broader `artifacts/pit_validation/` tree was previously measured at
  approximately 4 GiB

Exact HTTP-response-byte preservation is a stricter forensic standard. It is
present for important corrected/validation subsets, but has not been proven for
every historical API request. Do not confuse that narrower caveat with loss of
the practical Parquet source layer stored on Hugging Face.

### Feature dictionary

`docs/FEATURE_DICTIONARY.html` is a substantial, intentional explanatory
dictionary for the 367-column dataset. It documents the pipeline, feature
families, raw financial fields, derived ratios, momentum/macro inputs, targets,
model outputs, and fraud features. It is curated and groups some feature
families rather than providing a separate machine card for every column. Do not
call it incomplete without making that distinction.

### Survivorship bias

The project did extensive survivorship work, including:

- `pipeline/step0_historical_universe.py` and SEC full-index research
- historical-universe coverage reporting
- stale/delisted-company handling and sensitivity policies
- observed-only versus policy-imputed outcome separation
- corrected SEC provenance rebuilding
- contract-aligned labels and corrected feature populations
- fail-closed production/validation populations

The honest remaining limitation, stated in `docs/PIT_AUDIT_FINAL_REPORT.md`, is
that the dataset is historically enriched but not comprehensively
survivorship-free. Free SEC and market sources do not provide CRSP-quality
historical exchange membership, security-type/ticker history, delisting terms,
and returns. This is a source-coverage boundary after substantial mitigation,
not evidence that survivorship was ignored.

## Why the project became confusing

- The main data/model path and later corrected PIT artifacts were not folded
  back into one canonical product route.
- Validation sessions accumulated parallel manifests, reports, and artifact
  namespaces.
- Older documents sometimes describe an issue as resolved while later audits
  qualify the result.
- Historical-performance certification displaced the simpler dataset-first
  product objective.
- The prior active product framing narrowed the goal too far to merely
  producing a current shortlist.

## Frozen decisions for the next task

- Do **not** execute the archived V3.4 329-request market-ledger collection.
- Do not require Sharadar access or new external credentials.
- Do not collect or refresh data, retrain models, calculate performance, or
  begin another audit/recovery chain.
- Do not discard PIT artifacts or assume the legacy cleaned dataset is the
  canonical answer.
- Do not archive code or documentation until a dependency/equivalence map shows
  that it is superseded.
- Preserve historical reports as evidence, but do not present their performance
  numbers as current product claims.
- Prefer one canonical route over new abstractions, strategies, or datasets.

## Exact next task

Execute Product Session P1 from `docs/CODEX_ROADMAP.md`: one bounded,
read-only canonicalization decision.

P1 must determine:

1. Which existing files form the canonical bronze/source layer.
2. Which main-pipeline transformations remain valid and which later PIT
   corrections supersede them.
3. Which existing artifact is the best candidate for one canonical cleaned
   longitudinal dataset, and what prevents adopting it immediately.
4. Which split, feature-selection, model, backtest, liquidity, portfolio, and
   reporting implementations should form the single supported route.
5. Which paths are active, historical evidence, duplicates, or safe archive
   candidates.
6. Exactly one implementation task for P2.

P1 produces a concise canonical-path/dependency map and decision report. It
must not edit product code, move/archive files, call external services, rebuild
data, retrain, backtest, commit, or push.

## Next-session prompt

> Execute Product Session P1 from `docs/CODEX_ROADMAP.md` as one bounded,
> read-only canonicalization decision. Start with `docs/START_HERE.md` and
> `docs/CODEX_HANDOFF.md`; trust the documented 2026-07-17 repository review
> unless direct evidence contradicts it. Map the existing bronze/source data,
> main six-stage pipeline, later PIT-correction artifacts, cleaned-dataset
> candidates, temporal feature-selection/split code, model paths, backtest,
> liquidity gates, portfolio construction, reporting, and tests into one
> proposed canonical route. Distinguish confirmed facts, conflicts, unknowns,
> historical evidence, duplicates, and safe future archive candidates. Do not
> collect or refresh data, call external services, edit or archive files,
> retrain models, calculate performance, resume V3.4, commit, or push. Deliver a
> concise dependency map, the recommended canonical dataset-to-product route,
> blockers to adopting it, and exactly one bounded implementation task for P2.

## High-value references

Read these only as needed after the handoff; do not reread the whole docs tree:

- `docs/FEATURE_DICTIONARY.html`
- `docs/developer/architecture-v2-blueprint.md`
- `docs/architecture/orientation_pipeline_spine.md`
- `docs/architecture/orientation_modeling_alpha.md`
- `docs/architecture/orientation_research_backtest.md`
- `docs/architecture/orientation_quality_orchestration.md`
- `docs/UNIVERSE_COVERAGE_REPORT.md`
- `docs/PIT_AUDIT_FINAL_REPORT.md`
- `docs/PIT_ARTIFACT_ARCHIVE.md`
- `reports/pit_validation/08d_step2_provenance_rebuild.md`
- `reports/pit_validation/08e_contract_aligned_label_market_inputs.md`
- `reports/pit_validation/08f_corrected_feature_population.md`
- `reports/pit_validation/v3_1_production_table_contract.md`
- `reports/pit_validation/v3_2_oos_predictions.md`
- `reports/pit_validation/v3_3_liquidity_holdings.md`
- `docs/archive/VALIDATION_V3_HANDOFF.md`
- `docs/archive/VALIDATION_V3_ROADMAP.md`

## Current repository state

This handoff update changes documentation only. No product code, dataset,
model, PIT artifact, report, or external state was changed. The archived V3
program remains preserved and paused.
