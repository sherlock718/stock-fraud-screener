# Known Issues

Issues found during pipeline audit. Classified by type and severity.

## Critical

_(none yet)_

## High

### BROKEN-IMPORT-001: Dead imports to `pipeline.score_and_report`

- **Type:** code bug / legacy-file issue
- **Found:** Session 1 (pipeline atlas)
- **Details:** Three legacy modules (`enrich_insider_signals.py`, `enrich_market_cap.py`, `enrich_market_signals.py`) import `from pipeline.score_and_report import generate_report, print_report`. This module does not exist. The imports are inside `if __name__ == '__main__'` blocks and in `_regenerate_report()` helper functions, so they only fail when those files are run standalone.
- **Risk:** Low immediate risk (files are LEGACY_ARCHIVE_CANDIDATE, not on main data path). But confusing for anyone trying to use them.
- **Action:** Investigate in a later session. Either delete the broken imports or archive the legacy files entirely.

### LIQUIDITY-001: No liquidity/volume feature in main dataset

- **Type:** data issue / known limitation
- **Found:** Session 0 (pre-audit exploration)
- **Details:** `adtv_30d` exists only in backtester path (`build_monthly_price_cache.py` → `monthly_prices.parquet`). Main dataset has no trading volume or ADTV. Universe definition has no liquidity filter. Low-cap stocks protected only by inconsistent market cap floors ($10M/$50M/$150M across files).
- **Risk:** Backtests may show unrealistic returns on illiquid stocks.
- **Action:** Audit in Session 3 or 5. Decision in Session 5.5.

## Medium

### MUTATION-ORDER-001: Uncontrolled in-place mutation of final parquet

- **Type:** architecture / data-safety risk
- **Found:** Session 1 (parquet atlas)
- **Details:** `historical_dataset_clean.parquet` is mutated in-place by 16+ scripts post-step6. No enforced execution order, no idempotency checks, no versioning. Running scripts in wrong order or re-running one can corrupt downstream columns.
- **Risk:** Silent data corruption if mutation order changes or a script is re-run. No rollback mechanism beyond re-running the full pipeline.
- **Action:** Verify observed mutation order. Decide safer output strategy (append-only columns, checksum validation, or separate output files) in a later session.

### TEST-COVERAGE-001: No direct pipeline test coverage for Steps 1–7

- **Type:** test gap
- **Found:** Session 1 (test matrix)
- **Details:** Only `tests/test_pipeline.py` exists. It covers `train_models.py` logic (temporal split, feature exclusion, IC table) and `bias_audit.py` (period end calc). Zero test coverage for: step1 (tickers), step2 (snapshots), step3 (prices), step4 (macro), step5 (features), step6 (clean), p0f (universe). All are P0–P2 priority per the test matrix.
- **Risk:** Temporal leakage, schema drift, or formula errors in core pipeline steps would go undetected.
- **Action:** Sessions 2–5 create minimal critical step tests. Session 2 covers Steps 1–3.

## Low

_(none yet)_

---

## Parking Lot

Non-critical ideas and deferred items. Review during triage sessions.

_(empty)_
