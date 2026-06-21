# Known Issues

Issues found during pipeline audit. Classified by type and severity.

## Critical

### PRICE-UNADJUSTED-001: step3 uses unadjusted Close instead of Adj Close — FIXED

- **Type:** data bug
- **Found:** Session 2 (step 3 audit)
- **Fixed:** Session 2.5 — changed `hist['Close']` → `hist['Adj Close']` in `fetch_price_series()` (line 215)
- **Details:** `pipeline/step3_enrich_prices.py` line 212 uses `auto_adjust=False` and previously read `hist['Close']`. With yfinance 1.2.0, this returned the **unadjusted** close price. Now reads `hist['Adj Close']` (split/dividend-adjusted).
- **Remaining action:** Delete `data/price_cache.db` and re-run step 3 (`python3 pipeline/step3_enrich_prices.py`) to regenerate `prices.parquet` with corrected adjusted prices. Until then, cached prices and existing `prices.parquet` still contain unadjusted data.
- **Status:** Code fixed. Data regeneration pending.

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

### STEP1-TICKER-DEDUP-001: No dedup on ticker column

- **Type:** data quality gap
- **Found:** Session 2 (step 1 audit)
- **Details:** `step1_fetch_tickers.py` deduplicates on `cik` only. Two different CIKs could theoretically share the same ticker (after corporate actions like spin-offs or ticker reuse). This is rare but unguarded.
- **Risk:** Very low — SEC EDGAR assigns unique CIKs and ticker reuse across live companies is uncommon. No immediate fix needed.
- **Action:** Monitor. If duplicate tickers cause issues downstream, add a secondary dedup.

### STEP2-NO-PERIOD-END-001: No explicit period_end column

- **Type:** missing defensive check
- **Found:** Session 2 (step 2 audit)
- **Details:** `step2_build_snapshots.py` produces `fiscal_year` and `filed_date` but does NOT compute an explicit `period_end` column. The audit checklist specifies validating `filed_date >= period_end`, but there's no `period_end` to compare against. EDGAR data naturally has filing dates after period-end, so this is not a bug — just a missing assertion.
- **Risk:** Low. If a corrupt XBRL entry had `filed` before the fiscal year end, it would pass undetected.
- **Action:** Consider adding `period_end = f"{fiscal_year}-12-31"` column and assertion in a future session.

### STEP3-NO-DELISTED-IMPUTE-001: Missing -50% delisted imputation in step 3

- **Type:** known limitation
- **Found:** Session 2 (step 3 audit)
- **Details:** When no exit price is found (stock delisted), `forward_return()` returns `None`. The -50% survivorship imputation happens later in `scripts/mark_survivorship.py`. This is by design (separation of concerns) but means step 3 output alone underestimates negative returns.
- **Risk:** Low — downstream script handles it. But if step 3 output is used directly without `mark_survivorship.py`, survivorship bias is present.
- **Action:** None needed. Document in PARQUET_ATLAS that `prices.parquet` requires `mark_survivorship.py` post-processing.

---

## Parking Lot

Non-critical ideas and deferred items. Review during triage sessions.

_(empty)_
