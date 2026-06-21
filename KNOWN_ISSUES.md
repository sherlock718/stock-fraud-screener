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

### MACRO-USREC-VINTAGE-001: USREC recession flag uses revised data, not real-time vintage

- **Type:** look-ahead / PIT risk
- **Found:** Session 3 (step 4 audit, corrected in docs pass)
- **Details:** `pipeline/step4_enrich_macro.py` fetches FRED series `USREC` via `fred.get_series('USREC')` with no `realtime_start`/`realtime_end` parameters. This returns the **current revised** recession indicator, which reflects NBER committee decisions made months after recessions actually begin/end. For example, the 2020 recession was declared by NBER in June 2020 (start: Feb 2020) and the end was declared in July 2021 (end: April 2020). A filing in March 2020 would see `recession=1` in our data even though NBER hadn't announced it yet.
- **Risk:** Medium. The `recession` column and `macro_regime` (which uses `recession` as an override to regime=3) may contain information that was not publicly known at filing time. This affects: (a) the `recession` feature itself, (b) `macro_regime` derived feature, (c) step5 interaction features like `value_in_recession`, `quality_in_recession`, `momentum_in_expansion`.
- **Mitigation options (do not implement now):**
  1. Use FRED vintage API (`realtime_start`/`realtime_end`) to get point-in-time USREC values
  2. Lag the recession flag by 6+ months (NBER typical announcement delay)
  3. Replace USREC with a real-time recession proxy (e.g., 2 consecutive quarters of negative GDP growth, or yield curve inversion with lag)
  4. Exclude `recession` and `macro_regime` from model features (keep for analysis only)
- **Action:** Decide mitigation approach in a later session. For now, document as known PIT risk. Impact is bounded: recession periods are rare (~10% of dataset rows), and the flag is one of 350+ features.

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

## Medium

### MACRO-NO-ASOF-DATE-001: No macro_asof_date column in step4 output

- **Type:** auditability gap
- **Found:** Session 3 (step 4 audit)
- **Details:** `pipeline/step4_enrich_macro.py` uses `pd.merge_asof(direction='backward')` which correctly prevents future data leakage. However, it does not record WHICH date's macro observation was matched to each filing row. Without a `macro_asof_date` column, post-hoc PIT validation is impossible — you must trust the merge logic rather than verify it from the data alone.
- **Risk:** Medium. The merge logic is correct, but if the code were refactored or a bug introduced, there's no data-level audit trail to detect it.
- **Action:** Add `macro_asof_date` column recording the panel date that was matched to each filing. Small enhancement (capture the matched index during merge), not urgent.

### DOCS-PARQUET-ATLAS-001: PARQUET_ATLAS lists non-existent macro columns

- **Type:** documentation inaccuracy
- **Found:** Session 3 (step 4 audit)
- **Details:** PARQUET_ATLAS.md lists `gdp_growth, unemployment, spread_10y2y, macro_asof_date` as key columns of `data/macro.parquet`. None of these exist in the actual code. The real macro columns are: `treasury_10y, treasury_2y, yield_curve, fed_funds_rate, credit_spread_baa, hy_spread, cpi_yoy, recession, vix, real_rate_10y, credit_tightening, macro_regime`.
- **Risk:** Low. Documentation only — no runtime impact.
- **Action:** Fix in PARQUET_ATLAS.md (done this session).

### FX-MIXED-PORTFOLIO-001: Multi-market backtest mixes local-currency returns

- **Type:** backtest presentation issue
- **Found:** Session 3 (FX audit)
- **Details:** `scripts/backtester.py` and `scripts/build_portfolio.py` use `forward_return_1y` (local currency) for all markets. When run without `--market` filter, a global portfolio averages JPY returns, BRL returns, and USD returns equally. The reported CAGR is not a real USD return — it's an unweighted mix of local-currency returns.
- **Risk:** Medium. Backtest CAGR is misleading for multi-market runs. Does NOT affect model training (trains on `beat_local_market` which is benchmark-relative and currency-neutral). Single-market backtests are less affected because all holdings share one local currency.
- **Mitigation:** `--market US` flag exists for single-market runs. USD columns exist only when `bias_audit.py --fix` is run manually.
- **Before trusting global portfolio CAGR, a design decision is needed:**
  1. Add USD-adjusted return columns to the normal pipeline output (step3 or post-step6), or
  2. Add a `--usd` / `--currency` option to backtester that converts at backtest time, or
  3. Clearly restrict global CAGR reporting to local-relative analysis only (document limitation)
- **Severity upgrade:** If global backtest CAGR is used for real investment decisions, upgrade from Medium to High. Currently it is used only for research/comparison.
- **Action:** Design decision deferred. Do not change backtester code now.

### RANK-LEAKAGE-001: quality_composite and value_composite rank across full dataset without fiscal_year groupby — FIXED

- **Type:** cross-temporal rank leakage
- **Found:** Session 4 (step 5 audit)
- **Fixed:** Session 4 — added `.groupby(['fiscal_year', 'market'])` before `.rank(pct=True)` for both composites in `add_composite_scores()` (lines 531–551)
- **Details:** `pipeline/step5_compute_features.py` previously computed `quality_composite` and `value_composite` using global percentile ranks across all years. A company in 2010 was ranked against all companies from 2005–2024. Now ranks within `(fiscal_year, market)` cohorts, matching the pattern used by `add_momentum_ranks`.
- **Affected columns:** `quality_composite`, `value_composite`, and all downstream interactions.
- **Remaining action:** Re-run step 5 (`python3 pipeline/step5_compute_features.py`) to regenerate `historical_dataset.parquet` with corrected ranks. Until then, existing parquet data still contains globally-ranked composites.
- **Status:** Code fixed. Data regeneration pending.

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

### P0F-PRICE-FLOOR-001: Price floor applied to all exchanges, not just OTC

- **Type:** doc/code mismatch
- **Found:** Session 6 (p0f audit)
- **Details:** `p0f_universe_definition.py` docstring says "No OTC penny stocks: exclude if exchange == 'OTC' AND entry_price < market floor" but the code (line 144) excludes ALL rows below the market floor regardless of exchange. The `OTC_EXCHANGES` constant is defined but never used in the filter logic.
- **Risk:** Near-zero. NYSE/NASDAQ-listed stocks below $1 are exceptionally rare (they would be delisted). The effective behavior is correct for the purpose of filtering penny stocks.
- **Action:** Either fix the docstring to match code (simpler) or add the exchange check to match docstring. Low priority — no data impact.

### PIOTROSKI-FIRST-YEAR-001: Piotroski extension signals default to 0 (not NaN) for first observation per ticker

- **Type:** design observation
- **Found:** Session 6 (feature_library audit)
- **Details:** `add_piotroski_ext()` uses `shift(1)` to compare YoY. The first year per ticker has NaN shift → boolean comparison with NaN yields False → `astype(float)` produces 0.0. This means every ticker's first appearance gets 0 for all three extension signals, implying "no improvement" rather than "unknown."
- **Risk:** Very low. The 0 default is conservative (penalizes first observation). The `piotroski_f_score_9` for first years equals `piotroski_f_score + 0` which is semantically reasonable. Downstream training sees this consistently across all tickers.
- **Action:** None needed. Document behavior in tests. If NaN is desired for first-year rows, a one-line fix (`where(shift notna)`) would suffice.

---

## Parking Lot

Non-critical ideas and deferred items. Review during triage sessions.

### FEATURE-LIB-CONSOLIDATE-001: step5 and feature_library both contain feature logic without shared ownership

- **Type:** feature ownership / maintainability risk
- **Severity:** Medium
- **Found:** Session 6 (feature_library audit)
- **Details:** `step5_compute_features.py` and `feature_library.py` both contain feature-generation logic. `feature_library.py` is called defensively by downstream scripts (`train_models.py`, `backtester.py`, `generate_oof_scores.py`, `leverage_strategy.py`, `build_monthly_price_cache.py`) after loading parquet, while Step 5 does not import from it. This creates risk of silent formula drift or hidden post-Step-5 feature patches.
- **Current decision:** Do not fix now. Current behavior is tested and working. Both files have test coverage (step5: 32 tests, feature_library: 19 tests).
- **Future preferred design:** Make `feature_library.py` the canonical home for reusable feature helper functions. Step 5 should call those functions during dataset build, and downstream scripts may call the same functions defensively. Avoid importing from `step5_compute_features.py` because it is a large CLI-style pipeline step (972 lines with argparse/main).
- **Action:** Refactor in a future session. Small change: ~10 lines in step5 to import and call instead of inline compute.

### FEATURE-CONTRACT-001: No feature set/count validation between Step 5 and Step 6

- **Type:** dataset contract / future guardrail
- **Severity:** Low (Medium if downstream training/backtesting relies on an expected stable feature set)
- **Found:** Session 5 (step 6 audit)
- **Details:** Step 6 intentionally does not validate that the expected feature set/count exists. If Step 5 silently stops producing major feature groups (e.g., all growth YoY features vanish due to a code error), Step 6 will pass the dataset forward without warning. This is not a Step 6 bug — Step 6 is designed as a thin structural cleaner.
- **Future fix:** A lightweight dataset contract test, preferably near Step 5 output or as a separate pipeline contract test, checking required feature groups / critical columns rather than hardcoding an exact 355-column count. Could assert: "growth_yoy group has ≥10 cols", "momentum group has ≥5 cols", etc.
- **Action:** Implement when pipeline stability is prioritized. Not urgent while Step 5 is actively developed and tested.
