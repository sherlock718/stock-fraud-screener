# Known Issues

Issues found during pipeline audit (Sessions 0–6). Triaged in Session 6.5.

---

## Fix Priority Queue

Issues that should be resolved before next model train / backtest refresh:

| Priority | ID | Effort | What's Needed |
|---|---|---|---|
| **P1** | ~~P0F-PRICE-FLOOR-001~~ | ~~Tiny~~ | ✅ Fixed Session 15 |
| **P2** | DATA-ARTIFACT-001 | Small | Add snapshots.parquet to HF push + create pull script |
| **P2** | KR-DART-SCALING-001 | Medium | Design decision on KR fetch strategy |
| **P2** | MACRO-NO-ASOF-DATE-001 | Small | Add audit column to step4 |
| **P2** | FEATURE-LIB-CONSOLIDATE-001 | Small | Refactor step5 imports |
| **P3** | LIQUIDITY-001 | Large | Design decision + feature engineering |
| **P3** | MUTATION-ORDER-001 | Medium | Architecture redesign |
| **P3** | FX-MIXED-PORTFOLIO-001 | Medium | Design decision on currency handling |
| **P3** | MACRO-USREC-VINTAGE-001 | Medium | FRED vintage API or proxy approach |

---

## Fixed (Code Done, Data Regen Pending)

*(Empty — all code fixes validated against locally regenerated multi-market dataset.)*

---

## Fixed (Complete)

### PRICE-UNADJUSTED-001: step3 uses unadjusted Close instead of Adj Close — FIXED

- **Type:** data bug
- **Severity:** Critical
- **Found:** Session 2 | **Code fixed:** Session 2.5 | **Local data regenerated:** Session 12
- **Fix:** Changed `hist['Close']` → `hist['Adj Close']` in `fetch_price_series()`.
- **Validation:** Full multi-market pipeline rebuild. Fresh yfinance prices with no stale cache. `forward_return_1y` verified: no infinities, min=-1.0, 155K labeled rows.
- **Status:** ✅ Code fixed and local regenerated dataset validated. HuggingFace production artifact not yet updated. KR full coverage is partial and tracked separately under KR-DART-SCALING-001.

### RANK-LEAKAGE-001: quality/value composites ranked across full dataset — FIXED

- **Type:** cross-temporal rank leakage
- **Severity:** Medium
- **Found:** Session 4 | **Code fixed:** Session 4.5 | **Local data regenerated:** Session 12
- **Fix:** Added `.groupby(['fiscal_year', 'market'])` before `.rank(pct=True)` in `add_composite_scores()`.
- **Validation:** US 2015 mean=0.545, US 2020 mean=0.534, CA mean=0.484, JP mean=0.507, DE mean=0.507 (all ~0.5, confirming independent within-group ranking).
- **Status:** ✅ Code fixed and local regenerated dataset validated. HuggingFace production artifact not yet updated. KR full coverage is partial and tracked separately under KR-DART-SCALING-001.

### TAXONOMY-SUSPECT-OVERWRITE-001: enrich_fraud_taxonomy.py overwrites fraud_suspect — FIXED

- **Type:** data/semantic issue
- **Severity:** Medium
- **Found:** Session 10 | **Code fixed:** Session 11 | **Local data regenerated:** Session 12
- **Fix:** Removed `build_fraud_suspect()` from taxonomy. `fraud_suspect` exclusively owned by `enrich_fraud_labels.py`.
- **Validation:** After running labels then taxonomy: `fraud_suspect` binary {0,1}, 47,966 suspects flagged, 643 confirmed frauds all have `fraud_suspect=0`. Taxonomy did not overwrite.
- **Status:** ✅ Code fixed and local regenerated dataset validated. HuggingFace production artifact not yet updated. KR full coverage is partial and tracked separately under KR-DART-SCALING-001.

### DOCS-PARQUET-ATLAS-001: PARQUET_ATLAS listed non-existent macro columns — FIXED

- **Type:** documentation inaccuracy
- **Found:** Session 3 | **Fixed:** Session 3
- **Status:** ✅ Done. No remaining action.

### P0F-PRICE-FLOOR-001: Price floor applied to all exchanges, not just OTC — FIXED

- **Type:** doc/code mismatch
- **Found:** Session 6 | **Fixed:** Session 15
- **Fix:** Updated docstring and module-level comments to state price floor applies to all exchanges (matching actual code behavior). Removed unused `OTC_EXCHANGES` constant. Updated PIPELINE_ATLAS checklist 7.6.
- **Status:** ✅ Fixed complete. No data impact (code behavior was already correct).

---

## Open — Medium Severity

### DATA-ARTIFACT-001: Intermediate parquet files not persisted externally

- **Type:** infrastructure / developer experience
- **Severity:** Medium
- **Effort:** Small
- **Found:** Session 12 | **Tooling added:** Session 13
- **Details:** `data/snapshots.parquet`, `data/prices.parquet`, `data/macro.parquet` are gitignored ephemeral build artifacts. They are not stored on HuggingFace or any external storage. Only `historical_dataset_clean.parquet` is pushed to HF. This means any fresh checkout requires a full Step 1–2 rebuild (1–4 hours, network-dependent) before Step 3–6 can run.
- **Risk if ignored:** Developer friction. Any contributor must run multi-hour EDGAR fetch before they can iterate on features or scoring.
- **Session 13 progress:** `scripts/pull_from_hf.py`, `scripts/push_to_hf.py` (updated), and `scripts/generate_manifest.py` created. Tooling ready. Actual HuggingFace upload + restore verification pending.
- **Remaining action:** Run `push_to_hf.py --all-data-artifacts` to upload snapshots + manifest to HuggingFace. Then verify round-trip with `pull_from_hf.py --all` on a fresh checkout.
- **Status:** Tooling implemented. Upload/verification pending.

### KR-DART-SCALING-001: Full KR DART build is impractical with current API strategy

- **Type:** infrastructure / scaling
- **Severity:** Medium
- **Effort:** Medium (design decision needed)
- **Found:** Session 12
- **Details:** Full KR build requires ~208,282 API calls (2,762 tickers × 19 years × 4 report types). DART API responds at ~4.4s/call. Observed throughput: ~816 calls/hour. Total time for full build: ~255 hours (~10.6 days). Session 12 completed only 47/2,762 tickers (7,541 calls cached) before killing the process.
- **CI incompatibility:** GitHub Actions has 120-min timeout. A 2-hour CI run processes ~1,632 calls. With ~200,741 remaining, full CI completion would take ~123 weekly runs (~2.4 years). CI does not persist `dart_cache.db` or checkpoint files between runs.
- **Weekly refresh estimate (corrected):** Even after full build, refreshing 1 new year for all tickers requires ~11,048 calls (2,762 × 4 report types). At 816 calls/hour, that's ~13.5 hours — exceeds CI timeout.
- **Risk if ignored:** KR market permanently incomplete or stale. Current dataset has only 453 KR annual rows vs ~2,538 in previous production build.
- **Possible solutions:**
  1. Reduce KR universe (top 500 by market cap instead of all 2,762)
  2. One-time cloud VM completion ($5 DigitalOcean droplet, run 10 days)
  3. Store completed `snapshots_kr.parquet` on HuggingFace (skip re-fetching)
  4. Improve DART fetch strategy (parallel connections, batch endpoint if available)
  5. Mark KR as partial/optional until solved
- **Fix before continuing audits?** No. KR is 4% of dataset. Other markets fully regenerated.

### MUTATION-ORDER-001: Uncontrolled in-place mutation of final parquet

- **Type:** architecture / data-safety risk
- **Severity:** Medium
- **Effort:** Medium
- **Found:** Session 1
- **Details:** `historical_dataset_clean.parquet` is mutated in-place by 16+ scripts post-step6. No enforced execution order, no idempotency checks, no versioning.
- **Risk if ignored:** Silent data corruption if mutation order changes or a script is re-run. Mitigated by documented order in PARQUET_ATLAS.
- **Fix before continuing audits?** No. Documented order works. Not blocking.
- **Recommended session:** Architecture redesign (append-only columns, checksums, or separate output files).

### MACRO-USREC-VINTAGE-001: USREC recession flag uses revised data

- **Type:** look-ahead / PIT risk
- **Severity:** Medium
- **Effort:** Medium
- **Found:** Session 3
- **Details:** FRED `USREC` returns revised NBER dates, not real-time vintage. A filing in March 2020 sees `recession=1` even though NBER hadn't announced it yet.
- **Risk if ignored:** Bounded. Recession is 1 of 355 features, affects ~10% of rows, and is a weak predictor on its own. Interaction features (`value_in_recession`, etc.) amplify slightly.
- **Fix before continuing audits?** No. Impact is small and well-documented.
- **Mitigation options:** (1) FRED vintage API, (2) 6-month lag, (3) real-time proxy, (4) exclude from model features.
- **Recommended session:** Macro refinement session (after all audits complete).

### MACRO-NO-ASOF-DATE-001: No macro_asof_date audit column

- **Type:** auditability gap
- **Severity:** Medium
- **Effort:** Small (capture matched index during merge_asof)
- **Found:** Session 3
- **Details:** Step4 uses `merge_asof(direction='backward')` correctly but doesn't record which date's observation was matched. No post-hoc PIT verification possible from data alone.
- **Risk if ignored:** Low-medium. Logic is correct and tested. Risk materializes only if someone refactors the merge logic incorrectly.
- **Fix before continuing audits?** No. Correct behavior already tested (31 tests).
- **Recommended session:** Quick enhancement session (5 lines of code + test update).

### FX-MIXED-PORTFOLIO-001: Multi-market backtest mixes local-currency returns

- **Type:** backtest presentation issue
- **Severity:** Medium
- **Effort:** Medium
- **Found:** Session 3
- **Details:** Global portfolio averages JPY/BRL/USD returns without FX conversion. Reported CAGR is not a real USD return.
- **Risk if ignored:** Misleading global CAGR. Does NOT affect model training (uses `beat_local_market`). Single-market backtests unaffected.
- **Fix before continuing audits?** No. `--market US` workaround exists. Research-only usage.
- **Design decision needed:** (1) Add USD columns to pipeline, (2) Add `--usd` option to backtester, (3) Document limitation and restrict to single-market reporting.
- **Recommended session:** Backtest refinement session.

### FEATURE-LIB-CONSOLIDATE-001: step5 and feature_library dual ownership

- **Type:** feature ownership / maintainability risk
- **Severity:** Medium
- **Effort:** Small (~10 lines in step5)
- **Found:** Session 6
- **Details:** Both `step5_compute_features.py` and `feature_library.py` contain feature logic. Step5 doesn't import from feature_library. Risk of silent formula drift.
- **Risk if ignored:** Formula divergence between pipeline-build and inference-time. Currently mitigated by test coverage on both files (32 + 19 tests).
- **Fix before continuing audits?** No. Both files tested. Working correctly.
- **Recommended session:** Refactor session after all audits complete.

### LIQUIDITY-001: No liquidity/volume feature in main dataset

- **Type:** data issue / known limitation
- **Severity:** High for backtest/portfolio realism; Medium for pipeline correctness
- **Effort:** Large (design decision + migration from legacy + PIT safety)
- **Found:** Session 0
- **Details:** ADTV exists only in backtester path. Main dataset has no volume. Universe has no liquidity filter.
- **Risk if ignored:** Backtests may include illiquid stocks with unrealistic returns. Not a correctness bug — a completeness gap.
- **Fix before continuing audits?** No. Design decision needed first.
- **Recommended session:** Feature engineering session (after audit phase complete).

---

## Open — Low Severity

### FEATURE-COVERAGE-PHASEC-001: Regenerated dataset missing Phase C columns

- **Type:** informational / Phase C pending
- **Severity:** Low (expected gap, not a bug)
- **Found:** Session 12 | **Contract defined:** Session 14
- **Details:** Current regenerated dataset has 341 columns. The 26 missing columns are Phase C outputs that require model retrain/scoring. All 26 classified by category, producer script, and dependency in `docs/developer/feature-contract.md`.
- **Classification (Session 14):**
  - OOF scores (5): `ml_6m_oof`, `ml_1y_oof`, `ml_2y_oof`, `ml_3y_oof`, `ml_5y_oof` — `generate_oof_scores.py`
  - ML scores (6): `ml_6m`, `ml_1y`, `ml_2y`, `ml_3y`, `ml_5y`, `ml_pred_excess_3y` — `score_historical.py`
  - Alpha scores (6): `alpha_value`, `alpha_quality`, `alpha_momentum`, `alpha_growth`, `alpha_fraud_risk`, `alpha_composite` — `compute_alpha.py`
  - Vol patches (5): `equity_vol_6m`, `equity_vol_12m`, `equity_vol_36m`, `equity_vol_60m`, `roa_vol_5y` — `patch_equity_vol_features.py`
  - Survivorship (1): `delisted_flag` — `mark_survivorship.py`
  - Quarterly enriched (3): `revenue_qoq_std`, `earnings_momentum`, `filing_lag_trend` — `enrich_quarterly_features.py`
- **Validation:** `python3 scripts/validate_feature_contract.py` reports Phase B COMPLETE, Phase C PENDING.
- **Risk if ignored:** None. Expected ordering dependency. Not a data regeneration bug.
- **Fix:** Run Phase C pipeline (train → OOF → score → alpha → patches).
- **Status:** Tracked by feature contract. Phase C pending by design.

### DOCS-ANNUAL-ONLY-001: data-update-guide.md incorrectly claims parquet is annual-only

- **Type:** documentation inaccuracy
- **Severity:** Low
- **Found:** Session 12
- **Details:** `docs/developer/data-update-guide.md` states "After step6 clean: annual-only" and Schema Constraints says "Annual-only: period_type == 'annual' for all rows". In reality, `historical_dataset_clean.parquet` contains both annual and quarterly rows. Downstream consumers filter `period_type == 'annual'` at read time. Quarterly rows are retained because enrichment logic uses them.
- **Status:** ✅ Fixed in Session 12 (docs corrected in data-update-guide.md).

### BROKEN-IMPORT-001: Dead imports to `pipeline.score_and_report` — RESOLVED

- **Type:** code bug / legacy-file issue
- **Severity:** Low (was High — files are ARCHIVED, not on data path)
- **Effort:** Done
- **Found:** Session 1 | **Resolved:** Session 8
- **Details:** Three legacy modules imported non-existent `pipeline.score_and_report`. All three files (`enrich_insider_signals.py`, `enrich_market_cap.py`, `enrich_market_signals.py`) moved to `pipeline/archive/` in Session 8.
- **Status:** ✅ Resolved. Files archived. No runtime risk.
- **No remaining action.**

### STEP2-NO-PERIOD-END-001: No explicit period_end column

- **Type:** missing defensive check
- **Severity:** Low
- **Effort:** Tiny
- **Found:** Session 2
- **Risk if ignored:** Near-zero. EDGAR naturally has filed_date > period_end.
- **Fix before continuing audits?** No.

---

## Accepted Design (Will Not Fix)

These are intentional design choices documented for transparency. No action needed.

### PIOTROSKI-FIRST-YEAR-001: Extension signals default to 0 for first observation

- **Type:** design choice
- **Found:** Session 6
- **Rationale:** First year has no prior-year comparison → defaults to 0 ("no improvement shown"). Conservative, consistent across all tickers, doesn't distort training. NaN would reduce training coverage by ~10%.
- **Status:** Accepted. Tested. Will not change.

### STEP1-TICKER-DEDUP-001: No dedup on ticker column

- **Type:** design choice
- **Found:** Session 2
- **Rationale:** Dedup is on CIK (primary identifier). Ticker reuse across live companies is vanishingly rare. Adding ticker dedup would risk dropping legitimate spin-offs sharing temporary tickers.
- **Status:** Accepted. Monitor only.

### STEP3-NO-DELISTED-IMPUTE-001: No -50% imputation in step 3

- **Type:** separation of concerns
- **Found:** Session 2
- **Rationale:** Step 3 returns None for missing prices. `mark_survivorship.py` handles imputation downstream. This is correct layering — step 3 shouldn't assume imputation policy.
- **Status:** Accepted. By design.

---

## Resolved (Mostly Complete)

### TEST-COVERAGE-001: Pipeline test gap — MOSTLY RESOLVED

- **Type:** test gap
- **Severity:** Was Medium, now Low (residual gap only)
- **Found:** Session 1
- **Progress:** Steps 1–6 + feature_library + p0f all have test coverage (235 tests total). Remaining gaps: p0g, enrich_fraud_labels, multi-market logic.
- **Status:** Active — completing in Session 7.

---

## Parking Lot

Non-critical ideas deferred to future sessions.

### FEATURE-CONTRACT-001: No feature set/count validation between Step 5 and Step 6 — IMPLEMENTED

- **Type:** dataset contract / guardrail
- **Severity:** Low
- **Found:** Session 5 | **Implemented:** Session 14
- **Details:** `scripts/validate_feature_contract.py` validates column group presence by pipeline phase. Checks 8 Phase B groups + 6 Phase C groups. Group-level (not brittle column-level).
- **Documentation:** `docs/developer/feature-contract.md`
- **Status:** ✅ Done.
