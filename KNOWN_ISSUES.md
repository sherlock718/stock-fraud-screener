# Known Issues

Issues found during pipeline audit (Sessions 0–6). Triaged in Session 6.5.

---

## Fix Priority Queue

Issues that should be resolved before next model train / backtest refresh:

| Priority | ID | Effort | What's Needed |
|---|---|---|---|
| **P1** | P0F-PRICE-FLOOR-001 | Tiny | Fix docstring (5-line docs-only change) |
| **P2** | DATA-ARTIFACT-001 | Small | Add snapshots.parquet to HF push + create pull script |
| **P2** | MACRO-NO-ASOF-DATE-001 | Small | Add audit column to step4 |
| **P2** | FEATURE-LIB-CONSOLIDATE-001 | Small | Refactor step5 imports |
| **P3** | LIQUIDITY-001 | Large | Design decision + feature engineering |
| **P3** | MUTATION-ORDER-001 | Medium | Architecture redesign |
| **P3** | FX-MIXED-PORTFOLIO-001 | Medium | Design decision on currency handling |
| **P3** | MACRO-USREC-VINTAGE-001 | Medium | FRED vintage API or proxy approach |

---

## Fixed (Code Done, Data Regen Pending)

### PRICE-UNADJUSTED-001: step3 uses unadjusted Close instead of Adj Close — FIXED

- **Type:** data bug
- **Severity:** Critical
- **Found:** Session 2 | **Code fixed:** Session 2.5
- **Fix:** Changed `hist['Close']` → `hist['Adj Close']` in `fetch_price_series()`.
- **Status:** ✅ Code fixed. US-only regeneration validated (Session 12). Multi-market production regeneration in progress.

### RANK-LEAKAGE-001: quality/value composites ranked across full dataset — FIXED

- **Type:** cross-temporal rank leakage
- **Severity:** Medium
- **Found:** Session 4 | **Code fixed:** Session 4.5
- **Fix:** Added `.groupby(['fiscal_year', 'market'])` before `.rank(pct=True)` in `add_composite_scores()`.
- **Status:** ✅ Code fixed. US-only regeneration validated (Session 12). Multi-market production regeneration in progress.

### TAXONOMY-SUSPECT-OVERWRITE-001: enrich_fraud_taxonomy.py overwrites fraud_suspect — FIXED

- **Type:** data/semantic issue
- **Severity:** Medium
- **Found:** Session 10 | **Code fixed:** Session 11
- **Fix:** Removed `build_fraud_suspect()` from taxonomy. `fraud_suspect` now exclusively owned by `enrich_fraud_labels.py`.
- **Status:** ✅ Code fixed. US-only regeneration validated (Session 12). Multi-market production regeneration in progress.

---

## Fixed (Complete)

### DOCS-PARQUET-ATLAS-001: PARQUET_ATLAS listed non-existent macro columns — FIXED

- **Type:** documentation inaccuracy
- **Found:** Session 3 | **Fixed:** Session 3
- **Status:** ✅ Done. No remaining action.

---

## Open — Medium Severity

### DATA-ARTIFACT-001: Intermediate parquet files not persisted externally

- **Type:** infrastructure / developer experience
- **Severity:** Medium
- **Effort:** Small
- **Found:** Session 12
- **Details:** `data/snapshots.parquet`, `data/prices.parquet`, `data/macro.parquet` are gitignored ephemeral build artifacts. They are not stored on HuggingFace or any external storage. Only `historical_dataset_clean.parquet` is pushed to HF. This means any fresh checkout requires a full Step 1–2 rebuild (1–4 hours, network-dependent) before Step 3–6 can run.
- **Risk if ignored:** Developer friction. Any contributor must run multi-hour EDGAR fetch before they can iterate on features or scoring.
- **Recommended fix:** (1) Add `snapshots.parquet` to HuggingFace push. (2) Create `scripts/pull_from_hf.py` download script. (3) Optionally add `ARTIFACT_MANIFEST.json` with checksums.
- **Fix before continuing audits?** No. Working around it by running full pipeline locally.

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

### BROKEN-IMPORT-001: Dead imports to `pipeline.score_and_report` — RESOLVED

- **Type:** code bug / legacy-file issue
- **Severity:** Low (was High — files are ARCHIVED, not on data path)
- **Effort:** Done
- **Found:** Session 1 | **Resolved:** Session 8
- **Details:** Three legacy modules imported non-existent `pipeline.score_and_report`. All three files (`enrich_insider_signals.py`, `enrich_market_cap.py`, `enrich_market_signals.py`) moved to `pipeline/archive/` in Session 8.
- **Status:** ✅ Resolved. Files archived. No runtime risk.
- **No remaining action.**

### P0F-PRICE-FLOOR-001: Price floor applied to all exchanges, not just OTC

- **Type:** doc/code mismatch
- **Severity:** Low
- **Effort:** Tiny (fix docstring, 5 lines)
- **Found:** Session 6
- **Details:** Code excludes ALL stocks below market floor regardless of exchange. Docstring says "only OTC." Code behavior is correct (stricter is better).
- **Risk if ignored:** Zero data impact. Documentation confusion only.
- **Fix before continuing audits?** No, but easy to include in any session.
- **Recommended fix:** Change docstring + PIPELINE_ATLAS checklist 7.6 to match code. Remove unused `OTC_EXCHANGES` constant.

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

### FEATURE-CONTRACT-001: No feature set/count validation between Step 5 and Step 6

- **Type:** dataset contract / future guardrail
- **Severity:** Low
- **Found:** Session 5
- **Details:** Step 6 doesn't validate feature count/groups. If step5 silently drops a feature group, step6 passes it through.
- **Future fix:** Lightweight contract test asserting required feature groups exist.
- **Action:** Implement when pipeline stability is prioritized.
