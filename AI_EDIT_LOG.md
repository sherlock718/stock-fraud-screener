# AI Edit Log

Record of all AI-assisted changes, session by session.

---

## Session 0 — Guardrails Setup (2026-06-20)

**Branch:** `refactor/s0-guardrails`

**Files created:**
- `.claude/settings.json` — read-only bash permissions (grep, find, ls, wc, git log/status/diff)
- `scripts/hooks/pre_commit_guard.py` — tracked pre-commit guardrail script
- `.git/hooks/pre-commit` — calls tracked script (not committed, local only)
- `KNOWN_ISSUES.md` — issue tracker with Parking Lot section
- `AI_EDIT_LOG.md` — this file

**Guardrails active:**
- Pre-commit warns if >5 files staged
- Pre-commit warns if pipeline/step* changes without tests/pipeline/ change
- Pre-commit blocks data/*.parquet commits
- Branch-per-session enforced
- No production code changes in Sessions 0–1

**Known issues logged:**
- LIQUIDITY-001: No liquidity/volume feature in main dataset (high priority)

---

## Session 1 — Pipeline & Parquet Atlas (2026-06-21)

**Branch:** `refactor/s1-atlas`

**Files created:**
- `PIPELINE_ATLAS.md` — Full pipeline reference map (File Map, Call Graph, Test Matrix, Step Audit Checklist, Feature Coverage Matrix)
- `PARQUET_ATLAS.md` — Every parquet file: creator, readers, mutators, keys, type

**Key findings:**
- 33 files in `pipeline/`. 18 are CURRENT_CORE (step1→step6 chain), 5 CURRENT_SUPPORT, 1 QA utility, 9 LEGACY_ARCHIVE_CANDIDATE (JSON-era, operate on `companies_financials.json`)
- 3 legacy modules import `pipeline.score_and_report` which DOES NOT EXIST (broken imports, dead code path)
- `historical_dataset_clean.parquet` is mutated in-place by 16+ scripts post-step6 (fragile, order-dependent)
- Test coverage: only `tests/test_pipeline.py` exists — covers train_models logic only. Zero coverage for pipeline steps 1–6
- Liquidity features (ADTV, bid-ask) exist only in legacy JSON pipeline. Not connected to parquet pipeline
- Insider signals, governance, market signals — same: legacy JSON only, not in step5

**No production code changes. No test files created. Code-reading and grep only.**

**Session-end checklist:**
- Atlas update needed? No. Created this session.
- Parquet atlas update needed? No. Created this session.
- KNOWN_ISSUES update needed? Yes. Added BROKEN-IMPORT-001, MUTATION-ORDER-001, TEST-COVERAGE-001.
- AI_EDIT_LOG handoff updated? Yes.

---

## Living-Doc Maintenance Rules

`PIPELINE_ATLAS.md` and `PARQUET_ATLAS.md` are living documents. Every future session must update them if it changes pipeline file roles, file classifications, imports/call graph, parquet creators/readers/mutators, archive status, test coverage status, or feature ownership/layer classification. If no atlas update is needed, the session report must say why.

Before ending any session, check whether `PIPELINE_ATLAS.md`, `PARQUET_ATLAS.md`, `KNOWN_ISSUES.md`, or `AI_EDIT_LOG.md` needs updating. If not, say why.

---

## Session 2 — Audit Steps 1–3 + Tests (2026-06-21)

**Branch:** `refactor/s2-audit-steps-1-3`

**Files created:**
- `tests/pipeline/test_step1_fetch_tickers.py` — 14 tests: schema contract, dedup logic, survivorship, identifiers
- `tests/pipeline/test_step2_build_snapshots.py` — 16 tests: schema, primary key uniqueness, temporal integrity, YoY computation, coverage gating
- `tests/pipeline/test_step3_enrich_prices.py` — 30 tests: price lookup, forward return (future-only), momentum (past-only), volatility, 52w high, benchmark selection, enrich_row temporal contracts, survivorship handling, 1 xfail for PRICE-UNADJUSTED-001

**Files modified:**
- `KNOWN_ISSUES.md` — added 4 new issues
- `PIPELINE_ATLAS.md` — updated Test Matrix status + Coverage Summary
- `AI_EDIT_LOG.md` — this session report + Session 3 handoff

**Audit findings:**

| Step | Finding | Severity | ID |
|------|---------|----------|-----|
| Step 3 | Uses `hist['Close']` (unadjusted) instead of `hist['Adj Close']` (split-adjusted) | Critical | PRICE-UNADJUSTED-001 |
| Step 1 | Dedup on CIK only, no ticker-level dedup | Low | STEP1-TICKER-DEDUP-001 |
| Step 2 | No explicit `period_end` column for filed_date validation | Low | STEP2-NO-PERIOD-END-001 |
| Step 3 | No -50% delisted imputation (by design, handled downstream) | Low | STEP3-NO-DELISTED-IMPUTE-001 |

**Temporal integrity validated (no look-ahead bias):**
- Step 3 `forward_return()` uses only post-entry prices ✅
- Step 3 `prior_return()` uses only pre-entry prices ✅
- Step 3 `entry_date = filed_date` (PIT-safe) ✅
- Step 2 `filed_date` from actual EDGAR filing timestamp ✅
- Step 2 keeps most recently filed value per (fy, fp) key ✅

**No production code changes. Tests only + documentation updates.**

---

## Session 2.5 — Fix PRICE-UNADJUSTED-001 (2026-06-21)

**Branch:** `fix/s2-5-adjusted-close-step3`

**Production code changed:**
- `pipeline/step3_enrich_prices.py` line 215: `hist['Close'].copy()` → `hist['Adj Close'].copy()`

**Test changed:**
- `tests/pipeline/test_step3_enrich_prices.py`: removed `@pytest.mark.xfail` from `test_fetch_uses_adj_close_not_close`, renamed class `TestPriceUnadjustedBug` → `TestPriceAdjustedClose`

**Docs updated:**
- `KNOWN_ISSUES.md` — PRICE-UNADJUSTED-001 marked FIXED, remaining action noted (cache invalidation + step 3 rerun)
- `PIPELINE_ATLAS.md` — step3 status → "✅ covered", test count → 32
- `AI_EDIT_LOG.md` — this report

**Remaining action for data regeneration:**
1. Delete `data/price_cache.db`
2. Re-run `python3 pipeline/step3_enrich_prices.py`
3. Re-run step5 and step6 to propagate corrected prices through features

---

## Session 3 — Audit Step 4 (Macro) + Tests (2026-06-21)

**Branch:** `refactor/s3-audit-step4`

**Files created:**
- `tests/pipeline/test_step4_enrich_macro.py` — 31 tests: schema contract, row count preservation, PIT safety (backward merge_asof), NaN catastrophe prevention, derived formula correctness (CPI YoY, credit tightening, real rate, macro regime), panel structure, lookup function

**Files modified:**
- `KNOWN_ISSUES.md` — added MACRO-NO-ASOF-DATE-001 (Medium), DOCS-PARQUET-ATLAS-001 (Low)
- `PIPELINE_ATLAS.md` — updated Test Matrix status for step4, updated Coverage Summary
- `PARQUET_ATLAS.md` — corrected inaccurate key columns for `data/macro.parquet`
- `AI_EDIT_LOG.md` — this session report + Session 4 handoff

**Audit findings:**

| # | Check | Verdict | Notes |
|---|---|---|---|
| 4.1 | Schema | ✅ | 6 key cols + 12 macro cols. No gdp_growth/unemployment (those were documentation errors) |
| 4.2 | Row count | ✅ | `snap_out = snap[key_cols].copy()` — always exactly len(snap) rows |
| 4.3 | No future macro | ✅ | `pd.merge_asof(direction='backward')` — provably no future data |
| 4.4 | macro_asof_date | ⚠️ | Column does NOT exist. Logged as MACRO-NO-ASOF-DATE-001 |
| 4.5 | Derived formulas | ✅ | real_rate, credit_tightening, macro_regime — all correct |
| 4.6 | Fill rates | ✅ | Forward-fill from FRED_START=2007 ensures >95% coverage post-2009 |
| 4.7 | Same-date consistency | ✅ | Single asof lookup per row → all macro cols share same temporal anchor |
| 4.8 | Recession look-ahead | ⚠️ | USREC uses current/revised FRED data, not vintage. Possible look-ahead risk. Tracked as MACRO-USREC-VINTAGE-001 |
| 4.9 | US vs local macro | ⚠️ known limitation | All markets get US FRED. Non-US local rates would be more appropriate but not a bug |

**PRICE-UNADJUSTED-001:** Already fixed in Session 2.5. No action needed.

**FX Audit (completed same session):**

| # | Check | Verdict |
|---|---|---|
| FX.1 | Local vs USD returns | ✅ step3 stores local. USD only via `bias_audit --fix` |
| FX.2 | Entry/exit FX dates | ✅ `.asof(filed_date)` and `.asof(exit_date)` |
| FX.3 | Historical FX not current | ✅ Full history from 2005, `.asof()` lookup |
| FX.4 | Missing 6m/2y FX | ⚠️ Not computed (bias_audit only handles 1y/3y/5y) |
| FX.5 | GitHub refresh FX | ⚠️ CI runs `--ci`, not `--fix`. No USD cols in CI |
| FX.6 | Train/backtest mix | ⚠️ Train OK (local). Backtest mixes currencies in global runs |

FX issue logged: FX-MIXED-PORTFOLIO-001 (Medium) — multi-market backtest averages local-currency returns across JPY/BRL/USD without conversion.

**No production code changes. Tests only + documentation updates/corrections.**

---

## Session 3 Docs Correction (2026-06-21)

**Branch:** `docs/s3-macro-fx-audit-corrections`

**Docs-only corrections applied before Session 4:**

1. **USREC PIT risk:** Session 3 originally marked check 4.8 (recession look-ahead) as ✅ PIT-safe. This was too confident. FRED USREC uses revised NBER dates, not real-time vintage data. Corrected verdict to ⚠️. Added MACRO-USREC-VINTAGE-001 (Medium).

2. **Step 4 schema clarification:** Updated PIPELINE_ATLAS checklist item 4.1 to show actual output columns. Clarified that missing `gdp_growth`/`unemployment` are optional future features (not bugs), `yield_curve` is the equivalent of the old `spread_10y2y`, and `macro_asof_date` absence is already tracked.

3. **FX-MIXED-PORTFOLIO-001 clarification:** Expanded KNOWN_ISSUES entry with explicit design decision options, severity upgrade criteria, and confirmation that training is unaffected.

**Files modified:** `PIPELINE_ATLAS.md`, `KNOWN_ISSUES.md`, `AI_EDIT_LOG.md`

**No production code changes. No tests. No parquet changes.**

---

## Next Claude Session Handoff

- Status: Session 3 complete (step 4 audited + FX audited, 31 tests added)
- Branch: `refactor/s3-audit-step4` (local commit, not pushed, not merged)
- Files created: `tests/pipeline/test_step4_enrich_macro.py` (31 tests)
- Files modified: `KNOWN_ISSUES.md`, `PIPELINE_ATLAS.md`, `PARQUET_ATLAS.md`, `AI_EDIT_LOG.md`
- Guardrails: pre-commit hook active
- Issues found: MACRO-NO-ASOF-DATE-001 (medium), DOCS-PARQUET-ATLAS-001 (low, fixed), FX-MIXED-PORTFOLIO-001 (medium)
- PRICE-UNADJUSTED-001: Already fixed in Session 2.5. No action needed this session.
- Next goal: **Session 4 — Audit Step 5 (compute features) and add minimal critical tests for Step 5.**
- Branch flow: User merges `refactor/s3-audit-step4` into `main`, then Session 4 creates `refactor/s4-audit-step5` from updated `main`

### Session-end checklist (Session 3)

- Atlas update needed? Yes. Updated Test Matrix status for step4, updated Coverage Summary.
- Parquet atlas update needed? Yes. Fixed inaccurate key columns for `data/macro.parquet` (was listing non-existent gdp_growth/unemployment/spread_10y2y/macro_asof_date; now lists actual 12 macro columns).
- KNOWN_ISSUES update needed? Yes. Added MACRO-NO-ASOF-DATE-001 (Medium), DOCS-PARQUET-ATLAS-001 (Low).
- AI_EDIT_LOG handoff updated? Yes.

### Session 4 prompt (copy-paste into a fresh Claude Code conversation):

```
Start Session 4. Project: /Users/mhoque/Desktop/stock-fraud-screener-main
...
```

---

## Session 4 — Audit Step 5 (Compute Features) + Tests (2026-06-21)

**Branch:** `refactor/s4-audit-step5`

**Files created:**
- `tests/pipeline/test_step5_compute_features.py` — 28 tests + 1 xfail: temporal leakage (static + runtime), label leakage (static), rank leakage (momentum/sector pass + composite xfail), formula correctness (8 key features), winsorization (4 tests), output shape (3 tests), rolling past-only, safe division (4 tests), full pipeline smoke (3 tests)

**Files modified:**
- `KNOWN_ISSUES.md` — added RANK-LEAKAGE-001 (Medium)
- `PIPELINE_ATLAS.md` — updated Test Matrix status for step5, updated Coverage Summary
- `AI_EDIT_LOG.md` — this session report + Session 5 handoff

**Audit findings:**

| # | Check | Verdict | Notes |
|---|---|---|---|
| 5.1 | Formula correctness | ✅ | PE, ROA, Sloan, Altman Z coefficients, Beneish M coefficients — all correct |
| 5.2 | Temporal leakage | ✅ | forward_return_*, beat_local_market_* never used in feature functions (static + runtime verified) |
| 5.3 | Label leakage | ✅ | No fraud_confirmed, ml_*, alpha_* referenced in feature code |
| 5.4 | Rank leakage | ⚠️ | `momentum_ranks` and `sector_percentiles` correctly groupby fiscal_year. BUT `quality_composite` and `value_composite` rank across full dataset. Logged as RANK-LEAKAGE-001 |
| 5.5 | Rolling past-only | ✅ | `roe_volatility_5yr` uses trailing `.rolling(5)` after sort by (ticker, fiscal_year). Past-only confirmed |
| 5.6 | Coverage | ✅ | With valid inputs, all critical features are non-NaN |
| 5.7 | Value sanity (winsorization) | ✅ | `ratio_cols` list comprehensive, includes all growth_yoy columns (Rule 6) |
| 5.8 | Cross-market | ✅ | No hardcoded US assumptions. `market` column used for groupby. Altman X4 fallback uses book equity for non-US |
| 5.9 | Dataset shape | ✅ | Row count preserved through all functions. 100+ features computed |
| 5.10 | Step 5 sole authority | ✅ | All features computed here. No other pipeline script creates features that bypass step5 |

**No production code changes. Tests only + documentation updates.**

---

## Session 4.5 — Fix RANK-LEAKAGE-001 (2026-06-21)

**Branch:** `refactor/s4-audit-step5`

**Production code changed:**
- `pipeline/step5_compute_features.py` lines 528–551: `add_composite_scores()` — replaced global `.rank(pct=True)` with `.groupby(['fiscal_year', 'market']).rank(pct=True)` for both `quality_composite` and `value_composite`.

**Grouping choice:** `['fiscal_year', 'market']` — matches the pattern already used by `add_momentum_ranks` (line 578). Without market grouping, a Japanese micro-cap in 2015 would be ranked against US mega-caps in the same year. Sector percentiles use `['sic_2digit', 'fiscal_year']`; momentum ranks use `['fiscal_year', 'market']`. The composites are factor-level signals like momentum, so market×year is the correct cohort.

**Test changed:**
- `tests/pipeline/test_step5_compute_features.py`: removed `@pytest.mark.xfail` decorator from `test_quality_composite_grouped_by_fiscal_year`, simplified test body to assert groupby presence in source.

**Docs updated:**
- `KNOWN_ISSUES.md` — RANK-LEAKAGE-001 marked FIXED, remaining action noted (step 5 rerun for data regen)
- `PIPELINE_ATLAS.md` — test count updated: 29 tests (no xfail)
- `AI_EDIT_LOG.md` — this report

**Remaining action for data regeneration:**
1. Re-run `python3 pipeline/step5_compute_features.py`
2. Re-run step6 to propagate corrected composites
3. Re-run downstream scripts that use `quality_composite` or `value_composite`

---

## Session 4.6 — Behavioral regression tests for RANK-LEAKAGE-001 fix (2026-06-21)

**Branch:** `refactor/s4-audit-step5`

**Test changes only:**
- `tests/pipeline/test_step5_compute_features.py`: added 3 behavioral regression tests to `TestRankLeakage`:
  - `test_composite_future_year_isolation` — proves adding extreme 2021 rows does NOT change 2020 composites
  - `test_composite_market_isolation` — proves adding extreme JP rows does NOT change US composites in same year
  - `test_composite_nan_preserved_for_all_nan_inputs` — proves all-NaN row produces NaN composite
- Kept existing source-text assertion as belt-and-suspenders
- Test count: 29 → 32

**Docs updated:**
- `PIPELINE_ATLAS.md` — test count 32, coverage summary updated
- `AI_EDIT_LOG.md` — this entry

**No production code changes. PARQUET_ATLAS unchanged (no schema/flow change).**

---

## Session 5 — Audit Step 6 (Clean) + Tests (2026-06-21)

**Branch:** `refactor/s5-audit-step6`

**Files created:**
- `tests/pipeline/test_step6_clean.py` — 33 tests: row drop criteria (9), dedup logic (4), infinity handling (4), required columns (3), row count stability (3), filing_lag_days (3), as_of_date (2), sort order (1), no accidental filtering (3), multi-market dedup isolation (1)

**Files modified:**
- `PIPELINE_ATLAS.md` — updated Test Matrix status for step6, updated Coverage Summary
- `AI_EDIT_LOG.md` — this session report + Session 6 handoff

**Audit findings:**

| # | Check | Verdict | Notes |
|---|---|---|---|
| 6.1 | Non-null required cols | ✅ | `dropna(subset=REQUIRED_COLS)` — 5 cols: cik, ticker, filed_date, fiscal_year, period_type |
| 6.2 | No duplicates | ✅ | Dedup on `['cik', 'market', 'filed_date', 'period_type']`, keep='first' |
| 6.3 | No infinities | ✅ | All numeric cols: `replace([np.inf, -np.inf], np.nan)` |
| 6.4 | as_of_date == filed_date | ✅ | Line 80 explicitly sets this |
| 6.5 | filing_lag_days | ✅ (with caveat) | Hardcodes Dec-31 FY end. Non-Dec-FY companies get negative lag. Documented in docstring |
| 6.6 | No accidental filtering | ✅ | No revenue, market cap, sector, or price filters |
| 6.7 | Row count stable | ✅ | Conservative filters. Reports pct kept. In practice <1% dropped from valid data |

**Key observations (no issues to log):**
1. `filing_lag_days` assumes Dec-31 FY end — intentional, documented in code (line 79: "negative = non-Dec FY"). Not a bug.
2. Dedup key does not include `fiscal_year` — by design (two filings from same company on same date with different FYs is essentially impossible in practice).
3. Step 6 is minimal and well-behaved: 4 filters, 2 derived columns, sort, save. No hidden complexity.

**RANK-LEAKAGE-001:** Already fixed in Session 4.5 and merged to main. No action needed this session.

**No production code changes. Tests only + documentation updates.**

---

## Next Claude Session Handoff

- Status: Session 5 complete (step 6 audited, 33 tests added, all 168 tests pass)
- Branch: `refactor/s5-audit-step6` (local commit, not pushed, not merged)
- Files created: `tests/pipeline/test_step6_clean.py` (33 tests)
- Files modified: `PIPELINE_ATLAS.md`, `AI_EDIT_LOG.md`
- Guardrails: pre-commit hook active
- Issues found: None new. Step 6 is clean.
- Next goal: **Session 6 — Audit feature_library.py + p0f_universe_definition.py. Add tests for both.**
- Branch flow: User merges `refactor/s5-audit-step6` into `main`, then Session 6 creates `refactor/s6-audit-support` from updated `main`

### Session-end checklist (Session 5)

- Atlas update needed? Yes. Updated Test Matrix status for step6 (missing → ✅ covered), updated Coverage Summary.
- Parquet atlas update needed? No. Step 6 input/output files unchanged from Session 1 mapping.
- KNOWN_ISSUES update needed? No. Audit found no new issues. filing_lag_days behavior is documented and intentional.
- AI_EDIT_LOG handoff updated? Yes.

### Session 6 prompt (copy-paste into a fresh Claude Code conversation):

```
Start Session 6. Project: /Users/mhoque/Desktop/stock-fraud-screener-main

First, verify setup:
1. Run git status — confirm clean working tree
2. Confirm current branch is main
3. Create and checkout branch: git checkout -b refactor/s6-audit-support

Then read these files to understand the codebase state:
- PIPELINE_ATLAS.md (file map, call graph, test matrix, step audit checklist)
- PARQUET_ATLAS.md (parquet file registry, mutation order)
- KNOWN_ISSUES.md (logged issues from Sessions 0–5)
- AI_EDIT_LOG.md (session history and handoff)

Session 6 goal: Audit CURRENT_SUPPORT modules and add minimal critical tests.

Scope:
- Audit pipeline/feature_library.py (49 lines): formula correctness for add_normalised_ratios and add_piotroski_ext
- Audit pipeline/p0f_universe_definition.py (257 lines): filter logic, market-specific rules, in_universe semantics, fail-open behavior
- Write minimal critical tests for both (synthetic data, no network calls)
- Tests for feature_library: known-input → known-output for normalised ratios and Piotroski extension
- Tests for p0f: quarterly excluded, fiscal year bounds, revenue/assets filters (--apply-filters), SIC exclusions, missing data fail-open, in_universe column semantics

Rules:
- No broad refactor
- No feature engineering
- No archive/move/delete of pipeline files
- If a critical issue is found during audit, classify it, propose the smallest fix, and wait for approval before changing production code

At session end, update:
- KNOWN_ISSUES.md — add any new issues found during audit
- PIPELINE_ATLAS.md — update Test Matrix status column if tests are added
- AI_EDIT_LOG.md — add Session 6 report + Session 7 handoff with full prompt

Before ending the session, check whether PIPELINE_ATLAS.md, PARQUET_ATLAS.md, KNOWN_ISSUES.md, or AI_EDIT_LOG.md needs updating. If not, say why.

Session-end checklist:
- Atlas update needed? Yes/No. Reason:
- Parquet atlas update needed? Yes/No. Reason:
- KNOWN_ISSUES update needed? Yes/No. Reason:
- AI_EDIT_LOG handoff updated? Yes/No.

Commit locally at end. Do not push. Do not merge.
```
