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

## Session 5.1 — Log FEATURE-CONTRACT-001 (2026-06-21)

**Branch:** `refactor/s5-audit-step6`

**Docs updated:**
- `KNOWN_ISSUES.md` — added FEATURE-CONTRACT-001 to Parking Lot (dataset contract / future guardrail, Low severity)
- `PIPELINE_ATLAS.md` — added checklist item 6.8 noting feature completeness is intentionally out of Step 6 scope
- `AI_EDIT_LOG.md` — this entry

**No production code changes. No tests. No parquet changes.**

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
...
```

---

## Session 6 — Audit CURRENT_SUPPORT Modules + Tests (2026-06-21)

**Branch:** `refactor/s6-audit-support`

**Files created:**
- `tests/pipeline/test_feature_library.py` — 19 tests: normalised ratio formulas (5 exact values), effective tax rate, zero assets→NaN, missing cols, no overwrite, NaN propagation, Piotroski ext signals, f_score_9 sum, missing source skips, first-year behavior
- `tests/pipeline/test_p0f.py` — 48 tests: structural rules (9), investable filters (23), in_universe semantics (8), market-specific (3), missing columns (4), fail-open/fail-closed verification

**Files modified:**
- `KNOWN_ISSUES.md` — added P0F-PRICE-FLOOR-001 (Low), PIOTROSKI-FIRST-YEAR-001 (Low)
- `PIPELINE_ATLAS.md` — updated Test Matrix status for feature_library + p0f, updated Coverage Summary
- `AI_EDIT_LOG.md` — this session report + Session 7 handoff

**Audit findings:**

### feature_library.py (49 lines)

| # | Check | Verdict | Notes |
|---|---|---|---|
| FL.1 | `add_normalised_ratios` formulas | ✅ | 5 ratios = source/total_assets. Zero-guard correct. |
| FL.2 | `effective_tax_rate` formula | ✅ | tax_expense/pretax_income, only for pti>0. Standard. |
| FL.3 | Zero total_assets handling | ✅ | Replaced with NaN before division. Correct. |
| FL.4 | Missing columns handling | ✅ | `df.get()` returns None, loop guards on `src in df.columns`. Safe. |
| FL.5 | Overwrite protection | ✅ | `if dst not in df.columns` prevents clobber. |
| FL.6 | `add_piotroski_ext` signals | ✅ | shares_ok (no dilution), delta_gm (improving), delta_at (improving). All standard. |
| FL.7 | Sort before shift | ✅ | `sort_values(['ticker', 'fiscal_year'])` ensures correct temporal ordering. |
| FL.8 | Cross-ticker isolation | ✅ | `groupby('ticker').transform()` — shift is per-ticker. |
| FL.9 | f_score_9 summation | ✅ | `min_count=1` in `.sum()` — NaN-safe. |
| FL.10 | First-year behavior | ⚠️ Low | Shift→NaN→comparison→False→0.0. Logs as PIOTROSKI-FIRST-YEAR-001 (design choice, not bug). |

### p0f_universe_definition.py (257 lines)

| # | Check | Verdict | Notes |
|---|---|---|---|
| P0F.1 | Quarterly excluded | ✅ | `ptype != 'annual'` flagged. |
| P0F.2 | FY >= 2009 | ✅ | Correct lower bound. |
| P0F.3 | FY <= current_year - 1 | ✅ | Incomplete year excluded. |
| P0F.4 | Revenue/assets fail-open | ✅ | `notna() & (val < threshold)` — NaN passes through. |
| P0F.5 | Price fail-closed | ✅ | `isna() | (price <= 0)` → excluded. Intentional (no investability without price). |
| P0F.6 | SIC financials 6000–6999 | ✅ | Inclusive boundaries, NaN-safe. |
| P0F.7 | SIC utilities 4900–4999 | ✅ | Same pattern. |
| P0F.8 | Market-specific floors | ✅ | US=$1, CA=$0.05, others=0. Map with fillna(0). |
| P0F.9 | EU market codes | ✅ | EU tickers use country codes (DE/FR/etc.), all map to 0.0. |
| P0F.10 | Price floor vs OTC check | ⚠️ Low | Code applies floor to ALL exchanges, not just OTC. Logged as P0F-PRICE-FLOOR-001. |
| P0F.11 | in_universe dtype | ✅ | int8. |
| P0F.12 | excl_reason pipe format | ✅ | No leading/trailing pipes. Multiple reasons pipe-separated. |
| P0F.13 | No mutation of input | ✅ | `df.copy()` before adding columns. |

**No production code changes. Tests only + documentation updates.**

---

## Session 6.1 — Log FEATURE-LIB-CONSOLIDATE-001 (2026-06-21)

**Branch:** `refactor/s6-audit-support` (merged to `main`)

**Docs updated:**
- `KNOWN_ISSUES.md` — added FEATURE-LIB-CONSOLIDATE-001 to Parking Lot (feature ownership / maintainability risk, Medium severity)
- `AI_EDIT_LOG.md` — this entry

**No production code changes. No tests. No parquet changes.**

---

## Session 6.5 — Known Issues Triage (2026-06-21)

**Branch:** `refactor/s6-5-known-issues-triage`

**Purpose:** Triage all issues from Sessions 0–6. Reorganize KNOWN_ISSUES.md with clear classifications, fix priority queue, and accepted-design dispositions.

**Files modified:**
- `KNOWN_ISSUES.md` — full reorganization (see triage results below)
- `AI_EDIT_LOG.md` — this session report + updated handoff

**Triage results (12 unique issues + 3 accepted designs):**

| ID | Status | Severity | Effort | Fix Before Continuing? |
|---|---|---|---|---|
| PRICE-UNADJUSTED-001 | ✅ Code fixed, data regen pending | Critical | Tiny (rerun) | Yes — before model train |
| RANK-LEAKAGE-001 | ✅ Code fixed, data regen pending | Medium | Tiny (rerun) | Yes — before model train |
| DOCS-PARQUET-ATLAS-001 | ✅ Fixed complete | Low | — | Done |
| MUTATION-ORDER-001 | Open | Medium | Medium | No |
| MACRO-USREC-VINTAGE-001 | Open | Medium | Medium | No |
| MACRO-NO-ASOF-DATE-001 | Open | Medium | Small | No |
| FX-MIXED-PORTFOLIO-001 | Open | Medium | Medium | No |
| FEATURE-LIB-CONSOLIDATE-001 | Parking lot | Medium | Small | No |
| LIQUIDITY-001 | Open | High (backtest realism) / Medium (pipeline correctness) | Large | No (design decision) |
| BROKEN-IMPORT-001 | Open | Low (was High) | Tiny | No |
| P0F-PRICE-FLOOR-001 | Open | Low | Tiny | No |
| STEP2-NO-PERIOD-END-001 | Open | Low | Tiny | No |
| TEST-COVERAGE-001 | Mostly resolved | Low | Ongoing | No |
| PIOTROSKI-FIRST-YEAR-001 | Accepted design | — | — | Never |
| STEP1-TICKER-DEDUP-001 | Accepted design | — | — | Never |
| STEP3-NO-DELISTED-IMPUTE-001 | Accepted design | — | — | Never |
| FEATURE-CONTRACT-001 | Parking lot | Low | Small | No |

**Key decisions:**
1. BROKEN-IMPORT-001 downgraded High→Low (legacy files, not on data path, zero runtime risk)
2. LIQUIDITY-001 reclassified: High for backtest/portfolio realism, Medium for pipeline correctness (missing feature, not a bug — needs design decision)
3. Three issues reclassified as "Accepted Design" (will not fix — intentional choices)
4. TEST-COVERAGE-001 marked "Mostly Resolved" (Steps 1–6 + support modules covered, 235 tests)
5. Added Fix Priority Queue section for quick-reference ordering

**Top fixes before next model train / real backtest refresh:**
1. Data regeneration: rerun step3→step5→step6 (fixes PRICE-UNADJUSTED-001 + RANK-LEAKAGE-001)
2. P0F-PRICE-FLOOR-001: fix docstring to match code (5 lines, docs-only)
3. MACRO-NO-ASOF-DATE-001: add audit column (~5 lines production code + test)

**Top 3 deferred items (need design decisions, not blocking audits):**
1. LIQUIDITY-001: whether to add ADTV to pipeline, where, PIT safety approach
2. FX-MIXED-PORTFOLIO-001: whether to add USD columns or restrict global backtest
3. MUTATION-ORDER-001: whether to enforce order, use checksums, or restructure output

**No production code changes. No tests. No parquet changes.**

---

## Session 7 — Enrichment Audit + Tests (2026-06-21)

**Branch:** `refactor/s7-audit-enrichment` (recreated from updated `main` after Session 6.5 merge)

**Files created:**
- `tests/pipeline/test_p0g_confidence_score.py` — 27 tests: score range, coverage, consistency, timeliness, composite, idempotency
- `tests/pipeline/test_enrich_fraud_labels.py` — 31 tests: known CIKs, AAER window, suspect signals, label semantics, idempotency

**Files modified:**
- `PIPELINE_ATLAS.md` — updated Test Matrix status for p0g + enrich_fraud_labels, updated Coverage Summary
- `AI_EDIT_LOG.md` — this session report + Session 8 handoff

**Audit findings:**

### p0g_confidence_score.py (255 lines)

| # | Check | Verdict | Notes |
|---|---|---|---|
| P0G.1 | Columns read | ✅ | 19 core analytical cols + filing_lag_days + fiscal_year |
| P0G.2 | Column created | ✅ | `data_confidence` float [0,1] |
| P0G.3 | Downstream usage | ✅ | Only DB schema. NOT used in training/backtest/features |
| P0G.4 | Deterministic | ✅ | Pure function of existing columns |
| P0G.5 | Idempotent | ✅ | Overwrites same column with same result |
| P0G.6 | Leakage check | ⚠️ Info | `forward_return_1y` in coverage group is a target col. Not a bug because `data_confidence` is never a model feature. Reporting/quality metric only. |
| P0G.7 | Score range | ✅ | `clip(0.0, 1.0)` guarantees bounds |
| P0G.8 | NaN handling | ✅ | Missing cols → 0.5 default. Missing values → lower coverage score |
| P0G.9 | Mutator vs report | Info | Currently parquet mutator. Could be report-only. No consumer filters on it. Keep as-is. |

### enrich_fraud_labels.py (383 lines)

| # | Check | Verdict | Notes |
|---|---|---|---|
| FL.1 | Labels created | ✅ | `fraud_confirmed` (int8), `fraud_suspect` (int8) |
| FL.2 | Confirmed vs suspicious | ✅ | `fraud_confirmed` = SEC enforcement only. `fraud_suspect` = quantitative red flags (2+ signals). |
| FL.3 | Target leakage | ✅ | Both in EXCLUDE set in train_models.py. Never used as features. |
| FL.4 | Mutates in-place | ✅ | Yes. Reads + writes `historical_dataset_clean.parquet`. |
| FL.5 | Label/feature separation | ✅ | Clear. Labels here. Taxonomy scores in separate file. |
| FL.6 | Deterministic | ✅ | With cache. Non-deterministic only on first AAER fetch. |
| FL.7 | Idempotent | ✅ | Overwrites both columns fully each run. |
| FL.8 | Missing/fallback | ✅ | Falls back to KNOWN_FRAUD_CIKS if AAER unavailable. |
| FL.9 | Confirmed overrides suspect | ✅ | Line 327: `fraud_suspect=0` where `fraud_confirmed=1`. No double-count. |
| FL.10 | Enforcement window | ✅ | [e_year-5, e_year+2]. Documented. Intentional. |
| FL.11 | Separate from taxonomy | ✅ | Yes. Different concerns (is-fraud vs what-type). |

**No critical issues found. No production code changes. Tests only + documentation updates.**

---

## Next Claude Session Handoff

- Status: Session 9 complete (docs sync — all stale references to 10 archived legacy files fixed, 296 tests pass)
- Branch: `docs/s9-archive-doc-sync` (local commit, not pushed, not merged)
- Files modified: `README.md`, `ROADMAP.md`, `CONTEXT.md`, `docs/developer/pipeline-scripts.md`, `docs/developer/contributing.md`, `docs/developer/data-update-guide.md`, `AI_EDIT_LOG.md`
- Issues found: None new.
- Issues resolved: None (docs-only session).
- Next goal: **Session 10 — TBD.** Potential directions: data regeneration (PRICE-UNADJUSTED-001 + RANK-LEAKAGE-001), LIQUIDITY-001 design decision, or cross-sectional momentum feature gap.
- Branch flow: User merges `docs/s9-archive-doc-sync` into `main`, then Session 10 creates new branch from updated `main`

### Session-end checklist (Session 9)

- Docs updated? Yes. 6 files fixed for stale archived-file references.
- PIPELINE_ATLAS update needed? No. Already updated in Session 8 (files reclassified as ARCHIVED).
- PARQUET_ATLAS update needed? No. No schema/flow/mutation changes.
- KNOWN_ISSUES update needed? No. No new issues found. Docs-only session.
- AI_EDIT_LOG handoff updated? Yes.

---

## Session 9 — Docs Sync for Archived Legacy Files (2026-06-22)

**Branch:** `docs/s9-archive-doc-sync`

**Files modified:**
- `README.md` — Updated pipeline directory tree: removed stale `build_historical_dataset.py` and `fraud_signals.py` entries; replaced with current step-based pipeline (step1–step6 + support modules + archive/)
- `docs/developer/pipeline-scripts.md` — Added `⚠️ ARCHIVED (Session 8)` banners to 7 sections: value_metrics.py, fraud_signals.py, enrich_governance.py, enrich_insider_signals.py, enrich_market_signals.py, enrich_market_cap.py, build_historical_dataset.py, fetch_companies.py, market_cap_filter.py, auto_update.py. Content preserved for formula reference.
- `docs/developer/contributing.md` — Line 158: replaced `pipeline/build_historical_dataset.py` reference with `pipeline/phase_a_integrate_{market}.py` (current workflow)
- `docs/developer/data-update-guide.md` — Replaced `auto_update.py` reference with `scripts/refresh_data.py` + note about archive status
- `ROADMAP.md` — Marked `auto_update.py` and `enrich_governance.py` references as archived/deferred (not ✅ Done — features not actively implemented in current pipeline)
- `CONTEXT.md` — Updated `enrich_governance.py` path to `pipeline/archive/enrich_governance.py (ARCHIVED)` with migration note
- `AI_EDIT_LOG.md` — This session report + Session 10 handoff

**Grep coverage (files searched):**
- Searched all `.md`, `.yml`, `.yaml` files for full paths (`pipeline/fraud_signals.py`, etc.) and bare names (`fraud_signals.py`, etc.)
- Excluded: `pipeline/archive/` (archived files reference themselves), `PIPELINE_ATLAS.md` (already updated Session 8), `AI_EDIT_LOG.md` (historical records), `KNOWN_ISSUES.md` (already updated Session 8)
- `CHANGELOG.md` references are historical records — intentionally NOT modified

**Stale references found and fixed:**
| File | Stale Reference | Fix |
|---|---|---|
| `README.md:23` | `build_historical_dataset.py` in tree | Replaced with step-based pipeline tree |
| `README.md:24` | `fraud_signals.py` in tree | Replaced with step-based pipeline tree |
| `docs/developer/pipeline-scripts.md` | 10 sections for archived scripts | Added ARCHIVED banners (content preserved) |
| `docs/developer/contributing.md:158` | `pipeline/build_historical_dataset.py` | → `pipeline/phase_a_integrate_{market}.py` |
| `docs/developer/data-update-guide.md:149` | `auto_update.py` | → `scripts/refresh_data.py` + archive note |
| `ROADMAP.md:60` | `pipeline/enrich_governance.py` | Added (ARCHIVED) + migration note |
| `ROADMAP.md:97` | `pipeline/auto_update.py` as ✅ Done | → ⚠️ Archived + replacement note |
| `ROADMAP.md:116` | `pipeline/enrich_governance.py` as ✅ Done | → ⚠️ Archived + stranded logic note |
| `CONTEXT.md:95` | `pipeline/enrich_governance.py` | → `pipeline/archive/enrich_governance.py (ARCHIVED)` |

**NOT modified (intentional):**
- `CHANGELOG.md` — historical entries are records, not active references
- `PIPELINE_ATLAS.md` — already classified as ARCHIVED in Session 8
- `KNOWN_ISSUES.md` — no stale refs (BROKEN-IMPORT-001 already resolved)
- Python code — docs-only session
- Test files — docs-only session

**Test results:** 296 passed, 0 failed.

**No production code changes. No test changes. No parquet changes.**

---

## Session 8 — Legacy Archive / Call-Graph Proof (2026-06-22)

**Branch:** `refactor/s8-archive-legacy`

**Files created:**
- `pipeline/archive/STRANDED_LOGIC.md` — Documents features unique to archived files for future migration (insider signals, ADTV/volume, governance/going_concern)

**Files moved (git mv — history preserved):**
1. `pipeline/fraud_signals.py` → `pipeline/archive/fraud_signals.py`
2. `pipeline/fetch_companies.py` → `pipeline/archive/fetch_companies.py`
3. `pipeline/market_cap_filter.py` → `pipeline/archive/market_cap_filter.py`
4. `pipeline/value_metrics.py` → `pipeline/archive/value_metrics.py`
5. `pipeline/auto_update.py` → `pipeline/archive/auto_update.py`
6. `pipeline/enrich_governance.py` → `pipeline/archive/enrich_governance.py`
7. `pipeline/enrich_insider_signals.py` → `pipeline/archive/enrich_insider_signals.py`
8. `pipeline/enrich_market_cap.py` → `pipeline/archive/enrich_market_cap.py`
9. `pipeline/enrich_market_signals.py` → `pipeline/archive/enrich_market_signals.py`
10. `pipeline/build_historical_dataset.py` → `pipeline/archive/build_historical_dataset.py`

**Files modified:**
- `PIPELINE_ATLAS.md` — File Map (10 files reclassified ARCHIVED), Classification Key (added ARCHIVED, removed LEGACY_ARCHIVE_CANDIDATE), Call Graph (dead edges annotated)
- `KNOWN_ISSUES.md` — BROKEN-IMPORT-001 resolved
- `AI_EDIT_LOG.md` — this session report + Session 9 handoff

**Call-graph proof (grep results summary):**

| File | Imported By Active Code? | Imported By Other Legacy? | Classification |
|------|--------------------------|---------------------------|----------------|
| `fraud_signals.py` | NO | YES (build_hist, enrich_insider, enrich_mkt_cap, enrich_mkt_signals, auto_update) | Archive safe |
| `fetch_companies.py` | NO | YES (auto_update only) | Archive safe |
| `market_cap_filter.py` | NO | NO (self-contained) | Archive safe |
| `value_metrics.py` | NO | YES (fraud_signals, build_hist) | Archive safe |
| `auto_update.py` | NO | NO (string ref in run_phase_checks exclusion list only) | Archive safe |
| `enrich_governance.py` | NO | NO (docs/roadmap only) | Archive safe |
| `enrich_insider_signals.py` | NO | NO (comment in fraud_signals only) | Archive safe |
| `enrich_market_cap.py` | NO | NO (docs only) | Archive safe |
| `enrich_market_signals.py` | NO | NO (comment in fraud_signals + archived notebook) | Archive safe |
| `build_historical_dataset.py` | NO | NO (docs/contributing.md ref only) | Archive safe |

**Key observations:**
- `p0g_confidence_score.py` line 67 has `'fraud_signals'` as a dict key string (column group name), NOT an import of the module. Safe.
- `enrich_feature_dictionary.py` line 77 has `'step5_compute_features (fraud_signals.py)'` as a provenance string. NOT an import. Safe.
- `run_phase_checks.py` line 184 has `"auto_update.py"` in an `operator_only` exclusion set (string literal). NOT an import. Safe.
- `contributing.md` line 158 references `build_historical_dataset.py` in market integration docs — stale documentation to fix in Session 9.

**Test results:** 296 passed, 0 failed. Archive caused zero breakage.

**No production code changes to active pipeline files. No feature engineering. No parquet data changes.**

### Session-end checklist (Session 7)

- Atlas update needed? Yes. Updated Test Matrix status for p0g + enrich_fraud_labels (missing → ✅ covered), updated Coverage Summary.
- Parquet atlas update needed? No. No schema/flow/mutation changes. Both modules' mutations already documented.
- KNOWN_ISSUES update needed? No. Audit found no new issues.
- AI_EDIT_LOG handoff updated? Yes.

### Session 8 prompt (copy-paste into a fresh Claude Code conversation):

```
Start Session 8. Project: /Users/mhoque/Desktop/stock-fraud-screener-main

First, verify setup:
1. Run git status — confirm clean working tree on main
2. Confirm current branch is main
3. Create and checkout branch: git checkout -b refactor/s8-archive-legacy

Then read these files to understand the codebase state:
- PIPELINE_ATLAS.md (file map, call graph, test matrix)
- PARQUET_ATLAS.md (parquet file registry, mutation order)
- KNOWN_ISSUES.md (known issues, triaged)
- AI_EDIT_LOG.md (session history and handoff)

Session 8 goal: Legacy Archive / Call-Graph Proof.

Prove LEGACY_ARCHIVE_CANDIDATE files are NOT used by:
- current Step 1–7 pipeline (run_pipeline.py)
- ML training (train_models.py)
- OOF scoring (generate_oof_scores.py)
- backtester (backtester.py)
- portfolio builder (build_portfolio.py)
- app/dashboard (app_v2.py, api/)
- CI (.github/workflows/)
- docs/handoff scripts

10 candidate legacy files:
- pipeline/fraud_signals.py
- pipeline/fetch_companies.py
- pipeline/market_cap_filter.py
- pipeline/value_metrics.py
- pipeline/auto_update.py
- pipeline/enrich_governance.py
- pipeline/enrich_insider_signals.py
- pipeline/enrich_market_cap.py
- pipeline/enrich_market_signals.py
- pipeline/build_historical_dataset.py

For each file:
1. grep -rn for all imports/references across the repo (exclude __pycache__, .git)
2. Classify: truly unused / imported by another legacy file only / imported by active code
3. If useful stranded logic exists (insider signals, governance, ADTV/liquidity, fraud formulas), note it in a STRANDED_LOGIC.md or comments before archiving
4. If a file is imported by active code, do NOT archive it — flag for future refactor instead

Archive process:
- Create pipeline/archive/ folder
- Use git mv (not plain mv) to preserve file history
- Move confirmed-unused files there
- Do NOT delete anything

After archiving:
- Run: python3 -m pytest tests/ -q
- Confirm no tests broke from the move (import paths, test discovery)
- If tests break, fix the import or do not archive that file

Update:
- PIPELINE_ATLAS.md — file map (reclassify as ARCHIVED), call graph (remove dead edges), classification key (add ARCHIVED)
- PARQUET_ATLAS.md — only if a reader/mutator entry references an archived file
- KNOWN_ISSUES.md — resolve BROKEN-IMPORT-001 if all three broken-import files are archived
- AI_EDIT_LOG.md — Session 8 report + Session 9 handoff

Rules:
- No production code changes to active pipeline files
- No feature engineering
- No parquet data changes
- Use git mv for all moves
- Run full test suite after archive to catch breakage
- Do not push. Do not merge.
- If a file IS used by active code (not just other legacy files), STOP and report — do not archive it.

Session-end checklist:
- Atlas update needed? Yes/No. Reason:
- Parquet atlas update needed? Yes/No. Reason:
- KNOWN_ISSUES update needed? Yes/No. Reason:
- AI_EDIT_LOG handoff updated? Yes/No.

Commit locally at end. Do not push. Do not merge.
```
