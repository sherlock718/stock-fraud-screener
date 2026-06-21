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
| 4.8 | Recession look-ahead | ✅ | USREC via FRED is real-time (changes when NBER announces). PIT-safe |
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

First, verify setup:
1. Run git status — confirm clean working tree
2. Confirm current branch is main
3. Create and checkout branch: git checkout -b refactor/s4-audit-step5

Then read these files to understand the codebase state:
- PIPELINE_ATLAS.md (file map, call graph, test matrix, step audit checklist)
- PARQUET_ATLAS.md (parquet file registry, mutation order)
- KNOWN_ISSUES.md (logged issues from Sessions 0–3)
- AI_EDIT_LOG.md (session history and handoff)

Session 4 goal: Audit Step 5 (compute features), then add minimal critical tests for Step 5 only.

Scope:
- Audit Step 5 (compute features): temporal leakage (no forward_return_* used as feature), label leakage (no fraud_confirmed/ml_* used), rank leakage (cross-sectional ranks must groupby fiscal_year), rolling features past-only, formula spot-checks, winsorization applied to ratio_cols
- Inspect pipeline/step5_compute_features.py thoroughly (972 lines — focus on sections A-H)
- Write minimal critical tests for Step 5 only (synthetic data, no network calls)
- Tests must validate: no forward/label/rank leakage, formula correctness (spot-check 5-10 key features), winsorization bounds, output shape contract

Rules:
- No broad refactor
- No feature engineering
- No archive/move/delete of pipeline files
- If a critical issue is found during audit, classify it, propose the smallest fix, and wait for approval before changing production code
- Step 6 is NOT in scope — that belongs to Session 5

At session end, update:
- KNOWN_ISSUES.md — add any new issues found during audit
- PIPELINE_ATLAS.md — update Test Matrix status column if tests are added
- AI_EDIT_LOG.md — add Session 4 report + Session 5 handoff with full prompt

Before ending the session, check whether PIPELINE_ATLAS.md, PARQUET_ATLAS.md, KNOWN_ISSUES.md, or AI_EDIT_LOG.md needs updating. If not, say why.

Session-end checklist:
- Atlas update needed? Yes/No. Reason:
- Parquet atlas update needed? Yes/No. Reason:
- KNOWN_ISSUES update needed? Yes/No. Reason:
- AI_EDIT_LOG handoff updated? Yes/No.

Commit locally at end. Do not push. Do not merge.
```
