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

## Next Claude Session Handoff

- Status: Session 0 complete
- Branch: refactor/s0-guardrails (local commit `cf1dd58`, not pushed, not merged)
- Files changed: `scripts/hooks/pre_commit_guard.py`, `KNOWN_ISSUES.md`, `AI_EDIT_LOG.md` (committed); `.claude/settings.json` (local only, gitignored)
- Guardrails active: pre-commit hook (warn >5 files, warn step* without test, block parquet), read-only bash permissions in `.claude/settings.json`
- Next goal: Session 1 — Map all pipeline files, build PIPELINE_ATLAS.md and PARQUET_ATLAS.md. No test files created. No production code changes.
- Branch flow: User will merge `refactor/s0-guardrails` into local `main` before starting Session 1. Session 1 creates `refactor/s1-atlas` from updated `main`.

### Next prompt (copy-paste into a fresh Claude Code conversation):

```
Start Session 1. Project: /Users/mhoque/Desktop/stock-fraud-screener-main

First, verify setup:
1. Run git status — confirm clean working tree
2. Confirm current branch is main (user merged s0 into main already)
3. Confirm AI_EDIT_LOG.md and KNOWN_ISSUES.md exist
4. Create and checkout branch: git checkout -b refactor/s1-atlas

Then execute Session 1 goal:

Create PIPELINE_ATLAS.md and PARQUET_ATLAS.md. Use Caveman style (short, direct, practical).

PIPELINE_ATLAS.md must contain these sections:

1. File Map — every pipeline/ file classified as:
   CURRENT_CORE / CURRENT_SUPPORT / OPTIONAL_REPORT_OR_QA / LEGACY_ARCHIVE_CANDIDATE / UNKNOWN_INVESTIGATE

2. Call Graph — who imports who. Grep across pipeline/, scripts/, notebooks/, tests/. Show which scripts call which pipeline modules.

3. Test Matrix — columns: source file | role | critical risks | required tests | planned test file | priority | existing coverage | status
   Inspect existing tests/ folder first. Mark each as: already covered / partially covered / missing / duplicate risk.

4. Step Audit Checklist — include the full checklist:
   - Step 1: yfinance tickers, market codes, exchanges, inactive/delisted, duplicates, identifiers, US suffix, survivorship bias
   - Step 2: one row per ticker×fiscal_year×period_type, period_end, filed_date, filed_date>=period_end, currencies, coverage, units, annual/quarterly, non-US schema
   - Step 3: entry price after filed_date, prior momentum past-only, forward returns future-only, adjusted/unadjusted, missing prices survivorship, local benchmarks, FX returns, US-only backtest check
   - Step 4: schema, row count, no future macro, macro_asof_date, derived formulas, fill rates, same-date consistency, recession look-ahead, US vs local macro
   - FX: local vs USD returns, entry/exit FX dates, historical FX not current, missing 6m/2y FX, GitHub refresh FX, train/backtest mix
   - Step 5: formula correctness, temporal leakage, label leakage, rank leakage, rolling past-only, coverage, value sanity, cross-market, dataset shape, Step 5 sole authority
   - Step 6: non-null required cols, no duplicates, no infinities, as_of_date==filed_date, filing_lag_days, no accidental filtering, row count stable
   - Step 7: quarterly excluded, fiscal year cutoff, incomplete year excluded, default vs --apply-filters, revenue/assets/price filters, OTC floor, SIC exclusions, missing data behavior, in_universe meaning, currency, downstream respect
   - Liquidity: where liquidity data exists, ADTV PIT safety, train/backtest/portfolio access, volume filter missing, bid-ask missing, enrich_market_signals connected?, ADTV feature vs backtest-only, market cap inconsistency

5. Free Public Feature Coverage Matrix — audit 16 feature families:
   Table columns: Feature/family | Exists? | File/source | Current layer | Correct layer | PIT timestamp available? | Free public source? | Should pipeline own it? | Priority | Action
   
   Layer options: raw_ingestion / cleaned_base_dataset / base_pipeline_feature / factor_alpha_layer / model_feature_layer / portfolio_execution_layer / side_script / legacy / missing / park
   Actions: keep / audit / add later / park / archive legacy
   
   Families: (1) Filing fundamentals (2) Value factors (3) Quality/profitability (4) Growth (5) Momentum/price (6) Liquidity/execution (7) Fraud/manipulation (8) Insider/governance (9) Institutional ownership (10) Short interest (11) Corporate actions (12) Balance sheet/distress (13) Macro/rates/FX (14) Filing text (15) Analyst data (16) Explicitly excluded
   
   After the matrix, add sections:
   - High-Priority Missing Free Features
   - Features Present Only in Side Scripts
   - Legacy Features Worth Preserving Before Archive
   - Features to Park Because Not Free/Direct/PIT-Safe

   Rules: Do not implement features. Do not add data sources. Do not move logic. Do not rewrite Step 5. Classify only. pipeline/ = PIT data factory, not alpha/model/portfolio layer.

PARQUET_ATLAS.md must contain:
- Every parquet file referenced in pipeline/ and scripts/
- For each: who creates, who reads, who overwrites/mutates, key columns, unique keys, intermediate vs final

Rules:
- No test files created
- No production code changes
- Code-reading and grep only
- Commit locally at end (no push, no merge)
- Update AI_EDIT_LOG.md with Session 1 report and Session 2 handoff
```
