# Session Plan

Persistent session-by-session plan. Each session prompt should reference this file:
`/Users/mhoque/Desktop/stock-fraud-screener-main/SESSION_PLAN.md`

---

## Completed Sessions

| # | What | Commit | Date |
|---|------|--------|------|
| 19 | Full OOS backtest — Composite Sharpe 1.37 | 049fc1f | 2026-06-26 |
| 20 | Archive purge — 122 dead files removed | 4922403 | 2026-06-26 |
| 21 | Feature ablation — 1 load-bearing, 16 prune candidates | 5c773df | 2026-06-26 |
| 22 | Proper train/val/test split — unbiased Sharpe 0.954, GATE PASS | 97d1c97 | 2026-06-26 |
| 23 | Pruned feature set backtest — 27 features, Sharpe 1.124 | e6ead61 | 2026-06-26 |
| 24 | Agreement filter (LightGBM+Tree, t=0.35) — Sharpe 1.138, CAGR +34% | bae0eba | 2026-06-26 |
| 25 | Regime overlay (SPY DD >15% = risk-off) — insurance-only, dormant in test | fd9116b | 2026-06-26 |
| 26 | IC code consolidation — factor_research.py now calls ic_engine.py | 1e512ac | 2026-06-26 |
| 27 | Pipeline spine orientation — map + lineage + risks + backlog | e807af3 | 2026-06-26 |
| 28 | Modeling + alpha orientation — scoring, OOF, alpha factors, backlog | e367b63 | 2026-06-26 |
| 29 | Research + backtest orientation — engine internals, validation chain, risk register | 673426e | 2026-06-28 |
| 30 | Quality + orchestration orientation — final map, consolidated risks, test gaps | 57bd3eb | 2026-06-28 |
| 31 | Bug fixes — undefined BASE in BR pipeline + score_oof, delete dead JP file | 1ecbdd6 | 2026-06-28 |
| 32 | Unit tests for quality/ scripts — 41 tests covering check_data + dataset_quality | d874e10 | 2026-06-28 |
| 33 | Unit tests for alpha/ + backtest/ — 43 tests covering factors + WF engine | a4f6aff | 2026-06-28 |
| 34 | Unify feature sets (27 canonical) + model staleness check + tests for research/portfolio | 3c01a46 | 2026-06-28 |
| 35 | Consolidate EXCLUDE sets + load_data → modeling/constants.py | cb11c9d | 2026-06-28 |
| 36 | Honest survivorship imputation + filing-date rebalance gate | 0657f3d | 2026-06-28 |
| 37 | Alpha IC validation (all 5 pass) + fix non-US benchmark (ACWI ex-US) | 10bb452 | 2026-06-28 |
| 38 | ml_gates mode: ML-only ranking + WF tree agreement + hard gates (Sharpe 1.204 vs 0.937) | 583bbb7 | 2026-06-28 |
| 39 | Expand val to 2021-2023 + restrict feature selection to train-only | 3c9e691 | 2026-06-28 |
| 40 | Retrain decision tree (2008-2020) + LightGBM production models | b490f57 | 2026-06-28 |
| 41 | Parameterize ADTV by AUM ($200K retail) + FAQ quick-reference | 6b618f7 | 2026-06-28 |
| 42 | Clean model retrain + ROA gate + top 15 balanced config (OOS CAGR +25.9%, Sharpe 1.08) | b68b2e8 | 2026-06-28 |
| 43 | Production screener + regression ranking + $10B cap (CAGR +34.7%, Sharpe 0.97) | 7773ce0 | 2026-06-28 |
| 44 | Persist regression model as artifact — notebook loads instead of retraining | 1807f57 | 2026-06-28 |
| 45 | Centralize production thresholds + Altman Z gate in engine (CAGR +33.9%, Sharpe 1.115) | c303b1b | 2026-06-28 |

---

## GATE: Session 22 — RESOLVED

**Result: PASS (Sharpe 0.954 ≥ 0.8)**

Split: Train 2008-2014 / Validate 2015-2018 / Test 2019-2024
- Test CAGR: +31.9% vs SPY +17.1% (excess +14.8%)
- Feature stability: 50% Jaccard overlap across shifted train windows (27 stable features)
- Previous biased Sharpe (1.37) was ~44% overstated; honest Sharpe 0.954 still clears gate
- **Proceed to sessions 23-25** (prune features, explainable model, regime overlay)

---

## Upcoming Sessions

### Session 22: Proper Train/Validate/Test Split ✓ DONE

**Result:** Sharpe 0.954 on test period (2019-2024). Gate PASS.

**What was done:**
1. Temporal 3-way split: Train (2008-2014), Validate (2015-2018), Test (2019-2024)
2. Feature selection confined to TRAIN period only — 43 features survived
3. Walk-forward ML on test period using only train-selected features
4. Stability check: shifted window (2010-2016) → 50% feature overlap (27 stable)
5. Honest Sharpe: 0.954 vs biased 1.37 (44% overstatement corrected)

**Key output:** `reports/proper_split_results.md`, `reports/proper_split_results.json`
**Script:** `research/proper_split_backtest.py`

---

### Session 23: Pruned Feature Set Backtest ✓ DONE

**Result:** Lean 27-feature model Sharpe **1.124** (BETTER than full 43-feature 0.954). No add-back needed.

**What was done:**
1. Took 27 temporally stable features (survived both 2008-2014 and 2010-2016 train windows)
2. Re-ran walk-forward backtest on test period 2019-2024 with only these 27 features
3. Sharpe improved +0.17 vs full model — removing 16 noisy features reduced overfitting
4. CAGR +33.8% vs SPY +17.1% (excess +16.7%), hit rate 73.9%
5. No add-back triggered (lean model strictly better)

**Key output:** `reports/pruned_backtest_results.md`, `models/feature_sets_pruned.json`
**Script:** `research/pruned_backtest.py`

---

### Session 24: Explainable Decision Tree + Agreement Filter ✓ DONE

**Result:** Agreement filter (tree_prob ≥ 0.35) Sharpe **1.138**, CAGR **+34.0%**, 0% max DD.

**What was done:**
1. Trained depth-4 decision tree on 27 pruned features, walk-forward backtest 2019-2024
2. Extracted 5 human-readable BUY rules (IF/THEN conditions with thresholds)
3. Threshold sweep (0.30–0.50) for agreement filter:
   - 0.35: Sharpe 1.138, CAGR +34.0% ← selected (natural plateau, best CAGR)
   - 0.40: Sharpe 1.072, CAGR +33.2%
   - 0.50: Sharpe 1.536, CAGR +25.9% (too restrictive)
4. Decision: Agreement filter (0.35) = primary. LightGBM ranks, tree gates.
5. Beats LightGBM-only CAGR (+33.8%) AND adds explainability for every pick

**Key output:** `models/decision_tree_rules.json`, `reports/explainable_model_results.md`
**Script:** `research/explainable_tree.py`

---

### Session 25: Regime Overlay (Macro Signal) ✓ DONE

**Result:** Insurance-only overlay. Dormant in test period (base already 0% max DD). Cost: -2.25pp CAGR when it triggers in benign conditions. Value: protects against 2008-style crashes in deployment.

**What was done:**
1. Implemented SPY trailing drawdown > 15% from peak = "risk-off" signal
2. Risk-off action: reduce position size by 50% (hold 50% cash)
3. Regime triggered: 2009, 2010 (post-2008 crash), 2023 (post-2022 bear)
4. Overlay vs base (2019-2024): Sharpe 1.001 vs 1.138, CAGR +31.8% vs +34.0%, Max DD 0% vs 0%
5. Decision: ADOPT as insurance layer for deployment — no impact during normal markets

**Key output:** `reports/regime_overlay_results.md`, `reports/regime_overlay_results.json`
**Script:** `research/regime_overlay.py`

---

### Session 26: IC Code Consolidation ✓ DONE

**Result:** factor_research.py now delegates to ic_engine.compute_yearly_ic(). 36 lines removed, single source of truth.

---

## Orientation Phase (Sessions 27–30) ✓ COMPLETE

> **Rule: Do not refactor. Do not rewrite code.** These sessions are for mapping, lineage, and risk discovery only.
>
> **Status: ALL 4 SESSIONS COMPLETE.** Full architecture mapped. Consolidated risk register and priority backlog in `docs/architecture/BACKLOG.md`. Ready for execution phase.

Each session produces:
1. **What we covered** — summary of files read, architecture mapped, risks found
2. **Quiz** — 5-8 short questions to confirm your understanding
3. **Your backlog inputs** — after quiz, you add Critical / Parked items to `docs/architecture/BACKLOG.md`
4. **Next-session prompt** — self-contained, ready to paste

Backlog file: `docs/architecture/BACKLOG.md` — accumulates across sessions 27-30, becomes the post-orientation roadmap.

---

### Session 27: Pipeline Spine (raw data → clean dataset) ✓ DONE

**Result:** Full pipeline map, column lineage, risk register produced. Quiz score 3/10 (concepts OK, specifics need work). Backlog: ADTV addition, FAQ file, FX handling parked.

**Output:** `docs/architecture/orientation_pipeline_spine.md`, `docs/architecture/BACKLOG.md`

**Focus files:**
- `pipeline/step1_fetch_sec.py` — raw SEC EDGAR fetch
- `pipeline/step2_build_snapshots.py` — company-year snapshots
- `pipeline/step3_enrich_prices.py` — market cap, returns, price data
- `pipeline/step4_enrich_macro.py` — macro features (rates, indices)
- `pipeline/step5_compute_features.py` — ratio/growth feature generation
- `pipeline/step6_clean.py` — final cleaning, dedup, validation
- `pipeline/feature_library/` — shared feature definitions
- `_root.py` — path resolution

**Deliverables:**
1. End-to-end pipeline map (inputs → intermediates → output parquet)
2. Column lineage (key columns: where created, where consumed, any unclear/duplicated)
3. Risk register for pipeline (fragile assumptions, lookahead risks, naming issues)
4. Refactor candidates (archive/merge/rename suggestions — not executed)

**Output file:** `docs/architecture/orientation_pipeline_spine.md`

**Prompt:**
```
Session 27: Pipeline Spine Orientation

Read SESSION_PLAN.md first (repo root). This is an orientation session — NO refactoring, NO rewrites.

We are entering an orientation phase before any new feature work. Goal: understand the current pipeline spine deeply before improving architecture.

Important rule: Do not refactor. Do not rewrite code unless a tiny fix is required to run tests or inspect behavior. This session is for mapping, lineage, and risk discovery.

Inspect the pipeline files (step1 through step6, feature_library/, _root.py) and produce:

1. End-to-end pipeline map: raw inputs → intermediate datasets → feature generation → target/label creation → final output parquet

2. Column lineage: key columns created in each step, which files create them, which files consume them, any unclear or duplicated columns

3. Risk register: outdated docs, duplicated logic, fragile assumptions, lookahead/survivorship risks, naming inconsistencies, dead code

4. Refactor candidates: what should stay, what should be archived, what should be merged, what should be renamed — DO NOT EXECUTE any of these

5. Save output to docs/architecture/orientation_pipeline_spine.md

6. End with: quiz (5-8 questions to test my understanding), then the prompt for session 28.
```

---

### Session 28: Modeling + Alpha Scoring (features → signal)

**Focus files:**
- `modeling/train.py` — LightGBM training, walk-forward
- `modeling/score.py` — scoring pipeline
- `modeling/feature_selection.py` — feature importance / selection
- `alpha/factors/value.py`, `quality.py`, `momentum.py`, `growth.py`, `fraud_risk.py`
- `alpha/factors/composite.py` — weight blending
- `alpha/factors/__init__.py` — registry
- `models/model_meta.json`, `models/feature_sets_pruned.json`

**Deliverables:**
1. Model architecture map (what model, what features, what target, what split)
2. Alpha factor composition (weights, interactions, overrides)
3. Column lineage continued (model input features → alpha scores → composite)
4. Risk register (overfitting vectors, stale model artifacts, weight assumptions)
5. Refactor candidates

**Output file:** `docs/architecture/orientation_modeling_alpha.md`

**Prompt:**
```
Session 28: Modeling + Alpha Orientation

Read SESSION_PLAN.md first. This is an orientation session — NO refactoring.

Continue orientation phase. Focus: how features become a buy/sell signal.

Inspect modeling/ and alpha/ packages. Produce:

1. Model architecture map: what model type, input features, target variable, train/test split mechanics, walk-forward logic

2. Alpha factor composition: each factor's role, weight in composite, how they interact

3. Column lineage (continued from session 27): model input features → predicted scores → alpha factors → composite score

4. Risk register: overfitting vectors, stale model artifacts, weight assumptions, hardcoded thresholds

5. Refactor candidates (DO NOT EXECUTE)

6. Save output to docs/architecture/orientation_modeling_alpha.md

7. End with: quiz (5-8 specific questions), then ask me for backlog inputs (Critical / Parked items to add to docs/architecture/BACKLOG.md), then prompt for session 29.
```

---

### Session 29: Research + Backtest Engine (validation machinery)

**Focus files:**
- `backtest/engine.py` — walk-forward backtest core
- `research/proper_split_backtest.py` — the GATE script
- `research/pruned_backtest.py` — pruned feature backtest
- `research/explainable_tree.py` — decision tree + agreement filter
- `research/regime_overlay.py` — SPY drawdown overlay
- `research/ic_engine.py` — IC calculations (source of truth)
- `research/factor_research.py` — factor screening
- `research/feature_selection_engine.py` — automated selection

**Deliverables:**
1. Backtest engine internals (walk-forward mechanics, rebalance logic, return calculation)
2. Research pipeline map (which script depends on which, execution order)
3. Signal validation chain (IC → feature selection → backtest → agreement → regime)
4. Risk register (backtest bias, implicit assumptions, fragile paths)
5. Refactor candidates

**Output file:** `docs/architecture/orientation_research_backtest.md`

**Prompt:**
```
Session 29: Research + Backtest Orientation

Read SESSION_PLAN.md first. This is an orientation session — NO refactoring.

Continue orientation phase. Focus: how we validate that the signal works.

Inspect backtest/ and research/ packages. Produce:

1. Backtest engine internals: walk-forward mechanics, rebalance frequency, return calculation, position sizing

2. Research pipeline map: which script depends on which, execution order, shared state

3. Signal validation chain: IC analysis → feature selection → backtest → agreement filter → regime overlay

4. Risk register: backtest bias vectors, implicit assumptions, fragile paths, hardcoded thresholds

5. Refactor candidates (DO NOT EXECUTE)

6. Save output to docs/architecture/orientation_research_backtest.md

7. End with: quiz (5-8 questions), then prompt for session 30.
```

---

### Session 30: Support Packages (fraud, quality, portfolio, data_io, workflows)

**Focus files:**
- `fraud/` — fraud-specific rules, features, taxonomy
- `quality/` — bias audit, PIT validation, sync checker, dataset quality
- `portfolio/` — screener registry, portfolio construction
- `data_io/` — HuggingFace push/pull, AAER labels, merge
- `workflows/` — orchestration scripts

**Deliverables:**
1. Support package map (what each does, how it connects to the spine)
2. Quality gate inventory (what checks exist, what's enforced vs advisory)
3. Data I/O flows (what goes to/from HuggingFace, what's local-only)
4. Risk register (dead workflows, unchecked quality paths, stale fraud rules)
5. Refactor candidates
6. **Final synthesis:** consolidated architecture diagram across all 4 sessions

**Output file:** `docs/architecture/orientation_support_packages.md`

**Prompt:**
```
Session 30: Support Packages Orientation

Read SESSION_PLAN.md first. This is an orientation session — NO refactoring.

Final orientation session. Focus: everything outside the main spine (fraud, quality, portfolio, data_io, workflows).

Inspect fraud/, quality/, portfolio/, data_io/, workflows/. Produce:

1. Support package map: what each package does, how it connects to the pipeline/modeling/research spine

2. Quality gate inventory: what checks exist, what's enforced vs advisory, coverage gaps

3. Data I/O flows: what goes to/from HuggingFace, what's local-only, sync state

4. Risk register: dead workflows, unchecked quality paths, stale fraud rules, orphaned scripts

5. Refactor candidates (DO NOT EXECUTE)

6. Final synthesis: consolidated architecture diagram linking all 4 orientation sessions

7. Save output to docs/architecture/orientation_support_packages.md

8. End with: quiz (5-8 questions), then recommendation for session 31+ (what to build/fix first based on everything learned).
```

---

## Execution Phase (Sessions 31–41)

> **Rule: One commit per session. Run pytest after changes. Push to remote.**
> Backlog reference: `docs/architecture/BACKLOG.md`

### Session 31: Trivial Bug Fixes

**Items:** Critical #15 (BASE in run_pipeline_br.py), Parked #9 (BASE in score_oof.py), Parked #25 (archive dead jp_free file)

**Scope:** 3 one-line fixes + 1 file deletion. No logic changes.

**Prompt:**
```
Session 31: Trivial Bug Fixes

Read SESSION_PLAN.md first. Execution session.

Backlog items: Critical #15, Parked #9, Parked #25

Scope — fix 3 bugs (all trivial, no logic changes):
1. workflows/run_pipeline_br.py — uses BASE before `from _root import ROOT`. Move ROOT import + BASE assignment above first use of BASE.
2. modeling/score_oof.py — line 44 uses BASE before line 52 defines it. Move `BASE = ROOT` above `DATA_PATH`.
3. Delete pipeline/step1_fetch_tickers_jp_free.py if it exists (dead file, unused variant).

Working protocol:
- Token-optimized: read only the lines needed (offset/limit), no full-file dumps
- Caveman approach: minimal fix, no refactoring beyond the bug
- One commit, conventional message: `fix(pipeline,modeling): fix undefined BASE + archive dead file`

Session close checklist:
- Run `pytest tests/ -x` — confirm nothing broke
- Commit + push to remote
- Update SESSION_PLAN.md completed table (row: session 31, commit hash, date)
- Move Critical #15, Parked #9, Parked #25 → BACKLOG.md Completed section
- Write session 32 prompt (self-contained, copy-paste ready) at end of response
```

---

### Session 32: Unit Tests for quality/ Scripts ✓ DONE

**Items:** ~~Critical #13~~

**Scope:** Write tests for `check_data.py` and `test_dataset_quality.py` using synthetic in-memory data (same pattern as `tests/test_pipeline.py`).

**Prompt:**
```
Session 32: Unit Tests for Quality Scripts

Read SESSION_PLAN.md first. Execution session — add test coverage for CI gates.

quality/check_data.py and quality/test_dataset_quality.py are MANDATORY CI gates with zero unit tests.
Write tests/test_quality.py following the existing pattern in tests/test_pipeline.py (synthetic in-memory data, no disk I/O).

Test strategy:
- Build a minimal synthetic DataFrame matching pipeline schema (~30 rows)
- Test that passing data passes all checks (happy path)
- Test that broken data (nulls, out-of-range, missing columns) fails correctly
- Test edge cases: empty dataframe, single row, all-NaN column

Target: 10-15 test functions covering the most critical check categories (schema, structural, value ranges, forward return caps, growth winsorization).

After: run `pytest tests/ -x`. Commit as `test(quality): add unit tests for CI gate scripts`. Push.
```

---

### Session 33: Unit Tests for Alpha + Backtest

**Items:** Critical #14

**Scope:** Write tests for `alpha/factors/*.py` (compute functions) and `backtest/engine.py` (core WF logic).

**Prompt:**
```
Session 33: Unit Tests for Alpha Factors + Backtest Engine

Read SESSION_PLAN.md first. Execution session — test coverage for core scoring logic.

Write tests/test_alpha_factors.py and tests/test_backtest_engine.py.

Alpha tests (tests/test_alpha_factors.py):
- Each factor's compute() function with synthetic data
- Verify output is Series of float in [0,1] range (percentile ranks)
- Verify NaN handling (all-NaN input → NaN output, partial NaN → still computes)
- Verify composite weights sum to 1.0

Backtest tests (tests/test_backtest_engine.py):
- Test run_backtest() with synthetic 5-ticker, 5-year data
- Verify annual returns are computed correctly
- Verify position sizing respects MAX_POSITION_WEIGHT
- Verify sector cap respects MAX_SECTOR_WEIGHT
- Test edge case: fewer stocks than top_n

Use synthetic data only (same pattern as test_pipeline.py). After: pytest, commit as `test(alpha,backtest): add unit tests for factor computation and WF engine`. Push.
```

---

### Session 34: Unify Feature Sets + Model Staleness Check

**Items:** Critical #3 + #5

**Scope:** Pick 27-feature pruned set as canonical. Add CI staleness warning.

**Prompt:**
```
Session 34: Unify Feature Sets + Model Staleness Check

Read SESSION_PLAN.md first. Execution session.

Part 1 — Unify feature sets:
- Read models/feature_sets_pruned.json (27 features, Sharpe 1.124) and models/feature_sets_3y.json (45 features, Sharpe 0.954)
- The 27-feature pruned set is CANONICAL (proven better). Make score_oof.py, train.py, and any other consumer load from feature_sets_pruned.json for the 3y horizon.
- For other horizons (1y, 5y): keep their existing feature_sets_{h}.json unchanged.
- Document the decision in a comment at the load site.

Part 2 — Model staleness check:
- Create quality/check_model_staleness.py: compares mtime of models/model_meta.json vs data/historical_dataset_clean.parquet. Warns (exit 0) if model is older than data. Fails (exit 1) with --strict.
- Add to refresh_data.yml as a non-fatal step after ML scoring.

After: pytest, commit as `feat(modeling): unify feature sets (27 canonical) + add staleness check`. Push.
```

---

### Session 35: Code Consolidation (EXCLUDE sets + load_data)

**Items:** Parked #14 (EXCLUDE only), Parked #23 (load_data)

**Scope:** Consolidate the two duplications that have real leakage/drift risk. Skip abstractions a solo dev doesn't need (WF loop extraction, _sic_to_sector — Claude can update both copies inline).

**Prompt:**
```
Session 35: Code Consolidation

Read SESSION_PLAN.md first. Execution session — consolidate code with real drift risk.

Two tasks (skip abstractions that don't help a solo maintainer):

1. EXCLUDE_COLS/PATTERNS → modeling/constants.py
   - Currently defined in backtest/engine.py, research/factor_research.py, modeling/train.py
   - Create modeling/constants.py with canonical EXCLUDE_COLS and EXCLUDE_PATTERNS
   - Update all 3 consumers to import from there
   - WHY: if these diverge, features leak into ML training = contaminated model

2. Consolidate load_data() → modeling/constants.py (or modeling/data_loader.py)
   - Duplicated across train.py, score_oof.py, run_feature_selection.py with minor differences
   - Single shared function prevents silent drift in data loading logic

DO NOT extract: walk-forward loop (4 copies work fine, Claude updates all at once), _sic_to_sector (15 lines in 2 files, trivial to keep in sync).

After: pytest, commit as `refactor(modeling): consolidate EXCLUDE sets + load_data`. Push.
```

---

### Session 36: Survivorship Fix + Filing-Date Rebalance

**Items:** Critical #9 + #11

**Scope:** Change backtest defaults to honest behavior.

**Prompt:**
```
Session 36: Survivorship Fix + Filing-Date Rebalance Timing

Read SESSION_PLAN.md first. Execution session — backtest correctness.

Part 1 — Survivorship (backtest/engine.py):
- Change default behavior: instead of DROP rows with missing forward_return_1y, IMPUTE -50% return for stocks that disappear from the dataset next year.
- Add parameter `survivorship_mode='impute'` (options: 'impute', 'drop', 'flag_only')
- Default = 'impute'. The -50% value already exists as `fill_missing_return` param.

Part 2 — Filing-date rebalance (backtest/engine.py):
- Current: assumes all filings available Jan 1 of holding year
- Fix: only include stocks whose filed_date < holding_year Jan 1 in that year's portfolio
- Stocks filing later become eligible in the NEXT year's portfolio
- This means: filter `df[df['filed_date'] < f'{holding_year}-01-01']` before scoring

After BOTH changes: re-run the walk-forward backtest to get updated Sharpe. Report new vs old metrics in commit message. pytest, commit as `fix(backtest): honest survivorship imputation + filing-date rebalance gate`. Push.
```

---

### Session 37: Alpha IC Validation + Non-US Benchmark

**Items:** Critical #6 + #10

**Scope:** Validate each alpha factor predicts returns; add proper benchmark for iarb.

**Prompt:**
```
Session 37: Alpha Factor IC Validation + Non-US Benchmark

Read SESSION_PLAN.md first. Execution session.

Part 1 — Alpha factor IC validation:
- For each of the 5 alpha factors (value, quality, momentum, growth, fraud_risk): compute cross-sectional Spearman IC against forward_return_1y, yearly, then report mean IC and ICIR.
- Use research/ic_engine.py (already has the machinery).
- Save results to reports/alpha_factor_ic.csv
- If any factor has |mean IC| < 0.02: flag it in the report. Do NOT auto-remove — just report.

Part 2 — Non-US benchmark:
- In backtest/engine.py, add benchmark option: 'spy' (default for US), 'acwi' (for non-US/iarb)
- Create data_io/fetch_acwi_returns.py to download ACWI ETF (ticker: ACWI) annual returns via yfinance
- Update iarb strategy filter to use acwi benchmark when market != 'US'

After: pytest, commit as `feat(alpha,backtest): IC validation per factor + ACWI benchmark for non-US`. Push.
```

---

### Session 38: Remove Composite Weight Blend → ML-Only + Gates

**Items:** Critical #12

**Scope:** Add a `mode='ml_gates'` parameter to existing filter_composite(). No new files.

**Prompt:**
```
Session 38: Simplify Strategy — ML-Only Ranking + Hard Gates

Read SESSION_PLAN.md first. Execution session — architecture simplification.

Current: filter_composite() manually blends value/quality/ML/momentum/fraud_risk at hand-picked weights (25/20/30/15/10).
Problem: ML already learned the optimal blend. Manual weights compete with learned weights.

New architecture (mode='ml_gates'):
- Ranking signal: ml_3y_oof probability (single number)
- Agreement filter: tree_prob >= 0.35 (existing, kept)
- Hard gates only: Beneish M < -1.78, market_cap >= MIN, not delisted, Piotroski >= 3
- No soft score blending

Implementation:
1. Add `mode` parameter to filter_composite() in backtest/engine.py: 'blended' (current default) vs 'ml_gates' (new)
2. When mode='ml_gates': skip weight blending, rank by ml_3y_oof only, apply hard gates
3. Run walk-forward backtest with mode='ml_gates'
4. Report Sharpe/CAGR/hit_rate vs mode='blended' in commit message

NO new files. Just a mode param on the existing function.

After: pytest, commit as `feat(backtest): add ml_gates mode to filter_composite (no manual weight blend)`. Push.
```

---

### Session 39: Expand Validation Set + Fix Feature Selection Leakage

**Items:** Critical #7 + #8

**Scope:** Widen val window (3 years). Fix feature selection script to exclude test data (one-line filter, not full WF architecture — session 22 already proved stability).

**Prompt:**
```
Session 39: Expand Validation Set + Fix Feature Selection Leakage

Read SESSION_PLAN.md first. Execution session.

Part 1 — Expand validation set:
- Current: val is only 2023 (1 year, ~800 rows). Optuna/calibration could overfit to one year's market regime.
- Fix: val = 2021-2023 (3 years, ~2400 rows).
- Update modeling/train.py train/val split logic: val_years = [2021, 2022, 2023]
- Report: new val AUC vs old val AUC

Part 2 — Fix feature selection leakage (Critical #7):
- Current bug: run_feature_selection.py computes IC/PSI on ALL data including test years (soft leakage)
- Fix: add `--train-end` parameter (default 2020). Filter df to fiscal_year <= train_end BEFORE computing IC, ICIR, and PSI.
- This is the caveman fix — one filter line. Session 22 already proved the 27 features are temporally stable, so full WF per-fold selection is unnecessary.

After: pytest, commit as `feat(modeling): expand val to 2021-2023 + restrict feature selection to train-only`. Push.
```

---

### Session 40: Retrain Decision Tree on Production Split

**Items:** Critical #4

**Scope:** Retrain depth-4 tree on 2008-2020 window (matching LightGBM's TRAIN_CUTOFF after session 39).

**Prompt:**
```
Session 40: Retrain Decision Tree (2008-2020)

Read SESSION_PLAN.md first. Execution session.

Context: Session 39 changed TRAIN_CUTOFF from 2022 to 2020 (val=2021-2023, test=2024+).
Decision tree must match LightGBM's training window for consistency.

Current: decision tree trained on 2008-2018 only (from session 24 research).
Fix: retrain on 2008-2020 (production training window) using the canonical 27 pruned features.

Steps:
1. Load historical_dataset_clean.parquet, filter to train period 2008-2020
2. Train depth-4 DecisionTreeClassifier on beat_local_market_1y target (same as session 24)
3. Extract human-readable rules → models/decision_tree_rules.json (overwrite)
4. Re-run agreement filter threshold sweep (0.30-0.50) on val period 2021-2023
5. Report: did rules change? Did optimal threshold change? New Sharpe/CAGR on val?
6. Save updated tree model
7. Run train.py with default --train-cutoff 2020 and report val AUC (3-year val vs old 1-year val)
8. If val AUC is acceptable, do production retrain: python3 modeling/train.py --train-cutoff 2023
   (absorbs 2021-2023 into training for the deployed model, only 2024+ remains as pure test)

Concern: step 7 trains on 2 fewer years than before (2008-2020 vs old 2008-2022). Watch for val AUC degradation.
If val AUC drops materially, flag it — may need to skip straight to step 8 (production retrain on 2008-2023).
Test period (2024) is only ~800 rows — threshold sweep may be noisy; val (2021-2023) is more reliable for tuning.

After: pytest, commit as `feat(modeling): retrain decision tree + LightGBM on production split`. Push.
```

---

### Session 41: ADTV Filter + FAQ File ✓ DONE

**Items:** Critical #1 + #2

**Scope:** Parameterize liquidity filter; write quick-reference FAQ.

**Prompt:**
```
Session 41: ADTV Filter Parameterization + FAQ File

Read SESSION_PLAN.md first. Execution session — product readiness.

Part 1 — ADTV filter (backtest/engine.py or portfolio/build_portfolio.py):
- Current: hardcoded $1M ADTV floor (institutional assumption)
- Fix: add `aum_target` parameter (default $200K for retail)
- Formula: min_adtv = aum_target * 0.01 (can't be >1% of daily volume)
- Use MEDIAN daily volume (not mean) over trailing 30 days to avoid block-trade spikes
- Update backtest to use new filter

Part 2 — FAQ file:
- Create docs/FAQ.md (NOT a README, a quick-reference for LLM and human operators)
- Content: company count, feature count (27 canonical), pipeline steps (1-6), key thresholds (PSI 0.25, IC 0.02, Beneish -1.78, tree 0.35), model horizons (1y/3y/5y), data source (SEC EDGAR), update frequency (weekly CI)
- Keep it under 50 lines

After: pytest, commit as `feat(portfolio): parameterize ADTV by AUM + add FAQ`. Push.
```

---

### Session 43: Production Notebook (End-to-End Pipeline)

**Scope:** Create `notebooks/production_screener.ipynb` — a single notebook that IS the product.

**Prompt:**
```
Session 43: Production Screener Notebook

Read SESSION_PLAN.md first. Execution session — product delivery.

Create notebooks/production_screener.ipynb with these sections:

1. Data Import — Load historical_dataset_clean.parquet, add_piotroski_ext, add_normalised_ratios
2. Hard Gates — Apply fraud/distress filters FIRST:
   - Beneish M-score < -1.78 (fraud)
   - Piotroski >= 3 (minimum health)
   - piotroski_roa_pos == 1 (profitable)
   - fraud_suspect == 0
   - Market = US, market_cap >= $50M
   - Altman Z > 1.0 (not in extreme distress)
3. Feature Selection — Load 22 canonical features (from model_meta.json)
4. Model Scoring — Load production clean LightGBM + decision tree, score survivors
5. Agreement Gate — tree_prob >= 0.45
6. Portfolio Construction — Top 15 by ml_3y, equal-weight
7. Stock Analysis — For each pick: show key metrics, sector, thesis
8. LLM Summary — Generate natural-language buy rationale per stock (use Claude API or template)
9. Final Output — Table with: ticker, ml_3y, tree_prob, Piotroski, Altman Z, market_cap, expected_return

Investment philosophy embedded:
- Small-cap focus (institutional funds not fishing here)
- Fraud/distress removal is the PRIMARY alpha (not stock picking)
- Liquidity: ADTV filter for $200K AUM (1% of daily volume)
- Optimize 3y CAGR, target >30%, Sharpe >1.0

After: pytest, commit as `feat(notebooks): production screener notebook`. Push.
```

---

### Post-Session 43 — Hardening Phase (Sessions 44-48)

Production config validated: Regression + tree>=0.55 + $10B cap.
CAGR +34.7%, Sharpe 0.97, 0 negative years (2013-2023).

**Goal:** Fix flaws, document properly, then launch paper trading. Caveman: 1 session = 1 scope = 1 commit.

---

### Session 44: Persist Regression Model as Artifact

**Scope:** Notebook currently retrains regression on every run. Fix: train once, save to disk, notebook loads.

**Prompt:**
```
Session 44: Persist Regression Model

Read SESSION_PLAN.md (session 44 entry). Execution session.

Problem: notebooks/production_screener.ipynb retrains LGBMRegressor inline every run.
This breaks reproducibility — picks can drift between runs.

Fix:
1. modeling/train.py — after LightGBM classifier training for 3y horizon, also train
   LGBMRegressor(n_estimators=600, max_depth=6, lr=0.03) on forward_return_3y (clean data).
   Save as models/model_3y_regression.joblib. Add regression metadata to model_meta.json
   (features, train_medians, train_cutoff).
2. notebooks/production_screener.ipynb — replace inline regression training (cell "score")
   with joblib.load('models/model_3y_regression.joblib'). Use train_medians from meta for
   imputation (not survivors.median()).
3. Add assert for model file existence at top of notebook.

Verify: notebook produces same ranking loading model vs training inline.
After: pytest, commit as `fix(modeling): persist regression model as artifact`. Push.
```

---

### Session 45: Centralize Constants + Align Backtest Engine

**Scope:** Hardcoded thresholds in 5 places → single source of truth. Add Altman Z gate to engine.

**Prompt:**
```
Session 45: Centralize Constants

Read SESSION_PLAN.md (session 45 entry). Execution session.

Problem: Beneish=-1.78, tree=0.55, Piotroski=3, value_pct=0.7, Altman=1.0 are hardcoded
in backtest/engine.py, notebook, and train.py separately. Risk of divergence.
Also: notebook has Altman Z gate but backtest engine ml_gates mode doesn't.

Fix:
1. modeling/constants.py — add production threshold constants:
   BENEISH_THRESHOLD = -1.78, TREE_THRESHOLD = 0.55, PIOTROSKI_MIN = 3,
   VALUE_GATE_PCT = 0.70, ALTMAN_Z_MIN = 1.0, MAX_MARKET_CAP = 10_000_000_000
2. backtest/engine.py — import from constants in filter_composite ml_gates mode.
   Add Altman Z gate: s = s[s['altman_z_score'].fillna(0) > ALTMAN_Z_MIN]
3. Run backtest, confirm CAGR unchanged (~34.7%).

After: pytest, commit as `refactor(constants): centralize production thresholds`. Push.
```

---

### Session 46: Tests + Fix Stale FAQ

**Scope:** Add unit tests for gates. Fix FAQ.md stale values.

**Prompt:**
```
Session 46: Gate Tests + FAQ Fix

Read SESSION_PLAN.md (session 46 entry). Execution session.

Problem: No test coverage for regression ranking, max_cap, value gate, tree threshold.
FAQ.md still says tree>=0.35 (stale from session 24).

Fix:
1. tests/backtest/test_engine_gates.py (new file) — 8-10 tests:
   - test_max_cap_excludes_large_stocks
   - test_min_cap_excludes_micro_stocks
   - test_value_gate_excludes_overpriced
   - test_tree_threshold_filters_low_confidence
   - test_altman_z_gate
   - test_regression_ranking_uses_reg_3y_wf
   - test_clean_training_filter
   - test_piotroski_gate
   Use small synthetic DataFrames (5-20 rows), don't load real data.
2. docs/FAQ.md — fix: tree threshold 0.35→0.55, features "28 for 3y", add regression model note.

After: pytest (all pass including new), commit as `test(backtest): gate tests + fix FAQ`. Push.
```

---

### Session 47: Portfolio Output + Tracking

**Scope:** Notebook saves picks to JSON so paper trading has a baseline.

**Prompt:**
```
Session 47: Save Production Picks

Read SESSION_PLAN.md (session 47 entry). Execution session.

Problem: Notebook prints picks but doesn't persist them. No way to track over time.

Fix:
1. notebooks/production_screener.ipynb — add final cell that saves to:
   - data/production_picks_YYYY-MM-DD.json (timestamped, never overwritten)
   - data/production_picks_latest.json (always the most recent)
   Format: {date, config (thresholds + cap), picks [{ticker, name, sector, reg_3y,
   tree_prob, piotroski, altman_z, market_cap}], metadata {n_universe, n_survivors, n_agreed}}
2. If previous latest.json exists, print diff: "N new picks, N dropped since last run"

After: pytest, commit as `feat(notebook): persist production picks for tracking`. Push.
```

---

### Session 48: Regime Overlay + Paper Trading Launch

**Scope:** Wire existing regime overlay into notebook. Generate first real picks. Record entry.

**Prompt:**
```
Session 48: Regime Overlay + Paper Trading

Read SESSION_PLAN.md (session 48 entry). Execution session.

Problem: Regime overlay (SPY DD>15% = reduce 50%) was built in session 25 but never
wired into the production notebook. Also need to launch paper trading.

Fix:
1. notebooks/production_screener.ipynb — add regime check section after portfolio:
   - Load SPY data, compute trailing drawdown from peak
   - If DD > 15%: flag "RISK-OFF — reduce positions 50%"
   - If DD <= 15%: flag "NORMAL — full deployment"
   (Use research/regime_overlay.py logic as reference)
2. Run notebook for FY2025 → generate 15 picks with current scores
3. Save to data/paper_portfolio_2026-07.json with entry context
4. Create docs/PAPER_TRADING.md: picks, entry date, success criteria
   (annualized return > SPY + 15%, no position down >50%), 6-month review date

After: pytest, commit as `feat(portfolio): regime overlay + paper trading launch`. Push.
```

---

### Post-Session 48: Research Backlog

| # | Topic | When |
|---|-------|------|
| 49 | Multi-horizon blend (0.6×3y + 0.4×1y) | After 3 months paper trading |
| 50 | 6-month forward test honest report | January 2027 |
| 51 | Auto-retrain pipeline (Parked #11) | When live money deployed |

**Tracking at session close:** Move completed items from BACKLOG.md → Completed section.

---

## Known Risks (Not Session Tasks — Just Track)

| Risk | Severity | Mitigation |
|------|----------|------------|
| Capacity constraint (~$5-50M AUM before moving prices) | Medium | Document in screener output; commercial plan targets small institutional |
| OOS degradation (signal decays as others discover it) | Medium | Realistic expectation: deployed Sharpe ~0.6-0.8 after implementation shortfall |
| Data freshness (annual filings lag 60-90 days) | Low | Accept; quarterly data could supplement later |
| Universe bias (SEC EDGAR = US-listed only) | Low | IARB strategy covers non-US via ADRs |
| No independent dataset validation | Medium | Could validate on Compustat/Bloomberg if access obtained |

---

## Working Protocol

- One commit per session, conventional message
- Token-optimised: no redundant reads, no full-file dumps >200 lines, caveman approach
- Session closure: what done + commit hash + next session prompt + push
- Each session prompt includes: `Read SESSION_PLAN.md first`
- From session 22 onward: always push to remote
