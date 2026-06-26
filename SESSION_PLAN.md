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

### Session 24: Explainable Decision Tree Model

**Why:** Makes the tool usable for actual stock decisions. "Here's WHY this stock was picked."

**What to do:**
1. Train a depth-3-5 decision tree alongside LightGBM (same feature pool from session 22)
2. Extract human-readable rules (IF earnings_yield > X AND piotroski > Y → BUY)
3. Compare Sharpe: tree vs LightGBM (expect tree slightly worse but explainable)
4. Add tree rules to portfolio output/screener

**Depends on:** Session 23 (use the final pruned feature set)

---

### Session 25: Regime Overlay (Macro Signal)

**Why:** Biggest risk mitigation for cheapest effort. Prevents being fully invested during crashes.

**Pre-requisite:** Source VIX + yield curve (10y-2y) historical data. Options:
- FRED API (free, reliable) for yield curve
- Yahoo Finance for ^VIX history
- Or: use SPY drawdown > 15% as crude regime proxy (no external data needed)

**What to do:**
1. Source macro data OR implement SPY-drawdown proxy
2. When signal triggers → reduce position size by 50%
3. Re-run backtest with overlay, measure drawdown improvement
4. ~10-20 lines of code, no model risk

**Depends on:** Sessions 22-23 (use final model for accurate measurement)

---

### Session 26: IC Code Consolidation (Housekeeping — do when bored)

**Why:** Cleanup only. factor_research.py duplicates ic_engine.py logic.

**What to do:**
1. Refactor factor_research.py to call ic_engine.py
2. Remove duplicated IC formulas

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
