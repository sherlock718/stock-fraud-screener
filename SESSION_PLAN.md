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

---

## Upcoming Sessions

### Session 22: Proper Train/Validate/Test Split (HIGHEST PRIORITY)

**Why first:** Without this, all results have subtle look-ahead bias in feature selection. True OOS performance is unknown.

**What to do:**
1. Temporal 3-way split: Train (2005-2012), Validate (2013-2016), Test (2017-2023)
2. Feature selection confined to TRAIN period only (no peeking at validate/test)
3. Hyperparameter tuning on VALIDATE period
4. Final Sharpe/CAGR reported on TEST period only (2017-2023)
5. Compare "true OOS" Sharpe vs current 1.37 (expect lower — that's honest)

**Key files:** research/feature_selection_engine.py, backtest/engine.py (load_and_score), modeling/train.py

---

### Session 23: Pruned Feature Set Backtest

**Why:** Use ablation results + proper split to build a lean ~25-feature model.

**What to do:**
1. Drop the 16 prune candidates from feature set
2. Re-run proper-split backtest with lean set
3. Compare Sharpe: full 45 vs lean 25 (both on test period 2017-2023)

---

### Session 24: Explainable Decision Tree Model

**Why:** Makes the tool usable for actual stock decisions. "Here's WHY this stock was picked."

**What to do:**
1. Train a depth-3-5 decision tree alongside LightGBM
2. Extract human-readable rules (IF earnings_yield > X AND piotroski > Y → BUY)
3. Compare Sharpe: tree vs LightGBM (expect tree slightly worse but explainable)
4. Add tree rules to portfolio output/screener

---

### Session 25: Regime Overlay (Macro Signal)

**Why:** Biggest risk mitigation for cheapest effort. Prevents being fully invested during crashes.

**What to do:**
1. Track yield curve (10y-2y), VIX level
2. When curve inverts OR VIX > 30 → reduce position size by 50%
3. Re-run backtest with overlay, measure drawdown improvement
4. ~10 lines of code, no model risk

---

### Session 26: IC Code Consolidation (Housekeeping)

**Why:** Cleanup only. factor_research.py duplicates ic_engine.py logic.

**What to do:**
1. Refactor factor_research.py to call ic_engine.py
2. Remove duplicated IC formulas

---

## Working Protocol

- One commit per session, conventional message
- Token-optimised: no redundant reads, no full-file dumps >200 lines, caveman approach
- Session closure: what done + commit hash + next session prompt + push
- Each session prompt includes: `Read SESSION_PLAN.md first`
