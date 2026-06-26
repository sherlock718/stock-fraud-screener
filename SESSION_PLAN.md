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

## GATE: Session 22 Determines Path Forward

After Session 22, check test-period Sharpe (2017-2023 with unbiased feature selection):
- **Sharpe ≥ 0.8**: Signal is real. Proceed to sessions 23-25 (refine + harden).
- **Sharpe 0.5–0.8**: Signal exists but weaker than reported. Still proceed, but lower expectations.
- **Sharpe < 0.5**: Feature selection was overfitted. PIVOT — go back to feature engineering fundamentals, not downstream refinement.

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
6. **Stability check**: shift train window (2007-2014 instead of 2005-2012) — do similar features survive? If not, model is fragile regardless of Sharpe.

**Key files:** research/feature_selection_engine.py, backtest/engine.py (load_and_score), modeling/train.py

**Output:** reports/proper_split_results.md with honest test-period metrics + feature stability comparison

---

### Session 23: Pruned Feature Set Backtest

**Why:** Use ablation results + proper split to build a lean ~25-feature model.

**What to do:**
1. Drop the 16 prune candidates from feature set
2. Re-run proper-split backtest with lean set
3. **Validate as a GROUP** — don't assume individual ablation results sum linearly. Two correlated features (e.g. ps_ratio + ps_ratio_sector_pct) might both show "neutral" individually but removing both could hurt. Run the full pruned set as one backtest.
4. Compare Sharpe: full 45 vs lean ~25 (both on test period 2017-2023)
5. If pruned set is worse than full, add back features one at a time until stable

**Depends on:** Session 22 gate passing (Sharpe ≥ 0.5)

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
