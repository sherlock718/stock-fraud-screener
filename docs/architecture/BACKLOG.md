# Product Backlog

Collected during orientation sessions (27-30). Each item is a future session candidate.
Priority is owner's judgment, not urgency.

---

## Critical (do before commercialization)

| # | Item | Discovered | Context |
|---|------|-----------|---------|
| 1 | ADTV feature addition to step3 | Session 27 | Volume data is free (yfinance), already fetched but not persisted. Without it, can't filter illiquid stocks. |
| 2 | FAQ / key facts file | Session 27 | Quick-reference for LLM and human. Company count, feature count, pipeline flow, thresholds. Reduces re-audit cost. |
| 3 | Unify divergent feature sets (pick 27 pruned as canonical) | Session 28 | `feature_sets_3y.json` (45) vs `feature_sets_pruned.json` (27). OOF scoring uses 45, backtest uses 27. Pruned performed better (Sharpe 1.124 > 0.954). One canonical set per horizon needed. |
| 4 | Retrain decision tree on production split (2008-2022) | Session 28 | Tree currently trained on 2008-2018 only. Should match LightGBM's training window for consistency. Rules may change. |
| 5 | Add model staleness CI check | Session 28 | No automated check that `.joblib` artifacts are older than source data. Risk of deploying stale predictions. Add warning script or CI step + document regeneration command. |
| 6 | Validate alpha factors with IC analysis | Session 28 | Alpha factors are the customer-facing explainability layer. If any factor has near-zero IC against forward returns, it's misleading to show it. Run IC per factor, remove or downweight factors that don't predict. |
| 7 | Walk-forward feature selection for production pipeline | Session 28 | Current `run_feature_selection.py` selects once on full training data. Should re-select per fold to prevent selection bias. Already validated in session 22 GATE but production pipeline takes shortcut. |
| 8 | Expand validation set or use WF AUC for tuning | Session 28 | Val is only 2023 (1 year, ~800 rows). Optuna/calibration could overfit to one year's market regime. Options: expand val to 2021-2023, or use walk-forward mean AUC as Optuna objective instead of single-year val AUC. |

---

## Parked for Later (nice-to-have, not blocking)

| # | Item | Discovered | Context |
|---|------|-----------|---------|
| 1 | FX handling for cross-market model | Session 27 | Not needed while markets are trained separately. Only matters for combined global ranking. |
| 2 | Better enrich script structure | Session 27 | Rename to step7/step8 or add naming convention. Functional but confusing for maintenance. |
| 3 | Manual scripts need clarity | Session 27 | Which enrich_* are auto-called vs manual? No documentation distinguishes them. |
| 4 | Replace force-include hack with two-tier selection | Session 28 | 9 momentum/macro features bypass ICIR pipeline for 6m/1y. Works but fragile. Investigate relaxed IC stability threshold for momentum-class features as alternative. |
| 5 | Raise IC minimum from 0.02 to 0.03 | Session 28 | Current 0.02 is very permissive. Test if 0.03 drops marginal features without hurting WF AUC. Only apply if performance-neutral or better. |
| 6 | Tighten correlation dedup from 0.85 to 0.80 | Session 28 | 0.80 is more standard. Pruned backtest (fewer features = better) supports tighter dedup. Test impact before applying. |
| 7 | Per-horizon Optuna tuning for n_estimators | Session 28 | All 5 models use n_estimators=600. Shorter horizons (6m) may need fewer trees to avoid overfitting. Run `modeling/tune.py` per horizon. |
| 8 | Document fraud_risk circular dependency | Session 28 | ML → OOF → fraud_risk → composite. Not a bug (excluded from training) but fragile. Add comment + unit test asserting alpha columns stay in EXCLUDE set. |
| 9 | Fix BASE ordering bug in score_oof.py | Session 28 | Line 44 uses BASE before line 52 defines it. Trivial fix: move `BASE = ROOT` above `DATA_PATH`. |
| 10 | Document regression model vs classifier relationship | Session 28 | Two models answer different questions (probability vs magnitude). Not a bug but needs clear documentation for users. |
| 11 | Add automated retraining trigger in CI | Session 28 | Models manually retrained. No job checks data freshness. Needed for productionization. |
| 12 | Sweep agreement threshold 0.50–0.80 | Session 28 | Higher threshold = fewer picks, higher Sharpe, lower CAGR. Check if 0.6-0.8 gives acceptable CAGR (>20%) with very high selectivity. Report n_picks/CAGR/Sharpe tradeoff. |

---

## Ideas (unvalidated, revisit after session 30)

| # | Item | Discovered | Context |
|---|------|-----------|---------|
| 1 | Sector-conditional alpha weights | Session 28 | Equal 20% weights ignore industry differences. Tech might benefit from momentum+growth weighting, financials from quality+fraud_risk. Risk: overfitting with small per-sector samples (~50-100 stocks/sector/year). Research task. |
| 2 | Ensure alpha composite aligns with ML picks | Session 28 | If ML picks stocks with low alpha_composite, the product shows contradictory signals to customers. Either force alignment (require alpha ≥ threshold for displayed picks), fix factors so they correlate with returns, or honestly relabel as "stock profile" not "alpha." |

---

## Completed

| # | Item | Session done | Commit |
|---|------|-------------|--------|
| — | — | — | — |
