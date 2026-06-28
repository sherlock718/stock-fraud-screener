# Product Backlog

Collected during orientation sessions (27-30). Each item is a future session candidate.
Priority is owner's judgment, not urgency.

---

## Critical (do before commercialization)

| # | Item | Discovered | Context |
|---|------|-----------|---------|
| ~~1~~ | ~~ADTV filter: parameterize by AUM + use median not mean~~ | ~~Session 27/29~~ | **DONE — Session 41 (6b618f7). `aum_target` param (default $200K), formula min_adtv = aum_target × 0.01, median (not mean) trailing 30d. CLI: `--aum-target`.** |
| ~~2~~ | ~~FAQ / key facts file~~ | ~~Session 27~~ | **DONE — Session 41 (6b618f7). `docs/FAQ.md` — 37 lines covering company count, features, pipeline, thresholds, models, data source.** |
| ~~3~~ | ~~Unify divergent feature sets (pick 27 pruned as canonical)~~ | ~~Session 28~~ | **DONE — Session 34 (3c01a46). feature_sets_3y.json now uses 27 pruned features.** |
| ~~4~~ | ~~Retrain decision tree on production split (2008-2020)~~ | ~~Session 28~~ | **DONE — Session 40 (b490f57). Tree retrained on 2008-2020. Rules changed (5→8 BUY). Threshold 0.35 confirmed on val 2021-2023 (Sharpe 1.128). Dev models at cutoff 2020; production retrain (cutoff 2023) is a deployment-time step.** |
| ~~5~~ | ~~Add model staleness CI check~~ | ~~Session 28~~ | **DONE — Session 34 (3c01a46). quality/check_model_staleness.py added to CI as non-fatal step.** |
| ~~6~~ | ~~Validate alpha factors with IC analysis~~ | ~~Session 28~~ | **DONE — Session 37 (10bb452). All 5 factors pass: value IC=0.152, quality 0.095, momentum 0.038, growth 0.046, fraud_risk 0.143. No weight changes needed.** |
| ~~7~~ | ~~Walk-forward feature selection for production pipeline~~ | ~~Session 28~~ | **DONE — Session 39 (3c9e691). `run_feature_selection.py` now accepts `--train-end` (default 2020). IC/PSI/candidates restricted to fiscal_year <= train_end. Full WF per-fold selection unnecessary (session 22 proved 27 features temporally stable).** |
| ~~8~~ | ~~Expand validation set or use WF AUC for tuning~~ | ~~Session 28~~ | **DONE — Session 39 (3c9e691). Val expanded to 2021-2023 (3 years, ~2400 rows). TRAIN_CUTOFF=2020. Production retrain on train+val (--train-cutoff 2023) planned for session 40 step 8.** |
| ~~9~~ | ~~Survivorship bias: change default to impute or flag~~ | ~~Session 29~~ | **DONE — Session 36. Default survivorship_mode='impute' (-50%). CLI: --survivorship-mode.** |
| ~~10~~ | ~~Fix benchmark for non-US strategies (iarb)~~ | ~~Session 29~~ | **DONE — Session 37 (10bb452). Non-US strategies now use MSCI ACWI ex-US as primary benchmark. SPY remains informational. data/acwi_exus_returns.csv added (2008-2024).** |
| ~~11~~ | ~~Filing date rebalance timing~~ | ~~Session 29~~ | **DONE — Session 36. filing_date_gate=True by default; only stocks filed before holding year start are eligible.** |
| ~~12~~ | ~~Remove composite weight blend — rank by ML only~~ | ~~Session 29~~ | **DONE — Session 38. `mode='ml_gates'` added to filter_composite(). ML 3y WF as sole ranking signal + tree agreement gate (>=0.35) + hard gates (Beneish, Piotroski>=3, not-delisted). Sharpe 1.204 vs blended 0.937.** |

| ~~13~~ | ~~Unit tests for quality/ scripts (CI gates have 0 test coverage)~~ | ~~Session 30~~ | **DONE — Session 32 (d874e10)** |
| ~~14~~ | ~~Unit tests for alpha/factors and backtest/engine~~ | ~~Session 30~~ | **DONE — Session 33 (a4f6aff)** |
| ~~15~~ | ~~Fix undefined BASE in workflows/run_pipeline_br.py~~ | ~~Session 30~~ | **DONE — Session 31 (1ecbdd6)** |
| ~~16~~ | ~~Unit tests for research/ + portfolio/ modules~~ | ~~Session 34~~ | **DONE — Session 34 (1539bd3). Also fixed syntax bug in build_screener_registry.py.** |

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
| ~~9~~ | ~~Fix BASE ordering bug in score_oof.py~~ | ~~Session 28~~ | **DONE — Session 31 (1ecbdd6)** |
| 10 | Document regression model vs classifier relationship | Session 28 | Two models answer different questions (probability vs magnitude). Not a bug but needs clear documentation for users. |
| 11 | Add automated retraining trigger in CI | Session 28 | Models manually retrained. No job checks data freshness. Needed for productionization. |
| 12 | Sweep agreement threshold 0.50–0.80 | Session 28 | Higher threshold = fewer picks, higher Sharpe, lower CAGR. Check if 0.6-0.8 gives acceptable CAGR (>20%) with very high selectivity. Report n_picks/CAGR/Sharpe tradeoff. |
| 13 | Portfolio construction mode toggle | Session 29 | Two modes: (a) **Concentrated** (top 5-10, equal-weight, no sector cap) = DEFAULT for unleveraged Dhando-style investing. (b) **Diversified** (top 20, inverse-vol, position+sector caps) auto-activated when `leverage_multiplier > 1.0`. Leverage param IS the switch — no separate config needed. |
| ~~14~~ | ~~Consolidate duplicated code (WF loop, _sic_to_sector, EXCLUDE sets)~~ | ~~Session 29~~ | **Partially DONE — Session 35 (cb11c9d). EXCLUDE sets + load_data consolidated into modeling/constants.py. WF loop + _sic_to_sector intentionally left as-is (trivial to keep in sync).** |
| 15 | Time-varying risk-free rate | Session 29 | RISK_FREE=0.03 constant distorts Sharpe in zero-rate era (2009-2022). Load from Treasury 1y CSV, use matched rate per year. Doesn't affect stock selection, only reporting accuracy. |
| 16 | Economic rationale registry for features | Session 29 | Each surviving feature should map to a theoretical justification (why it predicts returns). Features without economic story get flagged. For future new features: integrate as automated gate in feature selection pipeline — lookup against registry, flag unmatched features for manual review before production entry. |
| 17 | Forward return survivorship in price data | Session 29 | Delisted stocks disappear from price data, biasing forward_return_1y upward. Fix requires survivorship-free price database (CRSP = expensive). Partial mitigation: flag tickers that disappear from next year's data and impute worst-case return. |
| 18 | Quarterly data integration | Session 29 | Currently one data point per company per year (annual filing). Quarterly filings could provide earlier signals and enable intra-year rebalancing. Major pipeline change — only pursue after annual model is production-ready. |
| 19 | Activate fraud/ package (extract from pipeline/) | Session 30 | `fraud/taxonomy.py` is a stub (NotImplementedError). All fraud logic lives in `pipeline/enrich_fraud_taxonomy.py`. Extract when taxonomy needs to be extended or reused. |
| 20 | CI non-fatal steps should alert on failure | Session 30 | factor_research and feature_selection failures are swallowed with `|| echo`. Should at least produce a GitHub annotation so regressions are visible. |
| 21 | Extract model hyperparams to config file | Session 28 | n_estimators=600, max_depth=6 hardcoded in train.py, score_oof.py, tune.py. One config file prevents drift. |
| 22 | Validate alpha factor signal presence (warn on NaN) | Session 28 | Alpha factors silently return NaN if all signals missing. Should log warning loudly. |
| ~~23~~ | ~~Consolidate load_data() duplication~~ | ~~Session 28~~ | **DONE — Session 35 (cb11c9d). Canonical load_data() now in modeling/constants.py.** |
| 24 | Strategy filters to separate module | Session 29 | `filter_composite/qem/scdv/iarb` live in backtest/engine.py but imported by research scripts. Move to `portfolio/strategies.py` to decouple. |
| ~~25~~ | ~~Archive dead file step1_fetch_tickers_jp_free.py~~ | ~~Session 27~~ | **DONE — Session 31 (1ecbdd6)** |

---

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
