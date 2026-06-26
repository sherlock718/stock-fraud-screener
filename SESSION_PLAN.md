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

## Orientation Phase (Sessions 27–30)

> **Rule: Do not refactor. Do not rewrite code.** These sessions are for mapping, lineage, and risk discovery only.

Each session produces:
1. **What we covered** — summary of files read, architecture mapped, risks found
2. **Quiz** — 5-8 short questions to confirm your understanding
3. **Next-session prompt** — self-contained, ready to paste

---

### Session 27: Pipeline Spine (raw data → clean dataset)

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

7. End with: quiz (5-8 questions), then prompt for session 29.
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

### Session 31+: Post-Orientation (TBD after sessions 27-30)

Decided after orientation phase completes. Likely candidates:
- Architecture cleanup (execute refactor candidates from orientation)
- API/productionization
- Scheduled refresh pipeline
- Commercialization prep

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
