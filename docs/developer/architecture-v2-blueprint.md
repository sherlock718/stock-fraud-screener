# Architecture V2 — Migration Blueprint (Token-Efficient Multi-Session)

## Core Goal

Single-person ADHD-friendly alpha research platform. Not over-engineered. Originally prototyped in one notebook. Key focus: **fraud features** (separate, prominent, user's primary research area).

---

## Architecture Decision: Fraud as First-Class Module

```
stock-screener/
├── pipeline/          ← Data collection + base feature computation
│   ├── step01_fetch_tickers.py (+ market variants)
│   ├── step02_build_snapshots.py (+ market variants)
│   ├── step03_enrich_prices.py
│   ├── step04_enrich_macro.py
│   ├── step05_compute_features.py (calls feature_library)
│   ├── step06_clean.py (merged: clean + quality_fix + confidence + impute + survivorship)
│   ├── feature_library.py (all formulas including fraud — pipeline source of truth)
│   ├── universe.py
│   └── run.py (orchestrator)
│
├── fraud/             ← USER'S MAIN FOCUS AREA (separate, prominent)
│   ├── rules.py       (Beneish > -1.78, Altman < 1.81, Piotroski, Montier — rule-based flags)
│   ├── features.py    (fraud-specific feature documentation + registry)
│   ├── research.ipynb  (fraud IC analysis, false positive rate, feature importance)
│   └── taxonomy.py    (fraud type classification: accounting, dilution, governance, insider)
│
├── research/          ← Feature selection + analysis (notebook-first)
│   ├── feature_selection.ipynb
│   ├── feature_coverage.ipynb
│   ├── ic_engine.py (IC/ICIR computation)
│   ├── feature_selection_engine.py (PSI + IC + ICIR + dedup)
│   └── feature_registry.md (all features documented: formula, group, IC, source)
│
├── modeling/          ← Both rule-based AND ML
│   ├── rules.py       (decision tree rules, threshold-based scoring)
│   ├── train.py       (LightGBM ensemble)
│   ├── tune.py        (Optuna)
│   ├── score_oof.py   (walk-forward OOF — ONLY unbiased scoring)
│   ├── alpha.py       (factor composite computation)
│   └── explain.py     (SHAP + rule explanations)
│
├── backtest/          ← Walk-forward validation (train/val/test)
│   ├── engine.py      (walk-forward backtester)
│   ├── strategies.py  (strategy definitions)
│   └── run.py
│
├── portfolio/         ← Construction + reports
│   ├── build.py       (without leverage)
│   ├── leverage.py    (with leverage, Kelly sizing)
│   ├── report.py      (monthly tracking, tearsheet)
│   └── registry.json  (alpha signal weights)
│
├── quality/           ← Validation gates (CI calls individually)
│   ├── bias_audit.py
│   ├── check_data.py
│   ├── monitor_drift.py
│   └── test_dataset.py
│
├── data_io/           ← External sync (HF, AAER, SPY)
│   ├── hf_push.py
│   ├── hf_pull.py
│   ├── fetch_aaer.py
│   └── fetch_spy.py
│
├── data/              ← Artifacts
├── models/            ← Model artifacts
├── tests/             ← Test suite
├── _archive/          ← Everything old
│
├── _root.py
├── README.md
├── CLAUDE.md (simplified)
├── pyproject.toml
└── requirements.txt
```

---

## Test Strategy

Tests live in `tests/` and are updated during each migration session (not deferred to a single "fix tests" session).

**Current**: 341 tests covering pipeline steps 1-6 + enrichments + feature_library.

**Added/updated during migration:**

| Module | Tests needed | When |
|---|---|---|
| `pipeline/step06_clean.py` (merged) | Test merged logic (clean + impute + survivorship + confidence) | Session 14 |
| `fraud/rules.py` | Test each rule threshold (Beneish, Altman, Piotroski, custom) | Session 15 |
| `fraud/taxonomy.py` | Test fraud type classification | Session 15 |
| `modeling/rules.py` | Test rule-based scorer produces expected flags | Session 15 |
| `backtest/engine.py` | Verify walk-forward split is correct (no future data in train) | Session 15 (move existing) |
| `research/feature_selection_engine.py` | Test that selection uses ONLY train split dates | Session 16 |
| Integration test | Full pipeline run on small synthetic data → features → selection → model → backtest | Session 17 |

**Test principle**: Every session that moves/merges code must run `pytest -q --tb=line` and stay at 341+ passes. New merged modules get tests in the same session they're created.

---

## Fraud Integration with Feature Set

Fraud features flow through the SAME selection pipeline as all other features — no special treatment in selection, only in research focus.

```
pipeline/feature_library.py
  └→ Computes 164 fraud features (Beneish, Altman, Ohlson, Piotroski, Montier, taxonomy)
  └→ These appear as columns in the parquet alongside all 314 features

research/feature_selection_engine.py
  └→ Evaluates ALL features (including fraud) through PSI → IC → ICIR → dedup
  └→ Some fraud features will be selected, some won't — based on predictive power
  └→ Uses ONLY train split for IC computation

fraud/rules.py
  └→ ADDITIONALLY: applies hard threshold rules (Beneish > -1.78 = manipulator flag)
  └→ These rule flags are SEPARATE from ML feature selection
  └→ Can override ML: if Beneish flags fraud, portfolio excludes regardless of ML score

fraud/research.ipynb
  └→ YOUR workspace: analyze fraud feature IC specifically
  └→ Develop NEW fraud features here → when ready, add to feature_library.py
  └→ Test new fraud features on train split only before promoting
```

**Integration flow:**
```
All 314 features (including 164 fraud)
    ↓
Feature selection (same train split) → ~45 selected per horizon
    ↓                                    (some will be fraud features)
ML model uses selected features
    ↓
Rule-based flags applied SEPARATELY (fraud/rules.py)
    ↓
Final score = ML score + rule overrides
    ↓
Portfolio excludes stocks flagged by fraud rules
```

**Key**: Fraud features compete fairly in selection (no forced inclusion). But fraud RULES (hard thresholds) are applied as a portfolio-level filter — even if ML says "buy", a Beneish-flagged stock is excluded. This gives you both: data-driven selection + domain-expert overrides.

---

## Feature Selection ↔ Model Training: Same Split

This is critical and already enforced in the current codebase. The plan maintains it:

```
SPLIT DEFINITION (one source of truth):
  train_cutoff_year = 2020
  train_cutoff_filed = "2021-01-01"

  Train: fiscal_year ≤ 2020 AND filed_date < 2021-01-01
  Val:   fiscal_year 2021-2022 (hyperparameter tuning ONLY)
  Test:  fiscal_year 2023+ (final evaluation, never used for decisions)

FEATURE SELECTION uses train split:
  research/feature_selection_engine.py
    └→ IC computed on rows WHERE fiscal_year ≤ train_cutoff_year
    └→ PSI: train distribution vs scoring-period distribution
    └→ Output: feature_sets_{horizon}.json (frozen before model sees val/test)

MODEL TRAINING uses same train split:
  modeling/train.py
    └→ Loads feature_sets_{horizon}.json (already frozen)
    └→ Trains on fiscal_year ≤ train_cutoff_year
    └→ Optuna tuning uses val split (2021-2022) for hyperparams ONLY
    └→ Never touches test split

WALK-FORWARD OOF (expanding window for final evaluation):
  modeling/score_oof.py
    └→ For each fold year Y: train on fiscal_year < Y, score year Y
    └→ Produces ml_1y_oof, ml_3y_oof, ml_5y_oof (genuinely OOS)
    └→ These scores go to backtest

BACKTEST uses OOF scores only:
  backtest/engine.py
    └→ Never sees in-sample scores
    └→ Walk-forward ensures no look-ahead
```

**The split is defined ONCE** (in a config or constant) and imported by both feature_selection_engine.py and train.py. No drift possible.

---

## Documentation Plan

**What exists now**: 44 docs, 8 top-level MDs, complex change checklists. Drifts constantly.

**Target**: 3 maintained files + archived reference.

| File | Purpose | Maintained? |
|---|---|---|
| `README.md` | What this is, how to install, how to run each module (1 page max) | Yes |
| `CLAUDE.md` | AI assistant rules (simplified: just commit convention + phase scope) | Yes |
| `research/feature_registry.md` | Every feature documented: name, formula, group, IC, source, notes | Yes — updated when features change |
| `_archive/docs/` | All 44 old docs (methodology, developer, guide, reference) | No — read-only reference |

**`research/feature_registry.md` structure:**

```markdown
# Feature Registry

## Fraud Risk (164 features)
| Feature | Formula | Source | IC (3y) | Notes |
|---|---|---|---|---|
| beneish_m_score | -4.84 + 0.92×DSRI + ... | Beneish 1999 | 0.031 | > -1.78 = manipulator |
| altman_z_score | 1.2×WC/A + 1.4×RE/A + ... | Altman 1968 | 0.028 | < 1.81 = distress |
...

## Value (18 features)
...

## Quality (83 features)
...

## Momentum (45 features)
...

## Growth (22 features)
...
```

**When to update**: Only when a feature is added, removed, or IC changes after retraining. Not on every commit. No change checklist — just keep the registry honest.

**Documentation is generated, not maintained**: `research/ic_engine.py` will output IC scores that go directly into the registry. No manual copying.

---

**Your preference**: Simple decision tree rule-based  
**My recommendation**: Both, layered.

**Why not single decision tree alone:**
- A single tree overfits to specific threshold splits (e.g., "if P/B < 1.2 AND roe > 0.15")
- Market regimes shift those thresholds — what worked 2010-2015 fails 2016-2020
- One tree captures maybe 3-5 interactions max

**Why LightGBM IS decision trees:**
- It's an ensemble of 600 shallow decision trees
- Each tree is simple (max_depth=6, just 6 if/else splits)
- The ensemble learns which rules matter when
- It's interpretable via SHAP (shows exactly which features drove each prediction)

**Recommended approach (what current system already does, simplified):**

```
Layer 1: Rule-Based Flags (transparent, explainable)
  - Beneish M-score > -1.78 → "likely manipulator"
  - Altman Z < 1.81 → "distress zone"
  - Piotroski F ≤ 3 → "financially weak"
  - Custom rules you define (your domain expertise)

Layer 2: ML Ensemble (captures non-linear interactions)
  - LightGBM learns which COMBINATIONS of rules/features predict returns
  - Walk-forward ensures no look-ahead
  - SHAP explains every prediction in rule-based language

Layer 3: Alpha Composite (final score)
  - Weighted blend of rule flags + ML score + factor scores
  - You control the weights manually based on research
```

**For your fraud focus specifically:**  
Rule-based is PERFECT for fraud detection (Beneish literally is a logistic regression with fixed coefficients). Keep rules for interpretability. Use ML to find patterns the academic rules miss. Report both.

---

## Call Graph Verification

**Critical dependency chain (verified):**

```
CI yaml
  └→ subprocess: pipeline/step01-06 (path strings)
  └→ subprocess: enrichments (will become step06 merged)
  └→ subprocess: quality/* (individual gates)
  └→ subprocess: modeling/compute_alpha
  └→ subprocess: research/feature_selection_engine (analysis)

pipeline/step05_compute_features.py
  └→ imports: pipeline.feature_library (STAYS IN PIPELINE — no break)

modeling/train.py
  └→ imports: pipeline.feature_library (feature names for EXCLUDE set)
  └→ imports: pipeline.step5_compute_features (for feature list)

backtest/engine.py
  └→ imports: pipeline.feature_library (feature metadata)

fraud/rules.py
  └→ imports: pipeline.feature_library (reads fraud formulas, adds rule thresholds)
  └→ NO reverse dependency (pipeline does NOT import from fraud/)

portfolio/build.py
  └→ imports: backtest.engine (strategy filters)
  └→ imports: modeling.alpha (factor scores)
```

**Key insight**: `pipeline/feature_library.py` is the foundation. Everything imports FROM it. It imports from NOTHING. This makes it safe to restructure everything else without touching pipeline internals.

**Fraud features stay in feature_library.py** (computed in pipeline). The `fraud/` folder adds:
- Rule interpretation (thresholds, flags)
- Fraud-specific research
- Taxonomy classification
- Future fraud features you develop

---

## Forward Bias Protection Design

```
Dataset: 58K rows, spans 2008-2027

TEMPORAL SPLIT (enforced everywhere):
  Train:  fiscal_year ≤ 2020 AND filed_date < 2021-01-01
  Val:    fiscal_year 2021-2022
  Test:   fiscal_year 2023+

FEATURE ENGINEERING: Done on FULL dataset BUT:
  - Cross-sectional ranks use fiscal_year groupby (no future peers)
  - Expanding-window imputation (only uses past data)
  - No target leakage (forward returns computed from FUTURE prices, not features)

FEATURE SELECTION: Uses ONLY train split
  - IC computed on train years only
  - PSI computed on train distribution
  - Features frozen BEFORE touching val/test

MODEL TRAINING: Walk-forward expanding window
  - For fold Y: train on fiscal_year < Y, score year Y
  - OOF scores: genuinely out-of-sample
  - No score_historical (killed — was contaminating train rows)

BACKTESTING: Walk-forward
  - Uses OOF scores only (never in-sample predictions)
  - filed_date cutoff prevents late-filing look-ahead
  - SPY benchmark from separate data source (no snooping)

SURVIVORSHIP: Handled
  - Delisted companies flagged, imputed -50% return
  - They STAY in training data (prevents positive bias)
  - survivorship_pct tracked per strategy
```

---

## Session Plan (6 sessions, low-token, self-contained)

Each session: clear scope → execute → commit → summary + next-session prompt.

### Session 13: Validation + Archive Dead Code
**Scope**: Verify repo works, archive dead files, save clean baseline  
**Time**: 30 min  
**Token budget**: Low (run commands, check results)

### Session 14: Pipeline Consolidation  
**Scope**: Merge enrichments into pipeline steps 6-9, kill score_historical  
**Time**: 60 min  
**Token budget**: Medium (file moves, import rewrites, CI updates)

### Session 15: Flatten to Target Architecture  
**Scope**: Create modeling/, backtest/, fraud/, quality/, data_io/ at top-level. Kill scripts/  
**Time**: 90 min  
**Token budget**: Medium-high (most file moves happen here)

### Session 16: Research + Docs  
**Scope**: Create research/ folder, write feature_registry.md, archive 44 docs  
**Time**: 60 min  
**Token budget**: Medium (new content creation)

### Session 17: Tests + CI Update  
**Scope**: Fix all broken imports in tests, update CI yaml, verify 341 tests pass  
**Time**: 60 min  
**Token budget**: Medium (targeted grep + edit)

### Session 18: Feature IC Notebook (first real research)  
**Scope**: Build feature analysis notebook in research/ — IC, ICIR, coverage, rankings  
**Time**: 60 min  
**Token budget**: Medium (notebook creation, parquet analysis)

---

## Session Handoff Format

At end of each session, provide:

```
## Session N Complete

**Done**: [1-2 sentences]
**Changed**: [files moved/created/deleted — count only]
**Commit**: [conventional message]
**Tests**: [pass count]
**Next session prompt**: [copy-paste starter for next session]
```

The next-session prompt will include:
- What was done in prior session
- Current branch + commit
- Exact scope for this session
- Files to touch (targeted, no broad exploration)
- Success criteria

---

## What Gets Archived

| Category | Files | Reason |
|---|---|---|
| Dead scripts (7) | score_historical, enrich_sectors_dividends, clean_dataset, analyze_distributions, patch_equity_vol, patch_montier_c2, generate_manifest | Never called or one-time patches |
| Dead quality | verify_doc_consistency | Docs being archived |
| All docs/ (44 files) | methodology/, developer/, guide/, reference/ | Replaced by feature_registry.md |
| Top-level MD (5) | CONTEXT, ROADMAP, KNOWN_ISSUES, CONTRIBUTING, CLAUDE_REFERENCE | Info absorbed into CLAUDE.md or irrelevant |
| alpha/ package (8 files) | factors/, explain.py, horizon_router.py | Consolidated into modeling/alpha.py + fraud/rules.py |

---

## What Gets Reused (not rewritten)

| Current file | Stays at | Reason |
|---|---|---|
| `pipeline/step01-05` | Same location | Already refactored, tests pass |
| `pipeline/feature_library.py` | Same location | Foundation of everything |
| `scripts/_shared/backtester.py` | `backtest/engine.py` (move only) | 1,023 lines, well-tested |
| `scripts/modeling/train_models.py` | `modeling/train.py` (move + rename) | Works, meets targets |
| `scripts/modeling/generate_oof_scores.py` | `modeling/score_oof.py` (move) | Walk-forward logic is correct |
| `scripts/quality/bias_audit.py` | `quality/bias_audit.py` (move) | 4 checks all pass |
| `tests/*` | Same location | 341 tests, fix imports only |

---

## Token-Control Rules

1. Do not re-read parquet schemas if already in context.
2. Avoid full-file reads over 200 lines without justification.
3. Each session starts with targeted grep, not broad exploration.
4. End sessions with handoff prompt — next session needs no prior context.
5. Don't re-audit completed phases.
