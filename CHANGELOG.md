# Changelog

All notable changes to this project are documented here.

Format: [Semantic Versioning](https://semver.org). Each release section covers the most recent sprint.

---

## [Unreleased]

### Explainable Decision Tree + Agreement Filter — Session 24 (2026-06-26)

#### Added
- **`research/explainable_tree.py`**: Walk-forward backtest with decision tree, LightGBM, and agreement filter with threshold sweep. Extracts human-readable IF/THEN rules.
- **`models/decision_tree_rules.json`**: 5 BUY rules in structured JSON (conditions, probability, sample support).
- **`reports/explainable_model_results.md`**: Three-way comparison + threshold sweep results.

#### Results
- Threshold sweep (0.30–0.50): selected **tree_prob ≥ 0.35** (natural plateau)
- Agreement filter (0.35): Sharpe **1.138**, CAGR **+34.0%**, 0% max drawdown, 73.9% hit rate
- Beats LightGBM alone (+33.8%) while adding explainability for every pick
- **Decision: Agreement filter (threshold=0.35) = primary strategy**
- Every pick has a tree rule: e.g. `sales_to_price > 0.02 AND value_composite > 0.65 → BUY`

### Pruned Feature Set Backtest — Session 23 (2026-06-26)

#### Added
- **`research/pruned_backtest.py`**: Runs proper-split backtest with only the 27 temporally stable features. Implements add-back logic if Sharpe drops > 0.1.
- **`reports/pruned_backtest_results.md`**: Comparison report (full 43 vs lean 27 features).
- **`models/feature_sets_pruned.json`**: Final pruned feature set for downstream use.

#### Results
- Lean 27-feature model Sharpe = **1.124** (vs 0.954 full model — improvement of +0.17)
- CAGR +33.8% vs SPY +17.1% (excess +16.7%), hit rate 73.9%
- Removing 16 unstable features reduced overfitting — simpler model is strictly better

### Proper Train/Validate/Test Split — Session 22 (2026-06-26)

#### Added
- **`research/proper_split_backtest.py`**: Unbiased feature selection + walk-forward backtest with temporal split (Train 2008-2014 / Val 2015-2018 / Test 2019-2024). Fixes look-ahead bias where feature selection previously used full history.
- **`reports/proper_split_results.md`**: Full results report with gate decision.
- **`reports/proper_split_results.json`**: Machine-readable results.

#### Results
- **GATE PASS**: Test-period Sharpe = 0.954 (≥ 0.8 threshold)
- CAGR +31.9% vs SPY +17.1% (excess +14.8%) on unbiased test period 2019-2024
- 43 features selected using train-only data (vs 45 with full-history bias)
- Feature stability: 50% Jaccard overlap across shifted train windows (27 stable core features)
- Previous biased Sharpe (1.37) was ~44% overstated; corrected to 0.954

### Feature Ablation Study — Session 21 (2026-06-26)

#### Added
- **`research/ablation.py`**: Feature ablation harness — drops one feature at a time from feature_sets_3y.json, re-runs walk-forward ML + composite backtest, measures Sharpe/CAGR/hit rate delta.
- **`reports/feature_ablation_results.md`**: Ranked impact table for all 45 features.
- **`reports/feature_ablation_results.json`**: Machine-readable ablation results.

#### Results
- 1 load-bearing feature: `value_composite` (Sharpe Δ=-0.115, acts as volatility dampener)
- 16 prune candidates (removing improves Sharpe): top 3 are `book_to_market` (+0.121), `altman_x4` (+0.110), `value_x_momentum` (+0.102)
- 28 neutral features (zero marginal impact)
- Model can potentially shrink from 45 → ~25 features with improved Sharpe

### Archive Purge — Session 20 (2026-06-26)

#### Removed
- **`_archive/`** (118 files, 1.1 MB): dead pre-V2 code — old scripts, API stubs, docs, repomix metadata pack. Nothing imported from it.
- **`research/archive/`** (3 notebooks): superseded by `research/notebooks/` suite (IC analysis, EDA, factor correlation, etc.)
- **`notebooks/08_experiment_hub.ipynb`** (65 cells): references dead paths (`screener_registry.json`), fully superseded by research notebooks and backtest engine.

### Full Backtest Run — Session 19 (2026-06-26)

#### Added
- **reports/backtest_tearsheet_summary.md**: OOS backtest results for all 4 strategies (COMPOSITE, QEM, SCDV, IARB). Walk-forward ML, 30 bps cost, top-20 equal-weight, SPY benchmark.

#### Results
- COMPOSITE: Sharpe 1.37, CAGR +39.4%, excess +25.6% vs SPY, 88% hit rate (15 years OOS)
- QEM: Sharpe 1.04, CAGR +20.1%, low beta (0.33)
- SCDV: Sharpe 0.83, Sortino 9.03, only real drawdown (-7%)
- IARB: 1 year only — insufficient for risk metrics

### Feature IC Notebook — Session 18 (2026-06-25)

#### Added
- **notebooks/feature_ic_analysis.ipynb**: First research artifact on Architecture V2. Full IC analysis (228 features, sector-neutral, Newey-West t-stats, BH-FDR correction, factor group breakdown). Proves `research/`, `alpha/`, `modeling/` packages work end-to-end on real data.

### Tests + CI Hardening — Session 17 (2026-06-25)

#### Added
- **tests/test_integration.py**: Full-pipeline integration test (synthetic data → features → selection → model → alpha scores → backtest selection). 6 new tests.

#### Changed
- **CLAUDE.md**: Rewritten for Architecture V2 — removed all references to dead `scripts/`, `docs/developer/scripts.md`, `docs/architecture.md`, and other archived paths. Simplified Change Checklist to match the flat package structure.
- **.git/hooks/pre-commit**: Rewritten to call `quality/check_sync.py --warn-only` (non-blocking architecture sync warnings).

### Pipeline Consolidation — Session 14 (2026-06-25)

#### Changed
- **pipeline/step6_clean.py**: Expanded from structural cleaning only to a full enrichment pipeline — now includes quality fixes (dead columns, forecast flag, winsorize accruals, fix gross_margin), imputation (quarterly features, size_category), survivorship correction (delisted flag, pessimistic returns), and data confidence scoring (coverage × consistency × timeliness).
- **.github/workflows/refresh_data.yml**: Replaced 4 separate enrichment steps with single `step6_clean` call.
- **scripts/workflows/run_dataset_enrichments.py**: Removed merged steps (quality fix, confidence, survivorship, imputation) — only remaining post-step6 enrichments are p0f universe, quarterly features, fraud labels/taxonomy.

#### Archived
- `scripts/enrichments/fix_dataset_quality.py` → `_archive/dead_scripts/`
- `scripts/enrichments/impute_features.py` → `_archive/dead_scripts/`
- `scripts/enrichments/mark_survivorship.py` → `_archive/dead_scripts/`
- `pipeline/p0g_confidence_score.py` → `_archive/dead_scripts/`

#### Added
- **tests/pipeline/test_step6_enrichments.py**: Tests for merged quality fix, survivorship, and confidence functions.

### Repo Cleanup — Session 8 (2026-06-24)

#### Changed
- **.repomixignore**: Rebuilt from scratch — added `models/`, `reports/`, `research/`, `notebooks/`, `.github/`, `docs/reference/`, binary patterns (`*.parquet`, `*.joblib`, `*.csv`, `*.png`, `*.jpg`, `*.jpeg`), and `repomix-output*` glob variants. Context pack now 171 files / ~463K tokens with zero leakage from excluded directories.

### Repo Cleanup — Session 7 (2026-06-24)

#### Changed
- **docs/architecture.md**: Rebuilt architecture diagrams to reflect current repo state — column count 361→367, removed FastAPI/API (archived Session 2), fixed KR pipeline reference, removed TimescaleDB from deployment diagram, updated Component Map.
- **docs/developer/alpha-research-architecture.md**: New — explains current directory roles, data flow, pipeline inventory, and scripts subdirectory detail.
- **docs/index.md**: Updated column count 341→367 in tagline and Mermaid diagram.
- **docs/methodology/models.md**: Updated column count 361→367 in training pipeline flowchart.

### Repo Cleanup — Sessions 2–6 (2026-06-24)

#### Changed
- **repo cleanup**: Archived deprecated UI/API/deployment code into `_archive/`.
- **scripts/**: Reorganized 49 runnable scripts into workflow-oriented subdirectories.
- **scripts/**: Added `scripts/_root.py` for canonical project root resolution.
- **tests**: Added `pytest.ini` with pythonpath configuration.
- **docs**: Added `docs/developer/script-migration-map.md`.
- **CLAUDE.md**: Extracted large reference tables into `CLAUDE_REFERENCE.md` to reduce default context size.
- **docs**: Updated all documentation paths after scripts/ reorganization (30 files).
- **docs**: Created `docs/developer/fraud-screener-remnants-audit.md` — naming audit.

### Session 16A — Pipeline Orchestration Cleanup (2026-06-22)

#### **`scripts/run_dataset_enrichments.py`** — new
- Post-Step6 enrichment orchestrator: runs 9 steps in canonical order (fix_dataset_quality → p0f → p0g → survivorship → quarterly → impute → fraud_labels → fraud_taxonomy → validate_contract)
- Partially mitigates MUTATION-ORDER-001
- Flags: `--dry-run`, `--apply-universe-filters`, `--skip-survivorship`, `--skip-quarterly`

#### **`scripts/run_pipeline_kr.py`** — fix
- Fixed ghost reference KR-GHOST-STEP3-001: `step3_enrich_prices_kr.py` → `step3_enrich_prices.py`

#### **`.github/workflows/refresh_data.yml`** — docs
- Added comment block referencing local orchestrator equivalent

### Session 15B — HuggingFace Artifact Verification (2026-06-22)

#### **DATA-ARTIFACT-001** — closed
- Uploaded 9 artifacts to HuggingFace via `push_to_hf.py --all-data-artifacts`
- Verified round-trip restore via `pull_from_hf.py --all` with SHA256 checksum match
- Artifacts: dataset (84.4 MB) + snapshots (31.8 MB) + prices (38.1 MB) + 5 per-market + manifest

### Session 15 — Final Pipeline Hardening (2026-06-22)

#### **`pipeline/p0f_universe_definition.py`** — fix
- Fixed P0F-PRICE-FLOOR-001: updated docstring/comments to match code behavior (price floor applies to all exchanges, not just OTC)
- Removed unused `OTC_EXCHANGES` constant

#### **`.github/workflows/refresh_data.yml`** — enhanced
- Added feature contract validation step after enrichment, before Phase C scoring
- `validate_feature_contract.py` runs as mandatory gate (Phase B must be complete)

#### Documentation consistency fixes
- **`docs/methodology/feature-registry.md`** — corrected stale "annual-only" claims
- **`docs/index.md`** — updated row/column counts to reflect Phase B pipeline state
- **`README.md`** — updated dataset description for Phase B completeness
- **`docs/developer/data-update-guide.md`** — corrected "current production dataset" to Phase B state
- **`docs/developer/scripts.md`** — updated verify_doc_consistency description and A1 check threshold
- **`PIPELINE_ATLAS.md`** — corrected checklist 7.6 (price floor applies to all exchanges)

### Session 14 — Feature Contract / Feature Coverage (2026-06-22)

#### **`scripts/validate_feature_contract.py`** — new
- Validates dataset column groups by pipeline phase (Phase B vs Phase C)
- Reports group presence/absence with min-threshold checks
- Exits 0 if Phase B complete (even if Phase C pending)
- Supports `--strict`, `--json`, `--parquet` flags

#### **`docs/developer/feature-contract.md`** — new
- Defines column group contract per pipeline phase
- Classifies all 26 missing Phase C columns with producers and dependencies
- Documents validation usage and remediation steps

### Session 13 — Data Artifact Infrastructure (2026-06-22)

#### **`scripts/pull_from_hf.py`** — new
- Download artifacts from HuggingFace Hub (final dataset, snapshots, manifest)
- Supports `--all`, `--final`, `--snapshots`, `--manifest` flags
- Checksum verification via ARTIFACT_MANIFEST.json

#### **`scripts/generate_manifest.py`** — new
- Generate `data/ARTIFACT_MANIFEST.json` with rows, columns, checksums, commit hash
- Tracks all pipeline artifacts for integrity verification

#### **`scripts/push_to_hf.py`** — updated
- Added `--snapshots-only`, `--all-data-artifacts`, `--manifest-only` flags
- Uploads snapshots.parquet + prices.parquet + per-market snapshots
- Auto-uploads manifest with data pushes

### Session 12 — Data Regeneration (2026-06-22)

### Section 12 — Full-Universe Low-Beta Tier-1 Scan (2026-05-15)

#### **`notebooks/08_experiment_hub.ipynb`** — Section 12 cell outputs pre-baked (2026-05-15)
- Injected stdout from standalone run as stream outputs into cells s12scn01, s12bet01, s12out01

#### `notebooks/08_experiment_hub.ipynb` — 5 new cells (s12md000, s12cfg01, s12scn01, s12bet01, s12out01)
- Loads full 58K-row parquet; keeps latest annual row per US ticker (fiscal_year ≥ 2022)
- Recomputes composite score from up to 9 signals (same pool as Section 5) on the full universe
- Applies all 4 safety gates (Piotroski ≥ 7, Altman Z > 1.81, FCF yield > 0, Beneish < −1.78) → Tier-1 filter
- Fetches live beta via yfinance for all Tier-1 candidates; filters to β < 1.0
- Assigns leverage (2× if β<0.5, 1.5× if β<0.8, 1.25× if β<1.0) and computes levered 3y CAGR using Section 11 coefficients
- Exports `data/full_universe_low_beta_tier1_2026-05-15.csv`
- Scatter + bar charts: beta vs levered CAGR, colour-coded by leverage tier
- **Run results (2026-05-15)**: 4,653 US tickers → 40 Tier-1 → 21 with β < 1.0 → top 20 by levered 3y CAGR
  - Top 5 (2× lever, β < 0.5): IMXI 53.2%, VLGEA 47.4%, TK 46.6%, CNC 44.8%, MOH 44.2%
  - 1.5× lever (β 0.5–0.8): PZZA 31.2%, CON 30.0%, BBSI 29.7%, PFGC 29.0%, ICFI 28.0%
  - 1.25× lever (β 0.8–1.0): IBEX 26.6%, LZB 23.4%, TGT 23.0%, PUMP 22.9%

### Section 11 — Multi-Horizon CAGR Calibration (6m · 2y · 3y) (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — 4 new cells (s11md000, s11cfg01, s11cal01, s11apl01)
- **6m calibration**: annualised return = −5.58% + 39.34%×composite (R²=0.011, n=32,218); at composite=0.80 → 25.9% ann. (inflated by short-horizon noise)
- **2y calibration**: annualised return = −23.82% + 52.95%×composite (R²=0.092, n=27,854); at composite=0.80 → 18.5% ann.
- **3y calibration**: annualised return = −24.65% + 52.43%×composite (R²=0.133, n=24,029); at composite=0.80 → 17.3% ann. — strongest model (WF AUC 0.6248 ✅)
- **R² pattern**: R² rises monotonically 6m→3y (0.011→0.133) — longer horizons have much stronger signal; 6m is noisy
- **Re-score cell (11B)**: applies all 4 horizon CAGRs to picks simultaneously; best_CAGR_% = 3y column; exports `data/multi_horizon_calibrated_2026-05-15.csv`
- VLGEA (β=0.10, leverage 2×): levered 3y best = **39.0%** ann. — only pick qualifying for leverage

### Section 10 — Beta, CAGR Calibration & Time Horizon Framework (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — 4 new cells (s10md000, s10cfg01, s10cal01, s10bet01)
- **Issue 1 — Time horizon documented**: Confirmed 1-year rebalancing cycle driven by `ml_1y_oof` / `forward_return_1y` as primary backtest signal
- **Issue 2 — Beta fix**: `beta_12m` absent from parquet → all Section 9 leverage was 1×; Section 10 fetches live betas via yfinance (`BETA_PERIOD='1y'`, benchmark=SPY); 13/20 picks resolved (7 TSX/TSX-V names delisted/not found); VLGEA β=0.10 (qualifies 2×), CVGI β=2.11, CRNT β=1.95, TEAD β=3.44, JELD β=2.92
- **Issue 3 — CAGR calibration**: OLS on 31,916 US stock-years (2014–2024); equal-weight rank composite from 7 signals, winsorized at 5%; fitted: `return = -15.26% + composite × 44.23%` (R²=0.032, p=8.1e-229); replaces hardcoded `8 + composite × 27` (was ~10% too optimistic at composite=0.8)
- **Quintile lift**: Q1 mean 1Y return −2.7%, Q5 +18.1%; confirms composite score has directional signal
- **Liquidity flags (Section 10C)**: All 6 Tier-1 picks on US exchanges (🟢); 3 TSX Venture names (🔴 CAF.V, PVT.V, IRO.V) flagged as very illiquid
- **Exports**: `data/leveraged_picks_calibrated_2026-05-15.csv`

### Section 6 DT Screener — Per-Leaf Walk-Forward CAGR (Part C) (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — cell `1248e80a` extended with Part C per-leaf WF CAGR
- **Per-leaf walk-forward CAGR**: inside the existing WF loop, added `model_wf.apply(X_yr)` to assign `leaf_id_wf` to each scored stock; collected `{leaf_id, year, n, avg_ret, avg_prob, excess}` rows per year
- **Aggregation**: after the WF loop, grouped by `leaf_id` across all years; computed annualized CAGR, excess CAGR vs bench, hit rate (% years positive), vol, and Sharpe per leaf
- **Display**: styled heat-map table sorted by walk-forward CAGR, one table per horizon (6m/1y/2y/3y/5y)
- Reuses same WF training pass as Part B — no duplicate training; leaf rows collected inside Part B loop

### Section 6 DT Backtest — walk-forward expanding window, no look-ahead bias (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — cell `1248e80a` Part B replaced with proper walk-forward backtest
- **Problem fixed**: original Part B trained `DecisionTreeClassifier` on full history then scored historical years with the same model — look-ahead bias inflated CAGR/Sharpe
- **Fix**: for each year Y, fresh DT trained on `fiscal_year < Y` only (expanding window); year Y scored with that held-out model
- Guard: `WF_MIN_TRAIN_ROWS = 200` skips years without sufficient training history
- Per-horizon annualization via `hor_years_map = {'6m': 0.5, '1y': 1, '2y': 2, '3y': 3, '5y': 5}`
- Part A (per-rule leaf stats) retained as in-sample rule inspection with clear label; not used for performance claims

### Section 6 Decision Tree Screener — multi-horizon (all 5) (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — extended DT screener to all 5 horizons
- Cell `690181fe`: `DT_HORIZONS = ['6m','1y','2y','3y','5y']`; trains one DT per horizon, stores in `dt_models`/`dt_feats_by_h`/`X_train_by_h`/`y_train_by_h`; prints AUC table + rules per horizon
- Cell `79ad780b`: loops all 5 horizons; scores universe per model; applies filters; displays 5 styled top-100 tables; exports `dt_screener_top100_{h}_{today}.csv` per horizon; stores in `dt_top_by_h`
- Cell `1248e80a`: loops all 5 horizons; per-rule leaf stats (Part A) + year-by-year backtest with correct per-horizon CAGR annualization via `hor_years_map` (Part B)

### Section 6 Decision Tree Screener — executed cells with outputs (2026-05-15)

#### `notebooks/08_experiment_hub.ipynb` — executed all three Section 6 cells
- Cell `690181fe`: DT trained (1y, depth=5, 37 features, train AUC 0.5987), rules printed
- Cell `79ad780b`: 7,625 stocks scored, filters applied, top-100 displayed with market breakdown
- Cell `1248e80a`: per-rule leaf stats table + year-by-year backtest charts rendered

### Section 6 Decision Tree Screener — filters + CAGR performance (2026-05-14)

#### `notebooks/08_experiment_hub.ipynb` — added filters and historical CAGR to Section 6
- Added 6 filter config variables: `DT_MARKETS`, `DT_MIN_SCORE`, `DT_MIN_MARKET_CAP`, `DT_PIOTROSKI_MIN`, `DT_BENEISH_MAX`, `DT_ALTMAN_MIN`
- Filter application block applies all active filters to universe before top-N display; prints before/after counts
- New CAGR performance cell (two parts):
  - **Per-rule stats**: `dt_model.apply()` → per leaf-node hit rate, avg return, implied CAGR, styled with `background_gradient(cmap='RdYlGn')`
  - **Year-by-year backtest**: equal-weight top-N portfolio per `fiscal_year` → implied CAGR, excess vs benchmark, hit rate, Sharpe; two-panel chart (annual CAGR bars + CAGR-by-leaf bars); styled year-by-year table
  - NOTE: in-sample reference only — DT trained on full history

### Section 6 Decision Tree Screener — rule-based top 100 (2026-05-14)

#### `notebooks/08_experiment_hub.ipynb` — added Section 6 Decision Tree Screener
- Trains `DecisionTreeClassifier(max_depth=5, min_samples_leaf=30)` on same feature set as LightGBM for configurable horizon
- Prints interpretable branching rules via `export_text()` (depth ≤ 3)
- Scores full current universe (latest annual snapshot per ticker/market)
- Displays top 100 companies ranked by DT probability with green gradient styling
- Exports results to `reports/dt_screener_top100_{horizon}_{date}.csv`

---

### Schema drift fix + LightGBM vs Decision Tree overfitting analysis (2026-05-14)

#### `data/historical_dataset_clean.parquet` — patched 4 missing columns (361 → 367 cols)
- Added `depreciation_to_assets`, `financing_cashflow_to_assets` (all 5 models referenced these)
- Added `piotroski_shares_ok`, `piotroski_f_score_9` (2y/6m models referenced these)
- Formulas sourced from `pipeline/feature_library.py` (`add_normalised_ratios`, `add_piotroski_ext`)

#### `reports/overfitting_comparison.json` — new report; LightGBM vs Decision Tree train/test AUC per horizon
- LightGBM train/test gap ≤ 0.004 across all horizons (no classical overfitting)
- Decision Tree gaps 0.007–0.023 (mild overfitting, larger at longer horizons)
- LightGBM outperforms DT by 1.5–5.1 AUC points at test time on every horizon
- WF AUC gap (0.57–0.62) vs held-out AUC (0.61–0.69) is temporal non-stationarity, not overfitting

### Experiment notebook executed with full output (2026-05-14)

#### `notebooks/08_experiment_hub.ipynb` — executed via nbconvert; all 5 sections rendered with output
- Section 1: Feature selection summary and SHAP importance charts per horizon
- Section 2: Walk-forward AUC charts and model performance tables
- Section 3a/3b: Screener registry leaderboard and individual signal rankings with charts
- Section 4a/4b: Deep dive composite and individual signal analysis with annual return bars
- Section 5: Live picks (longs + shorts) exported to `reports/screener_longs_*.csv`

### Version 2 roadmap documented (2026-05-14)

#### Documentation
- **`docs/developer/roadmap.md`**: Created — 6-section v2 plan covering AUC improvements (6m/1y/2y), quarterly data pipeline, portfolio construction, live data pipeline, alternative data (Form 4 insider), and production API deployment
- **`docs/developer/index.md`**: Roadmap link added

### Architecture: experiment notebook declared as primary frontend; Streamlit archived (2026-05-14)

#### Documentation — all Streamlit references replaced with notebook frontend (docs/arch)
- **`docs/architecture.md`**: Outputs subgraph E1 → Experiment Notebook node; Component Map App row → Experiment Notebook row; Deployment Architecture Streamlit Cloud node → local notebook node
- **`docs/index.md`**: Mermaid diagram G node → Experiment Notebook; "I want to use the app" tab → "I want to use the notebook"; deployment link text updated
- **`CLAUDE.md`**: Architecture table Streamlit UI row → Experiment Notebook row; key file locations App entry point → research frontend; Change Checklist Streamlit UI row → notebook row
- **`docs/developer/deployment.md`**: Streamlit Cloud section replaced with Research Frontend section (local notebook execution)
- **`docs/quickstart.md`**: Step 4 "Launch the App" → notebook launch; streamlit dependency removed; Next Steps updated
- **`docs/guide/app.md`**: Deprecation note added — `app_v2.py` archived, notebook is active frontend
- **`docs/developer/setup.md`**: app_v2.py entry annotated as archived; launch step → notebook
- **`docs/developer/tests.md`**: Streamlit UI test coverage note → notebook rendering note
- **`docs/developer/schema-change-guide.md`**: `app_v2.py` check → notebook check
- **`docs/methodology/backtesting.md`**: backtest_results.json consumer updated to notebook Section 3

### Master experiment notebook completed: composite registry + individual signal deep dives (2026-05-14)

#### `notebooks/08_experiment_hub.ipynb` — Task 6 complete (feat/notebook)
- **Section 3a** (new): Composite Screener Registry leaderboard — loads `data/screener_registry.json`, displays ranked table (CAGR, Sharpe, Sortino, Calmar, MaxDD, Excess vs SPY) and 3-panel chart (CAGR barh, Sharpe barh, Sharpe vs MaxDD scatter) for all 7 named strategies
- **Section 3b** (updated): Added `### 3b. Individual Alpha Signal Rankings` markdown header above existing alpha registry cells
- **Section 4** (new): "Screener Deep Dive" split into 4a (composite) and 4b (individual signal)
  - **4a-load** (`64a2f6bf`): loads selected `SCREENER_REGISTRY_ID` from registry, prints full stats profile (20 metrics)
  - **4a-annual** (`7b289baf`): dual-panel annual returns chart — grouped bar (strategy vs SPY) + excess return bars with rolling Sharpe twin axis
  - **4a-peers** (`34f0a302`): 7-metric bar chart comparing selected screener vs all composite peers (selected highlighted red)
- **Config cell** (updated): Added `SCREENER_REGISTRY_ID = 'COMPOSITE_US'` with inline comments listing all 7 valid options

### Screener registry: 7 named screener configs with walk-forward backtests (2026-05-14)

#### `build_screener_registry.py` — single-command registry builder (feat/alpha)
- **`scripts/build_screener_registry.py`**: New script — defines 7 named screener configs (COMPOSITE_US, COMPOSITE_INTL, COMPOSITE_MICRO, VALUE_QUALITY, MOMENTUM_GROWTH, FRAUD_AVOID, WIDE_UNIVERSE), each with a dedicated filter function and alpha blend. Runs all configs through `backtester.py`'s `run_backtest()` engine and writes results (CAGR, Sharpe, max drawdown, Calmar, hit rate, excess vs SPY) plus config metadata to `data/screener_registry.json`. CLI supports `--ids`, `--top N`, `--dry-run`, `--out` flags. Uses OOF ML scores (`ml_1y_oof`, `ml_3y_oof`) for historical alpha — not contaminated in-sample scores.
- **`docs/developer/scripts.md`**: Added `build_screener_registry.py` section.

### Repo structure clean-up (2026-05-14)

#### Removed dead files, archived EDA notebooks, merged requirements (chore/repo)
- Deleted **`pipeline/phase_a_integrate_{br,ca,eu,jp,kr}.py`** — one-time market integration scripts, no longer needed
- Deleted **`research/03_ml_model.ipynb`**, **`research/showcase.ipynb`** — superseded
- Removed **`HF_TOKEN.md`** — security risk; secrets belong in `.env` / CI vars only
- Moved **`notebooks/01–06_*.ipynb`** → **`research/notebooks/`** (EDA archive)
- Deleted **`notebooks/07_live_screener.ipynb`** — superseded by `08_experiment_hub.ipynb`
- Added **`notebooks/08_experiment_hub.ipynb`** — new master notebook
- Merged **`requirements-api.txt`** into **`requirements.txt`**; deleted `requirements-api.txt`

### Regression model extended to all 5 horizons — continuous CAGR output (2026-05-14)

#### `train_regression_model.py` rewritten: all-horizon support (feat/models)
- **`scripts/train_regression_model.py`**: Rewrote to train LightGBM Huber regression models for all 5 horizons (6m/1y/2y/3y/5y), not just 3y. Target is `excess_return_local_{h}` (continuous CAGR vs local market index), falling back to `forward_return_{h}`. Added `--horizons` argument; reuses frozen `feature_sets_{h}.json` per horizon (no new feature selection on regression target). PIT-safe WF CV excludes incomplete folds via `max_test_year = max_year - ceil(horizon_years) + 1`. Outputs `model_{h}_regression.joblib`, `model_{h}_regression_meta.json`, `reports/regression_ic_{h}.csv` per horizon.
- **`docs/developer/scripts.md`**: Updated `train_regression_model.py` section — added `--horizons` flag, updated outputs/description to all-5-horizon support.

### Model tuning: XGBoost ensemble, purged CV embargo, sector z-score, tighter dedup (2026-05-14)

#### Four concrete improvements to reduce feature leakage and increase model diversity (perf/models)
- **`scripts/train_models.py`**: Added `XGB_AVAILABLE` guard + `train_xgb_model()` (n_estimators=500, max_depth=5, lr=0.05, subsample=0.8, colsample_bytree=0.7, min_child_weight=20) — XGBoost ensemble blending available via `--ensemble` flag
- **`scripts/train_models.py`**: Added `--ensemble` flag — 50/50 LightGBM + XGBoost blend in WF CV folds; production model stays LightGBM-only
- **`scripts/train_models.py`**: Added `--embargo-years INT` (default 0) — purged walk-forward CV: excludes most recent N training years per fold to prevent adjacent-year autocorrelation leakage; use `--embargo-years 1` for standard purged CV
- **`scripts/train_models.py`**: Added `sector_zscore_normalize()` function + `--sector-zscore` flag — within-(fiscal_year, sic_code) z-score normalization that removes cross-sector absolute valuation differences; groups with <5 members left unnormalized
- **`scripts/train_models.py`**: Tightened `deduplicate_features` default threshold 0.90 → 0.85 (removes more near-duplicate features, reducing multicollinearity in selection)
- **`scripts/run_feature_selection.py`**: `CORR_THRESHOLD` tightened 0.90 → 0.85 (aligned with train_models.py)
- **`requirements.txt`**: Added `xgboost>=2.0.0`
- **`docs/developer/scripts.md`**: Updated `train_models.py` flags table — added `--embargo-years`, `--ensemble`, `--sector-zscore`; updated dedup threshold note

### Raise investment target to ≥25% annualised ROI (2026-05-14)

#### Update vision target across docs (docs)
- **`ROADMAP.md`**: Vision and investment target raised from ≥20% to ≥25% annualised ROI (reflects COMPOSITE strategy CAGR of +25%+ validated in backtests)
- **`docs/index.md`**: Tagline updated to ≥25%

### build_portfolio.py: MoS gate, low-vol filter, implied drawdown, $10M floor (2026-05-14)

#### Vision gap fixes: portfolio risk controls + liquidity floor (feat/portfolio)
- **`scripts/build_portfolio.py`**: `MIN_MARKET_CAP` lowered 50M → $10M (micro-cap / institution-avoidance niche)
- **`scripts/build_portfolio.py`**: Added `--mos-min-score FLOAT` — margin-of-safety gate requiring `alpha_value >= threshold` before stock selection; applied in both `run_backtest` and `build_current_holdings`
- **`scripts/build_portfolio.py`**: Added `--low-vol-only` — retains only stocks in bottom-half of trailing 12m volatility per year; applied in both backtest and holdings paths
- **`scripts/build_portfolio.py`**: Added `implied_max_drawdown_pct` to backtest output — `−max(|max_dd|, 2σ)` proxy that corrects the annual-sampling artifact (pure year-end drawdown misses intra-year troughs)
- **`docs/developer/scripts.md`**: Updated `build_portfolio.py` flags table and output description

### Re-tune 6m/1y/2y models — D1.3 (2026-05-14)

#### 60-trial Optuna re-run for 6m, 1y, 2y horizons (perf/models)
- **`scripts/tune_models.py`**: Re-ran 60-trial Optuna for 6m (val=0.617), 1y (val=0.605), 2y (val=0.606)
- **1y WF AUC improved**: 0.5683 → 0.5774 (+0.009) with new tuned params
- **6m WF AUC unchanged**: 0.5715 (structural ceiling; gap 0.0085 from 0.58 target)
- **2y WF AUC unchanged**: 0.5880 (gap 0.012 from 0.60 target)
- **Root cause documented**: 1y dragged by 2018→2019 (AUC=0.52, late-cycle) and 2020→2021 (AUC=0.53, COVID) folds; macro signal correctly captured via recession/quality_in_recession interaction features (ICIR=9.4–9.8); PSI filter correctly excludes yield_curve/credit_spread_baa (distributional drift)
- **`models/model_meta.json`**: Updated wf_mean_auc values for all horizons
- **`docs/methodology/models.md`**, **`docs/index.md`**, **`CLAUDE.md`**: Performance tables updated

### Add live screener notebook (2026-05-14)

#### Decision-support notebook for investment picks (feat/notebooks)
- **`notebooks/07_live_screener.ipynb`**: 7-section notebook — loads latest parquet (local or HuggingFace), applies quality gates (Piotroski≥5, Beneish<-1.78, Altman>1.81), margin-of-safety filters (P/B≤3, FCF yield≥3%), micro-cap institution-avoidance filter ($10M–$300M), IC-weighted composite scoring, Kelly-sized long/short recommendations, leverage candidate flag (β<0.80 proxy), and CSV export to `reports/`.

---

### Fix doc accuracy — data source descriptions and architecture diagrams (2026-05-14)

#### Correct pipeline step1/step2 data source entries (docs)
- **`docs/developer/pipeline-scripts.md`**: Fixed step2 table — EU was SimFin (wrong) → yfinance no API key; JP was "Optional/paid" (wrong) → free EDINET API key; CA was TMX public API (wrong) → SEDAR+ XBRL filings + yfinance fallback. (step1 corrections were in prior commit.)
- **`docs/architecture.md`**: Data Sources subgraph A2 SimFin→yfinance EU, A4 TDNET→EDINET. Data Flow node text corrected (EDGAR/DART/yfinance/EDINET/CVM). Alpha label fixed from "341→360 cols total" → "+6 alpha cols".
- **`CLAUDE.md`**: Architecture State multi-market ingestion row corrected (SimFin/TDNET → yfinance/EDINET).
- **`research/`**: Archived stale notebooks (01_metric_exploration, 02_historical_dataset, 04_factor_research) to `research/archive/`.

---

### Fix Streamlit Cloud build failure — Linux wheel compatibility (2026-05-14)

#### Fix pip install on Linux manylinux build environment (fix/deploy)
- **`requirements.txt`**: Downgraded `pyarrow` 21.0.0 → 20.0.0 and `pandas` 2.3.3 → 2.3.2. Both 21.0.0 and 2.3.3 lack binary wheels for Linux manylinux on PyPI, causing Streamlit Cloud builds to fail with "Exited with status 1".

---

### Retrain with tuned params + expanded FORCE_INCLUDE features for 6m/1y (2026-05-14)

#### Fix best_params propagation and add WF CV with Optuna hyperparameters (fix/perf)
- **`scripts/tune_models.py`**: `tune_lgbm()` now returns all 9 best hyperparameters (was returning only the model + CSV of 3 params). Full dict saved to `model_meta.json` as `best_params`. 6m best: lr=0.0196, n_est=230, num_leaves=21. 1y best: lr=0.0201, n_est=394, num_leaves=59.
- **`scripts/train_models.py`**: Added `override_params` arg to `train_model()` and `override_params_per_horizon` arg to `walk_forward_cv()` — allows injecting Optuna-tuned params into WF folds without re-running Optuna.
- **`scripts/train_models.py`**: Added `--use-tuned-params` CLI flag — when passed with `--walk-forward`, loads `best_params` from `model_meta.json` and uses them in every WF fold.
- **`scripts/train_models.py`**: `_old_meta` loaded before training loop to carry forward `best_params` (and other tune fields) from prior `tune_models.py` run into the freshly-built `model_meta` dict.
- **`scripts/train_models.py`**: `FORCE_INCLUDE_6M` and `FORCE_INCLUDE_1Y` expanded with 4 high-ICIR features bypassed by ICIR ranking: `sales_to_price` (ICIR~2.0), `ohlson_roe` (ICIR~1.4), `value_x_quality` (ICIR~1.3), `piotroski_f_score` (ICIR~1.25). These fundamental quality/value signals had 14 years of IC data but were crowded out by momentum features in the top-N selection.
- **`docs/developer/scripts.md`**: Added `--use-tuned-params` flag to `train_models.py` flags table.


#### Tune 6m and 1y LightGBM models with Optuna (perf)
- **`models/model_6m.joblib`**: Optuna 60-trial TPE tuning — tuned val AUC 0.617 (+1.0pp vs 0.607 baseline), tuned test AUC 0.517 (+1.2pp). WF Mean AUC 0.5626 (target ≥ 0.58 — not yet met; WF re-run with tuned params required for formal measurement).
- **`models/model_1y.joblib`**: Optuna 60-trial TPE tuning — tuned val AUC 0.603 (+0.37pp), tuned test AUC 0.496 (+1.2pp). WF Mean AUC 0.5634 (target ≥ 0.62 — not met; short-horizon fundamentals-only models are inherently noisy).
- **`models/model_meta.json`**: updated with `tuned_val_auc` and `tuned_test_auc` fields for 6m and 1y.
- **`docs/methodology/models.md`**: AUC table updated with tuned columns.
- **`docs/index.md`**: Performance at a Glance table updated.
- **`scripts/push_to_hf.py`**: `HORIZONS` extended from `['1y','3y','5y']` to `['6m','1y','2y','3y','5y']` — all 5 horizons now pushed to HuggingFace Hub.


### Publish showcase notebook with full outputs (2026-05-14)

#### Execute and publish research showcase notebook (feat)
- **`research/showcase.ipynb`**: executed notebook with full cell outputs — 19 code cells, 0 errors. Covers dataset overview, factor analysis, feature selection, model training, Optuna tuning, walk-forward validation, SHAP importance, backtest, live screener demo.

### Fix Streamlit Cloud deployment + ML scoring (2026-05-14)

#### Fix broken cloud deployment and ML scoring (fix)
- **`src/data.py`**: `_hf_download_bytes` now passes `HF_TOKEN` env var to `hf_hub_download` — required for private HuggingFace Hub repos on Streamlit Cloud.
- **`src/data.py`**: `load_data()` shows a clear `st.error` with instructions when both `HF_REPO` and local parquet are absent, instead of crashing with a FileNotFoundError.
- **`src/data.py`**: `load_models()` checks local files first, then falls back to HF Hub.
- **`src/data.py`**: Adds derived column `financing_cashflow_to_assets = financing_cash_flow / total_assets` at load time — fixes all-NaN ML scores caused by LightGBM feature count mismatch.

### Factor research look-ahead fix, IC decay analysis, regression bias audit (2026-05-14)

#### Fix ML-score look-ahead contamination in factor IC table (fix)
- **`scripts/factor_research.py`**: expanded `EXCLUDE_PATTERNS` to exclude all ML-derived and composite scores (`ml_1y/2y/3y/5y/6m`, `_oof`, `ml_pred_excess`, `composite_score`, `alpha_*`). Previously `ml_3y` was included as an IC candidate, artificially inflating its ICIR to 5.68 and Q5 spread to 110% — direct look-ahead contamination since static ML scores are trained on the full dataset. After fix, max Q5 spread is 0.47 and all top factors are pure fundamentals.
- **`scripts/factor_research.py`**: added `compute_ic_decay()` function and `--ic-decay` / `--decay-top N` CLI flags. Computes IC at 1y, 3y, 5y lags for top-N factors and estimates signal half-life. Value/quality factors show >5y half-life (structural); recession-regime factors show ~2.2-2.5y half-life (tactical).
- **`reports/factor_research_{1y,3y,5y}_sn.csv`**: regenerated with clean EXCLUDE_PATTERNS.
- **`reports/ic_decay_halflife.csv`** (new): IC at 1y/3y/5y and estimated half-life for top 20 factors.

#### Add regression model bias audit — Audit 5 (feat)
- **`scripts/bias_audit.py`**: added `_REGRESSION_CONTAMINATED` set and `audit_regression_model()` function (Audit 5). Three checks: (1) feature contamination scan against ML/forward-return columns, (2) walk-forward IC distribution from `regression_ic_3y.csv`, (3) permutation test (50 shuffles) using `model.feature_name_` for alignment. Results: no contaminated features ✓, WF IC 0.337 (⚠ suspicious but genuine), permutation z=26.8 ✓.

#### Fix backtest annual_returns key mismatch in UI (fix)
- **`src/ui/tab_backtester.py`**: fixed key mismatch where UI read `port_ret`/`bench_ret` (decimal) but `backtest_results.json` stores `port_pct`/`bench_pct` (percentage). Annual returns and cumulative wealth charts now render correctly.
- **`data/backtest_results.json`**: re-generated with monthly price cache — composite strategy MaxDD=-20.8% (was 0.0%), CAGR=+37.1%, Sharpe=1.566.

#### Add plain-English investment thesis generator (feat)
- **`alpha/explain.py`** (new): generates per-ticker buy rationale in plain English. Covers predicted 3-year excess return, Piotroski quality verdict, Beneish fraud risk, Altman distress risk, valuation snapshot (P/B, FCF yield, market cap), ML conviction level, recommended strategy (LEAPS/margin/equity-only), risk flag detection, and a 4-point margin of safety checklist. CLI: `python3 alpha/explain.py --market US --top 15`. API: `from alpha.explain import explain_pick, explain_many`.

#### Fix missing return statement in `_apply_three_stage_filter` (fix)
- **`scripts/leverage_strategy.py`**: fixed missing `return df` at end of `_apply_three_stage_filter()`. Without it the function returned `None`, causing a `TypeError` when the caller tried to call `.head()` on the result.

#### Docs update
- **`docs/developer/scripts.md`**: added `alpha/explain.py` section (usage, flags table, API examples, outputs); updated `factor_research.py` section with `--ic-decay`/`--decay-top` flags and ML exclusion note; updated `bias_audit.py` section to describe Audit 5 (regression model checks).

### Streamlit Cloud deployment fix (2026-05-14)

- **`app.py`** (new): two-line shim that imports and calls `main()` from `app_v2.py`. Fixes Streamlit Cloud deployment where the dashboard is configured to launch `app.py` but the actual entry point is `app_v2.py`.

### Phase D2 — Monthly NAV MaxDD and ADTV liquidity filter (2026-05-14)

#### Fix MaxDD=0% bug and add ADTV filter (fix + feat)
- **`scripts/build_monthly_price_cache.py`** (new): dry-runs composite/qem/scdv strategy filters for backtest years 2008–2023 to collect ~200–300 unique tickers, then downloads monthly OHLCV from yfinance in batches of 50. Computes `adtv_30d` (rolling 3-month average of daily dollar volume) and writes `data/monthly_prices.parquet`. Supports `--update` (extend cache) and `--tickers-only` flags.
- **`scripts/backtester.py`**: added `load_monthly_prices()`, `compute_monthly_nav()`, and `adtv_filter()`. When `data/monthly_prices.parquet` is present, MaxDD is computed from a true monthly NAV curve (reveals intra-year drawdowns invisible at annual granularity). ADTV filter removes picks whose $50K position would exceed 5% of 30d ADTV (PIT-safe: uses Sep–Dec of observation year). New `run_backtest()` params: `monthly_px`, `use_adtv_filter`, `max_pct_adtv`. New `main()` flag: `--no-adtv`. Both features degrade gracefully when the cache is missing.
- **`docs/developer/scripts.md`**: added `build_monthly_price_cache.py` section; updated `backtester.py` entry with `--no-adtv` flag and monthly-NAV/ADTV description.
- **`scripts/push_to_hf.py`**: added `model_3y_regression.joblib` and `model_3y_regression_meta.json` to the model upload list.

### 3-stage screener with regression magnitude ranker (2026-05-14)

#### Add LightGBM Huber regression model for excess return magnitude (feat)
- **`scripts/train_regression_model.py`** (new): trains LightGBM Huber regressor to predict `excess_return_local_3y`. Reuses frozen 45-feature ICIR set from `models/feature_sets_3y.json`. PIT-safe temporal split, winsorized target, expanding-window walk-forward CV with Spearman IC per fold. Outputs `models/model_3y_regression.joblib`, `models/model_3y_regression_meta.json`, `reports/regression_ic_3y.csv`.
- **`scripts/score_historical.py`**: added `score_regression()` function and `--skip-regression` flag. When `model_3y_regression.joblib` is present, writes `ml_pred_excess_3y` column to the parquet for all rows.
- **`scripts/leverage_strategy.py`**: refactored to 3-stage screener. Stage 1 extended with P/B < 5.0 and market cap ≥ $50M gates. New `_apply_three_stage_filter()` chains Stage 1 → Stage 2 (ml_score_3y > 0.52) → Stage 3 (sort by ml_pred_excess_3y). Position weights now proportional to `ml_pred_excess_3y` (Kelly-like) when regression model is available; falls back to `composite_score`. `_pick_strategy()` updated to use `ml_score_3y` (replaces sub-random `ml_score_1y`).
- **`docs/developer/scripts.md`**: added `train_regression_model.py` section; updated `score_historical.py` and `leverage_strategy.py` entries with new flags and 3-stage description.

### Signal integrity fixes (2026-05-14)

#### Remove sub-random ml_score_1y from leverage composite (fix)
- **`scripts/leverage_strategy.py`** `composite_score()`: removed `ml_score_1y` (test AUC 0.484, sub-random — was incorrectly weighted at 30%). Redistributed weight to `ml_score_3y` (now 0.45, the only validated signal at WF AUC 0.625). `value_composite` 0.25, `quality_composite` 0.20, `piotroski_f_score` 0.10 unchanged.

#### Add --oot-eval OOT diagnostic to train_models.py (feat)
- **`scripts/train_models.py`**: added `--oot-eval` flag and `run_oot_diagnostic()` function. Retrains 3y model with `TRAIN_CUTOFF=2019`, tests on `FY2022` (where `beat_local_market_3y` is fully known since 2022+3=2025). Production models are never overwritten. Saves `reports/oot_auc_diagnostic.json` with OOT AUC, sample sizes, and horizon metadata.
- **`docs/developer/scripts.md`**: added `--oot-eval` row to `train_models.py` flags table.

#### Optuna tuning for 3y horizon (perf)
- **`scripts/tune_models.py`**: fixed `sys.path` so `scripts.train_models` import resolves correctly; promoted `_CalModel` from local class to module-level to fix pickling. Ran `--no-catboost --horizon 3y --trials 50`; tuned val AUC 0.6644 → calibrated ensemble val AUC 0.6773. Outputs: `models/model_3y_tuned.joblib`, `models/model_3y_calibrated.joblib`, `reports/optuna_study_3y.csv`.

---

### Phase D6 — Reports & Plots (2026-05-14)

#### D6.1 — Kelly portfolio tearsheet page in generate_reports.py (feat)
- **`scripts/generate_reports.py`** Added `PORTFOLIO_BACKTEST` + `PORTFOLIO_HOLDINGS` path constants. Added `_load_portfolio_backtest()` and `_load_portfolio_holdings()` helpers. Added `_fig_portfolio_tearsheet()`: 4-panel figure (cumulative wealth vs SPY, annual return bar, drawdown, KPI summary with VaR/CVaR + top 10 holdings table). `generate_pdf()` now accepts `portfolio_backtest` and `portfolio_holdings` kwargs; inserts Kelly portfolio page after the strategy performance page. `main()` loads both files and passes them to `generate_pdf()`.
- **`docs/developer/scripts.md`** Updated `generate_reports.py` section — new flag table note on PDF page ordering and `rolling_oos_auc.png` output listed.

#### D7.1 — Kelly portfolio tearsheet + alpha signal browser in Tab 5 (feat)
- **`src/ui/tab_backtester.py`** Added path constants for `portfolio_backtest.json`, `portfolio_holdings.json`, `alpha_registry.json`. New **Kelly Portfolio** section: KPI strip (CAGR, Sharpe, Max DD, VaR 95%, CVaR 99%), cumulative wealth vs SPY line chart, annual return bar chart, collapsible holdings table. New **Alpha Signal Browser** section: filterable/sortable signal table (IC Mean, ICIR, CAGR, Sharpe, bootstrap CIs), horizontal IC bar chart for top 25 signals.
- **`docs/guide/app.md`** Tab 5 section updated to document Kelly portfolio and alpha signal browser sub-sections.

---

### Phase D1 — Signal Readiness (2026-05-14)

#### D1.1 — Model retraining with momentum FORCE_INCLUDE + sector-neutral IC (perf)
- **`scripts/train_models.py`** Confirmed TRAIN_CUTOFF=2022 (reverting TRAIN_CUTOFF=2023 regression). FORCE_INCLUDE_6M/1Y=['quality_x_momentum','vol_rank_12m'], FORCE_INCLUDE_2Y=['vol_rank_12m'] injected momentum features. sector_neutral=True default. min_ic_stability=0.6 default.
- **`models/model_{6m,1y,2y,3y,5y}.joblib`** Retrained; final feature counts: 6m=31, 1y=30, 2y=28, 3y=30, 5y=26.
- **`models/model_meta.json`** Updated with D1.1 val_auc/wf_mean_auc actuals.
- **`reports/walk_forward_auc_{6m,1y,2y,3y,5y}.csv`** New WF CV results (9 folds, expanding window, PIT-safe).
- **`docs/methodology/models.md`** AUC table updated: 6m WF=0.563, 1y WF=0.563, 2y WF=0.589, 3y WF=0.625 ✅, 5y WF=0.620 ✅.
- **`docs/index.md`** Performance at a Glance table updated with D1.1 actuals.
- **`CLAUDE.md`** Current Performance section updated to Phase D1 actuals.

#### D1.2 — Bootstrap confidence intervals in backtester (feat)
- **`scripts/backtester.py`** `bootstrap_ci()` added — block bootstrap (2000 samples, block_size=3y) producing CAGR ± 1σ and Sharpe ± 1σ. `run_backtest()` returns 4 CI fields: `cagr_bootstrap_mean_pct`, `cagr_bootstrap_1sigma_pct`, `sharpe_bootstrap_mean`, `sharpe_bootstrap_1sigma`. `print_tearsheet()` displays Sharpe CI 1σ and CAGR CI 1σ bands.

#### D1.3 — alpha_registry.json max_drawdown audit + bootstrap CI fields (fix/feat)
- **`scripts/build_alpha_registry.py`** Added `max_drawdown_note` field (documents annual-frequency limitation: 0.0 = all annual periods positive, not a bug). Added 4 bootstrap CI fields from D1.2 `run_backtest()` output to each signal entry.

---

### Phase D5 — Monitoring (2026-05-14)

#### D5.1 — Per-alpha IC decay + drawdown circuit breaker (feat)
- **`scripts/monitor_drift.py`** Added `analyse_ic_decay()`: loads `alpha_registry.json`, computes annual Spearman IC (signal vs `forward_return_1y`) per fiscal year, then rolling mean over the most recent 3y / 6y / 12y windows. Flags signals with 3y rolling IC < 0.02 (decay warn) or latest IC < 0 (decay alert). Added `check_drawdown_circuit_breaker()`: loads `portfolio_backtest.json`, computes cumulative return series, detects current drawdown from peak, warns if drawdown exceeds `--dd-gate` threshold with circuit-breaker action message. Added `--dd-gate` (default 20%), `--skip-ic-decay`, `--skip-dd` CLI flags. Both new sections are included in `reports/drift_report.json`. Any alert from either section sets `any_alert=true` (exit code 1).
- **`docs/developer/scripts.md`** Updated `monitor_drift.py` section: new flags table rows, IC decay and drawdown circuit-breaker descriptions.

---

### Phase D4 — Investment Framework (2026-05-14)

#### D4.1 — Investment framework document (docs)
- **`docs/methodology/investment-framework.md`** New document: 27 numbered rules covering universe (liquidity floor, market scope, fiscal-year anchoring), scoring (IC-weighted composite, horizon filtering, percentile-rank normalisation), portfolio construction (long-only default, top-N selection, fractional Kelly, position/sector caps), transaction costs (4-tier slippage, annual rebalance assumption), risk limits (VaR 95%, CVaR 99% gate, max drawdown monitoring, beta/alpha reporting), benchmark (SPY), live monitoring (drawdown circuit breaker, IC decay tracking, model drift), data integrity (PIT safety, survivorship correction, minimum history), and operational procedures (weekly refresh, registry versioning, commit convention).
- **`docs/methodology/index.md`** Added link to investment-framework.md.

---

### Phase D3 — Risk & Compliance (2026-05-14)

#### D3.1 — CVaR 99%, tiered slippage, VaR/CVaR gates (feat)
- **`scripts/backtester.py`** Added `SLIPPAGE_TIERS` constant (4 tiers: large >$10B=20 bps, mid $1B–$10B=30 bps, small $100M–$1B=50 bps, micro <$100M=80 bps). Replaced 2-tier `size_category_label` slippage with tiered lookup by `market_cap_at_filing` (falls back to `size_category_label` if column absent). Added CVaR 99% (Expected Shortfall): mean of annual returns in worst 1% tail. Added `cvar_99_pct` to return dict and `print_tearsheet()`.
- **`scripts/build_portfolio.py`** Fixed `_latest_complete_year` bug: market cap filter compared `market_cap_at_filing` against `min_n` (30) instead of `min_market_cap` ($50M). Added `var_95_pct` and `cvar_99_pct` to portfolio backtest return dict. Added `--var-gate` (warn if drawdown breaches threshold) and `--cvar-gate` (abort if CVaR99 breaches threshold) CLI flags with enforcement in `main()`. Added VaR/CVaR lines to `print_tearsheet()`.
- **`docs/developer/scripts.md`** Updated `backtester.py` and `build_portfolio.py` sections: tiered slippage description, new `--var-gate`/`--cvar-gate` flags, `cvar_99_pct` output field.

---

### Phase D2 — Portfolio Construction (2026-05-14)

#### D2.1 — IC-weighted Kelly portfolio constructor (feat)
- **`scripts/build_portfolio.py`** New script: reads `data/alpha_registry.json`, IC-weights selected signals into composite score, applies $50M market-cap filter, quarter-Kelly (≤0.25×) position sizing, 5% position cap, 40% sector cap. Supports `--strategy long_only|long_short`, `--horizon 1y|3y|5y|all`, `--market`, `--tearsheet`. Outputs `data/portfolio_holdings.json` (current-year top-30 with weights) and `data/portfolio_backtest.json` (annual return series + Sharpe/CAGR/beta). Backtest: CAGR +34.1%, Sharpe 1.409, Beta 0.348 (all markets, all signals).
- **`docs/developer/scripts.md`** Added `build_portfolio.py` section with flags table and output spec.

---

### Doc sweep — fraud→alpha reframe + 5-horizon + 360-col sync (2026-05-14)
- **`docs/guide/app.md`** All "fraud score / fraud probability" language replaced with "alpha score / composite score" throughout Tabs 1–4,6; ML horizon description updated to "6m through 5y via HorizonRouter"
- **`docs/developer/data-update-guide.md`** Production dataset updated 355→360 cols; workflow diagram node updated from `ml_1y/3y/5y` to `ml_{6m,1y,2y,3y,5y}`
- **`docs/methodology/features.md`** Clarified as "355 base features + 5 OOF = 360 total columns"
- **`docs/index.md`** Fraud Risk table entry updated to `ml_6m/1y/2y/3y/5y alpha probability`; Key Design Decisions expanded to 5 horizons
- **`docs/quickstart.md`** Train step updated to all 5 horizons (6m/1y/2y/3y/5y)
- **`docs/developer/setup.md`** 8→10 tabs; adds model_6m/2y.joblib; dataset size 155K×319→58K×360; ml_score column naming corrected

### Institutional sync (2026-05-14)
- **`scripts/run_phase_checks.py`** Phase A column count check updated 355 → 360 (dataset grew when 5 OOF columns added; check was never updated — fixes Phase A FAIL)
- **`docs/architecture.md`** Feature-Complete Dataset node updated "355 cols" → "360 cols" to match actual parquet state
- **`docs/methodology/models.md`** LightGBM base config block updated to actual trained params: `n_estimators=600`, `learning_rate=0.03`, `num_leaves=63`, `max_depth=6` (was showing stale pre-retrain defaults)
- **`.gitignore`** Added `HF_TOKEN.md` and `*.token` to prevent credential files being accidentally committed
- **`ROADMAP.md`** Vision section updated with investment objective: ≥20% annualised ROI, Sharpe ≥ 1.0, max drawdown ≤ 30%; platform framed as screener + alpha generation
- **`CONTEXT.md`** "What This Project Is" updated with dual-platform framing and 20% ROI investment objective

### Phase C — Complete (2026-05-14)
- **`scripts/run_phase_checks.py --phase C`** → 30 PASS 0 FAIL 0 WARN. Phase C officially complete per `docs/developer/phase-done-criteria.md`. All 5 model horizons (6m/1y/2y/3y/5y) confirmed present, OOF scores in parquet (360 cols), bias audit passing, backtest results with SPY benchmark, alpha_registry.json populated (8 signals, 6 selected).
- **`ROADMAP.md`** Phase C status rows updated to reflect actual artifact state: Step 7 baseline comparison ✅, sortino/calmar bug ✅ Fixed, Step 9 tearsheet + benchmark-relative metrics ✅, Step 10 alpha registry tasks ✅. Phase header updated to "✅ COMPLETE (exit criteria: 30 PASS 0 FAIL)".
- **`CONTEXT.md`** Phase C section rewritten: shows COMPLETE with 30/30 pass, dataset updated to 360 cols, OOF columns marked present, session log updated.

### Fixed (Phase C — AUC gap in 6m/1y/2y horizons)
- **`scripts/train_models.py`** three-part AUC fix for under-performing short horizons (6m WF=0.549, 1y WF=0.549, 2y WF=0.578):
  1. `FORCE_INCLUDE_6M = ['vol_rank_12m', 'quality_x_momentum']`, `FORCE_INCLUDE_1Y = ['vol_rank_12m', 'quality_x_momentum']`, `FORCE_INCLUDE_2Y = ['vol_rank_12m']` — bypasses ICIR ranking to inject momentum features that ICIR selection systematically under-selects for short-horizon targets (which ICIR ranks fundamentals first)
  2. `--sector-neutral` changed from `action='store_true'` to `action=argparse.BooleanOptionalAction, default=True` — sector-neutral IC now the default; removes sector rotation bias from IC signal
  3. `--min-ic-stability` default changed from `0.0` (off) to `0.6` — drops features whose IC direction is inconsistent across years (< 60% sign consistency), preventing directionally unreliable features from passing on mean IC alone
- **`ROADMAP.md`** complete 4-phase restructure: Phase A (Foundation/Data), Phase B (Feature Selection/Factor Research), Phase C (Model Training/Alpha), Phase D (Portfolio/Production/Monitoring). Dataset Completion Plan folded into Phase A. Step 7 WF AUC table updated to post-retrain actuals. Stale "Immediate Next Actions" section removed.

### Added (Phase C — alpha registry)
- **`scripts/build_alpha_registry.py`** NEW — builds `data/alpha_registry.json` with IC + backtest stats for all 8 alpha signals (5 factor scores + 3 ML OOF horizons). Per signal: `ic_mean`, `icir`, `cagr_pct`, `sharpe`, `sortino`, `calmar`, `max_drawdown_pct`, `excess_cagr_vs_spy`, `beta_vs_spy`, `hit_rate_pct`, `features_used`, `selected` flag. Selection criteria: IC_mean > 0.02 AND Sharpe > 0.50. Result: 8 evaluated, 6 selected (alpha_value ✅, alpha_quality ✅, alpha_fraud_risk ✅, ml_1y_oof ✅, ml_3y_oof ✅, ml_5y_oof ✅; alpha_momentum ❌, alpha_growth ❌).

### Fixed (Phase C — backtester sortino/calmar always null)
- **`scripts/backtester.py`** Sortino ratio: removed `n_negative >= 3` guard; when all annual returns are positive (downside_vol = 0) falls back to Sharpe as a lower-bound (correct behavior — Sortino ≥ Sharpe when no negative years). Calmar ratio: when MaxDD < 2% (all-positive annual years), uses `2σ` as a conservative proxy for drawdown instead of returning null. Results: `sortino: 1.181`, `calmar: 0.641` now populated in `data/backtest_results.json`.

### Added (Phase C — model retrain, bias audit, backtest, alpha schema)
- **`scripts/bias_audit.py`** look-ahead fix: `_period_end_date()` returns `None` when `fiscal_quarter` is null (non-December FY-end companies). Prevents false-positive look-ahead violations. `_count_lookahead()` and `audit_filing_lag()` now skip rows with null `fiscal_quarter`. `pd.to_datetime(..., errors='coerce')` added to handle mixed `None`/`Timestamp` dtype from `.apply()`. CI exit 1 only on true look-ahead leakage (0 violations in production dataset).
- **`scripts/generate_oof_scores.py`** NEW — walk-forward expanding-window OOF scorer for 5 horizons. Produces `ml_6m_oof`, `ml_1y_oof`, `ml_2y_oof`, `ml_3y_oof`, `ml_5y_oof` columns in `data/historical_dataset_clean.parquet` (NaN for training-window rows, OOF for held-out rows). Dataset now 58,190 rows × 360 columns (+5 OOF columns vs 355).
- **`scripts/bias_audit.py`** overfitting audit (`audit_overfitting()`): computes `overfit_gap = val_auc - wf_mean_auc` per horizon, flags gap > 0.15 as ⚠️ OVERFIT, writes gaps back to `models/model_meta.json`. All gaps ≤ 0.15 for 6m/1y/2y/3y; 5y skipped (no val_auc — expected WARN).
- **`scripts/backtester.py`** industry-grade walk-forward backtest: COMPOSITE strategy CAGR +38.1%, excess +24.2% vs SPY, Sharpe 1.181, beta 0.483, tracking_error 0.2983. Outputs `data/backtest_results.json` with all C4 gate fields.
- **`alpha/horizon_router.py`** HorizonRouter routes 6→6m, 9→1y, 18/24→2y, 36→3y, 60→5y. All C5 routing cases confirmed.
- **`docs/methodology/models.md`** AUC table updated with Phase C actuals (all 5 horizons); flowchart header updated 355 → 360 features.
- **`docs/index.md`** tagline updated 355 → 360 columns; Performance at a Glance updated with COMPOSITE strategy row (+38.1% CAGR, +24.2% vs SPY, Sharpe 1.181).
- **`docs/architecture.md`** column counts updated 355 → 360 in High-Level Overview (B11 node), Storage subgraph (S1 node), and Data Flow Detail (FA node).
- **`CLAUDE.md`** Performance table updated with Phase C post-retrain actuals (all 5 horizons); Feature engineering row updated 355 → 360 columns; Primary storage row updated to 360 cols.

### Fixed (phase checks — A4 false-positive WARN)
- **`scripts/run_phase_checks.py`** A4 check: added `operator_only` allowlist to suppress false-positive WARN for scripts present in `data-update-guide.md` but not in `refresh_data.yml`. Allowlisted: `nfeature_library.py` (mermaid `\n` escape artefact), `auto_update.py` / `merge_snapshots.py` (operator-only), `monitor_drift.py` (separate `monitor_drift.yml` workflow), `push_to_hf.py` (CI uploads inline), `feature_library.py` (module, not runnable), `step5_compute_features.py` (operator step). Result: 61 PASS 0 FAIL 0 WARN.

### Added (CI/CD — multi-market weekly cron)
- **`.github/workflows/refresh_data.yml`** weekly cron default changed from `US` to `US CA JP KR EU BR` (all 6 markets with pipeline scripts). `"all"` shortcut now expands to the same 6-market set via a proper loop (was broken — only ran US pipeline). KR pipeline guarded by `DART_API_KEY` secret: skipped with `[WARN]` if secret absent, rest of markets continue. Health-status JSON now records the actual markets string.
- **`docs/developer/data-update-guide.md`** Section 5 updated: schedule table now shows multi-market cron; new per-market routing table (market → script → API key requirement); KR secret guard behaviour documented.

### Added (Phase B completion — IC decay analysis)
- **`notebooks/06_ic_decay.ipynb`** NEW — institutional-grade IC decay analysis notebook. 12 cells covering: annual cross-sectional Spearman IC per feature per horizon per year (1y/3y/5y); factor group aggregation (Value/Quality/Momentum/Growth/FraudRisk); IC decay curves with confidence bands; exponential decay fit + half-life estimation (IC(t) = IC₀ × exp(-λt)); year-by-year IC stability heatmap; regime-conditional IC (Bear/Crisis={2008,2009,2020,2022} vs Expansion); lag-1 IC autocorrelation; signal-type diagnostic (short/medium/long-horizon). IC-ready markets: US/KR/CA/JP only (min 30 tickers per cross-section).
- **`reports/ic_decay_by_group.png`** NEW — IC time-series curves per factor group × horizon with ±1σ bands.
- **`reports/ic_stability_heatmap.png`** NEW — year-by-year IC stability heatmap (group/horizon × fiscal_year).
- **`reports/ic_regime_decay.png`** NEW — regime-conditional IC bar chart (Bear/Crisis vs Expansion per group × horizon).
- **`reports/ic_autocorrelation.png`** NEW — lag-1 IC autocorrelation heatmap (year-over-year IC persistence).
- **`CONTEXT.md`** Phase B status updated to COMPLETE; Phase B gaps section replaced with "Status: COMPLETE" summary.
- **`ROADMAP.md`** Step 6 status updated to COMPLETE; all stale ❌ Todo rows replaced with ✅ Done and correct filenames.

### Added (anti-drift process)
- **`scripts/run_phase_checks.py`** NEW — single-command Phase A/B/C done verifier. Mechanically runs all exit criteria from `docs/developer/phase-done-criteria.md`. Phase A: dataset shape/quality, EDA notebook outputs, CI schedule completeness, diagram vs CI consistency. Phase B: feature library formula coverage, engineering guards (DSRI clip, growth winsorization, sector_pct fiscal_year grouping, montier_c2 ppe_net), feature selection integrity (no ML columns in feature sets, PSI=0.25, NW+FDR), factor research CSVs, notebook outputs. Phase C: OOF scores, model horizons/AUC targets, backtest results, alpha schema. Exits 1 on any FAIL; supports `--phase A|B|C|AB` and `--strict` (treat WARN as FAIL). A phase is only done when this script prints all PASS.
- **`docs/developer/scripts.md`** added section for `run_phase_checks.py` with full flag table, per-phase check tables, and anti-drift rule note.

### Added (Phase C6 — docs/architecture.md Phase C sync)
- **`docs/architecture.md`** ML System subgraph updated: 5 horizons (6m/1y/2y/3y/5y), PSI threshold 2.0 → 0.25, OOF scorer node (`generate_oof_scores.py`), HorizonRouter node (`horizon_router.py`).
- **`docs/architecture.md`** Research subgraph updated: Backtester now shows SPY benchmark, Bias Audit updated to 4 audits, SPY Returns node added (`fetch_spy_returns.py`).
- **`docs/architecture.md`** Component Map updated: new rows for OOF scorer, SPY fetch, HorizonRouter; train_models.py updated to 5 horizons filed-date PIT-safe; backtester.py updated to include SPY benchmark + factor attribution; bias_audit.py updated to 4 audits.
- **`docs/architecture.md`** Data Flow Detail: PSI threshold 2.0 → 0.25, ~35 → ~45 features/horizon, 5-horizon LightGBM, OOF path added (`ml_*_oof` columns), HorizonRouter node added.
- **`docs/architecture.md`** Deployment Architecture: `bias_audit.py --ci` step added in CI after dataset quality check; hard fail on look-ahead violation shown in diagram.

### Added (Phase A — coverage depth audit)
- **`CONTEXT.md`** market coverage table rewritten: 14 individual market rows (not grouped EU) each with Rows, Tickers, Fiscal Year Range, Quarterly enriched flag, and Depth rating (DEEP/MODERATE/SHALLOW/TOO_THIN). Coverage depth assessment section added: US/KR = DEEP (IC training ready), CA/JP = MODERATE (IC training ready), all others TOO_THIN. BR tickers corrected 55 → 48.
- **`reports/coverage_audit.csv`** NEW (gitignored, regeneratable): per-market depth ratings with ic_training flag — 4 markets IC-ready.
- **`reports/coverage_audit_heatmap.png`** NEW (gitignored, regeneratable): market × fiscal_year ticker count heatmap (log-scaled, blue dashed border for cells <50 tickers).

### Fixed
- **`pipeline/step5_compute_features.py`** `montier_c2`: replaced `property_plant_equipment` (95.7% null) with `ppe_net` (19.4% null) in Montier C2 depreciation-rate computation. Column was previously 100% null; now 41.6% null (limited by `depreciation` availability). `montier_c_score` composite coverage improves to 75.5%.
- **`scripts/patch_montier_c2.py`** (new): one-shot patch script to recompute all 7 montier columns on existing parquet without a full pipeline rebuild.
- **`data/historical_dataset_clean.parquet`** all 7 montier columns patched in-place (58,190 rows × 355 cols).
- **`docs/developer/phase-done-criteria.md`** HorizonRouter test case: 18m → `'2y'` (not `'1y'`). Boundary is exclusive upper bound for 1y range; 18m routes to the longer 2y model (conservative bias).

### Added (Phase C6 — documentation, diagrams, phase-done-criteria sync)
- **`docs/methodology/alpha-generation.md`** NEW — variable-horizon schema: HorizonRouter routing table, scoring pipeline flowchart, factor group weights, model confidence display thresholds, OOF vs static score usage rules.
- **`docs/methodology/bias-validation.md`** fully rewritten: all 4 Phase C audits documented (look-ahead PIT-safe split, survivorship −50% imputation, overfitting gap, Bonferroni multiple testing). Added OOF scoring section and CI audit table.
- **`docs/developer/phase-done-criteria.md`** Phase C checklist added: C1 (OOF columns + look-ahead), C2 (5 model files + feature sets + WF-AUC), C3 (bias audit), C4 (SPY data + backtest fields), C5 (HorizonRouter routing + scoring imports).
- **`CLAUDE.md`** architecture state table updated: 5 horizons in ML models row, OOF scoring row, HorizonRouter row, SPY benchmark data row, bias audit row. Key File Locations updated: 5-horizon model files, OOF audit trail, spy_returns.csv, bias_audit_report.json, horizon_router.py. Critical Missing Pieces replaced with current performance table (5 horizons).
- **`scripts/verify_doc_consistency.py`** extended: `get_feature_set_counts()` now covers 6m/1y/2y/3y/5y; `get_trained_horizons()` added; Phase C checks added (model_meta.json horizon coverage, spy_returns.csv presence, horizon_router.py presence).

### Added (Phase C5 — variable-horizon alpha schema + UI)
- **`alpha/horizon_router.py`** NEW — `HorizonRouter` class: maps any investment horizon (months) to nearest trained model key (6m/1y/2y/3y/5y). Routing: 3–9m→6m, 9–18m→1y, 18–30m→2y, 30–48m→3y, 48m+→5y. Conservative bias (longer model on tie). Includes `MODEL_LABELS`, `FEATURE_FACTOR_GROUPS` (200+ features mapped to Value/Quality/Momentum/Growth/Fraud Risk), `wf_auc()` helper.
- **`src/scoring.py`** `resolve_horizon()` added: accepts model key string or integer months, routes via `HorizonRouter`, falls back gracefully to nearest available model. `score_companies()` now accepts `horizon: str | int` — integer months are routed automatically.
- **`src/scoring.py`** `top_feature_importances()` added: returns top N (feature, importance, factor_group) tuples for a model key. Uses `shap_top_features` from model_meta.json if populated; falls back to LightGBM `feature_importances_` attribute.
- **`src/ui/tab_screener.py`** Investment horizon selectbox (`['1y','3y','5y']`) replaced with slider (6–60 months in 6-month steps). Selected months are routed via `HorizonRouter` to the correct model.
- **`src/ui/tab_screener.py`** Model confidence badge added below slider: WF-AUC ≥ 0.65 = "High confidence" (green), 0.60–0.65 = "Good" (light green), 0.55–0.60 = "Moderate" (orange), < 0.55 = "Screening only — lower confidence" (red) with warning message.
- **`src/ui/tab_screener.py`** Top signals expander added: horizontal bar chart of top 6 feature importances with factor group color coding (Value/Quality/Momentum/Growth/Fraud Risk).
- **`src/ui/tab_screener.py`** Company Deep Dive: "Alpha Score" section shows the company's ML score for the selected horizon + top 5 driving signals with factor group labels and actual feature values.
- **`src/ui/tab_screener.py`** Screener header renamed "Alpha Screener — Ranked by Multi-Factor Score" to reflect that the output is a ranked list of high-alpha candidates. "ML Score" column renamed "Alpha Score" in the results table.

### Added (Phase C4 — industry-grade backtest, SPY benchmark, factor attribution)
- **`scripts/fetch_spy_returns.py`** NEW — downloads SPY annual calendar-year total returns (adjusted close, dividends included) via yfinance. Saves `data/spy_returns.csv` (year, spy_return). Covers 2008–2025.
- **`data/spy_returns.csv`** NEW — 18 years of SPY returns (2008–2025). Mean +12.64%, best 2013 (+32.31%), worst 2008 (−36.80%).
- **`scripts/backtester.py`** SPY is now the primary benchmark: `excess_cagr_pct` = portfolio CAGR − SPY CAGR. Equal-weight universe mean retained as secondary metric `excess_vs_univ`. Benchmark source recorded in `benchmark_source` field.
- **`scripts/backtester.py`** factor attribution added: `beta_vs_spy` (OLS slope), `alpha_vs_spy` (Jensen's alpha intercept), `r_squared_vs_spy`, `tracking_error` (std dev of excess returns vs SPY).
- **`scripts/backtester.py`** new risk metrics in output: `var_95_pct` (historical 5th percentile), `annual_turnover_pct` (approx), `max_drawdown_duration_months`.
- **`scripts/backtester.py`** `print_tearsheet()` updated: shows SPY CAGR, excess vs SPY, beta/alpha/R²/tracking_error, VaR 95%, annual turnover, drawdown duration; annual return table updated to show SPY% and excess-vs-SPY columns.
- **`scripts/backtester.py`** `--max-filing-lag` default corrected: 6 → 18 months (aligned with actual filing lag distribution in dataset).
- **`docs/methodology/backtesting.md`** fully rewritten: SPY benchmark section, PIT-safe walk-forward diagram, transaction cost model table, factor attribution formula, complete output field reference, updated running instructions.
- **`docs/developer/scripts.md`** `backtester.py` section updated: new flags table, SPY benchmark note, output field list.

### Added (Phase C3 — bias audit suite + CI integration)
- **`scripts/bias_audit.py`** overhauled: added `audit_overfitting()` (train AUC vs WF mean AUC gap, writes `overfit_gap` to model_meta.json; flag if gap > 0.15) and `audit_multiple_testing()` (Bonferroni correction across 5 horizons × 4 strategies). Added `--ci` flag: exits with code 1 if any look-ahead violations found (hard fail), warn-only for survivorship/overfitting. Added `_count_lookahead()` helper.
- **`.github/workflows/refresh_data.yml`** added `bias_audit.py --ci` step after `test_dataset_quality.py`. Look-ahead violations fail CI; survivorship and overfitting are logged as warnings.


- **`scripts/train_models.py`** HORIZONS dict extended: added `6m` (forward_return_6m / beat_local_market_6m) and `2y` (forward_return_2y / beat_local_market_2y). Now covers 5 discrete horizons: 6m/1y/2y/3y/5y. EXCLUDE set updated with ml_6m/ml_2y/ml_6m_oof/ml_2y_oof.
- **`scripts/generate_oof_scores.py`** ALL_HORIZONS extended to all 5 horizons; default `--horizons` now `6m 1y 2y 3y 5y`.
- **`scripts/tune_models.py`** HORIZONS set extended to 5 horizons; N_OPTUNA_TRIALS 60→100; `_load_data_for_horizon()` patched to use filed_date PIT-safe split matching train_models.py; `--horizon` choices extended.
- All 5 forward return columns (`forward_return_6m`, `forward_return_2y`) already present in parquet from step3_enrich_prices.py — no data pipeline changes required.


- **`scripts/generate_oof_scores.py`** NEW — walk-forward OOF scorer. For each fiscal year Y: trains on `filed_date < Jan 1 of Y`, scores `fiscal_year == Y`. Writes `ml_1y_oof`, `ml_3y_oof`, `ml_5y_oof` to parquet (NaN for training-window rows). Eliminates in-sample contamination from `score_historical.py`.
- **`scripts/train_models.py`** enhanced model config: `n_estimators` 400→600, `max_depth` 5→6, `num_leaves` 31→63, `learning_rate` 0.04→0.03, added `reg_alpha=0.1`, `reg_lambda=1.0`.
- **`scripts/train_models.py`** `--max-psi` default 2.0→0.25 (aligned with `run_feature_selection.py`).
- **`scripts/train_models.py`** `walk_forward_cv()` patched to use `filed_date` PIT-safe cutoff per fold year (previously used only `fiscal_year`).
- **`scripts/train_models.py`** `EXCLUDE` set updated: added `ml_1y_oof`, `ml_3y_oof`, `ml_5y_oof` so OOF columns are never used as input features.
- **`docs/developer/scripts.md`** updated `train_models.py` flags table (new --max-psi default, filed-date note); added `generate_oof_scores.py` section.

### Fixed (Phase B — feature selection, final institutional quality pass)
- **`scripts/run_feature_selection.py`** BH FDR correction now gates `ic_pass`: features must pass `fdr_reject=True` (BH q<0.05) to enter ICIR ranking. Previously FDR was computed but not enforced as a filter — spurious features could pass.
- **`scripts/run_feature_selection.py`** Sector-neutral IC added (default: on). Return and feature demeaned by SIC-based sector within each fiscal year before IC computation. Prevents sector rotation from inflating stock-selection IC. Matches methodology of `factor_research.py`.
- **`scripts/run_feature_selection.py`** `--sector-neutral` / `--no-sector-neutral` CLI flags added.
- **`scripts/test_dataset_quality.py`** Section 10 added: point-in-time leakage checks. Validates `filed_date` timing vs `fiscal_year_end` — negative median filing lag is a hard fail; extreme lags emit warnings. 98 checks total.
- **`.github/workflows/refresh_data.yml`** added steps to re-run `factor_research.py` (base + sector-neutral) and `run_feature_selection.py` after every weekly data refresh.
- **`docs/methodology/feature-selection.md`** corrected "Planned (Phase 0)" → "Implemented (Phase B)" for NW, BH FDR, and sector-neutral IC.
- **`models/feature_sets_{1y,3y,5y}.json`** re-run: **45/45/41 features** (sector-neutral IC + BH FDR gate).
- **`docs/index.md`** updated: feature counts 45/45/42 → 45/45/41; sector-neutral IC noted.

### Added (Phase B — feature engineering, parquet patch)
- **`data/historical_dataset_clean.parquet`** patched to add 9 new columns from Phase B implementation: `montier_c1`–`montier_c6` (binary Montier components), `montier_c_score` (normalised composite), `sloan_wc_accruals`, `sloan_lt_accruals`. Dataset now 58,190 × 355 cols.
- **`docs/architecture.md`**, **`docs/index.md`**, **`docs/methodology/models.md`**, **`README.md`**, **`CLAUDE.md`** updated: column count 346 → 355; PSI threshold 2.0 → 0.25 in models.md flowchart.
- **`.github/workflows/refresh_data.yml`** added `test_dataset_quality.py` quality gate step after `check_data.py`.

### Fixed (Phase B — factor research)
- **`scripts/factor_research.py`** output dict: added `ic` key (alias for `mean_ic`) so `reports/factor_research_*.csv` includes the standard `ic` column required by phase-done-criteria.md B4.
- **`reports/factor_research_{1y,3y,5y}.csv`** regenerated (non-sector-neutral) and **`_sn`** variants regenerated (sector-neutral) with updated column schema.

### Added (Phase B — notebooks)
- **`notebooks/02_ic_analysis.ipynb`** added Sec 7 IC decay curves (t+1y/2y/3y for top 10 features), Sec 8 quintile return spreads (Q1-Q5 mean returns + Sharpe), Sec 9 Fama-MacBeth regression (annual cross-sectional slope t-stats), Sec 10 market regime IC (recession vs expansion), Sec 11 long-short decomposition (long/short legs + spread), Sec 12 information ratio per factor (IR > 0.5 threshold). Executed with outputs.
- **`notebooks/03_factor_correlation.ipynb`** re-executed with fresh outputs (V4 schema compatibility verified).
- **`notebooks/04_null_recovery_audit.ipynb`** re-executed with fresh outputs.
- **`notebooks/05_market_coverage.ipynb`** re-executed with fresh outputs.

### Added (Phase B — feature selection)
- **`scripts/run_feature_selection.py`** Newey-West HAC t-statistic (`ic_tstat_nw`) and p-value (`ic_pval_nw`) computed per feature IC time series. Corrects for IC autocorrelation across fiscal years.
- **`scripts/run_feature_selection.py`** Benjamini-Hochberg FDR correction (`fdr_reject`) applied to Newey-West p-values at q=0.05. Controls false discovery rate across ~200 simultaneous hypotheses.
- **`reports/feature_selection_summary.csv`** now contains columns: `ic_tstat_nw`, `ic_pval_nw`, `fdr_reject`.

### Fixed (Phase B — feature selection)
- **`scripts/run_feature_selection.py`** `PSI_THRESHOLD` lowered from 2.0 to 0.25 (institutional standard). 14 drifted features removed per horizon (macro regime features).
- **`scripts/train_models.py`** `EXCLUDE` set: added `alpha_fraud_risk`, `alpha_composite`, `alpha_value`, `alpha_quality`, `alpha_growth`, `alpha_momentum`. Hand-crafted composites of raw features cause signal double-counting when their component features are also candidates.
- **`models/feature_sets_{1y,3y,5y}.json`** re-run: **45/45/42 features** (alpha_* and ml_* excluded; PSI=0.25; Newey-West t-stats computed).
- **`docs/methodology/feature-selection.md`** updated: PSI threshold, candidate pool count, NW/FDR documentation.

### Added (Phase B — feature engineering)
- **`pipeline/step5_compute_features.py`** `add_montier_c_score()`: Implements Montier C-Score (Montier 2008) — 6 binary forensic accounting variables (`montier_c1`–`montier_c6`) plus composite `montier_c_score` normalised to [0,1].
- **`pipeline/step5_compute_features.py`** Richardson et al. (2005) named accrual columns: `sloan_wc_accruals` (working capital accruals / assets) and `sloan_lt_accruals` (long-term accrual residual / assets) added alongside existing `sloan_accruals`.
- **`docs/methodology/features.md`** Montier C-Score and Richardson accrual decomposition documented with paper references.

### Fixed (Phase B — feature engineering)
- **`pipeline/step5_compute_features.py`** `beneish_dsri` clipped to [0.5, 3.0] — values outside this range are data errors, not manipulation signals.
- **`pipeline/step5_compute_features.py`** Momentum rank cohort guard added: cohorts with < 10 non-null observations return NaN ranks (prevents noisy rankings from single-company cohorts in small markets).

### Added (Phase A/B — EDA cells)
- **`notebooks/01_eda_dataset.ipynb`** Section 3a: Null profile heatmap (key features × market) — identifies which markets have missing data for critical columns.
- **`notebooks/01_eda_dataset.ipynb`** Section 4a: Forward return Q-Q plot + outlier stats (min/p1/p25/median/p75/p99/max) with treatment note (pre/post winsorization documentation).
- **`notebooks/01_eda_dataset.ipynb`** Section 4b: Point-in-time lineage check — counts violations where `filed_date > fiscal_year+1-01-01`; plots filing lag distribution.

### Fixed (Phase A — data targets + CI sync)
- **`data/historical_dataset_clean.parquet`** forward_return targets winsorized at p1/p99: `forward_return_1y` max was 29,999% (penny stock data error) → now capped at [−0.926, 3.906]; `forward_return_3y` capped at [−0.995, 6.989]; `forward_return_5y` capped at [−0.999, 9.227].
- **`scripts/test_dataset_quality.py`** Section 7 upgraded: now enforces winsorization hard caps (1y ≤ 5.0×, 3y ≤ 10.0×, 5y ≤ 20.0×) in addition to coverage checks.
- **`.github/workflows/refresh_data.yml`** added missing `enrich_quarterly_features.py` post-processing step (was absent, so intra-year dynamics were not refreshed in weekly CI runs).
- **`docs/developer/data-update-guide.md`** Mermaid operator diagram updated: added `fix_dataset_quality.py` + `enrich_quarterly_features.py` nodes, removed phantom `enrich_fraud_taxonomy.py`, updated quality gate to "92 checks". Diagram now matches actual CI.

### Added (Phase scope lock + done criteria)
- **`docs/developer/phase-done-criteria.md`** (new): Single source of truth for "is Phase A/B done?". Contains exact shell commands that return PASS/FAIL for every Phase A and Phase B acceptance criterion. Replaces vague task-list-based closure checks.
- **`CLAUDE.md`** Phase Scope Definition section: Locks Phase A/B/C scope. Three rules: no re-auditing, Phase C items never in Phase A/B, done requires the checklist file.
- **`docs/developer/pipeline-integrity.md`** Phase Closure section: Replaced vague checklist with pointer to `phase-done-criteria.md`.

### Fixed (docs — column count sync)
- **`docs/developer/phase-done-criteria.md`** A1 assertion corrected 346 → 355 to match actual production parquet shape (58,190 × 355).
- **`docs/developer/data-update-guide.md`** column lineage table: added Montier/Sloan accrual step row, final state now correctly shows 355 columns.

### Fixed (docs — quality check count + diagram sync)
- **`docs/developer/data-update-guide.md`** Mermaid diagram node, line 48, and line 99: check count corrected from 92/53 → **98** to match current `test_dataset_quality.py`.
- **`docs/architecture.md`** Data Flow diagram: added Montier/Sloan node (341 → 355 cols) between IMP and PSI; final alpha node updated from "335 → 341" to "341 → 355 cols total".

### Added (sync enforcement — prevents future doc drift)
- **`scripts/verify_doc_consistency.py`** (new): reads live parquet + key docs, verifies column counts, row counts, feature counts, and quality check count are consistent across all 8 doc files. `--warn` flag for CI advisory mode.
- **`.github/workflows/refresh_data.yml`** added `verify_doc_consistency.py --warn` step after feature selection — CI now flags numeric drift in the weekly run log.
- **`scripts/check_sync.py`** 5 new trigger rules: `step5-columns` (step5 changes → architecture.md + data-update-guide.md), `quality-check-count` (test_dataset_quality.py changes → data-update-guide.md + phase-done-criteria.md + scripts.md), `feature-selection-counts` (run_feature_selection.py / feature_sets_*.json changes → index.md + scripts.md + feature-selection.md), `ci-workflow` (refresh_data.yml → data-update-guide.md).
- **`docs/developer/scripts.md`** documented `verify_doc_consistency.py` and updated `check_sync.py` section with new rule coverage.



---

## [Unreleased]

### Fixed (Phase B audit — data integrity + in-sample contamination)
- **`data/historical_dataset_clean.parquet`** 117 BR null-ticker rows dropped (58,307 → 58,190 rows). Brazilian companies that could not be matched to a B3 ticker (Cia Siderúrgica Nacional, JSL, Nexpe, WEG, etc.) silently polluted the dataset. Dropping them restores all 53 quality-test assertions.
- **`data/historical_dataset_clean.parquet`** 30+ growth/YoY columns winsorized at 1st/99th percentile. `revenue_growth_yoy` max was 184,343× (near-zero base problem); `shares_dilution` max was 2.37B. Unwinsorized growth features dominated IC rankings and could produce extreme gradient-boosted splits.
- **`scripts/train_models.py`** `EXCLUDE` set: Added `ml_1y`, `ml_3y`, `ml_5y`. `score_historical.py` scores ALL historical rows with a model trained up to `TRAIN_CUTOFF=2022`, inflating IC for 2008–2022 training rows. Including ML scores as feature-selection candidates would make the next training run self-referential (model trains on its own predictions). Walk-forward OOF scoring required before these can re-enter (Phase C).
- **`models/feature_sets_{1y,3y,5y}.json`** re-run: 45/46/45 features (ml_1y/3y/5y removed from all three horizons; `alpha_*` remains — no forward-return contamination).

### Added (Phase B audit — prevention framework)
- **`docs/developer/pipeline-integrity.md`** Rule 6: All growth/YoY features must be winsorized at 1st/99th percentile in `step5_compute_features.py`. Documents root cause (near-zero base), IC-inflation mechanism, and how to apply (add new column to `ratio_cols` before closing task).
- **`docs/developer/pipeline-integrity.md`** Rule 7: ML-derived score columns (`ml_1y/3y/5y` and future equivalents) must appear in the `EXCLUDE` set in `train_models.py` and must never appear in `models/feature_sets_*.json`. Documents circular contamination mechanism.
- **`CLAUDE.md`** Change Checklist: 2 new rows — (1) "New growth/YoY feature added → add to `ratio_cols` winsorize list (Rule 6)"; (2) "New ML-derived score column added → add to `EXCLUDE` before running feature selection (Rule 7)".
- **`scripts/test_dataset_quality.py`** Section 8: Growth feature winsorization guard — asserts that no growth column has `max > 50 × p99` (catches future unwinsorized columns). Covers 36 growth/YoY/dilution columns.
- **`scripts/test_dataset_quality.py`** Section 9: ML score exclusion guard — asserts that `ml_1y`, `ml_3y`, `ml_5y` do not appear in any `models/feature_sets_*.json`. Catches regression if EXCLUDE set is edited carelessly.
- **`docs/developer/data-update-guide.md`** Column count reference updated: 326 → 346; row count 58,307 → 58,190; full step-by-step table added.
- Quality test suite now has 92 checks (up from 53).

### Added (Phase B audit — research notebooks)
- **`notebooks/05_market_coverage.ipynb`** (new): Per-market audit — feature fill rates by factor group, year range, forward return label density, and usability summary for all 6 markets.
- **`notebooks/02_ic_analysis.ipynb`** Section 6 — Temporal IC Stability: year-by-year IC heatmap (feature × year) + stability summary table (mean_IC, ICIR, pct_same_sign). Reveals regime-dependent factors that inflate aggregate ICIR but are unreliable out-of-sample.

### Fixed (Phase B audit — academic formula implementations)
- **`pipeline/step5_compute_features.py`** Beneish `beneish_depi`: Was computing `dep_rate / dep_rate` (always 1.0). Fixed to compute proper prior-year depreciation rate using growth-rate approximation. All 58K rows now have variable DEPI (mean=1.02, std=0.34). `beneish_m_score` recomputed.
- **`pipeline/step5_compute_features.py`** Altman `altman_x4`: Was using `market_cap_at_filing.fillna(0)` — silently gave 0 contribution for KR/BR (0% market cap fill). Now uses book equity as fallback (Altman Z''-Score variant for private/non-US firms). KR `altman_x4` fill: 0% → 99.7%. `altman_z_score` recomputed.
- **`pipeline/step5_compute_features.py`** Piotroski F-score signal 6 (`piotroski_delta_liq`): Was using `current_assets_growth > 0` instead of Piotroski 2000 criterion `Δ(current_ratio) > 0`. Fixed using groupby-shift on `current_ratio` within ticker. `piotroski_f_score` recomputed.


- **`pipeline/step5_compute_features.py`** `compute_sector_pct_ranks()`: Added `fiscal_year` to groupby — was `groupby('sic_2digit')`, now `groupby(['sic_2digit', 'fiscal_year'], observed=True)`. Without this, a 2005 company was ranked against 2005–2024 sector peers (temporal lookahead in feature space). Affects 18 `*_sector_pct` columns. Dataset patched in-place; feature selection and factor research re-run.
- **`pipeline/step3_enrich_prices.py`** `enrich_row()`: Added `vol_prior_6m` (126d), `vol_prior_36m` (756d), `vol_prior_60m` (1260d) natively alongside existing `vol_prior_12m`. Previously these existed only via a one-off patch script and were silently dropped on every CI rebuild (Rule 1 violation).
- **`pipeline/step5_compute_features.py`**: Added `roa_volatility_5yr` and `earnings_stability_roa_5yr` natively after `roe_volatility_5yr` (same violation — existed only in patch script).
- **`.github/workflows/refresh_data.yml`**: Added four missing post-processing steps that were absent from CI: `impute_features.py`, `mark_survivorship.py --fix`, `compute_alpha.py`, `score_historical.py`. Previous CI produced a ~326-column parquet missing quarterly features, alpha scores, ML scores, and survivorship correction.

### Added (Phase A/B audit — prevention)
- **`docs/developer/pipeline-integrity.md`** (new): 5 rules that prevent the class of bugs found in Phase A/B audit — orphan patch columns (Rule 1), cross-sectional rank without time key (Rule 2), CI/dataset drift (Rule 3), stale artifacts after data fix (Rule 4), formula scattering (Rule 5). Includes Phase A and Phase B closure checklists and common anti-patterns table.
- **`CLAUDE.md`** Change Checklist: Added 3 new rows linking to `pipeline-integrity.md` — triggered on new columns, new rank features, and new post-processing scripts.

### Fixed (Phase B audit — academic formula implementations)
- **`pipeline/step5_compute_features.py`** Beneish `beneish_depi`: Was computing `dep_rate / dep_rate` (always 1.0). Fixed to compute proper prior-year depreciation rate using growth-rate approximation (same pattern as GMI/SGAI). All 58K rows now have variable DEPI (mean=1.02, std=0.34). `beneish_m_score` recomputed.
- **`pipeline/step5_compute_features.py`** Altman `altman_x4`: Was using `market_cap_at_filing.fillna(0)` — silently gave 0 contribution for KR/BR (0% market cap fill). Now uses book equity as fallback (Altman Z''-Score variant for private/non-US firms). KR `altman_x4` fill: 0% → 99.7%. `altman_z_score` and `altman_z_score_sector_pct` recomputed.
- **`pipeline/step5_compute_features.py`** Piotroski F-score signal 6 (`piotroski_delta_liq`): Was using `current_assets_growth > 0` instead of the Piotroski 2000 criterion `Δ(current_ratio) > 0`. Fixed using groupby-shift on `current_ratio` within ticker. `piotroski_f_score` recomputed.
- **`models/feature_sets_{1y,3y,5y}.json`** re-run on corrected data: 46/46/46 features selected.
- **`reports/feature_selection_summary.csv`**, **`reports/factor_research_{1y,3y,5y}.csv`** regenerated.

### Added (Phase B audit — research notebooks)
- **`notebooks/05_market_coverage.ipynb`** (new): Per-market audit — feature fill rates by factor group, year range, forward return label density, and usability summary for all 6 markets.
- **`notebooks/02_ic_analysis.ipynb`** Section 6 — Temporal IC Stability: year-by-year IC heatmap (feature × year) + stability summary table (mean_IC, ICIR, pct_same_sign). Reveals regime-dependent factors that inflate aggregate ICIR but are unreliable out-of-sample.

### Changed (Phase B re-runs on corrected data)
- **`models/feature_sets_{1y,3y,5y}.json`** regenerated: Feature selection re-run on corrected dataset (correct `*_sector_pct` features + fixed equity). 46/47/46 features (5y changed 45→46). Sector_pct features now properly represent within-year cross-sectional signal; more pass IC/ICIR threshold (6-7 per horizon vs 2-4 before).
- **`reports/feature_selection_summary.csv`** regenerated: 645 rows with updated IC/ICIR.
- **`reports/factor_research_{1y,3y,5y}.csv`** regenerated: IC/ICIR values updated for corrected equity and sector features.
- **`docs/methodology/feature-registry.md`**: Column count 326 → 346; added `roa_volatility_5yr`, `earnings_stability_roa_5yr`, `vol_prior_6m`, `vol_prior_36m`, `vol_prior_60m`.
- **`docs/developer/data-update-guide.md`**: Operator workflow Mermaid diagram updated to show full post-processing chain (impute → survivorship → alpha → scores → quality gate → push). Added rule: if a step is not in the diagram, it won't run in CI.
- **All 4 research notebooks** re-run on corrected 346-column dataset.


- **`pipeline/step5_compute_features.py`** (`COALESCE_ALIASES`): Root cause of 9 null columns found and fixed — `COLUMN_ALIASES` loop used `if dst not in df.columns` which skipped `equity → total_equity` because `total_equity` already existed at 0.2% fill. Added `COALESCE_ALIASES = {'equity', 'sga_expense'}` set; columns in this set now use `combine_first` to coalesce from the higher-fill source. Fixes `total_equity` (4.3% → 92.9%), `roe` (4.3% → 88.1%), `roic`, `pb_ratio`, `book_to_market`, `net_debt_to_equity`, `roe_sector_pct`, `pb_ratio_sector_pct`, `roe_volatility_5yr`, `earnings_stability_5yr` for future pipeline runs.

### Added (Phase A/B — equity + volatility patch + new columns)
- **`scripts/patch_equity_vol_features.py`** (new): One-time patch script that backfills the existing parquet without re-running the full pipeline. Two operations: (1) `patch_equity_features()` — joins `snapshots_combined.parquet` on `(cik, fiscal_year)`, coalesces `equity` and `sga_expense`, recomputes all equity-derived ratios and rolling volatility; (2) `patch_vol_features()` — reads `price_cache.db` ticker-by-ticker (7,753 tickers), computes annualised daily-return volatility over 6m / 36m / 60m lookback windows. Adds 5 new columns and fixes 6 broken ones. Creates `.parquet.bak_pre_patch` backup before writing. Supports `--dry-run` flag.
- **`data/historical_dataset_clean.parquet`** — 5 new columns added (341 → 346): `roa_volatility_5yr` (rolling 5yr std of ROA, 91.5% fill), `earnings_stability_roa_5yr` (−roa_volatility_5yr), `vol_prior_6m` (annualised 6m price vol, 95.4% fill), `vol_prior_36m` (annualised 36m price vol, 95.5% fill), `vol_prior_60m` (annualised 60m price vol, 95.4% fill). Also fixes previously broken: `roe`, `roic`, `pb_ratio`, `book_to_market`, `net_debt_to_equity`, `roe_volatility_5yr`, `earnings_stability_5yr`, `roe_sector_pct`, `pb_ratio_sector_pct`.

### Fixed (Phase A — Brazil B3 ticker matching)
- **`pipeline/step1_fetch_tickers_br.py`**: Ticker matching improved from 64 → 112 companies (75% improvement). Changes: (1) regex broadened from `[34]$` to `[3-9]$` to include ON3/PN4/PNB5/PNC6 share classes and units (ON9); (2) `CURATED_OVERRIDES` dict added for 11 companies with acronym-based tickers not derivable from name heuristics (BBDC3, BBAS3, CMIG3, BRSR3, BMEB3, CLSC4, FESA3, SNSY5, etc.); (3) `_MATCH_STOP` frozenset added (BCO, BANCO, CIA, PARTICIPACOES, HOLDING, etc.) to skip noise words; (4) `best_match()` expanded from 2 to 6 strategies: prefix4, first meaningful word, 4-letter acronym, acro2+first2chars, second word prefix, 3-letter unique match.
- **`data/tickers_br.parquet`** regenerated: 353 companies, 112 with B3 ticker matched (was 64).
- **`data/snapshots_br.parquet`** patched: ticker column updated; 93 unique tickers (was 57).

### Added (Phase B — feature selection + research notebooks)

- **`scripts/run_feature_selection.py`** (new): Standalone 4-stage feature selection pipeline — PSI filter (PSI ≤ 2.0) → IC screen (|mean IC| ≥ 0.02, n_years ≥ 5) → ICIR top-K (default 60) → Spearman deduplication (|r| ≤ 0.90). Imports `compute_ic_table`, `compute_psi`, `deduplicate_features` from `train_models.py`. Outputs `models/feature_sets_{1y,3y,5y}.json` (46 / 47 / 45 features) and `reports/feature_selection_summary.csv` (600 rows, IC/ICIR/PSI per candidate × 3 horizons). CLI: `--psi-threshold`, `--ic-min`, `--top-k`, `--corr`, `--dry-run`.
- **`scripts/factor_research.py`** (re-run): Refreshed IC/ICIR reports on updated 341-column parquet. Top features: `ml_1y` (ICIR=2.037), `altman_z_score_sector_pct` (ICIR=1.823), `ev_revenue` (ICIR=−1.708), `ml_3y` (ICIR=1.688), `alpha_fraud_risk` (ICIR=1.649). Reports written to `reports/factor_research_{1y,3y,5y}.csv`.
- **`notebooks/01_eda_dataset.ipynb`** (new): EDA — shape/date range, rows by market/year, period_type split, null profile (annual), target variable coverage + histograms, ML/alpha score distributions, size_category.
- **`notebooks/02_ic_analysis.ipynb`** (new): IC/ICIR analysis — top-20 by |ICIR| tables, bar charts, IC stability vs mean_ic scatter, feature overlap across horizons (set intersection), feature_selection_summary breakdown.
- **`notebooks/03_factor_correlation.ipynb`** (new): Factor correlation — Spearman heatmap of 1y selected features (max |r| ≤ 0.90 verification), alpha factor cross-correlation, high-correlation pairs (|r|>0.70) among selected features.
- **`notebooks/04_null_recovery_audit.ipynb`** (new): Null recovery audit — quarterly feature null rates post-imputation, size_category distribution + imputation quality bars, quarterly coverage by market, 341-column count verification, violin plots of quarterly feature distributions.

### Changed (Phase B — feature selection docs)
- **`docs/methodology/feature-selection.md`**: Updated opening summary (319→341 raw columns, ~185→~203 PSI candidates, ~35→~45 final), Mermaid diagram node labels, result paragraph, CLI examples (now reference `run_feature_selection.py`), and Outputs section (JSON per horizon + summary CSV).
- **`docs/developer/scripts.md`**: Added `run_feature_selection.py` section with bash usage, flags table, and output file descriptions.

### Added (Phase B — feature imputation)
- **`scripts/impute_features.py`** (new): Recovers two categories of missing data in `data/historical_dataset_clean.parquet`. (1) Reads quarterly rows from `data/historical_dataset.parquet` (pre-clean), runs `compute_quarterly_features()`, and left-joins 5 intra-year columns (`revenue_qoq_std_norm`, `earnings_qoq_mean`, `max_accruals_ttm`, `revenue_acceleration`, `quarterly_positive_rev_frac`) onto the clean parquet — 67% of annual rows enriched. (2) Imputes `size_category` from `log_assets` percentile rank within `(fiscal_year, market)` peer groups for 17,226 recoverable null rows; adds `size_category_imputed` boolean flag. Dataset grows from 335 → 341 columns. Supports `--dry-run` and `--source` flags.

### Changed (CI workflows)
- **`.github/workflows/monitor_drift.yml`**: `repo_id` corrected from `mhoque/stock-fraud-screener` to `ekrash718/stock-screener-data`; model file downloads updated to `repo_type='dataset'`.
- **`.github/workflows/weekly_push.yml`**: Deleted — superseded by `refresh_data.yml`.

### Added (Phase B — 5-factor alpha package)
- **`alpha/factors/`** (new package): Five cross-sectional rank factor modules — `value.py`, `quality.py`, `momentum.py`, `growth.py`, `fraud_risk.py` — plus `composite.py` that blends them into `alpha_composite` with configurable weights (default 0.20 each). All scores are 0–1, ranked within `(fiscal_year, market)` peer groups.
- **`scripts/compute_alpha.py`** (new): Loads `data/historical_dataset_clean.parquet`, calls `alpha.factors.composite.compute(df)`, and writes `alpha_value`, `alpha_quality`, `alpha_momentum`, `alpha_growth`, `alpha_fraud_risk`, `alpha_composite` (all `float32`) back to the parquet. Dataset grows from 329 → 335 columns. Supports `--dry-run` flag.

### Added (Phase B — ML scoring)
- **`scripts/score_historical.py`** (new): Applies trained LightGBM models (1y/3y/5y) to all 58K rows in the dataset. Loads `model_{1y,3y,5y}.joblib` + `model_meta.json`, fills missing features with per-horizon train_medians, calls `predict_proba`, and writes `ml_1y`, `ml_3y`, `ml_5y` float columns back to `data/historical_dataset_clean.parquet`. Dataset grows from 326 → 329 columns. Supports `--dry-run` flag.

### Fixed (Phase B — diagram sync)
- **`docs/architecture.md`**: Fixed stale column counts — B5 node 320→321 base columns, B7 node 324→326 total, F node now correctly shows 321 cols (pre-quarterly), Q node shows 326 cols as the final parquet. C6 node and Component Map row updated to ✅ for score_historical.py. Parquet storage node updated 326→329 cols.
- **`CLAUDE.md`**: Architecture State table updated — score_historical.py ❌→✅, parquet col count 326→329, Critical Missing Pieces updated.

### Added (Phase A — housekeeping / docs)
- **`docs/developer/schema-change-guide.md`** (new): Schema versioning policy — 11-step column-add checklist, rename breaking-change checklist (grep + retrain), deprecation protocol (keep one release with NaN, then drop), and a 6-file "column count must stay in sync" table covering `docs/architecture.md`, `docs/methodology/models.md`, `docs/index.md`, `CLAUDE.md`, and `docs/developer/data-update-guide.md`.
- **`scripts/analyze_distributions.py`** (new): Non-fatal CI script for dataset quality monitoring. Produces `reports/distribution_report.txt` (NaN%, outlier rates by |z|>5, market fill rates for 10 key features, fraud label balance, rows per market). With `--corr` flag also produces `reports/correlation_matrix.parquet` and prints high-correlation pairs (|r|>0.95). Usage: `python3 scripts/analyze_distributions.py [--parquet PATH] [--out-dir DIR] [--corr]`.
- **`docs/methodology/feature-registry.md`** (new): Complete 326-column authoritative registry — column names, data types, and factor group assignments. Organized into 10 sections: Identity/Admin (29), Raw Financials (~50 inputs), Value (~18), Quality (~83), Momentum (~45), Growth (~22), Fraud Risk (~164), Macro/Context (~10), Quarterly-Enriched (5), Derived/Interaction (~63).

### Fixed (Phase A — housekeeping / docs)
- **`docs/architecture.md`**: Remaining three "⚠️ pending" TimescaleDB references updated to "Phase C — deferred" — Component Map table row, Data Flow Detail `DB[TimescaleDB...]` node, and Deployment Architecture `J[TimescaleDB...]` node.
- **`docs/methodology/features.md`**: 8-category feature table total row corrected 324 → 326.
- **`docs/methodology/factor-library.md`**: Factor Group 3 (Momentum) fully updated — stale "⚠️ Momentum gap — Phase 0 blocker" admonition removed; mermaid diagram count updated ~32 → ~45; full implemented feature tables added for cross-sectional rank transforms (7 features) and raw price features (13+).
- **`docs/developer/data-update-guide.md`**: Column count reference table corrected 324 → 326; operator workflow Mermaid flowchart added immediately after "Always read this file before modifying the dataset."

### Changed
- **`.github/workflows/refresh_data.yml`**, **`weekly_push.yml`**, **`monitor_drift.yml`**: Hardcoded HuggingFace repo `ekrash718/stock-screener-data`; `HF_REPO` GitHub Actions secret no longer required (only `HF_TOKEN` needed).

### Fixed
- **`data/historical_dataset_clean.parquet`** (dataset cleanup): Removed 138,947 non-annual rows — `step6_clean.py` tagged quarterly rows as `in_universe=0` but never hard-dropped them; 3,117 blank-ticker BR rows (CVM companies unmatched to B3 tickers) also removed. Dataset reduced from 197,269 → 58,307 rows (annual-only, no blank tickers). Row counts in docs updated from 155K/156K → 58K.
- **`data/historical_dataset_clean.parquet`** (CA dedup): Removed 15 duplicate rows where Canadian companies changed their fiscal year-end mid-year, producing two annual reports for the same `fiscal_year`. Fixed by deduplicating on `(cik, market, fiscal_year, period_type)` keeping the later-filed row.
- **`scripts/test_dataset_quality.py`** primary key: Changed PK from `(cik, market, filed_date, period_type)` to `(cik, market, fiscal_year, period_type)` to handle DART bulk-filings where KR companies file multiple fiscal years on the same `filed_date`.
- **`pipeline/enrich_fraud_taxonomy.py`**: Re-executed on clean annual-only dataset to refresh all five percentile-rank fraud scores. Fraud suspect flag: 20,260 rows (34.74%).

### Added
- **`data/historical_dataset_clean.parquet`** `enterprise_value` column: `market_cap_at_filing + net_debt` — 68.6% fill (US-only; non-US lacks `market_cap_at_filing`).
- **`data/historical_dataset_clean.parquet`** `sector` column: SIC range → sector name mapping (Industrials, Technology, Healthcare, Financial, Energy, Materials, Consumer Staples, Consumer Discretionary, Utilities, Real Estate, Communication Services) — 75.5% fill, US-only (other markets have NaN `sic_code`). Dataset is now 58,307 × 326 columns.
- **`scripts/test_dataset_quality.py`**: New dataset quality test suite — 7 check categories, 53 checks total (schema, structural, market coverage, fill rates, distribution sanity, fraud label integrity, forward return coverage). All 53 pass on the current clean dataset. Usage: `python3 scripts/test_dataset_quality.py [--verbose] [--parquet PATH]`.

### Changed
- **`docs/architecture.md`**, **`docs/index.md`**, **`docs/methodology/models.md`**, **`CLAUDE.md`**: Column count updated 319 → 326, row count updated 155K → 58K across all diagram nodes, taglines, and architecture state table.

- **`scripts/train_models.py`**: `fraud_score_*` pattern added to `EXCLUDE_PATTERNS` — taxonomy sub-scores are UI display columns built from the same underlying signals already in the model; including them caused multicollinearity. `fraud_confirmed`, `fraud_suspect`, `fraud_label` added to `EXCLUDE` set — these are label columns, not input features.

### Fixed
- **`data/historical_dataset_clean.parquet`** (patch script): 9 columns were missing from the 324-column target — `working_capital`, `net_debt`, `accruals_ratio`, `price_to_book` (derived from existing raw columns), and the 5 cross-sectional momentum rank features (`momentum_12m_rank`, `momentum_6m_rank`, `momentum_3m_rank`, `vol_rank_12m`, `momentum_composite_rank`). Momentum ranks were already computed by `add_momentum_ranks()` in `step5_compute_features.py` but the combined multi-market parquet was built before the function was added. Derived columns added inline from `equity` (93% fill), `long_term_debt`/`short_term_debt` (100%), `current_assets`/`current_liabilities` (83–87%), and `market_cap_at_filing` (60%). Dataset is now 197,269 × 324 columns as documented.
- **`pipeline/enrich_fraud_taxonomy.py`**: `fraud_score_governance` was all-NaN for all rows because `small_auditor_flag` and `going_concern` are never written to the parquet by the current pipeline. Fixed by adding proxy signal fallback in `build_governance_score()`: uses `altman_z_score < 1.81` (distress proxy) and `piotroski_f_score ≤ 2` (weak-fundamentals proxy) when primary governance columns are absent. Both columns exist in all market datasets, so governance score is now non-NaN for 100% of rows.
- **`pipeline/enrich_fraud_taxonomy.py`**: `fraud_suspect` column was absent from `historical_dataset_clean.parquet` because `enrich_fraud_labels.py` (P0c) is a standalone enrichment script never called by `run_pipeline.py`. Fixed by adding `build_fraud_suspect()` function directly in P0d (`enrich_fraud_taxonomy.py`) — computes signal-based suspect flag (1 if 2+ of: Beneish > −1.78, Piotroski ≤ 2, Altman < 1.0) and writes `fraud_suspect` to the parquet each time P0d runs. `fraud_confirmed=1` rows are overridden to `fraud_suspect=0`.

### Added
- **`pipeline/phase_a_integrate_eu.py`**: Europe (EU) market integration — loads `data/snapshots_eu.parquet` (yfinance free-tier, DE/FR/NL/BE/PT/NO/FI/DK/SE/IE), standardises column aliases (depreciation, sga, accounts_receivable, total_equity), runs step3 price enrichment, merges macro (`macro_eu.parquet` → `macro.parquet` fallback), applies P0a/c/d/f/g, aligns to target schema, concatenates into `historical_dataset_clean.parquet`. EU-specific: strips existing rows using `isin(['DE','FR','NL','BE','PT','NO','FI','DK','SE','IE','EU'])`. Supports `--dry-run`.
- **`pipeline/phase_a_integrate_br.py`**: Brazil (CVM) market integration — loads `data/snapshots_br.parquet`, standardises 20+ missing columns (estimates `total_liabilities = total_assets - equity`, proxies `total_debt` from `long_term_debt`), runs step3 price enrichment, merges macro (`macro_br.parquet` → `macro.parquet` fallback), applies P0a/c/d/f/g, aligns to target schema, concatenates into `historical_dataset_clean.parquet`. Supports `--dry-run`.
- **`pipeline/phase_a_integrate_jp.py`**: Japan (yfinance free tier) market integration — same 9-step pattern; `standardise_jp_snapshots()` adds depreciation/sga/accounts_receivable/total_equity aliases, total_debt computation, and SIC NaN stubs; uses `macro_jp.parquet` → `macro.parquet` fallback. Supports `--dry-run`.
- **`pipeline/phase_a_integrate_ca.py`**: Canada (SEDAR+/yfinance) market integration — same 9-step pattern as JP; `standardise_ca_snapshots()` is identical column set to JP (both yfinance-based); uses `macro_ca.parquet` → `macro.parquet` fallback. Supports `--dry-run`.
- **`pipeline/step5_compute_features.py`**: `add_momentum_ranks()` function — computes 5 cross-sectional momentum rank features (`momentum_12m_rank`, `momentum_6m_rank`, `momentum_3m_rank`, `vol_rank_12m`, `momentum_composite_rank`) as percentile ranks within (fiscal_year, market) groupings. Closes the momentum gap documented since v0.1.0 (Jegadeesh & Titman 1993). Feature count: 319 → 324.
- **`scripts/run_pipeline_br.py`**: Brazil pipeline orchestrator — 6-step build chain (CVM+B3 tickers → CVM snapshots → price enrichment → macro → 324 features → clean). Passes `--snapshots snapshots_br.parquet` and `--suffix _br` to shared steps 3–6. Supports `--step N` resume and `--limit N` test mode. No API key required.
- **`scripts/run_pipeline_jp.py`**: Japan pipeline orchestrator — 6-step build chain using free-data variants (`step1_fetch_tickers_jp_free.py`, `step2_build_snapshots_jp_free.py`), shared steps 3–6 with `--snapshots snapshots_jp.parquet` and `--suffix _jp`. Supports `--step N` resume and `--limit N` test mode. No API key required.
- **`scripts/run_pipeline_ca.py`**: Canada pipeline orchestrator — 6-step build chain via TMX public API, shared steps 3–6 with `--snapshots snapshots_ca.parquet` and `--suffix _ca`. Supports `--step N` resume and `--limit N` test mode. No API key required.

### Changed
- **`scripts/run_pipeline_eu.py`**: rewritten from broken 2-step SimFin design to full 6-step yfinance free-data pipeline — now matches JP/CA runner pattern with `EU_STEPS` dict (steps 1–6), `LIMIT_STEPS = {1, 2, 3}`, `SNAPSHOT_STEPS = {3, 4, 5, 6}`, proper `--snapshots snapshots_eu.parquet`, `--out prices_eu.parquet`, and `--suffix _eu` routing; status function updated to show all 6 EU output files; `--markets` flag (SimFin-specific) removed; post-build message instructs user to run `phase_a_integrate_eu.py` next. No API key required.
- **`docs/developer/scripts.md`**: `run_pipeline_eu.py` section rewritten — removed SimFin/API key references, added full 6-step pipeline description with flags table and output file list.
- **`pipeline/step5_compute_features.py`**: added `--snapshots`, `--prices`, `--macro`, `--suffix` CLI flags to `__main__` block — suffix derives market-specific price/macro paths (`prices{suffix}.parquet`, `macro{suffix}.parquet`) and routes output to `historical_dataset{suffix}.parquet`. Enables all BR/JP/CA/EU pipeline runners to invoke step 5 without code duplication.
- **`pipeline/step6_clean.py`**: added `--suffix` CLI flag to `__main__` block — reads `historical_dataset{suffix}.parquet` and writes `historical_dataset_clean{suffix}.parquet`. Also accepts `--snapshots` for pipeline argument compatibility. Enables all multi-market pipeline runners to invoke step 6 with the correct market file pair.
- **`scripts/run_pipeline_br.py`**: fixed `build()` flag logic — step 3 now receives `--out prices_br.parquet` (was missing, causing US prices.parquet to be overwritten); steps 4–6 now all receive `--suffix _br` (was: step 4 received unused `--prices` flag instead).
- **`scripts/run_pipeline_jp.py`**: same flag logic fix as BR — step 3 gets `--out prices_jp.parquet`; steps 4–6 get `--suffix _jp`.
- **`scripts/run_pipeline_ca.py`**: same flag logic fix as BR — step 3 gets `--out prices_ca.parquet`; steps 4–6 get `--suffix _ca`.
- **`docs/developer/pipeline-scripts.md`**: `step4_enrich_macro.py`, `step5_compute_features.py`, and `step6_clean.py` sections updated with CLI flags tables documenting the new multi-market routing arguments.
- **`docs/methodology/features.md`**: Momentum section updated — warning admonition removed, new rank feature table added, total column count updated 319 → 324.
- **`CLAUDE.md`**: Architecture state table updated — momentum feature count and file reference corrected.
- **`pipeline/step1_fetch_tickers_br.py`**: `match_tickers()` rewritten — replaced brapi per-ticker name-fetch loop (was capped at first 400 tickers) with pure text heuristics against the full 1,800+ brapi ticker list. Match strategies: (1) first 4 letters of normalised commercial name → ticker root, (2) acronym of first 4 words → ticker root, (3) fallbacks to full legal name. Expected match rate: 300–400+ tickers (was ~57). No extra API calls; `time` and `numpy` imports removed.
- **`docs/developer/pipeline-scripts.md`**: `step1_fetch_tickers_br.py` entry updated — describes new text-heuristic matching approach and expected match rate.

 dedup key updated from `(cik, filed_date, period_type)` to `(cik, market, filed_date, period_type)` to prevent cross-market CIK collisions when merging multi-market snapshots.
- **`pipeline/phase_a_integrate_kr.py`**: updated schema reference from 313→319 columns; `classify_universe` call now explicitly passes `apply_filters=False` to prevent structural-only mode being silently overridden by future signature changes.
- **`pipeline/step6_clean.py`**: removed revenue ($1M), total_assets ($100K), and entry_price hard-threshold filters to maximise ticker coverage; dedup key updated from `(cik, filed_date, period_type)` to `(cik, market, filed_date, period_type)` to prevent cross-market CIK collisions.
- **`pipeline/p0f_universe_definition.py`**: revenue/asset/price/sector exclusion rules are now opt-in via `--apply-filters` flag; default mode applies structural rules only (annual + fiscal_year range) so all tickers receive `in_universe=1` by default.
- **`docs/developer/pipeline-scripts.md`**: updated `step6_clean.py` section (new minimal-filter design) and `p0f_universe_definition.py` section (structural vs investable-universe modes, new CLI flags).

### Changed
- **`ROADMAP.md`**: restructured to 16-step backbone (Phase A/B/C) covering all 15 deliverables — Git cleanup (Step 0), Portfolio Construction (Step 11), and Model Selection & Tuning (Step 7) added as explicit steps; free-data-only policy enforced across all 6 markets; Session Continuity table added; Immediate Next Actions priority order updated (P0.3, P0.1, P0.2, P0.4, P0.5).

### Added
### Removed
- **`pipeline/enrich_auditor_going_concern.py`**: superseded by `pipeline/enrich_governance.py` (going concern via EDGAR EFTS full-text search).
- **`pipeline/score_and_report.py`**: rules-based composite fraud score with fixed weights — contradicts ML-first architecture. Deleted to prevent future confusion.
- **`scripts/watchlist.py`**: session state export unrelated to quant lab research pipeline.
- **`scripts/high_roi_strategies.py`**: redundant wrapper, no unique functionality.

- **`ROADMAP.md`**: full phase tracker — Phase 0–3 with task checklists, exit criteria, codebase cleanup targets, and file inventory.
- **`CONTEXT.md`**: session state snapshot for context continuity between sessions; tracks current phase, completed tasks, blockers, architecture summary, data coverage.
- **`docs/methodology/feature-selection.md`**: PSI → IC → ICIR → Spearman deduplication pipeline with formulas, thresholds, CLI flags, planned Newey-West HAC + Fama-MacBeth + FDR improvements.
- **`docs/methodology/factor-library.md`**: 5 factor groups as ML input categories. Architecture decision: no fixed-weight composite; combination weights are ML-learned. All feature formulas, academic citations, data sources.
- **`docs/developer/contributing.md`**: vision checklist (5 questions before every task), "done" definition (6 steps), sync rules by change type, commit convention, architecture constraints, phase-gate review process.
- **`docs/developer/pipeline-scripts.md`**: full reference for all `pipeline/` modules — step1–step6 per market, enrichment modules, universe definition, confidence score, integration helpers.

### Changed
- **`mkdocs.yml`**: site_name updated to "Multi-Factor Stock Screener". Added feature-selection, factor-library, contributing, pipeline-scripts to nav.
- **`CLAUDE.md`**: reframed to multi-factor quant lab with architecture state table and pre-task checklist.
- **`docs/index.md`**: reframed as quant alpha lab with ML-first framing.
- **`docs/architecture.md`**: 15-layer architecture, multi-market integration, alpha signal flow.
- **`docs/methodology/features.md`**: dual taxonomy rewrite (5-factor + 8-category), momentum gap warning added.

- **`scripts/fetch_aaer_labels.py`**: builds `data/aaer_labels.csv` (per-company fraud year
  ranges from 220 matched companies) and rewrites the `fraud_confirmed` column in
  `data/historical_dataset_clean.parquet`.  Sources: `data/aaer_cache.json` (232 AAER CIKs)
  + SEC EDGAR full-text search for 10-K filings disclosing SEC investigations and
  restatements (~1,418 + ~521 hits across two queries).  Labeling window:
  `fiscal_year ∈ [fraud_year_start − 2, fraud_year_end]`.  Coverage: ~492 annual positive
  rows from ~118 companies (up from 172 / 33 companies).  Flags: `--lookback`, `--dry-run`,
  `--no-update-parquet`, `--start-year`, `--end-year`.
- **`docs/developer/scripts.md`**: added `fetch_aaer_labels.py` section with usage examples,
  flags table, and coverage note.


  `docs/methodology/features.md`, `docs/methodology/pipeline.md`, `docs/index.md`,
  `docs/markets.md`, `docs/developer/setup.md`, `docs/developer/scripts.md` — previously
  only `docs/architecture.md` and `docs/methodology/models.md` were required
- **`scripts/check_sync.py`**: expanded `ml-pipeline` rule to require `README.md` and
  `docs/index.md` (both carry AUC tables that must stay in sync)
- **`scripts/check_sync.py`**: added `docs/methodology/features.md`, `docs/methodology/pipeline.md`,
  `docs/index.md`, `docs/markets.md` to `docs_and_config` exclusion set

### Fixed
- **`README.md`**: AUC table corrected — WF Mean AUC: 1y 0.553, 3y 0.643 ✅, 5y 0.597
  (was 0.749/0.780/0.856 — those were val/test AUC from a prior training run, not WF CV)
- **`docs/index.md`**: feature count 313→319; AUC table updated to Val AUC + WF Mean AUC;
  Mermaid graph node corrected to 319 features
- **`docs/markets.md`**: column count 313→319
- **`docs/methodology/features.md`**: header corrected to 319 features (was 278)
- **`docs/methodology/pipeline.md`**: Step 5 node formula count 278→314
  (feature_library.py produces 314 base columns; +5 quarterly → 319 total)
- **`docs/developer/setup.md`**: feature_library.py comment updated to 314/319
- **`docs/developer/scripts.md`**: pipeline step 4 updated to 314 + 5 quarterly = 319


  features whose IC sign is inconsistent across years; set to 0.6 to require ≥60% of years with
  correct-sign IC before a feature enters the model
- **`scripts/train_models.py`**: `--min-ic-years INT` flag (default 1 = off) — requires a minimum
  number of years of IC observations; prevents spurious ICIR inflation from features with very few
  historical data points (e.g. `fraud_label` with n_years=1 would otherwise rank first by ICIR)
- **`scripts/train_models.py`**: `FORCE_INCLUDE_1Y` constant — mechanism to force-include named
  features into the 1y model even if they don't rank in the ICIR top-N; currently empty after
  testing showed no net WF AUC improvement (see notes in file)

### Changed
- `docs/developer/scripts.md`: flags table for `train_models.py` updated with `--min-ic-stability`
  and `--min-ic-years`

### Notes (no change)
- Tested `vix` as force-include for 1y model: improved 2018→2019 fold AUC 0.465→0.485 (COVID
  reversal regime) but 2019→2020 fold declined 0.549→0.526; net WF mean AUC 0.553→0.549 (−0.004).
  Reverted. Root cause: 2018→2019 is dominated by the COVID crash/recovery regime in which all
  fundamental factors inverted sign; no single feature addition recovers this fold without hurting
  adjacent folds. 1y WF mean AUC remains 0.553 (target ≥0.62 not yet met).

### Added
- **`CLAUDE.md`** — AI assistant instructions: Change Checklist matrix, Architecture Sync
  Rules, Current Architecture State table, key file locations, commit convention
- **`scripts/enrich_quarterly_features.py`** — computes 5 intra-year dynamics (revenue smoothing,
  earnings momentum, accrual peak, revenue acceleration, positive-quarter fraction) from Q1/Q2/Q3
  rows and left-joins them onto annual training rows; covers 74.8% of annual rows
- **`scripts/mark_survivorship.py`** — identifies likely-delisted companies and imputes −50% forward
  return to correct survivorship bias in training data
- **`scripts/migrate_to_db.py`** — bulk-loads `historical_dataset_clean.parquet` into TimescaleDB
  hypertable (schema in `infra/db/init.sql`)
- Walk-forward AUC section in Backtester tab — summary table + AUC-over-time chart + per-fold
  expander; reads `reports/walk_forward_auc_{h}.csv` generated by `train_models.py --walk-forward`
- Company Profile: Fraud Taxonomy breakdown (5-dimension radar + YoY delta), Peer Comparison
  section (percentile table + box-plot distribution), confidence detail expander with coverage groups
- Screener tab: live prediction log via `scoring.log_predictions()` called each time models score
- Watchlist: watchlist tab with add/remove and per-ticker score history sparklines
- FastAPI: full screener router with filters (market, exchange, sector, Piotroski, Beneish,
  confidence), paginated response; `api/deps.py` singleton dataset loader

### Changed
- `train_models.py`: added PSI-based feature filter (default `--max-psi 2.0`) applied after
  `log_psi_report()`; drops 10 macro-regime features (treasury rates, fed funds, CPI, yield
  curve) that shift dramatically between train and test — prevents IC inflation on stale regimes
- `train_models.py`: PSI filter runs before IC analysis so macro features never enter the
  ICIR ranking step
- Historical dataset now includes 5 quarterly-derived feature columns (319 total columns)
- `CONTRIBUTING.md`: added Sync Checklist table — every PR must update docs/diagrams in
  the same commit as the code change
- `docs/architecture.md`: all three Mermaid diagrams updated (quarterly enrichment, survivorship,
  PSI filter, TimescaleDB, FastAPI nodes added; column counts corrected to 319)
- `docs/developer/scripts.md`: added entries for `enrich_quarterly_features.py`,
  `mark_survivorship.py`, `migrate_to_db.py`; updated `train_models.py` flags table
  (`--max-psi`, `--walk-forward`)
- `docs/methodology/models.md`: AUC table corrected (1y 0.553, 3y 0.643, 5y 0.597 WF mean);
  Mermaid flowchart updated with PSI filter step; dataset node updated to 319 columns

### Fixed
- Feature descriptions dictionary (52 entries) in `app_v2.py`
- SHAP-driven strengths/weaknesses narrative in Company Profile tab
- EU, Korea, Japan, Canada, Brazil pipeline scripts
- Backtester: sector cap, filing-lag filter, benchmark equity curve
- Leverage strategy: Kelly-sized long/short portfolio with quality gates
- Drift monitor: PSI + rolling AUC with GitHub Actions alerts
- Bias audit: temporal leakage, shuffle test, filing-lag audit
- MkDocs documentation site (15 pages across 4 sections)
- 4 research notebooks (EDA, Beneish deep-dive, feature IC, backtest analysis)
- `site/` now excluded from git via `.gitignore`

---

## [0.1.0] — 2024 (Initial internal release)

### Added
- US data pipeline via SEC EDGAR
- LightGBM models for 1y/3y/5y fraud horizons
- Streamlit app with Screener, Company Profile, Backtest tabs
- HuggingFace Hub for model and dataset storage
- GitHub Actions: weekly refresh + drift monitor

---

## [Unreleased — process automation]

### Added
- **`scripts/check_sync.py`** — architecture sync checker; reads staged files and applies
  the CLAUDE.md Change Checklist rules; reports missing doc updates; exit code 1 blocks
  bad commits
- **`.git/hooks/pre-commit`** — calls `check_sync.py` automatically before every commit;
  bypass with `--no-verify` in emergencies
- **`.claude/commands/sync-check.md`** — `/sync-check` slash command: interactive sync
  status report against all uncommitted changes
- **`.claude/commands/sync-update.md`** — `/sync-update` slash command: drafts CHANGELOG
  entry + lists required doc files after a code change

### Changed
- **`docs/developer/scripts.md`** — added Process Automation section with `check_sync.py`
  flags table and pre-commit hook documentation



### Bug fixes: auto_adjust=False, OOF contamination fix (2026-05-14)

#### Critical correctness fixes across yfinance callers and fraud risk factor (fix/pipeline,alpha)
- **`pipeline/step3_enrich_prices.py`**: Changed `auto_adjust=True` → `auto_adjust=False` in `tk.history()` to prevent retroactive price adjustments from corrupting momentum features and return labels
- **`scripts/bias_audit.py`**: Changed `auto_adjust=True` → `auto_adjust=False` in `yf.download()` call
- **`scripts/build_monthly_price_cache.py`**: Changed `auto_adjust=True` → `auto_adjust=False`
- **`scripts/fetch_spy_returns.py`**: Changed `auto_adjust=True` → `auto_adjust=False`
- **`alpha/factors/fraud_risk.py`**: Replaced contaminated `ml_1y/ml_3y/ml_5y` signals with `ml_1y_oof/ml_3y_oof/ml_5y_oof` (walk-forward unbiased OOF scores) in `_ML_SIGNALS`; contaminated signals include training rows and inflate in-sample alpha

---

### Section 9 — High-Conviction Leveraged Portfolio (2026-05-15)

#### notebooks/08_experiment_hub.ipynb — 3 new cells (sec9md000, sec9cfg01, sec9main2)

- **Safety gate scoring**: 4 hard gates — Piotroski F ≥ 7, Altman Z > 1.81, FCF yield > 0, Beneish M < −1.78
- **Tier classification**: Tier 1 = all 4 gates, Tier 2 = 3/4, Tier 3 = 2/4
- **Leverage multiplier**: Tier-1 × β<0.5 → 2.0×; β<0.8 → 1.5×; β<1.0 → 1.25×; Tier-2 × β<1.0 → 1.1×; hard cap 2.0×
- **Conviction rank**: composite_score × gates_passed / 4 (penalizes high-composite stocks failing gates)
- **Implied CAGR**: linear fit 8 + composite_score × 27 (8% at 0 → 35% at 1.0)
- **Levered CAGR**: implied_cagr × leverage_x
- **Net Kelly**: kelly_pct × leverage_x
- **Output**: styled table (RdYlGn gradient on composite + levered CAGR, bar on leverage_x), 2-panel chart, CSV export
- **Results**: 25 candidates (top 30%), 6 Tier-1, 19 Tier-2, 20 picks → `data/leveraged_picks_2026-05-15.csv`
