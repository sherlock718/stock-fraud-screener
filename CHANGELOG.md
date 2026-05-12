# Changelog

All notable changes to this project are documented here.

Format: [Semantic Versioning](https://semver.org). Each release section covers the most recent sprint.

---

## [Unreleased]

### Fixed (Phase A/B — equity coalesce bug + dataset patch)
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

