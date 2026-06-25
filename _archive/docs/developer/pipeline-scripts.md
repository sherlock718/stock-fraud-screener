# Pipeline Modules Reference

All modules live in `pipeline/`. These are low-level building blocks consumed by `scripts/workflows/run_pipeline.py` and the multi-market pipeline. Run them directly only when you need to re-run a specific step in isolation.

---

## Pipeline Step Modules

The core 6-step pipeline has one primary module per step, plus market-specific variants for non-US markets.

### `step1_fetch_tickers.py` — Fetch Ticker Universe

Fetches the company universe for a given market. Each market variant hits a different data source.

| Module | Market | Source |
|---|---|---|
| `step1_fetch_tickers.py` | US | SEC EDGAR `/submissions` endpoint (10-K filers) |
| `step1_fetch_tickers_eu.py` | EU | Wikipedia index constituent tables (DAX, CAC, AEX, IBEX, etc. — 13 indices) |
| `step1_fetch_tickers_kr.py` | KR | DART corp_code XML zip download + `/company` enrichment |
| `step1_fetch_tickers_jp.py` | JP | EDINET API — requires free `EDINET_API_KEY` in `.env` |
| `step1_fetch_tickers_jp_free.py` | JP | Static curated Nikkei 225 + TSE Prime list (~130 tickers, no API key) |
| `step1_fetch_tickers_ca.py` | CA | TMX public API (TSX + TSXV company directory, no API key required) |
| `step1_fetch_tickers_br.py` | BR | CVM company register + brapi.dev ticker list — text heuristic matching |

**BR matching** (`step1_fetch_tickers_br.py`): Downloads the CVM company register (~353 active BOLSA companies) and the full brapi.dev ticker list (1,800+ symbols, `/api/available` only). Matches CVM names to 4-letter B3 ticker roots using 6 strategies: (1) first 4 letters of normalised commercial name, (2) first 4 letters of first meaningful word, (3) 4-letter acronym, (4) 2+2 acronym, (5) second word prefix, (6) 3-letter unique match. Stop-words (`BANCO`, `CIA`, `HOLDING`, `PARTICIPACOES`, etc.) are filtered before matching. A `CURATED_OVERRIDES` dict provides confirmed tickers for 11+ companies with acronym-based symbols (BBDC3, BBAS3, CMIG3, BRSR3, etc.). Regex covers share classes 3–9. Match rate: 112 / 353 (32%).

Output: `data/tickers_{market}.csv` — columns: `ticker, cik, company_name, exchange, sic_code, country`

---

### `step2_build_snapshots.py` — Build Annual Financial Snapshots

For each ticker, fetches annual financial statements and aligns them by `fiscal_year`. One row = one company × one fiscal year.

| Module | Market | Notes |
|---|---|---|
| `step2_build_snapshots.py` | US | SEC EDGAR XBRL |
| `step2_build_snapshots_eu.py` | EU | yfinance, no API key — 4–5 years history across DE/FR/NL/BE etc. |
| `step2_build_snapshots_kr.py` | KR | DART API — requires `DART_API_KEY` |
| `step2_build_snapshots_jp_free.py` | JP | **Active** — yfinance, no API key. ~122–130 tickers |
| `step2_build_snapshots_jp.py` | JP | Optional — free EDINET API key required; 3,800+ TSE tickers |
| `step2_build_snapshots_ca.py` | CA | SEDAR+ XBRL filings + yfinance fallback |
| `step2_build_snapshots_br.py` | BR | CVM + brapi.dev |

**JP note**: `step2_build_snapshots_jp_free.py` is the default active variant and outputs `data/snapshots_jp.parquet`. For full TSE coverage (3,800+ tickers), use `step2_build_snapshots_jp.py` with a free EDINET API key.

Output: `data/snapshots_{market}.parquet`

---

### `step3_enrich_prices.py` — Enrich with Price Data

Downloads daily OHLCV from yfinance and computes forward returns and valuation ratios at fiscal year-end.

Added columns:
- `return_12m`, `return_24m`, `return_36m` — total return windows
- `excess_return_12m` — stock return minus local index return
- `beta_12m` — rolling beta vs local index
- `price_volume_ratio` — average daily dollar volume (3-month)
- `price_to_book`, `ev_ebitda`, `pe_ratio` — valuation ratios at fiscal year-end

---

### `step4_enrich_macro.py` — Enrich with Macro Data

Fetches 9 FRED series at startup (9 API calls total), builds a daily forward-filled macro panel,
then joins it to each snapshot row via `pd.merge_asof` (O(1) lookup per row).

**FRED series joined**:

| Column | FRED ID | Description |
|---|---|---|
| `treasury_10y` | DGS10 | 10-year Treasury yield (%) |
| `treasury_2y` | DGS2 | 2-year Treasury yield (%) |
| `yield_curve` | T10Y2Y | 10y − 2y spread (%) |
| `fed_funds_rate` | FEDFUNDS | Fed Funds Rate (%) |
| `credit_spread_baa` | BAA10Y | Baa corporate − 10y Treasury (%) |
| `hy_spread` | BAMLH0A0HYM2 | ICE BofA High Yield OAS (%) |
| `cpi_yoy` | CPIAUCSL | CPI YoY % change (computed) |
| `recession` | USREC | NBER recession indicator (0/1) |
| `vix` | VIXCLS | CBOE VIX |
| `real_rate_10y` | — | treasury_10y − cpi_yoy |
| `credit_tightening` | — | 6-month change in Baa spread |
| `macro_regime` | — | 0=low, 1=rising, 2=high, 3=recession |

Requires `FRED_API_KEY` env var (free registration at fred.stlouisfed.org). Without it, all macro
columns are written as `NaN`.

**CLI flags** (for multi-market use):

| Flag | Default | Description |
|---|---|---|
| `--snapshots PATH` | `data/snapshots.parquet` | Input snapshots file |
| `--suffix STR` | `''` | Market suffix, e.g. `_br`; sets output to `macro{suffix}.parquet` |

Output: `data/macro.parquet` (or `data/macro{suffix}.parquet`)

---

### `step5_compute_features.py` — Compute Features

Calls `feature_library.py` to compute 314 base features across 8 categories, plus 5 cross-sectional momentum rank features (324 total). Also computes Montier C-score (7 columns) and Sloan decomposition columns not in the shared library. All computations are purely cross-sectional within a fiscal year — no look-forward information is used.

**Montier C2 note**: `add_montier_c_score()` uses `ppe_net` (19.4% null) for the C2 depreciation-rate signal. Do **not** revert to `property_plant_equipment` (95.7% null) — doing so makes `montier_c2` and `montier_c_score` 100% null.

See [Feature Engineering →](../methodology/features.md) for the full feature list by category.

**CLI flags** (for multi-market use):

| Flag | Default | Description |
|---|---|---|
| `--snapshots PATH` | `data/snapshots.parquet` | Input snapshots file |
| `--prices PATH` | `data/prices{suffix}.parquet` | Input prices file (derived from suffix if omitted) |
| `--macro PATH` | `data/macro{suffix}.parquet` | Input macro file (derived from suffix if omitted) |
| `--suffix STR` | `''` | Market suffix, e.g. `_br`; sets output to `historical_dataset{suffix}.parquet` |

---

### `step6_clean.py` — Clean and Normalise Dataset

Applies minimal structural filters to preserve maximum ticker coverage. Revenue, asset, and price
thresholds have been removed — all tickers are kept regardless of size or liquidity.

| Operation | Detail |
|---|---|
| Required cols | Drop rows missing `cik`, `ticker`, `filed_date`, `fiscal_year`, `period_type` |
| Date filter | Drop rows with invalid or pre-2008 `filed_date` |
| Dedup | Drop duplicate `(cik, market, filed_date, period_type)` rows (keep first) |
| Inf → NaN | Replace `±inf` with `NaN` across all numeric columns |

**CLI flags** (for multi-market use):

| Flag | Default | Description |
|---|---|---|
| `--suffix STR` | `''` | Market suffix, e.g. `_br`; reads `historical_dataset{suffix}.parquet`, writes `historical_dataset_clean{suffix}.parquet` |
| `--snapshots PATH` | — | Accepted for pipeline compatibility; not used |
| PIT columns | Add `as_of_date` (alias of `filed_date`) and `filing_lag_days` |

Use `p0f_universe_definition.py --apply-filters` to compute an investable-universe subset.

Output: `data/historical_dataset_clean.parquet`

---

## Feature Engineering Modules

### `feature_library.py` — Shared Feature Engineering

Single source of truth for all feature formulas. Consumed by both `step5_compute_features.py` (pipeline) and `scripts/modeling/train_models.py` (ML training). Modifying a formula here changes behaviour in both the data pipeline and the model.

Contains 314 base feature formulas across 8 categories:
1. Accruals and earnings quality
2. Value and valuation ratios
3. Profitability and margins
4. Leverage and solvency
5. Growth and momentum
6. Efficiency and turnover
7. Forensic accounting signals
8. Macro / context features

See [Feature Engineering →](../methodology/features.md) for the complete annotated list.

---

### `value_metrics.py` — Investment Ratio Library

> **⚠️ ARCHIVED (Session 8)** — Moved to `pipeline/archive/value_metrics.py`. Logic fully duplicated in `step5_compute_features.py` § A+B. Kept here for formula reference only.

Computes investment analysis ratios that are **not** used in the fraud score but are displayed in the UI and used as ML features in the 5-factor alpha score.

Metrics: P/E, P/B, EV/EBITDA, FCF Yield, ROE, ROA, Gross Margin, Net Margin, Debt/Equity, Current Ratio, plus Greenblatt (ROIC, Earnings Yield), Carlisle (Acquirer's Multiple), Graham (NCAV), and Novy-Marx (Gross Profitability) extensions.

---

## Fraud Signal Modules

### `fraud_signals.py` — Fraud Score Computation

> **⚠️ ARCHIVED (Session 8)** — Moved to `pipeline/archive/fraud_signals.py`. Logic fully duplicated in `step5_compute_features.py` § C+D. Kept here for formula reference only.

Computes the forensic accounting fraud signals that feed into the Fraud Risk factor.

| Signal | Formula | Threshold |
|---|---|---|
| Beneish M-Score | 8-variable manipulation index | > −1.78 = manipulator |
| Piotroski F-Score | 9-point financial health (0–9) | ≤ 3 = weak |
| Accruals Ratio | (ΔWC − ΔD&A) / avg_assets | High = red flag |
| Cash Flow Divergence | net_income − operating_CF | Large gap = red flag |
| Altman Z-Score | Bankruptcy predictor (5-variable) | < 1.81 = distress |
| Revenue Quality | Receivables ratio + DSO | High AR = fake revenue |
| Earnings Quality | Operating vs net income gap | Non-operating inflation |
| Going Concern | SEC filing disclosure flag | True = disclosed doubt |
| Auditor Quality | Big 4 vs small auditor for large co. | Small auditor flag |

---

## Enrichment Modules

These modules add additional signal columns to `historical_dataset_clean.parquet`. All are checkpointed — safe to interrupt and resume.

### `enrich_fraud_labels.py` — Fraud Confirmation Labels (P0c)

Adds `fraud_confirmed` and `fraud_suspect` binary columns by matching against SEC AAER releases and Stanford SCAC data. See also `scripts/data_io/fetch_aaer_labels.py` for the standalone AAER-only updater.

### `enrich_fraud_taxonomy.py` — Fraud Taxonomy Sub-Scores (P0d)

Adds five fraud-type sub-scores (0.0–1.0 each) plus the `fraud_suspect` signal flag:

| Column | Fraud mechanism |
|---|---|
| `fraud_score_accounting` | Earnings manipulation via accruals / channel-stuffing |
| `fraud_score_dilution` | Equity issuance abuse / dilution fraud |
| `fraud_score_quality` | Earnings quality / cash flow divergence |
| `fraud_score_distress` | Financial distress / going-concern risk |
| `fraud_score_governance` | Governance failures (auditor, board, going concern). Falls back to `altman_z_score < 1.81` and `piotroski_f_score ≤ 2` as proxies when primary governance columns (`small_auditor_flag`, `going_concern`) are absent. |
| `fraud_score_composite` | Weighted average of the five sub-scores (accounting 0.30, quality 0.25, distress 0.20, dilution 0.15, governance 0.10) |
| `fraud_suspect` | Signal-based flag (1 if 2+ of: Beneish > −1.78, Piotroski ≤ 2, Altman < 1.0). Overridden to 0 for `fraud_confirmed=1` rows. |

### `enrich_governance.py` — Governance Signals

> **⚠️ ARCHIVED (Session 8)** — Moved to `pipeline/archive/enrich_governance.py`. Going concern logic preserved in `pipeline/archive/STRANDED_LOGIC.md` for future migration.

Adds `going_concern` flag (True if SEC filing disclosed going concern doubt) via EDGAR EFTS full-text search. Replaces older `enrich_auditor_going_concern.py`.

### `enrich_insider_signals.py` — Insider Trading Signals

> **⚠️ ARCHIVED (Session 8)** — Moved to `pipeline/archive/enrich_insider_signals.py`. Insider signal logic preserved in `pipeline/archive/STRANDED_LOGIC.md` for future migration.

Fetches SEC Form 4 filings for the last 12 months and computes:
- `insider_sale_count`, `insider_buy_count`
- `net_insider_shares` (negative = net selling)
- `insider_selling_flag` (net sold > 10K shares AND sales > buys)

### `enrich_market_signals.py` — Market-Based Signals

> **⚠️ ARCHIVED (Session 8)** — Moved to `pipeline/archive/enrich_market_signals.py`. ADTV/volume/pump-dump logic preserved in `pipeline/archive/STRANDED_LOGIC.md` for future migration.

Adds liquidity and momentum signals:
- `avg_volume_90d`, `volume_spike_ratio` (30d/90d)
- `price_change_90d`
- `illiquid_flag` (volume < 10K/day)
- `pump_dump_flag` (volume spike > 3× AND price up > 50%)
- `volatility_90d`, `beta`, `bid_ask_spread`

### `enrich_market_cap.py` — Market Cap Gap Fill

> **⚠️ ARCHIVED (Session 8)** — Moved to `pipeline/archive/enrich_market_cap.py`. JSON-era market cap enrichment.

Fills missing `market_cap` values for companies where step3 price enrichment didn't produce a market cap. Uses yfinance.

### `enrich_auditor_going_concern.py` — Auditor Quality (Legacy)

Adds `going_concern`, `auditor_name`, `big4_auditor`, `small_auditor_flag`. Superseded by `enrich_governance.py` for going concern signals; auditor name via SEC XBRL is not reliably available.

### `enrich_feature_dictionary.py` — Feature Metadata

Builds a human-readable feature dictionary mapping column names to descriptions, data types, and factor group assignments.

---

## Universe and Confidence Modules

### `p0f_universe_definition.py` — Universe Definition (P0f)

Tags each row with `in_universe` (0/1) and `excl_reason`. Does **not** drop rows.

**Structural rules** (always applied):
1. `period_type == 'annual'`
2. `fiscal_year >= 2009` (XBRL coverage)
3. `fiscal_year <= current_year - 1` (completed years only)

**Investable-universe rules** (`--apply-filters` only):
4. `revenue >= $1M`
5. `total_assets >= $100K`
6. `entry_price > 0`
7. Exclude SIC 6000–6999 (financials) — different accrual structure
8. Exclude SIC 4900–4999 (utilities) — regulated earnings distort signals
9. Size: include micro/small/mid/large (exclude nano/shell)
10. Exclude OTC stocks with price < market floor (US: $1.00, CA: $0.05, others: none)

```bash
python3 pipeline/p0f_universe_definition.py                           # structural rules only
python3 pipeline/p0f_universe_definition.py --dry-run                 # report without saving
python3 pipeline/p0f_universe_definition.py --apply-filters           # full investable-universe
python3 pipeline/p0f_universe_definition.py --apply-filters --dry-run # report only
```

Output: `data/historical_dataset_clean.parquet` (updated in-place), `reports/universe_summary.csv`

---

### `p0g_confidence_score.py` — Data Confidence Score (P0g)

Adds a per-row `data_confidence` score (0.0–1.0) expressing trust in data quality. Distinct from fraud score — a row can be high-confidence and fraudulent.

Scored across three dimensions (equally weighted):
1. **Coverage** — fraction of core columns (financial statements, fraud signals, prices, ratios) that are non-null
2. **Source quality** — data source tier (EDGAR XBRL > SimFin > scraped)
3. **Recency** — penalty for very old filings (> 10 years)

```bash
python3 pipeline/p0g_confidence_score.py
```

---

## Integration and Build Modules

### `build_historical_dataset.py` — Merge All Markets into One Parquet

> **⚠️ ARCHIVED (Session 8)** — Moved to `pipeline/archive/build_historical_dataset.py`. Superseded by the step2→step6 pipeline chain. Multi-market merging is now handled by `scripts/run_pipeline_{market}.py` scripts.

Merges all market snapshot files into a single `historical_dataset_clean.parquet`. Called automatically by `scripts/workflows/run_pipeline.py` step 6. Run directly to re-merge without re-running the full pipeline.

```bash
python3 pipeline/build_historical_dataset.py  # ARCHIVED — use step6_clean.py instead
```

---

> **Note:** The `phase_a_integrate_*.py` scripts documented below were planned but never created on disk. The actual market integration workflow uses `scripts/run_pipeline_{market}.py build`. The documentation below preserves the intended design (column standardisation, enrichment steps) for future reference.

### `phase_a_integrate_kr.py` — Korea Market Integration

Integrates Korea (KR) DART snapshots into `historical_dataset_clean.parquet`. Runs steps 3–9 (price enrichment through universe definition) for KR tickers and concatenates onto the clean dataset.

```bash
python3 pipeline/phase_a_integrate_kr.py
```

Steps performed:
1. Load + standardise `data/snapshots_kr.parquet`
2. Run step3 price enrichment → `data/prices_kr.parquet`
3. Merge KR snapshots + prices + macro
4. Apply step5 feature functions
5. Apply P0a (filing_lag_days, as_of_date)
6. Apply P0c (fraud labels)
7. Apply P0d (fraud taxonomy sub-scores)
8. Apply P0f (universe classification)
9. Apply P0g (data confidence score)
10. Align to target schema + concatenate

---

### `phase_a_integrate_br.py` — Brazil Market Integration

Integrates Brazil (CVM/B3) snapshots into `historical_dataset_clean.parquet`. CVM data covers IFRS filings 2010–2025 for ~300–400 B3-listed companies.

```bash
python3 pipeline/phase_a_integrate_br.py
python3 pipeline/phase_a_integrate_br.py --dry-run
```

**Prerequisite**: `data/snapshots_br.parquet` — built by `scripts/workflows/run_pipeline_br.py build --step 2`.

Steps performed:
1. Load + standardise `data/snapshots_br.parquet` — adds ~20 missing columns as NaN; estimates `total_liabilities = total_assets − equity`; proxies `total_debt = long_term_debt + short_term_debt`
2. Run step3 price enrichment → `data/prices_br.parquet`
3. Merge BR snapshots + prices + macro (`macro_br.parquet` → `macro.parquet` fallback)
4. Apply P0a (filing_lag_days, as_of_date)
5. Apply step5 feature functions (10 functions, try/except per function)
6. Apply P0c fraud labels
7. Apply P0d fraud taxonomy sub-scores
8. Apply P0f universe classification
9. Apply P0g data confidence score
10. Align to target schema + concatenate

---

### `phase_a_integrate_jp.py` — Japan Market Integration

Integrates Japan (yfinance free tier) snapshots into `historical_dataset_clean.parquet`. Free tier covers ~122–130 major TSE tickers.

```bash
python3 pipeline/phase_a_integrate_jp.py
python3 pipeline/phase_a_integrate_jp.py --dry-run
```

**Prerequisite**: `data/snapshots_jp.parquet` — built by `scripts/workflows/run_pipeline_jp.py build --step 2`.

Column standardisation (`standardise_jp_snapshots`): adds `depreciation` alias from `depreciation_amortization`, `sga` from `sga_expense`, `accounts_receivable` from `receivables`, `total_equity` from `equity`; computes `total_debt = long_term_debt + short_term_debt`; stubs `sic_code` as NaN; adds NaN stubs for `ppe_gross`, `other_noncurrent_assets`, `non_operating_income`, `dividends_per_share`, `stock_code`, `currency`, `financing_cash_flow`, `total_liabilities`.

Macro: `macro_jp.parquet` → `macro.parquet` fallback.

---

### `phase_a_integrate_eu.py` — Europe Market Integration

Integrates Europe (EU) yfinance snapshots into `historical_dataset_clean.parquet`. Free-tier coverage spans major exchange tickers across DE, FR, NL, BE, PT, NO, FI, DK, SE, IE, and other European markets (~4–5 years of history).

```bash
python3 pipeline/phase_a_integrate_eu.py
python3 pipeline/phase_a_integrate_eu.py --dry-run
```

**Prerequisite**: `data/snapshots_eu.parquet` — built by `scripts/workflows/run_pipeline_eu.py build --step 2`.

Column standardisation (`standardise_eu_snapshots`): same yfinance-based aliases as JP/CA — adds `depreciation`, `sga`, `accounts_receivable`, `total_equity` aliases; computes `total_debt = long_term_debt + short_term_debt`; stubs `sic_code` as NaN; adds NaN stubs for `ppe_gross`, `other_noncurrent_assets`, `non_operating_income`, `dividends_per_share`, `stock_code`, `currency`, `financing_cash_flow`, `total_liabilities`. Estimates `total_liabilities = total_assets − equity` if missing.

EU-specific: existing rows are stripped via `isin(['DE', 'FR', 'NL', 'BE', 'PT', 'NO', 'FI', 'DK', 'SE', 'IE', 'EU'])` (EU snapshots carry per-country market codes, not a single 'EU' code).

Macro: `macro_eu.parquet` → `macro.parquet` fallback.

---

### `phase_a_integrate_ca.py` — Canada Market Integration

Integrates Canada (SEDAR+/yfinance) snapshots into `historical_dataset_clean.parquet`. Data covers TSX/TSXV listed companies.

```bash
python3 pipeline/phase_a_integrate_ca.py
python3 pipeline/phase_a_integrate_ca.py --dry-run
```

**Prerequisite**: `data/snapshots_ca.parquet` — built by `scripts/workflows/run_pipeline_ca.py build --step 2`.

Column standardisation (`standardise_ca_snapshots`): identical column aliases to JP (both yfinance-based field mappings). Adds `depreciation`, `sga`, `accounts_receivable`, `total_equity` aliases; computes `total_debt`; stubs `sic_code` as NaN.

Macro: `macro_ca.parquet` → `macro.parquet` fallback.

---

### `fetch_companies.py` — Company Metadata Fetcher

> **⚠️ ARCHIVED (Session 8)** — Moved to `pipeline/archive/fetch_companies.py`. Superseded by `step1_fetch_tickers.py`.

Fetches and caches company metadata (name, SIC code, exchange, CIK) from SEC EDGAR. Called internally by step1 and step2 modules.

### `market_cap_filter.py` — Market Cap Filter Utility

> **⚠️ ARCHIVED (Session 8)** — Moved to `pipeline/archive/market_cap_filter.py`. Superseded by `p0f_universe_definition.py`.

Shared utility used by backtester and screener to apply the minimum market cap floor. Default: $50M.

### `auto_update.py` — Auto-Update Orchestrator

> **⚠️ ARCHIVED (Session 8)** — Moved to `pipeline/archive/auto_update.py`. Broken imports (JSON-era). Refresh is now handled by `scripts/workflows/refresh_data.py` + GitHub Actions.

Orchestrates an incremental refresh of the dataset: downloads the existing parquet from HuggingFace, identifies new fiscal years available, runs only the new rows through the pipeline, and re-merges. Used by the GitHub Actions weekly refresh workflow.
