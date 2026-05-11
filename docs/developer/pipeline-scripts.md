# Pipeline Modules Reference

All modules live in `pipeline/`. These are low-level building blocks consumed by `scripts/run_pipeline.py` and the multi-market pipeline. Run them directly only when you need to re-run a specific step in isolation.

---

## Pipeline Step Modules

The core 6-step pipeline has one primary module per step, plus market-specific variants for non-US markets.

### `step1_fetch_tickers.py` — Fetch Ticker Universe

Fetches the company universe for a given market. Each market variant hits a different data source.

| Module | Market | Source |
|---|---|---|
| `step1_fetch_tickers.py` | US | SEC EDGAR `/submissions` endpoint (10-K filers) |
| `step1_fetch_tickers_eu.py` | EU | SimFin API `/companies/list` with IFRS flag |
| `step1_fetch_tickers_kr.py` | KR | DART `/list` endpoint with auth token |
| `step1_fetch_tickers_jp.py` | JP | TDNET (paid) |
| `step1_fetch_tickers_jp_free.py` | JP | TDNET free tier |
| `step1_fetch_tickers_ca.py` | CA | SEDAR+ scraper |
| `step1_fetch_tickers_br.py` | BR | B3/CVM listing |

Output: `data/tickers_{market}.csv` — columns: `ticker, cik, company_name, exchange, sic_code, country`

---

### `step2_build_snapshots.py` — Build Annual Financial Snapshots

For each ticker, fetches annual financial statements and aligns them by `fiscal_year`. One row = one company × one fiscal year.

| Module | Market | Notes |
|---|---|---|
| `step2_build_snapshots.py` | US | SEC EDGAR XBRL |
| `step2_build_snapshots_eu.py` | EU | SimFin API — requires `SIMFIN_API_KEY` |
| `step2_build_snapshots_kr.py` | KR | DART API — requires `DART_API_KEY` |
| `step2_build_snapshots_jp_free.py` | JP | **Active** — yfinance, no API key. ~122–130 tickers |
| `step2_build_snapshots_jp.py` | JP | Optional/paid — EDINET API key required; 3,800+ TSE tickers |
| `step2_build_snapshots_ca.py` | CA | TMX public API |
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

Joins macroeconomic data at the fiscal year-end date:
- `tbill_3m` — 3-month US Treasury yield (or local equivalent)
- `cpi_yoy` — CPI year-over-year inflation
- `gdp_growth` — real GDP growth rate
- `credit_spread` — investment grade credit spread

---

### `step5_compute_features.py` — Compute Features

Calls `feature_library.py` to compute 314 base features across 8 categories. All computations are purely cross-sectional within a fiscal year — no look-forward information is used.

See [Feature Engineering →](../methodology/features.md) for the full feature list by category.

---

### `step6_clean.py` — Clean and Normalise Dataset

| Operation | Detail |
|---|---|
| Winsorize | Cap outliers at 1st/99th percentile per feature per fiscal year |
| Impute medians | Replace NaN with industry-year median (SIC 2-digit × fiscal_year) |
| Type cast | Floats to float32, categoricals encoded |
| Period tag | Add `period_type=annual` flag |
| Dedup | Drop duplicate ticker × fiscal_year rows (keep first) |

Output: `data/historical_dataset_clean.parquet`

---

## Feature Engineering Modules

### `feature_library.py` — Shared Feature Engineering

Single source of truth for all feature formulas. Consumed by both `step5_compute_features.py` (pipeline) and `scripts/train_models.py` (ML training). Modifying a formula here changes behaviour in both the data pipeline and the model.

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

Computes investment analysis ratios that are **not** used in the fraud score but are displayed in the UI and used as ML features in the 5-factor alpha score.

Metrics: P/E, P/B, EV/EBITDA, FCF Yield, ROE, ROA, Gross Margin, Net Margin, Debt/Equity, Current Ratio, plus Greenblatt (ROIC, Earnings Yield), Carlisle (Acquirer's Multiple), Graham (NCAV), and Novy-Marx (Gross Profitability) extensions.

---

## Fraud Signal Modules

### `fraud_signals.py` — Fraud Score Computation

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

Adds `fraud_confirmed` and `fraud_suspect` binary columns by matching against SEC AAER releases and Stanford SCAC data. See also `scripts/fetch_aaer_labels.py` for the standalone AAER-only updater.

### `enrich_fraud_taxonomy.py` — Fraud Taxonomy Sub-Scores (P0d)

Adds five fraud-type sub-scores (0.0–1.0 each):

| Column | Fraud mechanism |
|---|---|
| `fraud_score_accounting` | Earnings manipulation via accruals / channel-stuffing |
| `fraud_score_dilution` | Equity issuance abuse / dilution fraud |
| `fraud_score_governance` | Governance failures (auditor, board, going concern) |
| `fraud_score_insider` | Insider selling / related-party transactions |
| `fraud_score_macro` | Macro-driven fraud exposure (recession, sector stress) |

### `enrich_governance.py` — Governance Signals

Adds `going_concern` flag (True if SEC filing disclosed going concern doubt) via EDGAR EFTS full-text search. Replaces older `enrich_auditor_going_concern.py`.

### `enrich_insider_signals.py` — Insider Trading Signals

Fetches SEC Form 4 filings for the last 12 months and computes:
- `insider_sale_count`, `insider_buy_count`
- `net_insider_shares` (negative = net selling)
- `insider_selling_flag` (net sold > 10K shares AND sales > buys)

### `enrich_market_signals.py` — Market-Based Signals

Adds liquidity and momentum signals:
- `avg_volume_90d`, `volume_spike_ratio` (30d/90d)
- `price_change_90d`
- `illiquid_flag` (volume < 10K/day)
- `pump_dump_flag` (volume spike > 3× AND price up > 50%)
- `volatility_90d`, `beta`, `bid_ask_spread`

### `enrich_market_cap.py` — Market Cap Gap Fill

Fills missing `market_cap` values for companies where step3 price enrichment didn't produce a market cap. Uses yfinance.

### `enrich_auditor_going_concern.py` — Auditor Quality (Legacy)

Adds `going_concern`, `auditor_name`, `big4_auditor`, `small_auditor_flag`. Superseded by `enrich_governance.py` for going concern signals; auditor name via SEC XBRL is not reliably available.

### `enrich_feature_dictionary.py` — Feature Metadata

Builds a human-readable feature dictionary mapping column names to descriptions, data types, and factor group assignments.

---

## Universe and Confidence Modules

### `p0f_universe_definition.py` — Universe Definition (P0f)

Applies 10 inclusion/exclusion rules to define the investable universe. Outputs a universe-tagged parquet and summary CSV.

Rules:
1. `period_type == 'annual'`
2. `fiscal_year >= 2009` (XBRL coverage)
3. `fiscal_year <= current_year - 1` (completed years only)
4. `revenue >= $1M`
5. `total_assets >= $100K`
6. `entry_price > 0`
7. Exclude SIC 6000–6999 (financials) — different accrual structure
8. Exclude SIC 4900–4999 (utilities) — regulated earnings distort signals
9. Size: include micro/small/mid/large (exclude nano/shell)
10. Exclude OTC stocks with price < $1.00

```bash
python3 pipeline/p0f_universe_definition.py               # apply + report
python3 pipeline/p0f_universe_definition.py --dry-run     # report only
```

Output: `data/universe_clean.parquet`, `data/universe_summary.csv`

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

Merges all market snapshot files into a single `historical_dataset_clean.parquet`. Called automatically by `scripts/run_pipeline.py` step 6. Run directly to re-merge without re-running the full pipeline.

```bash
python3 pipeline/build_historical_dataset.py
```

---

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
10. Align to 319-column schema + concatenate

---

### `fetch_companies.py` — Company Metadata Fetcher

Fetches and caches company metadata (name, SIC code, exchange, CIK) from SEC EDGAR. Called internally by step1 and step2 modules.

### `market_cap_filter.py` — Market Cap Filter Utility

Shared utility used by backtester and screener to apply the minimum market cap floor. Default: $50M.

### `auto_update.py` — Auto-Update Orchestrator

Orchestrates an incremental refresh of the dataset: downloads the existing parquet from HuggingFace, identifies new fiscal years available, runs only the new rows through the pipeline, and re-merges. Used by the GitHub Actions weekly refresh workflow.
