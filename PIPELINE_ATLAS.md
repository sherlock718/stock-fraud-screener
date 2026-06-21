# PIPELINE_ATLAS.md — Session 1 Reference Map

Generated 2026-06-21. Read-only classification. No code changes.

> **Living document.** The File Map, Call Graph, Test Matrix, Parquet Atlas, and Feature
> Coverage Matrix are living snapshots. They reflect the repo at the latest completed
> session. They are not automatically live. Future sessions must update them when they
> change anything these docs describe.

---

## 1. File Map

| File | Lines | Classification | Role |
|------|-------|---------------|------|
| `pipeline/step1_fetch_tickers.py` | 147 | CURRENT_CORE | US ticker list from SEC EDGAR → `tickers.parquet` |
| `pipeline/step1_fetch_tickers_br.py` | 227 | CURRENT_CORE | BR tickers from CVM → `tickers_br.parquet` |
| `pipeline/step1_fetch_tickers_ca.py` | 108 | CURRENT_CORE | CA tickers from TMX → `tickers_ca.parquet` |
| `pipeline/step1_fetch_tickers_eu.py` | 251 | CURRENT_CORE | EU tickers from Wikipedia index scrape → `tickers_eu.parquet` |
| `pipeline/step1_fetch_tickers_jp.py` | 164 | CURRENT_CORE | JP tickers from EDINET (API key) → `tickers_jp.parquet` |
| `pipeline/step1_fetch_tickers_jp_free.py` | 206 | CURRENT_CORE | JP tickers free (Nikkei 225 hardcoded) → `tickers_jp.parquet` |
| `pipeline/step1_fetch_tickers_kr.py` | 156 | CURRENT_CORE | KR tickers from DART → `tickers_kr.parquet` |
| `pipeline/step2_build_snapshots.py` | 497 | CURRENT_CORE | US EDGAR XBRL snapshots → `snapshots.parquet` |
| `pipeline/step2_build_snapshots_br.py` | 359 | CURRENT_CORE | BR CVM financial statements → `snapshots_br.parquet` |
| `pipeline/step2_build_snapshots_ca.py` | 262 | CURRENT_CORE | CA yfinance financials → `snapshots_ca.parquet` |
| `pipeline/step2_build_snapshots_eu.py` | 282 | CURRENT_CORE | EU yfinance financials → `snapshots_eu.parquet` |
| `pipeline/step2_build_snapshots_jp.py` | 417 | CURRENT_CORE | JP EDINET full → `snapshots_jp.parquet` |
| `pipeline/step2_build_snapshots_jp_free.py` | 247 | CURRENT_CORE | JP yfinance free → `snapshots_jp.parquet` |
| `pipeline/step2_build_snapshots_kr.py` | 494 | CURRENT_CORE | KR DART financials → `snapshots_kr.parquet` |
| `pipeline/step3_enrich_prices.py` | 575 | CURRENT_CORE | Price enrichment + forward returns → `prices.parquet` |
| `pipeline/step4_enrich_macro.py` | 277 | CURRENT_CORE | FRED macro at filing date → `macro.parquet` |
| `pipeline/step5_compute_features.py` | 972 | CURRENT_CORE | Pure computation: 170+ features → `historical_dataset.parquet` |
| `pipeline/step6_clean.py` | 140 | CURRENT_CORE | Data quality filter → `historical_dataset_clean.parquet` |
| `pipeline/feature_library.py` | 49 | CURRENT_SUPPORT | Shared feature functions: `add_normalised_ratios`, `add_piotroski_ext` |
| `pipeline/p0f_universe_definition.py` | 257 | CURRENT_SUPPORT | Universe filter → adds `in_universe`, `excl_reason` columns |
| `pipeline/p0g_confidence_score.py` | 255 | CURRENT_SUPPORT | Data confidence → adds `data_confidence` column |
| `pipeline/enrich_fraud_labels.py` | 383 | CURRENT_SUPPORT | Fraud label enrichment → adds cols to `historical_dataset_clean.parquet` |
| `pipeline/enrich_fraud_taxonomy.py` | 415 | CURRENT_SUPPORT | Fraud sub-type scores → adds 5 cols to `historical_dataset_clean.parquet` |
| `pipeline/enrich_feature_dictionary.py` | 240 | OPTIONAL_REPORT_OR_QA | Feature metadata/documentation generator |
| `pipeline/fraud_signals.py` | 422 | LEGACY_ARCHIVE_CANDIDATE | Dict-based fraud signals (JSON pipeline era). Still imported by `build_historical_dataset.py` and `enrich_*` modules |
| `pipeline/value_metrics.py` | 217 | LEGACY_ARCHIVE_CANDIDATE | Dict-based value metrics (JSON pipeline era). Still imported by `build_historical_dataset.py` |
| `pipeline/build_historical_dataset.py` | 667 | LEGACY_ARCHIVE_CANDIDATE | v3 monolithic builder (JSON → parquet). Superseded by step2→step6 pipeline |
| `pipeline/fetch_companies.py` | 224 | LEGACY_ARCHIVE_CANDIDATE | Old step 1 (JSON-based). Superseded by `step1_fetch_tickers.py` |
| `pipeline/market_cap_filter.py` | 84 | LEGACY_ARCHIVE_CANDIDATE | JSON-era market cap filter. Superseded by `p0f_universe_definition.py` |
| `pipeline/auto_update.py` | 141 | LEGACY_ARCHIVE_CANDIDATE | Daily auto-update (JSON era). Imports missing `score_and_report` module |
| `pipeline/enrich_market_cap.py` | 104 | LEGACY_ARCHIVE_CANDIDATE | JSON-era market cap enrichment. References `companies_financials.json` |
| `pipeline/enrich_market_signals.py` | 182 | LEGACY_ARCHIVE_CANDIDATE | JSON-era market signal enrichment. References `companies_financials.json` |
| `pipeline/enrich_insider_signals.py` | 227 | LEGACY_ARCHIVE_CANDIDATE | JSON-era insider enrichment. References `companies_financials.json` |
| `pipeline/enrich_governance.py` | 117 | LEGACY_ARCHIVE_CANDIDATE | JSON-era governance enrichment. References `companies_financials.json` |

### Classification Key

- **CURRENT_CORE** — Actively invoked by `run_pipeline.py`. Part of the step1→step6 parquet chain.
- **CURRENT_SUPPORT** — Called by scripts or used as post-pipeline enrichment on `historical_dataset_clean.parquet`.
- **OPTIONAL_REPORT_OR_QA** — Documentation/metadata utility. Not on critical path.
- **LEGACY_ARCHIVE_CANDIDATE** — Operates on `companies_financials.json` (old JSON pipeline). No longer on the main data path. Some are still imported for sub-functions but logic is duplicated in step5.
- **UNKNOWN_INVESTIGATE** — None found. All files classified.

---

## 2. Call Graph

### Who calls pipeline modules (direction: caller → callee)

```
scripts/run_pipeline.py
  └─ subprocess → pipeline/step1_fetch_tickers.py
  └─ subprocess → pipeline/step2_build_snapshots.py
  └─ subprocess → pipeline/step3_enrich_prices.py
  └─ subprocess → pipeline/step4_enrich_macro.py
  └─ subprocess → pipeline/step5_compute_features.py
  └─ subprocess → pipeline/step6_clean.py

scripts/run_pipeline_{br,ca,eu,jp,kr}.py
  └─ subprocess → pipeline/step1_fetch_tickers_{br,ca,eu,jp_free,kr}.py
  └─ subprocess → pipeline/step2_build_snapshots_{br,ca,eu,jp_free,kr}.py
  └─ subprocess → (shared step3→step6 with --suffix flag)

scripts/train_models.py         → from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
scripts/generate_oof_scores.py  → from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
scripts/backtester.py           → from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
scripts/leverage_strategy.py    → from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
scripts/build_monthly_price_cache.py → from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
scripts/patch_montier_c2.py     → from pipeline.step5_compute_features import add_montier_c_score
```

### Internal pipeline cross-imports

```
pipeline/build_historical_dataset.py
  └─ from pipeline.fraud_signals import beneish_m_score, piotroski_f_score, ...
  └─ from pipeline.value_metrics import calculate_value_metrics

pipeline/fraud_signals.py
  └─ from pipeline.value_metrics import calculate_value_metrics (at __main__)

pipeline/enrich_insider_signals.py
  └─ from pipeline.fraud_signals import calculate_all_signals
  └─ from pipeline.score_and_report import generate_report, print_report  ← BROKEN (module missing)

pipeline/enrich_market_cap.py
  └─ from pipeline.fraud_signals import calculate_all_signals
  └─ from pipeline.score_and_report import generate_report, print_report  ← BROKEN (module missing)

pipeline/enrich_market_signals.py
  └─ from pipeline.fraud_signals import calculate_all_signals
  └─ from pipeline.score_and_report import generate_report, print_report  ← BROKEN (module missing)
```

### External callers (notebooks, tests)

```
tests/test_pipeline.py
  └─ from train_models import get_candidates, train_model, compute_ic_table  (scripts/, not pipeline/ direct)
  └─ from bias_audit import _period_end_date
  └─ from app_v2 import score_companies

notebooks/08_experiment_hub.ipynb
  └─ No direct pipeline imports (reads parquet files only)
```

---

## 3. Test Matrix

| Source File | Role | Critical Risks | Required Tests | Planned Test File | Priority | Existing Coverage | Status |
|---|---|---|---|---|---|---|---|
| `step1_fetch_tickers.py` | Ticker ingestion (US) | Duplicates, missing CIK, inactive tickers included, suffix errors | Schema validation, dedup, inactive handling | `tests/test_step1.py` | P1 | 14 tests (schema, dedup, survivorship, identifiers) | ✅ covered |
| `step1_fetch_tickers_{br,ca,eu,jp,kr}.py` | Multi-market tickers | Encoding, exchange mapping, missing identifiers | Schema + dedup per market | `tests/test_step1_markets.py` | P2 | None | missing |
| `step2_build_snapshots.py` | EDGAR XBRL parse | Wrong units, duplicate rows, missing filed_date, quarterly leak | One-row-per-ticker-year, filed_date≥period_end | `tests/test_step2.py` | P0 | 16 tests (schema, dedup, temporal, YoY, coverage) | ✅ covered |
| `step2_build_snapshots_{br,ca,eu,jp,kr}.py` | Multi-market snapshots | Schema drift, currency confusion, fiscal year boundaries | Schema match, currencies col, row counts | `tests/test_step2_markets.py` | P2 | None | missing |
| `step3_enrich_prices.py` | Price + forward returns | Look-ahead bias (entry price before filed_date), survivorship | entry_date > filed_date, forward returns future-only, delisted handling | `tests/test_step3.py` | P0 | 30 tests (temporal + 1 xfail for PRICE-UNADJUSTED-001) | ⚠️ covered, 1 known bug |
| `step4_enrich_macro.py` | FRED macro merge | Recession look-ahead, macro_asof_date after filing | macro_asof_date ≤ filed_date, no NaN spike | `tests/test_step4.py` | P1 | None | missing |
| `step5_compute_features.py` | Feature computation | Temporal leakage, label leakage, rank leakage, formula errors | No forward columns used, rank within fiscal_year, coverage checks | `tests/test_step5.py` | P0 | None | missing |
| `step6_clean.py` | Data quality filter | Silent row drops, missing required cols, infinity propagation | Row count delta, no inf, required cols present | `tests/test_step6.py` | P1 | None | missing |
| `feature_library.py` | Shared ratios/Piotroski | Formula errors, NaN propagation | Unit tests on known inputs | `tests/test_feature_library.py` | P1 | None | missing |
| `p0f_universe_definition.py` | Universe filter | Wrong exclusion logic, market-specific rules | Filter correctness per market, row preservation | `tests/test_p0f.py` | P2 | None | missing |
| `p0g_confidence_score.py` | Data confidence | Score range [0,1], NaN handling | Bounds check, coverage calc | `tests/test_p0g.py` | P2 | None | missing |
| `enrich_fraud_labels.py` | Fraud label merge | Label misalignment, date mismatch | Label matches known AAER set | `tests/test_fraud_labels.py` | P1 | None | missing |
| `fraud_signals.py` | Legacy fraud calcs | Formula correctness | Unit tests per signal function | `tests/test_fraud_signals.py` | P3 | None | missing (legacy) |
| `train_models.py` (scripts/) | ML training | Leakage in features, temporal split | Temporal no-overlap, EXCLUDE set complete | `tests/test_pipeline.py` | P0 | Partial | partially covered |
| `bias_audit.py` (scripts/) | Look-ahead audit | Missed leakage paths | Period end calc, filing lag | `tests/test_pipeline.py` | P1 | Partial | partially covered |
| `app_v2.py` (root) | Streamlit scoring | Wrong median fill, missing model | Predict with NaN, missing horizon | `tests/test_pipeline.py` | P2 | Partial | partially covered |

### Coverage Summary
- **tests/test_pipeline.py** covers: temporal split logic, feature exclusion, train medians, IC table, filing lag audit, app_v2 scoring. All synthetic data (no disk/network).
- **tests/test_step1.py** covers: schema contract (required columns, CIK format, ticker format), dedup logic, survivorship (OTC retained), identifier checks. 14 tests.
- **tests/test_step2.py** covers: schema contract, primary key uniqueness, temporal integrity (filed_date > period_end), YoY computation functions, coverage gating (revenue+assets required). 16 tests.
- **tests/test_step3.py** covers: price lookup (on_or_after), forward return (future-only), prior momentum (past-only), volatility (past-only), 52w high, benchmark selection, enrich_row temporal contracts, survivorship (None on missing). 29 pass + 1 xfail (PRICE-UNADJUSTED-001: fetch uses unadjusted Close).
- **Zero coverage** for: step4 (macro), step5 (features), step6 (clean), feature_library, universe/confidence, fraud labels, multi-market logic.
- **Duplicate risk**: `fraud_signals.py` logic is duplicated in `step5_compute_features.py` section D. Testing either covers the other's math.

---

## 4. Step Audit Checklist

### Step 1 — Fetch Tickers

| # | Check | What to verify |
|---|---|---|
| 1.1 | yfinance tickers | Ticker symbols are valid yfinance identifiers (with correct suffix per market) |
| 1.2 | Market codes | Each row has a `market` column matching {US, BR, CA, EU, JP, KR} |
| 1.3 | Exchanges | Exchange field populated where available (NYSE, NASDAQ, TSX, B3, etc.) |
| 1.4 | Inactive/delisted | Delisted companies ARE included (survivorship). Active status flagged but not filtered |
| 1.5 | Duplicates | No duplicate (ticker, market) pairs in output parquet |
| 1.6 | Identifiers | CIK (US), CNPJ (BR), DART code (KR), EDINET code (JP) — one primary ID per market |
| 1.7 | US suffix | US tickers have NO `.US` suffix (bare symbol). Non-US have exchange suffix (`.T`, `.KS`, `.SA`, etc.) |
| 1.8 | Survivorship bias | Must NOT filter out delisted/inactive companies at this stage |

### Step 2 — Build Snapshots

| # | Check | What to verify |
|---|---|---|
| 2.1 | One row per ticker×fiscal_year×period_type | No duplicates on this composite key |
| 2.2 | period_end | Derived from fiscal_year+quarter. Not a future date relative to filed_date |
| 2.3 | filed_date | Actual SEC/regulator filing timestamp. Not estimated |
| 2.4 | filed_date ≥ period_end | Filing always AFTER the period it covers (no time travel) |
| 2.5 | Currencies | `currency` column present. All monetary values in same currency per row |
| 2.6 | Coverage | Core fields (revenue, total_assets, net_income) have >80% fill rate |
| 2.7 | Units | All values in actual units (not thousands/millions). Normalized at ingestion |
| 2.8 | Annual/quarterly | `period_type` column distinguishes. Both kept at this stage |
| 2.9 | Non-US schema | Same column names across all market snapshots (unified schema) |

### Step 3 — Enrich Prices

| # | Check | What to verify |
|---|---|---|
| 3.1 | Entry price after filed_date | `entry_date` is the first trading day ON OR AFTER `filed_date` |
| 3.2 | Prior momentum past-only | `momentum_3m_prior`, `momentum_12m_prior` use prices BEFORE entry_date only |
| 3.3 | Forward returns future-only | `forward_return_{6m,1y,2y,3y,5y}` use prices AFTER entry_date only |
| 3.4 | Adjusted/unadjusted | All prices are split-adjusted (yfinance `Adj Close`) |
| 3.5 | Missing prices survivorship | If no exit price found → flag as delisted, impute -50% (not drop row) |
| 3.6 | Local benchmarks | `beat_local_market_{1y,3y,5y}` uses country-specific index, not just S&P 500 |
| 3.7 | FX returns | For non-US: returns in LOCAL currency (not USD-converted) at this stage |
| 3.8 | US-only backtest check | Confirm US rows still have USD-denominated returns (no accidental FX) |

### Step 4 — Enrich Macro

| # | Check | What to verify |
|---|---|---|
| 4.1 | Schema | Standard macro columns: `gdp_growth`, `unemployment`, `fed_funds_rate`, `cpi_yoy`, `spread_10y2y`, etc. |
| 4.2 | Row count | Output row count == input row count (macro is a left-join enrichment) |
| 4.3 | No future macro | Macro values are from BEFORE or AT `filed_date`, never after |
| 4.4 | macro_asof_date | Column recording the as-of date for each macro value. Must be ≤ filed_date |
| 4.5 | Derived formulas | `recession_flag`, `tightening_flag` derived from base macro, not hardcoded |
| 4.6 | Fill rates | Macro cols should have >95% fill (FRED has good coverage post-2000) |
| 4.7 | Same-date consistency | All macro cols for a row use the same as-of-date snapshot |
| 4.8 | Recession look-ahead | NBER recession dates are announcement-lagged. Use real-time proxy, not revised |
| 4.9 | US vs local macro | US macro for US rows. Local rates/GDP for non-US (where available) |

### FX Audit

| # | Check | What to verify |
|---|---|---|
| FX.1 | Local vs USD returns | Forward returns stored in local currency. USD conversion only if explicitly needed |
| FX.2 | Entry/exit FX dates | If USD returns computed: FX rate at entry_date for entry, exit_date for exit |
| FX.3 | Historical FX not current | Never use today's FX rate for historical return calculation |
| FX.4 | Missing 6m/2y FX | If FX data gaps: NaN the USD return, don't use stale rate |
| FX.5 | GitHub refresh FX | CI pipeline uses historical FX data source, not live API only |
| FX.6 | Train/backtest mix | Model trains on local returns. Backtest can report USD-equivalent separately |

### Step 5 — Compute Features

| # | Check | What to verify |
|---|---|---|
| 5.1 | Formula correctness | Spot-check 10 features against textbook definitions |
| 5.2 | Temporal leakage | No feature uses `forward_return_*`, `beat_local_market_*`, or any future column |
| 5.3 | Label leakage | No feature uses `fraud_confirmed`, `ml_*` scores, or alpha scores |
| 5.4 | Rank leakage | Cross-sectional ranks grouped by `fiscal_year` (not full dataset) |
| 5.5 | Rolling past-only | Any rolling window (momentum) uses only past data relative to the row's date |
| 5.6 | Coverage | Features have >50% non-null for the rows they apply to |
| 5.7 | Value sanity | Ratios bounded (PE not 999999, debt_to_equity not 1e10) — winsorize applied |
| 5.8 | Cross-market | Feature formulas work for non-US (no hardcoded US assumptions) |
| 5.9 | Dataset shape | Output has expected row count and column count (currently 58K × 361) |
| 5.10 | Step 5 sole authority | All computed features originate HERE. No other script creates features that bypass step5 |

### Step 6 — Clean

| # | Check | What to verify |
|---|---|---|
| 6.1 | Non-null required cols | Rows dropped only if missing REQUIRED columns (defined in code) |
| 6.2 | No duplicates | No duplicate (ticker, fiscal_year, period_type) in output |
| 6.3 | No infinities | All numeric columns finite (inf/−inf replaced with NaN or dropped) |
| 6.4 | as_of_date == filed_date | If `as_of_date` exists, it equals `filed_date` (PIT anchor) |
| 6.5 | filing_lag_days | `filing_lag_days` = filed_date − period_end. Always ≥ 0 |
| 6.6 | No accidental filtering | Step6 does NOT filter by market cap, revenue, or sector (that's p0f's job) |
| 6.7 | Row count stable | Output within 5% of input (no catastrophic drops) |

### Step 7 — Universe Definition (p0f)

| # | Check | What to verify |
|---|---|---|
| 7.1 | Quarterly excluded | Only `period_type == 'annual'` rows get `in_universe = 1` |
| 7.2 | Fiscal year cutoff | `fiscal_year >= 2009` (XBRL coverage) and `<= current_year - 1` |
| 7.3 | Incomplete year excluded | Most recent fiscal year excluded if < 80% of companies have filed |
| 7.4 | Default vs --apply-filters | Without `--apply-filters`: structural rules only. With: full investable universe |
| 7.5 | Revenue/assets/price filters | Only applied under `--apply-filters`: revenue≥$1M, assets≥$100K, price>0 |
| 7.6 | OTC floor | US OTC penny stocks excluded only if exchange=='OTC' AND price < $1.00 |
| 7.7 | SIC exclusions | Financials (6000–6999) and utilities (4900–4999) excluded under `--apply-filters` |
| 7.8 | Missing data behavior | Rows with NULL revenue/assets → NOT excluded by default (fail-open) |
| 7.9 | in_universe meaning | 1 = passes all applied filters. 0 = excluded. Column is additive (can re-run) |
| 7.10 | Currency | Filters use local-currency values (not USD-converted) |
| 7.11 | Downstream respect | `train_models.py` and `backtester.py` filter on `in_universe == 1` |

### Liquidity Audit

| # | Check | What to verify |
|---|---|---|
| LIQ.1 | Where liquidity data exists | `enrich_market_signals.py` (legacy JSON), `monthly_prices.parquet` (backtester) |
| LIQ.2 | ADTV PIT safety | Average daily volume must use data up to filed_date only |
| LIQ.3 | Train/backtest/portfolio access | Liquidity features available at decision time (not just backtest exit) |
| LIQ.4 | Volume filter missing | Step5 has no ADTV feature. Backtester has `--no-adtv` flag as workaround |
| LIQ.5 | Bid-ask missing | No bid-ask spread in parquet pipeline. Only in legacy `enrich_market_signals.py` |
| LIQ.6 | enrich_market_signals connected? | NO — operates on `companies_financials.json`, not parquet pipeline |
| LIQ.7 | ADTV feature vs backtest-only | ADTV currently backtest-only (not a trainable feature in step5) |
| LIQ.8 | Market cap inconsistency | `market_cap_at_filing` exists in step2 but no real-time liquidity proxy |

---

## 5. Free Public Feature Coverage Matrix

| # | Feature Family | Exists? | File/Source | Current Layer | Correct Layer | PIT Timestamp? | Free Public? | Pipeline Own? | Priority | Action |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | Filing fundamentals (revenue, assets, equity, OCF, NI) | YES | `step2_build_snapshots.py` | raw_ingestion | raw_ingestion | YES (filed_date) | YES (EDGAR) | YES | — | keep |
| 2 | Value factors (PE, PB, PS, EV/EBITDA, FCF yield, book/market) | YES | `step5_compute_features.py` § A | base_pipeline_feature | base_pipeline_feature | YES | YES (derived) | YES | — | keep |
| 3 | Quality/profitability (ROA, ROE, ROIC, margins, Piotroski) | YES | `step5_compute_features.py` § B + `feature_library.py` | base_pipeline_feature | base_pipeline_feature | YES | YES (derived) | YES | — | keep |
| 4 | Growth (17 YoY + 6 3yr trends) | YES | `step5_compute_features.py` via step2 YoY cols | base_pipeline_feature | base_pipeline_feature | YES | YES (derived) | YES | — | keep |
| 5 | Momentum/price (3m, 6m, 12m prior, 52w high, vol) | YES | `step3_enrich_prices.py` + `step5` § H | base_pipeline_feature | base_pipeline_feature | YES | YES (yfinance) | YES | — | keep |
| 6 | Liquidity/execution (ADTV, bid-ask, illiquid flag) | PARTIAL | `enrich_market_signals.py` (legacy JSON only) | legacy | base_pipeline_feature | NO (not PIT) | YES (yfinance) | YES (should migrate) | P1 | audit |
| 7 | Fraud/manipulation (Beneish, Altman, Ohlson, Montier, accruals) | YES | `step5_compute_features.py` § C+D | base_pipeline_feature | base_pipeline_feature | YES | YES (derived) | YES | — | keep |
| 8 | Insider/governance (Form 4 trades, going concern) | PARTIAL | `enrich_insider_signals.py` + `enrich_governance.py` (legacy JSON) | legacy | base_pipeline_feature | PARTIAL | YES (SEC EDGAR) | YES (should migrate) | P2 | audit |
| 9 | Institutional ownership (13F filings) | NO | — | missing | factor_alpha_layer | N/A | YES (SEC 13F) | NO (alpha layer) | P3 | park |
| 10 | Short interest (exchange reports) | NO | — | missing | factor_alpha_layer | N/A | PARTIAL (delayed) | NO (alpha layer) | P3 | park |
| 11 | Corporate actions (splits, M&A, buybacks) | NO | — | missing | base_pipeline_feature | N/A | YES (SEC 8-K) | YES | P3 | add later |
| 12 | Balance sheet/distress (Altman Z, Ohlson O, leverage) | YES | `step5_compute_features.py` § D+E | base_pipeline_feature | base_pipeline_feature | YES | YES (derived) | YES | — | keep |
| 13 | Macro/rates/FX (FRED, yields, spread, CPI) | YES | `step4_enrich_macro.py` | base_pipeline_feature | base_pipeline_feature | YES (macro_asof) | YES (FRED) | YES | — | keep |
| 14 | Filing text (10-K sentiment, readability, MD&A) | NO | — | missing | base_pipeline_feature | N/A | YES (EDGAR full text) | YES | P2 | add later |
| 15 | Analyst data (EPS estimates, revisions, coverage) | NO | — | missing | missing | N/A | NO (paid: IBES, FactSet) | NO | — | park |
| 16 | Explicitly excluded (features ruled out) | N/A | — | N/A | N/A | N/A | N/A | N/A | — | — |

### Explicitly Excluded (Family 16)

| Feature | Reason excluded |
|---|---|
| Analyst EPS estimates | Not free. Requires IBES/FactSet/Bloomberg subscription |
| Real-time order book | Not free, not PIT, not annual-frequency |
| Social media sentiment | Not PIT-safe at annual frequency. Noisy |
| Options implied volatility | Requires paid data (CBOE). Not free |
| Credit ratings (Moody's/S&P) | Proprietary. Not free |
| ESG scores | Proprietary (MSCI, Sustainalytics). Not free |

---

### High-Priority Missing Free Features

| Feature | Source | PIT-Safe? | Effort | Notes |
|---|---|---|---|---|
| ADTV (avg daily trading volume) | yfinance | YES (use pre-filing window) | Low | Migrate from legacy `enrich_market_signals.py` logic |
| Volume spike ratio | yfinance | YES | Low | Same migration |
| Volatility (realized, 90d) | yfinance | YES | Low | Same migration |
| Filing text features (word count, readability) | EDGAR full text | YES | Medium | 10-K already fetched; parse MD&A section |

### Features Present Only in Side Scripts

| Feature | Script | Correct Home | Action |
|---|---|---|---|
| ADTV, volume spike, pump_dump_flag | `pipeline/enrich_market_signals.py` | step3 or step5 | Migrate to parquet pipeline |
| Insider buy/sell counts, net shares | `pipeline/enrich_insider_signals.py` | step5 or post-step6 enrichment | Migrate |
| Going concern flag | `pipeline/enrich_governance.py` | step5 | Migrate |
| Sector, dividend yield | `scripts/enrich_sectors_dividends.py` | step2 or step5 | Migrate |

### Legacy Features Worth Preserving Before Archive

| Feature | Legacy File | Equivalent in Current Pipeline? |
|---|---|---|
| `beneish_m_score` (dict-based) | `pipeline/fraud_signals.py` | YES — duplicated in `step5` § D. Safe to archive |
| `piotroski_f_score` (dict-based) | `pipeline/fraud_signals.py` | YES — duplicated in `step5` § D + `feature_library.py`. Safe to archive |
| `altman_z_score` (dict-based) | `pipeline/fraud_signals.py` | YES — duplicated in `step5` § D. Safe to archive |
| `calculate_value_metrics` (dict) | `pipeline/value_metrics.py` | YES — duplicated in `step5` § A+B. Safe to archive |
| `insider_selling_flag` | `pipeline/enrich_insider_signals.py` | NO — not in step5. Preserve logic before archive |
| `pump_dump_flag` | `pipeline/enrich_market_signals.py` | NO — not in step5. Preserve logic before archive |
| `going_concern_flag` | `pipeline/enrich_governance.py` | NO — not in step5. Preserve logic before archive |

### Features to Park (Not Free / Not Direct / Not PIT-Safe)

| Feature | Reason to Park |
|---|---|
| Institutional ownership (13F) | Free but quarterly-lagged 45 days. Complex to make PIT-safe at annual granularity |
| Short interest | Free only via FINRA with 2-week delay. Not PIT-safe without careful date alignment |
| Analyst estimates | Not free (IBES/FactSet). No public source with history |
| Options data | Not free (CBOE subscription). Not available for non-US |
| ESG/sustainability | Not free (MSCI/Sustainalytics proprietary) |
| Credit default swaps | Not free. Not relevant for small caps |

---

*End of PIPELINE_ATLAS.md*
