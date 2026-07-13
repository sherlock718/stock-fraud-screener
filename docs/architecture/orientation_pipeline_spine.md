# Pipeline Spine Orientation (Session 27, updated Session 49)

Generated: 2026-06-26 | Updated: 2026-07-14 | Scope: `pipeline/` folder + `_root.py`

---

## 1. End-to-End Pipeline Map

```
SEC EDGAR APIs ──→ step1_fetch_tickers.py ──→ data/tickers.parquet
                                                    │
SEC EDGAR XBRL ──→ step2_build_snapshots.py ──→ data/snapshots.parquet
(company-facts)         (+ YoY features)            │
                                                    │
yfinance ────────→ step3_enrich_prices.py ──→ data/prices.parquet
(adj close)        (+ forward returns,              │
                    momentum, volatility)            │
                                                    │
FRED API ────────→ step4_enrich_macro.py ───→ data/macro.parquet
(9 series)         (+ derived macro features)       │
                                                    │
                                                    ▼
              ┌─── step5_compute_features.py ──→ data/historical_dataset.parquet
              │    (merges snap+prices+macro,
              │     170+ computed features,
              │     zero API calls)
              │
              ├──→ step6_clean.py ──────────→ data/historical_dataset_clean.parquet
              │    (structural clean, quality      ← CORE PIPELINE ENDS HERE
              │     fixes, imputation, survivorship,
              │     confidence score)
              │
              └──→ step7_fraud_taxonomy.py ─→ adds fraud_score_* columns (in-place)
                   step7b_fraud_labels.py ──→ adds fraud_confirmed/suspect (in-place)
                   (post-pipeline enrichments,    ← ENRICHMENT LAYER
                    run via workflows/run_dataset_enrichments.py)
```

### Multi-Market Variants

Each market (US, KR, BR, CA, EU, JP) has its own `step1_*` and `step2_*` scripts.
Steps 3-6 are shared — they accept `--suffix _kr` flags to process market-specific parquets.
Output naming: `data/historical_dataset_clean_kr.parquet`, etc.

| Market | step1 source | step2 source | Benchmark(s) |
|--------|---|---|---|
| US | SEC EDGAR | SEC XBRL company-facts | SPY, MDY, IWM, IWC (size-matched) |
| KR | DART (Korean FSS) | DART financials | ^KS11 (KOSPI), ^KQ11 (KOSDAQ) |
| BR | CVM (Brazilian SEC) | CVM filings | ^BVSP (Ibovespa) |
| CA | CSA / SEDAR | SEDAR financials | ^GSPTSE (TSX) |
| EU | national registries | per-country | DAX, FTSE, CAC, etc. |
| JP | FSA EDINET | EDINET XBRL | ^N225 (Nikkei) |

---

## 2. Column Lineage

### step1 → `data/tickers.parquet`
| Column | Source |
|--------|--------|
| cik | SEC company_tickers.json |
| ticker, name | SEC company_tickers.json |
| exchange | SEC company_tickers_exchange.json (fallback: 'OTC') |
| sic_code, sic_description | SEC submissions API per CIK |
| market, country, accounting_std | Hardcoded per market variant ('US', 'United States', 'GAAP') |

### step2 → `data/snapshots.parquet`
| Column Group | Origin | Count |
|---|---|---|
| Identifiers | Inherited from tickers.parquet | 9 |
| Period keys | fiscal_year, fiscal_quarter, period_type, filed_date | 4 |
| Income statement | EDGAR XBRL (revenue, net_income, gross_profit, etc.) | ~15 |
| Balance sheet | EDGAR XBRL (total_assets, equity, debt, etc.) | ~15 |
| Cash flow | EDGAR XBRL (operating_cash_flow, capex, depreciation) | ~5 |
| YoY growth | Computed in step2: `_yoy()` helper | 18 |
| Margin changes | Computed in step2: `_delta()` on ratios | 6 |
| 3-year trends | Computed in step2 (roa_trend_3y, etc.) | 6 |

**Key design**: step2 computes YoY features with `_yoy` suffix (e.g., `revenue_growth_yoy`). 
Step 5 later aliases these to shorter names (e.g., `revenue_growth`). Both survive in the final dataset.

### step3 → `data/prices.parquet`
| Column Group | Origin | Count |
|---|---|---|
| Keys | cik, ticker, filed_date, fiscal_year, etc. | 6 |
| Entry price | yfinance adj close on/after filed_date | 1 |
| Market cap | entry_price × shares_outstanding | 1 |
| Forward returns | 11 horizons (6m through 15y) | 11 |
| Benchmark returns | Size-matched ETF returns (same 11 horizons) | 11 |
| Beat local market | Binary: forward_return > benchmark | 11 |
| Excess return | forward_return - benchmark_return | 11 |
| Momentum | 3m/6m/12m prior return (skip 21d) | 3 |
| Volatility | 6m/12m/36m/60m prior annualised vol | 4 |
| Price to 52w high | entry_price / 52-week high | 1 |

**Labels/targets**: `forward_return_1y`, `forward_return_3y`, `forward_return_5y` are the ML targets.
`beat_local_market_{h}` is the binary classification target.

### step4 → `data/macro.parquet`
| Column | FRED series | Role |
|--------|-------------|------|
| treasury_10y | DGS10 | Long-rate environment |
| treasury_2y | DGS2 | Short-rate environment |
| yield_curve | T10Y2Y | Recession predictor |
| fed_funds_rate | FEDFUNDS | Policy rate |
| credit_spread_baa | BAA10Y | Credit stress |
| hy_spread | BAMLH0A0HYM2 | High-yield stress |
| cpi_yoy | CPIAUCSL (12m pct_change) | Inflation |
| recession | USREC | NBER binary |
| vix | VIXCLS | Implied vol |
| real_rate_10y | treasury_10y - cpi_yoy | Derived |
| credit_tightening | 6m Δ in credit_spread_baa | Derived |
| macro_regime | 0/1/2/3 (low/rising/high/recession) | Derived |

**Lookup mechanism**: daily panel built at startup, `merge_asof(direction='backward')` per filing date.

### step5 → `data/historical_dataset.parquet`

Merges snap + prices + macro, then computes ~170 features in groups:

| Group | Function | Example columns | Count |
|---|---|---|---|
| A. Valuation | `add_valuation()` | pe_ratio, ev_ebitda, fcf_yield | 13 |
| B. Profitability | `add_profitability()` | roa, roe, roic, gross_margin | 15 |
| C. Accruals | `add_accruals()` | sloan_accruals, noa, delta_dso | 12 |
| D. Fraud scores | `add_fraud_scores()` | beneish_m_score, altman_z_score, ohlson | ~25 |
| D2. Montier | `add_montier_c_score()` | montier_c1–c6, montier_c_score | 7 |
| E. Liquidity | `add_liquidity()` | current_ratio, debt_to_equity, piotroski | ~20 |
| F. Composites | `add_composite_scores()` | piotroski_f_score, quality_composite, value_composite | 3 |
| G. Size | `add_size_features()` | log_market_cap, size_category | 5 |
| H. Momentum ranks | `add_momentum_ranks()` | momentum_12m_rank, vol_rank_12m | 5 |
| I. Interactions | `add_interactions()` | value_x_quality, small_x_quality | 7 |
| I2. Sector pctl | `add_sector_percentiles()` | *_sector_pct (18 features) | 18 |
| J. Macro interact | `add_macro_interactions()` | value_in_recession, quality_in_recession | 5 |
| Stability | inline | roe_volatility_5yr, earnings_stability_5yr | 4 |

**Column aliasing**: step5 creates `COLUMN_ALIASES` dict — maps step2 `_yoy` names to shorter names. Both versions remain.

**Winsorization**: all growth columns + key ratios clipped at 1st/99th percentile. This is the ONLY place winsorization happens for growth features (plus step6 for accruals specifically).

### step6 → `data/historical_dataset_clean.parquet` (FINAL)

| Phase | What it does | Columns added |
|---|---|---|
| 1. Structural | Drop nulls in required cols, dedup, inf→NaN | as_of_date, filing_lag_days |
| 2. Quality | Drop dead columns, fix gross_margin >1.5, winsorize accruals | is_forecast |
| 3. Imputation | Quarterly features join, size_category from log_assets | 5 quarterly features, size_category_imputed |
| 4. Survivorship | Flag likely-delisted (no filing in 3+ years), impute -50% returns | likely_delisted |
| 5. Confidence | coverage × consistency × timeliness composite | data_confidence |

### Key Consumers of Final Dataset

| Consumer | File | What it reads |
|---|---|---|
| ML training | `modeling/train.py` | `data/historical_dataset_clean.parquet` — annual rows, 27 features |
| Factor research | `research/factor_research.py` | Same — all numeric columns as IC candidates |
| Backtest | `backtest/engine.py` | Same — filters by fiscal_year range |
| Alpha scoring | `alpha/factors/*.py` | Same — computes factor percentiles |
| Screener | `portfolio/build_screener_registry.py` | Same — latest year, top alpha picks |

---

## 3. Risk Register

### Lookahead / Contamination Risks

| Risk | Severity | Location | Detail |
|---|---|---|---|
| YoY growth from near-zero base | Medium | step2 lines 309-326 | `_yoy(1, 0.001)` = 99,900%. Mitigated by winsorization in step5 but could still dominate tree splits before winsorization |
| `current_assets_growth` aliased from `asset_growth_yoy` | Low | step5 line 812 | Semantically wrong (total vs current) but only used as proxy in Beneish AQI where it's approximate anyway |
| Sector percentiles grouped by fiscal_year | OK | step5 line 689 | Correctly includes fiscal_year to prevent temporal leakage |
| Survivorship imputation at -50% | Medium | step6 line 297 | Hardcoded. May overstate (some delistings are acquisitions at premium). No distinction between forced/voluntary delist |
| `filed_date` used as entry date | Low | step3 line 355 | Correct PIT approach but ignores ~5 day EDGAR processing lag. In practice, negligible at annual rebalance frequency |

### Fragile Assumptions

| Assumption | Location | Risk if broken |
|---|---|---|
| EDGAR returns HTTP 200 for all valid CIKs | step1, step2 | Silent data loss — checkpoint resumes skip failures |
| yfinance rate limit at 1.5 req/s is sufficient | step3 | IP ban → empty price cache → NaN forward returns |
| FRED API key present in .env | step4 | All macro columns are NaN (graceful but model trains without macro) |
| `total_assets > 0` and `revenue > 0` for feature validity | step5 | sdiv returns NaN, propagates to composites |
| Fiscal year == calendar year for Q4 detection | step2 line 229 | Companies with non-Dec fiscal year-end have Q4 incorrectly classified |

### Duplicated Logic

| Duplication | Files | Severity |
|---|---|---|
| ~~`add_normalised_ratios()`~~ | ~~`pipeline/feature_library.py` AND `research/factor_research.py`~~ | **RESOLVED (Session 49)** — `research/factor_research.py` now imports from `pipeline/feature_library.py` |
| Winsorization | step5 (line 890) AND step6 `winsorize_accruals()` | Low — step6 is market×year specific, step5 is global. Both needed but confusing |
| Size category logic | step5 `add_size_features()` AND step6 `_impute_size_category()` | Low — step6 fills gaps that step5 couldn't (missing market_cap) |

### Naming Inconsistencies

| Issue | Detail |
|---|---|
| `_yoy` suffix vs no suffix | step2 outputs `revenue_growth_yoy`; step5 aliases to `revenue_growth`. Both exist in final dataset |
| `equity` vs `total_equity` | step2 outputs `equity` from XBRL; step5 aliases to `total_equity`. Both exist |
| `shares_outstanding` vs `common_shares_outstanding` | Same — aliased in step5, both survive |
| `receivables` vs `accounts_receivable` | Same pattern |
| Feature names: `_growth` vs `_growth_yoy` | Inconsistent between step2's naming and step5's alias targets |

> **Updated Session 35**: `EXCLUDE_COLS` / `EXCLUDE_PATTERNS` (previously duplicated across
> `backtest/engine.py`, `research/factor_research.py`, and `modeling/train.py`) are now
> consolidated in `modeling/constants.py` — single source of truth. All consumers import from there.
> The canonical feature set is **27 temporally stable features** (`models/feature_sets_pruned.json`).

### Dead / Unused Code

| File | Status | Notes |
|---|---|---|
| `pipeline/build_monthly_price_cache.py` | Semi-active | Used by backtest engine, not by main pipeline |
| `pipeline/enrich_feature_dictionary.py` | Utility | Generates reports only, not in data path |
| `pipeline/p0f_universe_definition.py` | Active | Universe filter for research, not in pipeline run |
| Market variants (step1_*_br, _ca, _eu, _jp) | Active | Multi-market support, functional but less tested than US |
| ~~`pipeline/step1_fetch_tickers_jp_free.py`~~ | **Archived** | Moved to `pipeline/archive/` (session 48). Workflow reference fixed (session 49) |

---

## 4. Refactor Candidates

### Completed (Session 49)
- ~~`pipeline/step1_fetch_tickers_jp_free.py`~~ — **Archived** (session 48), workflow reference fixed
- ~~Column aliasing in step5~~ → **Extracted** to `pipeline/column_aliases.py`. step5 now calls `apply_column_aliases(df)`.
- ~~`add_normalised_ratios` duplication~~ → **Consolidated**. `research/factor_research.py` imports from `pipeline/feature_library.py`.
- ~~`enrich_fraud_taxonomy.py` rename~~ → **Done**: `pipeline/step7_fraud_taxonomy.py`
- ~~`enrich_fraud_labels.py` rename~~ → **Done**: `pipeline/step7b_fraud_labels.py`

### Should Stay As-Is
- `step1_fetch_tickers.py` — clear, single-purpose, well-documented
- `step2_build_snapshots.py` — complex but necessarily so (XBRL extraction)
- `step3_enrich_prices.py` — robust caching + rate limiting design
- `step4_enrich_macro.py` — clean vectorised implementation
- `step6_clean.py` — modular 5-phase pipeline, well-structured

### Could Be Renamed (low priority, not yet done)
- `step2_build_snapshots.py` → `step2_fetch_financials.py` (it fetches from EDGAR, not just "builds")

### Should NOT Be Touched
- The multi-market variant files (`_kr`, `_br`, `_ca`, `_eu`, `_jp`) — functional, less tested. Risk of breakage > value of cleanup.
- Winsorization in step5 vs step6 — they serve different purposes despite looking duplicated.

---

## 5. Architecture Observations

### Dependency Direction
```
step1 → step2 → step3 → step4 → step5 → step6 → step7/7b (enrichment layer)
  ↓        ↓        ↓        ↓        ↓        ↓            ↓
tickers  snapshots  prices   macro   dataset   clean_dataset  (in-place additions)
                                                     ↓
                                              modeling/train.py
                                              research/*.py
                                              backtest/engine.py
                                              alpha/factors/*.py
```

Steps 1-6 are **strictly linear** — no cycles, no backward references. Each step reads only from its predecessor's output.

Step 7/7b is an **enrichment layer** — it modifies `historical_dataset_clean.parquet` in-place (adds columns). It does NOT produce a new file. It runs via `workflows/run_dataset_enrichments.py` as a post-pipeline phase.

### Single Shared Entrypoint
- `_root.py` defines `ROOT = Path(__file__).resolve().parent`
- All scripts use `BASE = Path(__file__).parent.parent` or import from `_root`
- Inconsistency: step1–step5 use local `BASE = Path(...)`, only some enrichment scripts use `_root.ROOT`

### Data Volume
- ~13K US companies × ~15 years × annual = ~195K potential rows
- After dedup + quality filters: ~90-120K rows in final dataset
- ~250 columns in `historical_dataset_clean.parquet`

---

## 6. Summary

The pipeline spine is **well-architected**: linear dependency chain, checkpointed for resilience, zero-API-call feature computation, proper PIT (point-in-time) handling via filed_date, and survivorship correction. The main complexity lives in step5 (170+ features from financial theory) which is unavoidable given the domain.

**Session 49 updates**: Column alias duplication resolved (extracted to `pipeline/column_aliases.py`), `add_normalised_ratios` consolidated to single source in `pipeline/feature_library.py`, fraud enrichment scripts numbered as step7/7b, JP workflow fixed.

Remaining risks: hardcoded -50% survivorship return assumption (214 rows affected, low impact) and 3,900 rows with revenue ≤ 0 propagating NaN through valuation ratios (intentional — kept for model robustness).
