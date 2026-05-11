# Markets & Data Coverage

## Supported Markets

| Market | Code | Source | Accounting Std | Status |
|---|---|---|---|---|
| United States | US | SEC EDGAR / SimFin | US GAAP | ✅ Production (4,452 companies) |
| South Korea | KR | DART API | K-IFRS | ✅ Production (106 companies) |
| Canada | CA | SimFin API | IFRS / CA GAAP | ✅ Production (1,378 companies) |
| Japan | JP | TDNET / SimFin | J-GAAP | ✅ Production (122 companies) |
| Brazil | BR | B3 / SimFin | IFRS / BR GAAP | ✅ Production (55 companies) |
| Germany | DE | Deutsche Börse / SimFin | IFRS | ✅ Production (76 companies) |
| France | FR | Euronext / SimFin | IFRS | ✅ Production (40 companies) |
| Italy | IT | Borsa Italiana / SimFin | IFRS | ✅ Production (40 companies) |
| Spain | ES | BME / SimFin | IFRS | ✅ Production (35 companies) |
| Sweden | SE | Nasdaq Stockholm / SimFin | IFRS | ✅ Production (30 companies) |
| Netherlands | NL | Euronext Amsterdam / SimFin | IFRS | ✅ Production (24 companies) |
| Finland | FI | Nasdaq Helsinki / SimFin | IFRS | ✅ Production (25 companies) |
| Denmark | DK | Nasdaq Copenhagen / SimFin | IFRS | ✅ Production (15 companies) |
| Portugal | PT | Euronext Lisbon / SimFin | IFRS | ✅ Production (17 companies) |

## Dataset Statistics

The unified dataset (`data/historical_dataset_clean.parquet`) spans all 14 markets:

- **Total rows**: 155,696 (company × fiscal year × period type)
- **Columns**: 313
- **Period coverage**: 2008–2027 (US back to 2000)
- **Period types**: `annual` (primary for ML), `quarterly` (available for US)

### Per-Market Row Counts

| Market | Companies | Rows | Annual Rows | Coverage |
|---|---|---|---|---|
| US | 4,452 | 143,519 | 39,456 | 2000–2027 |
| KR | 106 | 3,396 | 927 | 2015–2026 |
| CA | 1,378 | 4,048 | — | 2021–2026 |
| JP | 122 | 498 | — | 2021–2026 |
| BR | 55 | 2,834 | 740 | 2010–2025 |
| DE | 76 | ~400 | — | 2021–2026 |
| FR | 40 | ~200 | — | 2021–2026 |
| IT | 40 | ~200 | — | 2021–2026 |
| ES | 35 | ~170 | — | 2021–2026 |
| SE | 30 | ~150 | — | 2021–2026 |
| NL | 24 | ~120 | — | 2021–2026 |
| FI | 25 | ~125 | — | 2021–2026 |
| DK | 15 | ~75 | — | 2021–2026 |
| PT | 17 | ~85 | — | 2021–2026 |

## Data Sources by Type

### Fundamental Data
- **US**: SEC EDGAR full-text search + SimFin for structured financials
- **KR**: DART (Data Analysis Retrieval and Transfer System) — FSS-mandated filings in K-IFRS
- **EU (DE/FR/IT/ES/SE/NL/FI/DK/PT)**: SimFin API — income statement, balance sheet, cash flow in IFRS
- **CA**: SimFin API — Canadian exchange-listed companies
- **BR**: SimFin API / B3/CVM — Brazilian Real-denominated financials
- **JP**: SimFin API / TDNET — J-GAAP annual filings

### Price Data
- **All markets**: yfinance — daily OHLCV, adjusted for splits and dividends
- **Enrichment**: 12M / 24M / 36M return windows, beta vs local index, volume ratios

### Macro Data
- **US**: US Treasury T-bill rates, CPI, GDP growth (FRED)
- **Other markets**: Local central bank rates, local inflation where available

## Data Quality Tiers

Data quality affects the `data_confidence` score (0.0–1.0) shown in the UI:

| Tier | Score Range | Coverage | Description |
|---|---|---|---|
| Tier 1 — High | ≥ 0.85 | 12+ features present, filing < 6 months old, post-2018 | Most reliable |
| Tier 2 — Good | 0.70–0.85 | 8–11 features, filing 6–18 months old | Reliable |
| Tier 3 — Medium | 0.55–0.70 | Moderate feature coverage | Use with caution |
| Tier 4 — Low | < 0.55 | Sparse data, late filing, pre-2012 vintage | Treat as indicative |

The `data_confidence` score is computed from three equally-weighted dimensions:

1. **Coverage** — fraction of 19 core analytical columns that are non-null
2. **Consistency** — 6 internal accounting identity checks (e.g. `total_assets > 0`, `gross_profit >= net_income`)
3. **Timeliness** — filing lag penalty + fiscal year vintage (pre-2012 XBRL era = 0.7 weight)

## Universe Definition (P0f)

Rows are classified `in_universe = 1` if they meet all of these criteria:

| Filter | Threshold |
|---|---|
| Period type | `annual` only |
| Fiscal year | 2009 – (current year − 1) |
| Revenue | ≥ USD 1M |
| Total assets | ≥ USD 100K |
| Entry price | > 0 (required) |
| OTC penny stocks | Excluded if exchange = OTC AND price < USD 1.00 (US only) |
| Financials sector | Excluded (SIC 6000–6999) — different accrual structure |
| Utilities sector | Excluded (SIC 4900–4999) — regulated earnings distort signals |

Per-market minimum price floors:

| Market | Min Price |
|---|---|
| US | USD 1.00 (OTC filter) |
| CA | CAD 0.05 |
| All others | No floor |

!!! note "Survivorship bias"
    The universe includes delisted and bankrupt companies for training purposes. Only the current universe filters apply for scoring (you can only invest in live companies). See [Bias & Validation](methodology/bias-validation.md).

## Accounting Standard Considerations

!!! warning "IFRS vs US GAAP"
    Several accrual-based features behave differently under IFRS vs US GAAP. The pipeline tags each row with `accounting_std` so models trained on US GAAP data should not be applied directly to IFRS companies without retraining or recalibration.

Key divergences:
- **Revenue recognition**: IFRS 15 vs ASC 606 — timing differences in contract revenues
- **Lease accounting**: IFRS 16 vs ASC 842 — operating lease capitalization rates differ
- **Inventory**: IFRS prohibits LIFO; affects cost of goods accruals
- **Goodwill**: IFRS allows impairment-only; US GAAP previously amortized

## Known Data Gaps

| Market | Gap | Notes |
|---|---|---|
| KR | `fraud_score_governance` = NaN | `small_auditor_flag` and `going_concern` require DART API integration (planned) |
| KR | SIC codes absent | Set to NaN; financials/utilities exclusion disabled for KR |
| Non-US | Quarterly rows sparse | ML models trained on annual data only |
| Non-US | AAER fraud labels | AAER database is US-only; `fraud_confirmed = 0` for all non-US rows |
