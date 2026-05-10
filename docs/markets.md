# Markets & Data Coverage

## Supported Markets

| Market | Code | Source | Accounting Std | Status |
|---|---|---|---|---|
| United States | US | SEC EDGAR | US GAAP | ✅ Production (8,001 companies) |
| European Union | EU | SimFin API | IFRS | 🔄 Pipeline stub built |
| South Korea | KR | DART API | K-IFRS | 🔄 Pipeline stub built |
| Japan | JP | TDNET | J-GAAP | 🗓 Planned |
| Canada | CA | SEDAR+ | IFRS / CA GAAP | 🗓 Planned |
| Brazil | BR | B3/CVM | IFRS / BR GAAP | 🗓 Planned |

## US Dataset Statistics

The US dataset (`data/historical_dataset_clean.parquet`) is the primary production dataset:

- **Companies**: 8,001 unique tickers
- **Rows**: ~163,000 (company × fiscal year)
- **Features**: 278 columns
- **Period coverage**: ~2000–2024
- **Period types**: `annual` (primary), `quarterly` (available)

## Data Sources by Type

### Fundamental Data
- **US**: SEC EDGAR full-text search + SimFin for structured financials
- **EU**: SimFin API — income statement, balance sheet, cash flow in IFRS
- **Korea**: DART (Data Analysis Retrieval and Transfer System) — FSS-mandated filings

### Price Data
- **All markets**: yfinance — daily OHLCV, adjusted for splits and dividends
- **Enrichment**: 12M / 24M / 36M return windows, beta vs local index, volume ratios

### Macro Data
- **US**: US Treasury T-bill rates, CPI, GDP growth (FRED)
- **Other markets**: Local central bank rates, local inflation where available

## Data Quality Tiers

Data quality affects the confidence score shown in the UI:

| Tier | Coverage | Confidence |
|---|---|---|
| Tier 1 | 12+ features present, filing < 6 months old, large cap | High |
| Tier 2 | 8–11 features, filing 6–18 months old, mid cap | Medium |
| Tier 3 | <8 features, filing >18 months old, small cap | Low |

## Accounting Standard Considerations

!!! warning "IFRS vs US GAAP"
    Several accrual-based features behave differently under IFRS vs US GAAP. The pipeline tags each row with `accounting_std` so models trained on US GAAP data should not be applied directly to IFRS companies without retraining or recalibration.

Key divergences:
- **Revenue recognition**: IFRS 15 vs ASC 606 — timing differences in contract revenues
- **Lease accounting**: IFRS 16 vs ASC 842 — operating lease capitalization rates differ
- **Inventory**: IFRS prohibits LIFO; affects cost of goods accruals
- **Goodwill**: IFRS allows impairment-only; US GAAP previously amortized

## Universe Definition

Companies are included if they meet all of these criteria at screening time:

| Filter | Threshold |
|---|---|
| Market cap | ≥ USD 150M |
| Avg daily volume (3M) | ≥ USD 500K |
| Reporting history | ≥ 3 years annual filings |
| Fiscal year completeness | ≥ 8 fundamental features present |
| Exchange listing | Major exchange only (no OTC) |

!!! note "Survivorship bias"
    The universe includes delisted and bankrupt companies for training purposes. Only the current universe filters apply for scoring (you can only invest in live companies). See [Bias & Validation](methodology/bias-validation.md).
