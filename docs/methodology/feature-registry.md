# Feature Registry

All 326 columns in `data/historical_dataset_clean.parquet`, organized by category.

This is the authoritative reference for column names, data types, and factor group assignments. For formula definitions see `pipeline/feature_library.py`. For narrative descriptions see [features.md](features.md).

---

## Identity / Administrative (non-feature)

| Column | Type | Description |
|---|---|---|
| `cik` | str | SEC CIK or DART corp_code (primary identifier) |
| `corp_code` | str | DART Korea corporation code |
| `stock_code` | str | Exchange-specific ticker code |
| `ticker` | str | Canonical ticker symbol |
| `name` | str | Company name |
| `market` | str | Market code: US / KR / EU / BR / CA / JP |
| `country` | str | ISO 2-letter country code |
| `exchange` | str | Exchange name |
| `currency` | str | Reporting currency |
| `accounting_std` | str | GAAP / IFRS / K-IFRS |
| `sector` | str | Sector name (SIC-derived) |
| `industry_code` | str | SIC 4-digit industry code |
| `sic_code` | str | Raw SIC code |
| `sic_2digit` | str | 2-digit SIC group |
| `sic_description` | str | SIC description |
| `fiscal_year` | int | Fiscal year end year |
| `fiscal_quarter` | str | Always null — annual-only dataset |
| `period_type` | str | Always `'annual'` |
| `filed_date` | date | SEC/DART filing date |
| `as_of_date` | date | Financial statement date |
| `filing_lag_days` | int | Days between fiscal year-end and filing |
| `benchmark_used` | str | Local benchmark index ticker |
| `size_category` | int | 1=micro, 2=small, 3=mid, 4=large |
| `size_category_label` | str | Human-readable size category |
| `data_confidence` | float | Data completeness confidence score |
| `denom` | float | Denominator used in scaling (internal) |
| `excl_reason` | str | Exclusion reason if `in_universe=0` |
| `in_universe` | bool | 1 = included in training universe |
| `acc_mt` | str | Accounting month (filing period end) |

---

## Raw Financials (input columns, not ML features)

Income statement, balance sheet, and cash flow raw values. Used to compute feature columns.

| Column | Notes |
|---|---|
| `revenue` | Total revenue |
| `cogs` | Cost of goods sold |
| `gross_profit` | Revenue − COGS |
| `sga_expense` / `sga` | SG&A expense |
| `rd_expense` | R&D expense |
| `depreciation` / `depreciation_amortization` | D&A |
| `operating_income` | EBIT proxy |
| `interest_expense` | Interest expense |
| `non_operating_income` | Non-operating income |
| `pretax_income` | Income before taxes |
| `tax_expense` | Income tax |
| `net_income` | Net income attributable to common |
| `eps_basic` / `eps_diluted` | Earnings per share |
| `dividends_per_share` | Dividends per common share |
| `total_assets` | Total assets |
| `current_assets` | Current assets |
| `cash` | Cash and equivalents |
| `receivables` / `accounts_receivable` | Trade receivables |
| `inventory` | Inventory |
| `ppe` / `ppe_gross` / `ppe_net` | Property, plant and equipment |
| `goodwill` | Goodwill |
| `intangibles` | Intangible assets |
| `other_noncurrent_assets` | Other noncurrent assets |
| `total_liabilities` | Total liabilities |
| `current_liabilities` | Current liabilities |
| `accounts_payable` | Trade payables |
| `short_term_debt` | Short-term borrowings |
| `long_term_debt` | Long-term debt |
| `total_debt` | Total interest-bearing debt |
| `retained_earnings` | Retained earnings |
| `total_equity` / `equity` | Total shareholders' equity |
| `common_shares_outstanding` / `shares_outstanding` | Shares outstanding |
| `shares_at_filing` | Shares outstanding at filing date |
| `operating_cash_flow` / `cfo` | Cash from operations |
| `capex` | Capital expenditures |
| `fcf` | Free cash flow |
| `financing_cash_flow` | Cash from financing |
| `cfi` | Cash from investing |
| `ebitda` | EBITDA |
| `enterprise_value` | Market cap + net debt |
| `market_cap_at_filing` | Market cap at filing date |
| `entry_price` | Price at fiscal year-end |
| `net_debt` | Total debt − cash |
| `noa` | Net operating assets |
| `working_capital` | Current assets − current liabilities |

---

## Value Features (~18 columns)

| Column | Formula |
|---|---|
| `price_to_book` / `pb_ratio` | Market cap / book equity |
| `pb_ratio_sector_pct` | Sector-relative percentile |
| `pe_ratio` | Price / EPS (TTM) |
| `pe_ratio_sector_pct` | Sector-relative percentile |
| `ev_ebitda` | Enterprise value / EBITDA |
| `ev_ebitda_sector_pct` | Sector-relative percentile |
| `ev_revenue` | Enterprise value / revenue |
| `ev_ocf` | Enterprise value / operating cash flow |
| `fcf_yield` | FCF / market cap |
| `earnings_yield` | EBIT / enterprise value |
| `ps_ratio` | Price / sales |
| `ps_ratio_sector_pct` | Sector-relative percentile |
| `pcf_ratio` | Price / cash flow |
| `book_to_market` | Book equity / market cap |
| `sales_to_price` | Revenue / market cap |
| `value_composite` | Composite value score |
| `value_in_high_rate` | Value × rate environment interaction |
| `value_in_recession` | Value × recession interaction |

---

## Quality Features (~83 columns)

### Profitability

| Column | Description |
|---|---|
| `roe` | Return on equity |
| `roe_sector_pct` | Sector-relative percentile |
| `roa` | Return on assets |
| `roa_operating` | Operating ROA |
| `roa_sector_pct` | Sector-relative percentile |
| `roic` | Return on invested capital |
| `gross_margin` | Gross profit / revenue |
| `gross_margin_sector_pct` | Sector-relative percentile |
| `operating_margin` | Operating income / revenue |
| `operating_margin_sector_pct` | Sector-relative percentile |
| `net_margin` | Net income / revenue |
| `ebitda_margin` | EBITDA / revenue |
| `ocf_margin` | Operating cash flow / revenue |
| `gross_profit_to_assets` | Novy-Marx gross profitability |
| `quality_composite` | Composite quality score |
| `quality_in_recession` | Quality × recession interaction |
| `small_x_quality` | Size × quality interaction |

### Piotroski F-Score components

| Column | Description |
|---|---|
| `piotroski_f_score` | Composite 0–9 |
| `piotroski_roa_pos` | ROA > 0 |
| `piotroski_ocf_pos` | Operating CF > 0 |
| `piotroski_delta_roa` | ROA increasing |
| `piotroski_delta_lev` | Leverage decreasing |
| `piotroski_delta_liq` | Current ratio improving |

### Accruals

| Column | Description |
|---|---|
| `accruals_to_assets` | (NI − OCF) / total assets |
| `wc_accruals_to_assets` | Sloan working capital accruals |
| `sloan_accruals` | Sloan accruals ratio |
| `sloan_accruals_sector_pct` | Sector-relative percentile |
| `accruals_avg_3y` | 3-year average accruals |
| `accruals_ratio` | Alternative accruals ratio |
| `noa_to_assets` | NOA / total assets |
| `noa_growth` | NOA year-over-year growth |
| `ocf_to_ni` | Cash flow quality ratio |
| `ocf_to_ni_sector_pct` | Sector-relative percentile |
| `soft_assets_ratio` | Soft (hard-to-audit) assets ratio |
| `cash_conversion` | Cash conversion efficiency |
| `cash_conversion_change` | Year-over-year change |

### Efficiency

| Column | Description |
|---|---|
| `asset_turnover` | Revenue / avg total assets |
| `asset_turnover_change` | Year-over-year change |
| `days_sales_outstanding` | Receivables days |
| `delta_dso` | Change in DSO |
| `days_inventory` | Inventory days |
| `days_payable` | Payable days |
| `cash_conversion_cycle` | DSO + DIO − DPO |
| `interest_coverage` | EBIT / interest expense |
| `financial_leverage` | Total assets / equity |

### Trend signals

| Column | Description |
|---|---|
| `gross_margin_trend_3y` | 3-year gross margin trend |
| `operating_margin_trend_3y` | 3-year operating margin trend |
| `roa_trend_3y` | 3-year ROA trend |
| `earnings_stability_5yr` | 5-year earnings volatility (inverse) |
| `roe_volatility_5yr` | 5-year ROE volatility |

### Interaction features (quality-related)

| Column | Description |
|---|---|
| `roa_x_noa_growth` | ROA × NOA growth interaction |
| `value_x_quality` | Value × quality cross-product |
| `quality_x_momentum` | Quality × momentum cross-product |

---

## Momentum Features (~45 columns)

### Cross-sectional rank transforms

| Column | Description |
|---|---|
| `momentum_12m_rank` | Percentile rank of 12m return within (fiscal_year × market) |
| `momentum_6m_rank` | Percentile rank of 6m return |
| `momentum_3m_rank` | Percentile rank of 3m return |
| `vol_rank_12m` | Inverted volatility rank |
| `momentum_composite_rank` | Mean of 12m/6m/3m ranks |
| `momentum_consistency` | Fraction of positive monthly returns (trailing 12m) |
| `momentum_in_expansion` | Momentum rank × non-recession indicator |

### Raw price features

| Column | Description |
|---|---|
| `momentum_12m_prior` | 12-month raw return |
| `momentum_6m_prior` | 6-month raw return |
| `momentum_3m_prior` | 3-month raw return |
| `momentum_12m_prior_sector_pct` | Sector-relative 12m return percentile |
| `vol_prior_12m` | 12-month realized volatility |
| `price_to_52w_high` | Price relative to 52-week high |

### Multi-horizon returns

| Column | Description |
|---|---|
| `forward_return_{6m,1y,2y,3y,4y,5y,6y,7y,8y,10y,15y}` | Forward total return (target variables) |
| `excess_return_local_{6m,1y,2y,3y,4y,5y,6y,7y,8y,10y,15y}` | Stock return minus local index |
| `benchmark_return_{6m,1y,2y,3y,4y,5y,6y,7y,8y,10y,15y}` | Local benchmark return |
| `beat_local_market_{6m,1y,2y,3y,4y,5y,6y,7y,8y,10y,15y}` | 1 if stock beat local benchmark |

---

## Growth Features (~22 columns)

| Column | Description |
|---|---|
| `revenue_cagr_3y` | 3-year revenue CAGR |
| `revenue_growth_yoy` / `revenue_growth` | Year-over-year revenue growth |
| `revenue_growth_sector_pct` | Sector-relative percentile |
| `eps_growth_yoy` / `eps_growth` | Year-over-year EPS growth |
| `net_income_growth_yoy` / `net_income_growth` | Year-over-year NI growth |
| `net_income_growth_sector_pct` | Sector-relative percentile |
| `gross_profit_growth_yoy` | Year-over-year GP growth |
| `asset_growth_yoy` / `assets_growth` | Year-over-year asset growth |
| `assets_growth_sector_pct` | Sector-relative percentile |
| `equity_growth` / `equity_change_yoy` | Year-over-year equity growth |
| `capex_growth` / `capex_growth_yoy` | Year-over-year capex growth |
| `capex_intensity` | Capex / total assets |
| `rd_intensity` / `rd_growth_yoy` | R&D investment intensity / growth |
| `ocf_growth` / `ocf_growth_yoy` | Year-over-year OCF growth |

---

## Fraud Risk Features (~164 columns)

### Beneish M-Score components

| Column | Description |
|---|---|
| `beneish_dsri` | Days Sales Receivables Index |
| `beneish_gmi` | Gross Margin Index |
| `beneish_aqi` | Asset Quality Index |
| `beneish_sgi` | Sales Growth Index |
| `beneish_depi` | Depreciation Index |
| `beneish_sgai` | SG&A Index |
| `beneish_tata` | Total Accruals to Total Assets |
| `beneish_lvgi` | Leverage Index |
| `beneish_m_score` | Composite M-Score (> −1.78 = manipulator) |
| `beneish_m_score_sector_pct` | Sector-relative percentile |

### Altman Z-Score

| Column | Description |
|---|---|
| `altman_x1` | Working capital / total assets |
| `altman_x2` | Retained earnings / total assets |
| `altman_x3` | EBIT / total assets |
| `altman_x4` | Market cap / total liabilities |
| `altman_x5` | Revenue / total assets |
| `altman_z_score` | Composite Z-Score (< 1.81 = distress) |
| `altman_z_score_sector_pct` | Sector-relative percentile |

### Ohlson O-Score (bankruptcy probability)

| Column | Description |
|---|---|
| `ohlson_size` | Log(assets / GNP deflator) |
| `ohlson_leverage` | Total liabilities / total assets |
| `ohlson_wc` | Working capital / total assets |
| `ohlson_roe` | Net income / total assets |
| `ohlson_ocf` | OCF / total liabilities |
| `ohlson_nits` | 1 if NI negative for two consecutive years |
| `ohlson_o_score` | Composite O-Score |
| `ohlson_prob_bankruptcy` | Probability of bankruptcy (logistic transform) |

### Leverage / solvency

| Column | Description |
|---|---|
| `debt_to_assets` | Total debt / total assets |
| `debt_to_assets_sector_pct` | Sector-relative percentile |
| `debt_to_equity` | Total debt / total equity |
| `net_debt_to_ebitda` | Net debt / EBITDA |
| `net_debt_to_equity` | Net debt / equity |
| `ocf_to_assets` | Operating CF / total assets |
| `ocf_to_debt` | Operating CF / total debt |
| `leverage_trend_3y` | 3-year debt-to-assets trend |
| `equity_ratio` | Equity / total assets |
| `ohlson_leverage` | Ohlson leverage ratio |

### Fraud taxonomy sub-scores

| Column | Description |
|---|---|
| `fraud_score_accounting` | Earnings manipulation (accruals, channel-stuffing) |
| `fraud_score_dilution` | Equity issuance / dilution fraud |
| `fraud_score_distress` | Financial distress signals |
| `fraud_score_governance` | Governance failures |
| `fraud_score_quality` | Earnings quality signals |
| `fraud_score_composite` | Composite fraud signal score |

### Labels / flags

| Column | Description |
|---|---|
| `fraud_confirmed` | 1 = AAER-confirmed fraud (target variable) |
| `fraud_suspect` | 1 = signal-based suspect flag |

### Shares / dilution

| Column | Description |
|---|---|
| `shares_dilution` | Year-over-year share count change |
| `shares_growth` | Shares outstanding growth |
| `ar_to_revenue_change` | Change in receivables-to-revenue ratio |
| `receivables_growth` / `receivables_growth_yoy` | Receivables growth |
| `receivables_minus_revenue_growth` | Receivables growing faster than revenue |
| `inventory_growth` / `inventory_growth_yoy` | Inventory growth |
| `ap_growth` / `ap_growth_yoy` | Accounts payable growth |
| `debt_growth` / `debt_growth_yoy` | Debt growth |
| `lt_debt_growth_yoy` | Long-term debt growth |
| `cogs_growth` / `cogs_growth_yoy` | COGS growth |
| `sga_growth` / `sga_growth_yoy` | SG&A growth |
| `cash_growth` | Cash growth |
| `ppe_growth` / `ppe_growth_yoy` | PP&E growth |
| `rd_growth` | R&D expense growth |
| `cash_change_yoy` | Cash balance year-over-year change |

### Log-transformed size

| Column | Description |
|---|---|
| `log_assets` | log(total_assets) |
| `log_market_cap` | log(market_cap_at_filing) |
| `log_revenue` | log(revenue) |

---

## Macro / Context Features (~10 columns)

| Column | Source | Notes |
|---|---|---|
| `tbill_3m` / `fed_funds_rate` | Federal Reserve | 3-month rate |
| `treasury_2y` / `treasury_10y` | Federal Reserve | Yield curve components |
| `yield_curve` | Derived | 10y − 2y spread |
| `cpi_yoy` | BLS | CPI year-over-year |
| `gdp_growth` | BEA | Real GDP growth |
| `credit_spread_baa` / `hy_spread` | FRED | Investment grade / high yield spreads |
| `vix` | CBOE | VIX closing level |
| `real_rate_10y` | Derived | 10y − CPI |
| `recession` | NBER | 1 if NBER recession quarter |
| `macro_regime` | Derived | Composite macro regime label |
| `credit_tightening` | Derived | 1 if credit spread widening |
| `levered_in_tight_credit` | Interaction | Leverage × credit tightening |

---

## Quarterly-Enriched Features (5 columns)

Added by `scripts/enrich_quarterly_features.py`. 74.8% coverage.

| Column | Description |
|---|---|
| `revenue_qoq_std_norm` | Std dev of Q1→Q3 quarterly revenue growth |
| `earnings_qoq_mean` | Mean QoQ net income growth |
| `max_accruals_ttm` | Max |wc_accruals_to_assets| across available quarters |
| `revenue_acceleration` | Q3/Q1 revenue ratio (intra-year sales ramp) |
| `quarterly_positive_rev_frac` | Fraction of quarters with positive QoQ revenue growth |

---

## Derived / Interaction Features (~63 columns)

Cross-products of top single features. Computed in `pipeline/feature_library.py`. Examples:

| Column | Description |
|---|---|
| `value_x_accruals` | Value composite × accruals ratio |
| `value_x_momentum` | Value composite × momentum rank |
| `value_x_quality` | Value composite × quality composite |
| `quality_x_momentum` | Quality composite × momentum rank |
| `roa_x_noa_growth` | ROA × NOA growth |
| `small_x_quality` | Size category × quality composite |

For the full list of derived columns, see `pipeline/feature_library.py` function `add_interaction_features()`.
