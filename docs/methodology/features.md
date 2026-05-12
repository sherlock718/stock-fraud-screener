# Feature Engineering

The pipeline computes **346 features** per company-year observation. Features are organized into two parallel taxonomies: the **5-factor grouping** (how they're consumed in portfolio construction) and the **8-category grouping** (how they're computed in `pipeline/feature_library.py`).

The ML models use ~35 ICIR-selected features per horizon. See [Feature Selection →](feature-selection.md) for the full selection methodology.

---
## 5-Factor Grouping

This is the primary taxonomy for portfolio construction and composite alpha score computation.

| Factor Group | Count | Key Features | Status |
|---|---|---|---|
| **Value** | ~18 | P/B, EV/EBITDA, P/E, P/FCF, Acquirer's Multiple | ✅ |
| **Quality** | ~83 | ROE, ROA, Piotroski F-Score, gross margin stability, accruals ratio, asset turnover | ✅ |
| **Momentum** | ~37 | 12m-1m price return, cross-sectional rank transforms, earnings revision, volume trend | ✅ |
| **Growth** | ~22 | Revenue CAGR, EPS acceleration, asset growth, reinvestment rate | ✅ |
| **Fraud Risk** | ~164 | Beneish M-Score, Altman Z-Score, AAER labels, going concern, forensic accruals, governance | ✅ |
| **Quarterly enriched** | 5 | Revenue QoQ std, earnings momentum, max accruals TTM, revenue acceleration | ✅ |

---

## 8-Category Pipeline Grouping

This is how features are organized in `pipeline/feature_library.py` (single source of truth for all formulas).

| Category | Count | Factor Group | Description |
|---|---|---|---|
| Accruals & Earnings Quality | 35 | Quality / Fraud Risk | Beneish components, accrual ratios, earnings quality |
| Value & Valuation Ratios | 18 | Value | P/B, EV/EBITDA, P/E, P/FCF relative to sector |
| Profitability & Margins | 28 | Quality | ROA, ROE, EBITDA margins, operating leverage |
| Leverage & Solvency | 24 | Fraud Risk | Debt ratios, interest coverage, Altman Z-Score |
| Growth & Momentum | 22 | Growth | Revenue, asset, headcount growth rates |
| Efficiency & Turnover | 20 | Quality | Asset turnover, receivables days, inventory days |
| Forensic Accounting Signals | 19 | Fraud Risk | Auditor change, restatement risk, governance flags |
| Macro / Context | 10 | Fraud Risk | T-bill rate, CPI, credit spread, GDP growth |
| Classical Scores | 7 | Fraud Risk | Beneish M-Score, Altman Z-Score, Piotroski F-Score (composites) |
| Market & Price | 32 | Momentum | Returns, beta, volatility, volume |
| Momentum rank transforms | 5 | Momentum | Cross-sectional ranks within (fiscal_year × market): momentum_12m_rank, momentum_6m_rank, momentum_3m_rank, vol_rank_12m, momentum_composite_rank |
| Derived / Interaction | 63 | All | Cross-products of top single features |
| **Quarterly enriched** | 5 | Quality / Fraud Risk | Intra-year dynamics from Q1/Q2/Q3 filings |
| **Volatility (multi-horizon)** | **5** | Momentum / Quality | Annualised price vol (6m/36m/60m) + ROA rolling std (5yr) |
| **Total** | **346** | | |

---

## Value Features

Core cheapness signals relative to intrinsic value or market.

| Feature | Formula | Notes |
|---|---|---|
| `price_to_book` | market_cap / book_equity | Sector-relative version also computed |
| `ev_ebitda` | enterprise_value / ebitda | Carlisle Acquirer's Multiple variant included |
| `pe_ratio` | price / earnings_per_share | Trailing twelve months |
| `fcf_yield` | free_cash_flow / market_cap | Favours capital-light businesses |
| `ev_revenue` | enterprise_value / revenue | Useful for pre-profit companies |
| `ncav` | (current_assets − total_liabilities) / market_cap | Graham Net-Net value |
| `earnings_yield` | ebit / enterprise_value | Greenblatt Magic Formula component |

---

## Quality Features

Balance sheet strength and earnings reliability.

### Profitability

| Feature | Formula | Notes |
|---|---|---|
| `roe` | net_income / avg_book_equity | Most predictive quality signal |
| `roa` | net_income / avg_total_assets | Piotroski component |
| `gross_margin` | (revenue − cogs) / revenue | Novy-Marx Gross Profitability factor |
| `ebitda_margin` | ebitda / revenue | Operating profitability |
| `fcf_to_ni` | free_cash_flow / net_income | < 0.7 consistently = quality red flag |
| `roic` | ebit(1−tax) / invested_capital | Greenblatt Magic Formula component |

### Piotroski F-Score (9-point health index)

**Profitability (4 tests)**

| Test | Pass if |
|---|---|
| ROA positive | net_income / avg_assets > 0 |
| Operating cash flow positive | operating_cf > 0 |
| ROA increasing | roa_t > roa_{t−1} |
| FCF > ROA | fcf / assets > roa (cash earnings quality) |

**Leverage / Liquidity (3 tests)**

| Test | Pass if |
|---|---|
| Leverage decreasing | debt_to_assets_t < debt_to_assets_{t−1} |
| Current ratio improving | current_ratio_t > current_ratio_{t−1} |
| No dilution | shares_outstanding_t ≤ shares_outstanding_{t−1} |

**Operating Efficiency (2 tests)**

| Test | Pass if |
|---|---|
| Gross margin improving | gross_margin_t > gross_margin_{t−1} |
| Asset turnover improving | revenue/assets_t > revenue/assets_{t−1} |

Score 0–9. F-Score ≤ 3 = weak; ≥ 7 = strong.

### Accruals

```
accruals_to_assets = (net_income − operating_cash_flow) / total_assets
```

One of the strongest quality and fraud signals. When accounting earnings far exceed cash earnings, it suggests either aggressive revenue recognition or cost deferral.

**Signal direction:** High (> 0.05) → lower quality, higher fraud probability.

### Efficiency

| Feature | Formula |
|---|---|
| `asset_turnover` | revenue / avg_total_assets |
| `receivable_days` | (receivables / revenue) × 365 |
| `inventory_days` | (inventory / cogs) × 365 |
| `payable_days` | (payables / cogs) × 365 |

---

## Momentum Features

Cross-sectional momentum ranks are computed in `add_momentum_ranks()` in `pipeline/step5_compute_features.py`. Ranks are percentile-scaled within each (fiscal_year, market) group so a 20% annual return is evaluated relative to the same-year, same-market universe — not in absolute terms.

| Feature | Description |
|---|---|
| `momentum_12m_rank` | Cross-sectional percentile rank of 12m prior return within (fiscal_year, market) |
| `momentum_6m_rank` | Cross-sectional percentile rank of 6m prior return |
| `momentum_3m_rank` | Cross-sectional percentile rank of 3m prior return |
| `vol_rank_12m` | Inverted volatility rank — low volatility = high rank (low-vol premium) |
| `momentum_composite_rank` | Mean of 12m/6m/3m ranks — single momentum factor score |

Raw price features (existing):

| Feature | Description |
|---|---|
| `return_12m` | Total 12-month price return |
| `return_24m` | Total 24-month price return |
| `return_36m` | Total 36-month price return |
| `excess_return_12m` | Stock return minus local index return |
| `beta_12m` | Rolling beta vs local index |
| `price_volume_ratio` | Average daily dollar volume (3-month) |
| `volatility_90d` | 90-day realized price volatility |

**Citation**: Jegadeesh & Titman (1993) — cross-sectional momentum (12m-1m) generates ~1% monthly alpha. The rank transform is preferred over raw returns because it is scale-invariant across markets and years.

---

## Multi-Horizon Volatility Features

Annualised daily-return volatility and rolling fundamental stability, added via `scripts/patch_equity_vol_features.py`. Computed from `price_cache.db` (daily prices) and rolling ROA/ROE series.

| Feature | Formula | Fill Rate | Notes |
|---|---|---|---|
| `vol_prior_6m` | `std(daily_returns[-126d]) × √252` | 95.4% | Short-term volatility, ~6 months lookback |
| `vol_prior_12m` | `std(daily_returns[-252d]) × √252` | 95.5% | Medium-term (already existed) |
| `vol_prior_36m` | `std(daily_returns[-756d]) × √252` | 95.5% | Long-term regime volatility |
| `vol_prior_60m` | `std(daily_returns[-1260d]) × √252` | 95.4% | 5-year baseline volatility |
| `roa_volatility_5yr` | `rolling(5yr).std(roa)`, min 3 obs | 91.5% | Fundamental earnings stability |
| `earnings_stability_roa_5yr` | `−roa_volatility_5yr` | 91.5% | Positive = more stable ROA |
| `roe_volatility_5yr` | `rolling(5yr).std(roe)`, min 3 obs | 64.3% | Lower fill due to sparser ROE |
| `earnings_stability_5yr` | `−roe_volatility_5yr` | 64.3% | Positive = more stable ROE |

Minimum 20 trading-day observations required for price-based windows. All price volatilities are annualised via ×√252.



Fundamental growth trajectory and capital allocation signals.

| Feature | Description |
|---|---|
| `revenue_cagr_3y` | 3-year compound annual revenue growth |
| `revenue_cagr_5y` | 5-year compound annual revenue growth |
| `eps_growth_yoy` | Year-over-year EPS growth |
| `asset_growth_yoy` | Year-over-year total asset growth |
| `capex_to_assets` | Capital expenditure intensity (reinvestment rate) |
| `rd_to_revenue` | R&D investment ratio |
| `sgi` | Sales Growth Index = revenue_t / revenue_{t−1} (Beneish component) |

---

## Fraud Risk Features

Forensic accounting signals, classical manipulation indices, and governance flags.

### Beneish M-Score (8-variable manipulation index)

| Component | Formula | Red Flag Direction |
|---|---|---|
| DSRI | (receivables_t/revenue_t) / (receivables_{t−1}/revenue_{t−1}) | > 1.05 |
| GMI | gross_margin_{t−1} / gross_margin_t | > 1.0 |
| AQI | non-current other assets ratio change | Increasing |
| SGI | revenue_t / revenue_{t−1} | High alone is OK; high + other flags = red |
| DEPI | (depreciation_{t−1} / (depreciation_{t−1}+ppe_{t−1})) / same_t | > 1.0 |
| SGAI | (SGA/revenue)_t / (SGA/revenue)_{t−1} | Increasing |
| TATA | (net_income − operating_cf) / total_assets | Large positive |
| LVGI | total_liabilities_t / total_assets_t, divided by prior year | > 1.0 |

Composite: M-Score > −1.78 → likely manipulator (Beneish original threshold).

### Days Sales Receivables Index (DSRI)

```
dsri = (receivables_t / revenue_t) / (receivables_{t-1} / revenue_{t-1})
```

DSRI > 1.05 means receivables are growing faster than revenue — classic channel stuffing signal.

### Gross Margin Index (GMI)

```
gmi = gross_margin_{t-1} / gross_margin_t
```

GMI > 1.0 = gross margin declined year-over-year. Companies under pressure may inflate revenue to mask margin deterioration.

### Asset Quality Index (AQI)

```
aqi = [1 - (current_assets + ppe) / total_assets]_t  ÷  [same]_{t-1}
```

Measures growth in hard-to-audit assets (goodwill, deferred charges, intangibles).

### Altman Z-Score (5-variable bankruptcy predictor)

```
Z = 1.2×(WC/A) + 1.4×(RE/A) + 3.3×(EBIT/A) + 0.6×(MC/TL) + 1.0×(Rev/A)
```

Z < 1.81 = distress zone; 1.81–2.99 = grey zone; ≥ 2.99 = safe zone.

### Governance / Audit Signals

| Feature | Description |
|---|---|
| `auditor_change` | 1 if auditor name changed year-over-year |
| `big4_auditor` | 1 if auditor is PwC, Deloitte, EY, or KPMG |
| `small_auditor_flag` | Large company (assets > $100M) with non-Big-4 auditor |
| `going_concern` | 1 if SEC filing disclosed going concern doubt |
| `insider_selling_flag` | Net sold > 10K shares AND sales > buys (Form 4) |

---

## Quarterly-Enriched Features (5 columns)

Computed by `scripts/enrich_quarterly_features.py` from Q1/Q2/Q3 filings and joined onto annual training rows (74.8% coverage).

| Feature | Description |
|---|---|
| `revenue_qoq_std_norm` | Std deviation of Q1→Q2→Q3 revenue growth (earnings smoothing proxy) |
| `earnings_qoq_mean` | Mean QoQ net income growth (earnings momentum) |
| `max_accruals_ttm` | Max \|wc_accruals_to_assets\| across available quarters |
| `revenue_acceleration` | Q3/Q1 revenue ratio (intra-year sales ramp) |
| `quarterly_positive_rev_frac` | Fraction of quarters with positive QoQ revenue growth |

---

## Macro Context Features

Joined at fiscal year-end date. Used primarily in Fraud Risk factor as contextual signals (distress is more likely in recessions).

| Feature | Source |
|---|---|
| `tbill_3m` | 3-month US Treasury yield (or local equivalent) |
| `cpi_yoy` | CPI year-over-year inflation |
| `gdp_growth` | Real GDP growth rate |
| `credit_spread` | Investment grade credit spread |

!!! note "PSI filter"
    Macro context features are frequently dropped by the PSI filter during ML training — their distributions shift significantly between training and scoring periods. They are retained in the dataset for the Fraud Risk factor computation but may not appear in final LightGBM feature sets.
