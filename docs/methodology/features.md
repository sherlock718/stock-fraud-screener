# Feature Engineering

The pipeline computes 319 features grouped into 8 categories (including 5 quarterly-enriched columns). The ML models use ~35 ICIR-selected features per horizon.

## Feature Categories

| Category | Count | Description |
|---|---|---|
| Accruals & Quality | 35 | Beneish components, accrual ratios, earnings quality |
| Profitability | 28 | ROA, ROE, EBITDA margins, operating leverage |
| Leverage & Solvency | 24 | Debt ratios, interest coverage, Altman Z-Score |
| Growth | 22 | Revenue, asset, headcount growth rates and indices |
| Efficiency | 20 | Asset turnover, receivables days, inventory days |
| Valuation | 18 | P/B, EV/EBITDA, P/E, P/FCF relative to sector |
| Market & Price | 32 | Returns, momentum, beta, volatility, volume |
| Governance & Audit | 19 | Auditor change, board size, insider ownership |
| Macro Context | 10 | T-bill rate, CPI, credit spread, GDP growth |
| Classical Scores | 7 | Beneish M-Score, Altman Z-Score, Piotroski F-Score, and components |
| Derived / Interaction | 63 | Cross-products of top single features |

## Key Features Explained

### Accruals (Beneish TATA)

```
accruals_to_assets = (net_income - operating_cash_flow) / total_assets
```

One of the most powerful fraud signals. When accruals (accounting earnings) significantly exceed cash flow, it suggests revenue recognition games or cost deferral.

**Signal direction:** High → higher fraud probability

### Days Sales Receivables Index (DSRI)

```
dsri = (receivables_t / revenue_t) / (receivables_{t-1} / revenue_{t-1})
```

A Beneish component. DSRI > 1.0 means receivables are growing faster than revenue — a classic channel stuffing signal.

**Signal direction:** > 1.05 → higher fraud probability

### Gross Margin Index (GMI)

```
gmi = gross_margin_{t-1} / gross_margin_t
```

GMI > 1.0 means gross margin declined year-over-year. Companies under financial pressure may manipulate revenue to mask margin deterioration.

**Signal direction:** > 1.0 → higher fraud probability

### Asset Quality Index (AQI)

```
aqi = (1 - (current_assets + ppe) / total_assets)_t / (1 - (current_assets + ppe) / total_assets)_{t-1}
```

Measures the proportion of assets in categories more easily manipulated (goodwill, deferred charges, intangibles).

### Revenue Growth — SGI

```
sgi = revenue_t / revenue_{t-1}
```

High revenue growth is not intrinsically suspicious, but SGI > 1.6 combined with other Beneish flags is a red flag.

### Auditor Change (Binary)

```
auditor_change = 1 if auditor_name_t != auditor_name_{t-1}
```

Binary flag. Auditor resignations (Form 8-K Item 4.01/4.02) are particularly significant — voluntary auditor switches are less alarming.

### Free Cash Flow to Net Income

```
fcf_to_ni = free_cash_flow / net_income
```

Values consistently below 0.7 suggest earnings are not converting to real cash — a quality red flag.

### Piotroski F-Score Components

9 binary tests across three pillars:

**Profitability (4 tests)**

| Test | Pass if |
|---|---|
| ROA positive | net_income / avg_assets > 0 |
| Operating cash flow positive | operating_cf > 0 |
| ROA increasing | roa_t > roa_{t-1} |
| FCF > ROA | fcf / assets > roa |

**Leverage / Liquidity (3 tests)**

| Test | Pass if |
|---|---|
| Leverage decreasing | debt_to_assets_t < debt_to_assets_{t-1} |
| Current ratio improving | current_ratio_t > current_ratio_{t-1} |
| No dilution | shares_outstanding_t <= shares_outstanding_{t-1} |

**Operating Efficiency (2 tests)**

| Test | Pass if |
|---|---|
| Gross margin improving | gross_margin_t > gross_margin_{t-1} |
| Asset turnover improving | revenue/assets_t > revenue/assets_{t-1} |

## ICIR Feature Selection

Before training, features are ranked by ICIR (Information Coefficient / StdIC):

```
IC_t = Spearman correlation(feature_t, return_{t+1year})
ICIR = mean(IC) / std(IC) over rolling window
```

Only features with ICIR > 0.05 (absolute) are retained. Then features with pairwise Spearman correlation > 0.90 are deduped — keeping only the higher-ICIR of each pair.

This typically selects ~35 features per horizon. The selection varies by horizon — longer horizons tend to select more structural features (governance, leverage) vs shorter horizons which favor more reactive features (accruals, momentum).

See [Factor Research](factor-research.md) for IC/ICIR analysis tools.
