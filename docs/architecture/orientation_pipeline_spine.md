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
              └──→ step6_clean.py ──────────→ data/historical_dataset_clean.parquet
                   (structural clean, quality      ← CORE PIPELINE ENDS HERE
                    fixes, imputation, survivorship,
                    confidence score)
                                                        │
                                                        ▼ (optional enrichment layer)
                              enrich_fraud_taxonomy.py ─→ adds fraud_score_* columns (in-place)
                              enrich_fraud_labels.py ───→ adds fraud_confirmed/suspect (in-place)
                              (run via workflows/run_dataset_enrichments.py)
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

## 2. APIs Used

| Step | API / Library | Endpoint / Series | # Calls | Rate Limit | Auth |
|------|---------------|-------------------|---------|------------|------|
| 1 | SEC EDGAR | `https://www.sec.gov/files/company_tickers.json` | 1 | 10 req/s (SEC fair use) | None (User-Agent required) |
| 1 | SEC EDGAR | `https://www.sec.gov/files/company_tickers_exchange.json` | 1 | " | " |
| 1 | SEC EDGAR | `https://data.sec.gov/submissions/CIK{cik}.json` | ~8,000 (1 per company) | " | " |
| 2 | SEC EDGAR XBRL | `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json` | ~8,000 (1 per company) | " | " |
| 3 | **yfinance** (Yahoo Finance) | `yf.download(ticker, start, end)` | ~8,000 tickers | 1.5 req/s (self-imposed) | None (cached in `data/price_cache.db` SQLite) |
| 4 | **FRED API** (fredapi library) | 9 series: DGS10, DGS2, T10Y2Y, FEDFUNDS, BAA10Y, BAMLH0A0HYM2, CPIAUCSL, USREC, VIXCLS | 9 | 120 req/min | `FRED_API_KEY` in `.env` |
| 5 | None | Pure computation from parquet files | 0 | — | — |
| 6 | None | Pure computation (clean/impute) | 0 | — | — |

**Total API cost**: $0 (all free APIs). Total runtime: ~4-6 hours for full US rebuild (dominated by step2/step3 rate limits).

---

## 3. Join Keys — How Steps Merge

### Universal Join Key

```
['cik', 'ticker', 'filed_date', 'fiscal_year', 'fiscal_quarter', 'period_type']
```

### Step-by-Step Merge Logic

| Merge | Method | Key | Join Type | Notes |
|-------|--------|-----|-----------|-------|
| step1 → step2 | step2 reads tickers.parquet, fetches XBRL per CIK | `cik` | Iteration (not a SQL join) | One API call per CIK produces multiple rows (one per fiscal period) |
| step2 → step3 | step3 reads snapshots.parquet, looks up stock prices | `(ticker, filed_date)` | Iteration + cache lookup | Price looked up on/after filed_date (skip weekends) |
| step2 → step4 | `pd.merge_asof(direction='backward')` | `filed_date` | Backward asof | Each filing maps to most recent macro observation — PIT safe |
| step5 merges all | `pd.merge(..., how='left')` × 2 | `['cik','ticker','filed_date','fiscal_year','fiscal_quarter','period_type']` | LEFT JOIN | snapshots LEFT JOIN prices, then LEFT JOIN macro |
| step6 dedup | `drop_duplicates(keep='first')` | `['cik','market','filed_date','period_type']` | In-place dedup | Different fiscal_year with same key → keeps first |

### Why `filed_date` is the Anchor (not `fiscal_year`)

`filed_date` is when the filing became public on EDGAR — it's the earliest date an investor could act on the information. This ensures:
- No lookahead bias (we never use future data)
- Prices are looked up AFTER the filing is public
- Macro state is the macro state AT the time of filing

---

## 4. Sample Data Per Stage

### Step 1 — `data/tickers.parquet` (8,021 rows × 9 cols)

```
       cik ticker                       name exchange sic_code                                      sic_description market       country accounting_std
0001045810   NVDA                NVIDIA CORP   Nasdaq     3674                     Semiconductors & Related Devices     US United States           GAAP
0000320193   AAPL                 Apple Inc.   Nasdaq     3571                                 Electronic Computers     US United States           GAAP
0000789019   MSFT             MICROSOFT CORP   Nasdaq     7372                        Services-Prepackaged Software     US United States           GAAP
0001018724   AMZN             AMAZON COM INC   Nasdaq     5961                   Retail-Catalog & Mail-Order Houses     US United States           GAAP
0001318605   TSLA                Tesla, Inc.   Nasdaq     3711                Motor Vehicles & Passenger Car Bodies     US United States           GAAP
0001326801   META       Meta Platforms, Inc.   Nasdaq     7370 Services-Computer Programming, Data Processing, Etc.     US United States           GAAP
0000104169    WMT               Walmart Inc.   Nasdaq     5331                                Retail-Variety Stores     US United States           GAAP
0000019617    JPM        JPMORGAN CHASE & CO     NYSE     6021                            National Commercial Banks     US United States           GAAP
0000002488    AMD ADVANCED MICRO DEVICES INC   Nasdaq     3674                     Semiconductors & Related Devices     US United States           GAAP
0001403161      V                  VISA INC.     NYSE     7389                      Services-Business Services, NEC     US United States           GAAP
0000050863   INTC                 INTEL CORP   Nasdaq     3674                     Semiconductors & Related Devices     US United States           GAAP
0000034088    XOM           EXXON MOBIL CORP     NYSE     2911                                   Petroleum Refining     US United States           GAAP
0000200406    JNJ          JOHNSON & JOHNSON     NYSE     2834                          Pharmaceutical Preparations     US United States           GAAP
0001141391     MA             Mastercard Inc     NYSE     7389                      Services-Business Services, NEC     US United States           GAAP
0000070858    BAC  BANK OF AMERICA CORP /DE/     NYSE     6021                            National Commercial Banks     US United States           GAAP
0000886982     GS    GOLDMAN SACHS GROUP INC     NYSE     6211      Security Brokers, Dealers & Flotation Companies     US United States           GAAP
0001065280   NFLX                NETFLIX INC   Nasdaq     7841                           Services-Video Tape Rental     US United States           GAAP
0000012927     BA                  BOEING CO     NYSE     3721                                             Aircraft     US United States           GAAP
0001744489    DIS             Walt Disney Co     NYSE     7990        Services-Miscellaneous Amusement & Recreation     US United States           GAAP
0000078003    PFE                 PFIZER INC     NYSE     2834                          Pharmaceutical Preparations     US United States           GAAP
```

### Step 2 — `data/snapshots.parquet` (191,579 rows × 95 cols)

Showing AAPL annual filings (key financial columns):

```
ticker  fiscal_year  filed_date       revenue       net_income      total_assets        equity   ocf             rev_growth_yoy  ni_growth_yoy  roa_trend_3y  rev_cagr_3y
  AAPL         2009  2010-01-25   24,578,000,000   3,495,000,000  36,171,000,000   9,984,000,000   5,470,000,000           NaN            NaN          NaN          NaN
  AAPL         2010  2010-10-27   37,491,000,000   6,119,000,000  36,171,000,000  14,531,000,000   9,596,000,000        0.5254         0.7508          NaN          NaN
  AAPL         2011  2011-10-26   42,905,000,000   8,235,000,000  47,501,000,000  22,297,000,000  10,159,000,000        0.1444         0.3458       0.0767       0.3212
  AAPL         2012  2013-04-24   65,225,000,000  14,013,000,000 116,371,000,000  31,640,000,000  18,595,000,000        0.5202         0.7016      -0.0488       0.3190
  AAPL         2013  2013-10-30  108,249,000,000  25,922,000,000 176,064,000,000  47,791,000,000  37,529,000,000        0.6596         0.8499      -0.0261       0.5884
  AAPL         2014  2014-10-27  156,508,000,000  41,733,000,000 207,000,000,000  76,615,000,000           NaN          0.4458         0.6099       0.0812       0.5490
  AAPL         2015  2015-10-28  170,910,000,000  37,037,000,000 231,839,000,000 118,210,000,000           NaN          0.0920        -0.1125       0.0125       0.2565
  AAPL         2016  2016-10-26  182,795,000,000  39,510,000,000 290,345,000,000 123,549,000,000           NaN          0.0695         0.0668      -0.0655       0.0807
  AAPL         2017  2017-11-03  233,715,000,000  53,394,000,000 321,686,000,000 111,547,000,000  81,266,000,000        0.2786         0.3514       0.0062       0.1694
  AAPL         2018  2018-11-05  215,639,000,000  45,687,000,000 375,319,000,000 119,355,000,000  66,231,000,000       -0.0773        -0.1443      -0.0144       0.0861
  AAPL         2019  2019-10-31  229,234,000,000  48,351,000,000 365,725,000,000 128,249,000,000  64,225,000,000        0.0630         0.0583      -0.0338      -0.0096
  AAPL         2020  2020-10-30  265,595,000,000  59,531,000,000 338,516,000,000 134,047,000,000  77,434,000,000        0.1586         0.2312       0.0541       0.1098
  AAPL         2021  2021-10-29  260,174,000,000  55,256,000,000 323,888,000,000 107,147,000,000  69,391,000,000       -0.0204        -0.0718       0.0384       0.0654
  AAPL         2022  2022-10-28  274,515,000,000  57,411,000,000 351,002,000,000  90,488,000,000  80,674,000,000        0.0551         0.0390      -0.0123       0.0167
  AAPL         2023  2023-11-03  365,817,000,000  94,680,000,000 352,755,000,000  65,339,000,000 104,038,000,000        0.3326         0.6492       0.0978       0.1858
  AAPL         2024  2024-11-01  394,328,000,000  99,803,000,000 352,583,000,000  63,090,000,000 122,151,000,000        0.0779         0.0541       0.1195       0.1985
  AAPL         2025  2025-10-31  383,285,000,000  96,995,000,000 364,980,000,000  50,672,000,000 110,543,000,000       -0.0280        -0.0281      -0.0026       0.0236
```

### Step 3 — `data/prices.parquet` (189,853 rows × 62 cols)

Showing AAPL annual filings (price/return columns):

```
ticker  fiscal_year  filed_date  entry_price    market_cap       benchmark  fwd_ret_1y  fwd_ret_3y  fwd_ret_5y  excess_1y  excess_3y  mom_12m  mom_6m  vol_12m  price_52w_high
  AAPL         2009  2010-01-25        6.08     5,400,122,573         MDY     0.6812      1.2381      3.1169     0.3917     0.6825   1.3874  0.3367   0.2684          0.9443
  AAPL         2010  2010-10-27        9.22     8,291,728,414         MDY     0.3147      0.7699      1.8034     0.1994     0.1511   0.4652  0.1036   0.2760          0.9680
  AAPL         2011  2011-10-26       11.99    10,984,431,572         SPY     0.5281      0.9311      1.2455     0.3659     0.2519   0.2279  0.0794   0.2803          0.9487
  AAPL         2012  2013-04-24       12.32    11,445,693,362         SPY     0.4357      0.9304      2.1488     0.2220     0.5267  -0.2176 -0.2884   0.3400          0.5835
  AAPL         2013  2013-10-30       16.16    15,174,367,664         SPY     0.4589      0.6085      2.1060     0.3059     0.3275  -0.1632  0.1135   0.2597          0.9868
  AAPL         2014  2014-10-27       23.16   145,770,287,159         SPY     0.1084      0.5832      1.5822     0.0339     0.1976   0.3457  0.1862   0.2017          0.9990
  AAPL         2015  2015-10-28       26.73   156,787,264,057         SPY    -0.0196      0.8776      3.1753    -0.0618     0.5375   0.0556 -0.1441   0.2611          0.9040
  AAPL         2016  2016-10-26       26.46   147,594,688,706         SPY     0.3858      1.2603      4.4846     0.1657     0.7568   0.0080  0.0959   0.2152          0.9775
  AAPL         2017  2017-11-03       40.18   214,400,454,431         SPY     0.1863      1.6259      2.5435     0.1092     1.2734   0.4472  0.0800   0.1778          1.0000
  AAPL         2018  2018-11-05       47.66   244,337,011,914         SPY     0.2953      2.0918      2.7133     0.1499     1.2913   0.2663  0.1826   0.2482          0.8687
  AAPL         2019  2019-10-31       59.73   284,015,456,713         SPY     0.7678      1.5202      2.8830     0.6705     1.1841   0.0676  0.1013   0.2469          0.9988
  AAPL         2020  2020-10-30      105.59 1,876,668,655,711         SPY     0.3772      0.5926      1.5470    -0.0515     0.2618   0.8995  0.5997   0.5267          0.8113
  AAPL         2021  2021-10-29      146.24 2,482,624,924,510         SPY     0.0294      0.5842        NaN      0.1757     0.2624   0.2471  0.0740   0.2353          0.9560
  AAPL         2022  2022-10-28      152.89 2,511,446,183,998         SPY     0.0999      0.7533        NaN      0.0148    -0.0822  -0.0767 -0.1415   0.3492          0.8727
  AAPL         2023  2023-11-03      174.44 2,781,218,048,454         SPY     0.2632        NaN         NaN     -0.0654       NaN    0.2954  0.0817   0.2002          0.9004
  AAPL         2024  2024-11-01      221.25 3,440,473,300,109         SPY     0.2126        NaN         NaN      0.0014       NaN    0.2880  0.3184   0.2459          0.9426
  AAPL         2025  2025-10-31      269.61 4,075,595,393,857         SPY       NaN         NaN         NaN        NaN        NaN    0.0908  0.1526   0.3635          0.9962
```

### Step 4 — `data/macro.parquet` (191,579 rows × 18 cols)

Showing AAPL annual filings (macro context at time of each filing):

```
ticker  filed_date  fiscal_year  treasury_10y  treasury_2y  yield_curve  fed_funds  credit_spread  cpi_yoy  recession    vix  real_rate_10y  credit_tight  macro_regime
  AAPL  2010-01-25         2009          3.66         0.86         2.80       0.11           2.56     2.62        0.0  25.41          1.04         -0.54             0
  AAPL  2010-10-27         2010          2.75         0.40         2.35       0.19           3.09     1.17        0.0  20.71          1.58          0.77             0
  AAPL  2011-10-26         2011          2.23         0.28         1.95       0.07           3.10     3.52        0.0  29.86         -1.29          0.44             0
  AAPL  2013-04-24         2012          1.73         0.23         1.50       0.15           2.80     1.14        0.0  13.61          0.59          0.10             0
  AAPL  2013-10-30         2013          2.55         0.33         2.22       0.09           2.68     0.88        0.0  13.65          1.67         -0.18             0
  AAPL  2014-10-27         2014          2.27         0.41         1.86       0.09           2.44     1.61        0.0  16.04          0.66          0.23             0
  AAPL  2015-10-28         2015          2.10         0.73         1.37       0.12           3.20     0.13        0.0  14.33          1.97          0.59             0
  AAPL  2016-10-26         2016          1.79         0.86         0.93       0.40           2.60     1.69        0.0  14.24          0.10         -0.25             0
  AAPL  2017-11-03         2017          2.34         1.63         0.71       1.16           1.91     2.17        0.0   9.14          0.17         -0.35             0
  AAPL  2018-11-05         2018          3.20         2.91         0.29       2.20           2.02     2.15        0.0  19.96          1.05          0.38             1
  AAPL  2019-10-31         2019          1.69         1.52         0.17       1.83           2.18     1.73        0.0  13.22         -0.04          0.04             0
  AAPL  2020-10-30         2020          0.88         0.14         0.74       0.09           2.61     1.23        0.0  38.02         -0.35         -0.62             0
  AAPL  2021-10-29         2021          1.55         0.48         1.07       0.08           1.69     6.24        0.0  16.26         -4.69         -0.27             0
  AAPL  2022-10-28         2022          4.02         4.41        -0.39       3.08           2.30     7.76        0.0  25.75         -3.74          0.23             2
  AAPL  2023-11-03         2023          4.57         4.83        -0.26       5.33           1.86     3.13        0.0  14.91          1.44         -0.49             2
  AAPL  2024-11-01         2024          4.37         4.21         0.16       4.64           1.46     2.72        0.0  21.88          1.65         -0.02             2
  AAPL  2025-10-31         2025          4.11         3.60         0.51       4.09           1.69     2.73        0.0  17.44          1.38         -0.27             2
```

`macro_regime`: 0=low rates, 1=rising rates, 2=high rates, 3=recession

### Step 5 — `data/historical_dataset.parquet` (191,579 rows × 321 cols)

Showing 18 well-known US stocks, fiscal year 2022, annual filings (key computed features):

```
ticker  revenue($B)  net_income($B)  total_assets($B)    roa    roe   pe_ratio  beneish_m  altman_z  piotroski  sloan_accruals  fwd_ret_1y  mom_12m  macro  size  value_comp  quality_comp
  AAPL       274.5          57.4           351.0       0.164  0.634     43.75     -2.692     7.582       6.0        -0.066       0.100   -0.077      2   3.0       0.529         0.771
   AMD         9.8           2.5            12.4       0.200  0.427     38.18     -1.695    10.894       7.0         0.114       1.260   -0.322      2   3.0       0.504         0.676
  AMZN       386.1          21.3           321.2       0.066  0.344     49.32     -2.917     4.216       5.0        -0.139       0.647   -0.293      2   3.0       0.628         0.831
    BA        58.2         -11.9           138.6      -0.086  0.792    -18.00     -2.090     1.030       3.0         0.047      -0.028    0.123      2   3.0       0.247         0.409
   BAC        85.5          17.9          3169.5       0.006  0.068     14.15     -2.280     0.196       4.0        -0.006       0.011   -0.193      2   3.0       0.845         0.707
   DIS        65.4          -2.9           203.6      -0.014 -0.032       NaN     -2.334     1.538       1.0          NaN      -0.023   -0.324      2   NaN         NaN         0.333
  INTC        77.9          20.9           168.4       0.124  0.219      5.29     -2.700     2.849       4.0        -0.089       0.593   -0.378      2   3.0       0.892         0.826
   JNJ        82.6          14.7           182.0       0.081    NaN       NaN     -2.516     0.558       5.0        -0.048       0.027    0.037      2   NaN         NaN         0.804
   JPM       120.0          29.1          3384.8       0.009  0.104     13.54     -1.969     0.227       2.0         0.032       0.333   -0.048      2   3.0       0.599         0.384
  META        86.0          29.1           166.0       0.176  0.288       NaN     -2.436     3.283       5.0        -0.058       1.516   -0.577      2   NaN         NaN         0.843
  MSFT       143.0          44.3           333.8       0.133  0.374     46.16     -2.548     7.484       8.0        -0.049       0.236   -0.055      1   3.0       0.506         0.841
  NFLX        25.0           2.8            44.6       0.062  0.364      5.87     -2.054     1.390       5.0         0.007       0.563   -0.139      2   3.0       0.867         0.702
  NVDA        10.9           2.8            28.8       0.097  0.299     23.38     -2.579     3.682       4.0        -0.068      -0.020    0.900      0   3.0       0.648         0.838
   PFE        41.7           9.2           181.5       0.050  0.119       NaN     -2.436     1.382       5.0        -0.029      -0.310   -0.022      2   NaN         NaN         0.786
  TSLA        31.5           0.7            62.1       0.012  0.024    744.77     -2.661    11.523       7.0        -0.084       0.081   -0.619      2   3.0       0.414         0.595
     V        21.8          10.9            82.9       0.131    NaN       NaN     -2.265     0.996       3.0         0.005       0.192   -0.050      2   NaN         NaN         0.791
   WMT       524.0          14.9           236.5       0.063  0.184       NaN     -2.274     3.281       6.0        -0.044      -0.016    0.061      0   NaN         NaN         0.716
   XOM       181.5         -22.4           332.8      -0.067 -0.133    -18.57     -2.491     2.787       3.0        -0.112      -0.011    0.543      2   3.0       0.466         0.271
```

### Step 6 — `data/historical_dataset_clean.parquet` (58,190 rows × 367 cols)

Same stocks after cleaning (adds data_confidence, filing_lag_days, fraud scores, alpha scores):

```
ticker  fiscal_year  filed_date  revenue($B)  net_income($B)    roa    roe  pe_ratio  beneish_m  altman_z  piotroski  fwd_ret_1y  confidence  lag_days  fraud_composite  alpha_composite
  AAPL         2022  2022-10-28       274.5          57.4       0.164  0.634    43.79     -2.692     7.227       6.0       0.100       0.947       -64            0.356            0.545
   AMD         2022  2023-02-27         9.8           2.5       0.200  0.427    38.18     -1.695    10.236       7.0       1.260       0.947        58            0.503            0.551
  AMZN         2022  2023-02-03       386.1          21.3       0.066  0.344    49.32     -2.917     3.945       5.0       0.647       0.930        34            0.388            0.625
    BA         2022  2023-01-27        58.2         -11.9      -0.086  0.792   -18.00     -2.090     1.182       3.0      -0.028       0.947        27            0.609            0.412
   BAC         2022  2023-02-22        85.5          17.9       0.006  0.068    14.23     -2.280     0.080       4.0       0.011       0.895        53            0.495            0.472
   DIS         2022  2022-11-29        65.4          -2.9      -0.014 -0.032      NaN     -2.334     0.930       1.0      -0.023       0.822       -32            0.765              NaN
  INTC         2022  2023-01-27        77.9          20.9       0.124  0.219     5.29     -2.700     2.056       4.0       0.593       0.947        27            0.350            0.597
   JNJ         2022  2023-02-16        82.6          14.7       0.081    NaN      NaN     -2.516     0.558       5.0       0.027       0.930        47            0.455              NaN
   JPM         2022  2023-02-21       120.0          29.1       0.009  0.104    13.54     -1.969     0.112       2.0       0.333       0.895        52            0.686            0.440
  META         2022  2023-02-02        86.0          29.1       0.176  0.288      NaN     -2.436     2.430       5.0       1.516       0.930        33            0.289              NaN
  MSFT         2022  2022-07-28       143.0          44.3       0.133  0.374    46.26     -2.548     7.000       8.0       0.236       0.947      -156            0.343            0.583
  NFLX         2022  2023-01-26        25.0           2.8       0.062  0.364     5.87     -2.054     1.151       5.0       0.563       0.930        26            0.567            0.653
  NVDA         2022  2022-03-18        10.9           2.8       0.097  0.299    23.41     -2.579     3.230       4.0      -0.020       0.947      -288            0.423            0.599
   PFE         2022  2023-02-23        41.7           9.2       0.050  0.119      NaN     -2.436     0.786       5.0      -0.310       0.912        54            0.513              NaN
  TSLA         2022  2023-01-31        31.5           0.7       0.012  0.024   744.77     -2.661    10.843       7.0       0.081       0.947        31            0.413            0.474
     V         2022  2022-11-16        21.8          10.9       0.131    NaN      NaN     -2.265     0.996       3.0       0.192       0.930       -45            0.483              NaN
   WMT         2022  2022-03-18       524.0          14.9       0.063  0.184      NaN     -2.274     2.802       6.0      -0.016       0.930      -288            0.352              NaN
   XOM         2022  2023-02-22       181.5         -22.4      -0.067 -0.133   -18.69     -2.491     2.088       3.0      -0.011       0.912        53            0.560            0.476
```

**Reading the data**: Boeing (BA) has highest fraud_composite (0.609) due to negative ROA + low Piotroski. META had +151% forward return from its 2022 low. Negative `filing_lag_days` means non-Dec fiscal year-end (e.g., AAPL ends Sep, files Oct → "filed before Dec 31").

---

## 5. Column Lineage

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
Step 5 later aliases these to shorter names (e.g., `revenue_growth`) via `pipeline/column_aliases.py`. Both survive in the final dataset.

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

**Column aliasing**: `pipeline/column_aliases.py` maps step2 `_yoy` names to shorter names. Both versions remain.

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

## 6. Risk Register

### Lookahead / Contamination Risks

| Risk | Severity | Status | Detail |
|---|---|---|---|
| YoY growth from near-zero base | Medium | **Active** (5,827 rows >1000%) | `_yoy(1, 0.001)` = 99,900%. Mitigated by step5 winsorization but could dominate tree splits in step2 output |
| `current_assets_growth` aliased from `asset_growth_yoy` | Low | **Resolved** | 44K mismatches — they're no longer identical (semantics fixed upstream) |
| Sector percentiles grouped by fiscal_year | OK | **Verified** | Correctly includes fiscal_year — no temporal leakage |
| Survivorship imputation at -50% | Medium | **Active** (214 rows) | Hardcoded. Some delistings are acquisitions at premium. Low impact at 214/58K rows |
| `filed_date` used as entry date | Low | **Active** (11K negative lag rows) | Correct PIT approach. Negative lag = non-Dec fiscal year-end companies. Negligible at annual rebalance |

### Fragile Assumptions

| Assumption | Location | Risk if broken | Verified Status |
|---|---|---|---|
| EDGAR returns HTTP 200 for all valid CIKs | step1, step2 | Silent data loss — checkpoint resumes skip failures | Active risk |
| yfinance rate limit at 1.5 req/s is sufficient | step3 | IP ban → empty price cache → NaN forward returns | Active risk |
| FRED API key present in .env | step4 | All macro columns are NaN (graceful) | **OK** — only 4.2% NaN (early dates) |
| `total_assets > 0` and `revenue > 0` for feature validity | step5 | sdiv returns NaN, propagates to composites | **6 rows** total_assets≤0, **3,900 rows** revenue≤0 (intentionally kept) |
| Fiscal year == calendar year for Q4 detection | step2 line 229 | Companies with non-Dec FY-end have Q4 incorrectly classified | Active risk |

### Duplicated Logic

| Duplication | Files | Severity |
|---|---|---|
| ~~`add_normalised_ratios()`~~ | ~~`pipeline/feature_library.py` AND `research/factor_research.py`~~ | **RESOLVED (Session 49)** — single source in `pipeline/feature_library.py` |
| Winsorization | step5 (line 890) AND step6 `winsorize_accruals()` | Low — step6 is market×year specific, step5 is global. Both needed but confusing |
| Size category logic | step5 `add_size_features()` AND step6 `_impute_size_category()` | Low — step6 fills gaps that step5 couldn't (missing market_cap) |

### Naming Inconsistencies

| Issue | Detail |
|---|---|
| `_yoy` suffix vs no suffix | step2 outputs `revenue_growth_yoy`; step5 aliases to `revenue_growth`. Both exist in final dataset |
| `equity` vs `total_equity` | step2 outputs `equity` from XBRL; step5 aliases to `total_equity`. Both exist |
| `shares_outstanding` vs `common_shares_outstanding` | Same — aliased in step5, both survive |
| `receivables` vs `accounts_receivable` | Same pattern |

> **Updated Session 35**: `EXCLUDE_COLS` / `EXCLUDE_PATTERNS` consolidated in `modeling/constants.py`.
> **Updated Session 49**: Column aliasing extracted to `pipeline/column_aliases.py`.

### Dead / Unused Code

| File | Status | Notes |
|---|---|---|
| `pipeline/build_monthly_price_cache.py` | Semi-active | Used by backtest engine, not by main pipeline |
| `pipeline/enrich_feature_dictionary.py` | Utility | Generates reports only, not in data path |
| `pipeline/p0f_universe_definition.py` | Active | Universe filter for research, not in pipeline run |
| Market variants (step1_*_br, _ca, _eu, _jp) | Active | Multi-market support, functional but less tested than US |
| ~~`pipeline/step1_fetch_tickers_jp_free.py`~~ | **Archived** | Moved to `pipeline/archive/` (session 48). Workflow reference fixed (session 49) |

---

## 7. Refactor Candidates

### Completed (Session 49)
- ~~`pipeline/step1_fetch_tickers_jp_free.py`~~ — **Archived** (session 48), workflow reference fixed
- ~~Column aliasing in step5~~ → **Extracted** to `pipeline/column_aliases.py`. step5 now calls `apply_column_aliases(df)`.
- ~~`add_normalised_ratios` duplication~~ → **Consolidated**. `research/factor_research.py` imports from `pipeline/feature_library.py`.

### Decided Against
- `enrich_fraud_taxonomy.py` / `enrich_fraud_labels.py` rename to `step7_*` — **Rejected**. These are NOT core pipeline steps. The `enrich_` prefix correctly signals they're optional post-pipeline enrichments.

### Should Stay As-Is
- `step1_fetch_tickers.py` — clear, single-purpose, well-documented
- `step2_build_snapshots.py` — complex but necessarily so (XBRL extraction)
- `step3_enrich_prices.py` — robust caching + rate limiting design
- `step4_enrich_macro.py` — clean vectorised implementation
- `step6_clean.py` — modular 5-phase pipeline, well-structured

### Could Be Renamed (low priority, decided against for now)
- `step2_build_snapshots.py` → `step2_fetch_financials.py` (it fetches from EDGAR, not just "builds")

### Should NOT Be Touched
- The multi-market variant files (`_kr`, `_br`, `_ca`, `_eu`, `_jp`) — functional, less tested. Risk of breakage > value of cleanup.
- Winsorization in step5 vs step6 — they serve different purposes despite looking duplicated.

---

## 8. Architecture Observations

### Dependency Direction
```
step1 → step2 → step3 → step4 → step5 → step6
  ↓        ↓        ↓        ↓        ↓        ↓
tickers  snapshots  prices   macro   dataset   clean_dataset
                                                     ↓
                                              modeling/train.py
                                              research/*.py
                                              backtest/engine.py
                                              alpha/factors/*.py
                                                     ↓ (optional)
                                              enrich_fraud_taxonomy.py
                                              enrich_fraud_labels.py
```

Steps 1-6 are **strictly linear** — no cycles, no backward references. Each step reads only from its predecessor's output. This IS the core pipeline.

The `enrich_*` scripts are an **optional enrichment layer** — they modify `historical_dataset_clean.parquet` in-place (add columns). They do NOT produce new files. They run via `workflows/run_dataset_enrichments.py` as a post-pipeline phase. The ML training works without them.

### Single Shared Entrypoint
- `_root.py` defines `ROOT = Path(__file__).resolve().parent`
- All scripts use `BASE = Path(__file__).parent.parent` or import from `_root`
- Inconsistency: step1–step5 use local `BASE = Path(...)`, only some enrichment scripts use `_root.ROOT`

### Data Volume
- ~8K US companies × ~15 years × annual + quarterly = ~191K rows in step2
- After step6 dedup + quality filters: **58,190 rows** in final dataset
- **367 columns** in `historical_dataset_clean.parquet`

---

## 9. Summary

The pipeline spine is **well-architected**: linear dependency chain, checkpointed for resilience, zero-API-call feature computation, proper PIT (point-in-time) handling via filed_date, and survivorship correction. The main complexity lives in step5 (170+ features from financial theory) which is unavoidable given the domain.

**Session 49 updates**: Column alias duplication resolved (extracted to `pipeline/column_aliases.py`), `add_normalised_ratios` consolidated to single source in `pipeline/feature_library.py`, JP workflow fixed. Decided against renaming `enrich_*` scripts to `step7_*` — they're optional enrichments, not core pipeline.

**Remaining risks**: hardcoded -50% survivorship return assumption (214 rows affected, low impact) and 3,900 rows with revenue ≤ 0 propagating NaN through valuation ratios (intentional — kept for model robustness).
