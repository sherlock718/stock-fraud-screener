# Orientation: Research + Backtest Packages — Session 29

Generated: 2026-06-27

---

## 1. Backtest Engine Internals

### File: `backtest/engine.py` (1023 lines)

Single-file walk-forward backtester. No class hierarchy — one `run_backtest()` function orchestrates everything.

#### Walk-Forward Mechanics

```
For each fiscal_year Y (2008–2023):
  1. Filter universe → filings for year Y
  2. Apply look-ahead protection (filing_lag_filter: filed_date <= FY-end + 18 months)
  3. Apply liquidity filters (market_cap >= $50M, ADTV >= $1M)
  4. Strategy filter selects top-N stocks by composite score
  5. Compute net returns = forward_return_1y − tiered_slippage
  6. Weight positions (inverse-vol, capped at 20% per stock, 35% per sector)
  7. Record annual portfolio return vs SPY benchmark
```

**Holding period**: Stocks selected from fiscal_year=Y filings are held for **calendar year Y+1** (Jan–Dec). This is the "filing year → next calendar year" assumption.

#### Rebalance Frequency

**Annual** — once per year at the start of the holding period. No intra-year rebalancing. The monthly NAV reconstruction (`compute_monthly_nav`) is for drawdown measurement only, not for trading.

#### Return Calculation

- `port_ret = dot(weights, net_rets)` where `net_rets = forward_return_1y − per_pick_cost`
- CAGR = `wealth[-1]^(1/n) − 1`
- Sharpe = `(CAGR − 0.03) / annual_vol` (risk-free = 3%)
- Sortino: downside_vol = `sqrt(mean(min(ret, 0)²))`
- Calmar: `CAGR / effective_dd` (floors drawdown at 2σ if < 2%)

#### Position Sizing

1. **Inverse-volatility weighting**: `weight_i = (1/vol_i) / Σ(1/vol_i)`, vol clipped [0.05, 3.0]
2. **Per-stock cap**: MAX_POSITION_WEIGHT = 0.20 (20% max per name)
3. **Sector cap**: MAX_SECTOR_WEIGHT = 0.35 (35% max per SIC sector, iterative scaling)
4. Fallback: equal-weight if vol data unavailable

#### Transaction Cost Model

Tiered by market cap at filing (SLIPPAGE_TIERS):

| Market Cap | Slippage (bps) |
|---|---|
| > $10B (large) | 20 |
| $1B–$10B (mid) | 30 |
| $100M–$1B (small) | 50 |
| < $100M (micro) | 80 |

Legacy flat cost: DEFAULT_COST_BPS = 30, SMALLCAP_COST_BPS = 60.

#### ML Scoring: `load_and_score()`

Walk-forward ML inside the backtest:
- For each score_year Y: train LightGBM on all data with fiscal_year < Y (expanding window)
- Feature selection: top-35 by |ICIR| on training data (`_ic_rank`)
- Imputation: expanding-window training median (no look-ahead)
- PIT: only filings with filed_date < January 1st of score year
- Produces `ml_{1y,3y,5y}_wf` columns
- Falls back to static pre-trained models (`models/model_{1y,3y,5y}.joblib`)

#### Strategy Filters (4 strategies)

| Strategy | Key Logic | Weight Allocation |
|---|---|---|
| `composite` | Blend: value 25%, quality 20%, ML_1y 30%, ML_3y 15%, piotroski 10%. Beneish < −1.78 gate. | Default |
| `qem` | Piotroski ≥ 7, EPS growth > 0, momentum > −10%. Blend: EPS 20%, quality 25%, ML_1y 25%, momentum 15%, value 15% | Default |
| `scdv` | Micro/small cap only, P/B < 2.0, Piotroski ≥ 6, Altman-Z > 1.81. Blend: value 35%, quality 25%, ML_3y 25%, piotroski 15%. D/E penalty. | Default |
| `iarb` | Non-US only, P/B < 1.5, Piotroski ≥ 6. Blend: value 30%, quality 25%, ML_3y 25%, momentum 20%. Country boost (KR +5%, BR +3%, CA +2%). | Default |

---

## 2. Research Pipeline Map

### Script Dependency Graph

```
research/ic_engine.py                    ← Core IC stats (Newey-West, BH FDR, compute_yearly_ic)
    ↑
research/factor_research.py              ← IC/ICIR analysis, turnover, quintile spread, IC decay
    ↑
research/feature_selection_engine.py     ← Re-exports from modeling/run_feature_selection.py
    ↑
research/proper_split_backtest.py        ← Session 22: train/val/test split, feature selection
    ↑
research/pruned_backtest.py              ← Session 23: 27 stable features, add-back logic
    ↑
research/explainable_tree.py             ← Session 24: Decision tree + agreement filter
    ↑
research/regime_overlay.py               ← Session 25: SPY drawdown macro signal
```

**Lateral dependencies**:
- All research scripts import from `backtest/engine.py` (filter_composite, slippage tiers, sector caps)
- All research scripts import from `modeling/train.py` (load_data, deduplicate_features, compute_psi)
- `research/ablation.py` imports full engine (load_full_hist, load_and_score, run_backtest)

### Execution Order (research pipeline)

1. `factor_research.py` — IC/ICIR analysis for all candidate features
2. `modeling/run_feature_selection.py` — PSI → IC → ICIR → dedup pipeline
3. `proper_split_backtest.py` — confined feature selection on train period + walk-forward test
4. `pruned_backtest.py` — stability test (shifted window), lean model
5. `explainable_tree.py` — decision tree + agreement filter threshold sweep
6. `regime_overlay.py` — macro overlay evaluation
7. `ablation.py` — drop-one-feature impact study
8. `generate_reports.py` — PDF tearsheet, CSV picks, OOS AUC chart

### Shared State / Artifacts

| Artifact | Producer | Consumers |
|---|---|---|
| `data/historical_dataset_clean.parquet` | pipeline | all research + backtest |
| `models/feature_sets_3y.json` | run_feature_selection.py | ablation.py |
| `models/feature_sets_pruned.json` | pruned_backtest.py | explainable_tree.py, regime_overlay.py |
| `models/decision_tree_rules.json` | explainable_tree.py | (downstream screener) |
| `reports/proper_split_results.json` | proper_split_backtest.py | pruned_backtest.py |
| `data/spy_returns.csv` | data_io/fetch_spy_returns | backtest/engine.py, all research |
| `data/monthly_prices.parquet` | pipeline/build_monthly_price_cache | backtest/engine.py |
| `models/model_{1y,3y,5y}.joblib` | modeling/train.py | backtest/engine.py (static fallback) |

---

## 3. Signal Validation Chain

```
IC Analysis                Feature Selection         Backtest                Post-Processing
────────────────────       ──────────────────       ────────────────        ──────────────────
factor_research.py    →    run_feature_selection    →  proper_split_        → agreement filter
                           (PSI+IC+ICIR+dedup)         backtest.py             (tree_prob ≥ 0.35)
  ↓                          ↓                         ↓                       ↓
Outputs:                   60 candidates →           Walk-forward ML         LightGBM ranks +
- IC per year per feat       dedup to ~43              on test period          tree gates
- ICIR = mean/std            features                  (2019-2024)              ↓
- Newey-West t-stat           ↓                         ↓                   regime_overlay.py
- Quintile spread          pruned_backtest.py        Sharpe/CAGR/DD           (SPY DD >15% →
- IC decay half-life         27 stable features      per strategy              50% cash)
                             (both train windows)
```

### Validation Layers

| Layer | What it tests | Key threshold |
|---|---|---|
| IC screen | Predictive power exists | \|mean_IC\| ≥ 0.02 |
| ICIR rank | Signal-to-noise is high enough | Top 60 by \|ICIR\| |
| PSI filter | Distribution stability train→val | PSI < 0.25 |
| Spearman dedup | No redundant features | \|r\| > 0.85 → drop |
| Temporal stability | Features survive shifted window | 50% Jaccard overlap |
| Walk-forward ML | No look-ahead in scoring | Expanding window, PIT |
| Backtest gate | Real-world signal after costs | Sharpe ≥ 0.8 (session 22) |
| Agreement filter | Both models agree on picks | tree_prob ≥ 0.35 |
| Regime overlay | Macro crash protection | SPY DD > 15% → risk-off |

---

## 4. Risk Register

### Backtest Bias Vectors

| # | Risk | Severity | Current Mitigation | Residual |
|---|---|---|---|---|
| 1 | **Survivorship bias** | HIGH | `survivorship_pct` reported; `fill_missing_return=-0.5` option | Default is DROP missing → optimistic. No delisted-company imputation by default |
| 2 | **Annual rebalance look-ahead** | MED | PIT filter (filed_date < Jan 1 of score year), 18-month filing lag filter | Assumes all filings available Jan 1 of holding year; actual filing dates vary (Mar-Jun typical for 10-K) |
| 3 | **Forward return availability** | MED | Uses `forward_return_1y` column | Column computed from price data that may itself have survivorship issues (delisted tickers drop out) |
| 4 | **Benchmark choice** | MED | SPY primary, universe mean fallback | SPY is US-only; non-US strategies (iarb) benchmark against SPY which is wrong; should use ACWI |
| 5 | **Single-country bias in IC** | LOW | Sector-neutral IC (demean within SIC sector) | Cross-country sector composition differences not addressed |
| 6 | **Transaction cost model** | LOW | Tiered slippage by market cap | No time-varying costs (spread compression 2020→2024), no market-impact function (assumes fixed bps regardless of position size) |
| 7 | **Annual frequency drawdown** | LOW | Monthly NAV when cache available | Monthly cache is optional; without it MaxDD is annual-only (massive understatement) |

### Implicit Assumptions

| Assumption | Where | Risk if Wrong |
|---|---|---|
| Holding period = exactly 12 months from Jan 1 | `run_backtest()` line 90–93 in monthly NAV | Real rebalance timing varies; early-year filings could be traded sooner |
| Market cap at filing ≈ market cap at entry | Slippage tier assignment | Cap can change dramatically between filing and trade date (6+ months) |
| vol_prior_12m is available and meaningful | Position sizing | Falls back to equal-weight; mixed regimes within portfolio |
| Single annual data point per company | All scripts | Ignores quarterly updates, events between annuals |
| Beneish M-score < −1.78 is universal fraud gate | composite, qem, scdv, iarb strategies | Gate may reject legitimate small-cap companies with unusual accruals |
| RISK_FREE = 3% (constant) | Sharpe computation | Distorts Sharpe in zero-rate era (2009-2022); should be time-varying |

### Fragile Paths

1. **`_ic_rank()` inside `load_and_score()`** — duplicates IC logic from `research/ic_engine.py` with slightly different parameters (top_n=35 vs engine's flexibility). Divergence risk.
2. **`EXCLUDE_COLS` / `EXCLUDE_PATTERNS`** — defined separately in `backtest/engine.py`, `research/factor_research.py`, and `modeling/train.py`. Must stay in sync manually.
3. **`_sic_to_sector()`** — duplicated verbatim in `backtest/engine.py` and `research/ic_engine.py`. Any change must be made in both.
4. **Walk-forward ML code** — copy-pasted across `proper_split_backtest.py`, `pruned_backtest.py`, `explainable_tree.py` (3 copies of the same training loop with minor variations).
5. **`filter_composite()` imports** — research scripts import strategy filters from `backtest/engine.py`, creating tight coupling between research and execution.

### Hardcoded Thresholds

| Threshold | Value | Location | Risk |
|---|---|---|---|
| RISK_FREE | 0.03 | engine.py:45 | Should be time-varying |
| MIN_MARKET_CAP | $50M | engine.py:46 | Arbitrary; may miss good micro-caps or include illiquid names |
| MAX_POSITION_WEIGHT | 0.20 | engine.py:55 | Conservative for concentrated strategies |
| MAX_SECTOR_WEIGHT | 0.35 | engine.py:56 | Allows heavy sector bets |
| MAX_FILING_LAG_MONTHS | 18 | engine.py:476 | Conservative; could miss valid late filers |
| DRAWDOWN_THRESHOLD | 0.15 | regime_overlay.py:24 | Arbitrary; not optimized |
| CASH_FRACTION | 0.50 | regime_overlay.py:25 | Arbitrary; not optimized |
| tree_threshold | 0.35 | explainable_tree.py (selected via sweep) | Only 5 values tested (0.30–0.50) |
| PSI_THRESHOLD | 0.25 | run_feature_selection.py:42 | Industry standard but may be loose |
| IC_MIN_ABS | 0.02 | run_feature_selection.py:43 | Low bar; many weak features pass |
| TOP_K_ICIR | 60 | run_feature_selection.py:44 | Large pre-dedup pool |
| CORR_THRESHOLD | 0.85 | run_feature_selection.py:45 | Allows correlated pairs through |
| TREE_MAX_DEPTH | 4 | explainable_tree.py:41 | Not tuned; deeper tree may capture more |
| min_train_years | 5 | engine.py:266 | Minimum history before WF scoring begins |
| Beneish gate | -1.78 | filter_composite/qem/scdv/iarb | Textbook threshold; not validated on this dataset |

---

## 5. Refactor Candidates (DO NOT EXECUTE)

### Priority 1: Extract shared walk-forward training loop

**Problem**: The WF training loop (expand window → IC rank → train LGB → score) is copy-pasted 4 times: `engine.py:load_and_score()`, `proper_split_backtest.py:walk_forward_backtest()`, `explainable_tree.py:walk_forward_tree_backtest()`, `explainable_tree.py:walk_forward_agreement_backtest()`.

**Fix**: Extract a single `wf_score(df, features, model_factory, year_range)` function in `backtest/` or `modeling/` that accepts a model factory callable.

### Priority 2: Consolidate EXCLUDE sets

**Problem**: `EXCLUDE_COLS`/`EXCLUDE_PATTERNS` defined in 3 places with slightly different contents.

**Fix**: Single canonical definition in `pipeline/feature_library.py` or `modeling/constants.py`, imported everywhere.

### Priority 3: Deduplicate `_sic_to_sector()`

**Problem**: Identical function in `backtest/engine.py:498` and `research/ic_engine.py:48`.

**Fix**: Single definition in a shared `utils/` or `pipeline/` module.

### Priority 4: Strategy filters → separate module

**Problem**: `filter_composite`, `filter_qem`, `filter_scdv`, `filter_iarb` are in `backtest/engine.py` (execution) but imported by research scripts. Mixes concerns.

**Fix**: Move to `portfolio/strategies.py`, import into both backtest and research.

### Priority 5: Time-varying risk-free rate

**Problem**: `RISK_FREE = 0.03` constant used for all Sharpe calculations regardless of era.

**Fix**: Load from `data/risk_free_rates.csv` (Treasury 1y), use matched rate per year.

### Priority 6: Benchmark alignment for non-US

**Problem**: iarb strategy benchmarks against SPY but selects non-US stocks.

**Fix**: Add MSCI ACWI or market-specific index as benchmark for non-US strategies.

---

## 6. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SIGNAL VALIDATION PIPELINE                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  [1] FACTOR RESEARCH              [2] FEATURE SELECTION                 │
│  ┌────────────────────┐           ┌────────────────────┐               │
│  │ ic_engine.py       │           │ run_feature_        │               │
│  │ factor_research.py │──────────▶│ selection.py        │               │
│  │  - IC/ICIR/t-stat  │           │  - PSI filter       │               │
│  │  - quintile spread │           │  - IC screen        │               │
│  │  - IC decay        │           │  - ICIR rank (top60)│               │
│  │  - turnover        │           │  - Spearman dedup   │               │
│  └────────────────────┘           └────────┬───────────┘               │
│                                            │                            │
│  [3] PROPER SPLIT                          ▼                            │
│  ┌────────────────────────────────────────────────────────┐            │
│  │ proper_split_backtest.py                               │            │
│  │  - Feature select on TRAIN only (2008-2014)            │            │
│  │  - Stability check: shifted window (2010-2016)         │            │
│  │  - Walk-forward ML on TEST (2019-2024)                 │            │
│  │  - Gate: Sharpe ≥ 0.8 → PASS                          │            │
│  └────────────────────────────┬───────────────────────────┘            │
│                               │                                         │
│  [4] PRUNING                  ▼                                         │
│  ┌────────────────────────────────────────────┐                        │
│  │ pruned_backtest.py                         │                        │
│  │  - 27 stable features (both windows)       │                        │
│  │  - Sharpe 1.124 > full 43-feat 0.954      │                        │
│  │  - Add-back threshold: Sharpe drop > 0.1   │                        │
│  └────────────────────────────┬───────────────┘                        │
│                               │                                         │
│  [5] AGREEMENT FILTER         ▼                                         │
│  ┌────────────────────────────────────────────┐                        │
│  │ explainable_tree.py                        │                        │
│  │  - LightGBM (ranking) + Tree (gating)      │                        │
│  │  - Threshold sweep: t=0.35 selected        │                        │
│  │  - Sharpe 1.138, CAGR +34%                 │                        │
│  └────────────────────────────┬───────────────┘                        │
│                               │                                         │
│  [6] REGIME OVERLAY           ▼                                         │
│  ┌────────────────────────────────────────────┐                        │
│  │ regime_overlay.py                          │                        │
│  │  - SPY DD > 15% = risk-off (50% cash)      │                        │
│  │  - Dormant in 2019-2024 test period        │                        │
│  │  - Insurance for 2008-style crashes         │                        │
│  └────────────────────────────────────────────┘                        │
│                                                                         │
│  [7] EXECUTION ENGINE                                                   │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ backtest/engine.py                                             │    │
│  │  - Walk-forward ML scoring (expanding window)                  │    │
│  │  - 4 strategies: composite, qem, scdv, iarb                   │    │
│  │  - Tiered slippage, sector caps, inv-vol weighting            │    │
│  │  - Monthly NAV reconstruction, bootstrap CI                    │    │
│  │  - Tearsheet output                                            │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Key Metrics Summary (from sessions 22-25)

| Stage | Sharpe | CAGR | Notes |
|---|---|---|---|
| Full 43-feature (biased) | 1.37 | — | Look-ahead in feature selection |
| Proper split (unbiased) | 0.954 | +31.9% | Gate PASS |
| Pruned 27 features | 1.124 | +33.8% | Simpler = better |
| Agreement filter (t=0.35) | 1.138 | +34.0% | Final production model |
| + Regime overlay | ~1.1 | +31.7% | Insurance only, dormant in test |
