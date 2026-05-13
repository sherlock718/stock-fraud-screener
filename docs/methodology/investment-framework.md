# Investment Framework

Numbered rules governing portfolio construction, position sizing, risk limits, and operational procedures. These rules are enforced programmatically where noted.

---

## 1. Universe

**Rule 1 — Liquidity floor.** Every stock must have a market cap at filing date ≥ $50M (USD or USD-equivalent). Stocks below this threshold are excluded before scoring.

**Rule 2 — Market scope.** The default universe spans all covered markets (US SEC, KR DART). Universe can be narrowed to a single market via `--market US|KR`. No market is given preference in scoring.

**Rule 3 — Fiscal-year anchoring.** Each stock is represented by its most recent annual filing in each fiscal year. Only one row per ticker per fiscal year enters the screener. Intra-year data enriches features but does not create duplicate candidates.

---

## 2. Scoring

**Rule 4 — IC-weighted composite score.** The composite alpha score is the IC-weighted average of percentile-ranked signals registered in `data/alpha_registry.json`. Only signals with `selected: true` are used. Weights are proportional to `ic_mean`; the weight vector sums to 1.

**Rule 5 — Horizon filtering.** ML OOF signals are tagged by training horizon (1y / 3y / 5y). When `--horizon` is not `all`, only ML signals matching that horizon are included. Factor signals (Value, Quality, Momentum, Growth, Fraud Risk) are horizon-agnostic and always included.

**Rule 6 — Percentile-rank normalisation.** Each signal is ranked within the cross-section of the current fiscal year before weighting. This makes the composite score robust to outliers and scale differences between signals.

---

## 3. Portfolio construction

**Rule 7 — Long-only default.** The primary strategy is `long_only`: buy the top-N stocks by composite score. The `long_short` strategy is available for research but is not the primary operational mode.

**Rule 8 — Top-N selection.** Default portfolio size is 30 stocks per year (`--top-n 30`). A minimum of `top_n` valid rows (after filters) must be available; years with insufficient coverage are skipped in the backtest.

**Rule 9 — Fractional Kelly sizing.** Position weights are derived from fractional Kelly: `f = kelly_fraction × (2p − 1).clip(0)` where `p` is the composite score. Default `kelly_fraction = 0.25` (quarter-Kelly). If all scores fall below 0.5 the portfolio reverts to equal weight.

**Rule 10 — Position cap.** No single position may exceed 5% of portfolio value (`--position-cap 0.05`). Applied before sector cap.

**Rule 11 — Sector cap.** No single SIC sector may exceed 40% of portfolio value (`--sector-cap 0.40`). Applied after position cap; both caps are enforced iteratively and the result is renormalised.

---

## 4. Transaction costs

**Rule 12 — Tiered slippage.** Estimated round-trip transaction costs are applied in the backtester by market-cap tier:

| Market cap | Slippage (round-trip) |
|---|---|
| > $10B (large-cap) | 20 bps |
| $1B – $10B (mid-cap) | 30 bps |
| $100M – $1B (small-cap) | 50 bps |
| < $100M (micro-cap) | 80 bps |

If `market_cap_at_filing` is unavailable, the fallback is: micro/small = `--smallcap-cost`, otherwise = `--cost`.

**Rule 13 — Annual rebalance assumption.** The backtest assumes one full portfolio turnover per year. Partial-year holds and intra-year trading are not modelled.

---

## 5. Risk limits

**Rule 14 — VaR 95% monitoring.** The backtester and portfolio builder both compute the 5th percentile of the annual return distribution as VaR 95% (`var_95_pct`). This is a warning metric; no hard stop is imposed by default.

**Rule 15 — CVaR 99% gate.** The portfolio builder supports a hard abort via `--cvar-gate <threshold_pct>`. If the CVaR 99% (Expected Shortfall — mean of annual returns in the worst 1% tail) is worse than the threshold, `build_portfolio.py` exits without writing holdings. Recommended gate: −40%.

**Rule 16 — Max drawdown monitoring.** Maximum drawdown over the backtest period is computed from the cumulative return series and reported as `max_drawdown_pct`. It is a monitoring metric; no automatic halt is imposed at this layer (see Rule 19 for the live circuit breaker).

**Rule 17 — Beta and alpha reporting.** When ≥ 3 years of SPY overlap exist, CAPM beta and Jensen's alpha (annualised) are computed and reported. These are diagnostic; no constraint is placed on beta in the current framework.

---

## 6. Benchmark

**Rule 18 — SPY as primary benchmark.** Annual SPY total returns in `data/spy_returns.csv` are the reference benchmark for excess CAGR and CAPM calculations. The benchmark is calendar-year aligned; fiscal-year portfolios are compared to the corresponding calendar year's SPY return.

---

## 7. Live monitoring

**Rule 19 — Drawdown circuit breaker.** Once live trading begins, portfolio drawdown is monitored continuously. If realised drawdown from the most recent peak exceeds 20%, position sizing is halved and no new positions are opened until drawdown recovers below 10%. Implemented in `scripts/monitor_drift.py`.

**Rule 20 — IC decay tracking.** Rolling IC is computed over 3m, 6m, and 12m windows for every registered alpha signal. If a signal's 3m rolling IC falls below 0.02 it is flagged for review. If it falls below 0 for two consecutive months it is removed from `alpha_registry.json` pending re-evaluation.

**Rule 21 — Model drift check.** PSI and rolling AUC are computed weekly by `scripts/monitor_drift.py`. If PSI > 0.25 on any feature or rolling AUC drops more than 0.05 below the walk-forward baseline, an alert is raised and a model retrain is scheduled.

---

## 8. Data integrity

**Rule 22 — Point-in-time safety.** All features are constructed using only information available at the filing date. No look-ahead leakage is permitted. Forward returns are target variables only — never input features.

**Rule 23 — Survivorship correction.** Delisted stocks receive an imputed forward return of −50% per year to correct for survivorship bias. This is applied by `scripts/mark_survivorship.py` before any backtest.

**Rule 24 — Minimum history.** A backtest requires at least 3 annual observations to compute Sharpe and at least 3 years of SPY overlap to compute beta and alpha. Metrics requiring more data are reported as `null` rather than suppressed silently.

---

## 9. Operational procedures

**Rule 25 — Weekly refresh cadence.** The full pipeline (`refresh_data.yml`) runs every Sunday. Steps: fetch new filings → compute features → score → regenerate alpha registry → rebuild portfolio. Outputs are pushed to HuggingFace Hub.

**Rule 26 — Registry versioning.** `data/alpha_registry.json` is the single source of truth for signal selection and weights. Any change to selected signals or IC weights must be reflected in this file and the corresponding CHANGELOG entry must record the `ic_mean` values before and after.

**Rule 27 — Commit convention.** All changes follow the conventional commit format defined in `CLAUDE.md`. Phase D changes use scope tags: `risk`, `portfolio`, `alpha`, `monitor`, `docs`.
