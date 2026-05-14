# Version 2 Roadmap

Current state: v1 complete — experiment notebook is the primary frontend, 3y/5y models meet AUC targets, COMPOSITE_US long-only backtest CAGR 20.98% (SPY +13.0%, excess +7.96%).

---

## Priority Order

Work through these in sequence. Earlier items unblock later ones.

---

### 1. Fix 6m / 1y / 2y AUC (highest ROI)

Current gaps: 6m 0.5715 (target 0.58), 1y 0.5774 (target 0.62), 2y 0.5880 (target 0.60).

**What to study**

- Short-horizon feature engineering: earnings revision velocity, analyst estimate dispersion, price-volume microstructure, 52-week high proximity
- LightGBM tuning with Optuna (increase to 200 trials; tune `min_child_samples`, `colsample_bytree`, `subsample_freq`)
- Regime-conditional models: encode market regime (rolling 12m return > 0 = bull) as a binary feature or train separate bull/bear sub-models
- Temporal CV pitfalls: avoid fold bleeding across the 2020 COVID fold; consider purging ±6 months around it

**What to do**

```bash
# Find which features have fastest IC decay at 6m vs 1y vs 3y
python3 scripts/ic_analysis.py --horizons 6m 1y 3y --output reports/ic_decay.csv

# Re-run Optuna with more trials on the failing horizons
python3 scripts/tune_models.py --horizons 6m 1y 2y --n-trials 200
```

**Target**: 6m ≥ 0.58, 1y ≥ 0.60, 2y ≥ 0.60

---

### 2. Add Quarterly Data

Current system: annual filings only — signals are 12–18 months stale when acted on.

**What to study**

- SimFin quarterly API (`simfin.load_income(variant='quarterly')`)
- SEC EDGAR XBRL quarterly parser (10-Q, not just 10-K)
- Point-in-time safety for quarterly data: `filed_date` from EDGAR, not fiscal quarter end

**What to do**

1. Extend `pipeline/step5_compute_features.py` to handle `period_type = 'Q'`
2. Build quarterly momentum features: sequential EPS surprise, revenue acceleration quarter-over-quarter
3. Add `period_type` filter in `scripts/backtester.py` so annual and quarterly runs stay separate

**Expected impact**: ~3–6 month reduction in signal staleness; most beneficial for 6m/1y horizons.

---

### 3. Portfolio Construction & Live Risk Management

Current system: equal-weight or inverse-vol; no live sizing, no rebalancing trigger.

**What to study**

- Kelly criterion with fractional sizing (you have `portfolio_backtest.json` using `kelly_fraction=0.25`)
- Mean-variance optimization with constraints: `cvxpy` or `scipy.optimize`
- Risk parity: equal risk contribution across positions
- Factor exposure neutralization: regress portfolio against Fama-French factors, hedge residual beta

**What to do**

1. Build `scripts/portfolio_optimizer.py`:
   - Input: screener output CSV with scores, market caps, sectors
   - Output: position sizes with max 20% per position, 35% per sector, net long constraint
2. Add rebalancing trigger: flag when any position score drops below 40th percentile

---

### 4. Live Data Pipeline

Current system: manual refresh; annual data only; pipeline can be months stale.

**What to study**

- `yfinance` daily price feed (free, ~15min delay)
- Alpaca Markets API (free tier, real-time US prices)
- SEC EDGAR full-text search API: `efts.sec.gov/hits.efts` for near-real-time 10-K/10-Q detection

**What to do**

1. Build `scripts/detect_new_filings.py` — poll SEC EDGAR every 6 hours for new 10-K filings
2. Trigger partial pipeline refresh (step3 → step5 → score_historical) on new filing detection
3. Add GitHub Actions workflow: `live_refresh.yml` on a 6-hour schedule (not just weekly)

---

### 5. Alternative Data

Current system: fundamental accounting data only; no insider, sentiment, or analyst signals.

**What to study**

- SEC Form 4 (insider trading): `data.sec.gov/submissions/{CIK}.json` has all insider filings; net insider buying is a strong 3–12m signal
- Earnings call NLP: OpenAI Whisper for transcription → sentiment/certainty scoring
- Analyst estimate revisions: free via web scraping Visible Alpha or Seeking Alpha consensus tables; paid via Refinitiv/Bloomberg

**What to do**

1. Build `scripts/fetch_insider_signals.py` — parse Form 4 filings; compute 3m net insider buy/sell ratio per company
2. Add `insider_buy_ratio_3m` as a feature candidate in `pipeline/feature_library.py`
3. Run feature selection on the enriched dataset to confirm IC > 0.02 before keeping

---

### 6. Production API

Current system: FastAPI skeleton built (`api/`) but not containerized or deployed.

**What to study**

- Docker multi-stage builds for Python ML services
- FastAPI async endpoints with background task queues (`BackgroundTasks`)
- Redis caching for expensive screener queries (60-minute TTL on scored results)
- API authentication: API key header (`X-API-Key`) via `fastapi.security`

**What to do**

1. Write `Dockerfile` for `api/` — multi-stage: builder installs dependencies, runtime copies artifacts
2. Add `/screen` (POST, filters + pagination) and `/company/{ticker}` (GET) endpoints
3. Add Redis cache layer for screener results
4. Add `docker-compose.yml` with `api`, `redis`, and optional `db` (TimescaleDB) services

---

## Version 2 Success Criteria

| Criterion | Target |
|---|---|
| 6m WF AUC | ≥ 0.58 |
| 1y WF AUC | ≥ 0.60 |
| 2y WF AUC | ≥ 0.60 |
| Signal staleness | ≤ 3 months (quarterly data live) |
| Live portfolio sizing | Kelly-fractioned, sector-capped, rebalancing trigger |
| Filing detection latency | ≤ 6 hours from SEC filing |
| API deployment | Docker-containerized, Redis-cached |

---

## Study Resources

| Topic | Resource |
|---|---|
| LightGBM tuning | LightGBM docs → Parameters Tuning; Optuna docs |
| Kelly sizing | Thorp, *The Kelly Criterion in Blackjack, Sports Betting, and the Stock Market* (2008) |
| Portfolio optimization | `cvxpy` docs → Portfolio Optimization examples |
| Factor model hedging | Barra USE4 handbook (free PDF); AQR's *Demystifying Managed Futures* |
| SEC EDGAR API | `efts.sec.gov` and `data.sec.gov` — no API key required |
| Earnings NLP | Loughran-McDonald sentiment dictionary; FinBERT (HuggingFace) |
| Docker for ML | "Production ML Services" chapter in *Designing Machine Learning Systems* (Chip Huyen) |
