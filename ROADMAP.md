# Roadmap — Renaissance Quant Lab

**Vision**: Industry-grade, academic-quality quantitative alpha lab. ML discovers factor importance dynamically. Hundreds of alpha signals, each backtested. Portfolio built from validated alphas.

**Architecture**: 15-layer monolith research pipeline + thin FastAPI + separate frontend.

---

## Phase 0 — Foundation
> Goal: Data pipeline solid, momentum gap fixed, ML scores in parquet, docs synced.
> Status: 🟡 In Progress (D8–D11 docs remaining, then code)

### Documentation (D-tasks)
- [x] D1 — CLAUDE.md reframe to multi-factor platform
- [x] D2 — docs/index.md reframe
- [x] D3 — docs/architecture.md reframe
- [x] D4 — docs/methodology/models.md feature selection clarification
- [x] D5 — docs/methodology/pipeline.md reframe
- [x] D6 — docs/methodology/feature-selection.md (new)
- [x] D7 — docs/methodology/features.md dual taxonomy rewrite
- [ ] D8 — docs/methodology/factor-library.md (feature groups as ML inputs, NOT score composite)
- [ ] D9 — docs/developer/contributing.md (vision checklist + sync rules)
- [ ] D10 — mkdocs.yml nav update + site_name fix
- [ ] D11 — git commit + push all docs

### Codebase Cleanup
- [ ] Delete `pipeline/enrich_auditor_going_concern.py` (superseded by enrich_governance.py)
- [ ] Delete `pipeline/score_and_report.py` (rules-based composite — contradicts ML-first vision)
- [ ] Delete `scripts/watchlist.py` (session state export, not quant lab material)
- [ ] Delete `scripts/high_roi_strategies.py` (redundant wrapper)

### Code (Phase 0 blockers)
- [ ] P0.1 — Add cross-sectional momentum (12m-1m) to `pipeline/feature_library.py`
- [ ] P0.2 — Fix SPY benchmark in `scripts/backtester.py` (currently equal-weight universe mean)
- [ ] P0.3 — Build `scripts/score_historical.py` (writes ml_1y/ml_3y/ml_5y to parquet) ← CRITICAL
- [ ] P0.4 — Newey-West HAC + Fama-MacBeth t-stats + FDR correction in `scripts/train_models.py`
- [ ] P0.5 — Verify 20-year data coverage per market, document gaps

**Phase 0 exit criteria**: momentum features > 0, ml_* columns in parquet, SPY benchmark, docs D1–D11 complete, WF AUC ≥ 0.62 on at least 2 of 3 horizons.

---

## Phase 1 — Alpha Signal Engine
> Goal: Generate hundreds of alpha signals. Each signal backtested. Alpha registry built.
> Status: ❌ Not started

### Layer 6 — Factor Research
- [ ] Notebooks: `notebooks/01_feature_ic_analysis.ipynb`, `notebooks/02_regime_analysis.ipynb`
- [ ] IC decay curves per feature (how long does each signal predict?)
- [ ] Regime-conditional analysis: does factor X work in bull/bear/neutral?
- [ ] Document hypotheses → `docs/research/factor_hypotheses.md`

### Layer 7 — Alpha Signal Generation
- [ ] Design alpha signal schema: `alpha/signals/base.py` (signal_id, horizon, market, segment, features_used, model_type)
- [ ] Build `alpha/signals/registry.py` (register, query, filter alpha library)
- [ ] Build `scripts/generate_alphas.py` — iteration loop: (market × horizon × segment × feature_subset × model_type) → train → register
- [ ] First run: US only, 1y horizon, all features → generate initial alpha library

### Layer 8 — Per-Alpha Backtesting
- [ ] Build `scripts/backtest_alpha.py` — walk-forward per signal
- [ ] Metrics per alpha: CAGR, Sharpe, max drawdown, IC, turnover, hit rate
- [ ] Transaction cost modeling (30bps default, 60bps small-cap)
- [ ] Benchmark per market (SPY for US, local index for others)
- [ ] Output: `reports/alpha_backtests/{signal_id}.json`

### Layer 9 — Alpha Selection
- [ ] Build `scripts/select_alphas.py` — filter (Sharpe > 0.5, drawdown < 30%, IC > 0.02)
- [ ] Deduplication: remove alphas with |r| > 0.85 information content (keep higher Sharpe)
- [ ] Ensemble: ML learns to combine surviving alphas
- [ ] Output: `data/alpha_registry.json` (all signals + backtest stats + selected flag)

**Phase 1 exit criteria**: alpha registry with ≥ 50 signals, ≥ 10 passing selection filter, end-to-end pipeline from data to selected alphas running on US.

---

## Phase 2 — Portfolio Construction & Risk
> Goal: Build investable portfolio from selected alphas. Risk controls. Long/short option.
> Status: ❌ Not started

### Layer 10 — Portfolio Construction
- [ ] Evolve `scripts/leverage_strategy.py` into `scripts/build_portfolio.py`
- [ ] Kelly criterion position sizing
- [ ] Risk-parity alternative
- [ ] Long-only and long/short variants
- [ ] Rebalancing: annual / semi-annual
- [ ] Notebooks: `notebooks/03_portfolio_construction.ipynb`

### Layer 11 — Risk Management
- [ ] VaR / CVaR calculation
- [ ] Drawdown circuit breakers (halt if > 20% drawdown)
- [ ] Factor exposure limits (no > 40% weight in single factor group)
- [ ] Liquidity constraints (min 30-day ADV for position size)
- [ ] Correlation limits between holdings

### Layer 12 — Investment Decision Framework
- [ ] Signal conviction scoring (high/medium/low based on alpha Sharpe + IC)
- [ ] Entry/exit rules (entry on confirmed signal, exit on signal reversal or stop-loss)
- [ ] Execution constraints (min market cap $100M, min ADV ratio)
- [ ] Compliance checklist (no insider trading flags, no going concern flags)
- [ ] Track record documentation template

**Phase 2 exit criteria**: full portfolio construction pipeline running, risk controls verified in backtest, investment decision framework documented.

---

## Phase 3 — Scale & Production
> Goal: All markets, production infrastructure, React frontend.
> Status: ❌ Not started

### Multi-Market Alpha Engine
- [ ] Integrate EU, KR, JP, CA, BR into alpha engine
- [ ] Market-specific feature adjustments (IFRS vs GAAP)
- [ ] Macro regime conditioning (classify regime → condition signal weights)
- [ ] Global alpha: signals that work across markets

### Layer 14 — Production Frontend
- [ ] React/Next.js quant lab UI (replaces Streamlit)
- [ ] Alpha signal browser (filter by market/horizon/Sharpe/factor)
- [ ] Portfolio builder UI
- [ ] Backtest visualizer
- [ ] Live screener with conviction scores

### Layer 15 — Production Infrastructure
- [ ] Docker containerization
- [ ] Cloud deployment (AWS or GCP)
- [ ] Production database (TimescaleDB or ClickHouse)
- [ ] Monitoring dashboard (Grafana or custom)
- [ ] CI/CD: auto-deploy on main push

**Phase 3 exit criteria**: all 6 markets in alpha engine, React frontend live, Docker deployed, monitoring active.

---

## Ongoing (All Phases)

| Cadence | Action |
|---|---|
| Every commit | CHANGELOG.md entry + docs in sync (check_sync.py) |
| Every session | Update CONTEXT.md with current state |
| Weekly | GitHub Actions: data refresh + drift monitor |
| Phase complete | Ask user for review + output walkthrough |
| Model retrain | HuggingFace push |

---

## File Inventory (Final State)

```
scripts/          ~30 files  (data, models, alpha engine, portfolio, monitoring)
pipeline/         ~30 files  (step1-6 per market, feature_library, enrichment)
alpha/            ~8 files   (engine, signals registry, base classes)
notebooks/        ~6 files   (research exploration)
docs/             ~20 files  (methodology, developer, architecture)
data/             parquet + alpha_registry.json
models/           joblib + model_meta.json
reports/          per-alpha backtest JSONs + tearsheets
infra/            DB schema, Docker
.github/          CI/CD workflows
CLAUDE.md         architecture state + pre-task checklist
CONTEXT.md        session state snapshot (updated every session)
ROADMAP.md        this file
CHANGELOG.md      commit history
mkdocs.yml        docs site config
```
