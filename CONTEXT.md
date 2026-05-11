# CONTEXT — Session State Snapshot

> Update this file at the start and end of every session.
> Purpose: give any new session full project context in under 60 seconds.

---

## What This Project Is

**Renaissance-style quantitative alpha lab.** NOT a fraud screener. NOT a fixed-weight factor composite.

ML discovers which features matter per market and regime. Generates hundreds of alpha signals. Each alpha is independently backtested. Portfolio construction selects and weights validated alphas.

See `ROADMAP.md` for phase plan. See `CLAUDE.md` for architecture state and pre-task checklist.

---

## Current Focus

**Phase A — Foundation** (🟡 In Progress — doing this properly before any Phase B/C work)

Phase B (Research & Signals) and Phase C (Portfolio & Production) are **parked** — see bottom of this file for their saved state. Do not start them until Phase A exit criteria are all met.

---

## Verified Data State (as of 2026-05-11)

### historical_dataset_clean.parquet — 155,696 rows, 319 cols

| Market | Rows | Tickers | Years | Quarterly? |
|---|---|---|---|---|
| US | 143,519 | 4,452 | 2008–2027 | ✅ 104,063 quarterly rows |
| KR | 3,396 | 106 | 2015–2026 | ✅ 2,469 quarterly rows |
| BR | 2,834 | 55 | 2010–2025 | ✅ 2,094 quarterly rows |
| CA | 4,048 | 1,378 | 2021–2026 | ❌ annual only |
| EU (DE/FR/IT/etc.) | ~1,500 | ~300 | 2021–2025 | ❌ annual only |
| JP | 498 | 122 | 2021–2026 | ❌ annual only |

**Known data issues:**
- KR: snapshot had 251 tickers, only 106 made it into clean dataset — 145 lost in pipeline (investigate)
- CA/EU/JP: no quarterly data — feature enrichment limited
- Refresh is **weekly** (Sunday 5am, `cron: '0 5 * * 0'`), not monthly
- ML score cols (ml_1y, ml_3y, ml_5y): **ABSENT** — `score_historical.py` not yet built

### Momentum features — already exist (P0.1 is NOT a blocker)

| Column | Null Rate (annual) |
|---|---|
| momentum_12m_prior | 3.4% |
| momentum_6m_prior | 1.6% |
| momentum_3m_prior | 0.7% |
| momentum_consistency | 0.0% |
| value_x_momentum | 32.2% |

Cross-sectional rank (Jegadeesh & Titman 12m-1m) still needs verification in feature_library.py.

---

## Phase A — Remaining Tasks

### Step 0 — Git & Repo Cleanup
- [ ] Audit orphaned scripts (no doc entry)
- [ ] Branch strategy: main (stable) / dev (integration) / feature/* (work)
- [ ] .gitignore audit (parquet, joblib, .env, site/, __pycache__)
- [ ] Remove stale notebooks and draft scripts

### Step 1 — Data Ingestion (verify completeness)
- [ ] Confirm KR ticker loss (251 → 106) — investigate pipeline drop
- [ ] Confirm quarterly coverage is sufficient for enrichment (US/KR/BR only)
- [ ] CA/EU/JP annual-only — document limitation explicitly

### Step 2 — Data Quality & Bias
- [ ] P0.5 — Data coverage verification (20-year depth check, especially US)
- [ ] Re-run `check_data.py` and `pit_validate.py` on current parquet

### Step 3 — Data Refresh
- [ ] Confirm weekly GitHub Actions job is still live and passing
- [ ] Verify HuggingFace push is working post-refresh

### Step 4 — Feature Engineering
- [ ] Verify cross-sectional momentum implementation in `pipeline/feature_library.py`
  (momentum cols exist but confirm they are rank-normalised cross-sectionally, not raw returns)
- [ ] value_x_momentum at 32.2% null — investigate why

---

## Current Model Performance

| Horizon | WF AUC | Target | Status |
|---|---|---|---|
| 1y | 0.553 | ≥ 0.62 | ❌ |
| 3y | 0.643 | ≥ 0.62 | ✅ |
| 5y | 0.597 | ≥ 0.62 | ❌ |

**Root cause of weak 1y/5y**: no ML scores in parquet (backtester blind), SPY benchmark using equal-weight universe mean.

---

## Critical Phase A Blockers

| ID | Task | File | Why |
|---|---|---|---|
| P0.3 | Build `score_historical.py` | `scripts/` | Backtester has zero ML signal without this |
| P0.2 | Fix SPY benchmark | `scripts/backtester.py` | All CAGR/excess return numbers misleading |
| P0.4 | Newey-West HAC + FDR | `scripts/train_models.py` | Feature selection not statistically sound |
| P0.5 | Coverage verification | audit script | Can't trust ICIR rankings without confirming depth |

---

## Key Files

| File | Purpose |
|---|---|
| `CLAUDE.md` | Architecture state, pre-task checklist, sync rules |
| `ROADMAP.md` | Phase plan with status rows |
| `CONTEXT.md` | This file |
| `pipeline/feature_library.py` | All 319 feature formulas |
| `scripts/train_models.py` | ML training: PSI → IC → ICIR → Spearman dedup |
| `scripts/backtester.py` | Walk-forward backtester (SPY benchmark broken) |
| `data/historical_dataset_clean.parquet` | Main dataset (319 cols, 155K rows, 14 markets) |
| `models/model_meta.json` | Selected features per horizon + training stats |

---

## Parked — Phase B & C (resume after Phase A exit criteria met)

### Phase B — Research & Signals (not started)
Steps 5–11: Feature Selection improvements (HAC/FDR), Factor Research notebooks (IC decay, regime analysis), Model baselines + ablation, Alpha signal generation + registry, Backtest per-alpha, Portfolio Construction, Final Alpha Selection.

**Saved blockers for Phase B:**
- Feature selection: HAC standard errors + Benjamini-Hochberg FDR not yet implemented
- Alpha registry schema not designed
- Portfolio construction: Kelly sizing, risk-parity, sector limits — nothing built

### Phase C — Portfolio & Production (not started)
Steps 12–16: Leverage strategy integration with alpha registry, full reporting tearsheets, per-alpha monitoring, React/Next.js frontend, Docker + cloud deployment.

**Saved state:** Streamlit + FastAPI already built. HuggingFace hosting live. Docker schema exists but not deployed.

---

## Session Log

| Date | Work Done |
|---|---|
| 2026-05-11 | Vision realignment; D1–D7 docs; ROADMAP.md; CONTEXT.md; vision memory saved |
| 2026-05-11 | Roadmap restructured to 16-step backbone; Phase B/C parked; full data audit run |
