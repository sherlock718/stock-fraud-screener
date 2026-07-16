# Stock Screener

This repository is being simplified into one workable product: an explainable
stock screener that turns available company data into a ranked shortlist.
Fraud risk is one input alongside value, quality, momentum, and growth; it is
not the whole product.

## Current status

The repository contains substantial pipeline, modeling, research, and
validation work, but the end-to-end product path has not yet been reverified.
Historical performance numbers are not presented as current product claims.

Start with [docs/START_HERE.md](docs/START_HERE.md). The active work plan is
[docs/CODEX_ROADMAP.md](docs/CODEX_ROADMAP.md).

## Intended product flow

```text
current company data
    -> data and eligibility checks
    -> factor and risk features
    -> ranking and portfolio constraints
    -> explainable shortlist
```

The next task is a read-only product readiness check. It will identify the
smallest existing path that can become the single supported product entrypoint
before code, data, or model work resumes.

## Repository map

- `pipeline/` — data preparation and features
- `modeling/` — training and scoring
- `alpha/` — value, quality, momentum, growth, and fraud-risk factors
- `portfolio/` — selection and portfolio construction
- `quality/` — data and temporal-integrity checks
- `backtest/` and `research/` — research support, not the active product surface
- `notebooks/` — experiments and the existing screener notebook candidate
- `docs/archive/` — retired plans and historical product claims

## Safety boundary

Do not treat archived backtests as verified current performance. Do not refresh
data, retrain models, or add another product path until the readiness check has
selected one canonical route.
