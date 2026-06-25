# Developer Setup

## Requirements

- Python 3.11+
- Git
- 8GB+ RAM recommended (training models on full US dataset)
- Optional: SIMFIN_API_KEY (for EU data), DART_API_KEY (for Korea), HF_TOKEN (HuggingFace)

## Clone and Install

```bash
git clone https://github.com/sherlock718/stock-fraud-screener.git
cd stock-fraud-screener

# Create virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install all dependencies
pip install -r requirements.txt

# Install mkdocs for documentation (optional)
pip install mkdocs-material
```

## Directory Structure

```
stock-fraud-screener/
├── app_v2.py                  # Streamlit app (archived — notebook is now primary frontend)
├── requirements.txt
├── mkdocs.yml
│
├── scripts/                   # Pipeline, training, analysis scripts
│   ├── run_pipeline.py        # Main US pipeline
│   ├── run_pipeline_eu.py     # EU pipeline (SimFin)
│   ├── run_pipeline_kr.py     # Korea pipeline (DART)
│   ├── feature_library.py     # 314 feature definitions (319 total incl. quarterly)
│   ├── train_models.py        # LightGBM training
│   ├── tune_models.py         # Optuna + CatBoost + calibration
│   ├── backtester.py          # Walk-forward backtester
│   ├── factor_research.py     # IC/ICIR/decay library
│   ├── leverage_strategy.py   # Long/short Kelly portfolio
│   ├── monitor_drift.py       # PSI + AUC drift monitoring
│   ├── generate_reports.py    # PDF tearsheet + CSV picks
│   └── bias_audit.py          # Look-ahead / survivorship audit
│
├── research/                  # Jupyter notebooks
│   ├── 01_data_exploration.ipynb
│   ├── 02_factor_analysis.ipynb
│   ├── 03_ml_model.ipynb
│   ├── 04_bias_audit.ipynb
│   └── 05_leverage_analysis.ipynb
│
├── data/                      # Generated data (gitignored)
│   ├── historical_dataset_clean.parquet
│   ├── backtest_results.json
│   └── refresh_status.json
│
├── models/                    # Trained model artifacts (gitignored)
│   ├── model_6m.joblib
│   ├── model_1y.joblib
│   ├── model_2y.joblib
│   ├── model_3y.joblib
│   ├── model_5y.joblib
│   └── model_meta.json
│
├── reports/                   # Generated reports (gitignored)
│   ├── tearsheet.pdf
│   ├── weekly_picks.csv
│   └── rolling_oos_auc.png
│
├── .github/workflows/
│   ├── refresh_data.yml       # Weekly data refresh (Sunday 05:00 UTC)
│   └── monitor_drift.yml      # Weekly drift check (Monday 07:00 UTC)
│
└── docs/                      # This documentation
```

## Environment Variables

| Variable | Required | Purpose |
|---|---|---|
| `HF_TOKEN` | Optional | HuggingFace write token (for dataset upload) |
| `HF_REPO` | Optional | HuggingFace repo ID (e.g. `username/stock-fraud-screener`) |
| `SIMFIN_API_KEY` | For EU pipeline | SimFin API key |
| `DART_API_KEY` | For Korea pipeline | DART (FSS) API key |
| `DATABASE_URL` | For DB-backed API | PostgreSQL DSN — see TimescaleDB section below |

Set in `.env` or export directly:

```bash
export HF_TOKEN=hf_xxx
export HF_REPO=sherlock718/stock-fraud-screener
```

---

## TimescaleDB — Local Setup and Migration

The API (`api/deps.py`) prefers TimescaleDB over the local parquet fallback when
`DATABASE_URL` is set and the `company_scores` table is non-empty.  The database
has **never been loaded** by default — you must run the steps below once.

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or any OCI-compatible runtime)
- Docker Compose v2 (`docker compose`) bundled with Docker Desktop

### 1 — Start the database container

```bash
# From the repo root — starts only the db service
docker compose up -d db

# Verify it is healthy (may take 10–20 s on first pull)
docker compose ps
docker compose logs db | tail -20
```

The `docker-compose.yml` maps `infra/db/init.sql` into the container's
`/docker-entrypoint-initdb.d/` directory, so the schema (including the
`snapshots`, `companies`, `fraud_labels`, and `watchlist` tables) is created
automatically on the first start.

Credentials and connection string:

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `screener` |
| User | `screener` |
| Password | `screener` |
| `DATABASE_URL` | `postgresql://screener:screener@localhost:5432/screener` |

### 2 — Run the migration (parquet → `company_scores`)

`scripts/data_io/migrate_to_db.py` loads `data/historical_dataset_clean.parquet`
(58,190 rows × 360 columns) into a `company_scores` table.  This table is
**separate** from the `snapshots` table defined in `init.sql` — it is a flat
denormalised table created by the migration script itself.

```bash
# Dry run first — prints schema, no writes
DATABASE_URL=postgresql://screener:screener@localhost:5432/screener \
    python3 scripts/data_io/migrate_to_db.py --dry-run

# Full migration (takes ~30–60 s on a laptop)
DATABASE_URL=postgresql://screener:screener@localhost:5432/screener \
    python3 scripts/data_io/migrate_to_db.py

# Verify
psql postgresql://screener:screener@localhost:5432/screener \
    -c 'SELECT COUNT(*) FROM company_scores;'
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--chunk-size N` | `1000` | Rows per INSERT batch |
| `--if-exists replace` | `replace` | `replace` drops and recreates; `append` adds rows; `fail` aborts |
| `--dry-run` | off | Print counts and schema only, do not write |

### 3 — Start the full stack

```bash
docker compose up -d          # starts db + api + app
```

The `api` service sets `DATABASE_URL` automatically via `docker-compose.yml`.

### Known schema notes

- `migrate_to_db.py` targets a `company_scores` table (flat denormalised).
  `infra/db/init.sql` defines a normalised `snapshots` + `companies` schema.
  These two tables coexist in the same database; `api/deps.py` reads from
  `company_scores` only.
- The parquet column `fraud_score_composite` maps to `fraud_score` in the
  screener API (`screener.py` checks both `fraud_score_composite` and
  `composite_score`). The parquet does **not** contain a `composite_score`
  column; the API falls back gracefully to `fraud_score_composite`.
- ML score columns (`ml_1y`, `ml_3y`, `ml_5y`, `ml_6m`, `ml_2y`) are produced by `scripts/modeling/train_models.py` and `scripts/modeling/score_historical.py`. The migration script will skip them silently; the API returns `null` for those fields until models are trained and scores are written back into the dataset.

### Stopping / resetting the DB

```bash
# Stop containers (data volume preserved)
docker compose stop db

# Full reset — destroys the pg_data volume and all loaded data
docker compose down -v
```

After a `down -v` you must re-run the migration (step 2 above).

## From-Scratch Build

To build everything from scratch:

```bash
# 1. Run US pipeline (30–60 min)
python3 scripts/workflows/run_pipeline.py --market US

# 2. Train base models
python3 scripts/modeling/train_models.py

# 3. (Optional) Tune with Optuna + CatBoost
python3 scripts/modeling/tune_models.py

# 4. Run backtest
python3 scripts/_shared/backtester.py

# 5. Generate reports
python3 scripts/analysis/generate_reports.py --top 25

# 6. Open the research notebook
jupyter notebook notebooks/08_experiment_hub.ipynb
```

## Development Branch

Active development happens on the `develop` branch. The `main` branch contains the latest stable release.

```bash
git checkout develop
git pull origin develop
```
