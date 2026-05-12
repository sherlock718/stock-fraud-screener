# Data Update Guide

How to add new rows, tickers, or features to `data/historical_dataset_clean.parquet`, and how the monthly refresh schedule works.

**Always read this file before modifying the dataset.**

---

## Operator Workflow

```mermaid
flowchart TD
    A[New data available\nfiling, ticker, or feature] --> B{What type?}

    B -->|New annual rows| C[run_pipeline_{mkt}.py\nbuild --step 1]
    B -->|New ticker| D[Add to tickers_{mkt}.parquet\nrun from step 2]
    B -->|New feature column| E[Add formula to\nfeature_library.py\nAND step3 or step5]

    C --> F[phase_a_integrate_{mkt}.py\nor merge_snapshots.py]
    D --> F
    E --> G[step5_compute_features.py]
    G --> F

    F --> H1[impute_features.py\nquarterly cols + size_category]
    H1 --> H2[mark_survivorship.py --fix\nsurvivorship correction]
    H2 --> H3[compute_alpha.py\n5-factor alpha scores]
    H3 --> H4[score_historical.py\nML fraud scores ml_1y/3y/5y]
    H4 --> H5[enrich_fraud_taxonomy.py\nrefresh fraud scores]
    H5 --> I[test_dataset_quality.py\n53 checks must pass]
    I -->|pass| J[push_to_hf.py\nupload to HuggingFace Hub]
    I -->|fail| K[Fix root cause\nre-run from affected step]
    J --> L[git commit + push\nGitHub → Streamlit Cloud auto-deploys]
```

> **Rule**: If a step is not in this diagram, it will not run during weekly CI refresh.
> Every post-processing script must appear here AND in `refresh_data.yml`.

---

## Before You Start

```bash
# Run quality checks on the current dataset first
python3 scripts/test_dataset_quality.py --verbose
```

All 53 checks must pass before and after any dataset modification.

---

## 1. Adding New Annual Rows (Existing Markets)

New fiscal year data arrives once a year per company after their annual filing.

### US (SEC EDGAR)

```bash
# Full rebuild — fetches tickers, snapshots, prices, macro, features, clean
python3 scripts/run_pipeline.py --step 1
# Resume from a specific step (e.g. if step 1 already done)
python3 scripts/run_pipeline.py --step 3
```

### Korea (KR)

```bash
python3 scripts/run_pipeline_kr.py build --step 1
python3 pipeline/phase_a_integrate_kr.py
```

### Canada (CA)

```bash
python3 scripts/run_pipeline_ca.py build --step 1
python3 pipeline/phase_a_integrate_ca.py
```

### Japan (JP)

```bash
python3 scripts/run_pipeline_jp.py build --step 1
python3 pipeline/phase_a_integrate_jp.py
```

### Brazil (BR)

```bash
python3 scripts/run_pipeline_br.py build --step 1
python3 pipeline/phase_a_integrate_br.py
```

### After any market rebuild

Always re-run the enrichment stack and quality checks:

```bash
python3 pipeline/enrich_fraud_taxonomy.py    # refresh fraud scores
python3 scripts/test_dataset_quality.py      # verify all 53 checks pass
```

---

## 2. Adding New Tickers

Tickers are discovered automatically by the step 1 fetcher for each market. To add a specific ticker manually:

1. Add a row to `data/tickers_{market}.csv` with columns: `ticker, cik, company_name, exchange, sic_code, country`
2. Re-run from step 2 for that market: `python3 scripts/run_pipeline_{market}.py build --step 2`
3. Re-integrate: `python3 pipeline/phase_a_integrate_{market}.py`
4. Re-run quality checks

**Important constraints:**
- `ticker` must not be blank — BR CVM companies without a B3 match are excluded by design
- `period_type` must be `'annual'` — no quarterly rows in the clean dataset
- `fiscal_year` must be 2008–2030

---

## 3. Adding a New Feature Column

Adding a column to the dataset is a three-file change: formula, pipeline, docs.

### Step 1 — Add the formula

Add the computation to `pipeline/feature_library.py`. Every formula must be:

- **Point-in-time safe**: uses only data available at `filed_date`
- **No look-ahead**: no future prices, future earnings, or future labels
- **Handles NaN gracefully**: use `.fillna()` only where economically justified; otherwise let NaN propagate

### Step 2 — Wire it into the pipeline

Add the column to `pipeline/step5_compute_features.py` in the appropriate `add_*` function, or create a standalone enrichment script in `pipeline/enrich_*.py` if the column requires external data (e.g. insider signals, governance flags).

### Step 3 — Re-run compute features for all markets

```bash
# US
python3 pipeline/step5_compute_features.py

# Other markets (example: KR)
python3 pipeline/step5_compute_features.py --snapshots data/snapshots_kr.parquet \
    --prices data/prices_kr.parquet --macro data/macro_kr.parquet --suffix _kr
```

Then re-integrate and re-clean each market, or use `auto_update.py` for a full refresh.

### Step 4 — Update documentation (required by CLAUDE.md)

| If you... | Update |
|---|---|
| Add a feature to `feature_library.py` | `docs/methodology/features.md` — add to the correct category table |
| Change total column count | `docs/architecture.md` diagram nodes + `docs/index.md` tagline + `docs/methodology/models.md` flowchart + `CLAUDE.md` architecture table |
| Add a factor-score column | `docs/methodology/factor-library.md` |
| Change CLI flags | `docs/developer/scripts.md` or `docs/developer/pipeline-scripts.md` |
| Any of the above | `CHANGELOG.md` — add entry under `[Unreleased]` |

### Step 5 — Verify

```bash
python3 scripts/test_dataset_quality.py --verbose
```

Add a fill-rate threshold for the new column to `FILL_THRESHOLDS` in `scripts/test_dataset_quality.py` if the column is core to downstream use.

---

## 4. Adding a New Market

1. Write `pipeline/step1_fetch_tickers_{mkt}.py` — outputs `data/tickers_{mkt}.csv`
2. Write `pipeline/step2_build_snapshots_{mkt}.py` — outputs `data/snapshots_{mkt}.parquet`
3. Write `scripts/run_pipeline_{mkt}.py` — orchestrates steps 1–6 with `--suffix _{mkt}`
4. Write `pipeline/phase_a_integrate_{mkt}.py` — runs steps 3–9 and concatenates into the clean parquet
5. Add market to `MIN_TICKERS` and `MIN_YEARS` in `scripts/test_dataset_quality.py`
6. Document in `docs/developer/pipeline-scripts.md`

---

## 5. Monthly Update Schedule

### Automated (GitHub Actions)

| Workflow | Schedule | What it does |
|---|---|---|
| `refresh_data.yml` | Sunday 05:00 UTC | Runs incremental refresh via `pipeline/auto_update.py`, pushes updated parquet + models to HuggingFace |
| `monitor_drift.yml` | Monday 07:00 UTC | Runs `scripts/monitor_drift.py` — PSI + rolling AUC; uploads drift report as artifact |

### Manual monthly checklist

Run this once a month to catch any data gaps:

```bash
# 1. Check what fiscal years are available per market
python3 -c "
import pandas as pd
df = pd.read_parquet('data/historical_dataset_clean.parquet')
print(df.groupby('market')['fiscal_year'].agg(['min','max','count']))
"

# 2. Run full quality suite
python3 scripts/test_dataset_quality.py --verbose

# 3. Refresh any markets with new filings available
# (run the relevant run_pipeline_*.py + phase_a_integrate_*.py)

# 4. Refresh fraud taxonomy scores
python3 pipeline/enrich_fraud_taxonomy.py

# 5. Run quality suite again
python3 scripts/test_dataset_quality.py

# 6. Push to HuggingFace if dataset changed
python3 scripts/push_to_hf.py
```

---

## 6. Schema Constraints

The dataset schema is enforced by `scripts/test_dataset_quality.py`. The following must always hold:

| Constraint | Rule |
|---|---|
| Annual-only | `period_type == 'annual'` for all rows |
| No blank tickers | `ticker != ''` and `ticker.notna()` |
| No duplicate primary keys | `(cik, market, fiscal_year, period_type)` is unique |
| No inf values | All numeric columns |
| Fraud scores in range | `fraud_score_*` ∈ [0, 1] |
| Piotroski in range | `piotroski_f_score` ∈ [0, 9] |
| No fraud label leakage | `abs(corr(fraud_score_*, fraud_confirmed))` < 0.80 |
| `fraud_suspect` zeroed | `fraud_confirmed=1` rows must have `fraud_suspect=0` |

---

## 7. Primary Key Note (KR / DART)

Korean companies sometimes file multiple fiscal years in a single batch submission with the same `filed_date`. This means `(cik, market, filed_date, period_type)` is **not** a valid primary key for KR data. Always use `(cik, market, fiscal_year, period_type)` as the primary key.

---

## 8. Column Count Reference

| Dataset state | Rows | Columns |
|---|---|---|
| After step5 (`historical_dataset.parquet`) | varies | ~320 |
| After step6 clean | annual-only | 326 |
| After quarterly enrichment + imputation + patch | 58,307 | 341 |
| After equity/vol patch + alpha scores + ML scores | 58,307 | 346 |
| After BR null-ticker drop + growth winsorization | 58,190 | 346 |

Current production dataset: **58,190 rows × 346 columns**
