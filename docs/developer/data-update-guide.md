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

    C --> F[merge_snapshots.py]
    D --> F
    E --> G[step5_compute_features.py]
    G --> F

    F --> H0[fix_dataset_quality.py\none-time quality fixes]
    H0 --> H1[enrich_quarterly_features.py\nintra-year dynamics]
    H1 --> H2[impute_features.py\nquarterly cols + size_category]
    H2 --> H3[mark_survivorship.py --fix\nsurvivorship correction]
    H3 --> H4[compute_alpha.py\n5-factor alpha scores]
    H4 --> H5[score_historical.py\nML alpha scores ml_{6m,1y,2y,3y,5y}]
    H5 --> H6[bias_audit.py --ci\nlook-ahead hard fail]
    H6 --> I[test_dataset_quality.py\n98 checks must pass]
    I -->|pass| I2[verify_doc_consistency.py\nnumbers consistent across docs]
    I2 --> J[push_to_hf.py\nupload to HuggingFace Hub]
    I -->|fail| K[Fix root cause\nre-run from affected step]
    J --> L[git commit + push\nGitHub → Streamlit Cloud auto-deploys]
```

> **Rule**: If a step is not in this diagram AND in `refresh_data.yml`, it will not run during weekly CI refresh.
> Both must be kept in sync — the diagram is descriptive, not aspirational.

---

## Before You Start

```bash
# Run quality checks on the current dataset first
python3 scripts/quality/test_dataset_quality.py --verbose
```

All 98 checks must pass before and after any dataset modification.

---

## 1. Adding New Annual Rows (Existing Markets)

New fiscal year data arrives once a year per company after their annual filing.

### US (SEC EDGAR)

```bash
# Full rebuild — fetches tickers, snapshots, prices, macro, features, clean
python3 scripts/workflows/run_pipeline.py --step 1
# Resume from a specific step (e.g. if step 1 already done)
python3 scripts/workflows/run_pipeline.py --step 3
```

### Korea (KR)

```bash
python3 scripts/workflows/run_pipeline_kr.py build --step 1
python3 pipeline/phase_a_integrate_kr.py
```

### Canada (CA)

```bash
python3 scripts/workflows/run_pipeline_ca.py build --step 1
python3 pipeline/phase_a_integrate_ca.py
```

### Japan (JP)

```bash
python3 scripts/workflows/run_pipeline_jp.py build --step 1
python3 pipeline/phase_a_integrate_jp.py
```

### Brazil (BR)

```bash
python3 scripts/workflows/run_pipeline_br.py build --step 1
python3 pipeline/phase_a_integrate_br.py
```

### After any market rebuild

Always re-run the enrichment stack and quality checks:

```bash
python3 pipeline/enrich_fraud_taxonomy.py    # refresh fraud scores
python3 scripts/quality/test_dataset_quality.py      # verify all 98 checks pass
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
- `period_type` must be `'annual'` for modeling/backtesting — downstream consumers filter to annual. The clean parquet retains quarterly rows for enrichment logic; `p0f_universe_definition.py` marks them as not investable.
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

Then re-integrate and re-clean each market, or use `scripts/workflows/refresh_data.py` for a full refresh.

> **Note:** The legacy `pipeline/auto_update.py` has been archived (Session 8). Use `scripts/workflows/refresh_data.py` + GitHub Actions for automated refreshes.

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
python3 scripts/quality/test_dataset_quality.py --verbose
```

Add a fill-rate threshold for the new column to `FILL_THRESHOLDS` in `scripts/quality/test_dataset_quality.py` if the column is core to downstream use.

---

## 4. Adding a New Market

1. Write `pipeline/step1_fetch_tickers_{mkt}.py` — outputs `data/tickers_{mkt}.csv`
2. Write `pipeline/step2_build_snapshots_{mkt}.py` — outputs `data/snapshots_{mkt}.parquet`
3. Write `scripts/run_pipeline_{mkt}.py` — orchestrates steps 1–6 with `--suffix _{mkt}`
4. Write `pipeline/phase_a_integrate_{mkt}.py` — runs steps 3–9 and concatenates into the clean parquet
5. Add market to `MIN_TICKERS` and `MIN_YEARS` in `scripts/quality/test_dataset_quality.py`
6. Document in `docs/developer/pipeline-scripts.md`

---

## 5. Monthly Update Schedule

### Automated (GitHub Actions)

| Workflow | Schedule | What it does |
|---|---|---|
| `refresh_data.yml` | Sunday 05:00 UTC | Refreshes all 6 markets (US CA JP KR EU BR), runs post-processing stack, pushes parquet + models to HuggingFace |
| `monitor_drift.yml` | Monday 07:00 UTC | Runs `scripts/quality/monitor_drift.py` — PSI + rolling AUC; uploads drift report as artifact |

### Per-market pipeline routing (weekly cron)

The cron default is `US CA JP KR EU BR`. Each market routes to its dedicated pipeline script:

| Market | Pipeline script | API key required? |
|---|---|---|
| US | `scripts/workflows/run_pipeline.py --market US` | None |
| CA | `scripts/workflows/run_pipeline.py --market CA` | None (SEDAR+) |
| JP | `scripts/workflows/run_pipeline.py --market JP` | None (TDNET) |
| KR | `scripts/workflows/run_pipeline_kr.py` | `DART_API_KEY` (GitHub secret) — skipped with warning if absent |
| EU (DE/FR/IT/ES/SE/NL/PT/DK/FI) | `scripts/workflows/run_pipeline_eu.py --market <mkt>` | None (SimFin free tier) |
| BR | `scripts/workflows/run_pipeline.py --market BR` | None (B3/CVM) |

**KR secret guard**: if `DART_API_KEY` is not set as a GitHub Actions secret, the KR step is skipped with a `[WARN]` message — the rest of the markets still run and the workflow does not fail.

To trigger a manual run for specific markets only: use `workflow_dispatch` with the `markets` input (e.g. `KR JP`).

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
python3 scripts/quality/test_dataset_quality.py --verbose

# 3. Refresh any markets with new filings available
# (run the relevant run_pipeline_*.py + phase_a_integrate_*.py)

# 4. Refresh fraud taxonomy scores
python3 pipeline/enrich_fraud_taxonomy.py

# 5. Run quality suite again
python3 scripts/quality/test_dataset_quality.py

# 6. Push to HuggingFace if dataset changed
python3 scripts/data_io/push_to_hf.py
```

---

## 6. Schema Constraints

The dataset schema is enforced by `scripts/quality/test_dataset_quality.py`. The following must always hold:

| Constraint | Rule |
|---|---|
| Modeling subset is annual | Downstream consumers filter `period_type == 'annual'`. Quarterly rows are retained in the parquet for enrichment use but marked `in_universe=0` by p0f. |
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
| After step6 clean | annual + quarterly (downstream filters to annual) | 326 |
| After quarterly enrichment + imputation + patch | 58,307 | 341 |
| After equity/vol patch + alpha scores + ML scores | 58,307 | 346 |
| After BR null-ticker drop + growth winsorization | 58,190 | 346 |
| After Montier C1-C6, montier_c_score, sloan_wc_accruals, sloan_lt_accruals | 58,190 | 355 |

Current local dataset (Phase B complete): **59,378 annual rows × 341 columns**. Phase C scoring overlay (26 additional columns) pending model retrain. HuggingFace production artifact not yet updated from local regeneration.

> **Montier C2 note (2026-05-13)**: `add_montier_c_score()` in `step5_compute_features.py` uses `ppe_net` for the C2 depreciation rate signal (19.4% null). Do not revert to `property_plant_equipment` (95.7% null) — that makes `montier_c2` 100% null.

---

## 9. Restoring Data from HuggingFace

Instead of rebuilding from scratch (Steps 1–6, several hours), you can restore pre-built artifacts from HuggingFace:

```bash
# Restore everything (final dataset + snapshots + manifest)
python3 scripts/data_io/pull_from_hf.py --all

# Restore only the final dataset
python3 scripts/data_io/pull_from_hf.py --final

# Restore snapshots (enables resuming from Step 4+ without Step 1-2 rebuild)
python3 scripts/data_io/pull_from_hf.py --snapshots
```

### When to pull vs rebuild

| Situation | Action |
|-----------|--------|
| Fresh checkout, need to run feature experiments | `pull_from_hf.py --all` |
| Only need final dataset for analysis/backtesting | `pull_from_hf.py --final` |
| Need to rerun Step 3+ (new price data needed) | `pull_from_hf.py --snapshots` then `run_pipeline.py build --step 3` |
| Need new tickers or filings from SEC/DART | Full rebuild: `run_pipeline.py build --step 1` |
| Suspect data corruption or want clean slate | Full rebuild |

### What gets stored on HuggingFace

| Artifact | Purpose | Required? |
|----------|---------|-----------|
| `historical_dataset_clean.parquet` | Final production dataset | Yes |
| `snapshots.parquet` | Combined multi-market snapshots (Step 3+ input) | Yes |
| `prices.parquet` | Price enrichment (saves ~45 min rebuild) | Optional |
| `snapshots_{market}.parquet` | Per-market snapshots | Optional |
| `ARTIFACT_MANIFEST.json` | Checksums + metadata for verification | Yes |

### What is NOT stored

- `price_cache.db` — disposable yfinance cache. Rebuild from scratch every time. The original PRICE-UNADJUSTED-001 bug was caused by a stale cache.
- `macro.parquet` — fast to rebuild from FRED (~1 min).
- `historical_dataset.parquet` — intermediate, fast to rebuild from snapshots + prices (~5 sec).

### Manifest verification

`pull_from_hf.py` automatically verifies sha256 checksums when a manifest is available. If a local file matches the manifest checksum, it skips the download. Use `--no-verify` to force re-download.
