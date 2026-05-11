# Deployment

## Streamlit Cloud

The app is deployed on Streamlit Cloud, loading data from HuggingFace Hub at startup.

### How it works

1. `app_v2.py` checks for `data/historical_dataset_clean.parquet` at startup
2. If not found locally, downloads from `HF_REPO` using `huggingface_hub`
3. Similarly downloads model files from `HF_REPO` (model repo)
4. Caches data in Streamlit's session cache to avoid re-downloading on every page navigation

### Streamlit Cloud Setup

1. Push the repo to GitHub
2. Connect to Streamlit Cloud at `share.streamlit.io`
3. Set the main file to `app_v2.py`
4. Add secrets in Streamlit Cloud settings:

```toml
# .streamlit/secrets.toml (local dev only, never commit)
HF_TOKEN = "hf_xxx"
HF_REPO = "sherlock718/stock-fraud-screener"
```

## GitHub Actions

Two automated workflows run on schedule.

### Weekly Data Refresh (`refresh_data.yml`)

**Schedule:** Every Sunday at 05:00 UTC

**What it does:**

1. Checks out the repo
2. Downloads existing dataset from HuggingFace as incremental base
3. Runs pipeline for specified markets (default: US)
4. Writes `data/refresh_status.json` with last refresh timestamp
5. Runs `generate_reports.py --top 25`
6. Uploads updated dataset + status to HuggingFace
7. Uploads pipeline logs and reports as GitHub Actions artifacts (retained 30–90 days)

**Manual trigger:**

```bash
# Trigger via GitHub UI (Actions tab → Weekly Data Refresh → Run workflow)
# Or via GitHub CLI:
gh workflow run refresh_data.yml -f markets="US"
gh workflow run refresh_data.yml -f markets="all"
```

### Weekly Drift Monitor (`monitor_drift.yml`)

**Schedule:** Every Monday at 07:00 UTC (day after refresh)

**What it does:**

1. Downloads latest dataset and models from HuggingFace
2. Runs `scripts/monitor_drift.py` — computes PSI and rolling AUC
3. Uploads drift report as artifact
4. If drift is detected (PSI > threshold), emits a GitHub Actions warning

**Drift alert threshold:** PSI > 0.25 for any feature triggers the warning.

## Required GitHub Secrets

| Secret | Value |
|---|---|
| `HF_TOKEN` | HuggingFace write token |
| `HF_REPO` | Dataset repo ID (e.g. `sherlock718/stock-fraud-screener`) |

Set at: `GitHub repo → Settings → Secrets and variables → Actions → New repository secret`

## HuggingFace Hub

The screener uses two HuggingFace repos:

| Repo type | Contents | Access |
|---|---|---|
| Dataset repo | `historical_dataset_clean.parquet`, `refresh_status.json` | Public read, token write |
| Model repo | `model_1y.joblib`, `model_3y.joblib`, `model_5y.joblib`, `model_meta.json` | Public read, token write |

Upload manually:

```python
from huggingface_hub import HfApi
api = HfApi(token="hf_xxx")

api.upload_file(
    path_or_fileobj='data/historical_dataset_clean.parquet',
    path_in_repo='historical_dataset_clean.parquet',
    repo_id='sherlock718/stock-fraud-screener',
    repo_type='dataset',
)
```

## Local Documentation Server

```bash
pip install mkdocs-material
mkdocs serve
# Open http://localhost:8000
```

Build static site:

```bash
mkdocs build
# Output: site/ directory
```
