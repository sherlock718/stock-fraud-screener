# Monitoring

## Overview

Two GitHub Actions workflows run on schedule to keep the data fresh and the model healthy.

```
Every Sunday 05:00 UTC  →  refresh_data.yml   (data refresh)
Every Monday 07:00 UTC  →  monitor_drift.yml  (drift check)
```

## Drift Monitoring

### What Is Drift?

Model drift occurs when the distribution of input features in production differs significantly from the training distribution. This can happen because:

- Market regime changes (e.g., post-COVID balance sheet norms)
- Reporting standard changes (FASB, IFRS updates)
- Composition of the screened universe shifts (sector tilts, size changes)

### Population Stability Index (PSI)

PSI quantifies how much a feature's distribution has changed between two periods.

```
PSI = Σ (Actual% - Expected%) × ln(Actual% / Expected%)
```

| PSI Range | Interpretation | Action |
|---|---|---|
| < 0.10 | No significant change | None |
| 0.10 – 0.20 | Minor change | Monitor |
| ≥ 0.20 | Significant shift | Investigate and consider retraining |

The GitHub Actions workflow uses `--psi-alert 0.25` (slightly more lenient than the moderate threshold) so that only clear shifts trigger the warning.

### Rolling AUC

The drift monitor also checks whether the model's AUC on the most recent scoring window has degraded relative to the validation baseline.

| Horizon | Val AUC Baseline | Alert if drops below |
|---|---|---|
| 1-year | 0.776 | 0.726 (−5 pts) |
| 3-year | 0.795 | 0.745 (−5 pts) |
| 5-year | 0.860 | 0.810 (−5 pts) |

The 5-pt threshold is set via `--auc-alert 0.05` in the workflow.

## GitHub Actions Workflows

### `refresh_data.yml` — Weekly Data Refresh

**Schedule:** Every Sunday at 05:00 UTC

**Steps:**

1. Check out repo
2. Install Python dependencies
3. Download existing dataset from HuggingFace (incremental base)
4. Run `scripts/workflows/refresh_data.py --markets US`
5. Write `data/refresh_status.json` with timestamp
6. Run `generate_reports.py --top 25`
7. Upload updated dataset + status to HuggingFace
8. Upload pipeline logs and reports as artifacts (retained 90 days)

**Manual trigger:**

```bash
# Via GitHub UI: Actions tab → Weekly Data Refresh → Run workflow

# Via GitHub CLI:
gh workflow run refresh_data.yml -f markets="US"
gh workflow run refresh_data.yml -f markets="all"
```

**Failure behavior:** The workflow will fail if the pipeline returns a non-zero exit code. Check the Actions tab for the error log artifact.

---

### `monitor_drift.yml` — Weekly Drift Monitor

**Schedule:** Every Monday at 07:00 UTC (one day after data refresh)

**Steps:**

1. Download latest dataset and models from HuggingFace
2. Run `scripts/quality/monitor_drift.py --psi-alert 0.25 --auc-alert 0.05`
3. Upload `reports/drift_report.json` and `reports/drift_report.csv` as artifacts
4. If exit code 1 (drift detected): emit a GitHub Actions **warning** annotation

**Inspecting drift reports:**

1. Go to the Actions tab in GitHub
2. Click the most recent `Weekly Drift Monitor` run
3. Download the `drift-report` artifact
4. Open `drift_report.csv` — sorted by PSI descending

**Understanding the CSV output:**

| Column | Description |
|---|---|
| `feature` | Feature name |
| `psi` | Population Stability Index |
| `psi_alert` | `True` if PSI ≥ alert threshold |
| `mean_expected` | Mean value in training distribution |
| `mean_actual` | Mean value in recent window |
| `drift_direction` | `+` (increased) or `−` (decreased) |

## Required Secrets

Both workflows require these GitHub repository secrets:

| Secret | Where to set |
|---|---|
| `HF_TOKEN` | GitHub → Settings → Secrets → Actions |
| `HF_REPO` | GitHub → Settings → Secrets → Actions |

## Retraining Trigger

There is no automated retraining yet. When drift is detected:

1. Check `drift_report.csv` for which features are drifting
2. Decide if the drift represents genuine regime change or a data quality issue
3. If genuine: run `python3 scripts/modeling/train_models.py` on updated data, then `python3 scripts/modeling/tune_models.py`
4. Upload new models to HuggingFace: `python3 scripts/data_io/push_to_hf.py`

A roadmap item (Phase 1) will automate retraining when PSI or AUC thresholds are breached.

## Local Drift Check

Run the drift monitor locally against the current dataset:

```bash
python3 scripts/quality/monitor_drift.py

# More sensitive thresholds for exploratory analysis:
python3 scripts/quality/monitor_drift.py --psi-alert 0.10

# Compare against a specific historical window:
python3 scripts/quality/monitor_drift.py --window 2022

# View output:
cat reports/drift_report.json
cat reports/drift_report.csv
```

## Score Distribution Monitor

In addition to per-feature PSI, the drift monitor checks whether the distribution of predicted fraud probabilities has shifted. A sudden increase in the mean predicted score across all companies may indicate data quality issues rather than genuine fraud signal.

Track the `score_mean_drift` field in `drift_report.json`:

```json
{
  "score_mean_expected": 0.312,
  "score_mean_actual":   0.341,
  "score_mean_drift":    0.029
}
```

A drift > 0.05 in the mean score warrants manual inspection of the dataset before publishing results.
