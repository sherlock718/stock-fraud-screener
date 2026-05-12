# Pipeline Integrity Rules

Rules that prevent the class of mistakes made during Phase A/B development.
Every rule has a root cause it prevents — so you can judge edge cases instead of blindly following it.

---

## Rule 1 — Every feature lives in the pipeline, not in a patch script

**Rule**: If a column exists in `historical_dataset_clean.parquet`, it must be produced by a
step in the main pipeline (`step3_enrich_prices.py` for price-derived, `step5_compute_features.py`
for fundamental-derived). Patch/one-off scripts are allowed for backfilling existing data but
must then be deleted or their logic merged into the relevant step.

**Why**: Patch scripts create orphan columns — present today, silently dropped on the next
weekly CI rebuild. This is how `vol_prior_6m/36m/60m` and `roa_volatility_5yr` ended up
existing only in the parquet but not regenerating. A weekly refresh would have silently lost them.

**How to apply**: Before closing any task that adds a new column:
1. Confirm the column is computed in `step3_enrich_prices.py` or `step5_compute_features.py`
2. Confirm a fresh run of the pipeline would produce that column
3. If it was added via a patch script, either merge the logic in or flag it explicitly in CHANGELOG

---

## Rule 2 — Cross-sectional features must always include a time dimension

**Rule**: Any `groupby` that computes a rank or percentile across companies must include
`fiscal_year` (or equivalent time key) in the group keys.

```python
# WRONG — ranks 2005 companies against 2005-2024 peers
df.groupby('sic_2digit')[feat].transform(rank)

# CORRECT — ranks 2005 companies only against 2005 peers
df.groupby(['sic_2digit', 'fiscal_year'], observed=True)[feat].transform(rank)
```

**Why**: Without a time key, the rank for a 2005 observation uses knowledge of what
the sector looks like in 2006-2024. This is temporal lookahead in feature space.
It inflates IC/ICIR during training (the feature appears predictive when it partly
encodes future sector evolution). This was the `*_sector_pct` bug: 18 features were
wrong across all 3 model horizons.

**How to apply**: Every time you add or modify a `groupby(...).transform()` that computes
a rank, percentile, z-score, or deviation: verify `fiscal_year` is in the group keys.
Search for `groupby` in `step5_compute_features.py` before each phase closure.

---

## Rule 3 — The CI workflow is the source of truth for the dataset

**Rule**: If a script is not in `refresh_data.yml`, it will not run during the weekly
rebuild. The CI-produced parquet must match the manually-patched parquet column for column.
Any gap = a bug.

**Why**: `refresh_data.yml` was missing `impute_features.py`, `mark_survivorship.py`,
`compute_alpha.py`, and `score_historical.py`. Had CI run, the produced dataset would
have been ~326 columns instead of 346, missing all alpha factor scores, ML scores,
survivorship correction, and quarterly features.

**How to apply**: After adding any new post-processing script, immediately add it to
`refresh_data.yml` in the correct order before closing the task. The diagram in
`docs/developer/data-update-guide.md` and the CI yaml must always be in sync.

---

## Rule 4 — Re-run artifacts after any data fix

**Rule**: When you fix a bug that changes feature values, you must re-run all downstream
artifacts that depend on those features before closing the phase. Stale artifacts silently
report wrong numbers.

| What changed | Must re-run |
|---|---|
| Feature values corrected | `run_feature_selection.py`, `factor_research.py` |
| Feature selection output changed | All 4 research notebooks |
| New columns added | `test_dataset_quality.py` (check 53 pass), notebook 04 |
| Model inputs changed | `train_models.py` + `score_historical.py` (Phase C only) |

**Why**: After fixing the equity coalesce bug, `roe` went from 4% → 88% fill.
Feature selection had been run on the broken data — the selected feature sets didn't
include equity-derived features because they all had near-zero IC. The IC analysis
notebook showed wrong fill rates. These were caught only during a manual audit.

**How to apply**: Treat any dataset change as triggering the "Phase Closure Checklist" (below).

---

## Rule 5 — No feature engineering outside `feature_library.py` / step3 / step5

**Rule**: Formulas for features belong in one of three canonical places:
- `pipeline/feature_library.py` — fundamental ratio formulas
- `pipeline/step3_enrich_prices.py` — price-derived features
- `pipeline/step5_compute_features.py` — post-join features (sector ranks, rolling, interactions)

Alpha factor scores (`alpha_*.py`) and ML scores are the only exceptions.

**Why**: Features scattered across patch scripts, alpha modules, and one-off scripts become
impossible to audit. When something breaks, you can't tell which script produced the column.
The feature registry `docs/methodology/feature-registry.md` and the pipeline must stay in sync.

---

## Phase Closure Checklist

**Do not use a task list or memory to declare a phase done.**

Run every check in `docs/developer/phase-done-criteria.md` for the relevant phase.
All checks must pass. If any fail, fix only the failing item and re-run that check.

The criteria file contains exact shell commands that return PASS/FAIL — no interpretation needed.

---

## Rule 6 — All growth YoY features must be winsorized at 1st/99th percentile

**Rule**: Every `*_growth_yoy`, `*_growth`, `*_dilution`, and `*_change_yoy` column must
appear in the `ratio_cols` winsorize list in `step5_compute_features.py`. No growth feature
may enter the pipeline unwinsorized.

```python
# In step5_compute_features.py, confirm your new column is in ratio_cols:
ratio_cols = [
    ...
    'your_new_growth_yoy',   # ← add it here
]
```

**Why**: Near-zero denominator companies (e.g. revenue near $0 rebounding) produce growth
multiples of 184,343× or higher. These outliers dominate ICIR scores, causing the feature
selector to rank an extreme-outlier column as the most predictive feature. `revenue_growth_yoy`
had a max of 184,343 before winsorization was added. A single unwinsorized growth feature
can inflate IC, skew feature selection, and break the gradient boosted model with extreme
splits.

**How to apply**: Any time you add a `*_growth`, `*_yoy`, `*_dilution`, or `*_change`
column to `feature_library.py` or `step5_compute_features.py`, immediately add it to
`ratio_cols` in the winsorize block of `step5_compute_features.py`. Run
`test_dataset_quality.py` to verify the check in Section 8 still passes.

---

## Rule 7 — ML-derived scores are not feature selection candidates

**Rule**: `ml_1y`, `ml_3y`, `ml_5y` (and any future ML-derived columns) must appear in
the `EXCLUDE` set in `scripts/train_models.py` and must never appear in any
`models/feature_sets_*.json` output.

```python
# In scripts/train_models.py:
EXCLUDE = {
    ...
    # ML-derived scores — in-sample contamination: score_historical.py scores ALL rows
    # including training rows, so IC(ml_1y, forward_return_1y) is inflated for 2008-TRAIN_CUTOFF.
    'ml_1y', 'ml_3y', 'ml_5y',
}
```

**Why**: `score_historical.py` loads a model trained on rows up to `train_cutoff=2022` and
then scores **all** historical rows, including the training rows. The resulting `ml_1y` column
therefore has an inflated IC for 2008–2022 rows (the model memorized them). If `ml_1y` passes
the ICIR filter, it enters the feature set and the next training run becomes self-referential
— the model trains on its own lagged predictions. This is circular contamination, not
generalization. The correct fix is walk-forward OOF scoring (Phase C).

**How to apply**: If you add a new ML-derived column (e.g. `ml_fraud_v2`), add it to
`EXCLUDE` before running `run_feature_selection.py`. After selection, verify it is absent
from all three `models/feature_sets_*.json` files. The quality test in Section 9 of
`test_dataset_quality.py` enforces this automatically.

---

## Common Anti-Patterns (and What To Do Instead)

| Anti-pattern | What happens | Do this instead |
|---|---|---|
| Add a column in a one-off `patch_*.py` script | Column silently disappears on next CI run | Add to step3 or step5; use patch only for backfill then merge the logic |
| `groupby('sic_2digit')` without `fiscal_year` | Cross-time lookahead in feature; inflated IC | Always `groupby(['sic_2digit', 'fiscal_year'], observed=True)` |
| Add post-processing script but not to CI yaml | CI produces incomplete dataset | Add to `refresh_data.yml` immediately; update diagram |
| Fix a feature bug, skip re-running selection | Selected feature sets based on broken data | Re-run `run_feature_selection.py` + `factor_research.py` always |
| Use `if dst not in df.columns` for coalesce | Low-fill sparse column blocks high-fill source | Use `combine_first` when a high-fill source should overwrite a sparse dst |
