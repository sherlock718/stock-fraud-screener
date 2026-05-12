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

Run this before marking any Phase A or Phase B task as Done.

```
PHASE A CLOSURE
[ ] Dataset shape correct (rows × expected_cols)
[ ] test_dataset_quality.py — all 53 checks pass
[ ] refresh_data.yml produces the same dataset as the current parquet (verify step-by-step)
[ ] data-update-guide.md diagram matches the actual script execution order in CI
[ ] CHANGELOG.md updated

PHASE B CLOSURE
[ ] All new features are in step3_enrich_prices.py or step5_compute_features.py
[ ] Every groupby rank/percentile includes fiscal_year in the group keys
[ ] run_feature_selection.py re-run on the current dataset → feature_sets_{1y,3y,5y}.json refreshed
[ ] factor_research.py re-run → reports/factor_research_{1y,3y,5y}.csv refreshed
[ ] All 4 notebooks re-run (kernel restart + run all) showing current numbers
[ ] feature-registry.md column count matches df.shape[1]
[ ] docs/methodology/features.md table totals match actual column count
[ ] CHANGELOG.md updated
```

---

## Common Anti-Patterns (and What To Do Instead)

| Anti-pattern | What happens | Do this instead |
|---|---|---|
| Add a column in a one-off `patch_*.py` script | Column silently disappears on next CI run | Add to step3 or step5; use patch only for backfill then merge the logic |
| `groupby('sic_2digit')` without `fiscal_year` | Cross-time lookahead in feature; inflated IC | Always `groupby(['sic_2digit', 'fiscal_year'], observed=True)` |
| Add post-processing script but not to CI yaml | CI produces incomplete dataset | Add to `refresh_data.yml` immediately; update diagram |
| Fix a feature bug, skip re-running selection | Selected feature sets based on broken data | Re-run `run_feature_selection.py` + `factor_research.py` always |
| Use `if dst not in df.columns` for coalesce | Low-fill sparse column blocks high-fill source | Use `combine_first` when a high-fill source should overwrite a sparse dst |
