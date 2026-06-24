# Schema Change Guide

Rules for adding, renaming, or removing columns from `data/historical_dataset_clean.parquet`.

---

## Versioning Policy

The dataset does **not** carry a formal version number in the filename. Instead, the column count and the `CHANGELOG.md` entry under `[Unreleased]` are the canonical record of the current schema. Before any schema change:

1. Check the current column count: `python3 -c "import pandas as pd; df = pd.read_parquet('data/historical_dataset_clean.parquet'); print(df.shape)"`
2. Record the before-count in your CHANGELOG entry.
3. After the change, confirm the after-count.

---

## Adding a Column

| Step | Action |
|---|---|
| 1 | Add the formula to `pipeline/feature_library.py` (point-in-time safe, no look-ahead) |
| 2 | Wire into `pipeline/step5_compute_features.py` in the appropriate `add_*` function |
| 3 | Re-run step5 for all markets; re-integrate via `pipeline/phase_a_integrate_{mkt}.py` |
| 4 | Add a fill-rate threshold for the new column in `FILL_THRESHOLDS` in `scripts/quality/test_dataset_quality.py` |
| 5 | Update `docs/methodology/features.md` — add to the correct category table |
| 6 | Update `docs/architecture.md` — column count in all flowchart labels (4 places) |
| 7 | Update `docs/index.md` tagline column count |
| 8 | Update `docs/methodology/models.md` top flowchart node |
| 9 | Update `CLAUDE.md` architecture table |
| 10 | Add `CHANGELOG.md` entry |
| 11 | Commit + push + HuggingFace push |

---

## Renaming a Column

A rename is a **breaking change**. Every downstream consumer that reads the column by name will break silently.

Checklist before renaming:

- [ ] Search all `.py` files for the old name: `grep -r "old_name" --include="*.py"`
- [ ] Search all `.ipynb` files: `grep -r "old_name" --include="*.ipynb"`
- [ ] Check `scripts/quality/test_dataset_quality.py` `FILL_THRESHOLDS` dict
- [ ] Check `notebooks/08_experiment_hub.ipynb` display logic
- [ ] Check `api/` screener filter logic
- [ ] Check `models/model_meta.json` feature lists — if the column is an ML feature, the trained model references it by name; **you must retrain** after renaming

After confirming all references are updated:
1. Do the rename in `feature_library.py` and `step5_compute_features.py`
2. Rebuild the dataset
3. Update `model_meta.json` if needed; retrain models if the column is a model feature
4. Update all docs (same list as adding a column)
5. CHANGELOG entry must include: old name → new name, reason

---

## Removing (Deprecating) a Column

1. Mark the column in `pipeline/feature_library.py` with a comment: `# DEPRECATED — reason — date`
2. Keep the column in the dataset for **at least one release cycle** (one HuggingFace push) with NaN fill so downstream code doesn't break immediately
3. On the subsequent release, remove the column from the pipeline and drop it from the parquet
4. Update `test_dataset_quality.py` to remove the fill-rate check for that column
5. CHANGELOG entry: column name, removal date, reason

---

## Places Where Column Count Appears (must all stay in sync)

| File | Location | Example text |
|---|---|---|
| `docs/architecture.md` | High-Level Overview S1 node | `58K rows · 326 columns` |
| `docs/architecture.md` | Data Flow Detail Feature Matrix node | `58K rows · 326 cols` |
| `docs/methodology/models.md` | Training Pipeline top flowchart node | `326 columns` |
| `docs/index.md` | Tagline | `326 features` |
| `CLAUDE.md` | Architecture State table | `326 columns` |
| `docs/developer/data-update-guide.md` | Column Count Reference table | `326` |

---

## Primary Key Constraint

`(cik, market, fiscal_year, period_type)` must remain unique after any schema change. New enrichment scripts that join on this key must use a left join — never an inner join — to avoid silently dropping rows.
