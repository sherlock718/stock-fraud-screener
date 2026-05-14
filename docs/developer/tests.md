# Tests

## Running Tests

```bash
# Run all tests
pytest tests/

# With verbose output
pytest tests/ -v

# Run a specific test class
pytest tests/test_pipeline.py::TestTemporalSplit -v

# Run a specific test
pytest tests/test_pipeline.py::TestTemporalSplit::test_no_overlap -v

# Stop on first failure
pytest tests/ -x

# Show slowest tests
pytest tests/ --durations=10
```

## Test Structure

All tests live in `tests/test_pipeline.py`. The suite uses **synthetic in-memory data only** — no files on disk, no network calls, no model files required.

A shared helper function `_make_annual_df()` generates a reproducible synthetic dataset:

```python
_make_annual_df(n_tickers=35, n_years=10, seed=42)
```

The synthetic data matches the pipeline schema (ticker, fiscal_year, revenue, total_assets, net_income, scores, forward returns, etc.) but uses random values — it is not representative of real financial data.

## Test Classes

### `TestTemporalSplit`

Verifies that the train/validation/test split has no overlap and covers all rows.

| Test | What it checks |
|---|---|
| `test_no_overlap` | Train max year ≤ cutoff; val min > cutoff; test min > val end |
| `test_all_rows_covered` | `len(train) + len(val) + len(test) == len(df)` |

### `TestFeatureEngineering`

Verifies computed feature correctness.

| Test | What it checks |
|---|---|
| `test_accruals_to_assets` | `(net_income - operating_cash_flow) / total_assets` within tolerance |
| `test_beneish_dsri` | DSRI formula: `(recv_t / rev_t) / (recv_{t-1} / rev_{t-1})` |
| `test_no_future_leakage` | Features at year T use only year T data |
| `test_icir_finite` | All ICIR values are finite (no NaN or Inf) after `compute_ic_table` |

### `TestFilingLagAudit`

Verifies the bias audit's period-end date logic.

| Test | What it checks |
|---|---|
| `test_detects_leakage` | FY2020 Q4 maps to `2020-12-31` |
| `test_quarterly_periods` | Q1–Q4 map to correct quarter-end dates |

### `TestScoreCompanies`

Verifies scoring logic using a mock model (from `app_v2.py`, retained for coverage).

| Test | What it checks |
|---|---|
| `test_uses_train_medians` | NaN feature values are filled with training medians before scoring |
| `test_missing_horizon_returns_nan` | If model is missing for a horizon, `ml_score` is all NaN (no crash) |

## What Is and Is Not Tested

### Covered

- Temporal split correctness (no data leakage at the split boundary)
- Key feature formulas (accruals, DSRI, ICIR)
- Bias audit's fiscal calendar logic
- Score pipeline's NaN handling and missing-model graceful degradation

### Not Covered

| Area | Reason |
|---|---|
| Full model training | Requires full dataset; 30–60 min; done by CI artifacts |
| Pipeline data fetch | Requires SEC EDGAR / SimFin / DART network access |
| Notebook rendering | Tested by manual execution of `notebooks/08_experiment_hub.ipynb` |
| PDF tearsheet output | Requires `reportlab`; tested manually |
| HuggingFace upload | Requires `HF_TOKEN`; tested via GitHub Actions dry-run |

## Test Dependencies

Tests use only packages already in `requirements.txt`. No additional test libraries are required beyond `pytest`:

```bash
pip install pytest
pytest tests/
```

`pytest` itself is not in `requirements.txt` since it is a dev-only dependency. Install it separately in your dev environment.

## Continuous Integration

Tests do not currently run automatically in GitHub Actions. To add CI:

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements.txt pytest
      - run: pytest tests/ -v
```

This is a roadmap item for Phase 1.

## Adding New Tests

When adding a new feature or script:

1. Add a test class to `tests/test_pipeline.py`
2. Use `_make_annual_df()` for synthetic data — extend it if new columns are needed
3. Keep tests isolated: no file I/O, no network, no global state mutation
4. Tests that require model files or large data belong in integration tests (not yet structured — use `pytest.mark.slow` to tag them when added)
