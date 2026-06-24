# Script Migration Map

> Session 3 artifact. No files are moved here — this is the plan only.
> Execution happens in Session 4 as one atomic commit.

---

## Target Directory Structure

```
scripts/
├── __init__.py
├── _root.py                      # ROOT = Path(__file__).parent.parent
├── _shared/
│   ├── __init__.py
│   └── backtester.py             # shared backtest engine
├── workflows/
│   ├── __init__.py
│   ├── refresh_data.py           # orchestrates full weekly refresh
│   ├── run_pipeline.py           # US pipeline
│   ├── run_pipeline_br.py        # Brazil pipeline
│   ├── run_pipeline_ca.py        # Canada pipeline
│   ├── run_pipeline_eu.py        # Europe pipeline
│   ├── run_pipeline_jp.py        # Japan pipeline
│   ├── run_pipeline_kr.py        # Korea pipeline
│   ├── run_dataset_enrichments.py
│   └── wait_and_merge.py         # multi-market merge workflow
├── data_io/
│   ├── __init__.py
│   ├── fetch_aaer_labels.py
│   ├── fetch_spy_returns.py
│   ├── merge_snapshots.py
│   ├── migrate_to_db.py
│   ├── pull_from_hf.py
│   └── push_to_hf.py
├── enrichments/
│   ├── __init__.py
│   ├── build_fraud_labels.py
│   ├── build_monthly_price_cache.py
│   ├── clean_dataset.py
│   ├── enrich_quarterly_features.py
│   ├── enrich_sectors_dividends.py
│   ├── fix_dataset_quality.py
│   ├── impute_features.py
│   ├── mark_survivorship.py
│   ├── patch_equity_vol_features.py
│   └── patch_montier_c2.py
├── modeling/
│   ├── __init__.py
│   ├── compute_alpha.py
│   ├── generate_oof_scores.py
│   ├── run_feature_selection.py
│   ├── score_historical.py
│   ├── train_models.py
│   ├── train_regression_model.py
│   └── tune_models.py
├── analysis/
│   ├── __init__.py
│   ├── analyze_distributions.py
│   ├── factor_research.py
│   └── generate_reports.py
├── portfolio/
│   ├── __init__.py
│   ├── build_alpha_registry.py
│   ├── build_portfolio.py
│   ├── build_screener_registry.py
│   └── leverage_strategy.py
├── quality/
│   ├── __init__.py
│   ├── bias_audit.py
│   ├── check_data.py
│   ├── check_sync.py
│   ├── monitor_drift.py
│   ├── pit_validate.py
│   ├── run_phase_checks.py
│   ├── test_dataset_quality.py
│   ├── validate_feature_contract.py
│   └── verify_doc_consistency.py
├── ops/
│   ├── __init__.py
│   └── generate_manifest.py
└── hooks/
    ├── __init__.py
    └── pre_commit_guard.py       # already exists at scripts/hooks/
```

---

## Old Path → New Path

| Old path | New path |
|---|---|
| `scripts/refresh_data.py` | `scripts/workflows/refresh_data.py` |
| `scripts/run_pipeline.py` | `scripts/workflows/run_pipeline.py` |
| `scripts/run_pipeline_br.py` | `scripts/workflows/run_pipeline_br.py` |
| `scripts/run_pipeline_ca.py` | `scripts/workflows/run_pipeline_ca.py` |
| `scripts/run_pipeline_eu.py` | `scripts/workflows/run_pipeline_eu.py` |
| `scripts/run_pipeline_jp.py` | `scripts/workflows/run_pipeline_jp.py` |
| `scripts/run_pipeline_kr.py` | `scripts/workflows/run_pipeline_kr.py` |
| `scripts/run_dataset_enrichments.py` | `scripts/workflows/run_dataset_enrichments.py` |
| `scripts/wait_and_merge.py` | `scripts/workflows/wait_and_merge.py` |
| `scripts/fetch_aaer_labels.py` | `scripts/data_io/fetch_aaer_labels.py` |
| `scripts/fetch_spy_returns.py` | `scripts/data_io/fetch_spy_returns.py` |
| `scripts/merge_snapshots.py` | `scripts/data_io/merge_snapshots.py` |
| `scripts/migrate_to_db.py` | `scripts/data_io/migrate_to_db.py` |
| `scripts/pull_from_hf.py` | `scripts/data_io/pull_from_hf.py` |
| `scripts/push_to_hf.py` | `scripts/data_io/push_to_hf.py` |
| `scripts/build_fraud_labels.py` | `scripts/enrichments/build_fraud_labels.py` |
| `scripts/build_monthly_price_cache.py` | `scripts/enrichments/build_monthly_price_cache.py` |
| `scripts/clean_dataset.py` | `scripts/enrichments/clean_dataset.py` |
| `scripts/enrich_quarterly_features.py` | `scripts/enrichments/enrich_quarterly_features.py` |
| `scripts/enrich_sectors_dividends.py` | `scripts/enrichments/enrich_sectors_dividends.py` |
| `scripts/fix_dataset_quality.py` | `scripts/enrichments/fix_dataset_quality.py` |
| `scripts/impute_features.py` | `scripts/enrichments/impute_features.py` |
| `scripts/mark_survivorship.py` | `scripts/enrichments/mark_survivorship.py` |
| `scripts/patch_equity_vol_features.py` | `scripts/enrichments/patch_equity_vol_features.py` |
| `scripts/patch_montier_c2.py` | `scripts/enrichments/patch_montier_c2.py` |
| `scripts/compute_alpha.py` | `scripts/modeling/compute_alpha.py` |
| `scripts/generate_oof_scores.py` | `scripts/modeling/generate_oof_scores.py` |
| `scripts/run_feature_selection.py` | `scripts/modeling/run_feature_selection.py` |
| `scripts/score_historical.py` | `scripts/modeling/score_historical.py` |
| `scripts/train_models.py` | `scripts/modeling/train_models.py` |
| `scripts/train_regression_model.py` | `scripts/modeling/train_regression_model.py` |
| `scripts/tune_models.py` | `scripts/modeling/tune_models.py` |
| `scripts/analyze_distributions.py` | `scripts/analysis/analyze_distributions.py` |
| `scripts/factor_research.py` | `scripts/analysis/factor_research.py` |
| `scripts/generate_reports.py` | `scripts/analysis/generate_reports.py` |
| `scripts/build_alpha_registry.py` | `scripts/portfolio/build_alpha_registry.py` |
| `scripts/build_portfolio.py` | `scripts/portfolio/build_portfolio.py` |
| `scripts/build_screener_registry.py` | `scripts/portfolio/build_screener_registry.py` |
| `scripts/leverage_strategy.py` | `scripts/portfolio/leverage_strategy.py` |
| `scripts/bias_audit.py` | `scripts/quality/bias_audit.py` |
| `scripts/check_data.py` | `scripts/quality/check_data.py` |
| `scripts/check_sync.py` | `scripts/quality/check_sync.py` |
| `scripts/monitor_drift.py` | `scripts/quality/monitor_drift.py` |
| `scripts/pit_validate.py` | `scripts/quality/pit_validate.py` |
| `scripts/run_phase_checks.py` | `scripts/quality/run_phase_checks.py` |
| `scripts/test_dataset_quality.py` | `scripts/quality/test_dataset_quality.py` |
| `scripts/validate_feature_contract.py` | `scripts/quality/validate_feature_contract.py` |
| `scripts/verify_doc_consistency.py` | `scripts/quality/verify_doc_consistency.py` |
| `scripts/generate_manifest.py` | `scripts/ops/generate_manifest.py` |
| `scripts/backtester.py` | `scripts/_shared/backtester.py` |
| `scripts/hooks/pre_commit_guard.py` | `scripts/hooks/pre_commit_guard.py` (no move) |

---

## `__init__.py` Plan

| Directory | `__init__.py` contents |
|---|---|
| `scripts/` | Empty (package marker only) |
| `scripts/_shared/` | Empty |
| `scripts/workflows/` | Empty |
| `scripts/data_io/` | Empty |
| `scripts/enrichments/` | Empty |
| `scripts/modeling/` | Empty |
| `scripts/analysis/` | Empty |
| `scripts/portfolio/` | Empty |
| `scripts/quality/` | Empty |
| `scripts/ops/` | Empty |
| `scripts/hooks/` | Empty (already exists implicitly) |

All `__init__.py` files are empty markers — no re-exports.

---

## `scripts/_root.py` Definition

```python
"""Canonical project root for all scripts."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
```

Every script that currently does `Path(__file__).parent.parent` will instead do:
```python
from scripts._root import ROOT
```

This survives the move because `_root.py` stays at `scripts/_root.py` and its
`.parent.parent` always resolves to the repo root regardless of subdirectory depth.

---

## Cross-Import Resolution Table

| Importer (new path) | Old import | New import |
|---|---|---|
| `scripts/modeling/train_regression_model.py` | `from scripts.train_models import load_data` | `from scripts.modeling.train_models import load_data` |
| `scripts/modeling/run_feature_selection.py` | `from scripts.train_models import (...)` | `from scripts.modeling.train_models import (...)` |
| `scripts/modeling/tune_models.py` | `from scripts.train_models import load_data` | `from scripts.modeling.train_models import load_data` |
| `scripts/portfolio/build_alpha_registry.py` | `from scripts.backtester import (...)` | `from scripts._shared.backtester import (...)` |
| `scripts/enrichments/build_monthly_price_cache.py` | `from scripts.backtester import (...)` | `from scripts._shared.backtester import (...)` |
| `scripts/portfolio/build_screener_registry.py` | `from scripts.backtester import (...)` | `from scripts._shared.backtester import (...)` |
| `scripts/enrichments/impute_features.py` | `from scripts.enrich_quarterly_features import ...` | `from scripts.enrichments.enrich_quarterly_features import ...` |
| `alpha/explain.py` | `from scripts.leverage_strategy import (...)` | `from scripts.portfolio.leverage_strategy import (...)` |

---

## Path Assumption Fix List

Every script below uses `Path(__file__).parent.parent` to derive project root.
After moves, the file is one level deeper, so `.parent.parent` would point to `scripts/`
not the repo root. All must switch to `from scripts._root import ROOT`.

| Script (new path) | Current pattern | Fix |
|---|---|---|
| `scripts/quality/bias_audit.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/modeling/train_regression_model.py` | `BASE = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/analysis/factor_research.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/modeling/generate_oof_scores.py` | `BASE = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/workflows/refresh_data.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/analysis/generate_reports.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/enrichments/patch_montier_c2.py` | `ROOT = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT` (remove sys.path.insert) |
| `scripts/workflows/run_pipeline_kr.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/enrichments/fix_dataset_quality.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/enrichments/patch_equity_vol_features.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/modeling/run_feature_selection.py` | `BASE = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/portfolio/build_alpha_registry.py` | `sys.path.insert(0, str(Path(__file__).parent.parent))` + `BASE = ...` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/modeling/train_models.py` | `BASE = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/ops/generate_manifest.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/quality/test_dataset_quality.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/workflows/run_pipeline_eu.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/enrichments/build_monthly_price_cache.py` | `BASE = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/quality/pit_validate.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/workflows/run_pipeline_jp.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/modeling/compute_alpha.py` | `sys.path.insert(0, str(Path(__file__).resolve().parent.parent))` | `from scripts._root import ROOT` (remove sys.path.insert) |
| `scripts/quality/run_phase_checks.py` | `ROOT = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT` (remove local ROOT + sys.path.insert) |
| `scripts/data_io/merge_snapshots.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/workflows/run_pipeline_br.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/data_io/fetch_spy_returns.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/workflows/run_pipeline.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/_shared/backtester.py` | `sys.path.insert(0, ...)` + `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/enrichments/build_fraud_labels.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/quality/verify_doc_consistency.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/data_io/migrate_to_db.py` | `BASE = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/workflows/wait_and_merge.py` | `Path(__file__).parent.parent / 'data'` etc. | `from scripts._root import ROOT; DATA = ROOT / 'data'` |
| `scripts/modeling/tune_models.py` | `BASE = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/enrichments/mark_survivorship.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/enrichments/enrich_quarterly_features.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/enrichments/clean_dataset.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/quality/check_data.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/data_io/pull_from_hf.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/portfolio/build_screener_registry.py` | `BASE = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/quality/monitor_drift.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/enrichments/enrich_sectors_dividends.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/portfolio/leverage_strategy.py` | `BASE = Path(__file__).parent.parent` + `sys.path.insert` | `from scripts._root import ROOT; BASE = ROOT` (remove sys.path.insert) |
| `scripts/data_io/push_to_hf.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/workflows/run_pipeline_ca.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/portfolio/build_portfolio.py` | `BASE = Path(__file__).parent.parent` | `from scripts._root import ROOT; BASE = ROOT` |
| `scripts/enrichments/impute_features.py` | `sys.path.insert(0, str(Path(__file__).resolve().parent.parent))` | `from scripts._root import ROOT` (remove sys.path.insert) |

---

## CI Workflow References to Update

### `.github/workflows/refresh_data.yml`

| Line | Old reference | New reference |
|---|---|---|
| 75 | `python3 scripts/run_pipeline_eu.py` | `python3 scripts/workflows/run_pipeline_eu.py` |
| 80 | `python3 scripts/run_pipeline_kr.py` | `python3 scripts/workflows/run_pipeline_kr.py` |
| 83 | `python3 scripts/run_pipeline.py` | `python3 scripts/workflows/run_pipeline.py` |
| 94 | `python3 scripts/fix_dataset_quality.py` | `python3 scripts/enrichments/fix_dataset_quality.py` |
| 98 | `python3 scripts/enrich_quarterly_features.py` | `python3 scripts/enrichments/enrich_quarterly_features.py` |
| 102 | `python3 scripts/impute_features.py` | `python3 scripts/enrichments/impute_features.py` |
| 106 | `python3 scripts/validate_feature_contract.py` | `python3 scripts/quality/validate_feature_contract.py` |
| 110 | `python3 scripts/mark_survivorship.py` | `python3 scripts/enrichments/mark_survivorship.py` |
| 114 | `python3 scripts/compute_alpha.py` | `python3 scripts/modeling/compute_alpha.py` |
| 137 | `python3 scripts/score_historical.py` | `python3 scripts/modeling/score_historical.py` |
| 142 | `python3 scripts/check_data.py` | `python3 scripts/quality/check_data.py` |
| 146 | `python3 scripts/test_dataset_quality.py` | `python3 scripts/quality/test_dataset_quality.py` |
| 150 | `python3 scripts/bias_audit.py` | `python3 scripts/quality/bias_audit.py` |
| 154-155 | `python3 scripts/factor_research.py` | `python3 scripts/analysis/factor_research.py` |
| 159 | `python3 scripts/run_feature_selection.py` | `python3 scripts/modeling/run_feature_selection.py` |
| 163 | `python3 scripts/verify_doc_consistency.py` | `python3 scripts/quality/verify_doc_consistency.py` |
| 185 | `python3 scripts/generate_reports.py` | `python3 scripts/analysis/generate_reports.py` |

### `.github/workflows/monitor_drift.yml`

| Line | Old reference | New reference |
|---|---|---|
| 83 | `python3 scripts/monitor_drift.py` | `python3 scripts/quality/monitor_drift.py` |
| 97 | `python3 scripts/train_models.py` | `python3 scripts/modeling/train_models.py` |

### `.github/workflows/ci.yml`

No direct script references — uses `pytest tests/` only. No changes needed.

---

## Test References to Update

No test files currently import scripts directly (verified via grep). Tests use `pytest`
fixtures and test the dataset/pipeline indirectly. No changes needed for tests.

---

## Subprocess / os.system References to Update

| Script (new path) | Line(s) | Current reference | New reference |
|---|---|---|---|
| `scripts/workflows/wait_and_merge.py` | 91 | `'scripts/merge_snapshots.py'` | `'scripts/data_io/merge_snapshots.py'` |
| `scripts/workflows/wait_and_merge.py` | 94 | `'scripts/run_pipeline.py'` | `'scripts/workflows/run_pipeline.py'` |

---

## Docstring / Comment References (non-breaking, but fix for consistency)

These are help strings and comments inside scripts that mention old paths.
Fix during the atomic move for completeness:

| Script (new path) | Approx lines | Content to update |
|---|---|---|
| `scripts/workflows/refresh_data.py` | 13-16 | Usage examples in module docstring |
| `scripts/workflows/run_pipeline.py` | 5-12, 73 | Usage examples + error message |
| `scripts/workflows/run_pipeline_eu.py` | 14-17 | Usage examples |
| `scripts/workflows/run_pipeline_kr.py` | 11-14, 130-138 | Usage examples + post-step instructions |
| `scripts/workflows/wait_and_merge.py` | 7-11 | Docstring instructions |

---

## Important Notes

1. **`pipeline/` is the production spine** — it must NOT move. `scripts/workflows/` orchestrates calls to pipeline modules but never replaces them.
2. **`scripts/analysis/` not `scripts/research/`** — avoids conflict with top-level `research/` directory.
3. **`research/`** is for notebooks and exploratory work; `scripts/analysis/` is for reproducible CLI analysis commands.
4. **`pytest.ini`** must be added at repo root with `pythonpath = .` so that `from scripts._root import ROOT` resolves without `sys.path` hacks.
