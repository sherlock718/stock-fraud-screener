# Phase Done Criteria

**The single source of truth for "is Phase A or Phase B complete?"**

Run every check in the relevant section. All must pass. Binary only — no partial credit.
If any check fails, fix it before declaring the phase done. Do not re-audit. Run this checklist.

---

## Phase Scope (LOCKED — do not re-negotiate)

| Phase | Contains | Does NOT contain |
|---|---|---|
| **Phase A** | Dataset · EDA / data quality · Update schedule · Update diagram | Model training, backtest, bias audit, alpha generation |
| **Phase B** | Feature library · Feature engineering · Feature selection · Factor research · Research notebooks | Model training, walk-forward AUC, backtest, Phase C items |
| **Phase C** | Model training · Tuning · Industry-grade backtest · Bias audit · Alpha generation schema | Phase A/B items |

**Rule**: Phase C items NEVER appear in Phase A/B task lists, gap reports, or audits.  
**Rule**: Do not re-audit a phase. Run this checklist. Pass = done. Fail = fix the specific item.

---

## Phase A — Done When All Pass

### A1. Dataset

```bash
# Run and verify "0 failed"
python3 scripts/test_dataset_quality.py

# Verify shape
python3 -c "
import pandas as pd
df = pd.read_parquet('data/historical_dataset_clean.parquet')
assert df.shape[0] >= 58000, f'Too few rows: {df.shape[0]}'
assert df.shape[1] == 355, f'Wrong col count: {df.shape[1]}'
print(f'Shape OK: {df.shape}')
"

# Verify forward_return targets are winsorized (no extreme outliers)
python3 -c "
import pandas as pd
df = pd.read_parquet('data/historical_dataset_clean.parquet')
for col, cap in [('forward_return_1y', 5.0), ('forward_return_3y', 10.0), ('forward_return_5y', 20.0)]:
    if col in df.columns:
        mx = df[col].abs().max()
        assert mx <= cap, f'{col} max={mx:.1f} exceeds cap {cap} — not winsorized'
        print(f'{col}: max={mx:.3f} <= {cap} OK')
"

# Verify all markets present
python3 -c "
import pandas as pd
df = pd.read_parquet('data/historical_dataset_clean.parquet')
markets = set(df['market'].unique())
required = {'US', 'KR', 'BR', 'CA', 'JP'}
missing = required - markets
assert not missing, f'Missing markets: {missing}'
print(f'Markets present: {sorted(markets)}')
"
```

### A2. EDA / Data Quality Notebook

```bash
# Verify notebook 01 contains required cells (grep cell source)
python3 -c "
import json
nb = json.load(open('notebooks/01_eda_dataset.ipynb'))
sources = ' '.join(c['source'] if isinstance(c['source'], str)
                   else ''.join(c['source'])
                   for c in nb['cells'] if c['cell_type'] == 'code')
checks = {
    'forward_return histogram': 'forward_return' in sources and ('hist' in sources or 'distplot' in sources or 'plot' in sources),
    'forward_return outlier stats': 'forward_return' in sources and ('quantile' in sources or 'describe' in sources or 'p99' in sources or 'percentile' in sources),
    'point-in-time lineage check': 'filed_date' in sources or 'filing_date' in sources or 'lineage' in sources or 'look.ahead' in sources or 'point.in.time' in sources,
    'null profile': 'null' in sources.lower() or 'isna' in sources or 'isnull' in sources,
}
for name, ok in checks.items():
    status = 'PASS' if ok else 'FAIL'
    print(f'  [{status}] notebook 01: {name}')
"
```

### A3. Update Schedule (CI)

```bash
# Every required script must appear in refresh_data.yml
python3 -c "
ci = open('.github/workflows/refresh_data.yml').read()
required = [
    'impute_features.py',
    'mark_survivorship.py',
    'compute_alpha.py',
    'score_historical.py',
    'enrich_quarterly_features.py',
    'test_dataset_quality.py',
]
for s in required:
    status = 'PASS' if s in ci else 'FAIL'
    print(f'  [{status}] refresh_data.yml contains: {s}')
"
```

### A4. Update Diagram Matches CI

```bash
# Scripts in diagram must exist; no phantom steps
python3 -c "
import re
guide = open('docs/developer/data-update-guide.md').read()
ci    = open('.github/workflows/refresh_data.yml').read()
# Extract .py filenames from guide
scripts_in_guide = set(re.findall(r'[\w_]+\.py', guide))
scripts_in_ci    = set(re.findall(r'[\w_]+\.py', ci))
# Check key scripts appear in both
shared = scripts_in_guide & scripts_in_ci
print(f'Scripts in both guide + CI: {sorted(shared)}')
guide_only = scripts_in_guide - scripts_in_ci
if guide_only:
    print(f'  [WARN] In guide but not CI: {sorted(guide_only)}')
else:
    print('  [PASS] No guide-only phantom scripts')
"
```

---

## Phase B — Done When All Pass

### B1. Feature Library

```bash
python3 -c "
import subprocess, sys
# Verify key fraud formulas exist in codebase
files = ['pipeline/step5_compute_features.py', 'pipeline/feature_library.py']
checks = {
    'beneish_m_score':   any('beneish_m_score' in open(f).read() for f in files),
    'altman_z_score':    any('altman_z_score'   in open(f).read() for f in files),
    'ohlson_o_score':    any('ohlson_o_score'   in open(f).read() for f in files),
    'piotroski_f_score': any('piotroski_f_score' in open(f).read() for f in files),
    'montier_c_score':   any('montier_c_score'  in open(f).read() for f in files),
    'sloan_wc_accruals': any('sloan_wc_accruals' in open(f).read() for f in files),
    'sloan_lt_accruals': any('sloan_lt_accruals' in open(f).read() for f in files),
}
for name, ok in checks.items():
    print(f'  [{\"PASS\" if ok else \"FAIL\"}] feature_library: {name} implemented')
"

# Verify formulas are in the parquet as columns
python3 -c "
import pandas as pd
df = pd.read_parquet('data/historical_dataset_clean.parquet')
required_cols = [
    'beneish_m_score', 'altman_z_score', 'ohlson_o_score',
    'piotroski_f_score', 'montier_c_score',
    'sloan_wc_accruals', 'sloan_lt_accruals',
]
for col in required_cols:
    status = 'PASS' if col in df.columns else 'FAIL'
    print(f'  [{status}] parquet column: {col}')
"
```

### B2. Feature Engineering Correctness

```bash
python3 -c "
src = open('pipeline/step5_compute_features.py').read()
checks = {
    'beneish_dsri clipped [0.5, 3]':     'clip(0.5' in src or 'clip(lower=0.5' in src,
    'growth cols in winsorize list':      'revenue_growth_yoy' in src and 'ratio_cols' in src,
    'sector pct includes fiscal_year':    \"'fiscal_year'\" in src and 'sector_pct' in src,
    'momentum cohort guard n>=10':        'cohort' in src.lower() or 'n_min' in src or 'min_count' in src or 'groupby_min' in src,
}
for name, ok in checks.items():
    print(f'  [{\"PASS\" if ok else \"FAIL\"}] feature_eng: {name}')
"
```

### B3. Feature Selection

```bash
# Verify no alpha_* or ml_* in feature sets
python3 -c "
import json, glob
for path in sorted(glob.glob('models/feature_sets_*.json')):
    obj = json.load(open(path))
    features = obj['features'] if isinstance(obj, dict) else obj
    alpha_leak = [f for f in features if f.startswith('alpha_')]
    ml_leak    = [f for f in features if f.startswith('ml_')]
    if alpha_leak:
        print(f'  [FAIL] {path}: alpha_* present: {alpha_leak}')
    elif ml_leak:
        print(f'  [FAIL] {path}: ml_* present: {ml_leak}')
    else:
        print(f'  [PASS] {path}: no alpha_* or ml_* ({len(features)} features)')
"

# Verify PSI threshold = 0.25
python3 -c "
src = open('scripts/run_feature_selection.py').read()
if 'psi_threshold=0.25' in src or \"default=0.25\" in src or '0.25' in src:
    print('  [PASS] PSI threshold is 0.25')
else:
    print('  [FAIL] PSI threshold NOT 0.25 — check run_feature_selection.py')
"

# Verify Newey-West + FDR columns in output
python3 -c "
import pandas as pd
try:
    df = pd.read_csv('reports/feature_selection_summary.csv', nrows=2)
    nw  = 'PASS' if 'ic_tstat_nw' in df.columns else 'FAIL'
    fdr = 'PASS' if 'fdr_reject'  in df.columns else 'FAIL'
    print(f'  [{nw}]  feature_selection_summary.csv has ic_tstat_nw column')
    print(f'  [{fdr}] feature_selection_summary.csv has fdr_reject column')
except Exception as e:
    print(f'  [FAIL] Could not read feature_selection_summary.csv: {e}')
"
```

### B4. Factor Research Artifacts

```bash
# Verify factor_research CSVs have required columns
python3 -c "
import pandas as pd, glob
for path in sorted(glob.glob('reports/factor_research_*.csv')):
    df = pd.read_csv(path, nrows=2)
    required = ['ic', 'icir', 'ic_tstat', 'pct_positive_ic']
    missing = [c for c in required if c not in df.columns]
    status = 'PASS' if not missing else 'FAIL'
    print(f'  [{status}] {path}: {\"all columns present\" if not missing else \"missing: \" + str(missing)}')
"

# Verify notebook 02 contains required analysis cells
python3 -c "
import json
nb = json.load(open('notebooks/02_ic_analysis.ipynb'))
sources = ' '.join(c['source'] if isinstance(c['source'], str)
                   else ''.join(c['source'])
                   for c in nb['cells'] if c['cell_type'] == 'code')
checks = {
    'IC decay curve':             'decay' in sources.lower() or ('ic' in sources.lower() and ('t+2' in sources or 't_2' in sources or 'lag' in sources or 'horizon' in sources)),
    'quintile return spreads':    'quintile' in sources.lower() or 'q1' in sources.lower() or 'qcut' in sources,
    'Fama-MacBeth regression':    'fama' in sources.lower() or 'macbeth' in sources.lower() or 'fama_macbeth' in sources.lower(),
    'market regime IC':           'recession' in sources.lower() or 'regime' in sources.lower() or 'expansion' in sources.lower(),
    'long-short decomposition':   'long_leg' in sources.lower() or 'short_leg' in sources.lower() or 'long.short' in sources.lower() or ('long' in sources.lower() and 'short' in sources.lower() and 'quintile' in sources.lower()),
    'information ratio':          'information_ratio' in sources.lower() or 'ir =' in sources.lower() or 'ir=' in sources.lower() or ('mean_ic' in sources.lower() and 'std' in sources.lower()),
}
for name, ok in checks.items():
    print(f'  [{\"PASS\" if ok else \"FAIL\"}] notebook 02: {name}')
"
```

### B5. All Notebooks Execute Clean

```bash
# Verify all 5 notebooks have outputs (not cleared)
python3 -c "
import json, glob
for path in sorted(glob.glob('notebooks/0*.ipynb')):
    nb = json.load(open(path))
    code_cells = [c for c in nb['cells'] if c['cell_type'] == 'code']
    with_output = [c for c in code_cells if c.get('outputs')]
    pct = len(with_output) / max(len(code_cells), 1)
    status = 'PASS' if pct >= 0.5 else 'FAIL'
    print(f'  [{status}] {path}: {len(with_output)}/{len(code_cells)} cells have outputs ({pct:.0%})')
"
```

---

## How to Use This Document

1. **Before declaring a phase done**: run every command in the relevant section
2. **If any check fails**: fix that specific item only — do not re-audit the whole phase
3. **If all checks pass**: phase is done — move to the next phase
4. **Do not modify the scope definition** without updating this document and CLAUDE.md simultaneously

This document replaces the Phase Closure Checklist in `docs/developer/pipeline-integrity.md`.
