"""
Alpha factor IC validation — walk-forward Spearman rank IC per factor.

For each of the 5 alpha factors (value, quality, momentum, growth, fraud_risk):
  - Compute the factor score cross-sectionally per fiscal_year-market cohort
  - Compute rank IC (Spearman) against forward_return_1y for each year
  - Report: mean IC, IC_IR (mean/std), hit rate (% years with positive IC)
  - Flag factors with |mean IC| < 0.02 or IC_IR < 0.3 as "not predictive"

Usage:
    python -m research.alpha_ic_validation
    python -m research.alpha_ic_validation --sector-neutral
"""
from __future__ import annotations

import argparse
import json
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats

from _root import ROOT
from alpha.factors.composite import compute as compute_alpha
from research.ic_engine import compute_yearly_ic, newey_west_tstat

BASE = ROOT
DATA_PATH = BASE / 'data' / 'historical_dataset_clean.parquet'
REPORTS = BASE / 'reports'
REPORTS.mkdir(exist_ok=True)

FACTORS = ['alpha_value', 'alpha_quality', 'alpha_momentum', 'alpha_growth', 'alpha_fraud_risk']
RET_COL = 'forward_return_1y'

IC_THRESHOLD = 0.02
ICIR_THRESHOLD = 0.3


def load_data() -> pd.DataFrame:
    df = pd.read_parquet(DATA_PATH)
    df = df[df['period_type'] == 'annual'].copy()
    df = df[df['fiscal_year'].between(2008, 2025)].copy()
    df = df.sort_values('total_assets', ascending=False, na_position='last')
    df = df.drop_duplicates(subset=['ticker', 'fiscal_year'], keep='first')
    return df.reset_index(drop=True)


def validate_alpha_factors(df: pd.DataFrame, sector_neutral: bool = False) -> list[dict]:
    """Compute walk-forward IC for each alpha factor."""
    alpha_df = compute_alpha(df)
    for col in alpha_df.columns:
        df[col] = alpha_df[col]

    results = []
    for factor in FACTORS:
        ic_series = compute_yearly_ic(
            df, factor, RET_COL,
            sector_neutral=sector_neutral,
            min_obs=30,
            sic_col_override="sic_code",
        )
        ics = ic_series.dropna()
        if len(ics) == 0:
            results.append({
                'factor': factor,
                'mean_ic': None,
                'std_ic': None,
                'ic_ir': None,
                'hit_rate': None,
                'n_years': 0,
                'nw_tstat': None,
                'predictive': False,
                'recommendation': 'REMOVE — no IC data',
            })
            continue

        mean_ic = float(ics.mean())
        std_ic = float(ics.std())
        ic_ir = mean_ic / std_ic if std_ic > 1e-8 else 0.0
        hit_rate = float((ics > 0).mean())
        nw_tstat = newey_west_tstat(ics)

        predictive = abs(mean_ic) >= IC_THRESHOLD and abs(ic_ir) >= ICIR_THRESHOLD

        if not predictive:
            if abs(mean_ic) < IC_THRESHOLD:
                rec = f'FLAG — |mean IC| {abs(mean_ic):.4f} < {IC_THRESHOLD}. Consider downweight.'
            else:
                rec = f'FLAG — |IC_IR| {abs(ic_ir):.3f} < {ICIR_THRESHOLD}. Signal inconsistent.'
        else:
            rec = 'PASS'

        results.append({
            'factor': factor,
            'mean_ic': round(mean_ic, 5),
            'std_ic': round(std_ic, 5),
            'ic_ir': round(ic_ir, 4),
            'hit_rate': round(hit_rate, 3),
            'n_years': int(len(ics)),
            'nw_tstat': round(float(nw_tstat), 3) if pd.notna(nw_tstat) else None,
            'predictive': predictive,
            'recommendation': rec,
            'yearly_ics': {int(yr): round(float(v), 5) for yr, v in ics.items()},
        })

    return results


def print_report(results: list[dict]) -> None:
    print('\n══════════════════════════════════════════════════════════════════════')
    print('  ALPHA FACTOR IC VALIDATION REPORT')
    print('══════════════════════════════════════════════════════════════════════')
    print(f'\n  Thresholds: |mean IC| >= {IC_THRESHOLD}, |IC_IR| >= {ICIR_THRESHOLD}')
    print(f'\n  {"Factor":<22} {"Mean IC":>9} {"IC_IR":>7} {"Hit%":>6} {"NW-t":>6} {"Yrs":>4}  {"Status"}')
    print('  ' + '─' * 80)

    all_pass = True
    for r in results:
        if r['mean_ic'] is None:
            print(f'  {r["factor"]:<22} {"N/A":>9} {"N/A":>7} {"N/A":>6} {"N/A":>6} {r["n_years"]:>4}  {r["recommendation"]}')
            all_pass = False
            continue
        status = '✓ PASS' if r['predictive'] else '✗ FLAG'
        if not r['predictive']:
            all_pass = False
        nw = f'{r["nw_tstat"]:>6.2f}' if r['nw_tstat'] is not None else '   N/A'
        print(f'  {r["factor"]:<22} {r["mean_ic"]:>+9.5f} {r["ic_ir"]:>7.3f} '
              f'{r["hit_rate"]:>5.0%} {nw} {r["n_years"]:>4}  {status}')

    print('  ' + '─' * 80)
    if all_pass:
        print('  RESULT: ALL FACTORS PASS — no changes needed to composite weights.')
    else:
        flagged = [r['factor'] for r in results if not r['predictive']]
        print(f'  RESULT: {len(flagged)} factor(s) flagged: {", ".join(flagged)}')
        print('  RECOMMENDATION: Downweight flagged factors in composite or investigate.')
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description='Alpha factor IC validation')
    parser.add_argument('--sector-neutral', action='store_true', default=False,
                        help='Apply sector neutralization before IC computation')
    args = parser.parse_args()

    print('Loading data...')
    df = load_data()
    print(f'  {len(df):,} rows | {df["ticker"].nunique():,} companies | '
          f'{df["fiscal_year"].nunique()} years')

    print('Computing alpha factor scores and walk-forward IC...')
    results = validate_alpha_factors(df, sector_neutral=args.sector_neutral)

    print_report(results)

    out_json = REPORTS / 'alpha_ic_validation.json'
    serializable = []
    for r in results:
        row = {k: v for k, v in r.items() if k != 'yearly_ics'}
        row['yearly_ics'] = r.get('yearly_ics', {})
        serializable.append(row)
    with open(out_json, 'w') as f:
        json.dump(serializable, f, indent=2)
    print(f'  Saved → {out_json}')

    out_csv = REPORTS / 'alpha_ic_validation.csv'
    summary_df = pd.DataFrame([{k: v for k, v in r.items() if k != 'yearly_ics'} for r in results])
    summary_df.to_csv(out_csv, index=False)
    print(f'  Saved → {out_csv}')


if __name__ == '__main__':
    main()
