"""
P0e — Data Lineage + Feature Dictionary

Generates a machine-readable feature dictionary from the live parquet dataset.
Output: reports/feature_dictionary.csv

Each row describes one column:
  feature          — column name
  dtype            — pandas dtype string
  fill_rate        — fraction of non-null values (0–1)
  mean / std / min / p25 / p50 / p75 / max  — numeric statistics
  category         — inferred category (identifier, label, feature, return)
  source           — inferred source pipeline step
  description      — human-readable description (templated)

Usage:
    python3 pipeline/enrich_feature_dictionary.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE    = Path(__file__).parent.parent
DATA    = BASE / 'data'
REPORTS = BASE / 'reports'
IN      = DATA / 'historical_dataset_clean.parquet'
OUT     = REPORTS / 'feature_dictionary.csv'
REPORTS.mkdir(exist_ok=True)


# ── Category & Source Inference ───────────────────────────────────────────────

IDENTIFIERS = {
    'cik', 'ticker', 'name', 'filed_date', 'fiscal_year', 'fiscal_quarter',
    'period_type', 'exchange', 'sic_code', 'sic_description', 'market',
    'country', 'accounting_std', 'size_category_label', 'corp_code', 'acc_mt',
    'as_of_date', 'filing_lag_days',
}

LABEL_PREFIXES = ('forward_return', 'beat_local_market', 'excess_return', 'benchmark_return',
                  'fraud_confirmed', 'fraud_suspect')

RETURN_PREFIXES = ('forward_return', 'beat_local_market', 'excess_return', 'benchmark_return')


def infer_category(col: str) -> str:
    if col in IDENTIFIERS:
        return 'identifier'
    if any(col.startswith(p) for p in RETURN_PREFIXES):
        return 'return_label'
    if col in ('fraud_confirmed', 'fraud_suspect'):
        return 'fraud_label'
    if col.startswith('fraud_score'):
        return 'fraud_taxonomy'
    return 'feature'


SOURCE_MAP = {
    # raw financial statement items
    'revenue': 'step2_build_snapshots',
    'net_income': 'step2_build_snapshots',
    'gross_profit': 'step2_build_snapshots',
    'operating_income': 'step2_build_snapshots',
    'total_assets': 'step2_build_snapshots',
    'total_equity': 'step2_build_snapshots',
    'operating_cash_flow': 'step2_build_snapshots',
    'capex': 'step2_build_snapshots',
    'long_term_debt': 'step2_build_snapshots',
    'short_term_debt': 'step2_build_snapshots',
    'cash': 'step2_build_snapshots',
    'accounts_receivable': 'step2_build_snapshots',
    # Beneish / Altman / Piotroski
    **{c: 'step5_compute_features (fraud_signals.py)' for c in [
        'beneish_m_score', 'beneish_dsri', 'beneish_gmi', 'beneish_aqi',
        'beneish_sgi', 'beneish_depi', 'beneish_sgai', 'beneish_lvgi', 'beneish_tata',
        'altman_z_score', 'altman_x1', 'altman_x2', 'altman_x3', 'altman_x4', 'altman_x5',
        'piotroski_f_score', 'piotroski_ocf_pos', 'piotroski_roa_pos',
        'piotroski_delta_roa', 'piotroski_delta_lev', 'piotroski_delta_liq',
        'sloan_accruals', 'accruals_to_assets', 'wc_accruals_to_assets', 'accruals_avg_3y',
    ]},
    # forward returns
    **{c: 'step3_enrich_prices' for c in [
        'forward_return_6m', 'forward_return_1y', 'forward_return_2y',
        'forward_return_3y', 'forward_return_5y', 'forward_return_10y', 'forward_return_15y',
        'entry_price',
    ]},
    # fraud labels
    'fraud_confirmed': 'step7b_fraud_labels.py (P0c)',
    'fraud_suspect': 'step7b_fraud_labels.py (P0c)',
    # fraud taxonomy
    **{c: 'step7_fraud_taxonomy.py (P0d)' for c in [
        'fraud_score_accounting', 'fraud_score_dilution', 'fraud_score_quality',
        'fraud_score_distress', 'fraud_score_governance', 'fraud_score_composite',
    ]},
    # point-in-time
    'as_of_date': 'step6_clean.py (P0a)',
    'filing_lag_days': 'step6_clean.py (P0a)',
}


def infer_source(col: str) -> str:
    if col in SOURCE_MAP:
        return SOURCE_MAP[col]
    if col.startswith('forward_return') or col.startswith('beat_local_market'):
        return 'step3_enrich_prices'
    if col.startswith('excess_return') or col.startswith('benchmark_return'):
        return 'step3_enrich_prices'
    if col.startswith('beneish') or col.startswith('altman') or col.startswith('piotroski'):
        return 'step5_compute_features (fraud_signals.py)'
    if col.startswith('fraud_score'):
        return 'step7_fraud_taxonomy.py (P0d)'
    if col in ('fraud_confirmed', 'fraud_suspect'):
        return 'step7b_fraud_labels.py (P0c)'
    if any(col.startswith(p) for p in ('value_', 'quality_', 'momentum_', 'growth_')):
        return 'step5_compute_features'
    if col.endswith('_sector_pct'):
        return 'step5_compute_features (sector normalisation)'
    if col.endswith('_yoy') or col.endswith('_3y') or col.endswith('_trend_3y'):
        return 'step5_compute_features (time series)'
    return 'step2_build_snapshots / step5_compute_features'


DESCRIPTIONS = {
    'cik': 'SEC Central Index Key — unique company identifier in EDGAR',
    'ticker': 'Exchange ticker symbol',
    'fiscal_year': 'Fiscal year of the filing',
    'filed_date': 'Date the 10-K/10-Q was filed with the SEC (point-in-time safe)',
    'as_of_date': 'Alias for filed_date — the date from which data is legally public (P0a)',
    'filing_lag_days': 'Days from FY Dec-31 year-end to filing date (P0a)',
    'beneish_m_score': 'Beneish (1999) 8-factor manipulation score. >-1.78 = likely manipulation',
    'altman_z_score': 'Altman (1968) Z-score. <1.81 = distress, >2.99 = safe',
    'piotroski_f_score': 'Piotroski (2000) 9-point fundamental quality score',
    'sloan_accruals': 'Sloan (1996) accruals component = (NI - CFO) / avg_total_assets',
    'accruals_to_assets': 'Total accruals divided by total assets (higher = more aggressive accruals)',
    'fraud_confirmed': 'Binary: 1 = confirmed SEC enforcement action or known major fraud (P0c)',
    'fraud_suspect': 'Binary: 1 = 2+ quantitative fraud signals fire, no confirmed enforcement (P0c)',
    'fraud_score_accounting': 'Accounting manipulation sub-score [0–1], higher = riskier (P0d)',
    'fraud_score_dilution': 'Dilution fraud sub-score [0–1] (P0d)',
    'fraud_score_quality': 'Earnings quality sub-score [0–1], higher = worse cash quality (P0d)',
    'fraud_score_distress': 'Financial distress sub-score [0–1] (P0d)',
    'fraud_score_governance': 'Governance risk sub-score [0–1] (P0d)',
    'fraud_score_composite': 'Weighted composite of 5 fraud sub-scores (P0d)',
}


def describe_col(col: str) -> str:
    if col in DESCRIPTIONS:
        return DESCRIPTIONS[col]
    if col.startswith('forward_return_'):
        h = col.replace('forward_return_', '')
        return f'Total return over {h} horizon from filing date (point-in-time)'
    if col.startswith('beat_local_market_'):
        h = col.replace('beat_local_market_', '')
        return f'Binary: 1 = beat local market index over {h} horizon'
    if col.endswith('_sector_pct'):
        base = col.replace('_sector_pct', '')
        return f'Percentile rank of {base} within same SIC-2 sector and fiscal year'
    if col.endswith('_yoy'):
        base = col.replace('_yoy', '')
        return f'Year-over-year change in {base}'
    if col.endswith('_growth'):
        base = col.replace('_growth', '')
        return f'Growth rate of {base} (year-over-year)'
    if col.endswith('_trend_3y'):
        base = col.replace('_trend_3y', '')
        return f'3-year linear trend in {base}'
    if 'composite' in col:
        return f'Composite score combining multiple {col.split("_")[0]} signals'
    return ''


# ── Main ──────────────────────────────────────────────────────────────────────

def run() -> None:
    if not IN.exists():
        print(f'ERROR: {IN} not found — run step 6 first')
        sys.exit(1)

    print('P0e — Building feature dictionary...')
    df = pd.read_parquet(IN)
    print(f'  Loaded {len(df):,} rows × {len(df.columns)} columns')

    rows = []
    numeric_df = df.select_dtypes(include=[np.number])

    for col in df.columns:
        series = df[col]
        fill_rate = series.notna().mean()
        dtype_str = str(series.dtype)
        category = infer_category(col)
        source   = infer_source(col)
        desc     = describe_col(col)

        row: dict = {
            'feature':     col,
            'dtype':       dtype_str,
            'fill_rate':   round(fill_rate, 4),
            'category':    category,
            'source':      source,
            'description': desc,
        }

        if col in numeric_df.columns:
            s = series.dropna()
            if len(s) > 0:
                row.update({
                    'mean': round(float(s.mean()), 6),
                    'std':  round(float(s.std()),  6),
                    'min':  round(float(s.min()),  6),
                    'p25':  round(float(s.quantile(0.25)), 6),
                    'p50':  round(float(s.median()), 6),
                    'p75':  round(float(s.quantile(0.75)), 6),
                    'max':  round(float(s.max()),  6),
                })
        rows.append(row)

    out_df = pd.DataFrame(rows)
    out_df = out_df.sort_values(['category', 'feature']).reset_index(drop=True)
    out_df.to_csv(OUT, index=False)

    # Summary
    print(f'\n  Feature Dictionary Summary:')
    for cat, grp in out_df.groupby('category'):
        print(f'    {cat:<20s}: {len(grp):>4} columns')
    print(f'\n  Fill rate buckets:')
    buckets = [0, 0.25, 0.50, 0.75, 0.90, 1.001]
    labels  = ['< 25%', '25–50%', '50–75%', '75–90%', '≥ 90%']
    for i, label in enumerate(labels):
        n = ((out_df['fill_rate'] >= buckets[i]) & (out_df['fill_rate'] < buckets[i+1])).sum()
        print(f'    fill {label:>8s}: {n:>4} columns')

    print(f'\n  Saved: {OUT}')


if __name__ == '__main__':
    run()
