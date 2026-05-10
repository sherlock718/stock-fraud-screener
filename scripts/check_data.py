"""
Data quality harness — run after every pipeline rebuild.

Usage:
    python3 scripts/check_data.py                # check historical_dataset_clean.parquet
    python3 scripts/check_data.py --verbose      # print per-market breakdown
    python3 scripts/check_data.py --fail-fast    # exit 1 on first failure

Returns exit code 0 if all checks pass, 1 if any assertion fails.
"""
from __future__ import annotations
import sys
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

BASE    = Path(__file__).parent.parent
SRC     = BASE / 'data' / 'historical_dataset_clean.parquet'
REPORTS = BASE / 'reports'

# ── Thresholds ────────────────────────────────────────────────────────────────
CHECKS = {
    'min_rows':                  100_000,
    'min_companies':             3_000,
    'min_annual_rows':           30_000,
    'fiscal_year_min':           2008,
    'fiscal_year_max':           2025,
    'forward_return_1y_min':     -1.0,
    'forward_return_1y_max':      5.0,
    'revenue_growth_yoy_max':    15.0,   # p99 after winsorize should be ~10
    'us_total_assets_null_max':   0.01,  # <1% for US annual
    'br_total_assets_null_max':   0.01,  # <1% for BR annual
    'labeled_1y_min':            30_000,
}


def run(src: Path = SRC, verbose: bool = False, fail_fast: bool = False) -> bool:
    if not src.exists():
        print(f'ERROR: {src} not found')
        return False

    df = pd.read_parquet(src)
    ann = df[df['period_type'] == 'annual']
    failures = []

    def check(name: str, passed: bool, detail: str):
        status = '✅' if passed else '❌'
        print(f'  {status} {name}: {detail}')
        if not passed:
            failures.append(name)
            if fail_fast:
                sys.exit(1)

    print(f'\n{"="*60}')
    print(f'Data Quality Report — {src.name}')
    print(f'{"="*60}\n')

    # ── Shape ────────────────────────────────────────────────────────────────
    print('Shape:')
    check('min_rows',      len(df) >= CHECKS['min_rows'],
          f'{len(df):,} rows (min {CHECKS["min_rows"]:,})')
    check('min_companies', df['cik'].nunique() >= CHECKS['min_companies'],
          f'{df["cik"].nunique():,} companies')
    check('min_annual_rows', len(ann) >= CHECKS['min_annual_rows'],
          f'{len(ann):,} annual rows')

    # ── Value ranges ─────────────────────────────────────────────────────────
    print('\nValue ranges:')
    fy_min = int(df['fiscal_year'].min()); fy_max = int(df['fiscal_year'].max())
    check('fiscal_year_range',
          fy_min >= CHECKS['fiscal_year_min'] and fy_max <= CHECKS['fiscal_year_max'],
          f'{fy_min}–{fy_max}')

    if 'forward_return_1y' in df.columns:
        fr_min = df['forward_return_1y'].min(); fr_max = df['forward_return_1y'].max()
        check('forward_return_1y_clipped',
              fr_min >= CHECKS['forward_return_1y_min'] and fr_max <= CHECKS['forward_return_1y_max'],
              f'{fr_min:.3f} to {fr_max:.3f}')

    if 'revenue_growth_yoy' in df.columns:
        rg_max = df['revenue_growth_yoy'].max()
        check('revenue_growth_winsorized', rg_max <= CHECKS['revenue_growth_yoy_max'],
              f'max = {rg_max:.2f} (threshold {CHECKS["revenue_growth_yoy_max"]})')

    # ── Required columns ─────────────────────────────────────────────────────
    print('\nRequired columns:')
    for col in ['likely_delisted', 'market', 'country', 'period_type',
                'forward_return_1y', 'revenue_growth_yoy']:
        check(f'col:{col}', col in df.columns, 'present' if col in df.columns else 'MISSING')

    # ── Null rates (annual) ──────────────────────────────────────────────────
    print('\nAnnual total_assets nulls:')
    for mkt, threshold in [('US', CHECKS['us_total_assets_null_max']),
                            ('BR', CHECKS['br_total_assets_null_max'])]:
        mkt_ann = ann[ann['market'] == mkt]
        if len(mkt_ann) == 0:
            continue
        null_rate = mkt_ann['total_assets'].isna().mean()
        check(f'total_assets_null_{mkt}',
              null_rate <= threshold,
              f'{null_rate*100:.1f}% null (max {threshold*100:.0f}%)')

    # ── Labeling ─────────────────────────────────────────────────────────────
    print('\nLabeling:')
    labeled = df['forward_return_1y'].notna().sum()
    check('labeled_1y_min', labeled >= CHECKS['labeled_1y_min'],
          f'{labeled:,} rows (min {CHECKS["labeled_1y_min"]:,})')

    # ── Duplicates ───────────────────────────────────────────────────────────
    print('\nDuplicates:')
    dup_count = ann.duplicated(['ticker', 'fiscal_year']).sum()
    check('no_annual_dups', dup_count == 0, f'{dup_count} duplicate (ticker, fiscal_year)')

    # ── Time-series continuity ────────────────────────────────────────────────
    print('\nTime-series continuity:')
    max_gap = 0
    n_gapped = 0
    for _tick, grp in ann.groupby('ticker'):
        yrs = sorted(grp['fiscal_year'].dropna().unique())
        if len(yrs) < 2:
            continue
        gaps = [yrs[i + 1] - yrs[i] for i in range(len(yrs) - 1)]
        g = max(gaps)
        if g > 2:
            n_gapped += 1
        if g > max_gap:
            max_gap = g
    check('ts_max_gap_le_5', max_gap <= 5,
          f'largest single-ticker gap = {max_gap} years; {n_gapped} tickers with gap > 2y')

    # ── Null-rate audit: core ML features ─────────────────────────────────────
    print('\nNull rate — core ML features (annual rows):')
    CORE_FEATURES = [
        'gross_margin', 'roe', 'debt_to_equity', 'current_ratio', 'pe_ratio',
        'piotroski_f_score', 'beneish_m_score', 'momentum_12m_prior',
        'value_composite', 'quality_composite',
    ]
    MAX_NULL_PCT = 40.0
    for feat in CORE_FEATURES:
        if feat not in ann.columns:
            check(f'feature_present:{feat}', False, 'MISSING column')
            continue
        null_pct = ann[feat].isna().mean() * 100
        check(f'null_rate:{feat}', null_pct <= MAX_NULL_PCT,
              f'{null_pct:.1f}% null (max {MAX_NULL_PCT:.0f}%)')

    # ── Cross-market return normalization ─────────────────────────────────────
    print('\nCross-market return sanity (forward_return_1y):')
    if 'forward_return_1y' in ann.columns:
        mkt_stats = (
            ann[ann['forward_return_1y'].notna()]
            .groupby('market')['forward_return_1y']
            .agg(median='median', p99=lambda x: x.quantile(0.99), n='count')
        )
        for mkt, row2 in mkt_stats.iterrows():
            if row2['n'] < 30:
                continue
            # Warn if median is implausibly extreme (>30% or <-20%) — suggests FX issue
            check(f'return_median_{mkt}',
                  -0.20 <= row2['median'] <= 0.30,
                  f'median={row2["median"]:+.3f}  p99={row2["p99"]:+.3f}  n={int(row2["n"])}')


    # ── Market presence ──────────────────────────────────────────────────────
    print('\nMarket presence:')
    markets = df['market'].unique().tolist()
    check('has_US',  'US' in markets, str(markets))
    check('has_non_US', any(m != 'US' for m in markets), str(markets))

    # ── Per-market breakdown ─────────────────────────────────────────────────
    if verbose:
        print('\nPer-market annual breakdown:')
        mkt_grp = ann.groupby('market').agg(
            rows=('cik', 'count'),
            companies=('cik', 'nunique'),
            ta_null_pct=('total_assets', lambda x: x.isna().mean() * 100),
            labeled_1y=('forward_return_1y', lambda x: x.notna().sum()),
        ).round(1)
        print(mkt_grp.to_string())

        print('\nForward return column ranges:')
        fwd_cols = sorted(c for c in df.columns if c.startswith('forward_return'))
        for c in fwd_cols:
            mn = df[c].min(); mx = df[c].max(); n = df[c].notna().sum()
            print(f'  {c}: [{mn:.2f}, {mx:.2f}]  n={n:,}')

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f'\n{"="*60}')
    if failures:
        print(f'FAILED: {len(failures)} check(s) — {", ".join(failures)}')
    else:
        print(f'All checks passed ({len(df):,} rows, {df["cik"].nunique():,} companies)')
    print(f'{"="*60}\n')

    return len(failures) == 0


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Stock fraud screener data quality harness')
    p.add_argument('--src',       default=str(SRC), help='Path to parquet file')
    p.add_argument('--verbose',   action='store_true', help='Print per-market breakdown')
    p.add_argument('--fail-fast', action='store_true', help='Exit 1 on first failure')
    args = p.parse_args()

    ok = run(Path(args.src), verbose=args.verbose, fail_fast=args.fail_fast)
    sys.exit(0 if ok else 1)
