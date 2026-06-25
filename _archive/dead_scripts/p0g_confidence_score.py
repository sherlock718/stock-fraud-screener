"""
P0g — Data Confidence Score

Adds a per-row `data_confidence` score (0.0–1.0) expressing how much we trust
the data quality for a given filing row. Higher = more complete and reliable data.

This is distinct from fraud scores — a row can have high confidence and be fraud,
or low confidence (sparse data) and be clean.

Confidence is built from three dimensions (equally weighted at 1/3 each):

1. COVERAGE (0.0–1.0):
   Fraction of core analytical columns that are non-null.
   Core column groups:
     Financial statement core (7): revenue, net_income, total_assets, total_equity,
                                    operating_cash_flow, gross_profit, operating_income
     Fraud signals (4): beneish_m_score, altman_z_score, piotroski_f_score, sloan_accruals
     Price & returns (2): entry_price, forward_return_1y
     Ratios (6): net_margin, roe, roa, ocf_margin, debt_to_equity, current_ratio

2. CONSISTENCY (0.0–1.0):
   Checks for internal accounting consistency. Each check contributes 1/N.
   Checks:
     a. total_assets > 0
     b. |total_equity| <= total_assets * 1.5  (equity can be negative but not wildly larger)
     c. revenue > 0  (positive revenue)
     d. |net_income| < revenue * 3  (loss shouldn't be more than 3x revenue)
     e. operating_cash_flow: not NaN if revenue > 0  (cash flow should exist for real companies)
     f. gross_profit >= net_income  (gross profit can't be below net income in most cases)

3. TIMELINESS (0.0–1.0):
   Penalises very old or very new (incomplete) filings.
   Based on filing_lag_days (days from FY Dec-31 to filed date):
     0–60 days:   1.0 (filed on time)
     60–180 days: linear decay to 0.7
     180+ days:   0.5 (very late filing = red flag)
   fiscal_year < 2012: 0.7 (early XBRL era — less reliable)
   fiscal_year >= 2018: 1.0 (full digital era)

Output column:
  data_confidence  — float [0, 1], higher = more trustworthy row

Usage:
    python3 pipeline/p0g_confidence_score.py
    python3 pipeline/p0g_confidence_score.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE   = Path(__file__).parent.parent
DATA   = BASE / 'data'
IN_OUT = DATA / 'historical_dataset_clean.parquet'

# ── Core column groups for coverage score ─────────────────────────────────────

COVERAGE_GROUPS: dict[str, list[str]] = {
    'financial_core': [
        'revenue', 'net_income', 'total_assets', 'total_equity',
        'operating_cash_flow', 'gross_profit', 'operating_income',
    ],
    'fraud_signals': [
        'beneish_m_score', 'altman_z_score', 'piotroski_f_score', 'sloan_accruals',
    ],
    'price_returns': [
        'entry_price', 'forward_return_1y',
    ],
    'ratios': [
        'net_margin', 'roe', 'roa', 'ocf_margin', 'debt_to_equity', 'current_ratio',
    ],
}

# ── Coverage Score ─────────────────────────────────────────────────────────────

def coverage_score(df: pd.DataFrame) -> pd.Series:
    """Fraction of core analytical columns that are non-null."""
    all_core = []
    for cols in COVERAGE_GROUPS.values():
        all_core.extend([c for c in cols if c in df.columns])

    if not all_core:
        return pd.Series(0.5, index=df.index)

    present = df[all_core].notna().astype(float)
    return present.mean(axis=1)


# ── Consistency Score ──────────────────────────────────────────────────────────

def consistency_score(df: pd.DataFrame) -> pd.Series:
    """Internal accounting consistency checks."""
    checks = []

    rev = pd.to_numeric(df['revenue'], errors='coerce') if 'revenue' in df.columns else None
    ta  = pd.to_numeric(df['total_assets'], errors='coerce') if 'total_assets' in df.columns else None
    te  = pd.to_numeric(df['total_equity'], errors='coerce') if 'total_equity' in df.columns else None
    ni  = pd.to_numeric(df['net_income'], errors='coerce') if 'net_income' in df.columns else None
    gp  = pd.to_numeric(df['gross_profit'], errors='coerce') if 'gross_profit' in df.columns else None
    ocf = pd.to_numeric(df['operating_cash_flow'], errors='coerce') if 'operating_cash_flow' in df.columns else None

    # a. total_assets > 0
    if ta is not None:
        checks.append((ta > 0).fillna(False).astype(float))

    # b. |total_equity| <= total_assets * 1.5 (when both available)
    if ta is not None and te is not None:
        ok = (ta.notna() & te.notna() & (ta > 0) & (te.abs() <= ta * 1.5))
        # If either is null, don't penalise — absence of data is handled by coverage
        ok = ok | ta.isna() | te.isna()
        checks.append(ok.astype(float))

    # c. revenue > 0
    if rev is not None:
        checks.append((rev > 0).fillna(False).astype(float))

    # d. |net_income| < revenue * 3
    if rev is not None and ni is not None:
        ok = (rev.notna() & ni.notna() & (rev > 0) & (ni.abs() < rev * 3))
        ok = ok | rev.isna() | ni.isna() | (rev <= 0)
        checks.append(ok.astype(float))

    # e. operating_cash_flow not null when revenue > 0
    if rev is not None and ocf is not None:
        has_rev = rev.notna() & (rev > 0)
        has_ocf = ocf.notna()
        ok = (~has_rev) | has_ocf  # only penalise if has revenue but missing OCF
        checks.append(ok.astype(float))

    # f. gross_profit >= net_income (net profit can't exceed gross profit)
    if gp is not None and ni is not None:
        ok = (gp.notna() & ni.notna() & (gp >= ni)) | gp.isna() | ni.isna()
        checks.append(ok.astype(float))

    if not checks:
        return pd.Series(0.5, index=df.index)

    stacked = pd.concat(checks, axis=1)
    return stacked.mean(axis=1)


# ── Timeliness Score ───────────────────────────────────────────────────────────

def timeliness_score(df: pd.DataFrame) -> pd.Series:
    """Score based on filing lag and fiscal year vintage."""
    score = pd.Series(1.0, index=df.index)

    # Filing lag penalty
    if 'filing_lag_days' in df.columns:
        lag = pd.to_numeric(df['filing_lag_days'], errors='coerce').fillna(90)
        # 0–60 days: 1.0; 60–180: linear decay; 180+: 0.5
        lag_score = pd.Series(1.0, index=df.index)
        mask_mid = (lag > 60) & (lag <= 180)
        mask_late = lag > 180
        lag_score[mask_mid] = 1.0 - 0.3 * (lag[mask_mid] - 60) / 120
        lag_score[mask_late] = 0.5
        # Negative lag (non-Dec fiscal year) is fine
        lag_score[lag < 0] = 1.0
        score = score * lag_score

    # Fiscal year vintage
    if 'fiscal_year' in df.columns:
        fy = pd.to_numeric(df['fiscal_year'], errors='coerce').fillna(2015)
        vintage = pd.Series(1.0, index=df.index)
        vintage[fy < 2012] = 0.7
        vintage[(fy >= 2012) & (fy < 2018)] = 0.85
        # 2018+ stays at 1.0
        score = score * vintage

    return score.clip(0.0, 1.0)


# ── Main ───────────────────────────────────────────────────────────────────────

def build_confidence(df: pd.DataFrame) -> pd.Series:
    cov  = coverage_score(df)
    cons = consistency_score(df)
    time = timeliness_score(df)
    composite = (cov + cons + time) / 3.0
    return composite.clip(0.0, 1.0).round(4)


def run(dry_run: bool = False) -> None:
    if not IN_OUT.exists():
        print(f'ERROR: {IN_OUT} not found — run step6 + P0c/P0d/P0f first')
        sys.exit(1)

    print('P0g — Data Confidence Score')
    df = pd.read_parquet(IN_OUT)
    print(f'  Loaded {len(df):,} rows × {len(df.columns)} columns')

    print('  Computing coverage score...')
    cov  = coverage_score(df)
    print('  Computing consistency score...')
    cons = consistency_score(df)
    print('  Computing timeliness score...')
    time = timeliness_score(df)

    df['data_confidence'] = ((cov + cons + time) / 3.0).clip(0.0, 1.0).round(4)

    # Report
    s = df['data_confidence']
    print(f'\n  Data Confidence Distribution:')
    print(f'    mean: {s.mean():.3f}  std: {s.std():.3f}')
    print(f'    p10:  {s.quantile(0.10):.3f}  p25: {s.quantile(0.25):.3f}  '
          f'p50: {s.quantile(0.50):.3f}  p75: {s.quantile(0.75):.3f}  p90: {s.quantile(0.90):.3f}')

    buckets = [(0, 0.40), (0.40, 0.55), (0.55, 0.70), (0.70, 0.85), (0.85, 1.001)]
    labels  = ['< 0.40', '0.40–0.55', '0.55–0.70', '0.70–0.85', '≥ 0.85']
    print(f'\n  Score buckets:')
    for (lo, hi), lbl in zip(buckets, labels):
        n = ((s >= lo) & (s < hi)).sum()
        pct = 100 * n / len(df)
        print(f'    {lbl:<12s}: {n:>8,} rows ({pct:>5.1f}%)')

    # By market
    if 'market' in df.columns:
        print(f'\n  Mean confidence by market:')
        for mkt, grp in df.groupby('market'):
            print(f'    {mkt:<5s}: {grp["data_confidence"].mean():.3f}  (n={len(grp):,})')

    # In-universe vs excluded
    if 'in_universe' in df.columns:
        in_u  = df[df['in_universe'] == 1]['data_confidence'].mean()
        excl  = df[df['in_universe'] == 0]['data_confidence'].mean()
        print(f'\n  Mean confidence: in_universe={in_u:.3f}  excluded={excl:.3f}')

    # Component scores
    print(f'\n  Component means:')
    print(f'    coverage:    {cov.mean():.3f}')
    print(f'    consistency: {cons.mean():.3f}')
    print(f'    timeliness:  {time.mean():.3f}')

    if dry_run:
        print('\n  [DRY RUN] — file not modified')
        return

    df.to_parquet(IN_OUT, index=False)
    print(f'\n  Saved: {IN_OUT}')
    print(f'  Column added: data_confidence')


def main() -> None:
    parser = argparse.ArgumentParser(description='Data confidence score (P0g)')
    parser.add_argument('--dry-run', action='store_true', help='Print stats without saving')
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
