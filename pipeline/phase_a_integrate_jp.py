"""
Phase A — Integrate Japan (JP) market data into historical_dataset_clean.parquet

JP snapshots (data/snapshots_jp.parquet) are built via yfinance free-tier (~130
major TSE tickers). For full TSE coverage use the EDINET-based pipeline.

This script:
  1. Loads data/snapshots_jp.parquet and standardises column names
  2. Runs step3 price enrichment for JP tickers -> data/prices_jp.parquet
  3. Merges JP snapshots + prices + macro
  4. Applies all step5 feature functions
  5. Applies P0a (filing_lag_days, as_of_date)
  6. Applies P0c (fraud_confirmed, fraud_suspect)
  7. Applies P0d (fraud taxonomy sub-scores)
  8. Applies P0f (universe classification)
  9. Applies P0g (data confidence score)
 10. Aligns to the target schema of historical_dataset_clean.parquet
 11. Concatenates and saves in-place

Usage:
    python3 pipeline/phase_a_integrate_jp.py
    python3 pipeline/phase_a_integrate_jp.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE  = Path(__file__).parent.parent
DATA  = BASE / 'data'

sys.path.insert(0, str(BASE))

from pipeline.step5_compute_features import (
    add_valuation, add_profitability, add_accruals, add_fraud_scores,
    add_liquidity, add_composite_scores, add_size_features,
    add_interactions, add_sector_percentiles, add_macro_interactions,
)
from pipeline.enrich_fraud_labels import (
    build_fraud_confirmed, build_fraud_suspect, get_aaer_records,
)
from pipeline.enrich_fraud_taxonomy import (
    build_accounting_score, build_dilution_score, build_quality_score,
    build_distress_score, build_governance_score, build_composite_fraud_score,
)
from pipeline.p0f_universe_definition import classify_universe
from pipeline.p0g_confidence_score import build_confidence


JP_SNAPSHOTS = DATA / 'snapshots_jp.parquet'
JP_PRICES    = DATA / 'prices_jp.parquet'
JP_STD       = DATA / 'snapshots_jp_std.parquet'
MACRO        = DATA / 'macro_jp.parquet'
MACRO_FALLBACK = DATA / 'macro.parquet'
CLEAN        = DATA / 'historical_dataset_clean.parquet'

MERGE_KEYS = ['cik', 'ticker', 'filed_date', 'fiscal_year', 'fiscal_quarter', 'period_type']


def standardise_jp_snapshots(df: pd.DataFrame) -> pd.DataFrame:
    """Add missing columns and aliases so step5 functions can run on JP data."""
    df = df.copy()

    # Depreciation alias (yfinance uses depreciation_amortization)
    if 'depreciation_amortization' in df.columns and 'depreciation' not in df.columns:
        df['depreciation'] = df['depreciation_amortization']

    # sga alias
    if 'sga_expense' in df.columns and 'sga' not in df.columns:
        df['sga'] = df['sga_expense']

    # accounts_receivable alias (yfinance uses receivables)
    if 'receivables' in df.columns and 'accounts_receivable' not in df.columns:
        df['accounts_receivable'] = df['receivables']

    # total_equity alias (yfinance uses equity)
    if 'equity' in df.columns and 'total_equity' not in df.columns:
        df['total_equity'] = df['equity']

    # total_debt
    if 'total_debt' not in df.columns:
        lt = df.get('long_term_debt', pd.Series(0, index=df.index)).fillna(0)
        st = df.get('short_term_debt', pd.Series(0, index=df.index)).fillna(0)
        df['total_debt'] = lt + st

    # SIC codes absent for JP
    if 'sic_code' not in df.columns:
        df['sic_code'] = np.nan
    if 'sic_description' not in df.columns:
        df['sic_description'] = ''

    # Columns expected by step5 but potentially absent
    for col in ['ppe_gross', 'other_noncurrent_assets', 'non_operating_income',
                'dividends_per_share', 'stock_code', 'currency',
                'financing_cash_flow', 'total_liabilities']:
        if col not in df.columns:
            df[col] = np.nan

    # total_liabilities: estimate from total_assets - equity if missing
    if df.get('total_liabilities', pd.Series(dtype=float)).isna().all():
        if 'total_assets' in df.columns and 'equity' in df.columns:
            df['total_liabilities'] = df['total_assets'].fillna(0) - df['equity'].fillna(0)

    return df


def run_step3_for_jp(jp_std: pd.DataFrame) -> pd.DataFrame:
    jp_std.to_parquet(JP_STD, index=False)
    print(f'  Saved standardised JP snapshots: {JP_STD} ({len(jp_std):,} rows)')

    import pipeline.step3_enrich_prices as step3
    print('  Running step3 price enrichment for JP tickers...')
    step3.run(snapshots_path=str(JP_STD), out_path=str(JP_PRICES))

    if not JP_PRICES.exists():
        print('  WARN: step3 produced no output — prices_jp.parquet missing')
        return pd.DataFrame()

    prices = pd.read_parquet(JP_PRICES)
    print(f'  Loaded JP prices: {len(prices):,} rows × {len(prices.columns)} cols')
    return prices


def merge_jp_data(jp: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    macro_path = MACRO if MACRO.exists() else MACRO_FALLBACK

    if prices.empty:
        print('  WARN: No price data — JP rows will have NaN price/return columns')
        merged = jp.copy()
    else:
        price_cols = set(prices.columns)
        avail_keys = [k for k in MERGE_KEYS if k in jp.columns and k in price_cols]
        print(f'  Merging on keys: {avail_keys}')
        snap_cols = set(jp.columns)
        price_only_cols = [c for c in prices.columns if c not in snap_cols or c in avail_keys]
        merged = jp.merge(prices[price_only_cols], on=avail_keys, how='left', suffixes=('', '_price'))

    if macro_path.exists():
        macro = pd.read_parquet(macro_path)
        macro_avail_keys = [k for k in MERGE_KEYS if k in merged.columns and k in macro.columns]
        macro_only_cols = [c for c in macro.columns if c not in set(merged.columns) or c in macro_avail_keys]
        merged = merged.merge(macro[macro_only_cols], on=macro_avail_keys, how='left', suffixes=('', '_macro'))
        print(f'  Macro merged ({macro_path.name}): {len(merged):,} rows')
    else:
        print('  WARN: No macro parquet found — macro columns will be NaN')

    return merged


def apply_p0a(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'as_of_date' not in df.columns:
        df['as_of_date'] = df.get('filed_date', pd.NaT)

    if 'filing_lag_days' not in df.columns and 'filed_date' in df.columns and 'fiscal_year' in df.columns:
        fd = pd.to_datetime(df['filed_date'], errors='coerce')
        fy = pd.to_numeric(df['fiscal_year'], errors='coerce').fillna(0).astype(int)
        fy_end = pd.to_datetime(fy.astype(str) + '-12-31', errors='coerce')
        df['filing_lag_days'] = (fd - fy_end).dt.days

    return df


def run(dry_run: bool = False) -> None:
    print('Phase A — Integrate Japan (JP) market data')
    print('=' * 60)

    if not JP_SNAPSHOTS.exists():
        print(f'ERROR: {JP_SNAPSHOTS} not found — run run_pipeline_jp.py build first')
        sys.exit(1)
    if not CLEAN.exists():
        print(f'ERROR: {CLEAN} not found — run full US pipeline first')
        sys.exit(1)

    print('\n[1/9] Loading existing clean dataset...')
    existing = pd.read_parquet(CLEAN)
    print(f'  Existing: {len(existing):,} rows × {len(existing.columns)} cols')
    target_cols = list(existing.columns)

    if 'market' in existing.columns:
        jp_existing = (existing['market'] == 'JP').sum()
        if jp_existing > 0:
            print(f'  NOTE: {jp_existing:,} JP rows already present — will be replaced')
            existing = existing[existing['market'] != 'JP'].copy()

    print('\n[2/9] Loading and standardising JP snapshots...')
    jp_raw = pd.read_parquet(JP_SNAPSHOTS)
    print(f'  JP raw: {len(jp_raw):,} rows × {len(jp_raw.columns)} cols')
    print(f'  Annual rows: {(jp_raw.get("period_type", pd.Series(dtype=str)) == "annual").sum():,}')

    jp = standardise_jp_snapshots(jp_raw)
    print(f'  After standardisation: {len(jp.columns)} cols')

    print('\n[3/9] Running step3 price enrichment for JP...')
    try:
        prices = run_step3_for_jp(jp)
    except Exception as e:
        print(f'  WARN: step3 failed ({e}) — continuing without price data')
        prices = pd.DataFrame()

    print('\n[4/9] Merging snapshots + prices + macro...')
    df = merge_jp_data(jp, prices)
    print(f'  Merged: {len(df):,} rows × {len(df.columns)} cols')

    print('\n[5/9] Applying P0a (filing_lag_days, as_of_date)...')
    df = apply_p0a(df)

    print('\n[6/9] Computing step5 features...')
    for fn_name, fn in [
        ('add_valuation',         add_valuation),
        ('add_profitability',     add_profitability),
        ('add_accruals',          add_accruals),
        ('add_fraud_scores',      add_fraud_scores),
        ('add_liquidity',         add_liquidity),
        ('add_composite_scores',  add_composite_scores),
        ('add_size_features',     add_size_features),
        ('add_interactions',      add_interactions),
        ('add_sector_percentiles',add_sector_percentiles),
        ('add_macro_interactions',add_macro_interactions),
    ]:
        try:
            df = fn(df)
            print(f'  {fn_name}: OK')
        except Exception as e:
            print(f'  {fn_name}: WARN ({e})')

    print('\n[7/9] Applying P0c fraud labels...')
    try:
        aaer_records = get_aaer_records(use_cache=True)
        df['fraud_confirmed'] = build_fraud_confirmed(df, aaer_records)
        df['fraud_suspect']   = build_fraud_suspect(df)
        df.loc[df['fraud_confirmed'] == 1, 'fraud_suspect'] = 0
        print(f'  fraud_confirmed: {int(df["fraud_confirmed"].sum())}  fraud_suspect: {int(df["fraud_suspect"].sum())}')
    except Exception as e:
        print(f'  WARN: P0c failed ({e}) — defaulting to 0')
        df['fraud_confirmed'] = 0
        df['fraud_suspect']   = 0

    print('\n[7b/9] Applying P0d fraud taxonomy sub-scores...')
    for score_name, builder in [
        ('fraud_score_accounting', build_accounting_score),
        ('fraud_score_dilution',   build_dilution_score),
        ('fraud_score_quality',    build_quality_score),
        ('fraud_score_distress',   build_distress_score),
        ('fraud_score_governance', build_governance_score),
    ]:
        try:
            df[score_name] = builder(df)
        except Exception as e:
            print(f'  WARN: {score_name} failed ({e})')
            df[score_name] = np.nan

    try:
        df['fraud_score_composite'] = build_composite_fraud_score(df)
    except Exception as e:
        print(f'  WARN: fraud_score_composite failed ({e})')
        df['fraud_score_composite'] = np.nan

    print('\n[8/9] Applying P0f universe classification...')
    try:
        df = classify_universe(df, apply_filters=False)
        print(f'  in_universe=1: {int(df["in_universe"].sum()):,} / {len(df):,} rows')
    except Exception as e:
        print(f'  WARN: P0f failed ({e})')
        df['in_universe'] = 0
        df['excl_reason'] = 'p0f_error'

    print('\n[8b/9] Applying P0g data confidence score...')
    try:
        df['data_confidence'] = build_confidence(df)
        print(f'  data_confidence mean: {df["data_confidence"].mean():.3f}')
    except Exception as e:
        print(f'  WARN: P0g failed ({e})')
        df['data_confidence'] = np.nan

    print('\n[9/9] Aligning JP rows to target schema...')
    for col in target_cols:
        if col not in df.columns:
            df[col] = np.nan
    jp_aligned = df[target_cols].copy()
    print(f'  JP aligned: {len(jp_aligned):,} rows × {len(jp_aligned.columns)} cols')

    annual_jp = (jp_aligned['period_type'] == 'annual').sum() if 'period_type' in jp_aligned.columns else 0
    print(f'\n  JP rows summary:')
    print(f'    Total rows:  {len(jp_aligned):,}')
    print(f'    Annual rows: {annual_jp:,}')
    if 'entry_price' in jp_aligned.columns:
        print(f'    Rows with entry_price: {jp_aligned["entry_price"].notna().sum():,}')
    if 'beneish_m_score' in jp_aligned.columns:
        print(f'    Rows with beneish_m_score: {jp_aligned["beneish_m_score"].notna().sum():,}')

    if dry_run:
        print('\n  [DRY RUN] — dataset not modified')
        return

    print('\n  Concatenating...')
    combined = pd.concat([existing, jp_aligned], ignore_index=True)
    print(f'  Combined: {len(combined):,} rows × {len(combined.columns)} cols')

    if 'market' in combined.columns:
        print('\n  Row count by market:')
        for mkt, cnt in combined['market'].value_counts().items():
            print(f'    {mkt}: {cnt:,}')

    for date_col in ['filed_date', 'as_of_date']:
        if date_col in combined.columns:
            combined[date_col] = pd.to_datetime(combined[date_col], errors='coerce')

    combined.to_parquet(CLEAN, index=False)
    print(f'\n  Saved: {CLEAN}')
    print('Phase A — JP complete.')


def main() -> None:
    parser = argparse.ArgumentParser(description='Integrate JP market data (Phase A)')
    parser.add_argument('--dry-run', action='store_true', help='Stats only, do not save')
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
