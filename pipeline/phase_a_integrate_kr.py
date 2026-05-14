"""
Phase A — Integrate Korea (KR) market data into historical_dataset_clean.parquet

KR snapshots (data/snapshots_kr.parquet) have 3,396 rows but 0 appear in the
clean dataset because step3 (price enrichment) was never run for KR tickers.

This script:
  1. Loads data/snapshots_kr.parquet (35 cols) and standardises column names
  2. Runs step3 price enrichment for KR tickers -> data/prices_kr.parquet
  3. Merges KR snapshots + prices + macro
  4. Applies all step5 feature functions
  5. Applies P0a (filing_lag_days, as_of_date)
  6. Applies P0c (fraud_confirmed, fraud_suspect)
  7. Applies P0d (fraud taxonomy sub-scores)
  8. Applies P0f (universe classification)
  9. Applies P0g (data confidence score)
 10. Aligns to the 319-column schema of historical_dataset_clean.parquet
 11. Concatenates and saves in-place

Usage:
    python3 pipeline/phase_a_integrate_kr.py
    python3 pipeline/phase_a_integrate_kr.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

BASE  = Path(__file__).parent.parent
DATA  = BASE / 'data'
PIPE  = BASE / 'pipeline'

sys.path.insert(0, str(BASE))

# ── Imports from pipeline ──────────────────────────────────────────────────────

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


# ── Constants ──────────────────────────────────────────────────────────────────

KR_SNAPSHOTS  = DATA / 'snapshots_kr.parquet'
KR_PRICES     = DATA / 'prices_kr.parquet'
KR_STD        = DATA / 'snapshots_kr_std.parquet'
MACRO         = DATA / 'macro.parquet'
CLEAN         = DATA / 'historical_dataset_clean.parquet'

MERGE_KEYS = ['cik', 'ticker', 'filed_date', 'fiscal_year', 'fiscal_quarter', 'period_type']

# step5 growth column aliases (same as in step5 run())
COLUMN_ALIASES = {
    'equity':                  'total_equity',
    'receivables':             'accounts_receivable',
    'revenue_growth_yoy':      'revenue_growth',
    'net_income_growth_yoy':   'net_income_growth',
    'asset_growth_yoy':        'assets_growth',
    'debt_growth_yoy':         'debt_growth',
    'receivables_growth_yoy':  'receivables_growth',
    'inventory_growth_yoy':    'inventory_growth',
    'ap_growth_yoy':           'ap_growth',
    'ocf_growth_yoy':          'ocf_growth',
    'capex_growth_yoy':        'capex_growth',
    'gross_profit_growth_yoy': 'gross_profit_growth',
    'sga_growth_yoy':          'sga_growth',
    'rd_growth_yoy':           'rd_growth',
    'eps_growth_yoy':          'eps_growth',
    'equity_change_yoy':       'equity_growth',
    'ppe_growth_yoy':          'ppe_growth',
    'cash_change_yoy':         'cash_growth',
    'cogs_growth_yoy':         'cogs_growth',
    'shares_dilution':         'shares_growth',
    'shares_outstanding':      'common_shares_outstanding',
}


# ── Step 1: Standardise KR columns ────────────────────────────────────────────

def standardise_kr_snapshots(df: pd.DataFrame) -> pd.DataFrame:
    """Add missing columns and aliases so step5 functions can run."""
    df = df.copy()

    # KR-specific column aliases
    if 'cfo' in df.columns and 'operating_cash_flow' not in df.columns:
        df['operating_cash_flow'] = df['cfo']

    if 'depreciation_amortization' in df.columns and 'depreciation' not in df.columns:
        df['depreciation'] = df['depreciation_amortization']

    if 'sga' in df.columns and 'sga_expense' not in df.columns:
        df['sga_expense'] = df['sga']

    if 'total_equity' in df.columns and 'equity' not in df.columns:
        df['equity'] = df['total_equity']

    if 'receivables' in df.columns and 'accounts_receivable' not in df.columns:
        df['accounts_receivable'] = df['receivables']

    # SIC codes absent for KR — set to NaN
    if 'sic_code' not in df.columns:
        df['sic_code'] = np.nan
    if 'sic_description' not in df.columns:
        df['sic_description'] = ''

    # Columns expected by step5 but absent from KR snapshots
    for col in ['short_term_debt', 'long_term_debt', 'total_liabilities',
                'non_operating_income', 'pretax_income', 'interest_expense',
                'tax_expense', 'eps_diluted', 'eps_basic', 'ppe_net', 'ppe_gross',
                'intangibles', 'goodwill', 'other_noncurrent_assets',
                'accounts_payable', 'dividends_per_share', 'financing_cash_flow',
                'cogs', 'cfi', 'fcf', 'stock_code', 'currency']:
        if col not in df.columns:
            df[col] = np.nan

    # Ensure total_debt has a value (already present in KR snapshots)
    if 'total_debt' in df.columns:
        if 'short_term_debt' not in df.columns or df['short_term_debt'].isna().all():
            df['short_term_debt'] = df['total_debt'] * 0.3  # rough split
        if 'long_term_debt' not in df.columns or df['long_term_debt'].isna().all():
            df['long_term_debt'] = df['total_debt'] * 0.7

    # Apply COLUMN_ALIASES (growth columns)
    for src, tgt in COLUMN_ALIASES.items():
        if src in df.columns and tgt not in df.columns:
            df[tgt] = df[src]

    return df


# ── Step 2: Run step3 price enrichment ────────────────────────────────────────

def run_step3_for_kr(kr_std: pd.DataFrame) -> pd.DataFrame:
    """
    Call step3_enrich_prices with KR snapshots, return price-enriched rows.
    Uses custom snapshots_path / out_path to avoid touching the US data.
    """
    # Save standardised KR snapshots for step3
    kr_std.to_parquet(KR_STD, index=False)
    print(f'  Saved standardised KR snapshots: {KR_STD} ({len(kr_std):,} rows)')

    # Import step3 and call with custom paths
    import pipeline.step3_enrich_prices as step3
    print('  Running step3 price enrichment for KR tickers...')
    step3.run(snapshots_path=str(KR_STD), out_path=str(KR_PRICES))

    if not KR_PRICES.exists():
        print('  WARN: step3 produced no output — prices_kr.parquet missing')
        return pd.DataFrame()

    prices = pd.read_parquet(KR_PRICES)
    print(f'  Loaded KR prices: {len(prices):,} rows × {len(prices.columns)} cols')
    return prices


# ── Step 3: Merge snapshots + prices + macro ──────────────────────────────────

def merge_kr_data(kr: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """Merge KR snapshots with price data (left join — keep all KR rows)."""
    if prices.empty:
        print('  WARN: No price data — KR rows will have NaN price/return columns')
        merged = kr.copy()
    else:
        # Determine which merge keys are available
        price_cols = set(prices.columns)
        avail_keys = [k for k in MERGE_KEYS if k in kr.columns and k in price_cols]
        print(f'  Merging on keys: {avail_keys}')

        # Price columns to add (avoid duplicating snapshot columns)
        snap_cols = set(kr.columns)
        price_only_cols = [c for c in prices.columns if c not in snap_cols or c in avail_keys]
        prices_slim = prices[price_only_cols]

        merged = kr.merge(prices_slim, on=avail_keys, how='left', suffixes=('', '_price'))

    # Merge macro
    if MACRO.exists():
        macro = pd.read_parquet(MACRO)
        macro_avail_keys = [k for k in MERGE_KEYS if k in merged.columns and k in macro.columns]
        macro_only_cols = [c for c in macro.columns if c not in set(merged.columns) or c in macro_avail_keys]
        macro_slim = macro[macro_only_cols]
        merged = merged.merge(macro_slim, on=macro_avail_keys, how='left', suffixes=('', '_macro'))
        print(f'  Macro merged: {len(merged):,} rows after merge')
    else:
        print('  WARN: macro.parquet not found — macro columns will be NaN')

    return merged


# ── Step 4: Apply P0a (filing_lag_days, as_of_date) ───────────────────────────

def apply_p0a(df: pd.DataFrame) -> pd.DataFrame:
    """Compute filing_lag_days = days from FY Dec-31 to filed_date."""
    df = df.copy()
    if 'as_of_date' not in df.columns:
        df['as_of_date'] = df.get('filed_date', pd.NaT)

    if 'filing_lag_days' not in df.columns and 'filed_date' in df.columns and 'fiscal_year' in df.columns:
        fd = pd.to_datetime(df['filed_date'], errors='coerce')
        fy = pd.to_numeric(df['fiscal_year'], errors='coerce').fillna(0).astype(int)
        fy_end = pd.to_datetime(fy.astype(str) + '-12-31', errors='coerce')
        df['filing_lag_days'] = (fd - fy_end).dt.days

    return df


# ── Main ───────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False) -> None:
    print('Phase A — Integrate Korea (KR) market data')
    print('=' * 60)

    # Validate inputs
    if not KR_SNAPSHOTS.exists():
        print(f'ERROR: {KR_SNAPSHOTS} not found')
        sys.exit(1)
    if not CLEAN.exists():
        print(f'ERROR: {CLEAN} not found — run full pipeline first')
        sys.exit(1)

    # ── Load existing clean dataset ────────────────────────────────────────────
    print('\n[1/9] Loading existing clean dataset...')
    existing = pd.read_parquet(CLEAN)
    print(f'  Existing: {len(existing):,} rows × {len(existing.columns)} cols')
    target_cols = list(existing.columns)

    # Check if KR already integrated
    if 'market' in existing.columns:
        kr_existing = (existing['market'] == 'KR').sum()
        if kr_existing > 0:
            print(f'  NOTE: {kr_existing:,} KR rows already present — will be replaced')
            existing = existing[existing['market'] != 'KR'].copy()
            print(f'  After removing old KR rows: {len(existing):,} rows')

    # ── Load and standardise KR snapshots ─────────────────────────────────────
    print('\n[2/9] Loading and standardising KR snapshots...')
    kr_raw = pd.read_parquet(KR_SNAPSHOTS)
    print(f'  KR raw: {len(kr_raw):,} rows × {len(kr_raw.columns)} cols')
    print(f'  Annual rows: {(kr_raw.get("period_type","") == "annual").sum():,}')

    kr = standardise_kr_snapshots(kr_raw)
    print(f'  After standardisation: {len(kr.columns)} cols')

    # ── Price enrichment ──────────────────────────────────────────────────────
    print('\n[3/9] Running step3 price enrichment for KR...')
    try:
        prices = run_step3_for_kr(kr)
    except Exception as e:
        print(f'  WARN: step3 failed ({e}) — continuing without price data')
        prices = pd.DataFrame()

    # ── Merge data ────────────────────────────────────────────────────────────
    print('\n[4/9] Merging snapshots + prices + macro...')
    df = merge_kr_data(kr, prices)
    print(f'  Merged: {len(df):,} rows × {len(df.columns)} cols')

    # ── Apply P0a ─────────────────────────────────────────────────────────────
    print('\n[5/9] Applying P0a (filing_lag_days, as_of_date)...')
    df = apply_p0a(df)

    # ── Apply step5 feature functions ─────────────────────────────────────────
    print('\n[6/9] Computing step5 features...')
    for fn_name, fn in [
        ('add_valuation',        add_valuation),
        ('add_profitability',    add_profitability),
        ('add_accruals',         add_accruals),
        ('add_fraud_scores',     add_fraud_scores),
        ('add_liquidity',        add_liquidity),
        ('add_composite_scores', add_composite_scores),
        ('add_size_features',    add_size_features),
        ('add_interactions',     add_interactions),
        ('add_sector_percentiles', add_sector_percentiles),
        ('add_macro_interactions', add_macro_interactions),
    ]:
        try:
            df = fn(df)
            print(f'  {fn_name}: OK')
        except Exception as e:
            print(f'  {fn_name}: WARN ({e})')

    # ── Apply P0c fraud labels ────────────────────────────────────────────────
    print('\n[7/9] Applying P0c fraud labels...')
    try:
        aaer_records = get_aaer_records(use_cache=True)
        df['fraud_confirmed'] = build_fraud_confirmed(df, aaer_records)
        df['fraud_suspect']   = build_fraud_suspect(df)
        df.loc[df['fraud_confirmed'] == 1, 'fraud_suspect'] = 0
        n_conf = int(df['fraud_confirmed'].sum())
        n_susp = int(df['fraud_suspect'].sum())
        print(f'  fraud_confirmed: {n_conf}  fraud_suspect: {n_susp}')
    except Exception as e:
        print(f'  WARN: P0c failed ({e}) — defaulting to 0')
        df['fraud_confirmed'] = 0
        df['fraud_suspect']   = 0

    # ── Apply P0d fraud taxonomy ───────────────────────────────────────────────
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

    # ── Apply P0f universe classification ────────────────────────────────────
    print('\n[8/9] Applying P0f universe classification...')
    try:
        df = classify_universe(df, apply_filters=False)
        in_u = int(df['in_universe'].sum())
        print(f'  in_universe=1: {in_u:,} / {len(df):,} rows')
    except Exception as e:
        print(f'  WARN: P0f failed ({e})')
        df['in_universe'] = 0
        df['excl_reason'] = 'p0f_error'

    # ── Apply P0g data confidence ─────────────────────────────────────────────
    print('\n[8b/9] Applying P0g data confidence score...')
    try:
        df['data_confidence'] = build_confidence(df)
        print(f'  data_confidence mean: {df["data_confidence"].mean():.3f}')
    except Exception as e:
        print(f'  WARN: P0g failed ({e})')
        df['data_confidence'] = np.nan

    # ── Align to target schema ────────────────────────────────────────────────
    print('\n[9/9] Aligning KR rows to 319-column schema...')
    for col in target_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Keep only columns in target schema (in target order)
    kr_aligned = df[target_cols].copy()
    print(f'  KR aligned: {len(kr_aligned):,} rows × {len(kr_aligned.columns)} cols')
    # ── Stats ──────────────────────────────────────────────────────────────────
    annual_kr = (kr_aligned['period_type'] == 'annual').sum() if 'period_type' in kr_aligned.columns else 0
    print(f'\n  KR rows summary:')
    print(f'    Total rows: {len(kr_aligned):,}')
    print(f'    Annual rows: {annual_kr:,}')
    if 'entry_price' in kr_aligned.columns:
        n_price = kr_aligned['entry_price'].notna().sum()
        print(f'    Rows with entry_price: {n_price:,}')
    if 'beneish_m_score' in kr_aligned.columns:
        n_beneish = kr_aligned['beneish_m_score'].notna().sum()
        print(f'    Rows with beneish_m_score: {n_beneish:,}')

    if dry_run:
        print('\n  [DRY RUN] — dataset not modified')
        return

    # ── Concatenate and save ──────────────────────────────────────────────────
    print('\n  Concatenating...')
    combined = pd.concat([existing, kr_aligned], ignore_index=True)
    print(f'  Combined: {len(combined):,} rows × {len(combined.columns)} cols')

    # Verify market distribution
    if 'market' in combined.columns:
        print('\n  Row count by market:')
        for mkt, cnt in combined['market'].value_counts().items():
            print(f'    {mkt}: {cnt:,}')

    # Fix date column dtypes before saving (KR rows may have string dates)
    for date_col in ['filed_date', 'as_of_date']:
        if date_col in combined.columns:
            combined[date_col] = pd.to_datetime(combined[date_col], errors='coerce')

    combined.to_parquet(CLEAN, index=False)
    print(f'\n  Saved: {CLEAN}')
    print('Phase A complete.')


def main() -> None:
    parser = argparse.ArgumentParser(description='Integrate KR market data (Phase A)')
    parser.add_argument('--dry-run', action='store_true', help='Stats only, do not save')
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
