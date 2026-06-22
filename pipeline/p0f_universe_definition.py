"""
P0f — Universe Definition Framework

Defines and documents the investment universe for each market.
Applies consistent inclusion/exclusion filters to historical_dataset_clean.parquet
and writes a universe-tagged parquet + a universe summary CSV.

By default (no flags), only structural rules are applied to in_universe:
  1. period_type == 'annual'          (annual filings only for cross-sectional ML)
  2. fiscal_year >= 2009              (full XBRL coverage starts 2009)
  3. fiscal_year <= current_year - 1  (prior completed fiscal years only)

Pass --apply-filters to also enforce investable-universe rules:
  4. revenue >= $1M
  5. total_assets >= $100K
  6. entry_price > 0
  7. Exclude pure financial sector     (SIC 6000–6999)
  8. Exclude pure utility sector       (SIC 4900–4999)
  9. Size: include micro, small, mid, large (exclude nano/shell)
  10. Price floor:                      exclude if entry_price < market-specific floor (all exchanges)

Market-specific price floors (--apply-filters only):
  - US:    $1.00 (penny stock filter)
  - CA:    $0.05 (TSXV penny stocks common)
  - BR/JP/KR/EU: no price floor

Columns added:
  in_universe     — 1 if row passes all applied filters for its market, else 0
  excl_reason     — pipe-separated string of exclusion reasons (empty if in_universe=1)

Output:
  data/historical_dataset_clean.parquet  — updated in-place with in_universe + excl_reason
  reports/universe_summary.csv           — per-market row counts and exclusion breakdown

Usage:
    python3 pipeline/p0f_universe_definition.py               # structural rules only
    python3 pipeline/p0f_universe_definition.py --dry-run     # report without saving
    python3 pipeline/p0f_universe_definition.py --apply-filters          # full investable-universe rules
    python3 pipeline/p0f_universe_definition.py --apply-filters --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE    = Path(__file__).parent.parent
DATA    = BASE / 'data'
REPORTS = BASE / 'reports'
IN_OUT  = DATA / 'historical_dataset_clean.parquet'
SUMMARY = REPORTS / 'universe_summary.csv'
REPORTS.mkdir(exist_ok=True)

# ── Universe Rules ─────────────────────────────────────────────────────────────

ANNUAL_ONLY        = True
MIN_FISCAL_YEAR    = 2009
MAX_FISCAL_YEAR_LAG = 1   # exclude current year (filings may be incomplete)

EXCLUDE_FINANCIALS = True   # SIC 6000–6999
EXCLUDE_UTILITIES  = True   # SIC 4900–4999

# Per-market minimum price (penny stock filter — applied to all exchanges)
MARKET_MIN_PRICE = {
    'US': 1.00,
    'CA': 0.05,
    'BR': 0.0,
    'JP': 0.0,
    'KR': 0.0,
    'DE': 0.0,
    'FR': 0.0,
    'IT': 0.0,
    'ES': 0.0,
    'SE': 0.0,
    'FI': 0.0,
    'NL': 0.0,
    'PT': 0.0,
    'DK': 0.0,
}


def _get_current_year() -> int:
    from datetime import date
    return date.today().year


def classify_universe(df: pd.DataFrame, apply_filters: bool = False) -> pd.DataFrame:
    """
    Add in_universe (0/1) and excl_reason columns.

    Structural rules (always applied):
      - period_type == 'annual'
      - fiscal_year in [2009, current_year-1]

    Investable-universe rules (apply_filters=True only):
      - revenue >= $1M, total_assets >= $100K, entry_price > 0
      - price >= market floor (US: $1, CA: $0.05, all exchanges)
      - exclude SIC 6000-6999 (financials) and SIC 4900-4999 (utilities)
    """
    current_year = _get_current_year()
    max_fy = current_year - MAX_FISCAL_YEAR_LAG

    # Numeric coercion
    fy       = pd.to_numeric(df['fiscal_year'],  errors='coerce').fillna(0).astype(int)
    rev      = pd.to_numeric(df['revenue'],       errors='coerce') if 'revenue'      in df.columns else pd.Series(np.nan, index=df.index)
    ta       = pd.to_numeric(df['total_assets'],  errors='coerce') if 'total_assets' in df.columns else pd.Series(np.nan, index=df.index)
    price    = pd.to_numeric(df['entry_price'],   errors='coerce') if 'entry_price'  in df.columns else pd.Series(np.nan, index=df.index)
    sic      = pd.to_numeric(df['sic_code'],      errors='coerce') if 'sic_code'     in df.columns else pd.Series(np.nan, index=df.index)
    exchange = df['exchange'].fillna('') if 'exchange' in df.columns else pd.Series('', index=df.index)
    market   = df['market'].fillna('') if 'market' in df.columns else pd.Series('', index=df.index)
    ptype    = df['period_type'].fillna('') if 'period_type' in df.columns else pd.Series('', index=df.index)

    # Min price per market
    min_price = market.map(MARKET_MIN_PRICE).fillna(0.0)

    reasons = pd.Series([''] * len(df), index=df.index)

    def _flag(mask: pd.Series, reason: str) -> None:
        reasons[mask] = (reasons[mask] + reason + '|').str.lstrip('|')

    # 1. Annual only
    if ANNUAL_ONLY:
        _flag(ptype != 'annual', 'quarterly_row')

    # 2. Fiscal year range
    _flag(fy < MIN_FISCAL_YEAR, f'fy<{MIN_FISCAL_YEAR}')
    _flag(fy > max_fy, f'fy>{max_fy}(incomplete)')

    if apply_filters:
        # 3. Revenue floor
        _flag(rev.notna() & (rev < 1e6), 'revenue<1M')

        # 4. Total assets floor
        _flag(ta.notna() & (ta < 1e5), 'assets<100K')

        # 5. Price check
        _flag(price.isna() | (price <= 0), 'no_price')
        _flag(price.notna() & (price > 0) & (price < min_price), 'price_below_market_floor')

        # 6. Financials exclusion
        if EXCLUDE_FINANCIALS:
            fin_mask = sic.notna() & (sic >= 6000) & (sic <= 6999)
            _flag(fin_mask, 'financial_sector')

        # 7. Utilities exclusion
        if EXCLUDE_UTILITIES:
            util_mask = sic.notna() & (sic >= 4900) & (sic <= 4999)
            _flag(util_mask, 'utility_sector')

    # Clean up trailing pipes
    reasons = reasons.str.strip('|')

    df = df.copy()
    df['in_universe'] = (reasons == '').astype('int8')
    df['excl_reason'] = reasons
    return df


# ── Summary Report ─────────────────────────────────────────────────────────────

def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Per-market universe summary table."""
    rows = []
    for market in sorted(df['market'].unique()):
        mdf = df[df['market'] == market]
        total = len(mdf)
        annual = (mdf['period_type'] == 'annual').sum() if 'period_type' in mdf.columns else 0
        in_u   = int(mdf['in_universe'].sum())
        excl   = total - in_u

        # Top exclusion reasons
        excl_df = mdf[mdf['in_universe'] == 0]['excl_reason']
        reason_counts: dict[str, int] = {}
        for reasons in excl_df:
            for r in reasons.split('|'):
                r = r.strip()
                if r:
                    reason_counts[r] = reason_counts.get(r, 0) + 1
        top_reasons = ', '.join(f'{r}={c}' for r, c in sorted(reason_counts.items(), key=lambda x: -x[1])[:5])

        rows.append({
            'market':          market,
            'total_rows':      total,
            'annual_rows':     annual,
            'in_universe':     in_u,
            'excluded':        excl,
            'excl_pct':        round(100 * excl / total, 1) if total > 0 else 0,
            'top_excl_reasons': top_reasons,
        })
    return pd.DataFrame(rows)


# ── Main ───────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False, apply_filters: bool = False) -> None:
    if not IN_OUT.exists():
        print(f'ERROR: {IN_OUT} not found — run step6 + P0c/P0d first')
        sys.exit(1)

    print('P0f — Universe Definition Framework')
    if apply_filters:
        print('  Mode: full investable-universe filters (--apply-filters)')
    else:
        print('  Mode: structural rules only (pass --apply-filters for investable-universe subset)')
    df = pd.read_parquet(IN_OUT)
    print(f'  Loaded {len(df):,} rows × {len(df.columns)} columns')

    df = classify_universe(df, apply_filters=apply_filters)

    in_u = int(df['in_universe'].sum())
    total = len(df)
    print(f'\n  Universe classification:')
    print(f'    In universe:  {in_u:,} rows ({100*in_u/total:.1f}%)')
    print(f'    Excluded:     {total - in_u:,} rows')

    summary = build_summary(df)
    print(f'\n  Per-market breakdown:')
    print(summary[['market', 'total_rows', 'annual_rows', 'in_universe',
                    'excluded', 'excl_pct', 'top_excl_reasons']].to_string(index=False))

    # Fiscal year coverage for in-universe rows
    in_df = df[df['in_universe'] == 1]
    if 'fiscal_year' in in_df.columns:
        fy_counts = in_df.groupby('fiscal_year').size()
        print(f'\n  In-universe rows by fiscal year (2009–):')
        for fy, cnt in sorted(fy_counts.items()):
            bar = '█' * min(50, cnt // 100)
            print(f'    {int(fy)}: {cnt:>6,}  {bar}')

    if dry_run:
        print('\n  [DRY RUN] — file not modified')
        return

    df.to_parquet(IN_OUT, index=False)
    summary.to_csv(SUMMARY, index=False)
    print(f'\n  Saved: {IN_OUT}')
    print(f'  Saved: {SUMMARY}')
    print(f'  Columns added: in_universe, excl_reason')


def main() -> None:
    parser = argparse.ArgumentParser(description='Universe definition framework (P0f)')
    parser.add_argument('--dry-run', action='store_true', help='Print stats without saving')
    parser.add_argument('--apply-filters', action='store_true',
                        help='Apply investable-universe rules (revenue/assets/price/sector)')
    args = parser.parse_args()
    run(dry_run=args.dry_run, apply_filters=args.apply_filters)


if __name__ == '__main__':
    main()
