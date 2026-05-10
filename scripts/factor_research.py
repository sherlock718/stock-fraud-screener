"""
Factor research library — IC/ICIR analysis, factor turnover, IC t-statistics.

For each candidate feature:
  - IC per year (Spearman rank correlation with forward returns)
  - Mean IC, StdIC, ICIR = MeanIC / StdIC
  - IC t-statistic  = MeanIC * sqrt(N) / StdIC  (tests if IC is reliably != 0)
  - Percent positive IC years
  - Factor turnover (mean Spearman rank correlation between consecutive years)
    Low turnover = factor rankings are stable year to year (cheaper to trade)

Output: reports/factor_research_{horizon}.csv

Usage:
    python3 scripts/factor_research.py
    python3 scripts/factor_research.py --horizon 3y
    python3 scripts/factor_research.py --factors value_composite pe_ratio piotroski_f_score
    python3 scripts/factor_research.py --top 20        # show top 20 by |ICIR|
    python3 scripts/factor_research.py --min-icir 0.1  # filter output by |ICIR|
"""
from __future__ import annotations

import argparse
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats

BASE      = Path(__file__).parent.parent
DATA_PATH = BASE / 'data' / 'historical_dataset_clean.parquet'
REPORTS   = BASE / 'reports'
REPORTS.mkdir(exist_ok=True)

HORIZONS = {
    '1y': 'forward_return_1y',
    '3y': 'forward_return_3y',
    '5y': 'forward_return_5y',
}

EXCLUDE = {
    # identifiers & metadata
    'cik', 'ticker', 'name', 'filed_date', 'fiscal_year', 'fiscal_quarter',
    'period_type', 'exchange', 'sic_code', 'sic_description', 'market',
    'country', 'accounting_std', 'size_category_label', 'corp_code', 'acc_mt',
    # raw dollar amounts — size-contaminated; normalised versions used instead
    'revenue', 'net_income', 'gross_profit', 'operating_income', 'pretax_income',
    'cogs', 'sga_expense', 'rd_expense', 'depreciation', 'da_expense',
    'operating_cash_flow', 'financing_cash_flow', 'investing_cash_flow',
    'capex', 'fcf',
    'long_term_debt', 'short_term_debt', 'total_debt',
    'total_assets', 'total_equity', 'current_assets', 'current_liabilities',
    'accounts_receivable', 'accounts_payable', 'receivables',
    'cash', 'intangibles', 'goodwill', 'ppe_net', 'noa',
    'market_cap_at_filing', 'tax_expense', 'interest_expense',
    'common_shares_outstanding', 'eps_diluted', 'eps_basic',
    'retained_earnings', 'additional_paid_in_capital', 'inventory',
}
EXCLUDE_PATTERNS = ['forward_return', 'beat_local_market', 'excess_return_local',
                    'benchmark_return']

# Map SIC code ranges to ~10 broad sectors for neutralization
def _sic_to_sector(sic: pd.Series) -> pd.Series:
    s = pd.to_numeric(sic, errors='coerce').fillna(0).astype(int)
    sector = pd.Series('Other', index=s.index)
    sector[s.between(100,  999)]  = 'Agriculture/Mining'
    sector[s.between(1000, 1499)] = 'Mining/Resources'
    sector[s.between(1500, 1999)] = 'Construction'
    sector[s.between(2000, 3999)] = 'Manufacturing'
    sector[s.between(4000, 4999)] = 'Utilities/Transport'
    sector[s.between(5000, 5999)] = 'Trade'
    sector[s.between(6000, 6799)] = 'Finance/Insurance/RE'
    sector[s.between(7000, 7999)] = 'Services/Hospitality'
    sector[s.between(8000, 8999)] = 'Services/Professional'
    return sector


def _sector_demean(values: pd.Series, sectors: pd.Series) -> pd.Series:
    """Subtract sector median to remove sector-level factor effects."""
    result = values.copy().astype(float)
    for sec in sectors.unique():
        mask = (sectors == sec) & values.notna()
        if mask.sum() < 5:
            continue
        med = values[mask].median()
        if pd.notna(med):
            result[mask] = values[mask] - med
    return result


def _add_normalised_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Add normalised versions of raw dollar features to remove size contamination."""
    ta  = df.get('total_assets')
    pti = df.get('pretax_income')

    if ta is not None:
        ta_safe = ta.replace(0, np.nan)
        for src, dst in [
            ('intangibles',         'intangibles_to_assets'),
            ('goodwill',            'goodwill_to_assets'),
            ('depreciation',        'depreciation_to_assets'),
            ('financing_cash_flow', 'financing_cashflow_to_assets'),
            ('fcf',                 'fcf_to_assets'),
        ]:
            if src in df.columns and dst not in df.columns:
                df[dst] = df[src] / ta_safe

    if 'tax_expense' in df.columns and pti is not None and 'effective_tax_rate' not in df.columns:
        pos = pti > 0
        df['effective_tax_rate'] = np.nan
        df.loc[pos, 'effective_tax_rate'] = df.loc[pos, 'tax_expense'] / pti[pos]

    return df


def load_data() -> pd.DataFrame:
    df = pd.read_parquet(DATA_PATH)
    df = df[df['period_type'] == 'annual'].copy()
    df = df[df['fiscal_year'].between(2008, 2025)].copy()
    df = df.sort_values('total_assets', ascending=False, na_position='last')
    df = df.drop_duplicates(subset=['ticker', 'fiscal_year'], keep='first')
    for col in [c for c in df.columns if 'growth_yoy' in c]:
        lo, hi = df[col].quantile(0.01), df[col].quantile(0.99)
        df[col] = df[col].clip(lo, hi)
    df = _add_normalised_ratios(df)
    return df.reset_index(drop=True)


def get_candidates(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if c not in EXCLUDE
        and not any(p in c for p in EXCLUDE_PATTERNS)
        and df[c].dtype in [np.float64, np.float32, np.int64, np.int32, 'Int64']
        and df[c].notna().mean() > 0.10
    ]


def compute_ic_series(df: pd.DataFrame, feature: str, ret_col: str,
                      sector_neutral: bool = False) -> list[float]:
    """Return list of annual IC values for a single factor."""
    sub = df[df[ret_col].notna() & df[feature].notna()].copy()
    ics = []
    for yr in sorted(sub['fiscal_year'].unique()):
        grp = sub[sub['fiscal_year'] == yr].copy()
        if len(grp) < 30:
            continue
        feat_vals = grp[feature]
        if sector_neutral and 'sic_code' in grp.columns:
            sectors = _sic_to_sector(grp['sic_code'])
            feat_vals = _sector_demean(feat_vals, sectors)
        corr, _ = stats.spearmanr(feat_vals, grp[ret_col])
        if not np.isnan(corr):
            ics.append(corr)
    return ics


def compute_turnover(df: pd.DataFrame, feature: str) -> float | None:
    """Mean Spearman rank correlation of feature rankings between consecutive years.

    High turnover (low correlation) = rankings change a lot year to year → costly to trade.
    Low turnover (high correlation) = stable rankings → cheaper to maintain positions.
    """
    years = sorted(df['fiscal_year'].unique())
    corrs = []
    for i in range(len(years) - 1):
        y0, y1 = years[i], years[i + 1]
        grp0 = df[df['fiscal_year'] == y0][['ticker', feature]].dropna()
        grp1 = df[df['fiscal_year'] == y1][['ticker', feature]].dropna()
        merged = grp0.merge(grp1, on='ticker', suffixes=('_y0', '_y1'))
        if len(merged) < 30:
            continue
        c, _ = stats.spearmanr(merged[f'{feature}_y0'], merged[f'{feature}_y1'])
        if not np.isnan(c):
            corrs.append(c)
    return float(np.mean(corrs)) if corrs else None


def analyse_factor(df: pd.DataFrame, feature: str, ret_col: str,
                   sector_neutral: bool = False) -> dict:
    ics = compute_ic_series(df, feature, ret_col, sector_neutral=sector_neutral)
    if not ics:
        return None
    mean_ic = np.mean(ics)
    std_ic  = np.std(ics) + 1e-8
    n       = len(ics)
    icir    = mean_ic / std_ic
    ic_tstat = mean_ic * np.sqrt(n) / std_ic  # t-statistic for H0: mean_ic = 0
    turnover = compute_turnover(df, feature)
    return {
        'feature':         feature,
        'mean_ic':         round(mean_ic, 5),
        'std_ic':          round(std_ic, 5),
        'icir':            round(icir, 4),
        'ic_tstat':        round(ic_tstat, 3),
        'pct_positive_ic': round(np.mean([ic > 0 for ic in ics]), 3),
        'n_years':         n,
        'turnover':        round(turnover, 4) if turnover is not None else None,
        'ic_min':          round(min(ics), 4),
        'ic_max':          round(max(ics), 4),
    }


def print_table(rows: list[dict], top_n: int, min_icir: float) -> None:
    filtered = [r for r in rows if abs(r['icir']) >= min_icir]
    if top_n:
        filtered = filtered[:top_n]
    if not filtered:
        print('  No factors passed the filter.')
        return
    print(f'\n  {"Feature":<40} {"MeanIC":>8} {"ICIR":>7} {"t-stat":>7} '
          f'{"%+IC":>6} {"Turn":>6} {"Yrs":>4}')
    print('  ' + '─' * 90)
    for r in filtered:
        turn = f'{r["turnover"]:.3f}' if r['turnover'] is not None else '  N/A'
        print(f'  {r["feature"]:<40} {r["mean_ic"]:>+8.4f} {r["icir"]:>7.3f} '
              f'{r["ic_tstat"]:>7.2f} {r["pct_positive_ic"]:>6.0%} '
              f'{turn:>6} {r["n_years"]:>4}')


def main() -> None:
    parser = argparse.ArgumentParser(description='Factor IC/ICIR/turnover research')
    parser.add_argument('--horizon',  default='1y', choices=['1y', '3y', '5y'],
                        help='Return horizon (default: 1y)')
    parser.add_argument('--all-horizons', action='store_true',
                        help='Run analysis for all three horizons')
    parser.add_argument('--factors', nargs='*', default=None,
                        help='Specific factor names (default: all candidates)')
    parser.add_argument('--top',     type=int,   default=30,
                        help='Show top N factors by |ICIR| (default: 30)')
    parser.add_argument('--sector-neutral', action='store_true', default=True,
                        help='Demean factors within sector before IC (default: True)')
    parser.add_argument('--no-sector-neutral', dest='sector_neutral', action='store_false',
                        help='Disable sector neutralization')
    parser.add_argument('--min-icir', type=float, default=0.05,
                        help='Minimum |ICIR| to include in output table (default: 0.05)')
    args = parser.parse_args()

    print('Loading data...')
    df = load_data()
    print(f'  {len(df):,} annual rows | {df["ticker"].nunique():,} companies | '
          f'{df["fiscal_year"].nunique()} years')

    horizons_to_run = list(HORIZONS.keys()) if args.all_horizons else [args.horizon]

    features = args.factors if args.factors else get_candidates(df)
    # Validate user-specified factors exist
    if args.factors:
        missing = [f for f in features if f not in df.columns]
        if missing:
            print(f'  WARNING: not found in data: {missing}')
        features = [f for f in features if f in df.columns]
    print(f'  Analysing {len(features)} factors across {horizons_to_run} horizons...')

    for h in horizons_to_run:
        ret_col = HORIZONS[h]
        if ret_col not in df.columns:
            print(f'  {h}: {ret_col} not found in dataset, skipping.')
            continue

        print(f'\n── {h} ({ret_col}) {"[sector-neutral]" if args.sector_neutral else ""} ─────────────────────────────────────')
        rows = []
        for i, feat in enumerate(features):
            if i % 50 == 0 and i > 0:
                print(f'  {i}/{len(features)}...', flush=True)
            result = analyse_factor(df, feat, ret_col, sector_neutral=args.sector_neutral)
            if result:
                rows.append(result)

        if not rows:
            print('  No results.')
            continue

        out_df = (pd.DataFrame(rows)
                  .assign(abs_icir=lambda x: x['icir'].abs())
                  .sort_values('abs_icir', ascending=False)
                  .drop(columns='abs_icir')
                  .reset_index(drop=True))

        suffix = '_sn' if args.sector_neutral else ''
        out_path = REPORTS / f'factor_research_{h}{suffix}.csv'
        out_df.to_csv(out_path, index=False)
        print(f'  Saved {len(out_df)} factors → {out_path}')

        n_sig = (out_df['ic_tstat'].abs() >= 2.0).sum()
        print(f'  Statistically significant (|t|≥2): {n_sig}/{len(out_df)}')

        print_table(out_df.to_dict('records'), args.top, args.min_icir)


if __name__ == '__main__':
    main()
