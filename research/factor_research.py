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
        reports/ic_decay_halflife.csv  (with --ic-decay flag)

Usage:
    python3 scripts/factor_research.py
    python3 scripts/factor_research.py --horizon 3y
    python3 scripts/factor_research.py --factors value_composite pe_ratio piotroski_f_score
    python3 scripts/factor_research.py --top 20        # show top 20 by |ICIR|
    python3 scripts/factor_research.py --min-icir 0.1  # filter output by |ICIR|
    python3 scripts/factor_research.py --ic-decay      # compute IC decay half-life for top 20 factors
"""
from __future__ import annotations

import argparse
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats
from _root import ROOT
from modeling.constants import EXCLUDE_COLS, EXCLUDE_PATTERNS
from research.ic_engine import compute_yearly_ic

BASE = ROOT

DATA_PATH = BASE / 'data' / 'historical_dataset_clean.parquet'
REPORTS   = BASE / 'reports'
REPORTS.mkdir(exist_ok=True)

HORIZONS = {
    '1y': 'forward_return_1y',
    '3y': 'forward_return_3y',
    '5y': 'forward_return_5y',
}

EXCLUDE = EXCLUDE_COLS



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
    ic_s = compute_yearly_ic(
        df, feature, ret_col,
        sector_neutral=sector_neutral,
        min_obs=30,
        sic_col_override="sic_code",
    )
    return ic_s.dropna().tolist()


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


def _compute_quintile_spread(df: pd.DataFrame, feature: str, ret_col: str,
                              n_quantiles: int = 5) -> dict | None:
    """Mean forward return per factor quintile (Alphalens-style).

    Returns q1_ret (bottom quintile), q5_ret (top quintile), q_spread = Q5 - Q1.
    High positive spread → factor monotonically predicts returns.
    """
    sub = df[df[ret_col].notna() & df[feature].notna()].copy()
    if len(sub) < 100:
        return None
    quantile_rets: dict[int, list[float]] = {q: [] for q in range(1, n_quantiles + 1)}
    for yr in sorted(sub['fiscal_year'].unique()):
        g = sub[sub['fiscal_year'] == yr].copy()
        if len(g) < n_quantiles * 5:
            continue
        try:
            g['_q'] = pd.qcut(g[feature], n_quantiles, labels=False, duplicates='drop') + 1
        except ValueError:
            continue
        for q in range(1, n_quantiles + 1):
            qret = g[g['_q'] == q][ret_col].mean()
            if pd.notna(qret):
                quantile_rets[q].append(qret)
    mean_by_q = {q: float(np.mean(v)) for q, v in quantile_rets.items() if v}
    if len(mean_by_q) < 2:
        return None
    q_low  = mean_by_q.get(1, np.nan)
    q_high = mean_by_q.get(n_quantiles, np.nan)
    spread = q_high - q_low if pd.notna(q_high) and pd.notna(q_low) else np.nan
    return {
        'q1_ret':   round(q_low,   4),
        'q5_ret':   round(q_high,  4),
        'q_spread': round(spread,  4) if pd.notna(spread) else None,
    }


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
    q_spread = _compute_quintile_spread(df, feature, ret_col)
    return {
        'feature':         feature,
        'ic':              round(mean_ic, 5),
        'mean_ic':         round(mean_ic, 5),
        'std_ic':          round(std_ic, 5),
        'icir':            round(icir, 4),
        'ic_tstat':        round(ic_tstat, 3),
        'pct_positive_ic': round(np.mean([ic > 0 for ic in ics]), 3),
        'n_years':         n,
        'turnover':        round(turnover, 4) if turnover is not None else None,
        'ic_min':          round(min(ics), 4),
        'ic_max':          round(max(ics), 4),
        'q1_ret':          q_spread['q1_ret'] if q_spread else None,
        'q5_ret':          q_spread['q5_ret'] if q_spread else None,
        'q_spread':        q_spread['q_spread'] if q_spread else None,
    }


def compute_ic_decay(df: pd.DataFrame, features: list[str],
                     base_ret_col: str = 'forward_return_1y',
                     top_n: int = 20) -> pd.DataFrame:
    """Compute IC across multiple forward lags to estimate signal half-life.

    Lags: 1m (approx 30d), 3m, 6m, 12m, 24m, 36m estimated from annual fiscal_year
    steps.  Since the dataset is annual, we proxy sub-annual lags by scaling the
    1y IC (rank correlation is a monotonic transform, so this is approximate).
    True multi-lag IC would require monthly price data per company.

    For lags ≥ 1y (annual periods) we use the actual forward_return_{h} columns
    directly and compute cross-sectional IC at each lag precisely.

    Returns DataFrame with columns: feature, ic_1y, ic_3y, ic_5y, halflife_yrs
    where halflife_yrs is the interpolated year at which IC drops to 50% of ic_1y.
    """
    lag_map = {
        '1y': 'forward_return_1y',
        '3y': 'forward_return_3y',
        '5y': 'forward_return_5y',
    }
    available_lags = {k: v for k, v in lag_map.items() if v in df.columns}

    # Use top_n features by |ICIR| on 1y if we have more than top_n
    if len(features) > top_n:
        ic1y_col = available_lags.get('1y', list(available_lags.values())[0])
        scored = []
        for feat in features:
            ics = compute_ic_series(df, feat, ic1y_col)
            if ics:
                icir = abs(np.mean(ics) / (np.std(ics) + 1e-8))
                scored.append((feat, icir))
        scored.sort(key=lambda x: x[1], reverse=True)
        features = [f for f, _ in scored[:top_n]]

    rows = []
    for feat in features:
        ic_per_lag: dict[str, float | None] = {}
        for lag_label, ret_col in available_lags.items():
            ics = compute_ic_series(df, feat, ret_col)
            ic_per_lag[f'ic_{lag_label}'] = round(float(np.mean(ics)), 5) if ics else None

        # Compute half-life: years at which IC falls to 50% of 1y IC
        ic_1y = ic_per_lag.get('ic_1y')
        halflife = None
        if ic_1y and abs(ic_1y) > 1e-4:
            target = ic_1y * 0.5
            lags_yrs = [(1, ic_per_lag.get('ic_1y')),
                        (3, ic_per_lag.get('ic_3y')),
                        (5, ic_per_lag.get('ic_5y'))]
            lags_yrs = [(y, v) for y, v in lags_yrs if v is not None]
            for i in range(len(lags_yrs) - 1):
                y0, v0 = lags_yrs[i]
                y1, v1 = lags_yrs[i + 1]
                # Check if target is between v0 and v1 (accounting for sign)
                if min(v0, v1) <= target <= max(v0, v1) and v1 != v0:
                    t = y0 + (target - v0) / (v1 - v0) * (y1 - y0)
                    halflife = round(t, 2)
                    break
            if halflife is None and len(lags_yrs) >= 2:
                # IC doesn't cross 50% within available lags — use last available
                last_y, last_v = lags_yrs[-1]
                if abs(last_v) > abs(ic_1y) * 0.5:
                    halflife = f'>{last_y}y'  # type: ignore[assignment]
                else:
                    halflife = f'<{lags_yrs[0][0]}y'  # type: ignore[assignment]

        row = {'feature': feat}
        row.update(ic_per_lag)
        row['halflife_yrs'] = halflife
        # IC decay ratio: 3y IC / 1y IC (1.0 = no decay, <1 = decaying signal)
        if ic_1y and ic_per_lag.get('ic_3y') and abs(ic_1y) > 1e-4:
            row['ic_decay_ratio_3y'] = round(ic_per_lag['ic_3y'] / ic_1y, 3)
        else:
            row['ic_decay_ratio_3y'] = None
        rows.append(row)

    return pd.DataFrame(rows)


def print_table(rows: list[dict], top_n: int, min_icir: float) -> None:
    filtered = [r for r in rows if abs(r['icir']) >= min_icir]
    if top_n:
        filtered = filtered[:top_n]
    if not filtered:
        print('  No factors passed the filter.')
        return
    print(f'\n  {"Feature":<40} {"MeanIC":>8} {"ICIR":>7} {"t-stat":>7} '
          f'{"%+IC":>6} {"Turn":>6} {"Yrs":>4} {"QSpread":>8}')
    print('  ' + '─' * 100)
    for r in filtered:
        turn  = f'{r["turnover"]:.3f}' if r['turnover'] is not None else '  N/A'
        qspr  = f'{r["q_spread"]:+.3f}' if r.get('q_spread') is not None else '    N/A'
        print(f'  {r["feature"]:<40} {r["mean_ic"]:>+8.4f} {r["icir"]:>7.3f} '
              f'{r["ic_tstat"]:>7.2f} {r["pct_positive_ic"]:>6.0%} '
              f'{turn:>6} {r["n_years"]:>4} {qspr:>8}')


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
    parser.add_argument('--ic-decay', action='store_true',
                        help='Compute IC decay half-life across 1y/3y/5y lags for top 20 factors')
    parser.add_argument('--decay-top', type=int, default=20,
                        help='Number of top factors for IC decay analysis (default: 20)')
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

    # IC decay half-life analysis (all-horizons, written to single CSV)
    if args.ic_decay:
        print('\n── IC Decay / Half-Life Analysis ─────────────────────────────────────')
        if 'forward_return_1y' not in df.columns:
            print('  forward_return_1y not in dataset — skipping IC decay.')
        else:
            decay_features = features  # already filtered above
            decay_df = compute_ic_decay(df, decay_features, top_n=args.decay_top)
            decay_path = REPORTS / 'ic_decay_halflife.csv'
            decay_df.to_csv(decay_path, index=False)
            print(f'  Saved IC decay analysis → {decay_path}')
            print(f'\n  {"Feature":<40} {"IC_1y":>8} {"IC_3y":>8} {"IC_5y":>8} '
                  f'{"Decay3y":>8} {"HalfLife":>10}')
            print('  ' + '─' * 90)
            for _, r in decay_df.iterrows():
                ic1 = f'{r["ic_1y"]:+.4f}' if pd.notna(r.get("ic_1y")) else '   N/A'
                ic3 = f'{r["ic_3y"]:+.4f}' if pd.notna(r.get("ic_3y")) else '   N/A'
                ic5 = f'{r["ic_5y"]:+.4f}' if pd.notna(r.get("ic_5y")) else '   N/A'
                dec = f'{r["ic_decay_ratio_3y"]:.3f}' if pd.notna(r.get("ic_decay_ratio_3y")) else '  N/A'
                hl  = str(r.get('halflife_yrs', 'N/A'))
                print(f'  {r["feature"]:<40} {ic1:>8} {ic3:>8} {ic5:>8} {dec:>8} {hl:>10}')


if __name__ == '__main__':
    main()
