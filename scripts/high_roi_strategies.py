"""
High-ROI investment strategies built on the fraud screener signals.

Strategy 1: Quality + Earnings Momentum (QEM)
  - Piotroski F-score >= 7 + positive EPS growth + top-half momentum
  - Historically highest hit-rate combination

Strategy 2: Small-Cap Deep Value (SCDV)
  - Micro/small cap + low P/B + high Piotroski + clean fraud scores
  - Less efficient, more alpha available

Strategy 3: International Arbitrage (IARB)
  - Non-US markets + deep value (P/B < 1.0) + quality gate
  - Targets governance-reform markets (KR, EU) and frontier (BR, CA)

Usage:
    python3 scripts/high_roi_strategies.py --strategy all --capital 10000
    python3 scripts/high_roi_strategies.py --strategy qem --market US --top 15
    python3 scripts/high_roi_strategies.py --strategy iarb --capital 10000
"""
from __future__ import annotations
import argparse
import json
import warnings
warnings.filterwarnings('ignore')

import joblib
import numpy as np
import pandas as pd
from pathlib import Path

BASE       = Path(__file__).parent.parent
FULL_DATA  = BASE / 'data' / 'historical_dataset_clean.parquet'
APP_DATA   = BASE / 'data' / 'app_data.parquet'
MODELS_DIR = BASE / 'models'


# ── Data loading ────────────────────────────────────────────────────────────

def _add_piotroski_ext(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(['ticker', 'fiscal_year'])
    for src, name in [
        ('shares_outstanding', 'piotroski_shares_ok'),
        ('gross_margin',       'piotroski_delta_gm'),
        ('asset_turnover',     'piotroski_delta_at'),
    ]:
        if src in df.columns:
            df[name] = df.groupby('ticker')[src].transform(
                lambda x: (x <= x.shift(1)).astype(float) if src == 'shares_outstanding'
                else (x > x.shift(1)).astype(float)
            )
    extra = [c for c in ['piotroski_shares_ok','piotroski_delta_gm','piotroski_delta_at'] if c in df.columns]
    if extra and 'piotroski_f_score' in df.columns:
        df['piotroski_f_score_9'] = df['piotroski_f_score'].astype('float64') + df[extra].sum(axis=1, min_count=1)
    return df


def load_full(market: str | None = None) -> pd.DataFrame:
    df = pd.read_parquet(FULL_DATA)
    df = df[df['period_type'] == 'annual']
    latest = df['fiscal_year'].max()
    df = df[df['fiscal_year'] >= latest - 1]
    df = _add_piotroski_ext(df)
    df = df.sort_values('fiscal_year', ascending=False).drop_duplicates('ticker', keep='first')
    if market:
        df = df[df['market'] == market]
    return df.reset_index(drop=True)


def load_hist_all() -> pd.DataFrame:
    df = pd.read_parquet(FULL_DATA)
    df = df[df['period_type'] == 'annual']
    return _add_piotroski_ext(df).reset_index(drop=True)


def load_models_and_score(df: pd.DataFrame) -> pd.DataFrame:
    meta_path = MODELS_DIR / 'model_meta.json'
    if not meta_path.exists():
        return df
    meta = json.loads(meta_path.read_text())
    for h in ['1y', '3y', '5y']:
        p = MODELS_DIR / f'model_{h}.joblib'
        if not p.exists():
            continue
        clf = joblib.load(p)
        feats = [f for f in meta[h]['features'] if f in df.columns]
        X = df[feats].fillna(df[feats].median())
        df[f'ml_{h}'] = clf.predict_proba(X)[:, 1]
    return df


# ── Strategy 1: Quality + Earnings Momentum ─────────────────────────────────

def strategy_qem(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """
    Quality + Earnings Momentum (QEM).
    Long thesis: high-quality companies with accelerating earnings are
    systematically underpriced because analysts anchor on historical earnings.
    """
    s = df.copy()

    # Quality gate
    s = s[s['piotroski_f_score'].fillna(0) >= 7]

    # Earnings momentum: positive YoY EPS growth
    if 'eps_growth_yoy' in s.columns:
        s = s[s['eps_growth_yoy'].fillna(-99) > 0]

    # Price momentum (avoid value traps)
    if 'momentum_12m_prior' in s.columns:
        s = s[s['momentum_12m_prior'].fillna(-99) > -0.10]

    # Fraud safety
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    if 'likely_delisted' in s.columns:
        s = s[s['likely_delisted'].fillna(1) == 0]

    # Score: weight earnings momentum + quality + ML
    score = pd.Series(0.0, index=s.index)
    weights = 0.0
    for col, w in [('eps_growth_yoy', 0.30), ('quality_composite', 0.25),
                   ('ml_1y', 0.25), ('momentum_12m_prior', 0.20)]:
        if col in s.columns and s[col].notna().sum() > 5:
            score += s[col].rank(pct=True) * w
            weights += w
    s['qem_score'] = score / weights if weights > 0 else np.nan

    cols = ['ticker', 'name', 'market', 'qem_score', 'piotroski_f_score',
            'eps_growth_yoy', 'momentum_12m_prior', 'beneish_m_score',
            'ml_1y', 'pe_ratio', 'pb_ratio', 'market_cap_at_filing']
    out_cols = [c for c in cols if c in s.columns]
    return s[out_cols].sort_values('qem_score', ascending=False).head(top_n).reset_index(drop=True)


# ── Strategy 2: Small-Cap Deep Value ─────────────────────────────────────────

def strategy_scdv(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """
    Small-Cap Deep Value (SCDV).
    Long thesis: institutional investors can't hold micro/small-caps due to
    liquidity constraints — systematic screening finds mis-pricings they ignore.
    """
    s = df.copy()

    # Universe: micro + small caps only
    if 'size_category_label' in s.columns:
        s = s[s['size_category_label'].isin(['micro', 'small'])]
    elif 'market_cap_at_filing' in s.columns:
        s = s[s['market_cap_at_filing'].fillna(0) < 2e9]

    # Value: P/B below 1.5 or value_composite above median
    if 'pb_ratio' in s.columns:
        s = s[s['pb_ratio'].fillna(999) < 2.0]

    # Quality floor
    s = s[s['piotroski_f_score'].fillna(0) >= 6]

    # Fraud safety (critical for small caps — harder to investigate)
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    if 'altman_z_score' in s.columns:
        s = s[s['altman_z_score'].fillna(0) > 1.81]
    if 'likely_delisted' in s.columns:
        s = s[s['likely_delisted'].fillna(1) == 0]

    # Score: value + quality + ML, penalise high leverage
    score = pd.Series(0.0, index=s.index)
    weights = 0.0
    for col, w in [('value_composite', 0.35), ('quality_composite', 0.25),
                   ('ml_3y', 0.25), ('piotroski_f_score', 0.15)]:
        if col in s.columns and s[col].notna().sum() > 5:
            score += s[col].rank(pct=True) * w
            weights += w
    s['scdv_score'] = score / weights if weights > 0 else np.nan

    # Penalise high debt (kills small-cap value traps)
    if 'debt_to_equity' in s.columns:
        debt_penalty = s['debt_to_equity'].fillna(0).clip(0, 5) / 5 * 0.10
        s['scdv_score'] = s['scdv_score'] - debt_penalty

    cols = ['ticker', 'name', 'market', 'scdv_score', 'size_category_label',
            'pb_ratio', 'piotroski_f_score', 'beneish_m_score', 'altman_z_score',
            'ml_3y', 'market_cap_at_filing', 'debt_to_equity']
    out_cols = [c for c in cols if c in s.columns]
    return s[out_cols].sort_values('scdv_score', ascending=False).head(top_n).reset_index(drop=True)


# ── Strategy 3: International Arbitrage ──────────────────────────────────────

def strategy_iarb(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """
    International Arbitrage (IARB).
    Long thesis: non-US markets are under-covered by global analysts.
    Deep value + quality in KR/EU/BR/CA beats benchmarks because the
    discount is structural, not fundamental.
    """
    s = df.copy()

    # Non-US only
    s = s[s['market'] != 'US']

    # Deep value: P/B below 1.5 (most non-US markets trade near book)
    if 'pb_ratio' in s.columns:
        s = s[s['pb_ratio'].fillna(999) < 1.5]

    # Quality floor
    s = s[s['piotroski_f_score'].fillna(0) >= 6]

    # Fraud safety
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    if 'likely_delisted' in s.columns:
        s = s[s['likely_delisted'].fillna(1) == 0]

    # Score: value + quality + ML; boost underresearched markets
    market_boost = {'KR': 0.05, 'BR': 0.03, 'CA': 0.02}
    score = pd.Series(0.0, index=s.index)
    weights = 0.0
    for col, w in [('value_composite', 0.30), ('quality_composite', 0.25),
                   ('ml_3y', 0.25), ('momentum_12m_prior', 0.20)]:
        if col in s.columns and s[col].notna().sum() > 5:
            score += s[col].rank(pct=True) * w
            weights += w
    s['iarb_score'] = score / weights if weights > 0 else np.nan
    s['iarb_score'] += s['market'].map(market_boost).fillna(0)

    cols = ['ticker', 'name', 'market', 'iarb_score', 'pb_ratio',
            'piotroski_f_score', 'beneish_m_score', 'ml_3y',
            'pe_ratio', 'roe', 'market_cap_at_filing']
    out_cols = [c for c in cols if c in s.columns]
    return s[out_cols].sort_values('iarb_score', ascending=False).head(top_n).reset_index(drop=True)


# ── Expected return estimate ─────────────────────────────────────────────────

def backtest_strategy(df_full: pd.DataFrame, filter_fn, label: str) -> dict:
    """
    Walk-forward expected return: for each year, apply filter_fn to that year's
    data and record the mean 1y forward return.
    """
    years = sorted(df_full['fiscal_year'].unique())
    annual_returns = []
    for yr in years:
        yr_df = df_full[df_full['fiscal_year'] == yr].copy()
        try:
            picks = filter_fn(yr_df, top_n=20)
            if len(picks) < 3:
                continue
            ret_col = 'forward_return_1y'
            if ret_col not in picks.columns:
                merged = picks.merge(yr_df[['ticker', ret_col]], on='ticker', how='left')
                ret = merged[ret_col].dropna()
            else:
                ret = picks[ret_col].dropna()
            if len(ret) < 3:
                continue
            annual_returns.append({'year': yr, 'mean_ret': ret.mean(), 'n': len(ret)})
        except Exception:
            continue

    if not annual_returns:
        return {'label': label, 'mean_annual_ret': np.nan, 'n_years': 0}
    res = pd.DataFrame(annual_returns)
    return {
        'label':           label,
        'mean_annual_ret': res['mean_ret'].mean(),
        'median_ret':      res['mean_ret'].median(),
        'worst_year':      res['mean_ret'].min(),
        'best_year':       res['mean_ret'].max(),
        'n_years':         len(res),
        'hit_rate':        (res['mean_ret'] > 0).mean(),
    }


# ── Report ────────────────────────────────────────────────────────────────────

def print_strategy(name: str, picks: pd.DataFrame, stats: dict, capital: float) -> None:
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"{'='*70}")
    print(f"  Expected annual return: {stats.get('mean_annual_ret', np.nan):.1%}  "
          f"| Worst year: {stats.get('worst_year', np.nan):.1%}  "
          f"| Hit rate: {stats.get('hit_rate', np.nan):.0%}")
    equal_pos = capital / len(picks) if len(picks) > 0 else 0
    print(f"  {len(picks)} picks | Equal-weight position: €{equal_pos:,.0f}")
    print()
    pd.set_option('display.max_colwidth', 28)
    pd.set_option('display.float_format', '{:.3f}'.format)
    print(picks.to_string(index=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--strategy', default='all',
                        choices=['all', 'qem', 'scdv', 'iarb'])
    parser.add_argument('--market',  default=None)
    parser.add_argument('--top',     default=15, type=int)
    parser.add_argument('--capital', default=10000, type=float)
    args = parser.parse_args()

    print('Loading data...')
    df = load_full(args.market)
    print(f'  {len(df):,} companies | markets: {df["market"].value_counts().to_dict()}')

    print('Scoring with ML models...')
    df = load_models_and_score(df)

    # Load historical for backtesting
    print('Loading historical data for backtests...')
    hist = load_hist_all()
    hist = load_models_and_score(hist)

    strategies = {
        'qem':  ('Quality + Earnings Momentum (QEM)',  strategy_qem),
        'scdv': ('Small-Cap Deep Value (SCDV)',         strategy_scdv),
        'iarb': ('International Arbitrage (IARB)',      strategy_iarb),
    }

    to_run = list(strategies.keys()) if args.strategy == 'all' else [args.strategy]

    for key in to_run:
        label, fn = strategies[key]
        picks = fn(df, top_n=args.top)
        # Merge forward returns for backtest display
        if 'forward_return_1y' not in picks.columns and 'forward_return_1y' in hist.columns:
            picks = picks.merge(
                hist[['ticker','fiscal_year','forward_return_1y']].drop_duplicates('ticker', keep='last'),
                on='ticker', how='left'
            )
        stats = backtest_strategy(hist, fn, label)
        print_strategy(label, picks, stats, args.capital)

        out = BASE / 'data' / f'strategy_{key}.csv'
        picks.to_csv(out, index=False)
        print(f'\n  Saved: {out}')

    print('\nDone.')


if __name__ == '__main__':
    main()
