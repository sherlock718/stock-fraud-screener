"""
Walk-forward backtester with transaction costs and slippage.

Usage:
    python3 scripts/backtester.py --strategy all
    python3 scripts/backtester.py --strategy composite --market US --top 20
    python3 scripts/backtester.py --strategy qem --top 15 --cost 40

Saves results to data/backtest_results.json (consumed by app_v2.py).
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
MODELS_DIR = BASE / 'models'
OUT_PATH   = BASE / 'data' / 'backtest_results.json'

# ── Cost model ────────────────────────────────────────────────────────────────
DEFAULT_COST_BPS = 30     # 30 bps round-trip (commission 10bps + slippage 20bps)
SMALLCAP_COST_BPS = 60    # Illiquidity premium for micro/small caps
RISK_FREE = 0.03          # Annual risk-free rate for Sharpe calculation


# ── Data helpers (same pattern as leverage_strategy.py) ───────────────────────

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


def load_full_hist() -> pd.DataFrame:
    df = pd.read_parquet(FULL_DATA)
    df = df[df['period_type'] == 'annual'].copy()
    return _add_piotroski_ext(df).reset_index(drop=True)


def load_and_score(df: pd.DataFrame) -> pd.DataFrame:
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


# ── Strategy filter functions ─────────────────────────────────────────────────

def filter_composite(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Series:
    s = yr_df.copy()
    if market:
        s = s[s['market'] == market]
    score = pd.Series(0.0, index=s.index)
    total_w = 0.0
    for col, w in [('value_composite', 0.25), ('quality_composite', 0.20),
                   ('ml_1y', 0.30), ('ml_3y', 0.15), ('piotroski_f_score', 0.10)]:
        if col in s.columns and s[col].notna().sum() > 5:
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    s['_score'] = score / total_w
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    if 'likely_delisted' in s.columns:
        s = s[s['likely_delisted'].fillna(1) == 0]
    return s.nlargest(top_n, '_score').index


def filter_qem(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Series:
    s = yr_df.copy()
    if market:
        s = s[s['market'] == market]
    s = s[s['piotroski_f_score'].fillna(0) >= 7]
    if 'eps_growth_yoy' in s.columns:
        s = s[s['eps_growth_yoy'].fillna(-99) > 0]
    if 'momentum_12m_prior' in s.columns:
        s = s[s['momentum_12m_prior'].fillna(-99) > -0.10]
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    score = pd.Series(0.0, index=s.index)
    total_w = 0.0
    for col, w in [('eps_growth_yoy', 0.30), ('quality_composite', 0.25),
                   ('ml_1y', 0.25), ('momentum_12m_prior', 0.20)]:
        if col in s.columns and s[col].notna().sum() > 3:
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    s['_score'] = score / total_w
    return s.nlargest(top_n, '_score').index


def filter_scdv(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Series:
    s = yr_df.copy()
    if market:
        s = s[s['market'] == market]
    if 'size_category_label' in s.columns:
        s = s[s['size_category_label'].isin(['micro', 'small'])]
    if 'pb_ratio' in s.columns:
        s = s[s['pb_ratio'].fillna(99) < 2.0]
    s = s[s['piotroski_f_score'].fillna(0) >= 6]
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    if 'altman_z_score' in s.columns:
        s = s[s['altman_z_score'].fillna(0) > 1.81]
    score = pd.Series(0.0, index=s.index)
    total_w = 0.0
    for col, w in [('value_composite', 0.35), ('quality_composite', 0.25),
                   ('ml_3y', 0.25), ('piotroski_f_score', 0.15)]:
        if col in s.columns and s[col].notna().sum() > 3:
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    s['_score'] = score / total_w
    if 'debt_to_equity' in s.columns:
        penalty = s['debt_to_equity'].fillna(0).clip(0, 2) * 0.05
        s['_score'] = s['_score'] - penalty
    return s.nlargest(top_n, '_score').index


def filter_iarb(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Series:
    s = yr_df.copy()
    if market:
        s = s[s['market'] == market]
    s = s[s['market'] != 'US']
    if 'pb_ratio' in s.columns:
        s = s[s['pb_ratio'].fillna(99) < 1.5]
    s = s[s['piotroski_f_score'].fillna(0) >= 6]
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    score = pd.Series(0.0, index=s.index)
    total_w = 0.0
    for col, w in [('value_composite', 0.30), ('quality_composite', 0.25),
                   ('ml_3y', 0.25), ('momentum_12m_prior', 0.20)]:
        if col in s.columns and s[col].notna().sum() > 3:
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    market_boost = {'KR': 0.05, 'BR': 0.03, 'CA': 0.02}
    s['_score'] = (score / total_w if total_w > 0 else score) + s['market'].map(market_boost).fillna(0)
    return s.nlargest(top_n, '_score').index


STRATEGIES = {
    'composite': filter_composite,
    'qem':       filter_qem,
    'scdv':      filter_scdv,
    'iarb':      filter_iarb,
}


# ── Walk-forward engine ───────────────────────────────────────────────────────

def run_backtest(df: pd.DataFrame, filter_fn, label: str,
                 top_n: int, market: str | None,
                 cost_bps: int, smallcap_cost_bps: int) -> dict:
    years = sorted(y for y in df['fiscal_year'].unique() if y <= 2023)
    annual_rows = []

    for yr in years:
        yr_df = df[df['fiscal_year'] == yr].copy()
        idx = filter_fn(yr_df, top_n, market)
        picks = yr_df.loc[idx]

        if 'forward_return_1y' not in picks.columns:
            continue
        rets = picks['forward_return_1y'].dropna()
        if len(rets) < 3:
            continue

        # Determine cost per pick (small cap = higher illiquidity cost)
        if 'size_category_label' in picks.columns:
            is_small = picks.loc[rets.index, 'size_category_label'].isin(['micro', 'small'])
            per_pick_cost = np.where(is_small,
                                     smallcap_cost_bps / 10000,
                                     cost_bps / 10000)
        else:
            per_pick_cost = np.full(len(rets), cost_bps / 10000)

        net_rets = rets.values - per_pick_cost[:len(rets)]
        port_ret = net_rets.mean()
        cost_drag = per_pick_cost[:len(rets)].mean()

        # Benchmark: equal-weight all valid stocks in same market/year
        bench_df = yr_df.copy()
        if market:
            bench_df = bench_df[bench_df['market'] == market]
        bench_rets = bench_df['forward_return_1y'].dropna()
        bench_ret = bench_rets.mean() if len(bench_rets) > 5 else np.nan

        annual_rows.append({
            'year':      yr,
            'port_ret':  port_ret,
            'bench_ret': bench_ret,
            'excess':    port_ret - bench_ret if pd.notna(bench_ret) else np.nan,
            'cost_drag': cost_drag,
            'n_picks':   len(rets),
            'hit_rate':  (rets.values > 0).mean(),
        })

    if not annual_rows:
        return {'label': label, 'n_years': 0, 'error': 'insufficient data'}

    res = pd.DataFrame(annual_rows)

    # Cumulative wealth index
    wealth = np.cumprod(1 + res['port_ret'].values)
    bench_wealth = np.cumprod(1 + res['bench_ret'].fillna(0).values)

    # Max drawdown
    peak = np.maximum.accumulate(wealth)
    drawdowns = (wealth - peak) / peak
    max_dd = float(drawdowns.min())

    n = len(res)
    cagr = float(wealth[-1] ** (1 / n) - 1)
    bench_cagr = float(bench_wealth[-1] ** (1 / n) - 1)
    vol = float(res['port_ret'].std())
    sharpe = float((cagr - RISK_FREE) / vol) if vol > 0 else np.nan
    info_ratio = float(res['excess'].mean() / res['excess'].std()) if res['excess'].std() > 0 else np.nan

    return {
        'label':           label,
        'n_years':         n,
        'cagr_pct':        round(cagr * 100, 2),
        'bench_cagr_pct':  round(bench_cagr * 100, 2),
        'excess_cagr_pct': round((cagr - bench_cagr) * 100, 2),
        'sharpe':          round(sharpe, 3) if pd.notna(sharpe) else None,
        'info_ratio':      round(info_ratio, 3) if pd.notna(info_ratio) else None,
        'max_drawdown_pct': round(max_dd * 100, 2),
        'hit_rate_pct':    round(res['hit_rate'].mean() * 100, 1),
        'avg_cost_drag_bps': round(res['cost_drag'].mean() * 10000, 1),
        'best_year_pct':   round(res['port_ret'].max() * 100, 2),
        'worst_year_pct':  round(res['port_ret'].min() * 100, 2),
        'annual_returns':  [
            {
                'year':      int(r['year']),
                'port_pct':  round(r['port_ret'] * 100, 2),
                'bench_pct': round(r['bench_ret'] * 100, 2) if pd.notna(r['bench_ret']) else None,
                'excess_pct': round(r['excess'] * 100, 2) if pd.notna(r['excess']) else None,
                'n_picks':   int(r['n_picks']),
            }
            for _, r in res.iterrows()
        ],
    }


# ── CLI entry ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Walk-forward backtester')
    parser.add_argument('--strategy', default='all',
                        choices=['all', 'composite', 'qem', 'scdv', 'iarb'])
    parser.add_argument('--market',  default=None, help='Filter to one market (e.g. US)')
    parser.add_argument('--top',     default=20, type=int, help='Top N picks per year')
    parser.add_argument('--cost',    default=DEFAULT_COST_BPS, type=int,
                        help=f'Round-trip cost in bps (default {DEFAULT_COST_BPS})')
    parser.add_argument('--smallcap_cost', default=SMALLCAP_COST_BPS, type=int,
                        help=f'Round-trip cost for micro/small caps in bps (default {SMALLCAP_COST_BPS})')
    args = parser.parse_args()

    print('Loading + scoring full historical data...')
    df = load_full_hist()
    df = load_and_score(df)
    print(f'  {len(df):,} annual rows across {df["fiscal_year"].nunique()} years')

    to_run = list(STRATEGIES.keys()) if args.strategy == 'all' else [args.strategy]
    results = {}

    for key in to_run:
        fn = STRATEGIES[key]
        mkt_label = args.market or 'all'
        label = f'{key.upper()} | {mkt_label} | top{args.top} | {args.cost}bps'
        print(f'  Backtesting {label}...')
        result = run_backtest(df, fn, label, args.top, args.market,
                              args.cost, args.smallcap_cost)
        results[key] = result

        if result.get('n_years', 0) > 0:
            print(f'    CAGR={result["cagr_pct"]:.1f}%  '
                  f'vs bench={result["bench_cagr_pct"]:.1f}%  '
                  f'(+{result["excess_cagr_pct"]:.1f}%)  '
                  f'Sharpe={result.get("sharpe","N/A")}  '
                  f'MaxDD={result["max_drawdown_pct"]:.1f}%  '
                  f'HitRate={result["hit_rate_pct"]:.0f}%')

    out = {
        'generated_at': pd.Timestamp.now().isoformat(),
        'cost_bps':     args.cost,
        'top_n':        args.top,
        'market':       args.market,
        'strategies':   results,
    }
    OUT_PATH.write_text(json.dumps(out, indent=2, default=str))
    print(f'\nSaved: {OUT_PATH}')


if __name__ == '__main__':
    main()
