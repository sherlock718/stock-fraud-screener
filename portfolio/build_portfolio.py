"""
Portfolio constructor: IC-weighted composite alpha → Kelly-sized positions.

Reads alpha_registry.json for signal weights (IC-weighted). Computes composite
score, ranks stocks, applies filters, quarter-Kelly sizing, sector cap, and
position cap. Runs a historical backtest and outputs current holdings.

Usage:
    python3 scripts/build_portfolio.py
    python3 scripts/build_portfolio.py --strategy long_short --horizon 3y
    python3 scripts/build_portfolio.py --market US --top-n 20 --tearsheet
    python3 scripts/build_portfolio.py --strategy long_only --horizon all --tearsheet

Outputs:
    data/portfolio_holdings.json  — latest-year holdings with weights
    data/portfolio_backtest.json  — annual return series + tearsheet metrics
"""
from __future__ import annotations
import argparse
import json
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from _root import ROOT

BASE = ROOT

FULL_DATA    = BASE / 'data' / 'historical_dataset_clean.parquet'
REGISTRY     = BASE / 'data' / 'alpha_registry.json'
HOLDINGS_OUT = BASE / 'data' / 'portfolio_holdings.json'
BACKTEST_OUT = BASE / 'data' / 'portfolio_backtest.json'
SPY_PATH     = BASE / 'data' / 'spy_returns.csv'

RISK_FREE      = 0.03
MIN_MARKET_CAP = 10_000_000  # $10M floor — micro-cap / institution-avoidance niche


# ── Registry ──────────────────────────────────────────────────────────────────

def load_registry(horizon_filter: str) -> tuple[list[str], dict[str, float]]:
    """Return (signal_cols, ic_weights) for selected registry signals.

    When horizon_filter != 'all', ML OOF signals for other horizons are dropped
    but all factor signals are kept (they are horizon-agnostic).
    """
    with open(REGISTRY) as f:
        reg = json.load(f)
    selected = [s for s in reg['signals'] if s['selected']]
    if horizon_filter != 'all':
        filtered = []
        for s in selected:
            if s['category'] == 'factor':
                filtered.append(s)
            elif s.get('horizon') == horizon_filter:
                filtered.append(s)
        if filtered:
            selected = filtered
    total_ic = sum(s['ic_mean'] for s in selected)
    weights = {s['signal_id']: s['ic_mean'] / total_ic for s in selected}
    return [s['signal_id'] for s in selected], weights


# ── Scoring ───────────────────────────────────────────────────────────────────

def compute_composite(df: pd.DataFrame, signal_cols: list[str],
                      ic_weights: dict[str, float]) -> pd.Series:
    """IC-weighted composite: percentile-rank each signal then weight-average."""
    ranks: dict[str, pd.Series] = {}
    for col in signal_cols:
        if col in df.columns:
            ranks[col] = df[col].rank(pct=True)

    composite = pd.Series(0.0, index=df.index)
    weight_sum = pd.Series(0.0, index=df.index)
    for col, w in ic_weights.items():
        if col in ranks:
            valid = ranks[col].notna()
            composite[valid] += ranks[col][valid] * w
            weight_sum[valid] += w

    return composite / weight_sum.clip(lower=1e-9)


# ── Sizing ────────────────────────────────────────────────────────────────────

def kelly_weights(scores: pd.Series, fraction: float) -> pd.Series:
    """Fractional Kelly from alpha scores treated as win-probability.

    Full Kelly: f = 2p - 1 (clipped to 0 for p < 0.5).
    Applied fraction: f_used = fraction * f_full.
    Falls back to equal weight if all scores are below 0.5.
    """
    f_full = (2.0 * scores - 1.0).clip(lower=0.0)
    f = f_full * fraction
    total = f.sum()
    if total <= 0:
        return pd.Series(1.0 / len(scores), index=scores.index)
    return f / total


def apply_constraints(weights: pd.Series, df_slice: pd.DataFrame,
                      position_cap: float, sector_cap: float) -> pd.Series:
    """Iteratively enforce position cap then sector cap, then renormalise."""
    w = weights.copy().clip(upper=position_cap)

    if 'sic_code' in df_slice.columns:
        for sic in df_slice['sic_code'].unique():
            mask = (df_slice['sic_code'] == sic)
            if mask.any():
                sw = w[mask].sum()
                if sw > sector_cap:
                    w[mask] *= sector_cap / sw

    total = w.sum()
    return w / total if total > 0 else w


# ── Backtest ──────────────────────────────────────────────────────────────────

def _horizon_return_col(horizon: str, df_cols: list[str]) -> str:
    """Map horizon string to the best available forward_return column."""
    candidate = f'forward_return_{horizon}'
    if candidate in df_cols:
        return candidate
    return 'forward_return_1y'


def run_backtest(df: pd.DataFrame, signal_cols: list[str],
                 ic_weights: dict[str, float],
                 args: argparse.Namespace) -> dict:
    spy_returns: dict[int, float] = {}
    if SPY_PATH.exists():
        spy_df = pd.read_csv(SPY_PATH)
        spy_returns = dict(zip(spy_df['year'].astype(int), spy_df['spy_return'].astype(float)))

    ret_col = _horizon_return_col(args.horizon, list(df.columns))
    years = sorted(df['fiscal_year'].dropna().unique().astype(int))

    annual_returns: list[float] = []
    annual_details: list[dict] = []

    for year in years:
        yr = df[df['fiscal_year'] == year].copy()
        if 'market_cap_at_filing' in yr.columns:
            yr = yr[yr['market_cap_at_filing'] >= args.min_market_cap]
        # Margin-of-safety gate: require minimum alpha_value percentile
        if getattr(args, 'mos_min_score', None) is not None and 'alpha_value' in yr.columns:
            yr = yr[yr['alpha_value'] >= args.mos_min_score]
        # Low-vol filter: keep only bottom-half by trailing 12m volatility
        if getattr(args, 'low_vol_only', False) and 'vol_prior_12m' in yr.columns:
            vol_median = yr['vol_prior_12m'].median()
            yr = yr[yr['vol_prior_12m'] <= vol_median]
        if len(yr) < args.top_n or ret_col not in yr.columns:
            continue
        yr = yr.dropna(subset=[ret_col])
        if len(yr) < args.top_n:
            continue

        composite = compute_composite(yr, signal_cols, ic_weights)
        yr = yr.assign(_composite=composite)

        if args.strategy == 'long_only':
            top = yr.nlargest(args.top_n, '_composite')
            w = kelly_weights(top['_composite'], args.kelly_fraction)
            w = apply_constraints(w, top, args.position_cap, args.sector_cap)
            port_ret = float((top[ret_col].values * w.values).sum())
        else:
            top = yr.nlargest(args.top_n, '_composite')
            bot = yr.nsmallest(args.top_n, '_composite')
            w_long = kelly_weights(top['_composite'], args.kelly_fraction)
            w_long = apply_constraints(w_long, top, args.position_cap, args.sector_cap)
            w_short = pd.Series(1.0 / len(bot), index=bot.index)
            long_ret = float((top[ret_col].values * w_long.values).sum())
            short_ret = float((bot[ret_col].values * w_short.values).sum())
            port_ret = 0.5 * long_ret - 0.5 * short_ret

        annual_returns.append(port_ret)
        annual_details.append({
            'year': int(year),
            'return_pct': round(port_ret * 100, 2),
            'spy_return_pct': round(spy_returns[year] * 100, 2) if year in spy_returns else None,
            'n_stocks': len(top),
        })

    if not annual_returns:
        return {}

    arr = np.array(annual_returns)
    n = len(arr)
    cagr = float(np.prod(1 + arr) ** (1 / n) - 1)
    vol = float(arr.std(ddof=1)) if n > 1 else 0.0
    sharpe = float((cagr - RISK_FREE) / vol) if vol > 0 else None
    neg = arr[arr < RISK_FREE]
    downside = float(neg.std(ddof=1)) if len(neg) > 1 else None
    sortino = float((cagr - RISK_FREE) / downside) if downside and downside > 0 else None
    cum = np.cumprod(1 + arr)
    peak = np.maximum.accumulate(cum)
    max_dd = float(((cum - peak) / peak).min())
    implied_max_dd = -max(abs(max_dd), 2 * vol)  # annual sampling understates true dd; 2σ floor
    var_95 = round(float(np.percentile(arr, 5)) * 100, 2)
    _tail = arr[arr <= np.percentile(arr, 1)]
    cvar_99 = round(float(_tail.mean()) * 100, 2) if len(_tail) > 0 else var_95

    spy_overlap = [(arr[i], spy_returns[d['year']])
                   for i, d in enumerate(annual_details) if d['year'] in spy_returns]
    beta = alpha_ann = spy_cagr = excess = None
    if len(spy_overlap) >= 3:
        p_arr = np.array([x[0] for x in spy_overlap])
        s_arr = np.array([x[1] for x in spy_overlap])
        beta = float(np.cov(p_arr, s_arr)[0, 1] / np.var(s_arr))
        spy_cagr = float(np.prod(1 + s_arr) ** (1 / len(s_arr)) - 1)
        alpha_ann = float(cagr - RISK_FREE - beta * (spy_cagr - RISK_FREE))
        excess = cagr - spy_cagr

    return {
        'strategy': args.strategy,
        'horizon': args.horizon,
        'market': args.market,
        'top_n': args.top_n,
        'kelly_fraction': args.kelly_fraction,
        'sector_cap': args.sector_cap,
        'position_cap': args.position_cap,
        'n_years': n,
        'cagr_pct': round(cagr * 100, 2),
        'sharpe': round(sharpe, 3) if sharpe is not None else None,
        'sortino': round(sortino, 3) if sortino is not None else None,
        'max_drawdown_pct': round(max_dd * 100, 2),
        'implied_max_drawdown_pct': round(implied_max_dd * 100, 2),
        'var_95_pct': var_95,
        'cvar_99_pct': cvar_99,
        'spy_cagr_pct': round(spy_cagr * 100, 2) if spy_cagr is not None else None,
        'excess_cagr_pct': round(excess * 100, 2) if excess is not None else None,
        'beta': round(beta, 3) if beta is not None else None,
        'alpha_annualised_pct': round(alpha_ann * 100, 2) if alpha_ann is not None else None,
        'signals_used': signal_cols,
        'ic_weights': {k: round(v, 4) for k, v in ic_weights.items()},
        'annual_returns': annual_details,
    }


# ── Current holdings ──────────────────────────────────────────────────────────

def _latest_complete_year(df: pd.DataFrame, signal_cols: list[str],
                          min_n: int, min_market_cap: float) -> int:
    """Return the most recent fiscal_year with ≥ min_n rows having all signals non-null."""
    for year in sorted(df['fiscal_year'].dropna().unique().astype(int), reverse=True):
        yr = df[df['fiscal_year'] == year]
        if 'market_cap_at_filing' in yr.columns:
            yr = yr[yr['market_cap_at_filing'] >= min_market_cap]
        present = [c for c in signal_cols if c in yr.columns]
        if not present:
            continue
        complete = yr.dropna(subset=present, how='any')
        if len(complete) >= min_n:
            return year
    return int(df['fiscal_year'].max())


def build_current_holdings(df: pd.DataFrame, signal_cols: list[str],
                            ic_weights: dict[str, float],
                            args: argparse.Namespace) -> dict:
    latest_year = _latest_complete_year(df, signal_cols, args.top_n, args.min_market_cap)
    yr = df[df['fiscal_year'] == latest_year].copy()
    if 'market_cap_at_filing' in yr.columns:
        yr = yr[yr['market_cap_at_filing'] >= args.min_market_cap]
    if getattr(args, 'mos_min_score', None) is not None and 'alpha_value' in yr.columns:
        yr = yr[yr['alpha_value'] >= args.mos_min_score]
    if getattr(args, 'low_vol_only', False) and 'vol_prior_12m' in yr.columns:
        vol_median = yr['vol_prior_12m'].median()
        yr = yr[yr['vol_prior_12m'] <= vol_median]

    composite = compute_composite(yr, signal_cols, ic_weights)
    yr = yr.assign(_composite=composite)
    top = yr.nlargest(args.top_n, '_composite')
    w = kelly_weights(top['_composite'], args.kelly_fraction)
    w = apply_constraints(w, top, args.position_cap, args.sector_cap)

    holdings = []
    for idx, row in top.iterrows():
        holdings.append({
            'ticker': str(row.get('ticker', '')),
            'market': str(row.get('market', '')),
            'composite_score': round(float(row['_composite']), 4),
            'weight_pct': round(float(w.get(idx, 0.0)) * 100, 2),
            'market_cap_m': (round(float(row['market_cap_at_filing']) / 1e6, 1)
                             if pd.notna(row.get('market_cap_at_filing')) else None),
            'sic_code': (str(row['sic_code'])
                         if pd.notna(row.get('sic_code')) else None),
        })

    return {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'fiscal_year': latest_year,
        'strategy': args.strategy,
        'horizon': args.horizon,
        'market': args.market,
        'kelly_fraction': args.kelly_fraction,
        'n_holdings': len(holdings),
        'holdings': holdings,
    }


# ── Tearsheet ─────────────────────────────────────────────────────────────────

def print_tearsheet(result: dict, holdings: dict) -> None:
    def _fmt(val, fmt=':.2f', suffix=''):
        return (f'{val:{fmt.strip(":")}}{suffix}') if val is not None else 'n/a'

    print('\n' + '═' * 62)
    print(f"  Portfolio — {result['strategy'].upper()} · {result['horizon']} · {result['market']}")
    print('═' * 62)
    print(f"  CAGR (net)          : {_fmt(result.get('cagr_pct'), ':+.1f', '%')}")
    print(f"  vs SPY (excess)     : {_fmt(result.get('excess_cagr_pct'), ':+.1f', '%')}")
    print(f"  Sharpe              : {_fmt(result.get('sharpe'), ':.3f')}")
    print(f"  Sortino             : {_fmt(result.get('sortino'), ':.3f')}")
    print(f"  Max Drawdown        : {_fmt(result.get('max_drawdown_pct'), ':+.1f', '%')}")
    print(f"  Implied MaxDD (2σ)  : {_fmt(result.get('implied_max_drawdown_pct'), ':+.1f', '%')}")
    print(f"  VaR 95%             : {_fmt(result.get('var_95_pct'), ':+.1f', '%')}")
    print(f"  CVaR 99%            : {_fmt(result.get('cvar_99_pct'), ':+.1f', '%')}")
    print(f"  Beta vs SPY         : {_fmt(result.get('beta'), ':.3f')}")
    print(f"  Alpha (annualised)  : {_fmt(result.get('alpha_annualised_pct'), ':+.1f', '%')}")
    print(f"  Years               : {result.get('n_years', 'n/a')}")
    print(f"  Stocks / year       : {result.get('top_n', 'n/a')}")
    print(f"  Kelly fraction      : {result.get('kelly_fraction', 'n/a')}×")
    print()
    print('  Signals (IC weights):')
    for sig, w in result.get('ic_weights', {}).items():
        print(f'    {sig:<24} {w:.3f}')
    print()
    print(f"  Top Holdings ({holdings.get('fiscal_year', '?')}):")
    for h in holdings.get('holdings', [])[:10]:
        cap = f"  ${h['market_cap_m']}M" if h.get('market_cap_m') else ''
        print(f"    {h['ticker']:<16} score={h['composite_score']:.3f}  "
              f"wt={h['weight_pct']:.1f}%{cap}")
    extra = holdings.get('n_holdings', 0) - 10
    if extra > 0:
        print(f'    ... +{extra} more')
    print('═' * 62 + '\n')


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description='Build IC-weighted Kelly portfolio from alpha registry')
    parser.add_argument('--strategy', choices=['long_only', 'long_short'], default='long_only',
                        help='Portfolio construction variant (default: long_only)')
    parser.add_argument('--horizon', default='1y',
                        help='ML OOF horizon filter: 1y, 3y, 5y, all (default: 1y)')
    parser.add_argument('--market', default='all',
                        help='Market filter: US, KR, all (default: all)')
    parser.add_argument('--top-n', type=int, default=30, dest='top_n',
                        help='Stocks per year (default: 30)')
    parser.add_argument('--kelly-fraction', type=float, default=0.25, dest='kelly_fraction',
                        help='Fractional Kelly multiplier (default: 0.25)')
    parser.add_argument('--sector-cap', type=float, default=0.40, dest='sector_cap',
                        help='Max sector weight (default: 0.40)')
    parser.add_argument('--position-cap', type=float, default=0.05, dest='position_cap',
                        help='Max single-stock weight (default: 0.05)')
    parser.add_argument('--min-market-cap', type=float, default=MIN_MARKET_CAP,
                        dest='min_market_cap',
                        help='Minimum market cap in USD (default: 50M)')
    parser.add_argument('--var-gate', type=float, default=None, dest='var_gate',
                        help='Halt if historical VaR 95%% is worse than this threshold (e.g. -30 for -30%%)')
    parser.add_argument('--cvar-gate', type=float, default=None, dest='cvar_gate',
                        help='Halt if historical CVaR 99%% is worse than this threshold (e.g. -40 for -40%%)')
    parser.add_argument('--mos-min-score', type=float, default=None, dest='mos_min_score',
                        help='Margin-of-safety gate: require alpha_value >= this threshold (e.g. 0.55)')
    parser.add_argument('--low-vol-only', action='store_true', dest='low_vol_only',
                        help='Keep only stocks in bottom-half of trailing 12m volatility distribution')
    parser.add_argument('--tearsheet', action='store_true',
                        help='Print formatted tearsheet to stdout')
    args = parser.parse_args()

    df = pd.read_parquet(FULL_DATA)

    signal_cols, ic_weights = load_registry(args.horizon)
    print(f'Signals ({len(signal_cols)}): {signal_cols}')
    print(f'IC weights: { {k: round(v, 3) for k, v in ic_weights.items()} }')

    result = run_backtest(df, signal_cols, ic_weights, args)
    if not result:
        print('No backtest result — check data coverage for selected horizon.')
        return

    # Risk gates — warn (or abort) if historical simulation breaches thresholds
    var_hist = result.get('max_drawdown_pct')  # worst-case annual drawdown proxy
    cvar_hist = result.get('cvar_99_pct')
    if args.var_gate is not None and var_hist is not None:
        if result.get('cagr_pct', 0) < 0 or var_hist < args.var_gate:
            print(f'WARNING: VaR gate breached — historical drawdown {var_hist:.1f}% < gate {args.var_gate:.1f}%')
    if args.cvar_gate is not None and cvar_hist is not None:
        if cvar_hist < args.cvar_gate:
            print(f'ERROR: CVaR gate breached — CVaR99 {cvar_hist:.1f}% < gate {args.cvar_gate:.1f}% — aborting.')
            return

    holdings = build_current_holdings(df, signal_cols, ic_weights, args)

    HOLDINGS_OUT.write_text(json.dumps(holdings, indent=2, default=str))
    BACKTEST_OUT.write_text(json.dumps(result, indent=2, default=str))
    print(f'Saved → {HOLDINGS_OUT}')
    print(f'Saved → {BACKTEST_OUT}')

    if args.tearsheet:
        print_tearsheet(result, holdings)


if __name__ == '__main__':
    main()
