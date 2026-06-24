#!/usr/bin/env python3
"""
Build data/alpha_registry.json — backtest stats and IC for all 8 alpha signals.

Signals:
  5 factor scores: alpha_value, alpha_quality, alpha_momentum, alpha_growth, alpha_fraud_risk
  3 ML OOF scores: ml_1y_oof, ml_3y_oof, ml_5y_oof  (already in parquet, unbiased)

For each signal:
  - IC     mean cross-sectional Spearman IC vs the signal's forward return
  - ICIR   IC / std(IC)
  - Sharpe, CAGR, max_drawdown via top-20 long-only backtest (30 bps cost)
  - features_used from alpha/factors/ source or model_meta.json

Selection criteria (selected=true): IC_mean > 0.02 AND Sharpe > 0.50

Usage:
    python3 scripts/build_alpha_registry.py
    python3 scripts/build_alpha_registry.py --top 20 --cost 30

Output:
    data/alpha_registry.json
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats

from scripts._shared.backtester import (
    load_full_hist, load_spy_returns, run_backtest,
    MIN_MARKET_CAP, DEFAULT_COST_BPS, SMALLCAP_COST_BPS,
)
from alpha.factors.value      import compute as _value_compute
from alpha.factors.quality    import compute as _quality_compute
from alpha.factors.momentum   import compute as _momentum_compute
from alpha.factors.growth     import compute as _growth_compute
from alpha.factors.fraud_risk import compute as _fraud_risk_compute
from scripts._root import ROOT

BASE = ROOT

REGISTRY_PATH = BASE / 'data' / 'alpha_registry.json'
META_PATH     = BASE / 'models' / 'model_meta.json'

SELECTION_IC_MIN    = 0.02
SELECTION_SHARPE_MIN = 0.50

# Features used by each factor (from alpha/factors/*.py _SIGNALS lists)
_FACTOR_FEATURES = {
    'alpha_value': [
        'ev_ebitda', 'ev_revenue', 'fcf_yield', 'earnings_yield',
        'book_to_market', 'ps_ratio', 'pe_ratio',
    ],
    'alpha_quality': [
        'roe', 'roa', 'roic', 'gross_margin', 'operating_margin',
        'ocf_to_ni', 'piotroski_f_score', 'accruals_to_assets',
        'sloan_accruals', 'gross_profit_to_assets',
    ],
    'alpha_momentum': [
        'momentum_12m_prior', 'momentum_6m_prior', 'momentum_3m_prior',
        'momentum_12m_rank', 'momentum_6m_rank', 'momentum_3m_rank',
    ],
    'alpha_growth': [
        'revenue_cagr_3y', 'revenue_growth_yoy', 'eps_growth_yoy',
        'net_income_growth_yoy', 'ocf_growth_yoy', 'gross_profit_growth_yoy',
    ],
    'alpha_fraud_risk': [
        'beneish_m_score', 'ohlson_prob_bankruptcy', 'altman_z_score',
        'fraud_score_composite', 'fraud_score_accounting', 'fraud_score_distress',
        'ml_1y', 'ml_3y', 'ml_5y',
    ],
}


def _load_ml_features() -> dict[str, list[str]]:
    if not META_PATH.exists():
        return {}
    meta = json.loads(META_PATH.read_text())
    return {
        'ml_1y_oof': meta.get('1y', {}).get('features', []),
        'ml_3y_oof': meta.get('3y', {}).get('features', []),
        'ml_5y_oof': meta.get('5y', {}).get('features', []),
    }


def _compute_ic(
    df: pd.DataFrame,
    score_col: str,
    ret_col: str,
    min_obs: int = 30,
) -> tuple[float, float]:
    """Return (mean_IC, ICIR) for score_col vs ret_col across fiscal years."""
    sub = df[df[ret_col].notna() & df[score_col].notna()]
    ics: list[float] = []
    for yr in sorted(sub['fiscal_year'].unique()):
        g = sub[sub['fiscal_year'] == yr]
        if len(g) < min_obs:
            continue
        c, _ = stats.spearmanr(g[score_col], g[ret_col])
        if not np.isnan(c):
            ics.append(float(c))
    if not ics:
        return np.nan, np.nan
    mean_ic = float(np.mean(ics))
    icir    = float(mean_ic / (np.std(ics) + 1e-8))
    return mean_ic, icir


def _make_filter(score_col: str):
    """Return a backtester-compatible filter_fn that picks top-N by score_col."""
    def filter_fn(yr_df: pd.DataFrame, top_n: int, market) -> pd.Index:
        s = yr_df.copy()
        if market:
            s = s[s['market'] == market]
        if score_col not in s.columns or s[score_col].notna().sum() < max(top_n, 3):
            return pd.Index([])
        return s.nlargest(top_n, score_col).index
    return filter_fn


def build_registry(
    top_n: int = 20,
    cost_bps: int = DEFAULT_COST_BPS,
    smallcap_cost_bps: int = SMALLCAP_COST_BPS,
    market: str | None = None,
) -> list[dict]:
    print('Loading historical data...')
    df = load_full_hist()
    spy_returns = load_spy_returns()
    print(f'  {len(df):,} rows, {df["fiscal_year"].nunique()} years')

    # ── Compute factor scores and attach to DataFrame ─────────────────────────
    print('Computing factor scores...')
    group_cols = ('fiscal_year', 'market')
    df['alpha_value']      = _value_compute(df,      group_cols)
    df['alpha_quality']    = _quality_compute(df,    group_cols)
    df['alpha_momentum']   = _momentum_compute(df,   group_cols)
    df['alpha_growth']     = _growth_compute(df,     group_cols)
    df['alpha_fraud_risk'] = _fraud_risk_compute(df, group_cols)

    ml_features = _load_ml_features()

    # ── Signal definitions ────────────────────────────────────────────────────
    # (signal_id, score_col, ic_ret_col, horizon, category)
    signals = [
        ('alpha_value',      'alpha_value',      'forward_return_1y', '1y', 'factor'),
        ('alpha_quality',    'alpha_quality',    'forward_return_1y', '1y', 'factor'),
        ('alpha_momentum',   'alpha_momentum',   'forward_return_1y', '1y', 'factor'),
        ('alpha_growth',     'alpha_growth',     'forward_return_1y', '1y', 'factor'),
        ('alpha_fraud_risk', 'alpha_fraud_risk', 'forward_return_1y', '1y', 'factor'),
        ('ml_1y_oof',        'ml_1y_oof',        'forward_return_1y', '1y', 'ml'),
        ('ml_3y_oof',        'ml_3y_oof',        'forward_return_3y', '3y', 'ml'),
        ('ml_5y_oof',        'ml_5y_oof',        'forward_return_5y', '5y', 'ml'),
    ]

    registry: list[dict] = []
    mkt_label = market or 'all'

    for signal_id, score_col, ret_col, horizon, category in signals:
        if score_col not in df.columns or df[score_col].notna().sum() < 100:
            print(f'  {signal_id}: SKIP — column missing or insufficient data')
            continue

        print(f'  {signal_id}: computing IC...', end=' ', flush=True)
        ic_mean, icir = _compute_ic(df, score_col, ret_col)

        print(f'IC={ic_mean:.4f}  ICIR={icir:.3f}  |  running backtest...', end=' ', flush=True)
        label = f'{signal_id} | {mkt_label} | top{top_n} | {cost_bps}bps'
        result = run_backtest(
            df,
            _make_filter(score_col),
            label,
            top_n,
            market,
            cost_bps,
            smallcap_cost_bps,
            min_market_cap=MIN_MARKET_CAP,
            vol_weighted=True,
            spy_returns=spy_returns,
        )

        n_years = result.get('n_years', 0)
        if n_years == 0:
            print('no data')
            continue

        sharpe   = result.get('sharpe')
        max_dd   = result.get('max_drawdown_pct')
        cagr     = result.get('cagr_pct')
        selected = (
            ic_mean > SELECTION_IC_MIN
            and sharpe is not None
            and sharpe > SELECTION_SHARPE_MIN
        )

        print(f'CAGR={cagr:+.1f}%  Sharpe={sharpe}  MaxDD={max_dd:.1f}%  '
              f'{"✅ SELECTED" if selected else "❌ not selected"}')

        if category == 'factor':
            features_used = _FACTOR_FEATURES.get(signal_id, [])
        else:
            features_used = ml_features.get(signal_id, [])

        entry: dict = {
            'signal_id':     signal_id,
            'category':      category,
            'horizon':       horizon,
            'market':        mkt_label,
            'features_used': features_used,
            'ic_mean':       round(float(ic_mean), 4) if not np.isnan(ic_mean) else None,
            'icir':          round(float(icir), 4)    if not np.isnan(icir)    else None,
            'cagr_pct':      cagr,
            'cagr_bootstrap_mean_pct':   result.get('cagr_bootstrap_mean_pct'),
            'cagr_bootstrap_1sigma_pct': result.get('cagr_bootstrap_1sigma_pct'),
            'sharpe':        sharpe,
            'sharpe_bootstrap_mean':     result.get('sharpe_bootstrap_mean'),
            'sharpe_bootstrap_1sigma':   result.get('sharpe_bootstrap_1sigma'),
            'sortino':       result.get('sortino'),
            'calmar':        result.get('calmar'),
            'max_drawdown_pct': max_dd,
            'max_drawdown_note': (
                'Annual-frequency backtest; intra-year drawdowns not captured. '
                '0.0 means all annual periods were positive — '
                'use bootstrap_1sigma for statistical uncertainty.'
                if max_dd == 0.0 else None
            ),
            'excess_cagr_vs_spy': result.get('excess_cagr_vs_spy'),
            'beta_vs_spy':   result.get('beta_vs_spy'),
            'hit_rate_pct':  result.get('hit_rate_pct'),
            'n_years':       n_years,
            'top_n':         top_n,
            'cost_bps':      cost_bps,
            'selected':      selected,
        }
        registry.append(entry)

    return registry


def main():
    parser = argparse.ArgumentParser(description='Build alpha_registry.json')
    parser.add_argument('--top',  default=20,  type=int, help='Top N picks per year')
    parser.add_argument('--cost', default=DEFAULT_COST_BPS, type=int,
                        help=f'Round-trip cost in bps (default {DEFAULT_COST_BPS})')
    parser.add_argument('--market', default=None, help='Restrict to one market (e.g. US)')
    args = parser.parse_args()

    registry = build_registry(
        top_n=args.top,
        cost_bps=args.cost,
        market=args.market,
    )

    n_selected = sum(1 for e in registry if e['selected'])
    print(f'\n{len(registry)} signals evaluated | {n_selected} selected')

    out = {
        'generated_at': pd.Timestamp.now().isoformat(),
        'selection_criteria': {
            'ic_min':    SELECTION_IC_MIN,
            'sharpe_min': SELECTION_SHARPE_MIN,
        },
        'signals': registry,
    }
    REGISTRY_PATH.write_text(json.dumps(out, indent=2, default=str))
    print(f'Saved: {REGISTRY_PATH}')

    # Summary table
    print('\nSignal                  IC     ICIR   Sharpe  MaxDD%  Selected')
    print('─' * 68)
    for e in registry:
        ic_s   = f'{e["ic_mean"]:.4f}' if e['ic_mean'] is not None else '  N/A '
        icir_s = f'{e["icir"]:.3f}'    if e['icir']    is not None else ' N/A '
        sh_s   = f'{e["sharpe"]:.3f}'  if e['sharpe']  is not None else ' N/A '
        dd_s   = f'{e["max_drawdown_pct"]:.1f}' if e['max_drawdown_pct'] is not None else 'N/A'
        sel_s  = '✅' if e['selected'] else '❌'
        print(f'{e["signal_id"]:<22}  {ic_s}  {icir_s}  {sh_s}   {dd_s:>6}  {sel_s}')


if __name__ == '__main__':
    main()
