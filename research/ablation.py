"""
Feature ablation study — drop one feature at a time, measure impact on backtest metrics.

For each feature in the top-N from feature_sets_3y.json, runs the full walk-forward
backtest (composite strategy) with that feature removed. Reports Sharpe delta, CAGR delta,
hit rate delta vs the all-features baseline.

Usage:
    python3 -m research.ablation --top 20
    python3 -m research.ablation --top 10 --fast   # skip monthly NAV for speed
"""
from __future__ import annotations
import argparse
import json
import time
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from pathlib import Path

from _root import ROOT
from backtest.engine import (
    load_full_hist, load_and_score, load_spy_returns, load_monthly_prices,
    run_backtest, filter_composite, DEFAULT_COST_BPS, SMALLCAP_COST_BPS,
    MIN_MARKET_CAP, MAX_FILING_LAG_MONTHS,
)

FEATURE_SETS_PATH = ROOT / 'models' / 'feature_sets_3y.json'
RESULTS_JSON = ROOT / 'reports' / 'feature_ablation_results.json'
RESULTS_MD = ROOT / 'reports' / 'feature_ablation_results.md'


def run_single_backtest(df: pd.DataFrame, spy_returns: dict,
                        monthly_px: pd.DataFrame | None) -> dict:
    """Run composite strategy backtest on a scored dataframe. Returns result dict."""
    return run_backtest(
        df, filter_composite, 'ablation_run', top_n=20, market=None,
        cost_bps=DEFAULT_COST_BPS, smallcap_cost_bps=SMALLCAP_COST_BPS,
        min_market_cap=MIN_MARKET_CAP, vol_weighted=True,
        fill_missing_return=None,
        max_filing_lag_months=MAX_FILING_LAG_MONTHS,
        spy_returns=spy_returns, monthly_px=monthly_px,
        use_adtv_filter=monthly_px is not None,
    )


def extract_metrics(result: dict) -> dict:
    """Pull the three key metrics from a backtest result."""
    if result.get('n_years', 0) == 0:
        return {'sharpe': None, 'cagr_pct': None, 'hit_rate_pct': None}
    return {
        'sharpe': result.get('sharpe'),
        'cagr_pct': result.get('cagr_pct'),
        'hit_rate_pct': result.get('hit_rate_pct'),
    }


def main():
    parser = argparse.ArgumentParser(description='Feature ablation study')
    parser.add_argument('--top', type=int, default=20,
                        help='Number of features to ablate (default 20)')
    parser.add_argument('--fast', action='store_true',
                        help='Skip monthly price cache for faster runs')
    args = parser.parse_args()

    with open(FEATURE_SETS_PATH) as f:
        feat_set = json.load(f)
    features = feat_set['features'][:args.top]
    print(f'Feature ablation: testing {len(features)} features from feature_sets_3y.json')

    spy_returns = load_spy_returns()
    monthly_px = None if args.fast else load_monthly_prices()

    # ── Baseline run (all features) ──────────────────────────────────────────
    print('\n[Baseline] Loading + scoring full dataset...', flush=True)
    t0 = time.time()
    df_base = load_full_hist()
    df_base = load_and_score(df_base)
    baseline_result = run_single_backtest(df_base, spy_returns, monthly_px)
    baseline = extract_metrics(baseline_result)
    t_base = time.time() - t0
    print(f'  Baseline: Sharpe={baseline["sharpe"]}, CAGR={baseline["cagr_pct"]}%, '
          f'Hit={baseline["hit_rate_pct"]}%  ({t_base:.0f}s)')

    # ── Ablation runs ────────────────────────────────────────────────────────
    results = []
    for i, feat in enumerate(features):
        print(f'\n[{i+1}/{len(features)}] Ablating: {feat}', flush=True)
        t1 = time.time()

        df_ablated = load_full_hist()
        if feat in df_ablated.columns:
            df_ablated = df_ablated.drop(columns=[feat])

        df_ablated = load_and_score(df_ablated)
        ablated_result = run_single_backtest(df_ablated, spy_returns, monthly_px)
        metrics = extract_metrics(ablated_result)

        sharpe_delta = None
        cagr_delta = None
        hit_delta = None
        if metrics['sharpe'] is not None and baseline['sharpe'] is not None:
            sharpe_delta = round(metrics['sharpe'] - baseline['sharpe'], 4)
        if metrics['cagr_pct'] is not None and baseline['cagr_pct'] is not None:
            cagr_delta = round(metrics['cagr_pct'] - baseline['cagr_pct'], 2)
        if metrics['hit_rate_pct'] is not None and baseline['hit_rate_pct'] is not None:
            hit_delta = round(metrics['hit_rate_pct'] - baseline['hit_rate_pct'], 1)

        elapsed = time.time() - t1
        results.append({
            'feature': feat,
            'sharpe_with_removed': metrics['sharpe'],
            'cagr_pct_with_removed': metrics['cagr_pct'],
            'hit_rate_pct_with_removed': metrics['hit_rate_pct'],
            'sharpe_delta': sharpe_delta,
            'cagr_delta_pct': cagr_delta,
            'hit_rate_delta_pct': hit_delta,
            'time_s': round(elapsed, 1),
        })
        print(f'  Sharpe Δ={sharpe_delta:+.4f}  CAGR Δ={cagr_delta:+.2f}%  '
              f'Hit Δ={hit_delta:+.1f}%  ({elapsed:.0f}s)')

    # ── Sort by Sharpe impact (most damaging removal first) ──────────────────
    results.sort(key=lambda r: r['sharpe_delta'] if r['sharpe_delta'] is not None else 0)

    # ── Save JSON ────────────────────────────────────────────────────────────
    output = {
        'generated': pd.Timestamp.now().isoformat(),
        'horizon': '3y',
        'strategy': 'composite',
        'n_features_tested': len(features),
        'baseline': baseline,
        'ablation_results': results,
    }
    RESULTS_JSON.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_JSON.write_text(json.dumps(output, indent=2))
    print(f'\nSaved: {RESULTS_JSON}')

    # ── Save markdown report ─────────────────────────────────────────────────
    lines = [
        '# Feature Ablation Results',
        '',
        f'**Horizon**: 3y | **Strategy**: Composite | **Date**: {pd.Timestamp.now().date()}',
        '',
        f'**Baseline**: Sharpe={baseline["sharpe"]}, CAGR={baseline["cagr_pct"]}%, '
        f'Hit Rate={baseline["hit_rate_pct"]}%',
        '',
        '## Impact Table (sorted by Sharpe impact)',
        '',
        '| # | Feature | Sharpe Δ | CAGR Δ (%) | Hit Δ (%) | Verdict |',
        '|---|---------|----------|------------|-----------|---------|',
    ]
    for i, r in enumerate(results):
        sd = r['sharpe_delta']
        if sd is None:
            verdict = '❓ N/A'
        elif sd < -0.10:
            verdict = '🔴 LOAD-BEARING'
        elif sd < -0.05:
            verdict = '🟡 Important'
        elif sd > 0.02:
            verdict = '🟢 Prune candidate'
        else:
            verdict = '⚪ Neutral'
        lines.append(
            f'| {i+1} | {r["feature"]} | {sd:+.4f} | '
            f'{r["cagr_delta_pct"]:+.2f} | {r["hit_rate_delta_pct"]:+.1f} | {verdict} |'
        )

    lines.extend([
        '',
        '## Interpretation',
        '',
        '- **LOAD-BEARING** (Sharpe drop > 0.10): Removing this feature significantly hurts performance.',
        '- **Important** (Sharpe drop 0.05–0.10): Contributes meaningfully but not critical alone.',
        '- **Prune candidate** (Sharpe improves > 0.02): Removing improves metrics — likely noise/overfitting.',
        '- **Neutral** (within ±0.05): Feature has minimal marginal impact given other features present.',
    ])
    RESULTS_MD.write_text('\n'.join(lines) + '\n')
    print(f'Saved: {RESULTS_MD}')

    # ── Summary ──────────────────────────────────────────────────────────────
    load_bearing = [r for r in results if r['sharpe_delta'] is not None and r['sharpe_delta'] < -0.10]
    prune_cands = [r for r in results if r['sharpe_delta'] is not None and r['sharpe_delta'] > 0.02]
    print(f'\n═══ Summary ═══')
    print(f'  Load-bearing features (Sharpe drop > 0.10): {len(load_bearing)}')
    for r in load_bearing:
        print(f'    • {r["feature"]}: Sharpe Δ={r["sharpe_delta"]:+.4f}')
    print(f'  Prune candidates (Sharpe improves > 0.02): {len(prune_cands)}')
    for r in prune_cands:
        print(f'    • {r["feature"]}: Sharpe Δ={r["sharpe_delta"]:+.4f}')


if __name__ == '__main__':
    main()
