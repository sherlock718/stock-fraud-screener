from __future__ import annotations

import json
import subprocess
import sys

import numpy as np
import pandas as pd
import streamlit as st

from src.config import BASE, BT_PATH


def tab_backtester() -> None:
    import matplotlib.pyplot as plt

    st.header('📈 Walk-Forward Backtester')
    st.caption('Transaction costs deducted: 30 bps large-cap, 60 bps micro/small-cap (round-trip).')

    has_results = BT_PATH.exists()
    col_run, col_note = st.columns([1, 3])
    with col_run:
        run_bt = st.button('▶ Run / Refresh Backtest', type='primary')
    with col_note:
        if has_results:
            ts = json.loads(BT_PATH.read_text()).get('generated_at', '')
            st.caption(f'Last run: {ts}')
        else:
            st.caption('No results yet — click Run to generate.')

    if run_bt:
        with st.spinner('Running walk-forward backtest (1–2 min)…'):
            res = subprocess.run(
                [sys.executable, str(BASE / 'scripts' / 'backtester.py'), '--strategy', 'all'],
                capture_output=True, text=True, cwd=str(BASE)
            )
        if res.returncode == 0:
            st.success('Backtest complete!')
        else:
            st.error(f'Backtester failed:\n{res.stderr[-1000:]}')
            return

    if not BT_PATH.exists():
        st.info('No backtest results found. Click **Run / Refresh Backtest** above.')
        return

    data = json.loads(BT_PATH.read_text())
    strats = data.get('strategies', {})

    st.subheader('Strategy Summary')
    rows = []
    for key, s in strats.items():
        rows.append({
            'Strategy':        s.get('label', key),
            'CAGR':            f"{s.get('cagr_pct', 0):.1f}%",
            'Bench CAGR':      f"{s.get('bench_cagr_pct', 0):.1f}%",
            'Excess CAGR':     f"{s.get('excess_cagr_pct', 0):.1f}%",
            'Sharpe':          f"{s.get('sharpe', 0):.2f}" if s.get('sharpe') else 'N/A',
            'Info Ratio':      f"{s.get('info_ratio', 0):.2f}" if s.get('info_ratio') else 'N/A',
            'Max Drawdown':    f"{s.get('max_drawdown_pct', 0):.1f}%",
            'Hit Rate':        f"{s.get('hit_rate_pct', 0):.0f}%",
            'Cost Drag (bps)': f"{s.get('avg_cost_drag_bps', 0):.0f}",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

    st.subheader('Annual Returns by Strategy')
    n_strats = len(strats)
    fig, axes = plt.subplots(1, n_strats, figsize=(6 * n_strats, 4), sharey=False)
    if n_strats == 1:
        axes = [axes]
    for ax, (key, s) in zip(axes, strats.items()):
        ann = s.get('annual_returns', [])
        if not ann:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center', transform=ax.transAxes)
            continue
        years  = [r['year'] for r in ann]
        rets   = [r['port_ret'] * 100 for r in ann]
        bench  = [r.get('bench_ret', 0) * 100 for r in ann]
        colors = ['#4CAF50' if v >= 0 else '#F44336' for v in rets]
        ax.bar(years, rets, color=colors, alpha=0.85, label='Strategy', edgecolor='white')
        ax.plot(years, bench, color='gray', lw=1.5, ls='--', label='Benchmark', marker='o', ms=3)
        ax.axhline(0, color='black', lw=0.5)
        ax.set_title(s.get('label', key), fontsize=10, fontweight='bold')
        ax.set_ylabel('Return (%)')
        ax.legend(fontsize=8)
        ax.grid(axis='y', alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
    plt.tight_layout()
    st.pyplot(fig, use_container_width=True)
    plt.close()

    st.subheader('Cumulative Wealth Index ($1 invested)')
    fig2, ax2 = plt.subplots(figsize=(12, 4))
    colors_map = {'composite': '#2196F3', 'qem': '#4CAF50', 'scdv': '#FF9800', 'iarb': '#9C27B0'}
    plotted_bench = False
    for key, s in strats.items():
        ann = s.get('annual_returns', [])
        if not ann:
            continue
        years  = [r['year'] for r in ann]
        rets   = np.array([r['port_ret'] for r in ann])
        wealth = np.cumprod(1 + rets)
        ax2.plot(years, wealth, lw=2, label=s.get('label', key),
                 color=colors_map.get(key, None))
        if not plotted_bench:
            bench    = np.array([r.get('bench_ret', 0) for r in ann])
            b_wealth = np.cumprod(1 + bench)
            ax2.plot(years, b_wealth, lw=1.5, ls='--', color='gray',
                     label='Benchmark (equal-weight)')
            plotted_bench = True
    ax2.axhline(1, color='black', lw=0.5)
    ax2.set_ylabel('Portfolio value ($1 = start)')
    ax2.set_title('Walk-Forward Cumulative Return', fontweight='bold')
    ax2.legend()
    ax2.grid(alpha=0.3)
    plt.tight_layout()
    st.pyplot(fig2, use_container_width=True)
    plt.close()

    # --- Walk-Forward Model AUC (from train_models.py --walk-forward) ---
    reports_dir = BASE / 'reports'
    wf_files = {h: reports_dir / f'walk_forward_auc_{h}.csv' for h in ['1y', '3y', '5y']}
    available = {h: p for h, p in wf_files.items() if p.exists()}

    if available:
        st.markdown('---')
        st.subheader('🎯 ML Model Walk-Forward AUC')
        st.caption(
            'Expanding-window CV: train on all data up to year t, evaluate on year t+1. '
            'Target AUC ≥ 0.62. Generated by `python3 scripts/train_models.py --walk-forward`.'
        )

        wf_dfs = {h: pd.read_csv(p) for h, p in available.items()}

        # Summary table
        summary_rows = []
        for h, wf in wf_dfs.items():
            summary_rows.append({
                'Horizon': h,
                'Folds': len(wf),
                'Mean AUC': f"{wf['auc'].mean():.4f}",
                'Min AUC':  f"{wf['auc'].min():.4f}",
                'Max AUC':  f"{wf['auc'].max():.4f}",
                'Folds ≥ 0.62': int((wf['auc'] >= 0.62).sum()),
            })
        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

        # AUC over time chart
        horizon_colors = {'1y': '#2196F3', '3y': '#4CAF50', '5y': '#FF9800'}
        fig3, ax3 = plt.subplots(figsize=(12, 4))
        for h, wf in wf_dfs.items():
            ax3.plot(wf['test_year'], wf['auc'], lw=2, marker='o', ms=5,
                     label=f'{h} horizon', color=horizon_colors.get(h))
        ax3.axhline(0.62, color='red', ls='--', lw=1.2, label='Target (0.62)')
        ax3.axhline(0.50, color='gray', ls=':', lw=1.0, label='Random (0.50)')
        ax3.set_xlabel('Test Year')
        ax3.set_ylabel('ROC AUC')
        ax3.set_title('Walk-Forward AUC by Year (Expanding Window)', fontweight='bold')
        ax3.legend()
        ax3.grid(alpha=0.3)
        plt.tight_layout()
        st.pyplot(fig3, use_container_width=True)
        plt.close()

        # Per-fold detail table in expander
        with st.expander('📋 Per-fold AUC detail', expanded=False):
            tabs = st.tabs(list(available.keys()))
            for tab, (h, wf) in zip(tabs, wf_dfs.items()):
                with tab:
                    disp = wf.copy()
                    disp['auc'] = disp['auc'].round(4)
                    disp['flag'] = disp['auc'].apply(
                        lambda v: '✅' if v >= 0.62 else ('🟡' if v >= 0.55 else '🔴')
                    )
                    st.dataframe(disp, use_container_width=True, hide_index=True)
    else:
        st.markdown('---')
        st.info(
            '**Walk-Forward AUC not yet generated.**  \n'
            'Run: `python3 scripts/train_models.py --walk-forward`  \n'
            'Results will appear here automatically once the CSV files exist in `reports/`.'
        )
