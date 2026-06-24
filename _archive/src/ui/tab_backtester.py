from __future__ import annotations

import json
import subprocess
import sys

import numpy as np
import pandas as pd
import streamlit as st

from src.config import BASE, BT_PATH

PORTFOLIO_BT_PATH  = BASE / 'data' / 'portfolio_backtest.json'
PORTFOLIO_HOL_PATH = BASE / 'data' / 'portfolio_holdings.json'
ALPHA_REGISTRY     = BASE / 'data' / 'alpha_registry.json'


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
        rets   = [r['port_pct'] for r in ann]
        bench  = [r.get('bench_pct', 0) for r in ann]
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
        rets   = np.array([r['port_pct'] / 100 for r in ann])
        wealth = np.cumprod(1 + rets)
        ax2.plot(years, wealth, lw=2, label=s.get('label', key),
                 color=colors_map.get(key, None))
        if not plotted_bench:
            bench    = np.array([r.get('bench_pct', 0) / 100 for r in ann])
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

    # ── Kelly Portfolio Tearsheet ─────────────────────────────────────────────
    st.markdown('---')
    st.subheader('💼 IC-Weighted Kelly Portfolio')

    if PORTFOLIO_BT_PATH.exists():
        pb = json.loads(PORTFOLIO_BT_PATH.read_text())
        annual = pb.get('annual_returns', [])

        if annual:
            adf = pd.DataFrame(annual).set_index('year')

            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric('CAGR', f"{pb.get('cagr_pct', 0):+.1f}%")
            col2.metric('Sharpe', f"{pb.get('sharpe', 0):.2f}" if pb.get('sharpe') else 'N/A')
            col3.metric('Max DD', f"{pb.get('max_drawdown_pct', 0):.1f}%")
            col4.metric('VaR 95%', f"{pb.get('var_95_pct', 0):.1f}%" if pb.get('var_95_pct') is not None else 'N/A')
            col5.metric('CVaR 99%', f"{pb.get('cvar_99_pct', 0):.1f}%" if pb.get('cvar_99_pct') is not None else 'N/A')

            # Cumulative wealth vs SPY
            fig_p, ax_p = plt.subplots(figsize=(12, 3.5))
            cum_port = (1 + adf['return_pct'] / 100).cumprod()
            ax_p.plot(cum_port.index, cum_port.values, lw=2, label='Kelly Portfolio', color='#2196F3')
            if 'spy_return_pct' in adf.columns:
                cum_spy = (1 + adf['spy_return_pct'].fillna(0) / 100).cumprod()
                ax_p.plot(cum_spy.index, cum_spy.values, lw=1.5, ls='--', color='#9E9E9E', label='SPY')
            ax_p.axhline(1, color='black', lw=0.5, ls=':')
            ax_p.set_ylabel('Wealth')
            ax_p.set_title('Cumulative Wealth — Kelly Portfolio vs SPY', fontweight='bold')
            ax_p.legend()
            ax_p.grid(alpha=0.3)
            plt.tight_layout()
            st.pyplot(fig_p, use_container_width=True)
            plt.close()

            # Annual return bar
            fig_a, ax_a = plt.subplots(figsize=(12, 3))
            rets = adf['return_pct'].fillna(0)
            colors_a = ['#4CAF50' if v >= 0 else '#F44336' for v in rets]
            ax_a.bar(rets.index, rets.values, color=colors_a, width=0.7, edgecolor='white')
            ax_a.axhline(0, color='black', lw=0.8)
            ax_a.set_ylabel('%')
            ax_a.set_title('Annual Return (%)', fontweight='bold')
            ax_a.grid(alpha=0.2, axis='y')
            plt.tight_layout()
            st.pyplot(fig_a, use_container_width=True)
            plt.close()

            # Holdings table
            if PORTFOLIO_HOL_PATH.exists():
                hol = pd.read_json(PORTFOLIO_HOL_PATH, orient='records')
                with st.expander('📋 Current Holdings', expanded=False):
                    disp_cols = ['ticker']
                    for c in ['composite_score', 'weight_pct', 'kelly_f', 'market']:
                        if c in hol.columns:
                            disp_cols.append(c)
                    st.dataframe(hol[disp_cols].round(4), use_container_width=True)
        else:
            st.info('portfolio_backtest.json exists but has no annual_returns — run build_portfolio.py.')
    else:
        st.info(
            '**Kelly portfolio not yet built.**  \n'
            'Run: `python3 scripts/build_portfolio.py`'
        )

    # ── Alpha Signal Browser ──────────────────────────────────────────────────
    st.markdown('---')
    st.subheader('🔍 Alpha Signal Browser')
    st.caption('Selected signals from alpha_registry.json — IC, ICIR, bootstrap confidence intervals.')

    if ALPHA_REGISTRY.exists():
        reg = json.loads(ALPHA_REGISTRY.read_text())
        signals = reg.get('signals', [])
        if signals:
            sig_df = pd.DataFrame(signals)
            # normalise column names that may differ across versions
            rename_map = {
                'ic_mean': 'IC Mean',
                'icir': 'ICIR',
                'n_years': 'Years',
                'selected': 'Selected',
                'cagr_pct': 'CAGR %',
                'sharpe': 'Sharpe',
                'cagr_bootstrap_mean_pct':   'CAGR CI Mean',
                'cagr_bootstrap_1sigma_pct': 'CAGR CI 1σ',
                'sharpe_bootstrap_mean':     'Sharpe CI Mean',
                'sharpe_bootstrap_1sigma':   'Sharpe CI 1σ',
            }
            sig_df = sig_df.rename(columns={k: v for k, v in rename_map.items() if k in sig_df.columns})

            # Filter controls
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                show_selected = st.checkbox('Selected signals only', value=True)
            with col_f2:
                sort_col = st.selectbox(
                    'Sort by',
                    [c for c in ['ICIR', 'IC Mean', 'CAGR %', 'Sharpe', 'signal_id']
                     if c in sig_df.columns],
                    index=0
                )

            disp = sig_df.copy()
            if show_selected and 'Selected' in disp.columns:
                disp = disp[disp['Selected'] == True]  # noqa: E712
            if sort_col in disp.columns:
                disp = disp.sort_values(sort_col, ascending=False)

            table_cols = ['signal_id']
            for c in ['IC Mean', 'ICIR', 'CAGR %', 'Sharpe', 'Years',
                      'CAGR CI Mean', 'CAGR CI 1σ', 'Sharpe CI Mean', 'Sharpe CI 1σ']:
                if c in disp.columns:
                    table_cols.append(c)

            float_cols = [c for c in table_cols if c != 'signal_id' and c in disp.columns]
            for c in float_cols:
                disp[c] = pd.to_numeric(disp[c], errors='coerce').round(4)

            st.dataframe(disp[table_cols].reset_index(drop=True),
                         use_container_width=True, hide_index=True)

            # IC bar chart for top signals
            if 'IC Mean' in disp.columns and 'signal_id' in disp.columns:
                top = disp[['signal_id', 'IC Mean']].dropna().head(25)
                if not top.empty:
                    fig_ic, ax_ic = plt.subplots(figsize=(12, max(3, len(top) * 0.32)))
                    colors_ic = ['#4CAF50' if v >= 0.03 else ('#FFC107' if v >= 0 else '#F44336')
                                 for v in top['IC Mean']]
                    ax_ic.barh(top['signal_id'], top['IC Mean'], color=colors_ic, edgecolor='white')
                    ax_ic.axvline(0.02, color='green', ls='--', lw=1, alpha=0.7, label='IC target 0.02')
                    ax_ic.axvline(0.0,  color='red',   ls=':',  lw=0.8)
                    ax_ic.set_xlabel('IC Mean (Spearman vs forward_return_1y)')
                    ax_ic.set_title('Alpha Signals — IC Mean (top 25 by ICIR)', fontweight='bold')
                    ax_ic.invert_yaxis()
                    ax_ic.legend(fontsize=8)
                    ax_ic.grid(alpha=0.25, axis='x')
                    plt.tight_layout()
                    st.pyplot(fig_ic, use_container_width=True)
                    plt.close()
        else:
            st.info('alpha_registry.json is empty — run build_alpha_registry.py.')
    else:
        st.info(
            '**Alpha registry not found.**  \n'
            'Run: `python3 scripts/build_alpha_registry.py`'
        )
