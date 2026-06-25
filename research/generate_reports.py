"""
Report generator: PDF tearsheet, weekly picks CSV, rolling OOS AUC chart.

Reads:
  data/backtest_results.json   — strategy performance (from backtester.py)
  data/portfolio_backtest.json — IC-weighted Kelly portfolio backtest (from build_portfolio.py)
  data/portfolio_holdings.json — current-year top holdings (from build_portfolio.py)
  models/model_meta.json       — per-horizon val/test AUC
  data/historical_dataset_clean.parquet — for current-year picks

Outputs:
  reports/tearsheet.pdf        — multi-page PDF: cumulative wealth, annual returns,
                                  drawdown, rolling Sharpe, OOS AUC, score distribution,
                                  portfolio tearsheet (Kelly portfolio page)
  reports/weekly_picks.csv     — top-N picks for the most recent fiscal year
  reports/rolling_oos_auc.png  — standalone OOS AUC chart (for README / CI artifact)

Usage:
    python3 scripts/generate_reports.py
    python3 scripts/generate_reports.py --top 25 --strategy composite
    python3 scripts/generate_reports.py --no-pdf   # CSV + PNG only
"""
from __future__ import annotations

import argparse
import json
import warnings
from _root import ROOT

BASE = ROOT
warnings.filterwarnings('ignore')

import joblib
import numpy as np
import pandas as pd
from pathlib import Path

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    from matplotlib.backends.backend_pdf import PdfPages
    MPL_OK = True
except ImportError:
    MPL_OK = False
    print('matplotlib not available — skipping PDF/PNG output')

BACKTEST            = BASE / 'data' / 'backtest_results.json'
PORTFOLIO_BACKTEST  = BASE / 'data' / 'portfolio_backtest.json'
PORTFOLIO_HOLDINGS  = BASE / 'data' / 'portfolio_holdings.json'
META_PATH           = BASE / 'models' / 'model_meta.json'
DATA_PATH           = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR          = BASE / 'models'
REPORTS             = BASE / 'reports'
REPORTS.mkdir(exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_backtest(strategy: str) -> dict | None:
    if not BACKTEST.exists():
        return None
    results = json.loads(BACKTEST.read_text())
    if isinstance(results, list):
        for r in results:
            if strategy in r.get('label', '').lower():
                return r
        return results[0] if results else None
    return results.get(strategy) or (list(results.values())[0] if results else None)


def _annual_df(result: dict) -> pd.DataFrame:
    rows = result.get('annual_returns', [])
    return pd.DataFrame(rows).set_index('year') if rows else pd.DataFrame()


def _cumulative_wealth(annual_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    port  = (1 + annual_df['port_pct']  / 100).cumprod()
    bench = (1 + annual_df['bench_pct'].fillna(0) / 100).cumprod()
    return port, bench


def _drawdown(cum: pd.Series) -> pd.Series:
    peak = cum.cummax()
    return (cum - peak) / peak * 100


def _load_meta() -> dict | None:
    if not META_PATH.exists():
        return None
    return json.loads(META_PATH.read_text())


def _load_portfolio_backtest() -> dict | None:
    if not PORTFOLIO_BACKTEST.exists():
        return None
    return json.loads(PORTFOLIO_BACKTEST.read_text())


def _load_portfolio_holdings() -> pd.DataFrame | None:
    if not PORTFOLIO_HOLDINGS.exists():
        return None
    return pd.read_json(PORTFOLIO_HOLDINGS, orient='records')


def _oos_auc_series(meta: dict) -> dict[str, dict]:
    """Extract val/test AUC per horizon from meta."""
    out = {}
    for h in ['1y', '3y', '5y']:
        m = meta.get(h, {})
        if m:
            out[h] = {
                'val_auc':          m.get('val_auc'),
                'test_auc':         m.get('test_auc'),
                'tuned_test_auc':   m.get('tuned_test_auc'),
                'ensemble_test_auc':m.get('ensemble_test_auc'),
                'train_cutoff':     m.get('train_cutoff'),
                'val_end':          m.get('val_end'),
            }
    return out


# ── Portfolio tearsheet ────────────────────────────────────────────────────────

def _fig_portfolio_tearsheet(pb: dict, holdings: pd.DataFrame | None) -> 'plt.Figure':
    annual = pb.get('annual_returns', [])
    adf = pd.DataFrame(annual).set_index('year') if annual else pd.DataFrame()

    fig = plt.figure(figsize=(14, 12))
    fig.suptitle('IC-Weighted Kelly Portfolio — Tearsheet',
                 fontsize=14, fontweight='bold', y=0.98)
    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.50, wspace=0.35)

    # 1. Cumulative wealth vs SPY
    ax1 = fig.add_subplot(gs[0, :])
    if not adf.empty and 'return_pct' in adf.columns:
        cum_port = (1 + adf['return_pct'] / 100).cumprod()
        ax1.plot(cum_port.index, cum_port.values, label='Kelly Portfolio',
                 color='#2196F3', lw=2)
        if 'spy_return_pct' in adf.columns:
            cum_spy = (1 + adf['spy_return_pct'].fillna(0) / 100).cumprod()
            ax1.plot(cum_spy.index, cum_spy.values, label='SPY',
                     color='#9E9E9E', lw=1.5, ls='--')
        ax1.axhline(1, color='black', lw=0.5, ls=':')
        ax1.set_title('Cumulative Wealth vs SPY (start=1)', fontsize=11)
        ax1.set_ylabel('Wealth')
        ax1.legend(fontsize=9)
        ax1.grid(True, alpha=0.3)

    # 2. Annual return bar
    ax2 = fig.add_subplot(gs[1, 0])
    if not adf.empty and 'return_pct' in adf.columns:
        rets = adf['return_pct'].fillna(0)
        colors = ['#4CAF50' if x >= 0 else '#F44336' for x in rets]
        ax2.bar(rets.index, rets.values, color=colors, width=0.7)
        ax2.axhline(0, color='black', lw=0.8)
        ax2.set_title('Annual Return (%)', fontsize=10)
        ax2.set_ylabel('%')
        ax2.grid(True, alpha=0.2, axis='y')

    # 3. Drawdown
    ax3 = fig.add_subplot(gs[1, 1])
    if not adf.empty and 'return_pct' in adf.columns:
        cum = (1 + adf['return_pct'].fillna(0) / 100).cumprod()
        dd = _drawdown(cum)
        ax3.fill_between(dd.index, dd.values, 0, color='#F44336', alpha=0.4)
        ax3.plot(dd.index, dd.values, color='#B71C1C', lw=1.2)
        ax3.set_title('Portfolio Drawdown (%)', fontsize=10)
        ax3.set_ylabel('%')
        ax3.grid(True, alpha=0.2, axis='y')

    # 4. KPI summary
    ax4 = fig.add_subplot(gs[2, 0])
    ax4.axis('off')
    kpis = [
        ('CAGR',          f'{pb.get("cagr_pct", "?"):+.1f}%'
                          if isinstance(pb.get("cagr_pct"), (int, float)) else '?'),
        ('SPY CAGR',      f'{pb.get("spy_cagr_pct", "?"):+.1f}%'
                          if isinstance(pb.get("spy_cagr_pct"), (int, float)) else '?'),
        ('Excess CAGR',   f'{pb.get("excess_cagr_pct", "?"):+.1f}%'
                          if isinstance(pb.get("excess_cagr_pct"), (int, float)) else '?'),
        ('Sharpe',        str(pb.get('sharpe', '?'))),
        ('Sortino',       str(pb.get('sortino', '?'))),
        ('Calmar',        str(pb.get('calmar', '?'))),
        ('Max DD',        f'{pb.get("max_drawdown_pct", "?"):.1f}%'
                          if isinstance(pb.get("max_drawdown_pct"), (int, float)) else '?'),
        ('VaR 95%',       f'{pb.get("var_95_pct", "?"):.1f}%'
                          if isinstance(pb.get("var_95_pct"), (int, float)) else '?'),
        ('CVaR 99%',      f'{pb.get("cvar_99_pct", "?"):.1f}%'
                          if isinstance(pb.get("cvar_99_pct"), (int, float)) else '?'),
        ('Avg Positions', str(pb.get('avg_positions', '?'))),
    ]
    y_pos = 0.95
    for label, val in kpis:
        ax4.text(0.05, y_pos, label, transform=ax4.transAxes, fontsize=9, color='#555555')
        ax4.text(0.65, y_pos, val,   transform=ax4.transAxes, fontsize=9, fontweight='bold')
        y_pos -= 0.09
    ax4.set_title('Portfolio KPIs', fontsize=10, pad=8)

    # 5. Top 10 holdings table
    ax5 = fig.add_subplot(gs[2, 1])
    ax5.axis('off')
    if holdings is not None and not holdings.empty:
        disp = holdings.head(10).copy()
        disp_cols = ['ticker']
        for c in ['composite_score', 'weight_pct', 'kelly_f']:
            if c in disp.columns:
                disp_cols.append(c)
        tbl_data = disp[disp_cols]
        for c in tbl_data.select_dtypes(include='float').columns:
            tbl_data = tbl_data.copy()
            tbl_data[c] = tbl_data[c].round(3)
        table = ax5.table(
            cellText=tbl_data.values,
            colLabels=tbl_data.columns,
            cellLoc='center',
            loc='center',
        )
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1, 1.35)
        for j in range(len(tbl_data.columns)):
            table[0, j].set_facecolor('#1565C0')
            table[0, j].set_text_props(color='white', fontweight='bold')
        for i in range(1, len(tbl_data) + 1):
            color = '#E3F2FD' if i % 2 == 0 else 'white'
            for j in range(len(tbl_data.columns)):
                table[i, j].set_facecolor(color)
        ax5.set_title('Top 10 Holdings', fontsize=10, pad=8)

    return fig


# ── Current picks ─────────────────────────────────────────────────────────────

def generate_weekly_picks(top_n: int = 20) -> pd.DataFrame | None:
    if not DATA_PATH.exists():
        print('  No dataset found — skipping weekly picks')
        return None
    if not META_PATH.exists():
        print('  No model_meta.json — skipping weekly picks')
        return None

    meta = _load_meta()
    df   = pd.read_parquet(DATA_PATH)
    df   = df[df['period_type'] == 'annual'].copy()
    df   = df.drop_duplicates(subset=['ticker', 'fiscal_year'], keep='first')

    latest_year = int(df['fiscal_year'].max())
    df_latest   = df[df['fiscal_year'] == latest_year].copy()
    print(f'  Scoring {len(df_latest):,} companies for FY{latest_year}')

    score_cols: list[str] = []
    for h in ['1y', '3y', '5y']:
        m = meta.get(h, {})
        if not m:
            continue
        # prefer calibrated ensemble, then base model
        for suffix in ['_calibrated', '']:
            mp = MODELS_DIR / f'model_{h}{suffix}.joblib'
            if mp.exists():
                model = joblib.load(mp)
                feats  = [f for f in m['features'] if f in df_latest.columns]
                if feats:
                    X = df_latest[feats].fillna(pd.Series(m['train_medians']))
                    df_latest[f'score_{h}'] = model.predict_proba(X)[:, 1]
                    score_cols.append(f'score_{h}')
                break

    if not score_cols:
        print('  No model scores — skipping weekly picks')
        return None

    df_latest['composite_score'] = df_latest[score_cols].mean(axis=1)
    df_latest = df_latest.sort_values('composite_score', ascending=False)

    keep_cols = ['ticker', 'fiscal_year', 'composite_score'] + score_cols
    for opt in ['company_name', 'market', 'sic_code', 'total_assets',
                'revenue', 'beneish_m_score', 'piotroski_f_score']:
        if opt in df_latest.columns:
            keep_cols.append(opt)

    picks = df_latest[keep_cols].head(top_n).reset_index(drop=True)
    picks.index += 1  # rank 1-based

    out_path = REPORTS / 'weekly_picks.csv'
    picks.to_csv(out_path)
    print(f'  Saved weekly_picks.csv — {len(picks)} picks for FY{latest_year}')
    return picks


# ── Charts ─────────────────────────────────────────────────────────────────────

def _fig_performance(result: dict) -> 'plt.Figure':
    adf = _annual_df(result)
    fig = plt.figure(figsize=(14, 10))
    fig.suptitle(f'Strategy Tearsheet — {result.get("label", "Unknown")}',
                 fontsize=14, fontweight='bold', y=0.98)
    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.45, wspace=0.35)

    # 1. Cumulative wealth
    ax1 = fig.add_subplot(gs[0, :])
    if not adf.empty:
        port, bench = _cumulative_wealth(adf)
        ax1.plot(port.index,  port.values,  label='Portfolio',  color='#2196F3', lw=2)
        ax1.plot(bench.index, bench.values, label='Benchmark',  color='#9E9E9E', lw=1.5, ls='--')
        ax1.axhline(1, color='black', lw=0.5, ls=':')
        ax1.set_title('Cumulative Wealth (start=1)', fontsize=11)
        ax1.set_ylabel('Wealth')
        ax1.legend(fontsize=9)
        ax1.grid(True, alpha=0.3)

    # 2. Annual excess return
    ax2 = fig.add_subplot(gs[1, 0])
    if not adf.empty and 'excess_pct' in adf.columns:
        excess = adf['excess_pct'].fillna(0)
        colors = ['#4CAF50' if x >= 0 else '#F44336' for x in excess]
        ax2.bar(excess.index, excess.values, color=colors, width=0.7)
        ax2.axhline(0, color='black', lw=0.8)
        ax2.set_title('Annual Excess Return (%)', fontsize=10)
        ax2.set_ylabel('%')
        ax2.grid(True, alpha=0.2, axis='y')

    # 3. Drawdown
    ax3 = fig.add_subplot(gs[1, 1])
    if not adf.empty:
        port, _ = _cumulative_wealth(adf)
        dd = _drawdown(port)
        ax3.fill_between(dd.index, dd.values, 0, color='#F44336', alpha=0.4)
        ax3.plot(dd.index, dd.values, color='#B71C1C', lw=1.2)
        ax3.set_title('Portfolio Drawdown (%)', fontsize=10)
        ax3.set_ylabel('%')
        ax3.grid(True, alpha=0.2, axis='y')

    # 4. Rolling 3y Sharpe
    ax4 = fig.add_subplot(gs[2, 0])
    if not adf.empty and 'rolling_sharpe' in adf.columns:
        rs = adf['rolling_sharpe'].dropna()
        ax4.plot(rs.index, rs.values, color='#9C27B0', lw=1.8, marker='o', markersize=4)
        ax4.axhline(0, color='black', lw=0.8)
        ax4.axhline(1, color='green', lw=0.8, ls='--', alpha=0.6)
        ax4.set_title('Rolling 3y Sharpe Ratio', fontsize=10)
        ax4.set_ylabel('Sharpe')
        ax4.grid(True, alpha=0.2)

    # 5. KPI summary text
    ax5 = fig.add_subplot(gs[2, 1])
    ax5.axis('off')
    kpis = [
        ('CAGR (net)',       f'{result.get("cagr_pct", "?"):+.1f}%'),
        ('Bench CAGR',       f'{result.get("bench_cagr_pct", "?"):+.1f}%'),
        ('Excess CAGR',      f'{result.get("excess_cagr_pct", "?"):+.1f}%'),
        ('Sharpe',           str(result.get('sharpe', '?'))),
        ('Sortino',          str(result.get('sortino', '?'))),
        ('Calmar',           str(result.get('calmar', '?'))),
        ('Max Drawdown',     f'{result.get("max_drawdown_pct", "?"):.1f}%'),
        ('Hit Rate',         f'{result.get("hit_rate_pct", "?"):.0f}%'),
        ('Avg Cost Drag',    f'{result.get("avg_cost_drag_bps", "?"):.0f} bps'),
    ]
    y_pos = 0.95
    for label, val in kpis:
        ax5.text(0.05, y_pos, label, transform=ax5.transAxes, fontsize=9, color='#555555')
        ax5.text(0.70, y_pos, val,   transform=ax5.transAxes, fontsize=9, fontweight='bold')
        y_pos -= 0.10
    ax5.set_title('Key Metrics', fontsize=10, pad=8)

    return fig


def _fig_oos_auc(oos: dict) -> 'plt.Figure':
    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle('Out-of-Sample AUC by Horizon', fontsize=13, fontweight='bold')

    horizons  = [h for h in ['1y', '3y', '5y'] if h in oos]
    x         = np.arange(len(horizons))
    width     = 0.18
    bar_specs = [
        ('val_auc',          'Val AUC',           '#42A5F5'),
        ('test_auc',         'Test AUC',           '#1565C0'),
        ('tuned_test_auc',   'Tuned Test AUC',     '#66BB6A'),
        ('ensemble_test_auc','Ensemble Test AUC',  '#2E7D32'),
    ]

    for i, (key, lbl, color) in enumerate(bar_specs):
        vals = [oos[h].get(key) for h in horizons]
        vals = [v if v is not None else 0 for v in vals]
        if any(v > 0 for v in vals):
            bars = ax.bar(x + (i - 1.5) * width, vals, width,
                          label=lbl, color=color, alpha=0.85)
            for bar, v in zip(bars, vals):
                if v > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.002,
                            f'{v:.3f}', ha='center', va='bottom', fontsize=7)

    ax.axhline(0.5, color='red', ls='--', lw=1.2, alpha=0.7, label='Random (0.5)')
    ax.set_xticks(x)
    ax.set_xticklabels([f'{h} horizon' for h in horizons], fontsize=11)
    ax.set_ylabel('ROC AUC')
    ax.set_ylim(0.45, min(1.0, ax.get_ylim()[1] + 0.05))
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.25, axis='y')

    # Annotate cutoff years
    for j, h in enumerate(horizons):
        cutoff = oos[h].get('train_cutoff')
        val_end = oos[h].get('val_end')
        if cutoff and val_end:
            ax.text(x[j], 0.462,
                    f'train≤{cutoff}\nval {cutoff+1}–{val_end}',
                    ha='center', va='bottom', fontsize=7, color='#666666')

    fig.tight_layout()
    return fig


def _fig_picks_preview(picks: pd.DataFrame) -> 'plt.Figure':
    fig, ax = plt.subplots(figsize=(12, max(4, len(picks) * 0.35 + 1.5)))
    ax.axis('off')

    disp_cols = ['ticker', 'composite_score']
    for c in ['score_1y', 'score_3y', 'score_5y', 'beneish_m_score', 'market']:
        if c in picks.columns:
            disp_cols.append(c)

    tbl = picks[disp_cols].copy()
    for c in tbl.select_dtypes(include='float').columns:
        tbl[c] = tbl[c].round(4)

    table = ax.table(
        cellText=tbl.values,
        colLabels=tbl.columns,
        rowLabels=picks.index.astype(str),
        cellLoc='center',
        loc='center',
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8.5)
    table.scale(1, 1.4)

    # Colour header row
    for j in range(len(tbl.columns)):
        table[0, j].set_facecolor('#1565C0')
        table[0, j].set_text_props(color='white', fontweight='bold')

    # Alternate row shading
    for i in range(1, len(tbl) + 1):
        color = '#E3F2FD' if i % 2 == 0 else 'white'
        for j in range(len(tbl.columns)):
            table[i, j].set_facecolor(color)

    ax.set_title(f'Top {len(picks)} Picks — FY{picks["fiscal_year"].iloc[0]}',
                 fontsize=12, fontweight='bold', pad=12)
    fig.tight_layout()
    return fig


# ── PDF assembly ──────────────────────────────────────────────────────────────

def generate_pdf(result: dict | None, oos: dict | None,
                 picks: pd.DataFrame | None, out_path: Path,
                 portfolio_backtest: dict | None = None,
                 portfolio_holdings: pd.DataFrame | None = None) -> None:
    if not MPL_OK:
        return

    import matplotlib
    matplotlib.rcParams.update({
        'font.family':   'DejaVu Sans',
        'axes.spines.top':    False,
        'axes.spines.right':  False,
    })

    with PdfPages(str(out_path)) as pdf:
        # Cover page
        fig_cover, ax = plt.subplots(figsize=(11, 8.5))
        ax.axis('off')
        from datetime import date
        ax.text(0.5, 0.60, 'Stock Fraud Screener',
                ha='center', va='center', fontsize=28, fontweight='bold',
                transform=ax.transAxes)
        ax.text(0.5, 0.50, 'Strategy Tearsheet & Model Report',
                ha='center', va='center', fontsize=16, color='#555555',
                transform=ax.transAxes)
        ax.text(0.5, 0.38, f'Generated: {date.today().isoformat()}',
                ha='center', va='center', fontsize=12, color='#888888',
                transform=ax.transAxes)
        pdf.savefig(fig_cover, bbox_inches='tight')
        plt.close(fig_cover)

        # Performance page
        if result is not None and result.get('n_years', 0) > 0:
            fig_perf = _fig_performance(result)
            pdf.savefig(fig_perf, bbox_inches='tight')
            plt.close(fig_perf)

        # Kelly portfolio tearsheet
        if portfolio_backtest is not None and portfolio_backtest.get('annual_returns'):
            fig_port = _fig_portfolio_tearsheet(portfolio_backtest, portfolio_holdings)
            pdf.savefig(fig_port, bbox_inches='tight')
            plt.close(fig_port)

        # OOS AUC page
        if oos:
            fig_auc = _fig_oos_auc(oos)
            pdf.savefig(fig_auc, bbox_inches='tight')
            plt.close(fig_auc)

        # Picks preview
        if picks is not None and not picks.empty:
            fig_picks = _fig_picks_preview(picks)
            pdf.savefig(fig_picks, bbox_inches='tight')
            plt.close(fig_picks)

    print(f'  Saved tearsheet → {out_path}')


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--strategy', default='composite',
                        help='Strategy label to pull from backtest_results.json')
    parser.add_argument('--top', type=int, default=20,
                        help='Number of picks for weekly_picks.csv (default: 20)')
    parser.add_argument('--no-pdf', action='store_true',
                        help='Skip PDF / PNG generation (CSV only)')
    args = parser.parse_args()

    print('── Generating reports ──────────────────────────────────────────')

    # Weekly picks CSV
    picks = generate_weekly_picks(top_n=args.top)

    if args.no_pdf:
        return

    if not MPL_OK:
        print('  matplotlib not installed — skipping visual reports')
        return

    # Load data
    result = _load_backtest(args.strategy)
    if result is None:
        print(f'  No backtest results for "{args.strategy}" — run backtester.py first')

    meta = _load_meta()
    oos  = _oos_auc_series(meta) if meta else None
    if not oos:
        print('  No model_meta.json — AUC page will be empty')

    portfolio_backtest = _load_portfolio_backtest()
    if portfolio_backtest is None:
        print('  No portfolio_backtest.json — Kelly portfolio page will be skipped')
    portfolio_holdings = _load_portfolio_holdings()

    # Standalone OOS AUC PNG (for CI artifacts, README)
    if oos:
        fig_auc = _fig_oos_auc(oos)
        png_path = REPORTS / 'rolling_oos_auc.png'
        fig_auc.savefig(str(png_path), dpi=150, bbox_inches='tight')
        plt.close(fig_auc)
        print(f'  Saved rolling_oos_auc.png → {png_path}')

    # Full PDF tearsheet
    pdf_path = REPORTS / 'tearsheet.pdf'
    generate_pdf(result, oos, picks, pdf_path,
                 portfolio_backtest=portfolio_backtest,
                 portfolio_holdings=portfolio_holdings)

    print('── Done ─────────────────────────────────────────────────────────')


if __name__ == '__main__':
    main()
