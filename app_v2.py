"""
app_v2.py — Multi-market stock fraud & value screener
Tabbed layout: Screener | Backtester | Watchlist | Strategies
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
BASE       = Path(__file__).parent
DATA_PATH  = BASE / 'data' / 'app_data.parquet'
META_PATH  = BASE / 'models' / 'model_meta.json'
MODELS_DIR = BASE / 'models'
BT_PATH    = BASE / 'data' / 'backtest_results.json'
WL_PATH    = BASE / 'data' / 'watchlist.json'
WL_LIVE    = BASE / 'data' / 'watchlist_live.json'

STRAT_FILES = {
    'QEM — Quality + Earnings Momentum':  BASE / 'data' / 'strategy_qem.csv',
    'SCDV — Small-Cap Deep Value':         BASE / 'data' / 'strategy_scdv.csv',
    'IARB — International Arbitrage':      BASE / 'data' / 'strategy_iarb.csv',
}

MARKET_LABELS = {
    'US': '🇺🇸 United States', 'CA': '🇨🇦 Canada', 'BR': '🇧🇷 Brazil',
    'JP': '🇯🇵 Japan', 'DE': '🇩🇪 Germany', 'FR': '🇫🇷 France',
    'IT': '🇮🇹 Italy', 'ES': '🇪🇸 Spain', 'SE': '🇸🇪 Sweden',
    'FI': '🇫🇮 Finland', 'NL': '🇳🇱 Netherlands', 'PT': '🇵🇹 Portugal',
    'DK': '🇩🇰 Denmark',
}

# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    df = pd.read_parquet(DATA_PATH)
    return df[df['period_type'] == 'annual'].copy()


@st.cache_resource(show_spinner=False)
def load_models() -> tuple[dict, dict]:
    models, meta = {}, {}
    if META_PATH.exists():
        meta = json.loads(META_PATH.read_text())
    try:
        import joblib
        for h in ['1y', '3y', '5y']:
            p = MODELS_DIR / f'model_{h}.joblib'
            if p.exists():
                models[h] = joblib.load(p)
    except Exception:
        pass
    return models, meta


def score_companies(df: pd.DataFrame, models: dict, meta: dict,
                    horizon: str = '1y') -> pd.DataFrame:
    if horizon not in models or horizon not in meta:
        df['ml_score'] = np.nan
        return df
    clf  = models[horizon]
    feats = [f for f in meta[horizon]['features'] if f in df.columns]
    X = df[feats].fillna(df[feats].median())
    try:
        df = df.copy()
        df['ml_score'] = clf.predict_proba(X)[:, 1]
    except Exception:
        df['ml_score'] = np.nan
    return df


def composite_rank(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def pct_rank(s: pd.Series, ascending: bool = True) -> pd.Series:
        return s.rank(pct=True, ascending=ascending, na_option='keep')

    components = {}
    if 'value_composite' in df.columns:
        components['value']   = pct_rank(df['value_composite'], ascending=False)
    elif 'pe_ratio' in df.columns:
        components['value']   = pct_rank(df['pe_ratio'], ascending=True)
    if 'quality_composite' in df.columns:
        components['quality'] = pct_rank(df['quality_composite'], ascending=False)
    elif 'piotroski_f_score' in df.columns:
        components['quality'] = pct_rank(df['piotroski_f_score'], ascending=False)
    if 'momentum_12m_prior' in df.columns:
        components['momentum']     = pct_rank(df['momentum_12m_prior'], ascending=False)
    if 'beneish_m_score' in df.columns:
        components['fraud_safety'] = pct_rank(df['beneish_m_score'], ascending=True)
    if 'ml_score' in df.columns and df['ml_score'].notna().any():
        components['ml_alpha']     = pct_rank(df['ml_score'], ascending=False)

    if not components:
        df['composite_score'] = np.nan
        return df

    weights = {'value': 0.25, 'quality': 0.20, 'momentum': 0.20,
               'fraud_safety': 0.20, 'ml_alpha': 0.15}
    total_w = sum(weights[k] for k in components)
    df['composite_score'] = sum(components[k] * (weights[k] / total_w) for k in components)
    return df


# ── Tab: Screener ─────────────────────────────────────────────────────────────

def tab_screener(df_all: pd.DataFrame, models: dict, meta: dict) -> None:
    model_loaded = bool(models)

    with st.sidebar:
        st.title('🔍 Screener Controls')

        st.subheader('Markets')
        avail_markets   = sorted(df_all['market'].unique())
        market_options  = {MARKET_LABELS.get(m, m): m for m in avail_markets}
        selected_labels = st.multiselect(
            'Select markets',
            options=list(market_options.keys()),
            default=[MARKET_LABELS.get(m, m) for m in avail_markets if m != 'US'] or list(market_options.keys())[:3],
        )
        selected_markets = [market_options[l] for l in selected_labels] if selected_labels else avail_markets

        st.subheader('Fiscal Year')
        year_min   = int(df_all['fiscal_year'].min())
        year_max   = int(df_all['fiscal_year'].max())
        year_range = st.slider('Fiscal year range', year_min, year_max,
                               (max(year_min, year_max - 5), year_max))

        st.subheader('Company Size')
        cap_preset = st.radio('Market cap preset', [
            'All sizes', 'Neglected ($50M–$500M)', 'Small cap ($50M–$2B)',
            'Mid cap ($2B–$10B)', 'Large cap (>$10B)',
        ], index=0)
        cap_filter = {
            'All sizes':              (0,     1e15),
            'Neglected ($50M–$500M)': (50e6,  500e6),
            'Small cap ($50M–$2B)':   (50e6,  2e9),
            'Mid cap ($2B–$10B)':     (2e9,   10e9),
            'Large cap (>$10B)':      (10e9,  1e15),
        }[cap_preset]

        st.subheader('ML Scoring Horizon')
        if model_loaded:
            horizon = st.selectbox('Investment horizon', ['1y', '3y', '5y'], index=0)
        else:
            st.warning('Models not saved yet.\nRun: python3 scripts/train_models.py')
            horizon = '1y'

        st.subheader('Composite Score Filter')
        min_composite = st.slider('Min composite score (percentile)', 0, 100, 60) / 100

        st.subheader('Risk Filters')
        exclude_delisted = st.checkbox('Exclude likely delisted', value=True)
        exclude_beneish  = st.checkbox('Exclude Beneish M > -2.22 (manipulators)', value=True)
        exclude_altman   = st.checkbox('Exclude Altman Z < 1.81 (distressed)', value=False)

        st.subheader('Search')
        text_query = st.text_input('Ticker / company name', placeholder='e.g. AAPL, Samsung…')

    df = df_all[
        df_all['market'].isin(selected_markets) &
        df_all['fiscal_year'].between(*year_range)
    ].copy()

    if 'market_cap_at_filing' in df.columns:
        df = df[df['market_cap_at_filing'].isna() | df['market_cap_at_filing'].between(*cap_filter)]
    if exclude_delisted and 'likely_delisted' in df.columns:
        df = df[~df['likely_delisted']]
    if exclude_beneish and 'beneish_m_score' in df.columns:
        df = df[df['beneish_m_score'].isna() | (df['beneish_m_score'] <= -2.22)]
    if exclude_altman and 'altman_z_score' in df.columns:
        df = df[df['altman_z_score'].isna() | (df['altman_z_score'] >= 1.81)]
    if text_query:
        q    = text_query.lower()
        mask = (df['ticker'].str.lower().str.contains(q, na=False) |
                df['name'].str.lower().str.contains(q, na=False))
        df   = df[mask]

    df     = score_companies(df, models, meta, horizon)
    df     = composite_rank(df)
    latest = (df.sort_values('fiscal_year', ascending=False)
                .drop_duplicates('cik', keep='first'))
    latest = latest[latest['composite_score'] >= min_composite].copy()
    latest = latest.sort_values('composite_score', ascending=False)

    st.title('🔍 Stock Fraud & Value Screener')
    model_status = f'ML model loaded ({horizon} horizon)' if model_loaded else '⚠️ ML model not saved yet'
    st.caption(f'{len(df_all):,} total rows | {model_status} | {len(latest):,} companies after filters')

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric('Companies shown', f'{len(latest):,}')
    c2.metric('Markets', len(selected_markets))
    c3.metric('Fiscal years', f'{year_range[0]}–{year_range[1]}')
    if 'beneish_m_score' in latest.columns:
        c4.metric('High Beneish risk', f'{(latest["beneish_m_score"] > -2.22).sum():,}')
    else:
        c4.metric('High Beneish risk', 'N/A')
    if model_loaded and 'ml_score' in latest.columns:
        c5.metric('ML score > 0.6', f'{(latest["ml_score"] > 0.6).sum():,}')
    else:
        c5.metric('ML score > 0.6', 'N/A')

    st.subheader('Top Companies by Composite Score')
    display_cols = (['ticker', 'name', 'market', 'fiscal_year', 'composite_score'] +
                    [c for c in ['value_composite', 'pe_ratio', 'pb_ratio'] if c in latest.columns][:2] +
                    [c for c in ['quality_composite', 'piotroski_f_score', 'roe'] if c in latest.columns][:2] +
                    [c for c in ['momentum_12m_prior'] if c in latest.columns] +
                    [c for c in ['beneish_m_score', 'altman_z_score'] if c in latest.columns] +
                    (['ml_score'] if model_loaded and 'ml_score' in latest.columns else []) +
                    (['market_cap_at_filing'] if 'market_cap_at_filing' in latest.columns else []))
    display_cols = [c for c in display_cols if c in latest.columns]
    show_df = latest[display_cols].head(200).copy()
    for col in show_df.select_dtypes('float').columns:
        if col == 'composite_score':
            show_df[col] = show_df[col].map(lambda x: f'{x:.3f}' if pd.notna(x) else '')
        elif col == 'market_cap_at_filing':
            show_df[col] = show_df[col].map(
                lambda x: (f'${x/1e9:.1f}B' if pd.notna(x) and x >= 1e9
                           else (f'${x/1e6:.0f}M' if pd.notna(x) else ''))
            )
        elif col in ['pe_ratio', 'pb_ratio', 'ev_ebitda']:
            show_df[col] = show_df[col].map(lambda x: f'{x:.1f}' if pd.notna(x) else '')
        else:
            show_df[col] = show_df[col].map(lambda x: f'{x:.3f}' if pd.notna(x) else '')
    st.dataframe(show_df, use_container_width=True, height=520)

    st.subheader('Company Deep Dive')
    tickers_list    = [''] + sorted(latest['ticker'].dropna().unique().tolist())
    selected_ticker = st.selectbox('Select ticker', tickers_list, index=0)
    if selected_ticker:
        co_all    = df_all[df_all['ticker'] == selected_ticker].sort_values('fiscal_year')
        co_latest = co_all[co_all['period_type'] == 'annual'].tail(1)
        if co_latest.empty:
            st.warning('No annual data found for this ticker.')
        else:
            row = co_latest.iloc[0]
            st.markdown(f"### {row.get('name','N/A')} ({selected_ticker})")
            d1, d2, d3, d4 = st.columns(4)
            d1.metric('Market', MARKET_LABELS.get(str(row.get('market', '')), str(row.get('market', ''))))
            d2.metric('Fiscal Year', int(row['fiscal_year']) if pd.notna(row['fiscal_year']) else 'N/A')
            d3.metric('Exchange', str(row.get('exchange', 'N/A')))
            if pd.notna(row.get('market_cap_at_filing')):
                cap = row['market_cap_at_filing']
                d4.metric('Market Cap', f'${cap/1e9:.2f}B' if cap >= 1e9 else f'${cap/1e6:.0f}M')
            fin_cols = [c for c in ['revenue','gross_profit','operating_income','net_income',
                                    'total_assets','equity','operating_cash_flow'] if c in co_all.columns]
            if fin_cols:
                st.markdown('**Financials**')
                fin_data = co_all[co_all['period_type'] == 'annual'][['fiscal_year'] + fin_cols].set_index('fiscal_year')
                st.dataframe(fin_data.tail(8).applymap(
                    lambda x: f'{x/1e6:,.0f}M' if pd.notna(x) and isinstance(x, (int, float)) else x
                ), use_container_width=True)
            st.markdown('**Risk Signals**')
            rc1, rc2, rc3 = st.columns(3)
            for idx, (label, col, thresh, direction) in enumerate([
                ('Beneish M-Score', 'beneish_m_score', -2.22, 'below'),
                ('Altman Z-Score',  'altman_z_score',   1.81, 'above'),
                ('Piotroski F',     'piotroski_f_score', 6.0, 'above'),
            ]):
                c_obj = [rc1, rc2, rc3][idx]
                if col in row.index and pd.notna(row[col]):
                    val = row[col]
                    ok  = val <= thresh if direction == 'below' else val >= thresh
                    c_obj.metric(label, f'{val:.2f}',
                                 delta='✅ OK' if ok else '⚠️ Risk',
                                 delta_color='normal' if ok else 'inverse')
            if any(c.startswith('forward_return') for c in co_all.columns):
                fwd = co_all[co_all['period_type'] == 'annual'][['fiscal_year', 'forward_return_1y']].dropna()
                if not fwd.empty:
                    import matplotlib.pyplot as plt
                    st.markdown('**Forward Return History (1y)**')
                    fig, ax = plt.subplots(figsize=(10, 3))
                    colors  = ['#4CAF50' if v >= 0 else '#F44336' for v in fwd['forward_return_1y']]
                    ax.bar(fwd['fiscal_year'], fwd['forward_return_1y'] * 100, color=colors, edgecolor='white')
                    ax.axhline(0, color='gray', lw=0.8)
                    ax.set_ylabel('1y Return (%)')
                    ax.set_title(f'{selected_ticker} — Annual 1-Year Forward Return')
                    ax.grid(axis='y', alpha=0.3)
                    st.pyplot(fig, use_container_width=True)
                    plt.close()

    with st.expander('📊 Dataset Overview Charts', expanded=False):
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(1, 3, figsize=(16, 4))
        mkt_counts       = df_all.drop_duplicates('cik').groupby('market').size().sort_values(ascending=False)
        mkt_labels_short = [MARKET_LABELS.get(m, m).split(' ')[-1] for m in mkt_counts.index]
        axes[0].bar(mkt_labels_short, mkt_counts.values, color='#2196F3', edgecolor='white')
        axes[0].set_title('Companies per Market', fontweight='bold')
        axes[0].tick_params(axis='x', rotation=45)
        axes[0].set_ylabel('Companies')
        labeled_by_yr = df_all[df_all['forward_return_1y'].notna()].groupby('fiscal_year').size()
        axes[1].fill_between(labeled_by_yr.index, labeled_by_yr.values, alpha=0.7, color='#4CAF50')
        axes[1].set_title('Labeled Rows (1y) per Fiscal Year', fontweight='bold')
        axes[1].set_ylabel('Count')
        axes[1].grid(alpha=0.3)
        if 'composite_score' in latest.columns and latest['composite_score'].notna().any():
            axes[2].hist(latest['composite_score'].dropna(), bins=40, color='#FF9800', edgecolor='white', alpha=0.85)
            axes[2].axvline(min_composite, color='red', ls='--', lw=1.5, label=f'Threshold {min_composite:.2f}')
            axes[2].set_title('Composite Score Distribution', fontweight='bold')
            axes[2].set_xlabel('Composite Score')
            axes[2].legend()
        plt.tight_layout()
        st.pyplot(fig, use_container_width=True)
        plt.close()

    st.subheader('Export')
    csv = latest[display_cols].head(1000).to_csv(index=False)
    st.download_button('⬇️ Download top 1000 as CSV', csv, 'screener_results.csv', 'text/csv')


# ── Tab: Backtester ───────────────────────────────────────────────────────────

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
                [sys.executable, str(BASE / 'scripts' / 'backtester.py'),
                 '--strategy', 'all'],
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

    # ── Summary table ─────────────────────────────────────────────────────────
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

    # ── Annual return bar charts ──────────────────────────────────────────────
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

    # ── Wealth index ──────────────────────────────────────────────────────────
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
            bench  = np.array([r.get('bench_ret', 0) for r in ann])
            b_wealth = np.cumprod(1 + bench)
            ax2.plot(years, b_wealth, lw=1.5, ls='--', color='gray', label='Benchmark (equal-weight)')
            plotted_bench = True
    ax2.axhline(1, color='black', lw=0.5)
    ax2.set_ylabel('Portfolio value ($1 = start)')
    ax2.set_title('Walk-Forward Cumulative Return', fontweight='bold')
    ax2.legend()
    ax2.grid(alpha=0.3)
    plt.tight_layout()
    st.pyplot(fig2, use_container_width=True)
    plt.close()


# ── Tab: Watchlist ────────────────────────────────────────────────────────────

def tab_watchlist() -> None:
    st.header('⭐ Watchlist & Price Alerts')

    def _load_wl() -> dict:
        return json.loads(WL_PATH.read_text()) if WL_PATH.exists() else {}

    def _save_wl(wl: dict) -> None:
        WL_PATH.write_text(json.dumps(wl, indent=2))

    wl = _load_wl()

    # ── Add ticker ────────────────────────────────────────────────────────────
    with st.expander('➕ Add ticker', expanded=len(wl) == 0):
        a1, a2, a3, a4, a5 = st.columns([2, 2, 2, 3, 1])
        new_ticker = a1.text_input('Ticker', placeholder='AAPL').strip().upper()
        new_above  = a2.number_input('Alert above ($)', value=0.0, min_value=0.0, step=1.0)
        new_below  = a3.number_input('Alert below ($)', value=0.0, min_value=0.0, step=1.0)
        new_note   = a4.text_input('Note', placeholder='QEM pick, strategy…')
        if a5.button('Add', type='primary'):
            if new_ticker:
                wl[new_ticker] = {
                    'added': datetime.utcnow().strftime('%Y-%m-%d'),
                    'above': new_above if new_above > 0 else None,
                    'below': new_below if new_below > 0 else None,
                    'note':  new_note,
                }
                _save_wl(wl)
                st.success(f'Added {new_ticker}')
                st.rerun()
            else:
                st.warning('Enter a ticker symbol.')

    if not wl:
        st.info('Your watchlist is empty. Add tickers above.')
        return

    # ── Live prices ───────────────────────────────────────────────────────────
    col_refresh, col_ts = st.columns([1, 3])
    with col_refresh:
        fetch_prices = st.button('🔄 Fetch Live Prices')
    with col_ts:
        if WL_LIVE.exists():
            live_data = json.loads(WL_LIVE.read_text())
            st.caption(f'Prices as of {live_data.get("generated_at", "")}')

    prices: dict[str, float | None] = {}
    if fetch_prices or WL_LIVE.exists():
        if fetch_prices:
            with st.spinner('Fetching prices…'):
                res = subprocess.run(
                    [sys.executable, str(BASE / 'scripts' / 'watchlist.py'), 'export'],
                    capture_output=True, text=True, cwd=str(BASE)
                )
            if res.returncode != 0:
                st.error(f'Price fetch failed: {res.stderr[-500:]}')
        if WL_LIVE.exists():
            live_data = json.loads(WL_LIVE.read_text())
            prices    = {r['ticker']: r.get('price') for r in live_data.get('items', [])}
            alerts    = live_data.get('alerts', [])
            if fetch_prices and alerts:
                for a in alerts:
                    st.warning(f"🚨 **{a['ticker']}** ${a['price']:,.2f} → {a['alert']}")

    # ── Watchlist table ───────────────────────────────────────────────────────
    st.subheader(f'Watching {len(wl)} ticker(s)')
    to_remove = []
    rows_data = []
    for tk, meta in sorted(wl.items()):
        price = prices.get(tk)
        above = meta.get('above')
        below = meta.get('below')
        alert_str = ''
        if price:
            if above and price >= above:
                alert_str = f'🚨 ABOVE ${above:,.2f}'
            elif below and price <= below:
                alert_str = f'🚨 BELOW ${below:,.2f}'
        rows_data.append({
            'Ticker':  tk,
            'Price':   f'${price:,.2f}' if price else '—',
            'Above':   f'${above:,.2f}' if above else '—',
            'Below':   f'${below:,.2f}' if below else '—',
            'Alert':   alert_str,
            'Added':   meta.get('added', ''),
            'Note':    meta.get('note', ''),
        })

    df_wl = pd.DataFrame(rows_data)
    st.dataframe(df_wl, use_container_width=True, hide_index=True)

    # ── Remove tickers ────────────────────────────────────────────────────────
    with st.expander('🗑 Remove tickers'):
        to_del = st.multiselect('Select tickers to remove', options=sorted(wl.keys()))
        if st.button('Remove selected', type='secondary') and to_del:
            for tk in to_del:
                wl.pop(tk, None)
            _save_wl(wl)
            st.success(f'Removed: {", ".join(to_del)}')
            st.rerun()

    # ── Download ──────────────────────────────────────────────────────────────
    st.download_button('⬇️ Download watchlist CSV',
                       df_wl.to_csv(index=False),
                       'watchlist.csv', 'text/csv')


# ── Tab: Strategies ───────────────────────────────────────────────────────────

def tab_strategies() -> None:
    st.header('🎯 High-ROI Strategy Picks')
    st.caption('Generated by `scripts/high_roi_strategies.py`. Re-run to refresh picks.')

    col_run, col_cap = st.columns([1, 2])
    with col_run:
        run_strats = st.button('▶ Run / Refresh Strategies', type='primary')
    with col_cap:
        capital = st.number_input('Portfolio capital (€)', value=10000, min_value=1000, step=1000)

    if run_strats:
        with st.spinner('Running strategy screener (30–60s)…'):
            res = subprocess.run(
                [sys.executable, str(BASE / 'scripts' / 'high_roi_strategies.py'),
                 '--strategy', 'all', '--capital', str(capital)],
                capture_output=True, text=True, cwd=str(BASE)
            )
        if res.returncode == 0:
            st.success('Strategies refreshed!')
        else:
            st.error(f'Strategy script failed:\n{res.stderr[-1000:]}')
            return

    any_found = False
    for label, path in STRAT_FILES.items():
        if not path.exists():
            continue
        any_found = True
        st.subheader(label)
        df_s = pd.read_csv(path)

        # Identify score column (ends with _score)
        score_col = next((c for c in df_s.columns if c.endswith('_score')), None)

        # KPI row
        n = len(df_s)
        k1, k2, k3, k4 = st.columns(4)
        k1.metric('Picks', n)
        if score_col and df_s[score_col].notna().any():
            k2.metric('Top score', f'{df_s[score_col].max():.3f}')
        if 'market_cap_at_filing' in df_s.columns:
            med_cap = df_s['market_cap_at_filing'].median()
            k3.metric('Median market cap',
                      f'${med_cap/1e9:.1f}B' if pd.notna(med_cap) and med_cap >= 1e9
                      else (f'${med_cap/1e6:.0f}M' if pd.notna(med_cap) else 'N/A'))
        if capital and n > 0:
            k4.metric('Position size (equal-weight)', f'€{capital/n:,.0f}')

        # Table
        st.dataframe(df_s, use_container_width=True, height=300)

        # Score distribution
        if score_col and df_s[score_col].notna().any():
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(8, 2.5))
            ax.barh(range(min(n, 20)), df_s[score_col].head(20), color='#2196F3', edgecolor='white')
            if 'ticker' in df_s.columns:
                ax.set_yticks(range(min(n, 20)))
                ax.set_yticklabels(df_s['ticker'].head(20), fontsize=8)
            ax.set_xlabel('Score')
            ax.set_title(f'Top {min(n, 20)} picks by score')
            ax.grid(axis='x', alpha=0.3)
            plt.tight_layout()
            st.pyplot(fig, use_container_width=True)
            plt.close()

        st.download_button(f'⬇️ Download {label.split("—")[0].strip()} CSV',
                           df_s.to_csv(index=False),
                           path.name, 'text/csv',
                           key=f'dl_{path.stem}')
        st.divider()

    if not any_found:
        st.info('No strategy CSVs found. Click **Run / Refresh Strategies** above.')


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title='Stock Fraud & Value Screener',
        page_icon='🔍',
        layout='wide',
        initial_sidebar_state='expanded',
    )

    with st.spinner('Loading dataset…'):
        df_all = load_data()
    models, meta = load_models()

    tab1, tab2, tab3, tab4 = st.tabs([
        '📊 Screener',
        '📈 Backtester',
        '⭐ Watchlist',
        '🎯 Strategies',
    ])

    with tab1:
        tab_screener(df_all, models, meta)
    with tab2:
        tab_backtester()
    with tab3:
        tab_watchlist()
    with tab4:
        tab_strategies()


if __name__ == '__main__':
    main()
