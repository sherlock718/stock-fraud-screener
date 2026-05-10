"""
app_v2.py — Multi-market stock fraud & value screener
Tabbed layout: Screener | Backtester | Watchlist | Strategies
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
BASE       = Path(__file__).parent
DATA_PATH  = BASE / 'data' / 'historical_dataset_clean.parquet'
META_PATH  = BASE / 'models' / 'model_meta.json'
MODELS_DIR = BASE / 'models'

# HuggingFace Hub — set HF_REPO env var to enable cloud data loading.
# Example: HF_REPO=your-username/stock-screener-data
HF_REPO = os.environ.get('HF_REPO', '')

# True when running on Streamlit Community Cloud (no local filesystem writes,
# no subprocess-based pipeline execution).
_IS_CLOUD = bool(HF_REPO) or not DATA_PATH.parent.exists()
BT_PATH      = BASE / 'data' / 'backtest_results.json'
WL_PATH      = BASE / 'data' / 'watchlist.json'
WL_LIVE      = BASE / 'data' / 'watchlist_live.json'
REFRESH_PATH = BASE / 'data' / 'refresh_status.json'

STRAT_FILES = {
    'QEM — Quality + Earnings Momentum':  BASE / 'data' / 'strategy_qem.csv',
    'SCDV — Small-Cap Deep Value':         BASE / 'data' / 'strategy_scdv.csv',
    'IARB — International Arbitrage':      BASE / 'data' / 'strategy_iarb.csv',
}
SECTOR_PATH = BASE / 'data' / 'sector_dividend_map.parquet'

MARKET_LABELS = {
    'US': '🇺🇸 United States', 'CA': '🇨🇦 Canada', 'BR': '🇧🇷 Brazil',
    'JP': '🇯🇵 Japan', 'DE': '🇩🇪 Germany', 'FR': '🇫🇷 France',
    'IT': '🇮🇹 Italy', 'ES': '🇪🇸 Spain', 'SE': '🇸🇪 Sweden',
    'FI': '🇫🇮 Finland', 'NL': '🇳🇱 Netherlands', 'PT': '🇵🇹 Portugal',
    'DK': '🇩🇰 Denmark',
}

# ── Data loading ──────────────────────────────────────────────────────────────

def _hf_download_bytes(repo_id: str, filename: str) -> bytes | None:
    """Download a file from HuggingFace Hub as raw bytes. Returns None on failure."""
    try:
        from huggingface_hub import hf_hub_download
        local = hf_hub_download(repo_id=repo_id, filename=filename, repo_type='dataset')
        return Path(local).read_bytes()
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    if HF_REPO and not DATA_PATH.exists():
        raw = _hf_download_bytes(HF_REPO, 'historical_dataset_clean.parquet')
        if raw is not None:
            df = pd.read_parquet(io.BytesIO(raw))
        else:
            st.error('Could not load dataset from HuggingFace Hub.')
            return pd.DataFrame()
    else:
        df = pd.read_parquet(DATA_PATH)

    df = df[df['period_type'] == 'annual'].copy()
    if SECTOR_PATH.exists():
        sec = pd.read_parquet(SECTOR_PATH)
        df = df.merge(sec[['ticker', 'sector', 'industry',
                            'dividendYield', 'dividendRate', 'payoutRatio',
                            'trailingAnnualDividendYield', 'trailingAnnualDividendRate',
                            'exDividendDate']],
                      on='ticker', how='left')
    return df


@st.cache_resource(show_spinner=False)
def load_models() -> tuple[dict, dict]:
    import joblib

    meta: dict = {}
    models: dict = {}

    if HF_REPO and not META_PATH.exists():
        raw = _hf_download_bytes(HF_REPO, 'models/model_meta.json')
        if raw is not None:
            meta = json.loads(raw.decode())
    elif META_PATH.exists():
        meta = json.loads(META_PATH.read_text())

    for h in ['1y', '3y', '5y']:
        p = MODELS_DIR / f'model_{h}.joblib'
        if p.exists():
            try:
                models[h] = joblib.load(p)
            except Exception:
                pass
        elif HF_REPO:
            raw = _hf_download_bytes(HF_REPO, f'models/model_{h}.joblib')
            if raw is not None:
                try:
                    models[h] = joblib.load(io.BytesIO(raw))
                except Exception:
                    pass

    return models, meta


def score_companies(df: pd.DataFrame, models: dict, meta: dict,
                    horizon: str = '1y') -> pd.DataFrame:
    if horizon not in models or horizon not in meta:
        df['ml_score'] = np.nan
        return df
    clf   = models[horizon]
    feats = [f for f in meta[horizon]['features'] if f in df.columns]
    train_medians = meta[horizon].get('train_medians', {})
    fill_vals = {f: train_medians.get(f, 0.0) for f in feats}
    X = df[feats].fillna(pd.Series(fill_vals))
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


# ── Sidebar: Live Data Refresh ───────────────────────────────────────────────

def _sidebar_refresh() -> None:
    st.markdown('### 🔄 Data Refresh')

    # Load status
    status: dict = {}
    if REFRESH_PATH.exists():
        try:
            status = json.loads(REFRESH_PATH.read_text())
        except Exception:
            pass

    # Data age badge
    last_refresh = status.get('last_refresh')
    if last_refresh:
        try:
            lr_dt = datetime.fromisoformat(last_refresh.replace('Z', '+00:00'))
            age_days = (datetime.now(lr_dt.tzinfo) - lr_dt).days
            if age_days < 7:
                st.success(f'Data age: {age_days}d  ✓ Fresh')
            elif age_days < 30:
                st.warning(f'Data age: {age_days}d  ⚠ Stale')
            else:
                st.error(f'Data age: {age_days}d  ✗ Very old')
        except Exception:
            st.info('Last refresh: unknown')
    else:
        st.info('No refresh run yet')

    # Last refresh stats
    if status and not status.get('in_progress'):
        ds = status.get('dataset', {})
        col_a, col_b = st.columns(2)
        col_a.metric('Companies', f"{ds.get('companies', '?'):,}" if isinstance(ds.get('companies'), int) else '?')
        col_b.metric('Max FY', ds.get('fiscal_year_max', '?'))
        st.caption(
            f"Mode: {status.get('last_mode_label', '?')}  |  "
            f"Duration: {status.get('last_elapsed_sec', '?')}s  |  "
            f"Size: {ds.get('file_size_mb', '?')} MB"
        )
        if status.get('last_error'):
            st.error(f"Last error: {status['last_error']}")

    if status.get('in_progress'):
        st.warning(f"⏳ Refresh in progress (mode: {status.get('mode', '?')})")
        st.caption(f"Started: {status.get('started_at', '?')}")
        return

    if _IS_CLOUD:
        st.info('Running in cloud mode — pipeline refresh is disabled. '
                'Push new data via scripts/push_to_hf.py from your local machine.')
        return

    st.markdown('---')

    REFRESH_SCRIPT = BASE / 'scripts' / 'refresh_data.py'

    def _run_refresh(mode: str) -> None:
        cmd = [sys.executable, str(REFRESH_SCRIPT), 'refresh', '--mode', mode]
        status_patch = json.loads(REFRESH_PATH.read_text()) if REFRESH_PATH.exists() else {}
        status_patch['in_progress'] = True
        status_patch['mode'] = mode
        REFRESH_PATH.write_text(json.dumps(status_patch, indent=2))

        placeholder = st.empty()
        with st.expander('Pipeline output', expanded=True):
            log_area = st.empty()
            log_lines: list[str] = []
            proc = subprocess.Popen(
                cmd, cwd=str(BASE),
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )
            for line in proc.stdout:  # type: ignore[union-attr]
                log_lines.append(line.rstrip())
                log_area.code('\n'.join(log_lines[-40:]))
            proc.wait()

        if proc.returncode == 0:
            placeholder.success('✓ Refresh complete — reloading…')
        else:
            placeholder.error(f'✗ Refresh failed (exit {proc.returncode})')

        st.cache_data.clear()
        st.rerun()

    col1, col2, col3 = st.columns(3)

    if col1.button('⚡ Quick\n~5 min', use_container_width=True, help='Re-compute features from existing data (no API calls)'):
        _run_refresh('quick')

    if col2.button('📈 Prices\n~45 min', use_container_width=True, help='Re-pull yfinance prices + features'):
        if st.session_state.get('_prices_confirm'):
            _run_refresh('prices')
            st.session_state['_prices_confirm'] = False
        else:
            st.session_state['_prices_confirm'] = True
            st.warning('Click again to confirm ~45 min price refresh')

    if col3.button('🔄 Full\n~4 h', use_container_width=True, help='Full rebuild from SEC EDGAR (hours)'):
        if st.session_state.get('_full_confirm'):
            _run_refresh('full')
            st.session_state['_full_confirm'] = False
        else:
            st.session_state['_full_confirm'] = True
            st.warning('Click again to confirm full ~4 h rebuild')


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

        if 'sector' in df_all.columns:
            st.subheader('GICS Sector')
            avail_sectors = sorted(df_all['sector'].dropna().unique().tolist())
            selected_sectors = st.multiselect('Filter by sector', avail_sectors,
                                              placeholder='All sectors (leave blank)')
        else:
            selected_sectors = []

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
    if selected_sectors and 'sector' in df.columns:
        df = df[df['sector'].isin(selected_sectors)]

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
                    ([c for c in ['sector'] if c in latest.columns]) +
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
            show_df[col] = show_df[col].map(lambda x: int(round(x * 100)) if pd.notna(x) else '')
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
        co_ann    = co_all[co_all['period_type'] == 'annual']
        co_latest = co_ann.tail(1)
        if co_latest.empty:
            st.warning('No annual data found for this ticker.')
        else:
            row = co_latest.iloc[0]
            st.markdown(f"### {row.get('name', 'N/A')} ({selected_ticker})")

            # ── Identity row ──────────────────────────────────────────────────
            d1, d2, d3, d4, d5, d6 = st.columns(6)
            d1.metric('Market', MARKET_LABELS.get(str(row.get('market', '')), str(row.get('market', ''))))
            d2.metric('Fiscal Year', int(row['fiscal_year']) if pd.notna(row.get('fiscal_year')) else 'N/A')
            d3.metric('Exchange', str(row.get('exchange', 'N/A')))
            if pd.notna(row.get('market_cap_at_filing')):
                cap = row['market_cap_at_filing']
                d4.metric('Market Cap', f'${cap/1e9:.2f}B' if cap >= 1e9 else f'${cap/1e6:.0f}M')
            if pd.notna(row.get('sector')):
                d5.metric('Sector', str(row['sector']))
            if pd.notna(row.get('industry')):
                d6.metric('Industry', str(row['industry']))

            # ── Dividend row ──────────────────────────────────────────────────
            div_yield = row.get('dividendYield') or row.get('trailingAnnualDividendYield')
            div_rate  = row.get('dividendRate') or row.get('trailingAnnualDividendRate')
            payout    = row.get('payoutRatio')
            if any(pd.notna(x) for x in [div_yield, div_rate, payout]):
                st.markdown('**Dividends**')
                e1, e2, e3 = st.columns(3)
                e1.metric('Dividend Yield',
                          f'{div_yield*100:.2f}%' if pd.notna(div_yield) else 'N/A')
                e2.metric('Annual Rate',
                          f'${div_rate:.2f}' if pd.notna(div_rate) else 'N/A')
                e3.metric('Payout Ratio',
                          f'{payout*100:.1f}%' if pd.notna(payout) else 'N/A')

            # ── Risk Signals ──────────────────────────────────────────────────
            st.markdown('**Risk Signals**')
            rc1, rc2, rc3, rc4 = st.columns(4)
            risk_checks = [
                ('Beneish M-Score', 'beneish_m_score', -2.22, 'below',
                 'Below -2.22 = low manipulation risk'),
                ('Altman Z-Score',  'altman_z_score',   1.81, 'above',
                 'Above 1.81 = financially stable'),
                ('Piotroski F',     'piotroski_f_score', 6.0, 'above',
                 '7–9 = high quality'),
            ]
            for c_obj, (label, col, thresh, direction, _help) in zip([rc1, rc2, rc3], risk_checks):
                if col in row.index and pd.notna(row[col]):
                    val = row[col]
                    ok  = val <= thresh if direction == 'below' else val >= thresh
                    c_obj.metric(label, f'{val:.2f}',
                                 delta='✅ OK' if ok else '⚠️ Risk',
                                 delta_color='normal' if ok else 'inverse',
                                 help=_help)
            if 'composite_score' in row.index and pd.notna(row.get('composite_score')):
                rc4.metric('Composite Score',
                           f'{int(round(row["composite_score"] * 100))}/100',
                           help='Percentile rank across value, quality, momentum, fraud safety, ML')

            # ── Live price chart (yfinance) ───────────────────────────────────
            with st.expander('📈 Price Chart (last 2 years)', expanded=True):
                try:
                    import yfinance as yf
                    hist = yf.Ticker(selected_ticker).history(period='2y', auto_adjust=True)
                    if not hist.empty:
                        fig_price = go.Figure()
                        fig_price.add_trace(go.Candlestick(
                            x=hist.index,
                            open=hist['Open'], high=hist['High'],
                            low=hist['Low'],   close=hist['Close'],
                            name=selected_ticker,
                            increasing_line_color='#4CAF50',
                            decreasing_line_color='#F44336',
                        ))
                        fig_price.update_layout(
                            title=f'{selected_ticker} — 2-Year Price History',
                            xaxis_title='Date', yaxis_title='Price (USD)',
                            height=360, margin=dict(l=0, r=0, t=40, b=0),
                            xaxis_rangeslider_visible=False,
                            template='plotly_dark',
                        )
                        st.plotly_chart(fig_price, use_container_width=True)
                    else:
                        st.info('No price data available for this ticker.')
                except Exception as e:
                    st.warning(f'Could not load price chart: {e}')

            # ── Fundamental trends ────────────────────────────────────────────
            fin_cols = [c for c in ['revenue', 'gross_profit', 'operating_income',
                                    'net_income', 'operating_cash_flow'] if c in co_ann.columns]
            if fin_cols and len(co_ann) > 1:
                with st.expander('📊 Fundamental Trends', expanded=True):
                    tab_f1, tab_f2, tab_f3 = st.tabs(['Income', 'Balance Sheet', 'Margins'])

                    with tab_f1:
                        income_cols = [c for c in ['revenue', 'gross_profit',
                                                    'operating_income', 'net_income'] if c in co_ann.columns]
                        if income_cols:
                            fig_inc = go.Figure()
                            for col in income_cols:
                                vals = co_ann[col].fillna(0) / 1e6
                                fig_inc.add_trace(go.Bar(
                                    x=co_ann['fiscal_year'], y=vals,
                                    name=col.replace('_', ' ').title(),
                                ))
                            fig_inc.update_layout(
                                barmode='group', height=320,
                                yaxis_title='USD Million',
                                legend=dict(orientation='h', y=-0.25),
                                margin=dict(l=0, r=0, t=10, b=0),
                                template='plotly_dark',
                            )
                            st.plotly_chart(fig_inc, use_container_width=True)

                    with tab_f2:
                        bs_cols = [c for c in ['total_assets', 'equity',
                                                'total_debt'] if c in co_ann.columns]
                        if bs_cols:
                            fig_bs = go.Figure()
                            for col in bs_cols:
                                vals = co_ann[col].fillna(0) / 1e6
                                fig_bs.add_trace(go.Scatter(
                                    x=co_ann['fiscal_year'], y=vals,
                                    name=col.replace('_', ' ').title(),
                                    mode='lines+markers',
                                ))
                            fig_bs.update_layout(
                                height=320, yaxis_title='USD Million',
                                legend=dict(orientation='h', y=-0.25),
                                margin=dict(l=0, r=0, t=10, b=0),
                                template='plotly_dark',
                            )
                            st.plotly_chart(fig_bs, use_container_width=True)

                    with tab_f3:
                        margin_cols = [c for c in ['gross_margin', 'operating_margin',
                                                    'net_margin', 'roe', 'roa'] if c in co_ann.columns]
                        if margin_cols:
                            fig_mar = go.Figure()
                            for col in margin_cols:
                                fig_mar.add_trace(go.Scatter(
                                    x=co_ann['fiscal_year'],
                                    y=co_ann[col] * 100,
                                    name=col.replace('_', ' ').title(),
                                    mode='lines+markers',
                                ))
                            fig_mar.add_hline(y=0, line_dash='dash', line_color='gray')
                            fig_mar.update_layout(
                                height=320, yaxis_title='%',
                                legend=dict(orientation='h', y=-0.25),
                                margin=dict(l=0, r=0, t=10, b=0),
                                template='plotly_dark',
                            )
                            st.plotly_chart(fig_mar, use_container_width=True)

            # ── Forward return history ────────────────────────────────────────
            if 'forward_return_1y' in co_ann.columns:
                fwd = co_ann[['fiscal_year', 'forward_return_1y']].dropna()
                if not fwd.empty:
                    with st.expander('📅 Forward Return History (1y)', expanded=False):
                        colors = ['#4CAF50' if v >= 0 else '#F44336'
                                  for v in fwd['forward_return_1y']]
                        fig_fwd = go.Figure(go.Bar(
                            x=fwd['fiscal_year'],
                            y=fwd['forward_return_1y'] * 100,
                            marker_color=colors,
                        ))
                        fig_fwd.add_hline(y=0, line_color='gray', line_width=1)
                        fig_fwd.update_layout(
                            yaxis_title='1-Year Return (%)', height=280,
                            margin=dict(l=0, r=0, t=10, b=0),
                            template='plotly_dark',
                        )
                        st.plotly_chart(fig_fwd, use_container_width=True)

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


# ── User Guide ────────────────────────────────────────────────────────────────

def tab_guide() -> None:
    st.title('📖 User Guide')
    st.markdown(
        'This guide explains every score, metric, and filter in the screener, '
        'and walks through how to use the tool for systematic stock research.'
    )

    with st.expander('🔍 What is this tool?', expanded=True):
        st.markdown("""
This is a **multi-market stock fraud and value screener** covering US, Canadian, European,
Japanese, and Brazilian listed companies. It combines:

- **Fundamental accounting data** from SEC EDGAR (annual filings)
- **Fraud-risk signals** using academic models (Beneish, Altman, Piotroski)
- **Machine learning scores** trained on historical forward returns
- **A composite ranking** that blends value, quality, momentum, safety, and ML signals

The goal is to surface **high-quality, undervalued, low-fraud-risk** companies across global markets
— useful for long-term fundamental investors, not day traders.
        """)

    with st.expander('📊 Composite Score (0–100)', expanded=True):
        st.markdown("""
The **Composite Score** is the main ranking number. It runs from **0 to 100**, where **100 is best**.

It is a weighted percentile rank across five components:

| Component | Weight | Higher is better when… |
|---|---|---|
| **Value** | 25% | P/E and P/B are low (cheap stock) |
| **Quality** | 20% | Piotroski F-Score is high (strong fundamentals) |
| **Momentum** | 20% | 12-month price return is high |
| **Fraud Safety** | 20% | Beneish M-Score is low (less earnings manipulation risk) |
| **ML Alpha** | 15% | ML model predicts strong 1-year forward return |

If a component is missing for a ticker, the remaining weights are rescaled to sum to 100%.

**How to use it:** Sort descending and look at scores above 70 as the starting pool.
Do not use the score alone — read the individual signals to understand *why* a company ranks high.
        """)

    with st.expander('🚨 Beneish M-Score (Earnings Manipulation Risk)'):
        st.markdown("""
The **Beneish M-Score** is an accounting model that estimates the probability of earnings
manipulation. It was developed by Professor Messod Beneish in 1999.

**Interpretation:**
- **M-Score > −1.78** → Possible manipulator — treat with caution
- **M-Score < −2.22** → Unlikely manipulator — lower risk
- Values in between are a grey zone

The score is built from eight financial ratios measuring changes in receivables, gross margins,
asset quality, sales growth, depreciation, leverage, and accruals.

**What it does NOT catch:** Fraud that doesn't show up in GAAP accounting (e.g. off-balance-sheet
vehicles, crypto asset manipulation). It is a signal, not a verdict.
        """)

    with st.expander('🏦 Altman Z-Score (Bankruptcy Risk)'):
        st.markdown("""
The **Altman Z-Score** predicts the probability of a company going bankrupt within two years.
It was developed by Edward Altman in 1968 and is still widely used.

**Interpretation (original manufacturing model):**
- **Z > 2.99** → Safe zone
- **1.81 < Z < 2.99** → Grey zone
- **Z < 1.81** → Distress zone — significant bankruptcy risk

Note: the thresholds differ for non-manufacturing and emerging-market companies. The screener
uses the original model for all companies — treat the thresholds as directional, not absolute.

**What to do with a low Z-Score:** Dig into leverage ratios, interest coverage, and cash runway.
A low Z-Score in a capital-intensive industry (utilities, real estate) is normal; in tech it is a warning.
        """)

    with st.expander('✅ Piotroski F-Score (Fundamental Quality)'):
        st.markdown("""
The **Piotroski F-Score** (0–9) is a checklist of nine binary signals across three categories:

| Category | Signals |
|---|---|
| **Profitability** (4 pts) | ROA > 0, Operating cash flow > 0, ROA improving, Cash flow > net income (accruals) |
| **Leverage & Liquidity** (3 pts) | Debt ratio falling, Current ratio rising, No dilution |
| **Operating Efficiency** (2 pts) | Gross margin improving, Asset turnover improving |

**Interpretation:**
- **8–9** → Strong fundamentals
- **5–7** → Average
- **0–2** → Weak — potential value trap or distress

**How to use it:** Pair with a low P/B ratio. Piotroski's original paper showed buying high-F-Score,
low P/B stocks outperformed the market by ~7.5% annually (1976–1996).
        """)

    with st.expander('🤖 ML Score (Machine Learning Alpha)'):
        st.markdown("""
The **ML Score** is the probability output of a LightGBM gradient-boosting model trained to
predict whether a company will be in the **top quartile of 1-year forward returns** given its
current fundamentals.

- Range: **0.0 to 1.0** (higher = model thinks this company will outperform)
- The model uses ~35 engineered features covering profitability, leverage, growth, valuation,
  and accounting quality — selected by IC/ICIR analysis to keep only stable, non-redundant predictors
- It is trained on historical annual filings with labels derived from actual price returns

**Caveats:**
- The model is trained on past data — regime changes may reduce accuracy
- It is better used as a ranking signal than as an absolute probability
- Always cross-check with Beneish and Piotroski before acting on a high ML score
        """)

    with st.expander('💰 Dividend Metrics'):
        st.markdown("""
Dividend data is fetched from Yahoo Finance and linked to each ticker.

| Field | Meaning |
|---|---|
| **Dividend Yield %** | Annual dividend / current price |
| **Annual Rate $** | Total dividends paid per share per year |
| **Payout Ratio %** | Dividends as % of earnings. >100% may be unsustainable |
| **Ex-Dividend Date** | You must own the stock before this date to receive the next dividend |

These are live data points and may differ slightly from the annual filing data used in scoring.
        """)

    with st.expander('🏭 GICS Sector Filter'):
        st.markdown("""
Companies are classified using the **Global Industry Classification Standard (GICS)**,
maintained by MSCI and S&P Global. The screener fetches this from Yahoo Finance.

The 11 GICS sectors are:
`Communication Services`, `Consumer Discretionary`, `Consumer Staples`, `Energy`,
`Financials`, `Health Care`, `Industrials`, `Information Technology`,
`Materials`, `Real Estate`, `Utilities`

Use the sector filter in the sidebar to focus on industries you understand well, or to
avoid sectors where the accounting models behave differently (e.g. Financials and Real Estate
have different leverage norms — Altman Z-Score thresholds don't apply directly).
        """)

    with st.expander('📈 Strategies Tab'):
        st.markdown("""
The **Strategies tab** shows pre-built stock screens derived from academic factor literature:

| Strategy | Logic |
|---|---|
| **QEM — Quality + Earnings Momentum** | High Piotroski F-Score + positive earnings revisions |
| **SCDV — Small-Cap Deep Value** | Low P/B + low P/E + small market cap |
| **IARB — International Arbitrage** | Undervalued in home market vs US-listed ADR peers |

Strategies are generated by running `python3 run_pipeline.py features` and saved as CSVs.
You can download each strategy list directly from the Strategies tab.
        """)

    with st.expander('⭐ Watchlist & Alerts'):
        st.markdown("""
The **Watchlist tab** lets you save tickers for ongoing monitoring.

- Add a ticker using the input box — it will be saved to `data/watchlist.json`
- Live prices are fetched from Yahoo Finance when you view the watchlist
- Alerts (price thresholds, score changes) are planned for a future release

**Tip:** Add tickers you've researched and want to track over multiple quarters.
Compare the Composite Score across annual reporting periods to see if fundamentals are improving.
        """)

    with st.expander('🔄 Data Refresh'):
        st.markdown("""
The sidebar **Data Refresh** panel lets you update the underlying dataset:

| Mode | What it does | Estimated time |
|---|---|---|
| **Quick** | Re-computes features from existing data (no API calls) | ~5 min |
| **Prices** | Re-pulls Yahoo Finance prices + recomputes features | ~30–60 min |
| **Full** | Full rebuild from SEC EDGAR (all filings) + prices | Several hours |

Run **Quick** after changing feature engineering code.
Run **Prices** monthly to update forward return calculations.
Run **Full** annually or after a major EDGAR data update.
        """)

    with st.expander('⚠️ Important Disclaimers'):
        st.markdown("""
**This tool is for research and educational purposes only. It is not financial advice.**

- Scores and rankings are based on historical accounting data, which may be restated or delayed
- The ML model is trained on past relationships that may not hold in the future
- Beneish and Altman scores are probabilistic signals, not auditor opinions
- No score replaces thorough fundamental research, reading actual filings, and understanding a business
- Past outperformance of any factor does not guarantee future results
- Always consult a qualified financial professional before making investment decisions

**Data sources:** SEC EDGAR (financial statements), Yahoo Finance (prices, sectors, dividends),
FRED (macroeconomic context)
        """)


# ── Company Profile ───────────────────────────────────────────────────────────

_FEATURE_GROUPS: dict[str, list[str]] = {
    '🚨 Fraud Signals': [
        'beneish_m_score', 'altman_z_score', 'piotroski_f_score',
        'sloan_accruals', 'accruals_to_assets', 'wc_accruals_to_assets',
        'beneish_m_score_sector_pct', 'altman_z_score_sector_pct', 'sloan_accruals_sector_pct',
    ],
    '📊 Beneish Components': [
        'beneish_dsri', 'beneish_gmi', 'beneish_aqi', 'beneish_sgi',
        'beneish_depi', 'beneish_sgai', 'beneish_lvgi', 'beneish_tata',
    ],
    '💉 Dilution & Shares': [
        'shares_dilution', 'shares_growth', 'eps_diluted', 'shares_outstanding',
        'common_shares_outstanding', 'dividends_per_share',
    ],
    '💰 Valuation': [
        'pe_ratio', 'pb_ratio', 'ps_ratio', 'ev_ebitda', 'ev_revenue', 'ev_ocf',
        'earnings_yield', 'fcf_yield', 'value_composite', 'pe_ratio_sector_pct',
    ],
    '⚙️ Quality': [
        'roa', 'roe', 'roic', 'gross_margin', 'operating_margin', 'net_margin',
        'current_ratio', 'net_debt_to_equity', 'quality_composite', 'accruals_avg_3y',
    ],
    '📈 Momentum & Growth': [
        'revenue_growth_yoy', 'eps_growth_yoy', 'ocf_growth_yoy', 'debt_growth_yoy',
        'roa_trend_3y', 'gross_margin_trend_3y', 'revenue_cagr_3y',
        'momentum_12m_prior', 'momentum_6m_prior', 'momentum_3m_prior',
    ],
    '🏗️ Balance Sheet': [
        'total_assets', 'total_debt', 'long_term_debt', 'short_term_debt',
        'cash', 'equity', 'retained_earnings', 'receivables', 'inventory',
        'current_assets', 'current_liabilities', 'operating_cash_flow',
    ],
}

_BENEISH_LABELS = {
    'beneish_dsri':  'DSRI (Receivables)',
    'beneish_gmi':   'GMI (Gross Margin)',
    'beneish_aqi':   'AQI (Asset Quality)',
    'beneish_sgi':   'SGI (Sales Growth)',
    'beneish_depi':  'DEPI (Depreciation)',
    'beneish_sgai':  'SGAI (SGA)',
    'beneish_lvgi':  'LVGI (Leverage)',
    'beneish_tata':  'TATA (Accruals)',
}


def _beneish_radar(row: pd.Series) -> 'go.Figure':
    components = list(_BENEISH_LABELS.keys())
    vals = [row.get(c, np.nan) for c in components]
    labels = list(_BENEISH_LABELS.values())
    if all(np.isnan(v) for v in vals):
        return None
    # Normalise to [0, 2] range for display
    vals_clamped = [max(0.0, min(float(v) if not np.isnan(v) else 1.0, 2.0)) for v in vals]
    vals_clamped.append(vals_clamped[0])
    labels.append(labels[0])
    fig = go.Figure(go.Scatterpolar(
        r=vals_clamped, theta=labels, fill='toself',
        fillcolor='rgba(239, 83, 80, 0.25)',
        line=dict(color='#EF5350', width=2),
        name='Beneish Components',
    ))
    fig.add_trace(go.Scatterpolar(
        r=[1.0] * len(labels), theta=labels,
        line=dict(color='grey', width=1, dash='dot'),
        name='Baseline (1.0)', showlegend=False,
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 2.2])),
        showlegend=False,
        margin=dict(t=30, b=20, l=40, r=40),
        height=320,
    )
    return fig


def tab_company_profile(df_all: pd.DataFrame, models: dict, meta: dict) -> None:
    st.title('🏢 Company Deep Dive')

    tickers = sorted(df_all['ticker'].unique().tolist())
    col_sel, col_year = st.columns([3, 1])
    with col_sel:
        selected_ticker = st.selectbox('Select ticker', tickers, key='profile_ticker')
    company_df = df_all[df_all['ticker'] == selected_ticker].sort_values('fiscal_year')
    if company_df.empty:
        st.warning('No data found for this ticker.')
        return

    avail_years = sorted(company_df['fiscal_year'].unique().tolist(), reverse=True)
    with col_year:
        selected_year = st.selectbox('Fiscal year', avail_years, index=0, key='profile_year')

    row = company_df[company_df['fiscal_year'] == selected_year].iloc[0]
    name_label = str(row.get('name', selected_ticker) or selected_ticker)
    mkt_label  = str(row.get('market', ''))

    st.subheader(f'{name_label}  ·  {selected_ticker}  ·  {mkt_label}  ·  FY{int(selected_year)}')

    # ── Hero metrics ────────────────────────────────────────────────────────
    def _fmt(v, decimals=2, suffix=''):
        return f'{v:.{decimals}f}{suffix}' if pd.notna(v) else '—'

    cols = st.columns(6)
    bm  = row.get('beneish_m_score')
    az  = row.get('altman_z_score')
    pf  = row.get('piotroski_f_score')
    cs  = row.get('composite_score')
    sl  = row.get('sloan_accruals')
    lmc = row.get('log_market_cap')

    cols[0].metric('Beneish M',
                   _fmt(bm),
                   delta='⚠️ manipulator' if pd.notna(bm) and bm > -2.22 else '✅ safe',
                   delta_color='inverse' if pd.notna(bm) and bm > -2.22 else 'off')
    cols[1].metric('Altman Z',
                   _fmt(az),
                   delta='distress' if pd.notna(az) and az < 1.81 else
                         'grey' if pd.notna(az) and az < 2.99 else 'safe')
    cols[2].metric('Piotroski F', f'{int(pf)}/9' if pd.notna(pf) else '—')
    cols[3].metric('Composite Score', _fmt(cs))
    cols[4].metric('Sloan Accruals', _fmt(sl, 3))
    cols[5].metric('Log Market Cap', _fmt(lmc, 1))

    # ── Score trend ─────────────────────────────────────────────────────────
    st.markdown('---')
    trend_cols = [c for c in ['beneish_m_score', 'altman_z_score', 'composite_score',
                               'piotroski_f_score', 'sloan_accruals'] if c in company_df.columns]
    if trend_cols:
        st.subheader('Score History')
        selected_trends = st.multiselect(
            'Metrics to plot', trend_cols,
            default=[c for c in ['beneish_m_score', 'altman_z_score'] if c in trend_cols],
            key='profile_trends',
        )
        if selected_trends:
            fig_trend = go.Figure()
            colors = ['#EF5350', '#42A5F5', '#66BB6A', '#FFA726', '#AB47BC']
            for i, sc in enumerate(selected_trends):
                data_sc = company_df[['fiscal_year', sc]].dropna()
                fig_trend.add_trace(go.Scatter(
                    x=data_sc['fiscal_year'], y=data_sc[sc],
                    mode='lines+markers',
                    name=sc.replace('_', ' ').title(),
                    line=dict(color=colors[i % len(colors)], width=2),
                    marker=dict(size=7),
                ))
            # Beneish threshold
            if 'beneish_m_score' in selected_trends:
                fig_trend.add_hline(y=-2.22, line_dash='dash', line_color='red',
                                    annotation_text='Beneish threshold (-2.22)')
            fig_trend.update_layout(height=320, margin=dict(t=10, b=20),
                                    xaxis_title='Fiscal Year', yaxis_title='Score')
            st.plotly_chart(fig_trend, use_container_width=True)

    # ── Beneish radar ────────────────────────────────────────────────────────
    beneish_available = [c for c in _BENEISH_LABELS if c in row.index and pd.notna(row.get(c))]
    if beneish_available:
        st.markdown('---')
        c1, c2 = st.columns([1, 1])
        with c1:
            st.subheader('Beneish Component Radar')
            radar_fig = _beneish_radar(row)
            if radar_fig:
                st.plotly_chart(radar_fig, use_container_width=True)
        with c2:
            st.subheader('Beneish Component Values')
            beneish_df = pd.DataFrame([
                {'Component': _BENEISH_LABELS[c], 'Value': round(float(row[c]), 4),
                 'Flag': '⚠️' if float(row[c]) > 1.0 else '✅'}
                for c in _BENEISH_LABELS if c in row.index and pd.notna(row.get(c))
            ])
            st.dataframe(beneish_df, use_container_width=True, hide_index=True)

    # ── Feature groups ───────────────────────────────────────────────────────
    st.markdown('---')
    st.subheader('Feature Deep Dive')
    for group_name, feature_list in _FEATURE_GROUPS.items():
        avail = [f for f in feature_list if f in row.index and pd.notna(row.get(f))]
        if not avail:
            continue
        with st.expander(f'{group_name}  ({len(avail)} features)', expanded=(group_name == '🚨 Fraud Signals')):
            n_per_row = 4
            for chunk_start in range(0, len(avail), n_per_row):
                chunk = avail[chunk_start:chunk_start + n_per_row]
                metric_cols = st.columns(n_per_row)
                for j, feat in enumerate(chunk):
                    val = row[feat]
                    metric_cols[j].metric(feat.replace('_', ' ').title(), f'{val:.4f}')

    # ── Full feature table ───────────────────────────────────────────────────
    with st.expander('📋 All numeric features (latest year)', expanded=False):
        numeric_row = {
            k: round(float(v), 5)
            for k, v in row.items()
            if isinstance(v, (int, float, np.floating, np.integer)) and pd.notna(v)
        }
        full_df = pd.DataFrame(list(numeric_row.items()), columns=['Feature', 'Value'])
        st.dataframe(full_df, use_container_width=True, height=420, hide_index=True)

    # ── ML model score drill-down ────────────────────────────────────────────
    if models and meta:
        st.markdown('---')
        st.subheader('ML Score Breakdown')
        horizon_sel = st.selectbox('Horizon', [h for h in ['1y', '3y', '5y'] if h in models],
                                   key='profile_ml_horizon')
        if horizon_sel and horizon_sel in meta:
            m = meta[horizon_sel]
            feats = [f for f in m['features'] if f in row.index]
            fill_vals = m.get('train_medians', {})
            X_single = pd.DataFrame([{f: (row[f] if pd.notna(row.get(f)) else fill_vals.get(f, 0.0))
                                       for f in feats}])
            try:
                prob = models[horizon_sel].predict_proba(X_single)[0, 1]
                st.metric(f'ML Score ({horizon_sel})', f'{prob:.4f}',
                          delta='High risk' if prob > 0.7 else 'Medium' if prob > 0.4 else 'Low risk')
                # Try SHAP if available
                try:
                    import shap
                    explainer = shap.TreeExplainer(models[horizon_sel])
                    shap_vals = explainer.shap_values(X_single)
                    if isinstance(shap_vals, list):
                        sv = shap_vals[1][0]
                    else:
                        sv = shap_vals[0] if shap_vals.ndim == 2 else shap_vals
                    shap_df = pd.DataFrame({'Feature': feats, 'SHAP': sv}).sort_values('SHAP', key=abs, ascending=False).head(15)
                    fig_shap = px.bar(shap_df, x='SHAP', y='Feature', orientation='h',
                                      color='SHAP', color_continuous_scale='RdBu_r',
                                      title=f'Top 15 SHAP values ({horizon_sel})')
                    fig_shap.update_layout(height=420, margin=dict(t=40, b=20))
                    st.plotly_chart(fig_shap, use_container_width=True)
                except ImportError:
                    st.info('Install shap (`pip install shap`) to see feature-level SHAP explanations.')
                except Exception:
                    pass
            except Exception as e:
                st.error(f'Could not score: {e}')


# ── Realtime Chart ────────────────────────────────────────────────────────────

def tab_realtime_chart(df_all: pd.DataFrame) -> None:
    st.title('📈 Realtime Price Chart')
    st.caption('Live price data via yfinance · fraud score overlay from screener dataset')

    col1, col2, col3 = st.columns([3, 1, 1])
    tickers = sorted(df_all['ticker'].unique().tolist())
    with col1:
        ticker = st.selectbox('Ticker', tickers, key='chart_ticker')
    with col2:
        period = st.selectbox('Period', ['6mo', '1y', '2y', '3y', '5y'], index=2, key='chart_period')
    with col3:
        chart_type = st.selectbox('Chart type', ['Candlestick', 'Line', 'OHLC'], key='chart_type')

    # ── Price chart ─────────────────────────────────────────────────────────
    try:
        import yfinance as yf
        with st.spinner(f'Fetching {ticker}…'):
            hist = yf.Ticker(ticker).history(period=period)

        if hist.empty:
            st.warning(f'No price data for {ticker}. Try a US-listed ticker (e.g. AAPL, MSFT).')
        else:
            hist.index = pd.to_datetime(hist.index)

            fig_price = go.Figure()
            if chart_type == 'Candlestick':
                fig_price.add_trace(go.Candlestick(
                    x=hist.index, open=hist['Open'], high=hist['High'],
                    low=hist['Low'], close=hist['Close'],
                    name=ticker, increasing_line_color='#26A69A', decreasing_line_color='#EF5350',
                ))
            elif chart_type == 'OHLC':
                fig_price.add_trace(go.Ohlc(
                    x=hist.index, open=hist['Open'], high=hist['High'],
                    low=hist['Low'], close=hist['Close'], name=ticker,
                ))
            else:
                fig_price.add_trace(go.Scatter(
                    x=hist.index, y=hist['Close'], mode='lines',
                    name=ticker, line=dict(color='#42A5F5', width=2),
                ))

            # Overlay fraud score vertical bands from screener data
            co_df = df_all[df_all['ticker'] == ticker].sort_values('fiscal_year')
            if not co_df.empty and 'beneish_m_score' in co_df.columns:
                for _, yr_row in co_df.iterrows():
                    bm = yr_row.get('beneish_m_score')
                    fy = yr_row.get('fiscal_year')
                    if pd.isna(bm) or pd.isna(fy):
                        continue
                    color = 'rgba(239,83,80,0.12)' if bm > -2.22 else 'rgba(38,166,154,0.08)'
                    x_pos = f'{int(fy)}-12-31'
                    fig_price.add_vline(x=x_pos, line_dash='dot', line_color='grey',
                                        annotation_text=f'FY{int(fy)} M={bm:.1f}',
                                        annotation_font_size=9)

            fig_price.update_layout(
                title=f'{ticker} — {period} Price Chart',
                xaxis_rangeslider_visible=False,
                height=460,
                margin=dict(t=50, b=20),
                yaxis_title='Price (USD)',
            )
            st.plotly_chart(fig_price, use_container_width=True)

            # Volume chart
            fig_vol = go.Figure(go.Bar(
                x=hist.index, y=hist['Volume'],
                marker_color='#78909C', name='Volume', opacity=0.6,
            ))
            fig_vol.update_layout(height=150, margin=dict(t=5, b=20),
                                  yaxis_title='Volume', showlegend=False)
            st.plotly_chart(fig_vol, use_container_width=True)

    except ImportError:
        st.error('yfinance is not installed — run `pip install yfinance`')
    except Exception as e:
        st.error(f'Could not load price data: {e}')

    # ── Fraud score timeline for this company ───────────────────────────────
    co_df = df_all[df_all['ticker'] == ticker].sort_values('fiscal_year')
    if not co_df.empty:
        st.markdown('---')
        st.subheader(f'Fraud Score Timeline — {ticker}')

        score_options = [c for c in ['beneish_m_score', 'altman_z_score', 'piotroski_f_score',
                                      'composite_score', 'sloan_accruals', 'value_composite',
                                      'quality_composite'] if c in co_df.columns]
        selected_scores = st.multiselect(
            'Scores to overlay', score_options,
            default=[c for c in ['beneish_m_score', 'composite_score'] if c in co_df.columns],
            key='chart_scores_overlay',
        )
        if selected_scores:
            fig_scores = go.Figure()
            colors = ['#EF5350', '#42A5F5', '#66BB6A', '#FFA726', '#AB47BC', '#26C6DA', '#EC407A']
            for i, sc in enumerate(selected_scores):
                data_sc = co_df[['fiscal_year', sc]].dropna()
                fig_scores.add_trace(go.Bar(
                    x=data_sc['fiscal_year'], y=data_sc[sc],
                    name=sc.replace('_', ' ').title(),
                    marker_color=colors[i % len(colors)],
                    opacity=0.75,
                ))
            if 'beneish_m_score' in selected_scores:
                fig_scores.add_hline(y=-2.22, line_dash='dash', line_color='red',
                                     annotation_text='Beneish threshold')
            fig_scores.update_layout(height=300, barmode='group',
                                     margin=dict(t=10, b=20),
                                     xaxis_title='Fiscal Year')
            st.plotly_chart(fig_scores, use_container_width=True)

        # Key financials table
        with st.expander('Key financials by year', expanded=False):
            fin_cols = [c for c in ['fiscal_year', 'revenue', 'net_income', 'total_assets',
                                     'total_debt', 'cash', 'operating_cash_flow',
                                     'gross_margin', 'net_margin', 'roe', 'roa']
                        if c in co_df.columns]
            st.dataframe(co_df[fin_cols].set_index('fiscal_year').sort_index(ascending=False),
                         use_container_width=True)


# ── Market Overview ───────────────────────────────────────────────────────────

def tab_market_overview(df_all: pd.DataFrame) -> None:
    st.title('🌍 Market Risk Overview')

    latest_year = int(df_all['fiscal_year'].max())
    col_yr, col_metric = st.columns([1, 2])
    with col_yr:
        year_sel = st.selectbox(
            'Year', sorted(df_all['fiscal_year'].unique().tolist(), reverse=True),
            index=0, key='overview_year',
        )
    df_yr = df_all[df_all['fiscal_year'] == year_sel].copy()

    risk_metric_options = [c for c in ['beneish_m_score', 'altman_z_score', 'composite_score',
                                        'sloan_accruals', 'piotroski_f_score'] if c in df_yr.columns]
    with col_metric:
        risk_metric = st.selectbox('Risk metric', risk_metric_options, key='overview_metric')

    if not risk_metric:
        st.warning('No risk metrics found in dataset.')
        return

    st.markdown(f'**FY{int(year_sel)} · {len(df_yr):,} companies · metric: `{risk_metric}`**')

    # ── Summary KPIs ────────────────────────────────────────────────────────
    kpi_cols = st.columns(4)
    metric_vals = df_yr[risk_metric].dropna()
    kpi_cols[0].metric('Companies', f'{len(df_yr):,}')
    kpi_cols[1].metric('Mean', f'{metric_vals.mean():.3f}' if len(metric_vals) else '—')
    kpi_cols[2].metric('Median', f'{metric_vals.median():.3f}' if len(metric_vals) else '—')
    if risk_metric == 'beneish_m_score':
        pct_risky = (metric_vals > -2.22).mean() * 100 if len(metric_vals) else 0
        kpi_cols[3].metric('% Risky (M > −2.22)', f'{pct_risky:.1f}%')
    elif risk_metric == 'altman_z_score':
        pct_distress = (metric_vals < 1.81).mean() * 100 if len(metric_vals) else 0
        kpi_cols[3].metric('% Distress (Z < 1.81)', f'{pct_distress:.1f}%')
    else:
        kpi_cols[3].metric('Std Dev', f'{metric_vals.std():.3f}' if len(metric_vals) else '—')

    st.markdown('---')

    # ── Score distribution ──────────────────────────────────────────────────
    col_dist, col_top = st.columns([1, 1])
    with col_dist:
        st.subheader('Score Distribution')
        fig_dist = px.histogram(df_yr, x=risk_metric, nbins=60,
                                color_discrete_sequence=['#42A5F5'],
                                labels={risk_metric: risk_metric.replace('_', ' ').title()})
        if risk_metric == 'beneish_m_score':
            fig_dist.add_vline(x=-2.22, line_dash='dash', line_color='red',
                               annotation_text='Manipulation threshold (−2.22)')
        elif risk_metric == 'altman_z_score':
            fig_dist.add_vline(x=1.81, line_dash='dash', line_color='red',
                               annotation_text='Distress threshold (1.81)')
            fig_dist.add_vline(x=2.99, line_dash='dash', line_color='orange',
                               annotation_text='Grey zone (2.99)')
        fig_dist.update_layout(height=350, margin=dict(t=20, b=20))
        st.plotly_chart(fig_dist, use_container_width=True)

    with col_top:
        st.subheader('Top 20 Highest Risk')
        ascending = risk_metric in ('piotroski_f_score',)
        top_risky = df_yr.nsmallest(20, risk_metric) if not ascending else df_yr.nsmallest(20, risk_metric)
        if risk_metric == 'beneish_m_score':
            top_risky = df_yr.nlargest(20, risk_metric)

        show_cols = [c for c in ['ticker', 'name', 'market', 'exchange', risk_metric,
                                  'altman_z_score', 'piotroski_f_score', 'composite_score']
                     if c in top_risky.columns]
        st.dataframe(top_risky[show_cols].reset_index(drop=True),
                     use_container_width=True, hide_index=True)

    # ── By-market bar chart ──────────────────────────────────────────────────
    if 'market' in df_yr.columns:
        st.markdown('---')
        st.subheader(f'{risk_metric.replace("_", " ").title()} by Market')
        mkt_agg = (df_yr.groupby('market')[risk_metric]
                      .agg(['mean', 'median', 'count'])
                      .rename(columns={'mean': 'Mean', 'median': 'Median', 'count': 'Count'})
                      .reset_index()
                      .sort_values('Mean', ascending=(risk_metric not in ('beneish_m_score',))))

        fig_mkt = go.Figure()
        fig_mkt.add_trace(go.Bar(x=mkt_agg['market'], y=mkt_agg['Mean'],
                                  name='Mean', marker_color='#EF5350'))
        fig_mkt.add_trace(go.Bar(x=mkt_agg['market'], y=mkt_agg['Median'],
                                  name='Median', marker_color='#42A5F5'))
        fig_mkt.update_layout(barmode='group', height=350, margin=dict(t=20, b=20),
                               yaxis_title=risk_metric.replace('_', ' ').title())
        st.plotly_chart(fig_mkt, use_container_width=True)

    # ── Score over time ──────────────────────────────────────────────────────
    st.markdown('---')
    st.subheader(f'{risk_metric.replace("_", " ").title()} Over Time')
    time_agg = (df_all.groupby('fiscal_year')[risk_metric]
                    .agg(['mean', 'median'])
                    .rename(columns={'mean': 'Mean', 'median': 'Median'})
                    .reset_index())
    fig_time = go.Figure()
    fig_time.add_trace(go.Scatter(x=time_agg['fiscal_year'], y=time_agg['Mean'],
                                   mode='lines+markers', name='Mean',
                                   line=dict(color='#EF5350', width=2)))
    fig_time.add_trace(go.Scatter(x=time_agg['fiscal_year'], y=time_agg['Median'],
                                   mode='lines+markers', name='Median',
                                   line=dict(color='#42A5F5', width=2, dash='dot')))
    if risk_metric == 'beneish_m_score':
        fig_time.add_hline(y=-2.22, line_dash='dash', line_color='red',
                           annotation_text='Threshold')
    fig_time.update_layout(height=320, margin=dict(t=10, b=20),
                            xaxis_title='Fiscal Year',
                            yaxis_title=risk_metric.replace('_', ' ').title())
    st.plotly_chart(fig_time, use_container_width=True)

    # ── Scatter: risk vs quality ─────────────────────────────────────────────
    scatter_y = 'quality_composite' if 'quality_composite' in df_yr.columns else 'piotroski_f_score'
    if scatter_y in df_yr.columns:
        st.markdown('---')
        st.subheader(f'Risk vs Quality Scatter — FY{int(year_sel)}')
        scatter_df = df_yr[[risk_metric, scatter_y, 'ticker', 'market']].dropna()
        if len(scatter_df) > 10:
            fig_scatter = px.scatter(
                scatter_df.head(2000),
                x=risk_metric, y=scatter_y,
                color='market',
                hover_data=['ticker'],
                opacity=0.55,
                labels={
                    risk_metric: risk_metric.replace('_', ' ').title(),
                    scatter_y:   scatter_y.replace('_', ' ').title(),
                },
            )
            if risk_metric == 'beneish_m_score':
                fig_scatter.add_vline(x=-2.22, line_dash='dash', line_color='red')
            fig_scatter.update_layout(height=400, margin=dict(t=20, b=20))
            st.plotly_chart(fig_scatter, use_container_width=True)


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

    with st.sidebar:
        _sidebar_refresh()

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        '📊 Screener',
        '🏢 Company Profile',
        '📈 Realtime Chart',
        '🌍 Market Overview',
        '📉 Backtester',
        '⭐ Watchlist',
        '🎯 Strategies',
        '📖 User Guide',
    ])

    with tab1:
        tab_screener(df_all, models, meta)
    with tab2:
        tab_company_profile(df_all, models, meta)
    with tab3:
        tab_realtime_chart(df_all)
    with tab4:
        tab_market_overview(df_all)
    with tab5:
        tab_backtester()
    with tab6:
        tab_watchlist()
    with tab7:
        tab_strategies()
    with tab8:
        tab_guide()


if __name__ == '__main__':
    main()
