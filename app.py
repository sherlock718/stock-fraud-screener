"""
app_v2.py — Multi-market stock fraud & value screener
Loads data/historical_dataset_clean.parquet + saved ML models.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────
BASE      = Path(__file__).parent
DATA_PATH = BASE / 'data' / 'app_data.parquet'
META_PATH = BASE / 'models' / 'model_meta.json'
MODELS_DIR = BASE / 'models'

MARKET_LABELS = {
    'US': '🇺🇸 United States',
    'CA': '🇨🇦 Canada',
    'BR': '🇧🇷 Brazil',
    'JP': '🇯🇵 Japan',
    'DE': '🇩🇪 Germany', 'FR': '🇫🇷 France', 'IT': '🇮🇹 Italy',
    'ES': '🇪🇸 Spain',   'SE': '🇸🇪 Sweden', 'FI': '🇫🇮 Finland',
    'NL': '🇳🇱 Netherlands', 'PT': '🇵🇹 Portugal', 'DK': '🇩🇰 Denmark',
}

EU_MARKETS = {'DE','FR','IT','ES','SE','FI','NL','PT','DK'}

RISK_SIGNALS = {
    'Beneish M-Score (manipulation)':  ('beneish_m_score',       'high', -2.22),
    'Altman Z-Score (distress)':        ('altman_z_score',        'low',   1.81),
    'Ohlson O-Score (bankruptcy)':      ('ohlson_o_score',        'high',  0.5),
    'High Sloan Accruals':              ('sloan_accruals',        'high',  0.05),
    'Piotroski F-Score (quality)':      ('piotroski_f_score',     'low',   3.0),
    'Likely Delisted':                  ('likely_delisted',       'flag',  True),
}

# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    df = pd.read_parquet(DATA_PATH)
    return df[df['period_type'] == 'annual'].copy()


@st.cache_resource(show_spinner=False)
def load_models() -> tuple[dict, dict]:
    """Returns (models_dict, meta_dict). Models may be empty if not saved yet."""
    models = {}
    meta   = {}
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
    """Add ml_score column using saved model. Returns df with score column."""
    if horizon not in models or horizon not in meta:
        df['ml_score'] = np.nan
        return df
    clf = models[horizon]
    feats = [f for f in meta[horizon]['features'] if f in df.columns]
    X = df[feats].fillna(df[feats].median())
    try:
        df = df.copy()
        df['ml_score'] = clf.predict_proba(X)[:, 1]
    except Exception:
        df['ml_score'] = np.nan
    return df


def composite_rank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a composite rank from value + quality + momentum + low-fraud.
    Higher rank = better candidate. All sub-scores 0–1 (percentile within screen).
    """
    df = df.copy()

    def pct_rank(s: pd.Series, ascending: bool = True) -> pd.Series:
        return s.rank(pct=True, ascending=ascending, na_option='keep')

    components = {}

    # Value (lower PE/PB/EV is better → ascending=True, higher pct = cheaper)
    if 'value_composite' in df.columns:
        components['value'] = pct_rank(df['value_composite'], ascending=False)
    elif 'pe_ratio' in df.columns:
        components['value'] = pct_rank(df['pe_ratio'], ascending=True)

    # Quality (higher ROE/piotroski/gross_margin is better)
    if 'quality_composite' in df.columns:
        components['quality'] = pct_rank(df['quality_composite'], ascending=False)
    elif 'piotroski_f_score' in df.columns:
        components['quality'] = pct_rank(df['piotroski_f_score'], ascending=False)

    # Momentum (higher 12m momentum is better)
    if 'momentum_12m_prior' in df.columns:
        components['momentum'] = pct_rank(df['momentum_12m_prior'], ascending=False)

    # Low fraud (lower Beneish = less manipulation risk → ascending=True)
    if 'beneish_m_score' in df.columns:
        components['fraud_safety'] = pct_rank(df['beneish_m_score'], ascending=True)

    # ML score
    if 'ml_score' in df.columns and df['ml_score'].notna().any():
        components['ml_alpha'] = pct_rank(df['ml_score'], ascending=False)

    if not components:
        df['composite_score'] = np.nan
        return df

    weights = {
        'value':        0.25,
        'quality':      0.20,
        'momentum':     0.20,
        'fraud_safety': 0.20,
        'ml_alpha':     0.15,
    }
    total_w = sum(weights[k] for k in components)
    df['composite_score'] = sum(
        components[k] * (weights[k] / total_w) for k in components
    )
    return df


# ── Streamlit App ─────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title='Stock Fraud & Value Screener',
        page_icon='🔍',
        layout='wide',
        initial_sidebar_state='expanded',
    )

    # ── Load data ─────────────────────────────────────────────────────────────
    with st.spinner('Loading dataset…'):
        df_all = load_data()
    models, meta = load_models()
    model_loaded = bool(models)

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.title('🔍 Screener Controls')

        # Market selector
        st.subheader('Markets')
        avail_markets = sorted(df_all['market'].unique())
        market_options = {MARKET_LABELS.get(m, m): m for m in avail_markets}
        selected_labels = st.multiselect(
            'Select markets',
            options=list(market_options.keys()),
            default=[MARKET_LABELS.get(m, m) for m in avail_markets if m != 'US'] or list(market_options.keys())[:3],
        )
        selected_markets = [market_options[l] for l in selected_labels] if selected_labels else avail_markets

        # Fiscal year
        st.subheader('Fiscal Year')
        year_min = int(df_all['fiscal_year'].min())
        year_max = int(df_all['fiscal_year'].max())
        year_range = st.slider('Fiscal year range', year_min, year_max,
                               (max(year_min, year_max - 5), year_max))

        # Market cap filter
        st.subheader('Company Size')
        cap_preset = st.radio('Market cap preset', [
            'All sizes',
            'Neglected ($50M–$500M)',
            'Small cap ($50M–$2B)',
            'Mid cap ($2B–$10B)',
            'Large cap (>$10B)',
        ], index=0)
        cap_filter = {
            'All sizes':            (0, 1e15),
            'Neglected ($50M–$500M)': (50e6,  500e6),
            'Small cap ($50M–$2B)':   (50e6,  2e9),
            'Mid cap ($2B–$10B)':     (2e9,   10e9),
            'Large cap (>$10B)':      (10e9,  1e15),
        }[cap_preset]

        # ML horizon
        st.subheader('ML Scoring Horizon')
        if model_loaded:
            horizon = st.selectbox('Investment horizon', ['1y', '3y', '5y'], index=0)
        else:
            st.warning('Models not saved yet.\nRun notebook Section 5 first.')
            horizon = '1y'

        # Composite score threshold
        st.subheader('Composite Score Filter')
        min_composite = st.slider('Min composite score (percentile)', 0, 100, 60) / 100

        # Risk flags
        st.subheader('Risk Filters')
        exclude_delisted = st.checkbox('Exclude likely delisted', value=True)
        exclude_beneish  = st.checkbox('Exclude Beneish M > -2.22 (manipulators)', value=True)
        exclude_altman   = st.checkbox('Exclude Altman Z < 1.81 (distressed)', value=False)

        # Text search
        st.subheader('Search')
        text_query = st.text_input('Ticker / company name', placeholder='e.g. AAPL, Samsung…')

    # ── Filter data ───────────────────────────────────────────────────────────
    df = df_all[
        df_all['market'].isin(selected_markets) &
        df_all['fiscal_year'].between(*year_range)
    ].copy()

    # Market cap filter
    if 'market_cap_at_filing' in df.columns:
        df = df[
            df['market_cap_at_filing'].isna() |
            df['market_cap_at_filing'].between(*cap_filter)
        ]

    # Risk exclusions
    if exclude_delisted and 'likely_delisted' in df.columns:
        df = df[~df['likely_delisted']]
    if exclude_beneish and 'beneish_m_score' in df.columns:
        df = df[df['beneish_m_score'].isna() | (df['beneish_m_score'] <= -2.22)]
    if exclude_altman and 'altman_z_score' in df.columns:
        df = df[df['altman_z_score'].isna() | (df['altman_z_score'] >= 1.81)]

    # Text search
    if text_query:
        q = text_query.lower()
        mask = (
            df['ticker'].str.lower().str.contains(q, na=False) |
            df['name'].str.lower().str.contains(q, na=False)
        )
        df = df[mask]

    # ── Score & rank ──────────────────────────────────────────────────────────
    df = score_companies(df, models, meta, horizon)
    df = composite_rank(df)

    # Most recent year per company
    latest = (
        df.sort_values('fiscal_year', ascending=False)
          .drop_duplicates('cik', keep='first')
    )
    latest = latest[latest['composite_score'] >= min_composite].copy()
    latest = latest.sort_values('composite_score', ascending=False)

    # ── Header ────────────────────────────────────────────────────────────────
    st.title('🔍 Stock Fraud & Value Screener')
    model_status = f'ML model loaded ({horizon} horizon)' if model_loaded else '⚠️ ML model not saved yet'
    st.caption(f'{len(df_all):,} total rows | {model_status} | {len(latest):,} companies after filters')

    # ── KPI row ───────────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric('Companies shown', f'{len(latest):,}')
    c2.metric('Markets', len(selected_markets))
    c3.metric('Fiscal years', f'{year_range[0]}–{year_range[1]}')

    if 'beneish_m_score' in latest.columns:
        high_risk = (latest['beneish_m_score'] > -2.22).sum()
        c4.metric('High Beneish risk', f'{high_risk:,}', delta=None)
    else:
        c4.metric('High Beneish risk', 'N/A')

    if model_loaded and 'ml_score' in latest.columns:
        top_decile = (latest['ml_score'] > 0.6).sum()
        c5.metric('ML score > 0.6', f'{top_decile:,}')
    else:
        c5.metric('ML score > 0.6', 'N/A')

    # ── Results table ─────────────────────────────────────────────────────────
    st.subheader('Top Companies by Composite Score')

    display_cols_base = ['ticker', 'name', 'market', 'fiscal_year', 'composite_score']
    display_cols_val  = [c for c in ['value_composite', 'pe_ratio', 'pb_ratio', 'ev_ebitda'] if c in latest.columns][:2]
    display_cols_qual = [c for c in ['quality_composite', 'piotroski_f_score', 'roe', 'gross_margin'] if c in latest.columns][:2]
    display_cols_mom  = [c for c in ['momentum_12m_prior', 'price_to_52w_high'] if c in latest.columns][:1]
    display_cols_risk = [c for c in ['beneish_m_score', 'altman_z_score'] if c in latest.columns]
    display_cols_ml   = ['ml_score'] if model_loaded and 'ml_score' in latest.columns else []
    display_cols_cap  = ['market_cap_at_filing'] if 'market_cap_at_filing' in latest.columns else []

    display_cols = (display_cols_base + display_cols_val + display_cols_qual +
                    display_cols_mom + display_cols_risk + display_cols_ml +
                    display_cols_cap)
    display_cols = [c for c in display_cols if c in latest.columns]

    show_df = latest[display_cols].head(200).copy()

    # Format numbers
    for col in show_df.select_dtypes('float').columns:
        if col == 'composite_score':
            show_df[col] = show_df[col].map(lambda x: f'{x:.3f}' if pd.notna(x) else '')
        elif col == 'market_cap_at_filing':
            show_df[col] = show_df[col].map(
                lambda x: f'${x/1e9:.1f}B' if pd.notna(x) and x >= 1e9
                else (f'${x/1e6:.0f}M' if pd.notna(x) else '')
            )
        elif col in ['pe_ratio', 'pb_ratio', 'ev_ebitda']:
            show_df[col] = show_df[col].map(lambda x: f'{x:.1f}' if pd.notna(x) else '')
        else:
            show_df[col] = show_df[col].map(lambda x: f'{x:.3f}' if pd.notna(x) else '')

    st.dataframe(show_df, use_container_width=True, height=520)

    # ── Company deep-dive ─────────────────────────────────────────────────────
    st.subheader('Company Deep Dive')
    tickers_list = [''] + sorted(latest['ticker'].dropna().unique().tolist())
    selected_ticker = st.selectbox('Select ticker', tickers_list, index=0)

    if selected_ticker:
        co_all = df_all[df_all['ticker'] == selected_ticker].sort_values('fiscal_year')
        co_latest = co_all[co_all['period_type'] == 'annual'].tail(1)
        if co_latest.empty:
            st.warning('No annual data found for this ticker.')
        else:
            row = co_latest.iloc[0]
            st.markdown(f"### {row.get('name','N/A')} ({selected_ticker})")
            d1, d2, d3, d4 = st.columns(4)
            d1.metric('Market', MARKET_LABELS.get(str(row.get('market','')), str(row.get('market',''))))
            d2.metric('Fiscal Year', int(row['fiscal_year']) if pd.notna(row['fiscal_year']) else 'N/A')
            d3.metric('Exchange', str(row.get('exchange', 'N/A')))
            if pd.notna(row.get('market_cap_at_filing')):
                cap = row['market_cap_at_filing']
                d4.metric('Market Cap', f'${cap/1e9:.2f}B' if cap >= 1e9 else f'${cap/1e6:.0f}M')

            # Financials
            st.markdown('**Financials**')
            fin_cols = [c for c in ['revenue','gross_profit','operating_income','net_income',
                                    'total_assets','equity','operating_cash_flow'] if c in co_all.columns]
            fin_data = co_all[co_all['period_type']=='annual'][['fiscal_year'] + fin_cols].set_index('fiscal_year')
            if not fin_data.empty:
                st.dataframe(fin_data.tail(8).applymap(
                    lambda x: f'{x/1e6:,.0f}M' if pd.notna(x) and isinstance(x, (int,float)) else x
                ), use_container_width=True)

            # Risk signals
            st.markdown('**Risk Signals**')
            rc1, rc2, rc3 = st.columns(3)
            cols_row = [rc1, rc2, rc3]
            signal_items = [
                ('Beneish M-Score', 'beneish_m_score', -2.22, 'below', 'red'),
                ('Altman Z-Score',  'altman_z_score',   1.81, 'above', 'green'),
                ('Piotroski F',     'piotroski_f_score', 6.0, 'above', 'green'),
            ]
            for idx, (label, col, thresh, direction, good_color) in enumerate(signal_items):
                if col in row.index and pd.notna(row[col]):
                    val = row[col]
                    if direction == 'below':
                        ok = val <= thresh
                    else:
                        ok = val >= thresh
                    cols_row[idx].metric(label, f'{val:.2f}',
                                         delta='✅ OK' if ok else '⚠️ Risk',
                                         delta_color='normal' if ok else 'inverse')

            # Forward returns history
            if any(c.startswith('forward_return') for c in co_all.columns):
                st.markdown('**Forward Return History (1y)**')
                fwd = co_all[co_all['period_type']=='annual'][['fiscal_year','forward_return_1y']].dropna()
                if not fwd.empty:
                    import matplotlib.pyplot as plt
                    fig, ax = plt.subplots(figsize=(10, 3))
                    colors = ['#4CAF50' if v >= 0 else '#F44336' for v in fwd['forward_return_1y']]
                    ax.bar(fwd['fiscal_year'], fwd['forward_return_1y'] * 100, color=colors, edgecolor='white')
                    ax.axhline(0, color='gray', lw=0.8)
                    ax.set_ylabel('1y Return (%)')
                    ax.set_title(f'{selected_ticker} — Annual 1-Year Forward Return')
                    ax.grid(axis='y', alpha=0.3)
                    st.pyplot(fig, use_container_width=True)
                    plt.close()

    # ── Market overview charts ─────────────────────────────────────────────────
    with st.expander('📊 Dataset Overview Charts', expanded=False):
        import matplotlib.pyplot as plt

        fig, axes = plt.subplots(1, 3, figsize=(16, 4))

        # Companies per market
        mkt_counts = df_all.drop_duplicates('cik').groupby('market').size().sort_values(ascending=False)
        mkt_labels_short = [MARKET_LABELS.get(m, m).split(' ')[-1] for m in mkt_counts.index]
        axes[0].bar(mkt_labels_short, mkt_counts.values, color='#2196F3', edgecolor='white')
        axes[0].set_title('Companies per Market', fontweight='bold')
        axes[0].tick_params(axis='x', rotation=45)
        axes[0].set_ylabel('Companies')

        # Labeled data per fiscal year
        labeled_by_yr = df_all[df_all['forward_return_1y'].notna()].groupby('fiscal_year').size()
        axes[1].fill_between(labeled_by_yr.index, labeled_by_yr.values, alpha=0.7, color='#4CAF50')
        axes[1].set_title('Labeled Rows (1y) per Fiscal Year', fontweight='bold')
        axes[1].set_ylabel('Count'); axes[1].grid(alpha=0.3)

        # Composite score distribution of filtered results
        if 'composite_score' in latest.columns and latest['composite_score'].notna().any():
            axes[2].hist(latest['composite_score'].dropna(), bins=40, color='#FF9800', edgecolor='white', alpha=0.85)
            axes[2].axvline(min_composite, color='red', ls='--', lw=1.5, label=f'Threshold {min_composite:.2f}')
            axes[2].set_title('Composite Score Distribution (filtered)', fontweight='bold')
            axes[2].set_xlabel('Composite Score'); axes[2].legend()
        else:
            axes[2].text(0.5, 0.5, 'No composite scores\n(run notebook Section 5)', ha='center', va='center', transform=axes[2].transAxes)

        plt.tight_layout()
        st.pyplot(fig, use_container_width=True)
        plt.close()

    # ── Download ──────────────────────────────────────────────────────────────
    st.subheader('Export')
    csv = latest[display_cols].head(1000).to_csv(index=False)
    st.download_button('⬇️ Download top 1000 as CSV', csv, 'screener_results.csv', 'text/csv')


if __name__ == '__main__':
    main()
