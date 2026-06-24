from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime

import pandas as pd
import streamlit as st

from src.config import BASE, WL_PATH, WL_LIVE
from src.scoring import composite_rank, score_companies

_FRAUD_TREND_COLS = [
    'fraud_score_composite',
    'fraud_score_accounting',
    'fraud_score_dilution',
    'fraud_score_quality',
    'fraud_score_distress',
]


def _fraud_alerts(df_all: pd.DataFrame, tickers: list[str]) -> list[dict]:
    """Return list of YoY fraud score change dicts for tickers in watchlist."""
    if 'fraud_score_composite' not in df_all.columns:
        return []
    src = df_all[df_all['period_type'] == 'annual'] if 'period_type' in df_all.columns else df_all
    alerts = []
    for tk in tickers:
        rows = (src[src['ticker'] == tk]
                .sort_values('fiscal_year', ascending=False)
                .head(2))
        if len(rows) < 2:
            continue
        curr, prev = rows.iloc[0], rows.iloc[1]
        curr_v = curr.get('fraud_score_composite')
        prev_v = prev.get('fraud_score_composite')
        if pd.isna(curr_v) or pd.isna(prev_v):
            continue
        delta = float(curr_v) - float(prev_v)
        if abs(delta) < 0.05:
            continue
        # Threshold crossing check
        crossed = ''
        if prev_v <= 0.65 < curr_v:
            crossed = ' — CROSSED into 🔴 HIGH RISK'
        elif prev_v <= 0.35 < curr_v:
            crossed = ' — CROSSED into 🟠 MEDIUM RISK'
        elif curr_v <= 0.35 < prev_v:
            crossed = ' — DROPPED to 🟢 LOW RISK'
        direction = '⬆️ Worsened' if delta > 0 else '⬇️ Improved'
        alerts.append({
            'Ticker':     tk,
            'Curr Score': round(float(curr_v), 3),
            'Prev Score': round(float(prev_v), 3),
            'Δ Change':   round(delta, 3),
            'Direction':  direction + crossed,
            'Curr Year':  int(curr['fiscal_year']),
            'Prev Year':  int(prev['fiscal_year']),
        })
    return sorted(alerts, key=lambda x: abs(x['Δ Change']), reverse=True)


def tab_watchlist(df_all: pd.DataFrame | None = None, models: dict | None = None, meta: dict | None = None) -> None:
    st.header('⭐ Watchlist & Price Alerts')

    def _load_wl() -> dict:
        return json.loads(WL_PATH.read_text()) if WL_PATH.exists() else {}

    def _save_wl(wl: dict) -> None:
        WL_PATH.write_text(json.dumps(wl, indent=2))

    wl = _load_wl()

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

    st.subheader(f'Watching {len(wl)} ticker(s)')
    rows_data = []
    _latest_scores: dict[str, float | None] = {}
    if df_all is not None and 'fraud_score_composite' in df_all.columns:
        src = (df_all[df_all['period_type'] == 'annual']
               if 'period_type' in df_all.columns else df_all)
        for tk in wl:
            tr = src[src['ticker'] == tk].sort_values('fiscal_year', ascending=False).head(1)
            _latest_scores[tk] = float(tr.iloc[0]['fraud_score_composite']) if not tr.empty and pd.notna(tr.iloc[0].get('fraud_score_composite')) else None

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
        fs = _latest_scores.get(tk)
        fraud_badge = (
            f'🔴 {fs:.3f}' if fs is not None and fs > 0.65 else
            f'🟠 {fs:.3f}' if fs is not None and fs > 0.35 else
            f'🟢 {fs:.3f}' if fs is not None else '—'
        )
        rows_data.append({
            'Ticker':        tk,
            'Price':         f'${price:,.2f}' if price else '—',
            'Above':         f'${above:,.2f}' if above else '—',
            'Below':         f'${below:,.2f}' if below else '—',
            'Alert':         alert_str,
            'Fraud Score':   fraud_badge,
            'Added':         meta.get('added', ''),
            'Note':          meta.get('note', ''),
        })

    df_wl = pd.DataFrame(rows_data)
    st.dataframe(df_wl, use_container_width=True, hide_index=True)

    # --- YoY fraud score change alerts ---
    if df_all is not None:
        fa = _fraud_alerts(df_all, list(wl.keys()))
        if fa:
            st.markdown('---')
            st.subheader('📊 Fraud Score Change Alerts')
            st.caption('Year-over-year changes in fraud_score_composite (≥ 0.05 threshold).')
            fa_df = pd.DataFrame(fa)
            st.dataframe(fa_df, use_container_width=True, hide_index=True)
            for item in fa:
                if abs(item['Δ Change']) >= 0.10 or 'CROSSED' in item['Direction']:
                    badge = '🔴' if item['Δ Change'] > 0 else '🟢'
                    st.warning(
                        f"{badge} **{item['Ticker']}**: fraud score "
                        f"{item['Prev Score']:.3f} → {item['Curr Score']:.3f} "
                        f"({item['Δ Change']:+.3f}) FY{item['Prev Year']}→FY{item['Curr Year']} "
                        f"— {item['Direction']}"
                    )

    with st.expander('🗑 Remove tickers'):
        to_del = st.multiselect('Select tickers to remove', options=sorted(wl.keys()))
        if st.button('Remove selected', type='secondary') and to_del:
            for tk in to_del:
                wl.pop(tk, None)
            _save_wl(wl)
            st.success(f'Removed: {", ".join(to_del)}')
            st.rerun()

    st.download_button('⬇️ Download watchlist CSV',
                       df_wl.to_csv(index=False),
                       'watchlist.csv', 'text/csv')

    # --- vs Screener Top Picks ---
    if df_all is not None and models and meta:
        with st.expander('📊 Compare vs Screener Top Picks'):
            src = df_all[df_all['period_type'] == 'annual'] if 'period_type' in df_all.columns else df_all
            latest = src.sort_values('fiscal_year', ascending=False).drop_duplicates('ticker', keep='first')
            scored = score_companies(latest, models, meta, horizon='1y')
            scored = composite_rank(scored)
            scored = scored[scored['composite_score'].notna()].copy()
            scored['rank'] = scored['composite_score'].rank(ascending=False, method='min').astype(int)
            total = len(scored)

            wl_tickers = list(wl.keys())
            wl_rows = scored[scored['ticker'].isin(wl_tickers)].copy()

            if wl_rows.empty:
                st.info('No watchlist tickers found in the current screener universe.')
            else:
                display_cols = ['ticker', 'rank', 'composite_score', 'ml_score',
                                'fraud_score_composite', 'piotroski_f_score', 'fiscal_year']
                show = [c for c in display_cols if c in wl_rows.columns]
                disp = wl_rows[show].copy()
                disp.insert(2, 'percentile', ((1 - (disp['rank'] - 1) / total) * 100).round(1))
                disp = disp.sort_values('rank')
                st.caption(f'Watchlist tickers ranked against {total:,} companies in screener (1y horizon, latest filing)')
                st.dataframe(disp, use_container_width=True, hide_index=True)

                missing = [t for t in wl_tickers if t not in wl_rows['ticker'].values]
                if missing:
                    st.caption(f'Not in screener universe: {", ".join(missing)}')
