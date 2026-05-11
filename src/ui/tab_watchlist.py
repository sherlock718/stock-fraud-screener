from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime

import pandas as pd
import streamlit as st

from src.config import BASE, WL_PATH, WL_LIVE


def tab_watchlist() -> None:
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
