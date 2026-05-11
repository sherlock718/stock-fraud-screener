from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime

import streamlit as st

from src.config import BASE, REFRESH_PATH, _IS_CLOUD


def sidebar_refresh() -> None:
    st.markdown('### 🔄 Data Refresh')

    status: dict = {}
    if REFRESH_PATH.exists():
        try:
            status = json.loads(REFRESH_PATH.read_text())
        except Exception:
            pass

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

    if col1.button('⚡ Quick\n~5 min', use_container_width=True,
                   help='Re-compute features from existing data (no API calls)'):
        _run_refresh('quick')

    if col2.button('📈 Prices\n~45 min', use_container_width=True,
                   help='Re-pull yfinance prices + features'):
        if st.session_state.get('_prices_confirm'):
            _run_refresh('prices')
            st.session_state['_prices_confirm'] = False
        else:
            st.session_state['_prices_confirm'] = True
            st.warning('Click again to confirm ~45 min price refresh')

    if col3.button('🔄 Full\n~4 h', use_container_width=True,
                   help='Full rebuild from SEC EDGAR (hours)'):
        if st.session_state.get('_full_confirm'):
            _run_refresh('full')
            st.session_state['_full_confirm'] = False
        else:
            st.session_state['_full_confirm'] = True
            st.warning('Click again to confirm full ~4 h rebuild')
