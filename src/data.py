from __future__ import annotations

import io
import json
import os

import pandas as pd
import streamlit as st

from src.config import (
    DATA_PATH, META_PATH, MODELS_DIR, SECTOR_PATH, HF_REPO,
)

# HF_TOKEN for private repo access (set as Streamlit secret or env var)
_HF_TOKEN = os.environ.get('HF_TOKEN', '')


def _hf_download_bytes(repo_id: str, filename: str) -> bytes | None:
    """Download a file from HuggingFace Hub as raw bytes. Returns None on failure."""
    try:
        from huggingface_hub import hf_hub_download
        kwargs: dict = dict(repo_id=repo_id, filename=filename, repo_type='dataset')
        if _HF_TOKEN:
            kwargs['token'] = _HF_TOKEN
        local = hf_hub_download(**kwargs)
        with open(local, 'rb') as f:
            return f.read()
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    if HF_REPO and not DATA_PATH.exists():
        with st.spinner('Downloading dataset from HuggingFace Hub…'):
            raw = _hf_download_bytes(HF_REPO, 'historical_dataset_clean.parquet')
        if raw is not None:
            df = pd.read_parquet(io.BytesIO(raw))
        else:
            st.error(
                'Could not load dataset. '
                'Set HF_REPO (and HF_TOKEN for private repos) in Streamlit secrets.'
            )
            return pd.DataFrame()
    elif DATA_PATH.exists():
        df = pd.read_parquet(DATA_PATH)
    else:
        st.error(
            'No dataset found. '
            'Add HF_REPO = "your-username/stock-screener-data" to Streamlit secrets.'
        )
        return pd.DataFrame()

    df = df[df['period_type'] == 'annual'].copy()

    # Derived feature expected by trained models but not stored in parquet
    if 'financing_cashflow_to_assets' not in df.columns:
        if 'financing_cash_flow' in df.columns and 'total_assets' in df.columns:
            df['financing_cashflow_to_assets'] = (
                df['financing_cash_flow'] / df['total_assets'].replace(0, float('nan'))
            )

    if SECTOR_PATH.exists():
        sec = pd.read_parquet(SECTOR_PATH)
        df = df.merge(
            sec[['ticker', 'sector', 'industry',
                 'dividendYield', 'dividendRate', 'payoutRatio',
                 'trailingAnnualDividendYield', 'trailingAnnualDividendRate',
                 'exDividendDate']],
            on='ticker', how='left',
        )
    return df


@st.cache_resource(show_spinner=False)
def load_models() -> tuple[dict, dict]:
    import joblib

    meta: dict = {}
    models: dict = {}

    if META_PATH.exists():
        meta = json.loads(META_PATH.read_text())
    elif HF_REPO:
        raw = _hf_download_bytes(HF_REPO, 'models/model_meta.json')
        if raw is not None:
            meta = json.loads(raw.decode())

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
