from __future__ import annotations

import io
import json

import pandas as pd
import streamlit as st

from src.config import (
    DATA_PATH, META_PATH, MODELS_DIR, SECTOR_PATH, HF_REPO,
)


def _hf_download_bytes(repo_id: str, filename: str) -> bytes | None:
    """Download a file from HuggingFace Hub as raw bytes. Returns None on failure."""
    try:
        from huggingface_hub import hf_hub_download
        local = hf_hub_download(repo_id=repo_id, filename=filename, repo_type='dataset')
        return local.__class__(local).read_bytes() if hasattr(local, 'read_bytes') else open(local, 'rb').read()
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
