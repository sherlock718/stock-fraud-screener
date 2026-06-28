"""
Canonical EXCLUDE sets and shared data loader for the modeling package.

All modules that need to distinguish "ML-eligible features" from metadata/targets/scores
import from here. Keeping one source of truth prevents silent divergence where a new
column leaks into training in one module but not another.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from _root import ROOT
from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext

BASE = ROOT
DATA_PATH = BASE / 'data' / 'historical_dataset_clean.parquet'

# ── Columns to exclude from ML features ──────────────────────────────────────

EXCLUDE_COLS = {
    # identifiers & metadata
    'cik', 'ticker', 'name', 'filed_date', 'fiscal_year', 'fiscal_quarter',
    'period_type', 'exchange', 'sic_code', 'sic_description', 'market',
    'country', 'accounting_std', 'size_category_label', 'corp_code', 'acc_mt',
    # raw dollar amounts — size-contaminated; normalised versions used instead
    'revenue', 'net_income', 'gross_profit', 'operating_income', 'pretax_income',
    'cogs', 'sga_expense', 'rd_expense', 'depreciation', 'da_expense',
    'operating_cash_flow', 'financing_cash_flow', 'investing_cash_flow',
    'capex', 'fcf',
    'long_term_debt', 'short_term_debt', 'total_debt',
    'total_assets', 'total_equity', 'current_assets', 'current_liabilities',
    'accounts_receivable', 'accounts_payable', 'receivables',
    'cash', 'intangibles', 'goodwill', 'ppe_net', 'noa',
    'market_cap_at_filing', 'tax_expense', 'interest_expense',
    'common_shares_outstanding', 'eps_diluted', 'eps_basic',
    'retained_earnings', 'additional_paid_in_capital', 'inventory',
    # fraud labels — targets/outputs, not input features
    'fraud_confirmed', 'fraud_suspect', 'fraud_label',
    # ML-derived scores — in-sample contamination
    'ml_1y', 'ml_3y', 'ml_5y', 'ml_6m', 'ml_2y',
    'ml_pred_excess_3y',
    'ml_1y_oof', 'ml_3y_oof', 'ml_5y_oof', 'ml_6m_oof', 'ml_2y_oof',
    # Alpha composites — hand-crafted blends cause signal double-counting
    'alpha_fraud_risk', 'alpha_composite', 'alpha_value', 'alpha_quality',
    'alpha_growth', 'alpha_momentum',
}

EXCLUDE_PATTERNS = [
    'forward_return', 'beat_local_market', 'excess_return_local',
    'benchmark_return',
    'fraud_score_',
    'ml_pred_excess',
    'composite_score',
]

# ── Production gate thresholds (single source of truth) ──────────────────────
BENEISH_THRESHOLD = -1.78
TREE_THRESHOLD = 0.55
PIOTROSKI_MIN = 3
VALUE_GATE_PCT = 0.70
ALTMAN_Z_MIN = 1.0
MAX_MARKET_CAP_PROD = 10_000_000_000


# ── Shared data loader ────────────────────────────────────────────────────────

def load_data(parquet_path: Path | None = None) -> pd.DataFrame:
    """Load, deduplicate, winsorize, and enrich the historical dataset."""
    path = parquet_path or DATA_PATH
    df_raw = pd.read_parquet(path)
    df = df_raw[df_raw['period_type'] == 'annual'].copy()
    df = df[df['fiscal_year'].between(2008, 2025)].copy()

    df = df.sort_values('total_assets', ascending=False, na_position='last')
    df = df.drop_duplicates(subset=['ticker', 'fiscal_year'], keep='first')

    for col in [c for c in df.columns if 'growth_yoy' in c]:
        lo, hi = df[col].quantile(0.01), df[col].quantile(0.99)
        df[col] = df[col].clip(lo, hi)

    for col in ['forward_return_1y', 'forward_return_3y', 'forward_return_5y']:
        if col in df.columns:
            df[col] = df[col].clip(-1.0, 5.0)

    df = add_piotroski_ext(df)
    df = add_normalised_ratios(df)

    labels_path = BASE / 'data' / 'fraud_labels.parquet'
    if labels_path.exists():
        ldf = pd.read_parquet(labels_path)
        if 'fraud_confirmed' in ldf.columns:
            confirmed = ldf[ldf['fraud_confirmed'].astype(bool)][['ticker', 'fraud_year']].copy()
        else:
            confirmed = ldf[['ticker', 'fraud_year']].copy()
        if not confirmed.empty:
            confirmed = confirmed.rename(columns={'fraud_year': 'fiscal_year'})
            confirmed['fraud_label'] = 1
            confirmed = confirmed.drop_duplicates(['ticker', 'fiscal_year'])
            df = df.merge(confirmed, on=['ticker', 'fiscal_year'], how='left')
            df['fraud_label'] = df['fraud_label'].fillna(0).astype(int)
            n_labeled = int(df['fraud_label'].sum())
            print(f'  AAER labels merged: {n_labeled:,} fraud-confirmed rows ({n_labeled/len(df)*100:.2f}%)')

    return df.reset_index(drop=True)


def get_feature_candidates(df: pd.DataFrame) -> list[str]:
    """Return numeric columns eligible as ML features (excludes metadata/targets/scores)."""
    return [
        c for c in df.columns
        if c not in EXCLUDE_COLS
        and not any(p in c for p in EXCLUDE_PATTERNS)
        and df[c].dtype in [np.float64, np.float32, np.int64, np.int32, 'Int64']
        and df[c].notna().mean() > 0.10
    ]
