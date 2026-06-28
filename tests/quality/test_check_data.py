"""
Unit tests for quality/check_data.py

Tests each validation check with minimal synthetic DataFrames.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import patch


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def good_df():
    """Minimal DataFrame that passes all checks in check_data.run()."""
    rng = np.random.default_rng(42)
    n_tickers = 6000  # > min_companies (3000)
    years = list(range(2008, 2025))  # 17 years
    n_years = len(years)
    # 6000 * 17 = 102,000 rows > min_rows (100,000)
    tickers = np.repeat([f"T{i:04d}" for i in range(n_tickers)], n_years)
    fy = np.tile(years, n_tickers)
    n = len(tickers)
    # 90% US, 10% BR
    markets = np.where(np.arange(n) < n * 0.9, "US", "BR")
    df = pd.DataFrame({
        "ticker": tickers,
        "cik": np.repeat(np.arange(n_tickers), n_years),
        "fiscal_year": fy,
        "market": markets,
        "period_type": "annual",
        "likely_delisted": False,
        "country": markets,
        "forward_return_1y": rng.uniform(-0.5, 0.5, n),
        "revenue_growth_yoy": rng.uniform(-0.5, 10.0, n),
        "total_assets": rng.uniform(1e6, 1e9, n),
        "accruals_to_assets": rng.uniform(-2, 2, n),
        "gross_margin": rng.uniform(0.1, 0.9, n),
        "is_forecast": False,
    })
    for feat in ["roe", "debt_to_equity", "current_ratio", "pe_ratio",
                 "piotroski_f_score", "beneish_m_score",
                 "momentum_12m_prior", "value_composite", "quality_composite"]:
        df[feat] = rng.uniform(0, 1, n)
    return df


@pytest.fixture
def tmp_parquet(good_df, tmp_path):
    """Write good_df to a temp parquet file and return its path."""
    p = tmp_path / "test.parquet"
    good_df.to_parquet(p, index=False)
    return p


# ─── Test: run() with good data passes ──────────────────────────────────────

def test_run_good_data_passes(tmp_parquet):
    from quality.check_data import run
    assert run(tmp_parquet) is True


# ─── Test: missing file returns False ────────────────────────────────────────

def test_run_missing_file():
    from quality.check_data import run
    assert run(Path("/nonexistent/path.parquet")) is False


# ─── Test: min_rows check fails ─────────────────────────────────────────────

def test_fails_min_rows(tmp_path):
    from quality.check_data import run
    df = pd.DataFrame({
        "ticker": ["A"] * 50,
        "cik": [1] * 50,
        "fiscal_year": [2020] * 50,
        "market": ["US"] * 50,
        "period_type": ["annual"] * 50,
        "forward_return_1y": [0.1] * 50,
        "revenue_growth_yoy": [0.1] * 50,
        "total_assets": [1e6] * 50,
        "likely_delisted": [False] * 50,
        "country": ["US"] * 50,
        "is_forecast": [False] * 50,
        "accruals_to_assets": [0.5] * 50,
        "gross_margin": [0.3] * 50,
    })
    p = tmp_path / "small.parquet"
    df.to_parquet(p, index=False)
    assert run(p) is False


# ─── Test: forward_return_1y out of range fails ─────────────────────────────

def test_fails_forward_return_out_of_range(good_df, tmp_path):
    from quality.check_data import run
    good_df.loc[0, "forward_return_1y"] = 10.0  # exceeds max 5.0
    p = tmp_path / "bad_return.parquet"
    good_df.to_parquet(p, index=False)
    assert run(p) is False


# ─── Test: revenue_growth_yoy not winsorized fails ──────────────────────────

def test_fails_revenue_growth_not_winsorized(good_df, tmp_path):
    from quality.check_data import run
    good_df.loc[0, "revenue_growth_yoy"] = 20.0  # exceeds threshold 15.0
    p = tmp_path / "bad_growth.parquet"
    good_df.to_parquet(p, index=False)
    assert run(p) is False


# ─── Test: duplicate annual rows detected ───────────────────────────────────

def test_fails_duplicate_annual_rows(good_df, tmp_path):
    from quality.check_data import run
    # Add a duplicate row
    dup = good_df.iloc[[0]].copy()
    df = pd.concat([good_df, dup], ignore_index=True)
    p = tmp_path / "dup.parquet"
    df.to_parquet(p, index=False)
    assert run(p) is False


# ─── Test: accruals out of range fails ──────────────────────────────────────

def test_fails_accruals_extreme(good_df, tmp_path):
    from quality.check_data import run
    good_df.loc[0, "accruals_to_assets"] = 100.0  # exceeds 50.0 threshold
    p = tmp_path / "bad_accruals.parquet"
    good_df.to_parquet(p, index=False)
    assert run(p) is False


# ─── Test: gross_margin percentage format fails ─────────────────────────────

def test_fails_gross_margin_pct_format(good_df, tmp_path):
    from quality.check_data import run
    # Set >0.1% of rows to have gross_margin > 1.5 (percentage format error)
    n_bad = int(len(good_df) * 0.01)  # 1% > threshold of 0.1%
    good_df.loc[:n_bad, "gross_margin"] = 75.0
    p = tmp_path / "bad_gm.parquet"
    good_df.to_parquet(p, index=False)
    assert run(p) is False


# ─── Test: known null columns still present fails ───────────────────────────

def test_fails_known_null_cols_present(good_df, tmp_path):
    from quality.check_data import run
    good_df["roic"] = np.nan  # should have been dropped
    p = tmp_path / "null_cols.parquet"
    good_df.to_parquet(p, index=False)
    assert run(p) is False


# ─── Test: high null rate on core feature fails ─────────────────────────────

def test_fails_high_null_rate_core_feature(good_df, tmp_path):
    from quality.check_data import run
    # Set >40% of roe to null
    mask = np.zeros(len(good_df), dtype=bool)
    mask[:int(len(good_df) * 0.5)] = True
    good_df.loc[mask, "roe"] = np.nan
    p = tmp_path / "null_feat.parquet"
    good_df.to_parquet(p, index=False)
    assert run(p) is False
