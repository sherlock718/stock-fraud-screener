"""
Unit tests for quality/test_dataset_quality.py

Tests each test_* function with minimal synthetic DataFrames.
"""
from __future__ import annotations

import json
import numpy as np
import pandas as pd
import pytest
from pathlib import Path

from quality.test_dataset_quality import (
    TestResult,
    test_schema as _check_schema,
    test_structural as _check_structural,
    test_market_coverage as _check_market_coverage,
    test_fill_rates as _check_fill_rates,
    test_distributions as _check_distributions,
    test_fraud_labels as _check_fraud_labels,
    test_forward_returns as _check_forward_returns,
    test_growth_winsorization as _check_growth_winsorization,
    test_ml_score_exclusion as _check_ml_score_exclusion,
    test_point_in_time as _check_point_in_time,
    REQUIRED_COLS,
    FRAUD_SCORE_COLS,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def result():
    return TestResult()


@pytest.fixture
def base_df():
    """Minimal DataFrame with all required columns and sane values."""
    rng = np.random.default_rng(99)
    n = 500
    df = pd.DataFrame({
        "ticker": [f"T{i:03d}" for i in range(n)],
        "cik": list(range(n)),
        "fiscal_year": np.tile(range(2010, 2020), n // 10),
        "market": ["US"] * (n // 2) + ["KR"] * (n // 2),
        "period_type": ["annual"] * n,
        "filed_date": pd.to_datetime(
            [f"{fy+1}-03-15" for fy in np.tile(range(2010, 2020), n // 10)]
        ),
        "revenue": rng.uniform(1e6, 1e9, n),
        "net_income": rng.uniform(-1e6, 1e8, n),
        "total_assets": rng.uniform(1e7, 1e10, n),
        "equity": rng.uniform(1e6, 1e9, n),
        "operating_cash_flow": rng.uniform(-1e6, 1e8, n),
        "long_term_debt": rng.uniform(0, 1e8, n),
        "short_term_debt": rng.uniform(0, 1e7, n),
        "net_debt": rng.uniform(-1e7, 1e8, n),
        "enterprise_value": rng.uniform(1e7, 1e10, n),
        "beneish_m_score": rng.uniform(0, 1, n),
        "piotroski_f_score": rng.integers(0, 10, n).astype(float),
        "altman_z_score": rng.uniform(0, 1, n),
        "fraud_score_accounting": rng.uniform(0, 0.5, n),
        "fraud_score_dilution": rng.uniform(0, 0.3, n),
        "fraud_score_quality": rng.uniform(0, 0.4, n),
        "fraud_score_distress": rng.uniform(0, 0.5, n),
        "fraud_score_governance": rng.uniform(0, 0.3, n),
        "fraud_score_composite": rng.uniform(0, 0.5, n),
        "fraud_suspect": rng.integers(0, 2, n),
        "momentum_12m_rank": rng.uniform(0, 1, n),
        "momentum_6m_rank": rng.uniform(0, 1, n),
        "momentum_composite_rank": rng.uniform(0, 1, n),
        "forward_return_1y": rng.uniform(-0.5, 2.0, n),
        "as_of_date": pd.date_range("2011-06-01", periods=n, freq="D"),
        "filing_lag_days": rng.integers(30, 180, n),
        "in_universe": rng.integers(0, 2, n),
        "sector": ["Technology"] * n,
    })
    return df


# ─── TestResult class ────────────────────────────────────────────────────────

def test_result_ok(result):
    result.ok("something passed")
    assert result.passed == 1
    assert result.failed == 0


def test_result_fail(result):
    result.fail("something broke")
    assert result.failed == 1
    assert "[FAIL]" in result.messages[0]


def test_result_summary(result):
    result.ok("a")
    result.fail("b")
    result.warn("c")
    s = result.summary()
    assert "1 passed" in s
    assert "1 failed" in s
    assert "1 warnings" in s


# ─── test_schema ─────────────────────────────────────────────────────────────

def test_schema_passes_with_all_cols(base_df, result):
    _check_schema(base_df, result)
    assert result.failed == 0


def test_schema_fails_missing_col(base_df, result):
    df = base_df.drop(columns=["ticker"])
    _check_schema(df, result)
    assert result.failed == 1


def test_schema_warns_bad_dtype(base_df, result):
    base_df["fiscal_year"] = base_df["fiscal_year"].astype(float)
    _check_schema(base_df, result)
    assert result.warnings == 1


# ─── test_structural ─────────────────────────────────────────────────────────

def test_structural_passes_clean(base_df, result):
    _check_structural(base_df, result)
    assert result.failed == 0


def test_structural_fails_non_annual(base_df, result):
    base_df.loc[0, "period_type"] = "quarterly"
    _check_structural(base_df, result)
    assert result.failed == 1


def test_structural_fails_blank_ticker(base_df, result):
    base_df.loc[0, "ticker"] = ""
    _check_structural(base_df, result)
    assert result.failed == 1


def test_structural_fails_inf_values(base_df, result):
    base_df.loc[0, "revenue"] = np.inf
    _check_structural(base_df, result)
    assert result.failed == 1


# ─── test_market_coverage ────────────────────────────────────────────────────

def test_market_coverage_passes(base_df, result):
    _check_market_coverage(base_df, result)
    # US has 250 tickers < MIN_TICKERS[US]=500, so it warns but doesn't fail
    assert result.failed == 0


def test_market_coverage_warns_missing_market(result):
    df = pd.DataFrame({
        "ticker": ["A"] * 10,
        "fiscal_year": list(range(2010, 2020)),
        "market": ["XX"] * 10,
    })
    _check_market_coverage(df, result)
    # All expected markets (US, CA, KR, JP, BR) get "no rows found" warnings
    assert result.warnings >= 5


# ─── test_fill_rates ─────────────────────────────────────────────────────────

def test_fill_rates_passes(base_df, result):
    _check_fill_rates(base_df, result)
    assert result.failed == 0


def test_fill_rates_fails_low_fill(base_df, result):
    # Set 50% of revenue to null (threshold is 90%)
    base_df.loc[:len(base_df)//2, "revenue"] = np.nan
    _check_fill_rates(base_df, result)
    assert result.failed >= 1


# ─── test_distributions ──────────────────────────────────────────────────────

def test_distributions_passes(base_df, result):
    _check_distributions(base_df, result)
    assert result.failed == 0


def test_distributions_fails_fraud_score_out_of_range(base_df, result):
    base_df.loc[0, "fraud_score_accounting"] = 1.5
    _check_distributions(base_df, result)
    assert result.failed == 1


def test_distributions_fails_in_universe_bad_values(base_df, result):
    base_df.loc[0, "in_universe"] = 99
    _check_distributions(base_df, result)
    assert result.failed == 1


# ─── test_fraud_labels ───────────────────────────────────────────────────────

def test_fraud_labels_warns_when_col_absent(base_df, result):
    _check_fraud_labels(base_df, result)
    assert result.warnings == 1  # fraud_confirmed absent


def test_fraud_labels_passes_clean(base_df, result):
    base_df["fraud_confirmed"] = 0
    base_df.loc[:5, "fraud_confirmed"] = 1
    base_df.loc[:5, "fraud_suspect"] = 0
    _check_fraud_labels(base_df, result)
    assert result.failed == 0


def test_fraud_labels_fails_leakage(base_df, result):
    base_df["fraud_confirmed"] = 0
    base_df.loc[:5, "fraud_confirmed"] = 1
    base_df.loc[:5, "fraud_suspect"] = 1  # should be 0 on confirmed rows
    _check_fraud_labels(base_df, result)
    assert result.failed == 1


# ─── test_forward_returns ────────────────────────────────────────────────────

def test_forward_returns_passes(base_df, result):
    _check_forward_returns(base_df, result)
    assert result.failed == 0


def test_forward_returns_fails_not_winsorized(base_df, result):
    base_df.loc[0, "forward_return_1y"] = 10.0  # exceeds cap of 5.0
    _check_forward_returns(base_df, result)
    assert result.failed == 1


def test_forward_returns_fails_low_coverage(result):
    df = pd.DataFrame({
        "market": ["US"] * 100,
        "forward_return_1y": [np.nan] * 90 + [0.1] * 10,  # 10% fill
    })
    _check_forward_returns(df, result)
    assert result.failed >= 1


# ─── test_growth_winsorization ───────────────────────────────────────────────

def test_growth_winsorization_passes(base_df, result):
    rng = np.random.default_rng(7)
    base_df["revenue_growth_yoy"] = rng.uniform(-0.5, 2.0, len(base_df))
    _check_growth_winsorization(base_df, result)
    assert result.failed == 0


def test_growth_winsorization_fails_extreme(base_df, result):
    rng = np.random.default_rng(7)
    base_df["revenue_growth_yoy"] = rng.uniform(-0.5, 2.0, len(base_df))
    # One extreme outlier that's >50x p99
    base_df.loc[0, "revenue_growth_yoy"] = 50000.0
    _check_growth_winsorization(base_df, result)
    assert result.failed == 1


# ─── test_ml_score_exclusion ─────────────────────────────────────────────────

def test_ml_score_exclusion_passes(result, tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    feat_file = models_dir / "feature_sets_27.json"
    feat_file.write_text(json.dumps({"features": ["roe", "pe_ratio", "momentum_12m"]}))
    _check_ml_score_exclusion(result, tmp_path)
    assert result.failed == 0


def test_ml_score_exclusion_fails_leaked(result, tmp_path):
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    feat_file = models_dir / "feature_sets_27.json"
    feat_file.write_text(json.dumps({"features": ["roe", "ml_1y", "momentum_12m"]}))
    _check_ml_score_exclusion(result, tmp_path)
    assert result.failed == 1


def test_ml_score_exclusion_warns_no_files(result, tmp_path):
    _check_ml_score_exclusion(result, tmp_path)
    assert result.warnings == 1


# ─── test_point_in_time ──────────────────────────────────────────────────────

def test_point_in_time_passes(base_df, result):
    # filed_date is already after fiscal_year end in base_df
    _check_point_in_time(base_df, result)
    assert result.failed == 0


def test_point_in_time_warns_missing_cols(result):
    df = pd.DataFrame({"ticker": ["A"]})
    _check_point_in_time(df, result)
    assert result.warnings == 1


def test_point_in_time_fails_negative_lag(result):
    df = pd.DataFrame({
        "fiscal_year": [2020] * 100,
        "filed_date": pd.to_datetime(["2018-01-01"] * 100),  # filed 2yrs before FY end
    })
    _check_point_in_time(df, result)
    assert result.failed >= 1
