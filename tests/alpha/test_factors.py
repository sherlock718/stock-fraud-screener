"""Unit tests for alpha/factors/ — each factor compute() + composite."""

import numpy as np
import pandas as pd
import pytest

from alpha.factors import value, quality, momentum, growth, fraud_risk, composite
from alpha.factors.composite import DEFAULT_WEIGHTS
from alpha.factors.fraud_risk import _ML_REQUIREMENTS
from modeling.prediction_lineage import add_synthetic_manifest
from modeling.prediction_lineage import manifest_col


@pytest.fixture
def sample_df():
    """Minimal synthetic DataFrame with columns needed by all factors."""
    rng = np.random.default_rng(42)
    n = 60
    df = pd.DataFrame({
        "fiscal_year": [2020] * 30 + [2021] * 30,
        "market": ["US"] * 60,
        "period_type": ["annual"] * 60,
        "entity_id": [f"US:{i}" for i in range(60)],
        "filed_date": pd.to_datetime(
            ["2021-03-01"] * 15 + ["2021-06-01"] * 15
            + ["2022-03-01"] * 15 + ["2022-06-01"] * 15
        ),
        "availability_timestamp": pd.to_datetime(
            ["2021-03-01"] * 15 + ["2021-06-01"] * 15
            + ["2022-03-01"] * 15 + ["2022-06-01"] * 15
        ),
        "availability_provenance": ["sec_primary_filing"] * 60,
        # Value signals
        "ev_ebitda": rng.uniform(3, 30, n),
        "ev_revenue": rng.uniform(0.5, 10, n),
        "fcf_yield": rng.uniform(-0.05, 0.15, n),
        "earnings_yield": rng.uniform(0, 0.2, n),
        "book_to_market": rng.uniform(0.1, 2.0, n),
        "ps_ratio": rng.uniform(0.5, 15, n),
        "pe_ratio": rng.uniform(5, 50, n),
        # Quality signals
        "roe": rng.uniform(-0.1, 0.4, n),
        "roa": rng.uniform(-0.05, 0.2, n),
        "roic": rng.uniform(-0.05, 0.3, n),
        "gross_margin": rng.uniform(0.1, 0.8, n),
        "operating_margin": rng.uniform(-0.1, 0.4, n),
        "ocf_to_ni": rng.uniform(0.5, 2.0, n),
        "piotroski_f_score": rng.integers(0, 10, n).astype(float),
        "accruals_to_assets": rng.uniform(-0.1, 0.2, n),
        "sloan_accruals": rng.uniform(-0.15, 0.15, n),
        "gross_profit_to_assets": rng.uniform(0.05, 0.5, n),
        # Momentum signals
        "momentum_12m_prior": rng.uniform(-0.3, 0.8, n),
        "momentum_6m_prior": rng.uniform(-0.2, 0.5, n),
        "momentum_3m_prior": rng.uniform(-0.15, 0.3, n),
        "momentum_12m_rank": rng.uniform(0, 1, n),
        "momentum_6m_rank": rng.uniform(0, 1, n),
        "momentum_3m_rank": rng.uniform(0, 1, n),
        # Growth signals
        "revenue_cagr_3y": rng.uniform(-0.1, 0.5, n),
        "revenue_growth_yoy": rng.uniform(-0.2, 0.8, n),
        "eps_growth_yoy": rng.uniform(-1.0, 2.0, n),
        "net_income_growth_yoy": rng.uniform(-1.0, 2.0, n),
        "ocf_growth_yoy": rng.uniform(-0.5, 1.5, n),
        "gross_profit_growth_yoy": rng.uniform(-0.3, 0.8, n),
        # Fraud risk signals
        "beneish_m_score": rng.uniform(-3.5, -1.0, n),
        "ohlson_prob_bankruptcy": rng.uniform(0, 0.5, n),
        "altman_z_score": rng.uniform(0.5, 5.0, n),
        "fraud_score_composite": rng.uniform(0, 1, n),
        "fraud_score_accounting": rng.uniform(0, 1, n),
        "fraud_score_distress": rng.uniform(0, 1, n),
        "ml_1y_oof": rng.uniform(0, 1, n),
        "ml_3y_oof": rng.uniform(0, 1, n),
        "ml_5y_oof": rng.uniform(0, 1, n),
    })
    df["decision_timestamp"] = pd.to_datetime(
        ["2022-01-01"] * 30 + ["2023-01-01"] * 30
    )
    return add_synthetic_manifest(
        df, _ML_REQUIREMENTS, source="oof_oos"
    )


class TestValueFactor:
    def test_output_range(self, sample_df):
        result = value(sample_df)
        assert result.min() >= 0.0
        assert result.max() <= 1.0

    def test_length_matches_input(self, sample_df):
        result = value(sample_df)
        assert len(result) == len(sample_df)

    def test_handles_nan(self, sample_df):
        df = sample_df.copy()
        df.loc[0:5, "ev_ebitda"] = np.nan
        df.loc[10:15, "fcf_yield"] = np.nan
        result = value(df)
        assert len(result) == len(df)
        assert result.notna().sum() > 0

    def test_no_signals_returns_nan(self):
        df = pd.DataFrame({"fiscal_year": [2020] * 10, "market": ["US"] * 10})
        result = value(df)
        assert result.isna().all()


class TestQualityFactor:
    def test_output_range(self, sample_df):
        result = quality(sample_df)
        assert result.min() >= 0.0
        assert result.max() <= 1.0

    def test_handles_nan(self, sample_df):
        df = sample_df.copy()
        df.loc[0:10, "roe"] = np.nan
        result = quality(df)
        assert result.notna().sum() > 0

    def test_no_signals_returns_nan(self):
        df = pd.DataFrame({"fiscal_year": [2020] * 10, "market": ["US"] * 10})
        result = quality(df)
        assert result.isna().all()


class TestMomentumFactor:
    def test_output_range(self, sample_df):
        result = momentum(sample_df)
        assert result.min() >= 0.0
        assert result.max() <= 1.0

    def test_handles_nan(self, sample_df):
        df = sample_df.copy()
        df.loc[0:10, "momentum_12m_prior"] = np.nan
        result = momentum(df)
        assert result.notna().sum() > 0

    def test_no_signals_returns_nan(self):
        df = pd.DataFrame({"fiscal_year": [2020] * 10, "market": ["US"] * 10})
        result = momentum(df)
        assert result.isna().all()


class TestGrowthFactor:
    def test_output_range(self, sample_df):
        result = growth(sample_df)
        assert result.min() >= 0.0
        assert result.max() <= 1.0

    def test_handles_nan(self, sample_df):
        df = sample_df.copy()
        df.loc[0:10, "revenue_growth_yoy"] = np.nan
        result = growth(df)
        assert result.notna().sum() > 0

    def test_no_signals_returns_nan(self):
        df = pd.DataFrame({"fiscal_year": [2020] * 10, "market": ["US"] * 10})
        result = growth(df)
        assert result.isna().all()


class TestFraudRiskFactor:
    def test_output_range(self, sample_df):
        result = fraud_risk(sample_df)
        assert result.min() >= 0.0
        assert result.max() <= 1.0

    def test_handles_nan(self, sample_df):
        df = sample_df.copy()
        df.loc[0:10, "beneish_m_score"] = np.nan
        df.loc[5:15, "altman_z_score"] = np.nan
        result = fraud_risk(df)
        assert result.notna().sum() > 0

    def test_no_signals_returns_nan(self):
        df = pd.DataFrame({"fiscal_year": [2020] * 10, "market": ["US"] * 10})
        result = fraud_risk(df)
        assert result.isna().all()

    def test_high_altman_gets_higher_score(self, sample_df):
        """Higher altman_z_score (healthier) should correlate with higher fraud_risk score."""
        df = sample_df.copy()
        # Only keep altman to isolate the signal
        for col, _ in [("beneish_m_score", True), ("ohlson_prob_bankruptcy", True),
                       ("fraud_score_composite", True), ("fraud_score_accounting", True),
                       ("fraud_score_distress", True)]:
            df.drop(columns=col, inplace=True)
        for col in ["ml_1y_oof", "ml_3y_oof", "ml_5y_oof"]:
            df[col] = 0.5
            df[manifest_col(col, "raw_prediction")] = 0.5
            df[manifest_col(col, "transformed_score")] = 0.5
        result = fraud_risk(df)
        # Within each group, rank correlation should be positive
        corr = result.corr(df["altman_z_score"])
        assert corr > 0


class TestComposite:
    def test_returns_dataframe_with_expected_columns(self, sample_df):
        result = composite(sample_df)
        expected_cols = {"alpha_value", "alpha_quality", "alpha_momentum",
                         "alpha_growth", "alpha_fraud_risk", "alpha_composite"}
        assert expected_cols == set(result.columns)

    def test_composite_in_range(self, sample_df):
        result = composite(sample_df)
        assert result["alpha_composite"].min() >= 0.0
        assert result["alpha_composite"].max() <= 1.0

    def test_default_weights_sum_to_one(self):
        assert abs(sum(DEFAULT_WEIGHTS.values()) - 1.0) < 1e-9

    def test_custom_weights(self, sample_df):
        w = {"value": 1.0, "quality": 0.0, "momentum": 0.0,
             "growth": 0.0, "fraud_risk": 0.0}
        result = composite(sample_df, weights=w)
        # Composite should equal value score when only value has weight
        value_score = result["alpha_value"]
        np.testing.assert_allclose(
            result["alpha_composite"].values, value_score.values, atol=1e-10
        )

    def test_handles_missing_columns(self):
        """Composite should still work if some factor columns are missing."""
        df = pd.DataFrame({
            "fiscal_year": [2020] * 20,
            "market": ["US"] * 20,
            "period_type": ["annual"] * 20,
            "entity_id": [f"US:missing-{i}" for i in range(20)],
            "filed_date": pd.to_datetime(["2021-03-01"] * 20),
            "availability_timestamp": pd.to_datetime(["2021-03-01"] * 20),
            "availability_provenance": ["sec_primary_filing"] * 20,
            "roe": np.random.default_rng(0).uniform(0, 0.3, 20),
            "roa": np.random.default_rng(0).uniform(0, 0.2, 20),
        })
        result = composite(df)
        assert len(result) == 20
        # Most factors will be NaN but quality should have values
        assert result["alpha_quality"].notna().any()

    def test_result_index_aligned(self, sample_df):
        result = composite(sample_df)
        assert (result.index == sample_df.index).all()
