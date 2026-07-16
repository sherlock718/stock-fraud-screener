"""Tests for merged step6_clean.py enrichment functions."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline.step6_clean import (
    add_forecast_flag,
    build_confidence,
    consistency_score,
    coverage_score,
    fix_gross_margin,
    fix_null_columns,
    run_quality_fix,
    run_survivorship,
    timeliness_score,
    winsorize_accruals,
    FORECAST_YEAR,
    DELISTING_RETURN,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Quality Fix Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestFixNullColumns:
    def test_drops_all_null_column(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [np.nan, np.nan, np.nan]})
        result, dropped = fix_null_columns(df)
        assert "b" not in result.columns
        assert "b" in dropped

    def test_keeps_partial_column(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [np.nan, 5.0, np.nan]})
        result, dropped = fix_null_columns(df)
        assert "b" in result.columns
        assert dropped == []


class TestForecastFlag:
    def test_flag_set_correctly(self):
        df = pd.DataFrame({"fiscal_year": [2020, 2024, 2025, 2026]})
        result = add_forecast_flag(df)
        assert result["is_forecast"].tolist() == [False, False, True, True]


class TestWinsorizeAccruals:
    def test_clips_extreme_values(self):
        np.random.seed(42)
        vals = np.random.normal(0, 0.1, 100).tolist() + [50.0, -50.0]
        n = len(vals)
        df = pd.DataFrame({
            "accruals_to_assets": vals,
            "fiscal_year": [2020] * n,
            "market": ["US"] * n,
            "period_type": ["annual"] * n,
            "entity_id": [f"US:{i}" for i in range(n)],
            "filed_date": pd.to_datetime(["2021-03-01"] * n),
            "availability_timestamp": pd.to_datetime(["2021-03-01"] * n),
            "availability_provenance": ["sec_primary_filing"] * n,
        })
        result = winsorize_accruals(df)
        assert result["accruals_to_assets"].max() < 50.0
        assert result["accruals_to_assets"].min() > -50.0

    def test_no_column_returns_unchanged(self):
        df = pd.DataFrame({"revenue": [100, 200]})
        result = winsorize_accruals(df)
        assert list(result.columns) == ["revenue"]


class TestFixGrossMargin:
    def test_divides_values_above_threshold(self):
        df = pd.DataFrame({"gross_margin": [0.8, 80.0, 1.2, 55.0]})
        result = fix_gross_margin(df)
        assert result["gross_margin"].iloc[0] == 0.8
        assert result["gross_margin"].iloc[1] == 0.8
        assert result["gross_margin"].iloc[2] == 1.2
        assert result["gross_margin"].iloc[3] == 0.55


# ═══════════════════════════════════════════════════════════════════════════════
# Survivorship Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestSurvivorship:
    def _make_df(self):
        return pd.DataFrame({
            "ticker": ["AAA"] * 3 + ["BBB"] * 3 + ["CCC"] * 2,
            "fiscal_year": [2018, 2019, 2020, 2018, 2019, 2023, 2022, 2023],
            "period_type": ["annual"] * 8,
            "filed_date": pd.to_datetime([
                "2019-03-01", "2020-03-01", "2021-03-01",
                "2019-03-01", "2020-03-01", "2024-03-01",
                "2023-03-01", "2024-03-01",
            ]),
            "forward_return_1y": [0.1, 0.2, np.nan, 0.05, 0.1, 0.15, np.nan, 0.2],
            "forward_return_6m": [0.05, 0.1, np.nan, 0.03, 0.05, 0.08, np.nan, 0.1],
            "forward_return_2y": [0.2, np.nan, np.nan, 0.1, 0.2, 0.3, np.nan, 0.2],
            "forward_return_3y": [0.3, np.nan, np.nan, 0.2, 0.3, 0.4, np.nan, 0.3],
            "forward_return_5y": [0.5, np.nan, np.nan, 0.3, 0.4, 0.6, np.nan, 0.4],
            "benchmark_return_3y": [0.1, 0.1, 0.2, 0.1, 0.1, 0.2, 0.1, 0.2],
            "excess_return_local_3y": [0.2, np.nan, np.nan, 0.1, 0.2, 0.2, np.nan, 0.1],
            "beat_local_market_3y": [1, np.nan, np.nan, 1, 1, 1, np.nan, 1],
            "benchmark_label_end_date_3y": pd.to_datetime([
                "2022-03-01", "2023-03-01", "2024-03-05",
                "2022-03-01", "2023-03-01", "2027-03-01",
                "2026-03-01", "2027-03-01",
            ]),
        })

    def test_flags_delisted_tickers(self):
        df = self._make_df()
        result = run_survivorship(df, lag=3)
        # AAA last year=2020, max=2023, lag=3 → 2020 <= 2023-3=2020 → delisted
        assert result[result["ticker"] == "AAA"]["likely_delisted"].all()
        # BBB last year=2023, not delisted
        assert not result[result["ticker"] == "BBB"]["likely_delisted"].any()

    def test_imputes_pessimistic_return(self):
        df = self._make_df()
        result = run_survivorship(df, lag=3)
        # AAA's last row (fiscal_year=2020) had NaN forward_return_1y → imputed
        aaa_last = result[(result["ticker"] == "AAA") & (result["fiscal_year"] == 2020)]
        assert aaa_last["forward_return_1y"].iloc[0] == DELISTING_RETURN

    def test_policy_imputed_three_year_label_has_no_availability_date(self):
        df = self._make_df()
        result = run_survivorship(df, lag=3)
        aaa_last = result[(result["ticker"] == "AAA") & (result["fiscal_year"] == 2020)]
        assert aaa_last["forward_return_3y"].iloc[0] == DELISTING_RETURN
        assert pd.isna(aaa_last["stock_label_end_date_3y"].iloc[0])
        assert pd.isna(aaa_last["label_end_date_3y"].iloc[0])
        assert aaa_last["policy_stock_label_available_date_3y"].iloc[0] == pd.Timestamp("2024-03-01")
        assert aaa_last["policy_label_available_date_3y"].iloc[0] == pd.Timestamp("2024-03-05")
        assert aaa_last["stock_label_provenance_3y"].iloc[0] == "policy_imputed_likely_delisted"
        assert aaa_last["label_provenance_3y"].iloc[0] == "policy_imputed_likely_delisted"
        assert aaa_last["excess_return_local_3y"].iloc[0] == pytest.approx(-0.7)

    def test_policy_availability_waits_for_longer_target_horizon(self):
        result = run_survivorship(self._make_df(), lag=3)
        aaa_last = result[(result["ticker"] == "AAA") & (result["fiscal_year"] == 2020)]
        expected = pd.Timestamp("2021-03-01") + pd.Timedelta(days=1825)
        assert aaa_last["policy_label_available_date_5y"].iloc[0] == expected

    @pytest.mark.parametrize("horizon", ["6m", "1y", "2y", "3y", "5y"])
    def test_policy_sensitivity_covers_every_trained_horizon(self, horizon):
        result = run_survivorship(self._make_df(), lag=3)
        aaa_last = result[(result["ticker"] == "AAA") & (result["fiscal_year"] == 2020)]
        assert aaa_last[f"forward_return_{horizon}"].iloc[0] == DELISTING_RETURN
        assert pd.notna(aaa_last[f"policy_stock_label_available_date_{horizon}"].iloc[0])


# ═══════════════════════════════════════════════════════════════════════════════
# Confidence Score Tests (supplement existing test_p0g)
# ═══════════════════════════════════════════════════════════════════════════════

class TestConfidenceIntegration:
    def test_returns_bounded_series(self):
        df = pd.DataFrame({
            "revenue": [1000, 0, np.nan],
            "total_assets": [5000, 5000, np.nan],
            "fiscal_year": [2020, 2020, 2010],
            "filing_lag_days": [30, 200, 90],
        })
        score = build_confidence(df)
        assert score.min() >= 0.0
        assert score.max() <= 1.0
        assert len(score) == 3

    def test_full_row_scores_higher_than_empty(self):
        full = pd.DataFrame({
            "revenue": [5_000_000],
            "net_income": [500_000],
            "total_assets": [10_000_000],
            "total_equity": [4_000_000],
            "operating_cash_flow": [800_000],
            "gross_profit": [2_000_000],
            "operating_income": [1_000_000],
            "fiscal_year": [2020],
            "filing_lag_days": [30],
        })
        empty = pd.DataFrame({
            "revenue": [np.nan],
            "fiscal_year": [2010],
            "filing_lag_days": [300],
        })
        assert build_confidence(full).iloc[0] > build_confidence(empty).iloc[0]
