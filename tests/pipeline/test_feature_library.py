"""Tests for pipeline/feature_library.py — add_normalised_ratios and add_piotroski_ext."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext


# ═══════════════════════════════════════════════════════════════════════════════
# add_normalised_ratios
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalisedRatios:
    """Known-input → known-output for asset-normalised ratios."""

    def _base_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            'total_assets': [1000.0, 2000.0, 500.0],
            'intangibles': [100.0, 400.0, 50.0],
            'goodwill': [50.0, 200.0, 25.0],
            'depreciation': [30.0, 60.0, 15.0],
            'financing_cash_flow': [-80.0, -200.0, -40.0],
            'fcf': [120.0, 300.0, 60.0],
            'tax_expense': [20.0, 50.0, 10.0],
            'pretax_income': [100.0, -50.0, 40.0],
        })

    def test_intangibles_to_assets(self):
        df = add_normalised_ratios(self._base_df())
        expected = pd.Series([0.10, 0.20, 0.10])
        pd.testing.assert_series_equal(df['intangibles_to_assets'], expected, check_names=False)

    def test_goodwill_to_assets(self):
        df = add_normalised_ratios(self._base_df())
        expected = pd.Series([0.05, 0.10, 0.05])
        pd.testing.assert_series_equal(df['goodwill_to_assets'], expected, check_names=False)

    def test_depreciation_to_assets(self):
        df = add_normalised_ratios(self._base_df())
        expected = pd.Series([0.03, 0.03, 0.03])
        pd.testing.assert_series_equal(df['depreciation_to_assets'], expected, check_names=False)

    def test_financing_cashflow_to_assets(self):
        df = add_normalised_ratios(self._base_df())
        expected = pd.Series([-0.08, -0.10, -0.08])
        pd.testing.assert_series_equal(df['financing_cashflow_to_assets'], expected, check_names=False)

    def test_fcf_to_assets(self):
        df = add_normalised_ratios(self._base_df())
        expected = pd.Series([0.12, 0.15, 0.12])
        pd.testing.assert_series_equal(df['fcf_to_assets'], expected, check_names=False)

    def test_effective_tax_rate_positive_pti(self):
        df = add_normalised_ratios(self._base_df())
        # Row 0: 20/100 = 0.20, Row 1: pti<0 → NaN, Row 2: 10/40 = 0.25
        assert df.loc[0, 'effective_tax_rate'] == pytest.approx(0.20)
        assert pd.isna(df.loc[1, 'effective_tax_rate'])
        assert df.loc[2, 'effective_tax_rate'] == pytest.approx(0.25)

    def test_zero_total_assets_produces_nan(self):
        df = pd.DataFrame({
            'total_assets': [0.0, 1000.0],
            'intangibles': [100.0, 200.0],
        })
        df = add_normalised_ratios(df)
        assert pd.isna(df.loc[0, 'intangibles_to_assets'])
        assert df.loc[1, 'intangibles_to_assets'] == pytest.approx(0.20)

    def test_missing_total_assets_no_crash(self):
        df = pd.DataFrame({'intangibles': [100.0], 'fcf': [50.0]})
        result = add_normalised_ratios(df)
        assert 'intangibles_to_assets' not in result.columns

    def test_does_not_overwrite_existing_column(self):
        df = self._base_df()
        df['intangibles_to_assets'] = 999.0
        result = add_normalised_ratios(df)
        assert (result['intangibles_to_assets'] == 999.0).all()

    def test_nan_in_source_propagates(self):
        df = pd.DataFrame({
            'total_assets': [1000.0, 2000.0],
            'intangibles': [np.nan, 200.0],
        })
        df = add_normalised_ratios(df)
        assert pd.isna(df.loc[0, 'intangibles_to_assets'])
        assert df.loc[1, 'intangibles_to_assets'] == pytest.approx(0.10)


# ═══════════════════════════════════════════════════════════════════════════════
# add_piotroski_ext
# ═══════════════════════════════════════════════════════════════════════════════

class TestPiotroskiExt:
    """Known-input → known-output for Piotroski extension signals."""

    def _base_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            'ticker': ['AAPL', 'AAPL', 'AAPL', 'MSFT', 'MSFT', 'MSFT'],
            'fiscal_year': [2019, 2020, 2021, 2019, 2020, 2021],
            'shares_outstanding': [100.0, 95.0, 110.0, 200.0, 200.0, 180.0],
            'gross_margin': [0.40, 0.42, 0.41, 0.60, 0.55, 0.58],
            'asset_turnover': [0.80, 0.85, 0.83, 0.70, 0.72, 0.75],
            'piotroski_f_score': [5.0, 6.0, 4.0, 7.0, 5.0, 6.0],
        })

    def test_shares_ok_no_dilution(self):
        df = add_piotroski_ext(self._base_df())
        # AAPL: [NaN→0, 95<=100→1, 110<=95→0]
        # MSFT: [NaN→0, 200<=200→1, 180<=200→1]
        assert df.loc[df['ticker'] == 'AAPL', 'piotroski_shares_ok'].tolist() == [0.0, 1.0, 0.0]
        assert df.loc[df['ticker'] == 'MSFT', 'piotroski_shares_ok'].tolist() == [0.0, 1.0, 1.0]

    def test_delta_gm_improving_margin(self):
        df = add_piotroski_ext(self._base_df())
        # AAPL: [NaN→0, 0.42>0.40→1, 0.41>0.42→0]
        # MSFT: [NaN→0, 0.55>0.60→0, 0.58>0.55→1]
        assert df.loc[df['ticker'] == 'AAPL', 'piotroski_delta_gm'].tolist() == [0.0, 1.0, 0.0]
        assert df.loc[df['ticker'] == 'MSFT', 'piotroski_delta_gm'].tolist() == [0.0, 0.0, 1.0]

    def test_delta_at_improving_turnover(self):
        df = add_piotroski_ext(self._base_df())
        # AAPL: [NaN→0, 0.85>0.80→1, 0.83>0.85→0]
        # MSFT: [NaN→0, 0.72>0.70→1, 0.75>0.72→1]
        assert df.loc[df['ticker'] == 'AAPL', 'piotroski_delta_at'].tolist() == [0.0, 1.0, 0.0]
        assert df.loc[df['ticker'] == 'MSFT', 'piotroski_delta_at'].tolist() == [0.0, 1.0, 1.0]

    def test_f_score_9_sums_correctly(self):
        df = add_piotroski_ext(self._base_df())
        # AAPL row 1 (2020): f_score=6 + shares_ok=1 + delta_gm=1 + delta_at=1 = 9
        aapl_2020 = df[(df['ticker'] == 'AAPL') & (df['fiscal_year'] == 2020)]
        assert aapl_2020['piotroski_f_score_9'].values[0] == pytest.approx(9.0)
        # MSFT row 2 (2021): f_score=6 + shares_ok=1 + delta_gm=1 + delta_at=1 = 9
        msft_2021 = df[(df['ticker'] == 'MSFT') & (df['fiscal_year'] == 2021)]
        assert msft_2021['piotroski_f_score_9'].values[0] == pytest.approx(9.0)

    def test_f_score_9_first_row_includes_zeros(self):
        df = add_piotroski_ext(self._base_df())
        # AAPL first year (2019): f_score=5 + shares_ok=0 + delta_gm=0 + delta_at=0 = 5
        aapl_2019 = df[(df['ticker'] == 'AAPL') & (df['fiscal_year'] == 2019)]
        assert aapl_2019['piotroski_f_score_9'].values[0] == pytest.approx(5.0)

    def test_missing_source_column_skips_signal(self):
        df = pd.DataFrame({
            'ticker': ['A', 'A'],
            'fiscal_year': [2020, 2021],
            'gross_margin': [0.3, 0.4],
            'piotroski_f_score': [5.0, 6.0],
        })
        result = add_piotroski_ext(df)
        assert 'piotroski_shares_ok' not in result.columns
        assert 'piotroski_delta_gm' in result.columns
        assert 'piotroski_delta_at' not in result.columns
        # f_score_9 = f_score + delta_gm only
        assert result.loc[1, 'piotroski_f_score_9'] == pytest.approx(7.0)

    def test_no_f_score_base_skips_f_score_9(self):
        df = pd.DataFrame({
            'ticker': ['A', 'A'],
            'fiscal_year': [2020, 2021],
            'shares_outstanding': [100.0, 90.0],
            'gross_margin': [0.3, 0.4],
            'asset_turnover': [0.5, 0.6],
        })
        result = add_piotroski_ext(df)
        assert 'piotroski_shares_ok' in result.columns
        assert 'piotroski_f_score_9' not in result.columns

    def test_nan_in_source_produces_nan_signal(self):
        df = pd.DataFrame({
            'ticker': ['A', 'A', 'A'],
            'fiscal_year': [2019, 2020, 2021],
            'shares_outstanding': [100.0, np.nan, 80.0],
            'piotroski_f_score': [5.0, 6.0, 7.0],
        })
        result = add_piotroski_ext(df)
        # Row 1: NaN <= 100 → False → 0.0 (NaN comparison yields False in pandas)
        # Row 2: 80 <= NaN → False → 0.0
        assert result.loc[1, 'piotroski_shares_ok'] == 0.0
        assert result.loc[2, 'piotroski_shares_ok'] == 0.0

    def test_single_ticker_first_year_all_zeros(self):
        df = pd.DataFrame({
            'ticker': ['X'],
            'fiscal_year': [2020],
            'shares_outstanding': [100.0],
            'gross_margin': [0.5],
            'asset_turnover': [0.8],
            'piotroski_f_score': [4.0],
        })
        result = add_piotroski_ext(df)
        # Only row → shift produces NaN → comparison yields False → 0
        assert result['piotroski_shares_ok'].values[0] == 0.0
        assert result['piotroski_delta_gm'].values[0] == 0.0
        assert result['piotroski_delta_at'].values[0] == 0.0
        assert result['piotroski_f_score_9'].values[0] == pytest.approx(4.0)
