"""Tests for pipeline/p0g_confidence_score.py — data confidence scoring."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline.p0g_confidence_score import (
    build_confidence,
    consistency_score,
    coverage_score,
    timeliness_score,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _full_row(**overrides) -> dict:
    """Row with all core columns populated (should get high confidence)."""
    base = {
        'revenue': 5_000_000.0,
        'net_income': 500_000.0,
        'total_assets': 10_000_000.0,
        'total_equity': 4_000_000.0,
        'operating_cash_flow': 800_000.0,
        'gross_profit': 2_000_000.0,
        'operating_income': 1_000_000.0,
        'beneish_m_score': -2.5,
        'altman_z_score': 3.0,
        'piotroski_f_score': 7.0,
        'sloan_accruals': 0.02,
        'entry_price': 50.0,
        'forward_return_1y': 0.15,
        'net_margin': 0.10,
        'roe': 0.125,
        'roa': 0.05,
        'ocf_margin': 0.16,
        'debt_to_equity': 0.5,
        'current_ratio': 2.0,
        'filing_lag_days': 45,
        'fiscal_year': 2020,
    }
    base.update(overrides)
    return base


# ═══════════════════════════════════════════════════════════════════════════════
# Score Range
# ═══════════════════════════════════════════════════════════════════════════════

class TestScoreRange:
    """data_confidence must always be in [0, 1]."""

    def test_full_row_high_confidence(self):
        df = pd.DataFrame([_full_row()])
        score = build_confidence(df)
        assert score.iloc[0] >= 0.8

    def test_empty_row_low_confidence(self):
        df = pd.DataFrame([{'fiscal_year': 2020, 'filing_lag_days': 45}])
        score = build_confidence(df)
        assert 0.0 <= score.iloc[0] <= 1.0
        assert score.iloc[0] < 0.7

    def test_score_always_bounded_0_1(self):
        rows = [
            _full_row(),
            _full_row(revenue=np.nan, net_income=np.nan, total_assets=np.nan),
            {'fiscal_year': 2005, 'filing_lag_days': 500},
        ]
        df = pd.DataFrame(rows)
        scores = build_confidence(df)
        assert (scores >= 0.0).all()
        assert (scores <= 1.0).all()

    def test_score_dtype_float(self):
        df = pd.DataFrame([_full_row()])
        score = build_confidence(df)
        assert score.dtype == np.float64 or score.dtype == np.float32


# ═══════════════════════════════════════════════════════════════════════════════
# Coverage Score
# ═══════════════════════════════════════════════════════════════════════════════

class TestCoverage:
    """Fraction of core columns that are non-null."""

    def test_all_present_gives_1(self):
        df = pd.DataFrame([_full_row()])
        score = coverage_score(df)
        assert score.iloc[0] == pytest.approx(1.0)

    def test_all_nan_gives_0(self):
        row = {k: np.nan for k in ['revenue', 'net_income', 'total_assets', 'total_equity',
                                    'operating_cash_flow', 'gross_profit', 'operating_income',
                                    'beneish_m_score', 'altman_z_score', 'piotroski_f_score',
                                    'sloan_accruals', 'entry_price', 'forward_return_1y',
                                    'net_margin', 'roe', 'roa', 'ocf_margin',
                                    'debt_to_equity', 'current_ratio']}
        df = pd.DataFrame([row])
        score = coverage_score(df)
        assert score.iloc[0] == pytest.approx(0.0)

    def test_half_present(self):
        row = _full_row()
        core_cols = ['revenue', 'net_income', 'total_assets', 'total_equity',
                     'operating_cash_flow', 'gross_profit', 'operating_income',
                     'beneish_m_score', 'altman_z_score', 'piotroski_f_score',
                     'sloan_accruals', 'entry_price', 'forward_return_1y',
                     'net_margin', 'roe', 'roa', 'ocf_margin',
                     'debt_to_equity', 'current_ratio']
        for c in core_cols[:10]:
            row[c] = np.nan
        df = pd.DataFrame([row])
        score = coverage_score(df)
        expected = 9.0 / 19.0
        assert score.iloc[0] == pytest.approx(expected, abs=0.01)

    def test_no_core_columns_returns_0_5(self):
        df = pd.DataFrame([{'ticker': 'X', 'fiscal_year': 2020}])
        score = coverage_score(df)
        assert score.iloc[0] == pytest.approx(0.5)


# ═══════════════════════════════════════════════════════════════════════════════
# Consistency Score
# ═══════════════════════════════════════════════════════════════════════════════

class TestConsistency:
    """Internal accounting consistency checks."""

    def test_all_consistent_gives_1(self):
        df = pd.DataFrame([_full_row()])
        score = consistency_score(df)
        assert score.iloc[0] == pytest.approx(1.0)

    def test_negative_assets_penalised(self):
        df = pd.DataFrame([_full_row(total_assets=-100)])
        score = consistency_score(df)
        assert score.iloc[0] < 1.0

    def test_equity_larger_than_assets_penalised(self):
        df = pd.DataFrame([_full_row(total_assets=100, total_equity=200)])
        score = consistency_score(df)
        assert score.iloc[0] < 1.0

    def test_negative_revenue_penalised(self):
        df = pd.DataFrame([_full_row(revenue=-1000)])
        score = consistency_score(df)
        assert score.iloc[0] < 1.0

    def test_loss_exceeds_3x_revenue_penalised(self):
        df = pd.DataFrame([_full_row(revenue=100, net_income=-400)])
        score = consistency_score(df)
        assert score.iloc[0] < 1.0

    def test_missing_ocf_with_revenue_penalised(self):
        df = pd.DataFrame([_full_row(operating_cash_flow=np.nan)])
        score = consistency_score(df)
        assert score.iloc[0] < 1.0

    def test_all_nan_returns_0_5(self):
        df = pd.DataFrame([{'ticker': 'X'}])
        score = consistency_score(df)
        assert score.iloc[0] == pytest.approx(0.5)

    def test_nan_inputs_not_penalised_individually(self):
        """NaN in total_equity should not trigger equity>assets check."""
        df = pd.DataFrame([_full_row(total_equity=np.nan)])
        score = consistency_score(df)
        # Only penalised on the missing OCF check if relevant
        assert score.iloc[0] >= 0.8


# ═══════════════════════════════════════════════════════════════════════════════
# Timeliness Score
# ═══════════════════════════════════════════════════════════════════════════════

class TestTimeliness:
    """Timeliness based on filing lag and fiscal year vintage."""

    def test_on_time_filing_modern_year(self):
        df = pd.DataFrame([{'filing_lag_days': 45, 'fiscal_year': 2020}])
        score = timeliness_score(df)
        assert score.iloc[0] == pytest.approx(1.0)

    def test_late_filing_penalised(self):
        df = pd.DataFrame([{'filing_lag_days': 200, 'fiscal_year': 2020}])
        score = timeliness_score(df)
        assert score.iloc[0] == pytest.approx(0.5)

    def test_mid_lag_linear_decay(self):
        df = pd.DataFrame([{'filing_lag_days': 120, 'fiscal_year': 2020}])
        score = timeliness_score(df)
        # lag=120: 1.0 - 0.3 * (120-60)/120 = 1.0 - 0.15 = 0.85
        assert score.iloc[0] == pytest.approx(0.85)

    def test_old_fiscal_year_penalised(self):
        df = pd.DataFrame([{'filing_lag_days': 45, 'fiscal_year': 2010}])
        score = timeliness_score(df)
        # fy < 2012 → vintage=0.7, lag OK → lag_score=1.0, product=0.7
        assert score.iloc[0] == pytest.approx(0.7)

    def test_negative_lag_not_penalised(self):
        df = pd.DataFrame([{'filing_lag_days': -30, 'fiscal_year': 2020}])
        score = timeliness_score(df)
        assert score.iloc[0] == pytest.approx(1.0)

    def test_no_columns_returns_1(self):
        df = pd.DataFrame([{'ticker': 'X'}])
        score = timeliness_score(df)
        assert score.iloc[0] == pytest.approx(1.0)

    def test_score_clipped_to_0_1(self):
        df = pd.DataFrame([{'filing_lag_days': 999, 'fiscal_year': 2005}])
        score = timeliness_score(df)
        assert 0.0 <= score.iloc[0] <= 1.0


# ═══════════════════════════════════════════════════════════════════════════════
# Composite: build_confidence
# ═══════════════════════════════════════════════════════════════════════════════

class TestBuildConfidence:
    """Integration: composite = (coverage + consistency + timeliness) / 3."""

    def test_composite_is_mean_of_three(self):
        df = pd.DataFrame([_full_row()])
        cov = coverage_score(df).iloc[0]
        cons = consistency_score(df).iloc[0]
        time_ = timeliness_score(df).iloc[0]
        expected = (cov + cons + time_) / 3.0
        actual = build_confidence(df).iloc[0]
        assert actual == pytest.approx(expected, abs=0.001)

    def test_idempotent(self):
        df = pd.DataFrame([_full_row()])
        s1 = build_confidence(df)
        s2 = build_confidence(df)
        pd.testing.assert_series_equal(s1, s2)

    def test_multiple_rows_independent(self):
        rows = [_full_row(), _full_row(revenue=np.nan, total_assets=np.nan)]
        df = pd.DataFrame(rows)
        scores = build_confidence(df)
        assert scores.iloc[0] > scores.iloc[1]

    def test_result_rounded_to_4_decimals(self):
        df = pd.DataFrame([_full_row()])
        score = build_confidence(df)
        val = score.iloc[0]
        assert val == round(val, 4)
