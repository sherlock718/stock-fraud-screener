"""Unit tests for portfolio/ modules — scoring, sizing, constraints, screener utilities."""
import numpy as np
import pandas as pd
import pytest

from portfolio.build_portfolio import (
    compute_composite,
    kelly_weights,
    apply_constraints,
)
from portfolio.build_screener_registry import (
    _rank_blend,
    _quality_gate,
)


# ── compute_composite ────────────────────────────────────────────────────────


class TestComputeComposite:
    @pytest.fixture
    def df_with_signals(self):
        return pd.DataFrame({
            'alpha_value': [0.8, 0.6, 0.4, 0.2, np.nan],
            'alpha_quality': [0.2, 0.4, 0.6, 0.8, 0.5],
            'alpha_momentum': [0.5, 0.5, 0.5, 0.5, 0.5],
        })

    def test_returns_series(self, df_with_signals):
        result = compute_composite(
            df_with_signals,
            ['alpha_value', 'alpha_quality'],
            {'alpha_value': 0.6, 'alpha_quality': 0.4},
        )
        assert isinstance(result, pd.Series)
        assert len(result) == 5

    def test_equal_weights_gives_balanced_scores(self):
        df = pd.DataFrame({
            'sig_a': [1.0, 2.0, 3.0, 4.0, 5.0],
            'sig_b': [5.0, 4.0, 3.0, 2.0, 1.0],
        })
        result = compute_composite(df, ['sig_a', 'sig_b'],
                                   {'sig_a': 0.5, 'sig_b': 0.5})
        assert result.std() < 0.01

    def test_single_signal_equals_rank_pct(self):
        df = pd.DataFrame({'sig': [10.0, 20.0, 30.0, 40.0, 50.0]})
        result = compute_composite(df, ['sig'], {'sig': 1.0})
        expected = df['sig'].rank(pct=True)
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_handles_missing_column_gracefully(self):
        df = pd.DataFrame({'sig_a': [1.0, 2.0, 3.0]})
        result = compute_composite(df, ['sig_a', 'nonexistent'],
                                   {'sig_a': 0.5, 'nonexistent': 0.5})
        assert result.isna().all()

    def test_output_bounded_zero_one(self, df_with_signals):
        result = compute_composite(
            df_with_signals,
            ['alpha_value', 'alpha_quality', 'alpha_momentum'],
            {'alpha_value': 0.4, 'alpha_quality': 0.3, 'alpha_momentum': 0.3},
        )
        valid = result[result.notna()]
        assert valid.min() >= 0.0
        assert valid.max() <= 1.0


# ── kelly_weights ────────────────────────────────────────────────────────────


class TestKellyWeights:
    def test_sums_to_one(self):
        scores = pd.Series([0.7, 0.8, 0.9, 0.6])
        w = kelly_weights(scores, fraction=0.25)
        assert abs(w.sum() - 1.0) < 1e-10

    def test_higher_score_gets_higher_weight(self):
        scores = pd.Series([0.6, 0.9], index=['low', 'high'])
        w = kelly_weights(scores, fraction=0.25)
        assert w['high'] > w['low']

    def test_all_below_half_gives_equal_weight(self):
        scores = pd.Series([0.3, 0.4, 0.2])
        w = kelly_weights(scores, fraction=0.25)
        assert abs(w.iloc[0] - w.iloc[1]) < 1e-10
        assert abs(w.sum() - 1.0) < 1e-10

    def test_fraction_scaling(self):
        scores = pd.Series([0.7, 0.8, 0.9])
        w1 = kelly_weights(scores, fraction=0.25)
        w2 = kelly_weights(scores, fraction=0.50)
        assert abs(w1.sum() - 1.0) < 1e-10
        assert abs(w2.sum() - 1.0) < 1e-10

    def test_single_stock(self):
        scores = pd.Series([0.8])
        w = kelly_weights(scores, fraction=0.25)
        assert abs(w.iloc[0] - 1.0) < 1e-10


# ── apply_constraints ────────────────────────────────────────────────────────


class TestApplyConstraints:
    def test_position_cap_clips_before_renorm(self):
        weights = pd.Series([0.5, 0.3, 0.2])
        df_slice = pd.DataFrame({'sic_code': [2500, 4500, 6200]}, index=weights.index)
        result = apply_constraints(weights, df_slice, position_cap=0.10, sector_cap=1.0)
        # After clipping to 0.10 each and renorming, all equal
        assert abs(result.iloc[0] - result.iloc[1]) < 1e-10
        assert abs(result.sum() - 1.0) < 1e-10

    def test_large_position_reduced_relative(self):
        weights = pd.Series([0.6, 0.2, 0.1, 0.1])
        df_slice = pd.DataFrame({'sic_code': [2500, 4500, 6200, 7500]}, index=weights.index)
        result = apply_constraints(weights, df_slice, position_cap=0.30, sector_cap=1.0)
        # Position 0 was 0.6, capped to 0.3 then renormed — must be less than original
        assert result.iloc[0] < 0.6

    def test_sector_cap_reduces_concentrated_sector(self):
        weights = pd.Series([0.3, 0.3, 0.2, 0.2])
        df_slice = pd.DataFrame({'sic_code': [2500, 2500, 4500, 4500]}, index=weights.index)
        result = apply_constraints(weights, df_slice, position_cap=1.0, sector_cap=0.40)
        # After sector cap + renorm, the sector-2500 pair should have less relative weight
        # than their original 60%
        sector_2500_weight = result.iloc[0] + result.iloc[1]
        assert sector_2500_weight < 0.60

    def test_sums_to_one_after_constraints(self):
        weights = pd.Series([0.4, 0.3, 0.2, 0.1])
        df_slice = pd.DataFrame({'sic_code': [2500, 2500, 4500, 6200]}, index=weights.index)
        result = apply_constraints(weights, df_slice, position_cap=0.15, sector_cap=0.50)
        assert abs(result.sum() - 1.0) < 1e-10

    def test_no_sic_column_still_works(self):
        weights = pd.Series([0.5, 0.3, 0.2])
        df_slice = pd.DataFrame({'other': [1, 2, 3]}, index=weights.index)
        result = apply_constraints(weights, df_slice, position_cap=0.30, sector_cap=0.50)
        assert abs(result.sum() - 1.0) < 1e-10


# ── _rank_blend (screener_registry) ──────────────────────────────────────────


class TestRankBlend:
    def test_returns_series(self):
        df = pd.DataFrame({
            'alpha_composite': [0.8, 0.6, 0.4],
            'value_composite': [0.3, 0.5, 0.7],
        })
        result = _rank_blend(df, [('alpha_composite', 0.6), ('value_composite', 0.4)])
        assert isinstance(result, pd.Series)
        assert len(result) == 3

    def test_handles_missing_columns(self):
        df = pd.DataFrame({'alpha_composite': [0.8, 0.6, 0.4]})
        result = _rank_blend(df, [('alpha_composite', 0.6), ('nonexistent', 0.4)])
        assert result.notna().all()

    def test_monotonic_with_single_signal(self):
        df = pd.DataFrame({'sig': [1.0, 2.0, 3.0, 4.0, 5.0]})
        result = _rank_blend(df, [('sig', 1.0)])
        assert (result.diff().dropna() >= 0).all()

    def test_bounded_zero_one(self):
        df = pd.DataFrame({
            'a': np.random.default_rng(1).normal(0, 1, 100),
            'b': np.random.default_rng(2).normal(0, 1, 100),
        })
        result = _rank_blend(df, [('a', 0.5), ('b', 0.5)])
        assert result.min() >= 0.0
        assert result.max() <= 1.0


# ── _quality_gate (screener_registry) ────────────────────────────────────────


class TestQualityGate:
    @pytest.fixture
    def gate_df(self):
        return pd.DataFrame({
            'piotroski_f_score': [8, 7, 5, 3, 9, 6],
            'beneish_m_score': [-2.5, -2.0, -1.5, -3.0, -2.2, -1.9],
            'likely_delisted': [0, 0, 0, 0, 1, 0],
        })

    def test_removes_low_piotroski(self, gate_df):
        result = _quality_gate(gate_df, min_fscore=6)
        assert (result['piotroski_f_score'] >= 6).all()

    def test_removes_high_beneish(self, gate_df):
        result = _quality_gate(gate_df, min_fscore=3)
        assert (result['beneish_m_score'] < -1.78).all()

    def test_future_derived_delisted_annotation_is_not_a_historical_gate(self, gate_df):
        changed = gate_df.copy()
        changed["likely_delisted"] = 1 - changed["likely_delisted"]
        original = _quality_gate(gate_df, min_fscore=3)
        reclassified = _quality_gate(changed, min_fscore=3)
        assert original.index.tolist() == reclassified.index.tolist()

    def test_combined_filters_reduce_rows(self, gate_df):
        result = _quality_gate(gate_df, min_fscore=7)
        assert len(result) < len(gate_df)

    def test_no_columns_returns_only_piotroski_filter(self):
        df = pd.DataFrame({'piotroski_f_score': [8, 3, 7, 2]})
        result = _quality_gate(df, min_fscore=6)
        assert len(result) == 2
        assert (result['piotroski_f_score'] >= 6).all()
