"""Unit tests for research/factor_research.py — IC/ICIR analysis orchestration."""
import numpy as np
import pandas as pd
import pytest

from research.factor_research import (
    get_candidates,
    compute_ic_series,
    compute_turnover,
    analyse_factor,
    _compute_quintile_spread,
    EXCLUDE,
    EXCLUDE_PATTERNS,
)


@pytest.fixture
def sample_df():
    """Synthetic DataFrame mimicking the annual dataset structure."""
    rng = np.random.default_rng(42)
    n_per_year = 60
    years = [2015, 2016, 2017, 2018, 2019]
    rows = []
    tickers = [f"T{i:03d}" for i in range(n_per_year)]
    for yr in years:
        feat = rng.normal(0, 1, n_per_year)
        ret = feat * 0.25 + rng.normal(0, 0.6, n_per_year)
        for i in range(n_per_year):
            rows.append({
                'ticker': tickers[i],
                'fiscal_year': yr,
                'good_signal': feat[i],
                'noise_signal': rng.normal(0, 1),
                'forward_return_1y': ret[i],
                'sic_code': rng.choice([2500, 4500, 6200]),
                'cik': 1000 + i,
                'name': f'Company {i}',
                'market': 'US',
            })
    df = pd.DataFrame(rows)
    df['sparse_col'] = np.nan
    df.loc[df.index[:5], 'sparse_col'] = 1.0
    return df


# ── get_candidates ───────────────────────────────────────────────────────────


class TestGetCandidates:
    def test_excludes_identifiers(self, sample_df):
        candidates = get_candidates(sample_df)
        for excl in ['cik', 'ticker', 'name']:
            assert excl not in candidates

    def test_excludes_forward_return_patterns(self, sample_df):
        candidates = get_candidates(sample_df)
        assert 'forward_return_1y' not in candidates

    def test_includes_numeric_features(self, sample_df):
        candidates = get_candidates(sample_df)
        assert 'good_signal' in candidates
        assert 'noise_signal' in candidates

    def test_excludes_sparse_columns(self, sample_df):
        candidates = get_candidates(sample_df)
        assert 'sparse_col' not in candidates

    def test_excludes_string_columns(self, sample_df):
        candidates = get_candidates(sample_df)
        assert 'market' not in candidates


# ── compute_ic_series ────────────────────────────────────────────────────────


class TestComputeICSeries:
    def test_returns_list(self, sample_df):
        ics = compute_ic_series(sample_df, 'good_signal', 'forward_return_1y')
        assert isinstance(ics, list)
        assert len(ics) > 0

    def test_correlated_signal_positive_ic(self, sample_df):
        ics = compute_ic_series(sample_df, 'good_signal', 'forward_return_1y')
        assert np.mean(ics) > 0.05

    def test_noise_signal_near_zero(self, sample_df):
        ics = compute_ic_series(sample_df, 'noise_signal', 'forward_return_1y')
        assert abs(np.mean(ics)) < 0.3

    def test_missing_column_raises(self, sample_df):
        with pytest.raises(KeyError):
            compute_ic_series(sample_df, 'nonexistent', 'forward_return_1y')


# ── compute_turnover ─────────────────────────────────────────────────────────


class TestComputeTurnover:
    def test_stable_feature_high_correlation(self, sample_df):
        turnover = compute_turnover(sample_df, 'good_signal')
        assert turnover is not None
        assert -1.0 <= turnover <= 1.0

    def test_raises_for_missing_column(self, sample_df):
        with pytest.raises(KeyError):
            compute_turnover(sample_df, 'nonexistent')

    def test_insufficient_overlap_returns_none(self):
        df = pd.DataFrame({
            'ticker': ['A', 'B', 'C'],
            'fiscal_year': [2018, 2019, 2020],
            'feat': [1.0, 2.0, 3.0],
        })
        turnover = compute_turnover(df, 'feat')
        assert turnover is None


# ── _compute_quintile_spread ─────────────────────────────────────────────────


class TestComputeQuintileSpread:
    def test_returns_dict_with_expected_keys(self, sample_df):
        result = _compute_quintile_spread(sample_df, 'good_signal', 'forward_return_1y')
        assert result is not None
        assert 'q1_ret' in result
        assert 'q5_ret' in result
        assert 'q_spread' in result

    def test_positive_signal_has_positive_spread(self, sample_df):
        result = _compute_quintile_spread(sample_df, 'good_signal', 'forward_return_1y')
        assert result is not None
        assert result['q_spread'] > 0

    def test_insufficient_data_returns_none(self):
        df = pd.DataFrame({
            'fiscal_year': [2020] * 10,
            'feat': range(10),
            'ret': range(10),
        })
        result = _compute_quintile_spread(df, 'feat', 'ret')
        assert result is None


# ── analyse_factor ───────────────────────────────────────────────────────────


class TestAnalyseFactor:
    def test_returns_complete_dict(self, sample_df):
        result = analyse_factor(sample_df, 'good_signal', 'forward_return_1y')
        assert result is not None
        expected_keys = [
            'feature', 'ic', 'mean_ic', 'std_ic', 'icir', 'ic_tstat',
            'pct_positive_ic', 'n_years', 'turnover', 'ic_min', 'ic_max',
        ]
        for key in expected_keys:
            assert key in result, f"Missing key: {key}"

    def test_good_signal_has_positive_icir(self, sample_df):
        result = analyse_factor(sample_df, 'good_signal', 'forward_return_1y')
        assert result['icir'] > 0

    def test_feature_name_in_result(self, sample_df):
        result = analyse_factor(sample_df, 'good_signal', 'forward_return_1y')
        assert result['feature'] == 'good_signal'

    def test_n_years_matches_data(self, sample_df):
        result = analyse_factor(sample_df, 'good_signal', 'forward_return_1y')
        assert result['n_years'] == 5

    def test_raises_for_missing_feature(self, sample_df):
        with pytest.raises(KeyError):
            analyse_factor(sample_df, 'nonexistent', 'forward_return_1y')

    def test_ic_min_less_than_max(self, sample_df):
        result = analyse_factor(sample_df, 'good_signal', 'forward_return_1y')
        assert result['ic_min'] <= result['ic_max']
