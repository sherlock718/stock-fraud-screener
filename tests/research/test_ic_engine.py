"""Unit tests for research/ic_engine.py — IC/ICIR computation engine."""
import numpy as np
import pandas as pd
import pytest

from research.ic_engine import (
    bh_fdr_correction,
    compute_yearly_ic,
    newey_west_tstat,
    _sic_to_sector,
)


# ── newey_west_tstat ─────────────────────────────────────────────────────────


class TestNeweyWestTstat:
    def test_constant_series_returns_high_tstat(self):
        s = pd.Series([0.1] * 10)
        t = newey_west_tstat(s)
        assert t > 5.0

    def test_zero_mean_returns_zero(self):
        s = pd.Series([0.1, -0.1, 0.1, -0.1, 0.1, -0.1])
        t = newey_west_tstat(s)
        assert abs(t) < 1.0

    def test_short_series_returns_nan(self):
        s = pd.Series([0.1, 0.2])
        t = newey_west_tstat(s)
        assert np.isnan(t)

    def test_handles_nan_values(self):
        s = pd.Series([0.1, np.nan, 0.12, 0.09, np.nan, 0.11, 0.10])
        t = newey_west_tstat(s)
        assert np.isfinite(t)
        assert t > 0

    def test_positive_mean_gives_positive_tstat(self):
        rng = np.random.default_rng(42)
        s = pd.Series(rng.normal(0.05, 0.02, 20))
        t = newey_west_tstat(s)
        assert t > 0

    def test_negative_mean_gives_negative_tstat(self):
        rng = np.random.default_rng(42)
        s = pd.Series(rng.normal(-0.05, 0.02, 20))
        t = newey_west_tstat(s)
        assert t < 0

    def test_max_lags_param(self):
        rng = np.random.default_rng(7)
        s = pd.Series(rng.normal(0.05, 0.03, 15))
        t1 = newey_west_tstat(s, max_lags=1)
        t4 = newey_west_tstat(s, max_lags=4)
        assert np.isfinite(t1)
        assert np.isfinite(t4)


# ── bh_fdr_correction ────────────────────────────────────────────────────────


class TestBHFdrCorrection:
    def test_all_significant(self):
        pvals = pd.Series([0.001, 0.002, 0.003])
        result = bh_fdr_correction(pvals, alpha=0.05)
        assert result.all()

    def test_none_significant(self):
        pvals = pd.Series([0.5, 0.6, 0.7, 0.8])
        result = bh_fdr_correction(pvals, alpha=0.05)
        assert not result.any()

    def test_partial_rejection(self):
        pvals = pd.Series([0.001, 0.01, 0.5, 0.9])
        result = bh_fdr_correction(pvals, alpha=0.05)
        assert result.iloc[0] is True or result.iloc[0] == True
        assert result.iloc[3] is False or result.iloc[3] == False

    def test_empty_series(self):
        pvals = pd.Series(dtype=float)
        result = bh_fdr_correction(pvals, alpha=0.05)
        assert len(result) == 0

    def test_preserves_index(self):
        pvals = pd.Series([0.001, 0.9, 0.002], index=['a', 'b', 'c'])
        result = bh_fdr_correction(pvals, alpha=0.05)
        assert list(result.index) == ['a', 'b', 'c']

    def test_strict_alpha(self):
        pvals = pd.Series([0.04, 0.06, 0.08])
        result_strict = bh_fdr_correction(pvals, alpha=0.01)
        result_loose = bh_fdr_correction(pvals, alpha=0.10)
        assert result_strict.sum() <= result_loose.sum()


# ── _sic_to_sector ───────────────────────────────────────────────────────────


class TestSicToSector:
    def test_manufacturing(self):
        s = pd.Series([2000, 3500, 3999])
        result = _sic_to_sector(s)
        assert (result == "Manufacturing").all()

    def test_finance(self):
        s = pd.Series([6000, 6500])
        result = _sic_to_sector(s)
        assert (result == "Finance/Insurance/RE").all()

    def test_other_for_unknown(self):
        s = pd.Series([0, 9999])
        result = _sic_to_sector(s)
        assert (result == "Other").all()

    def test_handles_nan(self):
        s = pd.Series([np.nan, 2500, None])
        result = _sic_to_sector(s)
        assert result.iloc[0] == "Other"
        assert result.iloc[1] == "Manufacturing"

    def test_handles_string_codes(self):
        s = pd.Series(["4500", "7500"])
        result = _sic_to_sector(s)
        assert result.iloc[0] == "Utilities/Transport"
        assert result.iloc[1] == "Services/Hospitality"


# ── compute_yearly_ic ────────────────────────────────────────────────────────


class TestComputeYearlyIC:
    @pytest.fixture
    def synthetic_df(self):
        rng = np.random.default_rng(123)
        n_per_year = 50
        years = [2018, 2019, 2020, 2021]
        rows = []
        for yr in years:
            feat = rng.normal(0, 1, n_per_year)
            ret = feat * 0.3 + rng.normal(0, 0.5, n_per_year)
            sic = rng.choice([2500, 4500, 6200], n_per_year)
            for i in range(n_per_year):
                rows.append({
                    'fiscal_year': yr,
                    'signal': feat[i],
                    'forward_return_1y': ret[i],
                    'sic_code': sic[i],
                })
        return pd.DataFrame(rows)

    def test_returns_series_indexed_by_year(self, synthetic_df):
        ic = compute_yearly_ic(synthetic_df, 'signal', 'forward_return_1y',
                               sector_neutral=False)
        assert isinstance(ic, pd.Series)
        assert len(ic) == 4
        assert set(ic.index) == {2018, 2019, 2020, 2021}

    def test_positive_ic_for_correlated_signal(self, synthetic_df):
        ic = compute_yearly_ic(synthetic_df, 'signal', 'forward_return_1y',
                               sector_neutral=False)
        assert ic.mean() > 0.1

    def test_sector_neutral_still_positive(self, synthetic_df):
        ic = compute_yearly_ic(synthetic_df, 'signal', 'forward_return_1y',
                               sector_neutral=True)
        assert ic.mean() > 0

    def test_min_obs_filter(self, synthetic_df):
        ic = compute_yearly_ic(synthetic_df, 'signal', 'forward_return_1y',
                               sector_neutral=False, min_obs=100)
        assert len(ic) == 0

    def test_handles_nan_in_feature(self, synthetic_df):
        df = synthetic_df.copy()
        df.loc[df.index[:10], 'signal'] = np.nan
        ic = compute_yearly_ic(df, 'signal', 'forward_return_1y',
                               sector_neutral=False)
        assert len(ic) > 0
        assert ic.notna().all()

    def test_sic_col_override(self, synthetic_df):
        df = synthetic_df.copy()
        df['my_sic'] = df['sic_code']
        ic = compute_yearly_ic(df, 'signal', 'forward_return_1y',
                               sector_neutral=True, sic_col_override='my_sic')
        assert len(ic) == 4

    def test_random_signal_near_zero_ic(self):
        rng = np.random.default_rng(999)
        n = 200
        df = pd.DataFrame({
            'fiscal_year': np.repeat([2018, 2019, 2020, 2021], 50),
            'noise': rng.normal(0, 1, n),
            'forward_return_1y': rng.normal(0, 1, n),
        })
        ic = compute_yearly_ic(df, 'noise', 'forward_return_1y',
                               sector_neutral=False)
        assert abs(ic.mean()) < 0.3
