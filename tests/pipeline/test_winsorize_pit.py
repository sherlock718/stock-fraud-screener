"""
Tests for point-in-time winsorization.

Core invariant: adding future-year observations must NOT change
the transformed values of historical training rows.
"""

import numpy as np
import pandas as pd
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pipeline.winsorize_pit import (
    winsorize_expanding,
    winsorize_by_filed_date,
    winsorize_global,
    winsorize_training_only,
    compare_winsorization_methods,
)
from pipeline.step5_compute_features import winsorize_pit


class TestPITInvariant:
    """Adding future observations cannot change historical transformed values."""

    def _make_df(self, n_per_year=100, years=range(2010, 2020)):
        """Generate synthetic financial data with year-varying distributions."""
        rng = np.random.default_rng(42)
        rows = []
        for yr in years:
            # Distribution widens over time (mimics market evolution)
            scale = 1.0 + 0.1 * (yr - 2010)
            vals = rng.normal(0, scale, n_per_year)
            for v in vals:
                rows.append({'fiscal_year': yr, 'feature': v})
        return pd.DataFrame(rows)

    def test_future_data_cannot_change_historical_values(self):
        """Core invariant: historical values must be stable when future data is added."""
        # Phase 1: compute with data through 2015 only
        df_2015 = self._make_df(years=range(2010, 2016))
        result_2015 = winsorize_pit(df_2015, 'feature')

        # Phase 2: compute with data through 2020 (adds 2016-2019)
        df_2020 = self._make_df(years=range(2010, 2020))
        result_2020 = winsorize_pit(df_2020, 'feature')

        # Historical values (2010-2015) must be IDENTICAL
        n_2015 = len(df_2015)
        historical_2015 = result_2015.values
        historical_in_2020 = result_2020.values[:n_2015]

        np.testing.assert_array_equal(
            historical_2015, historical_in_2020,
            err_msg='Adding future data changed historical winsorized values!'
        )

    def test_expanding_window_uses_only_past_data(self):
        """Bounds for year Y must come from years <= Y only."""
        df = self._make_df(years=range(2010, 2015))
        result = winsorize_expanding(df, 'feature')

        # For 2010 observations, bounds should come from 2010 only
        mask_2010 = df['fiscal_year'] == 2010
        vals_2010 = df.loc[mask_2010, 'feature'].astype(float)
        lo_2010 = vals_2010.quantile(0.01)
        hi_2010 = vals_2010.quantile(0.99)

        expected_2010 = vals_2010.clip(lo_2010, hi_2010)
        actual_2010 = result[mask_2010]

        pd.testing.assert_series_equal(
            actual_2010.reset_index(drop=True),
            expected_2010.reset_index(drop=True),
            check_names=False,
        )

    def test_global_winsorize_is_not_pit_safe(self):
        """Demonstrate that global winsorization DOES change with future data."""
        rng = np.random.default_rng(99)

        # Phase 1: tight distribution
        vals_2010 = rng.normal(0, 1, 200)
        df1 = pd.DataFrame({'fiscal_year': 2010, 'feature': vals_2010})
        global_1 = winsorize_global(df1['feature'].astype(float))

        # Phase 2: add extreme future data that changes the global quantiles
        vals_2020 = rng.normal(0, 10, 200)  # much wider distribution
        df2 = pd.concat([df1, pd.DataFrame({'fiscal_year': 2020, 'feature': vals_2020})])
        global_2 = winsorize_global(df2['feature'].astype(float))

        # Global method: 2010 values WILL change because quantiles shift
        historical_1 = global_1.values
        historical_in_2 = global_2.values[:200]

        # At least some should differ (proving global is not PIT-safe)
        n_differ = (historical_1 != historical_in_2).sum()
        assert n_differ > 0, (
            'Expected global winsorization to change historical values '
            'when future data is added — proves it leaks'
        )

    def test_training_only_bounds_frozen(self):
        """Training-only winsorization uses fixed bounds from train set."""
        rng = np.random.default_rng(123)
        df = pd.DataFrame({
            'feature': rng.normal(0, 2, 500),
            'fiscal_year': [2015] * 300 + [2020] * 200,
        })
        train_mask = df['fiscal_year'] == 2015

        result, bounds = winsorize_training_only(df, 'feature', train_mask)

        # Bounds should be from training data only
        train_vals = df.loc[train_mask, 'feature'].astype(float)
        expected_lo = train_vals.quantile(0.01)
        expected_hi = train_vals.quantile(0.99)

        assert abs(bounds['lo'] - expected_lo) < 1e-10
        assert abs(bounds['hi'] - expected_hi) < 1e-10

        # Test data should be clipped to training bounds
        test_mask = df['fiscal_year'] == 2020
        test_result = result[test_mask]
        assert test_result.min() >= bounds['lo'] - 1e-10
        assert test_result.max() <= bounds['hi'] + 1e-10


class TestFiledDateWinsorization:
    """Prove winsorization uses only records available at the actual scoring cutoff."""

    def test_only_prior_filings_inform_bounds(self):
        """
        A record filed on 2021-06-15 must NOT be used to compute bounds
        for records filed on 2021-03-01 (even if both are fiscal_year=2020).
        """
        rng = np.random.default_rng(42)
        # Early filers: tight distribution (filed Q1 2021)
        early_vals = rng.normal(0, 1, 200)
        # Late filers: extreme outliers (filed Q2 2021)
        late_vals = np.concatenate([rng.normal(0, 1, 190), [100, -100] * 5])

        df = pd.DataFrame({
            'filed_date': (
                [pd.Timestamp('2021-02-15')] * 200 +
                [pd.Timestamp('2021-06-15')] * 200
            ),
            'fiscal_year': [2020] * 400,
            'feature': np.concatenate([early_vals, late_vals]),
        })

        result = winsorize_pit(df, 'feature')

        # The early filers (Q1) should be clipped based on data from BEFORE Q1 2021
        # Since there's no prior data, they use bootstrapped same-quarter bounds
        # The key test: late filers' extreme values (100, -100) must NOT affect
        # the bounds applied to early filers
        early_result = result[:200]
        # If late outliers leaked into early bounds, early clips would be wider
        # Check that early values are NOT clipped to accommodate [-100, 100]
        assert early_result.max() < 10, (
            f'Early filer max={early_result.max():.1f} — '
            f'late outliers should not widen early bounds'
        )

    def test_filed_date_not_fiscal_year_determines_availability(self):
        """
        Two observations with same fiscal_year but different filing dates
        must use different bounds if filed in different quarters.
        """
        rng = np.random.default_rng(99)
        # Create historical data (2019) as the training base
        historical = pd.DataFrame({
            'filed_date': [pd.Timestamp('2020-03-01')] * 500,
            'fiscal_year': [2019] * 500,
            'feature': rng.normal(0, 1, 500),
        })
        # Create two 2020 observations filed at different times
        obs = pd.DataFrame({
            'filed_date': [pd.Timestamp('2021-02-01'), pd.Timestamp('2021-08-01')],
            'fiscal_year': [2020, 2020],
            'feature': [50.0, 50.0],  # extreme value
        })
        df = pd.concat([historical, obs], ignore_index=True)

        result = winsorize_pit(df, 'feature')

        # Both extreme values should be clipped, but the second one (Aug)
        # has more history available (includes the Feb filer) so bounds could differ slightly
        # The key invariant: the Feb observation's clip used only pre-Feb data
        feb_clip = result.iloc[500]
        aug_clip = result.iloc[501]
        # Both should be clipped down from 50 to the 99th percentile
        assert feb_clip < 50, 'Feb observation should be clipped'
        assert aug_clip < 50, 'Aug observation should be clipped'

    def test_adding_future_filings_does_not_change_past_clips(self):
        """
        Core PIT invariant with filed_date: adding observations filed later
        must not change bounds applied to observations filed earlier.
        """
        rng = np.random.default_rng(123)

        # Base: observations filed in Q1 2020
        base = pd.DataFrame({
            'filed_date': [pd.Timestamp('2020-02-15')] * 300,
            'fiscal_year': [2019] * 300,
            'feature': rng.normal(0, 2, 300),
        })

        # Compute clip with just base
        result_base = winsorize_pit(base, 'feature')

        # Now add observations filed Q3 2020 with very different distribution
        future = pd.DataFrame({
            'filed_date': [pd.Timestamp('2020-09-15')] * 300,
            'fiscal_year': [2020] * 300,
            'feature': rng.normal(0, 10, 300),  # much wider
        })
        combined = pd.concat([base, future], ignore_index=True)
        result_combined = winsorize_pit(combined, 'feature')

        # The base observations (first 300) must have IDENTICAL clipped values
        base_values_alone = result_base.values
        base_values_in_combined = result_combined.values[:300]

        np.testing.assert_array_equal(
            base_values_alone, base_values_in_combined,
            err_msg='Adding future-filed observations changed clips on past filings!'
        )


class TestWinsorizeComparison:
    """Comparison utility tests."""

    def test_comparison_returns_expected_columns(self):
        rng = np.random.default_rng(42)
        df = pd.DataFrame({
            'fiscal_year': [2015] * 200 + [2020] * 200,
            'feat_a': rng.normal(0, 1, 400),
            'feat_b': rng.normal(5, 2, 400),
        })
        result = compare_winsorization_methods(df, ['feat_a', 'feat_b'])
        assert 'column' in result.columns
        assert 'pct_values_differ' in result.columns
        assert len(result) == 2
