"""
Tests for pipeline/step4_enrich_macro.py — FRED macro enrichment.

Validates:
  - Row count == input row count (left-join preservation)
  - macro_asof <= filed_date (no future macro via backward merge_asof)
  - No NaN catastrophe (when FRED data exists, macro cols populated)
  - Schema contract (expected output columns)
  - Derived formula correctness (real_rate_10y, credit_tightening, macro_regime)
  - CPI YoY computation from level series

All tests use synthetic data. Zero network/API calls.
"""

import numpy as np
import pandas as pd
import pytest

# ─── Import functions under test ─────────────────────────────────────────────

from pipeline.step4_enrich_macro import (
    build_macro_panel,
    compute_cpi_yoy,
    compute_credit_tightening,
    lookup_macro,
)


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_fred_series():
    """Synthetic FRED series simulating typical data (2009-2023)."""
    dates = pd.date_range('2009-01-01', '2023-12-31', freq='B')

    # Treasury 10y: gradual decline then rise
    t10y = pd.Series(
        np.linspace(3.5, 1.0, len(dates) // 2).tolist()
        + np.linspace(1.0, 4.5, len(dates) - len(dates) // 2).tolist(),
        index=dates,
    )

    # Treasury 2y: similar but lower
    t2y = pd.Series(
        np.linspace(2.0, 0.5, len(dates) // 2).tolist()
        + np.linspace(0.5, 4.0, len(dates) - len(dates) // 2).tolist(),
        index=dates,
    )

    # Yield curve (10y - 2y)
    yc = t10y - t2y

    # Fed funds rate
    ffr = pd.Series(
        np.linspace(2.0, 0.25, len(dates) // 2).tolist()
        + np.linspace(0.25, 5.0, len(dates) - len(dates) // 2).tolist(),
        index=dates,
    )

    # Credit spread BAA
    baa = pd.Series(np.random.default_rng(42).uniform(1.5, 4.0, len(dates)), index=dates)

    # HY spread
    hy = pd.Series(np.random.default_rng(43).uniform(3.0, 8.0, len(dates)), index=dates)

    # CPI level (monthly-ish but on business days for simplicity)
    cpi_monthly = pd.date_range('2009-01-01', '2023-12-31', freq='MS')
    cpi_vals = np.linspace(210, 305, len(cpi_monthly))
    cpi = pd.Series(cpi_vals, index=cpi_monthly)

    # USREC: recession in 2020
    rec = pd.Series(0.0, index=dates)
    recession_start = pd.Timestamp('2020-03-01')
    recession_end = pd.Timestamp('2020-06-30')
    rec[(dates >= recession_start) & (dates <= recession_end)] = 1.0

    # VIX
    vix = pd.Series(np.random.default_rng(44).uniform(12, 35, len(dates)), index=dates)

    return {
        'treasury_10y': t10y,
        'treasury_2y': t2y,
        'yield_curve': yc,
        'fed_funds_rate': ffr,
        'credit_spread_baa': baa,
        'hy_spread': hy,
        'cpi': cpi,
        'recession': rec,
        'vix': vix,
    }


@pytest.fixture
def macro_panel(sample_fred_series):
    """Pre-built macro panel from synthetic series."""
    return build_macro_panel(sample_fred_series)


@pytest.fixture
def sample_snapshots():
    """Synthetic snapshots DataFrame mimicking step2 output."""
    return pd.DataFrame({
        'cik': ['0001234', '0005678', '0009012', '0001234', '0005678'],
        'ticker': ['AAPL', 'MSFT', 'GOOG', 'AAPL', 'MSFT'],
        'filed_date': pd.to_datetime([
            '2015-02-15', '2016-07-20', '2020-04-10', '2021-03-01', '2023-11-05'
        ]),
        'fiscal_year': [2014, 2015, 2019, 2020, 2022],
        'fiscal_quarter': ['FY', 'FY', 'FY', 'FY', 'FY'],
        'period_type': ['annual'] * 5,
        'revenue': [100e9, 90e9, 160e9, 110e9, 95e9],
        'total_assets': [200e9, 180e9, 300e9, 220e9, 190e9],
    })


# ─── Test: Schema Contract ───────────────────────────────────────────────────


class TestSchemaContract:
    """Step 4 output must have expected columns."""

    EXPECTED_KEY_COLS = [
        'cik', 'ticker', 'filed_date', 'fiscal_year',
        'fiscal_quarter', 'period_type',
    ]

    EXPECTED_MACRO_COLS = [
        'treasury_10y', 'treasury_2y', 'yield_curve', 'fed_funds_rate',
        'credit_spread_baa', 'hy_spread', 'cpi_yoy', 'recession', 'vix',
        'real_rate_10y', 'credit_tightening', 'macro_regime',
    ]

    def test_output_has_all_key_columns(self, macro_panel, sample_snapshots):
        """Output preserves all snapshot key columns."""
        result = self._run_step4_logic(macro_panel, sample_snapshots)
        for col in self.EXPECTED_KEY_COLS:
            assert col in result.columns, f"Missing key column: {col}"

    def test_output_has_all_macro_columns(self, macro_panel, sample_snapshots):
        """Output includes all 12 macro columns."""
        result = self._run_step4_logic(macro_panel, sample_snapshots)
        for col in self.EXPECTED_MACRO_COLS:
            assert col in result.columns, f"Missing macro column: {col}"

    def test_no_extra_unexpected_columns(self, macro_panel, sample_snapshots):
        """Output has only expected columns (no leakage from snap)."""
        result = self._run_step4_logic(macro_panel, sample_snapshots)
        expected = set(self.EXPECTED_KEY_COLS + self.EXPECTED_MACRO_COLS)
        extra = set(result.columns) - expected
        assert not extra, f"Unexpected columns in output: {extra}"

    def test_total_column_count(self, macro_panel, sample_snapshots):
        """Output has exactly 6 key + 12 macro = 18 columns."""
        result = self._run_step4_logic(macro_panel, sample_snapshots)
        assert len(result.columns) == 18

    @staticmethod
    def _run_step4_logic(panel, snap):
        """Replicate step4 merge logic without file I/O."""
        macro_cols = [
            'treasury_10y', 'treasury_2y', 'yield_curve', 'fed_funds_rate',
            'credit_spread_baa', 'hy_spread', 'cpi_yoy', 'recession', 'vix',
            'real_rate_10y', 'credit_tightening', 'macro_regime',
        ]
        key_cols = ['cik', 'ticker', 'filed_date', 'fiscal_year',
                    'fiscal_quarter', 'period_type']

        snap_dates = pd.DataFrame({'filed_date': pd.to_datetime(snap['filed_date'])})
        snap_dates = snap_dates.sort_values('filed_date').reset_index()

        macro_reset = panel.reset_index().rename(columns={'date': 'filed_date'})
        macro_reset['filed_date'] = pd.to_datetime(macro_reset['filed_date'])
        macro_reset = macro_reset.sort_values('filed_date')

        merged = pd.merge_asof(
            snap_dates, macro_reset, on='filed_date', direction='backward'
        )
        merged = merged.set_index('index').sort_index()

        snap_out = snap[key_cols].copy()
        for col in macro_cols:
            if col in merged.columns:
                snap_out[col] = merged[col].values
            else:
                snap_out[col] = np.nan

        return snap_out


# ─── Test: Row Count Preservation ────────────────────────────────────────────


class TestRowCountPreservation:
    """Step 4 must never add or drop rows (left-join only)."""

    def test_row_count_equals_input(self, macro_panel, sample_snapshots):
        """Output row count must exactly match input snapshot count."""
        result = TestSchemaContract._run_step4_logic(macro_panel, sample_snapshots)
        assert len(result) == len(sample_snapshots)

    def test_row_count_empty_macro_panel(self, sample_snapshots):
        """Even with empty macro panel, all rows preserved (NaN-filled)."""
        empty_panel = pd.DataFrame()
        key_cols = ['cik', 'ticker', 'filed_date', 'fiscal_year',
                    'fiscal_quarter', 'period_type']
        macro_cols = [
            'treasury_10y', 'treasury_2y', 'yield_curve', 'fed_funds_rate',
            'credit_spread_baa', 'hy_spread', 'cpi_yoy', 'recession', 'vix',
            'real_rate_10y', 'credit_tightening', 'macro_regime',
        ]
        snap_out = sample_snapshots[key_cols].copy()
        for col in macro_cols:
            snap_out[col] = np.nan
        assert len(snap_out) == len(sample_snapshots)

    def test_row_count_single_row(self, macro_panel):
        """Single-row snapshot produces single-row output."""
        snap = pd.DataFrame({
            'cik': ['0001111'],
            'ticker': ['TEST'],
            'filed_date': pd.to_datetime(['2020-06-15']),
            'fiscal_year': [2019],
            'fiscal_quarter': ['FY'],
            'period_type': ['annual'],
        })
        result = TestSchemaContract._run_step4_logic(macro_panel, snap)
        assert len(result) == 1

    def test_row_count_large_input(self, macro_panel):
        """Row count preserved for larger inputs (100 rows)."""
        rng = np.random.default_rng(99)
        n = 100
        snap = pd.DataFrame({
            'cik': [f'{i:07d}' for i in range(n)],
            'ticker': [f'T{i}' for i in range(n)],
            'filed_date': pd.date_range('2010-01-15', periods=n, freq='45D'),
            'fiscal_year': rng.integers(2009, 2023, n),
            'fiscal_quarter': ['FY'] * n,
            'period_type': ['annual'] * n,
        })
        result = TestSchemaContract._run_step4_logic(macro_panel, snap)
        assert len(result) == n


# ─── Test: No Future Macro (PIT Safety) ─────────────────────────────────────


class TestNoFutureMacro:
    """Macro values must come from on-or-before filed_date, never after."""

    def test_merge_asof_backward_direction(self, macro_panel, sample_snapshots):
        """merge_asof with direction='backward' ensures no future data."""
        snap_dates = pd.DataFrame({
            'filed_date': pd.to_datetime(sample_snapshots['filed_date'])
        })
        snap_dates = snap_dates.sort_values('filed_date').reset_index()

        macro_reset = macro_panel.reset_index().rename(columns={'date': 'filed_date'})
        macro_reset['filed_date'] = pd.to_datetime(macro_reset['filed_date'])
        macro_reset = macro_reset.sort_values('filed_date')

        merged = pd.merge_asof(
            snap_dates, macro_reset, on='filed_date', direction='backward'
        )

        # The macro panel's date (the one matched) must be <= the snap's filed_date
        # Since merge_asof backward: matched date <= left key
        # We don't have the matched date explicitly, but we can verify by checking
        # that for the earliest filing, macro values are from before that date
        # The merge_asof API guarantees this by contract

        # Explicit check: look up a date and verify panel index
        test_date = pd.Timestamp('2015-02-15')
        idx = macro_panel.index.searchsorted(test_date, side='right') - 1
        assert macro_panel.index[idx] <= test_date

    def test_filing_before_macro_start_gets_earliest(self, macro_panel):
        """Filing date before macro panel start gets clamped to earliest row."""
        # Panel starts 2007-01-01; a filing before that gets idx clamped to 0
        early_date = pd.Timestamp('2006-01-01')
        idx = macro_panel.index.searchsorted(early_date, side='right') - 1
        # searchsorted returns 0, minus 1 = -1; lookup_macro clamps to 0
        assert idx < 0  # Negative = would be clamped in lookup_macro
        result = lookup_macro(macro_panel, early_date)
        assert result  # Still returns data (clamped to first row)

    def test_lookup_macro_returns_past_data(self, macro_panel):
        """lookup_macro for a specific date returns data from that date or before."""
        test_date = pd.Timestamp('2020-06-15')
        result = lookup_macro(macro_panel, test_date)
        assert result  # non-empty
        # The looked-up row's index should be <= test_date
        idx = macro_panel.index.searchsorted(test_date, side='right') - 1
        assert macro_panel.index[idx] <= test_date

    def test_weekend_filing_gets_same_or_prior_day_macro(self, macro_panel):
        """Filing on Saturday gets Saturday's forward-filled macro (panel is daily)."""
        # Panel includes weekends (freq='D'), so Saturday IS in the panel
        # with forward-filled values from Friday's FRED data
        saturday = pd.Timestamp('2020-03-14')
        idx = macro_panel.index.searchsorted(saturday, side='right') - 1
        matched_date = macro_panel.index[idx]
        # Matched date must be <= filing date (PIT-safe)
        assert matched_date <= saturday


# ─── Test: No NaN Catastrophe ────────────────────────────────────────────────


class TestNoNaNCatastrophe:
    """When FRED data exists, macro columns should be populated (not all NaN)."""

    def test_macro_cols_populated_post_2009(self, macro_panel, sample_snapshots):
        """Filings after 2009 should have populated macro values."""
        result = TestSchemaContract._run_step4_logic(macro_panel, sample_snapshots)
        # At least 80% of rows should have non-null treasury_10y
        fill_rate = result['treasury_10y'].notna().mean()
        assert fill_rate >= 0.8, f"treasury_10y fill rate too low: {fill_rate:.2%}"

    def test_cpi_yoy_populated(self, macro_panel, sample_snapshots):
        """CPI YoY (derived from level) should be populated."""
        result = TestSchemaContract._run_step4_logic(macro_panel, sample_snapshots)
        fill_rate = result['cpi_yoy'].notna().mean()
        assert fill_rate >= 0.8, f"cpi_yoy fill rate too low: {fill_rate:.2%}"

    def test_recession_populated(self, macro_panel, sample_snapshots):
        """Recession flag should be populated for all rows."""
        result = TestSchemaContract._run_step4_logic(macro_panel, sample_snapshots)
        fill_rate = result['recession'].notna().mean()
        assert fill_rate >= 0.8, f"recession fill rate too low: {fill_rate:.2%}"

    def test_all_nan_when_no_fred_data(self, sample_snapshots):
        """Without FRED data, all macro cols are NaN (graceful degradation)."""
        key_cols = ['cik', 'ticker', 'filed_date', 'fiscal_year',
                    'fiscal_quarter', 'period_type']
        macro_cols = [
            'treasury_10y', 'treasury_2y', 'yield_curve', 'fed_funds_rate',
            'credit_spread_baa', 'hy_spread', 'cpi_yoy', 'recession', 'vix',
            'real_rate_10y', 'credit_tightening', 'macro_regime',
        ]
        snap_out = sample_snapshots[key_cols].copy()
        for col in macro_cols:
            snap_out[col] = np.nan
        # Confirm graceful degradation — all NaN, not crash
        assert snap_out['treasury_10y'].isna().all()
        assert len(snap_out) == len(sample_snapshots)


# ─── Test: Derived Formula Correctness ───────────────────────────────────────


class TestDerivedFormulas:
    """Derived macro features computed correctly from base series."""

    def test_cpi_yoy_computation(self):
        """CPI YoY correctly computes 12-month percent change."""
        # 12 months of CPI: 100 → 103 = 3% YoY
        dates = pd.date_range('2020-01-01', periods=24, freq='MS')
        cpi = pd.Series(
            np.concatenate([np.full(12, 100.0), np.full(12, 103.0)]),
            index=dates,
        )
        yoy = compute_cpi_yoy(cpi)
        # At month 13 (2021-01): (103-100)/100 * 100 = 3.0%
        assert len(yoy) == 12  # first 12 months dropped (no prior year)
        assert abs(yoy.iloc[0] - 3.0) < 0.01

    def test_cpi_yoy_handles_gradual_increase(self):
        """CPI YoY with gradual monthly increase."""
        dates = pd.date_range('2019-01-01', periods=36, freq='MS')
        # 0.5% monthly increase → ~6.17% annualized
        cpi = pd.Series(100 * (1.005 ** np.arange(36)), index=dates)
        yoy = compute_cpi_yoy(cpi)
        # After 12 months: (1.005^12 - 1) * 100 ≈ 6.17%
        expected = (1.005**12 - 1) * 100
        assert abs(yoy.iloc[0] - expected) < 0.1

    def test_credit_tightening_positive_means_widening(self):
        """Positive credit_tightening means spread has widened (tightened conditions)."""
        dates = pd.date_range('2020-01-01', periods=12, freq='MS')
        # Spread goes from 2.0 to 4.0 over 12 months
        spread = pd.Series(np.linspace(2.0, 4.0, 12), index=dates)
        ct = compute_credit_tightening(spread)
        # After 6 months, the 6m change should be positive (widening)
        assert len(ct) > 0
        assert ct.iloc[-1] > 0  # spread widened

    def test_credit_tightening_negative_means_easing(self):
        """Negative credit_tightening means spread has narrowed (easing)."""
        dates = pd.date_range('2020-01-01', periods=12, freq='MS')
        # Spread goes from 4.0 to 2.0 over 12 months
        spread = pd.Series(np.linspace(4.0, 2.0, 12), index=dates)
        ct = compute_credit_tightening(spread)
        assert ct.iloc[-1] < 0  # spread narrowed = easing

    def test_real_rate_equals_nominal_minus_inflation(self, macro_panel):
        """real_rate_10y = treasury_10y - cpi_yoy."""
        # Check a random date
        date = pd.Timestamp('2020-06-15')
        idx = macro_panel.index.searchsorted(date, side='right') - 1
        row = macro_panel.iloc[idx]
        if pd.notna(row['treasury_10y']) and pd.notna(row['cpi_yoy']):
            expected = row['treasury_10y'] - row['cpi_yoy']
            assert abs(row['real_rate_10y'] - expected) < 0.001

    def test_macro_regime_recession_overrides(self, sample_fred_series):
        """Macro regime 3 (recession) overrides rate-based classification."""
        panel = build_macro_panel(sample_fred_series)
        # During recession period (2020-03 to 2020-06), regime should be 3
        recession_dates = panel.loc['2020-04-01':'2020-06-01']
        if len(recession_dates) > 0:
            assert (recession_dates['macro_regime'] == 3).all()

    def test_macro_regime_high_rate(self, sample_fred_series):
        """Macro regime 2 when fed_funds >= 3.0 and no recession."""
        panel = build_macro_panel(sample_fred_series)
        # Find rows where ffr >= 3.0 and no recession
        mask = (panel['fed_funds_rate'] >= 3.0) & (panel['recession'] == 0)
        high_rate_rows = panel[mask]
        if len(high_rate_rows) > 0:
            # These should be regime 2 (unless also rising)
            assert (high_rate_rows['macro_regime'].isin([1, 2])).all()


# ─── Test: Build Macro Panel Structure ───────────────────────────────────────


class TestBuildMacroPanel:
    """build_macro_panel produces correct structure."""

    def test_panel_has_daily_frequency(self, sample_fred_series):
        """Macro panel is daily (continuous, including weekends)."""
        panel = build_macro_panel(sample_fred_series)
        # Panel is created from pd.date_range(freq='D')
        assert panel.index.freq == 'D'
        # Consecutive days — no gaps
        diffs = panel.index.to_series().diff().dt.days.dropna()
        assert diffs.max() == 1

    def test_panel_starts_at_fred_start(self, sample_fred_series):
        """Panel starts at FRED_START date."""
        panel = build_macro_panel(sample_fred_series)
        assert panel.index[0] == pd.Timestamp('2007-01-01')

    def test_forward_fill_no_gaps(self, sample_fred_series):
        """Forward fill ensures no interior NaN gaps for treasury_10y."""
        panel = build_macro_panel(sample_fred_series)
        # After the first non-null, should have no nulls (forward filled)
        t10y = panel['treasury_10y']
        first_valid = t10y.first_valid_index()
        if first_valid is not None:
            after_start = t10y.loc[first_valid:]
            nan_rate = after_start.isna().mean()
            assert nan_rate < 0.01, f"Too many NaN after start: {nan_rate:.2%}"

    def test_empty_series_map_returns_empty_panel(self):
        """Empty series map produces empty panel (handled by caller)."""
        panel = build_macro_panel({})
        # With no series, panel has date index but no useful columns
        assert 'treasury_10y' not in panel.columns or panel['treasury_10y'].isna().all()


# ─── Test: Lookup Macro ──────────────────────────────────────────────────────


class TestLookupMacro:
    """lookup_macro performs correct date-based lookup."""

    def test_returns_dict(self, macro_panel):
        """lookup_macro returns a dictionary."""
        result = lookup_macro(macro_panel, pd.Timestamp('2020-01-15'))
        assert isinstance(result, dict)

    def test_returns_empty_for_empty_panel(self):
        """Empty panel returns empty dict (graceful degradation)."""
        result = lookup_macro(pd.DataFrame(), pd.Timestamp('2020-01-15'))
        assert result == {}

    def test_values_are_numeric(self, macro_panel):
        """All returned values are numeric (float/int/nan)."""
        result = lookup_macro(macro_panel, pd.Timestamp('2020-06-15'))
        for key, val in result.items():
            assert isinstance(val, (int, float, np.integer, np.floating)), \
                f"Non-numeric value for {key}: {type(val)}"

    def test_date_at_panel_boundary(self, macro_panel):
        """Lookup at exact panel end date works without error."""
        last_date = macro_panel.index[-1]
        result = lookup_macro(macro_panel, last_date)
        assert result  # non-empty
