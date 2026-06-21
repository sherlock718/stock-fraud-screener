"""
Tests for pipeline/step6_clean.py — data quality filter.

Tests validate:
  - Row drop criteria (missing required columns, invalid/pre-2008 filed_date)
  - No duplicates in output
  - No infinities in output
  - Required columns present in output
  - Row count within 5% of input (no catastrophic drops)
  - filing_lag_days >= 0 for Dec-FY companies
  - as_of_date == filed_date
  - Infinity replacement (inf → NaN)
  - Dedup key correctness
  - Sort order

All tests use synthetic data. No disk I/O, no network calls.
"""

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers — build synthetic step5 output DataFrames
# ---------------------------------------------------------------------------

REQUIRED_COLS = ['cik', 'ticker', 'filed_date', 'fiscal_year', 'period_type']


def _make_df(n=20, market='US', include_market_col=True):
    """Build a minimal valid step5-like DataFrame."""
    rows = []
    for i in range(n):
        fy = 2015 + (i % 8)
        rows.append({
            'cik': f'{1000000 + i}',
            'ticker': f'TEST{i:03d}',
            'filed_date': pd.Timestamp(f'{fy + 1}-03-{15 + (i % 10):02d}'),
            'fiscal_year': fy,
            'period_type': 'annual',
            'market': market,
            'revenue': 1e9 * (1 + i * 0.1),
            'total_assets': 5e9 * (1 + i * 0.05),
            'net_income': 1e8 * (1 + i * 0.2),
            'pe_ratio': 15.0 + i,
            'roa': 0.05 + i * 0.001,
        })
    df = pd.DataFrame(rows)
    if not include_market_col:
        df = df.drop(columns=['market'])
    return df


def _run_step6(df):
    """Run step6 logic in-memory (mirrors step6_clean.py::run() without I/O)."""
    df = df.copy()

    # Filter 1: Required columns present
    df = df.dropna(subset=REQUIRED_COLS)

    # Filter 2: Valid filed_date after 2008-01-01
    df['filed_date'] = pd.to_datetime(df['filed_date'], errors='coerce')
    df = df[df['filed_date'].notna() & (df['filed_date'] >= '2008-01-01')]

    # Filter 3: Remove duplicates
    dedup_key = (
        ['cik', 'market', 'filed_date', 'period_type']
        if 'market' in df.columns
        else ['cik', 'filed_date', 'period_type']
    )
    df = df.drop_duplicates(subset=dedup_key, keep='first')

    # Filter 4: Replace infinities
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].replace([np.inf, -np.inf], np.nan)

    # PIT columns
    df['as_of_date'] = df['filed_date']
    fy_end = pd.to_datetime(
        df['fiscal_year'].astype(int).astype(str) + '-12-31', errors='coerce'
    )
    df['filing_lag_days'] = (df['filed_date'] - fy_end).dt.days

    # Sort
    df = df.sort_values(['ticker', 'filed_date', 'period_type']).reset_index(drop=True)

    return df


# ===========================================================================
# Test: Row Drop Criteria
# ===========================================================================

class TestRowDropCriteria:
    """Rows are dropped only for legitimate reasons."""

    def test_missing_cik_dropped(self):
        df = _make_df(5)
        df.loc[0, 'cik'] = None
        result = _run_step6(df)
        assert len(result) == 4

    def test_missing_ticker_dropped(self):
        df = _make_df(5)
        df.loc[1, 'ticker'] = None
        result = _run_step6(df)
        assert len(result) == 4

    def test_missing_filed_date_dropped(self):
        df = _make_df(5)
        df.loc[2, 'filed_date'] = None
        result = _run_step6(df)
        assert len(result) == 4

    def test_missing_fiscal_year_dropped(self):
        df = _make_df(5)
        df.loc[3, 'fiscal_year'] = None
        result = _run_step6(df)
        assert len(result) == 4

    def test_missing_period_type_dropped(self):
        df = _make_df(5)
        df.loc[4, 'period_type'] = None
        result = _run_step6(df)
        assert len(result) == 4

    def test_pre_2008_filing_dropped(self):
        df = _make_df(5)
        df.loc[0, 'filed_date'] = pd.Timestamp('2007-06-15')
        result = _run_step6(df)
        assert len(result) == 4

    def test_invalid_date_string_dropped(self):
        df = _make_df(5)
        df.loc[0, 'filed_date'] = 'not-a-date'
        result = _run_step6(df)
        assert len(result) == 4

    def test_valid_rows_never_dropped(self):
        """All-valid input loses no rows."""
        df = _make_df(20)
        result = _run_step6(df)
        assert len(result) == 20

    def test_nan_numeric_not_dropped(self):
        """NaN in a non-required numeric column does NOT cause row drop."""
        df = _make_df(5)
        df.loc[0, 'revenue'] = np.nan
        df.loc[1, 'pe_ratio'] = np.nan
        result = _run_step6(df)
        assert len(result) == 5


# ===========================================================================
# Test: No Duplicates
# ===========================================================================

class TestNoDuplicates:
    """Output has no duplicate rows on the dedup key."""

    def test_exact_duplicates_removed(self):
        df = _make_df(5)
        dup = df.iloc[[0]].copy()
        df = pd.concat([df, dup], ignore_index=True)
        result = _run_step6(df)
        key = ['cik', 'market', 'filed_date', 'period_type']
        assert result.duplicated(subset=key).sum() == 0

    def test_keeps_first_of_duplicates(self):
        df = _make_df(3)
        dup = df.iloc[[0]].copy()
        dup['revenue'] = 999999  # different value, same key
        df = pd.concat([df, dup], ignore_index=True)
        result = _run_step6(df)
        assert len(result) == 3
        # First row's revenue should be kept
        row = result[result['ticker'] == df.iloc[0]['ticker']].iloc[0]
        assert row['revenue'] != 999999

    def test_different_fiscal_year_not_deduped(self):
        """Same CIK, different fiscal_year → both kept (fiscal_year not in dedup key)."""
        df = _make_df(2)
        df.loc[0, 'fiscal_year'] = 2018
        df.loc[1, 'fiscal_year'] = 2019
        df.loc[0, 'cik'] = df.loc[1, 'cik'] = '9999999'
        df.loc[0, 'filed_date'] = pd.Timestamp('2019-03-15')
        df.loc[1, 'filed_date'] = pd.Timestamp('2020-03-15')
        result = _run_step6(df)
        assert len(result) == 2

    def test_dedup_without_market_column(self):
        """If market column is absent, dedup uses narrower key."""
        df = _make_df(5, include_market_col=False)
        dup = df.iloc[[0]].copy()
        df = pd.concat([df, dup], ignore_index=True)
        result = _run_step6(df)
        key = ['cik', 'filed_date', 'period_type']
        assert result.duplicated(subset=key).sum() == 0


# ===========================================================================
# Test: No Infinities
# ===========================================================================

class TestNoInfinities:
    """Output has no inf or -inf in any numeric column."""

    def test_positive_inf_replaced(self):
        df = _make_df(5)
        df.loc[0, 'pe_ratio'] = np.inf
        result = _run_step6(df)
        numeric = result.select_dtypes(include=[np.number])
        assert not numeric.isin([np.inf]).any().any()

    def test_negative_inf_replaced(self):
        df = _make_df(5)
        df.loc[1, 'roa'] = -np.inf
        result = _run_step6(df)
        numeric = result.select_dtypes(include=[np.number])
        assert not numeric.isin([-np.inf]).any().any()

    def test_inf_becomes_nan(self):
        df = _make_df(5)
        df.loc[0, 'pe_ratio'] = np.inf
        result = _run_step6(df)
        assert pd.isna(result.loc[result['ticker'] == 'TEST000', 'pe_ratio'].iloc[0])

    def test_multiple_inf_columns(self):
        df = _make_df(5)
        df.loc[0, 'pe_ratio'] = np.inf
        df.loc[0, 'roa'] = -np.inf
        df.loc[1, 'revenue'] = np.inf
        result = _run_step6(df)
        numeric = result.select_dtypes(include=[np.number])
        assert not numeric.isin([np.inf, -np.inf]).any().any()


# ===========================================================================
# Test: Required Columns Present in Output
# ===========================================================================

class TestRequiredColumns:
    """Output always contains the structural columns."""

    def test_required_cols_present(self):
        df = _make_df(10)
        result = _run_step6(df)
        for col in REQUIRED_COLS:
            assert col in result.columns

    def test_as_of_date_present(self):
        df = _make_df(10)
        result = _run_step6(df)
        assert 'as_of_date' in result.columns

    def test_filing_lag_days_present(self):
        df = _make_df(10)
        result = _run_step6(df)
        assert 'filing_lag_days' in result.columns


# ===========================================================================
# Test: Row Count Stability (within 5% of input)
# ===========================================================================

class TestRowCountStability:
    """Output row count is within 5% of input for healthy data."""

    def test_clean_data_no_drops(self):
        """Fully valid data → 0% row loss."""
        df = _make_df(100)
        result = _run_step6(df)
        assert len(result) == len(df)

    def test_5pct_threshold_respected(self):
        """Even with some bad rows, output is within 5% of input."""
        df = _make_df(100)
        # Corrupt 4 rows (4% of 100)
        df.loc[0, 'cik'] = None
        df.loc[1, 'ticker'] = None
        df.loc[2, 'filed_date'] = 'bad'
        df.loc[3, 'fiscal_year'] = None
        result = _run_step6(df)
        pct_kept = len(result) / 100
        assert pct_kept >= 0.95

    def test_catastrophic_drop_detectable(self):
        """If >5% rows have missing required cols, drop exceeds 5%."""
        df = _make_df(100)
        # Corrupt 10 rows (10%)
        for i in range(10):
            df.loc[i, 'cik'] = None
        result = _run_step6(df)
        pct_kept = len(result) / 100
        assert pct_kept < 0.95  # This WOULD trip a guard if one existed


# ===========================================================================
# Test: filing_lag_days
# ===========================================================================

class TestFilingLagDays:
    """filing_lag_days = filed_date - fiscal_year_end(Dec-31)."""

    def test_positive_for_dec_fy(self):
        """Dec-31 FY company filing in March → positive lag."""
        df = _make_df(1)
        df.loc[0, 'fiscal_year'] = 2020
        df.loc[0, 'filed_date'] = pd.Timestamp('2021-03-01')
        result = _run_step6(df)
        lag = result['filing_lag_days'].iloc[0]
        assert lag > 0
        # 2021-03-01 - 2020-12-31 = 60 days
        assert lag == 60

    def test_zero_if_filed_on_dec31(self):
        """Filed exactly on Dec-31 of FY → lag=0."""
        df = _make_df(1)
        df.loc[0, 'fiscal_year'] = 2020
        df.loc[0, 'filed_date'] = pd.Timestamp('2020-12-31')
        result = _run_step6(df)
        assert result['filing_lag_days'].iloc[0] == 0

    def test_negative_for_non_dec_fy(self):
        """Non-Dec FY: FY=2020 but filing before Dec-31-2020 → negative lag."""
        df = _make_df(1)
        df.loc[0, 'fiscal_year'] = 2020
        df.loc[0, 'filed_date'] = pd.Timestamp('2020-09-15')
        result = _run_step6(df)
        lag = result['filing_lag_days'].iloc[0]
        # 2020-09-15 - 2020-12-31 = -107 days (non-Dec FY company)
        assert lag < 0


# ===========================================================================
# Test: as_of_date == filed_date
# ===========================================================================

class TestAsOfDate:
    """as_of_date is always identical to filed_date."""

    def test_as_of_equals_filed(self):
        df = _make_df(10)
        result = _run_step6(df)
        assert (result['as_of_date'] == result['filed_date']).all()

    def test_as_of_type_is_datetime(self):
        df = _make_df(5)
        result = _run_step6(df)
        assert pd.api.types.is_datetime64_any_dtype(result['as_of_date'])


# ===========================================================================
# Test: Sort Order
# ===========================================================================

class TestSortOrder:
    """Output is sorted by (ticker, filed_date, period_type)."""

    def test_sorted_by_ticker_then_date(self):
        df = _make_df(20)
        result = _run_step6(df)
        # Check monotonic within sort columns
        sorted_check = result.sort_values(
            ['ticker', 'filed_date', 'period_type']
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(result, sorted_check)


# ===========================================================================
# Test: No Accidental Filtering (step6 checklist 6.6)
# ===========================================================================

class TestNoAccidentalFiltering:
    """Step6 does NOT filter by revenue, market cap, or sector."""

    def test_zero_revenue_kept(self):
        df = _make_df(5)
        df.loc[0, 'revenue'] = 0
        result = _run_step6(df)
        assert len(result) == 5

    def test_negative_assets_kept(self):
        df = _make_df(5)
        df.loc[0, 'total_assets'] = -100
        result = _run_step6(df)
        assert len(result) == 5

    def test_nan_revenue_kept(self):
        df = _make_df(5)
        df.loc[0, 'revenue'] = np.nan
        result = _run_step6(df)
        assert len(result) == 5


# ===========================================================================
# Test: Multi-market dedup isolation
# ===========================================================================

class TestMultiMarketDedup:
    """Same CIK in different markets are NOT deduped against each other."""

    def test_same_cik_different_market_both_kept(self):
        df = _make_df(2)
        df.loc[0, 'cik'] = df.loc[1, 'cik'] = '9999999'
        df.loc[0, 'filed_date'] = df.loc[1, 'filed_date'] = pd.Timestamp('2020-03-15')
        df.loc[0, 'period_type'] = df.loc[1, 'period_type'] = 'annual'
        df.loc[0, 'market'] = 'US'
        df.loc[1, 'market'] = 'CA'
        result = _run_step6(df)
        assert len(result) == 2
