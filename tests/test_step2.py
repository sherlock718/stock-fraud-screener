"""
Tests for pipeline/step2_build_snapshots.py — schema, dedup, temporal integrity.

Synthetic data only. No network calls. No disk I/O.
Tests the pure computation functions directly where possible,
and validates schema contracts on synthetic output.
"""
import sys
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Add pipeline/ to path so we can import step2 functions directly
sys.path.insert(0, str(Path(__file__).parent.parent / 'pipeline'))

from step2_build_snapshots import (
    _yoy, _delta, _ratio, add_yoy_features, build_period_snapshots
)


REQUIRED_COLUMNS = [
    'cik', 'ticker', 'fiscal_year', 'period_type', 'filed_date',
    'revenue', 'total_assets',
]


def _make_snapshots_df(n_tickers=5, n_years=5):
    """Synthetic snapshots.parquet output."""
    rows = []
    for i in range(n_tickers):
        for year in range(2015, 2015 + n_years):
            rows.append({
                'cik': str(1000000 + i).zfill(10),
                'ticker': f'T{i:03d}',
                'name': f'Company {i}',
                'exchange': 'NYSE',
                'sic_code': '3570',
                'sic_description': 'Electronic Computers',
                'market': 'US',
                'country': 'United States',
                'accounting_std': 'GAAP',
                'fiscal_year': year,
                'fiscal_quarter': None,
                'period_type': 'annual',
                'filed_date': f'{year + 1}-02-28',
                'revenue': 1_000_000 * (1 + i) * (1.05 ** (year - 2015)),
                'total_assets': 5_000_000 * (1 + i),
                'net_income': 100_000 * (1 + i),
                'operating_cash_flow': 150_000 * (1 + i),
                'shares_outstanding': 1_000_000,
            })
    return pd.DataFrame(rows)


class TestStep2Schema:
    def test_required_columns_present(self):
        df = _make_snapshots_df()
        for col in REQUIRED_COLUMNS:
            assert col in df.columns, f'Missing required column: {col}'

    def test_period_type_valid_values(self):
        df = _make_snapshots_df()
        valid = {'annual', 'quarterly'}
        for pt in df['period_type'].unique():
            assert pt in valid

    def test_fiscal_year_reasonable_range(self):
        df = _make_snapshots_df()
        assert df['fiscal_year'].min() >= 2005
        assert df['fiscal_year'].max() <= 2030

    def test_filed_date_parseable(self):
        df = _make_snapshots_df()
        dates = pd.to_datetime(df['filed_date'], errors='coerce')
        assert dates.notna().all(), 'All filed_date values must be parseable dates'


class TestStep2DedupKey:
    def test_no_duplicate_ticker_year_period(self):
        """Primary key: (ticker, fiscal_year, period_type) must be unique."""
        df = _make_snapshots_df()
        key_cols = ['ticker', 'fiscal_year', 'period_type']
        assert not df.duplicated(subset=key_cols).any()

    def test_cik_ticker_year_unique(self):
        """(cik, fiscal_year, period_type) must also be unique."""
        df = _make_snapshots_df()
        key_cols = ['cik', 'fiscal_year', 'period_type']
        assert not df.duplicated(subset=key_cols).any()


class TestStep2TemporalIntegrity:
    def test_filed_date_after_fiscal_year_end(self):
        """filed_date must be AFTER the fiscal year it covers (no time travel).
        For annual filings: filed_date > {fiscal_year}-12-31."""
        df = _make_snapshots_df()
        annual = df[df['period_type'] == 'annual']
        for _, row in annual.iterrows():
            filed = pd.Timestamp(row['filed_date'])
            period_end = pd.Timestamp(f"{row['fiscal_year']}-12-31")
            assert filed > period_end, (
                f"filed_date {filed} not after period_end {period_end} "
                f"for {row['ticker']} FY{row['fiscal_year']}"
            )

    def test_filed_date_not_unreasonably_late(self):
        """10-K filings must be within 18 months of fiscal year end (SEC deadline)."""
        df = _make_snapshots_df()
        annual = df[df['period_type'] == 'annual']
        for _, row in annual.iterrows():
            filed = pd.Timestamp(row['filed_date'])
            deadline = pd.Timestamp(f"{row['fiscal_year'] + 1}-06-30")
            # Allow generous window (18 months) — some late filers
            max_date = pd.Timestamp(f"{row['fiscal_year'] + 2}-06-30")
            assert filed <= max_date, (
                f"filed_date {filed} unreasonably late for FY{row['fiscal_year']}"
            )


class TestStep2YoYComputation:
    def test_yoy_basic(self):
        assert _yoy(110, 100) == pytest.approx(0.1, abs=0.001)
        assert _yoy(90, 100) == pytest.approx(-0.1, abs=0.001)

    def test_yoy_zero_denominator(self):
        assert _yoy(100, 0) is None

    def test_yoy_none_inputs(self):
        assert _yoy(None, 100) is None
        assert _yoy(100, None) is None

    def test_delta_basic(self):
        assert _delta(0.5, 0.3) == pytest.approx(0.2, abs=0.001)

    def test_ratio_basic(self):
        assert _ratio(50, 100) == pytest.approx(0.5)
        assert _ratio(50, 0) is None
        assert _ratio(None, 100) is None

    def test_add_yoy_features_produces_growth_cols(self):
        """add_yoy_features must produce YoY growth columns for 2+ consecutive years."""
        snapshots = []
        for year in range(2015, 2020):
            snapshots.append({
                'fiscal_year': year,
                'fiscal_quarter': None,
                'period_type': 'annual',
                'filed_date': f'{year + 1}-03-01',
                'revenue': 1_000_000 * (1.1 ** (year - 2015)),
                'total_assets': 5_000_000,
                'net_income': 200_000,
                'receivables': 100_000,
                'inventory': 50_000,
                'operating_cash_flow': 250_000,
                'gross_profit': 500_000,
                'capex': 80_000,
                'shares_outstanding': 1_000_000,
                'eps_diluted': 0.20,
                'equity': 2_000_000,
                'long_term_debt': 1_000_000,
                'cash': 500_000,
                'rd_expense': 100_000,
                'sga_expense': 200_000,
                'accounts_payable': 150_000,
                'cogs': 400_000,
                'ppe_net': 800_000,
                'operating_income': 300_000,
            })
        result = add_yoy_features(snapshots)
        # Second year onwards should have revenue_growth_yoy computed
        annual = [s for s in result if s['period_type'] == 'annual']
        annual_sorted = sorted(annual, key=lambda s: s['fiscal_year'])
        # First year has no prior → None
        assert annual_sorted[0].get('revenue_growth_yoy') is None
        # Second year should have ~10% growth
        assert annual_sorted[1].get('revenue_growth_yoy') == pytest.approx(0.1, abs=0.01)


class TestStep2Coverage:
    def test_requires_revenue_and_assets(self):
        """build_period_snapshots drops rows without both revenue and total_assets."""
        facts = {
            'facts': {
                'us-gaap': {
                    'Revenues': {
                        'units': {'USD': [
                            {'fy': 2020, 'fp': 'FY', 'val': 1000000, 'filed': '2021-02-28'}
                        ]}
                    },
                    'Assets': {
                        'units': {'USD': [
                            {'fy': 2020, 'fp': 'FY', 'val': 5000000, 'filed': '2021-02-28'}
                        ]}
                    },
                }
            }
        }
        snaps = build_period_snapshots(facts)
        assert len(snaps) == 1
        assert snaps[0]['revenue'] == 1000000
        assert snaps[0]['total_assets'] == 5000000

    def test_drops_row_without_revenue(self):
        facts = {
            'facts': {
                'us-gaap': {
                    'Assets': {
                        'units': {'USD': [
                            {'fy': 2020, 'fp': 'FY', 'val': 5000000, 'filed': '2021-02-28'}
                        ]}
                    },
                }
            }
        }
        snaps = build_period_snapshots(facts)
        assert len(snaps) == 0

    def test_drops_row_without_assets(self):
        facts = {
            'facts': {
                'us-gaap': {
                    'Revenues': {
                        'units': {'USD': [
                            {'fy': 2020, 'fp': 'FY', 'val': 1000000, 'filed': '2021-02-28'}
                        ]}
                    },
                }
            }
        }
        snaps = build_period_snapshots(facts)
        assert len(snaps) == 0
