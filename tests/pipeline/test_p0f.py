"""Tests for pipeline/p0f_universe_definition.py — classify_universe filter logic."""
from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from pipeline.p0f_universe_definition import classify_universe, MARKET_MIN_PRICE


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _make_row(**overrides) -> dict:
    """Standard investable row (passes all filters)."""
    base = {
        'ticker': 'AAPL',
        'market': 'US',
        'period_type': 'annual',
        'fiscal_year': 2020,
        'revenue': 5_000_000.0,
        'total_assets': 1_000_000.0,
        'entry_price': 50.0,
        'sic_code': 3674,
        'exchange': 'NASDAQ',
    }
    base.update(overrides)
    return base


def _classify(rows: list[dict], apply_filters: bool = False) -> pd.DataFrame:
    """Convenience: build DF, mock year to 2026, classify."""
    df = pd.DataFrame(rows)
    with patch('pipeline.p0f_universe_definition._get_current_year', return_value=2026):
        return classify_universe(df, apply_filters=apply_filters)


# ═══════════════════════════════════════════════════════════════════════════════
# Structural Rules (always applied)
# ═══════════════════════════════════════════════════════════════════════════════

class TestStructuralRules:
    """Tests for rules applied regardless of --apply-filters."""

    def test_annual_row_passes(self):
        result = _classify([_make_row(period_type='annual')])
        assert result['in_universe'].iloc[0] == 1

    def test_quarterly_row_excluded(self):
        result = _classify([_make_row(period_type='quarterly')])
        assert result['in_universe'].iloc[0] == 0
        assert 'quarterly_row' in result['excl_reason'].iloc[0]

    def test_10q_row_excluded(self):
        result = _classify([_make_row(period_type='10-Q')])
        assert result['in_universe'].iloc[0] == 0

    def test_fiscal_year_before_2009_excluded(self):
        result = _classify([_make_row(fiscal_year=2008)])
        assert result['in_universe'].iloc[0] == 0
        assert 'fy<2009' in result['excl_reason'].iloc[0]

    def test_fiscal_year_2009_passes(self):
        result = _classify([_make_row(fiscal_year=2009)])
        assert result['in_universe'].iloc[0] == 1

    def test_current_year_excluded(self):
        # Current year mocked to 2026, so max_fy = 2025. FY 2026 should be excluded.
        result = _classify([_make_row(fiscal_year=2026)])
        assert result['in_universe'].iloc[0] == 0
        assert 'incomplete' in result['excl_reason'].iloc[0]

    def test_prior_year_passes(self):
        # max_fy = 2025 → fiscal_year 2025 should pass
        result = _classify([_make_row(fiscal_year=2025)])
        assert result['in_universe'].iloc[0] == 1

    def test_structural_rules_ignore_revenue(self):
        """Without --apply-filters, low revenue doesn't matter."""
        result = _classify([_make_row(revenue=100.0)])
        assert result['in_universe'].iloc[0] == 1

    def test_structural_rules_ignore_sic(self):
        """Without --apply-filters, financial sector SIC passes."""
        result = _classify([_make_row(sic_code=6020)])
        assert result['in_universe'].iloc[0] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Investable-Universe Filters (--apply-filters)
# ═══════════════════════════════════════════════════════════════════════════════

class TestInvestableFilters:
    """Tests for rules only applied with apply_filters=True."""

    def test_good_row_passes_all_filters(self):
        result = _classify([_make_row()], apply_filters=True)
        assert result['in_universe'].iloc[0] == 1
        assert result['excl_reason'].iloc[0] == ''

    # ── Revenue / Assets ──

    def test_revenue_below_1m_excluded(self):
        result = _classify([_make_row(revenue=999_999.0)], apply_filters=True)
        assert result['in_universe'].iloc[0] == 0
        assert 'revenue<1M' in result['excl_reason'].iloc[0]

    def test_revenue_exactly_1m_passes(self):
        result = _classify([_make_row(revenue=1_000_000.0)], apply_filters=True)
        assert 'revenue' not in result['excl_reason'].iloc[0]

    def test_assets_below_100k_excluded(self):
        result = _classify([_make_row(total_assets=99_999.0)], apply_filters=True)
        assert result['in_universe'].iloc[0] == 0
        assert 'assets<100K' in result['excl_reason'].iloc[0]

    def test_assets_exactly_100k_passes(self):
        result = _classify([_make_row(total_assets=100_000.0)], apply_filters=True)
        assert 'assets' not in result['excl_reason'].iloc[0]

    # ── Fail-open: missing revenue/assets ──

    def test_nan_revenue_passes_fail_open(self):
        result = _classify([_make_row(revenue=np.nan)], apply_filters=True)
        assert 'revenue' not in result['excl_reason'].iloc[0]

    def test_nan_assets_passes_fail_open(self):
        result = _classify([_make_row(total_assets=np.nan)], apply_filters=True)
        assert 'assets' not in result['excl_reason'].iloc[0]

    # ── Fail-closed: missing price ──

    def test_nan_price_excluded_fail_closed(self):
        result = _classify([_make_row(entry_price=np.nan)], apply_filters=True)
        assert result['in_universe'].iloc[0] == 0
        assert 'no_price' in result['excl_reason'].iloc[0]

    def test_zero_price_excluded(self):
        result = _classify([_make_row(entry_price=0.0)], apply_filters=True)
        assert result['in_universe'].iloc[0] == 0
        assert 'no_price' in result['excl_reason'].iloc[0]

    def test_negative_price_excluded(self):
        result = _classify([_make_row(entry_price=-1.0)], apply_filters=True)
        assert result['in_universe'].iloc[0] == 0

    # ── Price floor per market ──

    def test_us_price_below_1_excluded(self):
        result = _classify([_make_row(market='US', entry_price=0.50)], apply_filters=True)
        assert result['in_universe'].iloc[0] == 0
        assert 'price_below_market_floor' in result['excl_reason'].iloc[0]

    def test_us_price_at_1_passes(self):
        result = _classify([_make_row(market='US', entry_price=1.00)], apply_filters=True)
        assert 'price_below_market_floor' not in result['excl_reason'].iloc[0]

    def test_ca_price_below_005_excluded(self):
        result = _classify([_make_row(market='CA', entry_price=0.03)], apply_filters=True)
        assert 'price_below_market_floor' in result['excl_reason'].iloc[0]

    def test_jp_no_price_floor(self):
        """JP market has 0.0 floor — any positive price passes."""
        result = _classify([_make_row(market='JP', entry_price=0.01)], apply_filters=True)
        assert 'price_below_market_floor' not in result['excl_reason'].iloc[0]

    def test_br_no_price_floor(self):
        result = _classify([_make_row(market='BR', entry_price=0.01)], apply_filters=True)
        assert 'price_below_market_floor' not in result['excl_reason'].iloc[0]

    # ── SIC exclusions ──

    def test_financial_sector_excluded(self):
        result = _classify([_make_row(sic_code=6020)], apply_filters=True)
        assert result['in_universe'].iloc[0] == 0
        assert 'financial_sector' in result['excl_reason'].iloc[0]

    def test_financial_sector_boundary_6000(self):
        result = _classify([_make_row(sic_code=6000)], apply_filters=True)
        assert 'financial_sector' in result['excl_reason'].iloc[0]

    def test_financial_sector_boundary_6999(self):
        result = _classify([_make_row(sic_code=6999)], apply_filters=True)
        assert 'financial_sector' in result['excl_reason'].iloc[0]

    def test_sic_5999_not_financial(self):
        result = _classify([_make_row(sic_code=5999)], apply_filters=True)
        assert 'financial_sector' not in result['excl_reason'].iloc[0]

    def test_utility_sector_excluded(self):
        result = _classify([_make_row(sic_code=4911)], apply_filters=True)
        assert result['in_universe'].iloc[0] == 0
        assert 'utility_sector' in result['excl_reason'].iloc[0]

    def test_utility_sector_boundary_4900(self):
        result = _classify([_make_row(sic_code=4900)], apply_filters=True)
        assert 'utility_sector' in result['excl_reason'].iloc[0]

    def test_utility_sector_boundary_4999(self):
        result = _classify([_make_row(sic_code=4999)], apply_filters=True)
        assert 'utility_sector' in result['excl_reason'].iloc[0]

    def test_sic_4899_not_utility(self):
        result = _classify([_make_row(sic_code=4899)], apply_filters=True)
        assert 'utility_sector' not in result['excl_reason'].iloc[0]

    # ── Fail-open: missing SIC ──

    def test_nan_sic_passes_fail_open(self):
        result = _classify([_make_row(sic_code=np.nan)], apply_filters=True)
        assert 'financial_sector' not in result['excl_reason'].iloc[0]
        assert 'utility_sector' not in result['excl_reason'].iloc[0]


# ═══════════════════════════════════════════════════════════════════════════════
# in_universe Column Semantics
# ═══════════════════════════════════════════════════════════════════════════════

class TestInUniverseSemantics:
    """Verify the meaning and behavior of in_universe and excl_reason columns."""

    def test_in_universe_dtype_int8(self):
        result = _classify([_make_row()])
        assert result['in_universe'].dtype == np.int8

    def test_in_universe_values_binary(self):
        rows = [_make_row(fiscal_year=2020), _make_row(fiscal_year=2008)]
        result = _classify(rows)
        assert set(result['in_universe'].unique()).issubset({0, 1})

    def test_excl_reason_empty_when_included(self):
        result = _classify([_make_row()])
        assert result['excl_reason'].iloc[0] == ''

    def test_excl_reason_populated_when_excluded(self):
        result = _classify([_make_row(period_type='quarterly')])
        assert result['excl_reason'].iloc[0] != ''

    def test_multiple_exclusion_reasons_pipe_separated(self):
        """A quarterly row from 2005 gets both reasons."""
        result = _classify([_make_row(period_type='quarterly', fiscal_year=2005)])
        reasons = result['excl_reason'].iloc[0]
        assert 'quarterly_row' in reasons
        assert 'fy<2009' in reasons
        assert '|' in reasons

    def test_no_trailing_pipe(self):
        result = _classify([_make_row(period_type='quarterly', fiscal_year=2005)])
        reason = result['excl_reason'].iloc[0]
        assert not reason.endswith('|')
        assert not reason.startswith('|')

    def test_original_df_not_mutated(self):
        df = pd.DataFrame([_make_row()])
        original_cols = set(df.columns)
        _classify([_make_row()])
        assert 'in_universe' not in df.columns

    def test_result_has_both_new_columns(self):
        result = _classify([_make_row()])
        assert 'in_universe' in result.columns
        assert 'excl_reason' in result.columns


# ═══════════════════════════════════════════════════════════════════════════════
# Market-Specific Rules
# ═══════════════════════════════════════════════════════════════════════════════

class TestMarketSpecific:
    """EU individual country codes, unknown markets, etc."""

    def test_de_market_maps_to_zero_floor(self):
        result = _classify([_make_row(market='DE', entry_price=0.01)], apply_filters=True)
        assert 'price_below_market_floor' not in result['excl_reason'].iloc[0]

    def test_unknown_market_defaults_to_zero_floor(self):
        """Market not in MARKET_MIN_PRICE dict → fillna(0.0) → no floor."""
        result = _classify([_make_row(market='ZZ', entry_price=0.01)], apply_filters=True)
        assert 'price_below_market_floor' not in result['excl_reason'].iloc[0]

    def test_kr_market_no_floor(self):
        result = _classify([_make_row(market='KR', entry_price=0.01)], apply_filters=True)
        assert 'price_below_market_floor' not in result['excl_reason'].iloc[0]


# ═══════════════════════════════════════════════════════════════════════════════
# Missing Column Handling (defensive)
# ═══════════════════════════════════════════════════════════════════════════════

class TestMissingColumns:
    """Verify graceful handling when optional columns are absent."""

    def test_no_revenue_column(self):
        row = _make_row()
        del row['revenue']
        result = _classify([row], apply_filters=True)
        assert 'revenue' not in result['excl_reason'].iloc[0]

    def test_no_sic_code_column(self):
        row = _make_row()
        del row['sic_code']
        result = _classify([row], apply_filters=True)
        assert 'financial_sector' not in result['excl_reason'].iloc[0]
        assert 'utility_sector' not in result['excl_reason'].iloc[0]

    def test_no_exchange_column(self):
        row = _make_row()
        del row['exchange']
        result = _classify([row], apply_filters=True)
        assert result['in_universe'].iloc[0] == 1

    def test_no_entry_price_column(self):
        row = _make_row()
        del row['entry_price']
        result = _classify([row], apply_filters=True)
        assert 'no_price' in result['excl_reason'].iloc[0]
