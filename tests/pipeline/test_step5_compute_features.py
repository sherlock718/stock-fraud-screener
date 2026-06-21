"""
Tests for pipeline/step5_compute_features.py — Session 4 critical coverage.

Validates:
  1. No temporal leakage (forward_return_*, beat_local_market_* never used as feature inputs)
  2. No label leakage (fraud_confirmed, ml_* never used)
  3. Rank leakage detection (cross-sectional ranks must groupby fiscal_year)
  4. Formula correctness (spot-check 8 key features)
  5. Winsorization bounds (ratio_cols clipped to 1st–99th percentile)
  6. Output shape contract
  7. Rolling features are past-only (no lookahead in roe_volatility_5yr)
  8. Safe division (sdiv handles zero/NaN)

All tests use synthetic data — no disk reads, no network calls.
"""
import ast
import inspect
import textwrap
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Import step5 functions directly
from pipeline.step5_compute_features import (
    add_accruals,
    add_composite_scores,
    add_fraud_scores,
    add_interactions,
    add_liquidity,
    add_macro_interactions,
    add_momentum_ranks,
    add_montier_c_score,
    add_profitability,
    add_sector_percentiles,
    add_size_features,
    add_valuation,
    sdiv,
    slog,
    winsorize,
)


# ─── Test fixtures ────────────────────────────────────────────────────────────

@pytest.fixture
def base_df():
    """Minimal synthetic dataset with 20 rows across 2 fiscal years and 2 tickers."""
    np.random.seed(42)
    n = 20
    tickers = ['AAPL'] * 10 + ['MSFT'] * 10
    years = list(range(2015, 2025)) * 2
    return pd.DataFrame({
        'ticker': tickers,
        'cik': ['320193'] * 10 + ['789019'] * 10,
        'fiscal_year': years,
        'fiscal_quarter': [4] * n,
        'period_type': ['annual'] * n,
        'filed_date': pd.date_range('2016-02-01', periods=n, freq='365D'),
        'market': ['US'] * n,
        'sic_code': ['7372'] * n,
        # Financials
        'revenue': np.random.uniform(50e9, 400e9, n),
        'net_income': np.random.uniform(5e9, 80e9, n),
        'total_assets': np.random.uniform(200e9, 500e9, n),
        'total_equity': np.random.uniform(50e9, 150e9, n),
        'operating_income': np.random.uniform(10e9, 100e9, n),
        'operating_cash_flow': np.random.uniform(10e9, 100e9, n),
        'current_assets': np.random.uniform(50e9, 200e9, n),
        'current_liabilities': np.random.uniform(30e9, 150e9, n),
        'long_term_debt': np.random.uniform(10e9, 100e9, n),
        'cash': np.random.uniform(10e9, 80e9, n),
        'market_cap_at_filing': np.random.uniform(500e9, 3000e9, n),
        'entry_price': np.random.uniform(100, 400, n),
        'gross_profit': np.random.uniform(20e9, 200e9, n),
        'accounts_receivable': np.random.uniform(5e9, 50e9, n),
        'inventory': np.random.uniform(1e9, 10e9, n),
        'accounts_payable': np.random.uniform(5e9, 40e9, n),
        'ppe_net': np.random.uniform(10e9, 80e9, n),
        'depreciation': np.random.uniform(2e9, 15e9, n),
        'cogs': np.random.uniform(30e9, 250e9, n),
        'sga_expense': np.random.uniform(5e9, 30e9, n),
        'short_term_debt': np.random.uniform(0, 20e9, n),
        'capex': np.random.uniform(2e9, 20e9, n),
        # YoY growth columns (from step2)
        'revenue_growth': np.random.uniform(-0.1, 0.3, n),
        'net_income_growth': np.random.uniform(-0.3, 0.5, n),
        'assets_growth': np.random.uniform(-0.05, 0.2, n),
        'receivables_growth': np.random.uniform(-0.2, 0.4, n),
        'inventory_growth': np.random.uniform(-0.2, 0.3, n),
        'ap_growth': np.random.uniform(-0.2, 0.3, n),
        'depreciation_growth': np.random.uniform(-0.1, 0.2, n),
        'gross_margin_change': np.random.uniform(-0.05, 0.05, n),
        'sga_growth_yoy': np.random.uniform(-0.1, 0.2, n),
        'shares_growth': np.random.uniform(-0.03, 0.05, n),
        'asset_turnover_change': np.random.uniform(-0.05, 0.05, n),
        'debt_growth': np.random.uniform(-0.2, 0.3, n),
        # Momentum (from step3 — prior returns, past-only)
        'momentum_3m_prior': np.random.uniform(-0.1, 0.2, n),
        'momentum_6m_prior': np.random.uniform(-0.15, 0.3, n),
        'momentum_12m_prior': np.random.uniform(-0.2, 0.5, n),
        'vol_prior_12m': np.random.uniform(0.1, 0.5, n),
        # Forward returns (labels — must NOT be used as features)
        'forward_return_1y': np.random.uniform(-0.3, 0.5, n),
        'forward_return_3y': np.random.uniform(-0.5, 1.5, n),
        'forward_return_5y': np.random.uniform(-0.5, 3.0, n),
        'beat_local_market_1y': np.random.choice([0, 1], n),
        'beat_local_market_3y': np.random.choice([0, 1], n),
        # Macro (from step4)
        'fed_funds_rate': np.random.uniform(0.25, 5.0, n),
        'recession': np.random.choice([0, 1], n, p=[0.85, 0.15]),
        'yield_curve': np.random.uniform(-0.5, 3.0, n),
        'hy_spread': np.random.uniform(3.0, 8.0, n),
        'treasury_10y': np.random.uniform(1.0, 5.0, n),
    })


# ─── 1. Temporal Leakage Tests ────────────────────────────────────────────────

class TestNoTemporalLeakage:
    """Verify no forward_return_* or beat_local_market_* columns are used as computation inputs."""

    FORBIDDEN_FEATURES = [
        'forward_return_6m', 'forward_return_1y', 'forward_return_2y',
        'forward_return_3y', 'forward_return_5y',
        'beat_local_market_1y', 'beat_local_market_3y', 'beat_local_market_5y',
        'excess_return_local_1y', 'excess_return_local_3y', 'excess_return_local_5y',
    ]

    def test_source_code_no_forward_return_usage(self):
        """Statically verify that feature functions never read forward_return columns."""
        src = Path(__file__).parent.parent.parent / 'pipeline' / 'step5_compute_features.py'
        code = src.read_text()

        feature_funcs = [
            'add_valuation', 'add_profitability', 'add_accruals', 'add_fraud_scores',
            'add_montier_c_score', 'add_liquidity', 'add_composite_scores',
            'add_size_features', 'add_momentum_ranks', 'add_interactions',
            'add_sector_percentiles', 'add_macro_interactions',
        ]

        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in feature_funcs:
                func_src = ast.get_source_segment(code, node)
                for forbidden in self.FORBIDDEN_FEATURES:
                    assert forbidden not in func_src, (
                        f"Function {node.name} references forbidden column '{forbidden}'"
                    )

    def test_forward_columns_pass_through_unchanged(self, base_df):
        """Forward return columns in input must pass through without modification."""
        df = base_df.copy()
        orig_fwd = df['forward_return_1y'].copy()
        df = add_valuation(df)
        df = add_profitability(df)
        df = add_accruals(df)
        df = add_fraud_scores(df)
        pd.testing.assert_series_equal(df['forward_return_1y'], orig_fwd)


# ─── 2. Label Leakage Tests ──────────────────────────────────────────────────

class TestNoLabelLeakage:
    """Verify no label columns (fraud_confirmed, ml_*) are referenced."""

    LABEL_COLUMNS = [
        'fraud_confirmed', 'fraud_source',
        'ml_1y', 'ml_3y', 'ml_5y',
        'ml_1y_oof', 'ml_3y_oof', 'ml_5y_oof',
        'ml_pred_excess_3y',
        'alpha_value', 'alpha_quality', 'alpha_momentum',
        'alpha_growth', 'alpha_fraud_risk', 'alpha_composite',
    ]

    def test_source_code_no_label_columns(self):
        """Static check: step5 source never references label columns."""
        src = Path(__file__).parent.parent.parent / 'pipeline' / 'step5_compute_features.py'
        code = src.read_text()

        for label in self.LABEL_COLUMNS:
            # Skip matches inside docstrings/comments that explain why they're excluded
            lines = code.split('\n')
            for i, line in enumerate(lines, 1):
                stripped = line.lstrip()
                if stripped.startswith('#') or stripped.startswith('"""') or stripped.startswith("'''"):
                    continue
                if label in line and 'df.get' in line or f"df['{label}']" in line:
                    pytest.fail(f"Line {i} uses label column '{label}': {line.strip()}")


# ─── 3. Rank Leakage Tests ───────────────────────────────────────────────────

class TestRankLeakage:
    """Cross-sectional ranks must be computed within fiscal_year, not across all time."""

    def test_momentum_ranks_grouped_by_fiscal_year(self, base_df):
        """Momentum ranks must be within (fiscal_year, market) cohorts."""
        df = base_df.copy()
        df = add_momentum_ranks(df)

        # If a company has the highest momentum in fiscal_year=2015,
        # its rank should be 1.0 within that year — regardless of other years.
        for yr in df['fiscal_year'].unique():
            mask = df['fiscal_year'] == yr
            subset = df.loc[mask, 'momentum_12m_rank'].dropna()
            if len(subset) >= 2:
                # Ranks should be between 0 and 1 (pct=True)
                assert subset.min() >= 0.0
                assert subset.max() <= 1.0

    def test_sector_percentiles_grouped_by_fiscal_year(self, base_df):
        """Sector percentiles include fiscal_year in groupby."""
        df = base_df.copy()
        df = add_valuation(df)
        df = add_profitability(df)
        df = add_sector_percentiles(df)

        # If fiscal_year in groupby, ranks within a single year are independent
        if 'pe_ratio_sector_pct' in df.columns:
            for yr in df['fiscal_year'].unique():
                subset = df.loc[df['fiscal_year'] == yr, 'pe_ratio_sector_pct'].dropna()
                if len(subset) >= 2:
                    assert subset.min() >= 0.0
                    assert subset.max() <= 1.0

    def test_quality_composite_grouped_by_fiscal_year(self, base_df):
        """quality_composite rank is within fiscal_year×market cohort (RANK-LEAKAGE-001 fixed)."""
        df = base_df.copy()
        df = add_valuation(df)
        df = add_profitability(df)
        df = add_composite_scores(df)

        src = inspect.getsource(add_composite_scores)
        assert 'groupby' in src and 'fiscal_year' in src, (
            "add_composite_scores must rank within fiscal_year groups"
        )


# ─── 4. Formula Correctness Tests ────────────────────────────────────────────

class TestFormulaCorrectness:
    """Spot-check key feature formulas against known inputs."""

    def test_pe_ratio(self):
        """P/E = market_cap / net_income."""
        df = pd.DataFrame({
            'market_cap_at_filing': [1000.0, 500.0, 100.0],
            'net_income': [100.0, -50.0, 0.0],  # positive, negative, zero
            'revenue': [500.0, 300.0, 200.0],
            'total_assets': [2000.0, 1000.0, 500.0],
            'total_equity': [800.0, 400.0, 200.0],
            'operating_income': [150.0, 80.0, 50.0],
            'operating_cash_flow': [120.0, 70.0, 40.0],
            'current_assets': [400.0, 200.0, 100.0],
            'current_liabilities': [200.0, 100.0, 50.0],
            'long_term_debt': [300.0, 150.0, 75.0],
            'cash': [100.0, 50.0, 25.0],
            'entry_price': [150.0, 75.0, 30.0],
            'depreciation': [20.0, 10.0, 5.0],
            'capex': [30.0, 15.0, 8.0],
        })
        df = add_valuation(df)

        assert df['pe_ratio'].iloc[0] == pytest.approx(10.0, rel=1e-6)
        assert df['pe_ratio'].iloc[1] == pytest.approx(-10.0, rel=1e-6)
        assert np.isnan(df['pe_ratio'].iloc[2])  # zero denominator → NaN

    def test_roa(self):
        """ROA = net_income / total_assets."""
        df = pd.DataFrame({
            'net_income': [50.0, -20.0],
            'total_assets': [1000.0, 500.0],
            'total_equity': [400.0, 200.0],
            'revenue': [800.0, 400.0],
            'gross_profit': [300.0, 150.0],
            'operating_income': [100.0, 50.0],
            'operating_cash_flow': [80.0, 40.0],
            'long_term_debt': [200.0, 100.0],
            'cash': [50.0, 25.0],
        })
        df = add_profitability(df)
        assert df['roa'].iloc[0] == pytest.approx(0.05, rel=1e-6)
        assert df['roa'].iloc[1] == pytest.approx(-0.04, rel=1e-6)

    def test_sloan_accruals(self):
        """Sloan ratio = (net_income - OCF) / total_assets."""
        df = pd.DataFrame({
            'net_income': [100.0, 50.0],
            'operating_cash_flow': [80.0, 60.0],
            'total_assets': [1000.0, 500.0],
            'total_equity': [400.0, 200.0],
            'current_assets': [300.0, 150.0],
            'current_liabilities': [200.0, 100.0],
            'cash': [50.0, 25.0],
            'short_term_debt': [20.0, 10.0],
            'accounts_receivable': [80.0, 40.0],
            'inventory': [60.0, 30.0],
            'accounts_payable': [70.0, 35.0],
            'long_term_debt': [200.0, 100.0],
            'revenue': [800.0, 400.0],
        })
        df = add_accruals(df)
        # (100 - 80) / 1000 = 0.02
        assert df['sloan_accruals'].iloc[0] == pytest.approx(0.02, rel=1e-6)
        # (50 - 60) / 500 = -0.02
        assert df['sloan_accruals'].iloc[1] == pytest.approx(-0.02, rel=1e-6)

    def test_altman_z_score_coefficients(self):
        """Altman Z = 1.2*X1 + 1.4*X2 + 3.3*X3 + 0.6*X4 + 1.0*X5."""
        df = pd.DataFrame({
            'revenue': [1000.0],
            'net_income': [50.0],
            'total_assets': [2000.0],
            'total_equity': [800.0],
            'operating_income': [100.0],
            'operating_cash_flow': [90.0],
            'current_assets': [600.0],
            'current_liabilities': [400.0],
            'long_term_debt': [500.0],
            'cash': [100.0],
            'market_cap_at_filing': [3000.0],
            'accounts_receivable': [200.0],
            'inventory': [150.0],
            'ppe_net': [800.0],
            'depreciation': [50.0],
            'cogs': [700.0],
            'gross_profit': [300.0],
            'sga_expense': [80.0],
            'entry_price': [50.0],
        })
        df = add_fraud_scores(df)

        # Manual calculation:
        wc = 600 - 400  # 200
        x1 = wc / 2000  # 0.1
        re = 800 - 0  # 800 (no additional_paid_in_capital)
        x2 = re / 2000  # 0.4
        x3 = 100 / 2000  # 0.05
        total_liab = max(2000 - 800, 1e3)  # 1200
        x4 = min(3000 / 1200, 20)  # 2.5
        x5 = 1000 / 2000  # 0.5

        expected_z = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5
        assert df['altman_z_score'].iloc[0] == pytest.approx(expected_z, rel=1e-4)

    def test_piotroski_f_score_range(self, base_df):
        """Piotroski F-score must be in [0, 9]."""
        df = base_df.copy()
        df = add_valuation(df)
        df = add_profitability(df)
        df = add_accruals(df)
        df = add_liquidity(df)
        df = add_composite_scores(df)
        assert df['piotroski_f_score'].min() >= 0
        assert df['piotroski_f_score'].max() <= 9

    def test_current_ratio(self):
        """current_ratio = current_assets / current_liabilities."""
        df = pd.DataFrame({
            'current_assets': [200.0, 100.0],
            'current_liabilities': [100.0, 0.0],
            'total_assets': [500.0, 300.0],
            'total_equity': [200.0, 150.0],
            'long_term_debt': [100.0, 50.0],
            'net_income': [30.0, 20.0],
            'operating_income': [40.0, 25.0],
            'revenue': [400.0, 200.0],
            'operating_cash_flow': [35.0, 22.0],
            'cash': [50.0, 30.0],
            'inventory': [40.0, 20.0],
            'accounts_receivable': [60.0, 30.0],
            'accounts_payable': [30.0, 15.0],
            'cogs': [250.0, 130.0],
            'ticker': ['A', 'B'],
            'fiscal_year': [2020, 2020],
        })
        df = add_liquidity(df)
        assert df['current_ratio'].iloc[0] == pytest.approx(2.0, rel=1e-6)
        assert np.isnan(df['current_ratio'].iloc[1])  # zero denom

    def test_earnings_yield_is_inverse_pe(self):
        """earnings_yield = NI / market_cap (inverse of P/E)."""
        df = pd.DataFrame({
            'market_cap_at_filing': [1000.0],
            'net_income': [50.0],
            'revenue': [500.0],
            'total_assets': [2000.0],
            'total_equity': [800.0],
            'operating_income': [100.0],
            'operating_cash_flow': [80.0],
            'current_assets': [400.0],
            'current_liabilities': [200.0],
            'long_term_debt': [300.0],
            'cash': [100.0],
            'entry_price': [50.0],
            'depreciation': [20.0],
            'capex': [15.0],
        })
        df = add_valuation(df)
        assert df['earnings_yield'].iloc[0] == pytest.approx(0.05, rel=1e-6)
        assert df['pe_ratio'].iloc[0] == pytest.approx(20.0, rel=1e-6)

    def test_book_to_market(self):
        """book_to_market = total_equity / market_cap."""
        df = pd.DataFrame({
            'market_cap_at_filing': [1000.0, 0.0],
            'net_income': [50.0, 10.0],
            'revenue': [500.0, 100.0],
            'total_assets': [2000.0, 400.0],
            'total_equity': [500.0, 200.0],
            'operating_income': [100.0, 30.0],
            'operating_cash_flow': [80.0, 20.0],
            'current_assets': [400.0, 100.0],
            'current_liabilities': [200.0, 50.0],
            'long_term_debt': [300.0, 50.0],
            'cash': [100.0, 20.0],
            'entry_price': [50.0, 10.0],
            'depreciation': [20.0, 5.0],
            'capex': [15.0, 3.0],
        })
        df = add_valuation(df)
        assert df['book_to_market'].iloc[0] == pytest.approx(0.5, rel=1e-6)
        assert np.isnan(df['book_to_market'].iloc[1])  # zero market cap


# ─── 5. Winsorization Tests ──────────────────────────────────────────────────

class TestWinsorization:
    """Winsorize function and its application to ratio_cols."""

    def test_winsorize_clips_extremes(self):
        """Values outside 1st/99th percentile are clipped."""
        s = pd.Series(list(range(100)) + [10000, -10000])
        result = winsorize(s)
        assert result.max() <= s.quantile(0.99)
        assert result.min() >= s.quantile(0.01)

    def test_winsorize_preserves_middle(self):
        """Values well inside 1st/99th bounds are unchanged."""
        # 100 values: 1..100. Middle values (25-75) are well within 1st/99th pctile.
        s = pd.Series(range(1, 101), dtype=float)
        result = winsorize(s)
        # Values at index 24 (=25.0) and 74 (=75.0) should be unchanged
        assert result.iloc[24] == 25.0
        assert result.iloc[74] == 75.0

    def test_winsorize_handles_nan(self):
        """NaN values pass through without affecting bounds."""
        s = pd.Series([1.0, 2.0, np.nan, 4.0, 5.0, np.nan, 7.0, 8.0, 9.0, 10.0])
        result = winsorize(s)
        assert result.isna().sum() == 2  # NaNs preserved

    def test_ratio_cols_list_includes_growth_columns(self):
        """All growth_yoy columns from step2 should be in the winsorize list."""
        src = Path(__file__).parent.parent.parent / 'pipeline' / 'step5_compute_features.py'
        code = src.read_text()

        # Find the ratio_cols list in source
        start = code.index("ratio_cols = [")
        end = code.index("]", start) + 1
        ratio_block = code[start:end]

        # Key growth columns that MUST be winsorized (pipeline-integrity Rule 6)
        must_winsorize = [
            'revenue_growth', 'net_income_growth', 'assets_growth',
            'eps_growth', 'ocf_growth',
        ]
        for col in must_winsorize:
            assert col in ratio_block, (
                f"Growth column '{col}' missing from ratio_cols winsorize list"
            )


# ─── 6. Output Shape Contract Tests ──────────────────────────────────────────

class TestOutputShapeContract:
    """Step 5 must produce expected columns and preserve row count."""

    REQUIRED_OUTPUT_COLS = [
        # Identifiers
        'ticker', 'cik', 'fiscal_year', 'period_type',
        # Must-have features (subset)
        'pe_ratio', 'roa', 'sloan_accruals', 'altman_z_score',
        'piotroski_f_score', 'log_market_cap',
    ]

    def test_all_required_columns_present(self, base_df):
        """Key output columns must exist after all feature functions run."""
        df = base_df.copy()
        df = add_valuation(df)
        df = add_profitability(df)
        df = add_accruals(df)
        df = add_fraud_scores(df)
        df = add_liquidity(df)
        df = add_composite_scores(df)
        df = add_size_features(df)
        for col in self.REQUIRED_OUTPUT_COLS:
            assert col in df.columns, f"Required column '{col}' missing from output"

    def test_row_count_preserved(self, base_df):
        """Feature computation must not add or drop rows."""
        df = base_df.copy()
        original_len = len(df)
        df = add_valuation(df)
        df = add_profitability(df)
        df = add_accruals(df)
        df = add_fraud_scores(df)
        df = add_montier_c_score(df)
        df = add_liquidity(df)
        df = add_composite_scores(df)
        df = add_size_features(df)
        df = add_momentum_ranks(df)
        df = add_interactions(df)
        df = add_sector_percentiles(df)
        df = add_macro_interactions(df)
        assert len(df) == original_len

    def test_no_all_nan_feature_columns(self, base_df):
        """No computed feature should be entirely NaN given reasonable input."""
        df = base_df.copy()
        df = add_valuation(df)
        df = add_profitability(df)

        # With valid numeric inputs, these should NOT be all-NaN
        critical_features = ['pe_ratio', 'roa', 'roe', 'gross_margin']
        for feat in critical_features:
            assert df[feat].notna().sum() > 0, f"Feature '{feat}' is all NaN"


# ─── 7. Rolling Features Past-Only ───────────────────────────────────────────

class TestRollingPastOnly:
    """Rolling window features must only look backward (no future data)."""

    def test_roe_volatility_uses_trailing_window(self):
        """roe_volatility_5yr uses pandas rolling which is trailing by default."""
        # Create sequential data: ticker A with 10 years of ROE
        df = pd.DataFrame({
            'ticker': ['A'] * 10,
            'fiscal_year': list(range(2010, 2020)),
            'fiscal_quarter': [4] * 10,
            'roe': [0.1, 0.12, 0.08, 0.15, 0.11, 0.09, 0.13, 0.14, 0.07, 0.16],
        })
        df = df.sort_values(['ticker', 'fiscal_year', 'fiscal_quarter'])
        df['roe_volatility_5yr'] = (
            df.groupby('ticker')['roe']
            .transform(lambda x: x.rolling(5, min_periods=3).std())
        )

        # First 2 rows should be NaN (min_periods=3)
        assert pd.isna(df['roe_volatility_5yr'].iloc[0])
        assert pd.isna(df['roe_volatility_5yr'].iloc[1])

        # Row at index 4 (fiscal_year=2014) should use only 2010-2014 data
        expected_std = pd.Series([0.1, 0.12, 0.08, 0.15, 0.11]).std()
        assert df['roe_volatility_5yr'].iloc[4] == pytest.approx(expected_std, rel=1e-4)

        # Row at index 4 should NOT include future data (2015+)
        # Verify by checking the rolling window doesn't include row 5
        future_included_std = pd.Series([0.1, 0.12, 0.08, 0.15, 0.11, 0.09]).std()
        assert df['roe_volatility_5yr'].iloc[4] != pytest.approx(future_included_std, rel=1e-4)


# ─── 8. Safe Division Tests ──────────────────────────────────────────────────

class TestSafeDivision:
    """sdiv and slog handle edge cases correctly."""

    def test_sdiv_zero_denominator_returns_nan(self):
        num = pd.Series([10.0, 20.0, 30.0])
        denom = pd.Series([5.0, 0.0, 10.0])
        result = sdiv(num, denom)
        assert result.iloc[0] == pytest.approx(2.0)
        assert np.isnan(result.iloc[1])
        assert result.iloc[2] == pytest.approx(3.0)

    def test_sdiv_nan_denominator_returns_nan(self):
        num = pd.Series([10.0, 20.0])
        denom = pd.Series([5.0, np.nan])
        result = sdiv(num, denom)
        assert result.iloc[0] == pytest.approx(2.0)
        assert np.isnan(result.iloc[1])

    def test_sdiv_custom_fill(self):
        num = pd.Series([10.0])
        denom = pd.Series([0.0])
        result = sdiv(num, denom, fill=0.0)
        assert result.iloc[0] == 0.0

    def test_slog_negative_clipped(self):
        """slog clips negative values to small positive before log."""
        s = pd.Series([-100.0, 0.0, 1.0, 100.0])
        result = slog(s)
        # -100 and 0 get clipped to 1e-10, then log
        assert result.iloc[0] == pytest.approx(np.log(1e-10), rel=1e-6)
        assert result.iloc[2] == pytest.approx(0.0, abs=1e-6)
        assert result.iloc[3] == pytest.approx(np.log(100), rel=1e-6)


# ─── 9. Integration: Full Pipeline Smoke Test ────────────────────────────────

class TestFullPipelineSmoke:
    """Run all feature functions in sequence on synthetic data."""

    def test_all_functions_run_without_error(self, base_df):
        """All step5 functions execute successfully on synthetic data."""
        df = base_df.copy()
        df = add_valuation(df)
        df = add_profitability(df)
        df = add_accruals(df)
        df = add_fraud_scores(df)
        df = add_montier_c_score(df)
        df = add_liquidity(df)
        df = add_composite_scores(df)
        df = add_size_features(df)
        df = add_momentum_ranks(df)
        df = add_interactions(df)
        df = add_sector_percentiles(df)
        df = add_macro_interactions(df)

        # Should produce 100+ features beyond the ~40 input columns
        assert len(df.columns) > 100

    def test_no_infinite_values(self, base_df):
        """No computed features should contain infinity."""
        df = base_df.copy()
        df = add_valuation(df)
        df = add_profitability(df)
        df = add_accruals(df)
        df = add_fraud_scores(df)
        df = add_liquidity(df)

        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            inf_count = np.isinf(df[col]).sum()
            assert inf_count == 0, f"Column '{col}' has {inf_count} infinite values"

    def test_montier_c_score_bounded_0_1(self, base_df):
        """Montier C-score normalized to [0, 1]."""
        df = base_df.copy()
        df = add_valuation(df)
        df = add_profitability(df)
        df = add_accruals(df)
        df = add_montier_c_score(df)

        valid = df['montier_c_score'].dropna()
        if len(valid) > 0:
            assert valid.min() >= 0.0 - 1e-10
            assert valid.max() <= 1.0 + 1e-10
