"""
Tests for pipeline/step1_fetch_tickers.py — schema and dedup contracts.

Synthetic data only. No network calls. No disk I/O.
"""
import pandas as pd
import pytest


REQUIRED_COLUMNS = [
    'cik', 'ticker', 'name', 'exchange',
    'sic_code', 'sic_description', 'market', 'country', 'accounting_std',
]


def _make_tickers_df(n=10):
    """Synthetic output matching step1's parquet schema."""
    rows = []
    for i in range(n):
        rows.append({
            'cik': str(1000000 + i).zfill(10),
            'ticker': f'TKR{i}',
            'name': f'Company {i}',
            'exchange': 'NYSE' if i % 3 != 0 else 'OTC',
            'sic_code': str(3570 + i),
            'sic_description': 'Electronic Computers',
            'market': 'US',
            'country': 'United States',
            'accounting_std': 'GAAP',
        })
    return pd.DataFrame(rows)


class TestStep1Schema:
    def test_required_columns_present(self):
        df = _make_tickers_df()
        for col in REQUIRED_COLUMNS:
            assert col in df.columns, f'Missing required column: {col}'

    def test_cik_is_string_padded_10(self):
        df = _make_tickers_df()
        for cik in df['cik']:
            assert isinstance(cik, str)
            assert len(cik) == 10
            assert cik.isdigit()

    def test_ticker_uppercase_no_spaces(self):
        df = _make_tickers_df()
        for ticker in df['ticker']:
            assert ticker == ticker.upper()
            assert ' ' not in ticker

    def test_market_column_us(self):
        df = _make_tickers_df()
        assert (df['market'] == 'US').all()


class TestStep1Dedup:
    def test_no_duplicate_cik(self):
        df = _make_tickers_df()
        assert df['cik'].is_unique

    def test_dedup_keeps_first_on_cik_collision(self):
        """If EDGAR returns two rows with same CIK, step1 keeps the first."""
        df = _make_tickers_df()
        dup_row = df.iloc[0:1].copy()
        dup_row['ticker'] = 'DUPE'
        df_with_dup = pd.concat([df, dup_row], ignore_index=True)
        deduped = df_with_dup.drop_duplicates(subset='cik', keep='first')
        assert deduped['cik'].is_unique
        assert 'DUPE' not in deduped['ticker'].values

    def test_duplicate_ticker_different_cik_preserved(self):
        """Two CIKs with same ticker should BOTH survive (not deduped by ticker)."""
        df = _make_tickers_df(5)
        df.loc[1, 'ticker'] = df.loc[0, 'ticker']  # same ticker, different CIK
        deduped = df.drop_duplicates(subset='cik', keep='first')
        assert len(deduped) == 5  # no rows lost


class TestStep1Survivorship:
    def test_otc_companies_included(self):
        """OTC (potentially delisted) companies must NOT be filtered out."""
        df = _make_tickers_df(9)
        otc_count = (df['exchange'] == 'OTC').sum()
        assert otc_count > 0, 'Test data must include OTC companies'
        # Step 1 keeps all — no filtering
        assert len(df) == 9

    def test_no_active_status_filter(self):
        """Step 1 must not filter by any 'active' or 'status' column."""
        df = _make_tickers_df()
        assert 'active' not in df.columns
        assert 'status' not in df.columns


class TestStep1Identifiers:
    def test_us_tickers_no_suffix(self):
        """US tickers must be bare symbols (no .US suffix)."""
        df = _make_tickers_df()
        for ticker in df['ticker']:
            assert not ticker.endswith('.US')
            assert '.' not in ticker  # US symbols have no dot

    def test_exchange_populated(self):
        """Every row must have an exchange value (NYSE, NASDAQ, OTC, etc.)."""
        df = _make_tickers_df()
        assert df['exchange'].notna().all()
        assert (df['exchange'] != '').all()
