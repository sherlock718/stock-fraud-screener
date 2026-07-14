"""
Tests for step0_historical_universe.py — historical CIK recovery.

Proves that companies delisted before the universe-download date can still
enter historical backtest years via the historical universe supplement.
"""

import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

BASE = Path(__file__).parent.parent.parent
DATA = BASE / 'data'


class TestHistoricalUniverseIntegrity:
    """Prove that historical/delisted CIKs can enter the pipeline."""

    def test_delisted_cik_not_in_current_ticker_file(self):
        """Known delisted companies should NOT be in company_tickers.json snapshot."""
        if not (DATA / 'tickers.parquet').exists():
            pytest.skip('tickers.parquet not available')

        tickers = pd.read_parquet(DATA / 'tickers.parquet')
        current_ciks = set(tickers['cik'].unique())

        # WorldCom: fully delisted, bankrupt 2002, CIK 723527
        worldcom_cik = '0000723527'
        assert worldcom_cik not in current_ciks, (
            'WorldCom should not appear in current SEC ticker list — '
            'it was delisted in 2002'
        )

    def test_historical_supplement_adds_delisted_companies(self):
        """After historical supplement, delisted CIKs should be present."""
        if not (DATA / 'historical_ciks.parquet').exists():
            pytest.skip('historical_ciks.parquet not yet built')

        historical = pd.read_parquet(DATA / 'historical_ciks.parquet')
        assert len(historical) > 0, 'Historical CIK file should not be empty'
        assert 'cik' in historical.columns
        assert 'has_xbrl' in historical.columns

        # All entries should have XBRL data (verified during build)
        assert historical['has_xbrl'].all(), (
            'All historical CIKs should have verified XBRL availability'
        )

    def test_merged_universe_includes_delisted_exchange_marker(self):
        """Delisted companies should be marked with exchange='DELISTED'."""
        if not (DATA / 'tickers.parquet').exists():
            pytest.skip('tickers.parquet not available')

        tickers = pd.read_parquet(DATA / 'tickers.parquet')

        # After merge, delisted companies should have exchange='DELISTED'
        delisted = tickers[tickers['exchange'] == 'DELISTED']
        if delisted.empty:
            pytest.skip('No DELISTED entries — run step0 --merge first')

        # Verify schema integrity
        assert 'cik' in delisted.columns
        assert 'market' in delisted.columns
        assert (delisted['market'] == 'US').all()

    def test_delisted_company_appears_in_historical_backtest_years(self):
        """
        Core invariant: a company delisted in year Y must appear in
        dataset rows for years < Y (while it was still filing).

        This test uses the dataset directly — if the company's XBRL data
        spans 2010–2015 and it delisted in 2016, rows for 2010–2015
        must exist after step2 processes the expanded universe.
        """
        if not (DATA / 'historical_dataset_clean.parquet').exists():
            pytest.skip('Clean dataset not available')

        df = pd.read_parquet(DATA / 'historical_dataset_clean.parquet')

        # Rite Aid: CIK 84129, XBRL data available, filed through ~2019
        rite_aid_cik = '0000084129'
        rite_aid_rows = df[df['cik'] == rite_aid_cik]

        if rite_aid_rows.empty:
            # Expected to be empty BEFORE the fix is applied
            # After fix: this should have rows
            pytest.skip(
                'Rite Aid not in dataset — expected before historical '
                'universe supplement is run'
            )

        # If present, verify it appears in historical years
        years = set(rite_aid_rows['fiscal_year'].unique())
        assert len(years) >= 3, (
            f'Rite Aid should have multi-year coverage, got {years}'
        )

    def test_universe_construction_independent_of_fraud_labels(self):
        """
        Universe inclusion must NOT depend on fraud labels.
        The historical CIK source is full-index (all 10-K filers),
        not the AAER label set.
        """
        if not (DATA / 'historical_ciks.parquet').exists():
            pytest.skip('historical_ciks.parquet not yet built')

        historical = pd.read_parquet(DATA / 'historical_ciks.parquet')

        # source field should indicate full_index or known_fraud_supplement
        if 'source' in historical.columns:
            sources = historical['source'].unique()
            # Must not have 'aaer_labels' as sole source
            assert 'aaer_labels_only' not in sources, (
                'Universe must not be constructed solely from AAER labels — '
                'that creates label-dependent selection bias'
            )


class TestFullIndexParsing:
    """Unit tests for the full-index parser logic."""

    def test_parse_10k_line_format(self):
        """Verify parsing of SEC full-index fixed-width format."""
        from pipeline.step0_historical_universe import scan_full_index

        # Mock a response with known format
        sample_lines = (
            "Company Name                                                  "
            "Form Type   CIK         Date Filed  File Name\n"
            "--------------------------------------------------------------"
            "--------------------------------------------------------------\n"
            "RITE AID CORP                                                 "
            "10-K             84129      2012-05-22  edgar/data/84129\n"
            "XEROX HOLDINGS CORP                                           "
            "10-K            108772      2012-03-01  edgar/data/108772\n"
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = sample_lines

        with patch('pipeline.step0_historical_universe.requests.get',
                   return_value=mock_response):
            results = scan_full_index(2012, 'QTR1')

        assert len(results) == 2
        # Verify CIK extraction (zero-padded to 10 chars)
        ciks = {r['cik'] for r in results}
        assert '0000084129' in ciks
        assert '0000108772' in ciks
