"""Tests for pipeline/enrich_fraud_labels.py — fraud label construction."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline.enrich_fraud_labels import (
    build_fraud_confirmed,
    build_fraud_suspect,
    ENFORCEMENT_WINDOW_AFTER,
    ENFORCEMENT_WINDOW_BEFORE,
    KNOWN_FRAUD_CIKS,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _make_row(**overrides) -> dict:
    """Clean company row (no fraud signals)."""
    base = {
        'cik': '999999',
        'ticker': 'CLEAN',
        'fiscal_year': 2015,
        'market': 'US',
        'beneish_m_score': -3.0,
        'piotroski_f_score': 7.0,
        'altman_z_score': 4.0,
        'going_concern': 0,
        'small_auditor_flag': 0,
        'market_cap_at_filing': 500_000_000.0,
    }
    base.update(overrides)
    return base


# ═══════════════════════════════════════════════════════════════════════════════
# fraud_confirmed — Known Fraud CIKs
# ═══════════════════════════════════════════════════════════════════════════════

class TestFraudConfirmedKnown:
    """Hardcoded KNOWN_FRAUD_CIKS matching."""

    def test_enron_in_fraud_year_labeled(self):
        df = pd.DataFrame([_make_row(cik='72971', fiscal_year=2000)])
        label = build_fraud_confirmed(df, [])
        assert label.iloc[0] == 1

    def test_enron_outside_fraud_years_not_labeled(self):
        df = pd.DataFrame([_make_row(cik='72971', fiscal_year=2010)])
        label = build_fraud_confirmed(df, [])
        assert label.iloc[0] == 0

    def test_clean_company_not_labeled(self):
        df = pd.DataFrame([_make_row(cik='999999', fiscal_year=2015)])
        label = build_fraud_confirmed(df, [])
        assert label.iloc[0] == 0

    def test_worldcom_labeled(self):
        df = pd.DataFrame([_make_row(cik='723527', fiscal_year=2001)])
        label = build_fraud_confirmed(df, [])
        assert label.iloc[0] == 1

    def test_multiple_rows_mixed(self):
        rows = [
            _make_row(cik='72971', fiscal_year=2000),   # Enron fraud year
            _make_row(cik='999999', fiscal_year=2015),  # clean
            _make_row(cik='72971', fiscal_year=2010),   # Enron but post-fraud
        ]
        df = pd.DataFrame(rows)
        labels = build_fraud_confirmed(df, [])
        assert labels.tolist() == [1, 0, 0]


# ═══════════════════════════════════════════════════════════════════════════════
# fraud_confirmed — AAER Records
# ═══════════════════════════════════════════════════════════════════════════════

class TestFraudConfirmedAAER:
    """AAER-based matching with enforcement window."""

    def test_aaer_match_within_window(self):
        aaer = [{'cik': '123456', 'year': 2015}]
        # fy_min = 2015 - 5 = 2010, fy_max = 2015 + 2 = 2017
        df = pd.DataFrame([_make_row(cik='123456', fiscal_year=2012)])
        label = build_fraud_confirmed(df, aaer)
        assert label.iloc[0] == 1

    def test_aaer_match_at_enforcement_year(self):
        aaer = [{'cik': '123456', 'year': 2015}]
        df = pd.DataFrame([_make_row(cik='123456', fiscal_year=2015)])
        label = build_fraud_confirmed(df, aaer)
        assert label.iloc[0] == 1

    def test_aaer_match_at_window_lower_bound(self):
        aaer = [{'cik': '123456', 'year': 2015}]
        # fy_min = 2015 - 5 = 2010
        df = pd.DataFrame([_make_row(cik='123456', fiscal_year=2010)])
        label = build_fraud_confirmed(df, aaer)
        assert label.iloc[0] == 1

    def test_aaer_match_at_window_upper_bound(self):
        aaer = [{'cik': '123456', 'year': 2015}]
        # fy_max = 2015 + 2 = 2017
        df = pd.DataFrame([_make_row(cik='123456', fiscal_year=2017)])
        label = build_fraud_confirmed(df, aaer)
        assert label.iloc[0] == 1

    def test_aaer_outside_window_not_labeled(self):
        aaer = [{'cik': '123456', 'year': 2015}]
        # fy_min = 2010 → fiscal_year 2009 is outside
        df = pd.DataFrame([_make_row(cik='123456', fiscal_year=2009)])
        label = build_fraud_confirmed(df, aaer)
        assert label.iloc[0] == 0

    def test_aaer_different_cik_not_labeled(self):
        aaer = [{'cik': '123456', 'year': 2015}]
        df = pd.DataFrame([_make_row(cik='654321', fiscal_year=2015)])
        label = build_fraud_confirmed(df, aaer)
        assert label.iloc[0] == 0

    def test_aaer_cik_leading_zeros_stripped(self):
        aaer = [{'cik': '0000123', 'year': 2015}]
        df = pd.DataFrame([_make_row(cik='123', fiscal_year=2015)])
        label = build_fraud_confirmed(df, aaer)
        assert label.iloc[0] == 1

    def test_empty_aaer_still_uses_known_frauds(self):
        df = pd.DataFrame([_make_row(cik='72971', fiscal_year=2000)])
        label = build_fraud_confirmed(df, [])
        assert label.iloc[0] == 1

    def test_aaer_with_missing_year_skipped(self):
        aaer = [{'cik': '123456', 'year': None}]
        df = pd.DataFrame([_make_row(cik='123456', fiscal_year=2015)])
        label = build_fraud_confirmed(df, aaer)
        assert label.iloc[0] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# fraud_suspect — Quantitative Red Flags
# ═══════════════════════════════════════════════════════════════════════════════

class TestFraudSuspect:
    """Suspect requires 2+ signals firing."""

    def test_no_signals_not_suspect(self):
        df = pd.DataFrame([_make_row()])
        label = build_fraud_suspect(df)
        assert label.iloc[0] == 0

    def test_one_signal_not_suspect(self):
        df = pd.DataFrame([_make_row(beneish_m_score=-1.0)])  # fires beneish only
        label = build_fraud_suspect(df)
        assert label.iloc[0] == 0

    def test_two_signals_is_suspect(self):
        df = pd.DataFrame([_make_row(
            beneish_m_score=-1.0,    # > -1.78 → fires
            piotroski_f_score=1.0,   # <= 2 → fires
        )])
        label = build_fraud_suspect(df)
        assert label.iloc[0] == 1

    def test_three_signals_is_suspect(self):
        df = pd.DataFrame([_make_row(
            beneish_m_score=-1.0,
            piotroski_f_score=1.0,
            altman_z_score=0.5,
        )])
        label = build_fraud_suspect(df)
        assert label.iloc[0] == 1

    def test_beneish_threshold_exact(self):
        """Beneish > -1.78 fires; exactly -1.78 does NOT fire."""
        df = pd.DataFrame([
            _make_row(beneish_m_score=-1.78, piotroski_f_score=1.0),
            _make_row(beneish_m_score=-1.77, piotroski_f_score=1.0),
        ])
        labels = build_fraud_suspect(df)
        assert labels.iloc[0] == 0  # -1.78 not > -1.78
        assert labels.iloc[1] == 1  # -1.77 > -1.78

    def test_going_concern_counts_as_signal(self):
        df = pd.DataFrame([_make_row(going_concern=1, altman_z_score=0.5)])
        label = build_fraud_suspect(df)
        assert label.iloc[0] == 1

    def test_small_auditor_large_cap_counts(self):
        df = pd.DataFrame([_make_row(
            small_auditor_flag=1,
            market_cap_at_filing=200_000_000.0,  # > $100M
            altman_z_score=0.5,                   # second signal
        )])
        label = build_fraud_suspect(df)
        assert label.iloc[0] == 1

    def test_small_auditor_small_cap_does_not_count(self):
        """Small auditor with small cap is normal — doesn't fire."""
        df = pd.DataFrame([_make_row(
            small_auditor_flag=1,
            market_cap_at_filing=50_000_000.0,  # < $100M
            altman_z_score=0.5,                  # only 1 signal fires
        )])
        label = build_fraud_suspect(df)
        assert label.iloc[0] == 0

    def test_nan_signals_do_not_fire(self):
        """NaN in scoring columns should not fire signals."""
        df = pd.DataFrame([_make_row(
            beneish_m_score=np.nan,
            piotroski_f_score=np.nan,
            altman_z_score=np.nan,
        )])
        label = build_fraud_suspect(df)
        assert label.iloc[0] == 0

    def test_missing_columns_handled(self):
        """If signal columns don't exist, no crash."""
        df = pd.DataFrame([{'cik': '999', 'ticker': 'X', 'fiscal_year': 2020}])
        label = build_fraud_suspect(df)
        assert label.iloc[0] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# Label Semantics
# ═══════════════════════════════════════════════════════════════════════════════

class TestLabelSemantics:
    """Column dtype, mutual exclusivity, no false positives."""

    def test_confirmed_dtype_int8(self):
        df = pd.DataFrame([_make_row()])
        label = build_fraud_confirmed(df, [])
        assert label.dtype == np.int8

    def test_suspect_dtype_int8(self):
        df = pd.DataFrame([_make_row()])
        label = build_fraud_suspect(df)
        assert label.dtype == np.int8

    def test_confirmed_values_binary(self):
        rows = [_make_row(cik='72971', fiscal_year=2000), _make_row()]
        df = pd.DataFrame(rows)
        labels = build_fraud_confirmed(df, [])
        assert set(labels.unique()).issubset({0, 1})

    def test_no_false_positives_random_cik(self):
        """A random CIK not in any fraud list should never be labeled."""
        df = pd.DataFrame([_make_row(cik='8888888', fiscal_year=y) for y in range(2000, 2025)])
        labels = build_fraud_confirmed(df, [])
        assert (labels == 0).all()

    def test_enforcement_window_constants(self):
        """Verify window constants match documented values."""
        assert ENFORCEMENT_WINDOW_BEFORE == 2
        assert ENFORCEMENT_WINDOW_AFTER == 5


# ═══════════════════════════════════════════════════════════════════════════════
# Confirmed Overrides Suspect (integration)
# ═══════════════════════════════════════════════════════════════════════════════

class TestConfirmedOverridesSuspect:
    """fraud_confirmed=1 must zero out fraud_suspect for same row."""

    def test_confirmed_fraud_not_also_suspect(self):
        """Enron row triggers both confirmed (known CIK) and suspect (low scores).
        After override, suspect must be 0."""
        df = pd.DataFrame([_make_row(
            cik='72971', fiscal_year=2000,  # Enron → confirmed
            beneish_m_score=-1.0,           # fires
            piotroski_f_score=1.0,          # fires → would be suspect
        )])
        df['fraud_confirmed'] = build_fraud_confirmed(df, [])
        df['fraud_suspect'] = build_fraud_suspect(df)
        # Apply the override logic from run()
        df.loc[df['fraud_confirmed'] == 1, 'fraud_suspect'] = 0
        assert df.loc[0, 'fraud_confirmed'] == 1
        assert df.loc[0, 'fraud_suspect'] == 0

    def test_non_confirmed_suspect_kept(self):
        """Non-confirmed row with 2+ signals stays suspect."""
        df = pd.DataFrame([_make_row(
            cik='999999', fiscal_year=2015,
            beneish_m_score=-1.0,
            piotroski_f_score=1.0,
        )])
        df['fraud_confirmed'] = build_fraud_confirmed(df, [])
        df['fraud_suspect'] = build_fraud_suspect(df)
        df.loc[df['fraud_confirmed'] == 1, 'fraud_suspect'] = 0
        assert df.loc[0, 'fraud_confirmed'] == 0
        assert df.loc[0, 'fraud_suspect'] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Label Not Used as Feature (cross-module guard)
# ═══════════════════════════════════════════════════════════════════════════════

class TestLabelNotFeature:
    """fraud_confirmed must be excluded from ML features."""

    def test_fraud_confirmed_in_train_models_exclude_set(self):
        """train_models.py EXCLUDE set must contain fraud_confirmed."""
        from pathlib import Path

        source = (Path(__file__).parent.parent.parent / 'scripts' / 'train_models.py').read_text()
        # Find the EXCLUDE set definition (large set literal spanning many lines)
        in_exclude = False
        found = False
        for line in source.splitlines():
            if 'EXCLUDE' in line and '=' in line and '{' in line:
                in_exclude = True
            if in_exclude and 'fraud_confirmed' in line:
                found = True
                break
            if in_exclude and '}' in line:
                break
        assert found, "fraud_confirmed not found in EXCLUDE set in train_models.py"


# ═══════════════════════════════════════════════════════════════════════════════
# Idempotency
# ═══════════════════════════════════════════════════════════════════════════════

class TestIdempotency:
    """Running twice produces same result."""

    def test_confirmed_idempotent(self):
        df = pd.DataFrame([_make_row(cik='72971', fiscal_year=2000), _make_row()])
        aaer = [{'cik': '123456', 'year': 2015}]
        r1 = build_fraud_confirmed(df, aaer)
        r2 = build_fraud_confirmed(df, aaer)
        pd.testing.assert_series_equal(r1, r2)

    def test_suspect_idempotent(self):
        df = pd.DataFrame([_make_row(beneish_m_score=-1.0, piotroski_f_score=1.0)])
        r1 = build_fraud_suspect(df)
        r2 = build_fraud_suspect(df)
        pd.testing.assert_series_equal(r1, r2)
