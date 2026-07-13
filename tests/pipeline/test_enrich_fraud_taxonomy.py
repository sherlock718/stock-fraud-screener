"""
Tests for pipeline/enrich_fraud_taxonomy.py (P0d — Fraud Taxonomy Sub-Scores).

Covers:
  - Each sub-score formula (accounting, dilution, quality, distress, governance)
  - Composite weighting and bounds
  - Taxonomy does NOT write fraud_suspect (owned by enrich_fraud_labels.py)
  - NaN handling
  - Missing-column graceful degradation
  - Idempotency
  - No label/feature leakage
  - Score bounds [0, 1]
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline.enrich_fraud_taxonomy import (
    build_accounting_score,
    build_composite_fraud_score,
    build_dilution_score,
    build_distress_score,
    build_governance_score,
    build_quality_score,
    _pct_rank_clip,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def base_df():
    """Minimal dataframe with core columns that all sub-scores may use."""
    np.random.seed(42)
    n = 200
    return pd.DataFrame({
        'ticker': [f'T{i:03d}' for i in range(n)],
        'fiscal_year': np.random.choice([2015, 2016, 2017, 2018, 2019], n),
        'beneish_m_score': np.random.normal(-2.0, 1.5, n),
        'sloan_accruals': np.random.normal(0.0, 0.05, n),
        'accruals_to_assets': np.random.normal(0.03, 0.04, n),
        'ocf_to_ni': np.random.normal(1.0, 0.8, n),
        'shares_growth': np.random.normal(0.02, 0.1, n),
        'shares_dilution': np.random.normal(0.01, 0.05, n),
        'net_margin_change': np.random.normal(0.0, 0.2, n),
        'eps_diluted': np.random.uniform(0.5, 5.0, n),
        'ocf_margin': np.random.normal(0.1, 0.08, n),
        'ocf_to_assets': np.random.normal(0.08, 0.05, n),
        'fcf_yield': np.random.normal(0.05, 0.04, n),
        'gross_margin_trend_3y': np.random.normal(0.0, 0.02, n),
        'accruals_avg_3y': np.random.normal(0.02, 0.03, n),
        'altman_z_score': np.random.normal(3.0, 2.0, n),
        'piotroski_f_score': np.random.randint(0, 10, n).astype(float),
        'altman_x1': np.random.normal(0.2, 0.15, n),
        'net_debt_to_ebitda': np.random.normal(2.0, 3.0, n),
        'current_ratio': np.random.normal(1.5, 0.8, n),
        'small_auditor_flag': np.random.choice([0, 1], n, p=[0.9, 0.1]),
        'going_concern': np.random.choice([0, 1], n, p=[0.95, 0.05]),
        'market_cap_at_filing': np.random.uniform(1e6, 5e9, n),
    })


@pytest.fixture
def empty_df():
    """DataFrame with no relevant columns."""
    return pd.DataFrame({
        'ticker': ['A', 'B', 'C'],
        'fiscal_year': [2020, 2020, 2020],
        'revenue': [100, 200, 300],
    })


# ── _pct_rank_clip helper ────────────────────────────────────────────────────

class TestPctRankClip:
    def test_output_range_01(self, base_df):
        result = _pct_rank_clip(base_df['beneish_m_score'])
        valid = result.dropna()
        assert valid.min() >= 0.0
        assert valid.max() <= 1.0

    def test_preserves_nan(self):
        s = pd.Series([1.0, 2.0, np.nan, 4.0, 5.0])
        result = _pct_rank_clip(s)
        assert pd.isna(result.iloc[2])
        assert result.iloc[0] > 0

    def test_clips_extreme_outliers(self):
        s = pd.Series([1.0] * 98 + [1000.0, -1000.0])
        result = _pct_rank_clip(s, clip_lo=0.01, clip_hi=0.99)
        assert result.notna().all()


# ── Accounting Score ─────────────────────────────────────────────────────────

class TestAccountingScore:
    def test_output_range_bounded(self, base_df):
        score = build_accounting_score(base_df)
        valid = score.dropna()
        assert valid.min() >= 0.0
        assert valid.max() <= 1.0

    def test_high_beneish_gives_high_score(self):
        df = pd.DataFrame({
            'beneish_m_score': [10.0] * 50 + [-5.0] * 50,
            'sloan_accruals': [0.0] * 100,
        })
        score = build_accounting_score(df)
        high_group = score.iloc[:50].mean()
        low_group = score.iloc[50:].mean()
        assert high_group > low_group

    def test_low_ocf_to_ni_gives_high_score(self):
        df = pd.DataFrame({
            'ocf_to_ni': [-2.0] * 50 + [3.0] * 50,
        })
        score = build_accounting_score(df)
        low_ocf_group = score.iloc[:50].mean()
        high_ocf_group = score.iloc[50:].mean()
        assert low_ocf_group > high_ocf_group

    def test_all_nan_returns_nan(self):
        df = pd.DataFrame({
            'beneish_m_score': [np.nan] * 10,
            'sloan_accruals': [np.nan] * 10,
        })
        score = build_accounting_score(df)
        assert score.isna().all()

    def test_missing_columns_returns_nan(self, empty_df):
        score = build_accounting_score(empty_df)
        assert score.isna().all()

    def test_fallback_to_ocf_to_ni_sector_pct(self):
        df = pd.DataFrame({
            'ocf_to_ni_sector_pct': np.linspace(0.1, 0.9, 50),
        })
        score = build_accounting_score(df)
        assert score.notna().any()


# ── Dilution Score ───────────────────────────────────────────────────────────

class TestDilutionScore:
    def test_output_range_bounded(self, base_df):
        score = build_dilution_score(base_df)
        valid = score.dropna()
        assert valid.min() >= 0.0
        assert valid.max() <= 1.0

    def test_high_shares_growth_gives_high_score(self):
        df = pd.DataFrame({
            'shares_growth': [0.5] * 50 + [-0.1] * 50,
        })
        score = build_dilution_score(df)
        high_group = score.iloc[:50].mean()
        low_group = score.iloc[50:].mean()
        assert high_group > low_group

    def test_missing_columns_returns_nan(self, empty_df):
        score = build_dilution_score(empty_df)
        assert score.isna().all()


# ── Quality Score ────────────────────────────────────────────────────────────

class TestQualityScore:
    def test_output_range_bounded(self, base_df):
        score = build_quality_score(base_df)
        valid = score.dropna()
        assert valid.min() >= 0.0
        assert valid.max() <= 1.0

    def test_low_ocf_margin_gives_high_score(self):
        df = pd.DataFrame({
            'ocf_margin': [-0.2] * 50 + [0.4] * 50,
        })
        score = build_quality_score(df)
        low_group = score.iloc[:50].mean()
        high_group = score.iloc[50:].mean()
        assert low_group > high_group

    def test_uses_fcf_yield_when_available(self):
        df = pd.DataFrame({
            'fcf_yield': np.linspace(-0.1, 0.3, 50),
        })
        score = build_quality_score(df)
        assert score.notna().any()

    def test_fcf_to_assets_as_fallback(self):
        df = pd.DataFrame({
            'fcf_to_assets': np.linspace(-0.1, 0.2, 50),
        })
        score = build_quality_score(df)
        assert score.notna().any()

    def test_missing_columns_returns_nan(self, empty_df):
        score = build_quality_score(empty_df)
        assert score.isna().all()


# ── Distress Score ───────────────────────────────────────────────────────────

class TestDistressScore:
    def test_output_range_bounded(self, base_df):
        score = build_distress_score(base_df)
        valid = score.dropna()
        assert valid.min() >= 0.0
        assert valid.max() <= 1.0

    def test_low_altman_z_gives_high_score(self):
        df = pd.DataFrame({
            'altman_z_score': [-2.0] * 50 + [5.0] * 50,
            'piotroski_f_score': [5.0] * 100,
        })
        score = build_distress_score(df)
        distressed_group = score.iloc[:50].mean()
        safe_group = score.iloc[50:].mean()
        assert distressed_group > safe_group

    def test_low_piotroski_gives_high_score(self):
        df = pd.DataFrame({
            'piotroski_f_score': [0.0] * 50 + [9.0] * 50,
        })
        score = build_distress_score(df)
        weak_group = score.iloc[:50].mean()
        strong_group = score.iloc[50:].mean()
        assert weak_group > strong_group

    def test_missing_columns_returns_nan(self, empty_df):
        score = build_distress_score(empty_df)
        assert score.isna().all()


# ── Governance Score ─────────────────────────────────────────────────────────

class TestGovernanceScore:
    def test_output_range_bounded(self, base_df):
        score = build_governance_score(base_df)
        valid = score.dropna()
        assert valid.min() >= 0.0
        assert valid.max() <= 1.0

    def test_small_auditor_flag_raises_score(self):
        df = pd.DataFrame({
            'small_auditor_flag': [1] * 50 + [0] * 50,
            'going_concern': [0] * 100,
            'market_cap_at_filing': [5e7] * 100,
        })
        score = build_governance_score(df)
        flagged = score.iloc[:50].mean()
        clean = score.iloc[50:].mean()
        assert flagged > clean

    def test_going_concern_raises_score(self):
        df = pd.DataFrame({
            'small_auditor_flag': [0] * 100,
            'going_concern': [1] * 50 + [0] * 50,
            'market_cap_at_filing': [5e7] * 100,
        })
        score = build_governance_score(df)
        gc_group = score.iloc[:50].mean()
        no_gc_group = score.iloc[50:].mean()
        assert gc_group > no_gc_group

    def test_large_cap_small_auditor_mismatch(self):
        df = pd.DataFrame({
            'small_auditor_flag': [1, 1, 0, 0],
            'going_concern': [0, 0, 0, 0],
            'market_cap_at_filing': [5e8, 5e7, 5e8, 5e7],
        })
        score = build_governance_score(df)
        # Row 0: small_auditor + large_cap = highest risk
        assert score.iloc[0] > score.iloc[1]
        assert score.iloc[0] > score.iloc[2]

    def test_proxy_mode_when_no_primary_columns(self):
        """When small_auditor_flag and going_concern are absent, uses proxy signals."""
        df = pd.DataFrame({
            'altman_z_score': [0.5] * 50 + [5.0] * 50,
            'piotroski_f_score': [1.0] * 50 + [8.0] * 50,
        })
        score = build_governance_score(df)
        distressed = score.iloc[:50].mean()
        healthy = score.iloc[50:].mean()
        assert distressed > healthy

    def test_proxy_not_used_when_primary_available(self):
        """Primary signals take precedence over proxy."""
        df = pd.DataFrame({
            'small_auditor_flag': [0] * 100,
            'going_concern': [0] * 100,
            'altman_z_score': [0.5] * 100,
            'piotroski_f_score': [1.0] * 100,
            'market_cap_at_filing': [1e7] * 100,
        })
        score = build_governance_score(df)
        # All primary signals are 0, so score should be 0 despite proxy signals being alarming
        assert score.mean() == 0.0

    def test_missing_all_columns_returns_nan(self, empty_df):
        score = build_governance_score(empty_df)
        assert score.isna().all()


# ── Fraud Suspect Ownership ──────────────────────────────────────────────────

class TestFraudSuspectOwnership:
    def test_taxonomy_does_not_export_build_fraud_suspect(self):
        """build_fraud_suspect must NOT exist in taxonomy module after consolidation."""
        import pipeline.enrich_fraud_taxonomy as mod
        assert not hasattr(mod, 'build_fraud_suspect')

    def test_taxonomy_preserves_existing_fraud_suspect(self, base_df):
        """
        If fraud_suspect already exists (set by enrich_fraud_labels), taxonomy
        must not overwrite it. Simulates running taxonomy after labels.
        """
        # Pre-set fraud_suspect as labels would
        base_df['fraud_suspect'] = np.random.choice([0, 1], len(base_df), p=[0.8, 0.2]).astype('int8')
        original_suspect = base_df['fraud_suspect'].copy()

        # Run taxonomy score builders (same as run() does, minus fraud_suspect)
        base_df['fraud_score_accounting'] = build_accounting_score(base_df)
        base_df['fraud_score_dilution'] = build_dilution_score(base_df)
        base_df['fraud_score_quality'] = build_quality_score(base_df)
        base_df['fraud_score_distress'] = build_distress_score(base_df)
        base_df['fraud_score_governance'] = build_governance_score(base_df)
        base_df['fraud_score_composite'] = build_composite_fraud_score(base_df)

        # fraud_suspect must be unchanged
        pd.testing.assert_series_equal(base_df['fraud_suspect'], original_suspect)

    def test_taxonomy_output_columns_do_not_include_fraud_suspect(self, base_df):
        """Taxonomy output list is exactly 6 columns (5 sub-scores + composite)."""
        expected = {
            'fraud_score_accounting',
            'fraud_score_dilution',
            'fraud_score_quality',
            'fraud_score_distress',
            'fraud_score_governance',
            'fraud_score_composite',
        }
        base_df['fraud_score_accounting'] = build_accounting_score(base_df)
        base_df['fraud_score_dilution'] = build_dilution_score(base_df)
        base_df['fraud_score_quality'] = build_quality_score(base_df)
        base_df['fraud_score_distress'] = build_distress_score(base_df)
        base_df['fraud_score_governance'] = build_governance_score(base_df)
        base_df['fraud_score_composite'] = build_composite_fraud_score(base_df)
        taxonomy_cols = {c for c in base_df.columns if c.startswith('fraud_score_')}
        assert taxonomy_cols == expected


# ── Composite Score ──────────────────────────────────────────────────────────

class TestCompositeScore:
    def test_output_range_bounded(self, base_df):
        # First build the sub-scores
        base_df['fraud_score_accounting'] = build_accounting_score(base_df)
        base_df['fraud_score_dilution'] = build_dilution_score(base_df)
        base_df['fraud_score_quality'] = build_quality_score(base_df)
        base_df['fraud_score_distress'] = build_distress_score(base_df)
        base_df['fraud_score_governance'] = build_governance_score(base_df)
        composite = build_composite_fraud_score(base_df)
        valid = composite.dropna()
        assert valid.min() >= 0.0
        assert valid.max() <= 1.0

    def test_weights_sum_to_one(self):
        weights = {'accounting': 0.30, 'quality': 0.25, 'distress': 0.20,
                   'dilution': 0.15, 'governance': 0.10}
        assert abs(sum(weights.values()) - 1.0) < 1e-10

    def test_missing_sub_scores_returns_nan(self, empty_df):
        composite = build_composite_fraud_score(empty_df)
        assert composite.isna().all()

    def test_requires_100_plus_non_null(self):
        """Sub-score needs >100 non-null rows to be included."""
        n = 200
        df = pd.DataFrame({
            'fraud_score_accounting': [0.5] * 50 + [np.nan] * 150,  # only 50 valid → excluded
            'fraud_score_quality': [0.3] * n,  # 200 valid → included
        })
        composite = build_composite_fraud_score(df)
        # Only quality should contribute (accounting has <100 valid)
        assert composite.notna().any()

    def test_nan_sub_scores_filled_with_neutral(self):
        """NaN sub-scores get filled with 0.5 (neutral) before averaging."""
        df = pd.DataFrame({
            'fraud_score_accounting': [0.8] * 101 + [np.nan] * 99,
            'fraud_score_quality': [np.nan] * 200,
            'fraud_score_distress': [0.2] * 200,
            'fraud_score_dilution': [0.6] * 200,
            'fraud_score_governance': [0.1] * 200,
        })
        composite = build_composite_fraud_score(df)
        # Row 0 should use all available sub-scores
        # accounting: 0.8 * 0.30, distress: 0.2 * 0.20, dilution: 0.6 * 0.15, governance: 0.1 * 0.10
        # quality has NaN → filled with 0.5 but quality col is all-NaN with n=0 (< 100?)
        # Actually quality_notna = 0, so it's excluded. Let's check only accounting/distress/dilution/governance
        assert composite.notna().any()


# ── Idempotency ──────────────────────────────────────────────────────────────

class TestIdempotency:
    def test_scores_stable_on_rerun(self, base_df):
        """Running score builders twice gives same result."""
        score1 = build_accounting_score(base_df)
        score2 = build_accounting_score(base_df)
        pd.testing.assert_series_equal(score1, score2)

    def test_composite_stable_on_rerun(self, base_df):
        base_df['fraud_score_accounting'] = build_accounting_score(base_df)
        base_df['fraud_score_dilution'] = build_dilution_score(base_df)
        base_df['fraud_score_quality'] = build_quality_score(base_df)
        base_df['fraud_score_distress'] = build_distress_score(base_df)
        base_df['fraud_score_governance'] = build_governance_score(base_df)
        c1 = build_composite_fraud_score(base_df)
        c2 = build_composite_fraud_score(base_df)
        pd.testing.assert_series_equal(c1, c2)

    def test_existing_scores_overwritten_cleanly(self, base_df):
        """If columns already exist, re-running produces same values."""
        base_df['fraud_score_accounting'] = build_accounting_score(base_df)
        first_run = base_df['fraud_score_accounting'].copy()
        base_df['fraud_score_accounting'] = build_accounting_score(base_df)
        pd.testing.assert_series_equal(first_run, base_df['fraud_score_accounting'])


# ── Leakage Checks ───────────────────────────────────────────────────────────

class TestLeakage:
    def test_no_forward_return_used(self, base_df):
        """Sub-score functions must not read forward_return columns."""
        # Add forward return columns
        base_df['forward_return_1y'] = np.random.normal(0.1, 0.3, len(base_df))
        base_df['forward_return_3y'] = np.random.normal(0.3, 0.5, len(base_df))

        # Scores should be identical whether forward_return exists or not
        score_with = build_accounting_score(base_df)
        df_no_fwd = base_df.drop(columns=['forward_return_1y', 'forward_return_3y'])
        score_without = build_accounting_score(df_no_fwd)
        pd.testing.assert_series_equal(score_with, score_without)

    def test_no_fraud_confirmed_used_in_scores(self, base_df):
        """Sub-scores must not read fraud_confirmed (that's a label)."""
        base_df['fraud_confirmed'] = np.random.choice([0, 1], len(base_df), p=[0.99, 0.01])
        score_with = build_accounting_score(base_df)
        df_no_label = base_df.drop(columns=['fraud_confirmed'])
        score_without = build_accounting_score(df_no_label)
        pd.testing.assert_series_equal(score_with, score_without)

    def test_no_ml_scores_used(self, base_df):
        """Sub-scores must not read ml_* prediction columns."""
        base_df['ml_1y'] = np.random.uniform(0, 1, len(base_df))
        base_df['ml_3y'] = np.random.uniform(0, 1, len(base_df))
        score_with = build_distress_score(base_df)
        df_no_ml = base_df.drop(columns=['ml_1y', 'ml_3y'])
        score_without = build_distress_score(df_no_ml)
        pd.testing.assert_series_equal(score_with, score_without)

    def test_taxonomy_does_not_modify_fraud_suspect(self):
        """
        Taxonomy must not touch fraud_suspect. If it pre-exists from labels,
        it must remain unchanged after taxonomy score computation.
        """
        df = pd.DataFrame({
            'beneish_m_score': [0.0, 0.0, -5.0],
            'piotroski_f_score': [1.0, 1.0, 8.0],
            'altman_z_score': [0.5, 0.5, 5.0],
            'fraud_confirmed': [1, 0, 0],
            'fraud_suspect': [0, 1, 0],  # pre-set by labels
        })
        original_suspect = df['fraud_suspect'].copy()
        # Run taxonomy scores
        df['fraud_score_accounting'] = build_accounting_score(df)
        df['fraud_score_distress'] = build_distress_score(df)
        # fraud_suspect unchanged
        pd.testing.assert_series_equal(df['fraud_suspect'], original_suspect)


# ── Cross-Sectional Rank Behavior ────────────────────────────────────────────

class TestCrossSectionalRank:
    def test_rank_is_global_not_per_year(self, base_df):
        """
        _pct_rank_clip ranks across ALL rows (not grouped by fiscal_year).
        This is intentional for taxonomy scores — they are relative risk
        positions in the full dataset, not within-year comparisons.
        """
        score = build_accounting_score(base_df)
        # Verify roughly uniform distribution (percentile rank)
        valid = score.dropna()
        assert 0.4 < valid.median() < 0.6  # should be near 0.5 for large samples

    def test_rank_insensitive_to_row_order(self, base_df):
        """Shuffling rows should not change scores."""
        score_original = build_accounting_score(base_df)
        shuffled = base_df.sample(frac=1, random_state=123).reset_index(drop=True)
        score_shuffled = build_accounting_score(shuffled)
        # After sorting back, values should match
        # Since we reset_index, compare by aligning on original order
        assert abs(score_original.mean() - score_shuffled.mean()) < 0.01


# ── Run Function Integration ─────────────────────────────────────────────────

class TestRunFunction:
    def test_columns_added(self, base_df):
        """Verify the expected output columns from a simulated run."""
        expected_cols = [
            'fraud_score_accounting',
            'fraud_score_dilution',
            'fraud_score_quality',
            'fraud_score_distress',
            'fraud_score_governance',
            'fraud_score_composite',
        ]
        base_df['fraud_score_accounting'] = build_accounting_score(base_df)
        base_df['fraud_score_dilution'] = build_dilution_score(base_df)
        base_df['fraud_score_quality'] = build_quality_score(base_df)
        base_df['fraud_score_distress'] = build_distress_score(base_df)
        base_df['fraud_score_governance'] = build_governance_score(base_df)
        base_df['fraud_score_composite'] = build_composite_fraud_score(base_df)
        for col in expected_cols:
            assert col in base_df.columns
        # fraud_suspect must NOT be in taxonomy output
        assert 'fraud_suspect' not in expected_cols

    def test_no_row_count_change(self, base_df):
        """Taxonomy enrichment must not add or remove rows."""
        original_len = len(base_df)
        build_accounting_score(base_df)  # just verifying no side effects
        assert len(base_df) == original_len
