"""Unit tests for backtest/engine.py — gate logic in filter_composite(mode='ml_gates')."""

import numpy as np
import pandas as pd
import pytest

from backtest.engine import (
    ENGINE_REQUIREMENTS,
    _apply_asof_historical_gates,
    filter_composite,
    run_backtest,
)
from modeling.prediction_lineage import add_synthetic_manifest
from modeling.constants import (
    TREE_THRESHOLD, PIOTROSKI_MIN, VALUE_GATE_PCT,
    ALTMAN_Z_MIN, BENEISH_THRESHOLD, MAX_MARKET_CAP_PROD,
)


def _make_df(n=10, **overrides):
    """Build a minimal DataFrame for ml_gates filter testing.

    Includes complete synthetic OOS manifests for both required ml_gates roles.
    """
    data = {
        "fiscal_year": [2020] * n,
        "market": ["US"] * n,
        "ticker": [f"T{i}" for i in range(n)],
        "forward_return_1y": np.linspace(0.05, 0.50, n),
        "market_cap_at_filing": [2e9] * n,
        "piotroski_f_score": [6.0] * n,
        "piotroski_roa_pos": [1.0] * n,
        "ps_ratio_sector_pct": [0.4] * n,
        "tree_prob": [0.7] * n,
        "altman_z_score": [3.0] * n,
        "beneish_m_score": [-3.0] * n,
        "reg_3y_wf": np.linspace(0.1, 1.0, n),
        "ml_3y_wf": np.linspace(0.3, 0.9, n),
        "ml_3y": np.linspace(0.3, 0.9, n),
        "likely_delisted": [0] * n,
    }
    data.update(overrides)
    return add_synthetic_manifest(
        pd.DataFrame(data), ENGINE_REQUIREMENTS["ml_gates"]
    )


class TestMaxCapGate:
    def test_max_cap_excludes_large_stocks(self):
        df = _make_df(10, market_cap_at_filing=[15e9] * 5 + [2e9] * 5)
        yr_df = df.copy()
        if MAX_MARKET_CAP_PROD > 0:
            yr_df = yr_df[yr_df["market_cap_at_filing"] <= MAX_MARKET_CAP_PROD]
        idx = filter_composite(yr_df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert selected.empty
        assert yr_df.attrs["historical_score_coverage"]["period_exclusion_reason"] == "insufficient_valid_score_coverage"

    def test_min_cap_excludes_micro_stocks(self):
        df = _make_df(10, market_cap_at_filing=[10e6] * 5 + [500e6] * 5)
        min_cap = 50_000_000
        yr_df = df[df["market_cap_at_filing"] >= min_cap]
        idx = filter_composite(yr_df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert selected.empty


class TestValueGate:
    def test_value_gate_excludes_overpriced(self):
        pcts = [0.3, 0.4, 0.5, 0.6, 0.65, 0.69, 0.71, 0.8, 0.9, 0.95]
        df = _make_df(10, ps_ratio_sector_pct=pcts)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert selected.empty


class TestTreeThreshold:
    def test_tree_threshold_filters_low_confidence(self):
        probs = [0.1, 0.2, 0.3, 0.4, 0.5, 0.54, 0.56, 0.7, 0.8, 0.9]
        df = _make_df(10, tree_prob=probs)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert selected.empty


class TestAltmanZGate:
    def test_altman_z_gate_excludes_distressed(self):
        scores = [0.2, 0.5, 0.8, 0.99, 1.01, 1.5, 2.0, 2.5, 3.0, 4.0]
        df = _make_df(10, altman_z_score=scores)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert selected.empty


class TestRegressionRanking:
    def test_regression_ranking_uses_reg_3y_wf(self):
        reg_scores = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        df = _make_df(10, reg_3y_wf=reg_scores)
        idx = filter_composite(df, top_n=5, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert len(selected) == 5
        assert selected["reg_3y_wf"].min() >= 0.6


class TestHistoricalDisappearanceGate:
    def test_future_derived_likely_delisted_cannot_change_selection(self):
        base = _make_df(10, likely_delisted=[0] * 10)
        changed = base.copy()
        changed["likely_delisted"] = 1
        original = filter_composite(base, top_n=5, market=None, mode="ml_gates")
        future_reclassified = filter_composite(
            changed, top_n=5, market=None, mode="ml_gates"
        )
        assert original.tolist() == future_reclassified.tolist()

    def test_appending_future_rows_cannot_change_asof_gate_eligibility(self):
        historical = pd.DataFrame({
            "ticker": ["HIST"],
            "asof_listing_eligible": [True],
            "asof_listing_eligible_timestamp": ["2020-12-31"],
            "asof_listing_eligible_source": ["exchange-snapshot"],
            "likely_delisted": [False],
        })
        future = pd.DataFrame({
            "ticker": ["FUTURE"],
            "asof_listing_eligible": [False],
            "asof_listing_eligible_timestamp": ["2025-12-31"],
            "asof_listing_eligible_source": ["exchange-snapshot"],
            "likely_delisted": [True],
        }, index=[1])
        before = _apply_asof_historical_gates(
            historical, pd.Timestamp("2021-01-01")
        )
        after = _apply_asof_historical_gates(
            pd.concat([historical, future]), pd.Timestamp("2021-01-01")
        )
        assert before.index.tolist() == [0]
        assert 0 in after.index

    def test_asof_gate_missing_provenance_fails_closed(self):
        row = pd.DataFrame({
            "asof_quote_recent": [True],
            "asof_quote_recent_timestamp": ["2020-12-31"],
        })
        result = _apply_asof_historical_gates(row, pd.Timestamp("2021-01-01"))
        assert result.empty


class TestCleanTrainingFilter:
    def test_beneish_gate_excludes_manipulators(self):
        scores = [-3.0, -2.5, -2.0, -1.78, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0]
        df = _make_df(10, beneish_m_score=scores)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert selected.empty


class TestPiotroskiGate:
    def test_piotroski_gate_filters_low_quality(self):
        scores = [0.0, 1.0, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        df = _make_df(10, piotroski_f_score=scores)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert selected.empty

    def test_roa_pos_gate(self):
        roa = [0, 0, 0, 1, 1, 1, 1, 1, 1, 1]
        df = _make_df(10, piotroski_roa_pos=roa)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert selected.empty
