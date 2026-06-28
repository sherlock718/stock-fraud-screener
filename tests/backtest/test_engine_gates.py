"""Unit tests for backtest/engine.py — gate logic in filter_composite(mode='ml_gates')."""

import numpy as np
import pandas as pd
import pytest

from backtest.engine import filter_composite, run_backtest
from modeling.constants import (
    TREE_THRESHOLD, PIOTROSKI_MIN, VALUE_GATE_PCT,
    ALTMAN_Z_MIN, BENEISH_THRESHOLD, MAX_MARKET_CAP_PROD,
)


def _make_df(n=10, **overrides):
    """Build a minimal DataFrame for ml_gates filter testing.

    Includes ml_3y as fallback column so ranking works even when
    fewer than 6 rows survive gates (engine's >5 threshold for _wf columns).
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
    return pd.DataFrame(data)


class TestMaxCapGate:
    def test_max_cap_excludes_large_stocks(self):
        df = _make_df(10, market_cap_at_filing=[15e9] * 5 + [2e9] * 5)
        yr_df = df.copy()
        if MAX_MARKET_CAP_PROD > 0:
            yr_df = yr_df[yr_df["market_cap_at_filing"] <= MAX_MARKET_CAP_PROD]
        idx = filter_composite(yr_df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert all(selected["market_cap_at_filing"] <= MAX_MARKET_CAP_PROD)

    def test_min_cap_excludes_micro_stocks(self):
        df = _make_df(10, market_cap_at_filing=[10e6] * 5 + [500e6] * 5)
        min_cap = 50_000_000
        yr_df = df[df["market_cap_at_filing"] >= min_cap]
        idx = filter_composite(yr_df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert all(selected["market_cap_at_filing"] >= min_cap)


class TestValueGate:
    def test_value_gate_excludes_overpriced(self):
        pcts = [0.3, 0.4, 0.5, 0.6, 0.65, 0.69, 0.71, 0.8, 0.9, 0.95]
        df = _make_df(10, ps_ratio_sector_pct=pcts)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert all(selected["ps_ratio_sector_pct"] <= VALUE_GATE_PCT)
        assert len(selected) < 10


class TestTreeThreshold:
    def test_tree_threshold_filters_low_confidence(self):
        probs = [0.1, 0.2, 0.3, 0.4, 0.5, 0.54, 0.56, 0.7, 0.8, 0.9]
        df = _make_df(10, tree_prob=probs)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert all(selected["tree_prob"] >= TREE_THRESHOLD)
        n_above = sum(1 for p in probs if p >= TREE_THRESHOLD)
        assert len(selected) == n_above


class TestAltmanZGate:
    def test_altman_z_gate_excludes_distressed(self):
        scores = [0.2, 0.5, 0.8, 0.99, 1.01, 1.5, 2.0, 2.5, 3.0, 4.0]
        df = _make_df(10, altman_z_score=scores)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert all(selected["altman_z_score"] > ALTMAN_Z_MIN)
        n_above = sum(1 for s in scores if s > ALTMAN_Z_MIN)
        assert len(selected) == n_above


class TestRegressionRanking:
    def test_regression_ranking_uses_reg_3y_wf(self):
        reg_scores = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        df = _make_df(10, reg_3y_wf=reg_scores)
        idx = filter_composite(df, top_n=5, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert len(selected) == 5
        assert selected["reg_3y_wf"].min() >= 0.6


class TestCleanTrainingFilter:
    def test_beneish_gate_excludes_manipulators(self):
        scores = [-3.0, -2.5, -2.0, -1.78, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0]
        df = _make_df(10, beneish_m_score=scores)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert all(selected["beneish_m_score"] < BENEISH_THRESHOLD)
        n_clean = sum(1 for s in scores if s < BENEISH_THRESHOLD)
        assert len(selected) == n_clean


class TestPiotroskiGate:
    def test_piotroski_gate_filters_low_quality(self):
        scores = [0.0, 1.0, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        df = _make_df(10, piotroski_f_score=scores)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert all(selected["piotroski_f_score"] >= PIOTROSKI_MIN)
        n_pass = sum(1 for s in scores if s >= PIOTROSKI_MIN)
        assert len(selected) == n_pass

    def test_roa_pos_gate(self):
        roa = [0, 0, 0, 1, 1, 1, 1, 1, 1, 1]
        df = _make_df(10, piotroski_roa_pos=roa)
        idx = filter_composite(df, top_n=10, market=None, mode="ml_gates")
        selected = df.loc[idx]
        assert all(selected["piotroski_roa_pos"] == 1)
