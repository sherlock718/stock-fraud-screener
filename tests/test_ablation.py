"""
Smoke test for research/ablation.py — verifies the module imports and core logic works
on a minimal synthetic dataset without running the full 2-hour ablation.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.ablation import extract_metrics, run_single_backtest


def test_extract_metrics_from_valid_result():
    result = {'n_years': 10, 'sharpe': 1.37, 'cagr_pct': 39.4, 'hit_rate_pct': 88.3}
    m = extract_metrics(result)
    assert m['sharpe'] == 1.37
    assert m['cagr_pct'] == 39.4
    assert m['hit_rate_pct'] == 88.3


def test_extract_metrics_from_empty_result():
    result = {'n_years': 0}
    m = extract_metrics(result)
    assert m['sharpe'] is None
    assert m['cagr_pct'] is None
    assert m['hit_rate_pct'] is None


def test_extract_metrics_missing_n_years():
    result = {}
    m = extract_metrics(result)
    assert m['sharpe'] is None
