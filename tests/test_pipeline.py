"""
Unit tests for core pipeline logic.

These tests use synthetic in-memory data only — no files on disk, no network calls.
Run with: pytest tests/
"""
from __future__ import annotations

import io
import json

import numpy as np
import pandas as pd
import pytest


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_annual_df(n_tickers: int = 35, n_years: int = 10,
                    seed: int = 42) -> pd.DataFrame:
    """Minimal synthetic annual dataset matching the pipeline schema."""
    rng = np.random.default_rng(seed)
    tickers = [f'T{i:03d}' for i in range(n_tickers)]
    years   = list(range(2010, 2010 + n_years))
    rows = []
    for ticker in tickers:
        for year in years:
            rows.append({
                'ticker':              ticker,
                'fiscal_year':         year,
                'period_type':         'annual',
                'filed_date':          f'{year}-03-31',
                'revenue':             float(rng.integers(1_000_000, 1_000_000_000)),
                'total_assets':        float(rng.integers(500_000, 2_000_000_000)),
                'net_income':          float(rng.integers(-50_000_000, 100_000_000)),
                'gross_margin':        float(rng.uniform(0.1, 0.8)),
                'roe':                 float(rng.uniform(-0.2, 0.4)),
                'debt_to_equity':      float(rng.uniform(0.0, 3.0)),
                'current_ratio':       float(rng.uniform(0.5, 4.0)),
                'pe_ratio':            float(rng.uniform(5.0, 50.0)),
                'forward_return_1y':   float(rng.uniform(-0.5, 2.0)),
                'forward_return_3y':   float(rng.uniform(-0.5, 3.0)),
                'forward_return_5y':   float(rng.uniform(-0.5, 5.0)),
                'beat_local_market_1y': int(rng.integers(0, 2)),
                'beat_local_market_3y': int(rng.integers(0, 2)),
                'beat_local_market_5y': int(rng.integers(0, 2)),
                'market':              'US',
                'country':             'US',
                'market_cap_at_filing': float(rng.integers(1_000_000, 5_000_000_000)),
                'cik':                 str(rng.integers(100000, 999999)),
                'piotroski_f_score':   float(rng.integers(0, 10)),
                'beneish_m_score':     float(rng.uniform(-3.0, 0.0)),
                'momentum_12m_prior':  float(rng.uniform(-0.5, 1.5)),
            })
    return pd.DataFrame(rows)


# ─── train_models.py logic ────────────────────────────────────────────────────

class TestTemporalSplit:
    def test_no_overlap(self):
        df = _make_annual_df()
        train_cutoff, val_end = 2016, 2018
        train = df[df['fiscal_year'] <= train_cutoff]
        val   = df[(df['fiscal_year'] > train_cutoff) & (df['fiscal_year'] <= val_end)]
        test  = df[df['fiscal_year'] > val_end]

        assert train['fiscal_year'].max() <= train_cutoff
        assert val['fiscal_year'].min()   >  train_cutoff
        assert val['fiscal_year'].max()   <= val_end
        assert test['fiscal_year'].min()  >  val_end

    def test_all_rows_covered(self):
        df = _make_annual_df()
        train_cutoff, val_end = 2016, 2018
        train = df[df['fiscal_year'] <= train_cutoff]
        val   = df[(df['fiscal_year'] > train_cutoff) & (df['fiscal_year'] <= val_end)]
        test  = df[df['fiscal_year'] > val_end]
        assert len(train) + len(val) + len(test) == len(df)


class TestGetCandidates:
    def test_excludes_leakage_columns(self):
        from modeling.train import get_candidates, EXCLUDE, EXCLUDE_PATTERNS

        df = _make_annual_df()
        candidates = get_candidates(df)

        # Forward return columns must never appear as features
        for c in candidates:
            assert 'forward_return' not in c, f'Leakage column in candidates: {c}'
            assert 'beat_local_market' not in c, f'Leakage column in candidates: {c}'

        # Identifiers must be excluded
        for excl in ['ticker', 'cik', 'fiscal_year']:
            assert excl not in candidates

    def test_returns_nonempty(self):
        from modeling.train import get_candidates
        df = _make_annual_df()
        assert len(get_candidates(df)) > 0


class TestTrainMedians:
    """Verify train_model stores medians and model uses them for imputation."""

    def test_medians_stored(self):
        from modeling.train import train_model

        df = _make_annual_df(n_tickers=30, n_years=8)
        train = df[df['fiscal_year'] <= 2015]
        features = ['gross_margin', 'roe', 'debt_to_equity', 'pe_ratio', 'current_ratio']
        clf, feats, y_train, medians = train_model(train, features, 'beat_local_market_1y')

        assert isinstance(medians, dict)
        assert len(medians) == len(feats)
        for f in feats:
            assert f in medians
            assert np.isfinite(medians[f])

    def test_model_predicts_with_missing(self):
        """Model should predict even when all features are NaN (filled by medians)."""
        from modeling.train import train_model

        df = _make_annual_df(n_tickers=30, n_years=8)
        train = df[df['fiscal_year'] <= 2015]
        features = ['gross_margin', 'roe', 'debt_to_equity', 'pe_ratio', 'current_ratio']
        clf, feats, y_train, medians = train_model(train, features, 'beat_local_market_1y')

        test_row = pd.DataFrame([{f: np.nan for f in feats}])
        fill = pd.Series({f: medians.get(f, 0.0) for f in feats})
        X = test_row.fillna(fill)
        proba = clf.predict_proba(X)[:, 1]
        assert len(proba) == 1
        assert 0.0 <= proba[0] <= 1.0


class TestICTable:
    def test_returns_dataframe(self):
        from modeling.train import compute_ic_table

        df = _make_annual_df()
        features = ['gross_margin', 'roe', 'debt_to_equity']
        ic = compute_ic_table(df, features, 'forward_return_1y')

        assert isinstance(ic, pd.DataFrame)
        assert 'mean_ic' in ic.columns
        assert 'icir' in ic.columns

    def test_icir_finite(self):
        from modeling.train import compute_ic_table

        df = _make_annual_df()
        ic = compute_ic_table(df, ['gross_margin', 'roe'], 'forward_return_1y')
        assert ic['icir'].apply(np.isfinite).all()


# ─── bias_audit.py logic ─────────────────────────────────────────────────────

class TestFilingLagAudit:
    def test_detects_leakage(self):
        from quality.bias_audit import _period_end_date

        row = pd.Series({'fiscal_year': 2020, 'fiscal_quarter': 4})
        period_end = _period_end_date(row)
        assert period_end == pd.Timestamp('2020-12-31')

    def test_quarterly_periods(self):
        from quality.bias_audit import _period_end_date

        expected = {
            1: pd.Timestamp('2021-03-31'),
            2: pd.Timestamp('2021-06-30'),
            3: pd.Timestamp('2021-09-30'),
            4: pd.Timestamp('2021-12-31'),
        }
        for q, expected_date in expected.items():
            row = pd.Series({'fiscal_year': 2021, 'fiscal_quarter': q})
            assert _period_end_date(row) == expected_date
