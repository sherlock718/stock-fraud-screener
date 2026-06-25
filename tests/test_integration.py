"""
Full-pipeline integration test on small synthetic data.

Tests the complete pipeline flow: synthetic data → features → selection → model → backtest.
Uses in-memory data only — no disk I/O, no network calls.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def _make_synthetic_dataset(n_tickers: int = 40, n_years: int = 8, seed: int = 99):
    """Build a minimal dataset that exercises the full pipeline."""
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n_tickers):
        ticker = f"SYN{i:03d}"
        cik = str(100000 + i)
        for year in range(2012, 2012 + n_years):
            revenue = float(rng.integers(5_000_000, 2_000_000_000))
            total_assets = float(rng.integers(10_000_000, 5_000_000_000))
            net_income = float(rng.integers(-50_000_000, 200_000_000))
            rows.append({
                "ticker": ticker,
                "cik": cik,
                "fiscal_year": year,
                "period_type": "annual",
                "filed_date": f"{year + 1}-03-15",
                "market": rng.choice(["US", "US", "US", "CA"]),
                "country": "US",
                "market_cap_at_filing": float(rng.integers(50_000_000, 5_000_000_000)),
                "revenue": revenue,
                "total_assets": total_assets,
                "net_income": net_income,
                "gross_profit": revenue * rng.uniform(0.2, 0.7),
                "operating_income": net_income * rng.uniform(0.8, 1.5),
                "operating_cash_flow": net_income * rng.uniform(0.5, 2.0),
                "total_equity": total_assets * rng.uniform(0.2, 0.8),
                "current_assets": total_assets * rng.uniform(0.2, 0.6),
                "current_liabilities": total_assets * rng.uniform(0.1, 0.4),
                "long_term_debt": total_assets * rng.uniform(0.0, 0.5),
                "total_debt": total_assets * rng.uniform(0.0, 0.6),
                "fcf": net_income * rng.uniform(0.3, 1.5),
                "capex": abs(revenue * rng.uniform(0.02, 0.15)),
                # Valuation
                "pe_ratio": float(rng.uniform(5, 60)),
                "ev_ebitda": float(rng.uniform(3, 30)),
                "ev_revenue": float(rng.uniform(0.5, 10)),
                "fcf_yield": float(rng.uniform(-0.05, 0.15)),
                "earnings_yield": float(rng.uniform(0.01, 0.20)),
                "book_to_market": float(rng.uniform(0.1, 2.0)),
                "ps_ratio": float(rng.uniform(0.5, 15)),
                # Quality
                "gross_margin": float(rng.uniform(0.1, 0.8)),
                "operating_margin": float(rng.uniform(-0.1, 0.4)),
                "roe": float(rng.uniform(-0.2, 0.5)),
                "roa": float(rng.uniform(-0.1, 0.3)),
                "roic": float(rng.uniform(-0.1, 0.3)),
                "current_ratio": float(rng.uniform(0.5, 4.0)),
                "debt_to_equity": float(rng.uniform(0.0, 3.0)),
                "piotroski_f_score": float(rng.integers(0, 10)),
                "ocf_to_ni": float(rng.uniform(0.5, 2.0)),
                "accruals_to_assets": float(rng.uniform(-0.2, 0.2)),
                "sloan_accruals": float(rng.uniform(-0.3, 0.3)),
                "gross_profit_to_assets": float(rng.uniform(0.05, 0.5)),
                # Momentum
                "momentum_12m_prior": float(rng.uniform(-0.5, 1.5)),
                "momentum_6m_prior": float(rng.uniform(-0.3, 0.8)),
                "momentum_3m_prior": float(rng.uniform(-0.2, 0.5)),
                "momentum_12m_rank": float(rng.uniform(0, 1)),
                "momentum_6m_rank": float(rng.uniform(0, 1)),
                "momentum_3m_rank": float(rng.uniform(0, 1)),
                # Growth
                "revenue_cagr_3y": float(rng.uniform(-0.2, 0.5)),
                "revenue_growth_yoy": float(rng.uniform(-0.3, 0.8)),
                "eps_growth_yoy": float(rng.uniform(-1.0, 2.0)),
                "net_income_growth_yoy": float(rng.uniform(-1.0, 2.0)),
                "ocf_growth_yoy": float(rng.uniform(-0.5, 1.5)),
                "gross_profit_growth_yoy": float(rng.uniform(-0.3, 0.8)),
                # Fraud signals
                "beneish_m_score": float(rng.uniform(-3.0, 0.5)),
                "ohlson_prob_bankruptcy": float(rng.uniform(0.0, 0.5)),
                "altman_z_score": float(rng.uniform(0.5, 5.0)),
                "fraud_score_composite": float(rng.uniform(0, 1)),
                "fraud_score_accounting": float(rng.uniform(0, 1)),
                "fraud_score_distress": float(rng.uniform(0, 1)),
                # Forward returns (targets)
                "forward_return_1y": float(rng.uniform(-0.5, 2.0)),
                "forward_return_3y": float(rng.uniform(-0.5, 3.0)),
                "forward_return_5y": float(rng.uniform(-0.5, 5.0)),
                "beat_local_market_1y": int(rng.integers(0, 2)),
                "beat_local_market_3y": int(rng.integers(0, 2)),
                "beat_local_market_5y": int(rng.integers(0, 2)),
            })
    return pd.DataFrame(rows)


class TestFullPipelineIntegration:
    """End-to-end: synthetic data → features → selection → model → alpha scores."""

    @pytest.fixture
    def dataset(self):
        return _make_synthetic_dataset()

    def test_feature_selection_pipeline(self, dataset):
        """Feature selection runs and returns a non-empty feature list."""
        from modeling.train import get_candidates, compute_ic_table, deduplicate_features

        candidates = get_candidates(dataset)
        assert len(candidates) > 5, "Should find candidate features"

        ic_table = compute_ic_table(dataset, candidates, "forward_return_1y")
        assert len(ic_table) > 0, "IC table should have entries"
        assert "icir" in ic_table.columns

        top_features = ic_table.head(20).index.tolist()
        deduped = deduplicate_features(dataset, top_features, corr_threshold=0.85)
        assert len(deduped) > 0, "Dedup should keep some features"
        assert len(deduped) <= len(top_features)

    def test_model_training(self, dataset):
        """LightGBM trains on synthetic data without error."""
        from modeling.train import get_candidates, train_model

        candidates = get_candidates(dataset)
        features = candidates[:10]
        train_df = dataset[dataset["fiscal_year"] <= 2017]

        clf, feats, y_train, medians = train_model(train_df, features, "beat_local_market_1y")
        assert clf is not None
        assert len(feats) > 0
        assert len(medians) == len(feats)

        # Model can predict
        test_df = dataset[dataset["fiscal_year"] > 2017]
        X_test = test_df[feats].fillna(pd.Series(medians))
        probs = clf.predict_proba(X_test)[:, 1]
        assert len(probs) == len(test_df)
        assert all(0 <= p <= 1 for p in probs)

    def test_alpha_factor_scores(self, dataset):
        """5-factor alpha composite runs and produces valid scores."""
        from alpha.factors.composite import compute as compute_composite

        scores = compute_composite(dataset)
        assert isinstance(scores, pd.DataFrame)
        expected_cols = [
            "alpha_value", "alpha_quality", "alpha_momentum",
            "alpha_growth", "alpha_fraud_risk", "alpha_composite",
        ]
        for col in expected_cols:
            assert col in scores.columns, f"Missing {col}"
            series = scores[col]
            valid = series.dropna()
            assert len(valid) > 0, f"{col} has no valid values"
            assert valid.min() >= 0.0, f"{col} min below 0"
            assert valid.max() <= 1.0, f"{col} max above 1"

    def test_temporal_split_no_leakage(self, dataset):
        """Train/val/test split has no temporal overlap."""
        train_cutoff = 2016
        val_end = 2018

        train = dataset[dataset["fiscal_year"] <= train_cutoff]
        val = dataset[(dataset["fiscal_year"] > train_cutoff) & (dataset["fiscal_year"] <= val_end)]
        test = dataset[dataset["fiscal_year"] > val_end]

        assert train["fiscal_year"].max() <= train_cutoff
        assert val["fiscal_year"].min() > train_cutoff
        assert val["fiscal_year"].max() <= val_end
        assert test["fiscal_year"].min() > val_end
        assert len(train) + len(val) + len(test) == len(dataset)

    def test_end_to_end_pipeline(self, dataset):
        """Full flow: candidates → IC → select → train → predict → alpha → validate."""
        from modeling.train import get_candidates, compute_ic_table, train_model

        # 1. Feature selection
        candidates = get_candidates(dataset)
        ic_table = compute_ic_table(dataset, candidates, "forward_return_1y")
        selected = ic_table.head(15).index.tolist()
        assert len(selected) >= 5

        # 2. Temporal split
        train_df = dataset[dataset["fiscal_year"] <= 2016]
        test_df = dataset[dataset["fiscal_year"] > 2016]
        assert len(train_df) > 0
        assert len(test_df) > 0

        # 3. Train model
        clf, feats, y_train, medians = train_model(train_df, selected, "beat_local_market_1y")
        assert clf is not None

        # 4. Score test set
        X_test = test_df[feats].fillna(pd.Series(medians))
        probs = clf.predict_proba(X_test)[:, 1]
        assert len(probs) == len(test_df)

        # 5. Alpha scores on full dataset
        from alpha.factors.composite import compute as compute_composite
        alpha_scores = compute_composite(dataset)
        assert "alpha_composite" in alpha_scores.columns
        assert alpha_scores["alpha_composite"].notna().sum() > 0

        # 6. Backtest-style top-N selection works
        test_df = test_df.copy()
        test_df["ml_score"] = probs
        for year in test_df["fiscal_year"].unique():
            yr_df = test_df[test_df["fiscal_year"] == year]
            top_10 = yr_df.nlargest(min(10, len(yr_df)), "ml_score")
            assert len(top_10) > 0
            mean_ret = top_10["forward_return_1y"].mean()
            assert np.isfinite(mean_ret)

    def test_horizon_router(self):
        """HorizonRouter maps months to correct model keys."""
        from alpha.horizon_router import HorizonRouter

        cases = [(3, "6m"), (6, "6m"), (9, "1y"), (12, "1y"),
                 (18, "2y"), (24, "2y"), (36, "3y"), (48, "5y"), (60, "5y")]
        for months, expected in cases:
            assert HorizonRouter.route(months) == expected, f"route({months}) != {expected}"
