"""Unit tests for backtest/engine.py — walk-forward engine."""

import numpy as np
import pandas as pd
import pytest

from backtest.engine import (
    load_and_score,
    run_backtest,
    bootstrap_ci,
    _apply_filing_lag_filter,
    _sic_to_sector,
    _apply_sector_cap,
    adtv_filter,
    filter_composite,
)


def test_policy_sensitivity_does_not_reuse_static_model_scores():
    df = pd.DataFrame({
        "fiscal_year": [2018, 2019],
        "ml_1y": [0.8, 0.7],
        "ml_3y": [0.9, 0.6],
        "ml_5y": [0.7, 0.5],
    })
    result = load_and_score(df, label_policy="include_policy_imputed")
    assert not {"ml_1y", "ml_3y", "ml_5y"} & set(result.columns)


@pytest.fixture
def backtest_df():
    """Minimal DataFrame that satisfies run_backtest requirements."""
    rng = np.random.default_rng(99)
    years = list(range(2010, 2021))
    rows_per_year = 50
    n = len(years) * rows_per_year

    df = pd.DataFrame({
        "fiscal_year": np.repeat(years, rows_per_year),
        "market": ["US"] * n,
        "ticker": [f"T{i:04d}" for i in range(n)],
        "forward_return_1y": rng.uniform(-0.3, 0.6, n),
        "market_cap_at_filing": rng.uniform(1e8, 5e10, n),
        "piotroski_f_score": rng.integers(3, 10, n).astype(float),
        "value_composite": rng.uniform(0, 1, n),
        "quality_composite": rng.uniform(0, 1, n),
        "ml_1y_wf": rng.uniform(0, 1, n),
        "ml_3y_wf": rng.uniform(0, 1, n),
        "beneish_m_score": rng.uniform(-4.0, -2.0, n),
        "sic_code": rng.choice([3500, 5000, 6000, 7370], n),
        "size_category_label": rng.choice(["large", "mid", "small"], n),
        "vol_prior_12m": rng.uniform(0.15, 0.60, n),
    })
    return df


def _top_n_filter(yr_df, top_n, market):
    """Trivial filter: pick top_n by forward_return_1y (for testing only)."""
    s = yr_df.copy()
    if market:
        s = s[s["market"] == market]
    return s.nlargest(top_n, "forward_return_1y").index


def _monthly_prices_for(df):
    rows = []
    for row in df.itertuples():
        dates = pd.date_range(
            f"{int(row.fiscal_year)}-12-31",
            f"{int(row.fiscal_year) + 1}-12-31",
            freq="ME",
        )
        terminal = max(1.0 + float(row.forward_return_1y), 0.01)
        prices = 100 * np.power(terminal, np.arange(13) / 12)
        rows.extend(
            {"ticker": row.ticker, "date": date, "adj_close": price}
            for date, price in zip(dates, prices)
        )
    return pd.DataFrame(rows)


class TestRunBacktest:
    def test_returns_dict_with_required_keys(self, backtest_df):
        result = run_backtest(
            backtest_df, _top_n_filter, "test", top_n=10,
            market=None, cost_bps=30, smallcap_cost_bps=60,
            spy_returns=None, monthly_px=None, use_adtv_filter=False,
        )
        assert isinstance(result, dict)
        assert "label" in result
        assert "n_years" in result
        assert result["official_performance_available"] is False
        assert result["nav_exclusions"][0]["code"] == "missing_monthly_price_schema"

    def test_n_years_matches_data(self, backtest_df):
        result = run_backtest(
            backtest_df, _top_n_filter, "test", top_n=10,
            market=None, cost_bps=30, smallcap_cost_bps=60,
            spy_returns=None, monthly_px=_monthly_prices_for(backtest_df), use_adtv_filter=False,
        )
        # Data goes 2010-2020, engine filters <=2023 so all 11 years used
        assert result["n_years"] == 11
        assert len(result["annual_returns"]) == 11

    def test_annual_returns_have_correct_fields(self, backtest_df):
        result = run_backtest(
            backtest_df, _top_n_filter, "test", top_n=10,
            market=None, cost_bps=30, smallcap_cost_bps=60,
            spy_returns=None, monthly_px=_monthly_prices_for(backtest_df), use_adtv_filter=False,
        )
        row = result["annual_returns"][0]
        assert "year" in row
        assert "port_pct" in row
        assert "n_picks" in row

    def test_respects_top_n(self, backtest_df):
        result = run_backtest(
            backtest_df, _top_n_filter, "test", top_n=5,
            market=None, cost_bps=30, smallcap_cost_bps=60,
            spy_returns=None, monthly_px=_monthly_prices_for(backtest_df), use_adtv_filter=False,
        )
        for row in result["annual_returns"]:
            assert row["n_picks"] <= 5

    def test_cost_drag_positive(self, backtest_df):
        result = run_backtest(
            backtest_df, _top_n_filter, "test", top_n=10,
            market=None, cost_bps=50, smallcap_cost_bps=80,
            spy_returns=None, monthly_px=_monthly_prices_for(backtest_df), use_adtv_filter=False,
        )
        assert result["avg_cost_drag_bps"] > 0

    def test_market_filter(self, backtest_df):
        df = backtest_df.copy()
        df.loc[df.index[:100], "market"] = "JP"
        result = run_backtest(
            df, _top_n_filter, "test", top_n=10,
            market="US", cost_bps=30, smallcap_cost_bps=60,
            spy_returns=None, monthly_px=_monthly_prices_for(df), use_adtv_filter=False,
        )
        assert result["n_years"] == 0
        assert result["official_performance_available"] is False
        assert result["unavailable_years"] == [2010, 2011]

    def test_empty_universe_returns_error(self):
        df = pd.DataFrame({
            "fiscal_year": [2020] * 5,
            "market": ["US"] * 5,
            "ticker": [f"T{i}" for i in range(5)],
            "forward_return_1y": [np.nan] * 5,
            "market_cap_at_filing": [1e9] * 5,
        })

        def _empty_filter(yr_df, top_n, market):
            return pd.Index([])

        result = run_backtest(
            df, _empty_filter, "empty", top_n=10,
            market=None, cost_bps=30, smallcap_cost_bps=60,
            spy_returns=None, monthly_px=None, use_adtv_filter=False,
        )
        assert result["n_years"] == 0

    def test_single_period(self):
        """Engine should work with a single fiscal year."""
        rng = np.random.default_rng(7)
        n = 30
        df = pd.DataFrame({
            "fiscal_year": [2020] * n,
            "market": ["US"] * n,
            "ticker": [f"T{i}" for i in range(n)],
            "forward_return_1y": rng.uniform(-0.2, 0.4, n),
            "market_cap_at_filing": [1e9] * n,
            "sic_code": [3500] * n,
            "vol_prior_12m": [0.25] * n,
        })
        result = run_backtest(
            df, _top_n_filter, "single", top_n=10,
            market=None, cost_bps=30, smallcap_cost_bps=60,
            spy_returns=None, monthly_px=_monthly_prices_for(df), use_adtv_filter=False,
        )
        assert result["n_years"] == 1

    def test_equal_weight_mode(self, backtest_df):
        result = run_backtest(
            backtest_df, _top_n_filter, "ew", top_n=10,
            market=None, cost_bps=30, smallcap_cost_bps=60,
            vol_weighted=False,
            spy_returns=None, monthly_px=_monthly_prices_for(backtest_df), use_adtv_filter=False,
        )
        assert result["n_years"] > 0

    def test_spy_benchmark(self, backtest_df):
        rng = np.random.default_rng(55)
        spy = {yr: rng.uniform(0.0, 0.2) for yr in range(2010, 2021)}
        result = run_backtest(
            backtest_df, _top_n_filter, "spy", top_n=10,
            market=None, cost_bps=30, smallcap_cost_bps=60,
            spy_returns=spy, monthly_px=_monthly_prices_for(backtest_df), use_adtv_filter=False,
        )
        assert result["benchmark_source"] is None
        assert result["spy_cagr_pct"] is None

    def test_fill_missing_return(self, backtest_df):
        df = backtest_df.copy()
        df.loc[df.index[:20], "forward_return_1y"] = np.nan
        with pytest.raises(ValueError, match="explicit return_policy"):
            run_backtest(
                df, _top_n_filter, "fill", top_n=10,
                market=None, cost_bps=30, smallcap_cost_bps=60,
                fill_missing_return=-0.5,
                spy_returns=None, monthly_px=None, use_adtv_filter=False,
            )


class TestBootstrapCI:
    def test_returns_dict_with_keys(self):
        rets = np.random.default_rng(0).uniform(-0.1, 0.3, 15)
        result = bootstrap_ci(rets)
        assert "cagr_bootstrap_mean_pct" in result
        assert "sharpe_bootstrap_mean" in result

    def test_short_array_returns_empty(self):
        result = bootstrap_ci(np.array([0.1, 0.2, 0.3]))
        assert result == {}

    def test_deterministic_with_seed(self):
        rets = np.array([0.1, 0.2, -0.05, 0.15, 0.3, 0.08, -0.02, 0.12])
        r1 = bootstrap_ci(rets)
        r2 = bootstrap_ci(rets)
        assert r1 == r2


class TestFilingLagFilter:
    def test_drops_late_filings(self):
        df = pd.DataFrame({
            "fiscal_year": [2020, 2020, 2020],
            "filed_date": ["2021-03-15", "2022-09-01", "2021-06-30"],
        })
        result = _apply_filing_lag_filter(df, 2020, max_lag_months=18)
        # Filing from 2022-09-01 is >18 months after 2020-12-31
        assert len(result) == 2

    def test_keeps_rows_without_filed_date(self):
        df = pd.DataFrame({
            "fiscal_year": [2020, 2020],
            "filed_date": [pd.NaT, "2021-03-01"],
        })
        result = _apply_filing_lag_filter(df, 2020, max_lag_months=18)
        assert len(result) == 2


def test_adtv_gate_requires_decision_time_evidence():
    candidates = pd.DataFrame({"ticker": ["COVERED", "TOO_SMALL", "MISSING"]})
    evidence = pd.DataFrame({
        "ticker": ["COVERED", "TOO_SMALL"],
        "date": [pd.Timestamp("2020-12-15"), pd.Timestamp("2020-12-15")],
        "adtv_30d": [1_400_000.0, 1_300_000.0],
    })
    result = adtv_filter(
        candidates, evidence, 2020, max_pct_adtv=0.01,
        aum_target=200_000, target_n=15,
    )
    assert result["ticker"].tolist() == ["COVERED"]


def test_adtv_gate_rejects_invalid_position_contract():
    candidates = pd.DataFrame({"ticker": ["A"]})
    evidence = pd.DataFrame({
        "ticker": ["A"], "date": [pd.Timestamp("2020-12-15")],
        "adtv_30d": [2_000_000.0],
    })
    with pytest.raises(ValueError, match="target_n > 0"):
        adtv_filter(candidates, evidence, 2020, target_n=0)

class TestSicToSector:
    def test_known_mappings(self):
        sic = pd.Series([3500, 6100, 7370, 100, 9999])
        sectors = _sic_to_sector(sic)
        assert sectors.iloc[0] == "Manufacturing"
        assert sectors.iloc[1] == "Finance/Insurance/RE"
        assert sectors.iloc[2] == "Services/Hospitality"
        assert sectors.iloc[3] == "Agriculture/Mining"
        assert sectors.iloc[4] == "Other"


class TestSectorCap:
    def test_caps_overweight_sector(self):
        weights = np.array([0.5, 0.3, 0.1, 0.1])
        picks = pd.DataFrame({"sic_code": [3500, 3600, 6000, 7000]})
        # First two are Manufacturing — total 0.8 > 0.35 cap
        result = _apply_sector_cap(weights, picks, max_sector_weight=0.35)
        # Manufacturing total should be reduced from 0.8
        mfg_total = result[0] + result[1]
        assert mfg_total < 0.8
        assert abs(result.sum() - 1.0) < 1e-9

    def test_no_change_when_within_cap(self):
        weights = np.array([0.25, 0.25, 0.25, 0.25])
        picks = pd.DataFrame({"sic_code": [3500, 5000, 6000, 7000]})
        result = _apply_sector_cap(weights, picks, max_sector_weight=0.35)
        np.testing.assert_allclose(result, weights, atol=1e-9)


class TestFilterComposite:
    def test_returns_index(self, backtest_df):
        yr_df = backtest_df[backtest_df["fiscal_year"] == 2015]
        idx = filter_composite(yr_df, top_n=10, market=None)
        assert isinstance(idx, pd.Index)
        assert len(idx) <= 10
