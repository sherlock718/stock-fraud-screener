import numpy as np
import pandas as pd
import pytest

from backtest.monthly_nav import (
    OBSERVED_ONLY,
    POLICY_IMPUTED_50,
    POLICY_IMPUTED_100,
    annual_returns_from_nav,
    build_monthly_nav,
    compute_nav_metrics,
)


def _annual_row(tickers=("A",), weights=None, costs=None, year=2020):
    n = len(tickers)
    return {
        "year": year,
        "_picks_valid": pd.DataFrame({"ticker": list(tickers)}),
        "_weights": weights or [1 / n] * n,
        "_per_pick_cost": costs or [0.0] * n,
        "n_picks": n,
        "cost_drag": float(np.dot(weights or [1 / n] * n, costs or [0.0] * n)),
    }


def _prices(paths):
    dates = pd.date_range("2020-12-31", "2021-12-31", freq="ME")
    rows = []
    for ticker, values in paths.items():
        rows.extend(
            {"ticker": ticker, "date": date, "adj_close": value}
            for date, value in zip(dates, values)
            if value is not None
        )
    return pd.DataFrame(rows)


def test_monotonic_nav_transaction_cost_and_annual_monthly_reconciliation():
    prices = _prices({"A": np.linspace(100, 112, 13)})
    no_cost = build_monthly_nav([_annual_row()], prices)
    assert no_cost["nav"]["net_nav"].is_monotonic_increasing
    result = build_monthly_nav([_annual_row(costs=[0.01])], prices)
    assert result["available"] is True
    nav = result["nav"]
    assert nav["net_nav"].iloc[1:].is_monotonic_increasing
    assert nav.iloc[1]["monthly_net_return"] == pytest.approx(0.99 * 101 / 100 - 1)
    annual = annual_returns_from_nav(nav)
    assert annual[0]["net_return"] == pytest.approx(0.99 * 1.12 - 1)
    assert abs(annual[0]["reconciliation_error"]) <= 1e-10


def test_known_drawdown_and_positive_endpoint_do_not_hide_monthly_loss():
    path = [100, 120, 60] + list(np.linspace(65, 130, 10))
    result = build_monthly_nav([_annual_row()], _prices({"A": path}))
    metrics = compute_nav_metrics(result["nav"])
    assert result["nav"].iloc[-1]["net_nav"] > result["nav"].iloc[0]["net_nav"]
    assert metrics["max_drawdown"] == pytest.approx(-0.5)
    assert metrics["max_drawdown_duration_months"] > 0


@pytest.mark.parametrize("missing_index", [0, 1, 6, 12])
def test_no_or_partial_ticker_and_month_coverage_fails_closed(missing_index):
    path = [100.0] * 13
    path[missing_index] = None
    result = build_monthly_nav([_annual_row()], _prices({"A": path}))
    assert result["available"] is False
    assert result["exclusions"][0]["code"] in {
        "missing_selected_holding_entry_price",
        "missing_selected_holding_price_or_event",
    }


def test_one_missing_selected_ticker_invalidates_full_portfolio_without_reweighting():
    prices = _prices({"A": [100.0] * 13, "B": [100.0] + [None] * 12})
    result = build_monthly_nav(
        [_annual_row(("A", "B"), weights=[0.5, 0.5])], prices
    )
    assert result["available"] is False
    assert result["exclusions"][0]["ticker"] == "B"
    assert result["exclusions"][0]["weight"] == pytest.approx(0.5)


def test_midyear_unresolved_disappearance_has_three_explicit_paths():
    path = [100.0] * 7 + [None] * 6
    events = pd.DataFrame({
        "ticker": ["A"],
        "effective_date": ["2021-07-15"],
        "status": ["unresolved"],
        "source": ["versioned-security-master"],
    })
    observed = build_monthly_nav(
        [_annual_row()], _prices({"A": path}),
        return_policy=OBSERVED_ONLY, corporate_actions=events,
    )
    sensitivity_50 = build_monthly_nav(
        [_annual_row()], _prices({"A": path}),
        return_policy=POLICY_IMPUTED_50, corporate_actions=events,
    )
    sensitivity_100 = build_monthly_nav(
        [_annual_row()], _prices({"A": path}),
        return_policy=POLICY_IMPUTED_100, corporate_actions=events,
    )
    assert observed["available"] is False
    assert observed["exclusions"][0]["code"] == "unresolved_corporate_action"
    assert sensitivity_50["nav"].iloc[-1]["net_nav"] == pytest.approx(0.5)
    assert sensitivity_100["nav"].iloc[-1]["net_nav"] == pytest.approx(0.0)
    assert sensitivity_50["return_policy"] != sensitivity_100["return_policy"]


def test_resolved_total_loss_is_bounded_at_zero_and_requires_evidence():
    path = [100.0] * 4 + [None] * 9
    event = pd.DataFrame({
        "ticker": ["A"],
        "effective_date": ["2021-04-10"],
        "status": ["resolved_cash"],
        "source": ["dated-cancellation-order"],
        "event_total_return": [-1.0],
        "post_event_cash": [True],
    })
    result = build_monthly_nav(
        [_annual_row(costs=[0.02])], _prices({"A": path}), corporate_actions=event
    )
    assert result["available"] is True
    assert result["nav"]["net_nav"].min() == 0.0
    assert result["nav"].iloc[-1]["net_nav"] == 0.0
    assert compute_nav_metrics(result["nav"])["cagr"] == -1.0


def test_metrics_share_one_canonical_nav_and_sharpe_needs_monthly_rf():
    path = [100, 104, 101, 108, 110, 107, 115, 119, 116, 122, 125, 123, 130]
    result = build_monthly_nav([_annual_row()], _prices({"A": path}))
    nav = result["nav"]
    without_rf = compute_nav_metrics(nav)
    rf = pd.Series(0.001, index=nav.loc[nav["monthly_net_return"].notna(), "date"])
    metrics = compute_nav_metrics(nav, rf)
    assert without_rf["sharpe"] is np.nan or np.isnan(without_rf["sharpe"])
    assert metrics["metric_nav_column"] == "net_nav"
    assert np.isfinite(metrics["sharpe"])
    assert metrics["calmar"] == pytest.approx(
        metrics["cagr"] / abs(metrics["max_drawdown"])
    )
    annual = annual_returns_from_nav(nav)[0]
    assert annual["net_return"] == pytest.approx(
        np.prod(1 + nav["monthly_net_return"].dropna()) - 1
    )
