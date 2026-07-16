"""Canonical month-end net total-return NAV construction and metrics.

Historical performance is valid only when every selected holding has complete
price coverage or dated corporate-action evidence.  Missing observations are
never interpreted as zero returns.  Policy-imputed disappearance scenarios are
kept separate from the observed-only path.
"""
from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd


OBSERVED_ONLY = "observed_only"
POLICY_IMPUTED_50 = "include_policy_imputed_50"
POLICY_IMPUTED_100 = "include_policy_imputed_100"
RETURN_POLICIES = (OBSERVED_ONLY, POLICY_IMPUTED_50, POLICY_IMPUTED_100)

_POLICY_EVENT_RETURNS = {
    POLICY_IMPUTED_50: -0.50,
    POLICY_IMPUTED_100: -1.00,
}


def _unavailable(policy: str, exclusions: list[dict]) -> dict:
    return {
        "available": False,
        "return_policy": policy,
        "nav": pd.DataFrame(columns=["date", "net_nav", "monthly_net_return"]),
        "ledger": pd.DataFrame(),
        "exclusions": exclusions,
    }


def _normalise_prices(monthly_px: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    required = {"ticker", "date", "adj_close"}
    if monthly_px is None or not required.issubset(monthly_px.columns):
        return pd.DataFrame(), "missing_monthly_price_schema"
    prices = monthly_px.loc[:, list(required)].copy()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices["adj_close"] = pd.to_numeric(prices["adj_close"], errors="coerce")
    prices["month"] = prices["date"].dt.to_period("M")
    prices = prices.dropna(subset=["ticker", "month"])
    prices = prices.sort_values(["ticker", "month", "date"])
    prices = prices.groupby(["ticker", "month"], as_index=False).tail(1)
    return prices.set_index(["ticker", "month"])["adj_close"], None


def _normalise_events(corporate_actions: pd.DataFrame | None) -> pd.DataFrame:
    columns = [
        "ticker", "effective_date", "status", "source", "event_total_return",
        "post_event_cash",
    ]
    if corporate_actions is None:
        return pd.DataFrame(columns=columns + ["month"])
    events = corporate_actions.copy()
    for column in columns:
        if column not in events:
            events[column] = np.nan
    events["effective_date"] = pd.to_datetime(events["effective_date"], errors="coerce")
    events["month"] = events["effective_date"].dt.to_period("M")
    return events.sort_values(["ticker", "effective_date"])


def _event_for_missing_month(
    events: pd.DataFrame,
    ticker: str,
    last_observed_month: pd.Period,
    missing_month: pd.Period,
) -> pd.Series | None:
    matches = events[
        (events["ticker"] == ticker)
        & (events["month"] > last_observed_month)
        & (events["month"] <= missing_month)
    ]
    if len(matches) != 1:
        return None
    return matches.iloc[0]


def build_monthly_nav(
    annual_rows: list[dict],
    monthly_px: pd.DataFrame | None,
    *,
    return_policy: str = OBSERVED_ONLY,
    corporate_actions: pd.DataFrame | None = None,
) -> dict:
    """Build one continuous security-level month-end net NAV.

    Each annual row must contain ``year``, ``_picks_valid``, ``_weights``, and
    ``_per_pick_cost``.  The existing calendar is retained: fiscal-year ``y``
    selections are held from the preceding December month-end through December
    of ``y + 1``.  Prices are total-return adjusted closes.
    """
    if return_policy not in RETURN_POLICIES:
        raise ValueError(f"Unknown return policy: {return_policy}")
    prices, price_error = _normalise_prices(monthly_px)
    if price_error:
        return _unavailable(return_policy, [{"code": price_error}])
    events = _normalise_events(corporate_actions)
    if not annual_rows:
        return _unavailable(return_policy, [{"code": "no_selected_periods"}])

    nav_records: list[dict] = []
    ledger_records: list[dict] = []
    previous_nav = 1.0
    previous_end: pd.Period | None = None

    for row in annual_rows:
        year = int(row["year"])
        start_month = pd.Period(f"{year}-12", freq="M")
        end_month = pd.Period(f"{year + 1}-12", freq="M")
        if previous_end is not None and start_month != previous_end:
            return _unavailable(return_policy, [{
                "code": "non_contiguous_holding_period",
                "year": year,
            }])

        picks = row.get("_picks_valid")
        weights = np.asarray(row.get("_weights"), dtype=float)
        costs = np.asarray(row.get("_per_pick_cost"), dtype=float)
        if picks is None or len(picks) == 0 or len(weights) != len(picks) or len(costs) != len(picks):
            return _unavailable(return_policy, [{
                "code": "missing_portfolio_ledger_inputs",
                "year": year,
            }])
        tickers = picks["ticker"].astype(str).tolist()
        if len(set(tickers)) != len(tickers) or not np.isfinite(weights).all() or weights.sum() <= 0:
            return _unavailable(return_policy, [{"code": "invalid_portfolio_weights", "year": year}])
        weights = weights / weights.sum()
        if (costs < 0).any() or not np.isfinite(costs).all():
            return _unavailable(return_policy, [{"code": "invalid_transaction_cost", "year": year}])

        if not nav_records:
            nav_records.append({
                "date": start_month.to_timestamp("M"),
                "net_nav": previous_nav,
                "monthly_net_return": np.nan,
                "cash": 0.0,
                "benchmark_nav": np.nan,
            })
        states: dict[str, dict] = {}
        for ticker, weight, cost in zip(tickers, weights, costs):
            anchor = prices.get((ticker, start_month), np.nan)
            if not np.isfinite(anchor) or anchor <= 0:
                return _unavailable(return_policy, [{
                    "code": "missing_selected_holding_entry_price",
                    "year": year,
                    "ticker": ticker,
                    "month": str(start_month),
                    "weight": float(weight),
                }])
            gross_allocation = previous_nav * float(weight)
            transaction_cost = gross_allocation * float(cost)
            initial_value = max(gross_allocation - transaction_cost, 0.0)
            states[ticker] = {
                "value": initial_value,
                "price": float(anchor),
                "shares": initial_value / float(anchor),
                "last_observed_month": start_month,
                "cash": False,
                "weight": float(weight),
                "cost": float(cost),
            }
            ledger_records.append({
                "year": year,
                "date": start_month.to_timestamp("M"),
                "ticker": ticker,
                "beginning_value": 0.0,
                "ending_value": initial_value,
                "security_total_return": np.nan,
                "shares": states[ticker]["shares"],
                "cash": False,
                "initial_weight": float(weight),
                "trade_notional": gross_allocation,
                "transaction_cost": transaction_cost,
                "trade_cost_rate": float(cost),
                "event_status": None,
                "return_policy": return_policy,
            })

        for month in pd.period_range(start_month + 1, end_month, freq="M"):
            for ticker, state in states.items():
                beginning_value = state["value"]
                event_status = None
                if not state["cash"]:
                    price = prices.get((ticker, month), np.nan)
                    if np.isfinite(price) and price > 0:
                        state["value"] *= float(price) / state["price"]
                        state["price"] = float(price)
                        state["last_observed_month"] = month
                    else:
                        event = _event_for_missing_month(
                            events, ticker, state["last_observed_month"], month
                        )
                        if event is None:
                            return _unavailable(return_policy, [{
                                "code": "missing_selected_holding_price_or_event",
                                "year": year,
                                "ticker": ticker,
                                "month": str(month),
                                "weight": state["weight"],
                            }])
                        source = event.get("source")
                        status = event.get("status")
                        if pd.isna(source) or not str(source).strip():
                            return _unavailable(return_policy, [{
                                "code": "missing_corporate_action_provenance",
                                "year": year,
                                "ticker": ticker,
                                "month": str(month),
                                "weight": state["weight"],
                            }])
                        if status == "unresolved":
                            if return_policy == OBSERVED_ONLY:
                                return _unavailable(return_policy, [{
                                    "code": "unresolved_corporate_action",
                                    "year": year,
                                    "ticker": ticker,
                                    "month": str(event["month"]),
                                    "weight": state["weight"],
                                    "source": str(source),
                                }])
                            event_return = _POLICY_EVENT_RETURNS[return_policy]
                            event_status = return_policy
                        elif status == "resolved_cash":
                            event_return = pd.to_numeric(event.get("event_total_return"), errors="coerce")
                            if (
                                not np.isfinite(event_return)
                                or event_return < -1
                                or not bool(event.get("post_event_cash"))
                            ):
                                return _unavailable(return_policy, [{
                                    "code": "incomplete_corporate_action_terms",
                                    "year": year,
                                    "ticker": ticker,
                                    "month": str(event["month"]),
                                    "weight": state["weight"],
                                }])
                            event_status = "resolved_cash"
                        else:
                            return _unavailable(return_policy, [{
                                "code": "unsupported_corporate_action_resolution",
                                "year": year,
                                "ticker": ticker,
                                "month": str(event["month"]),
                                "weight": state["weight"],
                            }])
                        state["value"] = max(state["value"] * (1.0 + float(event_return)), 0.0)
                        state["cash"] = True

                ledger_records.append({
                    "year": year,
                    "date": month.to_timestamp("M"),
                    "ticker": ticker,
                    "beginning_value": beginning_value,
                    "ending_value": state["value"],
                    "security_total_return": (
                        state["value"] / beginning_value - 1.0
                        if beginning_value > 0 else 0.0
                    ),
                    "shares": 0.0 if state["cash"] else state["shares"],
                    "cash": state["cash"],
                    "initial_weight": state["weight"],
                    "trade_notional": 0.0,
                    "transaction_cost": 0.0,
                    "trade_cost_rate": state["cost"],
                    "event_status": event_status,
                    "return_policy": return_policy,
                })

            current_nav = max(float(sum(state["value"] for state in states.values())), 0.0)
            monthly_return = current_nav / previous_nav - 1.0 if previous_nav > 0 else 0.0
            nav_records.append({
                "date": month.to_timestamp("M"),
                "net_nav": current_nav,
                "monthly_net_return": max(monthly_return, -1.0),
                "cash": float(sum(
                    state["value"] for state in states.values() if state["cash"]
                )),
                "benchmark_nav": np.nan,
            })
            previous_nav = current_nav
        previous_end = end_month

    nav = pd.DataFrame(nav_records)
    return {
        "available": True,
        "return_policy": return_policy,
        "nav": nav,
        "ledger": pd.DataFrame(ledger_records),
        "exclusions": [],
    }


def annual_returns_from_nav(nav: pd.DataFrame, tolerance: float = 1e-10) -> list[dict]:
    """Aggregate complete calendar-year returns and verify monthly reconciliation."""
    frame = nav.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date")
    output: list[dict] = []
    for year, group in frame.dropna(subset=["monthly_net_return"]).groupby(frame["date"].dt.year):
        if set(group["date"].dt.month) != set(range(1, 13)):
            continue
        monthly_product = float(np.prod(1.0 + group["monthly_net_return"].to_numpy()) - 1.0)
        start = frame[frame["date"] == pd.Timestamp(f"{year - 1}-12-31")]
        end = frame[frame["date"] == pd.Timestamp(f"{year}-12-31")]
        if len(start) != 1 or len(end) != 1:
            raise ValueError(f"Missing annual NAV endpoint for {year}")
        endpoint_return = float(end.iloc[0]["net_nav"] / start.iloc[0]["net_nav"] - 1.0)
        if abs(monthly_product - endpoint_return) > tolerance:
            raise ValueError(f"Annual/monthly NAV reconciliation failed for {year}")
        output.append({
            "year": int(year),
            "net_return": endpoint_return,
            "monthly_product_return": monthly_product,
            "reconciliation_error": endpoint_return - monthly_product,
        })
    return output


def _risk_free_returns(
    monthly_risk_free: pd.Series | pd.DataFrame | dict | None,
    dates: Iterable[pd.Timestamp],
) -> np.ndarray | None:
    if monthly_risk_free is None:
        return None
    if isinstance(monthly_risk_free, pd.DataFrame):
        if not {"date", "risk_free_return"}.issubset(monthly_risk_free.columns):
            return None
        series = monthly_risk_free.set_index(pd.to_datetime(monthly_risk_free["date"]))["risk_free_return"]
    elif isinstance(monthly_risk_free, pd.Series):
        series = monthly_risk_free.copy()
        series.index = pd.to_datetime(series.index)
    else:
        series = pd.Series(monthly_risk_free)
        series.index = pd.to_datetime(series.index)
    aligned = pd.to_numeric(series, errors="coerce").reindex(pd.DatetimeIndex(dates))
    if aligned.isna().any():
        return None
    return aligned.to_numpy(dtype=float)


def compute_nav_metrics(
    nav: pd.DataFrame,
    monthly_risk_free: pd.Series | pd.DataFrame | dict | None = None,
) -> dict:
    """Compute all official performance metrics from the same net monthly NAV."""
    frame = nav.sort_values("date").reset_index(drop=True)
    values = frame["net_nav"].to_numpy(dtype=float)
    returns = frame["monthly_net_return"].dropna().to_numpy(dtype=float)
    return_dates = frame.loc[frame["monthly_net_return"].notna(), "date"]
    elapsed_days = (pd.Timestamp(frame.iloc[-1]["date"]) - pd.Timestamp(frame.iloc[0]["date"])).days
    cagr = (
        float((values[-1] / values[0]) ** (365.2425 / elapsed_days) - 1.0)
        if elapsed_days > 0 and values[0] > 0 and values[-1] > 0
        else (-1.0 if values[-1] == 0 and elapsed_days > 0 else np.nan)
    )
    peaks = np.maximum.accumulate(values)
    drawdowns = values / np.where(peaks > 0, peaks, 1.0) - 1.0
    max_drawdown = float(drawdowns.min())
    duration = current = 0
    for value in drawdowns:
        current = current + 1 if value < 0 else 0
        duration = max(duration, current)
    volatility = float(np.std(returns, ddof=1) * np.sqrt(12)) if len(returns) > 1 else np.nan
    rf = _risk_free_returns(monthly_risk_free, return_dates)
    sharpe = sortino = np.nan
    if rf is not None and len(returns) > 1:
        excess = returns - rf
        excess_std = float(np.std(excess, ddof=1))
        if excess_std > 0:
            sharpe = float(np.mean(excess) / excess_std * np.sqrt(12))
        downside = np.minimum(excess, 0.0)
        downside_deviation = float(np.sqrt(np.mean(downside ** 2)) * np.sqrt(12))
        if downside_deviation > 0:
            sortino = float(np.mean(excess) * 12 / downside_deviation)
    calmar = float(cagr / abs(max_drawdown)) if max_drawdown < 0 and np.isfinite(cagr) else np.nan
    return {
        "metric_nav_column": "net_nav",
        "cagr": cagr,
        "volatility": volatility,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_drawdown,
        "max_drawdown_duration_months": duration,
        "calmar": calmar,
        "best_month": float(np.max(returns)) if len(returns) else np.nan,
        "worst_month": float(np.min(returns)) if len(returns) else np.nan,
        "negative_months": int((returns < 0).sum()),
    }
