from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backtest.free_data_v1_evidence import performance_contract
from backtest.free_data_v1_nav import (
    DGS1MO_NAMESPACE,
    FROZEN_B1C_MANIFEST_SHA256,
    OUTCOME_NAMESPACES,
    PERFORMANCE_NAMESPACES,
    ZERO_RATE_NAMESPACE,
    EvidenceBundle,
    EvidenceValidationError,
    RiskFreeUnavailableError,
    calculate_performance_metrics,
    load_frozen_b1c_evidence,
    run_performance_engine,
    validate_evidence_bundle,
)


FROZEN_B1C_ROOT = Path(
    "artifacts/performance_inputs/free_data_v1/20260731T115106Z-b1c"
)


def _market_close(day: str | pd.Timestamp) -> pd.Timestamp:
    stamp = pd.Timestamp(day)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_localize(None)
    return stamp.normalize().tz_localize("UTC") + pd.Timedelta(hours=20)


def _business_month_end(period: pd.Period) -> pd.Timestamp:
    day = period.end_time.normalize()
    while day.weekday() >= 5:
        day -= pd.Timedelta(days=1)
    return _market_close(day)


def _clock(year: int) -> tuple[pd.Timestamp, pd.Timestamp, list[pd.Timestamp]]:
    entry = _market_close(f"{year}-07-03")
    target_exit = entry + pd.DateOffset(months=36)
    exit_timestamp = target_exit
    month_ends = [
        _business_month_end(period)
        for period in pd.period_range(entry.tz_localize(None).to_period("M"), periods=36)
    ]
    assert month_ends[-1] < exit_timestamp
    return entry, exit_timestamp, month_ends


def _synthetic_bundle(
    *,
    years: tuple[int, ...] = (2020,),
    holdings_per_vintage: int = 2,
    stock_monthly_growth: float = 0.0,
    benchmark_monthly_growth: float = 0.0,
) -> EvidenceBundle:
    contract = performance_contract()
    requirement_rows = []
    identity_rows = []
    action_rows = []
    coverage_rows = []
    symbols: set[str] = {"BM"}
    holding_ids: list[str] = []

    for year in years:
        entry, exit_timestamp, month_ends = _clock(year)
        decision = pd.Timestamp(f"{year}-07-02T00:00:00Z")
        prediction = pd.Timestamp(f"{year}-07-02T00:01:00Z")
        for number in range(holdings_per_vintage):
            ticker = f"S{year % 100:02d}{number}"
            requirement_id = f"holding:{year}:{number}"
            stable_row_id = f"row-{year}-{number}"
            holding_ids.append(requirement_id)
            symbols.add(ticker)
            weight = 1.0 / holdings_per_vintage
            requirement_rows.append(
                {
                    "requirement_id": requirement_id,
                    "stable_row_id": stable_row_id,
                    "instrument_role": "holding",
                    "sec_cik": f"{year}{number}",
                    "ticker": ticker,
                    "provider_symbol": ticker,
                    "provider_exchange": "NMS",
                    "exchange_calendar": "XNYS",
                    "required_start": entry,
                    "required_end": exit_timestamp,
                    "requirement_state": "matured_2015_2023",
                    "decision_year": year,
                    "benchmark_symbol": "BM",
                    "entity_id": f"entity-{year}-{number}",
                    "cik": f"{year}{number}",
                    "exchange": "NASDAQ",
                    "decision_timestamp": decision,
                    "prediction_timestamp": prediction,
                    "entry_timestamp": entry,
                    "entry_session_date": entry.date().isoformat(),
                    "target_exit_timestamp": entry + pd.DateOffset(months=36),
                    "calendar_exit_timestamp": exit_timestamp,
                    "holding_months": 36,
                    "weight": weight,
                    "planned_vintage_aum_usd": 100.0,
                    "planned_entry_notional_usd": 100.0 * weight,
                    "transaction_cost_rate_per_side": 0.0025,
                    "transaction_cost_basis": "absolute_actual_traded_notional",
                    "vintage_clock_status": "calendar_exit_available",
                }
            )
            identity_rows.append(
                {
                    "requirement_id": requirement_id,
                    "instrument_role": "holding",
                    "stable_row_id": stable_row_id,
                    "issuer_id": f"issuer-{year}-{number}",
                    "security_id": f"security-{year}-{number}",
                    "ticker": ticker,
                    "required_start": entry,
                    "required_end": exit_timestamp,
                    "s1_coverage_status": "ambiguous",
                    "s1_identity_status": "current_association_only",
                    "s1_listing_status": "unsupported",
                    "s1_security_type_status": "unsupported",
                    "s1_event_status": "unresolved",
                    "s1_price_adjustment_status": "provider_adjusted_close_only",
                    "reason_codes": [],
                    "source_ids": [],
                    "current_ticker_substitution_used": False,
                    "ticker_chaining_used": False,
                    "dated_security_lineage_complete": False,
                    "identity_rule_status": "current_association_only_not_dated_security_lineage",
                    "certified_performance_identity_available": False,
                }
            )
            action_rows.append(
                {
                    "requirement_id": requirement_id,
                    "instrument_role": "holding",
                    "stable_row_id": stable_row_id,
                    "security_id": f"security-{year}-{number}",
                    "ticker": ticker,
                    "s1_event_status": "unresolved",
                    "s1_price_adjustment_status": "provider_adjusted_close_only",
                    "source_event_indicator_count": 0,
                    "holding_window_indicator_count": 0,
                    "deterministic_action": "unresolved",
                    "historical_evaluation_status": "unresolved",
                    "planned_document_count": np.nan,
                    "retrieved_document_count": np.nan,
                    "claim_count": np.nan,
                    "document_set_status": None,
                    "transaction_or_action_terms_complete": None,
                    "summary_status": None,
                    "primary_document_support_state": "not_in_current_primary_document_review",
                    "provider_adjclose_action_semantics_certified": False,
                    "forward_fill_across_unresolved_event_allowed": False,
                    "event_inferred_from_disappearance_or_form_family": False,
                    "unsupported_recovery_allowed_in_observed_namespace": False,
                    "assumed_outcome_allowed_in_labels_or_training": False,
                    "primary_return_available": False,
                    "event_type": None,
                    "event_effective_timestamp": pd.NaT,
                    "outcome_namespace": None,
                    "event_source_id": None,
                    "successor_symbol": None,
                    "successor_share_ratio": np.nan,
                    "cash_per_old_share": np.nan,
                    "cash_fraction": np.nan,
                    "terminal_total_return": np.nan,
                    "event_terms_complete": False,
                }
            )
            coverage_rows.append(
                {
                    "requirement_id": requirement_id,
                    "stable_row_id": stable_row_id,
                    "instrument_role": "holding",
                    "ticker": ticker,
                    "provider_symbol": ticker,
                    "requirement_state": "matured_2015_2023",
                    "decision_year": year,
                    "s1_coverage_status": "ambiguous",
                    "s1_identity_status": "current_association_only",
                    "s1_event_status": "unresolved",
                    "fully_matched_s1_requirement": False,
                    "provider_adjclose_semantics_certified": False,
                    "certified_security_action_ledger": False,
                    "benchmark_gap_scenario_imputation_allowed": False,
                    "assigned_benchmark_symbol": "BM",
                    "entry_observed_common": True,
                    "exit_observed_common": True,
                    "required_month_end_count": len(month_ends),
                    "observed_common_month_end_count": len(month_ends),
                    "missing_common_month_end_count": 0,
                    "benchmark_gap_count": 0,
                    "evidence_end_timestamp": exit_timestamp,
                    "price_coverage_status": "complete_common_entry_month_ends_and_exit",
                    "relative_evidence_status": "observed_common_provider_evidence_available",
                }
            )

    first_entry = min(_clock(year)[0] for year in years)
    last_exit = max(_clock(year)[1] for year in years)
    benchmark_id = "benchmark:BM"
    requirement_rows.append(
        {
            "requirement_id": benchmark_id,
            "stable_row_id": None,
            "instrument_role": "benchmark",
            "sec_cik": None,
            "ticker": "BM",
            "provider_symbol": "BM",
            "provider_exchange": "PCX",
            "exchange_calendar": "XNYS",
            "required_start": first_entry,
            "required_end": last_exit,
            "requirement_state": "unsupported_incomplete_benchmark_master",
            "decision_year": np.nan,
            "benchmark_symbol": "BM",
            "entity_id": None,
            "cik": None,
            "exchange": None,
            "decision_timestamp": pd.NaT,
            "prediction_timestamp": pd.NaT,
            "entry_timestamp": pd.NaT,
            "entry_session_date": None,
            "target_exit_timestamp": pd.NaT,
            "calendar_exit_timestamp": pd.NaT,
            "holding_months": np.nan,
            "weight": np.nan,
            "planned_vintage_aum_usd": np.nan,
            "planned_entry_notional_usd": np.nan,
            "transaction_cost_rate_per_side": np.nan,
            "transaction_cost_basis": None,
            "vintage_clock_status": None,
        }
    )
    identity_rows.append(
        {
            "requirement_id": benchmark_id,
            "instrument_role": "benchmark",
            "stable_row_id": None,
            "issuer_id": None,
            "security_id": "benchmark-BM",
            "ticker": "BM",
            "required_start": first_entry,
            "required_end": last_exit,
            "s1_coverage_status": "unsupported",
            "s1_identity_status": "unsupported",
            "s1_listing_status": "unsupported",
            "s1_security_type_status": "unsupported",
            "s1_event_status": "unsupported",
            "s1_price_adjustment_status": "provider_adjusted_close_only",
            "reason_codes": [],
            "source_ids": [],
            "current_ticker_substitution_used": False,
            "ticker_chaining_used": False,
            "dated_security_lineage_complete": False,
            "identity_rule_status": "benchmark_master_incomplete",
            "certified_performance_identity_available": False,
        }
    )
    action_rows.append(
        {
            "requirement_id": benchmark_id,
            "instrument_role": "benchmark",
            "stable_row_id": None,
            "security_id": "benchmark-BM",
            "ticker": "BM",
            "s1_event_status": "unsupported",
            "s1_price_adjustment_status": "provider_adjusted_close_only",
            "source_event_indicator_count": np.nan,
            "holding_window_indicator_count": np.nan,
            "deterministic_action": "unsupported_benchmark_master",
            "historical_evaluation_status": None,
            "planned_document_count": np.nan,
            "retrieved_document_count": np.nan,
            "claim_count": np.nan,
            "document_set_status": None,
            "transaction_or_action_terms_complete": None,
            "summary_status": None,
            "primary_document_support_state": "not_collected_for_benchmark_master",
            "provider_adjclose_action_semantics_certified": False,
            "forward_fill_across_unresolved_event_allowed": False,
            "event_inferred_from_disappearance_or_form_family": False,
            "unsupported_recovery_allowed_in_observed_namespace": False,
            "assumed_outcome_allowed_in_labels_or_training": False,
            "primary_return_available": False,
            "event_type": None,
            "event_effective_timestamp": pd.NaT,
            "outcome_namespace": None,
            "event_source_id": None,
            "successor_symbol": None,
            "successor_share_ratio": np.nan,
            "cash_per_old_share": np.nan,
            "cash_fraction": np.nan,
            "terminal_total_return": np.nan,
            "event_terms_complete": False,
        }
    )
    coverage_rows.append(
        {
            "requirement_id": benchmark_id,
            "stable_row_id": None,
            "instrument_role": "benchmark",
            "ticker": "BM",
            "provider_symbol": "BM",
            "requirement_state": "unsupported_incomplete_benchmark_master",
            "decision_year": np.nan,
            "s1_coverage_status": "unsupported",
            "s1_identity_status": "unsupported",
            "s1_event_status": "unsupported",
            "fully_matched_s1_requirement": False,
            "provider_adjclose_semantics_certified": False,
            "certified_security_action_ledger": False,
            "benchmark_gap_scenario_imputation_allowed": False,
            "assigned_benchmark_symbol": "BM",
            "entry_observed_common": True,
            "exit_observed_common": True,
            "required_month_end_count": np.nan,
            "observed_common_month_end_count": np.nan,
            "missing_common_month_end_count": np.nan,
            "benchmark_gap_count": 0,
            "evidence_end_timestamp": last_exit,
            "price_coverage_status": "payload_supported_to_evidence_end_but_master_requirement_incomplete",
            "relative_evidence_status": "benchmark_master_identity_action_incomplete",
        }
    )

    max_period = last_exit.tz_localize(None).to_period("M")
    all_dates = {
        _business_month_end(period)
        for period in pd.period_range(
            first_entry.tz_localize(None).to_period("M"), max_period
        )
    }
    for year in years:
        entry, exit_timestamp, _ = _clock(year)
        all_dates.update((entry, exit_timestamp))
    ordered_dates = sorted(all_dates)
    price_rows = []
    for symbol in sorted(symbols):
        growth = benchmark_monthly_growth if symbol == "BM" else stock_monthly_growth
        for index, instant in enumerate(ordered_dates):
            value = 100.0 * (1.0 + growth) ** index
            price_rows.append(
                {
                    "symbol": symbol,
                    "source_timestamp": instant,
                    "raw_close": value,
                    "total_return_close": value,
                    "session_date": instant.date().isoformat(),
                    "market_open": instant - pd.Timedelta(hours=6, minutes=30),
                    "market_close": instant,
                    "instrument_role": "benchmark" if symbol == "BM" else "holding",
                    "provider_exchange": "PCX" if symbol == "BM" else "NMS",
                    "exchange_calendar": "XNYS",
                    "currency": "USD",
                    "provider_instrument_type": "ETF" if symbol == "BM" else "EQUITY",
                    "provider_timezone": "America/New_York",
                    "data_granularity": "1d",
                    "raw_response_sha256": "1" * 64,
                    "normalized_sha256": "2" * 64,
                    "price_evidence_status": "synthetic_observed",
                    "adjustment_semantics": "synthetic total-return close",
                }
            )

    requirements = pd.DataFrame(requirement_rows)
    coverage = pd.DataFrame(coverage_rows)
    eligibility: dict[str, pd.DataFrame] = {}
    namespace_contracts = {}
    for namespace in PERFORMANCE_NAMESPACES:
        namespace_contracts[namespace] = {
            "namespace": namespace,
            **contract["namespaces"][namespace],
        }
        rows = []
        for requirement in requirement_rows:
            benchmark = requirement["instrument_role"] == "benchmark"
            rows.append(
                {
                    "requirement_id": requirement["requirement_id"],
                    "stable_row_id": requirement["stable_row_id"],
                    "instrument_role": requirement["instrument_role"],
                    "ticker": requirement["ticker"],
                    "requirement_state": requirement["requirement_state"],
                    "price_coverage_status": coverage.loc[
                        coverage["requirement_id"].eq(requirement["requirement_id"]),
                        "price_coverage_status",
                    ].iloc[0],
                    "s1_coverage_status": "unsupported" if benchmark else "ambiguous",
                    "relative_evidence_status": coverage.loc[
                        coverage["requirement_id"].eq(requirement["requirement_id"]),
                        "relative_evidence_status",
                    ].iloc[0],
                    "namespace": namespace,
                    "eligibility_state": (
                        "benchmark_master_incomplete"
                        if benchmark
                        else "eligible_no_terminal_scenario_required"
                    ),
                    "eligible_for_future_nav_engine": not benchmark,
                    "scenario_triggered": False,
                    "scenario_return_if_triggered": contract["namespaces"][namespace].get(
                        "terminal_assumption"
                    ),
                    "assumed_outcome_used": False,
                    "allowed_in_labels_or_training": False,
                    "benchmark_gap_imputed": False,
                    "performance_calculated": False,
                }
            )
        eligibility[namespace] = pd.DataFrame(rows)

    rate_status = {
        "schema_version": 1,
        "dgs1mo_namespace": DGS1MO_NAMESPACE,
        "dgs1mo_available": False,
        "dgs1mo_status": "unavailable_synthetic_fixture",
        "observations": None,
        "zero_risk_free_namespace": {
            "namespace": ZERO_RATE_NAMESPACE,
            "available": True,
            "risk_free_return": 0.0,
            "diagnostic_only": True,
            "physically_and_semantically_separate_from_dgs1mo": True,
        },
    }
    benchmark_requirements = coverage[coverage["instrument_role"].eq("benchmark")].copy()
    benchmark_requirements["required_start"] = first_entry
    benchmark_requirements["required_end"] = last_exit
    benchmark_requirements["assigned_holding_count"] = len(holding_ids)
    return EvidenceBundle(
        requirements=requirements,
        security_identity=pd.DataFrame(identity_rows),
        security_actions=pd.DataFrame(action_rows),
        prices=pd.DataFrame(price_rows),
        benchmark_requirements=benchmark_requirements,
        coverage=coverage,
        namespace_eligibility=eligibility,
        performance_contract=contract,
        namespace_contracts=namespace_contracts,
        rate_status=rate_status,
    )


def _with_event(bundle: EvidenceBundle, requirement_id: str, **values) -> EvidenceBundle:
    actions = bundle.security_actions.copy()
    mask = actions["requirement_id"].eq(requirement_id)
    assert mask.sum() == 1
    outcome = values.get("outcome_namespace")
    if outcome in {"observed", "provider_confirmed"}:
        values.setdefault("primary_return_available", True)
        values.setdefault("deterministic_action", "resolved")
    elif outcome == "unsupported_unresolved":
        values.setdefault("primary_return_available", False)
        values.setdefault("deterministic_action", "unresolved")
    for name, value in values.items():
        if name == "event_effective_timestamp":
            actions[name] = actions[name].astype("object")
        actions.loc[mask, name] = value
    return replace(bundle, security_actions=actions)


def _add_symbol(bundle: EvidenceBundle, old_symbol: str, new_symbol: str) -> EvidenceBundle:
    prices = bundle.prices.copy()
    extra = prices[prices["symbol"].eq(old_symbol)].copy()
    extra["symbol"] = new_symbol
    extra["raw_close"] = 100.0
    extra["total_return_close"] = 100.0
    return replace(bundle, prices=pd.concat([prices, extra], ignore_index=True))


def test_frozen_b1c_manifest_and_all_records_load_without_performance():
    bundle = load_frozen_b1c_evidence(FROZEN_B1C_ROOT)
    assert bundle.source_manifest_sha256 == FROZEN_B1C_MANIFEST_SHA256
    assert len(bundle.requirements) == 184
    assert len(bundle.prices) == 512_413
    assert bundle.rate_status["dgs1mo_available"] is False


def test_overlapping_vintages_remain_independent_and_aggregate_capital_once():
    bundle = _synthetic_bundle(years=(2020, 2021), holdings_per_vintage=1)
    result = run_performance_engine(
        bundle,
        performance_namespace="best_free_evidence_full_accounting",
    )
    assert result.available
    first = result.vintage_nav[result.vintage_nav["decision_year"].eq(2020)]
    assert first["date"].max() == _clock(2020)[1]
    assert result.aggregate_nav["active_vintage_count"].max() == 2
    assert result.aggregate_nav["external_contributions"].sum() == pytest.approx(200.0)
    first_trades = result.transaction_ledger[
        result.transaction_ledger["decision_year"].eq(2020)
    ]
    assert not first_trades["date"].eq(_clock(2021)[0]).any()
    assert len(result.capital_ledger) == 2


def test_exact_entry_month_end_exit_and_actual_notional_costs():
    bundle = _synthetic_bundle()
    result = run_performance_engine(
        bundle,
        performance_namespace="best_free_evidence_full_accounting",
    )
    assert result.available
    entry, exit_timestamp, month_ends = _clock(2020)
    path = result.holding_ledger[
        result.holding_ledger["requirement_id"].eq("holding:2020:0")
    ]
    assert path["date"].tolist() == sorted(set([entry, *month_ends, exit_timestamp]))
    portfolio_trades = result.transaction_ledger[
        result.transaction_ledger["stream"].eq("portfolio")
    ]
    entries = portfolio_trades[portfolio_trades["side"].eq("entry_buy")]
    exits = portfolio_trades[portfolio_trades["side"].eq("exit_sell")]
    assert entries["actual_traded_notional"].sum() == pytest.approx(100.0)
    assert entries["transaction_cost"].sum() == pytest.approx(0.25)
    assert exits["actual_traded_notional"].sum() == pytest.approx(100.0)
    assert exits["transaction_cost"].sum() == pytest.approx(0.25)
    assert result.vintage_nav.iloc[-1]["gross_nav"] == pytest.approx(1.0)
    assert result.vintage_nav.iloc[-1]["net_nav"] == pytest.approx(0.995)
    assert result.aggregate_metrics["net"]["turnover"] == pytest.approx(2.0)


def test_partial_and_complete_cash_exits_preserve_actual_exit_notional():
    bundle = _synthetic_bundle()
    partial = _with_event(
        bundle,
        "holding:2020:0",
        event_type="partial_cash_exit",
        event_effective_timestamp=_market_close("2021-01-15"),
        outcome_namespace="observed",
        event_source_id="synthetic:partial-exit",
        cash_per_old_share=100.0,
        cash_fraction=0.5,
        event_terms_complete=True,
    )
    result = run_performance_engine(
        partial,
        performance_namespace="best_free_evidence_full_accounting",
    )
    exits = result.transaction_ledger[
        result.transaction_ledger["side"].eq("exit_sell")
        & result.transaction_ledger["stream"].eq("portfolio")
        & result.transaction_ledger["requirement_id"].eq("holding:2020:0")
    ]
    assert exits["actual_traded_notional"].iloc[0] == pytest.approx(25.0)
    assert exits["transaction_cost"].iloc[0] == pytest.approx(0.0625)

    complete = _with_event(
        bundle,
        "holding:2020:0",
        event_type="cash_merger",
        event_effective_timestamp=_market_close("2021-01-15"),
        outcome_namespace="provider_confirmed",
        event_source_id="provider:cash-merger",
        cash_per_old_share=100.0,
        event_terms_complete=True,
    )
    complete_result = run_performance_engine(
        complete,
        performance_namespace="best_free_evidence_full_accounting",
    )
    complete_exits = complete_result.transaction_ledger[
        complete_result.transaction_ledger["side"].eq("exit_sell")
        & complete_result.transaction_ledger["stream"].eq("portfolio")
        & complete_result.transaction_ledger["requirement_id"].eq("holding:2020:0")
    ]
    assert complete_exits.empty
    assert len(complete_result.outcome_ledgers["provider_confirmed"]) == 1


@pytest.mark.parametrize("event_type", ["ticker_change", "stock_merger"])
def test_ticker_and_stock_identity_continuity_requires_explicit_terms(event_type):
    bundle = _synthetic_bundle(holdings_per_vintage=1)
    bundle = _add_symbol(bundle, "S200", "NEXT")
    changed = _with_event(
        bundle,
        "holding:2020:0",
        event_type=event_type,
        event_effective_timestamp=_market_close("2021-01-15"),
        outcome_namespace="provider_confirmed",
        event_source_id=f"provider:{event_type}",
        successor_symbol="NEXT",
        successor_share_ratio=1.0,
        cash_per_old_share=0.0,
        event_terms_complete=True,
    )
    result = run_performance_engine(
        changed,
        performance_namespace="best_free_evidence_full_accounting",
    )
    assert result.available
    assert result.holding_ledger.iloc[-1]["active_symbol"] == "NEXT"
    broken = _with_event(changed, "holding:2020:0", successor_symbol=None)
    with pytest.raises(EvidenceValidationError, match="continuity terms"):
        run_performance_engine(
            broken,
            performance_namespace="best_free_evidence_full_accounting",
        )


def test_bankruptcy_delisting_and_unsupported_terminal_paths_are_separate():
    bundle = _synthetic_bundle(holdings_per_vintage=1)
    bankruptcy = _with_event(
        bundle,
        "holding:2020:0",
        event_type="bankruptcy",
        event_effective_timestamp=_market_close("2021-01-15"),
        outcome_namespace="provider_confirmed",
        event_source_id="provider:bankruptcy-order",
        terminal_total_return=-1.0,
        event_terms_complete=True,
        primary_return_available=True,
    )
    provider_result = run_performance_engine(
        bankruptcy,
        performance_namespace="best_free_evidence_full_accounting",
    )
    assert provider_result.available
    assert provider_result.vintage_nav.iloc[-1]["gross_nav"] == 0.0
    observed_result = run_performance_engine(
        bankruptcy,
        performance_namespace="observed_available_diagnostic",
    )
    assert not observed_result.available
    assert observed_result.unavailable_reasons[0]["code"] == (
        "provider_confirmed_outcome_not_observed"
    )

    unsupported = _with_event(
        bundle,
        "holding:2020:0",
        event_type="delisting",
        event_effective_timestamp=_market_close("2021-01-15"),
        outcome_namespace="unsupported_unresolved",
        event_source_id=None,
        event_terms_complete=False,
    )
    unavailable = run_performance_engine(
        unsupported,
        performance_namespace="best_free_evidence_full_accounting",
    )
    assert not unavailable.available
    assert len(unavailable.outcome_ledgers["unsupported_unresolved"]) == 1
    sensitivity = run_performance_engine(
        unsupported,
        performance_namespace="legacy_minus_50_percent_unsupported_exit",
    )
    stress = run_performance_engine(
        unsupported,
        performance_namespace="conservative_terminal_loss_100_percent",
    )
    assert sensitivity.available and stress.available
    assert sensitivity.vintage_nav.iloc[-1]["gross_nav"] == pytest.approx(0.5)
    assert stress.vintage_nav.iloc[-1]["gross_nav"] == 0.0
    assert len(sensitivity.outcome_ledgers["bounded_scenario"]) == 1
    assert len(sensitivity.outcome_ledgers["unsupported_unresolved"]) == 1


def test_all_outcome_namespaces_and_gross_net_outputs_are_physically_separate():
    result = run_performance_engine(
        _synthetic_bundle(),
        performance_namespace="best_free_evidence_full_accounting",
    )
    assert set(result.outcome_ledgers) == set(OUTCOME_NAMESPACES)
    for namespace, frame in result.outcome_ledgers.items():
        if len(frame):
            assert frame["outcome_namespace"].eq(namespace).all()
    assert result.aggregate_nav["gross_nav"].iloc[-1] == pytest.approx(1.0)
    assert result.aggregate_nav["net_nav"].iloc[-1] == pytest.approx(0.995)
    assert result.aggregate_metrics["gross"] is not result.aggregate_metrics["net"]
    assert result.coverage["holding_count_denominator"] == 2
    assert result.coverage["capital_denominator"] == 100.0
    assert result.coverage["benchmark_gap_count"] == 0


def test_benchmark_gap_is_not_imputed_even_when_coverage_claims_complete():
    bundle = _synthetic_bundle(holdings_per_vintage=1)
    missing = _clock(2020)[2][4]
    prices = bundle.prices[
        ~(
            bundle.prices["symbol"].eq("BM")
            & bundle.prices["market_close"].eq(missing)
        )
    ].copy()
    broken = replace(bundle, prices=prices)
    with pytest.raises(EvidenceValidationError, match="benchmark-session gap"):
        run_performance_engine(
            broken,
            performance_namespace="best_free_evidence_full_accounting",
        )


def test_missing_stock_price_is_not_forward_filled_or_scenario_repaired():
    bundle = _synthetic_bundle(holdings_per_vintage=1)
    missing = _clock(2020)[2][4]
    prices = bundle.prices[
        ~(
            bundle.prices["symbol"].eq("S200")
            & bundle.prices["market_close"].eq(missing)
        )
    ].copy()
    broken = replace(bundle, prices=prices)
    with pytest.raises(EvidenceValidationError, match="missing exact price"):
        run_performance_engine(
            broken,
            performance_namespace="conservative_terminal_loss_100_percent",
        )


def test_dgs1mo_unavailable_fails_closed_and_zero_rate_stays_diagnostic():
    bundle = _synthetic_bundle()
    with pytest.raises(RiskFreeUnavailableError, match="exact frozen observations"):
        run_performance_engine(
            bundle,
            performance_namespace="best_free_evidence_full_accounting",
            risk_free_namespace=DGS1MO_NAMESPACE,
        )
    result = run_performance_engine(
        bundle,
        performance_namespace="best_free_evidence_full_accounting",
        risk_free_namespace=ZERO_RATE_NAMESPACE,
    )
    assert result.available
    assert result.aggregate_metrics["net"]["risk_free_namespace"] == ZERO_RATE_NAMESPACE
    assert result.coverage["risk_free_rate_namespace"] != DGS1MO_NAMESPACE


def test_metric_formulas_beta_alpha_tracking_information_and_hit_rate():
    returns = np.array([0.02, -0.01, 0.03, 0.00, 0.04, -0.02])
    benchmark = np.array([0.01, -0.005, 0.015, 0.002, 0.02, -0.01])
    dates = pd.date_range("2020-01-31", periods=len(returns), freq="ME", tz="UTC")
    frame = pd.DataFrame(
        {
            "date": dates,
            "net_return": returns,
            "net_nav": np.cumprod(1 + returns),
            "benchmark_net_return": benchmark,
            "benchmark_net_nav": np.cumprod(1 + benchmark),
        }
    )
    metrics = calculate_performance_metrics(
        frame,
        return_column="net_return",
        nav_column="net_nav",
        benchmark_return_column="benchmark_net_return",
        benchmark_nav_column="benchmark_net_nav",
        risk_free_returns=np.zeros(len(frame)),
        start_date=pd.Timestamp("2020-01-01", tz="UTC"),
        risk_free_namespace=ZERO_RATE_NAMESPACE,
    )
    expected_beta = np.cov(returns, benchmark, ddof=1)[0, 1] / np.var(
        benchmark, ddof=1
    )
    active = returns - benchmark
    assert metrics["beta"] == pytest.approx(expected_beta)
    assert metrics["alpha"] == pytest.approx(
        (returns.mean() - expected_beta * benchmark.mean()) * 12
    )
    assert metrics["tracking_error"] == pytest.approx(
        np.std(active, ddof=1) * np.sqrt(12)
    )
    assert metrics["information_ratio"] == pytest.approx(
        active.mean() / np.std(active, ddof=1) * np.sqrt(12)
    )
    assert metrics["hit_rate"] == pytest.approx(np.mean(active > 0))
    assert metrics["maximum_drawdown"] < 0
    assert np.isfinite(metrics["cagr"])
    assert np.isfinite(metrics["annualized_volatility"])
    assert np.isfinite(metrics["sharpe_ratio"])
    assert np.isfinite(metrics["sortino_ratio"])
    assert np.isfinite(metrics["calmar_ratio"])


def test_zero_denominators_and_insufficient_history_return_unavailable_not_inf():
    frame = pd.DataFrame(
        {
            "date": [pd.Timestamp("2020-01-31", tz="UTC")],
            "net_return": [0.0],
            "net_nav": [1.0],
            "benchmark_net_return": [0.0],
            "benchmark_net_nav": [1.0],
        }
    )
    metrics = calculate_performance_metrics(
        frame,
        return_column="net_return",
        nav_column="net_nav",
        benchmark_return_column="benchmark_net_return",
        benchmark_nav_column="benchmark_net_nav",
        risk_free_returns=np.zeros(1),
        start_date=frame["date"].iloc[0],
        risk_free_namespace=ZERO_RATE_NAMESPACE,
    )
    for name in (
        "cagr",
        "annualized_volatility",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "beta",
        "alpha",
        "tracking_error",
        "information_ratio",
        "turnover",
    ):
        assert np.isnan(metrics[name])
        assert metrics["availability"][name] is not None
    assert metrics["hit_rate"] == 0.0


def test_deterministic_reruns_reproduce_every_engine_frame_and_metric():
    bundle = _synthetic_bundle(years=(2020, 2021), stock_monthly_growth=0.005)
    first = run_performance_engine(
        bundle,
        performance_namespace="best_free_evidence_full_accounting",
    )
    second = run_performance_engine(
        bundle,
        performance_namespace="best_free_evidence_full_accounting",
    )
    pd.testing.assert_frame_equal(first.vintage_nav, second.vintage_nav)
    pd.testing.assert_frame_equal(first.aggregate_nav, second.aggregate_nav)
    pd.testing.assert_frame_equal(first.holding_ledger, second.holding_ledger)
    pd.testing.assert_frame_equal(first.transaction_ledger, second.transaction_ledger)
    assert first.aggregate_metrics == second.aggregate_metrics
    assert first.coverage == second.coverage


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("schema", "schema missing columns"),
        ("identity", "current ticker substitution flag"),
        ("coverage", "declared benchmark-session gaps"),
        ("namespace", "namespace contract drifted"),
        ("terminal", "effective timestamp"),
    ],
)
def test_schema_identity_coverage_namespace_and_terminal_inconsistencies_fail_closed(
    mutation,
    message,
):
    bundle = _synthetic_bundle(holdings_per_vintage=1)
    if mutation == "schema":
        bundle = replace(bundle, prices=bundle.prices.drop(columns="total_return_close"))
        with pytest.raises(EvidenceValidationError, match=message):
            validate_evidence_bundle(bundle)
        return
    if mutation == "identity":
        identity = bundle.security_identity.copy()
        identity.loc[identity["instrument_role"].eq("holding"), "current_ticker_substitution_used"] = True
        bundle = replace(bundle, security_identity=identity)
        with pytest.raises(EvidenceValidationError, match=message):
            validate_evidence_bundle(bundle)
        return
    if mutation == "coverage":
        coverage = bundle.coverage.copy()
        coverage.loc[coverage["instrument_role"].eq("holding"), "benchmark_gap_count"] = 1
        bundle = replace(bundle, coverage=coverage)
        with pytest.raises(EvidenceValidationError, match=message):
            validate_evidence_bundle(bundle)
        return
    if mutation == "namespace":
        contracts = dict(bundle.namespace_contracts)
        contracts["best_free_evidence_full_accounting"] = {
            **contracts["best_free_evidence_full_accounting"],
            "silent_holding_drop_allowed": True,
        }
        bundle = replace(bundle, namespace_contracts=contracts)
        with pytest.raises(EvidenceValidationError, match=message):
            validate_evidence_bundle(bundle)
        return
    bundle = _with_event(
        bundle,
        "holding:2020:0",
        event_type="delisting",
        event_effective_timestamp=pd.NaT,
        outcome_namespace="provider_confirmed",
        event_source_id="provider:bad",
        terminal_total_return=-1.0,
        event_terms_complete=True,
    )
    with pytest.raises(EvidenceValidationError, match=message):
        run_performance_engine(
            bundle,
            performance_namespace="best_free_evidence_full_accounting",
        )
