from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backtest.m1d_portfolio_performance import (
    M1DContractError,
    M1D_SCORE_STATUSES,
    P4_ROOT,
    _build_requirements_and_evidence,
    _locked_winners,
    _m1c_predictions_for_p4,
    _parse_test_report,
    _verify_contract_identity,
    prepare_m1d_lock,
    render_m1d_report,
    verify_m1d_lock,
)


def _junit(path: Path, *, failures: int = 0) -> Path:
    path.write_text(
        '<testsuites tests="3" failures="{failures}" errors="0" skipped="0">'
        '<testsuite name="focused" tests="3" failures="{failures}" '
        'errors="0" skipped="0"/></testsuites>'.format(failures=failures)
    )
    return path


def test_focused_test_report_must_pass(tmp_path: Path) -> None:
    passed = _parse_test_report(_junit(tmp_path / "passed.xml"))
    assert passed["tests"] == 3
    assert passed["passed"] == 3
    with pytest.raises(M1DContractError, match="did not pass"):
        _parse_test_report(_junit(tmp_path / "failed.xml", failures=1))


def test_locked_route_uses_only_inner_winners() -> None:
    winners = _locked_winners()
    assert len(winners) == 16
    assert {(row["target_role"]) for row in winners} == {
        "decision_tree",
        "lightgbm_regression",
    }
    assert all(row["selection_inputs"] == "inner_validation_only" for row in winners)
    assert not any(row["outer_oos_metric_consumed"] for row in winners)
    assert not any(row["portfolio_or_b1e_metric_consumed"] for row in winners)


def test_frozen_p4_b1d_b1e_contract_identity() -> None:
    result = _verify_contract_identity()
    assert result["p4_selection_contract"]["tree_threshold"] == 0.55
    assert result["p4_selection_contract"]["target_n"] == 15
    assert result["p4_selection_contract"]["hard_gates"] == [
        "market_us",
        "market_cap",
        "beneish",
        "piotroski",
        "roa_positive",
        "altman",
        "value",
        "momentum",
    ]


def test_m1c_row_complete_p4_mapping_preserves_open_boundary() -> None:
    mapped, lineage, validation = _m1c_predictions_for_p4()
    assert len(mapped) == len(lineage) == 87_612
    assert validation["source_rows"] == 43_806
    assert validation["open_2024_2026_scores_portfolio_eligible"] is True
    assert validation["open_2024_2026_scores_metric_eligible"] is False
    score_rows = lineage["prediction_status"].isin(M1D_SCORE_STATUSES)
    assert mapped.loc[score_rows, "prediction_status"].eq(
        "oos_prediction_available"
    ).all()
    open_rows = lineage["prediction_status"].eq("production_score_open_unlabeled")
    assert open_rows.sum() == 24_566
    assert not lineage.loc[open_rows, "metric_eligible"].astype(bool).any()


def test_prepare_lock_is_preperformance_and_non_overwriting(tmp_path: Path) -> None:
    version = "20990101T000000Z-m1d"
    root = tmp_path / version
    report = _junit(tmp_path / "focused.xml")
    locked = prepare_m1d_lock(
        root,
        version=version,
        focused_test_report=report,
        created_at_utc="2099-01-01T00:00:00+00:00",
    )
    assert locked.route["performance_result_observed"] is False
    assert locked.route["adaptive_retry_allowed"] is False
    assert locked.preflight["portfolio_constructed"] is False
    assert locked.preflight["performance_calculated"] is False
    verified = verify_m1d_lock(
        root,
        expected_lock_manifest_sha256=locked.lock_manifest_sha256,
    )
    assert verified.lock_manifest_sha256 == locked.lock_manifest_sha256
    with pytest.raises(M1DContractError, match="not empty"):
        prepare_m1d_lock(root, version=version, focused_test_report=report)


def test_started_lock_cannot_be_retried(tmp_path: Path) -> None:
    version = "20990101T000001Z-m1d"
    root = tmp_path / version
    locked = prepare_m1d_lock(
        root,
        version=version,
        focused_test_report=_junit(tmp_path / "focused.xml"),
    )
    state = root / "state/02_execution_started.json"
    state.write_text(json.dumps({"state": "started"}))
    with pytest.raises(M1DContractError, match="already started"):
        verify_m1d_lock(
            root,
            expected_lock_manifest_sha256=locked.lock_manifest_sha256,
        )


def test_evidence_adapter_preserves_frozen_p4_boundary() -> None:
    portfolio = {
        "holdings": pd.read_parquet(P4_ROOT / "outputs/holdings.parquet"),
        "vintage_plan": pd.read_parquet(
            P4_ROOT / "outputs/backtest_vintage_plan.parquet"
        ),
        "liquidity_coverage": pd.read_parquet(
            P4_ROOT / "support/liquidity_coverage.parquet"
        ),
    }
    bundle, summary, lineage = _build_requirements_and_evidence(portfolio)
    assert len(bundle.requirements) == 184
    assert summary["holding_rows"] == 180
    assert summary["benchmark_gaps"] == 0
    assert summary["missing_common_month_ends"] == 0
    assert len(lineage) == 136


def test_report_renders_threshold_miss_and_limitations() -> None:
    summary = {
        "primary_metrics": {
            "aggregate_net_cagr": 0.25,
            "aggregate_net_zero_rate_sharpe": 0.9,
            "aggregate_net_maximum_drawdown": -0.2,
            "aggregate_net_annualized_volatility": 0.3,
            "aggregate_net_turnover": 2.0,
            "benchmark_net_cagr": 0.1,
        },
        "thresholds": {
            "aggregate_net_cagr_met": False,
            "aggregate_net_zero_rate_sharpe_met": False,
        },
        "coverage": {
            "matured_holding_rows": 75,
            "matured_planned_capital": 1_000_000.0,
            "required_stock_session_count": 2_850,
            "required_benchmark_session_count": 2_850,
            "benchmark_gap_count": 0,
            "scenario_imputed_capital": 0.0,
        },
        "b1e_comparison": {
            "b1e_aggregate_net_cagr": 0.18687,
            "b1e_aggregate_net_zero_rate_sharpe": 0.826,
        },
        "stability": {
            "net_cagr_positive_vintages": 4,
            "net_cagr_median": 0.2,
            "net_cagr_minimum": -0.1,
            "net_cagr_maximum": 0.5,
            "all_physical_namespace_aggregate_nav_values_equal": True,
        },
    }
    report = render_m1d_report(summary)
    assert report.count("not met") == 2
    assert "open/unlabeled 2024-2026" in report
    assert "exact `DGS1MO` ALFRED 2026-07-17 observations remain absent" in report


def test_open_metric_flag_is_boolean_false() -> None:
    frame = pd.DataFrame(
        {"metric_eligible": [False, False], "year": [2024, 2026]}
    )
    assert not frame["metric_eligible"].any()
