from __future__ import annotations

import json

import numpy as np
import pandas as pd

from portfolio.build_canonical_product import (
    BACKTEST_BLOCKERS,
    MIN_ADTV,
    MODEL_ROLES,
    P2_FEATURES,
    P3_PREDICTIONS,
    TARGET_N,
    TRANSACTION_COST_PER_SIDE,
    WEIGHT,
    _candidate_liquidity,
    build_backtest_status,
    build_candidate_frame,
    consume_canonical_predictions,
    materialize_portfolios,
    validate_inputs,
)


def test_canonical_p3_consumption_is_row_complete_and_decision_eligible():
    p3, _, _, preflight = validate_inputs()
    predictions, validation = consume_canonical_predictions(
        pd.read_parquet(P3_PREDICTIONS), p3
    )
    assert preflight["canonical_p3_manifest_sha256"].startswith("8ed9e4a5")
    assert validation["result"] == "pass"
    assert validation["row_role_rows"] == 87_612
    assert validation["source_rows"] == 43_806
    assert validation["available_oos_predictions"] == 77_788
    assert validation["excluded_row_roles"] == 9_824
    assert validation["in_sample_predictions"] == 0
    assert set(validation["roles"]) == set(MODEL_ROLES)
    assert validation["decision_time_eligibility_reconfirmed"]
    assert predictions.duplicated(["stable_row_id", "model_role"]).sum() == 0


def test_canonical_p3_hard_gates_recompute_exactly_from_p2_features():
    p3, _, _, _ = validate_inputs()
    predictions, _ = consume_canonical_predictions(
        pd.read_parquet(P3_PREDICTIONS), p3
    )
    candidates, validation = build_candidate_frame(
        predictions, pd.read_parquet(P2_FEATURES)
    )
    assert validation["result"] == "pass"
    assert validation["source_rows"] == 43_806
    assert validation["hard_gate_recomputation_exact"]
    assert validation["liquidity_required_rows"] > TARGET_N
    assert candidates["liquidity_required"].any()


def test_liquidity_uses_exact_pre_prediction_sessions_and_unadjusted_volume():
    closes = pd.date_range(
        "2025-04-01 20:00:00+00:00", periods=40, freq="B"
    )
    calendar = pd.DataFrame(
        {
            "session_date": closes.strftime("%Y-%m-%d"),
            "market_open": closes - pd.Timedelta(hours=6, minutes=30),
            "market_close": closes,
        }
    )
    prices = pd.DataFrame(
        {
            "source_timestamp": closes - pd.Timedelta(hours=6, minutes=30),
            "session_date": closes.strftime("%Y-%m-%d"),
            "unadjusted_close": 20.0,
            "regular_session_volume": 100_000.0,
            "provider_total_return_close": 20.0,
        }
    )
    row = type(
        "Candidate",
        (),
        {
            "stable_row_id": "row-1",
            "provider_symbol": "ABC",
            "provider_exchange": "NMS",
            "exchange_calendar": "XNAS",
            "prediction_timestamp": closes[-1] - pd.Timedelta(hours=1),
        },
    )()
    meta = {
        "symbol": "ABC",
        "exchangeName": "NMS",
        "currency": "USD",
        "exchangeTimezoneName": "America/New_York",
    }
    evidence, coverage = _candidate_liquidity(
        row,
        prices,
        meta,
        calendar,
        {
            "retrieved_at_utc": "2026-07-16T00:00:00+00:00",
            "response_sha256": "abc",
        },
    )
    assert len(evidence) == 30
    assert evidence["market_close"].lt(row.prediction_timestamp).all()
    assert coverage["valid_session_count"] == 30
    assert coverage["median_30_session_dollar_volume"] == 2_000_000
    assert coverage["median_30_session_dollar_volume"] >= MIN_ADTV
    assert coverage["liquidity_pass"]


def _candidate_frame(n: int, decision: str = "2026-07-02") -> pd.DataFrame:
    rows = []
    for index in range(n):
        row = {
            "stable_row_id": f"row-{index:02d}",
            "entity_id": f"entity-{index}",
            "cik": str(index),
            "ticker": f"T{index}",
            "market": "US",
            "fiscal_year": 2025,
            "period_type": "annual",
            "availability_timestamp": pd.Timestamp(
                f"{decision[:4]}-03-01", tz="UTC"
            ),
            "event_time_materialization_timestamp": pd.Timestamp(
                f"{decision[:4]}-03-01", tz="UTC"
            ),
            "source_feature_available_at_decision": True,
            "decision_timestamp": pd.Timestamp(decision, tz="UTC"),
            "prediction_timestamp": pd.Timestamp(
                decision + " 00:01:00", tz="UTC"
            ),
            "entry_timestamp": pd.NaT,
            "fold_id": "fold",
            "all_non_model_hard_gates_pass": True,
            "hard_gate_exclusion_codes": "[]",
            "source_manifest_sha256": "source",
            "source_dataset_artifact_id": "dataset",
            "source_labels_artifact_id": "labels",
            "source_row_support_artifact_id": "support",
            "provider_symbol": f"T{index}",
            "provider_exchange": "NMS",
            "exchange_calendar": "XNAS",
            "feature_market_cap": 1_000_000_000.0,
            "decision_market_cap": 1_000_000_000.0,
            "beneish_m_score": -3.0,
            "piotroski_f_score": 7.0,
            "piotroski_roa_pos": 1.0,
            "altman_z_score": 3.0,
            "ps_ratio_sector_pct": 0.3,
            "momentum_12m_prior": 0.2,
            "fraud_score_accounting": 0.1,
            "fraud_score_dilution": 0.1,
            "fraud_score_quality": 0.1,
            "fraud_score_distress": 0.1,
            "fraud_score_governance": 0.1,
            "fraud_score_composite": 0.1,
            "tree_role_pass": True,
            "ranker_role_pass": True,
            "tree_threshold_pass": True,
            "liquidity_required": True,
            "decision_tree_prediction": 0.75,
            "decision_tree_prediction_status": "oos_prediction_available",
            "decision_tree_model_artifact_id": "tree-model",
            "lightgbm_regression_prediction": float(n - index),
            "lightgbm_regression_prediction_status": (
                "oos_prediction_available"
            ),
            "lightgbm_regression_model_artifact_id": "rank-model",
        }
        for gate in (
            "market_us",
            "market_cap",
            "beneish",
            "piotroski",
            "roa_positive",
            "altman",
            "value",
            "momentum",
        ):
            row[f"gate_{gate}_status"] = "supported"
            row[f"gate_{gate}_pass"] = True
            row[f"gate_{gate}_value"] = 1.0
            row[f"gate_{gate}_provenance"] = "dataset"
        rows.append(row)
    return pd.DataFrame(rows)


def test_portfolio_ranks_after_liquidity_and_keeps_equal_weight_top_15():
    candidates = _candidate_frame(16)
    liquidity = pd.DataFrame(
        {
            "stable_row_id": candidates["stable_row_id"],
            "liquidity_pass": [False] + [True] * 15,
            "median_30_session_dollar_volume": [1.0]
            + [MIN_ADTV] * 15,
            "exclusion_code": ["below"] + [None] * 15,
            "entry_timestamp": pd.Timestamp(
                "2026-07-02 20:00:00", tz="UTC"
            ),
            "entry_session_date": "2026-07-02",
            "entry_price_observed": True,
            "raw_response_sha256": "raw",
        }
    )
    frame, gates, _, holdings, periods, validation = materialize_portfolios(
        candidates, liquidity
    )
    assert frame.loc[
        frame["stable_row_id"].eq("row-00"), "rank"
    ].isna().all()
    assert len(gates) == len(candidates) * 12
    assert len(holdings) == TARGET_N
    assert np.allclose(holdings["weight"], WEIGHT)
    assert np.isclose(holdings["weight"].sum(), 1.0)
    assert bool(periods.iloc[0]["period_supported"])
    assert validation["ranking_after_all_gates"]


def test_incomplete_period_forms_no_portfolio():
    candidates = _candidate_frame(14)
    liquidity = pd.DataFrame(
        {
            "stable_row_id": candidates["stable_row_id"],
            "liquidity_pass": True,
            "median_30_session_dollar_volume": MIN_ADTV,
            "exclusion_code": None,
            "entry_timestamp": pd.Timestamp(
                "2026-07-02 20:00:00", tz="UTC"
            ),
            "entry_session_date": "2026-07-02",
            "entry_price_observed": True,
            "raw_response_sha256": "raw",
        }
    )
    # The product route intentionally fails if the latest decision is
    # incomplete. Add a supported later period to isolate the earlier closure.
    later = _candidate_frame(15, decision="2027-07-02")
    later_liquidity = liquidity.iloc[:0].copy()
    later_liquidity = pd.DataFrame(
        {
            "stable_row_id": later["stable_row_id"] + "-later",
            "liquidity_pass": True,
            "median_30_session_dollar_volume": MIN_ADTV,
            "exclusion_code": None,
            "entry_timestamp": pd.Timestamp(
                "2027-07-02 20:00:00", tz="UTC"
            ),
            "entry_session_date": "2027-07-02",
            "entry_price_observed": True,
            "raw_response_sha256": "raw",
        }
    )
    later["stable_row_id"] = later["stable_row_id"] + "-later"
    combined = pd.concat([candidates, later], ignore_index=True)
    combined_liquidity = pd.concat(
        [liquidity, later_liquidity], ignore_index=True
    )
    _, _, _, holdings, periods, _ = materialize_portfolios(
        combined, combined_liquidity
    )
    first = periods.loc[
        periods["decision_timestamp"].eq(
            pd.Timestamp("2026-07-02", tz="UTC")
        )
    ].iloc[0]
    assert not bool(first["period_supported"])
    assert holdings.loc[
        holdings["decision_timestamp"].eq(
            pd.Timestamp("2026-07-02", tz="UTC")
        )
    ].empty


def test_backtest_route_freezes_costs_and_fails_performance_closed():
    holdings = _candidate_frame(15)
    holdings["weight"] = WEIGHT
    holdings["entry_timestamp"] = pd.Timestamp(
        "2026-07-02 20:00:00", tz="UTC"
    )
    periods = pd.DataFrame(
        {
            "period_supported": [True],
            "decision_timestamp": [
                pd.Timestamp("2026-07-02", tz="UTC")
            ],
        }
    )
    closes = pd.date_range(
        "2026-07-01 20:00:00+00:00", periods=10, freq="365D"
    )
    calendar = pd.DataFrame(
        {
            "market_close": closes,
            "session_date": closes.strftime("%Y-%m-%d"),
        }
    )
    plan, status = build_backtest_status(holdings, periods, calendar)
    assert len(plan) == TARGET_N
    assert status["status"] == "unavailable_fail_closed"
    assert not status["official_performance_available"]
    assert not status["performance_calculated"]
    assert status["transaction_cost_policy"]["rate_per_side"] == 0.0025
    assert TRANSACTION_COST_PER_SIDE == 0.0025
    assert {item["code"] for item in status["blockers"]} == {
        item["code"] for item in BACKTEST_BLOCKERS
    }
    assert not status["old_v3_performance_transferred"]
    assert not status["future_performance_claimed"]


def test_backtest_blocker_payload_is_json_serializable():
    json.dumps(BACKTEST_BLOCKERS)
