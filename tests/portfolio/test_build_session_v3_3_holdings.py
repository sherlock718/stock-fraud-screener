import json

import numpy as np
import pandas as pd

from portfolio.build_session_v3_3_holdings import (
    EX_LIQUIDITY_INCOMPLETE,
    MIN_ADTV,
    TARGET_N,
    WEIGHT,
    build_requests,
    load_candidates,
    materialize_selection,
    validate_inputs,
)


def test_frozen_liquidity_equation_is_exact():
    assert TARGET_N == 15
    assert WEIGHT == 1 / 15
    assert MIN_ADTV == (200_000 / 15) / 0.01
    assert MIN_ADTV == 1_333_333.3333333333


def test_accepted_artifacts_materialize_exact_candidate_wide_scope():
    config, preflight = validate_inputs()
    candidates = load_candidates(config)
    required = candidates.loc[candidates["liquidity_required"]]
    assert preflight["v3_1_manifest_sha256"].startswith("2b5249cd")
    assert preflight["v3_2_manifest_sha256"].startswith("ba0e3b2d")
    assert preflight["v3_1_records_revalidated"] == 12
    assert preflight["v3_2_records_revalidated"] == 144
    assert len(required) == 1428
    assert required["decision_timestamp"].dt.year.value_counts().sort_index().to_dict() == {
        2015: 36, 2016: 226, 2017: 254, 2018: 285, 2019: 319, 2020: 308,
    }
    assert required["provider_symbol"].notna().all()


def test_build_requests_uses_exact_30_pre_prediction_market_closes():
    closes = pd.date_range("2020-05-01 20:00:00+00:00", periods=40, freq="B")
    schedule = pd.DataFrame({
        "session_date": closes.normalize().tz_localize(None),
        "market_open": closes - pd.Timedelta(hours=6, minutes=30),
        "market_close": closes,
        "exchange_calendar": "XNYS",
    })
    prediction = closes[-1] - pd.Timedelta(hours=1)
    candidates = pd.DataFrame([{
        "stable_row_id": "row-1", "provider_symbol": "ABC", "provider_exchange": "NYQ",
        "exchange_calendar": "XNYS", "prediction_timestamp": prediction,
        "liquidity_required": True,
    }])
    result = build_requests(candidates, schedule)
    expected = json.loads(result.iloc[0]["expected_session_dates_json"])
    assert len(expected) == 30
    assert pd.Timestamp(result.iloc[0]["expected_last_market_close"]) < prediction
    assert expected[-1] == closes[-2].strftime("%Y-%m-%d")


def _candidate_frame(n: int) -> pd.DataFrame:
    rows = []
    for i in range(n):
        row = {
            "stable_row_id": f"row-{i:02d}", "entity_id": f"entity-{i}", "cik": str(i),
            "ticker": f"T{i}", "provider_symbol": f"T{i}", "provider_exchange": "NYQ",
            "exchange_calendar": "XNYS", "fiscal_year": 2020,
            "decision_timestamp": pd.Timestamp("2020-07-02", tz="UTC"),
            "prediction_timestamp": pd.Timestamp("2020-07-02 00:01", tz="UTC"),
            "entry_timestamp": pd.Timestamp("2020-07-02 20:00", tz="UTC"),
            "all_non_model_hard_gates_pass": True, "hard_gate_exclusion_codes": "[]",
            "tree_role_pass": True, "ranker_role_pass": True, "tree_threshold_pass": True,
            "decision_tree_prediction": 0.75, "decision_tree_prediction_status": "oos_prediction_available",
            "lightgbm_regression_prediction": float(n - i),
            "lightgbm_regression_prediction_status": "oos_prediction_available",
            "liquidity_required": True,
            "decision_tree_model_artifact_id": "sha256:tree", "lightgbm_regression_model_artifact_id": "sha256:lgbm",
            "decision_tree_feature_artifact_id": "sha256:tf", "lightgbm_regression_feature_artifact_id": "sha256:lf",
            "decision_tree_preprocessing_artifact_id": "sha256:tp", "lightgbm_regression_preprocessing_artifact_id": "sha256:lp",
        }
        for gate in ("market_us", "market_cap", "beneish", "piotroski", "roa_positive", "altman", "value", "momentum"):
            row[f"gate_{gate}_status"] = "supported"
            row[f"gate_{gate}_pass"] = True
            row[f"gate_{gate}_value"] = 1.0
            row[f"gate_{gate}_provenance"] = "frozen"
        rows.append(row)
    return pd.DataFrame(rows)


def test_selection_ranks_only_after_liquidity_and_freezes_15_equal_weights():
    candidates = _candidate_frame(16)
    liquidity = pd.DataFrame({
        "stable_row_id": candidates["stable_row_id"],
        "liquidity_pass": [False] + [True] * 15,
        "median_30_session_dollar_volume": [1.0] + [MIN_ADTV] * 15,
        "exclusion_code": [EX_LIQUIDITY_INCOMPLETE] + [None] * 15,
    })
    candidate_table, _, exclusions, holdings, periods = materialize_selection(candidates, liquidity)
    assert candidate_table.loc[candidate_table["stable_row_id"].eq("row-00"), "rank"].isna().all()
    assert len(holdings) == 15
    assert np.allclose(holdings["weight"], WEIGHT)
    assert np.isclose(holdings["weight"].sum(), 1.0)
    assert periods.iloc[0]["period_supported"]
    assert EX_LIQUIDITY_INCOMPLETE in set(exclusions["exclusion_code"])


def test_incomplete_period_forms_no_portfolio():
    candidates = _candidate_frame(14)
    liquidity = pd.DataFrame({
        "stable_row_id": candidates["stable_row_id"], "liquidity_pass": True,
        "median_30_session_dollar_volume": MIN_ADTV, "exclusion_code": None,
    })
    _, _, _, holdings, periods = materialize_selection(candidates, liquidity)
    assert holdings.empty
    assert not periods.iloc[0]["period_supported"]
    assert periods.iloc[0]["holding_count"] == 0
