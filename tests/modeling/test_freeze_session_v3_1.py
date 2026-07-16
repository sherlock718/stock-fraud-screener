import numpy as np
import pandas as pd

from modeling.freeze_session_v3_1 import (
    AUM,
    MAX_POSITION_ADTV,
    MIN_ADTV,
    TARGET_N,
    asof_sector_percentile,
    freeze_configuration,
    winsorize_prior_sec_history,
)


def _clock_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "entity_id": ["US:1", "US:2", "US:3"],
        "fiscal_year": [2020, 2020, 2021],
        "period_type": ["annual"] * 3,
        "market": ["US"] * 3,
        "sic_2digit": [35, 35, 35],
        "availability_provenance": ["sec_primary_filing"] * 3,
        # New York end-of-day crosses UTC midnight; it remains valid SEC evidence.
        "availability_timestamp": pd.to_datetime([
            "2021-03-02 04:59:59+00:00",
            "2021-03-03 04:59:59+00:00",
            "2022-03-02 04:59:59+00:00",
        ]),
    })


def test_sec_clock_materializes_cross_utc_midnight_rows():
    frame = _clock_frame()
    result, method = winsorize_prior_sec_history(frame, pd.Series([1.0, 2.0, 100.0]))
    assert result.notna().all()
    assert method.eq("raw_sparse_prior_sec_history").all()


def test_future_rows_cannot_change_past_sector_percentile():
    frame = _clock_frame()
    base = asof_sector_percentile(frame.iloc[:2], pd.Series([1.0, 2.0]), min_count=2)
    extended = asof_sector_percentile(frame, pd.Series([1.0, 2.0, 0.1]), min_count=2)
    pd.testing.assert_series_equal(base, extended.iloc[:2], check_names=False)


def test_liquidity_equation_is_position_over_one_percent_adtv():
    assert MIN_ADTV == (AUM / TARGET_N) / MAX_POSITION_ADTV
    assert MIN_ADTV == 1_333_333.3333333333


def test_configuration_freezes_no_model_or_gate_columns_as_features():
    frame = pd.DataFrame({
        "fiscal_year": np.arange(20, dtype="int64"),
        "candidate": np.arange(20, dtype="float64"),
        "forward_return_3y": np.arange(20, dtype="float64"),
        "target_3y": np.arange(20, dtype="float64"),
        "tree_target_3y": np.arange(20, dtype="float64"),
        "gate_beneish_value": np.arange(20, dtype="float64"),
    })
    config = freeze_configuration(frame)
    candidates = config["feature_contract"]["candidate_columns"]
    assert candidates == ["candidate"]
    assert config["selection"]["target_n"] == 15
    assert config["selection"]["weight_each"] == 1 / 15
    assert config["selection"]["strategy"] == "production_v3_ml_gates"
    assert config["feature_contract"]["selector"]["top_n"] == 28
    assert config["decision_tree"]["pass_rule"] == "tree_prob >= 0.55"
    assert config["lightgbm_ranker"]["family"] == "lightgbm.LGBMRegressor"
    assert "fraud_suspect" not in config["model_training_population"]["required_filter"]
    assert config["legacy_performance_transferable"] is False
