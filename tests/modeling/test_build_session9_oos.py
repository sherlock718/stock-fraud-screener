from __future__ import annotations

import numpy as np
import pandas as pd

import modeling.build_session9_oos as session9


def test_complete_session8f_manifest_passes() -> None:
    result = session9.validate_session8f_manifest()
    assert result == {
        "result": "pass",
        "manifest_sha256": session9.SESSION8F_MANIFEST_SHA256,
        "validated_inputs": 2,
        "records": 33,
        "code_lineage": 7,
        "dirty_state_references": 3,
        "dirty_state_hashed": 3,
    }


def test_assemble_intersects_label_and_price_support() -> None:
    features = pd.DataFrame({
        "entity_id": ["US:1", "US:2", "US:3"],
        "fiscal_year": [2020, 2020, 2020],
        "stable_row_id": ["a", "b", "c"],
        "population": ["observed_only"] * 3,
        "price_feature_status": ["supported", "unavailable", "supported"],
        "price_feature_reason": ["supported", "missing_price", "supported"],
        "x": [1.0, 2.0, 3.0],
    })
    gates = pd.DataFrame({
        "entity_id": ["US:1", "US:2", "US:3"],
        "fiscal_year": [2020] * 3,
        "horizon": ["1y"] * 3,
        "decision_timestamp": pd.to_datetime(["2021-07-02"] * 3, utc=True),
        "prediction_timestamp": pd.to_datetime(["2021-07-02 00:01"] * 3, utc=True),
        "classification": ["supported", "supported", "unavailable"],
        "reason": ["supported", "supported", "missing_label"],
        "population": ["observed_only"] * 3,
    })
    labels = pd.DataFrame({
        "entity_id": ["US:1", "US:2"], "fiscal_year": [2020, 2020],
        "horizon": ["1y", "1y"],
        "decision_timestamp": pd.to_datetime(["2021-07-02"] * 2, utc=True),
        "label_end_date": pd.to_datetime(["2022-07-05"] * 2, utc=True),
        "relative_return": [0.1, -0.1], "outperformed_benchmark": [True, False],
        "label_provenance": ["observed", "observed"], "policy_imputed": [False, False],
    })
    result = session9._assemble_population("observed_only", features, gates, labels)
    assert result["base_eligible"].tolist() == [True, False, False]
    assert result.loc[1, "base_exclusion_reason"].startswith("session8f_price_unavailable")
    assert result.loc[2, "base_exclusion_reason"].startswith("session8e_unavailable")


def test_fold_fit_freezes_separate_feature_preprocessing_calibration_and_model(
    tmp_path, monkeypatch
) -> None:
    rng = np.random.default_rng(42)
    rows = 180
    train = pd.DataFrame({
        "fiscal_year": np.repeat([2018, 2019, 2020], 60),
        "decision_timestamp": pd.to_datetime(
            np.repeat(["2019-07-02", "2020-07-02", "2021-07-02"], 60), utc=True
        ),
        "label_end_date": pd.to_datetime(
            np.repeat(["2020-07-01", "2021-07-01", "2022-07-01"], 60), utc=True
        ),
        "x": rng.normal(size=rows),
    })
    train["relative_return"] = train["x"] + rng.normal(scale=0.2, size=rows)
    train["outperformed_benchmark"] = train["relative_return"].gt(0)
    score = pd.DataFrame({"x": [-1.0, 1.0]})
    def select_without_label_candidates(frame, target, config):
        assert "relative_return" not in frame
        assert "outperformed_benchmark" not in frame
        assert target.startswith("_session9_beat_local_market_")
        return ["x"]

    monkeypatch.setattr(session9, "select_fold_features", select_without_label_candidates)
    prediction, ids, reason = session9._fit_model(
        train, score, "observed_only", "1y", "decision_20230702T000000Z",
        "classification", tmp_path,
    )
    assert reason is None
    assert len(prediction) == 2
    assert prediction[0] < prediction[1]
    assert all(ids[name].startswith("sha256:") for name in (
        "feature_artifact_id", "preprocessing_artifact_id", "model_artifact_id",
        "calibration_artifact_id",
    ))
    artifact_dir = tmp_path / "models/observed_only/1y/decision_20230702T000000Z/classification"
    assert {path.name for path in artifact_dir.iterdir()} == {
        "features.json", "preprocessing.joblib", "model.joblib", "calibration.json"
    }


def test_macro_names_are_explicitly_excluded() -> None:
    assert {"yield_curve", "macro_regime", "value_in_recession"} <= session9.MACRO_FEATURES
