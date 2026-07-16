import json

import numpy as np
import pandas as pd

import modeling.build_session_v3_2_oos as v3_2


def _config() -> dict:
    return {
        "strategy_name": "production_v3_ml_gates",
        "decision_calendar": {
            "training_label_rule": "label_end_date strictly before fold decision_timestamp",
        },
        "targets": {
            "decision_tree": "tree_target_3y",
            "lightgbm_regression": "target_3y",
            "regression_clip": [-1.0, 5.0],
        },
        "feature_contract": {
            "candidate_columns": ["feature_a", "feature_b"],
            "selector": {
                "top_n": 28,
                "min_abs_ic": 0.0,
                "min_ic_years": 1,
                "min_group_size": 2,
                "corr_threshold": 0.85,
            },
        },
        "decision_tree": {
            "family": "sklearn.tree.DecisionTreeClassifier",
            "max_depth": 4,
            "min_samples_leaf": 1,
            "min_samples_split": 2,
            "random_state": 42,
        },
        "lightgbm_ranker": {
            "family": "lightgbm.LGBMRegressor",
            "parameters": {
                "n_estimators": 5,
                "max_depth": 2,
                "learning_rate": 0.1,
                "num_leaves": 3,
                "subsample": 0.8,
                "colsample_bytree": 0.7,
                "min_child_samples": 2,
                "reg_alpha": 0.1,
                "reg_lambda": 1.0,
                "random_state": 42,
                "n_jobs": 1,
                "verbose": -1,
            },
        },
        "model_training_population": {
            "required_filter": "piotroski_roa_pos == 1 AND beneish_m_score < -1.78",
        },
    }


def _table() -> pd.DataFrame:
    rows = 8
    return pd.DataFrame({
        "stable_row_id": [f"row-{i}" for i in range(rows)],
        "entity_id": [f"US:{i}" for i in range(rows)],
        "cik": np.arange(rows),
        "ticker": [f"T{i}" for i in range(rows)],
        "market": "US",
        "fiscal_year": [2018] * 4 + [2019] * 4,
        "period_type": "annual",
        "availability_timestamp": pd.to_datetime(["2019-03-01"] * rows, utc=True),
        "availability_provenance": "sec_primary_filing",
        "decision_timestamp": pd.to_datetime(["2023-07-02"] * 4 + ["2027-07-02"] * 4, utc=True),
        "prediction_timestamp": pd.to_datetime(["2023-07-02 00:01"] * 4 + ["2027-07-02 00:01"] * 4, utc=True),
        "entry_timestamp": pd.to_datetime(["2023-07-03"] * 4 + ["2027-07-03"] * 4, utc=True),
        "label_end_date": pd.to_datetime(["2022-07-01"] * rows, utc=True),
        "target_status_3y": "supported",
        "target_provenance_3y": "observed",
        "target_3y": np.arange(rows, dtype=float),
        "tree_target_3y": [False, True] * 4,
        "fold_id": ["decision_20230702T000000Z"] * 4 + ["decision_20270702T000000Z"] * 4,
        "all_non_model_hard_gates_pass": True,
        "hard_gate_exclusion_codes": "[]",
        "piotroski_roa_pos": 1,
        "beneish_m_score": -2.0,
        "feature_a": np.arange(rows, dtype=float),
        "feature_b": np.arange(rows, dtype=float)[::-1],
    })


def test_training_eligibility_is_strict_and_clean():
    table = _table()
    table.loc[0, "label_end_date"] = pd.Timestamp("2023-07-02", tz="UTC")
    table.loc[1, "piotroski_roa_pos"] = 0
    table.loc[2, "beneish_m_score"] = -1.78
    train = v3_2.eligible_training_rows(
        table.iloc[:4], pd.Timestamp("2023-07-02", tz="UTC"), "target_3y"
    )
    assert set(train["stable_row_id"]) == {"row-3"}
    assert train["label_end_date"].lt(pd.Timestamp("2023-07-02", tz="UTC")).all()


def test_role_selection_is_separate_and_bounded_to_frozen_candidates(monkeypatch):
    seen_targets = []

    def fake_selector(frame, target, selector):
        seen_targets.append(target)
        assert list(frame.columns) == ["fiscal_year", "feature_a", "feature_b", target]
        assert selector.top_n == 28
        return ["feature_a"]

    monkeypatch.setattr(v3_2, "select_fold_features", fake_selector)
    train = _table().iloc[:4]
    tree_features, _, _ = v3_2._select_features(
        train, "decision_tree", _config(), v3_2.selector_config(_config())
    )
    reg_features, _, _ = v3_2._select_features(
        train, "lightgbm_regression", _config(), v3_2.selector_config(_config())
    )
    assert tree_features == reg_features == ["feature_a"]
    assert seen_targets == [
        "_v3_2_beat_local_market_target",
        "_v3_2_forward_return_target",
    ]


def test_fold_fail_closed_codes_cover_required_conditions(tmp_path, monkeypatch):
    config = _config()
    score = _table().iloc[:2]
    prediction, _, code = v3_2.fit_fold_role(
        score.iloc[:0], score, "decision_tree", "fold", config, tmp_path
    )
    assert prediction is None and code == v3_2.EXCLUSION_NO_HISTORY

    one_class = _table().iloc[:4].copy()
    one_class["tree_target_3y"] = True
    monkeypatch.setattr(v3_2, "select_fold_features", lambda *args: ["feature_a"])
    prediction, _, code = v3_2.fit_fold_role(
        one_class, score, "decision_tree", "fold", config, tmp_path
    )
    assert prediction is None and code == v3_2.EXCLUSION_TREE_CLASSES

    monkeypatch.setattr(v3_2, "select_fold_features", lambda *args: [])
    prediction, _, code = v3_2.fit_fold_role(
        _table().iloc[:4], score, "lightgbm_regression", "fold", config, tmp_path
    )
    assert prediction is None and code == v3_2.EXCLUSION_NO_FEATURES

    monkeypatch.setattr(v3_2, "select_fold_features", lambda *args: ["feature_a"])
    invalid = _table().iloc[:4].copy()
    invalid["feature_a"] = np.nan
    prediction, _, code = v3_2.fit_fold_role(
        invalid, score, "lightgbm_regression", "fold", config, tmp_path
    )
    assert prediction is None and code == v3_2.EXCLUSION_INVALID_MEDIANS

    class InvalidTree:
        classes_ = np.array([0, 1])

        def __init__(self, **params):
            pass

        def fit(self, x, y):
            return self

        def predict_proba(self, x):
            return np.full((len(x), 2), np.nan)

    monkeypatch.setattr(v3_2, "DecisionTreeClassifier", InvalidTree)
    prediction, _, code = v3_2.fit_fold_role(
        _table().iloc[:4], score, "decision_tree", "invalid-output", config, tmp_path
    )
    assert prediction is None and code == v3_2.EXCLUSION_INVALID_OUTPUT


def test_regression_clips_training_target_and_persists_lineage(tmp_path, monkeypatch):
    config = _config()
    train = _table().iloc[:4].copy()
    train["target_3y"] = [-10.0, 0.0, 2.0, 20.0]
    score = _table().iloc[4:6].copy()
    monkeypatch.setattr(v3_2, "select_fold_features", lambda *args: ["feature_a"])
    captured = {}

    class FakeModel:
        def __init__(self, **params):
            captured["params"] = params

        def fit(self, x, y):
            captured["target"] = list(y)
            return self

        def predict(self, x):
            return np.full(len(x), 0.25)

    monkeypatch.setattr(v3_2.lgb, "LGBMRegressor", FakeModel)
    monkeypatch.setattr(
        v3_2.joblib,
        "dump",
        lambda model, path, compress: path.write_bytes(b"test-model"),
    )
    predictions, lineage, code = v3_2.fit_fold_role(
        train, score, "lightgbm_regression", "fold", config, tmp_path
    )
    assert code is None
    assert list(predictions) == [0.25, 0.25]
    assert captured["target"] == [-1.0, 0.0, 2.0, 5.0]
    assert captured["params"] == config["lightgbm_ranker"]["parameters"]
    assert lineage["selected_feature_count"] == 1
    assert lineage["model_configuration_artifact_id"].startswith("sha256:")
    target_payload = json.loads((tmp_path / "models/fold/lightgbm_regression/target.json").read_text())
    assert target_payload["regression_fit_clip"] == [-1.0, 5.0]


def test_label_derived_candidate_is_masked_until_label_end_date():
    score = pd.DataFrame({
        "observed_excess_return_3y": [100.0, 200.0, 300.0],
        "label_end_date": pd.to_datetime(
            ["2022-07-01", "2023-07-02", None], utc=True
        ),
    })
    masked, counts = v3_2.mask_unavailable_score_features(
        score,
        ["observed_excess_return_3y"],
        pd.Timestamp("2023-07-02", tz="UTC"),
    )
    assert masked["observed_excess_return_3y"].iloc[0] == 100.0
    assert masked["observed_excess_return_3y"].iloc[1:].isna().all()
    assert counts == {"observed_excess_return_3y": 2}


def test_future_rows_are_retained_with_exact_exclusion_code(tmp_path, monkeypatch):
    table = _table()

    def fake_fit(train, score, role, fold_id, config, output_root):
        return np.full(len(score), 0.5), {
            "feature_artifact_id": "sha256:f",
            "preprocessing_artifact_id": "sha256:p",
            "target_artifact_id": "sha256:t",
            "model_configuration_artifact_id": "sha256:c",
            "model_artifact_id": "sha256:m",
            "training_rows": 1,
            "training_label_end_max": "2022-01-01T00:00:00+00:00",
            "selected_feature_count": 1,
            "selected_features_json": '["feature_a"]',
            "training_population_fingerprint": "fingerprint",
        }, None

    monkeypatch.setattr(v3_2, "fit_fold_role", fake_fit)
    predictions, folds = v3_2.build_predictions(
        table, _config(), pd.Timestamp("2026-07-16", tz="UTC"), tmp_path
    )
    future = predictions["decision_timestamp"].dt.year.eq(2027)
    assert future.sum() == 8
    assert predictions.loc[future, "exclusion_code"].eq(v3_2.EXCLUSION_FUTURE).all()
    assert predictions.loc[future, "prediction"].isna().all()
    assert predictions.loc[~future, "prediction_status"].eq("oos_prediction_available").all()
    assert len(folds) == 2
