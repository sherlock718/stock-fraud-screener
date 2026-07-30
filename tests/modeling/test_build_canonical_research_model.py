import json

import numpy as np
import pandas as pd
import pytest

import modeling.build_canonical_research_model as p3


def _config() -> dict:
    return {
        "strategy_name": "production_v3_ml_gates",
        "targets": {
            "decision_tree": "tree_target_3y",
            "lightgbm_regression": "target_3y",
        },
        "feature_contract": {
            "candidate_columns": ["feature_a"],
            "selector": {
                "top_n": 28,
                "min_abs_ic": 0.02,
                "min_ic_years": 1,
                "min_group_size": 30,
                "corr_threshold": 0.85,
            },
        },
    }


def _table() -> pd.DataFrame:
    rows = 8
    decision = pd.to_datetime(
        ["2023-07-02"] * 4 + ["2027-07-02"] * 4, utc=True
    )
    return pd.DataFrame(
        {
            "stable_row_id": [f"row-{index}" for index in range(rows)],
            "entity_id": [f"US:{index}" for index in range(rows)],
            "cik": np.arange(rows),
            "ticker": [f"T{index}" for index in range(rows)],
            "market": "US",
            "fiscal_year": [2018] * 4 + [2019] * 4,
            "period_type": "annual",
            "availability_timestamp": pd.to_datetime(
                ["2019-03-01"] * rows, utc=True
            ),
            "availability_provenance": "sec_primary_filing",
            "event_time_materialization_timestamp": pd.to_datetime(
                ["2019-03-01"] * rows, utc=True
            ),
            "source_feature_available_at_decision": True,
            "decision_timestamp": decision,
            "prediction_timestamp": decision + pd.Timedelta(minutes=1),
            "entry_timestamp": decision + pd.Timedelta(days=1),
            "label_end_date": pd.to_datetime(
                ["2022-07-01"] * rows, utc=True
            ),
            "target_status_3y": "supported",
            "target_provenance_3y": "observed",
            "target_3y": np.arange(rows, dtype=float),
            "tree_target_3y": [False, True] * 4,
            "fold_id": [
                "decision_20230702T000000Z"
            ] * 4 + ["decision_20270702T000000Z"] * 4,
            "all_non_model_hard_gates_pass": True,
            "hard_gate_exclusion_codes": "[]",
            "piotroski_roa_pos": 1,
            "beneish_m_score": -2.0,
            "feature_a": np.arange(rows, dtype=float),
        }
    )


def _source_ids() -> dict[str, str]:
    return {
        "source_manifest_sha256": "manifest",
        "source_dataset_artifact_id": "sha256:dataset",
        "source_labels_artifact_id": "sha256:labels",
        "source_row_support_artifact_id": "sha256:support",
    }


def test_pinned_canonical_manifest_and_consumed_records_pass():
    manifest, preflight = p3.validate_canonical_manifest()
    assert manifest["primary_dataset"] == (
        "outputs/observed_only/features_taxonomy.parquet"
    )
    assert preflight["canonical_p2_manifest_sha256"] == (
        p3.CANONICAL_P2_MANIFEST_SHA256
    )
    assert preflight["records_validated_count"] == 4
    assert preflight["policy_only_additions"] == 0


def test_manifest_hash_mismatch_fails_before_loading(monkeypatch):
    monkeypatch.setattr(p3, "CANONICAL_P2_MANIFEST_SHA256", "0" * 64)
    with pytest.raises(RuntimeError, match="manifest hash mismatch"):
        p3.validate_canonical_manifest()


def test_actual_source_contract_reconfirms_target_and_availability_boundaries():
    features, labels, support, summary, _, _ = p3.load_canonical_inputs()
    result = p3.validate_source_contract(features, labels, support, summary)
    assert result == {
        "rows": 43_806,
        "stable_row_ids": 43_806,
        "observed_3y_targets": 19_025,
        "target_status_counts": {
            "supported": 19_025,
            "unavailable": 19_064,
            "excluded": 5_717,
        },
        "decision_folds": 19,
        "source_features_available_at_decision": 43_403,
        "source_features_unavailable_at_decision": 403,
        "certified_macro_supported_rows": 0,
        "policy_imputed_3y_rows": 0,
    }


def test_candidate_freeze_excludes_targets_support_and_uncertified_macro():
    frame = pd.DataFrame(
        {
            "feature": pd.Series([1.0, np.nan], dtype="float64"),
            "forward_return_3y": [1.0, 2.0],
            "observed_excess_return_3y": [1.0, 2.0],
            "label_status_numeric": [1, 2],
            "policy_imputed_3y": [0, 1],
            "fed_funds_rate": [1.0, 2.0],
            "flag": [True, False],
        }
    )
    assert p3.leakage_safe_feature_candidates(frame) == ["feature"]


def test_training_rows_require_strict_label_and_own_feature_availability():
    table = _table().iloc[:4].copy()
    table.loc[0, "label_end_date"] = pd.Timestamp(
        "2023-07-02", tz="UTC"
    )
    table.loc[1, "source_feature_available_at_decision"] = False
    table.loc[2, "beneish_m_score"] = -1.78
    train = p3.eligible_training_rows(
        table,
        pd.Timestamp("2023-07-02", tz="UTC"),
        "target_3y",
    )
    assert list(train["stable_row_id"]) == ["row-3"]


def test_predictions_retain_late_and_future_rows_with_exact_codes(
    tmp_path, monkeypatch
):
    table = _table()
    table.loc[0, "source_feature_available_at_decision"] = False

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
            "selected_features_json": json.dumps(["feature_a"]),
            "training_population_fingerprint": "fingerprint",
        }, None

    monkeypatch.setattr(p3, "fit_fold_role", fake_fit)
    predictions, folds = p3.build_oos_predictions(
        table,
        _config(),
        pd.Timestamp("2026-07-29", tz="UTC"),
        tmp_path,
        _source_ids(),
    )
    assert len(predictions) == len(table) * len(p3.MODEL_ROLES)
    assert not predictions.duplicated(["stable_row_id", "model_role"]).any()
    late = predictions["stable_row_id"].eq("row-0")
    future = predictions["decision_timestamp"].dt.year.eq(2027)
    assert predictions.loc[
        late, "exclusion_code"
    ].eq(p3.EXCLUSION_SOURCE_UNAVAILABLE).all()
    assert predictions.loc[future, "exclusion_code"].eq(
        p3.EXCLUSION_FUTURE
    ).all()
    available = ~(late | future)
    assert predictions.loc[available, "prediction_status"].eq(
        "oos_prediction_available"
    ).all()
    assert predictions[p3.SOURCE_LINEAGE_COLUMNS].notna().all().all()
    assert len(folds) == len(p3.MODEL_ROLES)
