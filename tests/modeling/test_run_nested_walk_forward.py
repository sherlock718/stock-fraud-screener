"""Mechanical and fail-closed tests for the Session M1C runner."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from modeling.nested_walk_forward import NestedWalkForwardContractError
from modeling.run_nested_walk_forward import (
    _excluded_predictions,
    _partial_attempt_inventory,
    _prediction_shell,
    _target_status,
    load_frozen_m1a_contract,
    verify_artifact,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stable_row_id": ["mature", "missing", "open"],
            "entity_id": ["1", "2", "3"],
            "ticker": ["A", "B", "C"],
            "fiscal_year": [2020, 2020, 2024],
            "decision_timestamp": pd.to_datetime(
                ["2020-07-02", "2020-07-02", "2024-07-02"], utc=True
            ),
            "label_end_date": pd.to_datetime(
                ["2023-07-01", None, None], utc=True
            ),
            "target_3y": [0.5, np.nan, np.nan],
            "tree_target_3y": [1.0, np.nan, np.nan],
        }
    )


def test_target_status_separates_mature_historical_missing_and_open_rows():
    status = _target_status(_frame(), "target_3y").tolist()
    assert status == [
        "matured_observed",
        "historical_label_unavailable_excluded_from_metrics",
        "open_2024_2026_unlabeled_excluded_from_metrics",
    ]


def test_future_exclusion_shell_is_explicit_and_target_preserving():
    frozen = load_frozen_m1a_contract()
    frame = _frame().iloc[[0]].copy()
    output = _excluded_predictions(
        frame,
        "lightgbm_regression",
        frozen,
        "outside_frozen_2010_2026_outer_boundary",
        "future_decision_after_frozen_scoring_cutoff",
    )
    assert output.loc[0, "target"] == 0.5
    assert output.loc[0, "prediction_status"] == "excluded"
    assert output.loc[0, "exclusion_code"] == (
        "future_decision_after_frozen_scoring_cutoff"
    )
    assert pd.isna(output.loc[0, "prediction"])


def test_prediction_shell_normalizes_role_targets_for_row_complete_parquet(
    tmp_path,
):
    frozen = load_frozen_m1a_contract()
    frame = _frame()
    regression = _prediction_shell(frame, "lightgbm_regression", frozen)
    tree = _prediction_shell(frame, "decision_tree", frozen)
    combined = pd.concat([regression, tree], ignore_index=True)
    assert combined["target"].dtype == np.dtype("float64")
    assert str(combined["selected_feature_count"].dtype) == "Int64"
    path = tmp_path / "row_complete.parquet"
    combined.to_parquet(path, index=False)
    restored = pd.read_parquet(path)
    assert len(restored) == 6
    assert restored["target"].dtype == np.dtype("float64")


def test_partial_attempt_inventory_preserves_and_classifies_files(tmp_path):
    parent = tmp_path / "nested_walk_forward"
    preflight_only = parent / "20260801T020100Z-m1c"
    partial_models = parent / "20260801T020300Z-m1c"
    current = parent / "20260801T070000Z-m1c"
    preflight_only.mkdir(parents=True)
    partial_models.mkdir(parents=True)
    (preflight_only / "preflight.json").write_text(
        json.dumps({"result": "pass"})
    )
    model = partial_models / "models" / "fold" / "role" / "model.joblib"
    model.parent.mkdir(parents=True)
    model.write_bytes(b"preserved-model")
    records = _partial_attempt_inventory(current)
    by_name = {record["artifact_directory"]: record for record in records}
    assert by_name[preflight_only.name]["status"] == (
        "failed_execution_after_passed_preflight_before_model_output"
    )
    assert by_name[partial_models.name]["status"] == (
        "failed_incomplete_execution_after_passed_preflight"
    )
    assert by_name[partial_models.name]["model_file_count"] == 1
    assert model.read_bytes() == b"preserved-model"


def test_verifier_fails_closed_without_manifest(tmp_path):
    with pytest.raises(NestedWalkForwardContractError, match="manifest is missing"):
        verify_artifact(tmp_path)
