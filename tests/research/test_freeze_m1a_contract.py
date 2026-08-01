"""Focused, contract-only verification for Session M1A."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from research.freeze_m1a_contract import (
    ARTIFACT_VERSION,
    DEFAULT_ARTIFACT_ROOT,
    build,
)


ROOT = Path(__file__).resolve().parents[2]


def _load(name: str):
    return json.loads((DEFAULT_ARTIFACT_ROOT / name).read_text())


def test_frozen_artifact_and_outer_fold_boundary_are_contract_only():
    contract = _load("experiment_contract.json")
    manifest = _load("manifest.json")
    assert contract["claim"]["contract_only"] is True
    assert contract["claim"]["model_execution"] is False
    assert contract["claim"]["performance_calculated"] is False
    assert manifest["version"] == ARTIFACT_VERSION
    assert contract["evidence_summary"]["p3_outer_decision_cohorts"][0].startswith("2010-")
    assert contract["evidence_summary"]["p3_outer_decision_cohorts"][-1].startswith("2026-")
    assert contract["outer_protocol"]["outer_oos_rule"].startswith("outer decision cohort")


def test_inner_folds_are_strictly_temporal_and_maturity_safe():
    rows = _load("inner_folds.json")
    available = [row for row in rows if row["availability_status"] == "available"]
    assert len(rows) == 17 * 2 * 3 * 3
    for row in available:
        train_end = pd.Timestamp(row["max_fitted_label_end"])
        validation = pd.Timestamp(row["validation_decision_start"])
        outer = pd.Timestamp(row["outer_decision_timestamp"])
        assert train_end < validation < outer
        assert row["train_row_count"] >= 100
        assert row["validation_row_count"] >= 50
        assert row["purge_count"] >= 0
    assert all(
        row["failure_reason"]
        for row in rows
        if row["availability_status"] == "unavailable"
    )


def test_label_maturity_ledger_requires_two_valid_inner_folds():
    ledger = _load("label_maturity_ledger.json")
    assert len(ledger) == 17 * 2 * 3
    for row in ledger:
        assert row["strict_label_end_before_outer_decision"] is True
        if row["availability_status"] == "available_for_tuning":
            assert row["valid_inner_fold_count"] >= row["minimum_valid_inner_folds"] == 2
        else:
            assert row["failure_reason"] == "fewer_than_two_valid_inner_folds"


def test_feature_contract_excludes_targets_support_future_outputs_and_macro():
    contract = _load("experiment_contract.json")
    features = contract["feature_contract"]
    excluded = features["exclusions"]
    forbidden_text = json.dumps(excluded)
    for token in ("target_3y", "tree_target_3y", "label_end_date", "forward_*", "prediction", "policy_*"):
        assert token in forbidden_text
    assert len(excluded["uncertified_macro_columns"]) == 17
    assert not set(features["gate_feature_regime_allowed_raw_inputs"]).intersection(
        excluded["uncertified_macro_columns"]
    )
    assert features["candidate_count"] == 200
    assert features["gate_feature_regime_allowed_raw_inputs"]


def test_grids_are_small_seeded_and_non_random():
    roles = _load("experiment_contract.json")["model_roles"]
    lgbm = roles["lightgbm_regression"]
    tree = roles["decision_tree"]
    assert lgbm["grid_size"] == 8
    assert tree["grid_size"] == 4
    assert lgbm["grid"]["random_state"] == [42]
    assert lgbm["grid"]["n_jobs"] == [1]
    assert lgbm["grid"]["deterministic"] == [True]
    assert tree["grid"]["random_state"] == [42]
    assert "random split" not in json.dumps(_load("experiment_contract.json")["inner_protocol"]).lower()


def test_b1e_performance_is_hash_boundary_only_and_rate_namespaces_stay_separate():
    contract = _load("experiment_contract.json")
    prohibited = contract["prohibited_inputs"]
    assert "outputs/metrics.parquet" in json.dumps(prohibited)
    assert "B1E may be hash-verified" in prohibited["meaning"]
    sequence = contract["execution_sequence"]
    assert "zero_risk_free_sharpe_diagnostic" in sequence["rate_namespace"]
    assert "dgs1mo_alfred_2026_07_17" in sequence["rate_namespace"]
    assert "CAGR" in contract["selection_rule"]["prohibited_selection_inputs"]
    assert "Sharpe" in contract["selection_rule"]["prohibited_selection_inputs"]


def test_contract_artifact_rerun_is_byte_deterministic(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    build(first)
    build(second)
    first_files = sorted(path.relative_to(first) for path in first.rglob("*") if path.is_file())
    second_files = sorted(path.relative_to(second) for path in second.rglob("*") if path.is_file())
    assert first_files == second_files
    for relative in first_files:
        assert (first / relative).read_bytes() == (second / relative).read_bytes()
