"""Synthetic fail-closed tests for the frozen Session M1B interfaces."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from modeling.fold_lineage import dataframe_fingerprint
from modeling.nested_walk_forward import (
    CandidateEvaluation,
    FoldPopulation,
    InnerFoldSpec,
    M1A_MANIFEST_SHA256,
    NestedWalkForwardContractError,
    TuningRunContext,
    assert_predictive_selection_inputs,
    candidate_columns_for_regime,
    evaluate_tuning_candidate,
    fit_fold_preprocessor,
    frozen_inner_fold_specs,
    frozen_grid_points,
    load_frozen_m1a_contract,
    materialize_inner_fold,
    select_features_fold_local,
    select_inner_winner,
    tuning_candidates,
    validate_candidate_columns,
    validate_fold_collection,
    validate_fold_population,
    validate_tuning_run_context,
)


@pytest.fixture(scope="module")
def frozen():
    return load_frozen_m1a_contract()


@pytest.fixture(scope="module")
def synthetic_table(frozen) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    rows_per_cohort = 110
    decisions = [
        pd.Timestamp(f"{year}-07-02", tz="UTC")
        for year in (*range(2010, 2017), 2020)
    ]
    records = []
    non_gate_candidates = list(
        candidate_columns_for_regime(frozen, "broad_downstream_gates")
    )
    signal_features = non_gate_candidates[:10]
    for decision in decisions:
        latent = rng.normal(size=(rows_per_cohort, len(signal_features)))
        regression_target = latent.sum(axis=1) + rng.normal(
            scale=0.15, size=rows_per_cohort
        )
        tree_target = (regression_target > np.median(regression_target)).astype(int)
        for offset in range(rows_per_cohort):
            row = {
                "stable_row_id": f"{decision.year}-{offset:04d}",
                "fiscal_year": decision.year,
                "decision_timestamp": decision,
                "label_end_date": decision + pd.DateOffset(years=3),
                "source_feature_available_at_decision": True,
                "target_3y": float(regression_target[offset]),
                "tree_target_3y": int(tree_target[offset]),
            }
            for index, feature in enumerate(frozen.candidate_columns):
                if feature in signal_features:
                    signal_index = signal_features.index(feature)
                    row[feature] = float(latent[offset, signal_index])
                else:
                    row[feature] = float(rng.normal())
            row["piotroski_roa_pos"] = 1.0
            row["beneish_m_score"] = -2.0
            records.append(row)
    frame = pd.DataFrame(records)
    # One fold-local missing value proves imputation is fitted from training.
    frame.loc[0, signal_features[0]] = np.nan
    return frame


def _spec(role: str, regime: str, validation_year: int, slot: int) -> InnerFoldSpec:
    return InnerFoldSpec(
        outer_fold="decision_20200702T000000Z",
        inner_fold=f"inner_{slot}",
        target_role=role,
        regime=regime,
        outer_decision_timestamp=pd.Timestamp("2020-07-02", tz="UTC"),
        validation_decision_timestamp=pd.Timestamp(
            f"{validation_year}-07-02", tz="UTC"
        ),
    )


def _populations(synthetic_table, frozen, role="decision_tree", regime="broad_downstream_gates"):
    return [
        materialize_inner_fold(
            synthetic_table,
            _spec(role, regime, year, slot),
            frozen,
        )
        for slot, year in enumerate((2014, 2015, 2016), start=1)
    ]


def test_frozen_contract_hash_grids_regimes_and_p3_candidates(frozen):
    assert frozen.manifest["version"] == "20260801T000000Z-m1a"
    assert M1A_MANIFEST_SHA256 == (
        "a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc"
    )
    assert len(frozen.outer_folds) == 34
    assert len(frozen.inner_folds) == 306
    assert len(frozen.label_maturity_ledger) == 102
    assert len(frozen.candidate_columns) == 200
    assert len(frozen_grid_points(frozen, "lightgbm_regression")) == 8
    assert len(frozen_grid_points(frozen, "decision_tree")) == 4
    assert len(tuning_candidates(frozen, "lightgbm_regression")) == 48
    assert len(tuning_candidates(frozen, "decision_tree")) == 24
    assert all(
        point["random_state"] == 42
        for role in ("lightgbm_regression", "decision_tree")
        for point in frozen_grid_points(frozen, role)
    )
    gate_features = set(
        frozen.contract["feature_contract"][
            "gate_feature_regime_allowed_raw_inputs"
        ]
    )
    assert gate_features.isdisjoint(
        candidate_columns_for_regime(frozen, "broad_downstream_gates")
    )
    assert gate_features.isdisjoint(
        candidate_columns_for_regime(frozen, "gate_eligible_training")
    )
    assert gate_features.issubset(
        candidate_columns_for_regime(frozen, "broad_gate_features")
    )
    specs = frozen_inner_fold_specs(
        frozen,
        "decision_20260702T000000Z",
        "lightgbm_regression",
        "broad_downstream_gates",
    )
    assert len(specs) == 3
    assert all(spec.contract_record_sha256 for spec in specs)
    assert [spec.inner_fold for spec in specs] == ["inner_1", "inner_2", "inner_3"]


def test_expanding_inner_populations_are_purged_and_wholly_inside_outer_training(
    synthetic_table, frozen
):
    populations = _populations(synthetic_table, frozen)
    validate_fold_collection(populations, frozen)
    assert [len(population.train) for population in populations] == [110, 220, 330]
    assert [len(population.validation) for population in populations] == [110] * 3
    assert all(population.purged_stable_row_ids for population in populations)
    for population in populations:
        assert population.train["label_end_date"].max() < (
            population.spec.validation_decision_timestamp
        )
        assert population.validation["label_end_date"].max() < (
            population.spec.outer_decision_timestamp
        )
        assert population.population_lineage["target_column"] == "tree_target_3y"


def test_label_overlap_and_outer_fold_reuse_fail_closed(synthetic_table, frozen):
    population = _populations(synthetic_table, frozen)[0]
    overlapping = population.train.copy()
    overlapping.loc[
        overlapping.index[0], "label_end_date"
    ] = population.spec.validation_decision_timestamp
    with pytest.raises(NestedWalkForwardContractError, match="label/decision overlap"):
        validate_fold_population(replace(population, train=overlapping), frozen)

    outer_reused = population.train.copy()
    outer_reused.loc[
        outer_reused.index[0], "decision_timestamp"
    ] = population.spec.outer_decision_timestamp
    with pytest.raises(NestedWalkForwardContractError, match="outer-OOS cohort"):
        validate_fold_population(replace(population, train=outer_reused), frozen)


def test_global_selection_and_transformer_imputation_leakage_fail_closed(
    synthetic_table, frozen
):
    population = _populations(synthetic_table, frozen)[1]
    selection = select_features_fold_local(
        population.train,
        population,
        frozen,
        "p3_fold_local_ic_selector",
    )
    leaked = pd.concat(
        [population.train, population.validation], ignore_index=True
    )
    with pytest.raises(NestedWalkForwardContractError, match="global or validation"):
        select_features_fold_local(
            leaked,
            population,
            frozen,
            "p3_fold_local_ic_selector",
        )
    with pytest.raises(NestedWalkForwardContractError, match="imputation fit"):
        fit_fold_preprocessor(leaked, population, selection, frozen)

    preprocessor = fit_fold_preprocessor(
        population.train, population, selection, frozen
    )
    changed_validation = population.validation.copy()
    changed_validation.loc[:, selection.selected_features[0]] = 1e12
    unchanged = fit_fold_preprocessor(
        population.train, population, selection, frozen
    )
    assert unchanged.medians == preprocessor.medians


def test_validation_row_reuse_across_inner_folds_fails_closed(
    synthetic_table, frozen
):
    populations = _populations(synthetic_table, frozen)
    second = populations[1]
    reused_validation = second.validation.copy()
    reused_validation.loc[
        reused_validation.index[0], "stable_row_id"
    ] = populations[0].validation["stable_row_id"].iloc[0]
    changed = replace(
        second,
        validation=reused_validation,
        # Public population construction uses this same order-independent
        # fingerprint; the attack therefore cannot hide behind stale lineage.
        validation_population_fingerprint=dataframe_fingerprint(
            reused_validation.reset_index(drop=True)
        ),
    )
    with pytest.raises(NestedWalkForwardContractError, match="validation row reused"):
        validate_fold_collection([populations[0], changed, populations[2]], frozen)


@pytest.mark.parametrize(
    "prohibited",
    [
        "target_3y",
        "label_end_date",
        "target_status_3y",
        "future_price_3y",
        "prediction_lightgbm",
        "model_score",
        "alpha_value",
        "policy_terminal_return",
        "fraud_score_model",
        "gate_liquidity_pass",
    ],
)
def test_prohibited_target_support_and_model_output_features_fail_closed(
    frozen, prohibited
):
    with pytest.raises(NestedWalkForwardContractError, match="prohibited feature"):
        validate_candidate_columns(
            [*frozen.candidate_columns, prohibited],
            frozen,
            require_exact=False,
        )


def test_b1e_performance_metric_consumption_fails_closed():
    for payload in (
        {"path": "artifacts/performance/free_data_v1/20260801T011135Z-b1e/outputs/metrics.parquet"},
        {"selection_metric": "aggregate net CAGR"},
        {"scenario_results": {"zero_risk_free_sharpe": 1.1}},
        {"outer_oos_labels": [1.0, 2.0]},
        {"p4_holdings": ["ABC"]},
    ):
        with pytest.raises(NestedWalkForwardContractError, match="prohibited selection"):
            assert_predictive_selection_inputs(payload)


def test_adaptive_retuning_after_failed_threshold_fails_closed():
    context = TuningRunContext(
        outer_fold="decision_20200702T000000Z",
        outer_decision_timestamp=pd.Timestamp("2020-07-02", tz="UTC"),
        tuning_attempt=2,
        trigger="failed_30pct_cagr_threshold",
        performance_threshold_observed=True,
        previous_threshold_result="failed",
    )
    with pytest.raises(NestedWalkForwardContractError, match="adaptive retuning"):
        validate_tuning_run_context(context)


def test_synthetic_tree_candidate_uses_inner_only_and_records_full_lineage(
    synthetic_table, frozen
):
    populations = _populations(synthetic_table, frozen)
    candidate = next(
        item
        for item in tuning_candidates(frozen, "decision_tree")
        if item.regime == "broad_downstream_gates"
        and item.selector_method == "p3_fold_local_ic_selector"
        and item.parameters["max_depth"] == 3
        and item.parameters["min_samples_leaf"] == 50
    )
    evaluation = evaluate_tuning_candidate(
        candidate,
        populations,
        frozen,
        execution_scope="synthetic_m1b_test",
    )
    assert evaluation.availability_status == "available_for_selection"
    assert evaluation.valid_inner_fold_count == 3
    assert evaluation.evidence_scope == "inner_validation_only"
    assert evaluation.lineage["outer_oos_consumed"] is False
    assert evaluation.lineage["b1e_performance_consumed"] is False
    for fold in evaluation.fold_evaluations:
        assert fold.availability_status == "available"
        assert fold.lineage["metric_scope"] == "inner_validation_only"
        assert set(fold.lineage) == {
            "fold",
            "features",
            "transformations",
            "parameters",
            "validation_population_fingerprint",
            "metric_scope",
        }


def test_exact_population_cache_reuses_only_selector_and_preprocessor_values(
    synthetic_table, frozen
):
    populations = _populations(synthetic_table, frozen)
    candidate = next(
        item
        for item in tuning_candidates(frozen, "decision_tree")
        if item.regime == "broad_downstream_gates"
        and item.selector_method == "p3_fold_local_ic_selector"
        and item.parameters["max_depth"] == 3
        and item.parameters["min_samples_leaf"] == 50
    )
    cache = {}
    first = evaluate_tuning_candidate(
        candidate,
        populations,
        frozen,
        execution_scope="synthetic_m1b_test",
        fold_local_cache=cache,
    )
    assert len(cache) == 3
    second = evaluate_tuning_candidate(
        candidate,
        populations,
        frozen,
        execution_scope="synthetic_m1b_test",
        fold_local_cache=cache,
    )
    assert len(cache) == 3
    assert first == second


def test_bounded_stability_selector_is_fold_local_and_records_diagnostics(
    synthetic_table, frozen
):
    population = _populations(
        synthetic_table,
        frozen,
        role="lightgbm_regression",
        regime="broad_downstream_gates",
    )[-1]
    selection = select_features_fold_local(
        population.train,
        population,
        frozen,
        "deterministic_stability_selection_with_redundancy_pruning",
    )
    diagnostics = selection.diagnostics
    assert 5 <= len(selection.selected_features) <= 28
    assert len(diagnostics["expanding_prefix_rows"]) == 5
    assert diagnostics["expanding_prefix_rows"] == sorted(
        diagnostics["expanding_prefix_rows"]
    )
    assert diagnostics["feature_diagnostics"]
    assert "redundancy_pairs_and_pruned_features" in diagnostics
    assert selection.lineage["fit_scope"] == "inner_training_only"
    assert (
        selection.lineage["fit_population_fingerprint"]
        == population.training_population_fingerprint
    )


def test_predictive_tie_breaker_prefers_simpler_tree(
    frozen,
):
    candidates = [
        candidate
        for candidate in tuning_candidates(frozen, "decision_tree")
        if candidate.regime == "broad_downstream_gates"
        and candidate.selector_method == "p3_fold_local_ic_selector"
    ]
    simple = min(candidates, key=lambda item: item.complexity_score)
    complex_candidate = max(candidates, key=lambda item: item.complexity_score)

    def evaluation(candidate):
        return CandidateEvaluation(
            candidate=candidate,
            availability_status="available_for_selection",
            failure_reason=None,
            valid_inner_fold_count=2,
            fold_evaluations=(),
            aggregate_metrics={
                "median_roc_auc": 0.6,
                "roc_auc_std": 0.02,
                "mean_selected_feature_count": 8.0,
            },
            evidence_scope="inner_validation_only",
            lineage={
                "outer_oos_consumed": False,
                "b1e_performance_consumed": False,
                "objective_inputs": "inner_validation_predictions_and_targets_only",
            },
        )

    context = TuningRunContext(
        outer_fold="decision_20200702T000000Z",
        outer_decision_timestamp=pd.Timestamp("2020-07-02", tz="UTC"),
    )
    winner = select_inner_winner(
        [evaluation(complex_candidate), evaluation(simple)],
        "decision_tree",
        frozen,
        context,
        selection_inputs={"objective": "median inner-fold ROC AUC"},
    )
    assert winner.candidate == simple


def test_contract_loader_rejects_manifest_drift(tmp_path, frozen):
    copied = tmp_path / "m1a"
    copied.mkdir()
    (copied / "manifest.json").write_text("{}\n")
    with pytest.raises(NestedWalkForwardContractError, match="manifest hash mismatch"):
        load_frozen_m1a_contract(
            copied,
            Path("unused"),
            expected_manifest_sha256=M1A_MANIFEST_SHA256,
        )
