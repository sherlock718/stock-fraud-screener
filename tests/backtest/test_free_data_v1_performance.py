from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from backtest.free_data_v1_nav import (
    DGS1MO_NAMESPACE,
    PERFORMANCE_NAMESPACES,
    ZERO_RATE_NAMESPACE,
    EvidenceValidationError,
)
from backtest.free_data_v1_performance import (
    FROZEN_B1D_ENGINE_SHA256,
    FROZEN_B1D_TEST_SHA256,
    MATURED_YEARS,
    OPEN_YEARS,
    build_free_data_v1_performance,
    preflight_controlled_run,
    sha256_file,
    verify_performance_artifact,
)


VERSION = "20260801T120000Z-test-b1e"
CREATED = "2026-08-01T12:00:00+00:00"


@pytest.fixture(scope="module")
def built_artifact(tmp_path_factory: pytest.TempPathFactory) -> Path:
    parent = tmp_path_factory.mktemp("b1e-primary")
    root = parent / VERSION
    build_free_data_v1_performance(
        root,
        version=VERSION,
        created_at_utc=CREATED,
    )
    return root


def test_preflight_reverifies_frozen_b1c_and_b1d_hashes() -> None:
    result = preflight_controlled_run()
    summary = result.summary
    assert summary["b1c_manifest_record_count"] == 29
    assert summary["b1c_validated_input_count"] == 7
    assert summary["b1c_code_lineage_count"] == 3
    assert summary["b1d_engine_sha256"] == FROZEN_B1D_ENGINE_SHA256
    assert summary["b1d_test_sha256"] == FROZEN_B1D_TEST_SHA256
    assert summary["matured_vintage_counts"] == {year: 15 for year in MATURED_YEARS}
    assert summary["open_vintage_counts"] == {year: 15 for year in OPEN_YEARS}
    assert summary["dgs1mo_available"] is False

    with pytest.raises(EvidenceValidationError, match="engine hash mismatch"):
        preflight_controlled_run(expected_engine_sha256="0" * 64)
    with pytest.raises(EvidenceValidationError, match="test hash mismatch"):
        preflight_controlled_run(expected_test_sha256="0" * 64)


def test_artifact_preserves_all_namespaces_coverage_and_rate_boundaries(
    built_artifact: Path,
) -> None:
    verified = verify_performance_artifact(built_artifact)
    assert verified["record_count"] > 70
    assert verified["metric_row_count"] == 2_080
    assert verified["verification_status"] == "all_records_and_report_reconciled"

    manifest = json.loads((built_artifact / "manifest.json").read_text())
    assert set(manifest["namespace_hashes"]) == set(PERFORMANCE_NAMESPACES)
    assert set(manifest["rate_namespace_hashes"]) == {
        DGS1MO_NAMESPACE,
        ZERO_RATE_NAMESPACE,
    }
    assert manifest["claim"]["historical_performance_calculated"] is True
    assert manifest["claim"]["survivorship_complete"] is False
    assert manifest["claim"]["provider_certified"] is False
    assert manifest["claim"]["model_executed"] is False

    metrics = pd.read_parquet(built_artifact / "outputs/metrics.parquet")
    assert set(metrics["performance_namespace"]) == set(PERFORMANCE_NAMESPACES)
    assert set(metrics["risk_free_namespace"]) == {
        DGS1MO_NAMESPACE,
        ZERO_RATE_NAMESPACE,
    }
    dgs = metrics[metrics["risk_free_namespace"].eq(DGS1MO_NAMESPACE)]
    assert len(dgs) == 400
    assert dgs["metric_value"].isna().all()
    assert dgs["risk_free_interval_count"].eq(0).all()
    assert dgs["availability_reason"].eq(
        "exact_dgs1mo_observations_absent"
    ).all()

    aggregate_net = metrics[
        metrics["performance_namespace"].eq("observed_available_diagnostic")
        & metrics["metric_scope"].eq("aggregate_strategy")
        & metrics["decision_year"].isna()
        & metrics["basis"].eq("net")
        & metrics["stream"].eq("portfolio")
        & metrics["risk_free_namespace"].eq(ZERO_RATE_NAMESPACE)
    ].set_index("metric_name")
    assert aggregate_net.loc["cagr", "metric_value"] == pytest.approx(
        0.18687408199068356
    )
    assert aggregate_net.loc[
        "maximum_drawdown", "metric_value"
    ] == pytest.approx(-0.3031790517342182)
    assert aggregate_net.loc["sharpe_ratio", "metric_value"] == pytest.approx(
        0.8261925399249463
    )
    assert aggregate_net["scenario_imputed_capital"].eq(0.0).all()
    assert aggregate_net["resolved_holding_count"].eq(135).all()
    assert aggregate_net["resolved_capital"].eq(1_800_000.0).all()
    assert aggregate_net["benchmark_gap_count"].eq(0).all()

    open_vintages = pd.read_parquet(
        built_artifact / "outputs/open_vintages.parquet"
    )
    assert len(open_vintages) == 45
    assert set(open_vintages["decision_year"].astype(int)) == set(OPEN_YEARS)
    assert not open_vintages["completed_vintage_metrics_included"].any()

    for namespace in PERFORMANCE_NAMESPACES:
        base = built_artifact / f"outputs/namespaces/{namespace}"
        vintage = pd.read_parquet(base / "nav/vintage.parquet")
        aggregate = pd.read_parquet(base / "nav/aggregate.parquet")
        coverage = pd.read_parquet(base / "ledgers/coverage.parquet")
        transactions = pd.read_parquet(base / "ledgers/transactions.parquet")
        capital = pd.read_parquet(base / "ledgers/capital.parquet")
        assert set(vintage["decision_year"].astype(int)) == set(MATURED_YEARS)
        assert len(aggregate) == 133
        assert len(coverage) == 135
        assert coverage["resolved_for_nav"].all()
        assert len(transactions) == 540
        assert len(capital) == 9
        assert pd.read_parquet(base / "outcomes/observed.parquet").shape[0] == 135
        assert pd.read_parquet(base / "outcomes/bounded_scenario.parquet").empty
        assert pd.read_parquet(base / "outcomes/provider_confirmed.parquet").empty
        assert pd.read_parquet(base / "outcomes/unsupported_unresolved.parquet").empty
        assert pd.read_parquet(base / "ledgers/events.parquet").empty
        assert pd.read_parquet(base / "ledgers/scenarios.parquet").empty

    report = (built_artifact / "report/product_report.md").read_text()
    assert report.index("## Coverage before performance") < report.index(
        "## Aggregate overlapping-strategy results"
    )
    assert "not survivorship-complete certification" in report
    assert "not personalized investment advice" in report
    assert "promise of future performance" in report
    assert f"`{DGS1MO_NAMESPACE}` has 0 observed intervals" in report


def test_frozen_rerun_is_byte_deterministic_and_non_overwriting(
    built_artifact: Path,
    tmp_path: Path,
) -> None:
    second = tmp_path / VERSION
    build_free_data_v1_performance(
        second,
        version=VERSION,
        created_at_utc=CREATED,
    )
    first_manifest = json.loads((built_artifact / "manifest.json").read_text())
    second_manifest = json.loads((second / "manifest.json").read_text())
    assert first_manifest == second_manifest
    assert sha256_file(built_artifact / "manifest.json") == sha256_file(
        second / "manifest.json"
    )
    assert {
        item["path"]: item["sha256"] for item in first_manifest["records"]
    } == {item["path"]: item["sha256"] for item in second_manifest["records"]}

    with pytest.raises(RuntimeError, match="target is not empty"):
        build_free_data_v1_performance(
            second,
            version=VERSION,
            created_at_utc=CREATED,
        )


def test_verifier_detects_generated_record_drift(
    built_artifact: Path,
    tmp_path: Path,
) -> None:
    copy = tmp_path / VERSION
    import shutil

    shutil.copytree(built_artifact, copy)
    report = copy / "report/product_report.md"
    report.write_text(report.read_text() + "drift\n")
    with pytest.raises(EvidenceValidationError, match="record (size|hash) mismatch"):
        verify_performance_artifact(copy)
