import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from backtest.free_data_v1_evidence import (
    BENCHMARK_SYMBOLS,
    DEFAULT_INPUTS,
    DGS1MO_METADATA_ENDPOINT,
    DGS1MO_METADATA_PARAMS,
    DGS1MO_OBSERVATIONS_ENDPOINT,
    DGS1MO_OBSERVATION_PARAMS,
    NAMESPACES,
    RATE_NAMESPACES,
    FrozenInputs,
    _coverage_tables,
    _namespace_eligibility,
    _not_collected_rate_result,
    _reconcile_requirements,
    _verify_record,
    _write_rate_evidence,
    acquire_dgs1mo,
    build_free_data_v1_evidence,
    dgs1mo_request_contract,
    performance_contract,
    sha256_file,
    validate_market_evidence,
    verify_frozen_inputs,
)


VERSION = "20260731T120000Z-b1c"
CREATED_AT = "2026-07-31T12:00:00+00:00"


@pytest.fixture(scope="module")
def validated_boundary():
    preflight = verify_frozen_inputs()
    market = validate_market_evidence(preflight)
    requirements, identity, actions, _ = _reconcile_requirements(
        preflight, market
    )
    coverage, benchmarks = _coverage_tables(
        requirements, identity, market
    )
    return {
        "preflight": preflight,
        "market": market,
        "requirements": requirements,
        "identity": identity,
        "actions": actions,
        "coverage": coverage,
        "benchmarks": benchmarks,
    }


@pytest.fixture(scope="module")
def built_artifact(tmp_path_factory, validated_boundary):
    root = tmp_path_factory.mktemp("b1c-primary") / VERSION
    with (
        patch(
            "backtest.free_data_v1_evidence.verify_frozen_inputs",
            return_value=validated_boundary["preflight"],
        ),
        patch(
            "backtest.free_data_v1_evidence.validate_market_evidence",
            return_value=validated_boundary["market"],
        ),
    ):
        manifest = build_free_data_v1_evidence(
            root,
            version=VERSION,
            created_at_utc=CREATED_AT,
        )
    return root, manifest


def test_upstream_manifest_and_input_hash_fail_closed(tmp_path):
    bad_p4 = tmp_path / "p4"
    bad_p4.mkdir()
    (bad_p4 / "manifest.json").write_text("{}\n")
    bad_inputs = replace(DEFAULT_INPUTS, p4_root=bad_p4)
    with pytest.raises(RuntimeError, match="p4 manifest hash mismatch"):
        verify_frozen_inputs(bad_inputs)

    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="hash mismatch"):
        _verify_record(
            payload,
            {"size_bytes": 7, "sha256": "0" * 64},
            "tampered input",
        )


def test_exact_180_plus_4_and_135_45_reconciliation(validated_boundary):
    requirements = validated_boundary["requirements"]
    identity = validated_boundary["identity"]
    assert len(requirements) == 184
    assert requirements["requirement_id"].is_unique
    holdings = requirements[requirements["instrument_role"].eq("holding")]
    benchmarks = requirements[
        requirements["instrument_role"].eq("benchmark")
    ]
    assert len(holdings) == 180
    assert holdings["stable_row_id"].notna().all()
    assert holdings["stable_row_id"].is_unique
    assert len(benchmarks) == 4
    assert set(benchmarks["ticker"]) == set(BENCHMARK_SYMBOLS)
    assert holdings["requirement_state"].value_counts().to_dict() == {
        "matured_2015_2023": 135,
        "open_2024_2026": 45,
    }
    assert identity["s1_coverage_status"].value_counts().to_dict() == {
        "ambiguous": 135,
        "unsupported": 49,
    }
    assert not identity["s1_coverage_status"].eq("matched").any()


def test_all_136_yahoo_symbols_and_exact_metadata_validate(validated_boundary):
    market = validated_boundary["market"]
    lineage = market["price_lineage"]
    assert len(market["relevant_symbols"]) == 136
    assert len(lineage) == 136
    assert lineage["provider_symbol"].is_unique
    assert (lineage["instrument_role"] == "holding").sum() == 132
    assert (lineage["instrument_role"] == "benchmark").sum() == 4
    assert lineage["currency"].eq("USD").all()
    assert lineage["provider_timezone"].eq("America/New_York").all()
    assert lineage["data_granularity"].eq("1d").all()
    assert not lineage["certified_security_action_ledger"].any()
    assert lineage["raw_stored_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()
    assert lineage["response_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()
    assert lineage["normalized_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()


def test_entry_exit_month_end_and_benchmark_alignment(validated_boundary):
    coverage = validated_boundary["coverage"]
    holdings = coverage[coverage["instrument_role"].eq("holding")]
    matured = holdings[
        holdings["requirement_state"].eq("matured_2015_2023")
    ]
    assert holdings["entry_observed_common"].all()
    assert matured["exit_observed_common"].all()
    assert matured["required_month_end_count"].eq(36).all()
    assert holdings["missing_common_month_end_count"].eq(0).all()
    assert holdings["benchmark_gap_count"].eq(0).all()
    assert matured["relative_evidence_status"].eq(
        "observed_common_provider_evidence_available"
    ).all()
    benchmarks = validated_boundary["benchmarks"]
    assert len(benchmarks) == 4
    assert benchmarks["s1_coverage_status"].eq("unsupported").all()
    assert benchmarks["assigned_holding_count"].sum() == 180


def test_no_ticker_substitution_forward_fill_or_unsupported_recovery(
    validated_boundary,
):
    requirements = validated_boundary["requirements"]
    holdings = requirements[requirements["instrument_role"].eq("holding")]
    identity = validated_boundary["identity"]
    actions = validated_boundary["actions"]
    assert holdings["ticker"].eq(holdings["provider_symbol"]).all()
    assert not identity["current_ticker_substitution_used"].any()
    assert not identity["ticker_chaining_used"].any()
    assert not actions["forward_fill_across_unresolved_event_allowed"].any()
    assert not actions[
        "event_inferred_from_disappearance_or_form_family"
    ].any()
    assert not actions[
        "unsupported_recovery_allowed_in_observed_namespace"
    ].any()
    assert not actions[
        "assumed_outcome_allowed_in_labels_or_training"
    ].any()
    current = actions[actions["retrieved_document_count"].notna()]
    assert len(current) == 15
    assert (current["retrieved_document_count"] > 0).sum() == 14
    assert current.loc[
        current["ticker"].eq("SSTK"), "primary_document_support_state"
    ].tolist() == ["no_retrieved_primary_document_claim"]


def test_adjusted_close_assumption_is_disclosed_not_certified(
    validated_boundary,
):
    contract = performance_contract()
    assert contract["price_and_event"]["price_field"] == (
        "frozen Session 8E Yahoo provider adjclose"
    )
    assert not contract["price_and_event"][
        "provider_certifies_exact_adjustment_semantics"
    ]
    assert not contract["price_and_event"][
        "certified_security_action_ledger"
    ]
    lineage = validated_boundary["market"]["price_lineage"]
    assert lineage["adjustment_semantics"].str.contains(
        "no_double_counting"
    ).all()
    assert lineage["adjustment_semantics"].str.contains(
        "does_not_certify"
    ).all()


def test_namespace_contracts_and_eligibility_are_physically_separate(
    built_artifact,
):
    root, _ = built_artifact
    for namespace in NAMESPACES:
        contract_path = root / f"contracts/namespaces/{namespace}.json"
        eligibility_path = (
            root / f"outputs/namespaces/{namespace}/eligibility.parquet"
        )
        assert contract_path.is_file()
        assert eligibility_path.is_file()
        eligibility = pd.read_parquet(eligibility_path)
        assert len(eligibility) == 184
        assert eligibility["namespace"].eq(namespace).all()
        assert not eligibility["performance_calculated"].any()
        assert not eligibility["assumed_outcome_used"].any()
        assert not eligibility["benchmark_gap_imputed"].any()
    assert len({
        (root / f"contracts/namespaces/{name}.json").resolve()
        for name in NAMESPACES
    }) == 4


def test_exact_dgs1mo_contract_and_failure_preservation(tmp_path):
    contract = dgs1mo_request_contract()
    assert contract["metadata"] == {
        "endpoint": DGS1MO_METADATA_ENDPOINT,
        "params": DGS1MO_METADATA_PARAMS,
    }
    assert contract["observations"] == {
        "endpoint": DGS1MO_OBSERVATIONS_ENDPOINT,
        "params": DGS1MO_OBSERVATION_PARAMS,
    }
    assert DGS1MO_OBSERVATION_PARAMS["realtime_start"] == "2026-07-17"
    assert DGS1MO_OBSERVATION_PARAMS["realtime_end"] == "2026-07-17"
    assert DGS1MO_OBSERVATION_PARAMS["observation_start"] == "2015-07-01"
    assert DGS1MO_OBSERVATION_PARAMS["observation_end"] == "2026-07-02"
    assert DGS1MO_OBSERVATION_PARAMS["frequency"] == "d"
    assert DGS1MO_OBSERVATION_PARAMS["units"] == "lin"

    missing = acquire_dgs1mo(api_key=None)
    assert missing["status"] == (
        "unavailable_not_collected_missing_fred_api_key"
    )
    assert not missing["request_made"]

    class FailingSession:
        def get(self, *args, **kwargs):
            raise OSError("preserved test transport failure")

    failed = acquire_dgs1mo(
        api_key="test-key",
        session=FailingSession(),
    )
    assert failed["status"] == "unavailable_acquisition_failure_preserved"
    assert failed["request_made"]
    assert len(failed["requests"]) == 2
    assert all(
        row["status"] == "transport_failure_preserved"
        for row in failed["requests"]
    )
    records = _write_rate_evidence(tmp_path, failed)
    request_manifest = tmp_path / "lineage/request_manifest.jsonl"
    assert request_manifest.is_file()
    saved = [json.loads(line) for line in request_manifest.read_text().splitlines()]
    assert len(saved) == 2
    assert all("api_key" not in row for row in saved)
    status = json.loads((tmp_path / "support/rate_status.json").read_text())
    assert status["dgs1mo_status"] == (
        "unavailable_acquisition_failure_preserved"
    )
    assert records


def test_zero_risk_free_namespace_is_separate(built_artifact):
    root, _ = built_artifact
    status = json.loads((root / "support/rate_status.json").read_text())
    assert status["dgs1mo_status"] == "not_collected_build_flag_disabled"
    zero = status["zero_risk_free_namespace"]
    assert zero["namespace"] == "zero_risk_free_sharpe_diagnostic"
    assert zero["risk_free_return"] == 0.0
    assert zero["diagnostic_only"]
    assert zero["physically_and_semantically_separate_from_dgs1mo"]
    for namespace in RATE_NAMESPACES:
        assert (root / f"contracts/rates/{namespace}.json").is_file()
    assert not (root / "outputs/risk_free_observations.parquet").exists()


def test_manifest_outputs_and_no_performance_artifacts(built_artifact):
    root, manifest_path = built_artifact
    manifest = json.loads(manifest_path.read_text())
    assert manifest["artifact_class"] == (
        "FREE_DATA_V1_PERFORMANCE_INPUT_EVIDENCE_B1C"
    )
    assert manifest["coverage"]["requirement_rows"] == 184
    assert manifest["coverage"]["common_entry_holding_rows"] == 180
    assert manifest["coverage"]["matured_common_exit_holding_rows"] == 135
    assert manifest["coverage"]["missing_common_month_end_observations"] == 0
    assert not manifest["claim"]["performance_calculated"]
    assert not manifest["claim"]["nav_created"]
    assert not manifest["claim"]["backtest_run"]
    required = {
        "outputs/requirements.parquet",
        "outputs/security_identity.parquet",
        "outputs/security_actions.parquet",
        "outputs/prices.parquet",
        "outputs/benchmark_requirements.parquet",
        "outputs/coverage.parquet",
        "support/coverage_summary.json",
        "support/rate_status.json",
        "lineage/request_manifest.jsonl",
    }
    paths = {record["path"] for record in manifest["records"]}
    assert required.issubset(paths)
    output_names = {path.name.lower() for path in (root / "outputs").rglob("*")}
    assert not any("nav" in name for name in output_names)
    assert not any("return" in name for name in output_names)
    assert not any("metric" in name for name in output_names)
    assert not any("backtest" in name for name in output_names)


def test_deterministic_artifact_hashes(
    tmp_path,
    built_artifact,
    validated_boundary,
):
    first_root, _ = built_artifact
    second_root = tmp_path / VERSION
    with (
        patch(
            "backtest.free_data_v1_evidence.verify_frozen_inputs",
            return_value=validated_boundary["preflight"],
        ),
        patch(
            "backtest.free_data_v1_evidence.validate_market_evidence",
            return_value=validated_boundary["market"],
        ),
    ):
        build_free_data_v1_evidence(
            second_root,
            version=VERSION,
            created_at_utc=CREATED_AT,
        )
    first = {
        path.relative_to(first_root).as_posix(): sha256_file(path)
        for path in first_root.rglob("*")
        if path.is_file()
    }
    second = {
        path.relative_to(second_root).as_posix(): sha256_file(path)
        for path in second_root.rglob("*")
        if path.is_file()
    }
    assert first == second


def test_refuses_nonempty_artifact_root_before_validation(tmp_path):
    root = tmp_path / VERSION
    root.mkdir()
    (root / "keep.txt").write_text("do not overwrite\n")
    with pytest.raises(RuntimeError, match="target is not empty"):
        build_free_data_v1_evidence(root, version=VERSION)
    assert (root / "keep.txt").read_text() == "do not overwrite\n"

