from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from portfolio.build_us_free_product import (
    P2_MANIFEST_SHA256,
    TARGET_N,
    USFreeProductContractError,
    assess_data_baseline,
    build,
    verify_product_artifact,
)


@pytest.fixture(scope="module")
def product_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("us1a") / "20260801T183000Z-us1a-test"
    build(root)
    return root


def test_data_baseline_freezes_p2_without_downstream_inputs() -> None:
    result = assess_data_baseline()
    assert result["selected_baseline"] == "accepted_P2"
    assert result["selected_manifest_sha256"] == P2_MANIFEST_SHA256
    assert result["downstream_output_observed_for_decision"] is False
    assert result["gates"]["row_coverage"]["status"] == "fail"
    assert result["gates"]["feature_compatibility"]["status"] == "fail"
    assert result["gates"]["schema"]["status"] == "pass"
    assert result["gates"]["source_lineage"]["status"] == "pass"


def test_product_artifact_is_row_complete_and_offline(product_root: Path) -> None:
    result = verify_product_artifact(
        product_root,
        reverify_preserved_boundaries=False,
    )
    assert result["score_row_roles"] == 87_612
    assert result["candidate_rows"] == 43_806
    assert result["gate_rows"] == 525_672
    assert result["liquidity_required_rows"] == 1_477
    assert result["liquidity_evidence_rows"] == 44_310
    assert result["holding_rows"] == 120
    assert result["shortlist_rows"] == TARGET_N
    assert result["performance_calculated"] is False
    assert result["external_data_collected"] is False


def test_product_event_mapping_is_exact_or_explicitly_uncollected(
    product_root: Path,
) -> None:
    shortlist = pd.read_parquet(
        product_root / "outputs/final_shortlist_2026.parquet"
    )
    assert shortlist["event_evidence_collected"].sum() == 5
    missing = shortlist.loc[~shortlist["event_evidence_collected"]]
    assert len(missing) == 10
    assert missing["event_evidence_status"].eq(
        "event_evidence_not_collected"
    ).all()
    covered = shortlist.loc[shortlist["event_evidence_collected"]]
    assert covered["event_evidence_exact_identity_match"].all()
    assert covered["e1_deterministic_action"].eq("unresolved").all()


def test_product_contract_and_report_prohibit_performance(product_root: Path) -> None:
    contract = json.loads(
        (product_root / "configuration/product_contract.json").read_text()
    )
    report = (product_root / "report/product_report.md").read_text()
    assert contract["model_route"]["m1d_performance_used_for_selection"] is False
    assert contract["claim"]["performance_calculated"] is False
    assert contract["claim"]["external_data_collected"] is False
    assert "US1A calculated no CAGR" in report
    assert "event_evidence_not_collected" in report


def test_product_target_is_non_overwriting(product_root: Path) -> None:
    with pytest.raises(FileExistsError):
        build(product_root)


def test_invalid_version_fails_before_materialization(tmp_path: Path) -> None:
    root = tmp_path / "mutable-latest"
    with pytest.raises(USFreeProductContractError):
        build(root)
    assert not root.exists()
