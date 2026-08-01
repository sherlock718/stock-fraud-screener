"""Build a non-promoted P2 candidate from versioned refreshed US evidence."""
from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import pipeline.build_corrected_feature_population as corrected_population
from pipeline.build_corrected_feature_population import (
    HORIZONS,
    NON_ML_FACTOR_PREREQUISITES,
    PRICE_FEATURES,
    PRIMARY_DATASET,
    PRIMARY_POPULATION,
    assert_identity,
    run_module,
    stable_row_id,
    validate_canonical_population,
)
from pipeline.event_time_cohorts import proven_availability
from pipeline.us_refresh_sources import (
    artifact_record,
    git_head,
    runtime_environment,
    sha256_file,
    validate_refresh_id,
)


ROOT = Path(__file__).resolve().parents[1]
PINNED_P2_MANIFEST_SHA256 = (
    "40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3"
)
MACRO_COLUMNS = {
    "treasury_10y",
    "treasury_2y",
    "yield_curve",
    "fed_funds_rate",
    "credit_spread_baa",
    "hy_spread",
    "cpi_yoy",
    "recession",
    "vix",
    "real_rate_10y",
    "credit_tightening",
    "macro_regime",
    "value_in_high_rate",
    "value_in_recession",
    "momentum_in_expansion",
    "quality_in_recession",
    "levered_in_tight_credit",
}


def _verify_manifest(
    artifact_root: Path,
    *,
    artifact_class: str,
) -> tuple[Path, dict[str, Any]]:
    manifest_path = artifact_root / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError(f"source manifest is missing: {manifest_path}")
    payload = json.loads(manifest_path.read_text())
    if payload.get("artifact_class") != artifact_class:
        raise RuntimeError(
            f"unexpected artifact class in {manifest_path}: "
            f"{payload.get('artifact_class')}"
        )
    for item in payload.get("records", []):
        path = artifact_root / item["path"]
        if (
            not path.is_file()
            or path.stat().st_size != int(item["size_bytes"])
            or sha256_file(path) != item["sha256"]
        ):
            raise RuntimeError(f"source record mismatch: {path}")
    return manifest_path, payload


def _macro_unavailable(frame: pd.DataFrame) -> dict[str, int]:
    counts = {
        column: int(frame[column].notna().sum())
        for column in sorted(MACRO_COLUMNS & set(frame.columns))
    }
    populated = {name: count for name, count in counts.items() if count}
    if populated:
        raise RuntimeError(
            f"uncertified macro fields were populated: {populated}"
        )
    return counts


def _copy_input(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)


def _build_market_inputs(
    annual: pd.DataFrame,
    row_support: pd.DataFrame,
    market_root: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Reuse the accepted P2 functions against a D1 versioned market root."""
    baseline_market_root = corrected_population.SESSION8E
    corrected_population.SESSION8E = market_root
    try:
        price_features = corrected_population.build_price_features(
            annual,
            row_support,
        )
        prepared, support = corrected_population.attach_population_labels(
            price_features,
            PRIMARY_POPULATION,
        )
    finally:
        corrected_population.SESSION8E = baseline_market_root
    return price_features, prepared, support


def build(
    *,
    step2_root: Path,
    market_root: Path,
    artifact_root: Path,
    refresh_id: str,
) -> dict[str, Any]:
    """Build one review-only observed P2 version without changing the baseline."""
    validate_refresh_id(refresh_id)
    step2_root = step2_root.resolve()
    market_root = market_root.resolve()
    artifact_root = artifact_root.resolve()
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(
            f"refusing to reuse non-empty P2 candidate root: {artifact_root}"
        )
    step2_manifest, step2_document = _verify_manifest(
        step2_root,
        artifact_class="CANONICAL_US_REFRESH_CORRECTED_STEP2",
    )
    market_manifest, market_document = _verify_manifest(
        market_root,
        artifact_class="CANONICAL_US_REFRESH_MARKET_LABEL_CONTRACTS",
    )
    if (
        step2_document.get("refresh_id") != refresh_id
        or market_document.get("refresh_id") != refresh_id
    ):
        raise RuntimeError("source refresh IDs do not match the P2 version")
    market_step2 = market_document["validated_inputs"][0]
    if market_step2["sha256"] != sha256_file(step2_manifest):
        raise RuntimeError("market contracts do not consume this Step 2 version")

    for directory in (
        "inputs",
        "configuration",
        "checkpoints",
        "intermediate/observed_only",
        "outputs/observed_only",
        "support",
        "logs",
        "lineage",
    ):
        (artifact_root / directory).mkdir(parents=True, exist_ok=True)
    input_sources = {
        "certified_snapshots.parquet": (
            step2_root / "outputs/certified_snapshots.parquet"
        ),
        "step2_manifest.json": step2_manifest,
        "market_manifest.json": market_manifest,
        "security_mapping.parquet": (
            market_root / "inputs/security_mapping.parquet"
        ),
        "observed_only_labels.parquet": (
            market_root / "outputs/observed_only/labels.parquet"
        ),
        "observed_only_row_horizon.parquet": (
            market_root / "support/observed_only_row_horizon.parquet"
        ),
        "horizon_support.parquet": (
            market_root / "support/horizon_support.parquet"
        ),
    }
    for name, source in input_sources.items():
        _copy_input(source, artifact_root / "inputs" / name)
    for contract in sorted((market_root / "contracts").glob("*.json")):
        _copy_input(
            contract,
            artifact_root / "inputs/contracts" / contract.name,
        )

    snapshots = pd.read_parquet(
        step2_root / "outputs/certified_snapshots.parquet"
    )
    annual = snapshots[snapshots["period_type"].eq("annual")].copy()
    _, eligible = proven_availability(annual)
    if annual.empty or annual["entity_id"].isna().any() or not eligible.all():
        raise RuntimeError("P2 source contains unproven annual rows")
    annual["availability_timestamp"] = pd.to_datetime(
        annual["availability_timestamp"],
        utc=True,
    )
    annual["stable_row_id"] = stable_row_id(annual)
    if annual["stable_row_id"].duplicated().any():
        raise RuntimeError("refreshed P2 stable row identity collision")
    annual_path = (
        artifact_root / "intermediate/certified_annual_with_row_id.parquet"
    )
    annual.to_parquet(annual_path, index=False)

    row_support = pd.read_parquet(
        market_root / "support/observed_only_row_horizon.parquet"
    )
    price_features, prepared, support = _build_market_inputs(
        annual,
        row_support,
        market_root,
    )
    price_path = artifact_root / "intermediate/decision_price_features.parquet"
    price_features.to_parquet(price_path, index=False)
    checkpoint = {
        "completed": True,
        "rows": len(price_features),
        "symbols_attempted": int(
            price_features["provider_symbol"].nunique(dropna=True)
        ),
        "status_counts": (
            price_features["price_feature_status"].value_counts().to_dict()
        ),
        "source_manifest_sha256": sha256_file(market_manifest),
    }
    checkpoint_path = artifact_root / "checkpoints/price_features.json"
    checkpoint_path.write_text(json.dumps(checkpoint, indent=2) + "\n")

    macro_scaffold = annual[
        [
            "cik",
            "ticker",
            "filed_date",
            "fiscal_year",
            "fiscal_quarter",
            "period_type",
        ]
    ].copy()
    macro_path = (
        artifact_root / "intermediate/macro_unavailable_scaffold.parquet"
    )
    macro_scaffold.to_parquet(macro_path, index=False)
    prepared_path = (
        artifact_root
        / "intermediate/observed_only/01_prepared_price_and_labels.parquet"
    )
    prepared.to_parquet(prepared_path, index=False)
    expected_ids = set(prepared["stable_row_id"])

    commands = []
    step5_path = (
        artifact_root
        / "intermediate/observed_only/02_step5_features.parquet"
    )
    step5_args = [
        "--snapshots",
        str(annual_path),
        "--prices",
        str(prepared_path),
        "--macro",
        str(macro_path),
        "--out",
        str(step5_path),
    ]
    run_module(
        "pipeline.step5_compute_features",
        step5_args,
        artifact_root / "logs/observed_only_step5.log",
    )
    commands.append(
        ["python3", "-m", "pipeline.step5_compute_features", *step5_args]
    )
    assert_identity(step5_path, expected_ids, PRIMARY_POPULATION)

    step6_path = (
        artifact_root / "intermediate/observed_only/03_step6_clean.parquet"
    )
    step6_args = [
        "--input",
        str(step5_path),
        "--out",
        str(step6_path),
        "--skip-imputation",
        "--skip-survivorship-policy",
    ]
    run_module(
        "pipeline.step6_clean",
        step6_args,
        artifact_root / "logs/observed_only_step6.log",
    )
    commands.append(
        ["python3", "-m", "pipeline.step6_clean", *step6_args]
    )
    assert_identity(step6_path, expected_ids, PRIMARY_POPULATION)

    final_path = artifact_root / PRIMARY_DATASET
    taxonomy_args = ["--input", str(step6_path), "--out", str(final_path)]
    run_module(
        "pipeline.enrich_fraud_taxonomy",
        taxonomy_args,
        artifact_root / "logs/observed_only_taxonomy.log",
    )
    commands.append(
        ["python3", "-m", "pipeline.enrich_fraud_taxonomy", *taxonomy_args]
    )
    assert_identity(final_path, expected_ids, PRIMARY_POPULATION)
    final = pd.read_parquet(final_path)
    canonical_validation = validate_canonical_population(final, expected_ids)
    macro_counts = _macro_unavailable(final)

    price_counts = (
        price_features["price_feature_status"].value_counts().to_dict()
    )
    summary_rows = [
        {
            "population": PRIMARY_POPULATION,
            "feature_family": "certified_accounting",
            "supported": len(final),
            "unavailable": 0,
            "excluded": 0,
        },
        {
            "population": PRIMARY_POPULATION,
            "feature_family": "required_price_features",
            "supported": int(price_counts.get("supported", 0)),
            "unavailable": int(price_counts.get("unavailable", 0)),
            "excluded": int(price_counts.get("excluded", 0)),
        },
    ]
    base_excluded = int(
        support[
            support["horizon"].eq("6m")
            & support["classification"].eq("excluded")
        ].shape[0]
    )
    summary_rows.append(
        {
            "population": PRIMARY_POPULATION,
            "feature_family": "macro_vintages",
            "supported": 0,
            "unavailable": len(final) - base_excluded,
            "excluded": base_excluded,
        }
    )
    summary_rows.append(
        {
            "population": PRIMARY_POPULATION,
            "feature_family": "fraud_taxonomy",
            "supported": int(final["fraud_score_composite"].notna().sum()),
            "unavailable": int(final["fraud_score_composite"].isna().sum()),
            "excluded": 0,
        }
    )
    for horizon in HORIZONS:
        counts = (
            support[support["horizon"].eq(horizon)]["classification"]
            .value_counts()
            .to_dict()
        )
        summary_rows.append(
            {
                "population": PRIMARY_POPULATION,
                "feature_family": f"label_{horizon}",
                "supported": int(counts.get("supported", 0)),
                "unavailable": int(counts.get("unavailable", 0)),
                "excluded": int(counts.get("excluded", 0)),
            }
        )
    summary = pd.DataFrame(summary_rows)
    if not (
        summary[["supported", "unavailable", "excluded"]].sum(axis=1)
        == len(annual)
    ).all():
        raise RuntimeError("P2 coverage rows do not partition the population")
    summary_path = artifact_root / "support/feature_population_summary.parquet"
    summary.to_parquet(summary_path, index=False)

    validation = {
        "annual_rows": len(annual),
        "stable_row_ids": annual["stable_row_id"].nunique(),
        "primary_population": PRIMARY_POPULATION,
        "primary_dataset": PRIMARY_DATASET,
        "macro_supported_rows": sum(macro_counts.values()),
        "macro_non_null_counts": macro_counts,
        "row_identity_preserved_after_each_stage": True,
        "step5_pit_transformations_reused": True,
        "step6_imputation_enabled": False,
        "step6_inferred_delisting_returns_enabled": False,
        "canonical_population": canonical_validation,
    }
    validation_path = artifact_root / "validation_summary.json"
    validation_path.write_text(json.dumps(validation, indent=2) + "\n")
    configuration = {
        "schema_version": 3,
        "session": "D1_P2_REVIEW_CANDIDATE",
        "refresh_id": refresh_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "US annual observed-only",
        "primary_population": PRIMARY_POPULATION,
        "primary_dataset": PRIMARY_DATASET,
        "required_price_features": list(PRICE_FEATURES),
        "non_ml_factor_prerequisites": {
            family: list(columns)
            for family, columns in NON_ML_FACTOR_PREREQUISITES.items()
        },
        "macro": (
            "unavailable until certified release-vintage evidence exists"
        ),
        "step5": "corrected PIT event-time transformations",
        "step6_imputation": "disabled",
        "step6_survivorship_policy": "disabled",
        "inferred_delisting_returns": "disabled",
        "promotion": "explicit authorization required",
        "commands": commands,
    }
    config_path = artifact_root / "configuration/config.json"
    config_path.write_text(json.dumps(configuration, indent=2) + "\n")
    lineage_path = artifact_root / "lineage/git_status_porcelain.txt"
    lineage_path.write_bytes(
        subprocess.check_output(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=ROOT,
        )
    )

    artifact_files = sorted(
        path
        for path in artifact_root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    )
    code_paths = [
        ROOT / "pipeline/build_refreshed_us_p2.py",
        ROOT / "pipeline/build_corrected_feature_population.py",
        ROOT / "pipeline/step5_compute_features.py",
        ROOT / "pipeline/step6_clean.py",
        ROOT / "pipeline/enrich_fraud_taxonomy.py",
        ROOT / "pipeline/event_time_cohorts.py",
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": "CANONICAL_US_ANNUAL_P2_REVIEW_CANDIDATE",
        "created_at_utc": configuration["created_at_utc"],
        "refresh_id": refresh_id,
        "current_head": git_head(),
        "canonical_entrypoint": {
            "command": (
                "python3 -m workflows.refresh_us_canonical "
                f"--refresh-id {refresh_id} --collect"
            ),
            "artifact_root": str(artifact_root),
            "primary_dataset": PRIMARY_DATASET,
            "population": PRIMARY_POPULATION,
        },
        "validated_inputs": [
            {
                "path": str(step2_manifest),
                "sha256": sha256_file(step2_manifest),
            },
            {
                "path": str(market_manifest),
                "sha256": sha256_file(market_manifest),
            },
        ],
        "configuration": configuration,
        "records": [
            artifact_record(
                artifact_root,
                path,
                "p2_candidate_input_output_checkpoint_or_lineage",
            )
            for path in artifact_files
        ],
        "code_lineage": [
            {
                "path": path.relative_to(ROOT).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
            for path in code_paths
        ],
        "environment": {
            **runtime_environment(),
            "platform_python": platform.python_version(),
            "numpy": np.__version__,
        },
        "primary_dataset": PRIMARY_DATASET,
        "canonical_validation": canonical_validation,
        "baseline": {
            "stage": "P2",
            "manifest_sha256": PINNED_P2_MANIFEST_SHA256,
            "mutated": False,
        },
        "promotion": {
            "status": "not_promoted",
            "automatic_promotion": False,
            "explicit_authorization_required": True,
            "downstream_p3_p4_consumption_allowed": False,
        },
        "limitations": [
            "No certified macro release-vintage evidence was supplied.",
            "Unsupported price, benchmark, session, and exit evidence remains unavailable.",
            "No inferred delisting return is present in the observed-only population.",
            "This review candidate is not consumed by the pinned P3/P4 route.",
        ],
    }
    manifest_path = artifact_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return {
        "manifest": manifest_path,
        "manifest_sha256": sha256_file(manifest_path),
        "validation": validation,
        "summary": summary,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--step2-root", type=Path, required=True)
    parser.add_argument("--market-root", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--refresh-id", required=True)
    args = parser.parse_args()
    result = build(
        step2_root=args.step2_root,
        market_root=args.market_root,
        artifact_root=args.artifact_root,
        refresh_id=args.refresh_id,
    )
    print(
        json.dumps(
            {
                "manifest": str(result["manifest"]),
                "manifest_sha256": result["manifest_sha256"],
                "promotion_status": "not_promoted",
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
