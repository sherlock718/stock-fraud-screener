"""Compare a refreshed P2 review candidate with the pinned P2 baseline."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PINNED_BASELINE_ROOT = ROOT / "artifacts/canonical/corrected_us_annual"
PINNED_BASELINE_STEP2_ROOT = (
    ROOT / "artifacts/pit_validation/corrected_step2"
)
PINNED_BASELINE_MARKET_ROOT = (
    ROOT / "artifacts/pit_validation/contract_aligned_label_inputs"
)
PINNED_BASELINE_MANIFEST_SHA256 = (
    "40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3"
)
PRIMARY_DATASET = "outputs/observed_only/features_taxonomy.parquet"
IDENTITY_COLUMNS = [
    "stable_row_id",
    "entity_id",
    "cik",
    "ticker",
    "fiscal_year",
    "period_type",
    "availability_timestamp",
]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _record(base: Path, path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(base).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _require_manifest(
    root: Path,
    *,
    expected_hash: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    path = root / "manifest.json"
    if not path.is_file():
        raise RuntimeError(f"manifest is missing: {path}")
    actual = sha256_file(path)
    if expected_hash and actual != expected_hash:
        raise RuntimeError(
            f"pinned baseline manifest mismatch: expected={expected_hash} "
            f"actual={actual}"
        )
    return path, json.loads(path.read_text())


def _identity_comparison(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    baseline_identity = baseline[IDENTITY_COLUMNS].copy()
    candidate_identity = candidate[IDENTITY_COLUMNS].copy()
    if baseline_identity["stable_row_id"].duplicated().any():
        raise RuntimeError("baseline stable row IDs are not unique")
    if candidate_identity["stable_row_id"].duplicated().any():
        raise RuntimeError("candidate stable row IDs are not unique")
    baseline_ids = set(baseline_identity["stable_row_id"])
    candidate_ids = set(candidate_identity["stable_row_id"])
    rows = pd.concat(
        [
            baseline_identity.assign(baseline_present=True),
            candidate_identity.assign(candidate_present=True),
        ],
        ignore_index=True,
    )
    rows["baseline_present"] = rows["baseline_present"].eq(True)
    rows["candidate_present"] = rows["candidate_present"].eq(True)
    flags = (
        rows.groupby("stable_row_id", as_index=False)
        .agg(
            baseline_present=("baseline_present", "max"),
            candidate_present=("candidate_present", "max"),
        )
    )
    identity = (
        pd.concat(
            [
                baseline_identity.assign(source="baseline"),
                candidate_identity.assign(source="candidate"),
            ],
            ignore_index=True,
        )
        .merge(flags, on="stable_row_id", how="left")
        .drop_duplicates(["stable_row_id", "source"])
        .sort_values(["stable_row_id", "source"])
        .reset_index(drop=True)
    )
    business_keys = ["entity_id", "fiscal_year", "period_type"]
    baseline_keys = set(map(tuple, baseline_identity[business_keys].to_numpy()))
    candidate_keys = set(
        map(tuple, candidate_identity[business_keys].to_numpy())
    )
    summary = {
        "baseline_rows": len(baseline),
        "candidate_rows": len(candidate),
        "common_stable_row_ids": len(baseline_ids & candidate_ids),
        "baseline_only_stable_row_ids": len(baseline_ids - candidate_ids),
        "candidate_only_stable_row_ids": len(candidate_ids - baseline_ids),
        "common_entity_period_keys": len(baseline_keys & candidate_keys),
        "baseline_only_entity_period_keys": len(
            baseline_keys - candidate_keys
        ),
        "candidate_only_entity_period_keys": len(
            candidate_keys - baseline_keys
        ),
    }
    return identity, summary


def _schema_comparison(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
) -> dict[str, Any]:
    baseline_types = {
        column: str(dtype) for column, dtype in baseline.dtypes.items()
    }
    candidate_types = {
        column: str(dtype) for column, dtype in candidate.dtypes.items()
    }
    common = sorted(set(baseline_types) & set(candidate_types))
    return {
        "baseline_columns": len(baseline_types),
        "candidate_columns": len(candidate_types),
        "common_columns": len(common),
        "baseline_only_columns": sorted(
            set(baseline_types) - set(candidate_types)
        ),
        "candidate_only_columns": sorted(
            set(candidate_types) - set(baseline_types)
        ),
        "dtype_changes": {
            column: {
                "baseline": baseline_types[column],
                "candidate": candidate_types[column],
            }
            for column in common
            if baseline_types[column] != candidate_types[column]
        },
    }


def _coverage_comparison(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    for column in sorted(set(baseline.columns) | set(candidate.columns)):
        baseline_count = (
            int(baseline[column].notna().sum())
            if column in baseline
            else 0
        )
        candidate_count = (
            int(candidate[column].notna().sum())
            if column in candidate
            else 0
        )
        baseline_rate = baseline_count / len(baseline) if len(baseline) else 0
        candidate_rate = (
            candidate_count / len(candidate) if len(candidate) else 0
        )
        rows.append(
            {
                "column": column,
                "baseline_present": column in baseline,
                "candidate_present": column in candidate,
                "baseline_non_null": baseline_count,
                "candidate_non_null": candidate_count,
                "non_null_count_delta": candidate_count - baseline_count,
                "baseline_missing_rate": 1 - baseline_rate,
                "candidate_missing_rate": 1 - candidate_rate,
                "missing_rate_delta": baseline_rate - candidate_rate,
            }
        )
    return pd.DataFrame(rows)


def _label_support(root: Path, version: str) -> pd.DataFrame:
    path = root / "inputs/observed_only_row_horizon.parquet"
    frame = pd.read_parquet(path)
    return (
        frame.groupby(["horizon", "classification"], dropna=False)
        .size()
        .rename("rows")
        .reset_index()
        .assign(version=version)
    )


def _label_values(root: Path, version: str) -> pd.DataFrame:
    frame = pd.read_parquet(root / "inputs/observed_only_labels.parquet")
    frame["label_end_date"] = pd.to_datetime(
        frame["label_end_date"],
        utc=True,
        errors="coerce",
    )
    rows = []
    for horizon, group in frame.groupby("horizon", sort=True):
        rows.append(
            {
                "version": version,
                "horizon": horizon,
                "rows": len(group),
                "unique_entity_years": int(
                    group[
                        ["entity_id", "fiscal_year"]
                    ].drop_duplicates().shape[0]
                ),
                "label_end_min": group["label_end_date"].min(),
                "label_end_max": group["label_end_date"].max(),
                "stock_return_mean": group["stock_return"].mean(),
                "stock_return_median": group["stock_return"].median(),
                "benchmark_return_mean": group["benchmark_return"].mean(),
                "benchmark_return_median": group["benchmark_return"].median(),
                "relative_return_mean": group["relative_return"].mean(),
                "relative_return_median": group["relative_return"].median(),
                "outperformance_rate": (
                    pd.to_numeric(
                        group["outperformed_benchmark"],
                        errors="coerce",
                    ).mean()
                ),
            }
        )
    return pd.DataFrame(rows)


def _gate_counts(root: Path, version: str) -> pd.DataFrame:
    price = pd.read_parquet(
        root / "intermediate/decision_price_features.parquet"
    )
    price_counts = (
        price.groupby(
            ["price_feature_status", "price_feature_reason"],
            dropna=False,
        )
        .size()
        .rename("rows")
        .reset_index()
        .rename(
            columns={
                "price_feature_status": "classification",
                "price_feature_reason": "reason",
            }
        )
        .assign(gate="price", horizon=pd.NA, version=version)
    )
    labels = pd.read_parquet(
        root / "inputs/observed_only_row_horizon.parquet"
    )
    label_counts = (
        labels.groupby(["horizon", "classification", "reason"], dropna=False)
        .size()
        .rename("rows")
        .reset_index()
        .assign(gate="label", version=version)
    )
    return pd.concat(
        [price_counts, label_counts],
        ignore_index=True,
        sort=False,
    )


def _response_drift(
    baseline_path: Path,
    candidate_path: Path,
    *,
    key: str,
) -> dict[str, Any]:
    def load(path: Path) -> dict[str, dict[str, Any]]:
        latest = {}
        for line in path.read_text().splitlines():
            item = json.loads(line)
            latest[str(item[key])] = item
        return latest

    baseline = load(baseline_path)
    candidate = load(candidate_path)
    common = set(baseline) & set(candidate)
    changed = [
        value
        for value in common
        if baseline[value].get("response_sha256")
        != candidate[value].get("response_sha256")
        or baseline[value].get("status") != candidate[value].get("status")
    ]
    timestamps = [
        item.get("retrieved_at_utc")
        for item in candidate.values()
        if item.get("retrieved_at_utc")
    ]
    return {
        "baseline_requests": len(baseline),
        "candidate_requests": len(candidate),
        "common_keys": len(common),
        "baseline_only_keys": len(set(baseline) - set(candidate)),
        "candidate_only_keys": len(set(candidate) - set(baseline)),
        "changed_common_payload_or_status": len(changed),
        "changed_common_key_sample": sorted(changed)[:100],
        "candidate_collection_timestamp_min_utc": (
            min(timestamps) if timestamps else None
        ),
        "candidate_collection_timestamp_max_utc": (
            max(timestamps) if timestamps else None
        ),
    }


def _universe_drift(
    baseline_step2_root: Path,
    candidate_step2_root: Path,
) -> dict[str, Any]:
    baseline = pd.read_parquet(
        baseline_step2_root / "inputs/tickers.parquet"
    )
    candidate = pd.read_parquet(
        candidate_step2_root / "inputs/tickers.parquet"
    )
    baseline_ciks = set(baseline["cik"].astype(str))
    candidate_ciks = set(candidate["cik"].astype(str))
    baseline_tickers = (
        baseline[["cik", "ticker"]]
        .astype(str)
        .set_index("cik")["ticker"]
        .to_dict()
    )
    candidate_tickers = (
        candidate[["cik", "ticker"]]
        .astype(str)
        .set_index("cik")["ticker"]
        .to_dict()
    )
    changed = [
        cik
        for cik in baseline_ciks & candidate_ciks
        if baseline_tickers[cik] != candidate_tickers[cik]
    ]
    return {
        "baseline_rows": len(baseline),
        "candidate_rows": len(candidate),
        "common_ciks": len(baseline_ciks & candidate_ciks),
        "baseline_only_ciks": len(baseline_ciks - candidate_ciks),
        "candidate_only_ciks": len(candidate_ciks - baseline_ciks),
        "changed_ticker_for_common_cik": len(changed),
        "changed_ticker_cik_sample": sorted(changed)[:100],
    }


def compare(
    *,
    candidate_root: Path,
    candidate_step2_root: Path,
    candidate_market_root: Path,
    output_root: Path,
    baseline_root: Path = PINNED_BASELINE_ROOT,
    baseline_step2_root: Path = PINNED_BASELINE_STEP2_ROOT,
    baseline_market_root: Path = PINNED_BASELINE_MARKET_ROOT,
) -> dict[str, Any]:
    candidate_root = candidate_root.resolve()
    output_root = output_root.resolve()
    if output_root.exists() and any(output_root.iterdir()):
        raise RuntimeError(f"comparison target is non-empty: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)
    baseline_manifest, _ = _require_manifest(
        baseline_root,
        expected_hash=PINNED_BASELINE_MANIFEST_SHA256,
    )
    candidate_manifest, candidate_document = _require_manifest(candidate_root)
    if (
        candidate_document.get("artifact_class")
        != "CANONICAL_US_ANNUAL_P2_REVIEW_CANDIDATE"
    ):
        raise RuntimeError("candidate is not a D1 P2 review artifact")
    baseline = pd.read_parquet(baseline_root / PRIMARY_DATASET)
    candidate = pd.read_parquet(candidate_root / PRIMARY_DATASET)

    identity, identity_summary = _identity_comparison(baseline, candidate)
    identity_path = output_root / "row_identity.parquet"
    identity.to_parquet(identity_path, index=False)
    schema = _schema_comparison(baseline, candidate)
    schema_path = output_root / "schema.json"
    schema_path.write_text(json.dumps(schema, indent=2) + "\n")
    coverage = _coverage_comparison(baseline, candidate)
    coverage_path = output_root / "coverage_missingness.parquet"
    coverage.to_parquet(coverage_path, index=False)
    labels = pd.concat(
        [
            _label_support(baseline_root, "baseline"),
            _label_support(candidate_root, "candidate"),
        ],
        ignore_index=True,
    )
    labels_path = output_root / "label_support.parquet"
    labels.to_parquet(labels_path, index=False)
    label_values = pd.concat(
        [
            _label_values(baseline_root, "baseline"),
            _label_values(candidate_root, "candidate"),
        ],
        ignore_index=True,
    )
    label_values_path = output_root / "label_values.parquet"
    label_values.to_parquet(label_values_path, index=False)
    gates = pd.concat(
        [
            _gate_counts(baseline_root, "baseline"),
            _gate_counts(candidate_root, "candidate"),
        ],
        ignore_index=True,
    )
    gates_path = output_root / "gates.parquet"
    gates.to_parquet(gates_path, index=False)
    source_drift = {
        "universe": _universe_drift(
            baseline_step2_root,
            candidate_step2_root,
        ),
        "sec_companyfacts": _response_drift(
            baseline_step2_root / "raw/response_manifest.jsonl",
            candidate_step2_root / "raw/response_manifest.jsonl",
            key="cik",
        ),
        "market_prices_and_benchmarks": _response_drift(
            baseline_market_root / "raw/response_manifest.jsonl",
            candidate_market_root / "raw/response_manifest.jsonl",
            key="symbol",
        ),
    }
    source_drift_path = output_root / "source_drift.json"
    source_drift_path.write_text(json.dumps(source_drift, indent=2) + "\n")

    summary = {
        "schema_version": 1,
        "comparison": "pinned_P2_vs_D1_review_candidate",
        "baseline": {
            "manifest": str(baseline_manifest),
            "manifest_sha256": sha256_file(baseline_manifest),
        },
        "candidate": {
            "manifest": str(candidate_manifest),
            "manifest_sha256": sha256_file(candidate_manifest),
        },
        "row_identity": identity_summary,
        "schema": {
            key: value
            for key, value in schema.items()
            if key
            in {
                "baseline_columns",
                "candidate_columns",
                "common_columns",
                "baseline_only_columns",
                "candidate_only_columns",
                "dtype_changes",
            }
        },
        "feature_coverage": {
            "columns_compared": len(coverage),
            "columns_with_missing_rate_change": int(
                coverage["missing_rate_delta"].abs().gt(0).sum()
            ),
        },
        "labels": {
            "support_records": len(labels),
            "value_summary_records": len(label_values),
            "all_horizons_present": sorted(
                labels["horizon"].dropna().unique().tolist()
            ),
        },
        "missingness": {
            "candidate_universally_missing_columns": coverage.loc[
                coverage["candidate_non_null"].eq(0),
                "column",
            ].tolist(),
        },
        "gates": {"records": len(gates)},
        "source_drift": source_drift,
        "promotion": {
            "status": "not_promoted",
            "automatic_promotion": False,
            "explicit_authorization_required": True,
        },
    }
    summary_path = output_root / "comparison.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    records = [
        _record(output_root, path, role)
        for path, role in (
            (identity_path, "row_identity_comparison"),
            (schema_path, "schema_comparison"),
            (coverage_path, "feature_coverage_and_missingness"),
            (labels_path, "label_support_comparison"),
            (label_values_path, "observed_label_value_comparison"),
            (gates_path, "gate_comparison"),
            (source_drift_path, "source_drift_comparison"),
            (summary_path, "comparison_summary"),
        )
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": "CANONICAL_P2_VERSION_COMPARISON",
        "baseline_manifest_sha256": PINNED_BASELINE_MANIFEST_SHA256,
        "candidate_manifest_sha256": sha256_file(candidate_manifest),
        "records": records,
        "promotion_status": "not_promoted",
    }
    manifest_path = output_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return {
        "manifest": manifest_path,
        "manifest_sha256": sha256_file(manifest_path),
        "summary": summary,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--candidate-step2-root", type=Path, required=True)
    parser.add_argument("--candidate-market-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    result = compare(
        candidate_root=args.candidate_root,
        candidate_step2_root=args.candidate_step2_root,
        candidate_market_root=args.candidate_market_root,
        output_root=args.output_root,
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
