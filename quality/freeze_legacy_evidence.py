"""Create the one-time LEGACY_SAVED evidence snapshot for PIT Session 7."""
from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / "artifacts" / "pit_validation" / "legacy_saved"
PRE_STATE = Path("/tmp/session7_legacy_state")
BASELINE = "3f706e3e10d2b354c6e8b9407760fa2074749c0a"
CHUNK = 1024 * 1024


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _copy(source: Path, relative: str) -> tuple[int, str]:
    target = DEST / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    with source.open("rb") as src, target.open("xb") as dst:
        while chunk := src.read(CHUNK):
            dst.write(chunk)
            digest.update(chunk)
            size += len(chunk)
    shutil.copystat(source, target)
    return size, digest.hexdigest()


def _present(
    artifact_id: str,
    category: str,
    source: str,
    relative: str,
    *,
    role: str = "legacy_evidence",
    legacy_input: bool = False,
    note: str = "",
) -> dict:
    size, digest = _copy(ROOT / source, relative)
    return {
        "id": artifact_id,
        "category": category,
        "status": "present",
        "role": role,
        "legacy_input": legacy_input,
        "source_path": source,
        "snapshot_path": relative,
        "size_bytes": size,
        "sha256": digest,
        "note": note,
    }


def _missing(artifact_id: str, category: str, source: str, note: str) -> dict:
    return {
        "id": artifact_id,
        "category": category,
        "status": "missing",
        "role": "required_legacy_evidence",
        "legacy_input": False,
        "source_path": source,
        "snapshot_path": None,
        "size_bytes": None,
        "sha256": None,
        "note": note,
    }


def _version(name: str) -> str:
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return "missing"


def main() -> int:
    if DEST.exists():
        raise SystemExit(f"refusing to overwrite frozen snapshot: {DEST}")
    for required in ("status.txt", "tracked.patch", "untracked.patch"):
        if not (PRE_STATE / required).is_file():
            raise SystemExit(f"missing pre-edit evidence: {PRE_STATE / required}")
    DEST.mkdir(parents=True)

    artifacts = [
        _present("clean_dataset", "dataset", "data/historical_dataset_clean.parquet", "dataset/historical_dataset_clean.parquet", legacy_input=True),
        _present("monthly_price_cache", "price", "data/monthly_prices.parquet", "dataset/monthly_prices.parquet", legacy_input=True,
                 note="Coverage cache only; adjusted-price provenance is absent."),
        _present("classifier_model_3y", "model", "models/model_3y.joblib", "models/model_3y.joblib"),
        _present("model_feature_metadata", "metadata", "models/model_meta.json", "models/model_meta.json"),
        _present("regression_model_3y", "model", "models/model_3y_regression.joblib", "models/model_3y_regression.joblib"),
        _present("regression_model_3y_metadata", "metadata", "models/model_3y_regression_meta.json", "models/model_3y_regression_meta.json"),
        _present("research_tree_model", "model", "models/research_tree_snapshot.joblib", "models/research_tree_snapshot.joblib"),
        _present("decision_tree_rules", "metadata", "models/decision_tree_rules.json", "models/decision_tree_rules.json"),
        _present("feature_sets_3y", "metadata", "models/feature_sets_3y.json", "models/feature_sets_3y.json"),
        _present("saved_backtest_result", "report", "data/backtest_results.json", "backtest/backtest_results.json",
                 note="Saved aggregate claim; holdings, weights, folds, and score sources are absent."),
        _present("saved_strategy_configuration", "configuration", "data/backtest_results.json", "backtest/saved_strategy_configuration.json",
                 note="Configuration is embedded in the saved result; this duplicate freezes that exact file."),
        _present("spy_annual_returns", "benchmark", "data/spy_returns.csv", "backtest/spy_returns.csv"),
        _present("acwi_ex_us_annual_returns", "benchmark", "data/acwi_exus_returns.csv", "backtest/acwi_exus_returns.csv",
                 note="Contextual benchmark file; not identified as the saved US result benchmark input."),
        _present("saved_backtest_tearsheet", "report", "reports/backtest_tearsheet_summary.md", "backtest/backtest_tearsheet_summary.md",
                 note="Context only; its stated top-20 run differs from the saved top-15 claim."),
        _present("session_5_path_audit", "report", "reports/pit_validation/05_backtest_path_audit.md", "backtest/05_backtest_path_audit.md"),
        _present("session_6b_nav_implementation", "configuration", "backtest/monthly_nav.py", "backtest/corrected_code/monthly_nav.py",
                 role="corrected_code_evidence", note="Session 6B NAV/event schema and return policies; not a legacy input."),
        _present("session_6b_engine_configuration", "configuration", "backtest/engine.py", "backtest/corrected_code/engine.py",
                 role="corrected_code_evidence", note="Corrected return-policy wiring; not a legacy input."),
        _present("session_6b_report", "report", "reports/pit_validation/06b_monthly_nav_correction.md", "backtest/corrected_code/06b_monthly_nav_correction.md",
                 role="corrected_code_evidence", note="Correction report; not a legacy input."),
        _missing("classifier_model_1y", "model", "models/model_1y.joblib", "Expected active saved 1-year model is missing; archived candidates are not accepted replacements."),
        _missing("classifier_model_5y", "model", "models/model_5y.joblib", "Expected active saved 5-year model is missing; archived candidates are not accepted replacements."),
        _missing("row_level_prediction_lineage", "prediction", "predictions/legacy_saved.*", "No standalone predictions, holdings, fold IDs, score sources, or compatible row-level lineage sidecar exists."),
        _missing("canonical_benchmark_nav", "benchmark", "data/benchmark_monthly_nav.parquet", "Annual benchmark returns are not a canonical monthly benchmark NAV."),
        _missing("monthly_risk_free_returns", "benchmark", "data/monthly_risk_free_returns.parquet", "No frozen time-aligned monthly risk-free return series exists."),
        _missing("adjusted_price_provenance", "price", "data/monthly_prices_provenance.json", "No vintage/source/dividend/split provenance accompanies adjusted closes."),
        _missing("corporate_action_evidence", "event", "data/corporate_actions.parquet", "No dated event evidence and terms exist for selected securities."),
        _missing("security_mapping_evidence", "event", "data/security_mapping.parquet", "No point-in-time ticker/security mapping evidence exists."),
        _missing("saved_holdings_and_weights", "report", "data/backtest_holdings.parquet", "The saved aggregate result does not serialize selected row identities or weights."),
        _missing("saved_fold_and_score_sources", "prediction", "data/backtest_prediction_lineage.parquet", "Fold identity, training cutoff, feature lineage, fallback choice, and score source are absent."),
    ]

    archive_files = sorted((ROOT / "models" / "archive").glob("*"))
    for source in archive_files:
        if source.is_file() and source.suffix in {".joblib", ".json"}:
            artifacts.append(_present(
                f"archived_model_evidence_{source.stem}",
                "archived_model_or_metadata",
                str(source.relative_to(ROOT)),
                f"models/archive/{source.name}",
                note="Available archived evidence, but no lineage proves it produced the saved claim.",
            ))

    dirty_records = []
    for name in ("status.txt", "tracked.patch", "untracked.patch"):
        size, digest = _copy(PRE_STATE / name, f"backtest/dirty_state/{name}")
        dirty_records.append({"path": f"backtest/dirty_state/{name}", "size_bytes": size, "sha256": digest})

    sensitive = ROOT / ".codex" / "config.toml"
    sensitive_record = {
        "path": ".codex/config.toml",
        "recorded_in_status": True,
        "content_archived": False,
        "reason": "potential credential-bearing configuration",
        "size_bytes": sensitive.stat().st_size if sensitive.exists() else None,
        "sha256": _hash(sensitive) if sensitive.exists() else None,
    }
    status_lines = (PRE_STATE / "status.txt").read_text(encoding="utf-8").splitlines()

    manifest = {
        "schema_version": 1,
        "snapshot_type": "LEGACY_SAVED",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_commit": BASELINE,
        "snapshot_code_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "claim": {
            "reproducible": False,
            "snapshot_is_evidence_only": True,
            "statement": "The saved performance claim is not reproducible from this evidence set; LEGACY_SAVED preserves only the bytes and limitations that currently exist.",
        },
        "provenance": {
            "saved_artifacts_predate": "Sessions 1-6B",
            "dataset_embedded_score_summary_from_session_5": {
                "rows": 58190,
                "static_score_columns": ["ml_1y", "ml_3y", "ml_5y", "ml_pred_excess_3y"],
                "static_non_null_rows_each": 58190,
                "oof_non_null_rows_each": 51475,
                "oof_years": "FY2014-FY2025",
                "compatible_row_level_lineage_sidecar": False,
            },
            "recorded_handoff_price_hash": "d0e7c3ee05d89751ad86c3a2a763bbc322672448634e345b3a1a982c647c3def",
            "actual_frozen_price_hash": next(a["sha256"] for a in artifacts if a["id"] == "monthly_price_cache"),
            "price_hash_discrepancy": "The actual bytes match the Session 5 report (9c7ad...) rather than the later handoff verification summary (d0e7...).",
        },
        "dirty_state": {
            "relative_to_baseline": BASELINE,
            "complete_status_recorded": True,
            "entry_count": len(status_lines),
            "tracked_patch": dirty_records[1],
            "untracked_patch": dirty_records[2],
            "status": dirty_records[0],
            "sensitive_content_exclusions": [sensitive_record],
            "note": "All pre-Session-7 dirty paths are in status.txt. Patch content excludes only the potential credential-bearing config file; its path, size, and hash remain recorded.",
        },
        "environment": {
            "python": platform.python_version(),
            "dependencies": {name: _version(name) for name in (
                "numpy", "pandas", "scikit-learn", "lightgbm", "joblib", "pyarrow"
            )},
        },
        "limitations": [
            {"id": "legacy_artifacts_predate_sessions_1_through_6b", "detail": "Saved data/model/result artifacts predate the Sessions 1-6B corrected code."},
            {"id": "legacy_performance_not_reproducible", "detail": "LEGACY_SAVED is evidentiary only and does not reproduce the saved performance claim."},
            {"id": "missing_holdings_weights_folds_and_score_sources", "detail": "Holdings, weights, folds, model identity, training cutoffs, feature lineage, fallback choice, and score sources are absent."},
            {"id": "missing_canonical_benchmark_nav", "detail": "Only annual benchmark return files exist; canonical monthly benchmark NAV is missing."},
            {"id": "missing_time_aligned_monthly_risk_free_returns", "detail": "No frozen time-aligned monthly risk-free returns exist."},
            {"id": "missing_adjusted_price_provenance", "detail": "Adjusted-close source, vintage, split, and dividend provenance are missing."},
            {"id": "missing_corporate_action_and_security_mapping_evidence", "detail": "Dated event terms and point-in-time security mappings are missing."},
            {"id": "corrected_nav_schema_is_not_a_legacy_input", "detail": "Session 6B NAV/event schema and explicit return policies are corrected-code evidence only."},
            {"id": "active_1y_and_5y_models_missing", "detail": "Expected active models/model_1y.joblib and models/model_5y.joblib are missing; archive files are not lineage-compatible substitutes."},
            {"id": "monthly_price_hash_handoff_discrepancy", "detail": "Frozen local bytes hash to 9c7ad..., not the d0e7... value in the later handoff summary."},
        ],
        "artifacts": artifacts,
        "reserved_snapshots": {
            "old_reconstructed": {"reserved": True, "populated": False},
            "corrected_partial": {"reserved": False, "populated": False},
            "full_pit": {"reserved": False, "populated": False},
        },
    }
    (DEST / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({
        "manifest": str(DEST / "manifest.json"),
        "artifacts": len(artifacts),
        "present": sum(a["status"] == "present" for a in artifacts),
        "missing": sum(a["status"] == "missing" for a in artifacts),
        "dirty_entries": len(status_lines),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
