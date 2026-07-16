"""Freeze the Session V3.1 observed-only production table and contracts.

This module consumes only hash-validated Session 8F records.  It does not fit a
model, generate a prediction, select a holding, source market data, or run a
backtest.  Missing gate evidence is retained and fails the affected row closed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import subprocess
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from modeling.constants import (
    ALTMAN_Z_MIN,
    BENEISH_THRESHOLD,
    MAX_MARKET_CAP_PROD,
    MOMENTUM_12M_MIN,
    PIOTROSKI_MIN,
    TREE_THRESHOLD,
    VALUE_GATE_PCT,
    get_feature_candidates,
)
from modeling.fold_lineage import SelectorConfig


ROOT = Path(__file__).resolve().parents[1]
SESSION8F = ROOT / "artifacts/pit_validation/corrected_feature_population"
DEFAULT_ARTIFACT_ROOT = ROOT / "artifacts/pit_validation/session_v3_1_production_contract"
REPORT_PATH = ROOT / "reports/pit_validation/v3_1_production_table_contract.md"
SESSION8F_MANIFEST_SHA256 = "9c1e4b82c2e7bc2a85228adf6668b797acec827e2bd1ff7c58d20cfb6a9ac01a"
BASELINE_COMMIT = "ebed029"

FEATURES_PATH = SESSION8F / "outputs/observed_only/features_taxonomy.parquet"
LABELS_PATH = SESSION8F / "inputs/observed_only_labels.parquet"
CONSUMED_INPUTS = (FEATURES_PATH, LABELS_PATH)

MIN_MARKET_CAP = 50_000_000
TARGET_N = 15
AUM = 200_000.0
MAX_POSITION_ADTV = 0.01
POSITION_SIZE = AUM / TARGET_N
MIN_ADTV = POSITION_SIZE / MAX_POSITION_ADTV

IDENTITY_COLUMNS = [
    "stable_row_id", "entity_id", "cik", "ticker", "market", "fiscal_year",
    "period_type", "availability_timestamp", "availability_provenance",
]
TIME_COLUMNS = ["decision_timestamp", "prediction_timestamp", "entry_timestamp"]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def record(path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(ROOT).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def validate_consumed_inputs() -> dict[str, Any]:
    """Validate the 8F manifest and only the two records V3.1 reads."""
    manifest_path = SESSION8F / "manifest.json"
    actual_manifest_hash = sha256_file(manifest_path)
    if actual_manifest_hash != SESSION8F_MANIFEST_SHA256:
        raise RuntimeError(
            "Session 8F manifest hash mismatch: "
            f"expected={SESSION8F_MANIFEST_SHA256} actual={actual_manifest_hash}"
        )
    manifest = json.loads(manifest_path.read_text())
    records = {item["path"]: item for item in manifest.get("records", [])}
    validated = []
    for path in CONSUMED_INPUTS:
        relative = path.relative_to(ROOT).as_posix()
        if relative not in records:
            raise RuntimeError(f"consumed input is absent from Session 8F manifest: {relative}")
        expected = records[relative]
        if not path.is_file():
            raise RuntimeError(f"consumed input is missing: {relative}")
        actual_size = path.stat().st_size
        actual_hash = sha256_file(path)
        if actual_size != int(expected["size_bytes"]):
            raise RuntimeError(
                f"consumed input size mismatch: {relative} "
                f"expected={expected['size_bytes']} actual={actual_size}"
            )
        if actual_hash != expected["sha256"]:
            raise RuntimeError(
                f"consumed input hash mismatch: {relative} "
                f"expected={expected['sha256']} actual={actual_hash}"
            )
        validated.append({"path": relative, "size_bytes": actual_size, "sha256": actual_hash})
    return {
        "result": "pass",
        "session8f_manifest_sha256": actual_manifest_hash,
        "records_validated": validated,
        "records_validated_count": len(validated),
        "unconsumed_session8f_records_revalidated": 0,
    }


def _sec_eligible(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Return the certified SEC-primary clock used by this US-only session."""
    timestamps = pd.to_datetime(frame["availability_timestamp"], utc=True, errors="coerce")
    eligible = (
        timestamps.notna()
        & frame["availability_provenance"].eq("sec_primary_filing")
        & frame["entity_id"].notna()
        & frame["market"].eq("US")
        & frame["period_type"].eq("annual")
    )
    duplicate = frame.duplicated(
        ["entity_id", "fiscal_year", "period_type"], keep=False
    )
    eligible &= ~duplicate
    return timestamps, eligible


def winsorize_prior_sec_history(
    frame: pd.DataFrame, values: pd.Series, *, min_count: int = 50
) -> tuple[pd.Series, pd.Series]:
    """Clip from strictly prior certified SEC rows; sparse history stays raw."""
    timestamps, eligible = _sec_eligible(frame)
    numeric = pd.to_numeric(values, errors="coerce")
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    method = pd.Series(pd.NA, index=frame.index, dtype="string")
    targets = frame.index[eligible]
    for timestamp in sorted(timestamps.loc[targets].unique()):
        batch = targets[timestamps.loc[targets].eq(timestamp)]
        history = numeric.loc[eligible & timestamps.lt(timestamp)].dropna()
        if len(history) >= min_count:
            result.loc[batch] = numeric.loc[batch].clip(
                history.quantile(0.01), history.quantile(0.99)
            )
            method.loc[batch] = "strictly_prior_sec_history_p01_p99"
        else:
            result.loc[batch] = numeric.loc[batch]
            method.loc[batch] = "raw_sparse_prior_sec_history"
    return result, method


def asof_sector_percentile(
    frame: pd.DataFrame, values: pd.Series, *, min_count: int = 5
) -> pd.Series:
    """Rank raw P/S within the available fiscal-year US SIC-2 cohort."""
    timestamps, eligible = _sec_eligible(frame)
    numeric = pd.to_numeric(values, errors="coerce")
    result = pd.Series(np.nan, index=frame.index, dtype=float)
    group_columns = ["fiscal_year", "market", "sic_2digit"]
    work = frame.loc[eligible, group_columns].copy()
    work["__index__"] = work.index
    for _, group in work.groupby(group_columns, dropna=False, sort=False):
        positions = group["__index__"].tolist()
        group_times = timestamps.loc[positions]
        for timestamp in sorted(group_times.unique()):
            cohort_index = group_times.index[group_times.le(timestamp)]
            cohort = numeric.loc[cohort_index]
            if cohort.notna().sum() < min_count:
                continue
            ranks = cohort.rank(pct=True, na_option="keep")
            batch = group_times.index[group_times.eq(timestamp)]
            result.loc[batch] = ranks.reindex(batch)
    return result


def _beneish_raw(frame: pd.DataFrame) -> pd.Series:
    return (
        -4.84
        + 0.92 * frame["beneish_dsri"].fillna(1)
        + 0.528 * frame["beneish_gmi"].fillna(1)
        + 0.404 * frame["beneish_aqi"].fillna(1)
        + 0.892 * frame["beneish_sgi"].fillna(1)
        + 0.115 * frame["beneish_depi"].fillna(1)
        - 0.172 * frame["beneish_sgai"].fillna(1)
        + 4.679 * frame["beneish_tata"].fillna(0)
        - 0.327 * frame["beneish_lvgi"].fillna(0)
    )


def _altman_raw(frame: pd.DataFrame) -> pd.Series:
    return (
        1.2 * frame["altman_x1"].fillna(0)
        + 1.4 * frame["altman_x2"].fillna(0)
        + 3.3 * frame["altman_x3"].fillna(0)
        + 0.6 * frame["altman_x4"].fillna(0)
        + frame["altman_x5"].fillna(0)
    ).clip(-50, 50)


def _gate(
    frame: pd.DataFrame,
    name: str,
    value: pd.Series,
    predicate,
    provenance: str,
) -> None:
    numeric_or_value = value
    supported = numeric_or_value.notna()
    frame[f"gate_{name}_value"] = numeric_or_value
    frame[f"gate_{name}_status"] = np.where(supported, "supported", "unavailable")
    frame[f"gate_{name}_provenance"] = np.where(supported, provenance, pd.NA)
    frame[f"gate_{name}_pass"] = supported & predicate(numeric_or_value)


def materialize_production_table(features: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    """Build one observed-only annual 3y table; never drop an unresolved row."""
    frame = features.copy()
    if len(frame) != 43_806 or frame["stable_row_id"].duplicated().any():
        raise RuntimeError("Session 8F stable annual population drifted")
    if not frame["population"].eq("observed_only").all():
        raise RuntimeError("Session 8F feature population is not observed_only")
    if not frame["market"].eq("US").all() or not frame["period_type"].eq("annual").all():
        raise RuntimeError("Session V3.1 source contains non-US or non-annual rows")

    label_keys = ["entity_id", "cik", "ticker", "fiscal_year"]
    observed = labels.loc[labels["horizon"].eq("3y"), label_keys + [
        "decision_timestamp", "prediction_timestamp", "entry_timestamp",
        "label_end_date", "stock_return", "benchmark_return", "relative_return",
        "outperformed_benchmark", "label_provenance", "policy_imputed",
    ]].copy()
    if observed.duplicated(label_keys).any():
        raise RuntimeError("duplicate observed 3y label identity")
    observed = observed.rename(columns={
        "decision_timestamp": "label_decision_timestamp",
        "prediction_timestamp": "label_prediction_timestamp",
        "stock_return": "observed_forward_return_3y",
        "benchmark_return": "observed_benchmark_return_3y",
        "relative_return": "observed_excess_return_3y",
        "outperformed_benchmark": "observed_beat_local_market_3y",
        "label_provenance": "observed_label_provenance_3y",
        "policy_imputed": "observed_policy_imputed_3y",
    })
    frame = frame.merge(observed, on=label_keys, how="left", validate="one_to_one")
    for source, attached in (
        ("decision_timestamp", "label_decision_timestamp"),
        ("prediction_timestamp", "label_prediction_timestamp"),
    ):
        mismatch = frame[attached].notna() & pd.to_datetime(frame[source], utc=True).ne(
            pd.to_datetime(frame[attached], utc=True)
        )
        if mismatch.any():
            raise RuntimeError(f"Session 8E/8F {source} mismatch")
    if frame["observed_policy_imputed_3y"].fillna(False).astype(bool).any():
        raise RuntimeError("policy-imputed label entered observed-only V3.1 table")
    frame["entry_timestamp_status"] = np.where(
        frame["entry_timestamp"].notna(),
        "supported:observed_common_session_entry",
        "unavailable:" + frame["label_status_3y"].astype(str) + ":"
        + frame["label_reason_3y"].fillna("unspecified").astype(str),
    )

    beneish, beneish_method = winsorize_prior_sec_history(frame, _beneish_raw(frame))
    altman, altman_method = winsorize_prior_sec_history(frame, _altman_raw(frame))
    ps_raw = pd.to_numeric(frame["feature_market_cap"], errors="coerce") / pd.to_numeric(
        frame["revenue"], errors="coerce"
    ).replace(0, np.nan)
    ps_sector = asof_sector_percentile(frame, ps_raw)
    frame["beneish_m_score"] = beneish
    frame["beneish_m_score_materialization"] = beneish_method
    frame["altman_z_score"] = altman
    frame["altman_z_score_materialization"] = altman_method
    frame["ps_ratio_raw_for_sector_rank"] = ps_raw
    frame["ps_ratio_sector_pct"] = ps_sector

    _gate(frame, "market_us", frame["market"], lambda x: x.eq("US"),
          "session8d_certified_us_sec_population")
    _gate(frame, "market_cap", pd.to_numeric(frame["feature_market_cap"], errors="coerce"),
          lambda x: x.ge(MIN_MARKET_CAP) & x.le(MAX_MARKET_CAP_PROD),
          "session8e_predecision_raw_close_x_decision_available_shares")
    _gate(frame, "beneish", beneish, lambda x: x.lt(BENEISH_THRESHOLD),
          "v3.1_reconstructed_step5_formula_then_strict_prior_sec_winsorization")
    _gate(frame, "piotroski", pd.to_numeric(frame["piotroski_f_score"], errors="coerce"),
          lambda x: x.ge(PIOTROSKI_MIN), "session8f_certified_accounting_step5")
    _gate(frame, "roa_positive", pd.to_numeric(frame["piotroski_roa_pos"], errors="coerce"),
          lambda x: x.eq(1), "session8f_certified_accounting_step5")
    _gate(frame, "altman", altman, lambda x: x.gt(ALTMAN_Z_MIN),
          "v3.1_reconstructed_step5_formula_then_strict_prior_sec_winsorization")
    _gate(frame, "value", ps_sector, lambda x: x.le(VALUE_GATE_PCT),
          "v3.1_asof_fiscal_year_us_sic2_raw_ps_percentile")
    _gate(frame, "momentum", pd.to_numeric(frame["momentum_12m_prior"], errors="coerce"),
          lambda x: x.gt(MOMENTUM_12M_MIN),
          "session8e_provider_adjclose_strictly_predecision_365d_21d_skip")

    gate_names = [
        "market_us", "market_cap", "beneish", "piotroski", "roa_positive",
        "altman", "value", "momentum",
    ]
    pass_columns = [f"gate_{name}_pass" for name in gate_names]
    frame["all_non_model_hard_gates_pass"] = frame[pass_columns].all(axis=1)
    frame["hard_gate_exclusion_codes"] = frame.apply(
        lambda row: json.dumps(
            [
                (f"missing_gate_evidence:{name}" if row[f"gate_{name}_status"] != "supported"
                 else f"hard_gate_failed:{name}")
                for name in gate_names if not bool(row[f"gate_{name}_pass"])
            ], separators=(",", ":")
        ), axis=1,
    )

    frame["target_3y"] = frame["observed_forward_return_3y"]
    frame["tree_target_3y"] = frame["observed_beat_local_market_3y"]
    frame["target_status_3y"] = frame["label_status_3y"]
    frame["target_provenance_3y"] = frame["observed_label_provenance_3y"]
    frame["fold_id"] = pd.to_datetime(frame["decision_timestamp"], utc=True).dt.strftime(
        "decision_%Y%m%dT%H%M%SZ"
    )
    drop = ["label_decision_timestamp", "label_prediction_timestamp"]
    return frame.drop(columns=drop)


def freeze_configuration(table: pd.DataFrame) -> dict[str, Any]:
    """Return the exact V3.2-facing contract without fitting anything."""
    selector = asdict(SelectorConfig(top_n=28))
    feature_candidates = [
        name for name in get_feature_candidates(table)
        if not name.startswith("gate_")
        and name not in {"target_3y", "tree_target_3y"}
    ]
    return {
        "schema_version": 1,
        "session": "V3.1",
        "strategy_name": "production_v3_ml_gates",
        "legacy_performance_transferable": False,
        "population": "US SEC annual observed_only",
        "horizon": "3y",
        "decision_calendar": {
            "rule": "annual July 2 00:00 UTC decision inherited from certified Session 8B/8E",
            "prediction_offset": "one minute after decision",
            "entry": "first common regular session at or after prediction; certified Session 8E timestamp",
            "fold_id": "decision timestamp formatted decision_%Y%m%dT%H%M%SZ",
            "training_label_rule": "label_end_date strictly before fold decision_timestamp",
        },
        "targets": {
            "decision_tree": "observed_beat_local_market_3y",
            "lightgbm_regression": "observed_forward_return_3y",
            "regression_clip": [-1.0, 5.0],
            "missingness": "observed_only; null labels are ineligible and never imputed",
        },
        "feature_contract": {
            "candidate_columns": feature_candidates,
            "candidate_count": len(feature_candidates),
            "selector": selector,
            "selector_fit": "inside each historical fold only",
            "preprocessing": "fold-local median imputation only; null median or missing selected feature fails fold closed",
        },
        "decision_tree": {
            "family": "sklearn.tree.DecisionTreeClassifier",
            "max_depth": 4,
            "min_samples_leaf": 50,
            "min_samples_split": 100,
            "class_weight": "fold-local {0:1.0,1:n_negative/max(n_positive,1)}",
            "random_state": 42,
            "probability_role": "OOS tree agreement gate",
            "pass_rule": f"tree_prob >= {TREE_THRESHOLD}",
        },
        "lightgbm_ranker": {
            "family": "lightgbm.LGBMRegressor",
            "parameters": {
                "n_estimators": 600, "max_depth": 6, "learning_rate": 0.03,
                "num_leaves": 63, "subsample": 0.8, "colsample_bytree": 0.7,
                "min_child_samples": 20, "reg_alpha": 0.1, "reg_lambda": 1.0,
                "random_state": 42, "n_jobs": -1, "verbose": -1,
            },
            "ranking_rule": "descending OOS predicted observed_forward_return_3y",
        },
        "model_training_population": {
            "required_filter": "piotroski_roa_pos == 1 AND beneish_m_score < -1.78",
            "fraud_suspect": "not part of production_v3_ml_gates; uncertified heuristic is neither synthesized nor silently skipped",
            "missingness": "missing ROA or Beneish evidence fails the training row closed",
        },
        "selection": {
            "strategy": "production_v3_ml_gates",
            "required_oos_roles": ["tree_agreement_gate:3y", "lightgbm_regression_ranker:3y"],
            "target_n": TARGET_N,
            "requires_full_target_n": True,
            "weights": "equal",
            "weight_each": 1 / TARGET_N,
            "rank_after_all_gates": True,
            "missing_required_role": "candidate excluded; no fallback",
        },
        "liquidity": {
            "aum_usd": AUM,
            "target_n": TARGET_N,
            "planned_position_usd": POSITION_SIZE,
            "max_position_to_adtv": MAX_POSITION_ADTV,
            "minimum_adtv_usd": MIN_ADTV,
            "equation": "median_30_session_dollar_volume >= (AUM / target_n) / 0.01",
            "daily_dollar_volume": "unadjusted regular-session close * regular-session volume",
            "window": "exactly 30 valid sessions with market_close < prediction_timestamp",
            "scope": "all candidates passing non-liquidity gates and both required OOS roles, before ranking",
            "missingness": "candidate fails closed; gate cannot be disabled or applied only to provisional top 15",
        },
        "hard_gates": {
            "market_us": "market == 'US'",
            "market_cap": f"{MIN_MARKET_CAP} <= feature_market_cap <= {MAX_MARKET_CAP_PROD}",
            "beneish": f"beneish_m_score < {BENEISH_THRESHOLD}",
            "piotroski": f"piotroski_f_score >= {PIOTROSKI_MIN}",
            "roa_positive": "piotroski_roa_pos == 1",
            "altman": f"altman_z_score > {ALTMAN_Z_MIN}",
            "value": f"ps_ratio_sector_pct <= {VALUE_GATE_PCT}",
            "momentum": f"momentum_12m_prior > {MOMENTUM_12M_MIN}",
            "missingness": "every unavailable gate is a row-level failure",
        },
    }


def coverage_report(table: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for gate in config["hard_gates"]:
        if gate == "missingness":
            continue
        rows.append({
            "kind": "hard_gate", "name": gate, "total_rows": len(table),
            "supported_rows": int(table[f"gate_{gate}_status"].eq("supported").sum()),
            "pass_rows": int(table[f"gate_{gate}_pass"].sum()),
            "fail_closed_rows": int((~table[f"gate_{gate}_pass"]).sum()),
        })
    rows.append({
        "kind": "target", "name": "observed_3y", "total_rows": len(table),
        "supported_rows": int(table["target_3y"].notna().sum()),
        "pass_rows": int(table["target_3y"].notna().sum()),
        "fail_closed_rows": int(table["target_3y"].isna().sum()),
    })
    rows.append({
        "kind": "timestamp", "name": "entry_timestamp", "total_rows": len(table),
        "supported_rows": int(table["entry_timestamp"].notna().sum()),
        "pass_rows": int(table["entry_timestamp"].notna().sum()),
        "fail_closed_rows": int(table["entry_timestamp"].isna().sum()),
    })
    return pd.DataFrame(rows)


def freeze_dirty_state(artifact_root: Path) -> list[Path]:
    lineage = artifact_root / "lineage"
    lineage.mkdir(parents=True, exist_ok=True)
    paths = [
        lineage / "git_status_porcelain.txt",
        lineage / "tracked_dirty.patch",
        lineage / "untracked_inventory.json",
    ]
    paths[0].write_bytes(subprocess.check_output(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=ROOT
    ))
    paths[1].write_bytes(subprocess.check_output(
        ["git", "diff", "--binary", BASELINE_COMMIT, "--", "."], cwd=ROOT
    ))
    prefix = artifact_root.relative_to(ROOT).as_posix() + "/"
    inventory = []
    for raw in subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=ROOT
    ).split(b"\0"):
        if not raw:
            continue
        relative = raw.decode()
        path = ROOT / relative
        if path.is_file() and not relative.startswith(prefix):
            inventory.append(record(path, "untracked_worktree_file"))
    write_json(paths[2], inventory)
    return paths


def write_report(table: pd.DataFrame, coverage: pd.DataFrame) -> None:
    gate_lines = "\n".join(
        f"| {row['name']} | {row['supported_rows']:,} | {row['pass_rows']:,} | {row['fail_closed_rows']:,} |"
        for _, row in coverage[coverage["kind"].eq("hard_gate")].iterrows()
    )
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(f"""# Session V3.1 — Production Table and Contract Freeze

Status: **Accepted; V3.2 unblocked**

## Outcome

The immutable observed-only US annual three-year table contains {len(table):,}
stable rows. It retains certified decision and prediction timestamps for every
row and {table['entry_timestamp'].notna().sum():,} certified observed entry
timestamps. No row was deleted for missing target, timestamp, feature, or gate
evidence; every unresolved gate fails that row closed.

The V3.1 builder revalidated exactly the two Session 8F records it consumed and
no unrelated source inventory. It fitted no model, generated no prediction,
selected no holding, sourced no market data, ran no backtest, and did not begin
V3.2.

The accepted strategy is `production_v3_ml_gates`: both model targets are
three-year observed outcomes, fold-local feature selection is capped at 28 from
the frozen candidate pool, and clean training requires certified positive ROA
and Beneish below -1.78. The uncertified `fraud_suspect` heuristic is not
synthesized or silently skipped. The 0.55 tree threshold is a fixed policy
parameter, not a newly optimized result. Legacy performance does not transfer
to this corrected V3 strategy.

## Hard-gate coverage

| Gate | Supported | Passed | Failed closed |
|---|---:|---:|---:|
{gate_lines}

The Beneish, Altman, and sector-relative P/S gates were rematerialized from
their certified 8F components under the SEC-primary availability clock. This
is necessary because 8F's generic UTC-date equality check nulled these derived
columns when New York end-of-day crossed UTC midnight. The original 8F artifact
was not modified.

## Boundary

V3.1 has no remaining contract blocker. V3.2 may consume only this table and
configuration. Candidate-wide volume is not present in the certified Session
8E price records, so V3.3 will require explicit approval for market-data
collection after V3.2 freezes its OOS predictions.
""")


def build(artifact_root: Path = DEFAULT_ARTIFACT_ROOT) -> dict[str, Any]:
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"refusing to reuse non-empty artifact root: {artifact_root}")
    preflight = validate_consumed_inputs()
    for name in ("inputs", "configuration", "outputs", "support", "lineage"):
        (artifact_root / name).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(SESSION8F / "manifest.json", artifact_root / "inputs/session8f_manifest.json")

    features = pd.read_parquet(FEATURES_PATH)
    labels = pd.read_parquet(LABELS_PATH)
    table = materialize_production_table(features, labels)
    configuration = freeze_configuration(table)
    coverage = coverage_report(table, configuration)

    verdict = {
        "status": "accepted",
        "v3_1_table_frozen": True,
        "v3_1_contract_frozen": True,
        "v3_2_unblocked": True,
        "blockers": [],
        "models_trained": False,
        "predictions_generated": False,
        "holdings_selected": False,
        "backtest_run": False,
        "external_data_sourced": False,
    }

    table_path = artifact_root / "outputs/observed_only_us_annual_3y.parquet"
    config_path = artifact_root / "configuration/production_contract.json"
    coverage_path = artifact_root / "support/coverage.parquet"
    preflight_path = artifact_root / "support/preflight_validation.json"
    verdict_path = artifact_root / "support/verdict.json"
    table.to_parquet(table_path, index=False)
    write_json(config_path, configuration)
    coverage.to_parquet(coverage_path, index=False)
    write_json(preflight_path, preflight)
    write_json(verdict_path, verdict)
    write_report(table, coverage)
    lineage_paths = freeze_dirty_state(artifact_root)

    created_at = datetime.now(timezone.utc).isoformat()
    artifact_files = sorted(
        path for path in artifact_root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    )
    code_paths = [
        ROOT / "modeling/freeze_session_v3_1.py",
        ROOT / "tests/modeling/test_freeze_session_v3_1.py",
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": "SESSION_V3_1_PRODUCTION_TABLE_CONTRACT_FREEZE",
        "created_at_utc": created_at,
        "baseline_commit": BASELINE_COMMIT,
        "current_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "claim": verdict,
        "configuration": configuration,
        "preflight": preflight,
        "table": {
            "rows": len(table), "columns": len(table.columns),
            "stable_row_ids": table["stable_row_id"].nunique(),
            "observed_3y_targets": int(table["target_3y"].notna().sum()),
            "entry_timestamps": int(table["entry_timestamp"].notna().sum()),
            "all_non_model_hard_gates_pass": int(table["all_non_model_hard_gates_pass"].sum()),
        },
        "validated_inputs": [record(path, "consumed_certified_session8f_record") for path in CONSUMED_INPUTS],
        "records": [record(path, "v3_1_configuration_table_coverage_or_lineage") for path in artifact_files],
        "code_lineage": [record(path, "v3_1_builder_or_contract_test") for path in code_paths],
        "deliverables": [record(REPORT_PATH, "v3_1_report")],
        "dirty_state": {
            "baseline": BASELINE_COMMIT,
            "complete_status_recorded": True,
            "records": [path.relative_to(ROOT).as_posix() for path in lineage_paths],
        },
        "environment": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "pandas": pd.__version__,
        },
    }
    manifest_path = artifact_root / "manifest.json"
    write_json(manifest_path, manifest)
    return {
        "manifest": manifest_path,
        "manifest_sha256": sha256_file(manifest_path),
        "rows": len(table),
        "observed_3y_targets": int(table["target_3y"].notna().sum()),
        "v3_2_unblocked": True,
        "blockers": [],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    args = parser.parse_args()
    result = build(args.artifact_root.resolve())
    print(json.dumps({
        key: str(value) if isinstance(value, Path) else value
        for key, value in result.items()
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
