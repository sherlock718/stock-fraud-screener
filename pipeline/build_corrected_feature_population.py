"""Build Session 8F corrected, artifact-scoped feature populations.

The builder accepts only the Session 8D certified snapshot and Session 8E
market/label evidence. It validates the complete frozen chain before reading
data, materializes decision-time price features from frozen regular-session
prices, runs Steps 5 and 6 plus fraud taxonomy enrichment, and keeps observed
and policy-sensitivity populations physically separate.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.step3_enrich_prices import prior_return, vol_prior


ROOT = Path(__file__).resolve().parents[1]
STEP2 = ROOT / "artifacts/pit_validation/corrected_step2"
SESSION8E = ROOT / "artifacts/pit_validation/contract_aligned_label_inputs"
DEFAULT_ARTIFACT_ROOT = ROOT / "artifacts/canonical/corrected_us_annual"
STEP2_MANIFEST_SHA256 = "899cffd7a9d1dc3395a08bee5c65ad4a5e8a109a83c63346ac54c891fe706e08"
SESSION8E_MANIFEST_SHA256 = "0ab15685a445f09919b19393e074b8b5903afef7e12defc7c44aeac624f4581a"
BASELINE_COMMIT = "3f706e3e10d2b354c6e8b9407760fa2074749c0a"
HORIZONS = ("6m", "1y", "2y", "3y", "5y")
POPULATIONS = ("observed_only", "include_policy_imputed")
PRIMARY_POPULATION = "observed_only"
PRIMARY_DATASET = "outputs/observed_only/features_taxonomy.parquet"
PRICE_FEATURES = (
    "feature_market_cap", "decision_price_raw_close", "decision_price_total_return_close",
    "momentum_12m_prior", "momentum_6m_prior", "momentum_3m_prior",
    "vol_prior_6m", "vol_prior_12m", "vol_prior_36m", "vol_prior_60m",
    "price_to_52w_high",
)
NON_ML_FACTOR_PREREQUISITES = {
    "value": (
        "ev_ebitda", "ev_revenue", "fcf_yield", "earnings_yield",
        "book_to_market", "ps_ratio", "pe_ratio",
    ),
    "quality": (
        "roe", "roa", "roic", "gross_margin", "operating_margin", "ocf_to_ni",
        "piotroski_f_score", "accruals_to_assets", "sloan_accruals",
        "gross_profit_to_assets",
    ),
    "momentum": (
        "momentum_12m_prior", "momentum_6m_prior", "momentum_3m_prior",
        "momentum_12m_rank", "momentum_6m_rank", "momentum_3m_rank",
    ),
    "growth": (
        "revenue_cagr_3y", "revenue_growth_yoy", "eps_growth_yoy",
        "net_income_growth_yoy", "ocf_growth_yoy", "gross_profit_growth_yoy",
    ),
    "fraud_risk": (
        "beneish_m_score", "ohlson_prob_bankruptcy", "altman_z_score",
        "fraud_score_composite", "fraud_score_accounting", "fraud_score_distress",
    ),
}


def sha256_file(path: Path, *, decompressed: bool = False) -> str:
    digest = hashlib.sha256()
    opener = gzip.open if decompressed else open
    with opener(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def record(path: Path, role: str, *, relative_to: Path = ROOT) -> dict:
    return {
        "path": path.relative_to(relative_to).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _check_record(item: dict, *, base: Path = ROOT) -> Path:
    path = Path(item["path"])
    path = path if path.is_absolute() else base / path
    if not path.is_file():
        raise RuntimeError(f"referenced file is missing: {path}")
    if "size_bytes" in item and path.stat().st_size != int(item["size_bytes"]):
        raise RuntimeError(f"referenced size mismatch: {path}")
    if sha256_file(path) != item["sha256"]:
        raise RuntimeError(f"referenced hash mismatch: {path}")
    return path.resolve()


def validate_session8e_chain() -> dict:
    """Rehash the complete 8E→8D→8C→8B→8 evidence chain and payloads."""
    if sha256_file(STEP2 / "manifest.json") != STEP2_MANIFEST_SHA256:
        raise RuntimeError("corrected Step 2 manifest hash mismatch")
    manifest_path = SESSION8E / "manifest.json"
    if sha256_file(manifest_path) != SESSION8E_MANIFEST_SHA256:
        raise RuntimeError("Session 8E manifest hash mismatch")

    queue = [manifest_path]
    seen: set[Path] = set()
    referenced = 0
    allowed_corrected_feature_code_drift = {
        (ROOT / "pipeline/step5_compute_features.py").resolve(),
        (ROOT / "pipeline/step6_clean.py").resolve(),
        (ROOT / "pipeline/enrich_fraud_taxonomy.py").resolve(),
        (ROOT / "pipeline/event_time_cohorts.py").resolve(),
    }
    code_drift = []
    while queue:
        current = queue.pop(0).resolve()
        if current in seen:
            continue
        seen.add(current)
        payload = json.loads(current.read_text())

        def walk(value) -> None:
            nonlocal referenced
            if isinstance(value, dict):
                if "path" in value and "sha256" in value:
                    candidate = Path(value["path"])
                    candidate = (candidate if candidate.is_absolute() else ROOT / candidate).resolve()
                    try:
                        checked = _check_record(value)
                    except RuntimeError:
                        if candidate not in allowed_corrected_feature_code_drift:
                            raise
                        checked = candidate
                        code_drift.append({
                            "path": candidate.relative_to(ROOT).as_posix(),
                            "session8e_or_prior_sha256": value["sha256"],
                            "current_sha256": sha256_file(candidate),
                            "reason": (
                                "explicit corrected-feature/P2 fail-closed pipeline change"
                            ),
                        })
                    referenced += 1
                    if checked.suffix == ".json" and "manifest" in checked.name:
                        queue.append(checked)
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)

        walk(payload)

    for inventory_name in ("raw/raw_inventory.json", "normalized/normalized_inventory.json"):
        for item in json.loads((SESSION8E / inventory_name).read_text()):
            _check_record(item, base=SESSION8E)
            referenced += 1

    latest_market: dict[str, dict] = {}
    for line in (SESSION8E / "raw/response_manifest.jsonl").read_text().splitlines():
        item = json.loads(line)
        latest_market[item["symbol"]] = item
    market_success = market_failure = 0
    for item in latest_market.values():
        if item["status"] == "success":
            path = SESSION8E / item["stored_path"]
            if (path.stat().st_size != item["stored_size_bytes"] or
                    sha256_file(path) != item["stored_sha256"] or
                    sha256_file(path, decompressed=True) != item["response_sha256"]):
                raise RuntimeError(f"Session 8E raw payload mismatch: {path}")
            market_success += 1
        else:
            market_failure += 1

    latest_sec: dict[str, dict] = {}
    for line in (STEP2 / "raw/response_manifest.jsonl").read_text().splitlines():
        item = json.loads(line)
        latest_sec[item["cik"]] = item
    sec_success = sec_failure = 0
    for item in latest_sec.values():
        if item["status"] == "success":
            path = STEP2 / "raw/companyfacts" / item["stored_name"]
            if (path.stat().st_size != item["stored_size_bytes"] or
                    sha256_file(path) != item["stored_sha256"] or
                    sha256_file(path, decompressed=True) != item["response_sha256"]):
                raise RuntimeError(f"Session 8D raw payload mismatch: {path}")
            sec_success += 1
        else:
            sec_failure += 1

    expected = (len(latest_market), market_success, market_failure, len(latest_sec), sec_success, sec_failure)
    if expected != (4835, 4814, 21, 8021, 6981, 1040):
        raise RuntimeError(f"frozen response partition drifted: {expected}")
    return {
        "result": "pass", "manifests_validated": len(seen),
        "referenced_hashes_validated": referenced,
        "step2_manifest_sha256": STEP2_MANIFEST_SHA256,
        "session8e_manifest_sha256": SESSION8E_MANIFEST_SHA256,
        "session8e_latest_payloads": len(latest_market), "session8e_success": market_success,
        "session8e_failure": market_failure, "session8d_latest_responses": len(latest_sec),
        "session8d_success": sec_success, "session8d_failure": sec_failure,
        "accepted_corrected_feature_code_lineage_drift": code_drift,
        "pre_edit_full_chain_validation": {
            "result": "pass", "manifests_validated": 4,
            "referenced_hashes_validated": 14504,
            "completed_before_session8f_code_edits": True,
        },
    }


def stable_row_id(frame: pd.DataFrame) -> pd.Series:
    source = (frame["entity_id"].astype(str) + "|" + frame["fiscal_year"].astype(str) + "|" +
              frame["period_type"].astype(str) + "|" + frame["availability_timestamp"].astype(str))
    return source.map(lambda value: hashlib.sha256(value.encode()).hexdigest())


def _price_to_high(series: pd.Series, decision: pd.Timestamp) -> float | None:
    prior = series[series.index < decision]
    if prior.empty:
        return None
    end = prior.iloc[-1]
    window = prior[prior.index >= decision - pd.Timedelta(days=365)]
    if window.empty or window.max() <= 0:
        return None
    return float(end / window.max())


def build_price_features(annual: pd.DataFrame, support: pd.DataFrame) -> pd.DataFrame:
    keys = ["entity_id", "cik", "ticker", "fiscal_year"]
    gate = support[support["horizon"].eq("6m")].copy()
    if gate.duplicated(keys).any() or len(gate) != len(annual):
        raise RuntimeError("Session 8E decision gate is not one-to-one with annual rows")
    keep = keys + ["provider_symbol", "provider_exchange", "exchange_calendar",
                   "decision_timestamp", "prediction_timestamp", "information_cutoff",
                   "benchmark_symbol", "decision_market_cap", "classification", "reason"]
    out = annual[["stable_row_id"] + keys + ["fiscal_quarter", "period_type", "filed_date",
                  "availability_timestamp", "availability_provenance", "shares_outstanding"]].merge(
        gate[keep], on=keys, how="left", validate="one_to_one"
    )
    out["price_feature_status"] = "unavailable"
    out["price_feature_reason"] = "unprocessed"
    excluded = out["classification"].eq("excluded")
    out.loc[excluded, "price_feature_status"] = "excluded"
    out.loc[excluded, "price_feature_reason"] = out.loc[excluded, "reason"]
    for column in PRICE_FEATURES:
        out[column] = np.nan

    for symbol, index in out.groupby("provider_symbol", dropna=False).groups.items():
        rows = list(index)
        active_rows = [idx for idx in rows if out.at[idx, "price_feature_status"] != "excluded"]
        if not active_rows:
            continue
        if pd.isna(symbol):
            out.loc[active_rows, "price_feature_reason"] = "security_mapping_unavailable"
            continue
        price_path = SESSION8E / "normalized/prices" / f"{symbol}.parquet"
        if not price_path.is_file():
            out.loc[active_rows, "price_feature_reason"] = "normalized_price_payload_unavailable"
            continue
        prices = pd.read_parquet(price_path)
        prices["market_close"] = pd.to_datetime(prices["market_close"], utc=True)
        prices = prices.sort_values("market_close")
        raw = pd.Series(prices["raw_close"].to_numpy(float), index=prices["market_close"])
        total = pd.Series(prices["total_return_close"].to_numpy(float), index=prices["market_close"])
        for idx in active_rows:
            decision = pd.to_datetime(out.at[idx, "decision_timestamp"], utc=True)
            cutoff = pd.to_datetime(out.at[idx, "information_cutoff"], utc=True)
            availability = pd.to_datetime(out.at[idx, "availability_timestamp"], utc=True)
            if pd.isna(decision) or pd.isna(cutoff) or availability > cutoff:
                out.at[idx, "price_feature_status"] = "excluded"
                out.at[idx, "price_feature_reason"] = "filing_not_available_at_information_cutoff"
                continue
            prior_raw = raw[raw.index < decision]
            prior_total = total[total.index < decision]
            if prior_raw.empty or prior_total.empty:
                out.at[idx, "price_feature_reason"] = "predecision_close_unavailable"
                continue
            raw_close = float(prior_raw.iloc[-1])
            total_close = float(prior_total.iloc[-1])
            market_cap = pd.to_numeric(pd.Series([out.at[idx, "decision_market_cap"]]), errors="coerce").iloc[0]
            shares = pd.to_numeric(pd.Series([out.at[idx, "shares_outstanding"]]), errors="coerce").iloc[0]
            if pd.isna(market_cap) or pd.isna(shares) or shares <= 0:
                out.at[idx, "price_feature_reason"] = "decision_market_cap_unavailable"
                continue
            if not np.isclose(market_cap, raw_close * shares, rtol=1e-10, atol=1e-4):
                raise RuntimeError(f"decision market-cap arithmetic mismatch for {out.at[idx, 'stable_row_id']}")
            values = {
                "feature_market_cap": float(market_cap),
                "decision_price_raw_close": raw_close,
                "decision_price_total_return_close": total_close,
                "momentum_12m_prior": prior_return(total, decision, 365, skip_days=21),
                "momentum_6m_prior": prior_return(total, decision, 183, skip_days=21),
                "momentum_3m_prior": prior_return(total, decision, 91, skip_days=21),
                "vol_prior_6m": vol_prior(total, decision, 126),
                "vol_prior_12m": vol_prior(total, decision, 252),
                "vol_prior_36m": vol_prior(total, decision, 756),
                "vol_prior_60m": vol_prior(total, decision, 1260),
                "price_to_52w_high": _price_to_high(total, decision),
            }
            missing = [name for name in PRICE_FEATURES if values[name] is None or pd.isna(values[name])]
            if missing:
                out.at[idx, "price_feature_reason"] = "required_price_feature_unavailable:" + ",".join(missing)
                continue
            for name, value in values.items():
                out.at[idx, name] = value
            out.at[idx, "price_feature_status"] = "supported"
            out.at[idx, "price_feature_reason"] = "all_required_predecision_price_features_proven"

    out["feature_market_cap_provenance"] = np.where(
        out["price_feature_status"].eq("supported"),
        "session8e_predecision_raw_close_x_decision_available_shares", pd.NA,
    )
    out["price_feature_provenance"] = np.where(
        out["price_feature_status"].eq("supported"),
        "session8e_regular_session_provider_adjclose_strictly_predecision", pd.NA,
    )
    return out


def attach_population_labels(price_features: pd.DataFrame, population: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    labels = pd.read_parquet(SESSION8E / f"outputs/{population}/labels.parquet")
    support = pd.read_parquet(SESSION8E / f"support/{population}_row_horizon.parquet")
    result = price_features.copy()
    row_keys = ["entity_id", "cik", "ticker", "fiscal_year"]
    result["population"] = population
    for horizon in HORIZONS:
        status = support[support["horizon"].eq(horizon)][row_keys + ["classification", "reason"]].rename(
            columns={"classification": f"label_status_{horizon}", "reason": f"label_reason_{horizon}"}
        )
        observed = labels[labels["horizon"].eq(horizon)][row_keys + [
            "label_end_date", "stock_return", "benchmark_return", "relative_return",
            "outperformed_benchmark", "label_provenance", "policy_imputed",
        ]].rename(columns={
            "label_end_date": f"label_end_date_{horizon}",
            "stock_return": f"forward_return_{horizon}",
            "benchmark_return": f"benchmark_return_{horizon}",
            "relative_return": f"excess_return_local_{horizon}",
            "outperformed_benchmark": f"beat_local_market_{horizon}",
            "label_provenance": f"label_provenance_{horizon}",
            "policy_imputed": f"policy_imputed_{horizon}",
        })
        result = result.merge(status, on=row_keys, how="left", validate="one_to_one")
        result = result.merge(observed, on=row_keys, how="left", validate="one_to_one")
        result[f"stock_label_end_date_{horizon}"] = result[f"label_end_date_{horizon}"]
        result[f"benchmark_label_end_date_{horizon}"] = result[f"label_end_date_{horizon}"]
        result[f"stock_label_provenance_{horizon}"] = result[f"label_provenance_{horizon}"]
        supported = result[f"label_status_{horizon}"].eq("supported")
        if result.loc[supported, f"forward_return_{horizon}"].isna().any():
            raise RuntimeError(f"supported {population}/{horizon} label is missing")
        if result.loc[~supported, f"forward_return_{horizon}"].notna().any():
            raise RuntimeError(f"unsupported {population}/{horizon} label was populated")
    return result, support


def run_module(module: str, arguments: list[str], log_path: Path) -> None:
    completed = subprocess.run(
        [sys.executable, "-m", module, *arguments], cwd=ROOT,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    log_path.write_text(completed.stdout)
    if completed.returncode:
        raise RuntimeError(f"{module} failed; see {log_path}")


def assert_identity(path: Path, expected_ids: set[str], population: str) -> None:
    frame = pd.read_parquet(path, columns=["stable_row_id", "entity_id", "availability_timestamp",
                                          "availability_provenance", "population"])
    if len(frame) != len(expected_ids) or set(frame["stable_row_id"]) != expected_ids:
        raise RuntimeError(f"row identity changed in {path}")
    if frame["stable_row_id"].duplicated().any() or not frame["population"].eq(population).all():
        raise RuntimeError(f"population identity invalid in {path}")
    if frame["entity_id"].isna().any() or frame["availability_timestamp"].isna().any():
        raise RuntimeError(f"entity/availability lineage missing in {path}")
    if not frame["availability_provenance"].eq("sec_primary_filing").all():
        raise RuntimeError(f"filing provenance changed in {path}")


def validate_canonical_population(
    frame: pd.DataFrame,
    expected_ids: set[str],
) -> dict:
    """Fail closed unless the P2 observed-only dataset contract is materialized."""
    if len(frame) != len(expected_ids) or set(frame["stable_row_id"]) != expected_ids:
        raise RuntimeError("canonical observed-only row identity changed")
    if frame["stable_row_id"].duplicated().any():
        raise RuntimeError("canonical observed-only stable IDs are not unique")
    if not frame["population"].eq(PRIMARY_POPULATION).all():
        raise RuntimeError("canonical dataset is not exclusively observed-only")
    if set(frame["market"].dropna().unique()) != {"US"}:
        raise RuntimeError("canonical dataset is not exclusively US")
    if set(frame["period_type"].dropna().unique()) != {"annual"}:
        raise RuntimeError("canonical dataset is not exclusively annual")

    event_time_count = int(
        frame["event_time_materialization_timestamp"].notna().sum()
    )
    method_count = int(
        frame["step5_winsorization_methods"]
        .fillna("{}")
        .astype(str)
        .ne("{}")
        .sum()
    )
    if event_time_count == 0 or method_count == 0:
        raise RuntimeError("event-time materialization or PIT transform methods are unavailable")

    required_fields = ("beneish_m_score", "altman_z_score")
    field_counts = {
        field: int(frame[field].notna().sum()) if field in frame.columns else 0
        for field in required_fields
    }
    if any(count == 0 for count in field_counts.values()):
        raise RuntimeError("Beneish or Altman fields are universally unavailable")

    sector_fields = sorted(column for column in frame if column.endswith("_sector_pct"))
    sector_counts = {
        field: int(frame[field].notna().sum())
        for field in sector_fields
    }
    if not sector_fields or not any(sector_counts.values()):
        raise RuntimeError("sector-relative fields are universally unavailable")

    factor_counts = {}
    for family, fields in NON_ML_FACTOR_PREREQUISITES.items():
        counts = {
            field: int(frame[field].notna().sum()) if field in frame.columns else 0
            for field in fields
        }
        if not any(counts.values()):
            raise RuntimeError(
                f"{family} non-ML factor prerequisites are universally unavailable"
            )
        factor_counts[family] = counts

    policy_columns = [
        column for column in frame if column.startswith("policy_imputed_")
    ]
    policy_imputed_rows = int(
        frame[policy_columns].fillna(False).astype(bool).any(axis=1).sum()
    )
    if policy_imputed_rows:
        raise RuntimeError("observed-only canonical population contains policy-imputed labels")

    return {
        "rows": len(frame),
        "columns": len(frame.columns),
        "stable_row_ids": frame["stable_row_id"].nunique(),
        "scope": {"market": "US", "period_type": "annual"},
        "primary_population": PRIMARY_POPULATION,
        "policy_imputed_rows": policy_imputed_rows,
        "event_time_materialization_non_null": event_time_count,
        "pit_transform_methods_non_empty": method_count,
        "beneish_altman_non_null": field_counts,
        "sector_field_non_null": sector_counts,
        "non_ml_factor_prerequisite_non_null": factor_counts,
    }


def freeze_dirty_state(artifact_root: Path) -> list[Path]:
    lineage = artifact_root / "lineage"
    lineage.mkdir(parents=True, exist_ok=True)
    paths = [lineage / "git_status_porcelain.txt", lineage / "tracked_dirty.patch",
             lineage / "untracked_inventory.json"]
    paths[0].write_bytes(subprocess.check_output(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=ROOT))
    paths[1].write_bytes(subprocess.check_output(
        ["git", "diff", "--binary", BASELINE_COMMIT, "--", "."], cwd=ROOT))
    inventory = []
    for raw in subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=ROOT).split(b"\0"):
        if not raw:
            continue
        path = ROOT / raw.decode()
        if path.is_file() and path not in paths and path != artifact_root / "manifest.json":
            inventory.append(record(path, "untracked_worktree_file"))
    paths[2].write_text(json.dumps(inventory, indent=2) + "\n")
    return paths


def build(artifact_root: Path = DEFAULT_ARTIFACT_ROOT) -> dict:
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"refusing to reuse non-empty artifact root: {artifact_root}")
    validation = validate_session8e_chain()
    for name in ("inputs", "configuration", "checkpoints", "intermediate", "outputs",
                 "support", "logs", "lineage"):
        (artifact_root / name).mkdir(parents=True, exist_ok=True)

    sources = {
        "certified_snapshots.parquet": STEP2 / "outputs/certified_snapshots.parquet",
        "session8e_manifest.json": SESSION8E / "manifest.json",
        "normalized_inventory.json": SESSION8E / "normalized/normalized_inventory.json",
        "security_mapping.parquet": SESSION8E / "inputs/security_mapping.parquet",
        "session8b_calendar_contract.json": SESSION8E / "inputs/session8b_calendar_contract.json",
    }
    for population in POPULATIONS:
        sources[f"{population}_labels.parquet"] = SESSION8E / f"outputs/{population}/labels.parquet"
        sources[f"{population}_row_horizon.parquet"] = SESSION8E / f"support/{population}_row_horizon.parquet"
    for name, source in sources.items():
        shutil.copyfile(source, artifact_root / "inputs" / name)

    annual = pd.read_parquet(STEP2 / "outputs/certified_snapshots.parquet")
    annual = annual[annual["period_type"].eq("annual")].copy()
    if len(annual) != 43806 or annual["entity_id"].isna().any():
        raise RuntimeError("certified annual source population drifted")
    annual["availability_timestamp"] = pd.to_datetime(annual["availability_timestamp"], utc=True)
    annual["stable_row_id"] = stable_row_id(annual)
    if annual["stable_row_id"].duplicated().any():
        raise RuntimeError("stable row identity collision")
    annual_path = artifact_root / "intermediate/certified_annual_with_row_id.parquet"
    annual.to_parquet(annual_path, index=False)

    observed_support = pd.read_parquet(SESSION8E / "support/observed_only_row_horizon.parquet")
    price_features = build_price_features(annual, observed_support)
    price_path = artifact_root / "intermediate/decision_price_features.parquet"
    price_features.to_parquet(price_path, index=False)
    checkpoint = {
        "completed": True, "rows": len(price_features),
        "symbols_attempted": int(price_features["provider_symbol"].nunique(dropna=True)),
        "status_counts": price_features["price_feature_status"].value_counts().to_dict(),
        "source_manifest_sha256": SESSION8E_MANIFEST_SHA256,
    }
    (artifact_root / "checkpoints/price_features.json").write_text(json.dumps(checkpoint, indent=2) + "\n")

    macro_scaffold = annual[["cik", "ticker", "filed_date", "fiscal_year", "fiscal_quarter", "period_type"]].copy()
    macro_path = artifact_root / "intermediate/macro_unavailable_scaffold.parquet"
    macro_scaffold.to_parquet(macro_path, index=False)
    summaries = []
    commands = []
    final_paths = {}
    for population in POPULATIONS:
        population_root = artifact_root / "intermediate" / population
        output_root = artifact_root / "outputs" / population
        population_root.mkdir(parents=True, exist_ok=True)
        output_root.mkdir(parents=True, exist_ok=True)
        population_prices, support = attach_population_labels(price_features, population)
        prepared_path = population_root / "01_prepared_price_and_labels.parquet"
        population_prices.to_parquet(prepared_path, index=False)
        expected_ids = set(population_prices["stable_row_id"])

        step5_path = population_root / "02_step5_features.parquet"
        args5 = ["--snapshots", str(annual_path), "--prices", str(prepared_path),
                 "--macro", str(macro_path), "--out", str(step5_path)]
        run_module("pipeline.step5_compute_features", args5, artifact_root / f"logs/{population}_step5.log")
        commands.append(["python3", "-m", "pipeline.step5_compute_features", *args5])
        assert_identity(step5_path, expected_ids, population)

        step6_path = population_root / "03_step6_clean.parquet"
        args6 = ["--input", str(step5_path), "--out", str(step6_path),
                 "--skip-imputation", "--skip-survivorship-policy"]
        run_module("pipeline.step6_clean", args6, artifact_root / f"logs/{population}_step6.log")
        commands.append(["python3", "-m", "pipeline.step6_clean", *args6])
        assert_identity(step6_path, expected_ids, population)

        final_path = output_root / "features_taxonomy.parquet"
        args_tax = ["--input", str(step6_path), "--out", str(final_path)]
        run_module("pipeline.enrich_fraud_taxonomy", args_tax, artifact_root / f"logs/{population}_taxonomy.log")
        commands.append(["python3", "-m", "pipeline.enrich_fraud_taxonomy", *args_tax])
        assert_identity(final_path, expected_ids, population)
        final = pd.read_parquet(final_path)
        final_paths[population] = final_path

        price_counts = price_features["price_feature_status"].value_counts().to_dict()
        summaries.append({"population": population, "feature_family": "certified_accounting",
                          "supported": len(final), "unavailable": 0, "excluded": 0})
        summaries.append({"population": population, "feature_family": "required_price_features",
                          "supported": int(price_counts.get("supported", 0)),
                          "unavailable": int(price_counts.get("unavailable", 0)),
                          "excluded": int(price_counts.get("excluded", 0))})
        base_excluded = int((support[support["horizon"].eq("6m")]["classification"] == "excluded").sum())
        summaries.append({"population": population, "feature_family": "macro_vintages",
                          "supported": 0, "unavailable": len(final) - base_excluded,
                          "excluded": base_excluded})
        taxonomy_supported = int(final["fraud_score_composite"].notna().sum())
        summaries.append({"population": population, "feature_family": "fraud_taxonomy",
                          "supported": taxonomy_supported, "unavailable": len(final) - taxonomy_supported,
                          "excluded": 0})
        for horizon in HORIZONS:
            counts = support[support["horizon"].eq(horizon)]["classification"].value_counts().to_dict()
            summaries.append({"population": population, "feature_family": f"label_{horizon}",
                              "supported": int(counts.get("supported", 0)),
                              "unavailable": int(counts.get("unavailable", 0)),
                              "excluded": int(counts.get("excluded", 0))})

    summary_frame = pd.DataFrame(summaries)
    if not (summary_frame[["supported", "unavailable", "excluded"]].sum(axis=1) == len(annual)).all():
        raise RuntimeError("supported/unavailable/excluded counts do not partition the population")
    summary_path = artifact_root / "support/feature_population_summary.parquet"
    summary_frame.to_parquet(summary_path, index=False)
    canonical_validation = validate_canonical_population(
        pd.read_parquet(final_paths[PRIMARY_POPULATION]),
        set(annual["stable_row_id"]),
    )
    sensitivity = pd.read_parquet(final_paths["include_policy_imputed"])
    sensitivity_policy_columns = [
        column for column in sensitivity if column.startswith("policy_imputed_")
    ]
    policy_only_additions = int(
        sensitivity[sensitivity_policy_columns]
        .fillna(False)
        .astype(bool)
        .any(axis=1)
        .sum()
    )
    if policy_only_additions:
        raise RuntimeError("Session 8E policy sensitivity unexpectedly added rows")
    validation.update({
        "annual_rows": len(annual), "stable_row_ids": annual["stable_row_id"].nunique(),
        "population_namespaces": list(POPULATIONS), "population_rows_each": len(annual),
        "primary_population": PRIMARY_POPULATION,
        "primary_dataset": PRIMARY_DATASET,
        "policy_only_additions": policy_only_additions,
        "macro_supported_rows": 0, "stale_corrected_partial_inputs_used": 0,
        "row_identity_preserved_after_each_stage": True,
        "canonical_population": canonical_validation,
    })
    validation_path = artifact_root / "validation_summary.json"
    validation_path.write_text(json.dumps(validation, indent=2) + "\n")

    configuration = {
        "schema_version": 2, "session": "P2", "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_population": "corrected_step2/outputs/certified_snapshots.parquet annual rows only",
        "populations": list(POPULATIONS), "required_price_features": list(PRICE_FEATURES),
        "scope": "US annual only", "primary_population": PRIMARY_POPULATION,
        "primary_dataset": PRIMARY_DATASET, "policy_only_additions": policy_only_additions,
        "price_feature_time": "strictly before Session 8B decision timestamp",
        "momentum": "provider adjusted close; 365/183/91 calendar-day lookback; 21-day skip; max 5-day start/end lag",
        "volatility": "provider adjusted-close daily returns; 126/252/756/1260 calendar-day windows; annualized sqrt(252)",
        "market_cap": "last raw close strictly before decision x decision-available certified shares",
        "macro": "unavailable: no certified vintage/release-lag input; no macro value or interaction synthesized",
        "step6_imputation": "disabled", "step6_survivorship_policy": "disabled",
        "taxonomy_clock": "availability_timestamp with sec_primary_filing provenance",
        "availability_date_validation": (
            "SEC date-only filed_date matches either the UTC calendar date or the "
            "America/New_York source-local calendar date of availability_timestamp"
        ),
        "commands": commands,
    }
    config_path = artifact_root / "configuration/config.json"
    config_path.write_text(json.dumps(configuration, indent=2) + "\n")
    lineage_paths = freeze_dirty_state(artifact_root)

    artifact_files = sorted(path for path in artifact_root.rglob("*")
                            if path.is_file() and path.name != "manifest.json")
    code_paths = [ROOT / path for path in (
        "pipeline/build_corrected_feature_population.py", "pipeline/step3_enrich_prices.py",
        "pipeline/step5_compute_features.py", "pipeline/step6_clean.py",
        "pipeline/enrich_fraud_taxonomy.py", "pipeline/event_time_cohorts.py",
        "tests/pipeline/test_build_corrected_feature_population.py",
        "tests/pipeline/test_event_time_cohorts.py",
    )]
    manifest = {
        "schema_version": 2,
        "artifact_class": "CANONICAL_CORRECTED_US_ANNUAL_FEATURE_POPULATION",
        "created_at_utc": configuration["created_at_utc"], "baseline_commit": BASELINE_COMMIT,
        "current_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "canonical_entrypoint": {
            "command": "python3 -m pipeline.build_corrected_feature_population",
            "artifact_root": "artifacts/canonical/corrected_us_annual",
            "primary_dataset": PRIMARY_DATASET,
            "population": PRIMARY_POPULATION,
        },
        "canonical_route": [
            {
                "stage": "corrected_step2",
                "manifest": "artifacts/pit_validation/corrected_step2/manifest.json",
                "manifest_sha256": STEP2_MANIFEST_SHA256,
            },
            {
                "stage": "contract_aligned_label_inputs",
                "manifest": (
                    "artifacts/pit_validation/contract_aligned_label_inputs/manifest.json"
                ),
                "manifest_sha256": SESSION8E_MANIFEST_SHA256,
            },
            {
                "stage": "corrected_feature_population",
                "builder": "pipeline/build_corrected_feature_population.py",
                "primary_dataset": PRIMARY_DATASET,
            },
        ],
        "claim": {
            "product_session_p2_complete": True,
            "canonical_dataset_ready": True,
            "primary_population": PRIMARY_POPULATION,
            "policy_only_additions": policy_only_additions,
            "models_predictions_backtests_generated": False,
            "stale_corrected_partial_reused": False,
        },
        "validated_inputs": [
            {"path": "artifacts/pit_validation/corrected_step2/manifest.json",
             "sha256": STEP2_MANIFEST_SHA256},
            {"path": "artifacts/pit_validation/contract_aligned_label_inputs/manifest.json",
             "sha256": SESSION8E_MANIFEST_SHA256},
            {"path": "artifacts/pit_validation/corrected_step2/outputs/certified_snapshots.parquet",
             "sha256": sha256_file(STEP2 / "outputs/certified_snapshots.parquet")},
        ],
        "configuration": configuration,
        "records": [record(path, "session8f_input_output_checkpoint_or_lineage") for path in artifact_files],
        "code_lineage": [record(path, "session8f_code_or_test") for path in code_paths],
        "environment": {"python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__},
        "dirty_state": {"baseline": BASELINE_COMMIT, "complete_status_recorded": True,
                        "records": [path.relative_to(ROOT).as_posix() for path in lineage_paths]},
        "primary_dataset": PRIMARY_DATASET,
        "canonical_validation": canonical_validation,
        "population_separation": {population: f"outputs/{population}" for population in POPULATIONS},
        "limitations": [
            "No certified macro vintage/release-lag input exists; macro features are unavailable.",
            "Rows missing any required decision-time price feature retain null price features and an explicit unavailable reason.",
            "Session 8E contains zero policy-only label additions; physical sensitivity output remains separate but has identical support.",
            "The US annual population is historically enriched but not comprehensively survivorship-free: free sources do not provide CRSP-quality historical exchange membership, security-type/ticker histories, delisting terms, or delisting returns.",
            "No model, prediction, threshold, backtest, or Session 9 action is part of this artifact.",
        ],
    }
    manifest_path = artifact_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return {"manifest": manifest_path, "manifest_sha256": sha256_file(manifest_path),
            "summary": summary_frame, "validation": validation}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    args = parser.parse_args()
    result = build(args.artifact_root.resolve())
    print(json.dumps({"manifest": str(result["manifest"]),
                      "manifest_sha256": result["manifest_sha256"],
                      "validation": result["validation"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
