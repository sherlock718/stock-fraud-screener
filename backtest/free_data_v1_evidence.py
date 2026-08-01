"""Assemble the manifest-backed free-data V1 performance-input evidence.

This module is deliberately performance-free.  It independently revalidates
the frozen P2-P4, Session 8E, S1, and E1 evidence, materializes the exact P4
holding/benchmark requirements, and records observed/scenario eligibility.
It does not build NAV, calculate a return, or infer a security event.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_PARENT = (
    ROOT / "artifacts/performance_inputs/free_data_v1"
)

P2_MANIFEST_SHA256 = (
    "40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3"
)
P3_MANIFEST_SHA256 = (
    "8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2"
)
P4_MANIFEST_SHA256 = (
    "28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7"
)
SESSION8E_MANIFEST_SHA256 = (
    "0ab15685a445f09919b19393e074b8b5903afef7e12defc7c44aeac624f4581a"
)
S1_MANIFEST_SHA256 = (
    "28317e4ae0126367c38bb40c9fc8169394fc1141c475072d0dc484c141479a1e"
)
E1_MANIFEST_SHA256 = (
    "e44ff9aa9d2ac3be2ae66e5fb006bba435cdda3eef4c4521b4d1e3362a58caa6"
)
E1_ADJUDICATION_MANIFEST_SHA256 = (
    "dcbf95776b50139797410674d7f0ca410cfdaa1401917dea4fb47f7b00e9c7c6"
)

BENCHMARK_SYMBOLS = ("IWC", "IWM", "MDY", "SPY")
NAMESPACES = (
    "observed_available_diagnostic",
    "best_free_evidence_full_accounting",
    "legacy_minus_50_percent_unsupported_exit",
    "conservative_terminal_loss_100_percent",
)
RATE_NAMESPACES = (
    "dgs1mo_alfred_2026_07_17",
    "zero_risk_free_sharpe_diagnostic",
)
VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*-b1c$")

DGS1MO_METADATA_ENDPOINT = "https://api.stlouisfed.org/fred/series"
DGS1MO_OBSERVATIONS_ENDPOINT = (
    "https://api.stlouisfed.org/fred/series/observations"
)
DGS1MO_METADATA_PARAMS = {
    "series_id": "DGS1MO",
    "realtime_start": "2026-07-17",
    "realtime_end": "2026-07-17",
    "file_type": "json",
}
DGS1MO_OBSERVATION_PARAMS = {
    **DGS1MO_METADATA_PARAMS,
    "observation_start": "2015-07-01",
    "observation_end": "2026-07-02",
    "frequency": "d",
    "units": "lin",
}


@dataclass(frozen=True)
class FrozenInputs:
    """Paths to the frozen evidence boundary consumed by B1C."""

    p2_root: Path = ROOT / "artifacts/canonical/corrected_us_annual"
    p3_root: Path = (
        ROOT / "artifacts/canonical/corrected_us_annual_3y_research_model"
    )
    p4_root: Path = (
        ROOT / "artifacts/canonical/corrected_us_annual_3y_product"
    )
    session8e_root: Path = (
        ROOT / "artifacts/pit_validation/contract_aligned_label_inputs"
    )
    s1_root: Path = (
        ROOT
        / "artifacts/security_ledger/us/20260730T141429Z-s1-final"
    )
    e1_root: Path = (
        ROOT / "artifacts/event_review/us/20260730T144043Z-e1-final"
    )
    e1_adjudication_root: Path = (
        ROOT
        / "artifacts/event_review/us/"
        "20260730T173110Z-e1-adjudication-v2"
    )


DEFAULT_INPUTS = FrozenInputs()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    )


def _record(base: Path, path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(base).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _manifest_index(manifest: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for section in ("records", "validated_inputs", "code_lineage"):
        for item in manifest.get(section, []):
            if isinstance(item, dict) and item.get("path"):
                output[str(item["path"])] = item
    return output


def _verify_record(
    path: Path,
    record: Mapping[str, Any],
    label: str,
) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(f"{label} is missing: {path}")
    size = path.stat().st_size
    digest = sha256_file(path)
    if record.get("size_bytes") is not None and size != int(
        record["size_bytes"]
    ):
        raise RuntimeError(f"{label} size mismatch")
    if digest != record.get("sha256"):
        raise RuntimeError(f"{label} hash mismatch")
    return {"path": str(path), "size_bytes": size, "sha256": digest}


def _verify_manifest_record(
    path: Path,
    manifest_path: Path,
    manifest: Mapping[str, Any],
    label: str,
) -> dict[str, Any]:
    index = _manifest_index(manifest)
    candidates: list[str] = []
    try:
        candidates.append(path.relative_to(ROOT).as_posix())
    except ValueError:
        pass
    try:
        candidates.append(path.relative_to(manifest_path.parent).as_posix())
    except ValueError:
        pass
    for candidate in candidates:
        if candidate in index:
            result = _verify_record(path, index[candidate], label)
            result["manifest_record_path"] = candidate
            return result
    raise RuntimeError(f"{label} is absent from its manifest")


def _load_pinned_manifest(
    path: Path,
    expected_sha256: str,
    expected_class: str,
    label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not path.is_file() or sha256_file(path) != expected_sha256:
        raise RuntimeError(f"{label} manifest hash mismatch")
    manifest = json.loads(path.read_text())
    if manifest.get("artifact_class") != expected_class:
        raise RuntimeError(f"{label} artifact class drifted")
    return manifest, {
        "name": label,
        "path": path.relative_to(ROOT).as_posix(),
        "size_bytes": path.stat().st_size,
        "sha256": expected_sha256,
    }


def verify_frozen_inputs(
    inputs: FrozenInputs = DEFAULT_INPUTS,
) -> dict[str, Any]:
    """Independently hash and validate every frozen boundary used by B1C."""
    definitions = {
        "p2": (
            inputs.p2_root / "manifest.json",
            P2_MANIFEST_SHA256,
            "CANONICAL_CORRECTED_US_ANNUAL_FEATURE_POPULATION",
        ),
        "p3": (
            inputs.p3_root / "manifest.json",
            P3_MANIFEST_SHA256,
            "CANONICAL_US_ANNUAL_OBSERVED_ONLY_3Y_RESEARCH_MODEL_OOS",
        ),
        "p4": (
            inputs.p4_root / "manifest.json",
            P4_MANIFEST_SHA256,
            "CANONICAL_US_ANNUAL_OBSERVED_ONLY_3Y_PRODUCT",
        ),
        "session8e": (
            inputs.session8e_root / "manifest.json",
            SESSION8E_MANIFEST_SHA256,
            "SESSION8E_CONTRACT_ALIGNED_LABEL_INPUTS",
        ),
        "s1": (
            inputs.s1_root / "manifest.json",
            S1_MANIFEST_SHA256,
            "VERSIONED_PROVIDER_NEUTRAL_SECURITY_LEDGER",
        ),
        "e1": (
            inputs.e1_root / "manifest.json",
            E1_MANIFEST_SHA256,
            "VERSIONED_HISTORICAL_THEN_LIVE_EVENT_REVIEW",
        ),
        "e1_adjudication": (
            inputs.e1_adjudication_root / "manifest.json",
            E1_ADJUDICATION_MANIFEST_SHA256,
            "VERSIONED_E1_OFFLINE_HUMAN_SIGNOFF_ADJUDICATION",
        ),
    }
    manifests: dict[str, dict[str, Any]] = {}
    manifest_records: list[dict[str, Any]] = []
    for name, (path, digest, artifact_class) in definitions.items():
        manifest, record = _load_pinned_manifest(
            path, digest, artifact_class, name
        )
        manifests[name] = manifest
        manifest_records.append(record)

    if (
        not manifests["p2"].get("claim", {}).get("canonical_dataset_ready")
        or manifests["p3"].get("claim", {}).get("status") != "accepted"
        or manifests["p4"].get("claim", {}).get("status")
        != "accepted_with_fail_closed_performance"
        or manifests["p4"].get("claim", {}).get("performance_calculated")
        or not manifests["session8e"].get("raw_payloads", {}).get(
            "all_success_hashes_verified"
        )
        or manifests["s1"].get("claim", {}).get("performance_calculated")
        or manifests["e1"].get("claim", {}).get("performance_calculated")
        or manifests["e1_adjudication"].get("claim", {}).get(
            "performance_calculated"
        )
    ):
        raise RuntimeError("frozen upstream acceptance contract drifted")

    consumed = {
        "p4_holdings": inputs.p4_root / "outputs/holdings.parquet",
        "p4_vintage_plan": (
            inputs.p4_root / "outputs/backtest_vintage_plan.parquet"
        ),
        "session8e_mapping": (
            inputs.session8e_root / "inputs/security_mapping.parquet"
        ),
        "session8e_config": (
            inputs.session8e_root / "configuration/config.json"
        ),
        "session8e_responses": (
            inputs.session8e_root / "raw/response_manifest.jsonl"
        ),
        "session8e_raw_inventory": (
            inputs.session8e_root / "raw/raw_inventory.json"
        ),
        "session8e_normalized_inventory": (
            inputs.session8e_root
            / "normalized/normalized_inventory.json"
        ),
        "session8e_normalization_summary": (
            inputs.session8e_root
            / "normalized/normalization_summary.parquet"
        ),
        "session8e_xnys_calendar": (
            inputs.session8e_root
            / "calendar/xnys_regular_sessions.parquet"
        ),
        "session8e_xnas_calendar": (
            inputs.session8e_root
            / "calendar/xnas_regular_sessions.parquet"
        ),
        "s1_requirements": (
            inputs.s1_root
            / "outputs/primary/required_instruments.parquet"
        ),
        "s1_coverage": (
            inputs.s1_root / "outputs/primary/coverage.parquet"
        ),
        "s1_events": inputs.s1_root / "outputs/primary/events.parquet",
        "s1_sensitivity": (
            inputs.s1_root / "sensitivity/legacy_minus_50_policy.json"
        ),
        "e1_historical": (
            inputs.e1_root
            / "outputs/historical/reconciliation.parquet"
        ),
        "e1_adjudication_names": (
            inputs.e1_adjudication_root
            / "outputs/live/name_level_adjudication.parquet"
        ),
    }
    owners = {
        name: (
            "p4" if name.startswith("p4_") else
            "session8e" if name.startswith("session8e_") else
            "s1" if name.startswith("s1_") else
            "e1_adjudication" if name.startswith("e1_adjudication_") else
            "e1"
        )
        for name in consumed
    }
    validated_records: dict[str, dict[str, Any]] = {}
    for name, path in consumed.items():
        owner = owners[name]
        validated_records[name] = _verify_manifest_record(
            path,
            definitions[owner][0],
            manifests[owner],
            name,
        )

    sensitivity = json.loads(consumed["s1_sensitivity"].read_text())
    if (
        sensitivity.get("scenario_id")
        != "legacy_minus_50_percent_unsupported_exit"
        or sensitivity.get("assumed_return") != -0.50
        or sensitivity.get("observed_fact") is not False
        or "model_training" not in sensitivity.get(
            "prohibited_destinations", []
        )
    ):
        raise RuntimeError("S1 legacy-minus-50 contract drifted")

    return {
        "manifests": manifests,
        "manifest_records": manifest_records,
        "validated_records": validated_records,
        "consumed_paths": consumed,
        "sensitivity": sensitivity,
    }


def _latest_response_records(path: Path) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        item = json.loads(line)
        symbol = str(item["symbol"])
        if symbol in latest and latest[symbol].get("status") == "success":
            raise RuntimeError(
                f"Session 8E response follows terminal success: {symbol} "
                f"line {number}"
            )
        latest[symbol] = item
    return latest


def _normalise_symbol(value: Any) -> str:
    return str(value or "").strip().upper().replace(".", "-")


def _validate_normalized_symbol(
    *,
    symbol: str,
    expected_role: str,
    expected_exchange: str,
    expected_calendar: str,
    session8e_root: Path,
    response: Mapping[str, Any],
    raw_inventory: Mapping[str, Mapping[str, Any]],
    normalized_inventory: Mapping[str, Mapping[str, Any]],
    calendars: Mapping[str, pd.DataFrame],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if response.get("status") != "success":
        raise RuntimeError(f"required Yahoo response is unavailable: {symbol}")
    raw_relative = str(response.get("stored_path"))
    raw_record = raw_inventory.get(raw_relative)
    if raw_record is None:
        raise RuntimeError(f"raw inventory omission: {symbol}")
    raw_path = session8e_root / raw_relative
    _verify_record(raw_path, raw_record, f"raw Yahoo payload {symbol}")
    if (
        raw_path.stat().st_size != int(response["stored_size_bytes"])
        or sha256_file(raw_path) != response["stored_sha256"]
    ):
        raise RuntimeError(f"raw response manifest mismatch: {symbol}")
    with gzip.open(raw_path, "rb") as handle:
        payload = handle.read()
    if (
        len(payload) != int(response["response_size_bytes"])
        or hashlib.sha256(payload).hexdigest()
        != response["response_sha256"]
    ):
        raise RuntimeError(f"decompressed response mismatch: {symbol}")

    try:
        result = json.loads(payload)["chart"]["result"][0]
        meta = result["meta"]
        timestamps = result.get("timestamp") or []
        quote = result.get("indicators", {}).get("quote", [{}])[0]
        raw_close = quote.get("close")
        volume = quote.get("volume")
        adjusted = (
            result.get("indicators", {})
            .get("adjclose", [{}])[0]
            .get("adjclose")
        )
    except Exception as exc:
        raise RuntimeError(f"invalid Yahoo payload: {symbol}") from exc
    if (
        not timestamps
        or raw_close is None
        or adjusted is None
        or volume is None
        or not len(timestamps) == len(raw_close) == len(adjusted) == len(volume)
    ):
        raise RuntimeError(f"misaligned Yahoo arrays: {symbol}")
    timestamp_values = np.asarray(timestamps, dtype=np.int64)
    if np.any(np.diff(timestamp_values) <= 0):
        raise RuntimeError(f"Yahoo timestamps are not strictly increasing: {symbol}")

    expected_type = "ETF" if expected_role == "benchmark" else "EQUITY"
    meta_contract = {
        "symbol": _normalise_symbol(meta.get("symbol")) == symbol,
        "currency": meta.get("currency") == "USD",
        "instrument_type": meta.get("instrumentType") == expected_type,
        "exchange": meta.get("exchangeName") == expected_exchange,
        "timezone": meta.get("exchangeTimezoneName")
        == "America/New_York",
        "granularity": meta.get("dataGranularity") == "1d",
    }
    if not all(meta_contract.values()):
        failed = sorted(key for key, value in meta_contract.items() if not value)
        raise RuntimeError(f"Yahoo metadata mismatch for {symbol}: {failed}")

    normalized_relative = f"normalized/prices/{symbol}.parquet"
    normalized_record = normalized_inventory.get(normalized_relative)
    if normalized_record is None:
        raise RuntimeError(f"normalized inventory omission: {symbol}")
    normalized_path = session8e_root / normalized_relative
    _verify_record(
        normalized_path,
        normalized_record,
        f"normalized Yahoo prices {symbol}",
    )
    prices = pd.read_parquet(normalized_path)
    expected_columns = {
        "symbol",
        "source_timestamp",
        "raw_close",
        "total_return_close",
        "session_date",
        "market_open",
        "market_close",
    }
    if set(prices.columns) != expected_columns or prices.empty:
        raise RuntimeError(f"normalized price schema mismatch: {symbol}")
    prices = prices.copy()
    for column in ("source_timestamp", "market_open", "market_close"):
        prices[column] = pd.to_datetime(prices[column], utc=True)
    if (
        not prices["symbol"].eq(symbol).all()
        or prices["source_timestamp"].duplicated().any()
        or prices["session_date"].duplicated().any()
        or not prices["source_timestamp"].is_monotonic_increasing
        or not prices["market_close"].is_monotonic_increasing
        or not np.isfinite(prices["raw_close"]).all()
        or not np.isfinite(prices["total_return_close"]).all()
        or not prices["raw_close"].gt(0).all()
        or not prices["total_return_close"].gt(0).all()
    ):
        raise RuntimeError(f"normalized price ordering/value mismatch: {symbol}")

    raw_frame = pd.DataFrame(
        {
            "source_timestamp": pd.to_datetime(timestamps, unit="s", utc=True),
            "raw_close_from_payload": pd.to_numeric(raw_close, errors="coerce"),
            "adjusted_from_payload": pd.to_numeric(adjusted, errors="coerce"),
        }
    )
    aligned = prices.merge(
        raw_frame, on="source_timestamp", how="left", validate="one_to_one"
    )
    if (
        aligned["raw_close_from_payload"].isna().any()
        or aligned["adjusted_from_payload"].isna().any()
        or not np.array_equal(
            aligned["raw_close"].to_numpy(),
            aligned["raw_close_from_payload"].to_numpy(),
        )
        or not np.array_equal(
            aligned["total_return_close"].to_numpy(),
            aligned["adjusted_from_payload"].to_numpy(),
        )
    ):
        raise RuntimeError(f"raw/normalized price alignment mismatch: {symbol}")

    timezone_name = ZoneInfo(str(meta["exchangeTimezoneName"]))
    local_dates = prices["source_timestamp"].map(
        lambda value: value.tz_convert(timezone_name).strftime("%Y-%m-%d")
    )
    if not local_dates.eq(prices["session_date"].astype(str)).all():
        raise RuntimeError(f"source/local session date mismatch: {symbol}")

    calendar = calendars[expected_calendar]
    calendar_fields = calendar[
        ["session_date", "market_open", "market_close"]
    ]
    checked = prices.merge(
        calendar_fields,
        on="session_date",
        how="left",
        validate="one_to_one",
        suffixes=("", "_calendar"),
    )
    if (
        checked["market_close_calendar"].isna().any()
        or not checked["market_open"].equals(checked["market_open_calendar"])
        or not checked["market_close"].equals(checked["market_close_calendar"])
    ):
        raise RuntimeError(f"exchange-calendar alignment mismatch: {symbol}")

    lineage = {
        "provider_symbol": symbol,
        "instrument_role": expected_role,
        "provider_exchange": expected_exchange,
        "exchange_calendar": expected_calendar,
        "currency": meta.get("currency"),
        "provider_instrument_type": meta.get("instrumentType"),
        "provider_timezone": meta.get("exchangeTimezoneName"),
        "data_granularity": meta.get("dataGranularity"),
        "raw_path": raw_relative,
        "raw_stored_size_bytes": raw_path.stat().st_size,
        "raw_stored_sha256": response["stored_sha256"],
        "response_size_bytes": response["response_size_bytes"],
        "response_sha256": response["response_sha256"],
        "normalized_path": normalized_relative,
        "normalized_size_bytes": normalized_path.stat().st_size,
        "normalized_sha256": normalized_record["sha256"],
        "retrieved_at_utc": response["retrieved_at_utc"],
        "regular_session_rows": len(prices),
        "first_session": str(prices["session_date"].iloc[0]),
        "last_session": str(prices["session_date"].iloc[-1]),
        "adjustment_semantics": (
            "provider_adjclose_sole_total_return_input_no_double_counting_"
            "provider_does_not_certify_exact_semantics"
        ),
        "certified_security_action_ledger": False,
    }
    prices["instrument_role"] = expected_role
    prices["provider_exchange"] = expected_exchange
    prices["exchange_calendar"] = expected_calendar
    prices["currency"] = "USD"
    prices["provider_instrument_type"] = expected_type
    prices["provider_timezone"] = "America/New_York"
    prices["data_granularity"] = "1d"
    prices["raw_response_sha256"] = response["response_sha256"]
    prices["normalized_sha256"] = normalized_record["sha256"]
    prices["price_evidence_status"] = (
        "frozen_session8e_yahoo_not_certified_security_action_ledger"
    )
    prices["adjustment_semantics"] = lineage["adjustment_semantics"]
    return prices, lineage


def _calendar_frames(session8e_root: Path) -> dict[str, pd.DataFrame]:
    output: dict[str, pd.DataFrame] = {}
    for name in ("XNYS", "XNAS"):
        frame = pd.read_parquet(
            session8e_root
            / f"calendar/{name.lower()}_regular_sessions.parquet"
        ).copy()
        frame["session_date"] = frame["session_date"].astype(str)
        frame["market_open"] = pd.to_datetime(frame["market_open"], utc=True)
        frame["market_close"] = pd.to_datetime(frame["market_close"], utc=True)
        if (
            frame["session_date"].duplicated().any()
            or not frame["market_close"].is_monotonic_increasing
        ):
            raise RuntimeError(f"{name} frozen calendar ordering mismatch")
        output[name] = frame
    if not output["XNYS"].equals(output["XNAS"]):
        raise RuntimeError("frozen XNYS/XNAS session calendars diverged")
    return output


def validate_market_evidence(
    preflight: Mapping[str, Any],
    inputs: FrozenInputs = DEFAULT_INPUTS,
) -> dict[str, Any]:
    """Validate all 132 holding-symbol and four benchmark Yahoo payloads."""
    consumed = preflight["consumed_paths"]
    holdings = pd.read_parquet(consumed["p4_holdings"])
    plan = pd.read_parquet(consumed["p4_vintage_plan"])
    mapping = pd.read_parquet(consumed["session8e_mapping"])
    summary = pd.read_parquet(consumed["session8e_normalization_summary"])
    if len(holdings) != 180 or len(plan) != 180:
        raise RuntimeError("P4 holdings/vintage row count drifted")
    if (
        holdings["stable_row_id"].duplicated().any()
        or plan["stable_row_id"].duplicated().any()
        or set(holdings["stable_row_id"]) != set(plan["stable_row_id"])
    ):
        raise RuntimeError("P4 stable_row_id contract drifted")
    holding_symbols = sorted(set(holdings["provider_symbol"].astype(str)))
    relevant_symbols = sorted(set(holding_symbols) | set(BENCHMARK_SYMBOLS))
    if len(holding_symbols) != 132 or len(relevant_symbols) != 136:
        raise RuntimeError("expected exactly 132 holding and four benchmark symbols")
    if not holdings["ticker"].astype(str).eq(
        holdings["provider_symbol"].astype(str)
    ).all():
        raise RuntimeError("P4 contains a prohibited ticker substitution")

    mapping_index = mapping.set_index("entity_id")
    for row in holdings.itertuples(index=False):
        if row.entity_id not in mapping_index.index:
            raise RuntimeError(f"Session 8E mapping absent: {row.stable_row_id}")
        mapped = mapping_index.loc[row.entity_id]
        if (
            str(mapped["ticker"]) != str(row.ticker)
            or str(mapped["provider_symbol"]) != str(row.provider_symbol)
            or str(mapped["mapping_policy"]) != "exact_uppercase"
        ):
            raise RuntimeError(f"Session 8E mapping drift: {row.stable_row_id}")

    responses = _latest_response_records(consumed["session8e_responses"])
    raw_inventory = {
        str(item["path"]): item
        for item in json.loads(consumed["session8e_raw_inventory"].read_text())
    }
    normalized_inventory = {
        str(item["path"]): item
        for item in json.loads(
            consumed["session8e_normalized_inventory"].read_text()
        )
    }
    summary_index = summary.set_index("symbol")
    calendars = _calendar_frames(inputs.session8e_root)

    symbol_contracts: dict[str, tuple[str, str, str]] = {}
    for symbol, group in holdings.groupby("provider_symbol", sort=True):
        contracts = group[
            ["provider_exchange", "exchange_calendar"]
        ].drop_duplicates()
        if len(contracts) != 1:
            raise RuntimeError(f"P4 symbol contract is not unique: {symbol}")
        contract = contracts.iloc[0]
        symbol_contracts[str(symbol)] = (
            "holding",
            str(contract["provider_exchange"]),
            str(contract["exchange_calendar"]),
        )
    for symbol in BENCHMARK_SYMBOLS:
        symbol_contracts[symbol] = ("benchmark", "PCX", "XNYS")

    price_frames: dict[str, pd.DataFrame] = {}
    lineage: list[dict[str, Any]] = []
    for symbol in relevant_symbols:
        if symbol not in responses or symbol not in summary_index.index:
            raise RuntimeError(f"required Session 8E symbol absent: {symbol}")
        if summary_index.loc[symbol, "status"] != "supported":
            raise RuntimeError(f"required Session 8E symbol unsupported: {symbol}")
        role, exchange, calendar = symbol_contracts[symbol]
        frame, item = _validate_normalized_symbol(
            symbol=symbol,
            expected_role=role,
            expected_exchange=exchange,
            expected_calendar=calendar,
            session8e_root=inputs.session8e_root,
            response=responses[symbol],
            raw_inventory=raw_inventory,
            normalized_inventory=normalized_inventory,
            calendars=calendars,
        )
        price_frames[symbol] = frame
        lineage.append(item)

    response_hash_by_symbol = {
        item["provider_symbol"]: item["response_sha256"] for item in lineage
    }
    expected_hashes = holdings["provider_symbol"].map(response_hash_by_symbol)
    if not expected_hashes.eq(holdings["raw_response_sha256"]).all():
        raise RuntimeError("P4 raw response hashes do not match Session 8E")

    return {
        "holdings": holdings,
        "plan": plan,
        "calendars": calendars,
        "price_frames": price_frames,
        "price_lineage": pd.DataFrame(lineage).sort_values(
            "provider_symbol"
        ).reset_index(drop=True),
        "relevant_symbols": relevant_symbols,
    }


def _reconcile_requirements(
    preflight: Mapping[str, Any],
    market: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    consumed = preflight["consumed_paths"]
    holdings = market["holdings"]
    plan = market["plan"]
    required = pd.read_parquet(consumed["s1_requirements"])
    s1_coverage = pd.read_parquet(consumed["s1_coverage"])
    e1 = pd.read_parquet(consumed["e1_historical"])
    adjudication = pd.read_parquet(consumed["e1_adjudication_names"])

    if (
        len(required) != 184
        or required["requirement_id"].duplicated().any()
        or int(required["instrument_role"].eq("holding").sum()) != 180
        or int(required["instrument_role"].eq("benchmark").sum()) != 4
    ):
        raise RuntimeError("S1 requirement primary-key/count contract drifted")
    holding_required = required[required["instrument_role"].eq("holding")]
    if (
        holding_required["stable_row_id"].isna().any()
        or holding_required["stable_row_id"].duplicated().any()
        or set(holding_required["stable_row_id"])
        != set(holdings["stable_row_id"])
        or set(
            required.loc[
                required["instrument_role"].eq("benchmark"), "ticker"
            ]
        )
        != set(BENCHMARK_SYMBOLS)
    ):
        raise RuntimeError("S1 requirement identity reconciliation failed")

    if (
        len(s1_coverage) != 184
        or s1_coverage["requirement_id"].duplicated().any()
        or set(s1_coverage["requirement_id"]) != set(required["requirement_id"])
        or s1_coverage["coverage_status"].eq("matched").any()
        or s1_coverage["coverage_status"].value_counts().to_dict()
        != {"ambiguous": 135, "unsupported": 49}
    ):
        raise RuntimeError("S1 coverage reconciliation drifted")

    selected = holdings[
        [
            "stable_row_id",
            "entity_id",
            "cik",
            "ticker",
            "provider_symbol",
            "exchange",
            "provider_exchange",
            "exchange_calendar",
            "decision_timestamp",
            "prediction_timestamp",
            "entry_timestamp",
            "entry_session_date",
            "benchmark_symbol",
            "weight",
        ]
    ].merge(
        plan[
            [
                "stable_row_id",
                "target_exit_timestamp",
                "calendar_exit_timestamp",
                "holding_months",
                "planned_vintage_aum_usd",
                "planned_entry_notional_usd",
                "transaction_cost_rate_per_side",
                "transaction_cost_basis",
                "vintage_clock_status",
            ]
        ],
        on="stable_row_id",
        validate="one_to_one",
    )
    selected["requirement_id"] = selected["stable_row_id"].map(
        lambda value: f"holding:{value}"
    )
    selected["instrument_role"] = "holding"
    selected["decision_year"] = pd.to_datetime(
        selected["decision_timestamp"], utc=True
    ).dt.year
    selected["requirement_state"] = np.where(
        selected["calendar_exit_timestamp"].notna(),
        "matured_2015_2023",
        "open_2024_2026",
    )
    selected = selected.merge(
        holding_required[
            ["requirement_id", "sec_cik", "required_start", "required_end"]
        ],
        on="requirement_id",
        validate="one_to_one",
    )
    if (
        selected["requirement_state"].value_counts().to_dict()
        != {"matured_2015_2023": 135, "open_2024_2026": 45}
        or set(selected.loc[
            selected["requirement_state"].eq("matured_2015_2023"),
            "decision_year",
        ])
        != set(range(2015, 2024))
        or set(selected.loc[
            selected["requirement_state"].eq("open_2024_2026"),
            "decision_year",
        ])
        != {2024, 2025, 2026}
    ):
        raise RuntimeError("P4 matured/open partition drifted")

    benchmark_required = required[
        required["instrument_role"].eq("benchmark")
    ].copy()
    benchmark_required["provider_symbol"] = benchmark_required["ticker"]
    benchmark_required["provider_exchange"] = "PCX"
    benchmark_required["exchange_calendar"] = "XNYS"
    benchmark_required["requirement_state"] = (
        "unsupported_incomplete_benchmark_master"
    )
    benchmark_required["decision_year"] = pd.NA
    benchmark_required["benchmark_symbol"] = benchmark_required["ticker"]
    benchmark_required["assigned_holding_count"] = benchmark_required[
        "ticker"
    ].map(selected["benchmark_symbol"].value_counts()).fillna(0).astype(int)

    requirement_columns = [
        "requirement_id",
        "stable_row_id",
        "instrument_role",
        "sec_cik",
        "ticker",
        "provider_symbol",
        "provider_exchange",
        "exchange_calendar",
        "required_start",
        "required_end",
        "requirement_state",
        "decision_year",
        "benchmark_symbol",
    ]
    holding_requirements = selected[requirement_columns + [
        "entity_id",
        "cik",
        "exchange",
        "decision_timestamp",
        "prediction_timestamp",
        "entry_timestamp",
        "entry_session_date",
        "target_exit_timestamp",
        "calendar_exit_timestamp",
        "holding_months",
        "weight",
        "planned_vintage_aum_usd",
        "planned_entry_notional_usd",
        "transaction_cost_rate_per_side",
        "transaction_cost_basis",
        "vintage_clock_status",
    ]].copy()
    for column in holding_requirements.columns:
        if column not in benchmark_required:
            benchmark_required[column] = pd.NA
    requirements = pd.DataFrame.from_records(
        [
            *holding_requirements.to_dict(orient="records"),
            *benchmark_required[holding_requirements.columns].to_dict(
                orient="records"
            ),
        ],
        columns=holding_requirements.columns,
    )
    requirements = requirements.sort_values(
        ["instrument_role", "requirement_id"]
    ).reset_index(drop=True)

    identity = s1_coverage.rename(
        columns={
            "coverage_status": "s1_coverage_status",
            "identity_status": "s1_identity_status",
            "listing_status": "s1_listing_status",
            "security_type_status": "s1_security_type_status",
            "event_status": "s1_event_status",
            "price_adjustment_status": "s1_price_adjustment_status",
        }
    )
    identity["current_ticker_substitution_used"] = False
    identity["ticker_chaining_used"] = False
    identity["dated_security_lineage_complete"] = False
    identity["identity_rule_status"] = (
        "current_association_only_not_dated_security_lineage"
    )
    identity["certified_performance_identity_available"] = False

    if (
        len(e1) != 180
        or e1["requirement_id"].duplicated().any()
        or set(e1["requirement_id"])
        != set(holding_required["requirement_id"])
        or not e1["deterministic_action"].eq("unresolved").all()
    ):
        raise RuntimeError("E1 historical reconciliation drifted")
    current_ids = set(
        selected.loc[selected["decision_year"].eq(2026), "requirement_id"]
    )
    if (
        len(adjudication) != 15
        or adjudication["requirement_id"].duplicated().any()
        or set(adjudication["requirement_id"]) != current_ids
        or not adjudication["deterministic_action"].eq("unresolved").all()
        or int(adjudication["retrieved_document_count"].gt(0).sum()) != 14
        or adjudication.loc[
            adjudication["ticker"].eq("SSTK"), "retrieved_document_count"
        ].tolist()
        != [0]
    ):
        raise RuntimeError("E1 current-document reconciliation drifted")

    action = identity[
        [
            "requirement_id",
            "instrument_role",
            "stable_row_id",
            "security_id",
            "ticker",
            "s1_event_status",
            "s1_price_adjustment_status",
        ]
    ].merge(
        e1[
            [
                "requirement_id",
                "source_event_indicator_count",
                "holding_window_indicator_count",
                "deterministic_action",
                "historical_evaluation_status",
            ]
        ],
        on="requirement_id",
        how="left",
        validate="one_to_one",
    ).merge(
        adjudication[
            [
                "requirement_id",
                "planned_document_count",
                "retrieved_document_count",
                "claim_count",
                "document_set_status",
                "transaction_or_action_terms_complete",
                "summary_status",
            ]
        ],
        on="requirement_id",
        how="left",
        validate="one_to_one",
    )
    action["deterministic_action"] = action["deterministic_action"].fillna(
        "unsupported_benchmark_master"
    )
    action["primary_document_support_state"] = np.select(
        [
            action["retrieved_document_count"].fillna(0).gt(0),
            action["ticker"].eq("SSTK"),
            action["instrument_role"].eq("benchmark"),
        ],
        [
            "retrieved_primary_documents_incomplete",
            "no_retrieved_primary_document_claim",
            "not_collected_for_benchmark_master",
        ],
        default="not_in_current_primary_document_review",
    )
    action["provider_adjclose_action_semantics_certified"] = False
    action["forward_fill_across_unresolved_event_allowed"] = False
    action["event_inferred_from_disappearance_or_form_family"] = False
    action["unsupported_recovery_allowed_in_observed_namespace"] = False
    action["assumed_outcome_allowed_in_labels_or_training"] = False
    action["primary_return_available"] = False
    return requirements, identity, action, benchmark_required


def _coverage_tables(
    requirements: pd.DataFrame,
    identity: pd.DataFrame,
    market: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    prices: Mapping[str, pd.DataFrame] = market["price_frames"]
    calendar = market["calendars"]["XNYS"].copy()
    calendar["month"] = calendar["market_close"].dt.tz_localize(None).dt.to_period(
        "M"
    )
    month_ends = calendar.sort_values("market_close").groupby("month").tail(1)
    s1 = identity.set_index("requirement_id")
    rows: list[dict[str, Any]] = []

    for requirement in requirements.itertuples(index=False):
        symbol = str(requirement.provider_symbol)
        stock = prices[symbol]
        observed_last = stock["market_close"].max()
        base = {
            "requirement_id": requirement.requirement_id,
            "stable_row_id": requirement.stable_row_id,
            "instrument_role": requirement.instrument_role,
            "ticker": requirement.ticker,
            "provider_symbol": symbol,
            "requirement_state": requirement.requirement_state,
            "decision_year": requirement.decision_year,
            "s1_coverage_status": s1.loc[
                requirement.requirement_id, "s1_coverage_status"
            ],
            "s1_identity_status": s1.loc[
                requirement.requirement_id, "s1_identity_status"
            ],
            "s1_event_status": s1.loc[
                requirement.requirement_id, "s1_event_status"
            ],
            "fully_matched_s1_requirement": False,
            "provider_adjclose_semantics_certified": False,
            "certified_security_action_ledger": False,
            "benchmark_gap_scenario_imputation_allowed": False,
        }
        if requirement.instrument_role == "benchmark":
            required_start = pd.Timestamp(requirement.required_start)
            required_end = pd.Timestamp(requirement.required_end)
            base.update(
                {
                    "assigned_benchmark_symbol": symbol,
                    "entry_observed_common": bool(
                        stock["market_close"].eq(required_start).any()
                    ),
                    "exit_observed_common": bool(
                        stock["market_close"].eq(required_end).any()
                    ),
                    "required_month_end_count": pd.NA,
                    "observed_common_month_end_count": pd.NA,
                    "missing_common_month_end_count": pd.NA,
                    "benchmark_gap_count": 0,
                    "evidence_end_timestamp": observed_last,
                    "price_coverage_status": (
                        "payload_supported_to_evidence_end_but_master_"
                        "requirement_incomplete"
                    ),
                    "relative_evidence_status": (
                        "benchmark_master_identity_action_incomplete"
                    ),
                }
            )
            rows.append(base)
            continue

        benchmark = prices[str(requirement.benchmark_symbol)]
        common = stock.merge(
            benchmark,
            on=["session_date", "market_close"],
            suffixes=("_stock", "_benchmark"),
            validate="one_to_one",
        )
        entry = pd.Timestamp(requirement.entry_timestamp)
        matured = pd.notna(requirement.calendar_exit_timestamp)
        end = (
            pd.Timestamp(requirement.calendar_exit_timestamp)
            if matured
            else min(stock["market_close"].max(), benchmark["market_close"].max())
        )
        required_month_ends = month_ends.loc[
            month_ends["market_close"].ge(entry)
            & month_ends["market_close"].le(end),
            "market_close",
        ].tolist()
        common_closes = set(common["market_close"])
        missing_month_ends = [
            instant for instant in required_month_ends
            if instant not in common_closes
        ]
        entry_common = entry in common_closes
        exit_common = (
            pd.Timestamp(requirement.calendar_exit_timestamp) in common_closes
            if matured
            else False
        )
        if not entry_common or (matured and not exit_common):
            raise RuntimeError(
                f"P4 common entry/exit alignment failed: {requirement.requirement_id}"
            )
        if missing_month_ends:
            raise RuntimeError(
                f"common month-end coverage gap: {requirement.requirement_id}"
            )
        base.update(
            {
                "assigned_benchmark_symbol": requirement.benchmark_symbol,
                "entry_observed_common": entry_common,
                "exit_observed_common": exit_common,
                "required_month_end_count": len(required_month_ends),
                "observed_common_month_end_count": len(required_month_ends),
                "missing_common_month_end_count": 0,
                "benchmark_gap_count": 0,
                "evidence_end_timestamp": end,
                "price_coverage_status": (
                    "complete_common_entry_month_ends_and_exit"
                    if matured
                    else "open_common_observations_to_evidence_end"
                ),
                "relative_evidence_status": (
                    "observed_common_provider_evidence_available"
                    if matured
                    else "open_not_matured"
                ),
            }
        )
        rows.append(base)

    coverage = pd.DataFrame(rows).sort_values(
        ["instrument_role", "requirement_id"]
    ).reset_index(drop=True)
    if (
        len(coverage) != 184
        or coverage["requirement_id"].duplicated().any()
        or int(coverage["fully_matched_s1_requirement"].sum()) != 0
        or int(
            coverage["requirement_state"].eq("matured_2015_2023").sum()
        )
        != 135
        or int(coverage["requirement_state"].eq("open_2024_2026").sum())
        != 45
    ):
        raise RuntimeError("B1C coverage reconciliation failed")

    benchmark_coverage = coverage[
        coverage["instrument_role"].eq("benchmark")
    ].merge(
        requirements[
            requirements["instrument_role"].eq("benchmark")
        ][
            [
                "requirement_id",
                "required_start",
                "required_end",
            ]
        ],
        on="requirement_id",
        validate="one_to_one",
    )
    counts = market["holdings"]["benchmark_symbol"].value_counts()
    benchmark_coverage["assigned_holding_count"] = benchmark_coverage[
        "ticker"
    ].map(counts).fillna(0).astype(int)
    return coverage, benchmark_coverage


def _namespace_contracts() -> dict[str, dict[str, Any]]:
    common = {
        "complete_requirement_accounting_required": True,
        "silent_holding_drop_allowed": False,
        "benchmark_gap_imputation_allowed": False,
        "ticker_substitution_allowed": False,
        "ticker_chaining_without_dated_lineage_allowed": False,
        "forward_fill_across_unresolved_terminal_event_allowed": False,
        "assumed_outcome_allowed_in_labels_or_training": False,
    }
    return {
        "observed_available_diagnostic": {
            **common,
            "namespace_class": "observed_price_diagnostic",
            "terminal_assumption": None,
            "unsupported_recovery_allowed": False,
            "provider_certified": False,
        },
        "best_free_evidence_full_accounting": {
            **common,
            "namespace_class": "best_free_evidence_research",
            "terminal_assumption": None,
            "unsupported_recovery_allowed": False,
            "provider_certified": False,
        },
        "legacy_minus_50_percent_unsupported_exit": {
            **common,
            "namespace_class": "predeclared_policy_sensitivity",
            "terminal_assumption": -0.50,
            "trigger": "unsupported_terminal_exit_only",
            "observed_fact": False,
        },
        "conservative_terminal_loss_100_percent": {
            **common,
            "namespace_class": "predeclared_terminal_loss_stress",
            "terminal_assumption": -1.00,
            "trigger": "unsupported_terminal_exit_only",
            "observed_fact": False,
        },
    }


def _namespace_eligibility(
    coverage: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    contracts = _namespace_contracts()
    for namespace, contract in contracts.items():
        frame = coverage[
            [
                "requirement_id",
                "stable_row_id",
                "instrument_role",
                "ticker",
                "requirement_state",
                "price_coverage_status",
                "s1_coverage_status",
                "relative_evidence_status",
            ]
        ].copy()
        matured = frame["requirement_state"].eq("matured_2015_2023")
        benchmark = frame["instrument_role"].eq("benchmark")
        frame["namespace"] = namespace
        frame["eligibility_state"] = np.select(
            [matured, benchmark],
            [
                (
                    "eligible_observed_common_price_evidence"
                    if namespace == "observed_available_diagnostic"
                    else "eligible_no_terminal_scenario_required"
                ),
                "benchmark_master_incomplete",
            ],
            default="open_not_matured",
        )
        frame["eligible_for_future_nav_engine"] = matured
        frame["scenario_triggered"] = False
        frame["scenario_return_if_triggered"] = contract.get(
            "terminal_assumption"
        )
        frame["assumed_outcome_used"] = False
        frame["allowed_in_labels_or_training"] = False
        frame["benchmark_gap_imputed"] = False
        frame["performance_calculated"] = False
        outputs[namespace] = frame.sort_values(
            ["instrument_role", "requirement_id"]
        ).reset_index(drop=True)
    return outputs


def performance_contract() -> dict[str, Any]:
    """Return the frozen B1B contract encoded by the B1C artifact."""
    return {
        "schema_version": 1,
        "contract_id": "free_data_v1_performance_contract_b1b",
        "b1c_scope": (
            "evidence_and_eligibility_only_no_nav_return_metric_or_backtest"
        ),
        "requirements": {
            "holding_rows": 180,
            "benchmark_master_rows": 4,
            "primary_key": "requirement_id",
            "holding_primary_key": "stable_row_id",
            "no_current_ticker_substitution": True,
        },
        "valuation": {
            "decision": "annual July 2 00:00 UTC",
            "prediction": "annual July 2 00:01 UTC",
            "entry": (
                "first common regular-session close strictly after prediction"
            ),
            "holding_months": 36,
            "exit": (
                "first common regular-session close on or after entry plus "
                "36 calendar months within ten calendar days"
            ),
            "month_end": (
                "calendar-designated final common XNYS/XNAS regular session "
                "on or before month-end"
            ),
            "overlapping_vintages_independent": True,
            "later_vintage_rebalances_earlier_vintage": False,
        },
        "price_and_event": {
            "price_field": "frozen Session 8E Yahoo provider adjclose",
            "provider_certifies_exact_adjustment_semantics": False,
            "provider_events_double_counted": False,
            "certified_security_action_ledger": False,
            "no_event_inference_from": [
                "disappearance",
                "form_family",
                "name_similarity",
                "model_knowledge",
            ],
            "no_unsupported_forward_fill": True,
            "benchmark_gap_policy": "relative_evidence_unavailable_no_imputation",
        },
        "transaction_cost": {
            "rate_per_side": 0.0025,
            "basis_points_per_side": 25,
            "round_trip_basis_points": 50,
            "cost_equation": "0.0025 * absolute actual traded notional",
            "turnover_equation": (
                "sum(abs(trade_notional)) / pre_cost_vintage_nav"
            ),
            "half_turnover_multiplier": False,
        },
        "namespaces": _namespace_contracts(),
        "rates": {
            "dgs1mo_alfred_2026_07_17": {
                "series_id": "DGS1MO",
                "vintage": "2026-07-17",
                "release_time": "16:15 America/New_York on observation date",
                "selection": (
                    "immediately preceding Federal Reserve business day "
                    "released by interval start; no farther lookback"
                ),
                "interval_return": (
                    "(1 + y / 200) ** (2 * d / 365.2425) - 1"
                ),
                "missing_policy": (
                    "Sharpe and Sortino unavailable; no carry, interpolation, "
                    "average, revision, or substitution"
                ),
            },
            "zero_risk_free_sharpe_diagnostic": {
                "risk_free_return": 0.0,
                "diagnostic_only": True,
                "must_not_be_relabelled_as_dgs1mo": True,
            },
        },
        "metrics_for_b1d_not_calculated_here": {
            "single_authoritative_stream": "net monthly NAV",
            "cagr": "(ending_nav / starting_nav) ** (365.2425 / elapsed_days) - 1",
            "volatility": "sample_std(monthly_net_return) * sqrt(12)",
            "sharpe": "mean(monthly_excess) / sample_std(monthly_excess) * sqrt(12)",
            "sortino": (
                "mean(monthly_excess) * 12 / "
                "(sqrt(mean(min(monthly_excess, 0)^2)) * sqrt(12))"
            ),
            "maximum_drawdown": "min(net_nav / running_max(net_nav) - 1)",
            "calmar": "cagr / abs(maximum_drawdown)",
            "active_return": "portfolio_monthly_return - benchmark_monthly_return",
            "tracking_error": "sample_std(active_return) * sqrt(12)",
            "information_ratio": "mean(active_return) / sample_std(active_return) * sqrt(12)",
        },
        "mandatory_metric_disclosures": [
            "holding_coverage",
            "capital_coverage",
            "scenario_imputed_exposure",
            "benchmark_coverage",
            "risk_free_rate_namespace",
        ],
    }


def dgs1mo_request_contract() -> dict[str, Any]:
    return {
        "metadata": {
            "endpoint": DGS1MO_METADATA_ENDPOINT,
            "params": DGS1MO_METADATA_PARAMS,
        },
        "observations": {
            "endpoint": DGS1MO_OBSERVATIONS_ENDPOINT,
            "params": DGS1MO_OBSERVATION_PARAMS,
        },
        "credential_parameter": "api_key",
        "credential_value_recorded": False,
        "response_format": "exact_JSON_response_bytes",
        "aggregation_requested": False,
        "units_transformation": "lin_no_transformation",
    }


def _not_collected_rate_result(reason: str) -> dict[str, Any]:
    request_contract = dgs1mo_request_contract()
    rows = []
    for name in ("metadata", "observations"):
        rows.append(
            {
                "request_name": name,
                "endpoint": request_contract[name]["endpoint"],
                "request_params": request_contract[name]["params"],
                "credential_parameter": "api_key",
                "credential_value_recorded": False,
                "request_made": False,
                "status": reason,
            }
        )
    return {
        "status": reason,
        "dgs1mo_available": False,
        "request_made": False,
        "request_contract": request_contract,
        "requests": rows,
        "observations": None,
        "raw_responses": {},
        "relevant_headers": {},
    }


def acquire_dgs1mo(
    *,
    api_key: str | None,
    timeout_seconds: float = 30.0,
    session: Any | None = None,
) -> dict[str, Any]:
    """Acquire only the exact authorized DGS1MO metadata/observation requests."""
    if not api_key:
        return _not_collected_rate_result(
            "unavailable_not_collected_missing_fred_api_key"
        )
    if session is None:
        import requests

        session = requests.Session()
    specs = {
        "metadata": (DGS1MO_METADATA_ENDPOINT, DGS1MO_METADATA_PARAMS),
        "observations": (
            DGS1MO_OBSERVATIONS_ENDPOINT,
            DGS1MO_OBSERVATION_PARAMS,
        ),
    }
    request_rows: list[dict[str, Any]] = []
    raw: dict[str, bytes] = {}
    headers: dict[str, dict[str, Any]] = {}
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    for name, (endpoint, params) in specs.items():
        started = utc_now()
        try:
            response = session.get(
                endpoint,
                params={**params, "api_key": api_key},
                timeout=timeout_seconds,
            )
            body = bytes(response.content)
            raw[name] = body
            relevant = {
                key: response.headers.get(key)
                for key in (
                    "Content-Type",
                    "Date",
                    "ETag",
                    "Last-Modified",
                    "Cache-Control",
                )
                if response.headers.get(key) is not None
            }
            headers[name] = relevant
            request_rows.append(
                {
                    "request_name": name,
                    "endpoint": endpoint,
                    "request_params": params,
                    "credential_parameter": "api_key",
                    "credential_sha256": key_hash,
                    "credential_value_recorded": False,
                    "request_made": True,
                    "requested_at_utc": started,
                    "completed_at_utc": utc_now(),
                    "http_status": int(response.status_code),
                    "response_size_bytes": len(body),
                    "response_sha256": hashlib.sha256(body).hexdigest(),
                    "relevant_headers": relevant,
                    "status": (
                        "success"
                        if int(response.status_code) == 200
                        else "http_failure_preserved"
                    ),
                }
            )
        except Exception as exc:
            request_rows.append(
                {
                    "request_name": name,
                    "endpoint": endpoint,
                    "request_params": params,
                    "credential_parameter": "api_key",
                    "credential_sha256": key_hash,
                    "credential_value_recorded": False,
                    "request_made": True,
                    "requested_at_utc": started,
                    "completed_at_utc": utc_now(),
                    "status": "transport_failure_preserved",
                    "failure_type": type(exc).__name__,
                    "failure_message": str(exc),
                }
            )
    if len(raw) != 2 or any(row["status"] != "success" for row in request_rows):
        return {
            "status": "unavailable_acquisition_failure_preserved",
            "dgs1mo_available": False,
            "request_made": True,
            "request_contract": dgs1mo_request_contract(),
            "requests": request_rows,
            "observations": None,
            "raw_responses": raw,
            "relevant_headers": headers,
        }

    try:
        metadata_doc = json.loads(raw["metadata"])
        observations_doc = json.loads(raw["observations"])
        series = metadata_doc["seriess"]
        observations = observations_doc["observations"]
        if len(series) != 1 or series[0].get("id") != "DGS1MO":
            raise ValueError("metadata series mismatch")
        metadata = series[0]
        if (
            metadata.get("frequency") != "Daily"
            or metadata.get("units") != "Percent"
            or observations_doc.get("realtime_start") != "2026-07-17"
            or observations_doc.get("realtime_end") != "2026-07-17"
        ):
            raise ValueError("DGS1MO metadata/vintage mismatch")
        frame = pd.DataFrame(observations)
        required_columns = {"realtime_start", "realtime_end", "date", "value"}
        if not required_columns.issubset(frame.columns):
            raise ValueError("observation schema mismatch")
        dates = pd.to_datetime(frame["date"], errors="raise")
        if (
            frame.empty
            or dates.duplicated().any()
            or not dates.is_monotonic_increasing
            or dates.min() < pd.Timestamp("2015-07-01")
            or dates.max() > pd.Timestamp("2026-07-02")
            or not frame["realtime_start"].eq("2026-07-17").all()
            or not frame["realtime_end"].eq("2026-07-17").all()
        ):
            raise ValueError("observation bounds/vintage mismatch")
        frame = frame.rename(columns={"value": "value_raw"})
        frame["value_percent"] = pd.to_numeric(
            frame["value_raw"].replace(".", np.nan), errors="coerce"
        )
        frame["series_id"] = "DGS1MO"
        frame["units"] = metadata["units"]
        frame["frequency"] = metadata["frequency"]
        frame["release_time_policy"] = (
            "16:15 America/New_York on observation date"
        )
        frame["alfred_vintage"] = "2026-07-17"
    except Exception as exc:
        return {
            "status": "unavailable_response_validation_failure_preserved",
            "dgs1mo_available": False,
            "request_made": True,
            "request_contract": dgs1mo_request_contract(),
            "requests": request_rows,
            "observations": None,
            "raw_responses": raw,
            "relevant_headers": headers,
            "validation_failure_type": type(exc).__name__,
            "validation_failure_message": str(exc),
        }
    return {
        "status": "available_exact_alfred_vintage",
        "dgs1mo_available": True,
        "request_made": True,
        "request_contract": dgs1mo_request_contract(),
        "requests": request_rows,
        "observations": frame,
        "raw_responses": raw,
        "relevant_headers": headers,
        "metadata": {
            key: metadata.get(key)
            for key in (
                "id",
                "title",
                "observation_start",
                "observation_end",
                "frequency",
                "frequency_short",
                "units",
                "units_short",
                "seasonal_adjustment",
                "seasonal_adjustment_short",
                "last_updated",
            )
        },
    }


def _write_rate_evidence(
    artifact_root: Path,
    result: Mapping[str, Any],
) -> list[tuple[Path, str]]:
    records: list[tuple[Path, str]] = []
    raw_records: dict[str, dict[str, Any]] = {}
    for name, body in result.get("raw_responses", {}).items():
        path = artifact_root / f"raw/fred/DGS1MO_{name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        records.append((path, "exact_raw_dgs1mo_response"))
        raw_records[name] = {
            "path": path.relative_to(artifact_root).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    request_path = artifact_root / "lineage/request_manifest.jsonl"
    request_path.parent.mkdir(parents=True, exist_ok=True)
    request_path.write_text(
        "".join(
            json.dumps(row, sort_keys=True, default=str) + "\n"
            for row in result["requests"]
        )
    )
    records.append((request_path, "dgs1mo_request_manifest"))

    observations = result.get("observations")
    observation_record = None
    if isinstance(observations, pd.DataFrame):
        path = artifact_root / "outputs/risk_free_observations.parquet"
        observations.to_parquet(path, index=False)
        records.append((path, "exact_dgs1mo_observations"))
        observation_record = {
            "path": path.relative_to(artifact_root).as_posix(),
            "rows": len(observations),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    status = {
        "schema_version": 1,
        "dgs1mo_namespace": "dgs1mo_alfred_2026_07_17",
        "dgs1mo_status": result["status"],
        "dgs1mo_available": result["dgs1mo_available"],
        "request_made": result["request_made"],
        "request_contract": result["request_contract"],
        "requests": result["requests"],
        "relevant_headers": result.get("relevant_headers", {}),
        "raw_responses": raw_records,
        "observations": observation_record,
        "metadata": result.get("metadata"),
        "zero_risk_free_namespace": {
            "namespace": "zero_risk_free_sharpe_diagnostic",
            "available": True,
            "risk_free_return": 0.0,
            "diagnostic_only": True,
            "physically_and_semantically_separate_from_dgs1mo": True,
        },
        "prohibited_substitutions": [
            "current_rate",
            "constant_nonzero_rate",
            "carried_rate",
            "interpolated_rate",
            "alternate_rate_series",
        ],
    }
    for key in ("validation_failure_type", "validation_failure_message"):
        if result.get(key) is not None:
            status[key] = result[key]
    status_path = artifact_root / "support/rate_status.json"
    _write_json(status_path, status)
    records.append((status_path, "rate_status"))
    return records


def _git_head() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def build_free_data_v1_evidence(
    artifact_root: Path,
    *,
    version: str,
    inputs: FrozenInputs = DEFAULT_INPUTS,
    created_at_utc: str | None = None,
    collect_dgs1mo: bool = False,
    fred_api_key: str | None = None,
    request_session: Any | None = None,
) -> Path:
    """Build one non-overwriting B1C evidence artifact."""
    if not VERSION_PATTERN.fullmatch(version):
        raise ValueError("version must be a path-safe identifier ending in -b1c")
    artifact_root = artifact_root.resolve()
    if artifact_root.name != version:
        raise ValueError("artifact root basename must equal version")
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"B1C target is not empty: {artifact_root}")

    preflight = verify_frozen_inputs(inputs)
    market = validate_market_evidence(preflight, inputs)
    requirements, identity, actions, _ = _reconcile_requirements(
        preflight, market
    )
    coverage, benchmark_requirements = _coverage_tables(
        requirements, identity, market
    )
    namespace_frames = _namespace_eligibility(coverage)

    if collect_dgs1mo:
        rate_result = acquire_dgs1mo(
            api_key=fred_api_key,
            session=request_session,
        )
    else:
        rate_result = _not_collected_rate_result(
            "not_collected_build_flag_disabled"
        )

    artifact_root.mkdir(parents=True, exist_ok=True)
    output_records: list[tuple[Path, str]] = []
    input_records: list[dict[str, Any]] = []
    manifest_sources = {
        "p2_manifest.json": inputs.p2_root / "manifest.json",
        "p3_manifest.json": inputs.p3_root / "manifest.json",
        "p4_manifest.json": inputs.p4_root / "manifest.json",
        "session8e_manifest.json": inputs.session8e_root / "manifest.json",
        "s1_manifest.json": inputs.s1_root / "manifest.json",
        "e1_manifest.json": inputs.e1_root / "manifest.json",
        "e1_adjudication_manifest.json": (
            inputs.e1_adjudication_root / "manifest.json"
        ),
    }
    for name, source in manifest_sources.items():
        target = artifact_root / "inputs" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        if sha256_file(target) != sha256_file(source):
            raise RuntimeError(f"frozen manifest copy mismatch: {name}")
        output_records.append((target, "frozen_upstream_manifest_copy"))
        input_records.append(
            {
                "source_path": source.relative_to(ROOT).as_posix(),
                "copied_path": target.relative_to(artifact_root).as_posix(),
                "size_bytes": source.stat().st_size,
                "sha256": sha256_file(source),
            }
        )
    references_path = artifact_root / "inputs/upstream_hash_references.json"
    _write_json(
        references_path,
        {
            "schema_version": 1,
            "manifests": input_records,
            "consumed_records": preflight["validated_records"],
            "frozen_upstream_code_lineage": (
                "preserved_inside_exact_manifest_copies"
            ),
            "b1c_assembler_code_lineage": "current_post_p4_worktree",
            "session8e_rebuilt_or_reinterpreted": False,
        },
    )
    output_records.append((references_path, "upstream_hash_references"))

    contract_path = (
        artifact_root
        / "contracts/free_data_v1_performance_contract.json"
    )
    _write_json(contract_path, performance_contract())
    output_records.append((contract_path, "frozen_b1b_performance_contract"))
    for namespace, contract in _namespace_contracts().items():
        path = artifact_root / f"contracts/namespaces/{namespace}.json"
        _write_json(path, {"namespace": namespace, **contract})
        output_records.append((path, "physically_separate_namespace_contract"))
    rate_contract = performance_contract()["rates"]
    for namespace in RATE_NAMESPACES:
        path = artifact_root / f"contracts/rates/{namespace}.json"
        _write_json(path, {"namespace": namespace, **rate_contract[namespace]})
        output_records.append((path, "physically_separate_rate_contract"))

    outputs = {
        "requirements.parquet": (requirements, "p4_requirement_master"),
        "security_identity.parquet": (
            identity,
            "s1_identity_reconciliation",
        ),
        "security_actions.parquet": (
            actions,
            "s1_e1_action_reconciliation",
        ),
        "benchmark_requirements.parquet": (
            benchmark_requirements,
            "benchmark_requirement_reconciliation",
        ),
        "coverage.parquet": (coverage, "requirement_coverage"),
    }
    for name, (frame, role) in outputs.items():
        path = artifact_root / "outputs" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        output_records.append((path, role))

    prices = pd.concat(
        [market["price_frames"][symbol] for symbol in market["relevant_symbols"]],
        ignore_index=True,
    ).sort_values(["symbol", "market_close"]).reset_index(drop=True)
    price_path = artifact_root / "outputs/prices.parquet"
    prices.to_parquet(price_path, index=False)
    output_records.append((price_path, "validated_frozen_session8e_prices"))
    price_inventory_path = artifact_root / "support/price_inventory.json"
    _write_json(
        price_inventory_path,
        market["price_lineage"].to_dict(orient="records"),
    )
    output_records.append((price_inventory_path, "hash_addressed_price_inventory"))

    for namespace, frame in namespace_frames.items():
        path = (
            artifact_root
            / f"outputs/namespaces/{namespace}/eligibility.parquet"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
        output_records.append((path, "physically_separate_namespace_eligibility"))

    coverage_summary = {
        "schema_version": 1,
        "requirement_rows": 184,
        "holding_rows": 180,
        "benchmark_master_rows": 4,
        "matured_2015_2023_holding_rows": 135,
        "open_2024_2026_holding_rows": 45,
        "s1_fully_matched_requirements": 0,
        "s1_ambiguous_matured_holdings": 135,
        "s1_unsupported_open_holdings": 45,
        "unsupported_incomplete_benchmark_masters": 4,
        "primary_document_supported_but_incomplete_current_holdings": 14,
        "sstk_retrieved_primary_document_claims": 0,
        "validated_holding_yahoo_symbols": 132,
        "validated_benchmark_yahoo_symbols": 4,
        "validated_total_yahoo_symbols": 136,
        "common_entry_holding_rows": int(
            coverage.loc[
                coverage["instrument_role"].eq("holding"),
                "entry_observed_common",
            ].fillna(False).sum()
        ),
        "matured_common_exit_holding_rows": int(
            coverage["exit_observed_common"].fillna(False).sum()
        ),
        "missing_common_month_end_observations": int(
            pd.to_numeric(
                coverage["missing_common_month_end_count"], errors="coerce"
            ).fillna(0).sum()
        ),
        "benchmark_gaps": int(coverage["benchmark_gap_count"].sum()),
        "provider_adjclose_semantics_certified": False,
        "performance_calculated": False,
        "nav_created": False,
    }
    summary_path = artifact_root / "support/coverage_summary.json"
    _write_json(summary_path, coverage_summary)
    output_records.append((summary_path, "coverage_summary"))
    output_records.extend(_write_rate_evidence(artifact_root, rate_result))

    current_code_paths = [
        ROOT / "backtest/free_data_v1_evidence.py",
        ROOT / "workflows/build_free_data_v1_evidence.py",
        ROOT / "tests/backtest/test_free_data_v1_evidence.py",
    ]
    code_lineage = [
        {
            "path": path.relative_to(ROOT).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
            "lineage_class": "current_post_p4_b1c_code",
        }
        for path in current_code_paths
        if path.is_file()
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": "FREE_DATA_V1_PERFORMANCE_INPUT_EVIDENCE_B1C",
        "version": version,
        "created_at_utc": created_at_utc or utc_now(),
        "build_mode": "offline_frozen_evidence_assembly",
        "current_head": _git_head(),
        "code_lineage": code_lineage,
        "lineage_distinction": {
            "upstream": "frozen_as_recorded_in_exact_manifest_copies",
            "assembler": "current_post_p4_worktree",
            "session8e_rebuilt_with_current_builder": False,
        },
        "validated_inputs": input_records,
        "records": [
            _record(artifact_root, path, role)
            for path, role in sorted(
                output_records, key=lambda item: item[0].as_posix()
            )
        ],
        "coverage": coverage_summary,
        "rate_status": {
            "dgs1mo": rate_result["status"],
            "zero_risk_free_diagnostic": "available_separate_namespace",
        },
        "claim": {
            "b1c_evidence_assembly_complete": True,
            "requirements_reconciled": True,
            "all_136_yahoo_payloads_validated": True,
            "security_action_ledger_certified": False,
            "provider_adjustment_semantics_certified": False,
            "performance_calculated": False,
            "nav_created": False,
            "backtest_run": False,
            "portfolio_changed": False,
            "model_executed": False,
        },
        "limitations": [
            "Yahoo is a frozen free-provider payload, not an official exchange feed.",
            "Yahoo does not certify the exact adjusted-close corporate-action semantics.",
            "S1 has zero fully matched dated security/action requirements.",
            "E1 resolves no historical or current security action.",
            "Open 2024-2026 holdings have not reached their 36-month exits.",
            "No NAV, return, performance metric, or backtest is present.",
        ],
    }
    manifest_path = artifact_root / "manifest.json"
    _write_json(manifest_path, manifest)
    return manifest_path


def preflight_summary(
    inputs: FrozenInputs = DEFAULT_INPUTS,
) -> dict[str, Any]:
    """Run the complete read-only local preflight and return compact counts."""
    preflight = verify_frozen_inputs(inputs)
    market = validate_market_evidence(preflight, inputs)
    requirements, identity, _, _ = _reconcile_requirements(preflight, market)
    coverage, _ = _coverage_tables(requirements, identity, market)
    return {
        "result": "pass",
        "requirements": len(requirements),
        "holding_requirements": int(
            requirements["instrument_role"].eq("holding").sum()
        ),
        "benchmark_requirements": int(
            requirements["instrument_role"].eq("benchmark").sum()
        ),
        "matured_holdings": int(
            requirements["requirement_state"].eq("matured_2015_2023").sum()
        ),
        "open_holdings": int(
            requirements["requirement_state"].eq("open_2024_2026").sum()
        ),
        "validated_yahoo_symbols": len(market["relevant_symbols"]),
        "common_entries": int(
            coverage.loc[
                coverage["instrument_role"].eq("holding"),
                "entry_observed_common",
            ].fillna(False).sum()
        ),
        "matured_common_exits": int(
            coverage["exit_observed_common"].fillna(False).sum()
        ),
        "missing_common_month_ends": int(
            pd.to_numeric(
                coverage["missing_common_month_end_count"], errors="coerce"
            ).fillna(0).sum()
        ),
        "performance_calculated": False,
        "external_request_made": False,
    }
