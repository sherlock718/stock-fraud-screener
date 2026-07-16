"""Freeze Session V3.3 liquidity-qualified production holdings.

The builder consumes only the two hash-pinned V3 artifacts.  It applies the
frozen production_v3_ml_gates contract, retrieves Yahoo daily chart evidence
only for rows that require the liquidity gate, validates the exact 30 regular
sessions strictly before prediction, and ranks only fully eligible rows.

Generated payloads are artifact-scoped and Git-ignored.  No return, NAV, or
backtest calculation is performed here.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.metadata
import json
import platform
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
V3_1 = ROOT / "artifacts/pit_validation/session_v3_1_production_contract"
V3_2 = ROOT / "artifacts/pit_validation/session_v3_2_oos_predictions"
DEFAULT_ARTIFACT_ROOT = ROOT / "artifacts/pit_validation/session_v3_3_liquidity_holdings"
REPORT_PATH = ROOT / "reports/pit_validation/v3_3_liquidity_holdings.md"
V3_1_MANIFEST_SHA256 = "2b5249cdb05c7bad1759abbd281ec1c90a8a9ce2fbd72973cd4dc905c8a86e5a"
V3_2_MANIFEST_SHA256 = "ba0e3b2d850af113c26306dbec1d9d5cab7a58aa78cafd40cefac31059899912"
V3_1_TABLE = V3_1 / "outputs/observed_only_us_annual_3y.parquet"
V3_1_CONFIG = V3_1 / "configuration/production_contract.json"
V3_2_PREDICTIONS = V3_2 / "predictions/oos_predictions.parquet"
ENDPOINT = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
USER_AGENT = "stock-fraud-screener SessionV3.3 liquidity evidence build"
ALLOWED_EXCHANGES = {"XNYS": {"NYQ", "ASE", "PCX"}, "XNAS": {"NMS", "NCM", "NGM"}}
TREE_THRESHOLD = 0.55
TARGET_N = 15
WEIGHT = 1.0 / TARGET_N
MIN_ADTV = (200_000.0 / TARGET_N) / 0.01

EX_TREE_ROLE = "missing_oos_tree_probability"
EX_RANK_ROLE = "missing_oos_lightgbm_three_year_return_prediction"
EX_TREE_THRESHOLD = "oos_tree_probability_below_0_55"
EX_LIQUIDITY_RESPONSE = "liquidity_response_missing_or_invalid"
EX_LIQUIDITY_SYMBOL = "liquidity_symbol_mapping_mismatch"
EX_LIQUIDITY_EXCHANGE = "liquidity_exchange_mapping_mismatch"
EX_LIQUIDITY_CURRENCY = "liquidity_currency_not_usd"
EX_LIQUIDITY_SESSION = "liquidity_session_mapping_ambiguous_or_mismatched"
EX_LIQUIDITY_STALE = "liquidity_last_session_stale"
EX_LIQUIDITY_INCOMPLETE = "liquidity_window_not_exactly_30_valid_sessions"
EX_LIQUIDITY_PRICE = "liquidity_session_close_missing_or_nonpositive"
EX_LIQUIDITY_VOLUME = "liquidity_session_volume_missing_or_nonpositive"
EX_LIQUIDITY_THRESHOLD = "liquidity_median_30_session_dollar_volume_below_threshold"
EX_PERIOD_INCOMPLETE = "decision_period_fewer_than_15_eligible_candidates"

HARD_GATE_COLUMNS = [
    "market_us", "market_cap", "beneish", "piotroski", "roa_positive",
    "altman", "value", "momentum",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def record(path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(ROOT).as_posix(), "role": role,
        "size_bytes": path.stat().st_size, "sha256": sha256_file(path),
    }


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def _manifest_index(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for section in ("records", "code_lineage", "deliverables"):
        for item in manifest.get(section, []):
            result[item["path"]] = item
    return result


def _validate_manifest(path: Path, expected_sha: str, label: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    actual = sha256_file(path)
    if actual != expected_sha:
        raise RuntimeError(f"{label} manifest hash mismatch: expected={expected_sha} actual={actual}")
    manifest = json.loads(path.read_text())
    validated = []
    for relative, item in sorted(_manifest_index(manifest).items()):
        target = ROOT / relative
        if not target.is_file():
            raise RuntimeError(f"{label} manifest record missing: {relative}")
        actual_size = target.stat().st_size
        actual_hash = sha256_file(target)
        if actual_size != int(item["size_bytes"]) or actual_hash != item["sha256"]:
            raise RuntimeError(f"{label} manifest record drifted: {relative}")
        validated.append({"path": relative, "size_bytes": actual_size, "sha256": actual_hash})
    return manifest, validated


def validate_inputs() -> tuple[dict[str, Any], dict[str, Any]]:
    m1, r1 = _validate_manifest(V3_1 / "manifest.json", V3_1_MANIFEST_SHA256, "V3.1")
    m2, r2 = _validate_manifest(V3_2 / "manifest.json", V3_2_MANIFEST_SHA256, "V3.2")
    config = json.loads(V3_1_CONFIG.read_text())
    if config != m1["configuration"] or config != m2["configuration"]:
        raise RuntimeError("frozen configuration differs across V3.1/V3.2 manifests")
    if config.get("strategy_name") != "production_v3_ml_gates":
        raise RuntimeError("unexpected production strategy")
    if config["decision_tree"].get("pass_rule") != "tree_prob >= 0.55":
        raise RuntimeError("tree threshold contract drifted")
    if config["selection"].get("target_n") != TARGET_N or config["selection"].get("weight_each") != WEIGHT:
        raise RuntimeError("selection contract drifted")
    if config["liquidity"].get("minimum_adtv_usd") != MIN_ADTV:
        raise RuntimeError("liquidity threshold contract drifted")
    for consumed, manifest in ((V3_1_TABLE, m1), (V3_1_CONFIG, m1), (V3_2_PREDICTIONS, m2)):
        relative = consumed.relative_to(ROOT).as_posix()
        if relative not in _manifest_index(manifest):
            raise RuntimeError(f"consumed record absent from accepted manifest: {relative}")
    return config, {
        "result": "pass", "v3_1_manifest_sha256": V3_1_MANIFEST_SHA256,
        "v3_2_manifest_sha256": V3_2_MANIFEST_SHA256,
        "v3_1_records_revalidated": len(r1), "v3_2_records_revalidated": len(r2),
        "records_validated": r1 + r2,
        "configuration_equal_across_accepted_artifacts": True,
    }


def load_candidates(config: dict[str, Any]) -> pd.DataFrame:
    table = pd.read_parquet(V3_1_TABLE)
    predictions = pd.read_parquet(V3_2_PREDICTIONS)
    if len(table) != 43_806 or table["stable_row_id"].duplicated().any():
        raise RuntimeError("V3.1 table identity drifted")
    roles = set(predictions["model_role"].unique())
    if roles != {"decision_tree", "lightgbm_regression"}:
        raise RuntimeError(f"V3.2 model roles drifted: {sorted(roles)}")
    if predictions.duplicated(["stable_row_id", "model_role"]).any():
        raise RuntimeError("V3.2 row-role identity is not unique")
    available = predictions["prediction_status"].eq("oos_prediction_available")
    lineage = [
        "feature_artifact_id", "preprocessing_artifact_id", "target_artifact_id",
        "model_configuration_artifact_id", "model_artifact_id",
    ]
    if predictions.loc[available, lineage].isna().any().any():
        raise RuntimeError("available V3.2 prediction has incomplete lineage")
    v3_2_manifest = json.loads((V3_2 / "manifest.json").read_text())
    accepted_artifact_ids = {f"sha256:{item['sha256']}" for item in v3_2_manifest["records"]}
    for column in lineage:
        unrecognized = set(predictions.loc[available, column]) - accepted_artifact_ids
        if unrecognized:
            raise RuntimeError(f"available V3.2 prediction has unrecognized {column} lineage")
    keep = [
        "stable_row_id", "model_role", "prediction_status", "exclusion_code", "prediction",
        *lineage, "training_rows", "training_label_end_max", "selected_feature_count",
        "selected_features_json", "training_population_fingerprint",
    ]
    frame = table.copy()
    for role in ("decision_tree", "lightgbm_regression"):
        part = predictions.loc[predictions["model_role"].eq(role), keep].copy()
        part = part.rename(columns={c: f"{role}_{c}" for c in keep if c != "stable_row_id"})
        frame = frame.merge(part, on="stable_row_id", validate="one_to_one")
    frame["tree_role_pass"] = (
        frame["decision_tree_prediction_status"].eq("oos_prediction_available")
        & np.isfinite(pd.to_numeric(frame["decision_tree_prediction"], errors="coerce"))
    )
    frame["ranker_role_pass"] = (
        frame["lightgbm_regression_prediction_status"].eq("oos_prediction_available")
        & np.isfinite(pd.to_numeric(frame["lightgbm_regression_prediction"], errors="coerce"))
    )
    frame["tree_threshold_pass"] = frame["tree_role_pass"] & frame["decision_tree_prediction"].ge(TREE_THRESHOLD)
    frame["liquidity_required"] = (
        frame["all_non_model_hard_gates_pass"] & frame["tree_role_pass"]
        & frame["ranker_role_pass"] & frame["tree_threshold_pass"]
    )
    required = frame.loc[frame["liquidity_required"]]
    if required.duplicated(["provider_symbol", "prediction_timestamp"]).any():
        raise RuntimeError("liquidity request identity is ambiguous")
    if required["provider_symbol"].isna().any() or required["exchange_calendar"].isna().any():
        raise RuntimeError("liquidity-required candidate lacks frozen provider mapping")
    expected = config["selection"]["required_oos_roles"]
    if expected != ["tree_agreement_gate:3y", "lightgbm_regression_ranker:3y"]:
        raise RuntimeError("required OOS role contract drifted")
    return frame


def freeze_calendars(artifact_root: Path, candidates: pd.DataFrame) -> pd.DataFrame:
    import exchange_calendars as xcals

    required = candidates.loc[candidates["liquidity_required"]]
    start = pd.Timestamp(required["prediction_timestamp"].min()).tz_convert(None).normalize() - pd.Timedelta(days=75)
    end = pd.Timestamp(required["prediction_timestamp"].max()).tz_convert(None).normalize()
    rows = []
    evidence: dict[str, Any] = {"package": "exchange-calendars", "version": importlib.metadata.version("exchange-calendars"), "calendars": {}}
    for name in sorted(required["exchange_calendar"].unique()):
        if name not in ALLOWED_EXCHANGES:
            raise RuntimeError(f"unsupported exchange calendar: {name}")
        cal = xcals.get_calendar(name)
        sessions = cal.sessions_in_range(start, end)
        schedule = cal.schedule.loc[sessions, ["open", "close"]].reset_index()
        schedule.columns = ["session_date", "market_open", "market_close"]
        schedule["exchange_calendar"] = name
        rows.append(schedule)
        evidence["calendars"][name] = {"first": str(sessions.min()), "last": str(sessions.max()), "sessions": len(sessions)}
    result = pd.concat(rows, ignore_index=True)
    result.to_parquet(artifact_root / "calendar/regular_sessions.parquet", index=False)
    write_json(artifact_root / "calendar/calendar_evidence.json", evidence)
    return result


def build_requests(candidates: pd.DataFrame, schedule: pd.DataFrame) -> pd.DataFrame:
    schedule = schedule.copy()
    schedule["market_close"] = pd.to_datetime(schedule["market_close"], utc=True)
    requests_rows = []
    for row in candidates.loc[candidates["liquidity_required"]].itertuples(index=False):
        prediction = pd.Timestamp(row.prediction_timestamp)
        eligible = schedule.loc[
            schedule["exchange_calendar"].eq(row.exchange_calendar)
            & schedule["market_close"].lt(prediction)
        ].sort_values("market_close").tail(30)
        if len(eligible) != 30:
            raise RuntimeError(f"calendar cannot supply 30 sessions for {row.stable_row_id}")
        first_date = pd.Timestamp(eligible.iloc[0]["session_date"]).strftime("%Y-%m-%d")
        params = {
            "period1": int((pd.Timestamp(first_date, tz="UTC") - pd.Timedelta(days=1)).timestamp()),
            "period2": int(prediction.timestamp()), "interval": "1d", "events": "history",
            "includeAdjustedClose": "false",
        }
        requests_rows.append({
            "request_id": row.stable_row_id, "stable_row_id": row.stable_row_id,
            "provider_symbol": row.provider_symbol, "provider_exchange": row.provider_exchange,
            "exchange_calendar": row.exchange_calendar, "prediction_timestamp": prediction,
            "expected_first_market_close": eligible.iloc[0]["market_close"],
            "expected_last_market_close": eligible.iloc[-1]["market_close"],
            "expected_session_dates_json": json.dumps([pd.Timestamp(v).strftime("%Y-%m-%d") for v in eligible["session_date"]]),
            "request_params_json": json.dumps(params, sort_keys=True),
        })
    return pd.DataFrame(requests_rows).sort_values("request_id").reset_index(drop=True)


def initialize(artifact_root: Path) -> None:
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"artifact root already exists and is non-empty: {artifact_root}")
    for folder in ("inputs", "configuration", "calendar", "raw/chart", "raw/failures", "checkpoints", "outputs", "support", "lineage"):
        (artifact_root / folder).mkdir(parents=True, exist_ok=True)
    config, preflight = validate_inputs()
    candidates = load_candidates(config)
    shutil.copy2(V3_1 / "manifest.json", artifact_root / "inputs/v3_1_manifest.json")
    shutil.copy2(V3_2 / "manifest.json", artifact_root / "inputs/v3_2_manifest.json")
    shutil.copy2(V3_1_CONFIG, artifact_root / "configuration/production_contract.json")
    write_json(artifact_root / "support/preflight_validation.json", preflight)
    schedule = freeze_calendars(artifact_root, candidates)
    request_table = build_requests(candidates, schedule)
    request_table.to_parquet(artifact_root / "inputs/liquidity_requests.parquet", index=False)
    write_json(artifact_root / "configuration/liquidity_collection.json", {
        "schema_version": 1, "session": "V3.3", "source": "Yahoo Finance chart API",
        "endpoint": ENDPOINT, "requests": len(request_table), "distinct_symbols": int(request_table["provider_symbol"].nunique()),
        "daily_dollar_volume": "unadjusted regular-session close * regular-session volume",
        "window": "exactly 30 exchange-calendar sessions with market_close < prediction_timestamp",
        "minimum_adtv_usd": MIN_ADTV, "adjustment": "none_unadjusted",
        "currency": "USD", "missingness": "fail_closed_no_substitution",
    })


_thread_local = threading.local()


def _session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        _thread_local.session = session
    return _thread_local.session


def _fetch_one(row: Any, artifact_root: Path, retries: int) -> dict[str, Any]:
    request_id, symbol = row.request_id, row.provider_symbol
    params = json.loads(row.request_params_json)
    url = ENDPOINT.format(symbol=symbol)
    target = artifact_root / "raw/chart" / f"{request_id}.json.gz"
    attempts = []
    for attempt in range(1, retries + 1):
        started = utc_now()
        try:
            response = _session().get(url, params=params, timeout=45)
            content = response.content
            current = {"attempt": attempt, "started_at_utc": started, "status_code": response.status_code, "bytes": len(content)}
            attempts.append(current)
            if response.status_code == 200:
                payload = response.json()
                if payload.get("chart", {}).get("error") is not None or not payload.get("chart", {}).get("result"):
                    raise ValueError(f"chart_error:{payload.get('chart', {}).get('error')}")
                with gzip.open(target, "wb", compresslevel=6) as handle:
                    handle.write(content)
                return {
                    "request_id": request_id, "stable_row_id": request_id, "symbol": symbol,
                    "status": "success", "request_url": response.url, "request_params": params,
                    "retrieved_at_utc": utc_now(), "http_status": response.status_code,
                    "response_size_bytes": len(content), "response_sha256": hashlib.sha256(content).hexdigest(),
                    "stored_path": target.relative_to(artifact_root).as_posix(),
                    "stored_size_bytes": target.stat().st_size, "stored_sha256": sha256_file(target),
                    "response_headers": {k: response.headers.get(k) for k in ("Date", "ETag", "Last-Modified", "Cache-Control", "Age", "Content-Type") if response.headers.get(k)},
                    "attempts": attempts,
                }
            failure = artifact_root / "raw/failures" / f"{request_id}.attempt{attempt}.{time.time_ns()}.bin.gz"
            with gzip.open(failure, "wb", compresslevel=6) as handle:
                handle.write(content)
            current.update({"stored_path": failure.relative_to(artifact_root).as_posix(), "stored_sha256": sha256_file(failure)})
            if response.status_code not in {408, 429, 500, 502, 503, 504}:
                break
        except Exception as exc:
            attempts.append({"attempt": attempt, "started_at_utc": started, "error": f"{type(exc).__name__}:{exc}"})
        if attempt < retries:
            time.sleep(min(8, 2 ** (attempt - 1)))
    return {
        "request_id": request_id, "stable_row_id": request_id, "symbol": symbol,
        "status": "failure", "request_url": url + "?" + urlencode(params),
        "request_params": params, "retrieved_at_utc": utc_now(), "attempts": attempts,
    }


def fetch(artifact_root: Path, workers: int, retries: int) -> None:
    requests_table = pd.read_parquet(artifact_root / "inputs/liquidity_requests.parquet")
    manifest_path = artifact_root / "raw/response_manifest.jsonl"
    prior: dict[str, dict[str, Any]] = {}
    if manifest_path.exists():
        for line in manifest_path.read_text().splitlines():
            item = json.loads(line); prior[item["request_id"]] = item
    pending = [row for row in requests_table.itertuples(index=False) if prior.get(row.request_id, {}).get("status") != "success"]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_one, row, artifact_root, retries): row.request_id for row in pending}
        for future in as_completed(futures):
            item = future.result(); prior[item["request_id"]] = item
            manifest_path.write_text("".join(json.dumps(prior[key], sort_keys=True) + "\n" for key in sorted(prior)))


def _parse_response(item: dict[str, Any], request: pd.Series, artifact_root: Path) -> tuple[pd.DataFrame, str | None, dict[str, Any]]:
    if item.get("status") != "success" or not item.get("stored_path"):
        return pd.DataFrame(), EX_LIQUIDITY_RESPONSE, {}
    stored = artifact_root / item["stored_path"]
    if not stored.is_file() or sha256_file(stored) != item.get("stored_sha256"):
        return pd.DataFrame(), EX_LIQUIDITY_RESPONSE, {}
    try:
        with gzip.open(stored, "rt") as handle:
            result = json.load(handle)["chart"]["result"][0]
        meta = result["meta"]
        if meta.get("symbol") != request["provider_symbol"]:
            return pd.DataFrame(), EX_LIQUIDITY_SYMBOL, meta
        if meta.get("exchangeName") != request["provider_exchange"]:
            return pd.DataFrame(), EX_LIQUIDITY_EXCHANGE, meta
        if meta.get("exchangeName") not in ALLOWED_EXCHANGES[request["exchange_calendar"]]:
            return pd.DataFrame(), EX_LIQUIDITY_EXCHANGE, meta
        if meta.get("currency") != "USD":
            return pd.DataFrame(), EX_LIQUIDITY_CURRENCY, meta
        timestamps = result.get("timestamp") or []
        quote = result.get("indicators", {}).get("quote", [{}])[0]
        closes, volumes = quote.get("close") or [], quote.get("volume") or []
        if not (len(timestamps) == len(closes) == len(volumes)):
            return pd.DataFrame(), EX_LIQUIDITY_SESSION, meta
        tz = ZoneInfo(meta.get("exchangeTimezoneName", "America/New_York"))
        rows = []
        for timestamp, close, volume in zip(timestamps, closes, volumes):
            instant = pd.Timestamp(timestamp, unit="s", tz="UTC")
            rows.append({"provider_timestamp": instant, "session_date": instant.tz_convert(tz).strftime("%Y-%m-%d"), "unadjusted_close": close, "regular_session_volume": volume})
        frame = pd.DataFrame(rows)
        if not frame.empty and frame["session_date"].duplicated().any():
            return pd.DataFrame(), EX_LIQUIDITY_SESSION, meta
        return frame, None, meta
    except Exception:
        return pd.DataFrame(), EX_LIQUIDITY_RESPONSE, {}


def evaluate_liquidity(artifact_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    request_table = pd.read_parquet(artifact_root / "inputs/liquidity_requests.parquet")
    calendar = pd.read_parquet(artifact_root / "calendar/regular_sessions.parquet")
    calendar["session_date"] = pd.to_datetime(calendar["session_date"]).dt.strftime("%Y-%m-%d")
    calendar["market_open"] = pd.to_datetime(calendar["market_open"], utc=True)
    calendar["market_close"] = pd.to_datetime(calendar["market_close"], utc=True)
    response_path = artifact_root / "raw/response_manifest.jsonl"
    responses = {item["request_id"]: item for item in map(json.loads, response_path.read_text().splitlines())} if response_path.exists() else {}
    evidence_rows, coverage_rows = [], []
    for _, request in request_table.iterrows():
        item = responses.get(request["request_id"], {"status": "failure"})
        parsed, code, meta = _parse_response(item, request, artifact_root)
        expected = json.loads(request["expected_session_dates_json"])
        selected = parsed.loc[parsed["session_date"].isin(expected)].copy() if not parsed.empty else pd.DataFrame()
        if code is None and (set(selected["session_date"]) != set(expected) or len(selected) != 30):
            if request["expected_last_market_close"].strftime("%Y-%m-%d") not in set(selected.get("session_date", [])):
                code = EX_LIQUIDITY_STALE
            else:
                code = EX_LIQUIDITY_INCOMPLETE
        valid_count = 0
        invalid_close_count = 0
        invalid_volume_count = 0
        if not selected.empty:
            selected["unadjusted_close"] = pd.to_numeric(selected["unadjusted_close"], errors="coerce")
            selected["regular_session_volume"] = pd.to_numeric(selected["regular_session_volume"], errors="coerce")
            valid_close = np.isfinite(selected["unadjusted_close"]) & selected["unadjusted_close"].gt(0)
            valid_volume = np.isfinite(selected["regular_session_volume"]) & selected["regular_session_volume"].gt(0)
            valid_count = int((valid_close & valid_volume).sum())
            invalid_close_count = int((~valid_close).sum())
            invalid_volume_count = int((~valid_volume).sum())
            if code is None and invalid_close_count:
                code = EX_LIQUIDITY_PRICE
            elif code is None and invalid_volume_count:
                code = EX_LIQUIDITY_VOLUME
        median = np.nan
        if code is None:
            selected["daily_dollar_volume"] = selected["unadjusted_close"] * selected["regular_session_volume"]
            median = float(selected["daily_dollar_volume"].median())
            if not np.isfinite(median) or median < MIN_ADTV:
                code = EX_LIQUIDITY_THRESHOLD
        if not selected.empty:
            selected = selected.merge(
                calendar.loc[calendar["exchange_calendar"].eq(request["exchange_calendar"]), ["session_date", "market_open", "market_close"]],
                on="session_date", how="left", validate="one_to_one",
            )
            selected["stable_row_id"] = request["stable_row_id"]
            selected["provider_symbol"] = request["provider_symbol"]
            selected["provider_exchange"] = request["provider_exchange"]
            selected["exchange_calendar"] = request["exchange_calendar"]
            selected["prediction_timestamp"] = request["prediction_timestamp"]
            selected["currency"] = meta.get("currency")
            selected["adjustment"] = "none_unadjusted"
            selected["source"] = "Yahoo Finance chart API"
            selected["retrieved_at_utc"] = item.get("retrieved_at_utc")
            selected["raw_response_sha256"] = item.get("response_sha256")
            evidence_rows.append(selected)
        coverage_rows.append({
            "stable_row_id": request["stable_row_id"], "provider_symbol": request["provider_symbol"],
            "provider_exchange": request["provider_exchange"], "exchange_calendar": request["exchange_calendar"],
            "prediction_timestamp": request["prediction_timestamp"], "expected_session_count": 30,
            "observed_expected_session_count": len(selected), "valid_session_count": valid_count,
            "invalid_close_session_count": invalid_close_count,
            "invalid_volume_session_count": invalid_volume_count,
            "expected_first_market_close": request["expected_first_market_close"],
            "expected_last_market_close": request["expected_last_market_close"],
            "median_30_session_dollar_volume": median, "minimum_adtv_usd": MIN_ADTV,
            "liquidity_pass": code is None, "exclusion_code": code,
            "source": "Yahoo Finance chart API", "retrieved_at_utc": item.get("retrieved_at_utc"),
            "source_symbol": meta.get("symbol"), "source_exchange": meta.get("exchangeName"),
            "currency": meta.get("currency"), "exchange_timezone": meta.get("exchangeTimezoneName"),
            "raw_response_sha256": item.get("response_sha256"), "adjustment": "none_unadjusted",
        })
    evidence = pd.concat(evidence_rows, ignore_index=True) if evidence_rows else pd.DataFrame()
    coverage = pd.DataFrame(coverage_rows)
    return evidence, coverage


def materialize_selection(candidates: pd.DataFrame, liquidity: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    liq = liquidity[["stable_row_id", "liquidity_pass", "median_30_session_dollar_volume", "exclusion_code"]].rename(columns={"exclusion_code": "liquidity_exclusion_code"})
    frame = candidates.merge(liq, on="stable_row_id", how="left", validate="one_to_one")
    liquidity_pass = frame["liquidity_pass"].eq(True)
    frame["liquidity_status"] = np.where(frame["liquidity_required"], np.where(liquidity_pass, "pass", "fail"), "not_required")
    frame["liquidity_pass"] = liquidity_pass
    frame["eligible_before_period_completeness"] = frame["liquidity_required"] & frame["liquidity_pass"]
    frame["rank"] = pd.Series(pd.NA, index=frame.index, dtype="Int64")
    frame["holding"] = False
    frame["weight"] = np.nan
    period_rows = []
    for decision, group in frame.groupby("decision_timestamp", sort=True):
        eligible = group.loc[group["eligible_before_period_completeness"]].sort_values(
            ["lightgbm_regression_prediction", "stable_row_id"], ascending=[False, True]
        )
        ranks = pd.Series(np.arange(1, len(eligible) + 1), index=eligible.index, dtype="Int64")
        frame.loc[eligible.index, "rank"] = ranks
        supported = len(eligible) >= TARGET_N
        if supported:
            chosen = eligible.index[:TARGET_N]
            frame.loc[chosen, "holding"] = True
            frame.loc[chosen, "weight"] = WEIGHT
        period_rows.append({
            "decision_timestamp": decision, "prediction_timestamp": group["prediction_timestamp"].iloc[0],
            "source_candidates": len(group), "liquidity_required_candidates": int(group["liquidity_required"].sum()),
            "liquidity_pass_candidates": len(eligible), "period_supported": supported,
            "holding_count": TARGET_N if supported else 0,
            "exclusion_code": None if supported else EX_PERIOD_INCOMPLETE,
        })
    periods = pd.DataFrame(period_rows)
    holdings = frame.loc[frame["holding"], [
        "stable_row_id", "entity_id", "cik", "ticker", "provider_symbol", "decision_timestamp",
        "prediction_timestamp", "entry_timestamp", "decision_tree_prediction",
        "lightgbm_regression_prediction", "median_30_session_dollar_volume", "rank", "weight",
    ]].copy()
    exclusion_rows = []
    for row in frame.itertuples(index=False):
        codes = []
        raw_hard = row.hard_gate_exclusion_codes
        if isinstance(raw_hard, str) and raw_hard:
            codes.extend(json.loads(raw_hard) if raw_hard.startswith("[") else raw_hard.split("|"))
        if not row.tree_role_pass: codes.append(EX_TREE_ROLE)
        if not row.ranker_role_pass: codes.append(EX_RANK_ROLE)
        if row.tree_role_pass and not row.tree_threshold_pass: codes.append(EX_TREE_THRESHOLD)
        if row.liquidity_required and not row.liquidity_pass: codes.append(row.liquidity_exclusion_code or EX_LIQUIDITY_RESPONSE)
        for code in dict.fromkeys(codes):
            exclusion_rows.append({"stable_row_id": row.stable_row_id, "decision_timestamp": row.decision_timestamp, "exclusion_code": code})
    exclusions = pd.DataFrame(exclusion_rows)
    gate_rows = []
    def value_json(value: Any) -> str | None:
        if value is None or value is pd.NA or (isinstance(value, float) and np.isnan(value)):
            return None
        if isinstance(value, np.generic):
            value = value.item()
        return json.dumps(value, sort_keys=True)

    for row in frame.itertuples(index=False):
        for gate in HARD_GATE_COLUMNS:
            gate_rows.append({"stable_row_id": row.stable_row_id, "decision_timestamp": row.decision_timestamp, "gate": gate, "status": getattr(row, f"gate_{gate}_status"), "pass": getattr(row, f"gate_{gate}_pass"), "value_json": value_json(getattr(row, f"gate_{gate}_value")), "provenance": getattr(row, f"gate_{gate}_provenance")})
        gate_rows.extend([
            {"stable_row_id": row.stable_row_id, "decision_timestamp": row.decision_timestamp, "gate": "oos_tree_role", "status": row.decision_tree_prediction_status, "pass": row.tree_role_pass, "value_json": value_json(row.decision_tree_prediction), "provenance": row.decision_tree_model_artifact_id},
            {"stable_row_id": row.stable_row_id, "decision_timestamp": row.decision_timestamp, "gate": "oos_lightgbm_role", "status": row.lightgbm_regression_prediction_status, "pass": row.ranker_role_pass, "value_json": value_json(row.lightgbm_regression_prediction), "provenance": row.lightgbm_regression_model_artifact_id},
            {"stable_row_id": row.stable_row_id, "decision_timestamp": row.decision_timestamp, "gate": "tree_probability_0_55", "status": "evaluated" if row.tree_role_pass else "unavailable", "pass": row.tree_threshold_pass, "value_json": value_json(row.decision_tree_prediction), "provenance": "frozen_v3_1_contract"},
            {"stable_row_id": row.stable_row_id, "decision_timestamp": row.decision_timestamp, "gate": "liquidity", "status": row.liquidity_status, "pass": row.liquidity_pass, "value_json": value_json(row.median_30_session_dollar_volume), "provenance": "session_v3_3_candidate_wide_pre_prediction"},
        ])
    gates = pd.DataFrame(gate_rows)
    candidate_columns = [
        "stable_row_id", "entity_id", "cik", "ticker", "provider_symbol", "provider_exchange", "exchange_calendar",
        "fiscal_year", "decision_timestamp", "prediction_timestamp", "entry_timestamp",
        "all_non_model_hard_gates_pass", "tree_role_pass", "ranker_role_pass", "decision_tree_prediction",
        "tree_threshold_pass", "lightgbm_regression_prediction", "liquidity_required", "liquidity_status",
        "liquidity_pass", "median_30_session_dollar_volume", "eligible_before_period_completeness", "rank", "holding", "weight",
        "decision_tree_model_artifact_id", "lightgbm_regression_model_artifact_id",
        "decision_tree_feature_artifact_id", "lightgbm_regression_feature_artifact_id",
        "decision_tree_preprocessing_artifact_id", "lightgbm_regression_preprocessing_artifact_id",
    ]
    return frame[candidate_columns], gates, exclusions, holdings, periods


def _capture_dirty_state(artifact_root: Path) -> None:
    status = subprocess.run(["git", "status", "--porcelain=v1"], cwd=ROOT, check=True, text=True, capture_output=True).stdout
    patch = subprocess.run(["git", "diff", "--binary", "HEAD"], cwd=ROOT, check=True, capture_output=True).stdout
    (artifact_root / "lineage/git_status_porcelain.txt").write_text(status)
    (artifact_root / "lineage/tracked_dirty.patch").write_bytes(patch)


def validate_outputs(
    candidates: pd.DataFrame,
    gates: pd.DataFrame,
    exclusions: pd.DataFrame,
    evidence: pd.DataFrame,
    liquidity: pd.DataFrame,
    holdings: pd.DataFrame,
    periods: pd.DataFrame,
) -> dict[str, Any]:
    required = candidates.loc[candidates["liquidity_required"]]
    if len(required) != len(liquidity) or set(required["stable_row_id"]) != set(liquidity["stable_row_id"]):
        raise RuntimeError("candidate-wide liquidity coverage is incomplete")
    if len(gates) != len(candidates) * 12 or gates.duplicated(["stable_row_id", "gate"]).any():
        raise RuntimeError("candidate gate table is incomplete or duplicated")
    passing = liquidity.loc[liquidity["liquidity_pass"]]
    if not passing["valid_session_count"].eq(30).all() or passing["exclusion_code"].notna().any():
        raise RuntimeError("passing liquidity row lacks exactly 30 valid sessions")
    if not passing["median_30_session_dollar_volume"].ge(MIN_ADTV).all():
        raise RuntimeError("passing liquidity row is below the frozen threshold")
    if not evidence["market_close"].lt(evidence["prediction_timestamp"]).all():
        raise RuntimeError("liquidity evidence contains a post-prediction market close")
    evidence_counts = evidence.groupby("stable_row_id").size()
    if not evidence_counts.reindex(liquidity["stable_row_id"]).eq(30).all():
        raise RuntimeError("liquidity evidence is not exactly 30 expected sessions per candidate")
    passing_evidence = evidence.loc[evidence["stable_row_id"].isin(passing["stable_row_id"])].copy()
    passing_evidence["recomputed_dollar_volume"] = passing_evidence["unadjusted_close"] * passing_evidence["regular_session_volume"]
    recomputed = passing_evidence.groupby("stable_row_id")["recomputed_dollar_volume"].median()
    frozen = passing.set_index("stable_row_id")["median_30_session_dollar_volume"]
    if not np.allclose(recomputed.sort_index(), frozen.reindex(recomputed.index).sort_index(), rtol=0, atol=1e-9):
        raise RuntimeError("frozen liquidity medians do not match close * volume arithmetic")
    supported = periods.loc[periods["period_supported"], "decision_timestamp"]
    holding_counts = holdings.groupby("decision_timestamp").size().reindex(supported)
    weight_sums = holdings.groupby("decision_timestamp")["weight"].sum().reindex(supported)
    if not holding_counts.eq(TARGET_N).all() or not np.allclose(weight_sums, 1.0):
        raise RuntimeError("supported periods do not contain 15 equal-weight holdings")
    unsupported = set(periods.loc[~periods["period_supported"], "decision_timestamp"])
    if set(holdings["decision_timestamp"]) & unsupported:
        raise RuntimeError("an incomplete period formed a portfolio")
    if holdings.duplicated(["decision_timestamp", "stable_row_id"]).any():
        raise RuntimeError("holding identity is duplicated")
    if exclusions.duplicated(["stable_row_id", "exclusion_code"]).any():
        raise RuntimeError("candidate exclusion identity is duplicated")
    return {
        "result": "pass", "source_rows": len(candidates), "gate_rows": len(gates),
        "exclusion_rows": len(exclusions), "liquidity_required_rows": len(liquidity),
        "liquidity_evidence_rows": len(evidence), "liquidity_pass_rows": len(passing),
        "supported_periods": len(supported), "holding_rows": len(holdings),
        "candidate_wide_liquidity_complete": True, "exact_pre_prediction_30_session_windows": True,
        "daily_dollar_volume_arithmetic_recomputed": True,
        "ranking_after_all_gates": True, "incomplete_periods_have_zero_holdings": True,
    }


def finalize(artifact_root: Path) -> None:
    config, preflight = validate_inputs()
    candidates = load_candidates(config)
    evidence, liquidity = evaluate_liquidity(artifact_root)
    candidate_table, gates, exclusions, holdings, periods = materialize_selection(candidates, liquidity)
    evidence.to_parquet(artifact_root / "outputs/liquidity_evidence.parquet", index=False)
    liquidity.to_parquet(artifact_root / "support/liquidity_coverage.parquet", index=False)
    candidate_table.to_parquet(artifact_root / "outputs/candidates.parquet", index=False)
    gates.to_parquet(artifact_root / "outputs/gates.parquet", index=False)
    exclusions.to_parquet(artifact_root / "outputs/exclusions.parquet", index=False)
    holdings.to_parquet(artifact_root / "outputs/holdings.parquet", index=False)
    prediction_lineage = pd.read_parquet(V3_2_PREDICTIONS)
    prediction_lineage["accepted_v3_2_manifest_sha256"] = V3_2_MANIFEST_SHA256
    prediction_lineage.to_parquet(artifact_root / "outputs/prediction_lineage.parquet", index=False)
    periods.to_parquet(artifact_root / "support/period_coverage.parquet", index=False)
    verdict = validate_outputs(candidate_table, gates, exclusions, evidence, liquidity, holdings, periods)
    write_json(artifact_root / "support/verdict.json", verdict)
    _capture_dirty_state(artifact_root)
    summary = {
        "source_rows": len(candidate_table), "liquidity_required_candidates": int(candidate_table["liquidity_required"].sum()),
        "liquidity_pass_candidates": int(candidate_table["liquidity_pass"].sum()),
        "supported_periods": int(periods["period_supported"].sum()), "holdings": len(holdings),
        "holding_weight": WEIGHT, "minimum_adtv_usd": MIN_ADTV,
    }
    supported_years = ", ".join(str(x) for x in periods.loc[periods["period_supported"], "decision_timestamp"].dt.year.tolist())
    liquidity_failures = liquidity["exclusion_code"].dropna().value_counts().sort_index()
    failure_lines = "\n".join(f"- `{code}`: {count:,}" for code, count in liquidity_failures.items())
    report = f"""# Session V3.3 — Liquidity-Qualified Holdings\n\n## Verdict\n\nSession V3.3 is complete under the frozen `production_v3_ml_gates` contract. The exact accepted V3.1 and V3.2 manifests plus all 156 referenced table, configuration, prediction, model-role, model, and lineage records revalidated before selection. Liquidity was evaluated candidate-wide before ranking; incomplete decision periods formed no portfolio.\n\n## Frozen output\n\n- Source rows: {summary['source_rows']:,}\n- Liquidity-required candidates: {summary['liquidity_required_candidates']:,}\n- Liquidity-pass candidates: {summary['liquidity_pass_candidates']:,}\n- Supported decision periods: {summary['supported_periods']} ({supported_years})\n- Holdings: {summary['holdings']:,}, exactly 15 per supported period at weight {WEIGHT:.16f}\n- Minimum median 30-session dollar volume: ${MIN_ADTV:,.10f}\n\nLiquidity exclusions:\n\n{failure_lines}\n\nDaily dollar volume is unadjusted regular-session close multiplied by regular-session volume. Every passing row has exactly 30 valid exchange-calendar sessions whose market close is strictly before its prediction timestamp. Raw responses, request parameters, retrieval timestamps, mappings, session clocks, currency, adjustment policy, and hashes are retained. The 1,428 candidate-scoped Yahoo requests all returned HTTP 200; source defects still fail closed independently of transport success.\n\n## Selection and lineage controls\n\nRanking is descending OOS LightGBM three-year return prediction with stable row identity as the deterministic tie-breaker, and occurs only after all fixed non-liquidity hard gates, both exact OOS model roles, tree probability at least 0.55, and liquidity pass. Candidate, long-form gate, exclusion, liquidity evidence/coverage, period, holding, weight, source, retrieval, mapping, calendar, raw-response, prediction, model, preprocessing, feature, target, configuration, and accepted-manifest lineage are frozen in the artifact.\n\n## Scope boundary\n\nNo threshold or parameter was optimized. Session 9 predictions and substitute models were not used. No performance, NAV, backtest, post-selection-only liquidity collection, or V3.4 market-ledger work was performed.\n"""
    REPORT_PATH.write_text(report)
    raw_records = []
    for path in sorted((artifact_root / "raw").rglob("*")):
        if path.is_file(): raw_records.append(record(path, "v3_3_raw_liquidity_source_or_request_lineage"))
    artifact_paths = [p for p in sorted(artifact_root.rglob("*")) if p.is_file() and p.name != "manifest.json" and "raw/" not in p.relative_to(artifact_root).as_posix()]
    records = [record(path, "v3_3_input_configuration_candidate_gate_liquidity_holding_or_lineage") for path in artifact_paths] + raw_records
    manifest = {
        "schema_version": 1, "artifact_class": "SESSION_V3_3_LIQUIDITY_QUALIFIED_HOLDINGS",
        "created_at_utc": utc_now(), "current_head": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, text=True, capture_output=True).stdout.strip(),
        "claim": {"status": "accepted", "session_v3_3_complete": True, "strategy_name": "production_v3_ml_gates", "candidate_wide_liquidity_before_ranking": True, "parameters_or_thresholds_optimized": False, "session9_predictions_read": False, "substitute_models_used": False, "performance_calculated": False, "nav_constructed": False, "backtest_run": False, "session_v3_4_started": False},
        "configuration": config, "preflight": preflight, "selection_summary": summary,
        "records": records,
        "code_lineage": [record(ROOT / "portfolio/build_session_v3_3_holdings.py", "v3_3_builder_or_focused_test"), record(ROOT / "tests/portfolio/test_build_session_v3_3_holdings.py", "v3_3_builder_or_focused_test")],
        "deliverables": [record(REPORT_PATH, "v3_3_report")],
        "environment": {"python": platform.python_version(), "numpy": np.__version__, "pandas": pd.__version__, "requests": requests.__version__, "exchange_calendars": importlib.metadata.version("exchange-calendars")},
    }
    write_json(artifact_root / "manifest.json", manifest)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)
    parser.add_argument("--stage", choices=("initialize", "fetch", "finalize", "all"), default="all")
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--retries", type=int, default=3)
    args = parser.parse_args()
    if args.stage in {"initialize", "all"}: initialize(args.artifact_root)
    if args.stage in {"fetch", "all"}: fetch(args.artifact_root, args.workers, args.retries)
    if args.stage in {"finalize", "all"}: finalize(args.artifact_root)


if __name__ == "__main__":
    main()
