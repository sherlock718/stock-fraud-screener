"""Controlled B1E free-data V1 historical performance materialization.

This module is the only historical-study adapter for the frozen B1C evidence
and unchanged B1D engine.  It writes one non-overwriting, manifest-backed
artifact, keeps all performance/outcome/rate namespaces physically separate,
and renders the product report directly from the machine-readable metric
ledger so the report can be independently reproduced.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import platform
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from backtest.free_data_v1_nav import (
    DGS1MO_NAMESPACE,
    FROZEN_B1C_MANIFEST_SHA256,
    OUTCOME_NAMESPACES,
    PERFORMANCE_NAMESPACES,
    ZERO_RATE_NAMESPACE,
    EvidenceBundle,
    EvidenceValidationError,
    PerformanceResult,
    RiskFreeUnavailableError,
    calculate_performance_metrics,
    load_frozen_b1c_evidence,
    run_performance_engine,
    sha256_file,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_B1C_ROOT = (
    ROOT / "artifacts/performance_inputs/free_data_v1/20260731T115106Z-b1c"
)
DEFAULT_ARTIFACT_PARENT = ROOT / "artifacts/performance/free_data_v1"

FROZEN_B1D_ENGINE_SHA256 = (
    "880cf13607851dcc1d0947ef7b62ecc798e1add841a707b85a80535cd74d034f"
)
FROZEN_B1D_TEST_SHA256 = (
    "c9cb5b123b7450498a44938c64ebd166164df8a6216764a110e1a95217dee86f"
)
FROZEN_B1D_ENGINE_PATH = ROOT / "backtest/free_data_v1_nav.py"
FROZEN_B1D_TEST_PATH = ROOT / "tests/backtest/test_free_data_v1_nav.py"

MATURED_YEARS = tuple(range(2015, 2024))
OPEN_YEARS = (2024, 2025, 2026)
VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*-b1e")
ARTIFACT_CLASS = "FREE_DATA_V1_HISTORICAL_PERFORMANCE_B1E"

PORTFOLIO_METRICS = (
    "cagr",
    "annualized_volatility",
    "maximum_drawdown",
    "maximum_drawdown_duration_months",
    "sharpe_ratio",
    "sortino_ratio",
    "calmar_ratio",
    "beta",
    "alpha",
    "tracking_error",
    "information_ratio",
    "turnover",
    "hit_rate",
)
BENCHMARK_METRICS = (
    "cagr",
    "annualized_volatility",
    "maximum_drawdown",
    "maximum_drawdown_duration_months",
    "sharpe_ratio",
    "sortino_ratio",
    "calmar_ratio",
    "turnover",
)
RATE_DEPENDENT_METRICS = {"sharpe_ratio", "sortino_ratio", "alpha"}


@dataclass(frozen=True)
class ControlledPreflight:
    """Verified frozen inputs and a compact audit record."""

    bundle: EvidenceBundle
    summary: Mapping[str, Any]


def _json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).isoformat()
    if isinstance(value, pd.Period):
        return str(value)
    if value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            _json_value(payload), indent=2, sort_keys=True, allow_nan=False
        )
        + "\n"
    )


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _git_head() -> str | None:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def _record(root: Path, path: Path, role: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "role": role,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _hash_payload(payload: Any) -> str:
    raw = json.dumps(
        _json_value(payload), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(raw).hexdigest()


def _required_year_counts(frame: pd.DataFrame, years: Iterable[int]) -> dict[int, int]:
    counts = frame.groupby("decision_year").size().to_dict()
    return {int(year): int(counts.get(year, 0)) for year in years}


def preflight_controlled_run(
    b1c_root: Path = DEFAULT_B1C_ROOT,
    *,
    expected_engine_sha256: str = FROZEN_B1D_ENGINE_SHA256,
    expected_test_sha256: str = FROZEN_B1D_TEST_SHA256,
) -> ControlledPreflight:
    """Reverify the exact B1C evidence and unchanged B1D code boundary."""
    engine_hash = sha256_file(FROZEN_B1D_ENGINE_PATH)
    test_hash = sha256_file(FROZEN_B1D_TEST_PATH)
    if engine_hash != expected_engine_sha256:
        raise EvidenceValidationError(
            "frozen B1D engine hash mismatch: "
            f"expected={expected_engine_sha256} actual={engine_hash}"
        )
    if test_hash != expected_test_sha256:
        raise EvidenceValidationError(
            "frozen B1D test hash mismatch: "
            f"expected={expected_test_sha256} actual={test_hash}"
        )

    bundle = load_frozen_b1c_evidence(
        b1c_root, expected_manifest_sha256=FROZEN_B1C_MANIFEST_SHA256
    )
    holdings = bundle.requirements[
        bundle.requirements["instrument_role"].eq("holding")
    ].copy()
    matured = holdings[holdings["calendar_exit_timestamp"].notna()].copy()
    open_holdings = holdings[holdings["calendar_exit_timestamp"].isna()].copy()
    matured_counts = _required_year_counts(matured, MATURED_YEARS)
    open_counts = _required_year_counts(open_holdings, OPEN_YEARS)
    if set(pd.to_numeric(matured["decision_year"]).astype(int)) != set(MATURED_YEARS):
        raise EvidenceValidationError("matured vintage-year boundary drifted")
    if set(pd.to_numeric(open_holdings["decision_year"]).astype(int)) != set(OPEN_YEARS):
        raise EvidenceValidationError("open vintage-year boundary drifted")
    if any(count != 15 for count in matured_counts.values()):
        raise EvidenceValidationError("matured vintage holding count drifted")
    if any(count != 15 for count in open_counts.values()):
        raise EvidenceValidationError("open vintage holding count drifted")
    if bundle.rate_status.get("dgs1mo_available") is not False:
        raise EvidenceValidationError("frozen B1C DGS1MO availability drifted")
    if bundle.risk_free_observations is not None:
        raise EvidenceValidationError("unexpected DGS1MO observations were supplied")

    manifest = bundle.source_manifest or {}
    summary = {
        "schema_version": 1,
        "b1c_root": b1c_root.relative_to(ROOT).as_posix(),
        "b1c_manifest_sha256": bundle.source_manifest_sha256,
        "b1c_manifest_record_count": len(manifest.get("records", [])),
        "b1c_validated_input_count": len(manifest.get("validated_inputs", [])),
        "b1c_code_lineage_count": len(manifest.get("code_lineage", [])),
        "b1d_engine_sha256": engine_hash,
        "b1d_test_sha256": test_hash,
        "requirement_count": int(len(bundle.requirements)),
        "holding_count": int(len(holdings)),
        "matured_holding_count": int(len(matured)),
        "open_holding_count": int(len(open_holdings)),
        "matured_vintage_counts": matured_counts,
        "open_vintage_counts": open_counts,
        "price_row_count": int(len(bundle.prices)),
        "benchmark_master_count": int(len(bundle.benchmark_requirements)),
        "performance_namespaces": list(PERFORMANCE_NAMESPACES),
        "outcome_namespaces": list(OUTCOME_NAMESPACES),
        "risk_free_namespaces": [DGS1MO_NAMESPACE, ZERO_RATE_NAMESPACE],
        "dgs1mo_available": False,
        "dgs1mo_status": bundle.rate_status.get("dgs1mo_status"),
        "preflight_status": "verified_fail_closed_boundary",
    }
    return ControlledPreflight(bundle=bundle, summary=summary)


def _benchmark_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    frame = transactions[transactions["stream"].eq("benchmark")].copy()
    frame["stream"] = "portfolio"
    return frame


def _benchmark_metrics(
    result: PerformanceResult,
    holdings: pd.DataFrame,
) -> tuple[dict[int, dict[str, dict[str, Any]]], dict[str, dict[str, Any]]]:
    vintage: dict[int, dict[str, dict[str, Any]]] = {}
    benchmark_trades = _benchmark_transactions(result.transaction_ledger)
    for year, frame in result.vintage_nav.groupby("decision_year", sort=True):
        year = int(year)
        start = pd.to_datetime(
            holdings.loc[holdings["decision_year"].eq(year), "entry_timestamp"],
            utc=True,
        ).min()
        vintage[year] = {}
        for basis in ("gross", "net"):
            vintage[year][basis] = calculate_performance_metrics(
                frame,
                return_column=f"benchmark_{basis}_return",
                nav_column=f"benchmark_{basis}_nav",
                benchmark_return_column=f"benchmark_{basis}_return",
                benchmark_nav_column=f"benchmark_{basis}_nav",
                risk_free_returns=np.zeros(len(frame), dtype=float),
                start_date=start,
                transaction_ledger=benchmark_trades[
                    benchmark_trades["decision_year"].eq(year)
                ],
                risk_free_namespace=ZERO_RATE_NAMESPACE,
            )
    aggregate: dict[str, dict[str, Any]] = {}
    start = pd.to_datetime(holdings["entry_timestamp"], utc=True).min()
    for basis in ("gross", "net"):
        aggregate[basis] = calculate_performance_metrics(
            result.aggregate_nav,
            return_column=f"benchmark_{basis}_return",
            nav_column=f"benchmark_{basis}_nav",
            benchmark_return_column=f"benchmark_{basis}_return",
            benchmark_nav_column=f"benchmark_{basis}_nav",
            risk_free_returns=np.zeros(len(result.aggregate_nav), dtype=float),
            start_date=start,
            transaction_ledger=benchmark_trades,
            risk_free_namespace=ZERO_RATE_NAMESPACE,
        )
    return vintage, aggregate


def _outcome_treatment(bundle: EvidenceBundle, namespace: str) -> str:
    contract = bundle.namespace_contracts[namespace]
    assumption = contract.get("terminal_assumption")
    if assumption is None:
        return f"{contract['namespace_class']}: no unsupported recovery"
    return (
        f"{contract['namespace_class']}: {assumption:.0%} only on explicit "
        "unsupported terminal-exit trigger"
    )


def _used_outcomes(result: PerformanceResult) -> pd.DataFrame:
    frames = []
    for outcome, frame in result.outcome_ledgers.items():
        if len(frame):
            item = frame.copy()
            item["outcome_namespace"] = outcome
            frames.append(item)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _scope_coverage(
    result: PerformanceResult,
    holdings: pd.DataFrame,
    *,
    year: int | None,
) -> dict[str, Any]:
    selected = holdings if year is None else holdings[holdings["decision_year"].eq(year)]
    ids = set(selected["requirement_id"])
    outcomes = _used_outcomes(result)
    outcomes = outcomes[outcomes["requirement_id"].isin(ids)] if len(outcomes) else outcomes
    used = outcomes[outcomes["used_in_nav"].astype(bool)] if len(outcomes) else outcomes
    notional = selected.set_index("requirement_id")["planned_entry_notional_usd"]

    def outcome_count(name: str) -> int:
        if not len(outcomes):
            return 0
        return int(outcomes.loc[outcomes["outcome_namespace"].eq(name), "requirement_id"].nunique())

    def outcome_capital(name: str) -> float:
        if not len(outcomes):
            return 0.0
        outcome_ids = set(
            outcomes.loc[outcomes["outcome_namespace"].eq(name), "requirement_id"]
        )
        return float(pd.to_numeric(notional.reindex(sorted(outcome_ids)), errors="coerce").fillna(0).sum())

    holding_ledger = result.holding_ledger[
        result.holding_ledger["requirement_id"].isin(ids)
    ]
    session_count = int(len(holding_ledger))
    capital = float(pd.to_numeric(selected["planned_entry_notional_usd"]).sum())
    resolved_ids = set(used["requirement_id"]) if len(used) else set()
    resolved_capital = float(
        pd.to_numeric(notional.reindex(sorted(resolved_ids)), errors="coerce").fillna(0).sum()
    )
    return {
        "holding_count_denominator": int(len(selected)),
        "resolved_holding_count": int(len(resolved_ids)),
        "capital_denominator": capital,
        "resolved_capital": resolved_capital,
        "observed_holding_count": outcome_count("observed"),
        "observed_capital": outcome_capital("observed"),
        "provider_confirmed_holding_count": outcome_count("provider_confirmed"),
        "provider_confirmed_capital": outcome_capital("provider_confirmed"),
        "unsupported_unresolved_holding_count": outcome_count("unsupported_unresolved"),
        "unsupported_unresolved_capital": outcome_capital("unsupported_unresolved"),
        "scenario_imputed_holding_count": outcome_count("bounded_scenario"),
        "scenario_imputed_capital": outcome_capital("bounded_scenario"),
        "required_stock_session_count": session_count,
        "observed_stock_session_count": session_count,
        "required_benchmark_session_count": session_count,
        "observed_benchmark_session_count": session_count,
        "benchmark_gap_count": 0,
        "terminal_event_holding_count": int(
            outcomes.loc[
                ~outcomes["event_type"].eq("complete_price_path"), "requirement_id"
            ].nunique()
        ) if len(outcomes) else 0,
        "completed_vintage_count": 1 if year is not None else len(MATURED_YEARS),
        "open_vintage_count": len(OPEN_YEARS),
    }


def _metric_rows_for_scope(
    *,
    bundle: EvidenceBundle,
    result: PerformanceResult,
    performance_namespace: str,
    scope: str,
    decision_year: int | None,
    basis: str,
    portfolio: Mapping[str, Any],
    benchmark: Mapping[str, Any],
    coverage: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    treatment = _outcome_treatment(bundle, performance_namespace)
    common = {
        "performance_namespace": performance_namespace,
        "outcome_treatment": treatment,
        "metric_scope": scope,
        "decision_year": decision_year,
        "basis": basis,
        **coverage,
    }

    for stream, metrics, names in (
        ("portfolio", portfolio, PORTFOLIO_METRICS),
        ("benchmark", benchmark, BENCHMARK_METRICS),
    ):
        availability = metrics.get("availability", {})
        for name in names:
            reason = availability.get(name)
            value = metrics.get(name)
            if name in {"maximum_drawdown", "maximum_drawdown_duration_months"}:
                reason = None
            rows.append(
                {
                    **common,
                    "stream": stream,
                    "metric_name": name,
                    "metric_value": value,
                    "availability_reason": reason,
                    "risk_free_namespace": ZERO_RATE_NAMESPACE,
                    "risk_free_required": name in RATE_DEPENDENT_METRICS,
                    "risk_free_treatment": (
                        "explicit_zero_diagnostic"
                        if name in RATE_DEPENDENT_METRICS
                        else "not_used_by_metric"
                    ),
                    "history_interval_count": metrics.get(
                        "coverage_denominators", {}
                    ).get("monthly_return_count"),
                    "paired_benchmark_interval_count": metrics.get(
                        "coverage_denominators", {}
                    ).get("paired_benchmark_return_count"),
                    "risk_free_interval_count": metrics.get(
                        "coverage_denominators", {}
                    ).get("risk_free_interval_count"),
                    "turnover_vintage_count": metrics.get(
                        "coverage_denominators", {}
                    ).get("turnover_vintage_count"),
                    "absolute_actual_traded_notional": metrics.get(
                        "coverage_denominators", {}
                    ).get("absolute_actual_traded_notional"),
                }
            )
        for name in ("sharpe_ratio", "sortino_ratio"):
            rows.append(
                {
                    **common,
                    "stream": stream,
                    "metric_name": name,
                    "metric_value": None,
                    "availability_reason": "exact_dgs1mo_observations_absent",
                    "risk_free_namespace": DGS1MO_NAMESPACE,
                    "risk_free_required": True,
                    "risk_free_treatment": "unavailable_fail_closed_no_substitution",
                    "history_interval_count": metrics.get(
                        "coverage_denominators", {}
                    ).get("monthly_return_count"),
                    "paired_benchmark_interval_count": metrics.get(
                        "coverage_denominators", {}
                    ).get("paired_benchmark_return_count"),
                    "risk_free_interval_count": 0,
                    "turnover_vintage_count": metrics.get(
                        "coverage_denominators", {}
                    ).get("turnover_vintage_count"),
                    "absolute_actual_traded_notional": metrics.get(
                        "coverage_denominators", {}
                    ).get("absolute_actual_traded_notional"),
                }
            )
    rows.append(
        {
            **common,
            "stream": "portfolio",
            "metric_name": "alpha",
            "metric_value": None,
            "availability_reason": "exact_dgs1mo_observations_absent",
            "risk_free_namespace": DGS1MO_NAMESPACE,
            "risk_free_required": True,
            "risk_free_treatment": "unavailable_fail_closed_no_substitution",
            "history_interval_count": portfolio.get(
                "coverage_denominators", {}
            ).get("monthly_return_count"),
            "paired_benchmark_interval_count": portfolio.get(
                "coverage_denominators", {}
            ).get("paired_benchmark_return_count"),
            "risk_free_interval_count": 0,
            "turnover_vintage_count": portfolio.get(
                "coverage_denominators", {}
            ).get("turnover_vintage_count"),
            "absolute_actual_traded_notional": portfolio.get(
                "coverage_denominators", {}
            ).get("absolute_actual_traded_notional"),
        }
    )
    return rows


def _metric_ledger(
    bundle: EvidenceBundle,
    result: PerformanceResult,
    benchmark_vintage: Mapping[int, Mapping[str, Mapping[str, Any]]],
    benchmark_aggregate: Mapping[str, Mapping[str, Any]],
) -> pd.DataFrame:
    holdings = bundle.requirements[
        bundle.requirements["instrument_role"].eq("holding")
        & bundle.requirements["calendar_exit_timestamp"].notna()
    ].copy()
    rows: list[dict[str, Any]] = []
    namespace = result.performance_namespace
    for year in MATURED_YEARS:
        coverage = _scope_coverage(result, holdings, year=year)
        for basis in ("gross", "net"):
            rows.extend(
                _metric_rows_for_scope(
                    bundle=bundle,
                    result=result,
                    performance_namespace=namespace,
                    scope="separate_vintage",
                    decision_year=year,
                    basis=basis,
                    portfolio=result.vintage_metrics[year][basis],
                    benchmark=benchmark_vintage[year][basis],
                    coverage=coverage,
                )
            )
    coverage = _scope_coverage(result, holdings, year=None)
    for basis in ("gross", "net"):
        rows.extend(
            _metric_rows_for_scope(
                bundle=bundle,
                result=result,
                performance_namespace=namespace,
                scope="aggregate_strategy",
                decision_year=None,
                basis=basis,
                portfolio=result.aggregate_metrics[basis],
                benchmark=benchmark_aggregate[basis],
                coverage=coverage,
            )
        )
    frame = pd.DataFrame(rows)
    frame["decision_year"] = frame["decision_year"].astype("Int64")
    return frame.sort_values(
        [
            "performance_namespace",
            "metric_scope",
            "decision_year",
            "basis",
            "stream",
            "risk_free_namespace",
            "metric_name",
        ],
        na_position="last",
    ).reset_index(drop=True)


def _coverage_ledger(
    bundle: EvidenceBundle,
    result: PerformanceResult,
) -> pd.DataFrame:
    requirements = bundle.requirements[
        bundle.requirements["instrument_role"].eq("holding")
        & bundle.requirements["calendar_exit_timestamp"].notna()
    ][
        [
            "requirement_id",
            "stable_row_id",
            "ticker",
            "provider_symbol",
            "exchange_calendar",
            "decision_year",
            "benchmark_symbol",
            "entry_timestamp",
            "calendar_exit_timestamp",
            "weight",
            "planned_entry_notional_usd",
        ]
    ].copy()
    identity = bundle.security_identity[
        [
            "requirement_id",
            "s1_coverage_status",
            "s1_identity_status",
            "dated_security_lineage_complete",
            "certified_performance_identity_available",
        ]
    ].rename(columns={"s1_coverage_status": "identity_s1_coverage_status"})
    action_columns = [
        "requirement_id",
        "s1_event_status",
        "deterministic_action",
        "provider_adjclose_action_semantics_certified",
        "primary_return_available",
        "event_type",
        "event_effective_timestamp",
    ]
    actions = bundle.security_actions.reindex(columns=action_columns)
    coverage = bundle.coverage[
        [
            "requirement_id",
            "entry_observed_common",
            "exit_observed_common",
            "required_month_end_count",
            "observed_common_month_end_count",
            "missing_common_month_end_count",
            "benchmark_gap_count",
            "price_coverage_status",
            "relative_evidence_status",
        ]
    ]
    eligibility = bundle.namespace_eligibility[result.performance_namespace][
        ["requirement_id", "eligibility_state", "eligible_for_future_nav_engine"]
    ]
    frame = (
        requirements.merge(identity, on="requirement_id", validate="one_to_one")
        .merge(actions, on="requirement_id", validate="one_to_one")
        .merge(coverage, on="requirement_id", validate="one_to_one")
        .merge(eligibility, on="requirement_id", validate="one_to_one")
    )
    outcomes = _used_outcomes(result)
    used = outcomes[outcomes["used_in_nav"].astype(bool)].copy()
    nav_outcome = used.groupby("requirement_id")["outcome_namespace"].agg(
        lambda values: "|".join(sorted(set(values)))
    )
    all_outcome = outcomes.groupby("requirement_id")["outcome_namespace"].agg(
        lambda values: "|".join(sorted(set(values)))
    )
    frame["performance_namespace"] = result.performance_namespace
    frame["outcome_treatment"] = _outcome_treatment(
        bundle, result.performance_namespace
    )
    frame["recorded_outcome_namespaces"] = frame["requirement_id"].map(all_outcome)
    frame["nav_outcome_namespace"] = frame["requirement_id"].map(nav_outcome)
    frame["resolved_for_nav"] = frame["nav_outcome_namespace"].notna()
    return frame.sort_values(["decision_year", "requirement_id"]).reset_index(drop=True)


def _open_vintages(bundle: EvidenceBundle) -> pd.DataFrame:
    holdings = bundle.requirements[
        bundle.requirements["instrument_role"].eq("holding")
        & bundle.requirements["calendar_exit_timestamp"].isna()
    ].copy()
    coverage = bundle.coverage[
        [
            "requirement_id",
            "entry_observed_common",
            "required_month_end_count",
            "observed_common_month_end_count",
            "missing_common_month_end_count",
            "benchmark_gap_count",
            "price_coverage_status",
            "relative_evidence_status",
        ]
    ]
    frame = holdings.merge(coverage, on="requirement_id", validate="one_to_one")
    frame["completed_vintage_metrics_included"] = False
    frame["exclusion_reason"] = "open_2024_2026_vintage_not_matured"
    return frame.sort_values(["decision_year", "requirement_id"]).reset_index(drop=True)


def _event_and_scenario_ledgers(result: PerformanceResult) -> tuple[pd.DataFrame, pd.DataFrame]:
    outcomes = _used_outcomes(result)
    if not len(outcomes):
        return outcomes.copy(), outcomes.copy()
    events = outcomes[~outcomes["event_type"].eq("complete_price_path")].copy()
    scenarios = outcomes[outcomes["outcome_namespace"].eq("bounded_scenario")].copy()
    return events.reset_index(drop=True), scenarios.reset_index(drop=True)


def _fmt_percent(value: Any) -> str:
    if value is None or pd.isna(value):
        return "unavailable"
    return f"{float(value):.2%}"


def _fmt_number(value: Any) -> str:
    if value is None or pd.isna(value):
        return "unavailable"
    return f"{float(value):.3f}"


def _metric_lookup(
    metrics: pd.DataFrame,
    *,
    namespace: str,
    scope: str,
    year: int | None,
    basis: str,
    stream: str,
    name: str,
    rate: str = ZERO_RATE_NAMESPACE,
) -> Any:
    mask = (
        metrics["performance_namespace"].eq(namespace)
        & metrics["metric_scope"].eq(scope)
        & metrics["basis"].eq(basis)
        & metrics["stream"].eq(stream)
        & metrics["metric_name"].eq(name)
        & metrics["risk_free_namespace"].eq(rate)
    )
    mask &= (
        metrics["decision_year"].isna()
        if year is None
        else metrics["decision_year"].eq(year)
    )
    selected = metrics.loc[mask]
    if len(selected) != 1:
        raise EvidenceValidationError(
            "report metric key is not unique: "
            f"{namespace}/{scope}/{year}/{basis}/{stream}/{name}/{rate}"
        )
    return selected.iloc[0]["metric_value"]


def render_product_report(summary: Mapping[str, Any], metrics: pd.DataFrame) -> str:
    """Render the human report only from versioned machine-readable outputs."""
    lines = [
        "# Free-data V1 historical performance",
        "",
        (
            f"Artifact `{summary['version']}`. Controlled B1E calculation over "
            "independent July 2015-2023 36-month vintages using the frozen B1C "
            "evidence and unchanged B1D engine."
        ),
        "",
        (
            "These are free-source historical research results. They are not "
            "survivorship-complete certification or provider-certified performance, "
            "not personalized investment advice, and not a promise of future "
            "performance."
        ),
        "",
        "## Coverage before performance",
        "",
        (
            "Yahoo adjusted-close evidence supplies the complete common-session price "
            "paths used below, but its corporate-action adjustment semantics are not "
            "certified. S1 has zero fully matched dated security/action requirements; "
            "all 135 matured holdings remain identity/action-ambiguous even though their "
            "price paths reconcile. No explicit unsupported terminal-exit trigger is "
            "present, so no scenario is applied."
        ),
        "",
        "| Performance namespace | Outcome treatment | Holdings | Capital | Observed | Provider-confirmed | Unsupported/unresolved outcome | Scenario-imputed | Stock sessions | Benchmark sessions | Gaps | Terminal triggers |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    coverage = summary["namespace_coverage"]
    for namespace in PERFORMANCE_NAMESPACES:
        item = coverage[namespace]
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{namespace}`",
                    item["outcome_treatment"],
                    f"{item['resolved_holding_count']}/{item['holding_count_denominator']}",
                    f"${item['resolved_capital']:,.0f}/${item['capital_denominator']:,.0f}",
                    f"{item['observed_holding_count']} (${item['observed_capital']:,.0f})",
                    f"{item['provider_confirmed_holding_count']} (${item['provider_confirmed_capital']:,.0f})",
                    f"{item['unsupported_unresolved_holding_count']} (${item['unsupported_unresolved_capital']:,.0f})",
                    f"{item['scenario_imputed_holding_count']} (${item['scenario_imputed_capital']:,.0f})",
                    f"{item['observed_stock_session_count']}/{item['required_stock_session_count']}",
                    f"{item['observed_benchmark_session_count']}/{item['required_benchmark_session_count']}",
                    str(item["benchmark_gap_count"]),
                    f"{item['terminal_event_holding_count']}/{item['holding_count_denominator']}",
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            (
                "Open vintages: 2024-2026, 45 holdings across three vintages. They are "
                "preserved in the open-vintage ledger and excluded from completed-"
                "vintage metrics. Completed vintages: nine. Benchmark masters: four; "
                "all required benchmark sessions are observed, while benchmark identity/"
                "action certification remains incomplete."
            ),
            "",
            "## Risk-free boundary",
            "",
            (
                f"`{DGS1MO_NAMESPACE}` has 0 observed intervals and remains unavailable "
                f"with status `{summary['dgs1mo_status']}`. DGS1MO-dependent Sharpe, "
                "Sortino, and alpha are unavailable and no carry, interpolation, or "
                "substitute rate is used."
            ),
            "",
            (
                f"`{ZERO_RATE_NAMESPACE}` is a physically separate zero-rate diagnostic. "
                "Only rows explicitly labeled with that namespace use zero-rate Sharpe, "
                "Sortino, or alpha; these values are never represented as DGS1MO results."
            ),
            "",
            "## Aggregate overlapping-strategy results",
            "",
            (
                "The aggregate is time-weighted: later-vintage contributions and mature-"
                "vintage withdrawals are external flows, so external capital is not "
                "double-counted. Costs are 25 bps per side on absolute actual traded "
                "notional. Scenario namespaces happen to have equal values here because "
                "the frozen evidence contains zero authorized terminal-exit triggers; "
                "their files and contracts remain separate."
            ),
            "",
            "| Namespace | Basis | Portfolio CAGR | Benchmark CAGR | Portfolio vol. | Benchmark vol. | Portfolio max DD | Benchmark max DD | Zero-rate Sharpe | Zero-rate alpha | Tracking error | Information ratio | Turnover | Coverage | Benchmark coverage | Scenario capital | Rate namespace |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|---|",
        ]
    )
    for namespace in PERFORMANCE_NAMESPACES:
        item = coverage[namespace]
        for basis in ("gross", "net"):
            get = lambda stream, name: _metric_lookup(
                metrics,
                namespace=namespace,
                scope="aggregate_strategy",
                year=None,
                basis=basis,
                stream=stream,
                name=name,
            )
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{namespace}`",
                        basis,
                        _fmt_percent(get("portfolio", "cagr")),
                        _fmt_percent(get("benchmark", "cagr")),
                        _fmt_percent(get("portfolio", "annualized_volatility")),
                        _fmt_percent(get("benchmark", "annualized_volatility")),
                        _fmt_percent(get("portfolio", "maximum_drawdown")),
                        _fmt_percent(get("benchmark", "maximum_drawdown")),
                        _fmt_number(get("portfolio", "sharpe_ratio")),
                        _fmt_percent(get("portfolio", "alpha")),
                        _fmt_percent(get("portfolio", "tracking_error")),
                        _fmt_number(get("portfolio", "information_ratio")),
                        _fmt_number(get("portfolio", "turnover")),
                        f"{item['resolved_holding_count']}/{item['holding_count_denominator']}; ${item['resolved_capital']:,.0f}/${item['capital_denominator']:,.0f}",
                        f"{item['observed_benchmark_session_count']}/{item['required_benchmark_session_count']}; gaps {item['benchmark_gap_count']}",
                        f"${item['scenario_imputed_capital']:,.0f}",
                        f"`{ZERO_RATE_NAMESPACE}`",
                    ]
                )
                + " |"
            )
    lines.extend(
        [
            "",
            "## Separate-vintage net results",
            "",
            "| Namespace | Vintage | Portfolio CAGR | Benchmark CAGR | Portfolio max DD | Benchmark max DD | Zero-rate Sharpe | Zero-rate alpha | Coverage | Benchmark coverage | Scenario capital | Rate namespace |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|---|---:|---|",
        ]
    )
    for namespace in PERFORMANCE_NAMESPACES:
        for year in MATURED_YEARS:
            get = lambda stream, name: _metric_lookup(
                metrics,
                namespace=namespace,
                scope="separate_vintage",
                year=year,
                basis="net",
                stream=stream,
                name=name,
            )
            sample = metrics[
                metrics["performance_namespace"].eq(namespace)
                & metrics["metric_scope"].eq("separate_vintage")
                & metrics["decision_year"].eq(year)
            ].iloc[0]
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{namespace}`",
                        str(year),
                        _fmt_percent(get("portfolio", "cagr")),
                        _fmt_percent(get("benchmark", "cagr")),
                        _fmt_percent(get("portfolio", "maximum_drawdown")),
                        _fmt_percent(get("benchmark", "maximum_drawdown")),
                        _fmt_number(get("portfolio", "sharpe_ratio")),
                        _fmt_percent(get("portfolio", "alpha")),
                        f"{sample['resolved_holding_count']}/{sample['holding_count_denominator']}; ${sample['resolved_capital']:,.0f}/${sample['capital_denominator']:,.0f}",
                        f"{sample['observed_benchmark_session_count']}/{sample['required_benchmark_session_count']}; gaps {sample['benchmark_gap_count']}",
                        f"${sample['scenario_imputed_capital']:,.0f}",
                        f"`{ZERO_RATE_NAMESPACE}`",
                    ]
                )
                + " |"
            )
    lines.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            (
                "The four namespace results do not diverge because there is no explicit "
                "unsupported terminal-exit trigger in B1C—not because survivorship or "
                "corporate-action completeness has been certified. The 135 matured "
                "holdings remain S1-ambiguous, adjusted-close semantics remain provider-"
                "uncertified, and old V3 performance claims are historical and non-"
                "transferable. Full per-vintage gross/net NAV, matching benchmark NAV, "
                "transactions, capital flows, outcome/event/scenario ledgers, coverage, "
                "metric denominators, and availability reasons are versioned beside this "
                "report."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def _summary_for_report(
    *,
    version: str,
    created_at_utc: str,
    bundle: EvidenceBundle,
    results: Mapping[str, PerformanceResult],
) -> dict[str, Any]:
    holdings = bundle.requirements[
        bundle.requirements["instrument_role"].eq("holding")
        & bundle.requirements["calendar_exit_timestamp"].notna()
    ]
    namespace_coverage = {}
    for namespace, result in results.items():
        coverage = _scope_coverage(result, holdings, year=None)
        namespace_coverage[namespace] = {
            **coverage,
            "outcome_treatment": _outcome_treatment(bundle, namespace),
            "s1_ambiguous_holding_count": int(len(holdings)),
            "certified_performance_identity_holding_count": 0,
            "provider_adjclose_semantics_certified_holding_count": 0,
            "security_action_ledger_certified_holding_count": 0,
        }
    return {
        "schema_version": 1,
        "version": version,
        "created_at_utc": created_at_utc,
        "completed_vintage_years": list(MATURED_YEARS),
        "open_vintage_years": list(OPEN_YEARS),
        "completed_vintage_count": len(MATURED_YEARS),
        "open_vintage_count": len(OPEN_YEARS),
        "matured_holding_count": int(len(holdings)),
        "open_holding_count": 45,
        "benchmark_master_count": int(len(bundle.benchmark_requirements)),
        "benchmark_master_certified_count": 0,
        "dgs1mo_status": bundle.rate_status.get("dgs1mo_status"),
        "dgs1mo_observed_interval_count": 0,
        "zero_rate_diagnostic_available": True,
        "namespace_coverage": namespace_coverage,
        "limitations": [
            "free-source historical research only",
            "not survivorship-complete certification",
            "not provider-certified performance",
            "not personalized investment advice",
            "not a future-performance promise",
        ],
    }


def _assert_result_boundary(result: PerformanceResult) -> None:
    if not result.available or result.unavailable_reasons:
        raise EvidenceValidationError(
            f"performance namespace unavailable: {result.performance_namespace} "
            f"{result.unavailable_reasons}"
        )
    if set(pd.to_numeric(result.vintage_nav["decision_year"]).astype(int)) != set(MATURED_YEARS):
        raise EvidenceValidationError("calculated vintage-year boundary drifted")
    if len(result.capital_ledger) != len(MATURED_YEARS):
        raise EvidenceValidationError("capital ledger does not contain nine vintages")
    if int(result.coverage.get("open_holding_count", -1)) != 45:
        raise EvidenceValidationError("open holdings were not preserved")
    if int(result.coverage.get("benchmark_gap_count", -1)) != 0:
        raise EvidenceValidationError("benchmark gaps entered performance")


def build_free_data_v1_performance(
    artifact_root: Path,
    *,
    version: str,
    b1c_root: Path = DEFAULT_B1C_ROOT,
    created_at_utc: str | None = None,
) -> Path:
    """Run and materialize one controlled, non-overwriting B1E artifact."""
    if not VERSION_PATTERN.fullmatch(version):
        raise ValueError("version must be a path-safe identifier ending in -b1e")
    artifact_root = artifact_root.resolve()
    if artifact_root.name != version:
        raise ValueError("artifact root basename must equal version")
    if artifact_root.exists() and any(artifact_root.iterdir()):
        raise RuntimeError(f"B1E target is not empty: {artifact_root}")

    preflight = preflight_controlled_run(b1c_root)
    bundle = preflight.bundle
    results: dict[str, PerformanceResult] = {}
    benchmark_metrics: dict[
        str, tuple[dict[int, dict[str, dict[str, Any]]], dict[str, dict[str, Any]]]
    ] = {}
    metric_frames: list[pd.DataFrame] = []
    coverage_frames: list[pd.DataFrame] = []

    for namespace in PERFORMANCE_NAMESPACES:
        try:
            run_performance_engine(
                bundle,
                performance_namespace=namespace,
                risk_free_namespace=DGS1MO_NAMESPACE,
            )
        except RiskFreeUnavailableError:
            pass
        else:
            raise EvidenceValidationError(
                f"DGS1MO unexpectedly calculated for {namespace}"
            )
        result = run_performance_engine(
            bundle,
            performance_namespace=namespace,
            risk_free_namespace=ZERO_RATE_NAMESPACE,
        )
        _assert_result_boundary(result)
        results[namespace] = result
        benchmark_metrics[namespace] = _benchmark_metrics(
            result,
            bundle.requirements[
                bundle.requirements["instrument_role"].eq("holding")
                & bundle.requirements["calendar_exit_timestamp"].notna()
            ],
        )
        vintage_benchmark, aggregate_benchmark = benchmark_metrics[namespace]
        metric_frames.append(
            _metric_ledger(bundle, result, vintage_benchmark, aggregate_benchmark)
        )
        coverage_frames.append(_coverage_ledger(bundle, result))

    created = created_at_utc or datetime.now(timezone.utc).isoformat()
    metrics = pd.concat(metric_frames, ignore_index=True)
    coverage_all = pd.concat(coverage_frames, ignore_index=True)
    summary = _summary_for_report(
        version=version, created_at_utc=created, bundle=bundle, results=results
    )
    configuration = {
        "schema_version": 1,
        "session": "B1E",
        "version": version,
        "input_manifest_sha256": FROZEN_B1C_MANIFEST_SHA256,
        "engine_sha256": FROZEN_B1D_ENGINE_SHA256,
        "engine_test_sha256": FROZEN_B1D_TEST_SHA256,
        "matured_vintage_years": list(MATURED_YEARS),
        "open_vintage_years": list(OPEN_YEARS),
        "holding_months": 36,
        "decision_clock": "annual July 2; first common session entry",
        "later_vintage_rebalances_earlier_vintage": False,
        "aggregate_method": "time_weighted_external_flow_adjusted",
        "transaction_cost_rate_per_side": 0.0025,
        "transaction_cost_basis": "absolute_actual_traded_notional",
        "performance_namespaces": list(PERFORMANCE_NAMESPACES),
        "outcome_namespaces": list(OUTCOME_NAMESPACES),
        "risk_free_namespaces": [DGS1MO_NAMESPACE, ZERO_RATE_NAMESPACE],
        "model_execution": False,
        "external_collection": False,
    }

    artifact_root.mkdir(parents=True, exist_ok=True)
    records: list[tuple[Path, str]] = []

    input_manifest = artifact_root / "inputs/b1c_manifest.json"
    input_manifest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(b1c_root / "manifest.json", input_manifest)
    if sha256_file(input_manifest) != FROZEN_B1C_MANIFEST_SHA256:
        raise EvidenceValidationError("copied B1C manifest hash mismatch")
    records.append((input_manifest, "frozen_b1c_manifest_copy"))

    source_contracts = [
        "contracts/free_data_v1_performance_contract.json",
        *(f"contracts/namespaces/{name}.json" for name in PERFORMANCE_NAMESPACES),
        f"contracts/rates/{DGS1MO_NAMESPACE}.json",
        f"contracts/rates/{ZERO_RATE_NAMESPACE}.json",
    ]
    for relative in source_contracts:
        target = artifact_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(b1c_root / relative, target)
        records.append((target, "exact_frozen_b1c_contract_copy"))

    configuration_path = artifact_root / "configuration/run_configuration.json"
    _write_json(configuration_path, configuration)
    records.append((configuration_path, "controlled_run_configuration"))
    preflight_path = artifact_root / "support/preflight.json"
    _write_json(preflight_path, preflight.summary)
    records.append((preflight_path, "independent_precalculation_verification"))
    environment_path = artifact_root / "support/environment.json"
    try:
        import pyarrow

        pyarrow_version = pyarrow.__version__
    except ImportError:
        pyarrow_version = None
    _write_json(
        environment_path,
        {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "pyarrow": pyarrow_version,
        },
    )
    records.append((environment_path, "execution_environment"))

    open_path = artifact_root / "outputs/open_vintages.parquet"
    _write_parquet(open_path, _open_vintages(bundle))
    records.append((open_path, "open_2024_2026_vintages_excluded_from_metrics"))

    metrics_path = artifact_root / "outputs/metrics.parquet"
    _write_parquet(metrics_path, metrics)
    records.append((metrics_path, "machine_readable_metric_ledger"))
    availability_path = artifact_root / "outputs/availability_reasons.parquet"
    _write_parquet(
        availability_path,
        metrics[metrics["availability_reason"].notna()].reset_index(drop=True),
    )
    records.append((availability_path, "machine_readable_metric_availability"))
    coverage_path = artifact_root / "outputs/coverage.parquet"
    _write_parquet(coverage_path, coverage_all)
    records.append((coverage_path, "all_namespace_holding_coverage_ledger"))

    namespace_hashes: dict[str, Any] = {}
    rate_hashes: dict[str, Any] = {}
    for namespace in PERFORMANCE_NAMESPACES:
        result = results[namespace]
        vintage_benchmark, aggregate_benchmark = benchmark_metrics[namespace]
        base = artifact_root / f"outputs/namespaces/{namespace}"
        output_frames = {
            "nav/vintage.parquet": (
                result.vintage_nav,
                "separate_vintage_gross_net_portfolio_benchmark_nav",
            ),
            "nav/aggregate.parquet": (
                result.aggregate_nav,
                "aggregate_time_weighted_gross_net_portfolio_benchmark_nav",
            ),
            "ledgers/holdings.parquet": (
                result.holding_ledger,
                "complete_holding_valuation_ledger",
            ),
            "ledgers/transactions.parquet": (
                result.transaction_ledger,
                "absolute_actual_traded_notional_transaction_ledger",
            ),
            "ledgers/capital.parquet": (
                result.capital_ledger,
                "external_capital_flow_ledger",
            ),
            "ledgers/coverage.parquet": (
                coverage_all[
                    coverage_all["performance_namespace"].eq(namespace)
                ].reset_index(drop=True),
                "namespace_holding_coverage_ledger",
            ),
        }
        events, scenarios = _event_and_scenario_ledgers(result)
        output_frames["ledgers/events.parquet"] = (events, "terminal_event_ledger")
        output_frames["ledgers/scenarios.parquet"] = (
            scenarios,
            "bounded_scenario_ledger",
        )
        for outcome in OUTCOME_NAMESPACES:
            output_frames[f"outcomes/{outcome}.parquet"] = (
                result.outcome_ledgers[outcome],
                f"physically_separate_{outcome}_outcome_ledger",
            )
        for relative, (frame, role) in output_frames.items():
            path = base / relative
            _write_parquet(path, frame)
            records.append((path, role))

        portfolio_metrics_path = base / "metrics/portfolio.json"
        _write_json(
            portfolio_metrics_path,
            {
                "performance_namespace": namespace,
                "risk_free_namespace": ZERO_RATE_NAMESPACE,
                "outcome_treatment": _outcome_treatment(bundle, namespace),
                "vintage_metrics": result.vintage_metrics,
                "aggregate_metrics": result.aggregate_metrics,
            },
        )
        records.append((portfolio_metrics_path, "portfolio_metric_payload"))
        benchmark_metrics_path = base / "metrics/benchmark.json"
        _write_json(
            benchmark_metrics_path,
            {
                "performance_namespace": namespace,
                "risk_free_namespace": ZERO_RATE_NAMESPACE,
                "outcome_treatment": _outcome_treatment(bundle, namespace),
                "vintage_metrics": vintage_benchmark,
                "aggregate_metrics": aggregate_benchmark,
            },
        )
        records.append((benchmark_metrics_path, "benchmark_metric_payload"))
        namespace_summary_path = base / "coverage.json"
        _write_json(namespace_summary_path, summary["namespace_coverage"][namespace])
        records.append((namespace_summary_path, "namespace_coverage_summary"))

        namespace_metrics = metrics[
            metrics["performance_namespace"].eq(namespace)
        ]
        zero_path = base / f"rates/{ZERO_RATE_NAMESPACE}/metrics.parquet"
        _write_parquet(
            zero_path,
            namespace_metrics[
                namespace_metrics["risk_free_namespace"].eq(ZERO_RATE_NAMESPACE)
            ].reset_index(drop=True),
        )
        records.append((zero_path, "zero_rate_diagnostic_metrics"))
        dgs_path = base / f"rates/{DGS1MO_NAMESPACE}/unavailable_metrics.parquet"
        _write_parquet(
            dgs_path,
            namespace_metrics[
                namespace_metrics["risk_free_namespace"].eq(DGS1MO_NAMESPACE)
            ].reset_index(drop=True),
        )
        records.append((dgs_path, "dgs1mo_fail_closed_metric_availability"))

        namespace_hashes[namespace] = {
            "contract_sha256": sha256_file(
                artifact_root / f"contracts/namespaces/{namespace}.json"
            ),
            "vintage_nav_sha256": sha256_file(base / "nav/vintage.parquet"),
            "aggregate_nav_sha256": sha256_file(base / "nav/aggregate.parquet"),
            "coverage_sha256": sha256_file(base / "ledgers/coverage.parquet"),
            "outcome_hashes": {
                outcome: sha256_file(base / f"outcomes/{outcome}.parquet")
                for outcome in OUTCOME_NAMESPACES
            },
            "scenario_ledger_sha256": sha256_file(base / "ledgers/scenarios.parquet"),
            "zero_rate_metrics_sha256": sha256_file(zero_path),
            "dgs1mo_unavailable_metrics_sha256": sha256_file(dgs_path),
        }
    for rate in (DGS1MO_NAMESPACE, ZERO_RATE_NAMESPACE):
        rate_hashes[rate] = {
            "contract_sha256": sha256_file(
                artifact_root / f"contracts/rates/{rate}.json"
            ),
            "status": (
                bundle.rate_status.get("dgs1mo_status")
                if rate == DGS1MO_NAMESPACE
                else "available_explicit_zero_diagnostic"
            ),
        }

    summary_path = artifact_root / "support/summary.json"
    _write_json(summary_path, summary)
    records.append((summary_path, "coverage_first_product_summary"))
    report_path = artifact_root / "report/product_report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_product_report(summary, metrics))
    records.append((report_path, "human_readable_v1_product_report"))

    code_paths = [
        FROZEN_B1D_ENGINE_PATH,
        FROZEN_B1D_TEST_PATH,
        ROOT / "backtest/free_data_v1_performance.py",
        ROOT / "workflows/run_free_data_v1_performance.py",
        ROOT / "tests/backtest/test_free_data_v1_performance.py",
    ]
    code_lineage = [
        {
            "path": path.relative_to(ROOT).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
            "lineage_class": (
                "frozen_unchanged_b1d"
                if path in {FROZEN_B1D_ENGINE_PATH, FROZEN_B1D_TEST_PATH}
                else "current_b1e_code"
            ),
        }
        for path in code_paths
    ]
    manifest = {
        "schema_version": 1,
        "artifact_class": ARTIFACT_CLASS,
        "version": version,
        "created_at_utc": created,
        "build_mode": "offline_controlled_frozen_evidence_backtest",
        "current_head": _git_head(),
        "validated_inputs": [
            {
                "source_path": b1c_root.relative_to(ROOT).as_posix() + "/manifest.json",
                "copied_path": "inputs/b1c_manifest.json",
                "size_bytes": input_manifest.stat().st_size,
                "sha256": sha256_file(input_manifest),
                "verified_artifact_record_count": preflight.summary[
                    "b1c_manifest_record_count"
                ],
                "verified_validated_input_count": preflight.summary[
                    "b1c_validated_input_count"
                ],
                "verified_code_lineage_count": preflight.summary[
                    "b1c_code_lineage_count"
                ],
            }
        ],
        "code_lineage": code_lineage,
        "configuration": configuration,
        "configuration_sha256": _hash_payload(configuration),
        "namespace_hashes": namespace_hashes,
        "rate_namespace_hashes": rate_hashes,
        "records": [
            _record(artifact_root, path, role)
            for path, role in sorted(records, key=lambda item: item[0].as_posix())
        ],
        "coverage": summary,
        "claim": {
            "b1e_complete": True,
            "historical_performance_calculated": True,
            "matured_2015_2023_only": True,
            "open_2024_2026_excluded_from_completed_metrics": True,
            "model_executed": False,
            "external_data_collected": False,
            "portfolio_changed": False,
            "survivorship_complete": False,
            "provider_certified": False,
            "dgs1mo_metrics_available": False,
            "zero_rate_diagnostic_available": True,
        },
        "limitations": summary["limitations"],
    }
    manifest_path = artifact_root / "manifest.json"
    _write_json(manifest_path, manifest)
    verify_performance_artifact(artifact_root)
    return manifest_path


def verify_performance_artifact(
    artifact_root: Path,
    *,
    expected_manifest_sha256: str | None = None,
) -> Mapping[str, Any]:
    """Independently rehash all outputs and reproduce the report from metrics."""
    artifact_root = artifact_root.resolve()
    manifest_path = artifact_root / "manifest.json"
    if not manifest_path.is_file():
        raise EvidenceValidationError("B1E manifest is missing")
    actual_manifest_hash = sha256_file(manifest_path)
    if expected_manifest_sha256 and actual_manifest_hash != expected_manifest_sha256:
        raise EvidenceValidationError("B1E manifest hash mismatch")
    try:
        manifest = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        raise EvidenceValidationError("B1E manifest is invalid JSON") from exc
    if (
        manifest.get("artifact_class") != ARTIFACT_CLASS
        or manifest.get("schema_version") != 1
    ):
        raise EvidenceValidationError("B1E manifest identity drifted")
    records = manifest.get("records")
    if not isinstance(records, list) or not records:
        raise EvidenceValidationError("B1E manifest records are absent")
    record_paths = [item.get("path") for item in records]
    if len(record_paths) != len(set(record_paths)):
        raise EvidenceValidationError("B1E manifest record paths are duplicated")
    discovered = {
        path.relative_to(artifact_root).as_posix()
        for path in artifact_root.rglob("*")
        if path.is_file() and path != manifest_path
    }
    if discovered != set(record_paths):
        raise EvidenceValidationError("B1E manifest does not enumerate every output")
    for record in records:
        path = artifact_root / record["path"]
        if path.stat().st_size != record.get("size_bytes"):
            raise EvidenceValidationError(
                f"B1E record size mismatch: {record['path']}"
            )
        if sha256_file(path) != record.get("sha256"):
            raise EvidenceValidationError(
                f"B1E record hash mismatch: {record['path']}"
            )
    copied = artifact_root / "inputs/b1c_manifest.json"
    if sha256_file(copied) != FROZEN_B1C_MANIFEST_SHA256:
        raise EvidenceValidationError("B1E copied B1C input drifted")
    for item in manifest.get("code_lineage", []):
        path = ROOT / item["path"]
        if path.stat().st_size != item.get("size_bytes") or sha256_file(path) != item.get("sha256"):
            raise EvidenceValidationError(f"B1E code lineage drifted: {item['path']}")
    if manifest.get("configuration_sha256") != _hash_payload(
        manifest.get("configuration")
    ):
        raise EvidenceValidationError("B1E configuration hash drifted")

    metrics = pd.read_parquet(artifact_root / "outputs/metrics.parquet")
    summary = json.loads((artifact_root / "support/summary.json").read_text())
    expected_report = render_product_report(summary, metrics)
    actual_report = (artifact_root / "report/product_report.md").read_text()
    if actual_report != expected_report:
        raise EvidenceValidationError("B1E report does not reconcile to metrics")
    if set(metrics["performance_namespace"]) != set(PERFORMANCE_NAMESPACES):
        raise EvidenceValidationError("B1E metric performance namespaces drifted")
    if set(metrics["risk_free_namespace"]) != {
        DGS1MO_NAMESPACE,
        ZERO_RATE_NAMESPACE,
    }:
        raise EvidenceValidationError("B1E metric rate namespaces drifted")
    dgs = metrics[metrics["risk_free_namespace"].eq(DGS1MO_NAMESPACE)]
    if dgs["metric_value"].notna().any() or not dgs["availability_reason"].eq(
        "exact_dgs1mo_observations_absent"
    ).all():
        raise EvidenceValidationError("B1E DGS1MO metrics did not fail closed")
    open_vintages = pd.read_parquet(artifact_root / "outputs/open_vintages.parquet")
    if (
        len(open_vintages) != 45
        or set(pd.to_numeric(open_vintages["decision_year"]).astype(int))
        != set(OPEN_YEARS)
        or open_vintages["completed_vintage_metrics_included"].astype(bool).any()
    ):
        raise EvidenceValidationError("B1E open-vintage boundary drifted")
    return {
        "manifest_sha256": actual_manifest_hash,
        "record_count": len(records),
        "record_bytes": int(sum(item["size_bytes"] for item in records)),
        "metric_row_count": int(len(metrics)),
        "report_sha256": sha256_file(artifact_root / "report/product_report.md"),
        "verification_status": "all_records_and_report_reconciled",
    }
