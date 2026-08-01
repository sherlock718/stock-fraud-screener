"""Provider-neutral free-data V1 vintage NAV and performance metrics.

This module consumes the schemas frozen by B1C.  It deliberately does not
contain a historical-study entry point: B1D exercises the engine with
synthetic evidence only, while B1E is responsible for any controlled run.

The engine keeps three boundaries separate:

* performance namespaces select one frozen B1B terminal-event policy;
* outcome namespaces retain observed, provider-confirmed, bounded-scenario,
  and unsupported/unresolved evidence in separate frames;
* risk-free namespaces never substitute the zero-rate diagnostic for DGS1MO.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd


FROZEN_B1C_MANIFEST_SHA256 = (
    "98635ab48c5f381a0145cc6ab99ff76e072bc24f3c2b04a669edb80371ee71df"
)
FROZEN_B1C_ARTIFACT_CLASS = "FREE_DATA_V1_PERFORMANCE_INPUT_EVIDENCE_B1C"

PERFORMANCE_NAMESPACES = (
    "observed_available_diagnostic",
    "best_free_evidence_full_accounting",
    "legacy_minus_50_percent_unsupported_exit",
    "conservative_terminal_loss_100_percent",
)
OUTCOME_NAMESPACES = (
    "observed",
    "bounded_scenario",
    "provider_confirmed",
    "unsupported_unresolved",
)
DGS1MO_NAMESPACE = "dgs1mo_alfred_2026_07_17"
ZERO_RATE_NAMESPACE = "zero_risk_free_sharpe_diagnostic"
RATE_NAMESPACES = (DGS1MO_NAMESPACE, ZERO_RATE_NAMESPACE)

TERMINAL_EVENT_TYPES = {"cash_merger", "bankruptcy", "delisting"}
CONTINUITY_EVENT_TYPES = {"ticker_change", "stock_merger"}
SUPPORTED_EVENT_TYPES = (
    TERMINAL_EVENT_TYPES | CONTINUITY_EVENT_TYPES | {"partial_cash_exit"}
)

_REQUIRED_ARTIFACT_PATHS = {
    "contracts/free_data_v1_performance_contract.json",
    *(f"contracts/namespaces/{name}.json" for name in PERFORMANCE_NAMESPACES),
    f"contracts/rates/{DGS1MO_NAMESPACE}.json",
    f"contracts/rates/{ZERO_RATE_NAMESPACE}.json",
    "outputs/requirements.parquet",
    "outputs/security_identity.parquet",
    "outputs/security_actions.parquet",
    "outputs/prices.parquet",
    "outputs/benchmark_requirements.parquet",
    "outputs/coverage.parquet",
    *(f"outputs/namespaces/{name}/eligibility.parquet" for name in PERFORMANCE_NAMESPACES),
    "support/rate_status.json",
}

_REQUIREMENT_COLUMNS = {
    "requirement_id",
    "stable_row_id",
    "instrument_role",
    "ticker",
    "provider_symbol",
    "exchange_calendar",
    "requirement_state",
    "decision_year",
    "benchmark_symbol",
    "decision_timestamp",
    "prediction_timestamp",
    "entry_timestamp",
    "target_exit_timestamp",
    "calendar_exit_timestamp",
    "holding_months",
    "weight",
    "planned_vintage_aum_usd",
    "planned_entry_notional_usd",
    "transaction_cost_rate_per_side",
    "transaction_cost_basis",
    "vintage_clock_status",
}
_IDENTITY_COLUMNS = {
    "requirement_id",
    "instrument_role",
    "stable_row_id",
    "ticker",
    "current_ticker_substitution_used",
    "ticker_chaining_used",
    "dated_security_lineage_complete",
    "certified_performance_identity_available",
}
_ACTION_COLUMNS = {
    "requirement_id",
    "instrument_role",
    "stable_row_id",
    "ticker",
    "deterministic_action",
    "provider_adjclose_action_semantics_certified",
    "forward_fill_across_unresolved_event_allowed",
    "event_inferred_from_disappearance_or_form_family",
    "unsupported_recovery_allowed_in_observed_namespace",
    "assumed_outcome_allowed_in_labels_or_training",
    "primary_return_available",
}
_COVERAGE_COLUMNS = {
    "requirement_id",
    "stable_row_id",
    "instrument_role",
    "ticker",
    "provider_symbol",
    "requirement_state",
    "assigned_benchmark_symbol",
    "entry_observed_common",
    "exit_observed_common",
    "required_month_end_count",
    "observed_common_month_end_count",
    "missing_common_month_end_count",
    "benchmark_gap_count",
    "benchmark_gap_scenario_imputation_allowed",
    "price_coverage_status",
    "relative_evidence_status",
}
_PRICE_COLUMNS = {
    "symbol",
    "total_return_close",
    "session_date",
    "market_close",
    "instrument_role",
    "exchange_calendar",
    "currency",
    "price_evidence_status",
    "adjustment_semantics",
}
_ELIGIBILITY_COLUMNS = {
    "requirement_id",
    "stable_row_id",
    "instrument_role",
    "ticker",
    "requirement_state",
    "namespace",
    "eligibility_state",
    "eligible_for_future_nav_engine",
    "scenario_triggered",
    "scenario_return_if_triggered",
    "assumed_outcome_used",
    "allowed_in_labels_or_training",
    "benchmark_gap_imputed",
    "performance_calculated",
}

_OUTCOME_LEDGER_COLUMNS = [
    "performance_namespace",
    "outcome_namespace",
    "requirement_id",
    "stable_row_id",
    "ticker",
    "event_type",
    "effective_timestamp",
    "source_id",
    "terminal_return",
    "scenario_triggered",
    "used_in_nav",
]


class EvidenceValidationError(RuntimeError):
    """Raised when evidence cannot satisfy the frozen fail-closed contract."""


class RiskFreeUnavailableError(EvidenceValidationError):
    """Raised when the selected risk-free namespace has no exact observations."""


@dataclass(frozen=True)
class EvidenceBundle:
    """In-memory representation of the provider-neutral B1C tables."""

    requirements: pd.DataFrame
    security_identity: pd.DataFrame
    security_actions: pd.DataFrame
    prices: pd.DataFrame
    benchmark_requirements: pd.DataFrame
    coverage: pd.DataFrame
    namespace_eligibility: Mapping[str, pd.DataFrame]
    performance_contract: Mapping[str, Any]
    namespace_contracts: Mapping[str, Mapping[str, Any]]
    rate_status: Mapping[str, Any]
    risk_free_observations: pd.DataFrame | None = None
    source_manifest_sha256: str | None = None
    source_manifest: Mapping[str, Any] | None = None


@dataclass
class PerformanceResult:
    """One physically isolated performance-namespace result."""

    available: bool
    performance_namespace: str
    risk_free_namespace: str
    unavailable_reasons: list[dict[str, Any]] = field(default_factory=list)
    vintage_nav: pd.DataFrame = field(default_factory=pd.DataFrame)
    aggregate_nav: pd.DataFrame = field(default_factory=pd.DataFrame)
    holding_ledger: pd.DataFrame = field(default_factory=pd.DataFrame)
    transaction_ledger: pd.DataFrame = field(default_factory=pd.DataFrame)
    capital_ledger: pd.DataFrame = field(default_factory=pd.DataFrame)
    outcome_ledgers: dict[str, pd.DataFrame] = field(default_factory=dict)
    coverage: dict[str, Any] = field(default_factory=dict)
    vintage_metrics: dict[int, dict[str, dict[str, Any]]] = field(default_factory=dict)
    aggregate_metrics: dict[str, dict[str, Any]] = field(default_factory=dict)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise EvidenceValidationError(f"{label} schema missing columns: {missing}")


def _normalise_timestamp_series(series: pd.Series, label: str) -> pd.Series:
    values = pd.to_datetime(series, utc=True, errors="coerce")
    if values.isna().any():
        raise EvidenceValidationError(f"{label} contains invalid timestamps")
    return values


def _safe_record_path(root: Path, relative: Any) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise EvidenceValidationError("manifest contains an unsafe record path")
    candidate = (root / relative).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as exc:
        raise EvidenceValidationError(
            f"manifest record escapes artifact root: {relative}"
        ) from exc
    return candidate


def _verify_manifest_records(
    root: Path,
    manifest: Mapping[str, Any],
) -> None:
    records = manifest.get("records")
    if not isinstance(records, list) or not records:
        raise EvidenceValidationError("manifest records are absent")
    paths = [item.get("path") for item in records if isinstance(item, Mapping)]
    if len(paths) != len(records) or len(set(paths)) != len(paths):
        raise EvidenceValidationError("manifest record paths are invalid or duplicated")
    if not _REQUIRED_ARTIFACT_PATHS.issubset(set(paths)):
        missing = sorted(_REQUIRED_ARTIFACT_PATHS - set(paths))
        raise EvidenceValidationError(f"manifest omits required B1C records: {missing}")
    index = {item["path"]: item for item in records}
    for relative, record in index.items():
        path = _safe_record_path(root, relative)
        if not path.is_file():
            raise EvidenceValidationError(f"manifest record is missing: {relative}")
        expected_size = record.get("size_bytes")
        expected_hash = record.get("sha256")
        if not isinstance(expected_size, int) or expected_size < 0:
            raise EvidenceValidationError(f"manifest record size is invalid: {relative}")
        if not isinstance(expected_hash, str) or len(expected_hash) != 64:
            raise EvidenceValidationError(f"manifest record hash is invalid: {relative}")
        if path.stat().st_size != expected_size:
            raise EvidenceValidationError(f"manifest record size mismatch: {relative}")
        if sha256_file(path) != expected_hash:
            raise EvidenceValidationError(f"manifest record hash mismatch: {relative}")

    validated_inputs = manifest.get("validated_inputs")
    if not isinstance(validated_inputs, list) or not validated_inputs:
        raise EvidenceValidationError("manifest validated inputs are absent")
    for item in validated_inputs:
        copied_path = item.get("copied_path")
        if copied_path not in index:
            raise EvidenceValidationError(
                f"validated input is not a manifest record: {copied_path}"
            )
        record = index[copied_path]
        if (
            record.get("size_bytes") != item.get("size_bytes")
            or record.get("sha256") != item.get("sha256")
        ):
            raise EvidenceValidationError(
                f"validated input metadata disagrees with record: {copied_path}"
            )

    code_lineage = manifest.get("code_lineage")
    if not isinstance(code_lineage, list) or not code_lineage:
        raise EvidenceValidationError("manifest code lineage is absent")
    repository_root = Path(__file__).resolve().parents[1]
    for item in code_lineage:
        relative = item.get("path")
        path = _safe_record_path(repository_root, relative)
        if not path.is_file():
            raise EvidenceValidationError(f"code-lineage file is missing: {relative}")
        if (
            path.stat().st_size != item.get("size_bytes")
            or sha256_file(path) != item.get("sha256")
        ):
            raise EvidenceValidationError(f"code-lineage file drifted: {relative}")


def load_frozen_b1c_evidence(
    root: str | Path,
    *,
    expected_manifest_sha256: str = FROZEN_B1C_MANIFEST_SHA256,
) -> EvidenceBundle:
    """Verify every B1C record, then load and cross-check its frozen schemas."""
    artifact_root = Path(root)
    manifest_path = artifact_root / "manifest.json"
    if not manifest_path.is_file():
        raise EvidenceValidationError("B1C manifest is missing")
    actual_hash = sha256_file(manifest_path)
    if actual_hash != expected_manifest_sha256:
        raise EvidenceValidationError(
            "B1C manifest hash mismatch: "
            f"expected={expected_manifest_sha256} actual={actual_hash}"
        )
    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise EvidenceValidationError("B1C manifest is not valid JSON") from exc
    if (
        manifest.get("artifact_class") != FROZEN_B1C_ARTIFACT_CLASS
        or manifest.get("schema_version") != 1
    ):
        raise EvidenceValidationError("B1C manifest identity/schema drifted")
    claim = manifest.get("claim", {})
    if any(
        bool(claim.get(name))
        for name in ("performance_calculated", "nav_created", "backtest_run")
    ):
        raise EvidenceValidationError("B1C artifact improperly claims performance")
    _verify_manifest_records(artifact_root, manifest)

    contract = json.loads(
        (artifact_root / "contracts/free_data_v1_performance_contract.json").read_text()
    )
    namespace_contracts: dict[str, Mapping[str, Any]] = {}
    eligibility: dict[str, pd.DataFrame] = {}
    for namespace in PERFORMANCE_NAMESPACES:
        namespace_contracts[namespace] = json.loads(
            (artifact_root / f"contracts/namespaces/{namespace}.json").read_text()
        )
        eligibility[namespace] = pd.read_parquet(
            artifact_root / f"outputs/namespaces/{namespace}/eligibility.parquet"
        )
    bundle = EvidenceBundle(
        requirements=pd.read_parquet(artifact_root / "outputs/requirements.parquet"),
        security_identity=pd.read_parquet(
            artifact_root / "outputs/security_identity.parquet"
        ),
        security_actions=pd.read_parquet(
            artifact_root / "outputs/security_actions.parquet"
        ),
        prices=pd.read_parquet(artifact_root / "outputs/prices.parquet"),
        benchmark_requirements=pd.read_parquet(
            artifact_root / "outputs/benchmark_requirements.parquet"
        ),
        coverage=pd.read_parquet(artifact_root / "outputs/coverage.parquet"),
        namespace_eligibility=eligibility,
        performance_contract=contract,
        namespace_contracts=namespace_contracts,
        rate_status=json.loads(
            (artifact_root / "support/rate_status.json").read_text()
        ),
        risk_free_observations=None,
        source_manifest_sha256=actual_hash,
        source_manifest=manifest,
    )
    validate_evidence_bundle(bundle, exact_frozen_b1c=True)
    return bundle


def _validate_cross_table_identity(bundle: EvidenceBundle) -> None:
    requirements = bundle.requirements
    requirement_ids = set(requirements["requirement_id"])
    if requirements["requirement_id"].isna().any() or requirements[
        "requirement_id"
    ].duplicated().any():
        raise EvidenceValidationError("requirement_id is null or duplicated")

    for label, frame in (
        ("security identity", bundle.security_identity),
        ("security actions", bundle.security_actions),
        ("coverage", bundle.coverage),
    ):
        if frame["requirement_id"].duplicated().any() or set(
            frame["requirement_id"]
        ) != requirement_ids:
            raise EvidenceValidationError(f"{label} requirement coverage drifted")
        joined = requirements[
            ["requirement_id", "instrument_role", "stable_row_id", "ticker"]
        ].merge(
            frame[
                ["requirement_id", "instrument_role", "stable_row_id", "ticker"]
            ],
            on="requirement_id",
            suffixes=("_required", "_actual"),
            validate="one_to_one",
        )
        for column in ("instrument_role", "stable_row_id", "ticker"):
            left = joined[f"{column}_required"].fillna("<NA>").astype(str)
            right = joined[f"{column}_actual"].fillna("<NA>").astype(str)
            if not left.eq(right).all():
                raise EvidenceValidationError(
                    f"{label} {column} identity reconciliation failed"
                )

    holdings = requirements[requirements["instrument_role"].eq("holding")]
    if holdings["stable_row_id"].isna().any() or holdings[
        "stable_row_id"
    ].duplicated().any():
        raise EvidenceValidationError("holding stable_row_id contract drifted")
    benchmarks = requirements[requirements["instrument_role"].eq("benchmark")]
    if len(benchmarks) == 0 or benchmarks["ticker"].duplicated().any():
        raise EvidenceValidationError("benchmark master identity contract drifted")
    if not set(holdings["benchmark_symbol"]).issubset(set(benchmarks["ticker"])):
        raise EvidenceValidationError("holding benchmark assignment is unresolved")

    identity = bundle.security_identity.set_index("requirement_id")
    actions = bundle.security_actions.set_index("requirement_id")
    for row in holdings.itertuples(index=False):
        ident = identity.loc[row.requirement_id]
        action = actions.loc[row.requirement_id]
        if str(row.ticker) != str(row.provider_symbol):
            raise EvidenceValidationError(
                f"prohibited current ticker substitution: {row.requirement_id}"
            )
        if bool(ident["current_ticker_substitution_used"]):
            raise EvidenceValidationError(
                f"current ticker substitution flag set: {row.requirement_id}"
            )
        if bool(ident["ticker_chaining_used"]):
            raise EvidenceValidationError(
                f"unapproved ticker chaining flag set: {row.requirement_id}"
            )
        prohibited_flags = (
            "forward_fill_across_unresolved_event_allowed",
            "event_inferred_from_disappearance_or_form_family",
            "unsupported_recovery_allowed_in_observed_namespace",
            "assumed_outcome_allowed_in_labels_or_training",
        )
        if any(bool(action[name]) for name in prohibited_flags):
            raise EvidenceValidationError(
                f"prohibited action policy enabled: {row.requirement_id}"
            )


def _validate_vintage_contract(requirements: pd.DataFrame) -> None:
    holdings = requirements[requirements["instrument_role"].eq("holding")].copy()
    holdings["decision_timestamp"] = _normalise_timestamp_series(
        holdings["decision_timestamp"], "holding decision_timestamp"
    )
    holdings["prediction_timestamp"] = _normalise_timestamp_series(
        holdings["prediction_timestamp"], "holding prediction_timestamp"
    )
    holdings["entry_timestamp"] = _normalise_timestamp_series(
        holdings["entry_timestamp"], "holding entry_timestamp"
    )
    if not (
        holdings["decision_timestamp"].dt.month.eq(7)
        & holdings["decision_timestamp"].dt.day.eq(2)
    ).all():
        raise EvidenceValidationError("holding decisions are not annual July 2")
    if not (
        holdings["decision_timestamp"] < holdings["prediction_timestamp"]
    ).all() or not (
        holdings["prediction_timestamp"] < holdings["entry_timestamp"]
    ).all():
        raise EvidenceValidationError("decision/prediction/entry order drifted")
    if not pd.to_numeric(holdings["holding_months"], errors="coerce").eq(36).all():
        raise EvidenceValidationError("holding period is not exactly 36 months")
    rates = pd.to_numeric(
        holdings["transaction_cost_rate_per_side"], errors="coerce"
    )
    if not np.isclose(rates.to_numpy(dtype=float), 0.0025).all():
        raise EvidenceValidationError("transaction cost is not 25 bps per side")
    if not holdings["transaction_cost_basis"].eq(
        "absolute_actual_traded_notional"
    ).all():
        raise EvidenceValidationError("transaction-cost basis drifted")

    numeric = (
        "weight",
        "planned_vintage_aum_usd",
        "planned_entry_notional_usd",
    )
    if any(
        pd.to_numeric(holdings[name], errors="coerce").isna().any()
        or pd.to_numeric(holdings[name], errors="coerce").le(0).any()
        for name in numeric
    ):
        raise EvidenceValidationError("holding weight/capital inputs are invalid")
    for year, group in holdings.groupby("decision_year", sort=True):
        weights = pd.to_numeric(group["weight"], errors="coerce")
        aum = pd.to_numeric(group["planned_vintage_aum_usd"], errors="coerce")
        notionals = pd.to_numeric(
            group["planned_entry_notional_usd"], errors="coerce"
        )
        if not np.isclose(weights.sum(), 1.0, atol=1e-12):
            raise EvidenceValidationError(f"vintage weights do not sum to one: {year}")
        if aum.nunique() != 1 or not np.isclose(notionals.sum(), aum.iloc[0]):
            raise EvidenceValidationError(f"vintage capital does not reconcile: {year}")
        if not np.allclose(notionals, aum.iloc[0] * weights, atol=1e-8):
            raise EvidenceValidationError(f"holding notionals do not match weights: {year}")

    matured = holdings[holdings["calendar_exit_timestamp"].notna()].copy()
    if len(matured):
        matured["target_exit_timestamp"] = _normalise_timestamp_series(
            matured["target_exit_timestamp"], "target_exit_timestamp"
        )
        matured["calendar_exit_timestamp"] = _normalise_timestamp_series(
            matured["calendar_exit_timestamp"], "calendar_exit_timestamp"
        )
        expected = matured["entry_timestamp"] + pd.DateOffset(months=36)
        if not expected.eq(matured["target_exit_timestamp"]).all():
            raise EvidenceValidationError("36-month target exits drifted")
        delay = matured["calendar_exit_timestamp"] - matured["target_exit_timestamp"]
        if (delay < pd.Timedelta(0)).any() or (delay > pd.Timedelta(days=10)).any():
            raise EvidenceValidationError("calendar exits violate the ten-day rule")


def _validate_namespace_contracts(bundle: EvidenceBundle) -> None:
    master = bundle.performance_contract
    if (
        master.get("schema_version") != 1
        or master.get("contract_id") != "free_data_v1_performance_contract_b1b"
    ):
        raise EvidenceValidationError("performance contract identity drifted")
    valuation = master.get("valuation", {})
    transaction = master.get("transaction_cost", {})
    if (
        valuation.get("holding_months") != 36
        or valuation.get("overlapping_vintages_independent") is not True
        or valuation.get("later_vintage_rebalances_earlier_vintage") is not False
        or transaction.get("rate_per_side") != 0.0025
        or transaction.get("half_turnover_multiplier") is not False
    ):
        raise EvidenceValidationError("frozen valuation/cost contract drifted")
    master_namespaces = master.get("namespaces")
    if not isinstance(master_namespaces, Mapping) or set(master_namespaces) != set(
        PERFORMANCE_NAMESPACES
    ):
        raise EvidenceValidationError("performance namespace set drifted")
    for namespace in PERFORMANCE_NAMESPACES:
        stored = dict(bundle.namespace_contracts.get(namespace, {}))
        if stored.pop("namespace", None) != namespace:
            raise EvidenceValidationError(f"namespace identity drifted: {namespace}")
        if stored != dict(master_namespaces[namespace]):
            raise EvidenceValidationError(f"namespace contract drifted: {namespace}")
        eligibility = bundle.namespace_eligibility.get(namespace)
        if not isinstance(eligibility, pd.DataFrame):
            raise EvidenceValidationError(f"namespace eligibility absent: {namespace}")
        _require_columns(eligibility, _ELIGIBILITY_COLUMNS, f"{namespace} eligibility")
        if not eligibility["namespace"].eq(namespace).all():
            raise EvidenceValidationError(f"eligibility namespace drifted: {namespace}")
        if eligibility["requirement_id"].duplicated().any() or set(
            eligibility["requirement_id"]
        ) != set(bundle.requirements["requirement_id"]):
            raise EvidenceValidationError(
                f"eligibility requirement coverage drifted: {namespace}"
            )
        prohibited = (
            "scenario_triggered",
            "assumed_outcome_used",
            "allowed_in_labels_or_training",
            "benchmark_gap_imputed",
            "performance_calculated",
        )
        if any(eligibility[name].astype(bool).any() for name in prohibited):
            raise EvidenceValidationError(
                f"B1C eligibility contains a precomputed outcome: {namespace}"
            )


def _validate_rate_namespaces(bundle: EvidenceBundle) -> None:
    rates = bundle.performance_contract.get("rates", {})
    if set(rates) != set(RATE_NAMESPACES):
        raise EvidenceValidationError("risk-free namespace set drifted")
    status = bundle.rate_status
    zero = status.get("zero_risk_free_namespace", {})
    if (
        zero.get("namespace") != ZERO_RATE_NAMESPACE
        or zero.get("risk_free_return") != 0.0
        or zero.get("diagnostic_only") is not True
        or zero.get("physically_and_semantically_separate_from_dgs1mo") is not True
    ):
        raise EvidenceValidationError("zero-rate diagnostic namespace drifted")
    if status.get("dgs1mo_namespace") != DGS1MO_NAMESPACE:
        raise EvidenceValidationError("DGS1MO namespace identity drifted")
    if status.get("dgs1mo_available") is False and bundle.risk_free_observations is not None:
        raise EvidenceValidationError(
            "DGS1MO observations supplied while frozen status is unavailable"
        )
    if status.get("dgs1mo_available") is True and bundle.risk_free_observations is None:
        raise EvidenceValidationError("DGS1MO marked available without observations")


def validate_evidence_bundle(
    bundle: EvidenceBundle,
    *,
    exact_frozen_b1c: bool = False,
) -> None:
    """Fail closed on schema, identity, coverage, namespace, and rate drift."""
    _require_columns(bundle.requirements, _REQUIREMENT_COLUMNS, "requirements")
    _require_columns(bundle.security_identity, _IDENTITY_COLUMNS, "security identity")
    _require_columns(bundle.security_actions, _ACTION_COLUMNS, "security actions")
    _require_columns(bundle.coverage, _COVERAGE_COLUMNS, "coverage")
    _require_columns(bundle.prices, _PRICE_COLUMNS, "prices")
    _validate_cross_table_identity(bundle)
    _validate_vintage_contract(bundle.requirements)
    _validate_namespace_contracts(bundle)
    _validate_rate_namespaces(bundle)

    prices = bundle.prices.copy()
    prices["market_close"] = _normalise_timestamp_series(
        prices["market_close"], "price market_close"
    )
    prices["total_return_close"] = pd.to_numeric(
        prices["total_return_close"], errors="coerce"
    )
    if (
        prices[["symbol", "market_close"]].isna().any().any()
        or prices.duplicated(["symbol", "market_close"]).any()
        or not np.isfinite(prices["total_return_close"]).all()
        or prices["total_return_close"].le(0).any()
    ):
        raise EvidenceValidationError("price primary key/value contract drifted")
    if not prices["currency"].eq("USD").all():
        raise EvidenceValidationError("non-USD price evidence is unsupported in V1")

    coverage = bundle.coverage
    if coverage["benchmark_gap_scenario_imputation_allowed"].astype(bool).any():
        raise EvidenceValidationError("benchmark-gap imputation was enabled")
    if pd.to_numeric(coverage["benchmark_gap_count"], errors="coerce").fillna(0).gt(0).any():
        raise EvidenceValidationError("declared benchmark-session gaps are unsupported")

    if exact_frozen_b1c:
        holdings = bundle.requirements[
            bundle.requirements["instrument_role"].eq("holding")
        ]
        benchmarks = bundle.requirements[
            bundle.requirements["instrument_role"].eq("benchmark")
        ]
        if len(holdings) != 180 or len(benchmarks) != 4:
            raise EvidenceValidationError("frozen B1C 180-plus-four boundary drifted")
        if holdings["requirement_state"].value_counts().to_dict() != {
            "matured_2015_2023": 135,
            "open_2024_2026": 45,
        }:
            raise EvidenceValidationError("frozen B1C matured/open coverage drifted")
        if len(bundle.prices) != 512_413:
            raise EvidenceValidationError("frozen B1C price row count drifted")
        if len(bundle.benchmark_requirements) != 4:
            raise EvidenceValidationError("frozen B1C benchmark table drifted")
        matured_ids = set(
            holdings.loc[
                holdings["requirement_state"].eq("matured_2015_2023"),
                "requirement_id",
            ]
        )
        holding_coverage = coverage[coverage["instrument_role"].eq("holding")]
        if not holding_coverage["entry_observed_common"].all():
            raise EvidenceValidationError("frozen B1C common entry coverage drifted")
        matured_coverage = holding_coverage[
            holding_coverage["requirement_id"].isin(matured_ids)
        ]
        if (
            not matured_coverage["exit_observed_common"].all()
            or not pd.to_numeric(
                matured_coverage["required_month_end_count"], errors="coerce"
            ).eq(36).all()
            or not pd.to_numeric(
                holding_coverage["missing_common_month_end_count"], errors="coerce"
            ).fillna(0).eq(0).all()
        ):
            raise EvidenceValidationError("frozen B1C session coverage drifted")
        manifest_coverage = (bundle.source_manifest or {}).get("coverage", {})
        if (
            manifest_coverage.get("requirement_rows") != 184
            or manifest_coverage.get("common_entry_holding_rows") != 180
            or manifest_coverage.get("matured_common_exit_holding_rows") != 135
            or manifest_coverage.get("missing_common_month_end_observations") != 0
        ):
            raise EvidenceValidationError("manifest/table coverage reconciliation failed")


class _PriceBook:
    def __init__(self, prices: pd.DataFrame):
        frame = prices.copy()
        frame["market_close"] = pd.to_datetime(frame["market_close"], utc=True)
        frame["month"] = frame["market_close"].dt.tz_localize(None).dt.to_period("M")
        self._price = {
            (str(row.symbol), pd.Timestamp(row.market_close)): float(row.total_return_close)
            for row in frame.itertuples(index=False)
        }
        self._symbols = set(frame["symbol"].astype(str))
        self._month_ends: dict[str, dict[pd.Period, pd.Timestamp]] = {}
        sessions = frame[["exchange_calendar", "month", "market_close"]].drop_duplicates()
        for calendar, group in sessions.groupby("exchange_calendar", sort=True):
            self._month_ends[str(calendar)] = (
                group.sort_values("market_close")
                .groupby("month", sort=True)
                .tail(1)
                .set_index("month")["market_close"]
                .to_dict()
            )

    def has(self, symbol: str, instant: pd.Timestamp) -> bool:
        return (str(symbol), pd.Timestamp(instant)) in self._price

    def get(self, symbol: str, instant: pd.Timestamp) -> float:
        key = (str(symbol), pd.Timestamp(instant))
        if key not in self._price:
            raise EvidenceValidationError(
                f"missing exact price observation: symbol={symbol} instant={instant.isoformat()}"
            )
        return self._price[key]

    def month_ends(
        self,
        calendar: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> list[pd.Timestamp]:
        if calendar not in self._month_ends:
            raise EvidenceValidationError(f"price calendar is absent: {calendar}")
        return [
            instant
            for _, instant in sorted(self._month_ends[calendar].items())
            if instant >= start and instant <= end
        ]


def _event_from_action(action: pd.Series) -> dict[str, Any] | None:
    event_type = action.get("event_type")
    if pd.isna(event_type) or not str(event_type).strip():
        return None
    event_type = str(event_type)
    if event_type not in SUPPORTED_EVENT_TYPES:
        raise EvidenceValidationError(f"unsupported terminal event type: {event_type}")
    effective = pd.to_datetime(action.get("event_effective_timestamp"), utc=True, errors="coerce")
    if pd.isna(effective):
        raise EvidenceValidationError("terminal event lacks an exact effective timestamp")
    outcome = str(action.get("outcome_namespace"))
    if outcome not in {"observed", "provider_confirmed", "unsupported_unresolved"}:
        raise EvidenceValidationError(
            "input terminal event has an invalid outcome namespace"
        )
    source_id = action.get("event_source_id")
    if outcome in {"observed", "provider_confirmed"} and (
        pd.isna(source_id) or not str(source_id).strip()
    ):
        raise EvidenceValidationError("resolved terminal event lacks source lineage")
    return {
        "event_type": event_type,
        "effective_timestamp": pd.Timestamp(effective),
        "outcome_namespace": outcome,
        "source_id": None if pd.isna(source_id) else str(source_id),
        "successor_symbol": (
            None
            if pd.isna(action.get("successor_symbol"))
            else str(action.get("successor_symbol"))
        ),
        "successor_share_ratio": pd.to_numeric(
            action.get("successor_share_ratio"), errors="coerce"
        ),
        "cash_per_old_share": pd.to_numeric(
            action.get("cash_per_old_share"), errors="coerce"
        ),
        "cash_fraction": pd.to_numeric(action.get("cash_fraction"), errors="coerce"),
        "terminal_return": pd.to_numeric(
            action.get("terminal_total_return"), errors="coerce"
        ),
        "terms_complete": bool(action.get("event_terms_complete", False)),
        "primary_return_available": bool(
            action.get("primary_return_available", False)
        ),
        "deterministic_action": str(action.get("deterministic_action")),
    }


def _validate_event_for_holding(event: dict[str, Any], requirement: pd.Series) -> None:
    entry = pd.Timestamp(requirement["entry_timestamp"])
    exit_timestamp = pd.Timestamp(requirement["calendar_exit_timestamp"])
    if not (entry < event["effective_timestamp"] <= exit_timestamp):
        raise EvidenceValidationError("terminal event lies outside the holding window")
    outcome = event["outcome_namespace"]
    event_type = event["event_type"]
    if outcome == "unsupported_unresolved":
        if (
            event["terms_complete"]
            or event["primary_return_available"]
            or event["deterministic_action"] != "unresolved"
        ):
            raise EvidenceValidationError("unsupported terminal event is marked complete")
        return
    if (
        not event["terms_complete"]
        or not event["primary_return_available"]
        or event["deterministic_action"] == "unresolved"
    ):
        raise EvidenceValidationError("resolved terminal-event terms are incomplete")
    if event_type in CONTINUITY_EVENT_TYPES:
        if (
            not event["successor_symbol"]
            or not np.isfinite(event["successor_share_ratio"])
            or event["successor_share_ratio"] <= 0
        ):
            raise EvidenceValidationError("ticker/stock continuity terms are incomplete")
    elif event_type == "cash_merger":
        if not np.isfinite(event["cash_per_old_share"]) or event[
            "cash_per_old_share"
        ] < 0:
            raise EvidenceValidationError("cash-merger terms are incomplete")
    elif event_type == "partial_cash_exit":
        if (
            not np.isfinite(event["cash_per_old_share"])
            or event["cash_per_old_share"] < 0
            or not np.isfinite(event["cash_fraction"])
            or not 0 < event["cash_fraction"] < 1
        ):
            raise EvidenceValidationError("partial-exit terms are incomplete")
    elif event_type in {"bankruptcy", "delisting"}:
        if (
            not np.isfinite(event["terminal_return"])
            or event["terminal_return"] < -1
        ):
            raise EvidenceValidationError("terminal return is incomplete or unbounded")


def _rate_returns(
    bundle: EvidenceBundle,
    namespace: str,
    dates: pd.Series,
) -> np.ndarray:
    if namespace not in RATE_NAMESPACES:
        raise EvidenceValidationError(f"unknown risk-free namespace: {namespace}")
    if namespace == ZERO_RATE_NAMESPACE:
        zero = bundle.rate_status.get("zero_risk_free_namespace", {})
        if (
            zero.get("namespace") != ZERO_RATE_NAMESPACE
            or zero.get("risk_free_return") != 0.0
            or zero.get("diagnostic_only") is not True
        ):
            raise EvidenceValidationError("zero-rate diagnostic contract drifted")
        return np.zeros(len(dates), dtype=float)

    if bundle.rate_status.get("dgs1mo_available") is not True:
        raise RiskFreeUnavailableError(
            "DGS1MO calculation unavailable: exact frozen observations are absent"
        )
    observations = bundle.risk_free_observations
    if observations is None:
        raise RiskFreeUnavailableError("DGS1MO observations are absent")
    required = {"namespace", "interval_end", "risk_free_return"}
    _require_columns(observations, required, "DGS1MO observations")
    if not observations["namespace"].eq(DGS1MO_NAMESPACE).all():
        raise EvidenceValidationError("risk-free observations are not DGS1MO")
    series = observations.copy()
    series["interval_end"] = pd.to_datetime(series["interval_end"], utc=True)
    if series["interval_end"].duplicated().any():
        raise EvidenceValidationError("DGS1MO interval observations are duplicated")
    aligned = series.set_index("interval_end")["risk_free_return"].reindex(
        pd.DatetimeIndex(pd.to_datetime(dates, utc=True))
    )
    if aligned.isna().any():
        raise RiskFreeUnavailableError(
            "DGS1MO interval coverage is incomplete; carry/interpolation prohibited"
        )
    values = pd.to_numeric(aligned, errors="coerce").to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise EvidenceValidationError("DGS1MO interval returns are invalid")
    return values


def _empty_outcome_ledgers() -> dict[str, pd.DataFrame]:
    return {
        namespace: pd.DataFrame(columns=_OUTCOME_LEDGER_COLUMNS)
        for namespace in OUTCOME_NAMESPACES
    }


def _append_outcome(
    ledgers: dict[str, list[dict[str, Any]]],
    namespace: str,
    record: dict[str, Any],
) -> None:
    if namespace not in OUTCOME_NAMESPACES:
        raise EvidenceValidationError(f"unknown outcome namespace: {namespace}")
    row = dict(record)
    row["outcome_namespace"] = namespace
    ledgers[namespace].append(row)


def _freeze_outcome_ledgers(
    ledgers: dict[str, list[dict[str, Any]]],
) -> dict[str, pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    for namespace in OUTCOME_NAMESPACES:
        frame = pd.DataFrame.from_records(
            ledgers[namespace], columns=_OUTCOME_LEDGER_COLUMNS
        )
        if len(frame):
            frame = frame.sort_values(
                ["requirement_id", "effective_timestamp"], na_position="last"
            ).reset_index(drop=True)
        outputs[namespace] = frame
    return outputs


def _resolve_outcomes(
    bundle: EvidenceBundle,
    namespace: str,
    holdings: pd.DataFrame,
) -> tuple[dict[str, dict[str, Any] | None], dict[str, pd.DataFrame], list[dict[str, Any]]]:
    contract = bundle.namespace_contracts[namespace]
    actions = bundle.security_actions.set_index("requirement_id")
    resolved: dict[str, dict[str, Any] | None] = {}
    ledger_rows: dict[str, list[dict[str, Any]]] = {
        name: [] for name in OUTCOME_NAMESPACES
    }
    reasons: list[dict[str, Any]] = []
    for requirement in holdings.itertuples(index=False):
        action = actions.loc[requirement.requirement_id]
        event = _event_from_action(action)
        base = {
            "performance_namespace": namespace,
            "requirement_id": requirement.requirement_id,
            "stable_row_id": requirement.stable_row_id,
            "ticker": requirement.ticker,
            "event_type": "complete_price_path" if event is None else event["event_type"],
            "effective_timestamp": (
                pd.NaT if event is None else event["effective_timestamp"]
            ),
            "source_id": None if event is None else event["source_id"],
            "terminal_return": np.nan,
            "scenario_triggered": False,
            "used_in_nav": True,
        }
        if event is None:
            _append_outcome(ledger_rows, "observed", base)
            resolved[requirement.requirement_id] = None
            continue
        req = holdings.loc[
            holdings["requirement_id"].eq(requirement.requirement_id)
        ].iloc[0]
        _validate_event_for_holding(event, req)
        outcome = event["outcome_namespace"]
        event_base = {
            **base,
            "terminal_return": event.get("terminal_return"),
        }
        if outcome == "observed":
            _append_outcome(ledger_rows, "observed", event_base)
            resolved[requirement.requirement_id] = event
        elif outcome == "provider_confirmed":
            if namespace == "observed_available_diagnostic":
                event_base["used_in_nav"] = False
                reasons.append(
                    {
                        "code": "provider_confirmed_outcome_not_observed",
                        "requirement_id": requirement.requirement_id,
                    }
                )
            _append_outcome(ledger_rows, "provider_confirmed", event_base)
            resolved[requirement.requirement_id] = event
        else:
            unresolved = {**event_base, "used_in_nav": False}
            _append_outcome(ledger_rows, "unsupported_unresolved", unresolved)
            assumption = contract.get("terminal_assumption")
            trigger = contract.get("trigger")
            if (
                event["event_type"] in TERMINAL_EVENT_TYPES
                and trigger == "unsupported_terminal_exit_only"
                and assumption in (-0.5, -1.0)
            ):
                scenario = dict(event)
                scenario["outcome_namespace"] = "bounded_scenario"
                scenario["terminal_return"] = float(assumption)
                scenario["terms_complete"] = True
                scenario["source_id"] = f"frozen_contract:{namespace}"
                bounded = {
                    **event_base,
                    "source_id": scenario["source_id"],
                    "terminal_return": float(assumption),
                    "scenario_triggered": True,
                    "used_in_nav": True,
                }
                _append_outcome(ledger_rows, "bounded_scenario", bounded)
                resolved[requirement.requirement_id] = scenario
            else:
                reasons.append(
                    {
                        "code": "unsupported_terminal_outcome",
                        "requirement_id": requirement.requirement_id,
                        "event_type": event["event_type"],
                    }
                )
                resolved[requirement.requirement_id] = event
    return resolved, _freeze_outcome_ledgers(ledger_rows), reasons


def _holding_schedule(
    requirement: pd.Series,
    price_book: _PriceBook,
) -> list[pd.Timestamp]:
    entry = pd.Timestamp(requirement["entry_timestamp"])
    exit_timestamp = pd.Timestamp(requirement["calendar_exit_timestamp"])
    month_ends = price_book.month_ends(
        str(requirement["exchange_calendar"]), entry, exit_timestamp
    )
    return sorted(set([entry, *month_ends, exit_timestamp]))


def _build_holding_path(
    requirement: pd.Series,
    event: dict[str, Any] | None,
    price_book: _PriceBook,
    namespace: str,
) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    requirement_id = str(requirement["requirement_id"])
    ticker = str(requirement["provider_symbol"])
    benchmark = str(requirement["benchmark_symbol"])
    entry = pd.Timestamp(requirement["entry_timestamp"])
    exit_timestamp = pd.Timestamp(requirement["calendar_exit_timestamp"])
    schedule = _holding_schedule(requirement, price_book)
    coverage_rows: list[dict[str, Any]] = []
    for instant in schedule:
        if not price_book.has(benchmark, instant):
            raise EvidenceValidationError(
                "benchmark-session gap; imputation prohibited: "
                f"{requirement_id} {instant.isoformat()}"
            )
        coverage_rows.append(
            {
                "requirement_id": requirement_id,
                "instant": instant,
                "benchmark_symbol": benchmark,
                "benchmark_observed": True,
            }
        )
    if not price_book.has(ticker, entry):
        raise EvidenceValidationError(f"holding entry price is absent: {requirement_id}")
    if not price_book.has(benchmark, entry):
        raise EvidenceValidationError(f"benchmark entry price is absent: {requirement_id}")

    planned_notional = float(requirement["planned_entry_notional_usd"])
    rate = float(requirement["transaction_cost_rate_per_side"])
    gross_shares = planned_notional / price_book.get(ticker, entry)
    benchmark_shares = planned_notional / price_book.get(benchmark, entry)
    gross_cash = 0.0
    benchmark_cash = 0.0
    portfolio_costs = planned_notional * rate
    benchmark_costs = planned_notional * rate
    current_symbol = ticker
    event_applied = False
    previous_stock_value = planned_notional
    transactions: list[dict[str, Any]] = [
        {
            "decision_year": int(requirement["decision_year"]),
            "requirement_id": requirement_id,
            "date": entry,
            "stream": "portfolio",
            "side": "entry_buy",
            "actual_traded_notional": planned_notional,
            "cost_rate": rate,
            "transaction_cost": planned_notional * rate,
        },
        {
            "decision_year": int(requirement["decision_year"]),
            "requirement_id": requirement_id,
            "date": entry,
            "stream": "benchmark",
            "side": "entry_buy",
            "actual_traded_notional": planned_notional,
            "cost_rate": rate,
            "transaction_cost": planned_notional * rate,
        },
    ]
    rows: list[dict[str, Any]] = []

    for instant in schedule:
        if event is not None and not event_applied and event["effective_timestamp"] <= instant:
            event_type = event["event_type"]
            old_shares = gross_shares
            if event["outcome_namespace"] == "bounded_scenario":
                gross_cash += previous_stock_value * (1.0 + event["terminal_return"])
                gross_shares = 0.0
            elif event_type in CONTINUITY_EVENT_TYPES:
                cash_term = (
                    float(event["cash_per_old_share"])
                    if np.isfinite(event["cash_per_old_share"])
                    else 0.0
                )
                gross_cash += old_shares * cash_term
                gross_shares = old_shares * float(event["successor_share_ratio"])
                current_symbol = str(event["successor_symbol"])
            elif event_type == "cash_merger":
                gross_cash += old_shares * float(event["cash_per_old_share"])
                gross_shares = 0.0
            elif event_type == "partial_cash_exit":
                fraction = float(event["cash_fraction"])
                gross_cash += old_shares * fraction * float(event["cash_per_old_share"])
                gross_shares = old_shares * (1.0 - fraction)
            elif event_type in {"bankruptcy", "delisting"}:
                gross_cash += previous_stock_value * (1.0 + event["terminal_return"])
                gross_shares = 0.0
            else:
                raise EvidenceValidationError(f"unhandled terminal event: {event_type}")
            event_applied = True

        if gross_shares > 0:
            stock_price = price_book.get(current_symbol, instant)
            stock_value = gross_shares * stock_price
            previous_stock_value = stock_value
        else:
            stock_value = 0.0
        portfolio_gross = gross_cash + stock_value
        benchmark_value = benchmark_cash + benchmark_shares * price_book.get(
            benchmark, instant
        )

        if instant == exit_timestamp:
            if gross_shares > 0:
                exit_notional = stock_value
                exit_cost = exit_notional * rate
                portfolio_costs += exit_cost
                transactions.append(
                    {
                        "decision_year": int(requirement["decision_year"]),
                        "requirement_id": requirement_id,
                        "date": instant,
                        "stream": "portfolio",
                        "side": "exit_sell",
                        "actual_traded_notional": exit_notional,
                        "cost_rate": rate,
                        "transaction_cost": exit_cost,
                    }
                )
                gross_cash += stock_value
                gross_shares = 0.0
                stock_value = 0.0
                portfolio_gross = gross_cash
            benchmark_exit = benchmark_shares * price_book.get(benchmark, instant)
            benchmark_exit_cost = benchmark_exit * rate
            benchmark_costs += benchmark_exit_cost
            transactions.append(
                {
                    "decision_year": int(requirement["decision_year"]),
                    "requirement_id": requirement_id,
                    "date": instant,
                    "stream": "benchmark",
                    "side": "exit_sell",
                    "actual_traded_notional": benchmark_exit,
                    "cost_rate": rate,
                    "transaction_cost": benchmark_exit_cost,
                }
            )
            benchmark_cash += benchmark_exit
            benchmark_shares = 0.0
            benchmark_value = benchmark_cash

        rows.append(
            {
                "performance_namespace": namespace,
                "decision_year": int(requirement["decision_year"]),
                "requirement_id": requirement_id,
                "stable_row_id": requirement["stable_row_id"],
                "ticker": requirement["ticker"],
                "active_symbol": current_symbol,
                "benchmark_symbol": benchmark,
                "date": instant,
                "portfolio_gross_value": portfolio_gross,
                "portfolio_net_value": max(portfolio_gross - portfolio_costs, 0.0),
                "benchmark_gross_value": benchmark_value,
                "benchmark_net_value": max(benchmark_value - benchmark_costs, 0.0),
                "portfolio_cumulative_cost": portfolio_costs,
                "benchmark_cumulative_cost": benchmark_costs,
                "portfolio_shares": gross_shares,
                "benchmark_shares": benchmark_shares,
                "cash_value": gross_cash,
                "event_applied": event_applied,
                "outcome_namespace": (
                    "observed" if event is None else event["outcome_namespace"]
                ),
            }
        )

    expected_months = len(
        price_book.month_ends(
            str(requirement["exchange_calendar"]), entry, exit_timestamp
        )
    )
    return pd.DataFrame(rows), transactions, [
        {
            "requirement_id": requirement_id,
            "required_session_count": len(schedule),
            "observed_stock_session_count": len(schedule),
            "observed_benchmark_session_count": len(schedule),
            "required_month_end_count": expected_months,
            "benchmark_gap_count": 0,
        }
    ]


def _build_vintage_nav(
    holding_ledger: pd.DataFrame,
    holdings: pd.DataFrame,
) -> pd.DataFrame:
    records: list[pd.DataFrame] = []
    for year, requirements in holdings.groupby("decision_year", sort=True):
        year = int(year)
        frame = holding_ledger[holding_ledger["decision_year"].eq(year)]
        date_sets = [
            tuple(group["date"].sort_values())
            for _, group in frame.groupby("requirement_id", sort=True)
        ]
        if len(set(date_sets)) != 1:
            raise EvidenceValidationError(
                f"vintage holdings do not share the exact V1 valuation clock: {year}"
            )
        values = (
            frame.groupby("date", as_index=False)[
                [
                    "portfolio_gross_value",
                    "portfolio_net_value",
                    "benchmark_gross_value",
                    "benchmark_net_value",
                ]
            ]
            .sum()
            .sort_values("date")
        )
        values["month"] = values["date"].dt.tz_localize(None).dt.to_period("M")
        monthly = values.groupby("month", as_index=False).tail(1).copy()
        aum = float(requirements["planned_vintage_aum_usd"].iloc[0])
        monthly["decision_year"] = year
        monthly["initial_capital"] = aum
        monthly["gross_nav"] = monthly["portfolio_gross_value"] / aum
        monthly["net_nav"] = monthly["portfolio_net_value"] / aum
        monthly["benchmark_gross_nav"] = monthly["benchmark_gross_value"] / aum
        monthly["benchmark_net_nav"] = monthly["benchmark_net_value"] / aum
        for basis in ("gross", "net"):
            for prefix in ("", "benchmark_"):
                nav_column = f"{prefix}{basis}_nav"
                return_column = f"{prefix}{basis}_return"
                interval_returns: list[float] = []
                previous = 1.0
                for current in monthly[nav_column].to_numpy(dtype=float):
                    if previous > 0:
                        interval_returns.append(float(current / previous - 1.0))
                    elif current == 0:
                        interval_returns.append(0.0)
                    else:
                        raise EvidenceValidationError(
                            "vintage NAV recovered from zero without external capital"
                        )
                    previous = current
                monthly[return_column] = interval_returns
        records.append(monthly)
    return pd.concat(records, ignore_index=True).sort_values(
        ["decision_year", "date"]
    ).reset_index(drop=True)


def _add_turnover_denominators(
    transactions: pd.DataFrame,
    vintage_nav: pd.DataFrame,
) -> pd.DataFrame:
    if transactions.empty:
        return transactions
    frame = transactions.copy().sort_values(
        ["decision_year", "stream", "date", "requirement_id"]
    )
    denominators: list[float] = []
    for row in frame.itertuples(index=False):
        vintage = vintage_nav[vintage_nav["decision_year"].eq(row.decision_year)]
        if row.side == "entry_buy":
            denominator = float(vintage["initial_capital"].iloc[0])
        else:
            column = (
                "portfolio_gross_value" if row.stream == "portfolio" else "benchmark_gross_value"
            )
            same_month = vintage[
                vintage["month"].eq(pd.Timestamp(row.date).tz_localize(None).to_period("M"))
            ]
            if same_month.empty:
                raise EvidenceValidationError("transaction turnover denominator is absent")
            denominator = float(same_month[column].iloc[-1])
        if denominator <= 0:
            denominator = np.nan
        denominators.append(denominator)
    frame["pre_cost_vintage_nav"] = denominators
    frame["turnover_contribution"] = (
        frame["actual_traded_notional"].abs() / frame["pre_cost_vintage_nav"]
    )
    return frame.reset_index(drop=True)


def _build_aggregate_nav(vintage_nav: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    running = {
        "gross_nav": 1.0,
        "net_nav": 1.0,
        "benchmark_gross_nav": 1.0,
        "benchmark_net_nav": 1.0,
    }
    last_values: dict[tuple[int, str], float] = {}
    first_month: dict[int, pd.Period] = {
        int(year): group["month"].min()
        for year, group in vintage_nav.groupby("decision_year")
    }
    last_month: dict[int, pd.Period] = {
        int(year): group["month"].max()
        for year, group in vintage_nav.groupby("decision_year")
    }
    for month, group in vintage_nav.groupby("month", sort=True):
        record: dict[str, Any] = {
            "month": month,
            "date": group["date"].max(),
            "active_vintage_count": int(len(group)),
            "external_contributions": float(
                group.loc[
                    group["decision_year"].map(first_month).eq(month), "initial_capital"
                ].sum()
            ),
            "external_withdrawals_gross": float(
                group.loc[
                    group["decision_year"].map(last_month).eq(month),
                    "portfolio_gross_value",
                ].sum()
            ),
            "external_withdrawals_net": float(
                group.loc[
                    group["decision_year"].map(last_month).eq(month),
                    "portfolio_net_value",
                ].sum()
            ),
        }
        for basis in ("gross", "net"):
            for prefix in ("", "benchmark_"):
                value_column = (
                    f"{prefix}{basis}_value"
                    if prefix
                    else f"portfolio_{basis}_value"
                )
                nav_column = f"{prefix}{basis}_nav"
                return_column = f"{prefix}{basis}_return"
                beginning_values = []
                ending_values = []
                for item in group.itertuples(index=False):
                    year = int(item.decision_year)
                    key = (year, nav_column)
                    beginning = last_values.get(key, float(item.initial_capital))
                    ending = float(getattr(item, value_column))
                    beginning_values.append(beginning)
                    ending_values.append(ending)
                    last_values[key] = ending
                denominator = float(sum(beginning_values))
                if denominator > 0:
                    monthly_return = float(sum(ending_values) / denominator - 1.0)
                elif all(value == 0 for value in ending_values):
                    monthly_return = 0.0
                else:
                    raise EvidenceValidationError(
                        "aggregate NAV has a zero denominator with positive ending capital"
                    )
                running[nav_column] *= 1.0 + monthly_return
                record[return_column] = monthly_return
                record[nav_column] = running[nav_column]
                record[f"active_{prefix}{basis}_capital"] = denominator
        rows.append(record)
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def calculate_performance_metrics(
    monthly: pd.DataFrame,
    *,
    return_column: str,
    nav_column: str,
    benchmark_return_column: str,
    benchmark_nav_column: str,
    risk_free_returns: np.ndarray,
    start_date: pd.Timestamp,
    transaction_ledger: pd.DataFrame | None = None,
    risk_free_namespace: str,
) -> dict[str, Any]:
    """Calculate frozen monthly metrics with explicit availability reasons."""
    required = {
        "date",
        return_column,
        nav_column,
        benchmark_return_column,
        benchmark_nav_column,
    }
    _require_columns(monthly, required, "metric monthly NAV")
    frame = monthly.sort_values("date").reset_index(drop=True)
    returns = pd.to_numeric(frame[return_column], errors="coerce").to_numpy(dtype=float)
    benchmark_returns = pd.to_numeric(
        frame[benchmark_return_column], errors="coerce"
    ).to_numpy(dtype=float)
    nav = pd.to_numeric(frame[nav_column], errors="coerce").to_numpy(dtype=float)
    benchmark_nav = pd.to_numeric(
        frame[benchmark_nav_column], errors="coerce"
    ).to_numpy(dtype=float)
    if (
        len(frame) == 0
        or not np.isfinite(returns).all()
        or not np.isfinite(benchmark_returns).all()
        or not np.isfinite(nav).all()
        or not np.isfinite(benchmark_nav).all()
        or (returns < -1).any()
        or (benchmark_returns < -1).any()
        or len(risk_free_returns) != len(frame)
        or not np.isfinite(risk_free_returns).all()
    ):
        raise EvidenceValidationError("metric inputs are incomplete or invalid")

    end_date = pd.Timestamp(frame["date"].iloc[-1])
    elapsed_days = (end_date - pd.Timestamp(start_date)).total_seconds() / 86_400
    ending_nav = float(nav[-1])
    cagr = (
        (ending_nav ** (365.2425 / elapsed_days) - 1.0)
        if elapsed_days > 0 and ending_nav > 0
        else (-1.0 if elapsed_days > 0 and ending_nav == 0 else np.nan)
    )
    path = np.concatenate(([1.0], nav))
    peaks = np.maximum.accumulate(path)
    drawdowns = path / np.where(peaks > 0, peaks, 1.0) - 1.0
    max_drawdown = float(drawdowns.min())
    drawdown_duration = current = 0
    for value in drawdowns:
        current = current + 1 if value < 0 else 0
        drawdown_duration = max(drawdown_duration, current)

    volatility = (
        float(np.std(returns, ddof=1) * np.sqrt(12))
        if len(returns) >= 2
        else np.nan
    )
    excess = returns - risk_free_returns
    excess_std = np.std(excess, ddof=1) if len(excess) >= 2 else np.nan
    sharpe = (
        float(np.mean(excess) / excess_std * np.sqrt(12))
        if len(excess) >= 2 and np.isfinite(excess_std) and excess_std > 0
        else np.nan
    )
    downside = np.minimum(excess, 0.0)
    downside_deviation = (
        np.sqrt(np.mean(downside**2)) * np.sqrt(12) if len(downside) else np.nan
    )
    sortino = (
        float(np.mean(excess) * 12 / downside_deviation)
        if (
            len(excess) >= 2
            and np.isfinite(downside_deviation)
            and downside_deviation > 0
        )
        else np.nan
    )
    calmar = (
        float(cagr / abs(max_drawdown))
        if max_drawdown < 0 and np.isfinite(cagr)
        else np.nan
    )

    benchmark_variance = (
        np.var(benchmark_returns, ddof=1) if len(benchmark_returns) >= 2 else np.nan
    )
    beta = (
        float(
            np.cov(returns, benchmark_returns, ddof=1)[0, 1]
            / benchmark_variance
        )
        if (
            len(returns) >= 2
            and np.isfinite(benchmark_variance)
            and benchmark_variance > 0
        )
        else np.nan
    )
    alpha = (
        float(
            (
                np.mean(excess)
                - beta * np.mean(benchmark_returns - risk_free_returns)
            )
            * 12
        )
        if np.isfinite(beta)
        else np.nan
    )
    active = returns - benchmark_returns
    active_std = np.std(active, ddof=1) if len(active) >= 2 else np.nan
    tracking_error = (
        float(active_std * np.sqrt(12))
        if len(active) >= 2 and np.isfinite(active_std)
        else np.nan
    )
    information_ratio = (
        float(np.mean(active) / active_std * np.sqrt(12))
        if len(active) >= 2 and np.isfinite(active_std) and active_std > 0
        else np.nan
    )
    hit_rate = float(np.mean(active > 0)) if len(active) else np.nan

    turnover = np.nan
    turnover_vintage_count = 0
    traded_notional = 0.0
    if transaction_ledger is not None and len(transaction_ledger):
        trades = transaction_ledger[transaction_ledger["stream"].eq("portfolio")]
        turnover_vintage_count = int(trades["decision_year"].nunique())
        traded_notional = float(trades["actual_traded_notional"].abs().sum())
        contributions = pd.to_numeric(
            trades["turnover_contribution"], errors="coerce"
        )
        if turnover_vintage_count > 0 and contributions.notna().all():
            turnover = float(contributions.sum() / turnover_vintage_count)

    availability = {
        "cagr": None if np.isfinite(cagr) else "nonpositive_or_zero_elapsed_denominator",
        "annualized_volatility": (
            None if np.isfinite(volatility) else "requires_at_least_two_returns"
        ),
        "sharpe_ratio": (
            None if np.isfinite(sharpe) else "insufficient_history_or_zero_excess_variance"
        ),
        "sortino_ratio": (
            None if np.isfinite(sortino) else "insufficient_history_or_zero_downside_deviation"
        ),
        "calmar_ratio": (
            None if np.isfinite(calmar) else "requires_nonzero_drawdown_and_available_cagr"
        ),
        "beta": None if np.isfinite(beta) else "requires_nonzero_benchmark_variance",
        "alpha": None if np.isfinite(alpha) else "requires_available_beta_and_risk_free_series",
        "tracking_error": (
            None if np.isfinite(tracking_error) else "requires_at_least_two_paired_returns"
        ),
        "information_ratio": (
            None if np.isfinite(information_ratio) else "requires_nonzero_tracking_error"
        ),
        "turnover": None if np.isfinite(turnover) else "requires_trade_denominators",
        "hit_rate": None if np.isfinite(hit_rate) else "requires_paired_returns",
    }
    return {
        "cagr": float(cagr) if np.isfinite(cagr) else np.nan,
        "annualized_volatility": volatility,
        "maximum_drawdown": max_drawdown,
        "maximum_drawdown_duration_months": int(drawdown_duration),
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "calmar_ratio": calmar,
        "beta": beta,
        "alpha": alpha,
        "tracking_error": tracking_error,
        "information_ratio": information_ratio,
        "turnover": turnover,
        "hit_rate": hit_rate,
        "risk_free_namespace": risk_free_namespace,
        "coverage_denominators": {
            "monthly_return_count": int(len(returns)),
            "paired_benchmark_return_count": int(len(active)),
            "risk_free_interval_count": int(len(risk_free_returns)),
            "turnover_vintage_count": turnover_vintage_count,
            "absolute_actual_traded_notional": traded_notional,
        },
        "availability": availability,
    }


def _coverage_summary(
    holdings: pd.DataFrame,
    open_holdings: pd.DataFrame,
    outcome_ledgers: Mapping[str, pd.DataFrame],
    session_coverage: list[dict[str, Any]],
    risk_free_namespace: str,
) -> dict[str, Any]:
    capital_total = float(holdings["planned_entry_notional_usd"].sum())
    capital_by_outcome: dict[str, float] = {}
    holding_by_outcome: dict[str, int] = {}
    notional = holdings.set_index("requirement_id")["planned_entry_notional_usd"]
    for namespace in OUTCOME_NAMESPACES:
        frame = outcome_ledgers[namespace]
        ids = set(frame["requirement_id"]) if len(frame) else set()
        holding_by_outcome[namespace] = len(ids)
        capital_by_outcome[namespace] = float(
            pd.to_numeric(notional.reindex(sorted(ids)), errors="coerce").fillna(0).sum()
        )
    required_sessions = sum(row["required_session_count"] for row in session_coverage)
    observed_stock = sum(
        row["observed_stock_session_count"] for row in session_coverage
    )
    observed_benchmark = sum(
        row["observed_benchmark_session_count"] for row in session_coverage
    )
    benchmark_gaps = sum(row["benchmark_gap_count"] for row in session_coverage)
    outcome_record_count = int(
        sum(len(outcome_ledgers[name]) for name in OUTCOME_NAMESPACES)
    )
    populated_outcome_frames = [
        outcome_ledgers[name]
        for name in OUTCOME_NAMESPACES
        if len(outcome_ledgers[name])
    ]
    terminal_records = (
        pd.concat(populated_outcome_frames, ignore_index=True)
        if populated_outcome_frames
        else pd.DataFrame(columns=_OUTCOME_LEDGER_COLUMNS)
    )
    terminal_records = terminal_records[
        ~terminal_records["event_type"].eq("complete_price_path")
    ]
    return {
        "holding_count_denominator": int(len(holdings)),
        "completed_holding_count": int(len(holdings)),
        "open_holding_count": int(len(open_holdings)),
        "capital_denominator": capital_total,
        "holding_count_by_outcome_namespace": holding_by_outcome,
        "capital_by_outcome_namespace": capital_by_outcome,
        "scenario_imputed_holding_count": holding_by_outcome["bounded_scenario"],
        "scenario_imputed_capital": capital_by_outcome["bounded_scenario"],
        "required_stock_session_count": int(required_sessions),
        "observed_stock_session_count": int(observed_stock),
        "required_benchmark_session_count": int(required_sessions),
        "observed_benchmark_session_count": int(observed_benchmark),
        "benchmark_gap_count": int(benchmark_gaps),
        "outcome_record_count": outcome_record_count,
        "terminal_event_record_count": int(len(terminal_records)),
        "terminal_event_holding_count": int(
            terminal_records["requirement_id"].nunique()
        ),
        "terminal_event_holding_denominator": int(len(holdings)),
        "risk_free_rate_namespace": risk_free_namespace,
        "risk_free_interval_policy": (
            "explicit_zero_diagnostic"
            if risk_free_namespace == ZERO_RATE_NAMESPACE
            else "exact_dgs1mo_only"
        ),
    }


def run_performance_engine(
    bundle: EvidenceBundle,
    *,
    performance_namespace: str,
    risk_free_namespace: str = ZERO_RATE_NAMESPACE,
) -> PerformanceResult:
    """Run one isolated namespace over completed synthetic V1 vintages."""
    validate_evidence_bundle(bundle)
    if performance_namespace not in PERFORMANCE_NAMESPACES:
        raise EvidenceValidationError(
            f"unknown performance namespace: {performance_namespace}"
        )
    holdings_all = bundle.requirements[
        bundle.requirements["instrument_role"].eq("holding")
    ].copy()
    holdings_all["entry_timestamp"] = pd.to_datetime(
        holdings_all["entry_timestamp"], utc=True
    )
    holdings_all["calendar_exit_timestamp"] = pd.to_datetime(
        holdings_all["calendar_exit_timestamp"], utc=True
    )
    holdings = holdings_all[holdings_all["calendar_exit_timestamp"].notna()].copy()
    open_holdings = holdings_all[holdings_all["calendar_exit_timestamp"].isna()].copy()
    if holdings.empty:
        return PerformanceResult(
            available=False,
            performance_namespace=performance_namespace,
            risk_free_namespace=risk_free_namespace,
            unavailable_reasons=[{"code": "no_completed_vintages"}],
            outcome_ledgers=_empty_outcome_ledgers(),
            coverage={
                "holding_count_denominator": 0,
                "open_holding_count": int(len(open_holdings)),
            },
        )

    # This check occurs before any NAV is built; unavailable DGS1MO cannot be
    # bypassed by the physically separate zero-rate diagnostic.
    _rate_returns(
        bundle,
        risk_free_namespace,
        pd.Series(pd.to_datetime([holdings["entry_timestamp"].min()], utc=True)),
    )
    resolved, outcome_ledgers, reasons = _resolve_outcomes(
        bundle, performance_namespace, holdings
    )
    if reasons:
        return PerformanceResult(
            available=False,
            performance_namespace=performance_namespace,
            risk_free_namespace=risk_free_namespace,
            unavailable_reasons=reasons,
            outcome_ledgers=outcome_ledgers,
            coverage=_coverage_summary(
                holdings, open_holdings, outcome_ledgers, [], risk_free_namespace
            ),
        )

    price_book = _PriceBook(bundle.prices)
    holding_frames: list[pd.DataFrame] = []
    transactions: list[dict[str, Any]] = []
    session_coverage: list[dict[str, Any]] = []
    coverage_index = bundle.coverage.set_index("requirement_id")
    for _, requirement in holdings.sort_values(
        ["decision_year", "requirement_id"]
    ).iterrows():
        path, trades, sessions = _build_holding_path(
            requirement,
            resolved[str(requirement["requirement_id"])],
            price_book,
            performance_namespace,
        )
        declared = coverage_index.loc[requirement["requirement_id"]]
        actual_months = sessions[0]["required_month_end_count"]
        if (
            int(declared["required_month_end_count"]) != actual_months
            or int(declared["observed_common_month_end_count"]) != actual_months
            or int(declared["missing_common_month_end_count"]) != 0
            or int(declared["benchmark_gap_count"]) != 0
            or not bool(declared["entry_observed_common"])
            or not bool(declared["exit_observed_common"])
        ):
            raise EvidenceValidationError(
                f"declared/actual session coverage mismatch: {requirement['requirement_id']}"
            )
        holding_frames.append(path)
        transactions.extend(trades)
        session_coverage.extend(sessions)
    holding_ledger = pd.concat(holding_frames, ignore_index=True).sort_values(
        ["decision_year", "requirement_id", "date"]
    ).reset_index(drop=True)
    transaction_ledger = pd.DataFrame(transactions)
    vintage_nav = _build_vintage_nav(holding_ledger, holdings)
    transaction_ledger = _add_turnover_denominators(
        transaction_ledger, vintage_nav
    )
    aggregate_nav = _build_aggregate_nav(vintage_nav)

    capital_records = []
    for year, group in vintage_nav.groupby("decision_year", sort=True):
        capital_records.append(
            {
                "decision_year": int(year),
                "entry_date": group["date"].min(),
                "exit_date": group["date"].max(),
                "external_contribution": float(group["initial_capital"].iloc[0]),
                "gross_withdrawal": float(group["portfolio_gross_value"].iloc[-1]),
                "net_withdrawal": float(group["portfolio_net_value"].iloc[-1]),
                "benchmark_gross_withdrawal": float(
                    group["benchmark_gross_value"].iloc[-1]
                ),
                "benchmark_net_withdrawal": float(
                    group["benchmark_net_value"].iloc[-1]
                ),
            }
        )
    capital_ledger = pd.DataFrame(capital_records)

    vintage_metrics: dict[int, dict[str, dict[str, Any]]] = {}
    for year, frame in vintage_nav.groupby("decision_year", sort=True):
        rates = _rate_returns(bundle, risk_free_namespace, frame["date"])
        start_date = holdings.loc[
            holdings["decision_year"].eq(year), "entry_timestamp"
        ].min()
        vintage_metrics[int(year)] = {}
        for basis in ("gross", "net"):
            vintage_metrics[int(year)][basis] = calculate_performance_metrics(
                frame,
                return_column=f"{basis}_return",
                nav_column=f"{basis}_nav",
                benchmark_return_column=f"benchmark_{basis}_return",
                benchmark_nav_column=f"benchmark_{basis}_nav",
                risk_free_returns=rates,
                start_date=start_date,
                transaction_ledger=transaction_ledger[
                    transaction_ledger["decision_year"].eq(year)
                ],
                risk_free_namespace=risk_free_namespace,
            )

    aggregate_rates = _rate_returns(
        bundle, risk_free_namespace, aggregate_nav["date"]
    )
    aggregate_metrics: dict[str, dict[str, Any]] = {}
    for basis in ("gross", "net"):
        aggregate_metrics[basis] = calculate_performance_metrics(
            aggregate_nav,
            return_column=f"{basis}_return",
            nav_column=f"{basis}_nav",
            benchmark_return_column=f"benchmark_{basis}_return",
            benchmark_nav_column=f"benchmark_{basis}_nav",
            risk_free_returns=aggregate_rates,
            start_date=holdings["entry_timestamp"].min(),
            transaction_ledger=transaction_ledger,
            risk_free_namespace=risk_free_namespace,
        )

    return PerformanceResult(
        available=True,
        performance_namespace=performance_namespace,
        risk_free_namespace=risk_free_namespace,
        vintage_nav=vintage_nav,
        aggregate_nav=aggregate_nav,
        holding_ledger=holding_ledger,
        transaction_ledger=transaction_ledger,
        capital_ledger=capital_ledger,
        outcome_ledgers=outcome_ledgers,
        coverage=_coverage_summary(
            holdings,
            open_holdings,
            outcome_ledgers,
            session_coverage,
            risk_free_namespace,
        ),
        vintage_metrics=vintage_metrics,
        aggregate_metrics=aggregate_metrics,
    )
