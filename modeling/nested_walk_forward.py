"""Frozen M1 nested temporal tuning interfaces.

The module implements the Session M1A contract but has no command-line entry
point and performs no work at import time.  Historical fitting is reserved for
M1C.  M1B exercises these interfaces only with synthetic populations.
"""
from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatchcase
from functools import cmp_to_key
import hashlib
from itertools import product
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
import warnings

import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import ConstantInputWarning
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.tree import DecisionTreeClassifier

from modeling.fold_lineage import (
    SelectorConfig,
    dataframe_fingerprint,
    select_fold_features,
)


ROOT = Path(__file__).resolve().parents[1]
M1A_VERSION = "20260801T000000Z-m1a"
M1A_MANIFEST_SHA256 = (
    "a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc"
)
DEFAULT_M1A_ROOT = (
    ROOT / "artifacts/modeling/nested_walk_forward" / M1A_VERSION
)
DEFAULT_P3_CONTRACT = (
    ROOT
    / "artifacts/canonical/corrected_us_annual_3y_research_model"
    / "configuration/research_model_contract.json"
)

MODEL_ROLES = ("decision_tree", "lightgbm_regression")
SELECTOR_METHODS = (
    "p3_fold_local_ic_selector",
    "deterministic_stability_selection_with_redundancy_pruning",
)
TRAINING_REGIMES = (
    "broad_downstream_gates",
    "gate_eligible_training",
    "broad_gate_features",
)


class NestedWalkForwardContractError(RuntimeError):
    """A leakage, lineage, or frozen-contract violation."""


class CandidateUnavailable(RuntimeError):
    """A valid candidate cannot be fit on enough inner folds."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_json(payload: Any) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )


def _payload_sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json(payload).encode()).hexdigest()


def _candidate_order_sha256(columns: Sequence[str]) -> str:
    # M1A froze the P3 order with compact JSON but without sorted keys.
    return hashlib.sha256(
        json.dumps(list(columns), separators=(",", ":")).encode()
    ).hexdigest()


def _timestamp(value: Any, field: str) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if result.tzinfo is None:
        result = result.tz_localize("UTC")
    else:
        result = result.tz_convert("UTC")
    if pd.isna(result):
        raise NestedWalkForwardContractError(f"{field} is missing")
    return result


def _iso(value: Any) -> str:
    return _timestamp(value, "timestamp").isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class FrozenM1AContract:
    """Hash-verified M1A configuration and fold tables."""

    root: Path
    manifest: dict[str, Any]
    contract: dict[str, Any]
    candidate_columns: tuple[str, ...]
    outer_folds: tuple[dict[str, Any], ...]
    inner_folds: tuple[dict[str, Any], ...]
    label_maturity_ledger: tuple[dict[str, Any], ...]


def _verify_generated_records(
    root: Path, manifest: Mapping[str, Any]
) -> None:
    records = manifest.get("records")
    if not isinstance(records, list) or not records:
        raise NestedWalkForwardContractError(
            "M1A manifest has no generated-record inventory"
        )
    for record in records:
        path = root / str(record.get("path", ""))
        if not path.is_file():
            raise NestedWalkForwardContractError(
                f"M1A generated record is missing: {path}"
            )
        if (
            path.stat().st_size != int(record.get("size_bytes", -1))
            or _sha256_file(path) != record.get("sha256")
        ):
            raise NestedWalkForwardContractError(
                f"M1A generated record hash/size mismatch: {path}"
            )


def _expand_grid(grid: Mapping[str, Sequence[Any]]) -> list[dict[str, Any]]:
    keys = sorted(grid)
    values: list[Sequence[Any]] = []
    for key in keys:
        options = grid[key]
        if not isinstance(options, list) or not options:
            raise NestedWalkForwardContractError(
                f"frozen grid entry {key} is not a non-empty list"
            )
        values.append(options)
    return [dict(zip(keys, point)) for point in product(*values)]


def _validate_contract_tables(
    manifest: Mapping[str, Any],
    contract: Mapping[str, Any],
    outer: Sequence[Mapping[str, Any]],
    inner: Sequence[Mapping[str, Any]],
    maturity: Sequence[Mapping[str, Any]],
) -> None:
    if (
        len(outer) != int(manifest.get("outer_fold_records", -1))
        or len(outer) != 34
    ):
        raise NestedWalkForwardContractError("M1A outer-fold count drifted")
    if (
        len(inner) != int(manifest.get("inner_fold_records", -1))
        or len(inner) != 306
    ):
        raise NestedWalkForwardContractError("M1A inner-fold count drifted")
    if (
        len(maturity) != int(manifest.get("label_maturity_records", -1))
        or len(maturity) != 102
    ):
        raise NestedWalkForwardContractError("M1A maturity-ledger count drifted")

    outer_keys: set[tuple[str, str]] = set()
    outer_decisions: set[str] = set()
    for row in outer:
        key = (str(row["outer_fold"]), str(row["target_role"]))
        if key in outer_keys or key[1] not in MODEL_ROLES:
            raise NestedWalkForwardContractError(
                "M1A outer folds contain duplicate or unsupported role lineage"
            )
        outer_keys.add(key)
        decision = _timestamp(row["decision_timestamp"], "outer decision")
        expected_fold = decision.strftime("decision_%Y%m%dT%H%M%SZ")
        if key[0] != expected_fold:
            raise NestedWalkForwardContractError(
                "M1A outer fold identifier/decision mismatch"
            )
        outer_decisions.add(_iso(decision))
        max_label = row.get("p3_max_fitted_label_end")
        if max_label is not None and _timestamp(max_label, "outer label end") >= decision:
            raise NestedWalkForwardContractError(
                "M1A outer training label is not strictly mature"
            )
    expected_decisions = set(
        contract.get("evidence_summary", {}).get(
            "p3_outer_decision_cohorts", []
        )
    )
    expected_outer_keys = {
        (
            _timestamp(decision, "frozen outer decision").strftime(
                "decision_%Y%m%dT%H%M%SZ"
            ),
            role,
        )
        for decision in expected_decisions
        for role in MODEL_ROLES
    }
    if outer_decisions != expected_decisions or outer_keys != expected_outer_keys:
        raise NestedWalkForwardContractError(
            "M1A exact P3 outer decision/role boundary drifted"
        )

    inner_keys: set[tuple[str, str, str, str]] = set()
    for row in inner:
        key = (
            str(row["outer_fold"]),
            str(row["target_role"]),
            str(row["regime"]),
            str(row["inner_fold"]),
        )
        if key in inner_keys:
            raise NestedWalkForwardContractError(
                "M1A inner folds contain duplicate fold lineage"
            )
        inner_keys.add(key)
        if key[1] not in MODEL_ROLES or key[2] not in TRAINING_REGIMES:
            raise NestedWalkForwardContractError(
                "M1A inner fold has unsupported role or regime"
            )
        outer_decision = _timestamp(
            row["outer_decision_timestamp"], "outer decision"
        )
        validation_value = row.get("validation_decision_start")
        if validation_value is None:
            if row.get("availability_status") != "unavailable" or not row.get(
                "failure_reason"
            ):
                raise NestedWalkForwardContractError(
                    "missing M1A validation cohort did not fail closed"
                )
            continue
        validation_decision = _timestamp(
            validation_value, "inner validation decision"
        )
        if validation_decision >= outer_decision:
            raise NestedWalkForwardContractError(
                "M1A inner validation is outside outer training"
            )
        if row.get("availability_status") == "available":
            max_label = _timestamp(
                row["max_fitted_label_end"], "inner training label end"
            )
            validation_label = _timestamp(
                row["validation_label_end_max"], "inner validation label end"
            )
            if not max_label < validation_decision < outer_decision:
                raise NestedWalkForwardContractError(
                    "M1A inner temporal ordering drifted"
                )
            if validation_label >= outer_decision:
                raise NestedWalkForwardContractError(
                    "M1A validation label is not mature before outer decision"
                )
        elif not row.get("failure_reason"):
            raise NestedWalkForwardContractError(
                "unavailable M1A inner fold has no failure reason"
            )
    expected_inner_keys = {
        (
            _timestamp(decision, "frozen outer decision").strftime(
                "decision_%Y%m%dT%H%M%SZ"
            ),
            role,
            regime,
            f"inner_{slot}",
        )
        for decision in expected_decisions
        for role in MODEL_ROLES
        for regime in TRAINING_REGIMES
        for slot in range(1, 4)
    }
    if inner_keys != expected_inner_keys:
        raise NestedWalkForwardContractError(
            "M1A exact inner fold/role/regime boundary drifted"
        )

    maturity_keys: set[tuple[str, str, str]] = set()
    for row in maturity:
        key = (
            str(row["outer_fold"]),
            str(row["target_role"]),
            str(row["regime"]),
        )
        if key in maturity_keys:
            raise NestedWalkForwardContractError(
                "M1A maturity ledger contains duplicate lineage"
            )
        maturity_keys.add(key)
        if not bool(row.get("strict_label_end_before_outer_decision")):
            raise NestedWalkForwardContractError(
                "M1A maturity ledger contains overlapping outer label"
            )
        available = row.get("availability_status") == "available_for_tuning"
        enough = int(row.get("valid_inner_fold_count", -1)) >= int(
            row.get("minimum_valid_inner_folds", -1)
        )
        if available != enough:
            raise NestedWalkForwardContractError(
                "M1A maturity availability does not match valid-fold count"
            )
    expected_maturity_keys = {
        (
            _timestamp(decision, "frozen outer decision").strftime(
                "decision_%Y%m%dT%H%M%SZ"
            ),
            role,
            regime,
        )
        for decision in expected_decisions
        for role in MODEL_ROLES
        for regime in TRAINING_REGIMES
    }
    if maturity_keys != expected_maturity_keys:
        raise NestedWalkForwardContractError(
            "M1A exact maturity role/regime boundary drifted"
        )

    roles = contract.get("model_roles", {})
    for role, expected_size in (
        ("lightgbm_regression", 8),
        ("decision_tree", 4),
    ):
        role_contract = roles.get(role, {})
        points = _expand_grid(role_contract.get("grid", {}))
        if len(points) != expected_size or len(points) != int(
            role_contract.get("grid_size", -1)
        ):
            raise NestedWalkForwardContractError(
                f"{role} frozen grid size drifted"
            )
        if any(point.get("random_state") != 42 for point in points):
            raise NestedWalkForwardContractError(
                f"{role} grid lost deterministic seed 42"
            )
    for point in _expand_grid(roles["lightgbm_regression"]["grid"]):
        if (
            point.get("n_jobs") != 1
            or point.get("deterministic") is not True
            or point.get("force_col_wise") is not True
        ):
            raise NestedWalkForwardContractError(
                "LightGBM deterministic execution contract drifted"
            )


def load_frozen_m1a_contract(
    root: Path = DEFAULT_M1A_ROOT,
    p3_contract_path: Path = DEFAULT_P3_CONTRACT,
    expected_manifest_sha256: str = M1A_MANIFEST_SHA256,
) -> FrozenM1AContract:
    """Load M1A after independently checking every generated byte."""
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise NestedWalkForwardContractError("frozen M1A manifest is missing")
    actual_manifest_sha = _sha256_file(manifest_path)
    if actual_manifest_sha != expected_manifest_sha256:
        raise NestedWalkForwardContractError(
            f"frozen M1A manifest hash mismatch: {actual_manifest_sha}"
        )
    manifest = json.loads(manifest_path.read_text())
    if (
        manifest.get("artifact_class")
        != "M1A_NESTED_WALK_FORWARD_EXPERIMENT_CONTRACT"
        or manifest.get("version") != M1A_VERSION
        or manifest.get("claim", {}).get("contract_only") is not True
        or manifest.get("claim", {}).get("model_execution") is not False
        or manifest.get("claim", {}).get("performance_calculated") is not False
    ):
        raise NestedWalkForwardContractError("M1A manifest identity/claim drifted")
    _verify_generated_records(root, manifest)

    contract_path = root / "experiment_contract.json"
    contract = json.loads(contract_path.read_text())
    configured = manifest.get("configuration", {})
    if (
        configured.get("path") != "experiment_contract.json"
        or configured.get("sha256") != _sha256_file(contract_path)
        or contract.get("claim") != manifest.get("claim")
    ):
        raise NestedWalkForwardContractError(
            "M1A experiment configuration lineage mismatch"
        )
    outer = json.loads((root / "outer_folds.json").read_text())
    inner = json.loads((root / "inner_folds.json").read_text())
    maturity = json.loads((root / "label_maturity_ledger.json").read_text())
    _validate_contract_tables(manifest, contract, outer, inner, maturity)

    p3_contract = json.loads(p3_contract_path.read_text())
    candidates = tuple(p3_contract["feature_contract"]["candidate_columns"])
    feature_contract = contract["feature_contract"]
    if (
        len(candidates) != int(feature_contract["candidate_count"])
        or _candidate_order_sha256(candidates)
        != feature_contract["candidate_order_sha256"]
    ):
        raise NestedWalkForwardContractError(
            "P3 candidate order does not match frozen M1A"
        )
    frozen = FrozenM1AContract(
        root=root,
        manifest=manifest,
        contract=contract,
        candidate_columns=candidates,
        outer_folds=tuple(outer),
        inner_folds=tuple(inner),
        label_maturity_ledger=tuple(maturity),
    )
    validate_candidate_columns(candidates, frozen, require_exact=True)
    return frozen


def _exclusion_patterns(frozen: FrozenM1AContract) -> tuple[str, ...]:
    exclusions = frozen.contract["feature_contract"]["exclusions"]
    return tuple(
        str(pattern)
        for category in exclusions.values()
        for pattern in category
    )


def validate_candidate_columns(
    columns: Sequence[str],
    frozen: FrozenM1AContract,
    *,
    require_exact: bool,
) -> tuple[str, ...]:
    """Reject target/support/future/model/policy/macro/gate-output inputs."""
    values = tuple(map(str, columns))
    if len(values) != len(set(values)):
        raise NestedWalkForwardContractError(
            "candidate feature list contains duplicates"
        )
    patterns = _exclusion_patterns(frozen)
    prohibited = sorted(
        column
        for column in values
        if any(fnmatchcase(column, pattern) for pattern in patterns)
    )
    if prohibited:
        raise NestedWalkForwardContractError(
            f"prohibited feature inputs: {prohibited}"
        )
    if require_exact and (
        len(values) != frozen.contract["feature_contract"]["candidate_count"]
        or _candidate_order_sha256(values)
        != frozen.contract["feature_contract"]["candidate_order_sha256"]
    ):
        raise NestedWalkForwardContractError(
            "feature selection did not start from the exact frozen P3 candidates"
        )
    if not set(values).issubset(frozen.candidate_columns):
        raise NestedWalkForwardContractError(
            "feature candidate escaped the frozen P3 universe"
        )
    return values


def candidate_columns_for_regime(
    frozen: FrozenM1AContract, regime: str
) -> tuple[str, ...]:
    if regime not in TRAINING_REGIMES:
        raise NestedWalkForwardContractError(
            f"unsupported frozen training regime: {regime}"
        )
    validate_candidate_columns(frozen.candidate_columns, frozen, require_exact=True)
    gate_features = set(
        frozen.contract["feature_contract"][
            "gate_feature_regime_allowed_raw_inputs"
        ]
    )
    if regime == "broad_gate_features":
        result = frozen.candidate_columns
    else:
        result = tuple(
            feature
            for feature in frozen.candidate_columns
            if feature not in gate_features
        )
    return validate_candidate_columns(result, frozen, require_exact=False)


@dataclass(frozen=True)
class InnerFoldSpec:
    outer_fold: str
    inner_fold: str
    target_role: str
    regime: str
    outer_decision_timestamp: pd.Timestamp
    validation_decision_timestamp: pd.Timestamp
    availability_status: str = "available"
    failure_reason: str | None = None
    expected_train_rows: int | None = None
    expected_validation_rows: int | None = None
    expected_purge_count: int | None = None
    contract_record_sha256: str | None = None

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> "InnerFoldSpec":
        validation = record.get("validation_decision_start")
        if validation is None:
            raise CandidateUnavailable(
                str(record.get("failure_reason") or "inner fold unavailable")
            )
        return cls(
            outer_fold=str(record["outer_fold"]),
            inner_fold=str(record["inner_fold"]),
            target_role=str(record["target_role"]),
            regime=str(record["regime"]),
            outer_decision_timestamp=_timestamp(
                record["outer_decision_timestamp"], "outer decision"
            ),
            validation_decision_timestamp=_timestamp(
                validation, "inner validation decision"
            ),
            availability_status=str(record["availability_status"]),
            failure_reason=record.get("failure_reason"),
            expected_train_rows=int(record["train_row_count"]),
            expected_validation_rows=int(record["validation_row_count"]),
            expected_purge_count=int(record["purge_count"]),
            contract_record_sha256=_payload_sha256(record),
        )


def frozen_inner_fold_specs(
    frozen: FrozenM1AContract,
    outer_fold: str,
    role: str,
    regime: str,
) -> tuple[InnerFoldSpec, ...]:
    """Return only available exact M1A records for one tuning unit."""
    if role not in MODEL_ROLES or regime not in TRAINING_REGIMES:
        raise NestedWalkForwardContractError(
            "unsupported role/regime for frozen inner folds"
        )
    records = [
        record
        for record in frozen.inner_folds
        if record["outer_fold"] == outer_fold
        and record["target_role"] == role
        and record["regime"] == regime
    ]
    if len(records) != 3:
        raise NestedWalkForwardContractError(
            "frozen M1A inner fold records are incomplete"
        )
    available = [
        InnerFoldSpec.from_record(record)
        for record in records
        if record["availability_status"] == "available"
    ]
    return tuple(
        sorted(available, key=lambda spec: spec.validation_decision_timestamp)
    )


def _verify_spec_record(spec: InnerFoldSpec, frozen: FrozenM1AContract) -> None:
    matches = [
        record
        for record in frozen.inner_folds
        if record["outer_fold"] == spec.outer_fold
        and record["inner_fold"] == spec.inner_fold
        and record["target_role"] == spec.target_role
        and record["regime"] == spec.regime
    ]
    if len(matches) != 1 or spec.contract_record_sha256 != _payload_sha256(matches[0]):
        raise NestedWalkForwardContractError(
            "inner fold spec does not match one exact frozen M1A record"
        )


@dataclass(frozen=True)
class FoldPopulation:
    spec: InnerFoldSpec
    target_column: str
    train: pd.DataFrame
    validation: pd.DataFrame
    purged_stable_row_ids: tuple[str, ...]
    training_population_fingerprint: str
    validation_population_fingerprint: str
    population_lineage: dict[str, Any]


def _required_columns(role: str, regime: str, target: str) -> set[str]:
    required = {
        "stable_row_id",
        "fiscal_year",
        "decision_timestamp",
        "label_end_date",
        "source_feature_available_at_decision",
        target,
    }
    if regime == "gate_eligible_training":
        required.update({"piotroski_roa_pos", "beneish_m_score"})
    return required


def _role_target(frozen: FrozenM1AContract, role: str) -> str:
    if role not in MODEL_ROLES:
        raise NestedWalkForwardContractError(f"unsupported model role: {role}")
    return str(frozen.contract["model_roles"][role]["target"])


def _training_mask(
    table: pd.DataFrame, target: str, regime: str
) -> pd.Series:
    mask = (
        pd.to_numeric(table[target], errors="coerce").notna()
        & pd.to_datetime(table["label_end_date"], utc=True, errors="coerce").notna()
        & table["source_feature_available_at_decision"].eq(True)
    )
    if regime == "gate_eligible_training":
        mask &= (
            pd.to_numeric(table["piotroski_roa_pos"], errors="coerce").eq(1)
            & pd.to_numeric(table["beneish_m_score"], errors="coerce").lt(-1.78)
        )
    return mask


def _population_fingerprint(frame: pd.DataFrame) -> str:
    return dataframe_fingerprint(frame.reset_index(drop=True))


def materialize_inner_fold(
    table: pd.DataFrame,
    spec: InnerFoldSpec,
    frozen: FrozenM1AContract,
    *,
    verify_frozen_counts: bool = False,
) -> FoldPopulation:
    """Materialize one expanding inner split with strict three-year purging."""
    if spec.target_role not in MODEL_ROLES or spec.regime not in TRAINING_REGIMES:
        raise NestedWalkForwardContractError(
            "inner fold has unsupported role or regime"
        )
    if spec.availability_status != "available":
        raise CandidateUnavailable(spec.failure_reason or "inner fold unavailable")
    if verify_frozen_counts:
        _verify_spec_record(spec, frozen)
    if spec.validation_decision_timestamp >= spec.outer_decision_timestamp:
        raise NestedWalkForwardContractError(
            "inner validation cohort is not wholly inside outer training"
        )
    target = _role_target(frozen, spec.target_role)
    missing = sorted(_required_columns(spec.target_role, spec.regime, target) - set(table))
    if missing:
        raise NestedWalkForwardContractError(
            f"fold population is missing required columns: {missing}"
        )
    work = table.copy()
    if work["stable_row_id"].isna().any() or work["stable_row_id"].duplicated().any():
        raise NestedWalkForwardContractError(
            "fold source population has missing or duplicate stable row identity"
        )
    work["decision_timestamp"] = pd.to_datetime(
        work["decision_timestamp"], utc=True, errors="coerce"
    )
    work["label_end_date"] = pd.to_datetime(
        work["label_end_date"], utc=True, errors="coerce"
    )
    if work["decision_timestamp"].isna().any():
        raise NestedWalkForwardContractError(
            "fold source population has invalid decision timestamp"
        )
    eligible = _training_mask(work, target, spec.regime)
    before_validation = work["decision_timestamp"].lt(
        spec.validation_decision_timestamp
    )
    pre_purge = eligible & before_validation
    mature_for_validation = work["label_end_date"].lt(
        spec.validation_decision_timestamp
    )
    train = work.loc[pre_purge & mature_for_validation].copy()
    purged = work.loc[pre_purge & ~mature_for_validation].copy()
    validation = work.loc[
        eligible
        & work["decision_timestamp"].eq(spec.validation_decision_timestamp)
        & work["label_end_date"].lt(spec.outer_decision_timestamp)
    ].copy()

    if verify_frozen_counts:
        expected = (
            spec.expected_train_rows,
            spec.expected_validation_rows,
            spec.expected_purge_count,
        )
        actual = (len(train), len(validation), len(purged))
        if expected != actual:
            raise NestedWalkForwardContractError(
                f"frozen inner-fold population drifted: expected={expected}, actual={actual}"
            )

    lineage = {
        "outer_fold": spec.outer_fold,
        "inner_fold": spec.inner_fold,
        "target_role": spec.target_role,
        "training_regime": spec.regime,
        "target_column": target,
        "outer_decision_timestamp": _iso(spec.outer_decision_timestamp),
        "validation_decision_timestamp": _iso(
            spec.validation_decision_timestamp
        ),
        "training_rows": len(train),
        "validation_rows": len(validation),
        "purged_rows": len(purged),
        "training_population_fingerprint": _population_fingerprint(train),
        "validation_population_fingerprint": _population_fingerprint(validation),
        "purged_identity_sha256": _payload_sha256(
            sorted(map(str, purged["stable_row_id"]))
        ),
        "training_filter": frozen.contract["training_regimes"][spec.regime][
            "training_filter"
        ],
        "label_maturity_rule": frozen.contract["inner_protocol"][
            "inner_training_rule"
        ],
        "horizon_purge_rule": frozen.contract["outer_protocol"][
            "horizon_purge_rule"
        ],
    }
    population = FoldPopulation(
        spec=spec,
        target_column=target,
        train=train,
        validation=validation,
        purged_stable_row_ids=tuple(sorted(map(str, purged["stable_row_id"]))),
        training_population_fingerprint=lineage[
            "training_population_fingerprint"
        ],
        validation_population_fingerprint=lineage[
            "validation_population_fingerprint"
        ],
        population_lineage=lineage,
    )
    validate_fold_population(population, frozen)
    return population


def validate_fold_population(
    population: FoldPopulation, frozen: FrozenM1AContract
) -> None:
    """Fail closed if a caller bypassed the temporal population builder."""
    spec = population.spec
    train = population.train
    validation = population.validation
    target = _role_target(frozen, spec.target_role)
    required = _required_columns(spec.target_role, spec.regime, target)
    if required - set(train) or required - set(validation):
        raise NestedWalkForwardContractError(
            "inner train/validation population schema is incomplete"
        )
    train_decision = pd.to_datetime(train["decision_timestamp"], utc=True)
    validation_decision = pd.to_datetime(
        validation["decision_timestamp"], utc=True
    )
    train_label_end = pd.to_datetime(train["label_end_date"], utc=True)
    validation_label_end = pd.to_datetime(
        validation["label_end_date"], utc=True
    )
    if (
        train_decision.eq(spec.outer_decision_timestamp).any()
        or validation_decision.eq(spec.outer_decision_timestamp).any()
    ):
        raise NestedWalkForwardContractError(
            "outer-OOS cohort was reused inside inner tuning"
        )
    if (
        not train_decision.lt(spec.validation_decision_timestamp).all()
        or not train_label_end.lt(spec.validation_decision_timestamp).all()
    ):
        raise NestedWalkForwardContractError(
            "inner-training label/decision overlap reached validation"
        )
    if (
        not validation_decision.eq(spec.validation_decision_timestamp).all()
        or not validation_label_end.lt(spec.outer_decision_timestamp).all()
        or spec.validation_decision_timestamp >= spec.outer_decision_timestamp
    ):
        raise NestedWalkForwardContractError(
            "inner validation is outside the mature outer-training population"
        )
    train_ids = set(map(str, train["stable_row_id"]))
    validation_ids = set(map(str, validation["stable_row_id"]))
    overlap = sorted(train_ids & validation_ids)
    if overlap:
        raise NestedWalkForwardContractError(
            f"validation rows were reused for fitting: {overlap[:5]}"
        )
    if (
        not train["source_feature_available_at_decision"].eq(True).all()
        or not validation["source_feature_available_at_decision"].eq(True).all()
        or pd.to_numeric(train[target], errors="coerce").isna().any()
        or pd.to_numeric(validation[target], errors="coerce").isna().any()
    ):
        raise NestedWalkForwardContractError(
            "unavailable source feature or target entered inner fold"
        )
    minimum_train = int(
        frozen.contract["inner_protocol"]["minimum_training_rows"]
    )
    minimum_validation = int(
        frozen.contract["inner_protocol"]["minimum_validation_rows"]
    )
    if len(train) < minimum_train or len(validation) < minimum_validation:
        raise NestedWalkForwardContractError(
            "available inner fold is below frozen row-count minimum"
        )
    if _population_fingerprint(train) != population.training_population_fingerprint:
        raise NestedWalkForwardContractError(
            "inner-training population lineage fingerprint mismatch"
        )
    if (
        _population_fingerprint(validation)
        != population.validation_population_fingerprint
    ):
        raise NestedWalkForwardContractError(
            "inner-validation population lineage fingerprint mismatch"
        )


def validate_fold_collection(
    populations: Sequence[FoldPopulation], frozen: FrozenM1AContract
) -> None:
    """Validate expanding order and prevent validation/outer-row reuse."""
    if not populations:
        raise NestedWalkForwardContractError("no inner fold populations supplied")
    ordered = sorted(
        populations, key=lambda item: item.spec.validation_decision_timestamp
    )
    identity = {
        (
            item.spec.outer_fold,
            item.spec.target_role,
            item.spec.regime,
            item.spec.inner_fold,
        )
        for item in ordered
    }
    if len(identity) != len(ordered):
        raise NestedWalkForwardContractError(
            "inner fold population was reused in tuning"
        )
    first = ordered[0].spec
    validation_seen: set[str] = set()
    previous_train: set[str] = set()
    previous_validation_decision: pd.Timestamp | None = None
    for population in ordered:
        validate_fold_population(population, frozen)
        spec = population.spec
        if (
            spec.outer_fold != first.outer_fold
            or spec.target_role != first.target_role
            or spec.regime != first.regime
            or spec.outer_decision_timestamp != first.outer_decision_timestamp
        ):
            raise NestedWalkForwardContractError(
                "mixed outer fold, role, or regime in one tuning collection"
            )
        if (
            previous_validation_decision is not None
            and spec.validation_decision_timestamp <= previous_validation_decision
        ):
            raise NestedWalkForwardContractError(
                "inner validation cohorts are not strictly increasing"
            )
        train_ids = set(map(str, population.train["stable_row_id"]))
        if previous_train and not previous_train.issubset(train_ids):
            raise NestedWalkForwardContractError(
                "inner-training populations are not expanding"
            )
        validation_ids = set(map(str, population.validation["stable_row_id"]))
        duplicate_validation = sorted(validation_seen & validation_ids)
        if duplicate_validation:
            raise NestedWalkForwardContractError(
                f"validation row reused across inner folds: {duplicate_validation[:5]}"
            )
        validation_seen.update(validation_ids)
        previous_train = train_ids
        previous_validation_decision = spec.validation_decision_timestamp


@dataclass(frozen=True)
class FeatureSelectionResult:
    method: str
    selected_features: tuple[str, ...]
    candidate_columns: tuple[str, ...]
    diagnostics: dict[str, Any]
    lineage: dict[str, Any]


def _selector_config(frozen: FrozenM1AContract) -> SelectorConfig:
    payload = frozen.contract["feature_contract"]["baseline_selector"]
    return SelectorConfig(
        top_n=int(payload["top_n"]),
        min_abs_ic=float(payload["min_abs_ic"]),
        min_ic_years=int(payload["min_ic_years"]),
        min_group_size=int(payload["min_group_size"]),
        corr_threshold=float(payload["corr_threshold"]),
    )


def _yearly_feature_ic(
    frame: pd.DataFrame,
    feature: str,
    target: str,
    min_group_size: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for year, group in frame[["fiscal_year", feature, target]].groupby(
        "fiscal_year", sort=True
    ):
        pair = group[[feature, target]].apply(
            pd.to_numeric, errors="coerce"
        ).dropna()
        if len(pair) < min_group_size:
            continue
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ConstantInputWarning)
            correlation, _ = stats.spearmanr(pair[feature], pair[target])
        if np.isfinite(correlation):
            rows.append(
                {
                    "fiscal_year": int(year),
                    "rows": len(pair),
                    "spearman_ic": float(correlation),
                }
            )
    return rows


def _baseline_selection(
    train: pd.DataFrame,
    target: str,
    candidates: Sequence[str],
    config: SelectorConfig,
) -> tuple[str, ...]:
    selection = train[["fiscal_year", *candidates]].copy()
    # The shared P3 selector discovers numeric candidates from this temporary
    # frame, so retain a canonical forbidden-label token in the target alias.
    alias = "_m1_forward_return_target"
    selection[alias] = pd.to_numeric(train[target], errors="coerce").to_numpy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConstantInputWarning)
        selected = select_fold_features(selection, alias, config)
    if not set(selected).issubset(candidates) or len(selected) > config.top_n:
        raise NestedWalkForwardContractError(
            "baseline selector escaped the frozen fold-local candidate contract"
        )
    return tuple(selected)


def _expanding_prefixes(train: pd.DataFrame) -> list[pd.DataFrame]:
    decisions = sorted(
        pd.to_datetime(train["decision_timestamp"], utc=True).unique()
    )
    if not decisions:
        return []
    prefixes: list[pd.DataFrame] = []
    for number in range(1, 6):
        endpoint = max(1, int(np.ceil(len(decisions) * number / 5.0)))
        cutoff = decisions[endpoint - 1]
        prefixes.append(
            train.loc[
                pd.to_datetime(train["decision_timestamp"], utc=True).le(cutoff)
            ].copy()
        )
    return prefixes


def _stability_selection(
    train: pd.DataFrame,
    target: str,
    candidates: Sequence[str],
    frozen: FrozenM1AContract,
) -> tuple[tuple[str, ...], dict[str, Any]]:
    baseline = _selector_config(frozen)
    variant = frozen.contract["feature_contract"]["bounded_variant"]
    prefixes = _expanding_prefixes(train)
    if len(prefixes) != 5:
        raise CandidateUnavailable("stability selector has no expanding prefixes")
    prefix_selected = [
        _baseline_selection(prefix, target, candidates, baseline)
        for prefix in prefixes
    ]
    feature_rows: list[dict[str, Any]] = []
    for feature in candidates:
        fold_ic = _yearly_feature_ic(
            train, feature, target, baseline.min_group_size
        )
        values = np.asarray(
            [row["spearman_ic"] for row in fold_ic], dtype=float
        )
        median_ic = float(np.median(values)) if len(values) else np.nan
        if len(values) and median_ic != 0:
            direction_stability = float(
                np.mean(np.sign(values) == np.sign(median_ic))
            )
        else:
            direction_stability = 0.0
        feature_rows.append(
            {
                "feature": feature,
                "selection_frequency": float(
                    np.mean([feature in selected for selected in prefix_selected])
                ),
                "direction_stability": direction_stability,
                "median_spearman_ic": median_ic if np.isfinite(median_ic) else None,
                "spearman_ic_std": (
                    float(np.std(values, ddof=0)) if len(values) else None
                ),
                "missingness_rate": float(
                    pd.to_numeric(train[feature], errors="coerce").isna().mean()
                ),
                "fold_level_spearman_ic": fold_ic,
            }
        )
    eligible = [
        row
        for row in feature_rows
        if row["selection_frequency"]
        >= float(variant["selection_frequency_minimum"])
        and row["direction_stability"]
        >= float(variant["direction_stability_minimum"])
        and row["missingness_rate"] <= float(variant["missingness_maximum"])
        and row["median_spearman_ic"] is not None
        and abs(float(row["median_spearman_ic"]))
        >= float(variant["minimum_abs_median_ic"])
    ]
    eligible.sort(
        key=lambda row: (
            -float(row["selection_frequency"]),
            -abs(float(row["median_spearman_ic"])),
            -float(row["direction_stability"]),
            float(row["spearman_ic_std"]),
            float(row["missingness_rate"]),
            str(row["feature"]),
        )
    )

    kept: list[str] = []
    redundancy: list[dict[str, Any]] = []
    threshold = float(variant["redundancy_spearman_abs_maximum"])
    for row in eligible:
        feature = str(row["feature"])
        pruned_by: str | None = None
        pruned_correlation: float | None = None
        for previous in kept:
            pair = train[[feature, previous]].apply(
                pd.to_numeric, errors="coerce"
            ).dropna()
            if len(pair) < baseline.min_group_size:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", ConstantInputWarning)
                correlation, _ = stats.spearmanr(pair[feature], pair[previous])
            if np.isfinite(correlation) and abs(float(correlation)) > threshold:
                pruned_by = previous
                pruned_correlation = float(correlation)
                break
        if pruned_by is not None:
            redundancy.append(
                {
                    "pruned_feature": feature,
                    "retained_feature": pruned_by,
                    "spearman_correlation": pruned_correlation,
                    "rows": len(pair),
                }
            )
            continue
        kept.append(feature)
        if len(kept) >= int(variant["selected_feature_cap"]):
            break
    if len(kept) < int(variant["minimum_selected_features"]):
        raise CandidateUnavailable(
            "stability selector produced fewer than the frozen minimum features"
        )
    diagnostics = {
        "expanding_prefix_rows": [len(prefix) for prefix in prefixes],
        "prefix_selected_features": [list(values) for values in prefix_selected],
        "feature_diagnostics": feature_rows,
        "redundancy_pairs_and_pruned_features": redundancy,
        "selected_set_size": len(kept),
    }
    return tuple(kept), diagnostics


def select_features_fold_local(
    fit_population: pd.DataFrame,
    population: FoldPopulation,
    frozen: FrozenM1AContract,
    method: str,
) -> FeatureSelectionResult:
    """Fit one frozen selector only on the recorded inner-training rows."""
    validate_fold_population(population, frozen)
    if _population_fingerprint(fit_population) != population.training_population_fingerprint:
        raise NestedWalkForwardContractError(
            "global or validation-contaminated feature selection attempted"
        )
    if method not in SELECTOR_METHODS:
        raise NestedWalkForwardContractError(
            f"unsupported frozen selector method: {method}"
        )
    candidates = candidate_columns_for_regime(frozen, population.spec.regime)
    missing = sorted(set(candidates) - set(fit_population))
    if missing:
        raise NestedWalkForwardContractError(
            f"fold-local selector is missing frozen candidates: {missing[:5]}"
        )
    if method == "p3_fold_local_ic_selector":
        selected = _baseline_selection(
            fit_population,
            population.target_column,
            candidates,
            _selector_config(frozen),
        )
        if not selected:
            raise CandidateUnavailable("baseline selector selected no features")
        diagnostics = {
            "feature_diagnostics": [
                {
                    "feature": feature,
                    "missingness_rate": float(
                        pd.to_numeric(
                            fit_population[feature], errors="coerce"
                        ).isna().mean()
                    ),
                    "fold_level_spearman_ic": _yearly_feature_ic(
                        fit_population,
                        feature,
                        population.target_column,
                        _selector_config(frozen).min_group_size,
                    ),
                }
                for feature in selected
            ],
            "redundancy_pairs_and_pruned_features": [],
            "selected_set_size": len(selected),
        }
        selector_parameters = frozen.contract["feature_contract"][
            "baseline_selector"
        ]
    else:
        selected, diagnostics = _stability_selection(
            fit_population,
            population.target_column,
            candidates,
            frozen,
        )
        selector_parameters = frozen.contract["feature_contract"][
            "bounded_variant"
        ]
    lineage = _feature_selection_lineage(
        population,
        method,
        candidates,
        selected,
        diagnostics,
        selector_parameters,
    )
    return FeatureSelectionResult(
        method=method,
        selected_features=selected,
        candidate_columns=candidates,
        diagnostics=diagnostics,
        lineage=lineage,
    )


def _feature_selection_lineage(
    population: FoldPopulation,
    method: str,
    candidates: Sequence[str],
    selected: Sequence[str],
    diagnostics: Mapping[str, Any],
    selector_parameters: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **population.population_lineage,
        "fit_scope": "inner_training_only",
        "selector_method": method,
        "selector_parameters": selector_parameters,
        "candidate_count": len(candidates),
        "candidate_order_sha256": _candidate_order_sha256(candidates),
        "selected_features": list(selected),
        "selected_feature_count": len(selected),
        "diagnostics_sha256": _payload_sha256(diagnostics),
        "fit_population_fingerprint": population.training_population_fingerprint,
    }


@dataclass(frozen=True)
class FoldPreprocessor:
    features: tuple[str, ...]
    medians: dict[str, float]
    lineage: dict[str, Any]


def fit_fold_preprocessor(
    fit_population: pd.DataFrame,
    population: FoldPopulation,
    selection: FeatureSelectionResult,
    frozen: FrozenM1AContract,
) -> FoldPreprocessor:
    """Fit the frozen P3 median transform on inner training only."""
    validate_fold_population(population, frozen)
    if _population_fingerprint(fit_population) != population.training_population_fingerprint:
        raise NestedWalkForwardContractError(
            "transformer/imputation fit included non-training rows"
        )
    if selection.lineage.get("fit_population_fingerprint") != population.training_population_fingerprint:
        raise NestedWalkForwardContractError(
            "selector and preprocessor populations do not match"
        )
    features = selection.selected_features
    if not features or not set(features).issubset(selection.candidate_columns):
        raise NestedWalkForwardContractError(
            "preprocessor feature lineage is invalid"
        )
    numeric = fit_population[list(features)].apply(
        pd.to_numeric, errors="coerce"
    )
    medians = numeric.median(axis=0, skipna=True)
    values = medians.to_numpy(dtype=float)
    if len(values) != len(features) or not np.isfinite(values).all():
        raise CandidateUnavailable("selected feature has no finite training median")
    median_map = {feature: float(medians[feature]) for feature in features}
    transformations = {
        "missingness_handling": (
            "selector-local support; bounded variant maximum missingness 0.50"
        ),
        "imputation": "fold_local_training_median",
        "winsorization": "not_applied_by_frozen_p3_contract",
        "encoding": "not_required_for_numeric_p3_candidates",
        "scaling": "not_applied_by_frozen_p3_contract",
        "fit_scope": "inner_training_only",
    }
    lineage = _fold_preprocessor_lineage(
        population, selection, median_map, transformations
    )
    return FoldPreprocessor(features=features, medians=median_map, lineage=lineage)


def _fold_preprocessor_lineage(
    population: FoldPopulation,
    selection: FeatureSelectionResult,
    medians: Mapping[str, float],
    transformations: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **population.population_lineage,
        "features": list(selection.selected_features),
        "feature_selection_lineage_sha256": _payload_sha256(selection.lineage),
        "transformations": dict(transformations),
        "transformation_parameters": {"medians": dict(medians)},
        "fit_population_fingerprint": population.training_population_fingerprint,
    }


def transform_with_fold_preprocessor(
    frame: pd.DataFrame, preprocessor: FoldPreprocessor
) -> np.ndarray:
    missing = sorted(set(preprocessor.features) - set(frame))
    if missing:
        raise NestedWalkForwardContractError(
            f"transform population is missing selected features: {missing}"
        )
    numeric = frame[list(preprocessor.features)].apply(
        pd.to_numeric, errors="coerce"
    )
    transformed = numeric.fillna(pd.Series(preprocessor.medians)).to_numpy(
        dtype=float
    )
    if transformed.shape != (len(frame), len(preprocessor.features)) or not np.isfinite(
        transformed
    ).all():
        raise NestedWalkForwardContractError(
            "fold preprocessing produced invalid transformed values"
        )
    return transformed


@dataclass(frozen=True)
class FoldLocalPreparedData:
    """Reusable values for an exact fold-local selector/transform fit.

    The cache deliberately stores only deterministic computations from an
    identical training/validation population. Fold-specific lineage is rebuilt
    for every consumer, and estimators are never cached.
    """

    selected_features: tuple[str, ...]
    candidate_columns: tuple[str, ...]
    diagnostics: dict[str, Any]
    selector_parameters: dict[str, Any]
    medians: dict[str, float]
    transformations: dict[str, Any]
    train_x: np.ndarray
    validation_x: np.ndarray


def _fold_local_cache_key(
    population: FoldPopulation,
    method: str,
) -> tuple[str, ...]:
    return (
        population.spec.target_role,
        population.spec.regime,
        population.target_column,
        method,
        population.training_population_fingerprint,
        population.validation_population_fingerprint,
    )


def _prepare_fold_local_data(
    population: FoldPopulation,
    frozen: FrozenM1AContract,
    method: str,
    cache: dict[tuple[str, ...], FoldLocalPreparedData | str] | None,
) -> tuple[FeatureSelectionResult, FoldPreprocessor, np.ndarray, np.ndarray]:
    key = _fold_local_cache_key(population, method)
    cached = cache.get(key) if cache is not None else None
    if isinstance(cached, str):
        raise CandidateUnavailable(cached)
    if cached is None:
        try:
            selection = select_features_fold_local(
                population.train, population, frozen, method
            )
            preprocessor = fit_fold_preprocessor(
                population.train, population, selection, frozen
            )
            train_x = transform_with_fold_preprocessor(
                population.train, preprocessor
            )
            validation_x = transform_with_fold_preprocessor(
                population.validation, preprocessor
            )
        except CandidateUnavailable as error:
            if cache is not None:
                cache[key] = str(error)
            raise
        cached = FoldLocalPreparedData(
            selected_features=selection.selected_features,
            candidate_columns=selection.candidate_columns,
            diagnostics=selection.diagnostics,
            selector_parameters=dict(selection.lineage["selector_parameters"]),
            medians=dict(preprocessor.medians),
            transformations=dict(preprocessor.lineage["transformations"]),
            train_x=train_x,
            validation_x=validation_x,
        )
        if cache is not None:
            cache[key] = cached
        return selection, preprocessor, train_x, validation_x

    candidates = candidate_columns_for_regime(frozen, population.spec.regime)
    if cached.candidate_columns != candidates:
        raise NestedWalkForwardContractError(
            "cached fold-local candidate order does not match the frozen regime"
        )
    selection = FeatureSelectionResult(
        method=method,
        selected_features=cached.selected_features,
        candidate_columns=cached.candidate_columns,
        diagnostics=cached.diagnostics,
        lineage=_feature_selection_lineage(
            population,
            method,
            cached.candidate_columns,
            cached.selected_features,
            cached.diagnostics,
            cached.selector_parameters,
        ),
    )
    preprocessor = FoldPreprocessor(
        features=cached.selected_features,
        medians=dict(cached.medians),
        lineage=_fold_preprocessor_lineage(
            population,
            selection,
            cached.medians,
            cached.transformations,
        ),
    )
    expected_train = (len(population.train), len(cached.selected_features))
    expected_validation = (
        len(population.validation),
        len(cached.selected_features),
    )
    if (
        cached.train_x.shape != expected_train
        or cached.validation_x.shape != expected_validation
        or not np.isfinite(cached.train_x).all()
        or not np.isfinite(cached.validation_x).all()
    ):
        raise NestedWalkForwardContractError(
            "cached fold-local transformed values do not match the exact population"
        )
    return selection, preprocessor, cached.train_x, cached.validation_x


@dataclass(frozen=True)
class TuningCandidate:
    candidate_id: str
    target_role: str
    regime: str
    selector_method: str
    parameters: dict[str, Any]
    complexity_score: int


def _complexity_score(role: str, parameters: Mapping[str, Any]) -> int:
    if role == "lightgbm_regression":
        return int(parameters["n_estimators"]) * int(
            parameters["num_leaves"]
        ) * max(1, int(parameters["max_depth"]))
    if role == "decision_tree":
        return int(parameters["max_depth"]) * int(
            parameters["min_samples_leaf"]
        )
    raise NestedWalkForwardContractError(f"unsupported model role: {role}")


def frozen_grid_points(
    frozen: FrozenM1AContract, role: str
) -> tuple[dict[str, Any], ...]:
    """Expand exactly the bounded, seeded M1A role grid."""
    if role not in MODEL_ROLES:
        raise NestedWalkForwardContractError(f"unsupported model role: {role}")
    role_contract = frozen.contract["model_roles"][role]
    points = _expand_grid(role_contract["grid"])
    if len(points) != int(role_contract["grid_size"]):
        raise NestedWalkForwardContractError(
            f"{role} grid cardinality does not match M1A"
        )
    return tuple(points)


def tuning_candidates(
    frozen: FrozenM1AContract, role: str
) -> tuple[TuningCandidate, ...]:
    """Return role x regime x selector x frozen-grid candidates."""
    candidates: list[TuningCandidate] = []
    for regime in TRAINING_REGIMES:
        for selector_method in SELECTOR_METHODS:
            for parameters in frozen_grid_points(frozen, role):
                identity = {
                    "target_role": role,
                    "regime": regime,
                    "selector_method": selector_method,
                    "parameters": parameters,
                }
                candidate_id = (
                    f"{role}|{regime}|{selector_method}|"
                    f"{_payload_sha256(identity)}"
                )
                candidates.append(
                    TuningCandidate(
                        candidate_id=candidate_id,
                        target_role=role,
                        regime=regime,
                        selector_method=selector_method,
                        parameters=parameters,
                        complexity_score=_complexity_score(role, parameters),
                    )
                )
    if len({candidate.candidate_id for candidate in candidates}) != len(candidates):
        raise NestedWalkForwardContractError(
            "canonical tuning candidate identifiers are not unique"
        )
    return tuple(candidates)


def _validate_candidate(candidate: TuningCandidate, frozen: FrozenM1AContract) -> None:
    if (
        candidate.target_role not in MODEL_ROLES
        or candidate.regime not in TRAINING_REGIMES
        or candidate.selector_method not in SELECTOR_METHODS
    ):
        raise NestedWalkForwardContractError(
            "tuning candidate escaped frozen role/regime/selector choices"
        )
    allowed = frozen_grid_points(frozen, candidate.target_role)
    if not any(candidate.parameters == point for point in allowed):
        raise NestedWalkForwardContractError(
            "tuning candidate parameters are outside the frozen grid"
        )
    identity = {
        "target_role": candidate.target_role,
        "regime": candidate.regime,
        "selector_method": candidate.selector_method,
        "parameters": candidate.parameters,
    }
    expected_id = (
        f"{candidate.target_role}|{candidate.regime}|"
        f"{candidate.selector_method}|{_payload_sha256(identity)}"
    )
    if (
        candidate.candidate_id != expected_id
        or candidate.complexity_score
        != _complexity_score(candidate.target_role, candidate.parameters)
    ):
        raise NestedWalkForwardContractError(
            "tuning candidate identifier/complexity lineage mismatch"
        )


def _fit_estimator(
    candidate: TuningCandidate,
    train_x: np.ndarray,
    train_y: pd.Series,
    frozen: FrozenM1AContract,
) -> Any:
    _validate_candidate(candidate, frozen)
    if candidate.target_role == "decision_tree":
        numeric_y = pd.to_numeric(train_y, errors="coerce")
        if numeric_y.isna().any() or set(numeric_y.astype(int).unique()) != {0, 1}:
            raise CandidateUnavailable(
                "decision tree requires two inner-training classes"
            )
        y = numeric_y.astype(int)
        n_negative = int(y.eq(0).sum())
        n_positive = int(y.eq(1).sum())
        parameters = dict(candidate.parameters)
        class_weight_token = parameters.pop("class_weight")
        if class_weight_token != "fold_local_{0:1.0,1:n_negative/max(n_positive,1)}":
            raise NestedWalkForwardContractError(
                "tree fold-local class-weight equation drifted"
            )
        parameters["class_weight"] = {
            0: 1.0,
            1: n_negative / max(n_positive, 1),
        }
        estimator = DecisionTreeClassifier(**parameters)
        estimator.fit(train_x, y)
        return estimator

    parameters = dict(candidate.parameters)
    clip_low, clip_high = frozen.contract["model_roles"][
        "lightgbm_regression"
    ]["regression_target_clip"]
    y = pd.to_numeric(train_y, errors="coerce").clip(clip_low, clip_high)
    if y.isna().any():
        raise NestedWalkForwardContractError(
            "missing regression target reached estimator fit"
        )
    estimator = lgb.LGBMRegressor(**parameters)
    estimator.fit(train_x, y)
    return estimator


def _predict_estimator(
    estimator: Any, role: str, values: np.ndarray
) -> np.ndarray:
    if role == "decision_tree":
        positions = np.flatnonzero(estimator.classes_ == 1)
        if len(positions) != 1:
            raise NestedWalkForwardContractError(
                "tree estimator has no unique positive class"
            )
        predictions = estimator.predict_proba(values)[:, int(positions[0])]
    elif role == "lightgbm_regression":
        predictions = estimator.predict(values)
    else:
        raise NestedWalkForwardContractError(f"unsupported model role: {role}")
    result = np.asarray(predictions, dtype=float)
    if len(result) != len(values) or not np.isfinite(result).all():
        raise NestedWalkForwardContractError(
            "inner estimator produced invalid predictions"
        )
    if role == "decision_tree" and ((result < 0) | (result > 1)).any():
        raise NestedWalkForwardContractError(
            "tree estimator produced invalid probabilities"
        )
    return result


def _regression_metrics(target: pd.Series, prediction: np.ndarray) -> dict[str, float]:
    y = pd.to_numeric(target, errors="coerce").to_numpy(dtype=float)
    if len(y) != len(prediction) or not np.isfinite(y).all():
        raise NestedWalkForwardContractError(
            "invalid inner-validation regression target"
        )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConstantInputWarning)
        spearman, _ = stats.spearmanr(y, prediction)
        pearson, _ = stats.pearsonr(y, prediction)
    if not np.isfinite(spearman):
        raise CandidateUnavailable(
            "inner-validation Spearman IC is unavailable"
        )
    return {
        "spearman_ic": float(spearman),
        "pearson_correlation": float(pearson) if np.isfinite(pearson) else 0.0,
        "mae": float(mean_absolute_error(y, prediction)),
        "rmse": float(mean_squared_error(y, prediction) ** 0.5),
        "coverage": 1.0,
    }


def _tree_metrics(target: pd.Series, prediction: np.ndarray) -> dict[str, float]:
    numeric = pd.to_numeric(target, errors="coerce")
    if numeric.isna().any() or set(numeric.astype(int).unique()) != {0, 1}:
        raise CandidateUnavailable(
            "inner validation requires two classes for ROC AUC"
        )
    y = numeric.astype(int).to_numpy()
    prevalence = float(np.mean(y))
    mean_probability = float(np.mean(prediction))
    return {
        "roc_auc": float(roc_auc_score(y, prediction)),
        "pr_auc": float(average_precision_score(y, prediction)),
        "brier_score": float(brier_score_loss(y, prediction)),
        "calibration_mean_probability": mean_probability,
        "calibration_observed_rate": prevalence,
        "calibration_gap": mean_probability - prevalence,
        "prevalence": prevalence,
        "coverage": 1.0,
    }


@dataclass(frozen=True)
class InnerFoldEvaluation:
    outer_fold: str
    inner_fold: str
    candidate_id: str
    availability_status: str
    failure_reason: str | None
    metrics: dict[str, float]
    selected_feature_count: int | None
    lineage: dict[str, Any]


@dataclass(frozen=True)
class CandidateEvaluation:
    candidate: TuningCandidate
    availability_status: str
    failure_reason: str | None
    valid_inner_fold_count: int
    fold_evaluations: tuple[InnerFoldEvaluation, ...]
    aggregate_metrics: dict[str, float]
    evidence_scope: str
    lineage: dict[str, Any]


def evaluate_tuning_candidate(
    candidate: TuningCandidate,
    populations: Sequence[FoldPopulation],
    frozen: FrozenM1AContract,
    *,
    execution_scope: str,
    fold_local_cache: (
        dict[tuple[str, ...], FoldLocalPreparedData | str] | None
    ) = None,
) -> CandidateEvaluation:
    """Evaluate one candidate using inner-validation predictions only."""
    _validate_candidate(candidate, frozen)
    if execution_scope not in {"synthetic_m1b_test", "frozen_m1c"}:
        raise NestedWalkForwardContractError(
            "candidate evaluation requires an explicit synthetic or frozen scope"
        )
    matching = [
        population
        for population in populations
        if population.spec.target_role == candidate.target_role
        and population.spec.regime == candidate.regime
    ]
    validate_fold_collection(matching, frozen)
    if execution_scope == "frozen_m1c":
        for population in matching:
            _verify_spec_record(population.spec, frozen)
            expected = (
                population.spec.expected_train_rows,
                population.spec.expected_validation_rows,
                population.spec.expected_purge_count,
            )
            actual = (
                len(population.train),
                len(population.validation),
                len(population.purged_stable_row_ids),
            )
            if expected != actual:
                raise NestedWalkForwardContractError(
                    "M1C evaluation population does not match frozen fold counts"
                )
    fold_results: list[InnerFoldEvaluation] = []
    for population in sorted(
        matching, key=lambda item: item.spec.validation_decision_timestamp
    ):
        try:
            selection, preprocessor, train_x, validation_x = (
                _prepare_fold_local_data(
                    population,
                    frozen,
                    candidate.selector_method,
                    fold_local_cache,
                )
            )
            estimator = _fit_estimator(
                candidate,
                train_x,
                population.train[population.target_column],
                frozen,
            )
            predictions = _predict_estimator(
                estimator, candidate.target_role, validation_x
            )
            if candidate.target_role == "lightgbm_regression":
                metrics = _regression_metrics(
                    population.validation[population.target_column], predictions
                )
            else:
                metrics = _tree_metrics(
                    population.validation[population.target_column], predictions
                )
            model_lineage = {
                "family": frozen.contract["model_roles"][
                    candidate.target_role
                ]["family"],
                "parameters": candidate.parameters,
                "complexity_score": candidate.complexity_score,
                "fit_scope": "inner_training_only",
                "fit_population_fingerprint": population.training_population_fingerprint,
                "target_column": population.target_column,
                "target_fingerprint": dataframe_fingerprint(
                    population.train[
                        ["stable_row_id", "label_end_date", population.target_column]
                    ]
                ),
            }
            lineage = {
                "fold": population.population_lineage,
                "features": selection.lineage,
                "transformations": preprocessor.lineage,
                "parameters": model_lineage,
                "validation_population_fingerprint": population.validation_population_fingerprint,
                "metric_scope": "inner_validation_only",
            }
            fold_results.append(
                InnerFoldEvaluation(
                    outer_fold=population.spec.outer_fold,
                    inner_fold=population.spec.inner_fold,
                    candidate_id=candidate.candidate_id,
                    availability_status="available",
                    failure_reason=None,
                    metrics=metrics,
                    selected_feature_count=len(selection.selected_features),
                    lineage=lineage,
                )
            )
        except CandidateUnavailable as error:
            fold_results.append(
                InnerFoldEvaluation(
                    outer_fold=population.spec.outer_fold,
                    inner_fold=population.spec.inner_fold,
                    candidate_id=candidate.candidate_id,
                    availability_status="unavailable",
                    failure_reason=str(error),
                    metrics={},
                    selected_feature_count=None,
                    lineage={"fold": population.population_lineage},
                )
            )

    available = [
        result
        for result in fold_results
        if result.availability_status == "available"
    ]
    minimum = int(
        frozen.contract["inner_protocol"][
            "minimum_valid_inner_folds_for_tuning"
        ]
    )
    if len(available) < minimum:
        status = "unavailable"
        reason = "fewer_than_two_valid_inner_folds"
        aggregate: dict[str, float] = {}
    else:
        status = "available_for_selection"
        reason = None
        selected_counts = np.asarray(
            [result.selected_feature_count for result in available], dtype=float
        )
        if candidate.target_role == "lightgbm_regression":
            values = np.asarray(
                [result.metrics["spearman_ic"] for result in available]
            )
            aggregate = {
                "median_spearman_ic": float(np.median(values)),
                "spearman_ic_std": float(np.std(values, ddof=0)),
                "positive_fold_frequency": float(np.mean(values > 0)),
                "mean_pearson_correlation": float(
                    np.mean(
                        [
                            result.metrics["pearson_correlation"]
                            for result in available
                        ]
                    )
                ),
                "mean_mae": float(
                    np.mean([result.metrics["mae"] for result in available])
                ),
                "mean_rmse": float(
                    np.mean([result.metrics["rmse"] for result in available])
                ),
                "mean_selected_feature_count": float(np.mean(selected_counts)),
            }
        else:
            values = np.asarray(
                [result.metrics["roc_auc"] for result in available]
            )
            aggregate = {
                "median_roc_auc": float(np.median(values)),
                "roc_auc_std": float(np.std(values, ddof=0)),
                "mean_pr_auc": float(
                    np.mean([result.metrics["pr_auc"] for result in available])
                ),
                "mean_brier_score": float(
                    np.mean(
                        [result.metrics["brier_score"] for result in available]
                    )
                ),
                "mean_absolute_calibration_gap": float(
                    np.mean(
                        [
                            abs(result.metrics["calibration_gap"])
                            for result in available
                        ]
                    )
                ),
                "mean_selected_feature_count": float(np.mean(selected_counts)),
            }
    lineage = {
        "candidate_id": candidate.candidate_id,
        "target_role": candidate.target_role,
        "training_regime": candidate.regime,
        "selector_method": candidate.selector_method,
        "parameters": candidate.parameters,
        "complexity_score": candidate.complexity_score,
        "inner_fold_lineage_sha256": _payload_sha256(
            [result.lineage for result in fold_results]
        ),
        "objective_inputs": "inner_validation_predictions_and_targets_only",
        "execution_scope": execution_scope,
        "outer_oos_consumed": False,
        "b1e_performance_consumed": False,
    }
    return CandidateEvaluation(
        candidate=candidate,
        availability_status=status,
        failure_reason=reason,
        valid_inner_fold_count=len(available),
        fold_evaluations=tuple(fold_results),
        aggregate_metrics=aggregate,
        evidence_scope="inner_validation_only",
        lineage=lineage,
    )


_PROHIBITED_SELECTION_TOKENS = (
    "outer_oos",
    "outer-oos",
    "p4_holdings",
    "p4_gates",
    "p4_gate",
    "b1d",
    "b1e",
    "cagr",
    "sharpe",
    "sortino",
    "alpha",
    "drawdown",
    "information_ratio",
    "information ratio",
    "turnover",
    "hit_rate",
    "hit rate",
    "scenario_result",
    "portfolio_nav",
    "portfolio nav",
    "risk_free",
    "risk-free",
    "outputs/metrics.parquet",
    "product_report.md",
)


def assert_predictive_selection_inputs(payload: Any) -> None:
    """Reject any outer/portfolio/performance-directed selection evidence."""
    def visit(value: Any, path: str) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                key_text = str(key).lower()
                if any(token in key_text for token in _PROHIBITED_SELECTION_TOKENS):
                    raise NestedWalkForwardContractError(
                        f"prohibited selection input at {path}.{key}"
                    )
                visit(item, f"{path}.{key}")
            return
        if isinstance(value, (list, tuple, set)):
            for index, item in enumerate(value):
                visit(item, f"{path}[{index}]")
            return
        if isinstance(value, (str, Path)):
            text = str(value).lower().replace("-", "_")
            if any(
                token.replace("-", "_") in text
                for token in _PROHIBITED_SELECTION_TOKENS
            ):
                raise NestedWalkForwardContractError(
                    f"prohibited selection input at {path}"
                )

    visit(payload, "selection_inputs")


@dataclass(frozen=True)
class TuningRunContext:
    outer_fold: str
    outer_decision_timestamp: pd.Timestamp
    execution_scope: str = "synthetic_m1b_test"
    tuning_attempt: int = 1
    trigger: str = "predeclared_inner_validation"
    outer_oos_consumed: bool = False
    b1e_performance_consumed: bool = False
    performance_threshold_observed: bool = False
    previous_threshold_result: str | None = None
    selection_locked: bool = False


def validate_tuning_run_context(
    context: TuningRunContext,
    *,
    require_locked: bool = False,
    frozen: FrozenM1AContract | None = None,
) -> None:
    if context.execution_scope not in {"synthetic_m1b_test", "frozen_m1c"}:
        raise NestedWalkForwardContractError(
            "unsupported nested tuning execution scope"
        )
    if (
        context.tuning_attempt != 1
        or context.trigger != "predeclared_inner_validation"
        or context.outer_oos_consumed
        or context.b1e_performance_consumed
        or context.performance_threshold_observed
        or context.previous_threshold_result is not None
    ):
        raise NestedWalkForwardContractError(
            "adaptive retuning or prohibited outer/performance feedback attempted"
        )
    if context.execution_scope == "frozen_m1c":
        if frozen is None:
            raise NestedWalkForwardContractError(
                "frozen M1C scope requires the verified M1A contract"
            )
        matching = [
            row
            for row in frozen.outer_folds
            if row["outer_fold"] == context.outer_fold
            and _timestamp(
                row["decision_timestamp"], "frozen outer decision"
            )
            == _timestamp(
                context.outer_decision_timestamp, "outer decision"
            )
        ]
        if len(matching) != 2:
            raise NestedWalkForwardContractError(
                "M1C context does not match the exact P3 outer fold"
            )
    if require_locked and not context.selection_locked:
        raise NestedWalkForwardContractError(
            "outer refit requires a winner frozen from inner evidence"
        )


def _compare_float(left: float, right: float, *, higher: bool, tolerance: float) -> int:
    if abs(left - right) <= tolerance:
        return 0
    if higher:
        return -1 if left > right else 1
    return -1 if left < right else 1


def select_inner_winner(
    evaluations: Sequence[CandidateEvaluation],
    role: str,
    frozen: FrozenM1AContract,
    context: TuningRunContext,
    *,
    selection_inputs: Mapping[str, Any] | None = None,
) -> CandidateEvaluation:
    """Apply the frozen predictive objective and simplicity tie-breakers."""
    validate_tuning_run_context(context, frozen=frozen)
    assert_predictive_selection_inputs(selection_inputs or {})
    available = [
        evaluation
        for evaluation in evaluations
        if evaluation.candidate.target_role == role
        and evaluation.availability_status == "available_for_selection"
    ]
    if not available:
        raise CandidateUnavailable("no candidate has enough valid inner folds")
    for evaluation in available:
        _validate_candidate(evaluation.candidate, frozen)
        assert_predictive_selection_inputs(evaluation.aggregate_metrics)
        if (
            evaluation.evidence_scope != "inner_validation_only"
            or evaluation.lineage.get("outer_oos_consumed") is not False
            or evaluation.lineage.get("b1e_performance_consumed") is not False
            or evaluation.lineage.get("objective_inputs")
            != "inner_validation_predictions_and_targets_only"
        ):
            raise NestedWalkForwardContractError(
                "winner candidate contains non-inner or performance evidence"
            )
    tolerance = float(frozen.contract["selection_rule"]["tie_tolerance"])

    def compare(
        left: CandidateEvaluation, right: CandidateEvaluation
    ) -> int:
        left_metrics = left.aggregate_metrics
        right_metrics = right.aggregate_metrics
        if role == "lightgbm_regression":
            checks = (
                ("median_spearman_ic", True),
                ("spearman_ic_std", False),
                ("positive_fold_frequency", True),
                ("mean_selected_feature_count", False),
            )
        elif role == "decision_tree":
            checks = (
                ("median_roc_auc", True),
                ("roc_auc_std", False),
                ("mean_selected_feature_count", False),
            )
        else:
            raise NestedWalkForwardContractError(
                f"unsupported model role: {role}"
            )
        for name, higher in checks:
            result = _compare_float(
                float(left_metrics[name]),
                float(right_metrics[name]),
                higher=higher,
                tolerance=tolerance,
            )
            if result:
                return result
        if left.candidate.complexity_score != right.candidate.complexity_score:
            return (
                -1
                if left.candidate.complexity_score
                < right.candidate.complexity_score
                else 1
            )
        if left.candidate.candidate_id == right.candidate.candidate_id:
            return 0
        return -1 if left.candidate.candidate_id < right.candidate.candidate_id else 1

    return sorted(available, key=cmp_to_key(compare))[0]


@dataclass(frozen=True)
class OuterRefitResult:
    candidate: TuningCandidate
    selected_features: tuple[str, ...]
    predictions: np.ndarray
    estimator: Any
    preprocessor: FoldPreprocessor
    lineage: dict[str, Any]


def refit_locked_outer_candidate(
    outer_training: pd.DataFrame,
    outer_score_features: pd.DataFrame,
    winner: CandidateEvaluation,
    frozen: FrozenM1AContract,
    context: TuningRunContext,
) -> OuterRefitResult:
    """M1C hook: refit once after selection, without reading outer labels."""
    validate_tuning_run_context(
        context, require_locked=True, frozen=frozen
    )
    candidate = winner.candidate
    _validate_candidate(candidate, frozen)
    if (
        winner.availability_status != "available_for_selection"
        or winner.evidence_scope != "inner_validation_only"
    ):
        raise NestedWalkForwardContractError(
            "outer refit winner was not frozen from valid inner evidence"
        )
    target = _role_target(frozen, candidate.target_role)
    required_training = _required_columns(
        candidate.target_role, candidate.regime, target
    )
    missing_training = sorted(required_training - set(outer_training))
    if missing_training:
        raise NestedWalkForwardContractError(
            f"outer training population is missing columns: {missing_training}"
        )
    candidates = candidate_columns_for_regime(frozen, candidate.regime)
    missing_candidates = sorted(set(candidates) - set(outer_training))
    if missing_candidates:
        raise NestedWalkForwardContractError(
            f"outer training population is missing candidates: {missing_candidates[:5]}"
        )
    score_required = {
        "stable_row_id",
        "decision_timestamp",
        "source_feature_available_at_decision",
        *candidates,
    }
    missing_score = sorted(score_required - set(outer_score_features))
    if missing_score:
        raise NestedWalkForwardContractError(
            f"outer score population is missing columns: {missing_score[:5]}"
        )
    allowed_metadata = {
        "stable_row_id",
        "decision_timestamp",
        "source_feature_available_at_decision",
    }
    prohibited_score_columns = sorted(
        column
        for column in outer_score_features
        if column not in allowed_metadata
        and column not in candidates
        and any(
            fnmatchcase(column, pattern)
            for pattern in _exclusion_patterns(frozen)
        )
    )
    if target in outer_score_features or prohibited_score_columns:
        raise NestedWalkForwardContractError(
            "outer score interface received target/support/model-output fields"
        )
    training = outer_training.copy()
    training["decision_timestamp"] = pd.to_datetime(
        training["decision_timestamp"], utc=True, errors="coerce"
    )
    training["label_end_date"] = pd.to_datetime(
        training["label_end_date"], utc=True, errors="coerce"
    )
    decision = _timestamp(
        context.outer_decision_timestamp, "outer decision timestamp"
    )
    if (
        not training["decision_timestamp"].lt(decision).all()
        or not training["label_end_date"].lt(decision).all()
        or not _training_mask(training, target, candidate.regime).all()
    ):
        raise NestedWalkForwardContractError(
            "outer refit population violates maturity, availability, or regime"
        )
    score_decision = pd.to_datetime(
        outer_score_features["decision_timestamp"], utc=True, errors="coerce"
    )
    if not score_decision.eq(decision).all():
        raise NestedWalkForwardContractError(
            "outer score rows do not equal the held-out outer decision"
        )
    if not outer_score_features["source_feature_available_at_decision"].eq(True).all():
        raise NestedWalkForwardContractError(
            "outer score row has unavailable source features"
        )
    overlap = set(map(str, training["stable_row_id"])) & set(
        map(str, outer_score_features["stable_row_id"])
    )
    if overlap:
        raise NestedWalkForwardContractError(
            "outer-OOS row was reused in outer refit"
        )

    # The same fold-local selector/preprocessor interfaces are reused with an
    # explicit synthetic population scope representing the complete outer fit.
    outer_spec = InnerFoldSpec(
        outer_fold=context.outer_fold,
        inner_fold="outer_refit_after_inner_lock",
        target_role=candidate.target_role,
        regime=candidate.regime,
        outer_decision_timestamp=decision + pd.Timedelta(days=1),
        validation_decision_timestamp=decision,
    )
    empty_validation = training.iloc[0:0].copy()
    population_lineage = {
        "outer_fold": context.outer_fold,
        "inner_fold": "outer_refit_after_inner_lock",
        "target_role": candidate.target_role,
        "training_regime": candidate.regime,
        "target_column": target,
        "outer_decision_timestamp": _iso(decision),
        "validation_decision_timestamp": None,
        "training_rows": len(training),
        "validation_rows": 0,
        "purged_rows": 0,
        "training_population_fingerprint": _population_fingerprint(training),
        "validation_population_fingerprint": _population_fingerprint(
            empty_validation
        ),
        "fit_scope": "complete_outer_training_after_inner_winner_lock",
    }
    outer_population = FoldPopulation(
        spec=outer_spec,
        target_column=target,
        train=training,
        validation=empty_validation,
        purged_stable_row_ids=(),
        training_population_fingerprint=population_lineage[
            "training_population_fingerprint"
        ],
        validation_population_fingerprint=population_lineage[
            "validation_population_fingerprint"
        ],
        population_lineage=population_lineage,
    )
    # Outer refit has no validation population, so invoke the fitting pieces
    # directly while retaining the same exact population fingerprint checks.
    if candidate.selector_method == "p3_fold_local_ic_selector":
        selected = _baseline_selection(
            training, target, candidates, _selector_config(frozen)
        )
        if not selected:
            raise CandidateUnavailable("outer refit selected no features")
        diagnostics = {
            "feature_diagnostics": [
                {
                    "feature": feature,
                    "missingness_rate": float(
                        pd.to_numeric(
                            training[feature], errors="coerce"
                        ).isna().mean()
                    ),
                    "fold_level_spearman_ic": _yearly_feature_ic(
                        training,
                        feature,
                        target,
                        _selector_config(frozen).min_group_size,
                    ),
                }
                for feature in selected
            ],
            "redundancy_pairs_and_pruned_features": [],
            "selected_set_size": len(selected),
        }
        parameters = frozen.contract["feature_contract"]["baseline_selector"]
    else:
        selected, diagnostics = _stability_selection(
            training, target, candidates, frozen
        )
        parameters = frozen.contract["feature_contract"]["bounded_variant"]
    selection = FeatureSelectionResult(
        method=candidate.selector_method,
        selected_features=selected,
        candidate_columns=candidates,
        diagnostics=diagnostics,
        lineage={
            **population_lineage,
            "fit_scope": "complete_outer_training_after_inner_winner_lock",
            "selector_method": candidate.selector_method,
            "selector_parameters": parameters,
            "selected_features": list(selected),
            "selected_feature_count": len(selected),
            "diagnostics_sha256": _payload_sha256(diagnostics),
            "diagnostics": diagnostics,
            "fit_population_fingerprint": population_lineage[
                "training_population_fingerprint"
            ],
        },
    )
    numeric = training[list(selected)].apply(pd.to_numeric, errors="coerce")
    medians = numeric.median(axis=0, skipna=True)
    if not np.isfinite(medians.to_numpy(dtype=float)).all():
        raise CandidateUnavailable("outer refit has invalid feature median")
    median_map = {feature: float(medians[feature]) for feature in selected}
    preprocessor = FoldPreprocessor(
        features=selected,
        medians=median_map,
        lineage={
            **population_lineage,
            "features": list(selected),
            "feature_selection_lineage_sha256": _payload_sha256(
                selection.lineage
            ),
            "transformations": {
                "imputation": "fold_local_training_median",
                "fit_scope": "complete_outer_training_after_inner_winner_lock",
                "winsorization": "not_applied_by_frozen_p3_contract",
                "encoding": "not_required_for_numeric_p3_candidates",
                "scaling": "not_applied_by_frozen_p3_contract",
            },
            "transformation_parameters": {"medians": median_map},
            "fit_population_fingerprint": population_lineage[
                "training_population_fingerprint"
            ],
        },
    )
    train_x = transform_with_fold_preprocessor(training, preprocessor)
    score_x = transform_with_fold_preprocessor(
        outer_score_features, preprocessor
    )
    estimator = _fit_estimator(
        candidate, train_x, training[target], frozen
    )
    predictions = _predict_estimator(
        estimator, candidate.target_role, score_x
    )
    lineage = {
        "fold": context.outer_fold,
        "population": population_lineage,
        "target": {
            "column": target,
            "training_label_end_max": _iso(training["label_end_date"].max()),
            "target_fingerprint": dataframe_fingerprint(
                training[["stable_row_id", "label_end_date", target]]
            ),
        },
        "features": selection.lineage,
        "transformations": preprocessor.lineage,
        "parameters": {
            "family": frozen.contract["model_roles"][candidate.target_role][
                "family"
            ],
            "values": candidate.parameters,
            "complexity_score": candidate.complexity_score,
        },
        "winner_lineage_sha256": _payload_sha256(winner.lineage),
        "outer_oos_target_consumed": False,
        "b1e_performance_consumed": False,
    }
    return OuterRefitResult(
        candidate=candidate,
        selected_features=selected,
        predictions=predictions,
        estimator=estimator,
        preprocessor=preprocessor,
        lineage=lineage,
    )
