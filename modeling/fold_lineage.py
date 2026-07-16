"""Fold-local feature selection and fail-closed artifact lineage.

Persisted feature/preprocessing state is reusable only when its recorded data,
population, cutoff, horizon, label policy, and selector configuration match the
consumer exactly.  Historical folds normally select locally instead of loading
an external feature set.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from modeling.constants import get_feature_candidates


LINEAGE_SCHEMA_VERSION = 1


class LineageError(ValueError):
    """Raised when persisted state cannot prove exact compatibility."""


@dataclass(frozen=True)
class SelectorConfig:
    top_n: int = 40
    min_abs_ic: float = 0.02
    min_ic_years: int = 1
    min_group_size: int = 30
    corr_threshold: float = 0.85


def _json_default(value: Any) -> str:
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).isoformat()
    return str(value)


def dataframe_fingerprint(df: pd.DataFrame) -> str:
    """Return a deterministic content fingerprint, independent of row order."""
    columns = sorted(map(str, df.columns))
    work = df.reindex(columns=columns).copy()
    schema = [(c, str(work[c].dtype)) for c in columns]
    digest = hashlib.sha256(json.dumps(schema, separators=(",", ":")).encode())
    try:
        row_hashes = pd.util.hash_pandas_object(work, index=False, categorize=True)
    except TypeError:
        serial = work.map(lambda x: json.dumps(x, sort_keys=True, default=_json_default))
        row_hashes = pd.util.hash_pandas_object(serial, index=False, categorize=True)
    digest.update(np.sort(row_hashes.to_numpy(dtype="uint64", copy=False)).tobytes())
    return digest.hexdigest()


def select_fold_features(
    df_train: pd.DataFrame,
    target_col: str,
    config: SelectorConfig | None = None,
    force_include: list[str] | None = None,
) -> list[str]:
    """Select candidates, IC ranks, and correlations on one training fold only."""
    cfg = config or SelectorConfig()
    if target_col not in df_train:
        return []
    candidates = get_feature_candidates(df_train)
    labeled = df_train[df_train[target_col].notna()]
    ranked: list[tuple[str, float]] = []
    for feature in candidates:
        yearly_ic: list[float] = []
        for _, group in labeled[["fiscal_year", feature, target_col]].groupby("fiscal_year"):
            pair = group[[feature, target_col]].dropna()
            if len(pair) < cfg.min_group_size:
                continue
            corr, _ = stats.spearmanr(pair[feature], pair[target_col])
            if np.isfinite(corr):
                yearly_ic.append(float(corr))
        if len(yearly_ic) < cfg.min_ic_years:
            continue
        mean_ic = float(np.mean(yearly_ic))
        if abs(mean_ic) < cfg.min_abs_ic:
            continue
        icir = abs(mean_ic / (float(np.std(yearly_ic)) + 1e-8))
        ranked.append((feature, icir))
    ranked.sort(key=lambda item: (-item[1], item[0]))
    ordered = [feature for feature, _ in ranked[: cfg.top_n]]
    for feature in force_include or []:
        if feature in candidates and feature not in ordered:
            ordered.append(feature)

    kept: list[str] = []
    for feature in ordered:
        duplicate = False
        for previous in kept:
            pair = df_train[[feature, previous]].dropna()
            if len(pair) < 50:
                continue
            corr, _ = stats.spearmanr(pair[feature], pair[previous])
            if np.isfinite(corr) and abs(corr) > cfg.corr_threshold:
                duplicate = True
                break
        if not duplicate:
            kept.append(feature)
    return kept


def make_lineage(
    *,
    dataset: pd.DataFrame,
    training_population: pd.DataFrame,
    horizon: str,
    target_col: str,
    label_policy: str,
    cutoff: str,
    selector_config: dict[str, Any],
    features: list[str],
    development_population: pd.DataFrame | None = None,
    selection_population: pd.DataFrame | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": LINEAGE_SCHEMA_VERSION,
        "dataset_fingerprint": dataframe_fingerprint(dataset),
        "training_population_fingerprint": dataframe_fingerprint(training_population),
        "selection_population_fingerprint": dataframe_fingerprint(
            selection_population if selection_population is not None else training_population
        ),
        "development_population_fingerprint": (
            dataframe_fingerprint(development_population)
            if development_population is not None else None
        ),
        "horizon": horizon,
        "target_col": target_col,
        "label_policy": label_policy,
        "cutoff": cutoff,
        "selector_config": selector_config,
        "features": list(features),
    }


def validate_lineage(recorded: dict[str, Any] | None, expected: dict[str, Any]) -> None:
    """Fail closed unless every expected lineage field matches exactly."""
    if not isinstance(recorded, dict):
        raise LineageError("artifact has no feature/preprocessing lineage")
    required = {
        "schema_version", "dataset_fingerprint", "training_population_fingerprint",
        "selection_population_fingerprint",
        "development_population_fingerprint", "horizon", "target_col",
        "label_policy", "cutoff", "selector_config", "features",
    }
    missing = sorted(required - recorded.keys())
    if missing:
        raise LineageError(f"artifact lineage is incomplete: missing {missing}")
    for field, value in expected.items():
        if recorded.get(field) != value:
            raise LineageError(
                f"artifact lineage mismatch for {field}: "
                f"recorded={recorded.get(field)!r}, expected={value!r}"
            )


def selector_config_dict(config: SelectorConfig) -> dict[str, Any]:
    return asdict(config)
