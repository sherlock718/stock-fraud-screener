"""Shared fold-local modeling helpers for canonical OOS prediction builders.

This module contains the reusable implementation extracted from the historical
Session V3.2 builder. Historical builders remain intact as evidence; active
canonical code imports this neutral module.
"""
from __future__ import annotations

import hashlib
import json
import warnings
from dataclasses import asdict
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from scipy.stats import ConstantInputWarning
from sklearn.tree import DecisionTreeClassifier

from modeling.fold_lineage import (
    SelectorConfig,
    dataframe_fingerprint,
    select_fold_features,
)


MODEL_ROLES = ("decision_tree", "lightgbm_regression")

EXCLUSION_NO_HISTORY = "fold_no_eligible_training_history"
EXCLUSION_TREE_CLASSES = "fold_tree_requires_two_training_classes"
EXCLUSION_NO_FEATURES = "fold_no_selected_features"
EXCLUSION_INVALID_MEDIANS = "fold_invalid_feature_medians"
EXCLUSION_INVALID_OUTPUT = "fold_invalid_model_output"
EXCLUSION_FUTURE = "future_decision_after_v3_1_freeze"
LABEL_DERIVED_CANDIDATE_AVAILABILITY = {
    "observed_excess_return_3y": "label_end_date",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    )


def selector_config(config: dict[str, Any]) -> SelectorConfig:
    frozen = config["feature_contract"]["selector"]
    result = SelectorConfig(**frozen)
    if asdict(result) != frozen or result.top_n != 28:
        raise RuntimeError(
            "V3.1 selector configuration did not round-trip exactly"
        )
    return result


def _role_contract(
    role: str, config: dict[str, Any]
) -> tuple[str, str]:
    if role == "decision_tree":
        return (
            config["targets"]["decision_tree"],
            "_v3_2_beat_local_market_target",
        )
    if role == "lightgbm_regression":
        return (
            config["targets"]["lightgbm_regression"],
            "_v3_2_forward_return_target",
        )
    raise ValueError(f"unsupported V3.2 model role: {role}")


def _select_features(
    train: pd.DataFrame,
    role: str,
    config: dict[str, Any],
    selector: SelectorConfig,
) -> tuple[list[str], str, str]:
    target, selector_target = _role_contract(role, config)
    candidates = list(config["feature_contract"]["candidate_columns"])
    selection = train[["fiscal_year", *candidates]].copy()
    selection[selector_target] = pd.to_numeric(
        train[target], errors="coerce"
    ).to_numpy()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConstantInputWarning)
        features = select_fold_features(
            selection, selector_target, selector
        )
    if not set(features).issubset(candidates):
        raise RuntimeError(
            "fold selector escaped the frozen 119 feature candidates"
        )
    if len(features) > selector.top_n:
        raise RuntimeError(
            "fold selector exceeded the frozen 28-feature cap"
        )
    return features, target, selector_target


def _medians(
    train: pd.DataFrame, features: list[str]
) -> dict[str, float] | None:
    medians = train[features].apply(
        pd.to_numeric, errors="coerce"
    ).median(axis=0, skipna=True)
    values = medians.to_numpy(dtype=float)
    if len(values) != len(features) or not np.isfinite(values).all():
        return None
    return {feature: float(medians[feature]) for feature in features}


def _transform(
    frame: pd.DataFrame,
    features: list[str],
    medians: dict[str, float],
) -> np.ndarray:
    numeric = frame[features].apply(pd.to_numeric, errors="coerce")
    return numeric.fillna(pd.Series(medians)).to_numpy(dtype=float)


def mask_unavailable_score_features(
    score: pd.DataFrame,
    features: list[str],
    decision_timestamp: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Mask label-derived candidates until their certified availability."""
    masked = score.copy()
    counts: dict[str, int] = {}
    for (
        feature,
        availability_column,
    ) in LABEL_DERIVED_CANDIDATE_AVAILABILITY.items():
        if feature not in features:
            continue
        available_at = pd.to_datetime(
            masked[availability_column], utc=True, errors="coerce"
        )
        unavailable = available_at.isna() | available_at.ge(
            decision_timestamp
        )
        counts[feature] = int(
            (unavailable & masked[feature].notna()).sum()
        )
        masked.loc[unavailable, feature] = np.nan
    return masked, counts


def _artifact_id(path: Path) -> str:
    return f"sha256:{sha256_file(path)}"


def fit_fold_role(
    train: pd.DataFrame,
    score: pd.DataFrame,
    role: str,
    fold_id: str,
    config: dict[str, Any],
    output_root: Path,
) -> tuple[np.ndarray | None, dict[str, Any], str | None]:
    """Fit one fold-local model role, returning exact fail-closed codes."""
    if train.empty:
        return None, {}, EXCLUSION_NO_HISTORY
    features, target, selector_target = _select_features(
        train, role, config, selector_config(config)
    )
    if (
        role == "decision_tree"
        and pd.to_numeric(train[target], errors="coerce").nunique() != 2
    ):
        return None, {}, EXCLUSION_TREE_CLASSES
    if not features:
        return None, {}, EXCLUSION_NO_FEATURES
    medians = _medians(train, features)
    if medians is None:
        return None, {}, EXCLUSION_INVALID_MEDIANS
    train_x = _transform(train, features, medians)
    fold_decision_timestamp = pd.Timestamp(
        score["decision_timestamp"].iloc[0]
    )
    score_available, masked_score_values = (
        mask_unavailable_score_features(
            score, features, fold_decision_timestamp
        )
    )
    score_x = _transform(score_available, features, medians)
    if not np.isfinite(train_x).all() or not np.isfinite(score_x).all():
        return None, {}, EXCLUSION_INVALID_MEDIANS

    artifact_dir = output_root / f"models/{fold_id}/{role}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    feature_path = artifact_dir / "features.json"
    preprocessing_path = artifact_dir / "preprocessing.json"
    target_path = artifact_dir / "target.json"
    model_configuration_path = artifact_dir / "model_configuration.json"
    model_path = artifact_dir / "model.joblib"
    candidates = config["feature_contract"]["candidate_columns"]
    selection_population = train[
        ["stable_row_id", "fiscal_year", target, *candidates]
    ].copy()
    write_json(
        feature_path,
        {
            "role": role,
            "fold_id": fold_id,
            "candidate_count": len(candidates),
            "candidate_columns": candidates,
            "selector": config["feature_contract"]["selector"],
            "selector_target_alias": selector_target,
            "selected_feature_count": len(features),
            "selected_features": features,
            "selection_population_fingerprint": dataframe_fingerprint(
                selection_population
            ),
        },
    )
    write_json(
        preprocessing_path,
        {
            "role": role,
            "fold_id": fold_id,
            "method": "fold_local_median_imputation_only",
            "fit_rows": len(train),
            "features": features,
            "medians": medians,
            "score_availability_rule": (
                "label-derived candidate value is masked when its certified "
                "label_end_date is null or not strictly before the score "
                "decision timestamp"
            ),
            "label_derived_candidate_availability": (
                LABEL_DERIVED_CANDIDATE_AVAILABILITY
            ),
            "masked_non_null_score_values": masked_score_values,
            "fit_population_fingerprint": dataframe_fingerprint(
                train[["stable_row_id", *features]]
            ),
        },
    )
    write_json(
        target_path,
        {
            "role": role,
            "fold_id": fold_id,
            "target": target,
            "regression_fit_clip": (
                config["targets"]["regression_clip"]
                if role == "lightgbm_regression"
                else None
            ),
            "training_label_rule": config["decision_calendar"][
                "training_label_rule"
            ],
            "training_label_end_max": pd.Timestamp(
                train["label_end_date"].max()
            ).isoformat(),
            "clean_training_filter": config[
                "model_training_population"
            ]["required_filter"],
            "training_rows": len(train),
            "target_fingerprint": dataframe_fingerprint(
                train[["stable_row_id", "label_end_date", target]]
            ),
        },
    )

    if role == "decision_tree":
        y = train[target].astype(int)
        n_negative = int(y.eq(0).sum())
        n_positive = int(y.eq(1).sum())
        params = {
            "max_depth": config["decision_tree"]["max_depth"],
            "min_samples_leaf": config["decision_tree"][
                "min_samples_leaf"
            ],
            "min_samples_split": config["decision_tree"][
                "min_samples_split"
            ],
            "class_weight": {
                0: 1.0,
                1: n_negative / max(n_positive, 1),
            },
            "random_state": config["decision_tree"]["random_state"],
        }
        write_json(
            model_configuration_path,
            {
                "family": config["decision_tree"]["family"],
                "role": role,
                "fold_id": fold_id,
                "parameters": params,
                "parameter_source": (
                    "V3.1 frozen configuration plus frozen fold-local "
                    "class-weight equation"
                ),
            },
        )
        model = DecisionTreeClassifier(**params)
        model.fit(train_x, y)
        class_positions = np.flatnonzero(model.classes_ == 1)
        if len(class_positions) != 1:
            return None, {}, EXCLUSION_INVALID_OUTPUT
        predictions = model.predict_proba(score_x)[
            :, int(class_positions[0])
        ]
    else:
        params = dict(config["lightgbm_ranker"]["parameters"])
        write_json(
            model_configuration_path,
            {
                "family": config["lightgbm_ranker"]["family"],
                "role": role,
                "fold_id": fold_id,
                "parameters": params,
                "parameter_source": "V3.1 frozen configuration",
            },
        )
        model = lgb.LGBMRegressor(**params)
        clip_low, clip_high = config["targets"]["regression_clip"]
        y = pd.to_numeric(
            train[target], errors="coerce"
        ).clip(clip_low, clip_high)
        model.fit(train_x, y)
        predictions = model.predict(score_x)

    predictions = np.asarray(predictions, dtype=float)
    if len(predictions) != len(score) or not np.isfinite(
        predictions
    ).all():
        return None, {}, EXCLUSION_INVALID_OUTPUT
    if role == "decision_tree" and (
        (predictions < 0) | (predictions > 1)
    ).any():
        return None, {}, EXCLUSION_INVALID_OUTPUT
    joblib.dump(model, model_path, compress=3)
    lineage = {
        "feature_artifact_id": _artifact_id(feature_path),
        "preprocessing_artifact_id": _artifact_id(
            preprocessing_path
        ),
        "target_artifact_id": _artifact_id(target_path),
        "model_configuration_artifact_id": _artifact_id(
            model_configuration_path
        ),
        "model_artifact_id": _artifact_id(model_path),
        "training_rows": len(train),
        "training_label_end_max": pd.Timestamp(
            train["label_end_date"].max()
        ).isoformat(),
        "selected_feature_count": len(features),
        "selected_features_json": json.dumps(
            features, separators=(",", ":")
        ),
        "training_population_fingerprint": dataframe_fingerprint(
            train[["stable_row_id", "label_end_date", target, *features]]
        ),
    }
    return predictions, lineage, None


def exclusion_report(predictions: pd.DataFrame) -> pd.DataFrame:
    return (
        predictions.loc[predictions["prediction_status"] != "oos_prediction_available"]
        .groupby(["model_role", "exclusion_code"], dropna=False)
        .size()
        .rename("rows")
        .reset_index()
        .sort_values(["model_role", "exclusion_code"])
        .reset_index(drop=True)
    )
