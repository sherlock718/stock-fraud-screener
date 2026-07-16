"""Fail-closed historical prediction consumption.

Historical strategies consume wide score columns, but every score must be backed
by row-level manifest columns named ``<score>__<field>``.  This module is the
single enforcement point for the Session 5A prediction-lineage and missingness
contracts.  It never manufactures a score or falls back to a final model.
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


ALLOWED_OOS_SOURCES = frozenset({"walk_forward_oos", "oof_oos"})

ROW_IDENTITY_FIELDS = (
    "entity_id",
    "ticker",
    "market",
    "fiscal_year",
    "period_type",
    "availability_timestamp",
    "availability_provenance",
    "decision_timestamp",
)

PREDICTION_KEY_FIELDS = (
    "entity_id",
    "market",
    "fiscal_year",
    "period_type",
    "availability_timestamp",
    "decision_timestamp",
)

# Horizon/model role/label policy are requirement or manifest fields.  The
# remaining fields are the immutable lineage needed to prove the score.
MANIFEST_FIELDS = (
    "score_source",
    "model_role",
    "horizon",
    "label_policy",
    "scoring_calendar_config_id",
    "eligible_fiscal_cohort",
    "score_units",
    "raw_prediction",
    "transformed_score",
    "non_null_status",
    "fold_id",
    "training_start",
    "training_cutoff",
    "max_eligible_publication_timestamp",
    "max_eligible_label_date",
    "dataset_id",
    "dataset_hash",
    "training_population_hash",
    "training_population_count",
    "target",
    "selector_config_hash",
    "selector_hash",
    "feature_list_hash",
    "feature_materialization_version",
    "preprocessing_artifact_id",
    "preprocessing_artifact_hash",
    "model_family_config",
    "model_artifact_id",
    "model_artifact_hash",
    "random_seed",
    "code_commit",
    "dirty_state_hash",
    "decision_timestamp",
)

SCORE_ELIGIBLE_COL = "historical_score_eligible"
SCORE_SOURCE_COL = "historical_score_source"
SCORE_EXCLUSION_COL = "historical_score_exclusion_reason"


@dataclass(frozen=True)
class ScoreRequirement:
    """One ML role that every selected historical row must satisfy."""

    score_col: str
    model_role: str
    horizon: str
    label_policy: str = "observed_only"


def manifest_col(score_col: str, field: str) -> str:
    return f"{score_col}__{field}"


def add_synthetic_manifest(
    df: pd.DataFrame,
    requirements: Iterable[ScoreRequirement],
    *,
    source: str = "walk_forward_oos",
) -> pd.DataFrame:
    """Attach a complete deterministic manifest for synthetic tests only."""
    out = df.copy()
    if "entity_id" not in out:
        out["entity_id"] = [f"synthetic:{i}" for i in range(len(out))]
    if "ticker" not in out:
        out["ticker"] = [f"SYN{i}" for i in range(len(out))]
    if "period_type" not in out:
        out["period_type"] = "annual"
    if "availability_timestamp" not in out:
        out["availability_timestamp"] = pd.to_datetime(
            out["fiscal_year"].astype(int).astype(str) + "-03-01"
        )
    if "availability_provenance" not in out:
        out["availability_provenance"] = "synthetic_primary_filing"
    if "decision_timestamp" not in out:
        out["decision_timestamp"] = pd.to_datetime(
            (out["fiscal_year"].astype(int) + 1).astype(str) + "-01-01"
        )

    decision = pd.to_datetime(out["decision_timestamp"])
    manifest_data: dict[str, object] = {}
    for req in requirements:
        values = {
            "score_source": source,
            "model_role": req.model_role,
            "horizon": req.horizon,
            "label_policy": req.label_policy,
            "scoring_calendar_config_id": "synthetic-calendar",
            "eligible_fiscal_cohort": "synthetic-cohort",
            "score_units": "probability_or_return",
            "raw_prediction": out[req.score_col],
            "transformed_score": out[req.score_col],
            "non_null_status": out[req.score_col].notna(),
            "fold_id": "synthetic-fold",
            "training_start": decision - pd.DateOffset(years=6),
            "training_cutoff": decision - pd.DateOffset(days=1),
            "max_eligible_publication_timestamp": decision - pd.DateOffset(days=2),
            "max_eligible_label_date": decision - pd.DateOffset(days=2),
            "dataset_id": "synthetic-dataset",
            "dataset_hash": "synthetic-dataset-hash",
            "training_population_hash": "synthetic-population-hash",
            "training_population_count": 100,
            "target": f"synthetic_target_{req.horizon}",
            "selector_config_hash": "synthetic-selector-config-hash",
            "selector_hash": "synthetic-selector-hash",
            "feature_list_hash": "synthetic-feature-hash",
            "feature_materialization_version": "synthetic-v1",
            "preprocessing_artifact_id": "synthetic-preprocess",
            "preprocessing_artifact_hash": "synthetic-preprocess-hash",
            "model_family_config": "synthetic-model-family",
            "model_artifact_id": "synthetic-model",
            "model_artifact_hash": "synthetic-model-hash",
            "random_seed": 42,
            "code_commit": "synthetic-commit",
            "dirty_state_hash": "synthetic-dirty-hash",
            "decision_timestamp": decision,
        }
        for field, value in values.items():
            manifest_data[manifest_col(req.score_col, field)] = value
    manifest = pd.DataFrame(manifest_data, index=out.index)
    return pd.concat([out, manifest], axis=1).copy()


def requirements_for_signals(signal_cols: Iterable[str]) -> tuple[ScoreRequirement, ...]:
    """Expand direct and alpha-factor ML dependencies without duplicates."""
    requested = set(signal_cols)
    horizons: set[str] = set()
    if requested & {"alpha_fraud_risk", "alpha_composite"}:
        horizons.update({"1y", "3y", "5y"})
    for horizon in ("1y", "3y", "5y"):
        if f"ml_{horizon}_oof" in requested:
            horizons.add(horizon)
    return tuple(
        ScoreRequirement(f"ml_{h}_oof", "oof_factor_input", h)
        for h in ("1y", "3y", "5y")
        if h in horizons
    )


def _blank(series: pd.Series) -> pd.Series:
    return series.isna() | series.astype(str).str.strip().eq("")


def validate_historical_scores(
    df: pd.DataFrame,
    requirements: Iterable[ScoreRequirement],
    *,
    annotate: bool = True,
) -> pd.Series:
    """Return rows with complete compatible OOS manifests and record reasons.

    Exclusion precedence is deterministic and uses the accepted Session 5A
    codes.  Multiple required roles are joined with ``;`` in the row reason.
    """
    requirements = tuple(requirements)
    eligible = pd.Series(True, index=df.index, dtype=bool)
    reasons: dict[object, list[str]] = {idx: [] for idx in df.index}
    sources: dict[object, list[str]] = {idx: [] for idx in df.index}
    role_coverage: dict[str, dict] = {}

    missing_identity = pd.Series(False, index=df.index)
    for field in ROW_IDENTITY_FIELDS:
        if field not in df.columns:
            missing_identity[:] = True
        else:
            missing_identity |= _blank(df[field])

    duplicate_key = pd.Series(False, index=df.index)
    if not missing_identity.all() and all(
        field in df.columns for field in PREDICTION_KEY_FIELDS
    ):
        duplicate_key = df.duplicated(list(PREDICTION_KEY_FIELDS), keep=False)

    for req in requirements:
        role_key = f"{req.model_role}:{req.horizon}"
        role_reason = pd.Series("", index=df.index, dtype=object)
        if req.score_col not in df.columns:
            role_reason[:] = "missing_prediction"
        else:
            role_reason.loc[df[req.score_col].isna()] = "missing_prediction"

        manifest_columns = [manifest_col(req.score_col, f) for f in MANIFEST_FIELDS]
        absent_manifest = [c for c in manifest_columns if c not in df.columns]
        undecided = role_reason.eq("")
        if absent_manifest:
            role_reason.loc[undecided] = "missing_prediction_manifest"
        else:
            model_cols = [
                manifest_col(req.score_col, field)
                for field in ("model_artifact_id", "model_artifact_hash")
            ]
            feature_cols = [
                manifest_col(req.score_col, field)
                for field in (
                    "selector_config_hash",
                    "selector_hash",
                    "feature_list_hash",
                    "feature_materialization_version",
                    "preprocessing_artifact_id",
                    "preprocessing_artifact_hash",
                )
            ]
            model_blank = pd.concat([_blank(df[c]) for c in model_cols], axis=1).any(axis=1)
            feature_blank = pd.concat([_blank(df[c]) for c in feature_cols], axis=1).any(axis=1)
            other_cols = [c for c in manifest_columns if c not in model_cols + feature_cols]
            other_blank = pd.concat([_blank(df[c]) for c in other_cols], axis=1).any(axis=1)
            role_reason.loc[undecided & model_blank] = "missing_model_artifact"
            role_reason.loc[role_reason.eq("") & feature_blank] = (
                "missing_feature_or_preprocessing_artifact"
            )
            role_reason.loc[role_reason.eq("") & other_blank] = "missing_prediction_manifest"

            undecided = role_reason.eq("")
            source_col = manifest_col(req.score_col, "score_source")
            source_ok = df[source_col].isin(ALLOWED_OOS_SOURCES)
            role_reason.loc[undecided & ~source_ok] = "non_oos_score_source"

            undecided = role_reason.eq("")
            role_ok = df[manifest_col(req.score_col, "model_role")].eq(req.model_role)
            horizon_ok = df[manifest_col(req.score_col, "horizon")].eq(req.horizon)
            policy_ok = df[manifest_col(req.score_col, "label_policy")].eq(req.label_policy)
            role_reason.loc[undecided & ~policy_ok] = "label_policy_mismatch"
            role_reason.loc[role_reason.eq("") & ~(role_ok & horizon_ok)] = "incompatible_lineage"

            score = pd.to_numeric(df[req.score_col], errors="coerce")
            raw = pd.to_numeric(
                df[manifest_col(req.score_col, "raw_prediction")], errors="coerce"
            )
            transformed = pd.to_numeric(
                df[manifest_col(req.score_col, "transformed_score")], errors="coerce"
            )
            declared_non_null = df[
                manifest_col(req.score_col, "non_null_status")
            ].eq(True)
            score_matches = pd.Series(
                np.isclose(score, raw, equal_nan=False)
                | np.isclose(score, transformed, equal_nan=False),
                index=df.index,
            )
            role_reason.loc[
                role_reason.eq("") & ~(declared_non_null & score_matches)
            ] = "incompatible_lineage"

            undecided = role_reason.eq("")
            row_decision = pd.to_datetime(df["decision_timestamp"], errors="coerce")
            manifest_decision = pd.to_datetime(
                df[manifest_col(req.score_col, "decision_timestamp")], errors="coerce"
            )
            cutoff = pd.to_datetime(
                df[manifest_col(req.score_col, "training_cutoff")], errors="coerce"
            )
            training_start = pd.to_datetime(
                df[manifest_col(req.score_col, "training_start")], errors="coerce"
            )
            max_publication = pd.to_datetime(
                df[manifest_col(req.score_col, "max_eligible_publication_timestamp")],
                errors="coerce",
            )
            max_label = pd.to_datetime(
                df[manifest_col(req.score_col, "max_eligible_label_date")], errors="coerce"
            )
            availability = pd.to_datetime(df["availability_timestamp"], errors="coerce")
            decision_ok = (
                manifest_decision.eq(row_decision)
                & availability.le(row_decision)
                & training_start.le(cutoff)
                & cutoff.lt(row_decision)
                & max_publication.lt(row_decision)
                & max_label.lt(row_decision)
            )
            role_reason.loc[undecided & ~decision_ok] = "decision_timestamp_mismatch"

        role_reason.loc[role_reason.eq("") & missing_identity] = "unproven_security_identity"
        role_reason.loc[role_reason.eq("") & duplicate_key] = "duplicate_prediction_key"

        bad = role_reason.ne("")
        eligible &= ~bad
        role_coverage[role_key] = {
            "score_col": req.score_col,
            "valid_count": int((~bad).sum()),
            "exclusions_by_code": {
                str(code): int(count)
                for code, count in role_reason[bad].value_counts().items()
            },
        }
        for idx in df.index[bad]:
            reasons[idx].append(f"{role_key}:{role_reason.at[idx]}")
        source_col = manifest_col(req.score_col, "score_source")
        if source_col in df.columns:
            for idx in df.index[~bad]:
                sources[idx].append(f"{role_key}:{df.at[idx, source_col]}")

    if annotate:
        annotations = pd.DataFrame(
            {
                SCORE_ELIGIBLE_COL: eligible,
                SCORE_SOURCE_COL: [";".join(sources[idx]) for idx in df.index],
                SCORE_EXCLUSION_COL: [";".join(reasons[idx]) for idx in df.index],
            },
            index=df.index,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pd.errors.PerformanceWarning)
            warnings.simplefilter("ignore", pd.errors.SettingWithCopyWarning)
            df[[SCORE_ELIGIBLE_COL, SCORE_SOURCE_COL, SCORE_EXCLUSION_COL]] = annotations
        df.attrs["historical_score_role_coverage"] = role_coverage
    return eligible


def complete_top_n(
    source_df: pd.DataFrame,
    candidates: pd.DataFrame,
    *,
    score_col: str,
    target_n: int,
) -> pd.Index:
    """Select exactly target_n non-null scores or fail the whole period closed."""
    valid = (
        candidates[candidates[score_col].notna()]
        if score_col in candidates
        else candidates.iloc[0:0]
    )
    selected = (
        valid.nlargest(target_n, score_col).index
        if len(valid) >= target_n
        else pd.Index([])
    )
    source_df.attrs["historical_score_coverage"] = {
        "candidate_count": int(len(source_df)),
        "gate_pass_count": int(len(candidates)),
        "valid_score_count": int(len(valid)),
        "valid_score_count_by_required_role": source_df.attrs.get(
            "historical_score_role_coverage", {}
        ),
        "target_n": int(target_n),
        "selected_count": int(len(selected)),
        "period_exclusion_reason": (
            None if len(selected) == target_n else "insufficient_valid_score_coverage"
        ),
        "row_exclusions": [
            {
                "index": str(idx),
                "entity_id": (
                    str(source_df.at[idx, "entity_id"])
                    if "entity_id" in source_df
                    else None
                ),
                "ticker": (
                    str(source_df.at[idx, "ticker"])
                    if "ticker" in source_df
                    else None
                ),
                "source": str(source_df.at[idx, SCORE_SOURCE_COL]),
                "reason": str(source_df.at[idx, SCORE_EXCLUSION_COL]),
            }
            for idx in source_df.index
            if SCORE_EXCLUSION_COL in source_df and source_df.at[idx, SCORE_EXCLUSION_COL]
        ],
    }
    return selected
