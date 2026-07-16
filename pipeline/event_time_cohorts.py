"""Shared filing-time cohort transformations.

This module materializes features at each row's proven publication timestamp.
It deliberately does not define a portfolio decision or rebalance calendar.
"""
from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd


AVAILABILITY_TIMESTAMP = "availability_timestamp"
AVAILABILITY_PROVENANCE = "availability_provenance"
ENTITY_ID = "entity_id"
CONTRACT_VERSION = "filing_time_v1"
PROVENANCE_POLICY = "proven_publication_only_v1"

TRANSFORM_CONTRACTS = {
    "step5_history_winsorization": {
        "population": "prior_proven_same_market_annual",
        "grouping_keys": ["market"],
        "minimum_non_null": 50,
        "timestamp_relation": "strictly_before",
    },
    "market_ranks": {
        "population": "available_fiscal_year_market",
        "grouping_keys": ["fiscal_year", "market"],
        "minimum_non_null": 10,
        "timestamp_relation": "at_or_before_equal_time_batch",
    },
    "sector_ranks": {
        "population": "available_fiscal_year_market_sector",
        "grouping_keys": ["fiscal_year", "market", "sic_2digit"],
        "minimum_non_null": 5,
        "timestamp_relation": "at_or_before_equal_time_batch",
    },
    "step6_accruals": {
        "population": "available_fiscal_market_then_prior_market_annual",
        "grouping_keys": ["fiscal_year", "market"],
        "minimum_non_null": [20, 50],
        "timestamp_relation": ["at_or_before_equal_time_batch", "strictly_before"],
    },
    "step6_size_imputation": {
        "population": "available_fiscal_year_market_all_log_assets",
        "grouping_keys": ["fiscal_year", "market"],
        "minimum_non_null": 20,
        "timestamp_relation": "at_or_before_equal_time_batch",
    },
    "alpha_factor_ranks": {
        "population": "available_fiscal_year_market",
        "grouping_keys": ["fiscal_year", "market"],
        "minimum_non_null": 10,
        "timestamp_relation": "at_or_before_equal_time_batch",
    },
}

PROVEN_PROVENANCES = frozenset({
    "sec_primary_filing",
    "edinet_submission",
    "dart_receipt",
})

REQUIRED_COLUMNS = frozenset({
    ENTITY_ID,
    "fiscal_year",
    "period_type",
    AVAILABILITY_TIMESTAMP,
    AVAILABILITY_PROVENANCE,
    "market",
})


def proven_availability(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Return parsed timestamps and the strict row-level eligibility mask."""
    timestamps = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
    if AVAILABILITY_TIMESTAMP in df.columns:
        timestamps = pd.to_datetime(df[AVAILABILITY_TIMESTAMP], errors="coerce", utc=True)

    if not REQUIRED_COLUMNS.issubset(df.columns):
        return timestamps, pd.Series(False, index=df.index, dtype=bool)

    eligible = (
        timestamps.notna()
        & df[AVAILABILITY_PROVENANCE].isin(PROVEN_PROVENANCES)
        & df[ENTITY_ID].notna()
        & df["fiscal_year"].notna()
        & df["period_type"].notna()
        & df["market"].notna()
    )
    if "filed_date" in df.columns:
        filed = pd.to_datetime(df["filed_date"], errors="coerce", utc=True)
        eligible &= filed.notna() & timestamps.dt.normalize().eq(filed.dt.normalize())

    # The current source-vintage policy permits only one earliest-primary row
    # per entity-period. Later versions and unresolved equal-time collisions are
    # not assigned an accession precedence here.
    version_keys = [ENTITY_ID, "fiscal_year", "period_type"]
    if "fiscal_quarter" in df.columns:
        version_keys.append("fiscal_quarter")
    eligible_rows = df.loc[eligible, version_keys].copy()
    eligible_rows["__timestamp__"] = timestamps.loc[eligible]
    for _, versions in eligible_rows.groupby(version_keys, dropna=False, sort=False):
        if len(versions) <= 1:
            continue
        earliest = versions["__timestamp__"].min()
        later = versions.index[versions["__timestamp__"].gt(earliest)]
        eligible.loc[later] = False
        earliest_rows = versions.index[versions["__timestamp__"].eq(earliest)]
        if len(earliest_rows) > 1:
            eligible.loc[earliest_rows] = False
    return timestamps, eligible


def attach_contract_provenance(df: pd.DataFrame) -> pd.DataFrame:
    """Attach compact row-level materialization provenance to pipeline output."""
    timestamps, eligible = proven_availability(df)
    df = df.copy()
    df["event_time_materialization_timestamp"] = timestamps.where(eligible)
    df["event_time_contract"] = pd.Series(
        np.where(eligible, CONTRACT_VERSION, None), index=df.index, dtype="object"
    )
    df["event_time_provenance_policy"] = PROVENANCE_POLICY
    df.attrs["event_time_contract_version"] = CONTRACT_VERSION
    df.attrs["event_time_provenance_policy"] = PROVENANCE_POLICY
    df.attrs["event_time_transform_contracts"] = TRANSFORM_CONTRACTS
    return df


def attach_result_contract(result: pd.Series, transform: str) -> pd.Series:
    """Attach the static population/minimum policy to an in-memory result."""
    result.attrs["event_time_contract_version"] = CONTRACT_VERSION
    result.attrs["event_time_provenance_policy"] = PROVENANCE_POLICY
    result.attrs["event_time_transform_contract"] = TRANSFORM_CONTRACTS[transform]
    return result


def _group_positions(df: pd.DataFrame, group_cols: Sequence[str], eligible: pd.Series):
    work = df.loc[eligible, list(group_cols)].copy()
    work["__index__"] = work.index
    for _, group in work.groupby(list(group_cols), dropna=False, sort=False):
        yield group["__index__"].to_list()


def event_time_rank(
    df: pd.DataFrame,
    col: str,
    *,
    group_cols: Sequence[str],
    min_count: int,
    winsorize: bool = False,
    lower: float = 0.01,
    upper: float = 0.99,
) -> pd.Series:
    """Rank each equal-time batch against proven same-group rows available by T."""
    result = pd.Series(np.nan, index=df.index, dtype=float)
    if col not in df.columns or any(key not in df.columns for key in group_cols):
        return result

    timestamps, eligible = proven_availability(df)
    values = pd.to_numeric(df[col], errors="coerce")
    for positions in _group_positions(df, group_cols, eligible):
        group_times = timestamps.loc[positions]
        for timestamp in sorted(group_times.unique()):
            cohort_idx = group_times.index[group_times.le(timestamp)]
            cohort = values.loc[cohort_idx]
            if cohort.notna().sum() < min_count:
                continue
            if winsorize:
                cohort = cohort.clip(cohort.quantile(lower), cohort.quantile(upper))
            ranks = cohort.rank(pct=True, na_option="keep")
            batch_idx = group_times.index[group_times.eq(timestamp)]
            result.loc[batch_idx] = ranks.reindex(batch_idx)
    return result


def winsorize_prior_market_history(
    df: pd.DataFrame,
    col: str,
    *,
    min_count: int = 50,
    lower: float = 0.01,
    upper: float = 0.99,
) -> tuple[pd.Series, pd.Series]:
    """Clip from strictly prior proven same-market annual history.

    Sparse proven rows retain their raw value and report ``raw_sparse``. Rows
    without proven availability fail closed to null and report no method.
    """
    values = pd.to_numeric(df[col], errors="coerce")
    result = pd.Series(np.nan, index=df.index, dtype=float)
    method = pd.Series(pd.NA, index=df.index, dtype="string")
    timestamps, eligible = proven_availability(df)
    if "market" not in df.columns or "period_type" not in df.columns:
        return result, method

    for market in df.loc[eligible, "market"].drop_duplicates():
        targets = df.index[eligible & df["market"].eq(market)]
        history_base = eligible & df["market"].eq(market) & df["period_type"].eq("annual")
        for timestamp in sorted(timestamps.loc[targets].unique()):
            batch_idx = targets[timestamps.loc[targets].eq(timestamp)]
            history = values.loc[history_base & timestamps.lt(timestamp)].dropna()
            if len(history) >= min_count:
                result.loc[batch_idx] = values.loc[batch_idx].clip(
                    history.quantile(lower), history.quantile(upper)
                )
                method.loc[batch_idx] = "prior_market_history"
            else:
                result.loc[batch_idx] = values.loc[batch_idx]
                method.loc[batch_idx] = "raw_sparse"
    return result, method


def winsorize_accruals_event_time(
    df: pd.DataFrame,
    col: str = "accruals_to_assets",
    *,
    cohort_min_count: int = 20,
    history_min_count: int = 50,
    lower: float = 0.01,
    upper: float = 0.99,
) -> tuple[pd.Series, pd.Series]:
    """Apply the accepted cohort-first, prior-history-second accrual policy."""
    values = pd.to_numeric(df[col], errors="coerce")
    result = pd.Series(np.nan, index=df.index, dtype=float)
    method = pd.Series(pd.NA, index=df.index, dtype="string")
    timestamps, eligible = proven_availability(df)
    if "market" not in df.columns or "fiscal_year" not in df.columns:
        return result, method

    keys = ["fiscal_year", "market"]
    for positions in _group_positions(df, keys, eligible):
        group_times = timestamps.loc[positions]
        market = df.loc[positions[0], "market"]
        for timestamp in sorted(group_times.unique()):
            batch_idx = group_times.index[group_times.eq(timestamp)]
            cohort_idx = group_times.index[group_times.le(timestamp)]
            cohort = values.loc[cohort_idx].dropna()
            if len(cohort) >= cohort_min_count:
                bounds = (cohort.quantile(lower), cohort.quantile(upper))
                method.loc[batch_idx] = "eligible_fiscal_market_cohort"
            else:
                history_mask = (
                    eligible
                    & df["market"].eq(market)
                    & df["period_type"].eq("annual")
                    & timestamps.lt(timestamp)
                )
                history = values.loc[history_mask].dropna()
                if len(history) >= history_min_count:
                    bounds = (history.quantile(lower), history.quantile(upper))
                    method.loc[batch_idx] = "prior_market_history"
                else:
                    result.loc[batch_idx] = values.loc[batch_idx]
                    method.loc[batch_idx] = "raw_sparse"
                    continue
            result.loc[batch_idx] = values.loc[batch_idx].clip(*bounds)
    return result, method
