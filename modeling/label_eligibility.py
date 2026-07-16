"""Point-in-time eligibility rules for forward-return training labels."""

from __future__ import annotations

import pandas as pd


OBSERVED_STOCK_PROVENANCE = "observed_market_price"
OBSERVED_RELATIVE_PROVENANCE = "observed_stock_and_benchmark_prices"
POLICY_IMPUTED_PROVENANCE = "policy_imputed_likely_delisted"

OBSERVED_ONLY = "observed_only"
INCLUDE_POLICY_IMPUTED = "include_policy_imputed"
LABEL_POLICIES = (OBSERVED_ONLY, INCLUDE_POLICY_IMPUTED)

STOCK_TARGET_PREFIX = "forward_return_"
RELATIVE_TARGET_PREFIXES = ("beat_local_market_", "excess_return_local_")


def _target_contract(target_col: str) -> tuple[str, str, str, str, str] | None:
    """Return observed/policy date, provenance, and horizon fields."""
    if target_col.startswith(STOCK_TARGET_PREFIX):
        horizon = target_col.removeprefix(STOCK_TARGET_PREFIX)
        return (
            f"stock_label_end_date_{horizon}",
            f"stock_label_provenance_{horizon}",
            OBSERVED_STOCK_PROVENANCE,
            horizon,
            f"policy_stock_label_available_date_{horizon}",
        )
    for prefix in RELATIVE_TARGET_PREFIXES:
        if target_col.startswith(prefix):
            horizon = target_col.removeprefix(prefix)
            return (
                f"label_end_date_{horizon}",
                f"label_provenance_{horizon}",
                OBSERVED_RELATIVE_PROVENANCE,
                horizon,
                f"policy_label_available_date_{horizon}",
            )
    return None


def training_label_eligible(
    df: pd.DataFrame,
    target_col: str,
    scoring_date: pd.Timestamp | str,
    label_policy: str = OBSERVED_ONLY,
) -> pd.Series:
    """Return rows whose complete target was observed before ``scoring_date``.

    Missing columns, dates, and unknown provenance are deliberately ineligible;
    legacy datasets receive no inferred fiscal-year fallback. Policy-imputed
    labels are excluded by default and require the explicit sensitivity mode.
    """
    if label_policy not in LABEL_POLICIES:
        raise ValueError(f"Unknown label_policy={label_policy!r}; expected one of {LABEL_POLICIES}")

    contract = _target_contract(target_col)
    if contract is None:
        return pd.Series(True, index=df.index, dtype=bool)
    end_col, provenance_col, expected_provenance, horizon, policy_date_col = contract

    required = {target_col, end_col, provenance_col}
    if not required.issubset(df.columns):
        return pd.Series(False, index=df.index, dtype=bool)

    end_dates = pd.to_datetime(df[end_col], errors="coerce")
    cutoff = pd.Timestamp(scoring_date)
    observed = (
        df[target_col].notna()
        & end_dates.notna()
        & end_dates.lt(cutoff)
        & df[provenance_col].eq(expected_provenance)
    )
    if label_policy == OBSERVED_ONLY:
        return observed

    if policy_date_col not in df.columns:
        return observed
    policy_dates = pd.to_datetime(df[policy_date_col], errors="coerce")
    policy = (
        df[target_col].notna()
        & policy_dates.notna()
        & policy_dates.lt(cutoff)
        & df[provenance_col].eq(POLICY_IMPUTED_PROVENANCE)
    )
    return observed | policy
