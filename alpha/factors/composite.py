"""
Composite alpha score: weighted blend of 5 factor scores.

Default weights are equal (0.20 each). Override via the `weights` argument.
Score is 0–1, higher = stronger composite alpha signal.
"""

from typing import Optional
import pandas as pd

from pipeline.event_time_cohorts import (
    CONTRACT_VERSION,
    PROVENANCE_POLICY,
    TRANSFORM_CONTRACTS,
)

from .value import compute as _value
from .quality import compute as _quality
from .momentum import compute as _momentum
from .growth import compute as _growth
from .fraud_risk import compute as _fraud_risk

DEFAULT_WEIGHTS = {
    "value":      0.20,
    "quality":    0.20,
    "momentum":   0.20,
    "growth":     0.20,
    "fraud_risk": 0.20,
}


def compute(
    df: pd.DataFrame,
    group_cols: tuple = ("fiscal_year", "market"),
    weights: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Compute all 5 factor scores plus the composite.

    Returns a DataFrame with columns:
      alpha_value, alpha_quality, alpha_momentum, alpha_growth, alpha_fraud_risk, alpha_composite
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS

    total = sum(weights.values())
    w = {k: v / total for k, v in weights.items()}

    scores = {
        "alpha_value":      _value(df, group_cols),
        "alpha_quality":    _quality(df, group_cols),
        "alpha_momentum":   _momentum(df, group_cols),
        "alpha_growth":     _growth(df, group_cols),
        "alpha_fraud_risk": _fraud_risk(df, group_cols),
    }

    result = pd.DataFrame(scores, index=df.index)

    composite = (
        result["alpha_value"]      * w["value"] +
        result["alpha_quality"]    * w["quality"] +
        result["alpha_momentum"]   * w["momentum"] +
        result["alpha_growth"]     * w["growth"] +
        result["alpha_fraud_risk"] * w["fraud_risk"]
    )
    result["alpha_composite"] = composite
    result.attrs["event_time_contract_version"] = CONTRACT_VERSION
    result.attrs["event_time_provenance_policy"] = PROVENANCE_POLICY
    result.attrs["event_time_transform_contract"] = TRANSFORM_CONTRACTS[
        "alpha_factor_ranks"
    ]

    return result
