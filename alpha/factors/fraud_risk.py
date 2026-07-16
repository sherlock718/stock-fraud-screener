"""
Fraud Risk factor: cross-sectional rank composite of fraud and distress signals.

HIGH score = LOWER fraud risk = SAFER company. All signals are inverted so that
a high fraud_risk score means you can trust the company more.

Signals used:
  - beneish_m_score     (high = manipulator → invert)
  - ohlson_prob_bankruptcy  (high = distress → invert)
  - altman_z_score      (high = healthy → no invert)
  - fraud_score_composite   (high = risky → invert)
  - ml_1y_oof, ml_3y_oof, ml_5y_oof (unbiased OOF; high = likely to beat market → no invert)
                             Note: ML scores proxy fundamental quality, not fraud
"""

import numpy as np
import pandas as pd

from modeling.prediction_lineage import (
    ScoreRequirement,
    validate_historical_scores,
)
from pipeline.event_time_cohorts import attach_result_contract, event_time_rank

_SIGNALS = [
    ("beneish_m_score",         True),
    ("ohlson_prob_bankruptcy",  True),
    ("altman_z_score",          False),
    ("fraud_score_composite",   True),
    ("fraud_score_accounting",  True),
    ("fraud_score_distress",    True),
]

_ML_SIGNALS = ["ml_1y_oof", "ml_3y_oof", "ml_5y_oof"]
_ML_REQUIREMENTS = tuple(
    ScoreRequirement(col, "oof_factor_input", horizon)
    for col, horizon in zip(_ML_SIGNALS, ("1y", "3y", "5y"))
)


def _winsorize(s: pd.Series, q: float = 0.01) -> pd.Series:
    lo, hi = s.quantile(q), s.quantile(1 - q)
    return s.clip(lo, hi)


def _cross_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True, na_option="keep")


def compute(df: pd.DataFrame, group_cols: tuple = ("fiscal_year", "market")) -> pd.Series:
    ml_eligible = validate_historical_scores(df, _ML_REQUIREMENTS)
    ranks = []
    for col, invert in _SIGNALS:
        if col not in df.columns:
            continue
        r = event_time_rank(df, col, group_cols=group_cols, min_count=10)
        if invert:
            r = 1.0 - r
        ranks.append(r)

    for col in _ML_SIGNALS:
        if col not in df.columns:
            continue
        r = event_time_rank(df, col, group_cols=group_cols, min_count=10)
        ranks.append(r)

    if not ranks:
        return attach_result_contract(
            pd.Series(np.nan, index=df.index), "alpha_factor_ranks"
        )

    result = pd.concat(ranks, axis=1).mean(axis=1)
    result.loc[~ml_eligible] = np.nan
    return attach_result_contract(result, "alpha_factor_ranks")
