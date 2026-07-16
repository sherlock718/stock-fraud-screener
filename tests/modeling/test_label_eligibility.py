"""Synthetic tests for horizon-specific label availability and policy modes."""

import pandas as pd
import pytest

from modeling.label_eligibility import (
    INCLUDE_POLICY_IMPUTED,
    training_label_eligible,
)


def _labels(horizon: str = "3y") -> pd.DataFrame:
    return pd.DataFrame({
        "row_id": ["before", "exact", "after", "missing", "policy", "unknown"],
        f"forward_return_{horizon}": [0.2, 0.2, 0.2, 0.2, -0.5, 0.2],
        f"beat_local_market_{horizon}": [1, 1, 1, 1, 0, 1],
        f"stock_label_end_date_{horizon}": [
            "2019-12-31", "2020-01-01", "2020-01-02", None, None, "2019-12-29",
        ],
        f"label_end_date_{horizon}": [
            "2019-12-31", "2020-01-01", "2020-01-02", None, None, "2019-12-29",
        ],
        f"policy_label_available_date_{horizon}": [
            None, None, None, None, "2019-12-30", None,
        ],
        f"policy_stock_label_available_date_{horizon}": [
            None, None, None, None, "2019-12-30", None,
        ],
        f"stock_label_provenance_{horizon}": [
            "observed_market_price", "observed_market_price",
            "observed_market_price", "observed_market_price",
            "policy_imputed_likely_delisted", "unknown",
        ],
        f"label_provenance_{horizon}": [
            "observed_stock_and_benchmark_prices",
            "observed_stock_and_benchmark_prices",
            "observed_stock_and_benchmark_prices",
            "observed_stock_and_benchmark_prices",
            "policy_imputed_likely_delisted", "unknown",
        ],
    })


@pytest.mark.parametrize("horizon", ["6m", "1y", "2y", "3y", "5y"])
def test_strict_boundaries_apply_to_every_trained_horizon(horizon):
    df = _labels(horizon)
    target = f"beat_local_market_{horizon}"
    mask = training_label_eligible(df, target, "2020-01-01")
    assert df.loc[mask, "row_id"].tolist() == ["before"]


def test_stock_target_uses_horizon_specific_stock_exit():
    df = _labels("1y")
    mask = training_label_eligible(df, "forward_return_1y", "2020-01-01")
    assert df.loc[mask, "row_id"].tolist() == ["before"]


def test_policy_label_is_excluded_by_default_and_explicitly_included():
    df = _labels("3y")
    primary = training_label_eligible(df, "beat_local_market_3y", "2020-01-01")
    sensitivity = training_label_eligible(
        df, "beat_local_market_3y", "2020-01-01", INCLUDE_POLICY_IMPUTED
    )
    assert df.loc[primary, "row_id"].tolist() == ["before"]
    assert df.loc[sensitivity, "row_id"].tolist() == ["before", "policy"]


def test_policy_availability_date_uses_same_strict_boundary():
    df = _labels("3y")
    df.loc[df["row_id"] == "policy", "policy_stock_label_available_date_3y"] = "2020-01-01"
    mask = training_label_eligible(
        df, "forward_return_3y", "2020-01-01", INCLUDE_POLICY_IMPUTED
    )
    assert "policy" not in df.loc[mask, "row_id"].tolist()


def test_legacy_dataset_without_dates_has_no_silent_fallback():
    legacy = pd.DataFrame({"forward_return_1y": [0.1], "fiscal_year": [2010]})
    assert not training_label_eligible(
        legacy, "forward_return_1y", "2020-01-01"
    ).any()


def test_fold_maximum_end_date_is_strictly_before_scoring_date():
    df = _labels("5y")
    cutoff = pd.Timestamp("2020-01-01")
    mask = training_label_eligible(df, "beat_local_market_5y", cutoff)
    assert pd.to_datetime(df.loc[mask, "label_end_date_5y"]).max() < cutoff


def test_adding_future_targets_cannot_change_historical_eligibility():
    base = _labels("2y").iloc[:2].copy()
    target = "beat_local_market_2y"
    before = training_label_eligible(base, target, "2020-01-01").to_list()
    extended = pd.concat([base, _labels("2y").iloc[[2]]], ignore_index=True)
    after = training_label_eligible(
        extended, target, "2020-01-01"
    ).iloc[:len(base)].to_list()
    assert after == before


def test_unknown_label_policy_is_rejected():
    with pytest.raises(ValueError, match="Unknown label_policy"):
        training_label_eligible(_labels(), "forward_return_3y", "2020-01-01", "mixed")
