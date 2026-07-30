"""Session 4C filing-time cohort contract tests."""
from __future__ import annotations

import numpy as np
import pandas as pd

from alpha.factors import momentum
from pipeline.event_time_cohorts import (
    event_time_rank,
    proven_availability,
    winsorize_prior_market_history,
)
from pipeline.step5_compute_features import (
    add_interactions,
    add_momentum_ranks,
    add_sector_percentiles,
)
from pipeline.step6_clean import _impute_size_category, winsorize_accruals


def _rows(values, dates, *, year=2020, market="US", provenance="sec_primary_filing"):
    n = len(values)
    dates = pd.to_datetime(dates)
    return pd.DataFrame({
        "entity_id": [f"{market}:issuer-{i}" for i in range(n)],
        "cik": [f"issuer-{i}" for i in range(n)],
        "fiscal_year": [year] * n,
        "period_type": ["annual"] * n,
        "market": [market] * n,
        "filed_date": dates,
        "availability_timestamp": dates,
        "availability_provenance": [provenance] * n,
        "signal": np.asarray(values, dtype=float),
    })


def test_early_and_late_filers_use_exact_available_peers_and_future_is_invariant():
    early = _rows(range(10), ["2021-03-01"] * 10)
    late = _rows(range(10, 20), ["2021-06-01"] * 10)
    late["entity_id"] = [f"US:late-{i}" for i in range(10)]
    base = pd.concat([early, late], ignore_index=True)

    ranked = event_time_rank(base, "signal", group_cols=["fiscal_year", "market"], min_count=10)
    assert ranked.iloc[9] == 1.0  # 9 is ranked only against the ten March peers.
    assert ranked.iloc[10] == 11 / 20  # 10 sees March plus its equal-time June batch.

    future = _rows([10_000] * 10, ["2022-02-01"] * 10, year=2021)
    combined = pd.concat([base, future], ignore_index=True)
    reranked = event_time_rank(
        combined, "signal", group_cols=["fiscal_year", "market"], min_count=10
    )
    pd.testing.assert_series_equal(ranked, reranked.iloc[: len(base)], check_names=False)


def test_equal_timestamp_batches_and_row_order_are_invariant():
    df = _rows(np.arange(20), ["2021-03-01"] * 20)
    expected = event_time_rank(df, "signal", group_cols=["fiscal_year", "market"], min_count=10)

    shuffled = df.sample(frac=1, random_state=7)
    actual = event_time_rank(
        shuffled, "signal", group_cols=["fiscal_year", "market"], min_count=10
    ).reindex(df.index)
    pd.testing.assert_series_equal(expected, actual, check_names=False)


def test_sparse_history_never_bootstraps_current_batch_or_other_market():
    history = _rows(np.linspace(-1, 1, 49), ["2020-03-01"] * 49, year=2019)
    target = _rows([100.0], ["2021-03-01"])
    target["entity_id"] = "US:target"
    other_market = _rows([-10_000, 10_000] * 50, ["2020-03-01"] * 100, year=2019, market="JP")
    sparse = pd.concat([history, target, other_market], ignore_index=True)

    values, method = winsorize_prior_market_history(sparse, "signal")
    target_idx = len(history)
    assert values.iloc[target_idx] == 100.0
    assert method.iloc[target_idx] == "raw_sparse"

    one_more = _rows([0.0], ["2020-03-01"], year=2019)
    one_more["entity_id"] = "US:history-50"
    sufficient = pd.concat([history, one_more, target], ignore_index=True)
    clipped, method = winsorize_prior_market_history(sufficient, "signal")
    assert clipped.iloc[-1] < 100.0
    assert method.iloc[-1] == "prior_market_history"


def test_sector_minimum_has_no_market_fallback():
    df = _rows(np.arange(10), ["2021-03-01"] * 10)
    df["sic_code"] = [7372] * 4 + [2834] * 6
    df["pe_ratio"] = np.arange(10, dtype=float)
    result = add_sector_percentiles(df.copy())
    assert result.loc[:3, "pe_ratio_sector_pct"].isna().all()
    assert result.loc[4:, "pe_ratio_sector_pct"].notna().all()


def test_step6_accrual_and_size_are_future_and_order_invariant():
    df = _rows(np.linspace(-0.1, 0.1, 20), ["2021-03-01"] * 20)
    df["accruals_to_assets"] = df["signal"]
    df["log_assets"] = np.arange(20, dtype=float)
    df["size_category"] = 1.0
    df.loc[19, "size_category"] = np.nan

    accrual_base = winsorize_accruals(df.copy()).sort_index()
    size_base = _impute_size_category(df.copy()).sort_index()

    future = _rows([999.0] * 20, ["2021-06-01"] * 20)
    future["entity_id"] = [f"US:future-{i}" for i in range(20)]
    future["accruals_to_assets"] = 999.0
    future["log_assets"] = -999.0
    future["size_category"] = np.nan
    combined = pd.concat([df, future], ignore_index=True).sample(frac=1, random_state=9)

    accrual_combined = winsorize_accruals(combined.copy()).reindex(df.index)
    size_combined = _impute_size_category(combined.copy()).reindex(df.index)
    pd.testing.assert_series_equal(
        accrual_base["accruals_to_assets"],
        accrual_combined["accruals_to_assets"],
        check_names=False,
    )
    pd.testing.assert_series_equal(
        size_base["size_category"], size_combined["size_category"], check_names=False
    )
    assert size_base.loc[19, "size_category"] == 3.0


def test_cross_module_eligible_cohort_and_rank_interaction_agree():
    df = _rows(np.arange(20), ["2021-03-01"] * 10 + ["2021-06-01"] * 10)
    df["momentum_12m_prior"] = df["signal"]
    expected = event_time_rank(
        df, "momentum_12m_prior", group_cols=["fiscal_year", "market"], min_count=10
    )

    step5 = add_momentum_ranks(df.copy())
    pd.testing.assert_series_equal(step5["momentum_12m_rank"], expected, check_names=False)
    alpha_rank = momentum(df)
    pd.testing.assert_series_equal(alpha_rank, expected, check_names=False)
    assert alpha_rank.attrs["event_time_transform_contract"]["minimum_non_null"] == 10

    step5["value_composite"] = 1.0
    step5["quality_composite"] = 1.0
    interactions = add_interactions(step5)
    pd.testing.assert_series_equal(
        interactions["value_x_momentum"], expected, check_names=False
    )


def test_missing_estimated_and_legacy_dates_fail_closed():
    proven = _rows(np.arange(10), ["2021-03-01"] * 10)
    legacy = proven.drop(columns=["availability_provenance"])
    _, legacy_mask = proven_availability(legacy)
    assert not legacy_mask.any()

    estimated = proven.copy()
    estimated["availability_provenance"] = "estimated_filing_date"
    _, estimated_mask = proven_availability(estimated)
    assert not estimated_mask.any()
    assert event_time_rank(
        estimated, "signal", group_cols=["fiscal_year", "market"], min_count=10
    ).isna().all()


def test_sec_date_only_new_york_end_of_day_survives_utc_rollover():
    filing = _rows([1.0], ["2021-03-01"])
    filing["availability_timestamp"] = "2021-03-02T04:59:59.999999+00:00"

    timestamps, eligible = proven_availability(filing)

    assert str(timestamps.iloc[0]) == "2021-03-02 04:59:59.999999+00:00"
    assert eligible.tolist() == [True]


def test_sec_date_only_source_local_mismatch_still_fails_closed():
    filing = _rows([1.0], ["2021-03-01"])
    filing["availability_timestamp"] = "2021-03-03T04:59:59.999999+00:00"

    _, eligible = proven_availability(filing)

    assert eligible.tolist() == [False]


def test_new_york_date_exception_requires_sec_primary_provenance():
    invalid = _rows(
        [1.0],
        ["2021-03-01"],
        provenance="estimated_filing_date",
    )
    invalid["availability_timestamp"] = "2021-03-02T04:59:59.999999+00:00"
    _, invalid_mask = proven_availability(invalid)
    assert invalid_mask.tolist() == [False]

    other_proven_source = invalid.copy()
    other_proven_source["availability_provenance"] = "edinet_submission"
    _, other_mask = proven_availability(other_proven_source)
    assert other_mask.tolist() == [False]


def test_later_versions_and_unresolved_equal_time_collisions_fail_closed():
    versions = _rows([1.0, 2.0], ["2021-03-01", "2021-06-01"])
    versions["entity_id"] = "US:same-issuer"
    versions["cik"] = "same-issuer"
    _, version_mask = proven_availability(versions)
    assert version_mask.tolist() == [True, False]

    collision = versions.copy()
    collision["availability_timestamp"] = pd.Timestamp("2021-03-01")
    collision["filed_date"] = pd.Timestamp("2021-03-01")
    _, collision_mask = proven_availability(collision)
    assert not collision_mask.any()
