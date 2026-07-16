"""Synthetic Session 6A invariance and fail-closed consumption tests."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from alpha.factors.composite import compute as alpha_composite
from alpha.factors.fraud_risk import _ML_REQUIREMENTS
from backtest.engine import (
    ENGINE_REQUIREMENTS,
    filter_composite,
    filter_iarb,
    filter_qem,
    filter_scdv,
    load_and_score,
    run_backtest,
)
from modeling.prediction_lineage import (
    SCORE_EXCLUSION_COL,
    ScoreRequirement,
    add_synthetic_manifest,
    manifest_col,
    validate_historical_scores,
)
from portfolio.build_alpha_registry import _make_filter
from portfolio.build_portfolio import compute_composite as registry_composite
from portfolio.build_screener_registry import filter_composite_us


def _engine_rows(n: int = 12, market: str = "US") -> pd.DataFrame:
    x = np.linspace(0.1, 0.9, n)
    df = pd.DataFrame({
        "entity_id": [f"{market}:{i}" for i in range(n)],
        "fiscal_year": [2020] * n,
        "market": [market] * n,
        "period_type": ["annual"] * n,
        "availability_timestamp": pd.to_datetime(["2021-03-01"] * n),
        "decision_timestamp": pd.to_datetime(["2022-01-01"] * n),
        "ticker": [f"T{i}" for i in range(n)],
        "piotroski_f_score": [8.0] * n,
        "piotroski_roa_pos": [1.0] * n,
        "ps_ratio_sector_pct": [0.3] * n,
        "tree_prob": [0.8] * n,
        "reg_3y_wf": x,
        "ml_1y_wf": x,
        "ml_3y_wf": x[::-1],
        "ml_1y": np.linspace(100, 200, n),
        "ml_3y": np.linspace(200, 300, n),
        "value_composite": x,
        "quality_composite": x,
        "eps_growth_yoy": [0.2] * n,
        "momentum_12m_prior": [0.2] * n,
        "earnings_yield": [0.1] * n,
        "size_category_label": ["small"] * n,
        "pb_ratio": [1.0] * n,
        "debt_to_equity": [0.2] * n,
        "altman_z_score": [3.0] * n,
        "beneish_m_score": [-3.0] * n,
    })
    requirements = (
        *ENGINE_REQUIREMENTS["ml_gates"],
        *ENGINE_REQUIREMENTS["composite"],
    )
    return add_synthetic_manifest(df, requirements)


def test_manifest_validator_records_source_and_row_exclusion_reason():
    req = ScoreRequirement("ml_1y_wf", "classifier_ranker", "1y")
    df = _engine_rows(3)
    df.loc[0, "ml_1y_wf"] = np.nan
    df.loc[1, manifest_col("ml_1y_wf", "score_source")] = "final"

    eligible = validate_historical_scores(df, [req])

    assert eligible.tolist() == [False, False, True]
    assert "missing_prediction" in df.loc[0, SCORE_EXCLUSION_COL]
    assert "non_oos_score_source" in df.loc[1, SCORE_EXCLUSION_COL]
    assert "walk_forward_oos" in df.loc[2, "historical_score_source"]


def test_manifest_validator_uses_specific_artifact_and_duplicate_codes():
    req = ScoreRequirement("ml_1y_wf", "classifier_ranker", "1y")
    df = _engine_rows(3)
    df.loc[0, manifest_col("ml_1y_wf", "model_artifact_hash")] = ""
    df.loc[1, manifest_col("ml_1y_wf", "preprocessing_artifact_hash")] = ""
    df.loc[2, "entity_id"] = df.loc[1, "entity_id"]
    df.loc[2, "ticker"] = df.loc[1, "ticker"]

    validate_historical_scores(df, [req])

    assert "missing_model_artifact" in df.loc[0, SCORE_EXCLUSION_COL]
    assert "missing_feature_or_preprocessing_artifact" in df.loc[1, SCORE_EXCLUSION_COL]
    assert "duplicate_prediction_key" in df.loc[2, SCORE_EXCLUSION_COL]


@pytest.mark.parametrize("valid_count", range(6))
def test_ml_gates_zero_to_five_valid_scores_never_activate_fallback(valid_count):
    df = _engine_rows(10)
    df.loc[df.index[valid_count:], "reg_3y_wf"] = np.nan

    selected = filter_composite(df, top_n=6, market=None, mode="ml_gates")

    assert selected.empty
    assert df.attrs["historical_score_coverage"]["valid_score_count"] == valid_count
    assert df.attrs["historical_score_coverage"]["period_exclusion_reason"] == "insufficient_valid_score_coverage"


def test_top_n_greater_than_non_null_scores_fails_closed():
    df = _engine_rows(8)
    df.loc[6:, "ml_1y_wf"] = np.nan
    selected = filter_qem(df, top_n=7, market=None)
    assert selected.empty
    assert df.attrs["historical_score_coverage"]["selected_count"] == 0


def test_five_row_qem_uses_required_ml_role_instead_of_dropping_its_weight():
    df = _engine_rows(5)
    df["eps_growth_yoy"] = 1.0
    df["quality_composite"] = 1.0
    df["momentum_12m_prior"] = 1.0
    df["value_composite"] = 1.0
    selected = filter_qem(df, top_n=3, market=None)
    assert selected.tolist() == [4, 3, 2]


def test_backtest_persists_period_and_row_score_coverage():
    df = _engine_rows(10)
    df["forward_return_1y"] = np.linspace(-0.1, 0.3, len(df))
    df["market_cap_at_filing"] = 1_000_000_000
    df.loc[5:, "reg_3y_wf"] = np.nan
    result = run_backtest(
        df,
        lambda rows, top_n, market: filter_composite(
            rows, top_n, market, mode="ml_gates"
        ),
        "synthetic",
        top_n=6,
        market=None,
        cost_bps=30,
        smallcap_cost_bps=60,
        use_adtv_filter=False,
    )
    assert result["n_years"] == 0
    assert result["error"] == "incomplete official score coverage"
    assert result["official_performance_available"] is False
    coverage = result["score_coverage"][0]
    assert coverage["period_exclusion_reason"] == "insufficient_valid_score_coverage"
    assert coverage["selected_count"] == 0
    assert len(coverage["row_exclusions"]) == 5
    assert coverage["valid_score_count_by_required_role"]["regression_ranker:3y"]["valid_count"] == 5


def test_final_model_columns_cannot_change_ml_gates_history():
    df = _engine_rows()
    expected = filter_composite(df.copy(), 5, None, mode="ml_gates")
    changed = df.copy()
    changed["ml_3y"] = changed["ml_3y"] * -10_000
    changed["ml_pred_excess_3y"] = np.arange(len(changed))[::-1]
    actual = filter_composite(changed, 5, None, mode="ml_gates")
    assert actual.equals(expected)


def test_later_same_year_row_cannot_impute_or_change_existing_oos_scores():
    base = _engine_rows(6)
    scored = load_and_score(base)
    future = base.iloc[[0]].copy()
    future.index = [99]
    future["entity_id"] = "US:future"
    future["availability_timestamp"] = pd.Timestamp("2021-12-01")
    future["ml_1y_wf"] = np.nan
    future["ml_1y"] = 999.0
    extended = load_and_score(pd.concat([base, future]))

    pd.testing.assert_series_equal(
        scored["ml_1y_wf"], extended.loc[base.index, "ml_1y_wf"]
    )
    assert pd.isna(extended.loc[99, "ml_1y_wf"])
    assert "ml_1y" not in extended


@pytest.mark.parametrize(
    ("strategy", "market", "required_col"),
    [
        (lambda df: filter_composite(df, 5, None, mode="blended"), "US", "ml_1y_wf"),
        (lambda df: filter_qem(df, 5, None), "US", "ml_1y_wf"),
        (lambda df: filter_scdv(df, 5, None), "US", "ml_3y_wf"),
        (lambda df: filter_iarb(df, 5, None), "JP", "ml_3y_wf"),
    ],
)
def test_engine_strategy_paths_require_complete_declared_scores(
    strategy, market, required_col
):
    df = _engine_rows(10, market=market)
    assert len(strategy(df.copy())) == 5
    df.loc[:5, required_col] = np.nan
    assert strategy(df).empty


def _alpha_rows(n: int = 12) -> pd.DataFrame:
    x = np.linspace(0.1, 0.9, n)
    df = pd.DataFrame({
        "entity_id": [f"US:a{i}" for i in range(n)],
        "fiscal_year": [2020] * n,
        "market": ["US"] * n,
        "period_type": ["annual"] * n,
        "availability_timestamp": pd.to_datetime(["2021-03-01"] * n),
        "availability_provenance": ["sec_primary_filing"] * n,
        "filed_date": pd.to_datetime(["2021-03-01"] * n),
        "decision_timestamp": pd.to_datetime(["2022-01-01"] * n),
        "ev_ebitda": x * 10,
        "roe": x,
        "momentum_12m_prior": x,
        "revenue_growth_yoy": x,
        "beneish_m_score": -x * 3,
        "ohlson_prob_bankruptcy": x,
        "altman_z_score": x * 5,
        "fraud_score_composite": x,
        "fraud_score_accounting": x,
        "fraud_score_distress": x,
        "ml_1y_oof": x,
        "ml_3y_oof": x,
        "ml_5y_oof": x,
    })
    return add_synthetic_manifest(df, _ML_REQUIREMENTS, source="oof_oos")


def test_alpha_composite_is_future_and_final_model_invariant():
    base = _alpha_rows()
    expected = alpha_composite(base.copy())["alpha_composite"]
    later = base.iloc[[0]].copy()
    later.index = [99]
    later["entity_id"] = "US:later"
    later["availability_timestamp"] = pd.Timestamp("2021-09-01")
    later["filed_date"] = pd.Timestamp("2021-09-01")
    extended = pd.concat([base, later])
    extended["ml_3y"] = np.arange(len(extended)) * 1000
    actual = alpha_composite(extended)["alpha_composite"]
    pd.testing.assert_series_equal(expected, actual.loc[base.index])


def test_alpha_composite_requires_every_declared_oof_family():
    df = _alpha_rows()
    df.loc[0, "ml_5y_oof"] = np.nan
    result = alpha_composite(df)
    assert pd.isna(result.loc[0, "alpha_fraud_risk"])
    assert pd.isna(result.loc[0, "alpha_composite"])
    assert "missing_prediction" in df.loc[0, SCORE_EXCLUSION_COL]


def test_screener_and_alpha_registry_paths_require_full_target_coverage():
    df = _alpha_rows(20)
    df["ticker"] = [f"R{i}" for i in range(len(df))]
    df["piotroski_f_score"] = 8
    df["beneish_m_score"] = -3.0
    df["alpha_composite"] = np.linspace(0, 1, len(df))
    df["value_composite"] = np.linspace(0, 1, len(df))
    df["quality_composite"] = np.linspace(0, 1, len(df))
    assert len(filter_composite_us(df.copy(), 10, None)) == 10

    df.loc[:10, "ml_3y_oof"] = np.nan
    assert filter_composite_us(df.copy(), 10, None).empty

    direct = _make_filter("ml_1y_oof")
    assert len(direct(_alpha_rows(20), 10, None)) == 10


def test_portfolio_registry_path_does_not_renormalize_missing_ml_role():
    df = _alpha_rows()
    df["alpha_composite"] = np.linspace(0, 1, len(df))
    df.loc[0, "ml_1y_oof"] = np.nan
    result = registry_composite(
        df,
        ["alpha_composite", "ml_1y_oof"],
        {"alpha_composite": 0.5, "ml_1y_oof": 0.5},
    )
    assert pd.isna(result.loc[0])
    assert result.iloc[1:].notna().all()
