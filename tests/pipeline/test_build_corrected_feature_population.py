import pandas as pd
import pytest

from pipeline.build_corrected_feature_population import (
    DEFAULT_ARTIFACT_ROOT,
    NON_ML_FACTOR_PREREQUISITES,
    validate_canonical_population,
    stable_row_id,
)
from pipeline.enrich_fraud_taxonomy import _taxonomy_as_of_dates
from pipeline.step5_compute_features import add_macro_interactions


def test_stable_row_id_uses_entity_period_and_availability():
    frame = pd.DataFrame({
        "entity_id": ["US:0000000001", "US:0000000001"],
        "fiscal_year": [2020, 2021],
        "period_type": ["annual", "annual"],
        "availability_timestamp": [
            "2021-03-01T04:59:59.999999+00:00",
            "2022-03-01T04:59:59.999999+00:00",
        ],
    })
    first = stable_row_id(frame)
    second = stable_row_id(frame.copy())
    assert first.equals(second)
    assert first.nunique() == 2
    assert first.str.len().eq(64).all()


def _canonical_frame() -> pd.DataFrame:
    frame = pd.DataFrame({
        "stable_row_id": ["row-1", "row-2"],
        "population": ["observed_only", "observed_only"],
        "market": ["US", "US"],
        "period_type": ["annual", "annual"],
        "event_time_materialization_timestamp": pd.to_datetime(
            ["2021-03-02T04:59:59.999999+00:00"] * 2,
            utc=True,
        ),
        "step5_winsorization_methods": [
            '{"pe_ratio": "raw_sparse"}',
            '{"pe_ratio": "prior_market_history"}',
        ],
        "pe_ratio_sector_pct": [0.5, 1.0],
        "policy_imputed_1y": [False, False],
    })
    for fields in NON_ML_FACTOR_PREREQUISITES.values():
        for field in fields:
            frame[field] = 1.0
    return frame


def test_canonical_population_contract_records_required_coverage():
    frame = _canonical_frame()

    result = validate_canonical_population(frame, {"row-1", "row-2"})

    assert result["scope"] == {"market": "US", "period_type": "annual"}
    assert result["primary_population"] == "observed_only"
    assert result["event_time_materialization_non_null"] == 2
    assert result["pit_transform_methods_non_empty"] == 2
    assert result["policy_imputed_rows"] == 0


def test_canonical_population_contract_fails_on_unavailable_factor_family():
    frame = _canonical_frame()
    for field in NON_ML_FACTOR_PREREQUISITES["growth"]:
        frame[field] = pd.NA

    with pytest.raises(
        RuntimeError,
        match="growth non-ML factor prerequisites are universally unavailable",
    ):
        validate_canonical_population(frame, {"row-1", "row-2"})


def test_canonical_builder_default_does_not_reuse_frozen_session8f_location():
    assert DEFAULT_ARTIFACT_ROOT.as_posix().endswith(
        "artifacts/canonical/corrected_us_annual"
    )


def test_macro_interactions_fail_closed_when_macro_vintage_is_missing():
    frame = pd.DataFrame({
        "value_composite": [0.8], "quality_composite": [0.7],
        "momentum_12m_prior": [0.2], "debt_to_assets": [0.4],
    })
    result = add_macro_interactions(frame)
    assert result[[
        "value_in_high_rate", "value_in_recession", "momentum_in_expansion",
        "quality_in_recession", "levered_in_tight_credit",
    ]].isna().all().all()


def test_taxonomy_prefers_proven_availability_timestamp():
    frame = pd.DataFrame({
        "filed_date": ["2021-03-01"],
        "availability_timestamp": ["2021-03-02T04:59:59.999999+00:00"],
        "availability_provenance": ["sec_primary_filing"],
    })
    result = _taxonomy_as_of_dates(frame)
    assert str(result.iloc[0]) == "2021-03-02 04:59:59.999999+00:00"


def test_taxonomy_rejects_unproven_availability():
    frame = pd.DataFrame({
        "availability_timestamp": ["2021-03-02T04:59:59.999999+00:00"],
        "availability_provenance": ["legacy_unknown"],
    })
    with pytest.raises(ValueError, match="SEC-primary"):
        _taxonomy_as_of_dates(frame)
