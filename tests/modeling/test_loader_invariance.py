"""Future-invariance contracts for downstream dataset loaders."""
from __future__ import annotations

import pandas as pd
import pytest

import modeling.constants as modeling_constants
import research.factor_research as factor_research


def _materialized_rows(include_future: bool = False) -> pd.DataFrame:
    rows = [
        {
            'ticker': 'OLD_A',
            'fiscal_year': 2019,
            'period_type': 'annual',
            'total_assets': 100.0,
            'revenue_growth_yoy': -25.0,
        },
        {
            'ticker': 'OLD_B',
            'fiscal_year': 2020,
            'period_type': 'annual',
            'total_assets': 200.0,
            'revenue_growth_yoy': 40.0,
        },
    ]
    if include_future:
        rows.append({
            'ticker': 'FUTURE',
            'fiscal_year': 2025,
            'period_type': 'annual',
            'total_assets': 300.0,
            'revenue_growth_yoy': 1_000_000.0,
        })
    return pd.DataFrame(rows)


def _write(path, include_future: bool = False) -> None:
    _materialized_rows(include_future).to_parquet(path, index=False)


def test_modeling_loader_preserves_materialized_growth_and_future_invariance(tmp_path, monkeypatch):
    historical_path = tmp_path / 'historical.parquet'
    extended_path = tmp_path / 'extended.parquet'
    _write(historical_path)
    _write(extended_path, include_future=True)
    monkeypatch.setattr(modeling_constants, 'BASE', tmp_path)
    monkeypatch.setattr(modeling_constants, 'add_piotroski_ext', lambda df: df)
    monkeypatch.setattr(modeling_constants, 'add_normalised_ratios', lambda df: df)

    historical = modeling_constants.load_data(historical_path).set_index('ticker')
    extended = modeling_constants.load_data(extended_path).set_index('ticker')

    assert historical.loc['OLD_A', 'revenue_growth_yoy'] == -25.0
    assert historical.loc['OLD_B', 'revenue_growth_yoy'] == 40.0
    pd.testing.assert_series_equal(
        historical['revenue_growth_yoy'].sort_index(),
        extended.loc[historical.index, 'revenue_growth_yoy'].sort_index(),
    )


def test_factor_research_loader_preserves_materialized_growth_and_future_invariance(tmp_path, monkeypatch):
    historical_path = tmp_path / 'historical.parquet'
    extended_path = tmp_path / 'extended.parquet'
    _write(historical_path)
    _write(extended_path, include_future=True)
    monkeypatch.setattr(factor_research, '_add_normalised_ratios', lambda df: df)

    monkeypatch.setattr(factor_research, 'DATA_PATH', historical_path)
    historical = factor_research.load_data().set_index('ticker')
    monkeypatch.setattr(factor_research, 'DATA_PATH', extended_path)
    extended = factor_research.load_data().set_index('ticker')

    assert historical.loc['OLD_A', 'revenue_growth_yoy'] == -25.0
    assert historical.loc['OLD_B', 'revenue_growth_yoy'] == 40.0
    pd.testing.assert_series_equal(
        historical['revenue_growth_yoy'].sort_index(),
        extended.loc[historical.index, 'revenue_growth_yoy'].sort_index(),
    )


@pytest.mark.parametrize('loader_name', ['modeling', 'research'])
def test_downstream_loaders_do_not_estimate_clipping_bounds(tmp_path, monkeypatch, loader_name):
    path = tmp_path / 'materialized.parquet'
    _write(path, include_future=True)
    monkeypatch.setattr(
        pd.Series,
        'quantile',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('loader fitted a quantile')),
    )

    if loader_name == 'modeling':
        monkeypatch.setattr(modeling_constants, 'BASE', tmp_path)
        monkeypatch.setattr(modeling_constants, 'add_piotroski_ext', lambda df: df)
        monkeypatch.setattr(modeling_constants, 'add_normalised_ratios', lambda df: df)
        loaded = modeling_constants.load_data(path)
    else:
        monkeypatch.setattr(factor_research, 'DATA_PATH', path)
        monkeypatch.setattr(factor_research, '_add_normalised_ratios', lambda df: df)
        loaded = factor_research.load_data()

    assert len(loaded) == 3
