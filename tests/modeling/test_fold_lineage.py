import json

import numpy as np
import pandas as pd
import pytest

from modeling.fold_lineage import (
    LineageError,
    SelectorConfig,
    dataframe_fingerprint,
    make_lineage,
    select_fold_features,
    validate_lineage,
)
from modeling.score_oof import load_feature_set, run_oof
from modeling.train import (
    apply_sector_zscore_params,
    fit_sector_zscore_params,
    train_baseline,
)
from modeling.train_regression_model import train_regression


def _panel(last_year: int = 2018, rows_per_year: int = 36) -> pd.DataFrame:
    rng = np.random.default_rng(9)
    years = np.repeat(np.arange(2008, last_year + 1), rows_per_year)
    n = len(years)
    signal = rng.normal(size=n)
    forward = signal + rng.normal(scale=0.08, size=n)
    beat = (forward > np.median(forward)).astype(int)
    label_end = pd.to_datetime(years.astype(str) + '-06-30') + pd.DateOffset(years=1)
    return pd.DataFrame({
        'ticker': [f'T{i:05d}' for i in range(n)],
        'fiscal_year': years,
        'filed_date': pd.to_datetime(years.astype(str) + '-03-01'),
        'sic_code': np.where(np.arange(n) % 2, 1000, 2000),
        'feature_signal': signal,
        'feature_noise': rng.normal(size=n),
        'sparse_future': np.where(years >= 2017, rng.normal(size=n), np.nan),
        'forward_return_1y': forward,
        'beat_local_market_1y': beat,
        'stock_label_end_date_1y': label_end,
        'stock_label_provenance_1y': 'observed_market_price',
        'label_end_date_1y': label_end,
        'label_provenance_1y': 'observed_stock_and_benchmark_prices',
    })


def test_appending_future_rows_cannot_change_fold_selection_or_predictions():
    extended = _panel(2020)
    base = extended[extended.fiscal_year <= 2018].copy()
    cutoff = pd.Timestamp('2018-01-01')
    train_base = base[(base.fiscal_year < 2018) & (base.label_end_date_1y < cutoff)]
    train_extended = extended[
        (extended.fiscal_year < 2018) & (extended.label_end_date_1y < cutoff)
    ]
    config = SelectorConfig(top_n=5, min_abs_ic=0.0, min_ic_years=2)

    assert select_fold_features(train_base, 'forward_return_1y', config) == (
        select_fold_features(train_extended, 'forward_return_1y', config)
    )

    scores_base, _ = run_oof(base, '1y', 'beat_local_market_1y', [], 6, 20)
    scores_extended, _ = run_oof(extended, '1y', 'beat_local_market_1y', [], 6, 20)
    idx_base, pred_base = scores_base[2018]
    idx_extended, pred_extended = scores_extended[2018]
    np.testing.assert_array_equal(idx_base, idx_extended)
    np.testing.assert_allclose(pred_base, pred_extended, rtol=0, atol=0)


def test_preprocessing_is_fit_on_training_and_frozen_for_scoring():
    df = _panel(2016)
    train = df[df.fiscal_year <= 2014]
    score = df[df.fiscal_year > 2014].copy()
    features = ['feature_signal', 'feature_noise']

    sector_params = fit_sector_zscore_params(train, features)
    transformed = apply_sector_zscore_params(score, features, sector_params)
    changed_score = score.copy()
    changed_score['feature_signal'] *= 1000
    assert fit_sector_zscore_params(train, features) == sector_params
    changed_transformed = apply_sector_zscore_params(changed_score, features, sector_params)
    assert not transformed['feature_signal'].equals(changed_transformed['feature_signal'])

    baseline = train_baseline(train, features, 'beat_local_market_1y', train[features].median())
    np.testing.assert_allclose(
        baseline.named_steps['scaler'].mean_, train[features].median().pipe(
            lambda med: train[features].fillna(med)
        ).mean().to_numpy(),
    )

    _, used, medians, lo, hi = train_regression(train, features, 'forward_return_1y')
    assert used == features
    assert medians == train[features].median().to_dict()
    assert lo == pytest.approx(train['forward_return_1y'].quantile(0.01))
    assert hi == pytest.approx(train['forward_return_1y'].quantile(0.99))


def test_held_out_test_values_and_coverage_cannot_choose_features(monkeypatch):
    import modeling.run_feature_selection as feature_selection

    df = _panel(2020)

    def fake_ic_filter(df_train, features, return_col, *args, **kwargs):
        table = pd.DataFrame({
            'mean_ic': np.linspace(0.2, 0.1, len(features)),
            'std_ic': 0.1,
            'icir': np.linspace(2.0, 1.0, len(features)),
            'n_years': 5,
            'pct_positive_ic': 1.0,
        }, index=features)
        table.index.name = 'feature'
        return list(features), table

    monkeypatch.setattr(feature_selection, 'ic_icir_filter', fake_ic_filter)
    first = feature_selection.run_selection(
        df, '1y', [], train_end=2015, development_end=2017,
        sector_neutral=False,
    )
    changed = df.copy()
    held_out = changed.fiscal_year > 2017
    changed.loc[held_out, 'feature_signal'] = 1e12
    changed.loc[held_out, 'feature_noise'] = np.nan
    second = feature_selection.run_selection(
        changed, '1y', [], train_end=2015, development_end=2017,
        sector_neutral=False,
    )
    assert first['features'] == second['features']


@pytest.mark.parametrize(
    ('field', 'bad_value'),
    [
        ('dataset_fingerprint', 'stale'),
        ('horizon', '3y'),
        ('label_policy', 'include_policy_imputed'),
        ('cutoff', '2099-01-01T00:00:00'),
    ],
)
def test_lineage_mismatch_fails_closed(field, bad_value):
    df = _panel(2014)
    features = ['feature_signal']
    lineage = make_lineage(
        dataset=df,
        training_population=df,
        horizon='1y',
        target_col='beat_local_market_1y',
        label_policy='observed_only',
        cutoff='2015-01-01T00:00:00',
        selector_config={'top_n': 1},
        features=features,
    )
    expected = dict(lineage)
    expected[field] = bad_value
    with pytest.raises(LineageError, match=field):
        validate_lineage(lineage, expected)


def test_missing_lineage_and_model_meta_fallback_fail_closed(tmp_path, monkeypatch):
    import modeling.score_oof as score_oof

    monkeypatch.setattr(score_oof, 'MODELS_DIR', tmp_path)
    (tmp_path / 'model_meta.json').write_text(json.dumps({
        '1y': {'features': ['feature_signal']}
    }))
    with pytest.raises(LineageError, match='missing feature artifact'):
        load_feature_set('1y', expected_lineage={'horizon': '1y'})

    (tmp_path / 'feature_sets_1y.json').write_text(json.dumps({
        'features': ['feature_signal']
    }))
    with pytest.raises(LineageError, match='no feature/preprocessing lineage'):
        load_feature_set('1y', expected_lineage={'horizon': '1y'})


def test_tuning_rejects_legacy_model_metadata(monkeypatch):
    import modeling.train as train_module
    from modeling.tune import _load_data_for_horizon

    df = _panel(2020)
    monkeypatch.setattr(train_module, 'load_data', lambda: df.copy())
    meta = {'1y': {
        'train_cutoff': 2015,
        'val_end': 2017,
        'features': ['feature_signal'],
        'ret_col': 'forward_return_1y',
        'beat_col': 'beat_local_market_1y',
        'train_medians': {'feature_signal': 0.0},
    }}
    with pytest.raises(LineageError, match='no feature/preprocessing lineage'):
        _load_data_for_horizon(meta, '1y')


def test_dataframe_fingerprint_is_order_invariant_but_content_sensitive():
    df = _panel(2010)
    assert dataframe_fingerprint(df) == dataframe_fingerprint(
        df.sample(frac=1, random_state=4)
    )
    changed = df.copy()
    changed.loc[0, 'feature_signal'] += 1
    assert dataframe_fingerprint(df) != dataframe_fingerprint(changed)
