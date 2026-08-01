import pandas as pd
import pytest

import pipeline.build_corrected_feature_population as corrected_population
from pipeline.build_refreshed_us_p2 import (
    _build_market_inputs,
    _macro_unavailable,
)


def test_refreshed_p2_macro_contract_fails_closed():
    frame = pd.DataFrame(
        {
            "yield_curve": [pd.NA],
            "macro_regime": [pd.NA],
            "value_in_recession": [pd.NA],
        }
    )
    counts = _macro_unavailable(frame)
    assert counts == {
        "macro_regime": 0,
        "value_in_recession": 0,
        "yield_curve": 0,
    }
    frame.loc[0, "yield_curve"] = 1.0
    with pytest.raises(RuntimeError, match="uncertified macro"):
        _macro_unavailable(frame)


def test_corrected_p2_helpers_accept_a_versioned_market_root(
    tmp_path,
    monkeypatch,
):
    market = tmp_path / "market"
    (market / "outputs/observed_only").mkdir(parents=True)
    (market / "support").mkdir()
    keys = {
        "entity_id": ["US:0000000001"],
        "cik": ["0000000001"],
        "ticker": ["ABC"],
        "fiscal_year": [2020],
    }
    label = pd.DataFrame(
        {
            **keys,
            "horizon": ["6m"],
            "label_end_date": pd.to_datetime(["2022-01-03"], utc=True),
            "stock_return": [0.1],
            "benchmark_return": [0.05],
            "relative_return": [0.05],
            "outperformed_benchmark": [True],
            "label_provenance": [
                "observed_common_session_provider_adjclose"
            ],
            "policy_imputed": [False],
        }
    )
    labels = pd.concat(
        [label.assign(horizon=horizon) for horizon in ("6m", "1y", "2y", "3y", "5y")],
        ignore_index=True,
    )
    labels.to_parquet(
        market / "outputs/observed_only/labels.parquet",
        index=False,
    )
    support = labels[
        ["entity_id", "cik", "ticker", "fiscal_year", "horizon"]
    ].assign(classification="supported", reason=pd.NA)
    support.to_parquet(
        market / "support/observed_only_row_horizon.parquet",
        index=False,
    )
    price_features = pd.DataFrame(
        {
            "stable_row_id": ["row"],
            **keys,
        }
    )
    baseline_market_root = corrected_population.SESSION8E
    monkeypatch.setattr(
        corrected_population,
        "build_price_features",
        lambda _annual, _support: price_features,
    )
    returned_prices, result, returned_support = _build_market_inputs(
        pd.DataFrame(keys),
        support,
        market,
    )
    assert returned_prices.equals(price_features)
    assert result["forward_return_3y"].iloc[0] == 0.1
    assert len(returned_support) == 5
    assert corrected_population.SESSION8E == baseline_market_root
