import pandas as pd

from pipeline.build_contract_label_inputs import (
    EXCHANGE_CALENDARS,
    PROVIDER_EXCHANGES,
    _benchmark_for_market_cap,
    _first_common,
    provider_symbol,
)


def test_provider_symbol_only_applies_dot_to_dash():
    assert provider_symbol("brk.b") == "BRK-B"
    assert provider_symbol(" AAPL ") == "AAPL"


def test_benchmark_thresholds_are_exact():
    assert _benchmark_for_market_cap(299_999_999) == "IWC"
    assert _benchmark_for_market_cap(300_000_000) == "IWM"
    assert _benchmark_for_market_cap(2_000_000_000) == "MDY"
    assert _benchmark_for_market_cap(10_000_000_000) == "SPY"


def test_exchange_calendar_and_provider_mappings_are_explicit():
    assert EXCHANGE_CALENDARS == {"NYSE": "XNYS", "Nasdaq": "XNAS"}
    assert "OTC" not in EXCHANGE_CALENDARS
    assert PROVIDER_EXCHANGES["NYSE"] == {"NYQ", "ASE", "PCX"}


def test_first_common_requires_identical_session_and_close():
    close = pd.to_datetime(["2020-07-02 20:00Z", "2020-07-06 20:00Z"])
    stock = pd.DataFrame({"session_date": ["2020-07-02", "2020-07-06"], "market_close": close, "total_return_close": [10.0, 11.0]})
    benchmark = pd.DataFrame({"session_date": ["2020-07-02", "2020-07-06"], "market_close": close, "total_return_close": [20.0, 21.0]})
    row = _first_common(stock, benchmark, after=pd.Timestamp("2020-07-02 00:01Z"), deadline=pd.Timestamp("2020-07-05 00:01Z"))
    assert row["session_date"] == "2020-07-02"
    shifted = benchmark.copy()
    shifted.loc[0, "market_close"] += pd.Timedelta(minutes=1)
    row = _first_common(stock, shifted, after=pd.Timestamp("2020-07-02 00:01Z"), deadline=pd.Timestamp("2020-07-07 00:01Z"))
    assert row["session_date"] == "2020-07-06"


def test_first_common_fails_closed_outside_deadline():
    stock = pd.DataFrame({"session_date": ["2020-07-06"], "market_close": pd.to_datetime(["2020-07-06 20:00Z"]), "total_return_close": [10.0]})
    benchmark = stock.copy()
    assert _first_common(stock, benchmark, after=pd.Timestamp("2020-07-02 00:01Z"), deadline=pd.Timestamp("2020-07-05 00:01Z")) is None
