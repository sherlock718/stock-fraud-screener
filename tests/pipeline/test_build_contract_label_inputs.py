import gzip

import pandas as pd

import pipeline.build_contract_label_inputs as label_inputs
from pipeline.build_contract_label_inputs import (
    EXCHANGE_CALENDARS,
    PROVIDER_EXCHANGES,
    _benchmark_for_market_cap,
    _first_common,
    _store_success_payload,
    _yahoo_cooldown,
    _yahoo_rate_wait,
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


def test_yahoo_rate_wait_spaces_request_starts(monkeypatch):
    now = [10.0]
    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(label_inputs.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(label_inputs.time, "sleep", fake_sleep)
    monkeypatch.setattr(label_inputs, "_YAHOO_NEXT_REQUEST_AT", 0.0)

    _yahoo_rate_wait(0.5)
    _yahoo_rate_wait(0.5)

    assert sleeps == [0.5]
    assert label_inputs._YAHOO_NEXT_REQUEST_AT == 11.0


def test_yahoo_cooldown_extends_shared_request_gate(monkeypatch):
    monkeypatch.setattr(label_inputs.time, "monotonic", lambda: 10.0)
    monkeypatch.setattr(label_inputs, "_YAHOO_NEXT_REQUEST_AT", 12.0)

    _yahoo_cooldown(15.0)

    assert label_inputs._YAHOO_NEXT_REQUEST_AT == 25.0


def test_success_payload_reuses_only_exact_interrupted_raw_file(tmp_path):
    artifact_root = tmp_path / "market"
    target = artifact_root / "raw/chart/ABC.json.gz"
    label_inputs._write_gzip_exclusive(target, b'{"exact":true}')

    stored, reused = _store_success_payload(
        "ABC", artifact_root, target, b'{"exact":true}', attempt=1,
    )

    assert stored == target
    assert reused is True
    with gzip.open(stored, "rb") as payload:
        assert payload.read() == b'{"exact":true}'


def test_success_payload_versions_changed_response_without_overwrite(tmp_path):
    artifact_root = tmp_path / "market"
    target = artifact_root / "raw/chart/ABC.json.gz"
    label_inputs._write_gzip_exclusive(target, b'{"version":1}')

    stored, reused = _store_success_payload(
        "ABC", artifact_root, target, b'{"version":2}', attempt=2,
    )

    assert stored != target
    assert reused is False
    with gzip.open(target, "rb") as payload:
        assert payload.read() == b'{"version":1}'
    with gzip.open(stored, "rb") as payload:
        assert payload.read() == b'{"version":2}'
