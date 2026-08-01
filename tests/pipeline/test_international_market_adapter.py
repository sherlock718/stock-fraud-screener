import pandas as pd
import pytest

from pipeline.international_market_adapter import (
    CA_CONTRACT,
    build_p2_compatibility,
    compatibility_summary,
)


def _source():
    return pd.DataFrame({
        "ticker": ["AAA.TO", "BBB.V"], "stock_code": ["AAA", "BBB"],
        "exchange": ["TSX", "TSXV"], "market": ["CA", "CA"],
        "country": ["CA", "CA"], "currency": ["CAD", "CAD"],
        "accounting_std": ["IFRS", "IFRS"], "filed_date": ["2023-03-01", "2023-03-02"],
        "fiscal_year": [2022, 2022], "period_type": ["annual", "annual"],
    })


def test_ca_contract_is_frozen_and_currency_aware():
    assert CA_CONTRACT.market == "CA"
    assert CA_CONTRACT.native_currency == "CAD"
    assert "SPY" not in CA_CONTRACT.benchmark_id
    assert "strictly before" in CA_CONTRACT.purge_policy


def test_build_p2_compatibility_is_row_complete_and_stable():
    first = build_p2_compatibility(_source())
    second = build_p2_compatibility(_source())
    assert len(first) == 2
    assert first["stable_row_id"].is_unique
    assert first["stable_row_id"].tolist() == second["stable_row_id"].tolist()
    assert first["availability_timestamp"].notna().all()
    assert first["target_status"].eq("unsupported_until_local_calendar_benchmark_action_evidence").all()


def test_ca_adapter_fails_closed_on_currency_or_market_drift():
    bad = _source()
    bad.loc[0, "currency"] = "USD"
    with pytest.raises(ValueError, match="currency"):
        build_p2_compatibility(bad)
    bad = _source()
    bad.loc[0, "market"] = "US"
    with pytest.raises(ValueError, match="non-CA"):
        build_p2_compatibility(bad)


def test_summary_prohibits_downstream_results():
    p2 = build_p2_compatibility(_source())
    summary = compatibility_summary(p2, [])
    assert summary["p2_core_row_complete"] is True
    assert summary["performance_calculated"] is False
    assert summary["p2_p3_targets"] == "unsupported_fail_closed"
