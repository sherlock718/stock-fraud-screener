import json

import pandas as pd

from quality.compare_canonical_p2_versions import (
    _coverage_comparison,
    _identity_comparison,
    _response_drift,
    _schema_comparison,
)


def _frame(row_ids):
    return pd.DataFrame(
        {
            "stable_row_id": row_ids,
            "entity_id": [f"US:{value}" for value in row_ids],
            "cik": [str(index).zfill(10) for index, _ in enumerate(row_ids)],
            "ticker": [value.upper() for value in row_ids],
            "fiscal_year": [2020] * len(row_ids),
            "period_type": ["annual"] * len(row_ids),
            "availability_timestamp": pd.to_datetime(
                ["2021-03-02T05:00:00Z"] * len(row_ids),
                utc=True,
            ),
            "feature": [1.0] * len(row_ids),
        }
    )


def test_comparison_covers_identity_schema_coverage_and_missingness():
    baseline = _frame(["a", "b"])
    candidate = _frame(["b", "c"])
    candidate["feature"] = [pd.NA, 2.0]
    identity, summary = _identity_comparison(baseline, candidate)
    assert summary["common_stable_row_ids"] == 1
    assert summary["baseline_only_stable_row_ids"] == 1
    assert summary["candidate_only_stable_row_ids"] == 1
    assert set(identity["source"]) == {"baseline", "candidate"}
    schema = _schema_comparison(baseline, candidate)
    assert schema["common_columns"] == len(baseline.columns)
    coverage = _coverage_comparison(baseline, candidate)
    feature = coverage.set_index("column").loc["feature"]
    assert feature["baseline_non_null"] == 2
    assert feature["candidate_non_null"] == 1
    assert feature["candidate_missing_rate"] == 0.5


def test_source_drift_compares_payload_hash_status_and_timestamps(tmp_path):
    baseline = tmp_path / "baseline.jsonl"
    candidate = tmp_path / "candidate.jsonl"
    baseline.write_text(
        json.dumps(
            {
                "cik": "1",
                "status": "success",
                "response_sha256": "a",
                "retrieved_at_utc": "2020-01-01T00:00:00Z",
            }
        )
        + "\n"
    )
    candidate.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "cik": "1",
                        "status": "success",
                        "response_sha256": "b",
                        "retrieved_at_utc": "2026-01-01T00:00:00Z",
                    }
                ),
                json.dumps(
                    {
                        "cik": "2",
                        "status": "failure",
                        "retrieved_at_utc": "2026-01-02T00:00:00Z",
                    }
                ),
            ]
        )
        + "\n"
    )
    result = _response_drift(baseline, candidate, key="cik")
    assert result["changed_common_payload_or_status"] == 1
    assert result["candidate_only_keys"] == 1
    assert result["candidate_collection_timestamp_max_utc"] == (
        "2026-01-02T00:00:00Z"
    )
