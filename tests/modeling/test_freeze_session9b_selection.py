import pandas as pd

from modeling.freeze_session9b_selection import build_candidate_table, strategy_inventory


def _predictions():
    rows = []
    for stable_row_id in ("a", "b"):
        for horizon in ("1y", "3y"):
            for kind in ("classification", "regression"):
                rows.append({
                    "stable_row_id": stable_row_id, "entity_id": f"US:{stable_row_id}",
                    "fiscal_year": 2020, "population": "observed_only",
                    "decision_timestamp": pd.Timestamp("2021-07-02", tz="UTC"),
                    "prediction_timestamp": pd.Timestamp("2021-07-02 00:01", tz="UTC"),
                    "label_end_date": pd.Timestamp("2020-12-31", tz="UTC"),
                    "label_provenance": "observed", "horizon": horizon,
                    "model_kind": kind, "fold_id": "fold", "eligible": True,
                    "exclusion_reason": "", "feature_artifact_id": "feature",
                    "preprocessing_artifact_id": "preprocess", "model_artifact_id": "model",
                    "calibration_artifact_id": "calibration", "prediction": 0.6, "rank": 1,
                })
    return pd.DataFrame(rows)


def test_inventory_never_substitutes_logistic_for_tree():
    spec = next(x for x in strategy_inventory() if x["strategy_path"] == "production_ml_gates")
    assert "tree_agreement_gate:3y" in spec["required_scores"]
    assert any("calibrated logistic" in blocker for blocker in spec["blockers"])
    assert spec["holdings_status"] == "unavailable"


def test_candidate_freeze_retains_every_row_and_selects_nothing():
    predictions = _predictions()
    inventory = strategy_inventory()
    candidates = build_candidate_table(predictions, inventory)
    assert len(candidates) == 2 * len(inventory)
    assert set(candidates["stable_row_id"]) == {"a", "b"}
    assert not candidates["selected"].any()
    assert candidates["weight"].isna().all()
    assert candidates["entry_timestamp"].isna().all()
    assert candidates["entry_timestamp_status"].eq(
        "unavailable:not_present_in_session9_predictions"
    ).all()
    assert candidates["exclusion_code"].str.startswith("path_unavailable:").all()
