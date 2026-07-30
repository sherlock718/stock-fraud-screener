import numpy as np
import pandas as pd

import modeling.build_session_v3_2_oos as historical
import modeling.oos_modeling as oos


def test_neutral_oos_contract_preserves_roles_and_fail_closed_code():
    names = (
        "MODEL_ROLES",
        "EXCLUSION_NO_HISTORY",
        "EXCLUSION_TREE_CLASSES",
        "EXCLUSION_NO_FEATURES",
        "EXCLUSION_INVALID_MEDIANS",
        "EXCLUSION_INVALID_OUTPUT",
        "EXCLUSION_FUTURE",
        "LABEL_DERIVED_CANDIDATE_AVAILABILITY",
    )
    assert {
        name: getattr(oos, name) for name in names
    } == {
        name: getattr(historical, name) for name in names
    }


def test_neutral_masking_preserves_only_strictly_available_label_value():
    score = pd.DataFrame(
        {
            "observed_excess_return_3y": [1.0, 2.0, 3.0],
            "label_end_date": pd.to_datetime(
                ["2022-01-01", "2023-01-01", None], utc=True
            ),
        }
    )
    masked, counts = oos.mask_unavailable_score_features(
        score,
        ["observed_excess_return_3y"],
        pd.Timestamp("2023-01-01", tz="UTC"),
    )
    assert masked["observed_excess_return_3y"].iloc[0] == 1.0
    assert np.isnan(masked["observed_excess_return_3y"].iloc[1])
    assert np.isnan(masked["observed_excess_return_3y"].iloc[2])
    assert counts == {"observed_excess_return_3y": 2}
