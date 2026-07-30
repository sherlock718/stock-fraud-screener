"""Neutral fixed portfolio-selection and liquidity contract.

The values are extracted from the accepted historical Session V3.3 route so
active canonical product code no longer imports a historically named module.
"""

TREE_THRESHOLD = 0.55
TARGET_N = 15
WEIGHT = 1.0 / TARGET_N
MIN_ADTV = (200_000.0 / TARGET_N) / 0.01

EX_TREE_ROLE = "missing_oos_tree_probability"
EX_RANK_ROLE = "missing_oos_lightgbm_three_year_return_prediction"
EX_TREE_THRESHOLD = "oos_tree_probability_below_0_55"
EX_LIQUIDITY_RESPONSE = "liquidity_response_missing_or_invalid"
EX_LIQUIDITY_SYMBOL = "liquidity_symbol_mapping_mismatch"
EX_LIQUIDITY_EXCHANGE = "liquidity_exchange_mapping_mismatch"
EX_LIQUIDITY_CURRENCY = "liquidity_currency_not_usd"
EX_LIQUIDITY_SESSION = (
    "liquidity_session_mapping_ambiguous_or_mismatched"
)
EX_LIQUIDITY_STALE = "liquidity_last_session_stale"
EX_LIQUIDITY_INCOMPLETE = (
    "liquidity_window_not_exactly_30_valid_sessions"
)
EX_LIQUIDITY_PRICE = "liquidity_session_close_missing_or_nonpositive"
EX_LIQUIDITY_VOLUME = (
    "liquidity_session_volume_missing_or_nonpositive"
)
EX_LIQUIDITY_THRESHOLD = (
    "liquidity_median_30_session_dollar_volume_below_threshold"
)
EX_PERIOD_INCOMPLETE = "decision_period_fewer_than_15_eligible_candidates"
