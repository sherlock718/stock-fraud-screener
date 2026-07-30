import portfolio.build_session_v3_3_holdings as historical
import portfolio.selection_contract as contract


def test_neutral_selection_contract_matches_accepted_values():
    names = (
        "TREE_THRESHOLD",
        "TARGET_N",
        "WEIGHT",
        "MIN_ADTV",
        "EX_TREE_ROLE",
        "EX_RANK_ROLE",
        "EX_TREE_THRESHOLD",
        "EX_LIQUIDITY_RESPONSE",
        "EX_LIQUIDITY_SYMBOL",
        "EX_LIQUIDITY_EXCHANGE",
        "EX_LIQUIDITY_CURRENCY",
        "EX_LIQUIDITY_SESSION",
        "EX_LIQUIDITY_STALE",
        "EX_LIQUIDITY_INCOMPLETE",
        "EX_LIQUIDITY_PRICE",
        "EX_LIQUIDITY_VOLUME",
        "EX_LIQUIDITY_THRESHOLD",
        "EX_PERIOD_INCOMPLETE",
    )
    assert {
        name: getattr(contract, name) for name in names
    } == {
        name: getattr(historical, name) for name in names
    }
