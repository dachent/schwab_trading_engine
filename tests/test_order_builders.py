from order_builders import build_order_spec, normalize_duration, normalize_session, normalize_tax_lot_method
from schemas import Duration, ExecutionProfile, OrderTemplate, Session, TaxLotMethod


def test_normalizers_match_workbook_rules() -> None:
    assert normalize_duration("gtc") == Duration.GOOD_TILL_CANCEL
    assert normalize_session("extended") == Session.SEAMLESS
    assert normalize_tax_lot_method("best tax") == TaxLotMethod.BTAX


def test_build_buy_limit_bracket_payload() -> None:
    payload = build_order_spec(
        order_template=OrderTemplate.BUY_LIMIT_BRACKET,
        account_number="12345678",
        symbol="SPY",
        quantity=10,
        chosen_limit_price=500.25,
        take_profit_price=510.0,
        stop_price=490.0,
        profile=ExecutionProfile(),
    )

    assert payload["orderStrategyType"] == "TRIGGER"
    assert payload["orderType"] == "LIMIT"
    assert payload["childOrderStrategies"][0]["orderStrategyType"] == "OCO"


def test_build_sell_market_payload() -> None:
    payload = build_order_spec(
        order_template=OrderTemplate.SELL_MARKET,
        account_number="12345678",
        symbol="SPY",
        quantity=-10,
        chosen_limit_price=None,
        take_profit_price=None,
        stop_price=None,
        profile=ExecutionProfile(),
    )

    assert payload["orderType"] == "MARKET"
    assert payload["orderLegCollection"][0]["instruction"] == "SELL"

