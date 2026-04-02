from __future__ import annotations

from typing import Any

from schemas import Duration, ExecutionProfile, OrderTemplate, Session, Side, TaxLotMethod


def normalize_duration(raw: str | Duration | None) -> Duration:
    if isinstance(raw, Duration):
        return raw
    value = str(raw or "").strip().upper().replace(" ", "_").replace("-", "_")
    mapping = {
        "": Duration.GOOD_TILL_CANCEL,
        "DAY": Duration.DAY,
        "GTC": Duration.GOOD_TILL_CANCEL,
        "GOODTILLCANCEL": Duration.GOOD_TILL_CANCEL,
        "GOOD_TILL_CANCEL": Duration.GOOD_TILL_CANCEL,
        "GOOD_TILL_CXL": Duration.GOOD_TILL_CANCEL,
        "IOC": Duration.IMMEDIATE_OR_CANCEL,
        "IMMEDIATE_OR_CANCEL": Duration.IMMEDIATE_OR_CANCEL,
        "FOK": Duration.FILL_OR_KILL,
        "FILL_OR_KILL": Duration.FILL_OR_KILL,
        "GTD": Duration.GOOD_TILL_DATE,
        "GOOD_TILL_DATE": Duration.GOOD_TILL_DATE,
        "EOW": Duration.END_OF_WEEK,
        "END_OF_WEEK": Duration.END_OF_WEEK,
        "EOM": Duration.END_OF_MONTH,
        "END_OF_MONTH": Duration.END_OF_MONTH,
    }
    return mapping.get(value, Duration.GOOD_TILL_CANCEL)


def normalize_session(raw: str | Session | None) -> Session:
    if isinstance(raw, Session):
        return raw
    value = str(raw or "").strip().upper().replace(" ", "_").replace("-", "_")
    mapping = {
        "": Session.NORMAL,
        "NORMAL": Session.NORMAL,
        "AM": Session.AM,
        "PRE": Session.AM,
        "PREMARKET": Session.AM,
        "PRE_MARKET": Session.AM,
        "PM": Session.PM,
        "POST": Session.PM,
        "POSTMARKET": Session.PM,
        "AFTERHOURS": Session.PM,
        "AFTER_HOURS": Session.PM,
        "SEAMLESS": Session.SEAMLESS,
        "EXT": Session.SEAMLESS,
        "EXTENDED": Session.SEAMLESS,
        "EXTENDED_HOURS": Session.SEAMLESS,
    }
    return mapping.get(value, Session.NORMAL)


def normalize_tax_lot_method(raw: str | TaxLotMethod | None) -> TaxLotMethod:
    if isinstance(raw, TaxLotMethod):
        return raw
    value = str(raw or "").strip().upper().replace(" ", "_").replace("-", "_")
    mapping = {
        "": TaxLotMethod.NONE,
        "FIFO": TaxLotMethod.FIFO,
        "LIFO": TaxLotMethod.LIFO,
        "HIGH_COST": TaxLotMethod.HIGH_COST,
        "HIGHCOST": TaxLotMethod.HIGH_COST,
        "HIGH": TaxLotMethod.HIGH_COST,
        "LOW_COST": TaxLotMethod.LOW_COST,
        "LOWCOST": TaxLotMethod.LOW_COST,
        "LOW": TaxLotMethod.LOW_COST,
        "AVERAGE_COST": TaxLotMethod.AVERAGE_COST,
        "AVERAGECOST": TaxLotMethod.AVERAGE_COST,
        "AVERAGE": TaxLotMethod.AVERAGE_COST,
        "SPECIFIC_LOT": TaxLotMethod.SPECIFIC_LOT,
        "SPECIFICLOT": TaxLotMethod.SPECIFIC_LOT,
        "SPECIFIED_LOT": TaxLotMethod.SPECIFIC_LOT,
        "SPECIFIEDLOT": TaxLotMethod.SPECIFIC_LOT,
        "BEST_TAX": TaxLotMethod.BTAX,
        "TAX_LOT_OPTIMIZER": TaxLotMethod.BTAX,
        "TAXLOTOPTIMIZER": TaxLotMethod.BTAX,
        "TLO": TaxLotMethod.BTAX,
        "BTAX": TaxLotMethod.BTAX,
    }
    return mapping.get(value, TaxLotMethod.NONE)


def template_side(order_template: OrderTemplate) -> Side:
    return Side.SELL if order_template in {OrderTemplate.SELL_LIMIT, OrderTemplate.SELL_MARKET} else Side.BUY


def template_requires_limit(order_template: OrderTemplate) -> bool:
    return order_template in {
        OrderTemplate.BUY_LIMIT,
        OrderTemplate.BUY_LIMIT_TAKE_PROFIT,
        OrderTemplate.BUY_LIMIT_STOP,
        OrderTemplate.BUY_LIMIT_BRACKET,
        OrderTemplate.SELL_LIMIT,
    }


def _base_order(
    *,
    strategy_type: str,
    session: Session,
    duration: Duration,
    tax_lot_method: TaxLotMethod,
    order_type: str,
    quantity: float,
    symbol: str,
    instruction: str,
    price: float | None = None,
    stop_price: float | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "orderStrategyType": strategy_type,
        "session": session.value,
        "duration": duration.value,
        "orderType": order_type,
        "orderLegCollection": [
            {
                "instruction": instruction,
                "quantity": abs(quantity),
                "instrument": {
                    "assetType": "EQUITY",
                    "symbol": symbol,
                },
            }
        ],
    }
    if tax_lot_method.value:
        payload["taxLotMethod"] = tax_lot_method.value
    if price is not None:
        payload["price"] = f"{price:.4f}".rstrip("0").rstrip(".")
    if stop_price is not None:
        payload["stopPrice"] = f"{stop_price:.4f}".rstrip("0").rstrip(".")
    return payload


def _normalize_profile(profile: ExecutionProfile, is_market: bool, uses_stop: bool) -> tuple[Duration, Session, TaxLotMethod]:
    duration = normalize_duration(profile.duration)
    session = normalize_session(profile.session)
    tax_lot_method = normalize_tax_lot_method(profile.tax_lot_method)

    if is_market:
        duration = Duration.DAY
        if session != Session.NORMAL:
            raise ValueError("Market orders require SESSION=NORMAL")

    if uses_stop and session != Session.NORMAL:
        raise ValueError("Stop orders require SESSION=NORMAL")

    return duration, session, tax_lot_method


def build_order_spec(
    *,
    order_template: OrderTemplate,
    account_number: str,
    symbol: str,
    quantity: float,
    chosen_limit_price: float | None,
    take_profit_price: float | None,
    stop_price: float | None,
    profile: ExecutionProfile,
) -> dict[str, Any]:
    side = template_side(order_template)
    if side == Side.BUY and quantity <= 0:
        raise ValueError("Buy templates require a positive quantity")
    if side == Side.SELL and quantity >= 0:
        raise ValueError("Sell templates require a negative quantity")

    if not account_number.strip():
        raise ValueError("account_number is required")
    if not symbol.strip():
        raise ValueError("symbol is required")

    is_market = order_template in {
        OrderTemplate.BUY_MARKET,
        OrderTemplate.BUY_MARKET_TAKE_PROFIT,
        OrderTemplate.BUY_MARKET_STOP,
        OrderTemplate.BUY_MARKET_BRACKET,
        OrderTemplate.SELL_MARKET,
    }
    uses_stop = order_template in {
        OrderTemplate.BUY_MARKET_STOP,
        OrderTemplate.BUY_LIMIT_STOP,
        OrderTemplate.BUY_MARKET_BRACKET,
        OrderTemplate.BUY_LIMIT_BRACKET,
    }
    duration, session, tax_lot_method = _normalize_profile(profile, is_market=is_market, uses_stop=uses_stop)

    if template_requires_limit(order_template) and chosen_limit_price is None:
        raise ValueError("Selected order template requires a computed or manual limit price")

    if order_template in {OrderTemplate.BUY_MARKET_TAKE_PROFIT, OrderTemplate.BUY_LIMIT_TAKE_PROFIT} and take_profit_price is None:
        raise ValueError("take_profit_price is required for the selected order template")
    if order_template in {OrderTemplate.BUY_MARKET_STOP, OrderTemplate.BUY_LIMIT_STOP} and stop_price is None:
        raise ValueError("stop_price is required for the selected order template")
    if order_template in {OrderTemplate.BUY_MARKET_BRACKET, OrderTemplate.BUY_LIMIT_BRACKET}:
        if take_profit_price is None or stop_price is None:
            raise ValueError("Both take_profit_price and stop_price are required for bracket orders")

    if order_template == OrderTemplate.BUY_LIMIT:
        return _base_order(
            strategy_type="SINGLE",
            session=session,
            duration=duration,
            tax_lot_method=tax_lot_method,
            order_type="LIMIT",
            quantity=quantity,
            symbol=symbol,
            instruction="BUY",
            price=chosen_limit_price,
        )

    if order_template == OrderTemplate.BUY_MARKET:
        return _base_order(
            strategy_type="SINGLE",
            session=session,
            duration=duration,
            tax_lot_method=tax_lot_method,
            order_type="MARKET",
            quantity=quantity,
            symbol=symbol,
            instruction="BUY",
        )

    if order_template == OrderTemplate.SELL_LIMIT:
        return _base_order(
            strategy_type="SINGLE",
            session=session,
            duration=duration,
            tax_lot_method=tax_lot_method,
            order_type="LIMIT",
            quantity=quantity,
            symbol=symbol,
            instruction="SELL",
            price=chosen_limit_price,
        )

    if order_template == OrderTemplate.SELL_MARKET:
        return _base_order(
            strategy_type="SINGLE",
            session=session,
            duration=duration,
            tax_lot_method=tax_lot_method,
            order_type="MARKET",
            quantity=quantity,
            symbol=symbol,
            instruction="SELL",
        )

    root_type = "MARKET" if order_template in {
        OrderTemplate.BUY_MARKET_TAKE_PROFIT,
        OrderTemplate.BUY_MARKET_STOP,
        OrderTemplate.BUY_MARKET_BRACKET,
    } else "LIMIT"

    root = _base_order(
        strategy_type="TRIGGER",
        session=session,
        duration=duration,
        tax_lot_method=tax_lot_method,
        order_type=root_type,
        quantity=quantity,
        symbol=symbol,
        instruction="BUY",
        price=chosen_limit_price if root_type == "LIMIT" else None,
    )

    limit_child = _base_order(
        strategy_type="SINGLE",
        session=session,
        duration=duration,
        tax_lot_method=TaxLotMethod.NONE,
        order_type="LIMIT",
        quantity=quantity,
        symbol=symbol,
        instruction="SELL",
        price=take_profit_price,
    )
    stop_child = _base_order(
        strategy_type="SINGLE",
        session=session,
        duration=duration,
        tax_lot_method=TaxLotMethod.NONE,
        order_type="STOP",
        quantity=quantity,
        symbol=symbol,
        instruction="SELL",
        stop_price=stop_price,
    )

    if order_template in {OrderTemplate.BUY_MARKET_TAKE_PROFIT, OrderTemplate.BUY_LIMIT_TAKE_PROFIT}:
        root["childOrderStrategies"] = [
            {
                "orderStrategyType": "OCO",
                "childOrderStrategies": [limit_child],
            }
        ]
        return root

    if order_template in {OrderTemplate.BUY_MARKET_STOP, OrderTemplate.BUY_LIMIT_STOP}:
        root["childOrderStrategies"] = [stop_child]
        return root

    root["childOrderStrategies"] = [
        {
            "orderStrategyType": "OCO",
            "childOrderStrategies": [limit_child, stop_child],
        }
    ]
    return root

