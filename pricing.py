from __future__ import annotations

import math
from typing import Any

from schemas import ExecutionProfile, LimitPricingMethod, PricingParams, Side


def _tick_size(price: float | None) -> float:
    if price is None or price <= 0:
        return 0.01
    return 0.0001 if price < 1 else 0.01


def _round_buy_to_tick(price: float, tick: float) -> float:
    return math.ceil(price / tick) * tick


def _round_sell_to_tick(price: float, tick: float) -> float:
    return math.floor(price / tick) * tick


def compute_nbbo_metrics(bid: float | None, ask: float | None) -> dict[str, float | None]:
    if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
        return {
            "nbbo_spread": None,
            "midpoint": None,
            "spread_percent": None,
        }

    midpoint = (bid + ask) / 2
    if midpoint <= 0:
        return {"nbbo_spread": None, "midpoint": None, "spread_percent": None}
    spread = ask - bid
    return {
        "nbbo_spread": spread,
        "midpoint": midpoint,
        "spread_percent": spread / midpoint,
    }


def legacy_workbook_nbbo(
    side: Side,
    bid: float | None,
    ask: float | None,
    params: PricingParams,
) -> dict[str, float | None]:
    metrics = compute_nbbo_metrics(bid, ask)
    midpoint = metrics["midpoint"]
    spread_percent = metrics["spread_percent"]
    if midpoint is None or spread_percent is None:
        return {**metrics, "delta": None, "buy_limit": None, "sell_limit": None, "chosen_limit_price": None}

    delta = max(params.legacy_d_min, params.legacy_k * spread_percent / 2)
    buy_limit = midpoint * (1 + delta)
    sell_limit = midpoint * (1 - delta)
    chosen = buy_limit if side == Side.BUY else sell_limit
    return {
        **metrics,
        "delta": delta,
        "buy_limit": buy_limit,
        "sell_limit": sell_limit,
        "chosen_limit_price": chosen,
    }


def baseline_nbbo(
    side: Side,
    bid: float | None,
    ask: float | None,
    params: PricingParams,
) -> dict[str, float | None]:
    metrics = compute_nbbo_metrics(bid, ask)
    midpoint = metrics["midpoint"]
    spread_percent = metrics["spread_percent"]
    if midpoint is None or spread_percent is None:
        return {**metrics, "delta": None, "buy_limit": None, "sell_limit": None, "chosen_limit_price": None}

    tick = _tick_size(midpoint)
    spread_bps = spread_percent * 10_000
    if spread_bps <= 2:
        k = 0.5
    elif spread_bps <= 10:
        k = 1.0
    elif spread_bps <= 40:
        k = 1.5
    else:
        k = 2.0

    delta_min = max(tick / midpoint, params.baseline_min_bps / 10_000)
    delta = max(delta_min, k * spread_percent / 2)
    delta = min(delta, params.delta_cap_bps / 10_000)

    raw_buy = midpoint * (1 + delta)
    raw_sell = midpoint * (1 - delta)
    buy_limit = _round_buy_to_tick(raw_buy, tick)
    sell_limit = _round_sell_to_tick(raw_sell, tick)

    if params.tick_cap is not None:
        buy_cap = midpoint + params.tick_cap * tick
        sell_cap = midpoint - params.tick_cap * tick
        buy_limit = min(buy_limit, buy_cap)
        sell_limit = max(sell_limit, sell_cap)

    chosen = buy_limit if side == Side.BUY else sell_limit
    return {
        **metrics,
        "delta": delta,
        "buy_limit": buy_limit,
        "sell_limit": sell_limit,
        "chosen_limit_price": chosen,
    }


def reference_price(
    chosen_limit: float | None,
    last_price: float | None,
    bid_price: float | None,
    ask_price: float | None,
    side: Side,
) -> float | None:
    if chosen_limit is not None:
        return chosen_limit
    if last_price is not None:
        return last_price
    if side == Side.BUY:
        return ask_price or bid_price
    return bid_price or ask_price


def price_preview(
    side: Side,
    last_price: float | None,
    bid_price: float | None,
    ask_price: float | None,
    limit_price_override: float | None,
    profile: ExecutionProfile,
    limit_required: bool,
    strict_market_data: bool = True,
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    if limit_price_override is not None and limit_price_override <= 0:
        errors.append("limit_price_override must be positive")

    if profile.limit_pricing_method == LimitPricingMethod.LEGACY_WORKBOOK_NBBO:
        details = legacy_workbook_nbbo(side, bid_price, ask_price, profile.pricing_params)
    elif profile.limit_pricing_method == LimitPricingMethod.BASELINE_NBBO:
        details = baseline_nbbo(side, bid_price, ask_price, profile.pricing_params)
    else:
        details = {
            **compute_nbbo_metrics(bid_price, ask_price),
            "delta": None,
            "buy_limit": None,
            "sell_limit": None,
            "chosen_limit_price": None,
        }

    chosen_limit = limit_price_override if limit_price_override is not None else details["chosen_limit_price"]
    if profile.limit_pricing_method == LimitPricingMethod.MANUAL_OVERRIDE and chosen_limit is None and limit_required:
        errors.append("limit_price_override is required for manual_override pricing")

    if (
        strict_market_data
        and profile.limit_pricing_method in {LimitPricingMethod.LEGACY_WORKBOOK_NBBO, LimitPricingMethod.BASELINE_NBBO}
        and limit_required
        and chosen_limit is None
    ):
        errors.append("valid bid/ask is required to compute an algorithmic limit")

    reference = reference_price(chosen_limit, last_price, bid_price, ask_price, side)
    estimated_notional = abs(reference) if reference is not None else None

    return {
        **details,
        "chosen_limit_price": chosen_limit,
        "estimated_notional_unit_price": reference,
        "estimated_notional": estimated_notional,
    }, errors
