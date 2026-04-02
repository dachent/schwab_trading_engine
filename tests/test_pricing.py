from pricing import baseline_nbbo, legacy_workbook_nbbo, price_preview
from schemas import ExecutionProfile, LimitPricingMethod, Side


def test_legacy_workbook_nbbo_matches_formula() -> None:
    result = legacy_workbook_nbbo(Side.BUY, 95.28, 96.43, ExecutionProfile().pricing_params)

    assert round(result["midpoint"], 6) == round((95.28 + 96.43) / 2, 6)
    assert round(result["nbbo_spread"], 6) == round(96.43 - 95.28, 6)
    assert result["buy_limit"] > result["midpoint"]
    assert result["sell_limit"] < result["midpoint"]


def test_baseline_nbbo_rounds_by_tick() -> None:
    result = baseline_nbbo(Side.BUY, 100.00, 100.06, ExecutionProfile().pricing_params)

    assert result["buy_limit"] >= result["midpoint"]
    assert round(result["buy_limit"] * 100) == result["buy_limit"] * 100


def test_manual_override_requires_override_for_limit_orders() -> None:
    profile = ExecutionProfile(limit_pricing_method=LimitPricingMethod.MANUAL_OVERRIDE)
    result, errors = price_preview(
        side=Side.BUY,
        last_price=100.0,
        bid_price=99.9,
        ask_price=100.1,
        limit_price_override=None,
        profile=profile,
        limit_required=True,
    )

    assert result["chosen_limit_price"] is None
    assert errors

