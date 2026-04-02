from schemas import ExecutionProfile, PreviewRow, Side
from tasks import _apply_execution_order


def test_default_sorting_sells_desc_then_buys_asc() -> None:
    profile = ExecutionProfile()
    rows = [
        PreviewRow(row_number=1, account_number="1", symbol="AAA", quantity=-5, side=Side.SELL, enabled=True, estimated_notional=50, local_status="READY"),
        PreviewRow(row_number=2, account_number="1", symbol="BBB", quantity=-5, side=Side.SELL, enabled=True, estimated_notional=120, local_status="READY"),
        PreviewRow(row_number=3, account_number="1", symbol="CCC", quantity=5, side=Side.BUY, enabled=True, estimated_notional=90, local_status="READY"),
        PreviewRow(row_number=4, account_number="1", symbol="DDD", quantity=5, side=Side.BUY, enabled=True, estimated_notional=30, local_status="READY"),
    ]

    ordered = _apply_execution_order(rows, profile.sort_preset)

    ranked = [row.symbol for row in ordered if row.execution_sequence is not None]
    assert ranked == ["BBB", "AAA", "DDD", "CCC"]
