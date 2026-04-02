from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class OrderTemplate(StrEnum):
    BUY_MARKET = "buy_market"
    BUY_LIMIT = "buy_limit"
    BUY_MARKET_TAKE_PROFIT = "buy_market_take_profit"
    BUY_LIMIT_TAKE_PROFIT = "buy_limit_take_profit"
    BUY_MARKET_STOP = "buy_market_stop"
    BUY_LIMIT_STOP = "buy_limit_stop"
    BUY_MARKET_BRACKET = "buy_market_bracket"
    BUY_LIMIT_BRACKET = "buy_limit_bracket"
    SELL_MARKET = "sell_market"
    SELL_LIMIT = "sell_limit"


class Duration(StrEnum):
    DAY = "DAY"
    GOOD_TILL_CANCEL = "GOOD_TILL_CANCEL"
    IMMEDIATE_OR_CANCEL = "IMMEDIATE_OR_CANCEL"
    FILL_OR_KILL = "FILL_OR_KILL"
    GOOD_TILL_DATE = "GOOD_TILL_DATE"
    END_OF_WEEK = "END_OF_WEEK"
    END_OF_MONTH = "END_OF_MONTH"


class Session(StrEnum):
    NORMAL = "NORMAL"
    AM = "AM"
    PM = "PM"
    SEAMLESS = "SEAMLESS"


class TaxLotMethod(StrEnum):
    NONE = ""
    FIFO = "FIFO"
    LIFO = "LIFO"
    HIGH_COST = "HIGH_COST"
    LOW_COST = "LOW_COST"
    AVERAGE_COST = "AVERAGE_COST"
    SPECIFIC_LOT = "SPECIFIC_LOT"
    BTAX = "BTAX"


class LimitPricingMethod(StrEnum):
    MANUAL_OVERRIDE = "manual_override"
    LEGACY_WORKBOOK_NBBO = "legacy_workbook_nbbo"
    BASELINE_NBBO = "baseline_nbbo"


class SortPreset(StrEnum):
    FILE_ORDER = "file_order"
    SELLS_DESC_THEN_BUYS_ASC = "sells_desc_then_buys_asc"
    SELLS_DESC_THEN_BUYS_DESC = "sells_desc_then_buys_desc"
    ABS_NOTIONAL_DESC = "abs_notional_desc"


class TaskStatus(StrEnum):
    SUCCESS = "success"
    ERROR = "error"
    TIMEOUT = "timeout"
    INVALID_INPUT = "invalid_input"
    INVALID_OUTPUT = "invalid_output"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PricingParams(BaseModel):
    model_config = ConfigDict(extra="ignore")

    legacy_d_min: float = 0.0005
    legacy_k: float = 3.0
    baseline_min_bps: float = 5.0
    baseline_conservative_min_bps: float = 10.0
    delta_cap_bps: float = 25.0
    tick_cap: int | None = 5


class ExecutionProfile(BaseModel):
    model_config = ConfigDict(use_enum_values=False)

    order_template: OrderTemplate = OrderTemplate.BUY_LIMIT
    duration: Duration = Duration.GOOD_TILL_CANCEL
    session: Session = Session.NORMAL
    tax_lot_method: TaxLotMethod = TaxLotMethod.BTAX
    limit_pricing_method: LimitPricingMethod = LimitPricingMethod.LEGACY_WORKBOOK_NBBO
    pricing_params: PricingParams = Field(default_factory=PricingParams)
    sort_preset: SortPreset = SortPreset.SELLS_DESC_THEN_BUYS_ASC
    preview_only: bool = False
    orders_lookback_days: int = 7

    @field_validator("orders_lookback_days")
    @classmethod
    def clamp_lookback_days(cls, value: int) -> int:
        return max(1, min(90, value))


class ImportOrderRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    row_number: int
    account_number: str
    symbol: str
    quantity: float
    enabled: bool = True
    limit_price_override: float | None = None
    take_profit_price: float | None = None
    stop_price: float | None = None
    note: str | None = None

    @field_validator("account_number", mode="before")
    @classmethod
    def normalize_account_number(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, value: Any) -> str:
        return str(value or "").strip().upper()

    @field_validator("note", mode="before")
    @classmethod
    def normalize_note(cls, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class ImportValidationError(BaseModel):
    row_number: int | None = None
    field_name: str
    message: str


class QuoteData(BaseModel):
    symbol: str
    open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    close_price: float | None = None
    last_price: float | None = None
    mark_price: float | None = None
    bid_price: float | None = None
    ask_price: float | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class PreviewRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    row_number: int
    account_number: str
    symbol: str
    quantity: float
    side: Side
    enabled: bool
    note: str | None = None
    limit_price_override: float | None = None
    take_profit_price: float | None = None
    stop_price: float | None = None
    open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    close_price: float | None = None
    last_price: float | None = None
    bid_price: float | None = None
    ask_price: float | None = None
    nbbo_spread: float | None = None
    midpoint: float | None = None
    spread_percent: float | None = None
    delta: float | None = None
    buy_limit: float | None = None
    sell_limit: float | None = None
    chosen_limit_price: float | None = None
    estimated_notional: float | None = None
    execution_sequence: int | None = None
    local_status: str = "PENDING"
    local_status_detail: str | None = None
    order_id: str | None = None
    broker_status: str | None = None
    broker_status_detail: str | None = None
    broker_quantity: float | None = None
    validation_errors: list[str] = Field(default_factory=list)
    order_payload: dict[str, Any] | None = None


class AccountSnapshot(BaseModel):
    account_name: str
    account_number: str
    account_hash: str
    cash_available: float | None = None
    liquidation_value: float | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class PositionSnapshot(BaseModel):
    account_name: str
    account_number: str
    account_hash: str
    symbol: str
    average_price: float | None = None
    quantity: float | None = None
    value: float | None = None
    day_pl: float | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class BrokerOrder(BaseModel):
    account_name: str
    account_number: str
    account_hash: str
    order_id: str
    quantity: float | None = None
    symbol: str | None = None
    price: float | None = None
    entered_time: str | None = None
    time_in_force: str | None = None
    session: str | None = None
    status: str | None = None
    status_details: str | None = None
    cost_basis_method: str | None = None
    cancelable: bool = False
    raw: dict[str, Any] = Field(default_factory=dict)


class ErrorInfo(BaseModel):
    type: str
    message: str
    traceback: str | None = None


class TaskMetrics(BaseModel):
    duration_ms: int


class TaskRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    request_id: str = Field(default_factory=lambda: str(uuid4()))
    task_name: str
    created_at: str = Field(default_factory=lambda: datetime.now().astimezone().isoformat())
    args: dict[str, Any] = Field(default_factory=dict)
    meta: dict[str, Any] = Field(default_factory=dict)


class TaskResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    request_id: str
    task_name: str
    started_at: str
    finished_at: str
    status: TaskStatus
    return_code: int
    output: dict[str, Any] | None = None
    error: ErrorInfo | None = None
    metrics: TaskMetrics
