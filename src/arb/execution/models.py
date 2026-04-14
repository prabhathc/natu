"""Order and fill data models."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    LIMIT = "limit"
    MARKET = "market"
    IOC = "ioc"
    POST_ONLY = "post_only"


class OrderStatus(str, Enum):
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class Order(BaseModel):
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    strategy_id: str
    market_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    side: Side
    order_type: OrderType
    price: Optional[Decimal] = None     # None for market orders
    size: Decimal
    status: OrderStatus = OrderStatus.OPEN
    filled_size: Decimal = Decimal("0")
    avg_fill_px: Optional[Decimal] = None
    cancelled_at: Optional[datetime] = None
    notes: str = ""

    @property
    def remaining_size(self) -> Decimal:
        return self.size - self.filled_size

    @property
    def is_done(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED)


class Fill(BaseModel):
    fill_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str
    market_id: str
    strategy_id: str
    ts: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    side: Side
    price: Decimal
    size: Decimal
    fee_bp: Decimal
    fee_usd: Decimal
    is_maker: bool = False
    slippage_bp: Optional[Decimal] = None
    adverse_sel_bp: Optional[Decimal] = None
