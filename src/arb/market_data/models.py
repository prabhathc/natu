"""Pydantic models for all market data events."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, field_validator


class MarketRegistry(BaseModel):
    """Canonical record for one HIP-3 market."""

    market_id: str
    venue_label: str                    # 'xyz', 'felix', 'hl_native'
    deployer: Optional[str] = None
    symbol: str
    asset_class: Literal["commodity", "index", "equity", "fx", "crypto"]
    collateral: str = "USDC"
    oracle_type: Optional[str] = None
    fee_mode: Optional[str] = None
    funding_formula: Optional[str] = None
    max_leverage: Optional[Decimal] = None
    session_notes: Optional[str] = None
    docs_url: Optional[str] = None
    is_active: bool = True


class RawQuote(BaseModel):
    """Top-of-book snapshot."""

    ts: datetime
    market_id: str
    bid_px: Decimal
    bid_sz: Decimal
    ask_px: Decimal
    ask_sz: Decimal
    source: str = "ws"

    @property
    def mid_px(self) -> Decimal:
        return (self.bid_px + self.ask_px) / 2

    @property
    def spread_bp(self) -> Optional[Decimal]:
        if self.bid_px > 0:
            return (self.ask_px - self.bid_px) / self.bid_px * 10_000
        return None


class RawTrade(BaseModel):
    """Single aggressor trade."""

    ts: datetime
    market_id: str
    trade_id: Optional[str] = None
    price: Decimal
    size: Decimal
    side: Literal["buy", "sell"]
    is_liquidation: bool = False


class MarketStateEvent(BaseModel):
    """Periodic mark/OI/premium snapshot."""

    ts: datetime
    market_id: str
    mark_px: Optional[Decimal] = None
    oracle_px: Optional[Decimal] = None
    open_interest: Optional[Decimal] = None
    day_volume: Optional[Decimal] = None

    @property
    def premium(self) -> Optional[Decimal]:
        if self.mark_px and self.oracle_px and self.oracle_px > 0:
            return (self.mark_px - self.oracle_px) / self.oracle_px
        return None


class FundingStateEvent(BaseModel):
    """Funding rate snapshot."""

    ts: datetime
    market_id: str
    funding_rate: Decimal           # 8h rate
    next_funding_ts: Optional[datetime] = None
    predicted_rate: Optional[Decimal] = None

    @property
    def annualized_rate(self) -> Decimal:
        # 3 funding periods per day * 365
        return self.funding_rate * 3 * 365


class ReferenceStateEvent(BaseModel):
    """External reference price (oracle / TradFi feed)."""

    ts: datetime
    symbol: str
    price: Decimal
    source: str
    confidence: Optional[Decimal] = None
