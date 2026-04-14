"""
Execution simulator with realistic fill modeling.

Fill model:
  - Market / IOC orders: always fill at ask (buy) or bid (sell) + slippage
  - Limit orders: fill if market crosses the price; queue position approximated
    by (order size / total book size at that level) * arrival probability
  - Post-only orders: cancel if would immediately cross

Slippage model:
  - Base slippage scales with urgency (taker vs maker) and order size relative
    to book depth.
  - A random shock term represents execution variance.

Fee model (Hyperliquid HIP-3):
  - Maker: 0.01% (1 bp) default; can be negative in growth mode
  - Taker: 0.035% (3.5 bp) default; varies by tier and HIP-3 mode
  - Deployer fee share: additional on top for HIP-3 markets
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from arb.execution.models import Fill, Order, OrderStatus, OrderType, Side


@dataclass
class FeeSchedule:
    """Fee schedule for a market (in basis points)."""
    maker_bp: float = 1.0       # positive = cost, negative = rebate
    taker_bp: float = 3.5
    deployer_share_bp: float = 0.0
    growth_mode: bool = False

    def total_maker_bp(self) -> float:
        return self.maker_bp + self.deployer_share_bp

    def total_taker_bp(self) -> float:
        return self.taker_bp + self.deployer_share_bp


@dataclass
class SlippageModel:
    """
    Slippage model calibrated to thin HIP-3 markets.

    base_bp: floor slippage for any taker fill
    depth_sensitivity: additional bp per 1% of book depth consumed
    noise_std_bp: random noise std dev
    """
    base_bp: float = 1.0
    depth_sensitivity: float = 5.0
    noise_std_bp: float = 0.5

    def estimate(
        self,
        size: float,
        book_depth: float,          # total size available at best level
        urgency: float = 1.0,       # 1.0 = taker, 0 = passive maker
    ) -> float:
        depth_fraction = size / max(book_depth, size)
        slip = (
            self.base_bp * urgency
            + self.depth_sensitivity * depth_fraction * urgency
            + random.gauss(0, self.noise_std_bp)
        )
        return max(0.0, slip)


class ExecutionSimulator:
    """
    Event-driven execution simulator.

    Feed market data events via `on_quote()` and `on_trade()`.
    Submit orders via `submit()`. Fills are returned via callbacks or
    accessible through `pending_fills`.
    """

    def __init__(
        self,
        fee_schedules: dict[str, FeeSchedule] | None = None,
        slippage_model: SlippageModel | None = None,
        latency_ms: float = 20.0,          # simulated order latency
        stale_quote_threshold_ms: float = 500.0,
    ) -> None:
        self._fees: dict[str, FeeSchedule] = fee_schedules or {}
        self._slippage = slippage_model or SlippageModel()
        self._latency_ms = latency_ms
        self._stale_threshold_ms = stale_quote_threshold_ms

        # Current market state (market_id -> {bid, ask, bid_sz, ask_sz, ts_ms})
        self._books: dict[str, dict] = {}

        # Active orders
        self._orders: dict[str, Order] = {}

        # Pending fills to be consumed
        self.pending_fills: list[Fill] = []

    # ── Market data feed ──────────────────────────────────────────────────────

    def on_quote(
        self,
        market_id: str,
        ts_ms: float,
        bid_px: float,
        bid_sz: float,
        ask_px: float,
        ask_sz: float,
    ) -> list[Fill]:
        self._books[market_id] = {
            "bid": bid_px, "bid_sz": bid_sz,
            "ask": ask_px, "ask_sz": ask_sz,
            "ts_ms": ts_ms,
        }
        return self._try_fill_passive(market_id, ts_ms)

    def on_trade(self, market_id: str, ts_ms: float, price: float, side: str) -> list[Fill]:
        # Trades can trigger passive fills
        return self._try_fill_on_trade(market_id, ts_ms, price, side)

    # ── Order management ──────────────────────────────────────────────────────

    def submit(self, order: Order, current_ts_ms: float) -> Optional[Fill]:
        """
        Submit an order. Market/IOC orders fill immediately against the
        current book. Limit/post-only orders rest in the queue.
        Returns a Fill if immediate execution occurred.
        """
        book = self._books.get(order.market_id)

        if order.order_type == OrderType.MARKET:
            return self._fill_market(order, book, current_ts_ms)
        elif order.order_type == OrderType.IOC:
            fill = self._fill_limit_aggressive(order, book, current_ts_ms)
            if fill is None:
                order.status = OrderStatus.CANCELLED
            return fill
        elif order.order_type == OrderType.POST_ONLY:
            if book and self._would_cross(order, book):
                order.status = OrderStatus.REJECTED
                return None
            self._orders[order.order_id] = order
            return None
        else:  # LIMIT
            self._orders[order.order_id] = order
            return None

    def cancel(self, order_id: str, ts_ms: float) -> bool:
        order = self._orders.pop(order_id, None)
        if order and not order.is_done:
            order.status = OrderStatus.CANCELLED
            return True
        return False

    def cancel_all(self, strategy_id: str) -> int:
        keys = [k for k, o in self._orders.items() if o.strategy_id == strategy_id]
        for k in keys:
            self.cancel(k, 0)
        return len(keys)

    # ── Fill mechanics ────────────────────────────────────────────────────────

    def _fill_market(self, order: Order, book: dict | None, ts_ms: float) -> Optional[Fill]:
        if not book:
            order.status = OrderStatus.REJECTED
            return None
        if self._is_stale(book, ts_ms):
            order.status = OrderStatus.REJECTED
            return None

        px, depth = (
            (book["ask"], book["ask_sz"]) if order.side == Side.BUY
            else (book["bid"], book["bid_sz"])
        )
        slip = self._slippage.estimate(float(order.size), depth, urgency=1.0)
        fill_px = px * (1 + slip / 10_000) if order.side == Side.BUY else px * (1 - slip / 10_000)

        return self._make_fill(order, fill_px, float(order.size), is_maker=False, slippage_bp=slip, ts_ms=ts_ms)

    def _fill_limit_aggressive(self, order: Order, book: dict | None, ts_ms: float) -> Optional[Fill]:
        if not book or self._is_stale(book, ts_ms):
            return None
        if not order.price:
            return self._fill_market(order, book, ts_ms)
        if order.side == Side.BUY and float(order.price) >= book["ask"]:
            slip = self._slippage.estimate(float(order.size), book["ask_sz"], urgency=0.7)
            fill_px = book["ask"] * (1 + slip / 10_000)
            return self._make_fill(order, fill_px, float(order.size), is_maker=False, slippage_bp=slip, ts_ms=ts_ms)
        if order.side == Side.SELL and float(order.price) <= book["bid"]:
            slip = self._slippage.estimate(float(order.size), book["bid_sz"], urgency=0.7)
            fill_px = book["bid"] * (1 - slip / 10_000)
            return self._make_fill(order, fill_px, float(order.size), is_maker=False, slippage_bp=slip, ts_ms=ts_ms)
        return None

    def _try_fill_passive(self, market_id: str, ts_ms: float) -> list[Fill]:
        fills = []
        for oid, order in list(self._orders.items()):
            if order.market_id != market_id or order.is_done:
                continue
            book = self._books.get(market_id)
            if not book or not order.price:
                continue
            fill = None
            if order.side == Side.BUY and book["ask"] <= float(order.price):
                # Queue position: small slip for passive fill
                slip = self._slippage.estimate(float(order.size), book["ask_sz"], urgency=0.1)
                fill = self._make_fill(order, float(order.price), float(order.size), is_maker=True, slippage_bp=slip, ts_ms=ts_ms)
            elif order.side == Side.SELL and book["bid"] >= float(order.price):
                slip = self._slippage.estimate(float(order.size), book["bid_sz"], urgency=0.1)
                fill = self._make_fill(order, float(order.price), float(order.size), is_maker=True, slippage_bp=slip, ts_ms=ts_ms)
            if fill:
                fills.append(fill)
                del self._orders[oid]
        return fills

    def _try_fill_on_trade(self, market_id: str, ts_ms: float, price: float, side: str) -> list[Fill]:
        fills = []
        for oid, order in list(self._orders.items()):
            if order.market_id != market_id or order.is_done or not order.price:
                continue
            if order.side == Side.BUY and price <= float(order.price) and side == "sell":
                slip = 0.0
                fill = self._make_fill(order, float(order.price), float(order.size), is_maker=True, slippage_bp=slip, ts_ms=ts_ms)
                fills.append(fill)
                del self._orders[oid]
            elif order.side == Side.SELL and price >= float(order.price) and side == "buy":
                slip = 0.0
                fill = self._make_fill(order, float(order.price), float(order.size), is_maker=True, slippage_bp=slip, ts_ms=ts_ms)
                fills.append(fill)
                del self._orders[oid]
        return fills

    def _make_fill(
        self,
        order: Order,
        fill_px: float,
        fill_sz: float,
        is_maker: bool,
        slippage_bp: float,
        ts_ms: float,
    ) -> Fill:
        from datetime import datetime, timezone
        fee_sched = self._fees.get(order.market_id, FeeSchedule())
        fee_bp = fee_sched.total_maker_bp() if is_maker else fee_sched.total_taker_bp()
        fee_usd = fill_sz * fill_px * fee_bp / 10_000

        order.filled_size = Decimal(str(fill_sz))
        order.avg_fill_px = Decimal(str(fill_px))
        order.status = OrderStatus.FILLED

        return Fill(
            order_id=order.order_id,
            market_id=order.market_id,
            strategy_id=order.strategy_id,
            ts=datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
            side=order.side,
            price=Decimal(str(fill_px)),
            size=Decimal(str(fill_sz)),
            fee_bp=Decimal(str(fee_bp)),
            fee_usd=Decimal(str(fee_usd)),
            is_maker=is_maker,
            slippage_bp=Decimal(str(slippage_bp)),
        )

    def _would_cross(self, order: Order, book: dict) -> bool:
        if not order.price:
            return True
        if order.side == Side.BUY:
            return float(order.price) >= book["ask"]
        return float(order.price) <= book["bid"]

    def _is_stale(self, book: dict, ts_ms: float) -> bool:
        return (ts_ms - book.get("ts_ms", ts_ms)) > self._stale_threshold_ms
