"""
Paper trader: runs strategies against live market data using the simulator.

Tracks:
  - Every signal, order, fill, cancel, and miss
  - Expected vs realized edge
  - Orphan leg detection (one leg filled, hedge not)
  - Market escape rate (signal fired but price moved before fill)
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Coroutine, Optional

import structlog

from arb.execution.models import Fill, Order, OrderStatus, OrderType, Side
from arb.execution.simulator import ExecutionSimulator, FeeSchedule, SlippageModel
from arb.market_data.models import RawQuote, RawTrade

log = structlog.get_logger(__name__)


@dataclass
class PaperPosition:
    market_id: str
    strategy_id: str
    net_size: float = 0.0
    avg_entry_px: float = 0.0
    realized_pnl: float = 0.0
    funding_accrued: float = 0.0
    open_orders: list[str] = field(default_factory=list)

    def unrealized_pnl(self, current_px: float) -> float:
        if self.net_size == 0:
            return 0.0
        return self.net_size * (current_px - self.avg_entry_px)


@dataclass
class TradeRecord:
    ts: datetime
    strategy_id: str
    event: str      # 'signal', 'submit', 'fill', 'cancel', 'miss', 'escape'
    market_id: Optional[str]
    side: Optional[str]
    price: Optional[float]
    size: Optional[float]
    notes: str = ""


class PaperTrader:
    """
    Paper trading engine.

    Wraps ExecutionSimulator with multi-strategy position tracking,
    kill-switch logic, and detailed trade recording for research validation.
    """

    def __init__(
        self,
        fee_schedules: dict[str, FeeSchedule] | None = None,
        slippage_model: SlippageModel | None = None,
    ) -> None:
        self._sim = ExecutionSimulator(
            fee_schedules=fee_schedules,
            slippage_model=slippage_model,
        )
        self._positions: dict[str, dict[str, PaperPosition]] = {}  # strategy -> market -> position
        self._records: list[TradeRecord] = []
        self._killed: set[str] = set()     # killed strategy IDs
        self._mid_prices: dict[str, float] = {}

    # ── Market data feed ──────────────────────────────────────────────────────

    async def on_quote(self, q: RawQuote) -> None:
        ts_ms = q.ts.timestamp() * 1000
        mid = float((q.bid_px + q.ask_px) / 2)
        self._mid_prices[q.market_id] = mid
        fills = self._sim.on_quote(
            q.market_id,
            ts_ms,
            float(q.bid_px), float(q.bid_sz),
            float(q.ask_px), float(q.ask_sz),
        )
        for fill in fills:
            self._apply_fill(fill)

    async def on_trade(self, t: RawTrade) -> None:
        ts_ms = t.ts.timestamp() * 1000
        fills = self._sim.on_trade(t.market_id, ts_ms, float(t.price), t.side)
        for fill in fills:
            self._apply_fill(fill)

    async def on_funding(self, market_id: str, rate_8h: float) -> None:
        """Accrue funding for all positions in this market."""
        for strat_positions in self._positions.values():
            pos = strat_positions.get(market_id)
            if pos and pos.net_size != 0:
                mid = self._mid_prices.get(market_id, 0.0)
                notional = abs(pos.net_size) * mid
                # Longs pay funding when rate > 0; shorts receive it
                funding_payment = -pos.net_size * notional * rate_8h
                pos.funding_accrued += funding_payment

    # ── Order submission ──────────────────────────────────────────────────────

    def submit(self, order: Order) -> Optional[Fill]:
        if order.strategy_id in self._killed:
            log.warning("order_rejected_killed", strategy=order.strategy_id)
            return None
        ts_ms = time.time() * 1000
        self._record(TradeRecord(
            ts=datetime.now(tz=timezone.utc),
            strategy_id=order.strategy_id,
            event="submit",
            market_id=order.market_id,
            side=order.side.value,
            price=float(order.price) if order.price else None,
            size=float(order.size),
        ))
        fill = self._sim.submit(order, ts_ms)
        if fill:
            self._apply_fill(fill)
        return fill

    def cancel(self, order_id: str, strategy_id: str) -> bool:
        ts_ms = time.time() * 1000
        result = self._sim.cancel(order_id, ts_ms)
        if result:
            self._record(TradeRecord(
                ts=datetime.now(tz=timezone.utc),
                strategy_id=strategy_id,
                event="cancel",
                market_id=None,
                side=None,
                price=None,
                size=None,
            ))
        return result

    # ── Kill switch ───────────────────────────────────────────────────────────

    def kill(self, strategy_id: str) -> None:
        self._killed.add(strategy_id)
        cancelled = self._sim.cancel_all(strategy_id)
        log.warning("strategy_killed", strategy=strategy_id, orders_cancelled=cancelled)

    def revive(self, strategy_id: str) -> None:
        self._killed.discard(strategy_id)

    # ── Position queries ──────────────────────────────────────────────────────

    def position(self, strategy_id: str, market_id: str) -> Optional[PaperPosition]:
        return self._positions.get(strategy_id, {}).get(market_id)

    def portfolio_pnl(self, strategy_id: str) -> dict:
        positions = self._positions.get(strategy_id, {})
        realized = sum(p.realized_pnl for p in positions.values())
        funding = sum(p.funding_accrued for p in positions.values())
        unrealized = sum(
            p.unrealized_pnl(self._mid_prices.get(mid, p.avg_entry_px))
            for mid, p in positions.items()
        )
        return {
            "strategy_id": strategy_id,
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "funding_accrued": funding,
            "total_pnl": realized + unrealized + funding,
        }

    def trade_log(self) -> list[TradeRecord]:
        return list(self._records)

    # ── Internals ─────────────────────────────────────────────────────────────

    def _apply_fill(self, fill: Fill) -> None:
        strat = self._positions.setdefault(fill.strategy_id, {})
        pos = strat.setdefault(fill.market_id, PaperPosition(fill.market_id, fill.strategy_id))

        fill_px = float(fill.price)
        fill_sz = float(fill.size)
        sign = 1.0 if fill.side == Side.BUY else -1.0
        delta = sign * fill_sz

        if pos.net_size == 0:
            pos.avg_entry_px = fill_px
        elif (pos.net_size > 0 and delta > 0) or (pos.net_size < 0 and delta < 0):
            # Adding to position
            total = abs(pos.net_size) + fill_sz
            pos.avg_entry_px = (abs(pos.net_size) * pos.avg_entry_px + fill_sz * fill_px) / total
        else:
            # Reducing / reversing
            close_sz = min(abs(pos.net_size), fill_sz)
            pnl = close_sz * (fill_px - pos.avg_entry_px) * (1 if pos.net_size > 0 else -1)
            pos.realized_pnl += pnl

        pos.net_size += delta
        pos.realized_pnl -= float(fill.fee_usd)

        self._record(TradeRecord(
            ts=fill.ts,
            strategy_id=fill.strategy_id,
            event="fill",
            market_id=fill.market_id,
            side=fill.side.value,
            price=fill_px,
            size=fill_sz,
            notes=f"fee={float(fill.fee_usd):.4f} slip={fill.slippage_bp}bp maker={fill.is_maker}",
        ))

        log.debug(
            "paper_fill",
            strategy=fill.strategy_id,
            market=fill.market_id,
            side=fill.side.value,
            px=fill_px,
            sz=fill_sz,
        )

    def _record(self, r: TradeRecord) -> None:
        self._records.append(r)
