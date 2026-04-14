"""
Event store: persist market data events to PostgreSQL.

Uses batch inserts with a flush interval to avoid per-event round trips
while keeping latency manageable.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Sequence

import sqlalchemy as sa
import structlog

from arb.db import session_scope
from arb.market_data.models import (
    FundingStateEvent,
    MarketRegistry,
    MarketStateEvent,
    RawQuote,
    RawTrade,
    ReferenceStateEvent,
)

log = structlog.get_logger(__name__)

# Raw table names (we insert via core expression language for performance)
_T_REGISTRY = sa.table(
    "market_registry",
    sa.column("market_id"),
    sa.column("venue_label"),
    sa.column("deployer"),
    sa.column("symbol"),
    sa.column("asset_class"),
    sa.column("collateral"),
    sa.column("oracle_type"),
    sa.column("fee_mode"),
    sa.column("funding_formula"),
    sa.column("max_leverage"),
    sa.column("session_notes"),
    sa.column("docs_url"),
    sa.column("is_active"),
)

_T_QUOTES = sa.table(
    "raw_quotes",
    sa.column("ts"), sa.column("market_id"),
    sa.column("bid_px"), sa.column("bid_sz"),
    sa.column("ask_px"), sa.column("ask_sz"),
    sa.column("source"),
)

_T_TRADES = sa.table(
    "raw_trades",
    sa.column("ts"), sa.column("market_id"), sa.column("trade_id"),
    sa.column("price"), sa.column("size"), sa.column("side"),
    sa.column("is_liquidation"),
)

_T_MSTATE = sa.table(
    "market_state",
    sa.column("ts"), sa.column("market_id"),
    sa.column("mark_px"), sa.column("oracle_px"),
    sa.column("open_interest"), sa.column("day_volume"),
)

_T_FUNDING = sa.table(
    "funding_state",
    sa.column("ts"), sa.column("market_id"),
    sa.column("funding_rate"), sa.column("annualized_rate"),
    sa.column("next_funding_ts"), sa.column("predicted_rate"),
)

_T_REFERENCE = sa.table(
    "reference_state",
    sa.column("ts"), sa.column("symbol"),
    sa.column("price"), sa.column("source"), sa.column("confidence"),
)

_T_GAPS = sa.table(
    "data_gaps",
    sa.column("market_id"), sa.column("data_type"),
    sa.column("gap_start"), sa.column("gap_end"),
    sa.column("duration_s"),
)


class EventStore:
    """
    Buffered, async event store.

    Events are accumulated in memory and flushed on `flush()` or when the
    buffer reaches `max_buffer` items.  Call `start()` to enable the
    background flush loop.
    """

    def __init__(
        self,
        flush_interval_s: float = 1.0,
        max_buffer: int = 500,
    ) -> None:
        self._flush_interval = flush_interval_s
        self._max_buffer = max_buffer
        self._quotes: list[RawQuote] = []
        self._trades: list[RawTrade] = []
        self._mstates: list[MarketStateEvent] = []
        self._fundings: list[FundingStateEvent] = []
        self._refs: list[ReferenceStateEvent] = []
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    # ── Ingestion ─────────────────────────────────────────────────────────────

    async def add_quote(self, q: RawQuote) -> None:
        async with self._lock:
            self._quotes.append(q)
            if len(self._quotes) >= self._max_buffer:
                await self._flush_locked()

    async def add_trade(self, t: RawTrade) -> None:
        async with self._lock:
            self._trades.append(t)
            if len(self._trades) >= self._max_buffer:
                await self._flush_locked()

    async def add_market_state(self, s: MarketStateEvent) -> None:
        async with self._lock:
            self._mstates.append(s)

    async def add_funding(self, f: FundingStateEvent) -> None:
        async with self._lock:
            self._fundings.append(f)

    async def add_reference(self, r: ReferenceStateEvent) -> None:
        async with self._lock:
            self._refs.append(r)

    # ── Registry upsert ───────────────────────────────────────────────────────

    async def upsert_registry(self, markets: Sequence[MarketRegistry]) -> None:
        if not markets:
            return
        rows = [
            {
                "market_id": m.market_id,
                "venue_label": m.venue_label,
                "deployer": m.deployer,
                "symbol": m.symbol,
                "asset_class": m.asset_class,
                "collateral": m.collateral,
                "oracle_type": m.oracle_type,
                "fee_mode": m.fee_mode,
                "funding_formula": m.funding_formula,
                "max_leverage": float(m.max_leverage) if m.max_leverage else None,
                "session_notes": m.session_notes,
                "docs_url": m.docs_url,
                "is_active": m.is_active,
            }
            for m in markets
        ]
        async with session_scope() as sess:
            stmt = (
                sa.dialects.postgresql.insert(_T_REGISTRY)  # type: ignore[attr-defined]
                .values(rows)
                .on_conflict_do_update(
                    index_elements=["market_id"],
                    set_={
                        "venue_label": sa.literal_column("excluded.venue_label"),
                        "fee_mode": sa.literal_column("excluded.fee_mode"),
                        "is_active": sa.literal_column("excluded.is_active"),
                    },
                )
            )
            await sess.execute(stmt)
        log.info("registry_upserted", count=len(rows))

    # ── Flush ─────────────────────────────────────────────────────────────────

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        """Must be called while holding self._lock."""
        try:
            await asyncio.gather(
                self._insert_quotes(),
                self._insert_trades(),
                self._insert_mstates(),
                self._insert_fundings(),
                self._insert_refs(),
            )
        except Exception as e:
            log.error("flush_failed", error=str(e))

    async def _insert_quotes(self) -> None:
        if not self._quotes:
            return
        rows = [
            {
                "ts": q.ts, "market_id": q.market_id,
                "bid_px": float(q.bid_px), "bid_sz": float(q.bid_sz),
                "ask_px": float(q.ask_px), "ask_sz": float(q.ask_sz),
                "source": q.source,
            }
            for q in self._quotes
        ]
        self._quotes.clear()
        async with session_scope() as sess:
            await sess.execute(sa.insert(_T_QUOTES), rows)

    async def _insert_trades(self) -> None:
        if not self._trades:
            return
        rows = [
            {
                "ts": t.ts, "market_id": t.market_id, "trade_id": t.trade_id,
                "price": float(t.price), "size": float(t.size),
                "side": t.side, "is_liquidation": t.is_liquidation,
            }
            for t in self._trades
        ]
        self._trades.clear()
        async with session_scope() as sess:
            await sess.execute(sa.insert(_T_TRADES), rows)

    async def _insert_mstates(self) -> None:
        if not self._mstates:
            return
        rows = [
            {
                "ts": s.ts, "market_id": s.market_id,
                "mark_px": float(s.mark_px) if s.mark_px else None,
                "oracle_px": float(s.oracle_px) if s.oracle_px else None,
                "open_interest": float(s.open_interest) if s.open_interest else None,
                "day_volume": float(s.day_volume) if s.day_volume else None,
            }
            for s in self._mstates
        ]
        self._mstates.clear()
        async with session_scope() as sess:
            await sess.execute(sa.insert(_T_MSTATE), rows)

    async def _insert_fundings(self) -> None:
        if not self._fundings:
            return
        rows = [
            {
                "ts": f.ts, "market_id": f.market_id,
                "funding_rate": float(f.funding_rate),
                "annualized_rate": float(f.annualized_rate),
                "next_funding_ts": f.next_funding_ts,
                "predicted_rate": float(f.predicted_rate) if f.predicted_rate else None,
            }
            for f in self._fundings
        ]
        self._fundings.clear()
        async with session_scope() as sess:
            await sess.execute(sa.insert(_T_FUNDING), rows)

    async def _insert_refs(self) -> None:
        if not self._refs:
            return
        rows = [
            {
                "ts": r.ts, "symbol": r.symbol,
                "price": float(r.price), "source": r.source,
                "confidence": float(r.confidence) if r.confidence else None,
            }
            for r in self._refs
        ]
        self._refs.clear()
        async with session_scope() as sess:
            await sess.execute(sa.insert(_T_REFERENCE), rows)

    # ── Background flush loop ─────────────────────────────────────────────────

    async def start(self) -> None:
        self._task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        await self.flush()

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            await self.flush()

    # ── Gap detection ─────────────────────────────────────────────────────────

    async def log_gap(
        self,
        market_id: str,
        data_type: str,
        gap_start: datetime,
        gap_end: datetime | None = None,
    ) -> None:
        duration = None
        if gap_end:
            duration = (gap_end - gap_start).total_seconds()
        async with session_scope() as sess:
            await sess.execute(
                sa.insert(_T_GAPS),
                [{
                    "market_id": market_id,
                    "data_type": data_type,
                    "gap_start": gap_start,
                    "gap_end": gap_end,
                    "duration_s": duration,
                }],
            )
