"""
Portfolio ledger: durable PnL tracking and position state.

Writes to the pnl_ledger table and maintains an in-memory view for
latency-sensitive queries.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
import uuid

import sqlalchemy as sa
import structlog

from arb.db import session_scope

log = structlog.get_logger(__name__)

_T_LEDGER = sa.table(
    "pnl_ledger",
    sa.column("ts"), sa.column("strategy_id"), sa.column("event_type"),
    sa.column("market_id"), sa.column("amount"), sa.column("running_total"),
    sa.column("notes"),
)

_T_POSITIONS = sa.table(
    "positions",
    sa.column("ts"), sa.column("strategy_id"), sa.column("market_id"),
    sa.column("net_size"), sa.column("avg_entry_px"),
    sa.column("unrealized_pnl"), sa.column("realized_pnl"),
    sa.column("funding_accrued"),
)


class PortfolioLedger:
    """
    Thin async wrapper around the pnl_ledger table.
    Maintains running totals per strategy in memory.
    """

    def __init__(self) -> None:
        self._running: dict[str, float] = {}    # strategy_id -> running total

    async def record(
        self,
        strategy_id: str,
        event_type: str,
        amount: float,
        market_id: Optional[str] = None,
        notes: str = "",
    ) -> None:
        running = self._running.get(strategy_id, 0.0) + amount
        self._running[strategy_id] = running
        async with session_scope() as sess:
            await sess.execute(
                sa.insert(_T_LEDGER),
                [{
                    "ts": datetime.now(tz=timezone.utc),
                    "strategy_id": strategy_id,
                    "event_type": event_type,
                    "market_id": market_id,
                    "amount": amount,
                    "running_total": running,
                    "notes": notes,
                }],
            )

    async def upsert_position(
        self,
        strategy_id: str,
        market_id: str,
        net_size: float,
        avg_entry_px: float,
        unrealized_pnl: float,
        realized_pnl: float,
        funding_accrued: float,
    ) -> None:
        async with session_scope() as sess:
            # Use INSERT ... ON CONFLICT DO UPDATE for upsert
            stmt = sa.text("""
                INSERT INTO positions (ts, strategy_id, market_id, net_size, avg_entry_px,
                                       unrealized_pnl, realized_pnl, funding_accrued)
                VALUES (:ts, :strat, :mkt, :ns, :aep, :upnl, :rpnl, :fa)
                ON CONFLICT (strategy_id, market_id)
                DO UPDATE SET
                    ts = excluded.ts,
                    net_size = excluded.net_size,
                    avg_entry_px = excluded.avg_entry_px,
                    unrealized_pnl = excluded.unrealized_pnl,
                    realized_pnl = excluded.realized_pnl,
                    funding_accrued = excluded.funding_accrued
            """)
            await sess.execute(stmt, {
                "ts": datetime.now(tz=timezone.utc),
                "strat": strategy_id,
                "mkt": market_id,
                "ns": net_size,
                "aep": avg_entry_px,
                "upnl": unrealized_pnl,
                "rpnl": realized_pnl,
                "fa": funding_accrued,
            })

    def running_total(self, strategy_id: str) -> float:
        return self._running.get(strategy_id, 0.0)
