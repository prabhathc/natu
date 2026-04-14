"""
Phase 1: Continuous market data collector.

Subscribes to all tracked markets via WebSocket and persists:
  - top-of-book quotes
  - trades
  - mark price / OI / oracle price
  - funding rates
  - latency metrics

Gap detection runs every minute and logs any data holes.

Usage:
    python -m arb.scripts.collect
    arb-collect
    arb-collect --markets XAU,XAG --flush-interval 2
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

import structlog
import typer

from arb.logging_config import configure_logging
from arb.market_data.client import HyperliquidClient
from arb.market_data.models import (
    FundingStateEvent,
    MarketStateEvent,
    RawQuote,
    RawTrade,
)
from arb.market_data.store import EventStore
from arb.signals.features import FeatureEngine

log = structlog.get_logger(__name__)
app = typer.Typer()


class Collector:
    def __init__(
        self,
        coins: list[str],
        flush_interval: float = 1.0,
    ) -> None:
        self.coins = coins
        self.client = HyperliquidClient()
        self.store = EventStore(flush_interval_s=flush_interval)
        self.features = FeatureEngine()

        # Gap detection: track last seen ts per (market, data_type)
        self._last_seen: dict[tuple[str, str], float] = {}
        self._gap_threshold_s = 60.0

    async def run(self) -> None:
        await self.store.start()
        await self.features.start()

        tasks = [
            asyncio.create_task(
                self.client.stream_l2_books(self.coins, self._on_quote),
                name="l2_books",
            ),
            asyncio.create_task(
                self.client.stream_trades(self.coins, self._on_trade),
                name="trades",
            ),
            asyncio.create_task(
                self.client.stream_active_asset_ctxs(
                    self.coins,
                    on_funding=self._on_funding,
                    on_state=self._on_state,
                ),
                name="asset_ctxs",
            ),
            asyncio.create_task(self._gap_monitor(), name="gap_monitor"),
            asyncio.create_task(self._stats_logger(), name="stats"),
        ]

        log.info("collector_started", coins=len(self.coins))
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            await self.store.stop()
            await self.features.stop()
            await self.client.close()
            log.info("collector_stopped")

    # ── Callbacks ─────────────────────────────────────────────────────────────

    async def _on_quote(self, q: RawQuote) -> None:
        recv_ts = time.time()
        self._last_seen[(q.market_id, "quotes")] = recv_ts
        await self.store.add_quote(q)
        await self.features.on_quote(q)

    async def _on_trade(self, t: RawTrade) -> None:
        recv_ts = time.time()
        self._last_seen[(t.market_id, "trades")] = recv_ts
        await self.store.add_trade(t)
        await self.features.on_trade(t)

    async def _on_funding(self, f: FundingStateEvent) -> None:
        self._last_seen[(f.market_id, "funding")] = time.time()
        await self.store.add_funding(f)
        await self.features.on_funding(f)

    async def _on_state(self, s: MarketStateEvent) -> None:
        await self.store.add_market_state(s)
        await self.features.on_market_state(s)

    # ── Gap monitor ───────────────────────────────────────────────────────────

    async def _gap_monitor(self) -> None:
        while True:
            await asyncio.sleep(60)
            now = time.time()
            for (market_id, data_type), last_ts in list(self._last_seen.items()):
                gap_s = now - last_ts
                if gap_s > self._gap_threshold_s:
                    gap_start = datetime.fromtimestamp(last_ts, tz=timezone.utc)
                    log.warning(
                        "data_gap_detected",
                        market=market_id,
                        data_type=data_type,
                        gap_s=gap_s,
                    )
                    await self.store.log_gap(market_id, data_type, gap_start)

    # ── Stats logger ──────────────────────────────────────────────────────────

    async def _stats_logger(self) -> None:
        while True:
            await asyncio.sleep(300)   # every 5 min
            total_markets = len(set(m for m, _ in self._last_seen))
            log.info("collector_stats", markets_active=total_markets, coins=len(self.coins))


async def _run(coins: list[str], flush_interval: float) -> None:
    configure_logging()

    if not coins:
        # Fetch full universe from registry or API
        client = HyperliquidClient()
        try:
            mids = await client.get_all_mids()
            coins = list(mids.keys())
            log.info("auto_discovered_markets", count=len(coins))
        finally:
            await client.close()

    collector = Collector(coins=coins, flush_interval=flush_interval)
    await collector.run()


@app.command()
def main(
    markets: str = typer.Option("", help="Comma-separated coin list (empty = all)"),
    flush_interval: float = typer.Option(1.0, help="Flush interval in seconds"),
) -> None:
    """Start the market data collector."""
    coin_list = [c.strip() for c in markets.split(",") if c.strip()]
    asyncio.run(_run(coin_list, flush_interval))


if __name__ == "__main__":
    app()
