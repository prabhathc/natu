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
from decimal import Decimal

import aiohttp
import structlog
import typer

from arb.logging_config import configure_logging
from arb.market_data.client import HyperliquidClient
from arb.market_data.models import (
    FundingStateEvent,
    MarketStateEvent,
    ReferenceStateEvent,
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
        market_ids: list[str],
        flush_interval: float = 1.0,
        reference_symbols: list[str] | None = None,
        reference_poll_s: float = 60.0,
    ) -> None:
        self.market_ids = market_ids
        self.coins = [m.split(":", 1)[1] for m in market_ids]
        self.perp_coins = [c for c in self.coins if not c.startswith("@")]
        self.client = HyperliquidClient()
        self.store = EventStore(flush_interval_s=flush_interval)
        self.features = FeatureEngine()
        self.reference_symbols = [s.upper() for s in (reference_symbols or [])]
        self.reference_poll_s = max(5.0, reference_poll_s)

        # Gap detection: track last seen ts per (market, data_type)
        self._last_seen: dict[tuple[str, str], float] = {}
        self._gap_threshold_s = 60.0

    async def run(self) -> None:
        await self.store.start()
        await self.features.start()

        tasks = [
            asyncio.create_task(self._gap_monitor(), name="gap_monitor"),
            asyncio.create_task(self._stats_logger(), name="stats"),
        ]
        if self.coins:
            tasks.append(
                asyncio.create_task(
                    self.client.stream_l2_books(self.coins, self._on_quote),
                    name="l2_books",
                )
            )
            tasks.append(
                asyncio.create_task(
                    self.client.stream_trades(self.coins, self._on_trade),
                    name="trades",
                )
            )
        if self.perp_coins:
            tasks.append(
                asyncio.create_task(
                    self.client.stream_active_asset_ctxs(
                        self.perp_coins,
                        on_funding=self._on_funding,
                        on_state=self._on_state,
                    ),
                    name="asset_ctxs",
                )
            )
        if self.reference_symbols:
            tasks.append(asyncio.create_task(self._reference_loop(), name="reference_prices"))

        log.info(
            "collector_started",
            markets=len(self.market_ids),
            coins=len(self.coins),
            perps=len(self.perp_coins),
            spots=len(self.coins) - len(self.perp_coins),
            references=len(self.reference_symbols),
        )
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
            log.info("collector_stats", markets_active=total_markets, configured_markets=len(self.market_ids))

    # ── Reference feed ─────────────────────────────────────────────────────────

    async def _reference_loop(self) -> None:
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    for symbol in self.reference_symbols:
                        price = await _fetch_reference_price(session, symbol)
                        if price is None:
                            continue
                        evt = ReferenceStateEvent(
                            ts=datetime.now(tz=timezone.utc),
                            symbol=symbol,
                            price=Decimal(str(price)),
                            source="stooq",
                        )
                        await self.store.add_reference(evt)
                        self._last_seen[(symbol, "reference")] = time.time()
            except Exception as e:
                log.warning("reference_feed_error", error=str(e))
            await asyncio.sleep(self.reference_poll_s)


_STOOQ_TICKER_MAP = {
    "SPX": "^SPX",
    "XAU": "XAUUSD",
    "GOLD": "XAUUSD",
    "TSLA": "TSLA.US",
    "NVDA": "NVDA.US",
    "AAPL": "AAPL.US",
    "QQQ": "QQQ.US",
}


async def _fetch_reference_price(session: aiohttp.ClientSession, symbol: str) -> float | None:
    ticker = _STOOQ_TICKER_MAP.get(symbol.upper())
    if not ticker:
        return None
    url = f"https://stooq.com/q/l/?s={ticker}&f=sd2t2ohlcv&h&e=csv"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        resp.raise_for_status()
        text = await resp.text()
    # Header: Symbol,Date,Time,Open,High,Low,Close,Volume
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return None
    cols = [c.strip() for c in lines[1].split(",")]
    if len(cols) < 7 or cols[6] in {"", "N/D"}:
        return None
    return float(cols[6])


async def _resolve_market_ids(requested: list[str], client: HyperliquidClient) -> list[str]:
    registry = await client.build_registry()
    by_symbol: dict[str, list[str]] = {}
    for r in registry:
        by_symbol.setdefault(r.symbol.upper(), []).append(r.market_id)

    if not requested:
        return [r.market_id for r in registry if r.is_active]

    resolved: list[str] = []
    missing: list[str] = []
    for raw in requested:
        token = raw.strip()
        if not token:
            continue
        if token.startswith("hl-perp:") or token.startswith("hl-spot:"):
            resolved.append(token)
            continue
        if token.startswith("@"):
            resolved.append(f"hl-spot:{token}")
            continue
        matches = by_symbol.get(token.upper(), [])
        if matches:
            resolved.extend(matches)
        else:
            resolved.append(f"hl-perp:{token.upper()}")
            missing.append(token)

    deduped = list(dict.fromkeys(resolved))
    if missing:
        log.warning("markets_not_in_registry_using_perp_fallback", tokens=missing)
    return deduped


async def _run(markets: list[str], flush_interval: float, reference_symbols: list[str], reference_poll_s: float) -> None:
    configure_logging()

    client = HyperliquidClient()
    try:
        market_ids = await _resolve_market_ids(markets, client)
    finally:
        await client.close()

    collector = Collector(
        market_ids=market_ids,
        flush_interval=flush_interval,
        reference_symbols=reference_symbols,
        reference_poll_s=reference_poll_s,
    )
    await collector.run()


@app.command()
def main(
    markets: str = typer.Option("", help="Comma-separated symbols/market ids (empty = all registry markets)"),
    references: str = typer.Option("SPX,XAU,TSLA,NVDA", help="Comma-separated external reference symbols"),
    reference_poll_s: float = typer.Option(60.0, help="External reference poll interval in seconds"),
    flush_interval: float = typer.Option(1.0, help="Flush interval in seconds"),
) -> None:
    """Start the market data collector."""
    market_list = [c.strip() for c in markets.split(",") if c.strip()]
    ref_list = [s.strip().upper() for s in references.split(",") if s.strip()]
    asyncio.run(_run(market_list, flush_interval, ref_list, reference_poll_s))


if __name__ == "__main__":
    app()
