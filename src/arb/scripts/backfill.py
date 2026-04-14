"""
API pull-mode backfill.

Best-effort historical bootstrap from REST endpoints before or alongside
continuous websocket collection.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import structlog
import typer

from arb.logging_config import configure_logging
from arb.market_data.client import HyperliquidClient, _market_id_for_coin
from arb.market_data.models import FundingStateEvent, MarketStateEvent, RawTrade
from arb.market_data.store import EventStore
from arb.scripts.collect import _resolve_market_ids

app = typer.Typer()
log = structlog.get_logger(__name__)


def _coin_from_market_id(market_id: str) -> str:
    return market_id.split(":", 1)[1]


async def _backfill_trades(client: HyperliquidClient, store: EventStore, market_ids: list[str], max_trades: int) -> int:
    inserted = 0
    endpoint_unavailable = False
    for market_id in market_ids:
        if endpoint_unavailable:
            break
        coin = _coin_from_market_id(market_id)
        try:
            rows = await client.get_trades(coin)
        except Exception as e:
            err = str(e)
            if "422" in err:
                # Current API behavior returns 422 for this trades REST payload.
                # Keep backfill useful by continuing with funding + mark snapshots.
                log.warning("backfill_trades_endpoint_unavailable", error=err)
                endpoint_unavailable = True
                break
            log.warning("backfill_trades_failed", market_id=market_id, error=err)
            continue
        for t in rows[:max_trades]:
            trade = RawTrade(
                ts=datetime.fromtimestamp(int(t["time"]) / 1000, tz=timezone.utc),
                market_id=market_id,
                trade_id=str(t.get("tid", "")),
                price=Decimal(str(t["px"])),
                size=Decimal(str(t["sz"])),
                side="buy" if t.get("side") == "B" else "sell",
                is_liquidation=t.get("liquidation") is not None,
            )
            await store.add_trade(trade)
            inserted += 1
    return inserted


async def _backfill_funding(
    client: HyperliquidClient,
    store: EventStore,
    perp_market_ids: list[str],
    funding_days: int,
) -> int:
    inserted = 0
    end_ms = int(time.time() * 1000)
    start_ms = int((datetime.now(tz=timezone.utc) - timedelta(days=funding_days)).timestamp() * 1000)
    for market_id in perp_market_ids:
        coin = _coin_from_market_id(market_id)
        try:
            rows = await client.get_funding_history(coin, start_ms=start_ms, end_ms=end_ms)
        except Exception as e:
            log.warning("backfill_funding_failed", market_id=market_id, error=str(e))
            continue
        for r in rows:
            ts_ms = r.get("time") or r.get("fundingTime")
            if ts_ms is None:
                continue
            evt = FundingStateEvent(
                ts=datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc),
                market_id=market_id,
                funding_rate=Decimal(str(r.get("fundingRate", r.get("funding", "0")))),
                predicted_rate=Decimal(str(r.get("predictedFundingRate"))) if r.get("predictedFundingRate") else None,
            )
            await store.add_funding(evt)
            inserted += 1
    return inserted


async def _backfill_mark_snapshot(client: HyperliquidClient, store: EventStore, market_ids: list[str]) -> int:
    inserted = 0
    try:
        mids = await client.get_all_mids()
    except Exception as e:
        log.warning("backfill_mids_failed", error=str(e))
        return 0
    now = datetime.now(tz=timezone.utc)
    for market_id in market_ids:
        coin = _coin_from_market_id(market_id)
        mid = mids.get(coin)
        if mid is None:
            continue
        evt = MarketStateEvent(
            ts=now,
            market_id=market_id,
            mark_px=Decimal(str(mid)),
        )
        await store.add_market_state(evt)
        inserted += 1
    return inserted


async def _run(markets: list[str], funding_days: int, max_trades: int, flush_interval: float) -> None:
    configure_logging()
    client = HyperliquidClient()
    store = EventStore(flush_interval_s=flush_interval)
    await store.start()
    try:
        market_ids = await _resolve_market_ids(markets, client)
        perp_market_ids = [m for m in market_ids if m.startswith("hl-perp:")]
        log.info(
            "backfill_start",
            markets=len(market_ids),
            perps=len(perp_market_ids),
            spots=len(market_ids) - len(perp_market_ids),
            funding_days=funding_days,
            max_trades=max_trades,
        )
        trades_n = await _backfill_trades(client, store, market_ids, max_trades=max_trades)
        funding_n = await _backfill_funding(client, store, perp_market_ids, funding_days=funding_days)
        mstate_n = await _backfill_mark_snapshot(client, store, market_ids)
        await store.flush()
        log.info("backfill_done", trades=trades_n, funding=funding_n, mark_state=mstate_n)
    finally:
        await store.stop()
        await client.close()


@app.command()
def main(
    markets: str = typer.Option("", help="Comma-separated symbols/market ids (empty = all registry markets)"),
    funding_days: int = typer.Option(7, help="How many days of funding history to backfill (perps)"),
    max_trades: int = typer.Option(2000, help="Max recent trades per market to ingest"),
    flush_interval: float = typer.Option(1.0, help="Flush interval seconds"),
) -> None:
    """Backfill recent trades/funding/mark snapshots from REST API."""
    market_list = [c.strip() for c in markets.split(",") if c.strip()]
    asyncio.run(_run(market_list, funding_days, max_trades, flush_interval))


if __name__ == "__main__":
    app()

