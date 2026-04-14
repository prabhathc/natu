"""
Hyperliquid REST + WebSocket client.

Covers:
  - /info endpoint (meta, allMids, funding, openInterest, L2Book, userState)
  - WebSocket subscriptions: l2Book, trades, allMids, activeAssetCtx
  - HIP-3 market enumeration

All timestamps from the exchange are milliseconds since epoch; we convert to
aware datetime objects immediately on ingestion.
"""

from __future__ import annotations

import asyncio
import time

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, AsyncIterator, Callable, Coroutine, Optional

import aiohttp
import orjson
import structlog
import websockets
from websockets.exceptions import ConnectionClosed

from arb.config import settings
from arb.market_data.models import (
    FundingStateEvent,
    MarketRegistry,
    MarketStateEvent,
    RawQuote,
    RawTrade,
)
from arb.market_data.normalizer import asset_class_from_symbol, venue_label_from_name

log = structlog.get_logger(__name__)


def _ts(ms: int | float | str) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


class HyperliquidClient:
    """Async client for Hyperliquid REST and WebSocket APIs."""

    def __init__(
        self,
        api_url: str = settings.hl_api_url,
        ws_url: str = settings.hl_ws_url,
    ) -> None:
        self._api_url = api_url.rstrip("/")
        self._ws_url = ws_url
        self._session: Optional[aiohttp.ClientSession] = None

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    async def _http(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                json_serialize=lambda o: orjson.dumps(o).decode(),
            )
        return self._session

    async def post(self, payload: dict) -> Any:
        session = await self._http()
        async with session.post(
            f"{self._api_url}/info",
            data=orjson.dumps(payload),
            headers={"Content-Type": "application/json"},
        ) as resp:
            resp.raise_for_status()
            return orjson.loads(await resp.read())

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ── REST queries ─────────────────────────────────────────────────────────

    async def get_meta(self) -> dict:
        """Fetch exchange metadata (all perp markets including HIP-3)."""
        return await self.post({"type": "meta"})

    async def get_meta_and_asset_ctxs(self) -> dict:
        """Fetch metadata + per-asset context (mark, OI, funding) in one call."""
        return await self.post({"type": "metaAndAssetCtxs"})

    async def get_all_mids(self) -> dict[str, str]:
        """Return mid prices keyed by market name."""
        return await self.post({"type": "allMids"})

    async def get_l2_book(self, coin: str, n_sig_figs: int = 5) -> dict:
        return await self.post({"type": "l2Book", "coin": coin, "nSigFigs": n_sig_figs})

    async def get_funding_history(self, coin: str, start_ms: int, end_ms: int | None = None) -> list:
        payload: dict = {"type": "fundingHistory", "coin": coin, "startTime": start_ms}
        if end_ms:
            payload["endTime"] = end_ms
        return await self.post(payload)

    async def get_trades(self, coin: str) -> list:
        return await self.post({"type": "trades", "coin": coin})

    async def get_spot_meta(self) -> dict:
        """Fetch spot token metadata (includes HIP-3 deployer tokens)."""
        return await self.post({"type": "spotMeta"})

    async def get_spot_meta_and_asset_ctxs(self) -> list:
        """Fetch spot metadata + per-asset context."""
        return await self.post({"type": "spotMetaAndAssetCtxs"})

    # ── Market registry builder ───────────────────────────────────────────────

    async def build_registry(self) -> list[MarketRegistry]:
        """
        Enumerate all markets: native perps + HIP-3 spot deployer markets.

        Architecture discovery:
        - Native perps: /info type=metaAndAssetCtxs → universe[]
        - HIP-3 deployer markets: /info type=spotMeta → tokens[] where
          deployerTradingFeeShare > 0. These trade as spot pairs (@N) on
          Hyperliquid's spot DEX with deployer-controlled oracle and fees.
        - trade[XYZ] (Wagyu.xyz): fullName contains "Wagyu.xyz"
        - Felix: fullName contains "Felix"
        """
        # Fetch both perp and spot universes concurrently
        perp_data, spot_data = await asyncio.gather(
            self.get_meta_and_asset_ctxs(),
            self.get_spot_meta(),
        )

        records: list[MarketRegistry] = []

        # ── Native perps ──────────────────────────────────────────────────────
        meta = perp_data[0] if isinstance(perp_data, list) else perp_data
        perp_universe: list[dict] = meta.get("universe", [])

        for asset in perp_universe:
            name: str = asset.get("name", "")
            if not name or asset.get("isDelisted"):
                continue

            asset_cls = asset_class_from_symbol(name)
            fee_mode = "standard"

            records.append(
                MarketRegistry(
                    market_id=f"hl-perp:{name}",
                    venue_label="hl_native",
                    deployer=None,
                    symbol=name,
                    asset_class=asset_cls,
                    collateral="USDC",
                    oracle_type="internal",
                    fee_mode=fee_mode,
                    max_leverage=Decimal(str(asset.get("maxLeverage", 20))),
                    session_notes="24/7 perp",
                    is_active=True,
                )
            )

        # ── HIP-3 spot deployer markets ───────────────────────────────────────
        tokens: list[dict] = spot_data.get("tokens", [])
        spot_pairs: list[dict] = spot_data.get("universe", [])

        # Build index: token_index -> token_info
        token_by_idx: dict[int, dict] = {t["index"]: t for t in tokens}

        # Build index: token_index -> spot_pair name
        pair_by_token: dict[int, str] = {}
        for pair in spot_pairs:
            pair_tokens = pair.get("tokens", [])
            if pair_tokens:
                pair_by_token[pair_tokens[0]] = pair.get("name", "")

        for token in tokens:
            fee_share = float(token.get("deployerTradingFeeShare", 0))
            if fee_share == 0:
                continue  # skip non-deployer tokens

            name: str = token.get("name", "")
            full_name: str = token.get("fullName") or ""
            if not name:
                continue

            venue = venue_label_from_name(name, full_name)
            asset_cls = asset_class_from_symbol(name)

            # Refine asset class from full_name for equity tokens
            if full_name and asset_cls == "crypto":
                fn_lower = full_name.lower()
                if any(w in fn_lower for w in ["tesla", "nvidia", "apple", "amazon", "google",
                                                "microsoft", "meta ", "spacex", "openai", "hood"]):
                    asset_cls = "equity"
                elif any(w in fn_lower for w in ["sp500", "spy", "qqq", "nasdaq", "s&p"]):
                    asset_cls = "index"
                elif any(w in fn_lower for w in ["gold", "silver", "xau", "oil"]):
                    asset_cls = "commodity"
                elif any(w in fn_lower for w in ["eur", "gbp", "jpy"]):
                    asset_cls = "fx"

            spot_pair_name = pair_by_token.get(token["index"], f"@{token['index']}")

            records.append(
                MarketRegistry(
                    market_id=f"hl-spot:{spot_pair_name}",
                    venue_label=venue,
                    deployer=full_name or None,
                    symbol=name,
                    asset_class=asset_cls,
                    collateral="USDC",
                    oracle_type="deployer",
                    fee_mode="deployer_share",
                    funding_formula="deployer_controlled",
                    session_notes=full_name or None,
                    is_active=True,
                )
            )

        # Deduplicate on market_id (same token can appear in multiple spot pairs)
        seen: dict[str, MarketRegistry] = {}
        for r in records:
            if r.market_id not in seen:
                seen[r.market_id] = r
        records = list(seen.values())

        log.info("registry_built", total=len(records), perps=len(perp_universe),
                 hip3=len([r for r in records if r.fee_mode == "deployer_share"]))
        return records

    # ── WebSocket subscriptions ───────────────────────────────────────────────

    async def stream_l2_books(
        self,
        coins: list[str],
        on_quote: Callable[[RawQuote], Coroutine],
    ) -> None:
        """Subscribe to L2 book updates for multiple coins and emit RawQuote events."""
        subs = [{"type": "l2Book", "coin": c} for c in coins]
        await self._ws_subscribe(subs, self._handle_l2, {"on_quote": on_quote})

    async def stream_trades(
        self,
        coins: list[str],
        on_trade: Callable[[RawTrade], Coroutine],
    ) -> None:
        subs = [{"type": "trades", "coin": c} for c in coins]
        await self._ws_subscribe(subs, self._handle_trades, {"on_trade": on_trade})

    async def stream_all_mids(
        self,
        on_state: Callable[[MarketStateEvent], Coroutine],
    ) -> None:
        subs = [{"type": "allMids"}]
        await self._ws_subscribe(subs, self._handle_all_mids, {"on_state": on_state})

    async def stream_active_asset_ctxs(
        self,
        coins: list[str],
        on_funding: Callable[[FundingStateEvent], Coroutine],
        on_state: Callable[[MarketStateEvent], Coroutine],
    ) -> None:
        subs = [{"type": "activeAssetCtx", "coin": c} for c in coins]
        await self._ws_subscribe(
            subs,
            self._handle_asset_ctx,
            {"on_funding": on_funding, "on_state": on_state},
        )

    # ── WS internals ─────────────────────────────────────────────────────────

    async def _ws_subscribe(
        self,
        subscriptions: list[dict],
        handler: Callable,
        ctx: dict,
        reconnect_delay: float = 2.0,
    ) -> None:
        while True:
            try:
                async with websockets.connect(
                    self._ws_url,
                    ping_interval=20,
                    ping_timeout=30,
                ) as ws:
                    for sub in subscriptions:
                        await ws.send(orjson.dumps({"method": "subscribe", "subscription": sub}).decode())

                    async for raw in ws:
                        msg = orjson.loads(raw)
                        await handler(msg, ctx)

            except ConnectionClosed as e:
                log.warning("ws_disconnected", reason=str(e), delay=reconnect_delay)
                await asyncio.sleep(reconnect_delay)
            except Exception as e:
                log.error("ws_error", error=str(e), delay=reconnect_delay)
                await asyncio.sleep(reconnect_delay)

    @staticmethod
    async def _handle_l2(msg: dict, ctx: dict) -> None:
        if msg.get("channel") != "l2Book":
            return
        data = msg.get("data", {})
        coin = data.get("coin", "")
        ts = _ts(data.get("time", time.time() * 1000))
        levels = data.get("levels", [[], []])
        bids, asks = levels[0], levels[1]
        if not bids or not asks:
            return
        best_bid = bids[0]
        best_ask = asks[0]
        quote = RawQuote(
            ts=ts,
            market_id=f"hl-perp:{coin}",
            bid_px=Decimal(str(best_bid["px"])),
            bid_sz=Decimal(str(best_bid["sz"])),
            ask_px=Decimal(str(best_ask["px"])),
            ask_sz=Decimal(str(best_ask["sz"])),
        )
        await ctx["on_quote"](quote)

    @staticmethod
    async def _handle_trades(msg: dict, ctx: dict) -> None:
        if msg.get("channel") != "trades":
            return
        for t in msg.get("data", []):
            trade = RawTrade(
                ts=_ts(t["time"]),
                market_id=f"hl-perp:{t['coin']}",
                trade_id=str(t.get("tid", "")),
                price=Decimal(str(t["px"])),
                size=Decimal(str(t["sz"])),
                side="buy" if t["side"] == "B" else "sell",
                is_liquidation=t.get("liquidation") is not None,
            )
            await ctx["on_trade"](trade)

    @staticmethod
    async def _handle_all_mids(msg: dict, ctx: dict) -> None:
        if msg.get("channel") != "allMids":
            return
        ts = datetime.now(tz=timezone.utc)
        for coin, mid in msg.get("data", {}).get("mids", {}).items():
            state = MarketStateEvent(
                ts=ts,
                market_id=f"hl-perp:{coin}",
                mark_px=Decimal(str(mid)),
            )
            await ctx["on_state"](state)

    @staticmethod
    async def _handle_asset_ctx(msg: dict, ctx: dict) -> None:
        if msg.get("channel") != "activeAssetCtx":
            return
        data = msg["data"]
        coin = data.get("coin", "")
        ctx_data = data.get("ctx", {})
        ts = datetime.now(tz=timezone.utc)

        funding = FundingStateEvent(
            ts=ts,
            market_id=f"hl-perp:{coin}",
            funding_rate=Decimal(str(ctx_data.get("funding", "0"))),
            predicted_rate=Decimal(str(ctx_data.get("predictedFunding", ctx_data.get("funding", "0")))),
        )
        await ctx["on_funding"](funding)

        state = MarketStateEvent(
            ts=ts,
            market_id=f"hl-perp:{coin}",
            mark_px=Decimal(str(ctx_data["markPx"])) if ctx_data.get("markPx") else None,
            oracle_px=Decimal(str(ctx_data["oraclePx"])) if ctx_data.get("oraclePx") else None,
            open_interest=Decimal(str(ctx_data["openInterest"])) if ctx_data.get("openInterest") else None,
            day_volume=Decimal(str(ctx_data["dayNtlVlm"])) if ctx_data.get("dayNtlVlm") else None,
        )
        await ctx["on_state"](state)
