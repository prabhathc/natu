from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, patch

from arb.market_data.client import HyperliquidClient, _market_id_for_coin
from arb.market_data.models import MarketRegistry
from arb.scripts.collect import _resolve_market_ids


def test_market_id_for_coin_perp_and_spot() -> None:
    assert _market_id_for_coin("BTC") == "hl-perp:BTC"
    assert _market_id_for_coin("@264") == "hl-spot:@264"


async def test_resolve_market_ids_symbol_expands_to_spot_and_perp() -> None:
    client = HyperliquidClient(api_url="http://fake", ws_url="ws://fake")
    registry = [
        MarketRegistry(
            market_id="hl-perp:SPX",
            venue_label="hl_native",
            symbol="SPX",
            asset_class="index",
            collateral="USDC",
            max_leverage=Decimal("10"),
        ),
        MarketRegistry(
            market_id="hl-spot:@279",
            venue_label="xyz",
            symbol="SPX",
            asset_class="index",
            collateral="USDC",
        ),
    ]
    with patch.object(client, "build_registry", new=AsyncMock(return_value=registry)):
        resolved = await _resolve_market_ids(["SPX"], client)
    assert resolved == ["hl-perp:SPX", "hl-spot:@279"]


async def test_resolve_market_ids_preserves_explicit_ids() -> None:
    client = HyperliquidClient(api_url="http://fake", ws_url="ws://fake")
    registry = [
        MarketRegistry(
            market_id="hl-perp:BTC",
            venue_label="hl_native",
            symbol="BTC",
            asset_class="crypto",
            collateral="USDC",
            max_leverage=Decimal("25"),
        ),
    ]
    with patch.object(client, "build_registry", new=AsyncMock(return_value=registry)):
        resolved = await _resolve_market_ids(["hl-spot:@264", "@265"], client)
    assert resolved == ["hl-spot:@264", "hl-spot:@265"]
