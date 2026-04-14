"""
Unit tests for market_data.client.build_registry.

Tests use synthetic API payloads to cover:
- Correct venue classification
- The @N index collision bug (token.index == pair @N number)
- Tokens with no active spot pair are skipped
- Delisted perps are skipped
- Deduplication on market_id
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from arb.market_data.client import HyperliquidClient
from arb.market_data.models import MarketRegistry


class TestBuildRegistry:
    @pytest.fixture
    def client(self):
        return HyperliquidClient(api_url="http://fake", ws_url="ws://fake")

    async def _build(self, client, perp_data, spot_data) -> list[MarketRegistry]:
        with (
            patch.object(client, "get_meta_and_asset_ctxs", new=AsyncMock(return_value=perp_data)),
            patch.object(client, "get_spot_meta", new=AsyncMock(return_value=spot_data)),
        ):
            return await client.build_registry()

    # ── Perp universe ─────────────────────────────────────────────────────────

    async def test_native_perps_included(self, client, fake_perp_meta, fake_spot_meta):
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        market_ids = {r.market_id for r in records}
        assert "hl-perp:BTC" in market_ids
        assert "hl-perp:ETH" in market_ids
        assert "hl-perp:SPX" in market_ids

    async def test_delisted_perp_excluded(self, client, fake_perp_meta, fake_spot_meta):
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        assert not any(r.market_id == "hl-perp:DELISTED" for r in records)

    async def test_native_perp_venue_label(self, client, fake_perp_meta, fake_spot_meta):
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        btc = next(r for r in records if r.market_id == "hl-perp:BTC")
        assert btc.venue_label == "hl_native"
        assert btc.asset_class == "crypto"

    async def test_spx_classified_as_index(self, client, fake_perp_meta, fake_spot_meta):
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        spx = next(r for r in records if r.market_id == "hl-perp:SPX")
        assert spx.asset_class == "index"

    # ── Spot / HIP-3 universe ─────────────────────────────────────────────────

    async def test_tsla_classified_as_xyz(self, client, fake_perp_meta, fake_spot_meta):
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        tsla = next((r for r in records if r.symbol == "TSLA"), None)
        assert tsla is not None
        assert tsla.venue_label == "xyz"
        assert tsla.market_id == "hl-spot:@5"

    async def test_aapl_classified_as_felix(self, client, fake_perp_meta, fake_spot_meta):
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        aapl = next((r for r in records if r.symbol == "AAPL"), None)
        assert aapl is not None
        assert aapl.venue_label == "felix"
        assert aapl.market_id == "hl-spot:@50"

    async def test_zero_fee_share_tokens_excluded(self, client, fake_perp_meta, fake_spot_meta):
        """USDC and PURR have feeShare=0 → must not appear in registry."""
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        symbols = {r.symbol for r in records}
        assert "USDC" not in symbols
        assert "PURR" not in symbols

    async def test_token_without_active_pair_excluded(self, client, fake_perp_meta, fake_spot_meta):
        """NOPAIR token has feeShare>0 but no spot pair → must be skipped."""
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        assert not any(r.symbol == "NOPAIR" for r in records)

    async def test_index_collision_bug_not_present(self, client, fake_perp_meta, fake_spot_meta):
        """
        Regression test for the @N index collision bug.

        COLLISION token has index=50. If we used fallback f"@{token.index}"
        it would generate market_id="hl-spot:@50" — same as AAPL's real pair.
        The fix skips tokens with no entry in pair_by_token instead of falling back.
        """
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        # COLLISION token should be absent (no active pair)
        assert not any(r.symbol == "COLLISION" for r in records)
        # AAPL should be present with the correct market_id
        aapl = next((r for r in records if r.symbol == "AAPL"), None)
        assert aapl is not None
        assert aapl.market_id == "hl-spot:@50"

    async def test_no_duplicate_market_ids(self, client, fake_perp_meta, fake_spot_meta):
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        market_ids = [r.market_id for r in records]
        assert len(market_ids) == len(set(market_ids)), "Duplicate market_ids found"

    async def test_spot_fee_mode(self, client, fake_perp_meta, fake_spot_meta):
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        tsla = next(r for r in records if r.symbol == "TSLA")
        assert tsla.fee_mode == "deployer_share"
        assert tsla.oracle_type == "deployer"

    async def test_perp_max_leverage(self, client, fake_perp_meta, fake_spot_meta):
        records = await self._build(client, fake_perp_meta, fake_spot_meta)
        btc = next(r for r in records if r.market_id == "hl-perp:BTC")
        assert btc.max_leverage == Decimal("50")
