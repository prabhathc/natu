"""Shared pytest fixtures."""

from __future__ import annotations

import pytest


# ─── Synthetic spot meta fixtures ────────────────────────────────────────────

@pytest.fixture
def fake_spot_meta():
    """
    Minimal spot meta payload that exercises all three bug categories:
    1. Felix tokens with evmContract + blank fullName
    2. Tokens whose index == a pair's @N number (collision vector)
    3. Tokens with no active spot pair (should be skipped)
    """
    tokens = [
        # USDC (base quote currency, evmContract, should be filtered by feeShare=0)
        {"index": 0, "name": "USDC", "deployerTradingFeeShare": 0, "evmContract": True, "fullName": ""},
        # Non-deployer token (feeShare=0, should be skipped)
        {"index": 1, "name": "PURR", "deployerTradingFeeShare": 0, "evmContract": None, "fullName": ""},
        # Wagyu/XYZ token with fullName
        {"index": 10, "name": "TSLA", "deployerTradingFeeShare": 0.8, "evmContract": None,
         "fullName": "Tesla - Wagyu.xyz"},
        # Felix token: evmContract + blank fullName
        {"index": 100, "name": "AAPL", "deployerTradingFeeShare": 1.0, "evmContract": True, "fullName": None},
        # Felix token that would collide if fallback @N were used (token index = 50 == pair @50 name)
        {"index": 50, "name": "COLLISION", "deployerTradingFeeShare": 1.0, "evmContract": True,
         "fullName": None},
        # Token with no active spot pair — must be SKIPPED
        {"index": 999, "name": "NOPAIR", "deployerTradingFeeShare": 1.0, "evmContract": None,
         "fullName": "No Pair Token"},
    ]
    universe = [
        # TSLA pair: token 10 is first
        {"name": "@5", "tokens": [10, 0]},
        # AAPL pair: token 100 is first
        {"name": "@50", "tokens": [100, 0]},
        # A different pair that coincidentally is named @50 but uses token index 50
        # — this is the collision: if we used fallback f"@{token.index}" for COLLISION (idx=50),
        #   it would match the @50 pair name which belongs to AAPL. We avoid this by
        #   skipping tokens not found in pair_by_token.
        # NOTE: COLLISION token (idx=50) is NOT the first token of any pair here,
        # so pair_by_token.get(50) = None → skip it correctly.
    ]
    return {"tokens": tokens, "universe": universe}


@pytest.fixture
def fake_perp_meta():
    """Minimal perp meta response."""
    return [
        {
            "universe": [
                {"name": "BTC", "maxLeverage": 50, "isDelisted": False},
                {"name": "ETH", "maxLeverage": 50, "isDelisted": False},
                {"name": "SPX", "maxLeverage": 20, "isDelisted": False},
                {"name": "DELISTED", "maxLeverage": 10, "isDelisted": True},
            ]
        }
    ]
