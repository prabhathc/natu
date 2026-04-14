"""
Tests for market_data.normalizer.

The normalizer is a critical classification component — it determines which
venue/asset-class each HIP-3 token belongs to. Regressions here cause wrong
registry entries which corrupts all downstream analysis.
"""

from __future__ import annotations

import pytest

from arb.market_data.normalizer import (
    AssetClass,
    asset_class_from_symbol,
    normalize_symbol,
    venue_label_from_name,
)


class TestNormalizeSymbol:
    def test_uppercase(self):
        assert normalize_symbol("btc") == "BTC"

    def test_strips_perp_suffix(self):
        assert normalize_symbol("ETH-PERP") == "ETH"

    def test_strips_usd_suffix(self):
        # The regex strips USD at end-of-string, so BTCUSD → BTC
        assert normalize_symbol("BTCUSD") == "BTC"

    def test_strips_felix_prefix(self):
        assert normalize_symbol("felix-AAPL") == "AAPL"

    def test_no_change_clean_symbol(self):
        assert normalize_symbol("SPX") == "SPX"


class TestAssetClassFromSymbol:
    @pytest.mark.parametrize("symbol,expected", [
        ("XAU", "commodity"),
        ("GOLD", "commodity"),
        ("OIL", "commodity"),
        ("XAG", "commodity"),
        ("SPX", "index"),
        ("QQQ", "index"),
        ("SPY", "index"),
        ("NDX", "index"),
        ("EURUSD", "fx"),
        ("DXY", "fx"),
        ("BTC", "crypto"),
        ("ETH", "crypto"),
        ("SOL", "crypto"),
        ("HYPE", "crypto"),
    ])
    def test_known_symbols(self, symbol: str, expected: AssetClass):
        assert asset_class_from_symbol(symbol) == expected

    def test_uppercase_equity_heuristic(self):
        # 1-5 uppercase letters not in any known set → equity
        assert asset_class_from_symbol("AAPL") == "equity"
        assert asset_class_from_symbol("MSFT") == "equity"
        assert asset_class_from_symbol("GOOGL") == "equity"

    def test_default_crypto_for_unknown(self):
        # Longer symbols or unknown → crypto default
        assert asset_class_from_symbol("FARTCOIN") == "crypto"


class TestVenueLabelFromName:
    # ── Felix tokens (identified by evmContract + blank fullName) ─────────────
    @pytest.mark.parametrize("name", [
        "AAPL", "GOOGL", "AMZN", "META", "MSFT",
        "SPY", "QQQ", "GLD", "SLV", "HOOD", "BNB1", "QQQM",
    ])
    def test_felix_by_name_set(self, name: str):
        assert venue_label_from_name(name) == "felix"

    def test_felix_by_full_name(self):
        assert venue_label_from_name("SOMETOKEN", full_name="Felix Protocol Apple") == "felix"

    # ── XYZ (Wagyu.xyz / trade[XYZ]) ─────────────────────────────────────────
    @pytest.mark.parametrize("name", ["TSLA", "NVDA", "SPACEX", "XMR1", "TAO1", "TRADE"])
    def test_xyz_by_name_set(self, name: str):
        assert venue_label_from_name(name) == "xyz"

    def test_xyz_by_full_name_wagyu(self):
        assert venue_label_from_name("TSLA", full_name="Tesla - Wagyu.xyz") == "xyz"

    def test_xyz_by_full_name_trade_fun(self):
        assert venue_label_from_name("UNKNOWN", full_name="trade.fun token") == "xyz"

    # ── Unit Protocol ─────────────────────────────────────────────────────────
    def test_unit_protocol(self):
        assert venue_label_from_name("UBTC", full_name="Unit UBTC") == "unit"

    # ── Melt ──────────────────────────────────────────────────────────────────
    def test_melt_unknown_symbol(self):
        # A token not in any known name set, but fullName identifies it as Melt
        assert venue_label_from_name("MELTX", full_name="Melt Protocol Token") == "melt"

    def test_felix_name_set_beats_fullname_melt(self):
        # QQQM is in _FELIX_TOKENS; even if fullName suggests Melt, felix wins
        # (name-set check is authoritative; fullName is secondary)
        assert venue_label_from_name("QQQM", full_name="Melt QQQ Monthly") == "felix"

    # ── Native fallback ───────────────────────────────────────────────────────
    def test_native_fallback(self):
        assert venue_label_from_name("UNKNOWN_TOKEN") == "hl_native"

    # ── Priority: XYZ overrides fullName Felix mention ─────────────────────
    def test_xyz_name_set_takes_priority_over_fullname(self):
        # TSLA is in _XYZ_TOKENS; a fullName with "felix" text would be odd but XYZ wins
        result = venue_label_from_name("TSLA", full_name="some felix mention")
        # fullName "felix" match comes AFTER XYZ name-set check in code, but fullName
        # "wagyu" check runs first — so "felix" in fullName would win unless name is in XYZ set first.
        # Current implementation: wagyu/trade checks → XYZ set → felix string → felix set → others
        # TSLA is in _XYZ_TOKENS so it should return xyz
        assert result == "xyz"

    # ── NVDA edge case: same token appears on Felix UI but is Wagyu's token ─
    def test_nvda_classified_as_xyz(self):
        assert venue_label_from_name("NVDA") == "xyz"

    # ── Case sensitivity ──────────────────────────────────────────────────────
    def test_case_insensitive_fullname(self):
        assert venue_label_from_name("X", full_name="WAGYU.XYZ TOKEN") == "xyz"
