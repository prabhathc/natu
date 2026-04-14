"""Symbol normalization and asset-class heuristics for HIP-3 markets."""

from __future__ import annotations

import re
from typing import Literal

AssetClass = Literal["commodity", "index", "equity", "fx", "crypto"]

# ─── Venue prefixes / suffixes used by known HIP-3 deployers ─────────────────
_VENUE_STRIP_RE = re.compile(
    r"^(?:xyz[-_]?|felix[-_]?)|(?:[-_]?perp|[-_]?usd|[-_]?usdc)$",
    re.IGNORECASE,
)

# ─── Known commodity tickers ──────────────────────────────────────────────────
_COMMODITIES = {
    "XAU", "GOLD",
    "XAG", "SILVER",
    "OIL", "WTI", "BRENT", "CL",
    "NG", "NATGAS",
    "HG", "COPPER",
    "WHEAT", "CORN", "SOYBEAN",
    "COTTON",
}

# ─── Known equity-index tickers ───────────────────────────────────────────────
_INDICES = {
    "SPX", "SP500", "SPY",
    "NDX", "NQ", "QQQ",
    "DJI", "DOW", "YM",
    "FTSE", "DAX", "CAC",
    "NIKKEI", "N225",
    "HSI",
    "VIX",
    "RUT", "IWM",
}

# ─── Known FX pairs ───────────────────────────────────────────────────────────
_FX = {
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF",
    "AUDUSD", "NZDUSD", "USDCAD",
    "EURJPY", "GBPJPY",
    "DXY",
}

# ─── Well-known crypto base assets ───────────────────────────────────────────
_CRYPTO = {
    "BTC", "ETH", "SOL", "ARB", "HYPE", "PURR",
    "AVAX", "BNB", "MATIC", "OP",
}


def normalize_symbol(raw: str) -> str:
    """Strip venue prefixes/suffixes and upper-case."""
    cleaned = _VENUE_STRIP_RE.sub("", raw).upper().strip("-_")
    return cleaned


def asset_class_from_symbol(symbol: str) -> AssetClass:
    """Best-effort asset class from normalized ticker."""
    # Check raw symbol first: normalization strips USD suffix which breaks FX pair detection
    upper = symbol.upper()
    if upper in _FX:
        return "fx"
    s = normalize_symbol(symbol)
    if s in _COMMODITIES:
        return "commodity"
    if s in _INDICES:
        return "index"
    if s in _FX:
        return "fx"
    if s in _CRYPTO:
        return "crypto"
    # Heuristic: looks like an equity ticker (1–5 uppercase letters, no digits)
    if re.fullmatch(r"[A-Z]{1,5}", s):
        return "equity"
    return "crypto"  # default for unknown HIP-3 markets


# Tokens treated as Felix in registry classification.
#
# Why explicit mapping:
# - Felix docs clearly position spot equities/ETFs as a core product line.
# - Hyperliquid metadata fields like `evmContract` are not unique to Felix, so
#   using them alone causes false positives.
# - We therefore use a curated symbol set (docs + live discovery), then layer
#   fullName text signals on top.
_FELIX_TOKENS = {
    "AAPL", "GOOGL", "AMZN", "META", "MSFT",
    "SPY", "QQQ", "GLD", "SLV",
    "HOOD", "BNB1", "QQQM", "FEUSD",
}

# Tokens confirmed as Wagyu.xyz (trade[XYZ]) — identified by fullName field.
_XYZ_TOKENS = {"TSLA", "NVDA", "SPACEX", "XMR1", "TAO1", "TRADE"}


def venue_label_from_name(name: str, full_name: str = "", has_evm_contract: bool = False) -> str:
    """Identify deployer/venue from token name, full_name, and evm contract presence."""
    lower = (name + " " + full_name).lower()

    if "wagyu" in lower or "wagyu.xyz" in lower:
        return "xyz"
    if "trade.fun" in lower or "trade[xyz]" in lower:
        return "xyz"
    if name in _XYZ_TOKENS:
        return "xyz"

    if "felix" in lower:
        return "felix"
    if name in _FELIX_TOKENS:
        return "felix"

    if "unit " in lower:
        return "unit"
    if "melt " in lower:
        return "melt"
    if "hybridge" in lower:
        return "hybridge"
    if "perpetuals" in lower:
        return "perpetuals"

    return "hl_native"
