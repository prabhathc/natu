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


def venue_label_from_name(name: str) -> str:
    """Identify deployer/venue from market name."""
    lower = name.lower()
    if "xyz" in lower or "trade" in lower:
        return "xyz"
    if "felix" in lower:
        return "felix"
    return "hl_native"
