"""Central configuration via environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from decimal import Decimal


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Hyperliquid API ──────────────────────────────────────────────────────
    hl_api_url: str = "https://api.hyperliquid.xyz"
    hl_ws_url: str = "wss://api.hyperliquid.xyz/ws"
    hl_wallet_address: str = ""
    hl_private_key: str = ""

    # ── Database ─────────────────────────────────────────────────────────────
    db_url: str = "postgresql+asyncpg://arb:arb@localhost:5432/arb"

    # ── Redis (optional) ──────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = "INFO"
    log_json: bool = False

    # ── Paper mode ────────────────────────────────────────────────────────────
    paper_trade: bool = True

    # ── Risk defaults ─────────────────────────────────────────────────────────
    risk_single_market_cap: float = 5_000.0      # USD notional
    risk_portfolio_gross_cap: float = 20_000.0
    risk_per_trade_loss_cap: float = 100.0
    risk_max_concurrent_spreads: int = 5
    risk_max_hold_minutes: int = 240

    @field_validator("log_level")
    @classmethod
    def upper_log(cls, v: str) -> str:
        return v.upper()


settings = Settings()
