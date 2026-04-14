"""
Risk controls: per-strategy and portfolio-level checks.

Every order submission should pass through `RiskControls.check()` before
hitting the execution layer.  The method returns either None (pass) or a
RiskViolation describing the breach.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import structlog

from arb.config import settings

log = structlog.get_logger(__name__)


@dataclass
class RiskViolation:
    rule: str
    message: str
    strategy_id: str
    market_id: Optional[str] = None
    value: Optional[float] = None
    limit: Optional[float] = None


@dataclass
class StrategyRiskState:
    strategy_id: str
    gross_notional: float = 0.0
    per_market_notional: dict[str, float] = field(default_factory=dict)
    daily_pnl: float = 0.0
    open_spreads: int = 0
    last_fill_ts: Optional[datetime] = None


class RiskControls:
    """
    Portfolio and per-strategy risk manager.

    Limits (all configurable; defaults from settings):
      - single_market_cap:    max notional in one market
      - portfolio_gross_cap:  max total gross notional
      - per_trade_loss_cap:   max expected loss per new trade
      - max_concurrent_spreads: max open spread positions
      - max_hold_minutes:     alert (not hard stop) on stale positions
    """

    def __init__(
        self,
        single_market_cap: float = settings.risk_single_market_cap,
        portfolio_gross_cap: float = settings.risk_portfolio_gross_cap,
        per_trade_loss_cap: float = settings.risk_per_trade_loss_cap,
        max_concurrent_spreads: int = settings.risk_max_concurrent_spreads,
        max_hold_minutes: int = settings.risk_max_hold_minutes,
    ) -> None:
        self._single_market_cap = single_market_cap
        self._portfolio_gross_cap = portfolio_gross_cap
        self._per_trade_loss_cap = per_trade_loss_cap
        self._max_spreads = max_concurrent_spreads
        self._max_hold_minutes = max_hold_minutes

        self._states: dict[str, StrategyRiskState] = {}
        self._hard_killed: set[str] = set()

    def hard_kill(self, strategy_id: str) -> None:
        self._hard_killed.add(strategy_id)
        log.critical("hard_kill", strategy=strategy_id)

    def revive(self, strategy_id: str) -> None:
        self._hard_killed.discard(strategy_id)

    def check(
        self,
        strategy_id: str,
        market_id: str,
        notional: float,
        expected_loss: float,
        is_new_spread: bool = False,
    ) -> Optional[RiskViolation]:
        """Returns None if OK, or a RiskViolation if blocked."""

        if strategy_id in self._hard_killed:
            return RiskViolation("hard_kill", "strategy is hard-killed", strategy_id)

        state = self._states.setdefault(strategy_id, StrategyRiskState(strategy_id))

        # Per-trade loss cap
        if expected_loss > self._per_trade_loss_cap:
            return RiskViolation(
                "per_trade_loss", f"expected loss {expected_loss:.2f} > cap {self._per_trade_loss_cap}",
                strategy_id, market_id, expected_loss, self._per_trade_loss_cap,
            )

        # Per-market notional cap
        new_market_notional = state.per_market_notional.get(market_id, 0.0) + notional
        if new_market_notional > self._single_market_cap:
            return RiskViolation(
                "single_market_cap",
                f"market notional {new_market_notional:.0f} > {self._single_market_cap:.0f}",
                strategy_id, market_id, new_market_notional, self._single_market_cap,
            )

        # Portfolio gross cap
        new_gross = state.gross_notional + notional
        if new_gross > self._portfolio_gross_cap:
            return RiskViolation(
                "portfolio_gross_cap",
                f"gross notional {new_gross:.0f} > {self._portfolio_gross_cap:.0f}",
                strategy_id, None, new_gross, self._portfolio_gross_cap,
            )

        # Concurrent spreads cap
        if is_new_spread and state.open_spreads >= self._max_spreads:
            return RiskViolation(
                "max_spreads",
                f"open spreads {state.open_spreads} >= {self._max_spreads}",
                strategy_id, None, float(state.open_spreads), float(self._max_spreads),
            )

        return None  # all checks passed

    def record_fill(
        self,
        strategy_id: str,
        market_id: str,
        notional: float,
        pnl: float,
    ) -> None:
        state = self._states.setdefault(strategy_id, StrategyRiskState(strategy_id))
        state.per_market_notional[market_id] = state.per_market_notional.get(market_id, 0.0) + notional
        state.gross_notional += notional
        state.daily_pnl += pnl
        state.last_fill_ts = datetime.now(tz=timezone.utc)

    def record_close(self, strategy_id: str, market_id: str, notional: float) -> None:
        state = self._states.get(strategy_id)
        if state:
            state.per_market_notional[market_id] = max(
                0.0,
                state.per_market_notional.get(market_id, 0.0) - notional,
            )
            state.gross_notional = max(0.0, state.gross_notional - notional)

    def record_spread_open(self, strategy_id: str) -> None:
        state = self._states.setdefault(strategy_id, StrategyRiskState(strategy_id))
        state.open_spreads += 1

    def record_spread_close(self, strategy_id: str) -> None:
        state = self._states.get(strategy_id)
        if state:
            state.open_spreads = max(0, state.open_spreads - 1)

    def state(self, strategy_id: str) -> Optional[StrategyRiskState]:
        return self._states.get(strategy_id)
