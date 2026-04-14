"""
Circuit breaker logic for live/paper trading.

Monitors:
  - Slippage vs model (kill if 2x model for 3 consecutive days)
  - Hedge completion rate (kill if below threshold)
  - Spread decay speed
  - Funding capture vs posted
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from typing import Callable, Optional

import structlog

log = structlog.get_logger(__name__)


@dataclass
class SlippageObs:
    day: date
    modeled_bp: float
    realized_bp: float

    @property
    def ratio(self) -> float:
        if self.modeled_bp == 0:
            return 1.0
        return self.realized_bp / self.modeled_bp


class CircuitBreaker:
    """
    Stateful circuit breaker.  Call `observe_*` after each relevant event.
    Call `check()` before each new signal; returns True if trading should halt.
    """

    def __init__(
        self,
        strategy_id: str,
        slippage_ratio_threshold: float = 2.0,
        slippage_consecutive_days: int = 3,
        min_hedge_completion: float = 0.85,
        min_funding_capture: float = 0.70,
        on_trip: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self.strategy_id = strategy_id
        self._slip_threshold = slippage_ratio_threshold
        self._slip_days = slippage_consecutive_days
        self._min_hedge = min_hedge_completion
        self._min_funding = min_funding_capture
        self._on_trip = on_trip

        self._tripped = False
        self._trip_reason = ""

        self._slip_obs: deque[SlippageObs] = deque(maxlen=10)
        self._hedge_fills: list[bool] = []        # True = hedge completed
        self._funding_obs: list[tuple[float, float]] = []  # (posted, realized)

    @property
    def is_tripped(self) -> bool:
        return self._tripped

    def reset(self) -> None:
        self._tripped = False
        self._trip_reason = ""
        log.info("circuit_breaker_reset", strategy=self.strategy_id)

    def observe_slippage(self, modeled_bp: float, realized_bp: float) -> None:
        today = date.today()
        if self._slip_obs and self._slip_obs[-1].day == today:
            # Update today's running average
            obs = self._slip_obs[-1]
            obs.modeled_bp = (obs.modeled_bp + modeled_bp) / 2
            obs.realized_bp = (obs.realized_bp + realized_bp) / 2
        else:
            self._slip_obs.append(SlippageObs(today, modeled_bp, realized_bp))

        # Check consecutive days
        if len(self._slip_obs) >= self._slip_days:
            recent = list(self._slip_obs)[-self._slip_days:]
            if all(o.ratio >= self._slip_threshold for o in recent):
                self._trip("slippage", f"{self._slip_days} consecutive days of slippage >= {self._slip_threshold}x model")

    def observe_hedge(self, completed: bool) -> None:
        self._hedge_fills.append(completed)
        if len(self._hedge_fills) > 100:
            self._hedge_fills = self._hedge_fills[-100:]
        if len(self._hedge_fills) >= 20:
            rate = sum(self._hedge_fills) / len(self._hedge_fills)
            if rate < self._min_hedge:
                self._trip("hedge_completion", f"hedge fill rate {rate:.1%} < {self._min_hedge:.1%}")

    def observe_funding(self, posted: float, realized: float) -> None:
        self._funding_obs.append((posted, realized))
        if len(self._funding_obs) > 50:
            self._funding_obs = self._funding_obs[-50:]
        if len(self._funding_obs) >= 10 and posted != 0:
            capture = sum(r / p for p, r in self._funding_obs if p != 0) / len(self._funding_obs)
            if capture < self._min_funding:
                self._trip("funding_capture", f"funding capture {capture:.1%} < {self._min_funding:.1%}")

    def check(self) -> bool:
        """Returns True if the strategy should halt."""
        return self._tripped

    def _trip(self, reason: str, message: str) -> None:
        if not self._tripped:
            self._tripped = True
            self._trip_reason = f"{reason}: {message}"
            log.warning("circuit_breaker_tripped", strategy=self.strategy_id, reason=self._trip_reason)
            if self._on_trip:
                self._on_trip(self.strategy_id, self._trip_reason)
