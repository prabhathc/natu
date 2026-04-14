"""
Funding rate analyzer.

Hypothesis C: A delta-neutral structure earns positive carry after fees and
expected slippage.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class FundingCarryStats:
    market: str
    avg_funding_8h: float           # average 8h funding rate
    annualized_rate: float          # funding_8h * 3 * 365
    variance: float
    funding_persistence: float      # AR(1) autocorrelation of funding rate
    realized_capture: float         # estimated fraction actually collectible
    slippage_drag_bp: float
    net_carry_bp_per_8h: float
    is_crowded: bool
    notes: str = ""


class FundingAnalyzer:
    """
    Tracks funding rate history for a market and computes carry statistics.
    """

    def __init__(self, market_id: str, history_n: int = 200) -> None:
        self.market_id = market_id
        self._rates: list[float] = []     # 8h funding rates
        self._history_n = history_n

    def update(self, rate_8h: float) -> None:
        self._rates.append(rate_8h)
        if len(self._rates) > self._history_n * 2:
            self._rates = self._rates[-self._history_n * 2:]

    def _tail(self, n: int | None = None) -> np.ndarray:
        n = n or self._history_n
        return np.array(self._rates[-n:])

    def persistence(self) -> float:
        """AR(1) autocorrelation of funding rates."""
        r = self._tail()
        if len(r) < 10:
            return 0.0
        return float(np.corrcoef(r[:-1], r[1:])[0, 1])

    def is_crowded(self, threshold_z: float = 2.0) -> bool:
        """
        Funding is considered 'crowded' when it is >threshold_z standard
        deviations above its rolling mean (many traders positioned for carry).
        """
        r = self._tail()
        if len(r) < 20:
            return False
        mu, sigma = np.mean(r), np.std(r)
        if sigma == 0:
            return False
        z = (r[-1] - mu) / sigma
        return abs(z) > threshold_z

    def compute_stats(
        self,
        entry_slippage_bp: float = 2.0,
        exit_slippage_bp: float = 2.0,
        hold_periods: int = 3,          # number of 8h periods expected to hold
    ) -> FundingCarryStats:
        r = self._tail()
        if len(r) == 0:
            return FundingCarryStats(
                market=self.market_id,
                avg_funding_8h=0.0,
                annualized_rate=0.0,
                variance=0.0,
                funding_persistence=0.0,
                realized_capture=0.0,
                slippage_drag_bp=0.0,
                net_carry_bp_per_8h=0.0,
                is_crowded=False,
            )

        avg = float(np.mean(r))
        var = float(np.var(r))
        persist = self.persistence()
        crowded = self.is_crowded()

        # Realized capture: discounted by persistence decay
        # If persistence is high, we expect to collect more periods
        realized = avg * min(hold_periods, 1 + max(persist, 0) * 2)

        # Slippage spread over hold_periods periods
        total_slippage = (entry_slippage_bp + exit_slippage_bp) / hold_periods

        # Net carry per 8h in bp (assuming entry/exit costs amortized)
        net_carry = realized * 10_000 - total_slippage  # funding rate → bp

        return FundingCarryStats(
            market=self.market_id,
            avg_funding_8h=avg,
            annualized_rate=avg * 3 * 365,
            variance=var,
            funding_persistence=persist,
            realized_capture=float(realized),
            slippage_drag_bp=total_slippage,
            net_carry_bp_per_8h=net_carry,
            is_crowded=crowded,
        )

    def carry_signal(
        self,
        min_net_carry_bp: float = 1.0,
    ) -> Optional[str]:
        """
        Returns 'long' (collect positive funding), 'short' (collect negative
        funding from short), or None.
        """
        stats = self.compute_stats()
        if stats.is_crowded:
            return None
        if stats.net_carry_bp_per_8h >= min_net_carry_bp:
            return "short" if stats.avg_funding_8h > 0 else "long"
        return None


class CrossMarketFundingArb:
    """
    Tracks funding differential between two markets on the same underlying.
    Signals when one is materially better to short (positive funding)
    while the other charges less (for hedging).
    """

    def __init__(self, market_long: str, market_short: str) -> None:
        self.market_long = market_long
        self.market_short = market_short
        self.long_analyzer = FundingAnalyzer(market_long)
        self.short_analyzer = FundingAnalyzer(market_short)

    def update(self, market_id: str, rate: float) -> None:
        if market_id == self.market_long:
            self.long_analyzer.update(rate)
        elif market_id == self.market_short:
            self.short_analyzer.update(rate)

    def differential_stats(self) -> dict:
        long_stats = self.long_analyzer.compute_stats()
        short_stats = self.short_analyzer.compute_stats()
        diff = short_stats.avg_funding_8h - long_stats.avg_funding_8h
        return {
            "market_long": self.market_long,
            "market_short": self.market_short,
            "long_funding_8h": long_stats.avg_funding_8h,
            "short_funding_8h": short_stats.avg_funding_8h,
            "differential_8h": diff,
            "annualized_differential": diff * 3 * 365,
            "long_crowded": long_stats.is_crowded,
            "short_crowded": short_stats.is_crowded,
        }
