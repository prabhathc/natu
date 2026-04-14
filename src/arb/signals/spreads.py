"""
Cross-market spread calculator and mean-reversion analytics.

Hypothesis B: Two economically linked markets diverge beyond noise and
mean-revert within a measurable half-life.

All price inputs are mid prices.  Hedge ratios estimated by OLS on rolling
windows.  Z-score uses rolling mean/std.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from statsmodels.regression.linear_model import OLS
from statsmodels.tsa.stattools import adfuller


@dataclass
class SpreadStats:
    pair: str                       # "market_a::market_b"
    hedge_ratio: float              # OLS beta (short market_b per 1 long market_a)
    half_life_s: Optional[float]    # seconds; None if AR(1) non-significant
    avg_edge_bp: float
    post_cost_edge_bp: float
    is_stationary: bool             # ADF p-value < 0.05
    adf_pvalue: float
    z_score: float                  # current z-score of the spread
    stress_failures: int = 0
    notes: str = ""


@dataclass
class SpreadBook:
    """Rolling register of all tracked pairs."""
    entries: dict[str, SpreadStats] = field(default_factory=dict)

    def update(self, stats: SpreadStats) -> None:
        self.entries[stats.pair] = stats

    def as_dataframe(self) -> pd.DataFrame:
        if not self.entries:
            return pd.DataFrame()
        return pd.DataFrame([vars(s) for s in self.entries.values()])


class SpreadCalculator:
    """
    Maintains rolling spread and statistics for a pair of markets.

    Usage:
        calc = SpreadCalculator("hl:XAU-xyz", "hl:XAU-felix", window=300)
        calc.update(ts, mid_a, mid_b)
        stats = calc.compute_stats(fee_bp=3.0)
    """

    def __init__(
        self,
        market_a: str,
        market_b: str,
        window: int = 300,          # samples in rolling window
        z_entry: float = 2.0,
        z_exit: float = 0.5,
    ) -> None:
        self.market_a = market_a
        self.market_b = market_b
        self.window = window
        self.z_entry = z_entry
        self.z_exit = z_exit

        self._ts: list[float] = []
        self._px_a: list[float] = []
        self._px_b: list[float] = []

    @property
    def pair(self) -> str:
        return f"{self.market_a}::{self.market_b}"

    def update(self, ts: float, mid_a: float, mid_b: float) -> None:
        self._ts.append(ts)
        self._px_a.append(mid_a)
        self._px_b.append(mid_b)
        if len(self._ts) > self.window * 2:
            self._ts = self._ts[-self.window * 2:]
            self._px_a = self._px_a[-self.window * 2:]
            self._px_b = self._px_b[-self.window * 2:]

    def _tail(self, n: int | None = None) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        n = n or self.window
        ts = np.array(self._ts[-n:])
        a = np.array(self._px_a[-n:])
        b = np.array(self._px_b[-n:])
        return ts, a, b

    def hedge_ratio(self, n: int | None = None) -> float:
        """OLS beta of market_a ~ market_b."""
        _, a, b = self._tail(n)
        if len(a) < 10:
            return 1.0
        # Use a no-intercept regression for trading hedge ratio. Adding a free
        # intercept absorbs level differences and can materially bias beta on
        # integrated price series.
        result = OLS(a, b[:, None]).fit()
        return float(result.params[0])

    def spread_series(self, beta: float | None = None) -> np.ndarray:
        """residual = price_a - beta * price_b"""
        _, a, b = self._tail()
        if beta is None:
            beta = self.hedge_ratio()
        return a - beta * b

    def z_score_series(self) -> tuple[np.ndarray, float, float]:
        """Returns (z_series, mean, std)."""
        s = self.spread_series()
        mu = float(np.mean(s))
        sigma = float(np.std(s))
        if sigma == 0:
            return np.zeros_like(s), mu, 0.0
        return (s - mu) / sigma, mu, sigma

    def current_z(self) -> float:
        zs, _, _ = self.z_score_series()
        return float(zs[-1]) if len(zs) > 0 else 0.0

    def half_life(self) -> Optional[float]:
        """
        Estimate mean-reversion half-life from AR(1) fit on spread.
        Returns seconds.  Returns None if non-mean-reverting.
        """
        s = self.spread_series()
        if len(s) < 20:
            return None
        lag = s[:-1]
        delta = np.diff(s)
        X = np.column_stack([lag, np.ones(len(lag))])
        res = OLS(delta, X).fit()
        rho = res.params[0]
        if rho >= 0:
            return None   # non-stationary / explosively trending
        hl_samples = -np.log(2) / rho
        if len(self._ts) >= 2:
            avg_interval_s = (self._ts[-1] - self._ts[0]) / (len(self._ts) - 1)
            return float(hl_samples * avg_interval_s)
        return None

    def is_stationary(self) -> tuple[bool, float]:
        """ADF test on the spread. Returns (stationary, p_value)."""
        s = self.spread_series()
        if len(s) < 20:
            return False, 1.0
        result = adfuller(s, autolag="AIC")
        pval = float(result[1])
        return pval < 0.05, pval

    def compute_stats(self, fee_bp: float = 3.0) -> SpreadStats:
        beta = self.hedge_ratio()
        zs, mu, sigma = self.z_score_series()
        stationary, pval = self.is_stationary()
        hl = self.half_life()
        current_z = float(zs[-1]) if len(zs) > 0 else 0.0

        # Average edge: mean absolute spread excursion beyond entry threshold in bp
        spread = self.spread_series(beta)
        if len(spread) > 0 and mu != 0:
            avg_edge_bp = float(np.mean(np.abs(spread[np.abs(zs) > self.z_entry])) / abs(mu) * 10_000)
        else:
            avg_edge_bp = 0.0

        post_cost = max(avg_edge_bp - fee_bp * 2, 0.0)  # round-trip fee

        return SpreadStats(
            pair=self.pair,
            hedge_ratio=beta,
            half_life_s=hl,
            avg_edge_bp=avg_edge_bp,
            post_cost_edge_bp=post_cost,
            is_stationary=stationary,
            adf_pvalue=pval,
            z_score=current_z,
        )

    def signal(self) -> Optional[str]:
        """
        Returns 'long_a_short_b', 'short_a_long_b', 'exit', or None.
        """
        z = self.current_z()
        if z < -self.z_entry:
            return "long_a_short_b"
        if z > self.z_entry:
            return "short_a_long_b"
        if abs(z) < self.z_exit:
            return "exit"
        return None
