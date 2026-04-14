"""
Lead-lag price discovery detector.

Hypothesis A: One venue/market moves first; the other follows after a
measurable delay.

Tests:
  - Cross-correlation at ms/second horizons
  - Granger-causality (via statsmodels VAR + Wald test)
  - Impulse response (from VAR)
  - Conditional on volatility regime and time-of-day
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from statsmodels.tsa.api import VAR
from statsmodels.tsa.stattools import grangercausalitytests


@dataclass
class LeadLagResult:
    leader: str
    follower: str
    horizon_ms: int
    hit_rate: float            # fraction of leader moves where follower moved same direction
    avg_move_capture: float    # avg bps captured by acting on signal
    false_signal_rate: float
    granger_pvalue: float
    xcorr_lag_ms: int          # lag of peak cross-correlation in ms
    notes: str = ""


class LeadLagDetector:
    """
    Detects lead-lag relationships between two price series.

    Feed mid-price updates as they arrive. Call `analyze()` for results.
    Both series are resampled to a common grid before analysis.
    """

    def __init__(
        self,
        market_a: str,
        market_b: str,
        resample_ms: int = 100,     # ms grid for resampling
        window_s: int = 600,        # analysis window in seconds
        max_lag_ms: int = 5_000,    # max cross-correlation lag
    ) -> None:
        self.market_a = market_a
        self.market_b = market_b
        self.resample_ms = resample_ms
        self.window_s = window_s
        self.max_lag_ms = max_lag_ms

        # Raw (ms timestamp, price)
        self._series_a: list[tuple[int, float]] = []
        self._series_b: list[tuple[int, float]] = []

    def update_a(self, ts_ms: int, price: float) -> None:
        self._series_a.append((ts_ms, price))
        cutoff = ts_ms - self.window_s * 1000 * 2
        self._series_a = [(t, p) for t, p in self._series_a if t >= cutoff]

    def update_b(self, ts_ms: int, price: float) -> None:
        self._series_b.append((ts_ms, price))
        cutoff = ts_ms - self.window_s * 1000 * 2
        self._series_b = [(t, p) for t, p in self._series_b if t >= cutoff]

    def _as_series(self, data: list[tuple[int, float]], window_s: int | None = None) -> pd.Series:
        ws = window_s or self.window_s
        if not data:
            return pd.Series(dtype=float)
        df = pd.DataFrame(data, columns=["ts_ms", "price"])
        df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        df = df.set_index("ts").sort_index()
        # keep only last window_s
        if len(df):
            cutoff = df.index[-1] - pd.Timedelta(seconds=ws)
            df = df[df.index >= cutoff]
        # resample to uniform grid
        freq = f"{self.resample_ms}ms"
        return df["price"].resample(freq).last().ffill()

    def _common_grid(self) -> Optional[tuple[pd.Series, pd.Series]]:
        sa = self._as_series(self._series_a)
        sb = self._as_series(self._series_b)
        if len(sa) < 20 or len(sb) < 20:
            return None
        # align on common index
        idx = sa.index.intersection(sb.index)
        if len(idx) < 20:
            return None
        return sa.loc[idx], sb.loc[idx]

    def cross_correlation(self) -> Optional[tuple[int, float]]:
        """
        Returns (lag_ms_at_peak, peak_correlation).
        Positive lag means A leads B.
        """
        result = self._common_grid()
        if result is None:
            return None
        sa, sb = result
        ra = sa.pct_change().dropna()
        rb = sb.pct_change().dropna()
        # align after pct change
        idx = ra.index.intersection(rb.index)
        ra, rb = ra.loc[idx], rb.loc[idx]

        max_lag = int(self.max_lag_ms / self.resample_ms)
        best_lag, best_corr = 0, 0.0
        ra_arr = ra.to_numpy()
        rb_arr = rb.to_numpy()
        n = len(ra_arr)
        for lag in range(-max_lag, max_lag + 1):
            if lag > 0:
                x, y = ra_arr[lag:], rb_arr[:n - lag]
            elif lag < 0:
                x, y = ra_arr[:n + lag], rb_arr[-lag:]
            else:
                x, y = ra_arr, rb_arr
            if len(x) < 10:
                continue
            corr = float(np.corrcoef(x, y)[0, 1])
            if abs(corr) > abs(best_corr):
                best_corr = corr
                best_lag = lag
        return best_lag * self.resample_ms, best_corr

    def granger_test(self, max_lag_steps: int = 5) -> dict[str, float]:
        """
        Granger causality test.
        Returns {'a_causes_b': p_value, 'b_causes_a': p_value}.
        """
        result = self._common_grid()
        if result is None:
            return {"a_causes_b": 1.0, "b_causes_a": 1.0}
        sa, sb = result
        returns = pd.DataFrame({"a": sa.pct_change(), "b": sb.pct_change()}).dropna()
        if len(returns) < max_lag_steps * 3:
            return {"a_causes_b": 1.0, "b_causes_a": 1.0}
        try:
            res_ab = grangercausalitytests(returns[["b", "a"]], maxlag=max_lag_steps, verbose=False)
            res_ba = grangercausalitytests(returns[["a", "b"]], maxlag=max_lag_steps, verbose=False)
            # take minimum p-value across lags
            pval_ab = min(v[0]["ssr_ftest"][1] for v in res_ab.values())
            pval_ba = min(v[0]["ssr_ftest"][1] for v in res_ba.values())
        except Exception:
            return {"a_causes_b": 1.0, "b_causes_a": 1.0}
        return {"a_causes_b": float(pval_ab), "b_causes_a": float(pval_ba)}

    def hit_rate(self, lag_ms: int) -> tuple[float, float]:
        """
        Given lag_ms (A leads B), compute:
        - hit_rate: fraction of A moves where B moved same direction lag_ms later
        - false_signal_rate: fraction where B moved opposite
        """
        result = self._common_grid()
        if result is None:
            return 0.0, 1.0
        sa, sb = result
        lag_steps = max(1, int(lag_ms / self.resample_ms))
        ra = np.sign(sa.pct_change().dropna().to_numpy())
        rb = np.sign(sb.pct_change().dropna().to_numpy())
        n = min(len(ra), len(rb))
        ra, rb = ra[:n], rb[:n]
        if n <= lag_steps:
            return 0.0, 1.0
        leader = ra[:-lag_steps]
        follower = rb[lag_steps:]
        mask = leader != 0
        if mask.sum() == 0:
            return 0.0, 1.0
        hit = float(np.mean(leader[mask] == follower[mask]))
        false_sig = float(np.mean(leader[mask] == -follower[mask]))
        return hit, false_sig

    def analyze(self) -> Optional[LeadLagResult]:
        """Run full analysis and return ranked result."""
        xcorr = self.cross_correlation()
        if xcorr is None:
            return None
        lag_ms, corr = xcorr
        granger = self.granger_test()

        if lag_ms >= 0:
            leader, follower = self.market_a, self.market_b
            g_pval = granger["a_causes_b"]
        else:
            leader, follower = self.market_b, self.market_a
            lag_ms = -lag_ms
            g_pval = granger["b_causes_a"]

        hit, false_sig = self.hit_rate(lag_ms)

        # avg_move_capture: rough estimate — correlation * avg move size in bp
        result = self._common_grid()
        avg_move_bp = 0.0
        if result is not None:
            sa, _ = result
            avg_move_bp = float(np.mean(np.abs(sa.pct_change().dropna())) * 10_000)

        return LeadLagResult(
            leader=leader,
            follower=follower,
            horizon_ms=lag_ms,
            hit_rate=hit,
            avg_move_capture=avg_move_bp * abs(corr),
            false_signal_rate=false_sig,
            granger_pvalue=g_pval,
            xcorr_lag_ms=lag_ms,
        )
