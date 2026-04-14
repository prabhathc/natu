"""
Feature engine: aggregates all per-market features and emits a unified
feature snapshot consumed by both research (notebooks) and live strategies.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Coroutine, Optional

from arb.market_data.models import FundingStateEvent, MarketStateEvent, RawQuote, RawTrade
from arb.signals.funding import FundingAnalyzer
from arb.signals.lead_lag import LeadLagDetector
from arb.signals.spreads import SpreadCalculator


@dataclass
class MarketFeatures:
    """Point-in-time feature snapshot for a single market."""

    market_id: str
    ts: datetime
    mid_px: Optional[float] = None
    spread_bp: Optional[float] = None
    mark_px: Optional[float] = None
    oracle_px: Optional[float] = None
    premium_bp: Optional[float] = None         # (mark - oracle) / oracle * 10000
    funding_rate_8h: Optional[float] = None
    funding_annualized: Optional[float] = None
    open_interest: Optional[float] = None
    vol_1m_bp: Optional[float] = None          # 1-min realized vol in bp
    vol_5m_bp: Optional[float] = None


@dataclass
class FeatureSnapshot:
    """All features at a given moment, keyed by market_id."""

    ts: datetime
    markets: dict[str, MarketFeatures] = field(default_factory=dict)
    spread_z_scores: dict[str, float] = field(default_factory=dict)
    lead_lag_signals: dict[str, str] = field(default_factory=dict)
    funding_signals: dict[str, str] = field(default_factory=dict)


class FeatureEngine:
    """
    Stateful feature engine.

    Receives raw events, maintains rolling state, and publishes
    FeatureSnapshot to registered listeners at each update.
    """

    def __init__(self, snapshot_interval_s: float = 1.0) -> None:
        self._interval = snapshot_interval_s
        self._market_state: dict[str, MarketFeatures] = {}
        self._recent_trades: dict[str, list[tuple[float, float]]] = {}  # market -> [(ts, price)]
        self._funding: dict[str, FundingAnalyzer] = {}
        self._spread_calcs: dict[str, SpreadCalculator] = {}
        self._ll_detectors: dict[str, LeadLagDetector] = {}
        self._listeners: list[Callable[[FeatureSnapshot], Coroutine]] = []
        self._task: Optional[asyncio.Task] = None

    # ── Registration ─────────────────────────────────────────────────────────

    def register_spread_pair(
        self,
        market_a: str,
        market_b: str,
        window: int = 300,
    ) -> SpreadCalculator:
        key = f"{market_a}::{market_b}"
        if key not in self._spread_calcs:
            self._spread_calcs[key] = SpreadCalculator(market_a, market_b, window)
        return self._spread_calcs[key]

    def register_lead_lag(
        self,
        market_a: str,
        market_b: str,
        resample_ms: int = 100,
    ) -> LeadLagDetector:
        key = f"{market_a}::{market_b}"
        if key not in self._ll_detectors:
            self._ll_detectors[key] = LeadLagDetector(market_a, market_b, resample_ms)
        return self._ll_detectors[key]

    def subscribe(self, fn: Callable[[FeatureSnapshot], Coroutine]) -> None:
        self._listeners.append(fn)

    # ── Ingest ────────────────────────────────────────────────────────────────

    async def on_quote(self, q: RawQuote) -> None:
        mid = float((q.bid_px + q.ask_px) / 2)
        spread_bp = float((q.ask_px - q.bid_px) / q.bid_px * 10_000) if q.bid_px > 0 else None
        mf = self._get_or_create(q.market_id, q.ts)
        mf.mid_px = mid
        mf.spread_bp = spread_bp

        ts_ms = int(q.ts.timestamp() * 1000)
        for key, ll in self._ll_detectors.items():
            if ll.market_a == q.market_id:
                ll.update_a(ts_ms, mid)
            elif ll.market_b == q.market_id:
                ll.update_b(ts_ms, mid)

        for key, sc in self._spread_calcs.items():
            other = sc.market_b if sc.market_a == q.market_id else (sc.market_a if sc.market_b == q.market_id else None)
            if other and other in self._market_state and self._market_state[other].mid_px:
                ts_s = q.ts.timestamp()
                if sc.market_a == q.market_id:
                    sc.update(ts_s, mid, self._market_state[other].mid_px)
                else:
                    sc.update(ts_s, self._market_state[other].mid_px, mid)

    async def on_trade(self, t: RawTrade) -> None:
        trades = self._recent_trades.setdefault(t.market_id, [])
        trades.append((t.ts.timestamp(), float(t.price)))
        cutoff = t.ts.timestamp() - 300  # 5 min
        self._recent_trades[t.market_id] = [(ts, p) for ts, p in trades if ts >= cutoff]
        self._update_vol(t.market_id)

    async def on_market_state(self, s: MarketStateEvent) -> None:
        mf = self._get_or_create(s.market_id, s.ts)
        if s.mark_px:
            mf.mark_px = float(s.mark_px)
        if s.oracle_px:
            mf.oracle_px = float(s.oracle_px)
        if mf.mark_px and mf.oracle_px and mf.oracle_px > 0:
            mf.premium_bp = (mf.mark_px - mf.oracle_px) / mf.oracle_px * 10_000
        if s.open_interest:
            mf.open_interest = float(s.open_interest)

    async def on_funding(self, f: FundingStateEvent) -> None:
        mf = self._get_or_create(f.market_id, f.ts)
        mf.funding_rate_8h = float(f.funding_rate)
        mf.funding_annualized = float(f.annualized_rate)
        if f.market_id not in self._funding:
            self._funding[f.market_id] = FundingAnalyzer(f.market_id)
        self._funding[f.market_id].update(float(f.funding_rate))

    # ── Snapshot emission ─────────────────────────────────────────────────────

    async def start(self) -> None:
        self._task = asyncio.create_task(self._emit_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def _emit_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            snap = self._build_snapshot()
            for listener in self._listeners:
                try:
                    await listener(snap)
                except Exception:
                    pass

    def _build_snapshot(self) -> FeatureSnapshot:
        ts = datetime.now(tz=timezone.utc)
        snap = FeatureSnapshot(ts=ts, markets=dict(self._market_state))

        for key, sc in self._spread_calcs.items():
            try:
                snap.spread_z_scores[key] = sc.current_z()
            except Exception:
                pass

        for mid, fa in self._funding.items():
            sig = fa.carry_signal()
            if sig:
                snap.funding_signals[mid] = sig

        for key, ll in self._ll_detectors.items():
            result = ll.analyze()
            if result and result.hit_rate > 0.55 and result.granger_pvalue < 0.05:
                snap.lead_lag_signals[key] = f"{result.leader}→{result.follower}@{result.horizon_ms}ms"

        return snap

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_or_create(self, market_id: str, ts: datetime) -> MarketFeatures:
        if market_id not in self._market_state:
            self._market_state[market_id] = MarketFeatures(market_id=market_id, ts=ts)
        self._market_state[market_id].ts = ts
        return self._market_state[market_id]

    def _update_vol(self, market_id: str) -> None:
        trades = self._recent_trades.get(market_id, [])
        if not trades:
            return
        mf = self._market_state.get(market_id)
        if not mf:
            return
        now = trades[-1][0]
        import numpy as np

        for window_s, attr in [(60, "vol_1m_bp"), (300, "vol_5m_bp")]:
            window_trades = [p for ts, p in trades if ts >= now - window_s]
            if len(window_trades) >= 2:
                returns = np.diff(np.log(window_trades))
                vol = float(np.std(returns) * 10_000)
                setattr(mf, attr, vol)
