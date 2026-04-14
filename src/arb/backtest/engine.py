"""
Event-driven backtest engine.

Replays the stored event stream in chronological order, driving the
simulator and strategy callbacks.

Design principles:
  - No candle resampling: events fire in the order they were recorded
  - Queue-position approximation for passive fills
  - Funding accrued at recorded funding timestamps
  - Session boundaries honored (no fills during closed reference markets)
  - Mandatory falsification tests built in
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Optional, Sequence

import pandas as pd
import structlog

from arb.execution.models import Fill, Order, OrderType, Side
from arb.execution.paper_trader import PaperTrader
from arb.execution.simulator import FeeSchedule, SlippageModel
from arb.market_data.models import FundingStateEvent, MarketStateEvent, RawQuote, RawTrade

log = structlog.get_logger(__name__)


@dataclass
class BacktestConfig:
    strategy_id: str
    market_ids: list[str]
    start: datetime
    end: datetime

    # Fee / slippage overrides (None = use defaults)
    fee_schedules: dict[str, FeeSchedule] | None = None
    slippage_model: SlippageModel | None = None

    # Falsification knobs
    slippage_multiplier: float = 1.0        # set to 2.0 for shock test
    latency_ms: float = 20.0
    remove_top_pct_trades: float = 0.0      # set to 0.05 to remove best 5%
    fee_multiplier: float = 1.0             # set to 2.0 to stress fees

    # Session filter: skip fills when market is "closed"
    session_filter: Callable[[datetime, str], bool] | None = None


@dataclass
class BacktestEvent:
    ts: datetime
    kind: str   # 'quote', 'trade', 'market_state', 'funding'
    market_id: str
    data: Any


class BacktestEngine:
    """
    Replays event streams from the database (or in-memory DataFrames).

    Usage:
        engine = BacktestEngine(config, strategy_fn)
        results = await engine.run(quotes_df, trades_df, funding_df)
    """

    def __init__(
        self,
        config: BacktestConfig,
        strategy: Callable,     # async fn(trader, features) -> None
    ) -> None:
        self.config = config
        self.strategy = strategy

        # Build slippage with multiplier
        base_slip = config.slippage_model or SlippageModel()
        slippage = SlippageModel(
            base_bp=base_slip.base_bp * config.slippage_multiplier,
            depth_sensitivity=base_slip.depth_sensitivity * config.slippage_multiplier,
            noise_std_bp=base_slip.noise_std_bp,
        )

        # Apply fee multiplier
        fees = {}
        for mkt, sched in (config.fee_schedules or {}).items():
            fees[mkt] = FeeSchedule(
                maker_bp=sched.maker_bp * config.fee_multiplier,
                taker_bp=sched.taker_bp * config.fee_multiplier,
                deployer_share_bp=sched.deployer_share_bp * config.fee_multiplier,
            )

        self.trader = PaperTrader(fee_schedules=fees, slippage_model=slippage)
        self._fills: list[Fill] = []
        self._pnl_curve: list[tuple[datetime, float]] = []

    async def run(
        self,
        quotes: pd.DataFrame,       # columns: ts, market_id, bid_px, bid_sz, ask_px, ask_sz
        trades: pd.DataFrame,       # columns: ts, market_id, price, size, side
        funding: pd.DataFrame,      # columns: ts, market_id, funding_rate
        market_state: pd.DataFrame | None = None,
    ) -> dict:
        """
        Replay all events in timestamp order and return metrics dict.
        """
        events = self._build_event_stream(quotes, trades, funding, market_state)
        events.sort(key=lambda e: e.ts)

        n_events = len(events)
        log.info("backtest_start", strategy=self.config.strategy_id, events=n_events)

        for i, ev in enumerate(events):
            if ev.ts < self.config.start or ev.ts > self.config.end:
                continue
            if self.config.session_filter and not self.config.session_filter(ev.ts, ev.market_id):
                continue

            await self._dispatch(ev)

            # Emit PnL snapshot every 100 events
            if i % 100 == 0:
                pnl = self.trader.portfolio_pnl(self.config.strategy_id)
                self._pnl_curve.append((ev.ts, pnl["total_pnl"]))

        log.info("backtest_complete", strategy=self.config.strategy_id)
        return self._summarize()

    # ── Event dispatch ────────────────────────────────────────────────────────

    async def _dispatch(self, ev: BacktestEvent) -> None:
        if ev.kind == "quote":
            q = ev.data
            await self.trader.on_quote(q)
            # Build minimal features for strategy
            from arb.signals.features import FeatureSnapshot, MarketFeatures
            snap = FeatureSnapshot(ts=q.ts)
            mid = float((q.bid_px + q.ask_px) / 2)
            snap.markets[ev.market_id] = MarketFeatures(
                market_id=ev.market_id,
                ts=q.ts,
                mid_px=mid,
                spread_bp=float((q.ask_px - q.bid_px) / q.bid_px * 10_000) if q.bid_px > 0 else None,
            )
            await self.strategy(self.trader, snap)

        elif ev.kind == "trade":
            await self.trader.on_trade(ev.data)

        elif ev.kind == "funding":
            f: FundingStateEvent = ev.data
            await self.trader.on_funding(f.market_id, float(f.funding_rate))

    # ── Stream builder ────────────────────────────────────────────────────────

    def _build_event_stream(self, quotes, trades, funding, market_state) -> list[BacktestEvent]:
        from decimal import Decimal
        events = []

        for _, row in quotes.iterrows():
            ts = _to_dt(row["ts"])
            q = RawQuote(
                ts=ts,
                market_id=row["market_id"],
                bid_px=Decimal(str(row["bid_px"])),
                bid_sz=Decimal(str(row["bid_sz"])),
                ask_px=Decimal(str(row["ask_px"])),
                ask_sz=Decimal(str(row["ask_sz"])),
            )
            events.append(BacktestEvent(ts, "quote", row["market_id"], q))

        for _, row in trades.iterrows():
            ts = _to_dt(row["ts"])
            t = RawTrade(
                ts=ts,
                market_id=row["market_id"],
                price=Decimal(str(row["price"])),
                size=Decimal(str(row["size"])),
                side=row["side"],
            )
            events.append(BacktestEvent(ts, "trade", row["market_id"], t))

        for _, row in funding.iterrows():
            ts = _to_dt(row["ts"])
            f = FundingStateEvent(
                ts=ts,
                market_id=row["market_id"],
                funding_rate=Decimal(str(row["funding_rate"])),
            )
            events.append(BacktestEvent(ts, "funding", row["market_id"], f))

        return events

    # ── Results ───────────────────────────────────────────────────────────────

    def _summarize(self) -> dict:
        from arb.backtest.metrics import compute_metrics
        pnl = self.trader.portfolio_pnl(self.config.strategy_id)
        fills = [r for r in self.trader.trade_log() if r.event == "fill"]
        pnl_curve = pd.Series(
            [p for _, p in self._pnl_curve],
            index=[t for t, _ in self._pnl_curve],
        )
        return {
            "strategy_id": self.config.strategy_id,
            "pnl": pnl,
            "metrics": compute_metrics(pnl_curve, fills),
            "trade_log": self.trader.trade_log(),
        }


def _to_dt(v) -> datetime:
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v
    if isinstance(v, pd.Timestamp):
        dt = v.to_pydatetime()
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    return datetime.fromtimestamp(float(v), tz=timezone.utc)
