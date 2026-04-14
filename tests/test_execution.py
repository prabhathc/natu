"""Tests for execution simulator."""

from __future__ import annotations

import time
from decimal import Decimal

import pytest

from arb.execution.models import Order, OrderType, Side
from arb.execution.simulator import ExecutionSimulator, FeeSchedule, SlippageModel


def _book(ts_ms: float, bid: float = 99.9, ask: float = 100.1, sz: float = 1.0):
    return {"bid": bid, "bid_sz": sz, "ask": ask, "ask_sz": sz, "ts_ms": ts_ms}


class TestExecutionSimulator:
    def test_market_buy_fills_immediately(self):
        sim = ExecutionSimulator()
        ts_ms = time.time() * 1000
        sim.on_quote("MKT", ts_ms, 99.9, 1.0, 100.1, 1.0)
        order = Order(strategy_id="s1", market_id="MKT", side=Side.BUY,
                      order_type=OrderType.MARKET, size=Decimal("0.1"))
        fill = sim.submit(order, ts_ms)
        assert fill is not None
        assert fill.side == Side.BUY
        assert float(fill.price) >= 100.1   # at or above ask (+ slippage)
        assert fill.is_maker is False

    def test_market_sell_fills_at_bid(self):
        sim = ExecutionSimulator()
        ts_ms = time.time() * 1000
        sim.on_quote("MKT", ts_ms, 99.9, 1.0, 100.1, 1.0)
        order = Order(strategy_id="s1", market_id="MKT", side=Side.SELL,
                      order_type=OrderType.MARKET, size=Decimal("0.1"))
        fill = sim.submit(order, ts_ms)
        assert fill is not None
        assert float(fill.price) <= 99.9

    def test_limit_buy_fills_when_ask_crosses(self):
        sim = ExecutionSimulator()
        ts_ms = time.time() * 1000
        sim.on_quote("MKT", ts_ms, 100.5, 1.0, 101.0, 1.0)
        order = Order(strategy_id="s1", market_id="MKT", side=Side.BUY,
                      order_type=OrderType.LIMIT, price=Decimal("100.8"), size=Decimal("0.1"))
        fill = sim.submit(order, ts_ms)
        assert fill is None, "Should not fill immediately when ask > limit"
        # Now market comes in below limit
        fills = sim.on_quote("MKT", ts_ms + 100, 100.2, 1.0, 100.7, 1.0)
        assert any(f.order_id == order.order_id for f in fills)

    def test_post_only_rejected_if_crosses(self):
        sim = ExecutionSimulator()
        ts_ms = time.time() * 1000
        sim.on_quote("MKT", ts_ms, 99.9, 1.0, 100.1, 1.0)
        order = Order(strategy_id="s1", market_id="MKT", side=Side.BUY,
                      order_type=OrderType.POST_ONLY, price=Decimal("100.5"), size=Decimal("0.1"))
        fill = sim.submit(order, ts_ms)
        assert fill is None
        from arb.execution.models import OrderStatus
        assert order.status == OrderStatus.REJECTED

    def test_fee_applied(self):
        sched = FeeSchedule(maker_bp=1.0, taker_bp=3.5)
        sim = ExecutionSimulator(fee_schedules={"MKT": sched})
        ts_ms = time.time() * 1000
        sim.on_quote("MKT", ts_ms, 99.9, 1.0, 100.1, 1.0)
        order = Order(strategy_id="s1", market_id="MKT", side=Side.BUY,
                      order_type=OrderType.MARKET, size=Decimal("1.0"))
        fill = sim.submit(order, ts_ms)
        assert fill is not None
        assert float(fill.fee_bp) == pytest.approx(3.5, abs=0.1)

    def test_stale_quote_rejected(self):
        sim = ExecutionSimulator(stale_quote_threshold_ms=100)
        old_ts = time.time() * 1000 - 200
        sim.on_quote("MKT", old_ts, 99.9, 1.0, 100.1, 1.0)
        order = Order(strategy_id="s1", market_id="MKT", side=Side.BUY,
                      order_type=OrderType.MARKET, size=Decimal("0.1"))
        fill = sim.submit(order, old_ts + 300)   # 300ms later
        from arb.execution.models import OrderStatus
        assert order.status == OrderStatus.REJECTED
        assert fill is None

    def test_cancel_removes_resting_order(self):
        sim = ExecutionSimulator()
        ts_ms = time.time() * 1000
        sim.on_quote("MKT", ts_ms, 99.9, 1.0, 100.1, 1.0)
        order = Order(strategy_id="s1", market_id="MKT", side=Side.BUY,
                      order_type=OrderType.LIMIT, price=Decimal("99.0"), size=Decimal("0.1"))
        sim.submit(order, ts_ms)
        assert order.order_id in sim._orders
        result = sim.cancel(order.order_id, ts_ms + 100)
        assert result is True
        assert order.order_id not in sim._orders
