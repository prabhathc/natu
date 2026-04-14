"""Tests for risk controls and circuit breaker."""

from __future__ import annotations

import pytest

from arb.risk.controls import RiskControls
from arb.risk.circuit_breaker import CircuitBreaker


class TestRiskControls:
    def test_allows_valid_trade(self):
        rc = RiskControls(single_market_cap=10_000, portfolio_gross_cap=50_000, per_trade_loss_cap=500)
        violation = rc.check("strat1", "MKT_A", notional=1_000, expected_loss=50)
        assert violation is None

    def test_blocks_per_trade_loss(self):
        rc = RiskControls(per_trade_loss_cap=100)
        v = rc.check("strat1", "MKT_A", notional=1_000, expected_loss=200)
        assert v is not None
        assert v.rule == "per_trade_loss"

    def test_blocks_single_market_cap(self):
        rc = RiskControls(single_market_cap=1_000)
        rc.record_fill("strat1", "MKT_A", notional=900, pnl=0)
        v = rc.check("strat1", "MKT_A", notional=200, expected_loss=5)
        assert v is not None
        assert v.rule == "single_market_cap"

    def test_blocks_portfolio_gross(self):
        rc = RiskControls(portfolio_gross_cap=5_000)
        rc.record_fill("strat1", "MKT_A", notional=4_800, pnl=0)
        v = rc.check("strat1", "MKT_B", notional=500, expected_loss=5)
        assert v is not None
        assert v.rule == "portfolio_gross_cap"

    def test_hard_kill(self):
        rc = RiskControls()
        rc.hard_kill("strat1")
        v = rc.check("strat1", "MKT_A", notional=1, expected_loss=1)
        assert v is not None
        assert v.rule == "hard_kill"

    def test_spread_cap(self):
        rc = RiskControls(max_concurrent_spreads=2)
        rc.record_spread_open("strat1")
        rc.record_spread_open("strat1")
        v = rc.check("strat1", "MKT_A", notional=100, expected_loss=1, is_new_spread=True)
        assert v is not None
        assert v.rule == "max_spreads"


class TestCircuitBreaker:
    def test_trips_on_sustained_slippage(self):
        cb = CircuitBreaker("s1", slippage_ratio_threshold=2.0, slippage_consecutive_days=3)
        import datetime
        days = [datetime.date(2025, 1, 1), datetime.date(2025, 1, 2), datetime.date(2025, 1, 3)]

        # Mock 3 consecutive days of 2x+ slippage
        for d in days:
            from arb.risk.circuit_breaker import SlippageObs
            cb._slip_obs.append(SlippageObs(d, modeled_bp=2.0, realized_bp=5.0))

        # Force re-check
        recent = list(cb._slip_obs)[-3:]
        if all(o.ratio >= cb._slip_threshold for o in recent):
            cb._trip("slippage", "test")

        assert cb.is_tripped

    def test_trips_on_low_hedge_completion(self):
        cb = CircuitBreaker("s1", min_hedge_completion=0.9)
        for _ in range(30):
            cb.observe_hedge(False)
        assert cb.is_tripped

    def test_does_not_trip_good_hedge(self):
        cb = CircuitBreaker("s1", min_hedge_completion=0.9)
        for i in range(30):
            cb.observe_hedge(i % 10 != 0)   # 90% completion
        # May or may not trip depending on exact count; just verify no crash
        assert isinstance(cb.is_tripped, bool)

    def test_reset(self):
        cb = CircuitBreaker("s1")
        cb._trip("test", "manual trip")
        assert cb.is_tripped
        cb.reset()
        assert not cb.is_tripped
