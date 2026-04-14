"""
Phase 4: Run a backtest from stored event data.

Loads quote, trade, and funding data from the database for specified markets
and date range, then runs the chosen strategy through the event-driven engine.

Usage:
    arb-backtest --strategy spread_reversion --markets "hl:XAU-xyz::hl:XAU-felix"
                 --start 2025-01-01 --end 2025-02-01
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pandas as pd
import structlog
import typer
from rich.console import Console

from arb.backtest.engine import BacktestConfig, BacktestEngine
from arb.backtest.metrics import falsification_suite
from arb.execution.simulator import FeeSchedule, SlippageModel
from arb.logging_config import configure_logging
from arb.reporting.memos import GoNoGoMemo

log = structlog.get_logger(__name__)
console = Console()
app = typer.Typer()


# ── Strategy stubs ─────────────────────────────────────────────────────────────

async def strategy_spread_reversion(trader, snap) -> None:
    """
    Candidate 2: Spread reversion.
    Buy/sell when z-score exceeds threshold; exit on normalization.
    """
    for pair, z in snap.spread_z_scores.items():
        mkt_a, mkt_b = pair.split("::")
        from arb.execution.models import Order, OrderType, Side
        from decimal import Decimal

        mid_a = snap.markets.get(mkt_a)
        mid_b = snap.markets.get(mkt_b)
        if not mid_a or not mid_b or not mid_a.mid_px or not mid_b.mid_px:
            continue

        size = Decimal("0.01")   # placeholder; real sizing from risk engine

        if z > 2.0:
            # short A, long B
            for mkt, side in [(mkt_a, Side.SELL), (mkt_b, Side.BUY)]:
                order = Order(
                    strategy_id="spread_reversion",
                    market_id=mkt,
                    side=side,
                    order_type=OrderType.MARKET,
                    size=size,
                )
                trader.submit(order)
        elif z < -2.0:
            # long A, short B
            for mkt, side in [(mkt_a, Side.BUY), (mkt_b, Side.SELL)]:
                order = Order(
                    strategy_id="spread_reversion",
                    market_id=mkt,
                    side=side,
                    order_type=OrderType.MARKET,
                    size=size,
                )
                trader.submit(order)


async def strategy_lead_lag(trader, snap) -> None:
    """
    Candidate 1: Lead-lag catcher.
    When a confirmed leader moves, cross into the follower.
    """
    for pair, signal in snap.lead_lag_signals.items():
        # signal format: "leader→follower@XXXms"
        pass    # implementation fills in from research phase


STRATEGIES = {
    "spread_reversion": strategy_spread_reversion,
    "lead_lag": strategy_lead_lag,
}


async def _load_data(markets: list[str], start: datetime, end: datetime) -> tuple:
    """Load event data from the database. Returns (quotes_df, trades_df, funding_df)."""
    from arb.db import get_engine
    import sqlalchemy as sa

    engine = get_engine()
    async with engine.connect() as conn:
        quotes = pd.DataFrame(
            (await conn.execute(
                sa.text("""
                    SELECT ts, market_id, bid_px, bid_sz, ask_px, ask_sz
                    FROM raw_quotes
                    WHERE market_id = ANY(:mkts) AND ts BETWEEN :start AND :end
                    ORDER BY ts
                """),
                {"mkts": markets, "start": start, "end": end},
            )).fetchall(),
            columns=["ts", "market_id", "bid_px", "bid_sz", "ask_px", "ask_sz"],
        )

        trades = pd.DataFrame(
            (await conn.execute(
                sa.text("""
                    SELECT ts, market_id, price, size, side
                    FROM raw_trades
                    WHERE market_id = ANY(:mkts) AND ts BETWEEN :start AND :end
                    ORDER BY ts
                """),
                {"mkts": markets, "start": start, "end": end},
            )).fetchall(),
            columns=["ts", "market_id", "price", "size", "side"],
        )

        funding = pd.DataFrame(
            (await conn.execute(
                sa.text("""
                    SELECT ts, market_id, funding_rate
                    FROM funding_state
                    WHERE market_id = ANY(:mkts) AND ts BETWEEN :start AND :end
                    ORDER BY ts
                """),
                {"mkts": markets, "start": start, "end": end},
            )).fetchall(),
            columns=["ts", "market_id", "funding_rate"],
        )

    return quotes, trades, funding


async def _run(
    strategy_name: str,
    markets: list[str],
    start: datetime,
    end: datetime,
    slippage_mult: float,
    fee_mult: float,
) -> None:
    configure_logging()

    strategy_fn = STRATEGIES.get(strategy_name)
    if strategy_fn is None:
        console.print(f"[red]Unknown strategy: {strategy_name}[/red]")
        console.print(f"Available: {', '.join(STRATEGIES)}")
        raise typer.Exit(1)

    log.info("loading_data", markets=markets, start=str(start), end=str(end))
    quotes, trades, funding = await _load_data(markets, start, end)

    log.info(
        "data_loaded",
        quotes=len(quotes),
        trades=len(trades),
        funding=len(funding),
    )

    config = BacktestConfig(
        strategy_id=strategy_name,
        market_ids=markets,
        start=start,
        end=end,
        slippage_multiplier=slippage_mult,
        fee_multiplier=fee_mult,
    )

    engine = BacktestEngine(config, strategy_fn)
    results = await engine.run(quotes, trades, funding)

    m = results["metrics"]
    console.print(f"\n[bold]Backtest Results: {strategy_name}[/bold]")
    console.print(f"Net PnL:       {m.net_pnl:,.4f}")
    console.print(f"Gross PnL:     {m.gross_pnl:,.4f}")
    console.print(f"Total Fees:    {m.total_fees:,.4f}")
    console.print(f"Sharpe:        {m.sharpe:.2f}" if m.sharpe else "Sharpe:        n/a")
    console.print(f"Max Drawdown:  {m.max_drawdown:,.4f}")
    console.print(f"Hit Rate:      {m.hit_rate:.1%}")
    console.print(f"Trades:        {m.n_trades}")


@app.command()
def main(
    strategy: str = typer.Option("spread_reversion", help=f"Strategy: {', '.join(STRATEGIES)}"),
    markets: str = typer.Option(..., help="Comma-separated market IDs"),
    start: str = typer.Option(..., help="Start date YYYY-MM-DD"),
    end: str = typer.Option(..., help="End date YYYY-MM-DD"),
    slippage_mult: float = typer.Option(1.0, help="Slippage multiplier (2.0 for stress test)"),
    fee_mult: float = typer.Option(1.0, help="Fee multiplier (2.0 for stress test)"),
) -> None:
    """Run an event-driven backtest."""
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
    market_list = [m.strip() for m in markets.split(",")]

    asyncio.run(_run(strategy, market_list, start_dt, end_dt, slippage_mult, fee_mult))


if __name__ == "__main__":
    app()
