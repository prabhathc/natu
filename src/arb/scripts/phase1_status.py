"""
Phase 1 readiness status report.

Summarizes ingestion volume, active coverage, and data-gap rates for a lookback
window. Provides a simple PASS/WARN signal for moving into Phase 2 analysis.
"""

from __future__ import annotations

import asyncio

import sqlalchemy as sa
import typer
from rich.console import Console
from rich.table import Table

from arb.db import get_engine

app = typer.Typer()
console = Console()


async def _query(hours: int) -> dict:
    engine = get_engine()
    h = max(1, int(hours))
    q = sa.text(
        """
        WITH
        q AS (
          SELECT COUNT(*) AS rows, COUNT(DISTINCT market_id) AS markets
          FROM raw_quotes
          WHERE ts >= NOW() - make_interval(hours => :hours)
        ),
        t AS (
          SELECT COUNT(*) AS rows, COUNT(DISTINCT market_id) AS markets
          FROM raw_trades
          WHERE ts >= NOW() - make_interval(hours => :hours)
        ),
        m AS (
          SELECT COUNT(*) AS rows, COUNT(DISTINCT market_id) AS markets
          FROM market_state
          WHERE ts >= NOW() - make_interval(hours => :hours)
        ),
        f AS (
          SELECT COUNT(*) AS rows, COUNT(DISTINCT market_id) AS markets
          FROM funding_state
          WHERE ts >= NOW() - make_interval(hours => :hours)
        ),
        r AS (
          SELECT COUNT(*) AS rows, COUNT(DISTINCT symbol) AS symbols
          FROM reference_state
          WHERE ts >= NOW() - make_interval(hours => :hours)
        ),
        g AS (
          SELECT COUNT(*) AS gaps
          FROM data_gaps
          WHERE detected_at >= NOW() - make_interval(hours => :hours)
        ),
        reg AS (
          SELECT COUNT(*) AS total_registry_markets
          FROM market_registry
          WHERE is_active = TRUE
        )
        SELECT
          q.rows AS q_rows, q.markets AS q_markets,
          t.rows AS t_rows, t.markets AS t_markets,
          m.rows AS m_rows, m.markets AS m_markets,
          f.rows AS f_rows, f.markets AS f_markets,
          r.rows AS r_rows, r.symbols AS r_symbols,
          g.gaps AS gaps, reg.total_registry_markets AS total_registry_markets
        FROM q, t, m, f, r, g, reg
        """
    )
    async with engine.connect() as conn:
        row = (await conn.execute(q, {"hours": h})).mappings().first()
    return dict(row) if row else {}


@app.command()
def main(
    hours: int = typer.Option(24, help="Lookback window in hours"),
    min_coverage_ratio: float = typer.Option(0.5, help="Minimum quote market coverage ratio for PASS"),
    max_gaps: int = typer.Option(100, help="Maximum allowed data gaps in window for PASS"),
) -> None:
    """Print Phase 1 ingestion health and readiness signal."""
    data = asyncio.run(_query(hours))
    if not data:
        console.print("[red]No data returned.[/red]")
        raise typer.Exit(1)

    total_registry = max(1, int(data["total_registry_markets"]))
    quote_coverage = float(data["q_markets"]) / total_registry
    pass_flag = quote_coverage >= min_coverage_ratio and int(data["gaps"]) <= max_gaps

    t = Table(title=f"Phase 1 Status (last {hours}h)")
    t.add_column("Metric")
    t.add_column("Value", justify="right")
    t.add_row("Active registry markets", str(total_registry))
    t.add_row("Quotes rows", str(data["q_rows"]))
    t.add_row("Quotes distinct markets", str(data["q_markets"]))
    t.add_row("Trades rows", str(data["t_rows"]))
    t.add_row("Trades distinct markets", str(data["t_markets"]))
    t.add_row("Market-state rows", str(data["m_rows"]))
    t.add_row("Funding rows", str(data["f_rows"]))
    t.add_row("Reference rows", str(data["r_rows"]))
    t.add_row("Data gaps", str(data["gaps"]))
    t.add_row("Quote market coverage", f"{quote_coverage:.1%}")
    t.add_row("Readiness", "PASS" if pass_flag else "WARN")
    console.print(t)


if __name__ == "__main__":
    app()

