"""
Phase 0: Build market registry by querying Hyperliquid /info.

Usage:
    python -m arb.scripts.build_registry
    arb-registry  (if installed via pyproject.toml)

Output:
  - Writes registry to the market_registry table
  - Prints a markdown table to stdout
"""

from __future__ import annotations

import asyncio

import structlog
import typer
from rich.console import Console
from rich.table import Table

from arb.logging_config import configure_logging
from arb.market_data.client import HyperliquidClient
from arb.market_data.store import EventStore

log = structlog.get_logger(__name__)
console = Console()
app = typer.Typer()


async def _run(save_to_db: bool, filter_venue: str | None) -> None:
    configure_logging()
    client = HyperliquidClient()
    try:
        log.info("fetching_registry")
        markets = await client.build_registry()

        if filter_venue:
            markets = [m for m in markets if m.venue_label == filter_venue]

        log.info("registry_fetched", count=len(markets))

        # Print table
        table = Table(title=f"HIP-3 Market Registry ({len(markets)} markets)")
        table.add_column("market_id", style="cyan")
        table.add_column("venue")
        table.add_column("symbol")
        table.add_column("asset_class")
        table.add_column("collateral")
        table.add_column("oracle_type")
        table.add_column("fee_mode")
        table.add_column("leverage")
        table.add_column("active")

        for m in markets:
            table.add_row(
                m.market_id,
                m.venue_label,
                m.symbol,
                m.asset_class,
                m.collateral,
                m.oracle_type or "-",
                m.fee_mode or "-",
                str(m.max_leverage) if m.max_leverage else "-",
                "Y" if m.is_active else "N",
            )

        console.print(table)

        if save_to_db:
            store = EventStore()
            await store.upsert_registry(markets)
            log.info("registry_saved", count=len(markets))
        else:
            log.info("dry_run_skipped_db_write")

    finally:
        await client.close()


@app.command()
def main(
    save: bool = typer.Option(True, help="Save to database"),
    venue: str | None = typer.Option(None, help="Filter by venue label (xyz/felix/hl_native)"),
) -> None:
    """Build and display the HIP-3 market registry."""
    asyncio.run(_run(save_to_db=save, filter_venue=venue))


if __name__ == "__main__":
    app()
