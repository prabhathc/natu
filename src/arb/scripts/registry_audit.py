"""
Registry drift audit.

Builds the live registry, snapshots it locally, and reports changes versus
the previous snapshot so venue coverage changes are explicit and reviewable.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from arb.market_data.client import HyperliquidClient
from arb.market_data.models import MarketRegistry

app = typer.Typer()
console = Console()


@dataclass
class RegistrySnapshot:
    generated_at: str
    total_markets: int
    venue_counts: dict[str, int]
    market_ids: list[str]
    symbols_by_venue: dict[str, list[str]]


def build_snapshot(records: list[MarketRegistry]) -> RegistrySnapshot:
    venue_counts: dict[str, int] = {}
    symbols_by_venue: dict[str, set[str]] = {}
    market_ids = sorted(r.market_id for r in records)
    for r in records:
        venue_counts[r.venue_label] = venue_counts.get(r.venue_label, 0) + 1
        symbols_by_venue.setdefault(r.venue_label, set()).add(r.symbol)
    return RegistrySnapshot(
        generated_at=datetime.now(tz=timezone.utc).isoformat(),
        total_markets=len(records),
        venue_counts=dict(sorted(venue_counts.items())),
        market_ids=market_ids,
        symbols_by_venue={k: sorted(v) for k, v in sorted(symbols_by_venue.items())},
    )


def load_snapshot(path: Path) -> RegistrySnapshot | None:
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return RegistrySnapshot(
        generated_at=data["generated_at"],
        total_markets=data["total_markets"],
        venue_counts=data["venue_counts"],
        market_ids=data["market_ids"],
        symbols_by_venue=data["symbols_by_venue"],
    )


def diff_snapshots(prev: RegistrySnapshot | None, cur: RegistrySnapshot) -> dict:
    if prev is None:
        return {
            "added_market_ids": cur.market_ids,
            "removed_market_ids": [],
            "venue_count_delta": {k: cur.venue_counts.get(k, 0) for k in cur.venue_counts},
            "new_symbols_by_venue": cur.symbols_by_venue,
            "removed_symbols_by_venue": {},
        }

    prev_ids = set(prev.market_ids)
    cur_ids = set(cur.market_ids)
    venues = sorted(set(prev.venue_counts) | set(cur.venue_counts))
    venue_count_delta = {v: cur.venue_counts.get(v, 0) - prev.venue_counts.get(v, 0) for v in venues}

    new_symbols_by_venue: dict[str, list[str]] = {}
    removed_symbols_by_venue: dict[str, list[str]] = {}
    for v in sorted(set(prev.symbols_by_venue) | set(cur.symbols_by_venue)):
        prev_syms = set(prev.symbols_by_venue.get(v, []))
        cur_syms = set(cur.symbols_by_venue.get(v, []))
        added = sorted(cur_syms - prev_syms)
        removed = sorted(prev_syms - cur_syms)
        if added:
            new_symbols_by_venue[v] = added
        if removed:
            removed_symbols_by_venue[v] = removed

    return {
        "added_market_ids": sorted(cur_ids - prev_ids),
        "removed_market_ids": sorted(prev_ids - cur_ids),
        "venue_count_delta": venue_count_delta,
        "new_symbols_by_venue": new_symbols_by_venue,
        "removed_symbols_by_venue": removed_symbols_by_venue,
    }


def render_report(cur: RegistrySnapshot, diff: dict) -> str:
    lines: list[str] = []
    lines.append(f"# Registry Audit ({cur.generated_at})")
    lines.append("")
    lines.append(f"- Total markets: **{cur.total_markets}**")
    lines.append("- Venue counts:")
    for venue, count in cur.venue_counts.items():
        delta = diff["venue_count_delta"].get(venue, 0)
        sign = f"+{delta}" if delta >= 0 else str(delta)
        lines.append(f"  - `{venue}`: {count} ({sign} vs previous)")
    lines.append("")
    lines.append(f"- Added market IDs: **{len(diff['added_market_ids'])}**")
    lines.append(f"- Removed market IDs: **{len(diff['removed_market_ids'])}**")
    lines.append("")
    if diff["new_symbols_by_venue"]:
        lines.append("## New symbols by venue")
        for venue, syms in diff["new_symbols_by_venue"].items():
            lines.append(f"- `{venue}`: {', '.join(syms)}")
        lines.append("")
    if diff["removed_symbols_by_venue"]:
        lines.append("## Removed symbols by venue")
        for venue, syms in diff["removed_symbols_by_venue"].items():
            lines.append(f"- `{venue}`: {', '.join(syms)}")
        lines.append("")
    if not diff["new_symbols_by_venue"] and not diff["removed_symbols_by_venue"]:
        lines.append("No symbol-level venue drift detected.")
        lines.append("")
    return "\n".join(lines)


async def _run(snapshot_dir: Path, report_dir: Path, write_latest: bool) -> None:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    client = HyperliquidClient()
    try:
        records = await client.build_registry()
    finally:
        await client.close()

    cur = build_snapshot(records)
    latest_snapshot = snapshot_dir / "latest.json"
    prev = load_snapshot(latest_snapshot)
    diff = diff_snapshots(prev, cur)

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    point_in_time_snapshot = snapshot_dir / f"registry-{ts}.json"
    point_in_time_snapshot.write_text(json.dumps(asdict(cur), indent=2))
    latest_snapshot.write_text(json.dumps(asdict(cur), indent=2))

    report = render_report(cur, diff)
    point_in_time_report = report_dir / f"registry-audit-{ts}.md"
    point_in_time_report.write_text(report)
    if write_latest:
        (report_dir / "latest.md").write_text(report)

    table = Table(title="Registry Audit Summary")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Total markets", str(cur.total_markets))
    table.add_row("Added market IDs", str(len(diff["added_market_ids"])))
    table.add_row("Removed market IDs", str(len(diff["removed_market_ids"])))
    table.add_row("Snapshot", str(point_in_time_snapshot))
    table.add_row("Report", str(point_in_time_report))
    console.print(table)


@app.command()
def main(
    snapshot_dir: str = typer.Option("data/registry_snapshots", help="Snapshot output directory"),
    report_dir: str = typer.Option("reports/registry_audit", help="Markdown report output directory"),
    write_latest: bool = typer.Option(True, "--write-latest/--no-write-latest", help="Write latest.md report"),
) -> None:
    """Build live registry snapshot and report drift."""
    asyncio.run(_run(Path(snapshot_dir), Path(report_dir), write_latest))


if __name__ == "__main__":
    app()

