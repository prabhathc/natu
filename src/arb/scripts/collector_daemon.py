"""
Daemon manager for long-running market collection.

Features:
- start/stop/restart/status/progress commands
- supervisor process that keeps collector alive (auto-restart)
- persistent config/state files for reproducibility
- optional cron @reboot schedule helper
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import sqlalchemy as sa
import typer
from rich.console import Console
from rich.table import Table

from arb.db import get_engine

app = typer.Typer()
console = Console()

STATE_DIR = Path.home() / ".arb" / "collector-daemon"
PID_FILE = STATE_DIR / "supervisor.pid"
CHILD_PID_FILE = STATE_DIR / "collector.pid"
CONFIG_FILE = STATE_DIR / "config.json"
SUPERVISOR_LOG = STATE_DIR / "supervisor.log"
COLLECTOR_LOG = STATE_DIR / "collector.log"


@dataclass
class DaemonConfig:
    markets: str = ""
    references: str = "SPX,XAU,TSLA,NVDA"
    reference_poll_s: float = 60.0
    flush_interval: float = 1.0
    restart_delay_s: float = 5.0

    def as_dict(self) -> dict:
        return {
            "markets": self.markets,
            "references": self.references,
            "reference_poll_s": self.reference_poll_s,
            "flush_interval": self.flush_interval,
            "restart_delay_s": self.restart_delay_s,
        }


def _ensure_state_dir() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def _read_pid(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        return int(path.read_text().strip())
    except Exception:
        return None


def _pid_is_running(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _write_pid(path: Path, pid: int) -> None:
    path.write_text(str(pid))


def _clear_pid(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def _load_config() -> DaemonConfig | None:
    if not CONFIG_FILE.exists():
        return None
    try:
        data = json.loads(CONFIG_FILE.read_text())
        return DaemonConfig(
            markets=data.get("markets", ""),
            references=data.get("references", "SPX,XAU,TSLA,NVDA"),
            reference_poll_s=float(data.get("reference_poll_s", 60.0)),
            flush_interval=float(data.get("flush_interval", 1.0)),
            restart_delay_s=float(data.get("restart_delay_s", 5.0)),
        )
    except Exception:
        return None


def _save_config(cfg: DaemonConfig) -> None:
    CONFIG_FILE.write_text(json.dumps(cfg.as_dict(), indent=2))


def _build_collector_cmd(cfg: DaemonConfig) -> list[str]:
    return [
        sys.executable,
        "-m",
        "arb.scripts.collect",
        "--markets",
        cfg.markets,
        "--references",
        cfg.references,
        "--reference-poll-s",
        str(cfg.reference_poll_s),
        "--flush-interval",
        str(cfg.flush_interval),
    ]


def _spawn_collector(cfg: DaemonConfig) -> subprocess.Popen:
    _ensure_state_dir()
    logf = open(COLLECTOR_LOG, "a", buffering=1)
    cmd = _build_collector_cmd(cfg)
    proc = subprocess.Popen(
        cmd,
        stdout=logf,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        close_fds=True,
    )
    _write_pid(CHILD_PID_FILE, proc.pid)
    return proc


async def _query_progress(hours: int) -> dict:
    engine = get_engine()
    lookback_hours = max(1, int(hours))
    query = sa.text(
        """
        WITH q AS (
            SELECT COUNT(*) AS n, COUNT(DISTINCT market_id) AS d
            FROM raw_quotes
            WHERE ts >= NOW() - make_interval(hours => :hours)
        ),
        t AS (
            SELECT COUNT(*) AS n, COUNT(DISTINCT market_id) AS d
            FROM raw_trades
            WHERE ts >= NOW() - make_interval(hours => :hours)
        ),
        m AS (
            SELECT COUNT(*) AS n, COUNT(DISTINCT market_id) AS d
            FROM market_state
            WHERE ts >= NOW() - make_interval(hours => :hours)
        ),
        f AS (
            SELECT COUNT(*) AS n, COUNT(DISTINCT market_id) AS d
            FROM funding_state
            WHERE ts >= NOW() - make_interval(hours => :hours)
        ),
        r AS (
            SELECT COUNT(*) AS n, COUNT(DISTINCT symbol) AS d
            FROM reference_state
            WHERE ts >= NOW() - make_interval(hours => :hours)
        )
        SELECT
            q.n AS quotes_rows, q.d AS quotes_markets,
            t.n AS trades_rows, t.d AS trades_markets,
            m.n AS mstate_rows, m.d AS mstate_markets,
            f.n AS funding_rows, f.d AS funding_markets,
            r.n AS reference_rows, r.d AS reference_symbols
        FROM q, t, m, f, r
        """
    )
    async with engine.connect() as conn:
        row = (await conn.execute(query, {"hours": lookback_hours})).mappings().first()
    return dict(row) if row else {}


def _tail(path: Path, lines: int = 20) -> list[str]:
    if not path.exists():
        return []
    content = path.read_text(errors="ignore").splitlines()
    return content[-lines:]


@app.command()
def start(
    markets: str = typer.Option("", help="Collector markets argument (empty=all registry)"),
    references: str = typer.Option("SPX,XAU,TSLA,NVDA", help="Reference symbols"),
    reference_poll_s: float = typer.Option(60.0, help="Reference polling seconds"),
    flush_interval: float = typer.Option(1.0, help="Flush interval seconds"),
    restart_delay_s: float = typer.Option(5.0, help="Supervisor restart delay seconds"),
) -> None:
    """Start collector supervisor in background."""
    _ensure_state_dir()
    running = _pid_is_running(_read_pid(PID_FILE))
    if running:
        console.print("[yellow]Collector daemon already running.[/yellow]")
        return

    cfg = DaemonConfig(
        markets=markets,
        references=references,
        reference_poll_s=reference_poll_s,
        flush_interval=flush_interval,
        restart_delay_s=restart_delay_s,
    )
    _save_config(cfg)
    cmd = [sys.executable, "-m", "arb.scripts.collector_daemon", "_run_supervisor"]
    with open(SUPERVISOR_LOG, "a", buffering=1) as logf:
        proc = subprocess.Popen(
            cmd,
            stdout=logf,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    _write_pid(PID_FILE, proc.pid)
    console.print(f"[green]Collector daemon started[/green] (supervisor pid={proc.pid})")
    console.print(f"Logs: {SUPERVISOR_LOG}")


@app.command()
def stop() -> None:
    """Stop collector supervisor and child."""
    sup_pid = _read_pid(PID_FILE)
    if not _pid_is_running(sup_pid):
        console.print("[yellow]Collector daemon is not running.[/yellow]")
        _clear_pid(PID_FILE)
        _clear_pid(CHILD_PID_FILE)
        return
    assert sup_pid is not None
    os.kill(sup_pid, signal.SIGTERM)
    deadline = time.time() + 10
    while time.time() < deadline and _pid_is_running(sup_pid):
        time.sleep(0.2)
    console.print("[green]Collector daemon stopped.[/green]")


@app.command()
def restart() -> None:
    """Restart collector supervisor using saved config."""
    cfg = _load_config()
    stop()
    if cfg is None:
        cfg = DaemonConfig()
    start(
        markets=cfg.markets,
        references=cfg.references,
        reference_poll_s=cfg.reference_poll_s,
        flush_interval=cfg.flush_interval,
        restart_delay_s=cfg.restart_delay_s,
    )


@app.command()
def status(log_lines: int = typer.Option(10, help="Number of log lines to show")) -> None:
    """Show daemon status and recent logs."""
    sup_pid = _read_pid(PID_FILE)
    child_pid = _read_pid(CHILD_PID_FILE)
    sup_running = _pid_is_running(sup_pid)
    child_running = _pid_is_running(child_pid)

    table = Table(title="Collector Daemon Status")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("Supervisor PID", str(sup_pid or "-"))
    table.add_row("Supervisor running", "yes" if sup_running else "no")
    table.add_row("Collector PID", str(child_pid or "-"))
    table.add_row("Collector running", "yes" if child_running else "no")
    table.add_row("State dir", str(STATE_DIR))
    table.add_row("Config file", str(CONFIG_FILE))
    console.print(table)

    if CONFIG_FILE.exists():
        console.print("\n[bold]Saved config[/bold]")
        console.print(CONFIG_FILE.read_text())

    console.print("\n[bold]Supervisor log tail[/bold]")
    for line in _tail(SUPERVISOR_LOG, lines=log_lines):
        console.print(line)

    console.print("\n[bold]Collector log tail[/bold]")
    for line in _tail(COLLECTOR_LOG, lines=log_lines):
        console.print(line)


@app.command()
def progress(
    hours: int = typer.Option(1, help="Lookback window in hours"),
) -> None:
    """Show ingestion progress from the database."""
    data = asyncio.run(_query_progress(hours))
    if not data:
        console.print("[red]No progress data returned.[/red]")
        raise typer.Exit(1)

    table = Table(title=f"Collection Progress (last {hours}h)")
    table.add_column("Metric")
    table.add_column("Rows", justify="right")
    table.add_column("Distinct IDs", justify="right")
    table.add_row("raw_quotes", str(data["quotes_rows"]), str(data["quotes_markets"]))
    table.add_row("raw_trades", str(data["trades_rows"]), str(data["trades_markets"]))
    table.add_row("market_state", str(data["mstate_rows"]), str(data["mstate_markets"]))
    table.add_row("funding_state", str(data["funding_rows"]), str(data["funding_markets"]))
    table.add_row("reference_state", str(data["reference_rows"]), str(data["reference_symbols"]))
    console.print(table)


@app.command()
def install_reboot_cron(apply: bool = typer.Option(False, help="Apply directly to user crontab")) -> None:
    """Install (or print) @reboot cron entry to start daemon on boot."""
    cmd = f'@reboot cd "{Path.cwd()}" && {sys.executable} -m arb.scripts.collector_daemon start >/dev/null 2>&1'
    if not apply:
        console.print("Add this to your user crontab:")
        console.print(cmd)
        return

    try:
        existing = subprocess.check_output(["crontab", "-l"], text=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        existing = ""
    lines = [ln for ln in existing.splitlines() if ln.strip()]
    if cmd not in lines:
        lines.append(cmd)
    payload = "\n".join(lines) + "\n"
    subprocess.run(["crontab", "-"], input=payload, text=True, check=True)
    console.print("[green]Installed @reboot collector daemon entry in crontab.[/green]")


@app.command("_run_supervisor", hidden=True)
def run_supervisor() -> None:
    """Internal: run supervisor loop in foreground."""
    _ensure_state_dir()
    cfg = _load_config() or DaemonConfig()
    _write_pid(PID_FILE, os.getpid())
    child: subprocess.Popen | None = None
    stop_flag = False

    def _handle_term(_sig: int, _frame: object) -> None:
        nonlocal stop_flag
        stop_flag = True

    signal.signal(signal.SIGTERM, _handle_term)
    signal.signal(signal.SIGINT, _handle_term)

    print(f"{datetime.now(tz=timezone.utc).isoformat()} supervisor_started cfg={cfg.as_dict()}", flush=True)

    try:
        while not stop_flag:
            if child is None or child.poll() is not None:
                if child is not None:
                    print(
                        f"{datetime.now(tz=timezone.utc).isoformat()} collector_exited rc={child.returncode}",
                        flush=True,
                    )
                    if stop_flag:
                        break
                    time.sleep(cfg.restart_delay_s)
                child = _spawn_collector(cfg)
                print(
                    f"{datetime.now(tz=timezone.utc).isoformat()} collector_started pid={child.pid}",
                    flush=True,
                )
            time.sleep(1.0)
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
        _clear_pid(CHILD_PID_FILE)
        _clear_pid(PID_FILE)
        print(f"{datetime.now(tz=timezone.utc).isoformat()} supervisor_stopped", flush=True)


if __name__ == "__main__":
    app()


def main() -> None:
    app()

