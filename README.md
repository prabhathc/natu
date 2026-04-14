<div align="center">
  <img src="https://raw.githubusercontent.com/PokeAPI/sprites/master/sprites/pokemon/versions/generation-v/black-white/animated/177.gif" alt="Natu" width="96" />
  <h1>natu</h1>
  <p><em>stares at the market. sees what moves first.</em></p>
</div>

---

HIP-3 Cross-Venue Basis and Lead-Lag Validation Program for Hyperliquid. Systematically tests four edge hypotheses across markets surfaced by Trade[XYZ] and Felix before any capital is deployed.

## What it does

Four hypotheses, tested independently:

| | Hypothesis | Edge type |
|---|---|---|
| A | Lead-lag price discovery | Informational |
| B | Cross-venue spread mean reversion | Structural |
| C | Delta-neutral funding carry | Carry |
| D | Smarter routing and inventory control | Execution |

The framework is designed to **prove or kill** each hypothesis, not to assume it.

## Phases

```
0  Market registry         → catalog all HIP-3 markets (XYZ, Felix, native)
1  Data engineering        → quotes, trades, mark, oracle, funding, OI
2  Hypothesis testing      → A/B/C/D notebooks with empirical validation
3  Strategy candidates     → four implementations derived from findings
4  Event-driven backtest   → realistic fees, slippage, queue position
5  Paper trading           → ≥2 weeks live, compare to backtest
6  Small-capital live test → constrained exposure with hard kill switches
```

## Structure

```
src/arb/
├── market_data/    # collector, client, normalizer, event store
├── signals/        # spread calculator, lead-lag detector, funding analyzer, feature engine
├── execution/      # simulator, paper trader, order models
├── risk/           # controls, circuit breaker
├── portfolio/      # PnL ledger
├── backtest/       # event-driven engine, metrics, falsification suite
├── reporting/      # weekly memo, go/no-go memo
└── scripts/        # collect, build_registry, run_backtest
notebooks/          # one notebook per hypothesis + registry + data quality + weekly memo
sql/schema.sql      # full TimescaleDB schema
```

## Quickstart

```bash
cp .env.example .env          # fill in HL_WALLET_ADDRESS if needed
docker compose up -d          # postgres (timescaledb) + redis
pip install -e ".[dev]"

arb-registry                  # Phase 0: fetch and display market registry
arb-collect                   # Phase 1: start data collector
arb-backtest --help           # Phase 4: run a backtest
```

## Running tests

```bash
pytest tests/ -v
```

## Key design decisions

- **No candle-only backtests.** Everything replays the raw event stream.
- **Falsification is mandatory.** The backtest engine ships with 2x slippage shock, 2x fee shock, and latency shock tests built in.
- **Edge is not assumed.** If the go/no-go criteria aren't met, the correct answer is *do not fund*, not *optimize harder*.

## Go/No-Go criteria

The strategy only gets funded if **all** of the following are true:

- [ ] Edge positive after realistic fees and slippage
- [ ] Results hold across ≥2 materially different regimes
- [ ] Live paper performance resembles backtest
- [ ] Orphan-leg and liquidity risks are tolerable
- [ ] Does not rely on one freak market or one freak week
- [ ] Operational complexity proportional to expected return
