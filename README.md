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

---

## Phase 0 findings — market discovery

Running `arb-registry` against the live Hyperliquid API revealed the actual universe:

| Venue | Count | Markets |
|---|---|---|
| **hl_native** | 229 perps | BTC, ETH, SOL, **SPX, NQ, QQQ** (index), **XAUT0** (gold proxy), and 223 others |
| **trade[XYZ] / Wagyu.xyz** | 5 tokens | **TSLA, NVDA, SPACEX** (equity), XMR1, TRADE |
| **Felix** | 1 token | FEUSD (their USD stablecoin — not a trading market yet) |
| **Unit Protocol** | 15 tokens | Wrapped crypto: UBTC, UETH, USOL, USPYX (SP500), UFART, etc. |
| **Other deployers** | ~180 tokens | AAPL, GOOGL, AMZN, META, SPY, QQQ, GLD, SLV and many more — deployer unknown |

**Key architectural finding:** HIP-3 markets do not appear in the `/info type=metaAndAssetCtxs` perp endpoint. They live in the **spot universe** (`/info type=spotMeta`) as tokens with `deployerTradingFeeShare > 0`.

**Hypothesis revision:** Felix should be treated as an active venue bucket in our registry (not FEUSD-only). Recent live registry snapshots classify a wider Felix set (e.g. AAPL/AMZN/GOOGL/META/MSFT/SPY/QQQ/GLD/SLV/QQQM/HOOD/BNB1/FEUSD), and research focus should be updated accordingly. The real cross-venue pairs are:

1. `hl-perp:SPX` (native index perp) ↔ `SPY` / `QQQ` spot deployer tokens — same underlying, different oracle and fee mechanics
2. `hl-perp:NQ` / `hl-perp:QQQ` ↔ `QQQM` (Melt) — Nasdaq basis
3. `XAUT0` (gold proxy commodity perp) ↔ `GLD` spot token ↔ external gold reference
4. `TSLA` / `NVDA` (XYZ equity tokens) ↔ their TradFi reference prices

Felix capability context (platform docs):
- Spot equities are a core Felix product and currently framed as broad tokenized US equity/ETF access.
- Felix docs also describe perpetual futures and lending rails (CDP/feUSD and vanilla markets).
- Our project still needs to distinguish between "Felix platform capabilities" and "markets currently discoverable/tradable through our Hyperliquid-focused ingestion path."

---

## Phase 1 status — data collection live

Collector supports mixed universes:
- native perps (e.g. `BTC`, `SPX`, `hl-perp:ETH`)
- HIP-3 spot deployer pairs (e.g. `@279`, `hl-spot:@279`)
- symbol resolution through registry (e.g. `SPX` can resolve to both perp + spot markets)

To start collecting for your focus markets:
```bash
arb-collect --markets "SPX,@279,@288,BTC,ETH,SOL" --references "SPX,XAU,TSLA,NVDA"
```

Data flowing into:
- `raw_quotes`, `raw_trades` (perp + spot)
- `market_state`, `funding_state` (perp contexts)
- `reference_state` (external reference polling)

**Minimum data needed before hypothesis testing:** 48h of continuous collection on the target pairs.

Why 48h?
- Funding and carry effects are periodic (8h cadence), so 48h captures multiple cycles instead of a single snapshot.
- Intraday behavior shifts by hour/session; 48h gives at least two full day/night rotations.
- Lead/lag and spread signals can look strong in short windows by chance; longer windows reduce false positives.
- Data engineering quality (disconnects, gaps, delayed writes) only shows up under sustained runtime.
- Before Phase 2, we need enough depth to estimate stationarity/half-life and validate robustness checks.

---

## Next steps

### Immediate (Phase 1)
- [x] Extend collector to cover spot deployer tokens: `@264` (TSLA), `@265` (NVDA), `@279` (SPY), `@288` (QQQ), `@182` (XAUT0)
- [x] Add reference price polling path for SPX, gold, TSLA, NVDA (currently Stooq-backed)
- [ ] Run collector continuously for 48h minimum
- [ ] Verify gap rate stays below 1% in `data_gaps` table

### Phase 2 — hypothesis testing (after 48h of data)
- [ ] `notebooks/02_lead_lag.py` — test SPX perp vs SPY/QQQ spot lead-lag
- [ ] `notebooks/03_spread_reversion.py` — test SPX/SPY basis stationarity
- [ ] `notebooks/04_funding_carry.py` — compare funding across native vs deployer markets
- [ ] Extend venue detection for AAPL, GOOGL, SPY, GLD, SLV deployers (currently `hl_native` fallback)

### Phase 3 — strategy candidates (after hypothesis validation)
- [ ] Implement passive lead-lag catcher (Candidate 1) if Hypothesis A holds
- [ ] Implement spread reversion pair (Candidate 2) if Hypothesis B holds
- [ ] Run falsification suite: 2x slippage, 2x fees, latency shock

### Known gaps to address
- Upgrade reference source from lightweight polling to production-grade feed(s) (e.g. Pyth/official vendor)
- Add operational automation around collector restarts and heartbeat alerts
- Keep Felix market discovery in sync with platform docs and live registry snapshots (avoid stale FEUSD-only assumptions)

---

## Phases

```
0  Market registry     ✅ DONE — 393 markets in DB, XYZ/Felix/Unit classified
1  Data engineering    🔄 IN PROGRESS — collector live, 7 markets streaming
2  Hypothesis testing  ⏳ BLOCKED on 48h data minimum
3  Strategy candidates ⏳ BLOCKED on Phase 2
4  Backtesting         ⏳ BLOCKED on Phase 3
5  Paper trading       ⏳ BLOCKED on Phase 4
6  Live small-capital  ⏳ BLOCKED on Phase 5
```

---

## Structure

```
src/arb/
├── market_data/    # collector, client (perp+spot), normalizer, event store
├── signals/        # spread calculator, lead-lag detector, funding analyzer, feature engine
├── execution/      # simulator, paper trader, order models
├── risk/           # controls, circuit breaker
├── portfolio/      # PnL ledger
├── backtest/       # event-driven engine, metrics, falsification suite
├── reporting/      # weekly memo, go/no-go memo
└── scripts/        # collect, build_registry, run_backtest
notebooks/          # one notebook per hypothesis + registry + data quality + weekly memo
sql/schema.sql      # full PostgreSQL schema (10 tables)
```

## Quickstart

```bash
cp .env.example .env
pip install -e ".[dev]"

# Start embedded postgres (no Docker needed)
python3 -c "
import pgserver, pathlib
pg = pgserver.get_server(pgdata=pathlib.Path('.pgdata'), cleanup_mode=None)
pg.ensure_pgdata_inited()
pg.ensure_postgres_running()
pg.psql(open('sql/schema.sql').read().replace('CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;','').replace('SELECT create_hypertable','--'))
print('ready')
"

arb-registry          # Phase 0: fetch and display market registry
arb-registry-audit    # snapshot + drift report against previous registry
arb-phase1-status     # Phase 1 readiness summary from DB
arb-backfill          # REST pull bootstrap (trades/funding/mark snapshot)
arb-collect           # Phase 1: start data collector
arb-collector-daemon  # Phase 1: managed daemon (start/stop/status/progress)
arb-backtest --help   # Phase 4: run a backtest
```

Registry drift workflow:

```bash
# Capture a live snapshot and diff versus previous snapshot
arb-registry-audit

# Outputs:
# - data/registry_snapshots/latest.json
# - reports/registry_audit/latest.md
```

Phase 1 readiness check:

```bash
arb-phase1-status --hours 24
```

Bootstrap + stream workflow (recommended):

```bash
# 1) Bootstrap recent history via REST
arb-backfill --markets "" --funding-days 7 --max-trades 2000

# 2) Continue with long-running websocket collection
arb-collector-daemon start --markets "" --references "SPX,XAU,TSLA,NVDA"
```

Notes on `arb-backfill`:
- Funding and mark-state bootstrap are the primary reliable pulls.
- Trade pull is best-effort and can vary with exchange API behavior.

Collector options:

```bash
arb-collect --help
# --markets: symbols, @N spot IDs, or canonical hl-perp:/hl-spot: IDs
# --references: comma-separated reference symbols
# --reference-poll-s: external reference polling interval
```

Daemon workflow (recommended for long runs):

```bash
# Start background supervisor + collector
arb-collector-daemon start --markets "" --references "SPX,XAU,TSLA,NVDA"

# Check runtime health and logs
arb-collector-daemon status

# Query DB ingestion progress (rows + distinct markets/symbols)
arb-collector-daemon progress --hours 1

# Restart with saved config
arb-collector-daemon restart

# Stop everything
arb-collector-daemon stop
```

Optional boot automation:

```bash
# Print @reboot cron line
arb-collector-daemon install-reboot-cron

# Install to user crontab
arb-collector-daemon install-reboot-cron --apply

# Print hourly ops cron lines (registry audit + phase1 status)
arb-collector-daemon install-ops-cron

# Install hourly ops cron lines
arb-collector-daemon install-ops-cron --apply
```

## Running tests

```bash
pytest tests/ -v
```

## Documentation discipline

- Update `CHANGELOG.md` for every meaningful change in goals, thinking, or implementation.
- Prefer entries that explain *why now* and *what changed in decision-making*, not only file diffs.
- Keep `docs/` updated for onboarding and operational playbooks:
  - `docs/REPO_GUIDE.md`
  - `docs/EXTERNAL_SOURCES.md`

## Key design decisions

- **No candle-only backtests.** Everything replays the raw event stream.
- **Falsification is mandatory.** 2x slippage shock, 2x fee shock, and latency shock are built into the backtest engine.
- **Edge is not assumed.** If the go/no-go criteria aren't met, the correct answer is *do not fund*, not *optimize harder*.

## Go/No-Go criteria

The strategy only gets funded if **all** of the following are true:

- [ ] Edge positive after realistic fees and slippage
- [ ] Results hold across ≥2 materially different regimes
- [ ] Live paper performance resembles backtest
- [ ] Orphan-leg and liquidity risks are tolerable
- [ ] Does not rely on one freak market or one freak week
- [ ] Operational complexity proportional to expected return
