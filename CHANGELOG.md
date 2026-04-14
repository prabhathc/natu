# Changelog

This changelog tracks not only what changed in code, but also *why* it changed:
- goal state for each phase,
- hypothesis or framing updates,
- decision rationale and expected impact.

Format intent:
- `Goal`: what we are trying to achieve at this point in time.
- `Thinking Shift`: how our understanding changed.
- `Implementation`: concrete code/data changes.
- `Validation`: evidence that the change works.
- `Next`: what still blocks progress.

## [Unreleased]

### Added
- Changelog process and structure to preserve evolving project intent.

### Documentation
- Updated `README.md` to reflect current Phase 1 capabilities:
  - mixed perp + spot collection in `arb-collect`,
  - reference polling support and CLI options,
  - revised immediate checklist and known gaps,
  - changelog maintenance expectations,
  - rationale for the 48h continuous collection gate before Phase 2.
- Added daemon operations documentation (`arb-collector-daemon`) for automation and status queries.
- Added Felix docs alignment notes so project framing reflects both:
  - live registry-discovered Felix symbols, and
  - documented Felix platform product scope (spot equities, perps, lending).

### Added
- New `arb-collector-daemon` manager:
  - `start`, `stop`, `restart`, `status`, `progress`,
  - auto-restart supervisor for collector child process,
  - persisted config and log files under `~/.arb/collector-daemon`,
  - optional `install-reboot-cron` helper for startup automation.
- New `arb-registry-audit` workflow:
  - snapshots live registry to `data/registry_snapshots/`,
  - writes drift report to `reports/registry_audit/`,
  - highlights added/removed market IDs and venue/symbol drift.
- New `arb-backfill` command for pull-mode bootstrap:
  - pulls recent trades by market,
  - pulls funding history for perp markets,
  - snapshots current mark state from allMids,
  - complements (not replaces) websocket collection.
- New `arb-phase1-status` command:
  - summarizes ingestion counts/coverage/gaps over a lookback window,
  - emits PASS/WARN readiness signal for Phase 2 handoff.
- Daemon ops automation additions:
  - `install-ops-cron` prints/installs hourly registry-audit + phase1-status jobs.
- Added local docs set for onboarding and source traceability:
  - `docs/REPO_GUIDE.md`,
  - `docs/EXTERNAL_SOURCES.md`.

### Validation
- Full test suite run: `92 passed`.
- Live registry smoke test (`arb.scripts.build_registry --no-save`) succeeded with current exchange data.
- Live collector smoke test (`arb.scripts.collect --markets "SPX,@279"`) started successfully and ran for 20s (terminated intentionally by timeout).

### Journal (2026-04-14)
- Kicked off operations stack:
  - full-universe backfill bootstrap,
  - live registry drift audit snapshot/report,
  - collector daemon running with broad reference symbols.
- Added operator tooling to reduce manual overhead:
  - `arb-backfill` (pull bootstrap),
  - `arb-registry-audit` (drift tracking),
  - `arb-phase1-status` (coverage/gap readiness signal),
  - daemon cron helpers for reboot + hourly ops checks.
- Clarified data strategy:
  - pull mode accelerates startup and reconciliation,
  - stream mode remains required for microstructure research and reproducible replay.

### Next Steps (while collection runs)
- Build Phase 2 batch jobs:
  - lead-lag report job,
  - spread stationarity/half-life report job,
  - funding carry persistence report job.
- Add paper-trade reconciliation dashboards:
  - signal → order → fill chain completeness,
  - orphan-leg and hedge completion monitoring.
- Generate first weekly memo directly from DB snapshots + audit outputs.

## [2026-04-14] - Phase 1 ingestion + signal stability pass

### Goal
- Keep Phase 1 moving toward reliable data collection for Hypotheses A/B/C.
- Remove blockers that made test results inconsistent with intended analytics behavior.

### Thinking Shift
- Spot deployer markets (`hl-spot:@N`) need to be first-class in ingestion, not treated as perp-only symbols.
- For hedge-ratio estimation on integrated price series, intercept-heavy regression can bias beta materially in this setup.
- Lead/lag sign convention must be explicit and consistent: positive lag means A leads B.

### Implementation
- `src/arb/signals/spreads.py`
  - Switched hedge ratio estimation to no-intercept OLS for trading beta.
- `src/arb/signals/lead_lag.py`
  - Fixed cross-correlation lag alignment to match documented sign convention.
- `src/arb/market_data/normalizer.py`
  - Fixed FX classification for raw symbols like `EURUSD`.
  - Expanded venue classification heuristics and token sets.
- `src/arb/market_data/client.py`
  - Added market-id mapping helper to route `@N` to `hl-spot:@N`.
  - Applied mapping in websocket quote/trade/state/funding handlers.
- `src/arb/scripts/collect.py`
  - Added mixed market resolution (symbol, `@N`, or canonical `hl-*` market IDs).
  - Split perp-only context subscriptions from shared quote/trade subscriptions.
  - Added reference price polling path and CLI options (`--references`, `--reference-poll-s`).
- Tests
  - Added/updated `tests/test_collect.py`, `tests/test_normalizer.py`, `tests/test_registry.py`, and shared fixtures in `tests/conftest.py`.

### Validation
- Full test suite passed after fixes: `92 passed`.
- Signals tests passed after estimator/sign-convention fixes.

### Next
- Run collector continuously for at least 48h on target markets.
- Verify `data_gaps` rate remains within Phase 1 threshold.

## [2026-04-13] - Phase 0 findings formalized

### Goal
- Discover the actual tradable universe and classify venues correctly before strategy work.

### Thinking Shift
- HIP-3 markets are surfaced in spot metadata and not only in perp metadata.
- Original XYZ-vs-Felix framing is incomplete; cross-venue opportunities are broader and include native perps vs spot deployer tokens.

### Implementation
- Built and persisted registry from Hyperliquid metadata sources.
- Updated README with market counts, discovered architecture, and revised hypothesis framing.

### Validation
- Registry construction and classification tests added and passing.

### Next
- Move into sustained Phase 1 collection with gap monitoring.

## [2026-04-12] - Project bootstrap

### Goal
- Stand up a falsification-first research framework for HIP-3 arbitrage hypotheses.

### Implementation
- Initial repository scaffolding across market data, signals, risk, execution, portfolio, backtest, reporting, and scripts.

### Next
- Populate registry and validate market-universe assumptions.

