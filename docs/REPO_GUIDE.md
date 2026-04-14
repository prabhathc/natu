# Repo Guide

This repository is a falsification-first research and execution prototype for
HIP-3 cross-venue trading ideas.

## How to navigate

- `src/arb/market_data`: API client, symbol/venue normalization, storage.
- `src/arb/signals`: lead-lag, spread, funding feature primitives.
- `src/arb/execution`: simulator and paper-trading components.
- `src/arb/risk`: guardrails, limits, circuit breakers.
- `src/arb/backtest`: event-driven replay and metrics.
- `src/arb/reporting`: weekly/go-no-go memo generation helpers.
- `src/arb/scripts`: operational entrypoints.

## Core CLI commands

- `arb-registry`: build and view current market registry.
- `arb-registry-audit`: snapshot registry and report drift.
- `arb-phase1-status`: print Phase 1 readiness/coverage summary.
- `arb-backfill`: REST bootstrap for recent trades/funding/mark snapshots.
- `arb-collect`: run collector directly in foreground.
- `arb-collector-daemon`: managed long-running collection.
- `arb-backtest`: run event-driven backtest over stored data.

## Contributor workflow

1. Run tests: `pytest tests/ -v`
2. Update docs where behavior/goals changed:
   - `README.md` for user-facing usage
   - `CHANGELOG.md` for rationale and decision drift
   - this `docs/` directory for deeper operational notes
3. If market assumptions changed, run `arb-registry-audit` and include report
   paths in PR notes.
