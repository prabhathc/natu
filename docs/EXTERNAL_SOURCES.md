# External Sources (Local Reference)

This file stores the key external documentation used to shape registry and
hypothesis assumptions. Keep this updated when assumptions change.

## Hyperliquid

- HIP-3 builder-deployed perpetuals:
  - https://hyperliquid.gitbook.io/hyperliquid-docs/hyperliquid-improvement-proposals-hips/hip-3-builder-deployed-perpetuals
- Info endpoint:
  - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- Fees:
  - https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees

## Trade[XYZ]

- Architecture/docs:
  - https://docs.trade.xyz/

## Felix

- Docs home:
  - https://usefelix.gitbook.io/docs
- Spot equities product:
  - https://usefelix.gitbook.io/docs/trading-products/spot-equities
- CDP market (feUSD):
  - https://usefelix.gitbook.io/docs/lending-products/quickstart

## How sources are used

- Live API metadata remains source-of-truth for *current* registry entries.
- External docs provide *capability context* and guide what should be watched
  for discovery drift.
- Any mismatch between docs and live API should be captured via
  `arb-registry-audit` and noted in `CHANGELOG.md`.
