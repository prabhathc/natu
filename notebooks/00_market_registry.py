"""
Phase 0: Market Registry Explorer
==================================
Run as a Jupyter notebook (convert with: jupytext --to ipynb this_file.py)
or execute directly as a script.

Output: master registry table with all HIP-3 markets classified by venue,
        asset class, fee mode, and oracle type.
"""

# %%
import asyncio
import pandas as pd
from IPython.display import display

from arb.logging_config import configure_logging
from arb.market_data.client import HyperliquidClient

configure_logging()

# %%
async def fetch_registry():
    client = HyperliquidClient()
    try:
        markets = await client.build_registry()
    finally:
        await client.close()
    return markets

markets = asyncio.run(fetch_registry())
df = pd.DataFrame([m.model_dump() for m in markets])
print(f"Total markets: {len(df)}")

# %%
# Breakdown by venue
print("\n=== By Venue ===")
print(df.groupby("venue_label").size().sort_values(ascending=False))

# %%
# Breakdown by asset class
print("\n=== By Asset Class ===")
print(df.groupby("asset_class").size().sort_values(ascending=False))

# %%
# Breakdown by fee mode
print("\n=== By Fee Mode ===")
print(df.groupby("fee_mode").size().sort_values(ascending=False))

# %%
# HIP-3 focus: non-native venues
hip3 = df[df["venue_label"].isin(["xyz", "felix"])]
print(f"\nHIP-3 (XYZ + Felix) markets: {len(hip3)}")
display(hip3[["market_id", "venue_label", "symbol", "asset_class", "fee_mode", "max_leverage"]].to_string())

# %%
# Commodity + index markets across all venues
tradfi = df[df["asset_class"].isin(["commodity", "index", "equity", "fx"])]
print(f"\nTradFi-class markets: {len(tradfi)}")
display(tradfi[["market_id", "venue_label", "symbol", "asset_class", "oracle_type", "fee_mode"]].to_string())

# %%
# Save registry to CSV for offline analysis
df.to_csv("registry_snapshot.csv", index=False)
print("\nSaved to registry_snapshot.csv")
