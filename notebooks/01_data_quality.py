"""
Phase 1: Data Quality Dashboard
================================
Checks completeness, gap frequency, and spread sanity across all collected markets.
Run after at least 24h of data collection.
"""

# %%
import asyncio
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlalchemy as sa
from arb.db import get_engine

engine = get_engine()

# %%
async def load_completeness():
    async with engine.connect() as conn:
        # Events per market per hour
        result = await conn.execute(sa.text("""
            SELECT
                market_id,
                date_trunc('hour', ts) AS hour,
                COUNT(*) AS n_quotes
            FROM raw_quotes
            WHERE ts > NOW() - INTERVAL '7 days'
            GROUP BY 1, 2
            ORDER BY 1, 2
        """))
        return pd.DataFrame(result.fetchall(), columns=["market_id", "hour", "n_quotes"])

completeness = asyncio.run(load_completeness())
print(f"Loaded {len(completeness)} market-hour buckets")

# %%
# Expected quotes: roughly 3600/s max on active markets, 1/s minimum threshold
pivot = completeness.pivot(index="hour", columns="market_id", values="n_quotes").fillna(0)
completeness_pct = (pivot > 0).mean() * 100
print("\nCompleteness % (fraction of hours with any data):")
print(completeness_pct.sort_values())

# %%
# Gap analysis
async def load_gaps():
    async with engine.connect() as conn:
        result = await conn.execute(sa.text("""
            SELECT market_id, data_type, gap_start, gap_end, duration_s
            FROM data_gaps
            WHERE detected_at > NOW() - INTERVAL '7 days'
            ORDER BY duration_s DESC
        """))
        return pd.DataFrame(result.fetchall(), columns=["market_id","data_type","gap_start","gap_end","duration_s"])

gaps = asyncio.run(load_gaps())
print(f"\nGaps detected: {len(gaps)}")
if len(gaps):
    print("\nTop 10 longest gaps:")
    print(gaps.head(10))

# %%
# Spread sanity check
async def load_spreads():
    async with engine.connect() as conn:
        result = await conn.execute(sa.text("""
            SELECT market_id, AVG(spread_bp) AS avg_spread, PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY spread_bp) AS p95
            FROM raw_quotes
            WHERE ts > NOW() - INTERVAL '7 days' AND spread_bp IS NOT NULL
            GROUP BY market_id
            ORDER BY avg_spread
        """))
        return pd.DataFrame(result.fetchall(), columns=["market_id","avg_spread","p95_spread"])

spreads = asyncio.run(load_spreads())
print("\nSpread summary by market:")
print(spreads.to_string())

# %%
# Funding rate coverage
async def load_funding_coverage():
    async with engine.connect() as conn:
        result = await conn.execute(sa.text("""
            SELECT market_id, COUNT(*) AS n_obs, MIN(ts) AS first, MAX(ts) AS last
            FROM funding_state
            WHERE ts > NOW() - INTERVAL '7 days'
            GROUP BY 1
            ORDER BY n_obs DESC
        """))
        return pd.DataFrame(result.fetchall(), columns=["market_id","n_obs","first","last"])

funding_cov = asyncio.run(load_funding_coverage())
print("\nFunding rate coverage:")
print(funding_cov.to_string())
