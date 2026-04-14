"""
Phase 2: Hypothesis A — Lead-Lag Analysis
==========================================
Tests whether any market family consistently leads another.

Steps:
1. Load aligned quote data for candidate pairs
2. Compute cross-correlations at ms/s horizons
3. Run Granger causality tests
4. Compute conditional tests by volatility regime and time of day
5. Output ranked lead-lag table

DO NOT assume XYZ leads or Felix lags. Prove or reject empirically.
"""

# %%
import asyncio
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlalchemy as sa
from arb.db import get_engine
from arb.signals.lead_lag import LeadLagDetector

engine = get_engine()

# %%
# Load mid-price series for candidate pairs
# First, identify markets with the same underlying across venues
async def load_mid_prices(market_a: str, market_b: str, lookback_hours: int = 48):
    async with engine.connect() as conn:
        query = sa.text("""
            SELECT ts, market_id, (bid_px + ask_px) / 2 AS mid_px
            FROM raw_quotes
            WHERE market_id IN (:a, :b)
              AND ts > NOW() - INTERVAL ':h hours'
            ORDER BY ts
        """)
        result = await conn.execute(query, {"a": market_a, "b": market_b, "h": lookback_hours})
        return pd.DataFrame(result.fetchall(), columns=["ts", "market_id", "mid_px"])

# Example: run with actual market IDs from registry
# df = asyncio.run(load_mid_prices("hl:XAU-xyz", "hl:XAU-felix"))

# %%
def analyze_pair(df: pd.DataFrame, market_a: str, market_b: str) -> dict:
    """Full lead-lag analysis for a price pair DataFrame."""
    det = LeadLagDetector(market_a, market_b, resample_ms=200, window_s=1800)

    for _, row in df.iterrows():
        ts_ms = int(row["ts"].timestamp() * 1000) if hasattr(row["ts"], "timestamp") else int(row["ts"]) * 1000
        px = float(row["mid_px"])
        if row["market_id"] == market_a:
            det.update_a(ts_ms, px)
        else:
            det.update_b(ts_ms, px)

    result = det.analyze()
    xcorr = det.cross_correlation()
    granger = det.granger_test()

    return {
        "pair": f"{market_a}::{market_b}",
        "result": result,
        "xcorr": xcorr,
        "granger": granger,
    }

# %%
# When data is loaded, run:
# analysis = analyze_pair(df, "hl:XAU-xyz", "hl:XAU-felix")
# if analysis["result"]:
#     r = analysis["result"]
#     print(f"Leader: {r.leader}")
#     print(f"Follower: {r.follower}")
#     print(f"Lag: {r.horizon_ms}ms")
#     print(f"Hit rate: {r.hit_rate:.1%}")
#     print(f"Granger p-value: {r.granger_pvalue:.4f}")

# %%
# Conditional analysis: by volatility regime
def split_by_vol_regime(df: pd.DataFrame, market_id: str, n_quantiles: int = 3) -> dict[str, pd.DataFrame]:
    """Split data into low/medium/high volatility regimes."""
    mdf = df[df["market_id"] == market_id].copy()
    mdf = mdf.set_index("ts").sort_index()
    mdf["returns"] = mdf["mid_px"].pct_change()
    mdf["vol_1h"] = mdf["returns"].rolling("1h").std()
    mdf["vol_regime"] = pd.qcut(mdf["vol_1h"].dropna(), q=n_quantiles, labels=["low","medium","high"])
    regimes = {}
    for regime in ["low", "medium", "high"]:
        regime_idx = mdf[mdf["vol_regime"] == regime].index
        regimes[regime] = df[df["ts"].isin(regime_idx)]
    return regimes

# %%
# Conditional analysis: by time of day (US session vs off-hours)
def split_by_session(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Split into US market hours vs off-hours."""
    df = df.copy()
    if hasattr(df["ts"].iloc[0], "hour"):
        hour = df["ts"].dt.hour
    else:
        hour = pd.to_datetime(df["ts"]).dt.hour
    us_hours = hour.between(13, 20)   # 9am-4pm ET = 13-20 UTC
    return {
        "us_session": df[us_hours],
        "off_hours": df[~us_hours],
    }

print("Lead-lag notebook loaded. Load data and run analyze_pair() to proceed.")
print("\nKey question: does any market family consistently lead another?")
print("Null hypothesis: no consistent lead-lag exists.")
print("Rejection threshold: Granger p < 0.05, hit rate > 55%, stable across regimes.")
