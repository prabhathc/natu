"""
Phase 2: Hypothesis B — Cross-Venue Spread Mean Reversion
==========================================================
Tests whether economically linked markets diverge beyond noise and
mean-revert within a measurable time frame.

Key questions:
- Is the spread stationary (ADF test)?
- What is the half-life?
- Does post-cost edge survive?
- Does the relationship break during news or reference-market reopen?
"""

# %%
import asyncio
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlalchemy as sa
from arb.db import get_engine
from arb.signals.spreads import SpreadCalculator, SpreadBook

engine = get_engine()
spread_book = SpreadBook()

# %%
async def load_aligned_mids(market_a: str, market_b: str, lookback_hours: int = 72, resample: str = "1s"):
    """Load and align mid prices to a common time grid."""
    async with engine.connect() as conn:
        result = await conn.execute(sa.text("""
            SELECT ts, market_id, (bid_px + ask_px) / 2 AS mid
            FROM raw_quotes
            WHERE market_id IN (:a, :b)
              AND ts > NOW() - INTERVAL ':h hours'
            ORDER BY ts
        """), {"a": market_a, "b": market_b, "h": lookback_hours})
        df = pd.DataFrame(result.fetchall(), columns=["ts", "market_id", "mid"])

    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    pivot = df.pivot(index="ts", columns="market_id", values="mid")
    return pivot.resample(resample).last().ffill().dropna()

# %%
def run_spread_analysis(df: pd.DataFrame, market_a: str, market_b: str, fee_bp: float = 3.5) -> None:
    """Full spread analysis on aligned DataFrame."""
    calc = SpreadCalculator(market_a, market_b, window=min(len(df), 600))
    now_base = df.index[0].timestamp()

    for i, (ts, row) in enumerate(df.iterrows()):
        calc.update(ts.timestamp(), row[market_a], row[market_b])

    stats = calc.compute_stats(fee_bp=fee_bp)
    spread_book.update(stats)

    print(f"\n=== Spread Analysis: {market_a} vs {market_b} ===")
    print(f"Hedge ratio:       {stats.hedge_ratio:.4f}")
    print(f"Is stationary:     {stats.is_stationary} (ADF p={stats.adf_pvalue:.4f})")
    print(f"Half-life:         {stats.half_life_s:.1f}s" if stats.half_life_s else "Half-life:         non-mean-reverting")
    print(f"Avg edge:          {stats.avg_edge_bp:.2f} bp")
    print(f"Post-cost edge:    {stats.post_cost_edge_bp:.2f} bp")
    print(f"Current z-score:   {stats.z_score:.2f}")

    # Z-score chart
    zs, mu, sigma = calc.z_score_series()
    fig, axes = plt.subplots(2, 1, figsize=(14, 8))
    axes[0].plot(df.index, df[market_a] / df[market_a].mean(), label=market_a, alpha=0.8)
    axes[0].plot(df.index, df[market_b] / df[market_b].mean(), label=market_b, alpha=0.8)
    axes[0].set_title(f"Normalized prices: {market_a} vs {market_b}")
    axes[0].legend()

    ts_arr = df.index[-len(zs):]
    axes[1].plot(ts_arr, zs, color="purple")
    axes[1].axhline(2.0, color="red", linestyle="--", alpha=0.5, label="+2σ entry")
    axes[1].axhline(-2.0, color="green", linestyle="--", alpha=0.5, label="-2σ entry")
    axes[1].axhline(0, color="black", linestyle="-", alpha=0.3)
    axes[1].set_title("Spread Z-score")
    axes[1].legend()
    plt.tight_layout()
    plt.savefig(f"spread_{market_a.replace(':','_')}_{market_b.replace(':','_')}.png")
    plt.show()

    return stats

# %%
# Breakdown test: does relationship hold during high-volatility events?
def stress_test_spread(df: pd.DataFrame, market_a: str, market_b: str) -> dict:
    """Split data into calm vs volatile periods and compare."""
    mid_a = df[market_a]
    vol = mid_a.pct_change().rolling(60).std()
    vol_threshold = vol.quantile(0.75)

    calm_df = df[vol <= vol_threshold]
    stress_df = df[vol > vol_threshold]

    results = {}
    for label, sub_df in [("calm", calm_df), ("stress", stress_df)]:
        if len(sub_df) < 50:
            results[label] = {"error": "insufficient data"}
            continue
        calc = SpreadCalculator(market_a, market_b, window=min(len(sub_df), 300))
        for ts, row in sub_df.iterrows():
            calc.update(ts.timestamp(), row[market_a], row[market_b])
        stats = calc.compute_stats()
        results[label] = {
            "stationary": stats.is_stationary,
            "half_life_s": stats.half_life_s,
            "post_cost_edge_bp": stats.post_cost_edge_bp,
        }

    return results

print("Spread reversion notebook loaded.")
print("Key check: post_cost_edge_bp > 0 AND is_stationary == True")
print("If both false: spread is noise, not edge.")
