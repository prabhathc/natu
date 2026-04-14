"""
Phase 2: Hypothesis C — Funding / Basis Carry
==============================================
Tests whether a delta-neutral or near-neutral structure earns positive
carry after fees and expected slippage.

Key questions:
- Is funding persistent enough to trade?
- Is the carry competed away by crowding?
- Does carry survive after slippage?
- What is the liquidation spillover risk?
"""

# %%
import asyncio
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlalchemy as sa
from arb.db import get_engine
from arb.signals.funding import FundingAnalyzer, CrossMarketFundingArb

engine = get_engine()

# %%
async def load_funding_history(market_ids: list[str], lookback_days: int = 30):
    async with engine.connect() as conn:
        result = await conn.execute(sa.text("""
            SELECT ts, market_id, funding_rate, annualized_rate
            FROM funding_state
            WHERE market_id = ANY(:mkts)
              AND ts > NOW() - INTERVAL ':d days'
            ORDER BY ts
        """), {"mkts": market_ids, "d": lookback_days})
        return pd.DataFrame(result.fetchall(), columns=["ts","market_id","funding_rate","annualized_rate"])

# %%
def analyze_funding(df: pd.DataFrame) -> pd.DataFrame:
    """Compute carry stats per market."""
    rows = []
    for mkt, grp in df.groupby("market_id"):
        fa = FundingAnalyzer(mkt)
        for _, row in grp.iterrows():
            fa.update(float(row["funding_rate"]))
        stats = fa.compute_stats(entry_slippage_bp=2.0, exit_slippage_bp=2.0, hold_periods=3)
        rows.append({
            "market": stats.market,
            "avg_funding_8h": stats.avg_funding_8h,
            "annualized_rate_%": stats.annualized_rate * 100,
            "variance": stats.variance,
            "persistence": stats.funding_persistence,
            "net_carry_bp_per_8h": stats.net_carry_bp_per_8h,
            "is_crowded": stats.is_crowded,
        })
    return pd.DataFrame(rows).sort_values("net_carry_bp_per_8h", ascending=False)

# %%
def plot_funding_history(df: pd.DataFrame, market_id: str) -> None:
    mdf = df[df["market_id"] == market_id].copy()
    mdf["ts"] = pd.to_datetime(mdf["ts"], utc=True)
    mdf = mdf.set_index("ts").sort_index()

    fig, axes = plt.subplots(2, 1, figsize=(14, 8))
    axes[0].plot(mdf.index, mdf["funding_rate"] * 10_000, color="blue")
    axes[0].axhline(0, color="black", linestyle="-", alpha=0.3)
    axes[0].set_title(f"Funding Rate (bp/8h): {market_id}")
    axes[0].set_ylabel("Funding Rate (bp)")

    # Rolling 30-period average
    rolling = mdf["funding_rate"].rolling(30).mean()
    axes[1].plot(mdf.index, mdf["funding_rate"] * 10_000, alpha=0.4, label="raw")
    axes[1].plot(mdf.index, rolling * 10_000, color="orange", label="30-period MA")
    axes[1].axhline(0, color="black", linestyle="-", alpha=0.3)
    axes[1].set_title("Funding Rate with Rolling Average")
    axes[1].legend()
    plt.tight_layout()
    plt.savefig(f"funding_{market_id.replace(':','_')}.png")
    plt.show()

# %%
# Cross-market carry arb: find pairs where funding differentials are exploitable
def find_carry_pairs(df: pd.DataFrame, min_diff_bp: float = 2.0) -> pd.DataFrame:
    """Find pairs with exploitable funding differential."""
    market_ids = df["market_id"].unique().tolist()
    pairs = []
    for i, a in enumerate(market_ids):
        for b in market_ids[i+1:]:
            arb = CrossMarketFundingArb(a, b)
            for _, row in df.iterrows():
                arb.update(row["market_id"], float(row["funding_rate"]))
            diff = arb.differential_stats()
            ann_diff = abs(diff["differential_8h"]) * 3 * 365 * 10_000  # bp annualized
            if ann_diff > min_diff_bp * 365 * 3:
                pairs.append({**diff, "ann_diff_bp": ann_diff})
    return pd.DataFrame(pairs).sort_values("ann_diff_bp", ascending=False)

print("Funding carry notebook loaded.")
print("Key check: net_carry_bp_per_8h > 0 AND is_crowded == False")
print("Crowded = funding spike driven by everyone entering the same trade.")
