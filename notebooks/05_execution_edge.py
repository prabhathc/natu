"""
Phase 2: Hypothesis D — Execution Edge
=======================================
Tests whether better execution alone (maker vs taker, fill quality,
inventory skew) can generate positive expectancy.

Key questions:
- What is the realized adverse selection after a fill?
- Is maker quoting profitable given fill rate?
- Does fill rate vary by time of day / volatility?
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
async def load_sim_fills(strategy_id: str | None = None):
    async with engine.connect() as conn:
        where = "WHERE strategy_id = :s" if strategy_id else ""
        result = await conn.execute(sa.text(f"""
            SELECT fill_id, order_id, market_id, strategy_id, ts,
                   side, price, size, fee_bp, fee_usd, is_maker,
                   slippage_bp, adverse_sel_bp
            FROM sim_fills
            {where}
            ORDER BY ts
        """), {"s": strategy_id} if strategy_id else {})
        return pd.DataFrame(result.fetchall(), columns=[
            "fill_id","order_id","market_id","strategy_id","ts",
            "side","price","size","fee_bp","fee_usd","is_maker",
            "slippage_bp","adverse_sel_bp",
        ])

# %%
def analyze_execution(fills: pd.DataFrame) -> dict:
    """Execution quality summary."""
    if fills.empty:
        return {"error": "no fills"}

    n = len(fills)
    maker_fills = fills[fills["is_maker"] == True]
    taker_fills = fills[fills["is_maker"] == False]

    return {
        "total_fills": n,
        "maker_pct": len(maker_fills) / n,
        "avg_fee_bp_maker": maker_fills["fee_bp"].mean() if len(maker_fills) else None,
        "avg_fee_bp_taker": taker_fills["fee_bp"].mean() if len(taker_fills) else None,
        "avg_slippage_bp": fills["slippage_bp"].mean(),
        "p95_slippage_bp": fills["slippage_bp"].quantile(0.95),
        "avg_adverse_sel_bp": fills["adverse_sel_bp"].mean() if "adverse_sel_bp" in fills else None,
        "net_edge_bp": (fills["fee_bp"].mean() - fills["slippage_bp"].mean()) * (-1),
    }

# %%
def adverse_selection_analysis(fills: pd.DataFrame, quotes_df: pd.DataFrame, horizon_s: int = 30) -> pd.DataFrame:
    """
    Measure mid-price drift after each fill as proxy for adverse selection.

    For each fill, measure mid_px change from fill time to fill_time + horizon_s.
    Positive adverse selection = filled at bad price relative to future mid.
    """
    results = []
    quotes_df = quotes_df.set_index(["market_id", "ts"]).sort_index()

    for _, fill in fills.iterrows():
        mkt = fill["market_id"]
        ts = pd.to_datetime(fill["ts"], utc=True)
        target_ts = ts + pd.Timedelta(seconds=horizon_s)

        try:
            mid_at_fill = (
                quotes_df.loc[(mkt,), :]
                .loc[ts:ts + pd.Timedelta(seconds=1), ["bid_px", "ask_px"]]
                .mean()
                .sum() / 2
            )
            mid_after = (
                quotes_df.loc[(mkt,), :]
                .loc[target_ts:target_ts + pd.Timedelta(seconds=5), ["bid_px", "ask_px"]]
                .mean()
                .sum() / 2
            )
            if mid_at_fill > 0:
                drift_bp = (mid_after - mid_at_fill) / mid_at_fill * 10_000
                sign = 1 if fill["side"] == "buy" else -1
                adverse_sel = sign * drift_bp   # positive = moved against fill
                results.append({"fill_id": fill["fill_id"], "adverse_sel_bp": adverse_sel})
        except Exception:
            pass

    return pd.DataFrame(results)

# %%
# Cancel/replace decay analysis
def cancel_replace_analysis(fills: pd.DataFrame, orders_df: pd.DataFrame) -> dict:
    """How often do resting orders get stale before fill?"""
    if orders_df.empty:
        return {}
    cancelled = orders_df[orders_df["status"] == "cancelled"]
    filled = orders_df[orders_df["status"] == "filled"]
    return {
        "total_orders": len(orders_df),
        "fill_rate": len(filled) / len(orders_df),
        "cancel_rate": len(cancelled) / len(orders_df),
        "avg_time_to_fill_s": (filled.get("filled_at", pd.Series()) - filled.get("created_at", pd.Series())).dt.total_seconds().mean(),
    }

print("Execution edge notebook loaded.")
print("Key check: net_edge_bp > 0 after adverse selection and fees.")
print("Maker-heavy strategies only work with reasonable fill rates (>30%).")
