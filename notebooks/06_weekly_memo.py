"""
Weekly Research Memo Generator
================================
Run this notebook each week to produce the findings memo.
Reads from the database and experiment_results table.
"""

# %%
import asyncio
from datetime import date, timedelta
import pandas as pd
import sqlalchemy as sa
from arb.db import get_engine
from arb.reporting.memos import WeeklyMemo

engine = get_engine()
week_ending = date.today()

# %%
async def load_experiment_results(hypothesis: str | None = None):
    async with engine.connect() as conn:
        where = "WHERE run_at > NOW() - INTERVAL '7 days'"
        if hypothesis:
            where += f" AND hypothesis = '{hypothesis}'"
        result = await conn.execute(sa.text(f"""
            SELECT hypothesis, strategy_id, run_at, metrics, verdict, notes
            FROM experiment_results
            {where}
            ORDER BY run_at DESC
        """))
        return pd.DataFrame(result.fetchall(), columns=["hypothesis","strategy_id","run_at","metrics","verdict","notes"])

# %%
async def compute_data_completeness():
    async with engine.connect() as conn:
        # Total possible market-hours vs actual
        r1 = await conn.execute(sa.text("""
            SELECT COUNT(DISTINCT market_id) FROM market_registry WHERE is_active
        """))
        n_markets = r1.scalar()

        r2 = await conn.execute(sa.text("""
            SELECT COUNT(DISTINCT (market_id, date_trunc('hour', ts)))
            FROM raw_quotes
            WHERE ts > NOW() - INTERVAL '7 days'
        """))
        actual_mh = r2.scalar()

        # Expected = n_markets * 24 * 7
        expected_mh = (n_markets or 0) * 24 * 7
        return (
            round(actual_mh / max(expected_mh, 1) * 100, 1),
            n_markets or 0,
        )

# %%
async def build_memo():
    completeness_pct, n_markets = await compute_data_completeness()
    experiments = await load_experiment_results()

    gap_count = 0
    async with engine.connect() as conn:
        r = await conn.execute(sa.text("SELECT COUNT(*) FROM data_gaps WHERE detected_at > NOW() - INTERVAL '7 days'"))
        gap_count = r.scalar() or 0

    memo = WeeklyMemo(
        week_ending=week_ending,
        markets_tracked=n_markets,
        data_completeness_pct=completeness_pct,
        gaps_detected=gap_count,
        observations=f"Auto-generated. {len(experiments)} experiment runs this week.",
    )

    print(memo.render())
    filename = f"weekly_memo_{week_ending}.md"
    with open(filename, "w") as f:
        f.write(memo.render())
    print(f"\nSaved to {filename}")
    return memo

memo = asyncio.run(build_memo())
