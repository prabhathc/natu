"""
Research memo generators.

WeeklyMemo: summarizes the past 7 days of data collection and hypothesis testing.
GoNoGoMemo: final funding recommendation with full risk analysis.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from typing import Optional

from arb.backtest.metrics import BacktestMetrics
from arb.signals.lead_lag import LeadLagResult
from arb.signals.spreads import SpreadStats
from arb.signals.funding import FundingCarryStats


@dataclass
class WeeklyMemo:
    week_ending: date
    markets_tracked: int
    data_completeness_pct: float
    gaps_detected: int

    # Hypothesis findings this week
    lead_lag_results: list[LeadLagResult] = field(default_factory=list)
    spread_results: list[SpreadStats] = field(default_factory=list)
    funding_results: list[FundingCarryStats] = field(default_factory=list)
    backtest_metrics: dict[str, BacktestMetrics] = field(default_factory=dict)

    observations: str = ""

    def render(self) -> str:
        lines = [
            f"# Weekly Research Memo — {self.week_ending}",
            "",
            "## Data Quality",
            f"- Markets tracked: {self.markets_tracked}",
            f"- Data completeness: {self.data_completeness_pct:.1f}%",
            f"- Gaps detected: {self.gaps_detected}",
            "",
            "## Hypothesis A: Lead-Lag",
        ]
        if not self.lead_lag_results:
            lines.append("- No significant lead-lag detected this week.")
        for r in self.lead_lag_results:
            lines.append(
                f"- {r.leader} → {r.follower} | lag={r.horizon_ms}ms "
                f"hit={r.hit_rate:.1%} granger_p={r.granger_pvalue:.3f}"
            )

        lines += ["", "## Hypothesis B: Spread Reversion"]
        if not self.spread_results:
            lines.append("- No significant spread edges this week.")
        for s in self.spread_results:
            lines.append(
                f"- {s.pair} | hl={s.half_life_s:.0f}s z={s.z_score:.2f} "
                f"post_cost={s.post_cost_edge_bp:.1f}bp stationary={s.is_stationary}"
            )

        lines += ["", "## Hypothesis C: Funding Carry"]
        if not self.funding_results:
            lines.append("- No significant carry opportunities this week.")
        for f in self.funding_results:
            lines.append(
                f"- {f.market} | avg_8h={f.avg_funding_8h:.4%} "
                f"ann={f.annualized_rate:.1%} net={f.net_carry_bp_per_8h:.2f}bp/8h "
                f"crowded={f.is_crowded}"
            )

        lines += ["", "## Backtest Results"]
        if not self.backtest_metrics:
            lines.append("- No backtest runs this week.")
        for name, m in self.backtest_metrics.items():
            sharpe_str = f"{m.sharpe:.2f}" if m.sharpe is not None else "n/a"
            lines.append(
                f"- {name}: net_pnl={m.net_pnl:.2f} sharpe={sharpe_str} "
                f"dd={m.max_drawdown:.2f} hit={m.hit_rate:.1%}"
            )

        if self.observations:
            lines += ["", "## Observations", self.observations]

        return "\n".join(lines)


@dataclass
class GoNoGoMemo:
    """Final seed-funding recommendation."""

    strategy_name: str
    evaluation_date: date

    # Required acceptance criteria
    edge_positive_post_cost: bool
    holds_across_two_regimes: bool
    paper_matches_backtest: bool
    orphan_risk_tolerable: bool
    not_dependent_on_single_period: bool
    complexity_proportional: bool

    # Supporting data
    backtest_metrics: Optional[BacktestMetrics] = None
    paper_metrics: Optional[BacktestMetrics] = None
    capital_envelope_usd: float = 0.0
    realistic_bottleneck: str = ""
    what_breaks_at_scale: str = ""
    risk_summary: str = ""

    # Research question answers
    answers: dict[str, str] = field(default_factory=dict)

    @property
    def verdict(self) -> str:
        required = [
            self.edge_positive_post_cost,
            self.holds_across_two_regimes,
            self.paper_matches_backtest,
            self.orphan_risk_tolerable,
            self.not_dependent_on_single_period,
            self.complexity_proportional,
        ]
        return "GO" if all(required) else "NO-GO"

    def render(self) -> str:
        lines = [
            f"# Go/No-Go Memo: {self.strategy_name}",
            f"Date: {self.evaluation_date}",
            f"## VERDICT: {self.verdict}",
            "",
            "## Acceptance Criteria",
            f"- [ {'x' if self.edge_positive_post_cost else ' '}] Edge positive after realistic fees and slippage",
            f"- [ {'x' if self.holds_across_two_regimes else ' '}] Results hold across ≥2 materially different regimes",
            f"- [ {'x' if self.paper_matches_backtest else ' '}] Live paper performance resembles backtest",
            f"- [ {'x' if self.orphan_risk_tolerable else ' '}] Orphan-leg and liquidity risks are tolerable",
            f"- [ {'x' if self.not_dependent_on_single_period else ' '}] Does not rely on one freak market or week",
            f"- [ {'x' if self.complexity_proportional else ' '}] Operational complexity proportional to expected return",
        ]

        if self.backtest_metrics:
            m = self.backtest_metrics
            sharpe_str = f"{m.sharpe:.2f}" if m.sharpe is not None else "n/a"
            lines += [
                "",
                "## Backtest Summary",
                f"- Net PnL: {m.net_pnl:.2f}",
                f"- Sharpe: {sharpe_str}",
                f"- Max Drawdown: {m.max_drawdown:.2f}",
                f"- Hit Rate: {m.hit_rate:.1%}",
                f"- Fee/Gross: {m.fee_to_gross:.1%}",
                f"- Slippage/Gross: {m.slippage_to_gross:.1%}",
            ]

        lines += [
            "",
            "## Capital Envelope",
            f"- Recommended starting capital: ${self.capital_envelope_usd:,.0f}",
            "",
            "## Bottleneck Analysis",
            f"- Realistic bottleneck: {self.realistic_bottleneck or 'not assessed'}",
            f"- What breaks at scale: {self.what_breaks_at_scale or 'not assessed'}",
        ]

        if self.answers:
            lines += ["", "## Research Questions"]
            for q, a in self.answers.items():
                lines.append(f"**{q}**\n{a}\n")

        if self.risk_summary:
            lines += ["", "## Risk Summary", self.risk_summary]

        if self.verdict == "NO-GO":
            lines += [
                "",
                "## Decision",
                "The correct output is not 'optimize harder.' Do not fund this strategy.",
                "Address the failing criteria above before re-evaluation.",
            ]

        return "\n".join(lines)
