"""
QUERY INTELLIGENCE SERVICE
==========================
Adaptive learning engine for the web discovery search queries.

Mirrors the architecture of SourceIntelligence (source_intelligence.py) but
applied at the SEARCH QUERY level instead of the URL pattern level. Every
discovery run teaches the system which queries produce new sources and
new leads, and which just burn Serper credits for nothing.

Classification ladder (same vocabulary as URL patterns):
    maybe  — default. Query is in the learning phase (< 3 runs).
    gold   — proven producer: ≥3 runs with ≥20% new-source yield OR
             ≥3 lifetime new leads.
    junk   — proven dud: 5+ runs with 0 lifetime new sources AND 0
             lifetime new leads.
    paused — a junk query re-armed for a single retry run after a 4-week
             cooldown. If the retry produces nothing, it goes back to junk.

Thresholds are imported from intelligence_config.py so they match the URL
pattern thresholds. Consistency keeps operator mental models simple —
"same rules apply at both levels."

Usage in discover_sources.py:

    from app.services.query_intelligence import QueryIntelligence, filter_active_queries

    # Before the run: load state for all queries, filter out the junk
    async with async_session() as s:
        active_queries, skipped = await filter_active_queries(s, ALL_QUERIES)

    # Run discovery with only active_queries
    ...

    # After the run: record per-query results
    async with async_session() as s:
        for query, result in per_query_results.items():
            qi = await QueryIntelligence.load_or_create(s, query)
            qi.record_run(
                new_sources=result["new_sources"],
                new_leads=result["new_leads"],
                duplicates=result["duplicates"],
                sample_domains=result["sample_domains"],
            )
            await qi.save(s)
        await s.commit()

For the weekly CLI report:

    from app.services.query_intelligence import build_report
    report = await build_report()
    print(report)
"""

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from sqlalchemy import select, func, desc

from app.config.intelligence_config import (
    GOLD_MIN_TESTED,
    GOLD_MIN_HIT_RATE,
    JUNK_MIN_TESTED,
)
from app.models.discovery_query_stat import DiscoveryQueryStat
from app.services.utils import local_now

logger = logging.getLogger(__name__)


# How long a junk query waits before getting one retry shot. Signals can
# drift — a brand dormant for months may start announcing openings again.
# 4 weeks means junk queries retry roughly once per discovery cycle month.
JUNK_RETRY_COOLDOWN = timedelta(weeks=4)

# Minimum lifetime new leads to auto-qualify a query for GOLD even if
# source yield is modest. A query that finds one real lead is valuable even
# if its source yield is noisy.
GOLD_MIN_LIFETIME_LEADS = 3


@dataclass
class QueryRunResult:
    """Shape of one discovery run's result for a single query."""

    new_sources: int = 0  # Unique domains this query surfaced that are
    # new to our database
    new_leads: int = 0  # Leads extracted from sources this query
    # surfaced, deduped against existing leads
    duplicates: int = 0  # Leads this run that matched existing leads
    # (still tracked — duplicate detection is value)
    sample_domains: Optional[list] = None  # First ~5 domains surfaced,
    # for debugging the CLI report


class QueryIntelligence:
    """
    Learning wrapper around a DiscoveryQueryStat row.

    Parallel to SourceIntelligence but operates per-query instead of
    per-source. Every method that mutates state requires a subsequent save().
    """

    def __init__(self, stat: DiscoveryQueryStat):
        self.stat = stat

    # ── Classification ────────────────────────────────────────────

    def _reclassify(self) -> None:
        """
        Promote or demote this query based on accumulated evidence.

        Flow: maybe → gold / junk (absorbing states until retry window)
        """
        s = self.stat
        # Need a minimum number of runs before any classification change
        if s.total_runs < GOLD_MIN_TESTED:
            # Too early — keep it in whatever status it's in (usually maybe)
            return

        # Compute yield rates
        source_yield = s.source_yield_rate  # Property on the model

        # GOLD criteria: high source yield OR ≥3 lifetime leads
        if (
            source_yield >= GOLD_MIN_HIT_RATE
            or s.total_new_leads >= GOLD_MIN_LIFETIME_LEADS
        ):
            if s.status != "gold":
                logger.info(
                    f"🥇 Query promoted to GOLD: {s.query_text!r:.60} "
                    f"(runs={s.total_runs}, sources={s.total_new_sources}, "
                    f"leads={s.total_new_leads})"
                )
                s.status = "gold"
                s.paused_until = None
            return

        # JUNK criteria: enough runs, zero total new sources AND zero leads
        if (
            s.total_runs >= JUNK_MIN_TESTED
            and s.total_new_sources == 0
            and s.total_new_leads == 0
        ):
            if s.status != "junk":
                logger.info(
                    f"🗑️  Query demoted to JUNK: {s.query_text!r:.60} "
                    f"(runs={s.total_runs}, 0 sources, 0 leads)"
                )
                s.status = "junk"
                # Next retry eligible in 4 weeks
                s.paused_until = local_now() + JUNK_RETRY_COOLDOWN
            return

        # Otherwise — either still maybe, or was paused and this run produced
        # nothing useful. If it was paused and still zero, send it back to junk.
        if s.status == "paused" and s.total_new_sources == 0 and s.total_new_leads == 0:
            logger.info(f"🗑️  Paused query re-demoted to JUNK: {s.query_text!r:.60}")
            s.status = "junk"
            s.paused_until = local_now() + JUNK_RETRY_COOLDOWN

    # ── Recording ────────────────────────────────────────────────

    def record_run(self, result: QueryRunResult) -> None:
        """
        Record the outcome of a single discovery run for this query.

        Updates counters, streak, timestamps, last_run_detail, and re-classifies.
        Caller must commit after save().
        """
        s = self.stat
        now = local_now()

        # Bump lifetime counters
        s.total_runs = (s.total_runs or 0) + 1
        s.total_new_sources = (s.total_new_sources or 0) + result.new_sources
        s.total_new_leads = (s.total_new_leads or 0) + result.new_leads
        s.total_duplicates = (s.total_duplicates or 0) + result.duplicates

        # Timestamps
        if s.first_run_at is None:
            s.first_run_at = now
        s.last_run_at = now
        if result.new_sources > 0 or result.new_leads > 0:
            s.last_success_at = now
            s.consecutive_zero_runs = 0
        else:
            s.consecutive_zero_runs = (s.consecutive_zero_runs or 0) + 1

        # If we were paused, this is the retry attempt — unpause
        # regardless of outcome. Classification below will re-junk if needed.
        if s.status == "paused":
            s.paused_until = None

        # Snapshot of this specific run (for the CLI report)
        s.last_run_detail = {
            "at": now.isoformat(),
            "new_sources": result.new_sources,
            "new_leads": result.new_leads,
            "duplicates": result.duplicates,
            "sample_domains": (result.sample_domains or [])[:5],
        }

        # Re-classify based on updated numbers
        self._reclassify()

    # ── Persistence ──────────────────────────────────────────────

    @classmethod
    async def load_or_create(cls, session, query_text: str) -> "QueryIntelligence":
        """
        Load the DiscoveryQueryStat for this query, or create a fresh row.

        The caller is responsible for committing after save().
        """
        result = await session.execute(
            select(DiscoveryQueryStat).where(
                DiscoveryQueryStat.query_text == query_text
            )
        )
        stat = result.scalar_one_or_none()
        if stat is None:
            stat = DiscoveryQueryStat(query_text=query_text, status="maybe")
            session.add(stat)
        return cls(stat)

    async def save(self, session) -> None:
        """
        Persist changes. The row is already in the session; this triggers
        the onupdate=func.now() on updated_at.
        """
        # SQLAlchemy's auto-update triggers on column change. Nudge it.
        self.stat.updated_at = local_now()
        await session.flush()


# ═══════════════════════════════════════════════════════════════════════
# PUBLIC FUNCTIONS — called by discover_sources.py
# ═══════════════════════════════════════════════════════════════════════


async def filter_active_queries(
    session, all_queries: list[str]
) -> tuple[list[str], dict]:
    """
    Given the full query list for this run, return only the queries that
    should actually execute. Skipped queries (junk + paused-in-cooldown)
    are reported in the stats dict so the CLI can show them.

    Returns:
      (active_queries, stats)
      stats = {
        "total": N,
        "skipped_junk": N,        # filtered out because junk + cooldown active
        "paused_retries": N,      # junk queries re-armed for retry this run
        "active": N,
      }

    The caller should:
      1. Run discovery on `active_queries` only
      2. For each active query, call QueryIntelligence.load_or_create().record_run()
    """
    now = local_now()

    result = await session.execute(select(DiscoveryQueryStat))
    rows_by_query = {s.query_text: s for s in result.scalars().all()}

    active: list[str] = []
    stats = {
        "total": len(all_queries),
        "skipped_junk": 0,
        "paused_retries": 0,
        "active": 0,
    }

    for q in all_queries:
        stat = rows_by_query.get(q)

        # Query we've never seen before → run it (becomes maybe after first run)
        if stat is None:
            active.append(q)
            continue

        # Junk query + cooldown passed → re-arm as a retry
        if stat.status == "junk" and stat.paused_until is not None:
            if now >= stat.paused_until:
                stat.status = "paused"
                stat.paused_until = None  # No longer paused, this run is the retry
                active.append(q)
                stats["paused_retries"] += 1
                continue

        if stat.should_skip(now):
            stats["skipped_junk"] += 1
            continue

        active.append(q)

    stats["active"] = len(active)

    # Commit the re-arming changes before discovery starts
    if stats["paused_retries"] > 0:
        await session.commit()

    return active, stats


async def build_report(session, top_n: int = 15) -> str:
    """
    Build a human-readable summary of query performance.
    Used by scripts/discovery_report.py and end-of-run discovery output.
    """
    lines = []
    lines.append("═" * 70)
    lines.append("  QUERY INTELLIGENCE REPORT")
    lines.append("═" * 70)

    # Counts by status
    counts_result = await session.execute(
        select(DiscoveryQueryStat.status, func.count()).group_by(
            DiscoveryQueryStat.status
        )
    )
    counts = dict(counts_result.all())
    total = sum(counts.values())

    lines.append(f"\nTotal queries tracked: {total}")
    for status in ("gold", "maybe", "paused", "junk"):
        n = counts.get(status, 0)
        pct = (n / total * 100) if total else 0
        lines.append(f"  {status:8s}  {n:>4}  ({pct:.0f}%)")

    # Top N by lifetime leads — the winners
    top_result = await session.execute(
        select(DiscoveryQueryStat)
        .order_by(
            desc(DiscoveryQueryStat.total_new_leads),
            desc(DiscoveryQueryStat.total_new_sources),
        )
        .limit(top_n)
    )
    top = top_result.scalars().all()
    lines.append(f"\n── TOP {top_n} QUERIES (by lifetime leads) ──")
    if not top or top[0].total_new_leads == 0:
        lines.append("  (no queries have produced leads yet — still learning)")
    else:
        for s in top:
            if s.total_new_leads == 0:
                break
            lines.append(
                f"  {s.total_new_leads:>3}L {s.total_new_sources:>3}S "
                f"[{s.status:6}] ({s.total_runs}r)  {s.query_text[:55]}"
            )

    # Junk queries — candidates for retirement
    junk_result = await session.execute(
        select(DiscoveryQueryStat)
        .where(DiscoveryQueryStat.status == "junk")
        .order_by(desc(DiscoveryQueryStat.total_runs))
    )
    junk = junk_result.scalars().all()
    if junk:
        lines.append(f"\n── JUNK QUERIES ({len(junk)}) ──")
        lines.append("  Skipped on normal runs; retried once every 4 weeks.")
        for s in junk[:15]:
            next_retry = (
                s.paused_until.strftime("%Y-%m-%d") if s.paused_until else "soon"
            )
            lines.append(
                f"  [{s.total_runs}r, retry {next_retry}]  {s.query_text[:60]}"
            )
        if len(junk) > 15:
            lines.append(f"  ... and {len(junk) - 15} more")

    # Paused retries this run
    paused_result = await session.execute(
        select(DiscoveryQueryStat).where(DiscoveryQueryStat.status == "paused")
    )
    paused = paused_result.scalars().all()
    if paused:
        lines.append(f"\n── PAUSED (retry on next run): {len(paused)} ──")

    # Gold — the heroes
    gold_result = await session.execute(
        select(DiscoveryQueryStat)
        .where(DiscoveryQueryStat.status == "gold")
        .order_by(desc(DiscoveryQueryStat.total_new_leads))
    )
    gold = gold_result.scalars().all()
    if gold:
        lines.append(f"\n── GOLD QUERIES ({len(gold)}) — running every time ──")
        for s in gold[:10]:
            lines.append(
                f"  {s.total_new_leads:>3}L {s.total_new_sources:>3}S "
                f"({s.total_runs}r)  {s.query_text[:60]}"
            )

    lines.append("")
    return "\n".join(lines)
