"""
DiscoveryQueryStat model
========================
Per-query learning state for the web discovery engine.

Mirrors the design of SourceIntelligence (app.services.source_intelligence)
but at the search-query level. Every discovery run updates one row per query
executed, tracking how many unique NEW sources and NEW leads that specific
query produced.

Classification states (the "status" column):

  maybe     — fewer than GOLD_MIN_TESTED runs (3). Grace period. Always run.
  gold      — proven performer: ≥3 runs with ≥20% new-source yield OR
              ever produced ≥3 new leads. Always run.
  junk      — proven waste: 5+ runs with 0 total new sources AND 0 total
              new leads ever. SKIPPED on normal runs.
  paused    — a junk query re-armed for a one-shot retry after cooldown.
              If it still produces nothing, goes back to junk.

Thresholds match the URL-pattern thresholds in intelligence_config.py
so query-level and URL-level learning behave consistently:
  GOLD_MIN_TESTED = 3
  GOLD_MIN_HIT_RATE = 0.20
  JUNK_MIN_TESTED = 5
  JUNK_MAX_HIT_RATE = 0.0

Used by:
  scripts.discover_sources.WebDiscoveryEngine    — loads stats, filters
                                                    active queries, records
                                                    results
  scripts.discovery_report                       — prints top/bottom queries
"""

from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from app.database import Base


class DiscoveryQueryStat(Base):
    __tablename__ = "discovery_query_stats"

    # The exact query string is the primary key — no separate auto ID
    # needed since queries are already unique by their own text.
    query_text = Column(String(500), primary_key=True, nullable=False)

    # Classification: maybe | gold | junk | paused
    status = Column(String(20), nullable=False, server_default="maybe")

    # Lifetime counters
    total_runs = Column(Integer, nullable=False, server_default="0")
    total_new_sources = Column(Integer, nullable=False, server_default="0")
    total_new_leads = Column(Integer, nullable=False, server_default="0")
    total_duplicates = Column(Integer, nullable=False, server_default="0")

    # Streak tracking — resets to 0 on any successful run (new source OR lead)
    consecutive_zero_runs = Column(Integer, nullable=False, server_default="0")

    # Timestamps
    first_run_at = Column(DateTime(timezone=True), nullable=True)
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    last_success_at = Column(DateTime(timezone=True), nullable=True)
    paused_until = Column(DateTime(timezone=True), nullable=True)

    # Last run detail — for CLI report + debugging
    last_run_detail = Column(JSONB, nullable=True)

    # Audit
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        onupdate=func.now(),
    )

    # ─── Public helpers ──────────────────────────────────────────

    @property
    def source_yield_rate(self) -> float:
        """Fraction of runs that produced at least 1 new source."""
        if self.total_runs == 0:
            return 0.0
        # Approximation: total_new_sources / total_runs isn't really a "rate"
        # but does track whether queries consistently produce domains.
        return self.total_new_sources / self.total_runs

    @property
    def lead_yield_rate(self) -> float:
        """Leads per run (can exceed 1 if a single run yields multiple leads)."""
        if self.total_runs == 0:
            return 0.0
        return self.total_new_leads / self.total_runs

    def should_skip(self, now) -> bool:
        """
        Whether to skip this query on the current run.

        Skip rules:
          - status=junk → skip unless paused_until expired (then it's a retry)
          - status=paused → skip if paused_until is in the future
          - anything else → run it
        """
        if self.status == "junk":
            # Junk queries get a one-shot retry every 4 weeks
            if self.paused_until is None:
                return True
            return now < self.paused_until
        if self.status == "paused" and self.paused_until is not None:
            return now < self.paused_until
        return False

    def __repr__(self):
        return (
            f"<DiscoveryQueryStat query={self.query_text!r:.40} "
            f"status={self.status} runs={self.total_runs} "
            f"sources={self.total_new_sources} leads={self.total_new_leads}>"
        )
