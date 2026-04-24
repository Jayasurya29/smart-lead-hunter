"""add_discovery_query_stats

Revision ID: 017
Revises: 016

Purpose:
  Per-query learning for the web discovery engine. Mirrors the pattern used
  by SourceIntelligence (Source.source_intelligence JSONB) but applied to
  search queries. Every discovery run records each query's result and the
  system learns which queries produce new sources + new leads.

  Promote query → GOLD (3+ runs with ≥20% "source yield" rate)
  Demote query  → JUNK (5+ runs with 0% source yield, 0 leads ever)
  Stay in MAYBE while learning (fewer than 3 runs)

  Junk queries are SKIPPED on subsequent runs, saving Serper credits.
  After 4 weeks, junk queries get one retry shot to catch drifting signals.

Create Date: 2026-04-24
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = '017'
down_revision: Union[str, None] = '016'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'discovery_query_stats',
        # The search query itself — primary key. Max 500 chars for safety
        # on very long brand-group queries like "JW Marriott W Hotels Edition
        # Conrad opening 2027".
        sa.Column('query_text', sa.String(500), primary_key=True, nullable=False),

        # Classification bucket: maybe (default) | gold | junk | paused
        # Same vocabulary as Source.patterns (gold/junk/maybe) for consistency.
        # "paused" is query-specific: a junk query temporarily re-enabled for
        # a retry run, to catch signals that may have come back.
        sa.Column('status', sa.String(20), nullable=False,
                  server_default='maybe'),

        # Aggregate counters — updated on every discovery run
        sa.Column('total_runs', sa.Integer, nullable=False, server_default='0'),
        sa.Column('total_new_sources', sa.Integer, nullable=False, server_default='0'),
        sa.Column('total_new_leads', sa.Integer, nullable=False, server_default='0'),
        sa.Column('total_duplicates', sa.Integer, nullable=False, server_default='0'),

        # Streak tracking — how many consecutive zero-yield runs. Resets on a
        # successful run. Drives the "promote to junk" decision.
        sa.Column('consecutive_zero_runs', sa.Integer, nullable=False, server_default='0'),

        # Timestamps
        sa.Column('first_run_at', postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('last_run_at', postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('last_success_at', postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column('paused_until', postgresql.TIMESTAMP(timezone=True), nullable=True),

        # Last run's per-query breakdown, for debugging and the CLI report.
        # Structure: {
        #   "sources_tested": int,
        #   "new_sources": int,
        #   "new_leads": int,
        #   "duplicates": int,
        #   "sample_domains": [list of domains],
        # }
        sa.Column('last_run_detail', postgresql.JSONB, nullable=True),

        # Audit
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True),
                  server_default=sa.func.now(), nullable=False),
        sa.Column('updated_at', postgresql.TIMESTAMP(timezone=True),
                  server_default=sa.func.now(), nullable=False),
    )

    # Index on status for fast filtering of active vs junk queries
    op.create_index(
        'ix_discovery_query_stats_status',
        'discovery_query_stats',
        ['status'],
    )


def downgrade() -> None:
    op.drop_index('ix_discovery_query_stats_status', table_name='discovery_query_stats')
    op.drop_table('discovery_query_stats')
