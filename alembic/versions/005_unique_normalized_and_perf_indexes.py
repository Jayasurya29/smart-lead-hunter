"""Add unique constraint on hotel_name_normalized + performance indexes

FIX C-03: Unique index prevents race condition in save_lead_to_db
          where concurrent scrapes could insert duplicate leads.
FIX M-04: Index on status for fast lead list filtering.
FIX M-05: Index on timeline_label for timeline filter.
FIX P-01: Composite index on (lead_score, status) for sorted queries.
          Index on created_at for "added today/this week" filter.
          Indexes on brand_tier, location_type for filter dropdowns.

Revision ID: 005_unique_norm_perf_idx
Revises: 004_add_user_tables
Create Date: 2026-03-20
"""

from typing import Sequence, Union
from alembic import op
from sqlalchemy import text

revision: str = "005_unique_norm_perf_idx"
down_revision: Union[str, None] = "004_add_user_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── C-03: Unique constraint on normalized hotel name ──
    # First, clean up any existing duplicates (keep the one with higher score)
    op.execute(text("""
        DELETE FROM potential_leads
        WHERE id NOT IN (
            SELECT DISTINCT ON (hotel_name_normalized) id
            FROM potential_leads
            WHERE hotel_name_normalized IS NOT NULL
            ORDER BY hotel_name_normalized, lead_score DESC NULLS LAST, id ASC
        )
        AND hotel_name_normalized IS NOT NULL
        AND hotel_name_normalized != ''
    """))

    op.execute(text(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_leads_name_normalized_unique "
        "ON potential_leads (hotel_name_normalized) "
        "WHERE hotel_name_normalized IS NOT NULL AND hotel_name_normalized != ''"
    ))

    # ── M-04 + M-05 + P-01: Performance indexes ──
    op.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_potential_leads_status "
        "ON potential_leads (status)"
    ))
    op.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_potential_leads_timeline_label "
        "ON potential_leads (timeline_label)"
    ))
    op.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_potential_leads_brand_tier "
        "ON potential_leads (brand_tier)"
    ))
    op.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_potential_leads_location_type "
        "ON potential_leads (location_type)"
    ))
    op.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_potential_leads_score_status "
        "ON potential_leads (lead_score DESC NULLS LAST, status)"
    ))
    op.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_potential_leads_created_at "
        "ON potential_leads (created_at DESC)"
    ))


def downgrade() -> None:
    op.execute(text("DROP INDEX IF EXISTS ix_leads_name_normalized_unique"))
    op.execute(text("DROP INDEX IF EXISTS ix_potential_leads_status"))
    op.execute(text("DROP INDEX IF EXISTS ix_potential_leads_timeline_label"))
    op.execute(text("DROP INDEX IF EXISTS ix_potential_leads_brand_tier"))
    op.execute(text("DROP INDEX IF EXISTS ix_potential_leads_location_type"))
    op.execute(text("DROP INDEX IF EXISTS ix_potential_leads_score_status"))
    op.execute(text("DROP INDEX IF EXISTS ix_potential_leads_created_at"))
