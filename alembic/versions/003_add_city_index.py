"""Add functional index on lower(city) for location filtering

Revision ID: 003_add_city_index
Revises: 002_add_timeline_label
Create Date: 2026-03-17
"""

from typing import Sequence, Union
from alembic import op
from sqlalchemy import text

revision: str = "003_add_city_index"
down_revision: Union[str, None] = "002_add_timeline_label"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Functional index on lower(city) — used by South Florida filter
    # which does func.lower(city).in_([50 cities])
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_leads_city_lower "
        "ON potential_leads (lower(city))"
    )
    # Functional index on lower(state) — used by state-based filters
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_leads_state_lower "
        "ON potential_leads (lower(state))"
    )
    # Functional index on lower(country) — used by Caribbean filter
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_leads_country_lower "
        "ON potential_leads (lower(country))"
    )
    # Index on opening_date for year filter (ILIKE '%2026%')
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_leads_opening_date_trgm "
        "ON potential_leads USING gin(opening_date gin_trgm_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_leads_city_lower")
    op.execute("DROP INDEX IF EXISTS idx_leads_state_lower")
    op.execute("DROP INDEX IF EXISTS idx_leads_country_lower")
    op.execute("DROP INDEX IF EXISTS idx_leads_opening_date_trgm")
