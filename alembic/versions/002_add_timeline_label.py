"""Add timeline_label column to potential_leads

Revision ID: 002_add_timeline_label
Revises: 001_initial_indexes
Create Date: 2026-03-17
"""

from typing import Sequence, Union
from alembic import op
from sqlalchemy import text

revision: str = "002_add_timeline_label"
down_revision: Union[str, None] = "001_initial_indexes"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(connection, table, column):
    result = connection.execute(
        text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :table AND column_name = :column"
        ),
        {"table": table, "column": column},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    conn = op.get_bind()

    # 1. Add column if it doesn't exist
    if not _column_exists(conn, "potential_leads", "timeline_label"):
        op.execute(
            "ALTER TABLE potential_leads "
            "ADD COLUMN timeline_label VARCHAR(10)"
        )

    # 2. Create index for fast filtering
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_leads_timeline "
        "ON potential_leads(timeline_label)"
    )

    # 3. Backfill existing leads with computed timeline labels.
    #    This uses a simple SQL mapping — bare years get TBD,
    #    everything else gets a rough estimate based on date math.
    #    For perfect accuracy, run the Python backfill script after migration.
    op.execute(
        "UPDATE potential_leads SET timeline_label = 'TBD' "
        "WHERE timeline_label IS NULL"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_leads_timeline")
    op.execute("ALTER TABLE potential_leads DROP COLUMN IF EXISTS timeline_label")
