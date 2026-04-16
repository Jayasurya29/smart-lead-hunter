"""Add strategist_priority and strategist_reasoning to lead_contacts

Revision ID: 011_add_strategist_priority
Revises: 010_fix_website_verified
Create Date: 2026-04-16

The iterative researcher's Iter 6 (Reasoning Pass) produces a final
priority verdict (P1/P2/P3/P4) and a one-sentence strategic reasoning
per contact. Both need to persist in lead_contacts so the dashboard
can show them to the sales team and sort contacts by strategist priority.
"""

from typing import Sequence, Union
from alembic import op
from sqlalchemy import text

revision: str = "011_add_strategist_priority"
down_revision: Union[str, None] = "010_fix_website_verified"
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

    if not _column_exists(conn, "lead_contacts", "strategist_priority"):
        op.execute(
            "ALTER TABLE lead_contacts "
            "ADD COLUMN strategist_priority VARCHAR(4)"
        )

    if not _column_exists(conn, "lead_contacts", "strategist_reasoning"):
        op.execute(
            "ALTER TABLE lead_contacts "
            "ADD COLUMN strategist_reasoning TEXT"
        )

    # Index on strategist_priority for fast sort in the dashboard route
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_lead_contacts_strategist_priority "
        "ON lead_contacts(strategist_priority)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_lead_contacts_strategist_priority")
    op.execute(
        "ALTER TABLE lead_contacts "
        "DROP COLUMN IF EXISTS strategist_reasoning"
    )
    op.execute(
        "ALTER TABLE lead_contacts "
        "DROP COLUMN IF EXISTS strategist_priority"
    )
