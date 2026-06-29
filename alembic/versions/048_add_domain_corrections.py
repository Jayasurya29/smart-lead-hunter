"""add domain_corrections (forward typo guard)

A small table of CONFIRMED typo->real domain corrections (and confirmed
drops), seeded from the human-reviewed typo audit. The inbox-sync upsert
path consults it so known typos are corrected / known-dead domains skipped
at ingest -- without any fuzzy edit-distance guessing in the live path.

Revision ID: 048
Revises: 047
"""
from alembic import op
import sqlalchemy as sa

revision = "048"
down_revision = "047"
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    return sa.inspect(conn).has_table(name)


def upgrade() -> None:
    conn = op.get_bind()
    if not _has_table(conn, "domain_corrections"):
        op.create_table(
            "domain_corrections",
            sa.Column("typo_domain", sa.Text, primary_key=True),  # bare, lowercased
            sa.Column("correct_domain", sa.Text, nullable=True),  # null when action='drop'
            sa.Column("action", sa.Text, nullable=False),         # 'correct' | 'drop'
            sa.Column("reason", sa.Text, nullable=True),
            sa.Column("added_by", sa.Text, nullable=True),
            sa.Column("added_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS domain_corrections")
