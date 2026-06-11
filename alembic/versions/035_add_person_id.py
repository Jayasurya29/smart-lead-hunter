"""add person_id grouping key to lead_contacts (entity resolution)

person_id is a nullable grouping key: lead_contacts rows that are the SAME
human share one person_id. It is NOT a row-collapsing merge and NOT an FK to a
persons table — it's an additive, reversible identity tag that lets the resolver
gather one person's several affiliations (incl. job changes across companies)
without deleting any row or touching the locked lead_id XOR existing_hotel_id
invariant. NULL = not yet resolved (the default for every existing row).

Revision ID: 035
Revises: 034
Create Date: 2026-06-10
"""

from alembic import op
import sqlalchemy as sa

revision = "035"
down_revision = "034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "lead_contacts",
        sa.Column("person_id", sa.Integer, nullable=True),
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_lead_contacts_person_id "
        "ON lead_contacts (person_id)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_lead_contacts_person_id")
    op.drop_column("lead_contacts", "person_id")
