"""add secondary_email to lead_contacts

Revision ID: 026
Revises: 025
Create Date: 2026-05-21
"""

from alembic import op
import sqlalchemy as sa

revision = "026"
down_revision = "025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "lead_contacts",
        sa.Column("secondary_email", sa.String(255), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("lead_contacts", "secondary_email")
