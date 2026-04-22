"""Add score_breakdown to lead_contacts

Revision ID: 013_add_contact_score_breakdown
Revises: 012_add_search_name

Adds a JSONB column to lead_contacts storing the per-contact scoring
breakdown: tier + base points, scope + multiplier, title_score,
strategist priority + floor, final_score, and the human-readable
formula string.

This closes the "why is Elie Khoury stuck at 5?" mystery by making
scoring transparent and auditable on every contact.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "013_add_contact_score_breakdown"
down_revision = "012_add_search_name"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "lead_contacts",
        sa.Column("score_breakdown", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("lead_contacts", "score_breakdown")
