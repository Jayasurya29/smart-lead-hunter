"""Add search_name and former_names to potential_leads

Revision ID: 012_add_search_name
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "012_add_search_name"
down_revision = "011_add_strategist_priority"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("potential_leads", sa.Column("search_name", sa.String(255), nullable=True))
    op.add_column("potential_leads", sa.Column("former_names", JSONB, nullable=True))


def downgrade() -> None:
    op.drop_column("potential_leads", "former_names")
    op.drop_column("potential_leads", "search_name")
