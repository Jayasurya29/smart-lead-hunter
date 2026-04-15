"""fix_website_verified_column_type

Changes website_verified from Boolean to VARCHAR(10) to store
'auto', 'manual', or NULL instead of true/false.

Revision ID: 010_fix_website_verified
Revises: 009_merge
Create Date: 2026-04-14
"""

from alembic import op
import sqlalchemy as sa

revision = "010_fix_website_verified"
down_revision = "009_merge"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "potential_leads",
        "website_verified",
        existing_type=sa.Boolean(),
        type_=sa.String(10),
        existing_nullable=True,
        postgresql_using="CASE WHEN website_verified THEN 'manual' ELSE NULL END",
    )


def downgrade():
    op.alter_column(
        "potential_leads",
        "website_verified",
        existing_type=sa.String(10),
        type_=sa.Boolean(),
        existing_nullable=True,
        postgresql_using="website_verified IS NOT NULL",
    )
