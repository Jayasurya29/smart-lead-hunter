"""widen scrape_logs.status to 30 chars

Created 2026-05-05 (audit fix bug #24).

The application writes 'completed' / 'completed_with_errors' which
exceeds the prior VARCHAR(20). On Postgres this would actually error
on insert — likely silently caught upstream. Widening to VARCHAR(30)
gives headroom and aligns the model with the writers.

Revision ID: 022
Revises: 021
Create Date: 2026-05-05
"""

from alembic import op
import sqlalchemy as sa


revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "scrape_logs",
        "status",
        existing_type=sa.String(length=20),
        type_=sa.String(length=30),
        existing_nullable=True,
    )


def downgrade() -> None:
    # Truncate any values that won't fit before narrowing the column
    op.execute("UPDATE scrape_logs SET status = LEFT(status, 20)")
    op.alter_column(
        "scrape_logs",
        "status",
        existing_type=sa.String(length=30),
        type_=sa.String(length=20),
        existing_nullable=True,
    )
