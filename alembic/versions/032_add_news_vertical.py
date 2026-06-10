"""032: add vertical to hotel_news.

The news feed now covers education and healthcare alongside hotels. This tags
each story with its sector so the UI can filter by it. Defaults to 'hotel' so
the 64 existing rows keep their (correct) hotel classification.

Revision ID: 032
Revises: 031
"""

import sqlalchemy as sa
from alembic import op

revision = "032"
down_revision = "031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "hotel_news",
        sa.Column(
            "vertical",
            sa.String(20),
            server_default="hotel",
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("hotel_news", "vertical")
