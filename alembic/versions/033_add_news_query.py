"""033: add query to hotel_news.

Records which search query first surfaced each story, so the Sources page can
show per-query productivity (workhorse vs dead queries) alongside per-source
patterns (continuous producers vs one-time hits).

Revision ID: 033
Revises: 032
"""

import sqlalchemy as sa
from alembic import op

revision = "033"
down_revision = "032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "hotel_news",
        sa.Column("query", sa.String(200), nullable=True),
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_hotel_news_query ON hotel_news (query)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_hotel_news_query")
    op.drop_column("hotel_news", "query")
