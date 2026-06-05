"""Hotel intelligence news feed: appointments + market news for USA/Caribbean
4-star+ properties, with relationship triangulation against our contacts.

Revision ID: 030
Revises: 029
"""

import sqlalchemy as sa
from alembic import op

revision = "030"
down_revision = "029"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "hotel_news",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("url", sa.Text, nullable=False, unique=True),
        sa.Column("title", sa.Text),
        sa.Column("snippet", sa.Text),
        sa.Column("source", sa.String(160)),
        sa.Column("published_hint", sa.String(80)),
        sa.Column("category", sa.String(40)),       # appointment/opening/...
        sa.Column("region", sa.String(20)),          # usa / caribbean / other
        sa.Column("hotel_name", sa.String(300)),
        sa.Column("brand", sa.String(160)),
        sa.Column("person_name", sa.String(200)),
        sa.Column("person_title", sa.String(200)),
        sa.Column("luxury", sa.Boolean, server_default=sa.text("false")),
        sa.Column("in_pipeline", sa.Boolean, server_default=sa.text("false")),
        sa.Column("pipeline_ref", sa.String(300)),
        sa.Column("relationship_hits", sa.dialects.postgresql.JSONB),
        sa.Column("raw", sa.dialects.postgresql.JSONB),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_hotel_news_created "
        "ON hotel_news (created_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_hotel_news_category "
        "ON hotel_news (category)"
    )


def downgrade() -> None:
    op.drop_table("hotel_news")
