"""add news_contact_review (new contact for a hotel already in pipeline)

When a news story names a person at a hotel we ALREADY own (in pipeline /
existing_hotels) and we don't already know that person, we don't add a
duplicate hotel — instead we queue the person here for one-click review so
they can be attached to the existing account as a fresh contact.

Revision ID: 053
Revises: 052
"""
from alembic import op
import sqlalchemy as sa

revision = "053"
down_revision = "052"
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    return sa.inspect(conn).has_table(name)


def upgrade() -> None:
    conn = op.get_bind()
    if not _has_table(conn, "news_contact_review"):
        op.create_table(
            "news_contact_review",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("news_id", sa.Integer),
            sa.Column("hotel_name", sa.Text),
            sa.Column("person_name", sa.Text, nullable=False),
            sa.Column("person_title", sa.Text),
            sa.Column("account_type", sa.String(20)),   # potential_lead / existing_hotel
            sa.Column("account_id", sa.Integer),
            sa.Column("account_name", sa.Text),
            sa.Column("region", sa.String(20)),
            sa.Column("event_type", sa.String(40)),
            sa.Column("status", sa.String(12), nullable=False, server_default="pending"),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("reviewed_at", sa.DateTime(timezone=True)),
        )
        # one pending row per (person, account) — collapses repeat stories
        op.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_news_contact_review_pending "
            "ON news_contact_review (lower(person_name), account_type, account_id) "
            "WHERE status='pending'"
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS ix_news_contact_review_status "
            "ON news_contact_review(status)"
        )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS news_contact_review")
