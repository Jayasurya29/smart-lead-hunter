"""add news_action_log (audit trail + revert for news-driven mutations)

Every mutating news action (approve a hotel, apply a person move, attach a GM)
writes a row here: what happened, when, and an `undo` recipe (a list of inverse
ops) so a bad one can be reverted cleanly. This is the safety net for the
automated writes the news system makes into potential_leads / lead_contacts /
contact_affiliations.

Revision ID: 052
Revises: 051
"""
from alembic import op
import sqlalchemy as sa

revision = "052"
down_revision = "051"
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    return sa.inspect(conn).has_table(name)


def upgrade() -> None:
    conn = op.get_bind()
    if not _has_table(conn, "news_action_log"):
        op.create_table(
            "news_action_log",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("action", sa.String(40), nullable=False),   # approve_lead / person_move / ...
            sa.Column("summary", sa.Text),                        # human-readable
            sa.Column("source_ref", sa.Text),                     # queue_id / review_id / url
            sa.Column("undo", sa.dialects.postgresql.JSONB),      # list of inverse ops
            sa.Column("reverted", sa.Boolean, nullable=False, server_default=sa.text("false")),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("reverted_at", sa.DateTime(timezone=True)),
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS ix_news_action_log_created "
            "ON news_action_log(created_at DESC)"
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS ix_news_action_log_reverted "
            "ON news_action_log(reverted)"
        )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS news_action_log")
