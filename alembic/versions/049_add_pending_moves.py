"""add pending_moves (unverified-move review queue)

A holding queue for DETECTED-but-UNVERIFIED job moves. When the move-detector
finds a likely move for a contact that has NO LinkedIn slug (so the lookup
could not anchor on the exact person and might be a namesake/work-history
artifact), it parks the candidate here instead of re-filing the live contact.
A human reviews + approves (applies the move) or rejects (drops the row).

Slug-VERIFIED moves never come here -- they auto-apply as before.

Revision ID: 049
Revises: 048
"""
from alembic import op
import sqlalchemy as sa

revision = "049"
down_revision = "048"
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    return sa.inspect(conn).has_table(name)


def upgrade() -> None:
    conn = op.get_bind()
    if not _has_table(conn, "pending_moves"):
        op.create_table(
            "pending_moves",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("contact_id", sa.Integer, nullable=False),
            sa.Column("email", sa.Text),
            sa.Column("name", sa.Text),
            sa.Column("from_org", sa.Text),          # org currently on file
            sa.Column("to_org", sa.Text, nullable=False),   # proposed new employer
            sa.Column("to_title", sa.Text),
            sa.Column("evidence", sa.Text),          # the LLM's one-line read
            sa.Column("citations", sa.Text),         # joined source URLs
            sa.Column("reason", sa.Text),            # why it was queued (no_slug, etc.)
            sa.Column(
                "status", sa.Text, nullable=False, server_default="pending"
            ),  # pending | approved | rejected
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("reviewed_at", sa.DateTime(timezone=True)),
            sa.CheckConstraint(
                "status IN ('pending','approved','rejected')",
                name="ck_pending_moves_status",
            ),
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS ix_pending_moves_status "
            "ON pending_moves(status)"
        )
        # one open candidate per contact -- re-running the sweep updates, not piles up
        op.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_pending_moves_open "
            "ON pending_moves(contact_id) WHERE status='pending'"
        )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS pending_moves")
