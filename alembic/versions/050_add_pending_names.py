"""add pending_names (name-review queue)

A holding queue for name candidates that TIE on surname + org but FAIL the
first-initial check -- e.g. r.brady@marriott.com where the only web result is
"Lauren Brady" (surname Brady + Marriott match, but initial L != R). These are
namesake-risky to auto-write (the email belongs to a different Brady), but
occasionally legitimate (nicknames, middle names, surname-first schemes), so a
human reviews + approves or rejects instead of the resolver guessing.

Revision ID: 050
Revises: 049
"""
from alembic import op
import sqlalchemy as sa

revision = "050"
down_revision = "049"
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    return sa.inspect(conn).has_table(name)


def upgrade() -> None:
    conn = op.get_bind()
    if not _has_table(conn, "pending_names"):
        op.create_table(
            "pending_names",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("contact_id", sa.Integer, nullable=False),
            sa.Column("email", sa.Text),
            sa.Column("org", sa.Text),
            sa.Column("candidate_name", sa.Text, nullable=False),  # the found name
            sa.Column("source", sa.Text),                          # serper | grounded
            sa.Column("reason", sa.Text),                          # why queued
            sa.Column("status", sa.Text, nullable=False, server_default="pending"),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("reviewed_at", sa.DateTime(timezone=True)),
            sa.CheckConstraint(
                "status IN ('pending','approved','rejected')",
                name="ck_pending_names_status",
            ),
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS ix_pending_names_status "
            "ON pending_names(status)"
        )
        op.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_pending_names_open "
            "ON pending_names(contact_id) WHERE status='pending'"
        )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS pending_names")
