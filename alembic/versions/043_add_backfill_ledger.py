"""backfill ledger + progress (one-time historical pull)

The 2020-onward backfill reads ~1.3M messages across mailboxes over many manual,
interruptible runs. Two tables make that safe:

  synced_messages   — one row per real email already COUNTED, keyed on the RFC
                      `Message-ID` header (stable across mailboxes). The count
                      gate inserts ON CONFLICT DO NOTHING; a hit means the email
                      was already counted (prior batch, prior run, or another
                      mailbox) so it is skipped. This is what makes "count each
                      email once" hold across the whole multi-run job.

  backfill_progress — per-mailbox checkpoint: the oldest→newest window completed,
                      so a re-run resumes instead of restarting.

Both are additive and backfill-only — daily sync never touches them.

Revision ID: 043
Revises: 042
Create Date: 2026-06-18
"""

from alembic import op
import sqlalchemy as sa

revision = "043"
down_revision = "042"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "synced_messages",
        sa.Column("rfc_message_id", sa.Text, primary_key=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "backfill_progress",
        sa.Column("mailbox", sa.Text, primary_key=True),
        sa.Column("window_done_before", sa.Date, nullable=True),  # oldest→newest checkpoint
        sa.Column("status", sa.Text, nullable=True),  # running / done / error
        sa.Column("messages_seen", sa.Integer, server_default="0"),
        sa.Column("messages_counted", sa.Integer, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("backfill_progress")
    op.drop_table("synced_messages")
