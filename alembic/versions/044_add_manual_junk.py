"""manual junk override + junk_domains (learning junk system)

Two pieces that let reps curate junk and have the system learn from it:

  contacts.manual_category   — a rep's override of the AI category. When set,
                               it WINS over contact_category everywhere (counts,
                               list, export) and is NEVER touched by the
                               classifier or re-sync. "Send to junk" sets it to
                               'junk'; "restore" clears it.

  junk_domains               — rep-curated domains that auto-junk. Pass 1 of the
                               classifier checks this set and resolves matching
                               contacts to junk deterministically (no LLM). One
                               human decision → a permanent, free rule. Reversible.

Effective category = COALESCE(manual_category, contact_category). Junk never
infiltrates the real contact count.

Revision ID: 044
Revises: 043
Create Date: 2026-06-18
"""

from alembic import op
import sqlalchemy as sa

revision = "044"
down_revision = "043"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("contacts", sa.Column("manual_category", sa.Text, nullable=True))
    op.add_column("contacts", sa.Column("manual_category_at", sa.DateTime(timezone=True), nullable=True))
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_contacts_manual_category ON contacts(manual_category)"
    )
    op.create_table(
        "junk_domains",
        sa.Column("domain", sa.Text, primary_key=True),  # bare, lowercased: 'mariana.com'
        sa.Column("added_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("added_by", sa.Text, nullable=True),
        sa.Column("reason", sa.Text, nullable=True),
        sa.Column("contacts_at_add", sa.Integer, server_default="0"),
    )


def downgrade() -> None:
    op.drop_table("junk_domains")
    op.execute("DROP INDEX IF EXISTS ix_contacts_manual_category")
    op.drop_column("contacts", "manual_category_at")
    op.drop_column("contacts", "manual_category")
