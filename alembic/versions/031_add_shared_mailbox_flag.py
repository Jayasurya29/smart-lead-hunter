"""031: add is_shared_mailbox to contacts.

Role inboxes (ap@, procurement@, frontdesk@…) are real outreach channels
but not people — this flag lets the UI/stats treat them as inboxes
instead of fake humans with ★DM badges.

Revision ID: 031
Revises: 030
"""

import sqlalchemy as sa
from alembic import op

revision = "031"
down_revision = "030"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "contacts",
        sa.Column(
            "is_shared_mailbox",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("contacts", "is_shared_mailbox")
