"""add secondary_email to contacts

When a contact changes employer (job-change detection in tier2 enrichment), the
email we have is their FORMER company's address (e.g. brendan.payze@ritzcarlton
.com) and is likely inactive. We NEVER overwrite the primary email -- it is the
record of how we knew them and anchors the thread history. Instead, a found
current-employer email (via Wiza lookup at the new company) is stored here as a
secondary so the UI can show both: "former: ...@ritzcarlton (may be inactive) /
current: ...@stregis (found)".

  secondary_email  TEXT  -- a found alternate/current email; primary is untouched

Revision ID: 041
Revises: 040
Create Date: 2026-06-16
"""

from alembic import op
import sqlalchemy as sa

revision = "041"
down_revision = "040"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("contacts", sa.Column("secondary_email", sa.Text, nullable=True))


def downgrade() -> None:
    op.drop_column("contacts", "secondary_email")
