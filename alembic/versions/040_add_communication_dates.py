"""add communication timeline dates to contacts

Real message dates from the Gmail thread (captured in inbox_sync from each
message's internalDate + From header), DISTINCT from sync time (first_seen/
last_seen, which are when our sync touched the row). These let the UI show the
true relationship picture instead of a misleading sync timestamp:

  first_message_at   timestamptz  -- earliest message in the relationship
  last_inbound_at    timestamptz  -- last time THEY wrote to us (real 2-way)
  last_outbound_at   timestamptz  -- last time WE wrote to them

The gap between last_outbound_at and last_inbound_at is the honest signal: if we
emailed (outbound) more recently than they ever replied (inbound), they've gone
quiet -- a human reads that and judges. The system shows dates, it does not label
anyone "cold".

Revision ID: 040
Revises: 039
Create Date: 2026-06-16
"""

from alembic import op
import sqlalchemy as sa

revision = "040"
down_revision = "039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("contacts", sa.Column("first_message_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("contacts", sa.Column("last_inbound_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("contacts", sa.Column("last_outbound_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("contacts", "last_outbound_at")
    op.drop_column("contacts", "last_inbound_at")
    op.drop_column("contacts", "first_message_at")
