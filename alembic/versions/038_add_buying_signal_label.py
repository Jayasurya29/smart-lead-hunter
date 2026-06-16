"""add buying_signal_label + buying_signal_team (relationship card)

Phase 2.5 of the content-based opportunity work. The buying-signal engine now,
in addition to a score, reads each thread to answer the two questions Jay
actually cares about:

  1. LABEL -- is this contact really PURCHASING from us, or are they a vendor /
     noise / just an external contact? Derived from body evidence (the buyer's
     own buying language), with a guard so transactional role inboxes
     (accounting@, ap@, invoice@) are never mislabeled as buyers.
       buyer_evidence  -- we can SEE them buying (approve/order/price/sample)
       active_contact  -- engaged (proposal/dialogue) but no clear buy yet
       contact         -- external, conversational, no buying signal
       vendor_or_noise -- selling to us / automated / role inbox
       internal        -- no external party

  2. TEAM -- WHO ELSE on the client side is in the conversation: the buyer's
     colleagues / the buying committee, pulled from the thread participants
     (name + email + org from their signatures). Stored as JSON.

Both are written onto the BUYER's contact row at sync time. These are a NEW
signal -- they do NOT overwrite the existing contact_category; the UI / a human
can compare and promote later.

  buying_signal_label  TEXT  (the five-way label above)
  buying_signal_team   JSONB (list of {name, email, org} -- their colleagues)

Revision ID: 038
Revises: 037
Create Date: 2026-06-16
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "038"
down_revision = "037"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("contacts", sa.Column("buying_signal_label", sa.Text, nullable=True))
    op.add_column(
        "contacts",
        sa.Column("buying_signal_team", postgresql.JSONB, nullable=True),
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_contacts_buying_signal_label "
        "ON contacts(buying_signal_label)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_contacts_buying_signal_label")
    op.drop_column("contacts", "buying_signal_team")
    op.drop_column("contacts", "buying_signal_label")
