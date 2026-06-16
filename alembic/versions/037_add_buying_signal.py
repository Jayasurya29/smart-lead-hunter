"""add buying_signal_* columns (content-based opportunity score)

The existing contacts.opportunity_score is EMAIL VOLUME (interaction_count x
brand_multiplier): a real procurement buyer with few emails scores ~3/100 while
the UI tags them "High opportunity" -- a visible contradiction.

These columns hold a CONTENT-based score instead: what the thread is SAYING and
who it is WITH, computed by app/services/buying_signal_engine.score_thread.

  buying_signal_score   0..100, from the BUYER's own buying intent (their
                        requests/approvals), corroborated by JA's internal
                        build work (style#, qty, cost, SKU activity = deal
                        substance). Scored to the EXTERNAL counterparty, never
                        to JA staff who type the action verbs.
  buying_signal_stage   sales-stage label derived from the furthest point the
                        thread reached: prospecting | dialogue | proposal/quote
                        | approved/ordered | existing-service | noise | internal
  buying_signal_reason  the explainable "why" string (signals + buyer + notes)
  buying_signal_deal    extracted deal size ("total 172 units; $9.10",
                        "3 sets each (par)", "60 employees")
  buying_signal_at      when this was last computed (NULL = never scored)

opportunity_score is LEFT IN PLACE (non-destructive, reversible). The UI decides
which to surface. A contact in multiple threads keeps the MAX score (hottest
active deal) with that thread's stage/reason.

Revision ID: 037
Revises: 036
Create Date: 2026-06-16
"""

from alembic import op
import sqlalchemy as sa

revision = "037"
down_revision = "036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("contacts", sa.Column("buying_signal_score", sa.Integer, nullable=True))
    op.add_column("contacts", sa.Column("buying_signal_stage", sa.Text, nullable=True))
    op.add_column("contacts", sa.Column("buying_signal_reason", sa.Text, nullable=True))
    op.add_column("contacts", sa.Column("buying_signal_deal", sa.Text, nullable=True))
    op.add_column(
        "contacts",
        sa.Column("buying_signal_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_contacts_buying_signal_score "
        "ON contacts(buying_signal_score DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_contacts_buying_signal_score")
    op.drop_column("contacts", "buying_signal_at")
    op.drop_column("contacts", "buying_signal_deal")
    op.drop_column("contacts", "buying_signal_reason")
    op.drop_column("contacts", "buying_signal_stage")
    op.drop_column("contacts", "buying_signal_score")
