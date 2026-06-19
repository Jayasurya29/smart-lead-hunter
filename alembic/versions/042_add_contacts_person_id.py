"""add person_id to contacts (person-identity grouping)

The `contacts` table is keyed purely on email, so one human with two addresses
(e.g. a former-employer email + the current-employer email found via Wiza and
stored in secondary_email) shows up as two separate rows with no link. This adds
a nullable `person_id` grouping key — the SAME additive, reversible pattern used
on lead_contacts (migration 035): a canonical contact id shared by every row that
is the same person. NULL by default (no behavior change until populated by
scripts/dedup_contacts.py, which is dry-run-first and reversible via --reset).

  contacts.person_id  INTEGER NULL  -- shared canonical id for the same human

Revision ID: 042
Revises: 041
Create Date: 2026-06-17
"""

from alembic import op
import sqlalchemy as sa

revision = "042"
down_revision = "041"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("contacts", sa.Column("person_id", sa.Integer, nullable=True))
    # CREATE INDEX IF NOT EXISTS — matches the project's idempotent-index rule.
    op.execute("CREATE INDEX IF NOT EXISTS ix_contacts_person_id ON contacts (person_id)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_contacts_person_id")
    op.drop_column("contacts", "person_id")
