"""Add project_type to potential_leads

Revision ID: 015_add_project_type
Revises: 014_add_contact_evidence

Adds a project_type column to distinguish between:
  - new_opening      (greenfield, first-ever opening)
  - renovation       (existing hotel, refurb, reopening after closure)
  - rebrand          (existing hotel, changing brand affiliation)
  - reopening        (hurricane closure, seasonal reopen)
  - conversion       (changing from one hotel type to another)
  - ownership_change (sold, new owner, no brand/operator change)
  - residences_only  (branded residences — no hotel, REJECT)

Without this column, the system can't tell a greenfield build from a
renovation reopening, which causes incorrect timeline labels:
  - Sandals Montego Bay: open since 1981, closed for $200M renovation,
    reopening Dec 2026. This is a renovation, NOT a new build.
  - Pyrmont Curaçao: may be an existing hotel rebranding to Autograph.

The field drives:
  - Lead detail "Project Type" badge in UI
  - Contact enrichment pre-opening owner boost (new_opening/renovation → HOT)
  - Timeline calculation (reopening dates feed into timeline_label properly)
"""
from alembic import op
import sqlalchemy as sa

revision = "015_add_project_type"
down_revision = "014_add_contact_evidence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "potential_leads",
        sa.Column("project_type", sa.String(length=30), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("potential_leads", "project_type")
