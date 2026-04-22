"""Add evidence array to lead_contacts

Revision ID: 014_add_contact_evidence
Revises: 013_add_contact_score_breakdown

Adds a JSONB column to lead_contacts storing per-contact evidence array.
Each evidence item is {quote, source_url, source_title, source_domain,
trust_tier, source_year, captured_at} — letting the UI show rich source
cards with trust badges instead of just a single opaque URL.

Evidence is captured at enrichment time, not retrofitted, so historical
contacts will have null/empty evidence until their leads are re-enriched.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "014_add_contact_evidence"
down_revision = "013_add_contact_score_breakdown"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "lead_contacts",
        sa.Column("evidence", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("lead_contacts", "evidence")
