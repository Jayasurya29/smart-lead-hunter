"""Add sources column to research_history.

Revision ID: 021
Revises: 020

Anti-hallucination Phase 3: capture every source URL the Researcher
gathered during the 30-snippet web search so the UI can show clickable
citation cards. Reps no longer have to trust the AI brief blindly —
they can click [News], [Press], [LinkedIn] etc. to verify exactly
where each fact came from.

Stored as JSONB. Each entry is {url, title, snippet, category}.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "research_history",
        sa.Column("sources", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("research_history", "sources")
