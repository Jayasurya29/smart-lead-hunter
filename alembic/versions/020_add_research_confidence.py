"""Add research_confidence column to research_history.

Revision ID: 020
Revises: 019

Anti-hallucination Phase 2: surface a confidence flag computed by the
Researcher agent ('high' / 'medium' / 'low') so the UI can show a
warning badge on briefs that came from sparse research data. Reps see
the warning and know to fact-check the brief manually before sending.
"""
from alembic import op
import sqlalchemy as sa


revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "research_history",
        sa.Column("research_confidence", sa.String(10), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("research_history", "research_confidence")
