"""add_status_index_and_insightly_ids

Revision ID: 007
Revises: 006_add_audit_logs
Create Date: 2026-03-20

- Adds index on potential_leads.status column
- Adds composite index for dashboard stats
- Adds expression indexes for location filters
- Adds insightly_lead_ids JSONB column for fast CRM cleanup
"""
from alembic import op


# revision identifiers
revision = "007"
down_revision = "006_add_audit_logs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # All indexes use IF NOT EXISTS to safely handle re-runs
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_potential_leads_status "
        "ON potential_leads (status)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_potential_leads_status_timeline_created "
        "ON potential_leads (status, timeline_label, created_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_leads_city_lower "
        "ON potential_leads (LOWER(city))"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_leads_state_lower "
        "ON potential_leads (LOWER(state))"
    )
    # Store all pushed Insightly Lead IDs for fast CRM cleanup
    op.execute(
        "ALTER TABLE potential_leads "
        "ADD COLUMN IF NOT EXISTS insightly_lead_ids JSONB DEFAULT '[]'"
    )


def downgrade() -> None:
    op.drop_column("potential_leads", "insightly_lead_ids")
    op.execute("DROP INDEX IF EXISTS ix_leads_state_lower")
    op.execute("DROP INDEX IF EXISTS ix_leads_city_lower")
    op.drop_index("ix_potential_leads_status_timeline_created", "potential_leads")
    op.drop_index("ix_potential_leads_status", "potential_leads")
