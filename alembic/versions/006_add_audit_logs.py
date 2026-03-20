"""Add audit_logs table

A-01: Immutable audit trail for lead state changes.
Tracks who did what, with before/after snapshots.

Revision ID: 006_add_audit_logs
Revises: 005_unique_norm_perf_idx
Create Date: 2026-03-20
"""

from typing import Sequence, Union
from alembic import op
from sqlalchemy import text

revision: str = "006_add_audit_logs"
down_revision: Union[str, None] = "005_unique_norm_perf_idx"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(text("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id SERIAL PRIMARY KEY,
            user_email VARCHAR(255),
            user_id INTEGER,
            action VARCHAR(50) NOT NULL,
            lead_id INTEGER,
            hotel_name VARCHAR(255),
            old_values JSONB DEFAULT '{}',
            new_values JSONB DEFAULT '{}',
            detail TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """))
    op.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_audit_logs_action ON audit_logs (action)"
    ))
    op.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_audit_logs_lead_id ON audit_logs (lead_id)"
    ))
    op.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_audit_logs_created_at ON audit_logs (created_at DESC)"
    ))


def downgrade() -> None:
    op.execute(text("DROP TABLE IF EXISTS audit_logs"))
